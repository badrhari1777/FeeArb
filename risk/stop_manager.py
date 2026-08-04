"""Protective stop/take management.

This module orchestrates protective orders (stop-loss / take-profit) across
exchanges using the existing ccxt-based ExchangeGateway infrastructure. It is
best-effort and defensive: all network/placement errors are logged and bubbled
up to the caller via the returned actions list; no exceptions are raised to
halt the main refresh loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from execution.accounts import (
    EXCHANGE_SPECS,
    ExchangeGateway,
    bitget_private_params,
    bitget_uta_enabled,
    normalize_symbol,
    _safe_float,
)
from exchanges import get_adapter
try:  # ccxt is optional for typing; handled at runtime.
    from ccxt.base.errors import RequestTimeout  # type: ignore
except Exception:  # pragma: no cover - fallback when ccxt missing
    RequestTimeout = tuple()  # type: ignore
from risk.config import RiskConfig
from utils.cache_db import SymbolMeta, get_or_fetch_symbol_meta, upsert_symbol_meta
from utils.notifications import NotificationRouter
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)
protective_logger = logging.getLogger("protective")
# Ensure protective log is visible in stdout even if root config is sparse.
if not protective_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    handler.setFormatter(formatter)
    protective_logger.addHandler(handler)
protective_logger.setLevel(logging.INFO)
protective_logger.propagate = False


def _fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    try:
        with urlopen(req, timeout=10) as resp:  # nosec
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        try:
            payload = exc.read()
            if payload:
                return json.loads(payload.decode("utf-8"))
        except Exception:  # pragma: no cover - defensive
            pass
        return {"code": exc.code, "msg": str(exc)}
    except URLError as exc:
        return {"code": "url_error", "msg": str(exc)}


@dataclass(slots=True)
class ProtectiveTarget:
    stop: float | None
    takes: list["TakeTarget"]
    quantity: float
    side: str
    exchange: str
    symbol: str
    position_id: str | None
    margin_mode: str | None = None
    pos_side: str | None = None
    mark_price: float | None = None
    entry_price: float | None = None


@dataclass(slots=True)
class TakeTarget:
    price: float
    quantity: float


class InvalidProtectivePrice(ValueError):
    """Raised when a computed stop/take price is invalid for placement."""


def _as_bool_text(value: bool) -> str:
    return "true" if value else "false"


def _format_step_value(value: float, step: float, *, rounding=ROUND_HALF_UP) -> str:
    step_dec = Decimal(str(step))
    if step_dec <= 0:
        return str(value)
    value_dec = Decimal(str(value))
    units = (value_dec / step_dec).to_integral_value(rounding=rounding)
    rounded = units * step_dec
    text = format(rounded.normalize(), "f")
    return "0" if text == "-0" else text


class ProtectiveOrderManager:
    """Best-effort protective order synchroniser built on ccxt gateways."""

    def __init__(
        self,
        risk_config: RiskConfig,
        blocked_exchanges: set[str] | None = None,
        notifier: NotificationRouter | None = None,
    ) -> None:
        self._risk_config = risk_config
        self._gateways: dict[str, ExchangeGateway] = {
            spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS
        }
        self._blocked = {ex.lower() for ex in (blocked_exchanges or {"mexc"})}
        self._notifier = notifier or NotificationRouter(
            primary_channel=getattr(risk_config, "notification_primary_channel", "telegram"),
            fallback_channel=getattr(risk_config, "notification_fallback_channel", "none"),
            telegram_chat_id=getattr(risk_config, "telegram_alert_chat_id", ""),
        )
        self._lock = asyncio.Lock()
        self._warned_at: dict[str, float] = {}
        self._existing_cache: dict[tuple[str, str, str | None, str | None, float | None, float | None], tuple[dict[str, Any], float]] = {}
        self._existing_cache_ttl = 30.0

    async def _call_with_time_sync_retry(
        self,
        gateway: ExchangeGateway,
        operation: str,
        callback,
    ) -> Any:
        retry = getattr(gateway, "_call_with_time_sync_retry", None)
        if callable(retry):
            return await retry(operation, callback)
        return await callback()

    def _refresh_symbol_meta(self, gateway: ExchangeGateway, symbol: str) -> SymbolMeta | None:
        if gateway.slug != "binance":
            return None
        try:
            adapter = get_adapter("binance")
        except Exception:
            return None
        exch_symbol = adapter.map_symbol(symbol) or symbol
        url = f"{getattr(adapter, 'base_url', 'https://fapi.binance.com')}/fapi/v1/exchangeInfo?"
        payload = _fetch_json(url + urlencode({"symbol": exch_symbol}))
        if payload.get("code") not in (None, 0):
            return None
        items = payload.get("symbols") or []
        info = items[0] if items else None
        if not info:
            return None
        filters = {item.get("filterType"): item for item in info.get("filters") or []}
        price_filter = filters.get("PRICE_FILTER") or {}
        lot_filter = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE") or {}
        notional_filter = filters.get("MIN_NOTIONAL") or {}
        meta = SymbolMeta(
            exchange=gateway.slug,
            symbol=exch_symbol,
            contract_size=_safe_float(info.get("contractSize")) or 1.0,
            price_step=_safe_float(price_filter.get("tickSize")),
            qty_step=_safe_float(lot_filter.get("stepSize")),
            min_qty=_safe_float(lot_filter.get("minQty")),
            max_qty=_safe_float(lot_filter.get("maxQty")),
            min_notional=_safe_float(notional_filter.get("notional") or notional_filter.get("minNotional")),
            max_leverage=_safe_float(info.get("maxLeverage")),
            tick_size=_safe_float(price_filter.get("tickSize")),
        )
        upsert_symbol_meta(meta)
        return meta

    async def _binance_algo_request(
        self,
        gateway: ExchangeGateway,
        *,
        method: str,
        path: str,
        params: Mapping[str, Any],
    ) -> Any:
        client = gateway.client
        if client is None:
            raise RuntimeError("client_unavailable")
        return await client.request(path, "fapiPrivate", method, dict(params))

    async def _cancel_binance_algo_order(
        self,
        gateway: ExchangeGateway,
        *,
        algo_id: str,
    ) -> None:
        await self._binance_algo_request(
            gateway,
            method="DELETE",
            path="algoOrder",
            params={"algoId": algo_id},
        )

    async def _fetch_binance_open_algo_orders(
        self,
        gateway: ExchangeGateway,
        symbol: str | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            exch_symbol = gateway.map_symbol(symbol) or symbol
            params["symbol"] = exch_symbol
        payload = await self._binance_algo_request(
            gateway,
            method="GET",
            path="openAlgoOrders",
            params=params,
        )
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return list(payload.get("orders") or payload.get("data") or payload.get("rows") or [])
        return []

    @staticmethod
    def _cancel_already_resolved(exchange: str, message: str) -> bool:
        msg = str(message or "").lower()
        if not msg:
            return False
        common_markers = (
            "unknown order",
            "order not found",
            "not exist",
            "does not exist",
            "already canceled",
            "already cancelled",
            "finished",
            "filled",
        )
        if any(marker in msg for marker in common_markers):
            return True
        exchange_name = str(exchange or "").lower()
        if exchange_name == "binance" and "-2011" in msg:
            return True
        if exchange_name == "kucoin" and ("100004" in msg or "order cannot be canceled" in msg):
            return True
        return False

    async def _place_binance_algo_conditional(
        self,
        gateway: ExchangeGateway,
        target: ProtectiveTarget,
        *,
        order_type: str,
        trigger_price: float,
        quantity: float,
    ) -> None:
        client = gateway.client
        if client is None:
            raise RuntimeError("client_unavailable")
        exch_symbol = gateway.map_symbol(target.symbol) or target.symbol
        side = "SELL" if target.side == "long" else "BUY"
        params: dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": exch_symbol,
            "side": side,
            "type": order_type,
            # Keep trigger basis aligned with liquidation/mark-risk logic and
            # guard against transient contract-price dislocations.
            "workingType": "MARK_PRICE",
            "priceProtect": "TRUE",
        }
        sym_meta = get_or_fetch_symbol_meta(
            gateway.slug,
            exch_symbol,
            lambda: self._refresh_symbol_meta(gateway, target.symbol),
        )
        price_step = _safe_float(getattr(sym_meta, "tick_size", None) if sym_meta else None) or _safe_float(
            getattr(sym_meta, "price_step", None) if sym_meta else None
        )
        qty_step = _safe_float(getattr(sym_meta, "qty_step", None) if sym_meta else None)
        if price_step and price_step > 0:
            params["triggerPrice"] = _format_step_value(trigger_price, price_step)
        else:
            params["triggerPrice"] = trigger_price
            for symbol_for_precision in (target.symbol, exch_symbol):
                try:
                    params["triggerPrice"] = client.price_to_precision(symbol_for_precision, trigger_price)
                    break
                except Exception:
                    continue
        if qty_step and qty_step > 0:
            params["quantity"] = _format_step_value(quantity, qty_step, rounding=ROUND_DOWN)
        else:
            params["quantity"] = quantity
            for symbol_for_precision in (target.symbol, exch_symbol):
                try:
                    params["quantity"] = client.amount_to_precision(symbol_for_precision, quantity)
                    break
                except Exception:
                    continue
        formatted_trigger = _safe_float(params.get("triggerPrice"))
        formatted_qty = _safe_float(params.get("quantity"))
        if formatted_trigger is None or not math.isfinite(formatted_trigger) or formatted_trigger <= 0:
            raise InvalidProtectivePrice(
                f"invalid_binance_trigger_price:{target.symbol}:{params.get('triggerPrice')}"
            )
        if formatted_qty is None or not math.isfinite(formatted_qty) or formatted_qty <= 0:
            raise InvalidProtectivePrice(
                f"invalid_binance_trigger_quantity:{target.symbol}:{params.get('quantity')}"
            )
        pos_side = str(target.pos_side or "").strip().lower()
        if pos_side in ("long", "short"):
            params["positionSide"] = pos_side.upper()
        elif pos_side in ("net", "both"):
            params["positionSide"] = "BOTH"
        if params.get("positionSide") in (None, "", "BOTH"):
            params["reduceOnly"] = _as_bool_text(True)
        try:
            await self._binance_algo_request(
                gateway,
                method="POST",
                path="algoOrder",
                params=params,
            )
        except Exception as exc:
            logger.warning(
                "Binance algo protective placement failed for %s %s %s params=%s err=%s",
                target.symbol,
                target.side,
                order_type,
                {
                    key: params.get(key)
                    for key in (
                        "algoType",
                        "symbol",
                        "side",
                        "type",
                        "triggerPrice",
                        "quantity",
                        "workingType",
                        "priceProtect",
                        "positionSide",
                        "reduceOnly",
                    )
                },
                exc,
            )
            raise

    def update_config(self, risk_config: RiskConfig) -> None:
        self._risk_config = risk_config
        self._notifier.update_config(
            primary_channel=getattr(risk_config, "notification_primary_channel", "telegram"),
            fallback_channel=getattr(risk_config, "notification_fallback_channel", "none"),
            telegram_chat_id=getattr(risk_config, "telegram_alert_chat_id", ""),
        )

    async def close(self) -> None:
        await asyncio.gather(
            *(gateway.close() for gateway in self._gateways.values()),
            return_exceptions=True,
        )

    async def sync_protective_orders(
        self,
        positions: Iterable[Mapping[str, Any]],
        *,
        force_fetch_existing: bool = False,
    ) -> list[dict[str, Any]]:
        """Compute and place protective orders for the supplied positions."""
        async with self._lock:
            tasks = []
            actions: list[dict[str, Any]] = []
            grouped = self._group_by_symbol(list(positions))
            for symbol, legs in grouped.items():
                targets = self._compute_targets(symbol, legs)
                for target in targets:
                    tasks.append(
                        asyncio.create_task(
                            self._sync_leg(target, force_fetch_existing=force_fetch_existing)
                        )
                    )
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, dict):
                        actions.append(result)
                    elif isinstance(result, Exception):
                        logger.warning("Protective sync error: %s", result)
            return actions

    async def verify_protective_orders(
        self,
        positions: Iterable[Mapping[str, Any]],
        *,
        force_fetch_existing: bool = True,
    ) -> list[dict[str, Any]]:
        """Verify that protective orders currently match computed targets without placing orders."""
        async with self._lock:
            tasks = []
            actions: list[dict[str, Any]] = []
            grouped = self._group_by_symbol(list(positions))
            for symbol, legs in grouped.items():
                targets = self._compute_targets(symbol, legs)
                for target in targets:
                    tasks.append(
                        asyncio.create_task(
                            self._verify_leg(target, force_fetch_existing=force_fetch_existing)
                        )
                    )
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, dict):
                        actions.append(result)
                    elif isinstance(result, Exception):
                        logger.warning("Protective verify error: %s", result)
            return actions

    async def discover_open_protective_targets(
        self,
        exchanges: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        """Discover symbols with open conditional protection without changing orders."""
        requested = {
            str(exchange or "").strip().lower()
            for exchange in (exchanges or self._gateways.keys())
            if str(exchange or "").strip()
        }
        targets: set[tuple[str, str]] = set()
        errors: list[dict[str, str]] = []
        async with self._lock:
            for exchange in sorted(requested):
                if exchange in self._blocked:
                    continue
                gateway = self._gateways.get(exchange)
                if gateway is None:
                    continue
                try:
                    await gateway.refresh_credentials_async()
                    await gateway.ensure_client()
                    if gateway.client is None:
                        continue
                    orders, fetch_errors = await self._fetch_all_open_protective_orders(gateway)
                    if fetch_errors and not orders:
                        errors.append(
                            {
                                "exchange": exchange,
                                "error": "; ".join(fetch_errors),
                            }
                        )
                    for order in orders:
                        info = order.get("info") or {}
                        if not isinstance(info, Mapping):
                            info = {}
                        otype = str(order.get("type") or info.get("type") or info.get("orderType") or "").lower()
                        stop_px = _safe_float(
                            order.get("stopPrice")
                            or order.get("triggerPrice")
                            or info.get("stopPrice")
                            or info.get("triggerPrice")
                            or info.get("triggerPx")
                            or info.get("slTriggerPx")
                        )
                        take_px = _safe_float(
                            order.get("takeProfitPrice")
                            or info.get("takeProfitPrice")
                            or info.get("tpTriggerPx")
                        )
                        if not self._is_protective_order(
                            exchange,
                            order,
                            info,
                            stop_px,
                            take_px,
                            otype,
                        ):
                            continue
                        reduce_flag = (
                            info.get("reduceOnly")
                            if info.get("reduceOnly") is not None
                            else info.get("reduce_only")
                        )
                        if reduce_flag is None:
                            reduce_flag = order.get("reduceOnly")
                        if (
                            reduce_flag is not None
                            and str(reduce_flag).strip().lower() in {"false", "0", "no"}
                            and exchange != "okx"
                        ):
                            # A conditional entry is not stale protection and
                            # must never be picked up by the orphan sweep.
                            continue
                        symbol = (
                            order.get("symbol")
                            or info.get("symbol")
                            or info.get("instId")
                            or info.get("contract")
                        )
                        symbol_key = self._protective_symbol_key(symbol)
                        if symbol_key:
                            targets.add((exchange, symbol_key))
                except Exception as exc:  # pylint: disable=broad-except
                    errors.append({"exchange": exchange, "error": str(exc)})
        return {
            "targets": [
                {"exchange": exchange, "symbol": symbol}
                for exchange, symbol in sorted(targets)
            ],
            "errors": errors,
        }

    async def _fetch_all_open_protective_orders(
        self,
        gateway: ExchangeGateway,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Fetch regular and conditional open orders for an entire venue."""
        orders: list[dict[str, Any]] = []
        errors: list[str] = []

        async def _fetch(params: Mapping[str, Any] | None = None) -> None:
            try:
                rows = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                    None,
                    params=dict(params or {}),
                )
                orders.extend(row for row in (rows or []) if isinstance(row, dict))
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(str(exc))

        if gateway.slug == "binance":
            options = getattr(gateway.client, "options", None)
            if isinstance(options, dict):
                options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        if gateway.slug == "kucoin":
            await _fetch()
            await _fetch({"stop": True})
        elif gateway.slug == "bybit":
            await _fetch()
            await _fetch({"trigger": True})
            await _fetch({"orderFilter": "tpslOrder"})
        elif gateway.slug == "okx":
            await _fetch()
            await _fetch({"trigger": True})
            await _fetch({"ordType": "conditional"})
        elif gateway.slug == "gate":
            await _fetch()
            await _fetch({"trigger": True})
        elif gateway.slug == "bitget":
            await _fetch(bitget_private_params({}) if bitget_uta_enabled() else {})
            plan_params = (
                bitget_private_params({"trigger": True})
                if bitget_uta_enabled()
                else {"trigger": True, "planType": "profit_loss"}
            )
            await _fetch(plan_params)
        else:
            await _fetch()

        if gateway.slug == "binance":
            try:
                algo_payload = await self._fetch_binance_open_algo_orders(gateway, None)
                for item in algo_payload or []:
                    if not isinstance(item, dict):
                        continue
                    info = dict(item)
                    orders.append(
                        {
                            "id": str(
                                info.get("algoId")
                                or info.get("algoOrderId")
                                or info.get("id")
                                or ""
                            ),
                            "symbol": info.get("symbol"),
                            "type": info.get("orderType") or info.get("type"),
                            "side": str(info.get("side") or "").lower(),
                            "amount": _safe_float(info.get("quantity") or info.get("origQty")),
                            "stopPrice": _safe_float(
                                info.get("triggerPrice") or info.get("stopPrice")
                            ),
                            "reduceOnly": info.get("reduceOnly"),
                            "info": info,
                        }
                    )
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(str(exc))

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for order in orders:
            info = order.get("info") or {}
            order_id = str(
                order.get("id")
                or (info.get("algoId") if isinstance(info, Mapping) else "")
                or ""
            )
            symbol = str(order.get("symbol") or "")
            key = (order_id, symbol)
            if order_id and key in seen:
                continue
            if order_id:
                seen.add(key)
            deduped.append(order)
        return deduped, errors

    async def cleanup_orphaned_protective_orders(
        self,
        exchange: str,
        symbol: str,
        *,
        sides: Iterable[str] = ("long", "short"),
        force_fetch_existing: bool = True,
    ) -> list[dict[str, Any]]:
        """Cancel known protective orders for a symbol after confirming no position remains."""
        normalized_exchange = str(exchange or "").strip().lower()
        normalized_symbol = normalize_symbol(symbol)
        async with self._lock:
            gw = self._gateways.get(normalized_exchange)
            seen_cancel_ids: set[str] = set()
            actions: list[dict[str, Any]] = []
            for raw_side in sides:
                side = str(raw_side or "").strip().lower()
                if side not in {"long", "short"}:
                    continue
                target = ProtectiveTarget(
                    stop=None,
                    takes=[],
                    quantity=0.0,
                    side=side,
                    exchange=normalized_exchange,
                    symbol=normalized_symbol,
                    position_id=None,
                )
                actions.append(
                    await self._cleanup_orphaned_leg(
                        gw,
                        target,
                        seen_cancel_ids=seen_cancel_ids,
                        force_fetch_existing=force_fetch_existing,
                    )
                )
            return actions

    async def _cleanup_orphaned_leg(
        self,
        gw: ExchangeGateway | None,
        target: ProtectiveTarget,
        *,
        seen_cancel_ids: set[str],
        force_fetch_existing: bool = True,
    ) -> dict[str, Any]:
        actions: dict[str, Any] = {
            "exchange": target.exchange,
            "symbol": target.symbol,
            "side": target.side,
            "status": "cleanup_skipped",
            "reason": "adapter_missing",
        }
        if gw is None:
            return actions
        try:
            await gw.refresh_credentials_async()
            await gw.ensure_client()
            if not gw.client:
                actions["reason"] = "no_client"
                return actions
            existing = await self._fetch_existing(
                gw,
                target.symbol,
                target.position_id,
                target.side,
                mark_price=None,
                entry_price=None,
                force_fetch=force_fetch_existing,
            )
            to_cancel = [
                str(order_id)
                for order_id in (existing.get("order_ids") or [])
                if order_id and str(order_id) not in seen_cancel_ids
            ]
            seen_cancel_ids.update(to_cancel)
            actions.update(
                {
                    "existing": existing,
                    "cancel_order_ids": to_cancel,
                    "status": "cleanup_skipped_no_orders",
                    "reason": "no_protective_orders",
                }
            )
            if not to_cancel:
                return actions
            cancel_failures = await self._cancel_protective_order_ids(gw, target, to_cancel, existing)
            if cancel_failures:
                actions["status"] = "cleanup_cancel_failed"
                actions["error"] = f"cancel_failed:{','.join(cancel_failures)}"
                actions["cancel_failures"] = cancel_failures
                return actions
            existing_after_cancel = await self._fetch_existing(
                gw,
                target.symbol,
                target.position_id,
                target.side,
                mark_price=None,
                entry_price=None,
                force_fetch=True,
            )
            active_ids = {
                str(order_id)
                for order_id in (existing_after_cancel.get("order_ids") or [])
                if order_id
            }
            lingering_cancel_ids = [oid for oid in to_cancel if oid in active_ids]
            actions["existing_after_cancel"] = existing_after_cancel
            if lingering_cancel_ids:
                actions["status"] = "cleanup_cancel_pending"
                actions["error"] = f"cancel_pending:{','.join(lingering_cancel_ids)}"
                actions["cancel_pending_ids"] = lingering_cancel_ids
            else:
                actions["status"] = "cleanup_cancelled"
                actions.pop("reason", None)
            return actions
        except Exception as exc:  # pylint: disable=broad-except
            actions["status"] = "cleanup_error"
            actions["error"] = str(exc)
            logger.warning(
                "Protective cleanup failed for %s %s %s: %s",
                target.exchange,
                target.symbol,
                target.side,
                exc,
                exc_info=True,
            )
            return actions
        finally:
            if gw and gw.requires_cycle_close():
                try:
                    await gw.close()
                except Exception:  # pragma: no cover - defensive
                    pass

    async def _cancel_protective_order_ids(
        self,
        gw: ExchangeGateway,
        target: ProtectiveTarget,
        order_ids: Iterable[str],
        existing: Mapping[str, Any],
    ) -> list[str]:
        cancel_failures: list[str] = []
        algo_ids = set(existing.get("algo_order_ids") or [])
        exchange_symbol = await self._resolve_ccxt_symbol(gw, target.symbol)
        for oid in order_ids:
            try:
                cancel_params = {}
                if gw.slug == "kucoin":
                    cancel_params["stop"] = True
                if gw.slug == "gate":
                    cancel_params["trigger"] = True
                if gw.slug == "bitget":
                    if bitget_uta_enabled():
                        cancel_params = bitget_private_params({"trigger": True})
                    else:
                        cancel_params["trigger"] = True
                        cancel_params["planType"] = "profit_loss"
                if gw.slug == "binance" and oid in algo_ids:
                    await self._call_with_time_sync_retry(
                        gw,
                        "cancel_binance_algo_order",
                        lambda: self._cancel_binance_algo_order(gw, algo_id=oid),
                    )
                    continue
                if gw.slug == "okx" and oid in algo_ids:
                    cancel_params["trigger"] = True
                await self._call_with_time_sync_retry(
                    gw,
                    "cancel_order",
                    lambda: gw.client.cancel_order(oid, exchange_symbol, params=cancel_params),
                )
            except Exception as exc:  # pragma: no cover - defensive
                message = str(exc)
                if self._cancel_already_resolved(target.exchange, message):
                    logger.info(
                        "%s cancel already resolved for %s: %s",
                        target.exchange,
                        target.symbol,
                        oid,
                    )
                    continue
                cancel_failures.append(str(oid))
                logger.warning(
                    "%s cancel %s failed for %s: %s",
                    target.exchange,
                    oid,
                    target.symbol,
                    exc,
                )
        return cancel_failures

    def _group_by_symbol(self, positions: List[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for leg in positions:
            symbol = normalize_symbol(leg.get("symbol") or leg.get("symbol_normalized"))
            if not symbol:
                continue
            grouped.setdefault(symbol, []).append(leg)
        return grouped

    def _compute_targets(self, symbol: str, legs: list[Mapping[str, Any]]) -> list[ProtectiveTarget]:
        if not legs:
            return []

        def _extract_pos_side(leg: Mapping[str, Any]) -> str | None:
            raw = leg.get("raw") or {}
            info = raw.get("info") if isinstance(raw, dict) else {}
            candidates = [
                leg.get("posSide"),
                leg.get("positionSide"),
                raw.get("posSide") if isinstance(raw, dict) else None,
                raw.get("positionSide") if isinstance(raw, dict) else None,
                info.get("posSide") if isinstance(info, dict) else None,
                info.get("positionSide") if isinstance(info, dict) else None,
            ]
            for value in candidates:
                if not value:
                    continue
                text = str(value).strip().lower()
                if text in ("net", "long", "short"):
                    return text
            return None

        def _leg_qty(leg: Mapping[str, Any]) -> float:
            qty = _safe_float(leg.get("contracts"))
            if qty is None or abs(qty) < 1e-12:
                qty = _safe_float(leg.get("coin_qty")) or _safe_float(leg.get("amount"))
            return abs(qty or 0.0)

        def _weighted_avg(pairs: list[tuple[float, float]]) -> float | None:
            total_qty = sum(q for _, q in pairs)
            if total_qty <= 0:
                return None
            return sum(price * q for price, q in pairs) / total_qty

        long_infos: list[dict[str, Any]] = []
        short_infos: list[dict[str, Any]] = []
        for leg in legs:
            side = (leg.get("side") or "").lower()
            qty = _leg_qty(leg)
            if qty <= 0:
                continue
            mark = _safe_float(leg.get("mark_price"))
            entry = _safe_float(leg.get("entry_price"))
            stop = self._target_stop_price(
                side,
                _safe_float(leg.get("liquidation_price")),
                mark_price=mark,
                entry_price=entry,
            )
            info = {"leg": leg, "qty": qty, "mark": mark, "entry": entry, "stop": stop}
            if side == "long":
                long_infos.append(info)
            elif side == "short":
                short_infos.append(info)

        avg_long_mark = _weighted_avg(
            [(info["mark"], info["qty"]) for info in long_infos if info.get("mark")]
        )
        avg_short_mark = _weighted_avg(
            [(info["mark"], info["qty"]) for info in short_infos if info.get("mark")]
        )

        def _build_take_targets(
            leg_info: Mapping[str, Any],
            opposite_infos: list[Mapping[str, Any]],
            *,
            avg_mark: float | None,
            avg_opposite_mark: float | None,
            side: str,
        ) -> list[TakeTarget]:
            qty = float(leg_info.get("qty") or 0.0)
            if qty <= 0:
                return []
            leg_reference = (
                _safe_float(leg_info.get("mark"))
                or _safe_float(leg_info.get("entry"))
                or avg_mark
            )

            def _valid_take(price: float | None) -> bool:
                if price is None or not math.isfinite(price) or price <= 0:
                    return False
                if not leg_reference or leg_reference <= 0:
                    return True
                min_gap = max(1e-8, float(leg_reference) * 0.0001)
                if side == "long":
                    return price >= float(leg_reference) + min_gap
                return price <= float(leg_reference) - min_gap

            candidates: list[tuple[float, float]] = []
            for opp in opposite_infos:
                opp_qty = float(opp.get("qty") or 0.0)
                if opp_qty <= 0:
                    continue
                opp_stop = _safe_float(opp.get("stop"))
                if not opp_stop or opp_stop <= 0:
                    continue
                opp_mark = _safe_float(opp.get("mark")) or avg_opposite_mark
                leg_mark = _safe_float(leg_info.get("mark")) or avg_mark
                if not opp_mark or not leg_mark or opp_mark <= 0 or leg_mark <= 0:
                    continue
                ratio = leg_mark / opp_mark
                if ratio <= 0:
                    continue
                candidate_price = opp_stop * ratio
                if _valid_take(candidate_price):
                    candidates.append((candidate_price, opp_qty))
            if not candidates:
                fallback = self._fallback_take_price(
                    side,
                    mark_price=leg_info.get("mark"),
                    entry_price=leg_info.get("entry"),
                    ratio=float(self._risk_config.fallback_take_rr_pct),
                )
                if _valid_take(fallback):
                    return [TakeTarget(price=fallback, quantity=qty)]
                return []
            total_weight = sum(weight for _, weight in candidates)
            if total_weight <= 0:
                return []
            takes: list[TakeTarget] = []
            allocated = 0.0
            for idx, (price, weight) in enumerate(candidates):
                if idx == len(candidates) - 1:
                    qty_part = max(0.0, qty - allocated)
                else:
                    qty_part = qty * (weight / total_weight)
                    allocated += qty_part
                if qty_part <= 0:
                    continue
                takes.append(TakeTarget(price=price, quantity=qty_part))
            return takes

        targets: list[ProtectiveTarget] = []
        for info in long_infos:
            leg = info["leg"]
            takes = _build_take_targets(
                info,
                short_infos,
                avg_mark=avg_long_mark,
                avg_opposite_mark=avg_short_mark,
                side="long",
            )
            targets.append(
                ProtectiveTarget(
                    stop=_safe_float(info.get("stop")),
                    takes=takes,
                    quantity=float(info.get("qty") or 0.0),
                    side="long",
                    exchange=str(leg.get("exchange") or "").lower(),
                    symbol=str(leg.get("symbol") or ""),
                    position_id=str(leg.get("position_id") or leg.get("id") or "") or None,
                    margin_mode=str(leg.get("marginMode") or leg.get("margin_mode") or leg.get("marginType") or leg.get("margin_type") or "").lower() or None,
                    pos_side=_extract_pos_side(leg),
                    mark_price=_safe_float(info.get("mark")),
                    entry_price=_safe_float(info.get("entry")),
                )
            )
        for info in short_infos:
            leg = info["leg"]
            takes = _build_take_targets(
                info,
                long_infos,
                avg_mark=avg_short_mark,
                avg_opposite_mark=avg_long_mark,
                side="short",
            )
            targets.append(
                ProtectiveTarget(
                    stop=_safe_float(info.get("stop")),
                    takes=takes,
                    quantity=float(info.get("qty") or 0.0),
                    side="short",
                    exchange=str(leg.get("exchange") or "").lower(),
                    symbol=str(leg.get("symbol") or ""),
                    position_id=str(leg.get("position_id") or leg.get("id") or "") or None,
                    margin_mode=str(leg.get("marginMode") or leg.get("margin_mode") or leg.get("marginType") or leg.get("margin_type") or "").lower() or None,
                    pos_side=_extract_pos_side(leg),
                    mark_price=_safe_float(info.get("mark")),
                    entry_price=_safe_float(info.get("entry")),
                )
            )
        return targets

    def _target_stop_price(
        self,
        side: str,
        liq_price: float | None,
        *,
        mark_price: float | None = None,
        entry_price: float | None = None,
    ) -> float | None:
        base_liq = None
        if liq_price is not None and liq_price > 0:
            base_liq = liq_price
        else:
            fallback = mark_price or entry_price
            if fallback is None or fallback <= 0:
                return None
            base_liq = fallback * (
                self._risk_config.fallback_liq_factor_long if side == "long" else self._risk_config.fallback_liq_factor_short
            )
        gap = max(0.0, float(self._risk_config.stop_gap_from_liq_pct))
        if gap <= 0:
            return None
        if side == "short":
            return base_liq * max(0.0001, (1.0 - gap))
        return base_liq * (1.0 + gap)

    async def _sync_leg(
        self,
        target: ProtectiveTarget,
        *,
        force_fetch_existing: bool = False,
    ) -> dict[str, Any]:
        gw = self._gateways.get(target.exchange)
        threshold = max(0.0, float(self._risk_config.stop_requote_threshold_pct))
        qty_threshold = max(0.01, threshold)
        max_stop_age_sec = max(0, int(getattr(self._risk_config, "stop_force_requote_max_age_sec", 120) or 0))
        if gw is None:
            return {"exchange": target.exchange, "symbol": target.symbol, "status": "skipped", "reason": "adapter_missing"}
        if target.quantity <= 0:
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "status": "skipped",
                "reason": "zero_quantity",
            }
        if target.stop is None and not target.takes:
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "status": "skipped",
                "reason": "no_targets",
            }
        if target.exchange in self._blocked:
            existing: dict[str, Any] = {"order_ids": []}
            try:
                await gw.refresh_credentials_async()
                await gw.ensure_client()
                if gw.client:
                    existing = await self._fetch_existing(
                        gw,
                        target.symbol,
                        target.position_id,
                        target.side,
                        mark_price=target.mark_price,
                        entry_price=target.entry_price,
                        force_fetch=force_fetch_existing,
                    )
            except Exception:  # pylint: disable=broad-except
                existing = {"order_ids": [], "error": "existing_fetch_failed"}
            stop_diff, stop_delta = self._needs_stop_update(
                existing.get("stop_orders") or [],
                target.stop,
                target.quantity,
                threshold,
                qty_threshold,
                max_age_sec=max_stop_age_sec,
            )
            stop_age = None
            stop_stale = False
            blocked_stop_orders = existing.get("stop_orders") or []
            if max_stop_age_sec > 0 and len(blocked_stop_orders) == 1:
                stop_age = self._order_age_sec(blocked_stop_orders[0])
                stop_stale = stop_age is not None and stop_age > float(max_stop_age_sec)
                if stop_stale:
                    protective_logger.warning(
                        "stale stop detected exchange=%s symbol=%s side=%s age_sec=%.1f max_age_sec=%s",
                        target.exchange,
                        target.symbol,
                        target.side,
                        stop_age,
                        max_stop_age_sec,
                    )
            take_diff, take_delta = self._needs_take_update(
                existing.get("take_orders") or [],
                target.takes,
                threshold,
                qty_threshold,
            )
            status = "blocked_ok"
            reason = "exchange_blocked"
            if target.stop and not (existing.get("stop_orders") or []):
                status = "blocked_missing_stop"
                reason = "stop_missing"
            elif target.stop and stop_diff:
                status = "blocked_bad_stop"
                reason = "stop_mismatch"
            missing_parts = []
            if not (existing.get("stop_orders") or []):
                missing_parts.append("stop")
            if target.takes and not (existing.get("take_orders") or []):
                missing_parts.append("take")
            actions: dict[str, Any] = {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "position_id": target.position_id,
                "side": target.side,
                "quantity": target.quantity,
                "existing": existing,
                "target_stop": target.stop,
                "target_take": self._summarize_takes(target.takes),
                "target_takes": [{"price": t.price, "qty": t.quantity} for t in target.takes],
                "status": status,
                "reason": reason,
                "delta_stop": stop_delta,
                "delta_take": take_delta,
                "stop_age_sec": stop_age,
                "stop_stale": stop_stale,
            }
            protective_logger.info(
                "protective action %s %s %s stop=%s take=%s qty=%s status=%s error=%s d_stop=%s d_take=%s",
                target.exchange,
                target.symbol,
                target.side,
                actions.get("target_stop"),
                actions.get("target_take"),
                target.quantity,
                actions.get("status"),
                actions.get("error"),
                None,
                None,
            )
            if missing_parts and str(target.exchange).lower() != "mexc":
                await self._send_telegram_warning(
                    f"[PROTECTIVE] {target.exchange} missing {','.join(missing_parts)} for {target.symbol} "
                    f"side={target.side} qty={target.quantity} stop={target.stop} take={self._summarize_takes(target.takes)}",
                    key=f"{target.exchange}:{normalize_symbol(target.symbol)}:{target.side}:missing"
                )
            if gw and gw.requires_cycle_close():
                try:
                    await gw.close()
                except Exception:  # pragma: no cover - defensive
                    pass
            return actions
        await gw.refresh_credentials_async()
        await gw.ensure_client()
        if not gw.client:
            return {"exchange": target.exchange, "symbol": target.symbol, "status": "skipped", "reason": "no_client"}
        existing = await self._fetch_existing(
            gw,
            target.symbol,
            target.position_id,
            target.side,
            mark_price=target.mark_price,
            entry_price=target.entry_price,
            force_fetch=force_fetch_existing,
        )
        actions: dict[str, Any] = {
            "exchange": target.exchange,
            "symbol": target.symbol,
            "position_id": target.position_id,
            "side": target.side,
            "existing": existing,
            "target_stop": target.stop,
            "target_take": self._summarize_takes(target.takes),
            "target_takes": [{"price": t.price, "qty": t.quantity} for t in target.takes],
            "status": "noop",
        }
        to_cancel: list[str] = []
        existing_stop_orders = existing.get("stop_orders") or []
        existing_take_orders = existing.get("take_orders") or []
        existing_unknown_orders = existing.get("unknown_orders") or []
        stop_diff, stop_delta = self._needs_stop_update(
            existing_stop_orders,
            target.stop,
            target.quantity,
            threshold,
            qty_threshold,
            max_age_sec=max_stop_age_sec,
        )
        stop_age = None
        stop_stale = False
        if max_stop_age_sec > 0 and len(existing_stop_orders) == 1:
            stop_age = self._order_age_sec(existing_stop_orders[0])
            stop_stale = stop_age is not None and stop_age > float(max_stop_age_sec)
            if stop_stale:
                protective_logger.warning(
                    "stale stop detected exchange=%s symbol=%s side=%s age_sec=%.1f max_age_sec=%s",
                    target.exchange,
                    target.symbol,
                    target.side,
                    stop_age,
                    max_stop_age_sec,
                )
        take_diff, take_delta = self._needs_take_update(
            existing_take_orders,
            target.takes,
            threshold,
            qty_threshold,
        )
        actions["stop_age_sec"] = stop_age
        actions["stop_stale"] = stop_stale

        invalid_side = bool(existing.get("invalid_side"))
        if invalid_side:
            to_cancel = [oid for oid in existing.get("order_ids", [])]
        else:
            stop_ids = [str(item.get("id") or "") for item in existing_stop_orders if item.get("id")]
            take_ids = [str(item.get("id") or "") for item in existing_take_orders if item.get("id")]
            unknown_ids = [str(item.get("id") or "") for item in existing_unknown_orders if item.get("id")]
            if stop_diff:
                to_cancel.extend(stop_ids)
                to_cancel.extend(unknown_ids)
            if take_diff:
                to_cancel.extend(take_ids)
                to_cancel.extend(unknown_ids)
            if to_cancel:
                # preserve order but ensure uniqueness
                seen = set()
                to_cancel = [oid for oid in to_cancel if not (oid in seen or seen.add(oid))]

        take_skipped_min = False
        stop_skipped_invalid = False
        take_skipped_invalid = False
        place_stop_required = bool(target.stop and (stop_diff or invalid_side))
        place_take_required = bool(target.takes and (take_diff or invalid_side))
        try:
            cancel_failures = await self._cancel_protective_order_ids(gw, target, to_cancel, existing) if to_cancel else []
            if cancel_failures:
                actions["status"] = "cancel_failed"
                actions["error"] = f"cancel_failed:{','.join(cancel_failures)}"
                actions["cancel_failures"] = cancel_failures
            else:
                lingering_cancel_ids: list[str] = []
                if to_cancel:
                    existing_after_cancel = await self._fetch_existing(
                        gw,
                        target.symbol,
                        target.position_id,
                        target.side,
                        mark_price=target.mark_price,
                        entry_price=target.entry_price,
                        force_fetch=True,
                    )
                    active_ids = {
                        str(order_id)
                        for order_id in (existing_after_cancel.get("order_ids") or [])
                        if order_id
                    }
                    lingering_cancel_ids = [oid for oid in to_cancel if oid in active_ids]
                    if lingering_cancel_ids:
                        actions["status"] = "cancel_pending"
                        actions["error"] = f"cancel_pending:{','.join(lingering_cancel_ids)}"
                        actions["cancel_pending_ids"] = lingering_cancel_ids
                        actions["existing_after_cancel"] = existing_after_cancel
                        logger.warning(
                            "%s cancel not confirmed for %s %s: %s",
                            target.exchange,
                            target.symbol,
                            target.side,
                            ",".join(lingering_cancel_ids),
                        )
                if not lingering_cancel_ids:
                    if place_stop_required:
                        try:
                            await self._place_stop(gw, target, target.stop)
                        except InvalidProtectivePrice as exc:
                            stop_skipped_invalid = True
                            actions["error"] = str(exc)
                            logger.info(
                                "Skipping stop placement (invalid price) for %s %s: %s",
                                target.exchange,
                                target.symbol,
                                exc,
                            )
                    if place_take_required:
                        for take in target.takes:
                            if take.quantity <= 0:
                                continue
                            try:
                                await self._place_take(gw, target, take.price, quantity=take.quantity)
                            except InvalidProtectivePrice as exc:
                                take_skipped_invalid = True
                                actions["error"] = str(exc)
                                logger.info(
                                    "Skipping take placement (invalid price) for %s %s: %s",
                                    target.exchange,
                                    target.symbol,
                                    exc,
                                )
                            except Exception as exc:  # pylint: disable=broad-except
                                msg = str(exc)
                                # Bybit enforces TP >=10% of position size; degrade gracefully.
                                if "10_pcnt" in msg or "PartialTakeProfit" in msg:
                                    take_skipped_min = True
                                    actions["error"] = msg
                                    logger.info(
                                        "Skipping take placement (min-size) for %s %s: %s",
                                        target.exchange,
                                        target.symbol,
                                        msg,
                                    )
                                else:
                                    raise
                    if stop_skipped_invalid:
                        actions["status"] = "stop_skipped_invalid_price"
                    elif take_skipped_invalid:
                        actions["status"] = "take_skipped_invalid_price"
                    elif take_skipped_min:
                        actions["status"] = "take_skipped_min_size"
                    else:
                        actions["status"] = "updated" if (place_stop_required or place_take_required) else "unchanged"
        except Exception as exc:  # pylint: disable=broad-except
            if RequestTimeout and isinstance(exc, RequestTimeout):
                actions["status"] = "timeout"
                actions["error"] = str(exc)
                logger.info("Protective order sync timeout for %s %s: %s", target.exchange, target.symbol, exc)
            else:
                msg = str(exc)
                is_no_position = False
                if target.exchange == "bingx" and ("110424" in msg or "available amount of 0" in msg):
                    is_no_position = True
                if target.exchange == "bitget" and ("22002" in msg or "No position to close" in msg):
                    is_no_position = True
                if target.exchange == "bybit" and "zero position" in msg.lower():
                    is_no_position = True
                if is_no_position:
                    actions["status"] = "skipped_no_position"
                    actions["error"] = msg
                    logger.info(
                        "Protective order skipped (no position) for %s %s: %s",
                        target.exchange,
                        target.symbol,
                        msg,
                    )
                else:
                    actions["status"] = "error"
                    actions["error"] = msg
                    logger.warning(
                        "Protective order sync failed for %s %s: %s",
                        target.exchange,
                        target.symbol,
                        msg,
                        exc_info=True,
                    )
        finally:
            if gw and gw.requires_cycle_close():
                try:
                    await gw.close()
                except Exception:  # pragma: no cover - defensive
                    pass
        status = actions.get("status")
        err_msg = actions.get("error") or actions.get("reason")
        ok_states = {"updated", "unchanged", "blocked_ok"}
        if status not in ok_states:
            protective_logger.warning(
                "protective issue exchange=%s symbol=%s side=%s status=%s stop=%s take=%s err=%s",
                target.exchange,
                target.symbol,
                target.side,
                status,
                actions.get("target_stop"),
                actions.get("target_take"),
                err_msg,
            )
        return actions

    async def _verify_leg(
        self,
        target: ProtectiveTarget,
        *,
        force_fetch_existing: bool = True,
    ) -> dict[str, Any]:
        gw = self._gateways.get(target.exchange)
        threshold = max(0.0, float(self._risk_config.stop_requote_threshold_pct))
        qty_threshold = max(0.01, threshold)
        max_stop_age_sec = max(0, int(getattr(self._risk_config, "stop_force_requote_max_age_sec", 120) or 0))
        if gw is None:
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "side": target.side,
                "status": "skipped",
                "reason": "adapter_missing",
            }
        if target.quantity <= 0:
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "side": target.side,
                "status": "skipped",
                "reason": "zero_quantity",
            }
        if target.stop is None and not target.takes:
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "side": target.side,
                "status": "skipped",
                "reason": "no_targets",
            }
        try:
            await gw.refresh_credentials_async()
            await gw.ensure_client()
        except Exception as exc:  # pylint: disable=broad-except
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "side": target.side,
                "status": "error",
                "reason": f"client_init_failed: {exc}",
            }
        if not gw.client:
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "side": target.side,
                "status": "skipped",
                "reason": "no_client",
            }
        existing = await self._fetch_existing(
            gw,
            target.symbol,
            target.position_id,
            target.side,
            mark_price=target.mark_price,
            entry_price=target.entry_price,
            force_fetch=force_fetch_existing,
        )
        existing_stop_orders = existing.get("stop_orders") or []
        existing_take_orders = existing.get("take_orders") or []
        stop_diff, stop_delta = self._needs_stop_update(
            existing_stop_orders,
            target.stop,
            target.quantity,
            threshold,
            qty_threshold,
            max_age_sec=max_stop_age_sec,
        )
        take_diff, take_delta = self._needs_take_update(
            existing_take_orders,
            target.takes,
            threshold,
            qty_threshold,
        )
        missing_parts = []
        if target.stop and not existing_stop_orders:
            missing_parts.append("stop")
        if target.takes and not existing_take_orders:
            missing_parts.append("take")
        status = "ok"
        reasons: list[str] = []
        if bool(existing.get("invalid_side")):
            status = "issue"
            reasons.append("invalid_side")
        if missing_parts:
            status = "issue"
            reasons.append(f"missing:{','.join(missing_parts)}")
        if stop_diff:
            status = "issue"
            reasons.append("stop_mismatch")
        if take_diff:
            status = "issue"
            reasons.append("take_mismatch")
        if not reasons:
            reasons.append("match")
        return {
            "exchange": target.exchange,
            "symbol": target.symbol,
            "position_id": target.position_id,
            "side": target.side,
            "status": status,
            "reason": ";".join(reasons),
            "existing": existing,
            "target_stop": target.stop,
            "target_take": self._summarize_takes(target.takes),
            "target_takes": [{"price": t.price, "qty": t.quantity} for t in target.takes],
            "delta_stop": stop_delta,
            "delta_take": take_delta,
        }

    async def _resolve_ccxt_symbol(
        self,
        gateway: ExchangeGateway,
        symbol: str,
    ) -> str:
        """Resolve an internal or venue id to the unified symbol CCXT expects."""
        if "/" in str(symbol or ""):
            return symbol
        mapped = gateway.map_symbol(symbol) or symbol
        if "/" in str(mapped):
            return str(mapped)
        client = gateway.client
        markets: Mapping[str, Any] = {}
        if client is not None:
            try:
                loaded = await client.load_markets()
                if isinstance(loaded, Mapping):
                    markets = loaded
            except Exception:  # pragma: no cover - falls back to cached markets
                cached = getattr(client, "markets", None)
                if isinstance(cached, Mapping):
                    markets = cached
        target_key = self._protective_symbol_key(symbol)
        for market in markets.values():
            if not isinstance(market, Mapping):
                continue
            unified = str(market.get("symbol") or "")
            venue_id = str(market.get("id") or "")
            if target_key in {
                self._protective_symbol_key(unified),
                self._protective_symbol_key(venue_id),
            }:
                return unified or str(mapped)
        return str(mapped)

    async def _fetch_existing(
        self,
        gateway: ExchangeGateway,
        symbol: str,
        position_id: str | None,
        side: str | None,
        *,
        mark_price: float | None = None,
        entry_price: float | None = None,
        force_fetch: bool = False,
    ) -> dict[str, Any]:
        cache_key = self._existing_cache_key(
            gateway.slug,
            symbol,
            position_id,
            side,
            mark_price,
            entry_price,
        )
        if not force_fetch:
            cached = self._existing_cache.get(cache_key)
            if cached:
                payload, ts = cached
                if time.time() - ts <= self._existing_cache_ttl:
                    return self._clone_existing(payload)
        exchange_symbol = await self._resolve_ccxt_symbol(gateway, symbol)
        orders: list[dict[str, Any]] = []
        try:
            # KuCoin stop orders live in a separate endpoint; fetch both.
            if gateway.slug == "kucoin":
                default_orders = await gateway.client.fetch_open_orders(exchange_symbol)  # type: ignore[union-attr]
                stop_orders = await gateway.client.fetch_open_orders(exchange_symbol, params={"stop": True})  # type: ignore[union-attr]
                orders = (default_orders or []) + (stop_orders or [])
            elif gateway.slug == "binance":
                orders = []
                try:
                    orders += await gateway.client.fetch_open_orders(exchange_symbol)  # type: ignore[union-attr]
                except Exception:
                    pass
                try:
                    algo_payload = await self._fetch_binance_open_algo_orders(gateway, symbol)
                    for item in algo_payload or []:
                        if not isinstance(item, dict):
                            continue
                        info = dict(item)
                        algo_id = str(
                            info.get("algoId")
                            or info.get("algoOrderId")
                            or info.get("id")
                            or info.get("clientAlgoId")
                            or ""
                        )
                        if not algo_id:
                            continue
                        if "algoId" not in info:
                            info["algoId"] = algo_id
                        order_type = str(info.get("orderType") or info.get("type") or "").lower()
                        algo_side = str(info.get("side") or "").lower()
                        qty = _safe_float(
                            info.get("quantity") or info.get("origQty") or info.get("amount")
                        )
                        trigger = _safe_float(
                            info.get("triggerPrice")
                            or info.get("stopPrice")
                            or info.get("triggerPx")
                        )
                        orders.append(
                            {
                                "id": algo_id,
                                "type": order_type,
                                "side": algo_side,
                                "amount": qty,
                                "stopPrice": trigger,
                                "reduceOnly": info.get("reduceOnly"),
                                "info": info,
                            }
                        )
                except Exception:
                    pass
            elif gateway.slug == "okx":
                default_orders = await gateway.client.fetch_open_orders(exchange_symbol)  # type: ignore[union-attr]
                trigger_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                    exchange_symbol,
                    params={"trigger": True},
                )
                conditional_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                    exchange_symbol,
                    params={"ordType": "conditional"},
                )
                orders = (default_orders or []) + (trigger_orders or []) + (conditional_orders or [])
            elif gateway.slug == "gate":
                default_orders = await gateway.client.fetch_open_orders(exchange_symbol)  # type: ignore[union-attr]
                trigger_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                    exchange_symbol,
                    params={"trigger": True},
                )
                orders = (default_orders or []) + (trigger_orders or [])
            elif gateway.slug == "bitget":
                default_orders = []
                tpsl_orders = []
                try:
                    default_params = bitget_private_params({}) if bitget_uta_enabled() else {}
                    default_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                        exchange_symbol,
                        params=default_params,
                    )
                except Exception:
                    pass
                try:
                    plan_params = (
                        bitget_private_params({"trigger": True})
                        if bitget_uta_enabled()
                        else {"trigger": True, "planType": "profit_loss"}
                    )
                    tpsl_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                        exchange_symbol,
                        params=plan_params,
                    )
                except Exception:
                    pass
                orders = (default_orders or []) + (tpsl_orders or [])
            elif gateway.slug == "bybit":
                orders = []
                try:
                    orders += await gateway.client.fetch_open_orders(exchange_symbol)  # type: ignore[union-attr]
                except Exception:
                    pass
                try:
                    orders += await gateway.client.fetch_open_orders(exchange_symbol, params={"trigger": True})  # type: ignore[union-attr]
                except Exception:
                    pass
                try:
                    orders += await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                        exchange_symbol,
                        params={"orderFilter": "tpslOrder"},
                    )
                except Exception:
                    pass
            else:
                orders = await gateway.client.fetch_open_orders(exchange_symbol)  # type: ignore[union-attr]
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("%s fetch_open_orders failed for %s: %s", gateway.slug, symbol, exc)
            return {"order_ids": []}
        # de-duplicate by id for any exchange
        if orders:
            deduped = []
            seen: set[str] = set()
            for item in orders:
                if not isinstance(item, dict):
                    continue
                oid = str(item.get("id") or "")
                if oid and oid in seen:
                    continue
                seen.add(oid)
                deduped.append(item)
            orders = deduped
        order_ids: list[str] = []
        algo_order_ids: list[str] = []
        stop_orders: list[dict[str, Any]] = []
        take_orders: list[dict[str, Any]] = []
        unknown_orders: list[dict[str, Any]] = []
        invalid_side = False
        expected_close_side = "sell" if (side or "").lower() == "long" else "buy"
        for order in orders or []:
            if not isinstance(order, dict):
                continue
            info = order.get("info") or {}
            if not isinstance(info, dict):
                info = {}
            if not self._order_matches_symbol(order, info, symbol):
                continue
            otype = str(order.get("type") or info.get("type") or "").lower()
            trigger_info = info.get("trigger")
            if not isinstance(trigger_info, dict):
                trigger_info = {}
            def _coerce_dict(value: object) -> dict[str, Any] | None:
                if isinstance(value, dict):
                    return value
                if isinstance(value, str):
                    text = value.strip()
                    if text.startswith("{") and text.endswith("}"):
                        try:
                            parsed = json.loads(text)
                        except Exception:
                            return None
                        if isinstance(parsed, dict):
                            return parsed
                return None

            def _extract_trigger_price(payload: dict[str, Any]) -> float | None:
                return _safe_float(
                    payload.get("stopPrice")
                    or payload.get("triggerPrice")
                    or payload.get("stopLossPrice")
                    or payload.get("takeProfitPrice")
                    or payload.get("stopLoss")
                    or payload.get("takeProfit")
                    or payload.get("price")
                )
            stop_px = _safe_float(
                order.get("stopLossPrice")
                or order.get("stopLoss")
                or info.get("stopLossPrice")
                or info.get("stopLoss")
                or info.get("stopLossEntrustPrice")
                or info.get("slTriggerPx")
                or order.get("stopPrice")
                or order.get("triggerPrice")
                or info.get("stopPrice")
                or info.get("triggerPrice")
                or info.get("triggerPx")
                or trigger_info.get("price")
                or trigger_info.get("trigger_price")
            )
            if stop_px is None:
                stop_payload = _coerce_dict(order.get("stopLoss")) or _coerce_dict(info.get("stopLoss"))
                if stop_payload:
                    stop_px = _extract_trigger_price(stop_payload)
            take_info = info.get("takeProfit")
            if not isinstance(take_info, dict):
                take_info = {}
            take_px = _safe_float(
                order.get("takeProfitPrice")
                or order.get("takeProfit")
                or info.get("takeProfitPrice")
                or info.get("takeProfitEntrustPrice")
                or info.get("tpTriggerPx")
            )
            if take_px is None:
                take_px = _safe_float(order.get("takeProfit") or info.get("takeProfit"))
            if take_px is None:
                take_px = _safe_float(take_info.get("stopPrice"))
            if take_px is None:
                take_payload = _coerce_dict(order.get("takeProfit")) or _coerce_dict(info.get("takeProfit"))
                if take_payload:
                    take_px = _extract_trigger_price(take_payload)
            if gateway.slug == "bitget":
                plan_type = str(info.get("planType") or "").lower()
                if "profit" in plan_type:
                    if take_px is None and stop_px is not None:
                        take_px = stop_px
                    stop_px = None
                elif "loss" in plan_type:
                    if stop_px is None and take_px is not None:
                        stop_px = take_px
                    take_px = None
            if gateway.slug == "bybit":
                stop_order_type = str(info.get("stopOrderType") or "").lower()
                if stop_order_type == "takeprofit":
                    take_px = take_px or stop_px
                    stop_px = None
                elif stop_order_type == "stoploss":
                    stop_px = stop_px or take_px
                    take_px = None
            reduce_flag = (
                info.get("reduceOnly")
                if info.get("reduceOnly") is not None
                else info.get("reduce_only")
            )
            if reduce_flag is None:
                reduce_flag = order.get("reduceOnly")
            is_protective = self._is_protective_order(gateway.slug, order, info, stop_px, take_px, otype)
            if not is_protective:
                continue
            if (
                reduce_flag is not None
                and str(reduce_flag).strip().lower() in {"false", "0", "no"}
                and gateway.slug != "okx"
            ):
                # Non-reduce-only on non-OKX is an entry order, not protection.
                continue
            oid = str(order.get("id") or "")
            if oid:
                order_ids.append(oid)
            if gateway.slug == "binance":
                algo_id = str(info.get("algoId") or "")
                if algo_id:
                    if algo_id not in algo_order_ids:
                        algo_order_ids.append(algo_id)
                    if algo_id not in order_ids:
                        order_ids.append(algo_id)
            if gateway.slug == "okx":
                algo_id = str(info.get("algoId") or "")
                if algo_id:
                    if algo_id not in algo_order_ids:
                        algo_order_ids.append(algo_id)
                    if algo_id not in order_ids:
                        order_ids.append(algo_id)
            order_side = str(order.get("side") or "").lower()
            if order_side and order_side != expected_close_side:
                invalid_side = True
            if "take_profit" in otype and not take_px and stop_px:
                take_px = stop_px
                stop_px = None
            order_price = take_px or stop_px
            order_qty = self._order_quantity(order, info)
            kind = self._classify_protective_kind(
                side=side,
                stop_px=stop_px,
                take_px=take_px,
                otype=otype,
                mark_price=mark_price,
                entry_price=entry_price,
            )
            order_ts = self._extract_order_created_ts(order, info)
            payload = {
                "id": oid,
                "price": order_price,
                "qty": order_qty,
                "created_ts": order_ts,
                "close_all": bool(
                    info.get("closeOrder")
                    or info.get("closePosition")
                    or info.get("closeOnTrigger")
                    or order.get("closeOrder")
                    or order.get("closePosition")
                    or order.get("closeOnTrigger")
                    or (
                        gateway.slug == "bybit"
                        and str(info.get("tpslMode") or "").lower() == "full"
                    )
                ),
            }
            if kind == "take":
                take_orders.append(payload)
            elif kind == "stop":
                stop_orders.append(payload)
            else:
                unknown_orders.append(payload)
        stop_candidates = [o.get("price") for o in stop_orders if _safe_float(o.get("price"))]
        take_candidates = [o.get("price") for o in take_orders if _safe_float(o.get("price"))]
        stop_val = None
        take_val = None
        if take_candidates:
            if (side or "").lower() == "short":
                take_val = min(take_candidates)
            else:
                take_val = max(take_candidates)
        if stop_candidates:
            if (side or "").lower() == "short":
                stop_val = max(stop_candidates)
            else:
                stop_val = min(stop_candidates)
        if take_val is None and len(stop_candidates) >= 2:
            if (side or "").lower() == "short":
                take_val = min(stop_candidates)
                stop_val = max(stop_candidates)
            else:
                stop_val = min(stop_candidates)
                take_val = max(stop_candidates)
        result = {
            "stop": stop_val,
            "take": take_val,
            "stop_orders": stop_orders,
            "take_orders": take_orders,
            "unknown_orders": unknown_orders,
            "order_ids": order_ids,
            "algo_order_ids": algo_order_ids,
            "invalid_side": invalid_side,
        }
        if len(self._existing_cache) > 500:
            self._existing_cache.clear()
        self._existing_cache[cache_key] = (self._clone_existing(result), time.time())
        return result

    @staticmethod
    def _round_cache_value(value: float | None) -> float | None:
        if value is None:
            return None
        try:
            return round(float(value), 4)
        except Exception:
            return None

    def _existing_cache_key(
        self,
        exchange: str,
        symbol: str,
        position_id: str | None,
        side: str | None,
        mark_price: float | None,
        entry_price: float | None,
    ) -> tuple[str, str, str | None, str | None, float | None, float | None]:
        return (
            exchange,
            symbol,
            position_id,
            side,
            self._round_cache_value(mark_price),
            self._round_cache_value(entry_price),
        )

    @staticmethod
    def _clone_existing(payload: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "stop": payload.get("stop"),
            "take": payload.get("take"),
            "stop_orders": list(payload.get("stop_orders") or []),
            "take_orders": list(payload.get("take_orders") or []),
            "unknown_orders": list(payload.get("unknown_orders") or []),
            "order_ids": list(payload.get("order_ids") or []),
            "algo_order_ids": list(payload.get("algo_order_ids") or []),
            "invalid_side": bool(payload.get("invalid_side")),
        }

    @staticmethod
    def _protective_symbol_key(value: Any) -> str:
        key = normalize_symbol(str(value or ""))
        if key.endswith("USDTM"):
            key = key[:-1]
        for suffix in ("SWAP", "PERP"):
            if key.endswith(suffix):
                key = key[: -len(suffix)]
        return key

    @classmethod
    def _order_matches_symbol(
        cls,
        order: Mapping[str, Any],
        info: Mapping[str, Any],
        target_symbol: str,
    ) -> bool:
        """Filter venues such as KuCoin that ignore the requested stop-order symbol."""
        target_key = cls._protective_symbol_key(target_symbol)
        if not target_key:
            return True
        raw_symbols = (
            order.get("symbol"),
            info.get("symbol"),
            info.get("instId"),
            info.get("contract"),
        )
        order_keys = {
            cls._protective_symbol_key(value)
            for value in raw_symbols
            if str(value or "").strip()
        }
        if not order_keys:
            return True
        return target_key in order_keys

    def _is_protective_order(
        self,
        slug: str,
        order: Mapping[str, Any],
        info: Mapping[str, Any],
        stop_px: float | None,
        take_px: float | None,
        otype: str,
    ) -> bool:
        # Explicit trigger/tp/sl fields.
        if stop_px or take_px:
            return True
        # Generic trigger indicators.
        if otype and any(key in otype for key in ("stop", "trigger", "conditional", "oco", "take_profit")):
            return True
        # Exchange-specific signals.
        if slug == "okx":
            if info.get("algoId"):
                return True
            ord_type = str(info.get("ordType") or "").lower()
            if ord_type in ("conditional", "oco", "trigger"):
                return True
            if info.get("slTriggerPx") or info.get("tpTriggerPx"):
                return True
        if slug == "bitget":
            if info.get("planType") or info.get("tpslMode"):
                return True
        if slug == "kucoin":
            if info.get("stop") or info.get("stopPrice"):
                return True
        return False

    def _classify_protective_kind(
        self,
        *,
        side: str | None,
        stop_px: float | None,
        take_px: float | None,
        otype: str,
        mark_price: float | None,
        entry_price: float | None,
    ) -> str:
        if take_px and not stop_px:
            return "take"
        if stop_px and not take_px:
            if "take_profit" in otype:
                return "take"
            if any(key in otype for key in ("stop", "trigger", "conditional")):
                return "stop"
            ref = mark_price or entry_price
            if ref:
                if (side or "").lower() == "short":
                    return "stop" if stop_px > ref else "take"
                return "stop" if stop_px < ref else "take"
            return "unknown"
        if take_px and stop_px:
            return "unknown"
        return "unknown"

    def _order_quantity(self, order: Mapping[str, Any], info: Mapping[str, Any]) -> float | None:
        qty = _safe_float(order.get("amount"))
        if qty is None or abs(qty) < 1e-12:
            qty = _safe_float(info.get("size")) or _safe_float(info.get("amount")) or _safe_float(info.get("origQty"))
        if qty is None:
            return None
        return abs(qty)

    def _extract_order_created_ts(self, order: Mapping[str, Any], info: Mapping[str, Any]) -> float | None:
        candidates = (
            order.get("timestamp"),
            order.get("lastUpdateTimestamp"),
            order.get("updateTime"),
            info.get("timestamp"),
            info.get("time"),
            info.get("createdTime"),
            info.get("cTime"),
            info.get("uTime"),
            info.get("ts"),
        )
        for raw in candidates:
            val = _safe_float(raw)
            if val is None or val <= 0:
                continue
            # Normalize ms to seconds.
            if val > 1e12:
                return val / 1000.0
            if val > 1e10:
                return val / 1000.0
            return val
        return None

    def _needs_update(self, current: float | None, target: float | None, threshold: float) -> tuple[bool, float | None]:
        if target is None:
            return False, None
        if current is None or current <= 0:
            return True, 1.0
        try:
            delta = abs(target - current) / current
        except Exception:
            return True, None
        return delta >= threshold, delta

    def _needs_qty_update(self, current: float | None, target: float, threshold: float) -> bool:
        if target <= 0:
            return False
        if current is None or current <= 0:
            return True
        try:
            delta = abs(target - current) / current
        except Exception:
            return True
        return delta >= threshold

    def _order_age_sec(self, order: Mapping[str, Any]) -> float | None:
        ts_val = _safe_float(order.get("created_ts") or order.get("timestamp"))
        if ts_val is None or ts_val <= 0:
            return None
        try:
            now = time.time()
            return max(0.0, now - ts_val)
        except Exception:
            return None

    def _needs_stop_update(
        self,
        existing: list[Mapping[str, Any]],
        target_stop: float | None,
        target_qty: float,
        price_threshold: float,
        qty_threshold: float,
        *,
        max_age_sec: int = 0,
    ) -> tuple[bool, float | None]:
        if target_stop is None:
            return False, None
        if not existing:
            return True, 1.0
        if len(existing) != 1:
            return True, None
        existing_stop = _safe_float(existing[0].get("price"))
        if existing_stop is None or existing_stop <= 0:
            return True, None
        price_diff, delta = self._needs_update(existing_stop, target_stop, price_threshold)
        qty_diff = False
        if not bool(existing[0].get("close_all")):
            qty_diff = self._needs_qty_update(
                _safe_float(existing[0].get("qty")),
                target_qty,
                qty_threshold,
            )
        stale = False
        if max_age_sec > 0:
            age_sec = self._order_age_sec(existing[0])
            stale = age_sec is not None and age_sec > float(max_age_sec)
        if price_diff or qty_diff or stale:
            return True, delta
        return False, delta

    def _needs_take_update(
        self,
        existing: list[Mapping[str, Any]],
        targets: list[TakeTarget],
        price_threshold: float,
        qty_threshold: float,
    ) -> tuple[bool, float | None]:
        if not targets:
            return False, None
        if not existing:
            return True, 1.0
        if len(existing) != len(targets):
            return True, None
        remaining = list(existing)
        max_delta: float | None = 0.0
        for target in targets:
            best_idx = None
            best_delta = None
            for idx, candidate in enumerate(remaining):
                cand_price = _safe_float(candidate.get("price"))
                if cand_price is None or cand_price <= 0:
                    continue
                cand_qty = _safe_float(candidate.get("qty"))
                price_delta = abs(target.price - cand_price) / cand_price
                if price_delta > price_threshold:
                    continue
                if not bool(candidate.get("close_all")):
                    if cand_qty is None or cand_qty <= 0:
                        continue
                    qty_delta = abs(target.quantity - cand_qty) / cand_qty
                    if qty_delta > qty_threshold:
                        continue
                if best_delta is None or price_delta < best_delta:
                    best_delta = price_delta
                    best_idx = idx
            if best_idx is None:
                return True, None
            if max_delta is not None and best_delta is not None:
                max_delta = max(max_delta, best_delta)
            remaining.pop(best_idx)
        return False, max_delta

    def _summarize_takes(self, takes: list[TakeTarget]) -> float | list[float] | None:
        if not takes:
            return None
        if len(takes) == 1:
            return takes[0].price
        return [take.price for take in takes]

    async def _place_stop(self, gateway: ExchangeGateway, target: ProtectiveTarget, price: float) -> None:
        symbol = gateway.map_symbol(target.symbol)
        rounded, tick = await self._round_price(gateway, symbol, price)
        if rounded is None or not math.isfinite(rounded) or rounded <= 0:
            raise InvalidProtectivePrice(
                f"invalid_stop_price exchange={gateway.slug} symbol={symbol} price={price}"
            )
        if gateway.slug == "binance" and tick and rounded <= tick:
            raise InvalidProtectivePrice(
                f"invalid_stop_price exchange=binance symbol={symbol} price={rounded} min_tick={tick}"
            )
        params = {
            "reduceOnly": True,
        }
        order_type = "market"
        # Exchange-specific hints
        if gateway.slug == "bybit":
            params.update(
                {
                    "stopLossPrice": rounded,
                    "slTriggerBy": "MarkPrice",
                    "tpslMode": "Full",
                }
            )
        elif gateway.slug == "binance":
            await self._place_binance_algo_conditional(
                gateway,
                target,
                order_type="STOP_MARKET",
                trigger_price=rounded,
                quantity=target.quantity,
            )
            return
        elif gateway.slug == "bitget":
            margin_coin = "USDT" if symbol.upper().endswith("USDT") else None
            hold_side = target.pos_side or target.side
            if bitget_uta_enabled():
                params = bitget_private_params(params)
                params.update(
                    {
                        "stopLossPrice": rounded,
                        "slTriggerBy": "mark",
                        "posSide": hold_side,
                    }
                )
            else:
                params.update(
                    {
                        "stopLossPrice": rounded,
                        "holdSide": hold_side,
                        "triggerType": "mark_price",
                        "planType": "loss_plan",
                        "executePrice": "0",
                    }
                )
                if margin_coin:
                    params["marginCoin"] = margin_coin
        elif gateway.slug == "okx":
            params["stopLossPrice"] = rounded
            params["slTriggerPxType"] = "mark"
            pos_side = target.pos_side or target.side
            if pos_side in ("net", "long", "short"):
                params["posSide"] = pos_side
            if target.margin_mode in ("isolated", "cross"):
                params["tdMode"] = target.margin_mode
        elif gateway.slug == "kucoin":
            trigger = "down" if target.side == "long" else "up"
            params.update(
                {
                    "stopPrice": rounded,
                    "stopPriceType": "MP",
                    "stop": trigger,
                    "closeOrder": True,  # ensure position-closing behaviour
                }
            )
            if target.margin_mode:
                params["marginMode"] = target.margin_mode
        elif gateway.slug == "gate":
            params.update(
                {
                    "stopLossPrice": rounded,
                    "price_type": 1,
                }
            )
        elif gateway.slug == "bingx":
            params.update(
                {
                    "stopLossPrice": rounded,
                    "workingType": "MARK_PRICE",
                }
            )
        else:
            params["stopLossPrice"] = rounded
        await self._call_with_time_sync_retry(
            gateway,
            "create_order",
            lambda: gateway.client.create_order(  # type: ignore[union-attr]
                symbol=symbol,
                type=order_type,
                side="sell" if target.side == "long" else "buy",
                amount=target.quantity,
                params=params,
            ),
        )

    async def _place_take(
        self,
        gateway: ExchangeGateway,
        target: ProtectiveTarget,
        price: float,
        *,
        quantity: float | None = None,
    ) -> None:
        symbol = gateway.map_symbol(target.symbol)
        rounded, tick = await self._round_price(gateway, symbol, price)
        if rounded is None or not math.isfinite(rounded) or rounded <= 0:
            raise InvalidProtectivePrice(
                f"invalid_take_price exchange={gateway.slug} symbol={symbol} price={price}"
            )
        if gateway.slug == "binance" and tick and rounded <= tick:
            raise InvalidProtectivePrice(
                f"invalid_take_price exchange=binance symbol={symbol} price={rounded} min_tick={tick}"
            )
        params = {
            "reduceOnly": True,
        }
        order_type = "market"
        if gateway.slug == "bybit":
            params.update(
                {
                    "takeProfitPrice": rounded,
                    "tpTriggerBy": "MarkPrice",
                    "tpslMode": "Full",
                }
            )
        elif gateway.slug == "binance":
            order_qty = quantity if quantity is not None else target.quantity
            await self._place_binance_algo_conditional(
                gateway,
                target,
                order_type="TAKE_PROFIT_MARKET",
                trigger_price=rounded,
                quantity=order_qty,
            )
            return
        elif gateway.slug == "bitget":
            margin_coin = "USDT" if symbol.upper().endswith("USDT") else None
            hold_side = target.pos_side or target.side
            if bitget_uta_enabled():
                params = bitget_private_params(params)
                params.update(
                    {
                        "takeProfitPrice": rounded,
                        "tpTriggerBy": "mark",
                        "posSide": hold_side,
                    }
                )
            else:
                params.update(
                    {
                        "takeProfitPrice": rounded,
                        "holdSide": hold_side,
                        "triggerType": "mark_price",
                        "planType": "profit_plan",
                        "executePrice": "0",
                    }
                )
                if margin_coin:
                    params["marginCoin"] = margin_coin
        elif gateway.slug == "okx":
            params["takeProfitPrice"] = rounded
            params["tpTriggerPxType"] = "mark"
            pos_side = target.pos_side or target.side
            if pos_side in ("net", "long", "short"):
                params["posSide"] = pos_side
            if target.margin_mode in ("isolated", "cross"):
                params["tdMode"] = target.margin_mode
        elif gateway.slug == "kucoin":
            trigger = "up" if target.side == "long" else "down"
            params.update(
                {
                    "stopPrice": rounded,
                    "stopPriceType": "MP",
                    "stop": trigger,
                    "closeOrder": True,
                }
            )
            if target.margin_mode:
                params["marginMode"] = target.margin_mode
        elif gateway.slug == "gate":
            params.update(
                {
                    "takeProfitPrice": rounded,
                    "price_type": 1,
                }
            )
        elif gateway.slug == "bingx":
            params.update(
                {
                    "takeProfitPrice": rounded,
                    "workingType": "MARK_PRICE",
                }
            )
        else:
            params["takeProfitPrice"] = rounded
        order_qty = quantity if quantity is not None else target.quantity
        await self._call_with_time_sync_retry(
            gateway,
            "create_order",
            lambda: gateway.client.create_order(  # type: ignore[union-attr]
                symbol=symbol,
                type=order_type,
                side="sell" if target.side == "long" else "buy",
                amount=order_qty,
                params=params,
            ),
        )

    async def _round_price(
        self,
        gateway: ExchangeGateway,
        symbol: str,
        price: float,
    ) -> tuple[float | None, float | None]:
        """Round price to tick size when metadata is available."""
        sym_meta = get_or_fetch_symbol_meta(gateway.slug, gateway.map_symbol(symbol) or symbol, lambda: None)
        tick = getattr(sym_meta, "tick_size", None) if sym_meta else None
        if not tick or tick <= 0:
            return price, None
        try:
            if price > 0 and tick >= price:
                refreshed = self._refresh_symbol_meta(gateway, symbol)
                tick = refreshed.tick_size if refreshed else tick
                if not tick or tick <= 0 or tick >= price:
                    return price, None
            steps = round(price / tick)
            if steps <= 0:
                refreshed = self._refresh_symbol_meta(gateway, symbol)
                tick = refreshed.tick_size if refreshed else tick
                if not tick or tick <= 0:
                    return price, None
                steps = round(price / tick)
                if steps <= 0:
                    return price, None
            return steps * tick, tick
        except Exception:
            return price, tick

    def _fallback_take_price(
        self, side: str, *, mark_price: float | None, entry_price: float | None, ratio: float
    ) -> float | None:
        """Fallback TP when peer stop is unavailable."""
        base = _safe_float(mark_price) or _safe_float(entry_price)
        if base is None or base <= 0 or ratio <= 0:
            return None
        bounded_ratio = min(0.50, max(0.01, float(ratio)))
        if side == "long":
            return base * (1.0 + bounded_ratio)
        return base * (1.0 - bounded_ratio)

    async def _send_telegram_warning(self, text: str, *, key: str | None = None) -> None:
        """Lightweight notifier for protective issues with cooldown."""
        if hasattr(self._risk_config, "send_missing_stop_alerts") and not self._risk_config.send_missing_stop_alerts:
            return
        cooldown = max(0, int(self._risk_config.protective_warn_cooldown_sec or 0))
        warn_key = key or text
        now = time.time()
        last = self._warned_at.get(warn_key, 0)
        if cooldown and (now - last) < cooldown:
            return
        try:
            if await self._notifier.send_text(text, title="FeeArb protective alert"):
                self._warned_at[warn_key] = now
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Protective notify error: %s", exc)
