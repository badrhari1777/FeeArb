"""Protective stop/take management.

This module orchestrates protective orders (stop-loss / take-profit) across
exchanges using the existing ccxt-based ExchangeGateway infrastructure. It is
best-effort and defensive: all network/placement errors are logged and bubbled
up to the caller via the returned actions list; no exceptions are raised to
halt the main refresh loop.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, normalize_symbol, _safe_float
try:  # ccxt is optional for typing; handled at runtime.
    from ccxt.base.errors import RequestTimeout  # type: ignore
except Exception:  # pragma: no cover - fallback when ccxt missing
    RequestTimeout = tuple()  # type: ignore
from risk.config import RiskConfig
from utils.cache_db import get_or_fetch_symbol_meta
import aiohttp

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
    mark_price: float | None = None
    entry_price: float | None = None


@dataclass(slots=True)
class TakeTarget:
    price: float
    quantity: float


class ProtectiveOrderManager:
    """Best-effort protective order synchroniser built on ccxt gateways."""

    def __init__(self, risk_config: RiskConfig, blocked_exchanges: set[str] | None = None) -> None:
        self._risk_config = risk_config
        self._gateways: dict[str, ExchangeGateway] = {
            spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS
        }
        self._blocked = {ex.lower() for ex in (blocked_exchanges or {"mexc"})}
        self._lock = asyncio.Lock()
        self._warned_at: dict[str, float] = {}

    def update_config(self, risk_config: RiskConfig) -> None:
        self._risk_config = risk_config

    async def sync_protective_orders(
        self,
        positions: Iterable[Mapping[str, Any]],
        *,
        anti_orphan_enabled: bool = False,
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
                            self._sync_leg(target, anti_orphan_enabled=anti_orphan_enabled)
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
                candidates.append((opp_stop * ratio, opp_qty))
            if not candidates:
                fallback = self._fallback_take_price(
                    side,
                    mark_price=leg_info.get("mark"),
                    entry_price=leg_info.get("entry"),
                    ratio=float(self._risk_config.fallback_take_rr_pct),
                )
                if fallback:
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
        anti_orphan_enabled: bool,
    ) -> dict[str, Any]:
        gw = self._gateways.get(target.exchange)
        threshold = max(0.0, float(self._risk_config.stop_requote_threshold_pct))
        qty_threshold = max(0.01, threshold)
        if gw is None:
            return {"exchange": target.exchange, "symbol": target.symbol, "status": "skipped", "reason": "adapter_missing"}
        if target.quantity <= 0:
            return {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "status": "skipped",
                "reason": "zero_quantity",
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
                    )
            except Exception:  # pylint: disable=broad-except
                existing = {"order_ids": [], "error": "existing_fetch_failed"}
            stop_diff, stop_delta = self._needs_stop_update(
                existing.get("stop_orders") or [],
                target.stop,
                target.quantity,
                threshold,
                qty_threshold,
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
        )
        take_diff, take_delta = self._needs_take_update(
            existing_take_orders,
            target.takes,
            threshold,
            qty_threshold,
        )

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
        try:
            if to_cancel:
                algo_ids = set(existing.get("algo_order_ids") or [])
                for oid in to_cancel:
                    try:
                        cancel_params = {}
                        if gw.slug == "kucoin":
                            cancel_params["stop"] = True
                        if gw.slug == "gate":
                            cancel_params["trigger"] = True
                        if gw.slug == "okx" and oid in algo_ids:
                            cancel_params["trigger"] = True
                        await gw.client.cancel_order(oid, target.symbol, params=cancel_params)
                    except Exception:  # pragma: no cover - defensive
                        logger.debug("%s cancel %s failed", target.exchange, oid)
            if target.stop and stop_diff:
                await self._place_stop(gw, target, target.stop)
            if target.takes and take_diff:
                for take in target.takes:
                    if take.quantity <= 0:
                        continue
                    try:
                        await self._place_take(gw, target, take.price, quantity=take.quantity)
                    except Exception as exc:  # pylint: disable=broad-except
                        msg = str(exc)
                        # Bybit enforces TP >=10% of position size; degrade gracefully.
                        if "10_pcnt" in msg or "PartialTakeProfit" in msg:
                            take_skipped_min = True
                            actions["error"] = msg
                            logger.info("Skipping take placement (min-size) for %s %s: %s", target.exchange, target.symbol, msg)
                        else:
                            raise
            if anti_orphan_enabled and target.takes and target.stop and not take_diff:
                # Ensure we always have both sides when requested.
                await self._ensure_dual_orders(gw, target, existing)
            if take_skipped_min:
                actions["status"] = "take_skipped_min_size"
            else:
                actions["status"] = "updated" if (stop_diff or take_diff) else "unchanged"
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

    async def _fetch_existing(
        self,
        gateway: ExchangeGateway,
        symbol: str,
        position_id: str | None,
        side: str | None,
        *,
        mark_price: float | None = None,
        entry_price: float | None = None,
    ) -> dict[str, Any]:
        orders: list[dict[str, Any]] = []
        try:
            # KuCoin stop orders live in a separate endpoint; fetch both.
            if gateway.slug == "kucoin":
                default_orders = await gateway.client.fetch_open_orders(symbol)  # type: ignore[union-attr]
                stop_orders = await gateway.client.fetch_open_orders(symbol, params={"stop": True})  # type: ignore[union-attr]
                orders = (default_orders or []) + (stop_orders or [])
            elif gateway.slug == "okx":
                default_orders = await gateway.client.fetch_open_orders(symbol)  # type: ignore[union-attr]
                trigger_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                    symbol,
                    params={"trigger": True},
                )
                conditional_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                    symbol,
                    params={"ordType": "conditional"},
                )
                orders = (default_orders or []) + (trigger_orders or []) + (conditional_orders or [])
            elif gateway.slug == "gate":
                default_orders = await gateway.client.fetch_open_orders(symbol)  # type: ignore[union-attr]
                trigger_orders = await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                    symbol,
                    params={"trigger": True},
                )
                orders = (default_orders or []) + (trigger_orders or [])
            elif gateway.slug == "bybit":
                orders = []
                try:
                    orders += await gateway.client.fetch_open_orders(symbol)  # type: ignore[union-attr]
                except Exception:
                    pass
                try:
                    orders += await gateway.client.fetch_open_orders(symbol, params={"trigger": True})  # type: ignore[union-attr]
                except Exception:
                    pass
                try:
                    orders += await gateway.client.fetch_open_orders(  # type: ignore[union-attr]
                        symbol,
                        params={"orderFilter": "tpslOrder"},
                    )
                except Exception:
                    pass
            else:
                orders = await gateway.client.fetch_open_orders(symbol)  # type: ignore[union-attr]
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
            otype = str(order.get("type") or info.get("type") or "").lower()
            trigger_info = info.get("trigger")
            if not isinstance(trigger_info, dict):
                trigger_info = {}
            stop_px = _safe_float(
                info.get("stopLossPrice")
                or info.get("stopLoss")
                or info.get("slTriggerPx")
                or order.get("stopPrice")
                or order.get("triggerPrice")
                or info.get("stopPrice")
                or info.get("triggerPrice")
                or info.get("triggerPx")
                or trigger_info.get("price")
                or trigger_info.get("trigger_price")
            )
            take_info = info.get("takeProfit")
            if not isinstance(take_info, dict):
                take_info = {}
            take_px = _safe_float(
                info.get("takeProfitPrice")
                or info.get("takeProfitEntrustPrice")
                or info.get("tpTriggerPx")
            )
            if take_px is None:
                take_px = _safe_float(info.get("takeProfit"))
            if take_px is None:
                take_px = _safe_float(take_info.get("stopPrice"))
            reduce_flag = info.get("reduceOnly") or info.get("reduce_only") or order.get("reduceOnly")
            is_protective = self._is_protective_order(gateway.slug, order, info, stop_px, take_px, otype)
            if not is_protective:
                continue
            oid = str(order.get("id") or "")
            if oid:
                order_ids.append(oid)
            if gateway.slug == "okx":
                algo_id = str(info.get("algoId") or "")
                if algo_id:
                    if algo_id not in algo_order_ids:
                        algo_order_ids.append(algo_id)
                    if algo_id not in order_ids:
                        order_ids.append(algo_id)
            if reduce_flag is False and gateway.slug != "okx":
                # Non-reduce-only on non-OKX is unlikely a protective order.
                continue
            order_side = str(order.get("side") or "").lower()
            if order_side and order_side != expected_close_side:
                invalid_side = True
            if "take_profit" in otype and not take_px and stop_px:
                take_px = stop_px
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
            payload = {"id": oid, "price": order_price, "qty": order_qty}
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
        return {
            "stop": stop_val,
            "take": take_val,
            "stop_orders": stop_orders,
            "take_orders": take_orders,
            "unknown_orders": unknown_orders,
            "order_ids": order_ids,
            "algo_order_ids": algo_order_ids,
            "invalid_side": invalid_side,
        }

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

    def _needs_stop_update(
        self,
        existing: list[Mapping[str, Any]],
        target_stop: float | None,
        target_qty: float,
        price_threshold: float,
        qty_threshold: float,
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
        qty_diff = self._needs_qty_update(_safe_float(existing[0].get("qty")), target_qty, qty_threshold)
        if price_diff or qty_diff:
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
        rounded = await self._round_price(gateway, symbol, price)
        params = {
            "reduceOnly": True,
        }
        # Exchange-specific hints
        if gateway.slug == "bitget":
            margin_coin = "USDT" if symbol.upper().endswith("USDT") else None
            # Bitget one-way mode expects holdSide=buy/sell (not long/short).
            hold_side = "buy" if target.side == "long" else "sell"
            params.update(
                {
                    "stopLossPrice": rounded,
                    "slTriggerType": "market_price",
                    "holdSide": hold_side,
                    "triggerType": "market_price",
                    "tpslMode": "full",
                }
            )
            if margin_coin:
                params["marginCoin"] = margin_coin
        elif gateway.slug == "okx":
            params["stopLossPrice"] = rounded
            if target.side in ("long", "short"):
                params["posSide"] = target.side
            if target.margin_mode in ("isolated", "cross"):
                params["tdMode"] = target.margin_mode
        elif gateway.slug == "kucoin":
            trigger = "down" if target.side == "long" else "up"
            params.update(
                {
                    "stopPrice": rounded,
                    "stop": trigger,
                    "closeOrder": True,  # ensure position-closing behaviour
                }
            )
            if target.margin_mode:
                params["marginMode"] = target.margin_mode
        else:
            params["stopLossPrice"] = rounded
        await gateway.client.create_order(  # type: ignore[union-attr]
            symbol=symbol,
            type="market",
            side="sell" if target.side == "long" else "buy",
            amount=target.quantity,
            params=params,
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
        rounded = await self._round_price(gateway, symbol, price)
        params = {
            "reduceOnly": True,
        }
        if gateway.slug == "bitget":
            margin_coin = "USDT" if symbol.upper().endswith("USDT") else None
            # Bitget one-way mode expects holdSide=buy/sell (not long/short).
            hold_side = "buy" if target.side == "long" else "sell"
            params.update(
                {
                    "takeProfitPrice": rounded,
                    "tpTriggerType": "market_price",
                    "holdSide": hold_side,
                    "triggerType": "market_price",
                    "tpslMode": "full",
                }
            )
            if margin_coin:
                params["marginCoin"] = margin_coin
        elif gateway.slug == "okx":
            params["takeProfitPrice"] = rounded
            if target.side in ("long", "short"):
                params["posSide"] = target.side
            if target.margin_mode in ("isolated", "cross"):
                params["tdMode"] = target.margin_mode
        elif gateway.slug == "kucoin":
            trigger = "up" if target.side == "long" else "down"
            params.update(
                {
                    "stopPrice": rounded,
                    "stop": trigger,
                    "closeOrder": True,
                }
            )
            if target.margin_mode:
                params["marginMode"] = target.margin_mode
        else:
            params["takeProfitPrice"] = rounded
        order_qty = quantity if quantity is not None else target.quantity
        await gateway.client.create_order(  # type: ignore[union-attr]
            symbol=symbol,
            type="market",
            side="sell" if target.side == "long" else "buy",
            amount=order_qty,
            params=params,
        )

    async def _ensure_dual_orders(
        self,
        gateway: ExchangeGateway,
        target: ProtectiveTarget,
        existing: Mapping[str, Any],
    ) -> None:
        """If only one side exists, re-place the missing leg."""
        if target.stop and not (existing.get("stop_orders") or []):
            await self._place_stop(gateway, target, target.stop)
        if target.takes and not (existing.get("take_orders") or []):
            for take in target.takes:
                if take.quantity <= 0:
                    continue
                await self._place_take(gateway, target, take.price, quantity=take.quantity)

    async def _round_price(self, gateway: ExchangeGateway, symbol: str, price: float) -> float:
        """Round price to tick size when metadata is available."""
        sym_meta = get_or_fetch_symbol_meta(gateway.slug, gateway.map_symbol(symbol) or symbol, lambda: None)
        tick = getattr(sym_meta, "tick_size", None) if sym_meta else None
        if not tick or tick <= 0:
            return price
        try:
            steps = round(price / tick)
            return steps * tick
        except Exception:
            return price

    def _fallback_take_price(
        self, side: str, *, mark_price: float | None, entry_price: float | None, ratio: float
    ) -> float | None:
        """Fallback TP when peer stop is unavailable."""
        base = _safe_float(mark_price) or _safe_float(entry_price)
        if base is None or base <= 0 or ratio <= 0:
            return None
        if side == "long":
            return base * (1.0 + ratio)
        # For shorts ensure positive price; floor to 1% of base to avoid zero.
        return max(base * 0.01, base * (1.0 - ratio))

    async def _send_telegram_warning(self, text: str, *, key: str | None = None) -> None:
        """Lightweight Telegram notifier for protective issues with cooldown."""
        if hasattr(self._risk_config, "send_missing_stop_alerts") and not self._risk_config.send_missing_stop_alerts:
            return
        cooldown = max(0, int(self._risk_config.protective_warn_cooldown_sec or 0))
        warn_key = key or text
        now = time.time()
        last = self._warned_at.get(warn_key, 0)
        if cooldown and (now - last) < cooldown:
            return
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip() or str(self._risk_config.telegram_alert_chat_id or "").strip()
        if not token or not chat_id:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": text}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(url, data=data) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.debug("Telegram protective warn failed (%s): %s", resp.status, body)
                        return
            self._warned_at[warn_key] = now
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Telegram protective warn error: %s", exc)
