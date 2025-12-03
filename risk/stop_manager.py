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
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, normalize_symbol, _safe_float
try:  # ccxt is optional for typing; handled at runtime.
    from ccxt.base.errors import RequestTimeout  # type: ignore
except Exception:  # pragma: no cover - fallback when ccxt missing
    RequestTimeout = tuple()  # type: ignore
from risk.config import RiskConfig
from utils.cache_db import get_or_fetch_symbol_meta

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
    take: float | None
    quantity: float
    side: str
    exchange: str
    symbol: str
    position_id: str | None


class ProtectiveOrderManager:
    """Best-effort protective order synchroniser built on ccxt gateways."""

    def __init__(self, risk_config: RiskConfig, blocked_exchanges: set[str] | None = None) -> None:
        self._risk_config = risk_config
        self._gateways: dict[str, ExchangeGateway] = {
            spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS
        }
        self._blocked = {ex.lower() for ex in (blocked_exchanges or {"mexc"})}
        self._lock = asyncio.Lock()

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
        if len(legs) < 1:
            return []
        longs = []
        shorts = []
        for leg in legs:
            side = (leg.get("side") or "").lower()
            qty = abs(_safe_float(leg.get("contracts")) or _safe_float(leg.get("coin_qty")) or 0.0)
            if side == "long" and qty > 0:
                longs.append((leg, qty))
            elif side == "short" and qty > 0:
                shorts.append((leg, qty))
        total_long = sum(q for _, q in longs)
        total_short = sum(q for _, q in shorts)
        match_qty = min(total_long, total_short)
        if match_qty <= 0:
            return []

        long_scale = match_qty / total_long if total_long > 0 else 0
        short_scale = match_qty / total_short if total_short > 0 else 0

        # pick representative marks for spread ratio
        long_mark = _safe_float(longs[0][0].get("mark_price")) if longs else None
        short_mark = _safe_float(shorts[0][0].get("mark_price")) if shorts else None
        ratio_long_to_short = (short_mark / long_mark) if long_mark and short_mark else None
        ratio_short_to_long = (long_mark / short_mark) if long_mark and short_mark else None

        targets: list[ProtectiveTarget] = []
        for leg, raw_qty in longs + shorts:
            side = (leg.get("side") or "").lower()
            alloc_qty = raw_qty * (long_scale if side == "long" else short_scale)
            if alloc_qty <= 0:
                continue
            liq = _safe_float(leg.get("liquidation_price"))
            mark = _safe_float(leg.get("mark_price"))
            entry = _safe_float(leg.get("entry_price"))
            stop = self._target_stop_price(side, liq, mark_price=mark, entry_price=entry)
            take = None
            if side == "long" and ratio_short_to_long and shorts:
                peer_leg = shorts[0][0]
                peer_stop = self._target_stop_price(
                    "short",
                    _safe_float(peer_leg.get("liquidation_price")),
                    mark_price=_safe_float(peer_leg.get("mark_price")),
                    entry_price=_safe_float(peer_leg.get("entry_price")),
                )
                if peer_stop:
                    take = peer_stop * ratio_short_to_long
            elif side == "short" and ratio_long_to_short and longs:
                peer_leg = longs[0][0]
                peer_stop = self._target_stop_price(
                    "long",
                    _safe_float(peer_leg.get("liquidation_price")),
                    mark_price=_safe_float(peer_leg.get("mark_price")),
                    entry_price=_safe_float(peer_leg.get("entry_price")),
                )
                if peer_stop:
                    take = peer_stop * ratio_long_to_short
            targets.append(
                ProtectiveTarget(
                    stop=stop,
                    take=take,
                    quantity=alloc_qty,
                    side=side,
                    exchange=str(leg.get("exchange") or "").lower(),
                    symbol=str(leg.get("symbol") or ""),
                    position_id=str(leg.get("position_id") or leg.get("id") or ""),
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
                    existing = await self._fetch_existing(gw, target.symbol, target.position_id)
            except Exception:  # pylint: disable=broad-except
                existing = {"order_ids": [], "error": "existing_fetch_failed"}
            actions: dict[str, Any] = {
                "exchange": target.exchange,
                "symbol": target.symbol,
                "position_id": target.position_id,
                "side": target.side,
                "existing": existing,
                "target_stop": target.stop,
                "target_take": target.take,
                "status": "blocked",
                "reason": "exchange_blocked",
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
        threshold = max(0.0, float(self._risk_config.stop_requote_threshold_pct))
        existing = await self._fetch_existing(gw, target.symbol, target.position_id)
        actions: dict[str, Any] = {
            "exchange": target.exchange,
            "symbol": target.symbol,
            "position_id": target.position_id,
            "side": target.side,
            "existing": existing,
            "target_stop": target.stop,
            "target_take": target.take,
            "status": "noop",
        }
        to_cancel: list[str] = []
        stop_diff, stop_delta = self._needs_update(existing.get("stop"), target.stop, threshold)
        take_diff, take_delta = self._needs_update(existing.get("take"), target.take, threshold)

        if stop_diff or take_diff:
            to_cancel = [oid for oid in existing.get("order_ids", [])]

        take_skipped_min = False
        try:
            if to_cancel:
                for oid in to_cancel:
                    try:
                        await gw.client.cancel_order(oid, target.symbol)
                    except Exception:  # pragma: no cover - defensive
                        logger.debug("%s cancel %s failed", target.exchange, oid)
            if target.stop and stop_diff:
                await self._place_stop(gw, target, target.stop)
            if target.take and take_diff:
                try:
                    await self._place_take(gw, target, target.take)
                except Exception as exc:  # pylint: disable=broad-except
                    msg = str(exc)
                    # Bybit enforces TP >=10% of position size; degrade gracefully.
                    if "10_pcnt" in msg or "PartialTakeProfit" in msg:
                        take_skipped_min = True
                        actions["error"] = msg
                        logger.info("Skipping take placement (min-size) for %s %s: %s", target.exchange, target.symbol, msg)
                    else:
                        raise
            if anti_orphan_enabled and target.take and target.stop and not take_diff:
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
                actions["status"] = "error"
                actions["error"] = str(exc)
                logger.warning("Protective order sync failed for %s %s: %s", target.exchange, target.symbol, exc, exc_info=True)
        finally:
            if gw and gw.requires_cycle_close():
                try:
                    await gw.close()
                except Exception:  # pragma: no cover - defensive
                    pass
        status = actions.get("status")
        err_msg = actions.get("error") or actions.get("reason")
        ok_states = {"updated", "unchanged"}
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
        self, gateway: ExchangeGateway, symbol: str, position_id: str | None
    ) -> dict[str, Any]:
        orders: list[dict[str, Any]] = []
        try:
            orders = await gateway.client.fetch_open_orders(symbol)  # type: ignore[union-attr]
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("%s fetch_open_orders failed for %s: %s", gateway.slug, symbol, exc)
            return {"order_ids": []}
        stop = None
        take = None
        order_ids: list[str] = []
        for order in orders or []:
            oid = str(order.get("id") or "")
            if oid:
                order_ids.append(oid)
            info = order.get("info") or {}
            stop_px = _safe_float(info.get("stopLossPrice") or order.get("stopPrice") or info.get("triggerPrice"))
            take_px = _safe_float(info.get("takeProfitPrice"))
            # Some venues (e.g., bitget) may not flag reduceOnly on TP/SL. Only skip if explicitly false.
            reduce_flag = info.get("reduceOnly") or info.get("reduce_only") or order.get("reduceOnly")
            if reduce_flag is False:
                continue
            if stop_px:
                stop = stop_px
            if take_px:
                take = take_px
        return {"stop": stop, "take": take, "order_ids": order_ids}

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

    async def _place_stop(self, gateway: ExchangeGateway, target: ProtectiveTarget, price: float) -> None:
        symbol = gateway.map_symbol(target.symbol)
        rounded = await self._round_price(gateway, symbol, price)
        params = {
            "reduceOnly": True,
        }
        # Exchange-specific hints
        if gateway.slug == "bitget":
            margin_coin = "USDT" if symbol.upper().endswith("USDT") else None
            params.update(
                {
                    "stopLossPrice": rounded,
                    "slTriggerType": "market_price",
                    "holdSide": "long" if target.side == "long" else "short",
                    "triggerType": "market_price",
                    "tpslMode": "full",
                }
            )
            if margin_coin:
                params["marginCoin"] = margin_coin
        else:
            params["stopLossPrice"] = rounded
        await gateway.client.create_order(  # type: ignore[union-attr]
            symbol=symbol,
            type="market",
            side="sell" if target.side == "long" else "buy",
            amount=target.quantity,
            params=params,
        )

    async def _place_take(self, gateway: ExchangeGateway, target: ProtectiveTarget, price: float) -> None:
        symbol = gateway.map_symbol(target.symbol)
        rounded = await self._round_price(gateway, symbol, price)
        params = {
            "reduceOnly": True,
        }
        if gateway.slug == "bitget":
            margin_coin = "USDT" if symbol.upper().endswith("USDT") else None
            params.update(
                {
                    "takeProfitPrice": rounded,
                    "tpTriggerType": "market_price",
                    "holdSide": "long" if target.side == "long" else "short",
                    "triggerType": "market_price",
                    "tpslMode": "full",
                }
            )
            if margin_coin:
                params["marginCoin"] = margin_coin
        else:
            params["takeProfitPrice"] = rounded
        await gateway.client.create_order(  # type: ignore[union-attr]
            symbol=symbol,
            type="market",
            side="buy" if target.side == "long" else "sell",
            amount=target.quantity,
            params=params,
        )

    async def _ensure_dual_orders(
        self,
        gateway: ExchangeGateway,
        target: ProtectiveTarget,
        existing: Mapping[str, Any],
    ) -> None:
        """If only one side exists, re-place the missing leg."""
        if target.stop and existing.get("stop") is None:
            await self._place_stop(gateway, target, target.stop)
        if target.take and existing.get("take") is None:
            await self._place_take(gateway, target, target.take)

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
