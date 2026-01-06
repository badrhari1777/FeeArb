from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

from config import EXCHANGE_COMMISSIONS
from exchanges import get_adapter, normalize_exchange_name
from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _safe_float, normalize_symbol

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OrderBookStats:
    best_bid: float | None
    best_ask: float | None
    spread: float | None
    mid: float | None
    bid_liquidity_top3: float
    ask_liquidity_top3: float
    min_liquidity_top3: float


def _precision_to_step(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    if numeric >= 1:
        try:
            return 10 ** (-int(numeric))
        except (TypeError, ValueError, OverflowError):
            return None
    return numeric


def _round_to_step(value: float, step: float | None, *, mode: str = "down") -> float:
    if step is None or step <= 0:
        return float(value)
    if value <= 0:
        return 0.0
    ratio = value / step
    if mode == "up":
        return math.ceil(ratio - 1e-12) * step
    return math.floor(ratio + 1e-12) * step


def _to_base_qty(qty: float | None, contract_size: float | None) -> float | None:
    if qty is None:
        return None
    if contract_size and contract_size > 0:
        return float(qty) * contract_size
    return float(qty)


def _min_qty_required(
    *,
    min_qty: float | None,
    min_notional: float | None,
    price: float | None,
    amount_step: float | None,
) -> float | None:
    candidate = 0.0
    if min_qty:
        candidate = max(candidate, float(min_qty))
    if min_notional and price and price > 0:
        candidate = max(candidate, float(min_notional) / float(price))
    if candidate <= 0:
        return None
    return _round_to_step(candidate, amount_step, mode="up")


def _choose_chunk_qty(
    *,
    remaining: float,
    requested_qty: float | None,
    min_chunk: float | None,
    max_chunk: float | None,
    amount_step: float | None,
) -> tuple[float | None, list[str]]:
    warnings: list[str] = []
    if min_chunk and remaining < min_chunk:
        warnings.append("remaining qty below exchange minimum; unable to execute final chunk")
        return None, warnings
    chunk = requested_qty or max_chunk or remaining
    if chunk <= 0:
        return None, warnings
    if max_chunk and chunk > max_chunk:
        warnings.append("chunk_qty above slippage cap; reduced")
        chunk = max_chunk
    if min_chunk and chunk < min_chunk:
        warnings.append("chunk_qty below exchange minimum; adjusted upward")
        chunk = min_chunk
    chunk = min(chunk, remaining)
    chunk = _round_to_step(chunk, amount_step, mode="down")
    if min_chunk and chunk < min_chunk:
        chunk = _round_to_step(min_chunk, amount_step, mode="up")
    if max_chunk and chunk > max_chunk:
        chunk = _round_to_step(max_chunk, amount_step, mode="down")
    if chunk <= 0:
        return None, warnings
    return chunk, warnings


def _apply_price_offset(
    price: float | None,
    *,
    side: str,
    offset_bps: float | None = None,
    offset_ticks: int | None = None,
    price_step: float | None = None,
    round_mode: str | None = None,
) -> float | None:
    if price is None or price <= 0:
        return None
    adjusted = float(price)
    if offset_ticks and price_step:
        if side == "buy":
            adjusted += price_step * offset_ticks
        else:
            adjusted -= price_step * offset_ticks
    elif offset_bps and offset_bps > 0:
        multiplier = 1 + (offset_bps / 10_000.0) if side == "buy" else 1 - (offset_bps / 10_000.0)
        adjusted *= multiplier
    if price_step:
        if round_mode == "up":
            mode = "up"
        elif round_mode == "down":
            mode = "down"
        elif round_mode == "passive":
            mode = "down" if side == "buy" else "up"
        elif round_mode == "aggressive":
            mode = "up" if side == "buy" else "down"
        else:
            mode = "up" if side == "buy" else "down"
        adjusted = _round_to_step(adjusted, price_step, mode=mode)
    return adjusted if adjusted > 0 else None


def _pct_to_fraction(value: float | None) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if numeric < 0:
        return None
    return numeric / 100.0 if numeric > 1 else numeric


def _to_ccxt_symbol(raw_symbol: str) -> str:
    symbol = (raw_symbol or "").strip().upper()
    if not symbol:
        return ""
    if ":" in symbol:
        symbol = symbol.split(":", 1)[0]
    if "/" in symbol:
        return symbol
    symbol = symbol.replace("-", "").replace("_", "")
    if symbol.endswith("USDTM"):
        symbol = symbol[:-1]
    if symbol.endswith("UMCBL") or symbol.endswith("DMCBL"):
        symbol = symbol[:-5]
    if symbol.endswith("SWAP"):
        symbol = symbol[:-4]
    if symbol.endswith("PERP"):
        symbol = symbol[:-4]
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"{base}/USDT"
    if symbol.endswith("USD"):
        base = symbol[:-3]
        return f"{base}/USD"
    return f"{symbol}/USDT"


def _top_notional(levels: Iterable[Iterable[float]], top_n: int = 3) -> float:
    total = 0.0
    count = 0
    for level in levels:
        if len(level) < 2:
            continue
        price = _safe_float(level[0]) or 0.0
        size = _safe_float(level[1]) or 0.0
        if price <= 0 or size <= 0:
            continue
        total += price * size
        count += 1
        if count >= top_n:
            break
    return total


def _scale_orderbook(orderbook: Mapping[str, Any], contract_size: float | None) -> dict[str, Any]:
    if not contract_size or contract_size == 1:
        return dict(orderbook)
    scaled: dict[str, Any] = dict(orderbook)
    for side in ("bids", "asks"):
        levels = orderbook.get(side) or []
        scaled_levels: list[list[float | str]] = []
        for level in levels:
            if len(level) < 2:
                continue
            price = _safe_float(level[0]) or 0.0
            size = _safe_float(level[1]) or 0.0
            if price <= 0 or size <= 0:
                continue
            rest = list(level[2:]) if len(level) > 2 else []
            scaled_levels.append([price, size * contract_size] + rest)
        scaled[side] = scaled_levels
    return scaled


def orderbook_stats(orderbook: Mapping[str, Any] | None, *, top_n: int = 3) -> OrderBookStats:
    bids = (orderbook or {}).get("bids") or []
    asks = (orderbook or {}).get("asks") or []
    best_bid = _safe_float(bids[0][0]) if bids else None
    best_ask = _safe_float(asks[0][0]) if asks else None
    spread = (best_ask - best_bid) if best_bid and best_ask else None
    mid = ((best_bid + best_ask) / 2.0) if best_bid and best_ask else None
    bid_liq = _top_notional(bids, top_n=top_n)
    ask_liq = _top_notional(asks, top_n=top_n)
    return OrderBookStats(
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        mid=mid,
        bid_liquidity_top3=bid_liq,
        ask_liquidity_top3=ask_liq,
        min_liquidity_top3=min(bid_liq, ask_liq) if bid_liq and ask_liq else 0.0,
    )

def estimate_fill(levels: Iterable[Iterable[float]], qty: float) -> dict[str, float | None]:
    remaining = max(0.0, float(qty))
    cost = 0.0
    filled = 0.0
    for level in levels:
        if remaining <= 0:
            break
        if len(level) < 2:
            continue
        price = _safe_float(level[0]) or 0.0
        size = _safe_float(level[1]) or 0.0
        if price <= 0 or size <= 0:
            continue
        take = size if size < remaining else remaining
        cost += take * price
        filled += take
        remaining -= take
    avg = (cost / filled) if filled > 0 else None
    return {"avg_price": avg, "filled_qty": filled, "remaining_qty": remaining}


def slippage_bps(best_price: float | None, avg_price: float | None, side: str) -> float | None:
    if not best_price or not avg_price or best_price <= 0:
        return None
    if side == "buy":
        delta = avg_price - best_price
    else:
        delta = best_price - avg_price
    return (delta / best_price) * 10_000.0


def spread_pct(long_price: float | None, short_price: float | None) -> float | None:
    if long_price is None or short_price is None or short_price == 0:
        return None
    return (long_price - short_price) / short_price * 100.0


def max_qty_for_slippage(
    levels: Iterable[Iterable[float]],
    *,
    side: str,
    max_bps: float | None,
) -> float | None:
    if max_bps is None or max_bps <= 0:
        return None
    entries = []
    for level in levels:
        if len(level) < 2:
            continue
        price = _safe_float(level[0]) or 0.0
        size = _safe_float(level[1]) or 0.0
        if price <= 0 or size <= 0:
            continue
        entries.append((price, size))
    if not entries:
        return None
    best_price = entries[0][0]
    if best_price <= 0:
        return None
    target_avg = best_price * (1 + (max_bps / 10_000.0) * (1 if side == "buy" else -1))
    qty = 0.0
    cost = 0.0
    for price, size in entries:
        if side == "buy":
            if price <= target_avg:
                qty += size
                cost += price * size
                continue
            numerator = target_avg * qty - cost
            denom = price - target_avg
            if denom <= 0:
                continue
            allowed = numerator / denom if numerator > 0 else 0.0
            allowed = min(size, max(0.0, allowed))
            qty += allowed
            cost += price * allowed
            break
        else:
            if price >= target_avg:
                qty += size
                cost += price * size
                continue
            numerator = cost - target_avg * qty
            denom = target_avg - price
            if denom <= 0:
                continue
            allowed = numerator / denom if numerator > 0 else 0.0
            allowed = min(size, max(0.0, allowed))
            qty += allowed
            cost += price * allowed
            break
    return qty if qty > 0 else None


def suggest_expensive_leg(
    long_exchange: str,
    short_exchange: str,
    *,
    fee_table: Mapping[str, Mapping[str, float]],
    liquidity: Mapping[str, float],
) -> dict[str, object]:
    long_fee = float(fee_table.get(long_exchange, {}).get("taker", 0.0))
    short_fee = float(fee_table.get(short_exchange, {}).get("taker", 0.0))
    long_fee_bps = long_fee * 10_000.0
    short_fee_bps = short_fee * 10_000.0
    long_liq = float(liquidity.get(long_exchange, 0.0))
    short_liq = float(liquidity.get(short_exchange, 0.0))
    suggestion = "long"
    reason = "higher_taker_fee"
    fee_diff = long_fee_bps - short_fee_bps
    if abs(fee_diff) >= 1.0:
        suggestion = "long" if fee_diff > 0 else "short"
        reason = "higher_taker_fee"
    else:
        ratio = (long_liq / short_liq) if short_liq else float("inf")
        if ratio < 0.8:
            suggestion = "long"
            reason = "lower_liquidity"
        elif ratio > 1.25:
            suggestion = "short"
            reason = "lower_liquidity"
        else:
            suggestion = "long"
            reason = "tie_break"
    return {
        "suggested_leg": suggestion,
        "reason": reason,
        "taker_fee_bps": {"long": long_fee_bps, "short": short_fee_bps},
        "top3_liquidity_usd": {"long": long_liq, "short": short_liq},
    }


class ManualTradeManager:
    """Best-effort manual trade orchestration with dry-run support."""

    def __init__(
        self,
        *,
        fee_table: Mapping[str, Mapping[str, float]] | None = None,
        orderbook_depth: int = 20,
        liquidity_top_n: int = 3,
    ) -> None:
        self._fees = fee_table or EXCHANGE_COMMISSIONS
        self._orderbook_depth = max(5, int(orderbook_depth))
        self._liquidity_top_n = max(1, int(liquidity_top_n))
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}
        self._lock = asyncio.Lock()
        self._position_mode_cache: dict[str, tuple[bool | None, float]] = {}

    async def enter(
        self,
        payload: Mapping[str, Any],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        return await self._handle_pair(payload, action="enter", log_cb=log_cb)

    async def exit(
        self,
        payload: Mapping[str, Any],
        positions: Iterable[Mapping[str, Any]],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        return await self._handle_pair(payload, action="exit", positions=positions, log_cb=log_cb)

    async def roll(
        self,
        payload: Mapping[str, Any],
        positions: Iterable[Mapping[str, Any]],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        return await self._handle_pair(payload, action="roll", positions=positions, log_cb=log_cb)

    async def analyze(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action") or "enter")
        plan = await self._build_plan(payload, action=action)
        plan["suggested_mode"] = self._suggest_mode(plan)
        return plan

    async def _handle_pair(
        self,
        payload: Mapping[str, Any],
        *,
        action: str,
        positions: Iterable[Mapping[str, Any]] | None = None,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        async with self._lock:
            plan = await self._build_plan(payload, action=action, positions=positions)
            if payload.get("dry_run", False) or plan["errors"]:
                return plan
            mode = str(payload.get("mode") or "limit-first-expensive")
            if action == "exit" and (payload.get("spread_min_pct") is not None or payload.get("spread_max_pct") is not None):
                if mode not in ("smart-exit", "fast-exit"):
                    mode = "smart-exit"
                    plan.setdefault("warnings", []).append("mode overridden to smart-exit for spread-guided exit")
            if action == "enter" and (payload.get("spread_min_pct") is not None or payload.get("spread_max_pct") is not None):
                if mode not in ("smart-enter", "fast-enter"):
                    mode = "smart-enter"
                    plan.setdefault("warnings", []).append("mode overridden to smart-enter for spread-guided enter")
            return await self._execute_plan(plan, mode=mode, payload=payload, log_cb=log_cb)

    async def _build_plan(
        self,
        payload: Mapping[str, Any],
        *,
        action: str,
        positions: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        symbol = str(payload.get("symbol") or "").upper().strip()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        use_orderbook_check = bool(payload.get("use_orderbook_check", True))
        include_orderbooks = bool(payload.get("include_orderbooks", False))
        notional = _safe_float(payload.get("notional"))
        qty = _safe_float(payload.get("qty"))
        errors: list[str] = []
        warnings: list[str] = []

        if action == "roll":
            from_exchange = normalize_exchange_name(str(payload.get("from_exchange") or ""))
            to_exchange = normalize_exchange_name(str(payload.get("to_exchange") or ""))
            side = str(payload.get("side") or "").lower()
            if not from_exchange or not to_exchange:
                errors.append("from_exchange and to_exchange are required.")
            if side not in ("long", "short"):
                errors.append("side must be 'long' or 'short'.")
            long_exchange = to_exchange if side == "long" else to_exchange
            short_exchange = from_exchange if side == "long" else from_exchange

        if not symbol:
            errors.append("symbol is required.")
        if action != "roll":
            if not long_exchange or not short_exchange:
                errors.append("long_exchange and short_exchange are required.")
        if qty is None and notional is None:
            errors.append("qty or notional is required.")

        legs: list[dict[str, Any]] = []
        if action == "enter":
            legs = [
                {"label": "long", "exchange": long_exchange, "side": "buy", "reduce_only": False},
                {"label": "short", "exchange": short_exchange, "side": "sell", "reduce_only": False},
            ]
        elif action == "exit":
            legs = [
                {"label": "long", "exchange": long_exchange, "side": "sell", "reduce_only": True},
                {"label": "short", "exchange": short_exchange, "side": "buy", "reduce_only": True},
            ]
        elif action == "roll":
            side = str(payload.get("side") or "").lower()
            from_exchange = normalize_exchange_name(str(payload.get("from_exchange") or ""))
            to_exchange = normalize_exchange_name(str(payload.get("to_exchange") or ""))
            if side == "long":
                legs = [
                    {"label": "to", "exchange": to_exchange, "side": "buy", "reduce_only": False},
                    {"label": "from", "exchange": from_exchange, "side": "sell", "reduce_only": True},
                ]
            elif side == "short":
                legs = [
                    {"label": "to", "exchange": to_exchange, "side": "sell", "reduce_only": False},
                    {"label": "from", "exchange": from_exchange, "side": "buy", "reduce_only": True},
                ]
            else:
                legs = []

        if errors:
            return self._plan_response(payload, legs, errors, warnings, action=action)

        if qty is None and positions:
            inferred = self._infer_qty_from_positions(
                symbol,
                positions,
                action=action,
                long_exchange=long_exchange,
                short_exchange=short_exchange,
                payload=payload,
            )
            if inferred:
                qty = inferred
                warnings.append(f"qty inferred from positions: {qty:g}")

        if qty is None:
            qty = await self._resolve_qty_from_notional(
                symbol, notional, long_exchange, short_exchange
            )
            if qty is None or qty <= 0:
                errors.append("Unable to resolve qty from notional (missing market price).")
        if errors:
            return self._plan_response(payload, legs, errors, warnings, action=action)

        orderbooks: dict[str, dict[str, Any]] = {}
        stats_by_exchange: dict[str, OrderBookStats] = {}
        slippage_by_exchange: dict[str, dict[str, float | None]] = {}
        liquidity_map: dict[str, float] = {}
        ccxt_symbols: dict[str, str] = {}
        max_qty_by_exchange: dict[str, float | None] = {}
        market_constraints: dict[str, dict[str, float | None]] = {}

        for leg in legs:
            exchange = leg["exchange"]
            client = await self._ensure_client(exchange, errors)
            if not client:
                continue
            ccxt_symbol = await self._resolve_market_symbol(client, symbol)
            if not ccxt_symbol:
                errors.append(f"{exchange}: unable to resolve symbol {symbol}")
                continue
            ccxt_symbols[exchange] = ccxt_symbol
            try:
                orderbook = await client.fetch_order_book(ccxt_symbol, limit=self._orderbook_depth)
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(f"{exchange}: orderbook fetch failed: {exc}")
                continue
            constraints = self._extract_market_constraints(client, ccxt_symbol)
            contract_size = constraints.get("contract_size")
            scaled_orderbook = _scale_orderbook(orderbook, contract_size)
            orderbooks[exchange] = scaled_orderbook
            stats = orderbook_stats(scaled_orderbook, top_n=self._liquidity_top_n)
            stats_by_exchange[exchange] = stats
            liquidity_map[exchange] = stats.min_liquidity_top3
            if stats.mid:
                constraints["min_qty_required"] = _min_qty_required(
                    min_qty=constraints.get("min_qty"),
                    min_notional=constraints.get("min_notional"),
                    price=stats.mid,
                    amount_step=constraints.get("amount_step"),
                )
            market_constraints[exchange] = constraints
            levels = scaled_orderbook.get("asks") if leg["side"] == "buy" else scaled_orderbook.get("bids")
            fill = estimate_fill(levels or [], float(qty))
            slip = slippage_bps(stats.best_ask if leg["side"] == "buy" else stats.best_bid, fill["avg_price"], leg["side"])
            slippage_by_exchange[exchange] = {
                "expected_slippage_bps": slip,
                "filled_qty": fill["filled_qty"],
                "remaining_qty": fill["remaining_qty"],
                "avg_price": fill["avg_price"],
            }
            max_qty_by_exchange[exchange] = max_qty_for_slippage(
                levels or [],
                side=leg["side"],
                max_bps=max_slippage_bps,
            )
            if use_orderbook_check and fill["remaining_qty"] and fill["remaining_qty"] > 0:
                errors.append(f"{exchange}: insufficient liquidity for qty {qty:g}")
            if use_orderbook_check and slip is not None and max_slippage_bps > 0 and slip > max_slippage_bps:
                warnings.append(f"{exchange}: expected slippage {slip:.2f} bps exceeds max {max_slippage_bps:.2f}")

        suggestion = suggest_expensive_leg(
            long_exchange,
            short_exchange,
            fee_table=self._fees,
            liquidity=liquidity_map,
        )
        funding = await self._fetch_funding_meta(symbol, [leg["exchange"] for leg in legs])
        long_stats = stats_by_exchange.get(long_exchange)
        short_stats = stats_by_exchange.get(short_exchange)
        spread_val = spread_pct(
            long_stats.mid if long_stats else None,
            short_stats.mid if short_stats else None,
        )
        within_range = None
        if spread_min_pct is not None or spread_max_pct is not None:
            if spread_val is None:
                warnings.append("spread unavailable for range check")
            else:
                within_range = True
                if spread_min_pct is not None and spread_val < spread_min_pct:
                    within_range = False
                if spread_max_pct is not None and spread_val > spread_max_pct:
                    within_range = False
                if within_range is False:
                    warnings.append("spread outside configured range")
        max_qty_candidates = [val for val in max_qty_by_exchange.values() if val is not None]
        recommended_qty = min(max_qty_candidates) if max_qty_candidates else None
        recommended_notional = None
        if recommended_qty and short_stats and short_stats.mid:
            recommended_notional = recommended_qty * short_stats.mid
        min_chunk_candidates = [
            val.get("min_qty_required")
            for val in market_constraints.values()
            if val.get("min_qty_required")
        ]
        min_chunk_qty = max(min_chunk_candidates) if min_chunk_candidates else None
        recommended_chunk_qty = None
        if qty and qty > 0:
            candidate = qty
            if recommended_qty:
                candidate = min(candidate, recommended_qty)
            if min_chunk_qty and candidate < min_chunk_qty:
                warnings.append("min chunk exceeds recommended size; execution may require larger chunks")
                candidate = min_chunk_qty
            recommended_chunk_qty = candidate
        mid_candidates = [stats.mid for stats in stats_by_exchange.values() if stats.mid]
        mid_price = sum(mid_candidates) / len(mid_candidates) if mid_candidates else None
        auto_min_notional = None
        auto_min_qty = None
        if mid_price and qty:
            auto_min_notional = max(50.0, mid_price * qty * 0.01)
            auto_min_qty = auto_min_notional / mid_price if mid_price > 0 else None
        return {
            "dry_run": bool(payload.get("dry_run", False)),
            "action": action,
            "symbol": symbol,
            "qty": qty,
            "notional": notional,
            "mode": payload.get("mode"),
            "legs": legs,
            "orderbooks": orderbooks if include_orderbooks else {},
            "stats": {
                exch: {
                    "best_bid": stats_by_exchange[exch].best_bid,
                    "best_ask": stats_by_exchange[exch].best_ask,
                    "spread": stats_by_exchange[exch].spread,
                    "mid": stats_by_exchange[exch].mid,
                    "bid_liquidity_top3": stats_by_exchange[exch].bid_liquidity_top3,
                    "ask_liquidity_top3": stats_by_exchange[exch].ask_liquidity_top3,
                    "min_liquidity_top3": stats_by_exchange[exch].min_liquidity_top3,
                }
                for exch in stats_by_exchange
            },
            "slippage": slippage_by_exchange,
            "fees": self._fees,
            "suggested_expensive_leg": suggestion,
            "funding": funding,
            "ccxt_symbols": ccxt_symbols,
            "market_constraints": market_constraints,
            "spread_pct": spread_val,
            "spread_range": {"min": spread_min_pct, "max": spread_max_pct},
            "spread_within_range": within_range,
            "recommended_qty": recommended_qty,
            "recommended_notional": recommended_notional,
            "min_chunk_qty": min_chunk_qty,
            "recommended_chunk_qty": recommended_chunk_qty,
            "max_qty_by_exchange": max_qty_by_exchange,
            "auto_limit_defaults": {
                "min_level_notional": auto_min_notional,
                "min_level_qty": auto_min_qty,
                "max_limit_deviation_bps": 30.0,
            },
            "errors": errors,
            "warnings": warnings,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _execute_plan(
        self,
        plan: Mapping[str, Any],
        *,
        mode: str,
        payload: Mapping[str, Any],
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        legs = list(plan.get("legs") or [])
        symbol = str(plan.get("symbol") or "")
        qty = _safe_float(plan.get("qty")) or 0.0
        timeout = int(_safe_float(payload.get("timeout_sec")) or 15)
        expensive_leg = payload.get("expensive_leg")
        fallback_to_market = bool(payload.get("fallback_to_market", False))
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        spread_val = _safe_float(plan.get("spread_pct"))
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        if mode in ("smart-exit", "fast-exit", "smart-enter", "fast-enter"):
            action = plan.get("action")
            if action == "exit" and mode in ("smart-exit", "fast-exit"):
                if mode == "smart-exit":
                    return await self._execute_smart_exit(plan, payload, log_cb=log_cb)
                return await self._execute_fast_exit(plan, payload, log_cb=log_cb)
            if action == "enter" and mode in ("smart-enter", "fast-enter"):
                if mode == "smart-enter":
                    return await self._execute_smart_enter(plan, payload, log_cb=log_cb)
                return await self._execute_fast_enter(plan, payload, log_cb=log_cb)
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": mode,
                "actions": actions,
                "errors": [f"{mode} is not supported for action {action}."],
                "warnings": plan.get("warnings") or [],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        if spread_min_pct is not None or spread_max_pct is not None:
            if spread_val is None:
                errors.append("spread unavailable; cannot validate range")
            else:
                if spread_min_pct is not None and spread_val < spread_min_pct:
                    errors.append("spread below minimum threshold")
                if spread_max_pct is not None and spread_val > spread_max_pct:
                    errors.append("spread above maximum threshold")
        if errors:
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": mode,
                "actions": actions,
                "errors": errors,
                "warnings": plan.get("warnings") or [],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        leg_by_label = {leg["label"]: leg for leg in legs}
        if mode == "limit-first-expensive":
            limit_label = self._resolve_expensive_label(expensive_leg, leg_by_label)
            if not limit_label:
                limit_label = "long" if "long" in leg_by_label else legs[0]["label"]
            market_label = "short" if limit_label == "long" and "short" in leg_by_label else None
            if not market_label:
                market_label = next((leg["label"] for leg in legs if leg["label"] != limit_label), limit_label)
            limit_leg = leg_by_label.get(limit_label)
            market_leg = leg_by_label.get(market_label)
            if not limit_leg or not market_leg:
                errors.append("Unable to resolve legs for limit-first-expensive.")
            else:
                self._emit_log(log_cb, "submit", f"limit {limit_leg['exchange']} qty={qty:g}")
                limit_result = await self._place_limit_then_wait(
                    limit_leg,
                    symbol,
                    qty,
                    timeout,
                    payload,
                )
                actions.append(limit_result)
                self._emit_log(log_cb, "result", "limit result", limit_result)
                filled = _safe_float(limit_result.get("filled_qty")) or 0.0
                if filled > 0:
                    self._emit_log(log_cb, "submit", f"market hedge {market_leg['exchange']} qty={filled:g}")
                    market_result = await self._place_market(
                        market_leg,
                        symbol,
                        filled,
                        payload,
                    )
                    actions.append(market_result)
                    self._emit_log(log_cb, "result", "market hedge result", market_result)
                elif fallback_to_market:
                    self._emit_log(log_cb, "submit", f"fallback market {limit_leg['exchange']} qty={qty:g}")
                    market_result = await self._place_market(
                        limit_leg,
                        symbol,
                        qty,
                        payload,
                    )
                    actions.append(market_result)
                    self._emit_log(log_cb, "result", "fallback result", market_result)
        elif mode == "dual-market":
            for leg in legs:
                self._emit_log(log_cb, "submit", f"market {leg['exchange']} qty={qty:g}")
                actions.append(await self._place_market(leg, symbol, qty, payload))
        elif mode == "dual-limit":
            pending: list[dict[str, Any]] = []
            for leg in legs:
                self._emit_log(log_cb, "submit", f"limit {leg['exchange']} qty={qty:g}")
                pending.append(await self._place_limit_then_wait(leg, symbol, qty, timeout, payload))
            actions.extend(pending)
            if fallback_to_market:
                for result, leg in zip(pending, legs):
                    if result.get("status") in ("partial", "open"):
                        remaining = max(0.0, qty - (_safe_float(result.get("filled_qty")) or 0.0))
                        if remaining > 0:
                            self._emit_log(log_cb, "submit", f"fallback market {leg['exchange']} qty={remaining:g}")
                            actions.append(await self._place_market(leg, symbol, remaining, payload))
        elif mode == "limit-then-market-fallback":
            primary = legs[0] if legs else None
            if not primary:
                errors.append("No legs available for limit-then-market-fallback.")
            else:
                self._emit_log(log_cb, "submit", f"limit {primary['exchange']} qty={qty:g}")
                result = await self._place_limit_then_wait(primary, symbol, qty, timeout, payload)
                actions.append(result)
                self._emit_log(log_cb, "result", "limit result", result)
                filled = _safe_float(result.get("filled_qty")) or 0.0
                if filled <= 0 and fallback_to_market:
                    self._emit_log(log_cb, "submit", f"fallback market {primary['exchange']} qty={qty:g}")
                    actions.append(await self._place_market(primary, symbol, qty, payload))
        else:
            errors.append(f"Unsupported mode '{mode}'.")

        return {
            "dry_run": False,
            "action": plan.get("action"),
            "symbol": symbol,
            "qty": qty,
            "mode": mode,
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": plan.get("warnings") or [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _execute_smart_exit(
        self,
        plan: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        legs = list(plan.get("legs") or [])
        symbol = str(plan.get("symbol") or "")
        qty = _safe_float(plan.get("qty")) or 0.0
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        timeout = int(_safe_float(payload.get("timeout_sec")) or 10)
        reprice_sec = _safe_float(payload.get("reprice_sec")) or 2.0
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 60)
        limit_offset_bps = _safe_float(payload.get("limit_offset_bps")) or 0.0
        limit_offset_ticks = int(_safe_float(payload.get("limit_offset_ticks")) or 0)
        hedge_order_type = str(payload.get("hedge_order_type") or "market").lower()
        hedge_offset_bps = _safe_float(payload.get("hedge_offset_bps")) or 2.0
        hedge_offset_ticks = int(_safe_float(payload.get("hedge_offset_ticks")) or 0)
        max_unhedged_sec = _safe_float(payload.get("max_unhedged_sec")) or 8.0
        max_unhedged_pct = _pct_to_fraction(_safe_float(payload.get("max_unhedged_pct")))
        fallback_to_market = bool(payload.get("fallback_to_market", False))

        leg_by_label = {leg["label"]: leg for leg in legs}
        expensive_label = self._resolve_expensive_label(payload.get("expensive_leg"), leg_by_label)
        if not expensive_label:
            expensive_label = "long" if "long" in leg_by_label else legs[0]["label"] if legs else ""
        primary_leg = leg_by_label.get(expensive_label)
        hedge_leg = next((leg for leg in legs if leg is not primary_leg), None)
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])

        if not primary_leg or not hedge_leg:
            errors.append("Unable to resolve primary/hedge legs for smart exit.")
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": "smart-exit",
                "actions": actions,
                "errors": errors,
                "warnings": warnings,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        constraints = plan.get("market_constraints") or {}
        amount_steps = [
            (constraints.get(primary_leg["exchange"]) or {}).get("amount_step"),
            (constraints.get(hedge_leg["exchange"]) or {}).get("amount_step"),
        ]
        amount_step = max([step for step in amount_steps if step], default=None)
        min_chunk_candidates = [
            (constraints.get(primary_leg["exchange"]) or {}).get("min_qty_required"),
            (constraints.get(hedge_leg["exchange"]) or {}).get("min_qty_required"),
        ]
        min_chunk_qty = max([val for val in min_chunk_candidates if val], default=None)
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))

        remaining = qty
        unhedged_qty = 0.0
        unhedged_since: float | None = None
        started_at = time.time()
        active_order_id: str | None = None
        active_price: float | None = None
        active_qty: float | None = None
        active_filled = 0.0
        active_since: float | None = None

        while remaining > 0 and (time.time() - started_at) < max_runtime_sec:
            snapshot = await self._snapshot_legs(symbol, [primary_leg, hedge_leg], max_slippage_bps=max_slippage_bps)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "orderbook fetch failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, reprice_sec))
                continue
            spread_val = snapshot.get("spread_pct")
            within_range = self._within_spread(spread_val, spread_min_pct, spread_max_pct)
            if within_range is False:
                if active_order_id:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                self._emit_log(log_cb, "wait", f"spread {spread_val:.4f} out of range; waiting")
                if unhedged_qty > 0:
                    hedge_result = await self._hedge_position(
                        hedge_leg,
                        symbol,
                        unhedged_qty,
                        hedge_order_type=hedge_order_type,
                        hedge_offset_bps=hedge_offset_bps,
                        hedge_offset_ticks=hedge_offset_ticks,
                        fallback_to_market=fallback_to_market,
                    )
                    actions.append(hedge_result)
                    self._emit_log(log_cb, "result", "hedge result", hedge_result)
                    unhedged_qty = 0.0
                    unhedged_since = None
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            max_qty_by_exchange = snapshot.get("max_qty_by_exchange") or {}
            max_chunk = None
            max_candidates = [val for val in max_qty_by_exchange.values() if val]
            if max_candidates:
                max_chunk = min(max_candidates)
            if max_slippage_bps > 0 and max_chunk is not None:
                if max_chunk <= 0:
                    self._emit_log(log_cb, "wait", "liquidity below slippage cap; waiting")
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                if min_chunk_qty and max_chunk < min_chunk_qty:
                    self._emit_log(
                        log_cb,
                        "wait",
                        f"liquidity below min chunk (max {max_chunk:g} < min {min_chunk_qty:g}); waiting",
                    )
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
            mid_price = snapshot.get("mid_price") or 0.0
            requested = requested_chunk
            if requested is None and chunk_notional and mid_price:
                requested = chunk_notional / mid_price
            chunk, chunk_warnings = _choose_chunk_qty(
                remaining=remaining,
                requested_qty=requested,
                min_chunk=min_chunk_qty,
                max_chunk=max_chunk,
                amount_step=amount_step,
            )
            warnings.extend(chunk_warnings)
            if not chunk:
                errors.append("Unable to determine chunk size for smart exit.")
                break

            price_step = (constraints.get(primary_leg["exchange"]) or {}).get("price_step")
            primary_stats = (snapshot.get("stats") or {}).get(primary_leg["exchange"])
            base_price = None
            if primary_stats:
                base_price = primary_stats.best_bid if primary_leg["side"] == "buy" else primary_stats.best_ask
            if base_price is None:
                base_price = snapshot.get("primary_best")
            limit_price = _apply_price_offset(
                base_price,
                side=primary_leg["side"],
                offset_bps=limit_offset_bps,
                offset_ticks=limit_offset_ticks,
                price_step=price_step,
                round_mode="passive",
            )
            if limit_price is None:
                errors.append("Unable to resolve limit price for smart exit.")
                break

            if active_order_id:
                if active_price != limit_price or (active_qty is not None and active_qty != chunk):
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None

            if active_order_id is None:
                self._emit_log(
                    log_cb,
                    "submit",
                    f"limit {primary_leg['exchange']} qty={chunk:g} price={limit_price:g}",
                )
                submit = await self._submit_order(
                    primary_leg,
                    symbol,
                    chunk,
                    "limit",
                    price=limit_price,
                    reduce_only=bool(primary_leg.get("reduce_only")),
                )
                actions.append(submit)
                if submit.get("status") == "error":
                    errors.append(submit.get("error") or "limit_submit_failed")
                    break
                order_id = submit.get("order_id")
                if not order_id:
                    errors.append("missing_order_id")
                    break
                active_order_id = order_id
                active_price = limit_price
                active_qty = chunk
                active_filled = 0.0
                active_since = time.time()

            if active_order_id:
                if active_since and (time.time() - active_since) > timeout:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                limit_wait_sec = max(1, int(reprice_sec or 1))
                limit_result = await self._wait_for_order_with_spread(
                    primary_leg,
                    symbol,
                    active_order_id,
                    limit_wait_sec,
                    spread_min_pct,
                    spread_max_pct,
                    [primary_leg, hedge_leg],
                    reprice_sec,
                    cancel_on_timeout=False,
                )
                self._emit_log(log_cb, "result", "limit result", limit_result)
                filled_qty = _safe_float(limit_result.get("filled_qty")) or 0.0
                delta = max(0.0, filled_qty - active_filled)
                if delta > 0:
                    active_filled = filled_qty
                    remaining = max(0.0, remaining - delta)
                    unhedged_qty += delta
                    if unhedged_since is None:
                        unhedged_since = time.time()
                if limit_result.get("status") in ("filled", "closed"):
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                if limit_result.get("cancel_reason"):
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None

            unhedged_age = time.time() - unhedged_since if unhedged_since else 0.0
            if unhedged_qty > 0:
                trigger_pct = max_unhedged_pct is not None and qty > 0 and (unhedged_qty / qty) >= max_unhedged_pct
                trigger_sec = max_unhedged_sec is not None and unhedged_age >= max_unhedged_sec
                if trigger_pct or trigger_sec or remaining <= 0:
                    hedge_result = await self._hedge_position(
                        hedge_leg,
                        symbol,
                        unhedged_qty,
                        hedge_order_type=hedge_order_type,
                        hedge_offset_bps=hedge_offset_bps,
                        hedge_offset_ticks=hedge_offset_ticks,
                        fallback_to_market=fallback_to_market,
                    )
                    actions.append(hedge_result)
                    self._emit_log(log_cb, "result", "hedge result", hedge_result)
                    unhedged_qty = 0.0
                    unhedged_since = None

            if remaining <= 0:
                break
            await asyncio.sleep(max(0.2, reprice_sec))

        if unhedged_qty > 0:
            hedge_result = await self._hedge_position(
                hedge_leg,
                symbol,
                unhedged_qty,
                hedge_order_type=hedge_order_type,
                hedge_offset_bps=hedge_offset_bps,
                hedge_offset_ticks=hedge_offset_ticks,
                fallback_to_market=fallback_to_market,
            )
            actions.append(hedge_result)
            self._emit_log(log_cb, "result", "hedge result", hedge_result)
            unhedged_qty = 0.0

        if active_order_id:
            await self._cancel_order(primary_leg, symbol, active_order_id)
            active_order_id = None

        if remaining > 0 and fallback_to_market:
            actions.append(await self._place_market(primary_leg, symbol, remaining, payload))
            actions.append(await self._place_market(hedge_leg, symbol, remaining, payload))
            remaining = 0.0

        if remaining > 0 and not fallback_to_market:
            warnings.append(f"Remaining qty {remaining:g} not exited (smart-exit runtime ended).")

        return {
            "dry_run": False,
            "action": plan.get("action"),
            "symbol": symbol,
            "qty": qty,
            "mode": "smart-exit",
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": warnings,
            "remaining_qty": remaining,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _execute_fast_exit(
        self,
        plan: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        legs = list(plan.get("legs") or [])
        symbol = str(plan.get("symbol") or "")
        qty = _safe_float(plan.get("qty")) or 0.0
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        reprice_sec = _safe_float(payload.get("reprice_sec")) or 0.5
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 20)
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        constraints = plan.get("market_constraints") or {}
        amount_steps = [
            (constraints.get(leg["exchange"]) or {}).get("amount_step")
            for leg in legs
        ]
        amount_step = max([step for step in amount_steps if step], default=None)
        min_chunk_candidates = [
            (constraints.get(leg["exchange"]) or {}).get("min_qty_required")
            for leg in legs
        ]
        min_chunk_qty = max([val for val in min_chunk_candidates if val], default=None)

        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])
        remaining = qty
        started_at = time.time()

        while remaining > 0 and (time.time() - started_at) < max_runtime_sec:
            snapshot = await self._snapshot_legs(symbol, legs, max_slippage_bps=max_slippage_bps)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "orderbook fetch failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, reprice_sec))
                continue
            spread_val = snapshot.get("spread_pct")
            within_range = self._within_spread(spread_val, spread_min_pct, spread_max_pct)
            if within_range is False:
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            max_qty_by_exchange = snapshot.get("max_qty_by_exchange") or {}
            max_candidates = [val for val in max_qty_by_exchange.values() if val]
            max_chunk = min(max_candidates) if max_candidates else None
            if max_slippage_bps > 0 and max_chunk is not None:
                if max_chunk <= 0:
                    self._emit_log(log_cb, "wait", "liquidity below slippage cap; waiting")
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                if min_chunk_qty and max_chunk < min_chunk_qty:
                    self._emit_log(
                        log_cb,
                        "wait",
                        f"liquidity below min chunk (max {max_chunk:g} < min {min_chunk_qty:g}); waiting",
                    )
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
            mid_price = snapshot.get("mid_price") or 0.0
            requested = requested_chunk
            if requested is None and chunk_notional and mid_price:
                requested = chunk_notional / mid_price
            chunk, chunk_warnings = _choose_chunk_qty(
                remaining=remaining,
                requested_qty=requested,
                min_chunk=min_chunk_qty,
                max_chunk=max_chunk,
                amount_step=amount_step,
            )
            warnings.extend(chunk_warnings)
            if not chunk:
                errors.append("Unable to determine chunk size for fast exit.")
                break
            for leg in legs:
                self._emit_log(log_cb, "submit", f"market {leg['exchange']} qty={chunk:g}")
                actions.append(await self._place_market(leg, symbol, chunk, payload))
            remaining = max(0.0, remaining - chunk)
            await asyncio.sleep(0.2)

        if remaining > 0:
            warnings.append(f"Remaining qty {remaining:g} not exited (fast-exit runtime ended).")

        return {
            "dry_run": False,
            "action": plan.get("action"),
            "symbol": symbol,
            "qty": qty,
            "mode": "fast-exit",
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": warnings,
            "remaining_qty": remaining,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _execute_smart_enter(
        self,
        plan: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        legs = list(plan.get("legs") or [])
        symbol = str(plan.get("symbol") or "")
        qty = _safe_float(plan.get("qty")) or 0.0
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        timeout = int(_safe_float(payload.get("timeout_sec")) or 10)
        reprice_sec = _safe_float(payload.get("reprice_sec")) or 2.0
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 60)
        limit_offset_bps = _safe_float(payload.get("limit_offset_bps")) or 0.0
        limit_offset_ticks = int(_safe_float(payload.get("limit_offset_ticks")) or 0)
        hedge_order_type = str(payload.get("hedge_order_type") or "market").lower()
        hedge_offset_bps = _safe_float(payload.get("hedge_offset_bps")) or 2.0
        hedge_offset_ticks = int(_safe_float(payload.get("hedge_offset_ticks")) or 0)
        max_unhedged_sec = _safe_float(payload.get("max_unhedged_sec")) or 4.0
        max_unhedged_pct = _pct_to_fraction(_safe_float(payload.get("max_unhedged_pct")))
        fallback_to_market = bool(payload.get("fallback_to_market", False))

        leg_by_label = {leg["label"]: leg for leg in legs}
        expensive_label = self._resolve_expensive_label(payload.get("expensive_leg"), leg_by_label)
        if not expensive_label:
            suggested = (plan.get("suggested_expensive_leg") or {}).get("suggested_leg")
            if suggested in leg_by_label:
                expensive_label = suggested
        if not expensive_label:
            expensive_label = "long" if "long" in leg_by_label else legs[0]["label"] if legs else ""
        primary_leg = leg_by_label.get(expensive_label)
        hedge_leg = next((leg for leg in legs if leg is not primary_leg), None)
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])

        if not primary_leg or not hedge_leg:
            errors.append("Unable to resolve primary/hedge legs for smart enter.")
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": "smart-enter",
                "actions": actions,
                "errors": errors,
                "warnings": warnings,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        constraints = plan.get("market_constraints") or {}
        amount_steps = [
            (constraints.get(primary_leg["exchange"]) or {}).get("amount_step"),
            (constraints.get(hedge_leg["exchange"]) or {}).get("amount_step"),
        ]
        amount_step = max([step for step in amount_steps if step], default=None)
        min_chunk_candidates = [
            (constraints.get(primary_leg["exchange"]) or {}).get("min_qty_required"),
            (constraints.get(hedge_leg["exchange"]) or {}).get("min_qty_required"),
        ]
        min_chunk_qty = max([val for val in min_chunk_candidates if val], default=None)
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))

        remaining = qty
        unhedged_qty = 0.0
        unhedged_since: float | None = None
        started_at = time.time()
        active_order_id: str | None = None
        active_price: float | None = None
        active_qty: float | None = None
        active_filled = 0.0
        active_since: float | None = None

        while remaining > 0 and (time.time() - started_at) < max_runtime_sec:
            snapshot = await self._snapshot_legs(symbol, [primary_leg, hedge_leg], max_slippage_bps=max_slippage_bps)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "orderbook fetch failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, reprice_sec))
                continue
            spread_val = snapshot.get("spread_pct")
            within_range = self._within_spread(spread_val, spread_min_pct, spread_max_pct)
            if within_range is False:
                if active_order_id:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                self._emit_log(log_cb, "wait", f"spread {spread_val:.4f} out of range; waiting")
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            max_qty_by_exchange = snapshot.get("max_qty_by_exchange") or {}
            max_chunk = None
            max_candidates = [val for val in max_qty_by_exchange.values() if val]
            if max_candidates:
                max_chunk = min(max_candidates)
            if max_slippage_bps > 0 and max_chunk is not None:
                if max_chunk <= 0:
                    self._emit_log(log_cb, "wait", "liquidity below slippage cap; waiting")
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                if min_chunk_qty and max_chunk < min_chunk_qty:
                    self._emit_log(
                        log_cb,
                        "wait",
                        f"liquidity below min chunk (max {max_chunk:g} < min {min_chunk_qty:g}); waiting",
                    )
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue

            mid_price = snapshot.get("mid_price") or 0.0
            requested = requested_chunk
            if requested is None and chunk_notional and mid_price:
                requested = chunk_notional / mid_price
            chunk, chunk_warnings = _choose_chunk_qty(
                remaining=remaining,
                requested_qty=requested,
                min_chunk=min_chunk_qty,
                max_chunk=max_chunk,
                amount_step=amount_step,
            )
            warnings.extend(chunk_warnings)
            if not chunk:
                errors.append("Unable to determine chunk size for smart enter.")
                break

            price_step = (constraints.get(primary_leg["exchange"]) or {}).get("price_step")
            primary_stats = (snapshot.get("stats") or {}).get(primary_leg["exchange"])
            base_price = None
            if primary_stats:
                base_price = primary_stats.best_bid if primary_leg["side"] == "buy" else primary_stats.best_ask
            if base_price is None:
                base_price = snapshot.get("primary_best")
            limit_price = _apply_price_offset(
                base_price,
                side=primary_leg["side"],
                offset_bps=limit_offset_bps,
                offset_ticks=limit_offset_ticks,
                price_step=price_step,
                round_mode="passive",
            )
            if limit_price is None:
                errors.append("Unable to resolve limit price for smart enter.")
                break

            if active_order_id:
                if active_price != limit_price or (active_qty is not None and active_qty != chunk):
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None

            if active_order_id is None:
                self._emit_log(
                    log_cb,
                    "submit",
                    f"limit {primary_leg['exchange']} qty={chunk:g} price={limit_price:g}",
                )
                submit = await self._submit_order(
                    primary_leg,
                    symbol,
                    chunk,
                    "limit",
                    price=limit_price,
                    reduce_only=bool(primary_leg.get("reduce_only")),
                )
                actions.append(submit)
                if submit.get("status") == "error":
                    errors.append(submit.get("error") or "limit_submit_failed")
                    break
                order_id = submit.get("order_id")
                if not order_id:
                    errors.append("missing_order_id")
                    break
                active_order_id = order_id
                active_price = limit_price
                active_qty = chunk
                active_filled = 0.0
                active_since = time.time()

            if active_order_id:
                if active_since and (time.time() - active_since) > timeout:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                limit_wait_sec = max(1, int(reprice_sec or 1))
                limit_result = await self._wait_for_order_with_spread(
                    primary_leg,
                    symbol,
                    active_order_id,
                    limit_wait_sec,
                    spread_min_pct,
                    spread_max_pct,
                    [primary_leg, hedge_leg],
                    reprice_sec,
                    cancel_on_timeout=False,
                )
                self._emit_log(log_cb, "result", "limit result", limit_result)
                filled_qty = _safe_float(limit_result.get("filled_qty")) or 0.0
                delta = max(0.0, filled_qty - active_filled)
                if delta > 0:
                    active_filled = filled_qty
                    remaining = max(0.0, remaining - delta)
                    self._emit_log(log_cb, "submit", f"hedge {hedge_leg['exchange']} qty={delta:g}")
                    hedge_result = await self._hedge_position(
                        hedge_leg,
                        symbol,
                        delta,
                        hedge_order_type=hedge_order_type,
                        hedge_offset_bps=hedge_offset_bps,
                        hedge_offset_ticks=hedge_offset_ticks,
                        fallback_to_market=fallback_to_market,
                    )
                    actions.append(hedge_result)
                    self._emit_log(log_cb, "result", "hedge result", hedge_result)

                    unhedged = 0.0
                    if hedge_order_type == "limit":
                        if hedge_result.get("fallback"):
                            unhedged = 0.0
                        elif hedge_result.get("status") == "error":
                            unhedged = delta
                        else:
                            hedge_filled = _safe_float(hedge_result.get("filled_qty")) or 0.0
                            unhedged = max(0.0, delta - hedge_filled)
                    else:
                        if hedge_result.get("status") == "error":
                            unhedged = delta

                    unhedged_qty = unhedged
                    if unhedged_qty > 0:
                        if unhedged_since is None:
                            unhedged_since = time.time()
                    else:
                        unhedged_since = None

                    unhedged_age = time.time() - unhedged_since if unhedged_since else 0.0
                    if unhedged_qty > 0:
                        trigger_pct = max_unhedged_pct is not None and qty > 0 and (unhedged_qty / qty) >= max_unhedged_pct
                        trigger_sec = max_unhedged_sec is not None and unhedged_age >= max_unhedged_sec
                        if trigger_pct or trigger_sec:
                            self._emit_log(log_cb, "submit", f"force hedge market {hedge_leg['exchange']} qty={unhedged_qty:g}")
                            actions.append(await self._place_market(hedge_leg, symbol, unhedged_qty, payload))
                            unhedged_qty = 0.0
                            unhedged_since = None
                if limit_result.get("status") in ("filled", "closed"):
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                if limit_result.get("cancel_reason"):
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None

            if remaining <= 0:
                break
            await asyncio.sleep(max(0.2, reprice_sec))

        if unhedged_qty > 0:
            self._emit_log(log_cb, "submit", f"final hedge market {hedge_leg['exchange']} qty={unhedged_qty:g}")
            actions.append(await self._place_market(hedge_leg, symbol, unhedged_qty, payload))
            unhedged_qty = 0.0

        if active_order_id:
            await self._cancel_order(primary_leg, symbol, active_order_id)
            active_order_id = None

        if remaining > 0 and fallback_to_market:
            self._emit_log(log_cb, "submit", f"fallback market {primary_leg['exchange']} qty={remaining:g}")
            actions.append(await self._place_market(primary_leg, symbol, remaining, payload))
            self._emit_log(log_cb, "submit", f"fallback market {hedge_leg['exchange']} qty={remaining:g}")
            actions.append(await self._place_market(hedge_leg, symbol, remaining, payload))
            remaining = 0.0

        if remaining > 0 and not fallback_to_market:
            warnings.append(f"Remaining qty {remaining:g} not entered (smart-enter runtime ended).")

        return {
            "dry_run": False,
            "action": plan.get("action"),
            "symbol": symbol,
            "qty": qty,
            "mode": "smart-enter",
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": warnings,
            "remaining_qty": remaining,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _execute_fast_enter(
        self,
        plan: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        legs = list(plan.get("legs") or [])
        symbol = str(plan.get("symbol") or "")
        qty = _safe_float(plan.get("qty")) or 0.0
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        reprice_sec = _safe_float(payload.get("reprice_sec")) or 0.5
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 20)
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        constraints = plan.get("market_constraints") or {}
        amount_steps = [
            (constraints.get(leg["exchange"]) or {}).get("amount_step")
            for leg in legs
        ]
        amount_step = max([step for step in amount_steps if step], default=None)
        min_chunk_candidates = [
            (constraints.get(leg["exchange"]) or {}).get("min_qty_required")
            for leg in legs
        ]
        min_chunk_qty = max([val for val in min_chunk_candidates if val], default=None)

        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])
        remaining = qty
        started_at = time.time()

        while remaining > 0 and (time.time() - started_at) < max_runtime_sec:
            snapshot = await self._snapshot_legs(symbol, legs, max_slippage_bps=max_slippage_bps)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "orderbook fetch failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, reprice_sec))
                continue
            spread_val = snapshot.get("spread_pct")
            within_range = self._within_spread(spread_val, spread_min_pct, spread_max_pct)
            if within_range is False:
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            max_qty_by_exchange = snapshot.get("max_qty_by_exchange") or {}
            max_candidates = [val for val in max_qty_by_exchange.values() if val]
            max_chunk = min(max_candidates) if max_candidates else None
            if max_slippage_bps > 0 and max_chunk is not None:
                if max_chunk <= 0:
                    self._emit_log(log_cb, "wait", "liquidity below slippage cap; waiting")
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                if min_chunk_qty and max_chunk < min_chunk_qty:
                    self._emit_log(
                        log_cb,
                        "wait",
                        f"liquidity below min chunk (max {max_chunk:g} < min {min_chunk_qty:g}); waiting",
                    )
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue

            mid_price = snapshot.get("mid_price") or 0.0
            requested = requested_chunk
            if requested is None and chunk_notional and mid_price:
                requested = chunk_notional / mid_price
            chunk, chunk_warnings = _choose_chunk_qty(
                remaining=remaining,
                requested_qty=requested,
                min_chunk=min_chunk_qty,
                max_chunk=max_chunk,
                amount_step=amount_step,
            )
            warnings.extend(chunk_warnings)
            if not chunk:
                errors.append("Unable to determine chunk size for fast enter.")
                break
            for leg in legs:
                self._emit_log(log_cb, "submit", f"market {leg['exchange']} qty={chunk:g}")
                actions.append(await self._place_market(leg, symbol, chunk, payload))
            remaining = max(0.0, remaining - chunk)
            await asyncio.sleep(0.2)

        if remaining > 0:
            warnings.append(f"Remaining qty {remaining:g} not entered (fast-enter runtime ended).")

        return {
            "dry_run": False,
            "action": plan.get("action"),
            "symbol": symbol,
            "qty": qty,
            "mode": "fast-enter",
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": warnings,
            "remaining_qty": remaining,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _place_limit_then_wait(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        timeout: int,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        limit_price = self._resolve_limit_price(leg, payload)
        if limit_price is None and bool(payload.get("auto_limit_price", True)):
            limit_price = await self._resolve_dynamic_limit_price(leg, symbol, qty, payload)
        if limit_price is None:
            return {"exchange": leg["exchange"], "status": "error", "error": "missing_limit_price"}
        submit = await self._submit_order(
            leg, symbol, qty, "limit", price=limit_price, reduce_only=bool(leg.get("reduce_only"))
        )
        if submit.get("status") == "error":
            return submit
        order_id = submit.get("order_id")
        if not order_id:
            return {"exchange": leg["exchange"], "status": "error", "error": "missing_order_id"}
        status = await self._wait_for_order(leg, symbol, order_id, timeout)
        filled_qty = _safe_float(status.get("filled_qty")) or 0.0
        return {
            "exchange": leg["exchange"],
            "status": status.get("status"),
            "order_id": order_id,
            "filled_qty": filled_qty,
            "avg_price": status.get("avg_price"),
        }

    async def _place_limit_at(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        price: float,
        timeout: int,
    ) -> dict[str, Any]:
        submit = await self._submit_order(
            leg, symbol, qty, "limit", price=price, reduce_only=bool(leg.get("reduce_only"))
        )
        if submit.get("status") == "error":
            return submit
        order_id = submit.get("order_id")
        if not order_id:
            return {"exchange": leg["exchange"], "status": "error", "error": "missing_order_id"}
        status = await self._wait_for_order(leg, symbol, order_id, timeout)
        filled_qty = _safe_float(status.get("filled_qty")) or 0.0
        return {
            "exchange": leg["exchange"],
            "status": status.get("status"),
            "order_id": order_id,
            "filled_qty": filled_qty,
            "avg_price": status.get("avg_price"),
        }

    async def _place_limit_then_wait_with_spread(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        price: float,
        timeout: int,
        spread_min_pct: float | None,
        spread_max_pct: float | None,
        spread_legs: list[Mapping[str, Any]],
        reprice_sec: float | None,
    ) -> dict[str, Any]:
        submit = await self._submit_order(
            leg, symbol, qty, "limit", price=price, reduce_only=bool(leg.get("reduce_only"))
        )
        if submit.get("status") == "error":
            return submit
        order_id = submit.get("order_id")
        if not order_id:
            return {"exchange": leg["exchange"], "status": "error", "error": "missing_order_id"}
        status = await self._wait_for_order_with_spread(
            leg,
            symbol,
            order_id,
            timeout,
            spread_min_pct,
            spread_max_pct,
            spread_legs,
            reprice_sec,
        )
        filled_qty = _safe_float(status.get("filled_qty")) or 0.0
        result = {
            "exchange": leg["exchange"],
            "status": status.get("status"),
            "order_id": order_id,
            "filled_qty": filled_qty,
            "avg_price": status.get("avg_price"),
        }
        if status.get("cancel_reason"):
            result["cancel_reason"] = status.get("cancel_reason")
        return result

    async def _place_market(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        submit = await self._submit_order(
            leg, symbol, qty, "market", price=None, reduce_only=bool(leg.get("reduce_only"))
        )
        return submit

    async def _submit_order(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        order_type: str,
        *,
        price: float | None,
        reduce_only: bool,
    ) -> dict[str, Any]:
        exchange = leg["exchange"]
        client = await self._ensure_client(exchange, [])
        if not client:
            return {"exchange": exchange, "status": "error", "error": "client_unavailable"}
        ccxt_symbol = await self._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return {"exchange": exchange, "status": "error", "error": "symbol_unavailable"}
        try:
            market = client.markets.get(ccxt_symbol) if getattr(client, "markets", None) else None
        except Exception:
            market = None
        contract_size = _safe_float(market.get("contractSize")) if isinstance(market, dict) else None
        order_qty = float(qty)
        if contract_size and contract_size > 0:
            order_qty = order_qty / contract_size
        params = {}
        if reduce_only:
            params["reduceOnly"] = True
        if exchange == "bitget":
            hedged = await self._resolve_bitget_hedged(client)
            if hedged is True:
                params["hedged"] = hedged
        if exchange == "okx":
            hedged = await self._resolve_okx_hedged(client)
            if hedged is True:
                params["hedged"] = hedged
        try:
            order = await client.create_order(
                ccxt_symbol,
                order_type,
                leg["side"],
                order_qty,
                price if order_type == "limit" else None,
                params,
            )
            filled = _to_base_qty(_safe_float(order.get("filled")), contract_size)
            return {
                "exchange": exchange,
                "status": "submitted",
                "order_id": order.get("id"),
                "filled_qty": filled,
                "avg_price": order.get("average"),
            }
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc)
            if exchange == "bitget" and "40774" in message and "hedged" not in params:
                retry_params = dict(params)
                retry_params["hedged"] = True
                try:
                    order = await client.create_order(
                        ccxt_symbol,
                        order_type,
                        leg["side"],
                        order_qty,
                        price if order_type == "limit" else None,
                        retry_params,
                    )
                    filled = _to_base_qty(_safe_float(order.get("filled")), contract_size)
                    return {
                        "exchange": exchange,
                        "status": "submitted",
                        "order_id": order.get("id"),
                        "filled_qty": filled,
                        "avg_price": order.get("average"),
                    }
                except Exception as retry_exc:  # pylint: disable=broad-except
                    message = str(retry_exc)
            return {"exchange": exchange, "status": "error", "error": message}

    async def _resolve_bitget_hedged(self, client: Any) -> bool | None:
        now = time.time()
        cached = self._position_mode_cache.get("bitget")
        if cached and (now - cached[1]) < 30:
            return cached[0]
        hedged: bool | None = None
        try:
            positions = await client.fetch_positions()
        except Exception:  # pylint: disable=broad-except
            positions = []
        for pos in positions or []:
            if pos.get("hedged") is not None:
                hedged = bool(pos.get("hedged"))
                break
        if hedged is None:
            for pos in positions or []:
                info = pos.get("info") or {}
                mode = info.get("posMode") or info.get("holdMode")
                if mode == "hedge_mode":
                    hedged = True
                    break
                if mode == "one_way_mode":
                    hedged = False
                    break
        self._position_mode_cache["bitget"] = (hedged, now)
        return hedged

    async def _resolve_okx_hedged(self, client: Any) -> bool | None:
        now = time.time()
        cached = self._position_mode_cache.get("okx")
        if cached and (now - cached[1]) < 30:
            return cached[0]
        hedged: bool | None = None
        try:
            if hasattr(client, "fetch_position_mode"):
                mode = await client.fetch_position_mode()
                if isinstance(mode, dict) and mode.get("hedged") is not None:
                    hedged = bool(mode.get("hedged"))
        except Exception:  # pylint: disable=broad-except
            hedged = None
        if hedged is None:
            try:
                positions = await client.fetch_positions()
            except Exception:  # pylint: disable=broad-except
                positions = []
            for pos in positions or []:
                if pos.get("hedged") is not None:
                    hedged = bool(pos.get("hedged"))
                    break
                info = pos.get("info") or {}
                pos_side = str(info.get("posSide") or "").lower()
                if pos_side in ("long", "short"):
                    hedged = True
                    break
                if pos_side == "net":
                    hedged = False
                    break
        self._position_mode_cache["okx"] = (hedged, now)
        return hedged

    async def _wait_for_order(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        order_id: str,
        timeout: int,
    ) -> dict[str, Any]:
        exchange = leg["exchange"]
        client = await self._ensure_client(exchange, [])
        if not client:
            return {"status": "error", "error": "client_unavailable"}
        ccxt_symbol = await self._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return {"status": "error", "error": "symbol_unavailable"}
        try:
            market = client.markets.get(ccxt_symbol) if getattr(client, "markets", None) else None
        except Exception:
            market = None
        contract_size = _safe_float(market.get("contractSize")) if isinstance(market, dict) else None
        deadline = time.time() + max(1, int(timeout))
        last_fill = 0.0
        last_avg = None
        while time.time() < deadline:
            try:
                order = await client.fetch_order(order_id, ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                break
            status = str(order.get("status") or "").lower()
            filled = _to_base_qty(_safe_float(order.get("filled")), contract_size) or 0.0
            avg = _safe_float(order.get("average"))
            last_fill = filled or last_fill
            last_avg = avg or last_avg
            if status in ("closed", "filled"):
                return {"status": "filled", "filled_qty": filled, "avg_price": avg}
            if filled > 0:
                await asyncio.sleep(1)
                continue
            await asyncio.sleep(1)
        # timeout, attempt cancel
        try:
            await client.cancel_order(order_id, ccxt_symbol)
        except Exception:  # pylint: disable=broad-except
            pass
        if last_fill > 0:
            return {"status": "partial", "filled_qty": last_fill, "avg_price": last_avg}
        return {"status": "open", "filled_qty": last_fill, "avg_price": last_avg}

    async def _cancel_order(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        order_id: str,
    ) -> None:
        exchange = leg["exchange"]
        client = await self._ensure_client(exchange, [])
        if not client:
            return
        ccxt_symbol = await self._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return
        try:
            await client.cancel_order(order_id, ccxt_symbol)
        except Exception:  # pylint: disable=broad-except
            return

    async def _wait_for_order_with_spread(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        order_id: str,
        timeout: int,
        spread_min_pct: float | None,
        spread_max_pct: float | None,
        spread_legs: list[Mapping[str, Any]],
        reprice_sec: float | None,
        *,
        cancel_on_timeout: bool = True,
    ) -> dict[str, Any]:
        exchange = leg["exchange"]
        client = await self._ensure_client(exchange, [])
        if not client:
            return {"status": "error", "error": "client_unavailable"}
        ccxt_symbol = await self._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return {"status": "error", "error": "symbol_unavailable"}
        try:
            market = client.markets.get(ccxt_symbol) if getattr(client, "markets", None) else None
        except Exception:
            market = None
        contract_size = _safe_float(market.get("contractSize")) if isinstance(market, dict) else None
        deadline = time.time() + max(1, int(timeout))
        last_fill = 0.0
        last_avg = None
        check_interval = max(0.5, float(reprice_sec or 1.0))
        while time.time() < deadline:
            try:
                order = await client.fetch_order(order_id, ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                break
            status = str(order.get("status") or "").lower()
            filled = _to_base_qty(_safe_float(order.get("filled")), contract_size) or 0.0
            avg = _safe_float(order.get("average"))
            last_fill = filled or last_fill
            last_avg = avg or last_avg
            if status in ("closed", "filled"):
                return {"status": "filled", "filled_qty": filled, "avg_price": avg}
            if spread_min_pct is not None or spread_max_pct is not None:
                snapshot = await self._snapshot_legs(symbol, spread_legs, max_slippage_bps=0.0)
                if not snapshot.get("errors"):
                    within_range = self._within_spread(
                        snapshot.get("spread_pct"),
                        spread_min_pct,
                        spread_max_pct,
                    )
                    if within_range is False:
                        try:
                            await client.cancel_order(order_id, ccxt_symbol)
                        except Exception:  # pylint: disable=broad-except
                            pass
                        status = "partial" if last_fill > 0 else "open"
                        return {
                            "status": status,
                            "filled_qty": last_fill,
                            "avg_price": last_avg,
                            "cancel_reason": "spread_outside",
                        }
            await asyncio.sleep(check_interval)
        if cancel_on_timeout:
            try:
                await client.cancel_order(order_id, ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                pass
            if last_fill > 0:
                return {"status": "partial", "filled_qty": last_fill, "avg_price": last_avg, "cancel_reason": "timeout"}
            return {"status": "open", "filled_qty": last_fill, "avg_price": last_avg, "cancel_reason": "timeout"}
        status = "partial" if last_fill > 0 else "open"
        return {"status": status, "filled_qty": last_fill, "avg_price": last_avg}

    async def _hedge_position(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        *,
        hedge_order_type: str,
        hedge_offset_bps: float,
        hedge_offset_ticks: int,
        fallback_to_market: bool,
    ) -> dict[str, Any]:
        if hedge_order_type != "limit":
            return await self._place_market(leg, symbol, qty, {})
        snapshot = await self._snapshot_legs(symbol, [leg], max_slippage_bps=0.0)
        if snapshot.get("errors"):
            return {"exchange": leg["exchange"], "status": "error", "error": "hedge_orderbook_failed"}
        constraints = snapshot.get("constraints") or {}
        price_step = (constraints.get(leg["exchange"]) or {}).get("price_step")
        limit_price = _apply_price_offset(
            snapshot.get("primary_best"),
            side=leg["side"],
            offset_bps=hedge_offset_bps,
            offset_ticks=hedge_offset_ticks,
            price_step=price_step,
        )
        if limit_price is None:
            return {"exchange": leg["exchange"], "status": "error", "error": "hedge_price_missing"}
        result = await self._place_limit_at(leg, symbol, qty, limit_price, timeout=3)
        filled_qty = _safe_float(result.get("filled_qty")) or 0.0
        if filled_qty < qty and fallback_to_market:
            remaining = max(0.0, qty - filled_qty)
            if remaining > 0:
                market_result = await self._place_market(leg, symbol, remaining, {})
                return {
                    "exchange": leg["exchange"],
                    "status": "partial",
                    "filled_qty": filled_qty,
                    "avg_price": result.get("avg_price"),
                    "fallback": market_result,
                }
        return result

    async def _ensure_client(self, exchange: str, errors: list[str]) -> Any | None:
        gateway = self._gateways.get(exchange)
        if gateway is None:
            errors.append(f"{exchange}: adapter unavailable")
            return None
        await gateway.refresh_credentials_async(force_env=True)
        await gateway.ensure_client()
        if gateway.client is None:
            errors.append(f"{exchange}: client unavailable (missing credentials?)")
            return None
        return gateway.client

    async def _snapshot_legs(
        self,
        symbol: str,
        legs: Iterable[Mapping[str, Any]],
        *,
        max_slippage_bps: float = 0.0,
    ) -> dict[str, Any]:
        legs = list(legs)
        errors: list[str] = []
        orderbooks: dict[str, dict[str, Any]] = {}
        stats_by_exchange: dict[str, OrderBookStats] = {}
        max_qty_by_exchange: dict[str, float | None] = {}
        constraints: dict[str, dict[str, float | None]] = {}
        for leg in legs:
            exchange = leg["exchange"]
            client = await self._ensure_client(exchange, errors)
            if not client:
                continue
            ccxt_symbol = await self._resolve_market_symbol(client, symbol)
            if not ccxt_symbol:
                errors.append(f"{exchange}: unable to resolve symbol {symbol}")
                continue
            try:
                orderbook = await client.fetch_order_book(ccxt_symbol, limit=self._orderbook_depth)
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(f"{exchange}: orderbook fetch failed: {exc}")
                continue
            constraints[exchange] = self._extract_market_constraints(client, ccxt_symbol)
            contract_size = (constraints.get(exchange) or {}).get("contract_size")
            scaled_orderbook = _scale_orderbook(orderbook, contract_size)
            orderbooks[exchange] = scaled_orderbook
            stats = orderbook_stats(scaled_orderbook, top_n=self._liquidity_top_n)
            stats_by_exchange[exchange] = stats
            levels = scaled_orderbook.get("asks") if leg["side"] == "buy" else scaled_orderbook.get("bids")
            max_qty_by_exchange[exchange] = max_qty_for_slippage(
                levels or [], side=leg["side"], max_bps=max_slippage_bps
            )
        if errors:
            return {"errors": errors}
        long_leg = next((leg for leg in legs if leg.get("label") == "long"), None)
        short_leg = next((leg for leg in legs if leg.get("label") == "short"), None)
        if long_leg and short_leg:
            long_stats = stats_by_exchange.get(long_leg["exchange"])
            short_stats = stats_by_exchange.get(short_leg["exchange"])
            spread_val = spread_pct(long_stats.mid if long_stats else None, short_stats.mid if short_stats else None)
            mid_price = ((long_stats.mid if long_stats else 0.0) + (short_stats.mid if short_stats else 0.0)) / 2.0
        else:
            spread_val = None
            mid_values = [stats.mid for stats in stats_by_exchange.values() if stats.mid]
            mid_price = sum(mid_values) / len(mid_values) if mid_values else 0.0
        primary_leg = next(iter(legs), None)
        primary_best = None
        if primary_leg:
            primary_stats = stats_by_exchange.get(primary_leg["exchange"])
            if primary_stats:
                primary_best = primary_stats.best_ask if primary_leg["side"] == "buy" else primary_stats.best_bid
        return {
            "errors": [],
            "orderbooks": orderbooks,
            "stats": stats_by_exchange,
            "spread_pct": spread_val,
            "mid_price": mid_price,
            "primary_best": primary_best,
            "max_qty_by_exchange": max_qty_by_exchange,
            "constraints": constraints,
        }

    def _within_spread(
        self,
        spread_val: float | None,
        spread_min_pct: float | None,
        spread_max_pct: float | None,
    ) -> bool | None:
        if spread_min_pct is None and spread_max_pct is None:
            return None
        if spread_val is None:
            return None
        if spread_min_pct is not None and spread_val < spread_min_pct:
            return False
        if spread_max_pct is not None and spread_val > spread_max_pct:
            return False
        return True

    async def _resolve_market_symbol(self, client: Any, symbol: str) -> str | None:
        ccxt_symbol = _to_ccxt_symbol(symbol)
        try:
            await client.load_markets()
        except Exception:  # pylint: disable=broad-except
            return None
        markets = getattr(client, "markets", None) or {}
        exact_market = markets.get(ccxt_symbol) if isinstance(markets, dict) else None
        if isinstance(exact_market, dict):
            market_type = str(exact_market.get("type") or "").lower()
            if exact_market.get("swap") or exact_market.get("future") or market_type in ("swap", "future"):
                return exact_market.get("symbol") or ccxt_symbol
        base = None
        quote = None
        if "/" in ccxt_symbol:
            base, quote = ccxt_symbol.split("/", 1)
            if quote and ":" in quote:
                quote = quote.split(":", 1)[0]
        elif ccxt_symbol.endswith("USDT"):
            base = ccxt_symbol[:-4]
            quote = "USDT"
        if base and quote:
            for market in markets.values():
                if market.get("base") != base or market.get("quote") != quote:
                    continue
                market_type = str(market.get("type") or "").lower()
                if market.get("swap") or market.get("future") or market_type in ("swap", "future"):
                    return market.get("symbol") or ccxt_symbol
            for market in markets.values():
                if market.get("base") == base and market.get("quote") == quote:
                    break
        return None

    def _extract_market_constraints(self, client: Any, ccxt_symbol: str) -> dict[str, float | None]:
        market = None
        try:
            if not getattr(client, "markets", None):
                return {}
            market = client.markets.get(ccxt_symbol)
        except Exception:  # pylint: disable=broad-except
            market = None
        if not isinstance(market, dict):
            return {}
        limits = market.get("limits") or {}
        amount_limits = limits.get("amount") or {}
        cost_limits = limits.get("cost") or {}
        info = market.get("info") or {}
        if not isinstance(info, dict):
            info = {}
        raw_min_qty = _safe_float(amount_limits.get("min"))
        min_notional = _safe_float(cost_limits.get("min"))
        if raw_min_qty is None:
            raw_min_qty = _safe_float(info.get("minQty") or info.get("minSize") or info.get("minOrderQty") or info.get("minSz"))
        if min_notional is None:
            min_notional = _safe_float(info.get("minNotional") or info.get("minOrderValue") or info.get("minValue"))
        precision = market.get("precision") or {}
        amount_step = _precision_to_step(precision.get("amount"))
        price_step = _precision_to_step(precision.get("price"))
        contract_size = _safe_float(market.get("contractSize"))
        min_qty = raw_min_qty
        if contract_size and contract_size > 0:
            if min_qty is not None:
                min_qty = min_qty * contract_size
            if amount_step is not None:
                amount_step = amount_step * contract_size
        return {
            "min_qty": min_qty,
            "min_notional": min_notional,
            "amount_step": amount_step,
            "price_step": price_step,
            "contract_size": contract_size,
            "min_qty_contracts": raw_min_qty,
        }

    def _collect_action_errors(self, actions: Iterable[Mapping[str, Any]]) -> list[str]:
        errors: list[str] = []
        for action in actions:
            status = str(action.get("status") or "").lower()
            if status != "error":
                continue
            exchange = action.get("exchange") or "unknown"
            reason = action.get("error") or "unknown error"
            errors.append(f"{exchange}: {reason}")
        return errors

    def _emit_log(
        self,
        log_cb: Optional[callable],
        event: str,
        message: str,
        data: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if not log_cb:
            return
        try:
            log_cb(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "event": event,
                    "message": message,
                    "data": data or {},
                }
            )
        except Exception:
            return

    async def _resolve_qty_from_notional(
        self,
        symbol: str,
        notional: float | None,
        long_exchange: str,
        short_exchange: str,
    ) -> float | None:
        if notional is None or notional <= 0:
            return None
        price = None
        for exchange in (long_exchange, short_exchange):
            client = await self._ensure_client(exchange, [])
            if not client:
                continue
            ccxt_symbol = await self._resolve_market_symbol(client, symbol)
            try:
                ticker = await client.fetch_ticker(ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                continue
            price = _safe_float(ticker.get("last")) or _safe_float(ticker.get("mark")) or _safe_float(ticker.get("close"))
            if price:
                break
        if not price:
            return None
        return notional / price

    def _infer_qty_from_positions(
        self,
        symbol: str,
        positions: Iterable[Mapping[str, Any]],
        *,
        action: str,
        long_exchange: str,
        short_exchange: str,
        payload: Mapping[str, Any],
    ) -> float | None:
        canonical = normalize_symbol(symbol)
        if not canonical:
            return None

        def _sum_qty(exchange: str, side: str) -> float:
            total = 0.0
            for pos in positions:
                exch = str(pos.get("exchange") or "").lower()
                if exch != exchange:
                    continue
                sym = normalize_symbol(pos.get("symbol") or pos.get("symbol_normalized") or "")
                if sym != canonical:
                    continue
                pos_side = str(pos.get("side") or "").lower()
                if pos_side != side:
                    continue
                qty = _safe_float(pos.get("coin_qty"))
                if qty is None:
                    qty = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
                total += abs(qty or 0.0)
            return total

        if action == "exit":
            long_qty = _sum_qty(long_exchange, "long")
            short_qty = _sum_qty(short_exchange, "short")
            if long_qty and short_qty:
                return min(long_qty, short_qty)
            return long_qty or short_qty or None

        if action == "roll":
            side = str(payload.get("side") or "").lower()
            from_exchange = normalize_exchange_name(str(payload.get("from_exchange") or ""))
            if side == "long":
                return _sum_qty(from_exchange, "long") or None
            if side == "short":
                return _sum_qty(from_exchange, "short") or None
        return None

    def _resolve_limit_price(self, leg: Mapping[str, Any], payload: Mapping[str, Any]) -> float | None:
        label = leg.get("label")
        if label == "long":
            return _safe_float(payload.get("limit_price_long"))
        if label == "short":
            return _safe_float(payload.get("limit_price_short"))
        if label == "to":
            return _safe_float(payload.get("limit_price_to")) or _safe_float(payload.get("limit_price_long"))
        if label == "from":
            return _safe_float(payload.get("limit_price_from")) or _safe_float(payload.get("limit_price_short"))
        return None

    async def _resolve_dynamic_limit_price(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        payload: Mapping[str, Any],
    ) -> float | None:
        exchange = leg["exchange"]
        client = await self._ensure_client(exchange, [])
        if not client:
            return None
        ccxt_symbol = await self._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return None
        try:
            orderbook = await client.fetch_order_book(ccxt_symbol, limit=self._orderbook_depth)
        except Exception:  # pylint: disable=broad-except
            return None
        constraints = self._extract_market_constraints(client, ccxt_symbol)
        contract_size = constraints.get("contract_size") if constraints else None
        orderbook = _scale_orderbook(orderbook, contract_size)
        bids = orderbook.get("bids") or []
        asks = orderbook.get("asks") or []
        levels = asks if leg["side"] == "buy" else bids
        parsed_levels: list[tuple[float, float]] = []
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price = _safe_float(level[0]) or 0.0
            size = _safe_float(level[1]) or 0.0
            if price <= 0 or size <= 0:
                continue
            parsed_levels.append((price, size))
        if not parsed_levels:
            return None
        parsed_levels.sort(key=lambda row: row[0], reverse=(leg["side"] == "sell"))
        best_price = parsed_levels[0][0]
        best_bid = _safe_float(bids[0][0]) if bids else None
        best_ask = _safe_float(asks[0][0]) if asks else None
        mid = ((best_bid + best_ask) / 2.0) if best_bid and best_ask else None

        min_level_notional = _safe_float(payload.get("min_level_notional")) or 50.0
        if mid and qty:
            min_level_notional = max(min_level_notional, mid * qty * 0.01)
        min_level_qty = _safe_float(payload.get("min_level_qty"))
        if min_level_qty is None and mid and min_level_notional:
            min_level_qty = min_level_notional / mid if mid > 0 else None

        cumulative_qty = 0.0
        cumulative_notional = 0.0
        candidate_price = best_price
        for price, size in parsed_levels:
            cumulative_qty += size
            cumulative_notional += price * size
            if min_level_qty and cumulative_qty >= min_level_qty:
                candidate_price = price
                break
            if min_level_notional and cumulative_notional >= min_level_notional:
                candidate_price = price
                break

        max_dev_bps = _safe_float(payload.get("max_limit_deviation_bps")) or 30.0
        if mid and mid > 0 and max_dev_bps > 0:
            if leg["side"] == "buy":
                cap = mid * (1 + max_dev_bps / 10_000.0)
                if candidate_price > cap:
                    candidate_price = cap
            else:
                cap = mid * (1 - max_dev_bps / 10_000.0)
                if candidate_price < cap:
                    candidate_price = cap

        price_step = constraints.get("price_step") if constraints else None
        offset_bps = _safe_float(payload.get("limit_offset_bps"))
        offset_ticks = _safe_float(payload.get("limit_offset_ticks"))
        if offset_bps or offset_ticks:
            candidate_price = _apply_price_offset(
                candidate_price,
                side=leg["side"],
                offset_bps=offset_bps,
                offset_ticks=int(offset_ticks or 0),
                price_step=price_step,
            ) or candidate_price
        if price_step:
            mode = "up" if leg["side"] == "buy" else "down"
            candidate_price = _round_to_step(candidate_price, price_step, mode=mode)
        return candidate_price if candidate_price > 0 else None

    def _resolve_expensive_label(self, explicit: Any, leg_by_label: dict[str, Any]) -> str | None:
        if explicit in ("long", "short", "to", "from"):
            return explicit
        if explicit in ("auto", None, ""):
            suggestion = leg_by_label.get("long") or leg_by_label.get("to")
            if suggestion:
                return suggestion.get("label")
        return None

    async def _fetch_funding_meta(self, symbol: str, exchanges: Iterable[str]) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        tasks = []
        for exchange in exchanges:
            tasks.append(asyncio.to_thread(self._fetch_funding_sync, exchange, symbol))
        for exchange, task in zip(exchanges, tasks):
            try:
                results[exchange] = await task
            except Exception as exc:  # pylint: disable=broad-except
                results[exchange] = {"error": str(exc)}
        return results

    def _fetch_funding_sync(self, exchange: str, symbol: str) -> dict[str, Any]:
        exchange = normalize_exchange_name(exchange)
        try:
            adapter = get_adapter(exchange)
        except KeyError as exc:
            return {"error": str(exc)}
        snapshots = adapter.fetch_market_snapshots([symbol])
        if not snapshots:
            return {"error": "no_snapshot"}
        snap = snapshots[0]
        next_funding = snap.next_funding_time
        minutes = None
        if next_funding:
            delta = next_funding - datetime.now(timezone.utc)
            minutes = max(0.0, delta.total_seconds() / 60.0)
        return {
            "funding_rate": snap.funding_rate,
            "next_funding_time": next_funding.isoformat() if next_funding else None,
            "minutes_to_funding": minutes,
            "mark_price": snap.mark_price,
        }

    def _suggest_mode(self, plan: Mapping[str, Any]) -> str:
        spread_val = _safe_float(plan.get("spread_pct"))
        stats = plan.get("stats") or {}
        liq_values = []
        for item in stats.values():
            liq_values.append(_safe_float(item.get("min_liquidity_top3")) or 0.0)
        min_liq = min(liq_values) if liq_values else 0.0
        if spread_val is not None and abs(spread_val) <= 0.05 and min_liq >= 50_000:
            return "dual-limit"
        if min_liq and min_liq < 20_000:
            return "limit-first-expensive"
        return "limit-first-expensive"

    def _plan_response(
        self,
        payload: Mapping[str, Any],
        legs: list[dict[str, Any]],
        errors: list[str],
        warnings: list[str],
        *,
        action: str,
    ) -> dict[str, Any]:
        return {
            "dry_run": bool(payload.get("dry_run", False)),
            "action": action,
            "symbol": payload.get("symbol"),
            "qty": payload.get("qty"),
            "mode": payload.get("mode"),
            "legs": legs,
            "errors": errors,
            "warnings": warnings,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
