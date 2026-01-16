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
from execution.ws_positions import LivePositionTracker
from execution.ws_orders import LiveOrderTracker

logger = logging.getLogger(__name__)
DEFAULT_MANUAL_LEVERAGE = 3.0
DEFAULT_MIN_LEVEL_NOTIONAL = 50.0
DEFAULT_MIN_LEVEL_CHUNK_PCT = 0.01
DEFAULT_LIMIT_IMPROVE_TICKS = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _order_filled_qty(order: Mapping[str, Any], contract_size: float | None) -> float:
    filled = _safe_float(order.get("filled"))
    if not filled:
        amount = _safe_float(order.get("amount"))
        remaining = _safe_float(order.get("remaining"))
        if amount is not None and remaining is not None:
            filled = max(0.0, amount - remaining)
    if not filled:
        info = order.get("info") or {}
        for key in ("dealSize", "filledSize", "dealQty", "filledQty", "executedQty"):
            value = _safe_float(info.get(key))
            if value:
                filled = value
                break
    return _to_base_qty(filled, contract_size) or 0.0


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


def _min_qty_with_buffer(
    min_qty: float | None,
    amount_step: float | None,
    *,
    buffer_pct: float = 0.15,
) -> float | None:
    if min_qty is None or min_qty <= 0:
        return None
    buffered = float(min_qty) * (1.0 + float(buffer_pct))
    return _round_to_step(buffered, amount_step, mode="up")


def _entry_position_side(leg: Mapping[str, Any]) -> str:
    label = str(leg.get("label") or "").lower()
    if label in ("long", "short"):
        return label
    side = str(leg.get("side") or "").lower()
    if side == "buy":
        return "long"
    if side == "sell":
        return "short"
    return ""


def _exit_position_side(leg: Mapping[str, Any]) -> str:
    label = str(leg.get("label") or "").lower()
    if label in ("long", "short"):
        return label
    side = str(leg.get("side") or "").lower()
    if side == "buy":
        return "short"
    if side == "sell":
        return "long"
    return ""


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


def _price_matches(left: float | None, right: float | None, price_step: float | None) -> bool:
    if left is None or right is None:
        return False
    if price_step and price_step > 0:
        return abs(left - right) <= (price_step * 0.5)
    return abs(left - right) <= 1e-9


def _resolve_level_thresholds(
    payload: Mapping[str, Any],
    *,
    qty: float | None,
    mid_price: float | None,
) -> tuple[float | None, float | None]:
    min_level_notional = _safe_float(payload.get("min_level_notional"))
    if min_level_notional is None:
        min_level_notional = DEFAULT_MIN_LEVEL_NOTIONAL
    min_chunk_pct = _pct_to_fraction(_safe_float(payload.get("min_level_chunk_pct")))
    if min_chunk_pct is None:
        min_chunk_pct = DEFAULT_MIN_LEVEL_CHUNK_PCT
    if mid_price and qty and min_chunk_pct:
        min_level_notional = max(min_level_notional, mid_price * qty * min_chunk_pct)
    min_level_qty = _safe_float(payload.get("min_level_qty"))
    if qty and min_chunk_pct:
        min_level_qty = max(min_level_qty or 0.0, qty * min_chunk_pct)
    if min_level_qty is None and mid_price and min_level_notional:
        min_level_qty = min_level_notional / mid_price if mid_price > 0 else None
    if min_level_qty is not None and min_level_qty <= 0:
        min_level_qty = None
    if min_level_notional is not None and min_level_notional <= 0:
        min_level_notional = None
    return min_level_qty, min_level_notional


def _effective_best_price(
    orderbook: Mapping[str, Any] | None,
    *,
    side: str,
    min_level_qty: float | None,
    min_level_notional: float | None,
    exclude_price: float | None = None,
    exclude_qty: float | None = None,
    price_step: float | None = None,
) -> float | None:
    if not orderbook:
        return None
    levels = orderbook.get("bids") if side == "buy" else orderbook.get("asks")
    if not levels:
        return None
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
    parsed_levels.sort(key=lambda row: row[0], reverse=(side == "buy"))
    cumulative_qty = 0.0
    cumulative_notional = 0.0
    for price, size in parsed_levels:
        adjusted_size = size
        if exclude_qty and exclude_price and _price_matches(price, exclude_price, price_step):
            adjusted_size = max(0.0, size - exclude_qty)
        if adjusted_size <= 0:
            continue
        cumulative_qty += adjusted_size
        cumulative_notional += price * adjusted_size
        if min_level_qty and cumulative_qty >= min_level_qty:
            return price
        if min_level_notional and cumulative_notional >= min_level_notional:
            return price
    return parsed_levels[0][0]


def _resolve_smart_limit_price(
    *,
    orderbook: Mapping[str, Any] | None,
    side: str,
    book_side: str | None,
    qty: float | None,
    payload: Mapping[str, Any],
    price_step: float | None,
    best_bid: float | None,
    best_ask: float | None,
    mid_price: float | None,
    improve_ticks: int,
    offset_bps: float | None,
    offset_ticks: int | None,
    round_mode: str | None,
    exclude_price: float | None = None,
    exclude_qty: float | None = None,
) -> float | None:
    min_level_qty, min_level_notional = _resolve_level_thresholds(payload, qty=qty, mid_price=mid_price)
    resolved_book_side = book_side or side
    effective_best = _effective_best_price(
        orderbook,
        side=resolved_book_side,
        min_level_qty=min_level_qty,
        min_level_notional=min_level_notional,
        exclude_price=exclude_price,
        exclude_qty=exclude_qty,
        price_step=price_step,
    )
    if effective_best is None:
        effective_best = best_bid if side == "buy" else best_ask
    if effective_best is None:
        return None
    candidate = effective_best
    if price_step and improve_ticks:
        tick = float(price_step) * max(0, int(improve_ticks))
        if side == "buy":
            candidate += tick
        else:
            candidate -= tick
    candidate = _apply_price_offset(
        candidate,
        side=side,
        offset_bps=offset_bps,
        offset_ticks=offset_ticks,
        price_step=price_step,
        round_mode=round_mode,
    ) or candidate
    candidate = _ensure_maker_price(
        candidate,
        side=side,
        best_bid=best_bid,
        best_ask=best_ask,
        price_step=price_step,
    )
    return candidate if candidate and candidate > 0 else None


def _ensure_maker_price(
    price: float | None,
    *,
    side: str,
    best_bid: float | None,
    best_ask: float | None,
    price_step: float | None,
) -> float | None:
    if price is None or price <= 0:
        return None
    if not price_step or price_step <= 0:
        return price
    adjusted = float(price)
    tick = float(price_step)
    if side == "buy":
        if best_bid is not None and adjusted > best_bid:
            adjusted = best_bid + tick
        if best_ask is not None and adjusted >= best_ask:
            adjusted = best_ask - tick
        adjusted = _round_to_step(adjusted, price_step, mode="down")
    else:
        if best_ask is not None and adjusted < best_ask:
            adjusted = best_ask - tick
        if best_bid is not None and adjusted <= best_bid:
            adjusted = best_bid + tick
        adjusted = _round_to_step(adjusted, price_step, mode="up")
    return adjusted if adjusted > 0 else None


def _pct_to_fraction(value: float | None) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if numeric < 0:
        return None
    return numeric / 100.0 if numeric > 1 else numeric


def _resolve_timeout(payload: Mapping[str, Any], default: int) -> int:
    raw = _safe_float(payload.get("timeout_sec"))
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _hedge_price_move_bps(side: str, order_price: float, best_bid: float | None, best_ask: float | None) -> tuple[float | None, float | None]:
    if order_price <= 0:
        return None, None
    if side == "buy":
        if best_ask is None:
            return None, None
        favorable = (order_price - best_ask) / order_price * 10_000.0 if best_ask < order_price else 0.0
        adverse = (best_ask - order_price) / order_price * 10_000.0 if best_ask > order_price else 0.0
        return favorable, adverse
    if best_bid is None:
        return None, None
    favorable = (best_bid - order_price) / order_price * 10_000.0 if best_bid > order_price else 0.0
    adverse = (order_price - best_bid) / order_price * 10_000.0 if best_bid < order_price else 0.0
    return favorable, adverse


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


def _symbol_matches(canonical: str, candidate: str) -> bool:
    if not canonical:
        return True
    if canonical == candidate:
        return True
    if canonical.endswith(("USDT", "USDC", "USD")):
        return False
    return candidate.startswith(canonical)


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


def _stats_payload(stats: OrderBookStats | None) -> dict[str, float | None]:
    if not stats:
        return {}
    return {
        "best_bid": stats.best_bid,
        "best_ask": stats.best_ask,
        "spread": stats.spread,
        "mid": stats.mid,
        "bid_liquidity_top3": stats.bid_liquidity_top3,
        "ask_liquidity_top3": stats.ask_liquidity_top3,
        "min_liquidity_top3": stats.min_liquidity_top3,
    }

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
    if long_price is None or short_price is None or long_price == 0:
        return None
    return (long_price - short_price) / long_price * 100.0


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
        orderbook_provider: Any | None = None,
    ) -> None:
        self._fees = fee_table or EXCHANGE_COMMISSIONS
        self._orderbook_depth = max(5, int(orderbook_depth))
        self._liquidity_top_n = max(1, int(liquidity_top_n))
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}
        self._lock = asyncio.Lock()
        self._position_mode_cache: dict[str, tuple[bool | None, float]] = {}
        self._orderbook_provider = orderbook_provider
        self._ws_positions = LivePositionTracker()
        self._ws_positions_stale_sec = 15.0
        self._ws_orders = LiveOrderTracker()
        self._ws_orders_stale_sec = 45.0
        self._stop_check: Optional[callable] = None

    def _contract_sizes_from_constraints(self, constraints: Mapping[str, Any]) -> dict[str, float | None]:
        sizes: dict[str, float | None] = {}
        for exchange, data in (constraints or {}).items():
            if not exchange:
                continue
            info = data or {}
            sizes[normalize_exchange_name(str(exchange))] = _safe_float(info.get("contract_size"))
        return sizes

    def _apply_ws_orders_health(self, payload: Mapping[str, Any]) -> None:
        overrides = payload.get("ws_orders_health")
        if isinstance(overrides, Mapping):
            self._ws_orders.set_health_configs(overrides)

    def _stop_requested(self) -> bool:
        if not self._stop_check:
            return False
        try:
            return bool(self._stop_check())
        except Exception:
            return False

    async def _ensure_ws_positions(
        self,
        exchanges: Iterable[str],
        *,
        contract_sizes: Mapping[str, float | None] | None = None,
    ) -> None:
        normalized = [normalize_exchange_name(str(exchange)) for exchange in exchanges if exchange]
        if not normalized:
            return
        await self._ws_positions.ensure(normalized)
        if contract_sizes:
            self._ws_positions.set_contract_sizes(dict(contract_sizes))

    async def _ensure_ws_orders(
        self,
        exchanges: Iterable[str],
        *,
        contract_sizes: Mapping[str, float | None] | None = None,
        symbol: str | None = None,
        log_cb: Optional[callable] = None,
    ) -> None:
        normalized = [normalize_exchange_name(str(exchange)) for exchange in exchanges if exchange]
        if not normalized:
            return
        symbols = None
        if symbol:
            symbols = {exchange: [symbol] for exchange in normalized}
        await self._ws_orders.ensure(normalized, symbols=symbols)
        if contract_sizes:
            self._ws_orders.set_contract_sizes(dict(contract_sizes))
        exchanges_text = ",".join(normalized)
        self._emit_story(
            log_cb,
            f"WS order stream requested: exchanges={exchanges_text or '-'} symbol={symbol or '-'} topics=orders",
            {"exchanges": normalized, "symbol": symbol, "topics": ["orders"]},
        )
        snapshots = {ex: self._ws_orders.health_snapshot(ex) for ex in normalized}
        if snapshots:
            summary = "; ".join(self._format_ws_health_entry(snap) for snap in snapshots.values())
            self._emit_story(log_cb, f"WS order stream health: {summary}", {"streams": snapshots})

    def _ws_live(self, exchange: str) -> bool:
        exchange = normalize_exchange_name(exchange)
        return self._ws_positions.is_live(exchange, stale_after=self._ws_positions_stale_sec)

    def _ws_position_qty(self, exchange: str, symbol: str, side: str) -> float | None:
        exchange = normalize_exchange_name(exchange)
        if not self._ws_live(exchange):
            return None
        positions = self._ws_positions.get_positions(exchange, symbol)
        return self._sum_position_qty(
            positions,
            exchange=exchange,
            side=side,
            symbol=symbol,
        )

    def _ws_orders_live(self, exchange: str) -> bool:
        exchange = normalize_exchange_name(exchange)
        return self._ws_orders.is_healthy(exchange)

    async def _ensure_ws_orders_healthy(
        self,
        exchange: str,
        *,
        reason: str | None = None,
        log_cb: Optional[callable] = None,
    ) -> bool:
        exchange = normalize_exchange_name(exchange)
        snapshot = self._ws_orders.health_snapshot(exchange)
        self._emit_story(
            log_cb,
            f"WS order health: {self._format_ws_health_entry(snapshot)}",
            snapshot,
        )
        if self._ws_orders.is_healthy(exchange):
            return True
        if snapshot.get("warming"):
            self._emit_story(
                log_cb,
                f"WS[{exchange}] warming; waiting for first frame",
                snapshot,
            )
            return True
        config = self._ws_orders.health_config(exchange)
        attempts = int(config.get("reconnect_attempts") or 0)
        grace_sec = _safe_float(config.get("reconnect_grace_sec")) or 0.0
        if attempts <= 0 or grace_sec <= 0:
            self._emit_story(
                log_cb,
                f"WS[{exchange}] stale; reconnect disabled ({self._format_ws_health_entry(snapshot)})",
                {"reason": reason, **snapshot},
            )
            return False
        self._emit_log(
            log_cb,
            "wait",
            "ws order stream stale; probing heartbeat/reconnect",
            {"exchange": exchange, "reason": reason, "attempts": attempts, "grace_sec": grace_sec},
        )
        recovered = await self._ws_orders.await_healthy(
            exchange,
            attempts=attempts,
            grace_sec=grace_sec,
        )
        if recovered:
            self._emit_story(
                log_cb,
                f"WS[{exchange}] recovered ({self._format_ws_health_entry(self._ws_orders.health_snapshot(exchange))})",
                self._ws_orders.health_snapshot(exchange),
            )
            self._emit_log(
                log_cb,
                "info",
                "ws order stream recovered",
                {"exchange": exchange, "reason": reason},
            )
        else:
            self._emit_story(
                log_cb,
                f"WS[{exchange}] still stale after probes ({self._format_ws_health_entry(self._ws_orders.health_snapshot(exchange))})",
                self._ws_orders.health_snapshot(exchange),
            )
        return recovered

    def _ws_order_info(self, exchange: str, order_id: str | None) -> dict[str, Any] | None:
        exchange = normalize_exchange_name(exchange)
        if not order_id:
            return None
        if not self._ws_orders_live(exchange):
            return None
        return self._ws_orders.get_order(exchange, str(order_id))

    async def _await_order_fill(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        order_id: str | None,
        expected_qty: float,
        timeout_sec: float,
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        exchange = leg.get("exchange") or ""
        if not order_id:
            return {
                "exchange": exchange,
                "status": "error",
                "error": "missing_order_id",
                "ts": _now_iso(),
            }
        if timeout_sec and timeout_sec > 0:
            deadline = time.time() + timeout_sec
        else:
            deadline = None
        last_info: dict[str, Any] | None = None
        if await self._ensure_ws_orders_healthy(exchange, reason="await_fill", log_cb=log_cb):
            self._emit_story(log_cb, f"WS[{exchange}] using order updates id={order_id}", {"order_id": order_id})
            while True:
                if self._stop_requested():
                    result = {
                        "exchange": exchange,
                        "status": "canceled",
                        "order_id": order_id,
                        "filled_qty": _safe_float(last_info.get("filled_qty") if last_info else 0.0) or 0.0,
                        "avg_price": last_info.get("avg_price") if last_info else None,
                        "source": "ws",
                        "cancel_reason": "user_stop",
                        "ts": _now_iso(),
                    }
                    self._emit_order_status(
                        log_cb,
                        exchange=exchange,
                        label=leg.get("label"),
                        order_id=order_id,
                        status=result.get("status"),
                        filled_qty=result.get("filled_qty"),
                        avg_price=result.get("avg_price"),
                        source=result.get("source"),
                    )
                    return result
                info = self._ws_order_info(exchange, order_id)
                if info:
                    last_info = info
                    filled = _safe_float(info.get("filled_qty")) or 0.0
                    status = str(info.get("status") or "").lower()
                    if expected_qty and filled >= expected_qty * 0.999:
                        result = {
                            "exchange": exchange,
                            "status": "filled",
                            "order_id": order_id,
                            "filled_qty": filled,
                            "avg_price": info.get("avg_price"),
                            "source": "ws",
                            "ts": _now_iso(),
                        }
                        self._emit_order_status(
                            log_cb,
                            exchange=exchange,
                            label=leg.get("label"),
                            order_id=order_id,
                            status=result.get("status"),
                            filled_qty=result.get("filled_qty"),
                            avg_price=result.get("avg_price"),
                            source=result.get("source"),
                        )
                        return result
                    if status in ("filled", "closed", "finished"):
                        result = {
                            "exchange": exchange,
                            "status": "filled",
                            "order_id": order_id,
                            "filled_qty": filled,
                            "avg_price": info.get("avg_price"),
                            "source": "ws",
                            "ts": _now_iso(),
                        }
                        self._emit_order_status(
                            log_cb,
                            exchange=exchange,
                            label=leg.get("label"),
                            order_id=order_id,
                            status=result.get("status"),
                            filled_qty=result.get("filled_qty"),
                            avg_price=result.get("avg_price"),
                            source=result.get("source"),
                        )
                        return result
                    if status in ("canceled", "cancelled"):
                        result = {
                            "exchange": exchange,
                            "status": "canceled",
                            "order_id": order_id,
                            "filled_qty": filled,
                            "avg_price": info.get("avg_price"),
                            "source": "ws",
                            "ts": _now_iso(),
                        }
                        self._emit_order_status(
                            log_cb,
                            exchange=exchange,
                            label=leg.get("label"),
                            order_id=order_id,
                            status=result.get("status"),
                            filled_qty=result.get("filled_qty"),
                            avg_price=result.get("avg_price"),
                            source=result.get("source"),
                        )
                        return result
                if deadline and time.time() >= deadline:
                    break
                await asyncio.sleep(0.2)
            if last_info:
                filled = _safe_float(last_info.get("filled_qty")) or 0.0
                status = str(last_info.get("status") or "open").lower()
                if status in ("filled", "closed", "finished"):
                    status = "filled"
                elif filled > 0:
                    status = "partial"
                result = {
                    "exchange": exchange,
                    "status": status,
                    "order_id": order_id,
                    "filled_qty": filled,
                    "avg_price": last_info.get("avg_price"),
                    "source": "ws",
                    "ts": _now_iso(),
                }
                self._emit_order_status(
                    log_cb,
                    exchange=exchange,
                    label=leg.get("label"),
                    order_id=order_id,
                    status=result.get("status"),
                    filled_qty=result.get("filled_qty"),
                    avg_price=result.get("avg_price"),
                    source=result.get("source"),
                )
                return result
        else:
            self._emit_story(
                log_cb,
                f"WS[{exchange}] unavailable; using REST order status id={order_id}",
                {"order_id": order_id},
            )
        status = await self._fetch_order_status(
            leg,
            symbol,
            order_id,
            expected_qty=expected_qty,
        )
        status["exchange"] = exchange
        status["order_id"] = order_id
        status["source"] = status.get("source") or "rest"
        status["ts"] = status.get("ts") or _now_iso()
        self._emit_order_status(
            log_cb,
            exchange=exchange,
            label=leg.get("label"),
            order_id=order_id,
            status=status.get("status"),
            filled_qty=status.get("filled_qty"),
            avg_price=status.get("avg_price"),
            source=status.get("source"),
        )
        return status


    async def enter(
        self,
        payload: Mapping[str, Any],
        *,
        log_cb: Optional[callable] = None,
        stop_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        return await self._handle_pair(payload, action="enter", log_cb=log_cb, stop_cb=stop_cb)

    async def exit(
        self,
        payload: Mapping[str, Any],
        positions: Iterable[Mapping[str, Any]],
        *,
        log_cb: Optional[callable] = None,
        stop_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        return await self._handle_pair(payload, action="exit", positions=positions, log_cb=log_cb, stop_cb=stop_cb)

    async def roll(
        self,
        payload: Mapping[str, Any],
        positions: Iterable[Mapping[str, Any]],
        *,
        log_cb: Optional[callable] = None,
        stop_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        return await self._handle_pair(payload, action="roll", positions=positions, log_cb=log_cb, stop_cb=stop_cb)

    async def analyze(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self._apply_ws_orders_health(payload)
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
        stop_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        async with self._lock:
            self._apply_ws_orders_health(payload)
            if log_cb:
                self._emit_log(log_cb, "payload", "manual payload", dict(payload))
            self._stop_check = stop_cb
            plan = await self._build_plan(payload, action=action, positions=positions)
            if payload.get("dry_run", False) or plan["errors"]:
                self._stop_check = None
                return plan
            if log_cb:
                def _ws_event_cb(payload: Mapping[str, Any]) -> None:
                    exchange = str(payload.get("exchange") or "")
                    action = str(payload.get("action") or "event")
                    message = self._format_ws_probe_message(exchange, action, payload)
                    self._emit_story(log_cb, message, payload)
                self._ws_orders.set_event_cb(_ws_event_cb)
            mode = str(payload.get("mode") or "limit-first-expensive")
            if action == "exit" and (payload.get("spread_min_pct") is not None or payload.get("spread_max_pct") is not None):
                if mode not in ("smart-exit", "fast-exit"):
                    mode = "smart-exit"
                    plan.setdefault("warnings", []).append("mode overridden to smart-exit for spread-guided exit")
            if action == "enter" and (payload.get("spread_min_pct") is not None or payload.get("spread_max_pct") is not None):
                if mode not in ("smart-enter", "fast-enter"):
                    mode = "smart-enter"
                    plan.setdefault("warnings", []).append("mode overridden to smart-enter for spread-guided enter")
            legs = list(plan.get("legs") or [])
            exchanges = []
            for leg in legs:
                exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
                if exchange and exchange not in exchanges:
                    exchanges.append(exchange)
            await self._log_positions_snapshot(
                exchanges=exchanges,
                symbol=str(plan.get("symbol") or ""),
                stage="start",
                log_cb=log_cb,
            )
            try:
                return await self._execute_plan(plan, mode=mode, payload=payload, log_cb=log_cb)
            finally:
                self._ws_orders.set_event_cb(None)
                self._stop_check = None
                await self._log_positions_snapshot(
                    exchanges=exchanges,
                    symbol=str(plan.get("symbol") or ""),
                    stage="end",
                    log_cb=log_cb,
                )

    async def _build_plan(
        self,
        payload: Mapping[str, Any],
        *,
        action: str,
        positions: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        is_dry_run = bool(payload.get("dry_run", False))
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
        chunk_qty = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        margin_mode = str(payload.get("margin_mode") or "").strip().lower()
        min_notional_overrides = payload.get("min_notional_overrides") or {}
        if not isinstance(min_notional_overrides, Mapping):
            min_notional_overrides = {}
        min_notional_buffer_pct = _safe_float(payload.get("min_notional_buffer_pct")) or 0.0
        if min_notional_buffer_pct < 0:
            min_notional_buffer_pct = 0.0
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
        def _leg_margin_mode(exchange: str) -> str | None:
            if margin_mode:
                return margin_mode
            return "isolated"

        if action == "enter":
            legs = [
                {
                    "label": "long",
                    "exchange": long_exchange,
                    "side": "buy",
                    "reduce_only": False,
                    "margin_mode": _leg_margin_mode(long_exchange),
                },
                {
                    "label": "short",
                    "exchange": short_exchange,
                    "side": "sell",
                    "reduce_only": False,
                    "margin_mode": _leg_margin_mode(short_exchange),
                },
            ]
        elif action == "exit":
            legs = [
                {
                    "label": "long",
                    "exchange": long_exchange,
                    "side": "sell",
                    "reduce_only": True,
                    "margin_mode": _leg_margin_mode(long_exchange),
                },
                {
                    "label": "short",
                    "exchange": short_exchange,
                    "side": "buy",
                    "reduce_only": True,
                    "margin_mode": _leg_margin_mode(short_exchange),
                },
            ]
        elif action == "roll":
            side = str(payload.get("side") or "").lower()
            from_exchange = normalize_exchange_name(str(payload.get("from_exchange") or ""))
            to_exchange = normalize_exchange_name(str(payload.get("to_exchange") or ""))
            if side == "long":
                legs = [
                    {
                        "label": "to",
                        "exchange": to_exchange,
                        "side": "buy",
                        "reduce_only": False,
                        "margin_mode": _leg_margin_mode(to_exchange),
                    },
                    {
                        "label": "from",
                        "exchange": from_exchange,
                        "side": "sell",
                        "reduce_only": True,
                        "margin_mode": _leg_margin_mode(from_exchange),
                    },
                ]
            elif side == "short":
                legs = [
                    {
                        "label": "to",
                        "exchange": to_exchange,
                        "side": "sell",
                        "reduce_only": False,
                        "margin_mode": _leg_margin_mode(to_exchange),
                    },
                    {
                        "label": "from",
                        "exchange": from_exchange,
                        "side": "buy",
                        "reduce_only": True,
                        "margin_mode": _leg_margin_mode(from_exchange),
                    },
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
            orderbook = await self._fetch_orderbook(
                client=client,
                exchange=exchange,
                symbol=symbol,
                ccxt_symbol=ccxt_symbol,
                depth=self._orderbook_depth,
                errors=errors,
            )
            if not orderbook:
                continue
            constraints = self._extract_market_constraints(client, ccxt_symbol)
            override = _safe_float(
                min_notional_overrides.get(exchange)
                or min_notional_overrides.get(exchange.lower())
                or min_notional_overrides.get(exchange.upper())
            )
            effective_min_notional = constraints.get("min_notional")
            if override is not None and override > 0:
                constraints["min_notional_override"] = override
                if effective_min_notional is None or override > effective_min_notional:
                    effective_min_notional = override
            if effective_min_notional is not None:
                constraints["min_notional_effective"] = effective_min_notional
            if min_notional_buffer_pct:
                constraints["min_notional_buffer_pct"] = min_notional_buffer_pct
            contract_size = constraints.get("contract_size")
            scaled_orderbook = _scale_orderbook(orderbook, contract_size)
            orderbooks[exchange] = scaled_orderbook
            stats = orderbook_stats(scaled_orderbook, top_n=self._liquidity_top_n)
            stats_by_exchange[exchange] = stats
            liquidity_map[exchange] = stats.min_liquidity_top3
            if stats.mid:
                price_for_min = stats.mid * (1.0 + min_notional_buffer_pct / 100.0)
                constraints["min_qty_required"] = _min_qty_required(
                    min_qty=constraints.get("min_qty"),
                    min_notional=effective_min_notional,
                    price=price_for_min,
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
            if use_orderbook_check:
                check_qty = qty
                if chunk_qty and chunk_qty > 0:
                    check_qty = min(check_qty, chunk_qty)
                elif chunk_notional and stats.mid:
                    check_qty = min(check_qty, chunk_notional / stats.mid)
                check_fill = fill
                if check_qty != qty:
                    check_fill = estimate_fill(levels or [], float(check_qty))
                if check_fill["remaining_qty"] and check_fill["remaining_qty"] > 0:
                    details = [f"{exchange}: insufficient liquidity for qty {check_qty:g}"]
                    if check_qty != qty:
                        details.append(f"requested={qty:g}")
                    if max_qty_by_exchange.get(exchange) is not None:
                        details.append(f"max_qty={max_qty_by_exchange[exchange]:g}")
                    if check_fill.get("remaining_qty"):
                        details.append(f"remaining={check_fill['remaining_qty']:g}")
                    if stats.min_liquidity_top3 is not None:
                        details.append(f"top3_usd={stats.min_liquidity_top3:.2f}")
                    message = "; ".join(details)
                    if is_dry_run:
                        warnings.append(message)
                    else:
                        errors.append(message)
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
        constraints_all: dict[str, dict[str, Any]] | None = None
        constraint_exchanges = payload.get("constraints_exchanges") or []
        if constraint_exchanges:
            constraints_all = await self._collect_constraints_all(
                symbol,
                constraint_exchanges,
                min_notional_overrides=min_notional_overrides,
                min_notional_buffer_pct=min_notional_buffer_pct,
            )
        orderbook_sources = {
            exch: (orderbooks.get(exch) or {}).get("source") for exch in orderbooks
        }
        return {
            "dry_run": bool(payload.get("dry_run", False)),
            "action": action,
            "symbol": symbol,
            "qty": qty,
            "notional": notional,
            "mode": payload.get("mode"),
            "legs": legs,
            "orderbooks": orderbooks if include_orderbooks else {},
            "orderbook_sources": orderbook_sources,
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
            "constraints_all": constraints_all,
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

    async def _collect_constraints_all(
        self,
        symbol: str,
        exchanges: Iterable[str],
        *,
        min_notional_overrides: Mapping[str, Any] | None = None,
        min_notional_buffer_pct: float | None = None,
    ) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        for name in exchanges:
            exchange = normalize_exchange_name(str(name) or "")
            if not exchange:
                continue
            try:
                errors: list[str] = []
                client = await self._ensure_client(exchange, errors)
                if not client:
                    results[exchange] = {"error": errors[-1] if errors else "client_unavailable"}
                    continue
                ccxt_symbol = await self._resolve_market_symbol(client, symbol)
                if not ccxt_symbol:
                    results[exchange] = {"error": f"{exchange}: unable to resolve symbol {symbol}"}
                    continue
                constraints = self._extract_market_constraints(client, ccxt_symbol)
                override = None
                if min_notional_overrides:
                    override = _safe_float(
                        min_notional_overrides.get(exchange)
                        or min_notional_overrides.get(exchange.lower())
                        or min_notional_overrides.get(exchange.upper())
                    )
                effective_min_notional = constraints.get("min_notional")
                if override is not None and override > 0:
                    constraints["min_notional_override"] = override
                    if effective_min_notional is None or override > effective_min_notional:
                        effective_min_notional = override
                if effective_min_notional is not None:
                    constraints["min_notional_effective"] = effective_min_notional
                buffer_pct = _safe_float(min_notional_buffer_pct) or 0.0
                if buffer_pct < 0:
                    buffer_pct = 0.0
                if buffer_pct:
                    constraints["min_notional_buffer_pct"] = buffer_pct
                price_hint = None
                try:
                    ticker = await client.fetch_ticker(ccxt_symbol)
                    bid = _safe_float(ticker.get("bid"))
                    ask = _safe_float(ticker.get("ask"))
                    last = _safe_float(ticker.get("last"))
                    if bid and ask:
                        price_hint = (bid + ask) / 2.0
                    elif last:
                        price_hint = last
                except Exception:  # pylint: disable=broad-except
                    price_hint = None
                if price_hint is None:
                    orderbook = await self._fetch_orderbook(
                        client=client,
                        exchange=exchange,
                        symbol=symbol,
                        ccxt_symbol=ccxt_symbol,
                        depth=1,
                    )
                    if orderbook:
                        contract_size = constraints.get("contract_size")
                        scaled_orderbook = _scale_orderbook(orderbook, contract_size)
                        stats = orderbook_stats(scaled_orderbook, top_n=1)
                        price_hint = stats.mid
                if price_hint:
                    price_for_min = price_hint * (1.0 + buffer_pct / 100.0)
                    constraints["min_qty_required"] = _min_qty_required(
                        min_qty=constraints.get("min_qty"),
                        min_notional=effective_min_notional,
                        price=price_for_min,
                        amount_step=constraints.get("amount_step"),
                    )
                constraints["ccxt_symbol"] = ccxt_symbol
                if price_hint:
                    constraints["price_hint"] = price_hint
                results[exchange] = constraints
            except Exception as exc:  # pylint: disable=broad-except
                results[exchange] = {"error": str(exc)}
        return results

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
        timeout = _resolve_timeout(payload, 15)
        expensive_leg = payload.get("expensive_leg")
        fallback_to_market = bool(payload.get("fallback_to_market", False))
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        spread_val = _safe_float(plan.get("spread_pct"))
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        if mode not in ("smart-exit", "fast-exit", "smart-enter", "fast-enter"):
            self._emit_log(
                log_cb,
                "start",
                "manual execution",
                {
                    "action": plan.get("action"),
                    "mode": mode,
                    "symbol": symbol,
                    "qty": qty,
                    "legs": legs,
                    "spread_pct": spread_val,
                },
            )
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
                    log_cb=log_cb,
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
                        reason="limit_hedge",
                        log_cb=log_cb,
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
                        reason="fallback_market",
                        log_cb=log_cb,
                    )
                    actions.append(market_result)
                    self._emit_log(log_cb, "result", "fallback result", market_result)
        elif mode == "dual-market":
            for leg in legs:
                self._emit_log(log_cb, "submit", f"market {leg['exchange']} qty={qty:g}")
                actions.append(await self._place_market(leg, symbol, qty, payload, reason="dual_market", log_cb=log_cb))
        elif mode == "dual-limit":
            pending: list[dict[str, Any]] = []
            for leg in legs:
                self._emit_log(log_cb, "submit", f"limit {leg['exchange']} qty={qty:g}")
                pending.append(await self._place_limit_then_wait(leg, symbol, qty, timeout, payload, log_cb=log_cb))
            actions.extend(pending)
            if fallback_to_market:
                for result, leg in zip(pending, legs):
                    if result.get("status") in ("partial", "open"):
                        remaining = max(0.0, qty - (_safe_float(result.get("filled_qty")) or 0.0))
                        if remaining > 0:
                            self._emit_log(log_cb, "submit", f"fallback market {leg['exchange']} qty={remaining:g}")
                            actions.append(
                                await self._place_market(
                                    leg,
                                    symbol,
                                    remaining,
                                    payload,
                                    reason="fallback_market",
                                    log_cb=log_cb,
                                )
                            )
        elif mode == "limit-then-market-fallback":
            primary = legs[0] if legs else None
            if not primary:
                errors.append("No legs available for limit-then-market-fallback.")
            else:
                self._emit_log(log_cb, "submit", f"limit {primary['exchange']} qty={qty:g}")
                result = await self._place_limit_then_wait(primary, symbol, qty, timeout, payload, log_cb=log_cb)
                actions.append(result)
                self._emit_log(log_cb, "result", "limit result", result)
                filled = _safe_float(result.get("filled_qty")) or 0.0
                if filled <= 0 and fallback_to_market:
                    self._emit_log(log_cb, "submit", f"fallback market {primary['exchange']} qty={qty:g}")
                    actions.append(
                        await self._place_market(
                            primary,
                            symbol,
                            qty,
                            payload,
                            reason="fallback_market",
                            log_cb=log_cb,
                        )
                    )
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
        timeout = _resolve_timeout(payload, 10)
        reprice_sec = _safe_float(payload.get("reprice_sec")) or 2.0
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 60)
        limit_offset_bps = _safe_float(payload.get("limit_offset_bps")) or 0.0
        limit_offset_ticks = int(_safe_float(payload.get("limit_offset_ticks")) or 0)
        hedge_order_type = str(payload.get("hedge_order_type") or "market").lower()
        hedge_offset_bps = _safe_float(payload.get("hedge_offset_bps")) or 2.0
        hedge_offset_ticks = int(_safe_float(payload.get("hedge_offset_ticks")) or 0)
        hedge_limit_mode = str(payload.get("hedge_limit_mode") or "passive").lower()
        hedge_favorable_bps = _safe_float(payload.get("hedge_favorable_bps")) or 2.0
        raw_adverse_bps = _safe_float(payload.get("hedge_adverse_bps"))
        hedge_adverse_bps = 10.0 if raw_adverse_bps is None else raw_adverse_bps
        hedge_reprice_min_sec = _safe_float(payload.get("hedge_reprice_min_sec")) or 2.0
        fallback_to_market = False
        verbose_logs = bool(payload.get("verbose_logs", True))

        leg_by_label = {leg["label"]: leg for leg in legs}
        expensive_label = self._resolve_expensive_label(payload.get("expensive_leg"), leg_by_label)
        if not expensive_label:
            expensive_label = "long" if "long" in leg_by_label else legs[0]["label"] if legs else ""
        primary_leg = leg_by_label.get(expensive_label)
        hedge_leg = next((leg for leg in legs if leg is not primary_leg), None)
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])

        self._emit_log(
            log_cb,
            "start",
            "manual smart-exit",
            {
                "action": plan.get("action"),
                "mode": "smart-exit",
                "symbol": symbol,
                "qty": qty,
                "primary": primary_leg,
                "hedge": hedge_leg,
                "spread_pct": plan.get("spread_pct"),
            },
        )

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
        contract_sizes = self._contract_sizes_from_constraints(constraints)
        await self._ensure_ws_positions(
            [primary_leg["exchange"], hedge_leg["exchange"]],
            contract_sizes=contract_sizes,
        )
        await self._ensure_ws_orders(
            [primary_leg["exchange"], hedge_leg["exchange"]],
            contract_sizes=contract_sizes,
            symbol=symbol,
            log_cb=log_cb,
        )
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
        min_hedge_qty = (constraints.get(hedge_leg["exchange"]) or {}).get("min_qty_required") or 0.0
        hedge_amount_step = (constraints.get(hedge_leg["exchange"]) or {}).get("amount_step")
        primary_amount_step = (constraints.get(primary_leg["exchange"]) or {}).get("amount_step")
        primary_fallback_min = _min_qty_with_buffer(
            (constraints.get(primary_leg["exchange"]) or {}).get("min_qty_required"),
            primary_amount_step,
        )
        hedge_fallback_min = _min_qty_with_buffer(
            (constraints.get(hedge_leg["exchange"]) or {}).get("min_qty_required"),
            hedge_amount_step,
        )
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        force_chunk = bool(payload.get("force_chunk_qty")) and requested_chunk is not None

        positions, pos_errors = await self._fetch_positions_for_symbol(
            exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
            symbol=symbol,
            allow_ws=True,
            contract_sizes=contract_sizes,
        )
        if pos_errors:
            warnings.extend(pos_errors)
        primary_side = _exit_position_side(primary_leg)
        hedge_side = _exit_position_side(hedge_leg)
        primary_pos_qty = self._sum_position_qty(
            positions,
            exchange=primary_leg["exchange"],
            side=primary_side,
            symbol=symbol,
        )
        hedge_pos_qty = self._sum_position_qty(
            positions,
            exchange=hedge_leg["exchange"],
            side=hedge_side,
            symbol=symbol,
        )
        if primary_pos_qty or hedge_pos_qty:
            self._emit_log(
                log_cb,
                "positions",
                "positions snapshot (pre-exit)",
                {
                    "symbol": symbol,
                    "primary_exchange": primary_leg["exchange"],
                    "hedge_exchange": hedge_leg["exchange"],
                    "primary_qty": primary_pos_qty,
                    "hedge_qty": hedge_pos_qty,
                    "delta": primary_pos_qty - hedge_pos_qty,
                },
            )
        if primary_pos_qty > 0 and hedge_pos_qty > 0:
            position_qty = min(primary_pos_qty, hedge_pos_qty)
            requested_qty = _safe_float(payload.get("qty"))
            requested_notional = _safe_float(payload.get("notional"))
            explicit_target = (
                (requested_qty is not None and requested_qty > 0)
                or (requested_notional is not None and requested_notional > 0)
            )
            if position_qty > 0:
                if explicit_target:
                    if qty > position_qty:
                        warnings.append(
                            f"requested qty {qty:g} exceeds position qty {position_qty:g}; exchange may reject reduce-only"
                        )
                else:
                    if abs(position_qty - qty) > 0:
                        warnings.append(f"qty adjusted to positions: {position_qty:g}")
                    qty = position_qty

        primary_pos_start = primary_pos_qty
        hedge_pos_start = hedge_pos_qty
        primary_pos_last = primary_pos_start
        hedge_pos_last = hedge_pos_start

        remaining = qty
        pending_hedge_qty = 0.0
        hedge_failed = False
        stopped_by_user = False
        started_at = time.time()
        active_order_id: str | None = None
        active_price: float | None = None
        active_qty: float | None = None
        active_filled = 0.0
        active_since: float | None = None
        primary_fill_map: dict[str, float] = {}
        order_qty_map: dict[str, float] = {}
        primary_filled_total = 0.0
        pending_order_ids: set[str] = set()

        def _vlog(event: str, message: str, data: Mapping[str, Any] | None = None) -> None:
            if verbose_logs:
                self._emit_log(log_cb, event, message, data)

        def _track_primary_order(order_id: str | None, qty_hint: float | None = None) -> None:
            if not order_id:
                return
            if order_id not in primary_fill_map:
                primary_fill_map[order_id] = 0.0
            if qty_hint is not None:
                order_qty_map[order_id] = qty_hint

        def _update_primary_fill(
            order_id: str | None,
            filled_qty: float | None,
            *,
            status: Mapping[str, Any] | None = None,
            reason: str | None = None,
        ) -> float:
            nonlocal primary_filled_total
            if not order_id:
                return 0.0
            filled = _safe_float(filled_qty) or 0.0
            prev = primary_fill_map.get(order_id, 0.0)
            if filled <= prev:
                return 0.0
            raw_delta = filled - prev
            remaining_target = max(0.0, qty - primary_filled_total)
            if remaining_target <= 0:
                return 0.0
            delta = min(raw_delta, remaining_target)
            if delta <= 0:
                return 0.0
            primary_fill_map[order_id] = prev + delta
            primary_filled_total += delta
            self._emit_story(
                log_cb,
                f"Primary fill update: {primary_leg.get('exchange')} delta={delta:g} total={primary_filled_total:g} id={order_id}",
                {
                    "exchange": primary_leg.get("exchange"),
                    "order_id": order_id,
                    "delta": delta,
                    "filled_total": primary_filled_total,
                    "reason": reason,
                },
            )
            _vlog(
                "fill",
                "primary fill update",
                {
                    "order_id": order_id,
                    "delta": delta,
                    "filled_qty": filled,
                    "filled_total": primary_filled_total,
                    "reason": reason,
                    "status": status,
                },
            )
            return delta

        async def _apply_primary_fill(
            order_id: str | None,
            filled_qty: float | None,
            *,
            status: Mapping[str, Any] | None = None,
            reason: str,
        ) -> float:
            delta = _update_primary_fill(order_id, filled_qty, status=status, reason=reason)
            if delta > 0:
                await _record_primary_delta(delta)
            return delta

        async def _record_primary_delta(delta: float) -> None:
            nonlocal remaining, pending_hedge_qty
            if delta <= 0:
                return
            remaining = max(0.0, remaining - delta)
            pending_hedge_qty += delta

        async def _sync_primary_from_orders(reason: str) -> tuple[float, bool]:
            nonlocal active_filled, active_order_id, active_price, active_qty, active_since
            if not self._ws_orders_live(primary_leg["exchange"]):
                return 0.0, False
            order_ids = list(pending_order_ids)
            if active_order_id and active_order_id not in order_ids:
                order_ids.append(active_order_id)
            total_delta = 0.0
            used_ws = False
            for order_id in order_ids:
                info = self._ws_order_info(primary_leg["exchange"], order_id)
                if not info:
                    continue
                used_ws = True
                filled_qty = _safe_float(info.get("filled_qty"))
                expected_qty = order_qty_map.get(order_id)
                if filled_qty is not None and expected_qty and filled_qty > expected_qty * 1.02:
                    info = dict(info)
                    filled_qty = expected_qty
                    info["filled_qty"] = filled_qty
                    info["clamped"] = True
                total_delta += await _apply_primary_fill(
                    order_id,
                    filled_qty,
                    status=info,
                    reason=reason,
                )
                state = str(info.get("status") or "").lower()
                if state in ("canceled", "cancelled", "closed", "filled", "finished"):
                    pending_order_ids.discard(order_id)
                    if active_order_id and order_id == active_order_id:
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
            if active_order_id:
                active_filled = primary_fill_map.get(active_order_id, 0.0)
            if not used_ws:
                return 0.0, False
            if total_delta > 0:
                _vlog(
                    "fill",
                    "primary fill update (ws orders)",
                    {
                        "delta": total_delta,
                        "filled_total": primary_filled_total,
                        "reason": reason,
                    },
                )
            return total_delta, True

        async def _sync_primary_fills(
            reason: str,
            *,
            delay: float | None = None,
            include_active: bool = True,
        ) -> float:
            nonlocal active_filled
            if delay:
                await asyncio.sleep(delay)
            ws_delta, used_ws = await _sync_primary_from_orders(reason)
            if await self._ensure_ws_orders_healthy(primary_leg["exchange"], reason=reason, log_cb=log_cb):
                if not used_ws:
                    _vlog(
                        "wait",
                        "primary ws order update missing; waiting",
                        {"exchange": primary_leg["exchange"], "reason": reason},
                    )
                return ws_delta
            _vlog(
                "warn",
                "primary ws order stream dead; using rest",
                {"exchange": primary_leg["exchange"], "reason": reason},
            )
            self._emit_story(
                log_cb,
                f"WS[{primary_leg['exchange']}] stale; using REST order status",
                {"exchange": primary_leg["exchange"], "reason": reason},
            )
            order_ids = list(pending_order_ids)
            if include_active and active_order_id and active_order_id not in order_ids:
                order_ids.append(active_order_id)
            total_delta = 0.0
            for order_id in order_ids:
                status = await self._fetch_order_status(
                    primary_leg,
                    symbol,
                    order_id,
                    expected_qty=order_qty_map.get(order_id),
                    allow_trades_fallback=False,
                )
                if status.get("status") == "error":
                    _vlog(
                        "warn",
                        "primary rest order status failed",
                        {"exchange": primary_leg["exchange"], "order_id": order_id, "error": status.get("error")},
                    )
                    continue
                total_delta += await _apply_primary_fill(
                    order_id,
                    status.get("filled_qty"),
                    status=status,
                    reason=reason,
                )
                state = str(status.get("status") or "").lower()
                if state in ("canceled", "cancelled", "closed", "filled"):
                    pending_order_ids.discard(order_id)
                    if active_order_id and order_id == active_order_id:
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
                    if active_order_id and order_id == active_order_id:
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
            if active_order_id:
                active_filled = primary_fill_map.get(active_order_id, 0.0)
            if primary_filled_total >= qty and remaining > 0:
                _vlog(
                    "guard",
                    "primary target reached",
                    {
                        "filled_total": primary_filled_total,
                        "target_qty": qty,
                        "remaining": remaining,
                    },
                )
            return total_delta

        async def _reconcile_positions(reason: str) -> None:
            nonlocal pending_hedge_qty, primary_pos_last, hedge_pos_last
            primary_current = self._ws_position_qty(primary_leg["exchange"], symbol, primary_side)
            hedge_current = self._ws_position_qty(hedge_leg["exchange"], symbol, hedge_side)
            if primary_current is None or hedge_current is None:
                return
            primary_pos_last = primary_current
            hedge_pos_last = hedge_current
            primary_delta = max(0.0, primary_pos_start - primary_current)
            hedge_delta = max(0.0, hedge_pos_start - hedge_current)
            imbalance = primary_delta - hedge_delta
            pending_hedge_qty = max(0.0, imbalance)
            if abs(imbalance) <= 0:
                return
            if imbalance > 0:
                threshold = hedge_fallback_min
                step = hedge_amount_step
                leg = hedge_leg
                qty_needed = imbalance
            else:
                threshold = primary_fallback_min
                step = primary_amount_step
                leg = primary_leg
                qty_needed = abs(imbalance)
            qty_needed = _round_to_step(qty_needed, step, mode="down") if step else qty_needed
            if qty_needed <= 0:
                return
            if threshold and qty_needed < threshold:
                _vlog(
                    "wait",
                    "reconcile imbalance below fallback threshold",
                    {"reason": reason, "imbalance": imbalance, "min_qty": threshold},
                )
                return
            self._emit_log(
                log_cb,
                "submit",
                f"reconcile market {leg['exchange']} qty={qty_needed:g}",
            )
            result = await self._place_market(
                leg,
                symbol,
                qty_needed,
                payload,
                reason="final_reconcile",
                log_cb=log_cb,
            )
            actions.append(result)
            self._emit_log(log_cb, "result", "reconcile result", result)
            pending_hedge_qty = 0.0

        async def _hedge_pending(reason: str) -> None:
            nonlocal pending_hedge_qty, hedge_failed
            hedge_qty = pending_hedge_qty
            if hedge_qty <= 0:
                return
            pending_hedge_qty = 0.0
            if min_hedge_qty and hedge_qty < min_hedge_qty:
                _vlog(
                    "wait",
                    "hedge below minimum; skipping",
                    {"pending_qty": hedge_qty, "min_qty": min_hedge_qty, "reason": reason},
                )
                return
            if min_hedge_qty:
                hedge_qty = math.floor(hedge_qty / min_hedge_qty) * min_hedge_qty
            hedge_qty = _round_to_step(hedge_qty, hedge_amount_step, mode="down")
            if hedge_qty <= 0:
                return
            self._emit_log(log_cb, "submit", f"hedge {hedge_leg['exchange']} qty={hedge_qty:g}")
            hedge_result = await self._hedge_position(
                hedge_leg,
                symbol,
                hedge_qty,
                hedge_order_type=hedge_order_type,
                hedge_offset_bps=hedge_offset_bps,
                hedge_offset_ticks=hedge_offset_ticks,
                hedge_limit_mode=hedge_limit_mode,
                hedge_favorable_bps=hedge_favorable_bps,
                hedge_adverse_bps=hedge_adverse_bps,
                hedge_reprice_min_sec=hedge_reprice_min_sec,
                payload=payload,
                min_qty_required=min_hedge_qty,
                log_cb=log_cb,
            )
            actions.append(hedge_result)
            self._emit_log(log_cb, "result", "hedge result", hedge_result)
            if hedge_result.get("status") == "error":
                errors.append(
                    f"hedge failed on {hedge_leg['exchange']}: {hedge_result.get('error') or 'unknown_error'}"
                )
                hedge_failed = True
                return
            pending_qty = _safe_float(hedge_result.get("pending_qty"))
            if pending_qty:
                _vlog(
                    "wait",
                    "hedge remainder below minimum; skipping",
                    {"pending_qty": pending_qty, "min_qty": min_hedge_qty, "reason": reason},
                )

        while remaining > 0 and (time.time() - started_at) < max_runtime_sec:
            if self._stop_requested():
                warnings.append("stopped_by_user")
                stopped_by_user = True
                self._emit_log(
                    log_cb,
                    "warn",
                    "manual stop requested; canceling active order",
                    {"exchange": primary_leg["exchange"], "remaining": remaining},
                )
                if active_order_id:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("stop_cancel", delay=0.2, include_active=False)
                break
            snapshot = await self._snapshot_legs(symbol, [primary_leg, hedge_leg], max_slippage_bps=max_slippage_bps)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "orderbook fetch failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, reprice_sec))
                continue
            _vlog(
                "snapshot",
                "smart-exit snapshot",
                {
                    "spread_pct": snapshot.get("spread_pct"),
                    "mid_price": snapshot.get("mid_price"),
                    "primary": _stats_payload(snapshot.get("stats", {}).get(primary_leg["exchange"])),
                    "hedge": _stats_payload(snapshot.get("stats", {}).get(hedge_leg["exchange"])),
                    "sources": snapshot.get("orderbook_sources"),
                },
            )
            spread_val = snapshot.get("spread_pct")
            within_range = self._within_spread(spread_val, spread_min_pct, spread_max_pct)
            if within_range is False:
                if active_order_id:
                    await _sync_primary_fills("spread_cancel")
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    _vlog(
                        "cancel",
                        "active order canceled: spread out of range",
                        {"order_id": active_order_id, "spread_pct": spread_val},
                    )
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_spread_cancel", delay=0.2, include_active=False)
                self._emit_log(log_cb, "wait", f"spread {spread_val:.4f} out of range; waiting")
                if pending_hedge_qty > 0 and active_order_id is None:
                    await _hedge_pending("spread_wait")
                    if hedge_failed:
                        break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            await _sync_primary_fills("loop")

            if active_order_id is None and pending_hedge_qty > 0:
                await _hedge_pending("post_primary")
                if hedge_failed:
                    break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            if pending_order_ids:
                await _sync_primary_fills("pre_chunk", include_active=False)
                if pending_order_ids:
                    _vlog(
                        "wait",
                        "primary cancel pending; waiting",
                        {"exchange": primary_leg["exchange"], "pending": list(pending_order_ids)},
                    )
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue

            max_qty_by_exchange = snapshot.get("max_qty_by_exchange") or {}
            max_chunk = None
            max_candidates = [val for val in max_qty_by_exchange.values() if val]
            if max_candidates:
                max_chunk = min(max_candidates)
            if not force_chunk and max_slippage_bps > 0 and max_chunk is not None:
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
                max_chunk=None if force_chunk else max_chunk,
                amount_step=amount_step,
            )
            warnings.extend(chunk_warnings)
            if not chunk:
                if chunk_warnings:
                    break
                errors.append("Unable to determine chunk size for smart exit.")
                break
            _vlog(
                "decision",
                "smart-exit chunk",
                {
                    "remaining": remaining,
                    "chunk": chunk,
                    "min_chunk_qty": min_chunk_qty,
                    "max_chunk": max_chunk,
                    "amount_step": amount_step,
                },
            )

            price_step = (constraints.get(primary_leg["exchange"]) or {}).get("price_step")
            primary_stats = (snapshot.get("stats") or {}).get(primary_leg["exchange"])
            orderbook = (snapshot.get("orderbooks") or {}).get(primary_leg["exchange"])
            improve_ticks = int(_safe_float(payload.get("limit_improve_ticks")) or DEFAULT_LIMIT_IMPROVE_TICKS)
            exclude_qty = None
            if active_price and active_qty is not None:
                open_qty = max(0.0, (active_qty or 0.0) - (active_filled or 0.0))
                if open_qty > 0:
                    exclude_qty = open_qty
            limit_price = _resolve_smart_limit_price(
                orderbook=orderbook,
                side=primary_leg["side"],
                book_side=None,
                qty=chunk,
                payload=payload,
                price_step=price_step,
                best_bid=primary_stats.best_bid if primary_stats else None,
                best_ask=primary_stats.best_ask if primary_stats else None,
                mid_price=primary_stats.mid if primary_stats else None,
                improve_ticks=improve_ticks,
                offset_bps=limit_offset_bps,
                offset_ticks=limit_offset_ticks,
                round_mode="passive",
                exclude_price=active_price,
                exclude_qty=exclude_qty,
            )
            if limit_price is None:
                errors.append("Unable to resolve limit price for smart exit.")
                break
            _vlog(
                "decision",
                "smart-exit limit price",
                {
                    "limit_price": limit_price,
                    "price_step": price_step,
                    "offset_bps": limit_offset_bps,
                    "offset_ticks": limit_offset_ticks,
                    "improve_ticks": improve_ticks,
                },
            )

            if active_order_id:
                if active_price != limit_price or (active_qty is not None and active_qty != chunk):
                    ws_delta, used_ws = await _sync_primary_from_orders("reprice")
                    if not used_ws:
                        if self._ws_orders_live(primary_leg["exchange"]):
                            self._emit_log(
                                log_cb,
                                "wait",
                                "primary ws order update missing; skipping rest sync",
                                {"exchange": primary_leg["exchange"], "order_id": active_order_id},
                            )
                            status = {"status": "open", "filled_qty": active_filled}
                        else:
                            status = await self._fetch_order_status(
                                primary_leg,
                                symbol,
                                active_order_id,
                                expected_qty=active_qty or order_qty_map.get(active_order_id),
                                allow_trades_fallback=False,
                            )
                            if status.get("status") == "error":
                                self._emit_log(
                                    log_cb,
                                    "warn",
                                    "primary rest order status failed",
                                    {"exchange": primary_leg["exchange"], "order_id": active_order_id, "error": status.get("error")},
                                )
                                status = {"status": "open", "filled_qty": active_filled}
                        filled_qty = _safe_float(status.get("filled_qty")) or 0.0
                        delta = max(0.0, filled_qty - active_filled)
                        if delta > 0:
                            active_filled = filled_qty
                            await _record_primary_delta(delta)
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final status before cancel (reprice)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "status": status,
                            },
                        )
                    else:
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final order ws sync before cancel (reprice)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "ws_delta": ws_delta,
                                "filled_total": primary_filled_total,
                            },
                        )
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    _vlog(
                        "cancel",
                        "active order canceled: repriced or resized",
                        {
                            "order_id": active_order_id,
                            "prev_price": active_price,
                            "new_price": limit_price,
                            "prev_qty": active_qty,
                            "new_qty": chunk,
                        },
                    )
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_reprice_cancel", delay=0.2, include_active=False)

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
                    log_cb=log_cb,
                )
                actions.append(submit)
                if submit.get("status") == "error":
                    errors.append(submit.get("error") or "limit_submit_failed")
                    _vlog("error", "limit submit failed", submit)
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
                _track_primary_order(order_id, chunk)
                initial_filled = _safe_float(submit.get("filled_qty")) or 0.0
                if initial_filled > 0:
                    await _apply_primary_fill(
                        order_id,
                        initial_filled,
                        status=submit,
                        reason="submit",
                    )
                    active_filled = primary_fill_map.get(order_id, 0.0)

            if active_order_id:
                if timeout > 0 and active_since and (time.time() - active_since) > timeout:
                    ws_delta, used_ws = await _sync_primary_from_orders("timeout_cancel")
                    if not used_ws:
                        if self._ws_orders_live(primary_leg["exchange"]):
                            self._emit_log(
                                log_cb,
                                "wait",
                                "primary ws order update missing; skipping rest sync",
                                {"exchange": primary_leg["exchange"], "order_id": active_order_id},
                            )
                            status = {"status": "open", "filled_qty": active_filled}
                        else:
                            status = await self._fetch_order_status(
                                primary_leg,
                                symbol,
                                active_order_id,
                                expected_qty=active_qty or order_qty_map.get(active_order_id),
                                allow_trades_fallback=False,
                            )
                            if status.get("status") == "error":
                                self._emit_log(
                                    log_cb,
                                    "warn",
                                    "primary rest order status failed",
                                    {"exchange": primary_leg["exchange"], "order_id": active_order_id, "error": status.get("error")},
                                )
                                status = {"status": "open", "filled_qty": active_filled}
                        await _apply_primary_fill(
                            active_order_id,
                            status.get("filled_qty"),
                            status=status,
                            reason="timeout_cancel",
                        )
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final status before cancel (timeout)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "status": status,
                            },
                        )
                    else:
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final order ws sync before cancel (timeout)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "ws_delta": ws_delta,
                                "filled_total": primary_filled_total,
                            },
                        )
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_timeout_cancel", delay=0.2, include_active=False)
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                if self._ws_orders_live(primary_leg["exchange"]):
                    await _sync_primary_from_orders("limit_wait")
                    await asyncio.sleep(max(0.2, reprice_sec))
                else:
                    limit_wait_sec = max(2, int(reprice_sec or 1) + 1)
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
                        log_cb=log_cb,
                    )
                    self._emit_log(log_cb, "result", "limit result", limit_result)
                    filled_qty = _safe_float(limit_result.get("filled_qty")) or 0.0
                    delta = max(0.0, filled_qty - active_filled)
                    if delta > 0:
                        active_filled = filled_qty
                        remaining = max(0.0, remaining - delta)
                        pending_hedge_qty += delta
                    if limit_result.get("status") in ("filled", "closed"):
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
                    if limit_result.get("cancel_reason"):
                        if active_order_id:
                            pending_order_ids.add(active_order_id)
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None

            if active_order_id is None and pending_hedge_qty > 0:
                await _hedge_pending("post_primary_loop")
                if hedge_failed:
                    break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            if remaining <= 0:
                break
            await asyncio.sleep(max(0.2, reprice_sec))


        if pending_hedge_qty > 0 and not hedge_failed and not stopped_by_user:
            await _hedge_pending("final_pending")

        if active_order_id:
            await self._cancel_order(primary_leg, symbol, active_order_id)
            active_order_id = None

        if remaining > 0:
            warnings.append(f"Remaining qty {remaining:g} not exited (smart-exit runtime ended).")

        if primary_leg and hedge_leg:
            post_positions, post_errors = await self._fetch_positions_for_symbol(
                exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
                symbol=symbol,
                allow_ws=True,
                contract_sizes=contract_sizes,
            )
            if post_errors:
                warnings.extend(post_errors)
            exchange_list = [
                normalize_exchange_name(primary_leg["exchange"]),
                normalize_exchange_name(hedge_leg["exchange"]),
            ]
            sources = {
                exchange: ("ws" if self._ws_live(exchange) else "rest")
                for exchange in exchange_list
                if exchange
            }
            counts: dict[str, int] = {}
            for entry in post_positions:
                exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
                if not exchange:
                    continue
                counts[exchange] = counts.get(exchange, 0) + 1
            self._emit_log(
                log_cb,
                "positions",
                "positions snapshot (pre-final-reconcile)",
                {
                    "stage": "pre-final-reconcile",
                    "symbol": symbol,
                    "exchanges": exchange_list,
                    "positions": post_positions,
                    "counts": counts,
                    "errors": post_errors,
                    "sources": sources,
                },
            )
            primary_current = self._sum_position_qty(
                post_positions,
                exchange=primary_leg["exchange"],
                side=primary_side,
                symbol=symbol,
            )
            hedge_current = self._sum_position_qty(
                post_positions,
                exchange=hedge_leg["exchange"],
                side=hedge_side,
                symbol=symbol,
            )
            primary_delta = max(0.0, primary_pos_start - primary_current)
            hedge_delta = max(0.0, hedge_pos_start - hedge_current)
            imbalance = primary_delta - hedge_delta
            if abs(imbalance) > 0:
                if imbalance > 0:
                    threshold = hedge_fallback_min
                    step = hedge_amount_step
                    leg = hedge_leg
                    qty_needed = imbalance
                else:
                    threshold = primary_fallback_min
                    step = primary_amount_step
                    leg = primary_leg
                    qty_needed = abs(imbalance)
                qty_needed = _round_to_step(qty_needed, step, mode="down") if step else qty_needed
                if qty_needed > 0:
                    if threshold and qty_needed < threshold:
                        _vlog(
                            "wait",
                            "final imbalance below fallback threshold",
                            {"imbalance": imbalance, "min_qty": threshold},
                        )
                    else:
                        self._emit_log(
                            log_cb,
                            "submit",
                            f"final reconcile market {leg['exchange']} qty={qty_needed:g}",
                        )
                        result = await self._place_market(
                            leg,
                            symbol,
                            qty_needed,
                            payload,
                            reason="final_reconcile",
                            log_cb=log_cb,
                        )
                        actions.append(result)
                        self._emit_log(log_cb, "result", "final reconcile result", result)

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
        market_refill_bps = _safe_float(payload.get("market_refill_bps"))
        if market_refill_bps is None:
            market_refill_bps = 10.0
        market_refill_buffer = _safe_float(payload.get("market_refill_buffer"))
        if market_refill_buffer is None:
            market_refill_buffer = 0.15
        market_refill_max_wait_sec = _safe_float(payload.get("market_refill_max_wait_sec")) or 5.0
        market_fill_timeout_sec = _safe_float(payload.get("market_fill_timeout_sec")) or 3.0
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        force_chunk = bool(payload.get("force_chunk_qty")) and requested_chunk is not None
        constraints = plan.get("market_constraints") or {}
        contract_sizes = self._contract_sizes_from_constraints(constraints)
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
        per_leg_amount_step = {
            leg["exchange"]: (constraints.get(leg["exchange"]) or {}).get("amount_step")
            for leg in legs
        }
        per_leg_min_qty = {
            leg["exchange"]: (constraints.get(leg["exchange"]) or {}).get("min_qty_required")
            for leg in legs
        }
        per_leg_min_buffer = {
            exchange: _min_qty_with_buffer(min_qty, per_leg_amount_step.get(exchange))
            for exchange, min_qty in per_leg_min_qty.items()
        }

        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])
        remaining = qty
        started_at = time.time()
        last_book_ts: dict[str, float | None] = {leg["exchange"]: None for leg in legs}
        exchange_list = [leg["exchange"] for leg in legs]
        stopped_by_user = False
        stopped_by_user = False

        def emit_positions_snapshot(stage: str, positions: list[dict[str, Any]], pos_errors: list[str]) -> None:
            counts: dict[str, int] = {}
            for entry in positions:
                exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
                counts[exchange] = counts.get(exchange, 0) + 1
            self._emit_log(
                log_cb,
                "positions",
                f"positions snapshot ({stage})",
                {
                    "stage": stage,
                    "symbol": symbol,
                    "exchanges": exchange_list,
                    "positions": positions,
                    "counts": counts,
                    "errors": pos_errors,
                },
            )

        self._emit_log(
            log_cb,
            "start",
            "manual fast-exit",
            {
                "action": plan.get("action"),
                "mode": "fast-exit",
                "symbol": symbol,
                "qty": qty,
                "legs": legs,
                "spread_pct": plan.get("spread_pct"),
            },
        )
        primary_name = primary_leg.get("exchange") if primary_leg else "-"
        hedge_name = hedge_leg.get("exchange") if hedge_leg else "-"
        primary_side = primary_leg.get("side") if primary_leg else "-"
        hedge_side = hedge_leg.get("side") if hedge_leg else "-"
        self._emit_story(
            log_cb,
            f"Start smart-exit: symbol={symbol} qty={qty:g} primary={primary_name}({primary_side}) hedge={hedge_name}({hedge_side})",
            {
                "action": plan.get("action"),
                "mode": "smart-exit",
                "symbol": symbol,
                "qty": qty,
                "primary_exchange": primary_name,
                "hedge_exchange": hedge_name,
            },
        )

        await self._ensure_ws_orders(
            exchange_list,
            contract_sizes=contract_sizes,
            symbol=symbol,
            log_cb=log_cb,
        )
        start_positions, start_errors = await self._fetch_positions_for_symbol(
            exchanges=exchange_list,
            symbol=symbol,
            allow_ws=False,
            contract_sizes=contract_sizes,
        )
        if start_errors:
            warnings.extend(start_errors)
        emit_positions_snapshot("start", start_positions, start_errors)
        start_qty_by_exchange = {
            leg["exchange"]: self._sum_position_qty(
                start_positions,
                exchange=leg["exchange"],
                side=_exit_position_side(leg),
                symbol=symbol,
            )
            for leg in legs
        }

        try:
            while remaining > 0 and (time.time() - started_at) < max_runtime_sec:
                if self._stop_requested():
                    warnings.append("stopped_by_user")
                    stopped_by_user = True
                    self._emit_log(
                        log_cb,
                        "warn",
                        "manual stop requested; halting",
                        {"remaining": remaining},
                    )
                    break
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
                if not force_chunk and max_slippage_bps > 0 and max_chunk is not None:
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
                    max_chunk=None if force_chunk else max_chunk,
                    amount_step=amount_step,
                )
                warnings.extend(chunk_warnings)
                if not chunk:
                    if chunk_warnings:
                        break
                    errors.append("Unable to determine chunk size for fast exit.")
                    break

                if any(last_book_ts.values()):
                    refill_deadline = time.time() + max(0.0, market_refill_max_wait_sec)
                    while True:
                        orderbooks = snapshot.get("orderbooks") or {}
                        refill_ready = True
                        refill_liqs = []
                        for leg in legs:
                            exchange = leg["exchange"]
                            orderbook = orderbooks.get(exchange)
                            orderbook_ts = self._orderbook_timestamp(orderbook)
                            last_ts = last_book_ts.get(exchange)
                            if last_ts and orderbook_ts and orderbook_ts <= last_ts:
                                refill_ready = False
                            liq = self._orderbook_refill_qty(
                                orderbook,
                                side=leg["side"],
                                max_bps=market_refill_bps,
                            )
                            refill_liqs.append(liq)
                        available = min(refill_liqs) if refill_liqs else 0.0
                        target_needed = chunk * (1.0 + market_refill_buffer)
                        if available < target_needed:
                            refill_ready = False
                        if refill_ready:
                            break
                        if time.time() >= refill_deadline:
                            adjusted = available / (1.0 + market_refill_buffer) if available > 0 else 0.0
                            adjusted = _round_to_step(adjusted, amount_step, mode="down") if amount_step else adjusted
                            if min_chunk_qty and adjusted < min_chunk_qty:
                                warnings.append("remaining qty below exchange minimum; unable to execute final chunk")
                                chunk = 0.0
                                break
                            if adjusted > 0:
                                if adjusted < chunk:
                                    self._emit_log(
                                        log_cb,
                                        "wait",
                                        "market refill timeout; reducing chunk",
                                        {"chunk": chunk, "adjusted": adjusted, "available": available},
                                    )
                                chunk = min(chunk, adjusted)
                            break
                        self._emit_log(
                            log_cb,
                            "wait",
                            "market refill waiting",
                            {"required": chunk, "available": available, "buffer": market_refill_buffer},
                        )
                        await asyncio.sleep(max(0.2, reprice_sec))
                        snapshot = await self._snapshot_legs(symbol, legs, max_slippage_bps=max_slippage_bps)
                        if snapshot.get("errors"):
                            continue
                    if not chunk:
                        break

                orderbooks = snapshot.get("orderbooks") or {}
                for leg in legs:
                    last_book_ts[leg["exchange"]] = self._orderbook_timestamp(orderbooks.get(leg["exchange"])) or time.time()

                submit_tasks = []
                for leg in legs:
                    self._emit_log(log_cb, "submit", f"market {leg['exchange']} qty={chunk:g}")
                    submit_tasks.append(
                        self._place_market(leg, symbol, chunk, payload, reason="fast_market", log_cb=log_cb)
                    )
                submit_results = await asyncio.gather(*submit_tasks)
                actions.extend(submit_results)

                fill_tasks = []
                for leg, submit in zip(legs, submit_results):
                    fill_tasks.append(
                        self._await_order_fill(
                            leg,
                            symbol,
                            submit.get("order_id"),
                            chunk,
                            market_fill_timeout_sec,
                            log_cb=log_cb,
                        )
                    )
                fill_results = await asyncio.gather(*fill_tasks)

                filled_by_exchange: dict[str, float] = {}
                blocked = False
                for leg, fill in zip(legs, fill_results):
                    exchange = leg["exchange"]
                    filled_qty = _safe_float(fill.get("filled_qty")) or 0.0
                    filled_by_exchange[exchange] = filled_qty
                    if fill.get("status") == "error":
                        errors.append(f"{exchange}: market fill error ({fill.get('error')})")
                        blocked = True
                if blocked:
                    break

                for leg, fill in zip(legs, fill_results):
                    exchange = leg["exchange"]
                    filled_qty = filled_by_exchange.get(exchange, 0.0)
                    remaining_leg = max(0.0, chunk - filled_qty)
                    min_needed = per_leg_min_buffer.get(exchange)
                    if remaining_leg <= 0:
                        continue
                    if min_needed and remaining_leg < min_needed:
                        warnings.append("remaining qty below exchange minimum; unable to execute final chunk")
                        blocked = True
                        continue
                    self._emit_log(log_cb, "submit", f"market top-up {exchange} qty={remaining_leg:g}")
                    topup = await self._place_market(
                        leg, symbol, remaining_leg, payload, reason="fast_market_topup", log_cb=log_cb
                    )
                    actions.append(topup)
                    topup_fill = await self._await_order_fill(
                        leg,
                        symbol,
                        topup.get("order_id"),
                        remaining_leg,
                        market_fill_timeout_sec,
                        log_cb=log_cb,
                    )
                    filled_qty += _safe_float(topup_fill.get("filled_qty")) or 0.0
                    filled_by_exchange[exchange] = filled_qty
                if blocked:
                    break

                filled_values = list(filled_by_exchange.values())
                matched = min(filled_values) if filled_values else 0.0
                imbalance = max(filled_values) - matched if filled_values else 0.0
                tolerance = amount_step or 0.0
                if matched <= 0:
                    warnings.append("market chunk produced no fills; stopping")
                    break
                if imbalance > tolerance:
                    warnings.append("market legs mismatch; stopping for final reconcile")
                    remaining = max(0.0, remaining - matched)
                    break
                remaining = max(0.0, remaining - matched)
                await asyncio.sleep(0.1)
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"fast-exit runtime error: {exc}")
        finally:
            end_positions, end_errors = await self._fetch_positions_for_symbol(
                exchanges=exchange_list,
                symbol=symbol,
                allow_ws=False,
                contract_sizes=contract_sizes,
            )
            if end_errors:
                warnings.extend(end_errors)
            emit_positions_snapshot("end", end_positions, end_errors)
            if stopped_by_user:
                self._emit_log(log_cb, "warn", "stop requested; skipping final reconcile", {"remaining": remaining})
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
            if len(legs) >= 2:
                deltas: dict[str, float] = {}
                for leg in legs:
                    exchange = leg["exchange"]
                    end_qty = self._sum_position_qty(
                        end_positions,
                        exchange=exchange,
                        side=_exit_position_side(leg),
                        symbol=symbol,
                    )
                    start_qty = start_qty_by_exchange.get(exchange, 0.0)
                    delta = max(0.0, start_qty - end_qty)
                    deltas[exchange] = delta
                primary = legs[0]
                secondary = legs[1]
                delta_primary = deltas.get(primary["exchange"], 0.0)
                delta_secondary = deltas.get(secondary["exchange"], 0.0)
                imbalance = delta_primary - delta_secondary
                if abs(imbalance) > (amount_step or 0.0):
                    if imbalance > 0:
                        lag_leg = secondary
                        qty_needed = imbalance
                    else:
                        lag_leg = primary
                        qty_needed = abs(imbalance)
                    qty_needed = _round_to_step(
                        qty_needed,
                        per_leg_amount_step.get(lag_leg["exchange"]),
                        mode="down",
                    )
                    min_needed = per_leg_min_buffer.get(lag_leg["exchange"])
                    if qty_needed > 0 and (not min_needed or qty_needed >= min_needed):
                        self._emit_log(
                            log_cb,
                            "submit",
                            f"final reconcile market {lag_leg['exchange']} qty={qty_needed:g}",
                        )
                        result = await self._place_market(
                            lag_leg,
                            symbol,
                            qty_needed,
                            payload,
                            reason="final_reconcile",
                            log_cb=log_cb,
                        )
                        actions.append(result)
                        self._emit_log(log_cb, "result", "final reconcile result", result)

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
        timeout = _resolve_timeout(payload, 10)
        reprice_sec = _safe_float(payload.get("reprice_sec")) or 2.0
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 60)
        limit_offset_bps = _safe_float(payload.get("limit_offset_bps")) or 0.0
        limit_offset_ticks = int(_safe_float(payload.get("limit_offset_ticks")) or 0)
        hedge_order_type = str(payload.get("hedge_order_type") or "market").lower()
        hedge_offset_bps = _safe_float(payload.get("hedge_offset_bps")) or 2.0
        hedge_offset_ticks = int(_safe_float(payload.get("hedge_offset_ticks")) or 0)
        hedge_limit_mode = str(payload.get("hedge_limit_mode") or "passive").lower()
        hedge_favorable_bps = _safe_float(payload.get("hedge_favorable_bps")) or 2.0
        raw_adverse_bps = _safe_float(payload.get("hedge_adverse_bps"))
        hedge_adverse_bps = 10.0 if raw_adverse_bps is None else raw_adverse_bps
        hedge_reprice_min_sec = _safe_float(payload.get("hedge_reprice_min_sec")) or 2.0
        fallback_to_market = False
        verbose_logs = bool(payload.get("verbose_logs", True))

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

        self._emit_log(
            log_cb,
            "start",
            "manual smart-enter",
            {
                "action": plan.get("action"),
                "mode": "smart-enter",
                "symbol": symbol,
                "qty": qty,
                "primary": primary_leg,
                "hedge": hedge_leg,
                "spread_pct": plan.get("spread_pct"),
            },
        )

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
        contract_sizes = self._contract_sizes_from_constraints(constraints)
        await self._ensure_ws_positions(
            [primary_leg["exchange"], hedge_leg["exchange"]],
            contract_sizes=contract_sizes,
        )
        await self._ensure_ws_orders(
            [primary_leg["exchange"], hedge_leg["exchange"]],
            contract_sizes=contract_sizes,
            symbol=symbol,
            log_cb=log_cb,
        )
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
        min_hedge_qty = (constraints.get(hedge_leg["exchange"]) or {}).get("min_qty_required") or 0.0
        hedge_amount_step = (constraints.get(hedge_leg["exchange"]) or {}).get("amount_step")
        primary_amount_step = (constraints.get(primary_leg["exchange"]) or {}).get("amount_step")
        primary_fallback_min = _min_qty_with_buffer(
            (constraints.get(primary_leg["exchange"]) or {}).get("min_qty_required"),
            primary_amount_step,
        )
        hedge_fallback_min = _min_qty_with_buffer(
            (constraints.get(hedge_leg["exchange"]) or {}).get("min_qty_required"),
            hedge_amount_step,
        )
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        force_chunk = bool(payload.get("force_chunk_qty")) and requested_chunk is not None

        primary_side = _entry_position_side(primary_leg)
        hedge_side = _entry_position_side(hedge_leg)
        positions, pos_errors = await self._fetch_positions_for_symbol(
            exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
            symbol=symbol,
            allow_ws=True,
            contract_sizes=contract_sizes,
        )
        if pos_errors:
            warnings.extend(pos_errors)
        primary_pos_start = self._sum_position_qty(
            positions,
            exchange=primary_leg["exchange"],
            side=primary_side,
            symbol=symbol,
        )
        hedge_pos_start = self._sum_position_qty(
            positions,
            exchange=hedge_leg["exchange"],
            side=hedge_side,
            symbol=symbol,
        )

        remaining = qty
        pending_hedge_qty = 0.0
        hedge_failed = False
        stopped_by_user = False
        started_at = time.time()
        active_order_id: str | None = None
        active_price: float | None = None
        active_qty: float | None = None
        active_filled = 0.0
        active_since: float | None = None
        primary_fill_map: dict[str, float] = {}
        order_qty_map: dict[str, float] = {}
        primary_filled_total = 0.0
        pending_order_ids: set[str] = set()

        def _vlog(event: str, message: str, data: Mapping[str, Any] | None = None) -> None:
            if verbose_logs:
                self._emit_log(log_cb, event, message, data)

        def _track_primary_order(order_id: str | None, qty_hint: float | None = None) -> None:
            if not order_id:
                return
            if order_id not in primary_fill_map:
                primary_fill_map[order_id] = 0.0
            if qty_hint is not None:
                order_qty_map[order_id] = qty_hint

        def _update_primary_fill(
            order_id: str | None,
            filled_qty: float | None,
            *,
            status: Mapping[str, Any] | None = None,
            reason: str | None = None,
        ) -> float:
            nonlocal primary_filled_total
            if not order_id:
                return 0.0
            filled = _safe_float(filled_qty) or 0.0
            prev = primary_fill_map.get(order_id, 0.0)
            if filled <= prev:
                return 0.0
            raw_delta = filled - prev
            remaining_target = max(0.0, qty - primary_filled_total)
            if remaining_target <= 0:
                return 0.0
            delta = min(raw_delta, remaining_target)
            if delta <= 0:
                return 0.0
            primary_fill_map[order_id] = prev + delta
            primary_filled_total += delta
            self._emit_story(
                log_cb,
                f"Primary fill update: {primary_leg.get('exchange')} delta={delta:g} total={primary_filled_total:g} id={order_id}",
                {
                    "exchange": primary_leg.get("exchange"),
                    "order_id": order_id,
                    "delta": delta,
                    "filled_total": primary_filled_total,
                    "reason": reason,
                },
            )
            _vlog(
                "fill",
                "primary fill update",
                {
                    "order_id": order_id,
                    "delta": delta,
                    "filled_qty": filled,
                    "filled_total": primary_filled_total,
                    "reason": reason,
                    "status": status,
                },
            )
            return delta

        async def _apply_primary_fill(
            order_id: str | None,
            filled_qty: float | None,
            *,
            status: Mapping[str, Any] | None = None,
            reason: str,
        ) -> float:
            delta = _update_primary_fill(order_id, filled_qty, status=status, reason=reason)
            if delta > 0:
                await _record_primary_delta(delta)
            return delta

        async def _record_primary_delta(delta: float) -> None:
            nonlocal remaining, pending_hedge_qty
            if delta <= 0:
                return
            remaining = max(0.0, remaining - delta)
            pending_hedge_qty += delta

        async def _sync_primary_from_orders(reason: str) -> tuple[float, bool]:
            nonlocal active_filled, active_order_id, active_price, active_qty, active_since
            if not self._ws_orders_live(primary_leg["exchange"]):
                return 0.0, False
            order_ids = list(pending_order_ids)
            if active_order_id and active_order_id not in order_ids:
                order_ids.append(active_order_id)
            total_delta = 0.0
            used_ws = False
            for order_id in order_ids:
                info = self._ws_order_info(primary_leg["exchange"], order_id)
                if not info:
                    continue
                used_ws = True
                filled_qty = _safe_float(info.get("filled_qty"))
                expected_qty = order_qty_map.get(order_id)
                if filled_qty is not None and expected_qty and filled_qty > expected_qty * 1.02:
                    info = dict(info)
                    filled_qty = expected_qty
                    info["filled_qty"] = filled_qty
                    info["clamped"] = True
                total_delta += await _apply_primary_fill(
                    order_id,
                    filled_qty,
                    status=info,
                    reason=reason,
                )
                state = str(info.get("status") or "").lower()
                if state in ("canceled", "cancelled", "closed", "filled", "finished"):
                    pending_order_ids.discard(order_id)
                    if active_order_id and order_id == active_order_id:
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
                    if active_order_id and order_id == active_order_id:
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
            if active_order_id:
                active_filled = primary_fill_map.get(active_order_id, 0.0)
            if not used_ws:
                return 0.0, False
            if total_delta > 0:
                _vlog(
                    "fill",
                    "primary fill update (ws orders)",
                    {
                        "delta": total_delta,
                        "filled_total": primary_filled_total,
                        "reason": reason,
                    },
                )
            return total_delta, True

        async def _sync_primary_fills(
            reason: str,
            *,
            delay: float | None = None,
            include_active: bool = True,
        ) -> float:
            nonlocal active_filled
            if delay:
                await asyncio.sleep(delay)
            ws_delta, used_ws = await _sync_primary_from_orders(reason)
            if await self._ensure_ws_orders_healthy(primary_leg["exchange"], reason=reason, log_cb=log_cb):
                if not used_ws:
                    _vlog(
                        "wait",
                        "primary ws order update missing; waiting",
                        {"exchange": primary_leg["exchange"], "reason": reason},
                    )
                return ws_delta
            _vlog(
                "warn",
                "primary ws order stream dead; using rest",
                {"exchange": primary_leg["exchange"], "reason": reason},
            )
            self._emit_story(
                log_cb,
                f"WS[{primary_leg['exchange']}] stale; using REST order status",
                {"exchange": primary_leg["exchange"], "reason": reason},
            )
            order_ids = list(pending_order_ids)
            if include_active and active_order_id and active_order_id not in order_ids:
                order_ids.append(active_order_id)
            total_delta = 0.0
            for order_id in order_ids:
                status = await self._fetch_order_status(
                    primary_leg,
                    symbol,
                    order_id,
                    expected_qty=order_qty_map.get(order_id),
                    allow_trades_fallback=False,
                )
                if status.get("status") == "error":
                    _vlog(
                        "warn",
                        "primary rest order status failed",
                        {"exchange": primary_leg["exchange"], "order_id": order_id, "error": status.get("error")},
                    )
                    continue
                total_delta += await _apply_primary_fill(
                    order_id,
                    status.get("filled_qty"),
                    status=status,
                    reason=reason,
                )
                state = str(status.get("status") or "").lower()
                if state in ("canceled", "cancelled", "closed", "filled"):
                    pending_order_ids.discard(order_id)
                    if active_order_id and order_id == active_order_id:
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
            if active_order_id:
                active_filled = primary_fill_map.get(active_order_id, 0.0)
            if primary_filled_total >= qty and remaining > 0:
                _vlog(
                    "guard",
                    "primary target reached",
                    {
                        "filled_total": primary_filled_total,
                        "target_qty": qty,
                        "remaining": remaining,
                    },
                )
            return total_delta

        async def _final_reconcile_positions(reason: str) -> None:
            positions, pos_errors = await self._fetch_positions_for_symbol(
                exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
                symbol=symbol,
                allow_ws=True,
                contract_sizes=contract_sizes,
            )
            if pos_errors:
                warnings.extend(pos_errors)
            exchange_list = [
                normalize_exchange_name(primary_leg["exchange"]),
                normalize_exchange_name(hedge_leg["exchange"]),
            ]
            sources = {
                exchange: ("ws" if self._ws_live(exchange) else "rest")
                for exchange in exchange_list
                if exchange
            }
            counts: dict[str, int] = {}
            for entry in positions:
                exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
                if not exchange:
                    continue
                counts[exchange] = counts.get(exchange, 0) + 1
            self._emit_log(
                log_cb,
                "positions",
                "positions snapshot (pre-final-reconcile)",
                {
                    "stage": "pre-final-reconcile",
                    "symbol": symbol,
                    "exchanges": exchange_list,
                    "positions": positions,
                    "counts": counts,
                    "errors": pos_errors,
                    "sources": sources,
                },
            )
            primary_current = self._sum_position_qty(
                positions,
                exchange=primary_leg["exchange"],
                side=primary_side,
                symbol=symbol,
            )
            hedge_current = self._sum_position_qty(
                positions,
                exchange=hedge_leg["exchange"],
                side=hedge_side,
                symbol=symbol,
            )
            primary_delta = max(0.0, primary_current - primary_pos_start)
            hedge_delta = max(0.0, hedge_current - hedge_pos_start)
            imbalance = primary_delta - hedge_delta
            if abs(imbalance) <= 0:
                return
            if imbalance > 0:
                threshold = hedge_fallback_min
                step = hedge_amount_step
                leg = hedge_leg
                qty_needed = imbalance
            else:
                threshold = primary_fallback_min
                step = primary_amount_step
                leg = primary_leg
                qty_needed = abs(imbalance)
            qty_needed = _round_to_step(qty_needed, step, mode="down") if step else qty_needed
            if qty_needed <= 0:
                return
            if threshold and qty_needed < threshold:
                _vlog(
                    "wait",
                    "final imbalance below fallback threshold",
                    {
                        "reason": reason,
                        "imbalance": imbalance,
                        "min_qty": threshold,
                    },
                )
                return
            self._emit_log(
                log_cb,
                "submit",
                f"final reconcile market {leg['exchange']} qty={qty_needed:g}",
            )
            result = await self._place_market(
                leg,
                symbol,
                qty_needed,
                payload,
                reason="final_reconcile",
                log_cb=log_cb,
            )
            actions.append(result)
            self._emit_log(log_cb, "result", "final reconcile result", result)

        async def _hedge_pending(reason: str) -> None:
            nonlocal pending_hedge_qty, hedge_failed
            hedge_qty = pending_hedge_qty
            if hedge_qty <= 0:
                return
            pending_hedge_qty = 0.0
            if min_hedge_qty and hedge_qty < min_hedge_qty:
                _vlog(
                    "wait",
                    "hedge below minimum; skipping",
                    {"pending_qty": hedge_qty, "min_qty": min_hedge_qty, "reason": reason},
                )
                return
            if min_hedge_qty:
                hedge_qty = math.floor(hedge_qty / min_hedge_qty) * min_hedge_qty
            hedge_qty = _round_to_step(hedge_qty, hedge_amount_step, mode="down")
            if hedge_qty <= 0:
                return
            self._emit_log(log_cb, "submit", f"hedge {hedge_leg['exchange']} qty={hedge_qty:g}")
            hedge_result = await self._hedge_position(
                hedge_leg,
                symbol,
                hedge_qty,
                hedge_order_type=hedge_order_type,
                hedge_offset_bps=hedge_offset_bps,
                hedge_offset_ticks=hedge_offset_ticks,
                hedge_limit_mode=hedge_limit_mode,
                hedge_favorable_bps=hedge_favorable_bps,
                hedge_adverse_bps=hedge_adverse_bps,
                hedge_reprice_min_sec=hedge_reprice_min_sec,
                payload=payload,
                min_qty_required=min_hedge_qty,
                log_cb=log_cb,
            )
            actions.append(hedge_result)
            self._emit_log(log_cb, "result", "hedge result", hedge_result)
            if hedge_result.get("status") == "error":
                errors.append(
                    f"hedge failed on {hedge_leg['exchange']}: {hedge_result.get('error') or 'unknown_error'}"
                )
                hedge_failed = True
                return
            pending_qty = _safe_float(hedge_result.get("pending_qty"))
            if pending_qty:
                _vlog(
                    "wait",
                    "hedge remainder below minimum; skipping",
                    {"pending_qty": pending_qty, "min_qty": min_hedge_qty, "reason": reason},
                )

        while remaining > 0 and (
            max_runtime_sec is None or (time.time() - started_at) < max_runtime_sec
        ):
            if self._stop_requested():
                warnings.append("stopped_by_user")
                stopped_by_user = True
                self._emit_log(
                    log_cb,
                    "warn",
                    "manual stop requested; canceling active order",
                    {"exchange": primary_leg["exchange"], "remaining": remaining},
                )
                if active_order_id:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("stop_cancel", delay=0.2, include_active=False)
                break
            snapshot = await self._snapshot_legs(symbol, [primary_leg, hedge_leg], max_slippage_bps=max_slippage_bps)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "orderbook fetch failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, reprice_sec))
                continue
            _vlog(
                "snapshot",
                "smart-enter snapshot",
                {
                    "spread_pct": snapshot.get("spread_pct"),
                    "mid_price": snapshot.get("mid_price"),
                    "primary": _stats_payload(snapshot.get("stats", {}).get(primary_leg["exchange"])),
                    "hedge": _stats_payload(snapshot.get("stats", {}).get(hedge_leg["exchange"])),
                    "sources": snapshot.get("orderbook_sources"),
                },
            )
            spread_val = snapshot.get("spread_pct")
            within_range = self._within_spread(spread_val, spread_min_pct, spread_max_pct)
            if within_range is False:
                if active_order_id:
                    ws_delta, used_ws = await _sync_primary_from_orders("spread_cancel")
                    if not used_ws:
                        if self._ws_orders_live(primary_leg["exchange"]):
                            self._emit_log(
                                log_cb,
                                "wait",
                                "primary ws order update missing; skipping rest sync",
                                {"exchange": primary_leg["exchange"], "order_id": active_order_id},
                            )
                            status = {"status": "open", "filled_qty": active_filled}
                        else:
                            status = await self._fetch_order_status(
                                primary_leg,
                                symbol,
                                active_order_id,
                                expected_qty=active_qty or order_qty_map.get(active_order_id),
                                allow_trades_fallback=False,
                            )
                            if status.get("status") == "error":
                                self._emit_log(
                                    log_cb,
                                    "warn",
                                    "primary rest order status failed",
                                    {"exchange": primary_leg["exchange"], "order_id": active_order_id, "error": status.get("error")},
                                )
                                status = {"status": "open", "filled_qty": active_filled}
                        await _apply_primary_fill(
                            active_order_id,
                            status.get("filled_qty"),
                            status=status,
                            reason="spread_cancel",
                        )
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final status before cancel (spread)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "status": status,
                            },
                        )
                    else:
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final order ws sync before cancel (spread)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "ws_delta": ws_delta,
                                "filled_total": primary_filled_total,
                            },
                        )
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    _vlog(
                        "cancel",
                        "active order canceled: spread out of range",
                        {"order_id": active_order_id, "spread_pct": spread_val},
                    )
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_spread_cancel", delay=0.2, include_active=False)
                self._emit_log(log_cb, "wait", f"spread {spread_val:.4f} out of range; waiting")
                if pending_hedge_qty > 0 and active_order_id is None:
                    await _hedge_pending("spread_wait")
                    if hedge_failed:
                        break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            await _sync_primary_fills("loop")

            if active_order_id is None and pending_hedge_qty > 0:
                await _hedge_pending("post_primary")
                if hedge_failed:
                    break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            if pending_order_ids:
                await _sync_primary_fills("pre_chunk", include_active=False)
                if pending_order_ids:
                    _vlog(
                        "wait",
                        "primary cancel pending; waiting",
                        {"exchange": primary_leg["exchange"], "pending": list(pending_order_ids)},
                    )
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                if remaining <= 0:
                    if active_order_id:
                        await self._cancel_order(primary_leg, symbol, active_order_id)
                        pending_order_ids.add(active_order_id)
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
                        await _sync_primary_fills("post_target_cancel", delay=0.2, include_active=False)
                    break

            max_qty_by_exchange = snapshot.get("max_qty_by_exchange") or {}
            max_chunk = None
            max_candidates = [val for val in max_qty_by_exchange.values() if val]
            if max_candidates:
                max_chunk = min(max_candidates)
            if not force_chunk and max_slippage_bps > 0 and max_chunk is not None:
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
                max_chunk=None if force_chunk else max_chunk,
                amount_step=amount_step,
            )
            warnings.extend(chunk_warnings)
            if not chunk:
                if chunk_warnings:
                    break
                errors.append("Unable to determine chunk size for smart enter.")
                break
            _vlog(
                "decision",
                "smart-enter chunk",
                {
                    "remaining": remaining,
                    "chunk": chunk,
                    "min_chunk_qty": min_chunk_qty,
                    "max_chunk": max_chunk,
                    "amount_step": amount_step,
                },
            )

            price_step = (constraints.get(primary_leg["exchange"]) or {}).get("price_step")
            primary_stats = (snapshot.get("stats") or {}).get(primary_leg["exchange"])
            orderbook = (snapshot.get("orderbooks") or {}).get(primary_leg["exchange"])
            improve_ticks = int(_safe_float(payload.get("limit_improve_ticks")) or DEFAULT_LIMIT_IMPROVE_TICKS)
            exclude_qty = None
            if active_price and active_qty is not None:
                open_qty = max(0.0, (active_qty or 0.0) - (active_filled or 0.0))
                if open_qty > 0:
                    exclude_qty = open_qty
            limit_price = _resolve_smart_limit_price(
                orderbook=orderbook,
                side=primary_leg["side"],
                book_side=None,
                qty=chunk,
                payload=payload,
                price_step=price_step,
                best_bid=primary_stats.best_bid if primary_stats else None,
                best_ask=primary_stats.best_ask if primary_stats else None,
                mid_price=primary_stats.mid if primary_stats else None,
                improve_ticks=improve_ticks,
                offset_bps=limit_offset_bps,
                offset_ticks=limit_offset_ticks,
                round_mode="passive",
                exclude_price=active_price,
                exclude_qty=exclude_qty,
            )
            if limit_price is None:
                errors.append("Unable to resolve limit price for smart enter.")
                break
            _vlog(
                "decision",
                "smart-enter limit price",
                {
                    "limit_price": limit_price,
                    "price_step": price_step,
                    "offset_bps": limit_offset_bps,
                    "offset_ticks": limit_offset_ticks,
                    "improve_ticks": improve_ticks,
                },
            )

            if active_order_id:
                if active_price != limit_price or (active_qty is not None and active_qty != chunk):
                    ws_delta, used_ws = await _sync_primary_from_orders("reprice_cancel")
                    if not used_ws:
                        if self._ws_orders_live(primary_leg["exchange"]):
                            self._emit_log(
                                log_cb,
                                "wait",
                                "primary ws order update missing; skipping rest sync",
                                {"exchange": primary_leg["exchange"], "order_id": active_order_id},
                            )
                            status = {"status": "open", "filled_qty": active_filled}
                        else:
                            status = await self._fetch_order_status(
                                primary_leg,
                                symbol,
                                active_order_id,
                                expected_qty=active_qty or order_qty_map.get(active_order_id),
                                allow_trades_fallback=False,
                            )
                            if status.get("status") == "error":
                                self._emit_log(
                                    log_cb,
                                    "warn",
                                    "primary rest order status failed",
                                    {"exchange": primary_leg["exchange"], "order_id": active_order_id, "error": status.get("error")},
                                )
                                status = {"status": "open", "filled_qty": active_filled}
                        await _apply_primary_fill(
                            active_order_id,
                            status.get("filled_qty"),
                            status=status,
                            reason="reprice_cancel",
                        )
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final status before cancel (reprice)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "status": status,
                            },
                        )
                    else:
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final order ws sync before cancel (reprice)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "ws_delta": ws_delta,
                                "filled_total": primary_filled_total,
                            },
                        )
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_reprice_cancel", delay=0.2, include_active=False)
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue

            if remaining <= 0:
                if active_order_id:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_target_cancel", delay=0.2, include_active=False)
                break

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
                    log_cb=log_cb,
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
                _track_primary_order(order_id, chunk)
                initial_filled = _safe_float(submit.get("filled_qty")) or 0.0
                if initial_filled > 0:
                    await _apply_primary_fill(
                        order_id,
                        initial_filled,
                        status=submit,
                        reason="submit",
                    )
                    active_filled = primary_fill_map.get(order_id, 0.0)

            if active_order_id:
                if timeout > 0 and active_since and (time.time() - active_since) > timeout:
                    ws_delta, used_ws = await _sync_primary_from_orders("timeout_cancel")
                    if not used_ws:
                        if self._ws_orders_live(primary_leg["exchange"]):
                            self._emit_log(
                                log_cb,
                                "wait",
                                "primary ws order update missing; skipping rest sync",
                                {"exchange": primary_leg["exchange"], "order_id": active_order_id},
                            )
                            status = {"status": "open", "filled_qty": active_filled}
                        else:
                            status = await self._fetch_order_status(
                                primary_leg,
                                symbol,
                                active_order_id,
                                expected_qty=active_qty or order_qty_map.get(active_order_id),
                                allow_trades_fallback=False,
                            )
                            if status.get("status") == "error":
                                self._emit_log(
                                    log_cb,
                                    "warn",
                                    "primary rest order status failed",
                                    {"exchange": primary_leg["exchange"], "order_id": active_order_id, "error": status.get("error")},
                                )
                                status = {"status": "open", "filled_qty": active_filled}
                        await _apply_primary_fill(
                            active_order_id,
                            status.get("filled_qty"),
                            status=status,
                            reason="timeout_cancel",
                        )
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final status before cancel (timeout)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "status": status,
                            },
                        )
                    else:
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final order ws sync before cancel (timeout)",
                            {
                                "exchange": primary_leg["exchange"],
                                "order_id": active_order_id,
                                "ws_delta": ws_delta,
                                "filled_total": primary_filled_total,
                            },
                        )
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_timeout_cancel", delay=0.2, include_active=False)
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue
                if self._ws_orders_live(primary_leg["exchange"]):
                    await _sync_primary_from_orders("limit_wait")
                    await asyncio.sleep(max(0.2, reprice_sec))
                else:
                    limit_wait_sec = max(2, int(reprice_sec or 1) + 1)
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
                        log_cb=log_cb,
                    )
                    self._emit_log(log_cb, "result", "limit result", limit_result)
                    filled_qty = _safe_float(limit_result.get("filled_qty")) or 0.0
                    await _apply_primary_fill(
                        active_order_id,
                        filled_qty,
                        status=limit_result,
                        reason="limit_result",
                    )
                    if active_order_id:
                        active_filled = primary_fill_map.get(active_order_id, 0.0)
                    if limit_result.get("status") in ("filled", "closed"):
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None
                    if limit_result.get("cancel_reason"):
                        if active_order_id:
                            pending_order_ids.add(active_order_id)
                        active_order_id = None
                        active_price = None
                        active_qty = None
                        active_filled = 0.0
                        active_since = None

            if remaining <= 0:
                if active_order_id:
                    await self._cancel_order(primary_leg, symbol, active_order_id)
                    pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_target_cancel", delay=0.2, include_active=False)
                break

            if remaining <= 0:
                break
            await asyncio.sleep(max(0.2, reprice_sec))

        if active_order_id:
            await self._cancel_order(primary_leg, symbol, active_order_id)
            pending_order_ids.add(active_order_id)
            active_order_id = None
        if pending_order_ids:
            await _sync_primary_fills("final_sync", delay=0.2, include_active=False)

        if pending_hedge_qty > 0 and not hedge_failed and not stopped_by_user:
            await _hedge_pending("final_pending")

        if remaining > 0 and max_runtime_sec is not None:
            warnings.append(f"Remaining qty {remaining:g} not entered (smart-enter runtime ended).")

        if not stopped_by_user:
            await _final_reconcile_positions("final")
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
        market_refill_bps = _safe_float(payload.get("market_refill_bps"))
        if market_refill_bps is None:
            market_refill_bps = 10.0
        market_refill_buffer = _safe_float(payload.get("market_refill_buffer"))
        if market_refill_buffer is None:
            market_refill_buffer = 0.15
        market_refill_max_wait_sec = _safe_float(payload.get("market_refill_max_wait_sec")) or 5.0
        market_fill_timeout_sec = _safe_float(payload.get("market_fill_timeout_sec")) or 3.0
        requested_chunk = _safe_float(payload.get("chunk_qty"))
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        force_chunk = bool(payload.get("force_chunk_qty")) and requested_chunk is not None
        constraints = plan.get("market_constraints") or {}
        contract_sizes = self._contract_sizes_from_constraints(constraints)
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
        per_leg_amount_step = {
            leg["exchange"]: (constraints.get(leg["exchange"]) or {}).get("amount_step")
            for leg in legs
        }
        per_leg_min_qty = {
            leg["exchange"]: (constraints.get(leg["exchange"]) or {}).get("min_qty_required")
            for leg in legs
        }
        per_leg_min_buffer = {
            exchange: _min_qty_with_buffer(min_qty, per_leg_amount_step.get(exchange))
            for exchange, min_qty in per_leg_min_qty.items()
        }

        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])
        remaining = qty
        started_at = time.time()
        last_book_ts: dict[str, float | None] = {leg["exchange"]: None for leg in legs}
        exchange_list = [leg["exchange"] for leg in legs]

        def emit_positions_snapshot(stage: str, positions: list[dict[str, Any]], pos_errors: list[str]) -> None:
            counts: dict[str, int] = {}
            for entry in positions:
                exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
                counts[exchange] = counts.get(exchange, 0) + 1
            self._emit_log(
                log_cb,
                "positions",
                f"positions snapshot ({stage})",
                {
                    "stage": stage,
                    "symbol": symbol,
                    "exchanges": exchange_list,
                    "positions": positions,
                    "counts": counts,
                    "errors": pos_errors,
                },
            )

        self._emit_log(
            log_cb,
            "start",
            "manual fast-enter",
            {
                "action": plan.get("action"),
                "mode": "fast-enter",
                "symbol": symbol,
                "qty": qty,
                "legs": legs,
                "spread_pct": plan.get("spread_pct"),
            },
        )
        primary_name = primary_leg.get("exchange") if primary_leg else "-"
        hedge_name = hedge_leg.get("exchange") if hedge_leg else "-"
        primary_side = primary_leg.get("side") if primary_leg else "-"
        hedge_side = hedge_leg.get("side") if hedge_leg else "-"
        self._emit_story(
            log_cb,
            f"Start smart-enter: symbol={symbol} qty={qty:g} primary={primary_name}({primary_side}) hedge={hedge_name}({hedge_side})",
            {
                "action": plan.get("action"),
                "mode": "smart-enter",
                "symbol": symbol,
                "qty": qty,
                "primary_exchange": primary_name,
                "hedge_exchange": hedge_name,
            },
        )

        await self._ensure_ws_orders(
            exchange_list,
            contract_sizes=contract_sizes,
            symbol=symbol,
            log_cb=log_cb,
        )
        start_positions, start_errors = await self._fetch_positions_for_symbol(
            exchanges=exchange_list,
            symbol=symbol,
            allow_ws=False,
            contract_sizes=contract_sizes,
        )
        if start_errors:
            warnings.extend(start_errors)
        emit_positions_snapshot("start", start_positions, start_errors)
        start_qty_by_exchange = {
            leg["exchange"]: self._sum_position_qty(
                start_positions,
                exchange=leg["exchange"],
                side=_entry_position_side(leg),
                symbol=symbol,
            )
            for leg in legs
        }

        try:
            while remaining > 0 and (time.time() - started_at) < max_runtime_sec:
                if self._stop_requested():
                    warnings.append("stopped_by_user")
                    stopped_by_user = True
                    self._emit_log(
                        log_cb,
                        "warn",
                        "manual stop requested; halting",
                        {"remaining": remaining},
                    )
                    break
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
                if not force_chunk and max_slippage_bps > 0 and max_chunk is not None:
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
                    max_chunk=None if force_chunk else max_chunk,
                    amount_step=amount_step,
                )
                warnings.extend(chunk_warnings)
                if not chunk:
                    if chunk_warnings:
                        break
                    errors.append("Unable to determine chunk size for fast enter.")
                    break

                if any(last_book_ts.values()):
                    refill_deadline = time.time() + max(0.0, market_refill_max_wait_sec)
                    while True:
                        orderbooks = snapshot.get("orderbooks") or {}
                        refill_ready = True
                        refill_liqs = []
                        for leg in legs:
                            exchange = leg["exchange"]
                            orderbook = orderbooks.get(exchange)
                            orderbook_ts = self._orderbook_timestamp(orderbook)
                            last_ts = last_book_ts.get(exchange)
                            if last_ts and orderbook_ts and orderbook_ts <= last_ts:
                                refill_ready = False
                            liq = self._orderbook_refill_qty(
                                orderbook,
                                side=leg["side"],
                                max_bps=market_refill_bps,
                            )
                            refill_liqs.append(liq)
                        available = min(refill_liqs) if refill_liqs else 0.0
                        target_needed = chunk * (1.0 + market_refill_buffer)
                        if available < target_needed:
                            refill_ready = False
                        if refill_ready:
                            break
                        if time.time() >= refill_deadline:
                            adjusted = available / (1.0 + market_refill_buffer) if available > 0 else 0.0
                            adjusted = _round_to_step(adjusted, amount_step, mode="down") if amount_step else adjusted
                            if min_chunk_qty and adjusted < min_chunk_qty:
                                warnings.append("remaining qty below exchange minimum; unable to execute final chunk")
                                chunk = 0.0
                                break
                            if adjusted > 0:
                                if adjusted < chunk:
                                    self._emit_log(
                                        log_cb,
                                        "wait",
                                        "market refill timeout; reducing chunk",
                                        {"chunk": chunk, "adjusted": adjusted, "available": available},
                                    )
                                chunk = min(chunk, adjusted)
                            break
                        self._emit_log(
                            log_cb,
                            "wait",
                            "market refill waiting",
                            {"required": chunk, "available": available, "buffer": market_refill_buffer},
                        )
                        await asyncio.sleep(max(0.2, reprice_sec))
                        snapshot = await self._snapshot_legs(symbol, legs, max_slippage_bps=max_slippage_bps)
                        if snapshot.get("errors"):
                            continue
                    if not chunk:
                        break

                orderbooks = snapshot.get("orderbooks") or {}
                for leg in legs:
                    last_book_ts[leg["exchange"]] = self._orderbook_timestamp(orderbooks.get(leg["exchange"])) or time.time()

                submit_tasks = []
                for leg in legs:
                    self._emit_log(log_cb, "submit", f"market {leg['exchange']} qty={chunk:g}")
                    submit_tasks.append(
                        self._place_market(leg, symbol, chunk, payload, reason="fast_market", log_cb=log_cb)
                    )
                submit_results = await asyncio.gather(*submit_tasks)
                actions.extend(submit_results)

                fill_tasks = []
                for leg, submit in zip(legs, submit_results):
                    fill_tasks.append(
                        self._await_order_fill(
                            leg,
                            symbol,
                            submit.get("order_id"),
                            chunk,
                            market_fill_timeout_sec,
                            log_cb=log_cb,
                        )
                    )
                fill_results = await asyncio.gather(*fill_tasks)

                filled_by_exchange: dict[str, float] = {}
                blocked = False
                for leg, fill in zip(legs, fill_results):
                    exchange = leg["exchange"]
                    filled_qty = _safe_float(fill.get("filled_qty")) or 0.0
                    filled_by_exchange[exchange] = filled_qty
                    if fill.get("status") == "error":
                        errors.append(f"{exchange}: market fill error ({fill.get('error')})")
                        blocked = True
                if blocked:
                    break

                for leg, fill in zip(legs, fill_results):
                    exchange = leg["exchange"]
                    filled_qty = filled_by_exchange.get(exchange, 0.0)
                    remaining_leg = max(0.0, chunk - filled_qty)
                    min_needed = per_leg_min_buffer.get(exchange)
                    if remaining_leg <= 0:
                        continue
                    if min_needed and remaining_leg < min_needed:
                        warnings.append("remaining qty below exchange minimum; unable to execute final chunk")
                        blocked = True
                        continue
                    self._emit_log(log_cb, "submit", f"market top-up {exchange} qty={remaining_leg:g}")
                    topup = await self._place_market(
                        leg, symbol, remaining_leg, payload, reason="fast_market_topup", log_cb=log_cb
                    )
                    actions.append(topup)
                    topup_fill = await self._await_order_fill(
                        leg,
                        symbol,
                        topup.get("order_id"),
                        remaining_leg,
                        market_fill_timeout_sec,
                        log_cb=log_cb,
                    )
                    filled_qty += _safe_float(topup_fill.get("filled_qty")) or 0.0
                    filled_by_exchange[exchange] = filled_qty
                if blocked:
                    break

                filled_values = list(filled_by_exchange.values())
                matched = min(filled_values) if filled_values else 0.0
                imbalance = max(filled_values) - matched if filled_values else 0.0
                tolerance = amount_step or 0.0
                if matched <= 0:
                    warnings.append("market chunk produced no fills; stopping")
                    break
                if imbalance > tolerance:
                    warnings.append("market legs mismatch; stopping for final reconcile")
                    remaining = max(0.0, remaining - matched)
                    break
                remaining = max(0.0, remaining - matched)
                await asyncio.sleep(0.1)
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"fast-enter runtime error: {exc}")
        finally:
            end_positions, end_errors = await self._fetch_positions_for_symbol(
                exchanges=exchange_list,
                symbol=symbol,
                allow_ws=False,
                contract_sizes=contract_sizes,
            )
            if end_errors:
                warnings.extend(end_errors)
            emit_positions_snapshot("end", end_positions, end_errors)
            if stopped_by_user:
                self._emit_log(log_cb, "warn", "stop requested; skipping final reconcile", {"remaining": remaining})
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
            if len(legs) >= 2:
                deltas: dict[str, float] = {}
                for leg in legs:
                    exchange = leg["exchange"]
                    end_qty = self._sum_position_qty(
                        end_positions,
                        exchange=exchange,
                        side=_entry_position_side(leg),
                        symbol=symbol,
                    )
                    start_qty = start_qty_by_exchange.get(exchange, 0.0)
                    delta = max(0.0, end_qty - start_qty)
                    deltas[exchange] = delta
                primary = legs[0]
                secondary = legs[1]
                delta_primary = deltas.get(primary["exchange"], 0.0)
                delta_secondary = deltas.get(secondary["exchange"], 0.0)
                imbalance = delta_primary - delta_secondary
                if abs(imbalance) > (amount_step or 0.0):
                    if imbalance > 0:
                        lag_leg = secondary
                        qty_needed = imbalance
                    else:
                        lag_leg = primary
                        qty_needed = abs(imbalance)
                    qty_needed = _round_to_step(
                        qty_needed,
                        per_leg_amount_step.get(lag_leg["exchange"]),
                        mode="down",
                    )
                    min_needed = per_leg_min_buffer.get(lag_leg["exchange"])
                    if qty_needed > 0 and (not min_needed or qty_needed >= min_needed):
                        self._emit_log(
                            log_cb,
                            "submit",
                            f"final reconcile market {lag_leg['exchange']} qty={qty_needed:g}",
                        )
                        result = await self._place_market(
                            lag_leg,
                            symbol,
                            qty_needed,
                            payload,
                            reason="final_reconcile",
                            log_cb=log_cb,
                        )
                        actions.append(result)
                        self._emit_log(log_cb, "result", "final reconcile result", result)

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
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        limit_price = self._resolve_limit_price(leg, payload)
        if limit_price is None and bool(payload.get("auto_limit_price", True)):
            limit_price = await self._resolve_dynamic_limit_price(leg, symbol, qty, payload)
        if limit_price is None:
            return {
                "exchange": leg["exchange"],
                "status": "error",
                "error": "missing_limit_price",
                "ts": _now_iso(),
            }
        submit = await self._submit_order(
            leg,
            symbol,
            qty,
            "limit",
            price=limit_price,
            reduce_only=bool(leg.get("reduce_only")),
            log_cb=log_cb,
        )
        if submit.get("status") == "error":
            return submit
        self._emit_order_submit(
            log_cb,
            exchange=leg["exchange"],
            label=leg.get("label"),
            side=leg.get("side"),
            order_type="limit",
            qty=qty,
            price=limit_price,
            order_id=submit.get("order_id"),
            reduce_only=bool(leg.get("reduce_only")),
        )
        order_id = submit.get("order_id")
        if not order_id:
            return {
                "exchange": leg["exchange"],
                "status": "error",
                "error": "missing_order_id",
                "ts": _now_iso(),
            }
        status = await self._wait_for_order(leg, symbol, order_id, timeout)
        filled_qty = _safe_float(status.get("filled_qty")) or 0.0
        self._emit_order_status(
            log_cb,
            exchange=leg["exchange"],
            label=leg.get("label"),
            order_id=order_id,
            status=status.get("status"),
            filled_qty=filled_qty,
            avg_price=status.get("avg_price"),
            source=status.get("source") or "rest",
        )
        return {
            "exchange": leg["exchange"],
            "status": status.get("status"),
            "order_id": order_id,
            "filled_qty": filled_qty,
            "avg_price": status.get("avg_price"),
            "ts": _now_iso(),
        }

    async def _place_limit_at(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        price: float,
        timeout: int,
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        submit = await self._submit_order(
            leg,
            symbol,
            qty,
            "limit",
            price=price,
            reduce_only=bool(leg.get("reduce_only")),
            log_cb=log_cb,
        )
        if submit.get("status") == "error":
            return submit
        self._emit_order_submit(
            log_cb,
            exchange=leg["exchange"],
            label=leg.get("label"),
            side=leg.get("side"),
            order_type="limit",
            qty=qty,
            price=price,
            order_id=submit.get("order_id"),
            reduce_only=bool(leg.get("reduce_only")),
        )
        order_id = submit.get("order_id")
        if not order_id:
            return {
                "exchange": leg["exchange"],
                "status": "error",
                "error": "missing_order_id",
                "ts": _now_iso(),
            }
        status = await self._wait_for_order(leg, symbol, order_id, timeout)
        filled_qty = _safe_float(status.get("filled_qty")) or 0.0
        self._emit_order_status(
            log_cb,
            exchange=leg["exchange"],
            label=leg.get("label"),
            order_id=order_id,
            status=status.get("status"),
            filled_qty=filled_qty,
            avg_price=status.get("avg_price"),
            source=status.get("source") or "rest",
        )
        return {
            "exchange": leg["exchange"],
            "status": status.get("status"),
            "order_id": order_id,
            "filled_qty": filled_qty,
            "avg_price": status.get("avg_price"),
            "ts": _now_iso(),
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
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        submit = await self._submit_order(
            leg,
            symbol,
            qty,
            "limit",
            price=price,
            reduce_only=bool(leg.get("reduce_only")),
            log_cb=log_cb,
        )
        if submit.get("status") == "error":
            return submit
        self._emit_order_submit(
            log_cb,
            exchange=leg["exchange"],
            label=leg.get("label"),
            side=leg.get("side"),
            order_type="limit",
            qty=qty,
            price=price,
            order_id=submit.get("order_id"),
            reduce_only=bool(leg.get("reduce_only")),
        )
        order_id = submit.get("order_id")
        if not order_id:
            return {
                "exchange": leg["exchange"],
                "status": "error",
                "error": "missing_order_id",
                "ts": _now_iso(),
            }
        status = await self._wait_for_order_with_spread(
            leg,
            symbol,
            order_id,
            timeout,
            spread_min_pct,
            spread_max_pct,
            spread_legs,
            reprice_sec,
            log_cb=log_cb,
        )
        filled_qty = _safe_float(status.get("filled_qty")) or 0.0
        self._emit_order_status(
            log_cb,
            exchange=leg["exchange"],
            label=leg.get("label"),
            order_id=order_id,
            status=status.get("status"),
            filled_qty=filled_qty,
            avg_price=status.get("avg_price"),
            source=status.get("source") or "rest",
        )
        result = {
            "exchange": leg["exchange"],
            "status": status.get("status"),
            "order_id": order_id,
            "filled_qty": filled_qty,
            "avg_price": status.get("avg_price"),
            "ts": _now_iso(),
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
        *,
        reason: str | None = None,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        submit = await self._submit_order(
            leg,
            symbol,
            qty,
            "market",
            price=None,
            reduce_only=bool(leg.get("reduce_only")),
            log_cb=log_cb,
        )
        if reason:
            submit["market_reason"] = reason
        if submit.get("status") != "error":
            self._emit_order_submit(
                log_cb,
                exchange=leg["exchange"],
                label=leg.get("label"),
                side=leg.get("side"),
                order_type="market",
                qty=qty,
                price=None,
                order_id=submit.get("order_id"),
                reduce_only=bool(leg.get("reduce_only")),
                reason=reason,
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
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        if order_type == "limit" and (price is None or price <= 0):
            self._emit_log(
                log_cb,
                "error",
                "missing limit price",
                {
                    "exchange": leg.get("exchange"),
                    "symbol": symbol,
                    "order_type": order_type,
                    "side": leg.get("side"),
                    "qty_base": float(qty),
                },
            )
            return {
                "exchange": leg.get("exchange"),
                "status": "error",
                "error": "missing_limit_price",
                "ts": _now_iso(),
            }
        exchange = leg["exchange"]
        client = await self._ensure_client(exchange, [])
        if not client:
            self._emit_log(
                log_cb,
                "error",
                "client unavailable",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "order_type": order_type,
                    "side": leg.get("side"),
                },
            )
            return {
                "exchange": exchange,
                "status": "error",
                "error": "client_unavailable",
                "ts": _now_iso(),
            }
        ccxt_symbol = await self._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            self._emit_log(
                log_cb,
                "error",
                "symbol unavailable",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "order_type": order_type,
                    "side": leg.get("side"),
                },
            )
            return {
                "exchange": exchange,
                "status": "error",
                "error": "symbol_unavailable",
                "ts": _now_iso(),
            }
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
        kucoin_margin_mode = None
        leg_margin_mode = str(leg.get("margin_mode") or "").strip().lower()
        if leg_margin_mode in ("isolated", "cross") and hasattr(client, "set_margin_mode"):
            margin_params: dict[str, object] | None = None
            if exchange == "okx":
                margin_params = {"lever": int(DEFAULT_MANUAL_LEVERAGE)}
            try:
                if margin_params:
                    await client.set_margin_mode(leg_margin_mode, ccxt_symbol, margin_params)
                else:
                    await client.set_margin_mode(leg_margin_mode, ccxt_symbol)
            except Exception as exc:  # pylint: disable=broad-except
                self._emit_log(
                    log_cb,
                    "warn",
                    "set_margin_mode failed",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "margin_mode": leg_margin_mode,
                        "error": str(exc),
                    },
                )
        if hasattr(client, "set_leverage"):
            leverage_params: dict[str, object] = {}
            if leg_margin_mode in ("isolated", "cross"):
                if exchange == "okx":
                    leverage_params["tdMode"] = leg_margin_mode
                elif exchange == "bitget":
                    leverage_params["marginMode"] = leg_margin_mode
                elif exchange == "bingx":
                    leverage_params["marginMode"] = leg_margin_mode
                else:
                    leverage_params["marginMode"] = leg_margin_mode
            if exchange == "bingx":
                leverage_params["side"] = "LONG" if leg.get("side") == "buy" else "SHORT"
            try:
                await client.set_leverage(DEFAULT_MANUAL_LEVERAGE, ccxt_symbol, leverage_params or None)
            except Exception as exc:  # pylint: disable=broad-except
                self._emit_log(
                    log_cb,
                    "warn",
                    "set_leverage failed",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "leverage": DEFAULT_MANUAL_LEVERAGE,
                        "params": leverage_params or None,
                        "error": str(exc),
                    },
                )
        if exchange == "bitget":
            params["posSide"] = "net"
            params["positionSide"] = "net"
            if leg_margin_mode in ("isolated", "cross"):
                params["marginMode"] = leg_margin_mode
        if exchange == "kucoin":
            kucoin_margin_mode = str(leg_margin_mode or "isolated").strip().upper()
            if kucoin_margin_mode:
                params["marginMode"] = kucoin_margin_mode
                params["marginType"] = kucoin_margin_mode
        if exchange == "okx":
            hedged = await self._resolve_okx_hedged(client)
            if hedged is True:
                params["hedged"] = hedged
                params["posSide"] = "long" if leg["side"] == "buy" else "short"
            if leg_margin_mode in ("isolated", "cross"):
                params["tdMode"] = leg_margin_mode
        self._emit_log(
            log_cb,
            "order",
            "prepare order",
            {
                "exchange": exchange,
                "symbol": symbol,
                "ccxt_symbol": ccxt_symbol,
                "side": leg.get("side"),
                "order_type": order_type,
                "qty_base": float(qty),
                "qty_contracts": order_qty,
                "price": price if order_type == "limit" else None,
                "reduce_only": reduce_only,
                "params": params,
                "leverage": DEFAULT_MANUAL_LEVERAGE,
                "contract_size": contract_size,
            },
        )
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
            self._emit_log(
                log_cb,
                "order",
                "order submitted",
                {
                    "exchange": exchange,
                    "order_id": order.get("id"),
                    "status": order.get("status"),
                    "filled_qty": filled,
                    "avg_price": order.get("average"),
                },
            )
            return {
                "exchange": exchange,
                "status": "submitted",
                "order_id": order.get("id"),
                "filled_qty": filled,
                "avg_price": order.get("average"),
                "qty_base": float(qty),
                "qty_contracts": order_qty if contract_size else None,
                "contract_size": contract_size,
                "ts": _now_iso(),
            }
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc)
            if exchange == "kucoin" and (
                "330005" in message or "margin mode" in message.lower()
            ):
                if kucoin_margin_mode and hasattr(client, "set_margin_mode"):
                    try:
                        await client.set_margin_mode(kucoin_margin_mode, ccxt_symbol)
                    except Exception as set_exc:  # pylint: disable=broad-except
                        return {
                            "exchange": exchange,
                            "status": "error",
                            "error": f"kucoin set_margin_mode failed ({kucoin_margin_mode}): {set_exc}",
                            "ts": _now_iso(),
                        }
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
                            "qty_base": float(qty),
                            "qty_contracts": order_qty if contract_size else None,
                            "contract_size": contract_size,
                            "ts": _now_iso(),
                        }
                    except Exception as retry_exc:  # pylint: disable=broad-except
                        message = str(retry_exc)
            if exchange == "bitget" and "40774" in message:
                retry_params = dict(params)
                if params.get("posSide") == "net":
                    retry_params.pop("posSide", None)
                    retry_params.pop("positionSide", None)
                    retry_params["hedged"] = True
                    pos_side = "long" if leg["side"] == "buy" else "short"
                    retry_params["posSide"] = pos_side
                    retry_params["positionSide"] = pos_side
                else:
                    retry_params.pop("hedged", None)
                    retry_params["posSide"] = "net"
                    retry_params["positionSide"] = "net"
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
                    self._emit_log(
                        log_cb,
                        "order",
                        "order submitted",
                        {
                            "exchange": exchange,
                            "order_id": order.get("id"),
                            "status": order.get("status"),
                            "filled_qty": filled,
                            "avg_price": order.get("average"),
                        },
                    )
                    return {
                        "exchange": exchange,
                        "status": "submitted",
                        "order_id": order.get("id"),
                        "filled_qty": filled,
                        "avg_price": order.get("average"),
                        "qty_base": float(qty),
                        "qty_contracts": order_qty if contract_size else None,
                        "contract_size": contract_size,
                        "ts": _now_iso(),
                    }
                except Exception as retry_exc:  # pylint: disable=broad-except
                    message = str(retry_exc)
            self._emit_log(
                log_cb,
                "error",
                "order submit failed",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "order_type": order_type,
                    "side": leg.get("side"),
                    "qty_base": float(qty),
                    "price": price if order_type == "limit" else None,
                    "params": params,
                    "error": message,
                },
            )
            return {
                "exchange": exchange,
                "status": "error",
                "error": message,
                "ts": _now_iso(),
            }

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

    async def _fetch_order_compat(
        self,
        client: Any,
        exchange: str,
        order_id: str,
        ccxt_symbol: str,
    ) -> Mapping[str, Any]:
        if exchange == "bybit":
            params = {"acknowledged": True}
            try:
                return await client.fetch_order(order_id, ccxt_symbol, params)
            except Exception:
                if hasattr(client, "fetch_open_order"):
                    try:
                        return await client.fetch_open_order(order_id, ccxt_symbol)
                    except Exception:
                        pass
                if hasattr(client, "fetch_closed_order"):
                    try:
                        return await client.fetch_closed_order(order_id, ccxt_symbol)
                    except Exception:
                        pass
                raise
        return await client.fetch_order(order_id, ccxt_symbol)

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
        use_deadline = timeout is not None and int(timeout) > 0
        deadline = time.time() + int(timeout) if use_deadline else None
        last_fill = 0.0
        last_avg = None
        while True:
            if deadline and time.time() >= deadline:
                break
            try:
                order = await self._fetch_order_compat(client, exchange, order_id, ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                if use_deadline:
                    break
                await asyncio.sleep(1)
                continue
            status = str(order.get("status") or "").lower()
            filled = _order_filled_qty(order, contract_size)
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
        if use_deadline:
            try:
                await client.cancel_order(order_id, ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                pass
        if last_fill > 0:
            return {"status": "partial", "filled_qty": last_fill, "avg_price": last_avg}
        return {"status": "open", "filled_qty": last_fill, "avg_price": last_avg}

    async def _fetch_order_status(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        order_id: str,
        *,
        expected_qty: float | None = None,
        allow_trades_fallback: bool = True,
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
        try:
            order = await self._fetch_order_compat(client, exchange, order_id, ccxt_symbol)
        except Exception as exc:  # pylint: disable=broad-except
            if allow_trades_fallback:
                fallback = await self._recover_filled_from_trades(
                    client,
                    ccxt_symbol,
                    order_id,
                    contract_size,
                )
                if fallback:
                    filled_qty = _safe_float(fallback.get("filled_qty")) or 0.0
                    if expected_qty and filled_qty > expected_qty * 1.02:
                        filled_qty = expected_qty
                        fallback["filled_qty"] = filled_qty
                        fallback["clamped"] = True
                    return {
                        "status": "partial",
                        "filled_qty": fallback.get("filled_qty"),
                        "avg_price": fallback.get("avg_price"),
                        "source": "trades_fallback",
                    }
            return {"status": "error", "error": str(exc)}
        status = str(order.get("status") or "").lower()
        filled = _order_filled_qty(order, contract_size)
        avg = _safe_float(order.get("average"))
        if filled <= 0:
            order_amount = _to_base_qty(_safe_float(order.get("amount")), contract_size)
            if allow_trades_fallback:
                fallback = await self._recover_filled_from_trades(
                    client,
                    ccxt_symbol,
                    order_id,
                    contract_size,
                )
                if fallback:
                    recovered = _safe_float(fallback.get("filled_qty")) or 0.0
                    max_expected = expected_qty or order_amount
                    if max_expected and recovered > max_expected * 1.02:
                        recovered = max_expected
                        fallback["filled_qty"] = recovered
                        fallback["clamped"] = True
                    recovered_avg = fallback.get("avg_price") or avg
                    recovered_status = "partial"
                    if order_amount and recovered + 1e-9 >= order_amount:
                        recovered_status = "filled"
                    return {
                        "status": recovered_status,
                        "filled_qty": recovered,
                        "avg_price": recovered_avg,
                        "source": "trades_fallback",
                    }
        return {"status": status, "filled_qty": filled, "avg_price": avg}

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

    async def _recover_filled_from_trades(
        self,
        client: Any,
        ccxt_symbol: str,
        order_id: str,
        contract_size: float | None,
    ) -> dict[str, float | None] | None:
        """Best-effort fill recovery via trades when fetch_order is unavailable."""
        if not hasattr(client, "fetch_my_trades"):
            return None
        try:
            trades = await client.fetch_my_trades(ccxt_symbol, None, None, {"order": order_id})
        except Exception:  # pylint: disable=broad-except
            return None
        if not trades:
            return None
        matched: list[Mapping[str, Any]] = []
        order_id_str = str(order_id)
        for trade in trades or []:
            info = trade.get("info") or {}
            candidates = [
                trade.get("order"),
                trade.get("order_id"),
                trade.get("orderId"),
                trade.get("orderID"),
                trade.get("ordId"),
                trade.get("ordID"),
                info.get("order"),
                info.get("order_id"),
                info.get("orderId"),
                info.get("orderID"),
                info.get("ordId"),
                info.get("ordID"),
                info.get("clOrdId"),
                trade.get("clientOrderId"),
            ]
            if any(str(val) == order_id_str for val in candidates if val is not None):
                matched.append(trade)
        if not matched:
            return None
        total_qty = 0.0
        total_cost = 0.0
        for trade in matched:
            qty = _safe_float(trade.get("amount"))
            price = _safe_float(trade.get("price"))
            if qty is None or price is None:
                continue
            total_qty += qty
            total_cost += qty * price
        if total_qty <= 0:
            return None
        filled = _to_base_qty(total_qty, contract_size)
        avg_price = (total_cost / total_qty) if total_qty > 0 else None
        return {"filled_qty": filled, "avg_price": avg_price}

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
        log_cb: Optional[callable] = None,
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
        use_deadline = timeout is not None and int(timeout) > 0
        deadline = time.time() + int(timeout) if use_deadline else None
        last_fill = 0.0
        last_avg = None
        check_interval = max(0.5, float(reprice_sec or 1.0) + 1.0)
        while True:
            if deadline and time.time() >= deadline:
                break
            try:
                order = await self._fetch_order_compat(client, exchange, order_id, ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                if use_deadline:
                    break
                await asyncio.sleep(check_interval)
                continue
            status = str(order.get("status") or "").lower()
            filled = _order_filled_qty(order, contract_size)
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
                        final_status = await self._fetch_order_status(leg, symbol, order_id)
                        self._emit_log(
                            log_cb,
                            "cancel",
                            "final status before cancel (spread)",
                            {
                                "exchange": exchange,
                                "order_id": order_id,
                                "status": final_status,
                            },
                        )
                        try:
                            await client.cancel_order(order_id, ccxt_symbol)
                        except Exception:  # pylint: disable=broad-except
                            pass
                        try:
                            order = await self._fetch_order_compat(client, exchange, order_id, ccxt_symbol)
                            last_fill = max(last_fill, _order_filled_qty(order, contract_size))
                            last_avg = _safe_float(order.get("average")) or last_avg
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
        if use_deadline and cancel_on_timeout:
            try:
                await client.cancel_order(order_id, ccxt_symbol)
            except Exception:  # pylint: disable=broad-except
                pass
            try:
                order = await self._fetch_order_compat(client, exchange, order_id, ccxt_symbol)
                last_fill = max(last_fill, _order_filled_qty(order, contract_size))
                last_avg = _safe_float(order.get("average")) or last_avg
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
        hedge_limit_mode: str,
        hedge_favorable_bps: float,
        hedge_adverse_bps: float,
        hedge_reprice_min_sec: float,
        payload: Mapping[str, Any] | None = None,
        min_qty_required: float | None = None,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        payload = payload or {}
        if min_qty_required and qty < min_qty_required:
            self._emit_log(
                log_cb,
                "wait",
                "hedge below minimum; pending",
                {"exchange": leg.get("exchange"), "qty": qty, "min_qty": min_qty_required},
            )
            return {
                "exchange": leg.get("exchange"),
                "status": "partial",
                "filled_qty": 0.0,
                "pending_qty": qty,
                "pending_reason": "below_min_qty",
            }
        if hedge_order_type != "limit":
            return await self._place_market(
                leg, symbol, qty, {}, reason="hedge_market_order_type", log_cb=log_cb
            )

        start_time = time.time()
        remaining = max(0.0, qty)
        filled_total = 0.0
        order_id: str | None = None
        order_price: float | None = None
        last_reprice = 0.0

        async def _await_cancel_terminal(order_id: str) -> dict[str, Any]:
            deadline = time.time() + max(1.0, hedge_reprice_min_sec)
            last_status: dict[str, Any] = {"status": "open", "filled_qty": filled_total}
            while True:
                status: dict[str, Any] | None = None
                ws_live = self._ws_orders_live(leg["exchange"])
                if ws_live:
                    ws_info = self._ws_order_info(leg["exchange"], order_id)
                    if ws_info:
                        status = {
                            "status": ws_info.get("status") or "open",
                            "filled_qty": _safe_float(ws_info.get("filled_qty")) or 0.0,
                            "avg_price": ws_info.get("avg_price"),
                            "source": "ws",
                        }
                if status is None:
                    status = await self._fetch_order_status(
                        leg,
                        symbol,
                        order_id,
                        allow_trades_fallback=not ws_live,
                    )
                last_status = status
                state = str(status.get("status") or "").lower()
                if state in ("canceled", "cancelled", "closed", "filled", "finished"):
                    return status
                if time.time() >= deadline:
                    return status
                await asyncio.sleep(0.2)

        while remaining > 0:
            snapshot = await self._snapshot_legs(symbol, [leg], max_slippage_bps=0.0)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "hedge orderbook failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, hedge_reprice_min_sec))
                continue
            stats = (snapshot.get("stats") or {}).get(leg["exchange"])
            if not stats:
                return {"exchange": leg["exchange"], "status": "error", "error": "hedge_stats_missing"}

            constraints = snapshot.get("constraints") or {}
            if min_qty_required is None:
                min_qty_required = (constraints.get(leg["exchange"]) or {}).get("min_qty_required")
            price_step = (constraints.get(leg["exchange"]) or {}).get("price_step")

            if order_id is None:
                if min_qty_required and remaining < min_qty_required:
                    self._emit_log(
                        log_cb,
                        "wait",
                        "hedge below minimum; pending",
                        {"exchange": leg.get("exchange"), "qty": remaining, "min_qty": min_qty_required},
                    )
                    return {
                        "exchange": leg.get("exchange"),
                        "status": "partial",
                        "filled_qty": filled_total,
                        "pending_qty": remaining,
                        "pending_reason": "below_min_qty",
                    }
                orderbook = (snapshot.get("orderbooks") or {}).get(leg["exchange"])
                round_mode = "aggressive" if hedge_limit_mode == "aggressive" else "passive"
                book_side = leg["side"]
                if hedge_limit_mode == "aggressive":
                    book_side = "sell" if leg["side"] == "buy" else "buy"
                improve_ticks = int(
                    _safe_float(payload.get("hedge_improve_ticks") or payload.get("limit_improve_ticks"))
                    or DEFAULT_LIMIT_IMPROVE_TICKS
                )
                limit_price = _resolve_smart_limit_price(
                    orderbook=orderbook,
                    side=leg["side"],
                    book_side=book_side,
                    qty=remaining,
                    payload=payload,
                    price_step=price_step,
                    best_bid=stats.best_bid,
                    best_ask=stats.best_ask,
                    mid_price=stats.mid,
                    improve_ticks=improve_ticks,
                    offset_bps=hedge_offset_bps,
                    offset_ticks=hedge_offset_ticks,
                    round_mode=round_mode,
                )
                if limit_price is None:
                    return {"exchange": leg["exchange"], "status": "error", "error": "hedge_price_missing"}
                self._emit_log(
                    log_cb,
                    "submit",
                    f"hedge limit {leg['exchange']} qty={remaining:g} price={limit_price:g}",
                )
                submit = await self._submit_order(
                    leg,
                    symbol,
                    remaining,
                    "limit",
                    price=limit_price,
                    reduce_only=bool(leg.get("reduce_only")),
                    log_cb=log_cb,
                )
                if submit.get("status") == "error":
                    return submit
                order_id = submit.get("order_id")
                if not order_id:
                    return {"exchange": leg["exchange"], "status": "error", "error": "missing_order_id"}
                self._emit_order_submit(
                    log_cb,
                    exchange=leg["exchange"],
                    label=leg.get("label"),
                    side=leg.get("side"),
                    order_type="limit",
                    qty=remaining,
                    price=limit_price,
                    order_id=order_id,
                    reduce_only=bool(leg.get("reduce_only")),
                )
                order_price = limit_price
                last_reprice = time.time()

            status = None
            if order_id:
                ws_live = self._ws_orders_live(leg["exchange"])
                ws_info = self._ws_order_info(leg["exchange"], order_id)
                if ws_info:
                    status = {
                        "status": ws_info.get("status") or "open",
                        "filled_qty": _safe_float(ws_info.get("filled_qty")) or 0.0,
                        "avg_price": ws_info.get("avg_price"),
                        "source": "ws",
                    }
                elif ws_live:
                    status = {
                        "status": "open",
                        "filled_qty": filled_total,
                        "avg_price": None,
                        "source": "ws",
                    }
            if status is None:
                status = await self._fetch_order_status(
                    leg,
                    symbol,
                    order_id,
                    allow_trades_fallback=not self._ws_orders_live(leg["exchange"]),
                )
            if status.get("status") == "error":
                return {
                    "exchange": leg["exchange"],
                    "status": "error",
                    "error": status.get("error") or "hedge_status_failed",
                }
            filled_total = _safe_float(status.get("filled_qty")) or filled_total
            remaining = max(0.0, qty - filled_total)
            state = str(status.get("status") or "").lower()
            if remaining <= 0:
                self._emit_order_status(
                    log_cb,
                    exchange=leg["exchange"],
                    label=leg.get("label"),
                    order_id=order_id,
                    status="filled",
                    filled_qty=filled_total,
                    avg_price=status.get("avg_price"),
                    source=status.get("source"),
                )
                return {
                    "exchange": leg["exchange"],
                    "status": "filled",
                    "order_id": order_id,
                    "filled_qty": filled_total,
                    "avg_price": status.get("avg_price"),
                }
            if state in ("canceled", "cancelled"):
                order_id = None
                order_price = None
                last_reprice = time.time()
                continue
            if min_qty_required and remaining < min_qty_required:
                await self._cancel_order(leg, symbol, order_id)
                cancel_status = await _await_cancel_terminal(order_id)
                filled_total = max(filled_total, _safe_float(cancel_status.get("filled_qty")) or filled_total)
                remaining = max(0.0, qty - filled_total)
                state = str(cancel_status.get("status") or "").lower()
                if remaining <= 0 or state in ("filled", "closed", "finished"):
                    self._emit_order_status(
                        log_cb,
                        exchange=leg["exchange"],
                        label=leg.get("label"),
                        order_id=order_id,
                        status="filled",
                        filled_qty=filled_total,
                        avg_price=cancel_status.get("avg_price"),
                        source=cancel_status.get("source"),
                    )
                    return {
                        "exchange": leg["exchange"],
                        "status": "filled",
                        "order_id": order_id,
                        "filled_qty": filled_total,
                        "avg_price": cancel_status.get("avg_price"),
                    }
                self._emit_log(
                    log_cb,
                    "wait",
                    "hedge remainder below minimum; pending",
                    {"exchange": leg.get("exchange"), "qty": remaining, "min_qty": min_qty_required},
                )
                self._emit_order_status(
                    log_cb,
                    exchange=leg["exchange"],
                    label=leg.get("label"),
                    order_id=order_id,
                    status="partial",
                    filled_qty=filled_total,
                    avg_price=status.get("avg_price"),
                    source=status.get("source"),
                )
                return {
                    "exchange": leg.get("exchange"),
                    "status": "partial",
                    "order_id": order_id,
                    "filled_qty": filled_total,
                    "avg_price": status.get("avg_price"),
                    "pending_qty": remaining,
                    "pending_reason": "below_min_qty",
                }

            if order_price:
                favorable_bps, adverse_bps = _hedge_price_move_bps(
                    leg["side"], order_price, stats.best_bid, stats.best_ask
                )
                now = time.time()
                if adverse_bps is not None and adverse_bps >= hedge_adverse_bps:
                    if min_qty_required and remaining < min_qty_required:
                        await self._cancel_order(leg, symbol, order_id)
                        cancel_status = await _await_cancel_terminal(order_id)
                        filled_total = max(filled_total, _safe_float(cancel_status.get("filled_qty")) or filled_total)
                        remaining = max(0.0, qty - filled_total)
                        state = str(cancel_status.get("status") or "").lower()
                        if remaining <= 0 or state in ("filled", "closed", "finished"):
                            self._emit_order_status(
                                log_cb,
                                exchange=leg["exchange"],
                                label=leg.get("label"),
                                order_id=order_id,
                                status="filled",
                                filled_qty=filled_total,
                                avg_price=cancel_status.get("avg_price"),
                                source=cancel_status.get("source"),
                            )
                            return {
                                "exchange": leg["exchange"],
                                "status": "filled",
                                "order_id": order_id,
                                "filled_qty": filled_total,
                                "avg_price": cancel_status.get("avg_price"),
                            }
                        self._emit_log(
                            log_cb,
                            "wait",
                            "hedge remainder below minimum; pending",
                            {"exchange": leg.get("exchange"), "qty": remaining, "min_qty": min_qty_required},
                        )
                        self._emit_order_status(
                            log_cb,
                            exchange=leg["exchange"],
                            label=leg.get("label"),
                            order_id=order_id,
                            status="partial",
                            filled_qty=filled_total,
                            avg_price=status.get("avg_price"),
                            source=status.get("source"),
                        )
                        return {
                            "exchange": leg.get("exchange"),
                            "status": "partial",
                            "order_id": order_id,
                            "filled_qty": filled_total,
                            "avg_price": status.get("avg_price"),
                            "pending_qty": remaining,
                            "pending_reason": "below_min_qty",
                        }
                    await self._cancel_order(leg, symbol, order_id)
                    cancel_status = await _await_cancel_terminal(order_id)
                    filled_total = max(filled_total, _safe_float(cancel_status.get("filled_qty")) or filled_total)
                    remaining = max(0.0, qty - filled_total)
                    state = str(cancel_status.get("status") or "").lower()
                    if remaining <= 0 or state in ("filled", "closed", "finished"):
                        self._emit_order_status(
                            log_cb,
                            exchange=leg["exchange"],
                            label=leg.get("label"),
                            order_id=order_id,
                            status="filled",
                            filled_qty=filled_total,
                            avg_price=cancel_status.get("avg_price"),
                            source=cancel_status.get("source"),
                        )
                        return {
                            "exchange": leg["exchange"],
                            "status": "filled",
                            "order_id": order_id,
                            "filled_qty": filled_total,
                            "avg_price": cancel_status.get("avg_price"),
                        }
                    self._emit_story(
                        log_cb,
                        f"Hedge adverse move; switching to market {leg['exchange']} qty={remaining:g}",
                        {"exchange": leg.get("exchange"), "adverse_bps": adverse_bps, "qty": remaining},
                    )
                    self._emit_log(
                        log_cb,
                        "submit",
                        f"hedge market {leg['exchange']} qty={remaining:g} adverse_bps={adverse_bps:.2f}",
                    )
                    market_result = await self._place_market(
                        leg, symbol, remaining, {}, reason="hedge_adverse_bps", log_cb=log_cb
                    )
                    self._emit_order_status(
                        log_cb,
                        exchange=leg["exchange"],
                        label=leg.get("label"),
                        order_id=order_id,
                        status="partial",
                        filled_qty=filled_total,
                        avg_price=status.get("avg_price"),
                        source=status.get("source"),
                    )
                    return {
                        "exchange": leg["exchange"],
                        "status": "partial",
                        "filled_qty": filled_total,
                        "avg_price": status.get("avg_price"),
                        "fallback": market_result,
                    }
                if (
                    favorable_bps is not None
                    and favorable_bps >= hedge_favorable_bps
                    and (now - last_reprice) >= hedge_reprice_min_sec
                ):
                    await self._cancel_order(leg, symbol, order_id)
                    cancel_status = await _await_cancel_terminal(order_id)
                    filled_total = max(filled_total, _safe_float(cancel_status.get("filled_qty")) or filled_total)
                    remaining = max(0.0, qty - filled_total)
                    state = str(cancel_status.get("status") or "").lower()
                    last_reprice = now
                    if remaining <= 0 or state in ("filled", "closed", "finished"):
                        self._emit_order_status(
                            log_cb,
                            exchange=leg["exchange"],
                            label=leg.get("label"),
                            order_id=order_id,
                            status="filled",
                            filled_qty=filled_total,
                            avg_price=cancel_status.get("avg_price"),
                            source=cancel_status.get("source"),
                        )
                        return {
                            "exchange": leg["exchange"],
                            "status": "filled",
                            "order_id": order_id,
                            "filled_qty": filled_total,
                            "avg_price": cancel_status.get("avg_price"),
                        }
                    if state in ("canceled", "cancelled", "closed"):
                        order_id = None
                        order_price = None
                        self._emit_log(
                            log_cb,
                            "reprice",
                            f"hedge reprice {leg['exchange']} favorable_bps={favorable_bps:.2f}",
                        )
                    else:
                        self._emit_log(
                            log_cb,
                            "wait",
                            "hedge cancel pending; keeping existing order",
                            {"exchange": leg.get("exchange"), "order_id": order_id, "status": cancel_status},
                        )

            await asyncio.sleep(max(0.5, hedge_reprice_min_sec))

        return {
            "exchange": leg["exchange"],
            "status": "open",
            "filled_qty": filled_total,
            "avg_price": status.get("avg_price") if order_id else None,
        }

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

    async def _fetch_orderbook(
        self,
        *,
        client: Any,
        exchange: str,
        symbol: str,
        ccxt_symbol: str,
        depth: int,
        errors: list[str] | None = None,
    ) -> dict[str, Any] | None:
        if self._orderbook_provider:
            try:
                orderbook = await self._orderbook_provider.get_orderbook(exchange, symbol, depth=depth)
            except Exception as exc:  # pylint: disable=broad-except
                if errors is not None:
                    errors.append(f"{exchange}: orderbook stream failed: {exc}")
            else:
                if orderbook:
                    result = dict(orderbook)
                    result["source"] = "ws"
                    return result
        try:
            orderbook = await client.fetch_order_book(ccxt_symbol, limit=depth)
        except Exception as exc:  # pylint: disable=broad-except
            if errors is not None:
                errors.append(f"{exchange}: orderbook fetch failed: {exc}")
            return None
        if not orderbook:
            return None
        result = dict(orderbook)
        result["source"] = "rest"
        return result

    def _orderbook_timestamp(self, orderbook: Mapping[str, Any] | None) -> float | None:
        if not orderbook:
            return None
        timestamp = _safe_float(orderbook.get("timestamp"))
        if timestamp is None:
            return None
        if timestamp > 1e12:
            timestamp = timestamp / 1000.0
        return float(timestamp)

    def _orderbook_refill_qty(
        self,
        orderbook: Mapping[str, Any] | None,
        *,
        side: str,
        max_bps: float,
    ) -> float:
        if not orderbook:
            return 0.0
        levels = orderbook.get("asks") if side == "buy" else orderbook.get("bids")
        if not levels:
            return 0.0
        best_price = _safe_float(levels[0][0]) or 0.0
        if best_price <= 0:
            return 0.0
        limit_price = best_price * (1.0 + max_bps / 10000.0) if side == "buy" else best_price * (1.0 - max_bps / 10000.0)
        total = 0.0
        for price, qty in levels:
            price_val = _safe_float(price)
            qty_val = _safe_float(qty)
            if price_val is None or qty_val is None:
                continue
            if side == "buy":
                if price_val > limit_price:
                    break
            else:
                if price_val < limit_price:
                    break
            total += float(qty_val)
        return total

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
            orderbook = await self._fetch_orderbook(
                client=client,
                exchange=exchange,
                symbol=symbol,
                ccxt_symbol=ccxt_symbol,
                depth=self._orderbook_depth,
                errors=errors,
            )
            if not orderbook:
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
        orderbook_sources = {
            exch: (orderbooks.get(exch) or {}).get("source") for exch in orderbooks
        }
        return {
            "errors": [],
            "orderbooks": orderbooks,
            "stats": stats_by_exchange,
            "spread_pct": spread_val,
            "mid_price": mid_price,
            "primary_best": primary_best,
            "max_qty_by_exchange": max_qty_by_exchange,
            "constraints": constraints,
            "orderbook_sources": orderbook_sources,
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
        markets = getattr(client, "markets", None)
        if not markets:
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
            raw_min_qty = _safe_float(
                info.get("minQty")
                or info.get("minQtySize")
                or info.get("minSize")
                or info.get("minSz")
                or info.get("minOrderQty")
                or info.get("minOrderSize")
                or info.get("minTradeSize")
                or info.get("minTradeQty")
            )
        if min_notional is None:
            min_notional = _safe_float(
                info.get("minNotional")
                or info.get("minOrderValue")
                or info.get("minOrderAmount")
                or info.get("minOrderAmt")
                or info.get("minValue")
                or info.get("minTradeAmount")
                or info.get("minTradeAmt")
            )
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

    def _emit_story(
        self,
        log_cb: Optional[callable],
        message: str,
        data: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self._emit_log(log_cb, "story", message, data)

    def _format_ws_health_entry(self, snapshot: Mapping[str, Any]) -> str:
        exchange = snapshot.get("exchange") or "unknown"
        healthy = bool(snapshot.get("healthy"))
        warming = bool(snapshot.get("warming"))
        status = "ok" if healthy else "warming" if warming else "stale"
        parts = [f"{exchange} {status}"]
        error = snapshot.get("error")
        if error:
            parts.append(f"error={error}")
        for key, label in (
            ("since_start_sec", "since_start"),
            ("last_rx_sec", "last_rx"),
            ("last_order_sec", "last_order"),
            ("last_ping_sec", "last_ping"),
            ("last_pong_sec", "last_pong"),
        ):
            value = _safe_float(snapshot.get(key))
            if value is None:
                continue
            parts.append(f"{label}={value:.1f}s")
        return " ".join(parts)

    def _format_ws_probe_message(self, exchange: str, action: str, data: Mapping[str, Any]) -> str:
        exchange = exchange or "-"
        silence = _safe_float(data.get("silence_sec"))
        rtt = _safe_float(data.get("rtt_sec"))
        attempt = data.get("attempt")
        attempts = data.get("attempts")
        grace_sec = _safe_float(data.get("grace_sec"))
        timeout_sec = _safe_float(data.get("timeout_sec"))
        if action == "connected":
            return f"WS[{exchange}] connected"
        if action == "server_ping":
            if silence is not None:
                return f"WS[{exchange}] server ping (silence={silence:.1f}s)"
            return f"WS[{exchange}] server ping"
        if action == "probe_ping_sent":
            if silence is not None:
                return f"WS[{exchange}] probe ping sent (silence={silence:.1f}s)"
            return f"WS[{exchange}] probe ping sent"
        if action == "probe_pong_received":
            if rtt is not None:
                return f"WS[{exchange}] probe pong received (rtt={rtt:.1f}s)"
            return f"WS[{exchange}] probe pong received"
        if action == "heartbeat_timeout":
            if silence is not None and timeout_sec is not None:
                return f"WS[{exchange}] heartbeat timeout (silence={silence:.1f}s > {timeout_sec:.1f}s)"
            return f"WS[{exchange}] heartbeat timeout"
        if action == "connect_start":
            return f"WS[{exchange}] connect start"
        if action == "connect_error":
            error = data.get("error")
            if error:
                return f"WS[{exchange}] connect error: {error}"
            return f"WS[{exchange}] connect error"
        if action == "auth_missing":
            return f"WS[{exchange}] auth missing"
        if action == "auth_failed":
            error = data.get("error")
            if error:
                return f"WS[{exchange}] auth failed: {error}"
            return f"WS[{exchange}] auth failed"
        if action == "listen_key_request":
            return f"WS[{exchange}] listenKey request"
        if action == "listen_key_ok":
            return f"WS[{exchange}] listenKey ok"
        if action == "listen_key_missing_api_key":
            return f"WS[{exchange}] listenKey missing api key"
        if action == "listen_key_failed":
            error = data.get("error")
            if error:
                return f"WS[{exchange}] listenKey failed: {error}"
            return f"WS[{exchange}] listenKey failed"
        if action == "reconnect_attempt":
            if attempt is not None and attempts is not None:
                if grace_sec is not None:
                    return f"WS[{exchange}] reconnect attempt {attempt}/{attempts} (grace={grace_sec:.1f}s)"
                return f"WS[{exchange}] reconnect attempt {attempt}/{attempts}"
            return f"WS[{exchange}] reconnect attempt"
        return f"WS[{exchange}] {action}"

    def _emit_order_submit(
        self,
        log_cb: Optional[callable],
        *,
        exchange: str,
        label: str | None,
        side: str | None,
        order_type: str,
        qty: float | None,
        price: float | None,
        order_id: str | None,
        reduce_only: bool,
        reason: str | None = None,
    ) -> None:
        if not log_cb:
            return
        label_text = f" ({label})" if label else ""
        parts = [f"Order submit{label_text}: {exchange} {order_type} {side or ''}".strip()]
        if qty is not None:
            parts.append(f"qty={qty:g}")
        if price:
            parts.append(f"price={price:g}")
        if reduce_only:
            parts.append("reduce_only")
        if reason:
            parts.append(f"reason={reason}")
        if order_id:
            parts.append(f"id={order_id}")
        self._emit_story(
            log_cb,
            " ".join(parts),
            {
                "exchange": exchange,
                "label": label,
                "side": side,
                "order_type": order_type,
                "qty_base": qty,
                "price": price,
                "order_id": order_id,
                "reduce_only": reduce_only,
                "reason": reason,
            },
        )

    def _emit_order_status(
        self,
        log_cb: Optional[callable],
        *,
        exchange: str,
        label: str | None,
        order_id: str | None,
        status: str | None,
        filled_qty: float | None,
        avg_price: float | None,
        source: str | None = None,
    ) -> None:
        if not log_cb:
            return
        label_text = f" ({label})" if label else ""
        parts = [f"Order status{label_text}: {exchange}"]
        if order_id:
            parts.append(f"id={order_id}")
        if status:
            parts.append(f"status={status}")
        if filled_qty is not None:
            parts.append(f"filled={filled_qty:g}")
        if avg_price:
            parts.append(f"avg={avg_price:g}")
        if source:
            parts.append(f"source={source}")
        self._emit_story(
            log_cb,
            " ".join(parts),
            {
                "exchange": exchange,
                "label": label,
                "order_id": order_id,
                "status": status,
                "filled_qty": filled_qty,
                "avg_price": avg_price,
                "source": source,
            },
        )

    async def _log_positions_snapshot(
        self,
        *,
        exchanges: Iterable[str],
        symbol: str,
        stage: str,
        log_cb: Optional[callable] = None,
    ) -> None:
        if not log_cb:
            return
        canonical = normalize_symbol(symbol)
        positions: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        errors: list[str] = []
        sources: dict[str, str] = {}
        for exchange in exchanges:
            exchange = normalize_exchange_name(str(exchange))
            if exchange:
                sources[exchange] = "rest"
            gateway = self._gateways.get(exchange)
            if gateway is None:
                errors.append(f"{exchange}: gateway unavailable")
                continue
            try:
                await gateway.refresh_credentials_async(force_env=True)
                await gateway.ensure_client()
                if gateway.client is None:
                    errors.append(f"{exchange}: client unavailable (missing credentials?)")
                    continue
                raw_positions = await gateway.fetch_positions()
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(f"{exchange}: positions fetch failed: {exc}")
                continue
            matched = []
            for pos in raw_positions or []:
                pos_symbol = normalize_symbol(
                    pos.get("symbol") or pos.get("symbol_normalized") or ""
                )
                if canonical and not _symbol_matches(canonical, pos_symbol):
                    continue
                entry = {
                    "exchange": exchange,
                    "symbol": pos.get("symbol"),
                    "side": pos.get("side"),
                    "coin_qty": pos.get("coin_qty"),
                    "contracts": pos.get("contracts"),
                    "notional": pos.get("notional"),
                    "margin_mode": pos.get("margin_mode"),
                    "leverage": pos.get("leverage"),
                    "entry_price": pos.get("entry_price"),
                    "mark_price": pos.get("mark_price"),
                }
                matched.append(entry)
                positions.append(entry)
            counts[exchange] = len(matched)
        self._emit_log(
            log_cb,
            "positions",
            f"positions snapshot ({stage})",
            {
                "stage": stage,
                "symbol": symbol,
                "exchanges": list(exchanges),
                "positions": positions,
                "counts": counts,
                "errors": errors,
                "sources": sources,
            },
        )

    async def _fetch_positions_for_symbol(
        self,
        *,
        exchanges: Iterable[str],
        symbol: str,
        allow_ws: bool = True,
        contract_sizes: Mapping[str, float | None] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        canonical = normalize_symbol(symbol)
        positions: list[dict[str, Any]] = []
        errors: list[str] = []
        exchange_list = [normalize_exchange_name(str(exchange)) for exchange in exchanges if exchange]
        if allow_ws:
            await self._ensure_ws_positions(exchange_list, contract_sizes=contract_sizes)
        for exchange in exchange_list:
            if allow_ws and self._ws_live(exchange):
                positions.extend(self._ws_positions.get_positions(exchange, symbol))
                continue
            gateway = self._gateways.get(exchange)
            if gateway is None:
                errors.append(f"{exchange}: gateway unavailable")
                continue
            try:
                await gateway.refresh_credentials_async(force_env=True)
                await gateway.ensure_client()
                if gateway.client is None:
                    errors.append(f"{exchange}: client unavailable (missing credentials?)")
                    continue
                raw_positions = await gateway.fetch_positions()
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(f"{exchange}: positions fetch failed: {exc}")
                continue
            for pos in raw_positions or []:
                pos_symbol = normalize_symbol(
                    pos.get("symbol") or pos.get("symbol_normalized") or ""
                )
                if canonical and not _symbol_matches(canonical, pos_symbol):
                    continue
                positions.append(
                    {
                        "exchange": normalize_exchange_name(str(exchange)),
                        "symbol": pos.get("symbol") or pos.get("symbol_normalized"),
                        "side": str(pos.get("side") or "").lower(),
                        "coin_qty": pos.get("coin_qty"),
                        "contracts": pos.get("contracts"),
                        "amount": pos.get("amount"),
                    }
                )
        return positions, errors

    def _sum_position_qty(
        self,
        positions: Iterable[Mapping[str, Any]],
        *,
        exchange: str,
        side: str,
        symbol: str,
    ) -> float:
        canonical = normalize_symbol(symbol)
        exchange = normalize_exchange_name(exchange)
        side = str(side or "").lower()
        total = 0.0
        for pos in positions:
            if normalize_exchange_name(str(pos.get("exchange") or "")) != exchange:
                continue
            pos_symbol = normalize_symbol(
                pos.get("symbol") or pos.get("symbol_normalized") or ""
            )
            if canonical and not _symbol_matches(canonical, pos_symbol):
                continue
            pos_side = str(pos.get("side") or "").lower()
            if side and pos_side != side:
                continue
            qty = _safe_float(pos.get("coin_qty"))
            if qty is None:
                qty = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
            total += abs(qty or 0.0)
        return total

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
                if canonical and not _symbol_matches(canonical, sym):
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
        orderbook = await self._fetch_orderbook(
            client=client,
            exchange=exchange,
            symbol=symbol,
            ccxt_symbol=ccxt_symbol,
            depth=self._orderbook_depth,
        )
        if not orderbook:
            return None
        constraints = self._extract_market_constraints(client, ccxt_symbol)
        contract_size = constraints.get("contract_size") if constraints else None
        orderbook = _scale_orderbook(orderbook, contract_size)
        stats = orderbook_stats(orderbook, top_n=self._liquidity_top_n)
        best_bid = stats.best_bid
        best_ask = stats.best_ask
        mid = stats.mid

        candidate_price = _resolve_smart_limit_price(
            orderbook=orderbook,
            side=leg["side"],
            book_side=None,
            qty=qty,
            payload=payload,
            price_step=(constraints.get("price_step") if constraints else None),
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid,
            improve_ticks=int(_safe_float(payload.get("limit_improve_ticks")) or DEFAULT_LIMIT_IMPROVE_TICKS),
            offset_bps=None,
            offset_ticks=None,
            round_mode="passive",
        )
        if candidate_price is None:
            return None

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
        candidate_price = _ensure_maker_price(
            candidate_price,
            side=leg["side"],
            best_bid=best_bid,
            best_ask=best_ask,
            price_step=price_step,
        )
        return candidate_price if candidate_price and candidate_price > 0 else None

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
