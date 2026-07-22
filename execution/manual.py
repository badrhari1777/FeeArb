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
from execution.accounts import (
    EXCHANGE_SPECS,
    ExchangeGateway,
    _extract_leverage,
    _extract_margin_mode,
    _safe_float,
    bitget_position_side,
    bitget_private_params,
    bitget_uta_enabled,
    normalize_symbol,
)
from execution.ws_positions import LivePositionTracker
from execution.ws_orders import LiveOrderTracker

logger = logging.getLogger(__name__)
DEFAULT_MANUAL_LEVERAGE = 3.0
DEFAULT_MIN_LEVEL_NOTIONAL = 50.0
DEFAULT_MIN_LEVEL_CHUNK_PCT = 0.01
DEFAULT_LIMIT_IMPROVE_TICKS = 1
PRECHECK_RETRIES = 3
PRECHECK_RETRY_DELAY_SEC = 0.75
PRECHECK_BALANCE_BUFFER_PCT = 0.05
DEFAULT_VENUE_LIQUIDITY_TIER = 3
VENUE_LIQUIDITY_TIERS = {
    "binance": 1,
    "okx": 2,
}
AUTO_EXIT_RECOMMENDED_CHUNK_SAFETY_FACTOR = 0.5
AUTO_EXIT_FALLBACK_CHUNK_PCT = 0.25
AUTO_EXIT_MARKET_FALLBACK_MAX_TIER = 2
SMART_CHUNK_NOTIONAL_CAP_BY_TIER = {
    1: 750.0,
    2: 500.0,
    3: 250.0,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_binance_time_sync_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "-1021" in message
        or "timestamp for this request was" in message
        or "timestamp outside recvwindow" in message
        or "invalid nonce" in message
    )


def _bitget_params(params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return bitget_private_params(params)


async def _fetch_positions_compat(client: Any, exchange: str, symbols: list[str] | None = None) -> Any:
    if exchange == "bitget":
        return await client.fetch_positions(symbols, _bitget_params({}))
    if symbols is not None:
        return await client.fetch_positions(symbols)
    return await client.fetch_positions()


@dataclass(slots=True)
class OrderBookStats:
    best_bid: float | None
    best_ask: float | None
    spread: float | None
    mid: float | None
    bid_liquidity_top3: float
    ask_liquidity_top3: float
    min_liquidity_top3: float


def _precision_to_step(value: Any, precision_mode: Any = None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    try:
        mode_value = int(precision_mode) if precision_mode is not None else None
    except (TypeError, ValueError):
        mode_value = None
    if mode_value == 4 or str(precision_mode or "").upper() == "TICK_SIZE":
        return numeric
    if numeric >= 1:
        try:
            return 10 ** (-int(numeric))
        except (TypeError, ValueError, OverflowError):
            return None
    return numeric


def _market_filter_value(market: Mapping[str, Any] | None, filter_type: str, *keys: str) -> float | None:
    if not isinstance(market, Mapping):
        return None
    info = market.get("info") or {}
    if not isinstance(info, Mapping):
        return None
    filters = info.get("filters")
    if not isinstance(filters, list):
        return None
    wanted = str(filter_type or "").upper()
    for item in filters:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("filterType") or "").upper() != wanted:
            continue
        for key in keys:
            value = _safe_float(item.get(key))
            if value is not None and value > 0:
                return value
    return None


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


def _pending_hedge_order_qty(
    qty: float,
    *,
    min_qty_required: float | None,
    amount_step: float | None,
) -> float:
    if qty <= 0:
        return 0.0
    rounded = _round_to_step(float(qty), amount_step, mode="down")
    if rounded <= 0:
        return 0.0
    if min_qty_required:
        tol = (float(amount_step) * 0.5) if amount_step and amount_step > 0 else 1e-12
        if rounded + tol < float(min_qty_required):
            return 0.0
    return rounded


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


def _position_side_for_leg(leg: Mapping[str, Any]) -> str:
    if leg.get("reduce_only"):
        return _exit_position_side(leg)
    return _entry_position_side(leg)


def _position_delta_for_leg(start: float | None, current: float | None, leg: Mapping[str, Any]) -> float:
    start_qty = _safe_float(start) or 0.0
    current_qty = _safe_float(current) or 0.0
    if leg.get("reduce_only"):
        return max(0.0, start_qty - current_qty)
    return max(0.0, current_qty - start_qty)


def _cap_qty_to_target(
    *,
    requested_qty: float,
    target_qty: float,
    leg_delta: float,
    amount_step: float | None = None,
) -> float:
    requested = max(0.0, _safe_float(requested_qty) or 0.0)
    target = max(0.0, _safe_float(target_qty) or 0.0)
    delta = max(0.0, _safe_float(leg_delta) or 0.0)
    remaining_to_target = max(0.0, target - delta)
    capped = min(requested, remaining_to_target)
    if amount_step:
        capped = _round_to_step(capped, amount_step, mode="down")
    return max(0.0, capped)


def _cap_qty_to_absolute_target(
    *,
    requested_qty: float,
    target_qty: float,
    current_qty: float,
    amount_step: float | None = None,
) -> float:
    requested = max(0.0, _safe_float(requested_qty) or 0.0)
    target = max(0.0, _safe_float(target_qty) or 0.0)
    current = max(0.0, _safe_float(current_qty) or 0.0)
    remaining_to_target = max(0.0, target - current)
    capped = min(requested, remaining_to_target)
    if amount_step:
        capped = _round_to_step(capped, amount_step, mode="down")
    return max(0.0, capped)


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


def _default_smart_chunk_notional_cap(legs: Iterable[Mapping[str, Any]]) -> float:
    tiers = [venue_liquidity_tier(leg.get("exchange")) for leg in legs if isinstance(leg, Mapping)]
    worst_tier = max(tiers) if tiers else DEFAULT_VENUE_LIQUIDITY_TIER
    if worst_tier <= 1:
        return SMART_CHUNK_NOTIONAL_CAP_BY_TIER[1]
    if worst_tier == 2:
        return SMART_CHUNK_NOTIONAL_CAP_BY_TIER[2]
    return SMART_CHUNK_NOTIONAL_CAP_BY_TIER[3]


def _cap_auto_chunk_by_notional(
    *,
    requested_qty: float | None,
    chunk_notional: float | None,
    max_chunk: float | None,
    mid_price: float | None,
    legs: Iterable[Mapping[str, Any]],
) -> tuple[float | None, float | None]:
    if requested_qty is not None and requested_qty > 0:
        return max_chunk, None
    if chunk_notional is not None and chunk_notional > 0:
        return max_chunk, None
    if mid_price is None or mid_price <= 0:
        return max_chunk, None
    cap_notional = _default_smart_chunk_notional_cap(legs)
    cap_qty = cap_notional / float(mid_price)
    if cap_qty <= 0:
        return max_chunk, None
    if max_chunk is None or cap_qty < max_chunk:
        return cap_qty, cap_notional
    return max_chunk, cap_notional


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


def _bingx_invalid_leverage_params(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "109400" in message
        or "invalid parameters" in message
        or "invalid parameter" in message
        or "requires a side argument" in message
        or "one of (long, short, both)" in message
    )


def _binance_margin_mode_noop(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no need to change margin type" in message or "-4046" in message


def _binance_margin_mode_blocked(exc: Exception) -> bool:
    message = str(exc).lower()
    return "position side cannot be changed" in message or "-4067" in message


def _binance_leverage_noop(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "no need to change leverage" in message
        or ("leverage" in message and "already exist" in message)
    )


def _binance_retryable_leverage_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "requesttimeout" in message
        or "read timed out" in message
        or "timed out" in message
        or "connection reset" in message
        or "connection aborted" in message
        or "service unavailable" in message
        or "temporarily unavailable" in message
        or "exchange not available" in message
        or "networkerror" in message
        or "network error" in message
        or "binanceusdm post https://fapi.binance.com/fapi/v1/leverage" in message
    )


def _kucoin_margin_mode_noop(exc: Exception) -> bool:
    message = str(exc).lower()
    if "margin" not in message:
        return False
    if "no need" in message or "already" in message:
        return True
    return "same" in message and "margin" in message


def _is_min_order_size_error(error: Any) -> bool:
    message = str(error or "").lower()
    if not message:
        return False
    patterns = (
        "min qty",
        "minimum qty",
        "minimum quantity",
        "min notional",
        "minimum notional",
        "less than minimum",
        "below minimum",
        "order amount too small",
        "quantity too small",
        "invalid quantity",
        "lot size",
        "filter failure: lot_size",
        "filter failure: min_notional",
        "insufficient notional",
        "amount precision",
    )
    return any(pattern in message for pattern in patterns)


def _classify_submit_error(error: Any) -> str | None:
    message = str(error or "")
    lower = message.lower()
    if not lower:
        return None
    if "invalid api-key" in lower or "permissions for action" in lower or "authenticationerror" in lower:
        return "auth_error"
    if "tradfi-perps agreement" in lower:
        return "tradfi_agreement_required"
    if "position risk control" in lower and "reduce-only" in lower:
        return "reduce_only_required"
    if "reduce-only" in lower and (
        "no open position" in lower
        or "position qty" in lower
        or "exceeds open position" in lower
        or "no reducible position" in lower
    ):
        return "reduce_only_required"
    if (
        "price band" in lower
        or "price limit" in lower
        or "outside price limits" in lower
        or "above max price" in lower
        or "below min price" in lower
    ):
        return "price_band"
    if "tick size" in lower:
        return "tick_size"
    if _is_min_order_size_error(message) or "notional must be no smaller than 5" in lower:
        return "min_order_size"
    if "maximum risk limit" in lower or "\"code\":\"300005\"" in lower or "\"code\":300005" in lower:
        return "risk_limit"
    return None


def _normalize_submit_values(
    *,
    qty: float,
    price: float | None,
    side: str,
    order_type: str,
    min_qty: float | None,
    min_notional: float | None,
    amount_step: float | None,
    price_step: float | None,
    price_min: float | None = None,
    price_max: float | None = None,
) -> tuple[float | None, float | None, str | None]:
    qty_base = float(qty)
    if amount_step and amount_step > 0:
        qty_base = _round_to_step(qty_base, amount_step, mode="down")
    if qty_base <= 0:
        return None, None, "qty_below_step"
    qty_tol = (amount_step * 0.5) if amount_step and amount_step > 0 else 1e-12
    if min_qty is not None and qty_base + qty_tol < float(min_qty):
        return None, None, f"qty {qty_base:g} below min qty {float(min_qty):g}"
    adjusted_price = float(price) if price is not None else None
    if adjusted_price is not None and adjusted_price <= 0:
        return None, None, "invalid_price"
    if adjusted_price is not None and price_step and price_step > 0 and order_type == "limit":
        round_mode = "down" if str(side).lower() == "buy" else "up"
        adjusted_price = _round_to_step(adjusted_price, price_step, mode=round_mode)
        if adjusted_price <= 0:
            return None, None, "price_below_step"
    if adjusted_price is not None and price_min is not None and adjusted_price < float(price_min):
        return None, None, f"price {adjusted_price:g} below min price {float(price_min):g}"
    if adjusted_price is not None and price_max is not None and adjusted_price > float(price_max):
        return None, None, f"price {adjusted_price:g} above max price {float(price_max):g}"
    if min_notional is not None and adjusted_price is not None and adjusted_price > 0:
        if qty_base * adjusted_price + 1e-12 < float(min_notional):
            return None, None, (
                f"order notional {(qty_base * adjusted_price):g} below min notional {float(min_notional):g}"
            )
    return qty_base, adjusted_price, None


def _ccxt_precision_value(client: Any, kind: str, symbol: str, value: float | None) -> float | None:
    if value is None:
        return None
    method_name = "price_to_precision" if kind == "price" else "amount_to_precision"
    method = getattr(client, method_name, None)
    if not callable(method):
        return float(value)
    try:
        return float(method(symbol, value))
    except Exception:  # pylint: disable=broad-except
        return float(value)


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


def _hedge_price_move_ticks(
    side: str,
    order_price: float,
    best_bid: float | None,
    best_ask: float | None,
    price_step: float | None,
) -> tuple[float | None, float | None]:
    if order_price <= 0 or not price_step or price_step <= 0:
        return None, None
    if side == "buy":
        if best_ask is None:
            return None, None
        favorable = (order_price - best_ask) / price_step if best_ask < order_price else 0.0
        adverse = (best_ask - order_price) / price_step if best_ask > order_price else 0.0
        return favorable, adverse
    if best_bid is None:
        return None, None
    favorable = (best_bid - order_price) / price_step if best_bid > order_price else 0.0
    adverse = (order_price - best_bid) / price_step if best_bid < order_price else 0.0
    return favorable, adverse


def _price_deviation_bps(current: float | None, target: float | None) -> float | None:
    if not current or not target:
        return None
    if current <= 0 or target <= 0:
        return None
    return abs(target - current) / current * 10_000.0


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


def _gate_contract_id(raw_symbol: str) -> str | None:
    symbol = (raw_symbol or "").strip().upper()
    if not symbol:
        return None
    if ":" in symbol:
        symbol = symbol.split(":", 1)[0]
    if "/" in symbol:
        base, quote = symbol.split("/", 1)
        quote = quote.split(":", 1)[0]
        symbol = f"{base}{quote}"
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
    else:
        base = symbol
    if not base:
        return None
    return f"{base}_USDT"


def _gate_market_from_contract(contract: Mapping[str, Any]) -> dict[str, Any] | None:
    contract_id = str(contract.get("name") or "").strip().upper()
    if not contract_id or not contract_id.endswith("_USDT"):
        return None
    base = contract_id[:-5]
    if not base:
        return None
    price_step = _safe_float(contract.get("order_price_round")) or _safe_float(
        contract.get("mark_price_round")
    )
    amount_min = _safe_float(contract.get("order_size_min"))
    amount_max = _safe_float(contract.get("order_size_max"))
    contract_size = _safe_float(contract.get("quanto_multiplier")) or 1.0
    symbol = f"{base}/USDT:USDT"
    active = not bool(contract.get("in_delisting")) and str(
        contract.get("status") or "trading"
    ).lower() == "trading"
    decimal_size = bool(contract.get("enable_decimal"))
    amount_precision = amount_min if decimal_size and amount_min and amount_min > 0 else 1.0
    return {
        "id": contract_id,
        "lowercaseId": contract_id.lower(),
        "symbol": symbol,
        "base": base,
        "quote": "USDT",
        "settle": "USDT",
        "baseId": base,
        "quoteId": "USDT",
        "settleId": "usdt",
        "type": "swap",
        "spot": False,
        "margin": False,
        "swap": True,
        "future": False,
        "option": False,
        "contract": True,
        "linear": True,
        "inverse": False,
        "active": active,
        "contractSize": contract_size,
        "precision": {
            "amount": amount_precision,
            "price": price_step,
        },
        "limits": {
            "amount": {"min": amount_min, "max": amount_max},
            "price": {"min": price_step, "max": None},
            "cost": {"min": None, "max": None},
            "leverage": {
                "min": _safe_float(contract.get("leverage_min")),
                "max": _safe_float(contract.get("leverage_max")),
            },
        },
        "info": dict(contract),
    }


def _normalize_manual_symbol(symbol: str | None) -> str:
    normalized = normalize_symbol(symbol or "")
    if not normalized:
        return ""
    for suffix in ("UMCBL", "DMCBL"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    if normalized.endswith("USDTM"):
        normalized = normalized[:-1]
    for suffix in ("SWAP", "PERP"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    for settle in ("USDT", "USDC", "USD"):
        double = settle + settle
        while normalized.endswith(double):
            normalized = normalized[: -len(settle)]
    return normalized


def _symbol_matches(canonical: str, candidate: str) -> bool:
    canonical = _normalize_manual_symbol(canonical)
    candidate = _normalize_manual_symbol(candidate)
    if not canonical:
        return True
    quote_suffixes = ("USDT", "USDC", "USD")

    def _parts(symbol: str) -> tuple[str, str | None]:
        for quote in quote_suffixes:
            if symbol.endswith(quote):
                symbol = symbol[: -len(quote)]
                if symbol == "XBT":
                    symbol = "BTC"
                return symbol, quote
        if symbol == "XBT":
            symbol = "BTC"
        return symbol, None

    canonical_base, canonical_quote = _parts(canonical)
    candidate_base, candidate_quote = _parts(candidate)
    if canonical_base != candidate_base:
        return False
    if canonical_quote is not None and candidate_quote is not None:
        return canonical_quote == candidate_quote
    return True


def _trigger_wait_sec(payload: Mapping[str, Any], max_runtime_sec: float) -> float:
    configured = _safe_float(payload.get("trigger_wait_sec"))
    if configured is None:
        configured = 30.0
    return max(1.0, min(float(configured), max(1.0, float(max_runtime_sec))))


def _normalize_position_side(side: Any, qty: float | None = None) -> str:
    side_raw = str(side or "").strip().lower()
    if side_raw in ("long", "buy"):
        return "long"
    if side_raw in ("short", "sell"):
        return "short"
    if qty is not None:
        if qty < 0:
            return "short"
        if qty > 0:
            return "long"
    return ""


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


def venue_liquidity_tier(exchange: str | None) -> int:
    normalized = normalize_exchange_name(str(exchange or ""))
    return int(VENUE_LIQUIDITY_TIERS.get(normalized, DEFAULT_VENUE_LIQUIDITY_TIER))


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
    long_tier = venue_liquidity_tier(long_exchange)
    short_tier = venue_liquidity_tier(short_exchange)
    suggestion = "long"
    reason = "higher_taker_fee"
    if long_tier != short_tier:
        suggestion = "long" if long_tier > short_tier else "short"
        reason = "lower_venue_tier"
    else:
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
        "venue_tier": {"long": long_tier, "short": short_tier},
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
        self._ws_order_blocked: dict[str, dict[str, Any]] = {}
        self._prepared_margin_settings: set[tuple[str, str, str]] = set()
        self._prepared_leverage_settings: set[tuple[str, str, str, int]] = set()

    async def close(self) -> None:
        await asyncio.gather(
            *(gateway.close() for gateway in self._gateways.values()),
            return_exceptions=True,
        )

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
        return bool(self._stop_signal().get("requested"))

    def _stop_signal(self) -> dict[str, Any]:
        if not self._stop_check:
            return {"requested": False, "force_finalize": False, "reason": None}
        try:
            value = self._stop_check()
            if isinstance(value, Mapping):
                return {
                    "requested": bool(value.get("requested", True)),
                    "force_finalize": bool(value.get("force_finalize")),
                    "reason": value.get("reason"),
                }
            return {"requested": bool(value), "force_finalize": False, "reason": None}
        except Exception:
            return {"requested": False, "force_finalize": False, "reason": None}

    def _stop_force_finalize(self) -> bool:
        return bool(self._stop_signal().get("force_finalize"))

    def _auto_exit_market_fallback_allowed(
        self,
        payload: Mapping[str, Any] | None,
        exchange: str | None,
        *,
        notional_usd: float | None = None,
    ) -> bool:
        if not payload or not bool(payload.get("auto_exit_agent")):
            return True
        tier_limit = int(
            _safe_float(payload.get("auto_exit_market_tier_max")) or AUTO_EXIT_MARKET_FALLBACK_MAX_TIER
        )
        if venue_liquidity_tier(exchange) > max(1, tier_limit):
            return False
        cleanup_cap = _safe_float(payload.get("auto_exit_market_cleanup_notional_max"))
        if cleanup_cap is not None:
            if cleanup_cap <= 0:
                return False
            if notional_usd is not None and notional_usd > cleanup_cap:
                return False
        return True

    def _auto_exit_final_reconcile_blocked(
        self,
        payload: Mapping[str, Any] | None,
        exchange: str | None,
        *,
        notional_usd: float | None = None,
        primary_delta: float | None = None,
        hedge_delta: float | None = None,
        primary_filled_total: float | None = None,
        hedge_filled_total: float | None = None,
    ) -> bool:
        filled_exposure_exists = any(
            (_safe_float(value) or 0.0) > 0
            for value in (primary_delta, hedge_delta, primary_filled_total, hedge_filled_total)
        )
        if filled_exposure_exists:
            return False
        return not self._auto_exit_market_fallback_allowed(
            payload,
            exchange,
            notional_usd=notional_usd,
        )

    def _apply_auto_exit_exit_overrides(
        self,
        payload: Mapping[str, Any],
        plan: Mapping[str, Any],
        *,
        log_cb: Optional[callable] = None,
    ) -> Mapping[str, Any]:
        if str(plan.get("action") or "") != "exit" or not bool(payload.get("auto_exit_agent")):
            return payload
        updated_payload = dict(payload)
        changes: dict[str, Any] = {}

        suggested_leg = (plan.get("suggested_expensive_leg") or {}).get("suggested_leg")
        if not updated_payload.get("expensive_leg") and suggested_leg in ("long", "short"):
            updated_payload["expensive_leg"] = suggested_leg
            changes["expensive_leg"] = suggested_leg

        requested_chunk = _safe_float(updated_payload.get("chunk_qty"))
        dynamic_chunking = bool(updated_payload.get("auto_exit_dynamic_chunk"))
        recommended_chunk = _safe_float(plan.get("recommended_chunk_qty"))
        min_chunk_qty = _safe_float(plan.get("min_chunk_qty"))
        qty = _safe_float(plan.get("qty"))
        if (requested_chunk is None or requested_chunk <= 0) and not dynamic_chunking:
            safe_chunk = None
            if recommended_chunk and recommended_chunk > 0:
                safe_chunk = recommended_chunk * AUTO_EXIT_RECOMMENDED_CHUNK_SAFETY_FACTOR
            elif qty and qty > 0:
                safe_chunk = qty * AUTO_EXIT_FALLBACK_CHUNK_PCT
            amount_steps = [
                _safe_float((info or {}).get("amount_step"))
                for info in (plan.get("market_constraints") or {}).values()
            ]
            amount_step = max([step for step in amount_steps if step], default=None)
            if safe_chunk and min_chunk_qty:
                safe_chunk = max(safe_chunk, min_chunk_qty)
            if safe_chunk and qty:
                safe_chunk = min(safe_chunk, qty)
            if safe_chunk and amount_step:
                safe_chunk = _round_to_step(safe_chunk, amount_step, mode="down")
                if min_chunk_qty and safe_chunk < min_chunk_qty:
                    safe_chunk = _round_to_step(min_chunk_qty, amount_step, mode="up")
            if safe_chunk and safe_chunk > 0:
                updated_payload["chunk_qty"] = safe_chunk
                changes["chunk_qty"] = safe_chunk

        if not updated_payload.get("hedge_order_type"):
            updated_payload["hedge_order_type"] = "limit"
            changes["hedge_order_type"] = "limit"
        if updated_payload.get("hedge_limit_mode") != "aggressive":
            updated_payload["hedge_limit_mode"] = "aggressive"
            changes["hedge_limit_mode"] = "aggressive"
        if updated_payload.get("hedge_offset_ticks") is None and updated_payload.get("hedge_offset_bps") is None:
            updated_payload["hedge_offset_ticks"] = 1
            changes["hedge_offset_ticks"] = 1
        if updated_payload.get("hedge_favorable_bps") is None:
            updated_payload["hedge_favorable_bps"] = 2.0
            changes["hedge_favorable_bps"] = 2.0
        if updated_payload.get("hedge_adverse_bps") is None:
            updated_payload["hedge_adverse_bps"] = 6.0
            changes["hedge_adverse_bps"] = 6.0
        if updated_payload.get("hedge_reprice_min_sec") is None:
            updated_payload["hedge_reprice_min_sec"] = 2.0
            changes["hedge_reprice_min_sec"] = 2.0
        if updated_payload.get("max_limit_deviation_bps") is None:
            updated_payload["max_limit_deviation_bps"] = 20.0
            changes["max_limit_deviation_bps"] = 20.0
        if updated_payload.get("auto_exit_market_tier_max") is None:
            updated_payload["auto_exit_market_tier_max"] = AUTO_EXIT_MARKET_FALLBACK_MAX_TIER
            changes["auto_exit_market_tier_max"] = AUTO_EXIT_MARKET_FALLBACK_MAX_TIER

        if changes:
            self._emit_log(log_cb, "decision", "auto-exit safety overrides applied", changes)
        return updated_payload

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

    def _mark_ws_order_blocked(self, exchange: str, action: str) -> None:
        exchange = normalize_exchange_name(exchange)
        if not exchange:
            return
        self._ws_order_blocked[exchange] = {
            "exchange": exchange,
            "action": action,
            "ts": time.time(),
        }

    async def _ensure_ws_orders_recovered(
        self,
        exchange: str,
        *,
        reason: str | None = None,
        log_cb: Optional[callable] = None,
    ) -> bool:
        exchange = normalize_exchange_name(exchange)
        if exchange not in self._ws_order_blocked:
            return True
        block = dict(self._ws_order_blocked.get(exchange) or {})
        self._emit_log(
            log_cb,
            "wait",
            "ws listenKey error; reconnecting before order action",
            {"exchange": exchange, "reason": reason, **block},
        )
        config = self._ws_orders.health_config(exchange)
        attempts = int(config.get("reconnect_attempts") or 0)
        grace_sec = _safe_float(config.get("reconnect_grace_sec")) or 0.0
        if attempts <= 0 or grace_sec <= 0:
            self._emit_log(
                log_cb,
                "error",
                "ws listenKey recovery disabled; blocking order action",
                {"exchange": exchange, "reason": reason, **block},
            )
            return False
        recovered = await self._ws_orders.await_healthy(
            exchange,
            attempts=attempts,
            grace_sec=grace_sec,
        )
        if recovered:
            self._ws_order_blocked.pop(exchange, None)
            self._emit_log(
                log_cb,
                "info",
                "ws order stream recovered after listenKey error",
                {"exchange": exchange, "reason": reason},
            )
            return True
        self._emit_log(
            log_cb,
            "error",
            "ws order stream recovery failed after listenKey error",
            {"exchange": exchange, "reason": reason, **block},
        )
        return False

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
                    # Some WS streams can emit terminal state with zero fill before
                    # REST catches up; treat it as inconclusive and continue probing.
                    if status in ("filled", "closed", "finished") and filled <= 0:
                        status = "open"
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
                if status in ("filled", "closed", "finished") and filled <= 0:
                    status = "open"
                elif status in ("filled", "closed", "finished"):
                    status = "filled"
                elif filled > 0:
                    status = "partial"
                if status != "open" or filled > 0:
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

    async def orphan_cleanup(
        self,
        payload: Mapping[str, Any],
        positions: Iterable[Mapping[str, Any]],
        *,
        log_cb: Optional[callable] = None,
        stop_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        orphan_payload = dict(payload)
        orphan_payload["orphan_cleanup_mode"] = True
        return await self._handle_pair(
            orphan_payload,
            action="exit",
            positions=positions,
            log_cb=log_cb,
            stop_cb=stop_cb,
        )

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
            self._prepared_margin_settings.clear()
            self._prepared_leverage_settings.clear()
            self._apply_ws_orders_health(payload)
            if log_cb:
                self._emit_log(log_cb, "payload", "manual payload", dict(payload))
            self._stop_check = stop_cb
            symbol = str(payload.get("symbol") or "").upper().strip()
            positions_for_plan = list(positions or [])
            if action == "exit" and bool(payload.get("orphan_cleanup_mode")):
                try:
                    return await self._execute_orphan_cleanup(
                        payload,
                        positions_for_plan,
                        log_cb=log_cb,
                    )
                finally:
                    self._stop_check = None
            exit_allow_flip = bool(payload.get("exit_allow_flip")) if action == "exit" else False
            exchanges_hint: list[str] = []
            if action == "roll":
                exchanges_hint = [
                    normalize_exchange_name(str(payload.get("from_exchange") or "")),
                    normalize_exchange_name(str(payload.get("to_exchange") or "")),
                ]
            else:
                exchanges_hint = [
                    normalize_exchange_name(str(payload.get("long_exchange") or "")),
                    normalize_exchange_name(str(payload.get("short_exchange") or "")),
                ]
            exchanges_hint = [ex for ex in exchanges_hint if ex]
            if symbol and exchanges_hint:
                positions_for_plan, pos_errors = await self._fetch_positions_with_retry(
                    exchanges=exchanges_hint,
                    symbol=symbol,
                    log_cb=log_cb,
                )
                if pos_errors:
                    if exit_allow_flip and action == "exit":
                        if log_cb:
                            self._emit_log(
                                log_cb,
                                "warn",
                                "positions fetch failed; continuing (exit allow flip)",
                                {"errors": pos_errors},
                            )
                    else:
                        self._stop_check = None
                        errors = [f"positions fetch failed: {err}" for err in pos_errors]
                        if log_cb:
                            self._emit_log(
                                log_cb,
                                "error",
                                "positions fetch failed; stopping",
                                {"errors": errors},
                            )
                        return {
                            "dry_run": False,
                            "action": action,
                            "symbol": symbol,
                            "qty": payload.get("qty"),
                            "mode": payload.get("mode"),
                            "legs": [],
                            "errors": errors,
                            "warnings": [],
                            "generated_at": datetime.now(timezone.utc).isoformat(),
                        }
                positions_for_plan = [
                    position
                    for position in positions_for_plan
                    if _symbol_matches(
                        symbol,
                        str(position.get("symbol") or position.get("symbol_normalized") or ""),
                    )
                ]
                if log_cb and action in ("exit", "roll"):
                    by_exchange_side: dict[str, float] = {}
                    sample: list[dict[str, Any]] = []
                    for pos in positions_for_plan:
                        exchange_name = normalize_exchange_name(str(pos.get("exchange") or ""))
                        qty = _safe_float(pos.get("coin_qty"))
                        if qty is None:
                            qty = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
                        side_name = _normalize_position_side(pos.get("side"), qty)
                        key = f"{exchange_name}:{side_name or '-'}"
                        by_exchange_side[key] = by_exchange_side.get(key, 0.0) + abs(qty or 0.0)
                        if len(sample) < 12:
                            sample.append(
                                {
                                    "exchange": exchange_name,
                                    "symbol": pos.get("symbol") or pos.get("symbol_normalized"),
                                    "side": str(pos.get("side") or ""),
                                    "side_normalized": side_name or None,
                                    "coin_qty": qty,
                                }
                            )
                    self._emit_log(
                        log_cb,
                        "positions",
                        "positions snapshot (precheck)",
                        {
                            "symbol": symbol,
                            "count": len(positions_for_plan),
                            "by_exchange_side": by_exchange_side,
                            "sample": sample,
                        },
                    )
            plan = await self._build_plan(payload, action=action, positions=positions_for_plan)
            if payload.get("dry_run", False) or plan["errors"]:
                self._stop_check = None
                return plan
            adjusted_payload = payload
            adjusted_plan = plan
            adjusted_payload = self._apply_auto_exit_exit_overrides(
                adjusted_payload,
                adjusted_plan,
                log_cb=log_cb,
            )
            precheck_errors: list[str] = []
            if action == "enter":
                balances, balance_errors = await self._fetch_balances_with_retry(
                    exchanges=exchanges_hint,
                    log_cb=log_cb,
                )
                if balance_errors:
                    precheck_errors.extend(
                        [f"balances fetch failed: {err}" for err in balance_errors]
                    )
                else:
                    prices, price_errors = await self._fetch_mark_prices_with_retry(
                        exchanges=exchanges_hint,
                        symbol=symbol,
                        log_cb=log_cb,
                    )
                    if price_errors:
                        precheck_errors.extend(
                            [f"mark price fetch failed: {err}" for err in price_errors]
                        )
                    else:
                        qty = _safe_float(adjusted_plan.get("qty")) or 0.0
                        if qty <= 0:
                            precheck_errors.append("qty must be > 0 for enter")
                        else:
                            min_qty, step = self._min_qty_and_step(adjusted_plan)
                            per_exchange: dict[str, Any] = {}
                            balance_limits: list[dict[str, float | str]] = []
                            for exchange in exchanges_hint:
                                available = _safe_float(balances.get(exchange, {}).get("available"))
                                mark_price = prices.get(exchange)
                                if available is None:
                                    precheck_errors.append(f"{exchange}: available balance missing")
                                    continue
                                if not mark_price or mark_price <= 0:
                                    precheck_errors.append(f"{exchange}: mark price unavailable")
                                    continue
                                required = (
                                    qty
                                    * mark_price
                                    / DEFAULT_MANUAL_LEVERAGE
                                    * (1.0 + PRECHECK_BALANCE_BUFFER_PCT)
                                )
                                per_exchange[exchange] = {
                                    "available": available,
                                    "mark_price": mark_price,
                                    "required": required,
                                }
                                if available < required:
                                    max_qty = (
                                        available
                                        * DEFAULT_MANUAL_LEVERAGE
                                        / (mark_price * (1.0 + PRECHECK_BALANCE_BUFFER_PCT))
                                    )
                                    min_qty_required = (
                                        min_qty
                                        * mark_price
                                        / DEFAULT_MANUAL_LEVERAGE
                                        * (1.0 + PRECHECK_BALANCE_BUFFER_PCT)
                                        if min_qty
                                        else 0.0
                                    )
                                    balance_limits.append(
                                        {
                                            "exchange": exchange,
                                            "available": available,
                                            "mark_price": mark_price,
                                            "required": required,
                                            "max_qty": max_qty,
                                            "min_qty_required": min_qty_required,
                                        }
                                    )
                            if per_exchange:
                                self._emit_log(
                                    log_cb,
                                    "precheck",
                                    "enter balance check",
                                    {
                                        "qty": qty,
                                        "buffer_pct": PRECHECK_BALANCE_BUFFER_PCT,
                                        "leverage": DEFAULT_MANUAL_LEVERAGE,
                                        "per_exchange": per_exchange,
                                    },
                                )
                            if balance_limits:
                                limiting = min(
                                    balance_limits,
                                    key=lambda item: float(item.get("max_qty") or 0.0),
                                )
                                new_qty = min(
                                    [float(item.get("max_qty") or 0.0) for item in balance_limits]
                                    + [qty]
                                )
                                if step:
                                    new_qty = _round_to_step(new_qty, step, mode="down")
                                if min_qty and new_qty < min_qty:
                                    precheck_errors.append(
                                        f"{limiting['exchange']}: insufficient balance for min qty {min_qty:g} "
                                        f"(available={float(limiting['available']):g} USDT, "
                                        f"required_min={float(limiting['min_qty_required']):g} USDT)"
                                    )
                                elif new_qty < qty:
                                    adjusted_payload, adjusted_plan = self._adjust_payload_qty(
                                        adjusted_payload,
                                        adjusted_plan,
                                        new_qty=new_qty,
                                        log_cb=log_cb,
                                        reason="enter balance limit",
                                    )
            elif action == "exit":
                long_exchange = normalize_exchange_name(str(adjusted_payload.get("long_exchange") or ""))
                short_exchange = normalize_exchange_name(str(adjusted_payload.get("short_exchange") or ""))
                qty = _safe_float(adjusted_plan.get("qty")) or 0.0
                if qty <= 0:
                    precheck_errors.append("qty must be > 0 for exit")
                elif exit_allow_flip:
                    if log_cb:
                        self._emit_log(
                            log_cb,
                            "precheck",
                            "exit allow flip: skipping position cap",
                            {
                                "symbol": symbol,
                                "qty": qty,
                                "long_exchange": long_exchange,
                                "short_exchange": short_exchange,
                            },
                        )
                else:
                    long_qty = self._sum_position_qty(
                        positions_for_plan,
                        exchange=long_exchange,
                        side="long",
                        symbol=symbol,
                    )
                    short_qty = self._sum_position_qty(
                        positions_for_plan,
                        exchange=short_exchange,
                        side="short",
                        symbol=symbol,
                    )
                    if long_qty <= 0:
                        opposite = self._sum_position_qty(
                            positions_for_plan,
                            exchange=long_exchange,
                            side="short",
                            symbol=symbol,
                        )
                        if opposite > 0:
                            precheck_errors.append(
                                f"{long_exchange}: expected long position, found short {opposite:g}"
                            )
                        else:
                            precheck_errors.append(
                                f"{long_exchange}: expected long position, found none"
                            )
                    if short_qty <= 0:
                        opposite = self._sum_position_qty(
                            positions_for_plan,
                            exchange=short_exchange,
                            side="long",
                            symbol=symbol,
                        )
                        if opposite > 0:
                            precheck_errors.append(
                                f"{short_exchange}: expected short position, found long {opposite:g}"
                            )
                        else:
                            precheck_errors.append(
                                f"{short_exchange}: expected short position, found none"
                            )
                    if not precheck_errors:
                        available_qty = min(long_qty, short_qty)
                        if available_qty < qty:
                            min_qty, step = self._min_qty_and_step(adjusted_plan)
                            new_qty = available_qty
                            if step:
                                new_qty = _round_to_step(new_qty, step, mode="down")
                            if min_qty and new_qty < min_qty:
                                precheck_errors.append(
                                    f"insufficient position for min qty {min_qty:g}"
                                )
                            elif new_qty < qty:
                                adjusted_payload, adjusted_plan = self._adjust_payload_qty(
                                    adjusted_payload,
                                    adjusted_plan,
                                    new_qty=new_qty,
                                    log_cb=log_cb,
                                    reason="exit position limit",
                                )
            elif action == "roll":
                from_exchange = normalize_exchange_name(str(adjusted_payload.get("from_exchange") or ""))
                side = str(adjusted_payload.get("side") or "").lower()
                qty = _safe_float(adjusted_plan.get("qty")) or 0.0
                if qty <= 0:
                    precheck_errors.append("qty must be > 0 for roll")
                elif side not in ("long", "short"):
                    precheck_errors.append("side must be 'long' or 'short' for roll")
                else:
                    from_qty = self._sum_position_qty(
                        positions_for_plan,
                        exchange=from_exchange,
                        side=side,
                        symbol=symbol,
                    )
                    if from_qty <= 0:
                        opposite = self._sum_position_qty(
                            positions_for_plan,
                            exchange=from_exchange,
                            side="short" if side == "long" else "long",
                            symbol=symbol,
                        )
                        if opposite > 0:
                            precheck_errors.append(
                                f"{from_exchange}: expected {side} position, found opposite {opposite:g}"
                            )
                        else:
                            precheck_errors.append(
                                f"{from_exchange}: expected {side} position, found none"
                            )
                    else:
                        if from_qty < qty:
                            min_qty, step = self._min_qty_and_step(adjusted_plan)
                            new_qty = from_qty
                            if step:
                                new_qty = _round_to_step(new_qty, step, mode="down")
                            if min_qty and new_qty < min_qty:
                                precheck_errors.append(
                                    f"insufficient position for min qty {min_qty:g}"
                                )
                            elif new_qty < qty:
                                adjusted_payload, adjusted_plan = self._adjust_payload_qty(
                                    adjusted_payload,
                                    adjusted_plan,
                                    new_qty=new_qty,
                                    log_cb=log_cb,
                                    reason="roll position limit",
                                )
            if precheck_errors:
                self._stop_check = None
                if log_cb:
                    self._emit_log(
                        log_cb,
                        "error",
                        "precheck failed; stopping",
                        {"errors": precheck_errors},
                    )
                return self._plan_with_runtime_errors(adjusted_plan, precheck_errors)
            leverage_errors = await self._ensure_binance_leverage_for_legs(
                adjusted_plan.get("legs") or [],
                symbol,
                log_cb=log_cb,
            )
            if leverage_errors:
                self._stop_check = None
                if log_cb:
                    self._emit_log(
                        log_cb,
                        "error",
                        "binance leverage precheck failed; stopping",
                        {"errors": leverage_errors},
                    )
                return self._plan_with_runtime_errors(adjusted_plan, leverage_errors)
            kucoin_errors = await self._ensure_kucoin_margin_mode_for_legs(
                adjusted_plan.get("legs") or [],
                symbol,
                log_cb=log_cb,
            )
            if kucoin_errors:
                self._stop_check = None
                if log_cb:
                    self._emit_log(
                        log_cb,
                        "error",
                        "kucoin margin mode precheck failed; stopping",
                        {"errors": kucoin_errors},
                    )
                return self._plan_with_runtime_errors(adjusted_plan, kucoin_errors)
            leverage_errors = await self._ensure_bingx_leverage_for_legs(
                adjusted_plan.get("legs") or [],
                symbol,
                log_cb=log_cb,
            )
            if leverage_errors:
                self._stop_check = None
                if log_cb:
                    self._emit_log(
                        log_cb,
                        "error",
                        "bingx leverage precheck failed; stopping",
                        {"errors": leverage_errors},
                    )
                return self._plan_with_runtime_errors(adjusted_plan, leverage_errors)
            legs = list(adjusted_plan.get("legs") or [])
            exchanges = []
            for leg in legs:
                exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
                if exchange and exchange not in exchanges:
                    exchanges.append(exchange)
            tracked_exchanges = set(exchanges)
            if log_cb:
                def _ws_event_cb(payload: Mapping[str, Any]) -> None:
                    exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
                    if tracked_exchanges and exchange not in tracked_exchanges:
                        return
                    action = str(payload.get("action") or "event")
                    if action in ("listen_key_expired", "listen_key_failed"):
                        self._mark_ws_order_blocked(exchange, action)
                    if action in ("probe_ping_sent", "probe_pong_received"):
                        return
                    message = self._format_ws_probe_message(exchange, action, payload)
                    self._emit_story(log_cb, message, payload)
                self._ws_orders.set_event_cb(_ws_event_cb)
            mode = str(
                adjusted_payload.get("mode")
                or ("smart-roll" if action == "roll" else "limit-first-expensive")
            )
            if action == "exit" and (adjusted_payload.get("spread_min_pct") is not None or adjusted_payload.get("spread_max_pct") is not None):
                if mode not in ("smart-exit", "fast-exit"):
                    mode = "smart-exit"
                    adjusted_plan.setdefault("warnings", []).append("mode overridden to smart-exit for spread-guided exit")
            if action == "enter" and (adjusted_payload.get("spread_min_pct") is not None or adjusted_payload.get("spread_max_pct") is not None):
                if mode not in ("smart-enter", "fast-enter"):
                    mode = "smart-enter"
                    adjusted_plan.setdefault("warnings", []).append("mode overridden to smart-enter for spread-guided enter")
            if action == "roll":
                requested_chunk = _safe_float(adjusted_payload.get("chunk_qty"))
                chunk_notional = _safe_float(adjusted_payload.get("chunk_notional"))
                force_chunk = bool(adjusted_payload.get("force_chunk_qty"))
                wants_chunk = (
                    (requested_chunk is not None and requested_chunk > 0)
                    or (chunk_notional is not None and chunk_notional > 0)
                    or force_chunk
                )
                if wants_chunk and mode != "smart-roll":
                    mode = "smart-roll"
                    adjusted_plan.setdefault("warnings", []).append(
                        "mode overridden to smart-roll because chunking requested"
                    )
            await self._log_positions_snapshot(
                exchanges=exchanges,
                symbol=str(adjusted_plan.get("symbol") or ""),
                stage="start",
                log_cb=log_cb,
            )
            try:
                return await self._execute_plan(
                    adjusted_plan,
                    mode=mode,
                    payload=adjusted_payload,
                    log_cb=log_cb,
                )
            finally:
                self._ws_orders.set_event_cb(None)
                self._stop_check = None
                await self._log_positions_snapshot(
                    exchanges=exchanges,
                    symbol=str(adjusted_plan.get("symbol") or ""),
                    stage="end",
                    log_cb=log_cb,
                )

    async def _ensure_bingx_leverage_for_legs(
        self,
        legs: Iterable[Mapping[str, Any]],
        symbol: str,
        *,
        log_cb: Optional[callable] = None,
    ) -> list[str]:
        errors: list[str] = []
        for leg in legs:
            exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
            if exchange != "bingx":
                continue
            if leg.get("reduce_only"):
                continue
            side = "LONG" if str(leg.get("side") or "").lower() == "buy" else "SHORT"
            client = await self._ensure_client(exchange, errors)
            if client is None:
                continue
            ccxt_symbol = await self._resolve_market_symbol(client, symbol)
            if not ccxt_symbol:
                errors.append(f"{exchange}: symbol unavailable for leverage precheck")
                continue
            current = None
            if hasattr(client, "fetch_leverage"):
                try:
                    info = await client.fetch_leverage(ccxt_symbol)
                    if isinstance(info, dict):
                        key = "longLeverage" if side == "LONG" else "shortLeverage"
                        current = _safe_float(info.get(key))
                except Exception as exc:  # pylint: disable=broad-except
                    self._emit_log(
                        log_cb,
                        "warn",
                        "bingx leverage fetch failed",
                        {"exchange": exchange, "symbol": symbol, "side": side, "error": str(exc)},
                    )
            if current is not None and abs(current - DEFAULT_MANUAL_LEVERAGE) <= 0.05:
                continue
            variants: list[tuple[str, Mapping[str, Any] | None]] = [
                (side, {"side": side}),
                ("BOTH", {"side": "BOTH"}),
                ("default", {}),
            ]
            last_exc: Exception | None = None
            invalid_param_only = True
            set_ok = False
            for variant_name, params in variants:
                try:
                    await client.set_leverage(DEFAULT_MANUAL_LEVERAGE, ccxt_symbol, params or None)
                    payload = {
                        "exchange": exchange,
                        "symbol": symbol,
                        "side": variant_name,
                        "leverage": DEFAULT_MANUAL_LEVERAGE,
                        "previous": current,
                    }
                    if variant_name != side:
                        payload["fallback_from"] = side
                    self._emit_log(
                        log_cb,
                        "precheck",
                        "bingx leverage set",
                        payload,
                    )
                    set_ok = True
                    break
                except Exception as exc:  # pylint: disable=broad-except
                    last_exc = exc
                    if not _bingx_invalid_leverage_params(exc):
                        invalid_param_only = False
                        break
            if set_ok:
                continue
            if invalid_param_only and last_exc is not None:
                self._emit_log(
                    log_cb,
                    "warn",
                    "bingx leverage precheck skipped (invalid params)",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "side": side,
                        "leverage": DEFAULT_MANUAL_LEVERAGE,
                        "error": str(last_exc),
                    },
                )
                continue
            error_text = str(last_exc) if last_exc is not None else "unknown_error"
            errors.append(f"{exchange}: set leverage failed ({side})")
            self._emit_log(
                log_cb,
                "error",
                "bingx leverage set failed",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "side": side,
                    "leverage": DEFAULT_MANUAL_LEVERAGE,
                    "error": error_text,
                },
            )
        return errors

    async def _ensure_kucoin_margin_mode_for_legs(
        self,
        legs: Iterable[Mapping[str, Any]],
        symbol: str,
        *,
        log_cb: Optional[callable] = None,
    ) -> list[str]:
        errors: list[str] = []
        for leg in legs:
            exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
            if exchange != "kucoin":
                continue
            if leg.get("reduce_only"):
                continue
            client = await self._ensure_client(exchange, errors)
            if client is None:
                continue
            ccxt_symbol = await self._resolve_market_symbol(client, symbol)
            if not ccxt_symbol:
                errors.append(f"{exchange}: symbol unavailable for margin mode precheck")
                continue
            desired = str(leg.get("margin_mode") or "isolated").strip().lower()
            if desired not in ("isolated", "cross"):
                continue
            current = await self._fetch_kucoin_symbol_margin_mode(
                client,
                symbol=symbol,
                ccxt_symbol=ccxt_symbol,
                log_cb=log_cb,
            )
            if current and current == desired:
                self._emit_log(
                    log_cb,
                    "precheck",
                    "kucoin margin mode already set",
                    {"exchange": exchange, "symbol": symbol, "margin_mode": desired},
                )
                continue
            params = {"marginMode": desired, "marginType": desired}
            try:
                await client.set_margin_mode(desired, ccxt_symbol, params)
                self._emit_log(
                    log_cb,
                    "precheck",
                    "kucoin margin mode set",
                    {"exchange": exchange, "symbol": symbol, "margin_mode": desired},
                )
            except Exception as exc:  # pylint: disable=broad-except
                if _kucoin_margin_mode_noop(exc):
                    self._emit_log(
                        log_cb,
                        "precheck",
                        "kucoin margin mode already set",
                        {
                            "exchange": exchange,
                            "symbol": symbol,
                            "margin_mode": desired,
                            "error": str(exc),
                        },
                    )
                    continue
                try:
                    await client.set_margin_mode(desired, ccxt_symbol)
                    self._emit_log(
                        log_cb,
                        "precheck",
                        "kucoin margin mode set",
                        {"exchange": exchange, "symbol": symbol, "margin_mode": desired},
                    )
                    continue
                except Exception:
                    pass
                errors.append(f"{exchange}: set margin mode failed")
                self._emit_log(
                    log_cb,
                    "error",
                    "kucoin margin mode set failed",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "margin_mode": desired,
                        "error": str(exc),
                    },
                )
        return errors

    async def _cancel_open_orders_for_symbol(
        self,
        client: Any,
        *,
        exchange: str,
        symbol: str,
        ccxt_symbol: str,
        log_cb: Optional[callable] = None,
    ) -> bool:
        used_cancel_all = False
        try:
            if hasattr(client, "cancel_all_orders"):
                cancel_all_params = _bitget_params({}) if exchange == "bitget" else {}
                await client.cancel_all_orders(ccxt_symbol, cancel_all_params)
                used_cancel_all = True
                self._emit_log(
                    log_cb,
                    "precheck",
                    "open orders canceled",
                    {"exchange": exchange, "symbol": symbol, "method": "cancel_all_orders"},
                )
                if exchange != "binance":
                    return True
        except Exception as exc:  # pylint: disable=broad-except
            self._emit_log(
                log_cb,
                "warn",
                "cancel_all_orders failed",
                {"exchange": exchange, "symbol": symbol, "error": str(exc)},
            )
        fetch_failed = False
        try:
            open_params = _bitget_params({}) if exchange == "bitget" else {}
            orders = await client.fetch_open_orders(ccxt_symbol, params=open_params)
        except Exception as exc:  # pylint: disable=broad-except
            self._emit_log(
                log_cb,
                "warn",
                "fetch_open_orders failed",
                {"exchange": exchange, "symbol": symbol, "error": str(exc)},
            )
            if exchange != "binance":
                return False
            fetch_failed = True
            orders = []
        if not orders and not used_cancel_all and exchange != "binance":
            return True
        canceled = 0
        for order in orders:
            order_id = order.get("id") if isinstance(order, dict) else None
            if not order_id:
                continue
            try:
                cancel_params = _bitget_params({}) if exchange == "bitget" else {}
                await client.cancel_order(order_id, ccxt_symbol, cancel_params)
                canceled += 1
            except Exception:
                continue
        self._emit_log(
            log_cb,
            "precheck",
            "open orders canceled",
            {"exchange": exchange, "symbol": symbol, "count": canceled, "method": "cancel_order"},
        )
        if exchange == "binance":
            algo_ok = await self._cancel_binance_open_algo_orders_for_symbol(
                client,
                exchange=exchange,
                symbol=symbol,
                ccxt_symbol=ccxt_symbol,
                log_cb=log_cb,
            )
            if not algo_ok:
                return False
        if fetch_failed and not used_cancel_all and canceled <= 0:
            return False
        return True

    async def _cancel_binance_open_algo_orders_for_symbol(
        self,
        client: Any,
        *,
        exchange: str,
        symbol: str,
        ccxt_symbol: str,
        log_cb: Optional[callable] = None,
    ) -> bool:
        if not hasattr(client, "request"):
            self._emit_log(
                log_cb,
                "warn",
                "binance algo order cancellation unavailable",
                {"exchange": exchange, "symbol": symbol, "error": "client.request missing"},
            )
            return False
        try:
            payload = await client.request(
                "openAlgoOrders",
                "fapiPrivate",
                "GET",
                {"symbol": ccxt_symbol},
            )
        except Exception as exc:  # pylint: disable=broad-except
            self._emit_log(
                log_cb,
                "warn",
                "binance openAlgoOrders fetch failed",
                {"exchange": exchange, "symbol": symbol, "error": str(exc)},
            )
            return False
        algo_orders: list[dict[str, Any]]
        if isinstance(payload, list):
            algo_orders = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, Mapping):
            rows = payload.get("orders") or payload.get("data") or payload.get("rows") or []
            algo_orders = [item for item in rows if isinstance(item, dict)]
        else:
            algo_orders = []
        if not algo_orders:
            return True
        canceled = 0
        failed = 0
        for item in algo_orders:
            algo_id = str(
                item.get("algoId")
                or item.get("algoOrderId")
                or item.get("id")
                or item.get("clientAlgoId")
                or ""
            ).strip()
            if not algo_id:
                continue
            try:
                await client.request(
                    "algoOrder",
                    "fapiPrivate",
                    "DELETE",
                    {"algoId": algo_id},
                )
                canceled += 1
            except Exception as exc:  # pylint: disable=broad-except
                failed += 1
                self._emit_log(
                    log_cb,
                    "warn",
                    "binance algo order cancel failed",
                    {"exchange": exchange, "symbol": symbol, "algo_id": algo_id, "error": str(exc)},
                )
        self._emit_log(
            log_cb,
            "precheck",
            "binance algo orders canceled",
            {
                "exchange": exchange,
                "symbol": symbol,
                "count": canceled,
                "failed": failed,
                "method": "algoOrder",
            },
        )
        if failed > 0:
            return False
        try:
            remain_payload = await client.request(
                "openAlgoOrders",
                "fapiPrivate",
                "GET",
                {"symbol": ccxt_symbol},
            )
        except Exception as exc:  # pylint: disable=broad-except
            self._emit_log(
                log_cb,
                "warn",
                "binance openAlgoOrders verify failed",
                {"exchange": exchange, "symbol": symbol, "error": str(exc)},
            )
            return False
        if isinstance(remain_payload, list):
            remain = [item for item in remain_payload if isinstance(item, dict)]
        elif isinstance(remain_payload, Mapping):
            rows = remain_payload.get("orders") or remain_payload.get("data") or remain_payload.get("rows") or []
            remain = [item for item in rows if isinstance(item, dict)]
        else:
            remain = []
        if remain:
            self._emit_log(
                log_cb,
                "warn",
                "binance algo orders remain after cancel",
                {"exchange": exchange, "symbol": symbol, "remaining": len(remain)},
            )
            return False
        return True

    async def _fetch_binance_symbol_settings(
        self,
        client: Any,
        *,
        symbol: str,
        ccxt_symbol: str,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        settings: dict[str, Any] = {}
        if hasattr(client, "fetch_leverage"):
            try:
                leverage_info = await client.fetch_leverage(ccxt_symbol)
            except Exception as exc:  # pylint: disable=broad-except
                self._emit_log(
                    log_cb,
                    "warn",
                    "binance leverage fetch failed",
                    {"exchange": "binance", "symbol": symbol, "error": str(exc)},
                )
            else:
                if isinstance(leverage_info, Mapping):
                    leverage = None
                    for key in ("leverage", "longLeverage", "shortLeverage"):
                        leverage = _safe_float(leverage_info.get(key))
                        if leverage is not None:
                            break
                    if leverage is None:
                        info = leverage_info.get("info") or {}
                        if isinstance(info, Mapping):
                            for key in ("leverage", "longLeverage", "shortLeverage"):
                                leverage = _safe_float(info.get(key))
                                if leverage is not None:
                                    break
                    if leverage is not None:
                        settings["leverage"] = leverage
                    margin_mode = (
                        leverage_info.get("marginMode")
                        or leverage_info.get("margin_mode")
                        or leverage_info.get("marginType")
                    )
                    if not margin_mode:
                        info = leverage_info.get("info") or {}
                        if isinstance(info, Mapping):
                            margin_mode = (
                                info.get("marginMode")
                                or info.get("margin_mode")
                                or info.get("marginType")
                            )
                    if margin_mode:
                        settings["margin_mode"] = str(margin_mode).strip().lower()
        if not hasattr(client, "fetch_positions"):
            return settings
        positions = None
        async def _fetch_positions_once() -> Any:
            try:
                return await client.fetch_positions([ccxt_symbol])
            except Exception:
                return await client.fetch_positions()
        try:
            positions = await _fetch_positions_once()
        except Exception as exc:  # pylint: disable=broad-except
            if _is_binance_time_sync_error(exc) and hasattr(client, "load_time_difference"):
                try:
                    await client.load_time_difference()
                    positions = await _fetch_positions_once()
                except Exception as retry_exc:  # pylint: disable=broad-except
                    self._emit_log(
                        log_cb,
                        "warn",
                        "binance positions fetch failed",
                        {"exchange": "binance", "symbol": symbol, "error": str(retry_exc)},
                    )
                    return settings
            else:
                self._emit_log(
                    log_cb,
                    "warn",
                    "binance positions fetch failed",
                    {"exchange": "binance", "symbol": symbol, "error": str(exc)},
                )
                return settings
        if positions is None:
            self._emit_log(
                log_cb,
                "warn",
                "binance positions fetch failed",
                {"exchange": "binance", "symbol": symbol, "error": "empty_positions_response"},
            )
            return settings
        canonical = normalize_symbol(symbol)
        for pos in positions or []:
            info = pos.get("info") or {}
            pos_symbol = pos.get("symbol") or pos.get("id") or info.get("symbol") or info.get("instId") or ""
            candidate = normalize_symbol(str(pos_symbol))
            if canonical and not _symbol_matches(canonical, candidate):
                continue
            margin_mode, _ = _extract_margin_mode(pos, "binance")
            leverage, _ = _extract_leverage(pos)
            qty = _safe_float(pos.get("contracts"))
            if qty is None:
                qty = _safe_float(pos.get("amount"))
            if qty is None and isinstance(info, dict):
                qty = _safe_float(info.get("positionAmt"))
            has_position = qty is not None and abs(qty) > 0
            if margin_mode or leverage is not None or has_position:
                if margin_mode:
                    settings["margin_mode"] = margin_mode
                if leverage is not None:
                    settings["leverage"] = leverage
                settings["has_position"] = has_position
                settings["position_qty"] = qty
                return settings
        return settings

    async def _set_binance_leverage_precheck(
        self,
        client: Any,
        *,
        symbol: str,
        ccxt_symbol: str,
        target: int,
        log_cb: Optional[callable] = None,
    ) -> tuple[bool, dict[str, Any] | None, Exception | None]:
        last_exc: Exception | None = None
        for attempt in range(1, PRECHECK_RETRIES + 1):
            try:
                await client.set_leverage(target, ccxt_symbol, {})
                return True, None, None
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                if _binance_leverage_noop(exc):
                    return True, None, exc
                if _is_binance_time_sync_error(exc) and hasattr(client, "load_time_difference"):
                    try:
                        await client.load_time_difference()
                    except Exception as sync_exc:  # pylint: disable=broad-except
                        self._emit_log(
                            log_cb,
                            "warn",
                            "binance leverage time sync failed",
                            {"exchange": "binance", "symbol": symbol, "error": str(sync_exc)},
                        )
                refreshed = await self._fetch_binance_symbol_settings(
                    client,
                    symbol=symbol,
                    ccxt_symbol=ccxt_symbol,
                    log_cb=log_cb,
                )
                refreshed_leverage = _safe_float(refreshed.get("leverage"))
                if refreshed_leverage is not None and abs(refreshed_leverage - target) <= 0.05:
                    return True, refreshed, exc
                if attempt >= PRECHECK_RETRIES or not _binance_retryable_leverage_error(exc):
                    return False, refreshed, exc
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
        return False, None, last_exc

    async def _fetch_kucoin_symbol_margin_mode(
        self,
        client: Any,
        *,
        symbol: str,
        ccxt_symbol: str,
        log_cb: Optional[callable] = None,
    ) -> str | None:
        if not hasattr(client, "fetch_positions"):
            return None
        try:
            try:
                positions = await client.fetch_positions([ccxt_symbol])
            except Exception:
                positions = await client.fetch_positions()
        except Exception as exc:  # pylint: disable=broad-except
            self._emit_log(
                log_cb,
                "warn",
                "kucoin positions fetch failed",
                {"exchange": "kucoin", "symbol": symbol, "error": str(exc)},
            )
            return None
        canonical = normalize_symbol(symbol)
        for pos in positions or []:
            info = pos.get("info") or {}
            pos_symbol = pos.get("symbol") or pos.get("id") or info.get("symbol") or info.get("instId") or ""
            candidate = normalize_symbol(str(pos_symbol))
            if canonical and not _symbol_matches(canonical, candidate):
                continue
            mode, _ = _extract_margin_mode(pos, "kucoin")
            if mode:
                return str(mode).lower()
        return None

    async def _ensure_binance_leverage_for_legs(
        self,
        legs: Iterable[Mapping[str, Any]],
        symbol: str,
        *,
        log_cb: Optional[callable] = None,
    ) -> list[str]:
        errors: list[str] = []
        for leg in legs:
            exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
            if exchange != "binance":
                continue
            if leg.get("reduce_only"):
                continue
            client = await self._ensure_client(exchange, errors)
            if client is None:
                continue
            ccxt_symbol = await self._resolve_market_symbol(client, symbol)
            if not ccxt_symbol:
                errors.append(f"{exchange}: symbol unavailable for leverage precheck")
                continue
            current = await self._fetch_binance_symbol_settings(
                client,
                symbol=symbol,
                ccxt_symbol=ccxt_symbol,
                log_cb=log_cb,
            )
            current_margin_mode = str(current.get("margin_mode") or "").strip().lower()
            current_leverage = _safe_float(current.get("leverage"))
            has_position = bool(current.get("has_position"))
            leg_margin_mode = str(leg.get("margin_mode") or "").strip().lower()
            target = int(round(DEFAULT_MANUAL_LEVERAGE))
            margin_ok = bool(leg_margin_mode) and current_margin_mode == leg_margin_mode
            leverage_ok = current_leverage is not None and abs(current_leverage - target) <= 0.05
            if margin_ok and leverage_ok:
                self._emit_log(
                    log_cb,
                    "precheck",
                    "binance margin/leverage already set",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "margin_mode": current_margin_mode,
                        "leverage": current_leverage,
                    },
                )
                continue
            if has_position:
                self._emit_log(
                    log_cb,
                    "warn",
                    "binance margin/leverage mismatch with open position; skipping precheck",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "current_margin_mode": current_margin_mode,
                        "current_leverage": current_leverage,
                        "target_margin_mode": leg_margin_mode,
                        "target_leverage": target,
                    },
                )
                continue
            if not margin_ok or not leverage_ok:
                await self._cancel_open_orders_for_symbol(
                    client,
                    exchange=exchange,
                    symbol=symbol,
                    ccxt_symbol=ccxt_symbol,
                    log_cb=log_cb,
                )
            if leg_margin_mode in ("isolated", "cross"):
                if not margin_ok:
                    try:
                        await client.set_margin_mode(leg_margin_mode, ccxt_symbol)
                        self._emit_log(
                            log_cb,
                            "precheck",
                            "binance margin mode set",
                            {
                                "exchange": exchange,
                                "symbol": symbol,
                                "margin_mode": leg_margin_mode,
                            },
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        if _binance_margin_mode_noop(exc):
                            self._emit_log(
                                log_cb,
                                "precheck",
                                "binance margin mode already set",
                                {
                                    "exchange": exchange,
                                    "symbol": symbol,
                                    "margin_mode": leg_margin_mode,
                                    "error": str(exc),
                                },
                            )
                        elif _binance_margin_mode_blocked(exc):
                            refreshed = await self._fetch_binance_symbol_settings(
                                client,
                                symbol=symbol,
                                ccxt_symbol=ccxt_symbol,
                                log_cb=log_cb,
                            )
                            refreshed_mode = str(refreshed.get("margin_mode") or "").strip().lower()
                            if refreshed_mode == leg_margin_mode:
                                self._emit_log(
                                    log_cb,
                                    "precheck",
                                    "binance margin mode already set",
                                    {
                                        "exchange": exchange,
                                        "symbol": symbol,
                                        "margin_mode": leg_margin_mode,
                                        "error": str(exc),
                                    },
                                )
                            else:
                                canceled = await self._cancel_open_orders_for_symbol(
                                    client,
                                    exchange=exchange,
                                    symbol=symbol,
                                    ccxt_symbol=ccxt_symbol,
                                    log_cb=log_cb,
                                )
                                if not canceled:
                                    errors.append(f"{exchange}: set margin mode failed")
                                    self._emit_log(
                                        log_cb,
                                        "error",
                                        "binance margin mode set failed",
                                        {
                                            "exchange": exchange,
                                            "symbol": symbol,
                                            "margin_mode": leg_margin_mode,
                                            "error": str(exc),
                                        },
                                    )
                                    continue
                                try:
                                    await client.set_margin_mode(leg_margin_mode, ccxt_symbol)
                                    self._emit_log(
                                        log_cb,
                                        "precheck",
                                        "binance margin mode set",
                                        {
                                            "exchange": exchange,
                                            "symbol": symbol,
                                            "margin_mode": leg_margin_mode,
                                            "note": "post_cancel",
                                        },
                                    )
                                except Exception as exc2:  # pylint: disable=broad-except
                                    if _binance_margin_mode_noop(exc2):
                                        self._emit_log(
                                            log_cb,
                                            "precheck",
                                            "binance margin mode already set",
                                            {
                                                "exchange": exchange,
                                                "symbol": symbol,
                                                "margin_mode": leg_margin_mode,
                                                "error": str(exc2),
                                            },
                                        )
                                    else:
                                        refreshed = await self._fetch_binance_symbol_settings(
                                            client,
                                            symbol=symbol,
                                            ccxt_symbol=ccxt_symbol,
                                            log_cb=log_cb,
                                        )
                                        refreshed_mode = str(refreshed.get("margin_mode") or "").strip().lower()
                                        if refreshed_mode == leg_margin_mode:
                                            self._emit_log(
                                                log_cb,
                                                "precheck",
                                                "binance margin mode already set",
                                                {
                                                    "exchange": exchange,
                                                    "symbol": symbol,
                                                    "margin_mode": leg_margin_mode,
                                                    "error": str(exc2),
                                                },
                                            )
                                        else:
                                            errors.append(f"{exchange}: set margin mode failed")
                                            self._emit_log(
                                                log_cb,
                                                "error",
                                                "binance margin mode set failed",
                                                {
                                                    "exchange": exchange,
                                                    "symbol": symbol,
                                                    "margin_mode": leg_margin_mode,
                                                    "error": str(exc2),
                                                },
                                            )
                                            continue
                        else:
                            errors.append(f"{exchange}: set margin mode failed")
                            self._emit_log(
                                log_cb,
                                "error",
                                "binance margin mode set failed",
                                {
                                    "exchange": exchange,
                                    "symbol": symbol,
                                    "margin_mode": leg_margin_mode,
                                    "error": str(exc),
                                },
                            )
                            continue
            if leverage_ok:
                self._emit_log(
                    log_cb,
                    "precheck",
                    "binance leverage already set",
                    {"exchange": exchange, "symbol": symbol, "leverage": current_leverage},
                )
                continue
            note = None
            success = False
            refreshed = None
            exc: Exception | None = None
            success, refreshed, exc = await self._set_binance_leverage_precheck(
                client,
                symbol=symbol,
                ccxt_symbol=ccxt_symbol,
                target=target,
                log_cb=log_cb,
            )
            if success:
                if exc is not None and _binance_leverage_noop(exc):
                    note = "already_set"
                elif refreshed and _safe_float(refreshed.get("leverage")) is not None:
                    note = "verified_after_error"
                payload = {"exchange": exchange, "symbol": symbol, "leverage": target}
                if note:
                    payload["note"] = note
                    if exc is not None:
                        payload["error"] = str(exc)
                self._emit_log(
                    log_cb,
                    "precheck",
                    "binance leverage set",
                    payload,
                )
                continue
            if exc is not None and _binance_margin_mode_blocked(exc):
                await self._cancel_open_orders_for_symbol(
                    client,
                    exchange=exchange,
                    symbol=symbol,
                    ccxt_symbol=ccxt_symbol,
                    log_cb=log_cb,
                )
                success, refreshed, retry_exc = await self._set_binance_leverage_precheck(
                    client,
                    symbol=symbol,
                    ccxt_symbol=ccxt_symbol,
                    target=target,
                    log_cb=log_cb,
                )
                if success:
                    payload = {"exchange": exchange, "symbol": symbol, "leverage": target, "note": "post_cancel"}
                    if refreshed and _safe_float(refreshed.get("leverage")) is not None:
                        payload["verified"] = True
                    if retry_exc is not None and _binance_leverage_noop(retry_exc):
                        payload["error"] = str(retry_exc)
                    self._emit_log(
                        log_cb,
                        "precheck",
                        "binance leverage set",
                        payload,
                    )
                    continue
                exc = retry_exc or exc
            errors.append(f"{exchange}: set leverage failed")
            self._emit_log(
                log_cb,
                "error",
                "binance leverage set failed",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "leverage": target,
                    "error": str(exc) if exc is not None else "unknown_error",
                },
            )
        return errors

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
        allow_liquidity_chunking = bool(payload.get("allow_liquidity_chunking"))
        margin_mode = str(payload.get("margin_mode") or "").strip().lower()
        min_notional_overrides = payload.get("min_notional_overrides") or {}
        if not isinstance(min_notional_overrides, Mapping):
            min_notional_overrides = {}
        min_notional_buffer_pct = _safe_float(payload.get("min_notional_buffer_pct")) or 0.0
        if min_notional_buffer_pct < 0:
            min_notional_buffer_pct = 0.0
        errors: list[str] = []
        warnings: list[str] = []
        position_rows = list(positions or [])
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
        if (
            spread_min_pct is not None
            and spread_max_pct is not None
            and spread_min_pct > spread_max_pct
        ):
            errors.append(
                "spread_min_pct must be less than or equal to spread_max_pct."
            )
        if bool(payload.get("force_chunk_qty")):
            warnings.append(
                "force_chunk_qty is treated as a requested target and remains capped by live liquidity."
            )

        legs: list[dict[str, Any]] = []
        def _position_margin_mode(exchange: str, position_side: str | None) -> str | None:
            exchange_name = normalize_exchange_name(exchange)
            target_side = str(position_side or "").strip().lower()
            for pos in position_rows:
                pos_exchange = normalize_exchange_name(str(pos.get("exchange") or ""))
                if pos_exchange != exchange_name:
                    continue
                qty_value = _safe_float(pos.get("coin_qty"))
                if qty_value is None:
                    qty_value = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
                pos_side = _normalize_position_side(pos.get("side"), qty_value)
                if target_side and pos_side and pos_side != target_side:
                    continue
                mode, _source = _extract_margin_mode(dict(pos), exchange_name)
                if mode in ("isolated", "cross"):
                    return mode
            return None

        def _leg_margin_mode(exchange: str, position_side: str | None = None) -> str | None:
            if margin_mode:
                return margin_mode
            position_mode = _position_margin_mode(exchange, position_side)
            if position_mode:
                return position_mode
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
                    "margin_mode": _leg_margin_mode(long_exchange, "long"),
                },
                {
                    "label": "short",
                    "exchange": short_exchange,
                    "side": "buy",
                    "reduce_only": True,
                    "margin_mode": _leg_margin_mode(short_exchange, "short"),
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
                        "margin_mode": _leg_margin_mode(from_exchange, "long"),
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
                        "margin_mode": _leg_margin_mode(from_exchange, "short"),
                    },
                ]
            else:
                legs = []

        if errors:
            return self._plan_response(payload, legs, errors, warnings, action=action)

        if qty is None and position_rows:
            inferred = self._infer_qty_from_positions(
                symbol,
                position_rows,
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
        liquidity_messages: dict[str, str] = {}
        slippage_messages: dict[str, str] = {}

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
                    liquidity_messages[exchange] = message
            if use_orderbook_check and slip is not None and max_slippage_bps > 0 and slip > max_slippage_bps:
                slippage_messages[exchange] = (
                    f"{exchange}: expected slippage {slip:.2f} bps exceeds max {max_slippage_bps:.2f}"
                )

        suggestion = suggest_expensive_leg(
            long_exchange,
            short_exchange,
            fee_table=self._fees,
            liquidity=liquidity_map,
        )
        smart_maker_first = action == "enter" and str(payload.get("mode") or "") == "smart-enter"
        _, planned_primary, planned_hedge = self._resolve_primary_hedge_legs(
            explicit=payload.get("expensive_leg"),
            plan={"suggested_expensive_leg": suggestion},
            legs=legs,
        )
        min_chunk_candidates = [
            val.get("min_qty_required")
            for val in market_constraints.values()
            if val.get("min_qty_required")
        ]
        min_chunk_qty = max(min_chunk_candidates) if min_chunk_candidates else None
        if smart_maker_first and not payload.get("expensive_leg") and planned_primary and planned_hedge:
            hedge_cap = max_qty_by_exchange.get(planned_hedge["exchange"])
            alternate_hedge_cap = max_qty_by_exchange.get(planned_primary["exchange"])
            hedge_ready = hedge_cap is not None and (
                not min_chunk_qty or float(hedge_cap) >= float(min_chunk_qty)
            )
            alternate_ready = alternate_hedge_cap is not None and (
                not min_chunk_qty or float(alternate_hedge_cap) >= float(min_chunk_qty)
            )
            if max_slippage_bps > 0 and not hedge_ready and alternate_ready:
                suggestion = dict(suggestion)
                suggestion["suggested_leg"] = planned_hedge.get("label")
                suggestion["reason"] = "hedge_liquidity_guard"
                _, planned_primary, planned_hedge = self._resolve_primary_hedge_legs(
                    explicit=None,
                    plan={"suggested_expensive_leg": suggestion},
                    legs=legs,
                )

        primary_exchange = planned_primary.get("exchange") if planned_primary else None
        hedge_exchange = planned_hedge.get("exchange") if planned_hedge else None
        for exchange, message in liquidity_messages.items():
            if smart_maker_first and exchange == primary_exchange:
                continue
            if is_dry_run or allow_liquidity_chunking:
                warnings.append(message)
            else:
                errors.append(message)
        for exchange, message in slippage_messages.items():
            if smart_maker_first and exchange == primary_exchange:
                continue
            warnings.append(message)
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
        if smart_maker_first and hedge_exchange:
            recommended_qty = max_qty_by_exchange.get(hedge_exchange)
        else:
            max_qty_candidates = [val for val in max_qty_by_exchange.values() if val is not None]
            recommended_qty = min(max_qty_candidates) if max_qty_candidates else None
        recommended_notional = None
        if recommended_qty and short_stats and short_stats.mid:
            recommended_notional = recommended_qty * short_stats.mid
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
            "execution_liquidity": {
                "primary_maker": {
                    "exchange": primary_exchange,
                    "ready": bool(
                        planned_primary
                        and stats_by_exchange.get(str(primary_exchange))
                        and stats_by_exchange[str(primary_exchange)].best_bid
                        and stats_by_exchange[str(primary_exchange)].best_ask
                    ),
                    "immediate_taker_max_qty": max_qty_by_exchange.get(str(primary_exchange)),
                    "taker_depth_blocking": False if smart_maker_first else True,
                },
                "hedge_taker": {
                    "exchange": hedge_exchange,
                    "ready": bool(
                        planned_hedge
                        and (
                            max_slippage_bps <= 0
                            or (
                                max_qty_by_exchange.get(str(hedge_exchange)) is not None
                                and (
                                    not min_chunk_qty
                                    or float(max_qty_by_exchange[str(hedge_exchange)]) >= float(min_chunk_qty)
                                )
                            )
                        )
                    ),
                    "max_qty_within_slippage": max_qty_by_exchange.get(str(hedge_exchange)),
                    "max_slippage_bps": max_slippage_bps,
                },
            },
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
        if mode not in ("smart-exit", "fast-exit", "smart-enter", "fast-enter", "smart-roll"):
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
        if mode in ("smart-exit", "fast-exit", "smart-enter", "fast-enter", "smart-roll"):
            action = plan.get("action")
            if action == "exit" and mode in ("smart-exit", "fast-exit"):
                if mode == "smart-exit":
                    return await self._execute_smart_exit(plan, payload, log_cb=log_cb)
                return await self._execute_fast_exit(plan, payload, log_cb=log_cb)
            if action == "enter" and mode in ("smart-enter", "fast-enter"):
                if mode == "smart-enter":
                    return await self._execute_smart_enter(plan, payload, log_cb=log_cb)
                return await self._execute_fast_enter(plan, payload, log_cb=log_cb)
            if action == "roll" and mode == "smart-roll":
                return await self._execute_smart_enter(
                    plan,
                    payload,
                    mode_label="smart-roll",
                    log_cb=log_cb,
                )
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": mode,
                "actions": actions,
                "errors": [f"{mode} is not supported for action {action}."],
                "warnings": plan.get("warnings") or [],
                "risk_flags": self._collect_risk_flags(actions, plan.get("warnings") or []),
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
                "risk_flags": self._collect_risk_flags(actions, plan.get("warnings") or []),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        if mode == "limit-first-expensive":
            _, limit_leg, market_leg = self._resolve_primary_hedge_legs(
                explicit=expensive_leg,
                plan=plan,
                legs=legs,
            )
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
            "risk_flags": self._collect_risk_flags(actions, plan.get("warnings") or []),
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
        exit_allow_flip = bool(payload.get("exit_allow_flip"))
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        timeout = _resolve_timeout(payload, 10)
        reprice_sec = max(3.0, _safe_float(payload.get("reprice_sec")) or 2.0)
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 60)
        trigger_wait_sec = _trigger_wait_sec(payload, max_runtime_sec)
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

        if exit_allow_flip:
            legs = [dict(leg, reduce_only=False) for leg in legs]
        expensive_label, primary_leg, hedge_leg = self._resolve_primary_hedge_legs(
            explicit=payload.get("expensive_leg"),
            plan=plan,
            legs=legs,
        )
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

        positions, pos_errors = await self._fetch_positions_with_retry(
            exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
            symbol=symbol,
            log_cb=log_cb,
        )
        if pos_errors:
            if exit_allow_flip:
                warnings.extend(pos_errors)
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions fetch failed; continuing (exit allow flip)",
                    {"stage": "start", "errors": pos_errors},
                )
            else:
                errors.extend(pos_errors)
                self._emit_log(
                    log_cb,
                    "error",
                    "positions fetch failed; stopping",
                    {"stage": "start", "errors": pos_errors},
                )
                return {
                    "dry_run": False,
                    "action": plan.get("action"),
                    "symbol": symbol,
                    "qty": qty,
                    "mode": "smart-exit",
                    "actions": actions,
                    "errors": errors,
                    "warnings": warnings,
                    "risk_flags": self._collect_risk_flags(actions, warnings),
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
        exchange_list = [
            normalize_exchange_name(primary_leg["exchange"]),
            normalize_exchange_name(hedge_leg["exchange"]),
        ]
        counts: dict[str, int] = {}
        for entry in positions:
            exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
            if not exchange:
                continue
            counts[exchange] = counts.get(exchange, 0) + 1
        self._emit_log(
            log_cb,
            "positions",
            "positions snapshot (start)",
            {
                "stage": "start",
                "symbol": symbol,
                "exchanges": exchange_list,
                "positions": positions,
                "counts": counts,
                "errors": pos_errors,
                "sources": {exchange: "rest" for exchange in exchange_list if exchange},
            },
        )
        if exit_allow_flip:
            primary_side = _position_side_for_leg(primary_leg)
            hedge_side = _position_side_for_leg(hedge_leg)
        else:
            primary_side = _exit_position_side(primary_leg)
            hedge_side = _exit_position_side(hedge_leg)
        primary_pos_qty = _safe_float(self._sum_position_qty(
            positions,
            exchange=primary_leg["exchange"],
            side=primary_side,
            symbol=symbol,
        )) or 0.0
        hedge_pos_qty = _safe_float(self._sum_position_qty(
            positions,
            exchange=hedge_leg["exchange"],
            side=hedge_side,
            symbol=symbol,
        )) or 0.0
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
        if not exit_allow_flip and primary_pos_qty > 0 and hedge_pos_qty > 0:
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
        hedge_filled_total = 0.0
        pending_order_ids: set[str] = set()
        pending_ws_rest_checked_at: float | None = None
        pending_ws_rest_interval_sec = 3.0
        cancel_pause_sec = 1.0
        ws_missing_grace_sec = 3.0
        ws_missing_rest_interval_sec = 3.0
        active_ws_missing_since: float | None = None
        active_ws_rest_checked_at: float | None = None
        active_ws_missing_open = False
        active_ws_missing_open = False

        def _vlog(event: str, message: str, data: Mapping[str, Any] | None = None) -> None:
            if verbose_logs:
                self._emit_log(log_cb, event, message, data)

        async def _pause_after_cancel(reason: str) -> None:
            if cancel_pause_sec <= 0:
                return
            _vlog(
                "wait",
                "post-cancel pause",
                {"exchange": primary_leg.get("exchange"), "reason": reason, "pause_sec": cancel_pause_sec},
            )
            await asyncio.sleep(cancel_pause_sec)

        async def _ensure_active_order_visible(reason: str) -> bool:
            nonlocal active_ws_missing_since, active_ws_rest_checked_at, active_ws_missing_open
            nonlocal active_order_id, active_price, active_qty, active_filled, active_since
            if not active_order_id:
                active_ws_missing_since = None
                active_ws_missing_open = False
                return True
            if not self._ws_orders_live(primary_leg["exchange"]):
                return True
            info = self._ws_order_info(primary_leg["exchange"], active_order_id)
            if info:
                active_ws_missing_since = None
                active_ws_missing_open = False
                return True
            now = time.time()
            if active_ws_missing_since is None:
                active_ws_missing_since = now
            elapsed = now - active_ws_missing_since
            if elapsed < ws_missing_grace_sec:
                _vlog(
                    "wait",
                    "primary ws order update missing; waiting",
                    {
                        "exchange": primary_leg.get("exchange"),
                        "reason": reason,
                        "elapsed_sec": round(elapsed, 2),
                        "grace_sec": ws_missing_grace_sec,
                    },
                )
                await asyncio.sleep(max(0.2, reprice_sec))
                return False
            if active_ws_rest_checked_at and (now - active_ws_rest_checked_at) < ws_missing_rest_interval_sec:
                await asyncio.sleep(max(0.2, reprice_sec))
                return False
            active_ws_rest_checked_at = now
            status = await self._fetch_order_status(
                primary_leg,
                symbol,
                active_order_id,
                expected_qty=active_qty or order_qty_map.get(active_order_id),
                allow_trades_fallback=False,
            )
            if status.get("status") == "error":
                _vlog(
                    "warn",
                    "primary rest order status failed",
                    {
                        "exchange": primary_leg["exchange"],
                        "order_id": active_order_id,
                        "error": status.get("error"),
                    },
                )
                await asyncio.sleep(max(0.2, reprice_sec))
                return False
            await _apply_primary_fill(
                active_order_id,
                status.get("filled_qty"),
                status=status,
                reason="ws_missing_rest",
            )
            state = str(status.get("status") or "").lower()
            if state in ("canceled", "cancelled", "closed", "filled", "finished"):
                pending_order_ids.discard(active_order_id)
                active_order_id = None
                active_price = None
                active_qty = None
                active_filled = 0.0
                active_since = None
                active_ws_missing_since = None
                active_ws_missing_open = False
                return True
            _vlog(
                "wait",
                "primary ws order update missing; rest confirmed open",
                {
                    "exchange": primary_leg.get("exchange"),
                    "order_id": active_order_id,
                    "status": status.get("status"),
                    "filled_qty": status.get("filled_qty"),
                },
            )
            active_ws_missing_open = True
            return True

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
            pending_order_ids.discard("")  # defensive: stale placeholder ids
            pending_order_ids.discard(None)  # type: ignore[arg-type]
            order_ids = [order_id for order_id in pending_order_ids if order_id]
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
            return total_delta, True

        async def _sync_primary_fills(
            reason: str,
            *,
            delay: float | None = None,
            include_active: bool = True,
            force_rest: bool = False,
        ) -> float:
            nonlocal active_filled, active_order_id, active_price, active_qty, active_since
            if delay:
                await asyncio.sleep(delay)
            ws_delta, used_ws = await _sync_primary_from_orders(reason)
            if not force_rest and await self._ensure_ws_orders_healthy(primary_leg["exchange"], reason=reason, log_cb=log_cb):
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
            pending_order_ids.discard("")  # defensive: stale placeholder ids
            pending_order_ids.discard(None)  # type: ignore[arg-type]
            order_ids = [order_id for order_id in pending_order_ids if order_id]
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
            nonlocal pending_hedge_qty, hedge_failed, hedge_filled_total, warnings
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
            hedge_qty = _pending_hedge_order_qty(
                hedge_qty,
                min_qty_required=min_hedge_qty,
                amount_step=hedge_amount_step,
            )
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
                hedge_adverse_ticks=_safe_float(payload.get("hedge_adverse_ticks")),
                hedge_reprice_min_sec=hedge_reprice_min_sec,
                payload=payload,
                min_qty_required=min_hedge_qty,
                log_cb=log_cb,
            )
            actions.append(hedge_result)
            self._emit_log(log_cb, "result", "hedge result", hedge_result)
            hedge_filled_total += _safe_float(hedge_result.get("filled_qty")) or 0.0
            if hedge_result.get("status") == "error":
                if primary_filled_total > hedge_filled_total:
                    hedge_result["risk_state"] = "partial_fill_exposure"
                    warnings.append("partial_fill_exposure")
                errors.append(
                    f"hedge failed on {hedge_leg['exchange']}: {hedge_result.get('error') or 'unknown_error'}"
                )
                hedge_failed = True
                return
            pending_qty = _safe_float(hedge_result.get("pending_qty"))
            if pending_qty:
                if min_hedge_qty and pending_qty < min_hedge_qty:
                    _vlog(
                        "wait",
                        "hedge remainder below minimum; skipping",
                        {"pending_qty": pending_qty, "min_qty": min_hedge_qty, "reason": reason},
                    )
                else:
                    pending_hedge_qty += pending_qty
                    _vlog(
                        "wait",
                        "hedge remainder pending; re-queued",
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
                if pending_hedge_qty > 0:
                    await _hedge_pending("stop_cancel")
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
                if not actions and (time.time() - started_at) >= trigger_wait_sec:
                    warnings.append("condition_not_met")
                    self._emit_log(
                        log_cb,
                        "result",
                        "spread condition not met; releasing execution worker",
                        {
                            "spread_pct": spread_val,
                            "spread_min_pct": spread_min_pct,
                            "spread_max_pct": spread_max_pct,
                            "trigger_wait_sec": trigger_wait_sec,
                        },
                    )
                    break
                if active_order_id:
                    await _sync_primary_fills("spread_cancel")
                    if active_order_id:
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
                    await _pause_after_cancel("post_spread_cancel")
                self._emit_log(log_cb, "wait", f"spread {spread_val:.4f} out of range; waiting")
                if pending_hedge_qty > 0 and active_order_id is None:
                    await _hedge_pending("spread_wait")
                    if hedge_failed:
                        break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            await _sync_primary_fills("loop")
            if not await _ensure_active_order_visible("loop"):
                continue
            if active_order_id is None and pending_hedge_qty > 0:
                await _hedge_pending("post_primary")
                if hedge_failed:
                    break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            if pending_order_ids:
                await _sync_primary_fills("pre_chunk", include_active=False)
                if pending_order_ids:
                    now = time.time()
                    if pending_ws_rest_checked_at is None or (now - pending_ws_rest_checked_at) >= pending_ws_rest_interval_sec:
                        pending_ws_rest_checked_at = now
                        await _sync_primary_fills("pending_rest", include_active=False, force_rest=True)
                        if not pending_order_ids:
                            continue
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
            limiting_exchange = None
            if max_chunk is not None:
                limiting_exchange = next(
                    (
                        exchange
                        for exchange, value in max_qty_by_exchange.items()
                        if value is not None and abs(float(value) - float(max_chunk)) <= 1e-9
                    ),
                    None,
                )
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
            max_chunk_for_choice, auto_chunk_notional_cap = _cap_auto_chunk_by_notional(
                requested_qty=requested,
                chunk_notional=chunk_notional,
                max_chunk=max_chunk if max_slippage_bps > 0 else None,
                mid_price=mid_price,
                legs=[primary_leg, hedge_leg],
            )
            chunk, chunk_warnings = _choose_chunk_qty(
                remaining=remaining,
                requested_qty=requested,
                min_chunk=min_chunk_qty,
                max_chunk=max_chunk_for_choice,
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
                    "max_chunk_for_choice": max_chunk_for_choice,
                    "auto_chunk_notional_cap": auto_chunk_notional_cap,
                    "max_qty_by_exchange": dict(max_qty_by_exchange),
                    "limiting_exchange": limiting_exchange,
                    "max_slippage_bps": max_slippage_bps,
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
                    if active_ws_missing_open and active_price != limit_price:
                        dev_bps = _price_deviation_bps(active_price, limit_price)
                        threshold = _safe_float(payload.get("max_limit_deviation_bps")) or 30.0
                        if dev_bps is not None and dev_bps < threshold:
                            self._emit_log(
                                log_cb,
                                "wait",
                                "primary ws order update missing; keeping order (deviation below threshold)",
                                {
                                    "exchange": primary_leg["exchange"],
                                    "order_id": active_order_id,
                                    "current_price": active_price,
                                    "target_price": limit_price,
                                    "deviation_bps": round(dev_bps, 2),
                                    "threshold_bps": threshold,
                                },
                            )
                            await asyncio.sleep(max(0.2, reprice_sec))
                            continue
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
                    if active_order_id:
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
                    await _pause_after_cancel("post_reprice_cancel")
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue

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
                active_ws_missing_since = None
                active_ws_rest_checked_at = None
                active_ws_missing_open = False
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
                    if active_order_id:
                        await self._cancel_order(primary_leg, symbol, active_order_id)
                        pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_timeout_cancel", delay=0.2, include_active=False)
                    await _pause_after_cancel("post_timeout_cancel")
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

        if stopped_by_user and not self._stop_force_finalize():
            self._emit_log(
                log_cb,
                "warn",
                "stop requested; skipping final reconcile",
                {"remaining": remaining},
            )
        elif primary_leg and hedge_leg and "condition_not_met" not in warnings:
            use_observed = False
            post_positions, post_errors = await self._fetch_positions_for_symbol(
                exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
                symbol=symbol,
                allow_ws=True,
                contract_sizes=contract_sizes,
            )
            if post_errors:
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions fetch failed; retrying",
                    {"stage": "final", "errors": post_errors},
                )
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
                retry_positions, retry_errors = await self._fetch_positions_for_symbol(
                    exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
                    symbol=symbol,
                    allow_ws=False,
                    contract_sizes=contract_sizes,
                )
                if retry_errors:
                    post_positions = retry_positions
                    post_errors = post_errors + retry_errors
                    warnings.extend(post_errors)
                    warnings.append("positions fetch failed; using observed fills for final reconcile")
                    use_observed = True
                else:
                    post_positions = retry_positions
                    post_errors = []
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
            self._emit_log(
                log_cb,
                "positions",
                "positions snapshot (end)",
                {
                    "stage": "end",
                    "symbol": symbol,
                    "exchanges": exchange_list,
                    "positions": post_positions,
                    "counts": counts,
                    "errors": post_errors,
                    "sources": sources,
                },
            )
            def _reconcile_price_for_exchange(exchange: str) -> float | None:
                for pos in post_positions:
                    pos_exchange = normalize_exchange_name(str(pos.get("exchange") or ""))
                    if pos_exchange != normalize_exchange_name(exchange):
                        continue
                    pos_symbol = str(pos.get("symbol") or pos.get("symbol_normalized") or "")
                    if not _symbol_matches(symbol, pos_symbol):
                        continue
                    mark = _safe_float(pos.get("mark_price"))
                    if mark is None:
                        mark = _safe_float(pos.get("entry_price"))
                    if mark and mark > 0:
                        return float(mark)
                return None
            if use_observed:
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions unavailable; using observed fills for final reconcile",
                    {
                        "primary_filled_total": primary_filled_total,
                        "hedge_filled_total": hedge_filled_total,
                    },
                )
                primary_delta = primary_filled_total
                hedge_delta = hedge_filled_total
            else:
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
                primary_delta = _position_delta_for_leg(primary_pos_start, primary_current, primary_leg)
                hedge_delta = _position_delta_for_leg(hedge_pos_start, hedge_current, hedge_leg)
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
                    reconcile_price = _reconcile_price_for_exchange(leg.get("exchange"))
                    reconcile_notional = (
                        qty_needed * reconcile_price if reconcile_price and reconcile_price > 0 else None
                    )
                    if threshold and qty_needed < threshold:
                        _vlog(
                            "wait",
                            "final imbalance below fallback threshold",
                            {"imbalance": imbalance, "min_qty": threshold},
                        )
                    elif self._auto_exit_final_reconcile_blocked(
                        payload,
                        leg.get("exchange"),
                        notional_usd=reconcile_notional,
                        primary_delta=primary_delta,
                        hedge_delta=hedge_delta,
                        primary_filled_total=primary_filled_total,
                        hedge_filled_total=hedge_filled_total,
                    ):
                        warnings.append(
                            f"{leg['exchange']}: final reconcile market skipped by auto-exit tier guard"
                        )
                        self._emit_log(
                            log_cb,
                            "warn",
                            "final reconcile market skipped by auto-exit tier guard",
                            {
                                "exchange": leg.get("exchange"),
                                "qty": qty_needed,
                                "venue_tier": venue_liquidity_tier(leg.get("exchange")),
                                "market_notional_est": reconcile_notional,
                            },
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
            if not stopped_by_user or self._stop_force_finalize():
                await self._finalize_exit_dust(
                    symbol=symbol,
                    legs=[primary_leg, hedge_leg],
                    start_qty_by_exchange={
                        primary_leg["exchange"]: primary_pos_start,
                        hedge_leg["exchange"]: hedge_pos_start,
                    },
                    requested_exit_qty=qty,
                    constraints=constraints,
                    payload=payload,
                    actions=actions,
                    warnings=warnings,
                    log_cb=log_cb,
                )

        return {
            "dry_run": False,
            "action": plan.get("action"),
            "symbol": symbol,
            "qty": qty,
            "mode": "smart-exit",
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": warnings,
            "risk_flags": self._collect_risk_flags(actions, warnings),
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
        exit_allow_flip = bool(payload.get("exit_allow_flip"))
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        reprice_sec = _safe_float(payload.get("reprice_sec")) or 0.5
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 20)
        trigger_wait_sec = _trigger_wait_sec(payload, max_runtime_sec)
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

        cancel_pause_sec = 1.0
        ws_fresh_sec = 1.0
        awaiting_ws_update = False
        if exit_allow_flip:
            legs = [dict(leg, reduce_only=False) for leg in legs]
        _, primary_leg, hedge_leg = self._resolve_primary_hedge_legs(
            explicit=payload.get("expensive_leg"),
            plan=plan,
            legs=legs,
        )
        if primary_leg and hedge_leg:
            legs = [primary_leg, hedge_leg]
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])
        remaining = qty
        started_at = time.time()
        last_book_ts: dict[str, float | None] = {leg["exchange"]: None for leg in legs}
        exchange_list = [leg["exchange"] for leg in legs]
        observed_fills: dict[str, float] = {leg["exchange"]: 0.0 for leg in legs}
        observed_fills: dict[str, float] = {leg["exchange"]: 0.0 for leg in legs}
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
        start_positions, start_errors = await self._fetch_positions_with_retry(
            exchanges=exchange_list,
            symbol=symbol,
            log_cb=log_cb,
        )
        emit_positions_snapshot("start", start_positions, start_errors)
        if start_errors:
            if exit_allow_flip:
                warnings.extend(start_errors)
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions fetch failed; continuing (exit allow flip)",
                    {"stage": "start", "errors": start_errors},
                )
            else:
                errors.extend(start_errors)
                self._emit_log(
                    log_cb,
                    "error",
                    "positions fetch failed; stopping",
                    {"stage": "start", "errors": start_errors},
                )
                return {
                    "dry_run": False,
                    "action": plan.get("action"),
                    "symbol": symbol,
                    "qty": qty,
                    "mode": "fast-exit",
                    "actions": actions,
                    "errors": errors,
                    "warnings": warnings,
                    "remaining_qty": remaining,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
        start_qty_by_exchange = {
            leg["exchange"]: (
                _safe_float(
                    self._sum_position_qty(
                        start_positions,
                        exchange=leg["exchange"],
                        side=_exit_position_side(leg),
                        symbol=symbol,
                    )
                )
                or 0.0
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
                    if not actions and (time.time() - started_at) >= trigger_wait_sec:
                        warnings.append("condition_not_met")
                        self._emit_log(
                            log_cb,
                            "result",
                            "spread condition not met; releasing execution worker",
                            {
                                "spread_pct": spread_val,
                                "spread_min_pct": spread_min_pct,
                                "spread_max_pct": spread_max_pct,
                                "trigger_wait_sec": trigger_wait_sec,
                            },
                        )
                        break
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
                max_chunk_for_choice, _auto_chunk_notional_cap = _cap_auto_chunk_by_notional(
                    requested_qty=requested,
                    chunk_notional=chunk_notional,
                    max_chunk=max_chunk if max_slippage_bps > 0 else None,
                    mid_price=mid_price,
                    legs=legs,
                )
                chunk, chunk_warnings = _choose_chunk_qty(
                    remaining=remaining,
                    requested_qty=requested,
                    min_chunk=min_chunk_qty,
                    max_chunk=max_chunk_for_choice,
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
                for exchange, filled_qty in filled_by_exchange.items():
                    observed_fills[exchange] = observed_fills.get(exchange, 0.0) + filled_qty
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
            use_observed = False
            end_positions, end_errors = await self._fetch_positions_for_symbol(
                exchanges=exchange_list,
                symbol=symbol,
                allow_ws=False,
                contract_sizes=contract_sizes,
            )
            if end_errors:
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions fetch failed; retrying",
                    {"stage": "final", "errors": end_errors},
                )
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
                retry_positions, retry_errors = await self._fetch_positions_for_symbol(
                    exchanges=exchange_list,
                    symbol=symbol,
                    allow_ws=False,
                    contract_sizes=contract_sizes,
                )
                if retry_errors:
                    end_positions = retry_positions
                    end_errors = end_errors + retry_errors
                    warnings.extend(end_errors)
                    warnings.append("positions fetch failed; using observed fills for final reconcile")
                    use_observed = True
                else:
                    end_positions = retry_positions
                    end_errors = []
            emit_positions_snapshot("end", end_positions, end_errors)
            if stopped_by_user and not self._stop_force_finalize():
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
            if stopped_by_user and self._stop_force_finalize():
                self._emit_log(
                    log_cb,
                    "warn",
                    "stop requested; forcing final reconcile",
                    {"remaining": remaining},
                )
            if len(legs) >= 2 and "condition_not_met" not in warnings:
                deltas: dict[str, float] = {}
                for leg in legs:
                    exchange = leg["exchange"]
                    if use_observed:
                        delta = observed_fills.get(exchange, 0.0)
                    else:
                        end_qty = (
                            _safe_float(
                                self._sum_position_qty(
                                    end_positions,
                                    exchange=exchange,
                                    side=_exit_position_side(leg),
                                    symbol=symbol,
                                )
                            )
                            or 0.0
                        )
                        start_qty = _safe_float(start_qty_by_exchange.get(exchange)) or 0.0
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
                await self._finalize_exit_dust(
                    symbol=symbol,
                    legs=legs,
                    start_qty_by_exchange=start_qty_by_exchange,
                    requested_exit_qty=qty,
                    constraints=constraints,
                    payload=payload,
                    actions=actions,
                    warnings=warnings,
                    log_cb=log_cb,
                )

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
            "risk_flags": self._collect_risk_flags(actions, warnings),
            "remaining_qty": remaining,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _execute_smart_enter(
        self,
        plan: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        mode_label: str = "smart-enter",
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        legs = list(plan.get("legs") or [])
        symbol = str(plan.get("symbol") or "")
        qty = _safe_float(plan.get("qty")) or 0.0
        spread_min_pct = _safe_float(payload.get("spread_min_pct"))
        spread_max_pct = _safe_float(payload.get("spread_max_pct"))
        max_slippage_bps = _safe_float(payload.get("max_slippage_bps")) or 0.0
        timeout = _resolve_timeout(payload, 10)
        reprice_sec = max(3.0, _safe_float(payload.get("reprice_sec")) or 2.0)
        max_runtime_sec = int(_safe_float(payload.get("max_runtime_sec")) or 60)
        trigger_wait_sec = _trigger_wait_sec(payload, max_runtime_sec)
        limit_offset_bps = _safe_float(payload.get("limit_offset_bps")) or 0.0
        limit_offset_ticks = int(_safe_float(payload.get("limit_offset_ticks")) or 0)
        hedge_order_type = str(payload.get("hedge_order_type") or "market").lower()
        hedge_offset_bps = _safe_float(payload.get("hedge_offset_bps")) or 2.0
        hedge_offset_ticks = int(_safe_float(payload.get("hedge_offset_ticks")) or 0)
        hedge_limit_mode = str(payload.get("hedge_limit_mode") or "passive").lower()
        hedge_mode_safety_override = False
        if plan.get("action") == "enter" and hedge_order_type == "limit" and hedge_limit_mode != "aggressive":
            hedge_limit_mode = "aggressive"
            hedge_mode_safety_override = True
        hedge_favorable_bps = _safe_float(payload.get("hedge_favorable_bps")) or 2.0
        raw_adverse_bps = _safe_float(payload.get("hedge_adverse_bps"))
        hedge_adverse_bps = 10.0 if raw_adverse_bps is None else raw_adverse_bps
        hedge_reprice_min_sec = _safe_float(payload.get("hedge_reprice_min_sec")) or 2.0
        fallback_to_market = False
        verbose_logs = bool(payload.get("verbose_logs", True))

        expensive_label, primary_leg, hedge_leg = self._resolve_primary_hedge_legs(
            explicit=payload.get("expensive_leg"),
            plan=plan,
            legs=legs,
        )
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = list(plan.get("warnings") or [])
        if hedge_mode_safety_override:
            warnings.append("smart-enter hedge upgraded to aggressive limit for bounded unhedged exposure")

        mode_label = mode_label or "smart-enter"

        self._emit_log(
            log_cb,
            "start",
            f"manual {mode_label}",
            {
                "action": plan.get("action"),
                "mode": mode_label,
                "symbol": symbol,
                "qty": qty,
                "primary": primary_leg,
                "hedge": hedge_leg,
                "spread_pct": plan.get("spread_pct"),
            },
        )

        if not primary_leg or not hedge_leg:
            errors.append(f"Unable to resolve primary/hedge legs for {mode_label}.")
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": mode_label,
                "actions": actions,
                "errors": errors,
                "warnings": warnings,
                "risk_flags": self._collect_risk_flags(actions, warnings),
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

        primary_side = _position_side_for_leg(primary_leg)
        hedge_side = _position_side_for_leg(hedge_leg)
        positions, pos_errors = await self._fetch_positions_with_retry(
            exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
            symbol=symbol,
            log_cb=log_cb,
        )
        if pos_errors:
            errors.extend(pos_errors)
            self._emit_log(
                log_cb,
                "error",
                "positions fetch failed; stopping",
                {"stage": "start", "errors": pos_errors},
            )
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": mode_label,
                "actions": actions,
                "errors": errors,
                "warnings": warnings,
                "risk_flags": self._collect_risk_flags(actions, warnings),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        exchange_list = [
            normalize_exchange_name(primary_leg["exchange"]),
            normalize_exchange_name(hedge_leg["exchange"]),
        ]
        counts: dict[str, int] = {}
        for entry in positions:
            exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
            if not exchange:
                continue
            counts[exchange] = counts.get(exchange, 0) + 1
        self._emit_log(
            log_cb,
            "positions",
            "positions snapshot (start)",
            {
                "stage": "start",
                "symbol": symbol,
                "exchanges": exchange_list,
                "positions": positions,
                "counts": counts,
                "errors": pos_errors,
                "sources": {exchange: "rest" for exchange in exchange_list if exchange},
            },
        )
        primary_pos_start = _safe_float(self._sum_position_qty(
            positions,
            exchange=primary_leg["exchange"],
            side=primary_side,
            symbol=symbol,
        )) or 0.0
        hedge_pos_start = _safe_float(self._sum_position_qty(
            positions,
            exchange=hedge_leg["exchange"],
            side=hedge_side,
            symbol=symbol,
        )) or 0.0
        # Smart-enter qty is an incremental target relative to start snapshot.
        # Absolute safety caps must use start+qty, not qty alone.
        primary_target_abs = max(0.0, primary_pos_start + qty)
        hedge_target_abs = max(0.0, hedge_pos_start + qty)

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
        hedge_filled_total = 0.0
        pending_order_ids: set[str] = set()
        cancel_pause_sec = 1.0
        ws_fresh_sec = 1.0
        awaiting_ws_update = False
        ws_missing_grace_sec = 3.0
        ws_missing_rest_interval_sec = 3.0
        active_ws_missing_since: float | None = None
        active_ws_rest_checked_at: float | None = None
        pending_ws_rest_checked_at: float | None = None
        pending_ws_rest_interval_sec = 3.0

        def _vlog(event: str, message: str, data: Mapping[str, Any] | None = None) -> None:
            if verbose_logs:
                self._emit_log(log_cb, event, message, data)

        async def _pause_after_cancel(reason: str) -> None:
            nonlocal awaiting_ws_update
            awaiting_ws_update = True
            if cancel_pause_sec > 0:
                _vlog(
                    "wait",
                    "post-cancel pause",
                    {"exchange": primary_leg.get("exchange"), "reason": reason, "pause_sec": cancel_pause_sec},
                )
                await asyncio.sleep(cancel_pause_sec)

        async def _require_fresh_ws_update(reason: str) -> bool:
            nonlocal awaiting_ws_update
            if not awaiting_ws_update:
                return True
            if not self._ws_orders_live(primary_leg["exchange"]):
                awaiting_ws_update = False
                return True
            snapshot = self._ws_orders.health_snapshot(primary_leg["exchange"])
            last_order_sec = _safe_float(snapshot.get("last_order_sec"))
            if last_order_sec is not None and last_order_sec <= ws_fresh_sec:
                awaiting_ws_update = False
                return True
            _vlog(
                "wait",
                "waiting for fresh ws order update",
                {
                    "exchange": primary_leg.get("exchange"),
                    "reason": reason,
                    "last_order_sec": last_order_sec,
                    "fresh_sec": ws_fresh_sec,
                },
            )
            await asyncio.sleep(max(0.2, reprice_sec))
            return False

        async def _ensure_active_order_visible(reason: str) -> bool:
            nonlocal active_ws_missing_since, active_ws_rest_checked_at, active_ws_missing_open
            nonlocal active_order_id, active_price, active_qty, active_filled, active_since
            if not active_order_id:
                active_ws_missing_since = None
                active_ws_missing_open = False
                return True
            if not self._ws_orders_live(primary_leg["exchange"]):
                return True
            info = self._ws_order_info(primary_leg["exchange"], active_order_id)
            if info:
                active_ws_missing_since = None
                active_ws_missing_open = False
                return True
            now = time.time()
            if active_ws_missing_since is None:
                active_ws_missing_since = now
            elapsed = now - active_ws_missing_since
            if elapsed < ws_missing_grace_sec:
                _vlog(
                    "wait",
                    "primary ws order update missing; waiting",
                    {
                        "exchange": primary_leg.get("exchange"),
                        "reason": reason,
                        "elapsed_sec": round(elapsed, 2),
                        "grace_sec": ws_missing_grace_sec,
                    },
                )
                await asyncio.sleep(max(0.2, reprice_sec))
                return False
            if active_ws_rest_checked_at and (now - active_ws_rest_checked_at) < ws_missing_rest_interval_sec:
                await asyncio.sleep(max(0.2, reprice_sec))
                return False
            active_ws_rest_checked_at = now
            status = await self._fetch_order_status(
                primary_leg,
                symbol,
                active_order_id,
                expected_qty=active_qty or order_qty_map.get(active_order_id),
                allow_trades_fallback=False,
            )
            if status.get("status") == "error":
                _vlog(
                    "warn",
                    "primary rest order status failed",
                    {
                        "exchange": primary_leg["exchange"],
                        "order_id": active_order_id,
                        "error": status.get("error"),
                    },
                )
                await asyncio.sleep(max(0.2, reprice_sec))
                return False
            await _apply_primary_fill(
                active_order_id,
                status.get("filled_qty"),
                status=status,
                reason="ws_missing_rest",
            )
            state = str(status.get("status") or "").lower()
            if state in ("canceled", "cancelled", "closed", "filled", "finished"):
                pending_order_ids.discard(active_order_id)
                active_order_id = None
                active_price = None
                active_qty = None
                active_filled = 0.0
                active_since = None
                active_ws_missing_since = None
                active_ws_missing_open = False
                return True
            _vlog(
                "wait",
                "primary ws order update missing; rest confirmed open",
                {
                    "exchange": primary_leg.get("exchange"),
                    "order_id": active_order_id,
                    "status": status.get("status"),
                    "filled_qty": status.get("filled_qty"),
                },
            )
            active_ws_missing_open = True
            return True

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
            pending_order_ids.discard("")  # defensive: stale placeholder ids
            pending_order_ids.discard(None)  # type: ignore[arg-type]
            order_ids = [order_id for order_id in pending_order_ids if order_id]
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
            return total_delta, True

        async def _sync_primary_fills(
            reason: str,
            *,
            delay: float | None = None,
            include_active: bool = True,
            force_rest: bool = False,
        ) -> float:
            nonlocal active_filled, active_order_id, active_price, active_qty, active_since
            if delay:
                await asyncio.sleep(delay)
            ws_delta, used_ws = await _sync_primary_from_orders(reason)
            if not force_rest and await self._ensure_ws_orders_healthy(primary_leg["exchange"], reason=reason, log_cb=log_cb):
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
            pending_order_ids.discard("")  # defensive: stale placeholder ids
            pending_order_ids.discard(None)  # type: ignore[arg-type]
            order_ids = [order_id for order_id in pending_order_ids if order_id]
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

        async def _cancel_primary_and_confirm(reason: str) -> bool:
            nonlocal active_order_id, active_price, active_qty, active_filled, active_since
            nonlocal active_ws_missing_since, active_ws_rest_checked_at, active_ws_missing_open
            order_id = active_order_id
            if not order_id:
                return True
            await _sync_primary_fills(
                f"{reason}_pre_cancel",
                include_active=True,
                force_rest=True,
            )
            if active_order_id is None:
                return True
            order_id = active_order_id
            await self._cancel_order(primary_leg, symbol, order_id)
            pending_order_ids.add(order_id)
            active_order_id = None
            active_price = None
            active_qty = None
            active_filled = 0.0
            active_since = None
            active_ws_missing_since = None
            active_ws_rest_checked_at = None
            active_ws_missing_open = False
            deadline = time.time() + max(3.0, min(10.0, reprice_sec + 2.0))
            while order_id in pending_order_ids and time.time() < deadline:
                await _sync_primary_fills(
                    f"{reason}_cancel_confirm",
                    delay=0.2,
                    include_active=False,
                    force_rest=True,
                )
                if order_id in pending_order_ids:
                    await asyncio.sleep(0.2)
            if order_id in pending_order_ids:
                message = f"primary cancel unconfirmed on {primary_leg['exchange']}: {order_id}"
                errors.append(message)
                self._emit_log(
                    log_cb,
                    "error",
                    "primary cancel unconfirmed",
                    {"exchange": primary_leg["exchange"], "order_id": order_id, "reason": reason},
                )
                return False
            self._emit_log(
                log_cb,
                "cancel",
                "primary cancel confirmed",
                {"exchange": primary_leg["exchange"], "order_id": order_id, "reason": reason},
            )
            return True

        async def _final_reconcile_positions(reason: str) -> bool:
            nonlocal primary_filled_total, hedge_filled_total
            use_observed = False
            positions, pos_errors = await self._fetch_positions_for_symbol(
                exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
                symbol=symbol,
                allow_ws=True,
                contract_sizes=contract_sizes,
            )
            if pos_errors:
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions fetch failed; retrying",
                    {"stage": "final", "errors": pos_errors},
                )
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
                retry_positions, retry_errors = await self._fetch_positions_for_symbol(
                    exchanges=[primary_leg["exchange"], hedge_leg["exchange"]],
                    symbol=symbol,
                    allow_ws=False,
                    contract_sizes=contract_sizes,
                )
                if retry_errors:
                    positions = retry_positions
                    pos_errors = pos_errors + retry_errors
                    warnings.extend(pos_errors)
                    warnings.append("positions fetch failed; using observed fills for final reconcile")
                    use_observed = True
                else:
                    positions = retry_positions
                    pos_errors = []
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
            self._emit_log(
                log_cb,
                "positions",
                "positions snapshot (end)",
                {
                    "stage": "end",
                    "symbol": symbol,
                    "exchanges": exchange_list,
                    "positions": positions,
                    "counts": counts,
                    "errors": pos_errors,
                    "sources": sources,
                },
            )
            if use_observed:
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions unavailable; using observed fills for final reconcile",
                    {
                        "primary_filled_total": primary_filled_total,
                        "hedge_filled_total": hedge_filled_total,
                    },
                )
                primary_delta = primary_filled_total
                hedge_delta = hedge_filled_total
                primary_current = (_safe_float(primary_pos_start) or 0.0) + primary_delta
                hedge_current = (_safe_float(hedge_pos_start) or 0.0) + hedge_delta
            else:
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
                primary_delta = _position_delta_for_leg(primary_pos_start, primary_current, primary_leg)
                hedge_delta = _position_delta_for_leg(hedge_pos_start, hedge_current, hedge_leg)
            primary_over = max(0.0, primary_delta - qty)
            hedge_over = max(0.0, hedge_delta - qty)
            if primary_over > 0 or hedge_over > 0:
                _vlog(
                    "guard",
                    "leg delta above target qty; capping final reconcile",
                    {
                        "primary_delta": primary_delta,
                        "hedge_delta": hedge_delta,
                        "target_qty": qty,
                        "primary_over": primary_over,
                        "hedge_over": hedge_over,
                    },
                )
            imbalance = primary_delta - hedge_delta
            if abs(imbalance) <= 0:
                return True
            if imbalance > 0:
                threshold = hedge_fallback_min
                step = hedge_amount_step
                leg = hedge_leg
                qty_needed = imbalance
                leg_delta = hedge_delta
            else:
                threshold = primary_fallback_min
                step = primary_amount_step
                leg = primary_leg
                qty_needed = abs(imbalance)
                leg_delta = primary_delta
            qty_needed = _cap_qty_to_target(
                requested_qty=qty_needed,
                target_qty=qty,
                leg_delta=leg_delta,
                amount_step=step,
            )
            leg_exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
            hedge_exchange = normalize_exchange_name(str(hedge_leg.get("exchange") or ""))
            leg_current = hedge_current if leg_exchange == hedge_exchange else primary_current
            leg_target_abs = hedge_target_abs if leg_exchange == hedge_exchange else primary_target_abs
            qty_needed = _cap_qty_to_absolute_target(
                requested_qty=qty_needed,
                target_qty=leg_target_abs,
                current_qty=leg_current,
                amount_step=step,
            )
            if qty_needed <= 0:
                _vlog(
                    "guard",
                    "final reconcile skipped by target cap",
                    {
                        "reason": reason,
                        "imbalance": imbalance,
                        "target_qty": qty,
                        "target_abs": leg_target_abs,
                        "leg_exchange": leg.get("exchange"),
                        "leg_delta": leg_delta,
                        "leg_current": leg_current,
                    },
                )
                return True
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
                return False
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
            if result.get("status") == "error":
                return False
            filled = _safe_float(result.get("filled_qty")) or 0.0
            if filled > 0:
                leg_exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
                if leg_exchange == normalize_exchange_name(str(primary_leg.get("exchange") or "")):
                    primary_filled_total += filled
                elif leg_exchange == normalize_exchange_name(str(hedge_leg.get("exchange") or "")):
                    hedge_filled_total += filled
            return True

        async def _hedge_pending(reason: str) -> None:
            nonlocal pending_hedge_qty, hedge_failed, hedge_filled_total, warnings
            hedge_qty = pending_hedge_qty
            if hedge_qty <= 0:
                return
            pending_hedge_qty = 0.0
            cap_source = "observed"
            cap_leg_delta = hedge_filled_total
            cap_current_qty = (_safe_float(hedge_pos_start) or 0.0) + cap_leg_delta
            pos_errors: list[str] = []
            cap_positions, cap_errors = await self._fetch_positions_for_symbol(
                exchanges=[hedge_leg["exchange"]],
                symbol=symbol,
                allow_ws=True,
                contract_sizes=contract_sizes,
            )
            if cap_errors:
                cap_positions, retry_errors = await self._fetch_positions_for_symbol(
                    exchanges=[hedge_leg["exchange"]],
                    symbol=symbol,
                    allow_ws=False,
                    contract_sizes=contract_sizes,
                )
                pos_errors = cap_errors + retry_errors
            else:
                cap_source = "positions_ws"
            if not pos_errors:
                hedge_current = self._sum_position_qty(
                    cap_positions,
                    exchange=hedge_leg["exchange"],
                    side=hedge_side,
                    symbol=symbol,
                )
                cap_leg_delta = _position_delta_for_leg(hedge_pos_start, hedge_current, hedge_leg)
                cap_current_qty = hedge_current
                if cap_source != "positions_ws":
                    cap_source = "positions_rest"
            else:
                _vlog(
                    "warn",
                    "hedge target cap using observed fills (positions unavailable)",
                    {"reason": reason, "errors": pos_errors},
                )
            raw_hedge_qty = hedge_qty
            hedge_qty = _cap_qty_to_target(
                requested_qty=hedge_qty,
                target_qty=qty,
                leg_delta=cap_leg_delta,
                amount_step=None,
            )
            if hedge_qty < raw_hedge_qty:
                _vlog(
                    "guard",
                    "hedge qty capped by target",
                    {
                        "requested_qty": raw_hedge_qty,
                        "capped_qty": hedge_qty,
                        "target_qty": qty,
                        "leg_delta": cap_leg_delta,
                        "cap_source": cap_source,
                        "reason": reason,
                    },
                )
            pre_abs_cap_qty = hedge_qty
            hedge_qty = _cap_qty_to_absolute_target(
                requested_qty=hedge_qty,
                target_qty=hedge_target_abs,
                current_qty=cap_current_qty,
                amount_step=None,
            )
            if hedge_qty < pre_abs_cap_qty:
                _vlog(
                    "guard",
                    "hedge qty capped by absolute target",
                    {
                        "requested_qty": pre_abs_cap_qty,
                        "capped_qty": hedge_qty,
                        "target_qty": qty,
                        "target_abs": hedge_target_abs,
                        "current_qty": cap_current_qty,
                        "cap_source": cap_source,
                        "reason": reason,
                    },
                )
            hedge_qty = _pending_hedge_order_qty(
                hedge_qty,
                min_qty_required=min_hedge_qty,
                amount_step=hedge_amount_step,
            )
            if hedge_qty <= 0:
                _vlog(
                    "guard",
                    "hedge target reached; skipping pending hedge",
                    {
                        "target_qty": qty,
                        "target_abs": hedge_target_abs,
                        "leg_delta": cap_leg_delta,
                        "current_qty": cap_current_qty,
                        "cap_source": cap_source,
                        "reason": reason,
                    },
                )
                return
            if min_hedge_qty and hedge_qty < min_hedge_qty:
                _vlog(
                    "wait",
                    "hedge below minimum; skipping",
                    {"pending_qty": hedge_qty, "min_qty": min_hedge_qty, "reason": reason},
                )
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
                hedge_adverse_ticks=_safe_float(payload.get("hedge_adverse_ticks")),
                hedge_reprice_min_sec=hedge_reprice_min_sec,
                payload=payload,
                min_qty_required=min_hedge_qty,
                log_cb=log_cb,
            )
            actions.append(hedge_result)
            self._emit_log(log_cb, "result", "hedge result", hedge_result)
            hedge_filled_total += _safe_float(hedge_result.get("filled_qty")) or 0.0
            if hedge_result.get("status") == "error":
                if primary_filled_total > hedge_filled_total:
                    hedge_result["risk_state"] = "partial_fill_exposure"
                    reconciled = await _final_reconcile_positions("hedge_error")
                    if reconciled:
                        hedge_result["handled_error"] = "final_reconcile"
                        hedge_result.pop("risk_state", None)
                        warnings.append("hedge_error_reconciled")
                        self._emit_log(
                            log_cb,
                            "warn",
                            "hedge error reconciled; continuing smart enter",
                            {
                                "exchange": hedge_leg["exchange"],
                                "error": hedge_result.get("error"),
                                "primary_filled_total": primary_filled_total,
                                "hedge_filled_total": hedge_filled_total,
                            },
                        )
                        return
                    warnings.append("partial_fill_exposure")
                errors.append(
                    f"hedge failed on {hedge_leg['exchange']}: {hedge_result.get('error') or 'unknown_error'}"
                )
                hedge_failed = True
                return
            pending_qty = _safe_float(hedge_result.get("pending_qty"))
            if pending_qty:
                if min_hedge_qty and pending_qty < min_hedge_qty:
                    _vlog(
                        "wait",
                        "hedge remainder below minimum; skipping",
                        {"pending_qty": pending_qty, "min_qty": min_hedge_qty, "reason": reason},
                    )
                else:
                    pending_hedge_qty += pending_qty
                    _vlog(
                        "wait",
                        "hedge remainder pending; re-queued",
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
                if pending_hedge_qty > 0:
                    await _hedge_pending("stop_cancel")
                break
            snapshot = await self._snapshot_legs(symbol, [primary_leg, hedge_leg], max_slippage_bps=max_slippage_bps)
            if snapshot.get("errors"):
                self._emit_log(log_cb, "wait", "orderbook fetch failed; waiting", {"errors": snapshot.get("errors")})
                await asyncio.sleep(max(0.5, reprice_sec))
                continue
            _vlog(
                "snapshot",
                f"{mode_label} snapshot",
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
                if not actions and (time.time() - started_at) >= trigger_wait_sec:
                    warnings.append("condition_not_met")
                    self._emit_log(
                        log_cb,
                        "result",
                        "spread condition not met; releasing execution worker",
                        {
                            "spread_pct": spread_val,
                            "spread_min_pct": spread_min_pct,
                            "spread_max_pct": spread_max_pct,
                            "trigger_wait_sec": trigger_wait_sec,
                        },
                    )
                    break
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
                    if active_order_id:
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
            if pending_hedge_qty > 0:
                if active_order_id and not await _cancel_primary_and_confirm("primary_partial_fill"):
                    break
                await _hedge_pending("primary_fill")
                if hedge_failed:
                    break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue
            if not await _ensure_active_order_visible("loop"):
                continue

            if active_order_id is None and pending_hedge_qty > 0:
                await _hedge_pending("post_primary")
                if hedge_failed:
                    break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            if pending_order_ids:
                await _sync_primary_fills("pre_chunk", include_active=False)
                if pending_order_ids:
                    if awaiting_ws_update:
                        if not await _require_fresh_ws_update("pending_cancel"):
                            now = time.time()
                            if pending_ws_rest_checked_at is None or (now - pending_ws_rest_checked_at) >= ws_missing_rest_interval_sec:
                                pending_ws_rest_checked_at = now
                                await _sync_primary_fills("pending_rest", include_active=False, force_rest=True)
                                awaiting_ws_update = False
                                if not pending_order_ids:
                                    continue
                        _vlog(
                            "wait",
                            "primary cancel pending; waiting",
                            {"exchange": primary_leg["exchange"], "pending": list(pending_order_ids)},
                        )
                        await asyncio.sleep(max(0.2, reprice_sec))
                        continue
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
            primary_taker_cap = max_qty_by_exchange.get(primary_leg["exchange"])
            hedge_taker_cap = max_qty_by_exchange.get(hedge_leg["exchange"])
            max_chunk = hedge_taker_cap
            limiting_exchange = hedge_leg["exchange"] if hedge_taker_cap is not None else None
            if max_slippage_bps > 0:
                hedge_ready = hedge_taker_cap is not None and float(hedge_taker_cap) > 0
                if hedge_ready and min_chunk_qty:
                    hedge_ready = float(hedge_taker_cap) >= float(min_chunk_qty)
                if not hedge_ready:
                    if active_order_id and not await _cancel_primary_and_confirm("hedge_liquidity_lost"):
                        break
                    self._emit_log(
                        log_cb,
                        "wait",
                        "hedge liquidity below safe chunk; primary maker paused",
                        {
                            "primary_exchange": primary_leg["exchange"],
                            "hedge_exchange": hedge_leg["exchange"],
                            "hedge_max_qty": hedge_taker_cap,
                            "min_chunk_qty": min_chunk_qty,
                            "max_slippage_bps": max_slippage_bps,
                        },
                    )
                    await asyncio.sleep(max(0.2, reprice_sec))
                    continue

            mid_price = snapshot.get("mid_price") or 0.0
            requested = requested_chunk
            if requested is None and chunk_notional and mid_price:
                requested = chunk_notional / mid_price
            max_chunk_for_choice, auto_chunk_notional_cap = _cap_auto_chunk_by_notional(
                requested_qty=requested,
                chunk_notional=chunk_notional,
                max_chunk=max_chunk if max_slippage_bps > 0 else None,
                mid_price=mid_price,
                legs=[hedge_leg],
            )
            chunk, chunk_warnings = _choose_chunk_qty(
                remaining=remaining,
                requested_qty=requested,
                min_chunk=min_chunk_qty,
                max_chunk=max_chunk_for_choice,
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
                f"{mode_label} chunk",
                {
                    "remaining": remaining,
                    "chunk": chunk,
                    "min_chunk_qty": min_chunk_qty,
                    "max_chunk": max_chunk,
                    "max_chunk_for_choice": max_chunk_for_choice,
                    "auto_chunk_notional_cap": auto_chunk_notional_cap,
                    "max_qty_by_exchange": dict(max_qty_by_exchange),
                    "primary_immediate_taker_cap": primary_taker_cap,
                    "hedge_taker_cap": hedge_taker_cap,
                    "limiting_exchange": limiting_exchange,
                    "max_slippage_bps": max_slippage_bps,
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
                f"{mode_label} limit price",
                {
                    "limit_price": limit_price,
                    "price_step": price_step,
                    "offset_bps": limit_offset_bps,
                    "offset_ticks": limit_offset_ticks,
                    "improve_ticks": improve_ticks,
                },
            )

            if active_order_id and timeout > 0 and active_since and (time.time() - active_since) > timeout:
                if not await _cancel_primary_and_confirm("timeout"):
                    break
                await asyncio.sleep(max(0.2, reprice_sec))
                continue

            if active_order_id:
                if active_price != limit_price or (active_qty is not None and active_qty != chunk):
                    if active_ws_missing_open and active_price != limit_price:
                        dev_bps = _price_deviation_bps(active_price, limit_price)
                        threshold = _safe_float(payload.get("max_limit_deviation_bps")) or 30.0
                        if dev_bps is not None and dev_bps < threshold:
                            self._emit_log(
                                log_cb,
                                "wait",
                                "primary ws order update missing; keeping order (deviation below threshold)",
                                {
                                    "exchange": primary_leg["exchange"],
                                    "order_id": active_order_id,
                                    "current_price": active_price,
                                    "target_price": limit_price,
                                    "deviation_bps": round(dev_bps, 2),
                                    "threshold_bps": threshold,
                                },
                            )
                            await asyncio.sleep(max(0.2, reprice_sec))
                            continue
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
                    if active_order_id:
                        await self._cancel_order(primary_leg, symbol, active_order_id)
                        pending_order_ids.add(active_order_id)
                    active_order_id = None
                    active_price = None
                    active_qty = None
                    active_filled = 0.0
                    active_since = None
                    await _sync_primary_fills("post_reprice_cancel", delay=0.2, include_active=False)
                    await _pause_after_cancel("post_reprice_cancel")
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
                    post_only=True,
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
                active_ws_missing_since = None
                active_ws_rest_checked_at = None
                active_ws_missing_open = False
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
            await _cancel_primary_and_confirm("runtime_end")
        if pending_order_ids:
            await _sync_primary_fills("final_sync", delay=0.2, include_active=False)

        if pending_hedge_qty > 0 and not hedge_failed and not stopped_by_user:
            await _hedge_pending("final_pending")

        if remaining > 0 and max_runtime_sec is not None:
            warnings.append(f"Remaining qty {remaining:g} not entered ({mode_label} runtime ended).")

        if (
            "condition_not_met" not in warnings
            and (not stopped_by_user or self._stop_force_finalize())
        ):
            await _final_reconcile_positions("final")
        return {
            "dry_run": False,
            "action": plan.get("action"),
            "symbol": symbol,
            "qty": qty,
            "mode": mode_label,
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": warnings,
            "risk_flags": self._collect_risk_flags(actions, warnings),
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
        trigger_wait_sec = _trigger_wait_sec(payload, max_runtime_sec)
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
        _, primary_leg, hedge_leg = self._resolve_primary_hedge_legs(
            explicit=payload.get("expensive_leg"),
            plan=plan,
            legs=legs,
        )
        if primary_leg and hedge_leg:
            legs = [primary_leg, hedge_leg]

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
        start_positions, start_errors = await self._fetch_positions_with_retry(
            exchanges=exchange_list,
            symbol=symbol,
            log_cb=log_cb,
        )
        emit_positions_snapshot("start", start_positions, start_errors)
        if start_errors:
            errors.extend(start_errors)
            self._emit_log(
                log_cb,
                "error",
                "positions fetch failed; stopping",
                {"stage": "start", "errors": start_errors},
            )
            return {
                "dry_run": False,
                "action": plan.get("action"),
                "symbol": symbol,
                "qty": qty,
                "mode": "fast-enter",
                "actions": actions,
                "errors": errors,
                "warnings": warnings,
                "risk_flags": self._collect_risk_flags(actions, warnings),
                "remaining_qty": remaining,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        start_qty_by_exchange = {
            leg["exchange"]: (
                _safe_float(
                    self._sum_position_qty(
                        start_positions,
                        exchange=leg["exchange"],
                        side=_entry_position_side(leg),
                        symbol=symbol,
                    )
                )
                or 0.0
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
                    if not actions and (time.time() - started_at) >= trigger_wait_sec:
                        warnings.append("condition_not_met")
                        self._emit_log(
                            log_cb,
                            "result",
                            "spread condition not met; releasing execution worker",
                            {
                                "spread_pct": spread_val,
                                "spread_min_pct": spread_min_pct,
                                "spread_max_pct": spread_max_pct,
                                "trigger_wait_sec": trigger_wait_sec,
                            },
                        )
                        break
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
                max_chunk_for_choice, _auto_chunk_notional_cap = _cap_auto_chunk_by_notional(
                    requested_qty=requested,
                    chunk_notional=chunk_notional,
                    max_chunk=max_chunk if max_slippage_bps > 0 else None,
                    mid_price=mid_price,
                    legs=legs,
                )
                chunk, chunk_warnings = _choose_chunk_qty(
                    remaining=remaining,
                    requested_qty=requested,
                    min_chunk=min_chunk_qty,
                    max_chunk=max_chunk_for_choice,
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
                for exchange, filled_qty in filled_by_exchange.items():
                    observed_fills[exchange] = observed_fills.get(exchange, 0.0) + filled_qty
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
            use_observed = False
            end_positions, end_errors = await self._fetch_positions_for_symbol(
                exchanges=exchange_list,
                symbol=symbol,
                allow_ws=False,
                contract_sizes=contract_sizes,
            )
            if end_errors:
                self._emit_log(
                    log_cb,
                    "warn",
                    "positions fetch failed; retrying",
                    {"stage": "final", "errors": end_errors},
                )
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
                retry_positions, retry_errors = await self._fetch_positions_for_symbol(
                    exchanges=exchange_list,
                    symbol=symbol,
                    allow_ws=False,
                    contract_sizes=contract_sizes,
                )
                if retry_errors:
                    end_positions = retry_positions
                    end_errors = end_errors + retry_errors
                    warnings.extend(end_errors)
                    warnings.append("positions fetch failed; using observed fills for final reconcile")
                    use_observed = True
                else:
                    end_positions = retry_positions
                    end_errors = []
            emit_positions_snapshot("end", end_positions, end_errors)
            if stopped_by_user and not self._stop_force_finalize():
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
                    "risk_flags": self._collect_risk_flags(actions, warnings),
                    "remaining_qty": remaining,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
            if stopped_by_user and self._stop_force_finalize():
                self._emit_log(
                    log_cb,
                    "warn",
                    "stop requested; forcing final reconcile",
                    {"remaining": remaining},
                )
            if len(legs) >= 2 and "condition_not_met" not in warnings:
                deltas: dict[str, float] = {}
                for leg in legs:
                    exchange = leg["exchange"]
                    if use_observed:
                        delta = observed_fills.get(exchange, 0.0)
                    else:
                        end_qty = (
                            _safe_float(
                                self._sum_position_qty(
                                    end_positions,
                                    exchange=exchange,
                                    side=_entry_position_side(leg),
                                    symbol=symbol,
                                )
                            )
                            or 0.0
                        )
                        start_qty = _safe_float(start_qty_by_exchange.get(exchange)) or 0.0
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
            "risk_flags": self._collect_risk_flags(actions, warnings),
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

    async def _place_limit_at_agent(
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
            require_ws=False,
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
        require_ws: bool = True,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        submit = await self._submit_order(
            leg,
            symbol,
            qty,
            "market",
            price=None,
            reduce_only=bool(leg.get("reduce_only")),
            require_ws=require_ws,
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

    async def agent_rebalance(
        self,
        *,
        exchange: str,
        symbol: str,
        side: str,
        qty_base: float,
        margin_mode: str | None = None,
        limit_timeout_sec: int = 10,
        limit_offset_bps: float = 2.0,
        max_slippage_bps: float = 8.0,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(exchange or ""))
        side = str(side or "").lower()
        qty_val = _safe_float(qty_base)
        if not exchange:
            return {"status": "error", "error": "missing_exchange"}
        if side not in ("buy", "sell"):
            return {"status": "error", "error": "invalid_side", "exchange": exchange}
        if qty_val is None or qty_val <= 0:
            return {"status": "error", "error": "invalid_qty", "exchange": exchange}
        limit_timeout = max(1, int(limit_timeout_sec))
        offset_bps = _safe_float(limit_offset_bps) or 0.0
        slippage_bps = _safe_float(max_slippage_bps) or 0.0
        leg = {
            "exchange": exchange,
            "side": side,
            "label": "rebalance",
            "reduce_only": True,
        }
        if margin_mode:
            leg["margin_mode"] = margin_mode
        remaining = float(qty_val)
        filled_total = 0.0
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        while remaining > 0:
            snapshot = await self._snapshot_legs(symbol, [leg], max_slippage_bps=slippage_bps)
            snap_errors = snapshot.get("errors") or []
            if snap_errors:
                errors.extend([str(err) for err in snap_errors])
                break
            stats = (snapshot.get("stats") or {}).get(exchange)
            constraints = (snapshot.get("constraints") or {}).get(exchange) or {}
            price_step = _safe_float(constraints.get("price_step"))
            amount_step = _safe_float(constraints.get("amount_step"))
            min_qty = _safe_float(constraints.get("min_qty"))
            min_notional = _safe_float(constraints.get("min_notional"))
            mid_price = stats.mid if stats else _safe_float(snapshot.get("mid_price"))
            min_required = _min_qty_required(
                min_qty=min_qty,
                min_notional=min_notional,
                price=mid_price or 0.0,
                amount_step=amount_step,
            )
            max_chunk = None
            if slippage_bps > 0:
                max_chunk = (snapshot.get("max_qty_by_exchange") or {}).get(exchange)
            chunk, chunk_warnings = _choose_chunk_qty(
                remaining=remaining,
                requested_qty=None,
                min_chunk=min_required,
                max_chunk=max_chunk,
                amount_step=amount_step,
            )
            if chunk_warnings:
                errors.extend(chunk_warnings)
            if not chunk or chunk <= 0:
                errors.append("chunk_below_min")
                break
            limit_price = _resolve_smart_limit_price(
                orderbook=(snapshot.get("orderbooks") or {}).get(exchange),
                side=side,
                book_side=None,
                qty=chunk,
                payload={},
                best_bid=stats.best_bid if stats else None,
                best_ask=stats.best_ask if stats else None,
                mid_price=stats.mid if stats else None,
                price_step=price_step,
                offset_bps=offset_bps,
                offset_ticks=0,
                improve_ticks=1,
                round_mode="aggressive",
            )
            if not limit_price:
                errors.append("missing_limit_price")
                break
            limit_result = await self._place_limit_at_agent(
                leg,
                symbol,
                chunk,
                limit_price,
                limit_timeout,
                log_cb=log_cb,
            )
            actions.append(
                {
                    "type": "limit",
                    "exchange": exchange,
                    "qty": chunk,
                    "price": limit_price,
                    "result": limit_result,
                }
            )
            status = str(limit_result.get("status") or "").lower()
            filled_qty = _safe_float(limit_result.get("filled_qty")) or 0.0
            if status == "filled":
                if filled_qty <= 0:
                    filled_qty = chunk
                filled_total += filled_qty
                remaining = max(0.0, remaining - filled_qty)
                continue
            if status == "error":
                errors.append(str(limit_result.get("error") or "limit_error"))
                break
            leftover = max(0.0, chunk - filled_qty)
            if leftover <= 0:
                remaining = max(0.0, remaining - filled_qty)
                continue
            market_result = await self._place_market(
                leg,
                symbol,
                leftover,
                {},
                reason="rebalance_timeout",
                require_ws=False,
                log_cb=log_cb,
            )
            actions.append(
                {
                    "type": "market",
                    "exchange": exchange,
                    "qty": leftover,
                    "result": market_result,
                }
            )
            if market_result.get("status") == "error":
                errors.append(str(market_result.get("error") or "market_error"))
                break
            filled_total += leftover
            remaining = max(0.0, remaining - leftover)
        status = "filled" if remaining <= 0 and not errors else "partial"
        if filled_total <= 0 and errors:
            status = "error"
        return {
            "exchange": exchange,
            "symbol": symbol,
            "side": side,
            "status": status,
            "requested_qty": float(qty_val),
            "filled_qty": filled_total,
            "remaining_qty": remaining,
            "errors": errors,
            "actions": actions,
        }

    async def analyze_rebalance(
        self,
        *,
        exchange: str,
        symbol: str,
        side: str,
        qty_base: float,
        max_slippage_bps: float = 8.0,
    ) -> dict[str, Any]:
        """Read-only single-leg preflight used by protective agents."""
        exchange = normalize_exchange_name(str(exchange or ""))
        side = str(side or "").lower()
        qty_val = _safe_float(qty_base)
        if not exchange or side not in {"buy", "sell"} or not qty_val or qty_val <= 0:
            return {"errors": ["invalid protective preflight request"]}
        snapshot = await self._snapshot_legs(
            symbol,
            [{"exchange": exchange, "side": side, "label": "protective"}],
            max_slippage_bps=max_slippage_bps,
        )
        errors = [str(item) for item in (snapshot.get("errors") or [])]
        constraints = dict((snapshot.get("constraints") or {}).get(exchange) or {})
        stats = (snapshot.get("stats") or {}).get(exchange)
        mid_price = _safe_float(snapshot.get("mid_price"))
        min_required = _min_qty_required(
            min_qty=_safe_float(constraints.get("min_qty")),
            min_notional=_safe_float(constraints.get("min_notional")),
            price=mid_price or 0.0,
            amount_step=_safe_float(constraints.get("amount_step")),
        )
        return {
            "errors": errors,
            "exchange": exchange,
            "symbol": symbol,
            "side": side,
            "requested_qty": float(qty_val),
            "constraints": constraints,
            "min_qty_required": min_required,
            "mid_price": mid_price,
            "max_qty_under_slippage": (
                snapshot.get("max_qty_by_exchange") or {}
            ).get(exchange),
            "stats": _stats_payload(stats),
            "orderbook_source": (
                snapshot.get("orderbook_sources") or {}
            ).get(exchange),
            "generated_at": _now_iso(),
        }

    async def _execute_orphan_cleanup(
        self,
        payload: Mapping[str, Any],
        positions: Iterable[Mapping[str, Any]],
        *,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        symbol = normalize_symbol(str(payload.get("symbol") or ""))
        cleanup_exchange = normalize_exchange_name(
            str(payload.get("cleanup_exchange") or payload.get("exchange") or "")
        )
        requested_qty = _safe_float(payload.get("qty"))
        requested_position_side = str(
            payload.get("cleanup_position_side") or payload.get("position_side") or ""
        ).strip().lower()
        panic_cleanup = bool(payload.get("panic_cleanup_mode"))
        margin_mode = str(payload.get("margin_mode") or "isolated").strip().lower() or "isolated"
        warnings: list[str] = []
        errors: list[str] = []
        actions: list[dict[str, Any]] = []
        if not symbol:
            errors.append("symbol is required")
        if not cleanup_exchange:
            errors.append("cleanup_exchange is required")
        if errors:
            return {
                "dry_run": False,
                "action": "exit",
                "symbol": symbol,
                "qty": requested_qty,
                "mode": "orphan-cleanup",
                "actions": actions,
                "errors": errors,
                "warnings": warnings,
                "risk_flags": self._collect_risk_flags(actions, warnings),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        def _pos_qty(entry: Mapping[str, Any]) -> float:
            qty = _safe_float(entry.get("coin_qty"))
            if qty is None:
                qty = _safe_float(entry.get("contracts"))
            if qty is None:
                qty = _safe_float(entry.get("amount"))
            return abs(float(qty or 0.0))

        matching: list[dict[str, Any]] = []
        for raw in positions or []:
            position = dict(raw)
            exchange = normalize_exchange_name(str(position.get("exchange") or ""))
            if exchange != cleanup_exchange:
                continue
            pos_symbol = str(position.get("symbol") or position.get("symbol_normalized") or "")
            if not _symbol_matches(symbol, pos_symbol):
                continue
            qty_hint = _safe_float(position.get("coin_qty"))
            if qty_hint is None:
                qty_hint = _safe_float(position.get("contracts")) or _safe_float(position.get("amount"))
            position_side = _normalize_position_side(position.get("side"), qty_hint)
            if requested_position_side in ("long", "short") and position_side != requested_position_side:
                continue
            position["_normalized_side"] = position_side
            matching.append(position)
        if not matching:
            return {
                "dry_run": False,
                "action": "exit",
                "symbol": symbol,
                "qty": requested_qty,
                "mode": "orphan-cleanup",
                "actions": actions,
                "errors": [f"{cleanup_exchange}: orphan position not found"],
                "warnings": warnings,
                "risk_flags": self._collect_risk_flags(actions, warnings),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        side_set = {str(item.get("_normalized_side") or "") for item in matching if item.get("_normalized_side")}
        if len(side_set) > 1:
            return {
                "dry_run": False,
                "action": "exit",
                "symbol": symbol,
                "qty": requested_qty,
                "mode": "orphan-cleanup",
                "actions": actions,
                "errors": [f"{cleanup_exchange}: multiple orphan sides visible"],
                "warnings": warnings,
                "risk_flags": self._collect_risk_flags(actions, warnings),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        position_side = next(iter(side_set), requested_position_side or "")
        current_qty = sum(_pos_qty(item) for item in matching)
        if current_qty <= 0:
            return {
                "dry_run": False,
                "action": "exit",
                "symbol": symbol,
                "qty": requested_qty,
                "mode": "orphan-cleanup",
                "actions": actions,
                "errors": [f"{cleanup_exchange}: orphan qty unavailable"],
                "warnings": warnings,
                "risk_flags": self._collect_risk_flags(actions, warnings),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        cleanup_qty = current_qty
        if requested_qty is not None and requested_qty > 0:
            cleanup_qty = min(cleanup_qty, float(requested_qty))
        close_side = "sell" if position_side == "long" else "buy"
        leg = {
            "exchange": cleanup_exchange,
            "side": close_side,
            "label": "orphan",
            "reduce_only": True,
            "margin_mode": margin_mode,
        }
        snapshot = await self._snapshot_legs(
            symbol,
            [leg],
            max_slippage_bps=_safe_float(payload.get("max_slippage_bps")) or 0.0,
        )
        constraints = (snapshot.get("constraints") or {}).get(cleanup_exchange) or {}
        self._emit_log(
            log_cb,
            "start",
            "orphan cleanup",
            {
                "symbol": symbol,
                "exchange": cleanup_exchange,
                "position_side": position_side,
                "close_side": close_side,
                "qty": cleanup_qty,
                "panic_cleanup_mode": panic_cleanup,
            },
        )

        if panic_cleanup:
            market_result = await self._place_market(
                leg,
                symbol,
                cleanup_qty,
                payload,
                reason="orphan_cleanup_panic",
                require_ws=False,
                log_cb=log_cb,
            )
            actions.append(market_result)
            if market_result.get("status") == "error":
                errors.append(str(market_result.get("error") or "orphan_cleanup_market_error"))
        else:
            rebalance = await self.agent_rebalance(
                exchange=cleanup_exchange,
                symbol=symbol,
                side=close_side,
                qty_base=cleanup_qty,
                margin_mode=margin_mode,
                limit_timeout_sec=int(_safe_float(payload.get("orphan_limit_timeout_sec")) or 6),
                limit_offset_bps=_safe_float(payload.get("orphan_limit_offset_bps")) or 1.0,
                max_slippage_bps=_safe_float(payload.get("max_slippage_bps")) or 8.0,
                log_cb=log_cb,
            )
            actions.append(
                {
                    "exchange": cleanup_exchange,
                    "status": "error" if rebalance.get("status") == "error" else "submitted",
                    "filled_qty": rebalance.get("filled_qty"),
                    "remaining_qty": rebalance.get("remaining_qty"),
                    "detail": rebalance,
                    "market_reason": "orphan_cleanup",
                }
            )
            if rebalance.get("status") == "error":
                errors.extend([str(item) for item in (rebalance.get("errors") or []) if item])
            elif rebalance.get("status") == "partial":
                warnings.append("orphan_cleanup_partial")

        await self._finalize_exit_dust(
            symbol=symbol,
            legs=[leg],
            start_qty_by_exchange={cleanup_exchange: current_qty},
            requested_exit_qty=cleanup_qty,
            constraints={cleanup_exchange: constraints},
            payload=payload,
            actions=actions,
            warnings=warnings,
            log_cb=log_cb,
        )

        end_positions, end_errors = await self._fetch_positions_for_symbol(
            exchanges=[cleanup_exchange],
            symbol=symbol,
            allow_ws=False,
            contract_sizes=self._contract_sizes_from_constraints({cleanup_exchange: constraints}),
        )
        if end_errors:
            warnings.extend(end_errors)
        remaining_qty = self._sum_position_qty(
            end_positions,
            exchange=cleanup_exchange,
            side=position_side,
            symbol=symbol,
        )
        if panic_cleanup and remaining_qty > 0:
            self._emit_log(
                log_cb,
                "submit",
                f"orphan forced final market {cleanup_exchange} qty={remaining_qty:g}",
                {"symbol": symbol, "exchange": cleanup_exchange},
            )
            final_result = await self._place_market(
                leg,
                symbol,
                remaining_qty,
                payload,
                reason="orphan_cleanup_final",
                require_ws=False,
                log_cb=log_cb,
            )
            actions.append(final_result)
            if final_result.get("status") == "error":
                errors.append(str(final_result.get("error") or "orphan_cleanup_final_error"))
            end_positions, end_errors = await self._fetch_positions_for_symbol(
                exchanges=[cleanup_exchange],
                symbol=symbol,
                allow_ws=False,
                contract_sizes=self._contract_sizes_from_constraints({cleanup_exchange: constraints}),
            )
            if end_errors:
                warnings.extend(end_errors)
            remaining_qty = self._sum_position_qty(
                end_positions,
                exchange=cleanup_exchange,
                side=position_side,
                symbol=symbol,
            )

        if remaining_qty > 0:
            warnings.append(f"{cleanup_exchange}: orphan residual {remaining_qty:g}")
            actions.append(
                {
                    "exchange": cleanup_exchange,
                    "status": "error",
                    "error": f"orphan residual {remaining_qty:g}",
                    "risk_state": "orphan_cleanup_residual",
                }
            )

        return {
            "dry_run": False,
            "action": "exit",
            "symbol": symbol,
            "qty": cleanup_qty,
            "mode": "orphan-cleanup",
            "actions": actions,
            "errors": errors + self._collect_action_errors(actions),
            "warnings": warnings,
            "risk_flags": self._collect_risk_flags(actions, warnings),
            "remaining_qty": remaining_qty,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _submit_order(
        self,
        leg: Mapping[str, Any],
        symbol: str,
        qty: float,
        order_type: str,
        *,
        price: float | None,
        reduce_only: bool,
        post_only: bool = False,
        require_ws: bool = True,
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
        if require_ws and not await self._ensure_ws_orders_recovered(
            exchange,
            reason="submit",
            log_cb=log_cb,
        ):
            return {
                "exchange": exchange,
                "status": "error",
                "error": "ws_listen_key_recover_failed",
                "ts": _now_iso(),
            }
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
        constraints = self._extract_market_constraints(client, ccxt_symbol)
        min_qty = _safe_float(constraints.get("min_qty"))
        min_notional = _safe_float(constraints.get("min_notional"))
        amount_step = _safe_float(constraints.get("amount_step"))
        price_step = _safe_float(constraints.get("price_step"))
        price_min = _safe_float(constraints.get("price_min"))
        price_max = _safe_float(constraints.get("price_max"))
        normalized_qty, normalized_price, normalize_error = _normalize_submit_values(
            qty=float(qty),
            price=price,
            side=str(leg.get("side") or ""),
            order_type=order_type,
            min_qty=min_qty,
            min_notional=min_notional,
            amount_step=amount_step,
            price_step=price_step,
            price_min=price_min,
            price_max=price_max,
        )
        if normalize_error:
            error_type = _classify_submit_error(normalize_error) or "precheck"
            self._emit_log(
                log_cb,
                "error",
                "order precheck failed",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "order_type": order_type,
                    "side": leg.get("side"),
                    "qty_base": float(qty),
                    "price": price if order_type == "limit" else None,
                    "error": normalize_error,
                    "error_type": error_type,
                },
            )
            return {
                "exchange": exchange,
                "status": "error",
                "error": normalize_error,
                "error_type": error_type,
                "ts": _now_iso(),
            }
        contract_size = _safe_float(market.get("contractSize")) if isinstance(market, dict) else None
        qty = float(normalized_qty if normalized_qty is not None else qty)
        if normalized_price is not None:
            price = float(normalized_price)
        order_qty = float(qty)
        if contract_size and contract_size > 0:
            order_qty = order_qty / contract_size
        order_qty = _ccxt_precision_value(client, "amount", ccxt_symbol, order_qty) or order_qty
        if order_type == "limit" and price is not None:
            precise_price = _ccxt_precision_value(client, "price", ccxt_symbol, price)
            if precise_price is not None and precise_price > 0:
                price = precise_price
        if reduce_only:
            reduce_only_error = await self._precheck_reduce_only_qty(
                client,
                exchange=exchange,
                symbol=symbol,
                ccxt_symbol=ccxt_symbol,
                leg=leg,
                qty_base=float(qty),
                contract_size=contract_size,
                log_cb=log_cb,
            )
            if reduce_only_error:
                error_type = _classify_submit_error(reduce_only_error) or "reduce_only_required"
                self._emit_log(
                    log_cb,
                    "error",
                    "reduce-only precheck failed",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "order_type": order_type,
                        "side": leg.get("side"),
                        "qty_base": float(qty),
                        "error": reduce_only_error,
                        "error_type": error_type,
                    },
                )
                return {
                    "exchange": exchange,
                    "status": "error",
                    "error": reduce_only_error,
                    "error_type": error_type,
                    "ts": _now_iso(),
                }
        params = {}
        if reduce_only:
            params["reduceOnly"] = True
        if post_only and order_type == "limit":
            params["postOnly"] = True
        kucoin_margin_mode = None
        leg_margin_mode = str(leg.get("margin_mode") or "").strip().lower()
        margin_key = (exchange, ccxt_symbol, leg_margin_mode)
        if (
            exchange not in ("kucoin", "binance")
            and not (exchange == "bitget" and bitget_uta_enabled())
            and leg_margin_mode in ("isolated", "cross")
            and hasattr(client, "set_margin_mode")
            and margin_key not in self._prepared_margin_settings
        ):
            margin_params: dict[str, object] | None = None
            if exchange == "okx":
                margin_params = {"lever": int(DEFAULT_MANUAL_LEVERAGE)}
            try:
                if margin_params:
                    await client.set_margin_mode(leg_margin_mode, ccxt_symbol, margin_params)
                else:
                    await client.set_margin_mode(leg_margin_mode, ccxt_symbol)
                self._prepared_margin_settings.add(margin_key)
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
        leverage_key = (
            exchange,
            ccxt_symbol,
            leg_margin_mode,
            int(DEFAULT_MANUAL_LEVERAGE),
        )
        if (
            exchange not in ("kucoin", "binance")
            and hasattr(client, "set_leverage")
            and leverage_key not in self._prepared_leverage_settings
        ):
            leverage_params: dict[str, object] = {}
            if leg_margin_mode in ("isolated", "cross"):
                if exchange == "okx":
                    leverage_params["tdMode"] = leg_margin_mode
                elif exchange == "bitget":
                    leverage_params = _bitget_params(leverage_params)
                    leverage_params["marginMode"] = "isolated" if leg_margin_mode == "isolated" else "crossed"
                    if leg_margin_mode == "isolated":
                        leverage_params["posSide"] = bitget_position_side(
                            str(leg.get("side") or ""),
                            reduce_only=reduce_only,
                        )
                elif exchange != "bingx":
                    leverage_params["marginMode"] = leg_margin_mode
            elif exchange == "bitget":
                leverage_params = _bitget_params(leverage_params)
            if exchange == "bingx":
                leverage_params["side"] = "LONG" if leg.get("side") == "buy" else "SHORT"
            try:
                await client.set_leverage(DEFAULT_MANUAL_LEVERAGE, ccxt_symbol, leverage_params or None)
                self._prepared_leverage_settings.add(leverage_key)
            except Exception as exc:  # pylint: disable=broad-except
                error_text = str(exc).lower()
                if exchange == "bybit" and (
                    "110043" in error_text or "leverage not modified" in error_text
                ):
                    self._prepared_leverage_settings.add(leverage_key)
                    self._emit_log(
                        log_cb,
                        "precheck",
                        "leverage already configured",
                        {
                            "exchange": exchange,
                            "symbol": symbol,
                            "leverage": DEFAULT_MANUAL_LEVERAGE,
                        },
                    )
                elif exchange == "bingx" and _bingx_invalid_leverage_params(exc):
                    fallback_params = dict(leverage_params)
                    fallback_params["side"] = "BOTH"
                    try:
                        await client.set_leverage(DEFAULT_MANUAL_LEVERAGE, ccxt_symbol, fallback_params)
                        self._prepared_leverage_settings.add(leverage_key)
                        self._emit_log(
                            log_cb,
                            "warn",
                            "set_leverage fallback",
                            {
                                "exchange": exchange,
                                "symbol": symbol,
                                "leverage": DEFAULT_MANUAL_LEVERAGE,
                                "params": fallback_params,
                                "fallback_from": leverage_params.get("side"),
                                "fallback_error": str(exc),
                            },
                        )
                    except Exception as fallback_exc:  # pylint: disable=broad-except
                        self._emit_log(
                            log_cb,
                            "warn",
                            "set_leverage failed",
                            {
                                "exchange": exchange,
                                "symbol": symbol,
                                "leverage": DEFAULT_MANUAL_LEVERAGE,
                                "params": fallback_params,
                                "error": str(fallback_exc),
                            },
                        )
                else:
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
            if bitget_uta_enabled():
                params = _bitget_params(params)
                if leg_margin_mode in ("isolated", "cross"):
                    params["marginMode"] = "isolated" if leg_margin_mode == "isolated" else "crossed"
                hedged = await self._resolve_bitget_hedged(client)
                if hedged is True:
                    params["hedged"] = True
                    params["posSide"] = bitget_position_side(
                        str(leg.get("side") or ""),
                        reduce_only=reduce_only,
                    )
            else:
                params["posSide"] = "net"
                params["positionSide"] = "net"
                if leg_margin_mode in ("isolated", "cross"):
                    params["marginMode"] = leg_margin_mode
        if exchange == "kucoin":
            kucoin_margin_mode = str(leg_margin_mode or "isolated").strip().upper()
            if kucoin_margin_mode:
                params["marginMode"] = kucoin_margin_mode
                params["marginType"] = kucoin_margin_mode
            params["leverage"] = int(DEFAULT_MANUAL_LEVERAGE)
            params.setdefault("positionSide", "BOTH")
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
                "constraints": constraints or None,
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
            if exchange == "bitget" and not bitget_uta_enabled() and "40774" in message:
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
            error_type = _classify_submit_error(message)
            if (
                error_type == "tick_size"
                and order_type == "limit"
                and price is not None
                and price_step
                and price_step > 0
            ):
                retry_mode = "down" if str(leg.get("side") or "").lower() == "buy" else "up"
                retry_price = _round_to_step(float(price), float(price_step), mode=retry_mode)
                precise_retry_price = _ccxt_precision_value(client, "price", ccxt_symbol, retry_price)
                if precise_retry_price is not None and precise_retry_price > 0:
                    retry_price = precise_retry_price
                if retry_price and retry_price > 0 and abs(float(retry_price) - float(price)) <= max(float(price_step), 1e-12):
                    try:
                        order = await client.create_order(
                            ccxt_symbol,
                            order_type,
                            leg["side"],
                            order_qty,
                            retry_price,
                            params,
                        )
                        filled = _to_base_qty(_safe_float(order.get("filled")), contract_size)
                        self._emit_log(
                            log_cb,
                            "order",
                            "order submitted after tick-size retry",
                            {
                                "exchange": exchange,
                                "order_id": order.get("id"),
                                "status": order.get("status"),
                                "filled_qty": filled,
                                "avg_price": order.get("average"),
                                "original_price": price,
                                "retry_price": retry_price,
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
                        error_type = _classify_submit_error(message)
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
                    "error_type": error_type,
                },
            )
            return {
                "exchange": exchange,
                "status": "error",
                "error": message,
                "error_type": error_type,
                "ts": _now_iso(),
            }

    async def _resolve_bitget_hedged(self, client: Any) -> bool | None:
        now = time.time()
        cached = self._position_mode_cache.get("bitget")
        if cached and (now - cached[1]) < 30:
            return cached[0]
        hedged: bool | None = None
        try:
            positions = await _fetch_positions_compat(client, "bitget")
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
        if exchange == "bitget":
            return await client.fetch_order(order_id, ccxt_symbol, _bitget_params({}))
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
                cancel_params = _bitget_params({}) if exchange == "bitget" else {}
                await client.cancel_order(order_id, ccxt_symbol, cancel_params)
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
                    exchange,
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
                    exchange,
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
        if not await self._ensure_ws_orders_recovered(exchange, reason="cancel", log_cb=None):
            return
        client = await self._ensure_client(exchange, [])
        if not client:
            return
        ccxt_symbol = await self._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return
        try:
            cancel_params = _bitget_params({}) if exchange == "bitget" else {}
            await client.cancel_order(order_id, ccxt_symbol, cancel_params)
        except Exception:  # pylint: disable=broad-except
            return

    async def _recover_filled_from_trades(
        self,
        client: Any,
        exchange: str,
        ccxt_symbol: str,
        order_id: str,
        contract_size: float | None,
    ) -> dict[str, float | None] | None:
        """Best-effort fill recovery via trades when fetch_order is unavailable."""
        if not hasattr(client, "fetch_my_trades"):
            return None
        params = {"order": order_id}
        if exchange == "bitget":
            params = _bitget_params(params)
        try:
            trades = await client.fetch_my_trades(ccxt_symbol, None, None, params)
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
                            cancel_params = _bitget_params({}) if exchange == "bitget" else {}
                            await client.cancel_order(order_id, ccxt_symbol, cancel_params)
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
                cancel_params = _bitget_params({}) if exchange == "bitget" else {}
                await client.cancel_order(order_id, ccxt_symbol, cancel_params)
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
        hedge_adverse_ticks: float | None,
        hedge_reprice_min_sec: float,
        payload: Mapping[str, Any] | None = None,
        min_qty_required: float | None = None,
        log_cb: Optional[callable] = None,
    ) -> dict[str, Any]:
        payload = payload or {}
        hedge_timeout_sec = max(1.0, _safe_float(payload.get("hedge_timeout_sec")) or 5.0)
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
                        state = str(status.get("status") or "").lower()
                        if state in ("filled", "closed", "finished") and (status.get("filled_qty") or 0.0) <= 0:
                            status = None
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
                    state = str(status.get("status") or "").lower()
                    if state in ("filled", "closed", "finished") and (status.get("filled_qty") or 0.0) <= 0:
                        status = None
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
                favorable_ticks = None
                adverse_ticks = None
                if hedge_adverse_ticks is not None and hedge_adverse_ticks > 0:
                    favorable_ticks, adverse_ticks = _hedge_price_move_ticks(
                        leg["side"], order_price, stats.best_bid, stats.best_ask, price_step
                    )
                now = time.time()
                deadline_triggered = (now - start_time) >= hedge_timeout_sec
                adverse_triggered = False
                if hedge_adverse_ticks is not None and hedge_adverse_ticks > 0:
                    if (
                        adverse_ticks is not None
                        and adverse_ticks >= hedge_adverse_ticks
                        and (now - last_reprice) >= hedge_reprice_min_sec
                    ):
                        adverse_triggered = True
                elif (
                    adverse_bps is not None
                    and (
                        adverse_bps
                        >= max(
                            hedge_adverse_bps,
                            (price_step / order_price * 10_000.0 + 0.01)
                            if price_step and order_price
                            else hedge_adverse_bps,
                        )
                    )
                    and (now - last_reprice) >= hedge_reprice_min_sec
                ):
                    adverse_triggered = True
                if deadline_triggered:
                    adverse_triggered = True
                if adverse_triggered:
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
                    last_reprice = now
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
                    if state not in ("canceled", "cancelled", "closed"):
                        self._emit_log(
                            log_cb,
                            "wait",
                            "hedge cancel pending; skipping market",
                            {
                                "exchange": leg.get("exchange"),
                                "order_id": order_id,
                                "status": cancel_status,
                            },
                        )
                        await asyncio.sleep(max(0.5, hedge_reprice_min_sec))
                        continue
                    market_price_est = (
                        stats.best_ask if leg["side"] == "buy" else stats.best_bid
                    ) or order_price
                    market_notional_est = (
                        remaining * market_price_est if market_price_est and market_price_est > 0 else None
                    )
                    if not self._auto_exit_market_fallback_allowed(
                        payload,
                        leg.get("exchange"),
                        notional_usd=market_notional_est,
                    ):
                        order_id = None
                        order_price = None
                        self._emit_log(
                            log_cb,
                            "reprice",
                            "hedge adverse move on lower-tier venue; repricing limit instead of market",
                            {
                                "exchange": leg.get("exchange"),
                                "adverse_bps": adverse_bps,
                                "adverse_ticks": adverse_ticks,
                                "remaining_qty": remaining,
                                "venue_tier": venue_liquidity_tier(leg.get("exchange")),
                                "market_notional_est": market_notional_est,
                            },
                        )
                        continue
                    self._emit_story(
                        log_cb,
                        (
                            f"Hedge deadline reached; switching to market {leg['exchange']} qty={remaining:g}"
                            if deadline_triggered
                            else f"Hedge adverse move; switching to market {leg['exchange']} qty={remaining:g}"
                        ),
                        {
                            "exchange": leg.get("exchange"),
                            "adverse_bps": adverse_bps,
                            "adverse_ticks": adverse_ticks,
                            "qty": remaining,
                            "deadline_triggered": deadline_triggered,
                            "hedge_timeout_sec": hedge_timeout_sec,
                        },
                    )
                    trigger_reason = "hedge_timeout" if deadline_triggered else "hedge_adverse_bps"
                    submit_message = (
                        f"hedge market {leg['exchange']} qty={remaining:g} reason={trigger_reason} "
                        f"adverse_bps={adverse_bps:.2f}"
                    )
                    if hedge_adverse_ticks is not None and hedge_adverse_ticks > 0 and adverse_ticks is not None:
                        submit_message += f" adverse_ticks={adverse_ticks:.2f}"
                    self._emit_log(log_cb, "submit", submit_message)
                    market_result = await self._place_market(
                        leg, symbol, remaining, {}, reason=trigger_reason, log_cb=log_cb
                    )
                    market_filled = max(0.0, _safe_float(market_result.get("filled_qty")) or 0.0)
                    market_status = None
                    market_order_id = market_result.get("order_id")
                    if market_order_id:
                        market_fill_timeout_sec = max(
                            2.0,
                            hedge_reprice_min_sec,
                            _safe_float(payload.get("market_fill_timeout_sec")) or 0.0,
                        )
                        market_status = await self._await_order_fill(
                            leg,
                            symbol,
                            market_order_id,
                            remaining,
                            market_fill_timeout_sec,
                            log_cb=None,
                        )
                        if market_status and market_status.get("status") != "error":
                            market_filled = max(
                                market_filled,
                                max(0.0, _safe_float(market_status.get("filled_qty")) or 0.0),
                            )
                    total_filled = min(qty, filled_total + market_filled)
                    remaining_after_market = max(0.0, qty - total_filled)
                    final_status = "filled" if remaining_after_market <= 0 else "partial"
                    resolved_avg = (
                        market_result.get("avg_price")
                        if market_filled > 0 and market_result.get("avg_price") is not None
                        else (
                            market_status.get("avg_price")
                            if isinstance(market_status, Mapping) and market_status.get("avg_price") is not None
                            else status.get("avg_price")
                        )
                    )
                    self._emit_order_status(
                        log_cb,
                        exchange=leg["exchange"],
                        label=leg.get("label"),
                        order_id=(
                            (market_status.get("order_id") if isinstance(market_status, Mapping) else None)
                            or market_result.get("order_id")
                            or order_id
                        ),
                        status=final_status,
                        filled_qty=total_filled,
                        avg_price=resolved_avg,
                        source=(
                            market_status.get("source")
                            if isinstance(market_status, Mapping)
                            else market_result.get("source")
                        )
                        or status.get("source"),
                    )
                    result = {
                        "exchange": leg["exchange"],
                        "status": final_status,
                        "order_id": (
                            (market_status.get("order_id") if isinstance(market_status, Mapping) else None)
                            or market_result.get("order_id")
                            or order_id
                        ),
                        "filled_qty": total_filled,
                        "avg_price": resolved_avg,
                        "fallback": market_result,
                    }
                    if isinstance(market_status, Mapping):
                        result["fallback_status"] = {
                            "status": market_status.get("status"),
                            "filled_qty": market_status.get("filled_qty"),
                            "avg_price": market_status.get("avg_price"),
                            "source": market_status.get("source"),
                        }
                    if remaining_after_market > 0:
                        result["pending_qty"] = remaining_after_market
                        result["pending_reason"] = "market_partial"
                    return result
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
                if str(getattr(client, "id", "") or "").lower() == "gate":
                    return await self._resolve_gate_market_symbol(client, symbol, ccxt_symbol)
                return None
            markets = getattr(client, "markets", None) or {}
        exact_market = markets.get(ccxt_symbol) if isinstance(markets, dict) else None
        if isinstance(exact_market, dict):
            market_type = str(exact_market.get("type") or "").lower()
            if exact_market.get("swap") or exact_market.get("future") or market_type in ("swap", "future"):
                if str(getattr(client, "id", "") or "").lower() == "gate":
                    refreshed = await self._resolve_gate_market_symbol(client, symbol, ccxt_symbol)
                    if refreshed:
                        return refreshed
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
                    if str(getattr(client, "id", "") or "").lower() == "gate":
                        refreshed = await self._resolve_gate_market_symbol(client, symbol, ccxt_symbol)
                        if refreshed:
                            return refreshed
                    return market.get("symbol") or ccxt_symbol
            for market in markets.values():
                if market.get("base") == base and market.get("quote") == quote:
                    break
        if str(getattr(client, "id", "") or "").lower() == "gate":
            return await self._resolve_gate_market_symbol(client, symbol, ccxt_symbol)
        return None

    async def _resolve_gate_market_symbol(
        self,
        client: Any,
        symbol: str,
        ccxt_symbol: str,
    ) -> str | None:
        contract_id = _gate_contract_id(symbol or ccxt_symbol)
        if not contract_id:
            return None
        try:
            fetch = getattr(client, "fetch", None)
            if callable(fetch):
                response = await fetch(
                    f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{contract_id}",
                    "GET",
                    {"X-Gate-Size-Decimal": "1"},
                )
            else:
                response = await client.publicFuturesGetSettleContractsContract(
                    {"settle": "usdt", "contract": contract_id}
                )
        except Exception:  # pylint: disable=broad-except
            try:
                response = await client.publicFuturesGetSettleContractsContract(
                    {"settle": "usdt", "contract": contract_id}
                )
            except Exception:  # pylint: disable=broad-except
                return None
        if not isinstance(response, Mapping):
            return None
        market = _gate_market_from_contract(response)
        if not market:
            return None
        symbol_key = str(market.get("symbol") or ccxt_symbol)
        market["symbol"] = symbol_key
        markets = getattr(client, "markets", None)
        if not isinstance(markets, dict):
            client.markets = {}
            markets = client.markets
        markets[symbol_key] = market
        markets_by_id = getattr(client, "markets_by_id", None)
        if not isinstance(markets_by_id, dict):
            client.markets_by_id = {}
            markets_by_id = client.markets_by_id
        markets_by_id[str(market.get("id"))] = [market]
        symbols = getattr(client, "symbols", None)
        if not isinstance(symbols, list):
            client.symbols = []
            symbols = client.symbols
        if symbol_key not in symbols:
            symbols.append(symbol_key)
        return symbol_key

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
        price_limits = limits.get("price") or {}
        info = market.get("info") or {}
        if not isinstance(info, dict):
            info = {}
        raw_min_qty = _safe_float(amount_limits.get("min"))
        min_notional = _safe_float(cost_limits.get("min"))
        price_min = _safe_float(price_limits.get("min"))
        price_max = _safe_float(price_limits.get("max"))
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
        if price_min is None:
            price_min = _safe_float(info.get("minPrice"))
        if price_max is None:
            price_max = _safe_float(info.get("maxPrice"))
        precision = market.get("precision") or {}
        precision_mode = getattr(client, "precisionMode", None)
        amount_step = _precision_to_step(precision.get("amount"), precision_mode)
        price_step = _precision_to_step(precision.get("price"), precision_mode)
        filter_amount_step = _market_filter_value(market, "LOT_SIZE", "stepSize", "qtyStep")
        filter_market_amount_step = _market_filter_value(market, "MARKET_LOT_SIZE", "stepSize", "qtyStep")
        filter_price_step = _market_filter_value(market, "PRICE_FILTER", "tickSize")
        filter_min_qty = _market_filter_value(market, "LOT_SIZE", "minQty", "minQtySize")
        filter_min_notional = _market_filter_value(
            market,
            "MIN_NOTIONAL",
            "notional",
            "minNotional",
        ) or _market_filter_value(
            market,
            "NOTIONAL",
            "minNotional",
            "notional",
        )
        if filter_amount_step is not None:
            amount_step = filter_amount_step
        elif filter_market_amount_step is not None:
            amount_step = filter_market_amount_step
        if filter_price_step is not None:
            price_step = filter_price_step
        if raw_min_qty is None and filter_min_qty is not None:
            raw_min_qty = filter_min_qty
        if min_notional is None and filter_min_notional is not None:
            min_notional = filter_min_notional
        contract_size = _safe_float(market.get("contractSize"))
        min_qty = raw_min_qty
        min_qty_contracts_effective = raw_min_qty
        if contract_size and contract_size > 0:
            is_contract_market = bool(
                market.get("contract")
                or market.get("swap")
                or market.get("future")
                or str(market.get("type") or "").lower() in ("swap", "future")
            )
            if is_contract_market and (min_qty_contracts_effective is None or min_qty_contracts_effective <= 0):
                min_qty_contracts_effective = 1.0
                if amount_step is None or amount_step <= 0 or amount_step < 1.0:
                    amount_step = 1.0
            min_qty = min_qty_contracts_effective
            if min_qty is not None:
                min_qty = min_qty * contract_size
            if amount_step is not None:
                amount_step = amount_step * contract_size
        return {
            "min_qty": min_qty,
            "min_notional": min_notional,
            "amount_step": amount_step,
            "price_step": price_step,
            "price_min": price_min,
            "price_max": price_max,
            "contract_size": contract_size,
            "min_qty_contracts": raw_min_qty,
            "min_qty_contracts_effective": min_qty_contracts_effective,
        }

    def _collect_action_errors(self, actions: Iterable[Mapping[str, Any]]) -> list[str]:
        errors: list[str] = []
        for action in actions:
            if action.get("handled_error"):
                continue
            status = str(action.get("status") or "").lower()
            if status != "error":
                continue
            exchange = action.get("exchange") or "unknown"
            reason = action.get("error") or "unknown error"
            risk_state = str(action.get("risk_state") or "").strip()
            if risk_state:
                reason = f"{reason} ({risk_state})"
            errors.append(f"{exchange}: {reason}")
        return errors

    def _collect_risk_flags(
        self,
        actions: Iterable[Mapping[str, Any]],
        warnings: Iterable[Any] = (),
    ) -> list[str]:
        flags: list[str] = []
        seen: set[str] = set()

        def _add(flag: Any) -> None:
            text = str(flag or "").strip()
            if not text or text in seen:
                return
            seen.add(text)
            flags.append(text)

        for action in actions:
            _add(action.get("risk_state"))
        for warning in warnings:
            if str(warning or "").strip() == "partial_fill_exposure":
                _add(warning)
        return flags

    async def _precheck_reduce_only_qty(
        self,
        client: Any,
        *,
        exchange: str,
        symbol: str,
        ccxt_symbol: str,
        leg: Mapping[str, Any],
        qty_base: float,
        contract_size: float | None,
        log_cb: Optional[callable] = None,
    ) -> str | None:
        if qty_base <= 0 or not hasattr(client, "fetch_positions"):
            return None

        async def _fetch_positions_once() -> Any:
            try:
                return await _fetch_positions_compat(client, exchange, [ccxt_symbol])
            except Exception:
                return await _fetch_positions_compat(client, exchange)

        try:
            positions = await _fetch_positions_once()
        except Exception as exc:  # pylint: disable=broad-except
            self._emit_log(
                log_cb,
                "warn",
                "reduce-only precheck positions fetch failed",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "error": str(exc),
                },
            )
            return None

        expected_side = _exit_position_side(leg)
        if not expected_side:
            return None
        canonical = normalize_symbol(symbol)
        position_qty = 0.0
        for pos in positions or []:
            info = pos.get("info") or {}
            pos_symbol = pos.get("symbol") or pos.get("id") or info.get("symbol") or info.get("instId") or ""
            candidate = normalize_symbol(str(pos_symbol))
            if canonical and not _symbol_matches(canonical, candidate):
                continue
            qty = _safe_float(pos.get("contracts"))
            if qty is None:
                qty = _safe_float(pos.get("amount"))
            if qty is None and isinstance(info, dict):
                qty = _safe_float(info.get("positionAmt"))
            qty_value = _to_base_qty(qty, contract_size)
            pos_side = _normalize_position_side(pos.get("side"), qty_value)
            if pos_side != expected_side:
                continue
            position_qty += abs(qty_value or 0.0)

        qty_tol = 1e-9
        if position_qty <= qty_tol:
            return f"reduce-only no open position for {expected_side}"
        if qty_base - position_qty > max(qty_tol, position_qty * 0.001):
            return f"reduce-only qty {qty_base:g} exceeds open position qty {position_qty:g}"
        return None

    async def _finalize_exit_dust(
        self,
        *,
        symbol: str,
        legs: Iterable[Mapping[str, Any]],
        start_qty_by_exchange: Mapping[str, float],
        requested_exit_qty: float,
        constraints: Mapping[str, Any],
        payload: Mapping[str, Any],
        actions: list[dict[str, Any]],
        warnings: list[str],
        log_cb: Optional[callable] = None,
    ) -> None:
        legs_list = list(legs or [])
        if not symbol or not legs_list or requested_exit_qty <= 0:
            return
        if any(not bool(leg.get("reduce_only")) for leg in legs_list):
            self._emit_log(
                log_cb,
                "info",
                "dust finalize skipped (non-reduce leg present)",
                {"symbol": symbol},
            )
            return
        exchanges = [normalize_exchange_name(str(leg.get("exchange") or "")) for leg in legs_list]
        exchanges = [exchange for exchange in exchanges if exchange]
        if not exchanges:
            return
        dust_notional_usd = _safe_float(payload.get("exit_dust_notional_usd"))
        if dust_notional_usd is None or dust_notional_usd <= 0:
            dust_notional_usd = 10.0
        max_legs = int(_safe_float(payload.get("exit_dust_max_legs")) or 1)
        if max_legs <= 0:
            max_legs = 1
        close_full_pair = bool(payload.get("exit_close_full_pair"))

        positions, pos_errors = await self._fetch_positions_for_symbol(
            exchanges=exchanges,
            symbol=symbol,
            allow_ws=True,
            contract_sizes=self._contract_sizes_from_constraints(constraints),
        )
        if pos_errors:
            await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
            retry_positions, retry_errors = await self._fetch_positions_for_symbol(
                exchanges=exchanges,
                symbol=symbol,
                allow_ws=False,
                contract_sizes=self._contract_sizes_from_constraints(constraints),
            )
            if retry_errors:
                warnings.extend(pos_errors + retry_errors)
                self._emit_log(
                    log_cb,
                    "warn",
                    "dust finalize skipped (positions unavailable)",
                    {"symbol": symbol, "errors": pos_errors + retry_errors},
                )
                return
            positions = retry_positions

        def _mark_price_for_leg(exchange: str, side: str) -> float | None:
            for pos in positions:
                pos_exchange = normalize_exchange_name(str(pos.get("exchange") or ""))
                if pos_exchange != exchange:
                    continue
                pos_symbol = str(pos.get("symbol") or pos.get("symbol_normalized") or "")
                if not _symbol_matches(symbol, pos_symbol):
                    continue
                qty_hint = _safe_float(pos.get("coin_qty"))
                if qty_hint is None:
                    qty_hint = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
                pos_side = _normalize_position_side(pos.get("side"), qty_hint)
                if pos_side != side:
                    continue
                mark = _safe_float(pos.get("mark_price"))
                if mark is None:
                    mark = _safe_float(pos.get("entry_price"))
                if mark and mark > 0:
                    return float(mark)
            return None

        candidates: list[dict[str, Any]] = []
        for leg in legs_list:
            exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
            if not exchange:
                continue
            side = _exit_position_side(leg)
            if not side:
                continue
            start_qty = max(0.0, _safe_float(start_qty_by_exchange.get(exchange)) or 0.0)
            # A 100% pair exit is resolved from the smaller (hedged) leg.  A
            # tiny pre-existing mismatch on the larger leg must therefore not
            # become the desired end position.  It is still closed only below
            # the normal dust/minimum thresholds, so a material orphan is not
            # market-closed silently.
            target_end_qty = (
                0.0
                if close_full_pair
                else max(0.0, start_qty - requested_exit_qty)
            )
            current_qty = self._sum_position_qty(
                positions,
                exchange=exchange,
                side=side,
                symbol=symbol,
            )
            residual_qty = max(0.0, current_qty - target_end_qty)
            if residual_qty <= 0:
                continue
            leg_constraints = constraints.get(exchange) or {}
            min_qty_required = _safe_float(leg_constraints.get("min_qty_required")) or _safe_float(
                leg_constraints.get("min_qty")
            )
            amount_step = _safe_float(leg_constraints.get("amount_step"))
            close_qty = _round_to_step(residual_qty, amount_step, mode="down") if amount_step else residual_qty
            if close_qty <= 0:
                close_qty = residual_qty
            mark_price = _mark_price_for_leg(exchange, side)
            residual_notional = residual_qty * mark_price if mark_price and mark_price > 0 else None
            dust_reasons: list[str] = []
            if min_qty_required and residual_qty < min_qty_required:
                dust_reasons.append("below_min_qty")
            if residual_notional is not None and residual_notional < dust_notional_usd:
                dust_reasons.append("below_dust_notional")
            if not dust_reasons:
                continue
            candidates.append(
                {
                    "leg": dict(leg),
                    "exchange": exchange,
                    "side": side,
                    "target_end_qty": target_end_qty,
                    "current_qty": current_qty,
                    "residual_qty": residual_qty,
                    "close_qty": close_qty,
                    "min_qty_required": min_qty_required,
                    "residual_notional": residual_notional,
                    "dust_reasons": dust_reasons,
                }
            )

        if not candidates:
            return
        candidates.sort(
            key=lambda item: (
                float(item.get("residual_notional"))
                if item.get("residual_notional") is not None
                else float(item.get("residual_qty") or 0.0),
                float(item.get("residual_qty") or 0.0),
            )
        )
        self._emit_log(
            log_cb,
            "dust",
            "dust finalize start",
            {
                "symbol": symbol,
                "requested_exit_qty": requested_exit_qty,
                "close_full_pair": close_full_pair,
                "dust_notional_usd": dust_notional_usd,
                "candidates": [
                    {
                        "exchange": item.get("exchange"),
                        "side": item.get("side"),
                        "current_qty": item.get("current_qty"),
                        "target_end_qty": item.get("target_end_qty"),
                        "residual_qty": item.get("residual_qty"),
                        "residual_notional": item.get("residual_notional"),
                        "reasons": item.get("dust_reasons"),
                    }
                    for item in candidates
                ],
            },
        )

        for item in candidates[:max_legs]:
            leg = item.get("leg") or {}
            exchange = str(item.get("exchange") or "")
            close_qty = _safe_float(item.get("close_qty")) or 0.0
            if close_qty <= 0:
                continue
            residual_notional = _safe_float(item.get("residual_notional"))
            if not self._auto_exit_market_fallback_allowed(
                payload,
                exchange,
                notional_usd=residual_notional,
            ):
                warnings.append(f"{exchange}: dust finalize skipped by auto-exit tier guard")
                self._emit_log(
                    log_cb,
                    "warn",
                    "dust finalize skipped by auto-exit tier guard",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "qty": close_qty,
                        "venue_tier": venue_liquidity_tier(exchange),
                        "market_notional_est": residual_notional,
                    },
                )
                continue
            self._emit_log(
                log_cb,
                "submit",
                f"dust finalize market {exchange} qty={close_qty:g}",
                {
                    "symbol": symbol,
                    "side": item.get("side"),
                    "residual_qty": item.get("residual_qty"),
                    "residual_notional": item.get("residual_notional"),
                    "reasons": item.get("dust_reasons"),
                },
            )
            result = await self._place_market(
                leg,
                symbol,
                close_qty,
                payload,
                reason="exit_dust_finalize",
                log_cb=log_cb,
            )
            actions.append(result)
            if result.get("status") == "error":
                error_text = str(result.get("error") or "unknown_error")
                if _is_min_order_size_error(error_text):
                    warnings.append(f"{exchange}: non-closeable dust {close_qty:g} ({error_text})")
                    self._emit_log(
                        log_cb,
                        "warn",
                        "dust finalize non-closeable",
                        {"exchange": exchange, "symbol": symbol, "qty": close_qty, "error": error_text},
                    )
                else:
                    warnings.append(f"{exchange}: dust finalize failed ({error_text})")
                    self._emit_log(
                        log_cb,
                        "warn",
                        "dust finalize failed",
                        {"exchange": exchange, "symbol": symbol, "qty": close_qty, "error": error_text},
                    )
                continue
            order_id = result.get("order_id")
            filled_qty = _safe_float(result.get("filled_qty")) or 0.0
            if order_id and filled_qty + 1e-9 < close_qty:
                fill_timeout_sec = max(2.0, _safe_float(payload.get("market_fill_timeout_sec")) or 3.0)
                fill = await self._await_order_fill(
                    leg,
                    symbol,
                    order_id,
                    close_qty,
                    fill_timeout_sec,
                    log_cb=None,
                )
                if fill.get("status") != "error":
                    filled_qty = max(filled_qty, _safe_float(fill.get("filled_qty")) or 0.0)
            if filled_qty + 1e-9 < close_qty:
                warnings.append(
                    f"{exchange}: dust finalize partial fill {filled_qty:g}/{close_qty:g}"
                )
                self._emit_log(
                    log_cb,
                    "warn",
                    "dust finalize partial",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "filled_qty": filled_qty,
                        "requested_qty": close_qty,
                    },
                )
            else:
                self._emit_log(
                    log_cb,
                    "result",
                    "dust finalize closed",
                    {"exchange": exchange, "symbol": symbol, "filled_qty": filled_qty},
                )

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

    async def _fetch_positions_with_retry(
        self,
        *,
        exchanges: Iterable[str],
        symbol: str,
        log_cb: Optional[callable] = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        errors: list[str] = []
        positions: list[dict[str, Any]] = []
        for attempt in range(1, PRECHECK_RETRIES + 1):
            positions, errors = await self._fetch_positions_for_symbol(
                exchanges=exchanges,
                symbol=symbol,
                allow_ws=False,
            )
            if not errors:
                return positions, []
            self._emit_log(
                log_cb,
                "warn",
                "positions fetch failed; retrying",
                {"attempt": attempt, "errors": errors},
            )
            if attempt < PRECHECK_RETRIES:
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
        return positions, errors

    async def _fetch_balances_with_retry(
        self,
        *,
        exchanges: Iterable[str],
        log_cb: Optional[callable] = None,
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        errors: list[str] = []
        balances: dict[str, dict[str, Any]] = {}
        exchange_list = [normalize_exchange_name(str(exchange)) for exchange in exchanges if exchange]
        for attempt in range(1, PRECHECK_RETRIES + 1):
            errors = []
            balances = {}
            for exchange in exchange_list:
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
                    balance = await gateway.fetch_balance()
                except Exception as exc:  # pylint: disable=broad-except
                    errors.append(f"{exchange}: balance fetch failed: {exc}")
                    continue
                available = _safe_float(balance.get("available"))
                if available is None:
                    errors.append(f"{exchange}: balance available missing")
                    continue
                balances[exchange] = balance
            if not errors:
                return balances, []
            self._emit_log(
                log_cb,
                "warn",
                "balance fetch failed; retrying",
                {"attempt": attempt, "errors": errors},
            )
            if attempt < PRECHECK_RETRIES:
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
        return balances, errors

    async def _fetch_mark_prices_with_retry(
        self,
        *,
        exchanges: Iterable[str],
        symbol: str,
        log_cb: Optional[callable] = None,
    ) -> tuple[dict[str, float], list[str]]:
        errors: list[str] = []
        prices: dict[str, float] = {}
        exchange_list = [normalize_exchange_name(str(exchange)) for exchange in exchanges if exchange]
        for attempt in range(1, PRECHECK_RETRIES + 1):
            errors = []
            prices = {}
            for exchange in exchange_list:
                client = await self._ensure_client(exchange, errors)
                if not client:
                    continue
                ccxt_symbol = await self._resolve_market_symbol(client, symbol)
                if not ccxt_symbol:
                    errors.append(f"{exchange}: unable to resolve symbol {symbol}")
                    continue
                try:
                    ticker = await client.fetch_ticker(ccxt_symbol)
                except Exception as exc:  # pylint: disable=broad-except
                    errors.append(f"{exchange}: mark price fetch failed: {exc}")
                    continue
                mark = _safe_float(ticker.get("mark")) or _safe_float(ticker.get("markPrice"))
                if not mark:
                    mark = _safe_float(ticker.get("last")) or _safe_float(ticker.get("close"))
                    if mark:
                        self._emit_log(
                            log_cb,
                            "warn",
                            f"{exchange}: mark unavailable; using last",
                            {"exchange": exchange, "price": mark},
                        )
                    else:
                        errors.append(f"{exchange}: mark price unavailable")
                        continue
                prices[exchange] = float(mark)
            if not errors:
                return prices, []
            self._emit_log(
                log_cb,
                "warn",
                "mark price fetch failed; retrying",
                {"attempt": attempt, "errors": errors},
            )
            if attempt < PRECHECK_RETRIES:
                await asyncio.sleep(PRECHECK_RETRY_DELAY_SEC)
        return prices, errors

    def _min_qty_and_step(self, plan: Mapping[str, Any]) -> tuple[float, float | None]:
        constraints = plan.get("market_constraints") or {}
        min_qtys: list[float] = []
        steps: list[float] = []
        for leg in plan.get("legs") or []:
            exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
            data = constraints.get(exchange) or {}
            min_qty = _safe_float(data.get("min_qty_required")) or _safe_float(data.get("min_qty"))
            if min_qty:
                min_qtys.append(float(min_qty))
            step = _safe_float(data.get("amount_step"))
            if step:
                steps.append(float(step))
        min_qty_val = max(min_qtys) if min_qtys else 0.0
        step_val = max(steps) if steps else None
        return min_qty_val, step_val

    def _adjust_payload_qty(
        self,
        payload: Mapping[str, Any],
        plan: Mapping[str, Any],
        *,
        new_qty: float,
        log_cb: Optional[callable] = None,
        reason: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        updated_payload = dict(payload)
        updated_plan = dict(plan)
        old_qty = _safe_float(plan.get("qty")) or _safe_float(payload.get("qty")) or 0.0
        updated_payload["qty"] = new_qty
        updated_plan["qty"] = new_qty
        ratio = new_qty / old_qty if old_qty > 0 else 1.0
        min_qty, step = self._min_qty_and_step(plan)
        chunk_qty = _safe_float(payload.get("chunk_qty"))
        if chunk_qty is not None and chunk_qty > 0:
            adjusted = chunk_qty * ratio
            if min_qty:
                adjusted = max(min_qty, adjusted)
            if step:
                adjusted = _round_to_step(adjusted, step, mode="down")
            updated_payload["chunk_qty"] = adjusted
        chunk_notional = _safe_float(payload.get("chunk_notional"))
        if chunk_notional is not None and chunk_notional > 0:
            updated_payload["chunk_notional"] = chunk_notional * ratio
        self._emit_log(
            log_cb,
            "precheck",
            f"qty adjusted: {old_qty:g} -> {new_qty:g} ({reason})",
            {
                "old_qty": old_qty,
                "new_qty": new_qty,
                "reason": reason,
                "ratio": ratio,
                "min_qty": min_qty or None,
                "amount_step": step,
            },
        )
        updated_plan.setdefault("warnings", []).append(f"qty adjusted: {reason}")
        return updated_payload, updated_plan

    async def _fetch_positions_for_symbol(
        self,
        *,
        exchanges: Iterable[str],
        symbol: str,
        allow_ws: bool = True,
        contract_sizes: Mapping[str, float | None] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        canonical = _normalize_manual_symbol(symbol)
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
                pos_symbol = _normalize_manual_symbol(
                    pos.get("symbol") or pos.get("symbol_normalized") or ""
                )
                if canonical and not _symbol_matches(canonical, pos_symbol):
                    continue
                qty = _safe_float(pos.get("coin_qty"))
                if qty is None:
                    qty = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
                positions.append(
                    {
                        "exchange": normalize_exchange_name(str(exchange)),
                        "symbol": pos.get("symbol") or pos.get("symbol_normalized"),
                        "side": _normalize_position_side(pos.get("side"), qty) or str(pos.get("side") or "").lower(),
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
        canonical = _normalize_manual_symbol(symbol)
        exchange = normalize_exchange_name(exchange)
        side = str(side or "").lower()
        total = 0.0
        for pos in positions:
            if normalize_exchange_name(str(pos.get("exchange") or "")) != exchange:
                continue
            pos_symbol = _normalize_manual_symbol(
                pos.get("symbol") or pos.get("symbol_normalized") or ""
            )
            if canonical and not _symbol_matches(canonical, pos_symbol):
                continue
            qty = _safe_float(pos.get("coin_qty"))
            if qty is None:
                qty = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
            pos_side = _normalize_position_side(pos.get("side"), qty)
            if side and pos_side != side:
                continue
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
        canonical = _normalize_manual_symbol(symbol)
        if not canonical:
            return None

        def _sum_qty(exchange: str, side: str) -> float:
            total = 0.0
            for pos in positions:
                exch = str(pos.get("exchange") or "").lower()
                if exch != exchange:
                    continue
                sym = _normalize_manual_symbol(pos.get("symbol") or pos.get("symbol_normalized") or "")
                if canonical and not _symbol_matches(canonical, sym):
                    continue
                qty = _safe_float(pos.get("coin_qty"))
                if qty is None:
                    qty = _safe_float(pos.get("contracts")) or _safe_float(pos.get("amount"))
                pos_side = _normalize_position_side(pos.get("side"), qty)
                if pos_side != side:
                    continue
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
            if explicit in leg_by_label:
                return str(explicit)
            if explicit == "long" and "to" in leg_by_label:
                return "to"
            if explicit == "short" and "from" in leg_by_label:
                return "from"
        if explicit in ("auto", None, ""):
            return None
        return None

    def _resolve_primary_hedge_legs(
        self,
        *,
        explicit: Any,
        plan: Mapping[str, Any],
        legs: list[dict[str, Any]],
    ) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
        leg_by_label = {leg["label"]: leg for leg in legs}
        expensive_label = self._resolve_expensive_label(explicit, leg_by_label)
        if not expensive_label:
            suggested = (plan.get("suggested_expensive_leg") or {}).get("suggested_leg")
            expensive_label = self._resolve_expensive_label(suggested, leg_by_label)
        if not expensive_label:
            if "long" in leg_by_label:
                expensive_label = "long"
            elif "to" in leg_by_label:
                expensive_label = "to"
            elif legs:
                expensive_label = str(legs[0].get("label") or "")
        primary_leg = leg_by_label.get(expensive_label) if expensive_label else None
        hedge_leg = next((leg for leg in legs if leg is not primary_leg), None)
        return expensive_label, primary_leg, hedge_leg

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

    def _plan_with_runtime_errors(
        self,
        plan: Mapping[str, Any],
        errors: Iterable[str],
    ) -> dict[str, Any]:
        result = dict(plan)
        result["dry_run"] = False
        result["errors"] = list(errors)
        result["warnings"] = list(plan.get("warnings") or [])
        result["generated_at"] = datetime.now(timezone.utc).isoformat()
        return result

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
