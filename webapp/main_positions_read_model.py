from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from execution.accounts import _safe_float, normalize_symbol
from exchanges import normalize_exchange_name

def _select_position_pair_from_legs(
    legs: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    long_legs: list[dict[str, Any]] = []
    short_legs: list[dict[str, Any]] = []
    for leg in legs or []:
        side = str(leg.get("side") or "").lower()
        if side not in ("long", "short"):
            continue
        exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
        qty = abs(_safe_float(leg.get("quantity")) or 0.0)
        if not exchange or qty <= 0:
            continue
        item = {
            "side": side,
            "exchange": exchange,
            "qty": float(qty),
            "raw": dict(leg),
        }
        if side == "long":
            long_legs.append(item)
        else:
            short_legs.append(item)
    if not long_legs or not short_legs:
        return None

    mode = "single_pair"
    selected_min_side = None
    selected_min_exchange = None
    selected_min_qty = None

    if len(long_legs) == 1 and len(short_legs) == 1:
        selected_long = long_legs[0]
        selected_short = short_legs[0]
    else:
        mode = "multileg_min_leg"
        all_legs = long_legs + short_legs
        min_leg = min(
            all_legs,
            key=lambda item: (float(item.get("qty") or 0.0), str(item.get("exchange") or ""), str(item.get("side") or "")),
        )
        selected_min_side = str(min_leg.get("side") or "")
        selected_min_exchange = str(min_leg.get("exchange") or "")
        selected_min_qty = float(min_leg.get("qty") or 0.0)
        if selected_min_side == "long":
            selected_long = min_leg
            selected_short = max(short_legs, key=lambda item: (float(item.get("qty") or 0.0), str(item.get("exchange") or "")))
        else:
            selected_short = min_leg
            selected_long = max(long_legs, key=lambda item: (float(item.get("qty") or 0.0), str(item.get("exchange") or "")))

    qty = min(float(selected_long.get("qty") or 0.0), float(selected_short.get("qty") or 0.0))
    if qty <= 0:
        return None

    return {
        "mode": mode,
        "long_legs": len(long_legs),
        "short_legs": len(short_legs),
        "long_exchange": str(selected_long.get("exchange") or ""),
        "short_exchange": str(selected_short.get("exchange") or ""),
        "long_qty": float(selected_long.get("qty") or 0.0),
        "short_qty": float(selected_short.get("qty") or 0.0),
        "qty": float(qty),
        "long_leg": dict(selected_long.get("raw") or {}),
        "short_leg": dict(selected_short.get("raw") or {}),
        "selected_min_side": selected_min_side,
        "selected_min_exchange": selected_min_exchange,
        "selected_min_qty": selected_min_qty,
    }

def build_main_positions_payload(
    *,
    status: str,
    accounts_snapshot: Mapping[str, Any],
    balances: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    grouped: Mapping[str, list[dict[str, Any]]],
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now_utc = now_utc or datetime.now(timezone.utc)
    def _parse_iso(value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        try:
            return datetime.fromisoformat(str(value)).astimezone(timezone.utc)
        except Exception:  # pylint: disable=broad-except
            return None

    def _minutes_to(value: Any) -> float | None:
        dt = _parse_iso(value)
        if dt is None:
            return None
        return round((dt - now_utc).total_seconds() / 60.0, 2)

    def _weighted_avg(items: list[Mapping[str, Any]], key: str) -> float | None:
        total_weight = 0.0
        total_value = 0.0
        for item in items:
            value = _safe_float(item.get(key))
            weight = abs(_safe_float(item.get("quantity")) or 0.0)
            if value is None or weight <= 0:
                continue
            total_weight += weight
            total_value += value * weight
        if total_weight <= 0:
            return None
        return total_value / total_weight

    def _pair_amount_usdt(
        longs: list[Mapping[str, Any]],
        shorts: list[Mapping[str, Any]],
        key: str = "current_notional",
    ) -> float | None:
        long_total = sum(abs(_safe_float(item.get(key)) or 0.0) for item in longs)
        short_total = sum(abs(_safe_float(item.get(key)) or 0.0) for item in shorts)
        if long_total > 0 and short_total > 0:
            return min(long_total, short_total)
        gross = long_total + short_total
        return gross if gross > 0 else None

    def _pair_label(summary_row: Mapping[str, Any], selected_pair: Mapping[str, Any] | None) -> str:
        long_exchange = normalize_exchange_name(str(summary_row.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(summary_row.get("short_exchange") or ""))
        long_count = int(summary_row.get("long_legs_count") or 0)
        short_count = int(summary_row.get("short_legs_count") or 0)
        if long_exchange and short_exchange and long_count == 1 and short_count == 1:
            return f"{long_exchange.upper()} / {short_exchange.upper()}"
        if selected_pair:
            long_sel = normalize_exchange_name(str(selected_pair.get("long_exchange") or ""))
            short_sel = normalize_exchange_name(str(selected_pair.get("short_exchange") or ""))
            if long_sel and short_sel:
                return f"{long_sel.upper()} / {short_sel.upper()} ({str(selected_pair.get('mode') or 'pair')})"
        return "multi-leg"


    cards: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("type") or "") != "summary":
            continue
        symbol = normalize_symbol(str(row.get("symbol") or ""))
        legs = [dict(item) for item in (grouped.get(symbol) or [])]
        longs = [leg for leg in legs if str(leg.get("side") or "").lower() == "long"]
        shorts = [leg for leg in legs if str(leg.get("side") or "").lower() == "short"]
        selected_pair = _select_position_pair_from_legs(legs)
        next_funding_iso = row.get("next_funding")
        minutes_to_next = _minutes_to(next_funding_iso)
        liq_distances = [
            abs(_safe_float(leg.get("dist_to_liq_pct")) or 0.0)
            for leg in legs
            if _safe_float(leg.get("dist_to_liq_pct")) is not None
        ]
        liq_distance_pct = min(liq_distances) if liq_distances else None
        quantity_abs = max(
            [abs(_safe_float(leg.get("quantity")) or 0.0) for leg in legs],
            default=0.0,
        )
        pair_amount = _pair_amount_usdt(longs, shorts)
        pair_entry_amount = _pair_amount_usdt(longs, shorts, key="entry_notional")
        selected_long_exchange = normalize_exchange_name(
            str((selected_pair or {}).get("long_exchange") or row.get("long_exchange") or "")
        )
        selected_short_exchange = normalize_exchange_name(
            str((selected_pair or {}).get("short_exchange") or row.get("short_exchange") or "")
        )
        long_quantity = float((selected_pair or {}).get("long_qty") or 0.0)
        short_quantity = float((selected_pair or {}).get("short_qty") or 0.0)
        hedged_quantity = float((selected_pair or {}).get("qty") or 0.0)
        imbalance_quantity = abs(long_quantity - short_quantity)
        imbalance_pct = (
            imbalance_quantity / hedged_quantity * 100.0
            if hedged_quantity > 0
            else None
        )
        long_leverage = _weighted_avg(longs, "leverage")
        short_leverage = _weighted_avg(shorts, "leverage")
        cards.append(
            {
                "symbol": symbol,
                "pair_label": _pair_label(row, selected_pair),
                "is_multi_leg": bool(selected_pair and str(selected_pair.get("mode") or "") != "single_pair"),
                "long_exchange": selected_long_exchange or None,
                "short_exchange": selected_short_exchange or None,
                "net_pnl": _safe_float(row.get("unrealized_pnl")),
                "expected_funding": _safe_float(row.get("expected_funding")),
                "live_spread_pct": _safe_float(row.get("mark_price")),
                "next_funding": next_funding_iso,
                "minutes_to_next_funding": minutes_to_next,
                "liq_distance_pct": liq_distance_pct,
                "risk_level": (
                    "high"
                    if liq_distance_pct is not None and liq_distance_pct <= 10.0
                    else "warn"
                    if liq_distance_pct is not None and liq_distance_pct <= 20.0
                    else "ok"
                ),
                "flags": {
                    "risk": bool(liq_distance_pct is not None and liq_distance_pct <= 20.0),
                    "funding_soon": bool(minutes_to_next is not None and minutes_to_next <= 120.0),
                },
                "position_summary": {
                    "quantity": quantity_abs if quantity_abs > 0 else None,
                    "long_quantity": long_quantity if long_quantity > 0 else None,
                    "short_quantity": short_quantity if short_quantity > 0 else None,
                    "hedged_quantity": hedged_quantity if hedged_quantity > 0 else None,
                    "imbalance_quantity": imbalance_quantity,
                    "imbalance_pct": imbalance_pct,
                    "amount_usdt": pair_amount,
                    "gross_amount_usdt": sum(
                        abs(_safe_float(leg.get("current_notional")) or 0.0) for leg in legs
                    ) or None,
                    "current_exposure_usdt": pair_amount,
                    "gross_current_exposure_usdt": sum(
                        abs(_safe_float(leg.get("current_notional")) or 0.0) for leg in legs
                    ) or None,
                    "entry_exposure_usdt": pair_entry_amount,
                    "gross_entry_exposure_usdt": sum(
                        abs(_safe_float(leg.get("entry_notional")) or 0.0) for leg in legs
                    ) or None,
                    "pair_entry_spread_pct": _safe_float(row.get("entry_price")),
                    "pair_mark_spread_pct": _safe_float(row.get("mark_price")),
                    "long_entry_avg": _safe_float(row.get("long_entry_avg")),
                    "short_entry_avg": _safe_float(row.get("short_entry_avg")),
                    "long_mark_avg": _safe_float(row.get("long_mark_avg")),
                    "short_mark_avg": _safe_float(row.get("short_mark_avg")),
                    "long_leverage_avg": long_leverage,
                    "short_leverage_avg": short_leverage,
                },
                "risk": {
                    "liq_distance_pct": liq_distance_pct,
                    "long_liq_price": _safe_float((longs[0] if longs else {}).get("liquidation_price")),
                    "short_liq_price": _safe_float((shorts[0] if shorts else {}).get("liquidation_price")),
                    "long_stop_price": _safe_float((longs[0] if longs else {}).get("stop_price")),
                    "short_stop_price": _safe_float((shorts[0] if shorts else {}).get("stop_price")),
                    "long_take_price": _safe_float((longs[0] if longs else {}).get("take_price")),
                    "short_take_price": _safe_float((shorts[0] if shorts else {}).get("take_price")),
                },
                "funding": {
                    "net_funding_rate": _safe_float(row.get("funding_rate")),
                    "expected_funding": _safe_float(row.get("expected_funding")),
                    "next_funding": next_funding_iso,
                    "minutes_to_next_funding": minutes_to_next,
                },
                "legs": legs,
            }
        )

    cards.sort(
        key=lambda item: (
            _minutes_to(item.get("next_funding")) if item.get("next_funding") else 10**9,
            str(item.get("symbol") or ""),
        )
    )
    return {
        "status": status if status != "idle" else ("ready" if accounts_snapshot.get("last_updated") else "idle"),
        "last_updated": accounts_snapshot.get("last_updated"),
        "account_last_updated": accounts_snapshot.get("last_updated"),
        "balances": balances,
        "cards": cards,
        "filters": {
            "all": len(cards),
            "risk": sum(1 for card in cards if bool((card.get("flags") or {}).get("risk"))),
            "funding_soon": sum(1 for card in cards if bool((card.get("flags") or {}).get("funding_soon"))),
        },
    }

def compact_account_balances(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in rows:
        exchange = normalize_exchange_name(str(row.get("exchange") or ""))
        if not exchange:
            continue
        total = _safe_float(row.get("total"))
        available = _safe_float(row.get("available"))
        used = _safe_float(row.get("used"))
        margin_ratio = _safe_float(row.get("margin_ratio"))
        equity = _safe_float(row.get("equity"))
        if total is None and equity is not None:
            total = equity
        if used is None and total is not None and available is not None:
            used = max(0.0, float(total) - float(available))
        if margin_ratio is None and total and used is not None and total > 0:
            margin_ratio = float(used) / float(total)
        row_status = str(row.get("status") or "").strip().lower()
        if row_status in {"error", "partial", "unavailable", "missing_credentials"}:
            status = row_status
        elif margin_ratio is None:
            status = "unknown"
        elif margin_ratio >= 0.8:
            status = "stress"
        elif margin_ratio >= 0.6:
            status = "watch"
        else:
            status = "ok"
        compact.append(
            {
                "exchange": exchange,
                "asset": row.get("asset") or row.get("currency") or "USDT",
                "total": total,
                "available": available,
                "used": used,
                "margin_ratio": margin_ratio,
                "status": status,
                "error": row.get("error"),
                "updated_at": row.get("updated_at") or row.get("timestamp"),
            }
        )
    compact.sort(key=lambda item: str(item.get("exchange") or ""))
    return compact
