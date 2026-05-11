from __future__ import annotations

import math
from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _spread_pct(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    mid = (left + right) / 2.0
    if abs(mid) <= 1e-12:
        return None
    return (left - right) / mid * 100.0


def _snapshot_mid(snapshot: Mapping[str, Any]) -> float | None:
    bid = _safe_float(snapshot.get("bid"))
    ask = _safe_float(snapshot.get("ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return _safe_float(snapshot.get("mid"))


def compute_derived_spread_table(
    left_snapshot: Mapping[str, Any],
    right_snapshot: Mapping[str, Any],
) -> dict[str, float | None]:
    bid_a = _safe_float(left_snapshot.get("bid"))
    ask_a = _safe_float(left_snapshot.get("ask"))
    bid_b = _safe_float(right_snapshot.get("bid"))
    ask_b = _safe_float(right_snapshot.get("ask"))
    mid_a = _snapshot_mid(left_snapshot)
    mid_b = _snapshot_mid(right_snapshot)
    mark_a = _safe_float(left_snapshot.get("mark_price"))
    mark_b = _safe_float(right_snapshot.get("mark_price"))
    index_a = _safe_float(left_snapshot.get("index_price"))
    index_b = _safe_float(right_snapshot.get("index_price"))

    premium_a = None
    if mark_a is not None and index_a is not None and abs(index_a) > 1e-12:
        premium_a = (mark_a - index_a) / index_a * 100.0
    premium_b = None
    if mark_b is not None and index_b is not None and abs(index_b) > 1e-12:
        premium_b = (mark_b - index_b) / index_b * 100.0

    premium_diff = None
    if premium_a is not None and premium_b is not None:
        premium_diff = premium_a - premium_b

    return {
        "mid_spread_pct": _spread_pct(mid_a, mid_b),
        "mark_spread_pct": _spread_pct(mark_a, mark_b),
        "index_spread_pct": _spread_pct(index_a, index_b),
        "open_spread_long_a_short_b_pct": _spread_pct(ask_a, bid_b),
        "open_spread_long_b_short_a_pct": _spread_pct(ask_b, bid_a),
        "close_spread_long_a_short_b_pct": _spread_pct(bid_a, ask_b),
        "close_spread_long_b_short_a_pct": _spread_pct(bid_b, ask_a),
        "premium_a_pct": premium_a,
        "premium_b_pct": premium_b,
        "premium_diff_pct": premium_diff,
    }


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if percentile <= 0:
        return min(values)
    if percentile >= 100:
        return max(values)
    ordered = sorted(values)
    rank = (len(ordered) - 1) * (percentile / 100.0)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return ordered[low]
    fraction = rank - low
    return ordered[low] * (1.0 - fraction) + ordered[high] * fraction


def _values_in_window(
    series: list[Mapping[str, Any]],
    *,
    now_ts_ms: int,
    hours: float,
) -> list[float]:
    out: list[float] = []
    if hours <= 0:
        return out
    cutoff = now_ts_ms - int(hours * 3600 * 1000)
    for item in series:
        ts = _safe_float(item.get("ts_ms"))
        val = _safe_float(item.get("spread_pct"))
        if ts is None or val is None:
            continue
        if int(ts) >= cutoff:
            out.append(val)
    return out


def _z_score(current: float | None, values: list[float]) -> float | None:
    if current is None or len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((val - mean) ** 2 for val in values) / len(values)
    std = math.sqrt(max(0.0, variance))
    if std <= 1e-12:
        return None
    return (current - mean) / std


def _lookup_delta(
    series: list[Mapping[str, Any]],
    *,
    now_ts_ms: int,
    minutes_back: int,
    current_value: float | None,
) -> float | None:
    if current_value is None or minutes_back <= 0:
        return None
    target_ts = now_ts_ms - minutes_back * 60 * 1000
    candidate_val = None
    best_ts = -1
    for item in series:
        ts = _safe_float(item.get("ts_ms"))
        val = _safe_float(item.get("spread_pct"))
        if ts is None or val is None:
            continue
        ts_int = int(ts)
        if ts_int <= target_ts and ts_int > best_ts:
            best_ts = ts_int
            candidate_val = val
    if candidate_val is None:
        return None
    return current_value - candidate_val


def compute_spread_window_features(
    series: list[Mapping[str, Any]],
    *,
    now_ts_ms: int,
) -> dict[str, float | None]:
    values_all = [
        _safe_float(item.get("spread_pct"))
        for item in series
        if _safe_float(item.get("spread_pct")) is not None
    ]
    values = [float(v) for v in values_all if v is not None]
    current = values[0] if values else None
    win_1h = _values_in_window(series, now_ts_ms=now_ts_ms, hours=1.0)
    win_4h = _values_in_window(series, now_ts_ms=now_ts_ms, hours=4.0)
    win_24h = _values_in_window(series, now_ts_ms=now_ts_ms, hours=24.0)
    win_7d = _values_in_window(series, now_ts_ms=now_ts_ms, hours=24.0 * 7.0)
    vel_1m = _lookup_delta(series, now_ts_ms=now_ts_ms, minutes_back=1, current_value=current)
    vel_5m = _lookup_delta(series, now_ts_ms=now_ts_ms, minutes_back=5, current_value=current)
    vel_15m = _lookup_delta(series, now_ts_ms=now_ts_ms, minutes_back=15, current_value=current)
    accel_1m_5m = None
    if vel_1m is not None and vel_5m is not None:
        accel_1m_5m = vel_1m - vel_5m
    return {
        "current_spread_pct": current,
        "spread_zscore_1h": _z_score(current, win_1h),
        "spread_zscore_4h": _z_score(current, win_4h),
        "spread_zscore_24h": _z_score(current, win_24h),
        "spread_percentile_24h": _percentile(win_24h, 50.0),
        "spread_percentile_7d": _percentile(win_7d, 50.0),
        "spread_velocity_1m": vel_1m,
        "spread_velocity_5m": vel_5m,
        "spread_velocity_15m": vel_15m,
        "spread_acceleration_1m_5m": accel_1m_5m,
        "spread_points_total": float(len(values)),
        "spread_points_24h": float(len(win_24h)),
    }

