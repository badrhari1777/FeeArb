from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from .spread import compute_derived_spread_table, compute_spread_window_features


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_to_ts_ms(value: Any) -> int | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except Exception:  # pylint: disable=broad-except
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _hours_to_next_funding(snapshot: Mapping[str, Any], now_ts_ms: int) -> float | None:
    ts_ms = _parse_iso_to_ts_ms(snapshot.get("next_funding_time"))
    if ts_ms is None:
        return None
    delta_ms = ts_ms - now_ts_ms
    return max(0.0, delta_ms / 3600_000.0)


def _oi_change_pct(history: list[Mapping[str, Any]], hours: int, now_ts_ms: int) -> float | None:
    if not history:
        return None
    points: list[tuple[int, float]] = []
    for item in history:
        ts = _safe_float(item.get("ts_ms"))
        val = _safe_float(
            item.get("open_interest_notional")
            or item.get("oi_notional")
            or item.get("openInterestValue")
        )
        if ts is None or val is None:
            continue
        points.append((int(ts), float(val)))
    if len(points) < 2:
        return None
    points.sort(key=lambda row: row[0], reverse=True)
    latest_ts, latest = points[0]
    if latest <= 1e-12:
        return None
    target_ts = latest_ts - hours * 3600 * 1000
    anchor = None
    for ts, value in points:
        if ts <= target_ts:
            anchor = value
            break
    if anchor is None:
        # Not enough range, choose oldest available point as soft proxy.
        anchor = points[-1][1]
    if anchor <= 1e-12:
        return None
    return (latest - anchor) / anchor * 100.0


def _decision_phase(hours_to_next: float | None) -> str:
    if hours_to_next is None:
        return "exploratory"
    minutes = hours_to_next * 60.0
    if minutes <= 1.0:
        return "boundary_immediate"
    if minutes <= 15.0:
        return "pre_boundary_15m"
    if minutes <= 20.0:
        return "pre_boundary_20m"
    return "mid_interval"


def _direction_scores(
    *,
    direction: str,
    open_spread_pct: float | None,
    close_spread_pct: float | None,
    net_funding_hourly: float | None,
    hours_to_next: float | None,
    spread_z_1h: float | None,
    spread_velocity_5m: float | None,
    premium_diff_pct: float | None,
    oi_divergence_6h_pct: float | None,
    coverage_pct: float,
) -> dict[str, Any]:
    reversion_potential = None
    if open_spread_pct is not None:
        # Favor entries where we buy cheaper exchange and short richer exchange.
        reversion_potential = -open_spread_pct
    funding_to_next = None
    if net_funding_hourly is not None and hours_to_next is not None:
        funding_to_next = net_funding_hourly * hours_to_next

    entry_score = 50.0
    continuation_risk = 35.0
    reasons: list[str] = []

    if coverage_pct < 70.0:
        entry_score -= 15.0
        continuation_risk += 10.0
        reasons.append("spread_history_low_coverage")

    if reversion_potential is not None:
        entry_score += max(-20.0, min(25.0, reversion_potential * 30.0))
        if reversion_potential > 0.10:
            reasons.append("spread_reversion_favorable")
        elif reversion_potential < -0.10:
            reasons.append("spread_not_attractive")
    else:
        entry_score -= 10.0
        reasons.append("spread_not_attractive")

    if funding_to_next is not None:
        entry_score += max(-20.0, min(20.0, funding_to_next * 100000.0))
        if funding_to_next < 0:
            reasons.append("funding_edge_negative")
    else:
        entry_score -= 8.0
        reasons.append("funding_edge_weak")

    if spread_z_1h is not None and abs(spread_z_1h) > 2.8:
        continuation_risk += 15.0
        reasons.append("spread_continuation_risk_high")
    if spread_velocity_5m is not None:
        # If velocity moves away from reversion direction, risk is higher.
        if reversion_potential is not None and (reversion_potential * spread_velocity_5m) < 0:
            continuation_risk += 8.0
    if premium_diff_pct is not None and abs(premium_diff_pct) >= 0.25:
        continuation_risk += 8.0
        reasons.append("premium_stress")
    if oi_divergence_6h_pct is not None and oi_divergence_6h_pct >= 25.0:
        continuation_risk += 10.0
        reasons.append("oi_divergence_high")

    continuation_risk = max(0.0, min(100.0, continuation_risk))
    entry_score = max(0.0, min(100.0, entry_score - (continuation_risk - 35.0) * 0.2))

    action = "NO_TRADE"
    if entry_score >= 70.0:
        action = "ENTRY_STRONG"
    elif entry_score >= 50.0:
        action = "ENTRY_SMALL"

    return {
        "direction": direction,
        "action": action,
        "reasons": sorted(set(reasons)),
        "scores": {
            "entry_score": round(entry_score, 2),
            "continuation_risk_score": round(continuation_risk, 2),
            "reversion_score": round(max(0.0, min(100.0, 100.0 - continuation_risk)), 2),
            "spread_profit_to_next_funding_ratio": (
                round(reversion_potential / funding_to_next, 4)
                if reversion_potential is not None
                and funding_to_next is not None
                and abs(funding_to_next) > 1e-12
                else None
            ),
        },
        "directional": {
            "open_spread_pct": open_spread_pct,
            "close_spread_pct": close_spread_pct,
            "reversion_potential_pct": reversion_potential,
            "net_funding_hourly": net_funding_hourly,
            "funding_to_next_pct": funding_to_next,
        },
    }


def build_pair_feature_snapshots(
    *,
    pair_key: str,
    canonical_symbol: str,
    left_exchange: str,
    right_exchange: str,
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    spread_series: list[Mapping[str, Any]],
    coverage_pct: float,
    now_ts_ms: int,
) -> dict[str, Any]:
    left_snapshot = dict(left.get("snapshot") or {})
    right_snapshot = dict(right.get("snapshot") or {})
    derived = compute_derived_spread_table(left_snapshot, right_snapshot)
    spread_features = compute_spread_window_features(spread_series, now_ts_ms=now_ts_ms)

    left_interval = _safe_float(left.get("funding_interval_hours_resolved"))
    right_interval = _safe_float(right.get("funding_interval_hours_resolved"))
    left_rate = _safe_float(left.get("latest_funding_rate"))
    right_rate = _safe_float(right.get("latest_funding_rate"))
    left_hourly = (left_rate / left_interval) if left_rate is not None and left_interval else None
    right_hourly = (right_rate / right_interval) if right_rate is not None and right_interval else None
    funding_net_a_b = (
        right_hourly - left_hourly
        if left_hourly is not None and right_hourly is not None
        else None
    )
    funding_net_b_a = (
        left_hourly - right_hourly
        if left_hourly is not None and right_hourly is not None
        else None
    )

    left_h_to_funding = _hours_to_next_funding(left_snapshot, now_ts_ms)
    right_h_to_funding = _hours_to_next_funding(right_snapshot, now_ts_ms)
    hours_to_next = None
    candidates = [value for value in (left_h_to_funding, right_h_to_funding) if value is not None]
    if candidates:
        hours_to_next = min(candidates)

    oi_left = list((left.get("open_interest") or {}).get("history") or [])
    oi_right = list((right.get("open_interest") or {}).get("history") or [])
    oi_left_6h = _oi_change_pct(oi_left, 6, now_ts_ms)
    oi_right_6h = _oi_change_pct(oi_right, 6, now_ts_ms)
    oi_divergence = None
    if oi_left_6h is not None and oi_right_6h is not None:
        oi_divergence = abs(oi_left_6h - oi_right_6h)

    common = {
        "pair_key": pair_key,
        "canonical_symbol": canonical_symbol,
        "left_exchange": left_exchange,
        "right_exchange": right_exchange,
        "decision_phase": _decision_phase(hours_to_next),
        "hours_to_next_funding_min": hours_to_next,
        "derived_spread": derived,
        "spread_features": spread_features,
        "funding": {
            "left_hourly": left_hourly,
            "right_hourly": right_hourly,
            "left_interval_hours": left_interval,
            "right_interval_hours": right_interval,
            "left_rate": left_rate,
            "right_rate": right_rate,
            "time_to_next_funding_hours_left": left_h_to_funding,
            "time_to_next_funding_hours_right": right_h_to_funding,
        },
        "oi": {
            "left_change_6h_pct": oi_left_6h,
            "right_change_6h_pct": oi_right_6h,
            "divergence_6h_pct": oi_divergence,
        },
    }

    dir_a_b = _direction_scores(
        direction="long_a_short_b",
        open_spread_pct=derived.get("open_spread_long_a_short_b_pct"),
        close_spread_pct=derived.get("close_spread_long_a_short_b_pct"),
        net_funding_hourly=funding_net_a_b,
        hours_to_next=hours_to_next,
        spread_z_1h=spread_features.get("spread_zscore_1h"),
        spread_velocity_5m=spread_features.get("spread_velocity_5m"),
        premium_diff_pct=derived.get("premium_diff_pct"),
        oi_divergence_6h_pct=oi_divergence,
        coverage_pct=coverage_pct,
    )
    dir_b_a = _direction_scores(
        direction="long_b_short_a",
        open_spread_pct=derived.get("open_spread_long_b_short_a_pct"),
        close_spread_pct=derived.get("close_spread_long_b_short_a_pct"),
        net_funding_hourly=funding_net_b_a,
        hours_to_next=hours_to_next,
        spread_z_1h=spread_features.get("spread_zscore_1h"),
        spread_velocity_5m=spread_features.get("spread_velocity_5m"),
        premium_diff_pct=derived.get("premium_diff_pct"),
        oi_divergence_6h_pct=oi_divergence,
        coverage_pct=coverage_pct,
    )

    return {
        "common": common,
        "directions": [dir_a_b, dir_b_a],
        "data_quality": {
            "coverage_pct": round(max(0.0, coverage_pct), 2),
            "spread_points_total": int(spread_features.get("spread_points_total") or 0),
            "spread_points_24h": int(spread_features.get("spread_points_24h") or 0),
            "left_oi_points": len(oi_left),
            "right_oi_points": len(oi_right),
        },
    }

