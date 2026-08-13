from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Iterable, Literal, Mapping, Sequence

from config import SUPPORTED_EXCHANGES
from exchanges import ADAPTER_FACTORIES, get_adapter_cached, normalize_exchange_name
from execution.accounts import _safe_float, normalize_symbol
from utils.cache_db import get_or_fetch_funding_history
from utils.funding import (
    enrich_history_intervals,
    infer_funding_interval_hours,
    normalize_interval_hours,
    parse_timestamp_ms,
    project_next_funding_time_iso,
)

FUNDING_HISTORY_EXCLUDED_EXCHANGES: tuple[str, ...] = ("bingx",)
FUNDING_HISTORY_DEFAULT_EXCHANGES: tuple[str, ...] = (
    "binance",
    "bybit",
    "okx",
    "gate",
    "bitget",
    "mexc",
    "kucoin",
)
FUNDING_HISTORY_WINDOWS_HOURS: tuple[int, ...] = (4, 12, 24, 72)
FUNDING_HISTORY_MAX_POINTS = 200

def _funding_history_ts_ms(value: object) -> int | None:
    return parse_timestamp_ms(value)

def _load_funding_history_cached(
    exchange: str,
    exchange_symbol: str,
    canonical_symbol: str,
    limit: int,
    adapter: Any,
) -> list[dict]:
    """Fetch funding history with caching, falling back to adapter hook."""
    fetch_limit = max(limit, min(limit + 8, 220))
    if hasattr(adapter, "funding_history"):
        try:
            return adapter.funding_history(canonical_symbol, limit=fetch_limit)
        except Exception:  # pylint: disable=broad-except
            return []

    def _fetch() -> list[dict]:
        return []

    try:
        return get_or_fetch_funding_history(
            normalize_exchange_name(exchange),
            exchange_symbol,
            _fetch,
            max_age_seconds=300,
            limit=limit,
        )
    except Exception:  # pylint: disable=broad-except
        return []


def _resolve_funding_interval_hours(
    history: list[dict[str, Any]],
    snapshot_interval: float | None,
) -> float | None:
    timestamp_interval = _infer_history_timestamp_interval_hours(history)
    inferred = infer_funding_interval_hours(history, snapshot_interval=snapshot_interval)
    return _resolve_row_interval_hours(inferred, timestamp_interval, snapshot_interval)


def _funding_interval_quality(
    interval_hours: float | None,
    timestamp_interval_hours: float | None,
) -> str:
    interval = normalize_interval_hours(interval_hours)
    ts_interval = normalize_interval_hours(timestamp_interval_hours)
    if interval is None:
        return "unresolved"
    if ts_interval is None:
        return "snapshot_or_declared_only"
    tolerance = max(0.1, min(interval, ts_interval) * 0.2)
    if abs(interval - ts_interval) <= tolerance:
        return "history_confirmed"
    return "history_mismatch"


def _funding_position_multiplier(
    direction: str,
    *,
    leg: Literal["left", "right"],
) -> float:
    direction_text = str(direction or "").lower()
    if direction_text == "long_b_short_a":
        return 1.0 if leg == "left" else -1.0
    return -1.0 if leg == "left" else 1.0


def _funding_event_segments(
    history: list[dict[str, Any]],
    snapshot_interval: float | None,
) -> list[dict[str, float]]:
    rows = enrich_history_intervals(history or [], snapshot_interval=snapshot_interval)
    segments: list[dict[str, float]] = []
    for row in rows:
        interval_hours = _safe_float(row.get("interval_hours"))
        raw_end_ts_ms = _funding_history_ts_ms(row.get("ts_ms") or row.get("timestamp"))
        end_ts_ms = (
            _funding_history_ts_ms(row.get("slot_ts_ms"))
            or _funding_slot_ts_ms(raw_end_ts_ms, interval_hours or snapshot_interval)
            or raw_end_ts_ms
        )
        rate = _funding_rate_from_row(row)
        if not end_ts_ms or interval_hours is None or interval_hours <= 0 or rate is None:
            continue
        duration_ms = int(interval_hours * 3600.0 * 1000.0)
        if duration_ms <= 0:
            continue
        segments.append(
            {
                "start_ts_ms": float(end_ts_ms - duration_ms),
                "end_ts_ms": float(end_ts_ms),
                "interval_hours": float(interval_hours),
                "rate": float(rate),
            }
        )
    segments.sort(key=lambda item: item.get("end_ts_ms") or 0.0)
    return segments


def _funding_rate_from_row(row: Mapping[str, Any]) -> float | None:
    for key in ("rate", "fundingRate", "funding_rate"):
        if key in row:
            value = _safe_float(row.get(key))
            if value is not None:
                return value
    return None


def _funding_slot_ts_ms(ts_ms: int | float | None, interval_hours: float | None = None) -> int | None:
    if ts_ms is None:
        return None
    ts_val = int(ts_ms)
    interval = normalize_interval_hours(interval_hours)
    bucket_ms = int((interval or 1.0) * 3600.0 * 1000.0)
    if bucket_ms <= 0:
        bucket_ms = 3600 * 1000
    return int(round(ts_val / float(bucket_ms)) * bucket_ms)


def _funding_slot_iso(ts_ms: int | float | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).isoformat()


def _infer_history_timestamp_interval_hours(history: Sequence[Mapping[str, Any]]) -> float | None:
    points: list[int] = []
    for row in history or []:
        if not isinstance(row, Mapping):
            continue
        ts_ms = _funding_history_ts_ms(
            row.get("ts_ms")
            or row.get("timestamp")
            or row.get("timepoint")
            or row.get("timePoint")
            or row.get("fundingTime")
        )
        if ts_ms is not None:
            points.append(int(ts_ms))
    unique = sorted(set(points), reverse=True)
    if len(unique) < 2:
        return None
    buckets: dict[float, int] = {}
    for idx in range(len(unique) - 1):
        diff_ms = abs(unique[idx] - unique[idx + 1])
        interval = normalize_interval_hours(diff_ms / 1000.0 / 3600.0)
        if interval is None:
            continue
        bucket = round(interval * 4.0) / 4.0
        buckets[bucket] = buckets.get(bucket, 0) + 1
    if not buckets:
        return None
    return max(buckets.items(), key=lambda item: (item[1], -item[0]))[0]


def _resolve_row_interval_hours(
    declared_interval: float | None,
    timestamp_interval: float | None,
    snapshot_interval: float | None,
) -> float | None:
    declared = normalize_interval_hours(declared_interval)
    ts_interval = normalize_interval_hours(timestamp_interval)
    snapshot = normalize_interval_hours(snapshot_interval)
    if ts_interval is not None:
        if declared is None:
            return ts_interval
        tolerance = max(0.1, min(declared, ts_interval) * 0.2)
        if abs(declared - ts_interval) > tolerance:
            return ts_interval
        return declared
    return declared if declared is not None else snapshot


def _compact_funding_history_rows(
    history: list[dict[str, Any]],
    *,
    snapshot_interval: float | None,
    limit: int,
) -> list[dict[str, Any]]:
    enriched = enrich_history_intervals(history or [], snapshot_interval=snapshot_interval)
    timestamp_interval = _infer_history_timestamp_interval_hours(enriched)
    rows: list[dict[str, Any]] = []
    for item in enriched:
        ts_ms = _funding_history_ts_ms(
            item.get("ts_ms")
            or item.get("timestamp")
            or item.get("timepoint")
            or item.get("timePoint")
            or item.get("fundingTime")
        )
        rate = _funding_rate_from_row(item)
        if ts_ms is None or rate is None:
            continue
        interval_hours = _safe_float(
            item.get("interval_hours")
            or item.get("intervalHours")
            or item.get("funding_interval_hours")
        )
        interval_hours = _resolve_row_interval_hours(
            interval_hours,
            timestamp_interval,
            snapshot_interval,
        )
        slot_ts_ms = _funding_slot_ts_ms(ts_ms, interval_hours or snapshot_interval)
        predicted = None
        for predicted_key in ("predicted_rate", "predictedFundingRate", "predicted_funding_rate"):
            if predicted_key in item:
                predicted = _safe_float(item.get(predicted_key))
                if predicted is not None:
                    break
        rows.append(
            {
                "ts_ms": int(ts_ms),
                "time_utc": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                "slot_ts_ms": slot_ts_ms,
                "slot_time_utc": _funding_slot_iso(slot_ts_ms),
                "rate": float(rate),
                "rate_bps": float(rate) * 10000.0,
                "predicted_rate": predicted,
                "predicted_bps": predicted * 10000.0 if predicted is not None else None,
                "interval_hours": interval_hours,
            }
        )
    rows.sort(key=lambda row: int(row.get("ts_ms") or 0), reverse=True)
    return rows[: max(1, int(limit))]


def _funding_carry_over_window_pct(
    segments: list[dict[str, float]],
    window_start_ms: int,
    window_end_ms: int,
    *,
    multiplier: float,
) -> tuple[float | None, float]:
    if window_end_ms <= window_start_ms:
        return None, 0.0
    total_pct = 0.0
    covered_ms = 0.0
    for item in segments:
        start_ts = int(_safe_float(item.get("start_ts_ms")) or 0)
        end_ts = int(_safe_float(item.get("end_ts_ms")) or 0)
        rate = _safe_float(item.get("rate"))
        interval_hours = _safe_float(item.get("interval_hours"))
        if start_ts <= 0 or end_ts <= start_ts or rate is None or interval_hours is None or interval_hours <= 0:
            continue
        overlap_ms = min(end_ts, window_end_ms) - max(start_ts, window_start_ms)
        if overlap_ms <= 0:
            continue
        duration_ms = interval_hours * 3600.0 * 1000.0
        if duration_ms <= 0:
            continue
        covered_ms += float(overlap_ms)
        total_pct += float(multiplier) * float(rate) * (float(overlap_ms) / float(duration_ms))
    if covered_ms <= 0:
        return None, 0.0
    return total_pct, min(100.0, covered_ms / max(1.0, float(window_end_ms - window_start_ms)) * 100.0)


def _funding_history_window_label(hours: int | float) -> str:
    hours_val = int(hours)
    if hours_val == 24:
        return "1d"
    if hours_val == 72:
        return "3d"
    return f"{hours_val}h"


def _funding_history_windows(windows_hours: Iterable[int | float] | None = None) -> list[dict[str, Any]]:
    raw_values = list(windows_hours or FUNDING_HISTORY_WINDOWS_HOURS)
    out: list[dict[str, Any]] = []
    seen: set[int] = set()
    for raw in raw_values:
        hours = int(_safe_float(raw) or 0)
        if hours <= 0 or hours > 24 * 14 or hours in seen:
            continue
        seen.add(hours)
        out.append({"hours": hours, "label": _funding_history_window_label(hours)})
    if not out:
        for hours in FUNDING_HISTORY_WINDOWS_HOURS:
            out.append({"hours": hours, "label": _funding_history_window_label(hours)})
    return out


def _funding_history_exchange_windows(
    history: list[dict[str, Any]],
    interval_hours: float | None,
    windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    segments = _funding_event_segments(history, interval_hours)
    if not segments:
        return []
    latest_end_ms = int(_safe_float(segments[-1].get("end_ts_ms")) or 0)
    out: list[dict[str, Any]] = []
    for window in windows:
        hours = int(window.get("hours") or 0)
        if hours <= 0:
            continue
        start_ms = latest_end_ms - hours * 3600 * 1000
        short_pct, short_cov = _funding_carry_over_window_pct(
            segments,
            start_ms,
            latest_end_ms,
            multiplier=1.0,
        )
        long_pct, long_cov = _funding_carry_over_window_pct(
            segments,
            start_ms,
            latest_end_ms,
            multiplier=-1.0,
        )
        out.append(
            {
                "label": window.get("label"),
                "hours": hours,
                "window_start_ms": start_ms,
                "window_end_ms": latest_end_ms,
                "short_carry_bps": short_pct * 10000.0 if short_pct is not None else None,
                "long_carry_bps": long_pct * 10000.0 if long_pct is not None else None,
                "coverage_pct": min(short_cov, long_cov),
            }
        )
    return out


def _build_funding_history_pair_analysis(
    exchange_rows: list[Mapping[str, Any]],
    windows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    usable = [
        row
        for row in exchange_rows
        if row.get("status") in {"ok", "partial"} and row.get("funding_history")
    ]
    pair_rows: list[dict[str, Any]] = []
    series_source: dict[str, Any] = {}
    for i in range(len(usable)):
        for j in range(i + 1, len(usable)):
            left = usable[i]
            right = usable[j]
            left_exchange = str(left.get("exchange") or "")
            right_exchange = str(right.get("exchange") or "")
            left_history = list(left.get("funding_history") or [])
            right_history = list(right.get("funding_history") or [])
            left_interval = _safe_float(left.get("funding_interval_hours_resolved"))
            right_interval = _safe_float(right.get("funding_interval_hours_resolved"))
            left_segments = _funding_event_segments(left_history, left_interval)
            right_segments = _funding_event_segments(right_history, right_interval)
            if not left_segments or not right_segments:
                continue
            pair_end_ms = min(
                int(_safe_float(left_segments[-1].get("end_ts_ms")) or 0),
                int(_safe_float(right_segments[-1].get("end_ts_ms")) or 0),
            )
            if pair_end_ms <= 0:
                continue
            for direction in ("long_a_short_b", "long_b_short_a"):
                left_mult = _funding_position_multiplier(direction, leg="left")
                right_mult = _funding_position_multiplier(direction, leg="right")
                if direction == "long_b_short_a":
                    long_exchange = right_exchange
                    short_exchange = left_exchange
                else:
                    long_exchange = left_exchange
                    short_exchange = right_exchange
                for window in windows:
                    hours = int(window.get("hours") or 0)
                    if hours <= 0:
                        continue
                    start_ms = pair_end_ms - hours * 3600 * 1000
                    left_pct, left_cov = _funding_carry_over_window_pct(
                        left_segments,
                        start_ms,
                        pair_end_ms,
                        multiplier=left_mult,
                    )
                    right_pct, right_cov = _funding_carry_over_window_pct(
                        right_segments,
                        start_ms,
                        pair_end_ms,
                        multiplier=right_mult,
                    )
                    net_pct = None
                    if left_pct is not None and right_pct is not None:
                        net_pct = float(left_pct) + float(right_pct)
                    coverage_pct = min(left_cov, right_cov)
                    status = "ok"
                    if net_pct is None:
                        status = "insufficient_data"
                    elif coverage_pct < 95.0:
                        status = "partial"
                    pair_rows.append(
                        {
                            "pair_key": f"{left_exchange}|{right_exchange}",
                            "pair_label": f"{left_exchange} vs {right_exchange}",
                            "left_exchange": left_exchange,
                            "right_exchange": right_exchange,
                            "direction": direction,
                            "direction_label": _direction_label(direction, left_exchange, right_exchange),
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "window_label": window.get("label"),
                            "window_hours": hours,
                            "window_start_ms": start_ms,
                            "window_end_ms": pair_end_ms,
                            "left_leg_bps": left_pct * 10000.0 if left_pct is not None else None,
                            "right_leg_bps": right_pct * 10000.0 if right_pct is not None else None,
                            "net_bps": net_pct * 10000.0 if net_pct is not None else None,
                            "net_pct": net_pct * 100.0 if net_pct is not None else None,
                            "annualized_pct": (
                                net_pct / float(hours) * 24.0 * 365.0 * 100.0
                                if net_pct is not None and hours > 0
                                else None
                            ),
                            "usd_per_1000_notional": net_pct * 1000.0 if net_pct is not None else None,
                            "coverage_pct": coverage_pct,
                            "status": status,
                        }
                    )
                    if (
                        net_pct is not None
                        and (not series_source or float(net_pct) > float(series_source.get("net_pct") or -999.0))
                        and hours == 24
                    ):
                        series_source = {
                            "left_exchange": left_exchange,
                            "right_exchange": right_exchange,
                            "direction": direction,
                            "net_pct": net_pct,
                            "left_history": left_history,
                            "right_history": right_history,
                            "left_interval": left_interval,
                            "right_interval": right_interval,
                        }

    best_by_window: dict[str, Any] = {}
    for window in windows:
        label = str(window.get("label") or "")
        candidates = [
            row
            for row in pair_rows
            if str(row.get("window_label") or "") == label and _safe_float(row.get("net_bps")) is not None
        ]
        complete = [row for row in candidates if float(_safe_float(row.get("coverage_pct")) or 0.0) >= 95.0]
        pool = complete or candidates
        if not pool:
            continue
        best = max(pool, key=lambda row: float(_safe_float(row.get("net_bps")) or -999999.0))
        verdict = "favorable" if float(_safe_float(best.get("net_bps")) or 0.0) > 0 else "avoid"
        if float(_safe_float(best.get("coverage_pct")) or 0.0) < 95.0:
            verdict = "partial_data"
        best_by_window[label] = {**best, "verdict": verdict}

    spread_series: dict[str, Any] = {"points": [], "source": {}}
    if series_source:
        direction = str(series_source.get("direction") or "long_a_short_b")
        points = _funding_net_hourly_series(
            list(series_source.get("left_history") or []),
            list(series_source.get("right_history") or []),
            left_interval_hours=_safe_float(series_source.get("left_interval")),
            right_interval_hours=_safe_float(series_source.get("right_interval")),
            direction=direction,
            max_points=96,
        )
        spread_series = {
            "points": points,
            "source": {
                "left_exchange": series_source.get("left_exchange"),
                "right_exchange": series_source.get("right_exchange"),
                "direction": direction,
            },
        }

    pair_rows.sort(
        key=lambda row: (
            int(row.get("window_hours") or 0),
            -(float(_safe_float(row.get("net_bps")) or -999999.0)),
            str(row.get("pair_label") or ""),
        )
    )
    return pair_rows, best_by_window, spread_series


def _build_funding_history_next_analysis(
    exchange_rows: list[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    usable = [
        row
        for row in exchange_rows
        if row.get("status") in {"ok", "partial"}
        and _safe_float(row.get("next_funding_rate")) is not None
        and _safe_float(row.get("funding_interval_hours_resolved")) is not None
    ]
    rows: list[dict[str, Any]] = []
    for i in range(len(usable)):
        for j in range(i + 1, len(usable)):
            left = usable[i]
            right = usable[j]
            left_exchange = str(left.get("exchange") or "")
            right_exchange = str(right.get("exchange") or "")
            left_rate = _safe_float(left.get("next_funding_rate"))
            right_rate = _safe_float(right.get("next_funding_rate"))
            left_interval = _safe_float(left.get("funding_interval_hours_resolved"))
            right_interval = _safe_float(right.get("funding_interval_hours_resolved"))
            if left_rate is None or right_rate is None or not left_interval or not right_interval:
                continue
            for direction in ("long_a_short_b", "long_b_short_a"):
                left_mult = _funding_position_multiplier(direction, leg="left")
                right_mult = _funding_position_multiplier(direction, leg="right")
                if direction == "long_b_short_a":
                    long_exchange = right_exchange
                    short_exchange = left_exchange
                else:
                    long_exchange = left_exchange
                    short_exchange = right_exchange
                left_pct = left_mult * left_rate
                right_pct = right_mult * right_rate
                net_pct = left_pct + right_pct
                left_hourly_pct = left_mult * left_rate / left_interval
                right_hourly_pct = right_mult * right_rate / right_interval
                net_hourly_pct = left_hourly_pct + right_hourly_pct
                next_left = str(left.get("next_funding_time") or "")
                next_right = str(right.get("next_funding_time") or "")
                next_sync = bool(next_left and next_right and next_left == next_right)
                status = "ok" if next_sync else "async_next_funding"
                rows.append(
                    {
                        "pair_key": f"{left_exchange}|{right_exchange}",
                        "pair_label": f"{left_exchange} vs {right_exchange}",
                        "left_exchange": left_exchange,
                        "right_exchange": right_exchange,
                        "direction": direction,
                        "direction_label": _direction_label(direction, left_exchange, right_exchange),
                        "long_exchange": long_exchange,
                        "short_exchange": short_exchange,
                        "window_label": "next",
                        "window_hours": None,
                        "next_left_time": next_left or None,
                        "next_right_time": next_right or None,
                        "next_sync": next_sync,
                        "left_interval_hours": left_interval,
                        "right_interval_hours": right_interval,
                        "left_leg_bps": left_pct * 10000.0,
                        "right_leg_bps": right_pct * 10000.0,
                        "net_bps": net_pct * 10000.0,
                        "net_pct": net_pct * 100.0,
                        "net_hourly_bps": net_hourly_pct * 10000.0,
                        "annualized_pct": net_hourly_pct * 24.0 * 365.0 * 100.0,
                        "usd_per_1000_notional": net_pct * 1000.0,
                        "coverage_pct": 100.0 if next_sync else 50.0,
                        "status": status,
                    }
                )
    rows.sort(
        key=lambda row: (
            -(float(_safe_float(row.get("net_hourly_bps")) or -999999.0)),
            -(float(_safe_float(row.get("net_bps")) or -999999.0)),
            str(row.get("pair_label") or ""),
        )
    )
    if not rows:
        return rows, {}
    complete = [row for row in rows if row.get("status") == "ok"]
    pool = complete or rows
    best = max(
        pool,
        key=lambda row: (
            float(_safe_float(row.get("net_hourly_bps")) or -999999.0),
            float(_safe_float(row.get("net_bps")) or -999999.0),
        ),
    )
    verdict = "favorable" if float(_safe_float(best.get("net_hourly_bps")) or 0.0) > 0 else "avoid"
    if best.get("status") != "ok":
        verdict = "async_next_funding"
    return rows, {**best, "verdict": verdict}


def _build_funding_history_timeline(
    exchange_rows: list[Mapping[str, Any]],
    *,
    max_hours: int,
) -> list[dict[str, Any]]:
    latest_ts_ms = 0
    for row in exchange_rows:
        for item in list(row.get("funding_history") or []):
            ts_ms = _funding_history_ts_ms(
                item.get("slot_ts_ms") or item.get("ts_ms") or item.get("timestamp")
            )
            if ts_ms:
                latest_ts_ms = max(latest_ts_ms, int(ts_ms))
    if latest_ts_ms <= 0:
        return []
    cutoff_ms = latest_ts_ms - max(1, int(max_hours)) * 3600 * 1000
    by_time: dict[int, dict[str, Any]] = {}
    for row in exchange_rows:
        exchange = str(row.get("exchange") or "")
        if not exchange:
            continue
        for item in list(row.get("funding_history") or []):
            raw_ts_ms = _funding_history_ts_ms(item.get("ts_ms") or item.get("timestamp"))
            slot_ts_ms = _funding_history_ts_ms(item.get("slot_ts_ms")) or _funding_slot_ts_ms(
                raw_ts_ms,
                _safe_float(item.get("interval_hours")),
            )
            if slot_ts_ms is None or slot_ts_ms < cutoff_ms:
                continue
            rate = _funding_rate_from_row(item)
            if rate is None:
                continue
            slot = by_time.setdefault(
                int(slot_ts_ms),
                {
                    "ts_ms": int(slot_ts_ms),
                    "time_utc": _funding_slot_iso(slot_ts_ms),
                    "exchanges": {},
                },
            )
            slot["exchanges"][exchange] = {
                "rate": float(rate),
                "rate_bps": float(rate) * 10000.0,
                "interval_hours": _safe_float(item.get("interval_hours")),
                "raw_ts_ms": raw_ts_ms,
                "raw_time_utc": _funding_slot_iso(raw_ts_ms),
            }
    return [by_time[key] for key in sorted(by_time.keys(), reverse=True)]


def _funding_net_hourly_series(
    left_history: list[dict[str, Any]],
    right_history: list[dict[str, Any]],
    *,
    left_interval_hours: float | None,
    right_interval_hours: float | None,
    direction: str,
    max_points: int = 168,
) -> list[dict[str, float]]:
    hour_ms = 3600 * 1000
    left_segments = _funding_event_segments(left_history, left_interval_hours)
    right_segments = _funding_event_segments(right_history, right_interval_hours)
    if not left_segments or not right_segments:
        return []

    latest_end_ms = min(
        int(_safe_float(left_segments[-1].get("end_ts_ms")) or 0),
        int(_safe_float(right_segments[-1].get("end_ts_ms")) or 0),
    )
    earliest_start_ms = max(
        int(_safe_float(left_segments[0].get("start_ts_ms")) or 0),
        int(_safe_float(right_segments[0].get("start_ts_ms")) or 0),
    )
    if latest_end_ms <= earliest_start_ms:
        return []

    bucket_end_ms = (latest_end_ms // hour_ms) * hour_ms
    if bucket_end_ms <= earliest_start_ms:
        bucket_end_ms += hour_ms

    left_mult = _funding_position_multiplier(direction, leg="left")
    right_mult = _funding_position_multiplier(direction, leg="right")
    rows: list[dict[str, float]] = []
    while bucket_end_ms - hour_ms >= earliest_start_ms:
        bucket_start_ms = bucket_end_ms - hour_ms
        left_pct, left_cov = _funding_carry_over_window_pct(
            left_segments,
            bucket_start_ms,
            bucket_end_ms,
            multiplier=left_mult,
        )
        right_pct, right_cov = _funding_carry_over_window_pct(
            right_segments,
            bucket_start_ms,
            bucket_end_ms,
            multiplier=right_mult,
        )
        if (
            left_pct is not None
            and right_pct is not None
            and left_cov >= 99.0
            and right_cov >= 99.0
        ):
            net_pct = float(left_pct) + float(right_pct)
            rows.append(
                {
                    "ts_ms": float(bucket_end_ms),
                    "left_bps": float(left_pct) * 10000.0,
                    "right_bps": float(right_pct) * 10000.0,
                    "net_bps": net_pct * 10000.0,
                }
            )
        bucket_end_ms -= hour_ms

    rows = list(reversed(rows))
    if max_points > 0 and len(rows) > max_points:
        rows = rows[-max_points:]
    return rows


def _direction_label(
    direction: str,
    left_exchange: str,
    right_exchange: str,
) -> str:
    if str(direction or "").lower() == "long_b_short_a":
        return f"Long {right_exchange} / Short {left_exchange}"
    return f"Long {left_exchange} / Short {right_exchange}"

class FundingHistoryService:
    async def analyze_funding_history(
        self,
        symbol: str,
        *,
        exchanges: Iterable[str] | None = None,
        windows_hours: Iterable[int | float] | None = None,
        funding_points: int = FUNDING_HISTORY_MAX_POINTS,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol must be provided for funding history analysis.")

        supported = [
            exchange
            for exchange in SUPPORTED_EXCHANGES
            if normalize_exchange_name(exchange) in ADAPTER_FACTORIES
            and normalize_exchange_name(exchange) not in FUNDING_HISTORY_EXCLUDED_EXCHANGES
        ]
        requested = list(exchanges or FUNDING_HISTORY_DEFAULT_EXCHANGES)
        selected: list[str] = []
        for item in requested:
            exchange = normalize_exchange_name(str(item or "").strip())
            if not exchange or exchange not in supported or exchange in selected:
                continue
            selected.append(exchange)
        if not selected:
            selected = [exchange for exchange in FUNDING_HISTORY_DEFAULT_EXCHANGES if exchange in supported]
        if not selected:
            raise ValueError("Enable at least one supported exchange.")

        windows = _funding_history_windows(windows_hours)
        max_window_hours = max(int(window.get("hours") or 0) for window in windows)
        fetch_limit = max(24, min(int(funding_points or FUNDING_HISTORY_MAX_POINTS), FUNDING_HISTORY_MAX_POINTS))
        fetch_limit = max(fetch_limit, min(FUNDING_HISTORY_MAX_POINTS, max_window_hours + 16))

        tasks = [
            self._analyze_funding_history_on_exchange(exchange, canonical, fetch_limit, windows)
            for exchange in selected
        ]
        exchange_rows = [row for row in await asyncio.gather(*tasks) if row]
        pair_windows, best_by_window, spread_series = _build_funding_history_pair_analysis(
            exchange_rows,
            windows,
        )
        next_funding_rows, best_next_funding = _build_funding_history_next_analysis(exchange_rows)
        if best_next_funding:
            best_by_window["next"] = best_next_funding
        timeline = _build_funding_history_timeline(exchange_rows, max_hours=max_window_hours)
        chart_series: dict[str, Any] = {}
        for row in exchange_rows:
            exchange = str(row.get("exchange") or "")
            if not exchange:
                continue
            history_asc = list(reversed(list(row.get("funding_history") or [])))
            chart_series[exchange] = [
                {
                    "ts_ms": int(item.get("ts_ms") or 0),
                    "rate_bps": _safe_float(item.get("rate_bps")),
                    "interval_hours": _safe_float(item.get("interval_hours")),
                }
                for item in history_asc
                if _safe_float(item.get("rate_bps")) is not None
            ][-120:]

        warnings: list[str] = []
        if len([row for row in exchange_rows if row.get("funding_history")]) < 2:
            warnings.append("pair_analysis_limited: fewer than two exchanges returned funding history")

        return {
            "symbol": canonical,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "supported_exchanges": supported,
            "default_exchanges": list(FUNDING_HISTORY_DEFAULT_EXCHANGES),
            "selected_exchanges": selected,
            "funding_points": fetch_limit,
            "windows": windows,
            "warnings": warnings,
            "exchanges": exchange_rows,
            "best_by_window": best_by_window,
            "pair_windows": pair_windows,
            "next_funding_windows": next_funding_rows,
            "best_next_funding": best_next_funding,
            "timeline": timeline,
            "charts": {
                "exchange_rates": chart_series,
                "best_pair_hourly": spread_series,
            },
            "method": {
                "carry_formula": "long leg receives -funding_rate, short leg receives +funding_rate",
                "interval_handling": "funding events are converted to time segments and prorated by overlap inside each analysis window",
                "windows": [window.get("label") for window in windows],
            },
        }

    async def _analyze_funding_history_on_exchange(
        self,
        exchange: str,
        canonical_symbol: str,
        funding_points: int,
        windows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "exchange": exchange,
            "symbol": canonical_symbol,
        }
        try:
            adapter = get_adapter_cached(exchange)
        except KeyError:
            result["status"] = "error"
            result["error"] = f"Adapter for {exchange} not registered."
            return result

        try:
            exchange_symbol = adapter.map_symbol(canonical_symbol)
        except Exception:  # pylint: disable=broad-except
            exchange_symbol = None
        if not exchange_symbol:
            result["status"] = "unsupported"
            result["error"] = "Symbol not supported on this exchange."
            return result

        result["exchange_symbol"] = exchange_symbol
        warnings: list[str] = []
        snapshot_dict: dict[str, Any] = {}
        try:
            snapshots = await adapter.fetch_market_snapshots_async([canonical_symbol])
            if snapshots:
                snapshot_dict = snapshots[0].to_dict()
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"snapshot_unavailable:{exc}")
        result["snapshot"] = snapshot_dict

        raw_history = await asyncio.to_thread(
            _load_funding_history_cached,
            exchange,
            exchange_symbol,
            canonical_symbol,
            funding_points,
            adapter,
        )
        interval_hours = _resolve_funding_interval_hours(
            raw_history,
            _safe_float(snapshot_dict.get("funding_interval_hours")),
        )
        funding_history = _compact_funding_history_rows(
            raw_history,
            snapshot_interval=interval_hours,
            limit=funding_points,
        )
        interval_hours = _resolve_funding_interval_hours(
            funding_history,
            interval_hours,
        )
        result["funding_history"] = funding_history
        result["funding_interval_hours_resolved"] = interval_hours
        result["latest_funding_rate"] = (
            _safe_float(funding_history[0].get("rate")) if funding_history else _safe_float(snapshot_dict.get("funding_rate"))
        )
        result["latest_funding_bps"] = (
            float(result["latest_funding_rate"]) * 10000.0
            if result.get("latest_funding_rate") is not None
            else None
        )
        result["latest_funding_hourly_bps"] = (
            float(result["latest_funding_rate"]) / float(interval_hours) * 10000.0
            if result.get("latest_funding_rate") is not None and interval_hours
            else None
        )
        next_funding_time = snapshot_dict.get("next_funding_time")
        if not next_funding_time:
            next_funding_time = project_next_funding_time_iso(funding_history, interval_hours=interval_hours)
        result["next_funding_time"] = next_funding_time
        snapshot_funding_rate = _safe_float(snapshot_dict.get("funding_rate"))
        result["next_funding_source"] = "snapshot_current" if snapshot_funding_rate is not None else "history_latest_fallback"
        result["next_funding_rate"] = snapshot_funding_rate if snapshot_funding_rate is not None else result["latest_funding_rate"]
        result["next_funding_bps"] = (
            float(result["next_funding_rate"]) * 10000.0
            if result.get("next_funding_rate") is not None
            else None
        )
        result["next_funding_hourly_bps"] = (
            float(result["next_funding_rate"]) / float(interval_hours) * 10000.0
            if result.get("next_funding_rate") is not None and interval_hours
            else None
        )
        result["windows"] = _funding_history_exchange_windows(funding_history, interval_hours, windows)
        timestamp_interval = _infer_history_timestamp_interval_hours(funding_history)
        result["data_quality"] = {
            "funding_points_received": len(funding_history),
            "oldest_ts_ms": funding_history[-1].get("ts_ms") if funding_history else None,
            "latest_ts_ms": funding_history[0].get("ts_ms") if funding_history else None,
            "timestamp_interval_hours": timestamp_interval,
            "snapshot_interval_hours": _safe_float(snapshot_dict.get("funding_interval_hours")),
            "interval_quality": _funding_interval_quality(interval_hours, timestamp_interval),
        }
        if funding_history and interval_hours is None:
            warnings.append("funding_interval_unresolved")
        if funding_history and len(funding_history) < 2:
            warnings.append("funding_history_short")
        if not funding_history:
            result["status"] = "error"
            result["error"] = "Funding history unavailable for this symbol/exchange."
        elif warnings:
            result["status"] = "partial"
        else:
            result["status"] = "ok"
        if warnings:
            result["warnings"] = warnings
        return result
