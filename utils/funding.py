from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence


_MAX_REASONABLE_INTERVAL_HOURS = 72.0


def _safe_float(value: object) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_timestamp_ms(value: object) -> int | None:
    """Parse timestamp-like value into unix milliseconds."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    ts_val = _safe_float(value)
    if ts_val is None and isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            iso = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    if ts_val is None or ts_val <= 0:
        return None

    # Futures APIs mix seconds/ms/us across exchanges.
    if ts_val >= 1e14:  # microseconds
        ts_val = ts_val / 1000.0
    elif ts_val < 1e11:  # seconds
        ts_val = ts_val * 1000.0
    return int(ts_val)


def normalize_interval_hours(value: object) -> float | None:
    """Normalize interval values that may be in hours/minutes/seconds."""
    hours = _safe_float(value)
    if hours is None or hours <= 0:
        return None

    if hours > _MAX_REASONABLE_INTERVAL_HOURS:
        # Most likely seconds.
        if hours >= 3600:
            hours = hours / 3600.0
        # Potentially minutes.
        elif hours >= 120:
            hours = hours / 60.0

    if hours <= 0 or hours > _MAX_REASONABLE_INTERVAL_HOURS:
        return None
    return float(hours)


def _history_ts_ms(row: Mapping[str, Any]) -> int | None:
    return parse_timestamp_ms(row.get("ts_ms") or row.get("timestamp"))


def _dominant_interval(
    values: Sequence[float],
    *,
    snapshot_hint: float | None = None,
) -> float | None:
    buckets: dict[float, int] = {}
    for raw in values:
        norm = normalize_interval_hours(raw)
        if norm is None:
            continue
        # Funding intervals are coarse; quarter-hour buckets suppress jitter.
        bucket = round(norm * 4.0) / 4.0
        buckets[bucket] = buckets.get(bucket, 0) + 1
    if not buckets:
        return None
    best_count = max(buckets.values())
    tied = [bucket for bucket, count in buckets.items() if count == best_count]
    if snapshot_hint is not None and tied:
        return min(tied, key=lambda item: abs(item - snapshot_hint))
    return min(tied)


def infer_funding_interval_hours(
    history: Sequence[Mapping[str, Any]],
    snapshot_interval: object = None,
) -> float | None:
    """Infer funding interval from explicit values and/or timestamp deltas."""
    snapshot_hint = normalize_interval_hours(snapshot_interval)

    declared: list[float] = []
    ts_points: list[int] = []
    for row in history or []:
        declared_val = normalize_interval_hours(row.get("interval_hours"))
        if declared_val is not None:
            declared.append(declared_val)
        ts_ms = _history_ts_ms(row)
        if ts_ms is not None:
            ts_points.append(ts_ms)

    if declared:
        choice = _dominant_interval(declared, snapshot_hint=snapshot_hint)
        if choice is not None:
            return choice

    if len(ts_points) >= 2:
        unique_points = sorted(set(ts_points), reverse=True)
        deltas: list[float] = []
        for idx in range(len(unique_points) - 1):
            diff_ms = abs(unique_points[idx] - unique_points[idx + 1])
            hours = normalize_interval_hours(diff_ms / 1000.0 / 3600.0)
            if hours is not None:
                deltas.append(hours)
        if deltas:
            choice = _dominant_interval(deltas, snapshot_hint=snapshot_hint)
            if choice is not None:
                return choice

    return snapshot_hint


def enrich_history_intervals(
    history: Sequence[Mapping[str, Any]],
    *,
    snapshot_interval: object = None,
) -> list[dict[str, Any]]:
    """Return mutable history rows with normalized/fallback interval_hours."""
    rows = [dict(item) for item in (history or []) if isinstance(item, Mapping)]
    interval = infer_funding_interval_hours(rows, snapshot_interval=snapshot_interval)
    for row in rows:
        current = normalize_interval_hours(row.get("interval_hours"))
        row["interval_hours"] = current if current is not None else interval
    return rows


def project_next_funding_time_iso(
    history: Sequence[Mapping[str, Any]],
    *,
    interval_hours: object = None,
    now: datetime | None = None,
) -> str | None:
    interval = normalize_interval_hours(interval_hours)
    if interval is None:
        interval = infer_funding_interval_hours(history, snapshot_interval=None)
    if interval is None:
        return None

    latest_ts_ms: int | None = None
    for row in history or []:
        if not isinstance(row, Mapping):
            continue
        ts_ms = _history_ts_ms(row)
        if ts_ms is None:
            continue
        if latest_ts_ms is None or ts_ms > latest_ts_ms:
            latest_ts_ms = ts_ms
    if latest_ts_ms is None:
        return None

    step_ms = int(interval * 3600.0 * 1000.0)
    if step_ms <= 0:
        return None

    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    now_ms = int(now_dt.timestamp() * 1000)

    next_ms = latest_ts_ms
    if next_ms <= now_ms:
        hops = ((now_ms - latest_ts_ms) // step_ms) + 1
        next_ms = latest_ts_ms + hops * step_ms

    return datetime.fromtimestamp(next_ms / 1000.0, tz=timezone.utc).isoformat()


def is_stale_next_funding_iso(
    value: str | None,
    *,
    now: datetime | None = None,
    grace_seconds: int = 300,
) -> bool:
    if not value:
        return True
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    return dt < (now_dt - timedelta(seconds=max(0, int(grace_seconds))))
