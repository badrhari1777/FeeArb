from __future__ import annotations

import html
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_features.bybit_pump_short_outcomes import (
    PumpEvent,
    Series,
    detect_pump_events,
    funding_sum_pct,
    load_samples,
    point_change_pct,
    sample_to_series,
    safe_max,
    safe_min,
    to_float,
    write_csv,
)
from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_lifecycle_research"

HOUR_MS = 3_600_000
TIMELINE_LOOKBACK_H = 24
TIMELINE_FORWARD_H = 168


@dataclass(slots=True)
class ResearchSeries:
    base: Series
    turnover: list[float | None]


def run_lifecycle_research(
    *,
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    max_events: int | None = None,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    samples = list(load_samples(input_path))
    series_by_symbol = {str(sample.get("symbol") or "").upper(): sample_to_research_series(sample) for sample in samples}
    btc_series = series_by_symbol.get("BTCUSDT")

    event_rows: list[dict[str, Any]] = []
    timeline_rows: list[dict[str, Any]] = []
    symbols_seen = 0
    events_seen = 0

    for research_series in series_by_symbol.values():
        symbols_seen += 1
        series = research_series.base
        events = detect_pump_events(series)
        for event in events:
            if max_events is not None and events_seen >= max_events:
                break
            events_seen += 1
            rows = build_event_timeline(research_series, event, btc_series=btc_series)
            timeline_rows.extend(rows)
            event_rows.append(build_event_summary(research_series, event, rows))
        if max_events is not None and events_seen >= max_events:
            break

    score_summary = build_score_summary(timeline_rows)

    write_csv(output_dir / "lifecycle_events.csv", event_rows)
    write_csv(output_dir / "lifecycle_timeline.csv", timeline_rows)
    write_csv(output_dir / "lifecycle_score_summary.csv", score_summary)
    (output_dir / "index.html").write_text(
        render_index(event_rows=event_rows, score_summary=score_summary),
        encoding="utf-8",
    )

    metadata = {
        "schema": "pump_lifecycle_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": len(event_rows),
        "timeline_rows": len(timeline_rows),
        "score_summary_rows": len(score_summary),
        "has_btc_context": btc_series is not None,
        "timeline_lookback_h": TIMELINE_LOOKBACK_H,
        "timeline_forward_h": TIMELINE_FORWARD_H,
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def sample_to_research_series(sample: dict[str, Any]) -> ResearchSeries:
    base = sample_to_series(sample)
    rows = sorted(
        (row for row in sample.get("series", {}).get("klines_1h", []) if row.get("ts_ms") is not None),
        key=lambda row: int(row["ts_ms"]),
    )
    turnover: list[float | None] = []
    for row in rows:
        value = to_float(row.get("turnover"))
        if value is None:
            volume = to_float(row.get("volume"))
            close = to_float(row.get("close"))
            value = volume * close if volume is not None and close is not None else None
        turnover.append(value)
    return ResearchSeries(base=base, turnover=turnover)


def build_event_timeline(
    research_series: ResearchSeries,
    event: PumpEvent,
    *,
    btc_series: ResearchSeries | None,
) -> list[dict[str, Any]]:
    series = research_series.base
    start_idx = max(0, event.trigger_idx - TIMELINE_LOOKBACK_H)
    end_idx = min(len(series.ts) - 1, event.trigger_idx + TIMELINE_FORWARD_H)
    rows: list[dict[str, Any]] = []
    for idx in range(start_idx, end_idx + 1):
        features = lifecycle_features(research_series, idx, event, btc_series=btc_series)
        scores = lifecycle_scores(features)
        state = classify_lifecycle_state(features, scores)
        rows.append(
            {
                "event_id": event.event_id,
                "symbol": event.symbol,
                "ts_ms": series.ts[idx],
                "iso": ms_to_iso(series.ts[idx]),
                "hours_from_trigger": idx - event.trigger_idx,
                "config_window_h": event.config_window_h,
                "config_threshold_pct": round_float(event.config_threshold_pct),
                "trigger_pump_pct": round_float(event.pump_pct),
                **features,
                **scores,
                "state": state,
            }
        )
    return rows


def lifecycle_features(
    research_series: ResearchSeries,
    idx: int,
    event: PumpEvent,
    *,
    btc_series: ResearchSeries | None,
) -> dict[str, Any]:
    series = research_series.base
    ts_ms = series.ts[idx]
    current_close = series.close[idx]
    high_since_trigger = safe_max(series.high[event.trigger_idx : idx + 1]) if idx >= event.trigger_idx else None
    pullback_from_high_pct = (
        (1.0 - current_close / high_since_trigger) * 100.0
        if current_close is not None and high_since_trigger not in {None, 0}
        else None
    )
    ret_1h = price_return(series, idx, 1)
    ret_6h = price_return(series, idx, 6)
    btc_ret_1h = aligned_return(btc_series.base if btc_series else None, ts_ms, 1) if btc_series else None
    btc_ret_6h = aligned_return(btc_series.base if btc_series else None, ts_ms, 6) if btc_series else None
    funding_8h = funding_sum_pct(series.funding, ts_ms - 8 * HOUR_MS, ts_ms)
    prev_funding_8h = funding_sum_pct(series.funding, ts_ms - 16 * HOUR_MS, ts_ms - 8 * HOUR_MS)
    funding_trend_8h = funding_8h - prev_funding_8h if funding_8h is not None and prev_funding_8h is not None else None
    volume_z_24h = z_score_current(research_series.turnover, idx, 24)
    volume_z_168h = z_score_current(research_series.turnover, idx, 168)
    return {
        "close": round_float(current_close),
        "return_1h_pct": round_float(ret_1h),
        "return_6h_pct": round_float(ret_6h),
        "btc_return_1h_pct": round_float(btc_ret_1h),
        "btc_return_6h_pct": round_float(btc_ret_6h),
        "btc_relative_1h_pct": round_float(ret_1h - btc_ret_1h if ret_1h is not None and btc_ret_1h is not None else None),
        "btc_relative_6h_pct": round_float(ret_6h - btc_ret_6h if ret_6h is not None and btc_ret_6h is not None else None),
        "funding_prev_8h_pct": round_float(funding_8h),
        "funding_prev_24h_pct": round_float(funding_sum_pct(series.funding, ts_ms - 24 * HOUR_MS, ts_ms)),
        "funding_trend_8h_pct": round_float(funding_trend_8h),
        "oi_change_1h_pct": round_float(point_change_pct(series.oi, series.ts, idx, 1)),
        "oi_change_6h_pct": round_float(point_change_pct(series.oi, series.ts, idx, 6)),
        "oi_change_24h_pct": round_float(point_change_pct(series.oi, series.ts, idx, 24)),
        "long_ratio": round_float(series.long_ratio.get(ts_ms), 6),
        "volume_z_24h": round_float(volume_z_24h),
        "volume_z_168h": round_float(volume_z_168h),
        "pullback_from_high_pct": round_float(pullback_from_high_pct),
        "high_since_trigger": round_float(high_since_trigger),
    }


def lifecycle_scores(features: dict[str, Any]) -> dict[str, Any]:
    continuation = continuation_score(features)
    continuation_max = continuation_available_points(features)
    exhaustion = exhaustion_score(features)
    exhaustion_max = exhaustion_available_points(features)
    security = security_dislocation_score(features)
    data_quality = data_quality_score(features)
    return {
        "squeeze_continuation_raw_points": round_float(continuation),
        "squeeze_continuation_available_points": round_float(continuation_max),
        "squeeze_continuation_score": round_float(scale_available_score(continuation, continuation_max)),
        "pump_exhaustion_raw_points": round_float(exhaustion),
        "pump_exhaustion_available_points": round_float(exhaustion_max),
        "pump_exhaustion_score": round_float(scale_available_score(exhaustion, exhaustion_max)),
        "security_dislocation_score": round_float(security),
        "data_quality_score": round_float(data_quality),
    }


def continuation_score(features: dict[str, Any]) -> float:
    score = 0.0
    funding_8h = to_float(features.get("funding_prev_8h_pct"))
    funding_trend = to_float(features.get("funding_trend_8h_pct"))
    oi_1h = to_float(features.get("oi_change_1h_pct"))
    oi_6h = to_float(features.get("oi_change_6h_pct"))
    volume_z = max_known(features.get("volume_z_24h"), features.get("volume_z_168h"))
    rel_1h = to_float(features.get("btc_relative_1h_pct"))
    rel_6h = to_float(features.get("btc_relative_6h_pct"))
    ret_1h = to_float(features.get("return_1h_pct"))
    ret_6h = to_float(features.get("return_6h_pct"))

    if funding_8h is not None:
        if funding_8h < -0.05:
            score += 10.0
        if funding_8h < -0.15:
            score += 6.0
        if funding_trend is not None and funding_trend < 0.0:
            score += 4.0

    if oi_1h is not None and oi_1h >= 15.0:
        score += 8.0
    if oi_6h is not None and oi_6h >= 30.0:
        score += 7.0

    if volume_z is not None:
        if volume_z >= 5.0:
            score += 15.0
        elif volume_z >= 3.0:
            score += 10.0
        elif volume_z >= 2.0:
            score += 5.0

    if rel_1h is not None and rel_1h >= 8.0:
        score += 8.0
    if rel_6h is not None and rel_6h >= 15.0:
        score += 7.0

    if ret_1h is not None and ret_1h >= 5.0:
        score += 7.0
    if ret_6h is not None and ret_6h >= 15.0:
        score += 8.0

    return min(100.0, score)


def exhaustion_score(features: dict[str, Any]) -> float:
    score = 0.0
    funding_8h = to_float(features.get("funding_prev_8h_pct"))
    oi_6h = to_float(features.get("oi_change_6h_pct"))
    ret_1h = to_float(features.get("return_1h_pct"))
    ret_6h = to_float(features.get("return_6h_pct"))
    pullback = to_float(features.get("pullback_from_high_pct"))
    long_ratio = to_float(features.get("long_ratio"))

    if funding_8h is not None:
        if 0.01 <= funding_8h <= 0.10:
            score += 15.0
        elif funding_8h > 0.0:
            score += 8.0
        elif funding_8h > -0.02:
            score += 5.0

    if pullback is not None:
        if pullback >= 20.0:
            score += 20.0
        elif pullback >= 10.0:
            score += 10.0

    if oi_6h is not None and oi_6h >= 10.0 and pullback is not None and pullback >= 5.0:
        score += 15.0

    if ret_1h is not None and ret_1h < 0.0:
        score += 7.0
    if ret_6h is not None and ret_6h < 0.0:
        score += 8.0

    if long_ratio is not None:
        if long_ratio >= 0.65:
            score += 10.0
        elif long_ratio >= 0.60:
            score += 5.0

    return min(100.0, score)


def continuation_available_points(features: dict[str, Any]) -> float:
    max_points = 0.0
    if to_float(features.get("funding_prev_8h_pct")) is not None:
        max_points += 20.0
    if to_float(features.get("oi_change_1h_pct")) is not None or to_float(features.get("oi_change_6h_pct")) is not None:
        max_points += 15.0
    if max_known(features.get("volume_z_24h"), features.get("volume_z_168h")) is not None:
        max_points += 15.0
    if to_float(features.get("btc_relative_1h_pct")) is not None or to_float(features.get("btc_relative_6h_pct")) is not None:
        max_points += 15.0
    if to_float(features.get("return_1h_pct")) is not None or to_float(features.get("return_6h_pct")) is not None:
        max_points += 15.0
    return max_points


def exhaustion_available_points(features: dict[str, Any]) -> float:
    max_points = 0.0
    if to_float(features.get("funding_prev_8h_pct")) is not None:
        max_points += 15.0
    if to_float(features.get("pullback_from_high_pct")) is not None:
        max_points += 20.0
    if to_float(features.get("oi_change_6h_pct")) is not None and to_float(features.get("pullback_from_high_pct")) is not None:
        max_points += 15.0
    if to_float(features.get("return_1h_pct")) is not None or to_float(features.get("return_6h_pct")) is not None:
        max_points += 15.0
    if to_float(features.get("long_ratio")) is not None:
        max_points += 10.0
    return max_points


def scale_available_score(raw_points: float, available_points: float) -> float | None:
    if available_points <= 0.0:
        return None
    return min(100.0, raw_points / available_points * 100.0)


def security_dislocation_score(features: dict[str, Any]) -> float:
    # Infrastructure, token-migration, deposit/withdrawal, and on-chain flags are
    # not present in the current Bybit 1h dataset. Keep this explicit instead of
    # implying that security risk has been checked.
    return 0.0


def data_quality_score(features: dict[str, Any]) -> float:
    keys = (
        "funding_prev_8h_pct",
        "oi_change_6h_pct",
        "long_ratio",
        "volume_z_24h",
        "return_1h_pct",
        "return_6h_pct",
    )
    available = sum(1 for key in keys if to_float(features.get(key)) is not None)
    score = available / len(keys) * 100.0
    if to_float(features.get("btc_relative_1h_pct")) is None:
        score -= 10.0
    return max(0.0, min(100.0, score))


def classify_lifecycle_state(features: dict[str, Any], scores: dict[str, Any]) -> str:
    security = to_float(scores.get("security_dislocation_score")) or 0.0
    continuation = to_float(scores.get("squeeze_continuation_score")) or 0.0
    exhaustion = to_float(scores.get("pump_exhaustion_score")) or 0.0
    pullback = to_float(features.get("pullback_from_high_pct")) or 0.0
    ret_1h = to_float(features.get("return_1h_pct")) or 0.0
    if security >= 70.0:
        return "SECURITY_DISLOCATION"
    if continuation >= 70.0 and exhaustion < 50.0:
        return "SHORT_SQUEEZE"
    if continuation >= 50.0 and exhaustion < 60.0:
        return "IGNITION"
    if exhaustion >= 70.0 and (ret_1h < 0.0 or pullback >= 20.0):
        return "BREAKDOWN"
    if exhaustion >= 50.0 or pullback >= 20.0:
        return "DISTRIBUTION"
    if continuation >= 25.0:
        return "WATCH"
    return "RESET"


def build_event_summary(
    research_series: ResearchSeries,
    event: PumpEvent,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    series = research_series.base
    trigger_row = next((row for row in rows if int(row["hours_from_trigger"]) == 0), rows[0] if rows else {})
    forward_end = min(len(series.ts) - 1, event.trigger_idx + TIMELINE_FORWARD_H)
    future_high = safe_max(series.high[event.trigger_idx : forward_end + 1])
    future_low = safe_min(series.low[event.trigger_idx : forward_end + 1])
    trigger_close = event.trigger_close
    first_exhaustion = first_state_row(rows, {"DISTRIBUTION", "BREAKDOWN"})
    first_continuation = first_state_row(rows, {"IGNITION", "SHORT_SQUEEZE"})
    return {
        "event_id": event.event_id,
        "symbol": event.symbol,
        "trigger_ts": event.trigger_ts,
        "trigger_iso": ms_to_iso(event.trigger_ts),
        "config_window_h": event.config_window_h,
        "config_threshold_pct": round_float(event.config_threshold_pct),
        "trigger_pump_pct": round_float(event.pump_pct),
        "trigger_continuation_score": trigger_row.get("squeeze_continuation_score"),
        "trigger_exhaustion_score": trigger_row.get("pump_exhaustion_score"),
        "trigger_state": trigger_row.get("state"),
        "max_continuation_score": round_float(max_values(rows, "squeeze_continuation_score")),
        "max_exhaustion_score": round_float(max_values(rows, "pump_exhaustion_score")),
        "first_continuation_h": first_continuation.get("hours_from_trigger") if first_continuation else None,
        "first_exhaustion_h": first_exhaustion.get("hours_from_trigger") if first_exhaustion else None,
        "future_high_168h_pct": round_float((future_high / trigger_close - 1.0) * 100.0 if future_high else None),
        "future_low_168h_pct": round_float((future_low / trigger_close - 1.0) * 100.0 if future_low else None),
        "data_quality_at_trigger": trigger_row.get("data_quality_score"),
    }


def build_score_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if int(row.get("hours_from_trigger") or 0) < 0:
            continue
        key = (str(row.get("state") or ""), score_bucket(to_float(row.get("squeeze_continuation_score")) or 0.0))
        groups.setdefault(key, []).append(row)
    summary: list[dict[str, Any]] = []
    for (state, bucket), items in sorted(groups.items()):
        summary.append(
            {
                "state": state,
                "continuation_bucket": bucket,
                "rows": len(items),
                "avg_continuation_score": round_float(mean_values(items, "squeeze_continuation_score")),
                "avg_exhaustion_score": round_float(mean_values(items, "pump_exhaustion_score")),
                "avg_data_quality_score": round_float(mean_values(items, "data_quality_score")),
                "avg_return_1h_pct": round_float(mean_values(items, "return_1h_pct")),
                "avg_return_6h_pct": round_float(mean_values(items, "return_6h_pct")),
                "avg_volume_z_24h": round_float(mean_values(items, "volume_z_24h")),
            }
        )
    return summary


def price_return(series: Series, idx: int, hours: int) -> float | None:
    prior_idx = idx - hours
    if prior_idx < 0:
        return None
    current = series.close[idx]
    prior = series.close[prior_idx]
    if current is None or prior in {None, 0}:
        return None
    return (current / prior - 1.0) * 100.0


def aligned_return(series: Series | None, ts_ms: int, hours: int) -> float | None:
    if series is None:
        return None
    try:
        idx = series.ts.index(ts_ms)
    except ValueError:
        return None
    return price_return(series, idx, hours)


def z_score_current(values: list[float | None], idx: int, window: int) -> float | None:
    if idx <= 0:
        return None
    start = max(0, idx - window)
    history = [float(value) for value in values[start:idx] if value is not None and math.isfinite(float(value))]
    current = values[idx] if idx < len(values) else None
    if current is None or len(history) < max(8, min(window, 24) // 2):
        return None
    mean = statistics.mean(history)
    stdev = statistics.pstdev(history)
    if stdev <= 0:
        return None
    return (float(current) - mean) / stdev


def first_state_row(rows: list[dict[str, Any]], states: set[str]) -> dict[str, Any] | None:
    candidates = [row for row in rows if int(row.get("hours_from_trigger") or 0) >= 0 and row.get("state") in states]
    return min(candidates, key=lambda row: int(row.get("hours_from_trigger") or 0)) if candidates else None


def score_bucket(score: float) -> str:
    if score >= 85.0:
        return "85_plus"
    if score >= 70.0:
        return "70_84"
    if score >= 50.0:
        return "50_69"
    if score >= 25.0:
        return "25_49"
    return "0_24"


def max_known(*items: Any) -> float | None:
    values = [to_float(item) for item in items]
    clean = [value for value in values if value is not None]
    return max(clean) if clean else None


def max_values(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    clean = [to_float(row.get(key)) for row in rows]
    values = [value for value in clean if value is not None]
    return max(values) if values else None


def mean_values(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    clean = [to_float(row.get(key)) for row in rows]
    values = [value for value in clean if value is not None]
    return statistics.mean(values) if values else None


def round_float(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    try:
        value_float = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value_float):
        return None
    return round(value_float, digits)


def ms_to_iso(ts_ms: int | None) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def render_index(*, event_rows: list[dict[str, Any]], score_summary: list[dict[str, Any]]) -> str:
    content = f"""
    <h1>Pump lifecycle research</h1>
    <p>Initial Bybit 1h lifecycle replay. Scores are rule-based and intentionally separated into continuation, exhaustion, security, and data quality.</p>
    <section>
      <h2>Run Summary</h2>
      <ul>
        <li>Events: {len(event_rows)}</li>
        <li>Security score: placeholder until infrastructure/on-chain data is collected.</li>
      </ul>
    </section>
    <section><h2>Top Events</h2>{html_table(event_rows[:100])}</section>
    <section><h2>Score Summary</h2>{html_table(score_summary)}</section>
    """
    return page_shell("Pump lifecycle research", content)


def html_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    head = "".join(f"<th>{html.escape(str(column))}</th>" for column in columns)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns)
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def page_shell(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2933; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
    th, td {{ border: 1px solid #d7dde5; padding: 4px 6px; text-align: left; }}
    th {{ background: #eef2f7; position: sticky; top: 0; }}
    section {{ margin: 24px 0; }}
  </style>
</head>
<body>{body}</body>
</html>"""


__all__ = [
    "DEFAULT_INPUT",
    "DEFAULT_OUTPUT_DIR",
    "classify_lifecycle_state",
    "continuation_score",
    "continuation_available_points",
    "data_quality_score",
    "exhaustion_score",
    "exhaustion_available_points",
    "run_lifecycle_research",
    "scale_available_score",
    "z_score_current",
]
