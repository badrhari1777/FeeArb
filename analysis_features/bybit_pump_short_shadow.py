from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from analysis_collectors.bybit_pump_short import (
    BybitCollectorConfig,
    BybitInstrument,
    BybitPumpShortCollector,
    dedupe_instruments,
    is_crypto_pump_short_instrument,
    normalize_symbol,
    now_ms,
)
from analysis_features.bybit_pump_short_outcomes import (
    DEFAULT_OUTPUT_DIR as DEFAULT_ANALYSIS_OUTPUT_DIR,
    PumpEvent,
    Series,
    detect_pump_events,
    funding_sum_pct,
    point_change_pct,
    sample_to_series,
    safe_max,
)
from analysis_features.bybit_pump_short_paper import apply_shadow_rows_to_paper
from config import BASE_DIR

DEFAULT_SHADOW_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_shadow"
SLOW_PUMP_WATCH_CONFIGS: tuple[tuple[int, float], ...] = (
    (72, 75.0),
    (168, 75.0),
)
SLOW_PUMP_WATCH_RECENT_HOURS = 336
SLOW_PUMP_WATCH_LATEST_FILE = "slow_pump_watch_latest.csv"
SLOW_PUMP_WATCH_HISTORY_FILE = "slow_pump_watch_history.jsonl"


@dataclass(slots=True)
class ShadowScanConfig:
    output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR
    analysis_dir: Path = DEFAULT_ANALYSIS_OUTPUT_DIR
    lookback_days: int = 14
    sleep_sec: float = 0.8
    max_symbols: int | None = 50
    symbols: list[str] = field(default_factory=list)
    newest_first: bool = True
    recent_event_hours: int = 168
    row_callback: Callable[[dict[str, Any]], None] | None = None


def run_shadow_scan(config: ShadowScanConfig | None = None) -> dict[str, Any]:
    cfg = config or ShadowScanConfig()
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    collector = BybitPumpShortCollector(
        BybitCollectorConfig(
            output_dir=cfg.output_dir,
            lookback_days=cfg.lookback_days,
            sleep_sec=cfg.sleep_sec,
            stop_on_403=True,
        )
    )
    profiles = load_candidate_profiles(cfg.analysis_dir)
    instruments = select_instruments(collector.load_instruments(), cfg)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    scan_ts = now_ms()
    for index, instrument in enumerate(instruments, start=1):
        try:
            sample = collector.collect_symbol(instrument)
            row = classify_shadow_sample(
                sample,
                profiles=profiles,
                scan_ts_ms=scan_ts,
                recent_event_hours=cfg.recent_event_hours,
            )
            row["scan_index"] = index
            row["requests_made"] = collector.stats.requests_made
            rows.append(row)
            if cfg.row_callback is not None:
                try:
                    cfg.row_callback(dict(row))
                except Exception as exc:  # pylint: disable=broad-except
                    errors.append(
                        {
                            "ts_ms": now_ms(),
                            "symbol": instrument.symbol,
                            "stage": "row_callback",
                            "error": str(exc),
                        }
                    )
        except Exception as exc:  # pylint: disable=broad-except
            errors.append({"ts_ms": now_ms(), "symbol": instrument.symbol, "error": str(exc)})

    rows = sorted(rows, key=shadow_sort_key)
    write_csv(cfg.output_dir / "shadow_scan_latest.csv", rows)
    append_jsonl(cfg.output_dir / "shadow_scan_history.jsonl", {"ts_ms": scan_ts, "rows": rows})
    slow_watch_rows = [row for row in rows if row.get("status") == "watch_slow_pump"]
    write_csv(cfg.output_dir / SLOW_PUMP_WATCH_LATEST_FILE, slow_watch_rows)
    append_jsonl(
        cfg.output_dir / SLOW_PUMP_WATCH_HISTORY_FILE,
        {
            "ts_ms": scan_ts,
            "mode": "research_only_no_trades",
            "configs": [
                {"window_h": window_h, "threshold_pct": threshold_pct}
                for window_h, threshold_pct in SLOW_PUMP_WATCH_CONFIGS
            ],
            "rows": slow_watch_rows,
        },
    )
    paper = apply_shadow_rows_to_paper(rows)
    if errors:
        for error in errors:
            append_jsonl(cfg.output_dir / "shadow_errors.jsonl", error)

    metadata = {
        "schema": "bybit_pump_short_shadow_scan_v1",
        "ts_ms": scan_ts,
        "symbols_seen": len(instruments),
        "rows": len(rows),
        "entry_candidates": sum(1 for row in rows if row.get("status") == "entry_candidate"),
        "watchlist": sum(1 for row in rows if str(row.get("status") or "").startswith("watch")),
        "slow_pump_watch": len(slow_watch_rows),
        "blocked": sum(1 for row in rows if str(row.get("status") or "").startswith("blocked")),
        "errors": len(errors),
        "paper_positions": paper.get("positions", 0),
        "paper_open_positions": paper.get("open_positions", 0),
        "paper_closed_positions": paper.get("closed_positions", 0),
        "paper_events": paper.get("events", 0),
        "requests_made": collector.stats.requests_made,
        "lookback_days": cfg.lookback_days,
        "recent_event_hours": cfg.recent_event_hours,
        "elapsed_sec": round(time.time() - started, 3),
    }
    (cfg.output_dir / "shadow_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def classify_shadow_sample(
    sample: dict[str, Any],
    *,
    profiles: list[dict[str, Any]],
    scan_ts_ms: int | None = None,
    recent_event_hours: int = 168,
) -> dict[str, Any]:
    series = sample_to_series(sample)
    latest_idx = len(series.ts) - 1
    scan_ts = scan_ts_ms or now_ms()
    summary = sample.get("summary") if isinstance(sample.get("summary"), dict) else {}
    base = {
        "ts_ms": scan_ts,
        "observed_at_ms": sample.get("ts_ms"),
        "symbol": series.symbol,
        "last_close": summary.get("last_close"),
        "return_24h_pct": summary.get("return_24h_pct"),
        "return_3d_pct": summary.get("return_3d_pct"),
        "return_7d_pct": summary.get("return_7d_pct"),
        "pump_score": summary.get("pump_score"),
        "continuation_risk_score": summary.get("continuation_risk_score"),
        "data_quality": json.dumps(summary.get("data_quality") or {}, ensure_ascii=True, sort_keys=True),
    }
    if latest_idx < 0:
        return {**base, "status": "no_data", "reason": "no_klines"}

    events = detect_pump_events(series)
    event = latest_recent_event(series, events, recent_event_hours=recent_event_hours)
    if event is None:
        slow_watch = classify_slow_pump_watch(series, sample=sample)
        if slow_watch:
            return {
                **base,
                **slow_watch,
                "status": "watch_slow_pump",
                "reason": f"slow_pump_{slow_watch['slow_pump_stage']}",
                "research_mode": "research_only_no_trades",
            }
        return {**base, "status": "no_recent_pump", "reason": "no_pump_trigger_in_recent_window"}

    features = current_event_features(series, event, latest_idx, sample=sample)
    match = match_candidate_profile(features, profiles)
    status, reason = shadow_status(features, match)
    return {
        **base,
        **features,
        "status": status,
        "reason": reason,
        "matched_profile": match.get("profile"),
        "matched_profile_rank": match.get("profile_rank"),
        "matched_entry_strategy": match.get("entry_strategy"),
        "matched_exit_strategy": match.get("exit_strategy"),
        "matched_anti_overfit_status": match.get("anti_overfit_status"),
    }


def current_event_features(series: Series, event: PumpEvent, latest_idx: int, *, sample: dict[str, Any] | None = None) -> dict[str, Any]:
    high_since_trigger = safe_max(series.high[event.trigger_idx : latest_idx + 1])
    current_close = series.close[latest_idx]
    high_from_trigger_pct = (
        (high_since_trigger / event.trigger_close - 1.0) * 100.0
        if high_since_trigger and event.trigger_close
        else None
    )
    pullback_from_high_pct = (
        (1.0 - current_close / high_since_trigger) * 100.0
        if current_close and high_since_trigger
        else None
    )
    latest_ts = series.ts[latest_idx]
    funding_prev_24h = funding_sum_pct(series.funding, latest_ts - 24 * 3_600_000, latest_ts)
    oi_change_4h = point_change_pct(series.oi, series.ts, latest_idx, 4)
    oi_change_24h = point_change_pct(series.oi, series.ts, latest_idx, 24)
    sample_series = sample.get("series") if isinstance(sample, dict) and isinstance(sample.get("series"), dict) else {}
    premium_index = sample_series.get("premium_index_1h") if isinstance(sample_series, dict) else []
    klines = sample_series.get("klines_1h") if isinstance(sample_series, dict) else []
    return {
        "event_id": event.event_id,
        "trigger_ts": event.trigger_ts,
        "hours_since_trigger": round((latest_ts - event.trigger_ts) / 3_600_000.0, 3),
        "config_window_h": event.config_window_h,
        "config_threshold_pct": event.config_threshold_pct,
        "trigger_pump_pct": round_float(event.pump_pct),
        "trigger_close": round_float(event.trigger_close),
        "high_from_trigger_pct": round_float(high_from_trigger_pct),
        "pullback_from_high_pct": round_float(pullback_from_high_pct),
        "oi_change_4h_pct": round_float(oi_change_4h),
        "oi_change_24h_pct": round_float(oi_change_24h),
        "long_ratio": round_float(series.long_ratio.get(latest_ts), 6),
        "funding_prev_24h_pct": round_float(funding_prev_24h),
        "premium_latest_pct": round_float(scale_pct(value_at_or_before(premium_index, latest_ts, "close"))),
        "premium_min_24h_pct": round_float(scale_pct(min_value_between(premium_index, latest_ts - 24 * 3_600_000, latest_ts, "low"))),
        "premium_relief_1h_pct": round_float(scale_pct(pct_point_change(
            value_at_or_before(premium_index, latest_ts, "close"),
            value_at_or_before(premium_index, latest_ts - 3_600_000, "close"),
        ))),
        "volume_z_24h": round_float(latest_volume_z(klines, latest_ts, lookback_rows=24)),
    }


def latest_recent_event(series: Series, events: list[PumpEvent], *, recent_event_hours: int) -> PumpEvent | None:
    if not events or not series.ts:
        return None
    latest_ts = series.ts[-1]
    recent = [
        event for event in events
        if 0 <= latest_ts - event.trigger_ts <= recent_event_hours * 3_600_000
    ]
    return max(recent, key=lambda event: event.trigger_ts) if recent else None


def classify_slow_pump_watch(series: Series, *, sample: dict[str, Any] | None = None) -> dict[str, Any]:
    event = latest_slow_pump_event(series)
    if event is None or not series.ts:
        return {}
    latest_idx = len(series.ts) - 1
    latest_ts = series.ts[latest_idx]
    current_close = series.close[latest_idx]
    high_since_trigger = safe_max(series.high[event["trigger_idx"] : latest_idx + 1])
    pullback = (
        (1.0 - current_close / high_since_trigger) * 100.0
        if current_close and high_since_trigger
        else None
    )
    high_from_trigger = (
        (high_since_trigger / event["trigger_close"] - 1.0) * 100.0
        if high_since_trigger and event["trigger_close"]
        else None
    )
    hours_since_trigger = (latest_ts - event["trigger_ts"]) / 3_600_000.0
    sample_series = sample.get("series") if isinstance(sample, dict) and isinstance(sample.get("series"), dict) else {}
    premium_index = sample_series.get("premium_index_1h") if isinstance(sample_series, dict) else []
    klines = sample_series.get("klines_1h") if isinstance(sample_series, dict) else []
    stage = slow_pump_stage(pullback)
    return {
        "slow_pump_event_id": event["event_id"],
        "slow_pump_trigger_ts": event["trigger_ts"],
        "slow_pump_hours_since_trigger": round(hours_since_trigger, 3),
        "slow_pump_window_h": event["window_h"],
        "slow_pump_threshold_pct": event["threshold_pct"],
        "slow_pump_return_pct": round_float(event["return_pct"]),
        "slow_pump_velocity_pct_per_h": round_float(event["return_pct"] / event["window_h"]),
        "slow_pump_trigger_close": round_float(event["trigger_close"]),
        "slow_pump_high_since_trigger_pct": round_float(high_from_trigger),
        "slow_pump_pullback_from_high_pct": round_float(pullback),
        "slow_pump_stage": stage,
        "slow_pump_funding_prev_24h_pct": round_float(
            funding_sum_pct(series.funding, latest_ts - 24 * 3_600_000, latest_ts)
        ),
        "slow_pump_oi_change_4h_pct": round_float(point_change_pct(series.oi, series.ts, latest_idx, 4)),
        "slow_pump_oi_change_24h_pct": round_float(point_change_pct(series.oi, series.ts, latest_idx, 24)),
        "slow_pump_long_ratio": round_float(series.long_ratio.get(latest_ts), 6),
        "slow_pump_premium_latest_pct": round_float(scale_pct(value_at_or_before(premium_index, latest_ts, "close"))),
        "slow_pump_volume_z_24h": round_float(latest_volume_z(klines, latest_ts, lookback_rows=24)),
    }


def latest_slow_pump_event(series: Series) -> dict[str, Any] | None:
    if not series.ts:
        return None
    latest_ts = series.ts[-1]
    events: list[dict[str, Any]] = []
    for window_h, threshold_pct in SLOW_PUMP_WATCH_CONFIGS:
        cooldown_until = -1
        for idx in range(window_h, len(series.ts)):
            if idx < cooldown_until:
                continue
            current = series.close[idx]
            prior = series.close[idx - window_h]
            prev_current = series.close[idx - 1]
            prev_prior = series.close[idx - 1 - window_h] if idx - 1 - window_h >= 0 else None
            if not current or not prior or not prev_current or not prev_prior:
                continue
            return_pct = (current / prior - 1.0) * 100.0
            previous_return_pct = (prev_current / prev_prior - 1.0) * 100.0
            if return_pct < threshold_pct or previous_return_pct >= threshold_pct:
                continue
            trigger_ts = series.ts[idx]
            if latest_ts - trigger_ts <= SLOW_PUMP_WATCH_RECENT_HOURS * 3_600_000:
                events.append(
                    {
                        "event_id": f"{series.symbol}|slow_w{window_h}|{int(threshold_pct)}|{trigger_ts}",
                        "trigger_idx": idx,
                        "trigger_ts": trigger_ts,
                        "trigger_close": current,
                        "window_h": window_h,
                        "threshold_pct": threshold_pct,
                        "return_pct": return_pct,
                    }
                )
            cooldown_until = idx + max(24, window_h // 2)
    if not events:
        return None
    return max(
        events,
        key=lambda item: (
            item["trigger_ts"],
            item["return_pct"] / item["threshold_pct"],
            -item["window_h"],
        ),
    )


def slow_pump_stage(pullback_pct: float | None) -> str:
    if pullback_pct is None or pullback_pct < 10.0:
        return "rising"
    if pullback_pct < 30.0:
        return "distribution"
    if pullback_pct < 60.0:
        return "breakdown"
    return "capitulation"


def match_candidate_profile(features: dict[str, Any], profiles: list[dict[str, Any]]) -> dict[str, Any]:
    for profile in profiles:
        entry = str(profile.get("entry_strategy") or "")
        if not entry.startswith("pb"):
            continue
        threshold = pullback_threshold_from_strategy(entry)
        oi_max = 0.0 if "oi0_lr_mid" in entry else 50.0 if "oi50_lr_mid" in entry else None
        pullback = to_float(features.get("pullback_from_high_pct"))
        oi_change = to_float(features.get("oi_change_24h_pct"))
        long_ratio = to_float(features.get("long_ratio"))
        funding = to_float(features.get("funding_prev_24h_pct"))
        if threshold is None or oi_max is None:
            continue
        if pullback is None or pullback < threshold:
            continue
        if oi_change is None or oi_change > oi_max:
            continue
        if long_ratio is None or not (0.45 <= long_ratio <= 0.65):
            continue
        if funding is not None and funding <= -1.0:
            continue
        if profile.get("anti_overfit_status") not in {"robust_candidate", "needs_more_validation"}:
            continue
        return profile
    return {}


def shadow_status(features: dict[str, Any], match: dict[str, Any]) -> tuple[str, str]:
    if match:
        return "entry_candidate", "matched_profile_conditions"
    oi_change = to_float(features.get("oi_change_24h_pct"))
    pullback = to_float(features.get("pullback_from_high_pct"))
    long_ratio = to_float(features.get("long_ratio"))
    funding = to_float(features.get("funding_prev_24h_pct"))
    if oi_change is not None and oi_change > 200.0:
        return "blocked_continuation", "oi_explosion_gt_200"
    if funding is not None and funding <= -1.0:
        return "blocked_funding", "funding_prev_24h_lte_minus_1"
    if long_ratio is not None and not (0.45 <= long_ratio <= 0.65):
        return "watch_ratio", "long_ratio_outside_45_65"
    if pullback is None or pullback < 10.0:
        return "watch_pullback", "waiting_pullback_10"
    if oi_change is None:
        return "watch_oi", "missing_oi_confirmation"
    if oi_change > 50.0:
        return "watch_oi", "waiting_oi_not_exploding"
    return "watch_profile", "conditions_do_not_match_selected_profiles"


def load_candidate_profiles(analysis_dir: Path = DEFAULT_ANALYSIS_OUTPUT_DIR) -> list[dict[str, Any]]:
    path = analysis_dir / "candidate_rule_profiles.csv"
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return []
    profile_order = {"conservative": 0, "balanced": 1, "aggressive": 2}
    rows.sort(
        key=lambda row: (
            profile_order.get(str(row.get("profile") or ""), 99),
            int(float(row.get("profile_rank") or 999)),
        )
    )
    return rows


def select_instruments(instruments: list[BybitInstrument], config: ShadowScanConfig) -> list[BybitInstrument]:
    requested = {normalize_symbol(item) for item in config.symbols if normalize_symbol(item)}
    if requested:
        instruments = [item for item in instruments if item.symbol in requested]
    else:
        instruments = [item for item in instruments if is_crypto_pump_short_instrument(item)]
    instruments = sorted(
        instruments,
        key=lambda item: item.launch_time_ms or 0,
        reverse=config.newest_first,
    )
    instruments = dedupe_instruments(instruments)
    if config.max_symbols is not None:
        instruments = instruments[: max(0, config.max_symbols)]
    return instruments


def pullback_threshold_from_strategy(strategy: str) -> float | None:
    if not strategy.startswith("pb"):
        return None
    digits = []
    for char in strategy[2:]:
        if not char.isdigit():
            break
        digits.append(char)
    return float("".join(digits)) if digits else None


def shadow_sort_key(row: dict[str, Any]) -> tuple[int, float, float]:
    status_rank = {
        "entry_candidate": 0,
        "watch_pullback": 1,
        "watch_oi": 2,
        "watch_ratio": 3,
        "watch_profile": 4,
        "watch_slow_pump": 5,
        "blocked_continuation": 6,
        "blocked_funding": 7,
        "no_recent_pump": 8,
        "no_data": 9,
    }
    return (
        status_rank.get(str(row.get("status") or ""), 99),
        -(to_float(row.get("trigger_pump_pct")) or 0.0),
        -(to_float(row.get("pullback_from_high_pct")) or 0.0),
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def round_float(value: Any, digits: int = 6) -> float | None:
    number = to_float(value)
    return round(number, digits) if number is not None else None


def to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in {float("inf"), float("-inf")} else None


def to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def scale_pct(value: Any) -> float | None:
    number = to_float(value)
    return number * 100.0 if number is not None else None


def value_at_or_before(rows: list[dict[str, Any]], ts_ms: int, key: str) -> float | None:
    out: float | None = None
    for row in rows or []:
        row_ts = to_int(row.get("ts_ms"))
        if row_ts is None or row_ts > ts_ms:
            continue
        value = to_float(row.get(key))
        if value is not None:
            out = value
    return out


def min_value_between(rows: list[dict[str, Any]], start_ms: int, end_ms: int, key: str) -> float | None:
    values: list[float] = []
    for row in rows or []:
        row_ts = to_int(row.get("ts_ms"))
        if row_ts is None or row_ts < start_ms or row_ts > end_ms:
            continue
        value = to_float(row.get(key))
        if value is not None:
            values.append(value)
    return min(values) if values else None


def pct_point_change(current: Any, prior: Any) -> float | None:
    current_value = to_float(current)
    prior_value = to_float(prior)
    if current_value is None or prior_value is None:
        return None
    return current_value - prior_value


def latest_volume_z(rows: list[dict[str, Any]], ts_ms: int, *, lookback_rows: int = 24) -> float | None:
    ordered = sorted(
        (row for row in rows or [] if to_int(row.get("ts_ms")) is not None and (to_int(row.get("ts_ms")) or 0) <= ts_ms),
        key=lambda row: to_int(row.get("ts_ms")) or 0,
    )
    if len(ordered) < 4:
        return None
    latest = to_float(ordered[-1].get("volume"))
    history = [to_float(row.get("volume")) for row in ordered[-lookback_rows - 1 : -1]]
    values = [value for value in history if value is not None]
    if latest is None or len(values) < 3:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    std = math.sqrt(variance)
    if std <= 0:
        return None
    return (latest - mean) / std


__all__ = [
    "DEFAULT_SHADOW_OUTPUT_DIR",
    "SLOW_PUMP_WATCH_CONFIGS",
    "SLOW_PUMP_WATCH_HISTORY_FILE",
    "SLOW_PUMP_WATCH_LATEST_FILE",
    "SLOW_PUMP_WATCH_RECENT_HOURS",
    "ShadowScanConfig",
    "classify_slow_pump_watch",
    "classify_shadow_sample",
    "load_candidate_profiles",
    "run_shadow_scan",
]
