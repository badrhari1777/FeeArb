from __future__ import annotations

import csv
import json
import math
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_analysis"

PUMP_CONFIGS: tuple[tuple[int, float], ...] = (
    (4, 50.0),
    (8, 80.0),
    (12, 100.0),
    (24, 150.0),
    (72, 250.0),
    (168, 400.0),
)
HORIZONS_HOURS: tuple[int, ...] = (24, 72, 168, 336, 720)
FEE_ROUNDTRIP_PCT = 0.18
EPISODE_SCAN_HOURS = 720
EPISODE_GAP_HOURS = 168
EPISODE_DRAWDOWN_END_PCT = 70.0
EXIT_PLANS: tuple[dict[str, Any], ...] = (
    {"name": "time_full_72", "max_hold_h": 72, "targets": ()},
    {"name": "time_full_168", "max_hold_h": 168, "targets": ()},
    {"name": "time_full_336", "max_hold_h": 336, "targets": ()},
    {"name": "tp25_full_168", "max_hold_h": 168, "targets": ((25.0, 1.0),)},
    {"name": "tp50_full_336", "max_hold_h": 336, "targets": ((50.0, 1.0),)},
    {"name": "tp70_full_336", "max_hold_h": 336, "targets": ((70.0, 1.0),)},
    {"name": "tp25_half_time_168", "max_hold_h": 168, "targets": ((25.0, 0.5),)},
    {"name": "tp25_50_halves_336", "max_hold_h": 336, "targets": ((25.0, 0.5), (50.0, 1.0))},
    {"name": "tp25_50_70_thirds_336", "max_hold_h": 336, "targets": ((25.0, 0.33), (50.0, 0.33), (70.0, 1.0))},
    {"name": "tp30_60_runner_336", "max_hold_h": 336, "targets": ((30.0, 0.5), (60.0, 0.6))},
)
LONG_HOLD_EXIT_PLANS: tuple[dict[str, Any], ...] = (
    {"name": "long_full_30d", "max_hold_h": 720, "targets": (), "time_covers": ()},
    {"name": "long_full_90d", "max_hold_h": 2160, "targets": (), "time_covers": ()},
    {"name": "long_full_180d", "max_hold_h": 4320, "targets": (), "time_covers": ()},
    {"name": "long_full_365d", "max_hold_h": 8760, "targets": (), "time_covers": ()},
    {"name": "cover25_72h_runner_90d", "max_hold_h": 2160, "targets": (), "time_covers": ((72, 0.25),)},
    {"name": "cover50_72h_runner_90d", "max_hold_h": 2160, "targets": (), "time_covers": ((72, 0.50),)},
    {"name": "cover25_168h_runner_180d", "max_hold_h": 4320, "targets": (), "time_covers": ((168, 0.25),)},
    {"name": "cover50_168h_runner_180d", "max_hold_h": 4320, "targets": (), "time_covers": ((168, 0.50),)},
    {"name": "cover25_30d_runner_365d", "max_hold_h": 8760, "targets": (), "time_covers": ((720, 0.25),)},
    {"name": "cover50_30d_runner_365d", "max_hold_h": 8760, "targets": (), "time_covers": ((720, 0.50),)},
    {"name": "tp25_half_runner_180d", "max_hold_h": 4320, "targets": ((25.0, 0.50),), "time_covers": ()},
    {"name": "tp50_half_runner_365d", "max_hold_h": 8760, "targets": ((50.0, 0.50),), "time_covers": ()},
    {
        "name": "tp25_half_cover25_30d_runner_365d",
        "max_hold_h": 8760,
        "targets": ((25.0, 0.50),),
        "time_covers": ((720, 0.25),),
    },
)
RULE_PROFILES: tuple[dict[str, Any], ...] = (
    {
        "profile": "conservative",
        "min_n": 150,
        "min_win_pct": 75.0,
        "min_avg_net_pct": 10.0,
        "min_median_net_pct": 15.0,
        "max_p90_mae_pct": 60.0,
        "max_p95_mae_pct": 90.0,
        "max_cat300_pct": 0.0,
        "max_liq3x_pct": 40.0,
    },
    {
        "profile": "balanced",
        "min_n": 150,
        "min_win_pct": 78.0,
        "min_avg_net_pct": 15.0,
        "min_median_net_pct": 20.0,
        "max_p90_mae_pct": 120.0,
        "max_p95_mae_pct": 170.0,
        "max_cat300_pct": 1.1,
        "max_liq3x_pct": 55.0,
    },
    {
        "profile": "aggressive",
        "min_n": 150,
        "min_win_pct": 75.0,
        "min_avg_net_pct": 20.0,
        "min_median_net_pct": 25.0,
        "max_p90_mae_pct": 180.0,
        "max_p95_mae_pct": 260.0,
        "max_cat300_pct": 3.0,
        "max_liq3x_pct": 65.0,
    },
)
TIME_TRAIN_RATIO = 2.0 / 3.0
SYMBOL_HOLDOUT_MOD = 3
FUNDING_HEAVY_NEG_PCT = -0.5
FUNDING_EXTREME_NEG_PCT = -2.0
REGIME_MIN_N = 10


@dataclass(slots=True)
class Series:
    symbol: str
    launch_ms: int | None
    ts: list[int]
    open: list[float | None]
    high: list[float | None]
    low: list[float | None]
    close: list[float | None]
    funding: list[tuple[int, float]]
    oi: dict[int, float]
    long_ratio: dict[int, float]


@dataclass(slots=True)
class PumpEvent:
    event_id: str
    symbol: str
    config_window_h: int
    config_threshold_pct: float
    trigger_idx: int
    trigger_ts: int
    pump_pct: float
    trigger_close: float
    age_days: float | None
    funding_prev_24h_pct: float | None
    funding_prev_72h_pct: float | None
    oi_change_4h_pct: float | None
    oi_change_24h_pct: float | None
    long_ratio: float | None


@dataclass(slots=True)
class PumpEpisode:
    episode_id: str
    symbol: str
    start_idx: int
    start_ts: int
    end_idx: int
    end_ts: int
    duration_h: int
    event_count: int
    first_event_id: str
    first_config_window_h: int
    first_config_threshold_pct: float
    start_close: float
    episode_high: float | None
    episode_low_after_high: float | None
    high_from_start_pct: float | None
    max_drawdown_from_high_pct: float | None
    time_to_high_h: int | None
    final_return_from_start_pct: float | None
    max_trigger_pump_pct: float
    age_days: float | None
    funding_prev_24h_pct: float | None
    oi_change_24h_pct: float | None
    long_ratio: float | None


def run_analysis(
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    events: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    exit_outcomes: list[dict[str, Any]] = []
    long_hold_outcomes: list[dict[str, Any]] = []
    started = time.time()
    symbols_seen = 0
    for sample in load_samples(input_path):
        symbols_seen += 1
        series = sample_to_series(sample)
        symbol_events = detect_pump_events(series)
        episodes.extend(episode_to_row(episode) for episode in detect_pump_episodes(series, symbol_events))
        for event in symbol_events:
            event_row = event_to_row(event)
            event_row.update(event_behavior_features(series, event))
            events.append(event_row)
            outcomes.extend(simulate_event_outcomes(series, event))
            exit_outcomes.extend(simulate_event_exit_outcomes(series, event))
            long_hold_outcomes.extend(simulate_event_long_hold_outcomes(series, event))

    write_csv(output_dir / "pump_events.csv", events)
    write_csv(output_dir / "pump_episodes.csv", episodes)
    write_csv(output_dir / "strategy_outcomes.csv", outcomes)
    write_csv(output_dir / "exit_strategy_outcomes.csv", exit_outcomes)
    write_csv(output_dir / "long_hold_outcomes.csv", long_hold_outcomes)
    strategy_summary = build_strategy_summary(outcomes)
    filter_summary = build_filter_summary(outcomes)
    entry_rule_summary = build_entry_rule_summary(outcomes)
    exit_rule_summary = build_exit_rule_summary(exit_outcomes)
    long_hold_rule_summary = build_long_hold_rule_summary(long_hold_outcomes)
    best_rules = build_best_rules(exit_rule_summary)
    best_long_hold_rules = build_best_long_hold_rules(long_hold_rule_summary)
    candidate_profiles = build_candidate_rule_profiles(exit_rule_summary)
    robustness_time_split = build_time_split_robustness(exit_outcomes)
    robustness_symbol_holdout = build_symbol_holdout_robustness(exit_outcomes)
    symbol_concentration = build_symbol_concentration(exit_outcomes)
    anti_overfit_report = build_anti_overfit_report(robustness_time_split, robustness_symbol_holdout, symbol_concentration)
    candidate_profiles = annotate_candidate_profiles(candidate_profiles, anti_overfit_report)
    worst_tail_events = build_worst_tail_events(exit_outcomes)
    symbol_summary = build_symbol_summary(outcomes)
    funding_regime_summary = build_regime_summary(exit_outcomes, ("funding_regime",))
    oi_regime_summary = build_regime_summary(exit_outcomes, ("oi_regime",))
    behavior_regime_summary = build_regime_summary(exit_outcomes, ("funding_regime", "oi_regime"))
    behavior_regime_recommendations = build_behavior_regime_recommendations(behavior_regime_summary)
    strategy_recommendations = build_strategy_recommendations(candidate_profiles, anti_overfit_report, behavior_regime_recommendations)
    write_csv(output_dir / "strategy_summary.csv", strategy_summary)
    write_csv(output_dir / "filter_summary.csv", filter_summary)
    write_csv(output_dir / "entry_rule_summary.csv", entry_rule_summary)
    write_csv(output_dir / "exit_rule_summary.csv", exit_rule_summary)
    write_csv(output_dir / "long_hold_rule_summary.csv", long_hold_rule_summary)
    write_csv(output_dir / "best_rules.csv", best_rules)
    write_csv(output_dir / "best_long_hold_rules.csv", best_long_hold_rules)
    write_csv(output_dir / "candidate_rule_profiles.csv", candidate_profiles)
    write_csv(output_dir / "robustness_time_split.csv", robustness_time_split)
    write_csv(output_dir / "robustness_symbol_holdout.csv", robustness_symbol_holdout)
    write_csv(output_dir / "symbol_concentration.csv", symbol_concentration)
    write_csv(output_dir / "anti_overfit_report.csv", anti_overfit_report)
    write_csv(output_dir / "worst_tail_events.csv", worst_tail_events)
    write_csv(output_dir / "symbol_event_summary.csv", symbol_summary)
    write_csv(output_dir / "funding_regime_summary.csv", funding_regime_summary)
    write_csv(output_dir / "oi_regime_summary.csv", oi_regime_summary)
    write_csv(output_dir / "behavior_regime_summary.csv", behavior_regime_summary)
    write_csv(output_dir / "behavior_regime_recommendations.csv", behavior_regime_recommendations)
    (output_dir / "strategy_recommendations.json").write_text(
        json.dumps(strategy_recommendations, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    metadata = {
        "schema": "bybit_pump_short_outcome_analysis_v2",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": len(events),
        "episodes": len(episodes),
        "outcomes": len(outcomes),
        "exit_outcomes": len(exit_outcomes),
        "long_hold_outcomes": len(long_hold_outcomes),
        "best_rules": len(best_rules),
        "best_long_hold_rules": len(best_long_hold_rules),
        "candidate_rule_profiles": len(candidate_profiles),
        "robustness_time_split": len(robustness_time_split),
        "robustness_symbol_holdout": len(robustness_symbol_holdout),
        "symbol_concentration": len(symbol_concentration),
        "anti_overfit_report": len(anti_overfit_report),
        "worst_tail_events": len(worst_tail_events),
        "funding_regime_summary": len(funding_regime_summary),
        "oi_regime_summary": len(oi_regime_summary),
        "behavior_regime_summary": len(behavior_regime_summary),
        "behavior_regime_recommendations": len(behavior_regime_recommendations),
        "pump_configs": [{"window_h": w, "threshold_pct": t} for w, t in PUMP_CONFIGS],
        "horizons_h": list(HORIZONS_HOURS),
        "exit_plans": [str(plan["name"]) for plan in EXIT_PLANS],
        "long_hold_exit_plans": [str(plan["name"]) for plan in LONG_HOLD_EXIT_PLANS],
        "rule_profiles": [str(profile["profile"]) for profile in RULE_PROFILES],
        "funding_heavy_neg_pct": FUNDING_HEAVY_NEG_PCT,
        "funding_extreme_neg_pct": FUNDING_EXTREME_NEG_PCT,
        "regime_min_n": REGIME_MIN_N,
        "time_train_ratio": TIME_TRAIN_RATIO,
        "symbol_holdout_mod": SYMBOL_HOLDOUT_MOD,
        "fee_roundtrip_pct": FEE_ROUNDTRIP_PCT,
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "analysis_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def load_samples(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            yield json.loads(line)


def sample_to_series(sample: dict[str, Any]) -> Series:
    klines = sample.get("series", {}).get("klines_1h") or []
    rows = sorted((row for row in klines if row.get("ts_ms") is not None), key=lambda row: int(row["ts_ms"]))
    funding = [
        (int(row["ts_ms"]), float(row["funding_rate"]))
        for row in sample.get("series", {}).get("funding", [])
        if row.get("ts_ms") is not None and row.get("funding_rate") is not None
    ]
    oi = {
        int(row["ts_ms"]): float(row["open_interest"])
        for row in sample.get("series", {}).get("open_interest_1h", [])
        if row.get("ts_ms") is not None and row.get("open_interest") is not None
    }
    long_ratio = {
        int(row["ts_ms"]): float(row["buy_ratio"])
        for row in sample.get("series", {}).get("long_short_1h", [])
        if row.get("ts_ms") is not None and row.get("buy_ratio") is not None
    }
    instrument = sample.get("instrument") or {}
    return Series(
        symbol=str(sample.get("symbol") or "").upper(),
        launch_ms=to_int(instrument.get("launch_time_ms")),
        ts=[int(row["ts_ms"]) for row in rows],
        open=[to_float(row.get("open")) for row in rows],
        high=[to_float(row.get("high")) for row in rows],
        low=[to_float(row.get("low")) for row in rows],
        close=[to_float(row.get("close")) for row in rows],
        funding=sorted(funding),
        oi=oi,
        long_ratio=long_ratio,
    )


def detect_pump_events(series: Series) -> list[PumpEvent]:
    events: list[PumpEvent] = []
    n = len(series.ts)
    if n < 12:
        return events
    for window_h, threshold_pct in PUMP_CONFIGS:
        cooldown_until = -1
        for idx in range(window_h, n):
            if idx < cooldown_until:
                continue
            current = series.close[idx]
            prior = series.close[idx - window_h]
            prev_current = series.close[idx - 1]
            prev_prior = series.close[idx - 1 - window_h] if idx - 1 - window_h >= 0 else None
            if not current or not prior or not prev_current or not prev_prior:
                continue
            pump_pct = (current / prior - 1.0) * 100.0
            prev_pump_pct = (prev_current / prev_prior - 1.0) * 100.0
            if pump_pct < threshold_pct or prev_pump_pct >= threshold_pct:
                continue
            # After a threshold crossing, one strategic decision is enough for
            # this config; skip a cooldown to avoid counting the same mania many times.
            event = build_event(series, idx, window_h, threshold_pct, pump_pct)
            events.append(event)
            cooldown_until = idx + max(24, window_h // 2)
    return events


def detect_pump_episodes(series: Series, events: list[PumpEvent] | None = None) -> list[PumpEpisode]:
    symbol_events = sorted(events if events is not None else detect_pump_events(series), key=lambda event: event.trigger_idx)
    if not symbol_events:
        return []

    clusters: list[list[PumpEvent]] = []
    current: list[PumpEvent] = []
    for event in symbol_events:
        if not current:
            current = [event]
            continue
        prev = current[-1]
        if event.trigger_idx - prev.trigger_idx <= EPISODE_GAP_HOURS:
            current.append(event)
        else:
            clusters.append(current)
            current = [event]
    if current:
        clusters.append(current)

    episodes: list[PumpEpisode] = []
    for cluster in clusters:
        episode = build_episode(series, cluster)
        if episode is not None:
            episodes.append(episode)
    return episodes


def build_episode(series: Series, events: list[PumpEvent]) -> PumpEpisode | None:
    first = events[0]
    start_idx = first.trigger_idx
    start_close = series.close[start_idx]
    if not start_close:
        return None
    scan_end_idx = min(len(series.ts) - 1, start_idx + EPISODE_SCAN_HOURS)
    high_idx: int | None = None
    episode_high: float | None = None
    for idx in range(start_idx, scan_end_idx + 1):
        high = series.high[idx]
        if high is None:
            continue
        if episode_high is None or high > episode_high:
            episode_high = high
            high_idx = idx

    end_idx = scan_end_idx
    episode_low_after_high: float | None = None
    if episode_high is not None and high_idx is not None:
        drawdown_level = episode_high * (1.0 - EPISODE_DRAWDOWN_END_PCT / 100.0)
        for idx in range(high_idx, scan_end_idx + 1):
            low = series.low[idx]
            if low is None:
                continue
            episode_low_after_high = low if episode_low_after_high is None else min(episode_low_after_high, low)
            if low <= drawdown_level:
                end_idx = idx
                break
    if episode_low_after_high is None and high_idx is not None:
        episode_low_after_high = safe_min(series.low[high_idx : scan_end_idx + 1])

    end_close = series.close[end_idx]
    high_from_start_pct = ((episode_high / start_close - 1.0) * 100.0) if episode_high else None
    max_drawdown = (
        ((1.0 - episode_low_after_high / episode_high) * 100.0)
        if episode_high and episode_low_after_high is not None
        else None
    )
    final_return = ((end_close / start_close - 1.0) * 100.0) if end_close else None
    time_to_high_h = (high_idx - start_idx) if high_idx is not None else None
    return PumpEpisode(
        episode_id=f"{series.symbol}|episode|{series.ts[start_idx]}",
        symbol=series.symbol,
        start_idx=start_idx,
        start_ts=series.ts[start_idx],
        end_idx=end_idx,
        end_ts=series.ts[end_idx],
        duration_h=end_idx - start_idx,
        event_count=len(events),
        first_event_id=first.event_id,
        first_config_window_h=first.config_window_h,
        first_config_threshold_pct=first.config_threshold_pct,
        start_close=float(start_close),
        episode_high=episode_high,
        episode_low_after_high=episode_low_after_high,
        high_from_start_pct=high_from_start_pct,
        max_drawdown_from_high_pct=max_drawdown,
        time_to_high_h=time_to_high_h,
        final_return_from_start_pct=final_return,
        max_trigger_pump_pct=max(event.pump_pct for event in events),
        age_days=first.age_days,
        funding_prev_24h_pct=first.funding_prev_24h_pct,
        oi_change_24h_pct=first.oi_change_24h_pct,
        long_ratio=first.long_ratio,
    )


def build_event(
    series: Series,
    idx: int,
    window_h: int,
    threshold_pct: float,
    pump_pct: float,
) -> PumpEvent:
    ts_ms = series.ts[idx]
    age_days = ((ts_ms - series.launch_ms) / 86_400_000.0) if series.launch_ms else None
    event_id = f"{series.symbol}|w{window_h}|{int(threshold_pct)}|{ts_ms}"
    return PumpEvent(
        event_id=event_id,
        symbol=series.symbol,
        config_window_h=window_h,
        config_threshold_pct=threshold_pct,
        trigger_idx=idx,
        trigger_ts=ts_ms,
        pump_pct=pump_pct,
        trigger_close=float(series.close[idx] or 0.0),
        age_days=age_days,
        funding_prev_24h_pct=funding_sum_pct(series.funding, ts_ms - 24 * 3_600_000, ts_ms),
        funding_prev_72h_pct=funding_sum_pct(series.funding, ts_ms - 72 * 3_600_000, ts_ms),
        oi_change_4h_pct=point_change_pct(series.oi, series.ts, idx, 4),
        oi_change_24h_pct=point_change_pct(series.oi, series.ts, idx, 24),
        long_ratio=series.long_ratio.get(ts_ms),
    )


def simulate_event_outcomes(series: Series, event: PumpEvent) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    entries = build_entry_strategies(series, event)
    for strategy, legs in entries:
        for horizon_h in HORIZONS_HOURS:
            row = outcome_row(series, event, strategy, legs, horizon_h)
            if row:
                rows.append(row)
    return rows


def build_entry_strategies(series: Series, event: PumpEvent) -> list[tuple[str, list[tuple[int, float]]]]:
    entries: list[tuple[str, list[tuple[int, float]]]] = []
    entries.append(("immediate", [(event.trigger_idx, event.trigger_close)]))
    for pullback_pct in (10.0, 20.0, 30.0):
        idx = find_pullback_entry(series, event.trigger_idx, pullback_pct, max_wait_h=168)
        if idx is not None and series.close[idx]:
            entries.append((f"pullback_{int(pullback_pct)}", [(idx, float(series.close[idx]))]))
    for step_pct in (30.0, 50.0, 100.0):
        legs = ladder_entries(series, event.trigger_idx, step_pct=step_pct, max_legs=3, max_wait_h=168)
        if legs:
            entries.append((f"ladder3_step_{int(step_pct)}", legs))
    for pullback_pct in (10.0, 20.0, 30.0):
        add_confirmed_pullback_entries(
            entries,
            series,
            event,
            pullback_pct=pullback_pct,
            oi_max_pct=50.0,
            label="oi50_lr_mid",
        )
        add_confirmed_pullback_entries(
            entries,
            series,
            event,
            pullback_pct=pullback_pct,
            oi_max_pct=0.0,
            label="oi0_lr_mid",
        )
    return entries


def simulate_event_exit_outcomes(series: Series, event: PumpEvent) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry_strategy, legs in build_entry_strategies(series, event):
        for plan in EXIT_PLANS:
            row = exit_outcome_row(series, event, entry_strategy, legs, plan)
            if row:
                rows.append(row)
    return rows


def simulate_event_long_hold_outcomes(series: Series, event: PumpEvent) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry_strategy, legs in build_entry_strategies(series, event):
        for plan in LONG_HOLD_EXIT_PLANS:
            row = exit_outcome_row(series, event, entry_strategy, legs, plan)
            if row:
                row["long_hold_plan"] = True
                rows.append(row)
    return rows


def add_confirmed_pullback_entries(
    entries: list[tuple[str, list[tuple[int, float]]]],
    series: Series,
    event: PumpEvent,
    *,
    pullback_pct: float,
    oi_max_pct: float,
    label: str,
) -> None:
    idx = find_confirmed_pullback_entry(
        series,
        event.trigger_idx,
        pullback_pct,
        max_wait_h=168,
        oi_max_pct=oi_max_pct,
        long_ratio_min=0.45,
        long_ratio_max=0.65,
        funding_min_pct=-1.0,
    )
    if idx is None or not series.close[idx]:
        return
    strategy = f"pb{int(pullback_pct)}_{label}"
    entries.append((strategy, [(idx, float(series.close[idx]))]))
    ladder = ladder_entries(series, idx, step_pct=50.0, max_legs=3, max_wait_h=168)
    if ladder:
        entries.append((f"{strategy}_ladder3_step_50", ladder))


def find_pullback_entry(
    series: Series,
    trigger_idx: int,
    pullback_pct: float,
    *,
    max_wait_h: int,
) -> int | None:
    high_water = series.high[trigger_idx] or series.close[trigger_idx]
    end_idx = min(len(series.ts) - 1, trigger_idx + max_wait_h)
    for idx in range(trigger_idx + 1, end_idx + 1):
        current_high = series.high[idx]
        if current_high is not None and (high_water is None or current_high > high_water):
            high_water = current_high
        close = series.close[idx]
        if high_water and close and close <= high_water * (1.0 - pullback_pct / 100.0):
            return idx
    return None


def find_confirmed_pullback_entry(
    series: Series,
    trigger_idx: int,
    pullback_pct: float,
    *,
    max_wait_h: int,
    oi_max_pct: float,
    long_ratio_min: float,
    long_ratio_max: float,
    funding_min_pct: float,
) -> int | None:
    high_water = series.high[trigger_idx] or series.close[trigger_idx]
    end_idx = min(len(series.ts) - 1, trigger_idx + max_wait_h)
    for idx in range(trigger_idx + 1, end_idx + 1):
        current_high = series.high[idx]
        if current_high is not None and (high_water is None or current_high > high_water):
            high_water = current_high
        close = series.close[idx]
        if not high_water or not close or close > high_water * (1.0 - pullback_pct / 100.0):
            continue
        oi_change = point_change_pct(series.oi, series.ts, idx, 24)
        long_ratio = series.long_ratio.get(series.ts[idx])
        funding_prev_24h = funding_sum_pct(series.funding, series.ts[idx] - 24 * 3_600_000, series.ts[idx])
        if oi_change is None or oi_change > oi_max_pct:
            continue
        if long_ratio is None or not (long_ratio_min <= long_ratio <= long_ratio_max):
            continue
        if funding_prev_24h is not None and funding_prev_24h <= funding_min_pct:
            continue
        return idx
    return None


def ladder_entries(
    series: Series,
    trigger_idx: int,
    *,
    step_pct: float,
    max_legs: int,
    max_wait_h: int,
) -> list[tuple[int, float]]:
    first_price = series.close[trigger_idx]
    if not first_price:
        return []
    legs = [(trigger_idx, float(first_price))]
    next_level = float(first_price) * (1.0 + step_pct / 100.0)
    end_idx = min(len(series.ts) - 1, trigger_idx + max_wait_h)
    for idx in range(trigger_idx + 1, end_idx + 1):
        high = series.high[idx]
        close = series.close[idx]
        if high is None or close is None:
            continue
        if high >= next_level:
            legs.append((idx, next_level))
            if len(legs) >= max_legs:
                break
            next_level = float(first_price) * (1.0 + step_pct / 100.0 * len(legs))
    return legs


def outcome_row(
    series: Series,
    event: PumpEvent,
    strategy: str,
    legs: list[tuple[int, float]],
    horizon_h: int,
) -> dict[str, Any] | None:
    if not legs:
        return None
    entry_idx = legs[0][0]
    exit_idx = min(len(series.ts) - 1, entry_idx + horizon_h)
    if exit_idx <= entry_idx:
        return None
    exit_price = series.close[exit_idx]
    if not exit_price:
        return None
    active_legs = [(idx, price) for idx, price in legs if idx <= exit_idx and price > 0]
    if not active_legs:
        return None
    net_components: list[float] = []
    funding_components: list[float] = []
    for leg_idx, entry_price in active_legs:
        price_pnl_pct = (1.0 - float(exit_price) / entry_price) * 100.0
        funding_pct = funding_sum_pct(series.funding, series.ts[leg_idx], series.ts[exit_idx]) or 0.0
        funding_components.append(funding_pct)
        net_components.append(price_pnl_pct + funding_pct - FEE_ROUNDTRIP_PCT)
    avg_entry = statistics.mean(price for _, price in active_legs)
    max_high = safe_max(series.high[entry_idx + 1 : exit_idx + 1])
    min_low = safe_min(series.low[entry_idx + 1 : exit_idx + 1])
    mae_pct = ((max_high / avg_entry - 1.0) * 100.0) if max_high and avg_entry else None
    mfe_pct = ((1.0 - min_low / avg_entry) * 100.0) if min_low and avg_entry else None
    row = event_to_row(event)
    row.update(event_behavior_features(series, event))
    row.update(
        {
            "strategy": strategy,
            "horizon_h": horizon_h,
            "entry_ts": series.ts[entry_idx],
            "entry_price_avg": avg_entry,
            "legs": len(active_legs),
            "exit_ts": series.ts[exit_idx],
            "exit_price": exit_price,
            "net_exit_pct": statistics.mean(net_components),
            "funding_during_pct": statistics.mean(funding_components),
            "mae_pct": mae_pct,
            "mfe_pct": mfe_pct,
            "win": 1 if statistics.mean(net_components) > 0 else 0,
            "catastrophic_100": 1 if mae_pct is not None and mae_pct >= 100.0 else 0,
            "catastrophic_300": 1 if mae_pct is not None and mae_pct >= 300.0 else 0,
            "liquidation_proxy_3x": 1 if mae_pct is not None and mae_pct >= 33.0 else 0,
            "liquidation_proxy_1x": 1 if mae_pct is not None and mae_pct >= 100.0 else 0,
            "filter_funding_not_toxic": bool_or_none(event.funding_prev_24h_pct is not None and event.funding_prev_24h_pct > -0.5),
            "filter_oi_cooling_24h": bool_or_none(event.oi_change_24h_pct is not None and event.oi_change_24h_pct <= 0.0),
            "filter_oi_not_exploding_24h": bool_or_none(event.oi_change_24h_pct is not None and event.oi_change_24h_pct <= 50.0),
            "filter_long_ratio_low": bool_or_none(event.long_ratio is not None and event.long_ratio <= 0.55),
            "filter_long_ratio_high": bool_or_none(event.long_ratio is not None and event.long_ratio >= 0.60),
            "filter_age_lt_30d": bool_or_none(event.age_days is not None and event.age_days < 30.0),
        }
    )
    return row


def exit_outcome_row(
    series: Series,
    event: PumpEvent,
    entry_strategy: str,
    legs: list[tuple[int, float]],
    plan: dict[str, Any],
) -> dict[str, Any] | None:
    if not legs:
        return None
    entry_idx = legs[0][0]
    max_hold_h = int(plan["max_hold_h"])
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + max_hold_h)
    if exit_limit_idx <= entry_idx:
        return None

    pending_legs = sorted((idx, float(price)) for idx, price in legs if price > 0)
    active: list[dict[str, float | int]] = [
        {"idx": leg_idx, "price": leg_price, "weight": 1.0}
        for leg_idx, leg_price in pending_legs
        if leg_idx == entry_idx
    ]
    realized: list[tuple[float, float, float]] = []
    exit_events: list[str] = []
    target_idx = 0
    time_cover_idx = 0
    max_mae: float | None = None
    max_mfe: float | None = None
    exit_idx = exit_limit_idx
    exit_reason = "time_stop"

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        for leg_idx, leg_price in pending_legs:
            if leg_idx == idx:
                active.append({"idx": leg_idx, "price": leg_price, "weight": 1.0})
        active = [item for item in active if float(item["weight"]) > 1e-9]
        if not active:
            continue

        avg_entry = weighted_avg_entry(active)
        high = series.high[idx]
        low = series.low[idx]
        if high is not None and avg_entry:
            mae_now = (high / avg_entry - 1.0) * 100.0
            max_mae = mae_now if max_mae is None else max(max_mae, mae_now)
        if low is not None and avg_entry:
            mfe_now = (1.0 - low / avg_entry) * 100.0
            max_mfe = mfe_now if max_mfe is None else max(max_mfe, mfe_now)

        targets = tuple(plan.get("targets") or ())
        while target_idx < len(targets) and active:
            target_pct, close_fraction = targets[target_idx]
            avg_entry = weighted_avg_entry(active)
            if not avg_entry or low is None:
                break
            target_price = avg_entry * (1.0 - float(target_pct) / 100.0)
            if low > target_price:
                break
            closed = close_active_fraction(
                series,
                active,
                exit_idx=idx,
                fill_price=target_price,
                fraction=float(close_fraction),
            )
            realized.extend(closed)
            exit_events.append(f"tp{int(target_pct)}:{round(sum(weight for _, weight, _ in closed), 6)}")
            target_idx += 1
            exit_idx = idx
            exit_reason = f"target_{int(target_pct)}"
            if float(close_fraction) >= 1.0 or open_weight(active) <= 1e-9:
                active.clear()
                break
        if not active and realized:
            break

        time_covers = tuple(plan.get("time_covers") or ())
        while time_cover_idx < len(time_covers) and active:
            cover_h, close_fraction = time_covers[time_cover_idx]
            if idx - entry_idx < int(cover_h):
                break
            close_price = series.close[idx]
            if not close_price:
                break
            closed = close_active_fraction(
                series,
                active,
                exit_idx=idx,
                fill_price=float(close_price),
                fraction=float(close_fraction),
            )
            realized.extend(closed)
            exit_events.append(f"cover{int(float(close_fraction) * 100)}_{int(cover_h)}h:{round(sum(weight for _, weight, _ in closed), 6)}")
            time_cover_idx += 1
            exit_idx = idx
            exit_reason = f"time_cover_{int(cover_h)}h"
            if float(close_fraction) >= 1.0 or open_weight(active) <= 1e-9:
                active.clear()
                break
        if not active and realized:
            break

    if active:
        close_price = series.close[exit_limit_idx]
        if not close_price:
            return None
        realized.extend(
            close_active_fraction(
                series,
                active,
                exit_idx=exit_limit_idx,
                fill_price=float(close_price),
                fraction=1.0,
            )
        )
        exit_idx = exit_limit_idx
        exit_events.append("time_stop:all")

    if not realized:
        return None

    net = weighted_mean(realized)
    funding_during = weighted_mean_component(realized, 2)
    entry_avg = weighted_avg_entry([{"idx": leg_idx, "price": leg_price, "weight": 1.0} for leg_idx, leg_price in pending_legs if leg_idx <= exit_idx])
    completed_horizon = 1 if exit_idx >= entry_idx + max_hold_h else 0
    repump_stats = repump_diagnostics(series, entry_idx, exit_idx, entry_avg)
    decay_stats = decay_diagnostics(series, entry_idx, exit_idx, entry_avg)
    funding_stats = funding_diagnostics(series, entry_idx, exit_idx)
    row = event_to_row(event)
    row.update(event_behavior_features(series, event))
    row.update(
        {
            "entry_strategy": entry_strategy,
            "entry_strategy_family": strategy_family(entry_strategy),
            "entry_confirmation": confirmation_label(entry_strategy),
            "exit_strategy": str(plan["name"]),
            "exit_family": exit_family(str(plan["name"])),
            "horizon_h": max_hold_h,
            "entry_ts": series.ts[entry_idx],
            "exit_ts": series.ts[exit_idx],
            "time_in_trade_h": exit_idx - entry_idx,
            "exit_reason": exit_reason,
            "exit_events": "|".join(exit_events),
            "legs_planned": len(legs),
            "legs_activated": sum(1 for leg_idx, _ in pending_legs if leg_idx <= exit_idx),
            "net_exit_pct": net,
            "funding_during_pct": funding_during,
            "mae_pct": max_mae,
            "mfe_pct": max_mfe,
            "completed_horizon": completed_horizon,
            "available_hold_h": exit_idx - entry_idx,
            "win": 1 if net > 0 else 0,
            "catastrophic_100": 1 if max_mae is not None and max_mae >= 100.0 else 0,
            "catastrophic_300": 1 if max_mae is not None and max_mae >= 300.0 else 0,
            "liquidation_proxy_3x": 1 if max_mae is not None and max_mae >= 33.0 else 0,
            "liquidation_proxy_1x": 1 if max_mae is not None and max_mae >= 100.0 else 0,
        }
    )
    row.update(repump_stats)
    row.update(decay_stats)
    row.update(funding_stats)
    return row


def weighted_avg_entry(active: list[dict[str, float | int]]) -> float | None:
    total_weight = open_weight(active)
    if total_weight <= 0:
        return None
    return sum(float(item["price"]) * float(item["weight"]) for item in active) / total_weight


def open_weight(active: list[dict[str, float | int]]) -> float:
    return sum(float(item["weight"]) for item in active)


def close_active_fraction(
    series: Series,
    active: list[dict[str, float | int]],
    *,
    exit_idx: int,
    fill_price: float,
    fraction: float,
) -> list[tuple[float, float, float]]:
    fraction = max(0.0, min(1.0, fraction))
    closed_net: list[tuple[float, float, float]] = []
    for item in active:
        weight = float(item["weight"])
        if weight <= 0:
            continue
        close_weight = weight * fraction
        if close_weight <= 0:
            continue
        leg_idx = int(item["idx"])
        entry_price = float(item["price"])
        price_pnl_pct = (1.0 - fill_price / entry_price) * 100.0
        funding_pct = funding_sum_pct(series.funding, series.ts[leg_idx], series.ts[exit_idx]) or 0.0
        net_pct = price_pnl_pct + funding_pct - FEE_ROUNDTRIP_PCT
        closed_net.append((net_pct, close_weight, funding_pct))
        item["weight"] = weight - close_weight
    return closed_net


def weighted_mean(values_with_weights: list[tuple[float, float, float]]) -> float:
    total_weight = sum(weight for _, weight, _ in values_with_weights)
    if total_weight <= 0:
        return 0.0
    return sum(value * weight for value, weight, _ in values_with_weights) / total_weight


def weighted_mean_component(values_with_weights: list[tuple[float, float, float]], index: int) -> float:
    total_weight = sum(weight for _, weight, _ in values_with_weights)
    if total_weight <= 0:
        return 0.0
    return sum(item[index] * item[1] for item in values_with_weights) / total_weight


def repump_diagnostics(series: Series, entry_idx: int, exit_idx: int, entry_price: float | None) -> dict[str, Any]:
    if not entry_price or exit_idx <= entry_idx:
        return {
            "max_repump_pct": None,
            "repump_30_count": 0,
            "repump_50_count": 0,
            "repump_100_count": 0,
        }
    highs = [value for value in series.high[entry_idx + 1 : exit_idx + 1] if value is not None]
    max_repump = ((max(highs) / entry_price - 1.0) * 100.0) if highs else None
    return {
        "max_repump_pct": max_repump,
        "repump_30_count": count_repump_crossings(series, entry_idx, exit_idx, entry_price, 30.0),
        "repump_50_count": count_repump_crossings(series, entry_idx, exit_idx, entry_price, 50.0),
        "repump_100_count": count_repump_crossings(series, entry_idx, exit_idx, entry_price, 100.0),
    }


def count_repump_crossings(series: Series, entry_idx: int, exit_idx: int, entry_price: float, threshold_pct: float) -> int:
    level = entry_price * (1.0 + threshold_pct / 100.0)
    in_zone = False
    count = 0
    for idx in range(entry_idx + 1, exit_idx + 1):
        high = series.high[idx]
        close = series.close[idx]
        if high is not None and high >= level and not in_zone:
            count += 1
            in_zone = True
        if close is not None and close < level:
            in_zone = False
    return count


def decay_diagnostics(series: Series, entry_idx: int, exit_idx: int, entry_price: float | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for threshold in (50.0, 70.0, 90.0):
        out[f"time_to_decay_{int(threshold)}_h"] = None
    if not entry_price or exit_idx <= entry_idx:
        return out
    for threshold in (50.0, 70.0, 90.0):
        level = entry_price * (1.0 - threshold / 100.0)
        for idx in range(entry_idx + 1, exit_idx + 1):
            low = series.low[idx]
            if low is not None and low <= level:
                out[f"time_to_decay_{int(threshold)}_h"] = idx - entry_idx
                break
    return out


def funding_diagnostics(series: Series, entry_idx: int, exit_idx: int) -> dict[str, Any]:
    if exit_idx <= entry_idx:
        return {
            "funding_full_period_pct": None,
            "funding_positive_share_pct": None,
            "funding_points": 0,
        }
    start_ms = series.ts[entry_idx]
    end_ms = series.ts[exit_idx]
    points = [(ts_ms, rate) for ts_ms, rate in series.funding if start_ms <= ts_ms <= end_ms]
    positive = sum(1 for _, rate in points if rate > 0.0)
    return {
        "funding_full_period_pct": funding_sum_pct(series.funding, start_ms, end_ms),
        "funding_positive_share_pct": pct(positive, len(points)) if points else None,
        "funding_points": len(points),
    }


def event_to_row(event: PumpEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "symbol": event.symbol,
        "trigger_ts": event.trigger_ts,
        "config_window_h": event.config_window_h,
        "config_threshold_pct": event.config_threshold_pct,
        "pump_pct": event.pump_pct,
        "trigger_close": event.trigger_close,
        "age_days": event.age_days,
        "funding_prev_24h_pct": event.funding_prev_24h_pct,
        "funding_prev_72h_pct": event.funding_prev_72h_pct,
        "oi_change_4h_pct": event.oi_change_4h_pct,
        "oi_change_24h_pct": event.oi_change_24h_pct,
        "long_ratio": event.long_ratio,
    }


def event_behavior_features(series: Series, event: PumpEvent) -> dict[str, Any]:
    ts_ms = event.trigger_ts
    next_24 = ts_ms + 24 * 3_600_000
    next_72 = ts_ms + 72 * 3_600_000
    funding_next_24 = funding_sum_pct(series.funding, ts_ms, next_24)
    funding_next_72 = funding_sum_pct(series.funding, ts_ms, next_72)
    funding_rates_24 = funding_rates_pct(series.funding, ts_ms, next_24)
    funding_rates_72 = funding_rates_pct(series.funding, ts_ms, next_72)
    min_funding_24 = min(funding_rates_24) if funding_rates_24 else None
    min_funding_72 = min(funding_rates_72) if funding_rates_72 else None
    heavy_neg_points_72 = sum(1 for value in funding_rates_72 if value <= FUNDING_HEAVY_NEG_PCT)
    extreme_neg_points_72 = sum(1 for value in funding_rates_72 if value <= FUNDING_EXTREME_NEG_PCT)
    oi_next_24 = point_change_from_idx_pct(series.oi, series.ts, event.trigger_idx, 24)
    oi_next_72 = point_change_from_idx_pct(series.oi, series.ts, event.trigger_idx, 72)
    oi_max_next_24 = point_max_change_from_idx_pct(series.oi, series.ts, event.trigger_idx, 24)
    oi_max_next_72 = point_max_change_from_idx_pct(series.oi, series.ts, event.trigger_idx, 72)
    oi_min_next_72 = point_min_change_from_idx_pct(series.oi, series.ts, event.trigger_idx, 72)
    funding_regime = classify_funding_regime(
        event.funding_prev_24h_pct,
        funding_next_24,
        funding_next_72,
        min_funding_72,
        heavy_neg_points_72,
        extreme_neg_points_72,
    )
    oi_regime = classify_oi_regime(event.oi_change_24h_pct, oi_next_24, oi_next_72, oi_max_next_72, oi_min_next_72)
    return {
        "pump_regime": classify_pump_regime(event.pump_pct),
        "age_regime": classify_age_regime(event.age_days),
        "funding_regime": funding_regime,
        "oi_regime": oi_regime,
        "long_ratio_regime": classify_long_ratio_regime(event.long_ratio),
        "behavior_regime": f"{funding_regime}|{oi_regime}",
        "funding_next_24h_pct": funding_next_24,
        "funding_next_72h_pct": funding_next_72,
        "funding_min_rate_next_24h_pct": min_funding_24,
        "funding_min_rate_next_72h_pct": min_funding_72,
        "funding_heavy_neg_points_next_72h": heavy_neg_points_72,
        "funding_extreme_neg_points_next_72h": extreme_neg_points_72,
        "oi_change_next_24h_pct": oi_next_24,
        "oi_change_next_72h_pct": oi_next_72,
        "oi_max_change_next_24h_pct": oi_max_next_24,
        "oi_max_change_next_72h_pct": oi_max_next_72,
        "oi_min_change_next_72h_pct": oi_min_next_72,
    }


def classify_pump_regime(pump_pct: float | None) -> str:
    value = pump_pct if pump_pct is not None else 0.0
    if value >= 400.0:
        return "mega_400_plus"
    if value >= 250.0:
        return "very_large_250_400"
    if value >= 150.0:
        return "large_150_250"
    if value >= 80.0:
        return "medium_80_150"
    return "small_50_80"


def classify_age_regime(age_days: float | None) -> str:
    if age_days is None:
        return "unknown_age"
    if age_days < 7.0:
        return "new_lt_7d"
    if age_days < 30.0:
        return "young_7_30d"
    if age_days < 180.0:
        return "mid_30_180d"
    return "old_180d_plus"


def classify_long_ratio_regime(long_ratio: float | None) -> str:
    if long_ratio is None:
        return "long_ratio_unknown"
    if long_ratio >= 0.70:
        return "crowded_long_70_plus"
    if long_ratio >= 0.60:
        return "long_60_70"
    if long_ratio <= 0.45:
        return "long_low_45_minus"
    return "balanced_45_60"


def classify_funding_regime(
    prev_24h_pct: float | None,
    next_24h_pct: float | None,
    next_72h_pct: float | None,
    min_next_72h_pct: float | None,
    heavy_neg_points_72h: int,
    extreme_neg_points_72h: int,
) -> str:
    min_rate = min_next_72h_pct if min_next_72h_pct is not None else 0.0
    next_24 = next_24h_pct if next_24h_pct is not None else 0.0
    next_72 = next_72h_pct if next_72h_pct is not None else 0.0
    prev_24 = prev_24h_pct if prev_24h_pct is not None else 0.0
    if extreme_neg_points_72h > 0 or min_rate <= FUNDING_EXTREME_NEG_PCT or next_24 <= -5.0:
        return "extreme_negative_funding"
    if heavy_neg_points_72h >= 3 or min_rate <= FUNDING_HEAVY_NEG_PCT or next_72 <= -3.0:
        return "persistent_negative_funding"
    if prev_24 <= -1.0:
        return "pre_toxic_negative_funding"
    if next_24 >= 0.2 and next_72 >= 0.0:
        return "short_receives_or_neutral"
    return "mild_or_mixed_funding"


def classify_oi_regime(
    prev_24h_pct: float | None,
    next_24h_pct: float | None,
    next_72h_pct: float | None,
    max_next_72h_pct: float | None,
    min_next_72h_pct: float | None,
) -> str:
    prev_24 = prev_24h_pct if prev_24h_pct is not None else 0.0
    next_24 = next_24h_pct if next_24h_pct is not None else 0.0
    next_72 = next_72h_pct if next_72h_pct is not None else 0.0
    max_next = max_next_72h_pct if max_next_72h_pct is not None else 0.0
    min_next = min_next_72h_pct if min_next_72h_pct is not None else 0.0
    if prev_24 >= 200.0 or max_next >= 300.0:
        return "oi_blowoff"
    if prev_24 >= 50.0 or max_next >= 100.0 or next_24 >= 50.0:
        return "oi_expansion"
    if next_72 <= -20.0 or min_next <= -25.0:
        return "oi_cooling_after_pump"
    return "oi_neutral"


def episode_to_row(episode: PumpEpisode) -> dict[str, Any]:
    return {
        "episode_id": episode.episode_id,
        "symbol": episode.symbol,
        "start_ts": episode.start_ts,
        "end_ts": episode.end_ts,
        "duration_h": episode.duration_h,
        "event_count": episode.event_count,
        "first_event_id": episode.first_event_id,
        "first_config_window_h": episode.first_config_window_h,
        "first_config_threshold_pct": episode.first_config_threshold_pct,
        "start_close": episode.start_close,
        "episode_high": episode.episode_high,
        "episode_low_after_high": episode.episode_low_after_high,
        "high_from_start_pct": episode.high_from_start_pct,
        "max_drawdown_from_high_pct": episode.max_drawdown_from_high_pct,
        "time_to_high_h": episode.time_to_high_h,
        "final_return_from_start_pct": episode.final_return_from_start_pct,
        "max_trigger_pump_pct": episode.max_trigger_pump_pct,
        "age_days": episode.age_days,
        "funding_prev_24h_pct": episode.funding_prev_24h_pct,
        "oi_change_24h_pct": episode.oi_change_24h_pct,
        "long_ratio": episode.long_ratio,
    }


def build_strategy_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            row.get("strategy"),
            row.get("horizon_h"),
            row.get("config_window_h"),
            row.get("config_threshold_pct"),
        )
        groups.setdefault(key, []).append(row)
    out = []
    for key, items in sorted(groups.items(), key=lambda item: (str(item[0][0]), item[0][1], item[0][2])):
        out.append(aggregate_rows(items, strategy=key[0], horizon_h=key[1], config_window_h=key[2], config_threshold_pct=key[3]))
    return out


def build_entry_rule_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row.get("strategy"), row.get("horizon_h"))
        groups.setdefault(key, []).append(row)
    out = []
    for (strategy, horizon_h), items in sorted(groups.items(), key=lambda item: (item[0][1], str(item[0][0]))):
        out.append(
            aggregate_rows(
                items,
                strategy=strategy,
                strategy_family=strategy_family(str(strategy)),
                confirmation=confirmation_label(str(strategy)),
                horizon_h=horizon_h,
            )
        )
    return out


def build_exit_rule_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row.get("entry_strategy"), row.get("exit_strategy"))
        groups.setdefault(key, []).append(row)
    out = []
    for (entry_strategy, exit_strategy), items in sorted(groups.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))):
        out.append(
            aggregate_rows(
                items,
                entry_strategy=entry_strategy,
                entry_strategy_family=strategy_family(str(entry_strategy)),
                entry_confirmation=confirmation_label(str(entry_strategy)),
                exit_strategy=exit_strategy,
                exit_family=exit_family(str(exit_strategy)),
            )
        )
    return out


def build_long_hold_rule_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row.get("entry_strategy"), row.get("exit_strategy"))
        groups.setdefault(key, []).append(row)
    out = []
    for (entry_strategy, exit_strategy), items in sorted(groups.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))):
        row = aggregate_rows(
            items,
            entry_strategy=entry_strategy,
            entry_strategy_family=strategy_family(str(entry_strategy)),
            entry_confirmation=confirmation_label(str(entry_strategy)),
            exit_strategy=exit_strategy,
            exit_family=exit_family(str(exit_strategy)),
        )
        row.update(
            {
                "completed_horizon_pct": pct(sum(1 for item in items if item.get("completed_horizon")), len(items)),
                "median_available_hold_h": median(values(items, "available_hold_h")),
                "p90_max_repump_pct": percentile(values(items, "max_repump_pct"), 90),
                "median_repump_30_count": median(values(items, "repump_30_count")),
                "p75_repump_50_count": percentile(values(items, "repump_50_count"), 75),
                "p90_repump_100_count": percentile(values(items, "repump_100_count"), 90),
                "median_funding_full_period_pct": median(values(items, "funding_full_period_pct")),
                "median_funding_positive_share_pct": median(values(items, "funding_positive_share_pct")),
                "median_time_to_decay_50_h": median(values(items, "time_to_decay_50_h")),
                "median_time_to_decay_70_h": median(values(items, "time_to_decay_70_h")),
                "median_time_to_decay_90_h": median(values(items, "time_to_decay_90_h")),
            }
        )
        out.append(row)
    return out


def build_best_long_hold_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        n = to_float(row.get("n")) or 0.0
        completed = to_float(row.get("completed_horizon_pct")) or 0.0
        if n < 50 or completed < 40.0:
            continue
        avg_net = to_float(row.get("avg_net_pct")) or 0.0
        median_net = to_float(row.get("median_net_pct")) or 0.0
        win_pct = to_float(row.get("win_pct")) or 0.0
        p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
        p90_repump = to_float(row.get("p90_max_repump_pct")) or 0.0
        cat300 = to_float(row.get("catastrophic_300_pct")) or 0.0
        funding = to_float(row.get("median_funding_full_period_pct")) or 0.0
        score = (
            avg_net * 0.30
            + median_net * 0.35
            + win_pct * 0.12
            + funding * 0.15
            - p90_mae * 0.08
            - p90_repump * 0.04
            - cat300 * 5.0
        )
        item = dict(row)
        item["long_hold_score"] = round(score, 6)
        item["long_hold_note"] = long_hold_note(item)
        candidates.append(item)
    candidates.sort(key=lambda row: to_float(row.get("long_hold_score")) or -9999.0, reverse=True)
    for idx, row in enumerate(candidates, start=1):
        row["long_hold_rank"] = idx
    return candidates[:100]


def long_hold_note(row: dict[str, Any]) -> str:
    median_net = to_float(row.get("median_net_pct")) or 0.0
    p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
    p90_repump = to_float(row.get("p90_max_repump_pct")) or 0.0
    funding = to_float(row.get("median_funding_full_period_pct")) or 0.0
    return f"median={median_net:.1f}, p90_mae={p90_mae:.1f}, p90_repump={p90_repump:.1f}, funding={funding:.1f}"


def strategy_family(strategy: str) -> str:
    if strategy == "immediate":
        return "immediate"
    if strategy.startswith("pullback_"):
        return "plain_pullback"
    if strategy.startswith("pb") and "ladder" in strategy:
        return "confirmed_pullback_ladder"
    if strategy.startswith("pb"):
        return "confirmed_pullback"
    if strategy.startswith("ladder"):
        return "trigger_ladder"
    return "other"


def exit_family(strategy: str) -> str:
    if strategy.startswith("long_full"):
        return "long_hold"
    if strategy.startswith("cover") or "runner" in strategy:
        return "runner"
    if strategy.startswith("time_full"):
        return "time_stop"
    if "_full_" in strategy:
        return "full_take_profit"
    if "halves" in strategy or "half" in strategy:
        return "partial"
    if "thirds" in strategy:
        return "partial"
    return "other"


def confirmation_label(strategy: str) -> str:
    if "oi0_lr_mid" in strategy:
        return "oi_cooling__long_ratio_45_65"
    if "oi50_lr_mid" in strategy:
        return "oi_not_exploding__long_ratio_45_65"
    return "none"


def build_filter_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filters = [
        ("all", lambda row: True),
        ("funding_not_toxic", lambda row: row.get("filter_funding_not_toxic") is True),
        ("oi_cooling_24h", lambda row: row.get("filter_oi_cooling_24h") is True),
        ("oi_not_exploding_24h", lambda row: row.get("filter_oi_not_exploding_24h") is True),
        ("long_ratio_low", lambda row: row.get("filter_long_ratio_low") is True),
        ("long_ratio_high", lambda row: row.get("filter_long_ratio_high") is True),
        ("age_lt_30d", lambda row: row.get("filter_age_lt_30d") is True),
        (
            "funding_ok__oi_not_exploding",
            lambda row: row.get("filter_funding_not_toxic") is True and row.get("filter_oi_not_exploding_24h") is True,
        ),
        (
            "funding_ok__oi_cooling__long_low",
            lambda row: row.get("filter_funding_not_toxic") is True
            and row.get("filter_oi_cooling_24h") is True
            and row.get("filter_long_ratio_low") is True,
        ),
    ]
    out = []
    for strategy in sorted({str(row.get("strategy")) for row in rows}):
        for horizon in HORIZONS_HOURS:
            base = [row for row in rows if row.get("strategy") == strategy and row.get("horizon_h") == horizon]
            for name, predicate in filters:
                items = [row for row in base if predicate(row)]
                if items:
                    out.append(aggregate_rows(items, strategy=strategy, horizon_h=horizon, filter_name=name))
    return out


def build_symbol_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("strategy") == "immediate" and row.get("horizon_h") == 168:
            groups.setdefault(str(row.get("symbol")), []).append(row)
    out = []
    for symbol, items in sorted(groups.items(), key=lambda item: len(item[1]), reverse=True):
        out.append(aggregate_rows(items, symbol=symbol))
    return out


def build_regime_summary(rows: list[dict[str, Any]], regime_keys: tuple[str, ...]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(regime_key) or "unknown" for regime_key in regime_keys) + (
            row.get("entry_strategy"),
            row.get("exit_strategy"),
        )
        groups.setdefault(key, []).append(row)
    out: list[dict[str, Any]] = []
    for key, items in sorted(groups.items(), key=lambda item: tuple(str(part) for part in item[0])):
        prefix = {regime_key: key[index] for index, regime_key in enumerate(regime_keys)}
        entry_strategy = key[len(regime_keys)]
        exit_strategy = key[len(regime_keys) + 1]
        row = aggregate_rows(
            items,
            **prefix,
            entry_strategy=entry_strategy,
            entry_strategy_family=strategy_family(str(entry_strategy)),
            entry_confirmation=confirmation_label(str(entry_strategy)),
            exit_strategy=exit_strategy,
            exit_family=exit_family(str(exit_strategy)),
        )
        row["regime_rule_score"] = regime_rule_score(row)
        row["regime_note"] = regime_note(row)
        out.append(row)
    out.sort(
        key=lambda row: (
            tuple(str(row.get(regime_key) or "") for regime_key in regime_keys),
            -(to_float(row.get("regime_rule_score")) or -9999.0),
        )
    )
    return out


def build_behavior_regime_recommendations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        n = to_float(row.get("n")) or 0.0
        if n < REGIME_MIN_N:
            continue
        funding_regime = str(row.get("funding_regime") or "unknown")
        oi_regime = str(row.get("oi_regime") or "unknown")
        groups.setdefault((funding_regime, oi_regime), []).append(row)

    out: list[dict[str, Any]] = []
    for (funding_regime, oi_regime), items in sorted(groups.items()):
        ranked = sorted(items, key=lambda row: to_float(row.get("regime_rule_score")) or -9999.0, reverse=True)
        for rank, row in enumerate(ranked[:5], start=1):
            item = {
                "funding_regime": funding_regime,
                "oi_regime": oi_regime,
                "rank": rank,
                "entry_strategy": row.get("entry_strategy"),
                "exit_strategy": row.get("exit_strategy"),
                "entry_strategy_family": row.get("entry_strategy_family"),
                "exit_family": row.get("exit_family"),
                "n": row.get("n"),
                "regime_rule_score": row.get("regime_rule_score"),
                "win_pct": row.get("win_pct"),
                "avg_net_pct": row.get("avg_net_pct"),
                "median_net_pct": row.get("median_net_pct"),
                "p90_mae_pct": row.get("p90_mae_pct"),
                "p95_mae_pct": row.get("p95_mae_pct"),
                "catastrophic_300_pct": row.get("catastrophic_300_pct"),
                "liq_proxy_3x_pct": row.get("liq_proxy_3x_pct"),
                "regime_note": row.get("regime_note"),
            }
            out.append(item)
    return out


def build_strategy_recommendations(
    candidate_profiles: list[dict[str, Any]],
    anti_overfit_rows: list[dict[str, Any]],
    regime_recommendations: list[dict[str, Any]],
) -> dict[str, Any]:
    profiles: dict[str, list[dict[str, Any]]] = {}
    for row in candidate_profiles:
        profile = str(row.get("profile") or "unknown")
        profiles.setdefault(profile, []).append(compact_rule_row(row))
    anti_index = rule_index(anti_overfit_rows)
    regime_items = []
    for row in regime_recommendations:
        key = (str(row.get("entry_strategy") or ""), str(row.get("exit_strategy") or ""))
        anti = anti_index.get(key, {})
        item = dict(row)
        item["anti_overfit_status"] = anti.get("anti_overfit_status")
        item["anti_overfit_score"] = anti.get("anti_overfit_score")
        regime_items.append(item)
    return {
        "schema": "bybit_pump_short_strategy_recommendations_v1",
        "overall_profiles": profiles,
        "by_behavior_regime": regime_items,
        "notes": [
            "Use overall_profiles as the first short list.",
            "Use by_behavior_regime to decide whether a signal belongs to a conservative, balanced, aggressive, or avoid bucket.",
            "Negative funding regimes are costly for shorts; prefer rules that keep p90/p95 MAE low and time-in-trade shorter there.",
            "OI blowoff regimes are continuation-risk regimes; require stronger pullback/OI cooling confirmation before considering a paper/live entry.",
        ],
    }


def compact_rule_row(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "profile",
        "profile_rank",
        "entry_strategy",
        "exit_strategy",
        "n",
        "profile_score",
        "win_pct",
        "avg_net_pct",
        "median_net_pct",
        "p90_mae_pct",
        "p95_mae_pct",
        "catastrophic_300_pct",
        "liq_proxy_3x_pct",
        "anti_overfit_status",
        "time_test_avg_net_pct",
        "symbol_holdout_avg_net_pct",
        "selection_note",
    )
    return {key: row.get(key) for key in keys if key in row}


def regime_rule_score(row: dict[str, Any]) -> float:
    n = to_float(row.get("n")) or 0.0
    avg_net = to_float(row.get("avg_net_pct")) or 0.0
    median_net = to_float(row.get("median_net_pct")) or 0.0
    win_pct = to_float(row.get("win_pct")) or 0.0
    p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
    p95_mae = to_float(row.get("p95_mae_pct")) or 0.0
    cat300 = to_float(row.get("catastrophic_300_pct")) or 0.0
    liq3x = to_float(row.get("liq_proxy_3x_pct")) or 0.0
    sample_bonus = min(10.0, math.log10(max(1.0, n)) * 3.0)
    return round(
        avg_net * 0.30
        + median_net * 0.30
        + win_pct * 0.18
        + sample_bonus
        - p90_mae * 0.12
        - p95_mae * 0.04
        - cat300 * 4.0
        - liq3x * 0.08,
        6,
    )


def regime_note(row: dict[str, Any]) -> str:
    funding_regime = str(row.get("funding_regime") or "")
    oi_regime = str(row.get("oi_regime") or "")
    p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
    avg_net = to_float(row.get("avg_net_pct")) or 0.0
    if "extreme_negative" in funding_regime or "persistent_negative" in funding_regime:
        return f"funding-cost regime; require faster exits and low MAE; avg={avg_net:.1f}, p90_mae={p90_mae:.1f}"
    if oi_regime == "oi_blowoff":
        return f"continuation-risk OI blowoff; prefer confirmed pullback/ladder only; avg={avg_net:.1f}, p90_mae={p90_mae:.1f}"
    if oi_regime == "oi_cooling_after_pump":
        return f"OI cooling regime; better fit for confirmed shorts; avg={avg_net:.1f}, p90_mae={p90_mae:.1f}"
    return f"mixed regime; validate with anti-overfit and symbol concentration; avg={avg_net:.1f}, p90_mae={p90_mae:.1f}"


def aggregate_rows(items: list[dict[str, Any]], **prefix: Any) -> dict[str, Any]:
    net = values(items, "net_exit_pct")
    mae = values(items, "mae_pct")
    mfe = values(items, "mfe_pct")
    funding = values(items, "funding_during_pct")
    row = dict(prefix)
    row.update(
        {
            "n": len(items),
            "win_pct": pct(sum(1 for item in items if item.get("win")), len(items)),
            "avg_net_pct": mean(net),
            "median_net_pct": median(net),
            "p25_net_pct": percentile(net, 25),
            "p75_net_pct": percentile(net, 75),
            "median_mae_pct": median(mae),
            "p90_mae_pct": percentile(mae, 90),
            "p95_mae_pct": percentile(mae, 95),
            "p99_mae_pct": percentile(mae, 99),
            "max_effective_leverage_p90": max_effective_leverage(percentile(mae, 90)),
            "max_effective_leverage_p95": max_effective_leverage(percentile(mae, 95)),
            "median_mfe_pct": median(mfe),
            "p75_mfe_pct": percentile(mfe, 75),
            "median_funding_pct": median(funding),
            "catastrophic_100_pct": pct(sum(1 for item in items if item.get("catastrophic_100")), len(items)),
            "catastrophic_300_pct": pct(sum(1 for item in items if item.get("catastrophic_300")), len(items)),
            "liq_proxy_3x_pct": pct(sum(1 for item in items if item.get("liquidation_proxy_3x")), len(items)),
            "liq_proxy_1x_pct": pct(sum(1 for item in items if item.get("liquidation_proxy_1x")), len(items)),
        }
    )
    return row


def build_best_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        n = to_float(row.get("n")) or 0.0
        if n < 100:
            continue
        avg_net = to_float(row.get("avg_net_pct")) or 0.0
        median_net = to_float(row.get("median_net_pct")) or 0.0
        win_pct = to_float(row.get("win_pct")) or 0.0
        p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
        cat300 = to_float(row.get("catastrophic_300_pct")) or 0.0
        liq3x = to_float(row.get("liq_proxy_3x_pct")) or 0.0
        score = avg_net * 0.45 + median_net * 0.35 + win_pct * 0.15 - p90_mae * 0.08 - cat300 * 3.0 - liq3x * 0.08
        item = dict(row)
        item["rule_score"] = round(score, 6)
        item["risk_note"] = risk_note(p90_mae=p90_mae, cat300=cat300, liq3x=liq3x)
        candidates.append(item)
    candidates.sort(key=lambda row: to_float(row.get("rule_score")) or -9999.0, reverse=True)
    for idx, row in enumerate(candidates, start=1):
        row["rank"] = idx
    return candidates[:100]


def build_candidate_rule_profiles(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for profile in RULE_PROFILES:
        profile_name = str(profile["profile"])
        candidates: list[dict[str, Any]] = []
        for row in rows:
            checks = profile_checks(row, profile)
            if not all(ok for ok, _ in checks.values()):
                continue
            item = dict(row)
            score = profile_score(row, profile_name)
            item.update(
                {
                    "profile": profile_name,
                    "profile_rank": 0,
                    "profile_score": round(score, 6),
                    "profile_thresholds": json.dumps(profile, ensure_ascii=True, sort_keys=True),
                    "selection_note": selection_note(profile_name, row),
                }
            )
            candidates.append(item)
        candidates.sort(key=lambda item: to_float(item.get("profile_score")) or -9999.0, reverse=True)
        for rank, item in enumerate(candidates[:10], start=1):
            item["profile_rank"] = rank
            out.append(item)
    return out


def profile_checks(row: dict[str, Any], profile: dict[str, Any]) -> dict[str, tuple[bool, float | None]]:
    n = to_float(row.get("n"))
    win_pct = to_float(row.get("win_pct"))
    avg_net = to_float(row.get("avg_net_pct"))
    median_net = to_float(row.get("median_net_pct"))
    p90_mae = to_float(row.get("p90_mae_pct"))
    p95_mae = to_float(row.get("p95_mae_pct"))
    cat300 = to_float(row.get("catastrophic_300_pct"))
    liq3x = to_float(row.get("liq_proxy_3x_pct"))
    return {
        "min_n": (n is not None and n >= float(profile["min_n"]), n),
        "min_win_pct": (win_pct is not None and win_pct >= float(profile["min_win_pct"]), win_pct),
        "min_avg_net_pct": (avg_net is not None and avg_net >= float(profile["min_avg_net_pct"]), avg_net),
        "min_median_net_pct": (median_net is not None and median_net >= float(profile["min_median_net_pct"]), median_net),
        "max_p90_mae_pct": (p90_mae is not None and p90_mae <= float(profile["max_p90_mae_pct"]), p90_mae),
        "max_p95_mae_pct": (p95_mae is not None and p95_mae <= float(profile["max_p95_mae_pct"]), p95_mae),
        "max_cat300_pct": (cat300 is not None and cat300 <= float(profile["max_cat300_pct"]), cat300),
        "max_liq3x_pct": (liq3x is not None and liq3x <= float(profile["max_liq3x_pct"]), liq3x),
    }


def profile_score(row: dict[str, Any], profile_name: str) -> float:
    avg_net = to_float(row.get("avg_net_pct")) or 0.0
    median_net = to_float(row.get("median_net_pct")) or 0.0
    win_pct = to_float(row.get("win_pct")) or 0.0
    p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
    p95_mae = to_float(row.get("p95_mae_pct")) or 0.0
    cat300 = to_float(row.get("catastrophic_300_pct")) or 0.0
    liq3x = to_float(row.get("liq_proxy_3x_pct")) or 0.0
    if profile_name == "conservative":
        return median_net * 0.35 + avg_net * 0.25 + win_pct * 0.25 - p90_mae * 0.16 - liq3x * 0.08 - cat300 * 5.0
    if profile_name == "balanced":
        return median_net * 0.35 + avg_net * 0.35 + win_pct * 0.20 - p90_mae * 0.10 - liq3x * 0.06 - cat300 * 4.0
    return median_net * 0.30 + avg_net * 0.45 + win_pct * 0.15 - p90_mae * 0.07 - p95_mae * 0.02 - cat300 * 3.0


def selection_note(profile_name: str, row: dict[str, Any]) -> str:
    entry = str(row.get("entry_strategy") or "")
    exit_strategy = str(row.get("exit_strategy") or "")
    p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
    avg_net = to_float(row.get("avg_net_pct")) or 0.0
    median_net = to_float(row.get("median_net_pct")) or 0.0
    if profile_name == "conservative":
        return f"shorter-tail candidate; p90_mae={p90_mae:.1f}, avg={avg_net:.1f}, median={median_net:.1f}"
    if profile_name == "balanced":
        return f"balanced return/tail candidate; entry={entry}, exit={exit_strategy}"
    return f"higher-return candidate; requires larger buffer; p90_mae={p90_mae:.1f}"


def build_time_split_robustness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = time_split_cutoff(rows)
    if cutoff is None:
        return []
    groups = group_by_rule(rows)
    out: list[dict[str, Any]] = []
    for (entry_strategy, exit_strategy), items in sorted(groups.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))):
        train = [row for row in items if (to_float(row.get("trigger_ts")) or 0.0) <= cutoff]
        test = [row for row in items if (to_float(row.get("trigger_ts")) or 0.0) > cutoff]
        if not train or not test:
            continue
        row = {
            "split": "time",
            "entry_strategy": entry_strategy,
            "exit_strategy": exit_strategy,
            "train_cutoff_ts": int(cutoff),
        }
        row.update(prefixed_metrics(train, "train"))
        row.update(prefixed_metrics(test, "test"))
        row.update(robustness_deltas(row))
        row["robustness_status"] = robustness_status(row)
        row["robustness_score"] = robustness_score(row)
        out.append(row)
    out.sort(key=lambda row: to_float(row.get("robustness_score")) or -9999.0, reverse=True)
    return out


def build_symbol_holdout_robustness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = group_by_rule(rows)
    out: list[dict[str, Any]] = []
    for (entry_strategy, exit_strategy), items in sorted(groups.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))):
        train = [row for row in items if symbol_bucket(str(row.get("symbol") or "")) != 0]
        test = [row for row in items if symbol_bucket(str(row.get("symbol") or "")) == 0]
        if not train or not test:
            continue
        row = {
            "split": "symbol_holdout",
            "entry_strategy": entry_strategy,
            "exit_strategy": exit_strategy,
            "holdout_mod": SYMBOL_HOLDOUT_MOD,
            "holdout_bucket": 0,
        }
        row.update(prefixed_metrics(train, "train"))
        row.update(prefixed_metrics(test, "test"))
        row.update(robustness_deltas(row))
        row["robustness_status"] = robustness_status(row)
        row["robustness_score"] = robustness_score(row)
        out.append(row)
    out.sort(key=lambda row: to_float(row.get("robustness_score")) or -9999.0, reverse=True)
    return out


def build_symbol_concentration(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rule_groups = group_by_rule(rows)
    out: list[dict[str, Any]] = []
    for (entry_strategy, exit_strategy), items in sorted(rule_groups.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))):
        symbol_groups: dict[str, list[dict[str, Any]]] = {}
        for row in items:
            symbol_groups.setdefault(str(row.get("symbol") or ""), []).append(row)
        if not symbol_groups:
            continue
        symbol_metrics = []
        for symbol, symbol_rows in symbol_groups.items():
            net = values(symbol_rows, "net_exit_pct")
            symbol_metrics.append(
                {
                    "symbol": symbol,
                    "n": len(symbol_rows),
                    "avg_net_pct": mean(net),
                    "median_net_pct": median(net),
                    "total_net_units": sum(net),
                }
            )
        symbol_metrics.sort(key=lambda row: float(row.get("total_net_units") or 0.0), reverse=True)
        best = symbol_metrics[0]
        worst = min(symbol_metrics, key=lambda row: float(row.get("total_net_units") or 0.0))
        positive_units = sum(max(0.0, float(row.get("total_net_units") or 0.0)) for row in symbol_metrics)
        top_positive_share = (
            max(0.0, float(best.get("total_net_units") or 0.0)) / positive_units * 100.0
            if positive_units > 0
            else None
        )
        all_metrics = aggregate_rows(items)
        row = {
            "entry_strategy": entry_strategy,
            "exit_strategy": exit_strategy,
            "n": len(items),
            "symbol_count": len(symbol_groups),
            "avg_net_pct": all_metrics.get("avg_net_pct"),
            "median_net_pct": all_metrics.get("median_net_pct"),
            "p90_mae_pct": all_metrics.get("p90_mae_pct"),
            "catastrophic_300_pct": all_metrics.get("catastrophic_300_pct"),
            "best_symbol": best.get("symbol"),
            "best_symbol_n": best.get("n"),
            "best_symbol_avg_net_pct": best.get("avg_net_pct"),
            "worst_symbol": worst.get("symbol"),
            "worst_symbol_n": worst.get("n"),
            "worst_symbol_avg_net_pct": worst.get("avg_net_pct"),
            "top_positive_symbol_share_pct": round(top_positive_share, 6) if top_positive_share is not None else None,
            "concentration_status": concentration_status(top_positive_share, len(symbol_groups)),
        }
        out.append(row)
    out.sort(
        key=lambda row: (
            str(row.get("concentration_status")),
            -(to_float(row.get("avg_net_pct")) or -9999.0),
        )
    )
    return out


def build_anti_overfit_report(
    time_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    concentration_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    symbol_index = rule_index(symbol_rows)
    concentration_index = rule_index(concentration_rows)
    out: list[dict[str, Any]] = []
    for time_row in time_rows:
        key = (str(time_row.get("entry_strategy")), str(time_row.get("exit_strategy")))
        symbol_row = symbol_index.get(key, {})
        concentration_row = concentration_index.get(key, {})
        test_avg = to_float(time_row.get("test_avg_net_pct")) or 0.0
        holdout_avg = to_float(symbol_row.get("test_avg_net_pct")) or 0.0
        top_share = to_float(concentration_row.get("top_positive_symbol_share_pct"))
        status = anti_overfit_status(time_row, symbol_row, concentration_row)
        score = anti_overfit_score(time_row, symbol_row, concentration_row)
        out.append(
            {
                "entry_strategy": key[0],
                "exit_strategy": key[1],
                "anti_overfit_status": status,
                "anti_overfit_score": score,
                "time_status": time_row.get("robustness_status"),
                "symbol_status": symbol_row.get("robustness_status"),
                "concentration_status": concentration_row.get("concentration_status"),
                "time_train_n": time_row.get("train_n"),
                "time_test_n": time_row.get("test_n"),
                "time_train_avg_net_pct": time_row.get("train_avg_net_pct"),
                "time_test_avg_net_pct": time_row.get("test_avg_net_pct"),
                "time_test_median_net_pct": time_row.get("test_median_net_pct"),
                "time_test_p90_mae_pct": time_row.get("test_p90_mae_pct"),
                "symbol_holdout_avg_net_pct": symbol_row.get("test_avg_net_pct"),
                "symbol_holdout_median_net_pct": symbol_row.get("test_median_net_pct"),
                "symbol_holdout_p90_mae_pct": symbol_row.get("test_p90_mae_pct"),
                "top_positive_symbol_share_pct": top_share,
                "best_symbol": concentration_row.get("best_symbol"),
                "worst_symbol": concentration_row.get("worst_symbol"),
                "selection_note": anti_overfit_note(status, test_avg, holdout_avg, top_share),
            }
        )
    out.sort(key=lambda row: to_float(row.get("anti_overfit_score")) or -9999.0, reverse=True)
    return out


def annotate_candidate_profiles(
    profile_rows: list[dict[str, Any]],
    anti_overfit_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    anti_index = rule_index(anti_overfit_rows)
    out: list[dict[str, Any]] = []
    for row in profile_rows:
        key = (str(row.get("entry_strategy") or ""), str(row.get("exit_strategy") or ""))
        anti = anti_index.get(key, {})
        item = dict(row)
        item.update(
            {
                "anti_overfit_status": anti.get("anti_overfit_status"),
                "anti_overfit_score": anti.get("anti_overfit_score"),
                "time_status": anti.get("time_status"),
                "symbol_status": anti.get("symbol_status"),
                "concentration_status": anti.get("concentration_status"),
                "time_test_avg_net_pct": anti.get("time_test_avg_net_pct"),
                "symbol_holdout_avg_net_pct": anti.get("symbol_holdout_avg_net_pct"),
            }
        )
        out.append(item)
    return out


def time_split_cutoff(rows: list[dict[str, Any]]) -> float | None:
    stamps = [value for value in (to_float(row.get("trigger_ts")) for row in rows) if value is not None]
    if not stamps:
        return None
    start = min(stamps)
    end = max(stamps)
    return start + (end - start) * TIME_TRAIN_RATIO


def group_by_rule(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row.get("entry_strategy") or ""), str(row.get("exit_strategy") or ""))
        groups.setdefault(key, []).append(row)
    return groups


def prefixed_metrics(items: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    metrics = aggregate_rows(items)
    keys = (
        "n",
        "win_pct",
        "avg_net_pct",
        "median_net_pct",
        "p25_net_pct",
        "p75_net_pct",
        "p90_mae_pct",
        "p95_mae_pct",
        "p99_mae_pct",
        "median_mfe_pct",
        "catastrophic_100_pct",
        "catastrophic_300_pct",
        "liq_proxy_3x_pct",
        "liq_proxy_1x_pct",
    )
    return {f"{prefix}_{key}": metrics.get(key) for key in keys}


def robustness_deltas(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "avg_net_delta_test_minus_train": numeric_delta(row, "test_avg_net_pct", "train_avg_net_pct"),
        "median_net_delta_test_minus_train": numeric_delta(row, "test_median_net_pct", "train_median_net_pct"),
        "p90_mae_delta_test_minus_train": numeric_delta(row, "test_p90_mae_pct", "train_p90_mae_pct"),
        "win_delta_test_minus_train": numeric_delta(row, "test_win_pct", "train_win_pct"),
    }


def numeric_delta(row: dict[str, Any], left: str, right: str) -> float | None:
    left_value = to_float(row.get(left))
    right_value = to_float(row.get(right))
    if left_value is None or right_value is None:
        return None
    return round(left_value - right_value, 6)


def robustness_status(row: dict[str, Any]) -> str:
    test_n = to_float(row.get("test_n")) or 0.0
    test_avg = to_float(row.get("test_avg_net_pct")) or -9999.0
    test_median = to_float(row.get("test_median_net_pct")) or -9999.0
    test_p90_mae_value = to_float(row.get("test_p90_mae_pct"))
    test_cat300_value = to_float(row.get("test_catastrophic_300_pct"))
    test_p90_mae = test_p90_mae_value if test_p90_mae_value is not None else 9999.0
    test_cat300 = test_cat300_value if test_cat300_value is not None else 9999.0
    avg_delta = to_float(row.get("avg_net_delta_test_minus_train")) or -9999.0
    if test_n < 20:
        return "thin_test_sample"
    if test_avg >= 10.0 and test_median >= 10.0 and test_p90_mae <= 120.0 and test_cat300 <= 1.5 and avg_delta >= -20.0:
        return "robust"
    if test_avg >= 0.0 and test_median >= 0.0 and test_cat300 <= 5.0:
        return "mixed_positive"
    return "failed_out_of_sample"


def robustness_score(row: dict[str, Any]) -> float:
    test_avg = to_float(row.get("test_avg_net_pct")) or 0.0
    test_median = to_float(row.get("test_median_net_pct")) or 0.0
    test_win = to_float(row.get("test_win_pct")) or 0.0
    test_p90_mae = to_float(row.get("test_p90_mae_pct")) or 0.0
    test_cat300 = to_float(row.get("test_catastrophic_300_pct")) or 0.0
    avg_delta = abs(to_float(row.get("avg_net_delta_test_minus_train")) or 0.0)
    return round(test_avg * 0.35 + test_median * 0.25 + test_win * 0.2 - test_p90_mae * 0.09 - test_cat300 * 4.0 - avg_delta * 0.08, 6)


def symbol_bucket(symbol: str) -> int:
    return sum(ord(ch) for ch in symbol.upper()) % SYMBOL_HOLDOUT_MOD


def concentration_status(top_positive_share: float | None, symbol_count: int) -> str:
    if symbol_count < 8:
        return "thin_symbol_count"
    if top_positive_share is None:
        return "no_positive_symbols"
    if top_positive_share >= 35.0:
        return "concentrated"
    if top_positive_share >= 25.0:
        return "watch_concentration"
    return "diversified"


def rule_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row.get("entry_strategy") or ""), str(row.get("exit_strategy") or "")): row
        for row in rows
    }


def anti_overfit_status(
    time_row: dict[str, Any],
    symbol_row: dict[str, Any],
    concentration_row: dict[str, Any],
) -> str:
    time_status = str(time_row.get("robustness_status") or "")
    symbol_status = str(symbol_row.get("robustness_status") or "")
    concentration = str(concentration_row.get("concentration_status") or "")
    if time_status == "robust" and symbol_status == "robust" and concentration in {"diversified", "watch_concentration"}:
        return "robust_candidate"
    if time_status in {"robust", "mixed_positive"} and symbol_status in {"robust", "mixed_positive"}:
        return "needs_more_validation"
    if time_status == "failed_out_of_sample" or symbol_status == "failed_out_of_sample":
        return "overfit_risk"
    return "inconclusive"


def anti_overfit_score(
    time_row: dict[str, Any],
    symbol_row: dict[str, Any],
    concentration_row: dict[str, Any],
) -> float:
    time_score = to_float(time_row.get("robustness_score")) or 0.0
    symbol_score = to_float(symbol_row.get("robustness_score")) or 0.0
    top_share = to_float(concentration_row.get("top_positive_symbol_share_pct")) or 50.0
    concentration_penalty = max(0.0, top_share - 25.0) * 0.25
    return round(time_score * 0.55 + symbol_score * 0.45 - concentration_penalty, 6)


def anti_overfit_note(status: str, test_avg: float, holdout_avg: float, top_share: float | None) -> str:
    share = "n/a" if top_share is None else f"{top_share:.1f}%"
    return f"{status}; time_test_avg={test_avg:.1f}; symbol_holdout_avg={holdout_avg:.1f}; top_symbol_share={share}"


def build_worst_tail_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [row for row in rows if (to_float(row.get("mae_pct")) or 0.0) >= 100.0 or (to_float(row.get("net_exit_pct")) or 0.0) <= -100.0]
    candidates.sort(
        key=lambda row: (
            to_float(row.get("mae_pct")) or 0.0,
            -(to_float(row.get("net_exit_pct")) or 0.0),
        ),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for row in candidates[:300]:
        out.append(
            {
                "symbol": row.get("symbol"),
                "event_id": row.get("event_id"),
                "trigger_ts": row.get("trigger_ts"),
                "entry_strategy": row.get("entry_strategy"),
                "exit_strategy": row.get("exit_strategy"),
                "net_exit_pct": row.get("net_exit_pct"),
                "mae_pct": row.get("mae_pct"),
                "mfe_pct": row.get("mfe_pct"),
                "pump_pct": row.get("pump_pct"),
                "age_days": row.get("age_days"),
                "funding_prev_24h_pct": row.get("funding_prev_24h_pct"),
                "oi_change_24h_pct": row.get("oi_change_24h_pct"),
                "long_ratio": row.get("long_ratio"),
                "exit_reason": row.get("exit_reason"),
                "exit_events": row.get("exit_events"),
            }
        )
    return out


def risk_note(*, p90_mae: float, cat300: float, liq3x: float) -> str:
    if cat300 > 0.0:
        return "has_300pct_tail"
    if p90_mae >= 100.0:
        return "needs_large_margin_buffer"
    if liq3x >= 40.0:
        return "unsafe_for_3x_isolated"
    return "cleaner_tail"


def max_effective_leverage(mae_pct: float | None) -> float | None:
    if mae_pct is None or mae_pct <= 0:
        return None
    return round(100.0 / mae_pct, 4)


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


def funding_sum_pct(rows: list[tuple[int, float]], start_ms: int, end_ms: int) -> float | None:
    vals = [rate for ts_ms, rate in rows if start_ms < ts_ms <= end_ms]
    if not vals:
        return None
    return sum(vals) * 100.0


def funding_rates_pct(rows: list[tuple[int, float]], start_ms: int, end_ms: int) -> list[float]:
    return [rate * 100.0 for ts_ms, rate in rows if start_ms < ts_ms <= end_ms]


def point_change_pct(points: dict[int, float], ts: list[int], idx: int, hours: int) -> float | None:
    current = points.get(ts[idx])
    prior_idx = idx - hours
    if prior_idx < 0:
        return None
    prior = points.get(ts[prior_idx])
    if current is None or prior in {None, 0}:
        return None
    return (current / prior - 1.0) * 100.0


def point_change_from_idx_pct(points: dict[int, float], ts: list[int], idx: int, hours: int) -> float | None:
    current = points.get(ts[idx])
    future_idx = idx + hours
    if future_idx >= len(ts):
        return None
    future = points.get(ts[future_idx])
    if current in {None, 0} or future is None:
        return None
    return (future / current - 1.0) * 100.0


def point_max_change_from_idx_pct(points: dict[int, float], ts: list[int], idx: int, hours: int) -> float | None:
    current = points.get(ts[idx])
    if current in {None, 0}:
        return None
    end_idx = min(len(ts) - 1, idx + hours)
    vals = [points.get(ts[item_idx]) for item_idx in range(idx + 1, end_idx + 1)]
    clean = [value for value in vals if value is not None]
    if not clean:
        return None
    return (max(clean) / current - 1.0) * 100.0


def point_min_change_from_idx_pct(points: dict[int, float], ts: list[int], idx: int, hours: int) -> float | None:
    current = points.get(ts[idx])
    if current in {None, 0}:
        return None
    end_idx = min(len(ts) - 1, idx + hours)
    vals = [points.get(ts[item_idx]) for item_idx in range(idx + 1, end_idx + 1)]
    clean = [value for value in vals if value is not None]
    if not clean:
        return None
    return (min(clean) / current - 1.0) * 100.0


def values(rows: list[dict[str, Any]], key: str) -> list[float]:
    out = []
    for row in rows:
        value = row.get(key)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            out.append(float(value))
    return out


def mean(vals: list[float]) -> float | None:
    return round(statistics.mean(vals), 6) if vals else None


def median(vals: list[float]) -> float | None:
    return round(statistics.median(vals), 6) if vals else None


def percentile(vals: list[float], pct_value: float) -> float | None:
    if not vals:
        return None
    vals = sorted(vals)
    idx = int(round((pct_value / 100.0) * (len(vals) - 1)))
    return round(vals[idx], 6)


def pct(count: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round(count / total * 100.0, 6)


def safe_max(vals: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in vals if value is not None]
    return max(clean) if clean else None


def safe_min(vals: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in vals if value is not None]
    return min(clean) if clean else None


def to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def bool_or_none(value: bool) -> bool:
    return bool(value)


__all__ = [
    "DEFAULT_INPUT",
    "DEFAULT_OUTPUT_DIR",
    "HORIZONS_HOURS",
    "LONG_HOLD_EXIT_PLANS",
    "PUMP_CONFIGS",
    "detect_pump_events",
    "detect_pump_episodes",
    "event_behavior_features",
    "run_analysis",
    "sample_to_series",
    "simulate_event_long_hold_outcomes",
    "simulate_event_outcomes",
]
