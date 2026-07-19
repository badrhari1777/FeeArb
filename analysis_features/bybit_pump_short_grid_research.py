from __future__ import annotations

import json
import math
import statistics
import time
from pathlib import Path
from typing import Any, Iterable

from config import BASE_DIR
from analysis_features.bybit_pump_short_outcomes import (
    FEE_ROUNDTRIP_PCT,
    PumpEvent,
    Series,
    close_active_fraction,
    detect_pump_events,
    event_behavior_features,
    event_to_row,
    find_confirmed_pullback_entry,
    find_pullback_entry,
    load_samples,
    open_weight,
    pct,
    percentile,
    point_change_pct,
    sample_to_series,
    to_float,
    weighted_avg_entry,
    weighted_mean,
    weighted_mean_component,
    write_csv,
)

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_grid_research"

LADDER_ENTRY_SETUPS: tuple[dict[str, Any], ...] = (
    {"name": "immediate", "kind": "immediate"},
    {
        "name": "pb20_oi50_lr_mid",
        "kind": "confirmed_pullback",
        "pullback_pct": 20.0,
        "oi_max_pct": 50.0,
    },
    {
        "name": "pb20_oi0_lr_mid",
        "kind": "confirmed_pullback",
        "pullback_pct": 20.0,
        "oi_max_pct": 0.0,
    },
)
LADDER_STEP_PCTS: tuple[float, ...] = (35.0, 50.0, 75.0, 100.0, 150.0, 200.0)
LADDER_MAX_LEGS: tuple[int, ...] = (1, 2, 3, 4, 5, 6)
LADDER_ADD_WINDOWS_H: tuple[int, ...] = (168,)
LADDER_SIZING_MODES: tuple[str, ...] = ("equal", "tapered")
LADDER_EXIT_PLANS: tuple[dict[str, Any], ...] = (
    {"name": "tp25_full_168", "max_hold_h": 168, "targets": ((25.0, 1.0),)},
    {"name": "tp25_50_halves_336", "max_hold_h": 336, "targets": ((25.0, 0.5), (50.0, 1.0))},
)

WAVE_ENTRY_SETUPS: tuple[dict[str, Any], ...] = (
    {
        "name": "pb20_oi50_lr_mid",
        "kind": "confirmed_pullback",
        "pullback_pct": 20.0,
        "oi_max_pct": 50.0,
    },
    {
        "name": "pb20_oi0_lr_mid",
        "kind": "confirmed_pullback",
        "pullback_pct": 20.0,
        "oi_max_pct": 0.0,
    },
)
WAVE_CONFIGS: tuple[dict[str, Any], ...] = tuple(
    {
        "take_profit_pct": take_profit_pct,
        "cover_fraction": cover_fraction,
        "readd_rally_pct": readd_rally_pct,
        "oi_min_pct": oi_min_pct,
        "volume_min_pct": volume_min_pct,
        "max_cycles": max_cycles,
        "max_hold_h": 2160,
        "ladder_step_pct": ladder_step_pct,
        "ladder_max_legs": ladder_max_legs,
    }
    for take_profit_pct in (25.0, 35.0, 50.0)
    for cover_fraction in (0.75,)
    for readd_rally_pct in (50.0, 75.0)
    for oi_min_pct in (25.0, 50.0)
    for volume_min_pct in (50.0,)
    for max_cycles in (1, 2)
    for ladder_step_pct in (50.0, 75.0)
    for ladder_max_legs in (3,)
)


class ResearchSeries:
    def __init__(self, base: Series, volume: list[float | None], turnover: list[float | None]) -> None:
        self.base = base
        self.volume = volume
        self.turnover = turnover


def run_grid_research(
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    symbols_seen = 0
    events_seen = 0
    ladder_rows: list[dict[str, Any]] = []
    wave_rows: list[dict[str, Any]] = []

    for sample in load_samples(input_path):
        symbols_seen += 1
        research_series = sample_to_research_series(sample)
        series = research_series.base
        events = detect_pump_events(series)
        events_seen += len(events)
        for event in events:
            base_row = base_research_row(series, event)
            ladder_rows.extend(simulate_ladder_sweeps(research_series, event, base_row))
            wave_rows.extend(simulate_wave_sweeps(research_series, event, base_row))

    ladder_summary = build_ladder_summary(ladder_rows)
    wave_summary = build_wave_summary(wave_rows)
    superpump_ladder_summary = build_ladder_summary(
        [row for row in ladder_rows if (to_float(row.get("max_adverse_from_first_pct")) or 0.0) >= 300.0],
        extra_prefix={"tail_bucket": "continued_300_plus"},
    )
    best_ladders = rank_ladder_rules(ladder_summary)
    best_waves = rank_wave_rules(wave_summary)
    worst_ladder_tails = worst_tail_rows(ladder_rows)
    worst_wave_tails = worst_tail_rows(wave_rows)

    write_csv(output_dir / "ladder_sweep_outcomes.csv", ladder_rows)
    write_csv(output_dir / "ladder_sweep_summary.csv", ladder_summary)
    write_csv(output_dir / "superpump_ladder_summary.csv", superpump_ladder_summary)
    write_csv(output_dir / "best_ladder_rules.csv", best_ladders)
    write_csv(output_dir / "worst_ladder_tails.csv", worst_ladder_tails)
    write_csv(output_dir / "wave_recycle_outcomes.csv", wave_rows)
    write_csv(output_dir / "wave_recycle_summary.csv", wave_summary)
    write_csv(output_dir / "best_wave_rules.csv", best_waves)
    write_csv(output_dir / "worst_wave_tails.csv", worst_wave_tails)

    metadata = {
        "schema": "bybit_pump_short_grid_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": events_seen,
        "ladder_outcomes": len(ladder_rows),
        "wave_outcomes": len(wave_rows),
        "ladder_rules": len(ladder_summary),
        "wave_rules": len(wave_summary),
        "ladder_entry_setups": [str(item["name"]) for item in LADDER_ENTRY_SETUPS],
        "ladder_step_pcts": list(LADDER_STEP_PCTS),
        "ladder_max_legs": list(LADDER_MAX_LEGS),
        "ladder_add_windows_h": list(LADDER_ADD_WINDOWS_H),
        "ladder_sizing_modes": list(LADDER_SIZING_MODES),
        "ladder_exit_plans": [str(item["name"]) for item in LADDER_EXIT_PLANS],
        "wave_configs": len(WAVE_CONFIGS),
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "grid_research_metadata.json").write_text(
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
    volume = [to_float(row.get("volume")) for row in rows]
    turnover = [to_float(row.get("turnover")) for row in rows]
    return ResearchSeries(base=base, volume=volume, turnover=turnover)


def simulate_ladder_sweeps(
    research_series: ResearchSeries,
    event: PumpEvent,
    base_row: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    series = research_series.base
    for setup in LADDER_ENTRY_SETUPS:
        entry_idx = resolve_entry_idx(series, event, setup)
        if entry_idx is None:
            continue
        for step_pct in LADDER_STEP_PCTS:
            for max_legs in LADDER_MAX_LEGS:
                for add_window_h in LADDER_ADD_WINDOWS_H:
                    for sizing_mode in LADDER_SIZING_MODES:
                        for exit_plan in LADDER_EXIT_PLANS:
                            row = simulate_ladder_rule(
                                series,
                                event,
                                base_row,
                                entry_setup=str(setup["name"]),
                                entry_idx=entry_idx,
                                step_pct=step_pct,
                                max_legs=max_legs,
                                add_window_h=add_window_h,
                                sizing_mode=sizing_mode,
                                exit_plan=exit_plan,
                            )
                            if row:
                                rows.append(row)
    return rows


def simulate_wave_sweeps(
    research_series: ResearchSeries,
    event: PumpEvent,
    base_row: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    series = research_series.base
    for setup in WAVE_ENTRY_SETUPS:
        entry_idx = resolve_entry_idx(series, event, setup)
        if entry_idx is None:
            continue
        for config in WAVE_CONFIGS:
            row = simulate_wave_rule(
                research_series,
                event,
                base_row,
                entry_setup=str(setup["name"]),
                entry_idx=entry_idx,
                config=config,
            )
            if row:
                rows.append(row)
    return rows


def resolve_entry_idx(series: Series, event: PumpEvent, setup: dict[str, Any]) -> int | None:
    kind = str(setup.get("kind") or "")
    if kind == "immediate":
        return event.trigger_idx
    if kind == "pullback":
        return find_pullback_entry(
            series,
            event.trigger_idx,
            float(setup.get("pullback_pct") or 20.0),
            max_wait_h=168,
        )
    if kind == "confirmed_pullback":
        return find_confirmed_pullback_entry(
            series,
            event.trigger_idx,
            float(setup.get("pullback_pct") or 20.0),
            max_wait_h=168,
            oi_max_pct=float(setup.get("oi_max_pct") or 50.0),
            long_ratio_min=0.45,
            long_ratio_max=0.65,
            funding_min_pct=-1.0,
        )
    return None


def simulate_ladder_rule(
    series: Series,
    event: PumpEvent,
    base_row: dict[str, Any],
    *,
    entry_setup: str,
    entry_idx: int,
    step_pct: float,
    max_legs: int,
    add_window_h: int,
    sizing_mode: str,
    exit_plan: dict[str, Any],
) -> dict[str, Any] | None:
    first_price = series.close[entry_idx]
    if not first_price or first_price <= 0:
        return None
    planned = planned_ladder(float(first_price), step_pct, max_legs, sizing_mode)
    return simulate_static_ladder(
        series,
        event,
        base_row,
        entry_setup=entry_setup,
        entry_idx=entry_idx,
        planned=planned,
        step_pct=step_pct,
        max_legs=max_legs,
        add_window_h=add_window_h,
        sizing_mode=sizing_mode,
        exit_strategy=str(exit_plan["name"]),
        max_hold_h=int(exit_plan["max_hold_h"]),
        targets=tuple(exit_plan.get("targets") or ()),
    )


def simulate_static_ladder(
    series: Series,
    event: PumpEvent,
    base_row: dict[str, Any],
    *,
    entry_setup: str,
    entry_idx: int,
    planned: list[dict[str, float]],
    step_pct: float,
    max_legs: int,
    add_window_h: int,
    sizing_mode: str,
    exit_strategy: str,
    max_hold_h: int,
    targets: tuple[tuple[float, float], ...],
) -> dict[str, Any] | None:
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + max_hold_h)
    if exit_limit_idx <= entry_idx or not planned:
        return None
    planned_weight = sum(item["weight"] for item in planned)
    active = [{"idx": entry_idx, "price": planned[0]["price"], "weight": planned[0]["weight"]}]
    activated_count = 1
    activated_weight = planned[0]["weight"]
    realized: list[tuple[float, float, float]] = []
    exit_events: list[str] = []
    target_idx = 0
    max_mae: float | None = None
    max_adverse_from_first: float | None = None
    max_margin_stress: float | None = None
    peak_active_weight = activated_weight
    exit_idx = exit_limit_idx
    exit_reason = "time_stop"

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        high = series.high[idx]
        low = series.low[idx]
        if idx - entry_idx <= add_window_h and high is not None:
            while activated_count < len(planned) and high >= planned[activated_count]["price"]:
                leg = planned[activated_count]
                active.append({"idx": idx, "price": leg["price"], "weight": leg["weight"]})
                activated_count += 1
                activated_weight += leg["weight"]
                peak_active_weight = max(peak_active_weight, open_weight(active))
        active = [item for item in active if float(item["weight"]) > 1e-9]
        if not active:
            continue

        avg_entry = weighted_avg_entry(active)
        if high is not None and avg_entry:
            max_mae = max_or_init(max_mae, (high / avg_entry - 1.0) * 100.0)
            max_adverse_from_first = max_or_init(max_adverse_from_first, (high / planned[0]["price"] - 1.0) * 100.0)
            stress = sum(float(item["weight"]) * max(0.0, high / float(item["price"]) - 1.0) * 100.0 for item in active)
            max_margin_stress = max_or_init(max_margin_stress, stress / planned_weight)

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
    deployed_weight = sum(weight for _, weight, _ in realized)
    net_deployed = weighted_mean(realized)
    net_reserved = sum(net * weight for net, weight, _ in realized) / planned_weight
    funding_deployed = weighted_mean_component(realized, 2)
    row = dict(base_row)
    row.update(
        {
            "research_family": "ladder_sweep",
            "entry_setup": entry_setup,
            "exit_strategy": exit_strategy,
            "step_pct": step_pct,
            "max_legs": max_legs,
            "add_window_h": add_window_h,
            "sizing_mode": sizing_mode,
            "entry_ts": series.ts[entry_idx],
            "exit_ts": series.ts[exit_idx],
            "time_in_trade_h": exit_idx - entry_idx,
            "exit_reason": exit_reason,
            "exit_events": "|".join(exit_events),
            "planned_weight": planned_weight,
            "deployed_weight": deployed_weight,
            "activated_weight": activated_weight,
            "peak_active_weight": peak_active_weight,
            "legs_activated": activated_count,
            "activation_weight_pct": pct_float(activated_weight, planned_weight),
            "net_deployed_pct": net_deployed,
            "net_reserved_pct": net_reserved,
            "funding_deployed_pct": funding_deployed,
            "mae_pct": max_mae,
            "max_adverse_from_first_pct": max_adverse_from_first,
            "max_margin_stress_reserved_pct": max_margin_stress,
            "win_deployed": 1 if net_deployed > 0 else 0,
            "win_reserved": 1 if net_reserved > 0 else 0,
            "cat300_first": 1 if (max_adverse_from_first or 0.0) >= 300.0 else 0,
            "cat700_first": 1 if (max_adverse_from_first or 0.0) >= 700.0 else 0,
            "cat1000_first": 1 if (max_adverse_from_first or 0.0) >= 1000.0 else 0,
            "stress100_reserved": 1 if (max_margin_stress or 0.0) >= 100.0 else 0,
            "stress200_reserved": 1 if (max_margin_stress or 0.0) >= 200.0 else 0,
        }
    )
    return row


def simulate_wave_rule(
    research_series: ResearchSeries,
    event: PumpEvent,
    base_row: dict[str, Any],
    *,
    entry_setup: str,
    entry_idx: int,
    config: dict[str, Any],
) -> dict[str, Any] | None:
    series = research_series.base
    first_price = series.close[entry_idx]
    if not first_price or first_price <= 0:
        return None
    max_hold_h = int(config["max_hold_h"])
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + max_hold_h)
    if exit_limit_idx <= entry_idx:
        return None
    planned = planned_ladder(
        float(first_price),
        float(config["ladder_step_pct"]),
        int(config["ladder_max_legs"]),
        "equal",
    )
    planned_weight = sum(item["weight"] for item in planned)
    active = [{"idx": entry_idx, "price": planned[0]["price"], "weight": planned[0]["weight"]}]
    activated_count = 1
    realized: list[tuple[float, float, float]] = []
    exit_events: list[str] = []
    max_mae: float | None = None
    max_adverse_from_first: float | None = None
    max_margin_stress: float | None = None
    peak_active_weight = 1.0
    restore_weight = 1.0
    last_cover_price: float | None = None
    cycles = 0
    readds = 0
    add_window_h = 168

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        high = series.high[idx]
        low = series.low[idx]
        if idx - entry_idx <= add_window_h and high is not None:
            while activated_count < len(planned) and high >= planned[activated_count]["price"]:
                leg = planned[activated_count]
                active.append({"idx": idx, "price": leg["price"], "weight": leg["weight"]})
                activated_count += 1
                restore_weight = max(restore_weight, open_weight(active))
                peak_active_weight = max(peak_active_weight, open_weight(active))

        active = [item for item in active if float(item["weight"]) > 1e-9]
        if not active:
            continue
        avg_entry = weighted_avg_entry(active)
        if high is not None and avg_entry:
            max_mae = max_or_init(max_mae, (high / avg_entry - 1.0) * 100.0)
            max_adverse_from_first = max_or_init(max_adverse_from_first, (high / planned[0]["price"] - 1.0) * 100.0)
            stress = sum(float(item["weight"]) * max(0.0, high / float(item["price"]) - 1.0) * 100.0 for item in active)
            max_margin_stress = max_or_init(max_margin_stress, stress / planned_weight)

        if cycles < int(config["max_cycles"]) and avg_entry and low is not None:
            target_price = avg_entry * (1.0 - float(config["take_profit_pct"]) / 100.0)
            if low <= target_price and open_weight(active) > 1e-9:
                restore_weight = max(restore_weight, open_weight(active))
                closed = close_active_fraction(
                    series,
                    active,
                    exit_idx=idx,
                    fill_price=target_price,
                    fraction=float(config["cover_fraction"]),
                )
                realized.extend(closed)
                cycles += 1
                last_cover_price = target_price
                exit_events.append(
                    f"cover{int(float(config['cover_fraction']) * 100)}_tp{int(float(config['take_profit_pct']))}:{round(sum(weight for _, weight, _ in closed), 6)}"
                )

        if last_cover_price and open_weight(active) + 1e-9 < restore_weight and high is not None:
            readd_level = last_cover_price * (1.0 + float(config["readd_rally_pct"]) / 100.0)
            if high >= readd_level and readd_signal_ok(research_series, idx, config):
                add_weight = min(restore_weight - open_weight(active), planned_weight - open_weight(active))
                if add_weight > 1e-9:
                    active.append({"idx": idx, "price": readd_level, "weight": add_weight})
                    readds += 1
                    peak_active_weight = max(peak_active_weight, open_weight(active))
                    last_cover_price = None
                    exit_events.append(f"readd{int(float(config['readd_rally_pct']))}:{round(add_weight, 6)}")

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
        exit_events.append("final_runner:all")

    if not realized:
        return None
    deployed_weight = sum(weight for _, weight, _ in realized)
    net_deployed = weighted_mean(realized)
    net_reserved = sum(net * weight for net, weight, _ in realized) / planned_weight
    row = dict(base_row)
    row.update(
        {
            "research_family": "wave_recycle",
            "entry_setup": entry_setup,
            "take_profit_pct": config["take_profit_pct"],
            "cover_fraction": config["cover_fraction"],
            "readd_rally_pct": config["readd_rally_pct"],
            "oi_min_pct": config["oi_min_pct"],
            "volume_min_pct": config["volume_min_pct"],
            "max_cycles": config["max_cycles"],
            "ladder_step_pct": config["ladder_step_pct"],
            "ladder_max_legs": config["ladder_max_legs"],
            "entry_ts": series.ts[entry_idx],
            "exit_ts": series.ts[exit_limit_idx],
            "time_in_trade_h": exit_limit_idx - entry_idx,
            "exit_events": "|".join(exit_events),
            "cycles": cycles,
            "readds": readds,
            "planned_weight": planned_weight,
            "deployed_weight": deployed_weight,
            "peak_active_weight": peak_active_weight,
            "legs_activated": activated_count,
            "net_deployed_pct": net_deployed,
            "net_reserved_pct": net_reserved,
            "funding_deployed_pct": weighted_mean_component(realized, 2),
            "mae_pct": max_mae,
            "max_adverse_from_first_pct": max_adverse_from_first,
            "max_margin_stress_reserved_pct": max_margin_stress,
            "win_deployed": 1 if net_deployed > 0 else 0,
            "win_reserved": 1 if net_reserved > 0 else 0,
            "cat300_first": 1 if (max_adverse_from_first or 0.0) >= 300.0 else 0,
            "cat700_first": 1 if (max_adverse_from_first or 0.0) >= 700.0 else 0,
            "cat1000_first": 1 if (max_adverse_from_first or 0.0) >= 1000.0 else 0,
            "stress100_reserved": 1 if (max_margin_stress or 0.0) >= 100.0 else 0,
            "stress200_reserved": 1 if (max_margin_stress or 0.0) >= 200.0 else 0,
        }
    )
    return row


def planned_ladder(first_price: float, step_pct: float, max_legs: int, sizing_mode: str) -> list[dict[str, float]]:
    weights = sizing_weights(max_legs, sizing_mode)
    return [
        {
            "price": first_price * (1.0 + step_pct / 100.0 * idx),
            "weight": weights[idx],
        }
        for idx in range(max_legs)
    ]


def sizing_weights(max_legs: int, sizing_mode: str) -> list[float]:
    if sizing_mode == "tapered":
        base = (1.0, 0.75, 0.5, 0.35, 0.25, 0.2)
        return [base[idx] if idx < len(base) else 0.2 for idx in range(max_legs)]
    return [1.0 for _ in range(max_legs)]


def readd_signal_ok(research_series: ResearchSeries, idx: int, config: dict[str, Any]) -> bool:
    series = research_series.base
    oi_change = point_change_pct(series.oi, series.ts, idx, 24)
    volume_change = rolling_sum_change_pct(research_series.volume, idx, 24)
    oi_min = float(config["oi_min_pct"])
    volume_min = float(config["volume_min_pct"])
    if oi_change is None or oi_change < oi_min:
        return False
    if volume_change is None or volume_change < volume_min:
        return False
    return True


def rolling_sum_change_pct(values: list[float | None], idx: int, window: int) -> float | None:
    if idx - window * 2 + 1 < 0:
        return None
    current = sum_clean(values[idx - window + 1 : idx + 1])
    prior = sum_clean(values[idx - window * 2 + 1 : idx - window + 1])
    if current is None or prior in {None, 0.0}:
        return None
    return (current / prior - 1.0) * 100.0


def sum_clean(values: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return sum(clean) if clean else None


def base_research_row(series: Series, event: PumpEvent) -> dict[str, Any]:
    row = event_to_row(event)
    row.update(event_behavior_features(series, event))
    return row


def build_ladder_summary(rows: list[dict[str, Any]], extra_prefix: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return summarize(
        rows,
        keys=("entry_setup", "exit_strategy", "step_pct", "max_legs", "add_window_h", "sizing_mode"),
        extra_prefix=extra_prefix,
    )


def build_wave_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return summarize(
        rows,
        keys=(
            "entry_setup",
            "take_profit_pct",
            "cover_fraction",
            "readd_rally_pct",
            "oi_min_pct",
            "volume_min_pct",
            "max_cycles",
            "ladder_step_pct",
            "ladder_max_legs",
        ),
    )


def summarize(
    rows: list[dict[str, Any]],
    *,
    keys: tuple[str, ...],
    extra_prefix: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(key) for key in keys), []).append(row)
    out: list[dict[str, Any]] = []
    for key_values, items in groups.items():
        row = dict(extra_prefix or {})
        row.update({key: value for key, value in zip(keys, key_values)})
        row.update(aggregate_research_rows(items))
        out.append(row)
    return out


def aggregate_research_rows(items: list[dict[str, Any]]) -> dict[str, Any]:
    net_deployed = numeric_values(items, "net_deployed_pct")
    net_reserved = numeric_values(items, "net_reserved_pct")
    mae = numeric_values(items, "mae_pct")
    first_adverse = numeric_values(items, "max_adverse_from_first_pct")
    margin_stress = numeric_values(items, "max_margin_stress_reserved_pct")
    activated = numeric_values(items, "legs_activated")
    cycles = numeric_values(items, "cycles")
    readds = numeric_values(items, "readds")
    return {
        "n": len(items),
        "symbol_count": len({str(item.get("symbol") or "") for item in items}),
        "win_deployed_pct": pct(sum(1 for item in items if item.get("win_deployed")), len(items)),
        "win_reserved_pct": pct(sum(1 for item in items if item.get("win_reserved")), len(items)),
        "avg_net_deployed_pct": rounded_mean(net_deployed),
        "median_net_deployed_pct": rounded_median(net_deployed),
        "avg_net_reserved_pct": rounded_mean(net_reserved),
        "median_net_reserved_pct": rounded_median(net_reserved),
        "p25_net_reserved_pct": percentile(net_reserved, 25),
        "p75_net_reserved_pct": percentile(net_reserved, 75),
        "median_mae_pct": rounded_median(mae),
        "p90_mae_pct": percentile(mae, 90),
        "p95_mae_pct": percentile(mae, 95),
        "p99_mae_pct": percentile(mae, 99),
        "p90_first_adverse_pct": percentile(first_adverse, 90),
        "p95_first_adverse_pct": percentile(first_adverse, 95),
        "p99_first_adverse_pct": percentile(first_adverse, 99),
        "p90_margin_stress_reserved_pct": percentile(margin_stress, 90),
        "p95_margin_stress_reserved_pct": percentile(margin_stress, 95),
        "p99_margin_stress_reserved_pct": percentile(margin_stress, 99),
        "avg_legs_activated": rounded_mean(activated),
        "cat300_first_pct": pct(sum(1 for item in items if item.get("cat300_first")), len(items)),
        "cat700_first_pct": pct(sum(1 for item in items if item.get("cat700_first")), len(items)),
        "cat1000_first_pct": pct(sum(1 for item in items if item.get("cat1000_first")), len(items)),
        "stress100_reserved_pct": pct(sum(1 for item in items if item.get("stress100_reserved")), len(items)),
        "stress200_reserved_pct": pct(sum(1 for item in items if item.get("stress200_reserved")), len(items)),
        "avg_cycles": rounded_mean(cycles),
        "avg_readds": rounded_mean(readds),
    }


def rank_ladder_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    for row in rows:
        if (to_float(row.get("n")) or 0.0) < 100:
            continue
        item = dict(row)
        item["research_score"] = ladder_score(row)
        item["research_note"] = ladder_note(row)
        ranked.append(item)
    ranked.sort(key=lambda item: to_float(item.get("research_score")) or -9999.0, reverse=True)
    for idx, item in enumerate(ranked[:100], start=1):
        item["rank"] = idx
    return ranked[:100]


def rank_wave_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    for row in rows:
        if (to_float(row.get("n")) or 0.0) < 100:
            continue
        item = dict(row)
        item["research_score"] = wave_score(row)
        item["research_note"] = wave_note(row)
        ranked.append(item)
    ranked.sort(key=lambda item: to_float(item.get("research_score")) or -9999.0, reverse=True)
    for idx, item in enumerate(ranked[:100], start=1):
        item["rank"] = idx
    return ranked[:100]


def ladder_score(row: dict[str, Any]) -> float:
    avg_reserved = to_float(row.get("avg_net_reserved_pct")) or 0.0
    median_reserved = to_float(row.get("median_net_reserved_pct")) or 0.0
    win_reserved = to_float(row.get("win_reserved_pct")) or 0.0
    p90_stress = to_float(row.get("p90_margin_stress_reserved_pct")) or 0.0
    p95_mae = to_float(row.get("p95_mae_pct")) or 0.0
    cat700 = to_float(row.get("cat700_first_pct")) or 0.0
    cat1000 = to_float(row.get("cat1000_first_pct")) or 0.0
    stress200 = to_float(row.get("stress200_reserved_pct")) or 0.0
    return round(
        avg_reserved * 0.35
        + median_reserved * 0.25
        + win_reserved * 0.18
        - p90_stress * 0.08
        - p95_mae * 0.04
        - cat700 * 3.0
        - cat1000 * 6.0
        - stress200 * 0.8,
        6,
    )


def wave_score(row: dict[str, Any]) -> float:
    avg_reserved = to_float(row.get("avg_net_reserved_pct")) or 0.0
    median_reserved = to_float(row.get("median_net_reserved_pct")) or 0.0
    win_reserved = to_float(row.get("win_reserved_pct")) or 0.0
    p90_stress = to_float(row.get("p90_margin_stress_reserved_pct")) or 0.0
    cat1000 = to_float(row.get("cat1000_first_pct")) or 0.0
    readds = to_float(row.get("avg_readds")) or 0.0
    return round(
        avg_reserved * 0.35
        + median_reserved * 0.25
        + win_reserved * 0.16
        + min(3.0, readds) * 1.5
        - p90_stress * 0.07
        - cat1000 * 6.0,
        6,
    )


def ladder_note(row: dict[str, Any]) -> str:
    return (
        f"reserved_avg={fmt(row.get('avg_net_reserved_pct'))}, "
        f"win={fmt(row.get('win_reserved_pct'))}, "
        f"p90_stress={fmt(row.get('p90_margin_stress_reserved_pct'))}, "
        f"legs={row.get('max_legs')} step={row.get('step_pct')}"
    )


def wave_note(row: dict[str, Any]) -> str:
    return (
        f"reserved_avg={fmt(row.get('avg_net_reserved_pct'))}, "
        f"win={fmt(row.get('win_reserved_pct'))}, "
        f"cycles={fmt(row.get('avg_cycles'))}, readds={fmt(row.get('avg_readds'))}"
    )


def worst_tail_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [
        row
        for row in rows
        if (to_float(row.get("max_adverse_from_first_pct")) or 0.0) >= 300.0
        or (to_float(row.get("max_margin_stress_reserved_pct")) or 0.0) >= 150.0
    ]
    candidates.sort(
        key=lambda row: (
            to_float(row.get("max_margin_stress_reserved_pct")) or 0.0,
            to_float(row.get("max_adverse_from_first_pct")) or 0.0,
        ),
        reverse=True,
    )
    keys = (
        "symbol",
        "event_id",
        "trigger_ts",
        "entry_setup",
        "exit_strategy",
        "step_pct",
        "max_legs",
        "sizing_mode",
        "net_reserved_pct",
        "net_deployed_pct",
        "mae_pct",
        "max_adverse_from_first_pct",
        "max_margin_stress_reserved_pct",
        "legs_activated",
        "pump_pct",
        "funding_regime",
        "oi_regime",
        "exit_events",
    )
    return [{key: row.get(key) for key in keys if key in row} for row in candidates[:300]]


def numeric_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    vals = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None:
            vals.append(value)
    return vals


def rounded_mean(vals: list[float]) -> float | None:
    return round(statistics.mean(vals), 6) if vals else None


def rounded_median(vals: list[float]) -> float | None:
    return round(statistics.median(vals), 6) if vals else None


def max_or_init(current: float | None, value: float) -> float:
    return value if current is None else max(current, value)


def pct_float(part: float, total: float) -> float | None:
    if total <= 0:
        return None
    return round(part / total * 100.0, 6)


def fmt(value: Any) -> str:
    number = to_float(value)
    return "n/a" if number is None else f"{number:.1f}"
