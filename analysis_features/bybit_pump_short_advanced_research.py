from __future__ import annotations

import csv
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any, Iterable

from config import BASE_DIR
from analysis_features.bybit_pump_short_grid_research import (
    ResearchSeries,
    max_or_init,
    planned_ladder,
    rolling_sum_change_pct,
    sample_to_research_series,
)
from analysis_features.bybit_pump_short_outcomes import (
    PumpEvent,
    Series,
    close_active_fraction,
    detect_pump_events,
    event_behavior_features,
    event_to_row,
    find_confirmed_pullback_entry,
    load_samples,
    open_weight,
    pct,
    percentile,
    point_change_pct,
    safe_min,
    to_float,
    weighted_avg_entry,
    weighted_mean,
    weighted_mean_component,
    write_csv,
)

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_GRID_OUTCOMES = (
    BASE_DIR
    / "data"
    / "research"
    / "bybit_pump_short_extended_grid_research"
    / "ladder_sweep_outcomes.csv"
)
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_advanced_research"

ENTRY_SETUPS: tuple[dict[str, Any], ...] = tuple(
    {
        "name": f"pb{pullback}_{label}",
        "pullback_pct": float(pullback),
        "oi_max_pct": float(oi_max),
    }
    for pullback in (20,)
    for label, oi_max in (("oi50_lr_mid", 50.0), ("oi0_lr_mid", 0.0))
)
RUNNER_LADDER_CONFIGS: tuple[dict[str, Any], ...] = (
    tuple({"step_pct": 50.0, "max_legs": int(max_legs), "sizing_mode": "equal"} for max_legs in (3, 4, 5, 6))
    + (
        {"step_pct": 35.0, "max_legs": 4, "sizing_mode": "equal"},
        {"step_pct": 75.0, "max_legs": 4, "sizing_mode": "equal"},
        {"step_pct": 50.0, "max_legs": 4, "sizing_mode": "tapered"},
    )
)
RUNNER_PLANS: tuple[dict[str, Any], ...] = tuple(
    {
        "take_profit_pct": float(take_profit),
        "cover_fraction": float(cover),
        "max_hold_h": int(max_hold_h),
    }
    for take_profit in (25, 50)
    for cover in (0.75, 0.85, 0.90)
    for max_hold_h in (720, 2160)
)
TIME_COVER_PLANS: tuple[dict[str, Any], ...] = tuple(
    {
        "time_cover_h": int(time_cover_h),
        "cover_fraction": float(cover),
        "max_hold_h": int(max_hold_h),
    }
    for time_cover_h in (168, 720)
    for cover in (0.75, 0.90)
    for max_hold_h in (2160, 4320)
    if time_cover_h < max_hold_h
)
COOLING_READD_CONFIGS: tuple[dict[str, Any], ...] = tuple(
    {
        "take_profit_pct": float(take_profit),
        "cover_fraction": float(cover),
        "readd_rally_pct": float(readd_rally),
        "oi_cool_min_pct": float(oi_cool),
        "oi_reexpand_min_pct": float(oi_reexpand),
        "volume_min_pct": float(volume_min),
        "max_cycles": int(max_cycles),
        "max_hold_h": 2160,
        "step_pct": 50.0,
        "max_legs": int(max_legs),
    }
    for take_profit in (25, 50)
    for cover in (0.75, 0.90)
    for readd_rally in (75,)
    for oi_cool in (-40,)
    for oi_reexpand in (50,)
    for volume_min in (100,)
    for max_cycles in (1,)
    for max_legs in (3, 4)
)
FUNDING_GATES: tuple[dict[str, Any], ...] = (
    {"name": "all", "live_safe": True},
    {"name": "prev24_gt_-0.25", "live_safe": True, "funding_prev_24h_min": -0.25},
    {"name": "prev24_gt_-0.50", "live_safe": True, "funding_prev_24h_min": -0.50},
    {"name": "prev24_gt_-1.00", "live_safe": True, "funding_prev_24h_min": -1.00},
    {"name": "prev72_gt_-1.00", "live_safe": True, "funding_prev_72h_min": -1.00},
    {"name": "prev72_gt_-2.00", "live_safe": True, "funding_prev_72h_min": -2.00},
    {"name": "prev24_gt_-0.50_prev72_gt_-2", "live_safe": True, "funding_prev_24h_min": -0.50, "funding_prev_72h_min": -2.0},
    {"name": "exclude_extreme_funding_regime", "live_safe": False, "exclude_funding_regimes": {"extreme_negative_funding"}},
    {
        "name": "exclude_persistent_or_extreme_regime",
        "live_safe": False,
        "exclude_funding_regimes": {"persistent_negative_funding", "extreme_negative_funding"},
    },
)


def run_advanced_research(
    input_path: Path = DEFAULT_INPUT,
    grid_outcomes_path: Path = DEFAULT_GRID_OUTCOMES,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    funding_gate_rows = build_funding_gate_reports(grid_outcomes_path)
    write_csv(output_dir / "funding_gate_ladder_summary.csv", funding_gate_rows)
    write_csv(output_dir / "best_funding_gate_ladder_rules.csv", rank_rules(funding_gate_rows))

    runner_rows: list[dict[str, Any]] = []
    readd_rows: list[dict[str, Any]] = []
    symbols_seen = 0
    events_seen = 0
    for sample in load_samples(input_path):
        symbols_seen += 1
        research_series = sample_to_research_series(sample)
        series = research_series.base
        events = detect_pump_events(series)
        events_seen += len(events)
        for event in events:
            base_row = base_research_row(series, event)
            entry_indices = resolve_entry_indices(series, event)
            for entry_setup, entry_idx in entry_indices:
                runner_rows.extend(simulate_runner_family(series, event, base_row, entry_setup, entry_idx))
                readd_rows.extend(simulate_cooling_readd_family(research_series, event, base_row, entry_setup, entry_idx))

    runner_summary = summarize_runner_rows(runner_rows)
    readd_summary = summarize_readd_rows(readd_rows)
    write_csv(output_dir / "small_runner_outcomes.csv", runner_rows)
    write_csv(output_dir / "small_runner_summary.csv", runner_summary)
    write_csv(output_dir / "best_small_runner_rules.csv", rank_rules(runner_summary))
    write_csv(output_dir / "cooling_readd_outcomes.csv", readd_rows)
    write_csv(output_dir / "cooling_readd_summary.csv", readd_summary)
    write_csv(output_dir / "best_cooling_readd_rules.csv", rank_rules(readd_summary))
    write_csv(output_dir / "advanced_research_worst_tails.csv", worst_tails(runner_rows + readd_rows))

    metadata = {
        "schema": "bybit_pump_short_advanced_research_v1",
        "input_path": str(input_path),
        "grid_outcomes_path": str(grid_outcomes_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": events_seen,
        "funding_gate_rows": len(funding_gate_rows),
        "runner_outcomes": len(runner_rows),
        "runner_rules": len(runner_summary),
        "cooling_readd_outcomes": len(readd_rows),
        "cooling_readd_rules": len(readd_summary),
        "entry_setups": [str(item["name"]) for item in ENTRY_SETUPS],
        "runner_ladder_configs": len(RUNNER_LADDER_CONFIGS),
        "runner_plans": len(RUNNER_PLANS),
        "time_cover_plans": len(TIME_COVER_PLANS),
        "cooling_readd_configs": len(COOLING_READD_CONFIGS),
        "funding_gates": [str(item["name"]) for item in FUNDING_GATES],
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "advanced_research_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def build_funding_gate_reports(grid_outcomes_path: Path) -> list[dict[str, Any]]:
    if not grid_outcomes_path.exists():
        return []
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    with grid_outcomes_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if not row_passes_core_ladder_scope(row):
                continue
            for gate in FUNDING_GATES:
                if not funding_gate_passes(row, gate):
                    continue
                key = (
                    gate["name"],
                    bool(gate.get("live_safe")),
                    row.get("entry_setup"),
                    row.get("exit_strategy"),
                    row.get("step_pct"),
                    row.get("max_legs"),
                    row.get("sizing_mode"),
                )
                groups.setdefault(key, []).append(row)
    out: list[dict[str, Any]] = []
    for key, items in groups.items():
        gate_name, live_safe, entry_setup, exit_strategy, step_pct, max_legs, sizing_mode = key
        prefix = {
            "gate_name": gate_name,
            "gate_live_safe": live_safe,
            "entry_setup": entry_setup,
            "exit_strategy": exit_strategy,
            "step_pct": step_pct,
            "max_legs": max_legs,
            "sizing_mode": sizing_mode,
        }
        prefix.update(aggregate_rows(items))
        out.append(prefix)
    return out


def row_passes_core_ladder_scope(row: dict[str, Any]) -> bool:
    return (
        row.get("entry_setup") in {"pb20_oi50_lr_mid", "pb20_oi0_lr_mid"}
        and row.get("exit_strategy") in {"tp25_full_168", "tp25_50_halves_336"}
        and row.get("step_pct") in {"35.0", "50.0", "75.0", "100.0"}
        and row.get("max_legs") in {"3", "4", "5", "6"}
    )


def funding_gate_passes(row: dict[str, Any], gate: dict[str, Any]) -> bool:
    prev24 = to_float(row.get("funding_prev_24h_pct"))
    prev72 = to_float(row.get("funding_prev_72h_pct"))
    min24 = gate.get("funding_prev_24h_min")
    min72 = gate.get("funding_prev_72h_min")
    if min24 is not None and (prev24 is None or prev24 <= float(min24)):
        return False
    if min72 is not None and (prev72 is None or prev72 <= float(min72)):
        return False
    excluded = gate.get("exclude_funding_regimes")
    if excluded and row.get("funding_regime") in excluded:
        return False
    return True


def resolve_entry_indices(series: Series, event: PumpEvent) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for setup in ENTRY_SETUPS:
        idx = find_confirmed_pullback_entry(
            series,
            event.trigger_idx,
            float(setup["pullback_pct"]),
            max_wait_h=168,
            oi_max_pct=float(setup["oi_max_pct"]),
            long_ratio_min=0.45,
            long_ratio_max=0.65,
            funding_min_pct=-1.0,
        )
        if idx is not None:
            out.append((str(setup["name"]), idx))
    return out


def simulate_runner_family(
    series: Series,
    event: PumpEvent,
    base_row: dict[str, Any],
    entry_setup: str,
    entry_idx: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    first_price = series.close[entry_idx]
    if not first_price or first_price <= 0:
        return rows
    for ladder_config in RUNNER_LADDER_CONFIGS:
        planned = planned_ladder(
            float(first_price),
            float(ladder_config["step_pct"]),
            int(ladder_config["max_legs"]),
            str(ladder_config["sizing_mode"]),
        )
        for plan in RUNNER_PLANS:
            row = simulate_small_runner(
                series,
                base_row,
                entry_setup=entry_setup,
                entry_idx=entry_idx,
                planned=planned,
                plan_type="tp_runner",
                step_pct=float(ladder_config["step_pct"]),
                max_legs=int(ladder_config["max_legs"]),
                sizing_mode=str(ladder_config["sizing_mode"]),
                take_profit_pct=float(plan["take_profit_pct"]),
                cover_fraction=float(plan["cover_fraction"]),
                max_hold_h=int(plan["max_hold_h"]),
            )
            if row:
                rows.append(row)
        for plan in TIME_COVER_PLANS:
            row = simulate_small_runner(
                series,
                base_row,
                entry_setup=entry_setup,
                entry_idx=entry_idx,
                planned=planned,
                plan_type="time_cover_runner",
                step_pct=float(ladder_config["step_pct"]),
                max_legs=int(ladder_config["max_legs"]),
                sizing_mode=str(ladder_config["sizing_mode"]),
                time_cover_h=int(plan["time_cover_h"]),
                cover_fraction=float(plan["cover_fraction"]),
                max_hold_h=int(plan["max_hold_h"]),
            )
            if row:
                rows.append(row)
    return rows


def simulate_small_runner(
    series: Series,
    base_row: dict[str, Any],
    *,
    entry_setup: str,
    entry_idx: int,
    planned: list[dict[str, float]],
    plan_type: str,
    step_pct: float,
    max_legs: int,
    sizing_mode: str,
    cover_fraction: float,
    max_hold_h: int,
    take_profit_pct: float | None = None,
    time_cover_h: int | None = None,
) -> dict[str, Any] | None:
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + max_hold_h)
    if exit_limit_idx <= entry_idx or not planned:
        return None
    planned_weight = sum(item["weight"] for item in planned)
    active = [{"idx": entry_idx, "price": planned[0]["price"], "weight": planned[0]["weight"]}]
    activated_count = 1
    realized: list[tuple[float, float, float]] = []
    exit_events: list[str] = []
    covered = False
    max_mae: float | None = None
    max_margin_stress: float | None = None
    max_adverse_first: float | None = None
    add_window_h = 168

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        high = series.high[idx]
        low = series.low[idx]
        if idx - entry_idx <= add_window_h and high is not None:
            while activated_count < len(planned) and high >= planned[activated_count]["price"]:
                leg = planned[activated_count]
                active.append({"idx": idx, "price": leg["price"], "weight": leg["weight"]})
                activated_count += 1
        active = [item for item in active if float(item["weight"]) > 1e-9]
        if not active:
            continue
        avg_entry = weighted_avg_entry(active)
        if high is not None and avg_entry:
            max_mae = max_or_init(max_mae, (high / avg_entry - 1.0) * 100.0)
            max_adverse_first = max_or_init(max_adverse_first, (high / planned[0]["price"] - 1.0) * 100.0)
            stress = sum(float(item["weight"]) * max(0.0, high / float(item["price"]) - 1.0) * 100.0 for item in active)
            max_margin_stress = max_or_init(max_margin_stress, stress / planned_weight)
        if covered:
            continue
        should_cover = False
        cover_price = series.close[idx]
        cover_label = ""
        if take_profit_pct is not None and low is not None and avg_entry:
            target_price = avg_entry * (1.0 - float(take_profit_pct) / 100.0)
            if low <= target_price:
                should_cover = True
                cover_price = target_price
                cover_label = f"tp{int(take_profit_pct)}"
        elif time_cover_h is not None and idx - entry_idx >= int(time_cover_h):
            should_cover = True
            cover_label = f"time_cover_{int(time_cover_h)}h"
        if should_cover and cover_price:
            closed = close_active_fraction(
                series,
                active,
                exit_idx=idx,
                fill_price=float(cover_price),
                fraction=cover_fraction,
            )
            realized.extend(closed)
            exit_events.append(f"{cover_label}:cover{int(cover_fraction * 100)}")
            covered = True

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
        exit_events.append("runner_time_stop:all")
    if not realized:
        return None
    row = dict(base_row)
    row.update(realized_row_metrics(realized, planned_weight))
    row.update(
        {
            "research_family": "small_runner",
            "entry_setup": entry_setup,
            "plan_type": plan_type,
            "step_pct": step_pct,
            "max_legs": max_legs,
            "sizing_mode": sizing_mode,
            "take_profit_pct": take_profit_pct,
            "time_cover_h": time_cover_h,
            "cover_fraction": cover_fraction,
            "runner_fraction": round(1.0 - cover_fraction, 6),
            "max_hold_h": max_hold_h,
            "entry_ts": series.ts[entry_idx],
            "exit_ts": series.ts[exit_limit_idx],
            "legs_activated": activated_count,
            "mae_pct": max_mae,
            "max_margin_stress_reserved_pct": max_margin_stress,
            "max_adverse_from_first_pct": max_adverse_first,
            "cat300_first": 1 if (max_adverse_first or 0.0) >= 300.0 else 0,
            "cat700_first": 1 if (max_adverse_first or 0.0) >= 700.0 else 0,
            "cat1000_first": 1 if (max_adverse_first or 0.0) >= 1000.0 else 0,
            "stress100_reserved": 1 if (max_margin_stress or 0.0) >= 100.0 else 0,
            "stress200_reserved": 1 if (max_margin_stress or 0.0) >= 200.0 else 0,
            "exit_events": "|".join(exit_events),
            "covered_early": 1 if covered else 0,
        }
    )
    return row


def simulate_cooling_readd_family(
    research_series: ResearchSeries,
    event: PumpEvent,
    base_row: dict[str, Any],
    entry_setup: str,
    entry_idx: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    series = research_series.base
    first_price = series.close[entry_idx]
    if not first_price or first_price <= 0:
        return rows
    for config in COOLING_READD_CONFIGS:
        planned = planned_ladder(float(first_price), float(config["step_pct"]), int(config["max_legs"]), "equal")
        row = simulate_cooling_readd(research_series, base_row, entry_setup, entry_idx, planned, config)
        if row:
            rows.append(row)
    return rows


def simulate_cooling_readd(
    research_series: ResearchSeries,
    base_row: dict[str, Any],
    entry_setup: str,
    entry_idx: int,
    planned: list[dict[str, float]],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    series = research_series.base
    max_hold_h = int(config["max_hold_h"])
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + max_hold_h)
    if exit_limit_idx <= entry_idx:
        return None
    planned_weight = sum(item["weight"] for item in planned)
    active = [{"idx": entry_idx, "price": planned[0]["price"], "weight": planned[0]["weight"]}]
    activated_count = 1
    realized: list[tuple[float, float, float]] = []
    exit_events: list[str] = []
    cycles = 0
    readds = 0
    last_cover_price: float | None = None
    restore_weight = open_weight(active)
    oi_cooling_seen = False
    max_mae: float | None = None
    max_margin_stress: float | None = None
    max_adverse_first: float | None = None

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        high = series.high[idx]
        low = series.low[idx]
        if idx - entry_idx <= 168 and high is not None:
            while activated_count < len(planned) and high >= planned[activated_count]["price"]:
                leg = planned[activated_count]
                active.append({"idx": idx, "price": leg["price"], "weight": leg["weight"]})
                activated_count += 1
                restore_weight = max(restore_weight, open_weight(active))
        active = [item for item in active if float(item["weight"]) > 1e-9]
        if active:
            avg_entry = weighted_avg_entry(active)
            if high is not None and avg_entry:
                max_mae = max_or_init(max_mae, (high / avg_entry - 1.0) * 100.0)
                max_adverse_first = max_or_init(max_adverse_first, (high / planned[0]["price"] - 1.0) * 100.0)
                stress = sum(float(item["weight"]) * max(0.0, high / float(item["price"]) - 1.0) * 100.0 for item in active)
                max_margin_stress = max_or_init(max_margin_stress, stress / planned_weight)

            oi_from_entry = point_change_from_entry(series, entry_idx, idx)
            if oi_from_entry is not None and oi_from_entry <= float(config["oi_cool_min_pct"]):
                oi_cooling_seen = True

            if cycles < int(config["max_cycles"]) and avg_entry and low is not None:
                target_price = avg_entry * (1.0 - float(config["take_profit_pct"]) / 100.0)
                if low <= target_price:
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
                    exit_events.append(f"cover{int(float(config['cover_fraction']) * 100)}_tp{int(float(config['take_profit_pct']))}")

        if last_cover_price and oi_cooling_seen and open_weight(active) + 1e-9 < restore_weight and high is not None:
            readd_level = last_cover_price * (1.0 + float(config["readd_rally_pct"]) / 100.0)
            if high >= readd_level and cooling_readd_signal_ok(research_series, idx, config):
                add_weight = min(restore_weight - open_weight(active), planned_weight - open_weight(active))
                if add_weight > 1e-9:
                    active.append({"idx": idx, "price": readd_level, "weight": add_weight})
                    readds += 1
                    last_cover_price = None
                    oi_cooling_seen = False
                    exit_events.append(f"readd_after_cooling:{round(add_weight, 6)}")

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
        exit_events.append("runner_time_stop:all")
    if not realized:
        return None
    row = dict(base_row)
    row.update(realized_row_metrics(realized, planned_weight))
    row.update(
        {
            "research_family": "cooling_readd",
            "entry_setup": entry_setup,
            "take_profit_pct": config["take_profit_pct"],
            "cover_fraction": config["cover_fraction"],
            "readd_rally_pct": config["readd_rally_pct"],
            "oi_cool_min_pct": config["oi_cool_min_pct"],
            "oi_reexpand_min_pct": config["oi_reexpand_min_pct"],
            "volume_min_pct": config["volume_min_pct"],
            "max_cycles": config["max_cycles"],
            "step_pct": config["step_pct"],
            "max_legs": config["max_legs"],
            "entry_ts": series.ts[entry_idx],
            "exit_ts": series.ts[exit_limit_idx],
            "cycles": cycles,
            "readds": readds,
            "legs_activated": activated_count,
            "mae_pct": max_mae,
            "max_margin_stress_reserved_pct": max_margin_stress,
            "max_adverse_from_first_pct": max_adverse_first,
            "cat300_first": 1 if (max_adverse_first or 0.0) >= 300.0 else 0,
            "cat700_first": 1 if (max_adverse_first or 0.0) >= 700.0 else 0,
            "cat1000_first": 1 if (max_adverse_first or 0.0) >= 1000.0 else 0,
            "stress100_reserved": 1 if (max_margin_stress or 0.0) >= 100.0 else 0,
            "stress200_reserved": 1 if (max_margin_stress or 0.0) >= 200.0 else 0,
            "exit_events": "|".join(exit_events),
        }
    )
    return row


def point_change_from_entry(series: Series, entry_idx: int, idx: int) -> float | None:
    start = series.oi.get(series.ts[entry_idx])
    current = series.oi.get(series.ts[idx])
    if start in {None, 0} or current is None:
        return None
    return (current / start - 1.0) * 100.0


def cooling_readd_signal_ok(research_series: ResearchSeries, idx: int, config: dict[str, Any]) -> bool:
    oi_change = point_change_pct(research_series.base.oi, research_series.base.ts, idx, 24)
    volume_change = rolling_sum_change_pct(research_series.volume, idx, 24)
    if oi_change is None or oi_change < float(config["oi_reexpand_min_pct"]):
        return False
    if volume_change is None or volume_change < float(config["volume_min_pct"]):
        return False
    return True


def realized_row_metrics(realized: list[tuple[float, float, float]], planned_weight: float) -> dict[str, Any]:
    deployed_weight = sum(weight for _, weight, _ in realized)
    net_deployed = weighted_mean(realized)
    net_reserved = sum(net * weight for net, weight, _ in realized) / planned_weight if planned_weight > 0 else 0.0
    funding_deployed = weighted_mean_component(realized, 2)
    funding_reserved = sum(funding * weight for _, weight, funding in realized) / planned_weight if planned_weight > 0 else 0.0
    return {
        "planned_weight": planned_weight,
        "deployed_weight": deployed_weight,
        "net_deployed_pct": net_deployed,
        "net_reserved_pct": net_reserved,
        "funding_deployed_pct": funding_deployed,
        "funding_reserved_pct": funding_reserved,
        "win_deployed": 1 if net_deployed > 0 else 0,
        "win_reserved": 1 if net_reserved > 0 else 0,
    }


def base_research_row(series: Series, event: PumpEvent) -> dict[str, Any]:
    row = event_to_row(event)
    row.update(event_behavior_features(series, event))
    return row


def summarize_runner_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = (
        "entry_setup",
        "plan_type",
        "step_pct",
        "max_legs",
        "sizing_mode",
        "take_profit_pct",
        "time_cover_h",
        "cover_fraction",
        "runner_fraction",
        "max_hold_h",
    )
    return summarize(rows, keys)


def summarize_readd_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = (
        "entry_setup",
        "take_profit_pct",
        "cover_fraction",
        "readd_rally_pct",
        "oi_cool_min_pct",
        "oi_reexpand_min_pct",
        "volume_min_pct",
        "max_cycles",
        "step_pct",
        "max_legs",
    )
    return summarize(rows, keys)


def summarize(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(key) for key in keys), []).append(row)
    out = []
    for key_values, items in groups.items():
        prefix = {key: value for key, value in zip(keys, key_values)}
        prefix.update(aggregate_rows(items))
        out.append(prefix)
    return out


def aggregate_rows(items: list[dict[str, Any]]) -> dict[str, Any]:
    net_reserved = numeric_values(items, "net_reserved_pct")
    net_deployed = numeric_values(items, "net_deployed_pct")
    funding_reserved = numeric_values(items, "funding_reserved_pct")
    mae = numeric_values(items, "mae_pct")
    stress = numeric_values(items, "max_margin_stress_reserved_pct")
    first_adverse = numeric_values(items, "max_adverse_from_first_pct")
    cycles = numeric_values(items, "cycles")
    readds = numeric_values(items, "readds")
    def flag_count(key: str) -> int:
        return sum(1 for item in items if (to_float(item.get(key)) or 0.0) > 0.0)

    return {
        "n": len(items),
        "symbol_count": len({str(item.get("symbol") or "") for item in items}),
        "win_reserved_pct": pct(flag_count("win_reserved"), len(items)),
        "win_deployed_pct": pct(flag_count("win_deployed"), len(items)),
        "avg_net_reserved_pct": rounded_mean(net_reserved),
        "median_net_reserved_pct": rounded_median(net_reserved),
        "p25_net_reserved_pct": percentile(net_reserved, 25),
        "p75_net_reserved_pct": percentile(net_reserved, 75),
        "avg_net_deployed_pct": rounded_mean(net_deployed),
        "median_net_deployed_pct": rounded_median(net_deployed),
        "avg_funding_reserved_pct": rounded_mean(funding_reserved),
        "median_funding_reserved_pct": rounded_median(funding_reserved),
        "p90_mae_pct": percentile(mae, 90),
        "p95_mae_pct": percentile(mae, 95),
        "p99_mae_pct": percentile(mae, 99),
        "p90_margin_stress_reserved_pct": percentile(stress, 90),
        "p95_margin_stress_reserved_pct": percentile(stress, 95),
        "p99_margin_stress_reserved_pct": percentile(stress, 99),
        "p90_first_adverse_pct": percentile(first_adverse, 90),
        "p95_first_adverse_pct": percentile(first_adverse, 95),
        "cat300_first_pct": pct(flag_count("cat300_first"), len(items)),
        "cat700_first_pct": pct(flag_count("cat700_first"), len(items)),
        "cat1000_first_pct": pct(flag_count("cat1000_first"), len(items)),
        "stress100_reserved_pct": pct(flag_count("stress100_reserved"), len(items)),
        "stress200_reserved_pct": pct(flag_count("stress200_reserved"), len(items)),
        "avg_cycles": rounded_mean(cycles),
        "avg_readds": rounded_mean(readds),
    }


def rank_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    for row in rows:
        if (to_float(row.get("n")) or 0.0) < 100:
            continue
        item = dict(row)
        item["advanced_score"] = advanced_score(item)
        item["advanced_note"] = advanced_note(item)
        ranked.append(item)
    ranked.sort(key=lambda item: to_float(item.get("advanced_score")) or -9999.0, reverse=True)
    for idx, item in enumerate(ranked[:150], start=1):
        item["rank"] = idx
    return ranked[:150]


def advanced_score(row: dict[str, Any]) -> float:
    avg_reserved = to_float(row.get("avg_net_reserved_pct")) or 0.0
    median_reserved = to_float(row.get("median_net_reserved_pct")) or 0.0
    win_reserved = to_float(row.get("win_reserved_pct")) or 0.0
    p90_stress = to_float(row.get("p90_margin_stress_reserved_pct")) or 0.0
    p95_stress = to_float(row.get("p95_margin_stress_reserved_pct")) or 0.0
    cat300 = to_float(row.get("cat300_first_pct")) or 0.0
    cat700 = to_float(row.get("cat700_first_pct")) or 0.0
    stress200 = to_float(row.get("stress200_reserved_pct")) or 0.0
    return round(
        avg_reserved * 0.35
        + median_reserved * 0.25
        + win_reserved * 0.16
        - p90_stress * 0.07
        - p95_stress * 0.025
        - cat300 * 1.5
        - cat700 * 4.0
        - stress200 * 1.0,
        6,
    )


def advanced_note(row: dict[str, Any]) -> str:
    return (
        f"avg={fmt(row.get('avg_net_reserved_pct'))}, "
        f"med={fmt(row.get('median_net_reserved_pct'))}, "
        f"win={fmt(row.get('win_reserved_pct'))}, "
        f"p90stress={fmt(row.get('p90_margin_stress_reserved_pct'))}, "
        f"cat300={fmt(row.get('cat300_first_pct'))}"
    )


def worst_tails(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [
        row
        for row in rows
        if (to_float(row.get("max_margin_stress_reserved_pct")) or 0.0) >= 100.0
        or (to_float(row.get("max_adverse_from_first_pct")) or 0.0) >= 300.0
    ]
    candidates.sort(
        key=lambda row: (
            to_float(row.get("max_margin_stress_reserved_pct")) or 0.0,
            to_float(row.get("max_adverse_from_first_pct")) or 0.0,
        ),
        reverse=True,
    )
    keys = (
        "research_family",
        "symbol",
        "event_id",
        "entry_setup",
        "plan_type",
        "step_pct",
        "max_legs",
        "sizing_mode",
        "cover_fraction",
        "runner_fraction",
        "max_hold_h",
        "net_reserved_pct",
        "funding_reserved_pct",
        "mae_pct",
        "max_margin_stress_reserved_pct",
        "max_adverse_from_first_pct",
        "funding_regime",
        "oi_regime",
        "exit_events",
    )
    return [{key: row.get(key) for key in keys if key in row} for row in candidates[:500]]


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


def fmt(value: Any) -> str:
    number = to_float(value)
    return "n/a" if number is None else f"{number:.1f}"
