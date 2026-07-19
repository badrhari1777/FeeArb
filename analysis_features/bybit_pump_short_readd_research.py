from __future__ import annotations

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
    funding_sum_pct,
    load_samples,
    open_weight,
    pct,
    percentile,
    point_change_pct,
    to_float,
    weighted_avg_entry,
    weighted_mean,
    weighted_mean_component,
    write_csv,
)

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_readd_research"

ENTRY_SETUPS: tuple[dict[str, Any], ...] = (
    {"name": "pb20_oi50_lr_mid", "pullback_pct": 20.0, "oi_max_pct": 50.0},
    {"name": "pb20_oi0_lr_mid", "pullback_pct": 20.0, "oi_max_pct": 0.0},
)
INITIAL_LADDERS: tuple[dict[str, Any], ...] = (
    {"initial_step_pct": 50.0, "initial_max_legs": 4, "initial_sizing_mode": "equal"},
)
FUNDING_GATES: tuple[dict[str, Any], ...] = (
    {"funding_gate": "all"},
    {"funding_gate": "prev24_gt_-0.50", "funding_prev_24h_min": -0.50},
)
INITIAL_COVERS: tuple[dict[str, Any], ...] = (
    {"take_profit_pct": 25.0, "cover_fraction": 0.75},
    {"take_profit_pct": 25.0, "cover_fraction": 0.90},
    {"take_profit_pct": 25.0, "cover_fraction": 1.00},
)
TRIGGERS: tuple[dict[str, Any], ...] = (
    {
        "reentry_trigger": "rally_oi_volume",
        "reentry_rally_pct": 50.0,
        "oi_reexpand_min_pct": 25.0,
        "volume_min_pct": 50.0,
    },
    {
        "reentry_trigger": "rally_oi_volume",
        "reentry_rally_pct": 75.0,
        "oi_reexpand_min_pct": 50.0,
        "volume_min_pct": 100.0,
    },
    {
        "reentry_trigger": "rally_oi_volume",
        "reentry_rally_pct": 100.0,
        "oi_reexpand_min_pct": 50.0,
        "volume_min_pct": 100.0,
    },
    {
        "reentry_trigger": "rally_pullback_confirmed",
        "reentry_rally_pct": 50.0,
        "reentry_pullback_pct": 10.0,
        "reentry_oi_max_pct": 50.0,
    },
    {
        "reentry_trigger": "rally_pullback_confirmed",
        "reentry_rally_pct": 75.0,
        "reentry_pullback_pct": 20.0,
        "reentry_oi_max_pct": 50.0,
    },
)
REENTRY_SIZES: tuple[dict[str, Any], ...] = (
    {"reentry_size_mode": "restore_single", "reentry_fraction": 1.0},
    {"reentry_size_mode": "restore_ladder", "reentry_fraction": 1.0, "reentry_ladder_legs": 3, "reentry_ladder_step_pct": 50.0},
    {"reentry_size_mode": "fresh_full_single", "reentry_fraction": 1.0},
)
RISK_CONTROLS: tuple[dict[str, Any], ...] = (
    {"hard_stop_pct": ""},
    {"hard_stop_pct": 100.0},
)


def run_readd_research(input_path: Path = DEFAULT_INPUT, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    rows: list[dict[str, Any]] = []
    symbols_seen = 0
    events_seen = 0
    entry_points = 0
    configs_tested = 0
    configs = build_configs()

    for sample in load_samples(input_path):
        symbols_seen += 1
        research_series = sample_to_research_series(sample)
        series = research_series.base
        events = detect_pump_events(series)
        events_seen += len(events)
        for event in events:
            base_row = base_research_row(series, event)
            for entry_setup in ENTRY_SETUPS:
                entry_idx = resolve_entry_idx(series, event, entry_setup)
                if entry_idx is None:
                    continue
                entry_points += 1
                for config in configs:
                    if config["entry_setup"] != entry_setup["name"]:
                        continue
                    configs_tested += 1
                    row = simulate_readd_rule(research_series, event, base_row, entry_idx, config)
                    if row:
                        rows.append(row)

    summary = summarize_rows(rows)
    actual_rows = [row for row in rows if (to_float(row.get("readds")) or 0.0) > 0.0]
    actual_summary = summarize_rows(actual_rows)
    write_csv(output_dir / "readd_deep_outcomes.csv", rows)
    write_csv(output_dir / "readd_deep_summary.csv", summary)
    write_csv(output_dir / "readd_deep_actual_outcomes.csv", actual_rows)
    write_csv(output_dir / "readd_deep_actual_summary.csv", actual_summary)
    write_csv(output_dir / "best_readd_deep_rules.csv", rank_rules(summary))
    write_csv(output_dir / "best_readd_deep_actual_rules.csv", rank_rules(actual_summary, min_n=25))
    write_csv(output_dir / "readd_deep_worst_tails.csv", worst_tails(rows))

    metadata = {
        "schema": "bybit_pump_short_readd_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": events_seen,
        "entry_points": entry_points,
        "configs": len(configs),
        "configs_tested": configs_tested,
        "outcomes": len(rows),
        "actual_readd_outcomes": len(actual_rows),
        "summary_rules": len(summary),
        "actual_summary_rules": len(actual_summary),
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "readd_research_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def build_configs() -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []
    for entry in ENTRY_SETUPS:
        for ladder in INITIAL_LADDERS:
            for gate in FUNDING_GATES:
                for cover in INITIAL_COVERS:
                    for trigger in TRIGGERS:
                        for size in REENTRY_SIZES:
                            for risk in RISK_CONTROLS:
                                config = {
                                    "entry_setup": entry["name"],
                                    "max_hold_h": 2160,
                                    "initial_add_window_h": 168,
                                    "reentry_add_window_h": 168,
                                    "post_reentry_take_profit_pct": 25.0,
                                    "dynamic_funding_min_pct": -0.50,
                                    "long_ratio_min": 0.45,
                                    "long_ratio_max": 0.65,
                                }
                                config.update(ladder)
                                config.update(gate)
                                config.update(cover)
                                config.update(trigger)
                                config.update(size)
                                config.update(risk)
                                configs.append(config)
    return configs


def resolve_entry_idx(series: Series, event: PumpEvent, setup: dict[str, Any]) -> int | None:
    return find_confirmed_pullback_entry(
        series,
        event.trigger_idx,
        float(setup["pullback_pct"]),
        max_wait_h=168,
        oi_max_pct=float(setup["oi_max_pct"]),
        long_ratio_min=0.45,
        long_ratio_max=0.65,
        funding_min_pct=-1.0,
    )


def simulate_readd_rule(
    research_series: ResearchSeries,
    event: PumpEvent,
    base_row: dict[str, Any],
    entry_idx: int,
    config: dict[str, Any],
) -> dict[str, Any] | None:
    series = research_series.base
    if not funding_gate_passes(series, event, config):
        return None
    first_price = series.close[entry_idx]
    if not first_price or first_price <= 0:
        return None

    planned = planned_ladder(
        float(first_price),
        float(config["initial_step_pct"]),
        int(config["initial_max_legs"]),
        str(config["initial_sizing_mode"]),
    )
    planned_weight = sum(item["weight"] for item in planned)
    reserved_weight = planned_weight
    active: list[dict[str, float | int]] = [{"idx": entry_idx, "price": planned[0]["price"], "weight": planned[0]["weight"]}]
    pending_readd: list[dict[str, float | int]] = []
    activated_initial_legs = 1
    realized: list[tuple[float, float, float]] = []
    exit_events: list[str] = []
    initial_cover_done = False
    initial_cover_idx: int | None = None
    initial_cover_price: float | None = None
    readds = 0
    readd_legs_activated = 0
    rally_seen = False
    rally_high: float | None = None
    max_mae: float | None = None
    max_margin_stress: float | None = None
    max_adverse_first: float | None = None
    exit_reason = "time_stop"
    exit_idx = min(len(series.ts) - 1, entry_idx + int(config["max_hold_h"]))
    if exit_idx <= entry_idx:
        return None

    for idx in range(entry_idx + 1, exit_idx + 1):
        high = series.high[idx]
        low = series.low[idx]

        if idx - entry_idx <= int(config["initial_add_window_h"]) and high is not None:
            while activated_initial_legs < len(planned) and high >= float(planned[activated_initial_legs]["price"]):
                leg = planned[activated_initial_legs]
                active.append({"idx": idx, "price": leg["price"], "weight": leg["weight"]})
                activated_initial_legs += 1

        if pending_readd and high is not None:
            still_pending: list[dict[str, float | int]] = []
            for leg in pending_readd:
                if high >= float(leg["price"]):
                    active.append({"idx": idx, "price": leg["price"], "weight": leg["weight"]})
                    readd_legs_activated += 1
                    exit_events.append(f"readd_ladder_leg:{round(float(leg['weight']), 6)}")
                else:
                    still_pending.append(leg)
            pending_readd = still_pending

        active = [item for item in active if float(item["weight"]) > 1e-9]
        if active:
            avg_entry = weighted_avg_entry(active)
            if high is not None and avg_entry:
                max_mae = max_or_init(max_mae, (high / avg_entry - 1.0) * 100.0)
                max_adverse_first = max_or_init(max_adverse_first, (high / planned[0]["price"] - 1.0) * 100.0)
                stress = sum(float(item["weight"]) * max(0.0, high / float(item["price"]) - 1.0) * 100.0 for item in active)
                max_margin_stress = max_or_init(max_margin_stress, stress / reserved_weight)

            hard_stop = to_float(config.get("hard_stop_pct"))
            if hard_stop is not None and hard_stop > 0 and high is not None and avg_entry:
                stop_price = avg_entry * (1.0 + hard_stop / 100.0)
                if high >= stop_price:
                    realized.extend(close_active_fraction(series, active, exit_idx=idx, fill_price=stop_price, fraction=1.0))
                    active.clear()
                    pending_readd.clear()
                    exit_events.append(f"hard_stop:{int(hard_stop)}")
                    exit_reason = "hard_stop"
                    exit_idx = idx
                    break

        if active and not initial_cover_done:
            avg_entry = weighted_avg_entry(active)
            if avg_entry and low is not None:
                target_price = avg_entry * (1.0 - float(config["take_profit_pct"]) / 100.0)
                if low <= target_price:
                    realized.extend(
                        close_active_fraction(
                            series,
                            active,
                            exit_idx=idx,
                            fill_price=target_price,
                            fraction=float(config["cover_fraction"]),
                        )
                    )
                    initial_cover_done = True
                    initial_cover_idx = idx
                    initial_cover_price = target_price
                    exit_events.append(
                        f"initial_cover{int(float(config['cover_fraction']) * 100)}_tp{int(float(config['take_profit_pct']))}"
                    )

        if initial_cover_done and readds == 0 and not pending_readd and initial_cover_price is not None and high is not None:
            trigger_price = initial_cover_price * (1.0 + float(config["reentry_rally_pct"]) / 100.0)
            if high >= trigger_price:
                rally_seen = True
                rally_high = max(rally_high or trigger_price, high)
                if config["reentry_trigger"] == "rally_oi_volume" and reentry_signal_ok(research_series, idx, config):
                    reserved_weight = open_reentry(series, active, pending_readd, idx, trigger_price, planned_weight, reserved_weight, config)
                    readds += 1
                    readd_legs_activated += 1
                    exit_events.append(f"readd_rally:{round(trigger_price, 8)}")
            elif rally_seen and high is not None:
                rally_high = max(rally_high or high, high)

        if (
            initial_cover_done
            and readds == 0
            and not pending_readd
            and rally_seen
            and rally_high
            and config["reentry_trigger"] == "rally_pullback_confirmed"
            and low is not None
        ):
            pullback_price = rally_high * (1.0 - float(config["reentry_pullback_pct"]) / 100.0)
            if low <= pullback_price and confirmed_reentry_ok(series, idx, config):
                reserved_weight = open_reentry(series, active, pending_readd, idx, pullback_price, planned_weight, reserved_weight, config)
                readds += 1
                readd_legs_activated += 1
                exit_events.append(f"readd_pullback:{round(pullback_price, 8)}")

        if readds > 0 and active:
            avg_entry = weighted_avg_entry(active)
            if avg_entry and low is not None:
                second_target = avg_entry * (1.0 - float(config["post_reentry_take_profit_pct"]) / 100.0)
                if low <= second_target:
                    realized.extend(close_active_fraction(series, active, exit_idx=idx, fill_price=second_target, fraction=1.0))
                    active.clear()
                    pending_readd.clear()
                    exit_events.append(f"post_reentry_tp{int(float(config['post_reentry_take_profit_pct']))}:all")
                    exit_reason = "post_reentry_tp"
                    exit_idx = idx
                    break

    if active:
        close_price = series.close[exit_idx]
        if not close_price:
            return None
        realized.extend(close_active_fraction(series, active, exit_idx=exit_idx, fill_price=float(close_price), fraction=1.0))
        exit_events.append("time_stop:all")

    if not realized:
        return None

    row = dict(base_row)
    row.update(realized_row_metrics(realized, reserved_weight))
    row.update(
        {
            "research_family": "readd_deep",
            "entry_setup": config["entry_setup"],
            "funding_gate": config["funding_gate"],
            "initial_step_pct": config["initial_step_pct"],
            "initial_max_legs": config["initial_max_legs"],
            "initial_sizing_mode": config["initial_sizing_mode"],
            "take_profit_pct": config["take_profit_pct"],
            "cover_fraction": config["cover_fraction"],
            "reentry_trigger": config["reentry_trigger"],
            "reentry_rally_pct": config["reentry_rally_pct"],
            "reentry_pullback_pct": config.get("reentry_pullback_pct", ""),
            "oi_reexpand_min_pct": config.get("oi_reexpand_min_pct", ""),
            "volume_min_pct": config.get("volume_min_pct", ""),
            "reentry_oi_max_pct": config.get("reentry_oi_max_pct", ""),
            "reentry_size_mode": config["reentry_size_mode"],
            "reentry_fraction": config["reentry_fraction"],
            "reentry_ladder_legs": config.get("reentry_ladder_legs", ""),
            "reentry_ladder_step_pct": config.get("reentry_ladder_step_pct", ""),
            "hard_stop_pct": config.get("hard_stop_pct", ""),
            "entry_ts": series.ts[entry_idx],
            "initial_cover_ts": series.ts[initial_cover_idx] if initial_cover_idx is not None else "",
            "exit_ts": series.ts[exit_idx],
            "time_in_trade_h": exit_idx - entry_idx,
            "exit_reason": exit_reason,
            "initial_cover_done": 1 if initial_cover_done else 0,
            "readds": readds,
            "readd_legs_activated": readd_legs_activated,
            "initial_legs_activated": activated_initial_legs,
            "reserved_weight": reserved_weight,
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


def open_reentry(
    series: Series,
    active: list[dict[str, float | int]],
    pending_readd: list[dict[str, float | int]],
    idx: int,
    price: float,
    planned_weight: float,
    reserved_weight: float,
    config: dict[str, Any],
) -> float:
    fraction = float(config["reentry_fraction"])
    if config["reentry_size_mode"] == "fresh_full_single":
        add_weight = planned_weight * fraction
        active.append({"idx": idx, "price": price, "weight": add_weight})
        return max(reserved_weight, open_weight(active))

    restore_gap = max(0.0, planned_weight - open_weight(active))
    add_weight = restore_gap * fraction
    if add_weight <= 1e-9:
        return reserved_weight
    if config["reentry_size_mode"] == "restore_ladder":
        legs = int(config.get("reentry_ladder_legs") or 3)
        step = float(config.get("reentry_ladder_step_pct") or 50.0)
        per_leg = add_weight / legs
        active.append({"idx": idx, "price": price, "weight": per_leg})
        for leg_idx in range(1, legs):
            pending_readd.append({"idx": idx, "price": price * (1.0 + step / 100.0 * leg_idx), "weight": per_leg})
        return reserved_weight
    active.append({"idx": idx, "price": price, "weight": add_weight})
    return reserved_weight


def funding_gate_passes(series: Series, event: PumpEvent, config: dict[str, Any]) -> bool:
    threshold = config.get("funding_prev_24h_min")
    if threshold is None:
        return True
    value = event.funding_prev_24h_pct
    return value is None or value > float(threshold)


def reentry_signal_ok(research_series: ResearchSeries, idx: int, config: dict[str, Any]) -> bool:
    series = research_series.base
    oi_change = point_change_pct(series.oi, series.ts, idx, 24)
    volume_change = rolling_sum_change_pct(research_series.volume, idx, 24)
    if oi_change is None or oi_change < float(config.get("oi_reexpand_min_pct") or 0.0):
        return False
    if volume_change is None or volume_change < float(config.get("volume_min_pct") or 0.0):
        return False
    funding_prev_24h = funding_sum_pct(series.funding, series.ts[idx] - 24 * 3_600_000, series.ts[idx])
    return funding_prev_24h is None or funding_prev_24h > float(config["dynamic_funding_min_pct"])


def confirmed_reentry_ok(series: Series, idx: int, config: dict[str, Any]) -> bool:
    oi_change = point_change_pct(series.oi, series.ts, idx, 24)
    long_ratio = series.long_ratio.get(series.ts[idx])
    funding_prev_24h = funding_sum_pct(series.funding, series.ts[idx] - 24 * 3_600_000, series.ts[idx])
    if oi_change is None or oi_change > float(config.get("reentry_oi_max_pct") or 50.0):
        return False
    if long_ratio is None or not (float(config["long_ratio_min"]) <= long_ratio <= float(config["long_ratio_max"])):
        return False
    return funding_prev_24h is None or funding_prev_24h > float(config["dynamic_funding_min_pct"])


def realized_row_metrics(realized: list[tuple[float, float, float]], reserved_weight: float) -> dict[str, Any]:
    deployed_weight = sum(weight for _, weight, _ in realized)
    net_deployed = weighted_mean(realized)
    net_reserved = sum(net * weight for net, weight, _ in realized) / reserved_weight if reserved_weight > 0 else 0.0
    funding_deployed = weighted_mean_component(realized, 2)
    funding_reserved = sum(funding * weight for _, weight, funding in realized) / reserved_weight if reserved_weight > 0 else 0.0
    return {
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


def summarize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return summarize(
        rows,
        keys=(
            "entry_setup",
            "funding_gate",
            "initial_step_pct",
            "initial_max_legs",
            "initial_sizing_mode",
            "take_profit_pct",
            "cover_fraction",
            "reentry_trigger",
            "reentry_rally_pct",
            "reentry_pullback_pct",
            "oi_reexpand_min_pct",
            "volume_min_pct",
            "reentry_oi_max_pct",
            "reentry_size_mode",
            "reentry_fraction",
            "reentry_ladder_legs",
            "reentry_ladder_step_pct",
            "hard_stop_pct",
        ),
    )


def summarize(rows: list[dict[str, Any]], *, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(key) for key in keys), []).append(row)
    out: list[dict[str, Any]] = []
    for key_values, items in groups.items():
        row = {key: value for key, value in zip(keys, key_values)}
        row.update(aggregate_rows(items))
        out.append(row)
    return out


def aggregate_rows(items: list[dict[str, Any]]) -> dict[str, Any]:
    net_reserved = numeric_values(items, "net_reserved_pct")
    net_deployed = numeric_values(items, "net_deployed_pct")
    funding_reserved = numeric_values(items, "funding_reserved_pct")
    stress = numeric_values(items, "max_margin_stress_reserved_pct")
    first_adverse = numeric_values(items, "max_adverse_from_first_pct")
    readds = numeric_values(items, "readds")
    initial_covers = numeric_values(items, "initial_cover_done")
    time_in_trade = numeric_values(items, "time_in_trade_h")

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
        "initial_cover_rate_pct": pct(sum(1 for item in items if (to_float(item.get("initial_cover_done")) or 0.0) > 0.0), len(items)),
        "readd_rate_pct": pct(sum(1 for item in items if (to_float(item.get("readds")) or 0.0) > 0.0), len(items)),
        "avg_readds": rounded_mean(readds),
        "avg_initial_covers": rounded_mean(initial_covers),
        "avg_time_in_trade_h": rounded_mean(time_in_trade),
    }


def rank_rules(rows: list[dict[str, Any]], *, min_n: int = 100) -> list[dict[str, Any]]:
    ranked = []
    for row in rows:
        if (to_float(row.get("n")) or 0.0) < min_n:
            continue
        item = dict(row)
        item["readd_score"] = readd_score(item)
        item["readd_note"] = readd_note(item)
        ranked.append(item)
    ranked.sort(key=lambda item: to_float(item.get("readd_score")) or -9999.0, reverse=True)
    return ranked


def readd_score(row: dict[str, Any]) -> float:
    avg = to_float(row.get("avg_net_reserved_pct")) or 0.0
    med = to_float(row.get("median_net_reserved_pct")) or 0.0
    win = to_float(row.get("win_reserved_pct")) or 0.0
    p90_stress = to_float(row.get("p90_margin_stress_reserved_pct")) or 0.0
    p95_stress = to_float(row.get("p95_margin_stress_reserved_pct")) or 0.0
    cat300 = to_float(row.get("cat300_first_pct")) or 0.0
    stress100 = to_float(row.get("stress100_reserved_pct")) or 0.0
    readd_rate = to_float(row.get("readd_rate_pct")) or 0.0
    return round(avg * 1.2 + med * 0.35 + win * 0.08 + min(readd_rate, 50.0) * 0.03 - p90_stress * 0.12 - p95_stress * 0.05 - cat300 * 1.4 - stress100 * 0.7, 6)


def readd_note(row: dict[str, Any]) -> str:
    def fmt(key: str) -> str:
        value = to_float(row.get(key))
        return "" if value is None else f"{value:.1f}"

    return (
        f"avg={fmt('avg_net_reserved_pct')}, med={fmt('median_net_reserved_pct')}, "
        f"win={fmt('win_reserved_pct')}, readd={fmt('readd_rate_pct')}, "
        f"p90stress={fmt('p90_margin_stress_reserved_pct')}, cat300={fmt('cat300_first_pct')}"
    )


def worst_tails(rows: list[dict[str, Any]], limit: int = 300) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: to_float(row.get("max_margin_stress_reserved_pct")) or -1.0, reverse=True)[:limit]


def numeric_values(items: Iterable[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for item in items:
        value = to_float(item.get(key))
        if value is not None and math.isfinite(value):
            values.append(value)
    return values


def rounded_mean(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 6) if values else None


def rounded_median(values: list[float]) -> float | None:
    return round(statistics.median(values), 6) if values else None
