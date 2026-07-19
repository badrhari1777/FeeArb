from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

from config import BASE_DIR
from analysis_features.bybit_pump_short_grid_research import (
    base_research_row,
    planned_ladder,
    simulate_static_ladder,
)
from analysis_features.bybit_pump_short_outcomes import (
    PumpEvent,
    Series,
    detect_pump_events,
    find_confirmed_pullback_entry,
    load_samples,
    pct,
    percentile,
    sample_to_series,
    to_float,
    write_csv,
)

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_tp_speed_research"

TP_VALUES: tuple[float, ...] = (15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 50.0, 60.0)
FUNDING_GATES: tuple[tuple[str, float], ...] = (
    ("all", -999.0),
    ("prev24_gt_-0.50", -0.50),
)
ADAPTIVE_RULES: tuple[str, ...] = (
    "fixed_25",
    "pump_pct_25_35_50",
    "velocity_25_35_50",
    "velocity_conservative_25_30_35",
    "hybrid_25_35_40_50",
)

ENTRY_SETUP = "pb20_oi50_lr_mid"
PULLBACK_PCT = 20.0
OI_MAX_PCT = 50.0
LONG_RATIO_MIN = 0.45
LONG_RATIO_MAX = 0.65
LADDER_STEP_PCT = 50.0
LADDER_MAX_LEGS = 4
LADDER_ADD_WINDOW_H = 168
SIZING_MODE = "equal"
MAX_HOLD_H = 168


def run_tp_speed_research(
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    symbols_seen = 0
    events_seen = 0
    fixed_rows: list[dict[str, Any]] = []
    adaptive_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []

    for sample in load_samples(input_path):
        symbols_seen += 1
        series = sample_to_series(sample)
        events = detect_pump_events(series)
        events_seen += len(events)
        for event in events:
            fixed, adaptive, skipped = simulate_event(series, event)
            fixed_rows.extend(fixed)
            adaptive_rows.extend(adaptive)
            skipped_rows.extend(skipped)

    tp_summary = summarize(fixed_rows, ("funding_gate", "tp_pct"))
    tp_by_speed_bucket = summarize(fixed_rows, ("funding_gate", "speed_bucket", "tp_pct"))
    tp_by_pump_bucket = summarize(fixed_rows, ("funding_gate", "pump_bucket", "tp_pct"))
    tp_by_window_bucket = summarize(fixed_rows, ("funding_gate", "window_bucket", "tp_pct"))
    adaptive_summary = summarize(adaptive_rows, ("funding_gate", "adaptive_rule", "selected_tp_pct"))
    adaptive_rule_summary = summarize(adaptive_rows, ("funding_gate", "adaptive_rule"))
    best_tp_by_speed = best_by_bucket(tp_by_speed_bucket, ("funding_gate", "speed_bucket"))
    best_tp_by_pump = best_by_bucket(tp_by_pump_bucket, ("funding_gate", "pump_bucket"))
    best_avg_tp_by_speed = best_by_bucket(tp_by_speed_bucket, ("funding_gate", "speed_bucket"), score_fn=avg_profit_score)
    best_avg_tp_by_pump = best_by_bucket(tp_by_pump_bucket, ("funding_gate", "pump_bucket"), score_fn=avg_profit_score)

    write_csv(output_dir / "tp_outcomes.csv", fixed_rows)
    write_csv(output_dir / "adaptive_tp_outcomes.csv", adaptive_rows)
    write_csv(output_dir / "skipped_entries.csv", skipped_rows)
    write_csv(output_dir / "tp_summary.csv", tp_summary)
    write_csv(output_dir / "tp_by_speed_bucket.csv", tp_by_speed_bucket)
    write_csv(output_dir / "tp_by_pump_bucket.csv", tp_by_pump_bucket)
    write_csv(output_dir / "tp_by_window_bucket.csv", tp_by_window_bucket)
    write_csv(output_dir / "adaptive_tp_summary.csv", adaptive_summary)
    write_csv(output_dir / "adaptive_tp_rule_summary.csv", adaptive_rule_summary)
    write_csv(output_dir / "best_tp_by_speed.csv", best_tp_by_speed)
    write_csv(output_dir / "best_tp_by_pump.csv", best_tp_by_pump)
    write_csv(output_dir / "best_avg_tp_by_speed.csv", best_avg_tp_by_speed)
    write_csv(output_dir / "best_avg_tp_by_pump.csv", best_avg_tp_by_pump)

    metadata = {
        "schema": "bybit_pump_short_tp_speed_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": events_seen,
        "fixed_outcomes": len(fixed_rows),
        "adaptive_outcomes": len(adaptive_rows),
        "skipped_entries": len(skipped_rows),
        "entry_setup": ENTRY_SETUP,
        "tp_values": list(TP_VALUES),
        "funding_gates": [name for name, _ in FUNDING_GATES],
        "adaptive_rules": list(ADAPTIVE_RULES),
        "ladder_step_pct": LADDER_STEP_PCT,
        "ladder_max_legs": LADDER_MAX_LEGS,
        "max_hold_h": MAX_HOLD_H,
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report(
        output_dir,
        metadata,
        tp_summary,
        best_tp_by_speed,
        best_tp_by_pump,
        best_avg_tp_by_speed,
        best_avg_tp_by_pump,
        adaptive_summary,
        adaptive_rule_summary,
    )
    return metadata


def simulate_event(series: Series, event: PumpEvent) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fixed_rows: list[dict[str, Any]] = []
    adaptive_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    base_row = base_research_row(series, event)
    feature_row = speed_feature_row(event)

    for funding_gate, funding_min_pct in FUNDING_GATES:
        entry_idx = find_confirmed_pullback_entry(
            series,
            event.trigger_idx,
            PULLBACK_PCT,
            max_wait_h=168,
            oi_max_pct=OI_MAX_PCT,
            long_ratio_min=LONG_RATIO_MIN,
            long_ratio_max=LONG_RATIO_MAX,
            funding_min_pct=funding_min_pct,
        )
        if entry_idx is None:
            skipped = dict(base_row)
            skipped.update(feature_row)
            skipped.update({"funding_gate": funding_gate, "reason": "no_confirmed_entry"})
            skipped_rows.append(skipped)
            continue

        for tp_pct in TP_VALUES:
            row = simulate_tp(series, event, base_row, entry_idx, tp_pct, f"tp{int(tp_pct)}_full_168")
            if row:
                row.update(feature_row)
                row.update({"funding_gate": funding_gate, "tp_pct": tp_pct})
                fixed_rows.append(row)

        for rule_name in ADAPTIVE_RULES:
            selected_tp = adaptive_tp_for_event(rule_name, event)
            row = simulate_tp(
                series,
                event,
                base_row,
                entry_idx,
                selected_tp,
                f"{rule_name}_tp{int(selected_tp)}_168",
            )
            if row:
                row.update(feature_row)
                row.update(
                    {
                        "funding_gate": funding_gate,
                        "adaptive_rule": rule_name,
                        "selected_tp_pct": selected_tp,
                    }
                )
                adaptive_rows.append(row)

    return fixed_rows, adaptive_rows, skipped_rows


def simulate_tp(
    series: Series,
    event: PumpEvent,
    base_row: dict[str, Any],
    entry_idx: int,
    tp_pct: float,
    exit_strategy: str,
) -> dict[str, Any] | None:
    first_price = series.close[entry_idx]
    if first_price is None or first_price <= 0:
        return None
    planned = planned_ladder(float(first_price), LADDER_STEP_PCT, LADDER_MAX_LEGS, SIZING_MODE)
    row = simulate_static_ladder(
        series,
        event,
        base_row,
        entry_setup=ENTRY_SETUP,
        entry_idx=entry_idx,
        planned=planned,
        step_pct=LADDER_STEP_PCT,
        max_legs=LADDER_MAX_LEGS,
        add_window_h=LADDER_ADD_WINDOW_H,
        sizing_mode=SIZING_MODE,
        exit_strategy=exit_strategy,
        max_hold_h=MAX_HOLD_H,
        targets=((tp_pct, 1.0),),
    )
    if row:
        row["take_profit_hit"] = 1 if str(row.get("exit_reason") or "").startswith("target_") else 0
    return row


def speed_feature_row(event: PumpEvent) -> dict[str, Any]:
    velocity_pct_per_h = event.pump_pct / event.config_window_h if event.config_window_h else None
    return {
        "pump_velocity_pct_per_h": round(velocity_pct_per_h, 6) if velocity_pct_per_h is not None else None,
        "pump_bucket": pump_bucket(event.pump_pct),
        "speed_bucket": speed_bucket(velocity_pct_per_h),
        "window_bucket": window_bucket(event.config_window_h),
    }


def adaptive_tp_for_event(rule_name: str, event: PumpEvent) -> float:
    velocity = event.pump_pct / event.config_window_h if event.config_window_h else 0.0
    pump_pct = event.pump_pct
    if rule_name == "fixed_25":
        return 25.0
    if rule_name == "pump_pct_25_35_50":
        if pump_pct >= 250.0:
            return 50.0
        if pump_pct >= 125.0:
            return 35.0
        return 25.0
    if rule_name == "velocity_25_35_50":
        if velocity >= 20.0:
            return 50.0
        if velocity >= 10.0:
            return 35.0
        return 25.0
    if rule_name == "velocity_conservative_25_30_35":
        if velocity >= 20.0:
            return 35.0
        if velocity >= 10.0:
            return 30.0
        return 25.0
    if rule_name == "hybrid_25_35_40_50":
        if pump_pct >= 250.0 or velocity >= 20.0:
            return 50.0
        if pump_pct >= 175.0 or velocity >= 15.0:
            return 40.0
        if pump_pct >= 100.0 or velocity >= 8.0:
            return 35.0
        return 25.0
    raise ValueError(f"unknown adaptive rule: {rule_name}")


def pump_bucket(pump_pct: float) -> str:
    if pump_pct < 100.0:
        return "050_100"
    if pump_pct < 150.0:
        return "100_150"
    if pump_pct < 250.0:
        return "150_250"
    if pump_pct < 400.0:
        return "250_400"
    return "400_plus"


def speed_bucket(velocity_pct_per_h: float | None) -> str:
    if velocity_pct_per_h is None:
        return "unknown"
    if velocity_pct_per_h < 5.0:
        return "lt5_pct_h"
    if velocity_pct_per_h < 10.0:
        return "05_10_pct_h"
    if velocity_pct_per_h < 20.0:
        return "10_20_pct_h"
    return "20_plus_pct_h"


def window_bucket(window_h: int) -> str:
    if window_h <= 8:
        return "fast_4_8h"
    if window_h <= 24:
        return "mid_12_24h"
    return "slow_72_168h"


def summarize(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(key) for key in keys), []).append(row)
    out: list[dict[str, Any]] = []
    for key_values, items in groups.items():
        row = {key: value for key, value in zip(keys, key_values)}
        row.update(aggregate_rows(items))
        out.append(row)
    out.sort(key=lambda item: tuple(str(item.get(key) or "") for key in keys))
    return out


def aggregate_rows(items: list[dict[str, Any]]) -> dict[str, Any]:
    net_reserved = numeric_values(items, "net_reserved_pct")
    net_deployed = numeric_values(items, "net_deployed_pct")
    mae = numeric_values(items, "mae_pct")
    first_adverse = numeric_values(items, "max_adverse_from_first_pct")
    margin_stress = numeric_values(items, "max_margin_stress_reserved_pct")
    hold_h = numeric_values(items, "time_in_trade_h")
    legs = numeric_values(items, "legs_activated")
    funding = numeric_values(items, "funding_deployed_pct")
    return {
        "n": len(items),
        "symbol_count": len({str(item.get("symbol") or "") for item in items}),
        "win_reserved_pct": pct(sum(1 for item in items if item.get("win_reserved")), len(items)),
        "take_profit_hit_pct": pct(sum(1 for item in items if item.get("take_profit_hit")), len(items)),
        "avg_net_reserved_pct": rounded_mean(net_reserved),
        "median_net_reserved_pct": rounded_median(net_reserved),
        "p25_net_reserved_pct": percentile(net_reserved, 25),
        "p75_net_reserved_pct": percentile(net_reserved, 75),
        "avg_net_deployed_pct": rounded_mean(net_deployed),
        "median_net_deployed_pct": rounded_median(net_deployed),
        "avg_funding_deployed_pct": rounded_mean(funding),
        "median_funding_deployed_pct": rounded_median(funding),
        "p90_mae_pct": percentile(mae, 90),
        "p95_mae_pct": percentile(mae, 95),
        "p90_first_adverse_pct": percentile(first_adverse, 90),
        "p95_first_adverse_pct": percentile(first_adverse, 95),
        "p90_margin_stress_reserved_pct": percentile(margin_stress, 90),
        "p95_margin_stress_reserved_pct": percentile(margin_stress, 95),
        "cat300_first_pct": pct(sum(1 for item in items if item.get("cat300_first")), len(items)),
        "stress100_reserved_pct": pct(sum(1 for item in items if item.get("stress100_reserved")), len(items)),
        "avg_hold_h": rounded_mean(hold_h),
        "avg_legs_activated": rounded_mean(legs),
    }


def best_by_bucket(
    rows: list[dict[str, Any]],
    bucket_keys: tuple[str, ...],
    *,
    score_fn: Any = None,
) -> list[dict[str, Any]]:
    if score_fn is None:
        score_fn = tp_score
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(key) for key in bucket_keys), []).append(row)
    out: list[dict[str, Any]] = []
    for items in groups.values():
        ranked = sorted(items, key=score_fn, reverse=True)
        for rank, row in enumerate(ranked[:3], start=1):
            item = dict(row)
            item["bucket_rank"] = rank
            item["tp_score"] = round(score_fn(row), 6)
            out.append(item)
    out.sort(key=lambda item: tuple(str(item.get(key) or "") for key in (*bucket_keys, "bucket_rank")))
    return out


def tp_score(row: dict[str, Any]) -> float:
    n = to_float(row.get("n")) or 0.0
    if n < 10.0:
        return -9999.0
    avg_net = to_float(row.get("avg_net_reserved_pct")) or 0.0
    median_net = to_float(row.get("median_net_reserved_pct")) or 0.0
    win = to_float(row.get("win_reserved_pct")) or 0.0
    p90_mae = to_float(row.get("p90_mae_pct")) or 0.0
    stress100 = to_float(row.get("stress100_reserved_pct")) or 0.0
    cat300 = to_float(row.get("cat300_first_pct")) or 0.0
    return avg_net * 0.35 + median_net * 0.25 + win * 0.15 - p90_mae * 0.05 - stress100 * 0.5 - cat300 * 1.5


def avg_profit_score(row: dict[str, Any]) -> float:
    n = to_float(row.get("n")) or 0.0
    if n < 10.0:
        return -9999.0
    return to_float(row.get("avg_net_reserved_pct")) or -9999.0


def numeric_values(items: Iterable[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for item in items:
        value = to_float(item.get(key))
        if value is not None:
            values.append(value)
    return values


def rounded_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def rounded_median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[midpoint], 6)
    return round((ordered[midpoint - 1] + ordered[midpoint]) / 2.0, 6)


def write_report(
    output_dir: Path,
    metadata: dict[str, Any],
    tp_summary: list[dict[str, Any]],
    best_tp_by_speed: list[dict[str, Any]],
    best_tp_by_pump: list[dict[str, Any]],
    best_avg_tp_by_speed: list[dict[str, Any]],
    best_avg_tp_by_pump: list[dict[str, Any]],
    adaptive_summary: list[dict[str, Any]],
    adaptive_rule_summary: list[dict[str, Any]],
) -> None:
    lines = [
        "# Bybit Pump Short TP Speed Research",
        "",
        "Scope: confirmed pullback entry `pb20_oi50_lr_mid`, 4 equal short legs, 50% adverse spacing, 168h max hold.",
        "",
        f"- Input: `{metadata['input_path']}`",
        f"- Symbols: {metadata['symbols_seen']}",
        f"- Pump events: {metadata['events']}",
        f"- Fixed TP outcomes: {metadata['fixed_outcomes']}",
        f"- Adaptive TP outcomes: {metadata['adaptive_outcomes']}",
        "",
        "## Fixed TP Summary",
        "",
    ]
    lines.extend(markdown_table(tp_summary, ("funding_gate", "tp_pct", "n", "win_reserved_pct", "take_profit_hit_pct", "avg_net_reserved_pct", "median_net_reserved_pct", "p90_mae_pct", "avg_hold_h")))
    lines.extend(["", "## Best TP By Speed Bucket", ""])
    lines.extend(markdown_table(best_tp_by_speed, ("funding_gate", "speed_bucket", "bucket_rank", "tp_pct", "n", "win_reserved_pct", "avg_net_reserved_pct", "median_net_reserved_pct", "p90_mae_pct", "tp_score")))
    lines.extend(["", "## Best TP By Pump Bucket", ""])
    lines.extend(markdown_table(best_tp_by_pump, ("funding_gate", "pump_bucket", "bucket_rank", "tp_pct", "n", "win_reserved_pct", "avg_net_reserved_pct", "median_net_reserved_pct", "p90_mae_pct", "tp_score")))
    lines.extend(["", "## Highest Average TP By Speed Bucket", ""])
    lines.extend(markdown_table(best_avg_tp_by_speed, ("funding_gate", "speed_bucket", "bucket_rank", "tp_pct", "n", "win_reserved_pct", "avg_net_reserved_pct", "median_net_reserved_pct", "p90_mae_pct", "tp_score")))
    lines.extend(["", "## Highest Average TP By Pump Bucket", ""])
    lines.extend(markdown_table(best_avg_tp_by_pump, ("funding_gate", "pump_bucket", "bucket_rank", "tp_pct", "n", "win_reserved_pct", "avg_net_reserved_pct", "median_net_reserved_pct", "p90_mae_pct", "tp_score")))
    lines.extend(["", "## Adaptive TP Rule Summary", ""])
    lines.extend(markdown_table(adaptive_rule_summary, ("funding_gate", "adaptive_rule", "n", "win_reserved_pct", "take_profit_hit_pct", "avg_net_reserved_pct", "median_net_reserved_pct", "p90_mae_pct", "avg_hold_h")))
    lines.extend(["", "## Adaptive TP Summary", ""])
    lines.extend(markdown_table(adaptive_summary, ("funding_gate", "adaptive_rule", "selected_tp_pct", "n", "win_reserved_pct", "take_profit_hit_pct", "avg_net_reserved_pct", "median_net_reserved_pct", "p90_mae_pct", "avg_hold_h")))
    (output_dir / "tp_speed_research_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def markdown_table(rows: list[dict[str, Any]], columns: tuple[str, ...], *, limit: int = 80) -> list[str]:
    shown = rows[:limit]
    if not shown:
        return ["_No rows._"]
    out = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in shown:
        out.append("| " + " | ".join(format_cell(row.get(column)) for column in columns) + " |")
    return out


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)
