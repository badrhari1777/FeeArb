from __future__ import annotations

import csv
import json
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from config import BASE_DIR

DEFAULT_GRID_OUTCOMES = (
    BASE_DIR
    / "data"
    / "research"
    / "bybit_pump_short_extended_grid_research"
    / "ladder_sweep_outcomes.csv"
)
DEFAULT_RUNNER_OUTCOMES = (
    BASE_DIR
    / "data"
    / "research"
    / "bybit_pump_short_advanced_research"
    / "small_runner_outcomes.csv"
)
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_tail25_report"
SINCE_UTC = datetime(2025, 1, 1, tzinfo=timezone.utc)
SINCE_MS = int(SINCE_UTC.timestamp() * 1000)


def run_tail25_report(
    *,
    grid_outcomes_path: Path = DEFAULT_GRID_OUTCOMES,
    runner_outcomes_path: Path = DEFAULT_RUNNER_OUTCOMES,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    since_ms: int = SINCE_MS,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()

    baseline_rows = [normalize_baseline_row(row) for row in read_rows(grid_outcomes_path) if baseline_filter(row, since_ms)]
    runner_rows = [normalize_runner_row(row) for row in read_rows(runner_outcomes_path) if runner_filter(row, since_ms)]
    all_rows = baseline_rows + runner_rows
    unique_baseline_rows = dedupe_trade_rows(baseline_rows)
    unique_runner_rows = dedupe_trade_rows(runner_rows)
    unique_all_rows = unique_baseline_rows + unique_runner_rows

    rule_summary = summarize(all_rows, ("variant",))
    unique_rule_summary = summarize(unique_all_rows, ("variant",))
    symbol_summary = summarize(all_rows, ("symbol", "variant"))
    unique_symbol_summary = summarize(unique_all_rows, ("symbol", "variant"))
    comparison_rows = build_event_comparison(baseline_rows, runner_rows)
    unique_comparison_rows = dedupe_comparison_rows(comparison_rows)
    symbol_comparison = build_symbol_comparison(comparison_rows)
    unique_symbol_comparison = build_symbol_comparison(unique_comparison_rows)
    worst_tail_rows = sorted(
        runner_rows,
        key=lambda row: (
            -(to_float(row.get("max_adverse_from_first_pct")) or 0.0),
            to_float(row.get("net_reserved_pct")) or 0.0,
        ),
    )[:100]

    write_csv(output_dir / "tail25_rule_summary.csv", rule_summary)
    write_csv(output_dir / "tail25_unique_rule_summary.csv", unique_rule_summary)
    write_csv(output_dir / "tail25_symbol_summary.csv", symbol_summary)
    write_csv(output_dir / "tail25_unique_symbol_summary.csv", unique_symbol_summary)
    write_csv(output_dir / "tail25_event_comparison.csv", comparison_rows)
    write_csv(output_dir / "tail25_unique_event_comparison.csv", unique_comparison_rows)
    write_csv(output_dir / "tail25_symbol_comparison.csv", symbol_comparison)
    write_csv(output_dir / "tail25_unique_symbol_comparison.csv", unique_symbol_comparison)
    write_csv(output_dir / "tail25_worst_tails.csv", worst_tail_rows)
    (output_dir / "tail25_report.md").write_text(
        render_markdown(unique_rule_summary, unique_symbol_comparison, worst_tail_rows, since_ms),
        encoding="utf-8",
    )

    ts_values = [int(to_float(row.get("trigger_ts")) or 0) for row in all_rows if to_float(row.get("trigger_ts"))]
    metadata = {
        "schema": "bybit_pump_short_tail25_report_v1",
        "since_utc": ms_to_iso(since_ms),
        "grid_outcomes_path": str(grid_outcomes_path),
        "runner_outcomes_path": str(runner_outcomes_path),
        "output_dir": str(output_dir),
        "baseline_rows": len(baseline_rows),
        "unique_baseline_rows": len(unique_baseline_rows),
        "runner_rows": len(runner_rows),
        "unique_runner_rows": len(unique_runner_rows),
        "comparison_rows": len(comparison_rows),
        "unique_comparison_rows": len(unique_comparison_rows),
        "symbols": len({str(row.get("symbol") or "") for row in all_rows}),
        "min_trigger_utc": ms_to_iso(min(ts_values)) if ts_values else None,
        "max_trigger_utc": ms_to_iso(max(ts_values)) if ts_values else None,
        "filters": {
            "entry_setup": "pb20_oi50_lr_mid",
            "funding_prev_24h_pct": "> -0.50",
            "step_pct": 50.0,
            "max_legs": 4,
            "sizing_mode": "equal",
            "baseline_exit": "tp25_full_168",
            "tail_runner": "cover 75% at TP25, keep 25% runner to 30d/90d",
        },
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "tail25_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def read_rows(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        yield from csv.DictReader(handle)


def baseline_filter(row: dict[str, Any], since_ms: int) -> bool:
    return (
        int(to_float(row.get("trigger_ts")) or 0) >= since_ms
        and row.get("entry_setup") == "pb20_oi50_lr_mid"
        and row.get("exit_strategy") == "tp25_full_168"
        and float(to_float(row.get("step_pct")) or 0.0) == 50.0
        and int(to_float(row.get("max_legs")) or 0) == 4
        and row.get("sizing_mode") == "equal"
        and funding_gate(row)
    )


def runner_filter(row: dict[str, Any], since_ms: int) -> bool:
    return (
        int(to_float(row.get("trigger_ts")) or 0) >= since_ms
        and row.get("entry_setup") == "pb20_oi50_lr_mid"
        and row.get("plan_type") == "tp_runner"
        and float(to_float(row.get("take_profit_pct")) or 0.0) == 25.0
        and float(to_float(row.get("cover_fraction")) or 0.0) == 0.75
        and float(to_float(row.get("runner_fraction")) or 0.0) == 0.25
        and int(to_float(row.get("max_hold_h")) or 0) in {720, 2160}
        and float(to_float(row.get("step_pct")) or 0.0) == 50.0
        and int(to_float(row.get("max_legs")) or 0) == 4
        and row.get("sizing_mode") == "equal"
        and funding_gate(row)
    )


def funding_gate(row: dict[str, Any]) -> bool:
    value = to_float(row.get("funding_prev_24h_pct"))
    return value is not None and value > -0.50


def normalize_baseline_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["variant"] = "baseline_full_tp25_168h"
    out["runner_hold_h"] = ""
    out["cover_fraction"] = 1.0
    out["runner_fraction"] = 0.0
    out["covered_early"] = 1 if "tp25" in str(row.get("exit_events") or "") else 0
    return out


def normalize_runner_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    hold_h = int(to_float(row.get("max_hold_h")) or 0)
    out["variant"] = f"tail25_runner_{int(hold_h / 24)}d"
    out["runner_hold_h"] = hold_h
    return out


def dedupe_trade_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("symbol"),
            row.get("variant"),
            row.get("entry_ts"),
            row.get("exit_ts"),
            row.get("exit_events"),
            row.get("net_reserved_pct"),
            row.get("max_margin_stress_reserved_pct"),
        )
        current = deduped.get(key)
        if current is None or (to_float(row.get("pump_pct")) or 0.0) > (to_float(current.get("pump_pct")) or 0.0):
            item = dict(row)
            item["deduped_from_trigger_rows"] = 1
            deduped[key] = item
        else:
            current["deduped_from_trigger_rows"] = int(current.get("deduped_from_trigger_rows") or 1) + 1
    return list(deduped.values())


def dedupe_comparison_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("symbol"),
            row.get("variant"),
            row.get("entry_utc"),
            row.get("runner_exit_events"),
            row.get("runner_net_reserved_pct"),
            row.get("runner_stress_reserved_pct"),
        )
        current = deduped.get(key)
        if current is None or (to_float(row.get("pump_pct")) or 0.0) > (to_float(current.get("pump_pct")) or 0.0):
            item = dict(row)
            item["deduped_from_trigger_rows"] = 1
            deduped[key] = item
        else:
            current["deduped_from_trigger_rows"] = int(current.get("deduped_from_trigger_rows") or 1) + 1
    return list(deduped.values())


def summarize(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(key) for key in keys)].append(row)
    out: list[dict[str, Any]] = []
    for key, items in groups.items():
        item = {name: value for name, value in zip(keys, key)}
        item.update(aggregate(items))
        out.append(item)
    return sorted(out, key=lambda row: (str(row.get(keys[0]) or ""), str(row.get(keys[-1]) or "")))


def aggregate(items: list[dict[str, Any]]) -> dict[str, Any]:
    net_reserved = numeric_values(items, "net_reserved_pct")
    net_deployed = numeric_values(items, "net_deployed_pct")
    funding_reserved = numeric_values_any(items, ("funding_reserved_pct", "funding_deployed_pct"))
    stress = numeric_values(items, "max_margin_stress_reserved_pct")
    first_adverse = numeric_values(items, "max_adverse_from_first_pct")
    return {
        "n": len(items),
        "symbol_count": len({str(item.get("symbol") or "") for item in items}),
        "event_count": len({str(item.get("event_id") or "") for item in items}),
        "first_trigger_utc": min_iso(items, "trigger_ts"),
        "last_trigger_utc": max_iso(items, "trigger_ts"),
        "win_reserved_pct": pct(sum(flag(item.get("win_reserved")) for item in items), len(items)),
        "win_deployed_pct": pct(sum(flag(item.get("win_deployed")) for item in items), len(items)),
        "avg_net_reserved_pct": rounded_mean(net_reserved),
        "median_net_reserved_pct": rounded_median(net_reserved),
        "p25_net_reserved_pct": percentile(net_reserved, 25),
        "p75_net_reserved_pct": percentile(net_reserved, 75),
        "avg_net_deployed_pct": rounded_mean(net_deployed),
        "median_net_deployed_pct": rounded_median(net_deployed),
        "avg_funding_reserved_pct": rounded_mean(funding_reserved),
        "median_funding_reserved_pct": rounded_median(funding_reserved),
        "p90_stress_reserved_pct": percentile(stress, 90),
        "p95_stress_reserved_pct": percentile(stress, 95),
        "p90_first_adverse_pct": percentile(first_adverse, 90),
        "p95_first_adverse_pct": percentile(first_adverse, 95),
        "cat300_first_pct": pct(sum(flag(item.get("cat300_first")) for item in items), len(items)),
        "cat700_first_pct": pct(sum(flag(item.get("cat700_first")) for item in items), len(items)),
        "cat1000_first_pct": pct(sum(flag(item.get("cat1000_first")) for item in items), len(items)),
        "stress100_reserved_pct": pct(sum(flag(item.get("stress100_reserved")) for item in items), len(items)),
        "stress200_reserved_pct": pct(sum(flag(item.get("stress200_reserved")) for item in items), len(items)),
        "covered_early_pct": pct(sum(flag(item.get("covered_early")) for item in items), len(items)),
    }


def build_event_comparison(
    baseline_rows: list[dict[str, Any]],
    runner_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_by_event = {str(row.get("event_id") or ""): row for row in baseline_rows}
    out: list[dict[str, Any]] = []
    for runner in runner_rows:
        event_id = str(runner.get("event_id") or "")
        base = baseline_by_event.get(event_id)
        if not base:
            continue
        row = {
            "event_id": event_id,
            "symbol": runner.get("symbol"),
            "trigger_utc": ms_to_iso(int(to_float(runner.get("trigger_ts")) or 0)),
            "entry_utc": ms_to_iso(int(to_float(runner.get("entry_ts")) or 0)),
            "pump_pct": runner.get("pump_pct"),
            "variant": runner.get("variant"),
            "covered_early": runner.get("covered_early"),
            "baseline_net_reserved_pct": base.get("net_reserved_pct"),
            "runner_net_reserved_pct": runner.get("net_reserved_pct"),
            "delta_net_reserved_pct": delta(runner.get("net_reserved_pct"), base.get("net_reserved_pct")),
            "baseline_funding_reserved_pct": base.get("funding_reserved_pct") or base.get("funding_deployed_pct"),
            "runner_funding_reserved_pct": runner.get("funding_reserved_pct"),
            "delta_funding_reserved_pct": delta(
                runner.get("funding_reserved_pct"),
                base.get("funding_reserved_pct") or base.get("funding_deployed_pct"),
            ),
            "baseline_stress_reserved_pct": base.get("max_margin_stress_reserved_pct"),
            "runner_stress_reserved_pct": runner.get("max_margin_stress_reserved_pct"),
            "delta_stress_reserved_pct": delta(
                runner.get("max_margin_stress_reserved_pct"),
                base.get("max_margin_stress_reserved_pct"),
            ),
            "runner_max_adverse_from_first_pct": runner.get("max_adverse_from_first_pct"),
            "runner_cat300_first": runner.get("cat300_first"),
            "runner_cat700_first": runner.get("cat700_first"),
            "runner_exit_events": runner.get("exit_events"),
        }
        out.append(row)
    return sorted(out, key=lambda item: (str(item.get("symbol") or ""), str(item.get("variant") or ""), str(item.get("trigger_utc") or "")))


def build_symbol_comparison(comparison_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in comparison_rows:
        groups[(str(row.get("symbol") or ""), str(row.get("variant") or ""))].append(row)
    out: list[dict[str, Any]] = []
    for (symbol, variant), items in groups.items():
        deltas = numeric_values(items, "delta_net_reserved_pct")
        stress_delta = numeric_values(items, "delta_stress_reserved_pct")
        item = {
            "symbol": symbol,
            "variant": variant,
            "n": len(items),
            "covered_early_pct": pct(sum(flag(item.get("covered_early")) for item in items), len(items)),
            "avg_delta_net_reserved_pct": rounded_mean(deltas),
            "median_delta_net_reserved_pct": rounded_median(deltas),
            "p25_delta_net_reserved_pct": percentile(deltas, 25),
            "p75_delta_net_reserved_pct": percentile(deltas, 75),
            "avg_delta_stress_reserved_pct": rounded_mean(stress_delta),
            "runner_cat300_first_pct": pct(sum(flag(item.get("runner_cat300_first")) for item in items), len(items)),
            "runner_cat700_first_pct": pct(sum(flag(item.get("runner_cat700_first")) for item in items), len(items)),
            "runner_cat1000_first_pct": pct(sum(flag(item.get("runner_cat1000_first")) for item in items), len(items)),
        }
        out.append(item)
    return sorted(out, key=lambda row: (str(row.get("variant")), -(to_float(row.get("avg_delta_net_reserved_pct")) or -9999.0)))


def render_markdown(
    rule_summary: list[dict[str, Any]],
    symbol_comparison: list[dict[str, Any]],
    worst_tail_rows: list[dict[str, Any]],
    since_ms: int,
) -> str:
    lines = [
        "# Pump-short 25% Tail Runner Report",
        "",
        f"Scope: Bybit pump events with `trigger_ts >= {ms_to_iso(since_ms)}`.",
        "",
        "Filters: `pb20_oi50_lr_mid`, funding prev24h `> -0.50%`, 4 equal ladder legs, 50% adverse step.",
        "",
        "Variants:",
        "- `baseline_full_tp25_168h`: full cover at TP25 or 168h time stop.",
        "- `tail25_runner_30d`: cover 75% at TP25, keep 25% runner to 30d.",
        "- `tail25_runner_90d`: cover 75% at TP25, keep 25% runner to 90d.",
        "",
        "## Rule Summary",
        "",
        "| variant | n | win | avg reserved | median reserved | funding | p90 stress | cat300 | covered early |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in sorted(rule_summary, key=lambda item: str(item.get("variant") or "")):
        lines.append(
            "| {variant} | {n} | {win_reserved_pct:.2f}% | {avg_net_reserved_pct:.2f}% | "
            "{median_net_reserved_pct:.2f}% | {avg_funding_reserved_pct:.2f}% | "
            "{p90_stress_reserved_pct:.2f}% | {cat300_first_pct:.2f}% | {covered_early_pct:.2f}% |".format(
                **format_row(row)
            )
        )
    lines.extend(["", "## Best Coin Deltas vs Baseline", ""])
    lines.extend(render_comparison_table(symbol_comparison, reverse=True))
    lines.extend(["", "## Worst Coin Deltas vs Baseline", ""])
    lines.extend(render_comparison_table(symbol_comparison, reverse=False))
    lines.extend(["", "## Worst Tail Events", ""])
    lines.append("| symbol | trigger | variant | net reserved | first adverse | stress | exit events |")
    lines.append("|---|---|---|---:|---:|---:|---|")
    for row in worst_tail_rows[:20]:
        lines.append(
            f"| {row.get('symbol')} | {ms_to_iso(int(to_float(row.get('trigger_ts')) or 0))} | "
            f"{row.get('variant')} | {num(row.get('net_reserved_pct')):.2f}% | "
            f"{num(row.get('max_adverse_from_first_pct')):.2f}% | "
            f"{num(row.get('max_margin_stress_reserved_pct')):.2f}% | {row.get('exit_events')} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_comparison_table(rows: list[dict[str, Any]], *, reverse: bool) -> list[str]:
    filtered = [row for row in rows if row.get("variant") == "tail25_runner_90d"]
    filtered.sort(key=lambda row: to_float(row.get("avg_delta_net_reserved_pct")) or 0.0, reverse=reverse)
    out = [
        "| symbol | n | covered early | avg delta | median delta | avg stress delta | cat300 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in filtered[:15]:
        out.append(
            "| {symbol} | {n} | {covered_early_pct:.2f}% | {avg_delta_net_reserved_pct:.2f}% | "
            "{median_delta_net_reserved_pct:.2f}% | {avg_delta_stress_reserved_pct:.2f}% | "
            "{runner_cat300_first_pct:.2f}% |".format(**format_row(row))
        )
    return out


def format_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key, value in list(out.items()):
        if key == "n":
            out[key] = int(to_float(value) or 0)
        elif isinstance(value, (int, float)):
            out[key] = value
        else:
            parsed = to_float(value)
            if parsed is not None:
                out[key] = parsed
    for key, value in list(out.items()):
        if value is None:
            out[key] = 0.0
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def numeric_values(items: Iterable[dict[str, Any]], key: str) -> list[float]:
    out = []
    for item in items:
        value = to_float(item.get(key))
        if value is not None:
            out.append(value)
    return out


def numeric_values_any(items: Iterable[dict[str, Any]], keys: tuple[str, ...]) -> list[float]:
    out = []
    for item in items:
        for key in keys:
            value = to_float(item.get(key))
            if value is not None:
                out.append(value)
                break
    return out


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def flag(value: Any) -> int:
    parsed = to_float(value)
    if parsed is not None:
        return 1 if parsed != 0 else 0
    return 1 if value is True else 0


def pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100.0, 6)


def rounded_mean(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 6) if values else None


def rounded_median(values: list[float]) -> float | None:
    return round(statistics.median(values), 6) if values else None


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    pos = (len(ordered) - 1) * (p / 100.0)
    lower = int(pos)
    upper = min(lower + 1, len(ordered) - 1)
    frac = pos - lower
    return round(ordered[lower] * (1.0 - frac) + ordered[upper] * frac, 6)


def delta(left: Any, right: Any) -> float | None:
    left_val = to_float(left)
    right_val = to_float(right)
    if left_val is None or right_val is None:
        return None
    return round(left_val - right_val, 6)


def min_iso(items: list[dict[str, Any]], key: str) -> str | None:
    values = [int(value) for value in numeric_values(items, key)]
    return ms_to_iso(min(values)) if values else None


def max_iso(items: list[dict[str, Any]], key: str) -> str | None:
    values = [int(value) for value in numeric_values(items, key)]
    return ms_to_iso(max(values)) if values else None


def ms_to_iso(ts_ms: int) -> str:
    if ts_ms <= 0:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def num(value: Any) -> float:
    return to_float(value) or 0.0


__all__ = ["run_tail25_report"]
