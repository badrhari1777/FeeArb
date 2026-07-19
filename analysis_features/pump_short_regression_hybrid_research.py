from __future__ import annotations

import csv
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from analysis_features.bybit_pump_short_grid_research import (
    base_research_row,
    resolve_entry_idx,
    simulate_ladder_rule,
)
from analysis_features.bybit_pump_short_outcomes import (
    PumpEvent,
    detect_pump_events,
    load_samples,
    sample_to_series,
    write_csv,
)
from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_COMPOUND_ACTIONS = (
    BASE_DIR / "data" / "research" / "pump_short_strategy_compound_report_1000" / "compound_actions.csv"
)
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_regression_hybrid_research"

CAPITAL_USD = 1_000.0
LEVERAGE = 3.0
BASE_ENTRY_SETUP = "pb20_oi50_lr_mid"
BASE_RULE_SLUG = "step50_legs4_equal_tp25_168"
TAIL_SYMBOLS = {
    "1000TOSHIUSDT",
    "1000TURBOUSDT",
    "HUSDT",
    "SIRENUSDT",
    "XCNUSDT",
}


@dataclass(frozen=True, slots=True)
class RuleConfig:
    slug: str
    step_pct: float
    max_legs: int
    sizing_mode: str
    exit_plan: dict[str, Any]


@dataclass(frozen=True, slots=True)
class GateConfig:
    slug: str
    description: str
    predicate: Callable[[dict[str, Any]], bool]


def run_regression_hybrid_research(
    *,
    input_path: Path = DEFAULT_INPUT,
    compound_actions_path: Path = DEFAULT_COMPOUND_ACTIONS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    max_defensive_rules: int = 80,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)

    rule_configs = build_rule_configs()
    base_rule = next(rule for rule in rule_configs if rule.slug == BASE_RULE_SLUG)
    events, outcome_map, symbols_seen = build_rule_outcomes(input_path, rule_configs)
    base_rows = [outcome_map[(event_id, BASE_RULE_SLUG)] for event_id in events if (event_id, BASE_RULE_SLUG) in outcome_map]

    compound_failures = load_compound_failures(compound_actions_path)
    symbol_profiles = build_symbol_profiles(base_rows)
    regression_rows = build_regression_report(base_rows)
    risk_gates = build_gate_configs()
    risk_gate_rows = build_risk_gate_summary(base_rows, risk_gates)
    worst_base_rows = worst_outcomes(base_rows, limit=150)
    rule_summary = build_rule_summary(outcome_map.values())
    defensive_rules = choose_defensive_rules(rule_summary, rule_configs, max_rules=max_defensive_rules)

    portfolio_summary: list[dict[str, Any]] = []
    for summary, _selected in evaluate_portfolios(
        events=events,
        outcome_map=outcome_map,
        base_rule=base_rule,
        defensive_rules=defensive_rules,
        gates=risk_gates,
    ):
        portfolio_summary.append(summary)

    portfolio_summary.sort(key=portfolio_sort_key)
    for idx, row in enumerate(portfolio_summary[:300], start=1):
        row["rank"] = idx

    practical_summary = build_practical_portfolio_summary(portfolio_summary)
    for idx, row in enumerate(practical_summary, start=1):
        row["practical_rank"] = idx

    selected_rows = rebuild_selected_rows(
        events=events,
        outcome_map=outcome_map,
        base_rule=base_rule,
        rule_configs=rule_configs,
        gates=risk_gates,
        summaries=portfolio_summary[:15] + practical_summary[:15],
    )

    write_csv(output_dir / "compound_failure_profile.csv", compound_failures)
    write_csv(output_dir / "base_worst_events.csv", worst_base_rows)
    write_csv(output_dir / "symbol_loss_profile.csv", symbol_profiles)
    write_csv(output_dir / "feature_regression.csv", regression_rows)
    write_csv(output_dir / "risk_gate_summary.csv", risk_gate_rows)
    write_csv(output_dir / "rule_summary.csv", rule_summary)
    write_csv(output_dir / "hybrid_portfolio_summary.csv", portfolio_summary)
    write_csv(output_dir / "practical_hybrid_portfolio_summary.csv", practical_summary)
    write_csv(output_dir / "top_hybrid_selected_trades.csv", selected_rows)

    report = render_report(
        compound_failures=compound_failures,
        worst_base_rows=worst_base_rows,
        symbol_profiles=symbol_profiles,
        regression_rows=regression_rows,
        risk_gate_rows=risk_gate_rows,
        portfolio_summary=portfolio_summary,
        practical_summary=practical_summary,
        rule_summary=rule_summary,
    )
    (output_dir / "regression_hybrid_report.md").write_text(report, encoding="utf-8")

    metadata = {
        "schema": "pump_short_regression_hybrid_research_v1",
        "input_path": str(input_path),
        "compound_actions_path": str(compound_actions_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events_with_base_rule": len(base_rows),
        "rule_configs": len(rule_configs),
        "simulated_rule_outcomes": len(outcome_map),
        "defensive_rules_tested": len(defensive_rules),
        "risk_gates": len(risk_gates),
        "portfolio_candidates": len(portfolio_summary),
        "capital_usd": CAPITAL_USD,
        "leverage": LEVERAGE,
        "base_entry_setup": BASE_ENTRY_SETUP,
        "base_rule": BASE_RULE_SLUG,
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def build_rule_configs() -> list[RuleConfig]:
    exit_plans = (
        {"name": "tp25_168", "max_hold_h": 168, "targets": ((25.0, 1.0),)},
        {"name": "tp25_336", "max_hold_h": 336, "targets": ((25.0, 1.0),)},
        {"name": "tp25_720", "max_hold_h": 720, "targets": ((25.0, 1.0),)},
        {"name": "tp35_336", "max_hold_h": 336, "targets": ((35.0, 1.0),)},
        {"name": "tp45_336", "max_hold_h": 336, "targets": ((45.0, 1.0),)},
        {"name": "tp25_50_half_336", "max_hold_h": 336, "targets": ((25.0, 0.5), (50.0, 1.0))},
        {"name": "tp35_70_half_720", "max_hold_h": 720, "targets": ((35.0, 0.5), (70.0, 1.0))},
    )
    configs: list[RuleConfig] = []
    for step_pct in (50.0, 75.0, 100.0, 150.0, 200.0):
        for max_legs in (3, 4, 5, 6):
            for sizing_mode in ("equal", "tapered"):
                for exit_plan in exit_plans:
                    slug = (
                        f"step{int(step_pct)}_legs{max_legs}_"
                        f"{sizing_mode}_{exit_plan['name']}"
                    )
                    configs.append(
                        RuleConfig(
                            slug=slug,
                            step_pct=step_pct,
                            max_legs=max_legs,
                            sizing_mode=sizing_mode,
                            exit_plan=exit_plan,
                        )
                    )
    return configs


def build_rule_outcomes(
    input_path: Path,
    rule_configs: list[RuleConfig],
) -> tuple[list[str], dict[tuple[str, str], dict[str, Any]], int]:
    events: list[str] = []
    outcome_map: dict[tuple[str, str], dict[str, Any]] = {}
    symbols_seen = 0
    for sample in load_samples(input_path):
        symbols_seen += 1
        series = sample_to_series(sample)
        for event in detect_pump_events(series):
            base_row = base_research_row(series, event)
            entry_idx = resolve_entry_idx(
                series,
                event,
                {
                    "name": BASE_ENTRY_SETUP,
                    "kind": "confirmed_pullback",
                    "pullback_pct": 20.0,
                    "oi_max_pct": 50.0,
                },
            )
            if entry_idx is None:
                continue
            if not live_like_event_ok(event):
                continue
            event_id = str(base_row["event_id"])
            events.append(event_id)
            for config in rule_configs:
                row = simulate_ladder_rule(
                    series,
                    event,
                    base_row,
                    entry_setup=BASE_ENTRY_SETUP,
                    entry_idx=entry_idx,
                    step_pct=config.step_pct,
                    max_legs=config.max_legs,
                    add_window_h=168,
                    sizing_mode=config.sizing_mode,
                    exit_plan={
                        "name": config.exit_plan["name"],
                        "max_hold_h": config.exit_plan["max_hold_h"],
                        "targets": config.exit_plan["targets"],
                    },
                )
                if row:
                    row["rule_slug"] = config.slug
                    outcome_map[(event_id, config.slug)] = row
    events = sorted(set(events), key=lambda event_id: event_sort_key(outcome_map, event_id))
    return events, outcome_map, symbols_seen


def live_like_event_ok(event: PumpEvent) -> bool:
    # Keep missing funding because some Bybit tail symbols have sparse historical funding.
    return event.funding_prev_24h_pct is None or event.funding_prev_24h_pct > -0.5


def event_sort_key(outcome_map: dict[tuple[str, str], dict[str, Any]], event_id: str) -> tuple[int, str, str]:
    row = outcome_map.get((event_id, BASE_RULE_SLUG))
    if not row:
        return (10**18, "", event_id)
    return (to_int(row.get("entry_ts")) or 10**18, str(row.get("symbol") or ""), event_id)


def load_compound_failures(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("strategy_slug") != "main_default_3coins_tp25":
                continue
            pnl = to_float(row.get("pnl_usd")) or 0.0
            if pnl >= 0:
                continue
            rows.append(
                {
                    "symbol": row.get("symbol"),
                    "entry_iso": row.get("entry_iso"),
                    "exit_iso": row.get("exit_iso"),
                    "pnl_usd": rounded(pnl),
                    "capital_before_entry_usd": rounded(to_float(row.get("capital_before_entry_usd"))),
                    "mae_pct": rounded(to_float(row.get("mae_pct"))),
                    "legs_filled": to_int(row.get("legs_filled")),
                    "manual_topup_beyond_alloc_usd": rounded(to_float(row.get("manual_topup_beyond_alloc_usd"))),
                    "exit_reason": row.get("exit_reason"),
                }
            )
    rows.sort(key=lambda item: to_float(item.get("pnl_usd")) or 0.0)
    return rows


def build_symbol_profiles(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get("symbol") or ""), []).append(row)
    out: list[dict[str, Any]] = []
    for symbol, items in groups.items():
        net = values(items, "net_reserved_pct")
        stress = values(items, "max_margin_stress_reserved_pct")
        adverse = values(items, "max_adverse_from_first_pct")
        out.append(
            {
                "symbol": symbol,
                "n": len(items),
                "loss_count": sum(1 for item in items if (to_float(item.get("net_reserved_pct")) or 0.0) < 0.0),
                "avg_net_reserved_pct": rounded_mean(net),
                "sum_net_reserved_pct": rounded(sum(net)),
                "worst_net_reserved_pct": rounded(min(net) if net else None),
                "max_margin_stress_reserved_pct": rounded(max(stress) if stress else None),
                "max_adverse_from_first_pct": rounded(max(adverse) if adverse else None),
                "tail_or_named": int(symbol in TAIL_SYMBOLS),
            }
        )
    out.sort(key=lambda item: (to_float(item.get("sum_net_reserved_pct")) or 0.0, -(to_int(item.get("n")) or 0)))
    return out


def build_regression_report(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features = (
        "pump_pct",
        "config_window_h",
        "config_threshold_pct",
        "age_days",
        "funding_prev_24h_pct",
        "funding_prev_72h_pct",
        "oi_change_4h_pct",
        "oi_change_24h_pct",
        "long_ratio",
    )
    targets = (
        "net_reserved_pct",
        "max_margin_stress_reserved_pct",
        "max_adverse_from_first_pct",
        "loss_label",
        "stress100_label",
    )
    prepared: list[dict[str, float]] = []
    for row in rows:
        item: dict[str, float] = {}
        for feature in features:
            value = to_float(row.get(feature))
            if value is not None and math.isfinite(value):
                item[feature] = value
        net = to_float(row.get("net_reserved_pct"))
        stress = to_float(row.get("max_margin_stress_reserved_pct"))
        adverse = to_float(row.get("max_adverse_from_first_pct"))
        if net is not None:
            item["net_reserved_pct"] = net
            item["loss_label"] = 1.0 if net < 0.0 else 0.0
        if stress is not None:
            item["max_margin_stress_reserved_pct"] = stress
            item["stress100_label"] = 1.0 if stress >= 100.0 else 0.0
        if adverse is not None:
            item["max_adverse_from_first_pct"] = adverse
        prepared.append(item)

    out: list[dict[str, Any]] = []
    for target in targets:
        model_rows = [item for item in prepared if target in item and all(feature in item for feature in features)]
        if len(model_rows) < 20:
            continue
        y = [item[target] for item in model_rows]
        xs = [[item[feature] for feature in features] for item in model_rows]
        model = standardized_ridge(xs, y, alpha=1.0)
        for feature, coefficient in zip(features, model["coefficients"]):
            out.append(
                {
                    "target": target,
                    "feature": feature,
                    "n": len(model_rows),
                    "standardized_coefficient": rounded(coefficient),
                    "abs_coefficient": rounded(abs(coefficient)),
                    "r2": rounded(model["r2"]),
                    "target_mean": rounded(statistics.mean(y)),
                    "note": regression_note(target, feature, coefficient),
                }
            )
    out.sort(key=lambda row: (str(row.get("target")), -(to_float(row.get("abs_coefficient")) or 0.0)))
    return out


def standardized_ridge(xs: list[list[float]], y: list[float], *, alpha: float) -> dict[str, Any]:
    if not xs:
        return {"coefficients": [], "r2": 0.0}
    cols = len(xs[0])
    means = [statistics.mean(row[idx] for row in xs) for idx in range(cols)]
    stdevs = [statistics.pstdev(row[idx] for row in xs) or 1.0 for idx in range(cols)]
    y_mean = statistics.mean(y)
    y_stdev = statistics.pstdev(y) or 1.0
    zxs = [[(row[idx] - means[idx]) / stdevs[idx] for idx in range(cols)] for row in xs]
    zy = [(value - y_mean) / y_stdev for value in y]
    xtx = [[0.0 for _ in range(cols)] for _ in range(cols)]
    xty = [0.0 for _ in range(cols)]
    for row, target in zip(zxs, zy):
        for i in range(cols):
            xty[i] += row[i] * target
            for j in range(cols):
                xtx[i][j] += row[i] * row[j]
    for i in range(cols):
        xtx[i][i] += alpha
    coefficients = solve_linear_system(xtx, xty)
    preds = [sum(coefficients[idx] * row[idx] for idx in range(cols)) for row in zxs]
    sse = sum((target - pred) ** 2 for target, pred in zip(zy, preds))
    sst = sum((target - statistics.mean(zy)) ** 2 for target in zy) or 1.0
    return {"coefficients": coefficients, "r2": max(-1.0, 1.0 - sse / sst)}


def solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    n = len(vector)
    aug = [row[:] + [vector[idx]] for idx, row in enumerate(matrix)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(aug[row][col]))
        if abs(aug[pivot][col]) < 1e-12:
            continue
        aug[col], aug[pivot] = aug[pivot], aug[col]
        div = aug[col][col]
        aug[col] = [value / div for value in aug[col]]
        for row in range(n):
            if row == col:
                continue
            factor = aug[row][col]
            if abs(factor) < 1e-12:
                continue
            aug[row] = [value - factor * base for value, base in zip(aug[row], aug[col])]
    return [aug[idx][-1] for idx in range(n)]


def regression_note(target: str, feature: str, coefficient: float) -> str:
    direction = "raises" if coefficient > 0 else "lowers"
    if target in {"loss_label", "stress100_label", "max_margin_stress_reserved_pct", "max_adverse_from_first_pct"}:
        return f"higher {feature} {direction} modeled tail/stress risk"
    return f"higher {feature} {direction} modeled net return"


def build_gate_configs() -> list[GateConfig]:
    return [
        GateConfig("always_defensive", "Use defensive rule on every trade", lambda row: True),
        GateConfig("pump_ge_80", "pump_pct >= 80", lambda row: f(row, "pump_pct") >= 80.0),
        GateConfig("pump_ge_100", "pump_pct >= 100", lambda row: f(row, "pump_pct") >= 100.0),
        GateConfig("pump_ge_150", "pump_pct >= 150", lambda row: f(row, "pump_pct") >= 150.0),
        GateConfig("pump_ge_250", "pump_pct >= 250", lambda row: f(row, "pump_pct") >= 250.0),
        GateConfig("oi24_ge_50", "oi_change_24h_pct >= 50", lambda row: f(row, "oi_change_24h_pct") >= 50.0),
        GateConfig("oi24_ge_100", "oi_change_24h_pct >= 100", lambda row: f(row, "oi_change_24h_pct") >= 100.0),
        GateConfig("oi4_ge_50", "oi_change_4h_pct >= 50", lambda row: f(row, "oi_change_4h_pct") >= 50.0),
        GateConfig("long_ge_60", "long_ratio >= 0.60", lambda row: f(row, "long_ratio") >= 0.60),
        GateConfig("long_ge_70", "long_ratio >= 0.70", lambda row: f(row, "long_ratio") >= 0.70),
        GateConfig(
            "young_pump_ge_80",
            "age_days < 30 and pump_pct >= 80",
            lambda row: f(row, "age_days", default=9999.0) < 30.0 and f(row, "pump_pct") >= 80.0,
        ),
        GateConfig(
            "pump100_or_oi24_100",
            "pump_pct >= 100 or oi_change_24h_pct >= 100",
            lambda row: f(row, "pump_pct") >= 100.0 or f(row, "oi_change_24h_pct") >= 100.0,
        ),
        GateConfig(
            "pump150_or_oi24_50",
            "pump_pct >= 150 or oi_change_24h_pct >= 50",
            lambda row: f(row, "pump_pct") >= 150.0 or f(row, "oi_change_24h_pct") >= 50.0,
        ),
        GateConfig(
            "super_aggression",
            "pump>=100 or oi24>=100 or oi4>=50 or long>=0.70",
            lambda row: (
                f(row, "pump_pct") >= 100.0
                or f(row, "oi_change_24h_pct") >= 100.0
                or f(row, "oi_change_4h_pct") >= 50.0
                or f(row, "long_ratio") >= 0.70
            ),
        ),
        GateConfig("named_tail_symbols", "SIREN/H/XCN/TOSHI/TURBO symbol override", lambda row: str(row.get("symbol") or "") in TAIL_SYMBOLS),
    ]


def build_risk_gate_summary(rows: list[dict[str, Any]], gates: list[GateConfig]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for gate in gates:
        risky = [row for row in rows if gate.predicate(row)]
        normal = [row for row in rows if not gate.predicate(row)]
        for bucket_name, bucket_rows in (("risk", risky), ("normal", normal)):
            out.append(gate_bucket_row(gate, bucket_name, bucket_rows, total=len(rows)))
    out.sort(key=lambda row: (str(row["gate_slug"]), str(row["bucket"])))
    return out


def gate_bucket_row(gate: GateConfig, bucket: str, rows: list[dict[str, Any]], *, total: int) -> dict[str, Any]:
    net = values(rows, "net_reserved_pct")
    stress = values(rows, "max_margin_stress_reserved_pct")
    adverse = values(rows, "max_adverse_from_first_pct")
    return {
        "gate_slug": gate.slug,
        "description": gate.description,
        "bucket": bucket,
        "n": len(rows),
        "coverage_pct": pct(len(rows), total),
        "loss_rate_pct": pct(sum(1 for row in rows if (to_float(row.get("net_reserved_pct")) or 0.0) < 0.0), len(rows)),
        "stress100_rate_pct": pct(sum(1 for row in rows if (to_float(row.get("max_margin_stress_reserved_pct")) or 0.0) >= 100.0), len(rows)),
        "avg_net_reserved_pct": rounded_mean(net),
        "median_net_reserved_pct": rounded_median(net),
        "worst_net_reserved_pct": rounded(min(net) if net else None),
        "p90_stress_pct": percentile(stress, 90),
        "p95_adverse_pct": percentile(adverse, 95),
    }


def build_rule_summary(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get("rule_slug") or ""), []).append(row)
    out: list[dict[str, Any]] = []
    for slug, items in groups.items():
        net = values(items, "net_reserved_pct")
        stress = values(items, "max_margin_stress_reserved_pct")
        adverse = values(items, "max_adverse_from_first_pct")
        row = {
            "rule_slug": slug,
            "n": len(items),
            "win_pct": pct(sum(1 for item in items if (to_float(item.get("net_reserved_pct")) or 0.0) > 0.0), len(items)),
            "avg_net_reserved_pct": rounded_mean(net),
            "median_net_reserved_pct": rounded_median(net),
            "worst_net_reserved_pct": rounded(min(net) if net else None),
            "p90_stress_pct": percentile(stress, 90),
            "p95_stress_pct": percentile(stress, 95),
            "p99_stress_pct": percentile(stress, 99),
            "p95_adverse_pct": percentile(adverse, 95),
            "stress100_pct": pct(sum(1 for item in items if (to_float(item.get("max_margin_stress_reserved_pct")) or 0.0) >= 100.0), len(items)),
            "cat300_pct": pct(sum(1 for item in items if (to_float(item.get("max_adverse_from_first_pct")) or 0.0) >= 300.0), len(items)),
        }
        row["score"] = rule_score(row)
        out.append(row)
    out.sort(key=lambda row: to_float(row.get("score")) or -9999.0, reverse=True)
    return out


def choose_defensive_rules(
    rule_summary: list[dict[str, Any]],
    rule_configs: list[RuleConfig],
    *,
    max_rules: int,
) -> list[RuleConfig]:
    by_slug = {rule.slug: rule for rule in rule_configs}
    chosen: list[RuleConfig] = []
    for row in rule_summary:
        slug = str(row.get("rule_slug") or "")
        if slug == BASE_RULE_SLUG:
            continue
        if slug in by_slug:
            chosen.append(by_slug[slug])
        if len(chosen) >= max_rules:
            break
    for slug in (
        "step100_legs4_equal_tp25_336",
        "step150_legs4_equal_tp25_336",
        "step200_legs4_equal_tp25_720",
        "step100_legs5_tapered_tp25_336",
        "step150_legs5_tapered_tp25_720",
        "step200_legs6_tapered_tp35_70_half_720",
    ):
        if slug in by_slug and all(rule.slug != slug for rule in chosen):
            chosen.append(by_slug[slug])
    return chosen


def evaluate_portfolios(
    *,
    events: list[str],
    outcome_map: dict[tuple[str, str], dict[str, Any]],
    base_rule: RuleConfig,
    defensive_rules: list[RuleConfig],
    gates: list[GateConfig],
) -> Iterable[tuple[dict[str, Any], list[dict[str, Any]]]]:
    rules = [base_rule] + defensive_rules
    sizing_caps: tuple[float | None, ...] = (None, 1_000.0, 2_000.0, 3_000.0, 5_000.0)
    for rule in rules:
        if rule.slug == base_rule.slug:
            gate_list = [GateConfig("base_only", "Base rule only", lambda row: False)]
        else:
            gate_list = gates
        for gate in gate_list:
            for slots in (1, 2, 3, 4, 5):
                for sizing_cap in sizing_caps:
                    rows = choose_hybrid_rows(
                        events=events,
                        outcome_map=outcome_map,
                        base_rule=base_rule,
                        defensive_rule=rule,
                        gate=gate,
                    )
                    if not rows:
                        continue
                    summary, selected = simulate_portfolio(
                        rows,
                        slots=slots,
                        sizing_cap_usd=sizing_cap,
                        gate=gate,
                        defensive_rule=rule,
                        base_rule=base_rule,
                    )
                    yield summary, selected


def build_practical_portfolio_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    practical: list[dict[str, Any]] = []
    for row in rows:
        cap = row.get("sizing_cap_usd")
        if cap == "uncapped":
            continue
        cap_value = to_float(cap)
        if cap_value is None or cap_value > 5_000.0:
            continue
        if (to_int(row.get("slots")) or 0) < 3:
            continue
        if (to_int(row.get("trades_taken")) or 0) < 60:
            continue
        if (to_int(row.get("skipped_insolvent")) or 0) > 0:
            continue
        if (to_float(row.get("max_concurrent_topup_usd")) or 0.0) > 3_000.0:
            continue
        practical.append(dict(row))
    practical.sort(
        key=lambda row: (
            -(to_float(row.get("final_capital_usd")) or -10**9),
            to_float(row.get("max_concurrent_topup_usd")) or 0.0,
            -(to_float(row.get("worst_trade_pnl_usd")) or 0.0),
        )
    )
    return practical


def rebuild_selected_rows(
    *,
    events: list[str],
    outcome_map: dict[tuple[str, str], dict[str, Any]],
    base_rule: RuleConfig,
    rule_configs: list[RuleConfig],
    gates: list[GateConfig],
    summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rule_by_slug = {rule.slug: rule for rule in rule_configs}
    gate_by_slug = {gate.slug: gate for gate in gates}
    gate_by_slug["base_only"] = GateConfig("base_only", "Base rule only", lambda row: False)
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for summary in summaries:
        slug = str(summary.get("candidate_slug") or "")
        if not slug or slug in seen:
            continue
        seen.add(slug)
        defensive_rule = rule_by_slug.get(str(summary.get("defensive_rule_slug") or ""))
        gate = gate_by_slug.get(str(summary.get("gate_slug") or ""))
        slots = to_int(summary.get("slots"))
        cap_text = summary.get("sizing_cap_usd")
        sizing_cap = None if cap_text == "uncapped" else to_float(cap_text)
        if not defensive_rule or not gate or not slots:
            continue
        rows = choose_hybrid_rows(
            events=events,
            outcome_map=outcome_map,
            base_rule=base_rule,
            defensive_rule=defensive_rule,
            gate=gate,
        )
        _, candidate_selected = simulate_portfolio(
            rows,
            slots=slots,
            sizing_cap_usd=sizing_cap,
            gate=gate,
            defensive_rule=defensive_rule,
            base_rule=base_rule,
        )
        selected.extend(candidate_selected)
    selected.sort(key=lambda row: (str(row.get("candidate_slug") or ""), to_int(row.get("entry_ts")) or 0))
    return selected


def choose_hybrid_rows(
    *,
    events: list[str],
    outcome_map: dict[tuple[str, str], dict[str, Any]],
    base_rule: RuleConfig,
    defensive_rule: RuleConfig,
    gate: GateConfig,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for event_id in events:
        base = outcome_map.get((event_id, base_rule.slug))
        if not base:
            continue
        use_defensive = defensive_rule.slug != base_rule.slug and gate.predicate(base)
        row = outcome_map.get((event_id, defensive_rule.slug if use_defensive else base_rule.slug))
        if not row:
            row = base
            use_defensive = False
        item = dict(row)
        item["hybrid_bucket"] = "defensive" if use_defensive else "base"
        item["gate_slug"] = gate.slug
        item["selected_rule_slug"] = defensive_rule.slug if use_defensive else base_rule.slug
        out.append(item)
    out.sort(key=lambda row: (to_int(row.get("entry_ts")) or 0, str(row.get("symbol") or ""), str(row.get("event_id") or "")))
    return out


def simulate_portfolio(
    rows: list[dict[str, Any]],
    *,
    slots: int,
    sizing_cap_usd: float | None,
    gate: GateConfig,
    defensive_rule: RuleConfig,
    base_rule: RuleConfig,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    current_capital = CAPITAL_USD
    active: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    skipped_slots = 0
    skipped_same_symbol = 0
    skipped_insolvent = 0

    def close_due(until_ts: int) -> None:
        nonlocal current_capital, active
        due = sorted([item for item in active if int(item["exit_ts"]) <= until_ts], key=lambda item: int(item["exit_ts"]))
        active = [item for item in active if int(item["exit_ts"]) > until_ts]
        for item in due:
            current_capital += to_float(item.get("pnl_usd")) or 0.0
            item["capital_after_exit_usd"] = rounded(current_capital)

    for row in rows:
        entry_ts = to_int(row.get("entry_ts")) or 0
        close_due(entry_ts)
        symbol = str(row.get("symbol") or "")
        if current_capital <= 0.0:
            skipped_insolvent += 1
            continue
        if any(item.get("symbol") == symbol for item in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= slots:
            skipped_slots += 1
            continue
        sizing_base = current_capital if sizing_cap_usd is None else min(current_capital, sizing_cap_usd)
        if sizing_base <= 0.0:
            skipped_insolvent += 1
            continue
        per_coin_capital = sizing_base / slots
        net_reserved = to_float(row.get("net_reserved_pct")) or 0.0
        stress = max(0.0, to_float(row.get("max_margin_stress_reserved_pct")) or 0.0)
        pnl_usd = per_coin_capital * LEVERAGE * net_reserved / 100.0
        peak_loss_usd = per_coin_capital * stress / 100.0
        manual_topup_usd = max(0.0, peak_loss_usd - per_coin_capital)
        action = {
            "candidate_slug": candidate_slug(gate, defensive_rule, slots, sizing_cap_usd, base_rule),
            "symbol": symbol,
            "event_id": row.get("event_id"),
            "entry_ts": entry_ts,
            "entry_iso": ms_to_iso(entry_ts),
            "exit_ts": to_int(row.get("exit_ts")) or entry_ts,
            "exit_iso": ms_to_iso(to_int(row.get("exit_ts")) or entry_ts),
            "selected_rule_slug": row.get("selected_rule_slug"),
            "hybrid_bucket": row.get("hybrid_bucket"),
            "slots": slots,
            "sizing_cap_usd": sizing_cap_usd if sizing_cap_usd is not None else "uncapped",
            "capital_before_entry_usd": rounded(current_capital),
            "sizing_base_usd": rounded(sizing_base),
            "per_coin_capital_usd": rounded(per_coin_capital),
            "pnl_usd": rounded(pnl_usd),
            "capital_after_exit_usd": None,
            "net_reserved_pct": rounded(net_reserved),
            "max_margin_stress_reserved_pct": rounded(stress),
            "max_adverse_from_first_pct": rounded(to_float(row.get("max_adverse_from_first_pct"))),
            "manual_topup_beyond_alloc_usd": rounded(manual_topup_usd),
            "pump_pct": rounded(to_float(row.get("pump_pct"))),
            "oi_change_4h_pct": rounded(to_float(row.get("oi_change_4h_pct"))),
            "oi_change_24h_pct": rounded(to_float(row.get("oi_change_24h_pct"))),
            "long_ratio": rounded(to_float(row.get("long_ratio"))),
            "age_days": rounded(to_float(row.get("age_days"))),
            "exit_reason": row.get("exit_reason"),
            "legs_activated": to_int(row.get("legs_activated")),
        }
        active.append(action)
        selected.append(action)

    close_due(10**18)
    net_pnl = current_capital - CAPITAL_USD
    wins = sum(1 for item in selected if (to_float(item.get("pnl_usd")) or 0.0) > 0.0)
    topups = [item for item in selected if (to_float(item.get("manual_topup_beyond_alloc_usd")) or 0.0) > 0.0]
    max_concurrent_topup = max_concurrent_value(selected, "manual_topup_beyond_alloc_usd")
    summary = {
        "candidate_slug": candidate_slug(gate, defensive_rule, slots, sizing_cap_usd, base_rule),
        "gate_slug": gate.slug,
        "gate_description": gate.description,
        "defensive_rule_slug": defensive_rule.slug,
        "slots": slots,
        "sizing_cap_usd": sizing_cap_usd if sizing_cap_usd is not None else "uncapped",
        "trades_taken": len(selected),
        "defensive_trades": sum(1 for item in selected if item.get("hybrid_bucket") == "defensive"),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_insolvent": skipped_insolvent,
        "final_capital_usd": rounded(current_capital),
        "net_pnl_usd": rounded(net_pnl),
        "roi_pct": rounded(net_pnl / CAPITAL_USD * 100.0),
        "win_rate_pct": pct(wins, len(selected)),
        "loss_trades": sum(1 for item in selected if (to_float(item.get("pnl_usd")) or 0.0) < 0.0),
        "worst_trade_pnl_usd": rounded(min((to_float(item.get("pnl_usd")) or 0.0 for item in selected), default=0.0)),
        "max_single_topup_usd": rounded(max((to_float(item.get("manual_topup_beyond_alloc_usd")) or 0.0 for item in selected), default=0.0)),
        "max_concurrent_topup_usd": rounded(max_concurrent_topup),
        "topup_events": len(topups),
        "stress100_trades": sum(1 for item in selected if (to_float(item.get("max_margin_stress_reserved_pct")) or 0.0) >= 100.0),
        "avg_net_reserved_pct": rounded_mean(values(selected, "net_reserved_pct")),
    }
    summary["risk_adjusted_roi_pct"] = rounded(
        (to_float(summary.get("net_pnl_usd")) or 0.0) / (CAPITAL_USD + max_concurrent_topup) * 100.0
    )
    return summary, selected


def candidate_slug(
    gate: GateConfig,
    defensive_rule: RuleConfig,
    slots: int,
    sizing_cap_usd: float | None,
    base_rule: RuleConfig,
) -> str:
    cap = "uncapped" if sizing_cap_usd is None else f"cap{int(sizing_cap_usd)}"
    if defensive_rule.slug == base_rule.slug:
        return f"base_slots{slots}_{cap}"
    return f"{gate.slug}__{defensive_rule.slug}__slots{slots}_{cap}"


def max_concurrent_value(actions: list[dict[str, Any]], key: str) -> float:
    events: list[tuple[int, float]] = []
    for row in actions:
        value = to_float(row.get(key)) or 0.0
        if value <= 0.0:
            continue
        events.append((to_int(row.get("entry_ts")) or 0, value))
        events.append((to_int(row.get("exit_ts")) or 0, -value))
    current = 0.0
    max_value = 0.0
    for _, delta in sorted(events, key=lambda item: (item[0], -item[1])):
        current += delta
        max_value = max(max_value, current)
    return max_value


def worst_outcomes(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: to_float(item.get("net_reserved_pct")) or 0.0)[:limit]:
        out.append(
            {
                "symbol": row.get("symbol"),
                "event_id": row.get("event_id"),
                "entry_iso": ms_to_iso(to_int(row.get("entry_ts")) or 0),
                "exit_iso": ms_to_iso(to_int(row.get("exit_ts")) or 0),
                "net_reserved_pct": rounded(to_float(row.get("net_reserved_pct"))),
                "max_margin_stress_reserved_pct": rounded(to_float(row.get("max_margin_stress_reserved_pct"))),
                "max_adverse_from_first_pct": rounded(to_float(row.get("max_adverse_from_first_pct"))),
                "pump_pct": rounded(to_float(row.get("pump_pct"))),
                "config_window_h": to_int(row.get("config_window_h")),
                "funding_prev_24h_pct": rounded(to_float(row.get("funding_prev_24h_pct"))),
                "oi_change_4h_pct": rounded(to_float(row.get("oi_change_4h_pct"))),
                "oi_change_24h_pct": rounded(to_float(row.get("oi_change_24h_pct"))),
                "long_ratio": rounded(to_float(row.get("long_ratio"))),
                "age_days": rounded(to_float(row.get("age_days"))),
                "legs_activated": to_int(row.get("legs_activated")),
                "exit_reason": row.get("exit_reason"),
            }
        )
    return out


def rule_score(row: dict[str, Any]) -> float:
    avg_net = to_float(row.get("avg_net_reserved_pct")) or 0.0
    median_net = to_float(row.get("median_net_reserved_pct")) or 0.0
    win = to_float(row.get("win_pct")) or 0.0
    p95_stress = to_float(row.get("p95_stress_pct")) or 0.0
    stress100 = to_float(row.get("stress100_pct")) or 0.0
    cat300 = to_float(row.get("cat300_pct")) or 0.0
    worst = to_float(row.get("worst_net_reserved_pct")) or 0.0
    return round(avg_net * 0.35 + median_net * 0.25 + win * 0.1 + worst * 0.1 - p95_stress * 0.07 - stress100 * 0.8 - cat300 * 1.5, 6)


def portfolio_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
    final = to_float(row.get("final_capital_usd")) or -10**9
    topup = to_float(row.get("max_concurrent_topup_usd")) or 0.0
    worst = to_float(row.get("worst_trade_pnl_usd")) or 0.0
    skipped = to_float(row.get("skipped_insolvent")) or 0.0
    return (-final, topup, -worst, skipped)


def render_report(
    *,
    compound_failures: list[dict[str, Any]],
    worst_base_rows: list[dict[str, Any]],
    symbol_profiles: list[dict[str, Any]],
    regression_rows: list[dict[str, Any]],
    risk_gate_rows: list[dict[str, Any]],
    portfolio_summary: list[dict[str, Any]],
    practical_summary: list[dict[str, Any]],
    rule_summary: list[dict[str, Any]],
) -> str:
    top_portfolios = portfolio_summary[:20]
    uncapped = [row for row in portfolio_summary if row.get("sizing_cap_usd") == "uncapped"][:10]
    capped = [row for row in portfolio_summary if row.get("sizing_cap_usd") != "uncapped"][:10]
    capped_display = [dict(row, capped_rank=idx) for idx, row in enumerate(capped, start=1)]
    lines = [
        "# Pump-short regression + hybrid research",
        "",
        f"Generated: {datetime.now(tz=timezone.utc).isoformat()}",
        "",
        "## What was tested",
        "",
        "- Base entry shape: Bybit extended data, confirmed 20% pullback, OI 24h <= 50%, long ratio 0.45..0.65, entry funding > -1% inside the existing entry resolver.",
        "- Base rule: 4 equal ladder legs, 50% spacing, TP25 or 168h time stop.",
        "- Defensive rules: 50/75/100/150/200% spacing, 3..6 legs, equal/tapered sizing, TP/hold 168/336/720h.",
        "- Hybrid rules: use base on normal pumps and switch to one defensive rule only when a risk gate fires.",
        "- Portfolio model: $1000 starting capital, 3x, dynamic compounding, slots 1..5, with and without sizing caps.",
        "",
        "## Last 3-coin strict-compounding failure",
        "",
        table(compound_failures[:12], ("symbol", "entry_iso", "pnl_usd", "mae_pct", "legs_filled", "manual_topup_beyond_alloc_usd")),
        "",
        "## Worst base-rule extended events",
        "",
        table(worst_base_rows[:15], ("symbol", "entry_iso", "net_reserved_pct", "max_margin_stress_reserved_pct", "pump_pct", "oi_change_24h_pct", "long_ratio")),
        "",
        "## Worst symbols under base rule",
        "",
        table(symbol_profiles[:15], ("symbol", "n", "loss_count", "sum_net_reserved_pct", "worst_net_reserved_pct", "max_margin_stress_reserved_pct")),
        "",
        "## Regression feature read",
        "",
        table(regression_rows[:18], ("target", "feature", "standardized_coefficient", "r2", "note")),
        "",
        "## Risk gates",
        "",
        table([row for row in risk_gate_rows if row["bucket"] == "risk"][:20], ("gate_slug", "n", "loss_rate_pct", "stress100_rate_pct", "avg_net_reserved_pct", "p90_stress_pct")),
        "",
        "## Best single rules by event-level score",
        "",
        table(rule_summary[:15], ("rule_slug", "win_pct", "avg_net_reserved_pct", "worst_net_reserved_pct", "p95_stress_pct", "score")),
        "",
        "## Best portfolios overall",
        "",
        table(top_portfolios, ("rank", "candidate_slug", "final_capital_usd", "roi_pct", "max_concurrent_topup_usd", "worst_trade_pnl_usd", "trades_taken", "defensive_trades")),
        "",
        "## Best practical portfolios",
        "",
        "Filter: slots >= 3, capped sizing <= $5000, trades >= 60, no insolvency, max concurrent top-up <= $3000.",
        "",
        table(practical_summary[:20], ("practical_rank", "candidate_slug", "final_capital_usd", "roi_pct", "max_concurrent_topup_usd", "worst_trade_pnl_usd", "trades_taken", "defensive_trades", "win_rate_pct")),
        "",
        "## Best uncapped portfolios",
        "",
        table(uncapped, ("rank", "candidate_slug", "final_capital_usd", "roi_pct", "max_concurrent_topup_usd", "worst_trade_pnl_usd")),
        "",
        "## Best capped portfolios",
        "",
        table(capped_display, ("capped_rank", "candidate_slug", "final_capital_usd", "roi_pct", "max_concurrent_topup_usd", "worst_trade_pnl_usd")),
        "",
        "## Human read",
        "",
        human_read(portfolio_summary, practical_summary, regression_rows, risk_gate_rows, symbol_profiles),
        "",
    ]
    return "\n".join(lines)


def human_read(
    portfolio_summary: list[dict[str, Any]],
    practical_summary: list[dict[str, Any]],
    regression_rows: list[dict[str, Any]],
    risk_gate_rows: list[dict[str, Any]],
    symbol_profiles: list[dict[str, Any]],
) -> str:
    best_uncapped = next((row for row in portfolio_summary if row.get("sizing_cap_usd") == "uncapped"), None)
    best_capped = next((row for row in portfolio_summary if row.get("sizing_cap_usd") != "uncapped"), None)
    best_practical = practical_summary[0] if practical_summary else None
    stress_rows = [
        row
        for row in regression_rows
        if row.get("target") == "max_margin_stress_reserved_pct"
    ][:4]
    gate_rows = [row for row in risk_gate_rows if row.get("bucket") == "risk"]
    gate_rows.sort(key=lambda row: to_float(row.get("stress100_rate_pct")) or 0.0, reverse=True)
    worst_symbols = ", ".join(str(row.get("symbol")) for row in symbol_profiles[:6])
    lines = [
        f"- Main tail symbols in the extended base rule: {worst_symbols}.",
    ]
    if stress_rows:
        features = ", ".join(f"{row['feature']} ({row['standardized_coefficient']})" for row in stress_rows)
        lines.append(f"- Regression-style stress predictors with largest standardized coefficients: {features}.")
    if gate_rows:
        row = gate_rows[0]
        lines.append(
            f"- Strongest simple risk bucket by stress rate: {row['gate_slug']} "
            f"with stress100 {row['stress100_rate_pct']}% over {row['n']} events."
        )
    if best_uncapped:
        lines.append(
            f"- Best uncapped portfolio ended at ${best_uncapped['final_capital_usd']} "
            f"but required max concurrent top-up ${best_uncapped['max_concurrent_topup_usd']}."
        )
    if best_capped:
        lines.append(
            f"- Best capped-sizing portfolio ended at ${best_capped['final_capital_usd']} "
            f"with max concurrent top-up ${best_capped['max_concurrent_topup_usd']}."
        )
    if best_practical:
        lines.append(
            f"- Best practical portfolio under the safety filter ended at ${best_practical['final_capital_usd']} "
            f"with {best_practical['trades_taken']} trades, worst trade ${best_practical['worst_trade_pnl_usd']}, "
            f"and max concurrent top-up ${best_practical['max_concurrent_topup_usd']}."
        )
    lines.append(
        "- Practical interpretation: the data supports a hybrid/wider-ladder mode for aggressive pumps, "
        "but the larger improvement comes from preventing unlimited compounding of per-step notional. "
        "Symbol-only gates are reported as diagnostics, not as a robust live default."
    )
    return "\n".join(lines)


def table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> str:
    if not rows:
        return "_No rows._"
    out = ["|" + "|".join(columns) + "|", "|" + "|".join("---" for _ in columns) + "|"]
    for row in rows:
        out.append("|" + "|".join(str(row.get(column, "")) for column in columns) + "|")
    return "\n".join(out)


def f(row: dict[str, Any], key: str, *, default: float = -10**9) -> float:
    value = to_float(row.get(key))
    return value if value is not None and math.isfinite(value) else default


def values(rows: Iterable[dict[str, Any]], key: str) -> list[float]:
    out = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None and math.isfinite(value):
            out.append(value)
    return out


def to_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def to_int(value: Any) -> int | None:
    parsed = to_float(value)
    return int(parsed) if parsed is not None else None


def rounded(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(float(value), digits)


def rounded_mean(items: list[float]) -> float | None:
    return rounded(statistics.mean(items)) if items else None


def rounded_median(items: list[float]) -> float | None:
    return rounded(statistics.median(items)) if items else None


def percentile(items: list[float], q: float) -> float | None:
    if not items:
        return None
    data = sorted(items)
    if len(data) == 1:
        return rounded(data[0])
    pos = (len(data) - 1) * q / 100.0
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return rounded(data[lo])
    weight = pos - lo
    return rounded(data[lo] * (1.0 - weight) + data[hi] * weight)


def pct(num: float, den: float) -> float | None:
    if den <= 0:
        return None
    return rounded(num / den * 100.0)


def ms_to_iso(ts_ms: int) -> str:
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
