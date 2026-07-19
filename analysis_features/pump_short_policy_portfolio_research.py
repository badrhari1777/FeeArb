from __future__ import annotations

import csv
import html
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from analysis_features.pump_short_per_event_strategy_research import (
    BASE_RULE_SLUG,
    DEFAULT_OUTPUT_DIR as PER_EVENT_DIR,
    CAPITAL_USD,
    LEVERAGE,
    build_rule_configs,
)
from config import BASE_DIR

DEFAULT_INPUT_DIR = PER_EVENT_DIR
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_policy_portfolio_research"


@dataclass(frozen=True, slots=True)
class GateSpec:
    slug: str
    description: str
    predicate: Callable[[dict[str, Any]], bool]


@dataclass(frozen=True, slots=True)
class PolicySpec:
    slug: str
    description: str
    gate_slug: str
    rule_slug: str
    mode: str  # base, static, gate_override, skip_gate


@dataclass(frozen=True, slots=True)
class PortfolioConfig:
    capital_usd: float
    slots: int
    sizing_mode: str


def run_policy_portfolio_research(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    top_selected_limit: int = 80,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_cases = load_csv(input_dir / "per_event_summary.csv")
    gates = build_gates()
    unique_cases = build_unique_cases(raw_cases)
    rule_slugs = sorted({rule.slug for rule in build_rule_configs()})
    outcomes = load_outcomes(input_dir / "per_event_all_outcomes.csv", {case["case_id"] for case in unique_cases})
    policies = build_policies(rule_slugs, gates)
    configs = build_portfolio_configs()

    split_ts = train_test_split_ts(unique_cases)
    summary_rows: list[dict[str, Any]] = []
    best_heap: list[dict[str, Any]] = []
    top_by_test: list[dict[str, Any]] = []

    for policy in policies:
        for config in configs:
            result = simulate_policy_portfolio(
                cases=unique_cases,
                outcomes=outcomes,
                gates=gates,
                policy=policy,
                config=config,
                split_ts=split_ts,
            )
            summary_rows.append(result)

    summary_rows.sort(key=portfolio_rank_key)
    for idx, row in enumerate(summary_rows, start=1):
        row["rank"] = idx
    top_by_test = sorted(summary_rows, key=test_rank_key)[:500]
    strict_live_rows = strict_live_candidate_rows(summary_rows)
    capped_live_rows = capped_live_candidate_rows(summary_rows)
    write_csv(output_dir / "portfolio_policy_summary.csv", summary_rows)
    write_csv(output_dir / "top_test_policy_summary.csv", top_by_test)
    write_csv(output_dir / "strict_live_candidate_policy_summary.csv", strict_live_rows)
    write_csv(output_dir / "capped_live_candidate_policy_summary.csv", capped_live_rows)

    selected_rows: list[dict[str, Any]] = []
    for row in summary_rows[:top_selected_limit]:
        selected_rows.extend(
            simulate_policy_portfolio(
                cases=unique_cases,
                outcomes=outcomes,
                gates=gates,
                policy=policy_from_summary(row),
                config=config_from_summary(row),
                split_ts=split_ts,
                return_trades=True,
            )["selected_trades"]
        )
    write_csv(output_dir / "top_policy_selected_trades.csv", selected_rows)

    live_selected_rows: list[dict[str, Any]] = []
    for row in strict_live_rows[:40]:
        live_selected_rows.extend(
            simulate_policy_portfolio(
                cases=unique_cases,
                outcomes=outcomes,
                gates=gates,
                policy=policy_from_summary(row),
                config=config_from_summary(row),
                split_ts=split_ts,
                return_trades=True,
            )["selected_trades"]
        )
    write_csv(output_dir / "strict_live_candidate_selected_trades.csv", live_selected_rows)

    (output_dir / "index.html").write_text(
        render_html_report(
            cases=unique_cases,
            policies=policies,
            configs=configs,
            summary_rows=summary_rows,
            top_by_test=top_by_test,
            strict_live_rows=strict_live_rows,
            capped_live_rows=capped_live_rows,
            selected_rows=selected_rows,
            live_selected_rows=live_selected_rows,
            split_ts=split_ts,
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema": "pump_short_policy_portfolio_research_v1",
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "unique_cases": len(unique_cases),
        "rule_slugs": len(rule_slugs),
        "gates": len(gates),
        "policies": len(policies),
        "portfolio_configs": len(configs),
        "portfolio_rows": len(summary_rows),
        "strict_live_candidate_rows": len(strict_live_rows),
        "capped_live_candidate_rows": len(capped_live_rows),
        "selected_trade_rows": len(selected_rows),
        "strict_live_selected_trade_rows": len(live_selected_rows),
        "split_ts": split_ts,
        "split_iso": ms_to_iso(split_ts),
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_unique_cases(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("status") != "entered":
            continue
        entry_ts = to_int(row.get("entry_ts"))
        symbol = str(row.get("symbol") or "")
        if not entry_ts or not symbol:
            continue
        groups.setdefault((symbol, entry_ts), []).append(row)

    out: list[dict[str, Any]] = []
    for (symbol, entry_ts), items in groups.items():
        representative = max(
            items,
            key=lambda row: (
                to_float(row.get("config_threshold_pct")) or -1.0,
                to_float(row.get("pump_pct")) or -1.0,
                -(to_int(row.get("trigger_ts")) or 10**18),
            ),
        )
        case = dict(representative)
        case["duplicate_trigger_count"] = len(items)
        case["trigger_ts_min"] = min(to_int(row.get("trigger_ts")) or 10**18 for row in items)
        case["trigger_ts_max"] = max(to_int(row.get("trigger_ts")) or 0 for row in items)
        case["pump_pct"] = max_value(items, "pump_pct")
        case["config_threshold_pct"] = max_value(items, "config_threshold_pct")
        case["config_window_h"] = max_value(items, "config_window_h")
        case["age_days"] = min_value(items, "age_days")
        case["funding_prev_24h_pct"] = min_value(items, "funding_prev_24h_pct")
        case["funding_prev_72h_pct"] = min_value(items, "funding_prev_72h_pct")
        case["oi_change_4h_pct"] = max_value(items, "oi_change_4h_pct")
        case["oi_change_24h_pct"] = max_value(items, "oi_change_24h_pct")
        long_values = values(items, "long_ratio")
        case["long_ratio_min"] = min(long_values) if long_values else ""
        case["long_ratio_max"] = max(long_values) if long_values else ""
        case["entry_ts"] = entry_ts
        case["entry_iso"] = ms_to_iso(entry_ts)
        out.append(case)
    out.sort(key=lambda row: (to_int(row.get("entry_ts")) or 0, str(row.get("symbol") or "")))
    return out


def load_outcomes(path: Path, case_ids: set[str]) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            case_id = str(row.get("case_id") or "")
            if case_id not in case_ids:
                continue
            out[(case_id, str(row.get("rule_slug") or ""))] = row
    return out


def build_gates() -> dict[str, GateSpec]:
    specs = [
        GateSpec("always", "Always true", lambda row: True),
        GateSpec("pump_ge_80", "pump_pct >= 80", lambda row: f(row, "pump_pct") >= 80.0),
        GateSpec("pump_ge_100", "pump_pct >= 100", lambda row: f(row, "pump_pct") >= 100.0),
        GateSpec("pump_ge_150", "pump_pct >= 150", lambda row: f(row, "pump_pct") >= 150.0),
        GateSpec("pump_ge_250", "pump_pct >= 250", lambda row: f(row, "pump_pct") >= 250.0),
        GateSpec("pump_ge_400", "pump_pct >= 400", lambda row: f(row, "pump_pct") >= 400.0),
        GateSpec("oi24_ge_50", "oi_change_24h_pct >= 50", lambda row: f(row, "oi_change_24h_pct") >= 50.0),
        GateSpec("oi24_ge_100", "oi_change_24h_pct >= 100", lambda row: f(row, "oi_change_24h_pct") >= 100.0),
        GateSpec("oi24_ge_200", "oi_change_24h_pct >= 200", lambda row: f(row, "oi_change_24h_pct") >= 200.0),
        GateSpec("long_max_ge_60", "long_ratio max >= 0.60", lambda row: f(row, "long_ratio_max") >= 0.60),
        GateSpec("long_max_ge_70", "long_ratio max >= 0.70", lambda row: f(row, "long_ratio_max") >= 0.70),
        GateSpec("long_min_le_45", "long_ratio min <= 0.45", lambda row: f(row, "long_ratio_min", default=10**9) <= 0.45),
        GateSpec("long_min_le_35", "long_ratio min <= 0.35", lambda row: f(row, "long_ratio_min", default=10**9) <= 0.35),
        GateSpec("funding_le_m05", "funding prev24 <= -0.5", lambda row: f(row, "funding_prev_24h_pct", default=10**9) <= -0.5),
        GateSpec("funding_le_m2", "funding prev24 <= -2.0", lambda row: f(row, "funding_prev_24h_pct", default=10**9) <= -2.0),
        GateSpec("young_lt_30d", "age_days < 30", lambda row: f(row, "age_days", default=10**9) < 30.0),
        GateSpec("super_aggression", "pump>=150 or oi24>=100", lambda row: f(row, "pump_pct") >= 150.0 or f(row, "oi_change_24h_pct") >= 100.0),
        GateSpec(
            "squeeze_risk",
            "oi24>=50 and long_ratio_min<=0.45",
            lambda row: f(row, "oi_change_24h_pct") >= 50.0 and f(row, "long_ratio_min", default=10**9) <= 0.45,
        ),
        GateSpec(
            "mania_or_toxic",
            "pump>=250 or oi24>=200 or funding<=-2",
            lambda row: f(row, "pump_pct") >= 250.0 or f(row, "oi_change_24h_pct") >= 200.0 or f(row, "funding_prev_24h_pct", default=10**9) <= -2.0,
        ),
    ]
    return {spec.slug: spec for spec in specs}


def build_policies(rule_slugs: list[str], gates: dict[str, GateSpec]) -> list[PolicySpec]:
    policies = [
        PolicySpec("base", "Base strategy only", "always", BASE_RULE_SLUG, "base"),
    ]
    for rule in rule_slugs:
        policies.append(PolicySpec(f"static__{rule}", f"Always use {rule}", "always", rule, "static"))
    for gate_slug in gates:
        if gate_slug == "always":
            continue
        policies.append(PolicySpec(f"skip__{gate_slug}", f"Skip when {gate_slug}", gate_slug, "SKIP", "skip_gate"))
        for rule in rule_slugs:
            policies.append(
                PolicySpec(
                    f"{gate_slug}__{rule}",
                    f"If {gate_slug}, use {rule}; else base",
                    gate_slug,
                    rule,
                    "gate_override",
                )
            )
    return policies


def build_portfolio_configs() -> list[PortfolioConfig]:
    configs: list[PortfolioConfig] = []
    for capital in (1_000.0, 3_000.0):
        for slots in range(1, 6):
            for sizing_mode in ("dynamic", "fixed_initial", "cap_2x", "cap_5x"):
                configs.append(PortfolioConfig(capital_usd=capital, slots=slots, sizing_mode=sizing_mode))
    return configs


def train_test_split_ts(cases: list[dict[str, Any]]) -> int:
    values_ts = sorted(to_int(row.get("entry_ts")) or 0 for row in cases)
    if not values_ts:
        return 0
    return values_ts[int(len(values_ts) * 0.70)]


def simulate_policy_portfolio(
    *,
    cases: list[dict[str, Any]],
    outcomes: dict[tuple[str, str], dict[str, Any]],
    gates: dict[str, GateSpec],
    policy: PolicySpec,
    config: PortfolioConfig,
    split_ts: int,
    return_trades: bool = False,
) -> dict[str, Any]:
    active: list[dict[str, Any]] = []
    current_capital = config.capital_usd
    selected: list[dict[str, Any]] = []
    skipped_slots = 0
    skipped_same_symbol = 0
    skipped_policy = 0
    skipped_missing_rule = 0
    skipped_insolvent = 0

    def close_due(until_ts: int) -> None:
        nonlocal current_capital, active
        due = sorted([item for item in active if int(item["exit_ts"]) <= until_ts], key=lambda item: int(item["exit_ts"]))
        active = [item for item in active if int(item["exit_ts"]) > until_ts]
        for item in due:
            current_capital += to_float(item.get("pnl_usd")) or 0.0
            item["capital_after_exit_usd"] = rounded(current_capital)

    for case in cases:
        entry_ts = to_int(case.get("entry_ts")) or 0
        close_due(entry_ts)
        if current_capital <= 0:
            skipped_insolvent += 1
            continue
        rule_slug = choose_rule(case, policy, gates)
        if rule_slug == "SKIP":
            skipped_policy += 1
            continue
        outcome = outcomes.get((str(case.get("case_id")), rule_slug))
        if not outcome:
            skipped_missing_rule += 1
            continue
        symbol = str(case.get("symbol") or "")
        if any(item["symbol"] == symbol for item in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= config.slots:
            skipped_slots += 1
            continue
        sizing_base = sizing_base_usd(current_capital, config)
        if sizing_base <= 0:
            skipped_insolvent += 1
            continue
        per_coin_capital = sizing_base / config.slots
        net_pct = to_float(outcome.get("net_reserved_pct")) or 0.0
        stress_pct = max(0.0, to_float(outcome.get("max_margin_stress_reserved_pct")) or 0.0)
        pnl_usd = per_coin_capital * LEVERAGE * net_pct / 100.0
        peak_loss_usd = per_coin_capital * stress_pct / 100.0
        topup_usd = max(0.0, peak_loss_usd - per_coin_capital)
        action = {
            "policy_slug": policy.slug,
            "policy_mode": policy.mode,
            "gate_slug": policy.gate_slug,
            "rule_slug": rule_slug,
            "capital_usd": config.capital_usd,
            "slots": config.slots,
            "sizing_mode": config.sizing_mode,
            "symbol": symbol,
            "case_id": case.get("case_id"),
            "entry_ts": entry_ts,
            "entry_iso": ms_to_iso(entry_ts),
            "exit_ts": to_int(outcome.get("exit_ts")) or entry_ts,
            "exit_iso": ms_to_iso(to_int(outcome.get("exit_ts")) or entry_ts),
            "split": "test" if entry_ts >= split_ts else "train",
            "capital_before_entry_usd": rounded(current_capital),
            "sizing_base_usd": rounded(sizing_base),
            "per_coin_capital_usd": rounded(per_coin_capital),
            "net_pct": rounded(net_pct),
            "pnl_usd": rounded(pnl_usd),
            "stress_pct": rounded(stress_pct),
            "topup_usd": rounded(topup_usd),
            "pump_pct": rounded(to_float(case.get("pump_pct"))),
            "oi24_pct": rounded(to_float(case.get("oi_change_24h_pct"))),
            "long_min": rounded(to_float(case.get("long_ratio_min"))),
            "long_max": rounded(to_float(case.get("long_ratio_max"))),
            "funding_prev_24h_pct": rounded(to_float(case.get("funding_prev_24h_pct"))),
            "exit_reason": outcome.get("exit_reason"),
        }
        selected.append(action)
        active.append(action)

    close_due(10**18)
    summary = build_portfolio_summary(
        policy=policy,
        config=config,
        selected=selected,
        final_capital=current_capital,
        skipped_slots=skipped_slots,
        skipped_same_symbol=skipped_same_symbol,
        skipped_policy=skipped_policy,
        skipped_missing_rule=skipped_missing_rule,
        skipped_insolvent=skipped_insolvent,
        split_ts=split_ts,
    )
    if return_trades:
        summary["selected_trades"] = selected
    return summary


def build_portfolio_summary(
    *,
    policy: PolicySpec,
    config: PortfolioConfig,
    selected: list[dict[str, Any]],
    final_capital: float,
    skipped_slots: int,
    skipped_same_symbol: int,
    skipped_policy: int,
    skipped_missing_rule: int,
    skipped_insolvent: int,
    split_ts: int,
) -> dict[str, Any]:
    train = [row for row in selected if row["split"] == "train"]
    test = [row for row in selected if row["split"] == "test"]
    all_metrics = metrics_for(selected, config.capital_usd, final_capital)
    train_metrics = metrics_for(train, config.capital_usd, None)
    test_metrics = metrics_for(test, config.capital_usd, None)
    return {
        "policy_slug": policy.slug,
        "policy_mode": policy.mode,
        "gate_slug": policy.gate_slug,
        "rule_slug": policy.rule_slug,
        "description": policy.description,
        "capital_usd": config.capital_usd,
        "slots": config.slots,
        "sizing_mode": config.sizing_mode,
        "split_ts": split_ts,
        "split_iso": ms_to_iso(split_ts),
        "trades": len(selected),
        "train_trades": len(train),
        "test_trades": len(test),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_policy": skipped_policy,
        "skipped_missing_rule": skipped_missing_rule,
        "skipped_insolvent": skipped_insolvent,
        **all_metrics,
        **{f"train_{key}": value for key, value in train_metrics.items()},
        **{f"test_{key}": value for key, value in test_metrics.items()},
    }


def metrics_for(rows: list[dict[str, Any]], initial_capital: float, final_capital: float | None) -> dict[str, Any]:
    pnl_values = values(rows, "pnl_usd")
    net_pct_values = values(rows, "net_pct")
    topups = values(rows, "topup_usd")
    net_pnl = sum(pnl_values)
    effective_final = final_capital if final_capital is not None else initial_capital + net_pnl
    max_concurrent_topup = max_concurrent_value(rows, "topup_usd")
    return {
        "final_capital_usd": rounded(effective_final),
        "net_pnl_usd": rounded(net_pnl),
        "roi_pct": rounded((effective_final - initial_capital) / initial_capital * 100.0),
        "risk_adjusted_roi_pct": rounded(net_pnl / (initial_capital + max_concurrent_topup) * 100.0),
        "win_rate_pct": pct(sum(1 for row in rows if (to_float(row.get("pnl_usd")) or 0.0) > 0.0), len(rows)),
        "loss_trades": sum(1 for row in rows if (to_float(row.get("pnl_usd")) or 0.0) < 0.0),
        "worst_trade_pnl_usd": rounded(min(pnl_values) if pnl_values else 0.0),
        "avg_net_pct": rounded_mean(net_pct_values),
        "median_net_pct": rounded_median(net_pct_values),
        "max_single_topup_usd": rounded(max(topups) if topups else 0.0),
        "max_concurrent_topup_usd": rounded(max_concurrent_topup),
        "topup_events": sum(1 for row in rows if (to_float(row.get("topup_usd")) or 0.0) > 0.0),
    }


def choose_rule(case: dict[str, Any], policy: PolicySpec, gates: dict[str, GateSpec]) -> str:
    if policy.mode == "base":
        return BASE_RULE_SLUG
    if policy.mode == "static":
        return policy.rule_slug
    gate = gates[policy.gate_slug]
    if policy.mode == "skip_gate":
        return "SKIP" if gate.predicate(case) else BASE_RULE_SLUG
    if policy.mode == "gate_override":
        return policy.rule_slug if gate.predicate(case) else BASE_RULE_SLUG
    return BASE_RULE_SLUG


def sizing_base_usd(current_capital: float, config: PortfolioConfig) -> float:
    if config.sizing_mode == "dynamic":
        return current_capital
    if config.sizing_mode == "fixed_initial":
        return min(current_capital, config.capital_usd)
    if config.sizing_mode == "cap_2x":
        return min(current_capital, config.capital_usd * 2.0)
    if config.sizing_mode == "cap_5x":
        return min(current_capital, config.capital_usd * 5.0)
    return current_capital


def policy_from_summary(row: dict[str, Any]) -> PolicySpec:
    return PolicySpec(
        slug=str(row["policy_slug"]),
        description=str(row.get("description") or ""),
        gate_slug=str(row["gate_slug"]),
        rule_slug=str(row["rule_slug"]),
        mode=str(row["policy_mode"]),
    )


def config_from_summary(row: dict[str, Any]) -> PortfolioConfig:
    return PortfolioConfig(
        capital_usd=to_float(row.get("capital_usd")) or CAPITAL_USD,
        slots=to_int(row.get("slots")) or 1,
        sizing_mode=str(row.get("sizing_mode") or "dynamic"),
    )


def portfolio_rank_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
    # Prefer survival, test quality, controlled top-up, then overall result.
    insolvent = to_float(row.get("skipped_insolvent")) or 0.0
    test_roi = to_float(row.get("test_risk_adjusted_roi_pct")) or -10**9
    all_risk_roi = to_float(row.get("risk_adjusted_roi_pct")) or -10**9
    topup = to_float(row.get("max_concurrent_topup_usd")) or 0.0
    worst = to_float(row.get("worst_trade_pnl_usd")) or 0.0
    return (insolvent, -test_roi, -all_risk_roi, topup, -worst)


def test_rank_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (
        -(to_float(row.get("test_risk_adjusted_roi_pct")) or -10**9),
        to_float(row.get("test_max_concurrent_topup_usd")) or 0.0,
        -(to_float(row.get("test_worst_trade_pnl_usd")) or 0.0),
    )


def strict_live_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("sizing_mode") == "fixed_initial"
        and (to_int(row.get("slots")) or 0) >= 3
        and (to_int(row.get("trades")) or 0) >= 50
        and (to_int(row.get("test_trades")) or 0) >= 10
        and (to_int(row.get("skipped_insolvent")) or 0) == 0
        and (to_float(row.get("max_concurrent_topup_usd")) or 0.0) <= (to_float(row.get("capital_usd")) or 0.0)
        and (to_float(row.get("test_max_concurrent_topup_usd")) or 0.0) <= (to_float(row.get("capital_usd")) or 0.0)
        and (to_float(row.get("worst_trade_pnl_usd")) or 0.0) >= -(to_float(row.get("capital_usd")) or 0.0) * 0.5
    ]


def capped_live_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("sizing_mode") in {"fixed_initial", "cap_2x"}
        and (to_int(row.get("slots")) or 0) >= 3
        and (to_int(row.get("trades")) or 0) >= 50
        and (to_int(row.get("test_trades")) or 0) >= 10
        and (to_int(row.get("skipped_insolvent")) or 0) == 0
        and (to_float(row.get("max_concurrent_topup_usd")) or 0.0) <= (to_float(row.get("capital_usd")) or 0.0) * 2.0
        and (to_float(row.get("test_max_concurrent_topup_usd")) or 0.0) <= (to_float(row.get("capital_usd")) or 0.0) * 2.0
        and (to_float(row.get("worst_trade_pnl_usd")) or 0.0) >= -(to_float(row.get("capital_usd")) or 0.0)
    ]


def max_concurrent_value(actions: list[dict[str, Any]], key: str) -> float:
    events: list[tuple[int, float]] = []
    for row in actions:
        value = to_float(row.get(key)) or 0.0
        if value <= 0:
            continue
        events.append((to_int(row.get("entry_ts")) or 0, value))
        events.append((to_int(row.get("exit_ts")) or 0, -value))
    current = 0.0
    max_value = 0.0
    for _, delta in sorted(events, key=lambda item: (item[0], -item[1])):
        current += delta
        max_value = max(max_value, current)
    return max_value


def render_html_report(
    *,
    cases: list[dict[str, Any]],
    policies: list[PolicySpec],
    configs: list[PortfolioConfig],
    summary_rows: list[dict[str, Any]],
    top_by_test: list[dict[str, Any]],
    selected_rows: list[dict[str, Any]],
    split_ts: int,
    strict_live_rows: list[dict[str, Any]] | None = None,
    capped_live_rows: list[dict[str, Any]] | None = None,
    live_selected_rows: list[dict[str, Any]] | None = None,
) -> str:
    strict_live_rows = strict_live_rows if strict_live_rows is not None else strict_live_candidate_rows(summary_rows)
    capped_live_rows = capped_live_rows if capped_live_rows is not None else capped_live_candidate_rows(summary_rows)
    live_selected_rows = live_selected_rows or []
    safe_rows = [
        row
        for row in summary_rows
        if (to_float(row.get("max_concurrent_topup_usd")) or 0.0) <= (to_float(row.get("capital_usd")) or 0.0) * 2.0
        and (to_float(row.get("test_max_concurrent_topup_usd")) or 0.0) <= (to_float(row.get("capital_usd")) or 0.0) * 2.0
        and (to_int(row.get("trades")) or 0) >= 20
        and (to_int(row.get("test_trades")) or 0) >= 5
        and (to_int(row.get("skipped_insolvent")) or 0) == 0
    ][:100]
    mode_summary = summarize_by(summary_rows, "policy_mode")
    gate_summary = summarize_by(summary_rows, "gate_slug")
    content = f"""
    <section class="panel">
      <h1>Pump-short policy portfolio research</h1>
      <p>This report tests indicator-gated policies on unique live-like Bybit entries. It uses capital, slot limits, same-symbol blocking, top-up diagnostics, sizing caps, and a chronological train/test split.</p>
      <div class="metrics">
        <div><b>{len(cases)}</b><span>unique entries</span></div>
        <div><b>{len(policies)}</b><span>policies</span></div>
        <div><b>{len(configs)}</b><span>portfolio configs</span></div>
        <div><b>{len(summary_rows)}</b><span>portfolio runs</span></div>
        <div><b>{ms_to_iso(split_ts)}</b><span>test starts</span></div>
      </div>
    </section>
    <section class="panel"><h2>Human Read</h2>{human_read(summary_rows, safe_rows)}</section>
    <section class="panel"><h2>Best Risk-Adjusted Runs</h2>{html_table(summary_rows[:80], summary_columns())}</section>
    <section class="panel"><h2>Strict Live Candidates</h2><p>Filter: fixed initial sizing, 3-5 slots, >=50 trades, >=10 test trades, no insolvency, concurrent top-up <= starting capital, worst trade no worse than 50% of starting capital.</p>{html_table(strict_live_rows[:80], summary_columns())}</section>
    <section class="panel"><h2>Capped Growth Candidates</h2><p>Filter: fixed initial or cap_2x sizing, 3-5 slots, >=50 trades, >=10 test trades, no insolvency, concurrent top-up <= 2x starting capital, worst trade no worse than starting capital.</p>{html_table(capped_live_rows[:80], summary_columns())}</section>
    <section class="panel"><h2>Best Safer Runs</h2><p>Filter: no insolvency, >=20 trades, >=5 test trades, max concurrent top-up <= 2x starting capital.</p>{html_table(safe_rows[:80], summary_columns())}</section>
    <section class="panel"><h2>Best Test-Split Runs</h2>{html_table(top_by_test[:80], summary_columns())}</section>
    <section class="panel"><h2>Policy Mode Summary</h2>{html_table(mode_summary, ("group","runs","best_policy_slug","best_risk_adjusted_roi_pct","best_test_risk_adjusted_roi_pct","best_max_concurrent_topup_usd"))}</section>
    <section class="panel"><h2>Gate Summary</h2>{html_table(gate_summary, ("group","runs","best_policy_slug","best_risk_adjusted_roi_pct","best_test_risk_adjusted_roi_pct","best_max_concurrent_topup_usd"))}</section>
    <section class="panel"><h2>Strict Live Equity Curves</h2>{equity_charts(live_selected_rows, max_groups=6)}</section>
    <section class="panel"><h2>Strict Live Candidate Trades</h2>{html_table(live_selected_rows[:500], ("policy_slug","capital_usd","slots","sizing_mode","symbol","entry_iso","exit_iso","split","rule_slug","net_pct","pnl_usd","topup_usd","pump_pct","oi24_pct","long_min","long_max","funding_prev_24h_pct"))}</section>
    <section class="panel"><h2>Selected Trades For Top Runs</h2>{html_table(selected_rows[:500], ("policy_slug","capital_usd","slots","sizing_mode","symbol","entry_iso","exit_iso","split","rule_slug","net_pct","pnl_usd","topup_usd","pump_pct","oi24_pct","long_min","long_max","funding_prev_24h_pct"))}</section>
    """
    return page_shell("Pump-short policy portfolio research", content)


def summary_columns() -> tuple[str, ...]:
    return (
        "rank",
        "policy_slug",
        "capital_usd",
        "slots",
        "sizing_mode",
        "trades",
        "test_trades",
        "final_capital_usd",
        "risk_adjusted_roi_pct",
        "test_risk_adjusted_roi_pct",
        "max_concurrent_topup_usd",
        "test_max_concurrent_topup_usd",
        "worst_trade_pnl_usd",
        "test_worst_trade_pnl_usd",
        "win_rate_pct",
        "test_win_rate_pct",
        "skipped_policy",
        "skipped_slots",
    )


def human_read(summary_rows: list[dict[str, Any]], safe_rows: list[dict[str, Any]]) -> str:
    best = summary_rows[0] if summary_rows else {}
    safe = safe_rows[0] if safe_rows else {}
    base_rows = [row for row in summary_rows if row.get("policy_slug") == "base"]
    best_base = sorted(base_rows, key=portfolio_rank_key)[0] if base_rows else {}
    parts = []
    if best_base:
        parts.append(
            f"<p>Best base-only run: final ${esc(best_base.get('final_capital_usd'))}, "
            f"risk-adjusted ROI {esc(best_base.get('risk_adjusted_roi_pct'))}%, "
            f"test risk-adjusted ROI {esc(best_base.get('test_risk_adjusted_roi_pct'))}%.</p>"
        )
    if best:
        parts.append(
            f"<p>Best overall run by ranking: <b>{esc(best.get('policy_slug'))}</b>, "
            f"capital ${esc(best.get('capital_usd'))}, slots {esc(best.get('slots'))}, sizing {esc(best.get('sizing_mode'))}, "
            f"final ${esc(best.get('final_capital_usd'))}, test risk-adjusted ROI {esc(best.get('test_risk_adjusted_roi_pct'))}%, "
            f"max concurrent top-up ${esc(best.get('max_concurrent_topup_usd'))}.</p>"
        )
    if safe:
        parts.append(
            f"<p>Best safer run: <b>{esc(safe.get('policy_slug'))}</b>, "
            f"final ${esc(safe.get('final_capital_usd'))}, risk-adjusted ROI {esc(safe.get('risk_adjusted_roi_pct'))}%, "
            f"test risk-adjusted ROI {esc(safe.get('test_risk_adjusted_roi_pct'))}%, "
            f"worst trade ${esc(safe.get('worst_trade_pnl_usd'))}.</p>"
        )
    parts.append(
        "<p>Interpretation: prefer policies that survive the test split and stay inside the top-up filter. "
        "Pure dynamic compounding can rank high, but fixed/capped sizing is the better live candidate if the top-up tail widens.</p>"
    )
    return "\n".join(parts)


def summarize_by(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get(key) or ""), []).append(row)
    out: list[dict[str, Any]] = []
    for group, items in groups.items():
        best = sorted(items, key=portfolio_rank_key)[0]
        out.append(
            {
                "group": group,
                "runs": len(items),
                "best_policy_slug": best.get("policy_slug"),
                "best_risk_adjusted_roi_pct": best.get("risk_adjusted_roi_pct"),
                "best_test_risk_adjusted_roi_pct": best.get("test_risk_adjusted_roi_pct"),
                "best_max_concurrent_topup_usd": best.get("max_concurrent_topup_usd"),
            }
        )
    out.sort(key=lambda row: -(to_float(row.get("best_test_risk_adjusted_roi_pct")) or -10**9))
    return out


def html_table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    head = "".join(f"<th>{esc(column)}</th>" for column in columns)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{esc(row.get(column, ''))}</td>" for column in columns) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def equity_charts(rows: list[dict[str, Any]], *, max_groups: int) -> str:
    if not rows:
        return "<p>No rows.</p>"
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            str(row.get("policy_slug") or ""),
            str(row.get("capital_usd") or ""),
            str(row.get("slots") or ""),
            str(row.get("sizing_mode") or ""),
        )
        groups.setdefault(key, []).append(row)
    charts = []
    for key, items in list(groups.items())[:max_groups]:
        label = f"{key[0]} / ${key[1]} / slots {key[2]} / {key[3]}"
        charts.append(f"<div class=\"chart\"><h3>{esc(label)}</h3>{equity_svg(items)}</div>")
    return "".join(charts)


def equity_svg(rows: list[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: to_int(row.get("exit_ts")) or to_int(row.get("entry_ts")) or 0)
    initial = to_float(ordered[0].get("capital_usd")) or CAPITAL_USD
    equity = initial
    points: list[tuple[int, float]] = []
    if ordered:
        points.append((to_int(ordered[0].get("entry_ts")) or 0, equity))
    for row in ordered:
        equity += to_float(row.get("pnl_usd")) or 0.0
        points.append((to_int(row.get("exit_ts")) or to_int(row.get("entry_ts")) or 0, equity))
    if len(points) < 2:
        return "<p>No chart.</p>"
    min_ts = min(ts for ts, _ in points)
    max_ts = max(ts for ts, _ in points)
    min_eq = min(value for _, value in points)
    max_eq = max(value for _, value in points)
    width = 900
    height = 220
    pad = 30
    x_span = max(1, max_ts - min_ts)
    y_span = max(1.0, max_eq - min_eq)
    svg_points = []
    for ts, value in points:
        x = pad + (ts - min_ts) / x_span * (width - pad * 2)
        y = height - pad - (value - min_eq) / y_span * (height - pad * 2)
        svg_points.append(f"{x:.2f},{y:.2f}")
    zero_y = height - pad - (initial - min_eq) / y_span * (height - pad * 2)
    return (
        f"<svg viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"equity curve\">"
        f"<line x1=\"{pad}\" y1=\"{zero_y:.2f}\" x2=\"{width-pad}\" y2=\"{zero_y:.2f}\" stroke=\"#9aa8b5\" stroke-dasharray=\"4 4\"/>"
        f"<polyline points=\"{' '.join(svg_points)}\" fill=\"none\" stroke=\"#1f7a8c\" stroke-width=\"2.5\"/>"
        f"<text x=\"{pad}\" y=\"18\" fill=\"#34495e\">start ${initial:.2f} / end ${equity:.2f} / min ${min_eq:.2f} / max ${max_eq:.2f}</text>"
        "</svg>"
    )


def page_shell(title: str, content: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <style>
    body {{ margin: 0; background: #f4f6f8; color: #17202a; font: 14px/1.45 Arial, sans-serif; }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 22px; }}
    .panel {{ background: #fff; border: 1px solid #dfe5eb; border-radius: 8px; padding: 18px; margin: 0 0 16px; overflow-x: auto; }}
    h1 {{ margin: 0 0 10px; font-size: 26px; }}
    h2 {{ margin: 0 0 10px; font-size: 19px; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin-top: 14px; }}
    .metrics div {{ border: 1px solid #e2e8ee; border-radius: 6px; padding: 10px; background: #fbfcfd; }}
    .metrics b {{ display: block; font-size: 20px; }}
    .metrics span {{ color: #607080; font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 7px 8px; border-bottom: 1px solid #e5eaf0; text-align: left; vertical-align: top; }}
    th {{ background: #f7f9fb; color: #34495e; position: sticky; top: 0; }}
    .chart {{ margin: 0 0 18px; }}
    .chart h3 {{ margin: 0 0 6px; font-size: 13px; color: #34495e; }}
    svg {{ width: 100%; height: auto; border: 1px solid #e5eaf0; border-radius: 6px; background: #fbfcfd; }}
  </style>
</head>
<body><main class="wrap">{content}</main></body>
</html>"""


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
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


def max_value(rows: Iterable[dict[str, Any]], key: str) -> float | str:
    items = values(rows, key)
    return max(items) if items else ""


def min_value(rows: Iterable[dict[str, Any]], key: str) -> float | str:
    items = values(rows, key)
    return min(items) if items else ""


def values(rows: Iterable[dict[str, Any]], key: str) -> list[float]:
    out = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None and math.isfinite(value):
            out.append(value)
    return out


def f(row: dict[str, Any], key: str, *, default: float = -10**9) -> float:
    value = to_float(row.get(key))
    return value if value is not None else default


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


def pct(num: float, den: float) -> float | None:
    if den <= 0:
        return None
    return rounded(num / den * 100.0)


def ms_to_iso(ts_ms: Any) -> str:
    parsed = to_int(ts_ms)
    if not parsed:
        return ""
    return datetime.fromtimestamp(parsed / 1000.0, tz=timezone.utc).isoformat()


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))
