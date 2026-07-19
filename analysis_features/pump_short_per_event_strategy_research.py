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
from typing import Any, Iterable

from analysis_features.bybit_pump_short_grid_research import (
    base_research_row,
    resolve_entry_idx,
    simulate_ladder_rule,
)
from analysis_features.bybit_pump_short_outcomes import (
    Series,
    detect_pump_events,
    load_samples,
    sample_to_series,
    write_csv,
)
from analysis_features.pump_short_regression_hybrid_research import standardized_ridge
from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_per_event_strategy_research"

CAPITAL_USD = 1_000.0
LEVERAGE = 3.0
ENTRY_SETUP = {
    "name": "pb20_oi50_lr_mid",
    "kind": "confirmed_pullback",
    "pullback_pct": 20.0,
    "oi_max_pct": 50.0,
}
BASE_RULE_SLUG = "step50_legs4_equal_tp25_168"


@dataclass(frozen=True, slots=True)
class RuleConfig:
    slug: str
    step_pct: float
    max_legs: int
    sizing_mode: str
    exit_plan: dict[str, Any]


def run_per_event_strategy_research(
    *,
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    max_event_pages: int | None = None,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    events_dir = output_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)

    rules = build_rule_configs()
    case_rows: list[dict[str, Any]] = []
    top_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    entered_cases: list[dict[str, Any]] = []
    skipped_cases: list[dict[str, Any]] = []
    symbols_seen = 0
    events_seen = 0

    for sample in load_samples(input_path):
        symbols_seen += 1
        series = sample_to_series(sample)
        ts_to_idx = {ts_ms: idx for idx, ts_ms in enumerate(series.ts)}
        events = detect_pump_events(series)
        events_seen += len(events)
        for event in events:
            result = simulate_event_case(series, ts_to_idx, event, rules)
            case_rows.append(result["case"])
            if result["status"] == "entered":
                entered_cases.append(result)
                top_rows.extend(result["top_rows"])
                outcome_rows.extend(result["outcome_rows"])
            else:
                skipped_cases.append(result)

    case_rows.sort(key=lambda row: (to_int(row.get("trigger_ts")) or 0, str(row.get("symbol") or "")))
    top_rows.sort(key=lambda row: (to_int(row.get("trigger_ts")) or 0, str(row.get("symbol") or ""), to_int(row.get("rank_balanced")) or 999))

    symbol_summary = build_symbol_summary(case_rows)
    bucket_summary = build_bucket_summary(case_rows)
    policy_bucket_summary = build_policy_bucket_summary(case_rows, outcome_rows)
    regression_rows = build_feature_regression(case_rows)

    pages_written = 0
    for result in entered_cases:
        if max_event_pages is not None and pages_written >= max_event_pages:
            break
        page_path = events_dir / f"{result['case']['case_id']}.html"
        page_path.write_text(render_event_page(result), encoding="utf-8")
        result["case"]["event_report"] = f"events/{result['case']['case_id']}.html"
        pages_written += 1

    write_csv(output_dir / "per_event_summary.csv", case_rows)
    write_csv(output_dir / "per_event_top_strategies.csv", top_rows)
    write_csv(output_dir / "per_event_all_outcomes.csv", outcome_rows)
    write_csv(output_dir / "symbol_summary.csv", symbol_summary)
    write_csv(output_dir / "bucket_dependency_summary.csv", bucket_summary)
    write_csv(output_dir / "policy_bucket_summary.csv", policy_bucket_summary)
    write_csv(output_dir / "feature_regression.csv", regression_rows)
    (output_dir / "index.html").write_text(
        render_index(
            case_rows=case_rows,
            symbol_summary=symbol_summary,
            bucket_summary=bucket_summary,
            policy_bucket_summary=policy_bucket_summary,
            regression_rows=regression_rows,
            pages_written=pages_written,
            rules_count=len(rules),
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema": "pump_short_per_event_strategy_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "pump_events": events_seen,
        "entered_cases": len(entered_cases),
        "skipped_cases": len(skipped_cases),
        "rules": len(rules),
        "outcome_rows": len(outcome_rows),
        "event_pages": pages_written,
        "capital_usd": CAPITAL_USD,
        "leverage": LEVERAGE,
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
    exit_plans: list[dict[str, Any]] = []
    for tp_pct in (15.0, 20.0, 25.0, 30.0, 35.0, 45.0, 60.0):
        exit_plans.append({"name": f"tp{int(tp_pct)}_168", "max_hold_h": 168, "targets": ((tp_pct, 1.0),)})
    exit_plans.extend(
        [
            {"name": "tp25_336", "max_hold_h": 336, "targets": ((25.0, 1.0),)},
            {"name": "tp25_720", "max_hold_h": 720, "targets": ((25.0, 1.0),)},
            {"name": "tp35_720", "max_hold_h": 720, "targets": ((35.0, 1.0),)},
            {"name": "tp25_50_half_336", "max_hold_h": 336, "targets": ((25.0, 0.5), (50.0, 1.0))},
            {"name": "tp35_70_half_720", "max_hold_h": 720, "targets": ((35.0, 0.5), (70.0, 1.0))},
        ]
    )
    configs: list[RuleConfig] = []
    for step_pct in (35.0, 50.0, 75.0, 100.0, 150.0, 200.0, 300.0):
        for max_legs in range(1, 7):
            for sizing_mode in ("equal", "tapered"):
                for exit_plan in exit_plans:
                    configs.append(
                        RuleConfig(
                            slug=f"step{int(step_pct)}_legs{max_legs}_{sizing_mode}_{exit_plan['name']}",
                            step_pct=step_pct,
                            max_legs=max_legs,
                            sizing_mode=sizing_mode,
                            exit_plan=exit_plan,
                        )
                    )
    return configs


def simulate_event_case(
    series: Series,
    ts_to_idx: dict[int, int],
    event: Any,
    rules: list[RuleConfig],
) -> dict[str, Any]:
    base_row = base_research_row(series, event)
    case_id = safe_id(str(base_row["event_id"]))
    entry_idx = resolve_entry_idx(series, event, ENTRY_SETUP)
    case = base_case_row(base_row, case_id)
    if entry_idx is None:
        case.update({"status": "skipped_no_confirmed_entry", "skip_reason": "No pb20/OI<=50/long-ratio-mid entry within 168h"})
        return {"status": "skipped", "case": case}

    outcomes: list[dict[str, Any]] = []
    for rule in rules:
        row = simulate_ladder_rule(
            series,
            event,
            base_row,
            entry_setup=str(ENTRY_SETUP["name"]),
            entry_idx=entry_idx,
            step_pct=rule.step_pct,
            max_legs=rule.max_legs,
            add_window_h=168,
            sizing_mode=rule.sizing_mode,
            exit_plan=rule.exit_plan,
        )
        if row:
            outcomes.append(outcome_row(row, rule, case_id, ts_to_idx))
    if not outcomes:
        case.update({"status": "skipped_no_outcomes", "skip_reason": "Entry resolved but no strategy produced a close"})
        return {"status": "skipped", "case": case}

    base = find_rule(outcomes, BASE_RULE_SLUG) or outcomes[0]
    for row in outcomes:
        row["balanced_score"] = balanced_score(row)
        row["net_pnl_usd"] = rounded(CAPITAL_USD * LEVERAGE * (to_float(row.get("net_reserved_pct")) or 0.0) / 100.0)
        row["improvement_vs_base_pct"] = rounded((to_float(row.get("net_reserved_pct")) or 0.0) - (to_float(base.get("net_reserved_pct")) or 0.0))
    raw_ranked = sorted(outcomes, key=lambda row: to_float(row.get("net_reserved_pct")) or -10**9, reverse=True)
    balanced_ranked = sorted(outcomes, key=lambda row: to_float(row.get("balanced_score")) or -10**9, reverse=True)
    best_raw = raw_ranked[0]
    best_balanced = balanced_ranked[0]

    top_rows: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    for idx, row in enumerate(raw_ranked[:10], start=1):
        item = dict(row)
        item["rank_raw"] = idx
        item["rank_balanced"] = ""
        selected_ids.add(str(row["rule_slug"]))
        top_rows.append(item)
    for idx, row in enumerate(balanced_ranked[:10], start=1):
        if str(row["rule_slug"]) in selected_ids:
            for item in top_rows:
                if item["rule_slug"] == row["rule_slug"]:
                    item["rank_balanced"] = idx
                    break
        else:
            item = dict(row)
            item["rank_raw"] = ""
            item["rank_balanced"] = idx
            top_rows.append(item)

    case.update(
        {
            "status": "entered",
            "skip_reason": "",
            "entry_ts": base.get("entry_ts"),
            "entry_iso": iso(base.get("entry_ts")),
            "base_rule_slug": BASE_RULE_SLUG,
            "base_net_reserved_pct": rounded(to_float(base.get("net_reserved_pct"))),
            "base_net_pnl_usd": rounded(CAPITAL_USD * LEVERAGE * (to_float(base.get("net_reserved_pct")) or 0.0) / 100.0),
            "base_stress_pct": rounded(to_float(base.get("max_margin_stress_reserved_pct"))),
            "base_exit_reason": base.get("exit_reason"),
            "base_exit_iso": iso(base.get("exit_ts")),
            "best_raw_rule_slug": best_raw.get("rule_slug"),
            "best_raw_net_reserved_pct": rounded(to_float(best_raw.get("net_reserved_pct"))),
            "best_raw_net_pnl_usd": best_raw.get("net_pnl_usd"),
            "best_raw_stress_pct": rounded(to_float(best_raw.get("max_margin_stress_reserved_pct"))),
            "best_raw_exit_iso": iso(best_raw.get("exit_ts")),
            "best_balanced_rule_slug": best_balanced.get("rule_slug"),
            "best_balanced_net_reserved_pct": rounded(to_float(best_balanced.get("net_reserved_pct"))),
            "best_balanced_net_pnl_usd": best_balanced.get("net_pnl_usd"),
            "best_balanced_stress_pct": rounded(to_float(best_balanced.get("max_margin_stress_reserved_pct"))),
            "best_balanced_score": rounded(to_float(best_balanced.get("balanced_score"))),
            "best_balanced_exit_iso": iso(best_balanced.get("exit_ts")),
            "best_balanced_improvement_vs_base_pct": rounded(to_float(best_balanced.get("improvement_vs_base_pct"))),
            "base_rank_by_net": rank_of(outcomes, BASE_RULE_SLUG, key="net_reserved_pct"),
            "base_rank_by_balanced": rank_of(outcomes, BASE_RULE_SLUG, key="balanced_score"),
            "strategy_count": len(outcomes),
            "positive_strategy_count": sum(1 for row in outcomes if (to_float(row.get("net_reserved_pct")) or 0.0) > 0.0),
            "any_rule_improves_base": int((to_float(best_balanced.get("improvement_vs_base_pct")) or 0.0) > 0.0),
            "base_lost": int((to_float(base.get("net_reserved_pct")) or 0.0) < 0.0),
            "best_balanced_lost": int((to_float(best_balanced.get("net_reserved_pct")) or 0.0) < 0.0),
            "event_report": f"events/{case_id}.html",
        }
    )
    chart = chart_payload(series, event.trigger_idx, entry_idx, base, best_balanced)
    return {
        "status": "entered",
        "case": case,
        "top_rows": top_rows,
        "outcome_rows": outcomes,
        "chart": chart,
        "base": base,
        "best_balanced": best_balanced,
        "best_raw": best_raw,
        "series": series,
    }


def base_case_row(row: dict[str, Any], case_id: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "symbol": row.get("symbol"),
        "event_id": row.get("event_id"),
        "trigger_ts": row.get("trigger_ts"),
        "trigger_iso": iso(row.get("trigger_ts")),
        "config_window_h": row.get("config_window_h"),
        "config_threshold_pct": row.get("config_threshold_pct"),
        "pump_pct": rounded(to_float(row.get("pump_pct"))),
        "age_days": rounded(to_float(row.get("age_days"))),
        "funding_prev_24h_pct": rounded(to_float(row.get("funding_prev_24h_pct"))),
        "funding_prev_72h_pct": rounded(to_float(row.get("funding_prev_72h_pct"))),
        "oi_change_4h_pct": rounded(to_float(row.get("oi_change_4h_pct"))),
        "oi_change_24h_pct": rounded(to_float(row.get("oi_change_24h_pct"))),
        "long_ratio": rounded(to_float(row.get("long_ratio"))),
        "pump_regime": row.get("pump_regime"),
        "funding_regime": row.get("funding_regime"),
        "oi_regime": row.get("oi_regime"),
        "long_ratio_regime": row.get("long_ratio_regime"),
    }


def outcome_row(row: dict[str, Any], rule: RuleConfig, case_id: str, ts_to_idx: dict[int, int]) -> dict[str, Any]:
    out = {
        "case_id": case_id,
        "symbol": row.get("symbol"),
        "event_id": row.get("event_id"),
        "trigger_ts": row.get("trigger_ts"),
        "trigger_iso": iso(row.get("trigger_ts")),
        "entry_ts": row.get("entry_ts"),
        "entry_iso": iso(row.get("entry_ts")),
        "exit_ts": row.get("exit_ts"),
        "exit_iso": iso(row.get("exit_ts")),
        "rule_slug": rule.slug,
        "step_pct": rule.step_pct,
        "max_legs": rule.max_legs,
        "sizing_mode": rule.sizing_mode,
        "exit_strategy": rule.exit_plan["name"],
        "max_hold_h": rule.exit_plan["max_hold_h"],
        "exit_reason": row.get("exit_reason"),
        "legs_activated": row.get("legs_activated"),
        "net_reserved_pct": rounded(to_float(row.get("net_reserved_pct"))),
        "net_deployed_pct": rounded(to_float(row.get("net_deployed_pct"))),
        "funding_deployed_pct": rounded(to_float(row.get("funding_deployed_pct"))),
        "max_margin_stress_reserved_pct": rounded(to_float(row.get("max_margin_stress_reserved_pct"))),
        "max_adverse_from_first_pct": rounded(to_float(row.get("max_adverse_from_first_pct"))),
        "time_in_trade_h": row.get("time_in_trade_h"),
        "pump_pct": rounded(to_float(row.get("pump_pct"))),
        "oi_change_24h_pct": rounded(to_float(row.get("oi_change_24h_pct"))),
        "long_ratio": rounded(to_float(row.get("long_ratio"))),
    }
    out["entry_idx"] = ts_to_idx.get(to_int(row.get("entry_ts")) or -1, "")
    out["exit_idx"] = ts_to_idx.get(to_int(row.get("exit_ts")) or -1, "")
    return out


def balanced_score(row: dict[str, Any]) -> float:
    net = to_float(row.get("net_reserved_pct")) or 0.0
    stress = max(0.0, to_float(row.get("max_margin_stress_reserved_pct")) or 0.0)
    adverse = max(0.0, to_float(row.get("max_adverse_from_first_pct")) or 0.0)
    hold = max(0.0, to_float(row.get("time_in_trade_h")) or 0.0)
    legs = max(1.0, to_float(row.get("max_legs")) or 1.0)
    return round(net - stress * 0.08 - adverse * 0.012 - hold / 720.0 - legs * 0.05, 6)


def rank_of(rows: list[dict[str, Any]], slug: str, *, key: str) -> int | None:
    ranked = sorted(rows, key=lambda row: to_float(row.get(key)) or -10**9, reverse=True)
    for idx, row in enumerate(ranked, start=1):
        if row.get("rule_slug") == slug:
            return idx
    return None


def find_rule(rows: list[dict[str, Any]], slug: str) -> dict[str, Any] | None:
    for row in rows:
        if row.get("rule_slug") == slug:
            return row
    return None


def build_symbol_summary(case_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in case_rows:
        groups.setdefault(str(row.get("symbol") or ""), []).append(row)
    out: list[dict[str, Any]] = []
    for symbol, rows in groups.items():
        entered = [row for row in rows if row.get("status") == "entered"]
        base = values(entered, "base_net_reserved_pct")
        best = values(entered, "best_balanced_net_reserved_pct")
        improvement = values(entered, "best_balanced_improvement_vs_base_pct")
        out.append(
            {
                "symbol": symbol,
                "pump_events": len(rows),
                "entered_cases": len(entered),
                "base_loss_count": sum(1 for row in entered if row.get("base_lost")),
                "best_balanced_loss_count": sum(1 for row in entered if row.get("best_balanced_lost")),
                "avg_base_net_pct": rounded_mean(base),
                "avg_best_balanced_net_pct": rounded_mean(best),
                "avg_improvement_pct": rounded_mean(improvement),
                "worst_base_net_pct": rounded(min(base) if base else None),
                "worst_best_balanced_net_pct": rounded(min(best) if best else None),
                "dominant_best_rule": most_common([str(row.get("best_balanced_rule_slug") or "") for row in entered]),
            }
        )
    out.sort(key=lambda row: (to_float(row.get("avg_improvement_pct")) or 0.0), reverse=True)
    return out


def build_bucket_summary(case_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entered = [row for row in case_rows if row.get("status") == "entered"]
    buckets: list[tuple[str, str, list[dict[str, Any]]]] = []
    for key in ("pump_regime", "oi_regime", "funding_regime", "long_ratio_regime"):
        values_seen = sorted({str(row.get(key) or "") for row in entered})
        for value in values_seen:
            buckets.append((key, value, [row for row in entered if str(row.get(key) or "") == value]))
    numeric_gates = (
        ("pump_pct_ge_80", lambda row: (to_float(row.get("pump_pct")) or 0.0) >= 80.0),
        ("pump_pct_ge_150", lambda row: (to_float(row.get("pump_pct")) or 0.0) >= 150.0),
        ("pump_pct_ge_250", lambda row: (to_float(row.get("pump_pct")) or 0.0) >= 250.0),
        ("oi24_ge_50", lambda row: (to_float(row.get("oi_change_24h_pct")) or -10**9) >= 50.0),
        ("oi24_ge_100", lambda row: (to_float(row.get("oi_change_24h_pct")) or -10**9) >= 100.0),
        ("long_ratio_ge_60", lambda row: (to_float(row.get("long_ratio")) or -10**9) >= 0.60),
        ("young_lt_30d", lambda row: (to_float(row.get("age_days")) or 10**9) < 30.0),
    )
    for name, predicate in numeric_gates:
        buckets.append(("gate", name, [row for row in entered if predicate(row)]))
    out: list[dict[str, Any]] = []
    for bucket_type, bucket_value, rows in buckets:
        if not rows:
            continue
        base = values(rows, "base_net_reserved_pct")
        best = values(rows, "best_balanced_net_reserved_pct")
        improvement = values(rows, "best_balanced_improvement_vs_base_pct")
        out.append(
            {
                "bucket_type": bucket_type,
                "bucket_value": bucket_value,
                "n": len(rows),
                "base_loss_rate_pct": pct(sum(1 for row in rows if row.get("base_lost")), len(rows)),
                "best_loss_rate_pct": pct(sum(1 for row in rows if row.get("best_balanced_lost")), len(rows)),
                "avg_base_net_pct": rounded_mean(base),
                "avg_best_balanced_net_pct": rounded_mean(best),
                "avg_improvement_pct": rounded_mean(improvement),
                "median_improvement_pct": rounded_median(improvement),
                "dominant_best_rule": most_common([str(row.get("best_balanced_rule_slug") or "") for row in rows]),
            }
        )
    out.sort(key=lambda row: (to_float(row.get("avg_improvement_pct")) or 0.0), reverse=True)
    return out


def build_policy_bucket_summary(case_rows: list[dict[str, Any]], outcome_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entered = [row for row in case_rows if row.get("status") == "entered"]
    by_case = {str(row.get("case_id")): row for row in entered}
    outcome_by_case: dict[str, list[dict[str, Any]]] = {}
    for row in outcome_rows:
        outcome_by_case.setdefault(str(row.get("case_id") or ""), []).append(row)

    bucket_defs: list[tuple[str, str, set[str]]] = []
    for key in ("pump_regime", "oi_regime", "funding_regime", "long_ratio_regime"):
        values_seen = sorted({str(row.get(key) or "") for row in entered})
        for value in values_seen:
            ids = {str(row.get("case_id")) for row in entered if str(row.get(key) or "") == value}
            bucket_defs.append((key, value, ids))
    numeric_gates = (
        ("pump_pct_ge_80", lambda row: (to_float(row.get("pump_pct")) or 0.0) >= 80.0),
        ("pump_pct_ge_150", lambda row: (to_float(row.get("pump_pct")) or 0.0) >= 150.0),
        ("pump_pct_ge_250", lambda row: (to_float(row.get("pump_pct")) or 0.0) >= 250.0),
        ("oi24_ge_50", lambda row: (to_float(row.get("oi_change_24h_pct")) or -10**9) >= 50.0),
        ("oi24_ge_100", lambda row: (to_float(row.get("oi_change_24h_pct")) or -10**9) >= 100.0),
        ("long_ratio_ge_60", lambda row: (to_float(row.get("long_ratio")) or -10**9) >= 0.60),
        ("young_lt_30d", lambda row: (to_float(row.get("age_days")) or 10**9) < 30.0),
    )
    for name, predicate in numeric_gates:
        ids = {str(row.get("case_id")) for row in entered if predicate(row)}
        bucket_defs.append(("gate", name, ids))

    out: list[dict[str, Any]] = []
    for bucket_type, bucket_value, case_ids in bucket_defs:
        if len(case_ids) < 5:
            continue
        base_cases = [by_case[case_id] for case_id in case_ids if case_id in by_case]
        base_net = values(base_cases, "base_net_reserved_pct")
        grouped: dict[str, list[dict[str, Any]]] = {}
        for case_id in case_ids:
            for outcome in outcome_by_case.get(case_id, []):
                grouped.setdefault(str(outcome.get("rule_slug") or ""), []).append(outcome)
        candidates: list[dict[str, Any]] = []
        for slug, rows in grouped.items():
            if len(rows) != len(case_ids):
                continue
            net = values(rows, "net_reserved_pct")
            stress = values(rows, "max_margin_stress_reserved_pct")
            score = values(rows, "balanced_score")
            candidates.append(
                {
                    "rule_slug": slug,
                    "avg_net_pct": rounded_mean(net),
                    "median_net_pct": rounded_median(net),
                    "loss_rate_pct": pct(sum(1 for row in rows if (to_float(row.get("net_reserved_pct")) or 0.0) < 0.0), len(rows)),
                    "avg_stress_pct": rounded_mean(stress),
                    "p90_stress_pct": percentile(stress, 90),
                    "avg_balanced_score": rounded_mean(score),
                }
            )
        if not candidates:
            continue
        candidates.sort(key=lambda row: to_float(row.get("avg_balanced_score")) or -10**9, reverse=True)
        best = candidates[0]
        base_rule_rows = grouped.get(BASE_RULE_SLUG, [])
        base_rule_score = rounded_mean(values(base_rule_rows, "balanced_score")) if base_rule_rows else None
        out.append(
            {
                "bucket_type": bucket_type,
                "bucket_value": bucket_value,
                "n": len(case_ids),
                "base_avg_net_pct": rounded_mean(base_net),
                "base_loss_rate_pct": pct(sum(1 for row in base_cases if row.get("base_lost")), len(base_cases)),
                "base_avg_balanced_score": base_rule_score,
                "best_policy_rule": best["rule_slug"],
                "best_policy_avg_net_pct": best["avg_net_pct"],
                "best_policy_median_net_pct": best["median_net_pct"],
                "best_policy_loss_rate_pct": best["loss_rate_pct"],
                "best_policy_avg_stress_pct": best["avg_stress_pct"],
                "best_policy_p90_stress_pct": best["p90_stress_pct"],
                "best_policy_avg_balanced_score": best["avg_balanced_score"],
                "policy_improvement_vs_base_net_pct": rounded((to_float(best.get("avg_net_pct")) or 0.0) - (rounded_mean(base_net) or 0.0)),
                "policy_score_improvement_vs_base": rounded((to_float(best.get("avg_balanced_score")) or 0.0) - (base_rule_score or 0.0)),
            }
        )
    out.sort(key=lambda row: to_float(row.get("policy_score_improvement_vs_base")) or -10**9, reverse=True)
    return out


def build_feature_regression(case_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for row in case_rows if row.get("status") == "entered"]
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
        "base_net_reserved_pct",
        "base_stress_pct",
    )
    targets = ("best_balanced_improvement_vs_base_pct", "base_lost", "best_balanced_net_reserved_pct")
    out: list[dict[str, Any]] = []
    for target in targets:
        model_rows = []
        for row in rows:
            y = to_float(row.get(target))
            xs = [to_float(row.get(feature)) for feature in features]
            if y is None or any(value is None for value in xs):
                continue
            model_rows.append((xs, y))
        if len(model_rows) < 20:
            continue
        xs = [item[0] for item in model_rows]
        y = [item[1] for item in model_rows]
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
                }
            )
    out.sort(key=lambda row: (str(row.get("target")), -(to_float(row.get("abs_coefficient")) or 0.0)))
    return out


def chart_payload(
    series: Series,
    trigger_idx: int,
    entry_idx: int,
    base: dict[str, Any],
    best: dict[str, Any],
) -> dict[str, Any]:
    base_exit_idx = to_int(base.get("exit_idx")) or entry_idx
    best_exit_idx = to_int(best.get("exit_idx")) or entry_idx
    end_idx = min(len(series.ts) - 1, max(base_exit_idx, best_exit_idx) + 24)
    start_idx = max(0, trigger_idx - 24)
    points = [
        {
            "idx": idx,
            "ts": series.ts[idx],
            "close": series.close[idx],
            "high": series.high[idx],
            "low": series.low[idx],
        }
        for idx in range(start_idx, end_idx + 1)
        if series.close[idx] is not None
    ]
    return {
        "start_idx": start_idx,
        "end_idx": end_idx,
        "trigger_idx": trigger_idx,
        "entry_idx": entry_idx,
        "base_exit_idx": base_exit_idx,
        "best_exit_idx": best_exit_idx,
        "points": points,
        "base_levels": planned_levels(series.close[entry_idx] or 0.0, to_float(base.get("step_pct")) or 50.0, to_int(base.get("max_legs")) or 4),
        "best_levels": planned_levels(series.close[entry_idx] or 0.0, to_float(best.get("step_pct")) or 50.0, to_int(best.get("max_legs")) or 4),
    }


def planned_levels(first_price: float, step_pct: float, max_legs: int) -> list[float]:
    if first_price <= 0:
        return []
    return [first_price * (1.0 + step_pct / 100.0 * idx) for idx in range(max_legs)]


def render_index(
    *,
    case_rows: list[dict[str, Any]],
    symbol_summary: list[dict[str, Any]],
    bucket_summary: list[dict[str, Any]],
    policy_bucket_summary: list[dict[str, Any]],
    regression_rows: list[dict[str, Any]],
    pages_written: int,
    rules_count: int,
) -> str:
    entered = [row for row in case_rows if row.get("status") == "entered"]
    skipped = [row for row in case_rows if row.get("status") != "entered"]
    base_values = values(entered, "base_net_reserved_pct")
    best_values = values(entered, "best_balanced_net_reserved_pct")
    improvement_values = values(entered, "best_balanced_improvement_vs_base_pct")
    worst_base = sorted(entered, key=lambda row: to_float(row.get("base_net_reserved_pct")) or 0.0)[:20]
    biggest_improvement = sorted(entered, key=lambda row: to_float(row.get("best_balanced_improvement_vs_base_pct")) or 0.0, reverse=True)[:20]
    content = f"""
    <section class="panel">
      <h1>Bybit pump-short per-event strategy research</h1>
      <p>Each pump case is tested independently with the same theoretical ${CAPITAL_USD:,.0f} capital and {LEVERAGE:.0f}x leverage. No portfolio slots, overlap constraints, compounding, top-up reuse, or multi-coin capital management are applied here.</p>
      <div class="metrics">
        <div><b>{len(case_rows)}</b><span>pump trigger cases</span></div>
        <div><b>{len(entered)}</b><span>entered cases</span></div>
        <div><b>{len(skipped)}</b><span>skipped/no entry</span></div>
        <div><b>{rules_count}</b><span>rules per entered case</span></div>
        <div><b>{fmt(rounded_mean(base_values))}%</b><span>avg base net</span></div>
        <div><b>{fmt(rounded_mean(best_values))}%</b><span>avg best balanced net</span></div>
        <div><b>{fmt(rounded_mean(improvement_values))}%</b><span>avg improvement</span></div>
        <div><b>{pages_written}</b><span>event pages</span></div>
      </div>
    </section>
    <section class="panel"><h2>Human conclusion</h2>{render_conclusions(case_rows, bucket_summary, policy_bucket_summary, regression_rows)}</section>
    <section class="panel"><h2>Worst base cases</h2>{html_table(worst_base, ("symbol","trigger_iso","pump_pct","base_net_reserved_pct","best_balanced_net_reserved_pct","best_balanced_improvement_vs_base_pct","best_balanced_rule_slug","event_report"))}</section>
    <section class="panel"><h2>Largest best-rule improvements</h2>{html_table(biggest_improvement, ("symbol","trigger_iso","pump_pct","base_net_reserved_pct","best_balanced_net_reserved_pct","best_balanced_improvement_vs_base_pct","best_balanced_rule_slug","event_report"))}</section>
    <section class="panel"><h2>Indicator bucket policies</h2><p>This is closer to a tradable conditional approach: one rule is selected for the whole bucket, instead of selecting the best rule separately after each event.</p>{html_table(policy_bucket_summary[:30], ("bucket_type","bucket_value","n","base_avg_net_pct","best_policy_rule","best_policy_avg_net_pct","policy_improvement_vs_base_net_pct","best_policy_loss_rate_pct","best_policy_p90_stress_pct"))}</section>
    <section class="panel"><h2>Top dependency buckets</h2>{html_table(bucket_summary[:30], ("bucket_type","bucket_value","n","avg_base_net_pct","avg_best_balanced_net_pct","avg_improvement_pct","dominant_best_rule"))}</section>
    <section class="panel"><h2>Regression read</h2>{html_table(regression_rows[:30], ("target","feature","standardized_coefficient","r2"))}</section>
    <section class="panel"><h2>Symbol summary</h2>{html_table(symbol_summary[:80], ("symbol","pump_events","entered_cases","base_loss_count","best_balanced_loss_count","avg_base_net_pct","avg_best_balanced_net_pct","avg_improvement_pct","dominant_best_rule"))}</section>
    """
    return page_shell("Bybit per-event strategy research", content)


def render_conclusions(
    case_rows: list[dict[str, Any]],
    bucket_summary: list[dict[str, Any]],
    policy_bucket_summary: list[dict[str, Any]],
    regression_rows: list[dict[str, Any]],
) -> str:
    entered = [row for row in case_rows if row.get("status") == "entered"]
    base_losses = sum(1 for row in entered if row.get("base_lost"))
    best_losses = sum(1 for row in entered if row.get("best_balanced_lost"))
    meaningful = sum(1 for row in entered if (to_float(row.get("best_balanced_improvement_vs_base_pct")) or 0.0) >= 5.0)
    strong_buckets = [row for row in bucket_summary if (to_int(row.get("n")) or 0) >= 10][:5]
    improvement_regs = [row for row in regression_rows if row.get("target") == "best_balanced_improvement_vs_base_pct"][:5]
    parts = [
        f"<p>Base strategy lost on <b>{base_losses}</b> of {len(entered)} entered cases. The best balanced per-event rule still lost on <b>{best_losses}</b> cases, so individual selection improves but does not make every pump safe.</p>",
        f"<p>A meaningful per-event gain of at least 5 percentage points over base appeared on <b>{meaningful}</b> entered cases. This supports a conditional approach, but only if the condition can be known at entry time and stays stable out of sample.</p>",
    ]
    if strong_buckets:
        parts.append("<p>Best improvement buckets: " + "; ".join(f"{esc(row['bucket_type'])}/{esc(row['bucket_value'])}: +{fmt(row.get('avg_improvement_pct'))}%" for row in strong_buckets) + ".</p>")
    strong_policies = [row for row in policy_bucket_summary if (to_int(row.get("n")) or 0) >= 10][:5]
    if strong_policies:
        parts.append("<p>Best indicator-bucket policies: " + "; ".join(f"{esc(row['bucket_type'])}/{esc(row['bucket_value'])}: {esc(row['best_policy_rule'])}, avg net +{fmt(row.get('policy_improvement_vs_base_net_pct'))}% vs base" for row in strong_policies) + ".</p>")
    if improvement_regs:
        parts.append("<p>Regression-style drivers of best-vs-base improvement: " + "; ".join(f"{esc(row['feature'])} {fmt(row['standardized_coefficient'])}" for row in improvement_regs) + ".</p>")
    parts.append("<p>Practical read: this pass is useful for discovering regimes and candidate rule families, not for choosing a different strategy for every single coin by hindsight. The robust next step is to test a small number of indicator-gated families, especially risk buckets where base loses or stress rises.</p>")
    return "\n".join(parts)


def render_event_page(result: dict[str, Any]) -> str:
    case = result["case"]
    top = sorted(result["top_rows"], key=lambda row: (to_int(row.get("rank_balanced")) or 999, to_int(row.get("rank_raw")) or 999))[:20]
    content = f"""
    <section class="panel">
      <p><a href="../index.html">back to index</a></p>
      <h1>{esc(case.get('symbol'))}: independent pump case</h1>
      <div class="metrics">
        <div><b>{fmt(case.get('pump_pct'))}%</b><span>pump</span></div>
        <div><b>{fmt(case.get('base_net_reserved_pct'))}%</b><span>base net</span></div>
        <div><b>{fmt(case.get('best_balanced_net_reserved_pct'))}%</b><span>best balanced net</span></div>
        <div><b>{fmt(case.get('best_balanced_improvement_vs_base_pct'))}%</b><span>improvement</span></div>
        <div><b>{fmt(case.get('base_stress_pct'))}%</b><span>base stress</span></div>
        <div><b>{fmt(case.get('best_balanced_stress_pct'))}%</b><span>best stress</span></div>
      </div>
    </section>
    <section class="panel"><h2>Price Chart</h2>{render_svg_chart(result)}<p class="legend">Gray dashed levels: base strategy. Orange levels: best balanced event-specific strategy. Red: pump trigger. Dark vertical: entry. Gray/green verticals: base/best exits.</p></section>
    <section class="panel"><h2>Case Indicators</h2>{html_table([case], ("trigger_iso","entry_iso","pump_pct","age_days","funding_prev_24h_pct","oi_change_4h_pct","oi_change_24h_pct","long_ratio","pump_regime","oi_regime","funding_regime"))}</section>
    <section class="panel"><h2>Base vs Best</h2>{html_table([case], ("base_rule_slug","base_net_reserved_pct","base_net_pnl_usd","base_stress_pct","base_exit_iso","best_balanced_rule_slug","best_balanced_net_reserved_pct","best_balanced_net_pnl_usd","best_balanced_stress_pct","best_balanced_exit_iso"))}</section>
    <section class="panel"><h2>Top Strategies</h2>{html_table(top, ("rank_balanced","rank_raw","rule_slug","net_reserved_pct","net_pnl_usd","balanced_score","max_margin_stress_reserved_pct","max_adverse_from_first_pct","time_in_trade_h","exit_reason"))}</section>
    """
    return page_shell(f"{case.get('symbol')} per-event strategy", content)


def render_svg_chart(result: dict[str, Any]) -> str:
    chart = result["chart"]
    points = chart["points"]
    if not points:
        return "<p>No chart data.</p>"
    width = 980
    height = 360
    pad_left = 56
    pad_right = 24
    pad_top = 24
    pad_bottom = 44
    prices = [to_float(point.get("close")) for point in points]
    prices.extend(chart["base_levels"])
    prices.extend(chart["best_levels"])
    clean = [price for price in prices if price is not None and math.isfinite(price)]
    min_price = min(clean)
    max_price = max(clean)
    span = max(max_price - min_price, max_price * 0.01)
    min_price -= span * 0.08
    max_price += span * 0.08
    start_idx = int(chart["start_idx"])
    end_idx = int(chart["end_idx"])
    idx_span = max(1, end_idx - start_idx)

    def x(idx: int) -> float:
        return pad_left + (idx - start_idx) / idx_span * (width - pad_left - pad_right)

    def y(price: float) -> float:
        return pad_top + (max_price - price) / (max_price - min_price) * (height - pad_top - pad_bottom)

    path = " ".join(f"{'M' if i == 0 else 'L'} {x(int(point['idx'])):.1f} {y(float(point['close'])):.1f}" for i, point in enumerate(points) if point.get("close") is not None)
    lines: list[str] = [f"<path d='{path}' class='price'/>"]
    for price in chart["base_levels"]:
        lines.append(f"<line x1='{pad_left}' y1='{y(price):.1f}' x2='{width-pad_right}' y2='{y(price):.1f}' class='base-level'/>")
    for price in chart["best_levels"]:
        lines.append(f"<line x1='{pad_left}' y1='{y(price):.1f}' x2='{width-pad_right}' y2='{y(price):.1f}' class='best-level'/>")
    markers = (
        ("trigger_idx", "trigger", "pump"),
        ("entry_idx", "entry", "entry"),
        ("base_exit_idx", "base-exit", "base exit"),
        ("best_exit_idx", "best-exit", "best exit"),
    )
    for key, cls, label in markers:
        idx = int(chart[key])
        lines.append(f"<line x1='{x(idx):.1f}' y1='{pad_top}' x2='{x(idx):.1f}' y2='{height-pad_bottom}' class='{cls}'/><text x='{x(idx)+4:.1f}' y='{pad_top+14}' class='tiny'>{esc(label)}</text>")
    return (
        f"<svg viewBox='0 0 {width} {height}' class='chart' role='img'>"
        f"<text x='8' y='{y(max_price - (max_price-min_price)*0.08):.1f}' class='axis'>{fmt(max_price)} </text>"
        f"<text x='8' y='{y(min_price + (max_price-min_price)*0.08):.1f}' class='axis'>{fmt(min_price)} </text>"
        + "".join(lines)
        + f"<text x='{pad_left}' y='{height-12}' class='axis'>{date_label(points[0]['ts'])}</text><text x='{width-180}' y='{height-12}' class='axis'>{date_label(points[-1]['ts'])}</text></svg>"
    )


def html_table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    head = "".join(f"<th>{esc(column)}</th>" for column in columns)
    body = []
    for row in rows:
        cells = []
        for column in columns:
            value = row.get(column, "")
            if column == "event_report" and value:
                cells.append(f"<td><a href='{esc(value)}'>open</a></td>")
            else:
                cells.append(f"<td>{esc(value)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def page_shell(title: str, content: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <style>
    body {{ margin: 0; background: #f4f6f8; color: #17202a; font: 14px/1.45 Arial, sans-serif; }}
    .wrap {{ max-width: 1220px; margin: 0 auto; padding: 22px; }}
    .panel {{ background: #fff; border: 1px solid #dfe5eb; border-radius: 8px; padding: 18px; margin: 0 0 16px; overflow-x: auto; }}
    h1 {{ margin: 0 0 10px; font-size: 26px; }}
    h2 {{ margin: 0 0 10px; font-size: 19px; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-top: 14px; }}
    .metrics div {{ border: 1px solid #e2e8ee; border-radius: 6px; padding: 10px; background: #fbfcfd; }}
    .metrics b {{ display: block; font-size: 20px; }}
    .metrics span {{ color: #607080; font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 7px 8px; border-bottom: 1px solid #e5eaf0; text-align: left; vertical-align: top; }}
    th {{ background: #f7f9fb; color: #34495e; position: sticky; top: 0; }}
    a {{ color: #0b63ce; text-decoration: none; }}
    .chart {{ width: 100%; height: auto; background: #fbfcfe; border: 1px solid #e1e6ed; border-radius: 8px; }}
    .price {{ fill: none; stroke: #1f5f99; stroke-width: 2; }}
    .base-level {{ stroke: #8a96a3; stroke-width: 1; stroke-dasharray: 6 5; }}
    .best-level {{ stroke: #e67e22; stroke-width: 1.5; stroke-dasharray: 3 4; }}
    .trigger {{ stroke: #c0392b; stroke-width: 1.4; }}
    .entry {{ stroke: #20262e; stroke-width: 1.2; }}
    .base-exit {{ stroke: #7f8c8d; stroke-width: 1.2; }}
    .best-exit {{ stroke: #1e8449; stroke-width: 1.8; }}
    .tiny, .axis {{ fill: #566573; font-size: 11px; }}
    .legend {{ color: #667788; }}
  </style>
</head>
<body><main class="wrap">{content}</main></body>
</html>"""


def most_common(items: list[str]) -> str:
    counts: dict[str, int] = {}
    for item in items:
        if not item:
            continue
        counts[item] = counts.get(item, 0) + 1
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


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


def fmt(value: Any, digits: int = 2) -> str:
    parsed = to_float(value)
    if parsed is None:
        return ""
    return f"{parsed:.{digits}f}"


def iso(value: Any) -> str:
    ts_ms = to_int(value)
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def date_label(value: Any) -> str:
    ts_ms = to_int(value)
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def safe_id(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value)[:180]
