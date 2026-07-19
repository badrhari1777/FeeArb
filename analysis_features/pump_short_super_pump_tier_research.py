from __future__ import annotations

import csv
import html
import itertools
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_features.pump_short_dynamic_combo_report import parse_date_to_ms
from analysis_features.pump_short_per_event_strategy_research import BASE_RULE_SLUG, LEVERAGE, build_rule_configs
from analysis_features.pump_short_policy_portfolio_research import (
    DEFAULT_INPUT_DIR as DEFAULT_PER_EVENT_DIR,
    build_unique_cases,
    load_csv,
    load_outcomes,
    max_concurrent_value,
    ms_to_iso,
    to_float,
    to_int,
    write_csv,
)
from config import BASE_DIR

DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_super_pump_tier_research"
CAPITAL_USD = 3_000.0
SLOTS = 4
DEFAULT_START = "2024-01-01"
TOP_RULES_PER_BUCKET = 8
TOP_SELECTED_POLICIES = 40


@dataclass(frozen=True, slots=True)
class PumpBucket:
    slug: str
    lo: float
    hi: float | None


@dataclass(frozen=True, slots=True)
class TieredPolicy:
    slug: str
    description: str
    tiers: tuple[tuple[float, str], ...]


BUCKETS = (
    PumpBucket("p100_150", 100.0, 150.0),
    PumpBucket("p150_250", 150.0, 250.0),
    PumpBucket("p250_400", 250.0, 400.0),
    PumpBucket("p400_plus", 400.0, None),
    PumpBucket("p100_plus", 100.0, None),
)


KNOWN_CANDIDATE_RULES = (
    "step50_legs2_tapered_tp25_336",
    "step50_legs3_tapered_tp25_336",
    "step50_legs2_equal_tp25_336",
    "step50_legs3_equal_tp25_336",
    "step50_legs2_tapered_tp25_720",
    "step50_legs3_tapered_tp25_720",
    "step75_legs2_tapered_tp25_336",
    "step75_legs3_tapered_tp25_336",
    "step100_legs2_tapered_tp25_336",
    BASE_RULE_SLUG,
)


def run_super_pump_tier_research(
    *,
    per_event_dir: Path = DEFAULT_PER_EVENT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    start_ts_ms: int | None = None,
    top_rules_per_bucket: int = TOP_RULES_PER_BUCKET,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    start_ts_ms = start_ts_ms if start_ts_ms is not None else parse_date_to_ms(DEFAULT_START)

    all_cases = build_unique_cases(load_csv(per_event_dir / "per_event_summary.csv"))
    cases = [case for case in all_cases if (to_int(case.get("entry_ts")) or 0) >= (start_ts_ms or 0)]
    super_cases = [case for case in cases if f(case, "pump_pct") >= 100.0]
    rule_slugs = sorted(rule.slug for rule in build_rule_configs())
    outcomes = load_outcomes(per_event_dir / "per_event_all_outcomes.csv", {str(case["case_id"]) for case in cases})

    bucket_rows = build_bucket_rule_summary(super_cases, outcomes, rule_slugs)
    write_csv(output_dir / "super_pump_bucket_rule_summary.csv", bucket_rows)

    policies = build_tiered_policies(bucket_rows, top_rules_per_bucket=top_rules_per_bucket)
    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    for policy in policies:
        result = simulate_tiered_policy(
            cases=cases,
            outcomes=outcomes,
            policy=policy,
            split_ts=train_test_split_ts(cases),
        )
        summary_rows.append({key: value for key, value in result.items() if key != "selected_trades"})
    summary_rows.sort(key=live_rank_key)
    for idx, row in enumerate(summary_rows, start=1):
        row["rank"] = idx
    write_csv(output_dir / "tiered_policy_summary.csv", summary_rows)

    for row in summary_rows[:TOP_SELECTED_POLICIES]:
        policy = policy_from_summary_row(row)
        result = simulate_tiered_policy(
            cases=cases,
            outcomes=outcomes,
            policy=policy,
            split_ts=train_test_split_ts(cases),
            return_trades=True,
        )
        for trade in result["selected_trades"]:
            trade_rows.append({"rank": row["rank"], **trade})
    write_csv(output_dir / "top_tiered_policy_trades.csv", trade_rows)

    (output_dir / "index.html").write_text(
        render_html_report(
            cases=cases,
            super_cases=super_cases,
            bucket_rows=bucket_rows,
            summary_rows=summary_rows,
            trade_rows=trade_rows,
            split_ts=train_test_split_ts(cases),
            start_ts_ms=start_ts_ms,
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema": "pump_short_super_pump_tier_research_v1",
        "start_ts": start_ts_ms,
        "start_iso": ms_to_iso(start_ts_ms),
        "all_unique_cases": len(all_cases),
        "filtered_unique_cases": len(cases),
        "super_pump_cases": len(super_cases),
        "rule_slugs": len(rule_slugs),
        "bucket_rule_rows": len(bucket_rows),
        "tiered_policies": len(policies),
        "summary_rows": len(summary_rows),
        "selected_trade_rows": len(trade_rows),
        "capital_usd": CAPITAL_USD,
        "slots": SLOTS,
        "split_ts": train_test_split_ts(cases),
        "split_iso": ms_to_iso(train_test_split_ts(cases)),
        "output_dir": str(output_dir),
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def build_bucket_rule_summary(
    cases: list[dict[str, Any]],
    outcomes: dict[tuple[str, str], dict[str, Any]],
    rule_slugs: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in BUCKETS:
        bucket_cases = [case for case in cases if case_in_bucket(case, bucket)]
        for rule in rule_slugs:
            items = [outcomes[(str(case["case_id"]), rule)] for case in bucket_cases if (str(case["case_id"]), rule) in outcomes]
            if not items:
                continue
            net = values(items, "net_reserved_pct")
            stress = values(items, "max_margin_stress_reserved_pct")
            hold = values(items, "time_in_trade_h")
            losses = [value for value in net if value < 0]
            row = {
                "bucket": bucket.slug,
                "pump_lo": bucket.lo,
                "pump_hi": bucket.hi if bucket.hi is not None else "",
                "case_count": len(bucket_cases),
                "rule_slug": rule,
                "n": len(items),
                "avg_net_pct": rounded(avg(net)),
                "median_net_pct": rounded(median(net)),
                "win_rate_pct": rounded(sum(1 for value in net if value > 0) / len(net) * 100.0 if net else 0.0),
                "loss_trades": len(losses),
                "worst_net_pct": rounded(min(net) if net else 0.0),
                "avg_stress_pct": rounded(avg(stress)),
                "max_stress_pct": rounded(max(stress) if stress else 0.0),
                "avg_hold_h": rounded(avg(hold)),
            }
            row["score"] = rounded(rule_score(row))
            rows.append(row)
    rows.sort(key=lambda row: (str(row["bucket"]), -(to_float(row.get("score")) or -10**9)))
    return rows


def build_tiered_policies(bucket_rows: list[dict[str, Any]], *, top_rules_per_bucket: int) -> list[TieredPolicy]:
    candidates = candidate_rules_by_bucket(bucket_rows, top_rules_per_bucket=top_rules_per_bucket)
    policies: dict[str, TieredPolicy] = {}

    def add(policy: TieredPolicy) -> None:
        policies.setdefault(policy.slug, policy)

    for rule100 in candidates["p100_plus"]:
        add(make_policy(((100.0, rule100),)))

    for rule100, rule250 in itertools.product(candidates["p100_150"], candidates["p250_400"]):
        add(make_policy(((100.0, rule100), (250.0, rule250))))

    for rule100, rule150 in itertools.product(candidates["p100_150"], candidates["p150_250"]):
        add(make_policy(((100.0, rule100), (150.0, rule150))))

    for rule100, rule150, rule250 in itertools.product(
        candidates["p100_150"],
        candidates["p150_250"],
        candidates["p250_400"],
    ):
        add(make_policy(((100.0, rule100), (150.0, rule150), (250.0, rule250))))

    for rule100, rule150, rule250, rule400 in itertools.product(
        candidates["p100_150"],
        candidates["p150_250"],
        candidates["p250_400"],
        candidates["p400_plus"],
    ):
        add(make_policy(((100.0, rule100), (150.0, rule150), (250.0, rule250), (400.0, rule400))))

    add(make_policy(((100.0, "step50_legs3_tapered_tp25_336"),)))
    add(make_policy(((100.0, "step50_legs3_tapered_tp25_336"), (250.0, "step50_legs2_tapered_tp25_720"))))
    add(make_policy(((100.0, "step50_legs3_tapered_tp25_336"), (250.0, "step75_legs2_tapered_tp25_720"))))
    return list(policies.values())


def candidate_rules_by_bucket(bucket_rows: list[dict[str, Any]], *, top_rules_per_bucket: int) -> dict[str, list[str]]:
    by_bucket: dict[str, list[str]] = {}
    known = set(KNOWN_CANDIDATE_RULES)
    for bucket in [bucket.slug for bucket in BUCKETS]:
        rows = [row for row in bucket_rows if row.get("bucket") == bucket]
        rows.sort(key=lambda row: -(to_float(row.get("score")) or -10**9))
        selected: list[str] = []
        for row in rows:
            rule = str(row.get("rule_slug") or "")
            if rule and rule not in selected:
                selected.append(rule)
            if len(selected) >= top_rules_per_bucket:
                break
        for rule in KNOWN_CANDIDATE_RULES:
            if rule in known and rule not in selected:
                selected.append(rule)
        by_bucket[bucket] = selected[: max(top_rules_per_bucket, len(KNOWN_CANDIDATE_RULES))]
    return by_bucket


def make_policy(tiers: tuple[tuple[float, str], ...]) -> TieredPolicy:
    normalized = tuple(sorted(tiers, key=lambda item: item[0]))
    slug = "__".join(f"p{int(threshold)}_{rule}" for threshold, rule in normalized)
    description = "Base below first threshold; " + "; ".join(
        f"pump >= {threshold:g}% -> {rule}" for threshold, rule in normalized
    )
    return TieredPolicy(slug=slug, description=description, tiers=normalized)


def simulate_tiered_policy(
    *,
    cases: list[dict[str, Any]],
    outcomes: dict[tuple[str, str], dict[str, Any]],
    policy: TieredPolicy,
    split_ts: int,
    return_trades: bool = False,
) -> dict[str, Any]:
    active: list[dict[str, Any]] = []
    current_capital = CAPITAL_USD
    selected: list[dict[str, Any]] = []
    skipped_slots = 0
    skipped_same_symbol = 0
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
        rule_slug = choose_tier_rule(case, policy)
        outcome = outcomes.get((str(case.get("case_id")), rule_slug))
        if not outcome:
            skipped_missing_rule += 1
            continue
        symbol = str(case.get("symbol") or "")
        if any(item["symbol"] == symbol for item in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= SLOTS:
            skipped_slots += 1
            continue
        per_coin_capital = current_capital / SLOTS
        net_pct = to_float(outcome.get("net_reserved_pct")) or 0.0
        stress_pct = max(0.0, to_float(outcome.get("max_margin_stress_reserved_pct")) or 0.0)
        pnl_usd = per_coin_capital * LEVERAGE * net_pct / 100.0
        peak_loss_usd = per_coin_capital * stress_pct / 100.0
        topup_usd = max(0.0, peak_loss_usd - per_coin_capital)
        action = {
            "policy_slug": policy.slug,
            "description": policy.description,
            "slots": SLOTS,
            "symbol": symbol,
            "case_id": case.get("case_id"),
            "entry_ts": entry_ts,
            "entry_iso": ms_to_iso(entry_ts),
            "exit_ts": to_int(outcome.get("exit_ts")) or entry_ts,
            "exit_iso": ms_to_iso(to_int(outcome.get("exit_ts")) or entry_ts),
            "split": "test" if entry_ts >= split_ts else "train",
            "rule_slug": rule_slug,
            "pump_pct": rounded(f(case, "pump_pct")),
            "oi24_pct": rounded(f(case, "oi_change_24h_pct")),
            "funding_prev_24h_pct": rounded(f(case, "funding_prev_24h_pct")),
            "capital_before_entry_usd": rounded(current_capital),
            "per_coin_capital_usd": rounded(per_coin_capital),
            "net_pct": rounded(net_pct),
            "pnl_usd": rounded(pnl_usd),
            "stress_pct": rounded(stress_pct),
            "topup_usd": rounded(topup_usd),
            "exit_reason": outcome.get("exit_reason"),
        }
        selected.append(action)
        active.append(action)
    close_due(10**18)

    summary = build_summary(policy, selected, current_capital, split_ts, skipped_slots, skipped_same_symbol, skipped_missing_rule, skipped_insolvent)
    if return_trades:
        summary["selected_trades"] = selected
    return summary


def choose_tier_rule(case: dict[str, Any], policy: TieredPolicy) -> str:
    pump = f(case, "pump_pct")
    rule = BASE_RULE_SLUG
    for threshold, candidate in policy.tiers:
        if pump >= threshold:
            rule = candidate
    return rule


def build_summary(
    policy: TieredPolicy,
    selected: list[dict[str, Any]],
    final_capital: float,
    split_ts: int,
    skipped_slots: int,
    skipped_same_symbol: int,
    skipped_missing_rule: int,
    skipped_insolvent: int,
) -> dict[str, Any]:
    train = [row for row in selected if row["split"] == "train"]
    test = [row for row in selected if row["split"] == "test"]
    super_rows = [row for row in selected if (to_float(row.get("pump_pct")) or 0.0) >= 100.0]
    return {
        "policy_slug": policy.slug,
        "description": policy.description,
        "tier_count": len(policy.tiers),
        "tier_rules": json.dumps(policy.tiers, ensure_ascii=True),
        "capital_usd": CAPITAL_USD,
        "slots": SLOTS,
        "split_ts": split_ts,
        "split_iso": ms_to_iso(split_ts),
        "trades": len(selected),
        "super_trades": len(super_rows),
        "train_trades": len(train),
        "test_trades": len(test),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_missing_rule": skipped_missing_rule,
        "skipped_insolvent": skipped_insolvent,
        **metrics_for(selected, final_capital=final_capital),
        **{f"train_{key}": value for key, value in metrics_for(train).items()},
        **{f"test_{key}": value for key, value in metrics_for(test).items()},
        **{f"super_{key}": value for key, value in metrics_for(super_rows).items()},
    }


def metrics_for(rows: list[dict[str, Any]], *, final_capital: float | None = None) -> dict[str, Any]:
    pnl = values(rows, "pnl_usd")
    net = values(rows, "net_pct")
    topups = values(rows, "topup_usd")
    net_pnl = sum(pnl)
    effective_final = final_capital if final_capital is not None else CAPITAL_USD + net_pnl
    max_topup = max_concurrent_value(rows, "topup_usd")
    return {
        "final_capital_usd": rounded(effective_final),
        "net_pnl_usd": rounded(net_pnl),
        "roi_pct": rounded((effective_final - CAPITAL_USD) / CAPITAL_USD * 100.0),
        "risk_adjusted_roi_pct": rounded(net_pnl / (CAPITAL_USD + max_topup) * 100.0 if CAPITAL_USD + max_topup > 0 else 0.0),
        "win_rate_pct": rounded(sum(1 for value in pnl if value > 0) / len(pnl) * 100.0 if pnl else 0.0),
        "loss_trades": sum(1 for value in pnl if value < 0),
        "worst_trade_pnl_usd": rounded(min(pnl) if pnl else 0.0),
        "avg_net_pct": rounded(avg(net)),
        "median_net_pct": rounded(median(net)),
        "max_single_topup_usd": rounded(max(topups) if topups else 0.0),
        "max_concurrent_topup_usd": rounded(max_topup),
        "topup_events": sum(1 for value in topups if value > 0),
    }


def live_rank_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
    capital = to_float(row.get("capital_usd")) or CAPITAL_USD
    worst = to_float(row.get("worst_trade_pnl_usd")) or 0.0
    topup = to_float(row.get("max_concurrent_topup_usd")) or 0.0
    return (
        max(0.0, abs(min(0.0, worst)) / capital - 1.0),
        max(0.0, topup / capital - 2.5),
        -(to_float(row.get("test_risk_adjusted_roi_pct")) or -10**9),
        topup,
        -worst,
    )


def policy_from_summary_row(row: dict[str, Any]) -> TieredPolicy:
    tiers = tuple((float(threshold), str(rule)) for threshold, rule in json.loads(str(row["tier_rules"])))
    return TieredPolicy(slug=str(row["policy_slug"]), description=str(row.get("description") or ""), tiers=tiers)


def rule_score(row: dict[str, Any]) -> float:
    avg_net = to_float(row.get("avg_net_pct")) or 0.0
    worst = to_float(row.get("worst_net_pct")) or 0.0
    loss = to_float(row.get("loss_trades")) or 0.0
    avg_stress = to_float(row.get("avg_stress_pct")) or 0.0
    max_stress = to_float(row.get("max_stress_pct")) or 0.0
    hold = to_float(row.get("avg_hold_h")) or 0.0
    n = to_float(row.get("n")) or 1.0
    return avg_net + min(0.0, worst) * 0.40 - loss / n * 8.0 - avg_stress * 0.03 - max_stress * 0.01 - hold / 720.0


def case_in_bucket(case: dict[str, Any], bucket: PumpBucket) -> bool:
    pump = f(case, "pump_pct")
    return pump >= bucket.lo and (bucket.hi is None or pump < bucket.hi)


def train_test_split_ts(cases: list[dict[str, Any]]) -> int:
    values_ts = sorted(to_int(row.get("entry_ts")) or 0 for row in cases)
    return values_ts[int(len(values_ts) * 0.70)] if values_ts else 0


def render_html_report(
    *,
    cases: list[dict[str, Any]],
    super_cases: list[dict[str, Any]],
    bucket_rows: list[dict[str, Any]],
    summary_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    split_ts: int,
    start_ts_ms: int | None,
) -> str:
    content = f"""
    <section class="panel">
      <h1>Super Pump Tier Research</h1>
      <p>This report searches tiered rules for pump >= 100% cases, then replays the resulting policies on all 2024+ trades with $3000 dynamic capital and 4 simultaneous coins.</p>
      <div class="metrics">
        <div><b>{len(cases)}</b><span>all 2024+ entries</span></div>
        <div><b>{len(super_cases)}</b><span>pump >= 100 entries</span></div>
        <div><b>{len(summary_rows)}</b><span>tiered policies</span></div>
        <div><b>{ms_to_iso(start_ts_ms)}</b><span>start filter</span></div>
        <div><b>{ms_to_iso(split_ts)}</b><span>test starts</span></div>
      </div>
    </section>
    <section class="panel"><h2>Human Read</h2>{human_read(summary_rows)}</section>
    <section class="panel"><h2>Best Tiered Portfolio Policies</h2>{html_table(summary_rows[:80], summary_columns())}</section>
    <section class="panel"><h2>Super Pump Bucket Counts</h2>{html_table(bucket_counts(super_cases), ("bucket","count","min_pump","max_pump","symbols"))}</section>
    <section class="panel"><h2>Best Rules By Super-Pump Bucket</h2>{html_table(top_bucket_rules(bucket_rows, limit_per_bucket=20), bucket_columns())}</section>
    <section class="panel"><h2>Worst Trades In Top Policies</h2>{html_table(worst_trades(trade_rows, 80), trade_columns())}</section>
    <section class="panel"><h2>Selected Trades From Top Policies</h2>{html_table(trade_rows[:500], trade_columns())}</section>
    """
    return page_shell("Super Pump Tier Research", content)


def human_read(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    best = rows[0]
    filtered = [
        row
        for row in rows
        if (to_float(row.get("max_concurrent_topup_usd")) or 0.0) <= 9_000.0
        and (to_float(row.get("worst_trade_pnl_usd")) or 0.0) >= -3_000.0
    ]
    parts = [
        f"<p>Best by practical ranking: <b>{esc(best.get('policy_slug'))}</b>, final ${esc(best.get('final_capital_usd'))}, "
        f"test risk-adjusted ROI {esc(best.get('test_risk_adjusted_roi_pct'))}%, max top-up ${esc(best.get('max_concurrent_topup_usd'))}, "
        f"worst trade ${esc(best.get('worst_trade_pnl_usd'))}.</p>"
    ]
    if filtered:
        clean = filtered[0]
        parts.append(
            f"<p>Best row under the old live-style filter max top-up <= $9000 and worst >= -$3000: "
            f"<b>{esc(clean.get('policy_slug'))}</b>, final ${esc(clean.get('final_capital_usd'))}, "
            f"test risk-adjusted ROI {esc(clean.get('test_risk_adjusted_roi_pct'))}%.</p>"
        )
    else:
        parts.append("<p>No tiered policy passed max top-up <= $9000 and worst >= -$3000.</p>")
    parts.append(
        "<p>Important: bucket rules are selected from historical outcomes. Treat tiers above 250% and 400% as research/shadow candidates because sample counts are small.</p>"
    )
    return "\n".join(parts)


def bucket_counts(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for bucket in BUCKETS[:-1]:
        items = [case for case in cases if case_in_bucket(case, bucket)]
        pumps = [f(case, "pump_pct") for case in items]
        rows.append(
            {
                "bucket": bucket.slug,
                "count": len(items),
                "min_pump": rounded(min(pumps) if pumps else 0.0),
                "max_pump": rounded(max(pumps) if pumps else 0.0),
                "symbols": ", ".join(sorted({str(case.get("symbol")) for case in items})[:20]),
            }
        )
    return rows


def top_bucket_rules(rows: list[dict[str, Any]], *, limit_per_bucket: int) -> list[dict[str, Any]]:
    out = []
    for bucket in [bucket.slug for bucket in BUCKETS]:
        items = [row for row in rows if row.get("bucket") == bucket]
        items.sort(key=lambda row: -(to_float(row.get("score")) or -10**9))
        out.extend(items[:limit_per_bucket])
    return out


def worst_trades(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: to_float(row.get("pnl_usd")) or 0.0)[:limit]


def summary_columns() -> tuple[str, ...]:
    return (
        "rank",
        "policy_slug",
        "tier_count",
        "trades",
        "super_trades",
        "test_trades",
        "final_capital_usd",
        "risk_adjusted_roi_pct",
        "test_risk_adjusted_roi_pct",
        "max_concurrent_topup_usd",
        "worst_trade_pnl_usd",
        "super_avg_net_pct",
        "super_worst_trade_pnl_usd",
        "loss_trades",
    )


def bucket_columns() -> tuple[str, ...]:
    return (
        "bucket",
        "case_count",
        "rule_slug",
        "score",
        "avg_net_pct",
        "median_net_pct",
        "win_rate_pct",
        "worst_net_pct",
        "avg_stress_pct",
        "max_stress_pct",
        "avg_hold_h",
    )


def trade_columns() -> tuple[str, ...]:
    return (
        "rank",
        "policy_slug",
        "symbol",
        "entry_iso",
        "rule_slug",
        "pump_pct",
        "oi24_pct",
        "funding_prev_24h_pct",
        "net_pct",
        "pnl_usd",
        "topup_usd",
        "capital_before_entry_usd",
        "exit_reason",
    )


def html_table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    head = "".join(f"<th>{esc(column)}</th>" for column in columns)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{esc(row.get(column, ''))}</td>" for column in columns) + "</tr>")
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
    .wrap {{ max-width: 1380px; margin: 0 auto; padding: 22px; }}
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
  </style>
</head>
<body><main class="wrap">{content}</main></body>
</html>"""


def values(rows: Iterable[dict[str, Any]], key: str) -> list[float]:
    out = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None and math.isfinite(value):
            out.append(value)
    return out


def avg(items: list[float]) -> float:
    return sum(items) / len(items) if items else 0.0


def median(items: list[float]) -> float:
    if not items:
        return 0.0
    sorted_items = sorted(items)
    mid = len(sorted_items) // 2
    if len(sorted_items) % 2:
        return sorted_items[mid]
    return (sorted_items[mid - 1] + sorted_items[mid]) / 2.0


def f(row: dict[str, Any], key: str) -> float:
    return to_float(row.get(key)) or 0.0


def rounded(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(float(value), digits)


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))
