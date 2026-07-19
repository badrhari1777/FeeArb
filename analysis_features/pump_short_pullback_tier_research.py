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

from analysis_features.bybit_pump_short_grid_research import base_research_row, resolve_entry_idx, simulate_ladder_rule
from analysis_features.bybit_pump_short_outcomes import detect_pump_events, load_samples, sample_to_series
from analysis_features.pump_short_dynamic_combo_report import parse_date_to_ms
from analysis_features.pump_short_per_event_strategy_research import (
    BASE_RULE_SLUG,
    LEVERAGE,
    RuleConfig,
    balanced_score,
    build_rule_configs,
    outcome_row,
    safe_id,
)
from analysis_features.pump_short_policy_portfolio_research import (
    DEFAULT_INPUT_DIR,
    max_concurrent_value,
    ms_to_iso,
    to_float,
    to_int,
    write_csv,
)
from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_pullback_tier_research"
CAPITAL_USD = 3_000.0
SLOTS = 4
PULLBACKS = (10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0)
START_DATE = "2024-01-01"
TOP_CANDIDATES_PER_BUCKET = 8


@dataclass(frozen=True, slots=True)
class PumpBucket:
    slug: str
    lo: float
    hi: float | None


@dataclass(frozen=True, slots=True)
class EntryRule:
    pullback_pct: float
    rule_slug: str


@dataclass(frozen=True, slots=True)
class PullbackPolicy:
    slug: str
    description: str
    tiers: tuple[tuple[float, EntryRule], ...]


BUCKETS = (
    PumpBucket("p000_080", 0.0, 80.0),
    PumpBucket("p080_100", 80.0, 100.0),
    PumpBucket("p100_150", 100.0, 150.0),
    PumpBucket("p150_250", 150.0, 250.0),
    PumpBucket("p250_plus", 250.0, None),
)


KNOWN_RULES = (
    BASE_RULE_SLUG,
    "step50_legs3_tapered_tp25_336",
    "step50_legs2_tapered_tp25_720",
    "step50_legs3_tapered_tp25_720",
    "step50_legs4_equal_tp25_168",
    "step50_legs4_equal_tp25_336",
    "step50_legs4_equal_tp25_720",
    "step50_legs5_equal_tp25_720",
)


def run_pullback_tier_research(
    *,
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    start_ts_ms: int | None = None,
    top_candidates_per_bucket: int = TOP_CANDIDATES_PER_BUCKET,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    start_ts_ms = start_ts_ms if start_ts_ms is not None else parse_date_to_ms(START_DATE)

    rules = build_rule_configs()
    rule_by_slug = {rule.slug: rule for rule in rules}
    event_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    symbols_seen = 0
    pump_events = 0

    for sample in load_samples(input_path):
        symbols_seen += 1
        series = sample_to_series(sample)
        ts_to_idx = {ts_ms: idx for idx, ts_ms in enumerate(series.ts)}
        events = detect_pump_events(series)
        pump_events += len(events)
        for event in events:
            base_row = base_research_row(series, event)
            event_id = safe_id(str(base_row["event_id"]))
            event_rows.append(event_case_row(base_row, event_id))
            for pb in PULLBACKS:
                setup = entry_setup(pb)
                entry_idx = resolve_entry_idx(series, event, setup)
                if entry_idx is None:
                    continue
                case_id = f"{event_id}__pb{int(pb)}"
                for rule in rules:
                    row = simulate_ladder_rule(
                        series,
                        event,
                        base_row,
                        entry_setup=str(setup["name"]),
                        entry_idx=entry_idx,
                        step_pct=rule.step_pct,
                        max_legs=rule.max_legs,
                        add_window_h=168,
                        sizing_mode=rule.sizing_mode,
                        exit_plan=rule.exit_plan,
                    )
                    if not row:
                        continue
                    out = outcome_row(row, rule, case_id, ts_to_idx)
                    out["event_uid"] = event_id
                    out["pullback_pct"] = pb
                    out["entry_setup"] = setup["name"]
                    out["balanced_score"] = balanced_score(out)
                    outcome_rows.append(out)

    bucket_rows = build_bucket_summary(outcome_rows)
    policy_rows, trade_rows = build_and_run_policies(
        event_rows=event_rows,
        outcome_rows=outcome_rows,
        bucket_rows=bucket_rows,
        start_ts_ms=start_ts_ms,
        top_candidates_per_bucket=top_candidates_per_bucket,
    )

    write_csv(output_dir / "pullback_event_summary.csv", event_rows)
    write_csv(output_dir / "pullback_all_outcomes.csv", outcome_rows)
    write_csv(output_dir / "pullback_bucket_rule_summary.csv", bucket_rows)
    write_csv(output_dir / "pullback_policy_summary.csv", policy_rows)
    write_csv(output_dir / "pullback_policy_trades.csv", trade_rows)
    (output_dir / "index.html").write_text(
        render_html_report(
            event_rows=event_rows,
            outcome_rows=outcome_rows,
            bucket_rows=bucket_rows,
            policy_rows=policy_rows,
            trade_rows=trade_rows,
            start_ts_ms=start_ts_ms,
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema": "pump_short_pullback_tier_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "pump_events": pump_events,
        "event_rows": len(event_rows),
        "pullbacks": list(PULLBACKS),
        "rules": len(rules),
        "outcome_rows": len(outcome_rows),
        "bucket_rows": len(bucket_rows),
        "policy_rows": len(policy_rows),
        "trade_rows": len(trade_rows),
        "start_ts": start_ts_ms,
        "start_iso": ms_to_iso(start_ts_ms),
        "capital_usd": CAPITAL_USD,
        "slots": SLOTS,
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def entry_setup(pb: float) -> dict[str, Any]:
    return {
        "name": f"pb{int(pb)}_oi50_lr_mid",
        "kind": "confirmed_pullback",
        "pullback_pct": pb,
        "oi_max_pct": 50.0,
    }


def event_case_row(base_row: dict[str, Any], event_uid: str) -> dict[str, Any]:
    return {
        "event_uid": event_uid,
        "event_id": base_row.get("event_id"),
        "symbol": base_row.get("symbol"),
        "trigger_ts": base_row.get("trigger_ts"),
        "trigger_iso": ms_to_iso(base_row.get("trigger_ts")),
        "pump_pct": rounded(to_float(base_row.get("pump_pct"))),
        "config_window_h": base_row.get("config_window_h"),
        "config_threshold_pct": base_row.get("config_threshold_pct"),
        "age_days": rounded(to_float(base_row.get("age_days"))),
        "funding_prev_24h_pct": rounded(to_float(base_row.get("funding_prev_24h_pct"))),
        "oi_change_24h_pct": rounded(to_float(base_row.get("oi_change_24h_pct"))),
        "long_ratio": rounded(to_float(base_row.get("long_ratio"))),
    }


def build_bucket_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, float, str], list[dict[str, Any]]] = {}
    for row in rows:
        pump = to_float(row.get("pump_pct")) or 0.0
        bucket = bucket_for_pump(pump).slug
        key = (bucket, float(to_float(row.get("pullback_pct")) or 0.0), str(row.get("rule_slug") or ""))
        groups.setdefault(key, []).append(row)
    out: list[dict[str, Any]] = []
    for (bucket, pb, rule), items in groups.items():
        net = values(items, "net_reserved_pct")
        stress = values(items, "max_margin_stress_reserved_pct")
        hold = values(items, "time_in_trade_h")
        row = {
            "bucket": bucket,
            "pullback_pct": pb,
            "rule_slug": rule,
            "n": len(items),
            "avg_net_pct": rounded(avg(net)),
            "median_net_pct": rounded(median(net)),
            "win_rate_pct": rounded(sum(1 for value in net if value > 0) / len(net) * 100.0 if net else 0.0),
            "loss_trades": sum(1 for value in net if value < 0),
            "worst_net_pct": rounded(min(net) if net else 0.0),
            "avg_stress_pct": rounded(avg(stress)),
            "max_stress_pct": rounded(max(stress) if stress else 0.0),
            "avg_hold_h": rounded(avg(hold)),
        }
        row["score"] = rounded(rule_score(row))
        out.append(row)
    out.sort(key=lambda row: (str(row["bucket"]), -(to_float(row.get("score")) or -10**9)))
    return out


def build_and_run_policies(
    *,
    event_rows: list[dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
    bucket_rows: list[dict[str, Any]],
    start_ts_ms: int | None,
    top_candidates_per_bucket: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = candidates_by_bucket(bucket_rows, top_candidates_per_bucket)
    policies = build_policies(candidates)
    outcomes = {
        (str(row.get("event_uid")), int(float(row.get("pullback_pct") or 0.0)), str(row.get("rule_slug"))): row
        for row in outcome_rows
    }
    filtered_events = [
        row for row in event_rows if (to_int(row.get("trigger_ts")) or 0) >= (start_ts_ms or 0)
    ]
    split_ts = train_test_split_ts(filtered_events)
    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    for policy in policies:
        result = simulate_policy(filtered_events, outcomes, policy, split_ts)
        summary_rows.append({key: value for key, value in result.items() if key != "selected_trades"})
    summary_rows.sort(key=live_rank_key)
    for idx, row in enumerate(summary_rows, start=1):
        row["rank"] = idx
    for row in summary_rows[:50]:
        policy = policy_from_row(row)
        result = simulate_policy(filtered_events, outcomes, policy, split_ts, return_trades=True)
        for trade in result["selected_trades"]:
            trade_rows.append({"rank": row["rank"], **trade})
    return summary_rows, trade_rows


def candidates_by_bucket(rows: list[dict[str, Any]], limit: int) -> dict[str, list[EntryRule]]:
    out: dict[str, list[EntryRule]] = {}
    known = [EntryRule(20.0, rule) for rule in KNOWN_RULES]
    known.extend([EntryRule(pb, BASE_RULE_SLUG) for pb in PULLBACKS])
    for bucket in [bucket.slug for bucket in BUCKETS]:
        selected: list[EntryRule] = []
        items = [row for row in rows if row.get("bucket") == bucket]
        items.sort(key=lambda row: -(to_float(row.get("score")) or -10**9))
        for row in items:
            candidate = EntryRule(float(to_float(row.get("pullback_pct")) or 20.0), str(row.get("rule_slug")))
            if candidate not in selected:
                selected.append(candidate)
            if len(selected) >= limit:
                break
        for candidate in known:
            if candidate not in selected:
                selected.append(candidate)
        out[bucket] = selected[: max(limit, len(known))]
    return out


def build_policies(candidates: dict[str, list[EntryRule]]) -> list[PullbackPolicy]:
    policies: dict[str, PullbackPolicy] = {}

    def add(policy: PullbackPolicy) -> None:
        policies.setdefault(policy.slug, policy)

    for base in candidates["p000_080"]:
        add(make_policy(((0.0, base),)))

    for base, p100 in itertools.product(candidates["p000_080"], candidates["p100_150"]):
        add(make_policy(((0.0, base), (100.0, p100))))

    for base, p100, p250 in itertools.product(candidates["p000_080"], candidates["p100_150"], candidates["p250_plus"]):
        add(make_policy(((0.0, base), (100.0, p100), (250.0, p250))))

    for base, p80, p100, p250 in itertools.product(
        candidates["p000_080"],
        candidates["p080_100"],
        candidates["p100_150"],
        candidates["p250_plus"],
    ):
        add(make_policy(((0.0, base), (80.0, p80), (100.0, p100), (250.0, p250))))

    add(make_policy(((0.0, EntryRule(20.0, BASE_RULE_SLUG)), (100.0, EntryRule(20.0, "step50_legs3_tapered_tp25_336")))))
    add(
        make_policy(
            (
                (0.0, EntryRule(20.0, BASE_RULE_SLUG)),
                (100.0, EntryRule(20.0, "step50_legs3_tapered_tp25_336")),
                (250.0, EntryRule(25.0, "step50_legs2_tapered_tp25_720")),
            )
        )
    )
    return list(policies.values())


def make_policy(tiers: tuple[tuple[float, EntryRule], ...]) -> PullbackPolicy:
    normalized = tuple(sorted(tiers, key=lambda item: item[0]))
    slug = "__".join(f"p{int(th)}_pb{int(rule.pullback_pct)}_{rule.rule_slug}" for th, rule in normalized)
    description = "; ".join(f"pump>={th:g}: pb{int(rule.pullback_pct)} {rule.rule_slug}" for th, rule in normalized)
    return PullbackPolicy(slug=slug, description=description, tiers=normalized)


def simulate_policy(
    event_rows: list[dict[str, Any]],
    outcomes: dict[tuple[str, int, str], dict[str, Any]],
    policy: PullbackPolicy,
    split_ts: int,
    *,
    return_trades: bool = False,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    skipped_missing_rule = 0
    for event in event_rows:
        entry_rule = choose_entry_rule(event, policy)
        outcome = outcomes.get((str(event.get("event_uid")), int(entry_rule.pullback_pct), entry_rule.rule_slug))
        if not outcome:
            skipped_missing_rule += 1
            continue
        candidates.append({"event": event, "outcome": outcome, "entry_rule": entry_rule})
    candidates.sort(key=lambda item: (to_int(item["outcome"].get("entry_ts")) or 0, str(item["event"].get("symbol") or "")))

    active: list[dict[str, Any]] = []
    current_capital = CAPITAL_USD
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

    for item in candidates:
        event = item["event"]
        outcome = item["outcome"]
        entry_rule = item["entry_rule"]
        entry_ts = to_int(outcome.get("entry_ts")) or 0
        close_due(entry_ts)
        if current_capital <= 0:
            skipped_insolvent += 1
            continue
        symbol = str(event.get("symbol") or "")
        if any(active_item["symbol"] == symbol for active_item in active):
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
            "symbol": symbol,
            "event_uid": event.get("event_uid"),
            "entry_ts": entry_ts,
            "entry_iso": ms_to_iso(entry_ts),
            "exit_ts": to_int(outcome.get("exit_ts")) or entry_ts,
            "exit_iso": ms_to_iso(to_int(outcome.get("exit_ts")) or entry_ts),
            "split": "test" if entry_ts >= split_ts else "train",
            "pullback_pct": entry_rule.pullback_pct,
            "rule_slug": entry_rule.rule_slug,
            "pump_pct": rounded(to_float(event.get("pump_pct"))),
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


def choose_entry_rule(event: dict[str, Any], policy: PullbackPolicy) -> EntryRule:
    pump = to_float(event.get("pump_pct")) or 0.0
    rule = policy.tiers[0][1]
    for threshold, candidate in policy.tiers:
        if pump >= threshold:
            rule = candidate
    return rule


def build_summary(
    policy: PullbackPolicy,
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
    return {
        "policy_slug": policy.slug,
        "description": policy.description,
        "tier_count": len(policy.tiers),
        "tier_rules": json.dumps([(threshold, rule.pullback_pct, rule.rule_slug) for threshold, rule in policy.tiers], ensure_ascii=True),
        "capital_usd": CAPITAL_USD,
        "slots": SLOTS,
        "split_ts": split_ts,
        "split_iso": ms_to_iso(split_ts),
        "trades": len(selected),
        "train_trades": len(train),
        "test_trades": len(test),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_missing_rule": skipped_missing_rule,
        "skipped_insolvent": skipped_insolvent,
        **metrics_for(selected, final_capital=final_capital),
        **{f"train_{key}": value for key, value in metrics_for(train).items()},
        **{f"test_{key}": value for key, value in metrics_for(test).items()},
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


def policy_from_row(row: dict[str, Any]) -> PullbackPolicy:
    tiers = tuple((float(th), EntryRule(float(pb), str(rule))) for th, pb, rule in json.loads(str(row["tier_rules"])))
    return PullbackPolicy(slug=str(row["policy_slug"]), description=str(row.get("description") or ""), tiers=tiers)


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


def rule_score(row: dict[str, Any]) -> float:
    avg_net = to_float(row.get("avg_net_pct")) or 0.0
    worst = to_float(row.get("worst_net_pct")) or 0.0
    loss = to_float(row.get("loss_trades")) or 0.0
    n = to_float(row.get("n")) or 1.0
    avg_stress = to_float(row.get("avg_stress_pct")) or 0.0
    max_stress = to_float(row.get("max_stress_pct")) or 0.0
    hold = to_float(row.get("avg_hold_h")) or 0.0
    return avg_net + min(0.0, worst) * 0.35 - loss / n * 7.0 - avg_stress * 0.03 - max_stress * 0.01 - hold / 720.0


def bucket_for_pump(pump: float) -> PumpBucket:
    for bucket in BUCKETS:
        if pump >= bucket.lo and (bucket.hi is None or pump < bucket.hi):
            return bucket
    return BUCKETS[-1]


def train_test_split_ts(rows: list[dict[str, Any]]) -> int:
    values_ts = sorted(to_int(row.get("trigger_ts")) or 0 for row in rows)
    return values_ts[int(len(values_ts) * 0.70)] if values_ts else 0


def render_html_report(
    *,
    event_rows: list[dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
    bucket_rows: list[dict[str, Any]],
    policy_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    start_ts_ms: int | None,
) -> str:
    content = f"""
    <section class="panel">
      <h1>Pullback Tier Research</h1>
      <p>This report varies confirmed pullback depth pb10..pb40, ranks pullback+ladder rules by pump-strength buckets, then replays tiered policies on $3000 dynamic capital with 4 simultaneous coins.</p>
      <div class="metrics">
        <div><b>{len(event_rows)}</b><span>pump events</span></div>
        <div><b>{len(outcome_rows)}</b><span>pullback outcomes</span></div>
        <div><b>{len(policy_rows)}</b><span>portfolio policies</span></div>
        <div><b>{ms_to_iso(start_ts_ms)}</b><span>portfolio start filter</span></div>
      </div>
    </section>
    <section class="panel"><h2>Human Read</h2>{human_read(policy_rows)}</section>
    <section class="panel"><h2>Best Portfolio Policies</h2>{html_table(policy_rows[:80], policy_columns())}</section>
    <section class="panel"><h2>Best Pullback Rules By Pump Bucket</h2>{html_table(top_bucket_rows(bucket_rows, 30), bucket_columns())}</section>
    <section class="panel"><h2>Pullback Depth Summary</h2>{html_table(pullback_depth_summary(bucket_rows), ("bucket","pullback_pct","best_rule_slug","best_score","best_avg_net_pct","best_worst_net_pct","best_win_rate_pct"))}</section>
    <section class="panel"><h2>Worst Trades In Top Policies</h2>{html_table(worst_trades(trade_rows, 100), trade_columns())}</section>
    """
    return page_shell("Pullback Tier Research", content)


def human_read(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    best = rows[0]
    filtered = [row for row in rows if (to_float(row.get("max_concurrent_topup_usd")) or 0.0) <= 9000 and (to_float(row.get("worst_trade_pnl_usd")) or 0.0) >= -3000]
    text = [
        f"<p>Best practical-ranked policy: <b>{esc(best.get('policy_slug'))}</b>, final ${esc(best.get('final_capital_usd'))}, "
        f"test risk-adjusted ROI {esc(best.get('test_risk_adjusted_roi_pct'))}%, max top-up ${esc(best.get('max_concurrent_topup_usd'))}, worst ${esc(best.get('worst_trade_pnl_usd'))}.</p>"
    ]
    if filtered:
        clean = filtered[0]
        text.append(
            f"<p>Best under max top-up <= $9000 and worst >= -$3000: <b>{esc(clean.get('policy_slug'))}</b>, "
            f"final ${esc(clean.get('final_capital_usd'))}, test risk-adjusted ROI {esc(clean.get('test_risk_adjusted_roi_pct'))}%.</p>"
        )
    else:
        text.append("<p>No policy passed max top-up <= $9000 and worst >= -$3000.</p>")
    text.append("<p>Pullback tiers are selected from historical data; use them first in shadow/paper before live.</p>")
    return "\n".join(text)


def top_bucket_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    out = []
    for bucket in [bucket.slug for bucket in BUCKETS]:
        items = [row for row in rows if row.get("bucket") == bucket]
        items.sort(key=lambda row: -(to_float(row.get("score")) or -10**9))
        out.extend(items[:limit])
    return out


def pullback_depth_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for bucket in [bucket.slug for bucket in BUCKETS]:
        for pb in PULLBACKS:
            items = [row for row in rows if row.get("bucket") == bucket and int(float(row.get("pullback_pct") or 0)) == int(pb)]
            if not items:
                continue
            best = sorted(items, key=lambda row: -(to_float(row.get("score")) or -10**9))[0]
            out.append(
                {
                    "bucket": bucket,
                    "pullback_pct": pb,
                    "best_rule_slug": best.get("rule_slug"),
                    "best_score": best.get("score"),
                    "best_avg_net_pct": best.get("avg_net_pct"),
                    "best_worst_net_pct": best.get("worst_net_pct"),
                    "best_win_rate_pct": best.get("win_rate_pct"),
                }
            )
    return out


def worst_trades(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: to_float(row.get("pnl_usd")) or 0.0)[:limit]


def policy_columns() -> tuple[str, ...]:
    return (
        "rank",
        "policy_slug",
        "tier_count",
        "trades",
        "test_trades",
        "final_capital_usd",
        "risk_adjusted_roi_pct",
        "test_risk_adjusted_roi_pct",
        "max_concurrent_topup_usd",
        "worst_trade_pnl_usd",
        "loss_trades",
    )


def bucket_columns() -> tuple[str, ...]:
    return (
        "bucket",
        "pullback_pct",
        "rule_slug",
        "n",
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
        "pullback_pct",
        "rule_slug",
        "pump_pct",
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
    ordered = sorted(items)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0


def rounded(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(float(value), digits)


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))
