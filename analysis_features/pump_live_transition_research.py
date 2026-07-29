from __future__ import annotations

import csv
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_features.pump_short_policy_portfolio_research import build_unique_cases, load_csv
from analysis_features.pump_cycle_portfolio_report import (
    ALLOCATIONS as CYCLE_ALLOCATIONS,
    DEFAULT_LONG_OUTCOMES,
    DEFAULT_SHORT_TRADES,
    LONG_TRACKS,
    SHORT_TRACKS,
    load_long_candidates,
    load_short_candidates,
    read_csv as read_cycle_csv,
    replay_cycle,
)
from config import BASE_DIR

START_TS_MS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
LEVERAGE = 3.0
FEE_NOTE = "Historical net_pct comes from the existing pump outcome engine and includes its fee/funding model."
DEFAULT_PER_EVENT_DIR = BASE_DIR / "data" / "research" / "pump_short_per_event_strategy_research"
DEFAULT_PULLBACK_DIR = BASE_DIR / "data" / "research" / "pump_short_pullback_tier_research"
DEFAULT_PAPER_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_shadow"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_live_transition_research"


@dataclass(frozen=True)
class Tier:
    min_pump_pct: float
    pullback_pct: float
    rule_slug: str


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    funding_min_pct: float
    oi_max_pct: float
    long_ratio_min: float
    long_ratio_max: float
    tiers: tuple[Tier, ...]
    min_entry_pump_pct: float = 0.0
    mode: str = "monitor"


STRATEGIES: tuple[StrategySpec, ...] = (
    StrategySpec(
        "main_pullback_tier",
        -1.0,
        50.0,
        0.45,
        0.65,
        (
            Tier(0.0, 25.0, "step50_legs5_equal_tp25_720"),
            Tier(80.0, 20.0, "step50_legs2_tapered_tp25_720"),
            Tier(100.0, 20.0, "step50_legs3_tapered_tp25_336"),
            Tier(250.0, 20.0, "step50_legs2_tapered_tp25_720"),
        ),
        mode="primary",
    ),
    StrategySpec(
        "conservative_control",
        -0.5,
        50.0,
        0.45,
        0.65,
        (
            Tier(0.0, 20.0, "step50_legs4_equal_tp25_168"),
            Tier(100.0, 20.0, "step50_legs3_tapered_tp25_336"),
        ),
    ),
    StrategySpec(
        "super_pump_shadow",
        -1.0,
        50.0,
        0.45,
        0.65,
        (
            Tier(0.0, 20.0, "step50_legs4_equal_tp25_168"),
            Tier(100.0, 20.0, "step50_legs3_tapered_tp25_336"),
            Tier(250.0, 20.0, "step50_legs2_tapered_tp25_720"),
        ),
    ),
    StrategySpec(
        "pb20_baseline",
        -0.5,
        50.0,
        0.45,
        0.65,
        (Tier(0.0, 20.0, "step50_legs4_equal_tp25_168"),),
    ),
    StrategySpec(
        "pb25_deeper_pullback",
        -1.0,
        50.0,
        0.45,
        0.65,
        (Tier(0.0, 25.0, "step50_legs5_equal_tp25_720"),),
    ),
    StrategySpec(
        "short_clean_p100_l3_shadow",
        -1.0,
        50.0,
        0.45,
        0.65,
        (Tier(100.0, 20.0, "step50_legs3_tapered_tp25_336"),),
        min_entry_pump_pct=100.0,
        mode="candidate",
    ),
    StrategySpec(
        "short_super_250_shadow",
        -1.0,
        50.0,
        0.45,
        0.65,
        (Tier(250.0, 20.0, "step50_legs2_tapered_tp25_720"),),
        min_entry_pump_pct=250.0,
        mode="candidate",
    ),
)

CAPITAL_SCENARIOS: tuple[tuple[float, float], ...] = (
    (3_000.0, 0.0),
    (5_000.0, 1_000.0),
    (5_000.0, 1_500.0),
    (7_500.0, 1_500.0),
    (10_000.0, 2_000.0),
)
SIZING_MODES = ("fixed", "full_dynamic", "reserve_stair_25_cap2")


def run_pump_live_transition_research(
    *,
    per_event_dir: Path = DEFAULT_PER_EVENT_DIR,
    pullback_dir: Path = DEFAULT_PULLBACK_DIR,
    paper_dir: Path = DEFAULT_PAPER_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)

    cases = [
        row
        for row in build_unique_cases(load_csv(per_event_dir / "per_event_summary.csv"))
        if to_int(row.get("entry_ts")) >= START_TS_MS
    ]
    cases.sort(key=lambda row: (to_int(row.get("entry_ts")), str(row.get("symbol") or "")))
    split_ts = percentile_timestamp([to_int(row.get("entry_ts")) for row in cases], 0.70)
    wanted = wanted_outcome_keys(cases, STRATEGIES)
    outcomes = load_selected_pullback_outcomes(pullback_dir / "pullback_all_outcomes.csv", wanted)

    candidate_rows: dict[str, list[dict[str, Any]]] = {}
    history_summary: list[dict[str, Any]] = []
    history_trades: list[dict[str, Any]] = []
    for spec in STRATEGIES:
        candidates = build_strategy_candidates(spec, cases, outcomes, split_ts)
        candidate_rows[spec.strategy_id] = candidates
        result = simulate_portfolio(
            candidates,
            strategy_id=spec.strategy_id,
            total_capital_usd=3_000.0,
            reserve_usd=0.0,
            slots=4,
            sizing_mode="fixed",
            split_ts=split_ts,
        )
        history_summary.append(result["summary"])
        history_trades.extend(result["trades"])

    money_rows: list[dict[str, Any]] = []
    money_trades: list[dict[str, Any]] = []
    for strategy_id in ("main_pullback_tier", "short_clean_p100_l3_shadow", "short_super_250_shadow"):
        candidates = candidate_rows[strategy_id]
        for total_capital, reserve in CAPITAL_SCENARIOS:
            for slots in (3, 4):
                for sizing_mode in SIZING_MODES:
                    result = simulate_portfolio(
                        candidates,
                        strategy_id=strategy_id,
                        total_capital_usd=total_capital,
                        reserve_usd=reserve,
                        slots=slots,
                        sizing_mode=sizing_mode,
                        split_ts=split_ts,
                    )
                    money_rows.append(result["summary"])
                    money_trades.extend(result["trades"])

    paper_rows, paper_scenarios = load_paper_results(paper_dir)
    cycle_reference = build_cycle_reference_since_2024()
    capability_rows = subaccount_capability_rows()

    write_csv(output_dir / "historical_strategy_summary.csv", history_summary)
    write_csv(output_dir / "historical_strategy_trades.csv", history_trades)
    write_csv(output_dir / "money_management_summary.csv", money_rows)
    write_csv(output_dir / "money_management_trades.csv", money_trades)
    write_csv(output_dir / "paper_strategy_summary.csv", paper_rows)
    write_csv(output_dir / "paper_short_scaling_scenarios.csv", paper_scenarios)
    write_csv(output_dir / "historical_cycle_reference_since_2024.csv", cycle_reference)
    write_csv(output_dir / "bybit_subaccount_capabilities.csv", capability_rows)

    report_path = output_dir / "index.md"
    report_path.write_text(
        render_markdown_report(
            cases=cases,
            outcomes=outcomes,
            split_ts=split_ts,
            history_summary=history_summary,
            money_rows=money_rows,
            paper_rows=paper_rows,
            paper_scenarios=paper_scenarios,
            cycle_reference=cycle_reference,
            capability_rows=capability_rows,
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema": "pump_live_transition_research_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "start_ts": START_TS_MS,
        "start_iso": ms_to_iso(START_TS_MS),
        "actual_case_entry_min_iso": ms_to_iso(min((to_int(row.get("entry_ts")) for row in cases), default=0)),
        "actual_case_entry_max_iso": ms_to_iso(max((to_int(row.get("entry_ts")) for row in cases), default=0)),
        "split_ts": split_ts,
        "split_iso": ms_to_iso(split_ts),
        "unique_cases": len(cases),
        "selected_outcomes": len(outcomes),
        "strategies": [item.strategy_id for item in STRATEGIES],
        "history_summary_rows": len(history_summary),
        "history_trade_rows": len(history_trades),
        "money_summary_rows": len(money_rows),
        "money_trade_rows": len(money_trades),
        "paper_rows": len(paper_rows),
        "cycle_reference_rows": len(cycle_reference),
        "report_path": str(report_path),
        "elapsed_sec": round(time.time() - started, 3),
        "limitations": [
            "current-listing survivor bias",
            "reconstructed hourly entries",
            "paper sample is short",
            "live slippage and transfer latency are not simulated",
        ],
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def wanted_outcome_keys(
    cases: Iterable[dict[str, Any]],
    strategies: Iterable[StrategySpec],
) -> set[tuple[str, int, str]]:
    wanted: set[tuple[str, int, str]] = set()
    for case in cases:
        case_id = str(case.get("case_id") or "")
        pump_pct = to_float(case.get("pump_pct"))
        if not case_id or pump_pct is None:
            continue
        for spec in strategies:
            tier = select_tier(spec, pump_pct)
            if tier is not None:
                wanted.add((case_id, int(tier.pullback_pct), tier.rule_slug))
    return wanted


def load_selected_pullback_outcomes(
    path: Path,
    wanted: set[tuple[str, int, str]],
) -> dict[tuple[str, int, str], dict[str, Any]]:
    out: dict[tuple[str, int, str], dict[str, Any]] = {}
    if not wanted:
        return out
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (
                str(row.get("event_uid") or ""),
                int(to_float(row.get("pullback_pct")) or 0),
                str(row.get("rule_slug") or ""),
            )
            if key in wanted:
                out[key] = row
                if len(out) == len(wanted):
                    break
    return out


def select_tier(spec: StrategySpec, pump_pct: float) -> Tier | None:
    if pump_pct < spec.min_entry_pump_pct:
        return None
    eligible = [tier for tier in spec.tiers if pump_pct >= tier.min_pump_pct]
    return max(eligible, key=lambda tier: tier.min_pump_pct) if eligible else None


def passes_online_gates(spec: StrategySpec, case: dict[str, Any]) -> tuple[bool, str]:
    funding = optional_float(case.get("funding_prev_24h_pct"))
    oi24 = optional_float(case.get("oi_change_24h_pct"))
    long_ratio = optional_float(case.get("long_ratio"))
    if funding is not None and funding <= spec.funding_min_pct:
        return False, "funding"
    if oi24 is None:
        return False, "missing_oi"
    if oi24 > spec.oi_max_pct:
        return False, "oi"
    if long_ratio is None:
        return False, "missing_long_ratio"
    if not (spec.long_ratio_min <= long_ratio <= spec.long_ratio_max):
        return False, "long_ratio"
    return True, "ready"


def build_strategy_candidates(
    spec: StrategySpec,
    cases: list[dict[str, Any]],
    outcomes: dict[tuple[str, int, str], dict[str, Any]],
    split_ts: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for case in cases:
        pump_pct = to_float(case.get("pump_pct"))
        if pump_pct is None:
            continue
        tier = select_tier(spec, pump_pct)
        if tier is None:
            continue
        passed, gate_reason = passes_online_gates(spec, case)
        if not passed:
            continue
        key = (str(case.get("case_id") or ""), int(tier.pullback_pct), tier.rule_slug)
        outcome = outcomes.get(key)
        if not outcome:
            continue
        entry_ts = to_int(outcome.get("entry_ts"))
        exit_ts = to_int(outcome.get("exit_ts")) or entry_ts
        if entry_ts < START_TS_MS:
            continue
        rows.append(
            {
                "strategy_id": spec.strategy_id,
                "mode": spec.mode,
                "symbol": str(case.get("symbol") or ""),
                "case_id": str(case.get("case_id") or ""),
                "entry_ts": entry_ts,
                "entry_iso": ms_to_iso(entry_ts),
                "exit_ts": exit_ts,
                "exit_iso": ms_to_iso(exit_ts),
                "split": "test" if entry_ts >= split_ts else "train",
                "pump_pct": rounded(pump_pct),
                "funding_prev_24h_pct": rounded(optional_float(case.get("funding_prev_24h_pct"))),
                "oi_change_24h_pct": rounded(optional_float(case.get("oi_change_24h_pct"))),
                "long_ratio": rounded(optional_float(case.get("long_ratio"))),
                "pullback_pct": tier.pullback_pct,
                "rule_slug": tier.rule_slug,
                "net_pct": rounded(to_float(outcome.get("net_reserved_pct"))),
                "stress_pct": rounded(max(0.0, to_float(outcome.get("max_margin_stress_reserved_pct")) or 0.0)),
                "exit_reason": str(outcome.get("exit_reason") or ""),
                "gate_reason": gate_reason,
            }
        )
    rows.sort(key=lambda row: (to_int(row.get("entry_ts")), str(row.get("symbol") or "")))
    return rows


def simulate_portfolio(
    candidates: list[dict[str, Any]],
    *,
    strategy_id: str,
    total_capital_usd: float,
    reserve_usd: float,
    slots: int,
    sizing_mode: str,
    split_ts: int,
) -> dict[str, Any]:
    if total_capital_usd <= 0 or slots <= 0 or reserve_usd < 0 or reserve_usd >= total_capital_usd:
        raise ValueError("invalid capital, reserve, or slots")
    if sizing_mode not in SIZING_MODES:
        raise ValueError(f"unknown sizing mode: {sizing_mode}")

    initial_deployable = total_capital_usd - reserve_usd
    deployable = initial_deployable
    reserve = reserve_usd
    initial_slot = initial_deployable / slots
    current_stair_slot = initial_slot
    active: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    skipped_slots = 0
    skipped_same_symbol = 0
    skipped_capital = 0
    max_rescue_required = 0.0
    max_reserve_locked = 0.0
    max_manual_topup = 0.0
    reserve_breach_events = 0
    peak_equity = total_capital_usd
    max_drawdown = 0.0

    def total_equity() -> float:
        return deployable + reserve

    def close_due(until_ts: int) -> None:
        nonlocal deployable, reserve, active, peak_equity, max_drawdown
        due = sorted(
            [item for item in active if to_int(item.get("exit_ts")) <= until_ts],
            key=lambda item: (to_int(item.get("exit_ts")), str(item.get("symbol") or "")),
        )
        active = [item for item in active if to_int(item.get("exit_ts")) > until_ts]
        for item in due:
            pnl = to_float(item.get("pnl_usd"))
            if sizing_mode == "reserve_stair_25_cap2" and pnl > 0:
                reserve += pnl * 0.30
                deployable += pnl * 0.70
            else:
                deployable += pnl
            if deployable < 0:
                reserve += deployable
                deployable = 0.0
            equity = total_equity()
            peak_equity = max(peak_equity, equity)
            max_drawdown = max(max_drawdown, peak_equity - equity)
            item["equity_after_exit_usd"] = rounded(equity)
            item["reserve_after_exit_usd"] = rounded(reserve)

    def next_slot_budget() -> float:
        nonlocal current_stair_slot
        if sizing_mode == "fixed":
            return initial_slot
        raw = max(0.0, deployable / slots)
        if sizing_mode == "full_dynamic":
            return raw
        if raw < current_stair_slot:
            current_stair_slot = raw
        while (
            current_stair_slot < initial_slot * 2.0
            and raw >= current_stair_slot * 1.25
        ):
            current_stair_slot = min(initial_slot * 2.0, current_stair_slot * 1.25)
        return min(raw, current_stair_slot)

    for candidate in candidates:
        entry_ts = to_int(candidate.get("entry_ts"))
        close_due(entry_ts)
        symbol = str(candidate.get("symbol") or "")
        if any(str(item.get("symbol") or "") == symbol for item in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= slots:
            skipped_slots += 1
            continue
        slot_budget = next_slot_budget()
        committed = sum(to_float(item.get("slot_budget_usd")) for item in active)
        available_for_commitment = max(0.0, deployable - committed)
        if slot_budget <= 0 or available_for_commitment + 1e-9 < slot_budget:
            skipped_capital += 1
            continue

        net_pct = to_float(candidate.get("net_pct"))
        stress_pct = max(0.0, to_float(candidate.get("stress_pct")))
        pnl = slot_budget * LEVERAGE * net_pct / 100.0
        rescue_required = slot_budget * max(0.0, stress_pct / 100.0 - 1.0)
        reserve_locked_before = sum(to_float(item.get("reserve_locked_usd")) for item in active)
        reserve_free = max(0.0, reserve - reserve_locked_before)
        reserve_locked = min(reserve_free, rescue_required)
        manual_topup = max(0.0, rescue_required - reserve_locked)
        if manual_topup > 0:
            reserve_breach_events += 1

        trade = {
            **candidate,
            "total_capital_usd": rounded(total_capital_usd),
            "initial_reserve_usd": rounded(reserve_usd),
            "slots": slots,
            "sizing_mode": sizing_mode,
            "slot_budget_usd": rounded(slot_budget),
            "pnl_usd": rounded(pnl),
            "rescue_required_usd": rounded(rescue_required),
            "reserve_locked_usd": rounded(reserve_locked),
            "manual_topup_usd": rounded(manual_topup),
            "equity_after_exit_usd": None,
            "reserve_after_exit_usd": None,
        }
        active.append(trade)
        trades.append(trade)
        concurrent_rescue = sum(to_float(item.get("rescue_required_usd")) for item in active)
        concurrent_locked = sum(to_float(item.get("reserve_locked_usd")) for item in active)
        concurrent_manual = sum(to_float(item.get("manual_topup_usd")) for item in active)
        max_rescue_required = max(max_rescue_required, concurrent_rescue)
        max_reserve_locked = max(max_reserve_locked, concurrent_locked)
        max_manual_topup = max(max_manual_topup, concurrent_manual)

    close_due(10**18)
    final_equity = total_equity()
    wins = sum(1 for row in trades if to_float(row.get("pnl_usd")) > 0)
    losses = sum(1 for row in trades if to_float(row.get("pnl_usd")) < 0)
    test_rows = [row for row in trades if to_int(row.get("entry_ts")) >= split_ts]
    train_rows = [row for row in trades if to_int(row.get("entry_ts")) < split_ts]
    roi_on_total_pct = (final_equity - total_capital_usd) / total_capital_usd * 100.0
    max_drawdown_pct = max_drawdown / total_capital_usd * 100.0
    risk_adjusted_roi_pct = (
        roi_on_total_pct
        - max_drawdown_pct
        - max_manual_topup / total_capital_usd * 0.25
        - max(0.0, -min((to_float(row.get("pnl_usd")) for row in trades), default=0.0))
        / total_capital_usd
        * 50.0
    )
    summary = {
        "strategy_id": strategy_id,
        "total_capital_usd": rounded(total_capital_usd),
        "initial_deployable_usd": rounded(initial_deployable),
        "initial_reserve_usd": rounded(reserve_usd),
        "initial_reserve_pct": rounded(reserve_usd / total_capital_usd * 100.0),
        "slots": slots,
        "sizing_mode": sizing_mode,
        "trades": len(trades),
        "train_trades": len(train_rows),
        "test_trades": len(test_rows),
        "wins": wins,
        "losses": losses,
        "win_pct": rounded(wins / len(trades) * 100.0 if trades else 0.0),
        "test_win_pct": rounded(
            sum(1 for row in test_rows if to_float(row.get("pnl_usd")) > 0) / len(test_rows) * 100.0
            if test_rows
            else 0.0
        ),
        "avg_net_pct": rounded(statistics.mean([to_float(row.get("net_pct")) for row in trades])) if trades else 0.0,
        "median_net_pct": rounded(statistics.median([to_float(row.get("net_pct")) for row in trades])) if trades else 0.0,
        "net_pnl_usd": rounded(final_equity - total_capital_usd),
        "final_equity_usd": rounded(final_equity),
        "roi_on_total_pct": rounded(roi_on_total_pct),
        "risk_adjusted_roi_pct": rounded(risk_adjusted_roi_pct),
        "roi_on_deployable_pct": rounded(
            (final_equity - total_capital_usd) / initial_deployable * 100.0
        ),
        "final_reserve_usd": rounded(reserve),
        "final_reserve_pct": rounded(reserve / final_equity * 100.0 if final_equity > 0 else 0.0),
        "max_drawdown_usd": rounded(max_drawdown),
        "max_drawdown_pct": rounded(max_drawdown_pct),
        "worst_trade_usd": rounded(min((to_float(row.get("pnl_usd")) for row in trades), default=0.0)),
        "max_concurrent_rescue_required_usd": rounded(max_rescue_required),
        "max_concurrent_reserve_locked_usd": rounded(max_reserve_locked),
        "max_concurrent_manual_topup_usd": rounded(max_manual_topup),
        "reserve_breach_events": reserve_breach_events,
        "reserve_coverage_pct": rounded(
            min(100.0, max_reserve_locked / max_rescue_required * 100.0)
            if max_rescue_required > 0
            else 100.0
        ),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_capital": skipped_capital,
        "first_entry_iso": trades[0]["entry_iso"] if trades else "",
        "last_exit_iso": max((str(row.get("exit_iso") or "") for row in trades), default=""),
    }
    return {"summary": summary, "trades": trades}


def load_paper_results(paper_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    strategy_state = read_json(paper_dir / "strategy_paper_positions.json")
    for strategy_id, summary in dict(strategy_state.get("strategy_summaries") or {}).items():
        rows.append({"paper_layer": "strategy_monitor", "strategy_id": strategy_id, **summary})

    cycle_state = read_json(paper_dir / "pump_cycle_paper_positions.json")
    for summary in cycle_state.get("track_summaries") or []:
        rows.append({"paper_layer": "cycle_main", **summary})

    candidate_state = read_json(paper_dir / "pump_cycle_candidate_paper_positions.json")
    for summary in candidate_state.get("track_summaries") or []:
        rows.append({"paper_layer": "cycle_candidate", **summary})

    short_summary = next(
        (
            row
            for row in rows
            if row.get("paper_layer") == "cycle_main"
            and row.get("track_id") == "short_main_tiered"
        ),
        None,
    )
    scenarios: list[dict[str, Any]] = []
    if short_summary:
        observed = to_float(short_summary.get("combined_pnl_usd"))
        for total, reserve in CAPITAL_SCENARIOS:
            deployable = total - reserve
            slot_budget = deployable / 4.0
            scale = slot_budget / 500.0
            scenarios.append(
                {
                    "total_capital_usd": total,
                    "reserve_usd": reserve,
                    "deployable_usd": deployable,
                    "slots": 4,
                    "slot_budget_usd": rounded(slot_budget),
                    "scale_vs_paper_500_slot": rounded(scale),
                    "same_trade_mark_pnl_usd": rounded(observed * scale),
                    "same_trade_mark_roi_on_total_pct": rounded(observed * scale / total * 100.0),
                    "warning": "small-sample linear scaling only; not a forecast",
                }
            )
    return rows, scenarios


def build_cycle_reference_since_2024() -> list[dict[str, Any]]:
    if not DEFAULT_LONG_OUTCOMES.exists() or not DEFAULT_SHORT_TRADES.exists():
        return []
    long_rows = read_cycle_csv(DEFAULT_LONG_OUTCOMES)
    short_rows = read_cycle_csv(DEFAULT_SHORT_TRADES)
    long_candidates = {
        track.track_id: [
            row
            for row in load_long_candidates(long_rows, track)
            if to_int(row.get("entry_ts")) >= START_TS_MS
        ]
        for track in LONG_TRACKS
    }
    short_track = next(track for track in SHORT_TRACKS if track.track_id == "short_clean_p100_l3")
    short_candidates = [
        row
        for row in load_short_candidates(short_rows, short_track)
        if to_int(row.get("entry_ts")) >= START_TS_MS
    ]
    wanted_allocations = {
        "short_only_4",
        "cycle_6_4s2l",
        "cycle_5_4s1l",
        "cycle_5_3s2l",
    }
    rows: list[dict[str, Any]] = []
    short_only = next(item for item in CYCLE_ALLOCATIONS if item.allocation_id == "short_only_4")
    summary, _, _ = replay_cycle(
        short_only,
        None,
        short_track,
        short_candidates,
        starting_capital_usd=3_000.0,
    )
    rows.append(
        {
            **summary,
            "period_start": "2024-01-01",
            "comparison_note": "narrow p100 short control; not the exact main tiered strategy",
        }
    )
    for long_track in LONG_TRACKS:
        candidates = [*short_candidates, *long_candidates[long_track.track_id]]
        for allocation in CYCLE_ALLOCATIONS:
            if allocation.allocation_id not in wanted_allocations or allocation.long_slots <= 0:
                continue
            summary, _, _ = replay_cycle(
                allocation,
                long_track,
                short_track,
                candidates,
                starting_capital_usd=3_000.0,
            )
            rows.append(
                {
                    **summary,
                    "period_start": "2024-01-01",
                    "comparison_note": "cycle reference uses p100 short, not the exact main tiered strategy",
                }
            )
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("long_track_id") or ""),
            str(row.get("allocation_id") or ""),
        ),
    )


def subaccount_capability_rows() -> list[dict[str, Any]]:
    return [
        {
            "capability": "list_subaccounts",
            "endpoint": "GET /v5/user/query-sub-members",
            "key": "master",
            "permission": "Account Transfer or Subaccount Transfer or Withdrawal",
            "use": "discover and validate target sub UID",
        },
        {
            "capability": "read_sub_balance",
            "endpoint": "GET /v5/asset/transfer/query-account-coin-balance",
            "key": "master",
            "permission": "wallet read/transfer scope",
            "use": "read wallet and transfer-safe amount before moving USDT",
        },
        {
            "capability": "transfer_main_sub",
            "endpoint": "POST /v5/asset/transfer/universal-transfer",
            "key": "master preferred",
            "permission": "SubMemberTransfer",
            "use": "UNIFIED to UNIFIED transfer with caller-generated UUID",
        },
        {
            "capability": "confirm_transfer",
            "endpoint": "GET /v5/asset/transfer/query-universal-transfer-list",
            "key": "master or sub",
            "permission": "SubMemberTransfer or SubMemberTransferList",
            "use": "confirm SUCCESS; do not treat POST acceptance as settlement",
        },
        {
            "capability": "trade_subaccount",
            "endpoint": "V5 order/position endpoints",
            "key": "sub",
            "permission": "ContractTrade Order and Position",
            "use": "isolate pump orders and positions from main strategies",
        },
        {
            "capability": "add_isolated_margin",
            "endpoint": "POST /v5/position/add-margin",
            "key": "sub",
            "permission": "Position",
            "use": "add margin only after transfer is confirmed and position is re-read",
        },
    ]


def render_markdown_report(
    *,
    cases: list[dict[str, Any]],
    outcomes: dict[tuple[str, int, str], dict[str, Any]],
    split_ts: int,
    history_summary: list[dict[str, Any]],
    money_rows: list[dict[str, Any]],
    paper_rows: list[dict[str, Any]],
    paper_scenarios: list[dict[str, Any]],
    cycle_reference: list[dict[str, Any]],
    capability_rows: list[dict[str, Any]],
) -> str:
    main_money = [
        row
        for row in money_rows
        if row.get("strategy_id") == "main_pullback_tier"
        and row.get("slots") == 4
        and row.get("sizing_mode") in {"fixed", "reserve_stair_25_cap2"}
    ]
    startup_recommended = next(
        (
            row
            for row in main_money
            if to_float(row.get("total_capital_usd")) == 5_000.0
            and to_float(row.get("initial_reserve_usd")) == 1_500.0
            and row.get("sizing_mode") == "fixed"
        ),
        None,
    )
    lines = [
        "# Pump/Dump paper-to-live transition research",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Scope and evidence",
        "",
        f"- Unique reconstructed cases since the requested 2024 boundary: **{len(cases)}**.",
        f"- Actual available entry range: **{ms_to_iso(min((to_int(row.get('entry_ts')) for row in cases), default=0))}** to **{ms_to_iso(max((to_int(row.get('entry_ts')) for row in cases), default=0))}**.",
        f"- Train/test boundary: **{ms_to_iso(split_ts)}**.",
        f"- Selected exact rule outcomes: **{len(outcomes)}**.",
        f"- {FEE_NOTE}",
        "- Historical data has current-listing survivor bias and is not sufficient by itself for unrestricted live trading.",
        "",
        "## Exact current strategy comparison: fixed $3000 deployable, 4 short slots",
        "",
        markdown_table(
            sorted(history_summary, key=lambda row: -to_float(row.get("roi_on_total_pct"))),
            (
                "strategy_id",
                "trades",
                "win_pct",
                "test_win_pct",
                "net_pnl_usd",
                "roi_on_total_pct",
                "risk_adjusted_roi_pct",
                "max_drawdown_pct",
                "max_concurrent_manual_topup_usd",
                "worst_trade_usd",
            ),
        ),
        "",
        "These tracks replay many of the same events. Their PnL must not be added together; extra tracks are controls/candidates, not independent diversification.",
        "",
        "## Current paper summaries",
        "",
        markdown_table(
            paper_rows,
            (
                "paper_layer",
                "strategy_id",
                "track_id",
                "positions",
                "closed_positions",
                "open_positions",
                "realized_pnl_usd",
                "unrealized_pnl_usd",
                "combined_pnl_usd",
                "roi_mark_pct",
                "roi_on_initial_pct",
                "win_pct",
            ),
        ),
        "",
        "The strategy-monitor rows also overlap the same paper events. The cycle candidate short rows can duplicate the main cycle short entries.",
        "",
        "## Historical long + short cycle reference since 2024",
        "",
        "This is a separate reference model based on the narrower `short_clean_p100_l3` control, not an apples-to-apples replacement for the exact tiered main strategy above.",
        "",
        markdown_table(
            cycle_reference,
            (
                "allocation_id",
                "long_track_id",
                "trades",
                "short_trades",
                "long_trades",
                "roi_pct",
                "risk_adjusted_roi_pct",
                "max_drawdown_pct",
                "max_concurrent_topup_usd",
                "worst_trade_pct",
            ),
        ),
        "",
        "## Main-short capital and reserve matrix",
        "",
        markdown_table(
            sorted(
                main_money,
                key=lambda row: (
                    to_float(row.get("total_capital_usd")),
                    to_float(row.get("initial_reserve_usd")),
                    str(row.get("sizing_mode")),
                ),
            ),
            (
                "total_capital_usd",
                "initial_reserve_usd",
                "initial_deployable_usd",
                "sizing_mode",
                "trades",
                "final_equity_usd",
                "roi_on_total_pct",
                "risk_adjusted_roi_pct",
                "final_reserve_usd",
                "max_drawdown_pct",
                "max_concurrent_rescue_required_usd",
                "max_concurrent_manual_topup_usd",
                "reserve_breach_events",
            ),
        ),
        "",
        "## Same observed paper trades scaled to four short slots",
        "",
        markdown_table(
            paper_scenarios,
            (
                "total_capital_usd",
                "reserve_usd",
                "deployable_usd",
                "slot_budget_usd",
                "same_trade_mark_pnl_usd",
                "same_trade_mark_roi_on_total_pct",
                "warning",
            ),
        ),
        "",
        "## Bybit subaccount API capability map",
        "",
        markdown_table(capability_rows, ("capability", "endpoint", "key", "permission", "use")),
        "",
        "## Research decision",
        "",
        "- Use a dedicated Bybit UTA subaccount for Pump/Dump. A same-account position is aggregated by symbol and position side, so order tags cannot preserve strategy ownership after fills.",
        "- First live phase should be short-only and capped. Keep long tracks in paper until their live-like sample is materially larger and no longer dominated by one stop.",
        "- Transfers must be an early liquidity operation, not the final liquidation defense: read both balances, query transfer-safe amount, transfer with an idempotent UUID, confirm SUCCESS, re-read position and only then add isolated margin.",
        "- Never auto-pull the main account below its own protected funding/grid reserve. If both accounts are stressed, stop new pump entries and escalate instead of moving the same dollars back and forth.",
        "- Start with manual approval for every transfer. Automatic transfer can follow only after testnet and small-mainnet shadow validation.",
        "- Startup sizing should be fixed. Full dynamic compounding is a stress diagnostic, not a live recommendation; the capped 25% stair can be considered only after a successful canary period.",
    ]
    if startup_recommended:
        row = startup_recommended
        lines.extend(
            [
                "",
                "## Conservative startup row",
                "",
                f"- Capital ${row['total_capital_usd']}, initial reserve ${row['initial_reserve_usd']}, "
                f"mode `{row['sizing_mode']}`, ROI {row['roi_on_total_pct']}%, "
                f"risk-adjusted ROI {row['risk_adjusted_roi_pct']}%, max drawdown {row['max_drawdown_pct']}%, "
                f"historical manual top-up ${row['max_concurrent_manual_topup_usd']}. "
                "This reserve is deliberately larger than the trajectory-minimum because live slippage, transfer latency, and delisted-coin bias are not simulated.",
            ]
        )
    return "\n".join(lines) + "\n"


def markdown_table(rows: Iterable[dict[str, Any]], columns: tuple[str, ...]) -> str:
    items = list(rows)
    if not items:
        return "_No rows._"
    head = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(str(row.get(column, "") if row.get(column) is not None else "") for column in columns) + " |"
        for row in items
    ]
    return "\n".join([head, separator, *body])


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not columns:
            return
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def percentile_timestamp(values: list[int], fraction: float) -> int:
    ordered = sorted(value for value in values if value > 0)
    if not ordered:
        return 0
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def ms_to_iso(value: int | float | None) -> str:
    parsed = int(value or 0)
    if parsed <= 0:
        return ""
    return datetime.fromtimestamp(parsed / 1000.0, tz=timezone.utc).isoformat()


def optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def to_float(value: Any) -> float:
    parsed = optional_float(value)
    return parsed if parsed is not None else 0.0


def to_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def rounded(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    return round(float(value), digits)
