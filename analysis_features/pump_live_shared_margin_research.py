from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from analysis_features.pump_live_margin_stress import (
    ShortPosition,
    combined_short_position,
    required_extra_margin_for_stop,
    round_up_usd,
)
from analysis_features.pump_live_transition_research import (
    START_TS_MS,
    STRATEGIES,
    build_strategy_candidates,
    load_selected_pullback_outcomes,
    wanted_outcome_keys,
)
from analysis_features.pump_short_policy_portfolio_research import build_unique_cases, load_csv
from config import BASE_DIR

LEVERAGE = 3.0
MMR = 0.025
TAKER_FEE = 0.00055
STOP_GAP_PCT = 2.5
NEXT_STEP_SAFETY_PCT = 2.5
FINAL_FILL_RACE_BUFFER_PCT = 20.0

DEFAULT_PER_EVENT_DIR = BASE_DIR / "data" / "research" / "pump_short_per_event_strategy_research"
DEFAULT_PULLBACK_DIR = BASE_DIR / "data" / "research" / "pump_short_pullback_tier_research"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_live_shared_margin_research"


@dataclass(frozen=True, slots=True)
class SharedMarginPolicy:
    policy_id: str
    slot_margin_usd: float
    ladder_gate: str
    main_loan_cap_usd: float
    loan_for_new_entries: bool
    own_capital_usd: float = 3_000.0
    max_positions: int = 4
    operating_floor_usd: float = 75.0
    max_position_topup_usd: float = 10_000.0
    max_portfolio_topup_usd: float = 10_000.0


POLICIES: tuple[SharedMarginPolicy, ...] = (
    SharedMarginPolicy(
        "current_v2_525",
        525.0,
        "current_next",
        0.0,
        False,
        max_position_topup_usd=525.0,
        max_portfolio_topup_usd=825.0,
    ),
    SharedMarginPolicy(
        "safe_pool_525_rescue2000",
        525.0,
        "projected_next_step",
        2_000.0,
        False,
        max_position_topup_usd=2_000.0,
        max_portfolio_topup_usd=2_825.0,
    ),
    SharedMarginPolicy(
        "safe_pool_600_loan1000",
        600.0,
        "projected_next_step",
        1_000.0,
        False,
        max_position_topup_usd=1_200.0,
        max_portfolio_topup_usd=1_525.0,
    ),
    SharedMarginPolicy(
        "safe_pool_625_loan1000",
        625.0,
        "projected_next_step",
        1_000.0,
        False,
        max_position_topup_usd=1_250.0,
        max_portfolio_topup_usd=1_500.0,
    ),
    SharedMarginPolicy(
        "safe_pool_650_loan1000",
        650.0,
        "projected_next_step",
        1_000.0,
        False,
        max_position_topup_usd=1_300.0,
        max_portfolio_topup_usd=1_325.0,
    ),
    SharedMarginPolicy(
        "user_pool_750_loan2000_rescue_only",
        750.0,
        "projected_next_step",
        2_000.0,
        False,
        max_position_topup_usd=2_000.0,
        max_portfolio_topup_usd=2_000.0,
    ),
    SharedMarginPolicy(
        "user_pool_750_loan2000_entries",
        750.0,
        "projected_next_step",
        2_000.0,
        True,
        max_position_topup_usd=2_000.0,
        max_portfolio_topup_usd=2_000.0,
    ),
    SharedMarginPolicy(
        "aggressive_pool_725_loan1750_rescue_only",
        725.0,
        "projected_next_step",
        1_750.0,
        False,
        max_position_topup_usd=2_000.0,
        max_portfolio_topup_usd=2_500.0,
    ),
)


def rule_weights(rule_slug: str) -> tuple[float, ...]:
    if "legs5_equal" in rule_slug:
        return (1.0, 1.0, 1.0, 1.0, 1.0)
    if "legs3_tapered" in rule_slug:
        return (1.0, 2.0, 3.0)
    if "legs2_tapered" in rule_slug:
        return (1.0, 2.0)
    raise ValueError(f"unsupported Pump rule: {rule_slug}")


def ladder_prefund_profile(
    *,
    rule_slug: str,
    slot_margin_usd: float,
    legs_activated: int,
    gate_mode: str,
) -> dict[str, Any]:
    if gate_mode not in {"current_next", "projected_next_step"}:
        raise ValueError(f"unknown ladder gate: {gate_mode}")
    weights = rule_weights(rule_slug)
    prices = tuple(1.0 + 0.5 * index for index in range(len(weights)))
    weight_sum = sum(weights)
    filled = ShortPosition(
        qty=(slot_margin_usd * weights[0] / weight_sum * LEVERAGE) / prices[0],
        avg_entry_price=prices[0],
        leverage=LEVERAGE,
        maintenance_margin_rate=MMR,
        taker_fee_rate=TAKER_FEE,
    )
    rows: list[dict[str, Any]] = []
    maximum_required = 0.0
    race_exposed_fills = 0
    # A next order exists after every historically filled state except the full ladder.
    gates_seen = min(max(1, legs_activated), len(weights) - 1)
    for filled_count in range(1, gates_seen + 1):
        next_index = filled_count
        if next_index >= len(weights):
            break
        next_price = prices[next_index]
        added_qty = (
            slot_margin_usd * weights[next_index] / weight_sum * LEVERAGE / next_price
        )
        projected = combined_short_position(
            filled,
            added_qty=added_qty,
            added_price=next_price,
        )
        if gate_mode == "current_next":
            target_stop = next_price * (1.0 + NEXT_STEP_SAFETY_PCT / 100.0)
            required = required_extra_margin_for_stop(
                filled,
                target_stop_price=target_stop,
                gap_from_liquidation_pct=STOP_GAP_PCT,
            )
            race_exposed_fills += 1
        else:
            if next_index + 1 < len(prices):
                target_stop = prices[next_index + 1] * (
                    1.0 + NEXT_STEP_SAFETY_PCT / 100.0
                )
            else:
                target_stop = next_price * (
                    1.0 + FINAL_FILL_RACE_BUFFER_PCT / 100.0
                )
            # The same already-confirmed margin must protect both the old stop
            # during the fill race and the projected post-fill position.
            current_required = required_extra_margin_for_stop(
                filled,
                target_stop_price=target_stop,
                gap_from_liquidation_pct=STOP_GAP_PCT,
            )
            projected_required = required_extra_margin_for_stop(
                projected,
                target_stop_price=target_stop,
                gap_from_liquidation_pct=STOP_GAP_PCT,
            )
            required = max(current_required, projected_required)
        rounded = round_up_usd(required, increment_usd=5.0)
        maximum_required = max(maximum_required, rounded)
        rows.append(
            {
                "filled_before_gate": filled_count,
                "next_leg": next_index + 1,
                "next_price": round(next_price, 8),
                "target_stop": round(target_stop, 8),
                "required_total_topup_usd": round(rounded, 6),
                "old_stop_fill_clearance_pct": round(
                    (target_stop / next_price - 1.0) * 100.0,
                    6,
                ),
            }
        )
        filled = projected
    return {
        "rule_slug": rule_slug,
        "slot_margin_usd": round(slot_margin_usd, 6),
        "legs_activated": int(legs_activated),
        "gate_mode": gate_mode,
        "max_ladder_prefund_usd": round(maximum_required, 6),
        "race_exposed_fills": race_exposed_fills,
        "gates": rows,
    }


def historical_rescue_usd(row: Mapping[str, Any], slot_margin_usd: float) -> float:
    stress_pct = max(0.0, _float(row.get("stress_pct")))
    return max(0.0, slot_margin_usd * (stress_pct / 100.0 - 1.0))


def enrich_candidates(
    candidates: Iterable[Mapping[str, Any]],
    outcomes: Mapping[tuple[str, int, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for candidate in candidates:
        row = dict(candidate)
        key = (
            str(row.get("case_id") or ""),
            int(_float(row.get("pullback_pct"))),
            str(row.get("rule_slug") or ""),
        )
        outcome = outcomes.get(key) or {}
        row["legs_activated"] = max(1, int(_float(outcome.get("legs_activated")) or 1))
        row["max_adverse_from_first_pct"] = round(
            _float(outcome.get("max_adverse_from_first_pct")), 6
        )
        result.append(row)
    return result


def replay_policy(
    candidates: list[Mapping[str, Any]],
    policy: SharedMarginPolicy,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    active: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    equity = policy.own_capital_usd
    peak_equity = equity
    max_drawdown = 0.0
    skipped_slots = 0
    skipped_same_symbol = 0
    skipped_entry_capital = 0
    risk_breaches = 0
    peak_borrowed = 0.0
    peak_topup = 0.0
    race_exposed_fills = 0
    last_ts: int | None = None
    borrowed_usd_hours = 0.0
    borrowed_hours = 0.0

    def commitments(rows: Iterable[Mapping[str, Any]]) -> tuple[float, float, float]:
        items = list(rows)
        base = sum(_float(item.get("slot_margin_usd")) for item in items)
        topup = sum(_float(item.get("required_topup_usd")) for item in items)
        borrowed = max(
            0.0,
            base + topup + policy.operating_floor_usd - policy.own_capital_usd,
        )
        return base, topup, borrowed

    def account_interval(ts: int) -> None:
        nonlocal last_ts, borrowed_usd_hours, borrowed_hours
        if last_ts is not None and ts > last_ts:
            borrowed = commitments(active)[2]
            hours = (ts - last_ts) / 3_600_000.0
            borrowed_usd_hours += borrowed * hours
            if borrowed > 0:
                borrowed_hours += hours
        last_ts = ts

    def close_due(ts: int) -> None:
        nonlocal active, equity, peak_equity, max_drawdown
        due = sorted(
            (item for item in active if int(item["exit_ts"]) <= ts),
            key=lambda item: (int(item["exit_ts"]), str(item["symbol"])),
        )
        for item in due:
            account_interval(int(item["exit_ts"]))
            equity += _float(item.get("pnl_usd"))
            peak_equity = max(peak_equity, equity)
            max_drawdown = max(max_drawdown, peak_equity - equity)
            active.remove(item)

    for source in sorted(
        candidates,
        key=lambda row: (int(_float(row.get("entry_ts"))), str(row.get("symbol") or "")),
    ):
        entry_ts = int(_float(source.get("entry_ts")))
        close_due(entry_ts)
        account_interval(entry_ts)
        symbol = str(source.get("symbol") or "")
        if any(item["symbol"] == symbol for item in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= policy.max_positions:
            skipped_slots += 1
            continue
        base_before, _topup_before, _borrowed_before = commitments(active)
        base_after = base_before + policy.slot_margin_usd
        own_base_limit = policy.own_capital_usd - policy.operating_floor_usd
        entry_limit = (
            own_base_limit + policy.main_loan_cap_usd
            if policy.loan_for_new_entries
            else own_base_limit
        )
        if base_after > entry_limit + 1e-9:
            skipped_entry_capital += 1
            continue

        ladder = ladder_prefund_profile(
            rule_slug=str(source.get("rule_slug") or ""),
            slot_margin_usd=policy.slot_margin_usd,
            legs_activated=int(_float(source.get("legs_activated")) or 1),
            gate_mode=policy.ladder_gate,
        )
        historical_rescue = historical_rescue_usd(source, policy.slot_margin_usd)
        required_topup = max(historical_rescue, _float(ladder["max_ladder_prefund_usd"]))
        trade_supported = required_topup <= policy.max_position_topup_usd + 1e-9
        projected_topup = commitments(
            [
                *active,
                {
                    "slot_margin_usd": policy.slot_margin_usd,
                    "required_topup_usd": required_topup,
                },
            ]
        )[1]
        if projected_topup > policy.max_portfolio_topup_usd + 1e-9:
            trade_supported = False
        projected_borrowed = max(
            0.0,
            base_after + projected_topup + policy.operating_floor_usd - policy.own_capital_usd,
        )
        if projected_borrowed > policy.main_loan_cap_usd + 1e-9:
            trade_supported = False
        if not trade_supported:
            risk_breaches += 1

        pnl = policy.slot_margin_usd * LEVERAGE * _float(source.get("net_pct")) / 100.0
        trade = {
            **dict(source),
            "policy_id": policy.policy_id,
            "slot_margin_usd": round(policy.slot_margin_usd, 6),
            "pnl_usd": round(pnl, 6),
            "historical_rescue_usd": round(historical_rescue, 6),
            "ladder_prefund_usd": ladder["max_ladder_prefund_usd"],
            "required_topup_usd": round(required_topup, 6),
            "race_exposed_fills": ladder["race_exposed_fills"],
            "supported_by_policy": trade_supported,
            "projected_borrowed_usd": round(projected_borrowed, 6),
        }
        active.append(trade)
        trades.append(trade)
        peak_borrowed = max(peak_borrowed, projected_borrowed)
        peak_topup = max(peak_topup, projected_topup)
        race_exposed_fills += int(ladder["race_exposed_fills"])

    close_due(10**18)
    pnl = equity - policy.own_capital_usd
    wins = sum(1 for row in trades if _float(row.get("pnl_usd")) > 0)
    return {
        **asdict(policy),
        "candidate_rows": len(candidates),
        "trades": len(trades),
        "wins": wins,
        "losses": len(trades) - wins,
        "win_rate_pct": round(wins / len(trades) * 100.0, 6) if trades else 0.0,
        "historical_pnl_if_fully_funded_usd": round(pnl, 6),
        "historical_roi_on_pump_capital_pct": round(
            pnl / policy.own_capital_usd * 100.0, 6
        ),
        "historical_max_drawdown_usd": round(max_drawdown, 6),
        "historical_max_drawdown_pct": round(
            max_drawdown / policy.own_capital_usd * 100.0, 6
        ),
        "risk_capacity_breaches": risk_breaches,
        "race_exposed_fills": race_exposed_fills,
        "peak_required_topup_usd": round(peak_topup, 6),
        "peak_main_borrowed_usd": round(peak_borrowed, 6),
        "borrowed_hours_conservative": round(borrowed_hours, 6),
        "borrowed_usd_hours_conservative": round(borrowed_usd_hours, 6),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_entry_capital": skipped_entry_capital,
        "return_is_capacity_validated": risk_breaches == 0,
        "capital_model_note": (
            "top-up need is conservatively locked for the full trade; historical main "
            "availability and transfer latency are not available"
        ),
    }, trades


def build_candidates(
    *,
    per_event_dir: Path,
    pullback_dir: Path,
) -> tuple[list[dict[str, Any]], int, int]:
    cases = [
        row
        for row in build_unique_cases(load_csv(per_event_dir / "per_event_summary.csv"))
        if int(_float(row.get("entry_ts"))) >= START_TS_MS
    ]
    cases.sort(key=lambda row: (int(_float(row.get("entry_ts"))), str(row.get("symbol") or "")))
    main_spec = next(item for item in STRATEGIES if item.strategy_id == "main_pullback_tier")
    wanted = wanted_outcome_keys(cases, (main_spec,))
    outcomes = load_selected_pullback_outcomes(
        pullback_dir / "pullback_all_outcomes.csv",
        wanted,
    )
    timestamps = sorted(int(_float(row.get("entry_ts"))) for row in cases)
    split_ts = timestamps[min(len(timestamps) - 1, math.floor(len(timestamps) * 0.70))]
    candidates = build_strategy_candidates(main_spec, cases, outcomes, split_ts)
    return enrich_candidates(candidates, outcomes), len(cases), split_ts


def write_report(
    *,
    per_event_dir: Path = DEFAULT_PER_EVENT_DIR,
    pullback_dir: Path = DEFAULT_PULLBACK_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    policies: Iterable[SharedMarginPolicy] = POLICIES,
) -> dict[str, Any]:
    candidates, unique_cases, split_ts = build_candidates(
        per_event_dir=per_event_dir,
        pullback_dir=pullback_dir,
    )
    summaries: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for policy in policies:
        summary, policy_trades = replay_policy(candidates, policy)
        summaries.append(summary)
        trades.extend(policy_trades)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "policy_summary.csv", summaries)
    _write_csv(output_dir / "policy_trades.csv", trades)
    metadata = {
        "schema": "pump_live_shared_margin_research_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_start_iso": datetime.fromtimestamp(START_TS_MS / 1000, tz=timezone.utc).isoformat(),
        "actual_candidate_min_iso": _iso(min((int(_float(row.get("entry_ts"))) for row in candidates), default=0)),
        "actual_candidate_max_iso": _iso(max((int(_float(row.get("entry_ts"))) for row in candidates), default=0)),
        "unique_cases": unique_cases,
        "eligible_candidates": len(candidates),
        "split_ts": split_ts,
        "policies": summaries,
        "limitations": [
            "current-listing survivor bias",
            "hourly reconstructed strategy outcomes",
            "15-second fill-to-stop race cannot be resolved from hourly candles",
            "top-up need is conservatively held for the full trade",
            "historical Bybit main-account free balance is unavailable",
            "transfer latency and exchange rejection are not simulated",
        ],
        "research_only": True,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _iso(ts_ms: int) -> str | None:
    if ts_ms <= 0:
        return None
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


__all__ = [
    "POLICIES",
    "SharedMarginPolicy",
    "build_candidates",
    "historical_rescue_usd",
    "ladder_prefund_profile",
    "replay_policy",
    "rule_weights",
    "write_report",
]
