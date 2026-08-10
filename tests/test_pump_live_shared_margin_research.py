from __future__ import annotations

from analysis_features.pump_live_shared_margin_research import (
    SharedMarginPolicy,
    ladder_prefund_profile,
    replay_policy,
)


def _candidate(*, entry: int = 100, exit_: int = 200, stress: float = 0.0) -> dict[str, object]:
    return {
        "strategy_id": "main_pullback_tier",
        "symbol": f"T{entry}USDT",
        "case_id": f"case-{entry}",
        "entry_ts": entry,
        "exit_ts": exit_,
        "rule_slug": "step50_legs5_equal_tp25_720",
        "pullback_pct": 25,
        "net_pct": 10,
        "stress_pct": stress,
        "legs_activated": 3,
    }


def test_projected_gate_requires_more_margin_and_removes_fill_race() -> None:
    current = ladder_prefund_profile(
        rule_slug="step50_legs5_equal_tp25_720",
        slot_margin_usd=525,
        legs_activated=3,
        gate_mode="current_next",
    )
    projected = ladder_prefund_profile(
        rule_slug="step50_legs5_equal_tp25_720",
        slot_margin_usd=525,
        legs_activated=3,
        gate_mode="projected_next_step",
    )

    assert current["race_exposed_fills"] == 3
    assert projected["race_exposed_fills"] == 0
    assert projected["max_ladder_prefund_usd"] > current["max_ladder_prefund_usd"]
    assert projected["gates"][0]["old_stop_fill_clearance_pct"] > 30


def test_loan_for_entries_changes_fourth_slot_admission() -> None:
    candidates = [_candidate(entry=index * 10, exit_=1_000) for index in range(1, 5)]
    rescue_only = SharedMarginPolicy(
        "rescue-only",
        750,
        "projected_next_step",
        2_000,
        False,
        max_position_topup_usd=2_000,
        max_portfolio_topup_usd=2_000,
    )
    entry_loan = SharedMarginPolicy(
        "entry-loan",
        750,
        "projected_next_step",
        2_000,
        True,
        max_position_topup_usd=2_000,
        max_portfolio_topup_usd=2_000,
    )

    rescue_summary, _ = replay_policy(candidates, rescue_only)
    entry_summary, _ = replay_policy(candidates, entry_loan)

    assert rescue_summary["trades"] == 3
    assert rescue_summary["skipped_entry_capital"] == 1
    assert entry_summary["trades"] == 4
    assert entry_summary["peak_main_borrowed_usd"] > 0


def test_current_cap_reports_tail_capacity_breach() -> None:
    policy = SharedMarginPolicy(
        "current",
        525,
        "current_next",
        0,
        False,
        max_position_topup_usd=525,
        max_portfolio_topup_usd=825,
    )
    summary, trades = replay_policy([_candidate(stress=210)], policy)

    assert summary["risk_capacity_breaches"] == 1
    assert summary["return_is_capacity_validated"] is False
    assert trades[0]["supported_by_policy"] is False


def test_shared_pool_tracks_conservative_borrow_duration() -> None:
    policy = SharedMarginPolicy(
        "shared",
        750,
        "projected_next_step",
        2_000,
        True,
        max_position_topup_usd=2_000,
        max_portfolio_topup_usd=2_000,
    )
    candidates = [
        _candidate(entry=index * 10, exit_=3_600_000, stress=150)
        for index in range(1, 5)
    ]
    for candidate in candidates:
        candidate["legs_activated"] = 1
        candidate["stress_pct"] = 0
    summary, _ = replay_policy(candidates, policy)

    assert summary["risk_capacity_breaches"] == 0
    assert summary["peak_main_borrowed_usd"] > 0
    assert summary["borrowed_usd_hours_conservative"] > 0
