from __future__ import annotations

from analysis_features.bybit_pump_short_outcomes import Series
from analysis_features.pump_live_budget_sweep_research import (
    ReplayConfig,
    build_trade_actions,
    replay_budget,
    select_unlimited_capital_trades,
)


HOUR = 3_600_000


def _series(highs: list[float], closes: list[float]) -> Series:
    return Series(
        symbol="TESTUSDT",
        launch_ms=0,
        ts=[index * HOUR for index in range(len(highs))],
        open=closes,
        high=highs,
        low=[value * 0.95 for value in closes],
        close=closes,
        funding=[],
        oi={},
        long_ratio={},
    )


def _candidate(*, entry: int = 0, exit_: int = 4 * HOUR, net_pct: float = 10.0) -> dict:
    return {
        "symbol": "TESTUSDT",
        "case_id": f"test_{entry}",
        "entry_ts": entry,
        "entry_iso": "entry",
        "exit_ts": exit_,
        "exit_iso": "exit",
        "rule_slug": "step50_legs5_equal_tp25_720",
        "pump_pct": 60.0,
        "legs_activated": 2,
        "net_pct": net_pct,
    }


def test_select_unlimited_capital_trades_uses_only_slot_and_symbol_limits() -> None:
    rows = [
        {"symbol": f"S{index}", "entry_ts": 0, "exit_ts": 10}
        for index in range(5)
    ]
    rows.append({"symbol": "S0", "entry_ts": 1, "exit_ts": 5})
    selected, skipped = select_unlimited_capital_trades(rows, max_positions=4)
    assert len(selected) == 4
    assert skipped == {"skipped_slots": 1, "skipped_same_symbol": 1}


def test_trade_actions_allocate_only_filled_legs_and_release_at_exit() -> None:
    series = _series(
        highs=[1.0, 1.12, 1.51, 1.2, 1.1],
        closes=[1.0, 1.1, 1.45, 1.15, 1.1],
    )
    actions, diagnostics = build_trade_actions(
        _candidate(),
        series,
        budget_usd=600.0,
        config=ReplayConfig(),
    )
    assert actions[0]["base_margin_usd"] == 120.0
    assert any(row["action"] == "arm_l2" for row in actions)
    assert any(row["action"] == "fill_l2" and row["base_margin_usd"] == 240.0 for row in actions)
    assert actions[-1]["action"] == "exit_release"
    assert actions[-1]["base_margin_usd"] == 0.0
    assert diagnostics["filled_legs_reconstructed"] == 2
    assert diagnostics["legs_match"] is True


def test_trade_actions_do_not_release_margin_in_the_same_hour_as_an_add() -> None:
    series = _series(
        highs=[1.0, 1.51, 1.1],
        closes=[1.0, 1.05, 1.0],
    )
    candidate = _candidate(exit_=2 * HOUR)
    candidate["rule_slug"] = "step50_legs2_tapered_tp25_720"
    candidate["legs_activated"] = 2
    actions, _diagnostics = build_trade_actions(
        candidate,
        series,
        budget_usd=600.0,
        config=ReplayConfig(),
    )
    first_hour = [row for row in actions if row["ts"] == HOUR]
    assert any(row["delta_topup_usd"] > 0 for row in first_hour)
    assert not any(row["action"] == "margin_release" for row in first_hour)


def test_budget_replay_scales_profit_and_tracks_unlimited_borrowing() -> None:
    series = _series(
        highs=[1.0, 1.2, 1.55, 2.1, 1.2],
        closes=[1.0, 1.15, 1.5, 2.0, 1.1],
    )
    config = ReplayConfig(own_capital_usd=100.0, operating_floor_usd=25.0)
    summary, details, timeline, loan_events, loan_episodes = replay_budget(
        [_candidate()],
        {"TESTUSDT": series},
        budget_usd=600.0,
        config=config,
    )
    assert summary["pnl_usd"] == 180.0
    assert summary["peak_borrowed_usd"] > 0
    assert summary["borrowed_hours"] > 0
    assert summary["final_working_capital_usd"] == 100.0
    assert summary["withdrawn_profit_usd"] == 180.0
    assert details[0]["peak_topup_usd"] > 0
    assert timeline[-1]["working_capital_usd"] == 100.0
    assert timeline[-1]["withdrawn_profit_usd"] == 180.0
    assert timeline[-1]["active_positions"] == 0
    assert any(row["event"] == "borrow" for row in loan_events)
    assert any(row["event"] == "repay" for row in loan_events)
    assert loan_episodes


def test_withdrawn_profit_never_increases_later_working_capital() -> None:
    quiet = _series(
        highs=[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        closes=[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    )
    winner = _candidate(entry=0, exit_=2 * HOUR, net_pct=10.0)
    loser = _candidate(entry=3 * HOUR, exit_=6 * HOUR, net_pct=-5.0)
    summary, _details, timeline, _events, _episodes = replay_budget(
        [winner, loser],
        {"TESTUSDT": quiet},
        budget_usd=600.0,
        config=ReplayConfig(own_capital_usd=3_000.0),
    )
    assert summary["pnl_usd"] == 90.0
    assert summary["withdrawn_profit_usd"] == 180.0
    assert summary["final_working_capital_usd"] == 2_910.0
    assert max(row["working_capital_usd"] for row in timeline) == 3_000.0
