from execution.auto_strategies import (
    StrategyCandidate,
    choose_candidate,
    completion_tolerance_qty,
    current_step,
    reconcile_step_progress,
    trigger_matches,
)


def test_current_step_hides_later_steps_until_current_completes() -> None:
    strategy = {
        "steps": [
            {"id": "one", "status": "partial"},
            {"id": "two", "status": "waiting"},
        ]
    }
    assert current_step(strategy)["id"] == "one"


def test_current_step_advances_after_dust_completion() -> None:
    strategy = {
        "steps": [
            {"id": "one", "status": "completed_with_dust"},
            {"id": "two", "status": "waiting"},
        ]
    }
    assert current_step(strategy)["id"] == "two"


def test_one_percent_tolerance_completes_small_residual() -> None:
    result = reconcile_step_progress(
        {"target_qty": 10_000.0},
        observed_filled_qty=9_920.0,
        tolerance_pct=1.0,
    )
    assert result["status"] == "completed_with_dust"
    assert result["remaining_qty"] == 80.0
    assert completion_tolerance_qty(10_000.0, tolerance_pct=1.0) == 100.0


def test_partial_progress_keeps_fixed_target() -> None:
    result = reconcile_step_progress(
        {"target_qty": 10_000.0},
        observed_filled_qty=6_000.0,
        tolerance_pct=1.0,
    )
    assert result["status"] == "partial"
    assert result["remaining_qty"] == 4_000.0


def test_trigger_direction_differs_for_enter_and_exit() -> None:
    enter = trigger_matches(
        action="enter",
        spread_pct=-2.2,
        spread_target_pct=-2.0,
        funding_delta_pct=0.1,
        funding_min_pct=0.0,
    )
    exit_ = trigger_matches(
        action="exit",
        spread_pct=-0.4,
        spread_target_pct=-0.5,
        funding_delta_pct=None,
        funding_min_pct=None,
    )
    assert enter == (True, "trigger_matched")
    assert exit_ == (True, "trigger_matched")


def test_exit_candidate_precedes_more_profitable_enter() -> None:
    selected = choose_candidate(
        [
            StrategyCandidate("enter", "a", "enter", 60, 10.0, 1.0),
            StrategyCandidate("exit", "b", "exit", 30, 0.1, 2.0),
        ]
    )
    assert selected.strategy_id == "exit"
