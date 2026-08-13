from execution.execution_invariants import (
    ExecutionCandidate,
    choose_execution_candidate,
    completion_tolerance_qty,
    current_unfinished_step,
    reconcile_fixed_target_progress,
)


def test_current_step_hides_later_steps_until_current_completes() -> None:
    strategy = {
        "steps": [
            {"id": "one", "status": "partial"},
            {"id": "two", "status": "waiting"},
        ]
    }
    assert current_unfinished_step(strategy)["id"] == "one"


def test_current_step_advances_after_dust_completion() -> None:
    strategy = {
        "steps": [
            {"id": "one", "status": "completed_with_dust"},
            {"id": "two", "status": "waiting"},
        ]
    }
    assert current_unfinished_step(strategy)["id"] == "two"


def test_one_percent_tolerance_completes_small_residual() -> None:
    result = reconcile_fixed_target_progress(
        {"target_qty": 10_000.0},
        observed_filled_qty=9_920.0,
        tolerance_pct=1.0,
    )
    assert result["status"] == "completed_with_dust"
    assert result["remaining_qty"] == 80.0
    assert completion_tolerance_qty(10_000.0, tolerance_pct=1.0) == 100.0


def test_partial_progress_keeps_fixed_target() -> None:
    result = reconcile_fixed_target_progress(
        {"target_qty": 10_000.0},
        observed_filled_qty=6_000.0,
        tolerance_pct=1.0,
    )
    assert result["status"] == "partial"
    assert result["remaining_qty"] == 4_000.0


def test_exit_candidate_precedes_more_profitable_enter() -> None:
    selected = choose_execution_candidate(
        [
            ExecutionCandidate("enter", "a", "enter", 60, 10.0, 1.0),
            ExecutionCandidate("exit", "b", "exit", 30, 0.1, 2.0),
        ]
    )
    assert selected.owner_id == "exit"
