from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Mapping

from execution.pump_state_machine import PumpStateMachine


def _legacy_reduce_cold_restart(state: dict[str, Any]) -> dict[str, Any]:
    state["entry_armed"] = False
    state["transient_recovery_pending"] = False
    state["healthy_recovery_cycles"] = 0
    state["portfolio_risk_restore_armed"] = False
    state["portfolio_risk_recovery_cycles"] = 0
    has_open_positions = any(
        isinstance(item, Mapping) and item.get("status") != "closed"
        for item in state.get("positions") or []
    )
    save_state = False
    if has_open_positions:
        state["monitor_enabled"] = True
        state["status"] = "recovery_monitoring"
        save_state = True
    elif state.get("status") not in {"disabled", "stopped"}:
        state["monitor_enabled"] = False
        state["status"] = "disarmed_after_restart"
        state["blocked_reason"] = "backend_restart"
        save_state = True
    return {
        "has_open_positions": has_open_positions,
        "save_state": save_state,
        "start_recovery_monitor": has_open_positions,
    }


def test_cold_restart_reducer_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent / "fixtures" / "pump_cold_restart_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = PumpStateMachine()
    for case in cases:
        legacy_state = deepcopy(case["state"])
        machine_state = deepcopy(case["state"])
        expected_result = _legacy_reduce_cold_restart(legacy_state)
        actual_result = machine.reduce_cold_restart(machine_state)

        assert machine_state == legacy_state, case["name"]
        assert actual_result == expected_result, case["name"]
        expected = case["expected"]
        for key in (
            "status",
            "monitor_enabled",
            "entry_armed",
            "transient_recovery_pending",
            "healthy_recovery_cycles",
            "portfolio_risk_restore_armed",
            "portfolio_risk_recovery_cycles",
            "blocked_reason",
        ):
            assert machine_state.get(key) == expected[key], f"{case['name']}:{key}"
        for key in (
            "has_open_positions",
            "save_state",
            "start_recovery_monitor",
        ):
            assert actual_result[key] == expected[key], f"{case['name']}:{key}"
