from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from execution.pump_state_machine import PumpStateMachine


NOW_MS = 1_786_650_000_000


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


def _legacy_reduce_disarm(
    state: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    has_open_positions = any(
        isinstance(item, Mapping) and item.get("status") != "closed"
        for item in state.get("positions") or []
    )
    state["entry_armed"] = False
    state["monitor_enabled"] = has_open_positions
    state["status"] = "monitoring" if has_open_positions else "disarmed"
    state["blocked_reason"] = reason
    state["transient_recovery_pending"] = False
    state["healthy_recovery_cycles"] = 0
    state["portfolio_risk_freeze_active"] = False
    state["portfolio_risk_freeze_reason"] = None
    state["portfolio_risk_freeze_symbol"] = None
    state["portfolio_risk_freeze_buffer_pct"] = None
    state["portfolio_risk_restore_armed"] = False
    state["portfolio_risk_recovery_cycles"] = 0
    state["updated_at_ms"] = NOW_MS
    state["pending_signals"] = []
    return {"has_open_positions": has_open_positions}


def test_disarm_reducer_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent / "fixtures" / "pump_disarm_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = PumpStateMachine()
    for case in cases:
        legacy_state = deepcopy(case["state"])
        machine_state = deepcopy(case["state"])
        expected_result = _legacy_reduce_disarm(
            legacy_state,
            reason=case["reason"],
        )
        actual_result = machine.reduce_disarm(
            machine_state,
            reason=case["reason"],
            now_ms=NOW_MS,
        )

        assert machine_state == legacy_state, case["name"]
        assert actual_result == expected_result, case["name"]
        expected = case["expected"]
        assert machine_state["status"] == expected["status"], case["name"]
        assert machine_state["monitor_enabled"] is expected["monitor_enabled"]
        assert actual_result["has_open_positions"] is expected["has_open_positions"]
        assert machine_state["blocked_reason"] == case["reason"]
        assert machine_state["pending_signals"] == []
        assert machine_state["portfolio_risk_freeze_active"] is False


def _legacy_reduce_stop_monitor(state: dict[str, Any]) -> dict[str, Any]:
    has_open_positions = any(
        isinstance(item, Mapping) and item.get("status") != "closed"
        for item in state.get("positions") or []
    )
    if has_open_positions:
        raise RuntimeError("pump_live_monitor_required_while_positions_open")
    state["entry_armed"] = False
    state["monitor_enabled"] = False
    state["status"] = "stopped"
    state["transient_recovery_pending"] = False
    state["healthy_recovery_cycles"] = 0
    state["updated_at_ms"] = NOW_MS
    return {"stop_thread": True}


def test_stop_monitor_reducer_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent / "fixtures" / "pump_stop_monitor_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = PumpStateMachine()
    for case in cases:
        legacy_state = deepcopy(case["state"])
        machine_state = deepcopy(case["state"])
        if case.get("expected_error"):
            with pytest.raises(RuntimeError, match=case["expected_error"]):
                _legacy_reduce_stop_monitor(legacy_state)
            with pytest.raises(RuntimeError, match=case["expected_error"]):
                machine.reduce_stop_monitor(machine_state, now_ms=NOW_MS)
            assert machine_state == legacy_state == case["state"], case["name"]
            continue

        expected_result = _legacy_reduce_stop_monitor(legacy_state)
        actual_result = machine.reduce_stop_monitor(machine_state, now_ms=NOW_MS)
        assert machine_state == legacy_state, case["name"]
        assert actual_result == expected_result, case["name"]
        expected = case["expected"]
        for key in (
            "status",
            "entry_armed",
            "monitor_enabled",
            "transient_recovery_pending",
            "healthy_recovery_cycles",
            "updated_at_ms",
        ):
            assert machine_state[key] == expected[key], f"{case['name']}:{key}"
        assert actual_result["stop_thread"] is expected["stop_thread"]


def _legacy_reduce_emergency_request(state: dict[str, Any]) -> dict[str, Any]:
    state["entry_armed"] = False
    state["monitor_enabled"] = True
    state["emergency_close_requested"] = True
    state["status"] = "emergency_closing"
    state["transient_recovery_pending"] = False
    state["healthy_recovery_cycles"] = 0
    state["portfolio_risk_freeze_active"] = False
    state["portfolio_risk_freeze_reason"] = None
    state["portfolio_risk_freeze_symbol"] = None
    state["portfolio_risk_freeze_buffer_pct"] = None
    state["portfolio_risk_restore_armed"] = False
    state["portfolio_risk_recovery_cycles"] = 0
    state["updated_at_ms"] = NOW_MS
    return {"start_monitor": True, "wake_monitor": True}


def test_emergency_request_reducer_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent / "fixtures" / "pump_emergency_request_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = PumpStateMachine()
    for case in cases:
        legacy_state = deepcopy(case["state"])
        machine_state = deepcopy(case["state"])
        expected_result = _legacy_reduce_emergency_request(legacy_state)
        actual_result = machine.reduce_emergency_request(
            machine_state,
            now_ms=NOW_MS,
        )
        assert machine_state == legacy_state, case["name"]
        assert actual_result == expected_result, case["name"]
        for key, value in case["expected"].items():
            source = actual_result if key in actual_result else machine_state
            assert source[key] == value, f"{case['name']}:{key}"
