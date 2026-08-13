from __future__ import annotations

from execution.state_replay_contract import (
    GRID_STATE_SCHEMA,
    PUMP_STATE_SCHEMA,
    audit_grid_state,
    audit_pump_state,
    compare_restart,
    project_grid_restart,
    project_pump_restart,
)


def _pump_position(*, live_id: str = "live-1", symbol: str = "ACEUSDT") -> dict:
    return {
        "live_id": live_id,
        "account_alias": "bybit_pump",
        "symbol": symbol,
        "strategy_id": "main_pullback_tier",
        "status": "open",
        "qty": 100.0,
        "avg_entry_price": 1.0,
        "stop_price": 1.4,
        "tp_price": 0.75,
        "risk_policy_id": "v3_3000_pool600",
        "risk_policy": {"policy_id": "v3_3000_pool600"},
        "legs": [
            {"step": 1, "status": "filled", "filled_qty": 100.0},
            {"step": 2, "status": "planned", "filled_qty": 0.0},
        ],
    }


def _pump_state() -> dict:
    return {
        "schema": PUMP_STATE_SCHEMA,
        "status": "armed",
        "monitor_enabled": True,
        "entry_armed": True,
        "positions": [_pump_position()],
        "seen_events": ["event-1"],
        "pending_signals": [{"event_id": "new"}],
        "transient_recovery_pending": True,
        "healthy_recovery_cycles": 1,
        "close_recovery_pending": True,
        "close_recovery_symbol": "OLDUSDT",
        "close_recovery_healthy_cycles": 1,
        "portfolio_risk_restore_armed": True,
        "portfolio_risk_recovery_cycles": 1,
        "capital_manager": {"active_risk_policy_id": "v3_3000_pool600"},
        "portfolio_risk_freeze_active": False,
        "emergency_close_requested": False,
    }


def _grid_rule(rule_id: str = "grid-1", *, exchange: str = "bybit") -> dict:
    return {
        "id": rule_id,
        "version": 1,
        "generation": 2,
        "symbol": "TUTUSDT",
        "long_exchange": "kucoin",
        "short_exchange": exchange,
        "mode": "live",
        "enabled": True,
        "status": "monitoring",
        "live_level": 2,
        "actual_hedged_qty": 100.0,
        "active_execution_id": None,
        "active_action": None,
        "levels": [{"level": 1}, {"level": 2}],
    }


def test_pump_restart_projection_is_fail_closed_and_idempotent() -> None:
    before = _pump_state()
    after = project_pump_restart(before)
    second = project_pump_restart(after)

    assert after == second
    assert after["entry_armed"] is False
    assert after["pending_signals"] == []
    assert after["status"] == "recovery_monitoring"
    assert after["monitor_enabled"] is True
    assert after["positions"] == before["positions"]
    assert compare_restart("pump_live", before, after)["valid"] is True


def test_pump_contract_rejects_duplicate_ownership_and_ladder_steps() -> None:
    state = _pump_state()
    duplicate = _pump_position(live_id="live-2")
    duplicate["legs"][1]["step"] = 1
    state["positions"].append(duplicate)

    report = audit_pump_state(state).as_dict()

    assert report["valid"] is False
    assert {item["code"] for item in report["issues"]} >= {
        "duplicate_ownership",
        "invalid_leg_step",
    }


def test_pump_contract_rejects_unknown_schema_and_non_object_state() -> None:
    wrong_schema = _pump_state()
    wrong_schema["schema"] = "pump_live_state_v999"

    assert audit_pump_state(wrong_schema).valid is False
    assert audit_pump_state([wrong_schema]).valid is False


def test_pump_contract_rejects_nonfinite_persisted_number() -> None:
    state = _pump_state()
    state["positions"][0]["qty"] = float("nan")

    report = audit_pump_state(state).as_dict()

    assert report["valid"] is False
    assert any(item["code"] == "nonfinite_number" for item in report["issues"])


def test_pump_contract_marks_missing_protection_for_exchange_reconciliation() -> None:
    state = _pump_state()
    state["positions"][0]["stop_price"] = None

    report = audit_pump_state(state).as_dict()

    assert report["valid"] is True
    assert any(
        item["code"] == "protection_reconciliation_required"
        and item["severity"] == "warning"
        for item in report["issues"]
    )


def test_grid_restart_preserves_rules_and_requires_exchange_reconciliation() -> None:
    rule = _grid_rule()
    rule.update(
        {
            "active_execution_id": "execution-1",
            "active_action": "enter",
            "active_from_level": 2,
            "active_to_level": 3,
        }
    )
    before = {"schema": GRID_STATE_SCHEMA, "version": 1, "rules": {rule["id"]: rule}}
    after = project_grid_restart(before)

    report = audit_grid_state(after).as_dict()

    assert after == project_grid_restart(after)
    assert report["valid"] is True
    assert report["restart_actions"] == [
        {
            "action": "reconcile_execution_from_exchange",
            "rule_id": "grid-1",
            "execution_id": "execution-1",
        }
    ]
    assert compare_restart("grid", before, after)["valid"] is True


def test_grid_contract_accepts_legacy_schema_but_reports_migration() -> None:
    state = {"version": 1, "rules": {}}

    report = audit_grid_state(state).as_dict()

    assert report["valid"] is True
    assert report["issues"][0]["code"] == "legacy_schema_missing"
    assert project_grid_restart(state)["schema"] == GRID_STATE_SCHEMA


def test_grid_contract_rejects_overlapping_live_ownership() -> None:
    first = _grid_rule("grid-1", exchange="bybit")
    second = _grid_rule("grid-2", exchange="okx")
    state = {
        "schema": GRID_STATE_SCHEMA,
        "version": 1,
        "rules": {first["id"]: first, second["id"]: second},
    }

    report = audit_grid_state(state).as_dict()

    assert report["valid"] is False
    assert any(item["code"] == "duplicate_live_ownership" for item in report["issues"])


def test_grid_contract_rejects_execution_without_live_ownership() -> None:
    rule = _grid_rule()
    rule["mode"] = "shadow"
    rule["active_execution_id"] = "orphan-execution"
    state = {
        "schema": GRID_STATE_SCHEMA,
        "version": 1,
        "rules": {rule["id"]: rule},
    }

    report = audit_grid_state(state).as_dict()

    assert report["valid"] is False
    assert any(
        item["code"] == "execution_without_live_ownership"
        for item in report["issues"]
    )


def test_restart_comparison_detects_durable_state_loss() -> None:
    pump_before = _pump_state()
    pump_after = project_pump_restart(pump_before)
    pump_after["positions"] = []
    grid_rule = _grid_rule()
    grid_before = {
        "schema": GRID_STATE_SCHEMA,
        "version": 1,
        "rules": {grid_rule["id"]: grid_rule},
    }
    grid_after = project_grid_restart(grid_before)
    grid_after["rules"] = {}

    assert compare_restart("pump_live", pump_before, pump_after)["valid"] is False
    assert compare_restart("grid", grid_before, grid_after)["valid"] is False
