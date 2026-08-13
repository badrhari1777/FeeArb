from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from execution.auto_arb_grid import (
    apply_grid_decision_confirmation,
    build_grid_pending_transition,
    complete_pending_grid_transition,
    decide_grid_transition,
    grid_hedge_imbalance_tolerance,
    grid_non_closeable_dust,
    grid_reset_after_flat_repair,
    grid_transition_completion_tolerance,
    reduce_grid_hedge_repair_execution,
    reduce_grid_transition_execution,
    reduce_partial_grid_transition,
)
from execution.grid_state_machine import GridStateMachine


NOW_ISO = "2026-08-13T00:00:00+00:00"
NOW_TS = 100.0
RETRY_SEC = 2.0


def _legacy_reduce_quote_cycle(rule, entry_spread_pct, exit_spread_pct):
    transition_event = None
    live_transition = None
    if entry_spread_pct is None or exit_spread_pct is None:
        rule["status"] = "waiting_data"
        rule["blocked_reason"] = "entry_or_exit_spread_unavailable"
        rule["pending_action"] = None
        rule["pending_samples"] = 0
        return {"action": "none", "decision": None, "transition_event": None, "live_transition": None}
    mode = str(rule.get("mode") or "shadow")
    current_level = int(rule.get("live_level") or 0) if mode == "live" else int(rule.get("shadow_level") or 0)
    pending_transition = dict(rule.get("pending_transition") or {}) if mode == "live" else {}
    if pending_transition:
        last_execution = rule.get("last_execution")
        last_result = last_execution.get("result") if isinstance(last_execution, dict) else None
        completed = complete_pending_grid_transition(
            rule, pending_transition=pending_transition, current_level=current_level,
            last_result=last_result if isinstance(last_result, dict) else None,
            now_iso=NOW_ISO, now_ts=NOW_TS, retry_sec=RETRY_SEC,
        )
        if completed:
            current_level = int(completed["current_level"])
            decision = dict(completed["decision"])
            transition_event = dict(completed["transition_event"])
            pending_transition = dict(completed["pending_transition"])
        else:
            partial = reduce_partial_grid_transition(
                rule, pending_transition=pending_transition, current_level=current_level,
                entry_spread_pct=entry_spread_pct, exit_spread_pct=exit_spread_pct,
                now_iso=NOW_ISO,
            )
            decision = dict(partial["decision"])
            transition_event = dict(partial["transition_event"]) if partial.get("transition_event") else None
            pending_transition = dict(partial["pending_transition"])
    else:
        decision = decide_grid_transition(
            entry_spread_pct=entry_spread_pct, exit_spread_pct=exit_spread_pct,
            levels=rule.get("levels") or [], current_level=current_level,
            max_levels_per_cycle=rule.get("max_levels_per_cycle") or 1,
        )
    reduced = apply_grid_decision_confirmation(
        rule, decision=decision, mode=mode, current_level=current_level,
        pending_transition=dict(pending_transition) if pending_transition else None,
        entry_spread_pct=entry_spread_pct, exit_spread_pct=exit_spread_pct,
        now_iso=NOW_ISO, now_ts=NOW_TS,
    )
    if reduced.get("live_transition"):
        live_transition = tuple(reduced["live_transition"])
    if reduced.get("transition_event"):
        transition_event = dict(reduced["transition_event"])
    return {"action": str(reduced["action"]), "decision": decision, "transition_event": transition_event, "live_transition": live_transition}


def test_grid_state_machine_matches_legacy_cycle_golden_cases() -> None:
    cases = json.loads((Path(__file__).parent / "fixtures" / "grid_quote_cycle_v1.json").read_text(encoding="utf-8"))
    machine = GridStateMachine()
    for case in cases:
        legacy_rule = deepcopy(case["rule"])
        machine_rule = deepcopy(case["rule"])
        legacy_result = _legacy_reduce_quote_cycle(legacy_rule, case["entry_spread_pct"], case["exit_spread_pct"])
        machine_result = machine.reduce_quote_cycle(
            machine_rule,
            entry_spread_pct=case["entry_spread_pct"],
            exit_spread_pct=case["exit_spread_pct"],
            now_iso=NOW_ISO,
            now_ts=NOW_TS,
            retry_sec=RETRY_SEC,
        )
        assert machine_rule == legacy_rule, case["name"]
        assert machine_result == legacy_result, case["name"]


def _legacy_plan_transition_start(rule, action, from_level, to_level, current_hedged_qty):
    levels = rule.get("levels") or []
    level_index = to_level - 1 if action == "enter" else from_level - 1
    if level_index < 0 or level_index >= len(levels):
        raise ValueError("Grid transition level is outside the configured range.")
    level_qty = float(levels[level_index].get("qty") or 0.0)
    level_target_qty = float(levels[to_level - 1].get("cumulative_qty") or 0.0) if to_level > 0 else 0.0
    built = build_grid_pending_transition(
        existing_transition=rule.get("pending_transition") or {}, action=action,
        from_level=from_level, to_level=to_level, level_qty=level_qty,
        level_target_qty=level_target_qty, current_hedged_qty=current_hedged_qty,
        now_iso=NOW_ISO,
    )
    qty = float(built["qty"])
    total = float(built["total_transition_qty"])
    tolerance = grid_transition_completion_tolerance(rule, total)
    return {
        "kind": "transition_complete" if qty <= tolerance else "submit_transition",
        "action": action, "from_level": from_level, "to_level": to_level,
        "qty": qty, "transition": dict(built["transition"]),
        "position_target_qty": float(built["position_target_qty"]),
        "entry_risk_target_qty": current_hedged_qty + qty if action == "enter" else None,
        "completion_tolerance_qty": tolerance,
    }


def test_transition_start_intent_matches_legacy_planning() -> None:
    cases = json.loads((Path(__file__).parent / "fixtures" / "grid_transition_start_intent_v1.json").read_text(encoding="utf-8"))
    machine = GridStateMachine()
    for case in cases:
        expected = _legacy_plan_transition_start(
            case["rule"], case["action"], case["from_level"], case["to_level"],
            case["current_hedged_qty"],
        )
        actual = machine.plan_transition_start(
            case["rule"], action=case["action"], from_level=case["from_level"],
            to_level=case["to_level"], current_hedged_qty=case["current_hedged_qty"],
            now_iso=NOW_ISO,
        )
        assert actual == expected, case["name"]


def test_execution_reconcile_io_plan_matches_golden_cases_without_mutation() -> None:
    cases = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "grid_execution_reconcile_io_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = GridStateMachine()
    for case in cases:
        rule = deepcopy(case["rule"])
        run = deepcopy(case.get("run"))
        original_rule = deepcopy(rule)
        original_run = deepcopy(run)
        actual = machine.plan_execution_reconcile_io(rule, run=run)
        assert actual == case["expected"], case["name"]
        assert rule == original_rule, case["name"]
        assert run == original_run, case["name"]


def _legacy_reduce_execution_reconcile_after_refresh(
    rule, *, run, quantities, reconcile_error, active_snapshot
):
    snapshot = active_snapshot or rule
    execution_id = str(
        snapshot.get("active_execution_id")
        or rule.get("active_execution_id")
        or ""
    )
    execution_status = str(run.get("status") or "")
    active_action = str(rule.get("active_action") or "")
    try:
        start_hedged_qty = (
            None
            if rule.get("active_start_hedged_qty") is None
            else float(rule.get("active_start_hedged_qty"))
        )
    except (TypeError, ValueError):
        start_hedged_qty = None
    reconcile_reducer = (
        "hedge_repair_execution"
        if active_action == "repair"
        else (
            "transition_execution"
            if rule.get("pending_transition")
            else "settle_without_transition"
        )
    )
    for key in (
        "active_execution_id",
        "active_action",
        "active_from_level",
        "active_to_level",
        "active_target_qty",
        "active_start_hedged_qty",
    ):
        rule[key] = None
    if quantities is None:
        rule["active_execution_id"] = execution_id
        for key in (
            "active_action",
            "active_from_level",
            "active_to_level",
            "active_target_qty",
            "active_start_hedged_qty",
        ):
            rule[key] = snapshot.get(key)
        rule["status"] = "waiting_reconcile"
        rule["blocked_reason"] = (
            f"position_refresh_failed: {reconcile_error}"
            if reconcile_error
            else f"execution_{execution_status or 'unknown'}"
        )
        rule["next_eligible_ts"] = NOW_TS + 30.0
        return {
            "completed": False,
            "repair_required": False,
            "event": {
                "event": "live_reconcile_deferred",
                "error": reconcile_error or run.get("error"),
                "result": run.get("result"),
            },
        }

    hedged_qty = float(quantities.get("hedged_qty") or 0.0)
    imbalance_qty = float(quantities.get("imbalance_qty") or 0.0)
    transition = dict(rule.get("pending_transition") or {})
    total_transition_qty = max(0.0, float(transition.get("target_qty") or 0.0))
    hedge_tolerance = grid_hedge_imbalance_tolerance(
        rule,
        transition_qty=total_transition_qty or None,
        hedged_qty=hedged_qty,
    )
    rule["actual_hedged_qty"] = hedged_qty
    rule["last_execution"] = {
        "execution_id": execution_id,
        "status": execution_status,
        "error": run.get("error"),
        "result": run.get("result"),
        "observed_hedged_qty": hedged_qty,
        "observed_imbalance_qty": imbalance_qty,
        "reconciled_at": NOW_ISO,
    }
    completed = False
    repair_required = False
    event = {}
    if reconcile_reducer == "hedge_repair_execution":
        reduced = reduce_grid_hedge_repair_execution(
            rule,
            transition=transition,
            execution_status=execution_status,
            execution_error=run.get("error"),
            execution_result=run.get("result"),
            hedged_qty=hedged_qty,
            imbalance_qty=imbalance_qty,
            hedge_tolerance=hedge_tolerance,
            now_ts=NOW_TS,
            retry_sec=RETRY_SEC,
        )
        event.update(reduced["event"])
        repair_required = bool(reduced["retry_repair"])
        completed = bool(reduced["completed"])
    elif reconcile_reducer == "transition_execution":
        reduced = reduce_grid_transition_execution(
            rule,
            transition=transition,
            active_action=active_action,
            execution_id=execution_id,
            execution_status=execution_status,
            execution_error=run.get("error"),
            execution_result=run.get("result"),
            hedged_qty=hedged_qty,
            imbalance_qty=imbalance_qty,
            start_hedged_qty=start_hedged_qty,
            now_iso=NOW_ISO,
            now_ts=NOW_TS,
            retry_sec=RETRY_SEC,
        )
        event.update(reduced["event"])
        repair_required = bool(reduced["repair_required"])
        completed = bool(reduced["completed"])
    else:
        rule["status"] = "monitoring"
        rule["blocked_reason"] = None
        completed = True
    if (
        not rule.get("enabled")
        and imbalance_qty <= hedge_tolerance
        and not repair_required
    ):
        rule["status"] = "paused"
        rule["blocked_reason"] = None
        rule["next_eligible_ts"] = 0.0
    return {
        "completed": completed,
        "repair_required": repair_required,
        "event": event,
    }


def test_execution_reconcile_after_refresh_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "grid_execution_reconcile_after_refresh_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = GridStateMachine()
    for case in cases:
        legacy_rule = deepcopy(case["rule"])
        machine_rule = deepcopy(case["rule"])
        kwargs = {
            "run": deepcopy(case["run"]),
            "quantities": deepcopy(case.get("quantities")),
            "reconcile_error": case.get("reconcile_error"),
            "active_snapshot": deepcopy(case.get("active_snapshot")),
        }
        expected_result = _legacy_reduce_execution_reconcile_after_refresh(
            legacy_rule,
            **deepcopy(kwargs),
        )
        actual_result = machine.reduce_execution_reconcile_after_refresh(
            machine_rule,
            **deepcopy(kwargs),
            now_iso=NOW_ISO,
            now_ts=NOW_TS,
            retry_sec=RETRY_SEC,
        )
        assert machine_rule == legacy_rule, case["name"]
        assert actual_result == expected_result, case["name"]
        expected = case["expected"]
        assert actual_result["completed"] == expected["completed"], case["name"]
        assert (
            actual_result["repair_required"] == expected["repair_required"]
        ), case["name"]
        assert (
            actual_result["event"].get("event") == expected["event"]
        ), case["name"]
        assert machine_rule["status"] == expected["status"], case["name"]
        if "next_eligible_ts" in expected:
            assert (
                machine_rule["next_eligible_ts"] == expected["next_eligible_ts"]
            ), case["name"]


def test_hedge_repair_start_intent_matches_golden_cases_without_mutation() -> None:
    cases = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "grid_hedge_repair_start_intent_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = GridStateMachine()
    for case in cases:
        rule = deepcopy(case["rule"])
        quantities = deepcopy(case["quantities"])
        preflight = deepcopy(case.get("preflight"))
        original_rule = deepcopy(rule)
        original_quantities = deepcopy(quantities)
        original_preflight = deepcopy(preflight)
        actual = machine.plan_hedge_repair_start(
            rule,
            quantities=quantities,
            preflight=preflight,
        )
        for key, expected in case["expected"].items():
            assert actual.get(key) == expected, f"{case['name']}:{key}"
        assert rule == original_rule, case["name"]
        assert quantities == original_quantities, case["name"]
        assert preflight == original_preflight, case["name"]


def _legacy_reduce_hedge_repair_settle(
    rule, *, repair_intent, quantities, preflight
):
    kind = repair_intent["kind"]
    hedged_qty = float(quantities.get("hedged_qty") or 0.0)
    transition = dict(rule.get("pending_transition") or {})
    remaining_qty = max(0.0, float(transition.get("remaining_qty") or 0.0))
    flat_repair_reset = False
    non_closeable_dust = False
    if kind == "settle_within_tolerance":
        transition_action = str(transition.get("action") or "")
        transition_tolerance = grid_transition_completion_tolerance(
            rule,
            float(transition.get("target_qty") or 0.0) or None,
        )
        last_execution = rule.get("last_execution")
        last_result = (
            last_execution.get("result")
            if isinstance(last_execution, dict)
            else None
        )
        non_closeable_dust = (
            bool(transition)
            and remaining_qty > 0
            and grid_non_closeable_dust(last_result, remaining_qty)
        )
        flat_repair_reset = grid_reset_after_flat_repair(rule, hedged_qty)
        if flat_repair_reset:
            rule["status"] = "waiting_entry"
        elif transition and (
            remaining_qty <= transition_tolerance or non_closeable_dust
        ):
            target_level = int(
                transition.get("to_level") or rule.get("live_level") or 0
            )
            rule["live_level"] = target_level
            rule["pending_transition"] = None
            rule["status"] = (
                "waiting_entry" if target_level == 0 else "monitoring"
            )
        elif transition:
            rule["status"] = f"partial_{transition_action or 'transition'}"
        else:
            rule["status"] = (
                "waiting_entry" if not rule.get("live_level") else "monitoring"
            )
    else:
        target_level = int(
            transition.get("to_level") or rule.get("live_level") or 0
        )
        rule["live_level"] = target_level
        rule["pending_transition"] = None
        rule["status"] = "waiting_entry" if target_level == 0 else "monitoring"
    for key in (
        "active_execution_id",
        "active_action",
        "active_from_level",
        "active_to_level",
        "active_target_qty",
        "active_start_hedged_qty",
    ):
        rule[key] = None
    rule["actual_hedged_qty"] = hedged_qty
    rule["blocked_reason"] = None
    rule["pending_action"] = None
    rule["pending_samples"] = 0
    rule["next_eligible_ts"] = NOW_TS + RETRY_SEC
    if kind == "settle_non_closeable_dust":
        event = {
            "event": "live_hedge_repair_non_closeable_dust",
            "live_level": int(rule.get("live_level") or 0),
            "cleanup_exchange": repair_intent.get("cleanup_exchange"),
            "cleanup_side": repair_intent.get("cleanup_side"),
            "imbalance_qty": float(repair_intent.get("imbalance_qty") or 0.0),
            "min_qty_required": repair_intent.get("min_qty_required"),
            "preflight": dict(preflight or {}),
        }
    else:
        event = {
            "event": "live_hedge_imbalance_within_tolerance",
            "imbalance_qty": float(repair_intent.get("imbalance_qty") or 0.0),
            "tolerance_qty": float(repair_intent.get("tolerance_qty") or 0.0),
            "remaining_qty": remaining_qty,
            "non_closeable_dust_completed": bool(non_closeable_dust),
            "flat_repair_reset": flat_repair_reset,
        }
    return {"event": event}


def test_hedge_repair_settle_reducer_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "grid_hedge_repair_settle_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = GridStateMachine()
    for case in cases:
        legacy_rule = deepcopy(case["rule"])
        machine_rule = deepcopy(case["rule"])
        kwargs = {
            "repair_intent": deepcopy(case["repair_intent"]),
            "quantities": deepcopy(case["quantities"]),
            "preflight": deepcopy(case.get("preflight")),
        }
        expected_result = _legacy_reduce_hedge_repair_settle(
            legacy_rule,
            **deepcopy(kwargs),
        )
        actual_result = machine.reduce_hedge_repair_settle(
            machine_rule,
            **deepcopy(kwargs),
            now_ts=NOW_TS,
            retry_sec=RETRY_SEC,
        )
        assert machine_rule == legacy_rule, case["name"]
        assert actual_result == expected_result, case["name"]
        expected = case["expected"]
        assert machine_rule["status"] == expected["status"], case["name"]
        assert machine_rule["live_level"] == expected["live_level"], case["name"]
        assert actual_result["event"]["event"] == expected["event"], case["name"]
        for key in (
            "pending_transition",
            "flat_repair_reset",
            "non_closeable_dust_completed",
            "remaining_qty",
            "cleanup_exchange",
            "cleanup_side",
        ):
            if key in expected:
                source = actual_result["event"] if key in actual_result["event"] else machine_rule
                assert source.get(key) == expected[key], f"{case['name']}:{key}"
        assert machine_rule["active_execution_id"] is None, case["name"]


def _legacy_reduce_hedge_repair_worker_start(
    rule, *, result, quantities, repair_intent
):
    worker_result = result or {}
    execution_id = str(worker_result.get("execution_id") or "")
    if execution_id:
        rule["active_execution_id"] = execution_id
        rule["active_action"] = "repair"
        rule["active_start_hedged_qty"] = float(
            quantities.get("hedged_qty") or 0.0
        )
        rule["status"] = "repairing_hedge"
        rule["blocked_reason"] = None
    else:
        rule["status"] = "hedge_repair_retry"
        rule["blocked_reason"] = str(
            worker_result.get("error") or "hedge_repair_worker_busy"
        )
        rule["next_eligible_ts"] = NOW_TS + RETRY_SEC
    return {
        "event": {
            "event": (
                "live_hedge_repair_started"
                if execution_id
                else "live_hedge_repair_deferred"
            ),
            "execution_id": execution_id or None,
            "cleanup_exchange": repair_intent.get("cleanup_exchange"),
            "cleanup_side": repair_intent.get("cleanup_side"),
            "qty": float(repair_intent.get("imbalance_qty") or 0.0),
            "result": result,
        }
    }


def test_hedge_repair_worker_start_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "grid_hedge_repair_worker_start_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = GridStateMachine()
    for case in cases:
        legacy_rule = deepcopy(case["rule"])
        machine_rule = deepcopy(case["rule"])
        kwargs = {
            "result": deepcopy(case.get("result")),
            "quantities": deepcopy(case["quantities"]),
            "repair_intent": deepcopy(case["repair_intent"]),
        }
        expected_result = _legacy_reduce_hedge_repair_worker_start(
            legacy_rule,
            **deepcopy(kwargs),
        )
        actual_result = machine.reduce_hedge_repair_worker_start(
            machine_rule,
            **deepcopy(kwargs),
            now_ts=NOW_TS,
            retry_sec=RETRY_SEC,
        )
        assert machine_rule == legacy_rule, case["name"]
        assert actual_result == expected_result, case["name"]
        expected = case["expected"]
        for key in (
            "status",
            "blocked_reason",
            "active_execution_id",
            "active_action",
            "active_start_hedged_qty",
            "next_eligible_ts",
        ):
            if key in expected:
                assert machine_rule.get(key) == expected[key], f"{case['name']}:{key}"
        assert actual_result["event"]["event"] == expected["event"], case["name"]
        assert (
            actual_result["event"]["execution_id"] == expected["execution_id"]
        ), case["name"]


def _legacy_reduce_transition_worker_start(
    rule,
    *,
    result,
    transition,
    quantities,
    action,
    from_level,
    to_level,
    position_target_qty,
    submit_qty,
):
    worker_result = result or {}
    execution_id = str(worker_result.get("execution_id") or "")
    current_transition = dict(transition)
    rule.pop("transition_starting", None)
    rule["pending_action"] = None
    rule["pending_samples"] = 0
    if execution_id:
        start_hedged_qty = float(quantities.get("hedged_qty") or 0.0)
        current_transition["last_start_hedged_qty"] = start_hedged_qty
        current_transition["updated_at"] = NOW_ISO
        rule["pending_transition"] = current_transition
        rule["active_execution_id"] = execution_id
        rule["active_action"] = action
        rule["active_from_level"] = from_level
        rule["active_to_level"] = to_level
        rule["active_target_qty"] = position_target_qty
        rule["active_start_hedged_qty"] = start_hedged_qty
        rule["status"] = f"executing_{action}"
        rule["blocked_reason"] = None
    else:
        rule["pending_transition"] = current_transition
        rule["status"] = "blocked_conflict"
        rule["blocked_reason"] = str(
            worker_result.get("error") or "execution_worker_busy"
        )
        rule["next_eligible_ts"] = NOW_TS + RETRY_SEC
    return {
        "event": {
            "event": (
                f"live_{action}_started" if execution_id else "live_start_failed"
            ),
            "execution_id": execution_id or None,
            "from_level": from_level,
            "to_level": to_level,
            "qty": submit_qty,
            "liquidity_chunking": True,
            "result": result,
        },
        "transition": current_transition,
    }


def test_transition_worker_start_matches_legacy_golden_cases() -> None:
    cases = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "grid_transition_worker_start_v1.json"
        ).read_text(encoding="utf-8")
    )
    machine = GridStateMachine()
    for case in cases:
        legacy_rule = deepcopy(case["rule"])
        machine_rule = deepcopy(case["rule"])
        kwargs = {
            "result": deepcopy(case.get("result")),
            "transition": deepcopy(case["transition"]),
            "quantities": deepcopy(case["quantities"]),
            "action": case["action"],
            "from_level": case["from_level"],
            "to_level": case["to_level"],
            "position_target_qty": case["position_target_qty"],
            "submit_qty": case["submit_qty"],
        }
        expected_result = _legacy_reduce_transition_worker_start(
            legacy_rule,
            **deepcopy(kwargs),
        )
        actual_result = machine.reduce_transition_worker_start(
            machine_rule,
            **deepcopy(kwargs),
            now_iso=NOW_ISO,
            now_ts=NOW_TS,
            retry_sec=RETRY_SEC,
        )
        assert machine_rule == legacy_rule, case["name"]
        assert actual_result == expected_result, case["name"]
        expected = case["expected"]
        for key in (
            "status",
            "blocked_reason",
            "active_execution_id",
            "active_action",
            "active_from_level",
            "active_to_level",
            "active_target_qty",
            "active_start_hedged_qty",
            "next_eligible_ts",
        ):
            if key in expected:
                assert machine_rule.get(key) == expected[key], f"{case['name']}:{key}"
        assert actual_result["event"]["event"] == expected["event"], case["name"]
        assert (
            actual_result["event"]["execution_id"] == expected["execution_id"]
        ), case["name"]
        assert "transition_starting" not in machine_rule, case["name"]
