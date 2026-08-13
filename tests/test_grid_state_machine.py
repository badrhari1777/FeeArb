from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from execution.auto_arb_grid import (
    apply_grid_decision_confirmation,
    build_grid_pending_transition,
    complete_pending_grid_transition,
    decide_grid_transition,
    grid_transition_completion_tolerance,
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
