from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from execution.auto_arb_grid import (
    apply_grid_decision_confirmation,
    complete_pending_grid_transition,
    decide_grid_transition,
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
