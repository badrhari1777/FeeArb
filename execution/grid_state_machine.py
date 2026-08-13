from __future__ import annotations

from typing import Any, Mapping

from .auto_arb_grid import (
    apply_grid_decision_confirmation,
    build_grid_pending_transition,
    complete_pending_grid_transition,
    decide_grid_transition,
    grid_transition_completion_tolerance,
    reduce_partial_grid_transition,
)


class GridStateMachine:
    """Pure orchestration facade for one Grid quote cycle."""

    def reduce_quote_cycle(
        self,
        rule: dict[str, Any],
        *,
        entry_spread_pct: float | None,
        exit_spread_pct: float | None,
        now_iso: str,
        now_ts: float,
        retry_sec: float,
    ) -> dict[str, Any]:
        transition_event: dict[str, Any] | None = None
        live_transition: tuple[str, str, int, int] | None = None
        if entry_spread_pct is None or exit_spread_pct is None:
            rule["status"] = "waiting_data"
            rule["blocked_reason"] = "entry_or_exit_spread_unavailable"
            rule["pending_action"] = None
            rule["pending_samples"] = 0
            return {
                "action": "none",
                "decision": None,
                "transition_event": None,
                "live_transition": None,
            }

        mode = str(rule.get("mode") or "shadow")
        current_level = (
            int(rule.get("live_level") or 0)
            if mode == "live"
            else int(rule.get("shadow_level") or 0)
        )
        pending_transition = (
            dict(rule.get("pending_transition") or {}) if mode == "live" else {}
        )
        if pending_transition:
            last_execution = rule.get("last_execution")
            last_result = (
                last_execution.get("result")
                if isinstance(last_execution, Mapping)
                else None
            )
            completed_transition = complete_pending_grid_transition(
                rule,
                pending_transition=pending_transition,
                current_level=current_level,
                last_result=(last_result if isinstance(last_result, Mapping) else None),
                now_iso=now_iso,
                now_ts=now_ts,
                retry_sec=retry_sec,
            )
            if completed_transition:
                current_level = int(completed_transition["current_level"])
                decision = dict(completed_transition["decision"])
                transition_event = dict(completed_transition["transition_event"])
                pending_transition = dict(completed_transition["pending_transition"])
            else:
                partial_reduced = reduce_partial_grid_transition(
                    rule,
                    pending_transition=pending_transition,
                    current_level=current_level,
                    entry_spread_pct=entry_spread_pct,
                    exit_spread_pct=exit_spread_pct,
                    now_iso=now_iso,
                )
                decision = dict(partial_reduced["decision"])
                transition_event = (
                    dict(partial_reduced["transition_event"])
                    if partial_reduced.get("transition_event")
                    else None
                )
                pending_transition = dict(partial_reduced["pending_transition"])
        else:
            decision = decide_grid_transition(
                entry_spread_pct=entry_spread_pct,
                exit_spread_pct=exit_spread_pct,
                levels=rule.get("levels") or [],
                current_level=current_level,
                max_levels_per_cycle=rule.get("max_levels_per_cycle") or 1,
            )
        reduced = apply_grid_decision_confirmation(
            rule,
            decision=decision,
            mode=mode,
            current_level=current_level,
            pending_transition=(dict(pending_transition) if pending_transition else None),
            entry_spread_pct=entry_spread_pct,
            exit_spread_pct=exit_spread_pct,
            now_iso=now_iso,
            now_ts=now_ts,
        )
        if reduced.get("live_transition"):
            live_transition = tuple(reduced["live_transition"])
        if reduced.get("transition_event"):
            transition_event = dict(reduced["transition_event"])
        return {
            "action": str(reduced["action"]),
            "decision": decision,
            "transition_event": transition_event,
            "live_transition": live_transition,
        }

    def plan_transition_start(
        self,
        rule: Mapping[str, Any],
        *,
        action: str,
        from_level: int,
        to_level: int,
        current_hedged_qty: float,
        now_iso: str,
    ) -> dict[str, Any]:
        """Plan a transition I/O intent without refreshing or submitting orders."""
        levels = rule.get("levels") or []
        level_index = to_level - 1 if action == "enter" else from_level - 1
        if level_index < 0 or level_index >= len(levels):
            raise ValueError("Grid transition level is outside the configured range.")
        level_qty = float(levels[level_index].get("qty") or 0.0)
        level_target_qty = (
            float(levels[to_level - 1].get("cumulative_qty") or 0.0)
            if to_level > 0
            else 0.0
        )
        built = build_grid_pending_transition(
            existing_transition=rule.get("pending_transition") or {},
            action=action,
            from_level=from_level,
            to_level=to_level,
            level_qty=level_qty,
            level_target_qty=level_target_qty,
            current_hedged_qty=current_hedged_qty,
            now_iso=now_iso,
        )
        qty = float(built["qty"])
        total_transition_qty = float(built["total_transition_qty"])
        tolerance = grid_transition_completion_tolerance(
            rule,
            total_transition_qty,
        )
        return {
            "kind": "transition_complete" if qty <= tolerance else "submit_transition",
            "action": action,
            "from_level": from_level,
            "to_level": to_level,
            "qty": qty,
            "transition": dict(built["transition"]),
            "position_target_qty": float(built["position_target_qty"]),
            "entry_risk_target_qty": (
                current_hedged_qty + qty if action == "enter" else None
            ),
            "completion_tolerance_qty": tolerance,
        }
