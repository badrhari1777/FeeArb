from __future__ import annotations

from typing import Any, Mapping

from .auto_arb_grid import (
    apply_grid_decision_confirmation,
    build_grid_pending_transition,
    complete_pending_grid_transition,
    decide_grid_transition,
    grid_hedge_imbalance_tolerance,
    grid_transition_completion_tolerance,
    reduce_grid_hedge_repair_execution,
    reduce_grid_transition_execution,
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

    def plan_execution_reconcile_io(
        self,
        rule: Mapping[str, Any],
        *,
        run: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Plan reconcile I/O and reducer routing without reading exchange state."""
        execution_id = str(rule.get("active_execution_id") or "")
        active_action = str(rule.get("active_action") or "")
        base = {
            "execution_id": execution_id or None,
            "execution_status": None,
            "active_action": active_action or None,
            "from_level": rule.get("active_from_level"),
            "to_level": rule.get("active_to_level"),
            "requires_position_refresh": False,
            "reducer": None,
        }
        if not execution_id:
            return {
                **base,
                "kind": "no_active_execution",
                "completed": True,
            }
        if not isinstance(run, Mapping):
            return {
                **base,
                "kind": "missing_execution",
                "completed": False,
                "requires_position_refresh": True,
                "reducer": "missing_execution",
            }

        execution_status = str(run.get("status") or "")
        if execution_status == "running":
            return {
                **base,
                "kind": "execution_running",
                "completed": False,
                "execution_status": execution_status,
            }

        if active_action == "repair":
            reducer = "hedge_repair_execution"
        elif rule.get("pending_transition"):
            reducer = "transition_execution"
        else:
            reducer = "settle_without_transition"
        return {
            **base,
            "kind": "terminal_execution",
            "completed": False,
            "execution_status": execution_status,
            "requires_position_refresh": True,
            "reducer": reducer,
        }

    def reduce_execution_reconcile_after_refresh(
        self,
        rule: dict[str, Any],
        *,
        run: Mapping[str, Any],
        quantities: Mapping[str, Any] | None,
        reconcile_error: str | None,
        active_snapshot: Mapping[str, Any] | None,
        now_iso: str,
        now_ts: float,
        retry_sec: float,
    ) -> dict[str, Any]:
        """Reduce a terminal execution after the caller's position refresh."""
        snapshot = active_snapshot or rule
        intent = self.plan_execution_reconcile_io(rule, run=run)
        reconcile_reducer = intent.get("reducer")
        execution_id = str(
            snapshot.get("active_execution_id")
            or rule.get("active_execution_id")
            or ""
        )
        execution_status = str(run.get("status") or "")
        active_action = str(rule.get("active_action") or "")
        start_hedged_qty = self._optional_float(rule.get("active_start_hedged_qty"))

        rule["active_execution_id"] = None
        rule["active_action"] = None
        rule["active_from_level"] = None
        rule["active_to_level"] = None
        rule["active_target_qty"] = None
        rule["active_start_hedged_qty"] = None

        if quantities is None:
            rule["active_execution_id"] = execution_id
            rule["active_action"] = snapshot.get("active_action")
            rule["active_from_level"] = snapshot.get("active_from_level")
            rule["active_to_level"] = snapshot.get("active_to_level")
            rule["active_target_qty"] = snapshot.get("active_target_qty")
            rule["active_start_hedged_qty"] = snapshot.get(
                "active_start_hedged_qty"
            )
            rule["status"] = "waiting_reconcile"
            rule["blocked_reason"] = (
                f"position_refresh_failed: {reconcile_error}"
                if reconcile_error
                else f"execution_{execution_status or 'unknown'}"
            )
            rule["next_eligible_ts"] = now_ts + 30.0
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
        total_transition_qty = max(
            0.0,
            float(transition.get("target_qty") or 0.0),
        )
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
            "reconciled_at": now_iso,
        }

        repair_required = False
        completed = False
        event: dict[str, Any] = {}
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
                now_ts=now_ts,
                retry_sec=retry_sec,
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
                now_iso=now_iso,
                now_ts=now_ts,
                retry_sec=retry_sec,
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

    def plan_hedge_repair_start(
        self,
        rule: Mapping[str, Any],
        *,
        quantities: Mapping[str, Any],
        preflight: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Plan hedge-repair analysis or cleanup without mutating state or doing I/O."""
        long_qty = float(quantities.get("long_qty") or 0.0)
        short_qty = float(quantities.get("short_qty") or 0.0)
        hedged_qty = float(quantities.get("hedged_qty") or 0.0)
        imbalance_qty = abs(long_qty - short_qty)
        tolerance = grid_hedge_imbalance_tolerance(
            rule,
            hedged_qty=hedged_qty,
        )
        base = {
            "imbalance_qty": imbalance_qty,
            "tolerance_qty": tolerance,
            "hedged_qty": hedged_qty,
            "cleanup_exchange": None,
            "cleanup_side": None,
            "close_side": None,
            "min_qty_required": None,
            "analysis_request": None,
            "cleanup_payload": None,
        }
        if imbalance_qty <= tolerance:
            return {**base, "kind": "settle_within_tolerance"}

        cleanup_long = long_qty > short_qty
        cleanup_exchange = (
            rule.get("long_exchange")
            if cleanup_long
            else rule.get("short_exchange")
        )
        cleanup_side = "long" if cleanup_long else "short"
        close_side = "sell" if cleanup_side == "long" else "buy"
        max_slippage_bps = float(rule.get("max_slippage_bps") or 8.0)
        routed = {
            **base,
            "cleanup_exchange": cleanup_exchange,
            "cleanup_side": cleanup_side,
            "close_side": close_side,
        }
        if preflight is None:
            return {
                **routed,
                "kind": "analyze_cleanup",
                "analysis_request": {
                    "exchange": str(cleanup_exchange or ""),
                    "symbol": str(rule.get("symbol") or ""),
                    "side": close_side,
                    "qty_base": imbalance_qty,
                    "max_slippage_bps": max_slippage_bps,
                },
            }

        min_required = self._optional_float(preflight.get("min_qty_required"))
        if min_required and imbalance_qty < min_required:
            return {
                **routed,
                "kind": "settle_non_closeable_dust",
                "min_qty_required": min_required,
            }
        return {
            **routed,
            "kind": "submit_cleanup",
            "min_qty_required": min_required,
            "cleanup_payload": {
                "symbol": rule.get("symbol"),
                "qty": imbalance_qty,
                "cleanup_exchange": cleanup_exchange,
                "cleanup_position_side": cleanup_side,
                "panic_cleanup_mode": False,
                "max_slippage_bps": max_slippage_bps,
                "max_runtime_sec": 120,
                "reprice_sec": 4.0,
                "use_orderbook_check": True,
                "fallback_to_market": False,
                "async_run": True,
                "dry_run": False,
                "margin_mode": "isolated",
                "auto_arb_agent": True,
                "auto_arb_rule_id": rule.get("id"),
                "auto_arb_rule_generation": int(rule.get("generation") or 0),
            },
        }

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
