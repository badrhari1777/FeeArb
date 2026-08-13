from __future__ import annotations

from typing import Any, Mapping


class PumpStateMachine:
    """Pure persisted-state reducers for Pump Live orchestration."""

    @staticmethod
    def reduce_cold_restart(state: dict[str, Any]) -> dict[str, Any]:
        """Fail closed after process restart without performing I/O.

        The caller remains responsible for persisting the mutation and starting
        recovery monitoring only after constructing the rest of the controller.
        """

        state["entry_armed"] = False
        state["transient_recovery_pending"] = False
        state["healthy_recovery_cycles"] = 0
        state["portfolio_risk_restore_armed"] = False
        state["portfolio_risk_recovery_cycles"] = 0

        positions = state.get("positions") or []
        has_open_positions = any(
            isinstance(item, Mapping) and item.get("status") != "closed"
            for item in positions
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

    @staticmethod
    def reduce_disarm(
        state: dict[str, Any],
        *,
        reason: str,
        now_ms: int,
    ) -> dict[str, Any]:
        """Disable entries while preserving monitoring for owned positions."""

        positions = state.get("positions") or []
        has_open_positions = any(
            isinstance(item, Mapping) and item.get("status") != "closed"
            for item in positions
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
        state["updated_at_ms"] = now_ms
        state["pending_signals"] = []
        return {"has_open_positions": has_open_positions}

    @staticmethod
    def reduce_stop_monitor(
        state: dict[str, Any],
        *,
        now_ms: int,
    ) -> dict[str, Any]:
        """Stop a flat controller, but never abandon an owned position."""

        positions = state.get("positions") or []
        has_open_positions = any(
            isinstance(item, Mapping) and item.get("status") != "closed"
            for item in positions
        )
        if has_open_positions:
            raise RuntimeError("pump_live_monitor_required_while_positions_open")
        state["entry_armed"] = False
        state["monitor_enabled"] = False
        state["status"] = "stopped"
        state["transient_recovery_pending"] = False
        state["healthy_recovery_cycles"] = 0
        state["updated_at_ms"] = now_ms
        return {"stop_thread": True}

    @staticmethod
    def reduce_emergency_request(
        state: dict[str, Any],
        *,
        now_ms: int,
    ) -> dict[str, Any]:
        """Freeze entries and request emergency processing by the monitor."""

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
        state["updated_at_ms"] = now_ms
        return {"start_monitor": True, "wake_monitor": True}

    @staticmethod
    def reduce_monitor_error(
        state: dict[str, Any],
        *,
        error: str,
        transient: bool,
        now_ms: int,
    ) -> dict[str, Any]:
        """Fail closed after an already-classified monitor-cycle failure."""

        recovery_pending = bool(
            transient
            and (
                state.get("entry_armed")
                or state.get("transient_recovery_pending")
            )
        )
        state["last_error"] = error
        state["entry_armed"] = False
        state["transient_recovery_pending"] = recovery_pending
        state["healthy_recovery_cycles"] = 0
        if recovery_pending:
            state["status"] = "recovering_monitor"
            state["blocked_reason"] = "monitor_cycle_transient_error"
        else:
            state["status"] = "error_monitoring"
            state["blocked_reason"] = "monitor_cycle_error"
        state["updated_at_ms"] = now_ms
        return {"recovery_pending": recovery_pending}

    @staticmethod
    def reduce_arm_success(
        state: dict[str, Any],
        *,
        now_ms: int,
    ) -> dict[str, Any]:
        """Arm only after the controller has completed every external check."""

        state.update(
            {
                "status": "armed",
                "monitor_enabled": True,
                "entry_armed": True,
                "armed_at_ms": now_ms,
                "updated_at_ms": now_ms,
                "blocked_reason": None,
                "pending_signals": [],
                "transient_recovery_pending": False,
                "healthy_recovery_cycles": 0,
                "portfolio_risk_freeze_active": False,
                "portfolio_risk_freeze_reason": None,
                "portfolio_risk_freeze_symbol": None,
                "portfolio_risk_freeze_buffer_pct": None,
                "portfolio_risk_restore_armed": False,
                "portfolio_risk_recovery_cycles": 0,
            }
        )
        return {"start_monitor": True}

    @staticmethod
    def advance_monitor_health(
        state: dict[str, Any],
        *,
        recovery_cycles: int,
    ) -> dict[str, Any]:
        """Advance transient recovery before close-recovery reconciliation."""

        recovery_pending = bool(
            state.get("transient_recovery_pending")
            and state.get("blocked_reason") == "monitor_cycle_transient_error"
            and not state.get("portfolio_risk_freeze_active")
        )
        recovered = False
        if recovery_pending:
            healthy = int(state.get("healthy_recovery_cycles") or 0) + 1
            state["healthy_recovery_cycles"] = healthy
            if healthy >= recovery_cycles:
                state["entry_armed"] = True
                state["transient_recovery_pending"] = False
                state["healthy_recovery_cycles"] = 0
                state["blocked_reason"] = None
                recovered = True
        return {
            "recovery_pending": recovery_pending,
            "recovered": recovered,
        }

    @staticmethod
    def finalize_monitor_success(
        state: dict[str, Any],
        *,
        recovery_pending: bool,
        recovered: bool,
        close_recovered: bool,
        now_ms: int,
    ) -> dict[str, Any]:
        """Finalize a healthy cycle after close-recovery reconciliation."""

        if state.get("monitor_enabled") and not recovery_pending:
            state["status"] = "armed" if state.get("entry_armed") else "monitoring"
        elif state.get("monitor_enabled"):
            state["status"] = "recovering_monitor"
        if recovered or close_recovered:
            state["status"] = "armed"
        state["last_error"] = None
        state["updated_at_ms"] = now_ms
        return {
            "emit_monitor_recovered": recovered,
            "emit_close_recovered": close_recovered,
        }

    @staticmethod
    def reduce_close_recovery(
        state: dict[str, Any],
        *,
        evidence_ready: bool,
        recovery_cycles: int,
    ) -> dict[str, Any]:
        """Advance close recovery from controller-supplied exchange evidence."""

        if not state.get("close_recovery_pending"):
            return {"recovered": False}
        if state.get("blocked_reason") != "position_absent_unconfirmed":
            state["close_recovery_pending"] = False
            state["close_recovery_healthy_cycles"] = 0
            return {"recovered": False}
        if not evidence_ready:
            return {"recovered": False}
        healthy = int(state.get("close_recovery_healthy_cycles") or 0) + 1
        state["close_recovery_healthy_cycles"] = healthy
        if healthy < recovery_cycles:
            return {"recovered": False}
        state["entry_armed"] = True
        state["monitor_enabled"] = True
        state["blocked_reason"] = None
        state["close_recovery_pending"] = False
        state["close_recovery_symbol"] = None
        state["close_recovery_healthy_cycles"] = 0
        return {"recovered": True}

    @staticmethod
    def reduce_portfolio_risk_freeze(
        state: dict[str, Any],
        *,
        snapshot: Mapping[str, Any],
        now_ms: int,
    ) -> dict[str, Any]:
        """Apply an already-calculated portfolio entry-risk freeze."""

        was_active = bool(state.get("portfolio_risk_freeze_active"))
        prior_reason = state.get("portfolio_risk_freeze_reason")
        prior_symbol = state.get("portfolio_risk_freeze_symbol")
        restore_armed = bool(state.get("portfolio_risk_restore_armed"))
        may_claim_entry_gate = bool(
            (state.get("entry_armed") and not state.get("blocked_reason"))
            or (
                was_active
                and state.get("blocked_reason") == "portfolio_risk_freeze"
            )
        )
        if state.get("entry_armed") and not state.get("blocked_reason"):
            restore_armed = True
        dropped_pending = len(state.get("pending_signals") or [])
        state["entry_armed"] = False
        state["portfolio_risk_freeze_active"] = True
        state["portfolio_risk_freeze_reason"] = snapshot.get("reason")
        state["portfolio_risk_freeze_symbol"] = snapshot.get("symbol")
        state["portfolio_risk_freeze_buffer_pct"] = snapshot.get("buffer_pct")
        state["portfolio_risk_restore_armed"] = restore_armed
        state["portfolio_risk_recovery_cycles"] = 0
        state["pending_signals"] = []
        if may_claim_entry_gate:
            state["blocked_reason"] = "portfolio_risk_freeze"
            state["status"] = "monitoring"
        state["updated_at_ms"] = now_ms
        changed = bool(
            not was_active
            or prior_reason != snapshot.get("reason")
            or prior_symbol != snapshot.get("symbol")
        )
        return {
            "changed": changed,
            "dropped_pending_signals": dropped_pending,
            "auto_recovery_eligible": restore_armed,
        }

    @staticmethod
    def reduce_portfolio_risk_recovery(
        state: dict[str, Any],
        *,
        snapshot: Mapping[str, Any],
        evidence_ready: bool,
        recovery_cycles: int,
        now_ms: int,
    ) -> dict[str, Any]:
        """Advance risk recovery from calculated market/exchange evidence."""

        if not state.get("portfolio_risk_freeze_active"):
            return {"recovered": False, "save_state": False}
        state["portfolio_risk_freeze_buffer_pct"] = snapshot.get("buffer_pct")
        if snapshot.get("freeze_required") or not snapshot.get("all_calm"):
            state["portfolio_risk_recovery_cycles"] = 0
            return {"recovered": False, "save_state": True}
        restore_armed = bool(state.get("portfolio_risk_restore_armed"))
        if not restore_armed or state.get("blocked_reason") != "portfolio_risk_freeze":
            state["portfolio_risk_freeze_active"] = False
            state["portfolio_risk_freeze_reason"] = None
            state["portfolio_risk_freeze_symbol"] = None
            state["portfolio_risk_freeze_buffer_pct"] = None
            state["portfolio_risk_restore_armed"] = False
            state["portfolio_risk_recovery_cycles"] = 0
            return {"recovered": False, "save_state": True}
        if not evidence_ready:
            state["portfolio_risk_recovery_cycles"] = 0
            return {"recovered": False, "save_state": True}
        healthy = int(state.get("portfolio_risk_recovery_cycles") or 0) + 1
        state["portfolio_risk_recovery_cycles"] = healthy
        state["monitor_enabled"] = True
        if healthy < recovery_cycles:
            return {"recovered": False, "save_state": True}
        state["entry_armed"] = True
        state["armed_at_ms"] = now_ms
        state["blocked_reason"] = None
        state["portfolio_risk_freeze_active"] = False
        state["portfolio_risk_freeze_reason"] = None
        state["portfolio_risk_freeze_symbol"] = None
        state["portfolio_risk_freeze_buffer_pct"] = None
        state["portfolio_risk_restore_armed"] = False
        state["portfolio_risk_recovery_cycles"] = 0
        state["updated_at_ms"] = now_ms
        return {"recovered": True, "save_state": True}
