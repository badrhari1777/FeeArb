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
