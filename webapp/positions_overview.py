from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Mapping


def build_positions_overview(
    main_payload: Mapping[str, Any] | None,
    pump_payload: Mapping[str, Any] | None,
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Build one read-only view without merging unlike strategy position rows."""
    now_value = int(now_ms if now_ms is not None else time.time() * 1000)
    main = dict(main_payload or {})
    pump = dict(pump_payload or {})
    main_cards = [dict(item) for item in main.get("cards") or [] if isinstance(item, Mapping)]
    pump_config = dict(pump.get("config") or {})
    pump_active_policy = dict(pump.get("active_risk_policy") or pump_config)
    pump_margin_manager = dict(pump.get("margin_manager") or {})
    pump_positions = [
        _pump_position_card(
            item,
            pump_config,
            pump_active_policy,
            pump_margin_manager,
            now_value,
        )
        for item in pump.get("positions") or []
        if isinstance(item, Mapping) and str(item.get("status") or "") != "closed"
    ]
    pump_positions.sort(key=lambda item: (str(item.get("risk_level") or ""), str(item.get("symbol") or "")))

    main_pnl = sum(_number(item.get("net_pnl")) or 0.0 for item in main_cards)
    pump_pnl = sum(_number(item.get("unrealized_pnl_usd")) or 0.0 for item in pump_positions)
    liq_buffers = [
        value
        for value in (
            [_number(item.get("liq_distance_pct")) for item in main_cards]
            + [_number(item.get("liq_buffer_pct")) for item in pump_positions]
        )
        if value is not None
    ]
    protection_issues = _main_protection_issues(main_cards) + sum(
        1 for item in pump_positions if not item.get("stop_price") or not item.get("tp_price")
    )
    high_risk = sum(
        1
        for item in main_cards + pump_positions
        if str(item.get("risk_level") or "") == "high"
    )
    warning_risk = sum(
        1
        for item in main_cards + pump_positions
        if str(item.get("risk_level") or "") == "warn"
    )
    main_updated_ms = _iso_to_ms(main.get("account_last_updated") or main.get("last_updated"))
    pump_cycle_ms = _integer(pump.get("last_cycle_at_ms"))

    return {
        "schema": "positions_overview_v1",
        "generated_at_ms": now_value,
        "summary": {
            "main_positions": len(main_cards),
            "pump_positions": len(pump_positions),
            "pump_cap": _integer(pump_config.get("entry_cap")) or 0,
            "total_unrealized_pnl_usd": round(main_pnl + pump_pnl, 8),
            "main_unrealized_pnl_usd": round(main_pnl, 8),
            "pump_unrealized_pnl_usd": round(pump_pnl, 8),
            "min_liq_buffer_pct": min(liq_buffers) if liq_buffers else None,
            "protection_issues": protection_issues,
            "high_risk_positions": high_risk,
            "warning_risk_positions": warning_risk,
            "main_age_sec": _age_sec(now_value, main_updated_ms),
            "pump_age_sec": _age_sec(now_value, pump_cycle_ms),
        },
        "main": {
            "status": main.get("status"),
            "last_updated": main.get("last_updated"),
            "account_last_updated": main.get("account_last_updated"),
            "balances": list(main.get("balances") or []),
            "filters": dict(main.get("filters") or {}),
            "positions": main_cards,
        },
        "pump": {
            "status": pump.get("status"),
            "entry_armed": bool(pump.get("entry_armed")),
            "blocked_reason": pump.get("blocked_reason"),
            "last_error": pump.get("last_error"),
            "last_cycle_at_ms": pump_cycle_ms,
            "monitor_thread_alive": bool(pump.get("monitor_thread_alive")),
            "config": {
                key: pump_config.get(key)
                for key in (
                    "total_capital_usd",
                    "deployable_capital_usd",
                    "reserve_usd",
                    "entry_cap",
                    "max_active_positions",
                    "slot_margin_usd",
                    "warning_liq_buffer_pct",
                    "panic_liq_buffer_pct",
                    "emergency_liq_buffer_pct",
                    "exchange_stop_gap_from_liq_pct",
                    "max_position_topup_usd",
                    "max_total_topup_usd",
                    "margin_reduce_trigger_buffer_pct",
                    "margin_reduce_target_buffer_pct",
                )
            },
            "balance": _pump_balance(pump),
            "capital_regime": dict(pump.get("capital_regime") or {}),
            "margin_manager": pump_margin_manager,
            "shared_pool": dict(pump.get("shared_pool") or {}),
            "capital_rescue_shadow": dict(pump.get("capital_rescue_shadow") or {}),
            "auto_transfer": dict(
                (pump.get("transfers") or {}).get("auto_risk") or {}
            ),
            "notifications": dict(pump.get("notifications") or {}),
            "positions": pump_positions,
            "recent_events": list(pump.get("recent_events") or [])[-20:],
        },
    }


def _pump_position_card(
    row: Mapping[str, Any],
    config: Mapping[str, Any],
    active_policy: Mapping[str, Any],
    margin_manager: Mapping[str, Any],
    now_ms: int,
) -> dict[str, Any]:
    opened_at_ms = _integer(row.get("opened_at_ms"))
    max_hold_h = _number(row.get("max_hold_h"))
    age_h = ((now_ms - opened_at_ms) / 3_600_000.0) if opened_at_ms else None
    remaining_h = max(0.0, max_hold_h - age_h) if max_hold_h is not None and age_h is not None else None
    buffer_pct = _number(row.get("liq_buffer_pct"))
    if buffer_pct is None:
        risk_level = "unknown"
    elif buffer_pct <= (_number(config.get("emergency_liq_buffer_pct")) or 10.0):
        risk_level = "high"
    elif buffer_pct <= (_number(config.get("warning_liq_buffer_pct")) or 20.0):
        risk_level = "warn"
    else:
        risk_level = "ok"
    legs = [dict(item) for item in row.get("legs") or [] if isinstance(item, Mapping)]
    position_policy = dict(row.get("risk_policy") or config)
    margin_topup_cap = max(
        _number(position_policy.get("max_position_topup_usd")) or 0.0,
        _number(active_policy.get("max_position_topup_usd")) or 0.0,
        _number(margin_manager.get("shared_max_position_topup_usd")) or 0.0,
    )
    return {
        "module": "pump_live",
        "account_alias": row.get("account_alias") or "bybit_pump",
        "live_id": row.get("live_id"),
        "strategy_id": row.get("strategy_id"),
        "symbol": row.get("symbol"),
        "side": "short",
        "status": row.get("status"),
        "qty": _number(row.get("qty")),
        "avg_entry_price": _number(row.get("avg_entry_price")),
        "mark_price": _number(row.get("mark_price")),
        "unrealized_pnl_usd": _number(row.get("unrealized_pnl_usd")),
        "liq_price": _number(row.get("liq_price")),
        "liq_buffer_pct": buffer_pct,
        "risk_level": risk_level,
        "tp_price": _number(row.get("tp_price")),
        "stop_price": _number(row.get("stop_price")),
        "protection_updated_at_ms": _integer(row.get("protection_updated_at_ms")),
        "margin_topup_usd": _number(row.get("margin_topup_usd")) or 0.0,
        "margin_prefund_floor_usd": (
            _number(row.get("margin_prefund_floor_usd")) or 0.0
        ),
        "margin_prefund_status": row.get("margin_prefund_status"),
        "margin_prefund_target_stop_price": _number(
            row.get("margin_prefund_target_stop_price")
        ),
        "margin_prefund_next_ladder_price": _number(
            row.get("margin_prefund_next_ladder_price")
        ),
        "margin_topup_cap_usd": margin_topup_cap,
        "margin_continuation_policy_id": row.get("margin_continuation_policy_id"),
        "ladder_gate_status": row.get("ladder_gate_status"),
        "ladder_gate_step": _integer(row.get("ladder_gate_step")),
        "ladder_gate_error": row.get("ladder_gate_error"),
        "margin_reduce_confirm_count": _integer(row.get("margin_reduce_confirm_count")) or 0,
        "opened_at_ms": opened_at_ms,
        "age_h": age_h,
        "max_hold_h": max_hold_h,
        "remaining_hold_h": remaining_h,
        "close_reason": row.get("close_reason"),
        "last_error": row.get("last_error"),
        "tier": dict(row.get("tier") or {}),
        "legs": legs,
        "legs_filled": sum(1 for item in legs if str(item.get("status") or "") == "filled"),
        "legs_open": sum(1 for item in legs if str(item.get("status") or "") in {"open", "submitted"}),
    }


def _main_protection_issues(cards: list[dict[str, Any]]) -> int:
    issues = 0
    for card in cards:
        legs = [item for item in card.get("legs") or [] if isinstance(item, Mapping)]
        if any(not _number(item.get("stop_price")) for item in legs):
            issues += 1
    return issues


def _pump_balance(pump: Mapping[str, Any]) -> dict[str, Any]:
    balance = dict(pump.get("last_balance") or {})
    if not balance:
        balance = dict((pump.get("last_preflight") or {}).get("account") or {})
    capital = dict(pump.get("capital_manager") or {})
    return {
        "total_usd": _number(balance.get("total") or balance.get("total_usdt")),
        "available_usd": _number(balance.get("available") or balance.get("available_usdt")),
        "used_usd": _number(balance.get("used")),
        "temporary_occupied_usd": _number(
            capital.get("temporary_transfer_outstanding_usd")
        ) or 0.0,
    }


def _number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _integer(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _iso_to_ms(value: Any) -> int | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(parsed.timestamp() * 1000)


def _age_sec(now_ms: int, then_ms: int | None) -> float | None:
    if then_ms is None:
        return None
    return max(0.0, (now_ms - then_ms) / 1000.0)


__all__ = ["build_positions_overview"]
