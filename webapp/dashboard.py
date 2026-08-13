from __future__ import annotations

from typing import Any, Mapping

from .balance_views import with_pump_account_balances
from .positions_overview import build_positions_overview


def build_dashboard_payload(
    runtime_payload: Mapping[str, Any] | None,
    main_positions_payload: Mapping[str, Any] | None,
    pump_payload: Mapping[str, Any] | None,
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Build the compact read-only payload used by the main dashboard.

    The dashboard deliberately does not expose the retired Auto Exit,
    Auto Strategy, de-risk or legacy candidate-analysis payloads. All source
    objects are cached service snapshots; building this view performs no
    exchange I/O.
    """

    runtime = dict(runtime_payload or {})
    pump = dict(pump_payload or {})
    main = with_pump_account_balances(main_positions_payload or {}, pump)
    positions = build_positions_overview(main, pump, now_ms=now_ms)
    grid_source = dict(runtime.get("grid") or {})
    grid_rules = [
        dict(item)
        for item in grid_source.get("rules") or []
        if isinstance(item, Mapping)
    ]
    grid_rules.sort(
        key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""),
        reverse=True,
    )
    events = [
        dict(item)
        for item in runtime.get("events") or []
        if isinstance(item, Mapping)
    ][-20:]

    return {
        "schema": "dashboard_v2",
        "generated_at_ms": positions["generated_at_ms"],
        "service": {
            "status": runtime.get("status"),
            "last_error": runtime.get("last_error"),
            "last_updated": runtime.get("last_updated"),
            "refresh_in_progress": bool(runtime.get("refresh_in_progress")),
            "refresh_intervals": dict(runtime.get("refresh_intervals") or {}),
            "api_load": dict(runtime.get("api_load") or {}),
        },
        "runtime_modules": dict(runtime.get("runtime_modules") or {}),
        "settings": dict(runtime.get("settings") or {}),
        "accounts": {
            "last_updated": main.get("account_last_updated") or main.get("last_updated"),
            "balances": list(main.get("balances") or []),
            "balance_summary": dict(main.get("balance_summary") or {}),
            "exchange_status": [
                dict(item)
                for item in runtime.get("exchange_status") or []
                if isinstance(item, Mapping)
            ],
        },
        "positions": positions,
        "grid": {
            "mode": grid_source.get("mode") or "live",
            "generated_at": grid_source.get("generated_at"),
            "rules": grid_rules,
            "total_rules": len(grid_rules),
            "enabled_rules": sum(1 for item in grid_rules if bool(item.get("enabled"))),
            "active_rules": sum(
                1
                for item in grid_rules
                if str(item.get("status") or "").lower()
                not in {"", "disabled", "idle", "waiting_entry", "completed"}
            ),
        },
        "events": events,
    }


__all__ = ["build_dashboard_payload"]
