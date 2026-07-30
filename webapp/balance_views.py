from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _timestamp_from_ms(value: Any) -> str | None:
    ts_ms = _safe_float(value)
    if ts_ms is None or ts_ms <= 0:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def _normalise_main_rows(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalised: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        if str(row.get("account_type") or "").lower() == "pump":
            continue
        if str(row.get("account_alias") or "").lower() == "bybit_pump":
            continue
        row.setdefault("account_alias", "main")
        row.setdefault("account_label", "Main account")
        row.setdefault("account_type", "main")
        normalised.append(row)
    return normalised


def _pump_balance_row(pump_status: Mapping[str, Any]) -> dict[str, Any]:
    balance = pump_status.get("last_balance")
    balance = balance if isinstance(balance, Mapping) else {}
    total = _safe_float(balance.get("wallet"))
    if total is None:
        total = _safe_float(balance.get("total"))
    available = _safe_float(balance.get("available"))
    used = _safe_float(balance.get("used"))
    if used is None and total is not None and available is not None:
        used = max(0.0, total - available)
    margin_ratio = (
        max(0.0, used) / total
        if total is not None and total > 0 and used is not None
        else None
    )
    buffer_pct = (
        max(0.0, available) / total * 100.0
        if total is not None and total > 0 and available is not None
        else None
    )
    error = pump_status.get("last_error") or pump_status.get("blocked_reason")
    if total is None:
        status = "unavailable"
    elif error:
        status = "partial"
    else:
        status = "ok"
    module_status = str(pump_status.get("status") or "unknown")
    timestamp = _timestamp_from_ms(
        pump_status.get("last_cycle_at_ms") or pump_status.get("updated_at_ms")
    )
    return {
        "exchange": "bybit",
        "account_alias": "bybit_pump",
        "account_label": "Pump subaccount",
        "account_type": "pump",
        "asset": "USDT",
        "total": total,
        "available": available,
        "used": used,
        "margin_ratio": margin_ratio,
        "equity": _safe_float(balance.get("total")) or total,
        "buffer_pct": buffer_pct,
        "status": status,
        "module_status": module_status,
        "message": f"Pump Live: {module_status}",
        "error": str(error) if error else None,
        "timestamp": timestamp,
        "updated_at": timestamp,
    }


def _aggregate(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    compatible = [
        row
        for row in rows
        if str(row.get("asset") or "USDT").upper() == "USDT"
        and _safe_float(row.get("total")) is not None
    ]

    def summed(key: str) -> float | None:
        values = [_safe_float(row.get(key)) for row in compatible]
        present = [value for value in values if value is not None]
        return sum(present) if present else None

    return {
        "asset": "USDT",
        "total": summed("total"),
        "available": summed("available"),
        "used": summed("used"),
        "reporting_accounts": len(compatible),
        "healthy_accounts": sum(
            1 for row in compatible if str(row.get("status") or "ok").lower() == "ok"
        ),
    }


def _balance_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    bybit_main_rows = [
        row
        for row in rows
        if str(row.get("exchange") or "").lower() == "bybit"
        and str(row.get("account_type") or "main").lower() == "main"
    ]
    bybit_pump_rows = [
        row
        for row in rows
        if str(row.get("exchange") or "").lower() == "bybit"
        and str(row.get("account_type") or "").lower() == "pump"
    ]
    return {
        "asset": "USDT",
        "overall": _aggregate(rows),
        "bybit_main": _aggregate(bybit_main_rows),
        "bybit_pump": _aggregate(bybit_pump_rows),
        "bybit_combined": _aggregate(bybit_main_rows + bybit_pump_rows),
    }


def with_pump_account_balances(
    payload: Mapping[str, Any],
    pump_status: Mapping[str, Any],
    *,
    accounts_key: str | None = None,
) -> dict[str, Any]:
    """Return a copy with main/Pump account labels, Pump balance and totals.

    ``accounts_key`` is used for the web state payload where balances live under
    ``accounts``. Mobile payloads keep balances at the top level.
    """

    result = dict(payload)
    if accounts_key:
        account_source = payload.get(accounts_key)
        accounts = dict(account_source) if isinstance(account_source, Mapping) else {}
        raw_rows = accounts.get("balances") or []
    else:
        accounts = result
        raw_rows = payload.get("balances") or []

    rows = _normalise_main_rows(
        [row for row in raw_rows if isinstance(row, Mapping)]
    )
    rows.append(_pump_balance_row(pump_status))
    rows.sort(
        key=lambda row: (
            str(row.get("exchange") or ""),
            0 if str(row.get("account_type") or "main") == "main" else 1,
            str(row.get("asset") or ""),
        )
    )
    accounts["balances"] = rows
    accounts["balance_summary"] = _balance_summary(rows)

    if accounts_key:
        result[accounts_key] = accounts
    else:
        result = accounts
    return result
