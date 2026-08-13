from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from webapp.balance_views import with_pump_account_balances


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _pump_status(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": "armed",
        "last_cycle_at_ms": 1_785_425_744_634,
        "last_balance": {
            "total": 1043.94,
            "wallet": 1043.94,
            "available": 1000.0,
            "used": 43.94,
        },
        "last_error": None,
        "capital_manager": {"temporary_transfer_outstanding_usd": 50.0},
    }
    payload.update(overrides)
    return payload


def test_mobile_balances_separate_main_and_pump_and_sum_once() -> None:
    source = {
        "balances": [
            {
                "exchange": "bybit",
                "asset": "USDT",
                "total": 7594.22,
                "available": 2211.38,
                "used": 5382.84,
                "status": "watch",
            },
            {
                "exchange": "binance",
                "asset": "USDT",
                "total": 5205.18,
                "available": 1443.88,
                "used": 3761.30,
                "status": "watch",
            },
        ]
    }
    original = deepcopy(source)

    payload = with_pump_account_balances(source, _pump_status())

    assert source == original
    assert [
        (row["exchange"], row["account_alias"])
        for row in payload["balances"]
    ] == [
        ("binance", "main"),
        ("bybit", "main"),
        ("bybit", "bybit_pump"),
    ]
    summary = payload["balance_summary"]
    assert summary["bybit_main"]["total"] == pytest.approx(7594.22)
    assert summary["bybit_pump"]["total"] == pytest.approx(1043.94)
    assert summary["bybit_combined"]["total"] == pytest.approx(8638.16)
    assert summary["bybit_pump"]["temporary_occupied_usd"] == pytest.approx(50.0)
    assert summary["bybit_combined"]["temporary_occupied_usd"] == pytest.approx(50.0)
    pump_row = next(row for row in payload["balances"] if row["account_type"] == "pump")
    assert pump_row["temporary_occupied_usd"] == pytest.approx(50.0)
    assert summary["overall"]["total"] == pytest.approx(13843.34)


def test_augmentation_is_idempotent_and_does_not_duplicate_pump_row() -> None:
    first = with_pump_account_balances(
        {"balances": [{"exchange": "bybit", "asset": "USDT", "total": 100.0}]},
        _pump_status(),
    )

    second = with_pump_account_balances(first, _pump_status())

    pump_rows = [
        row for row in second["balances"] if row.get("account_alias") == "bybit_pump"
    ]
    assert len(pump_rows) == 1
    assert second["balance_summary"]["bybit_combined"]["total"] == pytest.approx(1143.94)


def test_missing_pump_balance_is_visible_but_not_invented_as_zero() -> None:
    payload = with_pump_account_balances(
        {"balances": [{"exchange": "bybit", "asset": "USDT", "total": 100.0}]},
        _pump_status(last_balance={}, last_error="temporary exchange error"),
    )

    pump_row = next(
        row for row in payload["balances"] if row["account_alias"] == "bybit_pump"
    )
    assert pump_row["total"] is None
    assert pump_row["status"] == "unavailable"
    assert pump_row["error"] == "temporary exchange error"
    assert payload["balance_summary"]["bybit_combined"]["total"] == pytest.approx(100.0)


def test_web_accounts_payload_keeps_other_account_state() -> None:
    payload = with_pump_account_balances(
        {
            "status": "ready",
            "accounts": {
                "positions": [{"symbol": "DEXEUSDT"}],
                "balances": [{"exchange": "bybit", "asset": "USDT", "total": 50.0}],
            },
        },
        _pump_status(),
        accounts_key="accounts",
    )

    assert payload["status"] == "ready"
    assert payload["accounts"]["positions"] == [{"symbol": "DEXEUSDT"}]
    assert payload["accounts"]["balance_summary"]["bybit_combined"]["total"] == pytest.approx(
        1093.94
    )


def test_compact_dashboard_renders_balance_summary_without_recomputing_it() -> None:
    dashboard_js = (PROJECT_ROOT / "webapp" / "static" / "dashboard.js").read_text(
        encoding="utf-8"
    )

    assert "accounts.balance_summary || {}" in dashboard_js
    assert "summaries.bybit_main || {}" in dashboard_js
    assert "summaries.bybit_pump || {}" in dashboard_js
    assert "pumpBalance.available" in dashboard_js
