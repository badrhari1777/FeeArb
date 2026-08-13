from __future__ import annotations

import json
import unittest
from importlib import import_module
from unittest.mock import patch

from webapp.dashboard import build_dashboard_payload

from tests.test_positions_overview import main_payload, pump_payload


webapp_app = import_module("webapp.app")


def runtime_payload() -> dict:
    return {
        "status": "ready",
        "last_error": None,
        "last_updated": "2026-08-13T08:00:00+00:00",
        "refresh_in_progress": False,
        "refresh_intervals": {
            "dashboard_sec": 30,
            "accounts_sec": 60,
            "positions_market_sec": 60,
            "summary_sec": 1800,
        },
        "settings": {
            "sources": {"coinglass": False},
            "exchanges": {"binance": True},
            "analysis_exchanges": {"binance": True, "bybit": True},
            "parser_refresh_seconds": 1200,
            "exchange_refresh_seconds": 300,
            "table_refresh_seconds": 30,
            "account_refresh_seconds": 60,
            "positions_market_refresh_seconds": 60,
            "summary_refresh_seconds": 1800,
            "protective": {"auto_protect_enabled": True},
            "manual": {},
        },
        "runtime_modules": {
            "account_monitor": True,
            "auto_arb_grid": True,
            "pump_live": True,
            "auto_exit": False,
            "auto_strategies": False,
            "position_reduction": False,
        },
        "exchange_status": [{"exchange": "binance", "status": "ok"}],
        "events": [{"event": "account_refreshed"}],
        "grid": {
            "mode": "live",
            "rules": [
                {"rule_id": "idle", "enabled": True, "status": "waiting_entry"},
                {"rule_id": "live", "enabled": True, "status": "entering"},
                {"rule_id": "off", "enabled": False, "status": "disabled"},
            ],
        },
        "auto_exit": {"secret": "must-not-leak"},
        "auto_strategies": {"secret": "must-not-leak"},
        "emergency_derisk": {"secret": "must-not-leak"},
        "coin_analysis": {"secret": "must-not-leak"},
    }


def test_dashboard_is_compact_and_keeps_main_and_pump_balances() -> None:
    payload = build_dashboard_payload(
        runtime_payload(), main_payload(), pump_payload(), now_ms=1_100_000
    )

    assert payload["schema"] == "dashboard_v2"
    assert payload["positions"]["summary"]["main_positions"] == 1
    assert payload["positions"]["summary"]["pump_positions"] == 1
    assert payload["accounts"]["balance_summary"]["overall"]["total"] == 6000.0
    assert [row["account_alias"] for row in payload["accounts"]["balances"]] == [
        "main",
        "bybit_pump",
    ]
    assert payload["grid"]["total_rules"] == 3
    assert payload["grid"]["enabled_rules"] == 2
    assert payload["grid"]["active_rules"] == 1
    assert payload["runtime_modules"]["auto_exit"] is False
    assert "auto_exit" not in payload
    assert "auto_strategies" not in payload
    assert "emergency_derisk" not in payload
    assert "coin_analysis" not in payload
    assert "credentials" not in payload["positions"]["pump"]


class DashboardApiTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_dashboard_api_uses_one_pump_snapshot_and_compact_sources(self) -> None:
        with (
            patch.object(
                webapp_app.service,
                "dashboard_runtime_payload",
                return_value=runtime_payload(),
            ) as runtime_mock,
            patch.object(
                webapp_app.service,
                "mobile_positions_payload",
                return_value=main_payload(),
            ) as positions_mock,
            patch.object(
                webapp_app.bybit_pump_short_lab,
                "pump_live_status",
                return_value=pump_payload(),
            ) as pump_mock,
        ):
            response = await webapp_app.dashboard_api()

        assert response.status_code == 200
        payload = json.loads(response.body)
        assert payload["schema"] == "dashboard_v2"
        assert payload["positions"]["summary"]["main_positions"] == 1
        runtime_mock.assert_called_once_with()
        positions_mock.assert_called_once_with(include_auto_exit=False)
        pump_mock.assert_called_once_with()
