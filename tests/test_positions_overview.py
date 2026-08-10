from __future__ import annotations

import json
import unittest
from importlib import import_module
from unittest.mock import patch

from starlette.requests import Request

from webapp.positions_overview import build_positions_overview

webapp_app = import_module("webapp.app")


def main_payload() -> dict:
    return {
        "status": "ready",
        "last_updated": "2026-07-29T20:00:00+00:00",
        "account_last_updated": "2026-07-29T20:00:05+00:00",
        "balances": [{"exchange": "binance", "total": 5000.0}],
        "filters": {"all": 1, "risk": 0},
        "cards": [
            {
                "symbol": "DEXEUSDT",
                "net_pnl": 12.5,
                "liq_distance_pct": 24.0,
                "risk_level": "ok",
                "legs": [
                    {"exchange": "bybit", "side": "long", "stop_price": 3.5},
                    {"exchange": "binance", "side": "short", "stop_price": 6.5},
                ],
            }
        ],
    }


def pump_payload() -> dict:
    return {
        "status": "armed",
        "entry_armed": True,
        "last_cycle_at_ms": 1_000_000,
        "monitor_thread_alive": True,
        "config": {
            "entry_cap": 4,
            "max_active_positions": 4,
            "slot_margin_usd": 175.0,
            "warning_liq_buffer_pct": 20.0,
            "emergency_liq_buffer_pct": 10.0,
            "exchange_stop_gap_from_liq_pct": 2.5,
            "max_position_topup_usd": 175.0,
        },
        "active_risk_policy": {
            "policy_id": "v2_3000",
            "max_position_topup_usd": 525.0,
        },
        "credentials": {"api_secret_present": True},
        "last_balance": {"total": 1000.0, "available": 825.0, "used": 175.0},
        "notifications": {"configured": True, "last_status": "ok"},
        "capital_manager": {"temporary_transfer_outstanding_usd": 50.0},
        "capital_regime": {
            "mode": "stress",
            "prefund_floor_usd": 25.0,
            "temporary_occupied_usd": 50.0,
        },
        "transfers": {
            "auto_risk": {
                "enabled": True,
                "main_wallet_floor_usd": 2000.0,
                "daily_remaining_usd": 200.0,
            }
        },
        "positions": [
            {
                "live_id": "live-1",
                "account_alias": "bybit_pump",
                "strategy_id": "main_pullback_tier",
                "symbol": "TESTUSDT",
                "status": "open",
                "qty": 50.0,
                "avg_entry_price": 10.0,
                "mark_price": 12.0,
                "unrealized_pnl_usd": -20.0,
                "liq_price": 13.8,
                "liq_buffer_pct": 15.0,
                "tp_price": 7.5,
                "stop_price": 13.455,
                "margin_topup_usd": 25.0,
                "margin_prefund_floor_usd": 25.0,
                "margin_prefund_status": "confirmed",
                "margin_prefund_target_stop_price": 15.375,
                "margin_prefund_next_ladder_price": 15.0,
                "risk_policy": {"max_position_topup_usd": 175.0},
                "margin_continuation_policy_id": "v2_3000",
                "ladder_gate_status": "ready",
                "ladder_gate_step": 2,
                "opened_at_ms": 500_000,
                "max_hold_h": 336,
                "legs": [
                    {"step": 1, "status": "filled"},
                    {"step": 2, "status": "open"},
                ],
            }
        ],
        "recent_events": [{"event": "margin_added", "ts_ms": 900_000}],
    }


def test_overview_keeps_main_and_pump_groups_separate() -> None:
    payload = build_positions_overview(main_payload(), pump_payload(), now_ms=1_100_000)

    assert payload["summary"]["main_positions"] == 1
    assert payload["summary"]["pump_positions"] == 1
    assert payload["summary"]["pump_cap"] == 4
    assert payload["summary"]["total_unrealized_pnl_usd"] == -7.5
    assert payload["summary"]["min_liq_buffer_pct"] == 15.0
    assert payload["summary"]["protection_issues"] == 0
    assert payload["main"]["positions"][0]["symbol"] == "DEXEUSDT"
    assert payload["pump"]["positions"][0]["symbol"] == "TESTUSDT"
    assert payload["pump"]["positions"][0]["risk_level"] == "warn"
    assert payload["pump"]["positions"][0]["legs_filled"] == 1
    assert payload["pump"]["positions"][0]["margin_prefund_floor_usd"] == 25.0
    assert payload["pump"]["positions"][0]["margin_prefund_status"] == "confirmed"
    assert payload["pump"]["positions"][0]["margin_topup_cap_usd"] == 525.0
    assert payload["pump"]["positions"][0]["ladder_gate_status"] == "ready"
    assert payload["pump"]["positions"][0]["ladder_gate_step"] == 2
    assert payload["pump"]["positions"][0]["remaining_hold_h"] > 335.0
    assert payload["pump"]["balance"]["temporary_occupied_usd"] == 50.0
    assert payload["pump"]["capital_regime"]["mode"] == "stress"
    assert payload["pump"]["auto_transfer"]["enabled"] is True
    assert "credentials" not in payload["pump"]


def test_overview_reports_missing_exchange_protection() -> None:
    main = main_payload()
    main["cards"][0]["legs"][0]["stop_price"] = None
    pump = pump_payload()
    pump["positions"][0]["stop_price"] = None

    payload = build_positions_overview(main, pump, now_ms=1_100_000)

    assert payload["summary"]["protection_issues"] == 2


class PositionsOverviewApiTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_api_uses_whitelisted_combined_payload(self) -> None:
        with (
            patch.object(webapp_app.service, "mobile_positions_payload", return_value=main_payload()),
            patch.object(webapp_app.bybit_pump_short_lab, "pump_live_status", return_value=pump_payload()),
        ):
            response = await webapp_app.positions_overview_api()

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertEqual(payload["schema"], "positions_overview_v1")
        self.assertEqual(payload["summary"]["main_positions"], 1)
        self.assertEqual(payload["summary"]["pump_positions"], 1)
        self.assertNotIn("credentials", payload["pump"])

    async def test_detailed_positions_page_is_read_only_and_links_module_controls(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/positions",
                "root_path": "",
                "scheme": "http",
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 1),
                "server": ("127.0.0.1", 8000),
            }
        )

        response = await webapp_app.positions_page(request)
        body = response.body.decode("utf-8")

        self.assertIn("Position Control Center", body)
        self.assertIn("/static/positions.js", body)
        self.assertIn("/pump-short-strategies", body)
        self.assertNotIn("Emergency close all", body)

    async def test_main_page_exposes_all_main_and_pump_position_tabs(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "root_path": "",
                "scheme": "http",
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 1),
                "server": ("127.0.0.1", 8000),
            }
        )
        with patch.object(webapp_app.service, "state_payload", return_value={"status": "ready"}):
            response = await webapp_app.index(request)
        body = response.body.decode("utf-8")

        self.assertIn('data-positions-tab="all"', body)
        self.assertIn('data-positions-tab="main"', body)
        self.assertIn('data-positions-tab="pump"', body)
        self.assertIn('href="/positions"', body)
        self.assertIn("Position now", body)
        self.assertIn("Next funding", body)
