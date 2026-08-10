from __future__ import annotations

import json
import unittest
from importlib import import_module
from pathlib import Path
from unittest.mock import AsyncMock, patch

webapp_app = import_module("webapp.app")
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ManualExecApiTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_manual_web_exposes_and_submits_both_sizing_caps(self) -> None:
        template = (PROJECT_ROOT / "webapp" / "templates" / "manual.html").read_text(
            encoding="utf-8"
        )
        javascript = (PROJECT_ROOT / "webapp" / "static" / "manual.js").read_text(
            encoding="utf-8"
        )

        self.assertIn('id="manual-qty"', template)
        self.assertIn('id="manual-notional"', template)
        self.assertIn("If both caps are set, the smaller size is used.", template)
        self.assertIn("notional: parseOptionalNumber(getValue('notional'))", javascript)
        self.assertIn("'manual-notional': true", javascript)

        payload = webapp_app.ManualEnterPayload(
            symbol="BTCUSDT",
            qty=0.01,
            notional=500.0,
            mode="smart-enter",
            long_exchange="binance",
            short_exchange="bybit",
            dry_run=True,
        )
        data = webapp_app._manual_payload_dict(payload)

        self.assertEqual(data["qty"], 0.01)
        self.assertEqual(data["notional"], 500.0)

    async def test_explicit_mobile_runtime_is_not_raised_to_ten_minutes(self) -> None:
        payload = webapp_app.ManualEnterPayload(
            symbol="BTCUSDT",
            notional=100.0,
            mode="smart-enter",
            long_exchange="binance",
            short_exchange="bybit",
            max_runtime_sec=300,
            async_run=True,
            dry_run=False,
        )

        data = webapp_app._manual_payload_dict(payload)

        self.assertEqual(data["max_runtime_sec"], 300)

    async def test_missing_smart_notional_runtime_keeps_backend_safety_default(self) -> None:
        payload = webapp_app.ManualEnterPayload(
            symbol="BTCUSDT",
            notional=100.0,
            mode="smart-enter",
            long_exchange="binance",
            short_exchange="bybit",
            async_run=True,
            dry_run=False,
        )

        data = webapp_app._manual_payload_dict(payload)

        self.assertEqual(data["max_runtime_sec"], 600)

    async def test_failed_manual_execution_status_is_not_404(self) -> None:
        failed_status = {
            "execution_id": "exec-failed",
            "status": "failed",
            "error": "unsupported operand type(s) for +: 'NoneType' and 'float'",
            "logs": [],
            "result": {},
        }
        with patch.object(
            webapp_app.service,
            "manual_exec_status",
            new=AsyncMock(return_value=failed_status),
        ):
            response = await webapp_app.manual_exec_status("exec-failed")

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["error"], failed_status["error"])
