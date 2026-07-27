from __future__ import annotations

import json
import unittest
from importlib import import_module
from unittest.mock import AsyncMock, patch

webapp_app = import_module("webapp.app")


class ManualExecApiTestCase(unittest.IsolatedAsyncioTestCase):
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
