from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
import tempfile

from execution.accounts import AccountMonitor


class AccountMonitorMarginUsedTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.monitor = AccountMonitor(refresh_interval=60, summary_interval=60)

    def test_margin_used_prefers_explicit_field(self) -> None:
        position = {"margin_used": 12.5}
        self.assertAlmostEqual(self.monitor._position_margin_used(position), 12.5)

    def test_margin_used_falls_back_to_initial_margin(self) -> None:
        position = {
            "margin_used": None,
            "initial_margin": 646.16934626,
            "leverage": None,
            "notional": 1938.508,
        }
        self.assertAlmostEqual(
            self.monitor._position_margin_used(position) or 0.0,
            646.16934626,
        )

    def test_margin_used_falls_back_to_raw_info_margin(self) -> None:
        position = {
            "raw": {
                "info": {
                    "positionInitialMargin": "123.45",
                }
            }
        }
        self.assertAlmostEqual(self.monitor._position_margin_used(position) or 0.0, 123.45)

    def test_margin_used_falls_back_to_notional_leverage(self) -> None:
        position = {"notional": -1000.0, "leverage": 5.0}
        self.assertAlmostEqual(self.monitor._position_margin_used(position) or 0.0, 200.0)


class AccountMonitorSummaryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.monitor = AccountMonitor(refresh_interval=60, summary_interval=60)

    def test_build_positions_summary_variant_three(self) -> None:
        positions = [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "symbol_normalized": "BTCUSDT",
                "side": "long",
                "coin_qty": 900,
                "notional": 12400,
                "entry_price": 100.0,
                "mark_price": 101.0,
                "funding_rate": -0.00006,
            },
            {
                "exchange": "okx",
                "symbol": "BTCUSDT",
                "symbol_normalized": "BTCUSDT",
                "side": "short",
                "coin_qty": 1000,
                "notional": 13800,
                "entry_price": 100.5,
                "mark_price": 100.7,
                "funding_rate": 0.00008,
            },
            {
                "exchange": "bybit",
                "symbol": "ETHUSDT",
                "symbol_normalized": "ETHUSDT",
                "side": "long",
                "coin_qty": 500,
                "notional": 8100,
                "entry_price": 200.0,
                "mark_price": 199.9,
                "funding_rate": -0.00004,
            },
            {
                "exchange": "gate",
                "symbol": "ETHUSDT",
                "symbol_normalized": "ETHUSDT",
                "side": "short",
                "coin_qty": 500,
                "notional": 8300,
                "entry_price": 200.2,
                "mark_price": 200.1,
                "funding_rate": 0.00003,
            },
        ]
        text = self.monitor._build_positions_summary(
            positions,
            "2026-02-16T11:40:00+00:00",
        )
        lines = text.splitlines()
        self.assertEqual(lines[0], "14:40 Positions")
        self.assertIn("BN,OK  BTCUSDT  $12.4k  +0.80  +0.0140%  qDelta -100", lines)
        eth_line = next((line for line in lines if "ETHUSDT" in line), "")
        self.assertIn("BY,GT  ETHUSDT  $8.1k", eth_line)
        self.assertNotIn("qDelta", eth_line)

    def test_summary_slot_window(self) -> None:
        slot = self.monitor._summary_slot_key(
            datetime(2026, 2, 16, 11, 40, tzinfo=timezone.utc)
        )
        self.assertEqual(slot, "2026-02-16 14")
        self.assertEqual(
            self.monitor._summary_slot_key(
                datetime(2026, 2, 16, 11, 59, tzinfo=timezone.utc)
            ),
            "2026-02-16 14",
        )
        self.assertIsNone(
            self.monitor._summary_slot_key(
                datetime(2026, 2, 16, 11, 39, tzinfo=timezone.utc)
            )
        )
        self.assertIsNone(
            self.monitor._summary_slot_key(
                datetime(2026, 2, 16, 12, 0, tzinfo=timezone.utc)
            )
        )

    def test_build_positions_summary_dedupes_duplicated_settle_suffix(self) -> None:
        text = self.monitor._build_positions_summary(
            [
                {
                    "exchange": "binance",
                    "symbol": "RIVERUSDTUSDT",
                    "symbol_normalized": "RIVERUSDTUSDT",
                    "side": "long",
                    "coin_qty": 10,
                    "notional": 100,
                    "entry_price": 10.0,
                    "mark_price": 10.1,
                    "funding_rate": 0.0001,
                },
                {
                    "exchange": "okx",
                    "symbol": "RIVERUSDT",
                    "symbol_normalized": "RIVERUSDT",
                    "side": "short",
                    "coin_qty": 10,
                    "notional": 100,
                    "entry_price": 10.0,
                    "mark_price": 10.2,
                    "funding_rate": 0.0002,
                },
            ],
            "2026-02-16T11:40:00+00:00",
        )
        self.assertIn("RIVERUSDT", text)
        self.assertNotIn("RIVERUSDTUSDT", text)

    def test_summary_slot_claim_dedupes_with_sent_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self.monitor._summary_slot_marker_dir = Path(tmpdir)
            self.monitor._summary_slot_marker_dir.mkdir(parents=True, exist_ok=True)
            slot_key = "2026-02-16 14"
            acquired, claim_path, _reason = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired)
            self.assertIsNotNone(claim_path)
            self.monitor._finalize_summary_slot_claim(slot_key, claim_path, "ok")
            acquired_again, _claim_again, reason_again = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertFalse(acquired_again)
            self.assertEqual(reason_again, "sent")

    def test_summary_slot_claim_releases_on_http_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self.monitor._summary_slot_marker_dir = Path(tmpdir)
            self.monitor._summary_slot_marker_dir.mkdir(parents=True, exist_ok=True)
            slot_key = "2026-02-16 14"
            acquired, claim_path, _reason = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired)
            self.assertIsNotNone(claim_path)
            self.monitor._finalize_summary_slot_claim(slot_key, claim_path, "http_error")
            acquired_again, _claim_again, reason_again = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired_again)
            self.assertNotEqual(reason_again, "sent")

    def test_summary_slot_claim_blocks_after_ambiguous_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self.monitor._summary_slot_marker_dir = Path(tmpdir)
            self.monitor._summary_slot_marker_dir.mkdir(parents=True, exist_ok=True)
            slot_key = "2026-02-16 14"
            acquired, claim_path, _reason = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired)
            self.assertIsNotNone(claim_path)
            self.monitor._finalize_summary_slot_claim(slot_key, claim_path, "error")
            acquired_again, _claim_again, reason_again = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertFalse(acquired_again)
            self.assertEqual(reason_again, "claimed")


class _FakeGateClient:
    def __init__(self, *, fail_add: bool = False) -> None:
        self.fail_add = fail_add
        self.markets = {
            "ORCA/USDT:USDT": {
                "id": "ORCA_USDT",
                "symbol": "ORCA/USDT:USDT",
                "settle": "usdt",
                "settleId": "USDT",
                "swap": True,
            }
        }
        self.currencies = {
            "USDT": {"precision": 8},
            "usdt": {"precision": 8},
        }
        self.add_margin_calls: list[tuple[str, float, dict]] = []
        self.reduce_margin_calls: list[tuple[str, float, dict]] = []
        self.dual_margin_calls: list[dict] = []

    async def load_markets(self) -> dict:
        return self.markets

    def market(self, symbol: str) -> dict:
        return self.markets[symbol]

    def number_to_string(self, value: float) -> str:
        return f"{float(value):.8f}".rstrip("0").rstrip(".")

    async def add_margin(self, symbol: str, amount: float, params: dict) -> dict:
        self.add_margin_calls.append((symbol, amount, dict(params)))
        if self.fail_add:
            raise RuntimeError('gate {"label":"INVALID_PROTOCOL","message":"invalid argument: #3"}')
        return {"status": "ok", "method": "add_margin"}

    async def reduce_margin(self, symbol: str, amount: float, params: dict) -> dict:
        self.reduce_margin_calls.append((symbol, amount, dict(params)))
        return {"status": "ok", "method": "reduce_margin"}

    async def privateFuturesPostSettleDualCompPositionsContractMargin(self, request: dict) -> dict:
        self.dual_margin_calls.append(dict(request))
        return {"status": "ok", "method": "dual_comp_margin"}


class _FakeGateway:
    def __init__(self, client: _FakeGateClient) -> None:
        self.client = client

    async def refresh_credentials_async(self, force_env: bool = True) -> None:
        _ = force_env

    async def ensure_client(self) -> None:
        return None

    def map_symbol(self, symbol: str) -> str:
        return symbol

    async def close(self) -> None:
        return None


class AccountMonitorGateMarginTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_gate_dual_mode_add_uses_dual_endpoint(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _FakeGateClient()
        monitor._gateways = {"gate": _FakeGateway(client)}
        position = {
            "symbol": "ORCA/USDT:USDT",
            "exchange_symbol": "ORCA_USDT",
            "side": "long",
            "raw": {"info": {"mode": "dual_long"}},
        }
        result = await monitor._modify_margin(
            exchange="gate",
            position=position,
            amount=114.06611570247928,
            action="add",
        )
        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(len(client.add_margin_calls), 0)
        self.assertEqual(len(client.dual_margin_calls), 1)
        request = client.dual_margin_calls[0]
        self.assertEqual(request.get("contract"), "ORCA_USDT")
        self.assertEqual(request.get("dual_side"), "dual_long")
        self.assertEqual(request.get("change"), "114.0661157")

    async def test_gate_add_falls_back_to_dual_endpoint_on_invalid_protocol(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _FakeGateClient(fail_add=True)
        monitor._gateways = {"gate": _FakeGateway(client)}
        position = {
            "symbol": "ORCA/USDT:USDT",
            "exchange_symbol": "ORCA_USDT",
            "side": "short",
            "raw": {"info": {"mode": "single"}},
        }
        result = await monitor._modify_margin(
            exchange="gate",
            position=position,
            amount=114.06611570247928,
            action="add",
        )
        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(len(client.add_margin_calls), 1)
        self.assertAlmostEqual(client.add_margin_calls[0][1], 114.0661157)
        self.assertEqual(len(client.dual_margin_calls), 1)
        request = client.dual_margin_calls[0]
        self.assertEqual(request.get("dual_side"), "dual_short")
        self.assertEqual(request.get("change"), "114.0661157")


if __name__ == "__main__":
    unittest.main()
