from __future__ import annotations

import unittest
from datetime import datetime, timezone

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


if __name__ == "__main__":
    unittest.main()
