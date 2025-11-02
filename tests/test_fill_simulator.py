from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.fills import FillSimulator
from execution.market import DerivedMetrics
from execution.settings import ExecutionSettings
from execution.signals import SignalDecision
from execution.allocator import PreparedAllocation


class FillSimulatorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = ExecutionSettings()
        self.simulator = FillSimulator(self.settings)

    def test_simulate_mode_a(self) -> None:
        decision = SignalDecision(
            symbol="BTCUSDT",
            long_exchange="bybit",
            short_exchange="mexc",
            mode="mode_a",
            expected_edge=0.006,
            notional=500.0,
            time_to_funding=600.0,
        )
        allocation = PreparedAllocation(
            allocation_id="test",
            symbol="BTCUSDT",
            long_exchange="bybit",
            short_exchange="mexc",
            edge=0.006,
            notional=500.0,
            long_reservation_id="long",
            short_reservation_id="short",
            created_at=0.0,
        )
        metrics = DerivedMetrics(
            spread=0.1,
            mid_price=100.0,
            bid_liquidity_top3=5000.0,
            ask_liquidity_top3=5000.0,
            min_top_liquidity=5000.0,
            short_term_volatility=0.01,
        )
        quote = self.simulator.simulate(decision, allocation, metrics, metrics)
        self.assertGreater(quote.long_price, 100.0)
        self.assertLess(quote.short_price, 100.0)
        self.assertGreaterEqual(quote.slippage_bps, 0.0)


if __name__ == "__main__":
    unittest.main()
