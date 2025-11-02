from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from execution.allocator import Allocator
from execution.market import DerivedMetrics, MarketGateway
from execution.positions import PositionManager
from execution.settings import ExecutionSettings
from execution.signals import SignalEngine
from execution.wallet import WalletService
from orchestrator.models import FundingOpportunity


def _setup(tmp_dir: Path):
    wallet = WalletService(
        {"bybit": 5000.0, "mexc": 4000.0},
        state_path=tmp_dir / "wallet.json",
    )
    positions = PositionManager(
        wallet,
        state_path=tmp_dir / "positions.json",
    )
    settings = ExecutionSettings()
    allocator = Allocator(
        wallet,
        positions,
        settings,
        state_path=tmp_dir / "allocator.json",
    )
    gateway = MarketGateway()
    return settings, allocator, gateway


def _make_opportunity(edge: float, *, seconds_to_funding: int) -> FundingOpportunity:
    now = datetime.now(timezone.utc)
    next_funding = now + timedelta(seconds=seconds_to_funding)
    return FundingOpportunity(
        symbol="BTCUSDT",
        long_exchange="bybit",
        short_exchange="mexc",
        long_rate=0.0001,
        short_rate=0.0008,
        spread=0.0007,
        long_mark=100.0,
        short_mark=100.1,
        long_next_funding=next_funding,
        short_next_funding=next_funding,
        price_diff=0.1,
        price_diff_pct=0.001,
        effective_spread=edge,
        participants=4,
        long_ask=100.0,
        long_liquidity=10.0,
        long_liquidity_usd=1000.0,
        short_bid=100.1,
        short_liquidity=10.0,
        short_liquidity_usd=1001.0,
        long_funding_interval_hours=8.0,
        short_funding_interval_hours=8.0,
    )


class SignalEngineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir_ctx = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self.tmp_dir_ctx.name)
        self.settings, self.allocator, self.gateway = _setup(self.tmp_dir)

    def tearDown(self) -> None:
        self.tmp_dir_ctx.cleanup()

    def _set_metrics(self, volatility: float = 0.01, liquidity: float = 2000.0) -> None:
        metrics = DerivedMetrics(
            spread=0.1,
            mid_price=100.05,
            bid_liquidity_top3=liquidity,
            ask_liquidity_top3=liquidity,
            min_top_liquidity=liquidity,
            short_term_volatility=volatility,
        )
        self.gateway._latest_metrics[("bybit", "BTCUSDT")] = metrics
        self.gateway._latest_metrics[("mexc", "BTCUSDT")] = metrics

    def test_generate_decision(self) -> None:
        self._set_metrics()
        engine = SignalEngine(self.settings, self.allocator, self.gateway)
        opportunity = _make_opportunity(0.006, seconds_to_funding=600)
        decisions = engine.generate([opportunity])
        self.assertEqual(len(decisions), 1)
        decision = decisions[0]
        self.assertEqual(decision.mode, "mode_c")
        self.assertGreater(decision.notional, 0)

    def test_skip_on_low_liquidity(self) -> None:
        self._set_metrics(liquidity=10.0)
        engine = SignalEngine(self.settings, self.allocator, self.gateway)
        opportunity = _make_opportunity(0.006, seconds_to_funding=600)
        decisions = engine.generate([opportunity])
        self.assertEqual(decisions, [])


if __name__ == "__main__":
    unittest.main()
