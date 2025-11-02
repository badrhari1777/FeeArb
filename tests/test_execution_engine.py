from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from execution.allocator import Allocator
from execution.adapters import SimulatedTradingClient
from execution.engine import ExecutionEngine
from execution.market import DerivedMetrics, MarketGateway
from execution.positions import PositionManager
from execution.settings import ExecutionSettings
from execution.signals import SignalDecision
from execution.risk import RiskGuard
from execution.wallet import WalletService
from execution.telemetry import TelemetryClient
from execution.fills import FillSimulator
from orchestrator.models import FundingOpportunity


def _services(tmp_dir: Path):
    wallet = WalletService({"bybit": 5000.0, "mexc": 4000.0}, state_path=tmp_dir / "wallet.json")
    positions = PositionManager(wallet, state_path=tmp_dir / "positions.json")
    settings = ExecutionSettings()
    allocator = Allocator(wallet, positions, settings, state_path=tmp_dir / "allocator.json")
    market = MarketGateway()
    return wallet, settings, allocator, positions, market


def _decision(edge: float, seconds_to_funding: int) -> SignalDecision:
    now = datetime.now(timezone.utc)
    next_funding = now + timedelta(seconds=seconds_to_funding)
    opportunity = FundingOpportunity(
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
    return SignalDecision(
        symbol="BTCUSDT",
        long_exchange="bybit",
        short_exchange="mexc",
        mode="mode_a",
        expected_edge=edge,
        notional=600.0,
        time_to_funding=float(seconds_to_funding),
        opportunity=opportunity,
    )


class ExecutionEngineTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_execute_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            wallet, settings, allocator, positions, market = _services(tmp_path)
            risk_guard = RiskGuard(settings, allocator, wallet)
            settings.telemetry.structured_log_path = tmp_path / "telemetry.jsonl"
            from execution.telemetry import TelemetryClient

            telemetry = TelemetryClient(settings)
            simulator = FillSimulator(settings)
            engine = ExecutionEngine(
                settings,
                allocator,
                positions,
                market,
                risk_guard=risk_guard,
                telemetry=telemetry,
                simulator=simulator,
                trading_clients={
                    "bybit": SimulatedTradingClient("bybit", fill_simulator=simulator, telemetry=telemetry),
                    "mexc": SimulatedTradingClient("mexc", fill_simulator=simulator, telemetry=telemetry),
                },
            )

            metrics = DerivedMetrics(
                spread=0.1,
                mid_price=100.05,
                bid_liquidity_top3=5000.0,
                ask_liquidity_top3=5000.0,
                min_top_liquidity=5000.0,
                short_term_volatility=0.0005,
            )
            market._latest_metrics[("bybit", "BTCUSDT")] = metrics
            market._latest_metrics[("mexc", "BTCUSDT")] = metrics

            decision = _decision(0.006, 600)
            result = await engine.execute_decision(decision)
            self.assertIsNotNone(result.position)
            self.assertIsNone(result.error)
            if result.position:
                self.assertEqual(result.position.status, "observing")


if __name__ == "__main__":
    unittest.main()
