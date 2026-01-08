from __future__ import annotations

import asyncio
import time
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from execution.allocator import Allocator
from execution.adapters import SimulatedTradingClient
from execution.engine import ExecutionEngine
from execution.fills import FillSimulator
from execution.lifecycle import LifecycleController
from execution.market import DerivedMetrics, MarketGateway
from execution.orchestrator import ExecutionOrchestrator
from execution.positions import PositionManager
from execution.risk import RiskGuard
from execution.settings import ExecutionSettings
from execution.signals import SignalEngine
from execution.telemetry import TelemetryClient
from execution.wallet import WalletService
from orchestrator.models import FundingOpportunity
from pipeline import DataSnapshot


class EndToEndTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_full_emulation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            settings = ExecutionSettings()
            settings.telemetry.structured_log_path = tmp_path / "telemetry.jsonl"
            wallet = WalletService({"bybit": 5000.0, "mexc": 4000.0}, state_path=tmp_path / "wallet.json")
            positions = PositionManager(wallet, state_path=tmp_path / "positions.json")
            allocator = Allocator(wallet, positions, settings, state_path=tmp_path / "allocator.json")
            telemetry = TelemetryClient(settings)
            simulator = FillSimulator(settings)
            risk_guard = RiskGuard(settings, allocator, wallet)
            lifecycle = LifecycleController(settings, positions, allocator)
            market = MarketGateway()

            metrics = DerivedMetrics(
                spread=0.1,
                mid_price=100.0,
                bid_liquidity_top3=8000.0,
                ask_liquidity_top3=8000.0,
                min_top_liquidity=8000.0,
                short_term_volatility=0.0005,
            )
            market._latest_metrics[("bybit", "BTCUSDT")] = metrics
            market._latest_metrics[("mexc", "BTCUSDT")] = metrics

            signal_engine = SignalEngine(settings, allocator, market)
            execution_engine = ExecutionEngine(
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

            now = datetime.now(timezone.utc)
            next_funding = now + timedelta(minutes=15)
            opportunity = FundingOpportunity(
                symbol="BTCUSDT",
                long_exchange="bybit",
                short_exchange="mexc",
                long_rate=0.0001,
                short_rate=0.0009,
                spread=0.0008,
                long_mark=100.0,
                short_mark=100.1,
                long_next_funding=next_funding,
                short_next_funding=next_funding,
                price_diff=0.1,
                price_diff_pct=0.001,
                effective_spread=0.006,
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

            async def _snapshot_fetcher():
                return DataSnapshot(
                    generated_at=datetime.now(timezone.utc),
                    screener_rows=[],
                    coinglass_rows=[],
                    universe=[{"symbol": "BTCUSDT", "sources": "manual"}],
                    opportunities=[opportunity],
                    raw_payloads={},
                )

            orchestrator = ExecutionOrchestrator(
                signal_engine,
                execution_engine,
                lifecycle,
                market,
                telemetry,
                snapshot_fetcher=_snapshot_fetcher,
            )

            await orchestrator.run_once()
            lifecycle.evaluate(now=time.time() + settings.thresholds.observation_window_seconds + 10)
            for position in positions.active_positions():
                lifecycle.close(position.position_id)
            self.assertTrue(telemetry.tail())


if __name__ == "__main__":
    unittest.main()
