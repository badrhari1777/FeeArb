from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from execution.allocator import Allocator
from execution.lifecycle import LifecycleController
from execution.settings import ExecutionSettings
from execution.wallet import WalletService
from execution.positions import PositionManager


def _setup(tmp_dir: Path):
    wallet = WalletService({"bybit": 5000.0, "mexc": 4000.0}, state_path=tmp_dir / "wallet.json")
    positions = PositionManager(wallet, state_path=tmp_dir / "positions.json")
    settings = ExecutionSettings()
    allocator = Allocator(wallet, positions, settings, state_path=tmp_dir / "allocator.json")
    return wallet, positions, settings, allocator


class LifecycleControllerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmp.name)
        wallet, positions, settings, allocator = _setup(tmp_path)
        self.wallet = wallet
        self.positions = positions
        self.settings = settings
        self.allocator = allocator
        self.controller = LifecycleController(settings, positions, allocator)

        long_res = wallet.reserve("bybit", amount=500.0, symbol="BTCUSDT", reason="test")
        short_res = wallet.reserve("mexc", amount=500.0, symbol="BTCUSDT", reason="test")
        position = positions.create_position(
            symbol="BTCUSDT",
            strategy="mode_a",
            long_reservation=long_res.reservation_id,
            short_reservation=short_res.reservation_id,
            long_exchange="bybit",
            short_exchange="mexc",
            notional=500.0,
        )
        positions.record_fill(position.position_id, leg="long", amount=500.0, price=100.0)
        positions.record_fill(position.position_id, leg="short", amount=500.0, price=100.1)
        positions.mark_hedged(position.position_id)
        positions.start_observation(position.position_id)
        self.position_id = position.position_id

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_evaluate_marks_exit_pending(self) -> None:
        window = self.settings.thresholds.observation_window_seconds
        due = self.controller.evaluate(now=time.time() + window + 5)
        self.assertTrue(due)
        self.assertEqual(due[0].status, "exit_pending")

    def test_close_updates_cooldown(self) -> None:
        self.controller.close(self.position_id)
        remaining = self.allocator.cooldown_remaining("BTCUSDT")
        self.assertGreater(remaining, 0.0)


if __name__ == "__main__":
    unittest.main()
