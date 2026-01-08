from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.allocator import Allocator
from execution.risk import RiskGuard
from execution.settings import ExecutionSettings
from execution.wallet import WalletService
from execution.positions import PositionManager


def _services(tmp_dir: Path):
    wallet = WalletService({"bybit": 5000.0, "mexc": 4000.0}, state_path=tmp_dir / "wallet.json")
    positions = PositionManager(wallet, state_path=tmp_dir / "positions.json")
    settings = ExecutionSettings()
    allocator = Allocator(wallet, positions, settings, state_path=tmp_dir / "allocator.json")
    return wallet, positions, settings, allocator


class RiskGuardTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = tempfile.TemporaryDirectory()
        tmp_dir = Path(self.tmp_ctx.name)
        wallet, positions, settings, allocator = _services(tmp_dir)
        self.wallet = wallet
        self.settings = settings
        self.allocator = allocator
        self.guard = RiskGuard(settings, allocator, wallet)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_can_allocate(self) -> None:
        ok, reason = self.guard.can_allocate("BTCUSDT", 500.0)
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_leverage_limit(self) -> None:
        self.settings.risk.max_leverage = 0.05
        ok, reason = self.guard.can_allocate("BTCUSDT", 500.0)
        self.assertFalse(ok)
        self.assertIn("leverage_limit_exceeded", reason or "")

    def test_pause_on_failures(self) -> None:
        for _ in range(self.settings.risk.max_orphan_failures):
            self.guard.record_orphan_failure()
        status = self.guard.status()
        self.assertTrue(status.paused)
        self.assertEqual(status.reason, "orphan_failures")
        self.guard.resume()
        ok, _ = self.guard.can_allocate("BTCUSDT", 100.0)
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
