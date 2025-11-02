from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.allocator import AllocationLimitsExceeded, Allocator, CooldownActiveError, SymbolLockedError
from execution.positions import PositionManager
from execution.settings import ExecutionSettings
from execution.wallet import WalletService


def _create_services(tmp_dir: Path):
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
    return wallet, positions, allocator


class AllocatorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir_ctx = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self.tmp_dir_ctx.name)
        self.wallet, self.positions, self.allocator = _create_services(self.tmp_dir)

    def tearDown(self) -> None:
        self.tmp_dir_ctx.cleanup()

    def test_prepare_and_activate(self) -> None:
        allocation = self.allocator.prepare_allocation(
            symbol="BTCUSDT",
            long_exchange="bybit",
            short_exchange="mexc",
            expected_edge=0.006,
        )
        self.assertTrue(self.allocator.is_symbol_locked("BTCUSDT"))

        position = self.allocator.activate_allocation(
            allocation.allocation_id,
            strategy="mode_a",
        )
        self.assertEqual(position.symbol, "BTCUSDT")
        self.assertTrue(self.positions.active_symbol("BTCUSDT"))

        self.positions.close_position(position.position_id)
        self.allocator.mark_symbol_closed("BTCUSDT", cooldown_seconds=1)

    def test_symbol_lock_and_cooldown(self) -> None:
        allocation = self.allocator.prepare_allocation(
            symbol="ETHUSDT",
            long_exchange="bybit",
            short_exchange="mexc",
            expected_edge=0.006,
        )
        with self.assertRaises(SymbolLockedError):
            self.allocator.prepare_allocation(
                symbol="ETHUSDT",
                long_exchange="bybit",
                short_exchange="mexc",
                expected_edge=0.006,
            )

        self.allocator.cancel_allocation(allocation.allocation_id)
        self.allocator.mark_symbol_closed("ETHUSDT", cooldown_seconds=5)
        with self.assertRaises(CooldownActiveError):
            self.allocator.prepare_allocation(
                symbol="ETHUSDT",
                long_exchange="bybit",
                short_exchange="mexc",
                expected_edge=0.006,
            )

    def test_capacity_limits(self) -> None:
        settings = ExecutionSettings()
        settings.balance.max_positions = 1
        allocator = Allocator(
            self.wallet,
            self.positions,
            settings,
            state_path=self.tmp_dir / "allocator_small.json",
        )
        allocator.prepare_allocation(
            symbol="BTCUSDT",
            long_exchange="bybit",
            short_exchange="mexc",
            expected_edge=0.006,
        )
        with self.assertRaises(AllocationLimitsExceeded):
            allocator.prepare_allocation(
                symbol="ETHUSDT",
                long_exchange="bybit",
                short_exchange="mexc",
                expected_edge=0.006,
            )


if __name__ == "__main__":
    unittest.main()
