from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.positions import (
    InvalidPositionStateError,
    PositionExistsError,
    PositionManager,
)
from execution.wallet import WalletService


def _setup(state_dir: Path) -> tuple[WalletService, PositionManager]:
    wallet = WalletService(
        {"bybit": 5000.0, "mexc": 4000.0},
        state_path=state_dir / "wallet.json",
    )
    manager = PositionManager(
        wallet,
        state_path=state_dir / "positions.json",
    )
    return wallet, manager


class PositionManagerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.tmp_dir.name)
        self.wallet, self.manager = _setup(self.state_dir)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_position_lifecycle(self) -> None:
        wallet, manager = self.wallet, self.manager
        long_res = wallet.reserve("bybit", amount=600.0, symbol="BTCUSDT", reason="long leg")
        short_res = wallet.reserve("mexc", amount=600.0, symbol="BTCUSDT", reason="short leg")

        position = manager.create_position(
            symbol="BTCUSDT",
            strategy="mode_a",
            long_reservation=long_res.reservation_id,
            short_reservation=short_res.reservation_id,
            long_exchange="bybit",
            short_exchange="mexc",
            notional=600.0,
        )

        self.assertAlmostEqual(wallet.get_account("bybit").in_positions, 600.0)
        self.assertAlmostEqual(wallet.get_account("mexc").in_positions, 600.0)

        manager.record_fill(position.position_id, leg="long", amount=600.0, price=107000.0)
        manager.record_fill(position.position_id, leg="short", amount=600.0, price=107100.0)
        manager.mark_hedged(position.position_id)

        manager.start_observation(position.position_id)
        manager.mark_exit_pending(position.position_id)
        manager.close_position(position.position_id)

        self.assertAlmostEqual(wallet.get_account("bybit").in_positions, 0.0)
        self.assertAlmostEqual(wallet.get_account("mexc").in_positions, 0.0)
        self.assertEqual(manager.get(position.position_id).status, "closed")

    def test_single_active_position_per_symbol(self) -> None:
        wallet, manager = self.wallet, self.manager
        long_res = wallet.reserve("bybit", amount=100.0, symbol="BTCUSDT", reason="long")
        short_res = wallet.reserve("mexc", amount=100.0, symbol="BTCUSDT", reason="short")
        manager.create_position(
            symbol="BTCUSDT",
            strategy="mode_a",
            long_reservation=long_res.reservation_id,
            short_reservation=short_res.reservation_id,
            long_exchange="bybit",
            short_exchange="mexc",
            notional=100.0,
        )

        with self.assertRaises(PositionExistsError):
            manager.create_position(
                symbol="BTCUSDT",
                strategy="mode_a",
                long_reservation=long_res.reservation_id,
                short_reservation=short_res.reservation_id,
                long_exchange="bybit",
                short_exchange="mexc",
                notional=100.0,
            )

    def test_cannot_mark_hedged_without_fills(self) -> None:
        wallet, manager = self.wallet, self.manager
        long_res = wallet.reserve("bybit", amount=100.0, symbol="ETHUSDT", reason="long")
        short_res = wallet.reserve("mexc", amount=100.0, symbol="ETHUSDT", reason="short")
        position = manager.create_position(
            symbol="ETHUSDT",
            strategy="mode_a",
            long_reservation=long_res.reservation_id,
            short_reservation=short_res.reservation_id,
            long_exchange="bybit",
            short_exchange="mexc",
            notional=100.0,
        )

        with self.assertRaises(InvalidPositionStateError):
            manager.mark_hedged(position.position_id)


if __name__ == "__main__":
    unittest.main()
