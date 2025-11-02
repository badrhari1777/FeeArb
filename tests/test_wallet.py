from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.wallet import InsufficientFundsError, WalletService


class WalletServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tmp_dir.name) / "wallet.json"

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_reserve_commit_and_close(self) -> None:
        wallet = WalletService(
            {"bybit": 5000.0, "mexc": 4000.0},
            state_path=self.state_path,
        )

        reservation = wallet.reserve(
            "bybit", amount=800.0, symbol="BTCUSDT", reason="test"
        )
        account = wallet.get_account("bybit")
        self.assertAlmostEqual(account.available, 4200.0)
        self.assertAlmostEqual(account.reserved, 800.0)

        wallet.commit(reservation.reservation_id)
        account = wallet.get_account("bybit")
        self.assertAlmostEqual(account.reserved, 0.0)
        self.assertAlmostEqual(account.in_positions, 800.0)

        wallet.close_position("bybit", amount=800.0)
        account = wallet.get_account("bybit")
        self.assertAlmostEqual(account.in_positions, 0.0)
        self.assertAlmostEqual(account.available, 5000.0)

    def test_release_and_persistence(self) -> None:
        wallet = WalletService({"bybit": 5000.0}, state_path=self.state_path)
        reservation = wallet.reserve("bybit", amount=1000.0, symbol="ETHUSDT", reason="prep")
        wallet.release(reservation.reservation_id)

        reloaded = WalletService({"bybit": 0.0}, state_path=self.state_path)
        account = reloaded.get_account("bybit")
        self.assertAlmostEqual(account.available, 5000.0)
        self.assertFalse(list(reloaded.active_reservations()))

    def test_insufficient_available(self) -> None:
        wallet = WalletService({"bybit": 100.0}, state_path=self.state_path)
        with self.assertRaises(InsufficientFundsError):
            wallet.reserve("bybit", amount=150.0, symbol="BTCUSDT", reason="too much")


if __name__ == "__main__":
    unittest.main()
