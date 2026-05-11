from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from analysis_storage.coin_db import set_test_db_path
from webapp.services import DataService


class CoinSymbolSessionsServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_sessions_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_bootstrap_and_list_sessions(self) -> None:
        async def _run() -> None:
            first = await self.service.bootstrap_symbol_session(
                "btcusdt",
                ttl_sec=120,
                now_ms=1_700_000_000_000,
            )
            self.assertEqual(first["canonical_symbol"], "BTCUSDT")
            self.assertEqual(first["expires_at_ms"], 1_700_000_120_000)

            # Re-bootstrap should extend/update the same symbol session.
            second = await self.service.bootstrap_symbol_session(
                "BTCUSDT",
                ttl_sec=180,
                now_ms=1_700_000_010_000,
            )
            self.assertEqual(second["expires_at_ms"], 1_700_000_190_000)

            active = await self.service.list_active_coin_symbol_sessions(
                now_ms=1_700_000_050_000
            )
            self.assertEqual(len(active), 1)
            self.assertEqual(active[0]["canonical_symbol"], "BTCUSDT")

            expired = await self.service.list_active_coin_symbol_sessions(
                now_ms=1_700_000_250_000
            )
            self.assertEqual(expired, [])

        asyncio.run(_run())

    def test_start_extend_stop_wrappers(self) -> None:
        async def _run() -> None:
            start = await self.service.start_coin_symbol_session(
                "ETHUSDT",
                ttl_sec=120,
                now_ms=1_700_100_000_000,
            )
            self.assertEqual(start["canonical_symbol"], "ETHUSDT")
            self.assertTrue(start["tracking"])

            extend = await self.service.extend_coin_symbol_session(
                "ethusdt",
                ttl_sec=300,
                now_ms=1_700_100_010_000,
            )
            self.assertEqual(extend["canonical_symbol"], "ETHUSDT")
            self.assertEqual(extend["expires_at_ms"], 1_700_100_310_000)

            active = await self.service.list_active_coin_symbol_sessions(
                now_ms=1_700_100_020_000
            )
            self.assertEqual(len(active), 1)
            self.assertEqual(active[0]["canonical_symbol"], "ETHUSDT")

            stop = await self.service.stop_coin_symbol_session(
                "ETHUSDT",
                now_ms=1_700_100_030_000,
            )
            self.assertEqual(stop["canonical_symbol"], "ETHUSDT")
            self.assertFalse(stop["tracking"])

            after_stop = await self.service.list_active_coin_symbol_sessions(
                now_ms=1_700_100_040_000
            )
            self.assertEqual(after_stop, [])

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
