from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from analysis_storage.coin_db import (
    CoinPaperPositionRow,
    set_test_db_path,
    upsert_paper_position,
)
from webapp.services import DataService


class CoinPositionWatcherServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_position_watcher_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_run_once_processes_open_paper_symbol(self) -> None:
        async def _run() -> None:
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="paper-watch-1",
                    opened_at_ms=1_700_000_000_000,
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=0.3,
                    entry_context={"source": "unit_test"},
                )
            )
            fake_payload = {
                "position_logic": {
                    "summary": {
                        "paper_decisions_saved": 1,
                        "real_decisions_saved": 2,
                    }
                }
            }
            with patch.object(self.service, "analyze_symbol", new=AsyncMock(return_value=fake_payload)) as mocked:
                cycle = await self.service.run_coin_position_watcher_once(
                    force=True,
                    window_minutes=120,
                    funding_points=48,
                )

            self.assertEqual(cycle["symbols_total"], 1)
            self.assertEqual(cycle["symbols_processed"], 1)
            self.assertEqual(cycle["errors"], 0)
            self.assertEqual(cycle["position_decisions_saved"], 3)
            self.assertEqual(cycle["symbols_analyzed"], ["BTCUSDT"])
            self.assertEqual(mocked.await_count, 1)
            kwargs = mocked.await_args.kwargs
            self.assertFalse(kwargs["use_cache"])
            self.assertFalse(kwargs["persist_candidate_decision"])
            self.assertTrue(kwargs["run_position_logic"])
            self.assertEqual(kwargs["window_minutes"], 120)
            self.assertEqual(kwargs["funding_points"], 48)

            status = await self.service.get_coin_position_watcher_status(symbol="BTCUSDT")
            self.assertTrue(status["enabled"])
            self.assertGreater(int((status.get("last_cycle") or {}).get("ts_ms") or 0), 0)
            self.assertIsNotNone(status.get("symbol_last_run_ts_ms"))

        asyncio.run(_run())

    def test_run_once_respects_symbol_cooldown(self) -> None:
        async def _run() -> None:
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="paper-watch-2",
                    opened_at_ms=1_700_000_000_000,
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="ETHUSDT",
                    pair_key="ETHUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=0.2,
                    entry_context={"source": "unit_test"},
                )
            )
            fake_payload = {"position_logic": {"summary": {"paper_decisions_saved": 0, "real_decisions_saved": 0}}}
            with patch.object(self.service, "analyze_symbol", new=AsyncMock(return_value=fake_payload)) as mocked:
                first = await self.service.run_coin_position_watcher_once(force=False)
                second = await self.service.run_coin_position_watcher_once(force=False)

            self.assertEqual(first["symbols_processed"], 1)
            self.assertEqual(second["symbols_processed"], 0)
            self.assertEqual(second["symbols_skipped_cooldown"], 1)
            self.assertEqual(mocked.await_count, 1)

        asyncio.run(_run())

    def test_run_once_returns_disabled_cycle(self) -> None:
        async def _run() -> None:
            await self.service.set_coin_position_watcher_enabled(False)
            cycle = await self.service.run_coin_position_watcher_once(force=False)
            self.assertFalse(cycle["enabled"])
            self.assertEqual(cycle["reason"], "watcher_disabled")
            self.assertEqual(cycle["symbols_processed"], 0)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
