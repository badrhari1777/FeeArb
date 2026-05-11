from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from analysis_storage.coin_db import (
    get_paper_events,
    get_paper_positions,
    set_test_db_path,
)
from webapp.services import DataService


class CoinPaperFlowServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_paper_flow_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_paper_enter_and_actions_flow(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)

            async def _fake_exchange_row(exchange: str, *_args, **_kwargs):  # noqa: ANN001, ANN202
                if exchange == "binance":
                    price = 100.0
                    funding = 0.0001
                else:
                    price = 101.0
                    funding = 0.0002
                return {
                    "exchange": exchange,
                    "symbol": "BTCUSDT",
                    "status": "ok",
                    "snapshot": {
                        "bid": price,
                        "ask": price + 0.2,
                        "mark_price": price + 0.1,
                        "index_price": price - 0.1,
                        "next_funding_time": datetime.fromtimestamp(
                            (now_ms + 15 * 60_000) / 1000,
                            tz=timezone.utc,
                        ).isoformat(),
                    },
                    "funding_interval_hours_resolved": 1.0,
                    "latest_funding_rate": funding,
                    "candles_1m": [
                        {"ts_ms": now_ms - i * 60_000, "close": price + i * 0.01}
                        for i in range(60)
                    ],
                    "open_interest": {
                        "status": "ok",
                        "history": [
                            {"ts_ms": now_ms, "open_interest_notional": 1_000_000.0},
                            {
                                "ts_ms": now_ms - 6 * 3600_000,
                                "open_interest_notional": 950_000.0,
                            },
                        ],
                    },
                }

            with patch.object(
                self.service,
                "_coin_analysis_selected_exchanges",
                return_value=["binance", "kucoin"],
            ), patch.object(
                self.service,
                "_analyze_symbol_on_exchange",
                side_effect=_fake_exchange_row,
            ):
                entered = await self.service.coin_paper_enter(
                    {
                        "symbol": "BTCUSDT",
                        "qty": 1.0,
                        "action": "ENTRY_SMALL",
                    }
                )
                position_key = entered["position_key"]
                self.assertEqual(entered["status"], "open")
                self.assertEqual(entered["qty"], 1.0)

                add = await self.service.coin_paper_apply_action(
                    {
                        "position_key": position_key,
                        "action": "ADD_SMALL",
                    }
                )
                self.assertEqual(add["status"], "open")
                self.assertGreater(add["qty_after"], 1.0)

                partial = await self.service.coin_paper_apply_action(
                    {
                        "position_key": position_key,
                        "action": "PARTIAL_EXIT",
                        "fraction": 0.5,
                    }
                )
                self.assertEqual(partial["status"], "open")
                self.assertGreater(partial["qty_after"], 0.0)

                closed = await self.service.coin_paper_apply_action(
                    {
                        "position_key": position_key,
                        "action": "FULL_EXIT",
                    }
                )
                self.assertEqual(closed["status"], "closed")
                self.assertEqual(closed["qty_after"], 0.0)

            db_positions = get_paper_positions(status="closed")
            self.assertEqual(len(db_positions), 1)
            self.assertEqual(db_positions[0]["position_key"], position_key)

            events = get_paper_events(position_key, limit=10)
            self.assertEqual(len(events), 4)
            event_types = [row["event_type"] for row in events]
            self.assertIn("entry", event_types)
            self.assertIn("add_small", event_types)
            self.assertIn("partial_exit", event_types)
            self.assertIn("full_exit", event_types)

            listed = await self.service.get_coin_paper_positions(symbol="BTCUSDT", status="closed")
            self.assertEqual(listed["count"], 1)
            loaded_events = await self.service.get_coin_paper_events(position_key, limit=10)
            self.assertEqual(loaded_events["count"], 4)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
