from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from orchestrator.models import MarketSnapshot

from analysis_storage.coin_db import CoinPaperPositionRow, set_test_db_path, upsert_paper_position
from webapp.services import DataService


class _FakeAdapter:
    def __init__(self, exchange: str) -> None:
        self.exchange = exchange

    async def fetch_market_snapshots_async(self, symbols):  # noqa: ANN001, ANN201
        symbol = str((symbols or ["BTCUSDT"])[0]).upper()
        if self.exchange == "binance":
            return [
                MarketSnapshot(
                    exchange="binance",
                    symbol=symbol,
                    exchange_symbol=symbol,
                    funding_rate=0.0001,
                    next_funding_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    mark_price=100.5,
                    bid=100.0,
                    ask=101.0,
                    raw={"premiumIndex": {"indexPrice": "100.2"}},
                    bid_size=10.0,
                    ask_size=11.0,
                )
            ]
        return [
            MarketSnapshot(
                exchange="kucoin",
                symbol=symbol,
                exchange_symbol="XBTUSDTM" if symbol == "BTCUSDT" else f"{symbol[:-4]}USDTM",
                funding_rate=0.0002,
                next_funding_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                mark_price=100.7,
                bid=100.3,
                ask=101.2,
                raw={"contract": {"indexPrice": "100.4", "predictedFundingFeeRate": "0.00025"}},
                bid_size=7.0,
                ask_size=8.0,
            )
        ]


class CoinFocusCollectorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_focus_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_collect_coin_focus_once_inserts_rows_for_active_session(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            await self.service.bootstrap_symbol_session(
                "BTCUSDT",
                ttl_sec=300,
                now_ms=now_ms,
            )

            with patch.object(
                self.service,
                "_coin_analysis_selected_exchanges",
                return_value=["binance", "kucoin"],
            ), patch("webapp.services.get_adapter_cached", side_effect=lambda name: _FakeAdapter(name)):
                stats = await self.service.collect_coin_focus_once()

            self.assertEqual(stats["symbols"], 1)
            self.assertEqual(stats["rows"], 2)
            self.assertEqual(stats["session_symbols"], 1)
            self.assertEqual(stats["held_symbols"], 0)

            payload = await self.service.get_coin_focus_snapshots("BTCUSDT", limit=10)
            self.assertEqual(payload["points"], 2)
            exchanges = {row["exchange"] for row in payload["rows"]}
            self.assertEqual(exchanges, {"binance", "kucoin"})

        asyncio.run(_run())

    def test_collect_coin_focus_once_inserts_rows_for_open_paper_without_session(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="paper-btc-1",
                    opened_at_ms=now_ms - 60_000,
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=0.2,
                    entry_context={"source": "unit_test"},
                )
            )

            with patch.object(
                self.service,
                "_coin_analysis_selected_exchanges",
                return_value=["binance", "kucoin"],
            ), patch("webapp.services.get_adapter_cached", side_effect=lambda name: _FakeAdapter(name)):
                stats = await self.service.collect_coin_focus_once()

            self.assertEqual(stats["symbols"], 1)
            self.assertEqual(stats["rows"], 2)
            self.assertEqual(stats["session_symbols"], 0)
            self.assertEqual(stats["held_symbols"], 1)

            payload = await self.service.get_coin_focus_snapshots("BTCUSDT", limit=10)
            self.assertEqual(payload["points"], 2)
            reasons = {str(row.get("focus_reason") or "") for row in payload["rows"]}
            self.assertEqual(reasons, {"held_position"})

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
