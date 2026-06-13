from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from exchanges.binance import BinanceAdapter
from exchanges.bybit import BybitAdapter
from exchanges.okx import OKXAdapter, _resolve_next_funding_time
from exchanges.gate import GateAdapter
from exchanges.bitget import BitgetAdapter
from exchanges.mexc import MexcAdapter
from exchanges.bingx import BingXAdapter, _to_datetime as bingx_to_datetime


def _cache_passthrough(exchange, symbol, fetch_fn, **kwargs):  # noqa: ANN001, ANN201
    del exchange, symbol, kwargs
    return list(fetch_fn() or [])


class FundingHistoryAdaptersTestCase(unittest.TestCase):
    def test_binance_history_infers_interval(self) -> None:
        payload = [
            {"fundingTime": 1_700_028_800_000, "fundingRate": "0.0001"},
            {"fundingTime": 1_700_000_000_000, "fundingRate": "0.0002"},
        ]
        with patch("exchanges.binance._get_json", return_value=payload), patch(
            "exchanges.binance.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            rows = BinanceAdapter().funding_history("BTCUSDT", limit=2)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0].get("interval_hours") or 0.0), 8.0, places=6)

    def test_bybit_history_infers_interval(self) -> None:
        payload = {
            "retCode": 0,
            "result": {
                "list": [
                    {"fundingRateTimestamp": "1700028800000", "fundingRate": "0.0001"},
                    {"fundingRateTimestamp": "1700000000000", "fundingRate": "0.0002"},
                ]
            },
        }
        with patch("exchanges.bybit._get_json", return_value=payload), patch(
            "exchanges.bybit.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            rows = BybitAdapter().funding_history("BTCUSDT", limit=2)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0].get("interval_hours") or 0.0), 8.0, places=6)

    def test_bybit_history_overrides_stale_cached_declared_interval(self) -> None:
        cached_rows = [
            {"ts_ms": 1_700_007_200_000, "rate": "0.0001", "interval_hours": 4.0},
            {"ts_ms": 1_700_003_600_000, "rate": "0.0002", "interval_hours": 4.0},
            {"ts_ms": 1_700_000_000_000, "rate": "0.0003", "interval_hours": 4.0},
        ]
        with patch(
            "exchanges.bybit.get_or_fetch_funding_history",
            return_value=cached_rows,
        ):
            rows = BybitAdapter().funding_history("BTCUSDT", limit=3)
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(float(row.get("interval_hours") or 0.0) == 1.0 for row in rows))

    def test_okx_history_infers_interval(self) -> None:
        payload = {
            "code": "0",
            "data": [
                {"fundingTime": "1700014400000", "fundingRate": "0.0001"},
                {"fundingTime": "1700000000000", "fundingRate": "0.0002"},
            ],
        }
        with patch("exchanges.okx._get_json", return_value=payload), patch(
            "exchanges.okx.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            rows = OKXAdapter().funding_history("BTCUSDT", limit=2)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0].get("interval_hours") or 0.0), 4.0, places=6)

    def test_okx_next_funding_prefers_nearest_upcoming_slot(self) -> None:
        now = datetime.now(timezone.utc)
        funding_dt = now + timedelta(minutes=30)
        next_dt = now + timedelta(minutes=90)
        item = {
            "fundingTime": str(int(funding_dt.timestamp() * 1000)),
            "nextFundingTime": str(int(next_dt.timestamp() * 1000)),
        }
        chosen = _resolve_next_funding_time(item)
        self.assertIsNotNone(chosen)
        self.assertEqual(int(chosen.timestamp()), int(funding_dt.timestamp()))

    def test_gate_history_infers_interval_from_seconds_timestamps(self) -> None:
        payload = [
            {"t": 1_700_014_400, "r": "0.0001"},
            {"t": 1_700_000_000, "r": "0.0002"},
        ]
        with patch("exchanges.gate._get_json", return_value=payload), patch(
            "exchanges.gate.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            rows = GateAdapter().funding_history("BTCUSDT", limit=2)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0].get("interval_hours") or 0.0), 4.0, places=6)

    def test_gate_history_uses_btc_settle_for_inverse_contracts(self) -> None:
        seen_urls: list[str] = []

        def _fake_get_json(url: str):  # noqa: ANN001
            seen_urls.append(url)
            return []

        with patch("exchanges.gate._get_json", side_effect=_fake_get_json), patch(
            "exchanges.gate.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            GateAdapter().funding_history("BTCUSD", limit=2)
        self.assertEqual(len(seen_urls), 1)
        self.assertIn("/futures/btc/funding_rate?", seen_urls[0])

    def test_bitget_history_infers_interval(self) -> None:
        payload = {
            "code": "00000",
            "data": [
                {"timePoint": "1700028800000", "fundRate": "0.0001"},
                {"timePoint": "1700000000000", "fundRate": "0.0002"},
            ],
        }
        with patch("exchanges.bitget._get_json", return_value=payload), patch(
            "exchanges.bitget.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            rows = BitgetAdapter().funding_history("BTCUSDT", limit=2)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0].get("interval_hours") or 0.0), 8.0, places=6)

    def test_mexc_history_normalizes_collect_cycle_seconds(self) -> None:
        payload = {
            "data": [
                {
                    "timestamp": 1_700_028_800,
                    "fundingRate": "0.0001",
                    "collectCycle": 28_800,
                },
                {
                    "timestamp": 1_700_000_000,
                    "fundingRate": "0.0002",
                    "collectCycle": 28_800,
                },
            ]
        }
        with patch("exchanges.mexc._get_json", return_value=payload), patch(
            "exchanges.mexc.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            rows = MexcAdapter().funding_history("BTCUSDT", limit=2)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0].get("interval_hours") or 0.0), 8.0, places=6)

    def test_bingx_seconds_datetime_parsing(self) -> None:
        dt = bingx_to_datetime(1_700_000_000)
        self.assertIsNotNone(dt)
        self.assertGreater((dt.year if dt else 0), 2020)

    def test_bingx_history_uses_normalized_timestamp(self) -> None:
        payload = {
            "data": [
                {
                    "timestamp": 1_700_000_000,
                    "fundingRate": "0.0001",
                    "fundingIntervalHours": 8,
                }
            ]
        }
        with patch("exchanges.bingx._get_json", return_value=payload), patch(
            "exchanges.bingx.get_or_fetch_funding_history",
            side_effect=_cache_passthrough,
        ):
            rows = BingXAdapter().funding_history("BTCUSDT", limit=1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(rows[0].get("ts_ms") or 0), 1_700_000_000_000)
        self.assertAlmostEqual(float(rows[0].get("interval_hours") or 0.0), 8.0, places=6)


if __name__ == "__main__":
    unittest.main()
