from __future__ import annotations

import unittest
from unittest.mock import patch

from exchanges.binance import BinanceAdapter


class BinanceSnapshotBatchingTestCase(unittest.TestCase):
    def test_fetch_market_snapshots_uses_batch_endpoints(self) -> None:
        calls: list[str] = []

        def _fake_get_json(url: str):  # noqa: ANN202
            calls.append(url)
            if url.endswith("/fapi/v1/premiumIndex"):
                return [
                    {
                        "symbol": "BTCUSDT",
                        "lastFundingRate": "0.0001",
                        "nextFundingTime": "1700000000000",
                        "markPrice": "50000",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "lastFundingRate": "0.0002",
                        "nextFundingTime": "1700000000000",
                        "markPrice": "2500",
                    },
                ]
            if url.endswith("/fapi/v1/ticker/bookTicker"):
                return [
                    {
                        "symbol": "BTCUSDT",
                        "bidPrice": "49999",
                        "askPrice": "50001",
                        "bidQty": "10",
                        "askQty": "10",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "bidPrice": "2499",
                        "askPrice": "2501",
                        "bidQty": "20",
                        "askQty": "20",
                    },
                ]
            if url.endswith("/fapi/v1/exchangeInfo"):
                return {
                    "symbols": [
                        {
                            "symbol": "BTCUSDT",
                            "contractSize": "1",
                            "maxLeverage": "100",
                            "filters": [],
                        },
                        {
                            "symbol": "ETHUSDT",
                            "contractSize": "1",
                            "maxLeverage": "100",
                            "filters": [],
                        },
                    ]
                }
            raise AssertionError(f"Unexpected URL: {url}")

        with patch("exchanges.binance._get_json", side_effect=_fake_get_json), patch(
            "exchanges.binance.get_or_fetch_symbol_meta",
            return_value=None,
        ):
            rows = BinanceAdapter().fetch_market_snapshots(["BTCUSDT", "ETHUSDT"])

        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(1 for item in calls if "/premiumIndex?" in item), 0)
        self.assertEqual(sum(1 for item in calls if "/bookTicker?" in item), 0)
        self.assertIn("https://fapi.binance.com/fapi/v1/premiumIndex", calls)
        self.assertIn("https://fapi.binance.com/fapi/v1/ticker/bookTicker", calls)
        self.assertIn("https://fapi.binance.com/fapi/v1/exchangeInfo", calls)


if __name__ == "__main__":
    unittest.main()
