from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analysis_registry.coin_instruments import (
    binance_exchange_symbol_to_canonical,
    kucoin_exchange_symbol_to_canonical,
    refresh_binance_kucoin_registry,
)
from analysis_storage.coin_db import (
    get_instruments,
    get_pairs,
    set_test_db_path,
)


class CoinInstrumentsRegistryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_registry_test.db")

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_symbol_normalization_helpers(self) -> None:
        self.assertEqual(binance_exchange_symbol_to_canonical("BTCUSDT"), "BTCUSDT")
        self.assertEqual(binance_exchange_symbol_to_canonical("ZKUSDTUSDT"), "ZKUSDT")
        self.assertIsNone(binance_exchange_symbol_to_canonical("BTCUSD_PERP"))

        self.assertEqual(kucoin_exchange_symbol_to_canonical("XBTUSDTM"), "BTCUSDT")
        self.assertEqual(kucoin_exchange_symbol_to_canonical("ETHUSDTM"), "ETHUSDT")
        self.assertIsNone(kucoin_exchange_symbol_to_canonical("ETHUSD"))

    def test_refresh_persists_shared_pairs(self) -> None:
        binance_payload = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "contractSize": "1",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                },
                {
                    "symbol": "ETHUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                    "contractSize": "1",
                    "filters": [],
                },
                {
                    "symbol": "BNBUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "BNB",
                    "quoteAsset": "USDT",
                    "contractSize": "1",
                    "filters": [],
                },
            ]
        }
        kucoin_payload = {
            "code": "200000",
            "data": [
                {
                    "symbol": "XBTUSDTM",
                    "baseCurrency": "XBT",
                    "quoteCurrency": "USDT",
                    "status": "Open",
                    "tickSize": "0.1",
                    "lotSize": "1",
                    "currentFundingRateGranularity": 28800000,
                },
                {
                    "symbol": "ETHUSDTM",
                    "baseCurrency": "ETH",
                    "quoteCurrency": "USDT",
                    "status": "Open",
                    "tickSize": "0.01",
                    "lotSize": "1",
                    "currentFundingRateGranularity": 28800000,
                },
                {
                    "symbol": "SOLUSDTM",
                    "baseCurrency": "SOL",
                    "quoteCurrency": "USDT",
                    "status": "Open",
                    "tickSize": "0.001",
                    "lotSize": "1",
                    "currentFundingRateGranularity": 28800000,
                },
            ],
        }

        def fake_get_json(url: str):  # noqa: ANN001, ANN201
            if "fapi.binance.com" in url:
                return binance_payload
            if "api-futures.kucoin.com" in url:
                return kucoin_payload
            raise AssertionError(f"unexpected url: {url}")

        with patch("analysis_registry.coin_instruments._get_json", side_effect=fake_get_json):
            stats = refresh_binance_kucoin_registry()

        self.assertEqual(stats.binance_instruments, 3)
        self.assertEqual(stats.kucoin_instruments, 3)
        self.assertEqual(stats.shared_pairs, 2)

        instruments = get_instruments()
        self.assertEqual(len(instruments), 6)
        pairs = get_pairs()
        self.assertEqual(len(pairs), 2)
        self.assertEqual({item["canonical_symbol"] for item in pairs}, {"BTCUSDT", "ETHUSDT"})


if __name__ == "__main__":
    unittest.main()

