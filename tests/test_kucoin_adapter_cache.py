from __future__ import annotations

import unittest
from unittest.mock import patch

from exchanges.kucoin import KucoinAdapter


class KucoinAdapterCacheTestCase(unittest.TestCase):
    def test_missing_contract_is_negative_cached(self) -> None:
        adapter = KucoinAdapter()
        contracts_payload = {
            "code": "200000",
            "data": [{"symbol": "BTCUSDTM", "fundingFeeRate": 0.0001}],
        }

        with patch("exchanges.kucoin._get_json", return_value=contracts_payload) as get_json, patch(
            "exchanges.kucoin.time.time",
            return_value=1000.0,
        ), patch("exchanges.kucoin.logger.info") as info:
            first = adapter.fetch_market_snapshots(["DEXEUSDT"])
            second = adapter.fetch_market_snapshots(["DEXEUSDT"])

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertEqual(get_json.call_count, 1)
        info.assert_called_once_with("KuCoin: contract %s not found", "DEXEUSDTM")

    def test_contracts_reload_after_ttl(self) -> None:
        adapter = KucoinAdapter()

        first_payload = {
            "code": "200000",
            "data": [
                {
                    "symbol": "ORCAUSDTM",
                    "fundingFeeRate": -0.001,
                    "nextFundingRateDateTime": 1771308000000,
                }
            ],
        }
        second_payload = {
            "code": "200000",
            "data": [
                {
                    "symbol": "ORCAUSDTM",
                    "fundingFeeRate": -0.0041,
                    "nextFundingRateDateTime": 1771311600000,
                }
            ],
        }

        with patch("exchanges.kucoin._get_json", side_effect=[first_payload, second_payload]), patch(
            "exchanges.kucoin.time.time",
            side_effect=[1000.0, 1000.0, 1035.5, 1035.5],
        ):
            first = adapter._load_contracts()
            second = adapter._load_contracts()
            third = adapter._load_contracts()

        self.assertAlmostEqual(float(first["ORCAUSDTM"]["fundingFeeRate"]), -0.001)
        self.assertAlmostEqual(float(second["ORCAUSDTM"]["fundingFeeRate"]), -0.001)
        self.assertAlmostEqual(float(third["ORCAUSDTM"]["fundingFeeRate"]), -0.0041)

    def test_contracts_keep_stale_cache_on_fetch_error(self) -> None:
        adapter = KucoinAdapter()
        ok_payload = {
            "code": "200000",
            "data": [{"symbol": "ORCAUSDTM", "fundingFeeRate": -0.001}],
        }
        with patch("exchanges.kucoin._get_json", return_value=ok_payload), patch(
            "exchanges.kucoin.time.time",
            side_effect=[1000.0, 1000.0],
        ):
            first = adapter._load_contracts()

        with patch("exchanges.kucoin._get_json", side_effect=RuntimeError("network down")), patch(
            "exchanges.kucoin.time.time",
            side_effect=[1035.0, 1035.0],
        ):
            second = adapter._load_contracts()

        self.assertEqual(first, second)
        self.assertIn("ORCAUSDTM", second)


if __name__ == "__main__":
    unittest.main()
