from __future__ import annotations

import unittest
from unittest.mock import patch

from exchanges.bitget import BitgetAdapter


class BitgetFundingHistoryTestCase(unittest.TestCase):
    def test_history_uses_v2_endpoint_and_parses_funding_time(self) -> None:
        adapter = BitgetAdapter()

        payload = {
            "code": "00000",
            "data": [
                {"symbol": "LABUSDT", "fundingRate": "-0.00348", "fundingTime": "1780592400000"},
                {"symbol": "LABUSDT", "fundingRate": "-0.019343", "fundingTime": "1780588800000"},
            ],
        }

        captured_urls: list[str] = []

        def _fake_get_json(url: str) -> dict:
            captured_urls.append(url)
            return payload

        with patch("exchanges.bitget._get_json", side_effect=_fake_get_json), patch(
            "exchanges.bitget.get_or_fetch_funding_history",
            side_effect=lambda _exchange, _symbol, fetch, **_kwargs: fetch(),
        ):
            rows = adapter.funding_history("LABUSDT", limit=20)

        self.assertEqual(len(rows), 2)
        self.assertIn("/api/v2/mix/market/history-fund-rate", captured_urls[0])
        self.assertIn("symbol=LABUSDT", captured_urls[0])
        self.assertIn("productType=USDT-FUTURES", captured_urls[0])
        self.assertEqual(rows[0]["ts_ms"], 1780592400000)
        self.assertAlmostEqual(float(rows[0]["rate"] or 0.0), -0.00348)


if __name__ == "__main__":
    unittest.main()
