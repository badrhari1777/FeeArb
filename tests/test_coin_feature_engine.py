from __future__ import annotations

import unittest

from analysis_features import build_pair_feature_snapshots, compute_derived_spread_table


class CoinFeatureEngineTestCase(unittest.TestCase):
    def test_compute_derived_spread_table(self) -> None:
        left = {
            "bid": 100.0,
            "ask": 100.2,
            "mark_price": 100.1,
            "index_price": 99.9,
        }
        right = {
            "bid": 101.0,
            "ask": 101.3,
            "mark_price": 101.1,
            "index_price": 100.8,
        }
        table = compute_derived_spread_table(left, right)
        self.assertIsNotNone(table["mid_spread_pct"])
        self.assertIsNotNone(table["open_spread_long_a_short_b_pct"])
        self.assertIsNotNone(table["close_spread_long_b_short_a_pct"])
        self.assertIsNotNone(table["premium_diff_pct"])
        self.assertLess(table["open_spread_long_a_short_b_pct"] or 0.0, 0.0)

    def test_build_pair_feature_snapshots_directional(self) -> None:
        now_ts_ms = 1_700_000_000_000
        left_row = {
            "snapshot": {
                "bid": 100.0,
                "ask": 100.2,
                "mark_price": 100.1,
                "index_price": 99.9,
                "next_funding_time": "2023-11-14T22:23:20+00:00",
            },
            "funding_interval_hours_resolved": 1.0,
            "latest_funding_rate": 0.0001,
            "open_interest": {
                "history": [
                    {"ts_ms": now_ts_ms, "open_interest_notional": 1_200_000.0},
                    {"ts_ms": now_ts_ms - 6 * 3600_000, "open_interest_notional": 1_100_000.0},
                ]
            },
        }
        right_row = {
            "snapshot": {
                "bid": 101.0,
                "ask": 101.2,
                "mark_price": 101.0,
                "index_price": 100.7,
                "next_funding_time": "2023-11-14T22:20:00+00:00",
            },
            "funding_interval_hours_resolved": 1.0,
            "latest_funding_rate": 0.0002,
            "open_interest": {
                "history": [
                    {"ts_ms": now_ts_ms, "open_interest_notional": 1_250_000.0},
                    {"ts_ms": now_ts_ms - 6 * 3600_000, "open_interest_notional": 1_180_000.0},
                ]
            },
        }
        spread_series = [
            {"ts_ms": now_ts_ms - i * 60_000, "spread_pct": -0.5 + i * 0.01}
            for i in range(120)
        ]
        payload = build_pair_feature_snapshots(
            pair_key="BTCUSDT|binance|kucoin",
            canonical_symbol="BTCUSDT",
            left_exchange="binance",
            right_exchange="kucoin",
            left=left_row,
            right=right_row,
            spread_series=spread_series,
            coverage_pct=95.0,
            now_ts_ms=now_ts_ms,
        )
        self.assertEqual(payload["common"]["decision_phase"], "pre_boundary_15m")
        self.assertEqual(len(payload["directions"]), 2)
        directions = {item["direction"] for item in payload["directions"]}
        self.assertEqual(directions, {"long_a_short_b", "long_b_short_a"})
        self.assertIn("entry_score", payload["directions"][0]["scores"])


if __name__ == "__main__":
    unittest.main()
