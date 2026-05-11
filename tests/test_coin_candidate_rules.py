from __future__ import annotations

import unittest

from analysis_decisions import evaluate_candidate_pairs


class CoinCandidateRulesTestCase(unittest.TestCase):
    def test_entry_strong_candidate(self) -> None:
        payload = evaluate_candidate_pairs(
            [
                {
                    "pair_key": "BTCUSDT|binance|kucoin",
                    "left_exchange": "binance",
                    "right_exchange": "kucoin",
                    "selected_direction": "long_a_short_b",
                    "selected_action": "ENTRY_STRONG",
                    "score": 81.2,
                    "decision_phase": "pre_boundary_15m",
                    "spread": {"coverage_pct": 92.0},
                    "funding_interval_hours": {"match": True},
                    "reasons": ["spread_reversion_favorable"],
                    "feature_snapshot_ids": {"long_a_short_b": 123},
                }
            ]
        )
        self.assertEqual(payload["decision"], "enter_candidate")
        self.assertEqual(payload["recommended_action"], "ENTRY_STRONG")
        self.assertEqual(payload["recommended_pair"]["feature_snapshot_id"], 123)

    def test_block_on_low_coverage(self) -> None:
        payload = evaluate_candidate_pairs(
            [
                {
                    "pair_key": "ETHUSDT|binance|kucoin",
                    "left_exchange": "binance",
                    "right_exchange": "kucoin",
                    "selected_direction": "long_b_short_a",
                    "selected_action": "ENTRY_SMALL",
                    "score": 75.0,
                    "decision_phase": "pre_boundary_20m",
                    "spread": {"coverage_pct": 42.0},
                    "funding_interval_hours": {"match": True},
                    "reasons": ["spread_reversion_favorable"],
                }
            ]
        )
        self.assertEqual(payload["decision"], "reject")
        self.assertEqual(payload["recommended_action"], "NO_TRADE")
        self.assertIn("spread_history_low_coverage", payload["reason_codes"])


if __name__ == "__main__":
    unittest.main()
