from __future__ import annotations

import unittest

from analysis_decisions import evaluate_position_signal


class CoinPositionRulesTestCase(unittest.TestCase):
    def test_full_exit_on_high_continuation_risk(self) -> None:
        signal = evaluate_position_signal(
            position_key="p1",
            pair_key="BTCUSDT|binance|kucoin",
            direction="long_a_short_b",
            qty=1.0,
            decision_phase="pre_boundary_15m",
            spread_coverage_pct=92.0,
            direction_feature={
                "scores": {"entry_score": 20.0, "continuation_risk_score": 85.0},
                "directional": {"funding_to_next_pct": -0.0005, "reversion_potential_pct": -0.1},
            },
        )
        self.assertEqual(signal["action"], "FULL_EXIT")
        self.assertIn("position_thesis_deteriorating", signal["reason_codes"])

    def test_add_small_inside_decision_window(self) -> None:
        signal = evaluate_position_signal(
            position_key="p2",
            pair_key="ETHUSDT|binance|kucoin",
            direction="long_b_short_a",
            qty=0.5,
            decision_phase="pre_boundary_20m",
            spread_coverage_pct=88.0,
            direction_feature={
                "scores": {"entry_score": 78.0, "continuation_risk_score": 30.0},
                "directional": {"funding_to_next_pct": 0.0002, "reversion_potential_pct": 0.4},
            },
        )
        self.assertEqual(signal["action"], "ADD_SMALL")
        self.assertIn("decision_window_active", signal["reason_codes"])

    def test_add_blocked_outside_window(self) -> None:
        signal = evaluate_position_signal(
            position_key="p3",
            pair_key="SOLUSDT|binance|kucoin",
            direction="long_a_short_b",
            qty=2.0,
            decision_phase="boundary_immediate",
            spread_coverage_pct=90.0,
            direction_feature={
                "scores": {"entry_score": 80.0, "continuation_risk_score": 25.0},
                "directional": {"funding_to_next_pct": 0.0001, "reversion_potential_pct": 0.2},
            },
        )
        self.assertEqual(signal["action"], "ADD_BLOCKED")
        self.assertIn("add_blocked_risk", signal["reason_codes"])


if __name__ == "__main__":
    unittest.main()
