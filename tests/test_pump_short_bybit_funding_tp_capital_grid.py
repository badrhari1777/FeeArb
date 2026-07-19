from __future__ import annotations

import unittest

from analysis_features.pump_short_bybit_funding_tp_capital_grid import (
    Outcome,
    funding_window_sum_pct,
    simulate_slots,
    strategy_name,
)


class PumpShortBybitFundingTpCapitalGridTestCase(unittest.TestCase):
    def test_funding_window_sum_uses_only_selected_previous_hours(self) -> None:
        end_ms = 1_900_000_000_000
        rows = [
            (end_ms - 20 * 3_600_000, -0.004),
            (end_ms - 6 * 3_600_000, -0.002),
            (end_ms - 2 * 3_600_000, 0.001),
        ]

        value_24h, points_24h = funding_window_sum_pct(rows, end_ms, 24)
        value_3h, points_3h = funding_window_sum_pct(rows, end_ms, 3)

        self.assertAlmostEqual(value_24h, -0.5)
        self.assertEqual(points_24h, 3)
        self.assertAlmostEqual(value_3h, 0.1)
        self.assertEqual(points_3h, 1)

    def test_simulate_slots_scales_from_base_leg_notional(self) -> None:
        outcome = Outcome(
            symbol="TESTUSDT",
            trigger_ts=1_900_000_000_000,
            entry_ts=1_900_003_600_000,
            exit_ts=1_900_007_200_000,
            funding_window_h=24,
            funding_min_pct=-0.5,
            funding_prev_pct=-0.2,
            funding_points=3,
            tp_pct=25.0,
            pnl_usd_base=100.0,
            net_pct=10.0,
            funding_during_pct=0.0,
            mae_pct=20.0,
            legs_filled=1,
            exit_reason="take_profit",
            hold_h=1.0,
            win=1,
            cat300=0,
        )

        result = simulate_slots([outcome], capital_usd=1_000.0, slots=1, leverage=3.0)
        summary = result["summary"]

        self.assertEqual(summary["strategy"], strategy_name(funding_window_h=24, funding_min_pct=-0.5, tp_pct=25.0))
        self.assertAlmostEqual(summary["per_step_notional_usd"], 750.0)
        self.assertAlmostEqual(summary["net_pnl_usd"], 75.0)
        self.assertAlmostEqual(summary["roi_on_initial_pct"], 7.5)


if __name__ == "__main__":
    unittest.main()
