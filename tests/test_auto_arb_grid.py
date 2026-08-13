from __future__ import annotations

import unittest

from execution.auto_arb_grid import (
    build_grid_levels,
    decide_grid_transition,
    grid_completion_tolerance,
    grid_level_count_for_existing_qty,
    grid_rules_share_live_ownership,
    grid_symbol_ownership_key,
    recommend_level_count,
)


class AutoArbGridTestCase(unittest.TestCase):
    def test_builds_inclusive_negative_spread_levels(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=10_000,
        )

        self.assertEqual([row["entry_spread_pct"] for row in levels], [-2, -4, -6, -8, -10])
        self.assertEqual([row["exit_spread_pct"] for row in levels], [-1.5, -3.5, -5.5, -7.5, -9.5])
        self.assertEqual(levels[-1]["cumulative_qty"], 10_000)
        self.assertEqual(levels[0]["qty"], 2_000)

    def test_rejects_reversed_range(self) -> None:
        with self.assertRaises(ValueError):
            build_grid_levels(
                range_start_pct=-10,
                range_end_pct=-2,
                level_count=5,
                exit_gap_pct=0.5,
                max_qty=100,
            )

    def test_enters_only_one_level_after_large_jump(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=100,
        )
        decision = decide_grid_transition(
            entry_spread_pct=-9,
            exit_spread_pct=-8.8,
            levels=levels,
            current_level=0,
            max_levels_per_cycle=1,
        )

        self.assertEqual(decision["entry_target_level"], 4)
        self.assertEqual(decision["action"], "enter")
        self.assertEqual(decision["target_level"], 1)

    def test_exits_last_opened_level_on_reversion(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=100,
        )
        decision = decide_grid_transition(
            entry_spread_pct=-5.8,
            exit_spread_pct=-5.4,
            levels=levels,
            current_level=3,
        )

        self.assertEqual(decision["action"], "exit")
        self.assertEqual(decision["exit_target_level"], 2)
        self.assertEqual(decision["target_level"], 2)

    def test_hysteresis_holds_position_between_thresholds(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=100,
        )
        decision = decide_grid_transition(
            entry_spread_pct=-4.1,
            exit_spread_pct=-4.2,
            levels=levels,
            current_level=2,
        )

        self.assertEqual(decision["action"], "none")
        self.assertEqual(decision["target_level"], 2)

    def test_recommends_bounded_level_count(self) -> None:
        self.assertEqual(recommend_level_count(total_qty=10_000, safe_chunk_qty=1_200), 9)
        self.assertEqual(recommend_level_count(total_qty=10_000, safe_chunk_qty=20_000), 2)
        self.assertEqual(recommend_level_count(total_qty=10_000, safe_chunk_qty=10), 20)

    def test_live_ownership_normalizes_contract_symbols_and_exchange_alias(self) -> None:
        left = {
            "symbol": "TUT/USDT:USDT",
            "long_exchange": "kukoin",
            "short_exchange": "bybit",
        }
        right = {
            "symbol": "TUTUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "okx",
        }

        self.assertEqual(grid_symbol_ownership_key(left), "TUT")
        self.assertTrue(grid_rules_share_live_ownership(left, right))

    def test_quantity_fit_uses_same_one_percent_completion_tolerance(self) -> None:
        fit = grid_level_count_for_existing_qty(
            total_qty=10_000,
            existing_qty=2_010,
            preferred_count=5,
        )

        self.assertIsNotNone(fit)
        self.assertEqual(fit["level_count"], 5)
        self.assertEqual(fit["level"], 1)
        self.assertAlmostEqual(grid_completion_tolerance({"chunk_qty": 2_000}), 20.0)


if __name__ == "__main__":
    unittest.main()
