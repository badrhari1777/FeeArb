from __future__ import annotations

import unittest

from webapp.services import (
    _auto_exit_overall_spread_from_legs,
    _auto_exit_select_pair_from_legs,
    _is_auto_exit_multileg_rule,
)


class AutoExitMultilegSelectTestCase(unittest.TestCase):
    def test_single_pair_selection(self) -> None:
        selected = _auto_exit_select_pair_from_legs(
            [
                {"exchange": "binance", "side": "long", "quantity": 120.0},
                {"exchange": "kucoin", "side": "short", "quantity": -100.0},
            ]
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["mode"], "single_pair")
        self.assertEqual(selected["long_exchange"], "binance")
        self.assertEqual(selected["short_exchange"], "kucoin")
        self.assertAlmostEqual(float(selected["qty"]), 100.0)

    def test_multileg_selects_smallest_leg_then_opposite_largest(self) -> None:
        selected = _auto_exit_select_pair_from_legs(
            [
                {"exchange": "binance", "side": "long", "quantity": 50.0},
                {"exchange": "okx", "side": "long", "quantity": 120.0},
                {"exchange": "kucoin", "side": "short", "quantity": -200.0},
                {"exchange": "gate", "side": "short", "quantity": -70.0},
            ]
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["mode"], "multileg_min_leg")
        self.assertEqual(selected["selected_min_side"], "long")
        self.assertEqual(selected["selected_min_exchange"], "binance")
        self.assertEqual(selected["long_exchange"], "binance")
        self.assertEqual(selected["short_exchange"], "kucoin")
        self.assertAlmostEqual(float(selected["qty"]), 50.0)

    def test_multileg_smallest_short_selected(self) -> None:
        selected = _auto_exit_select_pair_from_legs(
            [
                {"exchange": "binance", "side": "long", "quantity": 100.0},
                {"exchange": "okx", "side": "long", "quantity": 80.0},
                {"exchange": "kucoin", "side": "short", "quantity": -90.0},
                {"exchange": "gate", "side": "short", "quantity": -30.0},
            ]
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["mode"], "multileg_min_leg")
        self.assertEqual(selected["selected_min_side"], "short")
        self.assertEqual(selected["selected_min_exchange"], "gate")
        self.assertEqual(selected["short_exchange"], "gate")
        self.assertEqual(selected["long_exchange"], "binance")
        self.assertAlmostEqual(float(selected["qty"]), 30.0)

    def test_returns_none_without_two_sides(self) -> None:
        self.assertIsNone(
            _auto_exit_select_pair_from_legs(
                [
                    {"exchange": "binance", "side": "long", "quantity": 100.0},
                    {"exchange": "okx", "side": "long", "quantity": 80.0},
                ]
            )
        )

    def test_ignores_zero_qty_legs(self) -> None:
        selected = _auto_exit_select_pair_from_legs(
            [
                {"exchange": "binance", "side": "long", "quantity": 0.0},
                {"exchange": "okx", "side": "long", "quantity": 10.0},
                {"exchange": "kucoin", "side": "short", "quantity": -20.0},
            ]
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["mode"], "single_pair")
        self.assertEqual(selected["long_exchange"], "okx")
        self.assertEqual(selected["short_exchange"], "kucoin")
        self.assertAlmostEqual(float(selected["qty"]), 10.0)

    def test_multileg_rule_marker_detection(self) -> None:
        self.assertTrue(_is_auto_exit_multileg_rule("multileg", "multileg"))
        self.assertTrue(_is_auto_exit_multileg_rule("MULTILEG", "multileg"))
        self.assertFalse(_is_auto_exit_multileg_rule("binance", "multileg"))

    def test_overall_spread_uses_live_mid_by_exchange(self) -> None:
        spread = _auto_exit_overall_spread_from_legs(
            [
                {"exchange": "binance", "side": "long", "quantity": 100.0, "mark_price": 1.0},
                {"exchange": "okx", "side": "short", "quantity": -30.0, "mark_price": 1.0},
                {"exchange": "kucoin", "side": "short", "quantity": -70.0, "mark_price": 1.0},
            ],
            live_mid_by_exchange={
                "binance": 1.40,
                "okx": 1.36,
                "kucoin": 1.35,
            },
        )
        self.assertIsNotNone(spread)
        assert spread is not None
        # short_avg = (30*1.36 + 70*1.35) / 100 = 1.353
        # spread = (1.40 - 1.353) / 1.40 * 100 = 3.3571%
        self.assertAlmostEqual(spread, 3.3571428571, places=6)

    def test_overall_spread_falls_back_to_mark_prices(self) -> None:
        spread = _auto_exit_overall_spread_from_legs(
            [
                {"exchange": "binance", "side": "long", "quantity": 100.0, "mark_price": 1.50},
                {"exchange": "okx", "side": "short", "quantity": -100.0, "mark_price": 1.47},
            ],
            live_mid_by_exchange={},
        )
        self.assertIsNotNone(spread)
        assert spread is not None
        self.assertAlmostEqual(spread, 2.0, places=6)


if __name__ == "__main__":
    unittest.main()
