from __future__ import annotations

import unittest

from webapp.services import _auto_exit_select_pair_from_legs


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


if __name__ == "__main__":
    unittest.main()
