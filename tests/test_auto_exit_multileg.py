from __future__ import annotations

import unittest

from webapp.services import (
    _auto_exit_edge_delta_bps,
    _auto_exit_executable_metrics_from_books,
    _auto_exit_execution_order,
    _auto_exit_market_cleanup_status,
    _auto_exit_pair_fee_bps,
    _auto_exit_policy_for_pair,
    _auto_exit_v1_decision,
    _auto_exit_v1_window,
    _auto_exit_overall_spread_from_legs,
    _auto_exit_select_pair_from_legs,
    _auto_exit_spread_trigger_status,
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

    def test_multileg_trigger_uses_overall_basket_not_selected_pair(self) -> None:
        status = _auto_exit_spread_trigger_status(
            is_multileg=True,
            target_pct=-3.5,
            overall_spread_pct=-4.504368604168459,
            pair_spread_pct=-0.5248427315778031,
            pair_net_spread_pct=-0.6748427315778032,
            edge_buffer_pct=0.08,
        )
        self.assertEqual(status["scope"], "overall_basket")
        self.assertAlmostEqual(float(status["trigger_spread_pct"]), -4.504368604168459)
        self.assertAlmostEqual(float(status["required_spread_pct"]), -3.5)
        self.assertTrue(status["live_ready"])
        self.assertFalse(status["target_reached"])

    def test_multileg_trigger_requires_live_selected_pair_books(self) -> None:
        status = _auto_exit_spread_trigger_status(
            is_multileg=True,
            target_pct=-3.5,
            overall_spread_pct=-3.4,
            pair_spread_pct=None,
            pair_net_spread_pct=None,
            edge_buffer_pct=0.08,
        )
        self.assertFalse(status["live_ready"])
        self.assertFalse(status["target_reached"])

    def test_single_pair_trigger_still_uses_net_spread_and_buffer(self) -> None:
        waiting = _auto_exit_spread_trigger_status(
            is_multileg=False,
            target_pct=0.4,
            overall_spread_pct=None,
            pair_spread_pct=0.55,
            pair_net_spread_pct=0.47,
            edge_buffer_pct=0.08,
        )
        ready = _auto_exit_spread_trigger_status(
            is_multileg=False,
            target_pct=0.4,
            overall_spread_pct=None,
            pair_spread_pct=0.57,
            pair_net_spread_pct=0.49,
            edge_buffer_pct=0.08,
        )
        self.assertFalse(waiting["target_reached"])
        self.assertTrue(ready["target_reached"])
        self.assertAlmostEqual(float(ready["required_spread_pct"]), 0.48)

    def test_pair_policy_uses_worst_tier(self) -> None:
        policy = _auto_exit_policy_for_pair("binance", "okx")
        self.assertEqual(policy["worst_tier"], 2)
        self.assertAlmostEqual(float(policy["chunk_notional_cap_usd"]), 500.0)
        self.assertAlmostEqual(float(policy["market_cleanup_notional_cap_usd"]), 800.0)
        self.assertAlmostEqual(float(policy["edge_buffer_bps"]), 4.0)

    def test_pair_policy_uses_manual_settings_override(self) -> None:
        policy = _auto_exit_policy_for_pair(
            "binance",
            "okx",
            manual_settings={
                "auto_exit_policy": {
                    "tier2": {
                        "chunk_notional_cap_usd": 7777.0,
                        "market_cleanup_notional_cap_usd": 555.0,
                        "edge_buffer_bps": 6.5,
                    }
                }
            },
        )
        self.assertEqual(policy["policy_key"], "tier2")
        self.assertAlmostEqual(float(policy["chunk_notional_cap_usd"]), 7777.0)
        self.assertAlmostEqual(float(policy["market_cleanup_notional_cap_usd"]), 555.0)
        self.assertAlmostEqual(float(policy["edge_buffer_bps"]), 6.5)

    def test_executable_metrics_use_exit_sides_and_book_capped_chunk(self) -> None:
        metrics = _auto_exit_executable_metrics_from_books(
            long_exchange="binance",
            short_exchange="gate",
            long_book={
                "bids": [[100.0, 5.0], [99.9, 5.0]],
                "asks": [[100.1, 5.0]],
            },
            short_book={
                "bids": [[97.8, 5.0]],
                "asks": [[98.0, 5.0], [98.1, 5.0]],
            },
            qty=10.0,
            max_slippage_bps=8.0,
            fee_bps=_auto_exit_pair_fee_bps("binance", "gate"),
            edge_buffer_bps=8.0,
            chunk_notional_cap_usd=500.0,
        )
        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertAlmostEqual(float(metrics["liquidity_cap_qty"]), 10.0, places=6)
        self.assertAlmostEqual(float(metrics["chunk_qty"]), 5.0, places=6)
        self.assertAlmostEqual(float(metrics["chunk_notional_usd"]), 500.0, places=6)
        self.assertAlmostEqual(float(metrics["avg_sell_long"]), 100.0, places=6)
        self.assertAlmostEqual(float(metrics["avg_buy_short"]), 98.0, places=6)
        self.assertAlmostEqual(float(metrics["spread_pct"]), 2.0, places=6)
        self.assertAlmostEqual(float(metrics["net_spread_pct"]), 1.91, places=6)
        self.assertIsNone(metrics["safety_factor"])

    def test_market_cleanup_status_blocks_lower_tier_venue(self) -> None:
        status = _auto_exit_market_cleanup_status(
            long_exchange="binance",
            short_exchange="gate",
            cleanup_cap_usd=2500.0,
            estimated_notional_usd=1000.0,
        )
        self.assertFalse(status["allowed"])
        self.assertIn("BINANCE:allow", status["summary"])
        self.assertIn("GATE:block:tier_blocked", status["summary"])

    def test_market_cleanup_status_blocks_by_notional_cap(self) -> None:
        status = _auto_exit_market_cleanup_status(
            long_exchange="okx",
            short_exchange="binance",
            cleanup_cap_usd=2500.0,
            estimated_notional_usd=3000.0,
        )
        self.assertFalse(status["allowed"])
        self.assertIn("OKX:block:notional_cap", status["summary"])
        self.assertIn("BINANCE:block:notional_cap", status["summary"])

    def test_execution_order_prefers_lower_tier_as_primary(self) -> None:
        order = _auto_exit_execution_order(
            long_exchange="binance",
            short_exchange="gate",
            long_book={"bids": [[100.0, 10.0], [99.9, 10.0], [99.8, 10.0]]},
            short_book={"asks": [[98.0, 5.0], [98.1, 5.0], [98.2, 5.0]]},
        )
        self.assertEqual(order["primary_label"], "short")
        self.assertEqual(order["primary_exchange"], "gate")
        self.assertEqual(order["hedge_label"], "long")
        self.assertEqual(order["hedge_exchange"], "binance")
        self.assertEqual(order["reason"], "lower_venue_tier")

    def test_edge_delta_bps_positive_and_negative(self) -> None:
        self.assertAlmostEqual(_auto_exit_edge_delta_bps(1.95, 1.91), 4.0)
        self.assertAlmostEqual(_auto_exit_edge_delta_bps(1.88, 1.91), -3.0)
        self.assertIsNone(_auto_exit_edge_delta_bps(None, 1.91))

    def test_v1_window_uses_short_interval_decision_bucket(self) -> None:
        window = _auto_exit_v1_window(60.0, 8.0)
        self.assertEqual(window["bucket"], "1h")
        self.assertEqual(window["stage"], "decision")
        self.assertAlmostEqual(float(window["take_profit_k"]), 4.0)
        self.assertAlmostEqual(float(window["hard_exit_negative_funding_bps"]), -2.0)

    def test_v1_decision_exits_on_negative_funding_inside_decision_window(self) -> None:
        window = _auto_exit_v1_window(60.0, 8.0)
        decision = _auto_exit_v1_decision(
            close_now_bps=1.0,
            funding_to_next_bps=-3.0,
            reversion_credit_bps=0.0,
            window=window,
        )
        self.assertEqual(decision["decision"], "exit")
        self.assertEqual(decision["reason"], "negative_funding_decision_window")

    def test_v1_take_profit_requires_at_least_40_bps(self) -> None:
        window = _auto_exit_v1_window(240.0, 200.0)
        decision = _auto_exit_v1_decision(
            close_now_bps=30.0,
            funding_to_next_bps=5.0,
            reversion_credit_bps=0.0,
            window=window,
        )
        self.assertEqual(decision["decision"], "hold")
        self.assertAlmostEqual(float(decision["take_profit_threshold_bps"]), 40.0)

    def test_v1_take_profit_uses_4x_funding_when_above_floor(self) -> None:
        window = _auto_exit_v1_window(240.0, 200.0)
        decision = _auto_exit_v1_decision(
            close_now_bps=85.0,
            funding_to_next_bps=20.5,
            reversion_credit_bps=0.0,
            window=window,
        )
        self.assertEqual(decision["decision"], "exit")
        self.assertEqual(decision["reason"], "take_profit_multiple")
        self.assertAlmostEqual(float(decision["take_profit_threshold_bps"]), 82.0)


if __name__ == "__main__":
    unittest.main()
