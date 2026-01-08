from __future__ import annotations

import unittest

from execution.manual import (
    _choose_chunk_qty,
    _min_qty_required,
    _precision_to_step,
    _round_to_step,
    estimate_fill,
    max_qty_for_slippage,
    orderbook_stats,
    slippage_bps,
    spread_pct,
    suggest_expensive_leg,
)


class ManualTradeHelpersTestCase(unittest.TestCase):
    def test_estimate_fill_buy(self) -> None:
        levels = [(100.0, 1.0), (101.0, 1.0)]
        result = estimate_fill(levels, 1.5)
        expected_avg = (100.0 * 1.0 + 101.0 * 0.5) / 1.5
        self.assertAlmostEqual(result["filled_qty"], 1.5)
        self.assertAlmostEqual(result["avg_price"], expected_avg)
        self.assertAlmostEqual(result["remaining_qty"], 0.0)

    def test_slippage_bps(self) -> None:
        slip_buy = slippage_bps(100.0, 100.5, "buy")
        slip_sell = slippage_bps(100.0, 99.5, "sell")
        self.assertAlmostEqual(slip_buy, 50.0)
        self.assertAlmostEqual(slip_sell, 50.0)

    def test_orderbook_stats(self) -> None:
        book = {
            "bids": [[99.0, 2.0], [98.5, 3.0], [98.0, 4.0]],
            "asks": [[101.0, 1.0], [101.5, 2.0], [102.0, 3.0]],
        }
        stats = orderbook_stats(book, top_n=2)
        self.assertAlmostEqual(stats.best_bid, 99.0)
        self.assertAlmostEqual(stats.best_ask, 101.0)
        self.assertAlmostEqual(stats.spread, 2.0)
        self.assertAlmostEqual(stats.bid_liquidity_top3, (99.0 * 2.0 + 98.5 * 3.0))

    def test_suggest_expensive_leg_prefers_fee(self) -> None:
        suggestion = suggest_expensive_leg(
            "bybit",
            "okx",
            fee_table={
                "bybit": {"taker": 0.001},
                "okx": {"taker": 0.0002},
            },
            liquidity={"bybit": 10000.0, "okx": 10000.0},
        )
        self.assertEqual(suggestion["suggested_leg"], "long")

    def test_spread_pct(self) -> None:
        self.assertAlmostEqual(spread_pct(100.0, 101.0), -1.0)

    def test_max_qty_for_slippage(self) -> None:
        levels = [(100.0, 1.0), (101.0, 1.0)]
        max_qty = max_qty_for_slippage(levels, side="buy", max_bps=50.0)
        self.assertIsNotNone(max_qty)

    def test_precision_to_step(self) -> None:
        self.assertAlmostEqual(_precision_to_step(3), 0.001)
        self.assertAlmostEqual(_precision_to_step(0.01), 0.01)
        self.assertIsNone(_precision_to_step(None))

    def test_round_to_step(self) -> None:
        self.assertAlmostEqual(_round_to_step(1.234, 0.1, mode="down"), 1.2)
        self.assertAlmostEqual(_round_to_step(1.234, 0.1, mode="up"), 1.3)

    def test_min_qty_required(self) -> None:
        required = _min_qty_required(min_qty=0.5, min_notional=10.0, price=4.0, amount_step=0.1)
        self.assertAlmostEqual(required, 2.5)

    def test_choose_chunk_qty_below_min(self) -> None:
        chunk, warnings = _choose_chunk_qty(
            remaining=0.5,
            requested_qty=None,
            min_chunk=1.0,
            max_chunk=2.0,
            amount_step=0.1,
        )
        self.assertIsNone(chunk)
        self.assertTrue(warnings)


if __name__ == "__main__":
    unittest.main()
