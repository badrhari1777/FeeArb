from __future__ import annotations

import asyncio
import unittest

from execution.manual import (
    ManualTradeManager,
    _choose_chunk_qty,
    _min_qty_required,
    _precision_to_step,
    _round_to_step,
    _symbol_matches,
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

    def test_symbol_matches_settle_and_swap_variants(self) -> None:
        self.assertTrue(_symbol_matches("FLOWUSDT", "FLOWUSDTUSDT"))
        self.assertTrue(_symbol_matches("FLOWUSDT", "FLOW-USDT-SWAP"))
        self.assertFalse(_symbol_matches("FLOWUSDT", "FLOWUSDC"))

    def test_sum_position_qty_normalizes_side_aliases(self) -> None:
        manager = ManualTradeManager()
        positions = [
            {"exchange": "gate", "symbol": "FLOW/USDT:USDT", "side": "buy", "coin_qty": 17090.0},
            {"exchange": "okx", "symbol": "FLOW-USDT-SWAP", "side": "sell", "coin_qty": 17500.0},
            {"exchange": "okx", "symbol": "FLOW-USDT-SWAP", "side": "net", "coin_qty": -10.0},
        ]
        self.assertAlmostEqual(
            manager._sum_position_qty(
                positions,
                exchange="gate",
                side="long",
                symbol="FLOWUSDT",
            ),
            17090.0,
        )
        self.assertAlmostEqual(
            manager._sum_position_qty(
                positions,
                exchange="okx",
                side="short",
                symbol="FLOWUSDT",
            ),
            17510.0,
        )


class _FakeBinanceClient:
    def __init__(self, *, algo_ids: list[str], fail_algo_ids: set[str] | None = None) -> None:
        self._algo_orders = [{"algoId": algo_id, "symbol": "ZKPUSDT"} for algo_id in algo_ids]
        self._fail_algo_ids = set(fail_algo_ids or set())
        self.cancel_all_calls: list[str] = []
        self.cancel_order_calls: list[str] = []

    async def cancel_all_orders(self, symbol: str) -> None:
        self.cancel_all_calls.append(symbol)

    async def fetch_open_orders(self, symbol: str):  # noqa: ARG002
        return []

    async def cancel_order(self, order_id: str, symbol: str) -> None:  # noqa: ARG002
        self.cancel_order_calls.append(str(order_id))

    async def request(self, path: str, api: str, method: str, params: dict):  # noqa: ARG002
        if path == "openAlgoOrders" and method.upper() == "GET":
            symbol = str(params.get("symbol") or "")
            return [item for item in self._algo_orders if str(item.get("symbol") or "") == symbol]
        if path == "algoOrder" and method.upper() == "DELETE":
            algo_id = str(params.get("algoId") or "")
            if algo_id in self._fail_algo_ids:
                raise RuntimeError("delete failed")
            self._algo_orders = [item for item in self._algo_orders if str(item.get("algoId") or "") != algo_id]
            return {"algoId": algo_id}
        raise RuntimeError(f"unsupported request path={path} method={method}")


class ManualTradeBinanceCancelTestCase(unittest.TestCase):
    def test_cancel_open_orders_also_cancels_binance_algo_orders(self) -> None:
        manager = ManualTradeManager()
        client = _FakeBinanceClient(algo_ids=["a1", "a2"])

        ok = asyncio.run(
            manager._cancel_open_orders_for_symbol(
                client,
                exchange="binance",
                symbol="ZKP",
                ccxt_symbol="ZKPUSDT",
            )
        )

        self.assertTrue(ok)
        self.assertEqual(client.cancel_all_calls, ["ZKPUSDT"])
        remaining = asyncio.run(client.request("openAlgoOrders", "fapiPrivate", "GET", {"symbol": "ZKPUSDT"}))
        self.assertEqual(remaining, [])

    def test_cancel_open_orders_returns_false_when_algo_cancel_fails(self) -> None:
        manager = ManualTradeManager()
        client = _FakeBinanceClient(algo_ids=["a1"], fail_algo_ids={"a1"})

        ok = asyncio.run(
            manager._cancel_open_orders_for_symbol(
                client,
                exchange="binance",
                symbol="ZKP",
                ccxt_symbol="ZKPUSDT",
            )
        )

        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
