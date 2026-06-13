from __future__ import annotations

import asyncio
import unittest

from execution.manual import (
    ManualTradeManager,
    OrderBookStats,
    _cap_qty_to_absolute_target,
    _cap_qty_to_target,
    _choose_chunk_qty,
    _classify_submit_error,
    _is_min_order_size_error,
    _min_qty_required,
    _normalize_submit_values,
    _precision_to_step,
    _round_to_step,
    _symbol_matches,
    estimate_fill,
    max_qty_for_slippage,
    orderbook_stats,
    slippage_bps,
    spread_pct,
    suggest_expensive_leg,
    venue_liquidity_tier,
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

    def test_suggest_expensive_leg_prefers_lower_tier_venue(self) -> None:
        suggestion = suggest_expensive_leg(
            "binance",
            "gate",
            fee_table={
                "binance": {"taker": 0.001},
                "gate": {"taker": 0.0},
            },
            liquidity={"binance": 100000.0, "gate": 100000.0},
        )
        self.assertEqual(suggestion["suggested_leg"], "short")
        self.assertEqual(suggestion["reason"], "lower_venue_tier")
        self.assertEqual(suggestion["venue_tier"], {"long": 1, "short": 3})

    def test_venue_liquidity_tier_defaults(self) -> None:
        self.assertEqual(venue_liquidity_tier("binance"), 1)
        self.assertEqual(venue_liquidity_tier("okx"), 2)
        self.assertEqual(venue_liquidity_tier("gate"), 3)

    def test_resolve_primary_hedge_legs_uses_suggested_leg_for_auto_hint(self) -> None:
        manager = ManualTradeManager()
        label, primary, hedge = manager._resolve_primary_hedge_legs(
            explicit=None,
            plan={"suggested_expensive_leg": {"suggested_leg": "short"}},
            legs=[
                {"label": "long", "exchange": "binance"},
                {"label": "short", "exchange": "gate"},
            ],
        )
        self.assertEqual(label, "short")
        self.assertEqual(primary, {"label": "short", "exchange": "gate"})
        self.assertEqual(hedge, {"label": "long", "exchange": "binance"})

    def test_resolve_primary_hedge_legs_maps_roll_suggestion_to_to_from(self) -> None:
        manager = ManualTradeManager()
        label, primary, hedge = manager._resolve_primary_hedge_legs(
            explicit=None,
            plan={"suggested_expensive_leg": {"suggested_leg": "long"}},
            legs=[
                {"label": "to", "exchange": "okx"},
                {"label": "from", "exchange": "gate"},
            ],
        )
        self.assertEqual(label, "to")
        self.assertEqual(primary, {"label": "to", "exchange": "okx"})
        self.assertEqual(hedge, {"label": "from", "exchange": "gate"})

    def test_auto_exit_market_fallback_allowed_only_on_tier_one_two(self) -> None:
        manager = ManualTradeManager()
        payload = {"auto_exit_agent": True}
        self.assertTrue(manager._auto_exit_market_fallback_allowed(payload, "binance"))
        self.assertTrue(manager._auto_exit_market_fallback_allowed(payload, "okx"))
        self.assertFalse(manager._auto_exit_market_fallback_allowed(payload, "gate"))

    def test_auto_exit_market_fallback_respects_notional_cap(self) -> None:
        manager = ManualTradeManager()
        payload = {
            "auto_exit_agent": True,
            "auto_exit_market_cleanup_notional_max": 2500.0,
        }
        self.assertTrue(
            manager._auto_exit_market_fallback_allowed(payload, "okx", notional_usd=2000.0)
        )
        self.assertFalse(
            manager._auto_exit_market_fallback_allowed(payload, "okx", notional_usd=3000.0)
        )

    def test_auto_exit_dynamic_chunk_keeps_chunk_qty_unset(self) -> None:
        manager = ManualTradeManager()
        updated = manager._apply_auto_exit_exit_overrides(
            {
                "auto_exit_agent": True,
                "auto_exit_dynamic_chunk": True,
                "chunk_notional": 2500.0,
            },
            {
                "action": "exit",
                "qty": 200.0,
                "recommended_chunk_qty": 100.0,
                "min_chunk_qty": 10.0,
                "market_constraints": {},
            },
        )
        self.assertIsNone(updated.get("chunk_qty"))
        self.assertEqual(updated.get("hedge_order_type"), "limit")
        self.assertEqual(updated.get("hedge_limit_mode"), "aggressive")
        self.assertEqual(updated.get("hedge_adverse_bps"), 6.0)
        self.assertEqual(updated.get("hedge_reprice_min_sec"), 2.0)

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

    def test_normalize_submit_values_rounds_limit_price_to_tick(self) -> None:
        qty, price, error = _normalize_submit_values(
            qty=200.0,
            price=0.63539,
            side="sell",
            order_type="limit",
            min_qty=1.0,
            min_notional=5.0,
            amount_step=0.1,
            price_step=0.001,
        )
        self.assertIsNone(error)
        self.assertAlmostEqual(qty or 0.0, 200.0)
        self.assertAlmostEqual(price or 0.0, 0.636)

    def test_classify_submit_error_detects_tradfi_agreement(self) -> None:
        self.assertEqual(
            _classify_submit_error(
                'binanceusdm {"code":-4411,"msg":"Please sign TradFi-Perps agreement contract fapi."}'
            ),
            "tradfi_agreement_required",
        )

    def test_classify_submit_error_detects_price_band(self) -> None:
        self.assertEqual(
            _classify_submit_error('okx {"code":"51006","msg":"Order price is above max price limit."}'),
            "price_band",
        )

    def test_cap_qty_to_target(self) -> None:
        self.assertAlmostEqual(
            _cap_qty_to_target(requested_qty=100.0, target_qty=150.0, leg_delta=120.0, amount_step=None),
            30.0,
        )
        self.assertAlmostEqual(
            _cap_qty_to_target(requested_qty=100.0, target_qty=150.0, leg_delta=160.0, amount_step=None),
            0.0,
        )
        self.assertAlmostEqual(
            _cap_qty_to_target(requested_qty=37.0, target_qty=150.0, leg_delta=120.0, amount_step=10.0),
            30.0,
        )

    def test_cap_qty_to_absolute_target(self) -> None:
        self.assertAlmostEqual(
            _cap_qty_to_absolute_target(requested_qty=100.0, target_qty=150.0, current_qty=120.0, amount_step=None),
            30.0,
        )
        self.assertAlmostEqual(
            _cap_qty_to_absolute_target(requested_qty=100.0, target_qty=150.0, current_qty=160.0, amount_step=None),
            0.0,
        )
        self.assertAlmostEqual(
            _cap_qty_to_absolute_target(requested_qty=37.0, target_qty=150.0, current_qty=120.0, amount_step=10.0),
            30.0,
        )
        self.assertAlmostEqual(
            _cap_qty_to_absolute_target(requested_qty=700.0, target_qty=1400.0, current_qty=700.0, amount_step=None),
            700.0,
        )

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


class _BinanceLeverageClient:
    def __init__(
        self,
        *,
        leverage_before: float | None = None,
        leverage_after: float | None = None,
        set_error: str | None = None,
        margin_mode: str = "isolated",
    ) -> None:
        self.leverage_before = leverage_before
        self.leverage_after = leverage_after if leverage_after is not None else leverage_before
        self.set_error = set_error
        self.margin_mode = margin_mode
        self.fetch_positions_calls = 0
        self.fetch_leverage_calls = 0
        self.set_leverage_calls = 0
        self.set_margin_mode_calls = 0
        self.cancel_all_calls: list[str] = []

    async def fetch_positions(self, symbols=None):  # noqa: D401, ARG002
        self.fetch_positions_calls += 1
        return []

    async def fetch_leverage(self, symbol):  # noqa: D401, ARG002
        self.fetch_leverage_calls += 1
        leverage = self.leverage_before if self.set_leverage_calls <= 0 else self.leverage_after
        if leverage is None:
            return {"symbol": symbol, "marginMode": self.margin_mode}
        return {
            "symbol": symbol,
            "longLeverage": leverage,
            "shortLeverage": leverage,
            "marginMode": self.margin_mode,
        }

    async def set_leverage(self, leverage, symbol, params=None):  # noqa: D401, ARG002
        self.set_leverage_calls += 1
        if self.set_error:
            raise RuntimeError(self.set_error)
        self.leverage_after = float(leverage)
        return {"symbol": symbol, "leverage": leverage}

    async def set_margin_mode(self, margin_mode, symbol):  # noqa: D401, ARG002
        self.set_margin_mode_calls += 1
        self.margin_mode = str(margin_mode)
        return {"symbol": symbol, "marginMode": margin_mode}

    async def cancel_all_orders(self, symbol):  # noqa: D401, ARG002
        self.cancel_all_calls.append(symbol)

    async def fetch_open_orders(self, symbol):  # noqa: D401, ARG002
        return []

    async def request(self, path: str, api: str, method: str, params: dict):  # noqa: ARG002
        if path == "openAlgoOrders" and method.upper() == "GET":
            return []
        raise RuntimeError(f"unsupported request path={path} method={method}")


class _BinanceLeverageManager(ManualTradeManager):
    def __init__(self, client) -> None:
        super().__init__()
        self._client = client

    async def _ensure_client(self, exchange, errors):  # noqa: D401, ARG002
        return self._client

    async def _resolve_market_symbol(self, client, symbol):  # noqa: D401, ARG002
        return "CLO/USDT:USDT"


class ManualTradeBinanceLeveragePrecheckTestCase(unittest.TestCase):
    def test_binance_leverage_skips_set_when_fetch_leverage_already_matches(self) -> None:
        client = _BinanceLeverageClient(leverage_before=3.0)
        manager = _BinanceLeverageManager(client)

        errors = asyncio.run(
            manager._ensure_binance_leverage_for_legs(
                [{"exchange": "binance", "side": "sell", "margin_mode": "isolated", "reduce_only": False}],
                "CLO",
            )
        )

        self.assertEqual(errors, [])
        self.assertEqual(client.set_leverage_calls, 0)
        self.assertGreaterEqual(client.fetch_leverage_calls, 1)

    def test_binance_leverage_error_is_accepted_when_readback_confirms_target(self) -> None:
        client = _BinanceLeverageClient(
            leverage_before=10.0,
            leverage_after=3.0,
            set_error="binanceusdm POST https://fapi.binance.com/fapi/v1/leverage",
        )
        manager = _BinanceLeverageManager(client)

        errors = asyncio.run(
            manager._ensure_binance_leverage_for_legs(
                [{"exchange": "binance", "side": "sell", "margin_mode": "isolated", "reduce_only": False}],
                "CLO",
            )
        )

        self.assertEqual(errors, [])
        self.assertEqual(client.set_leverage_calls, 1)
        self.assertGreaterEqual(client.fetch_leverage_calls, 2)

    def test_binance_leverage_error_blocks_when_readback_still_mismatched(self) -> None:
        client = _BinanceLeverageClient(
            leverage_before=10.0,
            leverage_after=10.0,
            set_error="binanceusdm POST https://fapi.binance.com/fapi/v1/leverage",
        )
        manager = _BinanceLeverageManager(client)

        errors = asyncio.run(
            manager._ensure_binance_leverage_for_legs(
                [{"exchange": "binance", "side": "sell", "margin_mode": "isolated", "reduce_only": False}],
                "CLO",
            )
        )

        self.assertEqual(errors, ["binance: set leverage failed"])
        self.assertEqual(client.set_leverage_calls, 3)


class _HedgeFallbackManager(ManualTradeManager):
    def __init__(self) -> None:
        super().__init__()
        self.cancelled_order_ids: list[str] = []

    async def _snapshot_legs(self, symbol, legs, max_slippage_bps=0.0):  # noqa: D401, ARG002
        exchange = legs[0]["exchange"]
        return {
            "errors": [],
            "stats": {
                exchange: OrderBookStats(
                    best_bid=97.0,
                    best_ask=101.0,
                    spread=4.0,
                    mid=99.0,
                    bid_liquidity_top3=1000.0,
                    ask_liquidity_top3=1000.0,
                    min_liquidity_top3=1000.0,
                )
            },
            "constraints": {exchange: {"price_step": 1.0}},
            "orderbooks": {
                exchange: {
                    "bids": [[97.0, 10.0]],
                    "asks": [[101.0, 10.0]],
                }
            },
        }

    def _ws_orders_live(self, exchange):  # noqa: D401, ARG002
        return True

    def _ws_order_info(self, exchange, order_id):  # noqa: D401, ARG002
        return None

    async def _submit_order(
        self,
        leg,
        symbol,
        qty,
        order_type,
        *,
        price,
        reduce_only,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "submitted",
            "order_id": "L1",
            "filled_qty": 0.0,
            "avg_price": None,
        }

    async def _cancel_order(self, leg, symbol, order_id):  # noqa: D401, ARG002
        self.cancelled_order_ids.append(str(order_id))
        return {"exchange": leg["exchange"], "status": "canceled"}

    async def _fetch_order_status(
        self,
        leg,
        symbol,
        order_id,
        *,
        expected_qty=None,
        allow_trades_fallback=True,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "canceled",
            "filled_qty": 0.0,
            "avg_price": None,
            "source": "rest",
            "order_id": order_id,
        }

    async def _place_market(
        self,
        leg,
        symbol,
        qty,
        payload,
        *,
        reason=None,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "submitted",
            "order_id": "M1",
            "filled_qty": float(qty),
            "avg_price": 99.5,
        }


class _HedgeFallbackSubmittedManager(_HedgeFallbackManager):
    async def _place_market(
        self,
        leg,
        symbol,
        qty,
        payload,
        *,
        reason=None,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "submitted",
            "order_id": "M1",
            "filled_qty": 0.0,
            "avg_price": None,
        }

    async def _await_order_fill(
        self,
        leg,
        symbol,
        order_id,
        expected_qty,
        timeout_sec,
        *,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "filled",
            "order_id": order_id,
            "filled_qty": float(expected_qty),
            "avg_price": 99.5,
            "source": "rest",
        }


class _LimitFirstAutoHintManager(ManualTradeManager):
    async def _place_limit_then_wait(
        self,
        leg,
        symbol,
        qty,
        timeout,
        payload,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "filled",
            "filled_qty": float(qty),
            "avg_price": 100.0,
        }

    async def _place_market(
        self,
        leg,
        symbol,
        qty,
        payload,
        *,
        reason=None,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "filled",
            "filled_qty": float(qty),
            "avg_price": 100.0,
            "reason": reason,
        }


class ManualTradeHedgeFallbackTestCase(unittest.TestCase):
    def test_limit_first_expensive_uses_auto_hint_suggested_leg(self) -> None:
        manager = _LimitFirstAutoHintManager()
        result = asyncio.run(
            manager._execute_plan(
                {
                    "action": "enter",
                    "symbol": "SIREN",
                    "qty": 1.0,
                    "legs": [
                        {"exchange": "binance", "side": "buy", "label": "long", "reduce_only": False},
                        {"exchange": "gate", "side": "sell", "label": "short", "reduce_only": False},
                    ],
                    "suggested_expensive_leg": {"suggested_leg": "short"},
                    "warnings": [],
                },
                mode="limit-first-expensive",
                payload={"expensive_leg": None},
            )
        )

        actions = result.get("actions") or []
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0].get("exchange"), "gate")
        self.assertEqual(actions[1].get("exchange"), "binance")

    def test_hedge_adverse_market_fallback_counts_market_fill(self) -> None:
        manager = _HedgeFallbackManager()
        leg = {
            "exchange": "binance",
            "side": "sell",
            "label": "long",
            "reduce_only": True,
        }

        result = asyncio.run(
            manager._hedge_position(
                leg,
                "TAC",
                10.0,
                hedge_order_type="limit",
                hedge_offset_bps=0.0,
                hedge_offset_ticks=0,
                hedge_limit_mode="passive",
                hedge_favorable_bps=9999.0,
                hedge_adverse_bps=0.1,
                hedge_adverse_ticks=None,
                hedge_reprice_min_sec=0.0,
                payload={},
                min_qty_required=None,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "filled")
        self.assertAlmostEqual(result.get("filled_qty") or 0.0, 10.0)
        self.assertEqual(result.get("order_id"), "M1")
        self.assertEqual(manager.cancelled_order_ids, ["L1"])

    def test_hedge_adverse_market_fallback_resolves_submitted_market_fill(self) -> None:
        manager = _HedgeFallbackSubmittedManager()
        leg = {
            "exchange": "binance",
            "side": "sell",
            "label": "long",
            "reduce_only": True,
        }

        result = asyncio.run(
            manager._hedge_position(
                leg,
                "TAC",
                10.0,
                hedge_order_type="limit",
                hedge_offset_bps=0.0,
                hedge_offset_ticks=0,
                hedge_limit_mode="passive",
                hedge_favorable_bps=9999.0,
                hedge_adverse_bps=0.1,
                hedge_adverse_ticks=None,
                hedge_reprice_min_sec=0.0,
                payload={},
                min_qty_required=None,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "filled")
        self.assertAlmostEqual(result.get("filled_qty") or 0.0, 10.0)
        self.assertEqual(result.get("order_id"), "M1")
        self.assertIsNone(result.get("pending_qty"))


class _BingxLeverageClient:
    def __init__(self, behavior: str) -> None:
        self.behavior = behavior
        self.calls: list[str] = []

    async def fetch_leverage(self, symbol):  # noqa: D401, ARG002
        return {}

    async def set_leverage(self, leverage, symbol, params=None):  # noqa: D401, ARG002
        side = str((params or {}).get("side") or "default")
        self.calls.append(side)
        if self.behavior == "invalid_all":
            raise RuntimeError('bingx {"code":109400,"msg":"Invalid parameters"}')
        if self.behavior == "side_required_all":
            raise RuntimeError("bingx setLeverage() requires a side argument, one of (LONG, SHORT, BOTH)")
        if self.behavior == "hard_error":
            raise RuntimeError("network_down")
        if self.behavior == "fallback_default_ok":
            if side in ("LONG", "SHORT", "BOTH"):
                raise RuntimeError('bingx {"code":109400,"msg":"Invalid parameters"}')
            return {"status": "ok"}
        return {"status": "ok"}


class _BingxLeverageManager(ManualTradeManager):
    def __init__(self, client) -> None:
        super().__init__()
        self._client = client

    async def _ensure_client(self, exchange, errors):  # noqa: D401, ARG002
        return self._client

    async def _resolve_market_symbol(self, client, symbol):  # noqa: D401, ARG002
        return "ENSO/USDT:USDT"


class ManualTradeBingxLeveragePrecheckTestCase(unittest.TestCase):
    def test_bingx_leverage_invalid_params_does_not_block(self) -> None:
        manager = _BingxLeverageManager(_BingxLeverageClient("invalid_all"))
        legs = [{"exchange": "bingx", "side": "buy", "reduce_only": False}]

        errors = asyncio.run(manager._ensure_bingx_leverage_for_legs(legs, "ENSO"))

        self.assertEqual(errors, [])

    def test_bingx_leverage_non_param_error_blocks(self) -> None:
        manager = _BingxLeverageManager(_BingxLeverageClient("hard_error"))
        legs = [{"exchange": "bingx", "side": "buy", "reduce_only": False}]

        errors = asyncio.run(manager._ensure_bingx_leverage_for_legs(legs, "ENSO"))

        self.assertTrue(errors)

    def test_bingx_leverage_default_variant_fallback(self) -> None:
        client = _BingxLeverageClient("fallback_default_ok")
        manager = _BingxLeverageManager(client)
        legs = [{"exchange": "bingx", "side": "buy", "reduce_only": False}]

        errors = asyncio.run(manager._ensure_bingx_leverage_for_legs(legs, "ENSO"))

        self.assertEqual(errors, [])
        self.assertEqual(client.calls, ["LONG", "BOTH", "default"])

    def test_bingx_leverage_side_required_message_does_not_block(self) -> None:
        manager = _BingxLeverageManager(_BingxLeverageClient("side_required_all"))
        legs = [{"exchange": "bingx", "side": "buy", "reduce_only": False}]

        errors = asyncio.run(manager._ensure_bingx_leverage_for_legs(legs, "ENSO"))

        self.assertEqual(errors, [])


class _DustFinalizeManager(ManualTradeManager):
    def __init__(self, *, positions, market_result=None) -> None:
        super().__init__()
        self._positions = list(positions)
        self._market_result = market_result
        self.market_calls: list[dict[str, float | str | None]] = []

    async def _fetch_positions_for_symbol(
        self,
        *,
        exchanges,
        symbol,
        allow_ws=True,
        contract_sizes=None,
    ):  # noqa: D401, ARG002
        return list(self._positions), []

    async def _place_market(
        self,
        leg,
        symbol,
        qty,
        payload,
        *,
        reason=None,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        self.market_calls.append(
            {
                "exchange": str(leg.get("exchange") or ""),
                "symbol": symbol,
                "qty": float(qty),
                "reason": reason,
            }
        )
        if self._market_result is not None:
            return dict(self._market_result)
        return {
            "exchange": leg.get("exchange"),
            "status": "submitted",
            "order_id": f"dust-{leg.get('exchange')}",
            "filled_qty": float(qty),
            "avg_price": 1.0,
        }


class _FastExitForceFinalizeManager(ManualTradeManager):
    def __init__(self, *, force_finalize: bool, end_positions: list[dict[str, object]]) -> None:
        super().__init__()
        self._force_finalize = force_finalize
        self._end_positions = list(end_positions)
        self.market_calls: list[dict[str, object]] = []
        self.finalize_calls = 0
        self._stop_check = lambda: {
            "requested": True,
            "force_finalize": self._force_finalize,
            "reason": "panic_priority",
        }

    async def _ensure_ws_orders(self, exchanges, contract_sizes=None, symbol=None, log_cb=None):  # noqa: D401, ARG002
        return None

    async def _fetch_positions_with_retry(self, exchanges, symbol, log_cb=None):  # noqa: D401, ARG002
        return (
            [
                {"exchange": "binance", "symbol": f"{symbol}/USDT:USDT", "side": "long", "coin_qty": 10.0},
                {"exchange": "okx", "symbol": f"{symbol}/USDT:USDT", "side": "short", "coin_qty": 10.0},
            ],
            [],
        )

    async def _fetch_positions_for_symbol(
        self,
        *,
        exchanges,
        symbol,
        allow_ws=True,
        contract_sizes=None,
    ):  # noqa: D401, ARG002
        return (list(self._end_positions), [])

    async def _place_market(
        self,
        leg,
        symbol,
        qty,
        payload,
        *,
        reason=None,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        self.market_calls.append(
            {
                "exchange": str(leg.get("exchange") or ""),
                "symbol": symbol,
                "qty": float(qty),
                "reason": reason,
            }
        )
        return {
            "exchange": leg.get("exchange"),
            "status": "submitted",
            "order_id": f"force-{leg.get('exchange')}",
            "filled_qty": float(qty),
            "avg_price": 100.0,
        }

    async def _finalize_exit_dust(
        self,
        *,
        symbol,
        legs,
        start_qty_by_exchange,
        requested_exit_qty,
        constraints,
        payload,
        actions,
        warnings,
        log_cb=None,
    ):  # noqa: D401, ARG002
        self.finalize_calls += 1


class _OrphanCleanupManager(ManualTradeManager):
    def __init__(
        self,
        *,
        fetch_responses: list[list[dict[str, object]]],
        rebalance_result: dict[str, object] | None = None,
    ) -> None:
        super().__init__()
        self._fetch_responses = [list(item) for item in fetch_responses]
        self._rebalance_result = dict(rebalance_result or {"status": "filled", "filled_qty": 3.0, "remaining_qty": 0.0})
        self.market_calls: list[dict[str, object]] = []
        self.rebalance_calls: list[dict[str, object]] = []
        self.finalize_calls = 0

    async def _snapshot_legs(self, symbol, legs, max_slippage_bps=0.0):  # noqa: D401, ARG002
        exchange = str((legs or [{}])[0].get("exchange") or "")
        return {
            "errors": [],
            "constraints": {
                exchange: {
                    "amount_step": 0.1,
                    "min_qty_required": 0.1,
                }
            },
            "stats": {},
            "orderbooks": {},
        }

    async def agent_rebalance(
        self,
        *,
        exchange,
        symbol,
        side,
        qty_base,
        margin_mode="isolated",
        limit_timeout_sec=6,
        limit_offset_bps=1.0,
        max_slippage_bps=8.0,
        log_cb=None,
    ):  # noqa: D401, ARG002
        self.rebalance_calls.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "side": side,
                "qty_base": float(qty_base),
                "margin_mode": margin_mode,
            }
        )
        return dict(self._rebalance_result)

    async def _finalize_exit_dust(
        self,
        *,
        symbol,
        legs,
        start_qty_by_exchange,
        requested_exit_qty,
        constraints,
        payload,
        actions,
        warnings,
        log_cb=None,
    ):  # noqa: D401, ARG002
        self.finalize_calls += 1

    async def _fetch_positions_for_symbol(
        self,
        *,
        exchanges,
        symbol,
        allow_ws=True,
        contract_sizes=None,
    ):  # noqa: D401, ARG002
        if self._fetch_responses:
            return (self._fetch_responses.pop(0), [])
        return ([], [])

    async def _place_market(
        self,
        leg,
        symbol,
        qty,
        payload,
        *,
        reason=None,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        self.market_calls.append(
            {
                "exchange": str(leg.get("exchange") or ""),
                "symbol": symbol,
                "qty": float(qty),
                "reason": reason,
            }
        )
        return {
            "exchange": leg.get("exchange"),
            "status": "submitted",
            "order_id": f"orphan-{len(self.market_calls)}",
            "filled_qty": float(qty),
            "avg_price": 100.0,
        }


class _SubmitOrderClient:
    def __init__(
        self,
        *,
        create_error: str | None = None,
        positions: list[dict[str, object]] | None = None,
    ) -> None:
        self.create_error = create_error
        self.positions = list(positions or [])
        self.calls: list[dict[str, object]] = []
        self.markets = {
            "SIREN/USDT:USDT": {
                "precision": {"amount": 1, "price": 3},
                "limits": {"amount": {"min": 1.0}, "cost": {"min": 5.0}},
                "contractSize": 1.0,
                "info": {},
            }
        }

    async def create_order(self, symbol, order_type, side, amount, price=None, params=None):  # noqa: D401
        self.calls.append(
            {
                "symbol": symbol,
                "order_type": order_type,
                "side": side,
                "amount": amount,
                "price": price,
                "params": params,
            }
        )
        if self.create_error:
            raise RuntimeError(self.create_error)
        return {"id": "OID-1", "filled": 0.0, "average": None, "status": "open"}

    async def fetch_positions(self, symbols=None):  # noqa: D401, ARG002
        return list(self.positions)


class _SubmitOrderManager(ManualTradeManager):
    def __init__(self, client) -> None:
        super().__init__()
        self._client = client

    async def _ensure_client(self, exchange, errors):  # noqa: D401, ARG002
        return self._client

    async def _resolve_market_symbol(self, client, symbol):  # noqa: D401, ARG002
        return "SIREN/USDT:USDT"

    async def _ensure_ws_orders_recovered(self, exchange, reason="submit", log_cb=None):  # noqa: D401, ARG002
        return True


class _SmartEnterPartialExposureManager(ManualTradeManager):
    async def _ensure_ws_positions(self, exchanges, contract_sizes=None):  # noqa: D401, ARG002
        return None

    async def _ensure_ws_orders(self, exchanges, contract_sizes=None, symbol=None, log_cb=None):  # noqa: D401, ARG002
        return None

    async def _fetch_positions_with_retry(self, exchanges, symbol, log_cb=None):  # noqa: D401, ARG002
        return [], []

    async def _fetch_positions_for_symbol(
        self,
        *,
        exchanges,
        symbol,
        allow_ws=True,
        contract_sizes=None,
    ):  # noqa: D401, ARG002
        exchange_set = {str(exchange) for exchange in exchanges}
        if exchange_set == {"okx"}:
            return ([], [])
        return (
            [
                {
                    "exchange": "binance",
                    "symbol": f"{symbol}/USDT:USDT",
                    "side": "long",
                    "coin_qty": 1.0,
                    "mark_price": 100.0,
                },
                {
                    "exchange": "okx",
                    "symbol": f"{symbol}/USDT:USDT",
                    "side": "short",
                    "coin_qty": 1.0,
                    "mark_price": 100.0,
                },
            ],
            [],
        )

    async def _snapshot_legs(self, symbol, legs, max_slippage_bps=0.0):  # noqa: D401, ARG002
        stats = {}
        constraints = {}
        orderbooks = {}
        for leg in legs:
            exchange = leg["exchange"]
            stats[exchange] = OrderBookStats(
                best_bid=99.0,
                best_ask=101.0,
                spread=2.0,
                mid=100.0,
                bid_liquidity_top3=1000.0,
                ask_liquidity_top3=1000.0,
                min_liquidity_top3=1000.0,
            )
            constraints[exchange] = {
                "price_step": 0.1,
                "amount_step": 0.1,
                "min_qty_required": 0.1,
            }
            orderbooks[exchange] = {
                "bids": [[99.0, 100.0]],
                "asks": [[101.0, 100.0]],
            }
        return {
            "errors": [],
            "stats": stats,
            "constraints": constraints,
            "orderbooks": orderbooks,
            "spread_pct": 0.0,
            "mid_price": 100.0,
            "orderbook_sources": {leg["exchange"]: "rest" for leg in legs},
        }

    def _ws_orders_live(self, exchange):  # noqa: D401, ARG002
        return False

    async def _submit_order(
        self,
        leg,
        symbol,
        qty,
        order_type,
        *,
        price,
        reduce_only,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "submitted",
            "order_id": "P1",
            "filled_qty": 0.0,
            "avg_price": None,
        }

    async def _wait_for_order_with_spread(
        self,
        leg,
        symbol,
        order_id,
        timeout,
        spread_min_pct,
        spread_max_pct,
        spread_legs,
        reprice_sec,
        *,
        cancel_on_timeout=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "filled",
            "order_id": order_id,
            "filled_qty": 1.0,
            "avg_price": 100.0,
        }

    async def _hedge_position(
        self,
        leg,
        symbol,
        qty,
        *,
        hedge_order_type,
        hedge_offset_bps,
        hedge_offset_ticks,
        hedge_limit_mode,
        hedge_favorable_bps,
        hedge_adverse_bps,
        hedge_adverse_ticks,
        hedge_reprice_min_sec,
        payload,
        min_qty_required=None,
        log_cb=None,
    ):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "error",
            "error": "hedge_submit_rejected",
            "error_type": "tick_size",
            "filled_qty": 0.0,
        }

    async def _cancel_order(self, leg, symbol, order_id):  # noqa: D401, ARG002
        return {"exchange": leg["exchange"], "status": "canceled", "order_id": order_id}


class ManualTradeSubmitOrderValidationTestCase(unittest.TestCase):
    def test_submit_order_rounds_price_to_tick_before_create(self) -> None:
        client = _SubmitOrderClient()
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "binance", "side": "sell"},
                "SIREN",
                200.0,
                "limit",
                price=0.63539,
                reduce_only=False,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "submitted")
        self.assertEqual(len(client.calls), 1)
        self.assertAlmostEqual(float(client.calls[0]["price"]), 0.636)

    def test_submit_order_prefers_binance_price_filter_tick_size(self) -> None:
        client = _SubmitOrderClient()
        client.markets["SIREN/USDT:USDT"]["info"] = {
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.005"},
                {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ]
        }
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "binance", "side": "sell"},
                "SIREN",
                200.0,
                "limit",
                price=0.63539,
                reduce_only=False,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "submitted")
        self.assertEqual(len(client.calls), 1)
        self.assertAlmostEqual(float(client.calls[0]["price"]), 0.64)

    def test_submit_order_blocks_below_min_notional_before_create(self) -> None:
        client = _SubmitOrderClient()
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "binance", "side": "sell"},
                "SIREN",
                6.0,
                "limit",
                price=0.77,
                reduce_only=False,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("error_type"), "min_order_size")
        self.assertEqual(client.calls, [])

    def test_submit_order_classifies_tradfi_error(self) -> None:
        client = _SubmitOrderClient(
            create_error='binanceusdm {"code":-4411,"msg":"Please sign TradFi-Perps agreement contract fapi."}'
        )
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "binance", "side": "sell"},
                "SIREN",
                20.0,
                "market",
                price=None,
                reduce_only=False,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("error_type"), "tradfi_agreement_required")

    def test_submit_order_blocks_static_price_limit_before_create(self) -> None:
        client = _SubmitOrderClient()
        client.markets["SIREN/USDT:USDT"]["limits"]["price"] = {"max": 0.7}
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "okx", "side": "sell"},
                "SIREN",
                20.0,
                "limit",
                price=0.8,
                reduce_only=False,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("error_type"), "price_band")
        self.assertEqual(client.calls, [])

    def test_submit_order_blocks_reduce_only_without_position(self) -> None:
        client = _SubmitOrderClient()
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "binance", "side": "sell"},
                "SIREN",
                20.0,
                "market",
                price=None,
                reduce_only=True,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("error_type"), "reduce_only_required")
        self.assertEqual(client.calls, [])

    def test_submit_order_blocks_reduce_only_above_position_qty(self) -> None:
        client = _SubmitOrderClient(
            positions=[
                {
                    "symbol": "SIREN/USDT:USDT",
                    "side": "long",
                    "contracts": 5.0,
                }
            ]
        )
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "binance", "side": "sell"},
                "SIREN",
                20.0,
                "market",
                price=None,
                reduce_only=True,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("error_type"), "reduce_only_required")
        self.assertIn("exceeds open position qty", str(result.get("error") or ""))
        self.assertEqual(client.calls, [])

    def test_collect_action_errors_marks_partial_fill_exposure(self) -> None:
        manager = ManualTradeManager()
        errors = manager._collect_action_errors(
            [
                {
                    "exchange": "binance",
                    "status": "error",
                    "error": 'binanceusdm {"code":-4014,"msg":"Price not increased by tick size."}',
                    "risk_state": "partial_fill_exposure",
                }
            ]
        )
        self.assertEqual(len(errors), 1)
        self.assertIn("partial_fill_exposure", errors[0])

    def test_smart_enter_returns_partial_fill_risk_flag_when_hedge_fails(self) -> None:
        manager = _SmartEnterPartialExposureManager()
        result = asyncio.run(
            manager._execute_smart_enter(
                {
                    "action": "enter",
                    "symbol": "SIREN",
                    "qty": 1.0,
                    "legs": [
                        {"exchange": "binance", "side": "buy", "label": "long", "reduce_only": False},
                        {"exchange": "okx", "side": "sell", "label": "short", "reduce_only": False},
                    ],
                    "market_constraints": {
                        "binance": {"amount_step": 0.1, "min_qty_required": 0.1, "price_step": 0.1},
                        "okx": {"amount_step": 0.1, "min_qty_required": 0.1, "price_step": 0.1},
                    },
                },
                {"verbose_logs": False},
            )
        )

        self.assertIn("partial_fill_exposure", result.get("risk_flags") or [])
        self.assertTrue(
            any("partial_fill_exposure" in str(err) for err in (result.get("errors") or []))
        )
        hedge_errors = [
            action
            for action in (result.get("actions") or [])
            if action.get("exchange") == "okx" and action.get("status") == "error"
        ]
        self.assertEqual(len(hedge_errors), 1)
        self.assertEqual(hedge_errors[0].get("risk_state"), "partial_fill_exposure")

    def test_smart_enter_uses_auto_hint_suggested_leg_as_primary(self) -> None:
        manager = _SmartEnterPartialExposureManager()
        result = asyncio.run(
            manager._execute_smart_enter(
                {
                    "action": "enter",
                    "symbol": "SIREN",
                    "qty": 1.0,
                    "legs": [
                        {"exchange": "binance", "side": "buy", "label": "long", "reduce_only": False},
                        {"exchange": "okx", "side": "sell", "label": "short", "reduce_only": False},
                    ],
                    "market_constraints": {
                        "binance": {"amount_step": 0.1, "min_qty_required": 0.1, "price_step": 0.1},
                        "okx": {"amount_step": 0.1, "min_qty_required": 0.1, "price_step": 0.1},
                    },
                    "suggested_expensive_leg": {"suggested_leg": "short"},
                },
                {"verbose_logs": False, "expensive_leg": None},
            )
        )

        actions = result.get("actions") or []
        self.assertTrue(actions)
        self.assertEqual(actions[0].get("exchange"), "okx")


class ManualTradeExitDustTestCase(unittest.TestCase):
    def test_is_min_order_size_error_detection(self) -> None:
        self.assertTrue(_is_min_order_size_error("Filter failure: LOT_SIZE"))
        self.assertTrue(_is_min_order_size_error("minimum notional not met"))
        self.assertFalse(_is_min_order_size_error("network timeout"))

    def test_finalize_exit_dust_closes_smallest_leg(self) -> None:
        manager = _DustFinalizeManager(
            positions=[
                {
                    "exchange": "binance",
                    "symbol": "AZTEC/USDT:USDT",
                    "side": "long",
                    "coin_qty": 6.0,
                    "mark_price": 1.0,
                },
                {
                    "exchange": "kucoin",
                    "symbol": "AZTEC/USDT:USDT",
                    "side": "short",
                    "coin_qty": 7.0,
                    "mark_price": 1.0,
                },
            ]
        )
        legs = [
            {"exchange": "binance", "label": "long", "side": "sell", "reduce_only": True},
            {"exchange": "kucoin", "label": "short", "side": "buy", "reduce_only": True},
        ]
        actions: list[dict[str, object]] = []
        warnings: list[str] = []

        asyncio.run(
            manager._finalize_exit_dust(
                symbol="AZTEC",
                legs=legs,
                start_qty_by_exchange={"binance": 100.0, "kucoin": 100.0},
                requested_exit_qty=95.0,
                constraints={},
                payload={"exit_dust_notional_usd": 10.0, "exit_dust_max_legs": 1},
                actions=actions,
                warnings=warnings,
                log_cb=None,
            )
        )

        self.assertEqual(len(manager.market_calls), 1)
        self.assertEqual(manager.market_calls[0]["exchange"], "binance")
        self.assertAlmostEqual(float(manager.market_calls[0]["qty"]), 1.0)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].get("exchange"), "binance")
        self.assertEqual(warnings, [])

    def test_finalize_exit_dust_non_closeable_warns(self) -> None:
        manager = _DustFinalizeManager(
            positions=[
                {
                    "exchange": "binance",
                    "symbol": "AZTEC/USDT:USDT",
                    "side": "long",
                    "coin_qty": 6.0,
                    "mark_price": 1.0,
                }
            ],
            market_result={
                "exchange": "binance",
                "status": "error",
                "error": "Filter failure: LOT_SIZE",
            },
        )
        legs = [{"exchange": "binance", "label": "long", "side": "sell", "reduce_only": True}]
        actions: list[dict[str, object]] = []
        warnings: list[str] = []

        asyncio.run(
            manager._finalize_exit_dust(
                symbol="AZTEC",
                legs=legs,
                start_qty_by_exchange={"binance": 100.0},
                requested_exit_qty=95.0,
                constraints={},
                payload={"exit_dust_notional_usd": 10.0, "exit_dust_max_legs": 1},
                actions=actions,
                warnings=warnings,
                log_cb=None,
            )
        )

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].get("status"), "error")
        self.assertTrue(any("non-closeable dust" in item for item in warnings))

    def test_finalize_exit_dust_skips_non_reduce_legs(self) -> None:
        manager = _DustFinalizeManager(
            positions=[
                {
                    "exchange": "binance",
                    "symbol": "AZTEC/USDT:USDT",
                    "side": "long",
                    "coin_qty": 6.0,
                    "mark_price": 1.0,
                }
            ]
        )
        actions: list[dict[str, object]] = []
        warnings: list[str] = []

        asyncio.run(
            manager._finalize_exit_dust(
                symbol="AZTEC",
                legs=[{"exchange": "binance", "label": "long", "side": "sell", "reduce_only": False}],
                start_qty_by_exchange={"binance": 100.0},
                requested_exit_qty=95.0,
                constraints={},
                payload={"exit_dust_notional_usd": 10.0},
                actions=actions,
                warnings=warnings,
                log_cb=None,
            )
        )

        self.assertEqual(actions, [])
        self.assertEqual(manager.market_calls, [])


class ManualTradeForcedFinalizeTestCase(unittest.TestCase):
    def test_fast_exit_force_finalize_reconciles_lagging_leg_on_stop(self) -> None:
        manager = _FastExitForceFinalizeManager(
            force_finalize=True,
            end_positions=[
                {"exchange": "binance", "symbol": "BTC/USDT:USDT", "side": "long", "coin_qty": 5.0},
                {"exchange": "okx", "symbol": "BTC/USDT:USDT", "side": "short", "coin_qty": 10.0},
            ],
        )

        result = asyncio.run(
            manager._execute_fast_exit(
                {
                    "action": "exit",
                    "symbol": "BTC",
                    "qty": 10.0,
                    "legs": [
                        {"exchange": "binance", "side": "sell", "label": "long", "reduce_only": True},
                        {"exchange": "okx", "side": "buy", "label": "short", "reduce_only": True},
                    ],
                    "market_constraints": {
                        "binance": {"amount_step": 0.1, "min_qty_required": 0.1},
                        "okx": {"amount_step": 0.1, "min_qty_required": 0.1},
                    },
                },
                {},
            )
        )

        self.assertEqual(result.get("mode"), "fast-exit")
        self.assertEqual(len(manager.market_calls), 1)
        self.assertEqual(manager.market_calls[0]["exchange"], "okx")
        self.assertAlmostEqual(float(manager.market_calls[0]["qty"]), 5.0)
        self.assertEqual(manager.market_calls[0]["reason"], "final_reconcile")
        self.assertEqual(manager.finalize_calls, 1)

    def test_fast_exit_plain_stop_skips_force_finalize(self) -> None:
        manager = _FastExitForceFinalizeManager(
            force_finalize=False,
            end_positions=[
                {"exchange": "binance", "symbol": "BTC/USDT:USDT", "side": "long", "coin_qty": 5.0},
                {"exchange": "okx", "symbol": "BTC/USDT:USDT", "side": "short", "coin_qty": 10.0},
            ],
        )

        result = asyncio.run(
            manager._execute_fast_exit(
                {
                    "action": "exit",
                    "symbol": "BTC",
                    "qty": 10.0,
                    "legs": [
                        {"exchange": "binance", "side": "sell", "label": "long", "reduce_only": True},
                        {"exchange": "okx", "side": "buy", "label": "short", "reduce_only": True},
                    ],
                    "market_constraints": {
                        "binance": {"amount_step": 0.1, "min_qty_required": 0.1},
                        "okx": {"amount_step": 0.1, "min_qty_required": 0.1},
                    },
                },
                {},
            )
        )

        self.assertEqual(result.get("mode"), "fast-exit")
        self.assertEqual(manager.market_calls, [])
        self.assertEqual(manager.finalize_calls, 0)


class ManualTradeOrphanCleanupTestCase(unittest.TestCase):
    def test_orphan_cleanup_non_panic_uses_rebalance_path(self) -> None:
        manager = _OrphanCleanupManager(fetch_responses=[[]])

        result = asyncio.run(
            manager.orphan_cleanup(
                {
                    "symbol": "BTCUSDT",
                    "cleanup_exchange": "gate",
                },
                [
                    {
                        "exchange": "gate",
                        "symbol": "BTC/USDT:USDT",
                        "side": "long",
                        "coin_qty": 3.0,
                    }
                ],
            )
        )

        self.assertEqual(result.get("mode"), "orphan-cleanup")
        self.assertEqual(len(manager.rebalance_calls), 1)
        self.assertEqual(manager.rebalance_calls[0]["exchange"], "gate")
        self.assertAlmostEqual(float(manager.rebalance_calls[0]["qty_base"]), 3.0)
        self.assertEqual(manager.market_calls, [])
        self.assertEqual(manager.finalize_calls, 1)
        self.assertEqual(result.get("warnings"), [])

    def test_orphan_cleanup_panic_forces_final_market_when_residual_remains(self) -> None:
        manager = _OrphanCleanupManager(
            fetch_responses=[
                [{"exchange": "gate", "symbol": "BTC/USDT:USDT", "side": "long", "coin_qty": 1.0}],
                [],
            ]
        )

        result = asyncio.run(
            manager.orphan_cleanup(
                {
                    "symbol": "BTCUSDT",
                    "cleanup_exchange": "gate",
                    "panic_cleanup_mode": True,
                },
                [
                    {
                        "exchange": "gate",
                        "symbol": "BTC/USDT:USDT",
                        "side": "long",
                        "coin_qty": 3.0,
                    }
                ],
            )
        )

        self.assertEqual(result.get("mode"), "orphan-cleanup")
        self.assertEqual([call["reason"] for call in manager.market_calls], ["orphan_cleanup_panic", "orphan_cleanup_final"])
        self.assertAlmostEqual(float(manager.market_calls[0]["qty"]), 3.0)
        self.assertAlmostEqual(float(manager.market_calls[1]["qty"]), 1.0)
        self.assertAlmostEqual(float(result.get("remaining_qty") or 0.0), 0.0)
        self.assertNotIn("orphan_cleanup_residual", result.get("risk_flags") or [])

    def test_orphan_cleanup_marks_residual_risk_when_position_still_open(self) -> None:
        manager = _OrphanCleanupManager(
            fetch_responses=[
                [{"exchange": "gate", "symbol": "BTC/USDT:USDT", "side": "long", "coin_qty": 1.0}],
                [{"exchange": "gate", "symbol": "BTC/USDT:USDT", "side": "long", "coin_qty": 0.5}],
            ]
        )

        result = asyncio.run(
            manager.orphan_cleanup(
                {
                    "symbol": "BTCUSDT",
                    "cleanup_exchange": "gate",
                    "panic_cleanup_mode": True,
                },
                [
                    {
                        "exchange": "gate",
                        "symbol": "BTC/USDT:USDT",
                        "side": "long",
                        "coin_qty": 3.0,
                    }
                ],
            )
        )

        self.assertIn("orphan_cleanup_residual", result.get("risk_flags") or [])
        self.assertTrue(any("orphan residual" in item for item in (result.get("warnings") or [])))


if __name__ == "__main__":
    unittest.main()
