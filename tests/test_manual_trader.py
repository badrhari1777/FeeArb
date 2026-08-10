from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock

from execution.manual import (
    ManualTradeManager,
    OrderBookStats,
    _cap_auto_chunk_by_notional,
    _cap_qty_to_absolute_target,
    _cap_qty_to_target,
    _choose_chunk_qty,
    _classify_submit_error,
    _is_min_order_size_error,
    _min_qty_required,
    _normalize_submit_values,
    _pending_hedge_order_qty,
    _position_delta_for_leg,
    _precision_to_step,
    _round_to_step,
    _symbol_matches,
    _trigger_wait_sec,
    estimate_fill,
    max_qty_for_slippage,
    orderbook_stats,
    slippage_bps,
    spread_pct,
    suggest_expensive_leg,
    venue_liquidity_tier,
)


class ManualTradeHelpersTestCase(unittest.TestCase):
    @staticmethod
    def _configured_plan_manager() -> ManualTradeManager:
        manager = ManualTradeManager()
        manager._ensure_client = AsyncMock(return_value=object())
        manager._resolve_market_symbol = AsyncMock(return_value="TEST/USDT:USDT")
        manager._fetch_orderbook = AsyncMock(
            return_value={
                "bids": [[99.0, 100.0]],
                "asks": [[101.0, 100.0]],
                "source": "test",
            }
        )
        manager._extract_market_constraints = lambda *_args, **_kwargs: {
            "min_qty": 0.001,
            "min_notional": None,
            "amount_step": 0.001,
            "price_step": 0.01,
            "contract_size": 1.0,
        }
        manager._fetch_funding_meta = AsyncMock(return_value={})
        return manager

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

    def test_position_delta_treats_missing_quantities_as_zero(self) -> None:
        self.assertEqual(_position_delta_for_leg(None, 5.0, {"reduce_only": False}), 5.0)
        self.assertEqual(_position_delta_for_leg(None, 5.0, {"reduce_only": True}), 0.0)
        self.assertEqual(_position_delta_for_leg(5.0, None, {"reduce_only": True}), 5.0)

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

    def test_auto_exit_final_reconcile_guard_allows_existing_fill_exposure(self) -> None:
        manager = ManualTradeManager()
        payload = {"auto_exit_agent": True}

        self.assertTrue(
            manager._auto_exit_final_reconcile_blocked(
                payload,
                "gate",
                primary_delta=0.0,
                hedge_delta=0.0,
                primary_filled_total=0.0,
                hedge_filled_total=0.0,
            )
        )
        self.assertFalse(
            manager._auto_exit_final_reconcile_blocked(
                payload,
                "gate",
                primary_delta=100.0,
                hedge_delta=0.0,
                primary_filled_total=100.0,
                hedge_filled_total=0.0,
            )
        )

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

    def test_pending_hedge_qty_uses_minimum_as_floor_not_lot_multiple(self) -> None:
        qty = _pending_hedge_order_qty(
            137.0,
            min_qty_required=99.8,
            amount_step=0.1,
        )
        self.assertAlmostEqual(qty, 137.0)

    def test_pending_hedge_qty_rejects_below_minimum_after_step_rounding(self) -> None:
        qty = _pending_hedge_order_qty(
            99.74,
            min_qty_required=99.8,
            amount_step=0.1,
        )
        self.assertEqual(qty, 0.0)

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

    def test_choose_chunk_qty_caps_requested_target_to_live_liquidity(self) -> None:
        chunk, warnings = _choose_chunk_qty(
            remaining=5000.0,
            requested_qty=5000.0,
            min_chunk=10.0,
            max_chunk=150.0,
            amount_step=1.0,
        )
        self.assertEqual(chunk, 150.0)
        self.assertTrue(any("slippage cap" in warning for warning in warnings))

    def test_auto_chunk_notional_cap_limits_lower_tier_pair_without_explicit_chunk(self) -> None:
        capped, cap_notional = _cap_auto_chunk_by_notional(
            requested_qty=None,
            chunk_notional=None,
            max_chunk=130_000.0,
            mid_price=0.028,
            legs=[{"exchange": "binance"}, {"exchange": "kucoin"}],
        )

        self.assertEqual(cap_notional, 250.0)
        self.assertAlmostEqual(capped or 0.0, 250.0 / 0.028)

    def test_auto_chunk_notional_cap_does_not_override_explicit_chunk(self) -> None:
        capped, cap_notional = _cap_auto_chunk_by_notional(
            requested_qty=50_000.0,
            chunk_notional=None,
            max_chunk=130_000.0,
            mid_price=0.028,
            legs=[{"exchange": "binance"}, {"exchange": "kucoin"}],
        )

        self.assertEqual(capped, 130_000.0)
        self.assertIsNone(cap_notional)

    def test_trigger_wait_is_bounded_by_runtime_and_server_default(self) -> None:
        self.assertEqual(_trigger_wait_sec({}, 180), 30.0)
        self.assertEqual(_trigger_wait_sec({"trigger_wait_sec": 60}, 20), 20.0)

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
        self.assertTrue(_symbol_matches("H", "H/USDT:USDT"))
        self.assertFalse(_symbol_matches("H", "HOME/USDT:USDT"))

    def test_build_plan_rejects_reversed_spread_range_before_market_access(self) -> None:
        manager = ManualTradeManager()
        result = asyncio.run(
            manager._build_plan(
                {
                    "symbol": "H",
                    "qty": 1000.0,
                    "long_exchange": "kucoin",
                    "short_exchange": "bybit",
                    "spread_min_pct": -10.0,
                    "spread_max_pct": -100.0,
                    "dry_run": True,
                },
                action="exit",
            )
        )
        self.assertIn(
            "spread_min_pct must be less than or equal to spread_max_pct.",
            result.get("errors") or [],
        )

    def test_notional_qty_uses_highest_current_price_across_both_exchanges(self) -> None:
        class FakeClient:
            def __init__(self, ticker):
                self.ticker = ticker

            async def fetch_ticker(self, _symbol):
                return self.ticker

        manager = ManualTradeManager()
        clients = {
            "binance": FakeClient({"bid": 99.0, "ask": 101.0, "last": 100.0}),
            "kucoin": FakeClient({"bid": 101.5, "ask": 102.0, "last": 102.5}),
        }
        manager._ensure_client = AsyncMock(
            side_effect=lambda exchange, *_args: clients[exchange]
        )
        manager._resolve_market_symbol = AsyncMock(return_value="TEST/USDT:USDT")

        qty, reference, prices, missing = asyncio.run(
            manager._resolve_qty_from_notional(
                "TESTUSDT",
                1000.0,
                "binance",
                "kucoin",
            )
        )

        self.assertAlmostEqual(qty or 0.0, 1000.0 / 102.5)
        self.assertEqual(reference, 102.5)
        self.assertEqual(prices, {"binance": 101.0, "kucoin": 102.5})
        self.assertEqual(missing, [])

    def test_build_plan_uses_notional_when_it_is_the_smaller_cap(self) -> None:
        manager = self._configured_plan_manager()
        manager._resolve_qty_from_notional = AsyncMock(
            return_value=(8.0, 125.0, {"binance": 120.0, "kucoin": 125.0}, [])
        )

        result = asyncio.run(
            manager._build_plan(
                {
                    "symbol": "TESTUSDT",
                    "qty": 10.0,
                    "notional": 1000.0,
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "dry_run": True,
                },
                action="enter",
            )
        )

        self.assertEqual(result.get("errors"), [])
        self.assertEqual(result["qty"], 8.0)
        self.assertEqual(result["sizing"]["selected_by"], "notional")
        self.assertEqual(result["sizing"]["requested_qty"], 10.0)
        self.assertTrue(any("USDT amount is the smaller cap" in item for item in result["warnings"]))

    def test_build_plan_uses_quantity_when_it_is_the_smaller_cap(self) -> None:
        manager = self._configured_plan_manager()
        manager._resolve_qty_from_notional = AsyncMock(
            return_value=(12.0, 100.0, {"binance": 99.0, "kucoin": 100.0}, [])
        )

        result = asyncio.run(
            manager._build_plan(
                {
                    "symbol": "TESTUSDT",
                    "qty": 10.0,
                    "notional": 1200.0,
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "dry_run": True,
                },
                action="enter",
            )
        )

        self.assertEqual(result.get("errors"), [])
        self.assertEqual(result["qty"], 10.0)
        self.assertEqual(result["sizing"]["selected_by"], "qty")
        self.assertTrue(any("quantity is the smaller cap" in item for item in result["warnings"]))

    def test_build_plan_fails_closed_when_one_notional_price_is_missing(self) -> None:
        manager = self._configured_plan_manager()
        manager._resolve_qty_from_notional = AsyncMock(
            return_value=(None, None, {"binance": 100.0}, ["kucoin"])
        )

        result = asyncio.run(
            manager._build_plan(
                {
                    "symbol": "TESTUSDT",
                    "qty": 10.0,
                    "notional": 1000.0,
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "dry_run": True,
                },
                action="enter",
            )
        )

        self.assertTrue(result.get("errors"))
        self.assertIn("Missing: kucoin", result["errors"][0])
        manager._fetch_orderbook.assert_not_awaited()

    def test_build_exit_plan_uses_position_margin_mode_when_payload_unset(self) -> None:
        manager = ManualTradeManager()
        result = asyncio.run(
            manager._build_plan(
                {
                    "symbol": "SLX",
                    "qty": 1000.0,
                    "long_exchange": "kucoin",
                    "short_exchange": "binance",
                    "spread_min_pct": 1.0,
                    "spread_max_pct": -1.0,
                    "dry_run": True,
                },
                action="exit",
                positions=[
                    {
                        "exchange": "kucoin",
                        "symbol": "SLX/USDT:USDT",
                        "side": "long",
                        "coin_qty": 1000.0,
                        "marginMode": "CROSS",
                    },
                    {
                        "exchange": "binance",
                        "symbol": "SLX/USDT:USDT",
                        "side": "short",
                        "coin_qty": 1000.0,
                        "marginMode": "ISOLATED",
                    },
                ],
            )
        )

        legs = {leg["exchange"]: leg for leg in result.get("legs") or []}
        self.assertEqual(legs["kucoin"].get("margin_mode"), "cross")
        self.assertEqual(legs["binance"].get("margin_mode"), "isolated")

    def test_gate_resolve_market_symbol_falls_back_to_single_contract(self) -> None:
        class FakeGateClient:
            id = "gate"
            markets = None
            markets_by_id = None
            symbols = None

            async def load_markets(self):
                raise TimeoutError("gate markets timeout")

            async def publicFuturesGetSettleContractsContract(self, params):
                self.contract_params = params
                return {
                    "name": "H_USDT",
                    "status": "trading",
                    "in_delisting": False,
                    "quanto_multiplier": "10",
                    "order_size_min": 1,
                    "order_size_max": 300000,
                    "order_price_round": "0.00001",
                    "leverage_min": "1",
                    "leverage_max": "10",
                }

        manager = ManualTradeManager()
        client = FakeGateClient()

        resolved = asyncio.run(manager._resolve_market_symbol(client, "H"))

        self.assertEqual(resolved, "H/USDT:USDT")
        self.assertEqual(client.contract_params, {"settle": "usdt", "contract": "H_USDT"})
        self.assertIn("H/USDT:USDT", client.markets)
        self.assertEqual(client.markets["H/USDT:USDT"]["contractSize"], 10.0)
        self.assertEqual(client.markets_by_id["H_USDT"][0]["symbol"], "H/USDT:USDT")

    def test_gate_decimal_contract_fallback_preserves_fractional_contract_step(self) -> None:
        class FakeGateClient:
            id = "gate"
            precisionMode = 4
            markets = None
            markets_by_id = None
            symbols = None

            async def load_markets(self):
                raise TimeoutError("gate markets timeout")

            async def fetch(self, url, method="GET", headers=None, body=None):  # noqa: D401, ARG002
                self.fetch_call = {"url": url, "headers": headers}
                return {
                    "name": "LAB_USDT",
                    "status": "trading",
                    "in_delisting": False,
                    "enable_decimal": True,
                    "quanto_multiplier": "100",
                    "order_size_min": "0.1",
                    "order_size_max": "1200",
                    "order_price_round": "0.00001",
                    "leverage_min": "1",
                    "leverage_max": "20",
                }

        manager = ManualTradeManager()
        client = FakeGateClient()

        resolved = asyncio.run(manager._resolve_market_symbol(client, "LAB"))
        constraints = manager._extract_market_constraints(client, resolved or "")

        self.assertEqual(resolved, "LAB/USDT:USDT")
        self.assertEqual(client.fetch_call["headers"], {"X-Gate-Size-Decimal": "1"})
        self.assertEqual(client.markets["LAB/USDT:USDT"]["precision"]["amount"], 0.1)
        self.assertAlmostEqual(constraints["min_qty"] or 0.0, 10.0)
        self.assertAlmostEqual(constraints["amount_step"] or 0.0, 10.0)

    def test_contract_market_zero_min_qty_defaults_to_one_contract(self) -> None:
        class FakeGateClient:
            id = "gate"
            precisionMode = 4
            markets = {
                "LAB/USDT:USDT": {
                    "id": "LAB_USDT",
                    "symbol": "LAB/USDT:USDT",
                    "type": "swap",
                    "swap": True,
                    "contract": True,
                    "contractSize": 100.0,
                    "precision": {"amount": 0.1, "price": 0.00001},
                    "limits": {
                        "amount": {"min": 0.0, "max": 1200.0},
                        "price": {"min": 0.00001, "max": None},
                        "cost": {"min": None, "max": None},
                    },
                    "info": {},
                }
            }

        constraints = ManualTradeManager()._extract_market_constraints(FakeGateClient(), "LAB/USDT:USDT")

        self.assertAlmostEqual(constraints["min_qty"] or 0.0, 100.0)
        self.assertAlmostEqual(constraints["amount_step"] or 0.0, 100.0)
        self.assertAlmostEqual(constraints["min_qty_contracts_effective"] or 0.0, 1.0)

    def test_build_plan_allows_grid_to_split_liquidity_limited_target(self) -> None:
        manager = ManualTradeManager()
        manager._ensure_client = AsyncMock(return_value=object())
        manager._resolve_market_symbol = AsyncMock(return_value="H/USDT:USDT")
        manager._fetch_orderbook = AsyncMock(
            return_value={
                "bids": [[0.25, 100.0]],
                "asks": [[0.251, 100.0]],
                "source": "test",
            }
        )
        manager._extract_market_constraints = lambda *_args, **_kwargs: {
            "min_qty": 10.0,
            "min_notional": None,
            "amount_step": 10.0,
            "price_step": 0.0001,
            "contract_size": 1.0,
        }
        manager._fetch_funding_meta = AsyncMock(return_value={})

        result = asyncio.run(
            manager._build_plan(
                {
                    "symbol": "HUSDT",
                    "qty": 1250.0,
                    "long_exchange": "kucoin",
                    "short_exchange": "bybit",
                    "max_slippage_bps": 16.0,
                    "use_orderbook_check": True,
                    "allow_liquidity_chunking": True,
                    "dry_run": False,
                },
                action="enter",
            )
        )

        self.assertEqual(result.get("errors"), [])
        self.assertTrue(
            any(
                "insufficient liquidity for qty 1250" in warning
                for warning in (result.get("warnings") or [])
            )
        )
        self.assertAlmostEqual(result["recommended_chunk_qty"], 100.0)

    def test_smart_enter_plan_uses_hedge_depth_not_primary_taker_depth(self) -> None:
        manager = ManualTradeManager()
        manager._ensure_client = AsyncMock(return_value=object())
        manager._resolve_market_symbol = AsyncMock(return_value="MIRA/USDT:USDT")

        async def _book(*, exchange, **_kwargs):
            if exchange == "kucoin":
                return {
                    "bids": [[100.0, 100.0]],
                    "asks": [[101.0, 100.0]],
                    "source": "test",
                }
            return {
                "bids": [[99.0, 10000.0]],
                "asks": [[100.0, 10000.0]],
                "source": "test",
            }

        manager._fetch_orderbook = AsyncMock(side_effect=_book)
        manager._extract_market_constraints = lambda *_args, **_kwargs: {
            "min_qty": 10.0,
            "min_notional": None,
            "amount_step": 10.0,
            "price_step": 0.1,
            "contract_size": 1.0,
        }
        manager._fetch_funding_meta = AsyncMock(return_value={})

        result = asyncio.run(
            manager._build_plan(
                {
                    "symbol": "MIRA",
                    "qty": 1000.0,
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "mode": "smart-enter",
                    "max_slippage_bps": 8.0,
                    "use_orderbook_check": True,
                    "allow_liquidity_chunking": True,
                    "dry_run": True,
                },
                action="enter",
            )
        )

        self.assertEqual(result.get("errors"), [])
        self.assertEqual(result["suggested_expensive_leg"]["suggested_leg"], "short")
        self.assertAlmostEqual(result["max_qty_by_exchange"]["kucoin"], 100.0)
        self.assertAlmostEqual(result["max_qty_by_exchange"]["binance"], 10000.0)
        self.assertAlmostEqual(result["recommended_qty"], 10000.0)
        self.assertAlmostEqual(result["recommended_chunk_qty"], 1000.0)
        liquidity = result["execution_liquidity"]
        self.assertEqual(liquidity["primary_maker"]["exchange"], "kucoin")
        self.assertFalse(liquidity["primary_maker"]["taker_depth_blocking"])
        self.assertEqual(liquidity["hedge_taker"]["exchange"], "binance")
        self.assertTrue(liquidity["hedge_taker"]["ready"])
        self.assertFalse(
            any("kucoin: insufficient liquidity" in warning for warning in result.get("warnings") or [])
        )

    def test_enter_balance_precheck_preserves_plan_diagnostics(self) -> None:
        manager = ManualTradeManager()
        plan = {
            "dry_run": False,
            "action": "enter",
            "symbol": "H",
            "qty": 7000.0,
            "mode": "smart-enter",
            "legs": [
                {"label": "long", "exchange": "bybit", "side": "buy"},
                {"label": "short", "exchange": "binance", "side": "sell"},
            ],
            "stats": {
                "bybit": {"best_bid": 0.292, "best_ask": 0.293, "min_liquidity_top3": 10000.0},
                "binance": {"best_bid": 0.289, "best_ask": 0.29, "min_liquidity_top3": 12000.0},
            },
            "slippage": {
                "bybit": {"expected_slippage_bps": 1.0, "filled_qty": 7000.0, "remaining_qty": 0.0},
                "binance": {"expected_slippage_bps": 1.5, "filled_qty": 7000.0, "remaining_qty": 0.0},
            },
            "market_constraints": {
                "bybit": {"min_qty_required": 18.0, "amount_step": 1.0},
                "binance": {"min_qty_required": 18.0, "amount_step": 1.0},
            },
            "errors": [],
            "warnings": ["force_chunk_qty is treated as a requested target and remains capped by live liquidity."],
            "generated_at": "test",
        }
        manager._fetch_positions_with_retry = AsyncMock(return_value=([], []))
        manager._build_plan = AsyncMock(return_value=plan)
        manager._fetch_balances_with_retry = AsyncMock(
            return_value=(
                {
                    "bybit": {"available": 5455.0},
                    "binance": {"available": 0.0},
                },
                [],
            )
        )
        manager._fetch_mark_prices_with_retry = AsyncMock(
            return_value=({"bybit": 0.292, "binance": 0.289}, [])
        )

        result = asyncio.run(
            manager.enter(
                {
                    "symbol": "H",
                    "qty": 7000.0,
                    "long_exchange": "bybit",
                    "short_exchange": "binance",
                    "mode": "smart-enter",
                }
            )
        )

        self.assertIn("binance: insufficient balance for min qty 18", result["errors"][0])
        self.assertEqual(result["stats"]["bybit"]["best_bid"], 0.292)
        self.assertEqual(result["slippage"]["binance"]["filled_qty"], 7000.0)
        self.assertEqual(result["warnings"], plan["warnings"])

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

    async def cancel_all_orders(self, symbol: str, params: dict | None = None) -> None:  # noqa: ARG002
        self.cancel_all_calls.append(symbol)

    async def fetch_open_orders(self, symbol: str, params: dict | None = None):  # noqa: ARG002
        return []

    async def cancel_order(self, order_id: str, symbol: str, params: dict | None = None) -> None:  # noqa: ARG002
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


class _FakeBitgetCancelClient:
    def __init__(self, *, fail_cancel_all: bool = False) -> None:
        self.fail_cancel_all = fail_cancel_all
        self.cancel_all_calls: list[tuple[str, dict]] = []
        self.fetch_open_calls: list[tuple[str, dict]] = []
        self.cancel_order_calls: list[tuple[str, str, dict]] = []

    async def cancel_all_orders(self, symbol: str, params: dict | None = None) -> None:
        self.cancel_all_calls.append((symbol, dict(params or {})))
        if self.fail_cancel_all:
            raise RuntimeError("cancel_all_failed")

    async def fetch_open_orders(self, symbol: str, params: dict | None = None):
        self.fetch_open_calls.append((symbol, dict(params or {})))
        return [{"id": "order-1"}]

    async def cancel_order(self, order_id: str, symbol: str, params: dict | None = None) -> None:
        self.cancel_order_calls.append((order_id, symbol, dict(params or {})))


class ManualTradeBitgetUtaCancelTestCase(unittest.TestCase):
    def test_cancel_open_orders_uses_uta_cancel_all(self) -> None:
        manager = ManualTradeManager()
        client = _FakeBitgetCancelClient()

        ok = asyncio.run(
            manager._cancel_open_orders_for_symbol(
                client,
                exchange="bitget",
                symbol="BTC",
                ccxt_symbol="BTC/USDT:USDT",
            )
        )

        self.assertTrue(ok)
        self.assertEqual(client.cancel_all_calls, [("BTC/USDT:USDT", {"uta": True})])
        self.assertEqual(client.fetch_open_calls, [])
        self.assertEqual(client.cancel_order_calls, [])

    def test_cancel_open_orders_fallback_uses_uta_fetch_and_cancel(self) -> None:
        manager = ManualTradeManager()
        client = _FakeBitgetCancelClient(fail_cancel_all=True)

        ok = asyncio.run(
            manager._cancel_open_orders_for_symbol(
                client,
                exchange="bitget",
                symbol="BTC",
                ccxt_symbol="BTC/USDT:USDT",
            )
        )

        self.assertTrue(ok)
        self.assertEqual(client.cancel_all_calls, [("BTC/USDT:USDT", {"uta": True})])
        self.assertEqual(client.fetch_open_calls, [("BTC/USDT:USDT", {"uta": True})])
        self.assertEqual(client.cancel_order_calls, [("order-1", "BTC/USDT:USDT", {"uta": True})])


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
        self.last_market_reason: str | None = None

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
        post_only=False,
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
        self.last_market_reason = reason
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

    def test_hedge_deadline_forces_market_when_price_is_static(self) -> None:
        manager = _HedgeFallbackManager()
        leg = {
            "exchange": "binance",
            "side": "buy",
            "label": "short",
            "reduce_only": False,
        }

        result = asyncio.run(
            manager._hedge_position(
                leg,
                "MIRA",
                10.0,
                hedge_order_type="limit",
                hedge_offset_bps=0.0,
                hedge_offset_ticks=0,
                hedge_limit_mode="passive",
                hedge_favorable_bps=9999.0,
                hedge_adverse_bps=9999.0,
                hedge_adverse_ticks=None,
                hedge_reprice_min_sec=0.0,
                payload={"hedge_timeout_sec": 1.0},
                min_qty_required=None,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "filled")
        self.assertAlmostEqual(result.get("filled_qty") or 0.0, 10.0)
        self.assertEqual(manager.cancelled_order_ids, ["L1"])
        self.assertEqual(manager.last_market_reason, "hedge_timeout")


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

    async def _ensure_ws_positions(self, exchanges, contract_sizes=None):  # noqa: D401, ARG002
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


class _GateDecimalSubmitClient:
    id = "gate"
    precisionMode = 4

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.markets = {
            "LAB/USDT:USDT": {
                "precision": {"amount": 0.1, "price": 0.00001},
                "limits": {
                    "amount": {"min": 0.1, "max": 1200.0},
                    "cost": {"min": None},
                    "price": {"min": 0.00001, "max": None},
                },
                "contractSize": 100.0,
                "info": {"enable_decimal": True},
            }
        }

    def amount_to_precision(self, symbol, value):  # noqa: D401, ARG002
        return f"{float(value):.10f}".rstrip("0").rstrip(".")

    def price_to_precision(self, symbol, value):  # noqa: D401, ARG002
        return f"{float(value):.5f}"

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
        return {"id": "GATE-1", "filled": 0.0, "average": None, "status": "open"}

    async def fetch_positions(self, symbols=None):  # noqa: D401, ARG002
        return []


class _GateDecimalSubmitManager(ManualTradeManager):
    def __init__(self, client) -> None:
        super().__init__()
        self._client = client

    async def _ensure_client(self, exchange, errors):  # noqa: D401, ARG002
        return self._client

    async def _resolve_market_symbol(self, client, symbol):  # noqa: D401, ARG002
        return "LAB/USDT:USDT"

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
        post_only=False,
        require_ws=True,
        log_cb=None,
    ):  # noqa: D401, ARG002
        self.last_primary_post_only = post_only
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
        self.last_hedge_limit_mode = hedge_limit_mode
        return {
            "exchange": leg["exchange"],
            "status": "error",
            "error": "hedge_submit_rejected",
            "error_type": "tick_size",
            "filled_qty": 0.0,
        }

    async def _cancel_order(self, leg, symbol, order_id):  # noqa: D401, ARG002
        return {"exchange": leg["exchange"], "status": "canceled", "order_id": order_id}

    async def _place_market(self, leg, symbol, qty, payload, *, reason, log_cb=None):  # noqa: D401, ARG002
        return {
            "exchange": leg["exchange"],
            "status": "error",
            "error": "reconcile_failed",
            "filled_qty": 0.0,
        }


class _SmartEnterNoneStartQtyManager(_SmartEnterPartialExposureManager):
    def _sum_position_qty(self, positions, *, exchange, side, symbol):  # noqa: D401, ARG002
        if not positions:
            return None
        return super()._sum_position_qty(positions, exchange=exchange, side=side, symbol=symbol)


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

    def test_submit_order_passes_post_only_for_primary_maker(self) -> None:
        client = _SubmitOrderClient()
        manager = _SubmitOrderManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "binance", "side": "sell"},
                "SIREN",
                200.0,
                "limit",
                price=0.635,
                reduce_only=False,
                post_only=True,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "submitted")
        self.assertEqual(len(client.calls), 1)
        self.assertIs((client.calls[0].get("params") or {}).get("postOnly"), True)

    def test_submit_order_preserves_gate_decimal_contract_amount(self) -> None:
        client = _GateDecimalSubmitClient()
        manager = _GateDecimalSubmitManager(client)

        result = asyncio.run(
            manager._submit_order(
                {"exchange": "gate", "side": "buy"},
                "LAB",
                120.0,
                "limit",
                price=14.81869,
                reduce_only=False,
                log_cb=None,
            )
        )

        self.assertEqual(result.get("status"), "submitted")
        self.assertEqual(len(client.calls), 1)
        self.assertAlmostEqual(float(client.calls[0]["amount"]), 1.2)
        self.assertAlmostEqual(float(result.get("qty_contracts") or 0.0), 1.2)

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

    def test_collect_action_errors_skips_reconciled_transient_error(self) -> None:
        manager = ManualTradeManager()
        errors = manager._collect_action_errors(
            [
                {
                    "exchange": "gate",
                    "status": "error",
                    "error": "temporary submit failure",
                    "handled_error": "final_reconcile",
                }
            ]
        )
        self.assertEqual(errors, [])

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
                {
                    "verbose_logs": False,
                    "hedge_order_type": "limit",
                    "hedge_limit_mode": "passive",
                },
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
        self.assertTrue(
            any(action.get("risk_state") == "partial_fill_exposure" for action in hedge_errors)
        )

        self.assertTrue(manager.last_primary_post_only)
        self.assertEqual(manager.last_hedge_limit_mode, "aggressive")
        self.assertTrue(
            any("hedge upgraded to aggressive" in warning for warning in (result.get("warnings") or []))
        )

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

    def test_smart_enter_treats_missing_start_position_qty_as_zero(self) -> None:
        manager = _SmartEnterNoneStartQtyManager()
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
                {"verbose_logs": False, "hedge_order_type": "limit", "hedge_limit_mode": "passive"},
            )
        )

        self.assertEqual(result.get("mode"), "smart-enter")
        self.assertIn("partial_fill_exposure", result.get("risk_flags") or [])


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

    def test_finalize_full_pair_exit_closes_tiny_preexisting_mismatch(self) -> None:
        manager = _DustFinalizeManager(
            positions=[
                {
                    "exchange": "bybit",
                    "symbol": "SIREN/USDT:USDT",
                    "side": "long",
                    "coin_qty": 9.0,
                    "mark_price": 0.032,
                }
            ]
        )
        legs = [
            {"exchange": "bybit", "label": "long", "side": "sell", "reduce_only": True},
            {"exchange": "kucoin", "label": "short", "side": "buy", "reduce_only": True},
        ]
        actions: list[dict[str, object]] = []
        warnings: list[str] = []

        asyncio.run(
            manager._finalize_exit_dust(
                symbol="SIREN",
                legs=legs,
                start_qty_by_exchange={"bybit": 59679.0, "kucoin": 59670.0},
                requested_exit_qty=59670.0,
                constraints={"bybit": {"amount_step": 1.0}},
                payload={
                    "exit_close_full_pair": True,
                    "exit_dust_notional_usd": 10.0,
                    "exit_dust_max_legs": 2,
                },
                actions=actions,
                warnings=warnings,
                log_cb=None,
            )
        )

        self.assertEqual(len(manager.market_calls), 1)
        self.assertEqual(manager.market_calls[0]["exchange"], "bybit")
        self.assertAlmostEqual(float(manager.market_calls[0]["qty"]), 9.0)
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

    def test_smart_exit_plain_stop_skips_force_finalize(self) -> None:
        manager = _FastExitForceFinalizeManager(
            force_finalize=False,
            end_positions=[
                {"exchange": "binance", "symbol": "BTC/USDT:USDT", "side": "long", "coin_qty": 5.0},
                {"exchange": "okx", "symbol": "BTC/USDT:USDT", "side": "short", "coin_qty": 10.0},
            ],
        )

        result = asyncio.run(
            manager._execute_smart_exit(
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
                {"qty": 10.0, "max_runtime_sec": 60},
            )
        )

        self.assertEqual(result.get("mode"), "smart-exit")
        self.assertIn("stopped_by_user", result.get("warnings") or [])
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


class _KucoinRiskLimitClient:
    id = "kucoinfutures"

    def __init__(self) -> None:
        self.markets = {
            "COTI/USDT:USDT": {
                "id": "COTIUSDTM",
                "symbol": "COTI/USDT:USDT",
                "base": "COTI",
                "quote": "USDT",
                "type": "swap",
                "swap": True,
                "contractSize": 10.0,
            }
        }

    def market(self, symbol):
        return self.markets[symbol]

    async def fetch_positions(self, symbols):
        return [
            {
                "symbol": symbols[0],
                "contracts": 34689.0,
                "info": {"riskLimit": 5000, "markValue": 5450.0},
            }
        ]

    async def fetch_ticker(self, _symbol):
        return {"last": 0.01572, "info": {"markPrice": 0.01572}}

    async def futuresPublicGetContractsRiskLimitSymbol(self, _params):
        return {
            "code": "200000",
            "data": [
                {"level": 1, "maxRiskLimit": 5000, "maxLeverage": 50},
                {"level": 2, "maxRiskLimit": 10000, "maxLeverage": 30},
            ],
        }

    async def futuresPrivatePostPositionRiskLimitLevelChange(self, _params):
        raise AssertionError("preflight must never change the risk level")


class _KucoinRiskLimitGateway:
    def __init__(self) -> None:
        self.client = _KucoinRiskLimitClient()

    async def refresh_credentials_async(self, force_env=False):
        return None

    async def ensure_client(self):
        return None

    async def close(self):
        return None


class ManualTradeRiskLimitPreflightTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_kucoin_preflight_reports_required_tier_without_changing_it(self) -> None:
        manager = ManualTradeManager()
        manager._gateways["kucoin"] = _KucoinRiskLimitGateway()

        result = await manager.entry_risk_limit_preflight(
            symbol="COTIUSDT",
            long_exchange="kucoin",
            short_exchange="bybit",
            target_position_qty=362307.0,
            leverage=3.0,
        )

        self.assertFalse(result["ready"])
        self.assertEqual(result["selected_level"], 1)
        self.assertEqual(result["selected_max_risk_limit_usd"], 5000.0)
        self.assertEqual(result["required_level"], 2)
        self.assertTrue(result["change_supported"])
        self.assertTrue(result["change_cancels_open_orders"])
        self.assertGreater(result["projected_notional_usd"], 5000.0)


class ProtectivePreflightTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_agent_rebalance_submits_limit_order(self) -> None:
        manager = ManualTradeManager()
        manager._snapshot_legs = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "errors": [],
                "constraints": {
                    "binance": {
                        "min_qty": 1.0,
                        "min_notional": 5.0,
                        "amount_step": 1.0,
                        "price_step": 0.00001,
                    }
                },
                "stats": {
                    "binance": OrderBookStats(
                        best_bid=0.25701,
                        best_ask=0.25702,
                        spread=0.00001,
                        mid=0.257015,
                        bid_liquidity_top3=1000.0,
                        ask_liquidity_top3=1000.0,
                        min_liquidity_top3=1000.0,
                    )
                },
                "mid_price": 0.257015,
                "max_qty_by_exchange": {"binance": 1000.0},
                "orderbooks": {},
            }
        )
        manager._place_limit_at_agent = AsyncMock(  # type: ignore[method-assign]
            return_value={"status": "filled", "filled_qty": 30.0, "order_id": "limit1"}
        )
        manager._place_market = AsyncMock()  # type: ignore[method-assign]

        result = await manager.agent_rebalance(
            exchange="binance",
            symbol="HUSDT",
            side="buy",
            qty_base=30.0,
            max_slippage_bps=16.0,
        )

        self.assertEqual(result["status"], "filled")
        self.assertAlmostEqual(result["filled_qty"], 30.0)
        manager._place_limit_at_agent.assert_awaited_once()
        manager._place_market.assert_not_awaited()

    async def test_analyze_rebalance_returns_market_minimums_without_orders(self) -> None:
        manager = ManualTradeManager()
        manager._snapshot_legs = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "errors": [],
                "constraints": {
                    "okx": {
                        "min_qty": 0.01,
                        "min_notional": 10.0,
                        "amount_step": 0.01,
                    }
                },
                "stats": {
                    "okx": OrderBookStats(
                        best_bid=99.0,
                        best_ask=101.0,
                        spread=2.0,
                        mid=100.0,
                        bid_liquidity_top3=1000.0,
                        ask_liquidity_top3=1000.0,
                        min_liquidity_top3=1000.0,
                    )
                },
                "mid_price": 100.0,
                "max_qty_by_exchange": {"okx": 5.0},
                "orderbook_sources": {"okx": "ws"},
            }
        )

        result = await manager.analyze_rebalance(
            exchange="okx",
            symbol="BTCUSDT",
            side="buy",
            qty_base=1.0,
        )

        self.assertEqual(result["errors"], [])
        self.assertAlmostEqual(float(result["min_qty_required"]), 0.1)
        self.assertEqual(result["orderbook_source"], "ws")


if __name__ == "__main__":
    unittest.main()
