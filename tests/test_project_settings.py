from __future__ import annotations

import json
import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from project_settings import SettingsManager
from webapp.services import (
    DataService,
    _position_pair_quantities,
    _protective_issue_kind,
)


class ProjectSettingsTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.settings_path = Path(self.tmp_dir.name) / "settings.json"
        state_dir = Path(self.tmp_dir.name) / "state"
        log_dir = Path(self.tmp_dir.name) / "logs"
        state_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        self._service_path_patchers = [
            patch("webapp.services.AUTO_ARB_STATE_PATH", state_dir / "auto_arb_rules.json"),
            patch("webapp.services.AUTO_ARB_HISTORY_PATH", log_dir / "auto_arb_history.jsonl"),
            patch(
                "webapp.services.PROTECTIVE_SHADOW_HISTORY_PATH",
                log_dir / "protective_shadow_history.jsonl",
            ),
            patch("execution.wallet.WalletService.DEFAULT_STATE_PATH", state_dir / "wallet_state.json"),
        ]
        for patcher in self._service_path_patchers:
            patcher.start()
        self.manager = SettingsManager(path=self.settings_path)

    def tearDown(self) -> None:
        for patcher in reversed(self._service_path_patchers):
            patcher.stop()
        self.tmp_dir.cleanup()

    def test_protective_toggles_persist(self) -> None:
        """Ensure disabling protective toggles survives reload."""
        self.manager.update(
            {"protective": {"auto_protect_enabled": False, "auto_take_enabled": False}}
        )
        reloaded = SettingsManager(path=self.settings_path)
        protective = reloaded.current.protective
        self.assertFalse(protective.get("auto_protect_enabled"))
        self.assertFalse(protective.get("auto_take_enabled"))

    def test_retired_reduction_settings_are_pruned(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "derisk_confirm_cycles": 5,
                    "auto_rebalance_enabled": True,
                    "rebalance_delta_pct": 0.2,
                    "auto_protect_enabled": True,
                },
                "manual": {
                    "auto_exit_policy": {"tier1": {"chunk_notional_cap_usd": 999}},
                    "enter_live_depth": 7,
                },
            }
        )

        self.assertNotIn("auto_derisk_enabled", self.manager.current.protective)
        self.assertNotIn("derisk_confirm_cycles", self.manager.current.protective)
        self.assertNotIn("auto_rebalance_enabled", self.manager.current.protective)
        self.assertNotIn("rebalance_delta_pct", self.manager.current.protective)
        self.assertTrue(self.manager.current.protective["auto_protect_enabled"])
        self.assertNotIn("auto_exit_policy", self.manager.current.manual)
        self.assertEqual(self.manager.current.manual["enter_live_depth"], 7)

    def test_protective_margin_control_settings_persist(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "enforce_isolated_margin": False,
                    "enforce_leverage": False,
                    "target_leverage": 2.5,
                    "kucoin_isolated_topup_only": False,
                }
            }
        )
        reloaded = SettingsManager(path=self.settings_path)
        protective = reloaded.current.protective
        self.assertFalse(protective.get("enforce_isolated_margin"))
        self.assertFalse(protective.get("enforce_leverage"))
        self.assertAlmostEqual(float(protective.get("target_leverage", 0.0)), 2.5)
        self.assertFalse(protective.get("kucoin_isolated_topup_only"))

    def test_notification_channel_settings_persist(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "notification_primary_channel": "ntfy",
                    "notification_fallback_channel": "telegram",
                }
            }
        )
        reloaded = SettingsManager(path=self.settings_path)
        protective = reloaded.current.protective
        self.assertEqual(protective.get("notification_primary_channel"), "ntfy")
        self.assertEqual(protective.get("notification_fallback_channel"), "telegram")

    def test_target_leverage_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            self.manager.update({"protective": {"target_leverage": 0}})

    def test_fallback_take_defaults_to_thirty_percent_and_loads_into_risk_config(self) -> None:
        protective = self.manager.current.protective
        self.assertAlmostEqual(float(protective.get("fallback_take_rr_pct", 0.0)), 0.30)

        service = DataService(settings_manager=self.manager)
        self.assertAlmostEqual(service._risk_config_from_settings().fallback_take_rr_pct, 0.30)

    def test_legacy_timed_stop_rotation_is_disabled(self) -> None:
        self.settings_path.write_text(
            json.dumps({"protective": {"stop_force_requote_max_age_sec": 60}}),
            encoding="utf-8",
        )

        reloaded = SettingsManager(path=self.settings_path)

        self.assertEqual(
            reloaded.current.protective.get("stop_force_requote_max_age_sec"),
            0,
        )
        service = DataService(settings_manager=reloaded)
        self.assertEqual(service._risk_config_from_settings().stop_force_requote_max_age_sec, 0)

    def test_fallback_take_rejects_unsafe_range(self) -> None:
        with self.assertRaises(ValueError):
            self.manager.update({"protective": {"fallback_take_rr_pct": 0}})
        with self.assertRaises(ValueError):
            self.manager.update({"protective": {"fallback_take_rr_pct": 0.51}})

    def test_notification_primary_channel_must_be_supported(self) -> None:
        with self.assertRaises(ValueError):
            self.manager.update({"protective": {"notification_primary_channel": "email"}})

    def test_notification_fallback_channel_must_be_supported(self) -> None:
        with self.assertRaises(ValueError):
            self.manager.update({"protective": {"notification_fallback_channel": "sms"}})



    async def test_protective_sync_skipped_when_disabled(self) -> None:
        """Protective sync should short-circuit when both toggles are off."""
        self.manager.update(
            {"protective": {"auto_protect_enabled": False, "auto_take_enabled": False}}
        )
        service = DataService(settings_manager=self.manager)

        class _SentinelProtective:
            called = False

            async def sync_protective_orders(self, *args, **kwargs):
                self.called = True
                return []

        service._protective_manager = _SentinelProtective()  # type: ignore[attr-defined]
        # _maybe_sync_protective_orders reads snapshot to enumerate positions; keep it minimal.
        service._accounts = type("X", (), {"snapshot": lambda self=None: {}})()  # type: ignore

        await service._maybe_sync_protective_orders()
        self.assertFalse(service._protective_manager.called)  # type: ignore[attr-defined]

    async def test_settings_refreshes_accounts_before_position_markets(self) -> None:
        service = DataService(settings_manager=self.manager)
        calls: list[str] = []

        async def _refresh_accounts(*, force_env: bool = False) -> None:  # noqa: ARG001
            calls.append("accounts")

        async def _refresh_markets(*, force: bool = False) -> None:  # noqa: ARG001
            calls.append("markets")

        service._accounts.refresh_now = _refresh_accounts  # type: ignore[method-assign]
        service._refresh_positions_market_snapshots = _refresh_markets  # type: ignore[method-assign]
        service._settings_refresh_pending = True

        await service._refresh_operational_state_after_settings()

        self.assertEqual(calls, ["accounts", "markets"])



    def test_protective_issue_kind_detects_auth_errors(self) -> None:
        self.assertEqual(
            _protective_issue_kind(
                'binanceusdm {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action"}'
            ),
            "auth_error",
        )



    def test_mobile_manual_defaults_payload_exposes_enabled_exchanges(self) -> None:
        analysis_map = {
            "binance": True,
            "okx": True,
            "bybit": False,
            "bingx": False,
            "bitget": False,
            "gate": False,
            "mexc": False,
            "kucoin": False,
        }
        self.manager.update(
            {
                "analysis_exchanges": analysis_map
            }
        )
        service = DataService(settings_manager=self.manager)

        payload = service.mobile_manual_defaults_payload()

        self.assertEqual(payload["exchanges"], ["binance", "okx"])
        self.assertEqual(payload["defaults"]["margin_mode"], "isolated")
        self.assertEqual(payload["defaults"]["max_runtime_sec"], 300)
        self.assertEqual(payload["main_modes"][0]["id"], "smart")

    def test_mobile_positions_payload_builds_cards(self) -> None:
        service = DataService(settings_manager=self.manager)
        next_funding = (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "binance",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "long",
                            "coin_qty": 1.0,
                            "notional": 100.0,
                            "entry_price": 100.0,
                            "mark_price": 101.0,
                            "unrealized_pnl": 1.0,
                            "funding_rate": 0.0001,
                            "next_funding": next_funding,
                            "leverage": 3.0,
                            "liquidation_price": 80.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "coin_qty": 1.0,
                            "notional": 100.0,
                            "entry_price": 99.0,
                            "mark_price": 98.0,
                            "unrealized_pnl": 1.0,
                            "funding_rate": 0.0002,
                            "next_funding": next_funding,
                            "leverage": 3.0,
                            "liquidation_price": 120.0,
                        },
                    ],
                    "balances": [
                        {
                            "exchange": "binance",
                            "asset": "USDT",
                            "total": "1000",
                            "available": "700",
                            "used": "300",
                            "margin_ratio": "0.3",
                        },
                        {
                            "exchange": "okx",
                            "asset": "USDT",
                            "total": 500,
                            "available": 150,
                        },
                    ],
                    "last_updated": "2026-05-30T10:00:00+00:00",
                }
            },
        )()
        payload = service.mobile_positions_payload()

        self.assertEqual(payload["filters"]["all"], 1)
        self.assertEqual(payload["account_last_updated"], "2026-05-30T10:00:00+00:00")
        self.assertEqual(len(payload["balances"]), 2)
        self.assertEqual(payload["balances"][0]["exchange"], "binance")
        self.assertAlmostEqual(payload["balances"][0]["margin_ratio"], 0.3)
        self.assertEqual(payload["balances"][1]["status"], "watch")
        self.assertEqual(len(payload["cards"]), 1)
        card = payload["cards"][0]
        self.assertEqual(card["symbol"], "BTCUSDT")
        self.assertEqual(card["pair_label"], "BINANCE / OKX")
        self.assertAlmostEqual(card["net_pnl"], 2.0)
        self.assertTrue(card["flags"]["funding_soon"])
        self.assertNotIn("auto_exit", card)
        self.assertNotIn("auto_exit_on", card["flags"])
        self.assertAlmostEqual(
            card["live_spread_pct"],
            card["position_summary"]["pair_mark_spread_pct"],
        )
        self.assertAlmostEqual(card["position_summary"]["hedged_quantity"], 1.0)
        self.assertAlmostEqual(card["position_summary"]["imbalance_quantity"], 0.0)
        self.assertAlmostEqual(card["position_summary"]["current_exposure_usdt"], 98.0)
        self.assertAlmostEqual(card["position_summary"]["gross_current_exposure_usdt"], 199.0)
        self.assertAlmostEqual(card["position_summary"]["entry_exposure_usdt"], 99.0)
        self.assertAlmostEqual(card["position_summary"]["gross_entry_exposure_usdt"], 199.0)
        self.assertAlmostEqual(card["expected_funding"], 0.0095)
        self.assertEqual(len(card["legs"]), 2)

    def test_position_valuation_is_exchange_neutral_for_all_supported_venues(self) -> None:
        service = DataService(settings_manager=self.manager)
        next_funding = (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()
        venues = (
            ("binance", 100.0, 1.0),
            ("bybit", 100.0, 1.0),
            ("kucoin", 10.0, 10.0),
            ("okx", 20.0, 5.0),
            ("gate", 1000.0, 0.1),
            ("bitget", 100.0, 1.0),
            ("mexc", 200.0, 0.5),
            ("bingx", 100.0, 1.0),
        )

        for exchange, contracts, contract_size in venues:
            with self.subTest(exchange=exchange):
                rows, grouped = service._positions_by_symbol(  # type: ignore[attr-defined]
                    [
                        {
                            "exchange": exchange,
                            "symbol": "LABUSDT",
                            "symbol_normalized": "LABUSDT",
                            "side": "long",
                            "contracts": contracts,
                            "contract_size": contract_size,
                            "notional": 9999.0,
                            "entry_price": 2.0,
                            "mark_price": 1.25,
                            "funding_rate": 0.01,
                            "funding_interval_hours": 4.0,
                            "next_funding": next_funding,
                        }
                    ],
                    return_grouped=True,
                )
                leg = grouped["LABUSDT"][0]
                self.assertAlmostEqual(leg["amount"], 125.0)
                self.assertAlmostEqual(leg["current_notional"], 125.0)
                self.assertAlmostEqual(leg["entry_notional"], 200.0)
                self.assertAlmostEqual(leg["exchange_notional"], 9999.0)
                self.assertAlmostEqual(leg["expected_funding"], -1.25)
                self.assertEqual(leg["valuation_status"], "current")
                self.assertEqual(leg["mark_price_source"], "position")
                self.assertAlmostEqual(leg["funding_interval_hours"], 4.0)
                self.assertEqual(rows[0]["exchange"], exchange)

    def test_position_valuation_is_unavailable_without_real_mark_price(self) -> None:
        service = DataService(settings_manager=self.manager)
        next_funding = (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat()

        _rows, grouped = service._positions_by_symbol(  # type: ignore[attr-defined]
            [
                {
                    "exchange": "kucoin",
                    "symbol": "LABUSDT",
                    "symbol_normalized": "LABUSDT",
                    "side": "short",
                    "contracts": 10.0,
                    "contract_size": 10.0,
                    "notional": 200.0,
                    "entry_price": 2.0,
                    "mark_price": None,
                    "funding_rate": -0.01,
                    "next_funding": next_funding,
                }
            ],
            return_grouped=True,
        )

        leg = grouped["LABUSDT"][0]
        self.assertIsNone(leg["current_notional"])
        self.assertIsNone(leg["current_mark_price"])
        self.assertIsNone(leg["expected_funding"])
        self.assertEqual(leg["entry_notional"], 200.0)
        self.assertEqual(leg["exchange_notional"], 200.0)
        self.assertEqual(leg["valuation_status"], "unavailable")

    def test_tut_native_cost_mismatch_normalizes_to_current_mark_exposure(self) -> None:
        service = DataService(settings_manager=self.manager)
        next_funding = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        rows, grouped = service._positions_by_symbol(  # type: ignore[attr-defined]
            [
                {
                    "exchange": "binance",
                    "symbol": "TUTUSDT",
                    "side": "long",
                    "contracts": 7520.0,
                    "contract_size": 1.0,
                    "notional": 1269.61664,
                    "entry_price": 0.217877,
                    "mark_price": 0.168832,
                    "funding_rate": 0.001,
                    "next_funding": next_funding,
                },
                {
                    "exchange": "kucoin",
                    "symbol": "TUTUSDT",
                    "side": "short",
                    "contracts": 755.0,
                    "contract_size": 10.0,
                    "notional": 1722.0795,
                    "entry_price": 0.22809,
                    "mark_price": 0.16982,
                    "funding_rate": 0.001,
                    "next_funding": next_funding,
                },
            ],
            return_grouped=True,
        )

        legs = {leg["exchange"]: leg for leg in grouped["TUTUSDT"]}
        self.assertAlmostEqual(legs["binance"]["current_notional"], 1269.61664)
        self.assertAlmostEqual(legs["kucoin"]["current_notional"], 1282.141)
        self.assertAlmostEqual(legs["kucoin"]["entry_notional"], 1722.0795)
        self.assertAlmostEqual(legs["kucoin"]["exchange_notional"], 1722.0795)
        summary = next(row for row in rows if row["type"] == "summary")
        self.assertAlmostEqual(summary["expected_funding"], 0.01252436)
        self.assertAlmostEqual(summary["imbalance_pct"], 30.0 / 7520.0 * 100.0)

    def test_position_pair_quantities_uses_smaller_coin_leg(self) -> None:
        quantities = _position_pair_quantities(
            [
                {
                    "exchange": "binance",
                    "symbol": "LABUSDT",
                    "side": "long",
                    "coin_qty": 120000.0,
                },
                {
                    "exchange": "kucoin",
                    "symbol": "LABUSDT",
                    "side": "short",
                    "coin_qty": 100000.0,
                },
            ],
            symbol="LABUSDT",
            long_exchange="binance",
            short_exchange="kucoin",
        )
        self.assertAlmostEqual(quantities["long_qty"], 120000.0)
        self.assertAlmostEqual(quantities["short_qty"], 100000.0)
        self.assertAlmostEqual(quantities["hedged_qty"], 100000.0)
        self.assertAlmostEqual(quantities["imbalance_qty"], 20000.0)
        self.assertAlmostEqual(quantities["imbalance_pct"], 20.0)

    async def test_position_action_resolves_percent_from_fresh_hedged_qty(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "binance",
                            "symbol": "LABUSDT",
                            "side": "long",
                            "coin_qty": 120000.0,
                        },
                        {
                            "exchange": "kucoin",
                            "symbol": "LABUSDT",
                            "side": "short",
                            "coin_qty": 100000.0,
                        },
                    ]
                }
            },
        )()
        captured: dict[str, object] = {}

        async def _manual_exit(payload):
            captured.update(payload)
            return {"dry_run": True, "errors": []}

        service.manual_exit = _manual_exit  # type: ignore[assignment]
        result = await service.position_action(
            {
                "symbol": "LABUSDT",
                "long_exchange": "binance",
                "short_exchange": "kucoin",
                "action": "exit",
                "percent": 25,
                "dry_run": True,
                "async_run": False,
                "max_runtime_sec": 300,
            }
        )

        self.assertAlmostEqual(float(captured["qty"]), 25000.0)
        self.assertAlmostEqual(result["position_action"]["hedged_qty"], 100000.0)
        self.assertAlmostEqual(result["position_action"]["action_qty"], 25000.0)
        self.assertEqual(captured["max_slippage_bps"], 8.0)
        self.assertEqual(captured["max_runtime_sec"], 300)
        self.assertTrue(captured["allow_liquidity_chunking"])
        self.assertFalse(captured["exit_close_full_pair"])
        self.assertEqual(result["position_action"]["quantity_basis"], "min_long_short_coin_qty")

        captured.clear()
        await service.position_action(
            {
                "symbol": "LABUSDT",
                "long_exchange": "binance",
                "short_exchange": "kucoin",
                "action": "exit",
                "percent": 100,
                "dry_run": True,
                "async_run": False,
            }
        )
        self.assertAlmostEqual(float(captured["qty"]), 100000.0)
        self.assertTrue(captured["exit_close_full_pair"])
        self.assertEqual(captured["exit_dust_max_legs"], 2)

        async def _manual_enter(payload):
            captured.clear()
            captured.update(payload)
            return {"dry_run": True, "errors": []}

        service.manual_enter = _manual_enter  # type: ignore[assignment]
        await service.position_action(
            {
                "symbol": "LABUSDT",
                "long_exchange": "binance",
                "short_exchange": "kucoin",
                "action": "add",
                "percent": 25,
                "dry_run": True,
                "async_run": False,
            }
        )
        self.assertEqual(captured["max_slippage_bps"], 12.0)
        self.assertTrue(captured["allow_liquidity_chunking"])

    async def test_mobile_manual_spread_uses_ws_orderbook(self) -> None:
        service = DataService(settings_manager=self.manager)

        class _MarketData:
            async def get_orderbook(self, exchange: str, symbol: str, **_: object) -> dict[str, object]:
                books = {
                    "binance": {"bids": [[100.0, 3.0]], "asks": [[101.0, 3.0]], "timestamp": 1_700_000_000.0},
                    "okx": {"bids": [[103.0, 2.0]], "asks": [[104.0, 2.0]], "timestamp": 1_700_000_000.0},
                }
                return books[exchange]

        service._market_data = _MarketData()  # type: ignore[attr-defined]

        payload = await service.mobile_manual_spread(
            {
                "symbol": "BTCUSDT",
                "action": "enter",
                "long_exchange": "binance",
                "short_exchange": "okx",
            }
        )

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["buy_exchange"], "binance")
        self.assertEqual(payload["sell_exchange"], "okx")
        self.assertAlmostEqual(payload["buy_price"], 101.0)
        self.assertAlmostEqual(payload["sell_price"], 103.0)
        self.assertAlmostEqual(payload["spread_pct"], (101.0 - 103.0) / 101.0 * 100.0)
        self.assertEqual(payload["quotes"]["binance"]["source"], "websocket")

    async def test_manual_async_runtime_end_without_fills_is_no_fill_status(self) -> None:
        service = DataService(settings_manager=self.manager)

        async def _manual_enter(payload, **_kwargs):
            return {
                "dry_run": False,
                "action": "enter",
                "symbol": payload["symbol"],
                "qty": 100.0,
                "errors": [],
                "warnings": ["Remaining qty 100 not entered (smart-enter runtime ended)."],
                "remaining_qty": 100.0,
                "actions": [],
            }

        service._manual.enter = _manual_enter  # type: ignore[method-assign]
        started = await service.manual_enter(
            {
                "symbol": "ESPORTS",
                "long_exchange": "binance",
                "short_exchange": "kucoin",
                "qty": 100.0,
                "mode": "smart-enter",
                "async_run": True,
                "dry_run": False,
            }
        )

        exec_id = str(started["execution_id"])
        status = {}
        for _ in range(20):
            status = await service.manual_exec_status(exec_id)
            if status.get("status") != "running":
                break
            await asyncio.sleep(0.01)

        self.assertEqual(status.get("status"), "completed_no_fill")
        summary = next(
            entry
            for entry in (status.get("logs") or [])
            if entry.get("event") == "summary"
        )
        self.assertEqual(summary["data"]["terminal_reason"], "no_fill_before_runtime")

    async def test_grid_rounding_residual_is_completed_with_dust(self) -> None:
        service = DataService(settings_manager=self.manager)

        async def _manual_enter(payload, **_kwargs):
            return {
                "dry_run": False,
                "action": "enter",
                "symbol": payload["symbol"],
                "qty": 100.0,
                "errors": ["bybit: order precheck failed (qty_below_step)"],
                "warnings": [
                    "Remaining qty 1e-14 not entered (smart-enter runtime ended).",
                ],
                "remaining_qty": 1e-14,
                "actions": [
                    {
                        "exchange": "bybit",
                        "status": "filled",
                        "filled_qty": 99.99999999999999,
                        "order_id": "filled-leg",
                    },
                    {
                        "exchange": "bybit",
                        "status": "error",
                        "error": "qty_below_step",
                        "filled_qty": 0.0,
                    },
                ],
            }

        service._manual.enter = _manual_enter  # type: ignore[method-assign]
        started = await service.manual_enter(
            {
                "symbol": "DEXEUSDT",
                "long_exchange": "bybit",
                "short_exchange": "binance",
                "qty": 100.0,
                "mode": "smart-enter",
                "async_run": True,
                "dry_run": False,
                "auto_arb_agent": True,
                "auto_arb_rule_id": "dexe-grid",
            }
        )

        exec_id = str(started["execution_id"])
        status = {}
        for _ in range(20):
            status = await service.manual_exec_status(exec_id)
            if status.get("status") != "running":
                break
            await asyncio.sleep(0.01)

        self.assertEqual(status.get("status"), "completed_with_dust")
        result = status.get("result") or {}
        self.assertEqual(result.get("errors"), [])
        self.assertEqual(
            result.get("dust_errors"),
            ["bybit: order precheck failed (qty_below_step)"],
        )
        summary = next(
            entry
            for entry in (status.get("logs") or [])
            if entry.get("event") == "summary"
        )
        self.assertEqual(summary["data"]["terminal_reason"], "completed_with_dust")
        self.assertEqual(summary["data"]["error_count"], 0)
        self.assertEqual(summary["data"]["dust_error_count"], 1)

    async def test_manual_exit_cleans_protection_only_through_verified_cleanup(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": []}},
        )()

        async def _manual_exit(payload, positions, **_kwargs):  # noqa: ARG001
            return {
                "dry_run": False,
                "action": "exit",
                "symbol": "SIRENUSDT",
                "errors": [],
                "warnings": [],
                "actions": [],
            }

        service._manual.exit = _manual_exit  # type: ignore[method-assign]
        cleanup = AsyncMock(
            return_value=[
                {
                    "exchange": "kucoin",
                    "symbol": "SIRENUSDT",
                    "status": "cleanup_cancelled",
                }
            ]
        )
        service._cleanup_verified_orphan_protective_targets = cleanup  # type: ignore[method-assign]

        result = await service.manual_exit(
            {
                "symbol": "SIRENUSDT",
                "long_exchange": "bybit",
                "short_exchange": "kucoin",
                "qty": 100.0,
                "dry_run": False,
                "async_run": False,
            }
        )

        cleanup.assert_awaited_once_with(
            {("bybit", "SIRENUSDT"), ("kucoin", "SIRENUSDT")},
            reason="manual_exit_completed",
        )
        self.assertEqual(result["protective_cleanup"][0]["status"], "cleanup_cancelled")

    async def test_periodic_protective_sweep_uses_healthy_exchange_discovery(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "exchange_health": {
                        "kucoin": {"health": "healthy"},
                        "gate": {"health": "healthy"},
                    },
                    "status": [
                        {
                            "exchange": "kucoin",
                            "status": "ok",
                            "positions_fetch_ok": True,
                            "checked_at": datetime.now(timezone.utc).isoformat(),
                        },
                        {
                            "exchange": "gate",
                            "status": "ok",
                            "positions_fetch_ok": True,
                            "checked_at": (
                                datetime.now(timezone.utc) - timedelta(minutes=10)
                            ).isoformat(),
                        },
                    ],
                }
            },
        )()
        discovery = AsyncMock(
            return_value={
                "targets": [{"exchange": "kucoin", "symbol": "GWEIUSDT"}],
                "errors": [],
            }
        )
        service._protective_manager.discover_open_protective_targets = discovery  # type: ignore[method-assign]
        cleanup = AsyncMock(return_value=[{"status": "cleanup_cancelled"}])
        service._cleanup_verified_orphan_protective_targets = cleanup  # type: ignore[method-assign]

        result = await service._maybe_sweep_orphan_protective_orders(
            reason="test",
            force=True,
        )

        discovery.assert_awaited_once_with({"kucoin"})
        cleanup.assert_awaited_once_with(
            {("kucoin", "GWEIUSDT")},
            reason="test",
        )
        self.assertEqual(result, [{"status": "cleanup_cancelled"}])

    async def test_mobile_manual_spread_roll_short_maps_buy_from_sell_to(self) -> None:
        service = DataService(settings_manager=self.manager)

        class _MarketData:
            async def get_orderbook(self, exchange: str, symbol: str, **_: object) -> dict[str, object]:
                books = {
                    "binance": {"bids": [[100.0, 1.0]], "asks": [[101.0, 1.0]], "timestamp": 1_700_000_000.0},
                    "kucoin": {"bids": [[99.0, 1.0]], "asks": [[100.0, 1.0]], "timestamp": 1_700_000_000.0},
                }
                return books[exchange]

        service._market_data = _MarketData()  # type: ignore[attr-defined]

        payload = await service.mobile_manual_spread(
            {
                "symbol": "ETHUSDT",
                "action": "roll",
                "from_exchange": "binance",
                "to_exchange": "kucoin",
                "side": "short",
            }
        )

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["buy_exchange"], "binance")
        self.assertEqual(payload["sell_exchange"], "kucoin")
        self.assertAlmostEqual(payload["buy_price"], 101.0)
        self.assertAlmostEqual(payload["sell_price"], 99.0)







    def test_position_scan_evidence_rejects_failed_position_fetch(self) -> None:
        checked_at = datetime.now(timezone.utc).isoformat()
        evidence = DataService._position_scan_evidence(
            {
                "status": [
                    {
                        "exchange": "binance",
                        "status": "error",
                        "positions_fetch_ok": False,
                        "positions_error": "timeout",
                        "checked_at": checked_at,
                    }
                ]
            },
            {"binance"},
        )

        self.assertFalse(evidence["trusted"])





























    def test_account_state_exposes_margin_diagnostics_and_logic_log(self) -> None:
        service = DataService(settings_manager=self.manager)

        class _FakeAccounts:
            def snapshot(self) -> dict:
                return {
                    "positions": [
                        {
                            "exchange": "kucoin",
                            "symbol": "ARIAUSDTM",
                            "symbol_normalized": "ARIAUSDT",
                            "side": "short",
                            "margin_mode": "isolated",
                            "margin_mode_source": "payload.marginMode",
                            "leverage": 4.5,
                            "leverage_source": "payload.realLeverage",
                            "mark_price": 1.0,
                            "liquidation_price": 1.4,
                            "notional": 450.0,
                            "raw": {
                                "positionValue": "450",
                                "posMargin": "100",
                            },
                        }
                    ],
                    "balances": [
                        {
                            "exchange": "kucoin",
                            "asset": "USDT",
                            "available": 250.0,
                            "used": 100.0,
                            "total": 350.0,
                        }
                    ],
                    "status": [],
                    "last_updated": "2026-04-02T10:00:00+00:00",
                }

        service._accounts = _FakeAccounts()  # type: ignore[attr-defined]
        service._positions_market_snapshot_lookup = lambda: ({}, {})  # type: ignore[attr-defined]
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: ([], {})
        )
        service._positions_market_state = lambda positions=None: {"status": []}  # type: ignore[attr-defined]

        first = service._account_state()
        diagnostics = first.get("margin_diagnostics") or []
        logic_log = first.get("margin_logic_log") or []
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["exchange"], "kucoin")
        self.assertEqual(diagnostics[0]["symbol"], "ARIAUSDT")
        self.assertEqual(diagnostics[0]["decision"], "add_margin")
        self.assertEqual(diagnostics[0]["reason"], "kucoin_target_leverage")
        self.assertTrue(logic_log)
        self.assertEqual(logic_log[-1]["event"], "decision")
        self.assertEqual(logic_log[-1]["decision"], "add_margin")

        second = service._account_state()
        second_log = second.get("margin_logic_log") or []
        self.assertEqual(len(second_log), len(logic_log))

    def test_margin_reduce_disabled_persists_shadow_candidate(self) -> None:
        self.manager.update(
            {"protective": {"auto_margin_reduce_enabled": False}}
        )
        service = DataService(settings_manager=self.manager)
        rows = service._margin_diagnostics(  # type: ignore[attr-defined]
            [
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT:USDT",
                    "side": "long",
                    "margin_mode": "isolated",
                    "mark_price": 100.0,
                    "liquidation_price": 50.0,
                    "margin_used": 200.0,
                }
            ],
            [],
        )
        self.assertEqual(rows[0]["decision"], "blocked")
        events = service._protective_shadow_events  # type: ignore[attr-defined]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "margin_reduce_candidate")
        self.assertEqual(events[0]["symbol"], "BTCUSDT")
        self.assertGreater(float(events[0]["planned_reduce_usd"]), 0.0)

    async def test_automation_worker_runs_only_grid(self) -> None:
        service = DataService(settings_manager=self.manager)
        calls: list[str] = []

        async def _record(name: str) -> None:
            calls.append(name)

        service._auto_arb_cycle = lambda: _record("grid")  # type: ignore[assignment]

        await service._automation_cycle()  # type: ignore[attr-defined]

        self.assertEqual(calls, ["grid"])

    def test_account_state_caches_grouping_when_snapshot_unchanged(self) -> None:
        service = DataService(settings_manager=self.manager)

        class _FakeAccounts:
            def snapshot(self) -> dict:
                return {
                    "positions": [
                        {
                            "exchange": "binance",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "long",
                        }
                    ],
                    "balances": [],
                    "status": [],
                    "last_updated": "2026-04-02T10:00:00+00:00",
                }

        calls = {"positions_by_symbol": 0}

        def _positions_by_symbol(positions, return_grouped=False, market_lookup=None, market_ts_lookup=None):
            calls["positions_by_symbol"] += 1
            return ([], {}) if return_grouped else []

        service._accounts = _FakeAccounts()  # type: ignore[attr-defined]
        service._positions_market_snapshot_lookup = lambda: ({}, {})  # type: ignore[attr-defined]
        service._positions_by_symbol = _positions_by_symbol  # type: ignore[attr-defined]
        service._positions_market_state = lambda positions=None: {"status": []}  # type: ignore[attr-defined]

        service._account_state()
        service._account_state()
        self.assertEqual(calls["positions_by_symbol"], 1)


if __name__ == "__main__":
    unittest.main()
