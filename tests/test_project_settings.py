from __future__ import annotations

import json
import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from execution.storage import JsonStateStore, JsonlEventStore
from project_settings import SettingsManager
from webapp.services import (
    DataService,
    _auto_exit_pending_continuation_mode,
    _auto_exit_partial_progress_rebind_mode,
    _auto_exit_position_signature,
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
            patch("webapp.services.AUTO_STRATEGY_STATE_PATH", state_dir / "auto_strategies.json"),
            patch("webapp.services.AUTO_STRATEGY_HISTORY_PATH", log_dir / "auto_strategy_history.jsonl"),
            patch("webapp.services.AUTO_EXIT_STATE_PATH", state_dir / "auto_exit_rules.json"),
            patch("webapp.services.AUTO_EXIT_HISTORY_PATH", log_dir / "auto_exit_history.jsonl"),
            patch("webapp.services.HEDGE_CLUSTER_STATE_PATH", state_dir / "hedge_clusters.json"),
            patch("webapp.services.DERISK_HISTORY_PATH", log_dir / "derisk_history.jsonl"),
            patch("webapp.services.DERISK_OUTCOME_STATE_PATH", state_dir / "derisk_outcome_state.json"),
            patch(
                "webapp.services.PROTECTIVE_SHADOW_HISTORY_PATH",
                log_dir / "protective_shadow_history.jsonl",
            ),
            patch("execution.wallet.WalletService.DEFAULT_STATE_PATH", state_dir / "wallet_state.json"),
            patch(
                "webapp.services.DataService._derisk_market_preflight",
                new_callable=AsyncMock,
                return_value={
                    "eligible": True,
                    "reason": "ok",
                    "errors": [],
                    "min_qty_required": 0.001,
                    "amount_step": 0.001,
                    "min_notional": 1.0,
                    "checked_at": "2026-06-14T00:00:00+00:00",
                },
            ),
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

    def test_manual_auto_exit_policy_persists(self) -> None:
        self.manager.update(
            {
                "manual": {
                    "auto_exit_policy": {
                        "tier1": {
                            "chunk_notional_cap_usd": 31000.0,
                            "market_cleanup_notional_cap_usd": 17000.0,
                            "edge_buffer_bps": 1.5,
                        }
                    }
                }
            }
        )
        reloaded = SettingsManager(path=self.settings_path)
        manual = reloaded.current.manual
        policy = manual.get("auto_exit_policy") or {}
        tier1 = policy.get("tier1") or {}
        self.assertAlmostEqual(float(tier1.get("chunk_notional_cap_usd", 0.0)), 31000.0)
        self.assertAlmostEqual(float(tier1.get("market_cleanup_notional_cap_usd", 0.0)), 17000.0)
        self.assertAlmostEqual(float(tier1.get("edge_buffer_bps", 0.0)), 1.5)
        self.assertIn("tier2", policy)
        self.assertIn("lower_tier", policy)

    def test_legacy_auto_exit_chunk_caps_migrate_to_live_defaults(self) -> None:
        self.manager.update(
            {
                "manual": {
                    "auto_exit_policy": {
                        "tier1": {"chunk_notional_cap_usd": 350.0},
                        "tier2": {"chunk_notional_cap_usd": 250.0},
                        "lower_tier": {"chunk_notional_cap_usd": 150.0},
                    }
                }
            }
        )

        policy = self.manager.current.manual["auto_exit_policy"]
        self.assertEqual(policy["tier1"]["chunk_notional_cap_usd"], 750.0)
        self.assertEqual(policy["tier2"]["chunk_notional_cap_usd"], 500.0)
        self.assertEqual(policy["lower_tier"]["chunk_notional_cap_usd"], 250.0)

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

    async def test_auto_exit_cycle_populates_running_diagnostics(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "binance",
                            "symbol": "BTCUSDT",
                            "side": "long",
                            "quantity": 1.0,
                        }
                    ]
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._manual_runs = {  # type: ignore[attr-defined]
            "run-1": {"status": "running", "action": "exit"}
        }
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "rule-1": {
                    "enabled": True,
                    "v1_enabled": True,
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "target_spread_pct": 0.5,
                }
            },
        }

        await service._auto_exit_cycle()

        diagnostics = service._auto_exit_diagnostics  # type: ignore[attr-defined]
        v1_diagnostics = service._auto_exit_v1_diagnostics  # type: ignore[attr-defined]
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["status"], "running")
        self.assertEqual(diagnostics[0]["reason"], "execution_running")
        self.assertEqual(len(v1_diagnostics), 1)
        self.assertEqual(v1_diagnostics[0]["status"], "running")
        self.assertEqual(v1_diagnostics[0]["reason"], "execution_running")

    async def test_auto_exit_cycle_populates_shadow_v1_diagnostics_when_disabled(self) -> None:
        service = DataService(settings_manager=self.manager)
        next_funding = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": [{"symbol": "BTCUSDT"}]}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {
                    "BTCUSDT": [
                        {
                            "exchange": "binance",
                            "side": "long",
                            "quantity": 1.0,
                            "amount": 100.0,
                            "entry_price": 100.0,
                            "mark_price": 100.0,
                            "expected_funding": -0.01,
                            "funding_interval_hours": 8.0,
                            "next_funding": next_funding,
                        },
                        {
                            "exchange": "okx",
                            "side": "short",
                            "quantity": -1.0,
                            "amount": 100.0,
                            "entry_price": 99.0,
                            "mark_price": 98.5,
                            "expected_funding": 0.03,
                            "funding_interval_hours": 8.0,
                            "next_funding": next_funding,
                        },
                    ]
                },
            )
        )
        service._market_data = type(  # type: ignore[attr-defined]
            "MD",
            (),
            {
                "get_orderbook": staticmethod(
                    lambda exchange, symbol, depth=20, max_age_sec=15.0: _shadow_book(exchange, symbol)
                )
            },
        )()

        async def _shadow_book(exchange, symbol):
            if str(exchange).lower() == "binance":
                return {"bids": [[100.0, 5.0]], "asks": [[100.2, 5.0]]}
            return {"bids": [[98.7, 5.0]], "asks": [[99.0, 5.0]]}

        async def _manual_exit_should_not_run(payload):
            raise AssertionError("manual_exit should not run for shadow v1 diagnostics")

        service.manual_exit = _manual_exit_should_not_run  # type: ignore[assignment]
        signature_legs = service._positions_by_symbol(  # type: ignore[attr-defined]
            [{"symbol": "BTCUSDT"}],
            return_grouped=True,
        )[1]["BTCUSDT"]
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "rule-1": {
                    "enabled": True,
                    "v1_enabled": False,
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "target_spread_pct": 5.0,
                    "position_signature": _auto_exit_position_signature(
                        "BTCUSDT",
                        signature_legs,
                        rule_long_exchange="binance",
                        rule_short_exchange="okx",
                    ),
                }
            },
        }

        await service._auto_exit_cycle()

        diagnostics = service._auto_exit_v1_diagnostics  # type: ignore[attr-defined]
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["status"], "shadow")
        self.assertEqual(diagnostics[0]["decision"], "exit")
        self.assertEqual(diagnostics[0]["reason"], "take_profit_multiple")

    def test_protective_issue_kind_detects_auth_errors(self) -> None:
        self.assertEqual(
            _protective_issue_kind(
                'binanceusdm {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action"}'
            ),
            "auth_error",
        )

    def test_auto_exit_payload_exposes_diagnostics(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_diagnostics = [  # type: ignore[attr-defined]
            {
                "symbol": "BTCUSDT",
                "rule_long_exchange": "binance",
                "rule_short_exchange": "okx",
                "status": "wait",
                "policy_key": "tier2",
            }
        ]
        payload = service.auto_exit_payload()
        diagnostics = payload.get("diagnostics") or []
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["symbol"], "BTCUSDT")
        self.assertEqual(diagnostics[0]["policy_key"], "tier2")

    def test_auto_exit_payload_exposes_v1_diagnostics(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_v1_diagnostics = [  # type: ignore[attr-defined]
            {
                "symbol": "BTCUSDT",
                "status": "hold",
                "decision": "hold",
                "window_stage": "watch",
            }
        ]
        payload = service.auto_exit_payload()
        diagnostics = payload.get("v1_diagnostics") or []
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["symbol"], "BTCUSDT")
        self.assertEqual(diagnostics[0]["window_stage"], "watch")

    def test_latest_snapshot_dict_caches_serialization(self) -> None:
        service = DataService(settings_manager=self.manager)

        class _FakeSnapshot:
            def __init__(self, marker: str) -> None:
                self.marker = marker
                self.calls = 0

            def as_dict(self) -> dict[str, object]:
                self.calls += 1
                return {"marker": self.marker}

        first = _FakeSnapshot("first")
        service._snapshot = first  # type: ignore[attr-defined]
        self.assertEqual(service.latest_snapshot_dict(), {"marker": "first"})
        self.assertEqual(service.latest_snapshot_dict(), {"marker": "first"})
        self.assertEqual(first.calls, 1)

        second = _FakeSnapshot("second")
        service._snapshot = second  # type: ignore[attr-defined]
        self.assertEqual(service.latest_snapshot_dict(), {"marker": "second"})
        self.assertEqual(second.calls, 1)

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
        service._last_refreshed = datetime.now(timezone.utc)  # type: ignore[attr-defined]
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
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "BTCUSDT|binance|okx": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "target_spread_pct": -0.5,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        }
        service._auto_exit_live_spreads = {  # type: ignore[attr-defined]
            "BTCUSDT|binance|okx": -0.25
        }
        service._auto_exit_diagnostics = [  # type: ignore[attr-defined]
            {
                "key": "BTCUSDT|binance|okx",
                "status": "wait",
                "reason": "target_not_met",
            }
        ]

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
        self.assertEqual(card["auto_exit"]["status"], "waiting")
        self.assertAlmostEqual(card["auto_exit"]["live_spread_pct"], -0.25)
        self.assertAlmostEqual(card["auto_exit"]["exit_percent"], 100.0)
        self.assertAlmostEqual(card["position_summary"]["hedged_quantity"], 1.0)
        self.assertAlmostEqual(card["position_summary"]["imbalance_quantity"], 0.0)
        self.assertEqual(len(card["legs"]), 2)

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

    async def test_update_auto_exit_rule_supports_v1_only_experiment(self) -> None:
        service = DataService(settings_manager=self.manager)
        result = await service.update_auto_exit_rule(
            {
                "symbol": "BTCUSDT",
                "long_exchange": "binance",
                "short_exchange": "okx",
                "enabled": False,
                "spread_enabled": False,
                "v1_enabled": True,
            }
        )
        rules = ((result.get("auto_exit") or {}).get("rules") or {})
        rule = rules.get("BTCUSDT|binance|okx") or {}
        self.assertFalse(rule.get("enabled"))
        self.assertTrue(rule.get("v1_enabled"))
        self.assertIsNone(rule.get("target_spread_pct"))
        self.assertTrue(rule.get("persist_on_missing"))

    async def test_update_auto_exit_rule_persists_partial_one_shot_settings(self) -> None:
        service = DataService(settings_manager=self.manager)
        result = await service.update_auto_exit_rule(
            {
                "symbol": "LABUSDT",
                "long_exchange": "binance",
                "short_exchange": "kucoin",
                "spread_enabled": True,
                "target_spread_pct": 0.4,
                "exit_percent": 50,
                "exit_once": True,
            }
        )
        rule = ((result.get("auto_exit") or {}).get("rules") or {}).get(
            "LABUSDT|binance|kucoin"
        ) or {}
        self.assertAlmostEqual(rule.get("exit_percent"), 50.0)
        self.assertTrue(rule.get("exit_once"))

    async def test_update_hedge_cluster_rule_supports_standalone(self) -> None:
        service = DataService(settings_manager=self.manager)
        result = await service.update_hedge_cluster_rule(
            {
                "symbol": "BTCUSDT",
                "kind": "standalone",
                "exchange": "gate",
                "enabled": True,
            }
        )
        rules = ((result.get("hedge_clusters") or {}).get("rules") or {})
        self.assertTrue(any(rule.get("kind") == "standalone" for rule in rules.values()))

    async def test_auto_exit_cycle_keeps_persistent_rule_when_position_temporarily_missing(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": []}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: ([], {})
        )
        missing_since = datetime.now(timezone.utc).timestamp() - 600.0
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {"auto_clear_no_position_sec": 60},
            "rules": {
                "BTCUSDT|binance|okx": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "persist_on_missing": True,
                    "target_spread_pct": -0.5,
                    "missing_since_ts": missing_since,
                }
            },
        }

        await service._auto_exit_cycle()

        rules = (service._auto_exit.get("rules") or {})  # type: ignore[attr-defined]
        rule = rules.get("BTCUSDT|binance|okx") or {}
        self.assertTrue(rule)
        self.assertTrue(rule.get("persist_on_missing"))
        self.assertGreater(float(rule.get("missing_since_ts") or 0.0), 0.0)

    async def test_auto_exit_cycle_clears_missing_rule_when_restore_disabled(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        service._cleanup_verified_orphan_protective_targets = AsyncMock(return_value=[])  # type: ignore[method-assign]
        checked_at = datetime.now(timezone.utc).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [],
                    "status": [
                        {
                            "exchange": exchange,
                            "status": "ok",
                            "positions_fetch_ok": True,
                            "positions_count": 0,
                            "checked_at": checked_at,
                        }
                        for exchange in ("binance", "okx")
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: ([], {})
        )
        missing_since = datetime.now(timezone.utc).timestamp() - 600.0
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {
                "auto_clear_no_position_sec": 60,
                "restore_spread_on_missing": False,
            },
            "rules": {
                "BTCUSDT|binance|okx": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "persist_on_missing": True,
                    "target_spread_pct": -0.5,
                    "missing_since_ts": missing_since,
                    "verified_missing_count": 1,
                    "verified_missing_evidence_id": "older-scan",
                }
            },
        }

        await service._auto_exit_cycle()

        rules = (service._auto_exit.get("rules") or {})  # type: ignore[attr-defined]
        self.assertNotIn("BTCUSDT|binance|okx", rules)

    async def test_auto_exit_cycle_clears_persistent_rule_after_verified_absence(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit_verified.json")  # type: ignore[attr-defined]
        cleanup = AsyncMock(return_value=[])
        service._cleanup_verified_orphan_protective_targets = cleanup  # type: ignore[method-assign]
        checked_at = datetime.now(timezone.utc).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [],
                    "status": [
                        {
                            "exchange": exchange,
                            "status": "ok",
                            "positions_fetch_ok": True,
                            "positions_count": 0,
                            "checked_at": checked_at,
                        }
                        for exchange in ("binance", "okx")
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: ([], {})
        )
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {
                "auto_clear_no_position_sec": 60,
                "restore_spread_on_missing": True,
                "clear_verified_missing": True,
                "verified_missing_confirmations": 2,
            },
            "rules": {
                "BTCUSDT|binance|okx": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "persist_on_missing": True,
                    "target_spread_pct": -0.5,
                    "missing_since_ts": datetime.now(timezone.utc).timestamp() - 600.0,
                    "verified_missing_count": 1,
                    "verified_missing_evidence_id": "older-scan",
                }
            },
        }

        await service._auto_exit_cycle()

        rules = (service._auto_exit.get("rules") or {})  # type: ignore[attr-defined]
        self.assertNotIn("BTCUSDT|binance|okx", rules)
        cleanup.assert_awaited_once()

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

    async def test_clear_auto_exit_spread_cache_preserves_v1_only_rule(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "BTCUSDT|binance|okx": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "persist_on_missing": True,
                    "target_spread_pct": -0.5,
                },
                "BTCUSDT|kucoin|binance": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "kucoin",
                    "short_exchange": "binance",
                    "enabled": True,
                    "v1_enabled": True,
                    "persist_on_missing": True,
                    "target_spread_pct": 0.25,
                },
                "ETHUSDT|binance|okx": {
                    "symbol": "ETHUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "persist_on_missing": True,
                    "target_spread_pct": -0.1,
                },
            },
        }

        result = await service.clear_auto_exit_spread_cache("BTCUSDT")

        rules = (service._auto_exit.get("rules") or {})  # type: ignore[attr-defined]
        self.assertEqual(result.get("removed"), 1)
        self.assertEqual(result.get("disabled"), 1)
        self.assertNotIn("BTCUSDT|binance|okx", rules)
        preserved = rules.get("BTCUSDT|kucoin|binance") or {}
        self.assertFalse(preserved.get("enabled"))
        self.assertTrue(preserved.get("v1_enabled"))
        self.assertIsNone(preserved.get("target_spread_pct"))
        self.assertIn("ETHUSDT|binance|okx", rules)

    async def test_clear_auto_exit_symbol_removes_v1_and_multileg_rules(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "LABUSDT|binance|kucoin": {
                    "symbol": "LABUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "enabled": True,
                    "v1_enabled": True,
                    "target_spread_pct": -0.5,
                },
                "LABUSDT|multileg|multileg": {
                    "symbol": "LABUSDT",
                    "long_exchange": "multileg",
                    "short_exchange": "multileg",
                    "enabled": True,
                    "v1_enabled": False,
                    "target_spread_pct": -1.0,
                },
                "ETHUSDT|binance|okx": {
                    "symbol": "ETHUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": True,
                    "target_spread_pct": 0.2,
                },
            },
        }

        result = await service.clear_auto_exit_spread_cache("LABUSDT", clear_v1=True)

        rules = (service._auto_exit.get("rules") or {})  # type: ignore[attr-defined]
        self.assertEqual(result.get("removed"), 2)
        self.assertEqual(result.get("cleared_v1"), 1)
        self.assertFalse(any("LABUSDT" in key for key in rules))
        self.assertIn("ETHUSDT|binance|okx", rules)

    async def test_completed_auto_exit_preserves_rule_while_position_restore_is_uncertain(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": []}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: ([], {})
        )
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "BTCUSDT|binance|okx": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": True,
                    "target_spread_pct": -0.5,
                    "rule_generation": 1,
                },
                "ETHUSDT|binance|okx": {
                    "symbol": "ETHUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "target_spread_pct": -0.1,
                },
            },
        }
        service._manual_runs = {  # type: ignore[attr-defined]
            "auto-exit-1": {
                "action": "exit",
                "status": "completed",
                "auto_exit_agent": True,
                "payload_symbol": "BTCUSDT",
                "auto_exit_rule_key": "BTCUSDT|binance|okx",
                "auto_exit_rule_generation": 1,
                "result": {},
            }
        }

        await service._cleanup_completed_auto_exit_spread_rules()

        rules = (service._auto_exit.get("rules") or {})  # type: ignore[attr-defined]
        preserved = rules.get("BTCUSDT|binance|okx") or {}
        self.assertTrue(preserved.get("enabled"))
        self.assertTrue(preserved.get("v1_enabled"))
        self.assertEqual(
            preserved.get("signature_status"),
            "awaiting_position_restore_after_auto_exit",
        )
        self.assertIn("ETHUSDT|binance|okx", rules)
        self.assertIn("auto-exit-1", service._auto_exit_completed_run_cleanup)  # type: ignore[attr-defined]

    async def test_completed_auto_exit_rebinds_rule_to_residual_pair(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        residual_legs = [
            {
                "exchange": "kucoin",
                "side": "long",
                "quantity": 25000.0,
                "amount": 150.0,
                "entry_price": 0.00646,
                "mark_price": 0.0061,
            },
            {
                "exchange": "binance",
                "side": "short",
                "quantity": -27000.0,
                "amount": 165.0,
                "entry_price": 0.006359,
                "mark_price": 0.0061,
            },
        ]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": [{"symbol": "GUNUSDT"}]}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"GUNUSDT": residual_legs},
            )
        )
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "GUNUSDT|kucoin|binance": {
                    "symbol": "GUNUSDT",
                    "long_exchange": "kucoin",
                    "short_exchange": "binance",
                    "enabled": False,
                    "v1_enabled": True,
                    "target_spread_pct": None,
                    "rule_generation": 4,
                },
            },
        }
        service._manual_runs = {  # type: ignore[attr-defined]
            "gun-exit": {
                "action": "exit",
                "status": "completed",
                "auto_exit_agent": True,
                "payload_symbol": "GUNUSDT",
                "auto_exit_rule_key": "GUNUSDT|kucoin|binance",
                "auto_exit_rule_generation": 4,
                "result": {},
            }
        }

        await service._cleanup_completed_auto_exit_spread_rules()

        rule = (service._auto_exit.get("rules") or {}).get("GUNUSDT|kucoin|binance") or {}  # type: ignore[attr-defined]
        self.assertTrue(rule.get("v1_enabled"))
        self.assertEqual(rule.get("signature_status"), "rebound_after_partial_auto_exit")
        signature = rule.get("position_signature") or {}
        self.assertEqual(len(signature.get("legs") or []), 2)

    async def test_completed_auto_exit_cannot_clear_newer_checkbox_generation(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": []}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: ([], {})
        )
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "GUNUSDT|kucoin|binance": {
                    "symbol": "GUNUSDT",
                    "long_exchange": "kucoin",
                    "short_exchange": "binance",
                    "enabled": False,
                    "v1_enabled": True,
                    "target_spread_pct": None,
                    "rule_generation": 2,
                    "signature_status": "bound",
                },
            },
        }
        service._manual_runs = {  # type: ignore[attr-defined]
            "old-gun-exit": {
                "action": "exit",
                "status": "completed",
                "auto_exit_agent": True,
                "payload_symbol": "GUNUSDT",
                "auto_exit_rule_key": "GUNUSDT|kucoin|binance",
                "auto_exit_rule_generation": 1,
                "result": {},
            }
        }

        await service._cleanup_completed_auto_exit_spread_rules()

        rule = (service._auto_exit.get("rules") or {}).get("GUNUSDT|kucoin|binance") or {}  # type: ignore[attr-defined]
        self.assertTrue(rule.get("v1_enabled"))
        self.assertEqual(rule.get("rule_generation"), 2)
        self.assertEqual(rule.get("signature_status"), "bound")

    async def test_partial_one_shot_auto_exit_keeps_v1_enabled(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        residual_legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 80.0,
                "amount": 1040.0,
                "entry_price": 12.991,
                "mark_price": 13.0,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -90.0,
                "amount": 1170.0,
                "entry_price": 12.79216,
                "mark_price": 13.0,
            },
        ]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": [{"symbol": "LABUSDT"}]}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"LABUSDT": residual_legs},
            )
        )
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "LABUSDT|binance|kucoin": {
                    "symbol": "LABUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "enabled": True,
                    "v1_enabled": True,
                    "target_spread_pct": 0.4,
                    "exit_once": True,
                    "rule_generation": 2,
                },
            },
        }
        service._manual_runs = {  # type: ignore[attr-defined]
            "lab-partial-exit": {
                "action": "exit",
                "status": "completed",
                "auto_exit_agent": True,
                "payload_symbol": "LABUSDT",
                "auto_exit_rule_key": "LABUSDT|binance|kucoin",
                "auto_exit_rule_generation": 2,
                "auto_exit_exit_percent": 50.0,
                "auto_exit_hedged_qty": 150.0,
                "result": {"remaining_qty": 5.0},
            }
        }

        await service._cleanup_completed_auto_exit_spread_rules()

        rule = (service._auto_exit.get("rules") or {}).get("LABUSDT|binance|kucoin") or {}  # type: ignore[attr-defined]
        self.assertTrue(rule.get("enabled"))
        self.assertTrue(rule.get("v1_enabled"))
        self.assertEqual(rule.get("signature_status"), "rebound_after_partial_auto_exit")

    async def test_partial_auto_exit_refreshes_accounts_before_residual_rebind(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        stale_legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 25000.0,
                "entry_price": 0.054352924,
                "mark_price": 0.0426,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -25000.0,
                "entry_price": 0.0555,
                "mark_price": 0.04308,
            },
        ]
        fresh_legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 19169.0,
                "entry_price": 0.054352924,
                "mark_price": 0.0426,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -19160.0,
                "entry_price": 0.0555,
                "mark_price": 0.04308,
            },
        ]

        class Accounts:
            def __init__(self) -> None:
                self.refreshed = False

            async def refresh_now_for_protective(self, *, force_env: bool = False) -> None:
                self.refreshed = force_env

            def snapshot(self) -> dict:
                return {"positions": fresh_legs if self.refreshed else stale_legs}

        accounts = Accounts()
        service._accounts = accounts  # type: ignore[attr-defined]
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"SIRENUSDT": positions},
            )
        )
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "SIRENUSDT|binance|kucoin": {
                    "symbol": "SIRENUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "enabled": True,
                    "v1_enabled": True,
                    "target_spread_pct": -1.3,
                    "exit_once": True,
                    "rule_generation": 5,
                },
            },
        }
        service._manual_runs = {  # type: ignore[attr-defined]
            "siren-partial-exit": {
                "action": "exit",
                "status": "completed",
                "auto_exit_agent": True,
                "payload_symbol": "SIRENUSDT",
                "auto_exit_rule_key": "SIRENUSDT|binance|kucoin",
                "auto_exit_rule_generation": 5,
                "auto_exit_trigger_mode": "spread",
                "auto_exit_exit_percent": 100.0,
                "auto_exit_hedged_qty": 25000.0,
                "auto_exit_requested_qty": 25000.0,
                "result": {"remaining_qty": 19160.0},
            }
        }

        await service._cleanup_completed_auto_exit_spread_rules()

        rule = (service._auto_exit.get("rules") or {}).get("SIRENUSDT|binance|kucoin") or {}  # type: ignore[attr-defined]
        signature = rule.get("position_signature") or {}
        quantities = sorted(float(item.get("qty") or 0.0) for item in signature.get("legs") or [])
        self.assertTrue(accounts.refreshed)
        self.assertEqual(quantities, [19160.0, 19169.0])
        self.assertEqual(rule.get("spread_remaining_qty"), 19160.0)
        self.assertIn("siren-partial-exit", service._auto_exit_completed_run_cleanup)  # type: ignore[attr-defined]

    async def test_partial_auto_exit_retries_reconciliation_after_refresh_failure(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._accounts.refresh_now_for_protective = AsyncMock(side_effect=RuntimeError("offline"))  # type: ignore[attr-defined]
        service._manual_runs = {  # type: ignore[attr-defined]
            "partial-exit": {
                "action": "exit",
                "status": "completed",
                "auto_exit_agent": True,
                "payload_symbol": "SIRENUSDT",
                "auto_exit_rule_key": "SIRENUSDT|binance|kucoin",
                "auto_exit_rule_generation": 1,
                "result": {"remaining_qty": 10.0},
            }
        }

        await service._cleanup_completed_auto_exit_spread_rules()

        self.assertNotIn("partial-exit", service._auto_exit_completed_run_cleanup)  # type: ignore[attr-defined]
        self.assertGreater(
            float(service._manual_runs["partial-exit"].get("auto_exit_reconcile_retry_after_ts") or 0.0),  # type: ignore[attr-defined]
            0.0,
        )

    def test_partial_progress_signature_can_rebind_only_expected_residual(self) -> None:
        old_legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 25000.0,
                "entry_price": 0.054352924,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -25000.0,
                "entry_price": 0.0555,
            },
        ]
        current_legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 19169.0,
                "entry_price": 0.054352924,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -19160.0,
                "entry_price": 0.0555,
            },
        ]
        old_signature = _auto_exit_position_signature(
            "SIRENUSDT",
            old_legs,
            rule_long_exchange="binance",
            rule_short_exchange="kucoin",
        )
        current_signature = _auto_exit_position_signature(
            "SIRENUSDT",
            current_legs,
            rule_long_exchange="binance",
            rule_short_exchange="kucoin",
        )
        rule = {
            "position_signature": old_signature,
            "spread_target_qty": 25000.0,
            "spread_remaining_qty": 19160.0,
        }

        self.assertEqual(
            _auto_exit_partial_progress_rebind_mode(rule, current_signature, 19160.0),
            "spread",
        )
        self.assertIsNone(
            _auto_exit_partial_progress_rebind_mode(rule, current_signature, 18000.0),
        )

    def test_partial_spread_continuation_owns_residual_before_v1(self) -> None:
        rule = {
            "spread_target_qty": 25000.0,
            "spread_remaining_qty": 19160.0,
            "v1_target_qty": 19160.0,
            "v1_remaining_qty": 19040.0,
        }

        self.assertEqual(_auto_exit_pending_continuation_mode(rule), "spread")

    def test_completed_or_dust_target_does_not_lock_continuation(self) -> None:
        rule = {
            "spread_target_qty": 25000.0,
            "spread_remaining_qty": 200.0,
            "v1_target_qty": None,
            "v1_remaining_qty": None,
        }

        self.assertIsNone(_auto_exit_pending_continuation_mode(rule))

    def test_explicit_continuation_owner_survives_zero_fill(self) -> None:
        rule = {
            "continuation_trigger_mode": "spread",
            "spread_target_qty": 25000.0,
            "spread_remaining_qty": 25000.0,
        }

        self.assertEqual(_auto_exit_pending_continuation_mode(rule), "spread")

    async def test_fulfilled_one_shot_auto_exit_disarms_rule_after_completion(self) -> None:
        service = DataService(settings_manager=self.manager)
        service._auto_exit_store = JsonStateStore(Path(self.tmp_dir.name) / "auto_exit.json")  # type: ignore[attr-defined]
        residual_legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 75.0,
                "amount": 975.0,
                "entry_price": 12.991,
                "mark_price": 13.0,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -85.0,
                "amount": 1105.0,
                "entry_price": 12.79216,
                "mark_price": 13.0,
            },
        ]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": [{"symbol": "LABUSDT"}]}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"LABUSDT": residual_legs},
            )
        )
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {},
            "rules": {
                "LABUSDT|binance|kucoin": {
                    "symbol": "LABUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "kucoin",
                    "enabled": True,
                    "v1_enabled": True,
                    "target_spread_pct": 0.4,
                    "exit_once": True,
                    "rule_generation": 2,
                },
            },
        }
        service._manual_runs = {  # type: ignore[attr-defined]
            "lab-complete-exit": {
                "action": "exit",
                "status": "completed",
                "auto_exit_agent": True,
                "payload_symbol": "LABUSDT",
                "auto_exit_rule_key": "LABUSDT|binance|kucoin",
                "auto_exit_rule_generation": 2,
                "auto_exit_exit_percent": 50.0,
                "auto_exit_hedged_qty": 150.0,
                "result": {"remaining_qty": 0.0},
            }
        }

        await service._cleanup_completed_auto_exit_spread_rules()

        rule = (service._auto_exit.get("rules") or {}).get("LABUSDT|binance|kucoin") or {}  # type: ignore[attr-defined]
        self.assertFalse(rule.get("enabled"))
        self.assertFalse(rule.get("v1_enabled"))
        self.assertEqual(rule.get("signature_status"), "one_shot_completed")

    async def test_auto_exit_cycle_blocks_unbound_legacy_rule(self) -> None:
        service = DataService(settings_manager=self.manager)
        legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 1.0,
                "amount": 100.0,
                "entry_price": 100.0,
                "mark_price": 100.0,
            },
            {
                "exchange": "okx",
                "side": "short",
                "quantity": -1.0,
                "amount": 100.0,
                "entry_price": 99.0,
                "mark_price": 99.0,
            },
        ]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": [{"symbol": "BTCUSDT"}]}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(legs)},
            )
        )

        async def _book(exchange, symbol, depth=20, max_age_sec=15.0):  # noqa: ANN001
            if str(exchange).lower() == "binance":
                return {"bids": [[101.0, 5.0]], "asks": [[101.2, 5.0]]}
            return {"bids": [[98.0, 5.0]], "asks": [[98.5, 5.0]]}

        service._market_data = type("MD", (), {"get_orderbook": staticmethod(_book)})()  # type: ignore[attr-defined]
        calls: list[dict[str, object]] = []

        async def _manual_exit(payload):
            calls.append(dict(payload))
            return {"execution_id": "should-not-run", "status": "running"}

        service.manual_exit = _manual_exit  # type: ignore[assignment]
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {"require_live": True},
            "rules": {
                "BTCUSDT|binance|okx": {
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "v1_enabled": False,
                    "target_spread_pct": -1.0,
                    "position_mode": "strict_signature",
                }
            },
        }

        await service._auto_exit_cycle()

        self.assertEqual(calls, [])
        diagnostics = service._auto_exit_diagnostics  # type: ignore[attr-defined]
        self.assertEqual(diagnostics[0]["reason"], "unbound_position_signature")

    async def test_auto_exit_current_balanced_ignores_qty_signature_and_uses_balanced_qty(self) -> None:
        service = DataService(settings_manager=self.manager)
        old_legs = [
            {
                "exchange": "bybit",
                "side": "long",
                "quantity": 5_000_000.0,
                "amount": 1700.0,
                "entry_price": 0.00032854,
                "mark_price": 0.0003462,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -5_000_000.0,
                "amount": 1600.0,
                "entry_price": 0.0003199,
                "mark_price": 0.0003404,
            },
        ]
        current_legs = [
            {
                "exchange": "bybit",
                "side": "long",
                "quantity": 15_000_000.0,
                "amount": 5850.0,
                "entry_price": 0.00037367,
                "mark_price": 0.0003900,
            },
            {
                "exchange": "kucoin",
                "side": "short",
                "quantity": -10_000_000.0,
                "amount": 3500.0,
                "entry_price": 0.0003355,
                "mark_price": 0.0003500,
            },
        ]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": [{"symbol": "BLASTUSDT"}]}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BLASTUSDT": list(current_legs)},
            )
        )

        async def _book(exchange, symbol, depth=20, max_age_sec=15.0):  # noqa: ANN001
            if str(exchange).lower() == "bybit":
                return {"bids": [[0.000389, 20_000_000.0]], "asks": [[0.000390, 20_000_000.0]]}
            return {"bids": [[0.000349, 20_000_000.0]], "asks": [[0.000350, 20_000_000.0]]}

        service._market_data = type("MD", (), {"get_orderbook": staticmethod(_book)})()  # type: ignore[attr-defined]
        calls: list[dict[str, object]] = []

        async def _manual_exit(payload):
            calls.append(dict(payload))
            return {"execution_id": "auto-exit-blast", "status": "running"}

        service.manual_exit = _manual_exit  # type: ignore[assignment]
        service._auto_exit = {  # type: ignore[attr-defined]
            "defaults": {
                "require_live": True,
                "position_mode": "current_balanced",
                "spread_confirm_cycles": 1,
            },
            "rules": {
                "BLASTUSDT|bybit|kucoin": {
                    "symbol": "BLASTUSDT",
                    "long_exchange": "bybit",
                    "short_exchange": "kucoin",
                    "enabled": True,
                    "v1_enabled": False,
                    "target_spread_pct": 8.0,
                    "exit_percent": 100.0,
                    "exit_once": True,
                    "position_mode": "current_balanced",
                    "spread_confirm_cycles": 1,
                    "position_signature": _auto_exit_position_signature(
                        "BLASTUSDT",
                        old_legs,
                        rule_long_exchange="bybit",
                        rule_short_exchange="kucoin",
                    ),
                }
            },
        }

        await service._auto_exit_cycle()

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["qty"], 10_000_000.0)
        self.assertEqual(calls[0]["long_exchange"], "bybit")
        self.assertEqual(calls[0]["short_exchange"], "kucoin")
        self.assertEqual(calls[0]["auto_exit_total_target_qty"], 10_000_000.0)
        self.assertEqual(calls[0]["auto_exit_position_mode"], "current_balanced")

    async def test_update_auto_exit_rule_binds_current_position_signature(self) -> None:
        service = DataService(settings_manager=self.manager)
        legs = [
            {
                "exchange": "binance",
                "side": "long",
                "quantity": 1.0,
                "amount": 100.0,
                "entry_price": 100.0,
            },
            {
                "exchange": "okx",
                "side": "short",
                "quantity": -1.0,
                "amount": 100.0,
                "entry_price": 99.0,
            },
        ]
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {"snapshot": lambda self=None: {"positions": [{"symbol": "BTCUSDT"}]}},
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(legs)},
            )
        )

        result = await service.update_auto_exit_rule(
            {
                "symbol": "BTCUSDT",
                "long_exchange": "binance",
                "short_exchange": "okx",
                "spread_enabled": True,
                "target_spread_pct": -0.5,
            }
        )

        rule = ((result.get("auto_exit") or {}).get("rules") or {}).get("BTCUSDT|binance|okx") or {}
        self.assertEqual(rule.get("signature_status"), "bound")
        signature = rule.get("position_signature") or {}
        self.assertEqual(signature.get("symbol"), "BTCUSDT")
        self.assertEqual(len(signature.get("legs") or []), 2)

    async def test_derisk_cycle_blocks_orphan_when_exchange_health_is_untrusted(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": False,
                    "auto_derisk_shadow_mode": True,
                    "derisk_confirm_cycles": 2,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
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
                            "quantity": 2.0,
                            "coin_qty": 2.0,
                            "amount": 200.0,
                            "entry_price": 100.0,
                            "mark_price": 99.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                            "entry_price": 100.0,
                            "mark_price": 99.0,
                        }
                    ],
                    "balances": [],
                    "status": [
                        {
                            "exchange": "binance",
                            "status": "error",
                            "error": 'binanceusdm {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action"}',
                            "checked_at": now,
                        },
                        {
                            "exchange": "okx",
                            "status": "ok",
                            "checked_at": now,
                        },
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }

        await service._auto_derisk_cycle()

        rows = service._derisk_diagnostics  # type: ignore[attr-defined]
        cluster = next((row for row in rows if row.get("kind") == "cluster"), {})
        self.assertEqual(cluster.get("status"), "blocked_by_exchange_health")
        self.assertEqual(cluster.get("long_error_kind"), "auth_error")

    async def test_derisk_cycle_confirms_orphan_after_confirm_cycles(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": False,
                    "auto_derisk_shadow_mode": True,
                    "derisk_confirm_cycles": 2,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                            "entry_price": 100.0,
                            "mark_price": 99.0,
                        }
                    ],
                    "balances": [],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }

        await service._auto_derisk_cycle()
        await service._auto_derisk_cycle()

        rows = service._derisk_diagnostics  # type: ignore[attr-defined]
        cluster = next((row for row in rows if row.get("kind") == "cluster"), {})
        self.assertEqual(cluster.get("status"), "confirmed_orphan")
        self.assertEqual(cluster.get("missing_cycles"), 2)

    async def test_derisk_cycle_triggers_partial_manual_exit_for_stressed_pair(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "auto_derisk_shadow_mode": False,
                    "derisk_confirm_cycles": 2,
                    "derisk_max_single_action_notional_usd": 500.0,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
        next_funding = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
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
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 900.0,
                            "notional": 900.0,
                            "entry_price": 100.0,
                            "mark_price": 98.0,
                            "unrealized_pnl": -10.0,
                            "leverage": 3.0,
                            "expected_funding": 2.0,
                            "funding_interval_hours": 8.0,
                            "next_funding": next_funding,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 900.0,
                            "notional": 900.0,
                            "entry_price": 99.0,
                            "mark_price": 101.0,
                            "unrealized_pnl": 12.0,
                            "leverage": 3.0,
                            "expected_funding": 1.0,
                            "funding_interval_hours": 8.0,
                            "next_funding": next_funding,
                        },
                    ],
                    "balances": [
                        {
                            "exchange": "binance",
                            "asset": "USDT",
                            "total": 110.0,
                            "available": 5.0,
                            "used": 100.0,
                            "buffer_pct": 4.5,
                        },
                        {
                            "exchange": "okx",
                            "asset": "USDT",
                            "total": 400.0,
                            "available": 250.0,
                            "used": 100.0,
                            "buffer_pct": 62.5,
                        },
                    ],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }
        calls: list[dict[str, object]] = []

        async def _manual_exit(payload):
            calls.append(dict(payload))
            return {"execution_id": "derisk-1", "status": "accepted"}

        service.manual_exit = _manual_exit  # type: ignore[assignment]

        await service._auto_derisk_cycle()

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["symbol"], "BTCUSDT")
        self.assertEqual(calls[0]["long_exchange"], "binance")
        self.assertEqual(calls[0]["short_exchange"], "okx")
        self.assertTrue(float(calls[0]["qty"]) > 0.0)
        self.assertTrue(bool(calls[0]["risk_emergency_agent"]))
        self.assertFalse(bool(calls[0]["auto_exit_agent"]))
        rows = service._derisk_diagnostics  # type: ignore[attr-defined]
        candidate = next((row for row in rows if row.get("kind") == "candidate"), {})
        self.assertEqual(candidate.get("stress_status"), "panic")
        self.assertIn(candidate.get("residual_status"), {"closable_normal", "dust_suspect", "flat"})
        events = service._derisk_events  # type: ignore[attr-defined]
        self.assertTrue(any(item.get("event") == "trigger" for item in events))

    async def test_derisk_cycle_requests_preempt_when_auto_exit_running(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "auto_derisk_shadow_mode": False,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
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
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 900.0,
                            "notional": 900.0,
                            "entry_price": 100.0,
                            "mark_price": 98.0,
                            "unrealized_pnl": -5.0,
                            "leverage": 3.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 900.0,
                            "notional": 900.0,
                            "entry_price": 99.0,
                            "mark_price": 101.0,
                            "unrealized_pnl": 4.0,
                            "leverage": 3.0,
                        },
                    ],
                    "balances": [
                        {
                            "exchange": "binance",
                            "asset": "USDT",
                            "total": 110.0,
                            "available": 5.0,
                            "used": 100.0,
                            "buffer_pct": 4.5,
                        },
                        {
                            "exchange": "okx",
                            "asset": "USDT",
                            "total": 400.0,
                            "available": 250.0,
                            "used": 100.0,
                            "buffer_pct": 62.5,
                        },
                    ],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }
        service._manual_runs = {"exec-1": {"status": "running", "action": "exit"}}  # type: ignore[attr-defined]
        stop_calls: list[dict[str, object]] = []

        async def _manual_exec_stop(execution_id, **kwargs):
            stop_calls.append({"execution_id": str(execution_id), **kwargs})
            return {"ok": True}

        async def _manual_exit_should_not_run(payload):
            raise AssertionError("manual_exit should not run while another auto-exit execution is running")

        service.manual_exec_stop = _manual_exec_stop  # type: ignore[assignment]
        service.manual_exit = _manual_exit_should_not_run  # type: ignore[assignment]

        await service._auto_derisk_cycle()

        self.assertEqual(len(stop_calls), 1)
        self.assertEqual(stop_calls[0]["execution_id"], "exec-1")
        self.assertTrue(bool(stop_calls[0]["force_finalize"]))
        self.assertEqual(stop_calls[0]["reason"], "emergency_derisk_priority")
        events = service._derisk_events  # type: ignore[attr-defined]
        self.assertTrue(any(item.get("event") == "preempt_requested" for item in events))

    async def test_derisk_cycle_triggers_live_orphan_cleanup_for_confirmed_orphan(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "auto_derisk_shadow_mode": False,
                    "orphan_cleanup_enabled": True,
                    "derisk_confirm_cycles": 1,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                            "entry_price": 100.0,
                            "mark_price": 99.0,
                        }
                    ],
                    "balances": [],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }
        calls: list[dict[str, object]] = []

        async def _manual_orphan_cleanup(payload):
            calls.append(dict(payload))
            return {"execution_id": "orphan-1", "status": "accepted"}

        async def _manual_exit_should_not_run(payload):
            raise AssertionError("manual_exit should not run for confirmed orphan cleanup")

        service.manual_orphan_cleanup = _manual_orphan_cleanup  # type: ignore[assignment]
        service.manual_exit = _manual_exit_should_not_run  # type: ignore[assignment]

        await service._auto_derisk_cycle()

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["symbol"], "BTCUSDT")
        self.assertEqual(calls[0]["cleanup_exchange"], "okx")
        self.assertEqual(calls[0]["cleanup_position_side"], "short")
        self.assertAlmostEqual(float(calls[0]["qty"]), 1.0)
        self.assertTrue(bool(calls[0]["panic_cleanup_mode"]))
        self.assertTrue(bool(calls[0]["risk_emergency_agent"]))
        events = service._derisk_events  # type: ignore[attr-defined]
        self.assertTrue(any(item.get("event") == "orphan_trigger" for item in events))

    async def test_derisk_cycle_aggregates_duplicate_expected_legs_for_orphan_cleanup(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "auto_derisk_shadow_mode": False,
                    "orphan_cleanup_enabled": True,
                    "derisk_confirm_cycles": 1,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
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
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 0.6,
                            "coin_qty": 0.6,
                            "amount": 60.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 0.6,
                            "coin_qty": 0.6,
                            "amount": 60.0,
                        },
                    ],
                    "balances": [],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }
        calls: list[dict[str, object]] = []

        async def _manual_orphan_cleanup(payload):
            calls.append(dict(payload))
            return {"execution_id": "orphan-dup-1", "status": "accepted"}

        service.manual_orphan_cleanup = _manual_orphan_cleanup  # type: ignore[assignment]

        await service._auto_derisk_cycle()

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["cleanup_exchange"], "okx")
        self.assertEqual(calls[0]["cleanup_position_side"], "short")
        self.assertAlmostEqual(float(calls[0]["qty"]), 0.2, places=6)
        rows = service._derisk_diagnostics  # type: ignore[attr-defined]
        cluster = next((row for row in rows if row.get("kind") == "cluster"), {})
        self.assertEqual(cluster.get("status"), "confirmed_orphan")
        self.assertEqual(cluster.get("duplicate_visible_leg_count"), 1)
        self.assertAlmostEqual(float(cluster.get("orphan_qty") or 0.0), 0.2, places=6)

    async def test_derisk_cycle_preempts_running_exit_for_confirmed_orphan_with_force_finalize(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "auto_derisk_shadow_mode": False,
                    "orphan_cleanup_enabled": True,
                    "derisk_confirm_cycles": 1,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                            "entry_price": 100.0,
                            "mark_price": 99.0,
                        }
                    ],
                    "balances": [],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }
        service._manual_runs = {"exec-1": {"status": "running", "action": "exit"}}  # type: ignore[attr-defined]
        stop_calls: list[dict[str, object]] = []

        async def _manual_exec_stop(execution_id, **kwargs):
            stop_calls.append({"execution_id": str(execution_id), **kwargs})
            return {"ok": True}

        async def _manual_orphan_cleanup_should_not_run(payload):
            raise AssertionError("manual_orphan_cleanup should not run while another execution is being preempted")

        service.manual_exec_stop = _manual_exec_stop  # type: ignore[assignment]
        service.manual_orphan_cleanup = _manual_orphan_cleanup_should_not_run  # type: ignore[assignment]

        await service._auto_derisk_cycle()

        self.assertEqual(len(stop_calls), 1)
        self.assertEqual(stop_calls[0]["execution_id"], "exec-1")
        self.assertTrue(bool(stop_calls[0]["force_finalize"]))
        self.assertEqual(stop_calls[0]["reason"], "orphan_cleanup_priority")
        events = service._derisk_events  # type: ignore[attr-defined]
        self.assertTrue(any(item.get("event") == "preempt_requested" for item in events))

    async def test_derisk_cycle_blocks_cluster_on_unexpected_extra_leg(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": False,
                    "auto_derisk_shadow_mode": True,
                    "derisk_confirm_cycles": 1,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
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
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                        },
                        {
                            "exchange": "gate",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 0.3,
                            "coin_qty": 0.3,
                            "amount": 30.0,
                        },
                    ],
                    "balances": [],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                        {"exchange": "gate", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }

        await service._auto_derisk_cycle()

        rows = service._derisk_diagnostics  # type: ignore[attr-defined]
        cluster = next((row for row in rows if row.get("kind") == "cluster"), {})
        self.assertEqual(cluster.get("status"), "blocked_by_cluster_conflict")
        self.assertEqual(cluster.get("cluster_conflict_reason"), "extra_visible_legs")
        self.assertEqual(cluster.get("unexpected_leg_count"), 1)
        self.assertEqual((cluster.get("unexpected_legs") or [{}])[0].get("exchange"), "gate")

    async def test_derisk_cycle_blocks_overlapping_cluster_leg_assignments(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": False,
                    "auto_derisk_shadow_mode": True,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
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
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                        },
                    ],
                    "balances": [],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                        {"exchange": "gate", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                },
                "BTCUSDT|binance|gate|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "gate",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                },
            }
        }

        await service._auto_derisk_cycle()

        rows = [row for row in (service._derisk_diagnostics or []) if row.get("kind") == "cluster"]  # type: ignore[attr-defined]
        first = next((row for row in rows if row.get("short_exchange") == "okx"), {})
        self.assertEqual(first.get("status"), "blocked_by_cluster_conflict")
        self.assertEqual(first.get("cluster_conflict_reason"), "overlapping_cluster_leg")
        self.assertTrue(first.get("overlap_conflicts"))

    async def test_derisk_cycle_persists_jsonl_history_for_cycle_and_event(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "auto_derisk_shadow_mode": False,
                    "orphan_cleanup_enabled": True,
                    "derisk_confirm_cycles": 1,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        history_path = Path(self.tmp_dir.name) / "derisk_history.jsonl"
        service._derisk_history_store = JsonlEventStore(history_path)  # type: ignore[attr-defined]
        now = datetime.now(timezone.utc).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "okx",
                            "symbol": "BTCUSDT",
                            "symbol_normalized": "BTCUSDT",
                            "side": "short",
                            "quantity": 1.0,
                            "coin_qty": 1.0,
                            "amount": 100.0,
                            "entry_price": 100.0,
                            "mark_price": 99.0,
                        }
                    ],
                    "balances": [
                        {
                            "exchange": "okx",
                            "asset": "USDT",
                            "total": 300.0,
                            "available": 200.0,
                            "used": 50.0,
                            "buffer_pct": 66.0,
                        }
                    ],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }

        async def _manual_orphan_cleanup(payload):
            return {"execution_id": "orphan-history-1", "status": "accepted", "mode": "orphan-cleanup"}

        service.manual_orphan_cleanup = _manual_orphan_cleanup  # type: ignore[assignment]

        await service._auto_derisk_cycle()

        self.assertTrue(history_path.exists())
        rows = [
            json.loads(line)
            for line in history_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertTrue(any(row.get("record_type") == "event" and row.get("event") == "orphan_trigger" for row in rows))
        cycle = next((row for row in rows if row.get("record_type") == "cycle"), {})
        self.assertEqual(cycle.get("settings", {}).get("enabled"), True)
        self.assertEqual(cycle.get("cycle_action", {}).get("type"), "orphan_trigger")
        self.assertTrue(any(item.get("kind") == "cluster" for item in (cycle.get("rows") or [])))
        self.assertTrue(any(item.get("kind") == "orphan_candidate" for item in (cycle.get("rows") or [])))

    async def test_derisk_cycle_persists_matured_outcome_followup(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": True,
                    "auto_derisk_shadow_mode": False,
                    "derisk_confirm_cycles": 1,
                    "derisk_max_single_action_notional_usd": 500.0,
                    "derisk_min_free_balance_abs": 0.0,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        history_path = Path(self.tmp_dir.name) / "derisk_history_outcome.jsonl"
        outcome_state_path = Path(self.tmp_dir.name) / "derisk_outcome_state.json"
        service._derisk_history_store = JsonlEventStore(history_path)  # type: ignore[attr-defined]
        service._derisk_outcome_store = JsonStateStore(outcome_state_path)  # type: ignore[attr-defined]
        service._derisk_outcome_state = {"tracked": {}}  # type: ignore[attr-defined]

        def _snapshot(available_binance: float, buffer_binance: float) -> dict[str, object]:
            checked_at = datetime.fromtimestamp(1000, tz=timezone.utc).isoformat()
            return {
                "positions": [
                    {
                        "exchange": "binance",
                        "symbol": "BTCUSDT",
                        "symbol_normalized": "BTCUSDT",
                        "side": "long",
                        "quantity": 1.0,
                        "coin_qty": 1.0,
                        "amount": 900.0,
                        "notional": 900.0,
                        "entry_price": 100.0,
                        "mark_price": 98.0,
                        "unrealized_pnl": -10.0,
                        "leverage": 3.0,
                        "expected_funding": 2.0,
                        "funding_interval_hours": 8.0,
                        "next_funding": (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat(),
                    },
                    {
                        "exchange": "okx",
                        "symbol": "BTCUSDT",
                        "symbol_normalized": "BTCUSDT",
                        "side": "short",
                        "quantity": 1.0,
                        "coin_qty": 1.0,
                        "amount": 900.0,
                        "notional": 900.0,
                        "entry_price": 99.0,
                        "mark_price": 101.0,
                        "unrealized_pnl": 12.0,
                        "leverage": 3.0,
                        "expected_funding": 1.0,
                        "funding_interval_hours": 8.0,
                        "next_funding": (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat(),
                    },
                ],
                "balances": [
                    {
                        "exchange": "binance",
                        "asset": "USDT",
                        "total": 110.0,
                        "available": available_binance,
                        "used": 100.0,
                        "buffer_pct": buffer_binance,
                    },
                    {
                        "exchange": "okx",
                        "asset": "USDT",
                        "total": 400.0,
                        "available": 250.0,
                        "used": 100.0,
                        "buffer_pct": 62.5,
                    },
                ],
                "status": [
                    {"exchange": "binance", "status": "ok", "checked_at": checked_at},
                    {"exchange": "okx", "status": "ok", "checked_at": checked_at},
                ],
            }

        snapshots = [_snapshot(5.0, 4.5), _snapshot(80.0, 30.0)]

        class _Accounts:
            def snapshot(self_nonlocal):  # noqa: ANN001
                return snapshots[0]

        accounts = _Accounts()
        service._accounts = accounts  # type: ignore[attr-defined]
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"BTCUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "BTCUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "BTCUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }

        async def _manual_exit(payload):
            return {"execution_id": "derisk-outcome-1", "status": "accepted", "mode": "smart-exit"}

        service.manual_exit = _manual_exit  # type: ignore[assignment]

        with patch("webapp.services.time.time", return_value=1000.0):
            await service._auto_derisk_cycle()

        tracked = dict((service._derisk_outcome_state or {}).get("tracked") or {})  # type: ignore[attr-defined]
        self.assertEqual(len(tracked), 1)
        cycle_id = next(iter(tracked.keys()))
        tracked[cycle_id]["horizons"] = {"1m": {"target_ts": 1001.0, "emitted": False}}
        service._derisk_outcome_state["tracked"] = tracked  # type: ignore[attr-defined]
        service._save_derisk_outcome_state()  # type: ignore[attr-defined]

        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": False,
                    "auto_derisk_shadow_mode": True,
                    "derisk_min_free_balance_abs": 0.0,
                }
            }
        )
        snapshots.pop(0)

        with patch("webapp.services.time.time", return_value=1002.0):
            await service._auto_derisk_cycle()

        rows = [
            json.loads(line)
            for line in history_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        outcome = next((row for row in rows if row.get("record_type") == "outcome"), {})
        self.assertEqual(outcome.get("cycle_id"), cycle_id)
        self.assertEqual(outcome.get("horizon"), "1m")
        self.assertEqual(outcome.get("heuristic_outcome", {}).get("label"), "improved")
        self.assertEqual(outcome.get("current", {}).get("stress_status"), "ok")

    async def test_derisk_cycle_escalates_to_full_cleanup_when_partial_leaves_dust(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_derisk_enabled": False,
                    "auto_derisk_shadow_mode": True,
                    "derisk_max_single_action_notional_usd": 500.0,
                    "derisk_dust_notional_usd": 10.0,
                    "derisk_min_free_balance_abs": 0.0,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        now = datetime.now(timezone.utc).isoformat()
        service._accounts = type(  # type: ignore[attr-defined]
            "X",
            (),
            {
                "snapshot": lambda self=None: {
                    "positions": [
                        {
                            "exchange": "binance",
                            "symbol": "DOGEUSDT",
                            "symbol_normalized": "DOGEUSDT",
                            "side": "long",
                            "quantity": 100.0,
                            "coin_qty": 100.0,
                            "amount": 8.0,
                            "notional": 8.0,
                            "entry_price": 0.40,
                            "mark_price": 0.39,
                            "unrealized_pnl": -1.0,
                            "leverage": 4.0,
                        },
                        {
                            "exchange": "okx",
                            "symbol": "DOGEUSDT",
                            "symbol_normalized": "DOGEUSDT",
                            "side": "short",
                            "quantity": 100.0,
                            "coin_qty": 100.0,
                            "amount": 8.0,
                            "notional": 8.0,
                            "entry_price": 0.41,
                            "mark_price": 0.42,
                            "unrealized_pnl": 1.2,
                            "leverage": 4.0,
                        },
                    ],
                    "balances": [
                        {
                            "exchange": "binance",
                            "asset": "USDT",
                            "total": 100.0,
                            "available": 8.0,
                            "used": 30.0,
                            "buffer_pct": 8.0,
                        },
                        {
                            "exchange": "okx",
                            "asset": "USDT",
                            "total": 300.0,
                            "available": 200.0,
                            "used": 40.0,
                            "buffer_pct": 66.0,
                        },
                    ],
                    "status": [
                        {"exchange": "binance", "status": "ok", "checked_at": now},
                        {"exchange": "okx", "status": "ok", "checked_at": now},
                    ],
                }
            },
        )()
        service._positions_by_symbol = (  # type: ignore[attr-defined]
            lambda positions, return_grouped=True, market_lookup=None, market_ts_lookup=None: (
                [],
                {"DOGEUSDT": list(positions or [])},
            )
        )
        service._hedge_clusters = {  # type: ignore[attr-defined]
            "rules": {
                "DOGEUSDT|binance|okx|hedged_pair": {
                    "kind": "hedged_pair",
                    "symbol": "DOGEUSDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "enabled": True,
                    "qty_tolerance_pct": 0.1,
                    "source": "manual",
                }
            }
        }

        await service._auto_derisk_cycle()

        rows = service._derisk_diagnostics  # type: ignore[attr-defined]
        cluster = next((row for row in rows if row.get("kind") == "cluster"), {})
        candidate = next((row for row in rows if row.get("kind") == "candidate"), {})
        self.assertEqual(cluster.get("action_mode"), "full_cleanup")
        self.assertEqual(cluster.get("residual_status"), "flat")
        self.assertEqual(candidate.get("action_mode"), "full_cleanup")
        self.assertEqual(candidate.get("residual_status"), "flat")
        self.assertAlmostEqual(float(candidate.get("action_qty") or 0.0), 100.0)

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

    async def test_rebalance_disabled_persists_shadow_candidate(self) -> None:
        self.manager.update(
            {
                "protective": {
                    "auto_rebalance_enabled": False,
                    "rebalance_delta_pct": 0.2,
                }
            }
        )
        service = DataService(settings_manager=self.manager)
        service._rebalance_prev_positions = {  # type: ignore[attr-defined]
            ("BTC", "binance", "long"): 10.0,
            ("BTC", "okx", "short"): 10.0,
        }
        await service._maybe_rebalance_positions(  # type: ignore[attr-defined]
            [
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT:USDT",
                    "side": "long",
                    "coin_qty": 5.0,
                },
                {
                    "exchange": "okx",
                    "symbol": "BTCUSDT",
                    "side": "short",
                    "coin_qty": 10.0,
                },
            ]
        )
        events = service._protective_shadow_events  # type: ignore[attr-defined]
        candidate = next(
            item for item in events if item.get("event") == "rebalance_candidate"
        )
        self.assertEqual(candidate["symbol"], "BTC")
        self.assertEqual(candidate["reduce_side"], "short")
        self.assertAlmostEqual(float(candidate["planned_qty"]), 5.0)

    async def test_auto_agent_worker_runs_derisk_before_other_strategies(self) -> None:
        service = DataService(settings_manager=self.manager)
        calls: list[str] = []

        async def _record(name: str) -> None:
            calls.append(name)

        service._derisk_last_worker_cycle_ts = 0.0  # type: ignore[attr-defined]
        service._auto_derisk_cycle = lambda: _record("derisk")  # type: ignore[assignment]
        service._auto_exit_cycle = lambda: _record("auto_exit")  # type: ignore[assignment]
        service._auto_strategy_cycle = lambda: _record("strategy")  # type: ignore[assignment]
        service._auto_arb_cycle = lambda: _record("grid")  # type: ignore[assignment]

        await service._auto_agent_cycle()  # type: ignore[attr-defined]

        self.assertEqual(calls, ["derisk", "auto_exit", "strategy", "grid"])

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

        def _positions_by_symbol(positions, return_grouped=True, market_lookup=None, market_ts_lookup=None):
            calls["positions_by_symbol"] += 1
            return ([], {})

        service._accounts = _FakeAccounts()  # type: ignore[attr-defined]
        service._positions_market_snapshot_lookup = lambda: ({}, {})  # type: ignore[attr-defined]
        service._positions_by_symbol = _positions_by_symbol  # type: ignore[attr-defined]
        service._positions_market_state = lambda positions=None: {"status": []}  # type: ignore[attr-defined]

        service._account_state()
        service._account_state()
        self.assertEqual(calls["positions_by_symbol"], 1)


if __name__ == "__main__":
    unittest.main()
