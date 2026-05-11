from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from project_settings import SettingsManager
from webapp.services import DataService, _protective_issue_kind


class ProjectSettingsTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.settings_path = Path(self.tmp_dir.name) / "settings.json"
        self.manager = SettingsManager(path=self.settings_path)

    def tearDown(self) -> None:
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
                    "notification_primary_channel": "pushbullet",
                    "notification_fallback_channel": "telegram",
                }
            }
        )
        reloaded = SettingsManager(path=self.settings_path)
        protective = reloaded.current.protective
        self.assertEqual(protective.get("notification_primary_channel"), "pushbullet")
        self.assertEqual(protective.get("notification_fallback_channel"), "telegram")

    def test_target_leverage_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            self.manager.update({"protective": {"target_leverage": 0}})

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
                    ]
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
        self.assertEqual(len(payload["cards"]), 1)
        card = payload["cards"][0]
        self.assertEqual(card["symbol"], "BTCUSDT")
        self.assertEqual(card["pair_label"], "BINANCE / OKX")
        self.assertAlmostEqual(card["net_pnl"], 2.0)
        self.assertTrue(card["flags"]["funding_soon"])
        self.assertEqual(card["auto_exit"]["status"], "waiting")
        self.assertAlmostEqual(card["auto_exit"]["live_spread_pct"], -0.25)
        self.assertEqual(len(card["legs"]), 2)

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
