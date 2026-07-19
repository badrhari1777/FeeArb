from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from risk.derisk_manager import (
    build_exchange_health,
    classify_residual_leg,
    derisk_candidate_score,
    derisk_score_allowed,
    derive_cluster_rules,
    exchange_stress_state,
    funding_time_weight,
    normalize_hedge_cluster_config,
)


class DeriskManagerHelpersTestCase(unittest.TestCase):
    def test_funding_time_weight_grows_near_event(self) -> None:
        near = funding_time_weight(15.0, 60.0)
        far = funding_time_weight(45.0, 60.0)
        self.assertGreater(near, far)
        self.assertGreater(near, 0.0)

    def test_exchange_health_blocks_auth_failure(self) -> None:
        now = datetime.now(timezone.utc)
        payload = build_exchange_health(
            [
                {
                    "exchange": "binance",
                    "status": "error",
                    "error": 'binanceusdm {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action"}',
                    "checked_at": now.isoformat(),
                }
            ],
            now_ts=now.timestamp(),
        )
        self.assertEqual(payload["binance"]["health"], "untrusted")
        self.assertEqual(payload["binance"]["last_error_kind"], "auth_error")

    def test_exchange_health_classifies_mexc_invalid_key_as_auth_error(self) -> None:
        now = datetime.now(timezone.utc)
        payload = build_exchange_health(
            [
                {
                    "exchange": "mexc",
                    "status": "error",
                    "error": 'mexc {"code":10072,"msg":"Api key info invalid"}',
                    "checked_at": now.isoformat(),
                }
            ],
            now_ts=now.timestamp(),
        )
        self.assertEqual(payload["mexc"]["health"], "untrusted")
        self.assertEqual(payload["mexc"]["last_error_kind"], "auth_error")

    def test_exchange_health_marks_old_success_as_stale(self) -> None:
        now = datetime.now(timezone.utc)
        payload = build_exchange_health(
            [
                {
                    "exchange": "okx",
                    "status": "ok",
                    "checked_at": (now - timedelta(minutes=10)).isoformat(),
                }
            ],
            now_ts=now.timestamp(),
            stale_after_sec=120,
        )
        self.assertEqual(payload["okx"]["health"], "stale")

    def test_exchange_stress_state_detects_panic(self) -> None:
        stress = exchange_stress_state(
            {"total": 1000.0, "used": 900.0, "available": 30.0, "buffer_pct": 3.0},
            target_buffer_pct=0.30,
            warning_buffer_pct=0.20,
            panic_buffer_pct=0.15,
            min_free_balance_abs=100.0,
        )
        self.assertEqual(stress["status"], "panic")
        self.assertGreaterEqual(stress["stress_score"], 1.0)

    def test_derisk_candidate_score_prefers_negative_funding(self) -> None:
        score = derisk_candidate_score(
            margin_relief_usd=100.0,
            close_cost_usd=5.0,
            funding_to_next_usd=-2.0,
            minutes_to_funding=20.0,
            interval_minutes=60.0,
        )
        self.assertIsNotNone(score)
        self.assertLess(score or 0.0, 0.05)

    def test_derisk_score_ceiling_blocks_expensive_candidate(self) -> None:
        self.assertTrue(derisk_score_allowed(0.20, 0.25))
        self.assertFalse(derisk_score_allowed(0.30, 0.25))
        self.assertFalse(derisk_score_allowed(None, 0.25))

    def test_classify_residual_leg_detects_dust(self) -> None:
        status = classify_residual_leg(qty=0.01, notional_usd=3.0, dust_notional_usd=5.0)
        self.assertEqual(status, "dust_suspect")

    def test_empty_exchange_account_is_not_treated_as_stress(self) -> None:
        state = exchange_stress_state(
            {"total": 0.0, "used": 0.0, "available": 0.0},
            target_buffer_pct=0.30,
            warning_buffer_pct=0.20,
            panic_buffer_pct=0.15,
            min_free_balance_abs=500.0,
        )
        self.assertEqual(state["status"], "ok")
        self.assertEqual(state["deficit_usd"], 0.0)

    def test_normalize_hedge_cluster_config_keeps_standalone_and_pair(self) -> None:
        payload = normalize_hedge_cluster_config(
            {
                "rules": {
                    "one": {
                        "kind": "standalone",
                        "symbol": "BTCUSDT",
                        "exchange": "gate",
                    },
                    "two": {
                        "kind": "hedged_pair",
                        "symbol": "ETHUSDT",
                        "long_exchange": "binance",
                        "short_exchange": "okx",
                    },
                }
            }
        )
        rules = payload.get("rules") or {}
        self.assertEqual(len(rules), 2)
        self.assertTrue(any(item.get("kind") == "standalone" for item in rules.values()))
        self.assertTrue(any(item.get("kind") == "hedged_pair" for item in rules.values()))

    def test_derive_cluster_rules_requires_active_bound_owner_and_visible_leg(self) -> None:
        auto_exit_rules = {
            "disabled": {
                "symbol": "OLDUSDT",
                "long_exchange": "binance",
                "short_exchange": "okx",
                "enabled": False,
                "v1_enabled": False,
                "position_signature": {"version": 1},
            },
            "unbound": {
                "symbol": "NEWUSDT",
                "long_exchange": "binance",
                "short_exchange": "okx",
                "enabled": True,
                "position_signature": None,
            },
            "active": {
                "symbol": "LIVEUSDT",
                "long_exchange": "binance",
                "short_exchange": "okx",
                "enabled": True,
                "v1_enabled": False,
                "position_signature": {"version": 1},
                "signature_status": "position_signature_match",
                "rule_generation": 4,
            },
        }
        payload = derive_cluster_rules(
            {"rules": {}},
            auto_exit_rules,
            active_position_legs={("LIVEUSDT", "binance", "long")},
        )
        rules = payload.get("rules") or {}
        self.assertEqual(len(rules), 1)
        rule = next(iter(rules.values()))
        self.assertEqual(rule["symbol"], "LIVEUSDT")
        self.assertEqual(rule["owner_type"], "auto_exit")
        self.assertEqual(rule["owner_generation"], 4)

    def test_derive_cluster_rules_rebinds_stale_multileg_to_unambiguous_live_pair(self) -> None:
        payload = derive_cluster_rules(
            {"rules": {}},
            {
                "HUSDT|multileg|multileg": {
                    "symbol": "HUSDT",
                    "long_exchange": "multileg",
                    "short_exchange": "multileg",
                    "enabled": True,
                    "position_signature": {"version": 1},
                    "signature_status": "position_signature_legs_changed",
                    "rule_generation": 3,
                }
            },
            active_position_legs={
                ("HUSDT", "kucoin", "long"),
                ("HUSDT", "binance", "short"),
            },
        )
        rules = payload.get("rules") or {}
        self.assertEqual(len(rules), 1)
        rule = next(iter(rules.values()))
        self.assertEqual(rule["long_exchange"], "kucoin")
        self.assertEqual(rule["short_exchange"], "binance")
        self.assertEqual(rule["source"], "auto_exit_multileg_live")
        self.assertEqual(rule["owner_type"], "auto_exit")

    def test_derive_cluster_rules_discovers_unowned_live_pair(self) -> None:
        payload = derive_cluster_rules(
            {"rules": {}},
            {},
            active_position_legs={
                ("NEWUSDT", "okx", "long"),
                ("NEWUSDT", "bybit", "short"),
            },
        )
        rule = next(iter((payload.get("rules") or {}).values()))
        self.assertEqual(rule["source"], "live_positions")
        self.assertEqual(rule["owner_type"], "positions")
