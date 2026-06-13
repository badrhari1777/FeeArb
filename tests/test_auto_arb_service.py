from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from project_settings import SettingsManager
from webapp.services import DataService


class AutoArbServiceTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        root = Path(self.tmp_dir.name)
        self.patchers = [
            patch("webapp.services.AUTO_ARB_STATE_PATH", root / "state" / "auto_arb.json"),
            patch("webapp.services.AUTO_ARB_HISTORY_PATH", root / "logs" / "auto_arb.jsonl"),
            patch("webapp.services.AUTO_STRATEGY_STATE_PATH", root / "state" / "auto_strategies.json"),
            patch("webapp.services.AUTO_STRATEGY_HISTORY_PATH", root / "logs" / "auto_strategy_history.jsonl"),
            patch("webapp.services.AUTO_EXIT_STATE_PATH", root / "state" / "auto_exit.json"),
            patch("webapp.services.AUTO_EXIT_HISTORY_PATH", root / "logs" / "auto_exit.jsonl"),
            patch("webapp.services.HEDGE_CLUSTER_STATE_PATH", root / "state" / "clusters.json"),
            patch("webapp.services.DERISK_HISTORY_PATH", root / "logs" / "derisk.jsonl"),
            patch("webapp.services.DERISK_OUTCOME_STATE_PATH", root / "state" / "outcomes.json"),
            patch("execution.wallet.WalletService.DEFAULT_STATE_PATH", root / "state" / "wallet.json"),
        ]
        for patcher in self.patchers:
            patcher.start()
        self.service = DataService(settings_manager=SettingsManager(path=root / "settings.json"))

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tmp_dir.cleanup()

    async def test_spreads_keep_entry_and_exit_in_same_orientation(self) -> None:
        self.service._mobile_quote_for_exchange = AsyncMock(
            side_effect=[
                {"bid": 98.5, "ask": 99.0},
                {"bid": 101.0, "ask": 101.5},
            ]
        )

        result = await self.service.auto_arb_spreads(
            symbol="BTCUSDT",
            long_exchange="bybit",
            short_exchange="kucoin",
        )

        self.assertAlmostEqual(result["entry_spread_pct"], (99.0 - 101.0) / 99.0 * 100.0)
        self.assertAlmostEqual(result["exit_spread_pct"], (98.5 - 101.5) / 98.5 * 100.0)
        self.assertLess(result["entry_spread_pct"], 0)
        self.assertLess(result["exit_spread_pct"], 0)

    async def test_analysis_uses_both_dry_runs_for_safe_chunk(self) -> None:
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "status": "ok",
                "entry_spread_pct": -2.0,
                "exit_spread_pct": -1.8,
                "long_quote": {"ask": 10.0, "bid": 9.9},
                "short_quote": {"bid": 10.2, "ask": 10.1},
            }
        )
        self.service.manual_analyze = AsyncMock(
            side_effect=[
                {"recommended_chunk_qty": 100.0, "min_chunk_qty": 10.0},
                {"recommended_chunk_qty": 80.0, "min_chunk_qty": 10.0},
            ]
        )

        result = await self.service.analyze_auto_arb(
            {
                "symbol": "BTCUSDT",
                "long_exchange": "bybit",
                "short_exchange": "kucoin",
                "budget_mode": "qty",
                "max_qty": 1000,
                "range_start_pct": -2,
                "range_end_pct": -10,
            }
        )

        self.assertAlmostEqual(result["safe_chunk_qty"], 56.0)
        self.assertEqual(result["config"]["level_count"], 18)
        self.assertEqual(len(result["config"]["levels"]), 18)
        self.assertEqual(self.service.manual_analyze.await_count, 2)

    async def test_shadow_cycle_requires_confirmation_and_moves_one_level(self) -> None:
        levels = [
            {
                "level": 1,
                "entry_spread_pct": -2.0,
                "exit_spread_pct": -1.5,
                "qty": 10.0,
                "cumulative_qty": 10.0,
            },
            {
                "level": 2,
                "entry_spread_pct": -4.0,
                "exit_spread_pct": -3.5,
                "qty": 10.0,
                "cumulative_qty": 20.0,
            },
        ]
        rule = {
            "id": "rule1",
            "generation": 1,
            "enabled": True,
            "mode": "shadow",
            "symbol": "BTCUSDT",
            "long_exchange": "bybit",
            "short_exchange": "kucoin",
            "level_count": 2,
            "levels": levels,
            "shadow_level": 0,
            "shadow_qty": 0.0,
            "confirm_samples": 2,
            "max_levels_per_cycle": 1,
        }
        self.service._auto_arb["rules"]["rule1"] = rule
        self.service.auto_arb_spreads = AsyncMock(
            side_effect=[
                {"entry_spread_pct": -5.0, "exit_spread_pct": -4.8},
                {"entry_spread_pct": -5.0, "exit_spread_pct": -4.8},
            ]
        )

        await self.service._auto_arb_cycle()
        self.assertEqual(rule["shadow_level"], 0)
        self.assertEqual(rule["pending_samples"], 1)

        await self.service._auto_arb_cycle()
        self.assertEqual(rule["shadow_level"], 1)
        self.assertEqual(rule["shadow_qty"], 10.0)
        self.assertEqual(rule["status"], "shadow_enter")

        history = self.service.auto_arb_history("rule1")
        self.assertEqual(history["events"][-1]["event"], "shadow_enter")

    async def test_analysis_builds_exchange_style_execution_table(self) -> None:
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "status": "ok",
                "entry_spread_pct": -1.0,
                "exit_spread_pct": -0.8,
                "long_quote": {"ask": 10.0, "bid": 9.9},
                "short_quote": {"bid": 10.2, "ask": 10.1},
            }
        )
        self.service.manual_analyze = AsyncMock(return_value={"recommended_chunk_qty": 5.0})

        result = await self.service.analyze_auto_arb(
            {
                "symbol": "BTCUSDT",
                "long_exchange": "bybit",
                "short_exchange": "kucoin",
                "budget_mode": "notional",
                "max_notional": 40,
                "range_start_pct": -2,
                "range_end_pct": -4,
                "level_count": 2,
            }
        )

        first = result["config"]["levels"][0]
        self.assertEqual(first["entry_action"], "BUY bybit / SELL kucoin")
        self.assertEqual(first["exit_action"], "SELL bybit / BUY kucoin")
        self.assertIn("entry spread <=", first["entry_condition"])
        self.assertGreater(first["chunk_notional_estimate"], 0)

    async def test_live_can_arm_from_zero_position(self) -> None:
        rule = {
            "id": "small",
            "generation": 1,
            "enabled": False,
            "mode": "shadow",
            "symbol": "BTCUSDT",
            "long_exchange": "bybit",
            "short_exchange": "kucoin",
            "max_qty": 4.0,
            "chunk_notional_estimate": 20.0,
            "total_notional_estimate": 40.0,
            "levels": [
                {"level": 1, "qty": 2.0, "cumulative_qty": 2.0},
                {"level": 2, "qty": 2.0, "cumulative_qty": 4.0},
            ],
        }
        self.service._auto_arb["rules"]["small"] = rule
        self.service._accounts.refresh_now_for_protective = AsyncMock(return_value=None)
        self.service._accounts.snapshot = MagicMock(return_value={"positions": []})

        result = await self.service.arm_auto_arb_live("small", "LIVE small")

        self.assertEqual(result["rule"]["mode"], "live")
        self.assertEqual(result["rule"]["live_level"], 0)
        self.assertEqual(result["rule"]["status"], "waiting_entry")
        self.assertTrue(result["rule"]["enabled"])

    async def test_live_rejects_large_test_budget(self) -> None:
        self.service._auto_arb["rules"]["large"] = {
            "id": "large",
            "mode": "shadow",
            "symbol": "BTCUSDT",
            "long_exchange": "bybit",
            "short_exchange": "kucoin",
            "max_qty": 20.0,
            "chunk_notional_estimate": 60.0,
            "total_notional_estimate": 120.0,
            "levels": [],
        }

        with self.assertRaisesRegex(ValueError, "exceeds the restricted Live limit"):
            await self.service.arm_auto_arb_live("large", "LIVE large")

    async def test_live_zero_position_starts_one_enter_chunk(self) -> None:
        self.service._auto_arb["rules"]["live1"] = {
            "id": "live1",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "BTCUSDT",
            "long_exchange": "bybit",
            "short_exchange": "kucoin",
            "max_qty": 20.0,
            "level_count": 2,
            "levels": [
                {
                    "level": 1,
                    "entry_spread_pct": -2.0,
                    "exit_spread_pct": -1.5,
                    "qty": 10.0,
                    "cumulative_qty": 10.0,
                },
                {
                    "level": 2,
                    "entry_spread_pct": -4.0,
                    "exit_spread_pct": -3.5,
                    "qty": 10.0,
                    "cumulative_qty": 20.0,
                },
            ],
            "live_level": 0,
            "actual_hedged_qty": 0.0,
            "confirm_samples": 1,
            "max_levels_per_cycle": 1,
        }
        self.service.auto_arb_spreads = AsyncMock(
            return_value={"entry_spread_pct": -5.0, "exit_spread_pct": -4.8}
        )
        self.service.manual_enter = AsyncMock(
            return_value={"execution_id": "gridexec1", "status": "running"}
        )

        await self.service._auto_arb_cycle()

        rule = self.service._auto_arb["rules"]["live1"]
        self.assertEqual(rule["active_execution_id"], "gridexec1")
        self.assertEqual(rule["active_from_level"], 0)
        self.assertEqual(rule["active_to_level"], 1)
        self.assertEqual(rule["status"], "executing_enter")
        payload = self.service.manual_enter.await_args.args[0]
        self.assertEqual(payload["qty"], 10.0)
        self.assertTrue(payload["auto_arb_agent"])

    async def test_live_strategy_keeps_only_first_unfinished_step_current(self) -> None:
        payload = await self.service.upsert_auto_strategy(
            {
                "id": "ladder1",
                "type": "enter_ladder",
                "symbol": "BTCUSDT",
                "long_exchange": "bybit",
                "short_exchange": "kucoin",
                "steps": [
                    {"notional_usd": 100, "spread_target_pct": -2.0},
                    {"notional_usd": 200, "spread_target_pct": -4.0},
                ],
            }
        )

        strategy = payload["strategies"][0]
        self.assertEqual(strategy["steps"][0]["status"], "waiting")
        self.assertEqual(strategy["steps"][1]["status"], "waiting")
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "entry_spread_pct": -5.0,
                "exit_spread_pct": -4.5,
                "long_quote": {"mid": 10.0},
                "short_quote": {"mid": 10.1},
            }
        )
        self.service._auto_strategy_funding_delta_pct = AsyncMock(return_value=0.1)
        self.service._accounts.refresh_now = AsyncMock(return_value=None)
        self.service._accounts.snapshot = MagicMock(return_value={"positions": []})
        self.service.manual_enter = AsyncMock(
            return_value={"execution_id": "strategyexec1", "status": "running"}
        )

        await self.service._auto_strategy_cycle()

        stored = self.service._auto_strategies["strategies"]["ladder1"]
        self.assertEqual(stored["steps"][0]["active_execution_id"], "strategyexec1")
        self.assertIsNone(stored["steps"][1]["active_execution_id"])
        self.assertAlmostEqual(stored["steps"][0]["target_qty"], 100.0 / 10.1)

    async def test_live_strategy_partial_reconciliation_keeps_fixed_target(self) -> None:
        await self.service.upsert_auto_strategy(
            {
                "id": "exit1",
                "type": "exit_ladder",
                "symbol": "BTCUSDT",
                "long_exchange": "bybit",
                "short_exchange": "kucoin",
                "steps": [{"qty": 100, "spread_target_pct": -1.0}],
            }
        )
        step = self.service._auto_strategies["strategies"]["exit1"]["steps"][0]
        step.update(
            {
                "target_qty": 100.0,
                "remaining_qty": 100.0,
                "baseline_hedged_qty": 100.0,
                "active_execution_id": "partial1",
                "status": "executing",
            }
        )
        self.service._manual_runs["partial1"] = {
            "status": "completed",
            "result": {"remaining_qty": 40.0},
            "error": None,
        }
        self.service._accounts.refresh_now = AsyncMock(return_value=None)
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {"symbol": "BTCUSDT", "exchange": "bybit", "side": "long", "quantity": 60},
                    {"symbol": "BTCUSDT", "exchange": "kucoin", "side": "short", "quantity": -60},
                ]
            }
        )

        await self.service._reconcile_auto_strategy_execution("exit1", step["id"], "partial1")

        self.assertEqual(step["status"], "partial")
        self.assertAlmostEqual(step["target_qty"], 100.0)
        self.assertAlmostEqual(step["filled_qty"], 40.0)
        self.assertAlmostEqual(step["remaining_qty"], 60.0)

    async def test_exit_strategy_preflight_uses_smaller_position_leg(self) -> None:
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {"symbol": "BTCUSDT", "exchange": "bybit", "side": "long", "quantity": 150},
                    {"symbol": "BTCUSDT", "exchange": "kucoin", "side": "short", "quantity": -120},
                ]
            }
        )
        self.service.manual_analyze = AsyncMock(return_value={"recommended_qty": 60.0})

        result = await self.service.analyze_auto_strategy(
            {
                "type": "exit_ladder",
                "symbol": "BTCUSDT",
                "long_exchange": "bybit",
                "short_exchange": "kucoin",
                "steps": [{"percent": 50, "spread_target_pct": -1.0}],
            }
        )

        self.assertAlmostEqual(result["hedged_qty"], 120.0)
        self.assertAlmostEqual(result["steps"][0]["requested_qty"], 60.0)
        self.assertAlmostEqual(self.service.manual_analyze.await_args.args[0]["qty"], 60.0)


if __name__ == "__main__":
    unittest.main()
