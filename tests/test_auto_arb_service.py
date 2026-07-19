from __future__ import annotations

import tempfile
import time
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
        self.assertAlmostEqual(result["config"]["exit_gap_pct"], 8.0 / 17.0)
        self.assertEqual(result["config"]["exit_gap_mode"], "arithmetic_grid_step")
        self.assertEqual(result["config"]["grid_interval_count"], 17)
        self.assertEqual(self.service.manual_analyze.await_count, 2)

    async def test_analysis_ignores_manual_exit_gap_and_uses_grid_step(self) -> None:
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "status": "ok",
                "entry_spread_pct": -18.0,
                "exit_spread_pct": -18.2,
                "long_quote": {"ask": 10.0, "bid": 9.9},
                "short_quote": {"bid": 12.0, "ask": 12.1},
            }
        )
        self.service.manual_analyze = AsyncMock(
            return_value={"recommended_chunk_qty": 300.0, "min_chunk_qty": 10.0}
        )

        result = await self.service.analyze_auto_arb(
            {
                "symbol": "HUSDT",
                "long_exchange": "kucoin",
                "short_exchange": "bybit",
                "budget_mode": "qty",
                "max_qty": 15000,
                "range_start_pct": -7,
                "range_end_pct": -25,
                "level_count": 12,
                "exit_gap_pct": 0.1,
            }
        )

        expected_gap = 18.0 / 11.0
        self.assertAlmostEqual(result["grid_step_pct"], expected_gap)
        self.assertAlmostEqual(result["config"]["exit_gap_pct"], expected_gap)
        self.assertAlmostEqual(
            result["config"]["levels"][7]["exit_spread_pct"],
            result["config"]["levels"][6]["entry_spread_pct"],
        )

    async def test_analysis_adjusts_level_count_to_existing_position(self) -> None:
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "status": "ok",
                "entry_spread_pct": -12.0,
                "exit_spread_pct": -12.2,
                "long_quote": {"ask": 0.19, "bid": 0.18},
                "short_quote": {"bid": 0.21, "ask": 0.22},
            }
        )
        self.service.manual_analyze = AsyncMock(
            return_value={"recommended_chunk_qty": 1500.0, "min_chunk_qty": 10.0}
        )
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {
                        "exchange": "kucoin",
                        "symbol": "H/USDT:USDT",
                        "side": "long",
                        "coin_qty": 4410.0,
                    },
                    {
                        "exchange": "bybit",
                        "symbol": "H/USDT:USDT",
                        "side": "short",
                        "coin_qty": -4400.0,
                    },
                ]
            }
        )

        result = await self.service.analyze_auto_arb(
            {
                "symbol": "HUSDT",
                "long_exchange": "kucoin",
                "short_exchange": "bybit",
                "budget_mode": "qty",
                "max_qty": 25000,
                "range_start_pct": -5,
                "range_end_pct": -25,
                "level_count": 15,
            }
        )

        config = result["config"]
        fit = config["existing_position_fit"]
        self.assertEqual(config["level_count"], 17)
        self.assertAlmostEqual(config["chunk_qty"], 25000.0 / 17.0)
        self.assertEqual(fit["level"], 3)
        self.assertTrue(fit["matches"])
        self.assertTrue(fit["adoption_will_match"])
        self.assertTrue(fit["level_count_adjusted"])
        self.assertAlmostEqual(fit["existing_qty"], 4400.0)
        self.assertLessEqual(fit["diff_qty"], fit["tolerance_qty"])
        self.assertTrue(
            any("adjusted from 15 to 17" in warning for warning in result["warnings"])
        )

    async def test_existing_position_exit_range_derives_entry_grid(self) -> None:
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "status": "ok",
                "entry_spread_pct": 0.8,
                "exit_spread_pct": 0.7,
                "long_quote": {"ask": 0.29, "bid": 0.289},
                "short_quote": {"bid": 0.287, "ask": 0.288},
            }
        )
        self.service.manual_analyze = AsyncMock(
            return_value={"recommended_chunk_qty": 1000.0, "min_chunk_qty": 10.0}
        )
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {
                        "exchange": "bybit",
                        "symbol": "H/USDT:USDT",
                        "side": "long",
                        "coin_qty": 10000.0,
                    },
                    {
                        "exchange": "binance",
                        "symbol": "H/USDT:USDT",
                        "side": "short",
                        "coin_qty": -10000.0,
                    },
                ]
            }
        )

        result = await self.service.analyze_auto_arb(
            {
                "symbol": "HUSDT",
                "long_exchange": "bybit",
                "short_exchange": "binance",
                "setup_mode": "existing_position_exit_range",
                "budget_mode": "qty",
                "max_qty": 1.0,
                "range_start_pct": 1.0,
                "range_end_pct": 15.0,
                "exit_range_start_pct": 1.0,
                "exit_range_end_pct": 15.0,
                "level_count": 20,
            }
        )

        config = result["config"]
        levels = config["levels"]
        self.assertEqual(config["setup_mode"], "existing_position_exit_range")
        self.assertAlmostEqual(config["max_qty"], 10000.0)
        self.assertAlmostEqual(config["chunk_qty"], 500.0)
        self.assertAlmostEqual(config["exit_gap_pct"], 14.0 / 19.0)
        self.assertAlmostEqual(config["range_start_pct"], 15.0 - 14.0 / 19.0)
        self.assertAlmostEqual(config["range_end_pct"], 1.0 - 14.0 / 19.0)
        self.assertAlmostEqual(levels[0]["exit_spread_pct"], 15.0)
        self.assertAlmostEqual(levels[-1]["exit_spread_pct"], 1.0)
        self.assertEqual(config["existing_position_fit"]["level"], 20)
        self.assertTrue(config["existing_position_fit"]["adoption_will_match"])

    async def test_adopt_existing_full_grid_keeps_strategy_max_qty(self) -> None:
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "status": "ok",
                "entry_spread_pct": 0.8,
                "exit_spread_pct": 0.7,
                "long_quote": {"ask": 0.29, "bid": 0.289},
                "short_quote": {"bid": 0.287, "ask": 0.288},
            }
        )
        self.service.manual_analyze = AsyncMock(
            return_value={"recommended_chunk_qty": 1000.0, "min_chunk_qty": 10.0}
        )
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {
                        "exchange": "bybit",
                        "symbol": "H/USDT:USDT",
                        "side": "long",
                        "coin_qty": 7000.0,
                    },
                    {
                        "exchange": "binance",
                        "symbol": "H/USDT:USDT",
                        "side": "short",
                        "coin_qty": -7000.0,
                    },
                ]
            }
        )

        result = await self.service.analyze_auto_arb(
            {
                "symbol": "HUSDT",
                "long_exchange": "bybit",
                "short_exchange": "binance",
                "setup_mode": "adopt_existing_full_grid",
                "budget_mode": "qty",
                "max_qty": 20000.0,
                "range_start_pct": 1.0,
                "range_end_pct": 15.0,
                "exit_range_start_pct": 1.0,
                "exit_range_end_pct": 15.0,
                "level_count": 15,
            }
        )

        config = result["config"]
        fit = config["existing_position_fit"]
        self.assertEqual(config["setup_mode"], "adopt_existing_full_grid")
        self.assertAlmostEqual(config["max_qty"], 20000.0)
        self.assertEqual(config["level_count"], 20)
        self.assertAlmostEqual(config["chunk_qty"], 1000.0)
        self.assertEqual(fit["level"], 7)
        self.assertTrue(fit["adoption_will_match"])
        self.assertTrue(fit["level_count_adjusted"])
        self.assertAlmostEqual(config["levels"][6]["cumulative_qty"], 7000.0)
        self.assertAlmostEqual(config["levels"][-1]["cumulative_qty"], 20000.0)

    async def test_adopt_existing_full_grid_accepts_partial_level(self) -> None:
        self.service.auto_arb_spreads = AsyncMock(
            return_value={
                "status": "ok",
                "entry_spread_pct": 6.8,
                "exit_spread_pct": 6.7,
                "long_quote": {"ask": 0.29, "bid": 0.289},
                "short_quote": {"bid": 0.287, "ask": 0.288},
            }
        )
        self.service.manual_analyze = AsyncMock(
            return_value={"recommended_chunk_qty": 1000.0, "min_chunk_qty": 10.0}
        )
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {
                        "exchange": "bybit",
                        "symbol": "H/USDT:USDT",
                        "side": "long",
                        "coin_qty": 2190.0,
                    },
                    {
                        "exchange": "binance",
                        "symbol": "H/USDT:USDT",
                        "side": "short",
                        "coin_qty": -2206.0,
                    },
                ]
            }
        )

        result = await self.service.analyze_auto_arb(
            {
                "symbol": "HUSDT",
                "long_exchange": "bybit",
                "short_exchange": "binance",
                "setup_mode": "adopt_existing_full_grid",
                "budget_mode": "qty",
                "max_qty": 20000.0,
                "range_start_pct": 1.0,
                "range_end_pct": 15.0,
                "exit_range_start_pct": 1.0,
                "exit_range_end_pct": 15.0,
                "level_count": 15,
            }
        )

        config = result["config"]
        fit = config["existing_position_fit"]
        self.assertEqual(config["level_count"], 15)
        self.assertAlmostEqual(config["max_qty"], 20000.0)
        self.assertEqual(fit["level"], 2)
        self.assertTrue(fit["adoption_will_match"])
        self.assertFalse(fit["adoption_exact"])
        self.assertTrue(fit["adoption_partial"])
        self.assertAlmostEqual(fit["imbalance_qty"], 16.0)
        self.assertTrue(
            any("partial level 2/15" in warning for warning in result["warnings"])
        )
        self.assertFalse(
            any("imbalanced" in warning for warning in result["warnings"])
        )

    async def test_live_adopts_partial_full_grid_level(self) -> None:
        self.service._auto_arb["rules"]["partial-adopt"] = {
            "id": "partial-adopt",
            "mode": "shadow",
            "setup_mode": "adopt_existing_full_grid",
            "symbol": "HUSDT",
            "long_exchange": "bybit",
            "short_exchange": "binance",
            "max_qty": 20000.0,
            "chunk_qty": 20000.0 / 15.0,
            "levels": [
                {
                    "level": level,
                    "qty": 20000.0 / 15.0,
                    "cumulative_qty": level * (20000.0 / 15.0),
                }
                for level in range(1, 16)
            ],
        }
        self.service._accounts.refresh_now_for_protective = AsyncMock(return_value=None)
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {
                        "exchange": "bybit",
                        "symbol": "H/USDT:USDT",
                        "side": "long",
                        "coin_qty": 2190.0,
                    },
                    {
                        "exchange": "binance",
                        "symbol": "H/USDT:USDT",
                        "side": "short",
                        "coin_qty": -2206.0,
                    },
                ]
            }
        )

        result = await self.service.arm_auto_arb_live(
            "partial-adopt",
            "LIVE partial-adopt",
        )

        self.assertEqual(result["rule"]["mode"], "live")
        self.assertEqual(result["rule"]["live_level"], 2)
        self.assertEqual(result["rule"]["adopted_level"], 2)
        self.assertAlmostEqual(result["rule"]["adopted_qty"], 2190.0)

    async def test_adopt_full_grid_carries_small_hedge_imbalance_to_next_step(self) -> None:
        rule = {
            "id": "adopt-dust-carry",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "setup_mode": "adopt_existing_full_grid",
            "symbol": "HUSDT",
            "long_exchange": "bybit",
            "short_exchange": "binance",
            "max_qty": 25000.0,
            "chunk_qty": 25000.0 / 15.0,
            "levels": [
                {
                    "level": level,
                    "qty": 25000.0 / 15.0,
                    "cumulative_qty": level * (25000.0 / 15.0),
                }
                for level in range(1, 16)
            ],
            "live_level": 3,
            "actual_hedged_qty": 3345.0,
            "active_execution_id": "tiny-exit",
            "active_action": "exit",
            "active_from_level": 3,
            "active_to_level": 2,
            "active_target_qty": 3333.333333333333,
            "active_start_hedged_qty": 3345.0,
            "pending_transition": {
                "action": "exit",
                "from_level": 3,
                "to_level": 2,
                "target_qty": 11.66666666666697,
                "filled_qty": 0.0,
                "remaining_qty": 11.66666666666697,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual_runs["tiny-exit"] = {
            "status": "completed",
            "result": {
                "remaining_qty": 11.66666666666697,
                "warnings": ["remaining qty below exchange minimum; unable to execute final chunk"],
            },
            "error": None,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 3350.0,
                "short_qty": 3334.0,
                "hedged_qty": 3334.0,
                "imbalance_qty": 16.0,
                "imbalance_pct": 16.0 / 3334.0 * 100.0,
            }
        )
        self.service.manual_orphan_cleanup = AsyncMock(
            return_value={"execution_id": "should-not-repair", "status": "running"}
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.service.manual_orphan_cleanup.assert_not_awaited()
        self.assertEqual(rule["live_level"], 2)
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["status"], "monitoring")
        self.assertIsNone(rule["blocked_reason"])

    async def test_live_cycle_clears_zero_fill_pending_exit_when_entry_recovers(self) -> None:
        levels = [
            {
                "level": level,
                "entry_spread_pct": -3.0 - (level - 1) * 2.0,
                "exit_spread_pct": -1.0 - (level - 1) * 2.0,
                "qty": 100.0,
                "cumulative_qty": level * 100.0,
            }
            for level in range(1, 16)
        ]
        rule = {
            "id": "stale-exit",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "binance",
            "short_exchange": "bybit",
            "level_count": 15,
            "levels": levels,
            "live_level": 12,
            "actual_hedged_qty": 1200.0,
            "confirm_samples": 1,
            "max_levels_per_cycle": 1,
            "pending_transition": {
                "action": "exit",
                "from_level": 12,
                "to_level": 11,
                "target_qty": 100.0,
                "filled_qty": 0.0,
                "remaining_qty": 100.0,
            },
            "last_execution": {
                "execution_id": "zero-exit",
                "status": "completed",
                "result": {
                    "remaining_qty": 100.0,
                    "warnings": ["condition_not_met"],
                },
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service.auto_arb_spreads = AsyncMock(
            return_value={"entry_spread_pct": -33.0, "exit_spread_pct": -33.0}
        )
        self.service._start_auto_arb_live_transition = AsyncMock()

        await self.service._auto_arb_cycle()

        self.service._start_auto_arb_live_transition.assert_awaited_once_with(
            "stale-exit",
            "enter",
            12,
            13,
        )
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["status"], "queued_enter")
        self.assertTrue(rule["last_decision"]["stale_pending_exit_cleared"])
        history = self.service.auto_arb_history("stale-exit")
        self.assertEqual(history["events"][-1]["event"], "live_pending_exit_cleared")

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

    async def test_live_accepts_budget_above_old_restricted_limits(self) -> None:
        self.service._auto_arb["rules"]["large"] = {
            "id": "large",
            "mode": "shadow",
            "symbol": "BTCUSDT",
            "long_exchange": "bybit",
            "short_exchange": "kucoin",
            "max_qty": 20.0,
            "chunk_notional_estimate": 60.0,
            "total_notional_estimate": 120.0,
            "levels": [
                {"level": 1, "qty": 10.0, "cumulative_qty": 10.0},
                {"level": 2, "qty": 10.0, "cumulative_qty": 20.0},
            ],
        }
        self.service._accounts.refresh_now_for_protective = AsyncMock(return_value=None)
        self.service._accounts.snapshot = MagicMock(return_value={"positions": []})

        result = await self.service.arm_auto_arb_live("large", "LIVE large")

        self.assertEqual(result["rule"]["mode"], "live")
        self.assertTrue(result["rule"]["enabled"])

    def test_live_grid_conflicts_with_symbol_wide_multileg_auto_exit(self) -> None:
        self.service._auto_exit["rules"]["HUSDT|multileg|multileg"] = {
            "symbol": "HUSDT",
            "long_exchange": "multileg",
            "short_exchange": "multileg",
            "enabled": True,
            "v1_enabled": False,
        }

        conflict = self.service._auto_arb_auto_exit_conflict(
            {
                "symbol": "HUSDT",
                "long_exchange": "kucoin",
                "short_exchange": "bybit",
            }
        )

        self.assertTrue(conflict)

    async def test_live_adopts_near_level_position_with_small_imbalance(self) -> None:
        self.service._auto_arb["rules"]["adopt"] = {
            "id": "adopt",
            "mode": "shadow",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 10000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {"level": 1, "qty": 2500.0, "cumulative_qty": 2500.0},
                {"level": 2, "qty": 2500.0, "cumulative_qty": 5000.0},
                {"level": 3, "qty": 2500.0, "cumulative_qty": 7500.0},
                {"level": 4, "qty": 2500.0, "cumulative_qty": 10000.0},
            ],
        }
        self.service._accounts.refresh_now_for_protective = AsyncMock(return_value=None)
        self.service._accounts.snapshot = MagicMock(
            return_value={
                "positions": [
                    {"symbol": "HUSDT", "exchange": "kucoin", "side": "long", "quantity": 5000},
                    {"symbol": "HUSDT", "exchange": "bybit", "side": "short", "quantity": -4998},
                ]
            }
        )

        result = await self.service.arm_auto_arb_live("adopt", "LIVE adopt")

        self.assertEqual(result["rule"]["live_level"], 2)
        self.assertEqual(result["rule"]["adopted_level"], 2)
        self.assertAlmostEqual(result["rule"]["adopted_qty"], 4998.0)

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
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 0.0,
                "short_qty": 0.0,
                "hedged_qty": 0.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )

        await self.service._auto_arb_cycle()

        rule = self.service._auto_arb["rules"]["live1"]
        self.assertEqual(rule["active_execution_id"], "gridexec1")
        self.assertEqual(rule["active_from_level"], 0)
        self.assertEqual(rule["active_to_level"], 1)
        self.assertEqual(rule["status"], "executing_enter")
        payload = self.service.manual_enter.await_args.args[0]
        self.assertEqual(payload["qty"], 10.0)
        self.assertIsNone(payload["chunk_qty"])
        self.assertFalse(payload["force_chunk_qty"])
        self.assertGreater(payload["chunk_notional"], 0)
        self.assertTrue(payload["use_orderbook_check"])
        self.assertTrue(payload["allow_liquidity_chunking"])
        self.assertTrue(payload["auto_arb_agent"])

    async def test_live_partial_level_keeps_residual_for_retry(self) -> None:
        rule = {
            "id": "partial-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 10000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {"level": 1, "qty": 2500.0, "cumulative_qty": 2500.0},
                {"level": 2, "qty": 2500.0, "cumulative_qty": 5000.0},
                {"level": 3, "qty": 2500.0, "cumulative_qty": 7500.0},
                {"level": 4, "qty": 2500.0, "cumulative_qty": 10000.0},
            ],
            "live_level": 2,
            "actual_hedged_qty": 5000.0,
            "active_execution_id": "grid-partial",
            "active_action": "enter",
            "active_from_level": 2,
            "active_to_level": 3,
            "active_target_qty": 7500.0,
            "active_start_hedged_qty": 5000.0,
            "pending_transition": {
                "action": "enter",
                "from_level": 2,
                "to_level": 3,
                "target_qty": 2500.0,
                "filled_qty": 0.0,
                "remaining_qty": 2500.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual_runs["grid-partial"] = {
            "status": "completed",
            "result": {"remaining_qty": 1000.0},
            "error": None,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 6500.0,
                "short_qty": 6500.0,
                "hedged_qty": 6500.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.assertTrue(rule["enabled"])
        self.assertEqual(rule["live_level"], 2)
        self.assertEqual(rule["status"], "partial_enter")
        self.assertAlmostEqual(rule["pending_transition"]["filled_qty"], 1500.0)
        self.assertAlmostEqual(rule["pending_transition"]["remaining_qty"], 1000.0)

    async def test_live_level_completes_with_small_dust(self) -> None:
        rule = {
            "id": "dust-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 10000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {"level": 1, "qty": 2500.0, "cumulative_qty": 2500.0},
                {"level": 2, "qty": 2500.0, "cumulative_qty": 5000.0},
                {"level": 3, "qty": 2500.0, "cumulative_qty": 7500.0},
                {"level": 4, "qty": 2500.0, "cumulative_qty": 10000.0},
            ],
            "live_level": 2,
            "active_execution_id": "grid-dust",
            "active_action": "enter",
            "active_from_level": 2,
            "active_to_level": 3,
            "active_target_qty": 7500.0,
            "active_start_hedged_qty": 5000.0,
            "pending_transition": {
                "action": "enter",
                "from_level": 2,
                "to_level": 3,
                "target_qty": 2500.0,
                "filled_qty": 0.0,
                "remaining_qty": 2500.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual_runs["grid-dust"] = {
            "status": "completed",
            "result": {"remaining_qty": 20.0},
            "error": None,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 7480.0,
                "short_qty": 7480.0,
                "hedged_qty": 7480.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.assertEqual(rule["live_level"], 3)
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["status"], "monitoring")

    async def test_live_non_closeable_exit_dust_completes_level(self) -> None:
        rule = {
            "id": "dust-min-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "bybit",
            "short_exchange": "binance",
            "max_qty": 7000.0,
            "chunk_qty": 466.6666666666667,
            "levels": [
                {"level": level, "qty": 466.6666666666667, "cumulative_qty": level * 466.6666666666667}
                for level in range(1, 16)
            ],
            "live_level": 15,
            "active_execution_id": "grid-dust-min",
            "active_action": "exit",
            "active_from_level": 15,
            "active_to_level": 14,
            "active_target_qty": 6533.333333333333,
            "active_start_hedged_qty": 6540.0,
            "pending_transition": {
                "action": "exit",
                "from_level": 15,
                "to_level": 14,
                "target_qty": 466.666666666665,
                "filled_qty": 460.0,
                "remaining_qty": 6.66666666666498,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual_runs["grid-dust-min"] = {
            "status": "completed_with_errors",
            "result": {
                "errors": ["bybit: qty 6.66667 below min qty 10"],
                "warnings": [
                    "remaining qty below exchange minimum; unable to execute final chunk",
                    "bybit: non-closeable dust 6.66667 (qty 6.66667 below min qty 10)",
                ],
                "actions": [
                    {
                        "exchange": "bybit",
                        "status": "error",
                        "error_type": "min_order_size",
                        "error": "qty 6.66667 below min qty 10",
                    }
                ],
            },
            "error": None,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 6540.0,
                "short_qty": 6540.0,
                "hedged_qty": 6540.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.assertEqual(rule["live_level"], 14)
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["status"], "monitoring")
        self.assertIsNone(rule["blocked_reason"])
        self.assertAlmostEqual(rule["actual_hedged_qty"], 6540.0)

    async def test_live_large_imbalance_starts_reduce_only_repair(self) -> None:
        rule = {
            "id": "repair-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 10000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {"level": 1, "qty": 2500.0, "cumulative_qty": 2500.0},
                {"level": 2, "qty": 2500.0, "cumulative_qty": 5000.0},
                {"level": 3, "qty": 2500.0, "cumulative_qty": 7500.0},
                {"level": 4, "qty": 2500.0, "cumulative_qty": 10000.0},
            ],
            "live_level": 2,
            "active_execution_id": "grid-imbalanced",
            "active_action": "enter",
            "active_from_level": 2,
            "active_to_level": 3,
            "active_target_qty": 7500.0,
            "active_start_hedged_qty": 5000.0,
            "pending_transition": {
                "action": "enter",
                "from_level": 2,
                "to_level": 3,
                "target_qty": 2500.0,
                "filled_qty": 0.0,
                "remaining_qty": 2500.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual_runs["grid-imbalanced"] = {
            "status": "completed",
            "result": {"remaining_qty": 1000.0},
            "error": None,
        }
        quantities = {
            "long_qty": 6600.0,
            "short_qty": 6500.0,
            "hedged_qty": 6500.0,
            "imbalance_qty": 100.0,
            "imbalance_pct": 100.0 / 6500.0 * 100.0,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(return_value=quantities)
        self.service.manual_orphan_cleanup = AsyncMock(
            return_value={"execution_id": "grid-repair", "status": "running"}
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.assertEqual(rule["active_execution_id"], "grid-repair")
        self.assertEqual(rule["active_action"], "repair")
        self.assertEqual(rule["status"], "repairing_hedge")
        repair_payload = self.service.manual_orphan_cleanup.await_args.args[0]
        self.assertEqual(repair_payload["cleanup_exchange"], "kucoin")
        self.assertEqual(repair_payload["cleanup_position_side"], "long")
        self.assertAlmostEqual(repair_payload["qty"], 100.0)

    async def test_live_missing_repair_run_retries_after_restart(self) -> None:
        rule = {
            "id": "missing-repair-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "bybit",
            "short_exchange": "binance",
            "max_qty": 7000.0,
            "chunk_qty": 466.6666666666667,
            "levels": [
                {"level": 13, "qty": 466.6666666666667, "cumulative_qty": 6066.666666666668},
                {"level": 14, "qty": 466.6666666666667, "cumulative_qty": 6533.333333333335},
            ],
            "live_level": 14,
            "active_execution_id": "lost-repair",
            "active_action": "repair",
            "active_start_hedged_qty": 6070.0,
            "actual_hedged_qty": 6070.0,
            "pending_transition": {
                "action": "exit",
                "from_level": 14,
                "to_level": 13,
                "target_qty": 473.3333333333321,
                "filled_qty": 470.0,
                "remaining_qty": 3.3333333333321207,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        quantities = {
            "long_qty": 6070.0,
            "short_qty": 6084.0,
            "hedged_qty": 6070.0,
            "imbalance_qty": 14.0,
            "imbalance_pct": 14.0 / 6070.0 * 100.0,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(return_value=quantities)
        self.service._manual.analyze_rebalance = AsyncMock(  # type: ignore[method-assign]
            return_value={"min_qty_required": 1.0, "errors": []}
        )
        self.service.manual_orphan_cleanup = AsyncMock(
            return_value={"execution_id": "repair-retry", "status": "running"}
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.assertTrue(rule["enabled"])
        self.assertEqual(rule["active_execution_id"], "repair-retry")
        self.assertEqual(rule["active_action"], "repair")
        self.assertEqual(rule["status"], "repairing_hedge")
        repair_payload = self.service.manual_orphan_cleanup.await_args.args[0]
        self.assertEqual(repair_payload["cleanup_exchange"], "binance")
        self.assertEqual(repair_payload["cleanup_position_side"], "short")
        self.assertAlmostEqual(repair_payload["qty"], 14.0)

    async def test_live_repair_below_exchange_min_completes_as_dust(self) -> None:
        rule = {
            "id": "repair-dust-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "bybit",
            "short_exchange": "binance",
            "max_qty": 7000.0,
            "chunk_qty": 466.6666666666667,
            "levels": [
                {"level": 13, "qty": 466.6666666666667, "cumulative_qty": 6066.666666666668},
                {"level": 14, "qty": 466.6666666666667, "cumulative_qty": 6533.333333333335},
            ],
            "live_level": 14,
            "actual_hedged_qty": 6070.0,
            "pending_transition": {
                "action": "exit",
                "from_level": 14,
                "to_level": 13,
                "target_qty": 473.3333333333321,
                "filled_qty": 470.0,
                "remaining_qty": 3.3333333333321207,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual.analyze_rebalance = AsyncMock(  # type: ignore[method-assign]
            return_value={"min_qty_required": 18.0, "errors": []}
        )
        self.service.manual_orphan_cleanup = AsyncMock(
            return_value={"execution_id": "should-not-start", "status": "running"}
        )
        quantities = {
            "long_qty": 6070.0,
            "short_qty": 6084.0,
            "hedged_qty": 6070.0,
            "imbalance_qty": 14.0,
            "imbalance_pct": 14.0 / 6070.0 * 100.0,
        }

        await self.service._start_auto_arb_hedge_repair(rule["id"], quantities)

        self.service.manual_orphan_cleanup.assert_not_awaited()
        self.assertEqual(rule["live_level"], 13)
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["status"], "monitoring")
        self.assertIsNone(rule["blocked_reason"])
        self.assertAlmostEqual(rule["actual_hedged_qty"], 6070.0)

    async def test_live_reconcile_refresh_failure_keeps_execution_for_retry(self) -> None:
        rule = {
            "id": "retry-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 10000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {"level": 1, "qty": 2500.0, "cumulative_qty": 2500.0},
                {"level": 2, "qty": 2500.0, "cumulative_qty": 5000.0},
            ],
            "live_level": 1,
            "active_execution_id": "grid-retry",
            "active_action": "enter",
            "active_from_level": 1,
            "active_to_level": 2,
            "active_target_qty": 5000.0,
            "active_start_hedged_qty": 2500.0,
            "pending_transition": {
                "action": "enter",
                "from_level": 1,
                "to_level": 2,
                "target_qty": 2500.0,
                "filled_qty": 0.0,
                "remaining_qty": 2500.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual_runs["grid-retry"] = {
            "status": "completed",
            "result": {"remaining_qty": 0.0},
            "error": None,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(
            side_effect=RuntimeError("positions unavailable")
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.assertEqual(rule["active_execution_id"], "grid-retry")
        self.assertEqual(rule["active_action"], "enter")
        self.assertEqual(rule["status"], "waiting_reconcile")
        self.assertGreater(rule["next_eligible_ts"], 0.0)

    async def test_live_partial_transition_retries_only_remaining_qty(self) -> None:
        rule = {
            "id": "continue-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 10000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {
                    "level": 1,
                    "entry_spread_pct": -7.0,
                    "exit_spread_pct": -6.5,
                    "qty": 2500.0,
                    "cumulative_qty": 2500.0,
                },
                {
                    "level": 2,
                    "entry_spread_pct": -13.0,
                    "exit_spread_pct": -12.5,
                    "qty": 2500.0,
                    "cumulative_qty": 5000.0,
                },
                {
                    "level": 3,
                    "entry_spread_pct": -19.0,
                    "exit_spread_pct": -18.5,
                    "qty": 2500.0,
                    "cumulative_qty": 7500.0,
                },
            ],
            "live_level": 2,
            "actual_hedged_qty": 6500.0,
            "confirm_samples": 1,
            "max_levels_per_cycle": 1,
            "pending_transition": {
                "action": "enter",
                "from_level": 2,
                "to_level": 3,
                "target_qty": 2500.0,
                "filled_qty": 1500.0,
                "remaining_qty": 1000.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service.auto_arb_spreads = AsyncMock(
            return_value={"entry_spread_pct": -20.0, "exit_spread_pct": -19.8}
        )
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 6500.0,
                "short_qty": 6500.0,
                "hedged_qty": 6500.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )
        self.service.manual_enter = AsyncMock(
            return_value={"execution_id": "grid-continue", "status": "running"}
        )

        await self.service._auto_arb_cycle()

        payload = self.service.manual_enter.await_args.args[0]
        self.assertAlmostEqual(payload["qty"], 1000.0)
        self.assertEqual(rule["active_execution_id"], "grid-continue")
        self.assertEqual(rule["pending_transition"]["remaining_qty"], 1000.0)

    async def test_live_exit_sizes_new_transition_from_actual_hedged_qty(self) -> None:
        levels = [
            {
                "level": level,
                "entry_spread_pct": 15.0 - level,
                "exit_spread_pct": 16.0 - level,
                "qty": 466.6666666666667,
                "cumulative_qty": level * 466.6666666666667,
            }
            for level in range(1, 16)
        ]
        rule = {
            "id": "actual-sized-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "bybit",
            "short_exchange": "binance",
            "max_qty": 7000.0,
            "chunk_qty": 466.6666666666667,
            "levels": levels,
            "live_level": 14,
            "actual_hedged_qty": 6540.0,
            "max_slippage_bps": 16.0,
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 6540.0,
                "short_qty": 6540.0,
                "hedged_qty": 6540.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )
        self.service.manual_exit = AsyncMock(
            return_value={"execution_id": "grid-actual", "status": "running"}
        )

        await self.service._start_auto_arb_live_transition(rule["id"], "exit", 14, 13)

        target_level_qty = levels[12]["cumulative_qty"]
        expected_qty = 6540.0 - target_level_qty
        payload = self.service.manual_exit.await_args.args[0]
        self.assertAlmostEqual(payload["qty"], expected_qty)
        self.assertEqual(rule["active_execution_id"], "grid-actual")
        self.assertAlmostEqual(rule["pending_transition"]["target_qty"], expected_qty)
        self.assertAlmostEqual(rule["pending_transition"]["remaining_qty"], expected_qty)

    async def test_live_cycle_self_heals_stale_non_closeable_dust(self) -> None:
        levels = [
            {
                "level": level,
                "entry_spread_pct": 15.0 - level,
                "exit_spread_pct": 16.0 - level,
                "qty": 466.6666666666667,
                "cumulative_qty": level * 466.6666666666667,
            }
            for level in range(1, 16)
        ]
        rule = {
            "id": "stale-dust-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "bybit",
            "short_exchange": "binance",
            "max_qty": 7000.0,
            "chunk_qty": 466.6666666666667,
            "confirm_samples": 1,
            "levels": levels,
            "live_level": 15,
            "actual_hedged_qty": 6540.0,
            "status": "retry_execution_error",
            "blocked_reason": "bybit: qty 6.66667 below min qty 10",
            "last_execution": {
                "status": "completed_with_errors",
                "result": {
                    "errors": ["bybit: qty 6.66667 below min qty 10"],
                    "warnings": [
                        "remaining qty below exchange minimum; unable to execute final chunk",
                        "bybit: non-closeable dust 6.66667 (qty 6.66667 below min qty 10)",
                    ],
                },
            },
            "pending_transition": {
                "action": "exit",
                "from_level": 15,
                "to_level": 14,
                "target_qty": 466.666666666665,
                "filled_qty": 460.0,
                "remaining_qty": 6.66666666666498,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service.auto_arb_spreads = AsyncMock(
            return_value={"entry_spread_pct": 3.0, "exit_spread_pct": 3.0}
        )
        self.service.manual_exit = AsyncMock(
            return_value={"execution_id": "should-not-start", "status": "running"}
        )

        await self.service._auto_arb_cycle()

        self.service.manual_exit.assert_not_awaited()
        self.assertEqual(rule["live_level"], 14)
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["status"], "monitoring")
        self.assertIsNone(rule["blocked_reason"])

    async def test_live_cycle_completes_pending_enter_inside_level_tolerance(self) -> None:
        levels = [
            {
                "level": level,
                "entry_spread_pct": -1.4285714285714286 * level,
                "exit_spread_pct": -1.4285714285714286 * (level - 1),
                "qty": 2500.0,
                "cumulative_qty": level * 2500.0,
            }
            for level in range(1, 9)
        ]
        rule = {
            "id": "micro-residual-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "binance",
            "short_exchange": "bybit",
            "max_qty": 20000.0,
            "chunk_qty": 2500.0,
            "confirm_samples": 1,
            "levels": levels,
            "live_level": 1,
            "actual_hedged_qty": 4984.0,
            "status": "partial_enter_waiting_trigger",
            "last_execution": {
                "status": "completed",
                "result": {
                    "warnings": [
                        "condition_not_met",
                        "Remaining qty 16 not entered (smart-enter runtime ended).",
                    ],
                    "remaining_qty": 16.0,
                },
            },
            "pending_transition": {
                "action": "enter",
                "from_level": 1,
                "to_level": 2,
                "target_qty": 16.0,
                "filled_qty": 0.0,
                "remaining_qty": 16.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service.auto_arb_spreads = AsyncMock(
            return_value={"entry_spread_pct": -2.67, "exit_spread_pct": -2.68}
        )
        self.service.manual_enter = AsyncMock(
            return_value={"execution_id": "should-not-start", "status": "running"}
        )

        await self.service._auto_arb_cycle()

        self.service.manual_enter.assert_not_awaited()
        self.assertEqual(rule["live_level"], 2)
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["status"], "monitoring")
        self.assertIsNone(rule["blocked_reason"])
        self.assertTrue(rule["last_decision"]["dust_completed"])

    async def test_live_partial_enter_reverses_when_exit_trigger_hits(self) -> None:
        rule = {
            "id": "reverse-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 10000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {
                    "level": 1,
                    "entry_spread_pct": -7.0,
                    "exit_spread_pct": -6.5,
                    "qty": 2500.0,
                    "cumulative_qty": 2500.0,
                },
                {
                    "level": 2,
                    "entry_spread_pct": -13.0,
                    "exit_spread_pct": -12.5,
                    "qty": 2500.0,
                    "cumulative_qty": 5000.0,
                },
                {
                    "level": 3,
                    "entry_spread_pct": -19.0,
                    "exit_spread_pct": -18.5,
                    "qty": 2500.0,
                    "cumulative_qty": 7500.0,
                },
            ],
            "live_level": 2,
            "actual_hedged_qty": 6500.0,
            "confirm_samples": 1,
            "max_levels_per_cycle": 1,
            "pending_transition": {
                "action": "enter",
                "from_level": 2,
                "to_level": 3,
                "target_qty": 2500.0,
                "filled_qty": 1500.0,
                "remaining_qty": 1000.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service.auto_arb_spreads = AsyncMock(
            return_value={"entry_spread_pct": -14.0, "exit_spread_pct": -12.0}
        )
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 6500.0,
                "short_qty": 6500.0,
                "hedged_qty": 6500.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )
        self.service.manual_enter = AsyncMock()
        self.service.manual_exit = AsyncMock(
            return_value={"execution_id": "grid-reversal", "status": "running"}
        )

        await self.service._auto_arb_cycle()

        self.service.manual_enter.assert_not_called()
        payload = self.service.manual_exit.await_args.args[0]
        self.assertEqual(payload["mode"], "smart-exit")
        self.assertAlmostEqual(payload["qty"], 1500.0)
        self.assertAlmostEqual(payload["spread_min_pct"], -12.5)
        self.assertEqual(rule["active_execution_id"], "grid-reversal")
        self.assertEqual(rule["active_action"], "exit")
        self.assertEqual(rule["active_from_level"], 3)
        self.assertEqual(rule["active_to_level"], 2)
        self.assertEqual(rule["pending_transition"]["action"], "exit")
        self.assertAlmostEqual(rule["pending_transition"]["target_qty"], 1500.0)
        self.assertEqual(rule["pending_transition"]["reversal_of"]["action"], "enter")
        self.assertTrue(rule["last_decision"]["reversal"])

    async def test_live_zero_fill_balance_error_uses_backoff(self) -> None:
        rule = {
            "id": "balance-grid",
            "generation": 1,
            "enabled": True,
            "mode": "live",
            "symbol": "HUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "max_qty": 5000.0,
            "chunk_qty": 2500.0,
            "levels": [
                {"level": 1, "qty": 2500.0, "cumulative_qty": 2500.0},
                {"level": 2, "qty": 2500.0, "cumulative_qty": 5000.0},
            ],
            "live_level": 1,
            "active_execution_id": "grid-balance",
            "active_action": "enter",
            "active_from_level": 1,
            "active_to_level": 2,
            "active_target_qty": 5000.0,
            "active_start_hedged_qty": 2500.0,
            "pending_transition": {
                "action": "enter",
                "from_level": 1,
                "to_level": 2,
                "target_qty": 2500.0,
                "filled_qty": 0.0,
                "remaining_qty": 2500.0,
            },
        }
        self.service._auto_arb["rules"][rule["id"]] = rule
        self.service._manual_runs["grid-balance"] = {
            "status": "completed_with_errors",
            "result": {
                "remaining_qty": 2500.0,
                "errors": ["insufficient balance"],
            },
            "error": None,
        }
        self.service._auto_arb_refresh_quantities = AsyncMock(
            return_value={
                "long_qty": 2500.0,
                "short_qty": 2500.0,
                "hedged_qty": 2500.0,
                "imbalance_qty": 0.0,
                "imbalance_pct": 0.0,
            }
        )

        await self.service._reconcile_auto_arb_execution(rule["id"])

        self.assertTrue(rule["enabled"])
        self.assertEqual(rule["status"], "blocked_balance")
        self.assertGreater(rule["next_eligible_ts"], time.time() + 50.0)

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
