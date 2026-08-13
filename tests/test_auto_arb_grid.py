from __future__ import annotations

import json
from pathlib import Path
import unittest

from execution.auto_arb_grid import (
    apply_grid_decision_confirmation,
    complete_pending_grid_transition,
    build_grid_levels,
    build_grid_pending_transition,
    decide_grid_transition,
    reduce_partial_grid_transition,
    grid_completion_tolerance,
    grid_level_count_for_existing_qty,
    grid_rules_share_live_ownership,
    grid_symbol_ownership_key,
    recommend_level_count,
)


class AutoArbGridTestCase(unittest.TestCase):
    def test_pending_transition_builder_matches_golden_fixtures(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "grid_pending_transition_v1.json"
        cases = json.loads(fixture_path.read_text(encoding="utf-8"))
        for case in cases:
            with self.subTest(case=case["name"]):
                result = build_grid_pending_transition(
                    existing_transition=case.get("existing_transition"),
                    action=case["action"],
                    from_level=case["from_level"],
                    to_level=case["to_level"],
                    level_qty=case["level_qty"],
                    level_target_qty=case["level_target_qty"],
                    current_hedged_qty=case["current_hedged_qty"],
                    now_iso="2026-08-13T00:00:00+00:00",
                )
                for key, expected in case["expected_result"].items():
                    self.assertEqual(result.get(key), expected, key)
                for key, expected in case["expected_transition"].items():
                    self.assertEqual(result["transition"].get(key), expected, key)
                for key in case.get("absent_transition_keys", []):
                    self.assertNotIn(key, result["transition"])

    def test_partial_transition_reducer_matches_golden_fixtures(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "grid_partial_transition_v1.json"
        cases = json.loads(fixture_path.read_text(encoding="utf-8"))
        for case in cases:
            with self.subTest(case=case["name"]):
                rule = dict(case["rule"])
                pending_transition = dict(case["pending_transition"])
                result = reduce_partial_grid_transition(
                    rule,
                    pending_transition=pending_transition,
                    current_level=case["current_level"],
                    entry_spread_pct=case["entry_spread_pct"],
                    exit_spread_pct=case["exit_spread_pct"],
                    now_iso="2026-08-13T00:00:00+00:00",
                )
                for key, expected in case["expected_decision"].items():
                    self.assertEqual(result["decision"].get(key), expected, key)
                for key, expected in case["expected_pending_transition"].items():
                    self.assertEqual(
                        result["pending_transition"].get(key), expected, key
                    )
                for key, expected in case.get("expected_rule", {}).items():
                    self.assertEqual(rule.get(key), expected, key)
                event = result.get("transition_event")
                self.assertEqual(
                    event.get("event") if event else None,
                    case.get("expected_event"),
                )

    def test_confirmation_reducer_matches_golden_fixtures(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "grid_transition_confirmation_v1.json"
        cases = json.loads(fixture_path.read_text(encoding="utf-8"))
        for case in cases:
            with self.subTest(case=case["name"]):
                rule = dict(case["rule"])
                result = apply_grid_decision_confirmation(
                    rule,
                    decision=dict(case["decision"]),
                    mode=case["mode"],
                    current_level=case["current_level"],
                    pending_transition=(
                        dict(case["pending_transition"])
                        if case["pending_transition"] is not None
                        else None
                    ),
                    entry_spread_pct=-2.0,
                    exit_spread_pct=-1.9,
                    now_iso="2026-08-13T00:00:00+00:00",
                    now_ts=case["now_ts"],
                )
                for key, expected in case["expected_rule"].items():
                    self.assertEqual(rule.get(key), expected, key)
                normalized_result = dict(result)
                if isinstance(normalized_result.get("live_transition"), tuple):
                    normalized_result["live_transition"] = list(
                        normalized_result["live_transition"]
                    )
                for key, expected in case["expected_result"].items():
                    self.assertEqual(normalized_result.get(key), expected, key)

    def test_builds_inclusive_negative_spread_levels(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=10_000,
        )

        self.assertEqual([row["entry_spread_pct"] for row in levels], [-2, -4, -6, -8, -10])
        self.assertEqual([row["exit_spread_pct"] for row in levels], [-1.5, -3.5, -5.5, -7.5, -9.5])
        self.assertEqual(levels[-1]["cumulative_qty"], 10_000)
        self.assertEqual(levels[0]["qty"], 2_000)

    def test_rejects_reversed_range(self) -> None:
        with self.assertRaises(ValueError):
            build_grid_levels(
                range_start_pct=-10,
                range_end_pct=-2,
                level_count=5,
                exit_gap_pct=0.5,
                max_qty=100,
            )

    def test_enters_only_one_level_after_large_jump(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=100,
        )
        decision = decide_grid_transition(
            entry_spread_pct=-9,
            exit_spread_pct=-8.8,
            levels=levels,
            current_level=0,
            max_levels_per_cycle=1,
        )

        self.assertEqual(decision["entry_target_level"], 4)
        self.assertEqual(decision["action"], "enter")
        self.assertEqual(decision["target_level"], 1)

    def test_exits_last_opened_level_on_reversion(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=100,
        )
        decision = decide_grid_transition(
            entry_spread_pct=-5.8,
            exit_spread_pct=-5.4,
            levels=levels,
            current_level=3,
        )

        self.assertEqual(decision["action"], "exit")
        self.assertEqual(decision["exit_target_level"], 2)
        self.assertEqual(decision["target_level"], 2)

    def test_hysteresis_holds_position_between_thresholds(self) -> None:
        levels = build_grid_levels(
            range_start_pct=-2,
            range_end_pct=-10,
            level_count=5,
            exit_gap_pct=0.5,
            max_qty=100,
        )
        decision = decide_grid_transition(
            entry_spread_pct=-4.1,
            exit_spread_pct=-4.2,
            levels=levels,
            current_level=2,
        )

        self.assertEqual(decision["action"], "none")
        self.assertEqual(decision["target_level"], 2)

    def test_recommends_bounded_level_count(self) -> None:
        self.assertEqual(recommend_level_count(total_qty=10_000, safe_chunk_qty=1_200), 9)
        self.assertEqual(recommend_level_count(total_qty=10_000, safe_chunk_qty=20_000), 2)
        self.assertEqual(recommend_level_count(total_qty=10_000, safe_chunk_qty=10), 20)

    def test_live_ownership_normalizes_contract_symbols_and_exchange_alias(self) -> None:
        left = {
            "symbol": "TUT/USDT:USDT",
            "long_exchange": "kukoin",
            "short_exchange": "bybit",
        }
        right = {
            "symbol": "TUTUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "okx",
        }

        self.assertEqual(grid_symbol_ownership_key(left), "TUT")
        self.assertTrue(grid_rules_share_live_ownership(left, right))

    def test_quantity_fit_uses_same_one_percent_completion_tolerance(self) -> None:
        fit = grid_level_count_for_existing_qty(
            total_qty=10_000,
            existing_qty=2_010,
            preferred_count=5,
        )

        self.assertIsNotNone(fit)
        self.assertEqual(fit["level_count"], 5)
        self.assertEqual(fit["level"], 1)
        self.assertAlmostEqual(grid_completion_tolerance({"chunk_qty": 2_000}), 20.0)

    def test_confirmation_reducer_matches_two_sample_shadow_transition(self) -> None:
        rule = {
            "id": "grid-1",
            "generation": 3,
            "symbol": "TUTUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "confirm_samples": 2,
            "pending_action": "enter",
            "pending_samples": 1,
            "levels": [{"cumulative_qty": 100.0}],
        }
        result = apply_grid_decision_confirmation(
            rule,
            decision={"action": "enter", "target_level": 1},
            mode="shadow",
            current_level=0,
            pending_transition=None,
            entry_spread_pct=-2.1,
            exit_spread_pct=-1.8,
            now_iso="2026-08-13T00:00:00+00:00",
            now_ts=100.0,
        )

        self.assertEqual(rule["status"], "shadow_enter")
        self.assertEqual(rule["shadow_level"], 1)
        self.assertEqual(rule["shadow_qty"], 100.0)
        self.assertIsNone(rule["pending_action"])
        self.assertEqual(result["transition_event"]["event"], "shadow_enter")
        self.assertIsNone(result["live_transition"])

    def test_confirmation_reducer_queues_live_transition_without_executing(self) -> None:
        rule = {
            "id": "grid-live",
            "confirm_samples": 1,
            "levels": [{"cumulative_qty": 100.0}],
        }
        result = apply_grid_decision_confirmation(
            rule,
            decision={"action": "enter", "target_level": 1},
            mode="live",
            current_level=0,
            pending_transition=None,
            entry_spread_pct=-2.0,
            exit_spread_pct=-1.9,
            now_iso="2026-08-13T00:00:00+00:00",
            now_ts=100.0,
        )

        self.assertEqual(rule["status"], "queued_enter")
        self.assertEqual(result["live_transition"], ("grid-live", "enter", 0, 1))
        self.assertIsNone(result["transition_event"])

    def test_confirmation_reducer_blocks_entry_during_risk_cooldown(self) -> None:
        rule = {
            "id": "grid-live",
            "entry_next_eligible_ts": 200.0,
            "entry_blocked_reason": "risk limit",
            "pending_action": "enter",
            "pending_samples": 1,
        }
        result = apply_grid_decision_confirmation(
            rule,
            decision={"action": "enter", "target_level": 1},
            mode="live",
            current_level=0,
            pending_transition=None,
            entry_spread_pct=-2.0,
            exit_spread_pct=-1.9,
            now_iso="2026-08-13T00:00:00+00:00",
            now_ts=100.0,
        )

        self.assertEqual(rule["status"], "blocked_risk_limit")
        self.assertEqual(rule["blocked_reason"], "risk limit")
        self.assertIsNone(rule["pending_action"])
        self.assertIsNone(result["live_transition"])

    def test_pending_transition_completes_within_tolerance(self) -> None:
        rule = {
            "id": "grid-partial",
            "generation": 2,
            "symbol": "TUTUSDT",
            "long_exchange": "kucoin",
            "short_exchange": "bybit",
            "chunk_qty": 100.0,
            "live_level": 2,
            "pending_action": "exit",
            "pending_samples": 2,
        }
        result = complete_pending_grid_transition(
            rule,
            pending_transition={
                "action": "exit",
                "from_level": 2,
                "to_level": 1,
                "target_qty": 100.0,
                "filled_qty": 99.5,
                "remaining_qty": 0.5,
            },
            current_level=2,
            last_result=None,
            now_iso="2026-08-13T00:00:00+00:00",
            now_ts=100.0,
            retry_sec=2.0,
        )

        self.assertIsNotNone(result)
        self.assertEqual(rule["live_level"], 1)
        self.assertEqual(rule["status"], "monitoring")
        self.assertIsNone(rule["pending_transition"])
        self.assertEqual(rule["next_eligible_ts"], 102.0)
        self.assertEqual(result["transition_event"]["event"], "live_exit")

    def test_pending_transition_does_not_complete_material_remainder(self) -> None:
        rule = {"id": "grid-partial", "chunk_qty": 100.0, "live_level": 2}

        result = complete_pending_grid_transition(
            rule,
            pending_transition={
                "action": "exit",
                "from_level": 2,
                "to_level": 1,
                "target_qty": 100.0,
                "filled_qty": 80.0,
                "remaining_qty": 20.0,
            },
            current_level=2,
            last_result={"errors": []},
            now_iso="2026-08-13T00:00:00+00:00",
            now_ts=100.0,
            retry_sec=2.0,
        )

        self.assertIsNone(result)
        self.assertEqual(rule["live_level"], 2)


if __name__ == "__main__":
    unittest.main()
