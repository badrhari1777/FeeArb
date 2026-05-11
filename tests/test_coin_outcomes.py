from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from analysis_storage.coin_db import (
    CoinDecisionRow,
    CoinFeatureSnapshotRow,
    CoinFocusSnapshotRow,
    CoinPaperPositionRow,
    CoinRealPositionObservationRow,
    get_outcomes,
    insert_decision,
    insert_feature_snapshot,
    insert_focus_snapshot,
    insert_outcome,
    insert_real_position_observation,
    upsert_paper_position,
    set_test_db_path,
)
from webapp.services import DataService


class CoinOutcomesServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_outcomes_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_evaluate_and_get_outcomes(self) -> None:
        async def _run() -> None:
            decision_ts = 1_700_000_000_000
            target_ts = decision_ts + 15 * 60 * 1000
            wait_ts = target_ts + 15 * 60 * 1000

            feature_id = insert_feature_snapshot(
                CoinFeatureSnapshotRow(
                    ts_ms=decision_ts,
                    pair_key="BTCUSDT|binance|kucoin",
                    canonical_symbol="BTCUSDT",
                    context_mode="candidate",
                    feature_set_version="v1",
                    direction="long_a_short_b",
                    features={
                        "common": {
                            "left_exchange": "binance",
                            "right_exchange": "kucoin",
                            "decision_phase": "pre_boundary_20m",
                            "funding": {
                                "left_interval_hours": 1.0,
                                "right_interval_hours": 1.0,
                            },
                        },
                        "directional": {
                            "open_spread_pct": -1.2,
                            "funding_to_next_pct": 0.01,
                        },
                    },
                    data_quality={"coverage_pct": 98.0},
                )
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="outcome-decision-1",
                    ts_ms=decision_ts,
                    mode="manual_candidate",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="ENTRY_SMALL",
                    decision_phase="pre_boundary_20m",
                    confidence_score=72.0,
                    reason_codes=["spread_reversion_favorable"],
                    reason_text=["spread below median"],
                    scores={"entry_score": 72.0},
                    features_ref=str(feature_id),
                )
            )

            # Entry-time reference snapshot.
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=decision_ts,
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    exchange_symbol="BTCUSDT",
                    bid=99.0,
                    ask=99.2,
                    mid=99.1,
                    mark_price=99.1,
                    next_funding_ts_ms=target_ts,
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=decision_ts,
                    canonical_symbol="BTCUSDT",
                    exchange="kucoin",
                    exchange_symbol="XBTUSDTM",
                    bid=99.9,
                    ask=100.0,
                    mid=99.95,
                    mark_price=99.95,
                    next_funding_ts_ms=target_ts,
                )
            )

            # 15m horizon snapshot (improvement vs entry).
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=target_ts,
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    exchange_symbol="BTCUSDT",
                    bid=99.2,
                    ask=99.3,
                    mid=99.25,
                    mark_price=99.25,
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=target_ts,
                    canonical_symbol="BTCUSDT",
                    exchange="kucoin",
                    exchange_symbol="XBTUSDTM",
                    bid=99.95,
                    ask=100.0,
                    mid=99.975,
                    mark_price=99.975,
                )
            )

            # +15m alternative snapshot.
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=wait_ts,
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    exchange_symbol="BTCUSDT",
                    bid=99.4,
                    ask=99.5,
                    mid=99.45,
                    mark_price=99.45,
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=wait_ts,
                    canonical_symbol="BTCUSDT",
                    exchange="kucoin",
                    exchange_symbol="XBTUSDTM",
                    bid=99.95,
                    ask=100.0,
                    mid=99.975,
                    mark_price=99.975,
                )
            )

            first = await self.service.evaluate_coin_outcomes(
                "BTCUSDT",
                horizons=["15m"],
                decision_limit=100,
                force=False,
            )
            self.assertEqual(first["evaluated"], 1)
            self.assertEqual(first["skipped"], 0)
            self.assertEqual(first["summary"]["total"], 1)
            self.assertEqual(first["summary"]["by_phase_bucket"]["pre_boundary"]["total"], 1)
            scorecard = first["summary"]["operator_scorecard_pre_boundary"]
            self.assertEqual(scorecard["phase_bucket"], "pre_boundary")
            self.assertEqual(scorecard["total_rows"], 1)
            self.assertEqual(scorecard["overall"]["correct"], 1)
            self.assertEqual(scorecard["overall"]["known_total"], 1)
            self.assertEqual(scorecard["overall"]["hit_rate_pct"], 100.0)
            self.assertEqual(scorecard["overall"]["wait_help_true"], 1)
            self.assertEqual(scorecard["overall"]["wait_help_rate_pct"], 100.0)
            self.assertEqual(scorecard["traffic_light"]["status"], "yellow")
            self.assertIn("sample_depth_low", scorecard["traffic_light"]["reasons"])
            outcome = first["rows"][0]["outcome"]
            self.assertEqual(outcome["decision_correctness"], "correct")
            self.assertTrue(outcome["direction_component_correct"])
            self.assertTrue(outcome["spread_component_correct"])
            self.assertTrue(outcome["funding_component_correct"])
            self.assertLess(outcome["fees_pnl_delta_pct"], 0.0)
            self.assertLess(outcome["slippage_pnl_delta_pct"], 0.0)
            self.assertGreater(outcome["net_pnl_delta_pct"], 0.0)
            self.assertEqual(
                set((outcome.get("pnl_assumptions") or {}).keys()),
                {"action_size_ratio", "fees_model", "slippage_bps_per_leg"},
            )
            self.assertTrue(outcome["would_waiting_15m_help"])

            listed = await self.service.get_coin_outcomes("BTCUSDT", limit=100)
            self.assertEqual(listed["symbol"], "BTCUSDT")
            self.assertEqual(listed["count"], 1)
            self.assertEqual(listed["rows"][0]["decision_id"], "outcome-decision-1")
            self.assertEqual(listed["summary"]["by_horizon"]["15m"]["correct"], 1)
            self.assertEqual(listed["summary"]["by_phase_bucket"]["pre_boundary"]["correct"], 1)
            self.assertEqual(listed["summary"]["operator_scorecard_pre_boundary"]["overall"]["hit_rate_pct"], 100.0)
            self.assertEqual(
                listed["summary"]["operator_scorecard_pre_boundary"]["traffic_light"]["status"],
                "yellow",
            )

            next_funding_eval = await self.service.evaluate_coin_outcomes(
                "BTCUSDT",
                horizons=["to_next_funding"],
                decision_limit=1,
                force=True,
            )
            self.assertEqual(next_funding_eval["evaluated"], 1)
            nf_outcome = next_funding_eval["rows"][0]["outcome"]
            self.assertEqual(nf_outcome["horizon"], "to_next_funding")
            self.assertEqual(nf_outcome["horizon_target_ts_ms"], target_ts)
            self.assertEqual(nf_outcome["size_appropriateness"], "too_conservative")
            self.assertAlmostEqual(nf_outcome["funding_pnl_delta_pct"], 0.01, places=6)

            insert_decision(
                CoinDecisionRow(
                    decision_id="outcome-decision-2",
                    ts_ms=decision_ts + 1_000,
                    mode="manual_position_review",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="FULL_EXIT",
                    decision_phase="mid_interval",
                    confidence_score=58.0,
                    reason_codes=["high_continuation_risk"],
                    reason_text=["risk increased"],
                    scores={"continuation_risk_score": 74.0},
                    features_ref=None,
                )
            )
            insert_outcome(
                "outcome-decision-2",
                "1h",
                {
                    "decision_correctness": "incorrect",
                    "timing_quality": "poor",
                    "decision_phase": "mid_interval",
                },
            )

            paper_key = "paper-to-exit-1"
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key=paper_key,
                    opened_at_ms=decision_ts - 5_000,
                    closed_at_ms=wait_ts,
                    status="closed",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=0.2,
                    entry_context={"source": "unit_test"},
                )
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="outcome-decision-3",
                    ts_ms=decision_ts + 2_000,
                    mode="manual_position_review",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="PARTIAL_EXIT",
                    decision_phase="pre_boundary_20m",
                    confidence_score=66.0,
                    reason_codes=["size_reduced_risk_control"],
                    reason_text=["reduce size"],
                    scores={"continuation_risk_score": 66.0},
                    features_ref=str(feature_id),
                    state_ref=paper_key,
                )
            )

            to_exit_eval = await self.service.evaluate_coin_outcomes(
                "BTCUSDT",
                horizons=["to_exit"],
                decision_limit=1,
                force=True,
            )
            self.assertEqual(to_exit_eval["evaluated"], 1)
            to_exit_outcome = to_exit_eval["rows"][0]["outcome"]
            self.assertEqual(to_exit_outcome["horizon"], "to_exit")
            self.assertEqual(to_exit_outcome["horizon_target_ts_ms"], wait_ts)

            real_state_ref = "real-btcusdt-binance-long-kucoin-short"
            insert_real_position_observation(
                CoinRealPositionObservationRow(
                    state_ref=real_state_ref,
                    ts_ms=decision_ts + 1500,
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    long_exchange="binance",
                    short_exchange="kucoin",
                    qty=0.3,
                    status="open",
                )
            )
            insert_real_position_observation(
                CoinRealPositionObservationRow(
                    state_ref=real_state_ref,
                    ts_ms=wait_ts + 5000,
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    long_exchange="binance",
                    short_exchange="kucoin",
                    qty=0.0,
                    status="closed",
                )
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="outcome-decision-4",
                    ts_ms=decision_ts + 3000,
                    mode="manual_position_review",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="FULL_EXIT",
                    decision_phase="pre_boundary_20m",
                    confidence_score=70.0,
                    reason_codes=["position_thesis_deteriorating"],
                    reason_text=["close now"],
                    scores={"continuation_risk_score": 80.0},
                    features_ref=str(feature_id),
                    state_ref=real_state_ref,
                )
            )

            real_to_exit_eval = await self.service.evaluate_coin_outcomes(
                "BTCUSDT",
                horizons=["to_exit"],
                decision_limit=1,
                force=True,
            )
            self.assertEqual(real_to_exit_eval["evaluated"], 1)
            real_to_exit_outcome = real_to_exit_eval["rows"][0]["outcome"]
            self.assertEqual(real_to_exit_outcome["horizon"], "to_exit")
            self.assertEqual(real_to_exit_outcome["horizon_target_ts_ms"], wait_ts + 5000)

            filtered_action = await self.service.get_coin_outcomes(
                "BTCUSDT",
                limit=100,
                actions=["ENTRY_SMALL"],
            )
            self.assertEqual(filtered_action["count"], 2)
            self.assertEqual(
                {row["decision_id"] for row in filtered_action["rows"]},
                {"outcome-decision-1"},
            )
            self.assertEqual(filtered_action["filters"]["actions"], ["ENTRY_SMALL"])

            filtered_phase = await self.service.get_coin_outcomes(
                "BTCUSDT",
                limit=100,
                phase_buckets=["mid_interval"],
            )
            self.assertEqual(filtered_phase["count"], 1)
            self.assertEqual(filtered_phase["rows"][0]["decision_id"], "outcome-decision-2")
            self.assertEqual(filtered_phase["filters"]["phase_buckets"], ["mid_interval"])

            filtered_horizon = await self.service.get_coin_outcomes(
                "BTCUSDT",
                limit=100,
                horizons=["15m"],
            )
            self.assertEqual(filtered_horizon["count"], 1)
            self.assertEqual(filtered_horizon["rows"][0]["decision_id"], "outcome-decision-1")
            self.assertEqual(filtered_horizon["filters"]["horizons"], ["15m"])

            filtered_combo = await self.service.get_coin_outcomes(
                "BTCUSDT",
                limit=100,
                horizons=["15m"],
                actions=["FULL_EXIT"],
            )
            self.assertEqual(filtered_combo["count"], 0)
            self.assertEqual(filtered_combo["summary"]["total"], 0)

            second = await self.service.evaluate_coin_outcomes(
                "BTCUSDT",
                horizons=["15m"],
                decision_limit=100,
                force=False,
            )
            self.assertGreaterEqual(second["evaluated"], 1)
            self.assertGreaterEqual(second["skipped"], 1)

            forced = await self.service.evaluate_coin_outcomes(
                "BTCUSDT",
                horizons=["15m"],
                decision_limit=100,
                force=True,
            )
            self.assertGreater(forced["evaluated"], second["evaluated"])

        asyncio.run(_run())

    def test_evaluate_outcomes_only_matured_gate(self) -> None:
        async def _run() -> None:
            decision_ts = 1_700_100_000_000
            target_ts = decision_ts + 15 * 60 * 1000

            feature_id = insert_feature_snapshot(
                CoinFeatureSnapshotRow(
                    ts_ms=decision_ts,
                    pair_key="ETHUSDT|binance|kucoin",
                    canonical_symbol="ETHUSDT",
                    context_mode="candidate",
                    feature_set_version="v1",
                    direction="long_a_short_b",
                    features={
                        "common": {
                            "left_exchange": "binance",
                            "right_exchange": "kucoin",
                            "decision_phase": "mid_interval",
                            "hours_to_next_funding_min": 1.0,
                        },
                        "directional": {
                            "open_spread_pct": -0.6,
                            "funding_to_next_pct": 0.002,
                            "net_funding_hourly": 0.002,
                        },
                    },
                    data_quality={"coverage_pct": 95.0},
                )
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="matured-decision-1",
                    ts_ms=decision_ts,
                    mode="manual_candidate",
                    canonical_symbol="ETHUSDT",
                    pair_key="ETHUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="ENTRY_SMALL",
                    decision_phase="mid_interval",
                    confidence_score=61.0,
                    reason_codes=["spread_reversion_favorable"],
                    reason_text=["test"],
                    scores={"entry_score": 61.0},
                    features_ref=str(feature_id),
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=decision_ts,
                    canonical_symbol="ETHUSDT",
                    exchange="binance",
                    exchange_symbol="ETHUSDT",
                    bid=100.0,
                    ask=100.1,
                    mid=100.05,
                    mark_price=100.05,
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=decision_ts,
                    canonical_symbol="ETHUSDT",
                    exchange="kucoin",
                    exchange_symbol="ETHUSDTM",
                    bid=100.6,
                    ask=100.7,
                    mid=100.65,
                    mark_price=100.65,
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=target_ts,
                    canonical_symbol="ETHUSDT",
                    exchange="binance",
                    exchange_symbol="ETHUSDT",
                    bid=100.3,
                    ask=100.4,
                    mid=100.35,
                    mark_price=100.35,
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=target_ts,
                    canonical_symbol="ETHUSDT",
                    exchange="kucoin",
                    exchange_symbol="ETHUSDTM",
                    bid=100.55,
                    ask=100.65,
                    mid=100.6,
                    mark_price=100.6,
                )
            )

            early = await self.service.evaluate_coin_outcomes(
                "ETHUSDT",
                horizons=["15m"],
                decision_limit=10,
                force=False,
                only_matured=True,
                now_ms=decision_ts + 5 * 60 * 1000,
            )
            self.assertEqual(early["evaluated"], 0)
            self.assertEqual(early["deferred"], 1)

            matured = await self.service.evaluate_coin_outcomes(
                "ETHUSDT",
                horizons=["15m"],
                decision_limit=10,
                force=False,
                only_matured=True,
                now_ms=decision_ts + 16 * 60 * 1000,
            )
            self.assertEqual(matured["evaluated"], 1)
            self.assertEqual(matured["deferred"], 0)
            self.assertEqual(matured["rows"][0]["decision_id"], "matured-decision-1")

        asyncio.run(_run())

    def test_outcomes_auto_status_symbol_pending(self) -> None:
        async def _run() -> None:
            insert_decision(
                CoinDecisionRow(
                    decision_id="status-decision-1",
                    ts_ms=1_700_200_000_000,
                    mode="manual_candidate",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="ENTRY_SMALL",
                    decision_phase="mid_interval",
                    confidence_score=60.0,
                    reason_codes=["spread_reversion_favorable"],
                    reason_text=["status test"],
                    scores={"entry_score": 60.0},
                    features_ref="1",
                )
            )

            status = await self.service.get_coin_outcomes_auto_status(symbol="BTCUSDT")
            self.assertIn("scheduler_running", status)
            self.assertIn("last_cycle", status)
            self.assertIn("recent_cycles", status)
            self.assertIn("health", status)
            self.assertEqual(status["health"]["status"], "stale")
            self.assertIn("scheduler_not_running", status["health"]["reasons"])
            self.assertEqual(status["symbol"], "BTCUSDT")
            pending = status["symbol_pending"]
            self.assertEqual(pending["decisions_total"], 1)
            self.assertEqual(pending["missing_total"], 5)
            self.assertEqual(pending["missing_by_horizon"]["15m"], 1)
            self.assertEqual(pending["missing_by_horizon"]["to_exit"], 1)

            insert_outcome(
                "status-decision-1",
                "15m",
                {"decision_correctness": "correct"},
            )
            cycle = await self.service.evaluate_matured_coin_outcomes_once(symbol="BTCUSDT")
            self.assertEqual(cycle["scope_symbol"], "BTCUSDT")
            status_after = await self.service.get_coin_outcomes_auto_status(symbol="BTCUSDT")
            pending_after = status_after["symbol_pending"]
            self.assertEqual(pending_after["missing_total"], 4)
            self.assertEqual(pending_after["missing_by_horizon"]["15m"], 0)
            self.assertGreaterEqual(len(status_after.get("recent_cycles") or []), 1)

        asyncio.run(_run())

    def test_evaluate_matured_outcomes_once_symbol_scope(self) -> None:
        async def _run() -> None:
            decision_ts = 1_700_300_000_000
            symbols = ["BTCUSDT", "ETHUSDT"]
            for idx, sym in enumerate(symbols):
                feature_id = insert_feature_snapshot(
                    CoinFeatureSnapshotRow(
                        ts_ms=decision_ts + idx,
                        pair_key=f"{sym}|binance|kucoin",
                        canonical_symbol=sym,
                        context_mode="candidate",
                        feature_set_version="v1",
                        direction="long_a_short_b",
                        features={
                            "common": {
                                "left_exchange": "binance",
                                "right_exchange": "kucoin",
                                "decision_phase": "mid_interval",
                                "funding": {
                                    "left_interval_hours": 1.0,
                                    "right_interval_hours": 1.0,
                                },
                            },
                            "directional": {
                                "open_spread_pct": -0.3,
                                "funding_to_next_pct": 0.001,
                            },
                        },
                        data_quality={"coverage_pct": 90.0},
                    )
                )
                insert_decision(
                    CoinDecisionRow(
                        decision_id=f"scope-decision-{sym.lower()}",
                        ts_ms=decision_ts + idx,
                        mode="manual_candidate",
                        canonical_symbol=sym,
                        pair_key=f"{sym}|binance|kucoin",
                        direction="long_a_short_b",
                        action="ENTRY_SMALL",
                        decision_phase="mid_interval",
                        confidence_score=55.0,
                        reason_codes=["spread_reversion_favorable"],
                        reason_text=["scope test"],
                        scores={"entry_score": 55.0},
                        features_ref=str(feature_id),
                    )
                )

            cycle = await self.service.evaluate_matured_coin_outcomes_once(symbol="BTCUSDT")
            self.assertEqual(cycle["scope_symbol"], "BTCUSDT")
            self.assertEqual(cycle["symbols_total"], 1)
            self.assertEqual(cycle["symbols_processed"], 1)
            self.assertGreaterEqual(cycle["evaluated"], 1)

            btc_outcomes = get_outcomes(canonical_symbol="BTCUSDT", limit=100)
            eth_outcomes = get_outcomes(canonical_symbol="ETHUSDT", limit=100)
            self.assertGreaterEqual(len(btc_outcomes), 1)
            self.assertEqual(len(eth_outcomes), 0)

        asyncio.run(_run())

    def test_outcomes_auto_scheduler_toggle(self) -> None:
        async def _run() -> None:
            paused = await self.service.set_coin_outcomes_scheduler_enabled(False)
            self.assertFalse(paused["scheduler_enabled"])
            self.assertIn("health", paused)
            self.assertIn("scheduler_paused", paused["health"]["reasons"])

            resumed = await self.service.set_coin_outcomes_scheduler_enabled(True)
            self.assertTrue(resumed["scheduler_enabled"])
            self.assertIn("health", resumed)
            self.assertNotIn("scheduler_paused", resumed["health"]["reasons"])

        asyncio.run(_run())

    def test_coin_retention_service_status_and_run(self) -> None:
        async def _run() -> None:
            status_before = await self.service.get_coin_analysis_maintenance_status()
            self.assertIn("retention", status_before)
            self.assertIn("table_counts", status_before)

            report = await self.service.run_coin_analysis_retention_once(
                max_age_days=7,
                closed_paper_days=30,
                reason="unit_test",
            )
            self.assertEqual(report["reason"], "unit_test")
            self.assertEqual(report["max_age_days"], 7)
            self.assertEqual(report["closed_paper_days"], 30)
            self.assertIn("deleted", report)
            self.assertIn("total_deleted", report["deleted"])

            status_after = await self.service.get_coin_analysis_maintenance_status()
            self.assertIn("last_report", status_after["retention"])
            self.assertEqual(
                status_after["retention"]["last_report"].get("reason"),
                "unit_test",
            )

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
