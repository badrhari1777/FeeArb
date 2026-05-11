from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from analysis_storage.coin_db import (
    CoinCandidateShortlistRow,
    CoinDecisionRow,
    CoinFeatureSnapshotRow,
    CoinFocusSnapshotRow,
    CoinFundingHistoryRow,
    CoinOpenInterestHistoryRow,
    CoinPaperPositionRow,
    CoinTradeActivityRow,
    insert_candidate_shortlist_rows,
    insert_decision,
    insert_feature_snapshot,
    insert_focus_snapshot,
    insert_outcome,
    insert_paper_event,
    insert_trade_activity,
    set_test_db_path,
    upsert_funding_history_rows,
    upsert_open_interest_history_rows,
    upsert_paper_position,
)
from webapp.services import DataService


class CoinExportServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_export_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_export_json_and_csv(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=now_ms,
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    exchange_symbol="BTCUSDT",
                    bid=100.0,
                    ask=100.2,
                    mid=100.1,
                    mark_price=100.15,
                    index_price=100.0,
                    premium_pct=0.15,
                    funding_rate=0.0001,
                    source_type="unit_test",
                    focus_reason="unit_test",
                )
            )
            upsert_funding_history_rows(
                [
                    CoinFundingHistoryRow(
                        canonical_symbol="BTCUSDT",
                        exchange="binance",
                        ts_ms=now_ms - 3_600_000,
                        funding_rate=0.0001,
                        interval_hours=1.0,
                        source_type="unit_test",
                    )
                ]
            )
            upsert_open_interest_history_rows(
                [
                    CoinOpenInterestHistoryRow(
                        canonical_symbol="BTCUSDT",
                        exchange="kucoin",
                        ts_ms=now_ms - 3_600_000,
                        oi_contracts=10000.0,
                        oi_notional=1_000_000.0,
                        interval_label="1h",
                        source_type="unit_test",
                    )
                ]
            )
            feature_id = insert_feature_snapshot(
                CoinFeatureSnapshotRow(
                    ts_ms=now_ms - 1_000,
                    pair_key="BTCUSDT|binance|kucoin",
                    canonical_symbol="BTCUSDT",
                    context_mode="candidate",
                    feature_set_version="v1",
                    direction="long_a_short_b",
                    features={"scores": {"entry_score": 70.0, "continuation_risk_score": 35.0}},
                    data_quality={"coverage_pct": 90.0},
                )
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="decision-export-1",
                    ts_ms=now_ms - 500,
                    mode="manual_candidate",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="ENTRY_SMALL",
                    decision_phase="pre_boundary_20m",
                    confidence_score=65.0,
                    reason_codes=["spread_reversion_favorable"],
                    reason_text=["spread location is favorable"],
                    scores={"best_pair_score": 65.0},
                    features_ref=str(feature_id),
                )
            )
            insert_outcome(
                "decision-export-1",
                "15m",
                {
                    "decision_correctness": "correct",
                    "spread_delta_pct": 0.25,
                    "funding_to_next_pct": 0.01,
                    "decision_phase": "pre_boundary_20m",
                },
                evaluated_at_ms=now_ms,
            )
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="paper-export-1",
                    opened_at_ms=now_ms - 20_000,
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=1.0,
                    entry_context={"source": "unit_test"},
                )
            )
            insert_paper_event(
                event_id="paper-event-export-1",
                position_key="paper-export-1",
                ts_ms=now_ms - 10_000,
                event_type="entry",
                payload={"qty": 1.0},
            )

            export_payload = await self.service.export_coin_analysis_json(
                "BTCUSDT",
                include_live_analysis=False,
            )
            self.assertEqual(export_payload["symbol"], "BTCUSDT")
            self.assertEqual(export_payload["schema_version"], "coin_export_v1")
            self.assertEqual(len(export_payload["raw_market_data"]["focus_snapshots"]), 1)
            self.assertEqual(len(export_payload["raw_market_data"]["funding_history"]), 1)
            self.assertEqual(len(export_payload["raw_market_data"]["open_interest_history"]), 1)
            self.assertEqual(len(export_payload["derived_features"]), 1)
            self.assertGreaterEqual(len(export_payload["decision_journal"]), 1)
            self.assertEqual(len(export_payload["decision_outcomes"]), 1)
            self.assertEqual(export_payload["decision_outcome_summary"]["total"], 1)
            self.assertEqual(export_payload["decision_outcome_summary"]["by_horizon"]["15m"]["correct"], 1)
            self.assertEqual(
                export_payload["decision_outcome_summary"]["operator_scorecard_pre_boundary"]["overall"]["hit_rate_pct"],
                100.0,
            )
            self.assertEqual(
                export_payload["decision_outcome_summary"]["operator_scorecard_pre_boundary"]["traffic_light"]["status"],
                "yellow",
            )
            self.assertEqual(len(export_payload["paper"]["positions"]), 1)
            self.assertEqual(len(export_payload["paper"]["events_by_position"]["paper-export-1"]), 1)

            csv_data = await self.service.export_coin_timeline_csv(
                "BTCUSDT",
                include_live_analysis=False,
            )
            self.assertIn("record_type", csv_data)
            self.assertIn("focus_snapshot", csv_data)
            self.assertIn("feature_snapshot", csv_data)
            self.assertIn("decision", csv_data)
            self.assertIn("decision_outcome", csv_data)
            self.assertIn("paper_event", csv_data)

        asyncio.run(_run())

    def test_weekly_review_export(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            insert_trade_activity(
                CoinTradeActivityRow(
                    event_id="activity-review-1",
                    ts_ms=now_ms - 5_000,
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    activity_type="paper_enter",
                    source="unit_test",
                    state_ref="paper-review-1",
                    payload={"qty": 1.0},
                )
            )
            insert_candidate_shortlist_rows(
                [
                    CoinCandidateShortlistRow(
                        ts_ms=now_ms - 2_000,
                        canonical_symbol="BTCUSDT",
                        pair_key="BTCUSDT|binance|kucoin",
                        rank=1,
                        source_name="unit_test",
                        direction_hint="long_a_short_b",
                        candidate_score=68.0,
                        funding_edge_pct=0.08,
                        entry_spread_pct=-0.22,
                        reason_codes=["tracked_top3"],
                        payload={"score": 68.0},
                    )
                ]
            )
            insert_candidate_shortlist_rows(
                [
                    CoinCandidateShortlistRow(
                        ts_ms=now_ms - 4_000,
                        canonical_symbol="ETHUSDT",
                        pair_key="ETHUSDT|binance|kucoin",
                        rank=1,
                        source_name="unit_test",
                        direction_hint="long_a_short_b",
                        candidate_score=72.0,
                        funding_edge_pct=0.09,
                        entry_spread_pct=-0.31,
                        reason_codes=["tracked_top3"],
                        payload={"score": 72.0},
                    )
                ]
            )
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="paper-review-1",
                    opened_at_ms=now_ms - 20_000,
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=1.0,
                    entry_context={"source": "unit_test"},
                )
            )
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="paper-stale-1",
                    opened_at_ms=now_ms - (30 * 3600 * 1000),
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="SOLUSDT",
                    pair_key="SOLUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=2.0,
                    entry_context={"source": "unit_test"},
                )
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="decision-late-exit-1",
                    ts_ms=now_ms - 20_000,
                    mode="manual_position_review",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="HOLD",
                    decision_phase="pre_boundary_20m",
                    confidence_score=55.0,
                    reason_codes=["hold_review"],
                    reason_text=["holding into boundary"],
                    scores={"wait_score": 1.0},
                )
            )
            insert_outcome(
                "decision-late-exit-1",
                "15m",
                {
                    "decision_correctness": "incorrect",
                    "decision_phase": "pre_boundary_20m",
                    "decision_action": "HOLD",
                    "would_exiting_15m_earlier_help": True,
                    "timing_quality": "poor",
                    "net_pnl_delta_vs_alternative": -0.15,
                    "net_pnl_delta_pct": -0.09,
                },
                evaluated_at_ms=now_ms - 1_000,
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="decision-bad-entry-1",
                    ts_ms=now_ms - 30_000,
                    mode="manual_candidate",
                    canonical_symbol="XRPUSDT",
                    pair_key="XRPUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="ENTRY_SMALL",
                    decision_phase="pre_boundary_20m",
                    confidence_score=61.0,
                    reason_codes=["entry_attempt"],
                    reason_text=["entry looked acceptable"],
                    scores={"entry_score": 61.0},
                )
            )
            insert_outcome(
                "decision-bad-entry-1",
                "15m",
                {
                    "decision_correctness": "incorrect",
                    "decision_phase": "pre_boundary_20m",
                    "decision_action": "ENTRY_SMALL",
                    "timing_quality": "poor",
                    "net_pnl_delta_pct": -0.08,
                    "net_pnl_delta_vs_alternative": -0.03,
                },
                evaluated_at_ms=now_ms - 3_000,
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="decision-good-no-trade-1",
                    ts_ms=now_ms - 40_000,
                    mode="manual_candidate",
                    canonical_symbol="ADAUSDT",
                    pair_key="ADAUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="NO_TRADE",
                    decision_phase="mid_interval",
                    confidence_score=58.0,
                    reason_codes=["skip_entry"],
                    reason_text=["skip looked safer"],
                    scores={"entry_score": 42.0},
                )
            )
            insert_outcome(
                "decision-good-no-trade-1",
                "15m",
                {
                    "decision_correctness": "correct",
                    "decision_phase": "mid_interval",
                    "decision_action": "NO_TRADE",
                    "timing_quality": "good",
                    "net_pnl_delta_pct": -0.06,
                },
                evaluated_at_ms=now_ms - 2_500,
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="decision-good-exit-1",
                    ts_ms=now_ms - 50_000,
                    mode="manual_position_review",
                    canonical_symbol="DOGEUSDT",
                    pair_key="DOGEUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="FULL_EXIT",
                    decision_phase="boundary",
                    confidence_score=63.0,
                    reason_codes=["exit_now"],
                    reason_text=["locking exit"],
                    scores={"exit_score": 63.0},
                )
            )
            insert_outcome(
                "decision-good-exit-1",
                "15m",
                {
                    "decision_correctness": "correct",
                    "decision_phase": "boundary",
                    "decision_action": "FULL_EXIT",
                    "timing_quality": "good",
                    "net_pnl_delta_pct": -0.01,
                    "net_pnl_delta_vs_alternative": -0.07,
                },
                evaluated_at_ms=now_ms - 2_200,
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="decision-bad-hold-1",
                    ts_ms=now_ms - 60_000,
                    mode="manual_position_review",
                    canonical_symbol="LTCUSDT",
                    pair_key="LTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="HOLD",
                    decision_phase="mid_interval",
                    confidence_score=49.0,
                    reason_codes=["hold_review"],
                    reason_text=["wait a bit longer"],
                    scores={"wait_score": 0.5},
                )
            )
            insert_outcome(
                "decision-bad-hold-1",
                "15m",
                {
                    "decision_correctness": "incorrect",
                    "decision_phase": "mid_interval",
                    "decision_action": "HOLD",
                    "timing_quality": "poor",
                    "would_exiting_15m_earlier_help": False,
                    "net_pnl_delta_pct": -0.05,
                    "net_pnl_delta_vs_alternative": -0.01,
                },
                evaluated_at_ms=now_ms - 1_800,
            )
            review = await self.service.get_coin_weekly_review(days=7, top=3)
            self.assertEqual(review["schema_version"], "coin_review_v1")
            self.assertEqual(review["summary"]["trade_activity_total"], 1)
            self.assertEqual(review["summary"]["symbols_shortlisted_count"], 2)
            self.assertIn("BTCUSDT", review["recent_traded_symbols"])
            self.assertIn("BTCUSDT", review["top_candidate_symbols"])
            self.assertIn("review_tag_counts", review["summary"])
            self.assertIn("review_tag_severity_counts", review["summary"])
            self.assertIn("top_review_items", review["summary"])
            self.assertIn("entry_review", review)
            self.assertIn("exit_review", review)
            self.assertIn("phase_scorecards", review["summary"])
            self.assertGreaterEqual(review["summary"]["review_tag_counts"].get("missed_entry", 0), 1)
            self.assertGreaterEqual(review["summary"]["review_tag_counts"].get("bad_entry", 0), 1)
            self.assertGreaterEqual(review["summary"]["review_tag_counts"].get("good_no_trade", 0), 1)
            self.assertGreaterEqual(review["summary"]["review_tag_counts"].get("late_exit", 0), 1)
            self.assertGreaterEqual(review["summary"]["review_tag_counts"].get("stale_position", 0), 1)
            self.assertGreaterEqual(review["summary"]["review_tag_counts"].get("good_exit", 0), 1)
            self.assertGreaterEqual(review["summary"]["review_tag_counts"].get("bad_hold", 0), 1)
            self.assertTrue(review["review_tags"])
            self.assertEqual(review["review_tags"][0]["tag"], "late_exit")
            self.assertGreaterEqual(float(review["review_tags"][0]["impact_score"]), 80.0)
            self.assertTrue(review["summary"]["top_review_items"])
            self.assertEqual(review["summary"]["top_review_items"][0]["tag"], "late_exit")
            self.assertEqual(review["entry_review"]["summary"]["total"], 3)
            self.assertEqual(review["entry_review"]["summary"]["tag_counts"]["missed_entry"], 1)
            self.assertEqual(review["entry_review"]["summary"]["tag_counts"]["bad_entry"], 1)
            self.assertEqual(review["entry_review"]["summary"]["tag_counts"]["good_no_trade"], 1)
            self.assertIn("action_scorecards", review["entry_review"]["summary"])
            self.assertEqual(review["entry_review"]["summary"]["action_scorecards"]["ENTRY_SMALL"]["total"], 1)
            self.assertEqual(review["entry_review"]["summary"]["action_scorecards"]["NO_TRADE"]["total"], 1)
            self.assertEqual(review["exit_review"]["summary"]["tag_counts"]["late_exit"], 1)
            self.assertEqual(review["exit_review"]["summary"]["tag_counts"]["stale_position"], 1)
            self.assertEqual(review["exit_review"]["summary"]["tag_counts"]["good_exit"], 1)
            self.assertEqual(review["exit_review"]["summary"]["tag_counts"]["bad_hold"], 1)
            self.assertIn("action_scorecards", review["exit_review"]["summary"])
            self.assertEqual(review["exit_review"]["summary"]["action_scorecards"]["HOLD"]["total"], 2)
            self.assertEqual(review["exit_review"]["summary"]["action_scorecards"]["FULL_EXIT"]["total"], 1)
            self.assertEqual(review["summary"]["phase_scorecards"]["pre_boundary"]["total"], 2)
            self.assertEqual(review["summary"]["phase_scorecards"]["mid_interval"]["total"], 2)
            self.assertEqual(review["summary"]["phase_scorecards"]["boundary"]["total"], 1)
            self.assertEqual(
                {row["tag"] for row in review["entry_review"]["tags"]},
                {"missed_entry", "bad_entry", "good_no_trade"},
            )
            self.assertEqual(
                {row["tag"] for row in review["exit_review"]["tags"]},
                {"late_exit", "stale_position", "good_exit", "bad_hold"},
            )

            symbol_review = await self.service.export_coin_review_json(
                symbol="BTCUSDT",
                days=7,
                top=3,
                include_live_analysis=False,
            )
            self.assertEqual(symbol_review["schema_version"], "coin_review_v1")
            self.assertEqual(symbol_review["review"]["scope"]["symbol"], "BTCUSDT")
            self.assertIsNotNone(symbol_review["symbol_export"])
            self.assertEqual(symbol_review["symbol_export"]["symbol"], "BTCUSDT")

            review_csv = await self.service.export_coin_review_csv(
                symbol="BTCUSDT",
                days=7,
                top=3,
            )
            self.assertIn("record_type", review_csv)
            self.assertIn("trade_activity", review_csv)
            self.assertIn("shortlist_candidate", review_csv)
            self.assertIn("review_tag", review_csv)
            self.assertIn("BTCUSDT", review_csv)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
