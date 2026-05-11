from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from analysis_storage.coin_db import (
    SCHEMA_VERSION,
    CoinCandidateShortlistRow,
    CoinDecisionRow,
    CoinFeatureSnapshotRow,
    CoinFocusSnapshotRow,
    CoinFundingHistoryRow,
    CoinInstrumentRow,
    CoinOpenInterestHistoryRow,
    CoinPairRow,
    CoinPaperPositionRow,
    CoinRealPositionObservationRow,
    CoinSymbolSessionRow,
    CoinTradeActivityRow,
    ensure_schema,
    expire_symbol_sessions,
    get_active_symbol_sessions,
    get_candidate_shortlist,
    get_coin_analysis_table_counts,
    get_decisions,
    get_feature_snapshot_by_id,
    get_feature_snapshots,
    get_funding_history,
    get_focus_snapshots,
    get_open_interest_history,
    get_outcomes,
    get_paper_events,
    get_paper_positions,
    get_real_position_observations,
    get_trade_activity,
    get_schema_version,
    insert_candidate_shortlist_rows,
    insert_decision,
    insert_feature_snapshot,
    insert_focus_snapshot,
    insert_outcome,
    insert_paper_event,
    insert_real_position_observation,
    insert_trade_activity,
    set_test_db_path,
    prune_coin_analysis_data,
    upsert_funding_history_rows,
    upsert_instrument,
    upsert_open_interest_history_rows,
    upsert_pair,
    upsert_paper_position,
    upsert_symbol_session,
)


class CoinAnalysisDbTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_analysis_test.db")

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_ensure_schema_is_idempotent_and_sets_version(self) -> None:
        ensure_schema()
        ensure_schema()
        self.assertEqual(get_schema_version(), SCHEMA_VERSION)

    def test_upsert_and_query_core_rows(self) -> None:
        now_ms = int(time.time() * 1000)
        upsert_instrument(
            CoinInstrumentRow(
                canonical_symbol="BTCUSDT",
                exchange="binance",
                exchange_symbol="BTCUSDT",
                tick_size=0.1,
                qty_step=0.001,
                min_qty=0.001,
                min_notional=5.0,
                funding_interval_hours=8.0,
                source_ts_ms=now_ms,
            )
        )
        upsert_pair(
            CoinPairRow(
                pair_key="BTCUSDT|binance|kucoin",
                canonical_symbol="BTCUSDT",
                exchange_a="binance",
                exchange_b="kucoin",
                exchange_a_symbol="BTCUSDT",
                exchange_b_symbol="XBTUSDTM",
            )
        )

        upsert_symbol_session(
            CoinSymbolSessionRow(
                canonical_symbol="BTCUSDT",
                started_at_ms=now_ms,
                expires_at_ms=now_ms + 60_000,
            )
        )
        active = get_active_symbol_sessions(now_ms=now_ms)
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].canonical_symbol, "BTCUSDT")

        expired = expire_symbol_sessions(now_ms=now_ms + 120_000)
        self.assertEqual(expired, 1)
        self.assertEqual(get_active_symbol_sessions(now_ms=now_ms + 120_000), [])

    def test_focus_feature_decision_and_paper_helpers(self) -> None:
        now_ms = int(time.time() * 1000)
        insert_focus_snapshot(
            CoinFocusSnapshotRow(
                ts_ms=now_ms,
                canonical_symbol="ETHUSDT",
                exchange="binance",
                exchange_symbol="ETHUSDT",
                bid=3000.0,
                ask=3001.0,
                mid=3000.5,
                mark_price=3000.2,
                index_price=3000.1,
                source_type="rest_fallback",
                focus_reason="manual_page",
            )
        )
        rows = get_focus_snapshots("ETHUSDT", exchange="binance", limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["exchange"], "binance")

        feature_id = insert_feature_snapshot(
            CoinFeatureSnapshotRow(
                ts_ms=now_ms,
                pair_key="ETHUSDT|binance|kucoin",
                canonical_symbol="ETHUSDT",
                context_mode="candidate",
                feature_set_version="v1",
                direction="long_a_short_b",
                features={"spread_z_1h": -1.2},
                data_quality={"local_points": 42},
            )
        )
        self.assertGreater(feature_id, 0)
        feature_rows = get_feature_snapshots(
            pair_key="ETHUSDT|binance|kucoin",
            canonical_symbol="ETHUSDT",
            direction="long_a_short_b",
            limit=5,
        )
        self.assertEqual(len(feature_rows), 1)
        self.assertEqual(feature_rows[0]["id"], feature_id)
        by_id = get_feature_snapshot_by_id(feature_id)
        self.assertIsNotNone(by_id)
        self.assertEqual((by_id or {}).get("pair_key"), "ETHUSDT|binance|kucoin")

        insert_decision(
            CoinDecisionRow(
                decision_id="decision-1",
                ts_ms=now_ms,
                mode="manual_candidate",
                canonical_symbol="ETHUSDT",
                pair_key="ETHUSDT|binance|kucoin",
                direction="long_a_short_b",
                action="ENTRY_SMALL",
                decision_phase="pre_boundary_20m",
                confidence_score=62.5,
                reason_codes=["spread_reversion_favorable"],
                reason_text=["spread below 24h median with stable funding"],
                scores={"reversion_score": 68.0, "continuation_risk_score": 35.0},
                features_ref=str(feature_id),
            )
        )
        insert_outcome(
            "decision-1",
            "15m",
            {
                "decision_correctness": "mixed",
                "funding_component_correct": True,
                "spread_component_correct": False,
            },
            evaluated_at_ms=now_ms + 900_000,
        )

        upsert_paper_position(
            CoinPaperPositionRow(
                position_key="paper-1",
                opened_at_ms=now_ms,
                closed_at_ms=None,
                status="open",
                canonical_symbol="ETHUSDT",
                pair_key="ETHUSDT|binance|kucoin",
                direction="long_a_short_b",
                qty=0.5,
                entry_context={"source_decision_id": "decision-1"},
            )
        )
        insert_paper_event(
            event_id="event-1",
            position_key="paper-1",
            ts_ms=now_ms + 1000,
            event_type="entry",
            payload={"price": 3001.0},
        )
        positions = get_paper_positions(status="open")
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["position_key"], "paper-1")
        events = get_paper_events("paper-1", limit=10)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "entry")

        insert_real_position_observation(
            CoinRealPositionObservationRow(
                state_ref="real-ethusdt-binance-long-kucoin-short",
                ts_ms=now_ms + 2000,
                canonical_symbol="ETHUSDT",
                pair_key="ETHUSDT|binance|kucoin",
                direction="long_a_short_b",
                long_exchange="binance",
                short_exchange="kucoin",
                qty=0.4,
                status="open",
                payload={"source": "unit_test"},
            )
        )
        real_obs = get_real_position_observations(canonical_symbol="ETHUSDT", limit=10)
        self.assertEqual(len(real_obs), 1)
        self.assertEqual(real_obs[0]["state_ref"], "real-ethusdt-binance-long-kucoin-short")
        self.assertEqual(real_obs[0]["status"], "open")

        decisions = get_decisions(canonical_symbol="ETHUSDT", limit=5)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["decision_id"], "decision-1")
        self.assertEqual(decisions[0]["action"], "ENTRY_SMALL")
        outcomes = get_outcomes(canonical_symbol="ETHUSDT", limit=5)
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]["decision_id"], "decision-1")
        self.assertEqual(outcomes[0]["horizon"], "15m")

    def test_funding_and_open_interest_history_upserts(self) -> None:
        now_ms = int(time.time() * 1000)
        funding_count = upsert_funding_history_rows(
            [
                CoinFundingHistoryRow(
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    ts_ms=now_ms - 3_600_000,
                    funding_rate=0.0001,
                    predicted_funding_rate=0.00012,
                    interval_hours=8.0,
                    mark_price=85_000.0,
                    source_type="unit_test",
                ),
                CoinFundingHistoryRow(
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    ts_ms=now_ms - 7_200_000,
                    funding_rate=0.00009,
                    predicted_funding_rate=0.0001,
                    interval_hours=8.0,
                    mark_price=84_900.0,
                    source_type="unit_test",
                ),
            ]
        )
        self.assertEqual(funding_count, 2)
        funding_rows = get_funding_history("BTCUSDT", exchange="binance", limit=10)
        self.assertEqual(len(funding_rows), 2)
        self.assertEqual(funding_rows[0]["source_type"], "unit_test")

        oi_count = upsert_open_interest_history_rows(
            [
                CoinOpenInterestHistoryRow(
                    canonical_symbol="BTCUSDT",
                    exchange="kucoin",
                    ts_ms=now_ms - 3_600_000,
                    oi_contracts=12345.0,
                    oi_notional=11_000_000.0,
                    interval_label="1h",
                    source_type="unit_test",
                ),
                CoinOpenInterestHistoryRow(
                    canonical_symbol="BTCUSDT",
                    exchange="kucoin",
                    ts_ms=now_ms,
                    oi_contracts=13000.0,
                    oi_notional=11_500_000.0,
                    interval_label="current",
                    source_type="unit_test",
                ),
            ]
        )
        self.assertEqual(oi_count, 2)
        oi_rows = get_open_interest_history("BTCUSDT", exchange="kucoin", limit=10)
        self.assertEqual(len(oi_rows), 2)
        self.assertEqual(oi_rows[0]["interval_label"], "current")

    def test_trade_activity_and_shortlist_helpers(self) -> None:
        now_ms = int(time.time() * 1000)
        insert_trade_activity(
            CoinTradeActivityRow(
                event_id="activity-1",
                ts_ms=now_ms,
                canonical_symbol="BTCUSDT",
                pair_key="BTCUSDT|binance|kucoin",
                direction="long_a_short_b",
                activity_type="paper_enter",
                source="unit_test",
                state_ref="paper-1",
                payload={"qty": 1.25},
            )
        )
        inserted = insert_candidate_shortlist_rows(
            [
                CoinCandidateShortlistRow(
                    ts_ms=now_ms,
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    rank=1,
                    source_name="unit_test",
                    direction_hint="long_a_short_b",
                    candidate_score=74.0,
                    funding_edge_pct=0.12,
                    entry_spread_pct=-0.35,
                    premium_diff_pct=0.03,
                    oi_change_1h_pct=4.2,
                    oi_change_4h_pct=8.8,
                    reason_codes=["spread_z_favorable", "funding_edge_positive"],
                    payload={"note": "tracked"},
                )
            ]
        )
        self.assertEqual(inserted, 1)

        activity_rows = get_trade_activity(
            canonical_symbol="BTCUSDT",
            since_ts_ms=now_ms - 1_000,
            limit=10,
        )
        self.assertEqual(len(activity_rows), 1)
        self.assertEqual(activity_rows[0]["activity_type"], "paper_enter")
        self.assertEqual(activity_rows[0]["payload"]["qty"], 1.25)

        shortlist_rows = get_candidate_shortlist(
            canonical_symbol="BTCUSDT",
            since_ts_ms=now_ms - 1_000,
            limit=10,
        )
        self.assertEqual(len(shortlist_rows), 1)
        self.assertEqual(shortlist_rows[0]["rank"], 1)
        self.assertEqual(shortlist_rows[0]["candidate_score"], 74.0)
        self.assertIn("funding_edge_positive", shortlist_rows[0]["reason_codes"])

    def test_prune_coin_analysis_data_removes_old_rows(self) -> None:
        now_ms = int(time.time() * 1000)
        old_ts = now_ms - (10 * 24 * 3600 * 1000)
        recent_ts = now_ms - (12 * 3600 * 1000)

        insert_focus_snapshot(
            CoinFocusSnapshotRow(
                ts_ms=old_ts,
                canonical_symbol="BTCUSDT",
                exchange="binance",
                exchange_symbol="BTCUSDT",
                bid=100.0,
                ask=100.1,
            )
        )
        insert_focus_snapshot(
            CoinFocusSnapshotRow(
                ts_ms=recent_ts,
                canonical_symbol="BTCUSDT",
                exchange="binance",
                exchange_symbol="BTCUSDT",
                bid=101.0,
                ask=101.1,
            )
        )
        upsert_funding_history_rows(
            [
                CoinFundingHistoryRow(
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    ts_ms=old_ts,
                    funding_rate=0.0001,
                ),
                CoinFundingHistoryRow(
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    ts_ms=recent_ts,
                    funding_rate=0.0002,
                ),
            ]
        )
        insert_decision(
            CoinDecisionRow(
                decision_id="old-dec-1",
                ts_ms=old_ts,
                mode="manual_candidate",
                canonical_symbol="BTCUSDT",
                pair_key="BTCUSDT|binance|kucoin",
                direction="long_a_short_b",
                action="ENTRY_SMALL",
                decision_phase="mid_interval",
                confidence_score=50.0,
                reason_codes=["spread_reversion_favorable"],
                reason_text=["old"],
                scores={},
            )
        )
        insert_outcome(
            "old-dec-1",
            "15m",
            {"decision_correctness": "incorrect"},
            evaluated_at_ms=old_ts + 1_000,
        )
        upsert_paper_position(
            CoinPaperPositionRow(
                position_key="closed-old-paper",
                opened_at_ms=old_ts - 10_000,
                closed_at_ms=old_ts,
                status="closed",
                canonical_symbol="BTCUSDT",
                pair_key="BTCUSDT|binance|kucoin",
                direction="long_a_short_b",
                qty=0.1,
            )
        )
        upsert_paper_position(
            CoinPaperPositionRow(
                position_key="open-recent-paper",
                opened_at_ms=recent_ts - 10_000,
                closed_at_ms=None,
                status="open",
                canonical_symbol="BTCUSDT",
                pair_key="BTCUSDT|binance|kucoin",
                direction="long_a_short_b",
                qty=0.2,
            )
        )

        before = get_coin_analysis_table_counts()
        self.assertGreaterEqual(before["ca_market_snapshots_focus"], 2)
        self.assertGreaterEqual(before["ca_funding_history"], 2)
        self.assertGreaterEqual(before["ca_decisions"], 1)
        self.assertGreaterEqual(before["ca_outcomes"], 1)
        self.assertIn("ca_trade_activity", before)
        self.assertIn("ca_candidate_shortlist_snapshots", before)
        self.assertGreaterEqual(before["ca_paper_positions_closed"], 1)
        self.assertGreaterEqual(before["ca_paper_positions_open"], 1)

        deleted = prune_coin_analysis_data(
            max_age_ms=2 * 24 * 3600 * 1000,
            closed_paper_max_age_ms=2 * 24 * 3600 * 1000,
            now_ms=now_ms,
        )
        self.assertGreaterEqual(deleted["total_deleted"], 1)

        after = get_coin_analysis_table_counts()
        self.assertGreaterEqual(after["ca_market_snapshots_focus"], 1)
        self.assertGreaterEqual(after["ca_funding_history"], 1)
        self.assertEqual(after["ca_paper_positions_open"], 1)
        self.assertEqual(after["ca_paper_positions_closed"], 0)


if __name__ == "__main__":
    unittest.main()
