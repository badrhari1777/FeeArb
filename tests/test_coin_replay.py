from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from analysis_storage.coin_db import (
    CoinDecisionRow,
    CoinFeatureSnapshotRow,
    insert_decision,
    insert_feature_snapshot,
    set_test_db_path,
)
from webapp.services import DataService


class CoinReplayServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_replay_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_replay_candidate_signals(self) -> None:
        async def _run() -> None:
            ts_a = 1_700_000_000_000
            ts_b = 1_700_000_060_000
            feature_a = insert_feature_snapshot(
                CoinFeatureSnapshotRow(
                    ts_ms=ts_a,
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
                        "scores": {
                            "entry_score": 76.0,
                            "continuation_risk_score": 35.0,
                        },
                        "reasons": ["spread_reversion_favorable"],
                    },
                    data_quality={"coverage_pct": 95.0},
                )
            )
            _feature_b = insert_feature_snapshot(
                CoinFeatureSnapshotRow(
                    ts_ms=ts_b,
                    pair_key="BTCUSDT|binance|kucoin",
                    canonical_symbol="BTCUSDT",
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
                        "scores": {
                            "entry_score": 42.0,
                            "continuation_risk_score": 60.0,
                        },
                        "reasons": ["spread_not_attractive"],
                    },
                    data_quality={"coverage_pct": 95.0},
                )
            )

            insert_decision(
                CoinDecisionRow(
                    decision_id="replay-decision-1",
                    ts_ms=ts_a + 500,
                    mode="manual_candidate",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="ENTRY_STRONG",
                    decision_phase="pre_boundary_20m",
                    confidence_score=76.0,
                    reason_codes=["spread_reversion_favorable"],
                    reason_text=["spread location is favorable"],
                    scores={"best_pair_score": 76.0},
                    features_ref=str(feature_a),
                )
            )

            payload = await self.service.replay_coin_candidate_signals(
                "BTCUSDT",
                limit=100,
                include_stored_decisions=True,
            )
            self.assertEqual(payload["symbol"], "BTCUSDT")
            self.assertEqual(payload["replay_points"], 2)
            self.assertIn("ENTRY_STRONG", payload["summary"]["actions"])
            self.assertIn("NO_TRADE", payload["summary"]["actions"])
            top = payload["timeline"][0]
            self.assertIn(top["recomputed"]["recommended_action"], {"ENTRY_STRONG", "NO_TRADE", "ENTRY_SMALL"})
            matched = [row for row in payload["timeline"] if row.get("stored")]
            self.assertEqual(len(matched), 1)
            self.assertEqual(matched[0]["stored"]["decision_id"], "replay-decision-1")

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
