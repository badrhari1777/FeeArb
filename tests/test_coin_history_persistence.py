from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from orchestrator.models import MarketSnapshot

from analysis_storage.coin_db import (
    CoinFocusSnapshotRow,
    CoinFundingHistoryRow,
    CoinOpenInterestHistoryRow,
    CoinDecisionRow,
    CoinPaperPositionRow,
    get_decisions,
    get_feature_snapshots,
    get_funding_history,
    get_open_interest_history,
    insert_focus_snapshot,
    insert_decision,
    insert_outcome,
    set_test_db_path,
    upsert_funding_history_rows,
    upsert_open_interest_history_rows,
    upsert_paper_position,
)
from webapp.services import DataService


class _AnalyzeFakeAdapter:
    def map_symbol(self, symbol: str) -> str | None:
        return "XBTUSDTM" if symbol.upper() == "BTCUSDT" else None

    async def fetch_market_snapshots_async(self, symbols):  # noqa: ANN001, ANN201
        canonical = str((symbols or ["BTCUSDT"])[0]).upper()
        return [
            MarketSnapshot(
                exchange="kucoin",
                symbol=canonical,
                exchange_symbol="XBTUSDTM",
                funding_rate=0.00021,
                next_funding_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                funding_interval_hours=1.0,
                mark_price=101.2,
                bid=101.0,
                ask=101.4,
                raw={"contract": {"indexPrice": "100.9"}},
            )
        ]

    def funding_history(self, symbol: str, limit: int = 200):  # noqa: ANN001, ANN201
        now_ms = int(time.time() * 1000)
        return [
            {
                "ts_ms": now_ms - 3_600_000,
                "rate": 0.0002,
                "interval_hours": 1.0,
                "mark_price": 100.5,
            },
            {
                "ts_ms": now_ms - 7_200_000,
                "rate": 0.00019,
                "interval_hours": 1.0,
                "mark_price": 100.2,
            },
        ][:limit]


class _KucoinSnapshotAdapter:
    def fetch_market_snapshots(self, symbols):  # noqa: ANN001, ANN201
        canonical = str((symbols or ["BTCUSDT"])[0]).upper()
        return [
            MarketSnapshot(
                exchange="kucoin",
                symbol=canonical,
                exchange_symbol="XBTUSDTM",
                funding_rate=0.0,
                next_funding_time=None,
                mark_price=100.0,
                bid=99.9,
                ask=100.1,
                raw={
                    "contract": {
                        "ts": int(time.time() * 1000),
                        "openInterest": "23456",
                        "openInterestValue": "2500000",
                    }
                },
            )
        ]


class CoinHistoryPersistenceServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        set_test_db_path(Path(self._tmpdir.name) / "coin_history_test.db")
        self.service = DataService()

    def tearDown(self) -> None:
        set_test_db_path(None)

    def test_analyze_exchange_persists_funding_and_oi_history(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            candles = [
                {"ts_ms": now_ms - (i * 60_000), "close": 100.0 + i * 0.01}
                for i in range(60)
            ]
            oi_payload = {
                "status": "ok",
                "history": [
                    {
                        "ts_ms": now_ms - 3_600_000,
                        "open_interest_contracts": 12000.0,
                        "open_interest_notional": 1_200_000.0,
                    },
                    {
                        "ts_ms": now_ms - 7_200_000,
                        "open_interest_contracts": 11800.0,
                        "open_interest_notional": 1_150_000.0,
                    },
                ],
                "current": {
                    "ts_ms": now_ms,
                    "open_interest_contracts": 12200.0,
                    "open_interest_notional": 1_250_000.0,
                },
                "source": "unit_test_oi",
            }
            with patch("webapp.services.get_adapter_cached", return_value=_AnalyzeFakeAdapter()), patch.object(
                self.service,
                "_fetch_candles_for_exchange",
                return_value=candles,
            ), patch.object(
                self.service,
                "_fetch_open_interest_for_exchange",
                return_value=oi_payload,
            ):
                payload = await self.service._analyze_symbol_on_exchange(  # pylint: disable=protected-access
                    "kucoin",
                    "BTCUSDT",
                    60,
                    48,
                )

            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["data_quality"]["funding_rows_upserted"], 2)
            self.assertEqual(payload["data_quality"]["oi_rows_upserted"], 3)

            funding_rows = get_funding_history("BTCUSDT", exchange="kucoin", limit=10)
            self.assertEqual(len(funding_rows), 2)
            self.assertEqual(funding_rows[0]["interval_hours"], 1.0)

            oi_rows = get_open_interest_history("BTCUSDT", exchange="kucoin", limit=10)
            self.assertEqual(len(oi_rows), 3)
            self.assertEqual(oi_rows[0]["source_type"], "unit_test_oi")

        asyncio.run(_run())

    def test_kucoin_oi_fallback_uses_contract_snapshot(self) -> None:
        with patch("webapp.services._ccxt_client", return_value=None), patch(
            "webapp.services.get_adapter_cached",
            return_value=_KucoinSnapshotAdapter(),
        ):
            payload = self.service._fetch_open_interest_for_exchange(  # pylint: disable=protected-access
                "kucoin",
                "BTCUSDT",
                180,
            )
        self.assertEqual(payload["status"], "partial")
        self.assertEqual(payload["source"], "kucoin_contract_snapshot")
        self.assertEqual(len(payload.get("history") or []), 1)
        self.assertEqual((payload.get("current") or {}).get("open_interest_contracts"), 23456.0)

    def test_pair_analysis_persists_feature_snapshots(self) -> None:
        now_ms = int(time.time() * 1000)
        left = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "snapshot": {
                "bid": 100.0,
                "ask": 100.2,
                "mark_price": 100.1,
                "index_price": 99.9,
                "next_funding_time": datetime.fromtimestamp((now_ms + 15 * 60_000) / 1000, tz=timezone.utc).isoformat(),
            },
            "funding_interval_hours_resolved": 1.0,
            "latest_funding_rate": 0.0001,
            "candles_1m": [{"ts_ms": now_ms - i * 60_000, "close": 100.0 + i * 0.01} for i in range(60)],
            "open_interest": {
                "history": [
                    {"ts_ms": now_ms, "open_interest_notional": 1_000_000.0},
                    {"ts_ms": now_ms - 6 * 3600_000, "open_interest_notional": 980_000.0},
                ]
            },
        }
        right = {
            "exchange": "kucoin",
            "symbol": "BTCUSDT",
            "snapshot": {
                "bid": 101.0,
                "ask": 101.2,
                "mark_price": 101.1,
                "index_price": 100.8,
                "next_funding_time": datetime.fromtimestamp((now_ms + 14 * 60_000) / 1000, tz=timezone.utc).isoformat(),
            },
            "funding_interval_hours_resolved": 1.0,
            "latest_funding_rate": 0.0002,
            "candles_1m": [{"ts_ms": now_ms - i * 60_000, "close": 101.0 + i * 0.01} for i in range(60)],
            "open_interest": {
                "history": [
                    {"ts_ms": now_ms, "open_interest_notional": 1_050_000.0},
                    {"ts_ms": now_ms - 6 * 3600_000, "open_interest_notional": 1_010_000.0},
                ]
            },
        }

        pair = self.service._analyze_pair(left, right, 60)  # pylint: disable=protected-access
        self.assertIn(pair["selected_direction"], {"long_a_short_b", "long_b_short_a"})
        self.assertEqual(len(pair["feature_snapshot_ids"]), 2)

        rows = get_feature_snapshots(pair_key=pair["pair_key"], canonical_symbol="BTCUSDT", limit=10)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["feature_set_version"], "v1")

    def test_analyze_symbol_persists_candidate_decision(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)

            async def _fake_exchange_row(exchange: str, *_args, **_kwargs):  # noqa: ANN001, ANN202
                if exchange == "binance":
                    price = 100.0
                    funding = 0.0001
                else:
                    price = 101.0
                    funding = 0.0002
                return {
                    "exchange": exchange,
                    "symbol": "BTCUSDT",
                    "status": "ok",
                    "snapshot": {
                        "bid": price,
                        "ask": price + 0.2,
                        "mark_price": price + 0.1,
                        "index_price": price - 0.1,
                        "next_funding_time": datetime.fromtimestamp(
                            (now_ms + 15 * 60_000) / 1000,
                            tz=timezone.utc,
                        ).isoformat(),
                    },
                    "funding_interval_hours_resolved": 1.0,
                    "latest_funding_rate": funding,
                    "candles_1m": [
                        {"ts_ms": now_ms - i * 60_000, "close": price + i * 0.01}
                        for i in range(60)
                    ],
                    "open_interest": {
                        "status": "ok",
                        "history": [
                            {"ts_ms": now_ms, "open_interest_notional": 1_000_000.0},
                            {
                                "ts_ms": now_ms - 6 * 3600_000,
                                "open_interest_notional": 950_000.0,
                            },
                        ],
                    },
                }

            with patch.object(
                self.service,
                "_coin_analysis_selected_exchanges",
                return_value=["binance", "kucoin"],
            ), patch.object(
                self.service,
                "_analyze_symbol_on_exchange",
                side_effect=_fake_exchange_row,
            ):
                payload = await self.service.analyze_symbol(
                    "BTCUSDT",
                    window_minutes=60,
                    funding_points=48,
                )

            self.assertIn(payload["bot_logic"]["decision"], {"enter_candidate", "watch", "reject"})
            journal = payload.get("decision_journal") or {}
            self.assertTrue(journal.get("decision_id"))
            decisions = get_decisions(canonical_symbol="BTCUSDT", mode="manual_candidate", limit=10)
            self.assertGreaterEqual(len(decisions), 1)
            self.assertEqual(decisions[0]["decision_id"], journal["decision_id"])
            self.assertIn(decisions[0]["action"], {"NO_TRADE", "ENTRY_SMALL", "ENTRY_STRONG"})

        asyncio.run(_run())

    def test_analyze_symbol_builds_visual_analysis_payload(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            aligned_end_ms = (now_ms // 3_600_000) * 3_600_000

            async def _fake_exchange_row(exchange: str, *_args, **_kwargs):  # noqa: ANN001, ANN202
                if exchange == "binance":
                    price = 100.0
                    history = [
                        {"ts_ms": aligned_end_ms - 4 * 3_600_000, "rate": 0.0008, "interval_hours": 4.0},
                        {"ts_ms": aligned_end_ms, "rate": 0.0008, "interval_hours": 4.0},
                    ]
                    interval_hours = 4.0
                else:
                    price = 101.0
                    history = [
                        {"ts_ms": aligned_end_ms - 3 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
                        {"ts_ms": aligned_end_ms - 2 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
                        {"ts_ms": aligned_end_ms - 1 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
                        {"ts_ms": aligned_end_ms, "rate": 0.0001, "interval_hours": 1.0},
                    ]
                    interval_hours = 1.0
                return {
                    "exchange": exchange,
                    "symbol": "BTCUSDT",
                    "status": "ok",
                    "snapshot": {
                        "bid": price,
                        "ask": price + 0.2,
                        "mark_price": price + 0.1,
                        "index_price": price - 0.1,
                        "next_funding_time": datetime.fromtimestamp(
                            (aligned_end_ms + 60 * 60_000) / 1000,
                            tz=timezone.utc,
                        ).isoformat(),
                    },
                    "funding_history": history,
                    "funding_interval_hours_resolved": interval_hours,
                    "latest_funding_rate": history[-1]["rate"],
                    "candles_1m": [
                        {"ts_ms": aligned_end_ms - i * 60_000, "close": price + i * 0.005}
                        for i in range(240)
                    ],
                    "open_interest": {
                        "status": "ok",
                        "history": [
                            {"ts_ms": aligned_end_ms, "open_interest_notional": 1_000_000.0},
                            {
                                "ts_ms": aligned_end_ms - 6 * 3600_000,
                                "open_interest_notional": 970_000.0,
                            },
                        ],
                    },
                }

            with patch.object(
                self.service,
                "_coin_analysis_selected_exchanges",
                return_value=["binance", "kucoin"],
            ), patch.object(
                self.service,
                "_analyze_symbol_on_exchange",
                side_effect=_fake_exchange_row,
            ):
                payload = await self.service.analyze_symbol(
                    "BTCUSDT",
                    window_minutes=240,
                    funding_points=48,
                    persist_candidate_decision=False,
                    run_position_logic=False,
                )

            visual = payload.get("visual_analysis") or {}
            self.assertEqual(visual.get("pair_key"), "BTCUSDT|binance|kucoin")
            self.assertTrue(str(visual.get("direction_label") or "").startswith("Long "))
            self.assertGreaterEqual(len(visual.get("windows") or []), 1)
            self.assertGreaterEqual(len((((visual.get("charts") or {}).get("spread") or {}).get("points") or [])), 1)
            self.assertGreaterEqual(len((((visual.get("charts") or {}).get("funding") or {}).get("points") or [])), 1)
            first_window = (visual.get("windows") or [])[0]
            self.assertIn(first_window.get("signal"), {"favorable", "watch", "avoid"})
            self.assertIsNotNone(first_window.get("funding_net_bps"))

        asyncio.run(_run())

    def test_analyze_symbol_generates_position_logic_for_open_paper(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="paper-pos-1",
                    opened_at_ms=now_ms - 60_000,
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=0.25,
                    entry_context={"source": "unit_test"},
                )
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="paper-prev-review-1",
                    ts_ms=now_ms - 30_000,
                    mode="manual_position_review",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="HOLD",
                    decision_phase="pre_boundary_20m",
                    confidence_score=57.0,
                    reason_codes=["spread_reversion_favorable"],
                    reason_text=["prior review"],
                    scores={"continuation_risk_score": 44.0},
                    state_ref="paper-pos-1",
                )
            )
            insert_outcome(
                "paper-prev-review-1",
                "15m",
                {
                    "decision_correctness": "correct",
                    "timing_quality": "good",
                },
            )

            async def _fake_exchange_row(exchange: str, *_args, **_kwargs):  # noqa: ANN001, ANN202
                if exchange == "binance":
                    price = 100.0
                    funding = 0.0001
                else:
                    price = 101.0
                    funding = 0.0002
                return {
                    "exchange": exchange,
                    "symbol": "BTCUSDT",
                    "status": "ok",
                    "snapshot": {
                        "bid": price,
                        "ask": price + 0.2,
                        "mark_price": price + 0.1,
                        "index_price": price - 0.1,
                        "next_funding_time": datetime.fromtimestamp(
                            (now_ms + 16 * 60_000) / 1000,
                            tz=timezone.utc,
                        ).isoformat(),
                    },
                    "funding_interval_hours_resolved": 1.0,
                    "latest_funding_rate": funding,
                    "candles_1m": [
                        {"ts_ms": now_ms - i * 60_000, "close": price + i * 0.01}
                        for i in range(60)
                    ],
                    "open_interest": {
                        "status": "ok",
                        "history": [
                            {"ts_ms": now_ms, "open_interest_notional": 1_000_000.0},
                            {
                                "ts_ms": now_ms - 6 * 3600_000,
                                "open_interest_notional": 970_000.0,
                            },
                        ],
                    },
                }

            with patch.object(
                self.service,
                "_coin_analysis_selected_exchanges",
                return_value=["binance", "kucoin"],
            ), patch.object(
                self.service,
                "_analyze_symbol_on_exchange",
                side_effect=_fake_exchange_row,
            ):
                payload = await self.service.analyze_symbol(
                    "BTCUSDT",
                    window_minutes=60,
                    funding_points=48,
                )

            position_logic = payload.get("position_logic") or {}
            paper_logic = list(position_logic.get("paper") or [])
            self.assertEqual(len(paper_logic), 1)
            self.assertEqual(paper_logic[0]["position_key"], "paper-pos-1")
            self.assertIn(
                paper_logic[0]["action"],
                {"HOLD", "PARTIAL_EXIT", "FULL_EXIT", "ADD_SMALL", "ADD_BLOCKED"},
            )
            self.assertGreater(float(paper_logic[0].get("decision_ts_ms") or 0), 0)
            self.assertGreater(float(paper_logic[0].get("minutes_to_next_funding") or 0), 0)
            self.assertEqual(paper_logic[0].get("latest_correctness"), "correct")
            self.assertEqual(paper_logic[0].get("latest_review_horizon"), "15m")
            self.assertEqual(paper_logic[0].get("latest_timing_quality"), "good")

            decisions = get_decisions(canonical_symbol="BTCUSDT", mode="manual_position_review", limit=10)
            self.assertGreaterEqual(len(decisions), 1)
            self.assertIn("paper-pos-1", [str(row.get("state_ref") or "") for row in decisions])

        asyncio.run(_run())

    def test_analyze_symbol_generates_position_logic_for_real_manual_positions(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)

            async def _fake_exchange_row(exchange: str, *_args, **_kwargs):  # noqa: ANN001, ANN202
                if exchange == "binance":
                    price = 100.0
                    funding = 0.0001
                else:
                    price = 101.0
                    funding = 0.0002
                return {
                    "exchange": exchange,
                    "symbol": "BTCUSDT",
                    "status": "ok",
                    "snapshot": {
                        "bid": price,
                        "ask": price + 0.2,
                        "mark_price": price + 0.1,
                        "index_price": price - 0.1,
                        "next_funding_time": datetime.fromtimestamp(
                            (now_ms + 16 * 60_000) / 1000,
                            tz=timezone.utc,
                        ).isoformat(),
                    },
                    "funding_interval_hours_resolved": 1.0,
                    "latest_funding_rate": funding,
                    "candles_1m": [
                        {"ts_ms": now_ms - i * 60_000, "close": price + i * 0.01}
                        for i in range(60)
                    ],
                    "open_interest": {
                        "status": "ok",
                        "history": [
                            {"ts_ms": now_ms, "open_interest_notional": 1_000_000.0},
                            {
                                "ts_ms": now_ms - 6 * 3600_000,
                                "open_interest_notional": 970_000.0,
                            },
                        ],
                    },
                }

            account_snapshot = {
                "positions": [
                    {
                        "exchange": "binance",
                        "symbol": "BTCUSDT",
                        "symbol_normalized": "BTCUSDT",
                        "side": "long",
                        "coin_qty": 0.4,
                    },
                    {
                        "exchange": "kucoin",
                        "symbol": "XBTUSDTM",
                        "symbol_normalized": "BTCUSDT",
                        "side": "short",
                        "coin_qty": 0.35,
                    },
                ],
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

            with patch.object(
                self.service,
                "_coin_analysis_selected_exchanges",
                return_value=["binance", "kucoin"],
            ), patch.object(
                self.service,
                "_analyze_symbol_on_exchange",
                side_effect=_fake_exchange_row,
            ), patch.object(
                self.service._accounts,  # pylint: disable=protected-access
                "snapshot",
                return_value=account_snapshot,
            ):
                payload = await self.service.analyze_symbol(
                    "BTCUSDT",
                    window_minutes=60,
                    funding_points=48,
                )

            position_logic = payload.get("position_logic") or {}
            real_logic = list(position_logic.get("real_manual") or [])
            self.assertEqual(len(real_logic), 1)
            self.assertEqual(real_logic[0]["position_source"], "real_manual")
            self.assertEqual(real_logic[0]["long_exchange"], "binance")
            self.assertEqual(real_logic[0]["short_exchange"], "kucoin")
            self.assertGreater(real_logic[0]["matched_qty"], 0.0)
            self.assertGreater(float(real_logic[0].get("decision_ts_ms") or 0), 0)
            self.assertGreater(float(real_logic[0].get("minutes_to_next_funding") or 0), 0)
            self.assertIn(
                real_logic[0]["action"],
                {"HOLD", "PARTIAL_EXIT", "FULL_EXIT", "ADD_SMALL", "ADD_BLOCKED"},
            )

            summary = dict(position_logic.get("summary") or {})
            self.assertEqual(summary.get("real_positions"), 1)
            self.assertEqual(summary.get("real_decisions_saved"), 1)
            self.assertEqual(summary.get("real_legs_detected"), 2)
            self.assertEqual(summary.get("real_unpaired_legs"), 1)

            decisions = get_decisions(canonical_symbol="BTCUSDT", mode="manual_position_review", limit=20)
            real_decisions = [row for row in decisions if str(row.get("state_ref") or "").startswith("real-")]
            self.assertGreaterEqual(len(real_decisions), 1)

        asyncio.run(_run())

    def test_symbol_context_helpers_return_focus_bootstrap_and_journal(self) -> None:
        async def _run() -> None:
            now_ms = int(time.time() * 1000)
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=now_ms - 60_000,
                    canonical_symbol="BTCUSDT",
                    exchange="binance",
                    exchange_symbol="BTCUSDT",
                    bid=100.0,
                    ask=100.2,
                    mid=100.1,
                    mark_price=100.1,
                    focus_reason="held_position",
                    source_type="unit_test",
                )
            )
            insert_focus_snapshot(
                CoinFocusSnapshotRow(
                    ts_ms=now_ms - 30_000,
                    canonical_symbol="BTCUSDT",
                    exchange="kucoin",
                    exchange_symbol="XBTUSDTM",
                    bid=100.3,
                    ask=100.5,
                    mid=100.4,
                    mark_price=100.4,
                    focus_reason="symbol_session",
                    source_type="unit_test",
                )
            )
            upsert_funding_history_rows(
                [
                    CoinFundingHistoryRow(
                        canonical_symbol="BTCUSDT",
                        exchange="binance",
                        ts_ms=now_ms - 3_600_000,
                        funding_rate=0.0001,
                        predicted_funding_rate=0.00012,
                        interval_hours=1.0,
                        mark_price=100.0,
                        source_type="unit_test",
                    )
                ]
            )
            upsert_open_interest_history_rows(
                [
                    CoinOpenInterestHistoryRow(
                        canonical_symbol="BTCUSDT",
                        exchange="binance",
                        ts_ms=now_ms - 3_600_000,
                        oi_contracts=12000.0,
                        oi_notional=1_200_000.0,
                        interval_label="1h",
                        source_type="unit_test",
                    )
                ]
            )
            insert_decision(
                CoinDecisionRow(
                    decision_id="ctx-decision-1",
                    ts_ms=now_ms - 20_000,
                    mode="manual_candidate",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    action="ENTRY_SMALL",
                    decision_phase="pre_boundary_20m",
                    confidence_score=60.0,
                    reason_codes=["spread_reversion_favorable"],
                    reason_text=["context helper test"],
                    scores={"entry_score": 60.0},
                )
            )
            insert_outcome(
                "ctx-decision-1",
                "15m",
                {"decision_correctness": "correct", "timing_quality": "good"},
                evaluated_at_ms=now_ms - 5_000,
            )
            upsert_paper_position(
                CoinPaperPositionRow(
                    position_key="ctx-paper-1",
                    opened_at_ms=now_ms - 10_000,
                    closed_at_ms=None,
                    status="open",
                    canonical_symbol="BTCUSDT",
                    pair_key="BTCUSDT|binance|kucoin",
                    direction="long_a_short_b",
                    qty=0.15,
                    entry_context={"source": "unit_test"},
                )
            )
            await self.service.start_coin_symbol_session(
                "BTCUSDT",
                ttl_sec=300,
                now_ms=now_ms,
            )

            focus = await self.service.load_focus_history(
                "BTCUSDT",
                limit=100,
                since_ts_ms=now_ms - 45_000,
            )
            self.assertEqual(focus["points"], 1)
            self.assertEqual((focus["rows"][0] or {}).get("exchange"), "kucoin")

            bootstrap = await self.service.load_bootstrap_history("BTCUSDT", funding_limit=100, oi_limit=100)
            self.assertEqual((bootstrap.get("counts") or {}).get("funding_points"), 1)
            self.assertEqual((bootstrap.get("counts") or {}).get("open_interest_points"), 1)

            context = await self.service.load_symbol_context(
                "BTCUSDT",
                focus_limit=100,
                funding_limit=100,
                oi_limit=100,
                decision_limit=100,
                outcome_limit=100,
                real_obs_limit=100,
            )
            self.assertEqual(context.get("symbol"), "BTCUSDT")
            self.assertIsNotNone(context.get("active_session"))
            self.assertEqual(((context.get("decision_journal") or {}).get("count")), 1)
            self.assertEqual(((context.get("decision_outcomes") or {}).get("count")), 1)
            self.assertEqual(((context.get("paper_positions_open") or {}).get("count")), 1)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
