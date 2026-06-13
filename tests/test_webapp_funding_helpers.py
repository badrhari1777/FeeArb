from __future__ import annotations

import unittest
from unittest.mock import patch

from project_settings import SettingsManager
from webapp.services import (
    DataService,
    _compact_funding_history_rows,
    _funding_history_ts_ms,
    _funding_net_hourly_series,
    _resolve_funding_interval_hours,
)


class WebappFundingHelpersTestCase(unittest.TestCase):
    def test_funding_history_ts_ms_normalizes_seconds(self) -> None:
        self.assertEqual(_funding_history_ts_ms(1_700_000_000), 1_700_000_000_000)
        self.assertEqual(_funding_history_ts_ms(1_700_000_000_000), 1_700_000_000_000)

    def test_resolve_interval_from_history_deltas(self) -> None:
        history = [
            {"ts_ms": 1_700_028_800_000, "rate": 0.0001},
            {"ts_ms": 1_700_000_000_000, "rate": 0.0002},
        ]
        self.assertAlmostEqual(_resolve_funding_interval_hours(history, None) or 0.0, 8.0, places=6)

    def test_resolve_interval_without_data_returns_none(self) -> None:
        self.assertIsNone(_resolve_funding_interval_hours([], None))

    def test_funding_net_hourly_series_normalizes_4h_vs_1h(self) -> None:
        base_ts = 1_728_000_000_000
        left_history = [
            {"ts_ms": base_ts, "rate": 0.0008, "interval_hours": 4.0},
        ]
        right_history = [
            {"ts_ms": base_ts - 3 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
            {"ts_ms": base_ts - 2 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
            {"ts_ms": base_ts - 1 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
            {"ts_ms": base_ts, "rate": 0.0001, "interval_hours": 1.0},
        ]

        rows = _funding_net_hourly_series(
            left_history,
            right_history,
            left_interval_hours=4.0,
            right_interval_hours=1.0,
            direction="long_a_short_b",
            max_points=24,
        )

        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertAlmostEqual(float(row["left_bps"]), -2.0, places=6)
            self.assertAlmostEqual(float(row["right_bps"]), 1.0, places=6)
            self.assertAlmostEqual(float(row["net_bps"]), -1.0, places=6)

    def test_compact_history_prefers_timestamp_interval_over_stale_declared_interval(self) -> None:
        base_ts = 1_728_000_000_000
        history = [
            {"ts_ms": base_ts - idx * 3_600_000, "rate": -0.001, "interval_hours": 4.0}
            for idx in range(6)
        ]

        rows = _compact_funding_history_rows(history, snapshot_interval=4.0, limit=10)
        resolved = _resolve_funding_interval_hours(rows, 4.0)

        self.assertEqual(len(rows), 6)
        self.assertAlmostEqual(float(resolved or 0.0), 1.0, places=6)
        self.assertTrue(all(float(row["interval_hours"]) == 1.0 for row in rows))


class FundingHistoryAnalysisTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_analyze_funding_history_compares_mixed_intervals_by_window(self) -> None:
        base_ts = 1_728_000_000_000

        class _FakeAdapter:
            def __init__(self, exchange: str) -> None:
                self.exchange = exchange

            def map_symbol(self, symbol: str) -> str | None:
                return symbol.upper()

            async def fetch_market_snapshots_async(self, _symbols):  # noqa: ANN001, ANN201
                return []

            def funding_history(self, _symbol: str, limit: int = 200):  # noqa: ANN001, ANN201
                if self.exchange == "binance":
                    rows = [
                        {"ts_ms": base_ts - 4 * 3_600_000, "rate": 0.0008, "interval_hours": 4.0},
                        {"ts_ms": base_ts, "rate": 0.0008, "interval_hours": 4.0},
                    ]
                else:
                    rows = [
                        {"ts_ms": base_ts - 3 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
                        {"ts_ms": base_ts - 2 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
                        {"ts_ms": base_ts - 1 * 3_600_000, "rate": 0.0001, "interval_hours": 1.0},
                        {"ts_ms": base_ts, "rate": 0.0001, "interval_hours": 1.0},
                    ]
                return list(reversed(rows))[:limit]

        service = DataService(settings_manager=SettingsManager())
        with patch(
            "webapp.services.get_adapter_cached",
            side_effect=lambda exchange: _FakeAdapter(exchange),
        ), patch.object(service, "_persist_coin_funding_history", return_value=0):
            payload = await service.analyze_funding_history(
                "LABUSDT",
                exchanges=["binance", "kucoin"],
                windows_hours=[4],
                funding_points=24,
            )

        best = (payload.get("best_by_window") or {}).get("4h") or {}
        self.assertEqual(best.get("long_exchange"), "kucoin")
        self.assertEqual(best.get("short_exchange"), "binance")
        self.assertAlmostEqual(float(best.get("net_bps") or 0.0), 4.0, places=6)
        self.assertAlmostEqual(float(best.get("coverage_pct") or 0.0), 100.0, places=6)
        self.assertEqual(best.get("status"), "ok")


if __name__ == "__main__":
    unittest.main()
