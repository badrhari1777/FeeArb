from __future__ import annotations

import unittest

from webapp.services import (
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


if __name__ == "__main__":
    unittest.main()
