from __future__ import annotations

import unittest
from datetime import datetime, timezone

from utils.funding import (
    infer_funding_interval_hours,
    is_stale_next_funding_iso,
    normalize_interval_hours,
    parse_timestamp_ms,
    project_next_funding_time_iso,
)


class FundingUtilsTestCase(unittest.TestCase):
    def test_parse_timestamp_ms_accepts_seconds_and_milliseconds(self) -> None:
        self.assertEqual(parse_timestamp_ms(1_700_000_000), 1_700_000_000_000)
        self.assertEqual(parse_timestamp_ms(1_700_000_000_000), 1_700_000_000_000)

    def test_normalize_interval_hours_from_seconds(self) -> None:
        self.assertAlmostEqual(normalize_interval_hours(28_800) or 0.0, 8.0)
        self.assertAlmostEqual(normalize_interval_hours(480) or 0.0, 8.0)

    def test_infer_interval_from_history_timestamps(self) -> None:
        base = 1_700_000_000_000
        history = [
            {"ts_ms": base, "rate": 0.0001},
            {"ts_ms": base - 8 * 3600 * 1000, "rate": 0.0002},
            {"ts_ms": base - 16 * 3600 * 1000, "rate": 0.0003},
        ]
        self.assertAlmostEqual(infer_funding_interval_hours(history) or 0.0, 8.0)

    def test_project_next_funding_rolls_forward_to_future_slot(self) -> None:
        history = [
            {"ts_ms": parse_timestamp_ms("2026-02-17T00:00:00+00:00"), "rate": 0.0001},
            {"ts_ms": parse_timestamp_ms("2026-02-16T16:00:00+00:00"), "rate": 0.0002},
        ]
        next_iso = project_next_funding_time_iso(
            history,
            interval_hours=8.0,
            now=datetime(2026, 2, 17, 17, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(next_iso, "2026-02-18T00:00:00+00:00")

    def test_is_stale_next_funding(self) -> None:
        now = datetime(2026, 2, 17, 12, 0, tzinfo=timezone.utc)
        self.assertTrue(
            is_stale_next_funding_iso(
                "2026-02-17T11:40:00+00:00",
                now=now,
                grace_seconds=300,
            )
        )
        self.assertFalse(
            is_stale_next_funding_iso(
                "2026-02-17T12:20:00+00:00",
                now=now,
                grace_seconds=300,
            )
        )


if __name__ == "__main__":
    unittest.main()
