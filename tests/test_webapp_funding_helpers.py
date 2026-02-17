from __future__ import annotations

import unittest

from webapp.services import _funding_history_ts_ms, _resolve_funding_interval_hours


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


if __name__ == "__main__":
    unittest.main()
