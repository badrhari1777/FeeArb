from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from analysis_features.bybit_pump_short_paper import apply_shadow_rows_to_paper, read_paper_summary


class BybitPumpShortPaperTestCase(unittest.TestCase):
    def test_opens_and_closes_paper_position_from_shadow_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "paper_positions.json"
            events_path = root / "paper_events.jsonl"
            csv_path = root / "paper_positions_latest.csv"
            open_row = {
                "ts_ms": 1_900_000_000_000,
                "status": "entry_candidate",
                "symbol": "TESTUSDT",
                "event_id": "TEST|event",
                "last_close": 100.0,
                "matched_profile": "conservative",
                "matched_profile_rank": "1",
                "matched_entry_strategy": "pb20_oi50_lr_mid_ladder3_step_50",
                "matched_exit_strategy": "tp25_full_168",
                "matched_anti_overfit_status": "robust_candidate",
            }

            opened = apply_shadow_rows_to_paper([open_row], state_path=state_path, events_path=events_path, csv_path=csv_path)
            self.assertEqual(opened["open_positions"], 1)

            update_row = dict(open_row)
            update_row["status"] = "watch_profile"
            update_row["ts_ms"] = 1_900_000_000_000 + 2 * 3_600_000
            update_row["last_close"] = 70.0
            closed = apply_shadow_rows_to_paper([update_row], state_path=state_path, events_path=events_path, csv_path=csv_path)
            summary = read_paper_summary(state_path=state_path)

        self.assertEqual(closed["closed_positions"], 1)
        self.assertEqual(summary["positions"][0]["status"], "closed")
        self.assertEqual(summary["positions"][0]["exit_reason"], "target_25")
        self.assertGreater(summary["positions"][0]["realized_net_pct"], 20.0)


if __name__ == "__main__":
    unittest.main()
