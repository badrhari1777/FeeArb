from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from analysis_features.pump_short_strategy_graphic_report import STRATEGIES, run_strategy_graphic_report


class PumpShortStrategyGraphicReportTestCase(unittest.TestCase):
    def test_builds_html_summary_actions_and_topups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            write_fixture_summary(input_dir / "capital_slot_summary.csv")
            write_fixture_trades(input_dir / "selected_trades.csv")

            metadata = run_strategy_graphic_report(input_dir=input_dir, output_dir=output_dir)

            self.assertEqual(metadata["summary_rows"], len(STRATEGIES))
            self.assertEqual(metadata["action_rows"], len(STRATEGIES))
            self.assertEqual(metadata["topup_rows"], len(STRATEGIES))
            html = (output_dir / "index.html").read_text(encoding="utf-8")
            self.assertIn("Pump-short", html)
            self.assertIn("<svg", html)
            self.assertTrue((output_dir / "strategy_summary.csv").exists())
            self.assertTrue((output_dir / "actions.csv").exists())
            self.assertTrue((output_dir / "topups.csv").exists())


def write_fixture_summary(path: Path) -> None:
    fieldnames = [
        "capital_usd",
        "slots",
        "funding_window_h",
        "funding_min_pct",
        "tp_pct",
        "trades_skipped_slots",
        "trades_skipped_same_symbol",
        "max_active_seen",
        "win_rate_pct",
        "take_profit_rate_pct",
        "net_pnl_usd",
        "avg_hold_h",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for spec in STRATEGIES:
            writer.writerow(
                {
                    "capital_usd": spec.capital_usd,
                    "slots": spec.slots,
                    "funding_window_h": spec.funding_window_h,
                    "funding_min_pct": spec.funding_min_pct,
                    "tp_pct": spec.tp_pct,
                    "trades_skipped_slots": 0,
                    "trades_skipped_same_symbol": 0,
                    "max_active_seen": spec.slots,
                    "win_rate_pct": 100,
                    "take_profit_rate_pct": 100,
                    "net_pnl_usd": 100,
                    "avg_hold_h": 24,
                }
            )


def write_fixture_trades(path: Path) -> None:
    fieldnames = [
        "capital_usd",
        "slots",
        "strategy",
        "funding_window_h",
        "funding_min_pct",
        "tp_pct",
        "symbol",
        "entry_ts",
        "entry_iso",
        "exit_ts",
        "exit_iso",
        "exit_reason",
        "legs_filled",
        "gross_notional_usd",
        "pnl_usd",
        "funding_usd",
        "net_pct",
        "mae_pct",
        "funding_prev_pct",
        "peak_unrealized_loss_usd",
        "current_margin_topup_usd",
        "manual_topup_beyond_alloc_usd",
    ]
    base_ts = 1_704_067_200_000
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for idx, spec in enumerate(STRATEGIES):
            entry_ts = base_ts + idx * 86_400_000
            exit_ts = entry_ts + 24 * 3_600_000
            writer.writerow(
                {
                    "capital_usd": spec.capital_usd,
                    "slots": spec.slots,
                    "strategy": spec.slug,
                    "funding_window_h": spec.funding_window_h,
                    "funding_min_pct": spec.funding_min_pct,
                    "tp_pct": spec.tp_pct,
                    "symbol": f"T{idx}USDT",
                    "entry_ts": entry_ts,
                    "entry_iso": "2024-01-01T00:00:00+00:00",
                    "exit_ts": exit_ts,
                    "exit_iso": "2024-01-02T00:00:00+00:00",
                    "exit_reason": "take_profit",
                    "legs_filled": 4,
                    "gross_notional_usd": 1000,
                    "pnl_usd": 100,
                    "funding_usd": 0,
                    "net_pct": 10,
                    "mae_pct": 120,
                    "funding_prev_pct": -0.2,
                    "peak_unrealized_loss_usd": 1200,
                    "current_margin_topup_usd": 200,
                    "manual_topup_beyond_alloc_usd": 100,
                }
            )


if __name__ == "__main__":
    unittest.main()
