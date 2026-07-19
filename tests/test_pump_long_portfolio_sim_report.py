from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from analysis_features.pump_long_portfolio_sim_report import (
    LEVERAGES,
    SIZING_MODES,
    SLOT_COUNTS,
    run_pump_long_portfolio_sim_report,
)


def test_pump_long_portfolio_sim_report_builds_csv_and_html() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        input_path = root / "outcomes.csv"
        output_dir = root / "report"
        write_fixture(input_path)

        metadata = run_pump_long_portfolio_sim_report(input_path=input_path, output_dir=output_dir)

        assert metadata["input_rows"] == 8
        assert metadata["simulation_rows"] >= len(SLOT_COUNTS) * len(LEVERAGES) * len(SIZING_MODES)
        assert metadata["trade_rows"] > 0
        assert (output_dir / "simulation_summary.csv").exists()
        assert (output_dir / "simulation_trades.csv").exists()
        assert (output_dir / "equity_points.csv").exists()
        html = (output_dir / "index.html").read_text(encoding="utf-8")
        assert "Bybit pump long" in html
        assert "<svg" in html


def write_fixture(path: Path) -> None:
    fieldnames = [
        "entry_rule",
        "exit_plan",
        "symbol",
        "event_id",
        "entry_ts",
        "entry_iso",
        "exit_ts",
        "exit_iso",
        "exit_reason",
        "net_pct",
        "long_funding_pct",
        "gross_price_pct",
        "mae_pct",
        "mfe_pct",
        "entry_wait_h",
        "entry_premium_pct",
        "entry_premium_relief_1h_pct",
        "entry_oi_change_4h_pct",
        "entry_volume_z",
        "trigger_pump_pct",
    ]
    base_ts = 1_704_067_200_000
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for idx in range(8):
            entry_ts = base_ts + idx * 12 * 3_600_000
            exit_ts = entry_ts + 6 * 3_600_000
            writer.writerow(
                {
                    "entry_rule": "deep_discount_survives",
                    "exit_plan": "tp30_sl25_hold72_fundrelief",
                    "symbol": f"T{idx % 3}USDT",
                    "event_id": f"event-{idx}",
                    "entry_ts": entry_ts,
                    "entry_iso": "2024-01-01T00:00:00+00:00",
                    "exit_ts": exit_ts,
                    "exit_iso": "2024-01-01T06:00:00+00:00",
                    "exit_reason": "take_profit" if idx % 4 else "stop_loss",
                    "net_pct": 30.0 if idx % 4 else -25.0,
                    "long_funding_pct": 0.2,
                    "gross_price_pct": 29.8,
                    "mae_pct": 10.0,
                    "mfe_pct": 35.0,
                    "entry_wait_h": 0.25,
                    "entry_premium_pct": -2.0,
                    "entry_premium_relief_1h_pct": 0.0,
                    "entry_oi_change_4h_pct": 25.0,
                    "entry_volume_z": 2.0,
                    "trigger_pump_pct": 80.0,
                }
            )
