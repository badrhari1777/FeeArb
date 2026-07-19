from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from analysis_features.pump_cycle_portfolio_report import run_pump_cycle_portfolio_report


def test_pump_cycle_portfolio_report_builds_combined_outputs() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        long_path = root / "long.csv"
        short_path = root / "short.csv"
        output_dir = root / "out"
        write_long_fixture(long_path)
        write_short_fixture(short_path)

        metadata = run_pump_cycle_portfolio_report(
            long_outcomes_path=long_path,
            short_trades_path=short_path,
            output_dir=output_dir,
            starting_capital_usd=3000.0,
        )

        assert metadata["summary_rows"] > 0
        assert metadata["trade_rows"] > 0
        assert (output_dir / "cycle_summary.csv").exists()
        assert (output_dir / "cycle_trades.csv").exists()
        assert (output_dir / "cycle_equity.csv").exists()
        html = (output_dir / "index.html").read_text(encoding="utf-8")
        assert "Pump-cycle" in html
        assert "<svg" in html


def write_long_fixture(path: Path) -> None:
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
        "entry_wait_h",
        "entry_premium_pct",
        "entry_oi_change_4h_pct",
        "entry_volume_z",
    ]
    base_ts = 1_704_067_200_000
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for idx in range(6):
            entry_ts = base_ts + idx * 10 * 3_600_000
            writer.writerow(
                {
                    "entry_rule": "deep_discount_survives",
                    "exit_plan": "tp30_sl25_hold72_fundrelief",
                    "symbol": f"L{idx}USDT",
                    "event_id": f"long-{idx}",
                    "entry_ts": entry_ts,
                    "entry_iso": "2024-01-01T00:00:00+00:00",
                    "exit_ts": entry_ts + 4 * 3_600_000,
                    "exit_iso": "2024-01-01T04:00:00+00:00",
                    "exit_reason": "take_profit",
                    "net_pct": 30.0,
                    "entry_wait_h": 0.25,
                    "entry_premium_pct": -2.0,
                    "entry_oi_change_4h_pct": 25.0,
                    "entry_volume_z": 2.0,
                }
            )


def write_short_fixture(path: Path) -> None:
    fieldnames = [
        "policy_slug",
        "slots",
        "sizing_mode",
        "symbol",
        "case_id",
        "entry_ts",
        "entry_iso",
        "exit_ts",
        "exit_iso",
        "per_coin_capital_usd",
        "net_pct",
        "topup_usd",
        "exit_reason",
        "pump_pct",
        "stress_pct",
        "rule_slug",
    ]
    base_ts = 1_704_067_200_000 + 2 * 3_600_000
    policies = [
        "pump_ge_100__step50_legs3_tapered_tp25_336",
        "pump_ge_80__step50_legs3_tapered_tp25_336",
        "pump_ge_100__step50_legs2_tapered_tp25_336",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for policy in policies:
            for idx in range(4):
                entry_ts = base_ts + idx * 9 * 3_600_000
                writer.writerow(
                    {
                        "policy_slug": policy,
                        "slots": 4,
                        "sizing_mode": "dynamic",
                        "symbol": f"S{idx}USDT",
                        "case_id": f"short-{policy}-{idx}",
                        "entry_ts": entry_ts,
                        "entry_iso": "2024-01-01T02:00:00+00:00",
                        "exit_ts": entry_ts + 6 * 3_600_000,
                        "exit_iso": "2024-01-01T08:00:00+00:00",
                        "per_coin_capital_usd": 750.0,
                        "net_pct": 5.0,
                        "topup_usd": 25.0 if idx == 0 else 0.0,
                        "exit_reason": "target_25",
                        "pump_pct": 100.0,
                        "stress_pct": 10.0,
                        "rule_slug": "step50_legs3_tapered_tp25_336",
                    }
                )
