import csv
import json
from pathlib import Path

from analysis_features.pump_short_strategy_compound_report import run_strategy_compound_report
from analysis_features.pump_short_strategy_graphic_report import CAPITAL_USD, STRATEGIES


def write_selected_trades(path: Path) -> None:
    rows = []
    base_entry = 1_704_067_200_000
    day_ms = 24 * 60 * 60 * 1000
    for idx, spec in enumerate(STRATEGIES):
        entry_ts = base_entry + idx * day_ms
        exit_ts = entry_ts + 12 * 60 * 60 * 1000
        static_step_notional = CAPITAL_USD / spec.slots / 4 * 3
        rows.append(
            {
                "capital_usd": str(CAPITAL_USD),
                "slots": str(spec.slots),
                "strategy": spec.slug,
                "funding_window_h": str(spec.funding_window_h),
                "funding_min_pct": str(spec.funding_min_pct),
                "tp_pct": str(spec.tp_pct),
                "symbol": f"TEST{idx}USDT",
                "entry_ts": str(entry_ts),
                "entry_iso": f"2024-01-0{idx + 1} 00:00 UTC",
                "exit_ts": str(exit_ts),
                "exit_iso": f"2024-01-0{idx + 1} 12:00 UTC",
                "exit_reason": "tp",
                "legs_filled": "2",
                "per_step_notional_usd": f"{static_step_notional:.8f}",
                "gross_notional_usd": f"{static_step_notional * 2:.8f}",
                "pnl_usd": "100.0",
                "funding_usd": "-2.0",
                "net_pct": "9.8",
                "mae_pct": "40.0",
                "funding_prev_pct": "-0.1",
                "peak_unrealized_loss_usd": "600.0",
                "current_margin_topup_usd": "40.0",
                "manual_topup_beyond_alloc_usd": "350.0",
            }
        )
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_compound_report_generates_html_and_csv(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    write_selected_trades(input_dir / "selected_trades.csv")

    metadata = run_strategy_compound_report(input_dir=input_dir, output_dir=output_dir)

    assert len(metadata["strategies"]) == len(STRATEGIES)
    assert metadata["action_rows"] == len(STRATEGIES)
    assert metadata["topup_rows"] > 0
    assert (output_dir / "index.html").exists()
    assert (output_dir / "compound_strategy_summary.csv").exists()
    assert (output_dir / "compound_actions.csv").exists()
    assert (output_dir / "compound_topups.csv").exists()

    html = (output_dir / "index.html").read_text(encoding="utf-8")
    assert "<svg" in html
    assert "capital at entry / max coins / 4 ladder steps" in html

    with (output_dir / "compound_strategy_summary.csv").open(encoding="utf-8") as f:
        summary_rows = list(csv.DictReader(f))
    assert float(summary_rows[0]["final_capital_usd"]) > CAPITAL_USD
    assert float(summary_rows[0]["roi_on_initial_pct"]) > 0

    stored_metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert stored_metadata["capital_model"] == "dynamic_current_capital_per_coin_per_ladder_step"
