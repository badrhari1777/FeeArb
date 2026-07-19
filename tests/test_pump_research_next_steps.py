from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

from analysis_features.pump_research_next_steps import (
    build_failed_absorption_classifier,
    build_funding_first_outcomes,
    build_premium_relief_exit_outcomes,
    run_pump_research_next_steps,
)


def test_relief_exit_and_funding_first_emit_outcomes() -> None:
    sample = make_sample()

    relief_rows = build_premium_relief_exit_outcomes([sample])
    funding_rows = build_funding_first_outcomes([sample])

    assert relief_rows
    assert funding_rows
    assert any(row["exit_reason"] in {"take_profit", "premium_relief_exit", "premium_relief_from_entry_exit"} for row in relief_rows)
    assert all("entry_prev_funding_rate_pct" in row for row in funding_rows)


def test_failed_absorption_classifier_separates_bad_cases() -> None:
    rows = [
        base_long_row("AUSDT", "take_profit", 30.0, entry_wait_h=0.25, oi4=20.0, volume_z=2.0),
        base_long_row("BUSDT", "stop_loss", -25.0, entry_wait_h=2.0, oi4=-5.0, volume_z=0.0),
    ]

    cases, summary, filters = build_failed_absorption_classifier(rows)

    assert {row["absorption_label"] for row in cases} == {"clean_tp", "failed_absorption"}
    assert summary
    assert filters


def test_research_next_steps_builds_outputs() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        event_windows = root / "event_windows.jsonl"
        long_outcomes = root / "long.csv"
        long_portfolio = root / "long_portfolio.csv"
        cycle_summary = root / "cycle_summary.csv"
        cycle_trades = root / "cycle_trades.csv"
        shadow_history = root / "shadow.jsonl"
        active_window = root / "active.csv"
        output_dir = root / "out"
        event_windows.write_text(json.dumps(make_sample()) + "\n", encoding="utf-8")
        write_rows(long_outcomes, [base_long_row("AUSDT", "take_profit", 30.0), base_long_row("BUSDT", "stop_loss", -25.0)])
        write_rows(
            long_portfolio,
            [
                {
                    "entry_rule": "deep_discount_survives",
                    "exit_plan": "tp30_sl25_hold72_fundrelief",
                    "filter_slug": "all_entries",
                    "slots": 2,
                    "trades": 12,
                    "roi_pct": 100,
                    "risk_adjusted_roi_pct": 80,
                    "win_pct": 75,
                    "max_drawdown_pct": 10,
                    "worst_trade_pct": -20,
                    "stop_loss_pct": 25,
                }
            ],
        )
        write_rows(
            cycle_summary,
            [
                {
                    "allocation_id": "cycle_6_4s2l",
                    "long_track_id": "long_broad",
                    "short_track_id": "short_clean_p100_l3",
                    "trades": 20,
                    "roi_pct": 120,
                    "risk_adjusted_roi_pct": 90,
                    "win_pct": 80,
                    "max_drawdown_pct": 8,
                    "worst_trade_pct": -15,
                }
            ],
        )
        write_rows(
            cycle_trades,
            [
                {
                    "side": "long",
                    "track_id": "long_broad",
                    "symbol": "AUSDT",
                    "event_id": "A",
                    "entry_ts": 10 * 3_600_000,
                    "entry_iso": "",
                    "net_pct": 30.0,
                }
            ],
        )
        shadow_history.write_text(json.dumps({"rows": [{"symbol": "AUSDT"}]}) + "\n", encoding="utf-8")
        write_rows(active_window, [{"symbol": "AUSDT"}])

        metadata = run_pump_research_next_steps(
            event_windows_path=event_windows,
            long_outcomes_path=long_outcomes,
            long_portfolio_path=long_portfolio,
            cycle_summary_path=cycle_summary,
            cycle_trades_path=cycle_trades,
            shadow_history_path=shadow_history,
            active_window_path=active_window,
            output_dir=output_dir,
        )

        assert metadata["scorecard_rows"] > 0
        assert (output_dir / "strategy_scorecard.csv").exists()
        assert (output_dir / "index.html").exists()


def make_sample() -> dict:
    hour_ms = 3_600_000
    trigger_ts = 10 * hour_ms
    klines = []
    premium = []
    oi = []
    for idx in range(90):
        ts_ms = idx * hour_ms // 4
        price = 100.0
        if ts_ms >= trigger_ts:
            price = 100.0 + min(30.0, (ts_ms - trigger_ts) / hour_ms * 10.0)
        klines.append(
            {
                "ts_ms": ts_ms,
                "open": price,
                "high": price * 1.03,
                "low": price * 0.98,
                "close": price,
                "volume": 100.0 + idx,
            }
        )
        prem = -0.03 if ts_ms <= trigger_ts + hour_ms else -0.004
        premium.append({"ts_ms": ts_ms, "open": prem, "high": prem, "low": prem, "close": prem})
        oi.append({"ts_ms": ts_ms, "open_interest": 1000.0 + idx * 10.0})
    return {
        "event": {"event_id": "AUSDT|pump", "trigger_pump_pct": 150.0},
        "symbol": "AUSDT",
        "trigger_ts": trigger_ts,
        "trigger_iso": "",
        "intervals": {"15": {"klines": klines, "premium_index_klines": premium, "open_interest": oi}},
        "funding": [
            {"ts_ms": trigger_ts - hour_ms, "funding_rate": -0.004},
            {"ts_ms": trigger_ts + 8 * hour_ms, "funding_rate": -0.004},
        ],
    }


def base_long_row(symbol: str, exit_reason: str, net_pct: float, *, entry_wait_h: float = 0.25, oi4: float = 10.0, volume_z: float = 1.0) -> dict:
    return {
        "entry_rule": "deep_discount_survives",
        "exit_plan": "tp30_sl25_hold72_fundrelief",
        "symbol": symbol,
        "event_id": symbol,
        "entry_ts": 10 * 3_600_000,
        "entry_iso": "",
        "exit_ts": 20 * 3_600_000,
        "exit_iso": "",
        "exit_reason": exit_reason,
        "entry_wait_h": entry_wait_h,
        "entry_premium_pct": -2.0,
        "entry_premium_relief_1h_pct": 0.0,
        "entry_return_1h_pct": 5.0,
        "entry_oi_change_4h_pct": oi4,
        "entry_volume_z": volume_z,
        "long_funding_pct": 0.2,
        "net_pct": net_pct,
        "mae_pct": 10.0,
        "mfe_pct": 30.0,
    }


def write_rows(path: Path, rows: list[dict]) -> None:
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
