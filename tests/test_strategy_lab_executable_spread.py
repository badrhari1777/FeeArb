from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from analysis_features.strategy_lab_executable_spread import (
    ExecutableSpreadConfig,
    classify_price_match,
    funding_cashflow,
    run_executable_spread_timing,
)


BASE_TS = 1_767_225_600_000


def _create_fixture(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE ca_feature_snapshots (
            ts_ms INTEGER, pair_key TEXT, canonical_symbol TEXT, direction TEXT,
            features_json TEXT, data_quality_json TEXT
        );
        CREATE TABLE ca_market_snapshots_focus (
            ts_ms INTEGER, canonical_symbol TEXT, exchange TEXT,
            bid REAL, ask REAL, bid_size REAL, ask_size REAL,
            quote_age_ms INTEGER, staleness_flag INTEGER
        );
        CREATE TABLE ca_funding_history (
            canonical_symbol TEXT, exchange TEXT, ts_ms INTEGER,
            funding_rate REAL
        );
        """
    )
    points = [
        (0, -1.00, -0.995, -1.10),
        (5, -1.30, -1.295, -1.40),
        (10, -1.10, -1.095, -1.20),
        (15, -0.80, -0.795, -0.90),
    ]
    quotes = {
        0: ((99.9, 100.0), (101.0, 101.1)),
        5: ((99.9, 100.0), (101.3, 101.4)),
        10: ((99.9, 100.0), (101.1, 101.2)),
        15: ((99.9, 100.0), (100.4, 100.5)),
    }
    for offset_min, mid, open_ab, close_ab in points:
        common = {
            "left_exchange": "binance",
            "right_exchange": "kucoin",
            "derived_spread": {
                "mid_spread_pct": mid,
                "mark_spread_pct": mid * 0.95,
                "index_spread_pct": mid * 0.9,
                "open_spread_long_a_short_b_pct": open_ab,
                "open_spread_long_b_short_a_pct": -open_ab,
                "close_spread_long_a_short_b_pct": close_ab,
                "close_spread_long_b_short_a_pct": -close_ab,
            },
            "spread_features": {
                "spread_zscore_1h": 3.0,
                "spread_zscore_4h": 3.0,
                "spread_velocity_5m": -0.1,
                "spread_velocity_15m": -0.1,
            },
            "funding": {
                "left_hourly": 0.0,
                "right_hourly": 0.0,
                "time_to_next_funding_hours_left": 8.0,
                "time_to_next_funding_hours_right": 8.0,
            },
            "hours_to_next_funding_min": 8.0,
            "oi": {"left_change_6h_pct": 1.0, "right_change_6h_pct": 1.0},
        }
        ts_ms = BASE_TS + offset_min * 60_000
        conn.execute(
            "INSERT INTO ca_feature_snapshots VALUES (?, ?, ?, ?, ?, ?)",
            (
                ts_ms,
                "TESTUSDT|binance|kucoin",
                "TESTUSDT",
                "long_a_short_b",
                json.dumps({"common": common}),
                json.dumps({"coverage_pct": 100.0, "spread_points_total": 100}),
            ),
        )
        for exchange, (bid, ask) in zip(("binance", "kucoin"), quotes[offset_min]):
            conn.execute(
                "INSERT INTO ca_market_snapshots_focus VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ts_ms, "TESTUSDT", exchange, bid, ask, 100.0, 100.0, 0, 0),
            )
    conn.commit()
    conn.close()


def test_soft_price_match_is_research_tolerance_only() -> None:
    assert classify_price_match(
        1.0, 1.7, exact_tolerance_pct=0.75, soft_tolerance_pct=2.0
    )[0] == "confirmed"
    assert classify_price_match(
        1.0, 2.0, exact_tolerance_pct=0.75, soft_tolerance_pct=2.0
    )[0] == "within_2pct_tolerance"
    assert classify_price_match(
        1.0, 3.1, exact_tolerance_pct=0.75, soft_tolerance_pct=2.0
    )[0] == "divergent"


def test_actual_funding_cashflow_uses_each_settlement(tmp_path: Path) -> None:
    db_path = tmp_path / "funding.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE ca_funding_history (canonical_symbol TEXT, exchange TEXT, ts_ms INTEGER, funding_rate REAL)"
    )
    conn.executemany(
        "INSERT INTO ca_funding_history VALUES (?, ?, ?, ?)",
        [
            ("TESTUSDT", "binance", BASE_TS + 60_000, 0.01),
            ("TESTUSDT", "kucoin", BASE_TS + 60_000, -0.02),
            ("TESTUSDT", "binance", BASE_TS + 120_000, 0.005),
        ],
    )
    conn.commit()
    conn.row_factory = sqlite3.Row
    result = funding_cashflow(
        conn,
        symbol="TESTUSDT",
        left_exchange="binance",
        right_exchange="kucoin",
        direction="long_a_short_b",
        entry_ts_ms=BASE_TS,
        exit_ts_ms=BASE_TS + 180_000,
        entry_row={"left_time_to_funding_h": 0.01, "right_time_to_funding_h": 0.01},
    )
    conn.close()

    assert result["status"] == "complete"
    assert result["settlements"] == 3
    assert result["cashflow_pct"] == pytest.approx(-3.5)


def test_missing_funding_schedule_is_not_assumed_zero(tmp_path: Path) -> None:
    db_path = tmp_path / "funding.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE ca_funding_history (canonical_symbol TEXT, exchange TEXT, ts_ms INTEGER, funding_rate REAL)"
    )
    conn.row_factory = sqlite3.Row
    result = funding_cashflow(
        conn,
        symbol="TESTUSDT",
        left_exchange="binance",
        right_exchange="kucoin",
        direction="long_a_short_b",
        entry_ts_ms=BASE_TS,
        exit_ts_ms=BASE_TS + 60_000,
        entry_row={"left_time_to_funding_h": None, "right_time_to_funding_h": 8.0},
    )
    conn.close()

    assert result["status"] == "incomplete"
    assert result["reason"] == "left_funding_schedule_missing"


def test_research_runner_writes_timing_and_fail_closed_capacity(tmp_path: Path) -> None:
    db_path = tmp_path / "coin_analysis.db"
    _create_fixture(db_path)
    output_dir = tmp_path / "output"
    metadata = run_executable_spread_timing(
        db_path=db_path,
        output_dir=output_dir,
        config=ExecutableSpreadConfig(
            source_max_ts_ms=BASE_TS + 15 * 60_000,
            entry_delays_min=(0,),
            outcome_horizons_min=(15,),
            feature_match_tolerance_min=1.0,
        ),
        code_commit="fixture-commit",
    )

    rows = list(__import__("csv").DictReader((output_dir / "timing_outcomes.csv").open(encoding="utf-8")))
    now = next(row for row in rows if row["entry_policy"] == "now")

    assert metadata["live_actions"] is False
    assert metadata["paper_promotion_allowed"] is False
    assert metadata["source_snapshot"]["ca_feature_snapshots"]["rows"] == 4
    assert metadata["source_snapshot_id"]
    assert metadata["evaluated_rows"] >= 1
    assert now["status"] == "EVALUATED"
    assert float(now["net_capture_pct"]) > 0
    assert now["capacity_status"] == "contract_multiplier_missing"
    assert now["capacity_usd"] == ""
    assert now["execution_ready"] == "False"
    summary = (output_dir / "timing_summary.csv").read_text(encoding="utf-8")
    assert "top_symbol_abs_contribution_share" in summary
    assert "No paper, shadow, ARM or live actions" in (output_dir / "index.md").read_text(encoding="utf-8")
