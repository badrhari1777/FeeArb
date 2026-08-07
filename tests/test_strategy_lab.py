from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analysis_features.strategy_lab import (
    StrategyLabConfig,
    extract_arbitrage_events,
    link_pump_and_arbitrage_events,
    load_pump_event_catalog,
    run_strategy_lab,
    select_api_enrichment_events,
)


BASE_TS = 1_767_225_600_000


def _feature_row(ts_ms: int, mid: float, *, zscore: float = 3.0) -> dict[str, object]:
    return {
        "ts_ms": ts_ms,
        "pair_key": "TESTUSDT|binance|kucoin",
        "canonical_symbol": "TESTUSDT",
        "left_exchange": "binance",
        "right_exchange": "kucoin",
        "mid_spread_pct": mid,
        "mark_spread_pct": mid * 0.9,
        "index_spread_pct": mid * 0.8,
        "open_ab_pct": mid + 0.1,
        "open_ba_pct": -mid - 0.1,
        "close_ab_pct": mid - 0.1,
        "close_ba_pct": -mid + 0.1,
        "zscore_1h": zscore,
        "zscore_4h": zscore,
        "velocity_5m": 0.1,
        "velocity_15m": 0.1,
        "left_funding_hourly": -0.001,
        "right_funding_hourly": -0.002,
        "hours_to_funding": 0.5,
        "left_oi_change_6h_pct": 10.0,
        "right_oi_change_6h_pct": 5.0,
        "coverage_pct": 100.0,
        "spread_points": 100,
    }


def test_event_anchor_is_first_observable_trigger_not_later_peak() -> None:
    rows = [
        _feature_row(BASE_TS, 0.5),
        _feature_row(BASE_TS + 5 * 60_000, 1.0),
        _feature_row(BASE_TS + 10 * 60_000, 4.0),
        _feature_row(BASE_TS + 20 * 60_000, 0.6),
        _feature_row(BASE_TS + 65 * 60_000, 0.4),
        _feature_row(BASE_TS + 245 * 60_000, 0.2),
    ]

    events, rejected = extract_arbitrage_events(rows, StrategyLabConfig())

    assert not rejected
    assert len(events) == 1
    assert events[0]["ts_ms"] == BASE_TS + 5 * 60_000
    assert events[0]["mid_spread_pct"] == 1.0
    assert events[0]["future_mid_spread_15m_pct"] == 0.6


def test_hard_invalid_spread_is_rejected() -> None:
    events, rejected = extract_arbitrage_events(
        [_feature_row(BASE_TS, 200.0)], StrategyLabConfig()
    )

    assert events == []
    assert rejected[0]["reason"] == "hard_invalid_spread_or_contract_mapping"


def test_pump_catalog_and_arbitrage_link_are_normalized(tmp_path: Path) -> None:
    source = tmp_path / "events.csv"
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", "symbol", "ts", "rise"])
        writer.writeheader()
        writer.writerow({"id": "e1", "symbol": "testusdt", "ts": BASE_TS, "rise": 42.0})
    catalog = load_pump_event_catalog(
        [
            {
                "source": "fixture",
                "path": source,
                "event_id": "id",
                "symbol": "symbol",
                "ts": "ts",
                "iso": "",
                "event_type": "surge",
                "metric_fields": ("rise",),
            }
        ]
    )
    arbitrage = {
        "event_id": "arb-1",
        "symbol": "TESTUSDT",
        "ts_ms": BASE_TS + 60 * 60_000,
        "ts_iso": "2026-01-01T01:00:00+00:00",
        "mid_spread_pct": 1.2,
        "mark_spread_pct": 1.1,
        "net_capture_after_cost_4h_pct": 0.3,
    }

    links = link_pump_and_arbitrage_events(catalog, [arbitrage])

    assert catalog[0]["symbol"] == "TESTUSDT"
    assert json.loads(catalog[0]["metrics_json"])["rise"] == 42.0
    assert links[0]["arb_minus_pump_hours"] == 1.0


def test_api_selection_uses_one_largest_event_per_symbol() -> None:
    selected = select_api_enrichment_events(
        [
            {"symbol": "AUSDT", "mid_spread_pct": 1.0},
            {"symbol": "AUSDT", "mid_spread_pct": 4.0},
            {"symbol": "BUSDT", "mid_spread_pct": -3.0},
        ],
        max_events=2,
    )

    assert [(row["symbol"], row["mid_spread_pct"]) for row in selected] == [
        ("AUSDT", 4.0),
        ("BUSDT", -3.0),
    ]


def test_full_research_run_writes_no_trading_evidence_package(tmp_path: Path) -> None:
    db_path = tmp_path / "coin_analysis.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE ca_feature_snapshots (
            ts_ms INTEGER, pair_key TEXT, canonical_symbol TEXT, direction TEXT,
            features_json TEXT, data_quality_json TEXT
        );
        CREATE TABLE ca_funding_history (
            canonical_symbol TEXT, exchange TEXT, ts_ms INTEGER, funding_rate REAL
        );
        """
    )
    for offset_min, mid in ((0, 0.5), (5, 1.0), (20, 0.6), (65, 0.4), (245, 0.2)):
        row = _feature_row(BASE_TS + offset_min * 60_000, mid)
        common = {
            "left_exchange": row["left_exchange"],
            "right_exchange": row["right_exchange"],
            "derived_spread": {
                "mid_spread_pct": row["mid_spread_pct"],
                "mark_spread_pct": row["mark_spread_pct"],
                "index_spread_pct": row["index_spread_pct"],
                "open_spread_long_a_short_b_pct": row["open_ab_pct"],
                "open_spread_long_b_short_a_pct": row["open_ba_pct"],
                "close_spread_long_a_short_b_pct": row["close_ab_pct"],
                "close_spread_long_b_short_a_pct": row["close_ba_pct"],
            },
            "spread_features": {
                "spread_zscore_1h": row["zscore_1h"],
                "spread_zscore_4h": row["zscore_4h"],
                "spread_velocity_5m": row["velocity_5m"],
                "spread_velocity_15m": row["velocity_15m"],
            },
            "funding": {
                "left_hourly": row["left_funding_hourly"],
                "right_hourly": row["right_funding_hourly"],
            },
            "hours_to_next_funding_min": row["hours_to_funding"],
            "oi": {
                "left_change_6h_pct": row["left_oi_change_6h_pct"],
                "right_change_6h_pct": row["right_oi_change_6h_pct"],
            },
        }
        conn.execute(
            "INSERT INTO ca_feature_snapshots VALUES (?, ?, ?, ?, ?, ?)",
            (
                row["ts_ms"],
                row["pair_key"],
                row["canonical_symbol"],
                "long_a_short_b",
                json.dumps({"common": common}),
                json.dumps({"coverage_pct": 100.0, "spread_points_total": 100}),
            ),
        )
    for index, rate in enumerate((-0.02, -0.015, -0.01)):
        conn.execute(
            "INSERT INTO ca_funding_history VALUES (?, ?, ?, ?)",
            ("TESTUSDT", "binance", BASE_TS + index * 8 * 3_600_000, rate),
        )
    conn.commit()
    conn.close()

    output_dir = tmp_path / "output"
    metadata = run_strategy_lab(
        db_path=db_path,
        log_dir=tmp_path / "missing_logs",
        output_dir=output_dir,
    )

    assert metadata["mode"] == "research_only_no_trading"
    assert metadata["arbitrage_events"] == 1
    assert metadata["api_enrichment_requested"] is False
    assert (output_dir / "hypothesis_registry.csv").exists()
    assert (output_dir / "pump_event_catalog.csv").exists()
    assert (output_dir / "pump_arbitrage_event_links.csv").read_text(
        encoding="utf-8"
    ).startswith("pump_event_id,arbitrage_event_id")
    assert "does not arm strategies" in (output_dir / "index.md").read_text(encoding="utf-8")
    assert datetime.fromisoformat(metadata["created_at"]).tzinfo == timezone.utc
