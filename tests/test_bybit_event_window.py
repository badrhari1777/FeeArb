from __future__ import annotations

from analysis_collectors.bybit_event_window import build_event_window_summary, select_events


def test_select_events_filters_symbols_and_pump_strength() -> None:
    events = [
        {"event_id": "A", "symbol": "AAAUSDT", "trigger_ts": "1000", "trigger_pump_pct": "50"},
        {"event_id": "B", "symbol": "BBBUSDT", "trigger_ts": "2000", "trigger_pump_pct": "150"},
        {"event_id": "C", "symbol": "CCCUSDT", "trigger_ts": "3000", "trigger_pump_pct": "120"},
        {"event_id": "B_SMALLER_DUP", "symbol": "BBBUSDT", "trigger_ts": "2000", "trigger_pump_pct": "130"},
    ]

    selected = select_events(events, min_pump_pct=100.0, max_events=2, symbols={"BBBUSDT", "CCCUSDT"})

    assert [event["event_id"] for event in selected] == ["B", "C"]


def test_build_event_window_summary_calculates_premium_funding_and_oi() -> None:
    hour_ms = 3_600_000
    trigger_ts = 10 * hour_ms
    klines = [
        {"ts_ms": (idx * hour_ms), "open": 100.0, "high": 100.0 + idx, "low": 100.0 - idx, "close": 100.0 + idx, "volume": 100.0 + idx}
        for idx in range(30)
    ]
    sample = {
        "event": {"event_id": "AAA|pump", "trigger_pump_pct": 120.0},
        "symbol": "AAAUSDT",
        "trigger_ts": trigger_ts,
        "trigger_iso": "trigger",
        "end_ts": trigger_ts + 72 * hour_ms,
        "intervals": {
            "60": {
                "klines": klines,
                "premium_index_klines": [
                    {"ts_ms": trigger_ts - hour_ms, "open": -0.02, "high": -0.015, "low": -0.025, "close": -0.02},
                    {"ts_ms": trigger_ts, "open": -0.03, "high": -0.02, "low": -0.04, "close": -0.03},
                    {"ts_ms": trigger_ts + hour_ms, "open": -0.01, "high": 0.0, "low": -0.02, "close": -0.01},
                ],
                "mark_price_klines": [{"ts_ms": trigger_ts, "close": 97.0}],
                "index_price_klines": [{"ts_ms": trigger_ts, "close": 100.0}],
                "open_interest": [
                    {"ts_ms": trigger_ts, "open_interest": 1000.0},
                    {"ts_ms": trigger_ts + 24 * hour_ms, "open_interest": 1500.0},
                ],
            }
        },
        "funding": [
            {"ts_ms": trigger_ts - hour_ms, "funding_rate": -0.01},
            {"ts_ms": trigger_ts + hour_ms, "funding_rate": -0.02},
        ],
    }

    summary = build_event_window_summary(sample)

    assert summary["premium_trigger_pct"] == -3.0
    assert summary["mark_index_basis_trigger_pct"] == -3.0
    assert summary["funding_prev_24h_pct"] == -1.0
    assert summary["funding_next_24h_pct"] == -2.0
    assert summary["oi_change_24h_pct"] == 50.0
