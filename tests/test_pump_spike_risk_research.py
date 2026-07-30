from __future__ import annotations

import json

from analysis_features.pump_spike_risk_research import (
    analyze_hourly_universe,
    analyze_mark_path,
    cluster_spike_bars,
    first_leg_notional_usd,
    protection_ratios,
    required_topup_for_l2,
)


def test_hourly_universe_reports_prefilter_coverage_and_spikes(tmp_path) -> None:
    samples = tmp_path / "symbol_samples.jsonl"
    done = tmp_path / "done_symbols.txt"
    payload = {
        "instrument": {"base": "TEST"},
        "symbol": "TESTUSDT",
        "series": {
            "klines_1h": [
                {"ts_ms": 1_704_067_200_000, "high": 100.0, "close": 100.0},
                {"ts_ms": 1_704_070_800_000, "high": 140.0, "close": 110.0},
                {"ts_ms": 1_704_074_400_000, "high": 150.0, "close": 120.0},
            ]
        },
    }
    samples.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    done.write_text("TESTUSDT\nQUIETUSDT\n", encoding="utf-8")

    summary, events, coverage = analyze_hourly_universe(
        samples,
        done_symbols_path=done,
    )

    row_30 = next(row for row in summary if row["threshold_pct"] == 30.0)
    assert row_30["qualifying_bars"] == 2
    assert row_30["episodes"] == 1
    assert row_30["wick_episodes_retrace_10pct"] == 1
    assert len(events) == 1
    assert coverage["symbols_checked_by_collector"] == 2
    assert coverage["symbols_with_hourly_archive"] == 1
    assert coverage["symbols_prefiltered_without_hourly_archive"] == 1


def test_hourly_spike_bars_cluster_within_six_hours() -> None:
    rows = [
        {
            "symbol": "TESTUSDT",
            "ts_ms": 1_000,
            "rise_pct": 35.0,
            "retrace_from_high_pct": 12.0,
        },
        {
            "symbol": "TESTUSDT",
            "ts_ms": 1_000 + 3 * 3_600_000,
            "rise_pct": 42.0,
            "retrace_from_high_pct": 8.0,
        },
        {
            "symbol": "TESTUSDT",
            "ts_ms": 1_000 + 10 * 3_600_000,
            "rise_pct": 31.0,
            "retrace_from_high_pct": 4.0,
        },
    ]

    episodes = cluster_spike_bars(rows, max_gap_ms=6 * 3_600_000)

    assert len(episodes) == 2
    assert episodes[0]["bars"] == 2
    assert episodes[0]["max_rise_pct"] == 42.0
    assert episodes[0]["max_retrace_pct"] == 12.0


def test_guaranteed_topup_places_every_current_first_leg_stop_beyond_l2() -> None:
    rules = (
        "step50_legs5_equal_tp25_720",
        "step50_legs3_tapered_tp25_336",
        "step50_legs2_tapered_tp25_720",
    )

    for rule in rules:
        first_notional = first_leg_notional_usd(
            rule,
            slot_margin_usd=175.0,
            leverage=3.0,
        )
        ratios = protection_ratios(
            first_leg_notional_usd=first_notional,
            extra_margin_usd=50.0,
        )
        assert required_topup_for_l2(
            first_leg_notional_usd=first_notional
        ) <= 50.0
        assert ratios["stop_after_topup"] > ratios["l2"]


def test_mark_path_flags_adjacent_warning_stop_and_same_bar_l2() -> None:
    ratios = {
        "warning": 1.08,
        "initial_stop": 1.27,
        "l2": 1.50,
        "stop_after_topup": 1.55,
    }
    candles = [
        {"open": 100.0, "high": 109.0, "close": 108.0},
        {"open": 108.0, "high": 155.0, "close": 120.0},
    ]

    result = analyze_mark_path(candles, ratios=ratios)

    assert result["warning_to_stop_minutes"] == 15
    assert result["warning_to_stop_le_15m"] is True
    assert result["stop_l2_same_15m"] is True
    assert result["burst_ge_30_pct"] is True
