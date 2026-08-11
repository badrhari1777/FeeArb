from __future__ import annotations

from analysis_features.bybit_pump_short_outcomes import Series
from analysis_features.pump_short_funding_corrected_portfolio import (
    STRATEGIES,
    find_fail_closed_entry,
    funding_at,
    select_tier,
    simulate_trade,
    synchronized_rescue_required,
)


def make_series() -> Series:
    hour = 3_600_000
    ts = [index * hour for index in range(40)]
    close = [100.0] * 40
    high = [100.0] * 40
    low = [99.0] * 40
    close[1:] = [80.0] * 39
    funding = [(stamp, -0.001) for stamp in ts[:26]] + [(stamp, 0.002) for stamp in ts[26:]]
    oi = {stamp: 100.0 for stamp in ts}
    ratio = {stamp: 0.55 for stamp in ts}
    return Series("TESTUSDT", None, ts, close, high, low, close, funding, oi, ratio)


def test_entry_waits_until_complete_funding_window_passes() -> None:
    series = make_series()
    strategy = STRATEGIES[0]
    tier = select_tier(strategy, 120.0)
    event = {"trigger_ts": 0, "pump_pct": 120.0}

    result = find_fail_closed_entry(series, event, strategy=strategy, tier=tier)

    assert result["ready"] is True
    assert result["funding_prev_24h_pct"] > -1.0
    assert result["entry_ts"] >= 26 * 3_600_000


def test_missing_funding_never_enters() -> None:
    series = make_series()
    series.funding = []
    strategy = STRATEGIES[0]
    result = find_fail_closed_entry(
        series,
        {"trigger_ts": 0, "pump_pct": 120.0},
        strategy=strategy,
        tier=select_tier(strategy, 120.0),
    )
    assert result["ready"] is False
    assert result["entry_ts"] is None


def test_partial_hourly_funding_window_is_missing() -> None:
    series = make_series()
    series.funding = series.funding[-10:]

    assert funding_at(series, series.ts[-1]) is None


def test_current_trade_uses_600_margin_and_live_taper_weights() -> None:
    series = make_series()
    series.close[27:] = [80.0] * 13
    series.high[28] = 121.0
    series.high[29] = 161.0
    series.low[30] = 70.0
    strategy = STRATEGIES[0]
    trade = simulate_trade(
        series,
        {"event_uid": "x", "pump_pct": 120.0},
        strategy=strategy,
        tier=select_tier(strategy, 120.0),
        entry_idx=27,
    )
    assert trade is not None
    assert [round(item["margin"], 6) for item in trade["fills"]] == [100.0, 200.0, 300.0]
    assert sum(item["notional"] for item in trade["fills"]) == 1800.0


def test_rescue_uses_synchronized_cash_paths_and_floor() -> None:
    rows = [
        {
            "entry_ts": 0,
            "exit_ts": 10,
            "net_pnl_usd": 100.0,
            "initial_action_cash_usd": 500.0,
            "cash_profile": [(0, 500.0), (2, 1800.0)],
        },
        {
            "entry_ts": 1,
            "exit_ts": 8,
            "net_pnl_usd": 50.0,
            "initial_action_cash_usd": 300.0,
            "cash_profile": [(1, 300.0), (2, 1500.0)],
        },
    ]

    assert synchronized_rescue_required(rows, 3000.0, start_ts=1) == 375.0
