from __future__ import annotations

from analysis_features.bybit_pump_short_outcomes import PumpEvent, Series
from analysis_features.pump_long_strategy_research import (
    LongEntryRule,
    LongExitPlan,
    build_long_regression_diagnostics,
    long_entry_matches,
    replay_long_portfolios,
    simulate_long_exit,
)


def test_long_entry_matches_score_volume_and_funding_filters() -> None:
    rule = LongEntryRule(
        "test",
        max_wait_h=12,
        min_continuation_score=70.0,
        max_exhaustion_score=60.0,
        min_return_1h_pct=5.0,
        min_volume_z=3.0,
        max_funding_8h_pct=-0.05,
        min_oi_6h_pct=30.0,
    )
    features = {
        "return_1h_pct": 6.0,
        "volume_z_24h": 4.0,
        "funding_prev_8h_pct": -0.1,
        "oi_change_6h_pct": 35.0,
    }
    scores = {"squeeze_continuation_score": 80.0, "pump_exhaustion_score": 20.0}

    assert long_entry_matches(features, scores, rule)


def test_long_exit_hits_take_profit_before_time_stop() -> None:
    series = make_series([1.0, 1.05, 1.2, 1.18])
    event = make_event()
    entry = {"idx": 0, "price": 1.0, "features": {}, "scores": {}}
    entry_rule = LongEntryRule("test_entry", 12, 70.0, 60.0)
    exit_plan = LongExitPlan("tp20_sl8_hold6", tp_pct=20.0, stop_pct=8.0, max_hold_h=6)

    outcome = simulate_long_exit(series, event, entry, entry_rule, exit_plan)

    assert outcome is not None
    assert outcome["exit_reason"] == "take_profit"
    assert outcome["gross_price_pct"] == 20.0
    assert outcome["net_pct"] > 19.0


def test_long_exit_stops_before_target_when_low_breaks_first() -> None:
    series = make_series([1.0, 0.9, 1.25])
    event = make_event()
    entry = {"idx": 0, "price": 1.0, "features": {}, "scores": {}}
    entry_rule = LongEntryRule("test_entry", 12, 70.0, 60.0)
    exit_plan = LongExitPlan("tp20_sl8_hold6", tp_pct=20.0, stop_pct=8.0, max_hold_h=6)

    outcome = simulate_long_exit(series, event, entry, entry_rule, exit_plan)

    assert outcome is not None
    assert outcome["exit_reason"] == "stop_loss"
    assert outcome["gross_price_pct"] == -8.0


def test_replay_long_portfolios_enforces_slot_overlap() -> None:
    rows = [
        make_outcome("AAAUSDT", 1000, 5000, 10.0),
        make_outcome("BBBUSDT", 2000, 6000, 10.0),
        make_outcome("CCCUSDT", 7000, 9000, -5.0),
    ]

    summary_rows, trade_rows = replay_long_portfolios(rows, starting_capital_usd=1000.0, slot_counts=(1,))

    summary = summary_rows[0]
    assert summary["trades"] == 2
    assert summary["skipped_slots"] == 1
    assert summary["roi_pct"] > 0
    assert len(trade_rows) == 2


def test_long_regression_diagnostics_emits_coefficients() -> None:
    rows = []
    for idx in range(30):
        net_pct = float(idx - 15)
        rows.append(
            {
                "entry_rule": "test_entry",
                "exit_plan": "test_exit",
                "trigger_pump_pct": float(idx),
                "entry_wait_h": 0.0,
                "continuation_score": float(idx),
                "exhaustion_score": float(30 - idx),
                "funding_prev_8h_pct": -0.1,
                "oi_change_6h_pct": float(idx),
                "volume_z_24h": float(idx),
                "pullback_from_high_pct": 5.0,
                "net_pct": net_pct,
                "mae_pct": max(0.0, 15.0 - net_pct),
                "exit_reason": "take_profit" if net_pct > 0 else "stop_loss",
            }
        )

    diagnostics = build_long_regression_diagnostics(rows)

    assert diagnostics
    assert any(row["target"] == "net_pct" and row["feature"] == "continuation_score" for row in diagnostics)


def make_series(closes: list[float]) -> Series:
    base_ts = 1_900_000_000_000
    return Series(
        symbol="TESTUSDT",
        launch_ms=base_ts - 30 * 86_400_000,
        ts=[base_ts + idx * 3_600_000 for idx in range(len(closes))],
        open=closes,
        high=[value * 1.01 for value in closes],
        low=[value * 0.99 for value in closes],
        close=closes,
        funding=[],
        oi={},
        long_ratio={},
    )


def make_event() -> PumpEvent:
    return PumpEvent(
        event_id="TEST|w4|50|1900000000000",
        symbol="TESTUSDT",
        config_window_h=4,
        config_threshold_pct=50.0,
        trigger_idx=0,
        trigger_ts=1_900_000_000_000,
        pump_pct=50.0,
        trigger_close=1.0,
        age_days=30.0,
        funding_prev_24h_pct=None,
        funding_prev_72h_pct=None,
        oi_change_4h_pct=None,
        oi_change_24h_pct=None,
        long_ratio=None,
    )


def make_outcome(symbol: str, entry_ts: int, exit_ts: int, levered_net_pct: float) -> dict[str, object]:
    return {
        "event_id": f"{symbol}|{entry_ts}",
        "symbol": symbol,
        "entry_rule": "test_entry",
        "exit_plan": "test_exit",
        "entry_ts": entry_ts,
        "entry_iso": "",
        "exit_ts": exit_ts,
        "exit_iso": "",
        "exit_reason": "take_profit" if levered_net_pct > 0 else "stop_loss",
        "net_pct": levered_net_pct / 2.0,
        "levered_net_pct": levered_net_pct,
        "mae_pct": 1.0,
        "mfe_pct": 2.0,
    }
