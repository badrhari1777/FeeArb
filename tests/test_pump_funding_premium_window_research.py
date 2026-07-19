from __future__ import annotations

from analysis_features.pump_funding_premium_window_research import (
    FilterSpec,
    build_factor_bucket_summary,
    build_feature_regression,
    build_filter_sweep_summary,
    build_portfolio_replays,
    filter_matches,
    simulate_sample,
)


def test_premium_discount_long_counts_negative_funding_as_credit() -> None:
    hour_ms = 3_600_000
    trigger_ts = 10 * hour_ms
    klines = []
    premium = []
    oi = []
    for idx in range(40):
        ts_ms = idx * hour_ms
        price = 100.0 if idx < 10 else 100.0 + max(0, idx - 10) * 2.0
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
        premium.append({"ts_ms": ts_ms, "open": -0.02, "high": -0.015, "low": -0.025, "close": -0.02})
        oi.append({"ts_ms": ts_ms, "open_interest": 1000.0 + idx * 20.0})
    sample = {
        "event": {"event_id": "AAA|pump", "trigger_pump_pct": 150.0},
        "symbol": "AAAUSDT",
        "trigger_ts": trigger_ts,
        "trigger_iso": "trigger",
        "intervals": {
            "60": {
                "klines": klines,
                "premium_index_klines": premium,
                "open_interest": oi,
            }
        },
        "funding": [
            {"ts_ms": trigger_ts + 8 * hour_ms, "funding_rate": -0.01},
            {"ts_ms": trigger_ts + 16 * hour_ms, "funding_rate": -0.01},
        ],
    }

    rows = simulate_sample(sample)

    assert rows
    best = max(rows, key=lambda row: row["net_pct"])
    assert best["long_funding_pct"] > 0
    assert best["net_pct"] > best["gross_price_pct"]


def test_premium_regression_and_buckets_emit_rows() -> None:
    rows = []
    for idx in range(45):
        net_pct = float(idx - 20)
        rows.append(
            {
                "entry_rule": "deep_discount_survives",
                "exit_plan": "tp30_sl25_hold72_fundrelief",
                "trigger_pump_pct": 50.0 + idx,
                "entry_wait_h": float(idx % 10),
                "entry_premium_pct": -3.0 + idx * 0.05,
                "entry_premium_relief_1h_pct": idx * 0.01,
                "entry_return_1h_pct": idx * 0.1,
                "entry_oi_change_4h_pct": idx * 0.2,
                "entry_volume_z": idx * 0.1,
                "long_funding_pct": idx * 0.05,
                "exit_premium_pct": -1.0 + idx * 0.01,
                "net_pct": net_pct,
                "mae_pct": 30.0 - net_pct,
                "exit_reason": "take_profit" if net_pct > 0 else "stop_loss",
            }
        )

    regression = build_feature_regression(rows)
    buckets = build_factor_bucket_summary(rows, [{"entry_rule": "deep_discount_survives", "exit_plan": "tp30_sl25_hold72_fundrelief"}])

    assert regression
    assert buckets


def test_filter_matches_uses_only_entry_features() -> None:
    row = {
        "entry_wait_h": 1.0,
        "entry_oi_change_4h_pct": 12.0,
        "entry_volume_z": 2.0,
        "entry_premium_relief_1h_pct": -0.5,
        "entry_premium_pct": -2.0,
        "entry_return_1h_pct": 8.0,
    }

    assert filter_matches(
        row,
        FilterSpec(
            "test",
            max_entry_wait_h=3.0,
            min_oi_change_4h_pct=10.0,
            min_entry_premium_pct=-5.0,
            max_entry_premium_pct=-1.2,
        ),
    )
    assert not filter_matches(row, FilterSpec("too_slow", max_entry_wait_h=0.0))
    assert not filter_matches(row, FilterSpec("too_shallow_premium", max_entry_premium_pct=-2.5))


def test_filter_sweep_and_portfolio_replay_emit_rows() -> None:
    rows = []
    base_ts = 1_900_000_000_000
    for idx in range(40):
        rows.append(
            {
                "entry_rule": "deep_discount_survives",
                "exit_plan": "tp30_sl25_hold72_fundrelief",
                "filter_slug": "all_entries",
                "symbol": f"SYM{idx % 10}USDT",
                "event_id": f"event-{idx}",
                "entry_ts": base_ts + idx * 10_000_000,
                "entry_iso": "",
                "exit_ts": base_ts + idx * 10_000_000 + 3_600_000,
                "exit_iso": "",
                "exit_reason": "take_profit" if idx % 3 else "stop_loss",
                "entry_wait_h": float(idx % 4),
                "entry_oi_change_4h_pct": float(idx),
                "entry_volume_z": 1.0,
                "entry_premium_relief_1h_pct": 0.0,
                "entry_premium_pct": -2.0,
                "entry_return_1h_pct": 5.0,
                "net_pct": 30.0 if idx % 3 else -25.0,
                "long_funding_pct": 0.2,
            }
        )

    filter_rows = build_filter_sweep_summary(rows)
    portfolio_rows, trade_rows = build_portfolio_replays(rows, slot_counts=(2,))

    assert filter_rows
    assert portfolio_rows
    assert trade_rows
