from __future__ import annotations

from analysis_features.pump_short_regression_hybrid_research import (
    GateConfig,
    RuleConfig,
    simulate_portfolio,
    standardized_ridge,
)


def test_standardized_ridge_identifies_positive_driver() -> None:
    xs = [[float(i), float(20 - i)] for i in range(1, 20)]
    y = [row[0] * 2.0 - row[1] * 0.1 for row in xs]

    model = standardized_ridge(xs, y, alpha=0.01)

    assert model["coefficients"][0] > 0
    assert model["coefficients"][1] < 0
    assert model["r2"] > 0.95


def test_sizing_cap_limits_late_compounded_loss() -> None:
    gate = GateConfig("base_only", "Base only", lambda row: False)
    rule = RuleConfig(
        slug="step50_legs4_equal_tp25_168",
        step_pct=50.0,
        max_legs=4,
        sizing_mode="equal",
        exit_plan={"name": "tp25_168", "max_hold_h": 168, "targets": ((25.0, 1.0),)},
    )
    rows = [
        {
            "symbol": "WINUSDT",
            "event_id": "win",
            "entry_ts": 1_000,
            "exit_ts": 2_000,
            "selected_rule_slug": rule.slug,
            "hybrid_bucket": "base",
            "net_reserved_pct": 100.0,
            "max_margin_stress_reserved_pct": 0.0,
        },
        {
            "symbol": "LOSSUSDT",
            "event_id": "loss",
            "entry_ts": 3_000,
            "exit_ts": 4_000,
            "selected_rule_slug": rule.slug,
            "hybrid_bucket": "base",
            "net_reserved_pct": -100.0,
            "max_margin_stress_reserved_pct": 200.0,
        },
    ]

    uncapped, uncapped_selected = simulate_portfolio(
        rows,
        slots=1,
        sizing_cap_usd=None,
        gate=gate,
        defensive_rule=rule,
        base_rule=rule,
    )
    capped, capped_selected = simulate_portfolio(
        rows,
        slots=1,
        sizing_cap_usd=1_000.0,
        gate=gate,
        defensive_rule=rule,
        base_rule=rule,
    )

    assert uncapped["final_capital_usd"] < capped["final_capital_usd"]
    assert uncapped_selected[-1]["pnl_usd"] < capped_selected[-1]["pnl_usd"]
