from __future__ import annotations

from analysis_features.pump_short_per_event_strategy_research import BASE_RULE_SLUG
from analysis_features.pump_short_policy_portfolio_research import (
    PolicySpec,
    PortfolioConfig,
    build_gates,
    build_policies,
    build_unique_cases,
    simulate_policy_portfolio,
)


def test_build_unique_cases_collapses_duplicate_trigger_rows() -> None:
    rows = [
        {
            "status": "entered",
            "symbol": "SIRENUSDT",
            "entry_ts": "1000",
            "trigger_ts": "500",
            "case_id": "low",
            "pump_pct": "90",
            "config_threshold_pct": "80",
            "config_window_h": "24",
            "age_days": "20",
            "funding_prev_24h_pct": "-0.2",
            "oi_change_24h_pct": "40",
            "long_ratio": "0.55",
        },
        {
            "status": "entered",
            "symbol": "SIRENUSDT",
            "entry_ts": "1000",
            "trigger_ts": "400",
            "case_id": "high",
            "pump_pct": "180",
            "config_threshold_pct": "150",
            "config_window_h": "72",
            "age_days": "15",
            "funding_prev_24h_pct": "-1.1",
            "oi_change_24h_pct": "120",
            "long_ratio": "0.41",
        },
    ]

    cases = build_unique_cases(rows)

    assert len(cases) == 1
    assert cases[0]["case_id"] == "high"
    assert cases[0]["duplicate_trigger_count"] == 2
    assert cases[0]["pump_pct"] == 180.0
    assert cases[0]["funding_prev_24h_pct"] == -1.1
    assert cases[0]["long_ratio_min"] == 0.41


def test_gate_override_uses_alternative_rule_and_scales_pnl() -> None:
    gates = build_gates()
    policy = PolicySpec(
        slug="pump_ge_150__fast",
        description="If pump>=150 use fast",
        gate_slug="pump_ge_150",
        rule_slug="fast",
        mode="gate_override",
    )
    cases = [
        {
            "case_id": "c1",
            "symbol": "AAAUSDT",
            "entry_ts": 1000,
            "pump_pct": 200,
            "oi_change_24h_pct": 0,
            "long_ratio_min": 0.5,
            "long_ratio_max": 0.5,
            "funding_prev_24h_pct": 0,
        }
    ]
    outcomes = {
        ("c1", BASE_RULE_SLUG): {"exit_ts": 2000, "net_reserved_pct": "5", "max_margin_stress_reserved_pct": "0"},
        ("c1", "fast"): {"exit_ts": 2000, "net_reserved_pct": "20", "max_margin_stress_reserved_pct": "0"},
    }

    result = simulate_policy_portfolio(
        cases=cases,
        outcomes=outcomes,
        gates=gates,
        policy=policy,
        config=PortfolioConfig(capital_usd=1000.0, slots=1, sizing_mode="fixed_initial"),
        split_ts=1500,
        return_trades=True,
    )

    assert result["trades"] == 1
    assert result["final_capital_usd"] == 1600.0
    assert result["selected_trades"][0]["rule_slug"] == "fast"
    assert result["selected_trades"][0]["pnl_usd"] == 600.0


def test_build_policies_includes_static_skip_and_gate_override() -> None:
    policies = build_policies(["r1"], build_gates())
    slugs = {policy.slug for policy in policies}

    assert "base" in slugs
    assert "static__r1" in slugs
    assert "skip__pump_ge_150" in slugs
    assert "pump_ge_150__r1" in slugs
