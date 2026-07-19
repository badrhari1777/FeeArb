from __future__ import annotations

from analysis_features.pump_short_per_event_strategy_research import (
    BASE_RULE_SLUG,
    build_rule_configs,
    balanced_score,
)


def test_build_rule_configs_contains_base_and_large_grid() -> None:
    configs = build_rule_configs()
    slugs = {config.slug for config in configs}

    assert BASE_RULE_SLUG in slugs
    assert "step300_legs6_tapered_tp35_70_half_720" in slugs
    assert len(configs) == 1008


def test_balanced_score_penalizes_stress_and_hold() -> None:
    clean = {
        "net_reserved_pct": 10.0,
        "max_margin_stress_reserved_pct": 5.0,
        "max_adverse_from_first_pct": 20.0,
        "time_in_trade_h": 24,
        "max_legs": 4,
    }
    stressed = {
        "net_reserved_pct": 10.0,
        "max_margin_stress_reserved_pct": 100.0,
        "max_adverse_from_first_pct": 200.0,
        "time_in_trade_h": 720,
        "max_legs": 6,
    }

    assert balanced_score(clean) > balanced_score(stressed)
