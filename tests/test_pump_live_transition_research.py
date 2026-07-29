from __future__ import annotations

from analysis_features.pump_live_transition_research import (
    STRATEGIES,
    StrategySpec,
    Tier,
    passes_online_gates,
    select_tier,
    simulate_portfolio,
)
from webapp.bybit_pump_short_lab import PUMP_STRATEGY_CATALOG


def test_strategy_catalog_matches_current_online_ids() -> None:
    assert [item.strategy_id for item in STRATEGIES] == [
        "main_pullback_tier",
        "conservative_control",
        "super_pump_shadow",
        "pb20_baseline",
        "pb25_deeper_pullback",
        "short_clean_p100_l3_shadow",
        "short_super_250_shadow",
    ]
    main = STRATEGIES[0]
    assert select_tier(main, 79.9).rule_slug == "step50_legs5_equal_tp25_720"
    assert select_tier(main, 100.0).rule_slug == "step50_legs3_tapered_tp25_336"
    assert select_tier(main, 250.0).rule_slug == "step50_legs2_tapered_tp25_720"

    online = {str(item["strategy_id"]): item for item in PUMP_STRATEGY_CATALOG}
    for spec in STRATEGIES[:5]:
        current = online[spec.strategy_id]
        assert spec.funding_min_pct == current["funding_min_pct"]
        assert spec.oi_max_pct == current["oi_max_pct"]
        assert spec.long_ratio_min == current["long_ratio_min"]
        assert spec.long_ratio_max == current["long_ratio_max"]
        assert [
            (tier.min_pump_pct, tier.rule_slug)
            for tier in spec.tiers
        ] == [
            (float(tier["min_pump_pct"]), str(tier["rule_slug"]))
            for tier in current["tiers"]
        ]


def test_online_gate_semantics_allow_missing_funding_but_not_oi_or_ratio() -> None:
    spec = StrategySpec(
        "x",
        -1.0,
        50.0,
        0.45,
        0.65,
        (Tier(0, 20, "rule"),),
    )
    assert passes_online_gates(spec, {"oi_change_24h_pct": 10, "long_ratio": 0.5}) == (True, "ready")
    assert passes_online_gates(spec, {"funding_prev_24h_pct": -1.1, "oi_change_24h_pct": 10, "long_ratio": 0.5}) == (
        False,
        "funding",
    )
    assert passes_online_gates(spec, {"long_ratio": 0.5}) == (False, "missing_oi")
    assert passes_online_gates(spec, {"oi_change_24h_pct": 10}) == (False, "missing_long_ratio")


def test_reserve_covers_concurrent_rescue_and_stair_locks_profit() -> None:
    candidates = [
        {
            "symbol": "AAAUSDT",
            "entry_ts": 1,
            "entry_iso": "a",
            "exit_ts": 10,
            "exit_iso": "b",
            "net_pct": 10.0,
            "stress_pct": 150.0,
            "split": "train",
        },
        {
            "symbol": "BBBUSDT",
            "entry_ts": 2,
            "entry_iso": "a",
            "exit_ts": 11,
            "exit_iso": "b",
            "net_pct": 10.0,
            "stress_pct": 150.0,
            "split": "test",
        },
    ]
    result = simulate_portfolio(
        candidates,
        strategy_id="x",
        total_capital_usd=5_000,
        reserve_usd=1_500,
        slots=4,
        sizing_mode="reserve_stair_25_cap2",
        split_ts=2,
    )
    summary = result["summary"]
    assert summary["trades"] == 2
    assert summary["max_concurrent_rescue_required_usd"] == 875.0
    assert summary["max_concurrent_manual_topup_usd"] == 0.0
    assert summary["final_reserve_usd"] > 1_500
    assert summary["final_equity_usd"] > 5_000


def test_insufficient_reserve_is_reported_as_manual_topup() -> None:
    result = simulate_portfolio(
        [
            {
                "symbol": "AAAUSDT",
                "entry_ts": 1,
                "entry_iso": "a",
                "exit_ts": 2,
                "exit_iso": "b",
                "net_pct": -5.0,
                "stress_pct": 300.0,
            }
        ],
        strategy_id="x",
        total_capital_usd=3_000,
        reserve_usd=500,
        slots=4,
        sizing_mode="fixed",
        split_ts=2,
    )
    assert result["summary"]["reserve_breach_events"] == 1
    assert result["summary"]["max_concurrent_manual_topup_usd"] == 750.0
