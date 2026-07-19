from __future__ import annotations

from analysis_features.pump_short_pullback_tier_research import (
    EntryRule,
    BASE_RULE_SLUG,
    bucket_for_pump,
    choose_entry_rule,
    entry_setup,
    make_policy,
)


def test_bucket_for_pump_uses_expected_ranges() -> None:
    assert bucket_for_pump(79.9).slug == "p000_080"
    assert bucket_for_pump(80.0).slug == "p080_100"
    assert bucket_for_pump(100.0).slug == "p100_150"
    assert bucket_for_pump(250.0).slug == "p250_plus"


def test_entry_setup_is_confirmed_pullback_with_oi_filter() -> None:
    setup = entry_setup(25.0)

    assert setup["name"] == "pb25_oi50_lr_mid"
    assert setup["kind"] == "confirmed_pullback"
    assert setup["pullback_pct"] == 25.0
    assert setup["oi_max_pct"] == 50.0


def test_choose_entry_rule_uses_highest_matching_tier() -> None:
    policy = make_policy(
        (
            (0.0, EntryRule(20.0, BASE_RULE_SLUG)),
            (100.0, EntryRule(25.0, "r100")),
            (250.0, EntryRule(30.0, "r250")),
        )
    )

    assert choose_entry_rule({"pump_pct": 50}, policy).rule_slug == BASE_RULE_SLUG
    assert choose_entry_rule({"pump_pct": 150}, policy) == EntryRule(25.0, "r100")
    assert choose_entry_rule({"pump_pct": 300}, policy) == EntryRule(30.0, "r250")
