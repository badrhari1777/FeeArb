from __future__ import annotations

from analysis_features.pump_short_per_event_strategy_research import BASE_RULE_SLUG
from analysis_features.pump_short_super_pump_tier_research import (
    PumpBucket,
    TieredPolicy,
    case_in_bucket,
    choose_tier_rule,
    make_policy,
)


def test_case_in_bucket_uses_half_open_upper_bound() -> None:
    bucket = PumpBucket("p100_150", 100.0, 150.0)

    assert case_in_bucket({"pump_pct": 100}, bucket)
    assert case_in_bucket({"pump_pct": 149.99}, bucket)
    assert not case_in_bucket({"pump_pct": 99.99}, bucket)
    assert not case_in_bucket({"pump_pct": 150}, bucket)


def test_choose_tier_rule_uses_highest_matching_threshold() -> None:
    policy = TieredPolicy(
        slug="test",
        description="",
        tiers=((100.0, "r100"), (150.0, "r150"), (250.0, "r250")),
    )

    assert choose_tier_rule({"pump_pct": 80}, policy) == BASE_RULE_SLUG
    assert choose_tier_rule({"pump_pct": 120}, policy) == "r100"
    assert choose_tier_rule({"pump_pct": 200}, policy) == "r150"
    assert choose_tier_rule({"pump_pct": 300}, policy) == "r250"


def test_make_policy_sorts_thresholds_for_stable_slug() -> None:
    policy = make_policy(((250.0, "r250"), (100.0, "r100")))

    assert policy.tiers == ((100.0, "r100"), (250.0, "r250"))
    assert policy.slug == "p100_r100__p250_r250"
