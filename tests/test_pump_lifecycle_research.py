from __future__ import annotations

from analysis_features.pump_lifecycle_research import (
    classify_lifecycle_state,
    continuation_available_points,
    continuation_score,
    exhaustion_score,
    scale_available_score,
    z_score_current,
)


def test_z_score_current_detects_volume_spike() -> None:
    values = [100.0, 105.0, 98.0, 102.0, 101.0, 99.0, 103.0, 100.0, 500.0]

    score = z_score_current(values, len(values) - 1, 8)

    assert score is not None
    assert score > 20.0


def test_continuation_score_rewards_negative_funding_oi_volume_and_strength() -> None:
    features = {
        "funding_prev_8h_pct": -0.2,
        "funding_trend_8h_pct": -0.05,
        "oi_change_1h_pct": 18.0,
        "oi_change_6h_pct": 35.0,
        "volume_z_24h": 4.0,
        "btc_relative_1h_pct": 10.0,
        "btc_relative_6h_pct": 20.0,
        "return_1h_pct": 7.0,
        "return_6h_pct": 18.0,
    }

    assert continuation_score(features) >= 70.0


def test_exhaustion_score_rewards_pullback_positive_funding_and_failed_price() -> None:
    features = {
        "funding_prev_8h_pct": 0.05,
        "oi_change_6h_pct": 20.0,
        "return_1h_pct": -3.0,
        "return_6h_pct": -8.0,
        "pullback_from_high_pct": 25.0,
        "long_ratio": 0.66,
    }

    assert exhaustion_score(features) >= 70.0


def test_state_classification_keeps_strong_continuation_out_of_short_states() -> None:
    features = {"pullback_from_high_pct": 3.0, "return_1h_pct": 6.0}
    scores = {
        "security_dislocation_score": 0.0,
        "squeeze_continuation_score": 75.0,
        "pump_exhaustion_score": 20.0,
    }

    assert classify_lifecycle_state(features, scores) == "SHORT_SQUEEZE"


def test_state_classification_marks_breakdown_after_exhaustion() -> None:
    features = {"pullback_from_high_pct": 22.0, "return_1h_pct": -4.0}
    scores = {
        "security_dislocation_score": 0.0,
        "squeeze_continuation_score": 15.0,
        "pump_exhaustion_score": 75.0,
    }

    assert classify_lifecycle_state(features, scores) == "BREAKDOWN"


def test_available_score_scales_when_btc_context_is_missing() -> None:
    features = {
        "funding_prev_8h_pct": -0.2,
        "funding_trend_8h_pct": -0.05,
        "oi_change_1h_pct": 18.0,
        "oi_change_6h_pct": 35.0,
        "volume_z_24h": 4.0,
        "btc_relative_1h_pct": None,
        "btc_relative_6h_pct": None,
        "return_1h_pct": 7.0,
        "return_6h_pct": 18.0,
    }

    raw = continuation_score(features)
    available = continuation_available_points(features)

    assert available == 65.0
    assert scale_available_score(raw, available) >= 90.0
