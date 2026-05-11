from __future__ import annotations

from typing import Any, Mapping

from .constants import POSITION_ACTIONS
from .reason_codes import normalize_reason_codes

_ACTION_RANK = {
    "HOLD": 0,
    "PARTIAL_EXIT": 1,
    "FULL_EXIT": 2,
    "ADD_BLOCKED": 1,
    "ADD_SMALL": 1,
}

_REASON_TEXT = {
    "data_quality_low": "Insufficient data quality for decisive position changes.",
    "position_thesis_intact": "Position thesis remains intact under current conditions.",
    "position_thesis_deteriorating": "Position thesis is deteriorating versus current regime.",
    "size_reduced_risk_control": "Risk control recommends reducing size.",
    "spread_continuation_risk_high": "Spread continuation risk is high for this position.",
    "funding_edge_negative": "Expected funding edge turned negative.",
    "decision_window_active": "Preferred decision window is active near funding boundary.",
    "outside_decision_window": "Outside preferred add window; postpone scaling.",
    "add_blocked_risk": "Add is blocked by elevated regime risk.",
}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_action(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in POSITION_ACTIONS:
        return text
    return "HOLD"


def _extract_direction_feature(
    pair_row: Mapping[str, Any],
    *,
    direction: str,
) -> Mapping[str, Any] | None:
    for item in list(pair_row.get("directional_features") or []):
        if str(item.get("direction") or "") == direction:
            return item
    return None


def _reason_text(reason_codes: list[str]) -> list[str]:
    out: list[str] = []
    for code in reason_codes:
        message = _REASON_TEXT.get(code)
        if message:
            out.append(message)
    return out


def evaluate_position_signal(
    *,
    position_key: str,
    pair_key: str,
    direction: str,
    qty: float,
    decision_phase: str,
    spread_coverage_pct: float,
    direction_feature: Mapping[str, Any] | None,
) -> dict[str, Any]:
    row = direction_feature or {}
    scores = row.get("scores") or {}
    directional = row.get("directional") or {}

    entry_score = _safe_float(scores.get("entry_score")) or 0.0
    continuation_risk = _safe_float(scores.get("continuation_risk_score")) or 50.0
    funding_to_next = _safe_float(directional.get("funding_to_next_pct"))
    reversion_potential = _safe_float(directional.get("reversion_potential_pct"))

    reasons: list[str] = []
    action = "HOLD"

    if spread_coverage_pct < 50.0:
        action = "HOLD"
        reasons.extend(["data_quality_low", "outside_decision_window"])
    elif continuation_risk >= 80.0:
        action = "FULL_EXIT"
        reasons.extend(["position_thesis_deteriorating", "spread_continuation_risk_high"])
    elif continuation_risk >= 65.0 or (funding_to_next is not None and funding_to_next < 0):
        action = "PARTIAL_EXIT"
        reasons.extend(["position_thesis_deteriorating", "size_reduced_risk_control"])
        if funding_to_next is not None and funding_to_next < 0:
            reasons.append("funding_edge_negative")
    elif entry_score >= 72.0 and continuation_risk <= 40.0:
        if decision_phase in {"pre_boundary_20m", "pre_boundary_15m"}:
            action = "ADD_SMALL"
            reasons.extend(["position_thesis_intact", "decision_window_active"])
        elif decision_phase in {"boundary_immediate", "exploratory"}:
            action = "ADD_BLOCKED"
            reasons.extend(["outside_decision_window", "add_blocked_risk"])
        else:
            action = "HOLD"
            reasons.append("position_thesis_intact")
    else:
        action = "HOLD"
        reasons.append("position_thesis_intact")

    if reversion_potential is not None and reversion_potential < -0.25:
        if action in {"HOLD", "ADD_SMALL"}:
            action = "PARTIAL_EXIT"
            reasons.extend(["position_thesis_deteriorating", "size_reduced_risk_control"])

    normalized_action = _normalize_action(action)
    reason_codes = sorted(set(normalize_reason_codes(reasons)))
    reason_text = _reason_text(reason_codes)
    confidence = max(0.0, min(100.0, 100.0 - continuation_risk if normalized_action in {"HOLD", "ADD_SMALL"} else continuation_risk))
    return {
        "position_key": position_key,
        "pair_key": pair_key,
        "direction": direction,
        "qty": float(max(0.0, qty)),
        "action": normalized_action,
        "decision_phase": decision_phase,
        "confidence_score": round(confidence, 2),
        "reason_codes": reason_codes,
        "reason_text": reason_text or ["Position rule engine completed."],
        "scores": {
            "entry_score": round(entry_score, 2),
            "continuation_risk_score": round(continuation_risk, 2),
            "reversion_score": round(max(0.0, min(100.0, 100.0 - continuation_risk)), 2),
            "spread_coverage_pct": round(spread_coverage_pct, 2),
            "funding_to_next_pct": funding_to_next,
            "reversion_potential_pct": reversion_potential,
            "action_rank": _ACTION_RANK.get(normalized_action, 0),
        },
    }


__all__ = [
    "POSITION_ACTIONS",
    "evaluate_position_signal",
]
