from __future__ import annotations

from typing import Any, Mapping

from .constants import CANDIDATE_ACTIONS
from .reason_codes import normalize_reason_codes

_ACTION_RANK = {
    "NO_TRADE": 0,
    "ENTRY_SMALL": 1,
    "ENTRY_STRONG": 2,
}

_REASON_TEXT = {
    "data_quality_low": "Data quality is insufficient for actionable entry sizing.",
    "spread_history_low_coverage": "Spread history coverage is below the minimum threshold.",
    "funding_interval_mismatch": "Funding intervals are mismatched between exchanges.",
    "funding_edge_negative": "Expected net funding is negative for this direction.",
    "funding_edge_weak": "Funding edge is weak or unavailable.",
    "spread_not_attractive": "Entry spread is not attractive for reversion.",
    "spread_reversion_favorable": "Spread location favors potential reversion.",
    "spread_continuation_risk_high": "Spread continuation risk is elevated.",
    "premium_stress": "Premium differential indicates stress.",
    "oi_divergence_high": "Open interest divergence is high.",
    "outside_decision_window": "Decision is outside the preferred pre-boundary window.",
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
    if text in CANDIDATE_ACTIONS:
        return text
    return "NO_TRADE"


def action_from_entry_score(score: float | None) -> str:
    value = _safe_float(score)
    if value is None:
        return "NO_TRADE"
    if value >= 70.0:
        return "ENTRY_STRONG"
    if value >= 50.0:
        return "ENTRY_SMALL"
    return "NO_TRADE"


def _extract_score(pair: Mapping[str, Any]) -> float:
    direct = _safe_float(pair.get("score"))
    if direct is not None:
        return float(direct)
    selected_direction = str(pair.get("selected_direction") or "")
    for row in list(pair.get("directional_features") or []):
        if str(row.get("direction") or "") != selected_direction:
            continue
        score = _safe_float((row.get("scores") or {}).get("entry_score"))
        if score is not None:
            return float(score)
    return 0.0


def _reason_text(reason_codes: list[str]) -> list[str]:
    out: list[str] = []
    for code in reason_codes:
        message = _REASON_TEXT.get(code)
        if message:
            out.append(message)
    return out


def evaluate_candidate_pairs(pairs: list[Mapping[str, Any]]) -> dict[str, Any]:
    if not pairs:
        return {
            "decision": "reject",
            "recommended_action": "NO_TRADE",
            "score": 0.0,
            "reason": "no_pairs_available",
            "reason_codes": ["data_quality_low"],
            "reason_text": ["No exchange pair data available for analysis."],
            "recommended_pair": None,
            "pair_reasons": ["data_quality_low"],
            "scores": {"best_pair_score": 0.0},
            "decision_phase": "exploratory",
            "top_candidates": [],
            "note": "decision is advisory; execute only after manual dry-run checks",
        }

    candidates: list[dict[str, Any]] = []
    for row in pairs:
        action = _normalize_action(row.get("selected_action"))
        score = max(0.0, min(100.0, _extract_score(row)))
        pair_reasons = normalize_reason_codes(list(row.get("reasons") or []))
        funding_meta = row.get("funding_interval_hours") or {}
        interval_match = bool(funding_meta.get("match"))
        coverage_pct = _safe_float((row.get("spread") or {}).get("coverage_pct")) or 0.0

        gated_action = action
        if not interval_match:
            pair_reasons.append("funding_interval_mismatch")
            gated_action = "NO_TRADE"
        if coverage_pct < 50.0:
            pair_reasons.append("spread_history_low_coverage")
            pair_reasons.append("data_quality_low")
            gated_action = "NO_TRADE"
        elif coverage_pct < 70.0 and gated_action == "ENTRY_STRONG":
            pair_reasons.append("spread_history_low_coverage")
            gated_action = "ENTRY_SMALL"

        candidate = {
            "pair_key": row.get("pair_key"),
            "left_exchange": row.get("left_exchange"),
            "right_exchange": row.get("right_exchange"),
            "direction": row.get("selected_direction") or "long_a_short_b",
            "action": gated_action,
            "score": round(score, 2),
            "coverage_pct": round(coverage_pct, 2),
            "decision_phase": row.get("decision_phase") or "exploratory",
            "reason_codes": sorted(set(normalize_reason_codes(pair_reasons))),
            "feature_snapshot_id": (row.get("feature_snapshot_ids") or {}).get(row.get("selected_direction")),
        }
        candidates.append(candidate)

    best = max(
        candidates,
        key=lambda item: (
            _ACTION_RANK.get(str(item.get("action")), 0),
            float(item.get("score") or 0.0),
        ),
    )
    best_action = str(best.get("action") or "NO_TRADE")
    best_score = float(best.get("score") or 0.0)

    decision = "reject"
    if best_action == "ENTRY_STRONG" and best_score >= 70.0:
        decision = "enter_candidate"
    elif best_action == "ENTRY_SMALL" and best_score >= 50.0:
        decision = "watch"

    reason_codes = list(best.get("reason_codes") or [])
    reason_text = _reason_text(reason_codes)
    if not reason_text:
        reason_text = ["Candidate rule engine completed with no explicit blockers."]
    return {
        "decision": decision,
        "recommended_action": best_action,
        "score": round(best_score, 2),
        "reason": "best_pair_score",
        "reason_codes": reason_codes,
        "reason_text": reason_text,
        "recommended_pair": {
            "pair_key": best.get("pair_key"),
            "left_exchange": best.get("left_exchange"),
            "right_exchange": best.get("right_exchange"),
            "direction": best.get("direction"),
            "action": best_action,
            "decision_phase": best.get("decision_phase"),
            "coverage_pct": best.get("coverage_pct"),
            "feature_snapshot_id": best.get("feature_snapshot_id"),
        },
        "pair_reasons": reason_codes,
        "scores": {
            "best_pair_score": round(best_score, 2),
            "action_rank": _ACTION_RANK.get(best_action, 0),
        },
        "decision_phase": best.get("decision_phase") or "exploratory",
        "top_candidates": candidates[:5],
        "note": "decision is advisory; execute only after manual dry-run checks",
    }


__all__ = [
    "CANDIDATE_ACTIONS",
    "action_from_entry_score",
    "evaluate_candidate_pairs",
]
