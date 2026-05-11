from __future__ import annotations

REASON_CODES: frozenset[str] = frozenset(
    {
        "data_quality_low",
        "funding_interval_mismatch",
        "spread_history_low_coverage",
        "stale_quotes",
        "liquidity_insufficient",
        "funding_edge_negative",
        "funding_edge_weak",
        "funding_flip_risk",
        "spread_not_attractive",
        "spread_reversion_favorable",
        "spread_continuation_risk_high",
        "premium_stress",
        "oi_divergence_high",
        "decision_window_active",
        "outside_decision_window",
        "position_thesis_intact",
        "position_thesis_deteriorating",
        "size_reduced_risk_control",
        "add_blocked_risk",
    }
)


def is_known_reason_code(code: str) -> bool:
    return code in REASON_CODES


def normalize_reason_codes(codes: list[str] | tuple[str, ...]) -> list[str]:
    out: list[str] = []
    for code in codes:
        if not code:
            continue
        normalized = str(code).strip()
        if not normalized:
            continue
        out.append(normalized)
    return out
