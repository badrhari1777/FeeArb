from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


TERMINAL_STEP_STATUSES = {"completed", "completed_with_dust"}
ACTIVE_STEP_STATUSES = {
    "waiting",
    "queued",
    "executing",
    "reconciling",
    "partial",
    "blocked_balance",
    "blocked_minimum",
    "blocked_conflict",
    "error",
}


def safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


def current_step(strategy: Mapping[str, Any]) -> dict[str, Any] | None:
    steps = strategy.get("steps") or []
    for raw in steps:
        if not isinstance(raw, Mapping):
            continue
        if str(raw.get("status") or "waiting") not in TERMINAL_STEP_STATUSES:
            return dict(raw)
    return None


def completion_tolerance_qty(
    target_qty: float,
    *,
    tolerance_pct: float = 1.0,
    min_executable_qty: float | None = None,
) -> float:
    percent_tolerance = max(0.0, float(target_qty)) * max(0.0, float(tolerance_pct)) / 100.0
    exchange_tolerance = max(0.0, float(min_executable_qty or 0.0))
    return max(1e-9, percent_tolerance, exchange_tolerance)


def reconcile_step_progress(
    step: Mapping[str, Any],
    *,
    observed_filled_qty: float,
    tolerance_pct: float = 1.0,
    min_executable_qty: float | None = None,
) -> dict[str, Any]:
    updated = dict(step)
    target_qty = max(0.0, safe_float(step.get("target_qty")) or 0.0)
    filled_qty = min(target_qty, max(0.0, float(observed_filled_qty)))
    remaining_qty = max(0.0, target_qty - filled_qty)
    tolerance_qty = completion_tolerance_qty(
        target_qty,
        tolerance_pct=tolerance_pct,
        min_executable_qty=min_executable_qty,
    )
    updated["filled_qty"] = filled_qty
    updated["remaining_qty"] = remaining_qty
    updated["completion_tolerance_qty"] = tolerance_qty
    if target_qty > 0 and remaining_qty <= tolerance_qty:
        updated["status"] = "completed" if remaining_qty <= 1e-9 else "completed_with_dust"
    elif filled_qty > 0:
        updated["status"] = "partial"
    else:
        updated["status"] = "waiting"
    return updated


def trigger_matches(
    *,
    action: str,
    spread_pct: float | None,
    spread_target_pct: float | None,
    funding_delta_pct: float | None,
    funding_min_pct: float | None,
) -> tuple[bool, str]:
    if spread_pct is None:
        return False, "spread_unavailable"
    if spread_target_pct is None:
        return False, "spread_target_missing"
    normalized_action = str(action or "").lower()
    if normalized_action == "enter":
        spread_ok = float(spread_pct) <= float(spread_target_pct)
        spread_reason = "entry_spread_above_target"
    else:
        spread_ok = float(spread_pct) >= float(spread_target_pct)
        spread_reason = "exit_spread_below_target"
    if not spread_ok:
        return False, spread_reason
    if funding_min_pct is not None:
        if funding_delta_pct is None:
            return False, "funding_unavailable"
        if float(funding_delta_pct) < float(funding_min_pct):
            return False, "funding_below_target"
    return True, "trigger_matched"


def trigger_edge(
    *,
    action: str,
    spread_pct: float,
    spread_target_pct: float,
) -> float:
    if str(action or "").lower() == "enter":
        return float(spread_target_pct) - float(spread_pct)
    return float(spread_pct) - float(spread_target_pct)


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    strategy_id: str
    step_id: str
    action: str
    priority: int
    edge: float
    waiting_since_ts: float


def choose_candidate(candidates: Iterable[StrategyCandidate]) -> StrategyCandidate | None:
    rows = list(candidates)
    if not rows:
        return None
    rows.sort(
        key=lambda item: (
            int(item.priority),
            -float(item.edge),
            float(item.waiting_since_ts),
            item.strategy_id,
            item.step_id,
        )
    )
    return rows[0]


def action_priority(action: str, strategy_type: str) -> int:
    normalized_action = str(action or "").lower()
    normalized_type = str(strategy_type or "").lower()
    if normalized_action == "exit":
        if normalized_type == "v1":
            return 10
        if normalized_type == "spread":
            return 20
        if normalized_type == "exit_ladder":
            return 30
        if normalized_type == "grid":
            return 40
        return 50
    if normalized_type == "grid":
        return 70
    return 60
