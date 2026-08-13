from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


TERMINAL_STEP_STATUSES = {"completed", "completed_with_dust"}


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def current_unfinished_step(strategy: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return only the first unfinished step of a sequential execution plan."""

    for raw in strategy.get("steps") or []:
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


def reconcile_fixed_target_progress(
    step: Mapping[str, Any],
    *,
    observed_filled_qty: float,
    tolerance_pct: float = 1.0,
    min_executable_qty: float | None = None,
) -> dict[str, Any]:
    """Reconcile actual fills while preserving the original target quantity."""

    updated = dict(step)
    target_qty = max(0.0, _safe_float(step.get("target_qty")) or 0.0)
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


@dataclass(frozen=True, slots=True)
class ExecutionCandidate:
    owner_id: str
    step_id: str
    action: str
    priority: int
    edge: float
    waiting_since_ts: float


def choose_execution_candidate(
    candidates: Iterable[ExecutionCandidate],
) -> ExecutionCandidate | None:
    """Choose one action deterministically; priority remains caller-owned."""

    rows = list(candidates)
    if not rows:
        return None
    rows.sort(
        key=lambda item: (
            int(item.priority),
            -float(item.edge),
            float(item.waiting_since_ts),
            item.owner_id,
            item.step_id,
        )
    )
    return rows[0]


__all__ = [
    "ExecutionCandidate",
    "choose_execution_candidate",
    "completion_tolerance_qty",
    "current_unfinished_step",
    "reconcile_fixed_target_progress",
]
