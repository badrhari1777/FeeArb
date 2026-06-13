from __future__ import annotations

import math
from typing import Any


MIN_LEVELS = 2
MAX_LEVELS = 20


def _positive_float(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a number.") from exc
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{field} must be greater than zero.")
    return result


def normalize_level_count(value: Any) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("level_count must be an integer.") from exc
    if result < MIN_LEVELS or result > MAX_LEVELS:
        raise ValueError(f"level_count must be between {MIN_LEVELS} and {MAX_LEVELS}.")
    return result


def build_grid_levels(
    *,
    range_start_pct: Any,
    range_end_pct: Any,
    level_count: Any,
    exit_gap_pct: Any,
    max_qty: Any | None = None,
    chunk_qty: Any | None = None,
) -> list[dict[str, float | int]]:
    start = float(range_start_pct)
    end = float(range_end_pct)
    count = normalize_level_count(level_count)
    gap = _positive_float(exit_gap_pct, "exit_gap_pct")
    if not math.isfinite(start) or not math.isfinite(end):
        raise ValueError("Spread range must contain finite numbers.")
    if end >= start:
        raise ValueError(
            "range_end_pct must be lower than range_start_pct "
            "for the negative-spread grid direction."
        )

    resolved_chunk = None
    resolved_max = None
    if chunk_qty is not None:
        resolved_chunk = _positive_float(chunk_qty, "chunk_qty")
        resolved_max = resolved_chunk * count
    elif max_qty is not None:
        resolved_max = _positive_float(max_qty, "max_qty")
        resolved_chunk = resolved_max / count

    step = (start - end) / (count - 1)
    levels: list[dict[str, float | int]] = []
    cumulative_qty = 0.0
    for index in range(count):
        entry = start - (step * index)
        qty = resolved_chunk
        if resolved_max is not None and resolved_chunk is not None:
            if index == count - 1:
                qty = max(0.0, resolved_max - cumulative_qty)
            cumulative_qty += float(qty)
        levels.append(
            {
                "level": index + 1,
                "entry_spread_pct": round(entry, 10),
                "exit_spread_pct": round(entry + gap, 10),
                "qty": round(float(qty), 12) if qty is not None else 0.0,
                "cumulative_qty": round(cumulative_qty, 12),
            }
        )
    return levels


def entry_target_level(entry_spread_pct: Any, levels: list[dict[str, Any]]) -> int:
    try:
        spread = float(entry_spread_pct)
    except (TypeError, ValueError):
        return 0
    return sum(
        1
        for level in levels
        if spread <= float(level["entry_spread_pct"])
    )


def exit_target_level(
    exit_spread_pct: Any,
    levels: list[dict[str, Any]],
    current_level: Any,
) -> int:
    try:
        spread = float(exit_spread_pct)
        current = max(0, min(int(current_level), len(levels)))
    except (TypeError, ValueError):
        return 0
    kept = sum(
        1
        for level in levels[:current]
        if spread < float(level["exit_spread_pct"])
    )
    return min(current, kept)


def decide_grid_transition(
    *,
    entry_spread_pct: Any,
    exit_spread_pct: Any,
    levels: list[dict[str, Any]],
    current_level: Any,
    max_levels_per_cycle: Any = 1,
) -> dict[str, Any]:
    current = max(0, min(int(current_level or 0), len(levels)))
    max_step = max(1, int(max_levels_per_cycle or 1))
    entry_target = entry_target_level(entry_spread_pct, levels)
    exit_target = exit_target_level(exit_spread_pct, levels, current)

    action = "none"
    target = current
    if entry_target > current:
        action = "enter"
        target = min(entry_target, current + max_step)
    elif exit_target < current:
        action = "exit"
        target = max(exit_target, current - max_step)

    return {
        "action": action,
        "current_level": current,
        "target_level": target,
        "entry_target_level": entry_target,
        "exit_target_level": exit_target,
        "levels_delta": target - current,
    }


def recommend_level_count(
    *,
    total_qty: Any,
    safe_chunk_qty: Any,
    minimum: int = MIN_LEVELS,
    maximum: int = MAX_LEVELS,
) -> int:
    total = _positive_float(total_qty, "total_qty")
    chunk = _positive_float(safe_chunk_qty, "safe_chunk_qty")
    return max(minimum, min(maximum, int(math.ceil(total / chunk))))

