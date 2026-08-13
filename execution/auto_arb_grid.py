from __future__ import annotations

import math
from typing import Any


MIN_LEVELS = 2
MAX_LEVELS = 20
COMPLETION_TOLERANCE_PCT = 1.0


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


def grid_symbol_ownership_key(rule: dict[str, Any] | Any) -> str:
    symbol = "".join(
        char for char in str(rule.get("symbol") or "").upper() if char.isalnum()
    )
    for settle in ("USDT", "USDC", "USD"):
        duplicated = settle + settle
        while symbol.endswith(duplicated):
            symbol = symbol[: -len(settle)]
    for quote in ("USDT", "USDC", "USD"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)]
    return symbol


def _grid_exchange_name(value: Any) -> str:
    name = str(value or "").lower()
    return "kucoin" if name == "kukoin" else name


def grid_rules_share_live_ownership(left: dict[str, Any] | Any, right: dict[str, Any] | Any) -> bool:
    left_symbol = grid_symbol_ownership_key(left)
    right_symbol = grid_symbol_ownership_key(right)
    if not left_symbol or left_symbol != right_symbol:
        return False
    left_venues = {
        _grid_exchange_name(left.get("long_exchange")),
        _grid_exchange_name(left.get("short_exchange")),
    }
    right_venues = {
        _grid_exchange_name(right.get("long_exchange")),
        _grid_exchange_name(right.get("short_exchange")),
    }
    left_venues.discard("")
    right_venues.discard("")
    return bool(left_venues.intersection(right_venues))


def grid_live_conflict(
    rules: Any,
    rule: Any,
    *,
    exclude_rule_id: str = "",
) -> Any | None:
    excluded = str(exclude_rule_id or rule.get("id") or "")
    candidates = rules.values() if hasattr(rules, "values") else []
    for candidate in candidates:
        if not hasattr(candidate, "get"):
            continue
        candidate_id = str(candidate.get("id") or "")
        if candidate_id and candidate_id == excluded:
            continue
        if not candidate.get("enabled") or candidate.get("mode") != "live":
            continue
        if grid_rules_share_live_ownership(rule, candidate):
            return candidate
    return None


def grid_live_conflict_message(conflict: Any) -> str:
    conflict_id = str(conflict.get("id") or "unknown")
    symbol = "".join(
        char for char in str(conflict.get("symbol") or "").upper() if char.isalnum()
    )
    for settle in ("USDT", "USDC", "USD"):
        duplicated = settle + settle
        while symbol.endswith(duplicated):
            symbol = symbol[: -len(settle)]
    return (
        f"Grid Live ownership conflict with rule {conflict_id}: {symbol} already "
        "has a Live Grid on one or both requested exchanges. Pause or delete that "
        "Grid before starting another one, including Adopt grid."
    )


def grid_completion_tolerance(
    rule: Any,
    target_qty: float | None = None,
    *,
    tolerance_pct: float = COMPLETION_TOLERANCE_PCT,
) -> float:
    qty = max(
        0.0,
        float(
            target_qty
            if target_qty is not None
            else rule.get("chunk_qty") or rule.get("max_qty") or 0.0
        ),
    )
    return max(1e-8, qty * tolerance_pct / 100.0)


def grid_transition_completion_tolerance(rule: Any, transition_qty: float | None = None) -> float:
    return max(
        grid_completion_tolerance(rule, transition_qty),
        grid_completion_tolerance(rule),
    )


def grid_hedge_imbalance_tolerance(
    rule: Any,
    *,
    transition_qty: float | None = None,
    hedged_qty: float | None = None,
) -> float:
    tolerance = grid_transition_completion_tolerance(rule, transition_qty)
    if str(rule.get("setup_mode") or "") == "adopt_existing_full_grid":
        try:
            current_qty = float(hedged_qty) if hedged_qty is not None else 0.0
        except (TypeError, ValueError):
            current_qty = 0.0
        if current_qty > 0:
            tolerance = max(tolerance, grid_completion_tolerance(rule, current_qty))
    return tolerance


def grid_non_closeable_dust(result: Any, remaining_qty: float) -> bool:
    if remaining_qty <= 0 or not hasattr(result, "get"):
        return False
    messages = [str(item) for item in (result.get("errors") or [])]
    messages.extend(str(item) for item in (result.get("warnings") or []))
    for action in result.get("actions") or []:
        if not hasattr(action, "get"):
            continue
        messages.extend(
            (
                str(action.get("error") or ""),
                str(action.get("error_type") or ""),
                str(action.get("market_reason") or ""),
            )
        )
    joined = " ".join(messages).lower()
    return any(
        token in joined
        for token in (
            "non-closeable dust",
            "below exchange minimum",
            "below min qty",
            "min_order_size",
        )
    )


def grid_dust_only_errors(result: Any) -> bool:
    if not hasattr(result, "get"):
        return False
    errors = [str(item).lower() for item in (result.get("errors") or [])]
    if not errors:
        return False
    dust_tokens = (
        "qty_below_step",
        "below min qty",
        "below exchange minimum",
        "min_order_size",
        "non-closeable dust",
    )
    return all(any(token in error for token in dust_tokens) for error in errors)


def grid_reset_after_flat_repair(rule: dict[str, Any], hedged_qty: float) -> bool:
    if max(0.0, float(hedged_qty or 0.0)) > grid_completion_tolerance(rule):
        return False
    rule["live_level"] = 0
    rule["pending_transition"] = None
    rule["pending_action"] = None
    rule["pending_samples"] = 0
    return True


def grid_level_for_qty(rule: Any, hedged_qty: float) -> int | None:
    qty = max(0.0, float(hedged_qty or 0.0))
    tolerance = grid_completion_tolerance(rule)
    if qty <= tolerance:
        return 0
    for level in rule.get("levels") or []:
        cumulative = float(level.get("cumulative_qty") or 0.0)
        if abs(qty - cumulative) <= tolerance:
            return int(level.get("level") or 0)
    return None


def grid_level_qty(rule: Any, level: int) -> float:
    if level <= 0:
        return 0.0
    levels = rule.get("levels") or []
    if level > len(levels):
        return 0.0
    try:
        return float((levels[level - 1] or {}).get("cumulative_qty") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def grid_partial_adoption_level_for_qty(rule: Any, hedged_qty: float) -> int | None:
    qty = max(0.0, float(hedged_qty or 0.0))
    tolerance = grid_completion_tolerance(rule)
    if qty <= tolerance:
        return 0
    levels = list(rule.get("levels") or [])
    if not levels:
        return None
    max_level = len(levels)
    max_qty = grid_level_qty(rule, max_level)
    if max_qty <= 0:
        max_qty = float(rule.get("max_qty") or 0.0)
    if max_qty <= 0 or qty > max_qty + tolerance:
        return None
    for level in levels:
        cumulative = float(level.get("cumulative_qty") or 0.0)
        if qty <= cumulative + tolerance:
            return int(level.get("level") or 0)
    return max_level


def grid_level_count_for_existing_qty(
    *,
    total_qty: float,
    existing_qty: float,
    preferred_count: int,
) -> dict[str, Any] | None:
    if total_qty <= 0 or existing_qty <= 0:
        return None
    preferred = max(MIN_LEVELS, min(MAX_LEVELS, int(preferred_count or MIN_LEVELS)))
    candidates: list[dict[str, Any]] = []
    for count in range(MIN_LEVELS, MAX_LEVELS + 1):
        chunk_qty = float(total_qty) / count
        if chunk_qty <= 0:
            continue
        level = max(0, min(count, int(round(float(existing_qty) / chunk_qty))))
        cumulative_qty = float(level) * chunk_qty
        diff_qty = abs(float(existing_qty) - cumulative_qty)
        tolerance_qty = max(1e-8, chunk_qty * COMPLETION_TOLERANCE_PCT / 100.0)
        matches = diff_qty <= tolerance_qty
        candidates.append(
            {
                "level_count": count,
                "level": level,
                "chunk_qty": chunk_qty,
                "cumulative_qty": cumulative_qty,
                "existing_qty": float(existing_qty),
                "diff_qty": diff_qty,
                "tolerance_qty": tolerance_qty,
                "matches": matches,
                "distance_from_preferred": abs(count - preferred),
                "normalized_diff": diff_qty / tolerance_qty if tolerance_qty else math.inf,
            }
        )
    if not candidates:
        return None
    matching = [item for item in candidates if item["matches"]]
    return min(
        matching or candidates,
        key=lambda item: (
            item["distance_from_preferred"],
            item["normalized_diff"],
            item["level_count"],
        ),
    )


def apply_grid_decision_confirmation(
    rule: dict[str, Any],
    *,
    decision: dict[str, Any],
    mode: str,
    current_level: int,
    pending_transition: dict[str, Any] | None,
    entry_spread_pct: float,
    exit_spread_pct: float,
    now_iso: str,
    now_ts: float,
) -> dict[str, Any]:
    """Pure state reducer after spread/partial-transition decision calculation."""
    action = str(decision.get("action") or "none")
    rule["last_decision"] = decision
    rule["blocked_reason"] = None
    live_transition: tuple[str, str, int, int] | None = None
    transition_event: dict[str, Any] | None = None
    entry_risk_cooldown = (
        action == "enter"
        and now_ts < float(rule.get("entry_next_eligible_ts") or 0.0)
    )
    if entry_risk_cooldown:
        rule["pending_action"] = None
        rule["pending_samples"] = 0
        rule["status"] = "blocked_risk_limit"
        rule["blocked_reason"] = str(
            rule.get("entry_blocked_reason")
            or "KuCoin entry risk-limit preflight is cooling down"
        )
    elif action == "none":
        rule["pending_action"] = None
        rule["pending_samples"] = 0
        rule["status"] = (
            f"partial_{pending_transition.get('action')}_waiting_trigger"
            if pending_transition
            else ("waiting_entry" if not current_level else "monitoring")
        )
    else:
        if rule.get("pending_action") == action:
            rule["pending_samples"] = int(rule.get("pending_samples") or 0) + 1
        else:
            rule["pending_action"] = action
            rule["pending_samples"] = 1
        rule["status"] = f"confirming_{action}"
        required = max(1, int(rule.get("confirm_samples") or 2))
        if int(rule["pending_samples"]) >= required:
            previous_level = (
                int(pending_transition.get("from_level") or current_level)
                if pending_transition
                else current_level
            )
            new_level = int(
                pending_transition.get("to_level")
                if pending_transition
                else decision["target_level"]
            )
            if mode == "live":
                rule["status"] = f"queued_{action}"
                live_transition = (str(rule.get("id") or ""), action, previous_level, new_level)
            else:
                rule["shadow_level"] = new_level
                levels = rule.get("levels") or []
                rule["shadow_qty"] = (
                    float(levels[new_level - 1].get("cumulative_qty") or 0.0)
                    if new_level > 0 and new_level <= len(levels)
                    else 0.0
                )
                rule["status"] = f"shadow_{action}"
                rule["pending_action"] = None
                rule["pending_samples"] = 0
                transition_event = {
                    "event": f"shadow_{action}",
                    "rule_id": rule.get("id"),
                    "generation": rule.get("generation"),
                    "symbol": rule.get("symbol"),
                    "long_exchange": rule.get("long_exchange"),
                    "short_exchange": rule.get("short_exchange"),
                    "from_level": previous_level,
                    "to_level": new_level,
                    "shadow_qty": rule.get("shadow_qty"),
                    "entry_spread_pct": entry_spread_pct,
                    "exit_spread_pct": exit_spread_pct,
                    "ts": now_iso,
                }
    return {
        "action": action,
        "live_transition": live_transition,
        "transition_event": transition_event,
    }


def complete_pending_grid_transition(
    rule: dict[str, Any],
    *,
    pending_transition: dict[str, Any],
    current_level: int,
    last_result: Any,
    now_iso: str,
    now_ts: float,
    retry_sec: float,
) -> dict[str, Any] | None:
    """Finalize a filled or non-closeable-dust partial transition, otherwise no-op."""
    remaining = max(0.0, float(pending_transition.get("remaining_qty") or 0.0))
    tolerance = grid_transition_completion_tolerance(
        rule,
        float(pending_transition.get("target_qty") or 0.0) or None,
    )
    filled = max(0.0, float(pending_transition.get("filled_qty") or 0.0))
    dust_terminal = filled > 0 and grid_non_closeable_dust(last_result, remaining)
    if remaining > tolerance and not dust_terminal:
        return None
    action = str(pending_transition.get("action") or "")
    from_level = int(pending_transition.get("from_level") or current_level)
    to_level = int(pending_transition.get("to_level") or current_level)
    rule["live_level"] = to_level
    rule["pending_transition"] = None
    rule["pending_action"] = None
    rule["pending_samples"] = 0
    rule["blocked_reason"] = None
    rule["next_eligible_ts"] = now_ts + retry_sec
    rule["status"] = "waiting_entry" if to_level == 0 else "monitoring"
    decision = {
        "action": "none",
        "current_level": to_level,
        "target_level": to_level,
        "entry_target_level": None,
        "exit_target_level": None,
        "levels_delta": to_level - from_level,
        "continuation": True,
        "remaining_qty": remaining,
        "dust_completed": remaining > 1e-9,
        "non_closeable_dust_completed": remaining > tolerance,
    }
    event = {
        "event": f"live_{action}",
        "rule_id": rule.get("id"),
        "generation": rule.get("generation"),
        "symbol": rule.get("symbol"),
        "long_exchange": rule.get("long_exchange"),
        "short_exchange": rule.get("short_exchange"),
        "from_level": from_level,
        "to_level": to_level,
        "live_level": to_level,
        "remaining_qty": remaining,
        "completion_tolerance_qty": tolerance,
        "dust_completed": remaining > 1e-9,
        "non_closeable_dust_completed": remaining > tolerance,
        "ts": now_iso,
    }
    return {
        "current_level": to_level,
        "decision": decision,
        "transition_event": event,
        "pending_transition": {},
    }
