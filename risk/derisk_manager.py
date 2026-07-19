from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping

from execution.accounts import normalize_symbol, _safe_float
from exchanges import normalize_exchange_name


def _parse_ts(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def issue_kind(message: Any) -> str | None:
    text = str(message or "").strip()
    if not text:
        return None
    lower = text.lower()
    if (
        "invalid api-key" in lower
        or "permissions for action" in lower
        or "authenticationerror" in lower
        or "retcode\":33004" in lower
        or "api key has expired" in lower
        or "api key info invalid" in lower
        or "\"code\":10072" in lower
    ):
        return "auth_error"
    if "rate limit" in lower or "too many requests" in lower:
        return "rate_limit"
    if "timeout" in lower:
        return "timeout"
    if (
        "exchangenotavailable" in lower
        or "dns" in lower
        or "bad gateway" in lower
        or "network" in lower
        or "getaddrinfo failed" in lower
    ):
        return "network_error"
    return "unknown_error"


def hedged_pair_key(symbol: str, long_exchange: str, short_exchange: str) -> str:
    return (
        f"{normalize_symbol(symbol)}|"
        f"{normalize_exchange_name(long_exchange)}|"
        f"{normalize_exchange_name(short_exchange)}|hedged_pair"
    )


def standalone_key(symbol: str, exchange: str, side: str | None = None) -> str:
    side_part = str(side or "").strip().lower() or "any"
    return (
        f"{normalize_symbol(symbol)}|"
        f"{normalize_exchange_name(exchange)}|"
        f"{side_part}|standalone"
    )


def normalize_hedge_cluster_config(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    rules: dict[str, Any] = {}
    incoming_rules = (payload or {}).get("rules") if isinstance(payload, Mapping) else None
    if not isinstance(incoming_rules, Mapping):
        return {"rules": rules}
    for _key, raw_rule in incoming_rules.items():
        if not isinstance(raw_rule, Mapping):
            continue
        kind = str(raw_rule.get("kind") or raw_rule.get("strategy_type") or "hedged_pair").strip().lower()
        symbol = normalize_symbol(raw_rule.get("symbol"))
        if not symbol:
            continue
        qty_tolerance_pct = _safe_float(raw_rule.get("qty_tolerance_pct"))
        if qty_tolerance_pct is None or qty_tolerance_pct < 0:
            qty_tolerance_pct = 0.1
        if kind == "standalone":
            exchange = normalize_exchange_name(str(raw_rule.get("exchange") or ""))
            side = str(raw_rule.get("side") or "").strip().lower() or None
            if not exchange:
                continue
            key = standalone_key(symbol, exchange, side)
            rules[key] = {
                "kind": "standalone",
                "symbol": symbol,
                "exchange": exchange,
                "side": side,
                "enabled": bool(raw_rule.get("enabled", True)),
                "source": str(raw_rule.get("source") or "manual"),
                "updated_at": raw_rule.get("updated_at"),
            }
            continue
        long_exchange = normalize_exchange_name(str(raw_rule.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(raw_rule.get("short_exchange") or ""))
        if not long_exchange or not short_exchange or long_exchange == short_exchange:
            continue
        key = hedged_pair_key(symbol, long_exchange, short_exchange)
        rules[key] = {
            "kind": "hedged_pair",
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "enabled": bool(raw_rule.get("enabled", True)),
            "qty_tolerance_pct": float(qty_tolerance_pct),
            "rehedge_allowed": bool(raw_rule.get("rehedge_allowed", False)),
            "source": str(raw_rule.get("source") or "manual"),
            "updated_at": raw_rule.get("updated_at"),
        }
    return {"rules": rules}


def derive_cluster_rules(
    explicit_rules: Mapping[str, Any] | None,
    auto_exit_rules: Mapping[str, Any] | None,
    *,
    active_position_legs: set[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    result = dict((explicit_rules or {}).get("rules") or {})
    legs_by_symbol: dict[str, dict[str, set[str]]] = {}
    for symbol, exchange, side in active_position_legs or set():
        normalized_symbol = normalize_symbol(symbol)
        normalized_exchange = normalize_exchange_name(exchange)
        normalized_side = str(side or "").strip().lower()
        if not normalized_symbol or not normalized_exchange or normalized_side not in {"long", "short"}:
            continue
        legs_by_symbol.setdefault(normalized_symbol, {"long": set(), "short": set()})[
            normalized_side
        ].add(normalized_exchange)

    def _add_position_pair(
        *,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        source: str,
        owner_type: str,
        owner_key: str | None = None,
        owner_generation: int = 0,
        position_signature: Mapping[str, Any] | None = None,
        signature_status: Any = None,
        updated_at: Any = None,
    ) -> None:
        key = hedged_pair_key(symbol, long_exchange, short_exchange)
        if key in result:
            return
        result[key] = {
            "kind": "hedged_pair",
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "enabled": True,
            "qty_tolerance_pct": 0.1,
            "rehedge_allowed": False,
            "source": source,
            "updated_at": updated_at,
            "owner_type": owner_type,
            "owner_key": owner_key,
            "owner_generation": int(owner_generation or 0),
            "position_signature": dict(position_signature or {}),
            "signature_status": signature_status,
        }

    for _key, raw_rule in dict(auto_exit_rules or {}).items():
        if not isinstance(raw_rule, Mapping):
            continue
        if not bool(raw_rule.get("enabled")) and not bool(raw_rule.get("v1_enabled")):
            continue
        position_signature = raw_rule.get("position_signature")
        if not isinstance(position_signature, Mapping):
            continue
        signature_status = str(raw_rule.get("signature_status") or "").strip().lower()
        if signature_status in {"binding_position_missing", "one_shot_completed"}:
            continue
        symbol = normalize_symbol(raw_rule.get("symbol"))
        long_exchange = normalize_exchange_name(str(raw_rule.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(raw_rule.get("short_exchange") or ""))
        if not symbol or not long_exchange or not short_exchange:
            continue
        if long_exchange == "multileg" or short_exchange == "multileg":
            visible = legs_by_symbol.get(symbol) or {}
            visible_longs = sorted(visible.get("long") or [])
            visible_shorts = sorted(visible.get("short") or [])
            if len(visible_longs) == 1 and len(visible_shorts) == 1:
                _add_position_pair(
                    symbol=symbol,
                    long_exchange=visible_longs[0],
                    short_exchange=visible_shorts[0],
                    source="auto_exit_multileg_live",
                    owner_type="auto_exit",
                    owner_key=str(_key),
                    owner_generation=int(raw_rule.get("rule_generation") or 0),
                    position_signature=position_signature,
                    signature_status=raw_rule.get("signature_status"),
                    updated_at=raw_rule.get("updated_at"),
                )
            continue
        if active_position_legs is not None and not (
            (symbol, long_exchange, "long") in active_position_legs
            or (symbol, short_exchange, "short") in active_position_legs
        ):
            continue
        _add_position_pair(
            symbol=symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            source="auto_exit",
            owner_type="auto_exit",
            owner_key=str(_key),
            owner_generation=int(raw_rule.get("rule_generation") or 0),
            position_signature=position_signature,
            signature_status=raw_rule.get("signature_status"),
            updated_at=raw_rule.get("updated_at"),
        )

    for symbol, visible in legs_by_symbol.items():
        visible_longs = sorted(visible.get("long") or [])
        visible_shorts = sorted(visible.get("short") or [])
        if len(visible_longs) != 1 or len(visible_shorts) != 1:
            continue
        _add_position_pair(
            symbol=symbol,
            long_exchange=visible_longs[0],
            short_exchange=visible_shorts[0],
            source="live_positions",
            owner_type="positions",
        )
    return {"rules": result}


def build_exchange_health(
    status_entries: list[Mapping[str, Any]],
    previous: Mapping[str, Any] | None = None,
    *,
    now_ts: float | None = None,
    stale_after_sec: int = 180,
    failure_block_count: int = 2,
) -> dict[str, Any]:
    previous = previous or {}
    now_ts = float(now_ts or datetime.now(timezone.utc).timestamp())
    health_map: dict[str, Any] = {str(key): dict(value or {}) for key, value in previous.items()}
    for entry in status_entries or []:
        exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
        if not exchange:
            continue
        current = dict(health_map.get(exchange) or {})
        checked_at = _parse_ts(entry.get("checked_at")) or now_ts
        status = str(entry.get("status") or "").strip().lower()
        message = entry.get("error") or entry.get("message") or entry.get("positions_error")
        if status == "ok":
            current["last_success_ts"] = checked_at
            current["consecutive_failures"] = 0
            current["last_error_kind"] = None
            current["last_error"] = None
        else:
            current["last_failure_ts"] = checked_at
            current["consecutive_failures"] = int(current.get("consecutive_failures") or 0) + 1
            current["last_error_kind"] = issue_kind(message)
            current["last_error"] = str(message or "")
        current["last_status"] = status or "unknown"
        current["checked_at_ts"] = checked_at
        health_map[exchange] = current

    for exchange, current in health_map.items():
        last_success_ts = _safe_float(current.get("last_success_ts"))
        last_error_kind = str(current.get("last_error_kind") or "").strip().lower() or None
        consecutive_failures = int(current.get("consecutive_failures") or 0)
        stale_sec = None
        if last_success_ts is not None:
            stale_sec = max(0.0, now_ts - float(last_success_ts))
        if last_error_kind == "auth_error":
            health = "untrusted"
        elif last_success_ts is None:
            health = "untrusted"
        elif stale_sec is not None and stale_sec > float(stale_after_sec):
            health = "stale"
        elif consecutive_failures >= int(failure_block_count):
            health = "degraded"
        else:
            health = "healthy"
        current["health"] = health
        current["stale_sec"] = stale_sec
    return health_map


def funding_time_weight(
    minutes_to_funding: float | None,
    interval_minutes: float | None,
    *,
    alpha: float = 1.7,
) -> float:
    minutes_val = _safe_float(minutes_to_funding)
    interval_val = _safe_float(interval_minutes)
    if minutes_val is None or interval_val is None or interval_val <= 0:
        return 0.0
    ratio = max(0.0, 1.0 - (float(minutes_val) / float(interval_val)))
    return float(ratio ** max(0.1, float(alpha)))


def effective_positive_funding(
    funding_to_next_usd: float | None,
    minutes_to_funding: float | None,
    interval_minutes: float | None,
    *,
    alpha: float = 1.7,
) -> float:
    funding = _safe_float(funding_to_next_usd)
    if funding is None or funding <= 0:
        return 0.0
    return float(funding) * funding_time_weight(minutes_to_funding, interval_minutes, alpha=alpha)


def exchange_stress_state(
    balance: Mapping[str, Any] | None,
    *,
    target_buffer_pct: float,
    warning_buffer_pct: float,
    panic_buffer_pct: float,
    min_free_balance_abs: float,
) -> dict[str, Any]:
    balance = balance or {}
    total = _safe_float(balance.get("total")) or 0.0
    used = _safe_float(balance.get("used")) or 0.0
    available = _safe_float(balance.get("available")) or 0.0
    if total <= 0 and used <= 0 and available <= 0:
        return {
            "status": "ok",
            "total_usd": 0.0,
            "used_usd": 0.0,
            "available_usd": 0.0,
            "buffer_pct": None,
            "target_free_usd": 0.0,
            "deficit_usd": 0.0,
            "stress_score": 0.0,
        }
    target_free_usd = max(float(min_free_balance_abs), float(used) * float(target_buffer_pct))
    deficit_usd = max(0.0, float(target_free_usd) - float(available))
    buffer_pct = _safe_float(balance.get("buffer_pct"))
    if buffer_pct is None and total > 0:
        buffer_pct = (float(available) / float(total)) * 100.0
    warning_threshold_pct = float(warning_buffer_pct) * 100.0
    panic_threshold_pct = float(panic_buffer_pct) * 100.0
    stress_score = 0.0
    if target_free_usd > 0:
        stress_score = float(deficit_usd) / float(target_free_usd)
    if buffer_pct is not None and buffer_pct <= panic_threshold_pct:
        status = "panic"
        stress_score = max(stress_score, 1.0)
    elif deficit_usd > 0 or (buffer_pct is not None and buffer_pct <= warning_threshold_pct):
        status = "stress"
    else:
        status = "ok"
    return {
        "status": status,
        "total_usd": float(total),
        "used_usd": float(used),
        "available_usd": float(available),
        "buffer_pct": float(buffer_pct) if buffer_pct is not None else None,
        "target_free_usd": float(target_free_usd),
        "deficit_usd": float(deficit_usd),
        "stress_score": float(stress_score),
    }


def classify_residual_leg(
    *,
    qty: float | None,
    notional_usd: float | None,
    min_qty: float | None = None,
    min_notional: float | None = None,
    amount_step: float | None = None,
    dust_notional_usd: float = 10.0,
) -> str:
    qty_val = abs(_safe_float(qty) or 0.0)
    notional_val = abs(_safe_float(notional_usd) or 0.0)
    min_qty_val = abs(_safe_float(min_qty) or 0.0)
    min_notional_val = abs(_safe_float(min_notional) or 0.0)
    step_val = abs(_safe_float(amount_step) or 0.0)
    if qty_val <= 0 or notional_val <= 0:
        return "flat"
    if min_qty_val and qty_val < min_qty_val:
        return "below_min_qty"
    if min_notional_val and notional_val < min_notional_val:
        return "below_min_notional"
    if step_val and qty_val < step_val:
        return "precision_blocked"
    if notional_val < float(dust_notional_usd):
        return "dust_suspect"
    return "closable_normal"


def derisk_candidate_score(
    *,
    margin_relief_usd: float | None,
    close_cost_usd: float | None,
    funding_to_next_usd: float | None,
    minutes_to_funding: float | None,
    interval_minutes: float | None,
    pressure_credit_usd: float | None = None,
    alpha: float = 1.7,
) -> float | None:
    relief = _safe_float(margin_relief_usd)
    if relief is None or relief <= 0:
        return None
    close_cost = max(0.0, float(_safe_float(close_cost_usd) or 0.0))
    positive_funding = effective_positive_funding(
        funding_to_next_usd,
        minutes_to_funding,
        interval_minutes,
        alpha=alpha,
    )
    negative_funding_credit = max(0.0, -(float(_safe_float(funding_to_next_usd) or 0.0)))
    pressure_credit = max(0.0, float(_safe_float(pressure_credit_usd) or 0.0))
    numerator = close_cost + positive_funding - negative_funding_credit - pressure_credit
    return float(numerator) / max(float(relief), 1e-9)


def derisk_score_allowed(
    candidate_score: float | None,
    max_candidate_score: float | None,
) -> bool:
    score = _safe_float(candidate_score)
    ceiling = _safe_float(max_candidate_score)
    if score is None or ceiling is None:
        return False
    return float(score) <= float(ceiling)


def qty_mismatch_ratio(long_qty: float | None, short_qty: float | None) -> float | None:
    long_val = abs(_safe_float(long_qty) or 0.0)
    short_val = abs(_safe_float(short_qty) or 0.0)
    if long_val <= 0 and short_val <= 0:
        return 0.0
    if max(long_val, short_val) <= 0:
        return None
    return abs(long_val - short_val) / max(long_val, short_val)


def price_velocity_bps(current_price: float | None, reference_price: float | None) -> float | None:
    current = _safe_float(current_price)
    reference = _safe_float(reference_price)
    if current is None or reference is None or reference <= 0:
        return None
    return ((float(current) - float(reference)) / float(reference)) * 10_000.0


def panic_severity(
    *,
    exchange_stress_score: float | None,
    adverse_move_bps: float | None,
    velocity_trigger_bps: float,
    recent_topup_count: int = 0,
) -> dict[str, Any]:
    stress_score = max(0.0, float(_safe_float(exchange_stress_score) or 0.0))
    adverse = abs(float(_safe_float(adverse_move_bps) or 0.0))
    velocity_trigger = max(1.0, float(velocity_trigger_bps))
    velocity_stress = max(0.0, adverse - velocity_trigger) / velocity_trigger
    topup_stress = 0.25 * max(0, int(recent_topup_count))
    score = (0.6 * stress_score) + (0.3 * velocity_stress) + (0.1 * topup_stress)
    if score >= 1.0:
        status = "emergency"
    elif score >= 0.6:
        status = "stress"
    else:
        status = "watch"
    return {
        "status": status,
        "score": float(score),
        "velocity_stress": float(velocity_stress),
        "topup_stress": float(topup_stress),
    }
