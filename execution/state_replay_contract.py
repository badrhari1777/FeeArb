from __future__ import annotations

import copy
import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Mapping


CONTRACT_SCHEMA = "execution_state_replay_contract_v1"
PUMP_STATE_SCHEMA = "pump_live_state_v1"
GRID_STATE_SCHEMA = "auto_arb_grid_state_v1"

_PUMP_DURABLE_KEYS = (
    "positions",
    "seen_events",
    "capital_manager",
    "portfolio_risk_freeze_active",
    "portfolio_risk_freeze_reason",
    "portfolio_risk_freeze_symbol",
    "portfolio_risk_freeze_buffer_pct",
    "emergency_close_requested",
)


@dataclass(frozen=True)
class StateContractReport:
    module: str
    state_schema: str | None
    valid: bool
    issues: tuple[dict[str, Any], ...]
    durable_fingerprint: str
    ownership_keys: tuple[str, ...]
    restart_actions: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": CONTRACT_SCHEMA,
            "module": self.module,
            "state_schema": self.state_schema,
            "valid": self.valid,
            "issues": [dict(item) for item in self.issues],
            "durable_fingerprint": self.durable_fingerprint,
            "ownership_keys": list(self.ownership_keys),
            "restart_actions": [dict(item) for item in self.restart_actions],
        }


def _issue(code: str, path: str, message: str, *, severity: str = "error") -> dict[str, str]:
    return {
        "severity": severity,
        "code": code,
        "path": path,
        "message": message,
    }


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        # Audit must still return a deterministic fingerprint for a rejected
        # snapshot that contains Python's non-standard NaN/Infinity tokens.
        allow_nan=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _nonfinite_paths(value: Any, path: str = "$") -> list[str]:
    if isinstance(value, float) and not math.isfinite(value):
        return [path]
    if isinstance(value, Mapping):
        paths: list[str] = []
        for key, item in value.items():
            paths.extend(_nonfinite_paths(item, f"{path}.{key}"))
        return paths
    if isinstance(value, (list, tuple)):
        paths = []
        for index, item in enumerate(value):
            paths.extend(_nonfinite_paths(item, f"{path}[{index}]"))
        return paths
    return []


def _symbol_base(value: Any) -> str:
    symbol = str(value or "").upper().replace("/", "").replace(":", "")
    for suffix in ("USDTUSDT", "USDCUSDC", "USDUSD"):
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)] + suffix[: len(suffix) // 2]
            break
    for quote in ("USDT", "USDC", "USD"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)]
    return symbol


def pump_durable_projection(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema": PUMP_STATE_SCHEMA,
        **{
            key: copy.deepcopy(state.get(key))
            for key in _PUMP_DURABLE_KEYS
        },
    }


def grid_durable_projection(state: Mapping[str, Any]) -> dict[str, Any]:
    rules = state.get("rules")
    return {
        "schema": GRID_STATE_SCHEMA,
        "version": 1,
        "rules": copy.deepcopy(dict(rules)) if isinstance(rules, Mapping) else {},
    }


def project_pump_restart(state: Mapping[str, Any]) -> dict[str, Any]:
    """Apply only the fail-closed volatile resets guaranteed at cold start."""
    projected = copy.deepcopy(dict(state))
    positions = projected.get("positions")
    open_positions = [
        item
        for item in positions if isinstance(item, Mapping) and item.get("status") != "closed"
    ] if isinstance(positions, list) else []
    projected.update(
        {
            "schema": PUMP_STATE_SCHEMA,
            "entry_armed": False,
            "pending_signals": [],
            "transient_recovery_pending": False,
            "healthy_recovery_cycles": 0,
            "close_recovery_pending": False,
            "close_recovery_symbol": None,
            "close_recovery_healthy_cycles": 0,
            "portfolio_risk_restore_armed": False,
            "portfolio_risk_recovery_cycles": 0,
        }
    )
    if open_positions:
        projected["monitor_enabled"] = True
        projected["status"] = "recovery_monitoring"
    elif projected.get("status") not in {"disabled", "stopped"}:
        projected["monitor_enabled"] = False
        projected["status"] = "disarmed_after_restart"
        projected["blocked_reason"] = "backend_restart"
    return projected


def project_grid_restart(state: Mapping[str, Any]) -> dict[str, Any]:
    """Grid rules persist verbatim; in-memory executions become reconciliation work."""
    return grid_durable_projection(state)


def audit_pump_state(state: Any) -> StateContractReport:
    issues: list[dict[str, Any]] = []
    ownership: list[str] = []
    actions: list[dict[str, Any]] = [
        {"action": "disarm_entries", "reason": "cold_restart"},
        {"action": "clear_pending_signals", "reason": "signals_are_not_replayable"},
    ]
    if not isinstance(state, Mapping):
        issues.append(_issue("state_not_object", "$", "State must be a JSON object."))
        state = {}
    for path in _nonfinite_paths(state):
        issues.append(_issue("nonfinite_number", path, "Persisted JSON numbers must be finite."))
    schema = str(state.get("schema") or "") or None
    if schema != PUMP_STATE_SCHEMA:
        issues.append(
            _issue(
                "schema_mismatch",
                "$.schema",
                f"Expected {PUMP_STATE_SCHEMA}; got {schema or 'missing'}.",
            )
        )
    raw_positions = state.get("positions")
    if not isinstance(raw_positions, list):
        issues.append(_issue("positions_not_list", "$.positions", "Positions must be a list."))
        raw_positions = []
    live_ids: set[str] = set()
    ownership_seen: set[str] = set()
    open_count = 0
    for index, raw in enumerate(raw_positions):
        path = f"$.positions[{index}]"
        if not isinstance(raw, Mapping):
            issues.append(_issue("position_not_object", path, "Position must be an object."))
            continue
        if str(raw.get("status") or "") == "closed":
            continue
        open_count += 1
        live_id = str(raw.get("live_id") or "")
        symbol = str(raw.get("symbol") or "").upper()
        account = str(raw.get("account_alias") or "")
        strategy = str(raw.get("strategy_id") or "")
        if not live_id:
            issues.append(_issue("missing_live_id", f"{path}.live_id", "Open position needs live_id."))
        elif live_id in live_ids:
            issues.append(_issue("duplicate_live_id", f"{path}.live_id", "live_id must be unique."))
        live_ids.add(live_id)
        if not symbol:
            issues.append(_issue("missing_symbol", f"{path}.symbol", "Open position needs symbol."))
        key = f"{account or 'missing'}|bybit|{symbol or 'missing'}|short|{strategy or 'missing'}"
        ownership.append(key)
        if key in ownership_seen:
            issues.append(
                _issue("duplicate_ownership", path, f"Duplicate Pump ownership key {key}.")
            )
        ownership_seen.add(key)
        legs = raw.get("legs")
        if not isinstance(legs, list) or not legs:
            issues.append(_issue("missing_legs", f"{path}.legs", "Open position needs ladder legs."))
        else:
            steps: set[int] = set()
            for leg_index, leg in enumerate(legs):
                if not isinstance(leg, Mapping):
                    issues.append(
                        _issue("leg_not_object", f"{path}.legs[{leg_index}]", "Leg must be an object.")
                    )
                    continue
                try:
                    step = int(leg.get("step"))
                except (TypeError, ValueError):
                    step = 0
                if step <= 0 or step in steps:
                    issues.append(
                        _issue(
                            "invalid_leg_step",
                            f"{path}.legs[{leg_index}].step",
                            "Leg steps must be unique positive integers.",
                        )
                    )
                steps.add(step)
        if not raw.get("risk_policy_id") or not isinstance(raw.get("risk_policy"), Mapping):
            issues.append(
                _issue(
                    "legacy_risk_policy",
                    path,
                    "Risk policy snapshot requires migration before ownership resume.",
                    severity="warning",
                )
            )
        if not raw.get("stop_price") or not raw.get("tp_price"):
            issues.append(
                _issue(
                    "protection_reconciliation_required",
                    path,
                    "Open position protection must be verified from the exchange.",
                    severity="warning",
                )
            )
        actions.append(
            {
                "action": "verify_exchange_ownership_and_protection",
                "live_id": live_id or None,
                "symbol": symbol or None,
            }
        )
    actions.append(
        {
            "action": "resume_monitoring" if open_count else "remain_disarmed",
            "open_positions": open_count,
        }
    )
    durable = pump_durable_projection(state)
    return StateContractReport(
        module="pump_live",
        state_schema=schema,
        valid=not any(item["severity"] == "error" for item in issues),
        issues=tuple(issues),
        durable_fingerprint=_canonical_hash(durable),
        ownership_keys=tuple(sorted(ownership)),
        restart_actions=tuple(actions),
    )


def _grid_rule_ownership(rule: Mapping[str, Any]) -> tuple[str, set[str]]:
    symbol = _symbol_base(rule.get("symbol"))
    venues = {
        str(rule.get("long_exchange") or "").strip().lower(),
        str(rule.get("short_exchange") or "").strip().lower(),
    }
    venues.discard("")
    return symbol, venues


def audit_grid_state(state: Any) -> StateContractReport:
    issues: list[dict[str, Any]] = []
    ownership: list[str] = []
    actions: list[dict[str, Any]] = []
    if not isinstance(state, Mapping):
        issues.append(_issue("state_not_object", "$", "State must be a JSON object."))
        state = {}
    for path in _nonfinite_paths(state):
        issues.append(_issue("nonfinite_number", path, "Persisted JSON numbers must be finite."))
    schema = str(state.get("schema") or "") or None
    if schema is None:
        issues.append(
            _issue(
                "legacy_schema_missing",
                "$.schema",
                f"Legacy state is accepted and will be saved as {GRID_STATE_SCHEMA}.",
                severity="warning",
            )
        )
    elif schema != GRID_STATE_SCHEMA:
        issues.append(
            _issue(
                "schema_mismatch",
                "$.schema",
                f"Expected {GRID_STATE_SCHEMA}; got {schema}.",
            )
        )
    if state.get("version", 1) != 1:
        issues.append(_issue("version_mismatch", "$.version", "Only Grid state version 1 is supported."))
    rules = state.get("rules")
    if not isinstance(rules, Mapping):
        issues.append(_issue("rules_not_object", "$.rules", "Rules must be an object keyed by rule id."))
        rules = {}
    live_rules: list[tuple[str, Mapping[str, Any], str, set[str]]] = []
    for key, raw in rules.items():
        path = f"$.rules.{key}"
        if not isinstance(raw, Mapping):
            issues.append(_issue("rule_not_object", path, "Rule must be an object."))
            continue
        rule_id = str(raw.get("id") or "")
        if not rule_id or rule_id != str(key):
            issues.append(_issue("rule_id_mismatch", f"{path}.id", "Rule id must match its map key."))
        mode = str(raw.get("mode") or "")
        if mode not in {"shadow", "live"}:
            issues.append(_issue("invalid_mode", f"{path}.mode", "Grid mode must be shadow or live."))
        active_execution = str(raw.get("active_execution_id") or "")
        if active_execution:
            if mode != "live":
                issues.append(
                    _issue(
                        "execution_without_live_ownership",
                        f"{path}.active_execution_id",
                        "Persisted active execution requires Live ownership.",
                    )
                )
            actions.append(
                {
                    "action": "reconcile_execution_from_exchange",
                    "rule_id": rule_id or str(key),
                    "execution_id": active_execution,
                }
            )
        if mode == "live" and (bool(raw.get("enabled")) or active_execution):
            symbol, venues = _grid_rule_ownership(raw)
            if not symbol or len(venues) != 2:
                issues.append(
                    _issue(
                        "incomplete_live_ownership",
                        path,
                        "Live Grid needs a symbol and two distinct exchanges.",
                    )
                )
            ownership.append(f"{symbol}|{'/'.join(sorted(venues))}")
            live_rules.append((str(key), raw, symbol, venues))
    for index, (left_id, _left, left_symbol, left_venues) in enumerate(live_rules):
        for right_id, _right, right_symbol, right_venues in live_rules[index + 1 :]:
            if left_symbol and left_symbol == right_symbol and left_venues.intersection(right_venues):
                issues.append(
                    _issue(
                        "duplicate_live_ownership",
                        "$.rules",
                        f"Rules {left_id} and {right_id} overlap Live ownership.",
                    )
                )
    if not actions:
        actions.append({"action": "resume_rule_monitoring", "active_executions": 0})
    durable = grid_durable_projection(state)
    return StateContractReport(
        module="grid",
        state_schema=schema,
        valid=not any(item["severity"] == "error" for item in issues),
        issues=tuple(issues),
        durable_fingerprint=_canonical_hash(durable),
        ownership_keys=tuple(sorted(ownership)),
        restart_actions=tuple(actions),
    )


def compare_restart(
    module: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> dict[str, Any]:
    if module == "pump_live":
        before_report = audit_pump_state(before)
        after_report = audit_pump_state(after)
        postconditions = {
            "entry_disarmed": after.get("entry_armed") is False,
            "pending_signals_cleared": list(after.get("pending_signals") or []) == [],
            "durable_fingerprint_preserved": (
                before_report.durable_fingerprint == after_report.durable_fingerprint
            ),
        }
    elif module == "grid":
        before_report = audit_grid_state(before)
        after_report = audit_grid_state(after)
        postconditions = {
            "durable_fingerprint_preserved": (
                before_report.durable_fingerprint == after_report.durable_fingerprint
            ),
        }
    else:
        raise ValueError(f"Unsupported state module: {module}")
    return {
        "schema": CONTRACT_SCHEMA,
        "module": module,
        "valid": before_report.valid
        and after_report.valid
        and all(postconditions.values()),
        "postconditions": postconditions,
        "before": before_report.as_dict(),
        "after": after_report.as_dict(),
    }
