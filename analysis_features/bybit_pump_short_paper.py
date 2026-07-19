from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from analysis_features.bybit_pump_short_outcomes import EXIT_PLANS
from config import BASE_DIR

DEFAULT_SHADOW_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_shadow"
DEFAULT_PAPER_STATE_PATH = DEFAULT_SHADOW_OUTPUT_DIR / "paper_positions.json"
DEFAULT_PAPER_EVENTS_PATH = DEFAULT_SHADOW_OUTPUT_DIR / "paper_events.jsonl"
DEFAULT_PAPER_CSV_PATH = DEFAULT_SHADOW_OUTPUT_DIR / "paper_positions_latest.csv"
FEE_ROUNDTRIP_PCT = 0.18


def apply_shadow_rows_to_paper(
    rows: list[dict[str, Any]],
    *,
    state_path: Path = DEFAULT_PAPER_STATE_PATH,
    events_path: Path = DEFAULT_PAPER_EVENTS_PATH,
    csv_path: Path = DEFAULT_PAPER_CSV_PATH,
) -> dict[str, Any]:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state = load_paper_state(state_path)
    positions = state.setdefault("positions", [])
    event_count = 0
    rows_by_symbol = {str(row.get("symbol") or ""): row for row in rows if row.get("symbol")}

    for row in rows:
        if row.get("status") != "entry_candidate":
            continue
        if open_position_exists(positions, row):
            continue
        position = open_paper_position(row)
        positions.append(position)
        append_jsonl(events_path, {"event": "paper_open", "position": position})
        event_count += 1

    for position in positions:
        if position.get("status") != "open":
            continue
        row = rows_by_symbol.get(str(position.get("symbol") or ""))
        if not row:
            continue
        events = update_paper_position(position, row)
        for event in events:
            append_jsonl(events_path, event)
            event_count += 1

    state["updated_at_ms"] = max((int(float(row.get("ts_ms") or 0)) for row in rows), default=state.get("updated_at_ms"))
    state["schema"] = "bybit_pump_short_paper_v1"
    save_paper_state(state_path, state)
    write_paper_csv(csv_path, positions)
    return {
        "positions": len(positions),
        "open_positions": sum(1 for item in positions if item.get("status") == "open"),
        "closed_positions": sum(1 for item in positions if item.get("status") == "closed"),
        "events": event_count,
    }


def open_paper_position(row: dict[str, Any]) -> dict[str, Any]:
    ts_ms = int(float(row.get("ts_ms") or 0))
    entry_price = to_float(row.get("last_close")) or to_float(row.get("trigger_close")) or 0.0
    paper_id = "|".join(
        [
            str(row.get("symbol") or ""),
            str(row.get("event_id") or row.get("trigger_ts") or ts_ms),
            str(row.get("matched_entry_strategy") or ""),
            str(row.get("matched_exit_strategy") or ""),
        ]
    )
    return {
        "paper_id": paper_id,
        "status": "open",
        "symbol": row.get("symbol"),
        "event_id": row.get("event_id"),
        "opened_at_ms": ts_ms,
        "updated_at_ms": ts_ms,
        "closed_at_ms": None,
        "entry_price": entry_price,
        "current_price": entry_price,
        "remaining_weight": 1.0,
        "realized_net_pct": 0.0,
        "unrealized_net_pct": 0.0,
        "combined_net_pct": 0.0,
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "exit_reason": None,
        "profile": row.get("matched_profile"),
        "profile_rank": row.get("matched_profile_rank"),
        "entry_strategy": row.get("matched_entry_strategy"),
        "exit_strategy": row.get("matched_exit_strategy"),
        "anti_overfit_status": row.get("matched_anti_overfit_status"),
        "open_snapshot": compact_shadow_snapshot(row),
        "last_snapshot": compact_shadow_snapshot(row),
        "target_hits": [],
    }


def update_paper_position(position: dict[str, Any], row: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    current_price = to_float(row.get("last_close"))
    entry_price = to_float(position.get("entry_price"))
    if current_price is None or entry_price in {None, 0.0}:
        return events
    now_ms = int(float(row.get("ts_ms") or position.get("updated_at_ms") or 0))
    position["updated_at_ms"] = now_ms
    position["current_price"] = current_price
    position["last_snapshot"] = compact_shadow_snapshot(row)

    raw_pnl = short_price_pnl_pct(entry_price, current_price)
    position["mfe_pct"] = max(to_float(position.get("mfe_pct")) or 0.0, raw_pnl)
    position["mae_pct"] = max(to_float(position.get("mae_pct")) or 0.0, -raw_pnl)

    plan = exit_plan(str(position.get("exit_strategy") or ""))
    if plan:
        for target_pct, fraction in plan.get("targets", ()):
            if raw_pnl < float(target_pct):
                continue
            if target_already_hit(position, float(target_pct)):
                continue
            event = close_fraction(position, now_ms=now_ms, fill_pnl_pct=float(target_pct), fraction=float(fraction), reason=f"target_{int(target_pct)}")
            events.append(event)
            if position.get("status") == "closed":
                break
        if position.get("status") == "open":
            max_hold_h = int(plan.get("max_hold_h") or 0)
            opened_at = int(float(position.get("opened_at_ms") or now_ms))
            if max_hold_h > 0 and now_ms - opened_at >= max_hold_h * 3_600_000:
                events.append(close_fraction(position, now_ms=now_ms, fill_pnl_pct=raw_pnl, fraction=1.0, reason="time_stop"))

    if position.get("status") == "open":
        remaining = to_float(position.get("remaining_weight")) or 0.0
        realized = to_float(position.get("realized_net_pct")) or 0.0
        unrealized = raw_pnl - FEE_ROUNDTRIP_PCT
        position["unrealized_net_pct"] = round(unrealized, 6)
        position["combined_net_pct"] = round(realized + remaining * unrealized, 6)
    return events


def close_fraction(
    position: dict[str, Any],
    *,
    now_ms: int,
    fill_pnl_pct: float,
    fraction: float,
    reason: str,
) -> dict[str, Any]:
    remaining = max(0.0, min(1.0, to_float(position.get("remaining_weight")) or 0.0))
    close_weight = remaining if fraction >= 1.0 else remaining * max(0.0, min(1.0, fraction))
    net = fill_pnl_pct - FEE_ROUNDTRIP_PCT
    realized = (to_float(position.get("realized_net_pct")) or 0.0) + close_weight * net
    remaining = max(0.0, remaining - close_weight)
    position["realized_net_pct"] = round(realized, 6)
    position["remaining_weight"] = round(remaining, 8)
    position["combined_net_pct"] = round(realized, 6) if remaining <= 1e-8 else position.get("combined_net_pct")
    position["target_hits"] = list(position.get("target_hits") or []) + [{"reason": reason, "ts_ms": now_ms, "weight": close_weight, "net_pct": net}]
    if remaining <= 1e-8:
        position["status"] = "closed"
        position["closed_at_ms"] = now_ms
        position["exit_reason"] = reason
        position["unrealized_net_pct"] = 0.0
        position["combined_net_pct"] = round(realized, 6)
    return {
        "event": "paper_close" if position.get("status") == "closed" else "paper_partial_close",
        "paper_id": position.get("paper_id"),
        "symbol": position.get("symbol"),
        "ts_ms": now_ms,
        "reason": reason,
        "closed_weight": round(close_weight, 8),
        "remaining_weight": position.get("remaining_weight"),
        "net_pct": round(net, 6),
        "realized_net_pct": position.get("realized_net_pct"),
    }


def open_position_exists(positions: list[dict[str, Any]], row: dict[str, Any]) -> bool:
    symbol = str(row.get("symbol") or "")
    event_id = str(row.get("event_id") or row.get("trigger_ts") or "")
    entry_strategy = str(row.get("matched_entry_strategy") or "")
    exit_strategy = str(row.get("matched_exit_strategy") or "")
    for position in positions:
        if position.get("status") != "open":
            continue
        if (
            str(position.get("symbol") or "") == symbol
            and str(position.get("event_id") or "") == event_id
            and str(position.get("entry_strategy") or "") == entry_strategy
            and str(position.get("exit_strategy") or "") == exit_strategy
        ):
            return True
    return False


def target_already_hit(position: dict[str, Any], target_pct: float) -> bool:
    target_reason = f"target_{int(target_pct)}"
    return any(str(item.get("reason")) == target_reason for item in position.get("target_hits") or [])


def exit_plan(name: str) -> dict[str, Any] | None:
    for plan in EXIT_PLANS:
        if str(plan.get("name")) == name:
            return plan
    return None


def compact_shadow_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "ts_ms",
        "status",
        "reason",
        "last_close",
        "trigger_pump_pct",
        "pullback_from_high_pct",
        "oi_change_24h_pct",
        "long_ratio",
        "funding_prev_24h_pct",
    )
    return {key: row.get(key) for key in keys}


def short_price_pnl_pct(entry_price: float, current_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    return (1.0 - current_price / entry_price) * 100.0


def read_paper_summary(
    *,
    state_path: Path = DEFAULT_PAPER_STATE_PATH,
    limit: int = 50,
) -> dict[str, Any]:
    state = load_paper_state(state_path)
    positions = list(state.get("positions") or [])
    positions.sort(key=lambda item: int(float(item.get("updated_at_ms") or 0)), reverse=True)
    return {
        "schema": state.get("schema") or "bybit_pump_short_paper_v1",
        "updated_at_ms": state.get("updated_at_ms"),
        "positions": positions[:limit],
        "open_positions": sum(1 for item in positions if item.get("status") == "open"),
        "closed_positions": sum(1 for item in positions if item.get("status") == "closed"),
    }


def load_paper_state(path: Path = DEFAULT_PAPER_STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"schema": "bybit_pump_short_paper_v1", "positions": [], "updated_at_ms": None}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema": "bybit_pump_short_paper_v1", "positions": [], "updated_at_ms": None}
    if not isinstance(payload, dict):
        return {"schema": "bybit_pump_short_paper_v1", "positions": [], "updated_at_ms": None}
    payload.setdefault("positions", [])
    return payload


def save_paper_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def write_paper_csv(path: Path, positions: list[dict[str, Any]]) -> None:
    rows = [flatten_position(position) for position in positions]
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def flatten_position(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_id": position.get("paper_id"),
        "status": position.get("status"),
        "symbol": position.get("symbol"),
        "profile": position.get("profile"),
        "entry_strategy": position.get("entry_strategy"),
        "exit_strategy": position.get("exit_strategy"),
        "opened_at_ms": position.get("opened_at_ms"),
        "updated_at_ms": position.get("updated_at_ms"),
        "closed_at_ms": position.get("closed_at_ms"),
        "entry_price": position.get("entry_price"),
        "current_price": position.get("current_price"),
        "remaining_weight": position.get("remaining_weight"),
        "realized_net_pct": position.get("realized_net_pct"),
        "unrealized_net_pct": position.get("unrealized_net_pct"),
        "combined_net_pct": position.get("combined_net_pct"),
        "mfe_pct": position.get("mfe_pct"),
        "mae_pct": position.get("mae_pct"),
        "exit_reason": position.get("exit_reason"),
        "anti_overfit_status": position.get("anti_overfit_status"),
    }


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in {float("inf"), float("-inf")} else None


__all__ = [
    "DEFAULT_PAPER_STATE_PATH",
    "apply_shadow_rows_to_paper",
    "open_paper_position",
    "read_paper_summary",
    "update_paper_position",
]
