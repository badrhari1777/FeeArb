from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping

from execution.state_replay_contract import GRID_STATE_SCHEMA, audit_grid_state
from execution.storage import JsonStateStore, JsonlEventStore


class GridStateRepository:
    """Single owner for versioned Grid state, history and mutation lock."""

    def __init__(self, *, state_path: Path, history_path: Path) -> None:
        self.store = JsonStateStore(state_path)
        self.history = JsonlEventStore(history_path)
        self.lock = asyncio.Lock()
        self.load_error: str | None = None
        self.state = self._load()

    def _load(self) -> dict[str, Any]:
        path = self.store.path
        if not path.exists():
            return {"schema": GRID_STATE_SCHEMA, "version": 1, "rules": {}}
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.load_error = f"grid_state_read_failed:{type(exc).__name__}"
            return {
                "schema": GRID_STATE_SCHEMA,
                "version": 1,
                "rules": {},
                "contract_error": self.load_error,
            }
        if not isinstance(raw, Mapping):
            self.load_error = "grid_state_not_object"
            return {
                "schema": GRID_STATE_SCHEMA,
                "version": 1,
                "rules": {},
                "contract_error": self.load_error,
            }
        rules = raw.get("rules")
        if not isinstance(rules, Mapping):
            self.load_error = "grid_state_rules_not_object"
            rules = {}
        state = {
            "schema": str(raw.get("schema") or GRID_STATE_SCHEMA),
            "version": raw.get("version", 1),
            "rules": dict(rules),
        }
        if self.load_error:
            state["contract_error"] = self.load_error
        return state

    def save(self) -> None:
        if self.load_error:
            raise RuntimeError(f"grid_state_contract_blocked:{self.load_error}")
        self.state["schema"] = GRID_STATE_SCHEMA
        self.state["version"] = 1
        self.store.save(self.state)

    def payload(self) -> dict[str, Any]:
        rules = list((self.state.get("rules") or {}).values())
        rules.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return {
            "schema": GRID_STATE_SCHEMA,
            "version": 1,
            "mode": "live",
            "live_limits": {
                "max_chunk_notional_usd": None,
                "max_total_notional_usd": None,
                "max_live_rules": None,
            },
            "rules": rules,
            "state_contract": audit_grid_state(self.state).as_dict(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
