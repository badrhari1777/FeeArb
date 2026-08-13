from __future__ import annotations

import json

import pytest

from execution.grid_state import GridStateRepository
from execution.state_replay_contract import GRID_STATE_SCHEMA


def test_repository_loads_legacy_state_and_saves_explicit_schema(tmp_path) -> None:
    state_path = tmp_path / "state" / "grid.json"
    history_path = tmp_path / "logs" / "grid.jsonl"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps({"version": 1, "rules": {}}), encoding="utf-8")

    repository = GridStateRepository(state_path=state_path, history_path=history_path)

    assert repository.state == {
        "schema": GRID_STATE_SCHEMA,
        "version": 1,
        "rules": {},
    }
    assert repository.payload()["state_contract"]["valid"] is True
    repository.save()
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["schema"] == GRID_STATE_SCHEMA


def test_repository_blocks_save_after_corrupt_state(tmp_path) -> None:
    state_path = tmp_path / "state" / "grid.json"
    history_path = tmp_path / "logs" / "grid.jsonl"
    state_path.parent.mkdir(parents=True)
    state_path.write_text("{broken", encoding="utf-8")

    repository = GridStateRepository(state_path=state_path, history_path=history_path)
    payload = repository.payload()

    assert payload["state_contract"]["valid"] is False
    assert payload["state_contract"]["issues"][0]["code"] == "state_load_error"
    with pytest.raises(RuntimeError, match="grid_state_contract_blocked"):
        repository.save()
    assert state_path.read_text(encoding="utf-8") == "{broken"


def test_repository_owns_shared_state_lock_and_history(tmp_path) -> None:
    repository = GridStateRepository(
        state_path=tmp_path / "state" / "grid.json",
        history_path=tmp_path / "logs" / "grid.jsonl",
    )

    repository.history.append({"event": "test", "rule_id": "grid-1"})

    assert repository.lock is not None
    assert repository.history.path.read_text(encoding="utf-8").strip()
