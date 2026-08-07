from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from analysis_features.strategy_lab_event_lake import (
    LEDGER_SCHEMA,
    MANIFEST_SCHEMA,
    validate_ledger_record,
    validate_window,
    stable_hash,
)


VALIDATION_SCHEMA = "strategy_lab_event_lake_validation_v1"


def validate_event_lake_output(
    output_dir: Path,
    *,
    require_complete: bool = True,
) -> dict[str, Any]:
    """Validate Event Lake identity, provenance, cache and ledger linkage."""

    manifest = read_json_object(output_dir / "manifest.json")
    metadata = read_json_object(output_dir / "metadata.json")
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ValueError("invalid Event Lake manifest schema")
    if manifest.get("public_only") is not True or metadata.get("public_only") is not True:
        raise ValueError("Event Lake validation requires public-only artifacts")
    if manifest.get("run_id") != metadata.get("run_id"):
        raise ValueError("Event Lake run_id mismatch")
    for key in ("code_commit", "config_hash", "source_manifest_hash"):
        if manifest.get(key) != metadata.get(key):
            raise ValueError(f"Event Lake {key} mismatch")

    tasks = [dict(row) for row in manifest.get("tasks") or []]
    physical_rows = [dict(row) for row in manifest.get("physical_windows") or []]
    task_by_id = unique_index(tasks, "task_id", "logical task")
    physical_by_id = unique_index(
        physical_rows, "physical_window_id", "physical window"
    )
    if not task_by_id or not physical_by_id:
        raise ValueError("Event Lake manifest has no tasks or physical windows")

    logical_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    representative_task: dict[str, dict[str, Any]] = {}
    for task in tasks:
        physical_window_id = str(task.get("physical_window_id") or "")
        physical = physical_by_id.get(physical_window_id)
        if physical is None:
            raise ValueError(f"logical task references unknown window: {physical_window_id}")
        for key in ("symbol", "exchange", "start_ms", "end_ms", "timeframe"):
            if task.get(key) != physical.get(key):
                raise ValueError(f"logical/physical identity mismatch: {key}")
        logical_key = (str(task.get("event_id") or ""), str(task.get("exchange") or ""))
        if not all(logical_key) or logical_key in logical_by_key:
            raise ValueError(f"duplicate or empty logical event/exchange key: {logical_key}")
        logical_by_key[logical_key] = task
        representative_task.setdefault(physical_window_id, task)

    coverage = read_csv_rows(output_dir / "coverage.csv")
    coverage_by_task = unique_index(coverage, "run_task_id", "coverage task")
    if require_complete and set(coverage_by_task) != set(task_by_id):
        raise ValueError("coverage task set does not match manifest")
    for task_id, row in coverage_by_task.items():
        task = task_by_id.get(task_id)
        if task is None:
            raise ValueError(f"coverage references unknown task: {task_id}")
        if str(row.get("physical_window_id") or "") != str(
            task.get("physical_window_id") or ""
        ):
            raise ValueError("coverage physical window mismatch")

    windows_dir = output_dir / "windows"
    actual_files = {path.stem: path for path in windows_dir.glob("*.json")}
    expected_window_ids = set(physical_by_id)
    if require_complete and set(actual_files) != expected_window_ids:
        missing = len(expected_window_ids - set(actual_files))
        extra = len(set(actual_files) - expected_window_ids)
        raise ValueError(
            f"physical window file set mismatch: missing={missing}, extra={extra}"
        )
    unexpected_files = set(actual_files) - expected_window_ids
    if unexpected_files:
        raise ValueError(f"unexpected physical window files: {len(unexpected_files)}")

    window_hashes: dict[str, str] = {}
    for physical_window_id, path in actual_files.items():
        window = read_json_object(path)
        validate_window(window, representative_task[physical_window_id])
        window_hashes[physical_window_id] = stable_hash(window)

    ledger = read_ledger_rows(output_dir / "ledger.jsonl")
    record_by_id = unique_index(ledger, "record_id", "ledger record")
    ledger_by_logical_key: dict[tuple[str, str], dict[str, Any]] = {}
    for record in ledger:
        validate_ledger_record(record)
        if record.get("schema") != LEDGER_SCHEMA:
            raise ValueError("invalid Event Lake ledger schema")
        exchange_pair = list(record.get("exchange_pair") or [])
        if len(exchange_pair) != 1:
            raise ValueError("Event Lake enrichment record must have one exchange")
        logical_key = (str(record.get("event_id") or ""), str(exchange_pair[0]))
        task = logical_by_key.get(logical_key)
        if task is None:
            raise ValueError(f"ledger references unknown logical task: {logical_key}")
        if logical_key in ledger_by_logical_key:
            raise ValueError(f"duplicate ledger logical key: {logical_key}")
        ledger_by_logical_key[logical_key] = record
        physical_window_id = str(task["physical_window_id"])
        expected_ref = f"windows/{physical_window_id}.json"
        if str(record.get("features_ref") or "").replace("\\", "/") != expected_ref:
            raise ValueError("ledger features_ref does not match physical window")
        expected_hash = window_hashes.get(physical_window_id)
        if expected_hash is None:
            if require_complete:
                raise ValueError("ledger references a missing physical window")
        elif record.get("features_hash") != expected_hash:
            raise ValueError("ledger features_hash does not match physical window")
        for key in ("run_id", "code_commit", "config_hash", "source_manifest_hash"):
            if record.get(key) != manifest.get(key):
                raise ValueError(f"ledger {key} does not match manifest")

    if require_complete:
        if metadata.get("executed_public") is not True:
            raise ValueError("Event Lake collection is not marked complete")
        if set(ledger_by_logical_key) != set(logical_by_key):
            raise ValueError("ledger logical task set does not match manifest")
        if len(record_by_id) != len(task_by_id):
            raise ValueError("ledger record count does not match logical task count")
        if int(metadata.get("logical_tasks") or 0) != len(task_by_id):
            raise ValueError("metadata logical task count mismatch")
        if int(metadata.get("physical_windows") or 0) != len(physical_by_id):
            raise ValueError("metadata physical window count mismatch")
        if int(metadata.get("coverage_rows") or 0) != len(coverage_by_task):
            raise ValueError("metadata coverage count mismatch")
        if int(metadata.get("ledger_records") or 0) != len(record_by_id):
            raise ValueError("metadata ledger record count mismatch")
        logical_event_count = len(
            {str(task.get("event_id") or "") for task in tasks}
        )
        if int(metadata.get("selected_events") or 0) != logical_event_count:
            raise ValueError("metadata logical event count mismatch")
        if any(str(task.get("status") or "") == "planned" for task in tasks):
            raise ValueError("completed Event Lake manifest still has planned tasks")

    decision_counts = Counter(str(record.get("decision") or "") for record in ledger)
    status_counts = Counter(str(row.get("status") or "") for row in coverage)
    if require_complete:
        expected_status_counts = {
            str(key): int(value)
            for key, value in dict(metadata.get("status_counts") or {}).items()
        }
        if dict(status_counts) != expected_status_counts:
            raise ValueError("metadata coverage status counts mismatch")
    return {
        "schema": VALIDATION_SCHEMA,
        "valid": True,
        "complete": bool(require_complete),
        "public_only": True,
        "run_id": manifest.get("run_id"),
        "logical_events": len({str(task.get("event_id") or "") for task in tasks}),
        "logical_tasks": len(task_by_id),
        "physical_windows_expected": len(physical_by_id),
        "physical_windows_present": len(actual_files),
        "coverage_rows": len(coverage_by_task),
        "ledger_records": len(record_by_id),
        "ledger_logical_keys": len(ledger_by_logical_key),
        "decision_counts": dict(sorted(decision_counts.items())),
        "coverage_status_counts": dict(sorted(status_counts.items())),
    }


def read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(f"missing Event Lake artifact: {path.name}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid Event Lake JSON: {path.name}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"Event Lake JSON is not an object: {path.name}")
    return dict(payload)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ValueError(f"missing Event Lake artifact: {path.name}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def read_ledger_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise ValueError(f"missing Event Lake artifact: {path.name}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid ledger JSON at line {line_number}") from exc
            if not isinstance(payload, Mapping):
                raise ValueError(f"ledger row is not an object at line {line_number}")
            rows.append(dict(payload))
    return rows


def unique_index(
    rows: list[dict[str, Any]] | list[dict[str, str]],
    key: str,
    label: str,
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        value = str(row.get(key) or "")
        if not value:
            raise ValueError(f"empty {label} identity")
        if value in indexed:
            raise ValueError(f"duplicate {label} identity: {value}")
        indexed[value] = dict(row)
    return indexed


__all__ = ["VALIDATION_SCHEMA", "validate_event_lake_output"]
