from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis_features.strategy_lab_event_lake import EventLakeConfig, run_event_lake
from analysis_features.strategy_lab_event_lake_validation import (
    validate_event_lake_output,
)
from tests.test_strategy_lab_event_lake import BASE_TS, FakePublicProvider, event


def build_duplicate_lake(output_dir: Path) -> None:
    run_event_lake(
        output_dir=output_dir,
        config=EventLakeConfig(
            exchanges=("binance", "bybit"),
            max_events=10,
            selection_mode="all_events",
        ),
        catalog_rows=[
            event("AUSDT", BASE_TS, "pump_lifecycle"),
            event("AUSDT", BASE_TS, "pump_universe_hourly_spike"),
        ],
        execute_public=True,
        provider=FakePublicProvider(),
        code_commit="abc",
    )


def test_complete_validator_links_logical_records_to_physical_cache(
    tmp_path: Path,
) -> None:
    build_duplicate_lake(tmp_path)

    result = validate_event_lake_output(tmp_path)

    assert result["valid"] is True
    assert result["logical_events"] == 2
    assert result["logical_tasks"] == 4
    assert result["physical_windows_expected"] == 2
    assert result["physical_windows_present"] == 2
    assert result["coverage_rows"] == 4
    assert result["ledger_records"] == 4
    assert result["ledger_logical_keys"] == 4
    assert result["decision_counts"] == {"WAIT": 4}
    assert result["coverage_status_counts"] == {"cache_reused": 2, "completed": 2}


def test_complete_validator_rejects_missing_physical_window(tmp_path: Path) -> None:
    build_duplicate_lake(tmp_path)
    next((tmp_path / "windows").glob("*.json")).unlink()

    with pytest.raises(ValueError, match="physical window file set mismatch"):
        validate_event_lake_output(tmp_path)


def test_in_progress_validator_accepts_consistent_subset(tmp_path: Path) -> None:
    build_duplicate_lake(tmp_path)
    window = next((tmp_path / "windows").glob("*.json"))
    removed_ref = f"windows/{window.name}"
    ledger_path = tmp_path / "ledger.jsonl"
    retained = [
        json.loads(line)
        for line in ledger_path.read_text(encoding="utf-8").splitlines()
        if str(json.loads(line).get("features_ref") or "").replace("\\", "/")
        != removed_ref
    ]
    ledger_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in retained),
        encoding="utf-8",
    )
    window.unlink()

    result = validate_event_lake_output(tmp_path, require_complete=False)

    assert result["logical_tasks"] == 4
    assert result["physical_windows_expected"] == 2
    assert result["physical_windows_present"] == 1
    assert result["ledger_records"] == 2
