from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from analysis_features.strategy_lab_event_lake import stable_hash, write_csv, write_json_atomic


MERGED_SCHEMA = "strategy_lab_merged_window_v1"
DATASETS = ("ohlcv", "funding", "open_interest", "long_short_ratio", "mark", "index", "premium")
RESOLUTION_RANK = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "event": 10_000, None: 10**9}


def merge_local_public_windows(
    local: Mapping[str, Any], public: Mapping[str, Any]
) -> dict[str, Any]:
    validate_identity(local, public)
    local_series = local.get("series") if isinstance(local.get("series"), Mapping) else {}
    public_series = public.get("series") if isinstance(public.get("series"), Mapping) else {}
    merged = {
        name: select_dataset(name, local_series.get(name), public_series.get(name))
        for name in DATASETS
    }
    identity = {
        key: public.get(key) for key in ("event_id", "symbol", "exchange", "start_ms", "end_ms")
    }
    return {
        "schema": MERGED_SCHEMA,
        **identity,
        "source_policy": "best_resolution_then_coverage_no_row_mixing",
        "network_calls": 0,
        "local_hash": stable_hash(local),
        "public_hash": stable_hash(public),
        "series": merged,
    }


def select_dataset(name: str, local: Any, public: Any) -> dict[str, Any]:
    candidates = [
        normalize_dataset("local_archive", local),
        normalize_dataset("public_cache", public),
    ]
    usable = [candidate for candidate in candidates if candidate["rows"]]
    selected: dict[str, Any] | None = None
    if usable:
        if name == "ohlcv":
            selected = min(
                usable,
                key=lambda item: (
                    RESOLUTION_RANK.get(item["resolution"], 10**8),
                    -len(item["rows"]),
                    0 if item["source"] == "local_archive" else 1,
                ),
            )
        else:
            selected = max(
                usable,
                key=lambda item: (
                    len(item["rows"]),
                    1 if item["source"] == "local_archive" else 0,
                ),
            )
    return {
        "selected_source": selected["source"] if selected else None,
        "resolution": selected["resolution"] if selected else None,
        "rows": selected["rows"] if selected else [],
        "selected_hash": stable_hash(selected["rows"]) if selected else None,
        "provenance": [
            {
                "source": candidate["source"],
                "rows": len(candidate["rows"]),
                "resolution": candidate["resolution"],
                "hash": stable_hash(candidate["rows"]),
                "error": candidate["error"],
            }
            for candidate in candidates
        ],
    }


def normalize_dataset(source: str, dataset: Any) -> dict[str, Any]:
    payload = dataset if isinstance(dataset, Mapping) else {}
    rows = [dict(row) for row in payload.get("rows") or [] if isinstance(row, Mapping)]
    resolution = payload.get("resolution")
    if source == "public_cache" and resolution is None and rows:
        resolution = "5m"
    return {
        "source": source,
        "rows": rows,
        "resolution": resolution,
        "error": str(payload.get("error") or ""),
    }


def validate_identity(local: Mapping[str, Any], public: Mapping[str, Any]) -> None:
    for key in ("event_id", "symbol", "exchange", "start_ms", "end_ms"):
        if local.get(key) != public.get(key):
            raise ValueError(f"local/public identity mismatch: {key}")


def run_merge_pilot(
    *, local_dir: Path, public_dir: Path, output_dir: Path
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    local = index_windows(local_dir)
    public = index_windows(public_dir)
    common = sorted(set(local) & set(public))
    coverage: list[dict[str, Any]] = []
    for key in common:
        merged = merge_local_public_windows(local[key], public[key])
        task_id = f"merged-{stable_hash(key)[:20]}"
        write_json_atomic(output_dir / "windows" / f"{task_id}.json", merged)
        coverage.append(
            {
                "event_id": key[0],
                "symbol": merged["symbol"],
                "exchange": key[1],
                **{
                    f"{name}_source": merged["series"][name]["selected_source"]
                    for name in DATASETS
                },
                **{
                    f"{name}_rows": len(merged["series"][name]["rows"])
                    for name in DATASETS
                },
            }
        )
    write_csv(output_dir / "coverage.csv", coverage)
    metadata = {
        "schema": "strategy_lab_merge_run_v1",
        "mode": "research_replay",
        "network_calls": 0,
        "local_windows": len(local),
        "public_windows": len(public),
        "merged_windows": len(common),
        "run_id": f"slab-merge-{stable_hash(common)[:16]}",
    }
    write_json_atomic(output_dir / "metadata.json", metadata)
    return metadata


def index_windows(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for item in sorted((path / "windows").glob("*.json")):
        payload = json.loads(item.read_text(encoding="utf-8"))
        key = (str(payload.get("event_id") or ""), str(payload.get("exchange") or ""))
        if all(key):
            if key in out:
                raise ValueError(f"duplicate window identity: {key}")
            out[key] = payload
    return out


__all__ = ["MERGED_SCHEMA", "merge_local_public_windows", "run_merge_pilot"]
