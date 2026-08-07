from __future__ import annotations

import csv
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from config import BASE_DIR

from analysis_features.strategy_lab_event_lake import (
    normalize_symbol,
    stable_hash,
    write_csv,
    write_json_atomic,
)


DEFAULT_ARCHIVE_ROOT = (
    BASE_DIR / "data" / "research" / "pump_short_multiexchange_2024_clean"
)
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "strategy_lab_local_archive"
ARCHIVE_INDEX_SCHEMA = "strategy_lab_local_archive_index_v1"
LOCAL_WINDOW_SCHEMA = "strategy_lab_local_window_v1"
EXCHANGES = ("binance", "bitget", "bybit", "kucoin", "mexc", "okx")


@dataclass(frozen=True, slots=True)
class LocalArchiveConfig:
    symbols: tuple[str, ...] = ("COTIUSDT", "HFTUSDT", "SIRENUSDT")
    exchanges: tuple[str, ...] = EXCHANGES
    pre_hours: int = 24
    post_hours: int = 72

    def validate(self) -> None:
        if not self.symbols:
            raise ValueError("at least one symbol is required")
        unsupported = sorted(set(self.exchanges) - set(EXCHANGES))
        if unsupported:
            raise ValueError(f"unsupported local exchanges: {unsupported}")
        if self.pre_hours < 0 or self.post_hours < 1:
            raise ValueError("invalid local event window")


def run_local_archive_pilot(
    *,
    events: Sequence[Mapping[str, Any]],
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    config: LocalArchiveConfig | None = None,
) -> dict[str, Any]:
    cfg = config or LocalArchiveConfig()
    cfg.validate()
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "archive_index.json"
    index, index_reused = build_or_load_archive_index(archive_root, index_path)
    reader = LocalArchiveReader(archive_root=archive_root, index=index)
    selected = select_events(events, cfg.symbols)
    if not selected:
        raise ValueError("no local-archive pilot events matched")
    windows_dir = output_dir / "windows"
    windows_dir.mkdir(parents=True, exist_ok=True)
    coverage: list[dict[str, Any]] = []
    for event in selected:
        event_ts = int(event.get("ts_ms") or 0)
        start_ms = event_ts - cfg.pre_hours * 3_600_000
        end_ms = event_ts + cfg.post_hours * 3_600_000
        for exchange in cfg.exchanges:
            task_identity = {
                "event_id": event.get("pump_event_id"),
                "symbol": normalize_symbol(event.get("symbol")),
                "exchange": exchange,
                "start_ms": start_ms,
                "end_ms": end_ms,
            }
            task_id = f"local-{stable_hash(task_identity)[:20]}"
            window = reader.read_window(
                exchange=exchange,
                symbol=str(task_identity["symbol"]),
                event_id=str(task_identity["event_id"]),
                start_ms=start_ms,
                end_ms=end_ms,
                task_id=task_id,
            )
            write_json_atomic(windows_dir / f"{task_id}.json", window)
            coverage.append(local_coverage_row(window, event_ts_ms=event_ts))
    write_csv(output_dir / "coverage.csv", coverage)
    inventory = inventory_archive_files(archive_root)
    write_csv(output_dir / "file_inventory.csv", inventory)
    config_hash = stable_hash(
        {
            "config": {
                "symbols": cfg.symbols,
                "exchanges": cfg.exchanges,
                "pre_hours": cfg.pre_hours,
                "post_hours": cfg.post_hours,
            },
            "index_source_hash": index["source_hash"],
            "events": [event.get("pump_event_id") for event in selected],
        }
    )
    metadata = {
        "schema": "strategy_lab_local_archive_run_v1",
        "mode": "research_replay",
        "network_calls": 0,
        "archive_root": str(archive_root),
        "index_schema": index["schema"],
        "index_source_hash": index["source_hash"],
        "index_reused": index_reused,
        "run_id": f"slab-local-{config_hash[:16]}",
        "events": len(selected),
        "tasks": len(coverage),
        "files": len(inventory),
        "archive_size_gib": round(
            sum(int(row["size_bytes"]) for row in inventory) / 1_073_741_824, 3
        ),
        "symbols_indexed": sum(int(item["rows"]) for item in index["exchanges"].values()),
        "available_tasks": sum(bool(row["symbol_available"]) for row in coverage),
        "ohlcv_tasks": sum(int(row["ohlcv_rows"]) > 0 for row in coverage),
        "funding_tasks": sum(int(row["funding_rows"]) > 0 for row in coverage),
        "oi_tasks": sum(int(row["oi_rows"]) > 0 for row in coverage),
        "long_short_tasks": sum(int(row["long_short_rows"]) > 0 for row in coverage),
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    write_json_atomic(output_dir / "metadata.json", metadata)
    (output_dir / "index.md").write_text(
        render_local_report(metadata=metadata, coverage=coverage), encoding="utf-8"
    )
    return metadata


def build_or_load_archive_index(
    archive_root: Path, index_path: Path
) -> tuple[dict[str, Any], bool]:
    cached = read_current_index(archive_root, index_path)
    if cached is not None:
        return cached, True
    index = build_archive_index(archive_root)
    write_json_atomic(index_path, index)
    return index, False


def build_archive_index(archive_root: Path) -> dict[str, Any]:
    if not archive_root.exists():
        raise FileNotFoundError(archive_root)
    exchange_entries: dict[str, Any] = {}
    source_identity: list[dict[str, Any]] = []
    for exchange in EXCHANGES:
        samples_path = archive_root / exchange / "symbol_samples.jsonl"
        summary_path = archive_root / exchange / "symbol_summary.csv"
        if not samples_path.exists() or not summary_path.exists():
            continue
        with summary_path.open("r", encoding="utf-8", newline="") as handle:
            summary_rows = list(csv.DictReader(handle))
        summary_symbols = [normalize_symbol(row.get("symbol")) for row in summary_rows]
        entries: dict[str, dict[str, int]] = {}
        digest = hashlib.sha256()
        with samples_path.open("rb") as handle:
            for line_number, symbol in enumerate(summary_symbols):
                offset = handle.tell()
                line = handle.readline()
                if not line:
                    raise ValueError(
                        f"archive rows shorter than summary for {exchange}: {line_number}"
                    )
                digest.update(line)
                entries[symbol] = {"offset": offset, "length": len(line)}
            if handle.readline():
                raise ValueError(f"archive rows longer than summary for {exchange}")
        samples_stat = samples_path.stat()
        summary_sha = file_sha256(summary_path)
        item = {
            "samples_path": str(samples_path.relative_to(archive_root)),
            "summary_path": str(summary_path.relative_to(archive_root)),
            "size_bytes": samples_stat.st_size,
            "mtime_ns": samples_stat.st_mtime_ns,
            "samples_sha256": digest.hexdigest(),
            "summary_sha256": summary_sha,
            "rows": len(entries),
            "entries": entries,
        }
        exchange_entries[exchange] = item
        source_identity.append(
            {
                "exchange": exchange,
                "samples_sha256": item["samples_sha256"],
                "summary_sha256": summary_sha,
                "rows": len(entries),
            }
        )
    return {
        "schema": ARCHIVE_INDEX_SCHEMA,
        "archive_root": str(archive_root.resolve()),
        "source_hash": stable_hash(source_identity),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "exchanges": exchange_entries,
    }


def read_current_index(archive_root: Path, index_path: Path) -> dict[str, Any] | None:
    if not index_path.exists():
        return None
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("schema") != ARCHIVE_INDEX_SCHEMA:
        return None
    if payload.get("archive_root") != str(archive_root.resolve()):
        return None
    for exchange, item in (payload.get("exchanges") or {}).items():
        samples_path = archive_root / str(item.get("samples_path") or "")
        summary_path = archive_root / str(item.get("summary_path") or "")
        if not samples_path.exists() or not summary_path.exists():
            return None
        stat = samples_path.stat()
        if stat.st_size != item.get("size_bytes") or stat.st_mtime_ns != item.get("mtime_ns"):
            return None
        if file_sha256(summary_path) != item.get("summary_sha256"):
            return None
        if exchange not in EXCHANGES:
            return None
    return payload


class LocalArchiveReader:
    def __init__(self, *, archive_root: Path, index: Mapping[str, Any]) -> None:
        self.archive_root = archive_root
        self.index = index

    def read_sample(self, exchange: str, symbol: str) -> dict[str, Any] | None:
        exchange_index = (self.index.get("exchanges") or {}).get(exchange)
        if not isinstance(exchange_index, Mapping):
            return None
        entry = (exchange_index.get("entries") or {}).get(normalize_symbol(symbol))
        if not isinstance(entry, Mapping):
            return None
        path = self.archive_root / str(exchange_index["samples_path"])
        with path.open("rb") as handle:
            handle.seek(int(entry["offset"]))
            raw = handle.read(int(entry["length"]))
        sample = json.loads(raw)
        if normalize_symbol(sample.get("symbol")) != normalize_symbol(symbol):
            raise ValueError(f"archive index symbol mismatch: {exchange} {symbol}")
        if str(sample.get("exchange") or "") != exchange:
            raise ValueError(f"archive index exchange mismatch: {exchange} {symbol}")
        return sample

    def read_window(
        self,
        *,
        exchange: str,
        symbol: str,
        event_id: str,
        start_ms: int,
        end_ms: int,
        task_id: str,
    ) -> dict[str, Any]:
        sample = self.read_sample(exchange, symbol)
        base = {
            "schema": LOCAL_WINDOW_SCHEMA,
            "task_id": task_id,
            "event_id": event_id,
            "symbol": normalize_symbol(symbol),
            "exchange": exchange,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "source": "local_multiexchange_archive",
            "network_calls": 0,
        }
        if sample is None:
            return {**base, "symbol_available": False, "market": {}, "series": empty_local_series()}
        series = sample.get("series") if isinstance(sample.get("series"), Mapping) else {}
        return {
            **base,
            "symbol_available": True,
            "market": {
                "exchange_symbol": sample.get("exchange_symbol"),
                "instrument": sample.get("instrument"),
                "sample_schema": sample.get("schema"),
            },
            "series": {
                "ohlcv": local_dataset(series.get("klines_1h"), start_ms, end_ms, "1h"),
                "funding": local_dataset(series.get("funding"), start_ms, end_ms, "event"),
                "open_interest": local_dataset(
                    series.get("open_interest_1h"), start_ms, end_ms, "1h"
                ),
                "long_short_ratio": local_dataset(
                    series.get("long_short_1h"), start_ms, end_ms, "1h"
                ),
                "mark": missing_local_dataset("not_present_in_archive_schema"),
                "index": missing_local_dataset("not_present_in_archive_schema"),
                "premium": missing_local_dataset("not_present_in_archive_schema"),
            },
        }


def local_dataset(rows: Any, start_ms: int, end_ms: int, resolution: str) -> dict[str, Any]:
    filtered = [
        dict(row)
        for row in rows or []
        if isinstance(row, Mapping)
        and row.get("ts_ms") is not None
        and start_ms <= int(row["ts_ms"]) < end_ms
    ]
    filtered.sort(key=lambda row: int(row["ts_ms"]))
    return {
        "available_in_schema": True,
        "source": "local_multiexchange_archive",
        "resolution": resolution,
        "rows": filtered,
        "error": "",
    }


def missing_local_dataset(reason: str) -> dict[str, Any]:
    return {
        "available_in_schema": False,
        "source": "local_multiexchange_archive",
        "resolution": None,
        "rows": [],
        "error": reason,
    }


def empty_local_series() -> dict[str, Any]:
    return {
        name: missing_local_dataset("symbol_unavailable")
        for name in (
            "ohlcv",
            "funding",
            "open_interest",
            "long_short_ratio",
            "mark",
            "index",
            "premium",
        )
    }


def local_coverage_row(window: Mapping[str, Any], *, event_ts_ms: int) -> dict[str, Any]:
    series = window.get("series") if isinstance(window.get("series"), Mapping) else {}
    counts = {
        name: len(dataset.get("rows") or [])
        for name, dataset in series.items()
        if isinstance(dataset, Mapping)
    }
    missing = [name for name, count in counts.items() if count == 0]
    return {
        "task_id": window.get("task_id"),
        "event_id": window.get("event_id"),
        "event_ts_ms": event_ts_ms,
        "symbol": window.get("symbol"),
        "exchange": window.get("exchange"),
        "symbol_available": window.get("symbol_available"),
        "ohlcv_rows": counts.get("ohlcv", 0),
        "funding_rows": counts.get("funding", 0),
        "oi_rows": counts.get("open_interest", 0),
        "long_short_rows": counts.get("long_short_ratio", 0),
        "mark_rows": counts.get("mark", 0),
        "index_rows": counts.get("index", 0),
        "premium_rows": counts.get("premium", 0),
        "missing_datasets": ",".join(missing),
        "network_calls": 0,
    }


def select_events(
    events: Sequence[Mapping[str, Any]], symbols: Sequence[str]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for requested in (normalize_symbol(symbol) for symbol in symbols):
        matches = [
            dict(event)
            for event in events
            if normalize_symbol(event.get("symbol")) == requested
        ]
        if matches:
            out.append(max(matches, key=lambda event: int(event.get("ts_ms") or 0)))
    return out


def inventory_archive_files(archive_root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(item for item in archive_root.rglob("*") if item.is_file()):
        relative = path.relative_to(archive_root)
        family = "comparison" if relative.parts[0] == "_comparison" else relative.parts[0]
        out.append(
            {
                "relative_path": str(relative),
                "family": family,
                "extension": path.suffix.lower(),
                "size_bytes": path.stat().st_size,
                "role": classify_archive_file(path.name),
            }
        )
    return out


def classify_archive_file(name: str) -> str:
    if name == "symbol_samples.jsonl":
        return "per_symbol_timeseries"
    if name == "symbol_summary.csv":
        return "per_symbol_summary"
    if name == "instruments_latest.json":
        return "instrument_metadata"
    if name == "collection_metadata.json":
        return "collection_metadata"
    if name == "outcomes.csv":
        return "strategy_outcomes"
    if name == "events.csv":
        return "pump_events"
    return "supporting_artifact"


def render_local_report(
    *, metadata: Mapping[str, Any], coverage: Sequence[Mapping[str, Any]]
) -> str:
    lines = [
        "# Strategy Lab local archive pilot",
        "",
        "Status: local research replay, zero network calls, no live changes.",
        "",
        f"- Run: `{metadata.get('run_id')}`",
        f"- Archive: {metadata.get('files')} files / {metadata.get('archive_size_gib')} GiB",
        f"- Indexed symbols: {metadata.get('symbols_indexed')}",
        f"- Events/tasks: {metadata.get('events')} / {metadata.get('tasks')}",
        f"- Index reused: {metadata.get('index_reused')}",
        "",
        "| Symbol | Exchange | Listed | OHLCV 1h | Funding | OI | L/S | Missing |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in coverage:
        lines.append(
            "| {symbol} | {exchange} | {symbol_available} | {ohlcv_rows} | "
            "{funding_rows} | {oi_rows} | {long_short_rows} | {missing_datasets} |".format(
                **row
            )
        )
    lines.append("")
    return "\n".join(lines)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1_048_576):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ARCHIVE_INDEX_SCHEMA",
    "LOCAL_WINDOW_SCHEMA",
    "LocalArchiveConfig",
    "LocalArchiveReader",
    "build_archive_index",
    "build_or_load_archive_index",
    "run_local_archive_pilot",
]
