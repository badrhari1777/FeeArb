from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from analysis_features.strategy_lab_local_archive import (
    LocalArchiveConfig,
    LocalArchiveReader,
    build_archive_index,
    build_or_load_archive_index,
    run_local_archive_pilot,
)


BASE_TS = 1_767_225_600_000


def make_archive(root: Path, *, summary_symbols: tuple[str, ...] = ("AUSDT", "BUSDT")) -> None:
    exchange_dir = root / "binance"
    exchange_dir.mkdir(parents=True)
    samples = [sample("AUSDT", 1.0), sample("BUSDT", 2.0)]
    with (exchange_dir / "symbol_samples.jsonl").open("w", encoding="utf-8") as handle:
        for row in samples:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    with (exchange_dir / "symbol_summary.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=["exchange", "symbol"])
        writer.writeheader()
        for symbol in summary_symbols:
            writer.writerow({"exchange": "binance", "symbol": symbol})


def sample(symbol: str, price: float) -> dict[str, object]:
    return {
        "exchange": "binance",
        "exchange_symbol": f"{symbol[:-4]}/USDT:USDT",
        "instrument": {"contract_size": 1.0},
        "schema": "ccxt_pump_short_collection_v1",
        "series": {
            "klines_1h": [
                {
                    "ts_ms": BASE_TS - 3_600_000,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 10,
                },
                {
                    "ts_ms": BASE_TS + 3_600_000,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 20,
                },
            ],
            "funding": [{"ts_ms": BASE_TS, "funding_rate": -0.01}],
            "open_interest_1h": [{"ts_ms": BASE_TS, "open_interest": 100}],
            "long_short_1h": [{"ts_ms": BASE_TS, "long_ratio": 0.4}],
        },
        "summary": {},
        "symbol": symbol,
        "ts_ms": BASE_TS,
    }


def test_index_supports_byte_offset_reads_and_reuse(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    make_archive(archive)
    index_path = tmp_path / "index.json"

    index, reused = build_or_load_archive_index(archive, index_path)
    second, second_reused = build_or_load_archive_index(archive, index_path)
    reader = LocalArchiveReader(archive_root=archive, index=second)
    loaded = reader.read_sample("binance", "BUSDT")

    assert reused is False
    assert second_reused is True
    assert index["source_hash"] == second["source_hash"]
    assert loaded is not None
    assert loaded["symbol"] == "BUSDT"


def test_window_is_causal_and_preserves_missing_fields(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    make_archive(archive)
    index = build_archive_index(archive)
    reader = LocalArchiveReader(archive_root=archive, index=index)

    window = reader.read_window(
        exchange="binance",
        symbol="AUSDT",
        event_id="event-a",
        start_ms=BASE_TS,
        end_ms=BASE_TS + 2 * 3_600_000,
        task_id="task-a",
    )

    assert len(window["series"]["ohlcv"]["rows"]) == 1
    assert window["series"]["ohlcv"]["rows"][0]["ts_ms"] == BASE_TS + 3_600_000
    assert len(window["series"]["funding"]["rows"]) == 1
    assert len(window["series"]["open_interest"]["rows"]) == 1
    assert window["series"]["mark"]["rows"] == []
    assert window["series"]["mark"]["available_in_schema"] is False


def test_full_local_pilot_is_zero_network_and_deterministic(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    make_archive(archive)
    output = tmp_path / "output"
    events = [
        {
            "pump_event_id": "pump|AUSDT|1",
            "symbol": "AUSDT",
            "ts_ms": BASE_TS,
        }
    ]
    config = LocalArchiveConfig(
        symbols=("AUSDT",), exchanges=("binance",), pre_hours=1, post_hours=2
    )

    first = run_local_archive_pilot(
        events=events, archive_root=archive, output_dir=output, config=config
    )
    second = run_local_archive_pilot(
        events=events, archive_root=archive, output_dir=output, config=config
    )

    assert first["network_calls"] == 0
    assert first["index_reused"] is False
    assert second["index_reused"] is True
    assert first["run_id"] == second["run_id"]
    assert first["tasks"] == 1
    assert first["ohlcv_tasks"] == 1
    assert "zero network calls" in (output / "index.md").read_text(encoding="utf-8")


def test_index_detects_summary_jsonl_identity_mismatch(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    make_archive(archive, summary_symbols=("BUSDT", "AUSDT"))
    index = build_archive_index(archive)
    reader = LocalArchiveReader(archive_root=archive, index=index)

    with pytest.raises(ValueError, match="symbol mismatch"):
        reader.read_sample("binance", "AUSDT")
