from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from analysis_features.strategy_lab_event_lake import (
    EventLakeConfig,
    append_jsonl_once,
    build_enrichment_manifest,
    estimate_full_catalog_run,
    fetch_derived_price_pages,
    fetch_oi_pages,
    fetch_ohlcv_pages,
    run_event_lake,
    select_catalog_events,
    validate_ledger_record,
)


BASE_TS = 1_767_225_600_000


def event(symbol: str, ts_ms: int, source: str = "pump_lifecycle") -> dict[str, Any]:
    return {
        "pump_event_id": f"{source}|{symbol}|{ts_ms}",
        "source": source,
        "event_type": "lifecycle_trigger",
        "symbol": symbol,
        "ts_ms": ts_ms,
        "ts_iso": "2026-01-01T00:00:00+00:00",
    }


class FakePublicProvider:
    public_only = True

    def __init__(self, *, market_available: bool = True) -> None:
        self.calls: list[str] = []
        self.market_available = market_available

    def fetch_window(self, task: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append(str(task["task_id"]))
        rows = (
            [
                {
                    "ts_ms": int(task["start_ms"]),
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                    "volume": 100.0,
                }
            ]
            if self.market_available
            else []
        )
        missing_error = "" if self.market_available else "symbol_unavailable"
        return {
            "schema": "strategy_lab_public_window_v3",
            "task_id": task["task_id"],
            "event_id": task["event_id"],
            "symbol": task["symbol"],
            "exchange": task["exchange"],
            "start_ms": task["start_ms"],
            "end_ms": task["end_ms"],
            "timeframe": task["timeframe"],
            "public_only": True,
            "market": {
                "available": self.market_available,
                "exchange_symbol": "TEST/USDT:USDT" if self.market_available else None,
                "market_id": "TESTUSDT" if self.market_available else None,
                "contract_size": 1.0 if self.market_available else None,
            },
            "series": {
                "ohlcv": {
                    "supported": True,
                    "calls": 1,
                    "rows": rows,
                    "error": missing_error,
                },
                "funding": {"supported": True, "calls": 1, "rows": [], "error": ""},
                "open_interest": {
                    "supported": True,
                    "calls": 1,
                    "rows": [],
                    "error": "",
                },
                "mark": {"supported": False, "calls": 0, "rows": [], "error": "unsupported"},
                "index": {"supported": False, "calls": 0, "rows": [], "error": "unsupported"},
                "premium": {
                    "supported": False,
                    "calls": 0,
                    "rows": [],
                    "error": "unsupported",
                },
            },
        }


def test_manifest_is_deterministic_and_estimates_bounded_calls() -> None:
    config = EventLakeConfig(
        exchanges=("binance", "bybit"),
        symbols=("AUSDT", "BUSDT", "CUSDT"),
        max_events=3,
    )
    events = [event("AUSDT", BASE_TS), event("BUSDT", BASE_TS), event("CUSDT", BASE_TS)]

    first = build_enrichment_manifest(events, config, code_commit="abc")
    second = build_enrichment_manifest(events, config, code_commit="different")

    assert first["run_id"] == second["run_id"]
    assert first["source_manifest_hash"] == second["source_manifest_hash"]
    assert len(first["tasks"]) == 6
    assert first["estimated_public_calls"] == 114
    assert {task["expected_candles"] for task in first["tasks"]} == {1152}


def test_selection_uses_latest_event_per_requested_symbol_in_requested_order() -> None:
    rows = [
        event("AUSDT", BASE_TS),
        event("AUSDT", BASE_TS + 1000, "pump_universe_hourly_spike"),
        event("BUSDT", BASE_TS + 2000),
    ]

    selected = select_catalog_events(rows, symbols=("BUSDT", "AUSDT"), max_events=2)

    assert [(row["symbol"], row["ts_ms"]) for row in selected] == [
        ("BUSDT", BASE_TS + 2000),
        ("AUSDT", BASE_TS + 1000),
    ]


def test_run_is_resumable_and_does_not_duplicate_ledger(tmp_path: Path) -> None:
    config = EventLakeConfig(exchanges=("binance", "bybit"), max_events=2)
    rows = [event("AUSDT", BASE_TS), event("BUSDT", BASE_TS + 1000)]
    first_provider = FakePublicProvider()

    first = run_event_lake(
        output_dir=tmp_path,
        config=config,
        catalog_rows=rows,
        execute_public=True,
        provider=first_provider,
        code_commit="abc",
    )
    first_lines = (tmp_path / "ledger.jsonl").read_text(encoding="utf-8").splitlines()
    second_provider = FakePublicProvider()
    second = run_event_lake(
        output_dir=tmp_path,
        config=config,
        catalog_rows=rows,
        execute_public=True,
        provider=second_provider,
        code_commit="abc",
    )
    second_lines = (tmp_path / "ledger.jsonl").read_text(encoding="utf-8").splitlines()

    assert first["status_counts"] == {"completed": 4}
    assert first["source_public_calls"] == 12
    assert first["public_calls_this_run"] == 12
    assert len(first_provider.calls) == 4
    assert len(first_lines) == 4
    assert second["status_counts"] == {"cache_reused": 4}
    assert second["source_public_calls"] == 12
    assert second["public_calls_this_run"] == 0
    assert second_provider.calls == []
    assert second_lines == first_lines
    assert len({json.loads(line)["record_id"] for line in second_lines}) == 4
    assert "public-only research replay" in (tmp_path / "index.md").read_text(
        encoding="utf-8"
    )

    cache_path = next((tmp_path / "windows").glob("*.json"))
    corrupted = json.loads(cache_path.read_text(encoding="utf-8"))
    corrupted["symbol"] = "WRONGUSDT"
    cache_path.write_text(json.dumps(corrupted), encoding="utf-8")
    repair_provider = FakePublicProvider()
    repaired = run_event_lake(
        output_dir=tmp_path,
        config=config,
        catalog_rows=rows,
        execute_public=True,
        provider=repair_provider,
        code_commit="abc",
    )

    assert repaired["status_counts"] == {"completed": 1, "cache_reused": 3}
    assert len(repair_provider.calls) == 1
    assert len((tmp_path / "ledger.jsonl").read_text(encoding="utf-8").splitlines()) == 4


def test_missing_market_creates_fail_closed_veto(tmp_path: Path) -> None:
    run_event_lake(
        output_dir=tmp_path,
        config=EventLakeConfig(exchanges=("binance",), max_events=1),
        catalog_rows=[event("MISSINGUSDT", BASE_TS)],
        execute_public=True,
        provider=FakePublicProvider(market_available=False),
        code_commit="abc",
    )

    record = json.loads((tmp_path / "ledger.jsonl").read_text(encoding="utf-8"))
    assert record["decision"] == "VETO"
    assert record["veto_reasons"] == ["market_unavailable", "ohlcv_missing"]


def test_non_public_provider_and_live_ledger_are_rejected(tmp_path: Path) -> None:
    provider = FakePublicProvider()
    provider.public_only = False
    with pytest.raises(ValueError, match="public_only"):
        run_event_lake(
            output_dir=tmp_path,
            config=EventLakeConfig(exchanges=("binance",), max_events=1),
            catalog_rows=[event("AUSDT", BASE_TS)],
            execute_public=True,
            provider=provider,
            code_commit="abc",
        )

    with pytest.raises(ValueError, match="invalid ledger mode"):
        validate_ledger_record(
            {
                "schema": "strategy_lab_ledger_v1",
                "run_id": "r",
                "record_id": "x",
                "record_type": "decision",
                "mode": "live",
                "event_id": "e",
                "hypothesis_id": "h",
                "symbol": "AUSDT",
                "source_ts_ms": BASE_TS,
                "decision": "ENTER",
                "features_hash": "f",
                "code_commit": "c",
                "config_hash": "c",
                "source_manifest_hash": "s",
            }
        )


def test_ohlcv_pagination_is_bounded_and_deduplicated() -> None:
    class FakeClient:
        has = {"fetchOHLCV": True}

        def __init__(self) -> None:
            self.calls = 0
            self.rows = [
                [BASE_TS + index * 300_000, 1, 2, 0.5, 1.5, 10]
                for index in range(5)
            ]

        def fetch_ohlcv(
            self, symbol: str, timeframe: str, *, since: int, limit: int
        ) -> list[list[float]]:
            del symbol, timeframe
            self.calls += 1
            return [row for row in self.rows if row[0] >= since][:limit]

    client = FakeClient()
    result = fetch_ohlcv_pages(
        client,
        symbol="A/USDT:USDT",
        timeframe="5m",
        start_ms=BASE_TS,
        end_ms=BASE_TS + 5 * 300_000,
        limit=2,
    )

    assert client.calls == 3
    assert result["calls"] == 3
    assert len(result["rows"]) == 5
    assert [row["ts_ms"] for row in result["rows"]] == sorted(
        row["ts_ms"] for row in result["rows"]
    )


def test_derived_price_pagination_uses_confirmed_price_kind() -> None:
    class FakeClient:
        has = {"fetchOHLCV": True}

        def __init__(self) -> None:
            self.params: list[dict[str, str]] = []
            self.rows = [
                [BASE_TS + index * 300_000, 1, 2, 0.5, 1.5, 0]
                for index in range(3)
            ]

        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            *,
            since: int,
            limit: int,
            params: dict[str, str],
        ) -> list[list[float]]:
            del symbol, timeframe
            self.params.append(params)
            return [row for row in self.rows if row[0] >= since][:limit]

    client = FakeClient()
    result = fetch_derived_price_pages(
        client,
        symbol="A/USDT:USDT",
        timeframe="5m",
        start_ms=BASE_TS,
        end_ms=BASE_TS + 3 * 300_000,
        limit=2,
        price_kind="premiumIndex",
    )

    assert len(result["rows"]) == 3
    assert result["endpoint_kind"] == "premiumIndex"
    assert client.params == [{"price": "premiumIndex"}, {"price": "premiumIndex"}]


def test_binance_oi_retention_gap_skips_network_call() -> None:
    class FakeClient:
        id = "binanceusdm"
        has = {"fetchOpenInterestHistory": True}

        def fetch_open_interest_history(self, *args: Any, **kwargs: Any) -> list[Any]:
            raise AssertionError("retention gap must not call Binance")

    result = fetch_oi_pages(
        FakeClient(),
        symbol="A/USDT:USDT",
        timeframe="5m",
        start_ms=BASE_TS,
        end_ms=BASE_TS + 300_000,
        limit=200,
        now_ms=BASE_TS + 31 * 86_400_000,
    )

    assert result["supported"] is True
    assert result["request_skipped"] is True
    assert result["calls"] == 0
    assert result["error"] == "retention_gap:binance_open_interest_latest_1_month"


def test_bybit_oi_pages_backwards_with_bounded_end_time() -> None:
    class FakeClient:
        id = "bybit"
        has = {"fetchOpenInterestHistory": True}

        def __init__(self) -> None:
            self.until: list[int] = []
            self.rows = [
                {"timestamp": BASE_TS + index * 300_000, "openInterestAmount": 10 + index}
                for index in range(5)
            ]

        def fetch_open_interest_history(
            self,
            symbol: str,
            timeframe: str,
            *,
            since: int,
            limit: int,
            params: dict[str, int],
        ) -> list[dict[str, Any]]:
            del symbol, timeframe
            self.until.append(params["until"])
            eligible = [row for row in self.rows if since <= row["timestamp"] <= params["until"]]
            return list(reversed(eligible[-limit:]))

    client = FakeClient()
    result = fetch_oi_pages(
        client,
        symbol="A/USDT:USDT",
        timeframe="5m",
        start_ms=BASE_TS,
        end_ms=BASE_TS + 5 * 300_000,
        limit=2,
        now_ms=BASE_TS + 100 * 86_400_000,
    )

    assert len(result["rows"]) == 5
    assert result["calls"] == 3
    assert client.until == [BASE_TS + 5 * 300_000 - 1, BASE_TS + 3 * 300_000 - 1, BASE_TS + 300_000 - 1]
    assert [row["ts_ms"] for row in result["rows"]] == sorted(
        row["ts_ms"] for row in result["rows"]
    )


def test_ledger_append_is_cross_process_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "ledger.jsonl"
    payload = {
        "schema": "strategy_lab_ledger_v1",
        "run_id": "run-one",
        "record_id": "record-one",
        "record_type": "enrichment_result",
        "mode": "research_replay",
        "event_id": "event-one",
        "hypothesis_id": "hypothesis-one",
        "symbol": "AUSDT",
        "source_ts_ms": BASE_TS,
        "decision": "WAIT",
        "features_hash": "features-one",
        "code_commit": "commit-one",
        "config_hash": "config-one",
        "source_manifest_hash": "source-one",
    }

    assert append_jsonl_once(path, payload) is True
    assert append_jsonl_once(path, payload) is False
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
    assert not (tmp_path / ".ledger.jsonl.lock").exists()


def test_full_run_estimate_separates_logical_and_physical_windows() -> None:
    rows = [
        event("AUSDT", BASE_TS),
        event("AUSDT", BASE_TS, "pump_universe_hourly_spike"),
        event("BUSDT", BASE_TS + 300_000),
    ]
    config = EventLakeConfig(exchanges=("binance", "bybit"), max_events=1)

    estimate = estimate_full_catalog_run(
        rows,
        config,
        as_of_ms=BASE_TS + 40 * 86_400_000,
        average_window_bytes=1000,
        pilot_calls=32,
        pilot_elapsed_sec=16,
    )

    assert estimate["logical_events"] == 3
    assert estimate["unique_symbol_timestamp_windows"] == 2
    assert estimate["logical_tasks_without_window_dedupe"] == 6
    assert estimate["physical_tasks_with_exact_window_dedupe"] == 4
    assert estimate["estimated_calls_with_exact_window_dedupe"] == 64
    assert estimate["estimated_calls_without_window_dedupe"] == 96
    assert estimate["estimated_disk_bytes_with_exact_window_dedupe"] == 4000
    assert estimate["estimated_runtime_sec_with_exact_window_dedupe"] == 32
