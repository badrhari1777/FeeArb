from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import subprocess
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from config import BASE_DIR

from analysis_features.strategy_lab import (
    PUBLIC_EXCHANGE_IDS,
    PUMP_EVENT_SOURCES,
    load_pump_event_catalog,
)


DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "strategy_lab_event_lake"
LEDGER_SCHEMA = "strategy_lab_ledger_v1"
MANIFEST_SCHEMA = "strategy_lab_enrichment_manifest_v1"
WINDOW_SCHEMA = "strategy_lab_public_window_v3"
ALLOWED_MODES = {"research_replay", "paper", "shadow"}
ALLOWED_DECISIONS = {"VETO", "WAIT", "ENTER", "HOLD", "EXIT", "RISK_EXIT"}
BINANCE_OI_RETENTION_MS = 30 * 86_400_000
DERIVED_PRICE_KINDS = {
    "mark": "mark",
    "index": "index",
    "premium": "premiumIndex",
}


@dataclass(frozen=True, slots=True)
class EventLakeConfig:
    exchanges: tuple[str, ...] = ("binance", "bybit")
    symbols: tuple[str, ...] = ()
    max_events: int = 3
    pre_hours: int = 24
    post_hours: int = 72
    timeframe: str = "5m"
    request_limit: int = 500
    mode: str = "research_replay"
    hypothesis_id: str = "pump_to_arbitrage_bridge"

    def validate(self) -> None:
        if self.mode not in ALLOWED_MODES:
            raise ValueError(f"unsupported mode: {self.mode}")
        if self.max_events < 1:
            raise ValueError("max_events must be positive")
        if self.pre_hours < 0 or self.post_hours < 1:
            raise ValueError("event window must include positive post history")
        if self.request_limit < 50 or self.request_limit > 1000:
            raise ValueError("request_limit must be within 50..1000")
        unsupported = sorted(set(self.exchanges) - set(PUBLIC_EXCHANGE_IDS))
        if unsupported:
            raise ValueError(f"unsupported exchanges: {unsupported}")
        timeframe_ms(self.timeframe)


class PublicEventProvider(Protocol):
    public_only: bool

    def fetch_window(self, task: Mapping[str, Any]) -> dict[str, Any]: ...


def run_event_lake(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    config: EventLakeConfig | None = None,
    catalog_rows: Sequence[Mapping[str, Any]] | None = None,
    execute_public: bool = False,
    provider: PublicEventProvider | None = None,
    code_commit: str | None = None,
) -> dict[str, Any]:
    """Create a bounded, resumable public-data event-lake run.

    No exchange client is created during preflight. Execution accepts only a
    provider that explicitly declares ``public_only=True``.
    """

    cfg = config or EventLakeConfig()
    cfg.validate()
    started = time.time()
    events = (
        [dict(row) for row in catalog_rows]
        if catalog_rows is not None
        else load_pump_event_catalog(PUMP_EVENT_SOURCES)
    )
    selected = select_catalog_events(events, symbols=cfg.symbols, max_events=cfg.max_events)
    if not selected:
        raise ValueError("no Pump events matched the Event Lake selection")
    commit = code_commit or current_git_commit()
    manifest = build_enrichment_manifest(selected, cfg, code_commit=commit)
    output_dir.mkdir(parents=True, exist_ok=True)
    windows_dir = output_dir / "windows"
    windows_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = output_dir / "ledger.jsonl"
    existing_record_ids = read_ledger_record_ids(ledger_path)

    active_provider = provider
    if execute_public:
        active_provider = active_provider or CcxtPublicEventProvider(
            request_limit=cfg.request_limit,
            timeframe=cfg.timeframe,
        )
        if not getattr(active_provider, "public_only", False):
            raise ValueError("Event Lake refuses a provider that is not public_only")

    coverage: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    for task in manifest["tasks"]:
        cache_path = windows_dir / f"{task['task_id']}.json"
        window: dict[str, Any] | None = read_valid_cache(cache_path, task)
        cache_reused = window is not None
        if not execute_public:
            task["status"] = "planned"
            coverage.append(coverage_from_task(task, None, cache_reused=False))
        else:
            if window is None:
                assert active_provider is not None
                window = active_provider.fetch_window(task)
                validate_window(window, task)
                write_json_atomic(cache_path, window)
            task["status"] = "cache_reused" if cache_reused else window_status(window)
            task["cache_path"] = str(cache_path.relative_to(output_dir))
            row = coverage_from_task(task, window, cache_reused=cache_reused)
            coverage.append(row)
            record = build_ledger_record(
                manifest=manifest,
                task=task,
                window=window,
                coverage=row,
                code_commit=commit,
            )
            validate_ledger_record(record)
            if record["record_id"] not in existing_record_ids:
                append_jsonl_once(ledger_path, record)
                existing_record_ids.add(str(record["record_id"]))
        status = str(task["status"])
        status_counts[status] = status_counts.get(status, 0) + 1

    manifest["status_counts"] = status_counts
    manifest["executed_public"] = bool(execute_public)
    write_json_atomic(output_dir / "manifest.json", manifest)
    write_csv(output_dir / "coverage.csv", coverage)
    metadata = {
        "schema": "strategy_lab_event_lake_run_v1",
        "mode": cfg.mode,
        "public_only": True,
        "run_id": manifest["run_id"],
        "config_hash": manifest["config_hash"],
        "source_manifest_hash": manifest["source_manifest_hash"],
        "code_commit": commit,
        "selected_events": len(selected),
        "tasks": len(manifest["tasks"]),
        "estimated_public_calls": manifest["estimated_public_calls"],
        "source_public_calls": sum(int(row.get("source_public_calls") or 0) for row in coverage),
        "public_calls_this_run": sum(
            int(row.get("public_calls_this_run") or 0) for row in coverage
        ),
        "executed_public": bool(execute_public),
        "status_counts": status_counts,
        "ledger_records": len(existing_record_ids),
        "coverage_rows": len(coverage),
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    write_json_atomic(output_dir / "metadata.json", metadata)
    (output_dir / "index.md").write_text(
        render_event_lake_report(metadata=metadata, coverage=coverage),
        encoding="utf-8",
    )
    return metadata


def select_catalog_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    symbols: Sequence[str],
    max_events: int,
) -> list[dict[str, Any]]:
    requested = [normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)]
    requested_order = {symbol: index for index, symbol in enumerate(requested)}
    candidates = [
        dict(row)
        for row in rows
        if not requested or normalize_symbol(row.get("symbol")) in requested_order
    ]
    source_priority = {
        "pump_lifecycle": 0,
        "pump_universe_hourly_spike": 1,
        "pump_premium_window": 2,
        "pump_live_like_trade_cases": 3,
    }
    candidates.sort(
        key=lambda row: (
            requested_order.get(normalize_symbol(row.get("symbol")), 10**6),
            -int(row.get("ts_ms") or 0),
            source_priority.get(str(row.get("source") or ""), 99),
            str(row.get("pump_event_id") or ""),
        )
    )
    selected: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for row in candidates:
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol or symbol in seen_symbols:
            continue
        selected.append(row)
        seen_symbols.add(symbol)
        if len(selected) >= max_events:
            break
    if not requested:
        selected.sort(key=lambda row: int(row.get("ts_ms") or 0), reverse=True)
    return selected


def build_enrichment_manifest(
    events: Sequence[Mapping[str, Any]],
    config: EventLakeConfig,
    *,
    code_commit: str,
) -> dict[str, Any]:
    config.validate()
    canonical_events = [
        {
            "event_id": str(row.get("pump_event_id") or ""),
            "event_type": str(row.get("event_type") or ""),
            "source": str(row.get("source") or ""),
            "symbol": normalize_symbol(row.get("symbol")),
            "ts_ms": int(row.get("ts_ms") or 0),
        }
        for row in events
    ]
    source_hash = stable_hash(canonical_events)
    config_payload = asdict(config)
    config_hash = stable_hash(config_payload)
    run_id = f"slab-event-lake-{stable_hash({'events': source_hash, 'config': config_hash})[:16]}"
    tasks: list[dict[str, Any]] = []
    tf_ms = timeframe_ms(config.timeframe)
    for event in canonical_events:
        start_ms = event["ts_ms"] - config.pre_hours * 3_600_000
        end_ms = event["ts_ms"] + config.post_hours * 3_600_000
        expected_candles = max(0, math.ceil((end_ms - start_ms) / tf_ms))
        expected_pages = max(1, math.ceil(expected_candles / config.request_limit))
        expected_oi_pages = max(
            1,
            math.ceil(expected_candles / min(config.request_limit, 200)),
        )
        for exchange in config.exchanges:
            identity = {
                "event_id": event["event_id"],
                "exchange": exchange,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "timeframe": config.timeframe,
            }
            tasks.append(
                {
                    "task_id": f"task-{stable_hash(identity)[:20]}",
                    **identity,
                    "event_type": event["event_type"],
                    "source": event["source"],
                    "symbol": event["symbol"],
                    "event_ts_ms": event["ts_ms"],
                    "expected_candles": expected_candles,
                    "estimated_calls": {
                        "ohlcv": expected_pages,
                        "funding": 1,
                        "open_interest": expected_oi_pages,
                        "mark": expected_pages,
                        "index": expected_pages,
                        "premium": expected_pages,
                        "total": expected_pages * 4 + expected_oi_pages + 1,
                    },
                    "status": "planned",
                }
            )
    return {
        "schema": MANIFEST_SCHEMA,
        "mode": config.mode,
        "public_only": True,
        "run_id": run_id,
        "source_manifest_hash": source_hash,
        "config_hash": config_hash,
        "code_commit": code_commit,
        "config": config_payload,
        "events": canonical_events,
        "tasks": tasks,
        "estimated_public_calls": sum(task["estimated_calls"]["total"] for task in tasks),
    }


def estimate_full_catalog_run(
    rows: Sequence[Mapping[str, Any]],
    config: EventLakeConfig,
    *,
    as_of_ms: int,
    average_window_bytes: float | None = None,
    pilot_calls: int | None = None,
    pilot_elapsed_sec: float | None = None,
) -> dict[str, Any]:
    """Estimate the full catalog without constructing clients or making requests."""

    config.validate()
    logical_events = {
        str(row.get("pump_event_id") or ""): (
            normalize_symbol(row.get("symbol")),
            int(row.get("ts_ms") or 0),
        )
        for row in rows
        if row.get("pump_event_id") and normalize_symbol(row.get("symbol"))
    }
    physical_events = set(logical_events.values())
    tf_ms = timeframe_ms(config.timeframe)
    expected_candles = math.ceil(
        (config.pre_hours + config.post_hours) * 3_600_000 / tf_ms
    )
    price_pages = max(1, math.ceil(expected_candles / config.request_limit))
    oi_pages = max(1, math.ceil(expected_candles / min(config.request_limit, 200)))
    def calls_for_events(exchange: str, events: Sequence[tuple[str, int]]) -> int:
        total = 0
        for _, event_ts_ms in events:
            end_ms = event_ts_ms + config.post_hours * 3_600_000
            oi_calls = oi_pages
            if exchange == "binance" and end_ms <= as_of_ms - BINANCE_OI_RETENTION_MS:
                oi_calls = 0
            total += price_pages * 4 + oi_calls + 1
        return total

    physical_event_rows = sorted(physical_events)
    logical_event_rows = list(logical_events.values())
    calls_per_exchange = {
        exchange: calls_for_events(exchange, physical_event_rows)
        for exchange in config.exchanges
    }
    calls_without_dedupe_per_exchange = {
        exchange: calls_for_events(exchange, logical_event_rows)
        for exchange in config.exchanges
    }
    exact_dedupe_calls = sum(calls_per_exchange.values())
    no_dedupe_calls = sum(calls_without_dedupe_per_exchange.values())
    physical_tasks = len(physical_events) * len(config.exchanges)
    logical_tasks = len(logical_events) * len(config.exchanges)
    estimate = {
        "schema": "strategy_lab_full_run_estimate_v1",
        "public_only": True,
        "as_of_ms": int(as_of_ms),
        "logical_events": len(logical_events),
        "unique_symbol_timestamp_windows": len(physical_events),
        "duplicate_logical_windows": len(logical_events) - len(physical_events),
        "logical_tasks_without_window_dedupe": logical_tasks,
        "physical_tasks_with_exact_window_dedupe": physical_tasks,
        "expected_candles_per_task": expected_candles,
        "estimated_calls_per_task": {
            "binance_retention_gap": price_pages * 4 + 1,
            "bybit_with_oi": price_pages * 4 + oi_pages + 1,
            "worst_case": price_pages * 4 + oi_pages + 1,
        },
        "estimated_calls_without_window_dedupe": no_dedupe_calls,
        "estimated_calls_with_exact_window_dedupe": exact_dedupe_calls,
        "estimated_calls_by_exchange_with_dedupe": calls_per_exchange,
        "estimated_calls_by_exchange_without_dedupe": calls_without_dedupe_per_exchange,
        "binance_oi_retention_policy": "latest_1_month_conservative_30d",
    }
    if average_window_bytes is not None and average_window_bytes > 0:
        estimate["estimated_disk_bytes_without_window_dedupe"] = round(
            logical_tasks * average_window_bytes
        )
        estimate["estimated_disk_bytes_with_exact_window_dedupe"] = round(
            physical_tasks * average_window_bytes
        )
    if pilot_calls and pilot_elapsed_sec is not None and pilot_elapsed_sec > 0:
        seconds_per_call = pilot_elapsed_sec / pilot_calls
        estimate["pilot_seconds_per_call"] = round(seconds_per_call, 6)
        estimate["estimated_runtime_sec_without_window_dedupe"] = round(
            no_dedupe_calls * seconds_per_call
        )
        estimate["estimated_runtime_sec_with_exact_window_dedupe"] = round(
            exact_dedupe_calls * seconds_per_call
        )
    return estimate


class CcxtPublicEventProvider:
    public_only = True

    def __init__(
        self,
        *,
        request_limit: int,
        timeframe: str,
        now_ms: int | None = None,
    ) -> None:
        import ccxt

        self._ccxt = ccxt
        self._request_limit = request_limit
        self._timeframe = timeframe
        self._now_ms = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        self._clients: dict[str, Any] = {}
        self._markets: dict[str, dict[str, Any]] = {}

    def fetch_window(self, task: Mapping[str, Any]) -> dict[str, Any]:
        exchange = str(task["exchange"])
        client, markets = self._client(exchange)
        market = resolve_public_market(markets, str(task["symbol"]))
        base = {
            "schema": WINDOW_SCHEMA,
            "task_id": task["task_id"],
            "event_id": task["event_id"],
            "symbol": task["symbol"],
            "exchange": exchange,
            "start_ms": int(task["start_ms"]),
            "end_ms": int(task["end_ms"]),
            "timeframe": task["timeframe"],
            "public_only": True,
        }
        if market is None:
            return {
                **base,
                "market": {"available": False},
                "series": empty_series("symbol_unavailable"),
            }
        symbol = str(market.get("symbol") or "")
        start_ms = int(task["start_ms"])
        end_ms = int(task["end_ms"])
        return {
            **base,
            "market": {
                "available": True,
                "exchange_symbol": symbol,
                "market_id": market.get("id"),
                "contract_size": finite_float(market.get("contractSize")),
                "linear": market.get("linear"),
                "inverse": market.get("inverse"),
            },
            "series": {
                "ohlcv": fetch_ohlcv_pages(
                    client,
                    symbol=symbol,
                    timeframe=self._timeframe,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    limit=self._request_limit,
                ),
                "funding": fetch_funding_pages(
                    client,
                    symbol=symbol,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    limit=min(self._request_limit, 200),
                ),
                "open_interest": fetch_oi_pages(
                    client,
                    symbol=symbol,
                    timeframe=self._timeframe,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    limit=min(self._request_limit, 200),
                    now_ms=self._now_ms,
                ),
                **{
                    name: fetch_derived_price_pages(
                        client,
                        symbol=symbol,
                        timeframe=self._timeframe,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        limit=self._request_limit,
                        price_kind=price_kind,
                    )
                    for name, price_kind in DERIVED_PRICE_KINDS.items()
                },
            },
        }

    def _client(self, exchange: str) -> tuple[Any, dict[str, Any]]:
        if exchange not in self._clients:
            exchange_class = getattr(self._ccxt, PUBLIC_EXCHANGE_IDS[exchange])
            options = {"defaultType": "future" if exchange == "binance" else "swap"}
            client = exchange_class(
                {"enableRateLimit": True, "timeout": 30_000, "options": options}
            )
            markets = client.load_markets()
            self._clients[exchange] = client
            self._markets[exchange] = {
                str(market.get("symbol") or key): market
                for key, market in markets.items()
                if isinstance(market, Mapping)
            }
        return self._clients[exchange], self._markets[exchange]


def fetch_ohlcv_pages(
    client: Any,
    *,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> dict[str, Any]:
    if not client.has.get("fetchOHLCV"):
        return unavailable_dataset("fetchOHLCV_not_supported")
    cursor = start_ms
    step_ms = timeframe_ms(timeframe)
    by_ts: dict[int, dict[str, Any]] = {}
    calls = 0
    error = ""
    while cursor < end_ms and calls < 50:
        calls += 1
        try:
            raw = client.fetch_ohlcv(symbol, timeframe, since=cursor, limit=limit)
        except Exception as exc:  # pylint: disable=broad-except
            error = compact_error(exc)
            break
        page_timestamps: list[int] = []
        for item in raw or []:
            if not item or item[0] is None:
                continue
            ts_ms = int(item[0])
            page_timestamps.append(ts_ms)
            if start_ms <= ts_ms < end_ms:
                by_ts[ts_ms] = {
                    "ts_ms": ts_ms,
                    "open": finite_float(item[1]),
                    "high": finite_float(item[2]),
                    "low": finite_float(item[3]),
                    "close": finite_float(item[4]),
                    "volume": finite_float(item[5]),
                }
        if not page_timestamps:
            break
        next_cursor = max(page_timestamps) + step_ms
        if next_cursor <= cursor:
            error = "pagination_stalled"
            break
        cursor = next_cursor
        if len(raw or []) < limit and cursor < end_ms:
            break
    return dataset_payload(by_ts, calls=calls, error=error)


def fetch_funding_pages(
    client: Any,
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> dict[str, Any]:
    if not client.has.get("fetchFundingRateHistory"):
        return unavailable_dataset("fetchFundingRateHistory_not_supported")
    cursor = start_ms
    by_ts: dict[int, dict[str, Any]] = {}
    calls = 0
    error = ""
    while cursor < end_ms and calls < 20:
        calls += 1
        try:
            raw = client.fetch_funding_rate_history(symbol, since=cursor, limit=limit)
        except Exception as exc:  # pylint: disable=broad-except
            error = compact_error(exc)
            break
        timestamps = [int(row["timestamp"]) for row in raw or [] if row.get("timestamp") is not None]
        for row in raw or []:
            ts_ms = row.get("timestamp")
            if ts_ms is not None and start_ms <= int(ts_ms) < end_ms:
                by_ts[int(ts_ms)] = {
                    "ts_ms": int(ts_ms),
                    "funding_rate": finite_float(row.get("fundingRate")),
                }
        if not timestamps:
            break
        next_cursor = max(timestamps) + 1
        if next_cursor <= cursor:
            error = "pagination_stalled"
            break
        cursor = next_cursor
        if len(raw or []) < limit:
            break
    return dataset_payload(by_ts, calls=calls, error=error)


def fetch_oi_pages(
    client: Any,
    *,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    now_ms: int | None = None,
) -> dict[str, Any]:
    if not client.has.get("fetchOpenInterestHistory"):
        return unavailable_dataset("fetchOpenInterestHistory_not_supported")
    exchange = str(getattr(client, "id", "") or "").lower()
    effective_now_ms = int(now_ms) if now_ms is not None else int(time.time() * 1000)
    query_start_ms = start_ms
    retention_note = ""
    if exchange.startswith("binance"):
        retention_start_ms = effective_now_ms - BINANCE_OI_RETENTION_MS
        if end_ms <= retention_start_ms:
            return skipped_dataset(
                "retention_gap:binance_open_interest_latest_1_month",
                retention_policy="latest_1_month_conservative_30d",
            )
        if start_ms < retention_start_ms:
            query_start_ms = retention_start_ms
            retention_note = "retention_partial:binance_open_interest_latest_1_month"
    if exchange == "bybit":
        return fetch_bybit_oi_pages(
            client,
            symbol=symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=min(limit, 200),
        )
    cursor = start_ms
    cursor = max(cursor, query_start_ms)
    step_ms = timeframe_ms(timeframe)
    by_ts: dict[int, dict[str, Any]] = {}
    calls = 0
    error = ""
    while cursor < end_ms and calls < 50:
        calls += 1
        try:
            raw = client.fetch_open_interest_history(symbol, timeframe, since=cursor, limit=limit)
        except Exception as exc:  # pylint: disable=broad-except
            error = compact_error(exc)
            break
        timestamps = [int(row["timestamp"]) for row in raw or [] if row.get("timestamp") is not None]
        for row in raw or []:
            ts_ms = row.get("timestamp")
            if ts_ms is None or not start_ms <= int(ts_ms) < end_ms:
                continue
            value = first_finite(
                row.get("openInterestAmount"),
                row.get("openInterest"),
                row.get("baseVolume"),
                row.get("quoteVolume"),
            )
            by_ts[int(ts_ms)] = {"ts_ms": int(ts_ms), "open_interest": value}
        if not timestamps:
            break
        next_cursor = max(timestamps) + step_ms
        if next_cursor <= cursor:
            error = "pagination_stalled"
            break
        cursor = next_cursor
        if len(raw or []) < limit:
            break
    return dataset_payload(by_ts, calls=calls, error=error or retention_note)


def fetch_bybit_oi_pages(
    client: Any,
    *,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> dict[str, Any]:
    """Page backwards because Bybit anchors bounded OI history at ``endTime``."""

    step_ms = timeframe_ms(timeframe)
    cursor_end = end_ms - 1
    by_ts: dict[int, dict[str, Any]] = {}
    calls = 0
    error = ""
    while cursor_end >= start_ms and calls < 50:
        calls += 1
        try:
            raw = client.fetch_open_interest_history(
                symbol,
                timeframe,
                since=start_ms,
                limit=limit,
                params={"until": cursor_end},
            )
        except Exception as exc:  # pylint: disable=broad-except
            error = compact_error(exc)
            break
        timestamps = [int(row["timestamp"]) for row in raw or [] if row.get("timestamp") is not None]
        for row in raw or []:
            ts_ms = row.get("timestamp")
            if ts_ms is None or not start_ms <= int(ts_ms) < end_ms:
                continue
            value = first_finite(
                row.get("openInterestAmount"),
                row.get("openInterest"),
                row.get("baseVolume"),
                row.get("quoteVolume"),
            )
            by_ts[int(ts_ms)] = {"ts_ms": int(ts_ms), "open_interest": value}
        if not timestamps:
            break
        next_end = min(timestamps) - 1
        if next_end >= cursor_end:
            error = "pagination_stalled"
            break
        cursor_end = next_end
        if min(timestamps) <= start_ms or len(raw or []) < limit:
            break
        if cursor_end < start_ms - step_ms:
            break
    return dataset_payload(
        by_ts,
        calls=calls,
        error=error,
        retention_policy="to_symbol_launch_time",
    )


def fetch_derived_price_pages(
    client: Any,
    *,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    price_kind: str,
) -> dict[str, Any]:
    if price_kind not in DERIVED_PRICE_KINDS.values():
        raise ValueError(f"unsupported derived price kind: {price_kind}")
    if not client.has.get("fetchOHLCV"):
        return unavailable_dataset("fetchOHLCV_not_supported")
    cursor = start_ms
    step_ms = timeframe_ms(timeframe)
    by_ts: dict[int, dict[str, Any]] = {}
    calls = 0
    error = ""
    while cursor < end_ms and calls < 50:
        calls += 1
        try:
            raw = client.fetch_ohlcv(
                symbol,
                timeframe,
                since=cursor,
                limit=limit,
                params={"price": price_kind},
            )
        except Exception as exc:  # pylint: disable=broad-except
            error = compact_error(exc)
            break
        page_timestamps: list[int] = []
        for item in raw or []:
            if not item or item[0] is None:
                continue
            ts_ms = int(item[0])
            page_timestamps.append(ts_ms)
            if start_ms <= ts_ms < end_ms:
                by_ts[ts_ms] = {
                    "ts_ms": ts_ms,
                    "open": finite_float(item[1]),
                    "high": finite_float(item[2]),
                    "low": finite_float(item[3]),
                    "close": finite_float(item[4]),
                }
        if not page_timestamps:
            break
        next_cursor = max(page_timestamps) + step_ms
        if next_cursor <= cursor:
            error = "pagination_stalled"
            break
        cursor = next_cursor
        if len(raw or []) < limit and cursor < end_ms:
            break
    return dataset_payload(
        by_ts,
        calls=calls,
        error=error,
        endpoint_kind=price_kind,
        retention_policy="not_limited_in_official_endpoint_docs",
    )


def coverage_from_task(
    task: Mapping[str, Any],
    window: Mapping[str, Any] | None,
    *,
    cache_reused: bool,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "run_task_id": task["task_id"],
        "event_id": task["event_id"],
        "symbol": task["symbol"],
        "exchange": task["exchange"],
        "event_ts_ms": task["event_ts_ms"],
        "start_ms": task["start_ms"],
        "end_ms": task["end_ms"],
        "status": task.get("status"),
        "cache_reused": cache_reused,
        "expected_candles": task["expected_candles"],
    }
    if window is None:
        row.update(
            {
                "market_available": None,
                "ohlcv_rows": None,
                "ohlcv_coverage_pct": None,
                "funding_rows": None,
                "oi_rows": None,
                "missing_datasets": "not_executed",
                "errors": "",
            }
        )
        return row
    market = dict(window.get("market") or {})
    series = dict(window.get("series") or {})
    counts = {name: len(dataset_rows(series.get(name))) for name in series}
    calls = {
        name: int(dataset.get("calls") or 0)
        for name, dataset in series.items()
        if isinstance(dataset, Mapping)
    }
    source_public_calls = sum(calls.values())
    missing = [
        name
        for name in ("ohlcv", "funding", "open_interest", "mark", "index", "premium")
        if counts.get(name, 0) == 0
    ]
    errors = [
        f"{name}:{dataset.get('error')}"
        for name, dataset in series.items()
        if isinstance(dataset, Mapping) and dataset.get("error")
    ]
    expected = int(task.get("expected_candles") or 0)
    ohlcv_rows = counts.get("ohlcv", 0)
    row.update(
        {
            "market_available": bool(market.get("available")),
            "exchange_symbol": market.get("exchange_symbol"),
            "market_id": market.get("market_id"),
            "contract_size": market.get("contract_size"),
            "ohlcv_rows": ohlcv_rows,
            "ohlcv_coverage_pct": round(min(100.0, ohlcv_rows / expected * 100.0), 3)
            if expected
            else None,
            "funding_rows": counts.get("funding", 0),
            "oi_rows": counts.get("open_interest", 0),
            "ohlcv_calls": calls.get("ohlcv", 0),
            "funding_calls": calls.get("funding", 0),
            "oi_calls": calls.get("open_interest", 0),
            "source_public_calls": source_public_calls,
            "public_calls_this_run": 0 if cache_reused else source_public_calls,
            "mark_rows": counts.get("mark", 0),
            "index_rows": counts.get("index", 0),
            "premium_rows": counts.get("premium", 0),
            "missing_datasets": ",".join(missing),
            "errors": " | ".join(errors),
        }
    )
    return row


def build_ledger_record(
    *,
    manifest: Mapping[str, Any],
    task: Mapping[str, Any],
    window: Mapping[str, Any],
    coverage: Mapping[str, Any],
    code_commit: str,
) -> dict[str, Any]:
    features_hash = stable_hash(window)
    veto_reasons: list[str] = []
    if not coverage.get("market_available"):
        veto_reasons.append("market_unavailable")
    if int(coverage.get("ohlcv_rows") or 0) == 0:
        veto_reasons.append("ohlcv_missing")
    missing_fields = [
        item for item in str(coverage.get("missing_datasets") or "").split(",") if item
    ]
    identity = {
        "run_id": manifest["run_id"],
        "task_id": task["task_id"],
        "record_type": "enrichment_result",
        "features_hash": features_hash,
    }
    return {
        "schema": LEDGER_SCHEMA,
        "run_id": manifest["run_id"],
        "record_id": f"record-{stable_hash(identity)[:24]}",
        "record_type": "enrichment_result",
        "mode": manifest["mode"],
        "event_id": task["event_id"],
        "hypothesis_id": manifest["config"]["hypothesis_id"],
        "symbol": task["symbol"],
        "exchange_pair": [task["exchange"]],
        "source_ts_ms": task["event_ts_ms"],
        "observed_at_ms": int(time.time() * 1000),
        "data_quality": {
            "market_available": coverage.get("market_available"),
            "ohlcv_coverage_pct": coverage.get("ohlcv_coverage_pct"),
            "cache_reused": coverage.get("cache_reused"),
        },
        "missing_fields": missing_fields,
        "veto_reasons": veto_reasons,
        "features_ref": task.get("cache_path"),
        "features_hash": features_hash,
        "prediction": None,
        "confidence": None,
        "decision": "VETO" if veto_reasons else "WAIT",
        "decision_reason": "data_quality_veto"
        if veto_reasons
        else "data_collected_no_signal_evaluation",
        "execution_assumptions": {
            "fee_pct": None,
            "slippage_pct": None,
            "funding_pct": None,
            "capacity_usd": None,
        },
        "outcome": {"status": "pending"},
        "code_commit": code_commit,
        "config_hash": manifest["config_hash"],
        "source_manifest_hash": manifest["source_manifest_hash"],
    }


def validate_ledger_record(record: Mapping[str, Any]) -> None:
    required = (
        "schema",
        "run_id",
        "record_id",
        "record_type",
        "mode",
        "event_id",
        "hypothesis_id",
        "symbol",
        "source_ts_ms",
        "decision",
        "features_hash",
        "code_commit",
        "config_hash",
        "source_manifest_hash",
    )
    missing = [key for key in required if record.get(key) in (None, "")]
    if missing:
        raise ValueError(f"invalid ledger record, missing: {missing}")
    if record.get("schema") != LEDGER_SCHEMA:
        raise ValueError("invalid ledger schema")
    if record.get("mode") not in ALLOWED_MODES:
        raise ValueError("invalid ledger mode")
    if record.get("decision") not in ALLOWED_DECISIONS:
        raise ValueError("invalid ledger decision")
    if record.get("mode") == "live":
        raise ValueError("Strategy Lab live mode is forbidden")


def render_event_lake_report(
    *,
    metadata: Mapping[str, Any],
    coverage: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Strategy Lab Event Lake — bounded run",
        "",
        "Status: public-only research replay. No keys, orders, ARM changes or live decisions.",
        "",
        f"- Run: `{metadata.get('run_id')}`",
        f"- Events/tasks: {metadata.get('selected_events')} / {metadata.get('tasks')}",
        f"- Estimated calls: {metadata.get('estimated_public_calls')}",
        f"- Source calls represented by cache: {metadata.get('source_public_calls')}",
        f"- Calls in this invocation: {metadata.get('public_calls_this_run')}",
        f"- Statuses: `{json.dumps(metadata.get('status_counts') or {}, sort_keys=True)}`",
        f"- Ledger records: {metadata.get('ledger_records')}",
        "",
        "| Symbol | Exchange | Status | OHLCV | Coverage | Funding | OI | Mark | Index | Premium | Missing |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in coverage:
        lines.append(
            "| {symbol} | {exchange} | {status} | {ohlcv_rows} | {ohlcv_coverage_pct}% | "
            "{funding_rows} | {oi_rows} | {mark_rows} | {index_rows} | {premium_rows} | "
            "{missing_datasets} |".format(
                **{key: row.get(key, "") for key in row}
            )
        )
    lines.extend(
        [
            "",
            "Missing values remain missing; they are never converted to zero. "
            "`WAIT` means only that data was collected and no signal was evaluated. "
            "`VETO` means the minimum public market/OHLCV gate failed.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_window(window: Mapping[str, Any], task: Mapping[str, Any]) -> None:
    if window.get("schema") != WINDOW_SCHEMA:
        raise ValueError("invalid Event Lake window schema")
    if window.get("task_id") != task.get("task_id"):
        raise ValueError("Event Lake task/cache identity mismatch")
    if window.get("public_only") is not True:
        raise ValueError("Event Lake window is not public-only")
    for key in ("event_id", "symbol", "exchange", "start_ms", "end_ms", "timeframe"):
        if key in task and window.get(key) != task.get(key):
            raise ValueError(f"Event Lake window identity mismatch: {key}")


def read_valid_cache(path: Path, task: Mapping[str, Any]) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        validate_window(payload, task)
        return payload
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def read_ledger_record_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid ledger JSON at line {line_number}") from exc
            validate_ledger_record(payload)
            record_id = str(payload["record_id"])
            if record_id in out:
                raise ValueError(f"duplicate ledger record_id at line {line_number}: {record_id}")
            out.add(record_id)
    return out


def resolve_public_market(markets: Mapping[str, Any], canonical_symbol: str) -> Mapping[str, Any] | None:
    target = normalize_symbol(canonical_symbol)
    for market in markets.values():
        if not isinstance(market, Mapping) or not (market.get("swap") or market.get("future")):
            continue
        joined = normalize_symbol(f"{market.get('base') or ''}{market.get('quote') or ''}")
        if joined == target:
            return market
        if normalize_symbol(market.get("id")) == target:
            return market
    return None


def dataset_payload(
    by_ts: Mapping[int, Mapping[str, Any]],
    *,
    calls: int,
    error: str,
    **metadata: Any,
) -> dict[str, Any]:
    return {
        "supported": True,
        "calls": calls,
        "rows": [dict(by_ts[ts]) for ts in sorted(by_ts)],
        "error": error,
        **metadata,
    }


def unavailable_dataset(reason: str) -> dict[str, Any]:
    return {"supported": False, "calls": 0, "rows": [], "error": reason}


def skipped_dataset(reason: str, **metadata: Any) -> dict[str, Any]:
    return {
        "supported": True,
        "request_skipped": True,
        "calls": 0,
        "rows": [],
        "error": reason,
        **metadata,
    }


def empty_series(reason: str) -> dict[str, Any]:
    return {
        name: unavailable_dataset(reason)
        for name in ("ohlcv", "funding", "open_interest", "mark", "index", "premium")
    }


def dataset_rows(dataset: Any) -> list[Mapping[str, Any]]:
    if not isinstance(dataset, Mapping):
        return []
    return [row for row in dataset.get("rows") or [] if isinstance(row, Mapping)]


def window_status(window: Mapping[str, Any]) -> str:
    if not (window.get("market") or {}).get("available"):
        return "market_unavailable"
    if not dataset_rows((window.get("series") or {}).get("ohlcv")):
        return "incomplete"
    return "completed"


def timeframe_ms(value: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    if len(value) < 2 or value[-1] not in units:
        raise ValueError(f"unsupported timeframe: {value}")
    try:
        count = int(value[:-1])
    except ValueError as exc:
        raise ValueError(f"unsupported timeframe: {value}") from exc
    if count < 1:
        raise ValueError(f"unsupported timeframe: {value}")
    return count * units[value[-1]]


def normalize_symbol(value: Any) -> str:
    return "".join(character for character in str(value or "").upper() if character.isalnum())


def finite_float(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def first_finite(*values: Any) -> float | None:
    for value in values:
        parsed = finite_float(value)
        if parsed is not None:
            return parsed
    return None


def compact_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:300]


def stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def current_git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=BASE_DIR,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(dict(payload), ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(path)


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    materialized = [dict(row) for row in rows]
    fields = list(dict.fromkeys(key for row in materialized for key in row))
    if not fields:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def append_jsonl_once(path: Path, payload: Mapping[str, Any]) -> bool:
    """Append one unique record under a short cross-process lock."""

    path.parent.mkdir(parents=True, exist_ok=True)
    record_id = str(payload.get("record_id") or "")
    if not record_id:
        raise ValueError("JSONL record_id is required")
    with exclusive_file_lock(path.with_name(f".{path.name}.lock")):
        if record_id in read_ledger_record_ids(path):
            return False
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(dict(payload), ensure_ascii=True, sort_keys=True))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    return True


@contextmanager
def exclusive_file_lock(path: Path, *, timeout_sec: float = 5.0):
    started = time.monotonic()
    token = {"pid": os.getpid(), "created_at_ms": int(time.time() * 1000)}
    while True:
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if remove_stale_lock(path):
                continue
            if time.monotonic() - started >= timeout_sec:
                raise TimeoutError(f"timed out waiting for ledger lock: {path.name}")
            time.sleep(0.05)
            continue
        try:
            os.write(descriptor, json.dumps(token, sort_keys=True).encode("ascii"))
        finally:
            os.close(descriptor)
        break
    try:
        yield
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def remove_stale_lock(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="ascii"))
        pid = int(payload.get("pid") or 0)
    except (OSError, ValueError, json.JSONDecodeError):
        try:
            if time.time() - path.stat().st_mtime <= 30:
                return False
            path.unlink()
            return True
        except (FileNotFoundError, OSError):
            return False
    if pid <= 0 or process_is_running(pid):
        return False
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return True


def process_is_running(pid: int) -> bool:
    if os.name == "nt":
        import ctypes

        synchronize = 0x00100000
        wait_timeout = 0x00000102
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(synchronize, False, pid)
        if not handle:
            return ctypes.get_last_error() == 5
        try:
            return kernel32.WaitForSingleObject(handle, 0) == wait_timeout
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except (OSError, PermissionError):
        return True
    return True


__all__ = [
    "CcxtPublicEventProvider",
    "EventLakeConfig",
    "LEDGER_SCHEMA",
    "append_jsonl_once",
    "build_enrichment_manifest",
    "build_ledger_record",
    "estimate_full_catalog_run",
    "fetch_derived_price_pages",
    "fetch_oi_pages",
    "fetch_ohlcv_pages",
    "run_event_lake",
    "select_catalog_events",
    "validate_ledger_record",
]
