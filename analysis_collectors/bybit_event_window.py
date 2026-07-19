from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_collectors.bybit_pump_short import (
    BybitCollectorConfig,
    BybitPumpShortCollector,
    dedupe_sort_by_ts,
    interval_to_ms,
    normalize_symbol,
    round_float,
    scale_pct,
    sum_funding_since,
    to_float,
    to_int,
)
from config import BASE_DIR

DEFAULT_EVENT_INPUT = BASE_DIR / "data" / "research" / "pump_lifecycle_research" / "lifecycle_events.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_event_windows"
HOUR_MS = 3_600_000


@dataclass(slots=True)
class EventWindowConfig:
    input_events: Path = DEFAULT_EVENT_INPUT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    intervals: tuple[str, ...] = ("15",)
    pre_hours: int = 72
    post_hours: int = 336
    min_pump_pct: float = 80.0
    max_events: int | None = 25
    sleep_sec: float = 0.8
    timeout_sec: float = 20.0
    max_retries: int = 3
    resume: bool = True


def collect_bybit_event_windows(
    config: EventWindowConfig | None = None,
    *,
    symbols: Iterable[str] | None = None,
) -> dict[str, Any]:
    cfg = config or EventWindowConfig()
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    collector = BybitEventWindowCollector(
        BybitCollectorConfig(
            output_dir=cfg.output_dir,
            sleep_sec=cfg.sleep_sec,
            timeout_sec=cfg.timeout_sec,
            max_retries=cfg.max_retries,
        )
    )
    requested = {normalize_symbol(symbol) for symbol in symbols or [] if normalize_symbol(symbol)}
    done = read_done_events(cfg.output_dir / "done_events.txt") if cfg.resume else set()
    done_keys = read_collected_event_keys(cfg.output_dir / "event_windows.jsonl") if cfg.resume else set()
    events = select_events(read_events(cfg.input_events), min_pump_pct=cfg.min_pump_pct, max_events=cfg.max_events, symbols=requested)
    summary_rows: list[dict[str, Any]] = []
    failed = 0
    skipped = 0
    collected = 0
    for event in events:
        event_id = str(event.get("event_id") or "")
        key = event_key(event)
        if cfg.resume and (event_id in done or key in done_keys):
            skipped += 1
            continue
        try:
            sample = collector.collect_event_window(event, intervals=cfg.intervals, pre_hours=cfg.pre_hours, post_hours=cfg.post_hours)
            append_jsonl(cfg.output_dir / "event_windows.jsonl", sample)
            done_keys.add(key)
            summary = build_event_window_summary(sample)
            summary_rows.append(summary)
            append_csv(cfg.output_dir / "event_window_summary.csv", summary)
            append_line(cfg.output_dir / "done_events.txt", event_id)
            collected += 1
        except Exception as exc:  # pylint: disable=broad-except
            failed += 1
            append_jsonl(
                cfg.output_dir / "errors.jsonl",
                {
                    "ts_ms": now_ms(),
                    "event_id": event_id,
                    "symbol": event.get("symbol"),
                    "error": str(exc),
                },
            )
    metadata = {
        "schema": "bybit_pump_event_windows_v1",
        "input_events": str(cfg.input_events),
        "output_dir": str(cfg.output_dir),
        "intervals": list(cfg.intervals),
        "pre_hours": cfg.pre_hours,
        "post_hours": cfg.post_hours,
        "min_pump_pct": cfg.min_pump_pct,
        "max_events": cfg.max_events,
        "events_selected": len(events),
        "events_collected": collected,
        "events_skipped": skipped,
        "events_failed": failed,
        "requests_made": collector.stats.requests_made,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (cfg.output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


class BybitEventWindowCollector(BybitPumpShortCollector):
    def collect_event_window(
        self,
        event: dict[str, Any],
        *,
        intervals: tuple[str, ...],
        pre_hours: int,
        post_hours: int,
    ) -> dict[str, Any]:
        symbol = normalize_symbol(event.get("symbol"))
        trigger_ts = to_int(event.get("trigger_ts"))
        if not symbol or trigger_ts is None:
            raise ValueError(f"bad event row: {event}")
        start_ms = trigger_ts - pre_hours * HOUR_MS
        end_ms = trigger_ts + post_hours * HOUR_MS
        interval_series: dict[str, Any] = {}
        for interval in intervals:
            oi_interval = bybit_oi_interval(interval)
            interval_series[str(interval)] = {
                "klines": self.fetch_klines(symbol, interval=str(interval), start_ms=start_ms, end_ms=end_ms),
                "mark_price_klines": self.fetch_price_klines("/v5/market/mark-price-kline", symbol, interval=str(interval), start_ms=start_ms, end_ms=end_ms),
                "index_price_klines": self.fetch_price_klines("/v5/market/index-price-kline", symbol, interval=str(interval), start_ms=start_ms, end_ms=end_ms),
                "premium_index_klines": self.fetch_price_klines("/v5/market/premium-index-price-kline", symbol, interval=str(interval), start_ms=start_ms, end_ms=end_ms),
                "open_interest": self.fetch_open_interest(symbol, interval_time=oi_interval, start_ms=start_ms, end_ms=end_ms),
            }
        funding = self.fetch_funding_history(symbol, start_ms=start_ms, end_ms=end_ms)
        return {
            "schema": "bybit_pump_event_window_v1",
            "event": event,
            "symbol": symbol,
            "trigger_ts": trigger_ts,
            "trigger_iso": ms_to_iso(trigger_ts),
            "start_ts": start_ms,
            "start_iso": ms_to_iso(start_ms),
            "end_ts": end_ms,
            "end_iso": ms_to_iso(end_ms),
            "intervals": interval_series,
            "funding": funding,
        }

    def fetch_price_klines(
        self,
        path: str,
        symbol: str,
        *,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        interval_ms = interval_to_ms(interval)
        max_span_ms = max(1, limit - 1) * interval_ms
        parsed: list[dict[str, Any]] = []
        cursor_start = start_ms
        while cursor_start <= end_ms:
            cursor_end = min(end_ms, cursor_start + max_span_ms)
            payload = self._get_json(
                path,
                {
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval,
                    "start": cursor_start,
                    "end": cursor_end,
                    "limit": limit,
                },
            )
            rows = ((payload.get("result") or {}).get("list") or []) if isinstance(payload, dict) else []
            for row in rows:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                parsed.append(
                    {
                        "ts_ms": to_int(row[0]),
                        "open": to_float(row[1]),
                        "high": to_float(row[2]),
                        "low": to_float(row[3]),
                        "close": to_float(row[4]),
                    }
                )
            cursor_start = cursor_end + interval_ms
        return dedupe_sort_by_ts(parsed)


def build_event_window_summary(sample: dict[str, Any]) -> dict[str, Any]:
    event = sample.get("event") if isinstance(sample.get("event"), dict) else {}
    trigger_ts = to_int(sample.get("trigger_ts")) or 0
    primary_interval = str(next(iter((sample.get("intervals") or {}).keys()), ""))
    primary = (sample.get("intervals") or {}).get(primary_interval) or {}
    klines = primary.get("klines") or []
    premium = primary.get("premium_index_klines") or []
    mark = primary.get("mark_price_klines") or []
    index = primary.get("index_price_klines") or []
    oi = primary.get("open_interest") or []
    funding = sample.get("funding") or []

    trigger_close = value_at_or_before(klines, trigger_ts, "close")
    max_high_24h = max_value_between(klines, trigger_ts, trigger_ts + 24 * HOUR_MS, "high")
    min_low_24h = min_value_between(klines, trigger_ts, trigger_ts + 24 * HOUR_MS, "low")
    max_high_72h = max_value_between(klines, trigger_ts, trigger_ts + 72 * HOUR_MS, "high")
    min_low_72h = min_value_between(klines, trigger_ts, trigger_ts + 72 * HOUR_MS, "low")
    max_high_full = max_value_between(klines, trigger_ts, to_int(sample.get("end_ts")) or trigger_ts, "high")

    premium_trigger = value_at_or_before(premium, trigger_ts, "close")
    premium_min_next_24h = min_value_between(premium, trigger_ts, trigger_ts + 24 * HOUR_MS, "low")
    premium_max_next_24h = max_value_between(premium, trigger_ts, trigger_ts + 24 * HOUR_MS, "high")
    premium_last = latest_value(premium, "close")
    oi_trigger = value_at_or_before(oi, trigger_ts, "open_interest")
    oi_max_next_24h = max_value_between(oi, trigger_ts, trigger_ts + 24 * HOUR_MS, "open_interest")
    oi_change_24h = pct_change(value_at_or_before(oi, trigger_ts + 24 * HOUR_MS, "open_interest"), oi_trigger)
    volume_z_peak_24h = peak_volume_z(klines, trigger_ts, lookback_rows=96, forward_ms=24 * HOUR_MS)
    mark_index_basis_trigger = basis_pct(value_at_or_before(mark, trigger_ts, "close"), value_at_or_before(index, trigger_ts, "close"))

    return {
        "event_id": event.get("event_id"),
        "symbol": sample.get("symbol"),
        "trigger_ts": trigger_ts,
        "trigger_iso": sample.get("trigger_iso"),
        "trigger_pump_pct": event.get("trigger_pump_pct"),
        "interval": primary_interval,
        "klines": len(klines),
        "premium_points": len(premium),
        "mark_points": len(mark),
        "index_points": len(index),
        "oi_points": len(oi),
        "funding_points": len(funding),
        "funding_prev_24h_pct": round_float(scale_pct(sum_funding_between(funding, trigger_ts - 24 * HOUR_MS, trigger_ts))),
        "funding_next_24h_pct": round_float(scale_pct(sum_funding_between(funding, trigger_ts, trigger_ts + 24 * HOUR_MS))),
        "funding_next_72h_pct": round_float(scale_pct(sum_funding_between(funding, trigger_ts, trigger_ts + 72 * HOUR_MS))),
        "premium_trigger_pct": round_float(scale_pct(premium_trigger)),
        "premium_min_next_24h_pct": round_float(scale_pct(premium_min_next_24h)),
        "premium_max_next_24h_pct": round_float(scale_pct(premium_max_next_24h)),
        "premium_last_pct": round_float(scale_pct(premium_last)),
        "premium_relief_24h_pct": round_float(scale_pct(pct_point_change(premium_min_next_24h, premium_trigger))),
        "mark_index_basis_trigger_pct": round_float(mark_index_basis_trigger),
        "oi_change_24h_pct": round_float(oi_change_24h),
        "oi_max_next_24h_pct_from_trigger": round_float(pct_change(oi_max_next_24h, oi_trigger)),
        "volume_z_peak_24h": round_float(volume_z_peak_24h),
        "future_high_24h_pct": round_float(pct_change(max_high_24h, trigger_close)),
        "future_low_24h_pct": round_float(pct_change(min_low_24h, trigger_close)),
        "future_high_72h_pct": round_float(pct_change(max_high_72h, trigger_close)),
        "future_low_72h_pct": round_float(pct_change(min_low_72h, trigger_close)),
        "future_high_full_pct": round_float(pct_change(max_high_full, trigger_close)),
    }


def select_events(
    events: list[dict[str, Any]],
    *,
    min_pump_pct: float,
    max_events: int | None,
    symbols: set[str],
) -> list[dict[str, Any]]:
    by_episode: dict[tuple[str, int], dict[str, Any]] = {}
    for event in events:
        symbol = normalize_symbol(event.get("symbol"))
        if symbols and symbol not in symbols:
            continue
        pump = to_float(event.get("trigger_pump_pct")) or 0.0
        if pump < min_pump_pct:
            continue
        trigger_ts = to_int(event.get("trigger_ts")) or 0
        key = (symbol, trigger_ts)
        previous = by_episode.get(key)
        if previous is None or pump > (to_float(previous.get("trigger_pump_pct")) or 0.0):
            by_episode[key] = event
    filtered = list(by_episode.values())
    filtered.sort(key=lambda row: (to_float(row.get("trigger_pump_pct")) or 0.0, to_int(row.get("trigger_ts")) or 0), reverse=True)
    return filtered[:max_events] if max_events is not None else filtered


def read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_done_events(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def read_collected_event_keys(path: Path) -> set[tuple[str, int]]:
    if not path.exists():
        return set()
    out: set[tuple[str, int]] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            symbol = normalize_symbol(row.get("symbol"))
            trigger_ts = to_int(row.get("trigger_ts")) or 0
            if symbol and trigger_ts:
                out.add((symbol, trigger_ts))
    return out


def event_key(event: dict[str, Any]) -> tuple[str, int]:
    return (normalize_symbol(event.get("symbol")), to_int(event.get("trigger_ts")) or 0)


def bybit_oi_interval(interval: str) -> str:
    text = str(interval)
    if text.endswith("min") or text.endswith("h") or text.endswith("d"):
        return text
    minutes = int(text)
    if minutes < 60:
        return f"{minutes}min"
    return f"{minutes // 60}h"


def value_at_or_before(rows: list[dict[str, Any]], ts_ms: int, key: str) -> float | None:
    out = None
    for row in rows:
        row_ts = to_int(row.get("ts_ms"))
        if row_ts is None or row_ts > ts_ms:
            break
        value = to_float(row.get(key))
        if value is not None:
            out = value
    return out


def latest_value(rows: list[dict[str, Any]], key: str) -> float | None:
    for row in reversed(rows):
        value = to_float(row.get(key))
        if value is not None:
            return value
    return None


def max_value_between(rows: list[dict[str, Any]], start_ms: int, end_ms: int, key: str) -> float | None:
    values = [to_float(row.get(key)) for row in rows if start_ms <= (to_int(row.get("ts_ms")) or -1) <= end_ms]
    clean = [value for value in values if value is not None]
    return max(clean) if clean else None


def min_value_between(rows: list[dict[str, Any]], start_ms: int, end_ms: int, key: str) -> float | None:
    values = [to_float(row.get(key)) for row in rows if start_ms <= (to_int(row.get("ts_ms")) or -1) <= end_ms]
    clean = [value for value in values if value is not None]
    return min(clean) if clean else None


def sum_funding_between(rows: list[dict[str, Any]], start_ms: int, end_ms: int) -> float | None:
    clean = [
        to_float(row.get("funding_rate"))
        for row in rows
        if start_ms <= (to_int(row.get("ts_ms")) or -1) <= end_ms
    ]
    values = [value for value in clean if value is not None]
    return sum(values) if values else None


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in {None, 0}:
        return None
    return (current / float(previous) - 1.0) * 100.0


def pct_point_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return current - previous


def basis_pct(mark_price: float | None, index_price: float | None) -> float | None:
    if mark_price is None or index_price in {None, 0}:
        return None
    return (mark_price / float(index_price) - 1.0) * 100.0


def peak_volume_z(rows: list[dict[str, Any]], trigger_ts: int, *, lookback_rows: int, forward_ms: int) -> float | None:
    peak = None
    for idx, row in enumerate(rows):
        ts_ms = to_int(row.get("ts_ms"))
        if ts_ms is None or ts_ms < trigger_ts or ts_ms > trigger_ts + forward_ms:
            continue
        history = [to_float(item.get("volume")) for item in rows[max(0, idx - lookback_rows) : idx]]
        history = [value for value in history if value is not None]
        volume = to_float(row.get("volume"))
        if volume is None or len(history) < 10:
            continue
        mean = sum(history) / len(history)
        stdev = math.sqrt(sum((value - mean) ** 2 for value in history) / len(history)) or 1.0
        z_score = (volume - mean) / stdev
        peak = z_score if peak is None else max(peak, z_score)
    return peak


def append_csv(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()), extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
        handle.write("\n")


def append_line(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{text}\n")


def now_ms() -> int:
    return int(time.time() * 1000)


def ms_to_iso(ts_ms: int | None) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


__all__ = [
    "BybitEventWindowCollector",
    "EventWindowConfig",
    "build_event_window_summary",
    "collect_bybit_event_windows",
    "select_events",
]
