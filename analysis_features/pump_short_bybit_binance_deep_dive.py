from __future__ import annotations

import csv
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import BASE_DIR
from analysis_features.pump_short_cross_exchange_research import (
    PUMP_CONFIGS,
    Series,
    detect_pump_events,
    ms_to_iso,
    nearest_idx_at_or_before,
    parse_series,
    pct,
    pct_change,
    safe_max,
    to_float,
    write_csv,
)

DEFAULT_INPUT_ROOT = BASE_DIR / "data" / "research" / "pump_short_multiexchange_2024_clean"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_bybit_binance_deep_dive"
DEFAULT_CAPITAL_USD = 10_000.0
DEFAULT_LEG_NOTIONAL_USD = 1_000.0
FEE_ROUNDTRIP_PCT = 0.18
_ENTRY_CACHE: dict[tuple[int, int, float], int | None] = {}
_LADDER_CACHE: dict[tuple[int, int, float, int, int], tuple[tuple[int, float], ...]] = {}
_EXIT_CACHE: dict[tuple[int, tuple[tuple[int, float], ...], float, int, float], dict[str, Any] | None] = {}


@dataclass(frozen=True, slots=True)
class Strategy:
    pullback_pct: float
    funding_min_prev24_pct: float | None
    ladder_legs: int
    ladder_step_pct: float
    tp_pct: float
    max_hold_h: int

    @property
    def name(self) -> str:
        funding = "any" if self.funding_min_prev24_pct is None else f"fg{self.funding_min_prev24_pct:g}"
        return (
            f"pb{self.pullback_pct:g}_{funding}_ladder{self.ladder_legs}_"
            f"step{self.ladder_step_pct:g}_tp{self.tp_pct:g}_hold{self.max_hold_h}"
        ).replace("-", "m").replace(".", "p")

    @property
    def funding_label(self) -> str:
        if self.funding_min_prev24_pct is None:
            return "any"
        return f">{self.funding_min_prev24_pct:g}%"


class Accumulator:
    __slots__ = (
        "exchange",
        "strategy",
        "pullback_pct",
        "funding_gate",
        "ladder_legs",
        "ladder_step_pct",
        "tp_pct",
        "max_hold_h",
        "n",
        "wins",
        "cat300",
        "take_profit",
        "time_stop",
        "net",
        "price",
        "funding",
        "fees",
        "mae",
        "hold",
        "legs",
        "pnl",
        "funding_usd",
    )

    def __init__(self, exchange: str, strategy: Strategy) -> None:
        self.exchange = exchange
        self.strategy = strategy.name
        self.pullback_pct = strategy.pullback_pct
        self.funding_gate = strategy.funding_label
        self.ladder_legs = strategy.ladder_legs
        self.ladder_step_pct = strategy.ladder_step_pct
        self.tp_pct = strategy.tp_pct
        self.max_hold_h = strategy.max_hold_h
        self.n = 0
        self.wins = 0
        self.cat300 = 0
        self.take_profit = 0
        self.time_stop = 0
        self.net: list[float] = []
        self.price: list[float] = []
        self.funding: list[float] = []
        self.fees: list[float] = []
        self.mae: list[float] = []
        self.hold: list[float] = []
        self.legs: list[float] = []
        self.pnl: list[float] = []
        self.funding_usd: list[float] = []

    def add(self, row: dict[str, Any]) -> None:
        self.n += 1
        self.wins += int(row.get("win") or 0)
        self.cat300 += int(row.get("cat300") or 0)
        if row.get("exit_reason") == "take_profit":
            self.take_profit += 1
        if row.get("exit_reason") == "time_stop":
            self.time_stop += 1
        append_float(self.net, row.get("net_pct"))
        append_float(self.price, row.get("price_pnl_pct"))
        append_float(self.funding, row.get("funding_during_pct"))
        append_float(self.fees, row.get("fee_pct"))
        append_float(self.mae, row.get("mae_pct"))
        append_float(self.hold, row.get("hold_h"))
        append_float(self.legs, row.get("legs_filled"))
        append_float(self.pnl, row.get("pnl_usd"))
        append_float(self.funding_usd, row.get("funding_usd"))

    def row(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "strategy": self.strategy,
            "pullback_pct": self.pullback_pct,
            "funding_gate": self.funding_gate,
            "ladder_legs": self.ladder_legs,
            "ladder_step_pct": self.ladder_step_pct,
            "tp_pct": self.tp_pct,
            "max_hold_h": self.max_hold_h,
            "n": self.n,
            "win_rate_pct": pct(self.wins, self.n),
            "take_profit_rate_pct": pct(self.take_profit, self.n),
            "avg_net_pct": mean(self.net),
            "median_net_pct": median(self.net),
            "avg_price_pnl_pct": mean(self.price),
            "avg_funding_pct": mean(self.funding),
            "median_funding_pct": median(self.funding),
            "avg_fee_pct": mean(self.fees),
            "sum_pnl_usd": sum(self.pnl),
            "sum_funding_usd": sum(self.funding_usd),
            "avg_pnl_usd": mean(self.pnl),
            "avg_funding_usd": mean(self.funding_usd),
            "p90_mae_pct": quantile(self.mae, 0.90),
            "p95_mae_pct": quantile(self.mae, 0.95),
            "cat300_pct": pct(self.cat300, self.n),
            "avg_hold_h": mean(self.hold),
            "avg_legs": mean(self.legs),
        }


def run_deep_dive(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    capital_usd: float = DEFAULT_CAPITAL_USD,
    leg_notional_usd: float = DEFAULT_LEG_NOTIONAL_USD,
    backfill_bybit_funding: bool = True,
) -> dict[str, Any]:
    _ENTRY_CACHE.clear()
    _LADDER_CACHE.clear()
    _EXIT_CACHE.clear()
    output_dir.mkdir(parents=True, exist_ok=True)
    funding_cache_path = output_dir / "bybit_funding_cache.jsonl"
    funding_cache = load_funding_cache(funding_cache_path)

    samples = list(load_exchange_samples(input_root, {"binance", "bybit"}))
    series_items: list[Series] = []
    event_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    for sample_idx, sample in enumerate(samples, start=1):
        if sample_idx % 50 == 0:
            print(f"loaded/inspecting samples {sample_idx}/{len(samples)}", flush=True)
        probe_series = parse_series(sample)
        probe_events = detect_pump_events(probe_series) if probe_series and len(probe_series.ts) >= 200 else []
        if backfill_bybit_funding and str(sample.get("exchange")) == "bybit" and probe_events:
            sample = ensure_bybit_funding(sample, funding_cache, funding_cache_path, probe_events)
        series = parse_series(sample)
        if not series or len(series.ts) < 200:
            continue
        events = detect_pump_events(series)
        series_items.append(series)
        coverage_rows.append(
            {
                "exchange": series.exchange,
                "symbol": series.symbol,
                "first_iso": ms_to_iso(series.ts[0]),
                "last_iso": ms_to_iso(series.ts[-1]),
                "hours": round((series.ts[-1] - series.ts[0]) / 3_600_000.0, 2),
                "funding_points": len(series.funding),
                "pump_events": len(events),
            }
        )
        for event in events:
            event_rows.append(event_without_idx(event))

    strategies = build_deep_strategy_grid()
    summary_by_key: dict[tuple[str, str], Accumulator] = {}
    funding_gate_summary: dict[tuple[str, str], Accumulator] = {}
    tp_summary: dict[tuple[str, float], Accumulator] = {}
    current_family_summary: dict[tuple[str, str, float], Accumulator] = {}
    symbol_summary: dict[tuple[str, str], Accumulator] = {}

    strategy_by_name = {strategy.name: strategy for strategy in strategies}
    for series_idx, series in enumerate(series_items, start=1):
        if series_idx % 50 == 0:
            print(f"simulating series {series_idx}/{len(series_items)}", flush=True)
        events = detect_pump_events(series)
        for event in events:
            for strategy in strategies:
                row = simulate_strategy(series, event, strategy, leg_notional_usd=leg_notional_usd)
                if not row:
                    continue
                key = (series.exchange, strategy.name)
                summary_by_key.setdefault(key, Accumulator(series.exchange, strategy)).add(row)
                funding_key = (series.exchange, strategy.funding_label)
                funding_gate_summary.setdefault(
                    funding_key,
                    Accumulator(series.exchange, Strategy(0, strategy.funding_min_prev24_pct, 0, 0, 0, 0)),
                ).add(row)
                tp_key = (series.exchange, strategy.tp_pct)
                tp_summary.setdefault(
                    tp_key,
                    Accumulator(series.exchange, Strategy(0, None, 0, 0, strategy.tp_pct, 0)),
                ).add(row)
                if is_current_family(strategy):
                    current_key = (series.exchange, strategy.funding_label, strategy.tp_pct)
                    current_family_summary.setdefault(current_key, Accumulator(series.exchange, strategy)).add(row)
                symbol_key = (series.exchange, series.symbol)
                symbol_acc = symbol_summary.setdefault(symbol_key, Accumulator(series.exchange, strategy))
                symbol_acc.strategy = series.symbol
                symbol_acc.add(row)

    strategy_summary = [acc.row() for acc in summary_by_key.values()]
    strategy_summary.sort(key=lambda row: (row["exchange"], -(to_float(row.get("sum_pnl_usd")) or 0.0)))
    write_csv(output_dir / "strategy_summary.csv", strategy_summary)
    write_csv(output_dir / "coverage.csv", coverage_rows)
    write_csv(output_dir / "events.csv", event_rows)
    write_csv(output_dir / "funding_gate_summary.csv", grouped_rows(funding_gate_summary))
    write_csv(output_dir / "tp_summary.csv", grouped_rows(tp_summary))
    write_csv(output_dir / "current_family_summary.csv", grouped_rows(current_family_summary))
    write_csv(output_dir / "symbol_summary.csv", grouped_rows(symbol_summary))

    selected = select_capital_strategies(strategy_summary)
    capital_rows = run_capital_pass(
        series_items=series_items,
        strategy_by_name=strategy_by_name,
        selected_strategy_names=selected,
        capital_usd=capital_usd,
        leg_notional_usd=leg_notional_usd,
    )
    write_csv(output_dir / "capital_selected_summary.csv", capital_rows)

    report_html = render_html_report(
        coverage_rows=coverage_rows,
        strategy_summary=strategy_summary,
        funding_gate_rows=grouped_rows(funding_gate_summary),
        tp_rows=grouped_rows(tp_summary),
        current_rows=grouped_rows(current_family_summary),
        capital_rows=capital_rows,
        symbol_rows=grouped_rows(symbol_summary),
        capital_usd=capital_usd,
        leg_notional_usd=leg_notional_usd,
    )
    (output_dir / "index.html").write_text(report_html, encoding="utf-8")

    metadata = {
        "schema": "pump_short_bybit_binance_deep_dive_v1",
        "input_root": str(input_root),
        "output_dir": str(output_dir),
        "samples": len(samples),
        "series": len(series_items),
        "events": len(event_rows),
        "strategies": len(strategies),
        "strategy_summary_rows": len(strategy_summary),
        "capital_selected_rows": len(capital_rows),
        "capital_usd": capital_usd,
        "leg_notional_usd": leg_notional_usd,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def finalize_deep_dive_from_existing(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    capital_usd: float = DEFAULT_CAPITAL_USD,
    leg_notional_usd: float = DEFAULT_LEG_NOTIONAL_USD,
) -> dict[str, Any]:
    funding_cache = load_funding_cache(output_dir / "bybit_funding_cache.jsonl")
    strategy_summary = read_csv(output_dir / "strategy_summary.csv")
    coverage_rows = read_csv(output_dir / "coverage.csv")
    funding_gate_rows = read_csv(output_dir / "funding_gate_summary.csv")
    tp_rows = read_csv(output_dir / "tp_summary.csv")
    current_rows = read_csv(output_dir / "current_family_summary.csv")
    symbol_rows = read_csv(output_dir / "symbol_summary.csv")

    samples = list(load_exchange_samples(input_root, {"binance", "bybit"}))
    series_items: list[Series] = []
    for sample_idx, sample in enumerate(samples, start=1):
        if sample_idx % 100 == 0:
            print(f"finalize loading samples {sample_idx}/{len(samples)}", flush=True)
        if str(sample.get("exchange")) == "bybit":
            sample = apply_cached_bybit_funding(sample, funding_cache)
        series = parse_series(sample)
        if series and len(series.ts) >= 200:
            series_items.append(series)

    strategies = build_deep_strategy_grid()
    strategy_by_name = {strategy.name: strategy for strategy in strategies}
    selected = select_capital_strategies(strategy_summary)
    print(f"finalize selected strategies={len(selected)} series={len(series_items)}", flush=True)
    capital_rows = run_capital_pass(
        series_items=series_items,
        strategy_by_name=strategy_by_name,
        selected_strategy_names=selected,
        capital_usd=capital_usd,
        leg_notional_usd=leg_notional_usd,
    )
    write_csv(output_dir / "capital_selected_summary.csv", capital_rows)
    html = render_html_report(
        coverage_rows=coverage_rows,
        strategy_summary=strategy_summary,
        funding_gate_rows=funding_gate_rows,
        tp_rows=tp_rows,
        current_rows=current_rows,
        capital_rows=capital_rows,
        symbol_rows=symbol_rows,
        capital_usd=capital_usd,
        leg_notional_usd=leg_notional_usd,
    )
    (output_dir / "index.html").write_text(html, encoding="utf-8")
    metadata = {
        "schema": "pump_short_bybit_binance_deep_dive_finalize_v1",
        "input_root": str(input_root),
        "output_dir": str(output_dir),
        "series": len(series_items),
        "selected_strategies": len(selected),
        "capital_selected_rows": len(capital_rows),
        "capital_usd": capital_usd,
        "leg_notional_usd": leg_notional_usd,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def build_deep_strategy_grid() -> list[Strategy]:
    strategies: list[Strategy] = []
    for pullback in (10.0, 15.0, 20.0, 25.0, 30.0):
        for funding_min in (None, -2.0, -1.5, -1.0, -0.5, -0.25, -0.1):
            for ladder_legs in (1, 3, 4, 5):
                for step in (35.0, 50.0, 75.0):
                    for tp_pct in (15.0, 20.0, 25.0, 30.0, 40.0, 50.0):
                        for max_hold in (72, 168, 336):
                            strategies.append(Strategy(pullback, funding_min, ladder_legs, step, tp_pct, max_hold))
    return strategies


def load_exchange_samples(input_root: Path, exchanges: set[str]) -> Iterable[dict[str, Any]]:
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for exchange in sorted(exchanges):
        path = input_root / exchange / "symbol_samples.jsonl"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                sample = json.loads(line)
                symbol = str(sample.get("symbol") or "")
                if symbol:
                    latest[(exchange, symbol)] = sample
    yield from latest.values()


def ensure_bybit_funding(
    sample: dict[str, Any],
    cache: dict[str, list[dict[str, Any]]],
    cache_path: Path,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol = str(sample.get("symbol") or "")
    series = sample.get("series") if isinstance(sample.get("series"), dict) else {}
    candles = series.get("klines_1h") or []
    if not symbol or not candles or not events:
        return sample
    first_candle = min(int(row["ts_ms"]) for row in candles if row.get("ts_ms") is not None)
    last_candle = max(int(row["ts_ms"]) for row in candles if row.get("ts_ms") is not None)
    start_ms = max(first_candle, min(int(event["trigger_ts"]) for event in events) - 24 * 3_600_000)
    end_ms = min(last_candle, max(int(event["trigger_ts"]) for event in events) + (168 + 336) * 3_600_000)
    cached = cache.get(symbol)
    if cached is None:
        print(f"backfill bybit funding {symbol} events={len(events)} {ms_to_iso(start_ms)}..{ms_to_iso(end_ms)}", flush=True)
        cached = fetch_bybit_funding_history(symbol, start_ms=start_ms, end_ms=end_ms)
        cache[symbol] = cached
        append_jsonl(cache_path, {"symbol": symbol, "funding": cached})
    if len(cached) > len(series.get("funding") or []):
        sample = json.loads(json.dumps(sample))
        sample.setdefault("series", {})["funding"] = cached
    return sample


def apply_cached_bybit_funding(sample: dict[str, Any], cache: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    symbol = str(sample.get("symbol") or "")
    cached = cache.get(symbol)
    if not cached:
        return sample
    series = sample.get("series") if isinstance(sample.get("series"), dict) else {}
    if len(cached) <= len(series.get("funding") or []):
        return sample
    sample = json.loads(json.dumps(sample))
    sample.setdefault("series", {})["funding"] = cached
    return sample


def fetch_bybit_funding_history(symbol: str, *, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    limit = 200
    window_ms = (limit - 1) * 8 * 3_600_000
    cursor = start_ms
    while cursor <= end_ms:
        cursor_end = min(end_ms, cursor + window_ms)
        url = "https://api.bybit.com/v5/market/funding/history?" + urlencode(
            {
                "category": "linear",
                "symbol": symbol,
                "startTime": cursor,
                "endTime": cursor_end,
                "limit": limit,
            }
        )
        payload = get_json(url)
        data = ((payload.get("result") or {}).get("list") or []) if isinstance(payload, dict) else []
        for item in data:
            if not isinstance(item, dict):
                continue
            ts_ms = to_int(item.get("fundingRateTimestamp"))
            rate = to_float(item.get("fundingRate"))
            if ts_ms is not None:
                rows.append({"ts_ms": ts_ms, "funding_rate": rate})
        cursor = cursor_end + 1
        time.sleep(0.03)
    by_ts = {int(row["ts_ms"]): row for row in rows if row.get("ts_ms") is not None}
    return [by_ts[ts] for ts in sorted(by_ts)]


def get_json(url: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            req = Request(url, headers={"Accept": "application/json", "User-Agent": "FeeArbResearch/1.0"})
            with urlopen(req, timeout=30) as resp:  # nosec
                payload = json.loads(resp.read().decode("utf-8"))
            if str(payload.get("retCode", "0")) not in {"0", ""}:
                raise RuntimeError(payload.get("retMsg") or payload)
            return payload
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(last_error)


def load_funding_cache(path: Path) -> dict[str, list[dict[str, Any]]]:
    cache: dict[str, list[dict[str, Any]]] = {}
    if not path.exists():
        return cache
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            cache[str(row.get("symbol") or "")] = row.get("funding") or []
    return cache


def simulate_strategy(
    series: Series,
    event: dict[str, Any],
    strategy: Strategy,
    *,
    leg_notional_usd: float,
) -> dict[str, Any] | None:
    entry_idx = cached_pullback_entry(series, int(event["trigger_idx"]), strategy.pullback_pct, 168)
    if entry_idx is None:
        return None
    funding_prev24 = funding_sum_pct(series.funding, series.ts[entry_idx] - 24 * 3_600_000, series.ts[entry_idx])
    if strategy.funding_min_prev24_pct is not None and funding_prev24 is not None:
        if funding_prev24 <= strategy.funding_min_prev24_pct:
            return None
    legs = cached_ladder_entries(
        series,
        entry_idx,
        step_pct=strategy.ladder_step_pct,
        max_legs=strategy.ladder_legs,
        max_wait_h=min(strategy.max_hold_h, 168),
    )
    if not legs:
        return None
    result = cached_exit(series, legs, strategy, leg_notional_usd=leg_notional_usd)
    if not result:
        return None
    result.update(
        {
            "exchange": series.exchange,
            "symbol": series.symbol,
            "strategy": strategy.name,
            "funding_gate": strategy.funding_label,
            "entry_ts": series.ts[entry_idx],
            "entry_iso": ms_to_iso(series.ts[entry_idx]),
            "funding_prev24_pct": funding_prev24,
        }
    )
    return result


def cached_pullback_entry(series: Series, trigger_idx: int, pullback_pct: float, max_wait_h: int) -> int | None:
    key = (id(series), trigger_idx, pullback_pct)
    if key not in _ENTRY_CACHE:
        _ENTRY_CACHE[key] = find_pullback_entry(series, trigger_idx, pullback_pct, max_wait_h)
    return _ENTRY_CACHE[key]


def cached_ladder_entries(
    series: Series,
    entry_idx: int,
    *,
    step_pct: float,
    max_legs: int,
    max_wait_h: int,
) -> list[tuple[int, float]]:
    key = (id(series), entry_idx, step_pct, max_legs, max_wait_h)
    if key not in _LADDER_CACHE:
        _LADDER_CACHE[key] = tuple(
            ladder_entries(series, entry_idx, step_pct=step_pct, max_legs=max_legs, max_wait_h=max_wait_h)
        )
    return list(_LADDER_CACHE[key])


def cached_exit(
    series: Series,
    legs: list[tuple[int, float]],
    strategy: Strategy,
    *,
    leg_notional_usd: float,
) -> dict[str, Any] | None:
    legs_key = tuple(legs)
    key = (id(series), legs_key, strategy.tp_pct, strategy.max_hold_h, leg_notional_usd)
    if key not in _EXIT_CACHE:
        _EXIT_CACHE[key] = simulate_exit(series, legs, strategy, leg_notional_usd=leg_notional_usd)
    cached = _EXIT_CACHE[key]
    return dict(cached) if cached is not None else None


def find_pullback_entry(series: Series, trigger_idx: int, pullback_pct: float, max_wait_h: int) -> int | None:
    high_water = series.high[trigger_idx] or series.close[trigger_idx]
    end_idx = min(len(series.ts) - 1, trigger_idx + max_wait_h)
    for idx in range(trigger_idx + 1, end_idx + 1):
        current_high = series.high[idx]
        if current_high is not None and (high_water is None or current_high > high_water):
            high_water = current_high
        close = series.close[idx]
        if high_water and close and close <= high_water * (1.0 - pullback_pct / 100.0):
            return idx
    return None


def ladder_entries(series: Series, entry_idx: int, *, step_pct: float, max_legs: int, max_wait_h: int) -> list[tuple[int, float]]:
    entry_price = series.close[entry_idx]
    if not entry_price:
        return []
    legs = [(entry_idx, float(entry_price))]
    next_level = float(entry_price) * (1.0 + step_pct / 100.0)
    end_idx = min(len(series.ts) - 1, entry_idx + max_wait_h)
    for idx in range(entry_idx + 1, end_idx + 1):
        high = series.high[idx]
        if high is not None and high >= next_level:
            legs.append((idx, next_level))
            if len(legs) >= max_legs:
                break
            next_level = float(entry_price) * (1.0 + step_pct / 100.0 * len(legs))
    return legs


def simulate_exit(series: Series, legs: list[tuple[int, float]], strategy: Strategy, *, leg_notional_usd: float) -> dict[str, Any] | None:
    entry_idx = legs[0][0]
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + strategy.max_hold_h)
    if exit_limit_idx <= entry_idx:
        return None
    pending = sorted(legs)
    active: list[tuple[int, float]] = [pending[0]]
    max_mae: float | None = None
    max_mfe: float | None = None
    exit_idx = exit_limit_idx
    exit_price = series.close[exit_idx]
    exit_reason = "time_stop"

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        for leg_idx, leg_price in pending:
            if leg_idx == idx and (leg_idx, leg_price) not in active:
                active.append((leg_idx, leg_price))
        avg_entry = statistics.mean(price for _, price in active)
        high = series.high[idx]
        low = series.low[idx]
        if high is not None:
            mae_now = (high / avg_entry - 1.0) * 100.0
            max_mae = mae_now if max_mae is None else max(max_mae, mae_now)
        if low is not None:
            mfe_now = (1.0 - low / avg_entry) * 100.0
            max_mfe = mfe_now if max_mfe is None else max(max_mfe, mfe_now)
        target_price = avg_entry * (1.0 - strategy.tp_pct / 100.0)
        if low is not None and low <= target_price:
            exit_idx = idx
            exit_price = target_price
            exit_reason = "take_profit"
            break
    if not exit_price:
        return None

    filled = [(idx, price) for idx, price in pending if idx <= exit_idx]
    if not filled:
        return None
    net_pcts: list[float] = []
    price_pcts: list[float] = []
    funding_pcts: list[float] = []
    fee_pcts: list[float] = []
    pnl_usd = 0.0
    funding_usd = 0.0
    for leg_idx, entry_price in filled:
        price_pct = (1.0 - float(exit_price) / entry_price) * 100.0
        funding_pct = funding_sum_pct(series.funding, series.ts[leg_idx], series.ts[exit_idx]) or 0.0
        fee_pct = FEE_ROUNDTRIP_PCT
        net_pct = price_pct + funding_pct - fee_pct
        price_pcts.append(price_pct)
        funding_pcts.append(funding_pct)
        fee_pcts.append(fee_pct)
        net_pcts.append(net_pct)
        pnl_usd += leg_notional_usd * net_pct / 100.0
        funding_usd += leg_notional_usd * funding_pct / 100.0
    max_notional = len(filled) * leg_notional_usd
    return {
        "exit_ts": series.ts[exit_idx],
        "exit_iso": ms_to_iso(series.ts[exit_idx]),
        "exit_reason": exit_reason,
        "hold_h": round((series.ts[exit_idx] - series.ts[entry_idx]) / 3_600_000.0, 2),
        "legs_filled": len(filled),
        "max_notional_usd": max_notional,
        "net_pct": statistics.mean(net_pcts),
        "price_pnl_pct": statistics.mean(price_pcts),
        "funding_during_pct": statistics.mean(funding_pcts),
        "fee_pct": statistics.mean(fee_pcts),
        "pnl_usd": pnl_usd,
        "funding_usd": funding_usd,
        "mae_pct": max_mae,
        "mfe_pct": max_mfe,
        "win": 1 if pnl_usd > 0 else 0,
        "cat300": 1 if max_mae is not None and max_mae >= 300.0 else 0,
    }


def select_capital_strategies(strategy_summary: list[dict[str, Any]]) -> set[str]:
    selected: set[str] = set()
    by_exchange: dict[str, list[dict[str, Any]]] = {}
    for row in strategy_summary:
        by_exchange.setdefault(str(row["exchange"]), []).append(row)
        pullback = to_float(row.get("pullback_pct"))
        ladder_legs = to_float(row.get("ladder_legs"))
        ladder_step = to_float(row.get("ladder_step_pct"))
        tp_pct = to_float(row.get("tp_pct"))
        max_hold = to_float(row.get("max_hold_h"))
        if (
            pullback == 20.0
            and ladder_legs == 4.0
            and ladder_step == 50.0
            and max_hold == 168.0
            and tp_pct in {15.0, 20.0, 25.0, 30.0, 40.0, 50.0}
        ):
            selected.add(str(row["strategy"]))
        if (
            pullback == 20.0
            and ladder_legs in {3.0, 4.0, 5.0}
            and ladder_step in {35.0, 50.0}
            and tp_pct == 25.0
            and max_hold in {72.0, 168.0}
        ):
            selected.add(str(row["strategy"]))
    for rows in by_exchange.values():
        selected.update(str(row["strategy"]) for row in sorted(rows, key=lambda r: -(to_float(r.get("sum_pnl_usd")) or 0.0))[:35])
        lowish = [
            row
            for row in rows
            if (to_float(row.get("p90_mae_pct")) or 999.0) <= 60.0
            and (to_float(row.get("cat300_pct")) or 999.0) <= 0.5
            and (to_float(row.get("avg_net_pct")) or -999.0) > 0.0
        ]
        selected.update(str(row["strategy"]) for row in sorted(lowish, key=lambda r: -(to_float(r.get("sum_pnl_usd")) or 0.0))[:35])
    return selected


def run_capital_pass(
    *,
    series_items: list[Series],
    strategy_by_name: dict[str, Strategy],
    selected_strategy_names: set[str],
    capital_usd: float,
    leg_notional_usd: float,
) -> list[dict[str, Any]]:
    trades: dict[tuple[str, str], list[dict[str, Any]]] = {}
    selected = [strategy_by_name[name] for name in sorted(selected_strategy_names) if name in strategy_by_name]
    for series in series_items:
        events = detect_pump_events(series)
        for event in events:
            for strategy in selected:
                row = simulate_strategy(series, event, strategy, leg_notional_usd=leg_notional_usd)
                if not row:
                    continue
                trades.setdefault((series.exchange, strategy.name), []).append(row)
    rows: list[dict[str, Any]] = []
    for (exchange, strategy_name), items in trades.items():
        result = simulate_capital(items, capital_usd=capital_usd)
        result.update({"exchange": exchange, "strategy": strategy_name, "simulated_trades": len(items)})
        rows.append(result)
    return sorted(rows, key=lambda row: (str(row["exchange"]), -(to_float(row.get("final_capital_usd")) or 0.0)))


def simulate_capital(rows: list[dict[str, Any]], *, capital_usd: float) -> dict[str, Any]:
    locks: list[tuple[int, float]] = []
    realized = 0.0
    funding_realized = 0.0
    taken = 0
    skipped = 0
    peak_reserved = 0.0
    reserve_unit = max(to_float(row.get("max_notional_usd")) or 0.0 for row in rows) if rows else 0.0
    for row in sorted(rows, key=lambda item: int(item.get("entry_ts") or 0)):
        entry_ts = int(row.get("entry_ts") or 0)
        locks = [(exit_ts, reserve) for exit_ts, reserve in locks if exit_ts > entry_ts]
        reserved = sum(reserve for _, reserve in locks)
        reserve = max(to_float(row.get("max_notional_usd")) or 0.0, reserve_unit)
        if reserved + reserve > capital_usd + realized:
            skipped += 1
            continue
        locks.append((int(row.get("exit_ts") or entry_ts), reserve))
        realized += to_float(row.get("pnl_usd")) or 0.0
        funding_realized += to_float(row.get("funding_usd")) or 0.0
        peak_reserved = max(peak_reserved, reserved + reserve)
        taken += 1
    return {
        "initial_capital_usd": capital_usd,
        "final_capital_usd": capital_usd + realized,
        "net_pnl_usd": realized,
        "funding_pnl_usd": funding_realized,
        "roi_pct": pct(realized, capital_usd),
        "trades_taken": taken,
        "trades_skipped_capital": skipped,
        "peak_reserved_usd": peak_reserved,
    }


def render_html_report(
    *,
    coverage_rows: list[dict[str, Any]],
    strategy_summary: list[dict[str, Any]],
    funding_gate_rows: list[dict[str, Any]],
    tp_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    capital_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    capital_usd: float,
    leg_notional_usd: float,
) -> str:
    best_capital = top_per_exchange(capital_rows, "final_capital_usd")
    best_raw = top_per_exchange(strategy_summary, "sum_pnl_usd")
    lowish = [
        row
        for row in strategy_summary
        if (to_float(row.get("n")) or 0) >= 100
        and (to_float(row.get("p90_mae_pct")) or 999.0) <= 60
        and (to_float(row.get("cat300_pct")) or 999.0) <= 0.5
        and (to_float(row.get("avg_net_pct")) or -999.0) > 0
    ]
    best_lowish = top_per_exchange(lowish, "sum_pnl_usd")
    coverage_by_exchange = summarize_coverage(coverage_rows)
    html = [
        "<!doctype html><html><head><meta charset='utf-8'><title>Bybit vs Binance Pump Short Deep Dive</title>",
        "<style>",
        "body{font-family:Inter,Segoe UI,Arial,sans-serif;margin:0;background:#f6f7f9;color:#18202a}",
        "header{padding:28px 36px;background:#111827;color:white} h1{margin:0 0 8px;font-size:28px} h2{margin-top:34px}",
        "main{padding:28px 36px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}",
        ".card{background:white;border:1px solid #dbe1ea;border-radius:8px;padding:16px}.metric{font-size:26px;font-weight:700}",
        "table{border-collapse:collapse;width:100%;background:white;margin:12px 0 28px;font-size:13px}th,td{border:1px solid #dbe1ea;padding:7px 8px;text-align:right}th:first-child,td:first-child{text-align:left}th{background:#eef2f7}",
        ".note{background:#fff7ed;border-left:4px solid #f97316;padding:12px 14px;margin:18px 0}.ok{color:#047857}.bad{color:#b91c1c}",
        "</style></head><body>",
        "<header><h1>Bybit vs Binance Pump Short Deep Dive</h1>",
        f"<div>Generated {datetime.now(tz=timezone.utc).isoformat()} | capital ${capital_usd:,.0f}, ladder leg ${leg_notional_usd:,.0f}, no tail runner</div></header><main>",
        "<div class='note'>Funding sign: positive funding helps shorts, negative funding hurts shorts. Funding gate is based on previous 24h funding sum at entry.</div>",
        "<h2>Data Coverage</h2>",
        table_rows(list(coverage_by_exchange.values()), ("exchange", "symbols", "pump_symbols", "events", "funding_points")),
        "<h2>Best Capital Simulation</h2>",
        table_rows(best_capital, ("exchange", "final_capital_usd", "roi_pct", "funding_pnl_usd", "trades_taken", "trades_skipped_capital", "peak_reserved_usd", "strategy")),
        "<h2>Best Raw Rules</h2>",
        table_rows(best_raw, ("exchange", "n", "win_rate_pct", "avg_net_pct", "median_net_pct", "avg_funding_pct", "sum_funding_usd", "p90_mae_pct", "cat300_pct", "sum_pnl_usd", "strategy")),
        "<h2>Best Lower-Stress Rules</h2>",
        table_rows(best_lowish, ("exchange", "n", "win_rate_pct", "avg_net_pct", "median_net_pct", "avg_funding_pct", "sum_funding_usd", "p90_mae_pct", "cat300_pct", "sum_pnl_usd", "strategy")),
        "<h2>Funding Gate Comparison</h2>",
        table_rows(sorted(funding_gate_rows, key=lambda r: (str(r.get("exchange")), gate_sort(str(r.get("funding_gate"))))), ("exchange", "funding_gate", "n", "win_rate_pct", "avg_net_pct", "median_net_pct", "avg_funding_pct", "sum_funding_usd", "p90_mae_pct", "cat300_pct")),
        "<h2>Take-Profit Comparison</h2>",
        table_rows(sorted(tp_rows, key=lambda r: (str(r.get("exchange")), to_float(r.get("tp_pct")) or 0)), ("exchange", "tp_pct", "n", "win_rate_pct", "take_profit_rate_pct", "avg_net_pct", "median_net_pct", "avg_funding_pct", "avg_hold_h", "p90_mae_pct")),
        "<h2>Current-Family Sweep: pb20, 4 legs, 50% step, 168h</h2>",
        table_rows(sorted(current_rows, key=lambda r: (str(r.get("exchange")), gate_sort(str(r.get("funding_gate"))), to_float(r.get("tp_pct")) or 0)), ("exchange", "funding_gate", "tp_pct", "n", "win_rate_pct", "take_profit_rate_pct", "avg_net_pct", "median_net_pct", "avg_funding_pct", "p90_mae_pct", "cat300_pct")),
        "<h2>Top Symbols By Aggregate PnL</h2>",
        table_rows(sorted(symbol_rows, key=lambda r: -(to_float(r.get("sum_pnl_usd")) or 0))[:40], ("exchange", "strategy", "n", "win_rate_pct", "avg_net_pct", "sum_pnl_usd", "avg_funding_pct", "p90_mae_pct")),
        "<h2>Interpretation Checklist</h2><ul>",
        "<li>Compare <b>avg_funding_pct</b> and <b>funding_pnl_usd</b> to see whether Bybit's negative funding actually hurts selected rules.</li>",
        "<li>TP 25% is not assumed here: TP 15/20/25/30/40/50 are swept, and the TP table shows where expectancy and hold time peak.</li>",
        "<li>Rules with high final capital but p90 MAE above 60% are research candidates, not direct live defaults.</li>",
        "</ul></main></body></html>",
    ]
    return "\n".join(html)


def is_current_family(strategy: Strategy) -> bool:
    return (
        strategy.pullback_pct == 20.0
        and strategy.ladder_legs == 4
        and strategy.ladder_step_pct == 50.0
        and strategy.max_hold_h == 168
    )


def event_without_idx(event: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in event.items() if key != "trigger_idx"}


def funding_sum_pct(rows: list[tuple[int, float]], start_ms: int, end_ms: int) -> float | None:
    values = [rate * 100.0 for ts_ms, rate in rows if start_ms <= ts_ms <= end_ms]
    return sum(values) if values else None


def summarize_coverage(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        exchange = str(row.get("exchange"))
        current = out.setdefault(exchange, {"exchange": exchange, "symbols": 0, "pump_symbols": 0, "events": 0, "funding_points": 0})
        current["symbols"] += 1
        events = int(row.get("pump_events") or 0)
        current["events"] += events
        current["funding_points"] += int(row.get("funding_points") or 0)
        if events > 0:
            current["pump_symbols"] += 1
    return out


def grouped_rows(group: dict[Any, Accumulator]) -> list[dict[str, Any]]:
    rows = [acc.row() for acc in group.values()]
    return sorted(rows, key=lambda row: (str(row.get("exchange")), -(to_float(row.get("sum_pnl_usd")) or 0.0)))


def top_per_exchange(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        exchange = str(row.get("exchange"))
        value = to_float(row.get(key))
        if value is None:
            continue
        if exchange not in out or value > (to_float(out[exchange].get(key)) or float("-inf")):
            out[exchange] = row
    return sorted(out.values(), key=lambda row: -(to_float(row.get(key)) or 0.0))


def table_rows(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    out = ["<table><thead><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr></thead><tbody>"]
    for row in rows:
        out.append("<tr>" + "".join(f"<td>{format_cell(row.get(col))}</td>" for col in columns) + "</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def append_float(target: list[float], value: Any) -> None:
    num = to_float(value)
    if num is not None and math.isfinite(num):
        target.append(num)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
        handle.write("\n")


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def mean(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * q))))
    return values[idx]


def gate_sort(label: str) -> float:
    if label == "any":
        return -999
    return to_float(label.replace(">", "").replace("%", "")) or 0.0


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    num = to_float(value)
    if num is not None and str(value).strip() != "":
        return f"{num:,.2f}"
    return str(value)


def to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


__all__ = ["finalize_deep_dive_from_existing", "run_deep_dive"]
