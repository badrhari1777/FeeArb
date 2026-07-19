from __future__ import annotations

import csv
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from config import BASE_DIR

DEFAULT_INPUT_ROOT = BASE_DIR / "data" / "research" / "pump_short_multiexchange"
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_ROOT / "_comparison"
DEFAULT_CAPITAL_USD = 10_000.0
DEFAULT_LEG_NOTIONAL_USD = 1_000.0
FEE_ROUNDTRIP_PCT = 0.18

PUMP_CONFIGS: tuple[tuple[int, float], ...] = (
    (4, 50.0),
    (8, 80.0),
    (12, 100.0),
    (24, 150.0),
    (72, 250.0),
    (168, 400.0),
)


@dataclass(frozen=True, slots=True)
class Strategy:
    pullback_pct: float
    max_wait_h: int
    funding_min_prev24_pct: float | None
    ladder_legs: int
    ladder_step_pct: float
    tp_pct: float
    max_hold_h: int

    @property
    def name(self) -> str:
        funding = "any" if self.funding_min_prev24_pct is None else f"fg{self.funding_min_prev24_pct:g}"
        return (
            f"pb{self.pullback_pct:g}_wait{self.max_wait_h}_"
            f"{funding}_ladder{self.ladder_legs}_step{self.ladder_step_pct:g}_"
            f"tp{self.tp_pct:g}_hold{self.max_hold_h}"
        ).replace("-", "m").replace(".", "p")


@dataclass(slots=True)
class Series:
    exchange: str
    symbol: str
    ts: list[int]
    open: list[float | None]
    high: list[float | None]
    low: list[float | None]
    close: list[float | None]
    funding: list[tuple[int, float]]


def run_cross_exchange_research(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    capital_usd: float = DEFAULT_CAPITAL_USD,
    leg_notional_usd: float = DEFAULT_LEG_NOTIONAL_USD,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    samples = list(load_samples(input_root))
    strategies = build_strategy_grid()
    outcomes: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []

    for sample in samples:
        series = parse_series(sample)
        if not series or len(series.ts) < 200:
            continue
        events = detect_pump_events(series)
        coverage_rows.append(
            {
                "exchange": series.exchange,
                "symbol": series.symbol,
                "first_ts": series.ts[0],
                "first_iso": ms_to_iso(series.ts[0]),
                "last_ts": series.ts[-1],
                "last_iso": ms_to_iso(series.ts[-1]),
                "hours": round((series.ts[-1] - series.ts[0]) / 3_600_000.0, 2),
                "funding_points": len(series.funding),
                "pump_events": len(events),
            }
        )
        for event in events:
            event_rows.append(event)
            for strategy in strategies:
                row = simulate_strategy(series, event, strategy, leg_notional_usd=leg_notional_usd)
                if row:
                    outcomes.append(row)

    write_csv(output_dir / "events.csv", event_rows)
    write_csv(output_dir / "outcomes.csv", outcomes)
    write_csv(output_dir / "coverage.csv", coverage_rows)

    rule_summary = build_rule_summary(outcomes)
    write_csv(output_dir / "rule_summary.csv", rule_summary)

    capital_summary = build_capital_summary(outcomes, capital_usd=capital_usd)
    write_csv(output_dir / "capital_10000_summary.csv", capital_summary)

    symbol_summary = build_symbol_summary(outcomes)
    write_csv(output_dir / "symbol_summary.csv", symbol_summary)

    listing_summary = build_listing_summary(input_root, coverage_rows)
    write_csv(output_dir / "listing_summary.csv", listing_summary)

    report = build_markdown_report(
        samples=samples,
        coverage=coverage_rows,
        rule_summary=rule_summary,
        capital_summary=capital_summary,
        symbol_summary=symbol_summary,
        listing_summary=listing_summary,
        capital_usd=capital_usd,
        leg_notional_usd=leg_notional_usd,
    )
    (output_dir / "cross_exchange_report.md").write_text(report, encoding="utf-8")

    metadata = {
        "schema": "pump_short_cross_exchange_research_v1",
        "input_root": str(input_root),
        "output_dir": str(output_dir),
        "samples": len(samples),
        "coverage_rows": len(coverage_rows),
        "events": len(event_rows),
        "strategies": len(strategies),
        "outcomes": len(outcomes),
        "capital_usd": capital_usd,
        "leg_notional_usd": leg_notional_usd,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def build_strategy_grid() -> list[Strategy]:
    strategies: list[Strategy] = []
    for pullback in (10.0, 15.0, 20.0, 25.0, 30.0):
        for funding_min in (None, -1.0, -0.5, -0.25, -0.1):
            for ladder_legs in (1, 3, 4, 5):
                for step in (35.0, 50.0, 75.0):
                    for tp_pct in (20.0, 25.0, 30.0):
                        for max_hold in (72, 168, 336):
                            strategies.append(
                                Strategy(
                                    pullback_pct=pullback,
                                    max_wait_h=168,
                                    funding_min_prev24_pct=funding_min,
                                    ladder_legs=ladder_legs,
                                    ladder_step_pct=step,
                                    tp_pct=tp_pct,
                                    max_hold_h=max_hold,
                                )
                            )
    return strategies


def load_samples(input_root: Path) -> Iterable[dict[str, Any]]:
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(input_root.glob("*/symbol_samples.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    sample = json.loads(text)
                except json.JSONDecodeError:
                    continue
                exchange = str(sample.get("exchange") or sample.get("instrument", {}).get("exchange") or path.parent.name)
                symbol = normalize_symbol(sample.get("symbol") or sample.get("exchange_symbol") or "")
                if not symbol:
                    continue
                latest[(exchange, symbol)] = sample
    yield from latest.values()


def parse_series(sample: dict[str, Any]) -> Series | None:
    series = sample.get("series") if isinstance(sample.get("series"), dict) else {}
    candles = sorted(
        (row for row in series.get("klines_1h", []) if row.get("ts_ms") is not None),
        key=lambda row: int(row["ts_ms"]),
    )
    if len(candles) < 2:
        return None
    funding = [
        (int(row["ts_ms"]), float(row["funding_rate"]))
        for row in series.get("funding", [])
        if row.get("ts_ms") is not None and row.get("funding_rate") is not None
    ]
    exchange = str(sample.get("exchange") or sample.get("instrument", {}).get("exchange") or "bybit")
    symbol = str(sample.get("symbol") or sample.get("exchange_symbol") or "")
    return Series(
        exchange=exchange,
        symbol=symbol,
        ts=[int(row["ts_ms"]) for row in candles],
        open=[to_float(row.get("open")) for row in candles],
        high=[to_float(row.get("high")) for row in candles],
        low=[to_float(row.get("low")) for row in candles],
        close=[to_float(row.get("close")) for row in candles],
        funding=sorted(funding),
    )


def detect_pump_events(series: Series, *, cooldown_h: int = 72) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    ts_to_idx = {ts_ms: idx for idx, ts_ms in enumerate(series.ts)}
    for idx, ts_ms in enumerate(series.ts):
        close = series.close[idx]
        if close is None:
            continue
        for window_h, threshold_pct in PUMP_CONFIGS:
            start_ts = ts_ms - window_h * 3_600_000
            start_idx = nearest_idx_at_or_before(series.ts, start_ts)
            if start_idx is None or start_idx >= idx:
                continue
            base = series.close[start_idx]
            if not base:
                continue
            high = safe_max(series.high[start_idx : idx + 1])
            pump_pct = pct_change(high, base)
            if pump_pct is not None and pump_pct >= threshold_pct:
                candidates.append(
                    {
                        "exchange": series.exchange,
                        "symbol": series.symbol,
                        "trigger_ts": ts_ms,
                        "trigger_iso": ms_to_iso(ts_ms),
                        "trigger_idx": idx,
                        "pump_window_h": window_h,
                        "pump_threshold_pct": threshold_pct,
                        "pump_pct": pump_pct,
                    }
                )
                break
    accepted: list[dict[str, Any]] = []
    last_ts: int | None = None
    cooldown_ms = cooldown_h * 3_600_000
    for event in candidates:
        ts_ms = int(event["trigger_ts"])
        if last_ts is not None and ts_ms - last_ts < cooldown_ms:
            continue
        event = dict(event)
        event["trigger_idx"] = ts_to_idx.get(ts_ms, event["trigger_idx"])
        accepted.append(event)
        last_ts = ts_ms
    return accepted


def simulate_strategy(
    series: Series,
    event: dict[str, Any],
    strategy: Strategy,
    *,
    leg_notional_usd: float,
) -> dict[str, Any] | None:
    entry_idx = find_pullback_entry(series, int(event["trigger_idx"]), strategy.pullback_pct, strategy.max_wait_h)
    if entry_idx is None:
        return None
    funding_prev24 = funding_sum_pct(series.funding, series.ts[entry_idx] - 24 * 3_600_000, series.ts[entry_idx])
    if strategy.funding_min_prev24_pct is not None and funding_prev24 is not None:
        if funding_prev24 <= strategy.funding_min_prev24_pct:
            return None
    legs = ladder_entries(
        series,
        entry_idx,
        step_pct=strategy.ladder_step_pct,
        max_legs=strategy.ladder_legs,
        max_wait_h=min(strategy.max_hold_h, 168),
    )
    if not legs:
        return None
    result = simulate_exit(series, legs, strategy, leg_notional_usd=leg_notional_usd)
    if not result:
        return None
    row = {key: value for key, value in event.items() if key != "trigger_idx"}
    row.update(
        {
            "strategy": strategy.name,
            "pullback_pct": strategy.pullback_pct,
            "funding_min_prev24_pct": strategy.funding_min_prev24_pct,
            "ladder_legs_target": strategy.ladder_legs,
            "ladder_step_pct": strategy.ladder_step_pct,
            "tp_pct": strategy.tp_pct,
            "max_hold_h": strategy.max_hold_h,
            "entry_ts": series.ts[entry_idx],
            "entry_iso": ms_to_iso(series.ts[entry_idx]),
            "entry_price": series.close[entry_idx],
            "funding_prev24_pct": funding_prev24,
        }
    )
    row.update(result)
    return row


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


def ladder_entries(
    series: Series,
    entry_idx: int,
    *,
    step_pct: float,
    max_legs: int,
    max_wait_h: int,
) -> list[tuple[int, float]]:
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


def simulate_exit(
    series: Series,
    legs: list[tuple[int, float]],
    strategy: Strategy,
    *,
    leg_notional_usd: float,
) -> dict[str, Any] | None:
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
    leg_net_pcts: list[float] = []
    leg_funding_pcts: list[float] = []
    pnl_usd = 0.0
    for leg_idx, entry_price in filled:
        price_pnl_pct = (1.0 - float(exit_price) / entry_price) * 100.0
        funding_pct = funding_sum_pct(series.funding, series.ts[leg_idx], series.ts[exit_idx]) or 0.0
        net_pct = price_pnl_pct + funding_pct - FEE_ROUNDTRIP_PCT
        leg_net_pcts.append(net_pct)
        leg_funding_pcts.append(funding_pct)
        pnl_usd += leg_notional_usd * net_pct / 100.0
    max_notional = len(filled) * leg_notional_usd
    return {
        "exit_ts": series.ts[exit_idx],
        "exit_iso": ms_to_iso(series.ts[exit_idx]),
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "hold_h": round((series.ts[exit_idx] - series.ts[entry_idx]) / 3_600_000.0, 2),
        "legs_filled": len(filled),
        "max_notional_usd": max_notional,
        "net_pct": statistics.mean(leg_net_pcts),
        "funding_during_pct": statistics.mean(leg_funding_pcts) if leg_funding_pcts else 0.0,
        "pnl_usd": pnl_usd,
        "mae_pct": max_mae,
        "mfe_pct": max_mfe,
        "win": 1 if pnl_usd > 0 else 0,
        "cat300": 1 if max_mae is not None and max_mae >= 300.0 else 0,
    }


def build_rule_summary(outcomes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in outcomes:
        grouped.setdefault((str(row["exchange"]), str(row["strategy"])), []).append(row)
    summary: list[dict[str, Any]] = []
    for (exchange, strategy), rows in grouped.items():
        nets = values(rows, "net_pct")
        pnls = values(rows, "pnl_usd")
        maes = values(rows, "mae_pct")
        summary.append(
            {
                "exchange": exchange,
                "strategy": strategy,
                "n": len(rows),
                "win_rate_pct": pct(sum(int(row.get("win") or 0) for row in rows), len(rows)),
                "avg_net_pct": mean(nets),
                "median_net_pct": median(nets),
                "sum_pnl_usd": sum(pnls),
                "avg_pnl_usd": mean(pnls),
                "p90_mae_pct": quantile(maes, 0.90),
                "p95_mae_pct": quantile(maes, 0.95),
                "cat300_pct": pct(sum(int(row.get("cat300") or 0) for row in rows), len(rows)),
                "avg_hold_h": mean(values(rows, "hold_h")),
                "avg_legs": mean(values(rows, "legs_filled")),
                "avg_funding_pct": mean(values(rows, "funding_during_pct")),
            }
        )
    return sorted(summary, key=lambda row: (str(row["exchange"]), -(to_float(row.get("sum_pnl_usd")) or 0.0)))


def build_capital_summary(outcomes: list[dict[str, Any]], *, capital_usd: float) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in outcomes:
        grouped.setdefault((str(row["exchange"]), str(row["strategy"])), []).append(row)
    rows: list[dict[str, Any]] = []
    for (exchange, strategy), items in grouped.items():
        result = simulate_capital(items, capital_usd=capital_usd)
        result.update({"exchange": exchange, "strategy": strategy})
        rows.append(result)
    return sorted(rows, key=lambda row: -(to_float(row.get("final_capital_usd")) or 0.0))


def simulate_capital(rows: list[dict[str, Any]], *, capital_usd: float) -> dict[str, Any]:
    locks: list[tuple[int, float]] = []
    realized = 0.0
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
        peak_reserved = max(peak_reserved, reserved + reserve)
        taken += 1
    return {
        "initial_capital_usd": capital_usd,
        "final_capital_usd": capital_usd + realized,
        "net_pnl_usd": realized,
        "roi_pct": pct(realized, capital_usd),
        "trades_taken": taken,
        "trades_skipped_capital": skipped,
        "peak_reserved_usd": peak_reserved,
    }


def build_symbol_summary(outcomes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in outcomes:
        grouped.setdefault((str(row["exchange"]), str(row["symbol"])), []).append(row)
    rows: list[dict[str, Any]] = []
    for (exchange, symbol), items in grouped.items():
        pnls = values(items, "pnl_usd")
        rows.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "outcomes": len(items),
                "events": len({(row.get("trigger_ts"), row.get("pump_window_h")) for row in items}),
                "sum_pnl_usd": sum(pnls),
                "avg_net_pct": mean(values(items, "net_pct")),
                "best_strategy_pnl_usd": max(pnls) if pnls else None,
                "worst_strategy_pnl_usd": min(pnls) if pnls else None,
            }
        )
    return sorted(rows, key=lambda row: -(to_float(row.get("sum_pnl_usd")) or 0.0))


def build_listing_summary(input_root: Path, coverage_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    symbols_by_exchange: dict[str, set[str]] = {}
    for path in sorted(input_root.glob("*/instruments_latest.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        exchange = str(payload.get("exchange") or path.parent.name)
        symbols = {
            normalize_symbol(item.get("canonical_symbol") or item.get("symbol"))
            for item in payload.get("symbols", [])
            if isinstance(item, dict)
        }
        symbols_by_exchange[exchange] = {symbol for symbol in symbols if symbol}
    pump_symbols_by_exchange: dict[str, set[str]] = {}
    for row in coverage_rows:
        if int(row.get("pump_events") or 0) <= 0:
            continue
        pump_symbols_by_exchange.setdefault(str(row["exchange"]), set()).add(str(row["symbol"]))
    all_symbols = sorted(set().union(*symbols_by_exchange.values()) if symbols_by_exchange else set())
    rows: list[dict[str, Any]] = []
    exchanges = sorted(symbols_by_exchange)
    for symbol in all_symbols:
        listed = [exchange for exchange in exchanges if symbol in symbols_by_exchange.get(exchange, set())]
        pumped = [exchange for exchange in exchanges if symbol in pump_symbols_by_exchange.get(exchange, set())]
        rows.append(
            {
                "symbol": symbol,
                "listed_exchange_count": len(listed),
                "listed_exchanges": ",".join(listed),
                "pump_exchange_count": len(pumped),
                "pump_exchanges": ",".join(pumped),
                "listed_on_binance": "binance" in listed,
            }
        )
    return sorted(rows, key=lambda row: (-int(row["pump_exchange_count"]), -int(row["listed_exchange_count"]), row["symbol"]))


def build_markdown_report(
    *,
    samples: list[dict[str, Any]],
    coverage: list[dict[str, Any]],
    rule_summary: list[dict[str, Any]],
    capital_summary: list[dict[str, Any]],
    symbol_summary: list[dict[str, Any]],
    listing_summary: list[dict[str, Any]],
    capital_usd: float,
    leg_notional_usd: float,
) -> str:
    exchanges = sorted({str(row.get("exchange") or "") for row in coverage if row.get("exchange")})
    lines = [
        "# Pump-short cross-exchange report",
        "",
        f"Generated: {datetime.now(tz=timezone.utc).isoformat()}",
        f"Capital model: start ${capital_usd:,.0f}, each ladder leg ${leg_notional_usd:,.0f}, no tail runner.",
        "",
        "## Data coverage",
        "",
        "| Exchange | Symbols with samples | Pump symbols | Events | Funding points |",
        "|---|---:|---:|---:|---:|",
    ]
    for exchange in exchanges:
        rows = [row for row in coverage if row.get("exchange") == exchange]
        pump_symbols = {row["symbol"] for row in rows if int(row.get("pump_events") or 0) > 0}
        lines.append(
            f"| {exchange} | {len(rows)} | {len(pump_symbols)} | "
            f"{sum(int(row.get('pump_events') or 0) for row in rows)} | "
            f"{sum(int(row.get('funding_points') or 0) for row in rows)} |"
        )
    lines.extend(["", "## Best capital results", "", table(top_rows(capital_summary, 20), (
        "exchange",
        "strategy",
        "final_capital_usd",
        "roi_pct",
        "trades_taken",
        "trades_skipped_capital",
        "peak_reserved_usd",
    ))])
    lines.extend(["", "## Best rules by raw outcomes", "", table(top_rows(rule_summary, 20), (
        "exchange",
        "strategy",
        "n",
        "win_rate_pct",
        "avg_net_pct",
        "median_net_pct",
        "sum_pnl_usd",
        "p90_mae_pct",
        "cat300_pct",
    ))])
    lines.extend(["", "## Most interesting symbols", "", table(top_rows(symbol_summary, 30), (
        "exchange",
        "symbol",
        "events",
        "sum_pnl_usd",
        "avg_net_pct",
        "best_strategy_pnl_usd",
        "worst_strategy_pnl_usd",
    ))])
    interesting_not_binance = [
        row for row in listing_summary if row.get("pump_exchange_count") and not row.get("listed_on_binance")
    ][:30]
    lines.extend(["", "## Pump symbols not listed on Binance", "", table(interesting_not_binance, (
        "symbol",
        "listed_exchange_count",
        "listed_exchanges",
        "pump_exchange_count",
        "pump_exchanges",
    ))])
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This report is a historical research simulation, not a live-ready signal by itself.",
            "- OI and long/short history is not consistently available across exchanges, so the cross-exchange layer uses price plus funding.",
            "- The main no-tail approach is preserved: full close by take-profit or time stop.",
        ]
    )
    return "\n".join(lines) + "\n"


def table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> str:
    if not rows:
        return "_No rows._"
    out = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        out.append("| " + " | ".join(format_cell(row.get(col)) for col in columns) + " |")
    return "\n".join(out)


def top_rows(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    return list(rows[:n])


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not fieldnames:
            handle.write("")
            return
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def nearest_idx_at_or_before(ts: list[int], target: int) -> int | None:
    lo = 0
    hi = len(ts) - 1
    best: int | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if ts[mid] <= target:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def funding_sum_pct(rows: list[tuple[int, float]], start_ms: int, end_ms: int) -> float | None:
    values = [rate * 100.0 for ts_ms, rate in rows if start_ms <= ts_ms <= end_ms]
    return sum(values) if values else None


def safe_max(values_: Iterable[float | None]) -> float | None:
    vals = [float(value) for value in values_ if value is not None and math.isfinite(float(value))]
    return max(vals) if vals else None


def values(rows: list[dict[str, Any]], key: str) -> list[float]:
    vals: list[float] = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None:
            vals.append(value)
    return vals


def mean(vals: list[float]) -> float | None:
    return statistics.mean(vals) if vals else None


def median(vals: list[float]) -> float | None:
    return statistics.median(vals) if vals else None


def quantile(vals: list[float], q: float) -> float | None:
    if not vals:
        return None
    vals = sorted(vals)
    idx = min(len(vals) - 1, max(0, int(round((len(vals) - 1) * q))))
    return vals[idx]


def pct(part: float, total: float) -> float | None:
    if not total:
        return None
    return part / total * 100.0


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in {None, 0}:
        return None
    return (float(current) / float(previous) - 1.0) * 100.0


def to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def normalize_symbol(value: Any) -> str:
    text = str(value or "").upper().strip()
    for part in ("/", ":", "-", "_"):
        text = text.replace(part, "")
    return text


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def ms_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


__all__ = ["run_cross_exchange_research"]
