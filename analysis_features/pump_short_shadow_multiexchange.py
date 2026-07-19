from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from analysis_collectors.ccxt_pump_short_history import (
    CcxtPumpShortCollectorConfig,
    CcxtPumpShortHistoryCollector,
    now_ms,
)
from analysis_features.pump_short_cross_exchange_research import (
    PUMP_CONFIGS,
    Series,
    detect_pump_events,
    funding_sum_pct,
    ms_to_iso,
    normalize_symbol,
    parse_series,
    safe_max,
    to_float,
    write_csv,
)
from config import BASE_DIR

DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_shadow_binance_bybit"
DEFAULT_EXCHANGES: tuple[str, ...] = ("binance", "bybit")
DEFAULT_LEG_NOTIONAL_USD = 1_000.0


@dataclass(frozen=True, slots=True)
class ShadowVariant:
    name: str
    pullback_pct: float
    funding_min_prev24_pct: float | None
    oi_max_24h_pct: float | None
    ladder_legs: int
    ladder_step_pct: float
    tp_pct: float
    max_hold_h: int
    min_velocity_pct_h: float | None = None
    min_pump_pct: float | None = None


@dataclass(slots=True)
class ShadowScanConfig:
    output_dir: Path = DEFAULT_OUTPUT_DIR
    exchanges: tuple[str, ...] = DEFAULT_EXCHANGES
    lookback_days: int = 21
    recent_event_hours: int = 168
    sleep_sec: float = 0.05
    max_symbols_per_exchange: int | None = None
    symbols: tuple[str, ...] = ()
    daily_prefilter: bool = True
    min_daily_pump_pct: float = 35.0
    min_3d_pump_pct: float = 70.0
    min_7d_pump_pct: float = 120.0
    leg_notional_usd: float = DEFAULT_LEG_NOTIONAL_USD
    orderbook_slippage_bps: float = 20.0


VARIANTS: tuple[ShadowVariant, ...] = (
    ShadowVariant(
        name="default_tp25",
        pullback_pct=20.0,
        funding_min_prev24_pct=-0.50,
        oi_max_24h_pct=50.0,
        ladder_legs=4,
        ladder_step_pct=50.0,
        tp_pct=25.0,
        max_hold_h=168,
    ),
    ShadowVariant(
        name="default_tp50_avg",
        pullback_pct=20.0,
        funding_min_prev24_pct=-0.50,
        oi_max_24h_pct=50.0,
        ladder_legs=4,
        ladder_step_pct=50.0,
        tp_pct=50.0,
        max_hold_h=168,
    ),
    ShadowVariant(
        name="fast_pb10_tp25",
        pullback_pct=10.0,
        funding_min_prev24_pct=-0.50,
        oi_max_24h_pct=100.0,
        ladder_legs=4,
        ladder_step_pct=50.0,
        tp_pct=25.0,
        max_hold_h=168,
    ),
    ShadowVariant(
        name="speed_or_superpump_tp50",
        pullback_pct=20.0,
        funding_min_prev24_pct=-0.50,
        oi_max_24h_pct=100.0,
        ladder_legs=4,
        ladder_step_pct=50.0,
        tp_pct=50.0,
        max_hold_h=168,
        min_velocity_pct_h=15.0,
        min_pump_pct=150.0,
    ),
    ShadowVariant(
        name="funding_soft_tp25",
        pullback_pct=20.0,
        funding_min_prev24_pct=-1.00,
        oi_max_24h_pct=50.0,
        ladder_legs=4,
        ladder_step_pct=50.0,
        tp_pct=25.0,
        max_hold_h=168,
    ),
)


def run_shadow_scan(config: ShadowScanConfig | None = None) -> dict[str, Any]:
    cfg = config or ShadowScanConfig()
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    scan_ts = now_ms()
    all_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []
    samples_written = 0
    requests_made = 0

    for exchange in cfg.exchanges:
        try:
            result = scan_exchange(exchange, cfg=cfg, scan_ts=scan_ts)
        except Exception as exc:  # pylint: disable=broad-except
            error_rows.append(
                {
                    "ts_ms": scan_ts,
                    "ts_iso": ms_to_iso(scan_ts),
                    "exchange": exchange,
                    "symbol": "",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        all_rows.extend(result["rows"])
        candidate_rows.extend([row for row in result["rows"] if row.get("status") == "entry_candidate"])
        error_rows.extend(result["errors"])
        samples_written += int(result["samples_written"])
        requests_made += int(result["requests_made"])

    all_rows.sort(key=shadow_sort_key)
    candidate_rows.sort(key=shadow_sort_key)
    write_csv(cfg.output_dir / "shadow_scan_latest.csv", all_rows)
    write_csv(cfg.output_dir / "shadow_candidates_latest.csv", candidate_rows)
    write_csv(cfg.output_dir / "shadow_errors_latest.csv", error_rows)
    append_jsonl(cfg.output_dir / "shadow_scan_history.jsonl", {"ts_ms": scan_ts, "rows": all_rows})

    metadata = {
        "schema": "pump_short_shadow_multiexchange_v1",
        "ts_ms": scan_ts,
        "ts_iso": ms_to_iso(scan_ts),
        "output_dir": str(cfg.output_dir),
        "exchanges": list(cfg.exchanges),
        "lookback_days": cfg.lookback_days,
        "recent_event_hours": cfg.recent_event_hours,
        "symbols_requested": list(cfg.symbols),
        "rows": len(all_rows),
        "entry_candidates": len(candidate_rows),
        "watch_rows": sum(1 for row in all_rows if str(row.get("status") or "").startswith("watch")),
        "blocked_rows": sum(1 for row in all_rows if str(row.get("status") or "").startswith("blocked")),
        "samples_written": samples_written,
        "errors": len(error_rows),
        "requests_made": requests_made,
        "variants": [asdict(variant) for variant in VARIANTS],
        "elapsed_sec": round(time.time() - started, 3),
    }
    (cfg.output_dir / "shadow_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report(cfg.output_dir, metadata, all_rows, candidate_rows)
    return metadata


def scan_exchange(exchange: str, *, cfg: ShadowScanConfig, scan_ts: int) -> dict[str, Any]:
    start_ms = scan_ts - cfg.lookback_days * 86_400_000
    collector = CcxtPumpShortHistoryCollector(
        CcxtPumpShortCollectorConfig(
            exchange=exchange,
            output_root=cfg.output_dir / "_collector_tmp",
            start_ms=start_ms,
            end_ms=scan_ts,
            sleep_sec=cfg.sleep_sec,
            daily_prefilter=cfg.daily_prefilter,
            min_daily_pump_pct=cfg.min_daily_pump_pct,
            min_3d_pump_pct=cfg.min_3d_pump_pct,
            min_7d_pump_pct=cfg.min_7d_pump_pct,
        )
    )
    instruments = collector.load_instruments()
    requested = {normalize_symbol(symbol) for symbol in cfg.symbols if normalize_symbol(symbol)}
    if requested:
        instruments = [
            item
            for item in instruments
            if normalize_symbol(item.get("canonical_symbol") or item.get("symbol")) in requested
            or normalize_symbol(item.get("symbol")) in requested
        ]
    instruments = sorted(instruments, key=lambda item: str(item.get("canonical_symbol") or item.get("symbol") or ""))
    if cfg.max_symbols_per_exchange is not None:
        instruments = instruments[: max(0, cfg.max_symbols_per_exchange)]

    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    samples_written = 0
    for index, instrument in enumerate(instruments, start=1):
        symbol = str(instrument.get("symbol") or "")
        try:
            if cfg.daily_prefilter and not passes_recent_prefilter(collector, symbol, cfg, start_ms, scan_ts):
                continue
            sample = collector.collect_symbol(instrument)
            sample_rows = classify_sample(
                sample,
                scan_ts=scan_ts,
                instrument=instrument,
                cfg=cfg,
                collector=collector,
                scan_index=index,
            )
            if sample_rows:
                rows.extend(sample_rows)
                append_sample(cfg.output_dir, exchange, sample)
                samples_written += 1
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(
                {
                    "ts_ms": scan_ts,
                    "ts_iso": ms_to_iso(scan_ts),
                    "exchange": exchange,
                    "symbol": symbol,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    return {
        "rows": rows,
        "errors": errors,
        "samples_written": samples_written,
        "requests_made": collector.stats.requests_made,
    }


def passes_recent_prefilter(
    collector: CcxtPumpShortHistoryCollector,
    symbol: str,
    cfg: ShadowScanConfig,
    start_ms: int,
    end_ms: int,
) -> bool:
    rows = collector.fetch_ohlcv(symbol, "1d", start_ms, end_ms, 1000)
    if len(rows) < 2:
        return False
    for idx, row in enumerate(rows):
        high = to_float(row.get("high"))
        if high is None:
            continue
        for days, threshold in (
            (1, cfg.min_daily_pump_pct),
            (3, cfg.min_3d_pump_pct),
            (7, cfg.min_7d_pump_pct),
        ):
            start = max(0, idx - days)
            base = to_float(rows[start].get("close"))
            if base and (high / base - 1.0) * 100.0 >= threshold:
                return True
    return False


def classify_sample(
    sample: dict[str, Any],
    *,
    scan_ts: int,
    instrument: dict[str, Any],
    cfg: ShadowScanConfig,
    collector: CcxtPumpShortHistoryCollector,
    scan_index: int,
) -> list[dict[str, Any]]:
    series = parse_series(sample)
    if not series or len(series.ts) < 2:
        return []
    events = detect_pump_events(series)
    event = latest_recent_event(series, events, cfg.recent_event_hours)
    base = base_row(sample, series, scan_ts, scan_index)
    if event is None:
        return [{**base, "variant": "", "status": "no_recent_pump", "reason": "no_pump_trigger_in_recent_window"}]

    latest_idx = len(series.ts) - 1
    features = event_features(series, event, latest_idx, oi_points=sample_oi_points(sample))
    rows: list[dict[str, Any]] = []
    liquidity_cache: dict[str, Any] | None = None
    for variant in VARIANTS:
        status, reason = classify_variant(features, variant)
        row = {
            **base,
            **features,
            "variant": variant.name,
            "status": status,
            "reason": reason,
            "pullback_rule_pct": variant.pullback_pct,
            "funding_min_prev24_pct": variant.funding_min_prev24_pct,
            "oi_max_24h_pct": variant.oi_max_24h_pct,
            "ladder_legs": variant.ladder_legs,
            "ladder_step_pct": variant.ladder_step_pct,
            "tp_pct": variant.tp_pct,
            "max_hold_h": variant.max_hold_h,
            "planned_gross_notional_usd": round(cfg.leg_notional_usd * variant.ladder_legs, 2),
            "first_leg_notional_usd": cfg.leg_notional_usd,
        }
        if status == "entry_candidate":
            if liquidity_cache is None:
                liquidity_cache = estimate_entry_liquidity(collector, instrument, cfg)
            row.update(liquidity_cache)
        rows.append(row)
    return rows


def latest_recent_event(series: Series, events: list[dict[str, Any]], recent_event_hours: int) -> dict[str, Any] | None:
    if not events or not series.ts:
        return None
    latest_ts = series.ts[-1]
    recent = [
        event
        for event in events
        if 0 <= latest_ts - int(event.get("trigger_ts") or 0) <= recent_event_hours * 3_600_000
    ]
    if not recent:
        return None
    return max(recent, key=lambda item: int(item.get("trigger_ts") or 0))


def base_row(sample: dict[str, Any], series: Series, scan_ts: int, scan_index: int) -> dict[str, Any]:
    summary = sample.get("summary") if isinstance(sample.get("summary"), dict) else {}
    return {
        "ts_ms": scan_ts,
        "ts_iso": ms_to_iso(scan_ts),
        "exchange": series.exchange,
        "symbol": series.symbol,
        "exchange_symbol": sample.get("exchange_symbol") or sample.get("instrument", {}).get("symbol") or series.symbol,
        "scan_index": scan_index,
        "last_ts": series.ts[-1] if series.ts else None,
        "last_iso": ms_to_iso(series.ts[-1]) if series.ts else None,
        "last_close": series.close[-1] if series.close else None,
        "return_24h_pct": recent_return(series, 24),
        "return_3d_pct": recent_return(series, 72),
        "return_7d_pct": recent_return(series, 168),
        "price_history_hours": summary.get("price_history_hours"),
    }


def event_features(
    series: Series,
    event: dict[str, Any],
    latest_idx: int,
    *,
    oi_points: dict[int, float],
) -> dict[str, Any]:
    trigger_idx = int(event["trigger_idx"])
    trigger_ts = int(event["trigger_ts"])
    high_since_trigger = safe_max(series.high[trigger_idx : latest_idx + 1])
    last_close = series.close[latest_idx]
    pullback = (1.0 - float(last_close) / high_since_trigger) * 100.0 if last_close and high_since_trigger else None
    funding_prev24 = funding_sum_pct(series.funding, series.ts[latest_idx] - 24 * 3_600_000, series.ts[latest_idx])
    oi_change_24h = oi_change_pct(series, latest_idx, 24, oi_points=oi_points)
    pump_pct = to_float(event.get("pump_pct"))
    window_h = to_float(event.get("pump_window_h"))
    velocity = pump_pct / window_h if pump_pct is not None and window_h else None
    return {
        "trigger_ts": trigger_ts,
        "trigger_iso": ms_to_iso(trigger_ts),
        "hours_since_trigger": round((series.ts[latest_idx] - trigger_ts) / 3_600_000.0, 3),
        "pump_window_h": event.get("pump_window_h"),
        "pump_threshold_pct": event.get("pump_threshold_pct"),
        "pump_pct": round_float(pump_pct),
        "pump_velocity_pct_h": round_float(velocity),
        "high_since_trigger": round_float(high_since_trigger),
        "pullback_from_high_pct": round_float(pullback),
        "funding_prev24_pct": round_float(funding_prev24),
        "oi_change_24h_pct": round_float(oi_change_24h),
    }


def classify_variant(features: dict[str, Any], variant: ShadowVariant) -> tuple[str, str]:
    pump_pct = to_float(features.get("pump_pct"))
    velocity = to_float(features.get("pump_velocity_pct_h"))
    if variant.min_pump_pct is not None and variant.min_velocity_pct_h is not None:
        if (pump_pct or 0.0) < variant.min_pump_pct and (velocity or 0.0) < variant.min_velocity_pct_h:
            return "watch_speed", "waiting_speed_or_superpump_condition"
    pullback = to_float(features.get("pullback_from_high_pct"))
    if pullback is None or pullback < variant.pullback_pct:
        return "watch_pullback", f"waiting_pullback_{variant.pullback_pct:g}"
    funding = to_float(features.get("funding_prev24_pct"))
    if variant.funding_min_prev24_pct is not None and funding is not None:
        if funding <= variant.funding_min_prev24_pct:
            return "blocked_funding", f"funding_prev24_lte_{variant.funding_min_prev24_pct:g}"
    oi_change = to_float(features.get("oi_change_24h_pct"))
    if variant.oi_max_24h_pct is not None and oi_change is not None:
        if oi_change > variant.oi_max_24h_pct:
            return "watch_oi", f"oi_change_gt_{variant.oi_max_24h_pct:g}"
    return "entry_candidate", "matched_shadow_variant"


def estimate_entry_liquidity(
    collector: CcxtPumpShortHistoryCollector,
    instrument: dict[str, Any],
    cfg: ShadowScanConfig,
) -> dict[str, Any]:
    symbol = str(instrument.get("symbol") or "")
    market = instrument.get("raw") if isinstance(instrument.get("raw"), dict) else {}
    contract_size = to_float(instrument.get("contract_size") or market.get("contractSize")) or 1.0
    try:
        collector._request_pause()  # pylint: disable=protected-access
        orderbook = collector.client.fetch_order_book(symbol, limit=100)
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "entry_liquidity_ok": 0,
            "entry_liquidity_error": f"{type(exc).__name__}: {exc}",
        }
    bids = orderbook.get("bids") or []
    if not bids:
        return {"entry_liquidity_ok": 0, "entry_liquidity_error": "empty_bids"}
    best_bid = to_float(bids[0][0])
    if not best_bid:
        return {"entry_liquidity_ok": 0, "entry_liquidity_error": "missing_best_bid"}
    min_price = best_bid * (1.0 - cfg.orderbook_slippage_bps / 10_000.0)
    notional = 0.0
    weighted_price = 0.0
    for level in bids:
        price = to_float(level[0])
        amount = to_float(level[1])
        if price is None or amount is None or price < min_price:
            break
        level_notional = price * amount * contract_size
        notional += level_notional
        weighted_price += price * level_notional
        if notional >= cfg.leg_notional_usd:
            break
    avg_price = weighted_price / notional if notional > 0 else None
    slippage_bps = (1.0 - avg_price / best_bid) * 10_000.0 if avg_price and best_bid else None
    return {
        "entry_liquidity_ok": 1 if notional >= cfg.leg_notional_usd else 0,
        "entry_best_bid": best_bid,
        "entry_max_short_notional_usd": round(notional, 2),
        "entry_est_slippage_bps": round_float(slippage_bps, 4),
        "entry_orderbook_slippage_limit_bps": cfg.orderbook_slippage_bps,
    }


def sample_oi_points(sample: dict[str, Any]) -> dict[int, float]:
    series = sample.get("series") if isinstance(sample.get("series"), dict) else {}
    points: dict[int, float] = {}
    for row in series.get("open_interest_1h", []) or []:
        if not isinstance(row, dict) or row.get("ts_ms") is None:
            continue
        value = to_float(
            row.get("open_interest")
            or row.get("openInterest")
            or row.get("openInterestAmount")
            or row.get("baseVolume")
            or row.get("quoteVolume")
        )
        if value is not None:
            points[int(row["ts_ms"])] = value
    return points


def oi_change_pct(series: Series, idx: int, hours: int, *, oi_points: dict[int, float]) -> float | None:
    if not oi_points:
        return None
    current = oi_points.get(series.ts[idx])
    prior_idx = idx - hours
    if prior_idx < 0:
        return None
    prior = oi_points.get(series.ts[prior_idx])
    if current is None or prior in {None, 0.0}:
        return None
    return (current / prior - 1.0) * 100.0


def recent_return(series: Series, hours: int) -> float | None:
    if len(series.ts) <= hours or not series.close:
        return None
    last = series.close[-1]
    prior = series.close[-1 - hours]
    if last is None or prior in {None, 0.0}:
        return None
    return round((last / prior - 1.0) * 100.0, 6)


def append_sample(output_dir: Path, exchange: str, sample: dict[str, Any]) -> None:
    path = output_dir / "interesting_samples" / exchange / "symbol_samples.jsonl"
    append_jsonl(path, sample)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
        handle.write("\n")


def shadow_sort_key(row: dict[str, Any]) -> tuple[int, str, str, str]:
    status_rank = {
        "entry_candidate": 0,
        "watch_pullback": 1,
        "watch_speed": 2,
        "watch_oi": 3,
        "blocked_funding": 4,
        "no_recent_pump": 5,
    }.get(str(row.get("status") or ""), 9)
    return (
        status_rank,
        str(row.get("exchange") or ""),
        str(row.get("symbol") or ""),
        str(row.get("variant") or ""),
    )


def write_report(
    output_dir: Path,
    metadata: dict[str, Any],
    rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> None:
    by_exchange = summarize(rows, ("exchange", "status"))
    by_variant = summarize(rows, ("exchange", "variant", "status"))
    lines = [
        "# Pump Short Shadow Binance/Bybit",
        "",
        f"- Scan: `{metadata['ts_iso']}`",
        f"- Exchanges: `{', '.join(metadata['exchanges'])}`",
        f"- Lookback days: `{metadata['lookback_days']}`",
        f"- Rows: `{metadata['rows']}`",
        f"- Entry candidates: `{metadata['entry_candidates']}`",
        f"- Interesting samples saved: `{metadata['samples_written']}`",
        f"- Requests: `{metadata['requests_made']}`",
        "",
        "## Status By Exchange",
        "",
    ]
    lines.extend(markdown_table(by_exchange, ("exchange", "status", "n")))
    lines.extend(["", "## Status By Variant", ""])
    lines.extend(markdown_table(by_variant, ("exchange", "variant", "status", "n")))
    lines.extend(["", "## Entry Candidates", ""])
    lines.extend(
        markdown_table(
            candidates,
            (
                "exchange",
                "symbol",
                "variant",
                "last_close",
                "pump_pct",
                "pump_velocity_pct_h",
                "pullback_from_high_pct",
                "funding_prev24_pct",
                "tp_pct",
                "entry_liquidity_ok",
                "entry_max_short_notional_usd",
            ),
            limit=100,
        )
    )
    (output_dir / "shadow_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], int] = {}
    for row in rows:
        grouped[tuple(row.get(key) for key in keys)] = grouped.get(tuple(row.get(key) for key in keys), 0) + 1
    out = []
    for key_values, count in grouped.items():
        item = {key: value for key, value in zip(keys, key_values)}
        item["n"] = count
        out.append(item)
    return sorted(out, key=lambda item: tuple(str(item.get(key) or "") for key in keys))


def markdown_table(rows: list[dict[str, Any]], columns: tuple[str, ...], *, limit: int = 50) -> list[str]:
    if not rows:
        return ["_No rows._"]
    out = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows[:limit]:
        out.append("| " + " | ".join(format_cell(row.get(column)) for column in columns) + " |")
    return out


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def round_float(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    return round(float(value), digits)


__all__ = ["ShadowScanConfig", "ShadowVariant", "VARIANTS", "run_shadow_scan"]
