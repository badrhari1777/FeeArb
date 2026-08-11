from __future__ import annotations

import csv
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from analysis_collectors.bybit_pump_short import BybitCollectorConfig, BybitPumpShortCollector
from analysis_features.bybit_pump_short_outcomes import Series, point_change_pct, sample_to_series
from config import BASE_DIR
from execution.pump_live import (
    MARGIN_MANAGER_V4_ON_DEMAND,
    ladder_prefund_plan,
    projected_short_liquidation_after_fill,
    required_margin_for_liq_buffer_usd,
)

HOUR_MS = 3_600_000
START_TS_MS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
HUSDT_EVENT_UID = "HUSDT_w24_150_1781046000000"
DEFAULT_SAMPLES = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_EVENTS = BASE_DIR / "data" / "research" / "pump_short_pullback_tier_research" / "pullback_event_summary.csv"
DEFAULT_OUTPUT = BASE_DIR / "data" / "research" / "pump_short_funding_corrected_portfolio"


@dataclass(frozen=True, slots=True)
class Tier:
    min_pump_pct: float
    pullback_pct: float
    legs: int
    sizing: str
    hold_h: int


@dataclass(frozen=True, slots=True)
class Strategy:
    slug: str
    title: str
    tiers: tuple[Tier, ...]
    funding_min_pct: float = -1.0
    oi_max_pct: float = 50.0
    lr_min: float = 0.45
    lr_max: float = 0.65
    tp_pct: float = 25.0


STRATEGIES: tuple[Strategy, ...] = (
    Strategy(
        "current_main_v4",
        "Current main: tiered PB20/PB25, current live weights",
        (
            Tier(0.0, 25.0, 5, "equal", 720),
            Tier(80.0, 20.0, 2, "tapered", 720),
            Tier(100.0, 20.0, 3, "tapered", 336),
            Tier(250.0, 20.0, 2, "tapered", 720),
        ),
    ),
    Strategy(
        "pb20_equal4_168",
        "Former PB20 baseline: four equal legs, 168h",
        (Tier(0.0, 20.0, 4, "equal", 168),),
        funding_min_pct=-0.5,
    ),
    Strategy(
        "pb25_equal5_720",
        "Former deeper PB25: five equal legs, 720h",
        (Tier(0.0, 25.0, 5, "equal", 720),),
    ),
    Strategy(
        "current_main_funding_zero",
        "Current tiers with strict non-negative previous-24h funding",
        (
            Tier(0.0, 25.0, 5, "equal", 720),
            Tier(80.0, 20.0, 2, "tapered", 720),
            Tier(100.0, 20.0, 3, "tapered", 336),
            Tier(250.0, 20.0, 2, "tapered", 720),
        ),
        funding_min_pct=0.0,
    ),
)


def run_funding_corrected_portfolio(
    *,
    samples_path: Path = DEFAULT_SAMPLES,
    events_path: Path = DEFAULT_EVENTS,
    output_dir: Path = DEFAULT_OUTPUT,
    sleep_sec: float = 0.05,
    reuse_raw: bool = False,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    series_by_symbol = load_series(samples_path)
    events = [row for row in read_csv(events_path) if to_int(row.get("trigger_ts")) >= START_TS_MS]
    collector = BybitPumpShortCollector(
        BybitCollectorConfig(output_dir=output_dir / "_collector_unused", sleep_sec=sleep_sec, lookback_days=1)
    )

    funding_path = output_dir / "funding_points.jsonl"
    if reuse_raw and funding_path.exists():
        funding_by_symbol = load_funding_points(funding_path)
    else:
        funding_by_symbol: dict[str, list[tuple[int, float]]] = {}
        for symbol, intervals in build_event_intervals(events, series_by_symbol).items():
            rows: list[dict[str, Any]] = []
            for start_ms, end_ms in merge_intervals(intervals):
                rows.extend(fetch_complete_funding(collector, symbol, start_ms=start_ms, end_ms=end_ms))
            funding_by_symbol[symbol] = dedupe_points(rows, "funding_rate")
        write_jsonl(
            funding_path,
            (
                {"symbol": symbol, "ts_ms": ts_ms, "funding_rate": rate}
                for symbol, points in sorted(funding_by_symbol.items())
                for ts_ms, rate in points
            ),
        )
    for symbol, points in funding_by_symbol.items():
        if symbol in series_by_symbol:
            series_by_symbol[symbol].funding = points

    candidates: list[dict[str, Any]] = []
    funding_audit: list[dict[str, Any]] = []
    for strategy in STRATEGIES:
        seen: set[tuple[str, int]] = set()
        for event in events:
            symbol = str(event.get("symbol") or "")
            series = series_by_symbol.get(symbol)
            if series is None:
                continue
            tier = select_tier(strategy, to_float(event.get("pump_pct")))
            entry = find_fail_closed_entry(series, event, strategy=strategy, tier=tier)
            funding_audit.append(
                {
                    "strategy": strategy.slug,
                    "symbol": symbol,
                    "event_uid": event.get("event_uid"),
                    "trigger_ts": event.get("trigger_ts"),
                    "trigger_iso": event.get("trigger_iso"),
                    **entry,
                }
            )
            entry_ts = to_int(entry.get("entry_ts"))
            if not entry.get("ready") or not entry_ts or (symbol, entry_ts) in seen:
                continue
            seen.add((symbol, entry_ts))
            trade = simulate_trade(series, event, strategy=strategy, tier=tier, entry_idx=series.ts.index(entry_ts))
            if trade:
                candidates.append({**trade, **{k: entry.get(k) for k in ("funding_prev_24h_pct", "oi_change_24h_pct", "long_ratio", "pullback_pct")}})

    husdt_event = next((row for row in events if row.get("event_uid") == HUSDT_EVENT_UID), None)
    husdt_original_trade: dict[str, Any] | None = None
    if husdt_event and "HUSDT" in series_by_symbol:
        strategy = STRATEGIES[0]
        tier = select_tier(strategy, to_float(husdt_event.get("pump_pct")))
        original_entry = find_fail_closed_entry(
            series_by_symbol["HUSDT"], husdt_event, strategy=strategy, tier=tier, require_funding=False
        )
        if original_entry.get("ready"):
            husdt_original_trade = simulate_trade(
                series_by_symbol["HUSDT"],
                husdt_event,
                strategy=strategy,
                tier=tier,
                entry_idx=series_by_symbol["HUSDT"].ts.index(to_int(original_entry["entry_ts"])),
            )

    mark_inputs = list(candidates)
    if husdt_original_trade:
        mark_inputs.append(husdt_original_trade)
    mark_path = output_dir / "mark_points.jsonl"
    if reuse_raw and mark_path.exists():
        mark_by_symbol = load_mark_points(mark_path)
    else:
        mark_by_symbol: dict[str, dict[int, dict[str, float]]] = {}
        for symbol, intervals in trade_intervals(mark_inputs).items():
            points: list[dict[str, Any]] = []
            for start_ms, end_ms in merge_intervals(intervals):
                points.extend(
                    collector.fetch_price_klines(
                        "/v5/market/mark-price-kline",
                        symbol,
                        interval="60",
                        start_ms=start_ms,
                        end_ms=end_ms,
                        limit=1000,
                    )
                )
            mark_by_symbol[symbol] = {to_int(row.get("ts_ms")): row for row in points if to_int(row.get("ts_ms"))}
        write_jsonl(
            mark_path,
            (
                {"symbol": symbol, **point}
                for symbol, points in sorted(mark_by_symbol.items())
                for _, point in sorted(points.items())
            ),
        )

    enriched = [enrich_trade(row, series_by_symbol[row["symbol"]], mark_by_symbol.get(row["symbol"], {})) for row in candidates]
    summaries: list[dict[str, Any]] = []
    portfolio_rows: list[dict[str, Any]] = []
    for strategy in STRATEGIES:
        result = simulate_portfolio([row for row in enriched if row["strategy"] == strategy.slug])
        summaries.append(result["summary"])
        portfolio_rows.extend(result["trades"])

    original_enriched = (
        enrich_trade(husdt_original_trade, series_by_symbol["HUSDT"], mark_by_symbol.get("HUSDT", {}))
        if husdt_original_trade
        else None
    )
    husdt = build_husdt_report(
        funding_audit,
        enriched,
        series_by_symbol.get("HUSDT"),
        original_enriched,
    )
    write_csv(output_dir / "strategy_comparison.csv", summaries)
    write_csv(output_dir / "portfolio_trades.csv", portfolio_rows)
    write_csv(output_dir / "funding_entry_audit.csv", funding_audit)
    write_csv(output_dir / "husdt_timeline.csv", husdt)
    (output_dir / "report.md").write_text(render_markdown(summaries, husdt), encoding="utf-8")
    (output_dir / "index.html").write_text(render_html(summaries, husdt), encoding="utf-8")
    metadata = {
        "schema": "pump_short_funding_corrected_portfolio_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "events": len(events),
        "symbols": len(series_by_symbol),
        "strategies": len(STRATEGIES),
        "trade_candidates": len(enriched),
        "funding_points": sum(len(points) for points in funding_by_symbol.values()),
        "mark_points": sum(len(points) for points in mark_by_symbol.values()),
        "raw_source": "saved_raw" if reuse_raw else "fresh_bybit_api",
        "funding_fail_closed": True,
        "capital_usd": 3000.0,
        "slots": 4,
        "slot_margin_usd": 600.0,
        "rescue_cap_usd": 2000.0,
        "rescue_excluded_from_return": True,
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def load_series(path: Path) -> dict[str, Series]:
    out: dict[str, Series] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            sample = json.loads(line)
            series = sample_to_series(sample)
            out[series.symbol] = series
    return out


def build_event_intervals(events: Iterable[Mapping[str, Any]], series: Mapping[str, Series]) -> dict[str, list[tuple[int, int]]]:
    out: dict[str, list[tuple[int, int]]] = {}
    for row in events:
        symbol = str(row.get("symbol") or "")
        trigger = to_int(row.get("trigger_ts"))
        if symbol not in series or not trigger:
            continue
        out.setdefault(symbol, []).append((trigger - 24 * HOUR_MS, trigger + 168 * HOUR_MS))
    return out


def fetch_complete_funding(
    collector: BybitPumpShortCollector,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    """Fetch bounded pages safe even when funding settles every hour."""
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    max_span = 167 * HOUR_MS
    while cursor <= end_ms:
        page_end = min(end_ms, cursor + max_span)
        rows.extend(collector.fetch_funding_history(symbol, start_ms=cursor, end_ms=page_end, limit=200))
        cursor = page_end + HOUR_MS
    return rows


def find_fail_closed_entry(
    series: Series,
    event: Mapping[str, Any],
    *,
    strategy: Strategy,
    tier: Tier,
    require_funding: bool = True,
) -> dict[str, Any]:
    trigger = to_int(event.get("trigger_ts"))
    if trigger not in series.ts:
        return {"ready": False, "reason": "trigger_missing"}
    trigger_idx = series.ts.index(trigger)
    high_water = series.high[trigger_idx] or series.close[trigger_idx]
    reasons: dict[str, int] = {}
    for idx in range(trigger_idx + 1, min(len(series.ts), trigger_idx + 169)):
        current_high = series.high[idx]
        if current_high is not None and (high_water is None or current_high > high_water):
            high_water = current_high
        close = series.close[idx]
        pullback = (1.0 - close / high_water) * 100.0 if high_water and close else None
        if pullback is None or pullback + 1e-9 < tier.pullback_pct:
            reasons["pullback"] = reasons.get("pullback", 0) + 1
            continue
        oi = point_change_pct(series.oi, series.ts, idx, 24)
        if oi is None or oi > strategy.oi_max_pct:
            reasons["oi"] = reasons.get("oi", 0) + 1
            continue
        lr = series.long_ratio.get(series.ts[idx])
        if lr is None or not (strategy.lr_min <= lr <= strategy.lr_max):
            reasons["ratio"] = reasons.get("ratio", 0) + 1
            continue
        funding = funding_at(series, series.ts[idx])
        if require_funding:
            if funding is None:
                reasons["funding_missing"] = reasons.get("funding_missing", 0) + 1
                continue
            if funding <= strategy.funding_min_pct:
                reasons["funding"] = reasons.get("funding", 0) + 1
                continue
        return {
            "ready": True,
            "reason": "ready",
            "entry_ts": series.ts[idx],
            "entry_iso": ms_to_iso(series.ts[idx]),
            "entry_price": close,
            "pullback_pct": round(pullback, 6),
            "oi_change_24h_pct": round(oi, 6),
            "long_ratio": round(lr, 6),
            "funding_prev_24h_pct": round(funding, 6) if funding is not None else None,
        }
    return {"ready": False, "reason": max(reasons, key=reasons.get) if reasons else "no_window", "entry_ts": None}


def simulate_trade(
    series: Series,
    event: Mapping[str, Any],
    *,
    strategy: Strategy,
    tier: Tier,
    entry_idx: int,
) -> dict[str, Any] | None:
    first = series.close[entry_idx]
    if not first:
        return None
    weights = [float(i + 1) for i in range(tier.legs)] if tier.sizing == "tapered" else [1.0] * tier.legs
    margins = [600.0 * weight / sum(weights) for weight in weights]
    prices = [first * (1.0 + 0.5 * idx) for idx in range(tier.legs)]
    notionals = [margin * 3.0 for margin in margins]
    planned_legs = [
        {"step": idx + 1, "trigger_price": prices[idx], "margin_usd": margins[idx], "notional_usd": notionals[idx]}
        for idx in range(tier.legs)
    ]
    fills = [{"step": 1, "ts_ms": series.ts[entry_idx], "price": prices[0], "margin": margins[0], "notional": notionals[0]}]
    active = 1
    exit_limit = min(len(series.ts) - 1, entry_idx + tier.hold_h)
    exit_idx = exit_limit
    exit_price = series.close[exit_limit]
    exit_reason = "time_stop"
    for idx in range(entry_idx + 1, exit_limit + 1):
        high = series.high[idx]
        while active < tier.legs and idx - entry_idx <= 168 and high is not None and high >= prices[active]:
            fills.append({"step": active + 1, "ts_ms": series.ts[idx], "price": prices[active], "margin": margins[active], "notional": notionals[active]})
            active += 1
        qty = sum(fill["notional"] / fill["price"] for fill in fills)
        entry_notional = sum(fill["notional"] for fill in fills)
        avg = entry_notional / qty
        target = avg * (1.0 - strategy.tp_pct / 100.0)
        if series.low[idx] is not None and series.low[idx] <= target:
            exit_idx = idx
            exit_price = target
            exit_reason = f"target_{int(strategy.tp_pct)}"
            break
    if exit_price is None:
        return None
    return {
        "strategy": strategy.slug,
        "symbol": series.symbol,
        "event_uid": event.get("event_uid"),
        "pump_pct": round(to_float(event.get("pump_pct")), 6),
        "entry_ts": series.ts[entry_idx],
        "entry_iso": ms_to_iso(series.ts[entry_idx]),
        "exit_ts": series.ts[exit_idx],
        "exit_iso": ms_to_iso(series.ts[exit_idx]),
        "exit_reason": exit_reason,
        "time_in_trade_h": exit_idx - entry_idx,
        "tier_pullback_pct": tier.pullback_pct,
        "tier_legs": tier.legs,
        "tier_sizing": tier.sizing,
        "planned_legs": planned_legs,
        "fills": fills,
        "exit_price": exit_price,
    }


def enrich_trade(row: dict[str, Any], series: Series, marks: Mapping[int, Mapping[str, Any]]) -> dict[str, Any]:
    fills = list(row["fills"])
    qty = sum(fill["notional"] / fill["price"] for fill in fills)
    entry_notional = sum(fill["notional"] for fill in fills)
    avg = entry_notional / qty
    price_pnl = qty * (avg - float(row["exit_price"]))
    funding_pnl = 0.0
    for ts_ms, rate in series.funding:
        if not (to_int(row["entry_ts"]) < ts_ms <= to_int(row["exit_ts"])):
            continue
        active_qty = sum(fill["notional"] / fill["price"] for fill in fills if to_int(fill["ts_ms"]) < ts_ms)
        mark = to_float((marks.get(ts_ms) or {}).get("close")) or close_at(series, ts_ms)
        if active_qty > 0 and mark:
            funding_pnl += active_qty * mark * rate
    fee = entry_notional * 0.0018
    margin = simulate_margin_profile(row, series, marks)
    net = price_pnl + funding_pnl - fee
    return {
        **row,
        "planned_legs_json": json.dumps(row["planned_legs"], separators=(",", ":")),
        "fills_json": json.dumps(fills, separators=(",", ":")),
        "fills": fills,
        "legs_filled": len(fills),
        "entry_notional_usd": round(entry_notional, 6),
        "avg_entry_price": round(avg, 12),
        "price_pnl_usd": round(price_pnl, 6),
        "funding_pnl_usd": round(funding_pnl, 6),
        "fees_usd": round(fee, 6),
        "net_pnl_usd": round(net, 6),
        **margin,
    }


def simulate_margin_profile(row: Mapping[str, Any], series: Series, marks: Mapping[int, Mapping[str, Any]]) -> dict[str, Any]:
    fills = list(row["fills"])
    planned = list(row["planned_legs"])
    first = fills[0]
    qty = first["notional"] / first["price"]
    liq = first["notional"] * (1.0 + 1.0 / 3.0) / (qty * 1.025)
    topup = 0.0
    next_index = 1
    if next_index < len(planned):
        plan = prefund(qty, liq, planned, planned[next_index])
        add = to_float(plan.get("required_add_usd"))
        topup += add
        liq += add / (qty * 1.025)
    initial_prefund = topup
    peak_topup = topup
    peak_cash = first["margin"] + (planned[next_index]["margin_usd"] if next_index < len(planned) else 0.0) + topup
    peak_mark = 0.0
    peak_mark_ts = 0
    cash_profile: list[tuple[int, float]] = []
    cumulative_funding = 0.0
    funding = {ts: rate for ts, rate in series.funding}
    for ts_ms in series.ts:
        if ts_ms < to_int(row["entry_ts"]) or ts_ms > to_int(row["exit_ts"]):
            continue
        while next_index < len(fills) and to_int(fills[next_index]["ts_ms"]) <= ts_ms:
            fill = fills[next_index]
            qty, liq = projected_short_liquidation_after_fill(
                qty=qty,
                current_liq_price=liq,
                added_notional_usd=fill["notional"],
                added_price=fill["price"],
                leverage=3.0,
                maintenance_margin_rate=0.025,
            )
            next_index += 1
            if next_index < len(planned):
                plan = prefund(qty, liq, planned, planned[next_index])
                add = to_float(plan.get("required_add_usd"))
                topup += add
                liq += add / (qty * 1.025)
        mark_row = marks.get(ts_ms) or {}
        mark_high = to_float(mark_row.get("high")) or high_at(series, ts_ms)
        if mark_high > peak_mark:
            peak_mark = mark_high
            peak_mark_ts = ts_ms
        if mark_high and (liq / mark_high - 1.0) * 100.0 <= 20.0:
            add = required_margin_for_liq_buffer_usd(
                qty=qty,
                current_liq_price=liq,
                mark_price=mark_high,
                target_buffer_pct=25.0,
                maintenance_margin_rate=0.025,
                taker_fee_rate=0.00055,
                round_up_increment_usd=5.0,
            )
            topup += add
            liq += add / (qty * 1.025)
        rate = funding.get(ts_ms)
        if rate is not None:
            active_qty = sum(fill["notional"] / fill["price"] for fill in fills if to_int(fill["ts_ms"]) < ts_ms)
            mark_close = to_float(mark_row.get("close")) or close_at(series, ts_ms)
            if active_qty > 0 and mark_close:
                cumulative_funding += active_qty * mark_close * rate
        filled_margin = sum(fill["margin"] for fill in fills if to_int(fill["ts_ms"]) <= ts_ms)
        add_window_open = ts_ms - to_int(row["entry_ts"]) <= 168 * HOUR_MS
        next_order_margin = planned[next_index]["margin_usd"] if add_window_open and next_index < len(planned) else 0.0
        cash_need = filled_margin + next_order_margin + topup + max(0.0, -cumulative_funding)
        cash_profile.append((ts_ms, cash_need))
        peak_topup = max(peak_topup, topup)
        peak_cash = max(peak_cash, cash_need)
    initial_action = first["margin"] + (planned[1]["margin_usd"] if len(planned) > 1 else 0.0) + initial_prefund
    return {
        "initial_prefund_usd": round(initial_prefund, 6),
        "peak_margin_topup_usd": round(peak_topup, 6),
        "peak_cash_required_usd": round(peak_cash, 6),
        "initial_action_cash_usd": round(initial_action, 6),
        "peak_mark_price": round(peak_mark, 12),
        "peak_mark_ts": peak_mark_ts,
        "peak_mark_iso": ms_to_iso(peak_mark_ts) if peak_mark_ts else None,
        "cash_profile": cash_profile,
        "cash_profile_json": json.dumps(cash_profile, separators=(",", ":")),
    }


def prefund(qty: float, liq: float, legs: list[dict[str, Any]], target: dict[str, Any]) -> dict[str, Any]:
    return ladder_prefund_plan(
        policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
        qty=qty,
        current_liq_price=liq,
        legs=legs,
        target_leg=target,
        leverage=3.0,
        stop_gap_from_liq_pct=2.5,
        safety_above_next_ladder_pct=2.5,
        final_fill_buffer_pct=20.0,
        maintenance_margin_rate=0.025,
        taker_fee_rate=0.00055,
        round_up_increment_usd=5.0,
        projected_reaction_buffer_pct=8.0,
    )


def simulate_portfolio(rows: list[dict[str, Any]]) -> dict[str, Any]:
    capital = 3000.0
    active: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    skipped_slots = skipped_same = skipped_admission = 0
    max_rescue = 0.0
    breaches = 0
    for row in sorted(rows, key=lambda item: (to_int(item["entry_ts"]), item["symbol"])):
        entry_ts = to_int(row["entry_ts"])
        due = [item for item in active if to_int(item["exit_ts"]) <= entry_ts]
        for item in due:
            capital += to_float(item["net_pnl_usd"])
        active = [item for item in active if to_int(item["exit_ts"]) > entry_ts]
        if any(item["symbol"] == row["symbol"] for item in active):
            skipped_same += 1
            continue
        if len(active) >= 4:
            skipped_slots += 1
            continue
        current_cash = sum(cash_at(item, entry_ts) for item in active)
        action = to_float(row["initial_action_cash_usd"])
        minimum_free = capital * 0.30 + 75.0
        if capital <= 0 or capital - current_cash - action < minimum_free:
            skipped_admission += 1
            continue
        active.append(row)
        selected.append(row)
        rescue = synchronized_rescue_required(active, capital, start_ts=entry_ts)
        max_rescue = max(max_rescue, rescue)
        if rescue > 2000.0:
            breaches += 1
            row["portfolio_capacity_status"] = "derisk_required"
        else:
            row["portfolio_capacity_status"] = "holdable"
        row["portfolio_rescue_required_usd"] = round(rescue, 6)
    for item in sorted(active, key=lambda row: to_int(row["exit_ts"])):
        capital += to_float(item["net_pnl_usd"])
    equity = realized_equity_stats(selected)
    summary = {
        "strategy": rows[0]["strategy"] if rows else "",
        "candidate_trades": len(rows),
        "selected_trades": len(selected),
        "wins": sum(to_float(row["net_pnl_usd"]) > 0 for row in selected),
        "losses": sum(to_float(row["net_pnl_usd"]) < 0 for row in selected),
        "win_rate_pct": round(100.0 * sum(to_float(row["net_pnl_usd"]) > 0 for row in selected) / len(selected), 6) if selected else 0.0,
        "total_net_pnl_usd": round(sum(to_float(row["net_pnl_usd"]) for row in selected), 6),
        "total_funding_pnl_usd": round(sum(to_float(row["funding_pnl_usd"]) for row in selected), 6),
        "final_capital_usd": round(capital, 6),
        "roi_on_3000_pct": round((capital / 3000.0 - 1.0) * 100.0, 6),
        "max_rescue_required_usd": round(max_rescue, 6),
        "capacity_breaches": breaches,
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same,
        "skipped_admission": skipped_admission,
        "median_trade_net_usd": round(statistics.median([to_float(row["net_pnl_usd"]) for row in selected]), 6) if selected else 0.0,
        "worst_trade_net_usd": round(min((to_float(row["net_pnl_usd"]) for row in selected), default=0.0), 6),
        **equity,
    }
    return {
        "summary": summary,
        "trades": [
            {k: v for k, v in row.items() if k not in {"fills", "planned_legs", "cash_profile"}}
            for row in selected
        ],
    }


def cash_at(row: Mapping[str, Any], ts_ms: int) -> float:
    profile = list(row.get("cash_profile") or [])
    values = [to_float(value) for stamp, value in profile if to_int(stamp) <= ts_ms]
    return values[-1] if values else to_float(row.get("initial_action_cash_usd"))


def realized_equity_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    capital = peak = 3000.0
    max_drawdown_usd = max_drawdown_pct = 0.0
    exits: dict[int, float] = {}
    sweep: list[tuple[int, int]] = []
    for row in rows:
        exits[to_int(row["exit_ts"])] = exits.get(to_int(row["exit_ts"]), 0.0) + to_float(row["net_pnl_usd"])
        sweep.append((to_int(row["entry_ts"]), 1))
        sweep.append((to_int(row["exit_ts"]), -1))
    for _, pnl in sorted(exits.items()):
        capital += pnl
        peak = max(peak, capital)
        drawdown = peak - capital
        max_drawdown_usd = max(max_drawdown_usd, drawdown)
        if peak > 0:
            max_drawdown_pct = max(max_drawdown_pct, drawdown / peak * 100.0)
    concurrent = maximum = 0
    for _, delta in sorted(sweep, key=lambda item: (item[0], item[1])):
        concurrent += delta
        maximum = max(maximum, concurrent)
    return {
        "max_realized_drawdown_usd": round(max_drawdown_usd, 6),
        "max_realized_drawdown_pct": round(max_drawdown_pct, 6),
        "max_concurrent_positions": maximum,
    }


def synchronized_rescue_required(rows: list[dict[str, Any]], capital: float, *, start_ts: int) -> float:
    timeline = {start_ts}
    for row in rows:
        timeline.add(to_int(row["exit_ts"]))
        timeline.update(to_int(stamp) for stamp, _ in row.get("cash_profile") or [] if to_int(stamp) >= start_ts)
    peak = 0.0
    for ts_ms in sorted(timeline):
        future_capital = capital + sum(
            to_float(row["net_pnl_usd"])
            for row in rows
            if start_ts < to_int(row["exit_ts"]) <= ts_ms
        )
        needed = sum(
            cash_at(row, ts_ms)
            for row in rows
            if to_int(row["entry_ts"]) <= ts_ms < to_int(row["exit_ts"])
        )
        peak = max(peak, needed + 75.0 - future_capital)
    return max(0.0, peak)


def build_husdt_report(
    audit: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    series: Series | None,
    original: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if series is None:
        return []
    corrected_entry = next(
        (
            row
            for row in audit
            if row.get("event_uid") == HUSDT_EVENT_UID and row.get("strategy") == "current_main_v4"
        ),
        None,
    )
    corrected = next(
        (
            row
            for row in trades
            if row.get("event_uid") == HUSDT_EVENT_UID and row.get("strategy") == "current_main_v4"
        ),
        None,
    )
    if corrected is None and corrected_entry and corrected_entry.get("entry_ts"):
        # Multiple overlapping detector windows can identify the same market
        # episode and are deliberately deduplicated before portfolio replay.
        corrected = next(
            (
                row
                for row in trades
                if row.get("symbol") == "HUSDT"
                and row.get("strategy") == "current_main_v4"
                and to_int(row.get("entry_ts")) == to_int(corrected_entry.get("entry_ts"))
            ),
            None,
        )
    out: list[dict[str, Any]] = [
        {
            "kind": "trigger",
            "ts_ms": 1781046000000,
            "ts_iso": ms_to_iso(1781046000000),
            "pump_pct": 156.059263,
        }
    ]
    if original:
        out.append(
            {
                "kind": "old_missing_funding_entry_invalid",
                "ts_ms": original["entry_ts"],
                "ts_iso": original["entry_iso"],
                "entry_price": original["fills"][0]["price"],
                "funding_prev_24h_pct": funding_at(series, to_int(original["entry_ts"])),
                "exit_iso": original["exit_iso"],
                "funding_pnl_usd": original["funding_pnl_usd"],
                "net_pnl_usd": original["net_pnl_usd"],
                "peak_cash_required_usd": original["peak_cash_required_usd"],
                "peak_margin_topup_usd": original["peak_margin_topup_usd"],
            }
        )
    first_calm = first_funding_pass(series, start_ts=1781046000000, minimum_pct=-1.0)
    if first_calm:
        out.append({"kind": "funding_first_above_minus_1", **first_calm})
    if corrected_entry:
        out.append({"kind": "corrected_all_gates_entry", **corrected_entry})
    if corrected:
        for fill in corrected["fills"]:
            out.append(
                {
                    "kind": f"fill_l{fill['step']}",
                    "ts_ms": fill["ts_ms"],
                    "ts_iso": ms_to_iso(fill["ts_ms"]),
                    "price": fill["price"],
                    "margin_usd": fill["margin"],
                    "notional_usd": fill["notional"],
                }
            )
        out.append(
            {
                "kind": "peak_mark",
                "ts_ms": corrected["peak_mark_ts"],
                "ts_iso": corrected["peak_mark_iso"],
                "price": corrected["peak_mark_price"],
                "peak_margin_topup_usd": corrected["peak_margin_topup_usd"],
                "peak_cash_required_usd": corrected["peak_cash_required_usd"],
            }
        )
        out.append(
            {
                "kind": "corrected_exit",
                "ts_ms": corrected["exit_ts"],
                "ts_iso": corrected["exit_iso"],
                "exit_reason": corrected["exit_reason"],
                "exit_price": corrected["exit_price"],
                "time_in_trade_h": corrected["time_in_trade_h"],
                "price_pnl_usd": corrected["price_pnl_usd"],
                "funding_pnl_usd": corrected["funding_pnl_usd"],
                "fees_usd": corrected["fees_usd"],
                "net_pnl_usd": corrected["net_pnl_usd"],
            }
        )
    return out


def funding_at(series: Series, end_ts: int) -> float | None:
    rows = [(ts, rate) for ts, rate in series.funding if end_ts - 24 * HOUR_MS < ts <= end_ts]
    if not funding_window_complete(series.funding, rows, end_ts=end_ts):
        return None
    return sum(rate for _, rate in rows) * 100.0


def first_funding_pass(series: Series, *, start_ts: int, minimum_pct: float) -> dict[str, Any] | None:
    for ts_ms in series.ts:
        if ts_ms <= start_ts:
            continue
        funding = funding_at(series, ts_ms)
        if funding is not None and funding > minimum_pct:
            return {
                "ts_ms": ts_ms,
                "ts_iso": ms_to_iso(ts_ms),
                "funding_prev_24h_pct": round(funding, 6),
                "price": close_at(series, ts_ms),
            }
    return None


def select_tier(strategy: Strategy, pump_pct: float) -> Tier:
    return max((tier for tier in strategy.tiers if pump_pct >= tier.min_pump_pct), key=lambda tier: tier.min_pump_pct)


def merge_intervals(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    out: list[list[int]] = []
    for start, end in sorted(intervals):
        if not out or start > out[-1][1] + HOUR_MS:
            out.append([start, end])
        else:
            out[-1][1] = max(out[-1][1], end)
    return [(start, end) for start, end in out]


def dedupe_points(rows: Iterable[Mapping[str, Any]], field: str) -> list[tuple[int, float]]:
    points = {to_int(row.get("ts_ms")): to_float(row.get(field)) for row in rows if to_int(row.get("ts_ms"))}
    return sorted(points.items())


def funding_window_complete(
    all_rows: list[tuple[int, float]],
    window_rows: list[tuple[int, float]],
    *,
    end_ts: int,
) -> bool:
    """Require a complete interval-aware 24h settlement window.

    Bybit instruments may settle every 1h, 4h, or 8h. Missing rows therefore
    cannot be detected using a fixed count of three or twenty-four.
    """
    if not window_rows:
        return False
    nearby = [ts for ts, _ in all_rows if end_ts - 72 * HOUR_MS < ts <= end_ts]
    deltas = [right - left for left, right in zip(nearby, nearby[1:]) if right > left]
    if not deltas:
        return False
    interval = int(statistics.median(deltas))
    if interval < HOUR_MS or interval > 8 * HOUR_MS:
        return False
    stamps = [ts for ts, _ in window_rows]
    expected = max(1, int((24 * HOUR_MS) // interval))
    return (
        len(stamps) >= expected
        and stamps[0] <= end_ts - 24 * HOUR_MS + interval
        and stamps[-1] >= end_ts - interval
        and all(right - left <= interval * 1.5 for left, right in zip(stamps, stamps[1:]))
    )


def trade_intervals(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[tuple[int, int]]]:
    out: dict[str, list[tuple[int, int]]] = {}
    for row in rows:
        out.setdefault(str(row["symbol"]), []).append((to_int(row["entry_ts"]), to_int(row["exit_ts"])))
    return out


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields and key not in {"fills", "planned_legs", "cash_profile"}:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), separators=(",", ":"), sort_keys=True) + "\n")


def load_funding_points(path: Path) -> dict[str, list[tuple[int, float]]]:
    out: dict[str, list[tuple[int, float]]] = {}
    for row in read_jsonl(path):
        out.setdefault(str(row["symbol"]), []).append((to_int(row["ts_ms"]), to_float(row["funding_rate"])))
    return {symbol: sorted(points) for symbol, points in out.items()}


def load_mark_points(path: Path) -> dict[str, dict[int, dict[str, float]]]:
    out: dict[str, dict[int, dict[str, float]]] = {}
    for row in read_jsonl(path):
        symbol = str(row.pop("symbol"))
        out.setdefault(symbol, {})[to_int(row["ts_ms"])] = row
    return out


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def render_markdown(summary: list[dict[str, Any]], husdt: list[dict[str, Any]]) -> str:
    by_kind = {str(row.get("kind")): row for row in husdt}
    invalid = by_kind.get("old_missing_funding_entry_invalid", {})
    calm = by_kind.get("funding_first_above_minus_1", {})
    entry = by_kind.get("corrected_all_gates_entry", {})
    peak = by_kind.get("peak_mark", {})
    exit_row = by_kind.get("corrected_exit", {})
    lines = [
        "# Pump Short: funding-corrected historical portfolio",
        "",
        "Research-only; live settings, orders and ARM state were not changed.",
        "",
        "## Model contract",
        "",
        "- Every entry requires a complete interval-aware previous-24h funding window. Missing data fails closed.",
        "- Funding cash flow uses every actual Bybit settlement and the corresponding hourly mark price.",
        "- Current portfolio: $3000 Pump-owned capital, four isolated slots, fixed $600 full ladder, 3x leverage.",
        "- Admission leaves 30% Pump-owned free cash plus the $75 hard floor. Main money cannot admit an entry.",
        "- Existing positions may use at most $2000 temporary main rescue; that principal is excluded from return.",
        "- Current v4 prefund/top-up formulas target the next ladder and restore a 25% liquidation buffer.",
        "- Fees use a conservative 0.18% of filled entry notional. Slot size does not compound automatically.",
        "",
        "## Strategy comparison",
        "",
        "| Strategy | Candidates | Trades | W/L | Win rate | Funding | Net PnL | ROI | Max DD | Max rescue |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary:
        lines.append(
            f"| {row['strategy']} | {row['candidate_trades']} | {row['selected_trades']} | "
            f"{row['wins']}/{row['losses']} | {row['win_rate_pct']:.2f}% | ${row['total_funding_pnl_usd']:.2f} | "
            f"${row['total_net_pnl_usd']:.2f} | {row['roi_on_3000_pct']:.2f}% | "
            f"{row['max_realized_drawdown_pct']:.2f}% | ${row['max_rescue_required_usd']:.2f} |"
        )
    lines.extend(
        [
            "",
            "`current_main_v4` has the highest aggregate net result. The deeper PB25 variant gives up 13.5% of that "
            "net result, but has the highest win rate, a much smaller 5.58% realized drawdown, and a far smaller worst "
            "trade; it is the strongest safer shadow candidate. Requiring non-negative funding removes profitable "
            "opportunities without improving the observed loss count enough to compensate.",
            "",
            "## HUSDT event 2026-06-09",
            "",
            f"The historical missing-data path would have entered at {invalid.get('ts_iso')} near "
            f"{to_float(invalid.get('entry_price')):.6f}. The restored previous-24h funding was "
            f"{to_float(invalid.get('funding_prev_24h_pct')):.6f}%, so that entry is invalid.",
            "",
            f"If forced anyway, the current $600 ladder fills all three legs and exits at "
            f"{invalid.get('exit_iso')}; price PnL is positive, but funding is "
            f"${to_float(invalid.get('funding_pnl_usd')):.2f}, net is ${to_float(invalid.get('net_pnl_usd')):.2f}, "
            f"and peak Pump cash need reaches ${to_float(invalid.get('peak_cash_required_usd')):.2f}. This exceeds "
            "the $3000 portfolio plus the $2000 rescue facility and would require derisk/close rather than passive holding.",
            "",
            f"Funding first rises above -1% on {calm.get('ts_iso')} at "
            f"{to_float(calm.get('funding_prev_24h_pct')):.6f}%, but the complete gate set is first valid only on "
            f"{entry.get('entry_iso')}. At that point: price {to_float(entry.get('entry_price')):.6f}, "
            f"pullback {to_float(entry.get('pullback_pct')):.2f}%, OI24 {to_float(entry.get('oi_change_24h_pct')):.2f}%, "
            f"long ratio {to_float(entry.get('long_ratio')):.4f}, funding24 {to_float(entry.get('funding_prev_24h_pct')):.6f}%.",
            "",
            "The corrected current ladder is $100/$200/$300 margin ($300/$600/$900 notional) at "
            "0.230190 / 0.345285 / 0.460380. Peak mark is "
            f"{to_float(peak.get('price')):.6f} at {peak.get('ts_iso')}; v4 needs "
            f"${to_float(peak.get('peak_margin_topup_usd')):.2f} cumulative isolated top-up and "
            f"${to_float(peak.get('peak_cash_required_usd')):.2f} total Pump cash. As a sole $3000 position it "
            "does not need main rescue.",
            "",
            f"TP25 is reached on {exit_row.get('ts_iso')} after {to_int(exit_row.get('time_in_trade_h'))}h. "
            f"Price PnL ${to_float(exit_row.get('price_pnl_usd')):.2f}, funding "
            f"${to_float(exit_row.get('funding_pnl_usd')):.2f}, fees ${to_float(exit_row.get('fees_usd')):.2f}, "
            f"net ${to_float(exit_row.get('net_pnl_usd')):.2f}.",
            "",
            "## Limitations",
            "",
            "- The universe is the archived current-listing sample (56 symbols), so delisted-symbol survivor bias remains.",
            "- Entry/ladder/TP ordering is reconstructed from hourly candles. A candle touching multiple levels is not tick-exact.",
            "- Mark-price and settlement funding are exact public Bybit history; partial fills, contract rounding and live slippage are not replayed.",
            "- Capacity is synchronized across concurrent cash profiles, but automatic donor cuts are not assigned hypothetical extra PnL.",
            "",
            "Evidence: `funding_points.jsonl`, `mark_points.jsonl`, `funding_entry_audit.csv`, "
            "`portfolio_trades.csv`, `strategy_comparison.csv`, and `husdt_timeline.csv`.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_html(summary: list[dict[str, Any]], husdt: list[dict[str, Any]]) -> str:
    rows = "".join(f"<tr><td>{r['strategy']}</td><td>{r['selected_trades']}</td><td>{r['win_rate_pct']:.2f}%</td><td>${r['total_net_pnl_usd']:.2f}</td><td>{r['roi_on_3000_pct']:.2f}%</td><td>${r['max_rescue_required_usd']:.2f}</td><td>{r['capacity_breaches']}</td></tr>" for r in summary)
    return f"<!doctype html><meta charset='utf-8'><title>Funding-corrected Pump Short</title><style>body{{font:15px system-ui;background:#0d1524;color:#e8eef9;max-width:1200px;margin:30px auto}}table{{border-collapse:collapse;width:100%}}th,td{{padding:9px;border:1px solid #34425b}}th{{background:#1b2940}}code{{color:#8be9fd}}</style><h1>Funding-corrected Pump Short portfolio</h1><p>Research-only. $3000 Pump capital, four slots, $600 per new symbol, temporary main rescue capped at $2000 and excluded from return.</p><table><tr><th>Strategy</th><th>Trades</th><th>Win rate</th><th>Net PnL</th><th>ROI</th><th>Max rescue</th><th>Breaches</th></tr>{rows}</table><h2>HUSDT</h2><pre>{json.dumps(husdt, ensure_ascii=False, indent=2)}</pre>"


def close_at(series: Series, ts_ms: int) -> float:
    return to_float(series.close[series.ts.index(ts_ms)]) if ts_ms in series.ts else 0.0


def high_at(series: Series, ts_ms: int) -> float:
    return to_float(series.high[series.ts.index(ts_ms)]) if ts_ms in series.ts else 0.0


def to_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def to_float(value: Any) -> float:
    try:
        number = float(value or 0.0)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).isoformat()


__all__ = [
    "STRATEGIES",
    "Strategy",
    "Tier",
    "fetch_complete_funding",
    "find_fail_closed_entry",
    "run_funding_corrected_portfolio",
    "simulate_trade",
]
