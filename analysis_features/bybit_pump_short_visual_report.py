from __future__ import annotations

import html
import json
import math
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import BASE_DIR
from analysis_features.bybit_pump_short_outcomes import (
    FEE_ROUNDTRIP_PCT,
    PumpEvent,
    Series,
    detect_pump_events,
    event_behavior_features,
    find_confirmed_pullback_entry,
    funding_sum_pct,
    load_samples,
    sample_to_series,
    to_float,
    write_csv,
)

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_visual_report"

LEG_NOTIONAL_USD = 1000.0
LEVERAGE = 3.0
MAX_LEGS = 4
STEP_PCT = 50.0
TAKE_PROFIT_PCT = 25.0
MAX_HOLD_H = 168
ADD_WINDOW_H = 168
ENTRY_PULLBACK_PCT = 20.0
ENTRY_OI_MAX_PCT = 50.0
LONG_RATIO_MIN = 0.45
LONG_RATIO_MAX = 0.65
ENTRY_FUNDING_MIN_PCT = -0.50


@dataclass
class Leg:
    idx: int
    price: float
    notional: float


def run_visual_report(input_path: Path = DEFAULT_INPUT, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    events_dir = output_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    results: list[dict[str, Any]] = []
    simulations: list[dict[str, Any]] = []
    symbols_seen = 0
    events_seen = 0

    for sample in load_samples(input_path):
        symbols_seen += 1
        series = sample_to_series(sample)
        events = detect_pump_events(series)
        events_seen += len(events)
        for event in events:
            result = simulate_default_strategy(series, event)
            results.append(result)
            if result["status"] == "entered":
                simulations.append(result)

    unique_simulations = annotate_trade_groups(simulations)
    rows = [summary_row(result) for result in results]
    unique_rows = [summary_row(result) for result in unique_simulations]

    for result in simulations:
        event_path = events_dir / f"{result['report_id']}.html"
        event_path.write_text(render_event_page(result), encoding="utf-8")

    write_csv(output_dir / "visual_strategy_simulations.csv", rows)
    write_csv(output_dir / "visual_strategy_entered.csv", [row for row in rows if row.get("status") == "entered"])
    write_csv(output_dir / "visual_strategy_unique_trades.csv", unique_rows)
    write_csv(output_dir / "visual_strategy_skipped.csv", [row for row in rows if row.get("status") != "entered"])
    (output_dir / "index.html").write_text(render_index(rows, simulations, unique_rows), encoding="utf-8")
    metadata = {
        "schema": "bybit_pump_short_visual_report_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": events_seen,
        "entered": len(simulations),
        "unique_trades": len(unique_simulations),
        "skipped": len(rows) - len(simulations),
        "leg_notional_usd": LEG_NOTIONAL_USD,
        "leverage": LEVERAGE,
        "max_legs": MAX_LEGS,
        "step_pct": STEP_PCT,
        "take_profit_pct": TAKE_PROFIT_PCT,
        "max_hold_h": MAX_HOLD_H,
        "entry_funding_min_pct": ENTRY_FUNDING_MIN_PCT,
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "visual_report_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def annotate_trade_groups(simulations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for result in simulations:
        key = (
            result.get("symbol"),
            result.get("entry_ts"),
            round(float(result.get("entry_price") or 0.0), 12),
            result.get("exit_ts"),
            round(float(result.get("exit_price") or 0.0), 12),
            result.get("legs_activated"),
        )
        groups.setdefault(key, []).append(result)

    unique: list[dict[str, Any]] = []
    for idx, (_, items) in enumerate(sorted(groups.items(), key=lambda item: (str(item[0][0]), item[0][1] or 0)), start=1):
        representative = sorted(items, key=lambda item: (int(item.get("trigger_ts") or 0), str(item.get("event_id") or "")))[0]
        trade_group_id = f"trade_{idx:04d}_{safe_id(str(representative.get('symbol') or ''))}"
        trigger_labels = [
            f"{item.get('pump_window_h')}h/{fmt(item.get('pump_threshold_pct'), 0)}@{ts(item.get('trigger_ts'))}"
            for item in sorted(items, key=lambda item: int(item.get("trigger_ts") or 0))
        ]
        for item in items:
            item["trade_group_id"] = trade_group_id
            item["duplicate_event_count"] = len(items)
            item["trade_representative_report_id"] = representative.get("report_id")
            item["trigger_labels"] = " | ".join(trigger_labels)
            item["is_trade_representative"] = 1 if item is representative else 0
        unique.append(representative)
    return unique


def simulate_default_strategy(series: Series, event: PumpEvent) -> dict[str, Any]:
    base = base_result(series, event)
    entry_idx = find_confirmed_pullback_entry(
        series,
        event.trigger_idx,
        ENTRY_PULLBACK_PCT,
        max_wait_h=ADD_WINDOW_H,
        oi_max_pct=ENTRY_OI_MAX_PCT,
        long_ratio_min=LONG_RATIO_MIN,
        long_ratio_max=LONG_RATIO_MAX,
        funding_min_pct=-1.0,
    )
    if entry_idx is None:
        base.update({"status": "skipped_no_confirmed_entry", "skip_reason": "No 20% pullback with OI <= 50 and long ratio 0.45..0.65 within 168h"})
        return base

    entry_funding_prev_24h = funding_sum_pct(series.funding, series.ts[entry_idx] - 24 * 3_600_000, series.ts[entry_idx])
    if entry_funding_prev_24h is not None and entry_funding_prev_24h <= ENTRY_FUNDING_MIN_PCT:
        base.update(
            {
                "status": "skipped_toxic_funding",
                "skip_reason": f"Entry funding prev24h {entry_funding_prev_24h:.4f}% <= {ENTRY_FUNDING_MIN_PCT:.2f}%",
                "entry_idx": entry_idx,
                "entry_ts": series.ts[entry_idx],
                "entry_funding_prev_24h_pct": entry_funding_prev_24h,
            }
        )
        return base

    first_price = series.close[entry_idx]
    if not first_price or first_price <= 0:
        base.update({"status": "skipped_bad_entry_price", "skip_reason": "Entry candle has no usable close price"})
        return base

    planned_prices = [float(first_price) * (1.0 + STEP_PCT / 100.0 * leg_idx) for leg_idx in range(MAX_LEGS)]
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + MAX_HOLD_H)
    active: list[Leg] = [Leg(entry_idx, planned_prices[0], LEG_NOTIONAL_USD)]
    activated_legs = 1
    actions: list[dict[str, Any]] = [
        action("entry", series, entry_idx, planned_prices[0], LEG_NOTIONAL_USD, "Первая ступень после подтвержденного отката")
    ]
    peak_loss_usd = 0.0
    peak_extra_margin_usd = 0.0
    peak_margin_idx = entry_idx
    peak_high_price = planned_prices[0]
    max_adverse_from_first_pct = 0.0
    exit_idx = exit_limit_idx
    exit_price = series.close[exit_limit_idx]
    exit_reason = "time_stop_168h"

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        high = series.high[idx]
        low = series.low[idx]
        if high is not None and idx - entry_idx <= ADD_WINDOW_H:
            while activated_legs < MAX_LEGS and high >= planned_prices[activated_legs]:
                price = planned_prices[activated_legs]
                active.append(Leg(idx, price, LEG_NOTIONAL_USD))
                actions.append(action("add", series, idx, price, LEG_NOTIONAL_USD, f"Докупка ступени {activated_legs + 1} при росте цены на {STEP_PCT * activated_legs:.0f}% от первой цены входа"))
                activated_legs += 1

        if high is not None and active:
            loss = unrealized_short_loss_usd(active, high)
            posted_margin = opened_notional(active) / LEVERAGE
            extra_margin = max(0.0, loss - posted_margin)
            if extra_margin > peak_extra_margin_usd:
                peak_extra_margin_usd = extra_margin
                peak_loss_usd = loss
                peak_margin_idx = idx
                peak_high_price = high
            max_adverse_from_first_pct = max(max_adverse_from_first_pct, (high / planned_prices[0] - 1.0) * 100.0)

        avg_entry = weighted_avg_entry_price(active)
        if active and avg_entry and low is not None:
            target_price = avg_entry * (1.0 - TAKE_PROFIT_PCT / 100.0)
            if low <= target_price:
                exit_idx = idx
                exit_price = target_price
                exit_reason = "take_profit_25"
                break

    if not exit_price:
        base.update({"status": "skipped_bad_exit_price", "skip_reason": "Exit candle has no usable close price"})
        return base

    actions.append(action("exit", series, exit_idx, float(exit_price), opened_notional(active), "Полное закрытие по take-profit 25% или time-stop 168h"))
    leg_results = close_legs(series, active, exit_idx, float(exit_price))
    gross_notional = sum(item["notional_usd"] for item in leg_results)
    initial_margin = gross_notional / LEVERAGE
    funding_usd = sum(item["funding_usd"] for item in leg_results)
    fees_usd = sum(item["fee_usd"] for item in leg_results)
    price_pnl_usd = sum(item["price_pnl_usd"] for item in leg_results)
    net_pnl_usd = sum(item["net_pnl_usd"] for item in leg_results)
    peak_capital_required = initial_margin + peak_extra_margin_usd
    planned_margin_capacity = LEG_NOTIONAL_USD * MAX_LEGS / LEVERAGE
    base.update(
        {
            "status": "entered",
            "skip_reason": "",
            "entry_idx": entry_idx,
            "entry_ts": series.ts[entry_idx],
            "entry_price": planned_prices[0],
            "entry_funding_prev_24h_pct": entry_funding_prev_24h,
            "exit_idx": exit_idx,
            "exit_ts": series.ts[exit_idx],
            "exit_price": float(exit_price),
            "exit_reason": exit_reason,
            "time_in_trade_h": exit_idx - entry_idx,
            "legs_activated": len(leg_results),
            "gross_notional_usd": gross_notional,
            "initial_margin_usd": initial_margin,
            "planned_margin_capacity_usd": planned_margin_capacity,
            "peak_loss_usd": peak_loss_usd,
            "peak_extra_margin_usd": peak_extra_margin_usd,
            "peak_capital_required_usd": peak_capital_required,
            "peak_margin_ts": series.ts[peak_margin_idx],
            "peak_high_price": peak_high_price,
            "price_pnl_usd": price_pnl_usd,
            "funding_usd": funding_usd,
            "fees_usd": fees_usd,
            "net_pnl_usd": net_pnl_usd,
            "roi_on_peak_capital_pct": (net_pnl_usd / peak_capital_required * 100.0) if peak_capital_required > 0 else 0.0,
            "roi_on_gross_notional_pct": (net_pnl_usd / gross_notional * 100.0) if gross_notional > 0 else 0.0,
            "max_adverse_from_first_pct": max_adverse_from_first_pct,
            "planned_prices": planned_prices,
            "actions": actions,
            "leg_results": leg_results,
            "chart": chart_payload(series, event.trigger_idx, entry_idx, exit_idx, actions, planned_prices, peak_margin_idx),
        }
    )
    return base


def base_result(series: Series, event: PumpEvent) -> dict[str, Any]:
    report_id = safe_id(f"{event.symbol}_{event.config_window_h}h_{int(event.config_threshold_pct)}_{event.trigger_ts}")
    features = event_behavior_features(series, event)
    return {
        "report_id": report_id,
        "symbol": event.symbol,
        "event_id": event.event_id,
        "trigger_idx": event.trigger_idx,
        "trigger_ts": event.trigger_ts,
        "trigger_close": event.trigger_close,
        "pump_window_h": event.config_window_h,
        "pump_threshold_pct": event.config_threshold_pct,
        "pump_pct": event.pump_pct,
        "event_funding_prev_24h_pct": event.funding_prev_24h_pct,
        "event_oi_change_24h_pct": event.oi_change_24h_pct,
        "event_long_ratio": event.long_ratio,
        "funding_regime": features.get("funding_regime"),
        "oi_regime": features.get("oi_regime"),
        "pump_regime": features.get("pump_regime"),
    }


def close_legs(series: Series, legs: list[Leg], exit_idx: int, exit_price: float) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for number, leg in enumerate(legs, start=1):
        price_pnl_pct = (1.0 - exit_price / leg.price) * 100.0
        funding_pct = funding_sum_pct(series.funding, series.ts[leg.idx], series.ts[exit_idx]) or 0.0
        fee_usd = leg.notional * FEE_ROUNDTRIP_PCT / 100.0
        price_pnl_usd = leg.notional * price_pnl_pct / 100.0
        funding_usd = leg.notional * funding_pct / 100.0
        net_pnl_usd = price_pnl_usd + funding_usd - fee_usd
        results.append(
            {
                "leg": number,
                "entry_ts": series.ts[leg.idx],
                "entry_price": leg.price,
                "notional_usd": leg.notional,
                "exit_price": exit_price,
                "price_pnl_pct": price_pnl_pct,
                "price_pnl_usd": price_pnl_usd,
                "funding_pct": funding_pct,
                "funding_usd": funding_usd,
                "fee_usd": fee_usd,
                "net_pnl_usd": net_pnl_usd,
            }
        )
    return results


def summary_row(result: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "symbol",
        "event_id",
        "status",
        "skip_reason",
        "pump_window_h",
        "pump_threshold_pct",
        "pump_pct",
        "funding_regime",
        "oi_regime",
        "entry_ts",
        "entry_price",
        "entry_funding_prev_24h_pct",
        "exit_ts",
        "exit_price",
        "exit_reason",
        "time_in_trade_h",
        "legs_activated",
        "gross_notional_usd",
        "initial_margin_usd",
        "peak_extra_margin_usd",
        "peak_capital_required_usd",
        "price_pnl_usd",
        "funding_usd",
        "fees_usd",
        "net_pnl_usd",
        "roi_on_peak_capital_pct",
        "roi_on_gross_notional_pct",
        "max_adverse_from_first_pct",
        "trade_group_id",
        "duplicate_event_count",
        "is_trade_representative",
        "trade_representative_report_id",
        "trigger_labels",
        "report_id",
    )
    return {key: result.get(key, "") for key in keys}


def chart_payload(
    series: Series,
    trigger_idx: int,
    entry_idx: int,
    exit_idx: int,
    actions: list[dict[str, Any]],
    planned_prices: list[float],
    peak_margin_idx: int,
) -> dict[str, Any]:
    start = max(0, trigger_idx - 24)
    end = min(len(series.ts) - 1, exit_idx + 24)
    points = [
        {
            "idx": idx,
            "ts": series.ts[idx],
            "close": series.close[idx],
            "high": series.high[idx],
            "low": series.low[idx],
        }
        for idx in range(start, end + 1)
        if series.close[idx] is not None
    ]
    return {
        "start_idx": start,
        "end_idx": end,
        "points": points,
        "actions": actions,
        "planned_prices": planned_prices,
        "trigger_idx": trigger_idx,
        "entry_idx": entry_idx,
        "exit_idx": exit_idx,
        "peak_margin_idx": peak_margin_idx,
    }


def render_index(rows: list[dict[str, Any]], simulations: list[dict[str, Any]]) -> str:
    entered = [row for row in rows if row.get("status") == "entered"]
    skipped = [row for row in rows if row.get("status") != "entered"]
    total_net = sum(to_float(row.get("net_pnl_usd")) or 0.0 for row in entered)
    avg_roi = statistics.fmean([to_float(row.get("roi_on_peak_capital_pct")) or 0.0 for row in entered]) if entered else 0.0
    win_rate = sum(1 for row in entered if (to_float(row.get("net_pnl_usd")) or 0.0) > 0.0) / len(entered) * 100.0 if entered else 0.0
    worst_margin = max((to_float(row.get("peak_extra_margin_usd")) or 0.0 for row in entered), default=0.0)
    rows_html = "\n".join(render_index_row(row) for row in rows)
    return page_shell(
        "Pump-short visual report",
        f"""
        <section class="panel">
          <h1>Визуальный отчет pump-short стратегии</h1>
          <p>Базовая стратегия: pump trigger -> подтвержденный откат 20% -> OI 24h <= 50% -> long ratio 0.45..0.65 -> funding prev24h > -0.50% -> short ladder 4 x $1000, шаг +50%, выход TP 25% или 168h.</p>
          <div class="metrics">
            <div><b>{len(rows)}</b><span>pump-событий</span></div>
            <div><b>{len(entered)}</b><span>входов</span></div>
            <div><b>{len(skipped)}</b><span>пропусков</span></div>
            <div><b>{money(total_net)}</b><span>суммарный net PnL</span></div>
            <div><b>{fmt(win_rate)}%</b><span>winrate входов</span></div>
            <div><b>{fmt(avg_roi)}%</b><span>средний ROI на peak capital</span></div>
            <div><b>{money(worst_margin)}</b><span>макс. top-up маржи</span></div>
          </div>
        </section>
        <section class="panel">
          <h2>Все симуляции</h2>
          <table>
            <thead><tr><th>Symbol</th><th>Status</th><th>Pump</th><th>Entry</th><th>Legs</th><th>Top-up</th><th>Funding</th><th>Net</th><th>ROI</th><th>Report</th></tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </section>
        """,
    )


def render_index_row(row: dict[str, Any]) -> str:
    status = str(row.get("status") or "")
    link = ""
    if status == "entered":
        link = f"<a href=\"events/{esc(row.get('report_id'))}.html\">open</a>"
    else:
        link = esc(row.get("skip_reason"))
    return (
        "<tr>"
        f"<td>{esc(row.get('symbol'))}</td>"
        f"<td>{esc(status)}</td>"
        f"<td>{fmt(row.get('pump_pct'))}% / {esc(row.get('pump_window_h'))}h</td>"
        f"<td>{ts(row.get('entry_ts'))}</td>"
        f"<td>{esc(row.get('legs_activated'))}</td>"
        f"<td>{esc(row.get('duplicate_event_count'))}</td>"
        f"<td>{money(row.get('peak_extra_margin_usd'))}</td>"
        f"<td>{money(row.get('funding_usd'))}</td>"
        f"<td class=\"num\">{money(row.get('net_pnl_usd'))}</td>"
        f"<td>{fmt(row.get('roi_on_peak_capital_pct'))}%</td>"
        f"<td>{link}</td>"
        "</tr>"
    )


def render_index(rows: list[dict[str, Any]], simulations: list[dict[str, Any]], unique_rows: list[dict[str, Any]]) -> str:
    entered = [row for row in rows if row.get("status") == "entered"]
    skipped = [row for row in rows if row.get("status") != "entered"]
    total_net = sum(to_float(row.get("net_pnl_usd")) or 0.0 for row in unique_rows)
    avg_roi = statistics.fmean([to_float(row.get("roi_on_peak_capital_pct")) or 0.0 for row in unique_rows]) if unique_rows else 0.0
    win_rate = sum(1 for row in unique_rows if (to_float(row.get("net_pnl_usd")) or 0.0) > 0.0) / len(unique_rows) * 100.0 if unique_rows else 0.0
    worst_margin = max((to_float(row.get("peak_extra_margin_usd")) or 0.0 for row in unique_rows), default=0.0)
    unique_rows_html = "\n".join(render_index_row(row) for row in unique_rows)
    rows_html = "\n".join(render_index_row(row) for row in rows)
    return page_shell(
        "Pump-short visual report",
        f"""
        <section class="panel">
          <h1>Pump-short visual report</h1>
          <p>Base strategy: pump trigger -> confirmed 20% pullback -> OI 24h <= 50% -> long ratio 0.45..0.65 -> funding prev24h > -0.50% -> short ladder 4 x $1000, +50% spacing, exit TP 25% or 168h.</p>
          <div class="metrics">
            <div><b>{len(rows)}</b><span>pump trigger events</span></div>
            <div><b>{len(entered)}</b><span>trigger entries</span></div>
            <div><b>{len(unique_rows)}</b><span>unique live-like trades</span></div>
            <div><b>{len(skipped)}</b><span>skipped triggers</span></div>
            <div><b>{money(total_net)}</b><span>unique-trade net PnL</span></div>
            <div><b>{fmt(win_rate)}%</b><span>unique-trade winrate</span></div>
            <div><b>{fmt(avg_roi)}%</b><span>avg ROI on peak capital</span></div>
            <div><b>{money(worst_margin)}</b><span>max margin top-up</span></div>
          </div>
        </section>
        <section class="panel">
          <h2>Unique live-like trades</h2>
          <p>Several pump-trigger rows can point to the same entry/exit path. In live mode this should be one position, not several duplicate positions.</p>
          <table>
            <thead><tr><th>Symbol</th><th>Status</th><th>Pump</th><th>Entry</th><th>Legs</th><th>Same trade triggers</th><th>Top-up</th><th>Funding</th><th>Net</th><th>ROI</th><th>Report</th></tr></thead>
            <tbody>{unique_rows_html}</tbody>
          </table>
        </section>
        <section class="panel">
          <h2>All pump-trigger events</h2>
          <p>This table shows all historical trigger events. It does not mean live would open duplicate positions for the same symbol and same trade path.</p>
          <table>
            <thead><tr><th>Symbol</th><th>Status</th><th>Pump</th><th>Entry</th><th>Legs</th><th>Same trade triggers</th><th>Top-up</th><th>Funding</th><th>Net</th><th>ROI</th><th>Report</th></tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </section>
        """,
    )


def render_event_page(result: dict[str, Any]) -> str:
    actions_html = "\n".join(render_action_row(item) for item in result["actions"])
    legs_html = "\n".join(render_leg_row(item) for item in result["leg_results"])
    chart_svg = render_svg_chart(result)
    return page_shell(
        f"{result['symbol']} simulation",
        f"""
        <section class="panel">
          <p><a href="../index.html">← index</a></p>
          <h1>{esc(result['symbol'])}: симуляция стратегии</h1>
          <div class="metrics">
            <div><b>{fmt(result['pump_pct'])}%</b><span>pump за {esc(result['pump_window_h'])}h</span></div>
            <div><b>{esc(result['legs_activated'])}/4</b><span>ступеней вошло</span></div>
            <div><b>{money(result['gross_notional_usd'])}</b><span>gross short notional</span></div>
            <div><b>{money(result['initial_margin_usd'])}</b><span>начальная маржа 3x</span></div>
            <div><b>{money(result['peak_extra_margin_usd'])}</b><span>нужный top-up маржи</span></div>
            <div><b>{money(result['funding_usd'])}</b><span>funding impact</span></div>
            <div><b>{money(result['net_pnl_usd'])}</b><span>net PnL</span></div>
            <div><b>{fmt(result['roi_on_peak_capital_pct'])}%</b><span>ROI на peak capital</span></div>
          </div>
        </section>
        <section class="panel">
          <h2>График</h2>
          {chart_svg}
          <p class="legend">Красный: pump trigger. Оранжевый: входы/докупки. Зеленый: выход. Фиолетовый: момент максимального расчетного top-up маржи.</p>
        </section>
        <section class="panel">
          <h2>Пошагово</h2>
          <table><thead><tr><th>Time</th><th>Action</th><th>Price</th><th>Notional</th><th>Comment</th></tr></thead><tbody>{actions_html}</tbody></table>
        </section>
        <section class="panel">
          <h2>Денежный расчет по ступеням</h2>
          <table><thead><tr><th>Leg</th><th>Entry</th><th>Entry price</th><th>Notional</th><th>Price PnL</th><th>Funding</th><th>Fee</th><th>Net</th></tr></thead><tbody>{legs_html}</tbody></table>
        </section>
        <section class="panel">
          <h2>Технические условия</h2>
          <ul>
            <li>Entry funding prev24h: {fmt(result.get('entry_funding_prev_24h_pct'))}%</li>
            <li>Event funding regime: {esc(result.get('funding_regime'))}</li>
            <li>OI regime: {esc(result.get('oi_regime'))}</li>
            <li>Exit reason: {esc(result.get('exit_reason'))}</li>
            <li>Time in trade: {esc(result.get('time_in_trade_h'))}h</li>
            <li>Max adverse from first entry: {fmt(result.get('max_adverse_from_first_pct'))}%</li>
            <li>Peak high for margin: {fmt(result.get('peak_high_price'))} at {ts(result.get('peak_margin_ts'))}</li>
          </ul>
        </section>
        """,
    )


def render_svg_chart(result: dict[str, Any]) -> str:
    chart = result["chart"]
    points = chart["points"]
    if not points:
        return "<p>No chart data.</p>"
    width = 980
    height = 360
    pad_left = 56
    pad_right = 24
    pad_top = 24
    pad_bottom = 44
    values = [float(point["close"]) for point in points if point.get("close") is not None]
    for price in chart["planned_prices"]:
        values.append(float(price))
    min_price = min(values)
    max_price = max(values)
    span = max(max_price - min_price, max_price * 0.01)
    min_price -= span * 0.08
    max_price += span * 0.08
    start_idx = int(chart["start_idx"])
    end_idx = int(chart["end_idx"])
    xspan = max(1, end_idx - start_idx)

    def x(idx: int) -> float:
        return pad_left + (idx - start_idx) / xspan * (width - pad_left - pad_right)

    def y(price: float) -> float:
        return pad_top + (max_price - price) / (max_price - min_price) * (height - pad_top - pad_bottom)

    path_parts = [f"{x(int(point['idx'])):.1f},{y(float(point['close'])):.1f}" for point in points]
    line = " ".join(path_parts)
    levels = "\n".join(
        f"<line x1='{pad_left}' y1='{y(price):.1f}' x2='{width - pad_right}' y2='{y(price):.1f}' class='level'/><text x='{width - pad_right - 80}' y='{y(price)-4:.1f}' class='tiny'>step {idx + 1}</text>"
        for idx, price in enumerate(chart["planned_prices"])
    )
    markers = [
        f"<line x1='{x(int(chart['trigger_idx'])):.1f}' y1='{pad_top}' x2='{x(int(chart['trigger_idx'])):.1f}' y2='{height - pad_bottom}' class='trigger'/><text x='{x(int(chart['trigger_idx']))+4:.1f}' y='{pad_top+14}' class='tiny red'>pump</text>",
        f"<line x1='{x(int(chart['peak_margin_idx'])):.1f}' y1='{pad_top}' x2='{x(int(chart['peak_margin_idx'])):.1f}' y2='{height - pad_bottom}' class='peak'/><text x='{x(int(chart['peak_margin_idx']))+4:.1f}' y='{pad_top+30}' class='tiny purple'>peak margin</text>",
    ]
    for item in chart["actions"]:
        cls = "exit-dot" if item["type"] == "exit" else "entry-dot"
        markers.append(f"<circle cx='{x(int(item['idx'])):.1f}' cy='{y(float(item['price'])):.1f}' r='5' class='{cls}'><title>{esc(item['type'])} {fmt(item['price'])}</title></circle>")
    y_ticks = "\n".join(
        f"<text x='8' y='{y(price):.1f}' class='axis'>{fmt(price)}</text><line x1='{pad_left}' y1='{y(price):.1f}' x2='{width-pad_right}' y2='{y(price):.1f}' class='grid'/>"
        for price in nice_ticks(min_price, max_price, 5)
    )
    return (
        f"<svg viewBox='0 0 {width} {height}' class='chart' role='img'>"
        f"{y_ticks}{levels}<polyline points='{line}' class='price-line'/>"
        + "".join(markers)
        + f"<text x='{pad_left}' y='{height-12}' class='axis'>{ts(points[0]['ts'])}</text><text x='{width-190}' y='{height-12}' class='axis'>{ts(points[-1]['ts'])}</text></svg>"
    )


def render_action_row(item: dict[str, Any]) -> str:
    return (
        "<tr>"
        f"<td>{ts(item.get('ts'))}</td>"
        f"<td>{esc(item.get('type'))}</td>"
        f"<td>{fmt(item.get('price'))}</td>"
        f"<td>{money(item.get('notional_usd'))}</td>"
        f"<td>{esc(item.get('comment'))}</td>"
        "</tr>"
    )


def render_leg_row(item: dict[str, Any]) -> str:
    return (
        "<tr>"
        f"<td>{esc(item.get('leg'))}</td>"
        f"<td>{ts(item.get('entry_ts'))}</td>"
        f"<td>{fmt(item.get('entry_price'))}</td>"
        f"<td>{money(item.get('notional_usd'))}</td>"
        f"<td>{money(item.get('price_pnl_usd'))}</td>"
        f"<td>{money(item.get('funding_usd'))} ({fmt(item.get('funding_pct'))}%)</td>"
        f"<td>{money(item.get('fee_usd'))}</td>"
        f"<td>{money(item.get('net_pnl_usd'))}</td>"
        "</tr>"
    )


def action(kind: str, series: Series, idx: int, price: float, notional: float, comment: str) -> dict[str, Any]:
    return {"type": kind, "idx": idx, "ts": series.ts[idx], "price": price, "notional_usd": notional, "comment": comment}


def unrealized_short_loss_usd(legs: list[Leg], price: float) -> float:
    return sum(max(0.0, price / leg.price - 1.0) * leg.notional for leg in legs)


def opened_notional(legs: list[Leg]) -> float:
    return sum(leg.notional for leg in legs)


def weighted_avg_entry_price(legs: list[Leg]) -> float | None:
    total = opened_notional(legs)
    if total <= 0:
        return None
    return sum(leg.price * leg.notional for leg in legs) / total


def page_shell(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; background: #f5f7fa; color: #17202a; }}
    .panel {{ margin: 18px auto; max-width: 1180px; background: white; border: 1px solid #d9e0e7; border-radius: 8px; padding: 18px; }}
    h1, h2 {{ margin: 0 0 12px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e5e9ef; padding: 7px 8px; text-align: left; vertical-align: top; }}
    th {{ background: #eef2f6; position: sticky; top: 0; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); gap: 10px; }}
    .metrics div {{ border: 1px solid #e1e6ed; border-radius: 8px; padding: 10px; background: #fbfcfe; }}
    .metrics b {{ display: block; font-size: 18px; }}
    .metrics span {{ display: block; font-size: 12px; color: #607080; margin-top: 4px; }}
    .chart {{ width: 100%; height: auto; background: #fbfcfe; border: 1px solid #e1e6ed; border-radius: 8px; }}
    .price-line {{ fill: none; stroke: #1f77b4; stroke-width: 2; }}
    .level {{ stroke: #aab4bf; stroke-width: 1; stroke-dasharray: 4 4; }}
    .trigger {{ stroke: #d62728; stroke-width: 1.5; }}
    .peak {{ stroke: #7b3fb2; stroke-width: 1.5; stroke-dasharray: 5 3; }}
    .entry-dot {{ fill: #ff9800; stroke: #9b5b00; stroke-width: 1; }}
    .exit-dot {{ fill: #2ca02c; stroke: #145c14; stroke-width: 1; }}
    .grid {{ stroke: #e9edf2; stroke-width: 1; }}
    .axis, .tiny {{ fill: #596875; font-size: 11px; }}
    .red {{ fill: #d62728; }}
    .purple {{ fill: #7b3fb2; }}
    .legend {{ color: #596875; font-size: 13px; }}
    a {{ color: #0b65c2; text-decoration: none; }}
  </style>
</head>
<body>{body}</body>
</html>"""


def nice_ticks(min_value: float, max_value: float, count: int) -> list[float]:
    if count <= 1 or max_value <= min_value:
        return [min_value, max_value]
    step = (max_value - min_value) / (count - 1)
    return [min_value + step * idx for idx in range(count)]


def safe_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in value)


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def fmt(value: Any, digits: int = 2) -> str:
    number = to_float(value)
    if number is None or not math.isfinite(number):
        return ""
    return f"{number:.{digits}f}"


def money(value: Any) -> str:
    number = to_float(value)
    if number is None or not math.isfinite(number):
        return ""
    return f"${number:,.2f}"


def ts(value: Any) -> str:
    number = to_float(value)
    if number is None:
        return ""
    import datetime as _dt

    return _dt.datetime.fromtimestamp(number / 1000.0, tz=_dt.UTC).strftime("%Y-%m-%d %H:%M UTC")
