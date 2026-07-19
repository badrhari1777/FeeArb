from __future__ import annotations

import csv
import html
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_features.pump_short_cross_exchange_research import ms_to_iso, write_csv
from config import BASE_DIR

DEFAULT_INPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_bybit_funding_tp_capital_grid"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_strategy_graphic_report_1000"
CAPITAL_USD = 1_000.0
TIMELINE_START_MS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class StrategySpec:
    slug: str
    title: str
    description: str
    capital_usd: float
    slots: int
    funding_window_h: int
    funding_min_pct: float
    tp_pct: float
    primary: bool = False


STRATEGIES: tuple[StrategySpec, ...] = (
    StrategySpec(
        slug="main_default_3coins_tp25",
        title="1. Основная стратегия",
        description="Bybit, 3 монеты, prev24h funding > -0.5%, TP25/168h.",
        capital_usd=CAPITAL_USD,
        slots=3,
        funding_window_h=24,
        funding_min_pct=-0.5,
        tp_pct=25.0,
        primary=True,
    ),
    StrategySpec(
        slug="best_pure_roi_1coin_tp30",
        title="2. Лучший чистый ROI",
        description="1 монета, 21h funding > -1.0%, TP30. ROI высокий, но концентрация и top-up тяжелые.",
        capital_usd=CAPITAL_USD,
        slots=1,
        funding_window_h=21,
        funding_min_pct=-1.0,
        tp_pct=30.0,
    ),
    StrategySpec(
        slug="best_3coins_tp45",
        title="3. Лучший вариант на 3 монеты по ROI",
        description="3 монеты, 4h funding > -1.0%, TP45. Выше ROI, но длиннее удержание и ниже TP-hit.",
        capital_usd=CAPITAL_USD,
        slots=3,
        funding_window_h=4,
        funding_min_pct=-1.0,
        tp_pct=45.0,
    ),
    StrategySpec(
        slug="best_4coins_tp25",
        title="4. Лучший вариант на 4 монеты",
        description="4 монеты, 3h funding > -0.9%, TP25.",
        capital_usd=CAPITAL_USD,
        slots=4,
        funding_window_h=3,
        funding_min_pct=-0.9,
        tp_pct=25.0,
    ),
    StrategySpec(
        slug="default_4coins_tp25",
        title="5. Основная логика, но 4 монеты",
        description="4 монеты, prev24h funding > -0.5%, TP25/168h.",
        capital_usd=CAPITAL_USD,
        slots=4,
        funding_window_h=24,
        funding_min_pct=-0.5,
        tp_pct=25.0,
    ),
)


def run_strategy_graphic_report(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_rows = load_strategy_summaries(input_dir / "capital_slot_summary.csv")
    trades_by_strategy = load_selected_trades(input_dir / "selected_trades.csv")

    reports: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    topup_rows: list[dict[str, Any]] = []
    compact_summary: list[dict[str, Any]] = []

    for spec in STRATEGIES:
        key = strategy_key(spec)
        summary = summary_rows.get(key)
        trades = trades_by_strategy.get(key, [])
        report = build_strategy_report(spec, summary or {}, trades)
        reports.append(report)
        compact_summary.append(report["summary"])
        action_rows.extend(report["actions"])
        topup_rows.extend(report["topups"])

    write_csv(output_dir / "strategy_summary.csv", compact_summary)
    write_csv(output_dir / "actions.csv", action_rows)
    write_csv(output_dir / "topups.csv", topup_rows)
    html_report = render_html_report(reports)
    (output_dir / "index.html").write_text(html_report, encoding="utf-8")
    metadata = {
        "schema": "pump_short_strategy_graphic_report_v1",
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "capital_usd": CAPITAL_USD,
        "strategies": [spec.slug for spec in STRATEGIES],
        "summary_rows": len(compact_summary),
        "action_rows": len(action_rows),
        "topup_rows": len(topup_rows),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def load_strategy_summaries(path: Path) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    rows: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if to_float(row.get("capital_usd")) != CAPITAL_USD:
                continue
            rows[summary_key(row)] = row
    return rows


def load_selected_trades(path: Path) -> dict[tuple[str, str, str, str, str], list[dict[str, Any]]]:
    wanted = {strategy_key(spec) for spec in STRATEGIES}
    out: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = {key: [] for key in wanted}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            key = summary_key(row)
            if key in wanted:
                out[key].append(row)
    for rows in out.values():
        rows.sort(key=lambda row: (to_int(row.get("entry_ts")) or 0, str(row.get("symbol") or "")))
    return out


def build_strategy_report(spec: StrategySpec, summary: dict[str, Any], trades: list[dict[str, Any]]) -> dict[str, Any]:
    actions = build_action_rows(spec, trades)
    exposure = build_exposure_metrics(spec, actions)
    equity_points = build_equity_points(actions)
    topups = [row for row in actions if to_float(row.get("manual_topup_beyond_alloc_usd")) and to_float(row.get("manual_topup_beyond_alloc_usd")) > 0]
    max_concurrent_manual = max_concurrent_value(actions, "manual_topup_beyond_alloc_usd")
    max_concurrent_margin = max_concurrent_value(actions, "current_margin_topup_usd")
    net_pnl = to_float(summary.get("net_pnl_usd")) or sum(to_float(row.get("pnl_usd")) or 0.0 for row in actions)
    final_capital = CAPITAL_USD + net_pnl
    summary_out = {
        "slug": spec.slug,
        "title": spec.title,
        "description": spec.description,
        "primary": spec.primary,
        "capital_usd": CAPITAL_USD,
        "slots": spec.slots,
        "funding_window_h": spec.funding_window_h,
        "funding_min_pct": spec.funding_min_pct,
        "tp_pct": spec.tp_pct,
        "trades_taken": len(actions),
        "trades_skipped_slots": to_int(summary.get("trades_skipped_slots")) or 0,
        "trades_skipped_same_symbol": to_int(summary.get("trades_skipped_same_symbol")) or 0,
        "max_active_seen": to_int(summary.get("max_active_seen")) or exposure["max_active_seen"],
        "win_rate_pct": rounded(to_float(summary.get("win_rate_pct"))),
        "take_profit_rate_pct": rounded(to_float(summary.get("take_profit_rate_pct"))),
        "net_pnl_usd": rounded(net_pnl),
        "final_capital_usd": rounded(final_capital),
        "roi_on_initial_pct": rounded(percent(net_pnl, CAPITAL_USD)),
        "max_single_manual_topup_usd": rounded(max((to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0 for row in actions), default=0.0)),
        "manual_topup_events": len(topups),
        "manual_topup_sum_usd": rounded(sum(to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0 for row in actions)),
        "max_concurrent_manual_topup_usd": rounded(max_concurrent_manual),
        "max_concurrent_margin_topup_usd": rounded(max_concurrent_margin),
        "roi_on_initial_plus_max_concurrent_topup_pct": rounded(percent(net_pnl, CAPITAL_USD + max_concurrent_manual)),
        "avg_hold_h": rounded(to_float(summary.get("avg_hold_h"))),
        "idle_h": rounded(exposure["idle_h"]),
        "active_h": rounded(exposure["active_h"]),
        "full_capacity_h": rounded(exposure["full_capacity_h"]),
        "idle_pct": rounded(exposure["idle_pct"]),
        "active_pct": rounded(exposure["active_pct"]),
        "full_capacity_pct": rounded(exposure["full_capacity_pct"]),
        "full_capacity_reached_count": exposure["full_capacity_reached_count"],
        "first_entry_iso": actions[0]["entry_iso"] if actions else None,
        "last_exit_iso": actions[-1]["exit_iso"] if actions else None,
    }
    return {
        "spec": spec,
        "summary": summary_out,
        "actions": actions,
        "topups": topups,
        "equity_points": equity_points,
        "active_segments": exposure["segments"],
        "equity_svg": render_equity_svg(actions, equity_points, spec),
        "active_svg": render_active_svg(exposure["segments"], spec),
    }


def build_action_rows(spec: StrategySpec, trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active: list[tuple[int, str]] = []
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(trades, start=1):
        entry_ts = to_int(row.get("entry_ts")) or 0
        exit_ts = to_int(row.get("exit_ts")) or entry_ts
        active = [(active_exit, symbol) for active_exit, symbol in active if active_exit > entry_ts]
        active_after_entry = len(active) + 1
        active.append((exit_ts, str(row.get("symbol") or "")))
        action = {
            "strategy_slug": spec.slug,
            "strategy_title": spec.title,
            "row": index,
            "symbol": row.get("symbol"),
            "entry_ts": entry_ts,
            "entry_iso": row.get("entry_iso") or ms_to_iso(entry_ts),
            "exit_ts": exit_ts,
            "exit_iso": row.get("exit_iso") or ms_to_iso(exit_ts),
            "hold_h": rounded((exit_ts - entry_ts) / 3_600_000.0) if exit_ts >= entry_ts else 0.0,
            "active_after_entry": active_after_entry,
            "exit_reason": row.get("exit_reason"),
            "legs_filled": to_int(row.get("legs_filled")) or 0,
            "gross_notional_usd": rounded(to_float(row.get("gross_notional_usd"))),
            "pnl_usd": rounded(to_float(row.get("pnl_usd"))),
            "funding_usd": rounded(to_float(row.get("funding_usd"))),
            "net_pct": rounded(to_float(row.get("net_pct"))),
            "mae_pct": rounded(to_float(row.get("mae_pct"))),
            "funding_prev_pct": rounded(to_float(row.get("funding_prev_pct"))),
            "peak_unrealized_loss_usd": rounded(to_float(row.get("peak_unrealized_loss_usd"))),
            "current_margin_topup_usd": rounded(to_float(row.get("current_margin_topup_usd"))),
            "manual_topup_beyond_alloc_usd": rounded(to_float(row.get("manual_topup_beyond_alloc_usd"))),
        }
        rows.append(action)
    return rows


def build_exposure_metrics(spec: StrategySpec, actions: list[dict[str, Any]]) -> dict[str, Any]:
    if not actions:
        return {
            "segments": [],
            "idle_h": 0.0,
            "active_h": 0.0,
            "full_capacity_h": 0.0,
            "idle_pct": None,
            "active_pct": None,
            "full_capacity_pct": None,
            "max_active_seen": 0,
            "full_capacity_reached_count": 0,
        }
    start = TIMELINE_START_MS
    end = max(to_int(row.get("exit_ts")) or start for row in actions)
    events: dict[int, int] = {}
    for row in actions:
        events[to_int(row.get("entry_ts")) or start] = events.get(to_int(row.get("entry_ts")) or start, 0) + 1
        events[to_int(row.get("exit_ts")) or start] = events.get(to_int(row.get("exit_ts")) or start, 0) - 1
    current = 0
    prev = start
    segments: list[dict[str, Any]] = []
    idle_ms = 0
    active_ms = 0
    full_ms = 0
    max_active = 0
    full_reached = 0
    for ts_ms in sorted(ts for ts in events if ts >= start):
        if ts_ms > prev:
            duration = ts_ms - prev
            segments.append({"start_ts": prev, "end_ts": ts_ms, "active": current})
            if current <= 0:
                idle_ms += duration
            else:
                active_ms += duration
            if current >= spec.slots:
                full_ms += duration
        before = current
        current += events[ts_ms]
        current = max(0, current)
        if before < spec.slots <= current:
            full_reached += 1
        max_active = max(max_active, current)
        prev = ts_ms
    if prev < end:
        duration = end - prev
        segments.append({"start_ts": prev, "end_ts": end, "active": current})
        if current <= 0:
            idle_ms += duration
        else:
            active_ms += duration
        if current >= spec.slots:
            full_ms += duration
    total_ms = max(1, end - start)
    return {
        "segments": segments,
        "idle_h": idle_ms / 3_600_000.0,
        "active_h": active_ms / 3_600_000.0,
        "full_capacity_h": full_ms / 3_600_000.0,
        "idle_pct": idle_ms / total_ms * 100.0,
        "active_pct": active_ms / total_ms * 100.0,
        "full_capacity_pct": full_ms / total_ms * 100.0,
        "max_active_seen": max_active,
        "full_capacity_reached_count": full_reached,
    }


def build_equity_points(actions: list[dict[str, Any]]) -> list[tuple[int, float]]:
    if not actions:
        return [(TIMELINE_START_MS, CAPITAL_USD)]
    points: list[tuple[int, float]] = [(TIMELINE_START_MS, CAPITAL_USD)]
    equity = CAPITAL_USD
    exits: dict[int, float] = {}
    for row in actions:
        exit_ts = to_int(row.get("exit_ts")) or TIMELINE_START_MS
        exits[exit_ts] = exits.get(exit_ts, 0.0) + (to_float(row.get("pnl_usd")) or 0.0)
    for exit_ts in sorted(exits):
        equity += exits[exit_ts]
        points.append((exit_ts, equity))
    return points


def max_concurrent_value(actions: list[dict[str, Any]], key: str) -> float:
    events: list[tuple[int, float]] = []
    for row in actions:
        value = to_float(row.get(key)) or 0.0
        if value <= 0:
            continue
        events.append((to_int(row.get("entry_ts")) or 0, value))
        events.append((to_int(row.get("exit_ts")) or 0, -value))
    current = 0.0
    max_value = 0.0
    for _, delta in sorted(events, key=lambda item: (item[0], -item[1])):
        current += delta
        max_value = max(max_value, current)
    return max_value


def render_equity_svg(actions: list[dict[str, Any]], points: list[tuple[int, float]], spec: StrategySpec) -> str:
    width = 1120
    height = 300
    pad_l = 62
    pad_r = 24
    pad_t = 22
    pad_b = 42
    min_ts = TIMELINE_START_MS
    max_ts = max((ts for ts, _ in points), default=TIMELINE_START_MS + 1)
    values = [value for _, value in points]
    min_y = min(values + [CAPITAL_USD])
    max_y = max(values + [CAPITAL_USD])
    span_y = max(1.0, max_y - min_y)

    def x(ts_ms: int) -> float:
        return pad_l + (ts_ms - min_ts) / max(1, max_ts - min_ts) * (width - pad_l - pad_r)

    def y(value: float) -> float:
        return pad_t + (max_y - value) / span_y * (height - pad_t - pad_b)

    poly = " ".join(f"{x(ts):.2f},{y(value):.2f}" for ts, value in points)
    topup_rows = [row for row in actions if (to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0) > 0]
    max_topup = max((to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0 for row in topup_rows), default=1.0)
    bars: list[str] = []
    for row in topup_rows:
        amount = to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0
        bx = x(to_int(row.get("entry_ts")) or min_ts)
        bh = max(4.0, amount / max_topup * 70.0)
        bars.append(
            f"<rect x='{bx-2:.2f}' y='{height-pad_b-bh:.2f}' width='4' height='{bh:.2f}' class='topup-bar'>"
            f"<title>{esc(row.get('symbol'))}: top-up {money(amount)} during trade</title></rect>"
        )
    return (
        f"<svg class='strategy-chart' viewBox='0 0 {width} {height}' role='img' "
        f"aria-label='{esc(spec.title)} equity chart'>"
        f"<line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' class='axis'/>"
        f"<line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' class='axis'/>"
        f"<text x='8' y='{y(max_y)+4:.2f}' class='axis-label'>{money(max_y)}</text>"
        f"<text x='8' y='{y(min_y)+4:.2f}' class='axis-label'>{money(min_y)}</text>"
        f"<polyline points='{poly}' class='equity-line'/>"
        + "".join(bars)
        + f"<text x='{pad_l}' y='{height-12}' class='axis-label'>{short_date(min_ts)}</text>"
        + f"<text x='{width-140}' y='{height-12}' class='axis-label'>{short_date(max_ts)}</text>"
        + "<text x='76' y='18' class='chart-caption'>capital line; red bars mark trades that needed extra top-up beyond allocated coin budget</text>"
        + "</svg>"
    )


def render_active_svg(segments: list[dict[str, Any]], spec: StrategySpec) -> str:
    width = 1120
    height = 150
    pad_l = 62
    pad_r = 24
    pad_t = 16
    pad_b = 34
    if not segments:
        return f"<svg class='strategy-chart strategy-chart--small' viewBox='0 0 {width} {height}'></svg>"
    min_ts = TIMELINE_START_MS
    max_ts = max(int(row["end_ts"]) for row in segments)
    max_active = max(spec.slots, max(int(row["active"]) for row in segments))

    def x(ts_ms: int) -> float:
        return pad_l + (ts_ms - min_ts) / max(1, max_ts - min_ts) * (width - pad_l - pad_r)

    def y(active: int) -> float:
        return pad_t + (max_active - active) / max(1, max_active) * (height - pad_t - pad_b)

    rects: list[str] = []
    for row in segments:
        active = int(row["active"])
        if active <= 0:
            cls = "active-idle"
            top = pad_t
        elif active >= spec.slots:
            cls = "active-full"
            top = y(active)
        else:
            cls = "active-fill"
            top = y(active)
        x1 = x(int(row["start_ts"]))
        x2 = x(int(row["end_ts"]))
        rects.append(
            f"<rect x='{x1:.2f}' y='{top:.2f}' width='{max(0.5, x2-x1):.2f}' "
            f"height='{height-pad_b-top:.2f}' class='{cls}'><title>{active} active coins</title></rect>"
        )
    grid = "".join(
        f"<line x1='{pad_l}' y1='{y(level):.2f}' x2='{width-pad_r}' y2='{y(level):.2f}' class='grid-line'/>"
        f"<text x='22' y='{y(level)+4:.2f}' class='axis-label'>{level}</text>"
        for level in range(0, max_active + 1)
    )
    return (
        f"<svg class='strategy-chart strategy-chart--small' viewBox='0 0 {width} {height}' role='img' "
        f"aria-label='{esc(spec.title)} active coins chart'>"
        + grid
        + "".join(rects)
        + f"<line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' class='axis'/>"
        + f"<text x='{pad_l}' y='{height-8}' class='axis-label'>{short_date(min_ts)}</text>"
        + f"<text x='{width-140}' y='{height-8}' class='axis-label'>{short_date(max_ts)}</text>"
        + "<text x='76' y='14' class='chart-caption'>active coins; dark = idle, green = in market, amber = full capacity</text>"
        + "</svg>"
    )


def render_html_report(reports: list[dict[str, Any]]) -> str:
    generated = datetime.now(tz=timezone.utc).isoformat()
    summary_rows = [report["summary"] for report in reports]
    body = [
        "<!DOCTYPE html><html lang='ru'><head><meta charset='utf-8'/>",
        "<title>Pump-short strategy graphic report</title>",
        "<style>",
        REPORT_CSS,
        "</style></head><body>",
        "<header><h1>Pump-short: графический отчет с 2024 года</h1>",
        f"<p>Капитал: {money(CAPITAL_USD)}. Bybit-only historical shadow model. Generated: {esc(generated)}</p>",
        "<p class='note'>Top-up маркеры привязаны к входу сделки, потому что текущий selected-trades CSV хранит максимум MAE/top-up за сделку, но не точный час пика. Консервативный concurrent top-up считает, что максимум мог понадобиться в любой момент пока сделка была открыта.</p>",
        "</header>",
        "<section><h2>Сравнение стратегий</h2>",
        render_summary_table(summary_rows),
        "</section>",
    ]
    for report in reports:
        summary = report["summary"]
        body.extend(
            [
                f"<section class='strategy {'primary' if summary['primary'] else ''}'>",
                f"<h2>{esc(summary['title'])}</h2>",
                f"<p>{esc(summary['description'])}</p>",
                render_metric_grid(summary),
                "<h3>Капитал и top-up по времени</h3>",
                report["equity_svg"],
                "<h3>Занятость по времени</h3>",
                report["active_svg"],
                "<h3>Сделки, где требовалось пополнение</h3>",
                render_topup_table(report["topups"]),
                "<h3>Все действия</h3>",
                render_actions_table(report["actions"]),
                "</section>",
            ]
        )
    body.append("</body></html>")
    return "\n".join(body)


def render_summary_table(rows: list[dict[str, Any]]) -> str:
    columns = (
        ("title", "Стратегия"),
        ("slots", "Монет"),
        ("funding_window_h", "Funding window"),
        ("funding_min_pct", "Funding min"),
        ("tp_pct", "TP"),
        ("trades_taken", "Сделок"),
        ("roi_on_initial_pct", "ROI"),
        ("roi_on_initial_plus_max_concurrent_topup_pct", "ROI incl max top-up"),
        ("max_concurrent_manual_topup_usd", "Max concurrent top-up"),
        ("idle_pct", "Idle"),
        ("full_capacity_pct", "На потолке"),
        ("full_capacity_reached_count", "Потолок раз"),
    )
    return render_table(rows, columns)


def render_metric_grid(summary: dict[str, Any]) -> str:
    metrics = [
        ("Final capital", money(summary["final_capital_usd"])),
        ("Net PnL", money(summary["net_pnl_usd"])),
        ("ROI initial", pct_text(summary["roi_on_initial_pct"])),
        ("ROI incl max top-up", pct_text(summary["roi_on_initial_plus_max_concurrent_topup_pct"])),
        ("Trades", str(summary["trades_taken"])),
        ("Win / TP-hit", f"{pct_text(summary['win_rate_pct'])} / {pct_text(summary['take_profit_rate_pct'])}"),
        ("Idle / active", f"{pct_text(summary['idle_pct'])} / {pct_text(summary['active_pct'])}"),
        ("Full capacity", f"{pct_text(summary['full_capacity_pct'])}, {summary['full_capacity_reached_count']} раз"),
        ("Max single top-up", money(summary["max_single_manual_topup_usd"])),
        ("Max concurrent top-up", money(summary["max_concurrent_manual_topup_usd"])),
        ("Top-up events", str(summary["manual_topup_events"])),
        ("Avg hold", f"{summary['avg_hold_h']:.2f}h" if summary["avg_hold_h"] is not None else "-"),
    ]
    return "<div class='metrics'>" + "".join(f"<div><span>{esc(k)}</span><strong>{esc(v)}</strong></div>" for k, v in metrics) + "</div>"


def render_topup_table(rows: list[dict[str, Any]]) -> str:
    columns = (
        ("entry_iso", "Entry"),
        ("exit_iso", "Exit"),
        ("symbol", "Coin"),
        ("active_after_entry", "Active"),
        ("legs_filled", "Legs"),
        ("pnl_usd", "PnL"),
        ("mae_pct", "MAE"),
        ("current_margin_topup_usd", "Margin top-up"),
        ("manual_topup_beyond_alloc_usd", "Extra top-up"),
    )
    return render_table(rows, columns, empty="Пополнений сверх лимита на монету не было.")


def render_actions_table(rows: list[dict[str, Any]]) -> str:
    columns = (
        ("row", "#"),
        ("entry_iso", "Entry"),
        ("exit_iso", "Exit"),
        ("symbol", "Coin"),
        ("active_after_entry", "Active"),
        ("exit_reason", "Exit"),
        ("legs_filled", "Legs"),
        ("gross_notional_usd", "Notional"),
        ("pnl_usd", "PnL"),
        ("net_pct", "Net %"),
        ("funding_usd", "Funding"),
        ("mae_pct", "MAE %"),
        ("manual_topup_beyond_alloc_usd", "Extra top-up"),
    )
    return render_table(rows, columns)


def render_table(rows: list[dict[str, Any]], columns: tuple[tuple[str, str], ...], *, empty: str = "Нет строк.") -> str:
    if not rows:
        return f"<p class='note'>{esc(empty)}</p>"
    out = ["<div class='table-wrap'><table><thead><tr>"]
    out.extend(f"<th>{esc(label)}</th>" for _, label in columns)
    out.append("</tr></thead><tbody>")
    for row in rows:
        tr_class = " class='warn'" if (to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0) > 0 else ""
        out.append(f"<tr{tr_class}>")
        for key, _ in columns:
            out.append(f"<td>{format_value(key, row.get(key))}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


def format_value(key: str, value: Any) -> str:
    if value is None:
        return ""
    if key.endswith("_usd") or key in {"final_capital_usd", "net_pnl_usd", "max_concurrent_manual_topup_usd"}:
        return esc(money(to_float(value) or 0.0))
    if key.endswith("_pct") or key in {"funding_min_pct", "tp_pct"}:
        return esc(pct_text(to_float(value)))
    if key.endswith("_iso"):
        return esc(short_iso(str(value)))
    if isinstance(value, float):
        return esc(f"{value:.2f}")
    return esc(value)


def strategy_key(spec: StrategySpec) -> tuple[str, str, str, str, str]:
    return (
        f"{spec.capital_usd:.1f}",
        str(spec.slots),
        str(spec.funding_window_h),
        f"{spec.funding_min_pct:.1f}",
        f"{spec.tp_pct:.1f}",
    )


def summary_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        f"{to_float(row.get('capital_usd')) or 0.0:.1f}",
        str(int(to_float(row.get("slots")) or 0)),
        str(int(to_float(row.get("funding_window_h")) or 0)),
        f"{to_float(row.get('funding_min_pct')) or 0.0:.1f}",
        f"{to_float(row.get('tp_pct')) or 0.0:.1f}",
    )


def to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def to_int(value: Any) -> int | None:
    try:
        out = int(float(value))
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def percent(part: float, total: float) -> float | None:
    if not total:
        return None
    return part / total * 100.0


def rounded(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def money(value: float | None) -> str:
    if value is None:
        return "-"
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def pct_text(value: Any) -> str:
    number = to_float(value)
    return "-" if number is None else f"{number:.2f}%"


def short_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def short_iso(value: str) -> str:
    return value.replace("T", " ").replace("+00:00", " UTC")


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


REPORT_CSS = """
:root { color-scheme: dark; font-family: Segoe UI, Arial, sans-serif; background:#0b1017; color:#e6edf3; }
body { margin:0; background:#0b1017; }
header, section { padding:22px 28px; border-bottom:1px solid #1f2a38; }
header { background:#111a28; }
h1, h2, h3 { margin:0 0 10px; }
p { color:#a7b2c2; }
.note { color:#8d98a8; font-size:13px; }
.strategy.primary { border-left:5px solid #34d399; }
.metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:1px; background:#263449; border:1px solid #263449; border-radius:8px; overflow:hidden; margin:14px 0; }
.metrics div { background:#111a28; padding:12px; display:grid; gap:4px; }
.metrics span { color:#8d98a8; font-size:12px; text-transform:uppercase; }
.metrics strong { font-size:18px; }
.table-wrap { overflow:auto; max-height:620px; border:1px solid #263449; border-radius:8px; }
table { width:100%; border-collapse:collapse; font-size:13px; }
th, td { padding:7px 9px; border-bottom:1px solid #1f2a38; text-align:left; white-space:nowrap; }
th { position:sticky; top:0; background:#162235; color:#cbd5e1; z-index:1; }
tr.warn td { background:rgba(239,68,68,.08); }
.strategy-chart { width:100%; height:auto; display:block; margin:8px 0 18px; background:#0d1522; border:1px solid #263449; border-radius:8px; }
.strategy-chart--small { margin-top:8px; }
.axis { stroke:#41516a; stroke-width:1; }
.grid-line { stroke:#233047; stroke-width:1; }
.axis-label, .chart-caption { fill:#91a0b8; font-size:12px; }
.equity-line { fill:none; stroke:#34d399; stroke-width:2.5; }
.topup-bar { fill:#ef4444; opacity:.78; }
.active-idle { fill:#1a2332; }
.active-fill { fill:#22c55e; opacity:.52; }
.active-full { fill:#f59e0b; opacity:.68; }
"""


__all__ = ["run_strategy_graphic_report"]
