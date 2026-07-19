from __future__ import annotations

import csv
import html
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analysis_collectors.bybit_pump_short import round_float, to_float, to_int
from analysis_features.bybit_pump_short_outcomes import write_csv
from analysis_features.pump_funding_premium_window_research import FILTER_SPECS, filter_matches
from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "pump_funding_premium_window_research_5m_candidates" / "premium_long_outcomes.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_long_portfolio_sim_report_5m"
STARTING_CAPITAL_USD = 3000.0
SLOT_COUNTS = (1, 2, 3, 4, 5, 6)
LEVERAGES = (1.0, 2.0, 3.0)
SIZING_MODES = ("split_initial", "split_dynamic", "fixed_750")
CORE_TRACKS = (
    ("deep_discount_survives", "tp30_sl25_hold72_fundrelief", "premium_not_toxic_oi_wait"),
    ("deep_discount_survives", "tp30_sl25_hold72_fundrelief", "veto_wait30_oi10"),
    ("deep_discount_survives", "tp30_sl25_hold72_fundrelief", "veto_high_confidence_midpremium"),
)


@dataclass(frozen=True, slots=True)
class PortfolioSpec:
    entry_rule: str
    exit_plan: str
    filter_slug: str
    slots: int
    leverage: float
    sizing_mode: str

    @property
    def slug(self) -> str:
        lev = str(self.leverage).replace(".", "p")
        return f"{self.entry_rule}__{self.exit_plan}__{self.filter_slug}__s{self.slots}__l{lev}__{self.sizing_mode}"


def run_pump_long_portfolio_sim_report(
    *,
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    starting_capital_usd: float = STARTING_CAPITAL_USD,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = read_csv(input_path)
    grouped = group_rows(rows)

    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    for (entry_rule, exit_plan, filter_slug), items in sorted(grouped.items()):
        for slots in SLOT_COUNTS:
            for leverage in LEVERAGES:
                for sizing_mode in SIZING_MODES:
                    spec = PortfolioSpec(entry_rule, exit_plan, filter_slug, slots, leverage, sizing_mode)
                    summary, trades, equity = replay_portfolio(spec, items, starting_capital_usd=starting_capital_usd)
                    summary_rows.append(summary)
                    trade_rows.extend(trades)
                    equity_rows.extend(equity)

    summary_rows.sort(key=summary_sort_key, reverse=True)
    slot_rows = build_slot_comparison(summary_rows)
    selected = select_reports(summary_rows)
    html_report = render_html_report(summary_rows, slot_rows, selected, trade_rows, equity_rows, starting_capital_usd)

    write_csv(output_dir / "simulation_summary.csv", summary_rows)
    write_csv(output_dir / "simulation_trades.csv", trade_rows)
    write_csv(output_dir / "equity_points.csv", equity_rows)
    write_csv(output_dir / "slot_comparison.csv", slot_rows)
    (output_dir / "index.html").write_text(html_report, encoding="utf-8")

    metadata = {
        "schema": "pump_long_portfolio_sim_report_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "starting_capital_usd": starting_capital_usd,
        "input_rows": len(rows),
        "simulation_rows": len(summary_rows),
        "trade_rows": len(trade_rows),
        "equity_rows": len(equity_rows),
        "slot_rows": len(slot_rows),
        "selected_reports": len(selected),
        "slot_counts": SLOT_COUNTS,
        "leverages": LEVERAGES,
        "sizing_modes": SIZING_MODES,
        "elapsed_sec": round_float(time.time() - started),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def group_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        entry_rule = str(row.get("entry_rule") or "")
        exit_plan = str(row.get("exit_plan") or "")
        for spec in FILTER_SPECS:
            if filter_matches(row, spec):
                groups.setdefault((entry_rule, exit_plan, spec.slug), []).append(row)
    for key, items in groups.items():
        groups[key] = sorted(items, key=lambda row: (to_int(row.get("entry_ts")) or 0, str(row.get("symbol") or ""), str(row.get("event_id") or "")))
    return groups


def replay_portfolio(
    spec: PortfolioSpec,
    rows: list[dict[str, Any]],
    *,
    starting_capital_usd: float,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    active: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    equity_points: list[dict[str, Any]] = []
    realized_pnl = 0.0
    closed_count = 0
    skipped_slots = 0
    skipped_same_symbol = 0
    skipped_insolvent = 0
    max_active_seen = 0
    full_capacity_reached_count = 0
    peak_equity = starting_capital_usd
    max_drawdown_usd = 0.0
    worst_trade_usd: float | None = None
    worst_trade_pct: float | None = None
    best_trade_pct: float | None = None
    first_entry_ts: int | None = None
    last_exit_ts: int | None = None
    events: dict[int, int] = {}
    start_ts = min((to_int(row.get("entry_ts")) or 0 for row in rows), default=0)
    if start_ts:
        equity_points.append(equity_row(spec, start_ts, starting_capital_usd, 0.0, 0, "start"))

    def current_equity() -> float:
        return starting_capital_usd + realized_pnl

    def release_until(ts_ms: int) -> None:
        nonlocal active, realized_pnl, closed_count, peak_equity, max_drawdown_usd, last_exit_ts
        closed = [trade for trade in active if (to_int(trade.get("exit_ts")) or 0) <= ts_ms]
        active = [trade for trade in active if (to_int(trade.get("exit_ts")) or 0) > ts_ms]
        for trade in sorted(closed, key=lambda item: to_int(item.get("exit_ts")) or 0):
            pnl = to_float(trade.get("pnl_usd")) or 0.0
            realized_pnl += pnl
            closed_count += 1
            equity = current_equity()
            peak_equity = max(peak_equity, equity)
            max_drawdown_usd = max(max_drawdown_usd, peak_equity - equity)
            last_exit_ts = to_int(trade.get("exit_ts")) or last_exit_ts
            equity_points.append(equity_row(spec, last_exit_ts or ts_ms, equity, realized_pnl, len(active), "close"))

    for row in rows:
        entry_ts = to_int(row.get("entry_ts")) or 0
        exit_ts = to_int(row.get("exit_ts")) or 0
        if entry_ts <= 0 or exit_ts <= entry_ts:
            continue
        release_until(entry_ts)
        if current_equity() <= 0:
            skipped_insolvent += 1
            continue
        symbol = str(row.get("symbol") or "")
        if any(str(item.get("symbol") or "") == symbol for item in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= spec.slots:
            skipped_slots += 1
            continue
        slot_budget = resolve_slot_budget(spec, starting_capital_usd, current_equity())
        if slot_budget <= 0:
            skipped_insolvent += 1
            continue
        net_pct = to_float(row.get("net_pct"))
        if net_pct is None:
            continue
        levered_net_pct = net_pct * spec.leverage
        pnl_usd = slot_budget * levered_net_pct / 100.0
        trade = build_trade_row(spec, row, slot_budget, levered_net_pct, pnl_usd)
        active.append(trade)
        trades.append(trade)
        first_entry_ts = entry_ts if first_entry_ts is None else min(first_entry_ts, entry_ts)
        last_exit_ts = exit_ts if last_exit_ts is None else max(last_exit_ts, exit_ts)
        before_active = len(active) - 1
        if before_active < spec.slots <= len(active):
            full_capacity_reached_count += 1
        max_active_seen = max(max_active_seen, len(active))
        worst_trade_usd = pnl_usd if worst_trade_usd is None else min(worst_trade_usd, pnl_usd)
        worst_trade_pct = levered_net_pct if worst_trade_pct is None else min(worst_trade_pct, levered_net_pct)
        best_trade_pct = levered_net_pct if best_trade_pct is None else max(best_trade_pct, levered_net_pct)
        events[entry_ts] = events.get(entry_ts, 0) + 1
        events[exit_ts] = events.get(exit_ts, 0) - 1
    release_until(10**18)

    exposure = build_exposure_metrics(events, first_entry_ts, last_exit_ts, spec.slots)
    final_equity = current_equity()
    roi_pct = (final_equity / starting_capital_usd - 1.0) * 100.0
    wins = sum(1 for trade in trades if (to_float(trade.get("pnl_usd")) or 0.0) > 0.0)
    losses = sum(1 for trade in trades if (to_float(trade.get("pnl_usd")) or 0.0) < 0.0)
    max_drawdown_pct = max_drawdown_usd / starting_capital_usd * 100.0
    risk_adjusted_roi = roi_pct - max_drawdown_pct - max(0.0, -(worst_trade_usd or 0.0)) / starting_capital_usd * 50.0
    net_values = values(trades, "net_pct")
    pnl_values = values(trades, "pnl_usd")
    summary = {
        "strategy_slug": spec.slug,
        "entry_rule": spec.entry_rule,
        "exit_plan": spec.exit_plan,
        "filter_slug": spec.filter_slug,
        "slots": spec.slots,
        "leverage": spec.leverage,
        "sizing_mode": spec.sizing_mode,
        "starting_capital_usd": round_float(starting_capital_usd),
        "trades": len(trades),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_insolvent": skipped_insolvent,
        "wins": wins,
        "losses": losses,
        "win_pct": pct(wins, len(trades)),
        "loss_pct": pct(losses, len(trades)),
        "take_profit_pct": pct(sum(1 for trade in trades if trade.get("exit_reason") == "take_profit"), len(trades)),
        "stop_loss_pct": pct(sum(1 for trade in trades if trade.get("exit_reason") == "stop_loss"), len(trades)),
        "time_stop_pct": pct(sum(1 for trade in trades if trade.get("exit_reason") == "time_stop"), len(trades)),
        "final_equity_usd": round_float(final_equity),
        "realized_pnl_usd": round_float(realized_pnl),
        "roi_pct": round_float(roi_pct),
        "risk_adjusted_roi_pct": round_float(risk_adjusted_roi),
        "max_drawdown_usd": round_float(max_drawdown_usd),
        "max_drawdown_pct": round_float(max_drawdown_pct),
        "worst_trade_usd": round_float(worst_trade_usd),
        "worst_trade_pct": round_float(worst_trade_pct),
        "best_trade_pct": round_float(best_trade_pct),
        "avg_trade_usd": round_float(statistics.mean(pnl_values) if pnl_values else None),
        "avg_net_pct": round_float(statistics.mean(net_values) if net_values else None),
        "median_net_pct": round_float(statistics.median(net_values) if net_values else None),
        "avg_hold_h": round_float(statistics.mean(values(trades, "hold_h")) if trades else None),
        "first_entry_iso": ms_to_iso(first_entry_ts),
        "last_exit_iso": ms_to_iso(last_exit_ts),
        "unique_symbols": len({str(trade.get("symbol") or "") for trade in trades}),
        "max_active_seen": max_active_seen,
        "full_capacity_reached_count": full_capacity_reached_count,
        "idle_pct": round_float(exposure["idle_pct"]),
        "active_pct": round_float(exposure["active_pct"]),
        "full_capacity_pct": round_float(exposure["full_capacity_pct"]),
    }
    return summary, trades, equity_points


def resolve_slot_budget(spec: PortfolioSpec, starting_capital_usd: float, current_equity: float) -> float:
    if spec.sizing_mode == "split_dynamic":
        return max(0.0, current_equity) / max(1, spec.slots)
    if spec.sizing_mode == "fixed_750":
        return 750.0
    return starting_capital_usd / max(1, spec.slots)


def build_trade_row(spec: PortfolioSpec, row: dict[str, Any], slot_budget: float, levered_net_pct: float, pnl_usd: float) -> dict[str, Any]:
    entry_ts = to_int(row.get("entry_ts")) or 0
    exit_ts = to_int(row.get("exit_ts")) or entry_ts
    return {
        "strategy_slug": spec.slug,
        "entry_rule": spec.entry_rule,
        "exit_plan": spec.exit_plan,
        "filter_slug": spec.filter_slug,
        "slots": spec.slots,
        "leverage": spec.leverage,
        "sizing_mode": spec.sizing_mode,
        "slot_budget_usd": round_float(slot_budget),
        "symbol": row.get("symbol"),
        "event_id": row.get("event_id"),
        "entry_ts": entry_ts,
        "entry_iso": row.get("entry_iso") or ms_to_iso(entry_ts),
        "exit_ts": exit_ts,
        "exit_iso": row.get("exit_iso") or ms_to_iso(exit_ts),
        "hold_h": round_float((exit_ts - entry_ts) / 3_600_000.0),
        "exit_reason": row.get("exit_reason"),
        "net_pct": row.get("net_pct"),
        "levered_net_pct": round_float(levered_net_pct),
        "pnl_usd": round_float(pnl_usd),
        "long_funding_pct": row.get("long_funding_pct"),
        "gross_price_pct": row.get("gross_price_pct"),
        "mae_pct": row.get("mae_pct"),
        "mfe_pct": row.get("mfe_pct"),
        "entry_wait_h": row.get("entry_wait_h"),
        "entry_premium_pct": row.get("entry_premium_pct"),
        "entry_premium_relief_1h_pct": row.get("entry_premium_relief_1h_pct"),
        "entry_oi_change_4h_pct": row.get("entry_oi_change_4h_pct"),
        "entry_volume_z": row.get("entry_volume_z"),
        "trigger_pump_pct": row.get("trigger_pump_pct"),
    }


def equity_row(spec: PortfolioSpec, ts_ms: int | None, equity: float, realized_pnl: float, active_count: int, reason: str) -> dict[str, Any]:
    return {
        "strategy_slug": spec.slug,
        "entry_rule": spec.entry_rule,
        "exit_plan": spec.exit_plan,
        "filter_slug": spec.filter_slug,
        "slots": spec.slots,
        "leverage": spec.leverage,
        "sizing_mode": spec.sizing_mode,
        "ts": ts_ms,
        "iso": ms_to_iso(ts_ms),
        "equity_usd": round_float(equity),
        "realized_pnl_usd": round_float(realized_pnl),
        "active_count": active_count,
        "reason": reason,
    }


def build_exposure_metrics(events: dict[int, int], first_entry_ts: int | None, last_exit_ts: int | None, slots: int) -> dict[str, float]:
    if not first_entry_ts or not last_exit_ts or last_exit_ts <= first_entry_ts:
        return {"idle_pct": None, "active_pct": None, "full_capacity_pct": None}
    current = 0
    prev = first_entry_ts
    idle_ms = 0
    active_ms = 0
    full_ms = 0
    for ts_ms in sorted(ts for ts in events if first_entry_ts <= ts <= last_exit_ts):
        if ts_ms > prev:
            duration = ts_ms - prev
            if current <= 0:
                idle_ms += duration
            else:
                active_ms += duration
            if current >= slots:
                full_ms += duration
        current = max(0, current + events[ts_ms])
        prev = ts_ms
    total_ms = max(1, last_exit_ts - first_entry_ts)
    return {
        "idle_pct": idle_ms / total_ms * 100.0,
        "active_pct": active_ms / total_ms * 100.0,
        "full_capacity_pct": full_ms / total_ms * 100.0,
    }


def build_slot_comparison(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = {}
    for row in summary_rows:
        key = (
            str(row.get("entry_rule")),
            str(row.get("exit_plan")),
            str(row.get("filter_slug")),
            str(row.get("leverage")),
            str(row.get("sizing_mode")),
        )
        groups.setdefault(key, []).append(row)
    out: list[dict[str, Any]] = []
    for key, rows in groups.items():
        best_risk = max(rows, key=lambda row: to_float(row.get("risk_adjusted_roi_pct")) or -10**9)
        best_roi = max(rows, key=lambda row: to_float(row.get("roi_pct")) or -10**9)
        slots_one = next((row for row in rows if to_int(row.get("slots")) == 1), None)
        slots_two = next((row for row in rows if to_int(row.get("slots")) == 2), None)
        slots_four = next((row for row in rows if to_int(row.get("slots")) == 4), None)
        out.append(
            {
                "entry_rule": key[0],
                "exit_plan": key[1],
                "filter_slug": key[2],
                "leverage": key[3],
                "sizing_mode": key[4],
                "best_risk_slots": best_risk.get("slots"),
                "best_risk_adjusted_roi_pct": best_risk.get("risk_adjusted_roi_pct"),
                "best_risk_trades": best_risk.get("trades"),
                "best_roi_slots": best_roi.get("slots"),
                "best_roi_pct": best_roi.get("roi_pct"),
                "slot1_trades": slots_one.get("trades") if slots_one else None,
                "slot1_roi_pct": slots_one.get("roi_pct") if slots_one else None,
                "slot1_risk_adjusted_roi_pct": slots_one.get("risk_adjusted_roi_pct") if slots_one else None,
                "slot2_trades": slots_two.get("trades") if slots_two else None,
                "slot2_roi_pct": slots_two.get("roi_pct") if slots_two else None,
                "slot2_risk_adjusted_roi_pct": slots_two.get("risk_adjusted_roi_pct") if slots_two else None,
                "slot4_trades": slots_four.get("trades") if slots_four else None,
                "slot4_roi_pct": slots_four.get("roi_pct") if slots_four else None,
                "slot4_risk_adjusted_roi_pct": slots_four.get("risk_adjusted_roi_pct") if slots_four else None,
            }
        )
    out.sort(key=lambda row: to_float(row.get("best_risk_adjusted_roi_pct")) or -10**9, reverse=True)
    return out


def select_reports(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for row in summary_rows:
        if int(row.get("trades") or 0) >= 10 and str(row.get("sizing_mode")) in {"split_initial", "split_dynamic"}:
            selected.setdefault(str(row["strategy_slug"]), row)
        if len(selected) >= 24:
            break
    for entry_rule, exit_plan, filter_slug in CORE_TRACKS:
        for slots in SLOT_COUNTS:
            for sizing_mode in ("split_initial", "fixed_750"):
                row = next(
                    (
                        item
                        for item in summary_rows
                        if item.get("entry_rule") == entry_rule
                        and item.get("exit_plan") == exit_plan
                        and item.get("filter_slug") == filter_slug
                        and to_int(item.get("slots")) == slots
                        and to_float(item.get("leverage")) == 2.0
                        and item.get("sizing_mode") == sizing_mode
                    ),
                    None,
                )
                if row:
                    selected[str(row["strategy_slug"])] = row
    return sorted(selected.values(), key=summary_sort_key, reverse=True)[:60]


def render_html_report(
    summary_rows: list[dict[str, Any]],
    slot_rows: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    equity_rows: list[dict[str, Any]],
    starting_capital_usd: float,
) -> str:
    trade_map: dict[str, list[dict[str, Any]]] = {}
    equity_map: dict[str, list[dict[str, Any]]] = {}
    for row in trade_rows:
        trade_map.setdefault(str(row.get("strategy_slug")), []).append(row)
    for row in equity_rows:
        equity_map.setdefault(str(row.get("strategy_slug")), []).append(row)
    core_rows = [
        row
        for row in summary_rows
        if (row.get("entry_rule"), row.get("exit_plan"), row.get("filter_slug")) in CORE_TRACKS
        and to_float(row.get("leverage")) == 2.0
        and row.get("sizing_mode") in {"split_initial", "fixed_750"}
    ]
    generated = datetime.now(tz=timezone.utc).isoformat()
    return "\n".join(
        [
            "<!DOCTYPE html><html lang='ru'><head><meta charset='utf-8'/>",
            "<title>Pump long portfolio simulations</title>",
            "<style>",
            REPORT_CSS,
            "</style></head><body>",
            "<header>",
            "<h1>Bybit pump long: портфельные симуляции</h1>",
            f"<p>Источник: 5m premium/funding candidate outcomes. Capital: {money(starting_capital_usd)}. Generated: {esc(generated)}.</p>",
            "<p class='note'>Отчет перебирает entry rule, exit plan, фильтры, слоты 1..6, плечо 1x/2x/3x и sizing modes. HTML показывает главные/топовые графики; полный sweep лежит в CSV.</p>",
            "</header>",
            "<section><h2>Главные выводы</h2>",
            render_takeaways(summary_rows),
            "</section>",
            "<section><h2>Core tracks: сравнение слотов</h2>",
            render_table(core_rows, SUMMARY_COLUMNS, limit=120),
            "</section>",
            "<section><h2>Top simulations</h2>",
            render_table(summary_rows, SUMMARY_COLUMNS, limit=80),
            "</section>",
            "<section><h2>Best slot by track</h2>",
            render_table(slot_rows, SLOT_COLUMNS, limit=80),
            "</section>",
            "<section><h2>Selected equity curves</h2>",
            "".join(render_strategy_block(row, trade_map.get(str(row.get("strategy_slug")), []), equity_map.get(str(row.get("strategy_slug")), []), starting_capital_usd) for row in selected),
            "</section>",
            "</body></html>",
        ]
    )


def render_takeaways(summary_rows: list[dict[str, Any]]) -> str:
    broad = find_core(summary_rows, "premium_not_toxic_oi_wait", 4, 2.0, "split_initial")
    clean = find_core(summary_rows, "veto_wait30_oi10", 4, 2.0, "split_initial")
    strict = find_core(summary_rows, "veto_high_confidence_midpremium", 4, 2.0, "split_initial")
    broad_slot1 = find_core(summary_rows, "premium_not_toxic_oi_wait", 1, 2.0, "split_initial")
    broad_fixed = find_core(summary_rows, "premium_not_toxic_oi_wait", 1, 2.0, "fixed_750")
    lines = [
        "<ul>",
        f"<li><strong>Broad paper track:</strong> {describe_row(broad)}.</li>",
        f"<li><strong>Clean control:</strong> {describe_row(clean)}.</li>",
        f"<li><strong>High-confidence:</strong> {describe_row(strict)}; это малая выборка, не live-доказательство.</li>",
        f"<li><strong>1 слот с полным капиталом:</strong> {describe_row(broad_slot1)}. Это проверяет концентрацию, а не только частоту.</li>",
        f"<li><strong>1 слот с тем же бюджетом $750:</strong> {describe_row(broad_fixed)}. Это изолирует вопрос, нужна ли параллельность без увеличения размера ставки.</li>",
        "</ul>",
    ]
    return "".join(lines)


def render_strategy_block(row: dict[str, Any], trades: list[dict[str, Any]], equity: list[dict[str, Any]], starting_capital_usd: float) -> str:
    title = f"{row.get('entry_rule')} / {row.get('exit_plan')} / {row.get('filter_slug')}"
    subtitle = f"slots={row.get('slots')}, leverage={row.get('leverage')}x, sizing={row.get('sizing_mode')}"
    return (
        "<article class='strategy'>"
        f"<h3>{esc(title)}</h3><p class='note'>{esc(subtitle)}</p>"
        + render_metric_grid(row)
        + render_equity_svg(equity, starting_capital_usd)
        + render_table(trades, TRADE_COLUMNS, limit=30)
        + "</article>"
    )


def render_metric_grid(row: dict[str, Any]) -> str:
    metrics = [
        ("Trades", row.get("trades")),
        ("ROI", pct_text(row.get("roi_pct"))),
        ("Risk adj", pct_text(row.get("risk_adjusted_roi_pct"))),
        ("Win", pct_text(row.get("win_pct"))),
        ("Max DD", pct_text(row.get("max_drawdown_pct"))),
        ("Worst", pct_text(row.get("worst_trade_pct"))),
        ("Skipped slots", row.get("skipped_slots")),
        ("Same-symbol skips", row.get("skipped_same_symbol")),
    ]
    return "<div class='metrics'>" + "".join(f"<div><span>{esc(k)}</span><strong>{format_cell(v)}</strong></div>" for k, v in metrics) + "</div>"


def render_equity_svg(points: list[dict[str, Any]], starting_capital_usd: float) -> str:
    width = 1120
    height = 280
    pad_l = 68
    pad_r = 24
    pad_t = 20
    pad_b = 42
    clean = [(to_int(row.get("ts")) or 0, to_float(row.get("equity_usd")) or starting_capital_usd) for row in points if to_int(row.get("ts"))]
    if not clean:
        clean = [(0, starting_capital_usd), (1, starting_capital_usd)]
    min_ts = min(ts for ts, _ in clean)
    max_ts = max(ts for ts, _ in clean)
    min_y = min(value for _, value in clean + [(min_ts, starting_capital_usd)])
    max_y = max(value for _, value in clean + [(min_ts, starting_capital_usd)])
    span_y = max(1.0, max_y - min_y)

    def x(ts_ms: int) -> float:
        return pad_l + (ts_ms - min_ts) / max(1, max_ts - min_ts) * (width - pad_l - pad_r)

    def y(value: float) -> float:
        return pad_t + (max_y - value) / span_y * (height - pad_t - pad_b)

    poly = " ".join(f"{x(ts):.2f},{y(value):.2f}" for ts, value in clean)
    zero_line = y(starting_capital_usd)
    return (
        f"<svg class='chart' viewBox='0 0 {width} {height}' role='img'>"
        f"<line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' class='axis'/>"
        f"<line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' class='axis'/>"
        f"<line x1='{pad_l}' y1='{zero_line:.2f}' x2='{width-pad_r}' y2='{zero_line:.2f}' class='base-line'/>"
        f"<text x='8' y='{y(max_y)+4:.2f}' class='axis-label'>{money(max_y)}</text>"
        f"<text x='8' y='{y(min_y)+4:.2f}' class='axis-label'>{money(min_y)}</text>"
        f"<polyline points='{poly}' class='equity-line'/>"
        f"<text x='{pad_l}' y='{height-12}' class='axis-label'>{short_date(min_ts)}</text>"
        f"<text x='{width-145}' y='{height-12}' class='axis-label'>{short_date(max_ts)}</text>"
        "</svg>"
    )


def render_table(rows: list[dict[str, Any]], columns: tuple[tuple[str, str], ...], *, limit: int) -> str:
    shown = rows[:limit]
    if not shown:
        return "<p class='note'>Нет строк.</p>"
    out = ["<div class='table-wrap'><table><thead><tr>"]
    out.extend(f"<th>{esc(label)}</th>" for _, label in columns)
    out.append("</tr></thead><tbody>")
    for row in shown:
        cls = "loss" if (to_float(row.get("pnl_usd")) or 0.0) < 0 else ""
        out.append(f"<tr class='{cls}'>")
        for key, _ in columns:
            out.append(f"<td>{format_cell(row.get(key), key)}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    if len(rows) > limit:
        out.append(f"<p class='note'>Показано {limit} из {len(rows)} строк. Полные данные в CSV.</p>")
    return "".join(out)


def find_core(summary_rows: list[dict[str, Any]], filter_slug: str, slots: int, leverage: float, sizing_mode: str) -> dict[str, Any] | None:
    return next(
        (
            row
            for row in summary_rows
            if row.get("entry_rule") == "deep_discount_survives"
            and row.get("exit_plan") == "tp30_sl25_hold72_fundrelief"
            and row.get("filter_slug") == filter_slug
            and to_int(row.get("slots")) == slots
            and to_float(row.get("leverage")) == leverage
            and row.get("sizing_mode") == sizing_mode
        ),
        None,
    )


def describe_row(row: dict[str, Any] | None) -> str:
    if not row:
        return "нет строки"
    return (
        f"{row.get('trades')} trades, ROI {pct_text(row.get('roi_pct'))}, "
        f"risk-adjusted {pct_text(row.get('risk_adjusted_roi_pct'))}, win {pct_text(row.get('win_pct'))}, "
        f"max DD {pct_text(row.get('max_drawdown_pct'))}, worst {pct_text(row.get('worst_trade_pct'))}"
    )


def summary_sort_key(row: dict[str, Any]) -> tuple[float, float, float, int]:
    return (
        to_float(row.get("risk_adjusted_roi_pct")) or -10**9,
        to_float(row.get("roi_pct")) or -10**9,
        to_float(row.get("win_pct")) or -10**9,
        to_int(row.get("trades")) or 0,
    )


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def values(rows: list[dict[str, Any]], key: str) -> list[float]:
    out = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None and not math.isnan(value):
            out.append(value)
    return out


def pct(part: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round_float(part / total * 100.0)


def ms_to_iso(ts_ms: int | None) -> str | None:
    if not ts_ms:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def short_date(ts_ms: int | None) -> str:
    if not ts_ms:
        return "-"
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def money(value: Any) -> str:
    num = to_float(value)
    if num is None:
        return "-"
    return f"${num:,.2f}"


def pct_text(value: Any) -> str:
    num = to_float(value)
    if num is None:
        return "-"
    return f"{num:.2f}%"


def format_cell(value: Any, key: str | None = None) -> str:
    if value is None:
        return ""
    if key and (key.endswith("_usd") or key in {"final_equity_usd", "realized_pnl_usd"}):
        return esc(money(value))
    if key and (key.endswith("_pct") or key in {"leverage"}):
        return esc(pct_text(value) if key != "leverage" else f"{to_float(value):.1f}x")
    if key and key.endswith("_iso"):
        text = str(value)
        return esc(text[:16].replace("T", " "))
    num = to_float(value)
    if num is not None and isinstance(value, float):
        return esc(f"{num:.2f}")
    return esc(value)


SUMMARY_COLUMNS = (
    ("entry_rule", "Entry"),
    ("exit_plan", "Exit"),
    ("filter_slug", "Filter"),
    ("slots", "Slots"),
    ("leverage", "Lev"),
    ("sizing_mode", "Sizing"),
    ("trades", "Trades"),
    ("skipped_slots", "Skip slots"),
    ("win_pct", "Win"),
    ("roi_pct", "ROI"),
    ("risk_adjusted_roi_pct", "Risk adj"),
    ("max_drawdown_pct", "Max DD"),
    ("worst_trade_pct", "Worst"),
    ("first_entry_iso", "First"),
    ("last_exit_iso", "Last"),
)

SLOT_COLUMNS = (
    ("entry_rule", "Entry"),
    ("exit_plan", "Exit"),
    ("filter_slug", "Filter"),
    ("leverage", "Lev"),
    ("sizing_mode", "Sizing"),
    ("best_risk_slots", "Best risk slots"),
    ("best_risk_adjusted_roi_pct", "Best risk adj"),
    ("best_risk_trades", "Trades"),
    ("best_roi_slots", "Best ROI slots"),
    ("best_roi_pct", "Best ROI"),
    ("slot1_roi_pct", "Slot1 ROI"),
    ("slot2_roi_pct", "Slot2 ROI"),
    ("slot4_roi_pct", "Slot4 ROI"),
)

TRADE_COLUMNS = (
    ("entry_iso", "Entry"),
    ("exit_iso", "Exit"),
    ("symbol", "Coin"),
    ("exit_reason", "Exit"),
    ("slot_budget_usd", "Budget"),
    ("levered_net_pct", "Levered net"),
    ("pnl_usd", "PnL"),
    ("entry_premium_pct", "Premium"),
    ("entry_oi_change_4h_pct", "OI 4h"),
    ("entry_volume_z", "Vol z"),
)

REPORT_CSS = """
body { margin: 0; font: 14px/1.45 -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #10131a; color: #e7ecf3; }
header, section { max-width: 1280px; margin: 0 auto; padding: 24px; }
h1, h2, h3 { margin: 0 0 12px; }
.note { color: #9aa8bb; }
.table-wrap { overflow: auto; border: 1px solid #283242; border-radius: 10px; margin: 12px 0 20px; }
table { width: 100%; border-collapse: collapse; white-space: nowrap; }
th, td { padding: 7px 9px; border-bottom: 1px solid #263040; text-align: right; }
th:first-child, td:first-child, th:nth-child(2), td:nth-child(2), th:nth-child(3), td:nth-child(3) { text-align: left; }
th { background: #192131; color: #b8c7da; position: sticky; top: 0; }
tr.loss td { background: rgba(239, 68, 68, 0.08); }
.strategy { background: #151b27; border: 1px solid #283242; border-radius: 14px; padding: 18px; margin: 18px 0; }
.metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(135px, 1fr)); gap: 10px; margin: 12px 0; }
.metrics div { background: #0f1520; border: 1px solid #273245; border-radius: 10px; padding: 10px; }
.metrics span { display: block; color: #8fa0b5; font-size: 12px; }
.metrics strong { display: block; color: #f7fafc; font-size: 17px; margin-top: 4px; }
.chart { width: 100%; height: auto; background: #0f1520; border: 1px solid #273245; border-radius: 10px; margin: 12px 0; }
.axis { stroke: #607086; stroke-width: 1; }
.base-line { stroke: #7c8798; stroke-dasharray: 4 4; stroke-width: 1; }
.axis-label { fill: #9aa8bb; font-size: 12px; }
.equity-line { fill: none; stroke: #22c55e; stroke-width: 2.5; }
ul { margin-top: 8px; }
li { margin: 8px 0; }
"""


__all__ = [
    "CORE_TRACKS",
    "DEFAULT_INPUT",
    "DEFAULT_OUTPUT_DIR",
    "LEVERAGES",
    "SIZING_MODES",
    "SLOT_COUNTS",
    "PortfolioSpec",
    "replay_portfolio",
    "run_pump_long_portfolio_sim_report",
]
