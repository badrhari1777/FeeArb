from __future__ import annotations

import csv
import html
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analysis_features.pump_short_strategy_graphic_report import (
    CAPITAL_USD,
    DEFAULT_INPUT_DIR,
    STRATEGIES,
    TIMELINE_START_MS,
    StrategySpec,
    build_exposure_metrics,
    money,
    pct_text,
    render_active_svg,
    render_equity_svg,
    short_iso,
    strategy_key,
    summary_key,
    to_float,
    to_int,
)
from analysis_features.pump_short_cross_exchange_research import write_csv
from config import BASE_DIR

DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_strategy_compound_report_1000"
LADDER_LEGS = 4
LEVERAGE = 3.0


def run_strategy_compound_report(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades_by_strategy = load_selected_trades(input_dir / "selected_trades.csv")
    reports: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    topup_rows: list[dict[str, Any]] = []

    for spec in STRATEGIES:
        trades = trades_by_strategy.get(strategy_key(spec), [])
        report = simulate_compound_strategy(spec, trades)
        reports.append(report)
        summary_rows.append(report["summary"])
        action_rows.extend(report["actions"])
        topup_rows.extend(report["topups"])

    write_csv(output_dir / "compound_strategy_summary.csv", summary_rows)
    write_csv(output_dir / "compound_actions.csv", action_rows)
    write_csv(output_dir / "compound_topups.csv", topup_rows)
    report_path = output_dir / "index.html"
    report_path.write_text(render_html_report(reports), encoding="utf-8")
    metadata = {
        "schema": "pump_short_strategy_compound_report_v1",
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "report_path": str(report_path),
        "capital_usd": CAPITAL_USD,
        "capital_model": "dynamic_current_capital_per_coin_per_ladder_step",
        "sizing": "capital_at_entry / max_active_coins / 4 ladder steps * 3x",
        "topup_model": "temporary external rescue capital; not added to strategy equity",
        "strategies": [spec.slug for spec in STRATEGIES],
        "summary_rows": len(summary_rows),
        "action_rows": len(action_rows),
        "topup_rows": len(topup_rows),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


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


def simulate_compound_strategy(spec: StrategySpec, trades: list[dict[str, Any]]) -> dict[str, Any]:
    current_capital = CAPITAL_USD
    equity_points: list[tuple[int, float]] = [(TIMELINE_START_MS, current_capital)]
    active: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    skipped_insolvent = 0

    def close_due(until_ts: int) -> None:
        nonlocal current_capital, active
        due = sorted([item for item in active if int(item["exit_ts"]) <= until_ts], key=lambda item: (int(item["exit_ts"]), int(item["row"])))
        active = [item for item in active if int(item["exit_ts"]) > until_ts]
        for item in due:
            current_capital += to_float(item.get("pnl_usd")) or 0.0
            item["capital_after_exit_usd"] = round(current_capital, 6)
            equity_points.append((int(item["exit_ts"]), current_capital))

    for index, row in enumerate(trades, start=1):
        entry_ts = to_int(row.get("entry_ts")) or 0
        close_due(entry_ts)
        if current_capital <= 0:
            skipped_insolvent += 1
            continue
        static_step_notional = to_float(row.get("per_step_notional_usd")) or 0.0
        per_coin_capital = current_capital / max(1, spec.slots)
        per_step_margin = per_coin_capital / LADDER_LEGS
        per_step_notional = per_step_margin * LEVERAGE
        scale = per_step_notional / static_step_notional if static_step_notional > 0 else 0.0
        action = build_compound_action(
            spec,
            row,
            index=index,
            active_after_entry=len(active) + 1,
            capital_before_entry=current_capital,
            per_coin_capital=per_coin_capital,
            per_step_margin=per_step_margin,
            per_step_notional=per_step_notional,
            scale=scale,
        )
        active.append(action)
        actions.append(action)

    close_due(10**18)
    if equity_points[-1][0] < max((to_int(row.get("exit_ts")) or TIMELINE_START_MS for row in actions), default=TIMELINE_START_MS):
        equity_points.append((max(to_int(row.get("exit_ts")) or TIMELINE_START_MS for row in actions), current_capital))

    exposure = build_exposure_metrics(spec, actions)
    topups = [row for row in actions if (to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0) > 0]
    max_concurrent_manual = max_concurrent_value(actions, "manual_topup_beyond_alloc_usd")
    max_concurrent_margin = max_concurrent_value(actions, "current_margin_topup_usd")
    net_pnl = current_capital - CAPITAL_USD
    wins = sum(1 for row in actions if (to_float(row.get("pnl_usd")) or 0.0) > 0)
    tp_hits = sum(1 for row in actions if row.get("exit_reason") == "take_profit")
    summary = {
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
        "skipped_insolvent": skipped_insolvent,
        "max_active_seen": exposure["max_active_seen"],
        "win_rate_pct": rounded(percent(wins, len(actions))),
        "take_profit_rate_pct": rounded(percent(tp_hits, len(actions))),
        "net_pnl_usd": rounded(net_pnl),
        "final_capital_usd": rounded(current_capital),
        "roi_on_initial_pct": rounded(percent(net_pnl, CAPITAL_USD)),
        "max_single_manual_topup_usd": rounded(max((to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0 for row in actions), default=0.0)),
        "manual_topup_events": len(topups),
        "manual_topup_turnover_usd": rounded(sum(to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0 for row in actions)),
        "max_concurrent_manual_topup_usd": rounded(max_concurrent_manual),
        "max_concurrent_margin_topup_usd": rounded(max_concurrent_margin),
        "roi_on_initial_plus_max_concurrent_topup_pct": rounded(percent(net_pnl, CAPITAL_USD + max_concurrent_manual)),
        "avg_hold_h": rounded(mean([to_float(row.get("hold_h")) or 0.0 for row in actions])),
        "avg_step_notional_usd": rounded(mean([to_float(row.get("per_step_notional_usd")) or 0.0 for row in actions])),
        "max_step_notional_usd": rounded(max((to_float(row.get("per_step_notional_usd")) or 0.0 for row in actions), default=0.0)),
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
        "summary": summary,
        "actions": actions,
        "topups": topups,
        "equity_points": equity_points,
        "active_segments": exposure["segments"],
        "equity_svg": render_equity_svg(actions, equity_points, spec),
        "active_svg": render_active_svg(exposure["segments"], spec),
    }


def build_compound_action(
    spec: StrategySpec,
    row: dict[str, Any],
    *,
    index: int,
    active_after_entry: int,
    capital_before_entry: float,
    per_coin_capital: float,
    per_step_margin: float,
    per_step_notional: float,
    scale: float,
) -> dict[str, Any]:
    entry_ts = to_int(row.get("entry_ts")) or 0
    exit_ts = to_int(row.get("exit_ts")) or entry_ts
    pnl = (to_float(row.get("pnl_usd")) or 0.0) * scale
    peak_loss = (to_float(row.get("peak_unrealized_loss_usd")) or 0.0) * scale
    current_margin_topup = (to_float(row.get("current_margin_topup_usd")) or 0.0) * scale
    manual_topup = max(0.0, peak_loss - per_coin_capital)
    return {
        "strategy_slug": spec.slug,
        "strategy_title": spec.title,
        "row": index,
        "symbol": row.get("symbol"),
        "entry_ts": entry_ts,
        "entry_iso": row.get("entry_iso"),
        "exit_ts": exit_ts,
        "exit_iso": row.get("exit_iso"),
        "hold_h": rounded((exit_ts - entry_ts) / 3_600_000.0) if exit_ts >= entry_ts else 0.0,
        "active_after_entry": active_after_entry,
        "capital_before_entry_usd": rounded(capital_before_entry),
        "capital_after_exit_usd": None,
        "per_coin_capital_usd": rounded(per_coin_capital),
        "per_step_margin_usd": rounded(per_step_margin),
        "per_step_notional_usd": rounded(per_step_notional),
        "size_scale_vs_static": rounded(scale),
        "exit_reason": row.get("exit_reason"),
        "legs_filled": to_int(row.get("legs_filled")) or 0,
        "gross_notional_usd": rounded((to_float(row.get("gross_notional_usd")) or 0.0) * scale),
        "pnl_usd": rounded(pnl),
        "funding_usd": rounded((to_float(row.get("funding_usd")) or 0.0) * scale),
        "net_pct": rounded(to_float(row.get("net_pct"))),
        "mae_pct": rounded(to_float(row.get("mae_pct"))),
        "funding_prev_pct": rounded(to_float(row.get("funding_prev_pct"))),
        "peak_unrealized_loss_usd": rounded(peak_loss),
        "current_margin_topup_usd": rounded(current_margin_topup),
        "manual_topup_beyond_alloc_usd": rounded(manual_topup),
    }


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


def render_html_report(reports: list[dict[str, Any]]) -> str:
    generated = datetime.now(tz=timezone.utc).isoformat()
    summary_rows = [report["summary"] for report in reports]
    body = [
        "<!DOCTYPE html><html lang='ru'><head><meta charset='utf-8'/>",
        "<title>Pump-short compounding report</title>",
        "<style>",
        REPORT_CSS,
        "</style></head><body>",
        "<header><h1>Pump-short: dynamic capital report</h1>",
        f"<p>Start capital: {money(CAPITAL_USD)}. Sizing formula: capital at entry / max coins / 4 ladder steps * 3x. Generated: {esc(generated)}</p>",
        "<p class='note'>Temporary top-up is not added to strategy capital. It is modeled as rescue cash added only while the trade is open and removed after the trade closes.</p>",
        "</header>",
        "<section><h2>Strategy comparison</h2>",
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
                "<h3>Capital growth and temporary top-up</h3>",
                report["equity_svg"],
                "<h3>Active coins on the same timeline</h3>",
                report["active_svg"],
                "<h3>Trades that needed temporary top-up</h3>",
                render_topup_table(report["topups"]),
                "<h3>All actions with dynamic sizing</h3>",
                render_actions_table(report["actions"]),
                "</section>",
            ]
        )
    body.append("</body></html>")
    return "\n".join(body)


def render_summary_table(rows: list[dict[str, Any]]) -> str:
    columns = (
        ("title", "Strategy"),
        ("slots", "Coins"),
        ("funding_window_h", "Funding window"),
        ("funding_min_pct", "Funding min"),
        ("tp_pct", "TP"),
        ("trades_taken", "Trades"),
        ("final_capital_usd", "Final capital"),
        ("roi_on_initial_pct", "ROI"),
        ("roi_on_initial_plus_max_concurrent_topup_pct", "ROI incl max top-up"),
        ("max_concurrent_manual_topup_usd", "Max concurrent top-up"),
        ("avg_step_notional_usd", "Avg step"),
        ("max_step_notional_usd", "Max step"),
        ("idle_pct", "Idle"),
        ("full_capacity_pct", "Full capacity"),
    )
    return render_table(rows, columns)


def render_metric_grid(summary: dict[str, Any]) -> str:
    metrics = [
        ("Final capital", money(to_float(summary["final_capital_usd"]))),
        ("Net PnL", money(to_float(summary["net_pnl_usd"]))),
        ("ROI initial", pct_text(summary["roi_on_initial_pct"])),
        ("ROI incl max top-up", pct_text(summary["roi_on_initial_plus_max_concurrent_topup_pct"])),
        ("Avg / max step", f"{money(to_float(summary['avg_step_notional_usd']))} / {money(to_float(summary['max_step_notional_usd']))}"),
        ("Trades", str(summary["trades_taken"])),
        ("Win / TP-hit", f"{pct_text(summary['win_rate_pct'])} / {pct_text(summary['take_profit_rate_pct'])}"),
        ("Idle / active", f"{pct_text(summary['idle_pct'])} / {pct_text(summary['active_pct'])}"),
        ("Full capacity", f"{pct_text(summary['full_capacity_pct'])}, {summary['full_capacity_reached_count']} times"),
        ("Max single top-up", money(to_float(summary["max_single_manual_topup_usd"]))),
        ("Max concurrent top-up", money(to_float(summary["max_concurrent_manual_topup_usd"]))),
        ("Top-up turnover", money(to_float(summary["manual_topup_turnover_usd"]))),
    ]
    return "<div class='metrics'>" + "".join(f"<div><span>{esc(k)}</span><strong>{esc(v)}</strong></div>" for k, v in metrics) + "</div>"


def render_topup_table(rows: list[dict[str, Any]]) -> str:
    columns = (
        ("entry_iso", "Entry"),
        ("exit_iso", "Exit"),
        ("symbol", "Coin"),
        ("active_after_entry", "Active"),
        ("capital_before_entry_usd", "Capital before"),
        ("per_step_notional_usd", "Step notional"),
        ("legs_filled", "Legs"),
        ("pnl_usd", "PnL"),
        ("mae_pct", "MAE"),
        ("current_margin_topup_usd", "Margin top-up"),
        ("manual_topup_beyond_alloc_usd", "Temporary top-up"),
    )
    return render_table(rows, columns, empty="No temporary top-up events.")


def render_actions_table(rows: list[dict[str, Any]]) -> str:
    columns = (
        ("row", "#"),
        ("entry_iso", "Entry"),
        ("exit_iso", "Exit"),
        ("symbol", "Coin"),
        ("active_after_entry", "Active"),
        ("capital_before_entry_usd", "Capital before"),
        ("capital_after_exit_usd", "Capital after"),
        ("per_step_notional_usd", "Step"),
        ("exit_reason", "Exit"),
        ("legs_filled", "Legs"),
        ("gross_notional_usd", "Notional"),
        ("pnl_usd", "PnL"),
        ("net_pct", "Net %"),
        ("mae_pct", "MAE %"),
        ("manual_topup_beyond_alloc_usd", "Temporary top-up"),
    )
    return render_table(rows, columns)


def render_table(rows: list[dict[str, Any]], columns: tuple[tuple[str, str], ...], *, empty: str = "No rows.") -> str:
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
    if key.endswith("_usd") or key in {"final_capital_usd", "net_pnl_usd"}:
        return esc(money(to_float(value) or 0.0))
    if key.endswith("_pct") or key in {"funding_min_pct", "tp_pct"}:
        return esc(pct_text(value))
    if key.endswith("_iso"):
        return esc(short_iso(str(value)))
    if isinstance(value, float):
        return esc(f"{value:.2f}")
    return esc(value)


def percent(part: float, total: float) -> float | None:
    if not total:
        return None
    return part / total * 100.0


def mean(values: list[float]) -> float | None:
    vals = [value for value in values if math.isfinite(value)]
    return sum(vals) / len(vals) if vals else None


def rounded(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


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


__all__ = ["run_strategy_compound_report", "simulate_compound_strategy"]
