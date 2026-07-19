from __future__ import annotations

import csv
import html
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_features.pump_short_per_event_strategy_research import build_rule_configs
from analysis_features.pump_short_policy_portfolio_research import (
    DEFAULT_INPUT_DIR as DEFAULT_PER_EVENT_DIR,
    DEFAULT_OUTPUT_DIR as DEFAULT_POLICY_DIR,
    PolicySpec,
    PortfolioConfig,
    build_gates,
    build_unique_cases,
    load_csv,
    load_outcomes,
    max_concurrent_value,
    ms_to_iso,
    policy_from_summary,
    simulate_policy_portfolio,
    to_float,
    to_int,
    write_csv,
)
from config import BASE_DIR

DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_dynamic_combo_report_3000"
DEFAULT_OUTPUT_DIR_2024 = BASE_DIR / "data" / "research" / "pump_short_dynamic_combo_report_3000_2024"
CAPITAL_USD = 3_000.0
SLOT_RANGE = (1, 2, 3, 4)


def run_dynamic_combo_report(
    *,
    per_event_dir: Path = DEFAULT_PER_EVENT_DIR,
    policy_dir: Path = DEFAULT_POLICY_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    combo_limit: int = 10,
    start_ts_ms: int | None = None,
    slots: tuple[int, ...] = SLOT_RANGE,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)

    all_cases = build_unique_cases(load_csv(per_event_dir / "per_event_summary.csv"))
    cases = filter_cases_by_start(all_cases, start_ts_ms)
    gates = build_gates()
    outcomes = load_outcomes(per_event_dir / "per_event_all_outcomes.csv", {str(case["case_id"]) for case in cases})
    split_ts = train_test_split_ts(cases)
    selected_policies = select_top_combo_policies(policy_dir, combo_limit=combo_limit)

    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    topup_rows: list[dict[str, Any]] = []

    for combo_rank, policy in enumerate(selected_policies, start=1):
        for slot_count in slots:
            result = simulate_policy_portfolio(
                cases=cases,
                outcomes=outcomes,
                gates=gates,
                policy=policy,
                config=PortfolioConfig(capital_usd=CAPITAL_USD, slots=slot_count, sizing_mode="dynamic"),
                split_ts=split_ts,
                return_trades=True,
            )
            summary = {key: value for key, value in result.items() if key != "selected_trades"}
            summary["combo_rank"] = combo_rank
            summary_rows.append(summary)

            trades = result["selected_trades"]
            for trade in trades:
                row = dict(trade)
                row["combo_rank"] = combo_rank
                trade_rows.append(row)
            topup_rows.extend(topup_cashflows(combo_rank=combo_rank, summary=summary, trades=trades))

    summary_rows.sort(key=lambda row: (to_int(row.get("combo_rank")) or 0, to_int(row.get("slots")) or 0))
    write_csv(output_dir / "dynamic_combo_summary.csv", summary_rows)
    write_csv(output_dir / "dynamic_combo_trades.csv", trade_rows)
    write_csv(output_dir / "dynamic_combo_topup_cashflows.csv", topup_rows)

    (output_dir / "index.html").write_text(
        render_html_report(
            selected_policies=selected_policies,
            summary_rows=summary_rows,
            trade_rows=trade_rows,
            topup_rows=topup_rows,
            split_ts=split_ts,
            start_ts_ms=start_ts_ms,
            slots=slots,
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema": "pump_short_dynamic_combo_report_v1",
        "capital_usd": CAPITAL_USD,
        "slots": list(slots),
        "combo_limit": combo_limit,
        "selected_combos": len(selected_policies),
        "all_unique_cases": len(all_cases),
        "filtered_unique_cases": len(cases),
        "portfolio_runs": len(summary_rows),
        "trade_rows": len(trade_rows),
        "topup_cashflow_rows": len(topup_rows),
        "start_ts": start_ts_ms,
        "start_iso": ms_to_iso(start_ts_ms),
        "case_entry_min_iso": ms_to_iso(min((to_int(case.get("entry_ts")) or 0 for case in cases), default=0)),
        "case_entry_max_iso": ms_to_iso(max((to_int(case.get("entry_ts")) or 0 for case in cases), default=0)),
        "per_event_dir": str(per_event_dir),
        "policy_dir": str(policy_dir),
        "output_dir": str(output_dir),
        "split_ts": split_ts,
        "split_iso": ms_to_iso(split_ts),
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def filter_cases_by_start(cases: list[dict[str, Any]], start_ts_ms: int | None) -> list[dict[str, Any]]:
    if not start_ts_ms:
        return cases
    return [case for case in cases if (to_int(case.get("entry_ts")) or 0) >= start_ts_ms]


def parse_date_to_ms(value: str | None) -> int | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if len(text) == 10:
        text = text + "T00:00:00+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def parse_slots(value: str | None) -> tuple[int, ...]:
    if not value:
        return SLOT_RANGE
    slots: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        parsed = int(part)
        if parsed <= 0:
            raise ValueError(f"slot value must be positive: {part}")
        slots.append(parsed)
    if not slots:
        raise ValueError("at least one slot value is required")
    return tuple(dict.fromkeys(slots))


def select_top_combo_policies(policy_dir: Path, *, combo_limit: int) -> list[PolicySpec]:
    selected: list[PolicySpec] = []
    seen: set[str] = set()
    sources = [
        policy_dir / "strict_live_candidate_policy_summary.csv",
        policy_dir / "capped_live_candidate_policy_summary.csv",
    ]
    for source in sources:
        if not source.exists():
            continue
        for row in load_csv(source):
            slug = str(row.get("policy_slug") or "")
            if not slug or slug in seen:
                continue
            policy = policy_from_summary(row)
            selected.append(policy)
            seen.add(slug)
            if len(selected) >= combo_limit:
                return selected

    if len(selected) >= combo_limit:
        return selected

    rule_slugs = {rule.slug for rule in build_rule_configs()}
    for row in load_csv(policy_dir / "portfolio_policy_summary.csv"):
        slug = str(row.get("policy_slug") or "")
        rule = str(row.get("rule_slug") or "")
        if not slug or slug in seen or (rule and rule not in rule_slugs and rule != "SKIP"):
            continue
        policy = policy_from_summary(row)
        selected.append(policy)
        seen.add(slug)
        if len(selected) >= combo_limit:
            break
    return selected


def train_test_split_ts(cases: list[dict[str, Any]]) -> int:
    values = sorted(to_int(row.get("entry_ts")) or 0 for row in cases)
    return values[int(len(values) * 0.70)] if values else 0


def topup_cashflows(
    *,
    combo_rank: int,
    summary: dict[str, Any],
    trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for trade in trades:
        topup = to_float(trade.get("topup_usd")) or 0.0
        if topup <= 0:
            continue
        common = {
            "combo_rank": combo_rank,
            "policy_slug": summary.get("policy_slug"),
            "slots": summary.get("slots"),
            "sizing_mode": summary.get("sizing_mode"),
            "symbol": trade.get("symbol"),
            "case_id": trade.get("case_id"),
            "topup_usd": round(topup, 6),
        }
        events.append(
            {
                **common,
                "ts": trade.get("entry_ts"),
                "iso": trade.get("entry_iso"),
                "event": "add_external_topup",
                "cashflow_usd": round(topup, 6),
            }
        )
        events.append(
            {
                **common,
                "ts": trade.get("exit_ts"),
                "iso": trade.get("exit_iso"),
                "event": "release_external_topup",
                "cashflow_usd": round(-topup, 6),
            }
        )
    current = 0.0
    sorted_events = sorted(events, key=lambda row: (to_int(row.get("ts")) or 0, -(to_float(row.get("cashflow_usd")) or 0.0)))
    for event in sorted_events:
        current += to_float(event.get("cashflow_usd")) or 0.0
        event["external_topup_open_usd"] = round(current, 6)
    return sorted_events


def render_html_report(
    *,
    selected_policies: list[PolicySpec],
    summary_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    topup_rows: list[dict[str, Any]],
    split_ts: int,
    start_ts_ms: int | None = None,
    slots: tuple[int, ...] = SLOT_RANGE,
) -> str:
    sections = []
    sections.append(
        f"""
        <section class="panel">
          <h1>Dynamic $3000 Pump-Short Combo Report</h1>
          <p>Each selected policy is rerun as a pure dynamic portfolio from $3000 with slots {esc(','.join(str(slot) for slot in slots))}. Strategy capital compounds only closed PnL. Manual top-ups are temporary external cashflows and are not added to strategy capital.</p>
          <div class="metrics">
            <div><b>{len(selected_policies)}</b><span>selected combos</span></div>
            <div><b>{len(summary_rows)}</b><span>dynamic runs</span></div>
            <div><b>{len(trade_rows)}</b><span>selected trades</span></div>
            <div><b>{ms_to_iso(start_ts_ms) or 'full'}</b><span>start filter</span></div>
            <div><b>{ms_to_iso(split_ts)}</b><span>test starts</span></div>
          </div>
        </section>
        """
    )
    sections.append(f"<section class=\"panel\"><h2>Overall Read</h2>{overall_read(summary_rows, slots=slots)}</section>")
    sections.append(
        "<section class=\"panel\"><h2>All Dynamic Runs</h2>"
        + html_table(summary_rows, summary_columns())
        + "</section>"
    )

    for combo_rank, policy in enumerate(selected_policies, start=1):
        rows = [row for row in summary_rows if to_int(row.get("combo_rank")) == combo_rank]
        trades = [row for row in trade_rows if to_int(row.get("combo_rank")) == combo_rank]
        topups = [row for row in topup_rows if to_int(row.get("combo_rank")) == combo_rank]
        sections.append(
            f"""
            <section class="panel">
              <h2>Combo {combo_rank}: {esc(policy.slug)}</h2>
              <p>{esc(policy.description)}</p>
              {combo_read(rows)}
              <h3>Equity Curves - USD</h3>
              {combined_equity_svg(rows, trades)}
              <h3>Equity Curves - ROI %</h3>
              {combined_equity_svg(rows, trades, normalized=True)}
              <h3>Summary</h3>
              {html_table(rows, summary_columns())}
              <h3>Top-up Cashflows</h3>
              {html_table(topups[:80], ("slots","event","iso","symbol","topup_usd","cashflow_usd","external_topup_open_usd"))}
              <h3>Worst Trades</h3>
              {html_table(worst_trades(trades, limit=20), ("slots","symbol","entry_iso","exit_iso","rule_slug","net_pct","pnl_usd","topup_usd","pump_pct","oi24_pct","long_min","long_max","funding_prev_24h_pct"))}
            </section>
            """
        )
    return page_shell("Dynamic $3000 Pump-Short Combo Report", "\n".join(sections))


def overall_read(summary_rows: list[dict[str, Any]], *, slots: tuple[int, ...] = SLOT_RANGE) -> str:
    if not summary_rows:
        return "<p>No rows.</p>"
    best_by_slots = []
    for slot_count in slots:
        rows = [row for row in summary_rows if to_int(row.get("slots")) == slot_count]
        if not rows:
            continue
        best = sorted(rows, key=dynamic_rank_key)[0]
        best_by_slots.append(
            f"<li>slots {slot_count}: <b>{esc(best.get('policy_slug'))}</b>, final ${esc(best.get('final_capital_usd'))}, "
            f"risk-adjusted ROI {esc(best.get('risk_adjusted_roi_pct'))}%, max top-up ${esc(best.get('max_concurrent_topup_usd'))}, "
            f"worst trade ${esc(best.get('worst_trade_pnl_usd'))}</li>"
        )
    best_live = sorted(summary_rows, key=live_rank_key)[0]
    return (
        "<p>Ranking should not be read by final capital alone. Dynamic compounding magnifies both good and bad tails, so the practical read prioritizes survival, test split, bounded external top-up, and worst trade.</p>"
        f"<p>Best practical row in this run: <b>{esc(best_live.get('policy_slug'))}</b>, slots {esc(best_live.get('slots'))}, "
        f"final ${esc(best_live.get('final_capital_usd'))}, test risk-adjusted ROI {esc(best_live.get('test_risk_adjusted_roi_pct'))}%, "
        f"max external top-up ${esc(best_live.get('max_concurrent_topup_usd'))}, worst trade ${esc(best_live.get('worst_trade_pnl_usd'))}.</p>"
        "<ul>" + "".join(best_by_slots) + "</ul>"
    )


def combo_read(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    best = sorted(rows, key=live_rank_key)[0]
    return (
        f"<p>Best slot setting here: <b>{esc(best.get('slots'))}</b> slots, final ${esc(best.get('final_capital_usd'))}, "
        f"test risk-adjusted ROI {esc(best.get('test_risk_adjusted_roi_pct'))}%, max top-up ${esc(best.get('max_concurrent_topup_usd'))}, "
        f"worst trade ${esc(best.get('worst_trade_pnl_usd'))}.</p>"
    )


def dynamic_rank_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
    return (
        -(to_float(row.get("test_risk_adjusted_roi_pct")) or -10**9),
        to_float(row.get("max_concurrent_topup_usd")) or 0.0,
        -(to_float(row.get("risk_adjusted_roi_pct")) or -10**9),
        -(to_float(row.get("worst_trade_pnl_usd")) or 0.0),
    )


def live_rank_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
    capital = to_float(row.get("capital_usd")) or CAPITAL_USD
    topup = to_float(row.get("max_concurrent_topup_usd")) or 0.0
    worst = to_float(row.get("worst_trade_pnl_usd")) or 0.0
    topup_penalty = max(0.0, topup / capital - 2.0)
    worst_penalty = max(0.0, abs(min(0.0, worst)) / capital - 1.0)
    return (
        worst_penalty,
        topup_penalty,
        -(to_float(row.get("test_risk_adjusted_roi_pct")) or -10**9),
        topup,
        -worst,
    )


def worst_trades(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: to_float(row.get("pnl_usd")) or 0.0)[:limit]


def combined_equity_svg(
    summary_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    *,
    normalized: bool = False,
) -> str:
    if not trade_rows:
        return "<p>No trades.</p>"
    series: list[tuple[int, str, list[tuple[int, float]]]] = []
    all_points: list[tuple[int, float]] = []
    for summary in sorted(summary_rows, key=lambda row: to_int(row.get("slots")) or 0):
        slots = to_int(summary.get("slots")) or 0
        rows = [row for row in trade_rows if to_int(row.get("slots")) == slots]
        points = equity_points(rows, normalized=normalized)
        if not points:
            continue
        series.append((slots, f"{slots} slots", points))
        all_points.extend(points)
    if not all_points:
        return "<p>No chart points.</p>"
    min_ts = min(ts for ts, _ in all_points)
    max_ts = max(ts for ts, _ in all_points)
    min_eq = min(value for _, value in all_points)
    max_eq = max(value for _, value in all_points)
    width = 980
    height = 300
    pad = 36
    x_span = max(1, max_ts - min_ts)
    y_span = max(1.0, max_eq - min_eq)
    colors = ["#1f7a8c", "#be6c2c", "#4f6f52", "#7d4e8a"]
    polylines = []
    legend = []
    for idx, (slots, label, points) in enumerate(series):
        coords = []
        for ts, value in points:
            x = pad + (ts - min_ts) / x_span * (width - pad * 2)
            y = height - pad - (value - min_eq) / y_span * (height - pad * 2)
            coords.append(f"{x:.2f},{y:.2f}")
        color = colors[idx % len(colors)]
        polylines.append(f"<polyline points=\"{' '.join(coords)}\" fill=\"none\" stroke=\"{color}\" stroke-width=\"2.4\"/>")
        final_value = points[-1][1]
        value_label = f"{final_value:,.2f}%" if normalized else f"${final_value:,.2f}"
        legend.append(f"<span><i style=\"background:{color}\"></i>{esc(label)}: {esc(value_label)}</span>")
    start_value = 0.0 if normalized else CAPITAL_USD
    start_y = height - pad - (start_value - min_eq) / y_span * (height - pad * 2)
    range_label = f"ROI range {min_eq:,.2f}%..{max_eq:,.2f}%" if normalized else f"equity range ${min_eq:,.2f}..${max_eq:,.2f}"
    return (
        f"<div class=\"legend\">{''.join(legend)}</div>"
        f"<svg viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"equity curves\">"
        f"<line x1=\"{pad}\" y1=\"{start_y:.2f}\" x2=\"{width-pad}\" y2=\"{start_y:.2f}\" stroke=\"#9aa8b5\" stroke-dasharray=\"4 4\"/>"
        f"{''.join(polylines)}"
        f"<text x=\"{pad}\" y=\"22\" fill=\"#34495e\">{esc(range_label)}</text>"
        "</svg>"
    )


def equity_points(rows: list[dict[str, Any]], *, normalized: bool = False) -> list[tuple[int, float]]:
    ordered = sorted(rows, key=lambda row: to_int(row.get("exit_ts")) or to_int(row.get("entry_ts")) or 0)
    if not ordered:
        return []
    equity = CAPITAL_USD
    points = [(to_int(ordered[0].get("entry_ts")) or 0, normalized_value(equity) if normalized else equity)]
    for row in ordered:
        equity += to_float(row.get("pnl_usd")) or 0.0
        points.append((to_int(row.get("exit_ts")) or to_int(row.get("entry_ts")) or 0, normalized_value(equity) if normalized else equity))
    return points


def normalized_value(equity: float) -> float:
    return (equity - CAPITAL_USD) / CAPITAL_USD * 100.0


def summary_columns() -> tuple[str, ...]:
    return (
        "combo_rank",
        "policy_slug",
        "slots",
        "trades",
        "test_trades",
        "final_capital_usd",
        "risk_adjusted_roi_pct",
        "test_risk_adjusted_roi_pct",
        "max_concurrent_topup_usd",
        "test_max_concurrent_topup_usd",
        "worst_trade_pnl_usd",
        "test_worst_trade_pnl_usd",
        "win_rate_pct",
        "test_win_rate_pct",
        "skipped_slots",
        "skipped_same_symbol",
    )


def html_table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    head = "".join(f"<th>{esc(column)}</th>" for column in columns)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{esc(row.get(column, ''))}</td>" for column in columns) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def page_shell(title: str, content: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <style>
    body {{ margin: 0; background: #f4f6f8; color: #17202a; font: 14px/1.45 Arial, sans-serif; }}
    .wrap {{ max-width: 1360px; margin: 0 auto; padding: 22px; }}
    .panel {{ background: #fff; border: 1px solid #dfe5eb; border-radius: 8px; padding: 18px; margin: 0 0 16px; overflow-x: auto; }}
    h1 {{ margin: 0 0 10px; font-size: 26px; }}
    h2 {{ margin: 0 0 10px; font-size: 19px; }}
    h3 {{ margin: 16px 0 8px; font-size: 14px; color: #34495e; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin-top: 14px; }}
    .metrics div {{ border: 1px solid #e2e8ee; border-radius: 6px; padding: 10px; background: #fbfcfd; }}
    .metrics b {{ display: block; font-size: 20px; }}
    .metrics span {{ color: #607080; font-size: 12px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 7px 8px; border-bottom: 1px solid #e5eaf0; text-align: left; vertical-align: top; }}
    th {{ background: #f7f9fb; color: #34495e; position: sticky; top: 0; }}
    svg {{ width: 100%; height: auto; border: 1px solid #e5eaf0; border-radius: 6px; background: #fbfcfd; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 0 0 8px; font-size: 12px; }}
    .legend span {{ display: inline-flex; align-items: center; gap: 5px; }}
    .legend i {{ width: 12px; height: 3px; display: inline-block; }}
  </style>
</head>
<body><main class="wrap">{content}</main></body>
</html>"""


def write_csv_local(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    write_csv(path, rows)


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))
