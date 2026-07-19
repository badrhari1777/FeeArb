from __future__ import annotations

import csv
import html
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analysis_collectors.bybit_event_window import build_event_window_summary
from analysis_collectors.bybit_pump_short import round_float, to_float, to_int
from config import BASE_DIR

DEFAULT_WINDOWS_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_event_windows_5m_candidates" / "event_windows.jsonl"
DEFAULT_OUTCOMES_INPUT = BASE_DIR / "data" / "research" / "pump_funding_premium_window_research_5m_candidates" / "premium_long_outcomes.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_premium_event_pages"
TARGET_ENTRY_RULE = "deep_discount_survives"
TARGET_EXIT_PLAN = "tp30_sl25_hold72_fundrelief"


def build_premium_event_pages(
    *,
    windows_input: Path = DEFAULT_WINDOWS_INPUT,
    outcomes_input: Path = DEFAULT_OUTCOMES_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    events_dir = output_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    windows = read_jsonl(windows_input)
    outcomes = read_csv(outcomes_input)
    outcomes_by_event: dict[str, list[dict[str, Any]]] = {}
    for row in outcomes:
        outcomes_by_event.setdefault(str(row.get("event_id") or ""), []).append(row)

    index_rows: list[dict[str, Any]] = []
    for sample in windows:
        event = sample.get("event") if isinstance(sample.get("event"), dict) else {}
        event_id = str(event.get("event_id") or sample.get("event_id") or "")
        event_outcomes = outcomes_by_event.get(event_id, [])
        summary = build_event_window_summary(sample)
        target = pick_target_outcome(event_outcomes)
        best = pick_best_outcome(event_outcomes)
        diagnosis = diagnose_event(summary, target, best)
        page_slug = safe_slug(f"{sample.get('symbol')}_{sample.get('trigger_ts')}")
        page_path = events_dir / f"{page_slug}.html"
        page_path.write_text(
            render_event_page(
                sample=sample,
                summary=summary,
                target=target,
                best=best,
                outcomes=event_outcomes,
                diagnosis=diagnosis,
            ),
            encoding="utf-8",
        )
        index_rows.append(
            {
                "symbol": sample.get("symbol"),
                "trigger_iso": sample.get("trigger_iso"),
                "trigger_pump_pct": summary.get("trigger_pump_pct"),
                "target_net_pct": target.get("net_pct"),
                "target_exit_reason": target.get("exit_reason"),
                "target_entry_wait_h": target.get("entry_wait_h"),
                "target_entry_premium_pct": target.get("entry_premium_pct"),
                "target_entry_oi_change_4h_pct": target.get("entry_oi_change_4h_pct"),
                "best_net_pct": best.get("net_pct"),
                "best_rule": f"{best.get('entry_rule', '')}/{best.get('exit_plan', '')}".strip("/"),
                "diagnosis": diagnosis,
                "page": f"events/{page_path.name}",
            }
        )

    index_rows.sort(key=lambda row: (to_float(row.get("target_net_pct")) or -10**9), reverse=True)
    write_csv(output_dir / "event_page_summary.csv", index_rows)
    (output_dir / "index.html").write_text(render_index(index_rows), encoding="utf-8")
    metadata = {
        "schema": "pump_premium_event_pages_v1",
        "windows_input": str(windows_input),
        "outcomes_input": str(outcomes_input),
        "output_dir": str(output_dir),
        "events": len(index_rows),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def pick_target_outcome(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        if row.get("entry_rule") == TARGET_ENTRY_RULE and row.get("exit_plan") == TARGET_EXIT_PLAN:
            return row
    return {}


def pick_best_outcome(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    return max(rows, key=lambda row: to_float(row.get("net_pct")) or -10**9)


def diagnose_event(summary: dict[str, Any], target: dict[str, Any], best: dict[str, Any]) -> str:
    net = to_float(target.get("net_pct"))
    if net is None:
        return "no_target_entry"
    premium = to_float(target.get("entry_premium_pct"))
    oi_change = to_float(target.get("entry_oi_change_4h_pct"))
    wait_h = to_float(target.get("entry_wait_h"))
    if net > 0 and (target.get("exit_reason") == "take_profit"):
        return "clean_discount_squeeze_tp"
    if net > 0:
        return "funding_or_partial_recovery_win"
    if wait_h is not None and wait_h > 3:
        return "late_entry_decay"
    if premium is not None and premium < -5:
        return "toxic_premium_trap"
    if oi_change is not None and oi_change < 0:
        return "oi_not_confirming"
    best_net = to_float(best.get("net_pct"))
    if best_net is not None and best_net > 0:
        return "exit_shape_mismatch"
    return "failed_discount_absorption"


def render_event_page(
    *,
    sample: dict[str, Any],
    summary: dict[str, Any],
    target: dict[str, Any],
    best: dict[str, Any],
    outcomes: list[dict[str, Any]],
    diagnosis: str,
) -> str:
    checkpoints = build_checkpoints(sample, target)
    outcome_rows = sorted(outcomes, key=lambda row: to_float(row.get("net_pct")) or -10**9, reverse=True)
    body = f"""
    <h1>{esc(sample.get('symbol'))} premium/funding event</h1>
    <p><a href="../index.html">Back to index</a></p>
    <section><h2>Diagnosis</h2><p><strong>{esc(diagnosis)}</strong></p></section>
    <section><h2>Event Summary</h2>{html_table([summary])}</section>
    <section><h2>Target Outcome</h2>{html_table([target] if target else [])}</section>
    <section><h2>Best Outcome</h2>{html_table([best] if best else [])}</section>
    <section><h2>Signal Checkpoints</h2>{html_table(checkpoints)}</section>
    <section><h2>All Outcomes</h2>{html_table(outcome_rows)}</section>
    """
    return page_shell(f"{sample.get('symbol')} premium event", body)


def render_index(rows: list[dict[str, Any]]) -> str:
    linked_rows = []
    for row in rows:
        linked = dict(row)
        page = str(row.get("page") or "")
        linked["page"] = f'<a href="{esc_attr(page)}">{esc(page)}</a>' if page else ""
        linked_rows.append(linked)
    body = f"""
    <h1>Premium/Funding Candidate Event Pages</h1>
    <p>Per-event review pages for the filtered 5m Bybit premium/funding long candidates.</p>
    <section><h2>Events</h2>{html_table(linked_rows, raw_columns={'page'})}</section>
    """
    return page_shell("Premium/Funding Candidate Event Pages", body)


def build_checkpoints(sample: dict[str, Any], target: dict[str, Any]) -> list[dict[str, Any]]:
    interval, series = primary_interval(sample)
    klines = series.get("klines") or []
    premium = series.get("premium_index_klines") or []
    oi = series.get("open_interest") or []
    trigger_ts = to_int(sample.get("trigger_ts")) or 0
    entry_ts = to_int(target.get("entry_ts")) if target else None
    exit_ts = to_int(target.get("exit_ts")) if target else None
    timestamps = [("trigger", trigger_ts)]
    if entry_ts:
        timestamps.append(("target_entry", entry_ts))
    if exit_ts:
        timestamps.append(("target_exit", exit_ts))
    out = []
    for label, ts_ms in timestamps:
        close = value_at_or_before(klines, ts_ms, "close")
        out.append(
            {
                "label": label,
                "ts_iso": ms_to_iso(ts_ms),
                "price": round_float(close),
                "premium_pct": round_float((value_at_or_before(premium, ts_ms, "close") or 0.0) * 100.0),
                "oi": round_float(value_at_or_before(oi, ts_ms, "open_interest")),
                "volume": round_float(value_at_or_before(klines, ts_ms, "volume")),
                "interval": interval,
            }
        )
    return out


def primary_interval(sample: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    intervals = sample.get("intervals") if isinstance(sample.get("intervals"), dict) else {}
    if not intervals:
        return "", {}
    key = sorted(intervals.keys(), key=lambda item: int(str(item).replace("min", "").replace("m", "")))[0]
    return str(key), intervals[key]


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


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                out.append(json.loads(line))
    return out


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def html_table(rows: list[dict[str, Any]], *, raw_columns: set[str] | None = None) -> str:
    if not rows:
        return "<p>No rows.</p>"
    raw_columns = raw_columns or set()
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    head = "".join(f"<th>{esc(column)}</th>" for column in columns)
    body = ""
    for row in rows:
        cells = []
        for column in columns:
            value = "" if row.get(column) is None else str(row.get(column))
            cells.append(f"<td>{value if column in raw_columns else esc(value)}</td>")
        body += "<tr>" + "".join(cells) + "</tr>"
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def page_shell(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{esc(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2933; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 12px; margin-top: 8px; }}
    th, td {{ border: 1px solid #d7dde5; padding: 4px 6px; text-align: left; vertical-align: top; }}
    th {{ background: #eef2f7; position: sticky; top: 0; }}
    section {{ margin: 24px 0; }}
  </style>
</head>
<body>{body}</body>
</html>"""


def safe_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)


def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def esc_attr(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)


def ms_to_iso(ts_ms: int | None) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


__all__ = ["build_premium_event_pages", "diagnose_event"]
