from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BASE_DIR

DEFAULT_RESEARCH_ROOT = BASE_DIR / "data" / "research" / "pump_short_multiexchange_2024_clean"
DEFAULT_COMPARISON_DIR = DEFAULT_RESEARCH_ROOT / "_comparison"


def build_exchange_decision_report(
    *,
    research_root: Path = DEFAULT_RESEARCH_ROOT,
    comparison_dir: Path = DEFAULT_COMPARISON_DIR,
) -> dict[str, Any]:
    coverage = read_csv(comparison_dir / "coverage.csv")
    capital = read_csv(comparison_dir / "capital_10000_summary.csv")
    rules = read_csv(comparison_dir / "rule_summary.csv")

    coverage_summary = summarize_coverage(coverage)
    best_capital = best_by_exchange(capital, "final_capital_usd")
    best_raw = best_by_exchange(rules, "sum_pnl_usd")
    best_lowish = best_lowish_rules(rules)
    listing = build_fixed_listing(research_root, coverage)
    symbol_overlap = summarize_symbol_overlap(coverage, listing)

    report = render_report(
        coverage_summary=coverage_summary,
        best_capital=best_capital,
        best_raw=best_raw,
        best_lowish=best_lowish,
        symbol_overlap=symbol_overlap,
    )
    out_path = comparison_dir / "exchange_decision_report.md"
    out_path.write_text(report, encoding="utf-8")

    fixed_listing_path = comparison_dir / "listing_summary_fixed.csv"
    write_csv(fixed_listing_path, listing)

    metadata = {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "report": str(out_path),
        "fixed_listing": str(fixed_listing_path),
        "exchanges": sorted(coverage_summary),
    }
    (comparison_dir / "exchange_decision_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def summarize_coverage(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        exchange = row.get("exchange") or ""
        current = out.setdefault(exchange, {"symbols": 0, "pump_symbols": 0, "events": 0, "funding_points": 0})
        current["symbols"] += 1
        events = to_int(row.get("pump_events")) or 0
        current["events"] += events
        current["funding_points"] += to_int(row.get("funding_points")) or 0
        if events > 0:
            current["pump_symbols"] += 1
    return out


def best_by_exchange(rows: list[dict[str, str]], key: str) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        exchange = row.get("exchange") or ""
        if not exchange:
            continue
        value = to_float(row.get(key))
        if value is None:
            continue
        if exchange not in out or value > (to_float(out[exchange].get(key)) or float("-inf")):
            out[exchange] = row
    return out


def best_lowish_rules(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for row in rows:
        n = to_int(row.get("n")) or 0
        p90 = to_float(row.get("p90_mae_pct")) or 999.0
        cat300 = to_float(row.get("cat300_pct")) or 999.0
        avg = to_float(row.get("avg_net_pct")) or -999.0
        win = to_float(row.get("win_rate_pct")) or 0.0
        if n >= 30 and p90 <= 60.0 and cat300 <= 0.5 and avg > 0.0 and win >= 60.0:
            candidates.append(row)
    return best_by_exchange(candidates, "sum_pnl_usd")


def build_fixed_listing(research_root: Path, coverage: list[dict[str, str]]) -> list[dict[str, Any]]:
    listed_by_exchange: dict[str, set[str]] = {}
    for path in sorted(research_root.glob("*/instruments_latest.json")):
        if path.parent.name.startswith("_"):
            continue
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
        listed_by_exchange[exchange] = {item for item in symbols if item}

    pump_by_exchange: dict[str, set[str]] = {}
    for row in coverage:
        events = to_int(row.get("pump_events")) or 0
        if events > 0:
            pump_by_exchange.setdefault(str(row.get("exchange") or ""), set()).add(str(row.get("symbol") or ""))

    exchanges = sorted(listed_by_exchange)
    all_symbols = sorted(set().union(*listed_by_exchange.values()) if listed_by_exchange else set())
    rows: list[dict[str, Any]] = []
    for symbol in all_symbols:
        listed = [exchange for exchange in exchanges if symbol in listed_by_exchange.get(exchange, set())]
        pumped = [exchange for exchange in exchanges if symbol in pump_by_exchange.get(exchange, set())]
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


def summarize_symbol_overlap(
    coverage: list[dict[str, str]],
    listing: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    binance_listed = {row["symbol"] for row in listing if row.get("listed_on_binance") is True}
    binance_pumped = {
        str(row.get("symbol") or "")
        for row in coverage
        if row.get("exchange") == "binance" and (to_int(row.get("pump_events")) or 0) > 0
    }
    pump_by_exchange: dict[str, set[str]] = {}
    for row in coverage:
        events = to_int(row.get("pump_events")) or 0
        if events > 0:
            pump_by_exchange.setdefault(str(row.get("exchange") or ""), set()).add(str(row.get("symbol") or ""))
    out: dict[str, dict[str, Any]] = {}
    for exchange, symbols in pump_by_exchange.items():
        listed_on_binance = symbols & binance_listed
        also_pumped_on_binance = symbols & binance_pumped
        out[exchange] = {
            "pump_symbols": len(symbols),
            "listed_on_binance": len(listed_on_binance),
            "listed_on_binance_pct": pct(len(listed_on_binance), len(symbols)),
            "also_pumped_on_binance": len(also_pumped_on_binance),
            "also_pumped_on_binance_pct": pct(len(also_pumped_on_binance), len(symbols)),
            "not_pumped_on_binance_examples": ",".join(sorted(symbols - binance_pumped)[:20]),
        }
    return out


def render_report(
    *,
    coverage_summary: dict[str, dict[str, Any]],
    best_capital: dict[str, dict[str, str]],
    best_raw: dict[str, dict[str, str]],
    best_lowish: dict[str, dict[str, str]],
    symbol_overlap: dict[str, dict[str, Any]],
) -> str:
    lines = [
        "# Pump-short exchange decision report",
        "",
        f"Generated: {datetime.now(tz=timezone.utc).isoformat()}",
        "",
        "## Exchange Coverage",
        "",
        "| Exchange | Sample symbols | Pump symbols | Events |",
        "|---|---:|---:|---:|",
    ]
    for exchange, row in sorted(coverage_summary.items(), key=lambda item: -int(item[1]["events"])):
        lines.append(f"| {exchange} | {row['symbols']} | {row['pump_symbols']} | {row['events']} |")

    lines.extend(["", "## Best $10k Capital Simulation", "", table(best_capital, (
        "final_capital_usd",
        "roi_pct",
        "trades_taken",
        "trades_skipped_capital",
        "peak_reserved_usd",
        "strategy",
    ))])
    lines.extend(["", "## Best Raw Rule Per Exchange", "", table(best_raw, (
        "n",
        "win_rate_pct",
        "avg_net_pct",
        "median_net_pct",
        "p90_mae_pct",
        "cat300_pct",
        "sum_pnl_usd",
        "strategy",
    ))])
    lines.extend(["", "## Best Lower-Stress Rule Per Exchange", "", table(best_lowish, (
        "n",
        "win_rate_pct",
        "avg_net_pct",
        "median_net_pct",
        "p90_mae_pct",
        "cat300_pct",
        "sum_pnl_usd",
        "strategy",
    ))])
    lines.extend(["", "## Binance Overlap", "", "| Exchange | Pump symbols | Listed on Binance | Also pumped on Binance | Not-pumped-on-Binance examples |", "|---|---:|---:|---:|---|"])
    for exchange, row in sorted(symbol_overlap.items()):
        lines.append(
            f"| {exchange} | {row['pump_symbols']} | "
            f"{row['listed_on_binance']} ({format_float(row['listed_on_binance_pct'])}%) | "
            f"{row['also_pumped_on_binance']} ({format_float(row['also_pumped_on_binance_pct'])}%) | "
            f"{row['not_pumped_on_binance_examples']} |"
        )

    lines.extend(
        [
            "",
            "## Read",
            "",
            "- By event count, Binance and Bybit are the primary venues; OKX and MEXC add extra coverage, while Bitget had too little material in this run.",
            "- The $10k capital result is theoretical and compounds realized PnL; peak reserved capital can exceed the initial $10k after profitable trades.",
            "- Cross-exchange layer uses price + funding only because OI/long-short history is not consistently available across exchanges.",
            "- Use this report to choose candidate exchange profiles, then rerun exchange-specific analyzers with OI/long-ratio where available.",
        ]
    )
    return "\n".join(lines) + "\n"


def table(rows_by_exchange: dict[str, dict[str, str]], columns: tuple[str, ...]) -> str:
    if not rows_by_exchange:
        return "_No rows._"
    lines = ["| Exchange | " + " | ".join(columns) + " |", "|---|" + "|".join("---" for _ in columns) + "|"]
    for exchange, row in sorted(rows_by_exchange.items(), key=lambda item: -(to_float(item[1].get(columns[0])) or 0.0)):
        lines.append("| " + exchange + " | " + " | ".join(format_cell(row.get(col)) for col in columns) + " |")
    return "\n".join(lines)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def normalize_symbol(value: Any) -> str:
    text = str(value or "").upper().strip()
    if text.endswith("-USDT-SWAP"):
        return text.replace("-USDT-SWAP", "USDT").replace("-", "")
    if "/USDT" in text:
        return text.split("/USDT", 1)[0].replace("/", "").replace("-", "").replace("_", "") + "USDT"
    for part in ("/", ":", "-", "_"):
        text = text.replace(part, "")
    return text


def to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def pct(part: int, total: int) -> float | None:
    return part / total * 100.0 if total else None


def format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.1f}"


def format_cell(value: Any) -> str:
    num = to_float(value)
    if num is not None and str(value).strip() != "":
        return f"{num:.2f}"
    return str(value or "")


__all__ = ["build_exchange_decision_report"]
