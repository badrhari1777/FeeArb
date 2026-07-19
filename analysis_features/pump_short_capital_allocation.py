from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from analysis_features.pump_short_cross_exchange_research import ms_to_iso, to_float, write_csv
from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "pump_short_multiexchange_2024_clean" / "_comparison" / "outcomes.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_capital_allocation_3000"
DEFAULT_CAPITAL_USD = 3_000.0
DEFAULT_LEVERAGE = 3.0
DEFAULT_LADDER_LEGS = 4
DEFAULT_BASE_LEG_NOTIONAL_USD = 1_000.0
DEFAULT_STRATEGY = "pb20_wait168_fgm0p5_ladder4_step50_tp25_hold168"
DEFAULT_EXCHANGES: tuple[str, ...] = ("binance", "bybit")


@dataclass(frozen=True, slots=True)
class Trade:
    exchange: str
    symbol: str
    strategy: str
    trigger_ts: int
    entry_ts: int
    exit_ts: int
    pnl_usd_base: float
    net_pct: float
    funding_pct: float
    mae_pct: float
    legs_filled: int
    exit_reason: str
    win: int
    cat300: int


def run_capital_allocation_analysis(
    *,
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    capital_usd: float = DEFAULT_CAPITAL_USD,
    leverage: float = DEFAULT_LEVERAGE,
    max_slots: int = 10,
    strategy: str = DEFAULT_STRATEGY,
    exchanges: tuple[str, ...] = DEFAULT_EXCHANGES,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades = load_trades(input_path, strategy=strategy, exchanges=exchanges)
    summary_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    worst_topup_rows: list[dict[str, Any]] = []

    groups: dict[str, list[Trade]] = {exchange: [] for exchange in exchanges}
    groups["binance_bybit_pool"] = []
    for trade in trades:
        groups.setdefault(trade.exchange, []).append(trade)
        groups["binance_bybit_pool"].append(trade)

    for group_name, group_trades in groups.items():
        if not group_trades:
            continue
        for slots in range(1, max_slots + 1):
            result = simulate_slots(
                group_trades,
                group_name=group_name,
                slots=slots,
                capital_usd=capital_usd,
                leverage=leverage,
            )
            summary_rows.append(result["summary"])
            selected_rows.extend(result["selected"])
            worst_topup_rows.extend(result["worst_topups"])

    write_csv(output_dir / "capital_slot_summary.csv", summary_rows)
    write_csv(output_dir / "selected_trades.csv", selected_rows)
    worst_topup_rows.sort(key=lambda row: to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0, reverse=True)
    write_csv(output_dir / "worst_manual_topups.csv", worst_topup_rows[:200])
    metadata = {
        "schema": "pump_short_capital_allocation_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "capital_usd": capital_usd,
        "leverage": leverage,
        "max_slots": max_slots,
        "strategy": strategy,
        "exchanges": list(exchanges),
        "trades_loaded": len(trades),
        "summary_rows": len(summary_rows),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report(output_dir, metadata, summary_rows)
    return metadata


def load_trades(input_path: Path, *, strategy: str, exchanges: tuple[str, ...]) -> list[Trade]:
    exchange_set = set(exchanges)
    trades: list[Trade] = []
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("strategy") != strategy:
                continue
            exchange = str(row.get("exchange") or "")
            if exchange not in exchange_set:
                continue
            entry_ts = to_int(row.get("entry_ts"))
            exit_ts = to_int(row.get("exit_ts"))
            trigger_ts = to_int(row.get("trigger_ts"))
            if entry_ts is None or exit_ts is None or trigger_ts is None:
                continue
            trades.append(
                Trade(
                    exchange=exchange,
                    symbol=str(row.get("symbol") or ""),
                    strategy=strategy,
                    trigger_ts=trigger_ts,
                    entry_ts=entry_ts,
                    exit_ts=exit_ts,
                    pnl_usd_base=to_float(row.get("pnl_usd")) or 0.0,
                    net_pct=to_float(row.get("net_pct")) or 0.0,
                    funding_pct=to_float(row.get("funding_during_pct")) or 0.0,
                    mae_pct=to_float(row.get("mae_pct")) or 0.0,
                    legs_filled=int(to_float(row.get("legs_filled")) or 0),
                    exit_reason=str(row.get("exit_reason") or ""),
                    win=int(to_float(row.get("win")) or 0),
                    cat300=int(to_float(row.get("cat300")) or 0),
                )
            )
    trades.sort(key=lambda trade: (trade.entry_ts, trade.exchange, trade.symbol))
    return trades


def simulate_slots(
    trades: list[Trade],
    *,
    group_name: str,
    slots: int,
    capital_usd: float,
    leverage: float,
) -> dict[str, Any]:
    per_coin_capital = capital_usd / slots
    per_step_margin = per_coin_capital / DEFAULT_LADDER_LEGS
    per_step_notional = per_step_margin * leverage
    scale = per_step_notional / DEFAULT_BASE_LEG_NOTIONAL_USD
    active: list[tuple[int, str]] = []
    selected: list[dict[str, Any]] = []
    skipped_slots = 0
    skipped_same_symbol = 0
    pnl = 0.0
    wins = 0
    tp_hits = 0
    cat300 = 0
    manual_topup_sum = 0.0
    manual_topup_events = 0
    max_single_topup = 0.0
    max_current_margin_topup = 0.0
    max_active = 0
    total_hold_h = 0.0
    total_funding_usd = 0.0
    total_scaled_notional = 0.0
    topup_rows: list[dict[str, Any]] = []

    for trade in trades:
        active = [(exit_ts, key) for exit_ts, key in active if exit_ts > trade.entry_ts]
        active_symbols = {key for _, key in active}
        key = symbol_key(trade, group_name)
        if key in active_symbols:
            skipped_same_symbol += 1
            continue
        if len(active) >= slots:
            skipped_slots += 1
            continue
        active.append((trade.exit_ts, key))
        max_active = max(max_active, len(active))

        scaled_pnl = trade.pnl_usd_base * scale
        gross_notional = trade.legs_filled * per_step_notional
        posted_initial_margin = gross_notional / leverage if leverage > 0 else 0.0
        peak_unrealized_loss = max(0.0, trade.mae_pct / 100.0 * gross_notional)
        current_margin_topup = max(0.0, peak_unrealized_loss - posted_initial_margin)
        manual_topup = max(0.0, peak_unrealized_loss - per_coin_capital)
        funding_usd = gross_notional * trade.funding_pct / 100.0
        pnl += scaled_pnl
        wins += 1 if scaled_pnl > 0 else 0
        tp_hits += 1 if trade.exit_reason == "take_profit" else 0
        cat300 += trade.cat300
        total_hold_h += max(0.0, (trade.exit_ts - trade.entry_ts) / 3_600_000.0)
        total_funding_usd += funding_usd
        total_scaled_notional += gross_notional
        max_current_margin_topup = max(max_current_margin_topup, current_margin_topup)
        max_single_topup = max(max_single_topup, manual_topup)
        if manual_topup > 0:
            manual_topup_events += 1
            manual_topup_sum += manual_topup

        row = {
            "group": group_name,
            "slots": slots,
            "exchange": trade.exchange,
            "symbol": trade.symbol,
            "entry_ts": trade.entry_ts,
            "entry_iso": ms_to_iso(trade.entry_ts),
            "exit_ts": trade.exit_ts,
            "exit_iso": ms_to_iso(trade.exit_ts),
            "exit_reason": trade.exit_reason,
            "legs_filled": trade.legs_filled,
            "per_step_margin_usd": round(per_step_margin, 6),
            "per_step_notional_usd": round(per_step_notional, 6),
            "gross_notional_usd": round(gross_notional, 6),
            "pnl_usd": round(scaled_pnl, 6),
            "funding_usd": round(funding_usd, 6),
            "net_pct": round(trade.net_pct, 6),
            "mae_pct": round(trade.mae_pct, 6),
            "peak_unrealized_loss_usd": round(peak_unrealized_loss, 6),
            "current_margin_topup_usd": round(current_margin_topup, 6),
            "manual_topup_beyond_alloc_usd": round(manual_topup, 6),
        }
        selected.append(row)
        if manual_topup > 0:
            topup_rows.append(row)

    taken = len(selected)
    final_capital = capital_usd + pnl
    capital_plus_topups = capital_usd + manual_topup_sum
    summary = {
        "group": group_name,
        "slots": slots,
        "initial_capital_usd": capital_usd,
        "per_coin_capital_usd": round(per_coin_capital, 6),
        "per_step_margin_usd": round(per_step_margin, 6),
        "per_step_notional_usd": round(per_step_notional, 6),
        "max_planned_notional_per_coin_usd": round(per_step_notional * DEFAULT_LADDER_LEGS, 6),
        "trades_available": len(trades),
        "trades_taken": taken,
        "trades_skipped_slots": skipped_slots,
        "trades_skipped_same_symbol": skipped_same_symbol,
        "max_active_seen": max_active,
        "win_rate_pct": pct(wins, taken),
        "take_profit_rate_pct": pct(tp_hits, taken),
        "cat300_rate_pct": pct(cat300, taken),
        "net_pnl_usd": round(pnl, 6),
        "final_capital_usd": round(final_capital, 6),
        "roi_on_initial_pct": pct(pnl, capital_usd),
        "manual_topup_sum_usd": round(manual_topup_sum, 6),
        "manual_topup_events": manual_topup_events,
        "max_single_manual_topup_usd": round(max_single_topup, 6),
        "max_current_margin_topup_usd": round(max_current_margin_topup, 6),
        "roi_on_initial_plus_topups_pct": pct(pnl, capital_plus_topups),
        "avg_pnl_per_taken_trade_usd": round(pnl / taken, 6) if taken else None,
        "avg_hold_h": round(total_hold_h / taken, 6) if taken else None,
        "funding_pnl_usd": round(total_funding_usd, 6),
        "avg_gross_notional_usd": round(total_scaled_notional / taken, 6) if taken else None,
    }
    topup_rows.sort(key=lambda row: to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0, reverse=True)
    return {"summary": summary, "selected": selected, "worst_topups": topup_rows[:25]}


def symbol_key(trade: Trade, group_name: str) -> str:
    if group_name == "binance_bybit_pool":
        return trade.symbol
    return f"{trade.exchange}:{trade.symbol}"


def write_report(output_dir: Path, metadata: dict[str, Any], summary_rows: list[dict[str, Any]]) -> None:
    lines = [
        f"# Pump Short Capital Allocation: ${format_number(metadata['capital_usd'])}",
        "",
        "Strategy: `pb20`, funding previous 24h `> -0.50%`, 4 ladder legs, 50% adverse spacing, TP25 or 168h time stop.",
        "",
        f"- Input: `{metadata['input_path']}`",
        f"- Trades loaded: `{metadata['trades_loaded']}`",
        f"- Capital: `${metadata['capital_usd']}`",
        f"- Leverage: `{metadata['leverage']}x`",
        "",
        "Interpretation: `slots` is the maximum number of different coins held at the same time. Capital per coin is split into 4 equal margin buckets, one per ladder step. Per-step notional is `per_step_margin * 3x`.",
        "",
    ]
    for group in ("bybit", "binance", "binance_bybit_pool"):
        rows = [row for row in summary_rows if row.get("group") == group]
        if not rows:
            continue
        lines.extend([f"## {group}", ""])
        lines.extend(
            markdown_table(
                rows,
                (
                    "slots",
                    "per_step_margin_usd",
                    "per_step_notional_usd",
                    "trades_taken",
                    "trades_skipped_slots",
                    "win_rate_pct",
                    "net_pnl_usd",
                    "roi_on_initial_pct",
                    "manual_topup_sum_usd",
                    "max_single_manual_topup_usd",
                    "roi_on_initial_plus_topups_pct",
                ),
            )
        )
        best = max(rows, key=lambda row: to_float(row.get("roi_on_initial_plus_topups_pct")) or -999999.0)
        lines.extend(
            [
                "",
                (
                    "Best by ROI after manual topups: "
                    f"`{best['slots']}` slots, final capital `${format_number(best['final_capital_usd'])}`, "
                    f"ROI initial `{format_number(best['roi_on_initial_pct'])}%`, "
                    f"topups `${format_number(best['manual_topup_sum_usd'])}`."
                ),
                "",
            ]
        )
    (output_dir / "capital_allocation_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def markdown_table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> list[str]:
    if not rows:
        return ["_No rows._"]
    out = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(format_cell(row.get(column)) for column in columns) + " |")
    return out


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return format_number(value)
    return str(value)


def format_number(value: Any) -> str:
    number = to_float(value)
    if number is None:
        return ""
    return f"{number:.2f}"


def pct(part: float, total: float) -> float | None:
    if not total:
        return None
    return round(part / total * 100.0, 6)


def to_int(value: Any) -> int | None:
    try:
        out = int(float(value))
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


__all__ = ["run_capital_allocation_analysis"]
