from __future__ import annotations

import csv
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_features.pump_short_cross_exchange_research import (
    detect_pump_events,
    find_pullback_entry,
    ladder_entries,
    ms_to_iso,
    parse_series,
    quantile,
    to_float,
    write_csv,
)
from config import BASE_DIR

DEFAULT_INPUT_ROOT = BASE_DIR / "data" / "research" / "pump_short_multiexchange_2024_clean"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_short_bybit_funding_tp_capital_grid"
DEFAULT_EXCHANGE = "bybit"
DEFAULT_LEVERAGE = 3.0
DEFAULT_LADDER_LEGS = 4
DEFAULT_LADDER_STEP_PCT = 50.0
DEFAULT_BASE_LEG_NOTIONAL_USD = 1_000.0
DEFAULT_PULLBACK_PCT = 20.0
DEFAULT_MAX_WAIT_H = 168
DEFAULT_MAX_HOLD_H = 168
DEFAULT_FUNDING_WINDOWS_H: tuple[int, ...] = tuple(range(24, 2, -1))
DEFAULT_FUNDING_MIN_PCTS: tuple[float, ...] = (-1.0, -0.9, -0.8, -0.7, -0.6, -0.5)
DEFAULT_TP_PCTS: tuple[float, ...] = (25.0, 30.0, 35.0, 45.0)
DEFAULT_CAPITALS_USD: tuple[float, ...] = (1_000.0, 3_000.0)
DEFAULT_MAX_SLOTS = 4
FEE_ROUNDTRIP_PCT = 0.18


@dataclass(frozen=True, slots=True)
class ExitPlan:
    tp_pct: float
    max_hold_h: int = DEFAULT_MAX_HOLD_H


@dataclass(frozen=True, slots=True)
class Outcome:
    symbol: str
    trigger_ts: int
    entry_ts: int
    exit_ts: int
    funding_window_h: int
    funding_min_pct: float
    funding_prev_pct: float
    funding_points: int
    tp_pct: float
    pnl_usd_base: float
    net_pct: float
    funding_during_pct: float
    mae_pct: float
    legs_filled: int
    exit_reason: str
    hold_h: float
    win: int
    cat300: int

    @property
    def strategy(self) -> str:
        return strategy_name(
            funding_window_h=self.funding_window_h,
            funding_min_pct=self.funding_min_pct,
            tp_pct=self.tp_pct,
        )


def run_bybit_funding_tp_capital_grid(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    exchange: str = DEFAULT_EXCHANGE,
    funding_windows_h: tuple[int, ...] = DEFAULT_FUNDING_WINDOWS_H,
    funding_min_pcts: tuple[float, ...] = DEFAULT_FUNDING_MIN_PCTS,
    tp_pcts: tuple[float, ...] = DEFAULT_TP_PCTS,
    capitals_usd: tuple[float, ...] = DEFAULT_CAPITALS_USD,
    max_slots: int = DEFAULT_MAX_SLOTS,
    leverage: float = DEFAULT_LEVERAGE,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    outcomes = build_outcomes(
        input_root=input_root,
        exchange=exchange,
        funding_windows_h=funding_windows_h,
        funding_min_pcts=funding_min_pcts,
        tp_pcts=tp_pcts,
    )
    outcome_rows = [outcome_to_row(outcome) for outcome in outcomes]
    write_csv(output_dir / "outcomes.csv", outcome_rows)

    raw_summary = build_raw_summary(outcomes)
    write_csv(output_dir / "raw_strategy_summary.csv", raw_summary)

    slot_summary: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    worst_topups: list[dict[str, Any]] = []
    for capital_usd in capitals_usd:
        for slots in range(1, max_slots + 1):
            for group in group_outcomes(outcomes).values():
                result = simulate_slots(
                    group,
                    capital_usd=capital_usd,
                    slots=slots,
                    leverage=leverage,
                )
                slot_summary.append(result["summary"])
                selected_rows.extend(result["selected"])
                worst_topups.extend(result["worst_topups"])

    write_csv(output_dir / "capital_slot_summary.csv", slot_summary)
    write_csv(output_dir / "selected_trades.csv", selected_rows)
    worst_topups.sort(key=lambda row: to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0, reverse=True)
    write_csv(output_dir / "worst_manual_topups.csv", worst_topups[:300])

    metadata = {
        "schema": "pump_short_bybit_funding_tp_capital_grid_v1",
        "input_root": str(input_root),
        "output_dir": str(output_dir),
        "exchange": exchange,
        "pullback_pct": DEFAULT_PULLBACK_PCT,
        "max_wait_h": DEFAULT_MAX_WAIT_H,
        "ladder_legs": DEFAULT_LADDER_LEGS,
        "ladder_step_pct": DEFAULT_LADDER_STEP_PCT,
        "max_hold_h": DEFAULT_MAX_HOLD_H,
        "base_leg_notional_usd": DEFAULT_BASE_LEG_NOTIONAL_USD,
        "leverage": leverage,
        "funding_windows_h": list(funding_windows_h),
        "funding_min_pcts": list(funding_min_pcts),
        "tp_pcts": list(tp_pcts),
        "capitals_usd": list(capitals_usd),
        "max_slots": max_slots,
        "outcomes": len(outcomes),
        "raw_summary_rows": len(raw_summary),
        "capital_slot_summary_rows": len(slot_summary),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    write_report(output_dir, metadata, slot_summary, raw_summary)
    return metadata


def build_outcomes(
    *,
    input_root: Path,
    exchange: str,
    funding_windows_h: tuple[int, ...],
    funding_min_pcts: tuple[float, ...],
    tp_pcts: tuple[float, ...],
) -> list[Outcome]:
    outcomes: list[Outcome] = []
    for sample in load_exchange_samples(input_root, exchange=exchange):
        series = parse_series(sample)
        if not series or series.exchange != exchange or len(series.ts) < 200:
            continue
        for event in detect_pump_events(series):
            entry_idx = find_pullback_entry(series, int(event["trigger_idx"]), DEFAULT_PULLBACK_PCT, DEFAULT_MAX_WAIT_H)
            if entry_idx is None:
                continue
            legs = ladder_entries(
                series,
                entry_idx,
                step_pct=DEFAULT_LADDER_STEP_PCT,
                max_legs=DEFAULT_LADDER_LEGS,
                max_wait_h=DEFAULT_MAX_HOLD_H,
            )
            if not legs:
                continue
            funding_by_window = {
                window_h: funding_window_sum_pct(series.funding, series.ts[entry_idx], window_h)
                for window_h in funding_windows_h
            }
            for tp_pct in tp_pcts:
                exit_result = simulate_exit(series, legs, ExitPlan(tp_pct=tp_pct))
                if not exit_result:
                    continue
                for window_h in funding_windows_h:
                    funding_prev_pct, funding_points = funding_by_window[window_h]
                    for funding_min_pct in funding_min_pcts:
                        if funding_prev_pct <= funding_min_pct:
                            continue
                        outcomes.append(
                            Outcome(
                                symbol=series.symbol,
                                trigger_ts=int(event["trigger_ts"]),
                                entry_ts=series.ts[entry_idx],
                                exit_ts=int(exit_result["exit_ts"]),
                                funding_window_h=window_h,
                                funding_min_pct=funding_min_pct,
                                funding_prev_pct=funding_prev_pct,
                                funding_points=funding_points,
                                tp_pct=tp_pct,
                                pnl_usd_base=float(exit_result["pnl_usd"]),
                                net_pct=float(exit_result["net_pct"]),
                                funding_during_pct=float(exit_result["funding_during_pct"]),
                                mae_pct=to_float(exit_result.get("mae_pct")) or 0.0,
                                legs_filled=int(exit_result["legs_filled"]),
                                exit_reason=str(exit_result["exit_reason"]),
                                hold_h=float(exit_result["hold_h"]),
                                win=int(exit_result["win"]),
                                cat300=int(exit_result["cat300"]),
                            )
                        )
    outcomes.sort(key=lambda item: (item.entry_ts, item.symbol, item.strategy))
    return outcomes


def load_exchange_samples(input_root: Path, *, exchange: str) -> Iterable[dict[str, Any]]:
    path = input_root / exchange / "symbol_samples.jsonl"
    if not path.exists():
        return
    latest: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                sample = json.loads(text)
            except json.JSONDecodeError:
                continue
            symbol = str(sample.get("symbol") or sample.get("exchange_symbol") or "")
            if symbol:
                latest[symbol] = sample
    yield from latest.values()


def funding_window_sum_pct(rows: list[tuple[int, float]], end_ms: int, window_h: int) -> tuple[float, int]:
    start_ms = end_ms - int(window_h) * 3_600_000
    values = [rate * 100.0 for ts_ms, rate in rows if start_ms <= ts_ms <= end_ms]
    return (sum(values), len(values))


def simulate_exit(series: Any, legs: list[tuple[int, float]], plan: ExitPlan) -> dict[str, Any] | None:
    entry_idx = legs[0][0]
    exit_limit_idx = min(len(series.ts) - 1, entry_idx + plan.max_hold_h)
    if exit_limit_idx <= entry_idx:
        return None
    pending = sorted(legs)
    active: list[tuple[int, float]] = [pending[0]]
    max_mae: float | None = None
    max_mfe: float | None = None
    exit_idx = exit_limit_idx
    exit_price = series.close[exit_idx]
    exit_reason = "time_stop"

    for idx in range(entry_idx + 1, exit_limit_idx + 1):
        for leg_idx, leg_price in pending:
            if leg_idx == idx and (leg_idx, leg_price) not in active:
                active.append((leg_idx, leg_price))
        avg_entry = statistics.mean(price for _, price in active)
        high = series.high[idx]
        low = series.low[idx]
        if high is not None:
            mae_now = (high / avg_entry - 1.0) * 100.0
            max_mae = mae_now if max_mae is None else max(max_mae, mae_now)
        if low is not None:
            mfe_now = (1.0 - low / avg_entry) * 100.0
            max_mfe = mfe_now if max_mfe is None else max(max_mfe, mfe_now)
        target_price = avg_entry * (1.0 - plan.tp_pct / 100.0)
        if low is not None and low <= target_price:
            exit_idx = idx
            exit_price = target_price
            exit_reason = "take_profit"
            break
    if not exit_price:
        return None

    filled = [(idx, price) for idx, price in pending if idx <= exit_idx]
    if not filled:
        return None
    leg_net_pcts: list[float] = []
    leg_funding_pcts: list[float] = []
    pnl_usd = 0.0
    for leg_idx, entry_price in filled:
        price_pnl_pct = (1.0 - float(exit_price) / entry_price) * 100.0
        funding_pct, _ = funding_window_between_pct(series.funding, series.ts[leg_idx], series.ts[exit_idx])
        net_pct = price_pnl_pct + funding_pct - FEE_ROUNDTRIP_PCT
        leg_net_pcts.append(net_pct)
        leg_funding_pcts.append(funding_pct)
        pnl_usd += DEFAULT_BASE_LEG_NOTIONAL_USD * net_pct / 100.0
    return {
        "exit_ts": series.ts[exit_idx],
        "exit_iso": ms_to_iso(series.ts[exit_idx]),
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "hold_h": round((series.ts[exit_idx] - series.ts[entry_idx]) / 3_600_000.0, 2),
        "legs_filled": len(filled),
        "net_pct": statistics.mean(leg_net_pcts),
        "funding_during_pct": statistics.mean(leg_funding_pcts) if leg_funding_pcts else 0.0,
        "pnl_usd": pnl_usd,
        "mae_pct": max_mae,
        "mfe_pct": max_mfe,
        "win": 1 if pnl_usd > 0 else 0,
        "cat300": 1 if max_mae is not None and max_mae >= 300.0 else 0,
    }


def funding_window_between_pct(rows: list[tuple[int, float]], start_ms: int, end_ms: int) -> tuple[float, int]:
    values = [rate * 100.0 for ts_ms, rate in rows if start_ms <= ts_ms <= end_ms]
    return (sum(values), len(values))


def group_outcomes(outcomes: list[Outcome]) -> dict[str, list[Outcome]]:
    grouped: dict[str, list[Outcome]] = {}
    for outcome in outcomes:
        grouped.setdefault(outcome.strategy, []).append(outcome)
    return grouped


def simulate_slots(
    outcomes: list[Outcome],
    *,
    capital_usd: float,
    slots: int,
    leverage: float,
) -> dict[str, Any]:
    per_coin_capital = capital_usd / slots
    per_step_margin = per_coin_capital / DEFAULT_LADDER_LEGS
    per_step_notional = per_step_margin * leverage
    scale = per_step_notional / DEFAULT_BASE_LEG_NOTIONAL_USD
    active: list[tuple[int, str]] = []
    selected: list[dict[str, Any]] = []
    topups: list[dict[str, Any]] = []
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

    for outcome in sorted(outcomes, key=lambda item: (item.entry_ts, item.symbol)):
        active = [(exit_ts, symbol) for exit_ts, symbol in active if exit_ts > outcome.entry_ts]
        active_symbols = {symbol for _, symbol in active}
        if outcome.symbol in active_symbols:
            skipped_same_symbol += 1
            continue
        if len(active) >= slots:
            skipped_slots += 1
            continue
        active.append((outcome.exit_ts, outcome.symbol))
        max_active = max(max_active, len(active))

        scaled_pnl = outcome.pnl_usd_base * scale
        gross_notional = outcome.legs_filled * per_step_notional
        posted_initial_margin = gross_notional / leverage if leverage > 0 else 0.0
        peak_unrealized_loss = max(0.0, outcome.mae_pct / 100.0 * gross_notional)
        current_margin_topup = max(0.0, peak_unrealized_loss - posted_initial_margin)
        manual_topup = max(0.0, peak_unrealized_loss - per_coin_capital)
        funding_usd = gross_notional * outcome.funding_during_pct / 100.0

        pnl += scaled_pnl
        wins += 1 if scaled_pnl > 0 else 0
        tp_hits += 1 if outcome.exit_reason == "take_profit" else 0
        cat300 += outcome.cat300
        total_hold_h += outcome.hold_h
        total_funding_usd += funding_usd
        total_scaled_notional += gross_notional
        max_current_margin_topup = max(max_current_margin_topup, current_margin_topup)
        max_single_topup = max(max_single_topup, manual_topup)
        if manual_topup > 0:
            manual_topup_events += 1
            manual_topup_sum += manual_topup

        row = {
            "capital_usd": capital_usd,
            "slots": slots,
            "strategy": outcome.strategy,
            "funding_window_h": outcome.funding_window_h,
            "funding_min_pct": outcome.funding_min_pct,
            "tp_pct": outcome.tp_pct,
            "symbol": outcome.symbol,
            "entry_ts": outcome.entry_ts,
            "entry_iso": ms_to_iso(outcome.entry_ts),
            "exit_ts": outcome.exit_ts,
            "exit_iso": ms_to_iso(outcome.exit_ts),
            "exit_reason": outcome.exit_reason,
            "legs_filled": outcome.legs_filled,
            "per_step_margin_usd": round(per_step_margin, 6),
            "per_step_notional_usd": round(per_step_notional, 6),
            "gross_notional_usd": round(gross_notional, 6),
            "pnl_usd": round(scaled_pnl, 6),
            "funding_usd": round(funding_usd, 6),
            "net_pct": round(outcome.net_pct, 6),
            "mae_pct": round(outcome.mae_pct, 6),
            "funding_prev_pct": round(outcome.funding_prev_pct, 6),
            "funding_points": outcome.funding_points,
            "peak_unrealized_loss_usd": round(peak_unrealized_loss, 6),
            "current_margin_topup_usd": round(current_margin_topup, 6),
            "manual_topup_beyond_alloc_usd": round(manual_topup, 6),
        }
        selected.append(row)
        if manual_topup > 0:
            topups.append(row)

    taken = len(selected)
    capital_plus_topups = capital_usd + manual_topup_sum
    first = outcomes[0] if outcomes else None
    summary = {
        "capital_usd": capital_usd,
        "slots": slots,
        "strategy": first.strategy if first else None,
        "funding_window_h": first.funding_window_h if first else None,
        "funding_min_pct": first.funding_min_pct if first else None,
        "tp_pct": first.tp_pct if first else None,
        "per_coin_capital_usd": round(per_coin_capital, 6),
        "per_step_margin_usd": round(per_step_margin, 6),
        "per_step_notional_usd": round(per_step_notional, 6),
        "max_planned_notional_per_coin_usd": round(per_step_notional * DEFAULT_LADDER_LEGS, 6),
        "trades_available": len(outcomes),
        "trades_taken": taken,
        "trades_skipped_slots": skipped_slots,
        "trades_skipped_same_symbol": skipped_same_symbol,
        "max_active_seen": max_active,
        "win_rate_pct": pct(wins, taken),
        "take_profit_rate_pct": pct(tp_hits, taken),
        "cat300_rate_pct": pct(cat300, taken),
        "net_pnl_usd": round(pnl, 6),
        "final_capital_usd": round(capital_usd + pnl, 6),
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
    topups.sort(key=lambda row: to_float(row.get("manual_topup_beyond_alloc_usd")) or 0.0, reverse=True)
    return {"summary": summary, "selected": selected, "worst_topups": topups[:25]}


def build_raw_summary(outcomes: list[Outcome]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for strategy, group in group_outcomes(outcomes).items():
        nets = [item.net_pct for item in group]
        maes = [item.mae_pct for item in group]
        first = group[0]
        rows.append(
            {
                "strategy": strategy,
                "funding_window_h": first.funding_window_h,
                "funding_min_pct": first.funding_min_pct,
                "tp_pct": first.tp_pct,
                "n": len(group),
                "win_rate_pct": pct(sum(item.win for item in group), len(group)),
                "take_profit_rate_pct": pct(sum(1 for item in group if item.exit_reason == "take_profit"), len(group)),
                "avg_net_pct": safe_mean(nets),
                "median_net_pct": statistics.median(nets) if nets else None,
                "sum_pnl_usd_base": sum(item.pnl_usd_base for item in group),
                "avg_pnl_usd_base": safe_mean([item.pnl_usd_base for item in group]),
                "avg_funding_during_pct": safe_mean([item.funding_during_pct for item in group]),
                "avg_funding_prev_pct": safe_mean([item.funding_prev_pct for item in group]),
                "avg_funding_points": safe_mean([float(item.funding_points) for item in group]),
                "p90_mae_pct": quantile(maes, 0.90),
                "p95_mae_pct": quantile(maes, 0.95),
                "cat300_pct": pct(sum(item.cat300 for item in group), len(group)),
                "avg_hold_h": safe_mean([item.hold_h for item in group]),
                "avg_legs": safe_mean([float(item.legs_filled) for item in group]),
            }
        )
    return sorted(rows, key=lambda row: -(to_float(row.get("sum_pnl_usd_base")) or 0.0))


def outcome_to_row(outcome: Outcome) -> dict[str, Any]:
    return {
        "exchange": DEFAULT_EXCHANGE,
        "symbol": outcome.symbol,
        "strategy": outcome.strategy,
        "trigger_ts": outcome.trigger_ts,
        "trigger_iso": ms_to_iso(outcome.trigger_ts),
        "entry_ts": outcome.entry_ts,
        "entry_iso": ms_to_iso(outcome.entry_ts),
        "exit_ts": outcome.exit_ts,
        "exit_iso": ms_to_iso(outcome.exit_ts),
        "funding_window_h": outcome.funding_window_h,
        "funding_min_pct": outcome.funding_min_pct,
        "funding_prev_pct": outcome.funding_prev_pct,
        "funding_points": outcome.funding_points,
        "tp_pct": outcome.tp_pct,
        "exit_reason": outcome.exit_reason,
        "hold_h": outcome.hold_h,
        "legs_filled": outcome.legs_filled,
        "net_pct": outcome.net_pct,
        "funding_during_pct": outcome.funding_during_pct,
        "pnl_usd_base": outcome.pnl_usd_base,
        "mae_pct": outcome.mae_pct,
        "win": outcome.win,
        "cat300": outcome.cat300,
    }


def write_report(
    output_dir: Path,
    metadata: dict[str, Any],
    slot_summary: list[dict[str, Any]],
    raw_summary: list[dict[str, Any]],
) -> None:
    lines = [
        "# Bybit pump-short funding/TP capital grid",
        "",
        f"Generated: `{metadata['created_at']}`",
        "",
        "Scope: Bybit only, `pb20`, 4 equal ladder legs, 50% adverse spacing, 168h max hold, 3x isolated-style sizing.",
        "Funding gate is the sum of settled funding over the previous N hours; if no settlement occurred in a short window, the previous-window sum is `0.0%`.",
        "",
        f"- Outcomes: `{metadata['outcomes']}`",
        f"- Funding windows: `{metadata['funding_windows_h'][0]}..{metadata['funding_windows_h'][-1]}` hours",
        f"- Funding thresholds: `{metadata['funding_min_pcts']}`",
        f"- TP values: `{metadata['tp_pcts']}`",
        f"- Capitals: `{metadata['capitals_usd']}`",
        "",
    ]
    for capital in metadata["capitals_usd"]:
        rows = [row for row in slot_summary if to_float(row.get("capital_usd")) == float(capital)]
        lines.extend([f"## Capital ${capital:,.0f}: top ROI rows", ""])
        lines.extend(
            markdown_table(
                sorted(rows, key=lambda row: to_float(row.get("roi_on_initial_pct")) or -999999.0, reverse=True)[:20],
                (
                    "slots",
                    "funding_window_h",
                    "funding_min_pct",
                    "tp_pct",
                    "trades_taken",
                    "roi_on_initial_pct",
                    "net_pnl_usd",
                    "win_rate_pct",
                    "take_profit_rate_pct",
                    "avg_hold_h",
                    "manual_topup_sum_usd",
                    "max_single_manual_topup_usd",
                ),
            )
        )
        lines.append("")
        lines.extend([f"## Capital ${capital:,.0f}: best row by slots", ""])
        best_by_slot: list[dict[str, Any]] = []
        for slots in range(1, int(metadata["max_slots"]) + 1):
            slot_rows = [row for row in rows if int(row.get("slots") or 0) == slots]
            if slot_rows:
                best_by_slot.append(max(slot_rows, key=lambda row: to_float(row.get("roi_on_initial_pct")) or -999999.0))
        lines.extend(
            markdown_table(
                best_by_slot,
                (
                    "slots",
                    "funding_window_h",
                    "funding_min_pct",
                    "tp_pct",
                    "trades_taken",
                    "roi_on_initial_pct",
                    "max_single_manual_topup_usd",
                ),
            )
        )
        lines.append("")

    lines.extend(["## Raw strategy top rows", ""])
    lines.extend(
        markdown_table(
            raw_summary[:20],
            (
                "funding_window_h",
                "funding_min_pct",
                "tp_pct",
                "n",
                "win_rate_pct",
                "take_profit_rate_pct",
                "avg_net_pct",
                "sum_pnl_usd_base",
                "p90_mae_pct",
                "avg_hold_h",
            ),
        )
    )
    lines.append("")
    (output_dir / "funding_tp_capital_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def strategy_name(*, funding_window_h: int, funding_min_pct: float, tp_pct: float) -> str:
    return (
        f"pb20_wait168_fg{funding_window_h}h_gt_{format_token(funding_min_pct)}_"
        f"ladder4_step50_tp{tp_pct:g}_hold168"
    )


def format_token(value: float) -> str:
    return f"{value:g}".replace("-", "m").replace(".", "p")


def pct(part: float, total: float) -> float | None:
    if not total:
        return None
    return round(part / total * 100.0, 6)


def safe_mean(values: list[float]) -> float | None:
    values = [value for value in values if math.isfinite(value)]
    return statistics.mean(values) if values else None


def markdown_table(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> list[str]:
    if not rows:
        return ["_No rows._"]
    out = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        out.append("| " + " | ".join(format_cell(row.get(column)) for column in columns) + " |")
    return out


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


__all__ = [
    "build_outcomes",
    "funding_window_sum_pct",
    "run_bybit_funding_tp_capital_grid",
    "simulate_slots",
    "strategy_name",
]
