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

DEFAULT_LONG_OUTCOMES = BASE_DIR / "data" / "research" / "pump_funding_premium_window_research_5m_candidates" / "premium_long_outcomes.csv"
DEFAULT_SHORT_TRADES = BASE_DIR / "data" / "research" / "pump_short_dynamic_combo_report_3000_2024" / "dynamic_combo_trades.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_cycle_portfolio_report"
STARTING_CAPITAL_USD = 3000.0
SHORT_LEVERAGE = 3.0


@dataclass(frozen=True, slots=True)
class TrackSpec:
    track_id: str
    side: str
    title: str
    entry_rule: str = ""
    exit_plan: str = ""
    filter_slug: str = ""
    policy_slug: str = ""
    source_slots: int = 4
    source_sizing_mode: str = "dynamic"
    leverage: float = 2.0


@dataclass(frozen=True, slots=True)
class AllocationSpec:
    allocation_id: str
    title: str
    total_slots: int
    short_slots: int
    long_slots: int
    sizing_mode: str


LONG_TRACKS: tuple[TrackSpec, ...] = (
    TrackSpec(
        track_id="long_broad",
        side="long",
        title="Long broad premium discount",
        entry_rule="deep_discount_survives",
        exit_plan="tp30_sl25_hold72_fundrelief",
        filter_slug="premium_not_toxic_oi_wait",
        leverage=2.0,
    ),
    TrackSpec(
        track_id="long_clean_oi",
        side="long",
        title="Long clean wait30 OI10",
        entry_rule="deep_discount_survives",
        exit_plan="tp30_sl25_hold72_fundrelief",
        filter_slug="veto_wait30_oi10",
        leverage=2.0,
    ),
    TrackSpec(
        track_id="long_high_conf",
        side="long",
        title="Long high confidence midpremium",
        entry_rule="deep_discount_survives",
        exit_plan="tp30_sl25_hold72_fundrelief",
        filter_slug="veto_high_confidence_midpremium",
        leverage=2.0,
    ),
)

SHORT_TRACKS: tuple[TrackSpec, ...] = (
    TrackSpec(
        track_id="short_clean_p100_l3",
        side="short",
        title="Short clean pump>=100 legs3 tapered",
        policy_slug="pump_ge_100__step50_legs3_tapered_tp25_336",
        source_slots=4,
        leverage=SHORT_LEVERAGE,
    ),
    TrackSpec(
        track_id="short_aggr_p80_l3",
        side="short",
        title="Short aggressive pump>=80 legs3 tapered",
        policy_slug="pump_ge_80__step50_legs3_tapered_tp25_336",
        source_slots=4,
        leverage=SHORT_LEVERAGE,
    ),
    TrackSpec(
        track_id="short_clean_p100_l2",
        side="short",
        title="Short clean pump>=100 legs2 tapered",
        policy_slug="pump_ge_100__step50_legs2_tapered_tp25_336",
        source_slots=4,
        leverage=SHORT_LEVERAGE,
    ),
)

ALLOCATIONS: tuple[AllocationSpec, ...] = (
    AllocationSpec("short_only_4", "Short-only control: 4 short slots", total_slots=4, short_slots=4, long_slots=0, sizing_mode="split_initial"),
    AllocationSpec("long_only_2", "Long-only control: 2 long slots", total_slots=2, short_slots=0, long_slots=2, sizing_mode="split_initial"),
    AllocationSpec("cycle_6_4s2l", "Combined cycle: 4 short + 2 long", total_slots=6, short_slots=4, long_slots=2, sizing_mode="split_initial"),
    AllocationSpec("cycle_5_4s1l", "Combined cycle: 4 short + 1 long", total_slots=5, short_slots=4, long_slots=1, sizing_mode="split_initial"),
    AllocationSpec("cycle_5_3s2l", "Combined cycle: 3 short + 2 long", total_slots=5, short_slots=3, long_slots=2, sizing_mode="split_initial"),
    AllocationSpec("cycle_6_4s2l_dynamic", "Combined dynamic: 4 short + 2 long", total_slots=6, short_slots=4, long_slots=2, sizing_mode="split_dynamic"),
    AllocationSpec("cycle_6_4s2l_fixed750", "Combined fixed $750: 4 short + 2 long", total_slots=6, short_slots=4, long_slots=2, sizing_mode="fixed_750"),
)


def run_pump_cycle_portfolio_report(
    *,
    long_outcomes_path: Path = DEFAULT_LONG_OUTCOMES,
    short_trades_path: Path = DEFAULT_SHORT_TRADES,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    starting_capital_usd: float = STARTING_CAPITAL_USD,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    long_rows = read_csv(long_outcomes_path)
    short_rows = read_csv(short_trades_path)
    long_candidates = {track.track_id: load_long_candidates(long_rows, track) for track in LONG_TRACKS}
    short_candidates = {track.track_id: load_short_candidates(short_rows, track) for track in SHORT_TRACKS}

    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    all_track_pairs = [(None, short) for short in SHORT_TRACKS] + [(long, None) for long in LONG_TRACKS] + [
        (long, short) for long in LONG_TRACKS for short in SHORT_TRACKS
    ]
    for long_track, short_track in all_track_pairs:
        for allocation in ALLOCATIONS:
            if allocation.long_slots == 0 and long_track is not None:
                continue
            if allocation.short_slots == 0 and short_track is not None:
                continue
            if long_track is None and allocation.long_slots > 0 and allocation.short_slots == 0:
                continue
            if short_track is None and allocation.short_slots > 0 and allocation.long_slots == 0:
                continue
            if long_track is None and allocation.long_slots > 0:
                continue
            if short_track is None and allocation.short_slots > 0:
                continue
            candidates = []
            if long_track:
                candidates.extend(long_candidates[long_track.track_id])
            if short_track:
                candidates.extend(short_candidates[short_track.track_id])
            if not candidates:
                continue
            summary, trades, equity = replay_cycle(
                allocation,
                long_track,
                short_track,
                candidates,
                starting_capital_usd=starting_capital_usd,
            )
            summary_rows.append(summary)
            trade_rows.extend(trades)
            equity_rows.extend(equity)

    summary_rows.sort(key=summary_sort_key, reverse=True)
    selected = select_report_rows(summary_rows)
    html_report = render_html_report(summary_rows, selected, trade_rows, equity_rows, starting_capital_usd)
    write_csv(output_dir / "cycle_summary.csv", summary_rows)
    write_csv(output_dir / "cycle_trades.csv", trade_rows)
    write_csv(output_dir / "cycle_equity.csv", equity_rows)
    (output_dir / "index.html").write_text(html_report, encoding="utf-8")
    metadata = {
        "schema": "pump_cycle_portfolio_report_v1",
        "long_outcomes_path": str(long_outcomes_path),
        "short_trades_path": str(short_trades_path),
        "output_dir": str(output_dir),
        "starting_capital_usd": starting_capital_usd,
        "long_input_rows": len(long_rows),
        "short_input_rows": len(short_rows),
        "summary_rows": len(summary_rows),
        "trade_rows": len(trade_rows),
        "equity_rows": len(equity_rows),
        "allocations": [item.allocation_id for item in ALLOCATIONS],
        "long_tracks": [item.track_id for item in LONG_TRACKS],
        "short_tracks": [item.track_id for item in SHORT_TRACKS],
        "elapsed_sec": round_float(time.time() - started),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def load_long_candidates(rows: list[dict[str, Any]], track: TrackSpec) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    spec = next(item for item in FILTER_SPECS if item.slug == track.filter_slug)
    for row in rows:
        if row.get("entry_rule") != track.entry_rule or row.get("exit_plan") != track.exit_plan:
            continue
        if not filter_matches(row, spec):
            continue
        out.append(
            {
                "side": "long",
                "track_id": track.track_id,
                "track_title": track.title,
                "symbol": row.get("symbol"),
                "event_id": row.get("event_id"),
                "entry_ts": to_int(row.get("entry_ts")) or 0,
                "entry_iso": row.get("entry_iso"),
                "exit_ts": to_int(row.get("exit_ts")) or 0,
                "exit_iso": row.get("exit_iso"),
                "exit_reason": row.get("exit_reason"),
                "net_pct": to_float(row.get("net_pct")) or 0.0,
                "leverage": track.leverage,
                "source_budget_usd": None,
                "source_topup_usd": 0.0,
                "entry_premium_pct": row.get("entry_premium_pct"),
                "entry_oi_change_4h_pct": row.get("entry_oi_change_4h_pct"),
                "entry_volume_z": row.get("entry_volume_z"),
            }
        )
    return sorted(out, key=candidate_sort_key)


def load_short_candidates(rows: list[dict[str, Any]], track: TrackSpec) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("policy_slug") != track.policy_slug:
            continue
        if to_int(row.get("slots")) != track.source_slots or row.get("sizing_mode") != track.source_sizing_mode:
            continue
        source_budget = to_float(row.get("per_coin_capital_usd")) or 0.0
        source_topup = to_float(row.get("topup_usd")) or 0.0
        out.append(
            {
                "side": "short",
                "track_id": track.track_id,
                "track_title": track.title,
                "symbol": row.get("symbol"),
                "event_id": row.get("case_id"),
                "entry_ts": to_int(row.get("entry_ts")) or 0,
                "entry_iso": row.get("entry_iso"),
                "exit_ts": to_int(row.get("exit_ts")) or 0,
                "exit_iso": row.get("exit_iso"),
                "exit_reason": row.get("exit_reason"),
                "net_pct": to_float(row.get("net_pct")) or 0.0,
                "leverage": track.leverage,
                "source_budget_usd": source_budget,
                "source_topup_usd": source_topup,
                "pump_pct": row.get("pump_pct"),
                "stress_pct": row.get("stress_pct"),
                "rule_slug": row.get("rule_slug"),
            }
        )
    return sorted(out, key=candidate_sort_key)


def replay_cycle(
    allocation: AllocationSpec,
    long_track: TrackSpec | None,
    short_track: TrackSpec | None,
    candidates: list[dict[str, Any]],
    *,
    starting_capital_usd: float,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = sorted(candidates, key=candidate_sort_key)
    active: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    equity_points: list[dict[str, Any]] = []
    realized_pnl = 0.0
    peak_equity = starting_capital_usd
    max_drawdown_usd = 0.0
    skipped_short_slots = 0
    skipped_long_slots = 0
    skipped_total_slots = 0
    skipped_same_symbol = 0
    skipped_conflict_symbol = 0
    skipped_insolvent = 0
    max_active_short = 0
    max_active_long = 0
    max_active_total = 0
    worst_trade_pct: float | None = None
    worst_trade_usd: float | None = None
    max_concurrent_topup_usd = 0.0
    first_entry_ts = min((to_int(row.get("entry_ts")) or 0 for row in candidates), default=0)
    if first_entry_ts:
        equity_points.append(make_equity_row(allocation, long_track, short_track, first_entry_ts, starting_capital_usd, 0.0, 0, 0, 0.0, "start"))

    def equity() -> float:
        return starting_capital_usd + realized_pnl

    def release_until(ts_ms: int) -> None:
        nonlocal active, realized_pnl, peak_equity, max_drawdown_usd, max_concurrent_topup_usd
        closed = [trade for trade in active if (to_int(trade.get("exit_ts")) or 0) <= ts_ms]
        active = [trade for trade in active if (to_int(trade.get("exit_ts")) or 0) > ts_ms]
        for trade in sorted(closed, key=lambda item: to_int(item.get("exit_ts")) or 0):
            realized_pnl += to_float(trade.get("pnl_usd")) or 0.0
            current_equity = equity()
            peak_equity = max(peak_equity, current_equity)
            max_drawdown_usd = max(max_drawdown_usd, peak_equity - current_equity)
            active_short, active_long = active_counts(active)
            topup = concurrent_topup(active)
            max_concurrent_topup_usd = max(max_concurrent_topup_usd, topup)
            equity_points.append(
                make_equity_row(
                    allocation,
                    long_track,
                    short_track,
                    to_int(trade.get("exit_ts")) or ts_ms,
                    current_equity,
                    realized_pnl,
                    active_short,
                    active_long,
                    topup,
                    "close",
                )
            )

    for candidate in candidates:
        entry_ts = to_int(candidate.get("entry_ts")) or 0
        exit_ts = to_int(candidate.get("exit_ts")) or 0
        if entry_ts <= 0 or exit_ts <= entry_ts:
            continue
        release_until(entry_ts)
        if equity() <= 0:
            skipped_insolvent += 1
            continue
        side = str(candidate.get("side"))
        active_short, active_long = active_counts(active)
        if active_short + active_long >= allocation.total_slots:
            skipped_total_slots += 1
            continue
        if side == "short" and active_short >= allocation.short_slots:
            skipped_short_slots += 1
            continue
        if side == "long" and active_long >= allocation.long_slots:
            skipped_long_slots += 1
            continue
        symbol = str(candidate.get("symbol") or "")
        if any(str(item.get("symbol") or "") == symbol and item.get("side") == side for item in active):
            skipped_same_symbol += 1
            continue
        if any(str(item.get("symbol") or "") == symbol and item.get("side") != side for item in active):
            skipped_conflict_symbol += 1
            continue
        slot_budget = resolve_slot_budget(allocation, starting_capital_usd, equity())
        if slot_budget <= 0:
            skipped_insolvent += 1
            continue
        leverage = to_float(candidate.get("leverage")) or 1.0
        net_pct = to_float(candidate.get("net_pct")) or 0.0
        pnl_usd = slot_budget * net_pct * leverage / 100.0
        topup_usd = scaled_topup(candidate, slot_budget)
        trade = build_trade_row(allocation, long_track, short_track, candidate, slot_budget, leverage, pnl_usd, topup_usd)
        active.append(trade)
        trades.append(trade)
        active_short, active_long = active_counts(active)
        max_active_short = max(max_active_short, active_short)
        max_active_long = max(max_active_long, active_long)
        max_active_total = max(max_active_total, active_short + active_long)
        max_concurrent_topup_usd = max(max_concurrent_topup_usd, concurrent_topup(active))
        levered_net_pct = net_pct * leverage
        worst_trade_pct = levered_net_pct if worst_trade_pct is None else min(worst_trade_pct, levered_net_pct)
        worst_trade_usd = pnl_usd if worst_trade_usd is None else min(worst_trade_usd, pnl_usd)
    release_until(10**18)

    final_equity = equity()
    roi_pct = (final_equity / starting_capital_usd - 1.0) * 100.0
    max_drawdown_pct = max_drawdown_usd / starting_capital_usd * 100.0
    wins = sum(1 for trade in trades if (to_float(trade.get("pnl_usd")) or 0.0) > 0)
    long_trades = [trade for trade in trades if trade.get("side") == "long"]
    short_trades = [trade for trade in trades if trade.get("side") == "short"]
    risk_adjusted_roi = roi_pct - max_drawdown_pct - max_concurrent_topup_usd / starting_capital_usd * 0.25 - max(0.0, -(worst_trade_usd or 0.0)) / starting_capital_usd * 50.0
    summary = {
        "allocation_id": allocation.allocation_id,
        "allocation_title": allocation.title,
        "long_track_id": long_track.track_id if long_track else "",
        "short_track_id": short_track.track_id if short_track else "",
        "sizing_mode": allocation.sizing_mode,
        "total_slots": allocation.total_slots,
        "short_slots": allocation.short_slots,
        "long_slots": allocation.long_slots,
        "starting_capital_usd": round_float(starting_capital_usd),
        "trades": len(trades),
        "short_trades": len(short_trades),
        "long_trades": len(long_trades),
        "win_pct": pct(wins, len(trades)),
        "short_win_pct": pct(sum(1 for trade in short_trades if (to_float(trade.get("pnl_usd")) or 0.0) > 0), len(short_trades)),
        "long_win_pct": pct(sum(1 for trade in long_trades if (to_float(trade.get("pnl_usd")) or 0.0) > 0), len(long_trades)),
        "final_equity_usd": round_float(final_equity),
        "realized_pnl_usd": round_float(realized_pnl),
        "roi_pct": round_float(roi_pct),
        "risk_adjusted_roi_pct": round_float(risk_adjusted_roi),
        "max_drawdown_usd": round_float(max_drawdown_usd),
        "max_drawdown_pct": round_float(max_drawdown_pct),
        "max_concurrent_topup_usd": round_float(max_concurrent_topup_usd),
        "worst_trade_usd": round_float(worst_trade_usd),
        "worst_trade_pct": round_float(worst_trade_pct),
        "avg_trade_usd": round_float(statistics.mean(values(trades, "pnl_usd")) if trades else None),
        "skipped_short_slots": skipped_short_slots,
        "skipped_long_slots": skipped_long_slots,
        "skipped_total_slots": skipped_total_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "skipped_conflict_symbol": skipped_conflict_symbol,
        "skipped_insolvent": skipped_insolvent,
        "max_active_short": max_active_short,
        "max_active_long": max_active_long,
        "max_active_total": max_active_total,
        "first_entry_iso": ms_to_iso(min((to_int(trade.get("entry_ts")) or 0 for trade in trades), default=0)),
        "last_exit_iso": ms_to_iso(max((to_int(trade.get("exit_ts")) or 0 for trade in trades), default=0)),
    }
    return summary, trades, equity_points


def resolve_slot_budget(allocation: AllocationSpec, starting_capital_usd: float, current_equity: float) -> float:
    if allocation.sizing_mode == "fixed_750":
        return 750.0
    capital = max(0.0, current_equity) if allocation.sizing_mode == "split_dynamic" else starting_capital_usd
    return capital / max(1, allocation.total_slots)


def scaled_topup(candidate: dict[str, Any], slot_budget: float) -> float:
    if candidate.get("side") != "short":
        return 0.0
    source_budget = to_float(candidate.get("source_budget_usd")) or 0.0
    source_topup = to_float(candidate.get("source_topup_usd")) or 0.0
    if source_budget <= 0 or source_topup <= 0:
        return 0.0
    return source_topup * slot_budget / source_budget


def build_trade_row(
    allocation: AllocationSpec,
    long_track: TrackSpec | None,
    short_track: TrackSpec | None,
    candidate: dict[str, Any],
    slot_budget: float,
    leverage: float,
    pnl_usd: float,
    topup_usd: float,
) -> dict[str, Any]:
    entry_ts = to_int(candidate.get("entry_ts")) or 0
    exit_ts = to_int(candidate.get("exit_ts")) or entry_ts
    net_pct = to_float(candidate.get("net_pct")) or 0.0
    return {
        "allocation_id": allocation.allocation_id,
        "long_track_id": long_track.track_id if long_track else "",
        "short_track_id": short_track.track_id if short_track else "",
        "side": candidate.get("side"),
        "track_id": candidate.get("track_id"),
        "symbol": candidate.get("symbol"),
        "event_id": candidate.get("event_id"),
        "entry_ts": entry_ts,
        "entry_iso": candidate.get("entry_iso") or ms_to_iso(entry_ts),
        "exit_ts": exit_ts,
        "exit_iso": candidate.get("exit_iso") or ms_to_iso(exit_ts),
        "hold_h": round_float((exit_ts - entry_ts) / 3_600_000.0),
        "exit_reason": candidate.get("exit_reason"),
        "slot_budget_usd": round_float(slot_budget),
        "leverage": leverage,
        "net_pct": round_float(net_pct),
        "levered_net_pct": round_float(net_pct * leverage),
        "pnl_usd": round_float(pnl_usd),
        "topup_usd": round_float(topup_usd),
        "pump_pct": candidate.get("pump_pct"),
        "stress_pct": candidate.get("stress_pct"),
        "entry_premium_pct": candidate.get("entry_premium_pct"),
        "entry_oi_change_4h_pct": candidate.get("entry_oi_change_4h_pct"),
        "entry_volume_z": candidate.get("entry_volume_z"),
    }


def make_equity_row(
    allocation: AllocationSpec,
    long_track: TrackSpec | None,
    short_track: TrackSpec | None,
    ts_ms: int,
    equity: float,
    realized_pnl: float,
    active_short: int,
    active_long: int,
    concurrent_topup_usd: float,
    reason: str,
) -> dict[str, Any]:
    return {
        "allocation_id": allocation.allocation_id,
        "long_track_id": long_track.track_id if long_track else "",
        "short_track_id": short_track.track_id if short_track else "",
        "ts": ts_ms,
        "iso": ms_to_iso(ts_ms),
        "equity_usd": round_float(equity),
        "realized_pnl_usd": round_float(realized_pnl),
        "active_short": active_short,
        "active_long": active_long,
        "active_total": active_short + active_long,
        "concurrent_topup_usd": round_float(concurrent_topup_usd),
        "reason": reason,
    }


def active_counts(active: list[dict[str, Any]]) -> tuple[int, int]:
    short_count = sum(1 for item in active if item.get("side") == "short")
    long_count = sum(1 for item in active if item.get("side") == "long")
    return short_count, long_count


def concurrent_topup(active: list[dict[str, Any]]) -> float:
    return sum(to_float(item.get("topup_usd")) or 0.0 for item in active)


def render_html_report(
    summary_rows: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    equity_rows: list[dict[str, Any]],
    starting_capital_usd: float,
) -> str:
    trade_map = group_by_strategy(trade_rows)
    equity_map = group_by_strategy(equity_rows)
    generated = datetime.now(tz=timezone.utc).isoformat()
    return "\n".join(
        [
            "<!DOCTYPE html><html lang='ru'><head><meta charset='utf-8'/>",
            "<title>Pump cycle combined portfolio report</title>",
            "<style>",
            REPORT_CSS,
            "</style></head><body>",
            "<header>",
            "<h1>Pump-cycle: общий капитал для short + long</h1>",
            f"<p>Capital: {money(starting_capital_usd)}. Short leverage: {SHORT_LEVERAGE:.0f}x. Long leverage: 2x. Generated: {esc(generated)}.</p>",
            "<p class='note'>Это исследовательский replay: short trades взяты из текущего dynamic combo report, long trades — из 5m premium/funding outcomes. Rescue/top-up считается отдельно и не добавляется в strategy equity.</p>",
            "</header>",
            "<section><h2>Главные выводы</h2>",
            render_takeaways(summary_rows),
            "</section>",
            "<section><h2>Все комбинации</h2>",
            render_table(summary_rows, SUMMARY_COLUMNS, limit=120),
            "</section>",
            "<section><h2>Selected equity curves</h2>",
            "".join(render_strategy_block(row, trade_map, equity_map) for row in selected),
            "</section>",
            "</body></html>",
        ]
    )


def render_takeaways(summary_rows: list[dict[str, Any]]) -> str:
    clean_combined = find_row(summary_rows, "cycle_6_4s2l", "long_broad", "short_clean_p100_l3")
    short_only = find_row(summary_rows, "short_only_4", "", "short_clean_p100_l3")
    long_only = find_row(summary_rows, "long_only_2", "long_broad", "")
    five_4s1l = find_row(summary_rows, "cycle_5_4s1l", "long_broad", "short_clean_p100_l3")
    five_3s2l = find_row(summary_rows, "cycle_5_3s2l", "long_broad", "short_clean_p100_l3")
    return (
        "<ul>"
        f"<li><strong>6 slots / 4 short + 2 long:</strong> {describe_row(clean_combined)}</li>"
        f"<li><strong>Short-only 4 slots:</strong> {describe_row(short_only)}</li>"
        f"<li><strong>Long-only 2 slots:</strong> {describe_row(long_only)}</li>"
        f"<li><strong>5 slots / 4 short + 1 long:</strong> {describe_row(five_4s1l)}</li>"
        f"<li><strong>5 slots / 3 short + 2 long:</strong> {describe_row(five_3s2l)}</li>"
        "<li><strong>Interpretation:</strong> compare ROI with drawdown and peak top-up. Combined paper is useful because it exposes capital conflicts and top-up overlap before any live decision.</li>"
        "</ul>"
    )


def render_strategy_block(row: dict[str, Any], trade_map: dict[str, list[dict[str, Any]]], equity_map: dict[str, list[dict[str, Any]]]) -> str:
    key = strategy_key(row)
    title = f"{row.get('allocation_id')} / long={row.get('long_track_id') or '-'} / short={row.get('short_track_id') or '-'}"
    return (
        "<article class='strategy'>"
        f"<h3>{esc(title)}</h3>"
        + render_metric_grid(row)
        + render_equity_svg(equity_map.get(key, []))
        + render_table(trade_map.get(key, []), TRADE_COLUMNS, limit=35)
        + "</article>"
    )


def render_metric_grid(row: dict[str, Any]) -> str:
    metrics = [
        ("Trades", row.get("trades")),
        ("Short / Long", f"{row.get('short_trades')} / {row.get('long_trades')}"),
        ("ROI", pct_text(row.get("roi_pct"))),
        ("Risk adj", pct_text(row.get("risk_adjusted_roi_pct"))),
        ("Win", pct_text(row.get("win_pct"))),
        ("Max DD", pct_text(row.get("max_drawdown_pct"))),
        ("Peak top-up", money(row.get("max_concurrent_topup_usd"))),
        ("Worst", money(row.get("worst_trade_usd"))),
    ]
    return "<div class='metrics'>" + "".join(f"<div><span>{esc(k)}</span><strong>{esc(v)}</strong></div>" for k, v in metrics) + "</div>"


def render_equity_svg(points: list[dict[str, Any]]) -> str:
    width = 1120
    height = 290
    pad_l = 72
    pad_r = 24
    pad_t = 20
    pad_b = 42
    clean = [(to_int(row.get("ts")) or 0, to_float(row.get("equity_usd")) or 0.0, to_float(row.get("concurrent_topup_usd")) or 0.0) for row in points if to_int(row.get("ts"))]
    if not clean:
        return f"<svg class='chart' viewBox='0 0 {width} {height}'></svg>"
    min_ts = min(ts for ts, _, _ in clean)
    max_ts = max(ts for ts, _, _ in clean)
    min_y = min(value for _, value, _ in clean)
    max_y = max(value for _, value, _ in clean)
    span_y = max(1.0, max_y - min_y)
    max_topup = max(topup for _, _, topup in clean) or 1.0

    def x(ts_ms: int) -> float:
        return pad_l + (ts_ms - min_ts) / max(1, max_ts - min_ts) * (width - pad_l - pad_r)

    def y(value: float) -> float:
        return pad_t + (max_y - value) / span_y * (height - pad_t - pad_b)

    poly = " ".join(f"{x(ts):.2f},{y(value):.2f}" for ts, value, _ in clean)
    bars = []
    for ts, _, topup in clean:
        if topup <= 0:
            continue
        bx = x(ts)
        bh = max(3.0, topup / max_topup * 58.0)
        bars.append(f"<rect x='{bx-1.5:.2f}' y='{height-pad_b-bh:.2f}' width='3' height='{bh:.2f}' class='topup-bar'/>")
    return (
        f"<svg class='chart' viewBox='0 0 {width} {height}' role='img'>"
        f"<line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' class='axis'/>"
        f"<line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' class='axis'/>"
        f"<text x='8' y='{y(max_y)+4:.2f}' class='axis-label'>{money(max_y)}</text>"
        f"<text x='8' y='{y(min_y)+4:.2f}' class='axis-label'>{money(min_y)}</text>"
        f"<polyline points='{poly}' class='equity-line'/>"
        + "".join(bars)
        + f"<text x='{pad_l}' y='{height-12}' class='axis-label'>{short_date(min_ts)}</text>"
        + f"<text x='{width-145}' y='{height-12}' class='axis-label'>{short_date(max_ts)}</text>"
        + "<text x='88' y='17' class='axis-label'>green equity, red bars = concurrent short top-up estimate</text>"
        + "</svg>"
    )


def select_report_rows(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for row in summary_rows:
        if row.get("allocation_id") in {"cycle_6_4s2l", "cycle_5_4s1l", "cycle_5_3s2l", "short_only_4", "long_only_2"}:
            selected[strategy_key(row)] = row
        if len(selected) >= 40:
            break
    return sorted(selected.values(), key=summary_sort_key, reverse=True)[:36]


def render_table(rows: list[dict[str, Any]], columns: tuple[tuple[str, str], ...], *, limit: int) -> str:
    if not rows:
        return "<p class='note'>Нет строк.</p>"
    out = ["<div class='table-wrap'><table><thead><tr>"]
    out.extend(f"<th>{esc(label)}</th>" for _, label in columns)
    out.append("</tr></thead><tbody>")
    for row in rows[:limit]:
        out.append("<tr>")
        for key, _ in columns:
            out.append(f"<td>{format_cell(row.get(key), key)}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    if len(rows) > limit:
        out.append(f"<p class='note'>Показано {limit} из {len(rows)}. Полные данные в CSV.</p>")
    return "".join(out)


def find_row(summary_rows: list[dict[str, Any]], allocation_id: str, long_track_id: str, short_track_id: str) -> dict[str, Any] | None:
    return next(
        (
            row
            for row in summary_rows
            if row.get("allocation_id") == allocation_id
            and row.get("long_track_id") == long_track_id
            and row.get("short_track_id") == short_track_id
            and row.get("sizing_mode") == "split_initial"
        ),
        None,
    )


def describe_row(row: dict[str, Any] | None) -> str:
    if not row:
        return "нет строки"
    return (
        f"{row.get('trades')} trades ({row.get('short_trades')} short / {row.get('long_trades')} long), "
        f"ROI {pct_text(row.get('roi_pct'))}, risk-adjusted {pct_text(row.get('risk_adjusted_roi_pct'))}, "
        f"max DD {pct_text(row.get('max_drawdown_pct'))}, peak top-up {money(row.get('max_concurrent_topup_usd'))}"
    )


def group_by_strategy(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(strategy_key(row), []).append(row)
    return out


def strategy_key(row: dict[str, Any]) -> str:
    return "|".join(str(row.get(key) or "") for key in ("allocation_id", "long_track_id", "short_track_id"))


def summary_sort_key(row: dict[str, Any]) -> tuple[float, float, int]:
    return (
        to_float(row.get("risk_adjusted_roi_pct")) or -10**9,
        to_float(row.get("roi_pct")) or -10**9,
        to_int(row.get("trades")) or 0,
    )


def candidate_sort_key(row: dict[str, Any]) -> tuple[int, str, str]:
    return (to_int(row.get("entry_ts")) or 0, str(row.get("side") or ""), str(row.get("symbol") or ""))


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
    if key and key.endswith("_usd"):
        return esc(money(value))
    if key and key.endswith("_pct"):
        return esc(pct_text(value))
    if key and key.endswith("_iso"):
        return esc(str(value)[:16].replace("T", " "))
    return esc(value)


SUMMARY_COLUMNS = (
    ("allocation_id", "Allocation"),
    ("long_track_id", "Long"),
    ("short_track_id", "Short"),
    ("total_slots", "Slots"),
    ("short_slots", "S"),
    ("long_slots", "L"),
    ("trades", "Trades"),
    ("short_trades", "Short trades"),
    ("long_trades", "Long trades"),
    ("roi_pct", "ROI"),
    ("risk_adjusted_roi_pct", "Risk adj"),
    ("max_drawdown_pct", "Max DD"),
    ("max_concurrent_topup_usd", "Peak top-up"),
    ("worst_trade_usd", "Worst"),
    ("skipped_short_slots", "Skip S"),
    ("skipped_long_slots", "Skip L"),
    ("skipped_conflict_symbol", "Conflict"),
)

TRADE_COLUMNS = (
    ("side", "Side"),
    ("track_id", "Track"),
    ("entry_iso", "Entry"),
    ("exit_iso", "Exit"),
    ("symbol", "Coin"),
    ("exit_reason", "Exit"),
    ("slot_budget_usd", "Budget"),
    ("leverage", "Lev"),
    ("levered_net_pct", "Net"),
    ("pnl_usd", "PnL"),
    ("topup_usd", "Top-up"),
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
.strategy { background: #151b27; border: 1px solid #283242; border-radius: 14px; padding: 18px; margin: 18px 0; }
.metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); gap: 10px; margin: 12px 0; }
.metrics div { background: #0f1520; border: 1px solid #273245; border-radius: 10px; padding: 10px; }
.metrics span { display: block; color: #8fa0b5; font-size: 12px; }
.metrics strong { display: block; color: #f7fafc; font-size: 17px; margin-top: 4px; }
.chart { width: 100%; height: auto; background: #0f1520; border: 1px solid #273245; border-radius: 10px; margin: 12px 0; }
.axis { stroke: #607086; stroke-width: 1; }
.axis-label { fill: #9aa8bb; font-size: 12px; }
.equity-line { fill: none; stroke: #22c55e; stroke-width: 2.5; }
.topup-bar { fill: rgba(239, 68, 68, 0.75); }
li { margin: 8px 0; }
"""


__all__ = [
    "ALLOCATIONS",
    "LONG_TRACKS",
    "SHORT_TRACKS",
    "AllocationSpec",
    "TrackSpec",
    "replay_cycle",
    "run_pump_cycle_portfolio_report",
]
