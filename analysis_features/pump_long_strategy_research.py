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
from typing import Any, Iterable

from analysis_features.bybit_pump_short_outcomes import (
    FEE_ROUNDTRIP_PCT,
    PumpEvent,
    Series,
    detect_pump_events,
    funding_sum_pct,
    load_samples,
    safe_max,
    safe_min,
    to_float,
    write_csv,
)
from analysis_features.pump_lifecycle_research import (
    DEFAULT_INPUT,
    ResearchSeries,
    lifecycle_features,
    lifecycle_scores,
    sample_to_research_series,
)
from config import BASE_DIR

DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_long_strategy_research"
HOUR_MS = 3_600_000
ENTRY_SEARCH_H = 24
LEVERAGE = 2.0


@dataclass(frozen=True, slots=True)
class LongEntryRule:
    slug: str
    max_wait_h: int
    min_continuation_score: float
    max_exhaustion_score: float
    min_return_1h_pct: float | None = None
    min_volume_z: float | None = None
    max_funding_8h_pct: float | None = None
    min_oi_6h_pct: float | None = None
    min_pullback_pct: float | None = None
    max_pullback_pct: float | None = None


@dataclass(frozen=True, slots=True)
class LongExitPlan:
    slug: str
    tp_pct: float
    stop_pct: float
    max_hold_h: int


ENTRY_RULES: tuple[LongEntryRule, ...] = (
    LongEntryRule("score70_any", 12, 70.0, 60.0),
    LongEntryRule("score85_strong", 12, 85.0, 60.0),
    LongEntryRule("breakout_volume_z3", 12, 70.0, 65.0, min_return_1h_pct=5.0, min_volume_z=3.0),
    LongEntryRule("funding_squeeze_oi", 18, 60.0, 65.0, max_funding_8h_pct=-0.05, min_oi_6h_pct=30.0),
    LongEntryRule("controlled_retest", 24, 50.0, 60.0, min_return_1h_pct=0.0, min_pullback_pct=5.0, max_pullback_pct=20.0),
)

EXIT_PLANS: tuple[LongExitPlan, ...] = (
    LongExitPlan("tp20_sl8_hold6", 20.0, 8.0, 6),
    LongExitPlan("tp30_sl10_hold12", 30.0, 10.0, 12),
    LongExitPlan("tp40_sl12_hold24", 40.0, 12.0, 24),
    LongExitPlan("tp60_sl15_hold48", 60.0, 15.0, 48),
)


def run_long_strategy_research(
    *,
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    max_events: int | None = None,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    samples = list(load_samples(input_path))
    series_by_symbol = {str(sample.get("symbol") or "").upper(): sample_to_research_series(sample) for sample in samples}
    btc_series = series_by_symbol.get("BTCUSDT")

    outcome_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    events_seen = 0
    symbols_seen = 0
    for research_series in series_by_symbol.values():
        symbols_seen += 1
        series = research_series.base
        for event in detect_pump_events(series):
            if max_events is not None and events_seen >= max_events:
                break
            events_seen += 1
            event_outcomes = simulate_event_long_strategies(research_series, event, btc_series=btc_series)
            outcome_rows.extend(event_outcomes)
            event_rows.append(build_event_row(research_series, event, event_outcomes))
        if max_events is not None and events_seen >= max_events:
            break

    summary_rows = build_strategy_summary(outcome_rows)
    portfolio_rows, portfolio_trade_rows = replay_long_portfolios(outcome_rows)
    regression_rows = build_long_regression_diagnostics(outcome_rows)
    factor_bucket_rows = build_factor_bucket_summary(outcome_rows, summary_rows[:1])
    write_csv(output_dir / "long_event_summary.csv", event_rows)
    write_csv(output_dir / "long_candidate_outcomes.csv", outcome_rows)
    write_csv(output_dir / "long_strategy_summary.csv", summary_rows)
    write_csv(output_dir / "long_portfolio_summary.csv", portfolio_rows)
    write_csv(output_dir / "long_portfolio_trades.csv", portfolio_trade_rows)
    write_csv(output_dir / "long_feature_regression.csv", regression_rows)
    write_csv(output_dir / "long_factor_bucket_summary.csv", factor_bucket_rows)
    (output_dir / "index.html").write_text(
        render_index(
            event_rows=event_rows,
            summary_rows=summary_rows,
            portfolio_rows=portfolio_rows,
            regression_rows=regression_rows,
            factor_bucket_rows=factor_bucket_rows,
        ),
        encoding="utf-8",
    )

    metadata = {
        "schema": "pump_long_strategy_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "symbols_seen": symbols_seen,
        "events": len(event_rows),
        "outcome_rows": len(outcome_rows),
        "portfolio_rows": len(portfolio_rows),
        "portfolio_trade_rows": len(portfolio_trade_rows),
        "regression_rows": len(regression_rows),
        "factor_bucket_rows": len(factor_bucket_rows),
        "entry_rules": [rule.slug for rule in ENTRY_RULES],
        "exit_plans": [plan.slug for plan in EXIT_PLANS],
        "leverage": LEVERAGE,
        "has_btc_context": btc_series is not None,
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return metadata


def simulate_event_long_strategies(
    research_series: ResearchSeries,
    event: PumpEvent,
    *,
    btc_series: ResearchSeries | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry_rule in ENTRY_RULES:
        entry = find_long_entry(research_series, event, entry_rule, btc_series=btc_series)
        if entry is None:
            continue
        for exit_plan in EXIT_PLANS:
            outcome = simulate_long_exit(research_series.base, event, entry, entry_rule, exit_plan)
            if outcome:
                rows.append(outcome)
    return rows


def find_long_entry(
    research_series: ResearchSeries,
    event: PumpEvent,
    rule: LongEntryRule,
    *,
    btc_series: ResearchSeries | None,
) -> dict[str, Any] | None:
    series = research_series.base
    end_idx = min(len(series.ts) - 1, event.trigger_idx + min(rule.max_wait_h, ENTRY_SEARCH_H))
    for idx in range(event.trigger_idx, end_idx + 1):
        features = lifecycle_features(research_series, idx, event, btc_series=btc_series)
        scores = lifecycle_scores(features)
        if not long_entry_matches(features, scores, rule):
            continue
        return {
            "idx": idx,
            "ts_ms": series.ts[idx],
            "price": series.close[idx],
            "features": features,
            "scores": scores,
        }
    return None


def long_entry_matches(features: dict[str, Any], scores: dict[str, Any], rule: LongEntryRule) -> bool:
    continuation = to_float(scores.get("squeeze_continuation_score"))
    exhaustion = to_float(scores.get("pump_exhaustion_score"))
    if continuation is None or continuation < rule.min_continuation_score:
        return False
    if exhaustion is not None and exhaustion > rule.max_exhaustion_score:
        return False
    if rule.min_return_1h_pct is not None and ((to_float(features.get("return_1h_pct")) or -10**9) < rule.min_return_1h_pct):
        return False
    volume_z = max_known(features.get("volume_z_24h"), features.get("volume_z_168h"))
    if rule.min_volume_z is not None and ((volume_z or -10**9) < rule.min_volume_z):
        return False
    if rule.max_funding_8h_pct is not None:
        funding = to_float(features.get("funding_prev_8h_pct"))
        if funding is None or funding > rule.max_funding_8h_pct:
            return False
    if rule.min_oi_6h_pct is not None and ((to_float(features.get("oi_change_6h_pct")) or -10**9) < rule.min_oi_6h_pct):
        return False
    pullback = to_float(features.get("pullback_from_high_pct"))
    if rule.min_pullback_pct is not None and (pullback is None or pullback < rule.min_pullback_pct):
        return False
    if rule.max_pullback_pct is not None and (pullback is None or pullback > rule.max_pullback_pct):
        return False
    return True


def simulate_long_exit(
    series: Series,
    event: PumpEvent,
    entry: dict[str, Any],
    entry_rule: LongEntryRule,
    exit_plan: LongExitPlan,
) -> dict[str, Any] | None:
    entry_idx = int(entry["idx"])
    entry_price = to_float(entry.get("price"))
    if entry_price in {None, 0}:
        return None
    end_idx = min(len(series.ts) - 1, entry_idx + exit_plan.max_hold_h)
    tp_price = entry_price * (1.0 + exit_plan.tp_pct / 100.0)
    stop_price = entry_price * (1.0 - exit_plan.stop_pct / 100.0)
    exit_idx = end_idx
    exit_price = series.close[end_idx]
    exit_reason = "time_stop"
    for idx in range(entry_idx + 1, end_idx + 1):
        low = series.low[idx]
        high = series.high[idx]
        if low is not None and low <= stop_price:
            exit_idx = idx
            exit_price = stop_price
            exit_reason = "stop_loss"
            break
        if high is not None and high >= tp_price:
            exit_idx = idx
            exit_price = tp_price
            exit_reason = "take_profit"
            break
    if exit_price is None:
        return None

    high_during = safe_max(series.high[entry_idx : exit_idx + 1])
    low_during = safe_min(series.low[entry_idx : exit_idx + 1])
    gross_price_pct = (exit_price / entry_price - 1.0) * 100.0
    funding_pct = funding_sum_pct(series.funding, series.ts[entry_idx], series.ts[exit_idx]) or 0.0
    long_funding_pct = -funding_pct
    net_pct = gross_price_pct + long_funding_pct - FEE_ROUNDTRIP_PCT
    mfe_pct = (high_during / entry_price - 1.0) * 100.0 if high_during is not None else None
    mae_pct = (1.0 - low_during / entry_price) * 100.0 if low_during is not None else None
    features = entry["features"]
    scores = entry["scores"]
    return {
        "event_id": event.event_id,
        "symbol": event.symbol,
        "trigger_ts": event.trigger_ts,
        "trigger_iso": ms_to_iso(event.trigger_ts),
        "trigger_pump_pct": round_float(event.pump_pct),
        "entry_rule": entry_rule.slug,
        "exit_plan": exit_plan.slug,
        "entry_ts": series.ts[entry_idx],
        "entry_iso": ms_to_iso(series.ts[entry_idx]),
        "entry_wait_h": entry_idx - event.trigger_idx,
        "entry_price": round_float(entry_price),
        "exit_ts": series.ts[exit_idx],
        "exit_iso": ms_to_iso(series.ts[exit_idx]),
        "exit_h": exit_idx - entry_idx,
        "exit_price": round_float(exit_price),
        "exit_reason": exit_reason,
        "gross_price_pct": round_float(gross_price_pct),
        "long_funding_pct": round_float(long_funding_pct),
        "net_pct": round_float(net_pct),
        "levered_net_pct": round_float(net_pct * LEVERAGE),
        "mfe_pct": round_float(mfe_pct),
        "mae_pct": round_float(mae_pct),
        "continuation_score": scores.get("squeeze_continuation_score"),
        "exhaustion_score": scores.get("pump_exhaustion_score"),
        "funding_prev_8h_pct": features.get("funding_prev_8h_pct"),
        "oi_change_6h_pct": features.get("oi_change_6h_pct"),
        "volume_z_24h": features.get("volume_z_24h"),
        "btc_relative_1h_pct": features.get("btc_relative_1h_pct"),
        "pullback_from_high_pct": features.get("pullback_from_high_pct"),
    }


def build_event_row(research_series: ResearchSeries, event: PumpEvent, outcomes: list[dict[str, Any]]) -> dict[str, Any]:
    series = research_series.base
    end_idx = min(len(series.ts) - 1, event.trigger_idx + 168)
    future_high = safe_max(series.high[event.trigger_idx : end_idx + 1])
    future_low = safe_min(series.low[event.trigger_idx : end_idx + 1])
    best = max(outcomes, key=lambda row: to_float(row.get("net_pct")) or -10**9) if outcomes else {}
    return {
        "event_id": event.event_id,
        "symbol": event.symbol,
        "trigger_ts": event.trigger_ts,
        "trigger_iso": ms_to_iso(event.trigger_ts),
        "trigger_pump_pct": round_float(event.pump_pct),
        "candidate_outcomes": len(outcomes),
        "best_entry_rule": best.get("entry_rule"),
        "best_exit_plan": best.get("exit_plan"),
        "best_net_pct": best.get("net_pct"),
        "best_exit_reason": best.get("exit_reason"),
        "future_high_168h_pct": round_float((future_high / event.trigger_close - 1.0) * 100.0 if future_high else None),
        "future_low_168h_pct": round_float((future_low / event.trigger_close - 1.0) * 100.0 if future_low else None),
    }


def build_strategy_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((str(row.get("entry_rule")), str(row.get("exit_plan"))), []).append(row)
    summary: list[dict[str, Any]] = []
    for (entry_rule, exit_plan), items in sorted(groups.items()):
        nets = values(items, "net_pct")
        maes = values(items, "mae_pct")
        mfes = values(items, "mfe_pct")
        summary.append(
            {
                "entry_rule": entry_rule,
                "exit_plan": exit_plan,
                "trades": len(items),
                "win_pct": pct(sum(1 for value in nets if value > 0), len(nets)),
                "avg_net_pct": round_float(statistics.mean(nets) if nets else None),
                "median_net_pct": round_float(statistics.median(nets) if nets else None),
                "p10_net_pct": percentile(nets, 10),
                "p90_mae_pct": percentile(maes, 90),
                "median_mfe_pct": round_float(statistics.median(mfes) if mfes else None),
                "take_profit_pct": pct(sum(1 for row in items if row.get("exit_reason") == "take_profit"), len(items)),
                "stop_loss_pct": pct(sum(1 for row in items if row.get("exit_reason") == "stop_loss"), len(items)),
                "time_stop_pct": pct(sum(1 for row in items if row.get("exit_reason") == "time_stop"), len(items)),
                "avg_entry_wait_h": round_float(mean(values(items, "entry_wait_h"))),
                "avg_hold_h": round_float(mean(values(items, "exit_h"))),
                "score": round_float(long_strategy_score(items)),
            }
        )
    summary.sort(key=lambda row: to_float(row.get("score")) or -10**9, reverse=True)
    return summary


def replay_long_portfolios(
    rows: list[dict[str, Any]],
    *,
    starting_capital_usd: float = 3000.0,
    slot_counts: Iterable[int] = (1, 2, 3, 4, 5),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((str(row.get("entry_rule")), str(row.get("exit_plan"))), []).append(row)

    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    for (entry_rule, exit_plan), items in sorted(groups.items()):
        ordered = sorted(
            items,
            key=lambda row: (
                int(to_float(row.get("entry_ts")) or 0),
                str(row.get("symbol") or ""),
                str(row.get("event_id") or ""),
            ),
        )
        for slots in slot_counts:
            summary, trades = replay_one_long_portfolio(
                ordered,
                entry_rule=entry_rule,
                exit_plan=exit_plan,
                starting_capital_usd=starting_capital_usd,
                slots=int(slots),
            )
            summary_rows.append(summary)
            trade_rows.extend(trades)

    summary_rows.sort(
        key=lambda row: (
            to_float(row.get("risk_adjusted_roi_pct")) or -10**9,
            to_float(row.get("roi_pct")) or -10**9,
        ),
        reverse=True,
    )
    return summary_rows, trade_rows


def replay_one_long_portfolio(
    rows: list[dict[str, Any]],
    *,
    entry_rule: str,
    exit_plan: str,
    starting_capital_usd: float,
    slots: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    slot_budget = starting_capital_usd / max(1, slots)
    active: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    realized_pnl = 0.0
    peak_equity = starting_capital_usd
    max_drawdown = 0.0
    skipped_slots = 0
    skipped_same_symbol = 0
    worst_trade_usd: float | None = None
    worst_trade_pct: float | None = None

    def release_until(ts_ms: int) -> None:
        nonlocal active, realized_pnl, peak_equity, max_drawdown
        closed = [trade for trade in active if int(to_float(trade.get("exit_ts")) or 0) <= ts_ms]
        active = [trade for trade in active if int(to_float(trade.get("exit_ts")) or 0) > ts_ms]
        for trade in sorted(closed, key=lambda item: int(to_float(item.get("exit_ts")) or 0)):
            realized_pnl += float(trade["pnl_usd"])
            equity = starting_capital_usd + realized_pnl
            peak_equity = max(peak_equity, equity)
            max_drawdown = max(max_drawdown, peak_equity - equity)

    for row in rows:
        entry_ts = int(to_float(row.get("entry_ts")) or 0)
        exit_ts = int(to_float(row.get("exit_ts")) or 0)
        if entry_ts <= 0 or exit_ts <= entry_ts:
            continue
        release_until(entry_ts)
        symbol = str(row.get("symbol") or "")
        if any(str(item.get("symbol") or "") == symbol for item in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= slots:
            skipped_slots += 1
            continue
        levered_net_pct = to_float(row.get("levered_net_pct"))
        if levered_net_pct is None:
            continue
        pnl_usd = slot_budget * levered_net_pct / 100.0
        worst_trade_usd = pnl_usd if worst_trade_usd is None else min(worst_trade_usd, pnl_usd)
        worst_trade_pct = levered_net_pct if worst_trade_pct is None else min(worst_trade_pct, levered_net_pct)
        trade = {
            "entry_rule": entry_rule,
            "exit_plan": exit_plan,
            "slots": slots,
            "slot_budget_usd": round_float(slot_budget),
            "symbol": symbol,
            "event_id": row.get("event_id"),
            "entry_ts": entry_ts,
            "entry_iso": row.get("entry_iso"),
            "exit_ts": exit_ts,
            "exit_iso": row.get("exit_iso"),
            "exit_reason": row.get("exit_reason"),
            "net_pct": row.get("net_pct"),
            "levered_net_pct": row.get("levered_net_pct"),
            "pnl_usd": round_float(pnl_usd),
            "mae_pct": row.get("mae_pct"),
            "mfe_pct": row.get("mfe_pct"),
            "trigger_pump_pct": row.get("trigger_pump_pct"),
            "continuation_score": row.get("continuation_score"),
            "exhaustion_score": row.get("exhaustion_score"),
            "volume_z_24h": row.get("volume_z_24h"),
            "oi_change_6h_pct": row.get("oi_change_6h_pct"),
            "funding_prev_8h_pct": row.get("funding_prev_8h_pct"),
        }
        active.append(trade)
        trades.append(trade)

    release_until(10**18)
    final_equity = starting_capital_usd + realized_pnl
    roi_pct = (final_equity / starting_capital_usd - 1.0) * 100.0
    wins = sum(1 for trade in trades if (to_float(trade.get("pnl_usd")) or 0.0) > 0.0)
    risk_adjusted_roi = roi_pct - max_drawdown / starting_capital_usd * 100.0 - max(0.0, -(worst_trade_usd or 0.0)) / starting_capital_usd * 50.0
    summary = {
        "entry_rule": entry_rule,
        "exit_plan": exit_plan,
        "slots": slots,
        "starting_capital_usd": round_float(starting_capital_usd),
        "slot_budget_usd": round_float(slot_budget),
        "trades": len(trades),
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
        "win_pct": pct(wins, len(trades)),
        "final_equity_usd": round_float(final_equity),
        "realized_pnl_usd": round_float(realized_pnl),
        "roi_pct": round_float(roi_pct),
        "risk_adjusted_roi_pct": round_float(risk_adjusted_roi),
        "max_drawdown_usd": round_float(max_drawdown),
        "max_drawdown_pct": round_float(max_drawdown / starting_capital_usd * 100.0),
        "worst_trade_usd": round_float(worst_trade_usd),
        "worst_trade_pct": round_float(worst_trade_pct),
        "avg_trade_usd": round_float(mean(values(trades, "pnl_usd"))),
        "take_profit_pct": pct(sum(1 for row in trades if row.get("exit_reason") == "take_profit"), len(trades)),
        "stop_loss_pct": pct(sum(1 for row in trades if row.get("exit_reason") == "stop_loss"), len(trades)),
    }
    return summary, trades


def build_long_regression_diagnostics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidate_features = [
        "trigger_pump_pct",
        "entry_wait_h",
        "continuation_score",
        "exhaustion_score",
        "funding_prev_8h_pct",
        "oi_change_6h_pct",
        "volume_z_24h",
        "btc_relative_1h_pct",
        "pullback_from_high_pct",
    ]
    target_specs = [
        ("net_pct", lambda row: to_float(row.get("net_pct"))),
        ("mae_pct", lambda row: to_float(row.get("mae_pct"))),
        ("win_label", lambda row: 1.0 if (to_float(row.get("net_pct")) or 0.0) > 0.0 else 0.0),
        ("stop_label", lambda row: 1.0 if row.get("exit_reason") == "stop_loss" else 0.0),
    ]
    groups: dict[str, list[dict[str, Any]]] = {"all": rows}
    for row in rows:
        groups.setdefault(str(row.get("exit_plan") or ""), []).append(row)

    out: list[dict[str, Any]] = []
    for group_name, group_rows in sorted(groups.items()):
        if len(group_rows) < 20:
            continue
        min_feature_count = max(20, int(len(group_rows) * 0.5))
        features = [
            feature
            for feature in candidate_features
            if sum(1 for row in group_rows if to_float(row.get(feature)) is not None) >= min_feature_count
        ]
        if not features:
            continue
        for target, getter in target_specs:
            model_rows: list[dict[str, Any]] = []
            y: list[float] = []
            xs: list[list[float]] = []
            for row in group_rows:
                target_value = getter(row)
                if target_value is None:
                    continue
                feature_values = [to_float(row.get(feature)) for feature in features]
                if any(value is None for value in feature_values):
                    continue
                model_rows.append(row)
                y.append(float(target_value))
                xs.append([float(value) for value in feature_values if value is not None])
            if len(model_rows) < 20:
                continue
            model = standardized_ridge(xs, y, alpha=1.0)
            for feature, coefficient in zip(features, model["coefficients"]):
                out.append(
                    {
                        "group": group_name,
                        "target": target,
                        "feature": feature,
                        "n": len(model_rows),
                        "standardized_coefficient": round_float(coefficient),
                        "abs_coefficient": round_float(abs(coefficient)),
                        "r2": round_float(model["r2"]),
                        "target_mean": round_float(statistics.mean(y)),
                        "interpretation": long_regression_note(target, feature, coefficient),
                    }
                )
    out.sort(key=lambda row: (str(row.get("group")), str(row.get("target")), -(to_float(row.get("abs_coefficient")) or 0.0)))
    return out


def build_factor_bucket_summary(rows: list[dict[str, Any]], top_summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not top_summary_rows:
        return []
    top = top_summary_rows[0]
    filtered = [
        row
        for row in rows
        if row.get("entry_rule") == top.get("entry_rule") and row.get("exit_plan") == top.get("exit_plan")
    ]
    out: list[dict[str, Any]] = []
    for feature in [
        "trigger_pump_pct",
        "continuation_score",
        "exhaustion_score",
        "volume_z_24h",
        "oi_change_6h_pct",
        "funding_prev_8h_pct",
        "pullback_from_high_pct",
    ]:
        pairs = [(to_float(row.get(feature)), row) for row in filtered]
        clean = [(value, row) for value, row in pairs if value is not None]
        if len(clean) < 20:
            continue
        clean.sort(key=lambda item: item[0])
        chunk_size = max(1, math.ceil(len(clean) / 3))
        low_chunk = clean[:chunk_size]
        mid_chunk = clean[chunk_size : chunk_size * 2]
        high_chunk = clean[chunk_size * 2 :]
        q33 = low_chunk[-1][0] if low_chunk else None
        q66 = mid_chunk[-1][0] if mid_chunk else q33
        buckets = [
            ("low", [row for _value, row in low_chunk]),
            ("mid", [row for _value, row in mid_chunk]),
            ("high", [row for _value, row in high_chunk]),
        ]
        for bucket, items in buckets:
            nets = values(items, "net_pct")
            bucket_values = values(items, feature)
            out.append(
                {
                    "entry_rule": top.get("entry_rule"),
                    "exit_plan": top.get("exit_plan"),
                    "feature": feature,
                    "bucket": bucket,
                    "bucket_min": round_float(min(bucket_values) if bucket_values else None),
                    "bucket_max": round_float(max(bucket_values) if bucket_values else None),
                    "threshold_low": round_float(q33),
                    "threshold_high": round_float(q66),
                    "trades": len(items),
                    "win_pct": pct(sum(1 for value in nets if value > 0), len(nets)),
                    "avg_net_pct": round_float(mean(nets)),
                    "median_net_pct": round_float(statistics.median(nets) if nets else None),
                    "p90_mae_pct": percentile(values(items, "mae_pct"), 90),
                    "stop_loss_pct": pct(sum(1 for row in items if row.get("exit_reason") == "stop_loss"), len(items)),
                }
            )
    out.sort(key=lambda row: (str(row.get("feature")), str(row.get("bucket"))))
    return out


def long_strategy_score(rows: list[dict[str, Any]]) -> float:
    nets = values(rows, "net_pct")
    maes = values(rows, "mae_pct")
    if not nets:
        return -10**9
    avg_net = statistics.mean(nets)
    win = sum(1 for value in nets if value > 0) / len(nets) * 100.0
    p90_mae = percentile(maes, 90) or 0.0
    stop_rate = sum(1 for row in rows if row.get("exit_reason") == "stop_loss") / len(rows) * 100.0
    return avg_net + win * 0.08 - p90_mae * 0.15 - stop_rate * 0.08


def render_index(
    *,
    event_rows: list[dict[str, Any]],
    summary_rows: list[dict[str, Any]],
    portfolio_rows: list[dict[str, Any]],
    regression_rows: list[dict[str, Any]],
    factor_bucket_rows: list[dict[str, Any]],
) -> str:
    content = f"""
    <h1>Pump long strategy research</h1>
    <p>Initial Bybit 1h long-entry/exit sweep using lifecycle continuation signals. This is a research report, not an auto-live strategy.</p>
    <section><h2>Top Strategy Rows</h2>{html_table(summary_rows[:80])}</section>
    <section><h2>Top Portfolio Replays</h2>{html_table(portfolio_rows[:80])}</section>
    <section><h2>Regression Diagnostics</h2>{html_table(regression_rows[:160])}</section>
    <section><h2>Top Strategy Factor Buckets</h2>{html_table(factor_bucket_rows[:120])}</section>
    <section><h2>Event Summary</h2>{html_table(event_rows[:120])}</section>
    """
    return page_shell("Pump long strategy research", content)


def html_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    head = "".join(f"<th>{html.escape(str(column))}</th>" for column in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def page_shell(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2933; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
    th, td {{ border: 1px solid #d7dde5; padding: 4px 6px; text-align: left; }}
    th {{ background: #eef2f7; position: sticky; top: 0; }}
    section {{ margin: 24px 0; }}
  </style>
</head>
<body>{body}</body>
</html>"""


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def max_known(*items: Any) -> float | None:
    values_out = [to_float(item) for item in items]
    clean = [item for item in values_out if item is not None]
    return max(clean) if clean else None


def values(rows: Iterable[dict[str, Any]], key: str) -> list[float]:
    out = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None:
            out.append(value)
    return out


def mean(items: list[float]) -> float | None:
    return statistics.mean(items) if items else None


def percentile(items: list[float], q: float) -> float | None:
    if not items:
        return None
    sorted_items = sorted(items)
    idx = int(round((q / 100.0) * (len(sorted_items) - 1)))
    return round_float(sorted_items[idx])


def pct(count: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round_float(count / total * 100.0)


def standardized_ridge(xs: list[list[float]], y: list[float], *, alpha: float) -> dict[str, Any]:
    if not xs:
        return {"coefficients": [], "r2": 0.0}
    cols = len(xs[0])
    means = [statistics.mean(row[idx] for row in xs) for idx in range(cols)]
    stdevs = [statistics.pstdev(row[idx] for row in xs) or 1.0 for idx in range(cols)]
    y_mean = statistics.mean(y)
    y_stdev = statistics.pstdev(y) or 1.0
    zxs = [[(row[idx] - means[idx]) / stdevs[idx] for idx in range(cols)] for row in xs]
    zy = [(value - y_mean) / y_stdev for value in y]
    xtx = [[0.0 for _ in range(cols)] for _ in range(cols)]
    xty = [0.0 for _ in range(cols)]
    for row, target in zip(zxs, zy):
        for i in range(cols):
            xty[i] += row[i] * target
            for j in range(cols):
                xtx[i][j] += row[i] * row[j]
    for i in range(cols):
        xtx[i][i] += alpha
    coefficients = solve_linear_system(xtx, xty)
    preds = [sum(coefficients[idx] * row[idx] for idx in range(cols)) for row in zxs]
    sse = sum((target - pred) ** 2 for target, pred in zip(zy, preds))
    sst = sum((target - statistics.mean(zy)) ** 2 for target in zy) or 1.0
    return {"coefficients": coefficients, "r2": max(-1.0, 1.0 - sse / sst)}


def solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    n = len(vector)
    aug = [row[:] + [vector[idx]] for idx, row in enumerate(matrix)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(aug[row][col]))
        if abs(aug[pivot][col]) < 1e-12:
            continue
        aug[col], aug[pivot] = aug[pivot], aug[col]
        div = aug[col][col]
        aug[col] = [value / div for value in aug[col]]
        for row in range(n):
            if row == col:
                continue
            factor = aug[row][col]
            if abs(factor) < 1e-12:
                continue
            aug[row] = [value - factor * aug[col][idx] for idx, value in enumerate(aug[row])]
    return [aug[row][-1] for row in range(n)]


def long_regression_note(target: str, feature: str, coefficient: float) -> str:
    direction = "raises" if coefficient > 0 else "lowers"
    if target == "net_pct":
        return f"higher {feature} {direction} expected long result"
    if target == "mae_pct":
        return f"higher {feature} {direction} adverse excursion"
    if target == "win_label":
        return f"higher {feature} {direction} win probability"
    if target == "stop_label":
        return f"higher {feature} {direction} stop-loss probability"
    return f"higher {feature} {direction} {target}"


def round_float(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    value = float(value)
    return round(value, digits) if math.isfinite(value) else None


def ms_to_iso(ts_ms: int | None) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


__all__ = [
    "ENTRY_RULES",
    "EXIT_PLANS",
    "LongEntryRule",
    "LongExitPlan",
    "build_long_regression_diagnostics",
    "replay_long_portfolios",
    "find_long_entry",
    "long_entry_matches",
    "run_long_strategy_research",
    "simulate_long_exit",
]
