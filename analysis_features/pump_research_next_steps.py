from __future__ import annotations

import csv
import html
import json
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis_collectors.bybit_event_window import HOUR_MS
from analysis_collectors.bybit_pump_short import (
    BybitCollectorConfig,
    BybitPumpShortCollector,
    dedupe_sort_by_ts,
    round_float,
    scale_pct,
    to_float,
    to_int,
)
from analysis_features.bybit_pump_short_outcomes import FEE_ROUNDTRIP_PCT, write_csv
from analysis_features.pump_funding_premium_window_research import (
    ENTRY_RULES,
    FILTER_SPECS,
    PORTFOLIO_LEVERAGE,
    PORTFOLIO_STARTING_CAPITAL_USD,
    PremiumEntryRule,
    build_portfolio_replays,
    filter_matches,
    find_entry,
    primary_interval,
    sum_funding_between,
    value_at_or_before,
)
from config import BASE_DIR

DEFAULT_EVENT_WINDOWS = BASE_DIR / "data" / "research" / "bybit_pump_event_windows_5m_candidates" / "event_windows.jsonl"
DEFAULT_LONG_OUTCOMES = BASE_DIR / "data" / "research" / "pump_funding_premium_window_research_5m_candidates" / "premium_long_outcomes.csv"
DEFAULT_LONG_PORTFOLIO = BASE_DIR / "data" / "research" / "pump_funding_premium_window_research_5m_candidates" / "premium_portfolio_summary.csv"
DEFAULT_CYCLE_SUMMARY = BASE_DIR / "data" / "research" / "pump_cycle_portfolio_report" / "cycle_summary.csv"
DEFAULT_CYCLE_TRADES = BASE_DIR / "data" / "research" / "pump_cycle_portfolio_report" / "cycle_trades.csv"
DEFAULT_SHADOW_HISTORY = BASE_DIR / "data" / "research" / "bybit_pump_short_shadow" / "shadow_scan_history.jsonl"
DEFAULT_ACTIVE_WINDOW = BASE_DIR / "data" / "research" / "bybit_pump_short_shadow" / "pump_active_window_latest.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_research_next_steps"


@dataclass(frozen=True, slots=True)
class ReliefExitPlan:
    slug: str
    tp_pct: float
    stop_pct: float
    max_hold_h: int
    min_hold_h: float = 0.0
    premium_exit_pct: float | None = None
    premium_relief_from_entry_pct: float | None = None
    min_profit_for_relief_pct: float = 0.0
    oi_fade_from_entry_pct: float | None = None
    funding_rate_exit_pct: float | None = None


@dataclass(frozen=True, slots=True)
class FundingFirstRule:
    slug: str
    max_wait_h: int
    max_premium_pct: float
    max_prev_funding_pct: float
    min_return_1h_pct: float
    min_oi_change_4h_pct: float | None = None
    min_volume_z: float | None = None


RELIEF_EXIT_PLANS: tuple[ReliefExitPlan, ...] = (
    ReliefExitPlan("tp30_sl25_hold72_base", tp_pct=30.0, stop_pct=25.0, max_hold_h=72),
    ReliefExitPlan(
        "tp30_sl25_hold72_premium_relief_m05",
        tp_pct=30.0,
        stop_pct=25.0,
        max_hold_h=72,
        min_hold_h=0.5,
        premium_exit_pct=-0.5,
        min_profit_for_relief_pct=5.0,
    ),
    ReliefExitPlan(
        "tp30_sl25_hold72_relief_from_entry_1p0",
        tp_pct=30.0,
        stop_pct=25.0,
        max_hold_h=72,
        min_hold_h=0.5,
        premium_relief_from_entry_pct=1.0,
        min_profit_for_relief_pct=5.0,
    ),
    ReliefExitPlan(
        "tp30_sl25_hold72_oi_fade10",
        tp_pct=30.0,
        stop_pct=25.0,
        max_hold_h=72,
        min_hold_h=1.0,
        oi_fade_from_entry_pct=-10.0,
        min_profit_for_relief_pct=5.0,
    ),
    ReliefExitPlan(
        "tp30_sl25_hold72_funding_relief",
        tp_pct=30.0,
        stop_pct=25.0,
        max_hold_h=72,
        min_hold_h=1.0,
        funding_rate_exit_pct=-0.05,
        min_profit_for_relief_pct=5.0,
    ),
)

FUNDING_FIRST_RULES: tuple[FundingFirstRule, ...] = (
    FundingFirstRule("funding_discount_survival_m10", 72, max_premium_pct=-1.0, max_prev_funding_pct=-0.10, min_return_1h_pct=-5.0),
    FundingFirstRule("funding_discount_oi_m10", 72, max_premium_pct=-1.0, max_prev_funding_pct=-0.10, min_return_1h_pct=-5.0, min_oi_change_4h_pct=0.0),
    FundingFirstRule("funding_deep_discount_m30", 72, max_premium_pct=-2.0, max_prev_funding_pct=-0.30, min_return_1h_pct=-5.0),
    FundingFirstRule("funding_deep_oi_volume_m30", 72, max_premium_pct=-2.0, max_prev_funding_pct=-0.30, min_return_1h_pct=-5.0, min_oi_change_4h_pct=0.0, min_volume_z=0.0),
    FundingFirstRule("funding_extreme_absorption_m50", 72, max_premium_pct=-3.5, max_prev_funding_pct=-0.50, min_return_1h_pct=-3.0, min_oi_change_4h_pct=10.0),
)


def run_pump_research_next_steps(
    *,
    event_windows_path: Path = DEFAULT_EVENT_WINDOWS,
    long_outcomes_path: Path = DEFAULT_LONG_OUTCOMES,
    long_portfolio_path: Path = DEFAULT_LONG_PORTFOLIO,
    cycle_summary_path: Path = DEFAULT_CYCLE_SUMMARY,
    cycle_trades_path: Path = DEFAULT_CYCLE_TRADES,
    shadow_history_path: Path = DEFAULT_SHADOW_HISTORY,
    active_window_path: Path = DEFAULT_ACTIVE_WINDOW,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    fetch_market_context: bool = False,
    market_context_path: Path | None = None,
    market_sleep_sec: float = 0.05,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    samples = read_jsonl(event_windows_path)
    long_rows = read_csv(long_outcomes_path)

    relief_rows = build_premium_relief_exit_outcomes(samples)
    relief_summary = summarize_outcomes(relief_rows, keys=("entry_rule", "exit_plan"))
    relief_portfolio_summary, relief_portfolio_trades = build_portfolio_replays(
        relief_rows,
        starting_capital_usd=PORTFOLIO_STARTING_CAPITAL_USD,
        leverage=PORTFOLIO_LEVERAGE,
        slot_counts=(1, 2, 3, 4),
    )

    failure_rows, failure_summary, failure_filters = build_failed_absorption_classifier(long_rows)
    divergence_summary = build_divergence_summary(long_rows + relief_rows)
    funding_rows = build_funding_first_outcomes(samples)
    funding_summary = summarize_outcomes(funding_rows, keys=("entry_rule", "exit_plan"))
    funding_portfolio_summary, funding_portfolio_trades = build_portfolio_replays(
        funding_rows,
        starting_capital_usd=PORTFOLIO_STARTING_CAPITAL_USD,
        leverage=PORTFOLIO_LEVERAGE,
        slot_counts=(1, 2, 3, 4),
    )

    market_context_error = ""
    resolved_market_context = market_context_path or output_dir / "market_context_5m.csv"
    if fetch_market_context:
        try:
            collect_market_context_from_rows(
                long_rows,
                output_path=resolved_market_context,
                sleep_sec=market_sleep_sec,
            )
        except Exception as exc:  # pylint: disable=broad-except
            market_context_error = str(exc)
    context_rows = join_market_context(long_rows, read_csv(resolved_market_context) if resolved_market_context.exists() else [])
    context_summary = build_market_context_summary(context_rows)

    visibility_rows, visibility_summary = build_shadow_visibility_replay(
        long_rows=long_rows,
        cycle_trades=read_csv(cycle_trades_path),
        shadow_history_path=shadow_history_path,
        active_window_rows=read_csv(active_window_path),
    )

    scorecard_rows = build_strategy_scorecard(
        long_portfolio_rows=read_csv(long_portfolio_path),
        cycle_summary_rows=read_csv(cycle_summary_path),
        relief_portfolio_rows=relief_portfolio_summary,
        funding_portfolio_rows=funding_portfolio_summary,
        visibility_summary=visibility_summary,
        failure_filters=failure_filters,
    )

    write_csv(output_dir / "premium_relief_exit_outcomes.csv", relief_rows)
    write_csv(output_dir / "premium_relief_exit_summary.csv", relief_summary)
    write_csv(output_dir / "premium_relief_portfolio_summary.csv", relief_portfolio_summary)
    write_csv(output_dir / "premium_relief_portfolio_trades.csv", relief_portfolio_trades)
    write_csv(output_dir / "failed_absorption_cases.csv", failure_rows)
    write_csv(output_dir / "failed_absorption_feature_summary.csv", failure_summary)
    write_csv(output_dir / "failed_absorption_filter_audit.csv", failure_filters)
    write_csv(output_dir / "oi_premium_divergence_summary.csv", divergence_summary)
    write_csv(output_dir / "funding_first_outcomes.csv", funding_rows)
    write_csv(output_dir / "funding_first_summary.csv", funding_summary)
    write_csv(output_dir / "funding_first_portfolio_summary.csv", funding_portfolio_summary)
    write_csv(output_dir / "funding_first_portfolio_trades.csv", funding_portfolio_trades)
    write_csv(output_dir / "premium_market_context_rows.csv", context_rows)
    write_csv(output_dir / "premium_market_context_summary.csv", context_summary)
    write_csv(output_dir / "shadow_visibility_replay.csv", visibility_rows)
    write_csv(output_dir / "shadow_visibility_summary.csv", visibility_summary)
    write_csv(output_dir / "strategy_scorecard.csv", scorecard_rows)
    (output_dir / "index.html").write_text(
        render_html_report(
            relief_summary=relief_summary,
            failure_summary=failure_summary,
            failure_filters=failure_filters,
            divergence_summary=divergence_summary,
            funding_summary=funding_summary,
            context_summary=context_summary,
            visibility_summary=visibility_summary,
            scorecard_rows=scorecard_rows,
        ),
        encoding="utf-8",
    )
    metadata = {
        "schema": "pump_research_next_steps_v1",
        "event_windows_path": str(event_windows_path),
        "long_outcomes_path": str(long_outcomes_path),
        "output_dir": str(output_dir),
        "samples": len(samples),
        "long_rows": len(long_rows),
        "relief_rows": len(relief_rows),
        "relief_summary_rows": len(relief_summary),
        "failure_cases": len(failure_rows),
        "failure_filter_rows": len(failure_filters),
        "divergence_rows": len(divergence_summary),
        "funding_first_rows": len(funding_rows),
        "market_context_rows": len(context_rows),
        "market_context_error": market_context_error,
        "visibility_rows": len(visibility_rows),
        "scorecard_rows": len(scorecard_rows),
        "elapsed_sec": round_float(time.time() - started),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def build_premium_relief_exit_outcomes(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sample in samples:
        interval, series = primary_interval(sample)
        klines = series.get("klines") or []
        premium = series.get("premium_index_klines") or []
        oi = series.get("open_interest") or []
        funding = sample.get("funding") or []
        trigger_ts = to_int(sample.get("trigger_ts")) or 0
        for entry_rule in ENTRY_RULES:
            entry = find_entry(klines, premium, oi, trigger_ts, entry_rule)
            if not entry:
                continue
            for exit_plan in RELIEF_EXIT_PLANS:
                row = simulate_relief_exit(sample, interval, klines, premium, oi, funding, entry, entry_rule.slug, exit_plan)
                if row:
                    rows.append(row)
    return rows


def build_funding_first_outcomes(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sample in samples:
        interval, series = primary_interval(sample)
        klines = series.get("klines") or []
        premium = series.get("premium_index_klines") or []
        oi = series.get("open_interest") or []
        funding = sample.get("funding") or []
        trigger_ts = to_int(sample.get("trigger_ts")) or 0
        for rule in FUNDING_FIRST_RULES:
            entry = find_funding_first_entry(klines, premium, oi, funding, trigger_ts, rule)
            if not entry:
                continue
            for exit_plan in RELIEF_EXIT_PLANS:
                row = simulate_relief_exit(sample, interval, klines, premium, oi, funding, entry, rule.slug, exit_plan)
                if row:
                    rows.append(row)
    return rows


def find_funding_first_entry(
    klines: list[dict[str, Any]],
    premium: list[dict[str, Any]],
    oi: list[dict[str, Any]],
    funding: list[dict[str, Any]],
    trigger_ts: int,
    rule: FundingFirstRule,
) -> dict[str, Any] | None:
    end_ts = trigger_ts + rule.max_wait_h * HOUR_MS
    for idx, row in enumerate(klines):
        ts_ms = to_int(row.get("ts_ms")) or 0
        if ts_ms < trigger_ts or ts_ms > end_ts:
            continue
        price = to_float(row.get("close"))
        premium_now = value_at_or_before(premium, ts_ms, "close")
        prev_funding_pct = last_funding_rate_pct(funding, ts_ms)
        if price is None or premium_now is None or prev_funding_pct is None:
            continue
        if premium_now * 100.0 > rule.max_premium_pct or prev_funding_pct > rule.max_prev_funding_pct:
            continue
        return_1h = pct_change(price, value_at_or_before(klines, ts_ms - HOUR_MS, "close"))
        if return_1h is None or return_1h < rule.min_return_1h_pct:
            continue
        oi_change_4h = pct_change(
            value_at_or_before(oi, ts_ms, "open_interest"),
            value_at_or_before(oi, ts_ms - 4 * HOUR_MS, "open_interest"),
        )
        if rule.min_oi_change_4h_pct is not None and (oi_change_4h is None or oi_change_4h < rule.min_oi_change_4h_pct):
            continue
        volume_z = volume_z_at(klines, idx, lookback=288)
        if rule.min_volume_z is not None and (volume_z is None or volume_z < rule.min_volume_z):
            continue
        premium_1h = value_at_or_before(premium, ts_ms - HOUR_MS, "close")
        return {
            "idx": idx,
            "ts_ms": ts_ms,
            "price": price,
            "premium_pct": premium_now * 100.0,
            "premium_relief_1h_pct": (premium_now - premium_1h) * 100.0 if premium_1h is not None else None,
            "return_1h_pct": return_1h,
            "oi_change_4h_pct": oi_change_4h,
            "volume_z": volume_z,
            "prev_funding_rate_pct": prev_funding_pct,
        }
    return None


def simulate_relief_exit(
    sample: dict[str, Any],
    interval: str,
    klines: list[dict[str, Any]],
    premium: list[dict[str, Any]],
    oi: list[dict[str, Any]],
    funding: list[dict[str, Any]],
    entry: dict[str, Any],
    entry_rule_slug: str,
    exit_plan: ReliefExitPlan,
) -> dict[str, Any] | None:
    entry_idx = int(entry["idx"])
    entry_ts = int(entry["ts_ms"])
    entry_price = float(entry["price"])
    entry_premium_pct = to_float(entry.get("premium_pct"))
    entry_oi = value_at_or_before(oi, entry_ts, "open_interest")
    end_ts = entry_ts + exit_plan.max_hold_h * HOUR_MS
    exit_idx = entry_idx
    exit_price = to_float(klines[entry_idx].get("close"))
    exit_reason = "time_stop"
    for idx in range(entry_idx + 1, len(klines)):
        row = klines[idx]
        ts_ms = to_int(row.get("ts_ms")) or 0
        if ts_ms > end_ts:
            break
        low = to_float(row.get("low"))
        high = to_float(row.get("high"))
        close = to_float(row.get("close"))
        if close is None:
            continue
        stop_price = entry_price * (1.0 - exit_plan.stop_pct / 100.0)
        tp_price = entry_price * (1.0 + exit_plan.tp_pct / 100.0)
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
        gross_now = (close / entry_price - 1.0) * 100.0
        hold_h = (ts_ms - entry_ts) / HOUR_MS
        if hold_h >= exit_plan.min_hold_h and gross_now >= exit_plan.min_profit_for_relief_pct:
            premium_now = value_at_or_before(premium, ts_ms, "close")
            oi_now = value_at_or_before(oi, ts_ms, "open_interest")
            funding_now_pct = last_funding_rate_pct(funding, ts_ms)
            if exit_plan.premium_exit_pct is not None and premium_now is not None and premium_now * 100.0 >= exit_plan.premium_exit_pct:
                exit_idx = idx
                exit_price = close
                exit_reason = "premium_relief_exit"
                break
            if (
                exit_plan.premium_relief_from_entry_pct is not None
                and premium_now is not None
                and entry_premium_pct is not None
                and premium_now * 100.0 - entry_premium_pct >= exit_plan.premium_relief_from_entry_pct
            ):
                exit_idx = idx
                exit_price = close
                exit_reason = "premium_relief_from_entry_exit"
                break
            if (
                exit_plan.oi_fade_from_entry_pct is not None
                and oi_now is not None
                and entry_oi is not None
                and pct_change(oi_now, entry_oi) is not None
                and (pct_change(oi_now, entry_oi) or 0.0) <= exit_plan.oi_fade_from_entry_pct
            ):
                exit_idx = idx
                exit_price = close
                exit_reason = "oi_fade_exit"
                break
            if (
                exit_plan.funding_rate_exit_pct is not None
                and funding_now_pct is not None
                and funding_now_pct >= exit_plan.funding_rate_exit_pct
            ):
                exit_idx = idx
                exit_price = close
                exit_reason = "funding_relief_exit"
                break
        exit_idx = idx
        exit_price = close
    if exit_price is None:
        return None
    exit_ts = to_int(klines[exit_idx].get("ts_ms")) or entry_ts
    gross_pct = (exit_price / entry_price - 1.0) * 100.0
    funding_sum_pct = scale_pct(sum_funding_between(funding, entry_ts, exit_ts) or 0.0) or 0.0
    long_funding_pct = -funding_sum_pct
    net_pct = gross_pct + long_funding_pct - FEE_ROUNDTRIP_PCT
    high_during = max_clean(klines[entry_idx : exit_idx + 1], "high")
    low_during = min_clean(klines[entry_idx : exit_idx + 1], "low")
    event = sample.get("event") if isinstance(sample.get("event"), dict) else {}
    return {
        "event_id": event.get("event_id"),
        "symbol": sample.get("symbol"),
        "trigger_ts": sample.get("trigger_ts"),
        "trigger_iso": sample.get("trigger_iso"),
        "trigger_pump_pct": event.get("trigger_pump_pct"),
        "interval": interval,
        "entry_rule": entry_rule_slug,
        "exit_plan": exit_plan.slug,
        "entry_ts": entry_ts,
        "entry_iso": ms_to_iso(entry_ts),
        "entry_wait_h": round_float((entry_ts - (to_int(sample.get("trigger_ts")) or entry_ts)) / HOUR_MS),
        "entry_price": round_float(entry_price),
        "entry_premium_pct": round_float(entry.get("premium_pct")),
        "entry_premium_relief_1h_pct": round_float(entry.get("premium_relief_1h_pct")),
        "entry_return_1h_pct": round_float(entry.get("return_1h_pct")),
        "entry_oi_change_4h_pct": round_float(entry.get("oi_change_4h_pct")),
        "entry_volume_z": round_float(entry.get("volume_z")),
        "entry_prev_funding_rate_pct": round_float(entry.get("prev_funding_rate_pct")),
        "exit_ts": exit_ts,
        "exit_iso": ms_to_iso(exit_ts),
        "exit_h": round_float((exit_ts - entry_ts) / HOUR_MS),
        "exit_price": round_float(exit_price),
        "exit_reason": exit_reason,
        "gross_price_pct": round_float(gross_pct),
        "funding_sum_pct": round_float(funding_sum_pct),
        "long_funding_pct": round_float(long_funding_pct),
        "net_pct": round_float(net_pct),
        "mfe_pct": round_float(pct_change(high_during, entry_price)),
        "mae_pct": round_float(-pct_change(low_during, entry_price) if low_during is not None else None),
        "exit_premium_pct": round_float(scale_pct(value_at_or_before(premium, exit_ts, "close"))),
        "exit_oi_change_from_entry_pct": round_float(pct_change(value_at_or_before(oi, exit_ts, "open_interest"), entry_oi)),
        "exit_funding_rate_pct": round_float(last_funding_rate_pct(funding, exit_ts)),
    }


def build_failed_absorption_classifier(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    target = [
        row
        for row in rows
        if row.get("entry_rule") == "deep_discount_survives"
        and row.get("exit_plan") == "tp30_sl25_hold72_fundrelief"
    ]
    case_rows: list[dict[str, Any]] = []
    for row in target:
        net = to_float(row.get("net_pct")) or 0.0
        exit_reason = str(row.get("exit_reason") or "")
        if exit_reason == "take_profit" and net > 0:
            label = "clean_tp"
        elif exit_reason == "stop_loss" or net <= -5.0:
            label = "failed_absorption"
        elif net > 0:
            label = "partial_recovery"
        else:
            label = "late_decay"
        out = dict(row)
        out["absorption_label"] = label
        out["risk_score"] = round_float(absorption_risk_score(row))
        case_rows.append(out)
    summary = summarize_outcomes(case_rows, keys=("absorption_label",), extra_features=True)
    filter_rows = []
    for spec in FILTER_SPECS:
        matched = [row for row in case_rows if filter_matches(row, spec)]
        if not matched:
            continue
        failures = sum(1 for row in matched if row.get("absorption_label") == "failed_absorption")
        wins = sum(1 for row in matched if (to_float(row.get("net_pct")) or 0.0) > 0.0)
        filter_rows.append(
            {
                "filter_slug": spec.slug,
                "n": len(matched),
                "win_pct": pct(wins, len(matched)),
                "failed_absorption_pct": pct(failures, len(matched)),
                "avg_net_pct": round_float(mean_value(matched, "net_pct")),
                "median_net_pct": round_float(median_value(matched, "net_pct")),
                "min_net_pct": round_float(min_value(matched, "net_pct")),
                "avg_risk_score": round_float(mean_value(matched, "risk_score")),
            }
        )
    filter_rows.sort(key=lambda row: (to_float(row.get("failed_absorption_pct")) or 999, -(to_int(row.get("n")) or 0), to_float(row.get("avg_net_pct")) or -999))
    return case_rows, summary, filter_rows


def build_divergence_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if to_float(row.get("net_pct")) is None:
            continue
        key = (
            premium_bucket(row.get("entry_premium_pct")),
            oi_bucket(row.get("entry_oi_change_4h_pct")),
            relief_bucket(row.get("entry_premium_relief_1h_pct")),
            volume_bucket(row.get("entry_volume_z")),
        )
        groups.setdefault(key, []).append(row)
    out = []
    for (premium_group, oi_group, relief_group, volume_group), items in groups.items():
        if len(items) < 3:
            continue
        wins = sum(1 for row in items if (to_float(row.get("net_pct")) or 0.0) > 0.0)
        out.append(
            {
                "premium_bucket": premium_group,
                "oi_bucket": oi_group,
                "premium_relief_bucket": relief_group,
                "volume_bucket": volume_group,
                "n": len(items),
                "win_pct": pct(wins, len(items)),
                "avg_net_pct": round_float(mean_value(items, "net_pct")),
                "median_net_pct": round_float(median_value(items, "net_pct")),
                "min_net_pct": round_float(min_value(items, "net_pct")),
                "avg_funding_pct": round_float(mean_value(items, "long_funding_pct")),
            }
        )
    out.sort(key=lambda row: (to_int(row.get("n")) or 0, to_float(row.get("median_net_pct")) or -999), reverse=True)
    return out


def collect_market_context_from_rows(
    rows: list[dict[str, Any]],
    *,
    output_path: Path,
    sleep_sec: float = 0.05,
) -> None:
    entry_times = [to_int(row.get("entry_ts")) for row in rows if to_int(row.get("entry_ts"))]
    if not entry_times:
        write_csv(output_path, [])
        return
    windows = merge_context_windows((entry_ts - 24 * HOUR_MS, entry_ts) for entry_ts in entry_times)
    collector = BybitPumpShortCollector(
        BybitCollectorConfig(
            output_dir=output_path.parent,
            sleep_sec=sleep_sec,
            timeout_sec=20.0,
            max_retries=3,
        )
    )
    context_rows: list[dict[str, Any]] = []
    for symbol in ("BTCUSDT", "ETHUSDT"):
        symbol_rows: list[dict[str, Any]] = []
        for start_ms, end_ms in windows:
            symbol_rows.extend(collector.fetch_klines(symbol, interval="5", start_ms=start_ms, end_ms=end_ms))
        for row in dedupe_sort_by_ts(symbol_rows):
            item = dict(row)
            item["symbol"] = symbol
            context_rows.append(item)
    write_csv(output_path, context_rows)


def merge_context_windows(windows: Iterable[tuple[int, int]], *, max_gap_ms: int = 5 * 60_000) -> list[tuple[int, int]]:
    ordered = sorted((int(start), int(end)) for start, end in windows if start < end)
    if not ordered:
        return []
    merged = [ordered[0]]
    for start, end in ordered[1:]:
        prev_start, prev_end = merged[-1]
        if start <= prev_end + max_gap_ms:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def join_market_context(long_rows: list[dict[str, Any]], context_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in context_rows:
        by_symbol.setdefault(str(row.get("symbol") or ""), []).append(row)
    for items in by_symbol.values():
        items.sort(key=lambda row: to_int(row.get("ts_ms")) or 0)
    out = []
    for row in long_rows:
        entry_ts = to_int(row.get("entry_ts"))
        if not entry_ts:
            continue
        item = dict(row)
        for symbol, prefix in (("BTCUSDT", "btc"), ("ETHUSDT", "eth")):
            series = by_symbol.get(symbol) or []
            now = value_at_or_before(series, entry_ts, "close")
            item[f"{prefix}_ret_1h_pct"] = round_float(pct_change(now, value_at_or_before(series, entry_ts - HOUR_MS, "close")))
            item[f"{prefix}_ret_4h_pct"] = round_float(pct_change(now, value_at_or_before(series, entry_ts - 4 * HOUR_MS, "close")))
            item[f"{prefix}_ret_24h_pct"] = round_float(pct_change(now, value_at_or_before(series, entry_ts - 24 * HOUR_MS, "close")))
            item[f"{prefix}_vol_z_24h"] = round_float(context_vol_z(series, entry_ts))
        out.append(item)
    return out


def build_market_context_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    out = []
    buckets = {
        "btc_4h_down": lambda row: (to_float(row.get("btc_ret_4h_pct")) or 0.0) < -1.0,
        "btc_4h_flat_up": lambda row: (to_float(row.get("btc_ret_4h_pct")) or 0.0) >= -1.0,
        "btc_24h_down": lambda row: (to_float(row.get("btc_ret_24h_pct")) or 0.0) < -3.0,
        "eth_4h_down": lambda row: (to_float(row.get("eth_ret_4h_pct")) or 0.0) < -1.0,
        "btc_vol_shock": lambda row: (to_float(row.get("btc_vol_z_24h")) or 0.0) >= 2.0,
    }
    for name, predicate in buckets.items():
        matched = [row for row in rows if predicate(row)]
        if not matched:
            continue
        wins = sum(1 for row in matched if (to_float(row.get("net_pct")) or 0.0) > 0.0)
        out.append(
            {
                "context_bucket": name,
                "n": len(matched),
                "win_pct": pct(wins, len(matched)),
                "avg_net_pct": round_float(mean_value(matched, "net_pct")),
                "median_net_pct": round_float(median_value(matched, "net_pct")),
                "min_net_pct": round_float(min_value(matched, "net_pct")),
            }
        )
    return out


def build_shadow_visibility_replay(
    *,
    long_rows: list[dict[str, Any]],
    cycle_trades: list[dict[str, Any]],
    shadow_history_path: Path,
    active_window_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scan_symbols = read_shadow_seen_symbols(shadow_history_path)
    rows: list[dict[str, Any]] = []
    for row in long_rows:
        if row.get("entry_rule") != "deep_discount_survives" or row.get("exit_plan") != "tp30_sl25_hold72_fundrelief":
            continue
        entry_wait_h = to_float(row.get("entry_wait_h")) or 0.0
        symbol = str(row.get("symbol") or "")
        rows.append(
            {
                "source": "historical_long",
                "track_id": "long_broad_or_clean",
                "symbol": symbol,
                "event_id": row.get("event_id"),
                "entry_ts": row.get("entry_ts"),
                "entry_iso": row.get("entry_iso"),
                "entry_wait_h": round_float(entry_wait_h),
                "simulated_1h_scan_visible": bool(entry_wait_h <= 3.0),
                "simulated_5m_followup_visible": bool(entry_wait_h <= 3.0 and (to_float(row.get("entry_premium_pct")) or 0.0) <= -1.0),
                "seen_in_current_shadow_history": symbol in scan_symbols,
                "net_pct": row.get("net_pct"),
            }
        )
    for row in cycle_trades:
        symbol = str(row.get("symbol") or "")
        rows.append(
            {
                "source": "cycle_trade",
                "track_id": row.get("track_id"),
                "side": row.get("side"),
                "symbol": symbol,
                "event_id": row.get("event_id"),
                "entry_ts": row.get("entry_ts"),
                "entry_iso": row.get("entry_iso"),
                "simulated_1h_scan_visible": True,
                "simulated_5m_followup_visible": row.get("side") == "long",
                "seen_in_current_shadow_history": symbol in scan_symbols,
                "net_pct": row.get("net_pct"),
            }
        )
    active_symbols = {str(row.get("symbol") or "") for row in active_window_rows}
    summary = []
    for source in sorted({str(row.get("source") or "") for row in rows}):
        items = [row for row in rows if row.get("source") == source]
        summary.append(
            {
                "source": source,
                "n": len(items),
                "simulated_1h_visible_pct": pct(sum(1 for row in items if row.get("simulated_1h_scan_visible")), len(items)),
                "simulated_5m_visible_pct": pct(sum(1 for row in items if row.get("simulated_5m_followup_visible")), len(items)),
                "seen_in_current_shadow_history_pct": pct(sum(1 for row in items if row.get("seen_in_current_shadow_history")), len(items)),
                "active_window_symbol_overlap": len(active_symbols.intersection({str(row.get("symbol") or "") for row in items})),
            }
        )
    return rows, summary


def build_strategy_scorecard(
    *,
    long_portfolio_rows: list[dict[str, Any]],
    cycle_summary_rows: list[dict[str, Any]],
    relief_portfolio_rows: list[dict[str, Any]],
    funding_portfolio_rows: list[dict[str, Any]],
    visibility_summary: list[dict[str, Any]],
    failure_filters: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in select_portfolio_rows(long_portfolio_rows, "long_existing"):
        rows.append(scorecard_from_portfolio(row, family="long_existing", recommendation="keep_shadow"))
    for row in select_cycle_rows(cycle_summary_rows):
        rows.append(scorecard_from_cycle(row, family="cycle_existing", recommendation="keep_paper"))
    for row in select_portfolio_rows(relief_portfolio_rows, "long_relief_exit"):
        rows.append(scorecard_from_portfolio(row, family="long_relief_exit", recommendation="research_shadow"))
    for row in select_portfolio_rows(funding_portfolio_rows, "funding_first"):
        rows.append(scorecard_from_portfolio(row, family="funding_first", recommendation="research_shadow"))
    if failure_filters:
        best_filter = failure_filters[0]
        rows.append(
            {
                "family": "failed_absorption_veto",
                "track_id": best_filter.get("filter_slug"),
                "n": best_filter.get("n"),
                "roi_pct": "",
                "risk_adjusted_roi_pct": "",
                "win_pct": best_filter.get("win_pct"),
                "max_drawdown_pct": "",
                "worst_trade_pct": best_filter.get("min_net_pct"),
                "failure_pct": best_filter.get("failed_absorption_pct"),
                "recommendation": "use_as_long_veto_candidate",
                "notes": "Lowest failed-absorption filter among tested entry-known filters.",
            }
        )
    for row in visibility_summary:
        rows.append(
            {
                "family": "shadow_visibility",
                "track_id": row.get("source"),
                "n": row.get("n"),
                "roi_pct": "",
                "risk_adjusted_roi_pct": "",
                "win_pct": "",
                "max_drawdown_pct": "",
                "worst_trade_pct": "",
                "failure_pct": "",
                "recommendation": "check_scanner_coverage",
                "notes": f"1h visible {row.get('simulated_1h_visible_pct')}%, 5m visible {row.get('simulated_5m_visible_pct')}%.",
            }
        )
    rows.sort(key=scorecard_sort_key, reverse=True)
    return rows


def scorecard_from_portfolio(row: dict[str, Any], *, family: str, recommendation: str) -> dict[str, Any]:
    return {
        "family": family,
        "track_id": "__".join(str(row.get(key) or "") for key in ("entry_rule", "exit_plan", "filter_slug", "slots")).strip("_"),
        "n": row.get("trades"),
        "roi_pct": row.get("roi_pct"),
        "risk_adjusted_roi_pct": row.get("risk_adjusted_roi_pct"),
        "win_pct": row.get("win_pct"),
        "max_drawdown_pct": row.get("max_drawdown_pct"),
        "worst_trade_pct": row.get("worst_trade_pct"),
        "failure_pct": row.get("stop_loss_pct"),
        "recommendation": recommendation,
        "notes": "",
    }


def scorecard_from_cycle(row: dict[str, Any], *, family: str, recommendation: str) -> dict[str, Any]:
    return {
        "family": family,
        "track_id": "__".join(str(row.get(key) or "") for key in ("allocation_id", "long_track_id", "short_track_id")).strip("_"),
        "n": row.get("trades"),
        "roi_pct": row.get("roi_pct"),
        "risk_adjusted_roi_pct": row.get("risk_adjusted_roi_pct"),
        "win_pct": row.get("win_pct"),
        "max_drawdown_pct": row.get("max_drawdown_pct"),
        "worst_trade_pct": row.get("worst_trade_pct"),
        "failure_pct": "",
        "recommendation": recommendation,
        "notes": "",
    }


def select_portfolio_rows(rows: list[dict[str, Any]], family: str) -> list[dict[str, Any]]:
    min_trades = 10 if family != "funding_first" else 3
    candidates = [
        row
        for row in rows
        if (to_int(row.get("trades")) or 0) >= min_trades
        and (to_int(row.get("slots")) or 0) >= 2
    ]
    candidates.sort(
        key=lambda row: (
            practical_portfolio_score(row),
            to_float(row.get("risk_adjusted_roi_pct")) or -10**9,
            to_float(row.get("roi_pct")) or -10**9,
        ),
        reverse=True,
    )
    return candidates[:8]


def select_cycle_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [row for row in rows if str(row.get("allocation_id") or "") in {"cycle_6_4s2l", "cycle_5_4s1l", "cycle_5_3s2l", "short_only_4", "long_only_2"}]
    candidates.sort(key=lambda row: (to_float(row.get("risk_adjusted_roi_pct")) or -10**9), reverse=True)
    return candidates[:8]


def summarize_outcomes(rows: list[dict[str, Any]], *, keys: tuple[str, ...], extra_features: bool = False) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row.get(key) for key in keys), []).append(row)
    out = []
    for key_values, items in groups.items():
        wins = sum(1 for row in items if (to_float(row.get("net_pct")) or 0.0) > 0.0)
        record = {key: value for key, value in zip(keys, key_values)}
        record.update(
            {
                "n": len(items),
                "win_pct": pct(wins, len(items)),
                "avg_net_pct": round_float(mean_value(items, "net_pct")),
                "median_net_pct": round_float(median_value(items, "net_pct")),
                "min_net_pct": round_float(min_value(items, "net_pct")),
                "avg_mae_pct": round_float(mean_value(items, "mae_pct")),
                "avg_mfe_pct": round_float(mean_value(items, "mfe_pct")),
                "avg_funding_pct": round_float(mean_value(items, "long_funding_pct")),
                "take_profit_pct": pct(sum(1 for row in items if row.get("exit_reason") == "take_profit"), len(items)),
                "stop_loss_pct": pct(sum(1 for row in items if row.get("exit_reason") == "stop_loss"), len(items)),
            }
        )
        if extra_features:
            record.update(
                {
                    "avg_entry_wait_h": round_float(mean_value(items, "entry_wait_h")),
                    "avg_entry_premium_pct": round_float(mean_value(items, "entry_premium_pct")),
                    "avg_entry_oi_change_4h_pct": round_float(mean_value(items, "entry_oi_change_4h_pct")),
                    "avg_entry_volume_z": round_float(mean_value(items, "entry_volume_z")),
                    "avg_risk_score": round_float(mean_value(items, "risk_score")),
                }
            )
        out.append(record)
    out.sort(key=lambda row: (to_int(row.get("n")) or 0, to_float(row.get("median_net_pct")) or -999), reverse=True)
    return out


def absorption_risk_score(row: dict[str, Any]) -> float:
    score = 0.0
    entry_wait = to_float(row.get("entry_wait_h")) or 0.0
    entry_premium = to_float(row.get("entry_premium_pct")) or 0.0
    oi4 = to_float(row.get("entry_oi_change_4h_pct"))
    volume_z = to_float(row.get("entry_volume_z"))
    return_1h = to_float(row.get("entry_return_1h_pct")) or 0.0
    if entry_wait > 0.5:
        score += min(30.0, entry_wait * 5.0)
    if entry_premium < -5.0:
        score += 25.0
    if oi4 is None or oi4 < 0:
        score += 20.0
    if volume_z is None or volume_z < 1.0:
        score += 15.0
    if return_1h > 20.0:
        score += 10.0
    return score


def read_shadow_seen_symbols(path: Path) -> set[str]:
    seen: set[str] = set()
    if not path.exists():
        return seen
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows = payload.get("rows") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                continue
            for row in rows:
                symbol = str((row or {}).get("symbol") or "")
                if symbol:
                    seen.add(symbol)
    return seen


def render_html_report(**sections: list[dict[str, Any]]) -> str:
    parts = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>Pump Research Next Steps</title>",
        "<style>body{font-family:Arial,sans-serif;margin:24px;line-height:1.35}table{border-collapse:collapse;width:100%;margin:12px 0 28px}th,td{border:1px solid #ddd;padding:5px 7px;font-size:12px}th{background:#f3f4f6;text-align:left}h2{margin-top:28px}.note{color:#555}</style>",
        "</head><body>",
        "<h1>Pump Research Next Steps</h1>",
        "<p class='note'>Research-only report: premium-relief exits, failed-absorption vetoes, OI/premium divergence, funding-first entries, BTC/ETH context, shadow visibility, and combined strategy scorecard.</p>",
    ]
    labels = {
        "scorecard_rows": "Strategy Scorecard",
        "relief_summary": "Premium/Funding Long: Relief Exit Sweep",
        "failure_summary": "Failed Absorption Feature Summary",
        "failure_filters": "Failed Absorption Veto Filter Audit",
        "divergence_summary": "OI + Premium Divergence Buckets",
        "funding_summary": "Funding-First Long Entries",
        "context_summary": "BTC/ETH Market Context",
        "visibility_summary": "Shadow Visibility Replay",
    }
    order = [
        "scorecard_rows",
        "relief_summary",
        "failure_summary",
        "failure_filters",
        "divergence_summary",
        "funding_summary",
        "context_summary",
        "visibility_summary",
    ]
    for key in order:
        rows = sections.get(key) or []
        parts.append(f"<h2>{html.escape(labels[key])}</h2>")
        parts.append(render_table(rows[:30]))
    parts.append("</body></html>")
    return "\n".join(parts)


def render_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p class='note'>No rows.</p>"
    headers = list(rows[0].keys())
    out = ["<table><thead><tr>"]
    out.extend(f"<th>{html.escape(str(header))}</th>" for header in headers)
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>")
        out.extend(f"<td>{html.escape(str(row.get(header, '')))}</td>" for header in headers)
        out.append("</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def pct(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return round_float(float(numerator) / float(denominator) * 100.0)


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return (float(current) / float(previous) - 1.0) * 100.0


def mean_value(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [to_float(row.get(key)) for row in rows]
    values = [value for value in values if value is not None]
    return statistics.mean(values) if values else None


def median_value(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [to_float(row.get(key)) for row in rows]
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def min_value(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [to_float(row.get(key)) for row in rows]
    values = [value for value in values if value is not None]
    return min(values) if values else None


def max_clean(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    values = [to_float(row.get(key)) for row in rows]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def min_clean(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    values = [to_float(row.get(key)) for row in rows]
    values = [value for value in values if value is not None]
    return min(values) if values else None


def ms_to_iso(ts_ms: int | None) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).isoformat()


def last_funding_rate_pct(funding: list[dict[str, Any]], ts_ms: int) -> float | None:
    candidates = [row for row in funding if (to_int(row.get("ts_ms")) or 0) <= ts_ms]
    if not candidates:
        return None
    latest = max(candidates, key=lambda row: to_int(row.get("ts_ms")) or 0)
    rate = to_float(latest.get("funding_rate"))
    return scale_pct(rate)


def volume_z_at(rows: list[dict[str, Any]], idx: int, *, lookback: int) -> float | None:
    if idx <= 1:
        return None
    start = max(0, idx - lookback)
    volumes = [to_float(row.get("volume")) for row in rows[start:idx]]
    volumes = [value for value in volumes if value is not None]
    current = to_float(rows[idx].get("volume"))
    if current is None or len(volumes) < 8:
        return None
    mean = statistics.mean(volumes)
    stdev = statistics.pstdev(volumes)
    if stdev <= 0:
        return 0.0
    return (current - mean) / stdev


def context_vol_z(series: list[dict[str, Any]], ts_ms: int) -> float | None:
    rows = [row for row in series if (to_int(row.get("ts_ms")) or 0) <= ts_ms]
    if len(rows) < 288:
        return None
    idx = len(rows) - 1
    return volume_z_at(rows, idx, lookback=288)


def premium_bucket(value: Any) -> str:
    raw = to_float(value)
    if raw is None:
        return "premium_unknown"
    if raw <= -5:
        return "premium_extreme_le_m5"
    if raw <= -3.5:
        return "premium_deep_m5_m35"
    if raw <= -1.2:
        return "premium_mid_m35_m12"
    if raw <= -0.5:
        return "premium_shallow_m12_m05"
    return "premium_not_discounted"


def oi_bucket(value: Any) -> str:
    raw = to_float(value)
    if raw is None:
        return "oi_unknown"
    if raw >= 20:
        return "oi_ge_20"
    if raw >= 10:
        return "oi_10_20"
    if raw >= 0:
        return "oi_0_10"
    return "oi_negative"


def relief_bucket(value: Any) -> str:
    raw = to_float(value)
    if raw is None:
        return "relief_unknown"
    if raw >= 1:
        return "relief_ge_1"
    if raw >= 0:
        return "relief_0_1"
    if raw >= -1:
        return "relief_m1_0"
    return "relief_lt_m1"


def volume_bucket(value: Any) -> str:
    raw = to_float(value)
    if raw is None:
        return "volume_unknown"
    if raw >= 2:
        return "volume_z_ge_2"
    if raw >= 1:
        return "volume_z_1_2"
    if raw >= 0:
        return "volume_z_0_1"
    return "volume_z_negative"


def scorecard_sort_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (
        to_float(row.get("risk_adjusted_roi_pct")) or -10**9,
        to_float(row.get("roi_pct")) or -10**9,
        to_float(row.get("win_pct")) or -10**9,
    )


def practical_portfolio_score(row: dict[str, Any]) -> float:
    risk_adjusted = to_float(row.get("risk_adjusted_roi_pct")) or -10**9
    max_drawdown = to_float(row.get("max_drawdown_pct")) or 0.0
    worst_trade = to_float(row.get("worst_trade_pct")) or 0.0
    slots = to_int(row.get("slots")) or 0
    slot_penalty = 30.0 if slots <= 1 else 0.0
    return risk_adjusted - max(0.0, max_drawdown - 35.0) * 5.0 - max(0.0, -worst_trade - 60.0) * 2.0 - slot_penalty
