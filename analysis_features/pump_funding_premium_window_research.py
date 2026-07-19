from __future__ import annotations

import csv
import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analysis_collectors.bybit_event_window import HOUR_MS, pct_change
from analysis_collectors.bybit_pump_short import round_float, scale_pct, to_float, to_int
from analysis_features.bybit_pump_short_outcomes import FEE_ROUNDTRIP_PCT, write_csv
from config import BASE_DIR

DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_event_windows" / "event_windows.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_funding_premium_window_research"


@dataclass(frozen=True, slots=True)
class PremiumEntryRule:
    slug: str
    max_wait_h: int
    max_premium_pct: float
    min_premium_relief_1h_pct: float | None = None
    min_return_1h_pct: float | None = None
    min_oi_change_4h_pct: float | None = None
    min_volume_z: float | None = None


@dataclass(frozen=True, slots=True)
class PremiumExitPlan:
    slug: str
    tp_pct: float
    stop_pct: float
    max_hold_h: int
    funding_credit_stop_relief: float


@dataclass(frozen=True, slots=True)
class FilterSpec:
    slug: str
    max_entry_wait_h: float | None = None
    min_oi_change_4h_pct: float | None = None
    min_volume_z: float | None = None
    min_premium_relief_1h_pct: float | None = None
    min_entry_premium_pct: float | None = None
    max_entry_premium_pct: float | None = None
    max_entry_return_1h_pct: float | None = None


ENTRY_RULES: tuple[PremiumEntryRule, ...] = (
    PremiumEntryRule("deep_discount_survives", 72, max_premium_pct=-1.0, min_return_1h_pct=-3.0),
    PremiumEntryRule("deep_discount_oi", 72, max_premium_pct=-1.0, min_return_1h_pct=-5.0, min_oi_change_4h_pct=5.0),
    PremiumEntryRule("premium_relief", 96, max_premium_pct=-0.4, min_premium_relief_1h_pct=0.25, min_return_1h_pct=-2.0),
    PremiumEntryRule("discount_volume_absorption", 72, max_premium_pct=-0.5, min_return_1h_pct=-4.0, min_volume_z=2.5),
)

EXIT_PLANS: tuple[PremiumExitPlan, ...] = (
    PremiumExitPlan("tp30_sl25_hold72_fundrelief", tp_pct=30.0, stop_pct=25.0, max_hold_h=72, funding_credit_stop_relief=0.5),
    PremiumExitPlan("tp60_sl35_hold168_fundrelief", tp_pct=60.0, stop_pct=35.0, max_hold_h=168, funding_credit_stop_relief=0.5),
    PremiumExitPlan("tp100_sl50_hold336_fundrelief", tp_pct=100.0, stop_pct=50.0, max_hold_h=336, funding_credit_stop_relief=0.5),
)

FILTER_SPECS: tuple[FilterSpec, ...] = (
    FilterSpec("all_entries"),
    FilterSpec("wait0", max_entry_wait_h=0.0),
    FilterSpec("wait_le_3h", max_entry_wait_h=3.0),
    FilterSpec("oi_ge_0", min_oi_change_4h_pct=0.0),
    FilterSpec("oi_ge_10", min_oi_change_4h_pct=10.0),
    FilterSpec("oi_ge_10_wait_le_3h", max_entry_wait_h=3.0, min_oi_change_4h_pct=10.0),
    FilterSpec("volume_ge_0_oi_ge_10", min_volume_z=0.0, min_oi_change_4h_pct=10.0),
    FilterSpec("volume_ge_0_wait_le_3h_oi_ge_10", max_entry_wait_h=3.0, min_volume_z=0.0, min_oi_change_4h_pct=10.0),
    FilterSpec("premium_not_toxic_ge_minus5", min_entry_premium_pct=-5.0),
    FilterSpec("premium_not_toxic_oi_wait", max_entry_wait_h=3.0, min_oi_change_4h_pct=0.0, min_entry_premium_pct=-5.0),
    FilterSpec("relief_ge_minus1_oi", min_oi_change_4h_pct=0.0, min_premium_relief_1h_pct=-1.0),
    FilterSpec("relief_ge_0_oi", min_oi_change_4h_pct=0.0, min_premium_relief_1h_pct=0.0),
    FilterSpec("return_le_10_oi_wait", max_entry_wait_h=3.0, min_oi_change_4h_pct=0.0, max_entry_return_1h_pct=10.0),
    FilterSpec("veto_wait_le_30m", max_entry_wait_h=0.5),
    FilterSpec("veto_wait30_oi10", max_entry_wait_h=0.5, min_oi_change_4h_pct=10.0),
    FilterSpec("veto_wait30_volume1", max_entry_wait_h=0.5, min_volume_z=1.0),
    FilterSpec("veto_wait30_premium_band", max_entry_wait_h=0.5, min_entry_premium_pct=-5.0, max_entry_premium_pct=-1.2),
    FilterSpec(
        "veto_core",
        max_entry_wait_h=0.5,
        min_oi_change_4h_pct=10.0,
        min_volume_z=1.0,
        min_entry_premium_pct=-5.0,
        max_entry_premium_pct=-1.2,
    ),
    FilterSpec(
        "veto_core_midpremium",
        max_entry_wait_h=0.5,
        min_oi_change_4h_pct=10.0,
        min_volume_z=1.0,
        min_entry_premium_pct=-3.5,
        max_entry_premium_pct=-1.2,
    ),
    FilterSpec(
        "veto_high_confidence_midpremium",
        max_entry_wait_h=0.5,
        min_oi_change_4h_pct=20.0,
        min_volume_z=1.0,
        min_entry_premium_pct=-3.5,
        max_entry_premium_pct=-1.2,
    ),
    FilterSpec(
        "veto_core_relief_m1",
        max_entry_wait_h=0.5,
        min_oi_change_4h_pct=10.0,
        min_volume_z=1.0,
        min_premium_relief_1h_pct=-1.0,
        min_entry_premium_pct=-5.0,
        max_entry_premium_pct=-1.2,
    ),
    FilterSpec(
        "veto_core_relief_m05",
        max_entry_wait_h=0.5,
        min_oi_change_4h_pct=10.0,
        min_volume_z=1.0,
        min_premium_relief_1h_pct=-0.5,
        min_entry_premium_pct=-5.0,
        max_entry_premium_pct=-1.2,
    ),
    FilterSpec(
        "veto_ultra",
        max_entry_wait_h=0.5,
        min_oi_change_4h_pct=20.0,
        min_volume_z=1.5,
        min_premium_relief_1h_pct=-0.5,
        min_entry_premium_pct=-4.0,
        max_entry_premium_pct=-1.2,
    ),
    FilterSpec(
        "veto_return20_core",
        max_entry_wait_h=0.5,
        min_oi_change_4h_pct=10.0,
        min_volume_z=1.0,
        min_entry_premium_pct=-5.0,
        max_entry_premium_pct=-1.2,
        max_entry_return_1h_pct=20.0,
    ),
    FilterSpec(
        "balanced_candidate",
        max_entry_wait_h=3.0,
        min_oi_change_4h_pct=0.0,
        min_volume_z=0.0,
        min_premium_relief_1h_pct=-1.0,
        min_entry_premium_pct=-5.0,
        max_entry_return_1h_pct=20.0,
    ),
    FilterSpec(
        "strict_candidate",
        max_entry_wait_h=3.0,
        min_oi_change_4h_pct=10.0,
        min_volume_z=0.0,
        min_premium_relief_1h_pct=-1.0,
        min_entry_premium_pct=-5.0,
        max_entry_return_1h_pct=20.0,
    ),
)

PORTFOLIO_STARTING_CAPITAL_USD = 3000.0
PORTFOLIO_LEVERAGE = 2.0


def run_funding_premium_window_research(
    *,
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    samples = read_jsonl(input_path)
    outcome_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    for sample in samples:
        rows = simulate_sample(sample)
        outcome_rows.extend(rows)
        event_rows.append(build_event_summary(sample, rows))
    summary_rows = build_strategy_summary(outcome_rows)
    regression_rows = build_feature_regression(outcome_rows)
    factor_bucket_rows = build_factor_bucket_summary(outcome_rows, summary_rows[:1])
    filter_rows = build_filter_sweep_summary(outcome_rows)
    portfolio_rows, portfolio_trade_rows = build_portfolio_replays(outcome_rows)
    write_csv(output_dir / "premium_long_outcomes.csv", outcome_rows)
    write_csv(output_dir / "premium_long_strategy_summary.csv", summary_rows)
    write_csv(output_dir / "premium_event_summary.csv", event_rows)
    write_csv(output_dir / "premium_feature_regression.csv", regression_rows)
    write_csv(output_dir / "premium_factor_bucket_summary.csv", factor_bucket_rows)
    write_csv(output_dir / "premium_filter_sweep_summary.csv", filter_rows)
    write_csv(output_dir / "premium_portfolio_summary.csv", portfolio_rows)
    write_csv(output_dir / "premium_portfolio_trades.csv", portfolio_trade_rows)
    metadata = {
        "schema": "pump_funding_premium_window_research_v1",
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "samples": len(samples),
        "events_with_candidates": sum(1 for row in event_rows if (to_int(row.get("candidate_outcomes")) or 0) > 0),
        "outcome_rows": len(outcome_rows),
        "strategy_rows": len(summary_rows),
        "regression_rows": len(regression_rows),
        "factor_bucket_rows": len(factor_bucket_rows),
        "filter_rows": len(filter_rows),
        "portfolio_rows": len(portfolio_rows),
        "portfolio_trade_rows": len(portfolio_trade_rows),
        "entry_rules": [rule.slug for rule in ENTRY_RULES],
        "exit_plans": [plan.slug for plan in EXIT_PLANS],
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def simulate_sample(sample: dict[str, Any]) -> list[dict[str, Any]]:
    interval, series = primary_interval(sample)
    if not series:
        return []
    klines = series.get("klines") or []
    premium = series.get("premium_index_klines") or []
    oi = series.get("open_interest") or []
    funding = sample.get("funding") or []
    trigger_ts = to_int(sample.get("trigger_ts")) or 0
    out: list[dict[str, Any]] = []
    for entry_rule in ENTRY_RULES:
        entry = find_entry(klines, premium, oi, trigger_ts, entry_rule)
        if entry is None:
            continue
        for exit_plan in EXIT_PLANS:
            outcome = simulate_exit(sample, interval, klines, premium, funding, entry, entry_rule, exit_plan)
            if outcome:
                out.append(outcome)
    return out


def find_entry(
    klines: list[dict[str, Any]],
    premium: list[dict[str, Any]],
    oi: list[dict[str, Any]],
    trigger_ts: int,
    rule: PremiumEntryRule,
) -> dict[str, Any] | None:
    end_ts = trigger_ts + rule.max_wait_h * HOUR_MS
    for idx, row in enumerate(klines):
        ts_ms = to_int(row.get("ts_ms")) or 0
        if ts_ms < trigger_ts or ts_ms > end_ts:
            continue
        price = to_float(row.get("close"))
        premium_now = value_at_or_before(premium, ts_ms, "close")
        if price is None or premium_now is None or premium_now * 100.0 > rule.max_premium_pct:
            continue
        return_1h = pct_change(price, value_at_or_before(klines, ts_ms - HOUR_MS, "close"))
        if rule.min_return_1h_pct is not None and (return_1h is None or return_1h < rule.min_return_1h_pct):
            continue
        premium_1h = value_at_or_before(premium, ts_ms - HOUR_MS, "close")
        relief = (premium_now - premium_1h) * 100.0 if premium_1h is not None else None
        if rule.min_premium_relief_1h_pct is not None and (relief is None or relief < rule.min_premium_relief_1h_pct):
            continue
        oi_now = value_at_or_before(oi, ts_ms, "open_interest")
        oi_4h = value_at_or_before(oi, ts_ms - 4 * HOUR_MS, "open_interest")
        oi_change_4h = pct_change(oi_now, oi_4h)
        if rule.min_oi_change_4h_pct is not None and (oi_change_4h is None or oi_change_4h < rule.min_oi_change_4h_pct):
            continue
        volume_z = volume_z_at(klines, idx, lookback=96)
        if rule.min_volume_z is not None and (volume_z is None or volume_z < rule.min_volume_z):
            continue
        return {
            "idx": idx,
            "ts_ms": ts_ms,
            "price": price,
            "premium_pct": premium_now * 100.0,
            "premium_relief_1h_pct": relief,
            "return_1h_pct": return_1h,
            "oi_change_4h_pct": oi_change_4h,
            "volume_z": volume_z,
        }
    return None


def simulate_exit(
    sample: dict[str, Any],
    interval: str,
    klines: list[dict[str, Any]],
    premium: list[dict[str, Any]],
    funding: list[dict[str, Any]],
    entry: dict[str, Any],
    entry_rule: PremiumEntryRule,
    exit_plan: PremiumExitPlan,
) -> dict[str, Any] | None:
    entry_idx = int(entry["idx"])
    entry_ts = int(entry["ts_ms"])
    entry_price = float(entry["price"])
    end_ts = entry_ts + exit_plan.max_hold_h * HOUR_MS
    base_stop_pct = exit_plan.stop_pct
    exit_idx = entry_idx
    exit_price = to_float(klines[entry_idx].get("close"))
    exit_reason = "time_stop"
    for idx in range(entry_idx + 1, len(klines)):
        row = klines[idx]
        ts_ms = to_int(row.get("ts_ms")) or 0
        if ts_ms > end_ts:
            break
        funding_credit_pct = -scale_pct(sum_funding_between(funding, entry_ts, ts_ms) or 0.0)
        dynamic_stop_pct = base_stop_pct + max(0.0, funding_credit_pct) * exit_plan.funding_credit_stop_relief
        stop_price = entry_price * (1.0 - dynamic_stop_pct / 100.0)
        tp_price = entry_price * (1.0 + exit_plan.tp_pct / 100.0)
        low = to_float(row.get("low"))
        high = to_float(row.get("high"))
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
        exit_idx = idx
        exit_price = to_float(row.get("close"))
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
        "entry_rule": entry_rule.slug,
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
        "exit_ts": exit_ts,
        "exit_iso": ms_to_iso(exit_ts),
        "exit_h": round_float((exit_ts - entry_ts) / HOUR_MS),
        "exit_price": round_float(exit_price),
        "exit_reason": exit_reason,
        "gross_price_pct": round_float(gross_pct),
        "funding_sum_pct": round_float(funding_sum_pct),
        "long_funding_pct": round_float(long_funding_pct),
        "net_pct": round_float(net_pct),
        "mfe_pct": round_float((high_during / entry_price - 1.0) * 100.0 if high_during else None),
        "mae_pct": round_float((1.0 - low_during / entry_price) * 100.0 if low_during else None),
        "exit_premium_pct": round_float((value_at_or_before(premium, exit_ts, "close") or 0.0) * 100.0),
    }


def build_strategy_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((str(row.get("entry_rule")), str(row.get("exit_plan"))), []).append(row)
    out = []
    for (entry_rule, exit_plan), items in sorted(groups.items()):
        nets = values(items, "net_pct")
        out.append(
            {
                "entry_rule": entry_rule,
                "exit_plan": exit_plan,
                "trades": len(items),
                "win_pct": pct(sum(1 for value in nets if value > 0), len(nets)),
                "avg_net_pct": round_float(statistics.mean(nets) if nets else None),
                "median_net_pct": round_float(statistics.median(nets) if nets else None),
                "min_net_pct": round_float(min(nets) if nets else None),
                "max_net_pct": round_float(max(nets) if nets else None),
                "avg_long_funding_pct": round_float(statistics.mean(values(items, "long_funding_pct")) if items else None),
                "median_entry_premium_pct": round_float(statistics.median(values(items, "entry_premium_pct")) if items else None),
                "take_profit_pct": pct(sum(1 for row in items if row.get("exit_reason") == "take_profit"), len(items)),
                "stop_loss_pct": pct(sum(1 for row in items if row.get("exit_reason") == "stop_loss"), len(items)),
            }
        )
    out.sort(key=lambda row: (to_float(row.get("median_net_pct")) or -10**9, to_float(row.get("avg_net_pct")) or -10**9), reverse=True)
    return out


def build_feature_regression(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    features = [
        "trigger_pump_pct",
        "entry_wait_h",
        "entry_premium_pct",
        "entry_premium_relief_1h_pct",
        "entry_return_1h_pct",
        "entry_oi_change_4h_pct",
        "entry_volume_z",
        "long_funding_pct",
        "exit_premium_pct",
    ]
    targets = [
        ("net_pct", lambda row: to_float(row.get("net_pct"))),
        ("mae_pct", lambda row: to_float(row.get("mae_pct"))),
        ("win_label", lambda row: 1.0 if (to_float(row.get("net_pct")) or 0.0) > 0.0 else 0.0),
        ("stop_label", lambda row: 1.0 if row.get("exit_reason") == "stop_loss" else 0.0),
    ]
    groups: dict[str, list[dict[str, Any]]] = {"all": rows}
    for row in rows:
        groups.setdefault(str(row.get("exit_plan") or ""), []).append(row)
        groups.setdefault(str(row.get("entry_rule") or ""), []).append(row)

    out: list[dict[str, Any]] = []
    for group, group_rows in sorted(groups.items()):
        if len(group_rows) < 30:
            continue
        usable_features = [
            feature
            for feature in features
            if sum(1 for row in group_rows if to_float(row.get(feature)) is not None) >= max(30, int(len(group_rows) * 0.5))
        ]
        if not usable_features:
            continue
        for target, getter in targets:
            xs: list[list[float]] = []
            y: list[float] = []
            for row in group_rows:
                target_value = getter(row)
                feature_values = [to_float(row.get(feature)) for feature in usable_features]
                if target_value is None or any(value is None for value in feature_values):
                    continue
                y.append(float(target_value))
                xs.append([float(value) for value in feature_values if value is not None])
            if len(xs) < 30:
                continue
            model = standardized_ridge(xs, y, alpha=1.0)
            for feature, coefficient in zip(usable_features, model["coefficients"]):
                out.append(
                    {
                        "group": group,
                        "target": target,
                        "feature": feature,
                        "n": len(xs),
                        "standardized_coefficient": round_float(coefficient),
                        "abs_coefficient": round_float(abs(coefficient)),
                        "r2": round_float(model["r2"]),
                        "target_mean": round_float(statistics.mean(y)),
                        "interpretation": regression_note(target, feature, coefficient),
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
        "entry_wait_h",
        "entry_premium_pct",
        "entry_premium_relief_1h_pct",
        "entry_return_1h_pct",
        "entry_oi_change_4h_pct",
        "entry_volume_z",
        "long_funding_pct",
    ]:
        pairs = [(to_float(row.get(feature)), row) for row in filtered]
        clean = [(value, row) for value, row in pairs if value is not None]
        if len(clean) < 30:
            continue
        clean.sort(key=lambda item: item[0])
        chunk_size = max(1, math.ceil(len(clean) / 3))
        chunks = [
            ("low", clean[:chunk_size]),
            ("mid", clean[chunk_size : chunk_size * 2]),
            ("high", clean[chunk_size * 2 :]),
        ]
        for bucket, chunk in chunks:
            items = [row for _value, row in chunk]
            bucket_values = [value for value, _row in chunk]
            nets = values(items, "net_pct")
            out.append(
                {
                    "entry_rule": top.get("entry_rule"),
                    "exit_plan": top.get("exit_plan"),
                    "feature": feature,
                    "bucket": bucket,
                    "bucket_min": round_float(min(bucket_values) if bucket_values else None),
                    "bucket_max": round_float(max(bucket_values) if bucket_values else None),
                    "trades": len(items),
                    "win_pct": pct(sum(1 for value in nets if value > 0), len(nets)),
                    "avg_net_pct": round_float(statistics.mean(nets) if nets else None),
                    "median_net_pct": round_float(statistics.median(nets) if nets else None),
                    "min_net_pct": round_float(min(nets) if nets else None),
                    "max_net_pct": round_float(max(nets) if nets else None),
                    "avg_long_funding_pct": round_float(statistics.mean(values(items, "long_funding_pct")) if items else None),
                    "take_profit_pct": pct(sum(1 for row in items if row.get("exit_reason") == "take_profit"), len(items)),
                    "stop_loss_pct": pct(sum(1 for row in items if row.get("exit_reason") == "stop_loss"), len(items)),
                }
            )
    out.sort(key=lambda row: (str(row.get("feature")), str(row.get("bucket"))))
    return out


def build_filter_sweep_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        for spec in FILTER_SPECS:
            if filter_matches(row, spec):
                groups.setdefault((str(row.get("entry_rule")), str(row.get("exit_plan")), spec.slug), []).append(row)
    for (entry_rule, exit_plan, filter_slug), items in sorted(groups.items()):
        if not items:
            continue
        ordered = sorted(items, key=lambda row: (to_int(row.get("entry_ts")) or 0, str(row.get("symbol") or "")))
        split_idx = max(1, int(len(ordered) * 0.7))
        train = ordered[:split_idx]
        test = ordered[split_idx:]
        nets = values(items, "net_pct")
        test_nets = values(test, "net_pct")
        out.append(
            {
                "entry_rule": entry_rule,
                "exit_plan": exit_plan,
                "filter_slug": filter_slug,
                "trades": len(items),
                "win_pct": pct(sum(1 for value in nets if value > 0), len(nets)),
                "avg_net_pct": round_float(statistics.mean(nets) if nets else None),
                "median_net_pct": round_float(statistics.median(nets) if nets else None),
                "min_net_pct": round_float(min(nets) if nets else None),
                "max_net_pct": round_float(max(nets) if nets else None),
                "avg_long_funding_pct": round_float(statistics.mean(values(items, "long_funding_pct")) if items else None),
                "take_profit_pct": pct(sum(1 for row in items if row.get("exit_reason") == "take_profit"), len(items)),
                "stop_loss_pct": pct(sum(1 for row in items if row.get("exit_reason") == "stop_loss"), len(items)),
                "train_trades": len(train),
                "test_trades": len(test),
                "test_win_pct": pct(sum(1 for value in test_nets if value > 0), len(test_nets)),
                "test_avg_net_pct": round_float(statistics.mean(test_nets) if test_nets else None),
                "test_median_net_pct": round_float(statistics.median(test_nets) if test_nets else None),
                "test_min_net_pct": round_float(min(test_nets) if test_nets else None),
                "score": round_float(filter_score(items, test)),
            }
        )
    out.sort(
        key=lambda row: (
            to_float(row.get("score")) or -10**9,
            to_float(row.get("test_median_net_pct")) or -10**9,
            to_float(row.get("trades")) or 0.0,
        ),
        reverse=True,
    )
    return out


def build_portfolio_replays(
    rows: list[dict[str, Any]],
    *,
    starting_capital_usd: float = PORTFOLIO_STARTING_CAPITAL_USD,
    leverage: float = PORTFOLIO_LEVERAGE,
    slot_counts: tuple[int, ...] = (1, 2, 3, 4, 5),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        for spec in FILTER_SPECS:
            if filter_matches(row, spec):
                groups.setdefault((str(row.get("entry_rule")), str(row.get("exit_plan")), spec.slug), []).append(row)
    for (entry_rule, exit_plan, filter_slug), items in sorted(groups.items()):
        ordered = sorted(items, key=lambda row: (to_int(row.get("entry_ts")) or 0, str(row.get("symbol") or ""), str(row.get("event_id") or "")))
        for slots in slot_counts:
            summary, trades = replay_one_portfolio(
                ordered,
                entry_rule=entry_rule,
                exit_plan=exit_plan,
                filter_slug=filter_slug,
                slots=slots,
                starting_capital_usd=starting_capital_usd,
                leverage=leverage,
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


def replay_one_portfolio(
    rows: list[dict[str, Any]],
    *,
    entry_rule: str,
    exit_plan: str,
    filter_slug: str,
    slots: int,
    starting_capital_usd: float,
    leverage: float,
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
        closed = [trade for trade in active if (to_int(trade.get("exit_ts")) or 0) <= ts_ms]
        active = [trade for trade in active if (to_int(trade.get("exit_ts")) or 0) > ts_ms]
        for trade in sorted(closed, key=lambda item: to_int(item.get("exit_ts")) or 0):
            realized_pnl += float(trade["pnl_usd"])
            equity = starting_capital_usd + realized_pnl
            peak_equity = max(peak_equity, equity)
            max_drawdown = max(max_drawdown, peak_equity - equity)

    for row in rows:
        entry_ts = to_int(row.get("entry_ts")) or 0
        exit_ts = to_int(row.get("exit_ts")) or 0
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
        net_pct = to_float(row.get("net_pct"))
        if net_pct is None:
            continue
        levered_net_pct = net_pct * leverage
        pnl_usd = slot_budget * levered_net_pct / 100.0
        worst_trade_usd = pnl_usd if worst_trade_usd is None else min(worst_trade_usd, pnl_usd)
        worst_trade_pct = levered_net_pct if worst_trade_pct is None else min(worst_trade_pct, levered_net_pct)
        trade = {
            "entry_rule": entry_rule,
            "exit_plan": exit_plan,
            "filter_slug": filter_slug,
            "slots": slots,
            "leverage": leverage,
            "slot_budget_usd": round_float(slot_budget),
            "symbol": symbol,
            "event_id": row.get("event_id"),
            "entry_ts": entry_ts,
            "entry_iso": row.get("entry_iso"),
            "exit_ts": exit_ts,
            "exit_iso": row.get("exit_iso"),
            "exit_reason": row.get("exit_reason"),
            "net_pct": row.get("net_pct"),
            "levered_net_pct": round_float(levered_net_pct),
            "pnl_usd": round_float(pnl_usd),
            "long_funding_pct": row.get("long_funding_pct"),
            "entry_premium_pct": row.get("entry_premium_pct"),
            "entry_oi_change_4h_pct": row.get("entry_oi_change_4h_pct"),
            "entry_volume_z": row.get("entry_volume_z"),
            "entry_wait_h": row.get("entry_wait_h"),
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
        "filter_slug": filter_slug,
        "slots": slots,
        "starting_capital_usd": round_float(starting_capital_usd),
        "slot_budget_usd": round_float(slot_budget),
        "leverage": leverage,
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
        "avg_trade_usd": round_float(statistics.mean(values(trades, "pnl_usd")) if trades else None),
        "take_profit_pct": pct(sum(1 for row in trades if row.get("exit_reason") == "take_profit"), len(trades)),
        "stop_loss_pct": pct(sum(1 for row in trades if row.get("exit_reason") == "stop_loss"), len(trades)),
    }
    return summary, trades


def filter_matches(row: dict[str, Any], spec: FilterSpec) -> bool:
    checks = [
        (spec.max_entry_wait_h, row.get("entry_wait_h"), lambda value, limit: value <= limit),
        (spec.min_oi_change_4h_pct, row.get("entry_oi_change_4h_pct"), lambda value, limit: value >= limit),
        (spec.min_volume_z, row.get("entry_volume_z"), lambda value, limit: value >= limit),
        (spec.min_premium_relief_1h_pct, row.get("entry_premium_relief_1h_pct"), lambda value, limit: value >= limit),
        (spec.min_entry_premium_pct, row.get("entry_premium_pct"), lambda value, limit: value >= limit),
        (spec.max_entry_premium_pct, row.get("entry_premium_pct"), lambda value, limit: value <= limit),
        (spec.max_entry_return_1h_pct, row.get("entry_return_1h_pct"), lambda value, limit: value <= limit),
    ]
    for limit, raw_value, predicate in checks:
        if limit is None:
            continue
        value = to_float(raw_value)
        if value is None or not predicate(value, limit):
            return False
    return True


def filter_score(items: list[dict[str, Any]], test_items: list[dict[str, Any]]) -> float:
    nets = values(items, "net_pct")
    test_nets = values(test_items, "net_pct")
    if not nets:
        return -10**9
    trade_penalty = max(0.0, 30.0 - len(items)) * 0.5
    median_net = statistics.median(nets)
    test_median = statistics.median(test_nets) if test_nets else median_net
    win = sum(1 for value in nets if value > 0) / len(nets) * 100.0
    stop = sum(1 for row in items if row.get("exit_reason") == "stop_loss") / len(items) * 100.0
    worst = min(nets)
    return median_net * 0.45 + test_median * 0.35 + win * 0.12 + worst * 0.08 - stop * 0.06 - trade_penalty


def build_event_summary(sample: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    best = max(rows, key=lambda row: to_float(row.get("net_pct")) or -10**9) if rows else {}
    return {
        "event_id": (sample.get("event") or {}).get("event_id") if isinstance(sample.get("event"), dict) else None,
        "symbol": sample.get("symbol"),
        "trigger_iso": sample.get("trigger_iso"),
        "trigger_pump_pct": (sample.get("event") or {}).get("trigger_pump_pct") if isinstance(sample.get("event"), dict) else None,
        "candidate_outcomes": len(rows),
        "best_entry_rule": best.get("entry_rule"),
        "best_exit_plan": best.get("exit_plan"),
        "best_net_pct": best.get("net_pct"),
        "best_long_funding_pct": best.get("long_funding_pct"),
        "best_exit_reason": best.get("exit_reason"),
    }


def primary_interval(sample: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    intervals = sample.get("intervals") if isinstance(sample.get("intervals"), dict) else {}
    if not intervals:
        return "", {}
    key = sorted(intervals.keys(), key=lambda item: int(str(item).replace("min", "").replace("m", "")))[0]
    return str(key), intervals[key]


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


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


def sum_funding_between(rows: list[dict[str, Any]], start_ms: int, end_ms: int) -> float | None:
    values_out = [
        to_float(row.get("funding_rate"))
        for row in rows
        if start_ms <= (to_int(row.get("ts_ms")) or -1) <= end_ms
    ]
    clean = [value for value in values_out if value is not None]
    return sum(clean) if clean else None


def volume_z_at(rows: list[dict[str, Any]], idx: int, *, lookback: int) -> float | None:
    volume = to_float(rows[idx].get("volume"))
    history = [to_float(row.get("volume")) for row in rows[max(0, idx - lookback) : idx]]
    clean = [value for value in history if value is not None]
    if volume is None or len(clean) < 10:
        return None
    mean = statistics.mean(clean)
    stdev = statistics.pstdev(clean) or 1.0
    return (volume - mean) / stdev


def max_clean(rows: list[dict[str, Any]], key: str) -> float | None:
    clean = [to_float(row.get(key)) for row in rows]
    clean = [value for value in clean if value is not None]
    return max(clean) if clean else None


def min_clean(rows: list[dict[str, Any]], key: str) -> float | None:
    clean = [to_float(row.get(key)) for row in rows]
    clean = [value for value in clean if value is not None]
    return min(clean) if clean else None


def values(rows: list[dict[str, Any]], key: str) -> list[float]:
    out = []
    for row in rows:
        value = to_float(row.get(key))
        if value is not None:
            out.append(value)
    return out


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
    xty = [0.0 for _idx in range(cols)]
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


def regression_note(target: str, feature: str, coefficient: float) -> str:
    direction = "raises" if coefficient > 0 else "lowers"
    if target == "net_pct":
        return f"higher {feature} {direction} expected net result"
    if target == "mae_pct":
        return f"higher {feature} {direction} adverse excursion"
    if target == "win_label":
        return f"higher {feature} {direction} win probability"
    if target == "stop_label":
        return f"higher {feature} {direction} stop probability"
    return f"higher {feature} {direction} {target}"


def ms_to_iso(ts_ms: int | None) -> str:
    if ts_ms is None:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


__all__ = [
    "ENTRY_RULES",
    "EXIT_PLANS",
    "FILTER_SPECS",
    "FilterSpec",
    "PremiumEntryRule",
    "PremiumExitPlan",
    "find_entry",
    "build_factor_bucket_summary",
    "build_feature_regression",
    "build_filter_sweep_summary",
    "build_portfolio_replays",
    "filter_matches",
    "run_funding_premium_window_research",
    "simulate_sample",
]
