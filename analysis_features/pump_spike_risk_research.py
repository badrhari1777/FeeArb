from __future__ import annotations

import csv
import json
import math
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from analysis_collectors.bybit_pump_short import NON_CRYPTO_BASE_COINS
from analysis_features.pump_live_transition_research import (
    DEFAULT_PER_EVENT_DIR,
    DEFAULT_PULLBACK_DIR,
    START_TS_MS,
    STRATEGIES,
    load_selected_pullback_outcomes,
    passes_online_gates,
    select_tier,
    to_float,
    to_int,
    wanted_outcome_keys,
)
from analysis_features.pump_short_policy_portfolio_research import (
    build_unique_cases,
    load_csv,
)
from config import BASE_DIR

DEFAULT_UNIVERSE_DIR = (
    BASE_DIR
    / "data"
    / "research"
    / "pump_short_multiexchange_2024_clean"
    / "bybit"
)
DEFAULT_EVENT_WINDOWS = (
    BASE_DIR / "data" / "research" / "bybit_pump_event_windows" / "event_windows.jsonl"
)
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_spike_risk_research"

HOUR_MS = 3_600_000
FIFTEEN_MIN_MS = 900_000
SPIKE_THRESHOLDS_PCT = (10.0, 20.0, 30.0, 50.0, 100.0)
SPIKE_EPISODE_GAP_H = 6
ACTIVE_SPIKE_MIN_PCT = 20.0
LEVERAGE = 3.0
MMR = 0.025
TAKER_FEE_RATE = 0.00055
STOP_GAP_PCT = 2.5
SLOT_MARGIN_USD = 175.0
GUARANTEED_TOPUP_USD = 50.0


def run_pump_spike_risk_research(
    *,
    universe_dir: Path = DEFAULT_UNIVERSE_DIR,
    per_event_dir: Path = DEFAULT_PER_EVENT_DIR,
    pullback_dir: Path = DEFAULT_PULLBACK_DIR,
    event_windows_path: Path = DEFAULT_EVENT_WINDOWS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    started = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)

    hourly_summary, hourly_events, universe_coverage = analyze_hourly_universe(
        universe_dir / "symbol_samples.jsonl",
        done_symbols_path=universe_dir / "done_symbols.txt",
    )
    case_rows, spike_bars, strategy_coverage = analyze_strategy_cases(
        per_event_dir=per_event_dir,
        pullback_dir=pullback_dir,
        event_windows_path=event_windows_path,
    )
    tier_summary = summarize_strategy_tiers(case_rows)
    protection_rows = build_protection_comparison(case_rows)

    write_csv(output_dir / "universe_hourly_spike_summary.csv", hourly_summary)
    write_csv(output_dir / "universe_hourly_spike_events.csv", hourly_events)
    write_csv(output_dir / "strategy_case_spike_risk.csv", case_rows)
    write_csv(output_dir / "strategy_active_spike_bars.csv", spike_bars)
    write_csv(output_dir / "strategy_tier_summary.csv", tier_summary)
    write_csv(output_dir / "protection_comparison.csv", protection_rows)

    metadata = {
        "schema": "pump_spike_risk_research_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "start_iso": "2024-01-01T00:00:00+00:00",
        "definitions": {
            "hourly_surge": (
                "hourly high is at least threshold_pct above the previous "
                "continuous hourly close"
            ),
            "hourly_spike_episode": (
                f"qualifying bars for one symbol clustered when separated by no more "
                f"than {SPIKE_EPISODE_GAP_H} hours"
            ),
            "wick_retrace": "episode contains a candle closing at least 10% below its high",
            "active_15m_spike": (
                "15m mark high is at least threshold_pct above max(candle open, previous close) "
                "while the reconstructed current-tier short is active"
            ),
            "fast_protection_crossing": (
                "first warning-to-initial-stop crossings are no more than 15 minutes apart; "
                "15m OHLC cannot prove whether the real move was faster or slower than 15 seconds"
            ),
        },
        "assumptions": {
            "leverage": LEVERAGE,
            "mmr": MMR,
            "stop_gap_pct": STOP_GAP_PCT,
            "slot_margin_usd": SLOT_MARGIN_USD,
            "guaranteed_topup_usd": GUARANTEED_TOPUP_USD,
        },
        "universe_coverage": universe_coverage,
        "strategy_coverage": strategy_coverage,
        "hourly_summary_rows": len(hourly_summary),
        "hourly_event_rows": len(hourly_events),
        "strategy_case_rows": len(case_rows),
        "strategy_spike_bar_rows": len(spike_bars),
        "tier_summary_rows": len(tier_summary),
        "protection_rows": len(protection_rows),
        "limitations": [
            "current-listing survivor bias: contracts delisted before collection are absent",
            "217 of 602 symbols failed the historical daily pump prefilter and have no hourly archive",
            "hourly universe counts describe market surges, not strategy exposure",
            "strategy speed classification is limited to archived 15-minute Mark Price candles",
            "intrabar ordering and moves faster than one minute are unavailable offline",
            "fills, slippage, and exchange matching order inside one candle are not reconstructed",
        ],
        "elapsed_sec": round(time.time() - started, 3),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "index.md").write_text(
        render_report(
            metadata=metadata,
            hourly_summary=hourly_summary,
            case_rows=case_rows,
            tier_summary=tier_summary,
            protection_rows=protection_rows,
        ),
        encoding="utf-8",
    )
    return metadata


def analyze_hourly_universe(
    samples_path: Path,
    *,
    done_symbols_path: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    threshold_bars: dict[float, list[dict[str, Any]]] = {
        threshold: [] for threshold in SPIKE_THRESHOLDS_PCT
    }
    collected_symbols = 0
    candle_rows = 0
    continuous_pairs = 0
    min_ts: int | None = None
    max_ts: int | None = None

    with samples_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            instrument = dict(payload.get("instrument") or {})
            base = str(instrument.get("base") or "").upper()
            if base in NON_CRYPTO_BASE_COINS:
                continue
            symbol = str(payload.get("symbol") or "")
            candles = list((payload.get("series") or {}).get("klines_1h") or [])
            if not symbol or not candles:
                continue
            collected_symbols += 1
            previous: tuple[int, float] | None = None
            for candle in candles:
                ts_ms = to_int(candle.get("ts_ms"))
                high = finite_float(candle.get("high"))
                close = finite_float(candle.get("close"))
                if ts_ms is None or high is None or close is None or close <= 0:
                    continue
                candle_rows += 1
                min_ts = ts_ms if min_ts is None else min(min_ts, ts_ms)
                max_ts = ts_ms if max_ts is None else max(max_ts, ts_ms)
                if previous is not None:
                    previous_ts, previous_close = previous
                    delta = ts_ms - previous_ts
                    if (
                        3_000_000 <= delta <= 7_200_000
                        and previous_close > 0
                        and high > 0
                    ):
                        continuous_pairs += 1
                        rise_pct = (high / previous_close - 1.0) * 100.0
                        retrace_pct = max(0.0, (high / close - 1.0) * 100.0)
                        for threshold in SPIKE_THRESHOLDS_PCT:
                            if rise_pct + 1e-12 >= threshold:
                                threshold_bars[threshold].append(
                                    {
                                        "symbol": symbol,
                                        "ts_ms": ts_ms,
                                        "ts_iso": ms_to_iso(ts_ms),
                                        "rise_pct": round(rise_pct, 8),
                                        "retrace_from_high_pct": round(retrace_pct, 8),
                                        "previous_close": previous_close,
                                        "high": high,
                                        "close": close,
                                    }
                                )
                previous = (ts_ms, close)

    summary_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    for threshold in SPIKE_THRESHOLDS_PCT:
        episodes = cluster_spike_bars(
            threshold_bars[threshold],
            max_gap_ms=SPIKE_EPISODE_GAP_H * HOUR_MS,
        )
        summary_rows.append(
            {
                "threshold_pct": threshold,
                "qualifying_bars": len(threshold_bars[threshold]),
                "episodes": len(episodes),
                "symbols": len({str(item["symbol"]) for item in episodes}),
                "wick_episodes_retrace_10pct": sum(
                    1 for item in episodes if float(item["max_retrace_pct"]) >= 10.0
                ),
                "max_rise_pct": round(
                    max((float(item["max_rise_pct"]) for item in episodes), default=0.0),
                    8,
                ),
            }
        )
        if threshold == 30.0:
            event_rows.extend(episodes)

    checked_symbols = 0
    if done_symbols_path and done_symbols_path.exists():
        checked_symbols = len(
            {
                line.strip()
                for line in done_symbols_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            }
        )
    coverage = {
        "symbols_checked_by_collector": checked_symbols,
        "symbols_with_hourly_archive": collected_symbols,
        "symbols_prefiltered_without_hourly_archive": max(
            0, checked_symbols - collected_symbols
        ),
        "hourly_candles": candle_rows,
        "continuous_hour_pairs": continuous_pairs,
        "first_candle_iso": ms_to_iso(min_ts),
        "last_candle_iso": ms_to_iso(max_ts),
    }
    return summary_rows, event_rows, coverage


def cluster_spike_bars(
    bars: Iterable[Mapping[str, Any]],
    *,
    max_gap_ms: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in bars:
        grouped[str(item.get("symbol") or "")].append(item)
    episodes: list[dict[str, Any]] = []
    for symbol, rows in grouped.items():
        ordered = sorted(rows, key=lambda item: to_int(item.get("ts_ms")) or 0)
        current: list[Mapping[str, Any]] = []
        for item in ordered:
            ts_ms = to_int(item.get("ts_ms")) or 0
            previous_ts = to_int(current[-1].get("ts_ms")) if current else None
            if current and previous_ts is not None and ts_ms - previous_ts > max_gap_ms:
                episodes.append(compact_episode(symbol, current))
                current = []
            current.append(item)
        if current:
            episodes.append(compact_episode(symbol, current))
    episodes.sort(key=lambda item: (to_int(item.get("start_ts")) or 0, item["symbol"]))
    return episodes


def compact_episode(symbol: str, rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    max_row = max(rows, key=lambda item: finite_float(item.get("rise_pct")) or 0.0)
    start_ts = min(to_int(item.get("ts_ms")) or 0 for item in rows)
    end_ts = max(to_int(item.get("ts_ms")) or 0 for item in rows)
    return {
        "symbol": symbol,
        "start_ts": start_ts,
        "start_iso": ms_to_iso(start_ts),
        "end_ts": end_ts,
        "end_iso": ms_to_iso(end_ts),
        "bars": len(rows),
        "max_rise_pct": round(
            max(finite_float(item.get("rise_pct")) or 0.0 for item in rows), 8
        ),
        "max_retrace_pct": round(
            max(
                finite_float(item.get("retrace_from_high_pct")) or 0.0
                for item in rows
            ),
            8,
        ),
        "peak_ts": to_int(max_row.get("ts_ms")),
        "peak_iso": max_row.get("ts_iso"),
        "peak_high": max_row.get("high"),
        "peak_close": max_row.get("close"),
    }


def analyze_strategy_cases(
    *,
    per_event_dir: Path,
    pullback_dir: Path,
    event_windows_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    cases = [
        row
        for row in build_unique_cases(load_csv(per_event_dir / "per_event_summary.csv"))
        if to_int(row.get("entry_ts")) >= START_TS_MS
    ]
    main_spec = next(
        item for item in STRATEGIES if item.strategy_id == "main_pullback_tier"
    )
    outcomes = load_selected_pullback_outcomes(
        pullback_dir / "pullback_all_outcomes.csv",
        wanted_outcome_keys(cases, STRATEGIES),
    )

    selected: list[tuple[dict[str, Any], Any, dict[str, Any], bool, str]] = []
    for case in cases:
        pump_pct = to_float(case.get("pump_pct"))
        tier = select_tier(main_spec, pump_pct) if pump_pct is not None else None
        if tier is None:
            continue
        outcome = outcomes.get(
            (
                str(case.get("case_id") or ""),
                int(tier.pullback_pct),
                tier.rule_slug,
            )
        )
        if not outcome:
            continue
        gate_passed, gate_reason = passes_online_gates(main_spec, case)
        selected.append((case, tier, outcome, gate_passed, gate_reason))

    wanted_ids = {str(item[0].get("event_id") or "") for item in selected}
    windows: dict[str, dict[str, Any]] = {}
    with event_windows_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            event_id = str((payload.get("event") or {}).get("event_id") or "")
            if event_id in wanted_ids:
                windows[event_id] = payload

    case_rows: list[dict[str, Any]] = []
    spike_bar_map: dict[tuple[str, int], dict[str, Any]] = {}
    for case, tier, outcome, gate_passed, gate_reason in selected:
        event_id = str(case.get("event_id") or "")
        window = windows.get(event_id)
        parsed_rule = parse_rule(tier.rule_slug)
        first_notional = first_leg_notional_usd(
            tier.rule_slug,
            slot_margin_usd=SLOT_MARGIN_USD,
            leverage=LEVERAGE,
        )
        ratios = protection_ratios(
            first_leg_notional_usd=first_notional,
            extra_margin_usd=GUARANTEED_TOPUP_USD,
        )
        base_row = {
            "case_id": case.get("case_id"),
            "event_id": event_id,
            "symbol": case.get("symbol"),
            "trigger_iso": case.get("trigger_iso"),
            "entry_ts": to_int(outcome.get("entry_ts")),
            "entry_iso": outcome.get("entry_iso"),
            "exit_ts": to_int(outcome.get("exit_ts")),
            "exit_iso": outcome.get("exit_iso"),
            "pump_pct": rounded(to_float(case.get("pump_pct"))),
            "tier_bucket": pump_tier_bucket(to_float(case.get("pump_pct")) or 0.0),
            "rule_slug": tier.rule_slug,
            "ladder_legs": parsed_rule["legs"],
            "gate_passed": gate_passed,
            "gate_reason": gate_reason,
            "first_leg_notional_usd": rounded(first_notional),
            "required_topup_to_protect_l2_usd": rounded(
                required_topup_for_l2(first_leg_notional_usd=first_notional)
            ),
            "initial_warning_pct": rounded((ratios["warning"] - 1.0) * 100.0),
            "initial_stop_pct": rounded((ratios["initial_stop"] - 1.0) * 100.0),
            "stop_after_50_pct": rounded((ratios["stop_after_topup"] - 1.0) * 100.0),
            "l2_pct": 50.0,
            "window_available": bool(window),
        }
        if not window:
            case_rows.append(base_row)
            continue

        candles = list(
            ((window.get("intervals") or {}).get("15") or {}).get(
                "mark_price_klines"
            )
            or []
        )
        entry_ts = to_int(outcome.get("entry_ts")) or 0
        exit_ts = to_int(outcome.get("exit_ts")) or (1 << 63) - 1
        active = [
            item
            for item in candles
            if entry_ts <= (to_int(item.get("ts_ms")) or 0) <= exit_ts
        ]
        path = analyze_mark_path(active, ratios=ratios)
        case_rows.append({**base_row, **path})

        previous_close: float | None = None
        for candle in active:
            ts_ms = to_int(candle.get("ts_ms"))
            open_price = finite_float(candle.get("open"))
            high = finite_float(candle.get("high"))
            close = finite_float(candle.get("close"))
            if (
                ts_ms is None
                or open_price is None
                or high is None
                or close is None
                or open_price <= 0
                or close <= 0
            ):
                continue
            base = max(open_price, previous_close or open_price)
            rise_pct = (high / base - 1.0) * 100.0
            if rise_pct >= ACTIVE_SPIKE_MIN_PCT:
                key = (str(case.get("symbol") or ""), ts_ms)
                row = spike_bar_map.setdefault(
                    key,
                    {
                        "symbol": case.get("symbol"),
                        "ts_ms": ts_ms,
                        "ts_iso": ms_to_iso(ts_ms),
                        "rise_pct": rounded(rise_pct),
                        "retrace_from_high_pct": rounded(
                            max(0.0, (high / close - 1.0) * 100.0)
                        ),
                        "high": high,
                        "close": close,
                        "case_ids": [],
                        "gate_passed_case": False,
                    },
                )
                row["case_ids"].append(str(case.get("case_id") or ""))
                row["gate_passed_case"] = bool(
                    row["gate_passed_case"] or gate_passed
                )
                row["rise_pct"] = max(float(row["rise_pct"]), rounded(rise_pct) or 0.0)
            previous_close = close

    spike_bars: list[dict[str, Any]] = []
    for row in spike_bar_map.values():
        row["case_ids"] = "|".join(sorted(set(row["case_ids"])))
        spike_bars.append(row)
    spike_bars.sort(key=lambda item: (to_int(item.get("ts_ms")) or 0, item["symbol"]))
    case_rows.sort(key=lambda item: (to_int(item.get("entry_ts")) or 0, item["symbol"]))

    coverage = {
        "unique_cases_since_2024": len(cases),
        "current_tier_outcomes": len(selected),
        "event_windows_loaded": len(windows),
        "cases_with_15m_window": sum(
            1 for item in case_rows if item.get("window_available")
        ),
        "gate_passed_cases": sum(1 for item in case_rows if item.get("gate_passed")),
        "gate_passed_cases_with_15m_window": sum(
            1
            for item in case_rows
            if item.get("gate_passed") and item.get("window_available")
        ),
        "active_unique_spike_bars_ge_20pct": len(spike_bars),
        "first_entry_iso": next(
            (item.get("entry_iso") for item in case_rows if item.get("entry_iso")),
            None,
        ),
        "last_entry_iso": next(
            (
                item.get("entry_iso")
                for item in reversed(case_rows)
                if item.get("entry_iso")
            ),
            None,
        ),
    }
    return case_rows, spike_bars, coverage


def analyze_mark_path(
    candles: list[Mapping[str, Any]],
    *,
    ratios: Mapping[str, float],
) -> dict[str, Any]:
    if not candles:
        return {"window_available": False}
    entry_price = finite_float(candles[0].get("open"))
    if entry_price is None or entry_price <= 0:
        entry_price = finite_float(candles[0].get("close"))
    if entry_price is None or entry_price <= 0:
        return {"window_available": False}

    crossings: dict[str, int] = {}
    max_burst = 0.0
    max_retrace = 0.0
    previous_close: float | None = None
    burst_flags = {10: False, 20: False, 30: False, 40: False, 50: False, 100: False}
    max_high = entry_price
    for index, candle in enumerate(candles):
        open_price = finite_float(candle.get("open"))
        high = finite_float(candle.get("high"))
        close = finite_float(candle.get("close"))
        if open_price is None or high is None or close is None or close <= 0:
            continue
        max_high = max(max_high, high)
        base = max(open_price, previous_close or open_price)
        burst_pct = (high / base - 1.0) * 100.0 if base > 0 else 0.0
        retrace_pct = max(0.0, (high / close - 1.0) * 100.0)
        max_burst = max(max_burst, burst_pct)
        max_retrace = max(max_retrace, retrace_pct)
        for threshold in burst_flags:
            burst_flags[threshold] = burst_flags[threshold] or burst_pct >= threshold
        for name in ("warning", "initial_stop", "l2"):
            if name not in crossings and high >= entry_price * float(ratios[name]):
                crossings[name] = index
        previous_close = close

    warning_index = crossings.get("warning")
    stop_index = crossings.get("initial_stop")
    l2_index = crossings.get("l2")
    warning_to_stop = (
        (stop_index - warning_index) * 15
        if warning_index is not None and stop_index is not None
        else None
    )
    warning_to_l2 = (
        (l2_index - warning_index) * 15
        if warning_index is not None and l2_index is not None
        else None
    )
    return {
        "window_available": True,
        "active_15m_candles": len(candles),
        "entry_mark_price": rounded(entry_price, 12),
        "max_adverse_from_entry_pct": rounded((max_high / entry_price - 1.0) * 100.0),
        "max_15m_burst_pct": rounded(max_burst),
        "max_15m_retrace_pct": rounded(max_retrace),
        **{f"burst_ge_{threshold}_pct": value for threshold, value in burst_flags.items()},
        "warning_crossed": warning_index is not None,
        "initial_stop_crossed": stop_index is not None,
        "l2_crossed": l2_index is not None,
        "warning_cross_index": warning_index,
        "initial_stop_cross_index": stop_index,
        "l2_cross_index": l2_index,
        "warning_to_stop_minutes": warning_to_stop,
        "warning_to_l2_minutes": warning_to_l2,
        "warning_stop_same_15m": (
            warning_index is not None and warning_index == stop_index
        ),
        "warning_to_stop_le_15m": (
            warning_to_stop is not None and warning_to_stop <= 15
        ),
        "stop_l2_same_15m": stop_index is not None and stop_index == l2_index,
        "entry_stop_same_15m": stop_index == 0,
    }


def summarize_strategy_tiers(case_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for gated_only in (False, True):
        scope = "current_tier_all_setups" if not gated_only else "current_main_gated"
        scoped = [
            item
            for item in case_rows
            if item.get("window_available")
            and (not gated_only or item.get("gate_passed"))
        ]
        for tier in ("ordinary_lt80", "strong_80_100", "strong_100_250", "super_250_plus"):
            items = [item for item in scoped if item.get("tier_bucket") == tier]
            if not items:
                continue
            rows.append(
                {
                    "scope": scope,
                    "tier_bucket": tier,
                    "cases": len(items),
                    "symbols": len({str(item.get("symbol") or "") for item in items}),
                    "warning_crossed": count_true(items, "warning_crossed"),
                    "initial_stop_crossed": count_true(items, "initial_stop_crossed"),
                    "l2_crossed": count_true(items, "l2_crossed"),
                    "fast_warning_to_stop_le_15m": count_true(
                        items, "warning_to_stop_le_15m"
                    ),
                    "burst_ge_20_pct_cases": count_true(items, "burst_ge_20_pct"),
                    "burst_ge_30_pct_cases": count_true(items, "burst_ge_30_pct"),
                    "burst_ge_50_pct_cases": count_true(items, "burst_ge_50_pct"),
                    "max_15m_burst_pct": rounded(
                        max(
                            (finite_float(item.get("max_15m_burst_pct")) or 0.0)
                            for item in items
                        )
                    ),
                }
            )
    return rows


def build_protection_comparison(case_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gated = [
        item
        for item in case_rows
        if item.get("gate_passed") and item.get("window_available")
    ]
    super_rows = [
        item for item in gated if item.get("tier_bucket") == "super_250_plus"
    ]
    current_fast = count_true(gated, "warning_to_stop_le_15m")
    current_races = count_true(gated, "stop_l2_same_15m")
    warning_cases = count_true(gated, "warning_crossed")
    super_warning = count_true(super_rows, "warning_crossed")
    super_fast = count_true(super_rows, "warning_to_stop_le_15m")
    return [
        {
            "policy": "current_dynamic_margin_preplaced_l2",
            "historical_cases": len(gated),
            "fast_crossing_unresolved_cases": current_fast,
            "stop_l2_same_15m_order_race_cases": current_races,
            "positions_prefunded_50": 0,
            "prefund_without_warning_cases": 0,
            "capital_note": "50 USD guaranteed per position but added only on demand",
            "interpretation": "capital efficient; sub-15m ordering remains unresolved",
        },
        {
            "policy": "gate_l2_until_confirmed_topup_50",
            "historical_cases": len(gated),
            "fast_crossing_unresolved_cases": current_fast,
            "stop_l2_same_15m_order_race_cases": 0,
            "positions_prefunded_50": 0,
            "prefund_without_warning_cases": 0,
            "capital_note": "same reserve; L2 may be missed during a jump",
            "interpretation": "removes add-order race but not initial stop latency",
        },
        {
            "policy": "prefund_50_all_and_gate_l2",
            "historical_cases": len(gated),
            "fast_crossing_unresolved_cases": 0,
            "stop_l2_same_15m_order_race_cases": 0,
            "positions_prefunded_50": len(gated),
            "prefund_without_warning_cases": len(gated) - warning_cases,
            "capital_note": "up to 200 USD tied for four positions immediately",
            "interpretation": "strongest simple protection; least capital efficient",
        },
        {
            "policy": "prefund_50_super250_gate_l2_all",
            "historical_cases": len(gated),
            "fast_crossing_unresolved_cases": max(0, current_fast - super_fast),
            "stop_l2_same_15m_order_race_cases": 0,
            "positions_prefunded_50": len(super_rows),
            "prefund_without_warning_cases": len(super_rows) - super_warning,
            "capital_note": "50 USD immediate only for pump >=250%; other tiers on demand",
            "interpretation": (
                "best observed balance: covers every gated fast case in this sample"
            ),
        },
    ]


def protection_ratios(
    *,
    first_leg_notional_usd: float,
    extra_margin_usd: float,
) -> dict[str, float]:
    initial_liq = (1.0 + 1.0 / LEVERAGE) / (1.0 + MMR)
    after_topup_liq = (
        1.0
        + 1.0 / LEVERAGE
        + extra_margin_usd
        / max(first_leg_notional_usd, 1e-12)
        / (1.0 + TAKER_FEE_RATE)
    ) / (1.0 + MMR)
    return {
        "warning": initial_liq / 1.20,
        "panic": initial_liq / 1.15,
        "initial_stop": initial_liq * (1.0 - STOP_GAP_PCT / 100.0),
        "l2": 1.50,
        "stop_after_topup": after_topup_liq
        * (1.0 - STOP_GAP_PCT / 100.0),
    }


def required_topup_for_l2(*, first_leg_notional_usd: float) -> float:
    target_liq_ratio = 1.50 / (1.0 - STOP_GAP_PCT / 100.0)
    required_ratio = (
        target_liq_ratio * (1.0 + MMR) - 1.0 - 1.0 / LEVERAGE
    )
    return max(
        0.0,
        required_ratio * first_leg_notional_usd * (1.0 + TAKER_FEE_RATE),
    )


def first_leg_notional_usd(
    rule_slug: str,
    *,
    slot_margin_usd: float,
    leverage: float,
) -> float:
    parsed = parse_rule(rule_slug)
    legs = int(parsed["legs"])
    sizing = str(parsed["sizing"])
    weights = (
        [float(index + 1) for index in range(legs)]
        if sizing == "tapered"
        else [1.0] * legs
    )
    return slot_margin_usd * weights[0] / sum(weights) * leverage


def parse_rule(rule_slug: str) -> dict[str, Any]:
    match = re.search(r"step(\d+(?:\.\d+)?)_legs(\d+)_(equal|tapered)", rule_slug)
    if not match:
        raise ValueError(f"unsupported rule slug: {rule_slug}")
    return {
        "step_pct": float(match.group(1)),
        "legs": int(match.group(2)),
        "sizing": match.group(3),
    }


def pump_tier_bucket(pump_pct: float) -> str:
    if pump_pct < 80.0:
        return "ordinary_lt80"
    if pump_pct < 100.0:
        return "strong_80_100"
    if pump_pct < 250.0:
        return "strong_100_250"
    return "super_250_plus"


def render_report(
    *,
    metadata: Mapping[str, Any],
    hourly_summary: list[dict[str, Any]],
    case_rows: list[dict[str, Any]],
    tier_summary: list[dict[str, Any]],
    protection_rows: list[dict[str, Any]],
) -> str:
    coverage = dict(metadata.get("universe_coverage") or {})
    strategy = dict(metadata.get("strategy_coverage") or {})
    gated = [
        item
        for item in case_rows
        if item.get("gate_passed") and item.get("window_available")
    ]
    hourly = {float(item["threshold_pct"]): item for item in hourly_summary}
    gated_tiers = [
        item for item in tier_summary if item.get("scope") == "current_main_gated"
    ]
    lines = [
        "# Pump Spike Risk Research",
        "",
        "## Coverage",
        "",
        f"- Checked Bybit instruments: **{coverage.get('symbols_checked_by_collector', 0)}**.",
        f"- Hourly archives after pump prefilter: **{coverage.get('symbols_with_hourly_archive', 0)}** symbols / "
        f"**{coverage.get('hourly_candles', 0):,}** candles.",
        f"- Hourly range: **{coverage.get('first_candle_iso')}** to **{coverage.get('last_candle_iso')}**.",
        f"- Current-tier reconstructed setups since 2024: **{strategy.get('current_tier_outcomes', 0)}**; "
        f"15m windows: **{strategy.get('cases_with_15m_window', 0)}**.",
        f"- Current main-gated setups with 15m windows: **{strategy.get('gate_passed_cases_with_15m_window', 0)}**.",
        "",
        "## Market-wide hourly surges",
        "",
        "| Threshold | Episodes | Symbols | Wick episodes |",
        "|---:|---:|---:|---:|",
    ]
    for threshold in SPIKE_THRESHOLDS_PCT:
        item = hourly[threshold]
        lines.append(
            f"| {threshold:.0f}% | {item['episodes']} | {item['symbols']} | "
            f"{item['wick_episodes_retrace_10pct']} |"
        )
    lines.extend(
        [
            "",
            "These are market events, not necessarily trades. A surge is an hourly high "
            "above the previous continuous hourly close; adjacent bars within six hours "
            "are one episode.",
            "",
            "## Current main strategy exposure",
            "",
            f"- Cases: **{len(gated)}**.",
            f"- At least one active 15m burst >=20%: **{count_true(gated, 'burst_ge_20_pct')}**.",
            f"- Active 15m burst >=30%: **{count_true(gated, 'burst_ge_30_pct')}**.",
            f"- Active 15m burst >=50%: **{count_true(gated, 'burst_ge_50_pct')}**.",
            f"- Warning-to-initial-stop crossing <=15m: **{count_true(gated, 'warning_to_stop_le_15m')}**.",
            f"- Initial stop and L2 crossed in the same 15m candle: **{count_true(gated, 'stop_l2_same_15m')}**.",
            "",
            "| Tier | Cases | >=20% burst | >=30% burst | Fast warning->stop | Max 15m burst |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for item in gated_tiers:
        lines.append(
            f"| {item['tier_bucket']} | {item['cases']} | "
            f"{item['burst_ge_20_pct_cases']} | {item['burst_ge_30_pct_cases']} | "
            f"{item['fast_warning_to_stop_le_15m']} | {item['max_15m_burst_pct']:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## Protection comparison",
            "",
            "| Policy | Fast unresolved | L2 races | Prefunded cases | Unused prefund |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for item in protection_rows:
        lines.append(
            f"| {item['policy']} | {item['fast_crossing_unresolved_cases']} | "
            f"{item['stop_l2_same_15m_order_race_cases']} | "
            f"{item['positions_prefunded_50']} | "
            f"{item['prefund_without_warning_cases']} |"
        )
    lines.extend(
        [
            "",
            "## Preliminary design read",
            "",
            "The strongest capital-aware candidate is to keep the exchange Mark Price stop, "
            "gate L2 until a confirmed $50 position-margin addition, and pre-fund that $50 "
            "immediately for pump >=250% entries. In this archived sample the only gated "
            "fast warning-to-stop case was in the >=250% tier. This is a research result, "
            "not authorization to change live execution.",
            "",
            "## Limitations",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in metadata.get("limitations") or [])
    return "\n".join(lines) + "\n"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def count_true(rows: Iterable[Mapping[str, Any]], key: str) -> int:
    return sum(1 for item in rows if bool(item.get(key)))


def finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def rounded(value: float | None, digits: int = 8) -> float | None:
    return None if value is None else round(float(value), digits)


def ms_to_iso(value: int | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


__all__ = [
    "analyze_hourly_universe",
    "analyze_mark_path",
    "cluster_spike_bars",
    "first_leg_notional_usd",
    "protection_ratios",
    "required_topup_for_l2",
    "run_pump_spike_risk_research",
]
