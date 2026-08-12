from __future__ import annotations

import csv
import html
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from analysis_features.bybit_pump_short_outcomes import Series, load_samples, sample_to_series
from analysis_features.pump_live_margin_stress import ShortPosition, short_liquidation_price_usdt
from analysis_features.pump_live_shared_margin_research import (
    DEFAULT_PER_EVENT_DIR,
    DEFAULT_PULLBACK_DIR,
    LEVERAGE,
    MMR,
    TAKER_FEE,
    build_candidates,
    rule_weights,
)
from analysis_features.pump_short_policy_portfolio_research import ms_to_iso
from config import BASE_DIR
from execution.pump_live import (
    MARGIN_MANAGER_V4_ON_DEMAND,
    ladder_prefund_plan,
    required_margin_for_liq_buffer_usd,
)


DEFAULT_INPUT = BASE_DIR / "data" / "research" / "bybit_pump_short_extended" / "symbol_samples.jsonl"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "pump_live_budget_sweep_research"
BUDGET_LEVELS_USD = tuple(float(value) for value in range(600, 1201, 50))


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    own_capital_usd: float = 3_000.0
    max_positions: int = 4
    operating_floor_usd: float = 75.0
    ladder_step_pct: float = 50.0
    ladder_add_window_h: int = 168
    ladder_activation_distance_pct: float = 35.0
    ladder_release_distance_pct: float = 45.0
    fill_reaction_buffer_pct: float = 12.0
    stop_gap_from_liq_pct: float = 2.5
    warning_liq_buffer_pct: float = 20.0
    restore_liq_buffer_pct: float = 25.0
    margin_reduce_trigger_buffer_pct: float = 35.0
    margin_reduce_target_buffer_pct: float = 25.0
    margin_adjust_increment_usd: float = 5.0
    margin_reduce_chunk_usd: float = 75.0
    financing_apr_pct: float = 10.0


def _number(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _round_up(value: float, increment: float = 5.0) -> float:
    return math.ceil(max(0.0, value) / increment - 1e-12) * increment


def _percentile(values: Iterable[float], fraction: float) -> float:
    ordered = sorted(float(item) for item in values)
    if not ordered:
        return 0.0
    index = (len(ordered) - 1) * min(1.0, max(0.0, fraction))
    lo = math.floor(index)
    hi = math.ceil(index)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - index) + ordered[hi] * (index - lo)


def select_unlimited_capital_trades(
    candidates: Iterable[Mapping[str, Any]],
    *,
    max_positions: int = 4,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Apply only current strategy ownership constraints, never a cash cap."""
    active: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    skipped_slots = 0
    skipped_same_symbol = 0
    for source in sorted(
        (dict(row) for row in candidates),
        key=lambda row: (_integer(row.get("entry_ts")), str(row.get("symbol") or "")),
    ):
        entry_ts = _integer(source.get("entry_ts"))
        active = [row for row in active if _integer(row.get("exit_ts")) > entry_ts]
        symbol = str(source.get("symbol") or "").upper()
        if any(str(row.get("symbol") or "").upper() == symbol for row in active):
            skipped_same_symbol += 1
            continue
        if len(active) >= max_positions:
            skipped_slots += 1
            continue
        active.append(source)
        selected.append(source)
    return selected, {
        "skipped_slots": skipped_slots,
        "skipped_same_symbol": skipped_same_symbol,
    }


def load_relevant_series(path: Path, symbols: set[str]) -> dict[str, Series]:
    wanted = {symbol.upper() for symbol in symbols}
    result: dict[str, Series] = {}
    for sample in load_samples(path):
        symbol = str(sample.get("symbol") or "").upper()
        if symbol not in wanted:
            continue
        result[symbol] = sample_to_series(sample)
        if len(result) == len(wanted):
            break
    missing = sorted(wanted - set(result))
    if missing:
        raise ValueError("missing historical series: " + ",".join(missing))
    return result


def _position(
    *,
    prices: list[float],
    margins: list[float],
    filled_count: int,
) -> ShortPosition:
    notionals = [margins[index] * LEVERAGE for index in range(filled_count)]
    quantities = [notionals[index] / prices[index] for index in range(filled_count)]
    qty = sum(quantities)
    average = sum(quantities[index] * prices[index] for index in range(filled_count)) / qty
    return ShortPosition(
        qty=qty,
        avg_entry_price=average,
        leverage=LEVERAGE,
        maintenance_margin_rate=MMR,
        taker_fee_rate=TAKER_FEE,
    )


def _base_required_for_target_buffer(
    position: ShortPosition,
    *,
    mark_price: float,
    target_buffer_pct: float,
    increment: float,
) -> float:
    base_liq = short_liquidation_price_usdt(position, extra_margin_usd=0.0)
    return required_margin_for_liq_buffer_usd(
        qty=position.qty,
        current_liq_price=base_liq,
        mark_price=mark_price,
        target_buffer_pct=target_buffer_pct,
        maintenance_margin_rate=MMR,
        taker_fee_rate=TAKER_FEE,
        round_up_increment_usd=increment,
    )


def _prefund_add(
    position: ShortPosition,
    *,
    current_topup_usd: float,
    target_step: int,
    prices: list[float],
    margins: list[float],
    config: ReplayConfig,
) -> float:
    current_liq = short_liquidation_price_usdt(
        position,
        extra_margin_usd=current_topup_usd,
    )
    legs = [
        {
            "step": index + 1,
            "trigger_price": prices[index],
            "notional_usd": margins[index] * LEVERAGE,
        }
        for index in range(len(prices))
    ]
    plan = ladder_prefund_plan(
        policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
        qty=position.qty,
        current_liq_price=current_liq,
        legs=legs,
        target_leg=legs[target_step - 1],
        leverage=LEVERAGE,
        stop_gap_from_liq_pct=config.stop_gap_from_liq_pct,
        safety_above_next_ladder_pct=config.fill_reaction_buffer_pct,
        final_fill_buffer_pct=20.0,
        maintenance_margin_rate=MMR,
        taker_fee_rate=TAKER_FEE,
        round_up_increment_usd=config.margin_adjust_increment_usd,
        projected_reaction_buffer_pct=config.fill_reaction_buffer_pct,
    )
    return _number(plan.get("required_add_usd"))


def build_trade_actions(
    candidate: Mapping[str, Any],
    series: Series,
    *,
    budget_usd: float,
    config: ReplayConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Reconstruct v4 cash actions from the archived hourly candles.

    High is used for adverse/fill checks and close for cooling/reduction. This
    intentionally assumes that the 15-second live controller reacts inside an
    archived one-hour candle; the report labels that execution limitation.
    """
    entry_ts = _integer(candidate.get("entry_ts"))
    exit_ts = _integer(candidate.get("exit_ts"))
    ts_to_idx = {ts: index for index, ts in enumerate(series.ts)}
    if entry_ts not in ts_to_idx or exit_ts not in ts_to_idx:
        raise ValueError(f"historical window missing for {candidate.get('case_id')}")
    entry_idx = ts_to_idx[entry_ts]
    exit_idx = ts_to_idx[exit_ts]
    entry_price = _number(series.close[entry_idx])
    if entry_price <= 0:
        raise ValueError(f"entry price missing for {candidate.get('case_id')}")

    weights = list(rule_weights(str(candidate.get("rule_slug") or "")))
    weight_sum = sum(weights)
    prices = [entry_price * (1.0 + config.ladder_step_pct / 100.0 * index) for index in range(len(weights))]
    margins = [budget_usd * weight / weight_sum for weight in weights]
    filled_count = 1
    gate_active = False
    topup = 0.0
    actions: list[dict[str, Any]] = []
    peak_topup = 0.0
    peak_single_hour_add = 0.0
    release_total = 0.0
    rescue_add_total = 0.0
    prefund_add_total = 0.0

    def record(ts: int, action: str, delta_topup: float = 0.0) -> None:
        actions.append(
            {
                "ts": ts,
                "action": action,
                "base_margin_usd": round(sum(margins[:filled_count]), 6),
                "topup_usd": round(topup, 6),
                "filled_legs": filled_count,
                "gate_active": gate_active,
                "delta_topup_usd": round(delta_topup, 6),
            }
        )

    record(entry_ts, "entry_l1")
    last_hour_add = 0.0
    last_add_ts: int | None = None
    for idx in range(entry_idx + 1, exit_idx + 1):
        ts = series.ts[idx]
        high = _number(series.high[idx])
        close = _number(series.close[idx])
        if high <= 0 and close <= 0:
            continue
        hour_add = 0.0
        within_add_window = idx - entry_idx <= config.ladder_add_window_h

        # Arm/fill repeatedly so a wide hourly candle can cross multiple rungs.
        while filled_count < len(prices) and within_add_window:
            target_price = prices[filled_count]
            activation_price = target_price / (1.0 + config.ladder_activation_distance_pct / 100.0)
            if not gate_active and high >= activation_price:
                position = _position(prices=prices, margins=margins, filled_count=filled_count)
                required_add = _prefund_add(
                    position,
                    current_topup_usd=topup,
                    target_step=filled_count + 1,
                    prices=prices,
                    margins=margins,
                    config=config,
                )
                if required_add > 0:
                    topup += required_add
                    hour_add += required_add
                    prefund_add_total += required_add
                gate_active = True
                record(ts, f"arm_l{filled_count + 1}", required_add)
            if not gate_active or high + 1e-12 < target_price:
                break
            filled_count += 1
            gate_active = False
            record(ts, f"fill_l{filled_count}")

        position = _position(prices=prices, margins=margins, filled_count=filled_count)
        if high > 0:
            current_liq = short_liquidation_price_usdt(position, extra_margin_usd=topup)
            buffer_pct = (current_liq / high - 1.0) * 100.0
            if buffer_pct <= config.warning_liq_buffer_pct + 1e-9:
                required_add = required_margin_for_liq_buffer_usd(
                    qty=position.qty,
                    current_liq_price=current_liq,
                    mark_price=high,
                    target_buffer_pct=config.restore_liq_buffer_pct,
                    maintenance_margin_rate=MMR,
                    taker_fee_rate=TAKER_FEE,
                    round_up_increment_usd=config.margin_adjust_increment_usd,
                )
                if required_add > 0:
                    topup += required_add
                    hour_add += required_add
                    rescue_add_total += required_add
                    record(ts, "warning_restore", required_add)

        # A cooled close deactivates the nearest order. Margin removal then
        # follows the live $75/30m cadence conservatively as one chunk/hour.
        if gate_active and filled_count < len(prices) and close > 0:
            distance_pct = (prices[filled_count] / close - 1.0) * 100.0
            if distance_pct >= config.ladder_release_distance_pct - 1e-9:
                gate_active = False
                record(ts, f"deactivate_l{filled_count + 1}")
        if topup > 0 and not gate_active and close > 0 and hour_add <= 0:
            current_liq = short_liquidation_price_usdt(position, extra_margin_usd=topup)
            buffer_pct = (current_liq / close - 1.0) * 100.0
            if buffer_pct >= config.margin_reduce_trigger_buffer_pct - 1e-9:
                target_topup = _base_required_for_target_buffer(
                    position,
                    mark_price=close,
                    target_buffer_pct=config.margin_reduce_target_buffer_pct,
                    increment=config.margin_adjust_increment_usd,
                )
                removable = max(0.0, topup - target_topup)
                removed = min(config.margin_reduce_chunk_usd, removable)
                removed = math.floor(removed / config.margin_adjust_increment_usd + 1e-12) * config.margin_adjust_increment_usd
                if removed > 0:
                    topup -= removed
                    release_total += removed
                    record(ts, "margin_release", -removed)

        peak_topup = max(peak_topup, topup)
        peak_single_hour_add = max(peak_single_hour_add, hour_add)
        if hour_add > 0:
            last_hour_add = hour_add
            last_add_ts = ts

    # The exchange releases all isolated margin at the strategy exit.
    actions.append(
        {
            "ts": exit_ts,
            "action": "exit_release",
            "base_margin_usd": 0.0,
            "topup_usd": 0.0,
            "filled_legs": filled_count,
            "gate_active": False,
            "delta_topup_usd": round(-topup, 6),
        }
    )
    expected_legs = max(1, _integer(candidate.get("legs_activated"), 1))
    return actions, {
        "filled_legs_reconstructed": filled_count,
        "filled_legs_source": expected_legs,
        "legs_match": filled_count == expected_legs,
        "peak_topup_usd": round(peak_topup, 6),
        "prefund_add_total_usd": round(prefund_add_total, 6),
        "rescue_add_total_usd": round(rescue_add_total, 6),
        "margin_release_total_usd": round(release_total, 6),
        "peak_single_hour_add_usd": round(peak_single_hour_add, 6),
        "last_margin_add_ts": last_add_ts,
        "last_margin_add_usd": round(last_hour_add, 6),
    }


def _compress_timeline(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not points:
        return []
    result = [points[0]]
    for point in points[1:]:
        previous = result[-1]
        keys = (
            "working_capital_usd",
            "withdrawn_profit_usd",
            "base_margin_usd",
            "topup_usd",
            "borrowed_usd",
            "active_positions",
        )
        if all(abs(_number(point.get(key)) - _number(previous.get(key))) < 1e-9 for key in keys):
            previous["ts_end"] = point["ts"]
            continue
        result.append(point)
    return result


def replay_budget(
    trades: list[Mapping[str, Any]],
    series_by_symbol: Mapping[str, Series],
    *,
    budget_usd: float,
    config: ReplayConfig,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    events: dict[int, list[dict[str, Any]]] = defaultdict(list)
    details: list[dict[str, Any]] = []
    for trade_index, source in enumerate(trades):
        symbol = str(source.get("symbol") or "").upper()
        actions, diagnostics = build_trade_actions(
            source,
            series_by_symbol[symbol],
            budget_usd=budget_usd,
            config=config,
        )
        trade_id = f"{trade_index}:{source.get('case_id')}"
        for action in actions:
            events[_integer(action.get("ts"))].append(
                {"kind": "cash", "trade_id": trade_id, "symbol": symbol, **action}
            )
        pnl = budget_usd * LEVERAGE * _number(source.get("net_pct")) / 100.0
        events[_integer(source.get("exit_ts"))].append(
            {"kind": "pnl", "trade_id": trade_id, "symbol": symbol, "pnl_usd": pnl}
        )
        details.append(
            {
                "budget_usd": budget_usd,
                "symbol": symbol,
                "case_id": source.get("case_id"),
                "entry_iso": source.get("entry_iso"),
                "exit_iso": source.get("exit_iso"),
                "rule_slug": source.get("rule_slug"),
                "split": source.get("split"),
                "exit_reason": source.get("exit_reason"),
                "pump_pct": source.get("pump_pct"),
                "legs_activated": source.get("legs_activated"),
                "net_pct": source.get("net_pct"),
                "pnl_usd": round(pnl, 6),
                **diagnostics,
            }
        )

    states: dict[str, dict[str, Any]] = {}
    working_capital = config.own_capital_usd
    withdrawn_profit = 0.0
    cumulative_strategy_pnl = 0.0
    peak_strategy_wealth = config.own_capital_usd
    max_drawdown = 0.0
    timeline: list[dict[str, Any]] = []
    loan_events: list[dict[str, Any]] = []
    loan_episodes: list[dict[str, Any]] = []
    borrowed_usd_hours = 0.0
    borrowed_hours = 0.0
    previous_ts: int | None = None
    previous_borrow = 0.0
    peak_borrow = 0.0
    peak_committed = 0.0
    peak_base = 0.0
    peak_topup = 0.0
    peak_committed_at_ts: int | None = None
    minimum_own_free = float("inf")
    minimum_own_free_at_ts: int | None = None
    minimum_working_capital = working_capital
    current_episode: dict[str, Any] | None = None
    active_hours = Counter()
    peak_committed_by_concurrency: Counter[int] = Counter()
    peak_borrowed_by_concurrency: Counter[int] = Counter()
    minimum_free_by_concurrency: dict[int, float] = {}

    for ts in sorted(events):
        if previous_ts is not None and ts > previous_ts:
            hours = (ts - previous_ts) / 3_600_000.0
            borrowed_usd_hours += previous_borrow * hours
            if previous_borrow > 0:
                borrowed_hours += hours
            active_hours[len(states)] += hours
        timestamp_events = sorted(
            events[ts],
            key=lambda row: 0 if row["kind"] == "cash" else 1,
        )
        action_labels: list[str] = []
        draw_causes: list[str] = []
        repay_causes: list[str] = []
        # Cash state is applied before PnL at an identical exit timestamp.
        for event in timestamp_events:
            if event["kind"] == "cash":
                trade_id = str(event["trade_id"])
                base = _number(event.get("base_margin_usd"))
                topup = _number(event.get("topup_usd"))
                symbol = str(event.get("symbol") or "")
                action_name = str(event.get("action") or "")
                label = f"{symbol}:{action_name}"
                action_labels.append(label)
                delta_topup = _number(event.get("delta_topup_usd"))
                if action_name == "entry_l1" or action_name.startswith("fill_l") or delta_topup > 0:
                    draw_causes.append(label)
                if action_name == "exit_release" or delta_topup < 0:
                    repay_causes.append(label)
                if base <= 0 and topup <= 0:
                    states.pop(trade_id, None)
                else:
                    states[trade_id] = {
                        "base": base,
                        "topup": topup,
                        "symbol": symbol,
                    }
            else:
                pnl_delta = _number(event.get("pnl_usd"))
                cumulative_strategy_pnl += pnl_delta
                gross_working = working_capital + pnl_delta
                if gross_working > config.own_capital_usd:
                    withdrawn_profit += gross_working - config.own_capital_usd
                    working_capital = config.own_capital_usd
                else:
                    working_capital = gross_working
                minimum_working_capital = min(minimum_working_capital, working_capital)
                action_labels.append(
                    f"{event.get('symbol')}:pnl_{'profit' if pnl_delta >= 0 else 'loss'}"
                )
                if pnl_delta >= 0:
                    repay_causes.append(f"{event.get('symbol')}:pnl_profit")
                else:
                    draw_causes.append(f"{event.get('symbol')}:pnl_loss")
                strategy_wealth = working_capital + withdrawn_profit
                peak_strategy_wealth = max(peak_strategy_wealth, strategy_wealth)
                max_drawdown = max(
                    max_drawdown,
                    peak_strategy_wealth - strategy_wealth,
                )
        base_total = sum(item["base"] for item in states.values())
        topup_total = sum(item["topup"] for item in states.values())
        committed = base_total + topup_total + config.operating_floor_usd
        borrowed = max(0.0, committed - working_capital)
        own_free = max(0.0, working_capital - committed)
        active_symbols = sorted(str(item["symbol"]) for item in states.values())
        loan_delta = borrowed - previous_borrow
        if abs(loan_delta) > 1e-9:
            loan_events.append(
                {
                    "budget_usd": budget_usd,
                    "ts": ts,
                    "iso": ms_to_iso(ts),
                    "event": "borrow" if loan_delta > 0 else "repay",
                    "amount_usd": round(abs(loan_delta), 6),
                    "borrowed_after_usd": round(borrowed, 6),
                    "working_capital_usd": round(working_capital, 6),
                    "base_margin_usd": round(base_total, 6),
                    "topup_usd": round(topup_total, 6),
                    "committed_plus_floor_usd": round(committed, 6),
                    "active_positions": len(states),
                    "active_symbols": "|".join(active_symbols),
                    "causes": "|".join(draw_causes if loan_delta > 0 else repay_causes),
                    "all_actions": "|".join(action_labels),
                }
            )
        if borrowed > 0 and previous_borrow <= 0:
            current_episode = {
                "budget_usd": budget_usd,
                "start_ts": ts,
                "start_iso": ms_to_iso(ts),
                "end_ts": None,
                "end_iso": None,
                "duration_h": None,
                "peak_borrowed_usd": borrowed,
                "peak_ts": ts,
                "peak_iso": ms_to_iso(ts),
                "start_active_positions": len(states),
                "start_active_symbols": "|".join(active_symbols),
                "start_causes": "|".join(draw_causes),
            }
        if current_episode is not None and borrowed > _number(current_episode.get("peak_borrowed_usd")):
            current_episode["peak_borrowed_usd"] = borrowed
            current_episode["peak_ts"] = ts
            current_episode["peak_iso"] = ms_to_iso(ts)
        if borrowed <= 0 and previous_borrow > 0 and current_episode is not None:
            current_episode["end_ts"] = ts
            current_episode["end_iso"] = ms_to_iso(ts)
            current_episode["duration_h"] = round(
                (ts - _integer(current_episode.get("start_ts"))) / 3_600_000.0,
                6,
            )
            current_episode["end_causes"] = "|".join(repay_causes)
            loan_episodes.append(current_episode)
            current_episode = None
        peak_borrow = max(peak_borrow, borrowed)
        if committed > peak_committed:
            peak_committed = committed
            peak_committed_at_ts = ts
        peak_base = max(peak_base, base_total)
        peak_topup = max(peak_topup, topup_total)
        concurrency = len(states)
        peak_committed_by_concurrency[concurrency] = max(
            peak_committed_by_concurrency[concurrency],
            committed,
        )
        peak_borrowed_by_concurrency[concurrency] = max(
            peak_borrowed_by_concurrency[concurrency],
            borrowed,
        )
        minimum_free_by_concurrency[concurrency] = min(
            minimum_free_by_concurrency.get(concurrency, float("inf")),
            own_free,
        )
        if own_free < minimum_own_free:
            minimum_own_free = own_free
            minimum_own_free_at_ts = ts
        timeline.append(
            {
                "budget_usd": budget_usd,
                "ts": ts,
                "iso": ms_to_iso(ts),
                "working_capital_usd": round(working_capital, 6),
                "withdrawn_profit_usd": round(withdrawn_profit, 6),
                "cumulative_strategy_pnl_usd": round(cumulative_strategy_pnl, 6),
                "strategy_wealth_usd": round(working_capital + withdrawn_profit, 6),
                "base_margin_usd": round(base_total, 6),
                "topup_usd": round(topup_total, 6),
                "committed_plus_floor_usd": round(committed, 6),
                "own_free_usd": round(own_free, 6),
                "borrowed_usd": round(borrowed, 6),
                "active_positions": len(states),
            }
        )
        previous_ts = ts
        previous_borrow = borrowed

    if current_episode is not None and previous_ts is not None:
        current_episode["end_ts"] = previous_ts
        current_episode["end_iso"] = ms_to_iso(previous_ts)
        current_episode["duration_h"] = round(
            (previous_ts - _integer(current_episode.get("start_ts"))) / 3_600_000.0,
            6,
        )
        current_episode["end_causes"] = "research_window_end"
        loan_episodes.append(current_episode)
    pnl = cumulative_strategy_pnl
    financing_cost = borrowed_usd_hours * (config.financing_apr_pct / 100.0) / (365.25 * 24.0)
    wins = sum(1 for row in details if _number(row.get("pnl_usd")) > 0)
    losses = len(details) - wins
    train_pnl = sum(_number(row.get("pnl_usd")) for row in details if row.get("split") == "train")
    test_pnl = sum(_number(row.get("pnl_usd")) for row in details if row.get("split") == "test")
    loan_values = [_number(point.get("borrowed_usd")) for point in timeline]
    legs_match = sum(1 for row in details if row.get("legs_match"))
    return {
        "budget_usd": budget_usd,
        "trades": len(details),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(wins / len(details) * 100.0, 6) if details else 0.0,
        "final_working_capital_usd": round(working_capital, 6),
        "minimum_working_capital_usd": round(minimum_working_capital, 6),
        "withdrawn_profit_usd": round(withdrawn_profit, 6),
        "final_strategy_wealth_usd": round(working_capital + withdrawn_profit, 6),
        "pnl_usd": round(pnl, 6),
        "roi_on_initial_3000_pct": round(pnl / config.own_capital_usd * 100.0, 6),
        "max_realized_drawdown_usd": round(max_drawdown, 6),
        "max_realized_drawdown_pct": round(max_drawdown / config.own_capital_usd * 100.0, 6),
        "peak_committed_plus_floor_usd": round(peak_committed, 6),
        "peak_committed_at_iso": ms_to_iso(peak_committed_at_ts or 0),
        "peak_base_margin_usd": round(peak_base, 6),
        "peak_topup_usd": round(peak_topup, 6),
        "peak_borrowed_usd": round(peak_borrow, 6),
        "borrowed_hours": round(borrowed_hours, 6),
        "borrowed_usd_hours": round(borrowed_usd_hours, 6),
        "loan_episode_count": len(loan_episodes),
        "max_loan_episode_h": round(
            max((_number(row.get("duration_h")) for row in loan_episodes), default=0.0),
            6,
        ),
        "mean_loan_when_sampled_usd": round(statistics.mean([v for v in loan_values if v > 0]), 6) if any(v > 0 for v in loan_values) else 0.0,
        "p95_sampled_loan_usd": round(_percentile(loan_values, 0.95), 6),
        "financing_apr_pct": config.financing_apr_pct,
        "financing_cost_at_apr_usd": round(financing_cost, 6),
        "pnl_after_financing_usd": round(pnl - financing_cost, 6),
        "return_on_peak_economic_capital_pct": round(pnl / (config.own_capital_usd + peak_borrow) * 100.0, 6),
        "roi_on_peak_capital_employed_pct": round(
            pnl / max(config.own_capital_usd, peak_committed) * 100.0,
            6,
        ),
        "minimum_own_free_usd": round(0.0 if minimum_own_free == float("inf") else minimum_own_free, 6),
        "minimum_own_free_pct_of_initial": round(
            (0.0 if minimum_own_free == float("inf") else minimum_own_free)
            / config.own_capital_usd
            * 100.0,
            6,
        ),
        "minimum_own_free_at_iso": ms_to_iso(minimum_own_free_at_ts or 0),
        "train_pnl_usd": round(train_pnl, 6),
        "test_pnl_usd": round(test_pnl, 6),
        "test_trades": sum(1 for row in details if row.get("split") == "test"),
        "peak_single_trade_topup_usd": round(max((_number(row.get("peak_topup_usd")) for row in details), default=0.0), 6),
        "peak_single_hour_margin_add_usd": round(max((_number(row.get("peak_single_hour_add_usd")) for row in details), default=0.0), 6),
        "legs_reconstruction_matches": legs_match,
        "legs_reconstruction_total": len(details),
        "max_concurrent_positions": max(active_hours, default=0),
        "hours_by_concurrency": json.dumps({str(key): round(value, 3) for key, value in sorted(active_hours.items())}),
        "peak_committed_at_four_positions_usd": round(peak_committed_by_concurrency[4], 6),
        "peak_borrowed_at_four_positions_usd": round(peak_borrowed_by_concurrency[4], 6),
        "minimum_free_at_four_positions_usd": round(
            minimum_free_by_concurrency.get(4, 0.0),
            6,
        ),
    }, details, _compress_timeline(timeline), loan_events, loan_episodes


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _sparkline(
    points: list[Mapping[str, Any]],
    field: str,
    *,
    color: str,
    label: str,
    prefix: str = "$",
) -> str:
    width, height = 920, 190
    values = [_number(row.get(field)) for row in points]
    if not values:
        return ""
    lo, hi = min(values), max(values)
    if abs(hi - lo) < 1e-9:
        hi = lo + 1.0
    timestamps = [_integer(row.get("ts")) for row in points]
    first_ts = min(timestamps)
    last_ts = max(timestamps)
    span = max(1, last_ts - first_ts)
    coords = []
    for timestamp, value in zip(timestamps, values):
        x = 45 + (timestamp - first_ts) / span * (width - 65)
        y = 15 + (hi - value) / (hi - lo) * (height - 45)
        coords.append(f"{x:.1f},{y:.1f}")
    return f"""
    <div class="chart"><div class="chart-title">{html.escape(label)}: min {prefix}{lo:,.0f}, max {prefix}{max(values):,.0f}</div>
    <svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(label)}">
      <line x1="45" y1="15" x2="45" y2="{height-30}" class="axis"/><line x1="45" y1="{height-30}" x2="{width-20}" y2="{height-30}" class="axis"/>
      <polyline points="{' '.join(coords)}" fill="none" stroke="{color}" stroke-width="3"/>
      <text x="4" y="22">{prefix}{hi:,.0f}</text><text x="4" y="{height-30}">{prefix}{lo:,.0f}</text>
      <text x="45" y="{height-9}">{ms_to_iso(first_ts)[:10]}</text><text x="{width-92}" y="{height-9}">{ms_to_iso(last_ts)[:10]}</text>
    </svg></div>"""


def _render_html(
    summaries: list[Mapping[str, Any]],
    timelines: Mapping[float, list[Mapping[str, Any]]],
    loan_episodes: Mapping[float, list[Mapping[str, Any]]],
    *,
    config: ReplayConfig,
    metadata: Mapping[str, Any],
) -> str:
    best_economic = max(summaries, key=lambda row: _number(row.get("roi_on_peak_capital_employed_pct")))
    low_loan = min(summaries, key=lambda row: (_number(row.get("peak_borrowed_usd")), -_number(row.get("pnl_usd"))))
    rows = "".join(
        "<tr>" + "".join(
            f"<td>{value}</td>" for value in (
                f"${_number(row.get('budget_usd')):,.0f}",
                f"${_number(row.get('pnl_usd')):,.0f}",
                f"{_number(row.get('roi_on_initial_3000_pct')):.1f}%",
                f"{_number(row.get('max_realized_drawdown_pct')):.1f}%",
                f"${_number(row.get('withdrawn_profit_usd')):,.0f}",
                f"${_number(row.get('final_working_capital_usd')):,.0f}",
                f"${_number(row.get('peak_borrowed_usd')):,.0f}",
                f"{_number(row.get('borrowed_hours')):,.0f}",
                f"{_integer(row.get('loan_episode_count'))}",
                f"{_number(row.get('max_loan_episode_h')):,.0f}",
                f"${_number(row.get('financing_cost_at_apr_usd')):,.2f}",
                f"{_number(row.get('roi_on_peak_capital_employed_pct')):.1f}%",
            )
        ) + "</tr>" for row in summaries
    )
    sections = []
    for row in summaries:
        budget = _number(row.get("budget_usd"))
        points = list(timelines[budget])
        episodes = list(loan_episodes.get(budget) or [])
        episode_rows = "".join(
            "<tr>" + "".join(
                f"<td>{html.escape(str(value))}</td>"
                for value in (
                    str(episode.get("start_iso") or "")[:16],
                    str(episode.get("end_iso") or "")[:16],
                    f"{_number(episode.get('duration_h')):,.1f}",
                    f"${_number(episode.get('peak_borrowed_usd')):,.0f}",
                    episode.get("start_active_positions"),
                    episode.get("start_active_symbols"),
                    episode.get("start_causes"),
                )
            ) + "</tr>"
            for episode in episodes
        ) or '<tr><td colspan="7">Заем не потребовался</td></tr>'
        sections.append(f"""
        <section class="level">
          <h2>${budget:,.0f} на одну монету</h2>
          <div class="metrics">
            <b>Итоговый PnL: ${_number(row.get('pnl_usd')):,.0f}</b>
            <span>ROI на собственные $3000: {_number(row.get('roi_on_initial_3000_pct')):.1f}%</span>
            <span>Пиковый заем: ${_number(row.get('peak_borrowed_usd')):,.0f}</span>
            <span>Часов с займом: {_number(row.get('borrowed_hours')):,.0f}</span>
            <span>Эпизодов займа: {_integer(row.get('loan_episode_count'))}</span>
            <span>Прибыль выведена: ${_number(row.get('withdrawn_profit_usd')):,.0f}</span>
            <span>Рабочий капитал в конце: ${_number(row.get('final_working_capital_usd')):,.0f}</span>
            <span>Пик занято при 4 монетах: ${_number(row.get('peak_committed_at_four_positions_usd')):,.0f}</span>
            <span>Заем при 4 монетах: ${_number(row.get('peak_borrowed_at_four_positions_usd')):,.0f}</span>
            <span>Пиковый top-up одной монеты: ${_number(row.get('peak_single_trade_topup_usd')):,.0f}</span>
            <span>Макс. довнесение за час: ${_number(row.get('peak_single_hour_margin_add_usd')):,.0f}</span>
          </div>
          {_sparkline(points, 'working_capital_usd', color='#2dd4bf', label='Рабочий капитал Pump, ограничен $3000')}
          {_sparkline(points, 'withdrawn_profit_usd', color='#a78bfa', label='Накопленная выведенная прибыль')}
          {_sparkline(points, 'borrowed_usd', color='#fb7185', label='Временно занято с main')}
          {_sparkline(points, 'committed_plus_floor_usd', color='#60a5fa', label='Занято в позициях + защитная маржа + $75 floor')}
          {_sparkline(points, 'active_positions', color='#fbbf24', label='Одновременно открытых монет', prefix='')}
          <h3>Когда занимали и возвращали</h3>
          <table><thead><tr><th>Начало</th><th>Возврат</th><th>Часы</th><th>Пик</th><th>Позиций</th><th>Монеты</th><th>Причина старта</th></tr></thead><tbody>{episode_rows}</tbody></table>
        </section>""")
    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8"><title>Pump Live budget sweep</title>
    <style>
      body{{margin:0;background:#07111f;color:#dbeafe;font:15px system-ui,sans-serif}}main{{max-width:1180px;margin:auto;padding:28px}}
      h1,h2{{color:#f8fafc}}.note,.level,table{{background:#0f1d31;border:1px solid #243750;border-radius:14px;padding:18px;margin:18px 0}}
      table{{width:100%;border-collapse:collapse}}th,td{{padding:9px;border-bottom:1px solid #243750;text-align:right}}th:first-child,td:first-child{{text-align:left}}
      .metrics{{display:flex;flex-wrap:wrap;gap:18px;margin-bottom:12px}}.chart{{background:#0a1728;padding:10px;border-radius:10px;margin:10px 0}}
      .chart-title{{color:#bfdbfe;margin:0 0 4px 40px}}svg text{{fill:#94a3b8;font-size:12px}}.axis{{stroke:#334155;stroke-width:1}}
      .warn{{color:#fbbf24}}code{{color:#93c5fd}}
    </style></head><body><main>
    <h1>Pump Live: $600–$1200 на одну монету</h1>
    <div class="note"><b>Что считается.</b> $600/$650/…/$1200 — это полный максимальный бюджет одной позиции, а не размер каждой ступени. Он делится по действующей лестнице 5×равно, 3×(1:2:3) или 2×(1:2). Рабочий Pump-капитал начинается с $3000 и никогда не растет выше $3000: прибыль сверх лимита сразу считается выведенной, убыток уменьшает рабочий капитал, следующая прибыль сначала восстанавливает его до $3000. Максимум 4 монеты, внешний заем не ограничивает действия и учитывается отдельно до полного возврата.</div>
    <div class="note"><b>Покрытие.</b> Исследовательская граница: {metadata['research_start_iso']}; первые/последние сигналы текущей стратегии: {metadata['actual_candidate_min_iso']} — {metadata['actual_candidate_max_iso']}. Кандидатов: {metadata['candidate_count']}, исполнено при лимите 4: {metadata['selected_trade_count']}.</div>
    <div class="note warn"><b>Ограничение.</b> Сигналы, funding/fees и исходы — из существующего часового Pump replay. Маржинальные действия восстановлены по 1h high/close с идеальной реакцией контроллера внутри свечи. Это годится для выбора размера капитала и оценки потребности в займе, но не доказывает переживание внутриминутного гэпа или latency перевода.</div>
    <div class="note"><b>Текущий live-контур для сопоставления:</b> максимум top-up одной позиции $5000, shared rescue facility $2000. В тесте эти лимиты не блокируют действия; превышения показывают, какие уровни нельзя переносить в live без отдельного изменения риск-политики.</div>
    <h2>Сводное сравнение</h2><table><thead><tr><th>Бюджет</th><th>PnL</th><th>ROI/$3000</th><th>DD</th><th>Выведено</th><th>Рабочий капитал</th><th>Пик займа</th><th>Часы займа</th><th>Эпизоды</th><th>Макс. эпизод, ч</th><th>Цена займа {config.financing_apr_pct:.0f}% APR</th><th>ROI/пик занято</th></tr></thead><tbody>{rows}</tbody></table>
    <div class="note">Максимальная отдача на пиковый экономический капитал в этой выборке: <b>${_number(best_economic.get('budget_usd')):,.0f}</b>. Минимальная потребность в займе: <b>${_number(low_loan.get('budget_usd')):,.0f}</b>. Итоговый выбор должен учитывать не только линейно растущий PnL, но и пиковый заем, длительность долга и часовую гранулярность исходных свечей.</div>
    {''.join(sections)}
    </main></body></html>"""


def run_budget_sweep(
    *,
    input_path: Path = DEFAULT_INPUT,
    per_event_dir: Path = DEFAULT_PER_EVENT_DIR,
    pullback_dir: Path = DEFAULT_PULLBACK_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    budgets: Iterable[float] = BUDGET_LEVELS_USD,
    config: ReplayConfig = ReplayConfig(),
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    budget_values = tuple(float(value) for value in budgets)
    candidates, unique_cases, split_ts = build_candidates(
        per_event_dir=per_event_dir,
        pullback_dir=pullback_dir,
    )
    selected, skips = select_unlimited_capital_trades(
        candidates,
        max_positions=config.max_positions,
    )
    series_by_symbol = load_relevant_series(
        input_path,
        {str(row.get("symbol") or "").upper() for row in selected},
    )
    summaries: list[dict[str, Any]] = []
    all_details: list[dict[str, Any]] = []
    all_timelines: list[dict[str, Any]] = []
    all_loan_events: list[dict[str, Any]] = []
    all_loan_episodes: list[dict[str, Any]] = []
    timelines_by_budget: dict[float, list[dict[str, Any]]] = {}
    loan_episodes_by_budget: dict[float, list[dict[str, Any]]] = {}
    for value in budget_values:
        summary, details, timeline, loan_events, loan_episodes = replay_budget(
            selected,
            series_by_symbol,
            budget_usd=value,
            config=config,
        )
        summaries.append(summary)
        if summary["legs_reconstruction_matches"] != summary["legs_reconstruction_total"]:
            raise RuntimeError(
                f"ladder reconstruction mismatch for budget {value:.0f}: "
                f"{summary['legs_reconstruction_matches']}/{summary['legs_reconstruction_total']}"
            )
        all_details.extend(details)
        all_timelines.extend(timeline)
        all_loan_events.extend(loan_events)
        all_loan_episodes.extend(loan_episodes)
        timelines_by_budget[value] = timeline
        loan_episodes_by_budget[value] = loan_episodes

    metadata = {
        "schema": "pump_live_budget_sweep_research_v2_capped_pool",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_start_iso": ms_to_iso(1_704_067_200_000),
        "actual_candidate_min_iso": min((str(row.get("entry_iso")) for row in candidates), default=""),
        "actual_candidate_max_iso": max((str(row.get("entry_iso")) for row in candidates), default=""),
        "unique_cases_after_boundary": unique_cases,
        "candidate_count": len(candidates),
        "selected_trade_count": len(selected),
        "skipped_slots": skips["skipped_slots"],
        "skipped_same_symbol": skips["skipped_same_symbol"],
        "split_ts": split_ts,
        "split_iso": ms_to_iso(split_ts),
        "budgets_usd": list(budget_values),
        "config": asdict(config),
        "limitations": [
            "current-listing survivor bias",
            "hourly reconstructed entries, fills, margin actions, and exits",
            "ideal controller response inside each hourly candle",
            "unlimited and instant main-account liquidity",
            "working Pump capital capped at $3000 and excess profit swept",
            "no transfer latency or intra-hour gap ordering",
        ],
    }
    _write_csv(output_dir / "budget_summary.csv", summaries)
    _write_csv(output_dir / "trade_details.csv", all_details)
    _write_csv(output_dir / "portfolio_timeline.csv", all_timelines)
    _write_csv(output_dir / "loan_events.csv", all_loan_events)
    _write_csv(output_dir / "loan_episodes.csv", all_loan_episodes)
    (output_dir / "report.html").write_text(
        _render_html(
            summaries,
            timelines_by_budget,
            loan_episodes_by_budget,
            config=config,
            metadata=metadata,
        ),
        encoding="utf-8",
    )
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return {**metadata, "summaries": summaries, "output_dir": str(output_dir)}


__all__ = [
    "BUDGET_LEVELS_USD",
    "ReplayConfig",
    "build_trade_actions",
    "load_relevant_series",
    "replay_budget",
    "run_budget_sweep",
    "select_unlimited_capital_trades",
]
