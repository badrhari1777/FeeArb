from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ShortPosition:
    qty: float
    avg_entry_price: float
    leverage: float
    maintenance_margin_rate: float
    taker_fee_rate: float = 0.00055
    maintenance_margin_deduction: float = 0.0


def short_liquidation_price_usdt(
    position: ShortPosition,
    *,
    extra_margin_usd: float = 0.0,
) -> float:
    numerator = (
        position.avg_entry_price * position.qty
        + position.avg_entry_price * position.qty / position.leverage
        + extra_margin_usd / (1.0 + position.taker_fee_rate)
        + position.maintenance_margin_deduction
    )
    denominator = position.qty * (1.0 + position.maintenance_margin_rate)
    return numerator / denominator


def emergency_stop_price(
    liquidation_price: float,
    *,
    gap_from_liquidation_pct: float,
) -> float:
    return liquidation_price * (1.0 - gap_from_liquidation_pct / 100.0)


def required_extra_margin_for_stop(
    position: ShortPosition,
    *,
    target_stop_price: float,
    gap_from_liquidation_pct: float,
) -> float:
    target_liquidation = target_stop_price / (
        1.0 - gap_from_liquidation_pct / 100.0
    )
    base_numerator = (
        position.avg_entry_price * position.qty
        + position.avg_entry_price * position.qty / position.leverage
        + position.maintenance_margin_deduction
    )
    required_numerator = (
        target_liquidation
        * position.qty
        * (1.0 + position.maintenance_margin_rate)
    )
    return max(
        0.0,
        (required_numerator - base_numerator) * (1.0 + position.taker_fee_rate),
    )


def combined_short_position(
    first: ShortPosition,
    *,
    added_qty: float,
    added_price: float,
) -> ShortPosition:
    total_qty = first.qty + added_qty
    average = (
        first.qty * first.avg_entry_price + added_qty * added_price
    ) / total_qty
    return ShortPosition(
        qty=total_qty,
        avg_entry_price=average,
        leverage=first.leverage,
        maintenance_margin_rate=first.maintenance_margin_rate,
        taker_fee_rate=first.taker_fee_rate,
        maintenance_margin_deduction=first.maintenance_margin_deduction,
    )


def round_up_usd(value: float, *, increment_usd: float = 5.0) -> float:
    if increment_usd <= 0:
        raise ValueError("increment_usd must be positive")
    return math.ceil(max(0.0, value) / increment_usd - 1e-12) * increment_usd


def build_bank_prefund_rows(
    *,
    first: ShortPosition,
    combined: ShortPosition,
    second_price: float,
    stop_gap_pct: float,
    base_portfolio_margin_usd: float,
    total_capital_usd: float,
    operating_cash_floor_usd: float,
    max_positions: int,
    topup_levels_usd: tuple[float, ...] = (
        0.0,
        25.0,
        45.0,
        50.0,
        60.0,
        75.0,
        100.0,
    ),
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for extra in topup_levels_usd:
        first_liq = short_liquidation_price_usdt(
            first,
            extra_margin_usd=extra,
        )
        first_stop = emergency_stop_price(
            first_liq,
            gap_from_liquidation_pct=stop_gap_pct,
        )
        combined_liq = short_liquidation_price_usdt(
            combined,
            extra_margin_usd=extra,
        )
        combined_stop = emergency_stop_price(
            combined_liq,
            gap_from_liquidation_pct=stop_gap_pct,
        )
        committed = base_portfolio_margin_usd + max_positions * extra
        free_capital = total_capital_usd - committed
        rows.append(
            {
                "upfront_topup_usd": round(extra, 8),
                "first_leg_liq_price": round(first_liq, 12),
                "first_leg_stop_price": round(first_stop, 12),
                "stop_clearance_above_l2_pct": round(
                    (first_stop / second_price - 1.0) * 100.0,
                    6,
                ),
                "l2_reachable_before_stop": first_stop > second_price,
                "first_leg_stop_loss_usd": round(
                    first.qty * (first_stop - first.avg_entry_price),
                    6,
                ),
                "post_l2_stop_price": round(combined_stop, 12),
                "post_l2_stop_loss_usd": round(
                    combined.qty
                    * (combined_stop - combined.avg_entry_price),
                    6,
                ),
                "post_l2_stop_loss_account_pct": round(
                    combined.qty
                    * (combined_stop - combined.avg_entry_price)
                    / total_capital_usd
                    * 100.0,
                    6,
                ),
                "four_position_committed_usd": round(committed, 6),
                "free_capital_after_four_usd": round(free_capital, 6),
                "free_after_operating_floor_usd": round(
                    free_capital - operating_cash_floor_usd,
                    6,
                ),
                "operating_floor_preserved": (
                    free_capital + 1e-9 >= operating_cash_floor_usd
                ),
            }
        )
    return rows


def build_tier_ladder_protection_rows(
    *,
    slot_margin_usd: float,
    leverage: float,
    maintenance_margin_rate: float,
    taker_fee_rate: float,
    stop_gap_pct: float,
    safety_above_next_ladder_pct: float,
    max_position_topup_usd: float,
    max_total_topup_usd: float,
    guaranteed_position_topup_usd: float,
    max_positions: int,
    base_portfolio_margin_usd: float,
    total_capital_usd: float,
    operating_cash_floor_usd: float,
) -> list[dict[str, Any]]:
    tiers = (
        ("ordinary_lt80", (1.0, 1.0, 1.0, 1.0, 1.0)),
        ("strong_80_100", (1.0, 2.0)),
        ("strong_100_250", (1.0, 2.0, 3.0)),
        ("super_250_plus", (1.0, 2.0)),
    )
    rows: list[dict[str, Any]] = []
    for tier, weights in tiers:
        position: ShortPosition | None = None
        weight_sum = sum(weights)
        for leg_index, weight in enumerate(weights):
            leg_price = 1.0 + 0.5 * leg_index
            leg_margin = slot_margin_usd * weight / weight_sum
            leg_qty = leg_margin * leverage / leg_price
            if position is None:
                position = ShortPosition(
                    qty=leg_qty,
                    avg_entry_price=leg_price,
                    leverage=leverage,
                    maintenance_margin_rate=maintenance_margin_rate,
                    taker_fee_rate=taker_fee_rate,
                )
            else:
                position = combined_short_position(
                    position,
                    added_qty=leg_qty,
                    added_price=leg_price,
                )
            if leg_index + 1 >= len(weights):
                continue
            next_price = 1.0 + 0.5 * (leg_index + 1)
            target_stop = next_price * (
                1.0 + safety_above_next_ladder_pct / 100.0
            )
            required = required_extra_margin_for_stop(
                position,
                target_stop_price=target_stop,
                gap_from_liquidation_pct=stop_gap_pct,
            )
            rounded = round_up_usd(required, increment_usd=5.0)
            other_guarantees = (
                max_positions - 1
            ) * guaranteed_position_topup_usd
            portfolio_topup = rounded + other_guarantees
            free_capital = (
                total_capital_usd
                - base_portfolio_margin_usd
                - portfolio_topup
            )
            protected_stop = emergency_stop_price(
                short_liquidation_price_usdt(
                    position,
                    extra_margin_usd=rounded,
                ),
                gap_from_liquidation_pct=stop_gap_pct,
            )
            rows.append(
                {
                    "tier": tier,
                    "filled_through_leg": leg_index + 1,
                    "next_leg": leg_index + 2,
                    "next_ladder_pct_from_first": round(
                        (next_price - 1.0) * 100.0,
                        6,
                    ),
                    "target_stop_safety_above_next_pct": (
                        safety_above_next_ladder_pct
                    ),
                    "required_total_topup_usd": round(required, 8),
                    "rounded_total_topup_usd": round(rounded, 8),
                    "protected_stop_pct_from_first": round(
                        (protected_stop - 1.0) * 100.0,
                        6,
                    ),
                    "within_position_topup_cap": (
                        rounded <= max_position_topup_usd + 1e-9
                    ),
                    "portfolio_topup_with_three_other_guarantees_usd": round(
                        portfolio_topup,
                        6,
                    ),
                    "within_portfolio_topup_cap": (
                        portfolio_topup <= max_total_topup_usd + 1e-9
                    ),
                    "free_capital_after_four_slot_capacity_usd": round(
                        free_capital,
                        6,
                    ),
                    "free_after_operating_floor_usd": round(
                        free_capital - operating_cash_floor_usd,
                        6,
                    ),
                }
            )
    return rows


def build_portfolio_policy_rows(
    *,
    leverage: float,
    maintenance_margin_rate: float,
    taker_fee_rate: float,
    stop_gap_pct: float,
    total_capital_usd: float,
    max_positions: int,
    operating_cash_floor_usd: float,
) -> list[dict[str, Any]]:
    policies = (
        (
            "current_on_demand_175_plus_0",
            175.0,
            0.0,
            "maximum free cash; L2 is unreachable unless the monitor tops up first",
        ),
        (
            "same_size_prefund_45",
            175.0,
            45.0,
            "minimum rounded amount above L2, but only about 1% BANK clearance",
        ),
        (
            "same_size_prefund_50",
            175.0,
            50.0,
            "simple current-size protection with about 2.5% target clearance",
        ),
        (
            "same_size_prefund_60",
            175.0,
            60.0,
            "more gap tolerance, but little shared reserve remains at four positions",
        ),
        (
            "same_size_prefund_75",
            175.0,
            75.0,
            "uses the entire account at four positions and violates the cash floor",
        ),
        (
            "rebudget_150_trade_plus_50_protection",
            150.0,
            50.0,
            "14.3% less trade exposure than current and twice the free cash of flat 50",
        ),
        (
            "rebudget_125_trade_plus_50_protection",
            125.0,
            50.0,
            "28.6% less trade exposure; keeps the original 300 USDT reserve free",
        ),
    )
    rows: list[dict[str, Any]] = []
    for policy, trade_margin, prefund, note in policies:
        first_notional = trade_margin
        second_notional = trade_margin * 2.0
        first = ShortPosition(
            qty=first_notional,
            avg_entry_price=1.0,
            leverage=leverage,
            maintenance_margin_rate=maintenance_margin_rate,
            taker_fee_rate=taker_fee_rate,
        )
        combined = combined_short_position(
            first,
            added_qty=second_notional / 1.5,
            added_price=1.5,
        )
        first_stop = emergency_stop_price(
            short_liquidation_price_usdt(
                first,
                extra_margin_usd=prefund,
            ),
            gap_from_liquidation_pct=stop_gap_pct,
        )
        combined_stop = emergency_stop_price(
            short_liquidation_price_usdt(
                combined,
                extra_margin_usd=prefund,
            ),
            gap_from_liquidation_pct=stop_gap_pct,
        )
        committed = max_positions * (trade_margin + prefund)
        free_capital = total_capital_usd - committed
        rows.append(
            {
                "policy": policy,
                "trade_margin_per_position_usd": trade_margin,
                "upfront_protection_per_position_usd": prefund,
                "trade_notional_per_position_usd": trade_margin * leverage,
                "committed_per_position_usd": trade_margin + prefund,
                "committed_at_four_positions_usd": committed,
                "free_capital_at_four_positions_usd": free_capital,
                "free_after_operating_floor_usd": (
                    free_capital - operating_cash_floor_usd
                ),
                "operating_floor_preserved": (
                    free_capital + 1e-9 >= operating_cash_floor_usd
                ),
                "l2_reachable_before_stop": first_stop > 1.5,
                "stop_clearance_above_l2_pct": round(
                    (first_stop / 1.5 - 1.0) * 100.0,
                    6,
                ),
                "post_l2_stop_loss_usd": round(
                    combined.qty
                    * (combined_stop - combined.avg_entry_price),
                    6,
                ),
                "post_l2_stop_loss_account_pct": round(
                    combined.qty
                    * (combined_stop - combined.avg_entry_price)
                    / total_capital_usd
                    * 100.0,
                    6,
                ),
                "note": note,
            }
        )
    return rows


def simulate_bank_rise(
    *,
    duration_sec: int,
    start_price: float = 0.1673,
    end_price: float = 0.36,
    monitor_interval_sec: int = 15,
    topup_cooldown_sec: int = 300,
    warning_buffer_pct: float = 20.0,
    panic_buffer_pct: float = 15.0,
    emergency_buffer_pct: float = 10.0,
    topup_chunk_usd: float = 25.0,
    max_position_topup_usd: float = 175.0,
    guaranteed_position_topup_usd: float = 50.0,
    stop_gap_pct: float = 2.5,
) -> dict[str, Any]:
    position = ShortPosition(
        qty=1010.0,
        avg_entry_price=0.17180881,
        leverage=3.0,
        maintenance_margin_rate=0.025,
    )
    second_qty = 1350.0
    second_price = 0.25766
    extra_margin = 0.0
    last_topup_sec = -topup_cooldown_sec
    ladder_filled = False
    closed = False
    close_reason: str | None = None
    close_price: float | None = None
    events: list[dict[str, Any]] = []
    previous_price = start_price

    def protection() -> tuple[float, float]:
        liquidation = short_liquidation_price_usdt(
            position,
            extra_margin_usd=extra_margin,
        )
        stop = emergency_stop_price(
            liquidation,
            gap_from_liquidation_pct=stop_gap_pct,
        )
        return liquidation, stop

    for second in range(monitor_interval_sec, duration_sec + 1, monitor_interval_sec):
        price = start_price + (end_price - start_price) * second / duration_sec
        liquidation, stop = protection()
        crossings: list[tuple[float, str]] = []
        if previous_price < stop <= price:
            crossings.append((stop, "exchange_stop"))
        if not ladder_filled and previous_price < second_price <= price:
            crossings.append((second_price, "second_ladder"))
        crossings.sort()
        for crossing_price, event in crossings:
            if event == "exchange_stop":
                closed = True
                close_reason = "exchange_stop"
                close_price = crossing_price
                events.append(
                    {
                        "second": second,
                        "event": event,
                        "price": crossing_price,
                        "extra_margin_usd": extra_margin,
                    }
                )
                break
            position = combined_short_position(
                position,
                added_qty=second_qty,
                added_price=second_price,
            )
            ladder_filled = True
            liquidation, stop = protection()
            events.append(
                {
                    "second": second,
                    "event": event,
                    "price": crossing_price,
                    "extra_margin_usd": extra_margin,
                    "new_qty": position.qty,
                    "new_avg_entry": position.avg_entry_price,
                    "new_stop_price": stop,
                }
            )
            if crossing_price < stop <= price:
                closed = True
                close_reason = "exchange_stop_after_ladder"
                close_price = stop
                events.append(
                    {
                        "second": second,
                        "event": "exchange_stop_after_ladder",
                        "price": stop,
                        "extra_margin_usd": extra_margin,
                    }
                )
                break
        if closed:
            break

        liquidation, stop = protection()
        buffer_pct = (liquidation / price - 1.0) * 100.0
        if buffer_pct <= warning_buffer_pct:
            in_cooldown = second - last_topup_sec < topup_cooldown_sec
            if not (in_cooldown and buffer_pct > emergency_buffer_pct):
                desired = (
                    topup_chunk_usd * 2.0
                    if buffer_pct <= panic_buffer_pct
                    else topup_chunk_usd
                )
                position_cap = (
                    max_position_topup_usd
                    if buffer_pct <= panic_buffer_pct
                    else min(
                        max_position_topup_usd,
                        guaranteed_position_topup_usd,
                    )
                )
                allowed = min(
                    desired,
                    max(0.0, position_cap - extra_margin),
                )
                if allowed >= 1.0:
                    extra_margin += allowed
                    last_topup_sec = second
                    liquidation, stop = protection()
                    events.append(
                        {
                            "second": second,
                            "event": "margin_added",
                            "price": price,
                            "amount_usd": allowed,
                            "extra_margin_usd": extra_margin,
                            "buffer_before_pct": buffer_pct,
                            "liq_price": liquidation,
                            "stop_price": stop,
                        }
                    )
                elif buffer_pct <= emergency_buffer_pct:
                    closed = True
                    close_reason = "bot_emergency_close"
                    close_price = price
                    events.append(
                        {
                            "second": second,
                            "event": close_reason,
                            "price": price,
                            "extra_margin_usd": extra_margin,
                        }
                    )
                    break
        previous_price = price

    final_liq, final_stop = protection()
    return {
        "duration_sec": duration_sec,
        "start_price": start_price,
        "end_price": end_price,
        "closed": closed,
        "close_reason": close_reason,
        "close_price": close_price,
        "ladder_filled": ladder_filled,
        "extra_margin_usd": extra_margin,
        "final_qty": position.qty,
        "final_avg_entry": position.avg_entry_price,
        "final_liq_price": final_liq,
        "final_stop_price": final_stop,
        "events": events,
    }


def build_bank_margin_stress(
    *,
    first_qty: float = 1010.0,
    first_entry_price: float = 0.17180881,
    second_qty: float = 1350.0,
    second_price: float = 0.25766,
    leverage: float = 3.0,
    maintenance_margin_rate: float = 0.025,
    taker_fee_rate: float = 0.00055,
    stop_gap_pct: float = 2.5,
    topup_chunk_usd: float = 25.0,
    max_position_topup_usd: float = 175.0,
    base_portfolio_margin_usd: float = 700.0,
    max_positions: int = 4,
    max_total_topup_usd: float = 275.0,
    operating_cash_floor_usd: float = 25.0,
    total_capital_usd: float = 1000.0,
) -> dict[str, Any]:
    first = ShortPosition(
        qty=first_qty,
        avg_entry_price=first_entry_price,
        leverage=leverage,
        maintenance_margin_rate=maintenance_margin_rate,
        taker_fee_rate=taker_fee_rate,
    )
    combined = combined_short_position(
        first,
        added_qty=second_qty,
        added_price=second_price,
    )
    required_for_ladder = required_extra_margin_for_stop(
        first,
        target_stop_price=second_price,
        gap_from_liquidation_pct=stop_gap_pct,
    )
    chunks_for_ladder = int(
        -(-required_for_ladder // topup_chunk_usd)
    )
    rounded_for_ladder = chunks_for_ladder * topup_chunk_usd
    margin_rows: list[dict[str, Any]] = []
    extra = 0.0
    while extra <= max_position_topup_usd + 1e-9:
        first_liq = short_liquidation_price_usdt(
            first,
            extra_margin_usd=extra,
        )
        combined_liq = short_liquidation_price_usdt(
            combined,
            extra_margin_usd=extra,
        )
        margin_rows.append(
            {
                "extra_margin_usd": round(extra, 8),
                "first_leg_liq_price": round(first_liq, 12),
                "first_leg_stop_price": round(
                    emergency_stop_price(
                        first_liq,
                        gap_from_liquidation_pct=stop_gap_pct,
                    ),
                    12,
                ),
                "second_ladder_reachable_before_stop": emergency_stop_price(
                    first_liq,
                    gap_from_liquidation_pct=stop_gap_pct,
                )
                > second_price,
                "post_fill_qty": combined.qty,
                "post_fill_avg_entry": round(combined.avg_entry_price, 12),
                "post_fill_liq_price": round(combined_liq, 12),
                "post_fill_stop_price": round(
                    emergency_stop_price(
                        combined_liq,
                        gap_from_liquidation_pct=stop_gap_pct,
                    ),
                    12,
                ),
            }
        )
        extra += topup_chunk_usd

    guaranteed_per_position = rounded_for_ladder
    guaranteed_total = guaranteed_per_position * max_positions
    shared_emergency = max(
        0.0,
        max_total_topup_usd - guaranteed_total,
    )
    portfolio_rows = [
        {
            "scenario": "base_four_slots",
            "required_usd": base_portfolio_margin_usd,
            "remaining_usd": total_capital_usd - base_portfolio_margin_usd,
            "fits_1000": base_portfolio_margin_usd <= total_capital_usd,
        },
        {
            "scenario": "four_reach_second_ladder",
            "required_usd": base_portfolio_margin_usd + guaranteed_total,
            "remaining_usd": (
                total_capital_usd
                - base_portfolio_margin_usd
                - guaranteed_total
            ),
            "fits_1000": (
                base_portfolio_margin_usd + guaranteed_total
                <= total_capital_usd
            ),
        },
        {
            "scenario": "configured_total_topup_cap",
            "required_usd": base_portfolio_margin_usd + max_total_topup_usd,
            "remaining_usd": (
                total_capital_usd
                - base_portfolio_margin_usd
                - max_total_topup_usd
            ),
            "fits_1000": (
                base_portfolio_margin_usd + max_total_topup_usd
                <= total_capital_usd
            ),
        },
        {
            "scenario": "four_use_full_per_position_cap",
            "required_usd": (
                base_portfolio_margin_usd
                + max_position_topup_usd * max_positions
            ),
            "remaining_usd": (
                total_capital_usd
                - base_portfolio_margin_usd
                - max_position_topup_usd * max_positions
            ),
            "fits_1000": (
                base_portfolio_margin_usd
                + max_position_topup_usd * max_positions
                <= total_capital_usd
            ),
        },
    ]
    rise_scenarios = [
        {
            "scenario": name,
            **simulate_bank_rise(duration_sec=duration),
        }
        for name, duration in (
            ("slow_rise_6h", 6 * 3600),
            ("fast_rise_60s", 60),
            ("spike_30s", 30),
        )
    ]
    bank_prefund_rows = build_bank_prefund_rows(
        first=first,
        combined=combined,
        second_price=second_price,
        stop_gap_pct=stop_gap_pct,
        base_portfolio_margin_usd=base_portfolio_margin_usd,
        total_capital_usd=total_capital_usd,
        operating_cash_floor_usd=operating_cash_floor_usd,
        max_positions=max_positions,
    )
    tier_ladder_protection_rows = build_tier_ladder_protection_rows(
        slot_margin_usd=base_portfolio_margin_usd / max_positions,
        leverage=leverage,
        maintenance_margin_rate=maintenance_margin_rate,
        taker_fee_rate=taker_fee_rate,
        stop_gap_pct=stop_gap_pct,
        safety_above_next_ladder_pct=2.5,
        max_position_topup_usd=max_position_topup_usd,
        max_total_topup_usd=max_total_topup_usd,
        guaranteed_position_topup_usd=guaranteed_per_position,
        max_positions=max_positions,
        base_portfolio_margin_usd=base_portfolio_margin_usd,
        total_capital_usd=total_capital_usd,
        operating_cash_floor_usd=operating_cash_floor_usd,
    )
    portfolio_policy_rows = build_portfolio_policy_rows(
        leverage=leverage,
        maintenance_margin_rate=maintenance_margin_rate,
        taker_fee_rate=taker_fee_rate,
        stop_gap_pct=stop_gap_pct,
        total_capital_usd=total_capital_usd,
        max_positions=max_positions,
        operating_cash_floor_usd=operating_cash_floor_usd,
    )
    return {
        "schema": "pump_live_margin_stress_v2",
        "inputs": {
            "first_position": asdict(first),
            "second_qty": second_qty,
            "second_price": second_price,
            "combined_position": asdict(combined),
            "stop_gap_pct": stop_gap_pct,
            "topup_chunk_usd": topup_chunk_usd,
            "max_position_topup_usd": max_position_topup_usd,
            "base_portfolio_margin_usd": base_portfolio_margin_usd,
            "max_positions": max_positions,
            "max_total_topup_usd": max_total_topup_usd,
            "operating_cash_floor_usd": operating_cash_floor_usd,
            "total_capital_usd": total_capital_usd,
        },
        "summary": {
            "observed_first_liq_price": round(
                short_liquidation_price_usdt(first),
                12,
            ),
            "required_extra_for_ladder_usd": round(required_for_ladder, 8),
            "rounded_topup_for_ladder_usd": rounded_for_ladder,
            "guaranteed_topup_per_position_usd": guaranteed_per_position,
            "guaranteed_topup_total_usd": guaranteed_total,
            "shared_emergency_pool_usd": shared_emergency,
            "operating_cash_floor_usd": operating_cash_floor_usd,
            "reserve_budget_check_usd": (
                guaranteed_total
                + shared_emergency
                + operating_cash_floor_usd
            ),
            "full_rescue_shortfall_usd": max(
                0.0,
                (
                    base_portfolio_margin_usd
                    + max_position_topup_usd * max_positions
                    - total_capital_usd
                ),
            ),
        },
        "margin_rows": margin_rows,
        "bank_prefund_rows": bank_prefund_rows,
        "tier_ladder_protection_rows": tier_ladder_protection_rows,
        "portfolio_policy_rows": portfolio_policy_rows,
        "portfolio_rows": portfolio_rows,
        "rise_scenarios": rise_scenarios,
    }


def write_bank_margin_stress(
    output_dir: Path,
    **kwargs: Any,
) -> dict[str, Any]:
    report = build_bank_margin_stress(**kwargs)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_csv(output_dir / "bank_margin_levels.csv", report["margin_rows"])
    _write_csv(
        output_dir / "bank_prefund_comparison.csv",
        report["bank_prefund_rows"],
    )
    _write_csv(
        output_dir / "tier_ladder_protection.csv",
        report["tier_ladder_protection_rows"],
    )
    _write_csv(
        output_dir / "portfolio_policy_comparison.csv",
        report["portfolio_policy_rows"],
    )
    _write_csv(output_dir / "portfolio_capacity.csv", report["portfolio_rows"])
    _write_csv(
        output_dir / "rise_scenarios.csv",
        [
            {
                key: (
                    json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if key == "events"
                    else value
                )
                for key, value in row.items()
            }
            for row in report["rise_scenarios"]
        ],
    )
    return report


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


__all__ = [
    "ShortPosition",
    "build_bank_prefund_rows",
    "build_bank_margin_stress",
    "build_portfolio_policy_rows",
    "build_tier_ladder_protection_rows",
    "combined_short_position",
    "emergency_stop_price",
    "required_extra_margin_for_stop",
    "round_up_usd",
    "simulate_bank_rise",
    "short_liquidation_price_usdt",
    "write_bank_margin_stress",
]
