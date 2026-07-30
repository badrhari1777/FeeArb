from __future__ import annotations

import csv
import json
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
    return {
        "schema": "pump_live_margin_stress_v1",
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
    "build_bank_margin_stress",
    "combined_short_position",
    "emergency_stop_price",
    "required_extra_margin_for_stop",
    "simulate_bank_rise",
    "short_liquidation_price_usdt",
    "write_bank_margin_stress",
]
