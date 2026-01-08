from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .market import DerivedMetrics
from .settings import ExecutionSettings
from .signals import SignalDecision
from .allocator import PreparedAllocation


@dataclass(slots=True)
class FillQuote:
    long_price: float
    short_price: float
    slippage_bps: float
    raw_slippage_bps: float
    orphan_time_ms: int


class FillSimulator:
    def __init__(self, settings: ExecutionSettings) -> None:
        self._settings = settings

    def simulate(
        self,
        decision: SignalDecision,
        allocation: PreparedAllocation,
        metrics_long: Optional[DerivedMetrics],
        metrics_short: Optional[DerivedMetrics],
    ) -> FillQuote:
        cap = self._settings.thresholds.slippage_bps_cap
        liquidity = 0.0
        volatility = 0.0
        long_mid = metrics_long.mid_price if metrics_long else None
        short_mid = metrics_short.mid_price if metrics_short else None
        if metrics_long and metrics_short:
            liquidity = max(1.0, min(metrics_long.min_top_liquidity, metrics_short.min_top_liquidity))
            volatility = max(metrics_long.short_term_volatility, metrics_short.short_term_volatility)

        liquidity_ratio = allocation.notional / liquidity if liquidity else 0.0
        base_slippage = (volatility or 0.0) * 10000.0 * 0.5 + liquidity_ratio * 5.0

        mode_multiplier = {
            "mode_b": 1.5,
            "mode_c": 0.8,
        }.get(decision.mode, 1.0)
        raw_slippage = base_slippage * mode_multiplier
        slippage_bps = min(cap, raw_slippage)

        orphan_time = {
            "mode_b": 150,
            "mode_c": 450,
        }.get(decision.mode, 300)
        if liquidity_ratio > 1.0:
            orphan_time += int(liquidity_ratio * 200)

        long_price = _apply_slippage(long_mid, slippage_bps, direction="buy")
        short_price = _apply_slippage(short_mid, slippage_bps, direction="sell")

        return FillQuote(
            long_price=long_price,
            short_price=short_price,
            slippage_bps=slippage_bps,
            raw_slippage_bps=raw_slippage,
            orphan_time_ms=orphan_time,
        )


def _apply_slippage(price: Optional[float], slippage_bps: float, *, direction: str) -> float:
    base = price or 0.0
    if base <= 0:
        return 0.0
    factor = 1 + (slippage_bps / 10000.0)
    if direction == "sell":
        return base / factor
    return base * factor
