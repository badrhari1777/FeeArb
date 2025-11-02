from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List, Optional

from orchestrator.models import FundingOpportunity

from .allocator import Allocator
from .market import MarketGateway
from .settings import ExecutionSettings


@dataclass(slots=True)
class SignalDecision:
    symbol: str
    long_exchange: str
    short_exchange: str
    mode: str
    expected_edge: float
    notional: float
    time_to_funding: float
    reasons: List[str] = field(default_factory=list)
    opportunity: FundingOpportunity | None = None


class SignalEngine:
    """Evaluate opportunities and produce executable decisions."""

    def __init__(
        self,
        settings: ExecutionSettings,
        allocator: Allocator,
        market_gateway: MarketGateway,
    ) -> None:
        self._settings = settings
        self._allocator = allocator
        self._market_gateway = market_gateway

    def generate(self, opportunities: Iterable[FundingOpportunity]) -> List[SignalDecision]:
        decisions: List[SignalDecision] = []
        for opportunity in opportunities:
            decision = self._evaluate(opportunity)
            if decision:
                decisions.append(decision)
        return decisions

    def _evaluate(self, opportunity: FundingOpportunity) -> Optional[SignalDecision]:
        now = time.time()
        symbol = opportunity.symbol.upper()
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange
        edge = float(opportunity.effective_spread or 0.0)

        thresholds = self._settings.thresholds
        reasons: List[str] = []

        if edge < thresholds.base_spread_threshold:
            return None

        if self._allocator.is_symbol_locked(symbol):
            return None

        time_to_long = _seconds_until(opportunity.long_next_funding, now)
        time_to_short = _seconds_until(opportunity.short_next_funding, now)
        time_to_funding = _positive_min(time_to_long, time_to_short)
        if time_to_funding is None:
            reasons.append("missing_next_funding")
            time_to_funding = math.inf

        if (
            time_to_funding < thresholds.entry_window_seconds
            and edge < thresholds.high_spread_threshold
        ):
            reasons.append("too_close_to_funding")
            return None

        metrics_long = self._market_gateway.latest_metrics(long_exchange, symbol)
        metrics_short = self._market_gateway.latest_metrics(short_exchange, symbol)
        if not metrics_long or not metrics_short:
            reasons.append("missing_market_metrics")
            return None

        min_liquidity = min(metrics_long.min_top_liquidity, metrics_short.min_top_liquidity)

        estimated_notional = self._allocator.estimate_notional(
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            expected_edge=edge,
        )
        if estimated_notional <= 0:
            return None

        if min_liquidity < estimated_notional:
            reasons.append("insufficient_depth")
            return None

        mode = self._select_mode(edge, time_to_funding, metrics_long, metrics_short)

        return SignalDecision(
            symbol=symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            mode=mode,
            expected_edge=edge,
            notional=estimated_notional,
            time_to_funding=time_to_funding,
            reasons=reasons,
            opportunity=opportunity,
        )

    def _select_mode(
        self,
        edge: float,
        time_to_funding: float,
        metrics_long,
        metrics_short,
    ) -> str:
        thresholds = self._settings.thresholds
        volatility = max(
            metrics_long.short_term_volatility,
            metrics_short.short_term_volatility,
        )

        if (
            time_to_funding <= thresholds.entry_window_seconds
            and edge >= thresholds.high_spread_threshold
        ):
            return "mode_b"
        if (
            edge >= thresholds.extreme_spread_threshold
            and time_to_funding <= thresholds.extended_entry_window_seconds
        ):
            return "mode_b"
        if volatility < 0.02 and time_to_funding > thresholds.entry_window_seconds:
            return "mode_c"
        return "mode_a"


def _seconds_until(dt: Optional[datetime], now_ts: float) -> Optional[float]:
    if dt is None:
        return None
    return (dt.timestamp() - now_ts)


def _positive_min(*values: Optional[float]) -> Optional[float]:
    candidates = [value for value in values if value is not None and value > 0]
    if not candidates:
        return None
    return min(candidates)
