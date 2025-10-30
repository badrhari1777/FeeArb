from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class MarketSnapshot:
    """Aggregated market data for a symbol on a specific exchange."""

    exchange: str
    symbol: str  # canonical symbol, e.g. BTCUSDT
    exchange_symbol: str  # exchange ticker, e.g. BTC_USDT
    funding_rate: float | None
    next_funding_time: datetime | None
    mark_price: float | None
    bid: float | None
    ask: float | None
    raw: dict[str, Any]
    bid_size: float | None = None
    ask_size: float | None = None


@dataclass(slots=True)
class ValidationResult:
    """Comparison outcome between two exchanges for a symbol."""

    symbol: str
    long_exchange: str
    short_exchange: str
    long_rate: float
    short_rate: float
    spread: float
    long_mark: float | None
    short_mark: float | None
    long_next_funding: datetime | None
    short_next_funding: datetime | None


@dataclass(slots=True)
class FundingOpportunity:
    """Highest long vs lowest short funding combination across exchanges."""

    symbol: str
    long_exchange: str
    short_exchange: str
    long_rate: float
    short_rate: float
    spread: float
    long_mark: float | None
    short_mark: float | None
    long_next_funding: datetime | None
    short_next_funding: datetime | None
    price_diff: float
    price_diff_pct: float
    effective_spread: float
    participants: int
    long_ask: float | None = None
    long_liquidity: float | None = None
    long_liquidity_usd: float | None = None
    short_bid: float | None = None
    short_liquidity: float | None = None
    short_liquidity_usd: float | None = None

