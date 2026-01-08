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
    funding_interval_hours: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "exchange_symbol": self.exchange_symbol,
            "funding_rate": self.funding_rate,
            "next_funding_time": self.next_funding_time.isoformat() if self.next_funding_time else None,
            "mark_price": self.mark_price,
            "bid": self.bid,
            "ask": self.ask,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "funding_interval_hours": self.funding_interval_hours,
        }


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
    long_funding_interval_hours: float | None = None
    short_funding_interval_hours: float | None = None
    long_ask: float | None = None
    long_liquidity: float | None = None
    long_liquidity_usd: float | None = None
    short_bid: float | None = None
    short_liquidity: float | None = None
    short_liquidity_usd: float | None = None

