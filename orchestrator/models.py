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

