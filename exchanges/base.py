from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from orchestrator.models import MarketSnapshot


class ExchangeAdapter(ABC):
    """Base interface for exchange adapters."""

    name: str

    @abstractmethod
    def map_symbol(self, symbol: str) -> str | None:
        """Return the exchange ticker for the canonical symbol or None if unsupported."""

    @abstractmethod
    def fetch_market_snapshots(self, symbols: Iterable[str]) -> list[MarketSnapshot]:
        """Return funding/mark-price snapshots for the provided symbols."""

