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

    async def fetch_market_snapshots_async(
        self, symbols: Iterable[str]
    ) -> list[MarketSnapshot]:
        """Async wrapper to preserve compatibility with legacy sync adapters."""

        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.fetch_market_snapshots, list(symbols))

    # Protective order hooks (optional; no-op by default)
    def list_protective_orders(self, symbol: str, position_id: str | None = None) -> list[dict]:
        """Return existing protective orders (stop/take) for the symbol/position."""
        return []

    def cancel_position_stops(self, position_id: str | None = None, symbol: str | None = None) -> None:
        """Cancel protective stop/take orders for a position. Override in concrete adapters."""
        return None

    def set_stop_market(
        self,
        position_id: str,
        stop_price: float,
        quantity: float,
        reduce_only: bool = True,
        close_on_trigger: bool = True,
    ) -> None:
        """Place/replace a stop-market protective order. Override in concrete adapters."""
        raise NotImplementedError("set_stop_market not implemented for this adapter")

    def set_take_profit(
        self,
        position_id: str,
        take_price: float,
        quantity: float,
        reduce_only: bool = True,
    ) -> None:
        """Place/replace a take-profit protective order. Override in concrete adapters."""
        raise NotImplementedError("set_take_profit not implemented for this adapter")
