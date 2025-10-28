from __future__ import annotations

import logging
from typing import Iterable, List

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter

logger = logging.getLogger(__name__)


class BingXAdapter(ExchangeAdapter):
    """Placeholder adapter for BingX until public endpoints are integrated."""

    name = "bingx"

    def map_symbol(self, symbol: str) -> str | None:  # pragma: no cover - trivial
        return symbol.upper().strip()

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        if symbols:
            logger.warning("BingX: public funding endpoints require API keys; skipping")
        return []
