from __future__ import annotations

import logging
from typing import Iterable, List

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter

logger = logging.getLogger(__name__)


class HTXAdapter(ExchangeAdapter):
    """Placeholder adapter for HTX/Huobi.

    HTX requires authenticated access for funding endpoints from our environment,
    so for now we simply log the limitation and return no snapshots.
    """

    name = "htx"

    def map_symbol(self, symbol: str) -> str | None:  # pragma: no cover - trivial
        return symbol.upper().strip()

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        if symbols:
            logger.warning("HTX: public funding API unavailable in this environment")
        return []
