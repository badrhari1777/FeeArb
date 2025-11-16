from __future__ import annotations

import logging
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import SymbolMeta, get_or_fetch_symbol_meta

logger = logging.getLogger(__name__)


class BingXAdapter(ExchangeAdapter):
    """REST adapter for BingX perpetuals (public endpoints)."""

    name = "bingx"
    base_url = "https://open-api.bingx.com"
    _META_TTL_SECONDS = 86_400  # 24h

    def map_symbol(self, symbol: str) -> str | None:  # pragma: no cover - trivial
        symbol = symbol.upper().strip()
        if symbol.endswith("USDT"):
            return symbol
        return None

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        snapshots: list[MarketSnapshot] = []
        targets = {sym.upper(): self.map_symbol(sym) for sym in symbols}
        targets = {canon: exch for canon, exch in targets.items() if exch}
        if not targets:
            return []

        ticker_payload = _get_json(
            f"{self.base_url}/openApi/swap/v2/quote/contracts"
        ).get("data", [])
        funding_payload = _get_json(
            f"{self.base_url}/openApi/swap/v2/quote/fundingRate"
        ).get("data", [])
        funding_map = {item.get("symbol"): item for item in funding_payload if isinstance(item, dict)}
        ticker_map = {item.get("symbol"): item for item in ticker_payload if isinstance(item, dict)}

        for canonical, exch_symbol in targets.items():
            ticker_item = ticker_map.get(exch_symbol, {})
            funding_item = funding_map.get(exch_symbol, {})
            if not ticker_item:
                logger.debug("BingX: no ticker for %s", exch_symbol)
                continue
            self._cache_symbol_meta(exch_symbol, ticker_item)
            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=exch_symbol,
                    funding_rate=_to_float(funding_item.get("fundingRate")),
                    next_funding_time=_to_float(funding_item.get("nextFundingTime")),
                    mark_price=_to_float(ticker_item.get("lastPrice")),
                    bid=_to_float(ticker_item.get("bestBid")),
                    ask=_to_float(ticker_item.get("bestAsk")),
                    raw={"ticker": ticker_item, "funding": funding_item},
                )
            )
        return snapshots

    def _cache_symbol_meta(self, exch_symbol: str, ticker: dict | None) -> None:
        def _fetch() -> SymbolMeta | None:
            if not isinstance(ticker, dict):
                return None
            return SymbolMeta(
                exchange=self.name,
                symbol=exch_symbol,
                contract_size=_to_float(ticker.get("contractSize")),
                price_step=_to_float(ticker.get("tickSize")),
                qty_step=_to_float(ticker.get("stepSize")),
                min_qty=_to_float(ticker.get("minQty")),
                max_qty=_to_float(ticker.get("maxQty")),
                min_notional=None,
                max_leverage=_to_float(ticker.get("maxLeverage")),
                tick_size=_to_float(ticker.get("tickSize")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            exch_symbol,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )


def _get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:  # nosec
        import json

        return json.loads(resp.read().decode("utf-8"))


def _to_float(value: object):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
