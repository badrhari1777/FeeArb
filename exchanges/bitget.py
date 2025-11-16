from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import (
    SymbolMeta,
    get_or_fetch_symbol_meta,
    get_or_fetch_funding_history,
)

logger = logging.getLogger(__name__)


class BitgetAdapter(ExchangeAdapter):
    name = "bitget"
    base_url = "https://api.bitget.com"
    _META_TTL_SECONDS = 86_400  # 24h

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}USDT_UMCBL"
        if symbol.endswith("USD"):
            base = symbol[:-3]
            return f"{base}USD_DMCBL"
        return None

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        snapshots: list[MarketSnapshot] = []

        for canonical in {sym.upper() for sym in symbols}:
            contract = self.map_symbol(canonical)
            if not contract:
                logger.debug("Bitget: unsupported symbol %s", canonical)
                continue

            try:
                ticker_payload = _get_json(
                    f"{self.base_url}/api/mix/v1/market/ticker?" + urlencode({"symbol": contract})
                )
            except HTTPError as exc:
                if exc.code == 400:
                    logger.debug("Bitget: contract %s not available", contract)
                    continue
                raise

            if ticker_payload.get("code") != "00000":
                logger.warning(
                    "Bitget: ticker error for %s: %s", contract, ticker_payload.get("msg")
                )
                continue
            ticker_item = ticker_payload.get("data") or {}

            try:
                funding_payload = _get_json(
                    f"{self.base_url}/api/mix/v1/market/funding-time?"
                    + urlencode({"symbol": contract})
                )
            except HTTPError as exc:
                if exc.code == 400:
                    logger.debug("Bitget: funding data not available for %s", contract)
                    continue
                raise
            funding_item = funding_payload.get("data") or {}

            self._cache_symbol_meta(contract)
            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=contract,
                    funding_rate=_to_float(ticker_item.get("fundingRate")),
                    next_funding_time=_to_datetime(funding_item.get("fundingTime")),
                    mark_price=_to_float(ticker_item.get("indexPrice"))
                    or _to_float(ticker_item.get("last")),
                    bid=_to_float(ticker_item.get("bestBid")),
                    ask=_to_float(ticker_item.get("bestAsk")),
                    raw={"ticker": ticker_item, "funding": funding_item},
                )
            )

        return snapshots

    def _cache_symbol_meta(self, contract: str) -> None:
        def _fetch() -> SymbolMeta | None:
            try:
                payload = _get_json(
                    f"{self.base_url}/api/mix/v1/market/contracts?"
                    + urlencode({"productType": "umcbl"})
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Bitget: market meta fetch failed: %s", exc)
                return None
            items = payload.get("data") or []
            info = next((item for item in items if item.get("symbol") == contract), None)
            if not info:
                return None
            return SymbolMeta(
                exchange=self.name,
                symbol=contract,
                contract_size=_to_float(info.get("size")),
                price_step=_to_float(info.get("priceScale")),
                qty_step=_to_float(info.get("volumePlace")) or _to_float(info.get("sizePlace")),
                min_qty=_to_float(info.get("minTradeNum")),
                max_qty=_to_float(info.get("maxTradeNum")),
                min_notional=None,
                max_leverage=_to_float(info.get("maxLeverage")),
                tick_size=_to_float(info.get("priceScale")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            contract,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history with 1h refresh."""
        contract = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            url = f"{self.base_url}/api/mix/v1/market/history-fundRate?" + urlencode(
                {"symbol": contract, "pageSize": limit}
            )
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Bitget funding history fetch failed for %s: %s", contract, exc)
                return []
            if payload.get("code") != "00000":
                return []
            items = payload.get("data") or []
            out: list[dict] = []
            for item in items:
                ts = _to_float(item.get("timePoint"))
                out.append(
                    {
                        "ts_ms": int(ts) if ts else 0,
                        "rate": _to_float(item.get("fundRate")),
                        "interval_hours": 8.0,
                        "mark_price": None,
                    }
                )
            return out

        return get_or_fetch_funding_history(
            self.name,
            contract,
            _fetch,
            max_age_seconds=3600,
            limit=limit,
        )


def _get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:  # nosec
        import json

        return json.loads(resp.read().decode("utf-8"))


def _to_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_datetime(value: object) -> datetime | None:
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
