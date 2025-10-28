from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter

logger = logging.getLogger(__name__)


class BitgetAdapter(ExchangeAdapter):
    name = "bitget"
    base_url = "https://api.bitget.com"

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
