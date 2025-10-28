from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter

logger = logging.getLogger(__name__)


class OKXAdapter(ExchangeAdapter):
    name = "okx"
    base_url = "https://www.okx.com"

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if symbol.endswith("USDT") or symbol.endswith("USD"):
            base = symbol[:-4] if symbol.endswith("USDT") else symbol[:-3]
            quote = "USDT" if symbol.endswith("USDT") else "USD"
            return f"{base}-{quote}-SWAP"
        return None

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        snapshots: list[MarketSnapshot] = []
        for canonical in {sym.upper() for sym in symbols}:
            inst_id = self.map_symbol(canonical)
            if not inst_id:
                logger.debug("OKX: unsupported symbol %s", canonical)
                continue

            funding = _get_json(
                f"{self.base_url}/api/v5/public/funding-rate?" + urlencode({"instId": inst_id})
            )
            ticker = _get_json(
                f"{self.base_url}/api/v5/market/ticker?" + urlencode({"instId": inst_id})
            )

            funding_item = (funding.get("data") or [{}])[0]
            ticker_item = (ticker.get("data") or [{}])[0]

            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=inst_id,
                    funding_rate=_to_float(funding_item.get("fundingRate")),
                    next_funding_time=_to_datetime(funding_item.get("nextFundingTime")),
                    mark_price=_to_float(ticker_item.get("markPx"))
                    or _to_float(ticker_item.get("last")),
                    bid=_to_float(ticker_item.get("bidPx")),
                    ask=_to_float(ticker_item.get("askPx")),
                    raw={"funding": funding_item, "ticker": ticker_item},
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
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
