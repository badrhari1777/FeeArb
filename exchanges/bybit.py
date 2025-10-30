from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter


logger = logging.getLogger(__name__)


class BybitAdapter(ExchangeAdapter):
    """Public REST adapter for Bybit funding and mark price data."""

    name = "bybit"
    base_url = "https://api.bybit.com"

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if not symbol:
            return None
        if symbol.endswith("USDT"):
            return symbol
        if symbol.endswith("USD"):
            # Enforce USDT contracts going forward.
            base = symbol[:-3]
            return f"{base}USDT"
        return f"{symbol}USDT"

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        snapshots: list[MarketSnapshot] = []
        normalized = {sym.upper(): self.map_symbol(sym) for sym in symbols}

        for canonical, exchange_symbol in normalized.items():
            if not exchange_symbol:
                logger.debug("Bybit: symbol %s is not supported", canonical)
                continue

            category = "linear" if exchange_symbol.endswith("USDT") else "inverse"
            params = urlencode({"category": category, "symbol": exchange_symbol})
            url = f"{self.base_url}/v5/market/tickers?{params}"

            try:
                data = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Bybit: request failed for %s: %s", exchange_symbol, exc)
                continue

            if data.get("retCode") != 0:
                logger.warning(
                    "Bybit: retCode=%s, retMsg=%s for %s",
                    data.get("retCode"),
                    data.get("retMsg"),
                    exchange_symbol,
                )
                continue

            items = data.get("result", {}).get("list") or []
            if not items:
                logger.info("Bybit: empty list for %s", exchange_symbol)
                continue

            item = items[0]
            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=exchange_symbol,
                    funding_rate=_to_float(item.get("fundingRate")),
                    next_funding_time=_to_datetime(item.get("nextFundingTime")),
                    funding_interval_hours=_to_float(item.get("fundingIntervalHour")),
                    mark_price=_to_float(item.get("markPrice")),
                    bid=_to_float(item.get("bid1Price")),
                    ask=_to_float(item.get("ask1Price")),
                    bid_size=_to_float(item.get("bid1Size")),
                    ask_size=_to_float(item.get("ask1Size")),
                    raw=item,
                )
            )

        return snapshots


def _get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:  # nosec - public API call
        import json

        return json.loads(resp.read().decode("utf-8"))


def _to_float(value: object) -> float | None:
    if value in (None, "", "null"):
        return None
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
