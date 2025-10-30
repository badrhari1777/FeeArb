from __future__ import annotations

import logging
from typing import Iterable, List
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter


logger = logging.getLogger(__name__)


class MexcAdapter(ExchangeAdapter):
    """Public REST adapter for the MEXC futures API."""

    name = "mexc"
    ticker_url = "https://contract.mexc.com/api/v1/contract/ticker"
    funding_url_tpl = "https://contract.mexc.com/api/v1/contract/fundingRate/{symbol}"

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if not symbol:
            return None
        if symbol.endswith("USDT"):
            base = symbol[:-4]
        elif symbol.endswith("USD"):
            base = symbol[:-3]
        else:
            base = symbol
        if not base:
            return None
        return f"{base}_USDT"

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        mapped = {sym.upper(): self.map_symbol(sym) for sym in symbols}
        targets = {symbol for symbol in mapped.values() if symbol}
        if not targets:
            return []

        try:
            payload = _get_json(self.ticker_url)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("MEXC: HTTP error: %s", exc)
            return []

        items = {item.get("symbol"): item for item in payload.get("data", []) if isinstance(item, dict)}

        snapshots: list[MarketSnapshot] = []
        for canonical, exchange_symbol in mapped.items():
            if not exchange_symbol:
                logger.debug("MEXC: symbol %s is not supported", canonical)
                continue

            item = items.get(exchange_symbol)
            if not item:
                logger.info("MEXC: no data for %s", exchange_symbol)
                continue

            funding_item = _get_funding(exchange_symbol)

            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=exchange_symbol,
                    funding_rate=_to_float(item.get("fundingRate")),
                    next_funding_time=_to_datetime(funding_item.get("nextFundingTime")),
                    mark_price=_to_float(item.get("fairPrice")) or _to_float(item.get("lastPrice")),
                    bid=_to_float(item.get("bid1")),
                    ask=_to_float(item.get("ask1")),
                    bid_size=_to_float(item.get("bid1Size") or item.get("bid1Qty")),
                    ask_size=_to_float(item.get("ask1Size") or item.get("ask1Qty")),
                    raw=item,
                )
            )

        return snapshots


def _get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:  # nosec - public API call
        import json

        return json.loads(resp.read().decode("utf-8"))


def _get_funding(exchange_symbol: str) -> dict:
    try:
        payload = _get_json(MexcAdapter.funding_url_tpl.format(symbol=exchange_symbol))
    except Exception as exc:  # pylint: disable=broad-except
        logger.debug("MEXC: funding API failed for %s: %s", exchange_symbol, exc)
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return {}


def _to_float(value: object) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_datetime(value: object):
    if value in (None, "", "null"):
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    from datetime import datetime, timezone

    if seconds <= 0:
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc)
