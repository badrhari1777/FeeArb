from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter

logger = logging.getLogger(__name__)


class GateAdapter(ExchangeAdapter):
    name = "gate"
    base_url = "https://api.gateio.ws/api/v4"

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}_USDT"
        if symbol.endswith("USD"):
            base = symbol[:-3]
            return f"{base}_USD"
        return None

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        snapshots: list[MarketSnapshot] = []

        for canonical in {sym.upper() for sym in symbols}:
            contract = self.map_symbol(canonical)
            if not contract:
                logger.debug("Gate: unsupported symbol %s", canonical)
                continue

            ticker_url = (
                f"{self.base_url}/futures/usdt/tickers?" + urlencode({"contract": contract})
            )
            contract_url = f"{self.base_url}/futures/usdt/contracts/{contract}"

            ticker_payload = _get_json(ticker_url)
            if not isinstance(ticker_payload, list) or not ticker_payload:
                logger.info("Gate: empty ticker for %s", contract)
                continue
            ticker_item = ticker_payload[0]

            contract_payload = _get_json(contract_url)

            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=contract,
                    funding_rate=_to_float(
                        ticker_item.get("funding_rate")
                        or ticker_item.get("funding_rate_indicative")
                    ),
                    next_funding_time=_to_datetime(contract_payload.get("funding_next_apply")),
                    mark_price=_to_float(ticker_item.get("mark_price")),
                    bid=_to_float(ticker_item.get("highest_bid")),
                    ask=_to_float(ticker_item.get("lowest_ask")),
                    raw={"ticker": ticker_item, "contract": contract_payload},
                )
            )

        return snapshots


def _get_json(url: str) -> dict | list:
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
    if value in (None, ""):
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc)
