from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import SymbolMeta, get_or_fetch_symbol_meta, get_or_fetch_funding_history

logger = logging.getLogger(__name__)


class GateAdapter(ExchangeAdapter):
    name = "gate"
    base_url = "https://api.gateio.ws/api/v4"
    _META_TTL_SECONDS = 86_400  # 24h

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
            self._cache_symbol_meta(contract, contract_payload)

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

    def _cache_symbol_meta(self, contract: str, payload: dict | None) -> None:
        def _fetch() -> SymbolMeta | None:
            if not isinstance(payload, dict):
                return None
            return SymbolMeta(
                exchange=self.name,
                symbol=contract,
                contract_size=_to_float(payload.get("quanto_multiplier")),
                price_step=_to_float(payload.get("order_price_round")),
                qty_step=_to_float(payload.get("order_size_min")),
                min_qty=_to_float(payload.get("order_size_min")),
                max_qty=_to_float(payload.get("order_size_max")),
                min_notional=_to_float(payload.get("order_price_deviate")),
                max_leverage=_to_float(payload.get("leverage_max")),
                tick_size=_to_float(payload.get("order_price_round")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            contract,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history (expected 8h interval) with 1h refresh."""
        contract = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            url = (
                f"{self.base_url}/futures/usdt/funding_rate?"
                + urlencode({"contract": contract, "limit": limit})
            )
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Gate funding history fetch failed for %s: %s", contract, exc)
                return []
            if not isinstance(payload, list):
                return []
            out: list[dict] = []
            for item in payload:
                ts = _to_float(item.get("t"))
                out.append(
                    {
                        "ts_ms": int(ts * 1000) if ts else 0,
                        "rate": _to_float(item.get("r")),
                        "interval_hours": 8.0,
                        "mark_price": _to_float(item.get("p")),
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
