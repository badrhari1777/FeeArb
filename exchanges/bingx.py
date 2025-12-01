from __future__ import annotations

import logging
from typing import Iterable, List
from datetime import datetime, timezone
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
        if not symbol:
            return None
        if "-" in symbol:
            return symbol
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT"
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
        ticker_map = {item.get("symbol"): item for item in ticker_payload if isinstance(item, dict)}

        for canonical, exch_symbol in targets.items():
            ticker_item = ticker_map.get(exch_symbol, {})
            funding_item = _get_json(
                f"{self.base_url}/openApi/swap/v2/quote/fundingRate?"
                + urlencode({"symbol": exch_symbol})
            ).get("data", [{}])[0]
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
                    next_funding_time=_to_datetime(funding_item.get("nextFundingTime")),
                    mark_price=_to_float(ticker_item.get("lastPrice")),
                    bid=_to_float(ticker_item.get("bestBid")),
                    ask=_to_float(ticker_item.get("bestAsk")),
                    raw={"ticker": ticker_item, "funding": funding_item},
                )
            )
        return snapshots

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history with 1h refresh (best-effort)."""
        exch_symbol = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            url = (
                f"{self.base_url}/openApi/swap/v2/quote/fundingRate?"
                + urlencode({"symbol": exch_symbol})
            )
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("BingX funding history fetch failed for %s: %s", exch_symbol, exc)
                return []
            data = payload.get("data") or []
            out: list[dict] = []
            for item in data[:limit]:
                ts_ms = _to_float(item.get("timestamp") or item.get("nextFundingTime"))
                out.append(
                    {
                        "ts_ms": int(ts_ms) if ts_ms else 0,
                        "rate": _to_float(item.get("fundingRate")),
                        "interval_hours": 8.0,
                        "mark_price": _to_float(item.get("markPrice")),
                    }
                )
            return out

        from utils.cache_db import get_or_fetch_funding_history

        return get_or_fetch_funding_history(
            self.name,
            exch_symbol,
            _fetch,
            max_age_seconds=3600,
            limit=limit,
        )

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


def _to_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts > 10_000_000:  # treat as ms
        ts = ts / 1000.0
    return datetime.fromtimestamp(ts, tz=timezone.utc)
