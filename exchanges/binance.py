from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import SymbolMeta, get_or_fetch_funding_history, get_or_fetch_symbol_meta
from utils.funding import enrich_history_intervals, parse_timestamp_ms

logger = logging.getLogger(__name__)


class BinanceAdapter(ExchangeAdapter):
    """REST adapter for Binance USDT-M perpetuals (public endpoints)."""

    name = "binance"
    base_url = "https://fapi.binance.com"
    _META_TTL_SECONDS = 86_400  # 24h

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if ":" in symbol:
            symbol = symbol.split(":", 1)[0]
        symbol = symbol.replace("/", "").replace("-", "").replace("_", "")
        if not symbol:
            return None
        # Guard against double-suffixed symbols (e.g., ZKUSDTUSDT).
        while symbol.endswith("USDTUSDT"):
            symbol = symbol[:-4]
        if symbol.endswith("USDUSDT"):
            symbol = symbol[:-4]
        if symbol.endswith("USDT"):
            return symbol
        if symbol.endswith("USD"):
            return f"{symbol[:-3]}USDT"
        return f"{symbol}USDT"

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        snapshots: list[MarketSnapshot] = []
        targets = {sym.upper(): self.map_symbol(sym) for sym in symbols}
        targets = {canon: exch for canon, exch in targets.items() if exch}
        if not targets:
            return []

        for canonical, exch_symbol in targets.items():
            premium = _get_json(
                f"{self.base_url}/fapi/v1/premiumIndex?" + urlencode({"symbol": exch_symbol})
            )
            if premium.get("code") not in (None, 0):
                logger.debug("Binance premiumIndex error for %s: %s", exch_symbol, premium)
                continue
            book = _get_json(
                f"{self.base_url}/fapi/v1/ticker/bookTicker?" + urlencode({"symbol": exch_symbol})
            )
            if book.get("code") not in (None, 0):
                logger.debug("Binance bookTicker error for %s: %s", exch_symbol, book)
                book = {}

            self._cache_symbol_meta(exch_symbol)

            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=exch_symbol,
                    funding_rate=_to_float(premium.get("lastFundingRate")),
                    next_funding_time=_to_datetime(premium.get("nextFundingTime")),
                    mark_price=_to_float(premium.get("markPrice")),
                    bid=_to_float(book.get("bidPrice")),
                    ask=_to_float(book.get("askPrice")),
                    bid_size=_to_float(book.get("bidQty")),
                    ask_size=_to_float(book.get("askQty")),
                    raw={"premiumIndex": premium, "bookTicker": book},
                )
            )

        return snapshots

    def _cache_symbol_meta(self, exch_symbol: str) -> None:
        def _fetch() -> SymbolMeta | None:
            url = f"{self.base_url}/fapi/v1/exchangeInfo?" + urlencode({"symbol": exch_symbol})
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Binance exchangeInfo fetch failed for %s: %s", exch_symbol, exc)
                return None
            if payload.get("code") not in (None, 0):
                return None
            items = payload.get("symbols") or []
            info = items[0] if items else None
            if not info:
                return None
            filters = {item.get("filterType"): item for item in info.get("filters") or []}
            price_filter = filters.get("PRICE_FILTER") or {}
            lot_filter = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE") or {}
            notional_filter = filters.get("MIN_NOTIONAL") or {}
            return SymbolMeta(
                exchange=self.name,
                symbol=exch_symbol,
                contract_size=_to_float(info.get("contractSize")) or 1.0,
                price_step=_to_float(price_filter.get("tickSize")),
                qty_step=_to_float(lot_filter.get("stepSize")),
                min_qty=_to_float(lot_filter.get("minQty")),
                max_qty=_to_float(lot_filter.get("maxQty")),
                min_notional=_to_float(notional_filter.get("notional") or notional_filter.get("minNotional")),
                max_leverage=_to_float(info.get("maxLeverage")),
                tick_size=_to_float(price_filter.get("tickSize")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            exch_symbol,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history with ~2m refresh."""
        exch_symbol = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            url = f"{self.base_url}/fapi/v1/fundingRate?" + urlencode(
                {"symbol": exch_symbol, "limit": limit}
            )
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Binance funding history fetch failed for %s: %s", exch_symbol, exc)
                return []
            if isinstance(payload, dict) and payload.get("code") not in (None, 0):
                return []
            items = payload if isinstance(payload, list) else []
            out: list[dict] = []
            for item in items:
                ts_ms = parse_timestamp_ms(item.get("fundingTime"))
                out.append(
                    {
                        "ts_ms": ts_ms or 0,
                        "rate": _to_float(item.get("fundingRate")),
                        "interval_hours": None,
                        "mark_price": None,
                    }
                )
            return enrich_history_intervals(out)

        return get_or_fetch_funding_history(
            self.name,
            exch_symbol,
            _fetch,
            max_age_seconds=120,
            limit=limit,
        )


def _get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    try:
        with urlopen(req, timeout=15) as resp:  # nosec
            import json

            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        try:
            import json

            payload = exc.read()
            if payload:
                return json.loads(payload.decode("utf-8"))
        except Exception:  # pylint: disable=broad-except
            pass
        return {"code": exc.code, "msg": str(exc)}
    except URLError as exc:
        return {"code": "url_error", "msg": str(exc)}


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
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
