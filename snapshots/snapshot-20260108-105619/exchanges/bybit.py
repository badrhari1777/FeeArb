from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.missing_symbols import is_recently_missing, record_missing
from utils.cache_db import SymbolMeta, get_or_fetch_symbol_meta, get_or_fetch_funding_history
from utils.cache_db import SymbolMeta, get_or_fetch_symbol_meta


logger = logging.getLogger(__name__)


class BybitAdapter(ExchangeAdapter):
    """REST adapter for Bybit funding and mark price data."""

    name = "bybit"
    base_url = "https://api.bybit.com"
    ws_url_linear = "wss://stream.bybit.com/v5/public/linear"
    _META_TTL_SECONDS = 86_400  # 24h

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
        """Legacy REST fallback (kept for compatibility/tests)."""

        snapshots: list[MarketSnapshot] = []
        normalized = {sym.upper(): self.map_symbol(sym) for sym in symbols}

        for canonical, exchange_symbol in normalized.items():
            if not exchange_symbol:
                logger.debug("Bybit: symbol %s is not supported", canonical)
                continue
            if is_recently_missing(self.name, exchange_symbol):
                logger.debug("Bybit: skipping %s (recently missing)", exchange_symbol)
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
                msg = (data.get("retMsg") or "").lower()
                if data.get("retCode") == 10001 or "symbol invalid" in msg:
                    record_missing(self.name, exchange_symbol)
                continue

            items = data.get("result", {}).get("list") or []
            if not items:
                logger.info("Bybit: empty list for %s", exchange_symbol)
                record_missing(self.name, exchange_symbol)
                continue

            item = items[0]
            self._cache_symbol_meta(exchange_symbol, category)
            snapshots.append(self._snapshot_from_ticker(canonical, exchange_symbol, item))

        return snapshots

    async def fetch_market_snapshots_async(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        # Use REST-only path to avoid websocket dependencies/timeouts.
        return await super().fetch_market_snapshots_async(symbols)

    @staticmethod
    def _snapshot_from_ticker(
        canonical: str,
        exchange_symbol: str,
        item: dict,
    ) -> MarketSnapshot:
        return MarketSnapshot(
            exchange="bybit",
            symbol=canonical,
            exchange_symbol=exchange_symbol,
            funding_rate=_to_float(item.get("fundingRate")),
            next_funding_time=_to_datetime(item.get("nextFundingTime")),
            funding_interval_hours=_to_float(item.get("fundingIntervalHour"))
            or _derive_interval_hours(item),
            mark_price=_to_float(item.get("markPrice")),
            bid=_to_float(item.get("bid1Price")),
            ask=_to_float(item.get("ask1Price")),
            bid_size=_to_float(item.get("bid1Size")),
            ask_size=_to_float(item.get("ask1Size")),
            raw=item,
        )

    def _cache_symbol_meta(self, exchange_symbol: str, category: str) -> None:
        def _fetch() -> SymbolMeta | None:
            params = urlencode({"category": category, "symbol": exchange_symbol})
            url = f"{self.base_url}/v5/market/instruments-info?{params}"
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Bybit meta fetch failed for %s: %s", exchange_symbol, exc)
                return None
            if payload.get("retCode") != 0:
                return None
            items = payload.get("result", {}).get("list") or []
            if not items:
                return None
            info = items[0]
            price_filter = info.get("priceFilter") or {}
            lot_filter = info.get("lotSizeFilter") or {}
            leverage_filter = info.get("leverageFilter") or {}
            return SymbolMeta(
                exchange=self.name,
                symbol=exchange_symbol,
                contract_size=_to_float(info.get("contractSize")),
                price_step=_to_float(price_filter.get("tickSize")),
                qty_step=_to_float(lot_filter.get("qtyStep")),
                min_qty=_to_float(lot_filter.get("minOrderQty")),
                max_qty=_to_float(lot_filter.get("maxOrderQty")),
                min_notional=_to_float(lot_filter.get("minOrderValue")),
                max_leverage=_to_float(leverage_filter.get("maxLeverage")),
                tick_size=_to_float(price_filter.get("tickSize")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            exchange_symbol,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history (8h interval) with ~2m refresh TTL."""
        exchange_symbol = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            params = urlencode(
                {
                    "category": "linear",
                    "symbol": exchange_symbol,
                    "limit": limit,
                }
            )
            url = f"{self.base_url}/v5/market/funding/history?{params}"
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Bybit funding history fetch failed for %s: %s", exchange_symbol, exc)
                return []
            if payload.get("retCode") != 0:
                return []
            items = payload.get("result", {}).get("list") or []
            out: list[dict] = []
            for item in items:
                ts = _to_float(item.get("fundingRateTimestamp"))
                out.append(
                    {
                        "ts_ms": int(ts) if ts is not None else 0,
                        "rate": _to_float(item.get("fundingRate")),
                        "interval_hours": 8.0,
                        "mark_price": _to_float(item.get("markPrice")),
                    }
                )
            return out

        return get_or_fetch_funding_history(
            self.name,
            exchange_symbol,
            _fetch,
            max_age_seconds=120,
            limit=limit,
        )


def _derive_interval_hours(item: dict) -> float | None:
    """Best-effort derive funding interval from websocket payload timestamps."""
    now = _to_float(item.get("ts"))
    next_funding = _to_float(item.get("nextFundingTime"))
    if now is None or next_funding is None:
        return None
    try:
        delta = (next_funding - now) / 1000.0 / 3600.0
    except ZeroDivisionError:
        return None
    if delta <= 0:
        return None
    return delta


def _get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:  # nosec - public API call
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
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
