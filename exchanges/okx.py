from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import (
    SymbolMeta,
    get_or_fetch_symbol_meta,
    get_or_fetch_funding_history,
)
from utils.cache_db import SymbolMeta, get_or_fetch_symbol_meta

logger = logging.getLogger(__name__)


class OKXAdapter(ExchangeAdapter):
    name = "okx"
    base_url = "https://www.okx.com"
    _META_TTL_SECONDS = 86_400  # 24h

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
            self._cache_symbol_meta(inst_id)

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

    def _cache_symbol_meta(self, inst_id: str) -> None:
        def _fetch() -> SymbolMeta | None:
            url = f"{self.base_url}/api/v5/public/instruments?" + urlencode({"instType": "SWAP", "instId": inst_id})
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("OKX: instruments fetch failed for %s: %s", inst_id, exc)
                return None
            items = payload.get("data") or []
            info = items[0] if items else None
            if not info:
                return None
            return SymbolMeta(
                exchange=self.name,
                symbol=inst_id,
                contract_size=_to_float(info.get("ctVal")),
                price_step=_to_float(info.get("tickSz")),
                qty_step=_to_float(info.get("lotSz")),
                min_qty=_to_float(info.get("minSz")),
                max_qty=_to_float(info.get("maxLmtSz")),
                min_notional=_to_float(info.get("minSz")),
                max_leverage=_to_float(info.get("lever")),
                tick_size=_to_float(info.get("tickSz")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            inst_id,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history with 1h refresh."""
        inst_id = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            url = f"{self.base_url}/api/v5/public/funding-rate-history?" + urlencode(
                {"instId": inst_id, "limit": limit}
            )
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("OKX: funding history fetch failed for %s: %s", inst_id, exc)
                return []
            if payload.get("code") not in (None, "0", "00000", "0000"):
                return []
            items = payload.get("data") or []
            out: list[dict] = []
            for item in items:
                ts = _to_float(item.get("fundingTime"))
                out.append(
                    {
                        "ts_ms": int(ts) if ts else 0,
                        "rate": _to_float(item.get("fundingRate")),
                        "interval_hours": 8.0,
                        "mark_price": _to_float(item.get("realizedRate")),
                    }
                )
            return out

        return get_or_fetch_funding_history(
            self.name,
            inst_id,
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
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
