from __future__ import annotations

import logging
from typing import Iterable, List
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import SymbolMeta, get_or_fetch_funding_history, get_or_fetch_symbol_meta
from utils.funding import (
    enrich_history_intervals,
    is_stale_next_funding_iso,
    normalize_interval_hours,
    parse_timestamp_ms,
)

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

        contract_payload = _get_json(
            f"{self.base_url}/openApi/swap/v2/quote/contracts"
        ).get("data", [])
        contract_map = {
            item.get("symbol"): item for item in contract_payload if isinstance(item, dict)
        }
        ticker_payload = _get_json(
            f"{self.base_url}/openApi/swap/v2/quote/ticker"
        ).get("data", [])
        ticker_map = {item.get("symbol"): item for item in ticker_payload if isinstance(item, dict)}

        for canonical, exch_symbol in targets.items():
            contract_item = contract_map.get(exch_symbol, {})
            ticker_item = ticker_map.get(exch_symbol, {})
            funding_item = _get_json(
                f"{self.base_url}/openApi/swap/v2/quote/fundingRate?"
                + urlencode({"symbol": exch_symbol})
            ).get("data", [{}])[0]
            if not contract_item and not ticker_item:
                logger.debug("BingX: no ticker/contract for %s", exch_symbol)
                continue
            if contract_item:
                self._cache_symbol_meta(exch_symbol, contract_item)
            mark_price = (
                _to_float(ticker_item.get("lastPrice"))
                or _to_float(funding_item.get("markPrice"))
            )
            interval_hours = _funding_interval_hours(funding_item, contract_item)
            next_funding = _resolve_next_funding_time(
                funding_item,
                interval_hours=interval_hours,
            )
            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=exch_symbol,
                    funding_rate=_to_float(funding_item.get("fundingRate")),
                    next_funding_time=next_funding,
                    funding_interval_hours=interval_hours,
                    mark_price=mark_price,
                    bid=_to_float(ticker_item.get("bidPrice") or ticker_item.get("bestBid")),
                    ask=_to_float(ticker_item.get("askPrice") or ticker_item.get("bestAsk")),
                    raw={"ticker": ticker_item, "funding": funding_item},
                )
            )
        return snapshots

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history with ~2m refresh (best-effort)."""
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
                ts_ms = parse_timestamp_ms(
                    item.get("timestamp")
                    or item.get("fundingTime")
                    or item.get("nextFundingTime")
                )
                out.append(
                    {
                        "ts_ms": ts_ms or 0,
                        "rate": _to_float(item.get("fundingRate")),
                        "interval_hours": _funding_interval_hours(item, None),
                        "mark_price": _to_float(item.get("markPrice")),
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
    ts_ms = parse_timestamp_ms(value)
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _funding_interval_hours(funding_item: dict | None, contract_item: dict | None) -> float | None:
    candidates: list[object] = []
    if isinstance(funding_item, dict):
        candidates.extend(
            [
                funding_item.get("fundingIntervalHour"),
                funding_item.get("fundingIntervalHours"),
                funding_item.get("fundingInterval"),
                funding_item.get("fundingRateInterval"),
            ]
        )
    if isinstance(contract_item, dict):
        candidates.extend(
            [
                contract_item.get("fundingIntervalHour"),
                contract_item.get("fundingIntervalHours"),
                contract_item.get("fundingInterval"),
                contract_item.get("fundingRateInterval"),
            ]
        )
    for candidate in candidates:
        parsed = normalize_interval_hours(candidate)
        if parsed is not None:
            return parsed
    return None


def _resolve_next_funding_time(
    funding_item: dict,
    *,
    interval_hours: float | None,
) -> datetime | None:
    next_dt = _to_datetime(funding_item.get("nextFundingTime"))
    if next_dt is not None:
        if not is_stale_next_funding_iso(next_dt.isoformat()):
            return next_dt
        if interval_hours:
            return _roll_forward(next_dt, interval_hours)

    funding_dt = _to_datetime(funding_item.get("fundingTime"))
    if funding_dt is None or not interval_hours:
        return None
    return _roll_forward(funding_dt, interval_hours)


def _roll_forward(start: datetime, interval_hours: float) -> datetime:
    now = datetime.now(timezone.utc)
    step_sec = int(interval_hours * 3600.0)
    if step_sec <= 0:
        return start
    next_ts = start.timestamp()
    now_ts = now.timestamp()
    if next_ts <= now_ts:
        hops = int((now_ts - next_ts) // step_sec) + 1
        next_ts = next_ts + hops * step_sec
    return datetime.fromtimestamp(next_ts, tz=timezone.utc)
