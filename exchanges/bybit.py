from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websockets

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter


logger = logging.getLogger(__name__)


class BybitAdapter(ExchangeAdapter):
    """Public websocket + REST adapter for Bybit funding and mark price data."""

    name = "bybit"
    base_url = "https://api.bybit.com"
    ws_url_linear = "wss://stream.bybit.com/v5/public/linear"

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
            snapshots.append(self._snapshot_from_ticker(canonical, exchange_symbol, item))

        return snapshots

    async def fetch_market_snapshots_async(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        normalized = {sym.upper(): self.map_symbol(sym) for sym in symbols}
        targets = {canon: exch for canon, exch in normalized.items() if exch}
        if not targets:
            return []

        args = [f"tickers.{symbol}" for symbol in targets.values()]
        pending = {symbol: canonical for canonical, symbol in targets.items()}
        snapshots: list[MarketSnapshot] = []
        deadline = asyncio.get_event_loop().time() + 5.0

        try:
            async with websockets.connect(
                self.ws_url_linear,
                ping_interval=20,
                ping_timeout=20,
                max_size=1_000_000,
            ) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))

                while pending and asyncio.get_event_loop().time() < deadline:
                    timeout = max(0.1, deadline - asyncio.get_event_loop().time())
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        break
                    data = json.loads(message)

                    topic = data.get("topic")
                    if not topic or not topic.startswith("tickers."):
                        continue

                    payload = data.get("data")
                    if isinstance(payload, list):
                        payload = payload[0] if payload else None
                    if not isinstance(payload, dict):
                        continue

                    symbol_code = topic.split(".", 1)[1]
                    canonical = pending.pop(symbol_code, None)
                    if not canonical:
                        continue

                    snapshots.append(self._snapshot_from_ticker(canonical, symbol_code, payload))

        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Bybit websocket fetch failed: %r", exc)

        if pending:
            missing_canonicals = [
                canonical
                for canonical, symbol in targets.items()
                if symbol in pending
            ]
            if missing_canonicals:
                logger.debug(
                    "Bybit websocket missing %s symbols; falling back to REST",
                    ", ".join(missing_canonicals),
                )
                rest_snapshots = await super().fetch_market_snapshots_async(missing_canonicals)
                snapshots.extend(rest_snapshots)

        return snapshots

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
