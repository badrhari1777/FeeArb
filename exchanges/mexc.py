from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from typing import Iterable, List
from urllib.request import Request, urlopen

import aiohttp
import websockets

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import (
    SymbolMeta,
    get_or_fetch_symbol_meta,
    get_or_fetch_funding_history,
)


logger = logging.getLogger(__name__)
_ENV_BOOTSTRAPPED = False


def _ensure_env_loaded() -> None:
    global _ENV_BOOTSTRAPPED  # pylint: disable=global-statement
    if _ENV_BOOTSTRAPPED:
        return
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        _ENV_BOOTSTRAPPED = True
        return
    try:
        with env_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key.startswith("#"):
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    except OSError:
        logger.debug("Unable to read .env file for MEXC credentials")
    _ENV_BOOTSTRAPPED = True


class MexcAdapter(ExchangeAdapter):
    """REST adapter for the MEXC futures API."""

    name = "mexc"
    ticker_url = "https://contract.mexc.com/api/v1/contract/ticker"
    funding_url_tpl = "https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}"
    ws_url = "wss://contract.mexc.com/ws"
    detail_url = "https://contract.mexc.com/api/v1/contract/detail"
    _login_timeout_seconds = 5.0
    _META_TTL_SECONDS = 86_400  # 24h

    def __init__(self, api_key: str | None = None, api_secret: str | None = None) -> None:
        _ensure_env_loaded()
        self.api_key = api_key or os.getenv("MEXC_API_KEY") or ""
        self.api_secret = api_secret or os.getenv("MEXC_API_SECRET") or ""
        self._last_login_success: bool | None = None
        self._ws_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Origin": "https://contract.mexc.com",
        }

    @property
    def has_api_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @property
    def last_login_success(self) -> bool | None:
        return self._last_login_success

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
        """Legacy REST fallback."""

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
                self._snapshot_from_payload(
                    canonical,
                    exchange_symbol,
                    ticker=item,
                    funding=funding_item or {},
                )
            )

        return snapshots

    async def fetch_market_snapshots_async(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        # Use REST-only path to avoid websocket dependencies/timeouts.
        return await self._fetch_via_rest_async(symbols)

    async def test_private_connection(self) -> bool:
        """Perform a standalone websocket authentication probe."""
        if not self.has_api_credentials:
            raise RuntimeError("MEXC_API_KEY and MEXC_API_SECRET are required for authentication test")
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=1_000_000,
            extra_headers=self._ws_headers,
        ) as ws:
            return await self._authenticate_ws(ws)

    async def _authenticate_ws(self, ws) -> bool:
        """Authenticate against the private websocket if credentials are available."""
        if not self.has_api_credentials:
            self._last_login_success = None
            return True

        req_time = int(time.time() * 1000)
        payload = f"apiKey={self.api_key}&reqTime={req_time}"
        signature = hmac.new(self.api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

        login_message = {
            "method": "login",
            "params": {
                "apiKey": self.api_key,
                "reqTime": req_time,
                "sign": signature,
            },
        }

        await ws.send(json.dumps(login_message))
        try:
            response_text = await asyncio.wait_for(ws.recv(), timeout=self._login_timeout_seconds)
        except asyncio.TimeoutError:
            logger.warning("MEXC websocket login timed out after %.1fs", self._login_timeout_seconds)
            self._last_login_success = False
            return False
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("MEXC websocket login failed: %r", exc)
            self._last_login_success = False
            return False

        try:
            data = json.loads(response_text)
        except json.JSONDecodeError:
            logger.warning("MEXC websocket login returned invalid JSON")
            self._last_login_success = False
            return False

        success = _is_login_success(data)
        if success:
            logger.debug("MEXC websocket login acknowledged")
        else:
            logger.warning("MEXC websocket login rejected: %s", data)
        self._last_login_success = success
        return success

    async def _fetch_via_rest_async(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        mapped = {sym.upper(): self.map_symbol(sym) for sym in symbols}
        targets = {canon: exch for canon, exch in mapped.items() if exch}
        if not targets:
            return []

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as session:
            try:
                async with session.get(self.ticker_url) as resp:
                    resp.raise_for_status()
                    ticker_payload = await resp.json()
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("MEXC REST ticker fetch failed: %r", exc)
                ticker_payload = {"data": []}

            ticker_map = {
                item.get("symbol"): item
                for item in ticker_payload.get("data", [])
                if isinstance(item, dict)
            }
            detail_map = await self._fetch_detail_map(session)

            async def _fetch_funding(symbol: str) -> dict:
                url = self.funding_url_tpl.format(symbol=symbol)
                try:
                    async with session.get(url) as resp:
                        resp.raise_for_status()
                        payload = await resp.json()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.debug("MEXC REST funding fetch failed for %s: %r", symbol, exc)
                    return {}
                data = payload.get("data")
                if isinstance(data, dict):
                    return data
                return {}

            funding_results = await asyncio.gather(
                *(_fetch_funding(symbol) for symbol in targets.values()),
                return_exceptions=True,
            )

        funding_map: dict[str, dict] = {}
        for symbol, result in zip(targets.values(), funding_results):
            if isinstance(result, Exception):
                logger.debug("MEXC funding gather error for %s: %r", symbol, result)
                funding_map[symbol] = {}
            else:
                funding_map[symbol] = result

        snapshots: list[MarketSnapshot] = []
        for canonical, exchange_symbol in targets.items():
            ticker = ticker_map.get(exchange_symbol, {})
            funding = funding_map.get(exchange_symbol, {})
            self._cache_symbol_meta(exchange_symbol, detail_map.get(exchange_symbol))
            snapshots.append(
                self._snapshot_from_payload(
                    canonical,
                    exchange_symbol,
                    ticker=ticker,
                    funding=funding,
                )
            )
        return snapshots

    async def _fetch_detail_map(self, session: aiohttp.ClientSession) -> dict:
        try:
            async with session.get(self.detail_url) as resp:
                resp.raise_for_status()
                payload = await resp.json()
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("MEXC contract detail fetch failed: %r", exc)
            return {}
        data = payload.get("data") or []
        return {item.get("symbol"): item for item in data if isinstance(item, dict)}

    def _cache_symbol_meta(self, exchange_symbol: str, detail: dict | None) -> None:
        def _fetch() -> SymbolMeta | None:
            if not isinstance(detail, dict):
                return None
            return SymbolMeta(
                exchange=self.name,
                symbol=exchange_symbol,
                contract_size=_to_float(detail.get("contractSize")),
                price_step=_to_float(detail.get("priceScale")),
                qty_step=_to_float(detail.get("volScale")),
                min_qty=_to_float(detail.get("minVol")),
                max_qty=_to_float(detail.get("maxVol")),
                min_notional=_to_float(detail.get("minValue")),
                max_leverage=_to_float(detail.get("maxLever")),
                tick_size=_to_float(detail.get("priceScale")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            exchange_symbol,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history with 1h refresh (best-effort)."""
        exch_symbol = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            url = f"https://contract.mexc.com/api/v1/contract/funding_rate/history/" + exch_symbol
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("MEXC funding history fetch failed for %s: %s", exch_symbol, exc)
                return []
            data = payload.get("data") or []
            out: list[dict] = []
            for item in data[:limit]:
                ts = _to_float(item.get("timestamp") or item.get("time"))
                out.append(
                    {
                        "ts_ms": int(ts * 1000) if ts and ts < 10_000_000_000 else int(ts or 0),
                        "rate": _to_float(item.get("fundingRate") or item.get("fundingRate")),
                        "interval_hours": _to_float(item.get("collectCycle")) or 8.0,
                        "mark_price": _to_float(item.get("fairPrice")),
                    }
                )
            return out

        return get_or_fetch_funding_history(
            self.name,
            exch_symbol,
            _fetch,
            max_age_seconds=3600,
            limit=limit,
        )

    def _snapshot_from_payload(
        self,
        canonical: str,
        exchange_symbol: str,
        *,
        ticker: dict,
        funding: dict,
    ) -> MarketSnapshot:
        return MarketSnapshot(
            exchange=self.name,
            symbol=canonical,
            exchange_symbol=exchange_symbol,
            funding_rate=_to_float(funding.get("fundingRate"))
            or _to_float(ticker.get("fundingRate")),
            next_funding_time=_to_datetime(funding.get("nextSettleTime")),
            funding_interval_hours=_to_float(funding.get("collectCycle")),
            mark_price=_to_float(ticker.get("fairPrice")) or _to_float(ticker.get("lastPrice")),
            bid=_to_float(ticker.get("bid1") or ticker.get("bidPrice")),
            ask=_to_float(ticker.get("ask1") or ticker.get("askPrice")),
            bid_size=_to_float(ticker.get("bid1Size") or ticker.get("bid1Qty")),
            ask_size=_to_float(ticker.get("ask1Size") or ticker.get("ask1Qty")),
            raw={"ticker": ticker, "funding": funding},
        )


def _get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:  # nosec - public API call
        return json.loads(resp.read().decode("utf-8"))


def _is_login_success(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("code") == 0:
        return True
    msg = str(data.get("msg") or "").lower()
    if msg in {"success", "suc"}:
        return True
    if data.get("success") is True or data.get("result") is True:
        return True
    channel = data.get("channel") or data.get("method")
    if isinstance(channel, str) and channel.lower() in {"rs.login", "login"}:
        payload = data.get("data")
        if isinstance(payload, dict):
            if payload.get("isSuccess") is True or payload.get("success") is True:
                return True
        if data.get("code") == 0:
            return True
    return False


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
    if seconds <= 0:
        return None
    # API may return milliseconds; normalize to seconds.
    if seconds > 10_000_000_000:  # > ~Sat Nov 20 2286 (ms threshold)
        seconds /= 1000.0
    from datetime import datetime, timezone

    return datetime.fromtimestamp(seconds, tz=timezone.utc)
