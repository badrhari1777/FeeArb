from __future__ import annotations

import asyncio
import json
import logging
import time
import zlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp
import websockets

from execution.accounts import _safe_float
from exchanges import normalize_exchange_name
from .manual_symbols import (
    _normalize_binance_symbol,
    _normalize_bingx_symbol,
    _normalize_bitget_symbol,
    _normalize_bybit_symbol,
    _normalize_gate_symbol,
    _normalize_kucoin_symbol,
    _normalize_mexc_symbol,
    _normalize_okx_symbol,
)

logger = logging.getLogger(__name__)

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BINANCE_WS_URL = "wss://fstream.binance.com/ws"
BINGX_WS_URL = "wss://open-api-swap.bingx.com/swap-market"
MEXC_WS_URL = "wss://contract.mexc.com/edge"
BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
GATE_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
KUCOIN_REST_URL = "https://api-futures.kucoin.com/api/v1/bullet-public"


@dataclass(slots=True)
class OrderBookLite:
    bids: List[List[float]]
    asks: List[List[float]]

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None


@dataclass(slots=True)
class OrderBookState:
    bids: Dict[float, float]
    asks: Dict[float, float]

    @classmethod
    def empty(cls) -> "OrderBookState":
        return cls(bids={}, asks={})

    def apply_snapshot(self, bids: List[List[float]], asks: List[List[float]]) -> None:
        self.bids = {price: qty for price, qty in bids if qty > 0}
        self.asks = {price: qty for price, qty in asks if qty > 0}

    def apply_update(self, bids: List[List[float]], asks: List[List[float]]) -> None:
        self._apply_side(self.bids, bids)
        self._apply_side(self.asks, asks)

    def _apply_side(self, side: Dict[float, float], levels: List[List[float]]) -> None:
        for price, qty in levels:
            if qty <= 0:
                side.pop(price, None)
            else:
                side[price] = qty

    def to_book(self, depth: int) -> OrderBookLite:
        bid_prices = sorted(self.bids.keys(), reverse=True)[:depth]
        ask_prices = sorted(self.asks.keys())[:depth]
        bids = [[price, self.bids[price]] for price in bid_prices]
        asks = [[price, self.asks[price]] for price in ask_prices]
        return OrderBookLite(bids=bids, asks=asks)


@dataclass(slots=True)
class BookRecord:
    book: OrderBookLite
    updated_at: float


def _decode_message(message: Any) -> str | None:
    if isinstance(message, bytes):
        try:
            return zlib.decompress(message, 16 + zlib.MAX_WBITS).decode("utf-8")
        except Exception:
            try:
                return message.decode("utf-8")
            except Exception:
                return None
    if isinstance(message, str):
        return message
    return None


def _parse_levels(raw: Any, *, side: str, allow_zero: bool = False) -> List[List[float]]:
    levels: List[List[float]] = []
    if not isinstance(raw, list):
        return levels
    for item in raw:
        price = 0.0
        qty = 0.0
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            price = _safe_float(item[0]) or 0.0
            qty = _safe_float(item[1]) or 0.0
        elif isinstance(item, dict):
            price = _safe_float(item.get("p") or item.get("price")) or 0.0
            qty = _safe_float(item.get("s") or item.get("size")) or 0.0
        else:
            continue
        if price <= 0 or qty < 0:
            continue
        if not allow_zero and qty <= 0:
            continue
        levels.append([price, qty])
    if side == "bid":
        levels.sort(key=lambda row: row[0], reverse=True)
    elif side == "ask":
        levels.sort(key=lambda row: row[0])
    return levels


def _trim_book(book: OrderBookLite, depth: int) -> OrderBookLite:
    if depth <= 0:
        return book
    if len(book.bids) <= depth and len(book.asks) <= depth:
        return book
    return OrderBookLite(bids=book.bids[:depth], asks=book.asks[:depth])


class MarketDataBus:
    def __init__(self, *, max_age_sec: float = 2.0, default_depth: int = 20) -> None:
        self._max_age_sec = max(0.0, float(max_age_sec))
        self._default_depth = max(1, int(default_depth))
        self._lock = asyncio.Lock()
        self._books: dict[tuple[str, str], BookRecord] = {}
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._stops: dict[tuple[str, str], asyncio.Event] = {}

    async def shutdown(self) -> None:
        async with self._lock:
            tasks = list(self._tasks.values())
            stops = list(self._stops.values())
            self._tasks.clear()
            self._stops.clear()
        for stop in stops:
            stop.set()
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        async with self._lock:
            self._books.clear()

    async def get_orderbook(
        self,
        exchange: str,
        symbol: str,
        *,
        depth: Optional[int] = None,
        max_age_sec: Optional[float] = None,
    ) -> dict[str, Any] | None:
        key = await self._ensure_subscription(exchange, symbol)
        if not key:
            return None
        async with self._lock:
            record = self._books.get(key)
        if not record:
            return None
        age = time.time() - record.updated_at
        effective_max_age = self._max_age_sec if max_age_sec is None else max(0.0, float(max_age_sec))
        if effective_max_age and age > effective_max_age:
            return None
        target_depth = max(1, int(depth or self._default_depth))
        book = _trim_book(record.book, target_depth)
        return {"bids": book.bids, "asks": book.asks, "timestamp": record.updated_at}

    async def _ensure_subscription(self, exchange: str, symbol: str) -> tuple[str, str] | None:
        norm_exchange = normalize_exchange_name(exchange).lower()
        norm_symbol = str(symbol or "").upper()
        if not norm_exchange or not norm_symbol:
            return None
        key = (norm_exchange, norm_symbol)
        async with self._lock:
            if key in self._tasks:
                return key
            stop = asyncio.Event()
            task = asyncio.create_task(self._run_exchange_loop(norm_exchange, norm_symbol, stop))
            self._tasks[key] = task
            self._stops[key] = stop
        return key

    async def _update_book(self, exchange: str, symbol: str, book: OrderBookLite) -> None:
        key = (exchange, symbol)
        record = BookRecord(book=book, updated_at=time.time())
        async with self._lock:
            self._books[key] = record

    async def _run_exchange_loop(self, exchange: str, symbol: str, stop: asyncio.Event) -> None:
        handlers = {
            "bybit": self._bybit_loop,
            "binance": self._binance_loop,
            "bingx": self._bingx_loop,
            "mexc": self._mexc_loop,
            "bitget": self._bitget_loop,
            "okx": self._okx_loop,
            "gate": self._gate_loop,
            "kucoin": self._kucoin_loop,
        }
        handler = handlers.get(exchange)
        if not handler:
            return
        await handler(symbol, exchange, stop)

    async def _bybit_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_bybit_symbol(symbol)
        state = OrderBookState.empty()
        while not stop.is_set():
            try:
                async with websockets.connect(
                    BYBIT_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {"op": "subscribe", "args": [f"orderbook.50.{stream_symbol}"]}
                    await ws.send(json.dumps(sub))
                    while not stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        payload = json.loads(text)
                        if payload.get("success") is False:
                            logger.debug("Bybit subscribe error: %s", payload)
                        if payload.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        data = payload.get("data")
                        if not data:
                            continue
                        if isinstance(data, list):
                            data = data[0] if data else None
                        if not isinstance(data, dict):
                            continue
                        is_snapshot = payload.get("type") == "snapshot"
                        bids = _parse_levels(
                            data.get("b") or data.get("bids") or [],
                            side="bid",
                            allow_zero=not is_snapshot,
                        )
                        asks = _parse_levels(
                            data.get("a") or data.get("asks") or [],
                            side="ask",
                            allow_zero=not is_snapshot,
                        )
                        if not bids and not asks:
                            continue
                        if is_snapshot or not state.bids or not state.asks:
                            state.apply_snapshot(bids, asks)
                        else:
                            state.apply_update(bids, asks)
                        await self._update_book(exchange, symbol, state.to_book(50))
            except Exception as exc:
                logger.debug("Bybit WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _binance_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_binance_symbol(symbol).lower()
        if not stream_symbol:
            return
        ws_url = f"{BINANCE_WS_URL}/{stream_symbol}@depth20@100ms"
        while not stop.is_set():
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    while not stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        payload = json.loads(text)
                        bids = _parse_levels(payload.get("b") or [], side="bid", allow_zero=True)
                        asks = _parse_levels(payload.get("a") or [], side="ask", allow_zero=True)
                        if not bids or not asks:
                            continue
                        await self._update_book(
                            exchange,
                            symbol,
                            OrderBookLite(bids=bids, asks=asks),
                        )
            except Exception as exc:
                logger.debug("Binance WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _bingx_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_bingx_symbol(symbol)
        interval = "200ms" if stream_symbol in ("BTC-USDT", "ETH-USDT") else "500ms"
        data_type = f"{stream_symbol}@depth20@{interval}"
        while not stop.is_set():
            try:
                async with websockets.connect(
                    BINGX_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {"id": "manual", "reqType": "sub", "dataType": data_type}
                    await ws.send(json.dumps(sub))
                    while not stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        payload = json.loads(text)
                        if "ping" in payload:
                            await ws.send(json.dumps({"pong": payload.get("ping")}))
                            continue
                        data_type_resp = payload.get("dataType") or ""
                        if data_type_resp and stream_symbol not in data_type_resp:
                            continue
                        data = payload.get("data") or {}
                        bids = _parse_levels(data.get("bids") or data.get("b") or [], side="bid")
                        asks = _parse_levels(data.get("asks") or data.get("a") or [], side="ask")
                        if not bids or not asks:
                            continue
                        await self._update_book(exchange, symbol, OrderBookLite(bids=bids, asks=asks))
            except Exception as exc:
                logger.debug("BingX WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _mexc_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_mexc_symbol(symbol)
        state = OrderBookState.empty()
        while not stop.is_set():
            try:
                async with websockets.connect(
                    MEXC_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "method": "sub.depth",
                                "param": {"symbol": stream_symbol, "depth": 20},
                                "id": 1,
                            }
                        )
                    )
                    while not stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        payload = json.loads(text)
                        channel = payload.get("channel")
                        if channel == "rs.error":
                            logger.debug("MEXC subscribe error: %s", payload)
                            continue
                        if channel == "rs.sub.depth":
                            continue
                        if payload.get("method") == "ping":
                            await ws.send(json.dumps({"method": "pong"}))
                            continue
                        if channel != "push.depth":
                            continue
                        data = payload.get("data") or {}
                        if not isinstance(data, dict):
                            continue
                        bids = _parse_levels(data.get("bids") or [], side="bid", allow_zero=True)
                        asks = _parse_levels(data.get("asks") or [], side="ask", allow_zero=True)
                        if not bids and not asks:
                            continue
                        if not state.bids and not state.asks:
                            snap_bids = [level for level in bids if level[1] > 0]
                            snap_asks = [level for level in asks if level[1] > 0]
                            state.apply_snapshot(snap_bids, snap_asks)
                        else:
                            state.apply_update(bids, asks)
                        await self._update_book(exchange, symbol, state.to_book(50))
            except Exception as exc:
                logger.debug("MEXC WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _bitget_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_bitget_symbol(symbol)
        state = OrderBookState.empty()
        while not stop.is_set():
            try:
                async with websockets.connect(
                    BITGET_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {
                        "op": "subscribe",
                        "args": [{"instType": "USDT-FUTURES", "channel": "books", "instId": stream_symbol}],
                    }
                    await ws.send(json.dumps(sub))
                    while not stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        if text == "pong":
                            continue
                        if text == "ping":
                            await ws.send("pong")
                            continue
                        payload = json.loads(text)
                        if payload.get("event") == "error":
                            logger.debug("Bitget subscribe error: %s", payload)
                        data = payload.get("data") or []
                        if not data:
                            continue
                        entry = data[0] if isinstance(data, list) else data
                        if not isinstance(entry, dict):
                            continue
                        is_snapshot = payload.get("action") == "snapshot"
                        bids = _parse_levels(
                            entry.get("bids") or entry.get("bid") or [],
                            side="bid",
                            allow_zero=not is_snapshot,
                        )
                        asks = _parse_levels(
                            entry.get("asks") or entry.get("ask") or [],
                            side="ask",
                            allow_zero=not is_snapshot,
                        )
                        if not bids and not asks:
                            continue
                        if is_snapshot or not state.bids or not state.asks:
                            state.apply_snapshot(bids, asks)
                        else:
                            state.apply_update(bids, asks)
                        await self._update_book(exchange, symbol, state.to_book(50))
            except Exception as exc:
                logger.debug("Bitget WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _okx_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_okx_symbol(symbol)
        while not stop.is_set():
            try:
                async with websockets.connect(
                    OKX_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {"op": "subscribe", "args": [{"channel": "books5", "instId": stream_symbol}]}
                    await ws.send(json.dumps(sub))
                    while not stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        if text == "pong":
                            continue
                        if text == "ping":
                            await ws.send("pong")
                            continue
                        payload = json.loads(text)
                        data = payload.get("data") or []
                        if not data:
                            continue
                        entry = data[0] if isinstance(data, list) else data
                        if not isinstance(entry, dict):
                            continue
                        bids = _parse_levels(entry.get("bids") or [], side="bid")
                        asks = _parse_levels(entry.get("asks") or [], side="ask")
                        if not bids or not asks:
                            continue
                        await self._update_book(exchange, symbol, OrderBookLite(bids=bids, asks=asks))
            except Exception as exc:
                logger.debug("OKX WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _gate_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_gate_symbol(symbol)
        while not stop.is_set():
            try:
                async with websockets.connect(
                    GATE_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {
                        "time": int(time.time()),
                        "channel": "futures.order_book",
                        "event": "subscribe",
                        "payload": [stream_symbol, "20", "0"],
                    }
                    await ws.send(json.dumps(sub))
                    while not stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        payload = json.loads(text)
                        if payload.get("event") not in ("update", "all"):
                            continue
                        result = payload.get("result") or {}
                        bids = _parse_levels(result.get("bids") or [], side="bid")
                        asks = _parse_levels(result.get("asks") or [], side="ask")
                        if not bids or not asks:
                            continue
                        await self._update_book(exchange, symbol, OrderBookLite(bids=bids, asks=asks))
            except Exception as exc:
                logger.debug("Gate WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _kucoin_loop(self, symbol: str, exchange: str, stop: asyncio.Event) -> None:
        stream_symbol = _normalize_kucoin_symbol(symbol)
        while not stop.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(KUCOIN_REST_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        payload = await resp.json()
                data = payload.get("data") or {}
                servers = data.get("instanceServers") or []
                if not servers:
                    raise RuntimeError("Kucoin WS servers unavailable")
                endpoint = servers[0].get("endpoint")
                token = data.get("token")
                ping_interval = servers[0].get("pingInterval", 15000) / 1000.0
                if not endpoint or not token:
                    raise RuntimeError("Kucoin WS token missing")
                ws_url = f"{endpoint}?token={token}"
                async with websockets.connect(
                    ws_url,
                    ping_interval=None,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    topic = f"/contractMarket/level2Depth5:{stream_symbol}"
                    await ws.send(json.dumps({"id": "manual", "type": "subscribe", "topic": topic, "response": True}))
                    last_ping = time.time()
                    while not stop.is_set():
                        if time.time() - last_ping > ping_interval:
                            await ws.send(json.dumps({"id": "manual", "type": "ping"}))
                            last_ping = time.time()
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        text = _decode_message(message)
                        if not text:
                            continue
                        payload = json.loads(text)
                        if payload.get("type") == "pong":
                            continue
                        data = payload.get("data") or {}
                        bids = _parse_levels(data.get("bids") or [], side="bid")
                        asks = _parse_levels(data.get("asks") or [], side="ask")
                        if not bids or not asks:
                            continue
                        await self._update_book(exchange, symbol, OrderBookLite(bids=bids, asks=asks))
            except Exception as exc:
                logger.debug("Kucoin WS error: %s", exc)
                await asyncio.sleep(1.0)
