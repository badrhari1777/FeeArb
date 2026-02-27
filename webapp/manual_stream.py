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
from fastapi import WebSocket

from execution.manual import spread_pct
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

    @property
    def mid(self) -> float | None:
        if not self.best_bid or not self.best_ask:
            return None
        return (self.best_bid + self.best_ask) / 2.0


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


def _top_liquidity(levels: List[List[float]], top_n: int = 3) -> float:
    total = 0.0
    for price, qty in levels[:top_n]:
        total += (price or 0.0) * (qty or 0.0)
    return total


 


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


class ManualSpreadStream:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        self._config: dict[str, Any] = {}
        self._subscriptions: dict[str, str] = {}
        self._stream_key: tuple[str, str, str] | None = None

    async def run(self) -> None:
        try:
            while True:
                message = await self._websocket.receive_json()
                action = message.get("action")
                if action == "subscribe":
                    await self._start_stream(message)
                elif action == "close":
                    break
        except Exception:
            pass
        finally:
            await self._shutdown()

    async def _start_stream(self, message: dict[str, Any]) -> None:
        symbol = str(message.get("symbol") or "").upper()
        long_exchange = normalize_exchange_name(str(message.get("long_exchange") or "")).lower()
        short_exchange = normalize_exchange_name(str(message.get("short_exchange") or "")).lower()
        if not symbol or not long_exchange or not short_exchange:
            await self._websocket.send_json({"type": "error", "error": "missing subscription fields"})
            return
        next_config = dict(message)
        next_config["symbol"] = symbol
        next_config["long_exchange"] = long_exchange
        next_config["short_exchange"] = short_exchange
        next_key = (symbol, long_exchange, short_exchange)
        if self._tasks and self._stream_key == next_key:
            # Keep existing WS loops alive; only update dispatch settings.
            self._config = next_config
            return
        await self._shutdown()
        self._config = next_config
        self._stream_key = next_key
        self._stop.clear()
        exchanges = {long_exchange, short_exchange}
        self._subscriptions = self._build_subscriptions(symbol, exchanges)
        if "bybit" in exchanges:
            self._tasks.append(asyncio.create_task(self._bybit_loop(symbol)))
        if "binance" in exchanges:
            self._tasks.append(asyncio.create_task(self._binance_loop(symbol)))
        if "bingx" in exchanges:
            self._tasks.append(asyncio.create_task(self._bingx_loop(symbol)))
        if "mexc" in exchanges:
            self._tasks.append(asyncio.create_task(self._mexc_loop(symbol)))
        if "bitget" in exchanges:
            self._tasks.append(asyncio.create_task(self._bitget_loop(symbol)))
        if "okx" in exchanges:
            self._tasks.append(asyncio.create_task(self._okx_loop(symbol)))
        if "gate" in exchanges:
            self._tasks.append(asyncio.create_task(self._gate_loop(symbol)))
        if "kucoin" in exchanges:
            self._tasks.append(asyncio.create_task(self._kucoin_loop(symbol)))
        self._tasks.append(asyncio.create_task(self._dispatch_loop()))

    async def _shutdown(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._tasks.clear()
        self._subscriptions = {}
        self._stream_key = None

    async def _dispatch_loop(self) -> None:
        latest: dict[str, OrderBookLite] = {}
        last_seen: dict[str, float] = {}
        last_status = 0.0
        while not self._stop.is_set():
            update = None
            try:
                update = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                update = None
            if update:
                exchange = update.get("exchange")
                if exchange:
                    latest[exchange] = update["book"]
                    last_seen[exchange] = time.time()
            config = self._config or {}
            symbol = str(config.get("symbol") or "")
            long_exchange = normalize_exchange_name(str(config.get("long_exchange") or "")).lower()
            short_exchange = normalize_exchange_name(str(config.get("short_exchange") or "")).lower()
            spread_min_pct = _safe_float(config.get("spread_min_pct"))
            spread_max_pct = _safe_float(config.get("spread_max_pct"))
            include_orderbook = bool(config.get("include_orderbook"))
            orderbook_depth = int(_safe_float(config.get("orderbook_depth")) or 5)
            orderbook_depth = max(1, orderbook_depth)
            long_book = latest.get(long_exchange)
            short_book = latest.get(short_exchange)
            now = time.time()
            if long_book and short_book:
                spread_value = spread_pct(long_book.mid, short_book.mid)
                within = None
                if spread_min_pct is not None or spread_max_pct is not None:
                    if spread_value is not None:
                        within = True
                        if spread_min_pct is not None and spread_value < spread_min_pct:
                            within = False
                        if spread_max_pct is not None and spread_value > spread_max_pct:
                            within = False
                payload = {
                    "type": "spread",
                    "symbol": symbol,
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "spread_pct": spread_value,
                    "spread_range": {"min": spread_min_pct, "max": spread_max_pct},
                    "within_range": within,
                    "timestamp": now,
                    "long": self._book_payload(long_book, include_levels=include_orderbook, depth=orderbook_depth),
                    "short": self._book_payload(short_book, include_levels=include_orderbook, depth=orderbook_depth),
                    "subscriptions": dict(self._subscriptions),
                }
                await self._websocket.send_json(payload)
                last_status = now
                continue

            if now - last_status >= 2.0:
                missing = []
                if not long_book:
                    missing.append(long_exchange)
                if not short_book:
                    missing.append(short_exchange)
                await self._websocket.send_json(
                    {
                        "type": "status",
                        "status": "waiting",
                        "symbol": symbol,
                        "missing": missing,
                        "last_seen": {key: last_seen.get(key) for key in missing},
                        "subscriptions": dict(self._subscriptions),
                        "timestamp": now,
                    }
                )
                last_status = now

    async def _bybit_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_bybit_symbol(symbol)
        state = OrderBookState.empty()
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    BYBIT_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {"op": "subscribe", "args": [f"orderbook.50.{stream_symbol}"]}
                    await ws.send(json.dumps(sub))
                    while not self._stop.is_set():
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
                        await self._queue.put(
                            {
                                "exchange": "bybit",
                                "book": state.to_book(50),
                            }
                        )
            except Exception as exc:
                logger.debug("Bybit WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _binance_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_binance_symbol(symbol).lower()
        if not stream_symbol:
            return
        ws_url = f"{BINANCE_WS_URL}/{stream_symbol}@depth20@100ms"
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    while not self._stop.is_set():
                        message = await ws.recv()
                        text = _decode_message(message)
                        if not text:
                            continue
                        payload = json.loads(text)
                        bids = _parse_levels(payload.get("b") or [], side="bid", allow_zero=True)
                        asks = _parse_levels(payload.get("a") or [], side="ask", allow_zero=True)
                        if not bids or not asks:
                            continue
                        await self._queue.put(
                            {
                                "exchange": "binance",
                                "book": OrderBookLite(bids=bids, asks=asks),
                            }
                        )
            except Exception as exc:
                logger.debug("Binance WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _bingx_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_bingx_symbol(symbol)
        interval = "200ms" if stream_symbol in ("BTC-USDT", "ETH-USDT") else "500ms"
        data_type = f"{stream_symbol}@depth20@{interval}"
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    BINGX_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {"id": "manual", "reqType": "sub", "dataType": data_type}
                    await ws.send(json.dumps(sub))
                    while not self._stop.is_set():
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
                        await self._queue.put(
                            {
                                "exchange": "bingx",
                                "book": OrderBookLite(bids=bids, asks=asks),
                            }
                        )
            except Exception as exc:
                logger.debug("BingX WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _mexc_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_mexc_symbol(symbol)
        state = OrderBookState.empty()
        while not self._stop.is_set():
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
                    while not self._stop.is_set():
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
                        await self._queue.put(
                            {
                                "exchange": "mexc",
                                "book": state.to_book(50),
                            }
                        )
            except Exception as exc:
                logger.debug("MEXC WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _bitget_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_bitget_symbol(symbol)
        state = OrderBookState.empty()
        while not self._stop.is_set():
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
                    while not self._stop.is_set():
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
                        await self._queue.put(
                            {
                                "exchange": "bitget",
                                "book": state.to_book(50),
                            }
                        )
            except Exception as exc:
                logger.debug("Bitget WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _okx_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_okx_symbol(symbol)
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    OKX_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_000_000,
                ) as ws:
                    sub = {"op": "subscribe", "args": [{"channel": "books5", "instId": stream_symbol}]}
                    await ws.send(json.dumps(sub))
                    while not self._stop.is_set():
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
                        await self._queue.put(
                            {
                                "exchange": "okx",
                                "book": OrderBookLite(bids=bids, asks=asks),
                            }
                        )
            except Exception as exc:
                logger.debug("OKX WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _gate_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_gate_symbol(symbol)
        while not self._stop.is_set():
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
                    while not self._stop.is_set():
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
                        await self._queue.put(
                            {
                                "exchange": "gate",
                                "book": OrderBookLite(bids=bids, asks=asks),
                            }
                        )
            except Exception as exc:
                logger.debug("Gate WS error: %s", exc)
                await asyncio.sleep(1.0)

    async def _kucoin_loop(self, symbol: str) -> None:
        stream_symbol = _normalize_kucoin_symbol(symbol)
        while not self._stop.is_set():
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
                    while not self._stop.is_set():
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
                        await self._queue.put(
                            {
                                "exchange": "kucoin",
                                "book": OrderBookLite(bids=bids, asks=asks),
                            }
                        )
            except Exception as exc:
                logger.debug("Kucoin WS error: %s", exc)
                await asyncio.sleep(1.0)

    def _book_payload(
        self,
        book: OrderBookLite,
        *,
        include_levels: bool = False,
        depth: int = 5,
    ) -> dict[str, Any]:
        bid_liq = _top_liquidity(book.bids)
        ask_liq = _top_liquidity(book.asks)
        payload = {
            "best_bid": book.best_bid,
            "best_ask": book.best_ask,
            "mid": book.mid,
            "bid_liquidity_top3": bid_liq,
            "ask_liquidity_top3": ask_liq,
            "min_liquidity_top3": min(bid_liq, ask_liq) if bid_liq and ask_liq else 0.0,
        }
        if include_levels:
            limit = max(1, min(20, int(depth)))
            payload["bids"] = book.bids[:limit]
            payload["asks"] = book.asks[:limit]
        return payload

    def _build_subscriptions(self, symbol: str, exchanges: set[str]) -> dict[str, str]:
        subs: dict[str, str] = {}
        if "bybit" in exchanges:
            bybit_symbol = _normalize_bybit_symbol(symbol)
            subs["bybit"] = f"bybit orderbook.50.{bybit_symbol}"
        if "binance" in exchanges:
            binance_symbol = _normalize_binance_symbol(symbol).lower()
            subs["binance"] = f"binance depth20 {binance_symbol}"
        if "bingx" in exchanges:
            bingx_symbol = _normalize_bingx_symbol(symbol)
            interval = "200ms" if bingx_symbol in ("BTC-USDT", "ETH-USDT") else "500ms"
            subs["bingx"] = f"bingx swap-market {bingx_symbol}@depth20@{interval}"
        if "mexc" in exchanges:
            mexc_symbol = _normalize_mexc_symbol(symbol)
            subs["mexc"] = f"mexc edge sub.depth {mexc_symbol} depth20"
        if "bitget" in exchanges:
            bitget_symbol = _normalize_bitget_symbol(symbol)
            subs["bitget"] = f"bitget USDT-FUTURES books {bitget_symbol}"
        if "okx" in exchanges:
            okx_symbol = _normalize_okx_symbol(symbol)
            subs["okx"] = f"okx books5 {okx_symbol}"
        if "gate" in exchanges:
            gate_symbol = _normalize_gate_symbol(symbol)
            subs["gate"] = f"gate futures.order_book {gate_symbol}"
        if "kucoin" in exchanges:
            kucoin_symbol = _normalize_kucoin_symbol(symbol)
            subs["kucoin"] = f"kucoin futures level2Depth5 {kucoin_symbol}"
        return subs
