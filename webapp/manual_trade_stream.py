from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, Optional

import websockets
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from config import BASE_DIR
from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _bootstrap_env, _ccxt_perp_symbol
from execution.accounts import _safe_float
from exchanges import normalize_exchange_name
from .manual_symbols import _normalize_bybit_symbol, _normalize_okx_symbol

logger = logging.getLogger(__name__)
_DEBUG_LOGGER = logging.getLogger("manual_trade_ws")
if not _DEBUG_LOGGER.handlers:
    _DEBUG_LOGGER.setLevel(logging.INFO)
    log_path = BASE_DIR / "logs" / "manual_trade_ws.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _DEBUG_LOGGER.addHandler(handler)
    _DEBUG_LOGGER.propagate = False

BYBIT_TRADE_WS_URL = "wss://stream.bybit.com/v5/trade"
OKX_PRIVATE_WS_URL = "wss://ws.okx.com:8443/ws/v5/private"


def _precision_to_step(precision: int | None) -> float | None:
    if precision is None:
        return None
    try:
        return 10 ** (-int(precision))
    except (TypeError, ValueError, OverflowError):
        return None


def _round_to_step(value: float, step: float | None, *, mode: str) -> float:
    if step is None or step <= 0:
        return value
    if mode == "up":
        return (int(value / step + 0.999999) * step)
    return (int(value / step) * step)


class ManualTradeStream:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._stop = asyncio.Event()
        self._remote_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._trade_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._trade_reader_task: Optional[asyncio.Task] = None
        self._config: dict[str, Any] = {}
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}
        self._ready = False
        self._ready_private = False
        self._ready_trade = False
        self._last_rx_ts = 0.0
        self._last_rx_ts_trade = 0.0

    async def run(self) -> None:
        try:
            while True:
                message = await self._websocket.receive_json()
                action = message.get("action")
                if action == "connect":
                    await self._connect(message)
                elif action == "order":
                    await self._place_order(message)
                elif action == "cancel":
                    await self._cancel_order(message)
                elif action == "fetch":
                    await self._fetch_order(message)
                elif action == "close":
                    break
        except WebSocketDisconnect:
            logger.info("ManualTradeStream client disconnected")
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("ManualTradeStream crashed")
            await self._send({"type": "error", "error": f"server_error: {exc}"})
        finally:
            await self._shutdown()

    async def _send(self, payload: dict[str, Any]) -> None:
        try:
            await self._websocket.send_json(payload)
        except Exception:
            return
        try:
            logger.info("manual-trade -> ui: %s", payload)
        except Exception:
            pass
        try:
            _DEBUG_LOGGER.info("ui_tx %s", payload)
        except Exception:
            pass

    async def _shutdown(self) -> None:
        self._stop.set()
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except Exception:
                pass
            self._reader_task = None
        if self._trade_reader_task:
            self._trade_reader_task.cancel()
            try:
                await self._trade_reader_task
            except Exception:
                pass
            self._trade_reader_task = None
        if self._remote_ws:
            try:
                await self._remote_ws.close()
            except Exception:
                pass
            self._remote_ws = None
        if self._trade_ws:
            try:
                await self._trade_ws.close()
            except Exception:
                pass
            self._trade_ws = None
        self._ready = False
        self._ready_private = False
        self._ready_trade = False
        self._last_rx_ts = 0.0
        self._last_rx_ts_trade = 0.0

    async def _connect(self, message: dict[str, Any]) -> None:
        await self._shutdown()
        exchange = normalize_exchange_name(str(message.get("exchange") or "")).lower()
        symbol = str(message.get("symbol") or "").upper().strip()
        if exchange not in ("bybit", "okx"):
            await self._send({"type": "error", "error": "only bybit/okx are supported for now"})
            return
        if not symbol:
            await self._send({"type": "error", "error": "symbol is required"})
            return
        self._config = {"exchange": exchange, "symbol": symbol}
        self._ready = False
        self._ready_private = False
        self._ready_trade = False
        try:
            if exchange == "bybit":
                self._trade_ws = await websockets.connect(
                    BYBIT_TRADE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                )
                self._remote_ws = self._trade_ws
            else:
                self._remote_ws = await websockets.connect(
                    OKX_PRIVATE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                )
        except Exception as exc:  # pylint: disable=broad-except
            await self._send({"type": "error", "error": f"{exchange} ws connect failed: {exc}"})
            return
        if exchange == "bybit":
            ok_trade = await self._bybit_auth_ws(self._trade_ws, source="bybit-trade")
            if not ok_trade:
                await self._send({"type": "error", "error": "bybit ws auth failed"})
                return
            self._trade_reader_task = asyncio.create_task(
                self._bybit_reader_loop(self._trade_ws, source="bybit-trade")
            )
            self._ready_private = False
            self._ready_trade = True
            self._ready = True
        else:
            ok = await self._okx_auth()
            if not ok:
                await self._send({"type": "error", "error": "okx ws auth failed"})
                return
            await self._okx_send({"op": "subscribe", "args": [{"channel": "orders", "instType": "SWAP"}]})
            self._reader_task = asyncio.create_task(self._okx_reader_loop())
            self._ready = True
        await self._send(
            {
                "type": "status",
                "status": "connected",
                "exchange": exchange,
                "symbol": symbol,
            }
        )

    async def _bybit_auth_ws(
        self,
        ws: Optional[websockets.WebSocketClientProtocol],
        *,
        source: str,
    ) -> bool:
        if ws is None:
            return False
        _bootstrap_env(force=True)
        gateway = self._gateways.get("bybit")
        if gateway is None:
            return False
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        if not api_key or not api_secret:
            await self._send({"type": "error", "error": "bybit api key/secret missing"})
            return False
        expires = int(time.time() * 1000) + 5000
        sign_payload = f"GET/realtime{expires}"
        signature = hmac.new(
            api_secret.encode("utf-8"),
            sign_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        try:
            await ws.send(json.dumps({"op": "auth", "args": [api_key, expires, signature]}))
        except Exception as exc:  # pylint: disable=broad-except
            await self._send({"type": "error", "error": f"{source} auth send failed: {exc}"})
            return False
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=2)
            except Exception:
                continue
            payload = self._decode_message(message)
            if not payload:
                continue
            if payload.get("op") == "auth":
                if payload.get("success") is True or str(payload.get("retCode")) == "0":
                    await self._send({"type": "status", "status": "auth_ok", "source": source})
                    return True
                await self._send({"type": "error", "error": f"{source} auth_failed: {payload}"})
                return False
            await self._send({"type": "event", "payload": payload})
        return False

    async def _bybit_reader_loop(
        self,
        ws: Optional[websockets.WebSocketClientProtocol],
        *,
        source: str,
    ) -> None:
        if not ws:
            return
        while not self._stop.is_set():
            try:
                message = await ws.recv()
            except Exception:
                await self._send({"type": "status", "status": "ws_closed"})
                break
            raw_text: str | None = None
            raw_b64: str | None = None
            if isinstance(message, bytes):
                try:
                    raw_text = message.decode("utf-8")
                except Exception:
                    raw_b64 = base64.b64encode(message).decode("ascii")
            elif isinstance(message, str):
                raw_text = message
            await self._send(
                {
                    "type": "rx_raw",
                    "source": source,
                    "raw": raw_text,
                    "raw_b64": raw_b64,
                }
            )
            try:
                logger.info("bybit %s rx_raw: %s", source, raw_text if raw_text is not None else raw_b64)
            except Exception:
                pass
            try:
                _DEBUG_LOGGER.info(
                    "bybit_rx %s %s",
                    source,
                    raw_text if raw_text is not None else raw_b64,
                )
            except Exception:
                pass
            payload = self._decode_message(message)
            if not payload:
                continue
            now = time.time()
            self._last_rx_ts = now
            if source == "bybit-trade":
                self._last_rx_ts_trade = now
            await self._send({"type": "rx", "source": source, "payload": payload})
            if payload.get("op") == "ping":
                try:
                    await ws.send(json.dumps({"op": "pong"}))
                except Exception:
                    pass
                continue
            if payload.get("op") in ("order.create", "order.cancel"):
                order_id = None
                data = payload.get("data")
                if isinstance(data, dict):
                    order_id = data.get("orderId") or data.get("order_id")
                await self._send(
                    {
                        "type": "order_ack",
                        "order_id": order_id,
                        "success": payload.get("success"),
                        "retCode": payload.get("retCode") or payload.get("code"),
                        "retMsg": payload.get("retMsg") or payload.get("msg"),
                        "source": source,
                        "payload": payload,
                    }
                )
                continue
            await self._send({"type": "event", "payload": payload})

    async def _okx_auth(self) -> bool:
        _bootstrap_env(force=True)
        gateway = self._gateways.get("okx")
        if gateway is None:
            return False
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        passphrase = gateway.password
        if not api_key or not api_secret or not passphrase:
            await self._send({"type": "error", "error": "okx api key/secret/passphrase missing"})
            return False
        timestamp = str(time.time())
        prehash = f"{timestamp}GET/users/self/verify"
        signature = base64.b64encode(
            hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
        await self._okx_send(
            {
                "op": "login",
                "args": [
                    {
                        "apiKey": api_key,
                        "passphrase": passphrase,
                        "timestamp": timestamp,
                        "sign": signature,
                    }
                ],
            }
        )
        if not self._remote_ws:
            return False
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                message = await asyncio.wait_for(self._remote_ws.recv(), timeout=2)
            except Exception:
                continue
            payload = self._decode_message(message)
            if not payload:
                continue
            if payload.get("event") == "login":
                if payload.get("code") == "0":
                    await self._send({"type": "status", "status": "auth_ok"})
                    return True
                await self._send({"type": "error", "error": f"auth_failed: {payload}"})
                return False
            await self._send({"type": "event", "payload": payload})
        return False

    async def _okx_reader_loop(self) -> None:
        if not self._remote_ws:
            return
        while not self._stop.is_set():
            try:
                message = await self._remote_ws.recv()
            except Exception:
                await self._send({"type": "status", "status": "ws_closed"})
                break
            payload = self._decode_message(message)
            if not payload:
                continue
            self._last_rx_ts = time.time()
            await self._send({"type": "rx", "payload": payload})
            if payload.get("event") in ("order", "cancel-order"):
                order_id = None
                data = payload.get("data") or []
                if isinstance(data, list) and data:
                    order_id = data[0].get("ordId") or data[0].get("orderId")
                await self._send({"type": "order_ack", "order_id": order_id, "payload": payload})
                continue
            await self._send({"type": "event", "payload": payload})

    async def _bybit_trade_send(self, payload: dict[str, Any]) -> None:
        if not self._trade_ws:
            return
        try:
            await self._trade_ws.send(json.dumps(payload))
        except Exception as exc:  # pylint: disable=broad-except
            await self._send({"type": "error", "error": f"bybit trade ws send failed: {exc}"})
        await self._send({"type": "tx", "source": "bybit-trade", "payload": payload})

    async def _okx_send(self, payload: dict[str, Any]) -> None:
        if not self._remote_ws:
            return
        try:
            await self._remote_ws.send(json.dumps(payload))
        except Exception as exc:  # pylint: disable=broad-except
            await self._send({"type": "error", "error": f"okx ws send failed: {exc}"})
        await self._send({"type": "tx", "source": "okx", "payload": payload})

    async def _resolve_price(
        self,
        *,
        exchange: str,
        symbol: str,
        side: str,
        price_mode: str,
        offset_bps: float,
        offset_ticks: int,
    ) -> float | None:
        gateway = self._gateways.get(exchange)
        if gateway is None:
            return None
        await gateway.refresh_credentials_async(force_env=True)
        await gateway.ensure_client()
        client = gateway.client
        if client is None:
            return None
        ccxt_symbol = _ccxt_perp_symbol(symbol)
        try:
            await client.load_markets()
        except Exception:
            pass
        market = client.markets.get(ccxt_symbol) if getattr(client, "markets", None) else None
        price_step = None
        if isinstance(market, dict):
            price_step = _safe_float(market.get("priceStep"))
            if price_step is None:
                precision = None
                if isinstance(market.get("precision"), dict):
                    precision = market["precision"].get("price")
                price_step = _precision_to_step(precision)
        try:
            orderbook = await client.fetch_order_book(ccxt_symbol, limit=5)
        except Exception:
            return None
        bids = orderbook.get("bids") or []
        asks = orderbook.get("asks") or []
        best_bid = _safe_float(bids[0][0]) if bids else None
        best_ask = _safe_float(asks[0][0]) if asks else None
        if best_bid is None or best_ask is None:
            return None
        side = side.lower()
        if price_mode == "marketable":
            price = best_ask if side == "buy" else best_bid
        else:
            price = best_bid if side == "buy" else best_ask
            if offset_bps:
                adj = price * (offset_bps / 10000.0)
                price = price - adj if side == "buy" else price + adj
            if offset_ticks and price_step:
                price = price - (offset_ticks * price_step) if side == "buy" else price + (offset_ticks * price_step)
        if price_step:
            price = _round_to_step(price, price_step, mode="down" if side == "buy" else "up")
        return price

    async def _place_order(self, message: dict[str, Any]) -> None:
        exchange = normalize_exchange_name(str(message.get("exchange") or self._config.get("exchange") or "")).lower()
        if not exchange:
            await self._send({"type": "error", "error": "exchange is required"})
            return
        if not self._remote_ws:
            await self._send({"type": "error", "error": "not connected"})
            return
        if exchange == "bybit" and not self._ready_trade:
            await self._send({"type": "error", "error": "not authenticated"})
            return
        if exchange != "bybit" and not self._ready:
            await self._send({"type": "error", "error": "not authenticated"})
            return
        symbol_input = str(message.get("symbol") or self._config.get("symbol") or "").upper().strip()
        if exchange == "bybit":
            symbol = _normalize_bybit_symbol(symbol_input)
        elif exchange == "okx":
            symbol = symbol_input
        else:
            await self._send({"type": "error", "error": "unsupported exchange"})
            return
        side = str(message.get("side") or "").lower()
        order_type = str(message.get("order_type") or "limit").lower()
        qty = _safe_float(message.get("qty"))
        reduce_only = bool(message.get("reduce_only"))
        position_side = str(message.get("position_side") or "").lower()
        price = _safe_float(message.get("price"))
        price_mode = str(message.get("price_mode") or "")
        offset_bps = _safe_float(message.get("offset_bps")) or 0.0
        offset_ticks = int(_safe_float(message.get("offset_ticks")) or 0)
        if not symbol or side not in ("buy", "sell") or not qty:
            await self._send({"type": "error", "error": "symbol, side, qty required"})
            return
        if order_type == "limit" and (price is None or price <= 0) and price_mode:
            price = await self._resolve_price(
                exchange=exchange,
                symbol=symbol_input,
                side=side,
                price_mode=price_mode,
                offset_bps=offset_bps,
                offset_ticks=offset_ticks,
            )
        if order_type == "limit" and (price is None or price <= 0):
            await self._send({"type": "error", "error": "limit price missing"})
            return
        if exchange == "bybit":
            req_id = f"req-{int(time.time() * 1000)}"
            api_ts = int(time.time() * 1000)
            recv_window = 5000
            args: dict[str, Any] = {
                "category": "linear",
                "symbol": symbol,
                "side": "Buy" if side == "buy" else "Sell",
                "orderType": "Limit" if order_type == "limit" else "Market",
                "qty": str(qty),
                "timeInForce": "GTC",
                "apiTimestamp": str(api_ts),
                "recvWindow": recv_window,
            }
            if order_type == "limit":
                args["price"] = str(price)
            if reduce_only:
                args["reduceOnly"] = True
            if position_side in ("long", "short", "net"):
                args["positionIdx"] = 0
                if position_side == "long":
                    args["positionIdx"] = 1
                elif position_side == "short":
                    args["positionIdx"] = 2
            await self._bybit_trade_send(
                {
                    "op": "order.create",
                    "reqId": req_id,
                    "header": {
                        "X-BAPI-TIMESTAMP": str(api_ts),
                        "X-BAPI-RECV-WINDOW": str(recv_window),
                    },
                    "args": [args],
                }
            )
            return

        if exchange == "okx":
            req_id = f"req-{int(time.time() * 1000)}"
            inst_id = _normalize_okx_symbol(symbol_input)
            args = {
                "instId": inst_id,
                "tdMode": "isolated",
                "side": side,
                "ordType": "limit" if order_type == "limit" else "market",
                "sz": str(qty),
            }
            if order_type == "limit":
                args["px"] = str(price)
            if reduce_only:
                args["reduceOnly"] = True
            if position_side in ("long", "short"):
                args["posSide"] = position_side
            await self._okx_send({"op": "order", "id": req_id, "args": [args]})
            await self._send(
                {
                    "type": "info",
                    "message": "order sent; awaiting okx response",
                    "req_id": req_id,
                }
            )
            asyncio.create_task(self._warn_if_no_rx(exchange, "okx", 3.0))

    async def _cancel_order(self, message: dict[str, Any]) -> None:
        exchange = normalize_exchange_name(str(message.get("exchange") or self._config.get("exchange") or "")).lower()
        if not exchange:
            await self._send({"type": "error", "error": "exchange is required"})
            return
        if not self._remote_ws:
            await self._send({"type": "error", "error": "not connected"})
            return
        if exchange == "bybit" and not self._ready_trade:
            await self._send({"type": "error", "error": "not authenticated"})
            return
        if exchange != "bybit" and not self._ready:
            await self._send({"type": "error", "error": "not authenticated"})
            return
        symbol = str(message.get("symbol") or self._config.get("symbol") or "")
        order_id = str(message.get("order_id") or "").strip()
        if not symbol or not order_id:
            await self._send({"type": "error", "error": "symbol and order_id required"})
            return
        if exchange == "bybit":
            symbol = _normalize_bybit_symbol(symbol)
            await self._bybit_trade_send(
                {
                    "op": "order.cancel",
                    "args": [
                        {
                            "category": "linear",
                            "symbol": symbol,
                            "orderId": order_id,
                            "apiTimestamp": str(int(time.time() * 1000)),
                            "recvWindow": 5000,
                        }
                    ],
                    "header": {
                        "X-BAPI-TIMESTAMP": str(int(time.time() * 1000)),
                        "X-BAPI-RECV-WINDOW": "5000",
                    },
                }
            )
            return
        if exchange == "okx":
            inst_id = _normalize_okx_symbol(symbol)
            await self._okx_send(
                {"op": "cancel-order", "args": [{"instId": inst_id, "ordId": order_id}]}
            )

    async def _fetch_order(self, message: dict[str, Any]) -> None:
        exchange = normalize_exchange_name(str(message.get("exchange") or self._config.get("exchange") or "")).lower()
        symbol = str(message.get("symbol") or self._config.get("symbol") or "")
        order_id = str(message.get("order_id") or "").strip()
        if not self._ready:
            await self._send({"type": "error", "error": "not authenticated"})
            return
        gateway = self._gateways.get(exchange)
        if gateway is None:
            await self._send({"type": "error", "error": f"{exchange} gateway unavailable"})
            return
        await gateway.refresh_credentials_async(force_env=True)
        await gateway.ensure_client()
        client = gateway.client
        if client is None:
            await self._send({"type": "error", "error": f"{exchange} client unavailable"})
            return
        ccxt_symbol = _ccxt_perp_symbol(symbol)
        try:
            await client.load_markets()
        except Exception:
            pass
        if not order_id:
            await self._send({"type": "error", "error": "order_id required"})
            return
        try:
            params = {"acknowledged": True} if exchange == "bybit" else None
            order = await client.fetch_order(order_id, ccxt_symbol, params)
        except Exception as exc:  # pylint: disable=broad-except
            if hasattr(client, "fetch_open_order"):
                try:
                    order = await client.fetch_open_order(order_id, ccxt_symbol)
                except Exception:
                    order = None
            else:
                order = None
            if order is None and hasattr(client, "fetch_closed_order"):
                try:
                    order = await client.fetch_closed_order(order_id, ccxt_symbol)
                except Exception:
                    order = None
            if order is None:
                await self._send({"type": "error", "error": f"fetch failed: {exc}"})
                return
        await self._send({"type": "fetch", "order": order})

    async def _warn_if_no_rx(self, exchange: str, source: str, timeout: float) -> None:
        start = time.time()
        last_rx = self._last_rx_ts_trade if source == "bybit-trade" else self._last_rx_ts
        await asyncio.sleep(timeout)
        if source == "bybit-trade":
            current = self._last_rx_ts_trade
        else:
            current = self._last_rx_ts
        if current <= last_rx and time.time() - start >= timeout:
            await self._send(
                {
                    "type": "error",
                    "error": f"{exchange} ws: no response after {timeout:.0f}s (check auth/permissions/symbol)",
                    "source": source,
                }
            )

    @staticmethod
    def _decode_message(message: Any) -> dict[str, Any] | None:
        if isinstance(message, bytes):
            try:
                try:
                    message = gzip.decompress(message).decode("utf-8")
                except Exception:
                    message = message.decode("utf-8")
            except Exception:
                return None
        if isinstance(message, str):
            try:
                return json.loads(message)
            except Exception:
                return None
        if isinstance(message, dict):
            return message
        return None
