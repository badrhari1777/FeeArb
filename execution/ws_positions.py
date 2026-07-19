from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Mapping, Optional
import zlib

import websockets

from execution.accounts import ExchangeGateway, EXCHANGE_SPECS, _bootstrap_env, normalize_symbol
from execution.accounts import _safe_float
from execution.kucoin_auth import fetch_kucoin_private_ws_endpoint

logger = logging.getLogger(__name__)

BYBIT_PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private"
OKX_PRIVATE_WS_URL = "wss://ws.okx.com:8443/ws/v5/private"
GATE_PRIVATE_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
BITGET_PRIVATE_WS_URL = "wss://ws.bitget.com/v2/ws/private"
BINGX_SWAP_WS_URL = "wss://open-api-swap.bingx.com/swap-market"


def _decode_gzip_message(message: object) -> str | None:
    if isinstance(message, bytes):
        try:
            return gzip.decompress(message).decode("utf-8")
        except Exception:
            try:
                return zlib.decompress(message, 16 + zlib.MAX_WBITS).decode("utf-8")
            except Exception:
                try:
                    return zlib.decompress(message).decode("utf-8")
                except Exception:
                    try:
                        return message.decode("utf-8")
                    except Exception:
                        return None
        return None
    if isinstance(message, str):
        return message
    return None


def _bingx_is_ping_message(text: str, payload: object) -> bool:
    if isinstance(payload, dict) and "ping" in payload:
        return True
    stripped = text.strip().lower()
    if stripped == "ping":
        return True
    if '"ping"' in stripped and '"pong"' not in stripped:
        return True
    return False


def _gate_sign_auth_message(
    *,
    channel: str,
    event: str,
    timestamp: int,
    payload: object,
    api_key: str,
    api_secret: str,
) -> dict[str, Any]:
    signature_payload = f"channel={channel}&event={event}&time={timestamp}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        signature_payload.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()
    return {
        "time": timestamp,
        "channel": channel,
        "event": event,
        "payload": payload,
        "auth": {"method": "api_key", "KEY": api_key, "SIGN": signature},
    }


class _BasePositionStream:
    def __init__(self, exchange: str, contract_size: float | None = None) -> None:
        self.exchange = exchange
        self._contract_size = contract_size
        self._positions: dict[str, dict[str, float]] = {}
        self._last_update = 0.0
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def set_contract_size(self, contract_size: float | None) -> None:
        if contract_size and contract_size > 0:
            self._contract_size = contract_size

    def is_live(self, *, stale_after: float) -> bool:
        if not self._last_update:
            return False
        return (time.time() - self._last_update) <= stale_after

    def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        canonical = normalize_symbol(symbol) if symbol else ""
        positions: list[dict[str, Any]] = []
        for sym, sides in self._positions.items():
            if canonical and sym != canonical:
                continue
            for side, qty in sides.items():
                positions.append(
                    {
                        "exchange": self.exchange,
                        "symbol": sym,
                        "side": side,
                        "coin_qty": qty,
                    }
                )
        return positions

    def _update_position(self, symbol: str, side: str, qty: float | None) -> None:
        if qty is None:
            return
        canonical = normalize_symbol(symbol)
        if not canonical:
            return
        side = side.lower()
        if side not in ("long", "short"):
            return
        if canonical not in self._positions:
            self._positions[canonical] = {}
        self._positions[canonical][side] = max(0.0, float(qty))
        self._last_update = time.time()

    def _mark_live(self) -> None:
        self._last_update = time.time()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
            self._task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("%s ws stream error: %s", self.exchange, exc)
                await asyncio.sleep(2.0)

    async def _connect_and_listen(self) -> None:
        raise NotImplementedError


class BybitPositionStream(_BasePositionStream):
    def __init__(self, gateway: ExchangeGateway, contract_size: float | None = None) -> None:
        super().__init__("bybit", contract_size=contract_size)
        self._gateway = gateway

    async def _connect_and_listen(self) -> None:
        await self.stop()
        self._ws = await websockets.connect(
            BYBIT_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._ws.send(json.dumps({"op": "subscribe", "args": ["position"]}))
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            if payload.get("op") == "ping":
                self._mark_live()
                await self._ws.send(json.dumps({"op": "pong"}))
                continue
            if payload.get("topic") != "position":
                continue
            data = payload.get("data") or []
            self._mark_live()
            for item in data:
                symbol = str(item.get("symbol") or "")
                side_raw = str(item.get("side") or "").lower()
                size = _safe_float(item.get("size")) or 0.0
                qty = size
                if self._contract_size:
                    qty = size * self._contract_size
                side = "long" if side_raw in ("buy", "long") else "short"
                self._update_position(symbol, side, qty)

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        if not api_key or not api_secret:
            logger.warning("bybit ws auth missing api key/secret")
            return False
        expires = int(time.time() * 1000) + 5000
        sign_payload = f"GET/realtime{expires}"
        signature = hmac.new(
            api_secret.encode("utf-8"),
            sign_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        await self._ws.send(json.dumps({"op": "auth", "args": [api_key, expires, signature]}))
        deadline = time.time() + 5
        while time.time() < deadline:
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, dict) and payload.get("op") == "auth":
                if payload.get("success") is True or str(payload.get("retCode")) == "0":
                    return True
                break
        logger.warning("bybit ws auth failed")
        return False


class OkxPositionStream(_BasePositionStream):
    def __init__(self, gateway: ExchangeGateway, contract_size: float | None = None) -> None:
        super().__init__("okx", contract_size=contract_size)
        self._gateway = gateway

    async def _connect_and_listen(self) -> None:
        await self.stop()
        self._ws = await websockets.connect(
            OKX_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "positions", "instType": "SWAP"}]}))
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            if payload.get("event"):
                continue
            if payload.get("arg", {}).get("channel") != "positions":
                continue
            data = payload.get("data") or []
            self._mark_live()
            for item in data:
                symbol = str(item.get("instId") or "")
                pos_side = str(item.get("posSide") or "").lower()
                pos = _safe_float(item.get("pos"))
                if pos is None:
                    continue
                qty = pos
                if self._contract_size:
                    qty = pos * self._contract_size
                if pos_side == "net":
                    side = "long" if pos >= 0 else "short"
                    self._update_position(symbol, side, abs(qty))
                else:
                    self._update_position(symbol, pos_side, abs(qty))

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        passphrase = self._gateway.password
        if not api_key or not api_secret or not passphrase:
            logger.warning("okx ws auth missing api key/secret/passphrase")
            return False
        timestamp = str(time.time())
        prehash = f"{timestamp}GET/users/self/verify"
        signature = base64.b64encode(
            hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
        await self._ws.send(
            json.dumps(
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
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, dict) and payload.get("event") == "login":
                if payload.get("code") == "0":
                    return True
                break
        logger.warning("okx ws auth failed")
        return False


class GatePositionStream(_BasePositionStream):
    def __init__(self, gateway: ExchangeGateway, contract_size: float | None = None) -> None:
        super().__init__("gate", contract_size=contract_size)
        self._gateway = gateway
        self._server_time_offset: float | None = None
        self._api_key: str | None = None
        self._api_secret: str | None = None

    async def _connect_and_listen(self) -> None:
        await self.stop()
        self._ws = await websockets.connect(
            GATE_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._subscribe()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            self._ingest_server_time(payload)
            if payload.get("event") == "subscribe":
                result = payload.get("result") or {}
                if result.get("status") in (None, "", "success"):
                    self._mark_live()
                continue
            if payload.get("channel") != "futures.positions":
                continue
            result = payload.get("result") or []
            self._mark_live()
            for item in result:
                symbol = str(item.get("contract") or "")
                size = _safe_float(item.get("size"))
                if size is None:
                    continue
                qty = size
                if self._contract_size:
                    qty = size * self._contract_size
                if size >= 0:
                    self._update_position(symbol, "long", abs(qty))
                else:
                    self._update_position(symbol, "short", abs(qty))

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        self._api_key = self._gateway.api_key
        self._api_secret = self._gateway.api_secret
        if not self._api_key or not self._api_secret:
            logger.warning("gate ws auth missing api key/secret")
            return False
        timestamp = str(self._current_server_time())
        sign_payload = f"api\nfutures.login\n\n{timestamp}"
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            sign_payload.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()
        payload = {
            "time": int(timestamp),
            "channel": "futures.login",
            "event": "api",
            "payload": {
                "api_key": self._api_key,
                "signature": signature,
                "timestamp": timestamp,
                "req_id": f"{int(time.time() * 1000)}-login",
                "request_param": "",
                "headers": {},
            },
        }
        await self._ws.send(json.dumps(payload))
        return True

    async def _subscribe(self) -> None:
        if not self._api_key or not self._api_secret:
            return
        timestamp = self._current_server_time()
        payload = _gate_sign_auth_message(
            channel="futures.positions",
            event="subscribe",
            timestamp=timestamp,
            payload=["!all"],
            api_key=self._api_key,
            api_secret=self._api_secret,
        )
        await self._ws.send(json.dumps(payload))

    def _current_server_time(self) -> int:
        if self._server_time_offset is None:
            return int(time.time())
        return int(time.time() + self._server_time_offset)

    def _ingest_server_time(self, payload: Mapping[str, Any]) -> None:
        server_time_ms = None
        time_ms = payload.get("time_ms")
        if time_ms is not None:
            try:
                server_time_ms = int(time_ms)
            except (TypeError, ValueError):
                server_time_ms = None
        if server_time_ms is None:
            header = payload.get("header") or {}
            response_time = header.get("response_time")
            if response_time is not None:
                try:
                    server_time_ms = int(response_time)
                except (TypeError, ValueError):
                    server_time_ms = None
        if server_time_ms:
            self._server_time_offset = (server_time_ms / 1000.0) - time.time()


class BitgetPositionStream(_BasePositionStream):
    def __init__(self, gateway: ExchangeGateway, contract_size: float | None = None) -> None:
        super().__init__("bitget", contract_size=contract_size)
        self._gateway = gateway

    async def _connect_and_listen(self) -> None:
        await self.stop()
        self._ws = await websockets.connect(
            BITGET_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._subscribe()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            if text == "ping":
                self._mark_live()
                await self._ws.send("pong")
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            if payload.get("event") in ("login", "subscribe"):
                self._mark_live()
                continue
            if payload.get("arg", {}).get("channel") != "positions":
                continue
            data = payload.get("data") or []
            self._mark_live()
            for item in data:
                symbol = str(item.get("instId") or item.get("symbol") or "")
                side_raw = str(item.get("holdSide") or item.get("posSide") or item.get("side") or "").lower()
                size = _safe_float(item.get("total") or item.get("size") or item.get("availPos"))
                if size is None:
                    continue
                qty = size
                if self._contract_size:
                    qty = size * self._contract_size
                if side_raw in ("long", "buy"):
                    self._update_position(symbol, "long", abs(qty))
                elif side_raw in ("short", "sell"):
                    self._update_position(symbol, "short", abs(qty))

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        passphrase = self._gateway.password
        if not api_key or not api_secret or not passphrase:
            logger.warning("bitget ws auth missing api key/secret/passphrase")
            return False
        timestamp = str(int(time.time()))
        prehash = f"{timestamp}GET/user/verify"
        signature = base64.b64encode(
            hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
        payload = {
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
        await self._ws.send(json.dumps(payload))
        return True

    async def _subscribe(self) -> None:
        payload = {
            "op": "subscribe",
            "args": [
                {"instType": "USDT-FUTURES", "channel": "positions", "instId": "default"}
            ],
        }
        await self._ws.send(json.dumps(payload))


class KucoinPositionStream(_BasePositionStream):
    def __init__(self, gateway: ExchangeGateway, contract_size: float | None = None) -> None:
        super().__init__("kucoin", contract_size=contract_size)
        self._gateway = gateway

    async def _connect_and_listen(self) -> None:
        await self.stop()
        endpoint = await self._fetch_private_endpoint()
        if not endpoint:
            return
        self._ws = await websockets.connect(
            endpoint,
            ping_interval=20,
            ping_timeout=10,
        )
        await self._subscribe()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            if payload.get("type") == "ping":
                self._mark_live()
                await self._ws.send(json.dumps({"id": payload.get("id"), "type": "pong"}))
                continue
            if payload.get("type") != "message":
                continue
            topic = str(payload.get("topic") or "")
            if "/contract/position" not in topic:
                continue
            data = payload.get("data") or {}
            self._mark_live()
            symbol = str(data.get("symbol") or "")
            qty_raw = _safe_float(data.get("currentQty"))
            if qty_raw is None:
                continue
            qty = qty_raw
            if self._contract_size:
                qty = qty_raw * self._contract_size
            if qty_raw >= 0:
                self._update_position(symbol, "long", abs(qty))
            else:
                self._update_position(symbol, "short", abs(qty))

    async def _fetch_private_endpoint(self) -> str | None:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        passphrase = self._gateway.password
        if not api_key or not api_secret or not passphrase:
            logger.warning("kucoin ws auth missing api key/secret/passphrase")
            return None
        try:
            endpoint = await fetch_kucoin_private_ws_endpoint(self._gateway)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("kucoin position ws direct token fetch failed: %s", exc)
            endpoint = None
        if endpoint:
            return endpoint
        await self._gateway.ensure_client()
        client = self._gateway.client
        if not client:
            return None
        try:
            token_info = await client.futuresPrivatePostBulletPrivate()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("kucoin position ws ccxt token fetch failed: %s", exc)
            return None
        data = (token_info or {}).get("data") or {}
        server = data.get("instanceServers", [{}])[0] if data else {}
        endpoint = data.get("endpoint") or server.get("endpoint") if data else None
        token = data.get("token") if data else None
        if not endpoint or not token:
            return None
        return f"{endpoint}?token={token}"

    async def _subscribe(self) -> None:
        payload = {
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": "/contract/positionAll",
            "privateChannel": True,
            "response": True,
        }
        await self._ws.send(json.dumps(payload))


class BingxPositionStream(_BasePositionStream):
    def __init__(self, gateway: ExchangeGateway, contract_size: float | None = None) -> None:
        super().__init__("bingx", contract_size=contract_size)
        self._gateway = gateway
        self._listen_key: Optional[str] = None
        self._keepalive_task: Optional[asyncio.Task] = None

    async def _connect_and_listen(self) -> None:
        await self.stop()
        listen_key = await self._fetch_listen_key()
        if not listen_key:
            return
        self._listen_key = listen_key
        ws_url = f"{BINGX_SWAP_WS_URL}?listenKey={listen_key}"
        self._ws = await websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=10,
            max_size=1_000_000,
        )
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if _bingx_is_ping_message(text, payload):
                self._mark_live()
                await self._ws.send("Pong")
                continue
            if isinstance(payload, dict) and "pong" in payload:
                self._mark_live()
                continue
            if text.strip().lower() == "pong":
                self._mark_live()
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("e") != "ACCOUNT_UPDATE":
                continue
            data = payload.get("a") or {}
            positions = data.get("P") or []
            self._mark_live()
            for item in positions:
                symbol = str(item.get("s") or "")
                pos_side = str(item.get("ps") or "").lower()
                qty_raw = _safe_float(item.get("pa"))
                if qty_raw is None:
                    continue
                qty = qty_raw
                if self._contract_size:
                    qty = qty_raw * self._contract_size
                if pos_side == "short":
                    self._update_position(symbol, "short", abs(qty))
                else:
                    self._update_position(symbol, "long", abs(qty))

    async def stop(self) -> None:
        await super().stop()
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except Exception:
                pass
            self._keepalive_task = None
        if self._listen_key:
            await self._close_listen_key()
            self._listen_key = None

    async def _fetch_listen_key(self) -> str | None:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        if not api_key:
            logger.warning("bingx listenKey missing api key")
            return None
        headers = {"X-BX-APIKEY": api_key}
        params = {"timestamp": str(int(time.time() * 1000))} if api_secret else {}
        url = "https://open-api.bingx.com/openApi/user/auth/userDataStream"
        if params and api_secret:
            query = "timestamp=" + params["timestamp"]
            signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
            url = f"{url}?{query}&signature={signature}"
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    payload = await resp.json()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("bingx listenKey request failed: %s", exc)
            return None
        listen_key = payload.get("listenKey") or payload.get("data", {}).get("listenKey") or payload.get("data")
        if not listen_key:
            logger.warning("bingx listenKey response missing: %s", payload)
            return None
        return str(listen_key)

    async def _extend_listen_key(self) -> None:
        if not self._listen_key:
            return
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        if not api_key:
            return
        headers = {"X-BX-APIKEY": api_key}
        params = {"listenKey": self._listen_key}
        if api_secret:
            params["timestamp"] = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = "https://open-api.bingx.com/openApi/user/auth/userDataStream"
        if api_secret:
            signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
            url = f"{url}?{query}&signature={signature}"
        else:
            url = f"{url}?{query}"
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.put(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    await resp.text()
        except Exception:
            return

    async def _close_listen_key(self) -> None:
        if not self._listen_key:
            return
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        if not api_key:
            return
        headers = {"X-BX-APIKEY": api_key}
        params = {"listenKey": self._listen_key}
        if api_secret:
            params["timestamp"] = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = "https://open-api.bingx.com/openApi/user/auth/userDataStream"
        if api_secret:
            signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
            url = f"{url}?{query}&signature={signature}"
        else:
            url = f"{url}?{query}"
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    await resp.text()
        except Exception:
            return

    async def _keepalive_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(30 * 60)
            if self._stop.is_set():
                break
            await self._extend_listen_key()


class LivePositionTracker:
    def __init__(self) -> None:
        self._streams: dict[str, _BasePositionStream] = {}
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}

    def set_contract_sizes(self, contract_sizes: dict[str, float | None]) -> None:
        for exchange, size in contract_sizes.items():
            stream = self._streams.get(exchange)
            if stream:
                stream.set_contract_size(size)

    async def ensure(self, exchanges: list[str]) -> None:
        for exchange in exchanges:
            if exchange in self._streams:
                await self._streams[exchange].start()
                continue
            gateway = self._gateways.get(exchange)
            if gateway is None:
                continue
            stream = self._build_stream(exchange, gateway)
            if stream is None:
                continue
            self._streams[exchange] = stream
            await stream.start()

    def is_live(self, exchange: str, *, stale_after: float) -> bool:
        stream = self._streams.get(exchange)
        if not stream:
            return False
        return stream.is_live(stale_after=stale_after)

    def get_positions(self, exchange: str, symbol: str | None = None) -> list[dict[str, Any]]:
        stream = self._streams.get(exchange)
        if not stream:
            return []
        return stream.get_positions(symbol)

    async def close(self) -> None:
        for stream in list(self._streams.values()):
            await stream.stop()
        self._streams.clear()

    def _build_stream(self, exchange: str, gateway: ExchangeGateway) -> _BasePositionStream | None:
        if exchange == "bybit":
            return BybitPositionStream(gateway)
        if exchange == "okx":
            return OkxPositionStream(gateway)
        if exchange == "gate":
            return GatePositionStream(gateway)
        if exchange == "bitget":
            return BitgetPositionStream(gateway)
        if exchange == "kucoin":
            return KucoinPositionStream(gateway)
        if exchange == "bingx":
            return BingxPositionStream(gateway)
        return None
