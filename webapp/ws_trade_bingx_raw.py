from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
import zlib
from typing import Optional
from urllib.parse import urlencode

import aiohttp
import websockets
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from config import BASE_DIR
from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _bootstrap_env

BINGX_LISTEN_KEY_URL = "https://open-api.bingx.com/openApi/user/auth/userDataStream"
BINGX_PRIVATE_WS_URL = "wss://open-api-swap.bingx.com/swap-market"
BINGX_LISTEN_KEY_RENEW_SECONDS = 30 * 60

logger = logging.getLogger(__name__)
_RAW_LOGGER = logging.getLogger("ws_trade_bingx_raw")
if not _RAW_LOGGER.handlers:
    _RAW_LOGGER.setLevel(logging.INFO)
    log_path = BASE_DIR / "logs" / "ws_trade_bingx_raw.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _RAW_LOGGER.addHandler(handler)
    _RAW_LOGGER.propagate = False


def _bingx_gateway() -> ExchangeGateway:
    for spec in EXCHANGE_SPECS:
        if spec.slug == "bingx":
            return ExchangeGateway(spec)
    raise RuntimeError("bingx spec not found")


def _decode_message(message: object) -> str | None:
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


class WsTradeBingxRawStream:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._remote_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._listen_key: Optional[str] = None
        self._connected_url: Optional[str] = None

    async def run(self) -> None:
        try:
            while True:
                message = await self._websocket.receive_json()
                action = str(message.get("action") or "")
                if action == "connect":
                    await self._connect()
                elif action in ("extend_listen_key", "extend"):
                    await self._extend_listen_key()
                elif action in ("close_listen_key", "close_listen"):
                    await self._close_listen_key()
                elif action == "send":
                    await self._send_raw(message)
                elif action in ("disconnect", "close"):
                    break
        except WebSocketDisconnect:
            pass
        finally:
            await self._shutdown()

    async def _log_line(self, line: str) -> None:
        try:
            _RAW_LOGGER.info(line)
        except Exception:
            pass
        try:
            await self._websocket.send_text(line)
        except Exception:
            return

    async def _shutdown(self) -> None:
        self._stop.set()
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._keepalive_task = None
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._reader_task = None
        if self._remote_ws:
            try:
                await self._remote_ws.close()
            except Exception:
                pass
            self._remote_ws = None
        if self._listen_key:
            await self._close_listen_key()
            self._listen_key = None

    async def _connect(self) -> None:
        await self._shutdown()
        self._stop.clear()
        listen_key = await self._fetch_listen_key()
        if not listen_key:
            await self._log_line("[err] bingx listenKey missing")
            return
        self._listen_key = listen_key
        ws_url = f"{BINGX_PRIVATE_WS_URL}?listenKey={listen_key}"
        try:
            self._remote_ws = await websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                max_size=1_000_000,
            )
            self._connected_url = ws_url
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] connect failed: {exc}")
            return
        self._reader_task = asyncio.create_task(self._reader_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        await self._log_line("[sys] connected to bingx swap user stream (listenKey)")
        if self._connected_url:
            await self._log_line(f"[sys] bingx ws url: {self._connected_url}")
        await self._log_line(
            "[sys] bingx user stream: no subscribe required "
            "(ACCOUNT_UPDATE / ORDER_TRADE_UPDATE / ACCOUNT_CONFIG_UPDATE)"
        )

    async def _fetch_listen_key(self) -> str | None:
        _bootstrap_env(force=True)
        gateway = _bingx_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        if not api_key:
            await self._log_line("[err] bingx api key missing")
            return None
        headers = {"X-BX-APIKEY": api_key}
        url = self._build_signed_url(
            BINGX_LISTEN_KEY_URL,
            {"timestamp": str(int(time.time() * 1000))} if api_secret else {},
            api_secret,
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    payload = await resp.json()
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] listenKey request failed: {exc}")
            return None
        listen_key = (
            payload.get("listenKey")
            or payload.get("data", {}).get("listenKey")
            or payload.get("data")
        )
        if not listen_key:
            await self._log_line(f"[err] listenKey response: {payload}")
            return None
        await self._log_line(f"[sys] bingx listenKey: {self._mask_token(str(listen_key))}")
        return str(listen_key)

    async def _send_raw(self, message: dict) -> None:
        if not self._remote_ws:
            await self._log_line("[err] not connected")
            return
        raw = message.get("raw")
        payload = message.get("payload")
        if raw:
            text = str(raw)
        elif payload is not None:
            text = json.dumps(payload)
        else:
            await self._log_line("[err] missing raw payload")
            return
        try:
            await self._remote_ws.send(text)
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] send failed: {exc}")
            return
        await self._log_line(f"[tx] {text}")

    async def _keepalive_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(BINGX_LISTEN_KEY_RENEW_SECONDS)
            except asyncio.CancelledError:
                break
            if self._stop.is_set():
                break
            await self._extend_listen_key()

    async def _extend_listen_key(self) -> None:
        if not self._listen_key:
            await self._log_line("[err] listenKey missing (extend)")
            return
        ok = await self._listen_key_request("put", self._listen_key)
        if ok:
            await self._log_line("[sys] bingx listenKey extended")

    async def _close_listen_key(self) -> None:
        if not self._listen_key:
            return
        ok = await self._listen_key_request("delete", self._listen_key)
        if ok:
            await self._log_line("[sys] bingx listenKey closed")
            self._listen_key = None
            if self._keepalive_task:
                self._keepalive_task.cancel()
                try:
                    await self._keepalive_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                self._keepalive_task = None

    async def _listen_key_request(self, method: str, listen_key: str) -> bool:
        _bootstrap_env(force=True)
        gateway = _bingx_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        if not api_key:
            await self._log_line("[err] bingx api key missing")
            return False
        params = {"listenKey": listen_key}
        if api_secret:
            params["timestamp"] = str(int(time.time() * 1000))
        url = self._build_signed_url(BINGX_LISTEN_KEY_URL, params, api_secret)
        headers = {"X-BX-APIKEY": api_key}
        try:
            async with aiohttp.ClientSession() as session:
                request = session.request
                async with request(method.upper(), url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    status = resp.status
                    if status in (200, 204):
                        return True
                    payload = await resp.text()
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] listenKey {method} failed: {exc}")
            return False
        await self._log_line(f"[err] listenKey {method} status {status}: {payload}")
        return False

    @staticmethod
    def _build_signed_url(base_url: str, params: dict, api_secret: str | None) -> str:
        if not params:
            return base_url
        query = urlencode(params)
        if api_secret:
            signature = hmac.new(
                api_secret.encode("utf-8"),
                query.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            query = f"{query}&signature={signature}"
        return f"{base_url}?{query}"

    @staticmethod
    def _mask_token(value: str) -> str:
        if len(value) <= 8:
            return value
        return f"{value[:4]}...{value[-4:]}"

    def _is_ping_message(self, text: str, payload: object) -> bool:
        if isinstance(payload, dict) and "ping" in payload:
            return True
        stripped = text.strip().lower()
        if stripped == "ping":
            return True
        if '"ping"' in stripped and '"pong"' not in stripped:
            return True
        return False

    async def _send_pong(self) -> None:
        if not self._remote_ws:
            return
        try:
            await self._remote_ws.send("Pong")
            await self._log_line("[tx] Pong")
        except Exception:
            pass

    async def _reader_loop(self) -> None:
        if not self._remote_ws:
            return
        while not self._stop.is_set():
            try:
                message = await self._remote_ws.recv()
            except Exception:
                await self._log_line("[sys] remote ws closed")
                break
            text = _decode_message(message)
            if not text:
                continue
            await self._log_line(f"[rx] {text}")
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if self._is_ping_message(text, payload):
                await self._send_pong()
                continue
