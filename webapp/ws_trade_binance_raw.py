from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import aiohttp
import websockets
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect, WebSocketState

from config import BASE_DIR
from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _bootstrap_env

BINANCE_LISTEN_KEY_URL = "https://fapi.binance.com/fapi/v1/listenKey"
BINANCE_PRIVATE_WS_URL = "wss://fstream.binance.com/ws"
BINANCE_LISTEN_KEY_RENEW_SECONDS = 1500

logger = logging.getLogger(__name__)
_RAW_LOGGER = logging.getLogger("ws_trade_binance_raw")
if not _RAW_LOGGER.handlers:
    _RAW_LOGGER.setLevel(logging.INFO)
    log_path = BASE_DIR / "logs" / "ws_trade_binance_raw.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _RAW_LOGGER.addHandler(handler)
    _RAW_LOGGER.propagate = False


def _binance_gateway() -> ExchangeGateway:
    for spec in EXCHANGE_SPECS:
        if spec.slug == "binance":
            return ExchangeGateway(spec)
    raise RuntimeError("binance spec not found")


class WsTradeBinanceRawStream:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._remote_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._listen_key: Optional[str] = None

    async def run(self) -> None:
        try:
            if self._websocket.application_state == WebSocketState.CONNECTING:
                await self._websocket.accept()
            while True:
                if (
                    self._websocket.application_state != WebSocketState.CONNECTED
                    or self._websocket.client_state != WebSocketState.CONNECTED
                ):
                    break
                try:
                    message = await self._websocket.receive_json()
                except (WebSocketDisconnect, RuntimeError):
                    break
                action = str(message.get("action") or "")
                if action == "connect":
                    await self._connect()
                elif action in ("extend_listen_key", "extend"):
                    await self._extend_listen_key()
                elif action in ("close_listen_key", "close_listen"):
                    await self._close_listen_key()
                elif action == "ping":
                    await self._send_ping()
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
                if asyncio.current_task() is not self._keepalive_task:
                    await self._keepalive_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._keepalive_task = None
        if self._reader_task:
            self._reader_task.cancel()
            try:
                if asyncio.current_task() is not self._reader_task:
                    await self._reader_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._reader_task = None
        if self._reconnect_task and self._reconnect_task.done():
            self._reconnect_task = None
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
        async with self._connect_lock:
            await self._shutdown()
            self._stop.clear()
            listen_key = await self._fetch_listen_key()
            if not listen_key:
                await self._log_line("[err] binance listenKey missing")
                return
            self._listen_key = listen_key
            ws_url = f"{BINANCE_PRIVATE_WS_URL}/{listen_key}"
            try:
                self._remote_ws = await websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=1_000_000,
                )
            except Exception as exc:  # pylint: disable=broad-except
                await self._log_line(f"[err] connect failed: {exc}")
                return
            self._reader_task = asyncio.create_task(self._reader_loop())
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())
            await self._log_line("[sys] connected to binance futures user stream (listenKey)")
            await self._log_line(f"[sys] binance ws url: {ws_url}")

    def _request_reconnect(self, reason: str) -> None:
        if self._stop.is_set():
            return
        if self._reconnect_task and not self._reconnect_task.done():
            return

        async def _runner() -> None:
            await self._log_line(f"[sys] binance reconnect requested ({reason})")
            await self._connect()

        self._reconnect_task = asyncio.create_task(_runner())

    async def _send_ping(self) -> None:
        if not self._remote_ws:
            await self._log_line("[err] not connected")
            return
        try:
            await self._remote_ws.ping()
            await self._log_line("[tx] ping")
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] ping failed: {exc}")

    async def _fetch_listen_key(self) -> str | None:
        _bootstrap_env(force=True)
        gateway = _binance_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        if not api_key:
            await self._log_line("[err] binance api key missing")
            return None
        headers = {"X-MBX-APIKEY": api_key}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    BINANCE_LISTEN_KEY_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    payload = await resp.json()
            except Exception as exc:  # pylint: disable=broad-except
                await self._log_line(f"[err] listenKey request failed: {exc}")
                return None
        listen_key = payload.get("listenKey") if isinstance(payload, dict) else None
        if not listen_key:
            await self._log_line(f"[err] listenKey response missing: {payload}")
            return None
        await self._log_line("[sys] binance listenKey ok")
        return str(listen_key)

    async def _extend_listen_key(self) -> None:
        if not self._listen_key:
            await self._log_line("[err] listenKey missing")
            return
        _bootstrap_env(force=True)
        gateway = _binance_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        if not api_key:
            await self._log_line("[err] binance api key missing")
            return
        headers = {"X-MBX-APIKEY": api_key}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.put(
                    BINANCE_LISTEN_KEY_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    payload = await resp.json()
            except Exception as exc:  # pylint: disable=broad-except
                await self._log_line(f"[err] listenKey extend failed: {exc}")
                return
        await self._log_line(f"[sys] listenKey extended: {payload}")

    async def _close_listen_key(self) -> None:
        if not self._listen_key:
            return
        _bootstrap_env(force=True)
        gateway = _binance_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        if not api_key:
            await self._log_line("[err] binance api key missing")
            return
        headers = {"X-MBX-APIKEY": api_key}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.delete(
                    BINANCE_LISTEN_KEY_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    payload = await resp.json()
            except Exception as exc:  # pylint: disable=broad-except
                await self._log_line(f"[err] listenKey close failed: {exc}")
                return
        await self._log_line(f"[sys] listenKey closed: {payload}")

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

    async def _reader_loop(self) -> None:
        if not self._remote_ws:
            return
        while not self._stop.is_set():
            try:
                message = await self._remote_ws.recv()
            except asyncio.CancelledError:
                break
            except Exception:
                await self._log_line("[sys] remote ws closed")
                break
            if isinstance(message, bytes):
                try:
                    text = message.decode("utf-8")
                except Exception:
                    await self._log_line("[rx-binary] <binary>")
                    continue
            else:
                text = str(message)
            await self._log_line(f"[rx] {text}")
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, dict) and payload.get("e") == "listenKeyExpired":
                self._request_reconnect("listenKeyExpired")

    async def _keepalive_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(BINANCE_LISTEN_KEY_RENEW_SECONDS)
            if self._stop.is_set():
                break
            await self._extend_listen_key()
