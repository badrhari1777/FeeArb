from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from typing import Optional

import websockets
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from config import BASE_DIR
from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _bootstrap_env

BYBIT_PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private"

logger = logging.getLogger(__name__)
_RAW_LOGGER = logging.getLogger("ws_trade_private_raw")
if not _RAW_LOGGER.handlers:
    _RAW_LOGGER.setLevel(logging.INFO)
    log_path = BASE_DIR / "logs" / "ws_trade_private_raw.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _RAW_LOGGER.addHandler(handler)
    _RAW_LOGGER.propagate = False


def _bybit_gateway() -> ExchangeGateway:
    for spec in EXCHANGE_SPECS:
        if spec.slug == "bybit":
            return ExchangeGateway(spec)
    raise RuntimeError("bybit spec not found")


class WsTradePrivateRawStream:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._remote_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def run(self) -> None:
        try:
            while True:
                message = await self._websocket.receive_json()
                action = str(message.get("action") or "")
                if action == "connect":
                    await self._connect()
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

    async def _connect(self) -> None:
        await self._shutdown()
        self._stop.clear()
        try:
            self._remote_ws = await websockets.connect(
                BYBIT_PRIVATE_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            )
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] connect failed: {exc}")
            return
        self._reader_task = asyncio.create_task(self._reader_loop())
        await self._log_line("[sys] connected to bybit private ws")
        await self._auth_bybit()

    async def _auth_bybit(self) -> None:
        if not self._remote_ws:
            return
        _bootstrap_env(force=True)
        gateway = _bybit_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        if not api_key or not api_secret:
            await self._log_line("[err] bybit api key/secret missing")
            return
        expires = int(time.time() * 1000) + 5000
        sign_payload = f"GET/realtime{expires}"
        signature = hmac.new(
            api_secret.encode("utf-8"),
            sign_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        payload = {"op": "auth", "args": [api_key, expires, signature]}
        try:
            await self._remote_ws.send(json.dumps(payload))
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] auth send failed: {exc}")
            return
        await self._log_line(f"[tx] {json.dumps(payload)}")

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
            raw_text = None
            raw_b64 = None
            if isinstance(message, bytes):
                try:
                    raw_text = message.decode("utf-8")
                except Exception:
                    raw_b64 = base64.b64encode(message).decode("ascii")
            elif isinstance(message, str):
                raw_text = message
            if raw_text is not None:
                await self._log_line(f"[rx] {raw_text}")
            elif raw_b64 is not None:
                await self._log_line(f"[rx-b64] {raw_b64}")
            if raw_text:
                try:
                    payload = json.loads(raw_text)
                except Exception:
                    payload = None
                if isinstance(payload, dict) and payload.get("op") == "ping":
                    pong = {"op": "pong"}
                    try:
                        await self._remote_ws.send(json.dumps(pong))
                        await self._log_line(f"[tx] {json.dumps(pong)}")
                    except Exception:
                        pass
