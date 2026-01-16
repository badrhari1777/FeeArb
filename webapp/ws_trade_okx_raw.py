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

OKX_PRIVATE_WS_URL = "wss://ws.okx.com:8443/ws/v5/private"

logger = logging.getLogger(__name__)
_RAW_LOGGER = logging.getLogger("ws_trade_okx_raw")
if not _RAW_LOGGER.handlers:
    _RAW_LOGGER.setLevel(logging.INFO)
    log_path = BASE_DIR / "logs" / "ws_trade_okx_raw.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _RAW_LOGGER.addHandler(handler)
    _RAW_LOGGER.propagate = False


def _okx_gateway() -> ExchangeGateway:
    for spec in EXCHANGE_SPECS:
        if spec.slug == "okx":
            return ExchangeGateway(spec)
    raise RuntimeError("okx spec not found")


class WsTradeOkxRawStream:
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
                OKX_PRIVATE_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            )
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] connect failed: {exc}")
            return
        self._reader_task = asyncio.create_task(self._reader_loop())
        await self._log_line("[sys] connected to okx private ws")
        await self._auth_okx()

    async def _auth_okx(self) -> None:
        if not self._remote_ws:
            return
        _bootstrap_env(force=True)
        gateway = _okx_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        passphrase = gateway.password
        if not api_key or not api_secret or not passphrase:
            await self._log_line("[err] okx api key/secret/passphrase missing")
            return
        timestamp = str(time.time())
        prehash = f"{timestamp}GET/users/self/verify"
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
        try:
            await self._remote_ws.send(json.dumps(payload))
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] login send failed: {exc}")
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
            if isinstance(message, bytes):
                try:
                    text = message.decode("utf-8")
                except Exception:
                    text = base64.b64encode(message).decode("ascii")
                    await self._log_line(f"[rx-b64] {text}")
                    continue
            else:
                text = str(message)
            await self._log_line(f"[rx] {text}")
            if text == "ping":
                try:
                    await self._remote_ws.send("pong")
                    await self._log_line("[tx] pong")
                except Exception:
                    pass
