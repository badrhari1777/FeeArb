from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Optional

import aiohttp
import websockets
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from config import BASE_DIR
from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _bootstrap_env

KUCOIN_PRIVATE_REST_URL = "https://api-futures.kucoin.com/api/v1/bullet-private"
KUCOIN_SERVER_TIME_URL = "https://api-futures.kucoin.com/api/v1/timestamp"

logger = logging.getLogger(__name__)
_RAW_LOGGER = logging.getLogger("ws_trade_kucoin_raw")
if not _RAW_LOGGER.handlers:
    _RAW_LOGGER.setLevel(logging.INFO)
    log_path = BASE_DIR / "logs" / "ws_trade_kucoin_raw.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _RAW_LOGGER.addHandler(handler)
    _RAW_LOGGER.propagate = False


def _kucoin_gateway() -> ExchangeGateway:
    for spec in EXCHANGE_SPECS:
        if spec.slug == "kucoin":
            return ExchangeGateway(spec)
    raise RuntimeError("kucoin spec not found")


def _sign_kucoin(secret: str, payload: str) -> str:
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")


def _is_kucoin_timestamp_error(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    code = str(payload.get("code") or "").strip()
    message = str(payload.get("msg") or payload.get("message") or "").lower()
    return code == "400002" or "kc-api-timestamp" in message


class WsTradeKucoinRawStream:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._remote_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._ping_interval = 15.0

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
        endpoint = await self._fetch_private_endpoint()
        if not endpoint:
            return
        try:
            self._remote_ws = await websockets.connect(
                endpoint,
                ping_interval=None,
                ping_timeout=20,
                max_size=1_000_000,
            )
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] connect failed: {exc}")
            return
        self._reader_task = asyncio.create_task(self._reader_loop())
        await self._log_line("[sys] connected to kucoin private ws")

    async def _fetch_private_endpoint(self) -> str | None:
        _bootstrap_env(force=True)
        gateway = _kucoin_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        passphrase = gateway.password
        if not api_key or not api_secret or not passphrase:
            await self._log_line("[err] kucoin api key/secret/passphrase missing")
            return None
        timestamp = str(int(time.time() * 1000))
        options = getattr(gateway, "options", None) or getattr(gateway.spec, "options", None) or {}
        key_version = str(
            os.getenv("KUCOIN_API_KEY_VERSION")
            or options.get("keyVersion")
            or options.get("key_version")
            or "2"
        )
        passphrase_value = passphrase
        if key_version == "2":
            passphrase_value = _sign_kucoin(api_secret, passphrase)
        try:
            async with aiohttp.ClientSession() as session:
                server_timestamp = await self._fetch_server_timestamp(session)
                payload = await self._post_private_bullet(
                    session,
                    api_key=api_key,
                    api_secret=api_secret,
                    passphrase_value=passphrase_value,
                    key_version=key_version,
                    timestamp=server_timestamp or timestamp,
                )
                if _is_kucoin_timestamp_error(payload):
                    retry_timestamp = await self._fetch_server_timestamp(session)
                    if retry_timestamp:
                        payload = await self._post_private_bullet(
                            session,
                            api_key=api_key,
                            api_secret=api_secret,
                            passphrase_value=passphrase_value,
                            key_version=key_version,
                            timestamp=retry_timestamp,
                        )
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] bullet-private failed: {exc}")
            return None
        data = payload.get("data") or {}
        servers = data.get("instanceServers") or []
        endpoint = data.get("endpoint") or (servers[0].get("endpoint") if servers else None)
        token = data.get("token")
        if servers and "pingInterval" in servers[0]:
            self._ping_interval = float(servers[0].get("pingInterval", 15000)) / 1000.0
        if not endpoint or not token:
            await self._log_line(f"[err] bullet-private response: {payload}")
            return None
        return f"{endpoint}?token={token}"

    async def _fetch_server_timestamp(self, session: aiohttp.ClientSession) -> str | None:
        try:
            async with session.get(
                KUCOIN_SERVER_TIME_URL,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                payload = await resp.json()
        except Exception:
            return None
        raw = payload.get("data") if isinstance(payload, dict) else None
        try:
            value = int(float(raw))
        except Exception:
            return None
        if value <= 0:
            return None
        if value < 1_000_000_000_000:
            value *= 1000
        return str(value)

    async def _post_private_bullet(
        self,
        session: aiohttp.ClientSession,
        *,
        api_key: str,
        api_secret: str,
        passphrase_value: str,
        key_version: str,
        timestamp: str,
    ) -> dict:
        method = "POST"
        path = "/api/v1/bullet-private"
        prehash = f"{timestamp}{method}{path}"
        signature = _sign_kucoin(api_secret, prehash)
        headers = {
            "KC-API-KEY": api_key,
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": timestamp,
            "KC-API-PASSPHRASE": passphrase_value,
            "KC-API-KEY-VERSION": key_version,
        }
        async with session.post(
            KUCOIN_PRIVATE_REST_URL,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            return await resp.json()

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
        last_ping = time.time()
        while not self._stop.is_set():
            if time.time() - last_ping > self._ping_interval:
                try:
                    await self._remote_ws.send(json.dumps({"id": "ping", "type": "ping"}))
                    await self._log_line('[tx] {"id":"ping","type":"ping"}')
                except Exception:
                    pass
                last_ping = time.time()
            try:
                message = await asyncio.wait_for(self._remote_ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
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
