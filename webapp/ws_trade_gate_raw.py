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

GATE_PRIVATE_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"

logger = logging.getLogger(__name__)
_RAW_LOGGER = logging.getLogger("ws_trade_gate_raw")
if not _RAW_LOGGER.handlers:
    _RAW_LOGGER.setLevel(logging.INFO)
    log_path = BASE_DIR / "logs" / "ws_trade_gate_raw.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _RAW_LOGGER.addHandler(handler)
    _RAW_LOGGER.propagate = False


def _gate_gateway() -> ExchangeGateway:
    for spec in EXCHANGE_SPECS:
        if spec.slug == "gate":
            return ExchangeGateway(spec)
    raise RuntimeError("gate spec not found")


def _sign_gate_auth_message(
    *,
    channel: str,
    event: str,
    timestamp: int,
    payload: object,
    api_key: str,
    api_secret: str,
) -> dict:
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


def _sign_gate_api_signature(
    *,
    event: str,
    channel: str,
    timestamp: int,
    req_param: object,
    api_secret: str,
) -> str:
    if req_param is None:
        req_param_text = ""
    elif isinstance(req_param, str):
        req_param_text = req_param
    else:
        req_param_text = json.dumps(req_param, separators=(",", ":"), ensure_ascii=False)
    signature_payload = f"{event}\n{channel}\n{req_param_text}\n{timestamp}"
    return hmac.new(
        api_secret.encode("utf-8"),
        signature_payload.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()


class WsTradeGateRawStream:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        self._remote_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._server_time_offset: float | None = None
        self._login_attempts = 0

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
                GATE_PRIVATE_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            )
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] connect failed: {exc}")
            return
        self._reader_task = asyncio.create_task(self._reader_loop())
        await self._log_line("[sys] connected to gate private ws")
        await self._login_gate()

    def _current_server_time(self) -> int:
        if self._server_time_offset is None:
            return int(time.time())
        return int(time.time() + self._server_time_offset)

    async def _login_gate(self) -> None:
        if not self._remote_ws:
            return
        _bootstrap_env(force=True)
        gateway = _gate_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        if not api_key or not api_secret:
            await self._log_line("[err] gate api key/secret missing")
            return
        timestamp = self._current_server_time()
        signature = _sign_gate_api_signature(
            event="api",
            channel="futures.login",
            timestamp=timestamp,
            req_param="",
            api_secret=api_secret,
        )
        req_id = f"{int(time.time() * 1000)}-1"
        payload = {
            "time": timestamp,
            "channel": "futures.login",
            "event": "api",
            "payload": {
                "api_key": api_key,
                "signature": signature,
                "timestamp": str(timestamp),
                "req_id": req_id,
                "request_param": "",
                "headers": {},
            },
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
            text = await self._prepare_payload(payload)
        else:
            await self._log_line("[err] missing raw payload")
            return
        try:
            await self._remote_ws.send(text)
        except Exception as exc:  # pylint: disable=broad-except
            await self._log_line(f"[err] send failed: {exc}")
            return
        await self._log_line(f"[tx] {text}")

    async def _prepare_payload(self, payload: object) -> str:
        if not isinstance(payload, dict):
            return json.dumps(payload)
        if "auth" in payload:
            return json.dumps(payload)
        channel = payload.get("channel")
        event = payload.get("event")
        if not channel or not event:
            return json.dumps(payload)
        _bootstrap_env(force=True)
        gateway = _gate_gateway()
        await gateway.refresh_credentials_async(force_env=True)
        api_key = gateway.api_key
        api_secret = gateway.api_secret
        if not api_key or not api_secret:
            return json.dumps(payload)
        timestamp = self._current_server_time()
        if str(event) == "api":
            req_payload = payload.get("payload")
            if not isinstance(req_payload, dict):
                req_payload = {}
            req_param = req_payload.get("req_param")
            req_timestamp = req_payload.get("timestamp")
            if req_payload.get("api_key") and req_payload.get("signature") and req_timestamp:
                try:
                    timestamp = int(req_timestamp)
                except (TypeError, ValueError):
                    timestamp = self._current_server_time()
                signed_payload = dict(req_payload)
            else:
                signature = _sign_gate_api_signature(
                    event=str(event),
                    channel=str(channel),
                    timestamp=timestamp,
                    req_param=req_param,
                    api_secret=api_secret,
                )
                signed_payload = dict(req_payload)
                signed_payload.setdefault("req_id", f"{int(time.time() * 1000)}-1")
                signed_payload["api_key"] = api_key
                signed_payload["signature"] = signature
                signed_payload["timestamp"] = str(timestamp)
                signed_payload.setdefault("headers", {})
            signed = {
                "time": timestamp,
                "channel": str(channel),
                "event": str(event),
                "payload": signed_payload,
            }
            if "id" in payload:
                signed["id"] = payload["id"]
            return json.dumps(signed)
        signed = _sign_gate_auth_message(
            channel=str(channel),
            event=str(event),
            timestamp=timestamp,
            payload=payload.get("payload"),
            api_key=api_key,
            api_secret=api_secret,
        )
        if "id" in payload:
            signed["id"] = payload["id"]
        return json.dumps(signed)

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
            self._ingest_server_time(text)
            await self._maybe_retry_login(text)

    def _ingest_server_time(self, text: str) -> None:
        try:
            payload = json.loads(text)
        except Exception:
            return
        server_time_ms = None
        if isinstance(payload, dict):
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

    async def _maybe_retry_login(self, text: str) -> None:
        try:
            payload = json.loads(text)
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        header = payload.get("header") or {}
        channel = header.get("channel") or payload.get("channel")
        event = header.get("event") or payload.get("event")
        if channel != "futures.login" or event != "api":
            return
        data = payload.get("data") or {}
        errs = data.get("errs") or {}
        message = str(errs.get("message") or "")
        if "Timestamp" not in message:
            return
        if self._login_attempts >= 2:
            return
        self._login_attempts += 1
        await self._login_gate()
