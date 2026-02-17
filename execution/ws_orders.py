
from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Iterable, Mapping, Optional
import zlib

import websockets

from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _bootstrap_env, _safe_float, normalize_symbol
from exchanges import normalize_exchange_name

logger = logging.getLogger(__name__)

BYBIT_PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private"
BINANCE_PRIVATE_WS_URL = "wss://fstream.binance.com/ws"
OKX_PRIVATE_WS_URL = "wss://ws.okx.com:8443/ws/v5/private"
GATE_PRIVATE_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
BITGET_PRIVATE_WS_URL = "wss://ws.bitget.com/v2/ws/private"
BINGX_SWAP_WS_URL = "wss://open-api-swap.bingx.com/swap-market"
BINANCE_LISTEN_KEY_URL = "https://fapi.binance.com/fapi/v1/listenKey"
BINGX_LISTEN_KEY_RENEW_SECONDS = 120
BINANCE_LISTEN_KEY_RENEW_SECONDS = 1500

DEFAULT_WS_ORDER_HEALTH = {
    "binance": {
        "heartbeat_interval": 15.0,
        "heartbeat_timeout": 45.0,
        "reconnect_attempts": 3,
        "reconnect_grace_sec": 12.0,
    },
    "bybit": {
        "heartbeat_interval": 15.0,
        "heartbeat_timeout": 45.0,
        "reconnect_attempts": 3,
        "reconnect_grace_sec": 12.0,
    },
    "okx": {
        "heartbeat_interval": 15.0,
        "heartbeat_timeout": 45.0,
        "reconnect_attempts": 3,
        "reconnect_grace_sec": 12.0,
    },
    "gate": {
        "heartbeat_interval": 20.0,
        "heartbeat_timeout": 60.0,
        "reconnect_attempts": 3,
        "reconnect_grace_sec": 15.0,
    },
    "bitget": {
        "heartbeat_interval": 15.0,
        "heartbeat_timeout": 45.0,
        "reconnect_attempts": 3,
        "reconnect_grace_sec": 12.0,
    },
    "kucoin": {
        "heartbeat_interval": 15.0,
        "heartbeat_timeout": 45.0,
        "reconnect_attempts": 3,
        "reconnect_grace_sec": 12.0,
    },
    "bingx": {
        "heartbeat_interval": 30.0,
        "heartbeat_timeout": 90.0,
        "reconnect_attempts": 3,
        "reconnect_grace_sec": 20.0,
    },
}

DEFAULT_WS_ORDER_HEALTH_FALLBACK = {
    "heartbeat_interval": 15.0,
    "heartbeat_timeout": 45.0,
    "reconnect_attempts": 3,
    "reconnect_grace_sec": 12.0,
}


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


def _bingx_is_pong_message(text: str, payload: object) -> bool:
    if isinstance(payload, dict) and "pong" in payload:
        return True
    stripped = text.strip().lower()
    if stripped == "pong":
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


def _safe_order_id(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_status(value: Any) -> str:
    text = str(value or "").lower()
    if text in ("filled", "closed", "done", "success"):
        return "filled"
    if text in ("canceled", "cancelled", "canceled_by_user", "cancelled_by_user"):
        return "canceled"
    if text in ("partially_filled", "partial", "part_filled"):
        return "partial"
    if text in ("open", "new", "live", "submitted", "active"):
        return "open"
    if text in ("finished", "finish", "complete"):
        return "finished"
    return text


def _coerce_positive_float(value: Any, fallback: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    if parsed <= 0:
        return fallback
    return parsed


def _coerce_nonnegative_int(value: Any, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    if parsed < 0:
        return fallback
    return parsed


def _normalize_health_config(
    override: Mapping[str, Any] | None,
    default: Mapping[str, Any],
) -> dict[str, float | int]:
    cfg = dict(default)
    if not override:
        return cfg
    cfg["heartbeat_interval"] = _coerce_positive_float(
        override.get("heartbeat_interval"), float(cfg.get("heartbeat_interval", 15.0))
    )
    cfg["heartbeat_timeout"] = _coerce_positive_float(
        override.get("heartbeat_timeout"), float(cfg.get("heartbeat_timeout", 45.0))
    )
    cfg["reconnect_attempts"] = _coerce_nonnegative_int(
        override.get("reconnect_attempts"), int(cfg.get("reconnect_attempts", 3))
    )
    cfg["reconnect_grace_sec"] = _coerce_positive_float(
        override.get("reconnect_grace_sec"), float(cfg.get("reconnect_grace_sec", 12.0))
    )
    return cfg


class _BaseOrderStream:
    def __init__(
        self,
        exchange: str,
        contract_size: float | None = None,
        *,
        health_config: Mapping[str, Any] | None = None,
        event_cb: Optional[callable] = None,
    ) -> None:
        self.exchange = exchange
        self._contract_size = contract_size
        self._orders: dict[str, dict[str, Any]] = {}
        self._last_update = 0.0
        self._last_order_update = 0.0
        self._last_ping = 0.0
        self._last_pong = 0.0
        self._last_live_log = 0.0
        self._started_at = 0.0
        self._event_cb: Optional[callable] = event_cb
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._health_config: dict[str, float | int] = _normalize_health_config(
            health_config, DEFAULT_WS_ORDER_HEALTH_FALLBACK
        )
        self._stop = asyncio.Event()

    def set_contract_size(self, contract_size: float | None) -> None:
        if contract_size and contract_size > 0:
            self._contract_size = contract_size

    def set_event_cb(self, event_cb: Optional[callable]) -> None:
        self._event_cb = event_cb

    def set_health_config(self, health_config: Mapping[str, Any] | None) -> None:
        self._health_config = _normalize_health_config(
            health_config, self._health_config or DEFAULT_WS_ORDER_HEALTH_FALLBACK
        )

    def health_snapshot(self) -> dict[str, Any]:
        now = time.time()
        since_start = (now - self._started_at) if self._started_at else None
        warmup_sec = float(self._health_config.get("reconnect_grace_sec", 12.0) or 0.0)
        warming = bool(since_start is not None and not self._last_update and warmup_sec > 0 and since_start < warmup_sec)
        return {
            "exchange": self.exchange,
            "healthy": self.is_healthy(),
            "warming": warming,
            "since_start_sec": since_start,
            "last_rx_sec": (now - self._last_update) if self._last_update else None,
            "last_order_sec": (now - self._last_order_update) if self._last_order_update else None,
            "last_ping_sec": (now - self._last_ping) if self._last_ping else None,
            "last_pong_sec": (now - self._last_pong) if self._last_pong else None,
        }

    def is_healthy(self, *, stale_after: float | None = None) -> bool:
        if not self._last_update:
            return False
        timeout = stale_after if stale_after is not None else float(
            self._health_config.get("heartbeat_timeout", 45.0)
        )
        if timeout <= 0:
            return True
        return (time.time() - self._last_update) <= timeout

    def _emit_event(self, action: str, data: Mapping[str, Any] | None = None) -> None:
        if not self._event_cb:
            return
        payload = {"exchange": self.exchange, "action": action}
        if data:
            payload.update(data)
        try:
            self._event_cb(payload)
        except Exception:
            return

    def is_live(self, *, stale_after: float) -> bool:
        return self.is_healthy(stale_after=stale_after)

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        entry = self._orders.get(str(order_id))
        if not entry:
            return None
        return dict(entry)

    def _mark_live(self, reason: str | None = None) -> None:
        now = time.time()
        prev = self._last_update
        self._last_update = now
        if reason == "connected":
            self._emit_event("connected", {"since_start_sec": now - self._started_at if self._started_at else None})
        elif reason == "ping":
            gap = (now - prev) if prev else None
            self._emit_event("server_ping", {"silence_sec": gap})
        if self.exchange != "bingx":
            return
        gap = (now - prev) if prev else None
        should_log = reason == "ping" or (gap is not None and gap > 20)
        if should_log and (now - self._last_live_log) >= 15:
            logger.info(
                "bingx order ws heartbeat: reason=%s gap=%.1fs",
                reason or "data",
                gap or 0.0,
            )
            self._last_live_log = now

    def _mark_order_update(self) -> None:
        self._last_order_update = time.time()
        self._mark_live()

    def _record_ping(self) -> None:
        self._last_ping = time.time()

    def _mark_pong(self) -> None:
        now = time.time()
        self._last_pong = now
        self._mark_live("pong")
        rtt = (now - self._last_ping) if self._last_ping else None
        self._emit_event("probe_pong_received", {"rtt_sec": rtt})

    async def ensure_symbols(self, symbols: Iterable[str]) -> None:
        return

    def _apply_contract_size(self, qty: float | None) -> float | None:
        if qty is None:
            return None
        if self._contract_size and self._contract_size > 0:
            return float(qty) * self._contract_size
        return float(qty)

    def _set_filled(self, order_id: str, filled_qty: float | None) -> None:
        if filled_qty is None:
            return
        entry = self._orders.setdefault(order_id, {"order_id": order_id, "filled_qty": 0.0})
        value = self._apply_contract_size(filled_qty)
        if value is None:
            return
        entry["filled_qty"] = max(entry.get("filled_qty") or 0.0, float(value))
        entry["updated_at"] = time.time()
        self._mark_order_update()

    def _add_fill(self, order_id: str, delta_qty: float | None) -> None:
        if delta_qty is None:
            return
        entry = self._orders.setdefault(order_id, {"order_id": order_id, "filled_qty": 0.0})
        value = self._apply_contract_size(delta_qty)
        if value is None:
            return
        entry["filled_qty"] = (entry.get("filled_qty") or 0.0) + float(value)
        entry["updated_at"] = time.time()
        self._mark_order_update()

    def _update_order(
        self,
        order_id: str,
        *,
        symbol: str | None = None,
        side: str | None = None,
        filled_qty: float | None = None,
        status: str | None = None,
        mode: str = "set",
    ) -> None:
        if not order_id:
            return
        entry = self._orders.setdefault(order_id, {"order_id": order_id, "filled_qty": 0.0})
        if symbol:
            entry["symbol"] = normalize_symbol(symbol)
        if side:
            entry["side"] = str(side).lower()
        if filled_qty is not None:
            if mode == "add":
                self._add_fill(order_id, filled_qty)
            else:
                self._set_filled(order_id, filled_qty)
        if status:
            entry["status"] = _normalize_status(status)
        entry["updated_at"] = time.time()
        self._mark_order_update()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._started_at = time.time()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        await self._reset_connection()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
            self._task = None
        self._started_at = 0.0

    async def _reset_connection(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._heartbeat_task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._last_update = 0.0
        self._last_order_update = 0.0
        self._last_ping = 0.0
        self._last_pong = 0.0

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pylint: disable=broad-except
                self._emit_event("connect_error", {"error": str(exc)})
                logger.warning("%s order ws error: %s", self.exchange, exc)
                await asyncio.sleep(2.0)

    def _ensure_heartbeat(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            if self._stop.is_set():
                break
            ws = self._ws
            if not ws:
                continue
            interval = float(self._health_config.get("heartbeat_interval", 15.0))
            timeout = float(self._health_config.get("heartbeat_timeout", 45.0))
            now = time.time()
            if timeout > 0 and self._last_update and (now - self._last_update) > timeout:
                self._emit_event(
                    "heartbeat_timeout",
                    {"silence_sec": now - self._last_update, "timeout_sec": timeout},
                )
                logger.warning(
                    "%s order ws heartbeat timeout; reconnecting (silence=%.1fs last_ping=%.1fs last_pong=%.1fs)",
                    self.exchange,
                    now - self._last_update,
                    (now - self._last_ping) if self._last_ping else -1.0,
                    (now - self._last_pong) if self._last_pong else -1.0,
                )
                try:
                    await ws.close()
                except Exception:
                    pass
                continue
            if interval <= 0:
                continue
            if not self._last_update or (now - self._last_update) >= interval:
                silence = (now - self._last_update) if self._last_update else None
                self._emit_event("probe_ping_sent", {"silence_sec": silence, "interval_sec": interval})
                await self._send_ping()

    async def _send_ping(self) -> None:
        if not self._ws:
            return
        self._record_ping()
        try:
            waiter = self._ws.ping()
        except Exception:
            return
        self._mark_live("ping-send")

        async def _await_pong() -> None:
            try:
                await asyncio.wait_for(waiter, timeout=5)
            except Exception:
                return
            self._mark_pong()

        asyncio.create_task(_await_pong())

    async def force_reconnect(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        await self._reset_connection()

    def emit_control_event(self, action: str, data: Mapping[str, Any] | None = None) -> None:
        self._emit_event(action, data)

    async def _connect_and_listen(self) -> None:
        raise NotImplementedError

class BybitOrderStream(_BaseOrderStream):
    def __init__(
        self,
        gateway: ExchangeGateway,
        contract_size: float | None = None,
        *,
        event_cb: Optional[callable] = None,
    ) -> None:
        super().__init__("bybit", contract_size=contract_size, event_cb=event_cb)
        self._gateway = gateway

    async def _connect_and_listen(self) -> None:
        await self._reset_connection()
        self._emit_event("connect_start")
        self._ws = await websockets.connect(
            BYBIT_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._ws.send(json.dumps({"op": "subscribe", "args": ["order", "execution"]}))
        self._mark_live("connected")
        self._ensure_heartbeat()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            self._mark_live()
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            self._mark_live()
            if payload.get("op") == "pong":
                self._mark_pong()
                continue
            if payload.get("op") == "ping":
                self._mark_live("ping")
                await self._ws.send(json.dumps({"op": "pong"}))
                continue
            topic = payload.get("topic")
            if topic not in ("order", "execution"):
                continue
            data = payload.get("data") or []
            if not isinstance(data, list):
                data = [data]
            if topic == "order":
                for item in data:
                    order_id = _safe_order_id(item.get("orderId") or item.get("order_id"))
                    if not order_id:
                        continue
                    filled = _safe_float(item.get("cumExecQty"))
                    status = item.get("orderStatus") or item.get("order_status") or item.get("status")
                    symbol = item.get("symbol")
                    side = item.get("side")
                    self._update_order(
                        order_id,
                        symbol=symbol,
                        side=side,
                        filled_qty=filled,
                        status=status,
                        mode="set",
                    )
            else:
                for item in data:
                    order_id = _safe_order_id(item.get("orderId") or item.get("order_id"))
                    if not order_id:
                        continue
                    exec_qty = _safe_float(item.get("execQty") or item.get("exec_qty"))
                    symbol = item.get("symbol")
                    side = item.get("side")
                    self._update_order(
                        order_id,
                        symbol=symbol,
                        side=side,
                        filled_qty=exec_qty,
                        status=None,
                        mode="add",
                    )

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        if not api_key or not api_secret:
            self._emit_event("auth_missing")
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
        self._emit_event("auth_failed")
        return False

    async def _send_ping(self) -> None:
        if not self._ws:
            return
        self._record_ping()
        try:
            await self._ws.send(json.dumps({"op": "ping"}))
        except Exception:
            return
        self._mark_live("ping-send")


class BinanceOrderStream(_BaseOrderStream):
    def __init__(
        self,
        gateway: ExchangeGateway,
        contract_size: float | None = None,
        *,
        event_cb: Optional[callable] = None,
    ) -> None:
        super().__init__("binance", contract_size=contract_size, event_cb=event_cb)
        self._gateway = gateway
        self._listen_key: Optional[str] = None
        self._keepalive_task: Optional[asyncio.Task] = None

    async def _connect_and_listen(self) -> None:
        await self._reset_connection()
        self._emit_event("connect_start")
        listen_key = await self._fetch_listen_key()
        if not listen_key:
            return
        self._listen_key = listen_key
        ws_url = f"{BINANCE_PRIVATE_WS_URL}/{listen_key}"
        self._ws = await websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=10,
            max_size=1_000_000,
        )
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        self._mark_live("connected")
        self._ensure_heartbeat()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            self._mark_live()
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            event = payload.get("e")
            if event == "listenKeyExpired":
                self._emit_event("listen_key_expired")
                self._listen_key = None
                break
            if event != "ORDER_TRADE_UPDATE":
                continue
            data = payload.get("o") or {}
            order_id = _safe_order_id(data.get("i") or data.get("orderId"))
            if not order_id:
                continue
            filled = _safe_float(data.get("z"))
            status = data.get("X")
            symbol = data.get("s")
            side = data.get("S")
            self._update_order(
                order_id,
                symbol=symbol,
                side=side,
                filled_qty=filled,
                status=status,
                mode="set",
            )

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

    async def _reset_connection(self) -> None:
        await super()._reset_connection()
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._keepalive_task = None

    async def _fetch_listen_key(self) -> str | None:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        if not api_key:
            self._emit_event("listen_key_missing_api_key")
            logger.warning("binance listenKey missing api key")
            return None
        self._emit_event("listen_key_request")
        headers = {"X-MBX-APIKEY": api_key}
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    BINANCE_LISTEN_KEY_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    payload = await resp.json()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("binance listenKey request failed: %s", exc)
            self._emit_event("listen_key_failed", {"error": str(exc)})
            return None
        listen_key = payload.get("listenKey") if isinstance(payload, dict) else None
        if not listen_key:
            logger.warning("binance listenKey response missing: %s", payload)
            self._emit_event("listen_key_failed", {"error": "missing_listen_key"})
            return None
        self._emit_event("listen_key_ok")
        return str(listen_key)

    async def _extend_listen_key(self) -> None:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        if not api_key:
            return
        headers = {"X-MBX-APIKEY": api_key}
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.put(
                    BINANCE_LISTEN_KEY_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    await resp.text()
        except Exception:
            return

    async def _close_listen_key(self) -> None:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        if not api_key:
            return
        headers = {"X-MBX-APIKEY": api_key}
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.delete(
                    BINANCE_LISTEN_KEY_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    await resp.text()
        except Exception:
            return

    async def _keepalive_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(BINANCE_LISTEN_KEY_RENEW_SECONDS)
            if self._stop.is_set():
                break
            await self._extend_listen_key()


class OkxOrderStream(_BaseOrderStream):
    def __init__(
        self,
        gateway: ExchangeGateway,
        contract_size: float | None = None,
        *,
        event_cb: Optional[callable] = None,
    ) -> None:
        super().__init__("okx", contract_size=contract_size, event_cb=event_cb)
        self._gateway = gateway

    async def _connect_and_listen(self) -> None:
        await self._reset_connection()
        self._emit_event("connect_start")
        self._ws = await websockets.connect(
            OKX_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "orders", "instType": "SWAP"}]}))
        self._mark_live("connected")
        self._ensure_heartbeat()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            self._mark_live()
            if text == "pong":
                self._mark_pong()
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            if payload.get("event"):
                self._mark_live()
                continue
            if payload.get("arg", {}).get("channel") != "orders":
                continue
            data = payload.get("data") or []
            if not isinstance(data, list):
                data = [data]
            for item in data:
                order_id = _safe_order_id(item.get("ordId") or item.get("orderId"))
                if not order_id:
                    continue
                symbol = item.get("instId")
                side = item.get("side")
                status = item.get("state")
                filled = _safe_float(item.get("accFillSz"))
                mode = "set"
                if filled is None:
                    filled = _safe_float(item.get("fillSz"))
                    mode = "add"
                self._update_order(
                    order_id,
                    symbol=symbol,
                    side=side,
                    filled_qty=filled,
                    status=status,
                    mode=mode,
                )

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        passphrase = self._gateway.password
        if not api_key or not api_secret or not passphrase:
            self._emit_event("auth_missing")
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
        self._emit_event("auth_failed")
        return False

    async def _send_ping(self) -> None:
        if not self._ws:
            return
        self._record_ping()
        try:
            await self._ws.send("ping")
        except Exception:
            return
        self._mark_live("ping-send")
        self._mark_live("ping-send")

class GateOrderStream(_BaseOrderStream):
    def __init__(
        self,
        gateway: ExchangeGateway,
        contract_size: float | None = None,
        *,
        event_cb: Optional[callable] = None,
    ) -> None:
        super().__init__("gate", contract_size=contract_size, event_cb=event_cb)
        self._gateway = gateway
        self._server_time_offset: float | None = None
        self._api_key: str | None = None
        self._api_secret: str | None = None

    async def _connect_and_listen(self) -> None:
        await self._reset_connection()
        self._emit_event("connect_start")
        self._ws = await websockets.connect(
            GATE_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._subscribe()
        self._mark_live("connected")
        self._ensure_heartbeat()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            self._mark_live()
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            self._mark_live()
            self._ingest_server_time(payload)
            if payload.get("channel") == "futures.pong":
                self._mark_pong()
                continue
            if payload.get("event") == "subscribe":
                result = payload.get("result") or {}
                if result.get("status") in (None, "", "success"):
                    self._mark_live()
                continue
            if payload.get("channel") != "futures.orders":
                continue
            result = payload.get("result") or []
            if not isinstance(result, list):
                result = [result]
            for item in result:
                order_id = _safe_order_id(item.get("id") or item.get("order_id") or item.get("orderId"))
                if not order_id:
                    continue
                size = _safe_float(item.get("size"))
                left = _safe_float(item.get("left"))
                filled = None
                if size is not None:
                    if left is None:
                        filled = abs(size)
                    else:
                        filled = max(0.0, abs(size) - abs(left))
                status = item.get("status") or item.get("finish_as")
                symbol = item.get("contract") or item.get("symbol")
                side = item.get("side") or item.get("tradeSide")
                self._update_order(
                    order_id,
                    symbol=symbol,
                    side=side,
                    filled_qty=filled,
                    status=status,
                    mode="set",
                )

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        self._api_key = self._gateway.api_key
        self._api_secret = self._gateway.api_secret
        if not self._api_key or not self._api_secret:
            self._emit_event("auth_missing")
            logger.warning("gate ws auth missing api key/secret")
            return False
        return True

    async def _subscribe(self) -> None:
        if not self._api_key or not self._api_secret:
            return
        timestamp = self._current_server_time()
        payload = _gate_sign_auth_message(
            channel="futures.orders",
            event="subscribe",
            timestamp=timestamp,
            payload=["!all"],
            api_key=self._api_key,
            api_secret=self._api_secret,
        )
        await self._ws.send(json.dumps(payload))

    async def _send_ping(self) -> None:
        if not self._ws:
            return
        self._record_ping()
        payload = {"time": self._current_server_time(), "channel": "futures.ping"}
        try:
            await self._ws.send(json.dumps(payload))
        except Exception:
            return
        self._mark_live("ping-send")

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


class BitgetOrderStream(_BaseOrderStream):
    def __init__(
        self,
        gateway: ExchangeGateway,
        contract_size: float | None = None,
        *,
        event_cb: Optional[callable] = None,
    ) -> None:
        super().__init__("bitget", contract_size=contract_size, event_cb=event_cb)
        self._gateway = gateway
        self._inst_ids: set[str] = set()

    def _format_inst_id(self, symbol: str) -> str:
        normalized = normalize_symbol(symbol)
        settle = str(self._gateway.spec.settle_currency or "USDT").upper()
        if settle and normalized and not normalized.endswith(settle):
            normalized = f"{normalized}{settle}"
        return normalized

    async def ensure_symbols(self, symbols: Iterable[str]) -> None:
        new_ids = {self._format_inst_id(symbol) for symbol in symbols if symbol}
        new_ids.discard("")
        to_add = new_ids - self._inst_ids
        if not to_add:
            return
        self._inst_ids |= to_add
        if not self._ws:
            return
        for inst_id in sorted(to_add):
            payload = {
                "op": "subscribe",
                "args": [
                    {"instType": "USDT-FUTURES", "channel": "orders", "instId": inst_id}
                ],
            }
            await self._ws.send(json.dumps(payload))

    async def _connect_and_listen(self) -> None:
        await self._reset_connection()
        self._emit_event("connect_start")
        self._ws = await websockets.connect(
            BITGET_PRIVATE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
        )
        if not await self._auth():
            return
        await self._subscribe()
        self._mark_live("connected")
        self._ensure_heartbeat()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            self._mark_live()
            if text == "pong":
                self._mark_pong()
                continue
            if text == "ping":
                self._mark_live("ping")
                await self._ws.send("pong")
                continue
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            self._mark_live()
            if payload.get("event") in ("login", "subscribe"):
                self._mark_live()
                continue
            if payload.get("arg", {}).get("channel") != "orders":
                continue
            data = payload.get("data") or []
            if not isinstance(data, list):
                data = [data]
            for item in data:
                order_id = _safe_order_id(item.get("orderId") or item.get("order_id") or item.get("id"))
                if not order_id:
                    continue
                symbol = item.get("instId") or item.get("symbol")
                side = item.get("side")
                status = item.get("status") or item.get("state")
                # Bitget emits cumulative accBaseVolume and per-fill baseVolume; prefer cumulative totals.
                filled = None
                for key in ("accBaseVolume", "accFillSz", "filledQty"):
                    filled = _safe_float(item.get(key))
                    if filled is not None:
                        break
                mode = "set"
                if filled is None:
                    for key in ("baseVolume", "fillSz", "fillQty"):
                        filled = _safe_float(item.get(key))
                        if filled is not None:
                            break
                    mode = "add"
                self._update_order(
                    order_id,
                    symbol=symbol,
                    side=side,
                    filled_qty=filled,
                    status=status,
                    mode=mode,
                )

    async def _auth(self) -> bool:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        passphrase = self._gateway.password
        if not api_key or not api_secret or not passphrase:
            self._emit_event("auth_missing")
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
        inst_ids = self._inst_ids or {"default"}
        for inst_id in sorted(inst_ids):
            payload = {
                "op": "subscribe",
                "args": [
                    {"instType": "USDT-FUTURES", "channel": "orders", "instId": inst_id}
                ],
            }
            await self._ws.send(json.dumps(payload))

    async def _send_ping(self) -> None:
        if not self._ws:
            return
        self._record_ping()
        try:
            await self._ws.send("ping")
        except Exception:
            return

class KucoinOrderStream(_BaseOrderStream):
    def __init__(
        self,
        gateway: ExchangeGateway,
        contract_size: float | None = None,
        *,
        event_cb: Optional[callable] = None,
    ) -> None:
        super().__init__("kucoin", contract_size=contract_size, event_cb=event_cb)
        self._gateway = gateway

    async def _connect_and_listen(self) -> None:
        await self._reset_connection()
        self._emit_event("connect_start")
        endpoint = await self._fetch_private_endpoint()
        if not endpoint:
            return
        self._ws = await websockets.connect(
            endpoint,
            ping_interval=20,
            ping_timeout=10,
        )
        await self._subscribe()
        self._mark_live("connected")
        self._ensure_heartbeat()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            self._mark_live()
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                continue
            if payload.get("type") == "pong":
                self._mark_pong()
                continue
            if payload.get("type") == "ping":
                self._mark_live("ping")
                await self._ws.send(json.dumps({"id": payload.get("id"), "type": "pong"}))
                continue
            if payload.get("type") != "message":
                continue
            topic = str(payload.get("topic") or "")
            if "/contractMarket/tradeOrders" not in topic:
                continue
            data = payload.get("data") or {}
            items = data if isinstance(data, list) else [data]
            self._mark_live()
            for item in items:
                order_id = _safe_order_id(item.get("orderId") or item.get("order_id"))
                if not order_id:
                    continue
                symbol = item.get("symbol")
                side = item.get("side")
                status = item.get("status") or item.get("type")
                filled = _safe_float(item.get("filledSize"))
                if filled is None:
                    size = _safe_float(item.get("size"))
                    remain = _safe_float(item.get("remainSize"))
                    if size is not None:
                        filled = max(0.0, abs(size) - abs(remain or 0.0))
                self._update_order(
                    order_id,
                    symbol=symbol,
                    side=side,
                    filled_qty=filled,
                    status=status,
                    mode="set",
                )

    async def _fetch_private_endpoint(self) -> str | None:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        passphrase = self._gateway.password
        if not api_key or not api_secret or not passphrase:
            self._emit_event("auth_missing")
            logger.warning("kucoin ws auth missing api key/secret/passphrase")
            return None
        await self._gateway.ensure_client()
        client = self._gateway.client
        if not client:
            return None
        try:
            token_info = await client.futuresPrivatePostBulletPrivate()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("kucoin ws token fetch failed: %s", exc)
            self._emit_event("auth_failed", {"error": str(exc)})
            return None
        data = (token_info or {}).get("data") or {}
        endpoint = data.get("instanceServers", [{}])[0].get("endpoint") if data else None
        token = data.get("token") if data else None
        if not endpoint or not token:
            self._emit_event("auth_failed", {"error": "missing_token"})
            return None
        return f"{endpoint}?token={token}"

    async def _subscribe(self) -> None:
        payload = {
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": "/contractMarket/tradeOrders",
            "privateChannel": True,
            "response": True,
        }
        await self._ws.send(json.dumps(payload))

    async def _send_ping(self) -> None:
        if not self._ws:
            return
        self._record_ping()
        payload = {"id": str(int(time.time() * 1000)), "type": "ping"}
        try:
            await self._ws.send(json.dumps(payload))
        except Exception:
            return
        self._mark_live("ping-send")

class BingxOrderStream(_BaseOrderStream):
    def __init__(
        self,
        gateway: ExchangeGateway,
        contract_size: float | None = None,
        *,
        event_cb: Optional[callable] = None,
    ) -> None:
        super().__init__("bingx", contract_size=contract_size, event_cb=event_cb)
        self._gateway = gateway
        self._listen_key: Optional[str] = None
        self._keepalive_task: Optional[asyncio.Task] = None

    async def _connect_and_listen(self) -> None:
        await self._reset_connection()
        self._emit_event("connect_start")
        logger.info("bingx order ws connect start")
        listen_key = await self._fetch_listen_key()
        if not listen_key:
            logger.warning("bingx order ws connect aborted: listenKey missing")
            return
        self._listen_key = listen_key
        ws_url = f"{BINGX_SWAP_WS_URL}?listenKey={listen_key}"
        logger.info("bingx order ws connecting")
        self._ws = await websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=10,
            max_size=1_000_000,
        )
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        self._mark_live("connected")
        logger.info("bingx order ws connected")
        self._ensure_heartbeat()
        while not self._stop.is_set():
            message = await self._ws.recv()
            text = _decode_gzip_message(message)
            if not text:
                continue
            self._mark_live()
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if _bingx_is_ping_message(text, payload):
                self._mark_live("ping")
                await self._ws.send("Pong")
                continue
            if _bingx_is_pong_message(text, payload):
                self._mark_pong()
                continue
            if not isinstance(payload, dict):
                continue
            event = payload.get("e")
            if event:
                self._mark_live(f"event:{event}")
            if event == "listenKeyExpired":
                logger.warning("bingx order ws listenKey expired; reconnecting")
                self._emit_event("listen_key_expired")
                self._listen_key = None
                break
            if event not in ("ORDER_TRADE_UPDATE", "TRADE_UPDATE"):
                continue
            data = payload.get("o") or {}
            self._mark_live("order")
            order_id = _safe_order_id(data.get("i") or data.get("orderId") or data.get("orderID"))
            if not order_id:
                continue
            filled = _safe_float(data.get("z"))
            status = data.get("X")
            symbol = data.get("s")
            side = data.get("S")
            self._update_order(
                order_id,
                symbol=symbol,
                side=side,
                filled_qty=filled,
                status=status,
                mode="set",
            )

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

    async def _reset_connection(self) -> None:
        await super()._reset_connection()
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._keepalive_task = None

    async def _fetch_listen_key(self) -> str | None:
        _bootstrap_env(force=True)
        await self._gateway.refresh_credentials_async(force_env=True)
        api_key = self._gateway.api_key
        api_secret = self._gateway.api_secret
        if not api_key:
            self._emit_event("listen_key_missing_api_key")
            logger.warning("bingx listenKey missing api key")
            return None
        self._emit_event("listen_key_request")
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
            self._emit_event("listen_key_failed", {"error": str(exc)})
            return None
        listen_key = payload.get("listenKey") or payload.get("data", {}).get("listenKey") or payload.get("data")
        if not listen_key:
            logger.warning("bingx listenKey response missing: %s", payload)
            self._emit_event("listen_key_failed", {"error": "missing_listen_key"})
            return None
        self._emit_event("listen_key_ok")
        logger.info("bingx listenKey ok (len=%d)", len(str(listen_key)))
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
            await asyncio.sleep(BINGX_LISTEN_KEY_RENEW_SECONDS)
            if self._stop.is_set():
                break
            await self._extend_listen_key()

class LiveOrderTracker:
    def __init__(self) -> None:
        self._streams: dict[str, _BaseOrderStream] = {}
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}
        self._health_configs = {
            exchange: dict(config) for exchange, config in DEFAULT_WS_ORDER_HEALTH.items()
        }
        self._event_cb: Optional[callable] = None

    def set_event_cb(self, event_cb: Optional[callable]) -> None:
        self._event_cb = event_cb
        for stream in self._streams.values():
            stream.set_event_cb(event_cb)

    def set_contract_sizes(self, contract_sizes: dict[str, float | None]) -> None:
        for exchange, size in contract_sizes.items():
            stream = self._streams.get(exchange)
            if stream:
                stream.set_contract_size(size)

    def set_health_configs(self, configs: Mapping[str, Mapping[str, Any]] | None) -> None:
        if not configs:
            return
        for exchange, override in configs.items():
            if not exchange:
                continue
            normalized = normalize_exchange_name(str(exchange))
            base = self._health_configs.get(normalized, DEFAULT_WS_ORDER_HEALTH_FALLBACK)
            self._health_configs[normalized] = _normalize_health_config(override, base)
            stream = self._streams.get(normalized)
            if stream:
                stream.set_health_config(self._health_configs[normalized])

    def health_config(self, exchange: str) -> dict[str, float | int]:
        normalized = normalize_exchange_name(str(exchange))
        cfg = self._health_configs.get(normalized)
        if cfg:
            return dict(cfg)
        return dict(DEFAULT_WS_ORDER_HEALTH_FALLBACK)

    async def ensure(self, exchanges: list[str], *, symbols: Mapping[str, Iterable[str]] | None = None) -> None:
        for exchange in exchanges:
            if exchange in self._streams:
                stream = self._streams[exchange]
                stream.set_health_config(self._health_configs.get(exchange))
                stream.set_event_cb(self._event_cb)
                if symbols and exchange in symbols:
                    await stream.ensure_symbols(symbols[exchange])
                await stream.start()
                continue
            gateway = self._gateways.get(exchange)
            if gateway is None:
                continue
            stream = self._build_stream(exchange, gateway)
            if stream is None:
                continue
            if symbols and exchange in symbols:
                await stream.ensure_symbols(symbols[exchange])
            stream.set_health_config(self._health_configs.get(exchange))
            stream.set_event_cb(self._event_cb)
            self._streams[exchange] = stream
            await stream.start()

    def is_live(self, exchange: str, *, stale_after: float | None = None) -> bool:
        stream = self._streams.get(exchange)
        if not stream:
            return False
        if stale_after is not None:
            return stream.is_live(stale_after=stale_after)
        return stream.is_healthy()

    def is_healthy(self, exchange: str) -> bool:
        stream = self._streams.get(exchange)
        if not stream:
            return False
        return stream.is_healthy()

    async def await_healthy(self, exchange: str, *, attempts: int, grace_sec: float) -> bool:
        stream = self._streams.get(exchange)
        if not stream:
            return False
        await stream.start()
        if stream.is_healthy():
            return True
        attempts = max(1, int(attempts))
        grace_sec = max(0.0, float(grace_sec))
        for _idx in range(attempts):
            stream.emit_control_event(
                "reconnect_attempt",
                {"attempt": _idx + 1, "attempts": attempts, "grace_sec": grace_sec},
            )
            await stream.force_reconnect()
            deadline = time.time() + grace_sec
            while time.time() < deadline:
                if stream.is_healthy():
                    return True
                await asyncio.sleep(0.5)
        return stream.is_healthy()

    def get_order(self, exchange: str, order_id: str) -> dict[str, Any] | None:
        stream = self._streams.get(exchange)
        if not stream:
            return None
        return stream.get_order(order_id)

    def health_snapshot(self, exchange: str) -> dict[str, Any]:
        stream = self._streams.get(exchange)
        if not stream:
            return {"exchange": exchange, "healthy": False, "error": "missing_stream"}
        return stream.health_snapshot()

    async def close(self) -> None:
        for stream in list(self._streams.values()):
            await stream.stop()
        self._streams.clear()

    def _build_stream(self, exchange: str, gateway: ExchangeGateway) -> _BaseOrderStream | None:
        if exchange == "bybit":
            return BybitOrderStream(gateway, event_cb=self._event_cb)
        if exchange == "binance":
            return BinanceOrderStream(gateway, event_cb=self._event_cb)
        if exchange == "okx":
            return OkxOrderStream(gateway, event_cb=self._event_cb)
        if exchange == "gate":
            return GateOrderStream(gateway, event_cb=self._event_cb)
        if exchange == "bitget":
            return BitgetOrderStream(gateway, event_cb=self._event_cb)
        if exchange == "kucoin":
            return KucoinOrderStream(gateway, event_cb=self._event_cb)
        if exchange == "bingx":
            return BingxOrderStream(gateway, event_cb=self._event_cb)
        return None
