from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from typing import Any, Mapping

import aiohttp

from execution.accounts import ExchangeGateway, _bootstrap_env

KUCOIN_PRIVATE_REST_URL = "https://api-futures.kucoin.com/api/v1/bullet-private"
KUCOIN_SERVER_TIME_URL = "https://api-futures.kucoin.com/api/v1/timestamp"


def _sign_kucoin(secret: str, payload: str) -> str:
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")


def _is_kucoin_timestamp_error(payload: object) -> bool:
    if not isinstance(payload, Mapping):
        return False
    code = str(payload.get("code") or "").strip()
    message = str(payload.get("msg") or payload.get("message") or "").lower()
    return code == "400002" or "kc-api-timestamp" in message


async def _fetch_server_timestamp(session: aiohttp.ClientSession) -> str | None:
    try:
        async with session.get(
            KUCOIN_SERVER_TIME_URL,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            payload = await resp.json()
    except Exception:
        return None
    raw = payload.get("data") if isinstance(payload, Mapping) else None
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
    session: aiohttp.ClientSession,
    *,
    api_key: str,
    api_secret: str,
    passphrase_value: str,
    key_version: str,
    timestamp: str,
) -> dict[str, Any]:
    method = "POST"
    path = "/api/v1/bullet-private"
    prehash = f"{timestamp}{method}{path}"
    headers = {
        "KC-API-KEY": api_key,
        "KC-API-SIGN": _sign_kucoin(api_secret, prehash),
        "KC-API-TIMESTAMP": timestamp,
        "KC-API-PASSPHRASE": passphrase_value,
        "KC-API-KEY-VERSION": key_version,
    }
    async with session.post(
        KUCOIN_PRIVATE_REST_URL,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=10),
    ) as resp:
        payload = await resp.json()
    return payload if isinstance(payload, dict) else {"raw": payload}


def _extract_endpoint(payload: Mapping[str, Any]) -> str | None:
    data = payload.get("data") or {}
    if not isinstance(data, Mapping):
        return None
    servers = data.get("instanceServers") or []
    server = servers[0] if isinstance(servers, list) and servers else {}
    endpoint = data.get("endpoint") or (server.get("endpoint") if isinstance(server, Mapping) else None)
    token = data.get("token")
    if not endpoint or not token:
        return None
    return f"{endpoint}?token={token}"


async def fetch_kucoin_private_ws_endpoint(gateway: ExchangeGateway) -> str | None:
    _bootstrap_env(force=True)
    await gateway.refresh_credentials_async(force_env=True)
    api_key = gateway.api_key
    api_secret = gateway.api_secret
    passphrase = gateway.password
    if not api_key or not api_secret or not passphrase:
        return None

    options = getattr(gateway, "options", None) or getattr(gateway.spec, "options", None) or {}
    key_version = str(
        os.getenv("KUCOIN_API_KEY_VERSION")
        or options.get("keyVersion")
        or options.get("key_version")
        or "2"
    )
    passphrase_value = _sign_kucoin(api_secret, passphrase) if key_version == "2" else passphrase

    async with aiohttp.ClientSession() as session:
        timestamp = await _fetch_server_timestamp(session)
        payload = await _post_private_bullet(
            session,
            api_key=api_key,
            api_secret=api_secret,
            passphrase_value=passphrase_value,
            key_version=key_version,
            timestamp=timestamp or str(int(time.time() * 1000)),
        )
        if _is_kucoin_timestamp_error(payload):
            retry_timestamp = await _fetch_server_timestamp(session)
            if retry_timestamp:
                payload = await _post_private_bullet(
                    session,
                    api_key=api_key,
                    api_secret=api_secret,
                    passphrase_value=passphrase_value,
                    key_version=key_version,
                    timestamp=retry_timestamp,
                )
    return _extract_endpoint(payload)
