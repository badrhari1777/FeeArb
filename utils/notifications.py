from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp

from config import BASE_DIR

logger = logging.getLogger(__name__)

_DOTENV_PATH = BASE_DIR / ".env"
_ENV_CACHE: dict[str, Any] = {
    "mtime_ns": None,
    "values": {},
}


def _load_dotenv_values() -> dict[str, str]:
    try:
        stat = _DOTENV_PATH.stat()
    except OSError:
        _ENV_CACHE["mtime_ns"] = None
        _ENV_CACHE["values"] = {}
        return {}
    cached_mtime = _ENV_CACHE.get("mtime_ns")
    if cached_mtime == stat.st_mtime_ns:
        return dict(_ENV_CACHE.get("values") or {})
    values: dict[str, str] = {}
    try:
        for raw_line in _DOTENV_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            values[key] = value
    except OSError:
        values = {}
    _ENV_CACHE["mtime_ns"] = stat.st_mtime_ns
    _ENV_CACHE["values"] = dict(values)
    return values


def env_or_dotenv(name: str) -> str:
    value = os.getenv(name)
    if value is not None and str(value).strip():
        return str(value).strip()
    return str((_load_dotenv_values().get(name) or "")).strip()


def _normalize_primary_channel(value: str | None) -> str:
    channel = str(value or "").strip().lower()
    if channel not in {"telegram", "pushbullet", "ntfy"}:
        return "telegram"
    return channel


def _normalize_fallback_channel(value: str | None, primary_channel: str) -> str:
    channel = str(value or "").strip().lower()
    if channel not in {"none", "telegram", "pushbullet", "ntfy"}:
        return "none"
    if channel == primary_channel:
        return "none"
    return channel


def _derive_pushbullet_title(text: str) -> str:
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if line:
            return line[:100]
    return "FeeArb notification"


class NotificationRouter:
    def __init__(
        self,
        *,
        primary_channel: str = "telegram",
        fallback_channel: str = "none",
        telegram_chat_id: str = "",
    ) -> None:
        self._primary_channel = _normalize_primary_channel(primary_channel)
        self._fallback_channel = _normalize_fallback_channel(fallback_channel, self._primary_channel)
        self._telegram_chat_id = str(telegram_chat_id or "").strip()
        self._missing_config_warned: set[str] = set()

    def update_config(
        self,
        *,
        primary_channel: str | None = None,
        fallback_channel: str | None = None,
        telegram_chat_id: str | None = None,
    ) -> None:
        if primary_channel is not None:
            self._primary_channel = _normalize_primary_channel(primary_channel)
        if telegram_chat_id is not None:
            self._telegram_chat_id = str(telegram_chat_id or "").strip()
        if fallback_channel is not None:
            self._fallback_channel = _normalize_fallback_channel(fallback_channel, self._primary_channel)
        else:
            self._fallback_channel = _normalize_fallback_channel(self._fallback_channel, self._primary_channel)

    async def send_text_status(self, text: str, *, title: str | None = None) -> str:
        channels = [self._primary_channel]
        if self._fallback_channel != "none" and self._fallback_channel != self._primary_channel:
            channels.append(self._fallback_channel)
        statuses: list[str] = []
        for idx, channel in enumerate(channels):
            status = await self._send_via_channel(channel, text, title=title)
            statuses.append(status)
            if status == "ok":
                if idx > 0:
                    logger.info("Notification delivered via fallback channel=%s", channel)
                return "ok"
            if idx == 0 and len(channels) > 1:
                logger.warning(
                    "Notification primary channel failed channel=%s status=%s; trying fallback=%s",
                    channel,
                    status,
                    channels[1],
                )
        if "http_error" in statuses:
            return "http_error"
        if "error" in statuses:
            return "error"
        return "skipped"

    async def send_text(self, text: str, *, title: str | None = None) -> bool:
        return (await self.send_text_status(text, title=title)) == "ok"

    async def _send_via_channel(self, channel: str, text: str, *, title: str | None = None) -> str:
        if channel == "telegram":
            return await self._send_telegram_text_status(text)
        if channel == "pushbullet":
            return await self._send_pushbullet_text_status(text, title=title)
        if channel == "ntfy":
            return await self._send_ntfy_text_status(text, title=title)
        return "skipped"

    async def _send_telegram_text_status(self, text: str) -> str:
        token = env_or_dotenv("TELEGRAM_BOT_TOKEN")
        chat_id = env_or_dotenv("TELEGRAM_CHAT_ID") or self._telegram_chat_id
        if not token or not chat_id:
            if "telegram" not in self._missing_config_warned:
                logger.info("Telegram send skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
                self._missing_config_warned.add("telegram")
            return "skipped"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": text}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(url, data=data) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.warning("Telegram alert failed (%s): %s", resp.status, body)
                        return "http_error"
            return "ok"
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Telegram alert error: %s", exc)
            return "error"

    async def _send_pushbullet_text_status(self, text: str, *, title: str | None = None) -> str:
        token = env_or_dotenv("PUSHBULLET_ACCESS_TOKEN")
        if not token:
            if "pushbullet" not in self._missing_config_warned:
                logger.info("Pushbullet send skipped: PUSHBULLET_ACCESS_TOKEN not set")
                self._missing_config_warned.add("pushbullet")
            return "skipped"
        payload = {
            "type": "note",
            "title": str(title or _derive_pushbullet_title(text)),
            "body": str(text or ""),
        }
        headers = {
            "Access-Token": token,
            "Content-Type": "application/json",
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post("https://api.pushbullet.com/v2/pushes", json=payload, headers=headers) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.warning("Pushbullet alert failed (%s): %s", resp.status, body)
                        return "http_error"
            return "ok"
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Pushbullet alert error: %s", exc)
            return "error"

    async def _send_ntfy_text_status(self, text: str, *, title: str | None = None) -> str:
        base_url = env_or_dotenv("NTFY_BASE_URL") or "https://ntfy.sh"
        topic = env_or_dotenv("NTFY_TOPIC")
        token = env_or_dotenv("NTFY_TOKEN")
        if not topic:
            if "ntfy" not in self._missing_config_warned:
                logger.info("ntfy send skipped: NTFY_TOPIC not set")
                self._missing_config_warned.add("ntfy")
            return "skipped"
        base_url = base_url.rstrip("/")
        url = f"{base_url}/{quote(topic, safe='')}"
        headers = {
            "Title": str(title or _derive_pushbullet_title(text))[:250],
            "Priority": env_or_dotenv("NTFY_PRIORITY") or "4",
            "Tags": env_or_dotenv("NTFY_TAGS") or "warning",
            "Content-Type": "text/plain; charset=utf-8",
        }
        click_url = env_or_dotenv("NTFY_CLICK_URL")
        if click_url:
            headers["Click"] = click_url
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(url, data=str(text or "").encode("utf-8"), headers=headers) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.warning("ntfy alert failed (%s): %s", resp.status, body)
                        return "http_error"
            return "ok"
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("ntfy alert error: %s", exc)
            return "error"
