from __future__ import annotations

import hmac
import os
import secrets
from pathlib import Path


REMOTE_TOKEN_HEADER = "x-feearb-token"
REMOTE_TOKEN_ENV = "FEEARB_REMOTE_ACCESS_TOKEN"
REMOTE_TOKEN_PATH = Path(__file__).resolve().parents[1] / "state" / "remote_access_token.txt"
PUBLIC_PROXY_HEADER = "x-feearb-public-proxy"
PUBLIC_PROXY_VALUE = "tailscale-funnel"


def _read_saved_token() -> str:
    try:
        return REMOTE_TOKEN_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def remote_access_token() -> str:
    configured = str(os.getenv(REMOTE_TOKEN_ENV) or "").strip()
    if configured:
        return configured

    saved = _read_saved_token()
    if saved:
        return saved

    token = secrets.token_urlsafe(32)
    REMOTE_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    REMOTE_TOKEN_PATH.write_text(f"{token}\n", encoding="utf-8")
    return token


def is_cloudflare_request(headers: object) -> bool:
    get = getattr(headers, "get", None)
    if not callable(get):
        return False
    return bool(get("cf-connecting-ip") or get("cf-ray"))


def is_public_proxy_request(headers: object) -> bool:
    get = getattr(headers, "get", None)
    if not callable(get):
        return False
    supplied = str(get(PUBLIC_PROXY_HEADER) or "").strip().lower()
    return hmac.compare_digest(supplied, PUBLIC_PROXY_VALUE)


def has_valid_remote_token(headers: object, expected_token: str | None = None) -> bool:
    get = getattr(headers, "get", None)
    if not callable(get):
        return False
    supplied = str(get(REMOTE_TOKEN_HEADER) or "").strip()
    expected = expected_token if expected_token is not None else remote_access_token()
    return bool(supplied and expected and hmac.compare_digest(supplied, expected))
