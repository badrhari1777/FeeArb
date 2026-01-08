from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from config import CACHE_DIR


def load_cache(name: str, ttl_seconds: int) -> Any | None:
    """Return cached payload if it is younger than ``ttl_seconds``."""

    path = CACHE_DIR / name
    if not path.exists():
        return None

    age = time.time() - path.stat().st_mtime
    if age > ttl_seconds:
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def save_cache(name: str, payload: Any) -> Path:
    """Persist payload in the cache directory and return the resulting path."""

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / name
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(text, encoding="utf-8")
    return path

