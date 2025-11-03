from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Dict

from config import CACHE_DIR

REGISTRY_PATH = CACHE_DIR / "missing_symbols.json"
DEFAULT_TTL_SECONDS = 24 * 60 * 60

_lock = threading.Lock()
_registry: Dict[str, Dict[str, float]] = {}
_loaded = False


def _ensure_loaded() -> None:
    global _loaded, _registry  # pylint: disable=global-statement
    if _loaded:
        return
    with _lock:
        if _loaded:
            return
        if REGISTRY_PATH.exists():
            try:
                with REGISTRY_PATH.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                _registry = {
                    exchange: {symbol: float(ts) for symbol, ts in symbols.items()}
                    for exchange, symbols in payload.items()
                    if isinstance(symbols, dict)
                }
            except (ValueError, OSError):
                _registry = {}
        else:
            _registry = {}
        _loaded = True


def _save() -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
        exchange: {symbol: timestamp for symbol, timestamp in symbols.items()}
        for exchange, symbols in _registry.items()
        if symbols
    }
    with REGISTRY_PATH.open("w", encoding="utf-8") as handle:
        json.dump(serializable, handle, indent=2, sort_keys=True)


def is_recently_missing(exchange: str, symbol: str, *, ttl: int = DEFAULT_TTL_SECONDS) -> bool:
    """Return True if we recently observed the symbol as missing on the exchange."""
    _ensure_loaded()
    now = time.time()
    with _lock:
        entry = _registry.get(exchange, {}).get(symbol)
        if entry is None:
            return False
        if now - entry <= ttl:
            return True
        # Expired; remove and persist.
        del _registry[exchange][symbol]
        if not _registry[exchange]:
            del _registry[exchange]
        _save()
        return False


def record_missing(exchange: str, symbol: str) -> None:
    """Persist the fact that the symbol is unavailable on the exchange."""
    _ensure_loaded()
    now = time.time()
    with _lock:
        symbols = _registry.setdefault(exchange, {})
        prev = symbols.get(symbol)
        symbols[symbol] = now
        # Avoid excessive writes if timestamp unchanged
        if prev is None or now - prev > 60:
            _save()


def purge_expired(*, ttl: int = DEFAULT_TTL_SECONDS) -> None:
    """Remove any expired entries from the registry."""
    _ensure_loaded()
    now = time.time()
    removed = False
    with _lock:
        for exchange in list(_registry):
            symbols = _registry[exchange]
            for symbol in list(symbols):
                if now - symbols[symbol] > ttl:
                    del symbols[symbol]
                    removed = True
            if not symbols:
                del _registry[exchange]
                removed = True
        if removed:
            _save()
