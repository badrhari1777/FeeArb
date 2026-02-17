"""Exchange adapter registry."""

from __future__ import annotations

from typing import Dict, Type

from .base import ExchangeAdapter
from .binance import BinanceAdapter
from .bingx import BingXAdapter
from .bitget import BitgetAdapter
from .bybit import BybitAdapter
from .gate import GateAdapter
from .htx import HTXAdapter
from .kucoin import KucoinAdapter
from .mexc import MexcAdapter
from .okx import OKXAdapter

ADAPTER_FACTORIES: Dict[str, Type[ExchangeAdapter]] = {
    "bybit": BybitAdapter,
    "binance": BinanceAdapter,
    "okx": OKXAdapter,
    "htx": HTXAdapter,
    "bitget": BitgetAdapter,
    "bingx": BingXAdapter,
    "gate": GateAdapter,
    "mexc": MexcAdapter,
    "kucoin": KucoinAdapter,
}

EXCHANGE_ALIASES: Dict[str, str] = {
    "kukoin": "kucoin",
}
_ADAPTER_CACHE: Dict[str, ExchangeAdapter] = {}


def normalize_exchange_name(name: str) -> str:
    key = name.lower()
    return EXCHANGE_ALIASES.get(key, key)


def get_adapter(name: str) -> ExchangeAdapter:
    canonical = normalize_exchange_name(name)
    cls = ADAPTER_FACTORIES.get(canonical)
    if not cls:
        raise KeyError(f"No adapter registered for exchange '{name}'")
    return cls()


def get_adapter_cached(name: str) -> ExchangeAdapter:
    canonical = normalize_exchange_name(name)
    cached = _ADAPTER_CACHE.get(canonical)
    if cached is not None:
        return cached
    cls = ADAPTER_FACTORIES.get(canonical)
    if not cls:
        raise KeyError(f"No adapter registered for exchange '{name}'")
    adapter = cls()
    _ADAPTER_CACHE[canonical] = adapter
    return adapter


def clear_adapter_cache(name: str | None = None) -> None:
    if name is None:
        _ADAPTER_CACHE.clear()
        return
    canonical = normalize_exchange_name(name)
    _ADAPTER_CACHE.pop(canonical, None)


__all__ = [
    "ExchangeAdapter",
    "get_adapter",
    "get_adapter_cached",
    "clear_adapter_cache",
    "normalize_exchange_name",
    "ADAPTER_FACTORIES",
]
