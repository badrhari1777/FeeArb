"""Exchange adapter registry."""

from __future__ import annotations

from typing import Dict, Type

from .base import ExchangeAdapter
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


def normalize_exchange_name(name: str) -> str:
    key = name.lower()
    return EXCHANGE_ALIASES.get(key, key)


def get_adapter(name: str) -> ExchangeAdapter:
    canonical = normalize_exchange_name(name)
    cls = ADAPTER_FACTORIES.get(canonical)
    if not cls:
        raise KeyError(f"No adapter registered for exchange '{name}'")
    return cls()


__all__ = [
    "ExchangeAdapter",
    "get_adapter",
    "normalize_exchange_name",
    "ADAPTER_FACTORIES",
]
