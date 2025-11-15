"""
Project-wide configuration.

This is a central place for static settings such as supported exchanges.
API credentials and anything secret should remain in `.env` or environment
variables - keep this file for non-sensitive defaults only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Final, List

# Root directory of the project (useful for resolving relative paths).
BASE_DIR: Final[Path] = Path(__file__).resolve().parent

# Persistent state directory (wallets, positions, etc.).
STATE_DIR: Final[Path] = BASE_DIR / "state"

# Exchanges we currently support across the pipeline.
SUPPORTED_EXCHANGES: Final[List[str]] = [
    "bybit",
    "mexc",
    # Remaining adapters stay registered but are inactive by default.
]

# Default maker/taker commission rates (VIP 0) for perp contracts.
EXCHANGE_COMMISSIONS: Final[Dict[str, Dict[str, float]]] = {
    "bybit": {"maker": 0.000324, "taker": 0.0009},
    "okx": {"maker": 0.0002, "taker": 0.0005},
    "bingx": {"maker": 0.0002, "taker": 0.0005},
    "bitget": {"maker": 0.00036, "taker": 0.001},
    "gate": {"maker": 0.0002, "taker": 0.0005},
    "mexc": {"maker": 0.00008, "taker": 0.00032},
}

# Cached scraper payloads remain valid for this many seconds.
PARSE_CACHE_TTL_SECONDS: Final[int] = 300

# Base directory for lightweight cache files.
CACHE_DIR: Final[Path] = BASE_DIR / "data" / "cache"

# Default path for execution-layer runtime configuration.
EXECUTION_SETTINGS_PATH: Final[Path] = BASE_DIR / "data" / "execution_settings.json"
