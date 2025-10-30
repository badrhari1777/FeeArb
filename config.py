"""
Project-wide configuration.

This is a central place for static settings such as supported exchanges.
API credentials and anything secret should remain in `.env` or environment
variables - keep this file for non-sensitive defaults only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, List

# Root directory of the project (useful for resolving relative paths).
BASE_DIR: Final[Path] = Path(__file__).resolve().parent

# Exchanges we currently support across the pipeline.
SUPPORTED_EXCHANGES: Final[List[str]] = [
    "bybit",
    "mexc",
    # Remaining adapters stay registered but are inactive by default.
]

# Cached scraper payloads remain valid for this many seconds.
PARSE_CACHE_TTL_SECONDS: Final[int] = 300

# Base directory for lightweight cache files.
CACHE_DIR: Final[Path] = BASE_DIR / "data" / "cache"
