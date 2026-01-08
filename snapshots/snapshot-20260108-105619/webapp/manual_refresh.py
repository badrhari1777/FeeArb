from __future__ import annotations

"""
Utility entry point that forces a data snapshot before the web server starts.

Usage:
    python -m webapp.manual_refresh

This helps environments where the automatic startup fetch (which uses
pyppeteer via requests-html) is blocked or requires additional time. Running
the command once will populate the cache so that the first HTTP request served
by the FastAPI app responds immediately.
"""

import logging

from pipeline import collect_snapshot
from utils import setup_logging
from project_settings import SettingsManager


def main() -> None:
    setup_logging()
    settings = SettingsManager().current
    logging.info("Collecting snapshot...")
    snapshot = collect_snapshot(
        source_settings=settings.sources,
        exchange_settings=settings.exchanges,
    )
    logging.info(
        "Snapshot prepared at %s with %d opportunities.",
        snapshot.generated_at.isoformat(),
        len(snapshot.opportunities),
    )


if __name__ == "__main__":
    main()
