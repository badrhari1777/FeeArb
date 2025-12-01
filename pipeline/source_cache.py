from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from utils.sources import get_cached_source, upsert_cached_source

logger = logging.getLogger(__name__)


async def fetch_with_cache_async(
    source_name: str,
    fetch_fn: Callable[[], Awaitable[Any]],
) -> Any:
    """
    Try to fetch live; on failure, fall back to cached payload.
    On success, cache the payload.
    """
    try:
        payload = await fetch_fn()
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("%s: live fetch failed, using cache if available: %s", source_name, exc)
        cached = get_cached_source(source_name)
        if cached is None:
            raise
        return cached
    else:
        try:
            upsert_cached_source(source_name, payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("%s: failed to cache payload: %s", source_name, exc)
        return payload
