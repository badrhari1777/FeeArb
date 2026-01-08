from __future__ import annotations

import json
from typing import Any, Optional

from .cache_db import ensure_schema, _connect


def upsert_cached_source(source: str, payload: dict | list | str) -> None:
    ensure_schema()
    if isinstance(payload, (dict, list)):
        payload_text = json.dumps(payload)
    else:
        payload_text = str(payload)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO source_cache (source, payload, fetched_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(source) DO UPDATE SET
                payload=excluded.payload,
                fetched_at=CURRENT_TIMESTAMP
            """,
            (source, payload_text),
        )


def get_cached_source(source: str) -> Optional[Any]:
    ensure_schema()
    with _connect() as conn:
        cur = conn.execute(
            "SELECT payload FROM source_cache WHERE source = ?",
            (source,),
        )
        row = cur.fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return row[0]
