from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence

from config import BASE_DIR

# Re-export helpers grouped at bottom.


DB_PATH = Path(BASE_DIR) / "state" / "cache.db"


@contextmanager
def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_schema() -> None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS symbol_meta (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                contract_size REAL,
                price_step REAL,
                qty_step REAL,
                min_qty REAL,
                max_qty REAL,
                min_notional REAL,
                max_leverage REAL,
                tick_size REAL,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, symbol)
            );

            CREATE TABLE IF NOT EXISTS risk_limits (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                tier INTEGER NOT NULL,
                max_notional REAL,
                max_leverage REAL,
                maintenance_margin_rate REAL,
                initial_margin_rate REAL,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, symbol, tier)
            );

            CREATE TABLE IF NOT EXISTS funding_history (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                rate REAL,
                interval_hours REAL,
                mark_price REAL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, symbol, ts_ms)
            );

            CREATE TABLE IF NOT EXISTS source_cache (
                source TEXT NOT NULL,
                payload TEXT NOT NULL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source)
            );
            """
        )


@dataclass
class SymbolMeta:
    exchange: str
    symbol: str
    contract_size: Optional[float] = None
    price_step: Optional[float] = None
    qty_step: Optional[float] = None
    min_qty: Optional[float] = None
    max_qty: Optional[float] = None
    min_notional: Optional[float] = None
    max_leverage: Optional[float] = None
    tick_size: Optional[float] = None
    ts: Optional[str] = None


def upsert_symbol_meta(meta: SymbolMeta) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO symbol_meta
            (exchange, symbol, contract_size, price_step, qty_step, min_qty, max_qty, min_notional, max_leverage, tick_size, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(exchange, symbol) DO UPDATE SET
                contract_size=excluded.contract_size,
                price_step=excluded.price_step,
                qty_step=excluded.qty_step,
                min_qty=excluded.min_qty,
                max_qty=excluded.max_qty,
                min_notional=excluded.min_notional,
                max_leverage=excluded.max_leverage,
                tick_size=excluded.tick_size,
                ts=CURRENT_TIMESTAMP
            """,
            (
                meta.exchange,
                meta.symbol,
                meta.contract_size,
                meta.price_step,
                meta.qty_step,
                meta.min_qty,
                meta.max_qty,
                meta.min_notional,
                meta.max_leverage,
                meta.tick_size,
            ),
        )


def get_symbol_meta(exchange: str, symbol: str) -> Optional[SymbolMeta]:
    ensure_schema()
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT exchange, symbol, contract_size, price_step, qty_step, min_qty, max_qty,
                   min_notional, max_leverage, tick_size, ts
            FROM symbol_meta
            WHERE exchange = ? AND symbol = ?
            """,
            (exchange, symbol),
        )
        row = cur.fetchone()
    if not row:
        return None
    return SymbolMeta(*row)


def upsert_risk_limits(
    exchange: str,
    symbol: str,
    limits: Iterable[dict],
) -> None:
    ensure_schema()
    rows = []
    for entry in limits:
        rows.append(
            (
                exchange,
                symbol,
                int(entry.get("tier") or 0),
                entry.get("max_notional"),
                entry.get("max_leverage"),
                entry.get("maintenance_margin_rate"),
                entry.get("initial_margin_rate"),
            )
        )
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO risk_limits
            (exchange, symbol, tier, max_notional, max_leverage, maintenance_margin_rate, initial_margin_rate, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(exchange, symbol, tier) DO UPDATE SET
                max_notional=excluded.max_notional,
                max_leverage=excluded.max_leverage,
                maintenance_margin_rate=excluded.maintenance_margin_rate,
                initial_margin_rate=excluded.initial_margin_rate,
                ts=CURRENT_TIMESTAMP
            """,
            rows,
        )


def insert_funding_history(
    exchange: str,
    symbol: str,
    records: Iterable[dict],
) -> None:
    ensure_schema()
    rows = []
    for rec in records:
        rows.append(
            (
                exchange,
                symbol,
                int(rec.get("ts_ms") or rec.get("timestamp") or 0),
                rec.get("rate"),
                rec.get("interval_hours"),
                rec.get("mark_price"),
            )
        )
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO funding_history
            (exchange, symbol, ts_ms, rate, interval_hours, mark_price, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(exchange, symbol, ts_ms) DO UPDATE SET
                rate=COALESCE(excluded.rate, funding_history.rate),
                interval_hours=COALESCE(excluded.interval_hours, funding_history.interval_hours),
                mark_price=COALESCE(excluded.mark_price, funding_history.mark_price),
                fetched_at=CURRENT_TIMESTAMP
            """,
            rows,
        )


def _last_funding_fetch(exchange: str, symbol: str) -> Optional[datetime]:
    ensure_schema()
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT MAX(fetched_at) FROM funding_history
            WHERE exchange = ? AND symbol = ?
            """,
            (exchange, symbol),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    try:
        return datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def get_funding_history(
    exchange: str,
    symbol: str,
    *,
    since_ms: Optional[int] = None,
    limit: int = 500,
) -> List[dict]:
    ensure_schema()
    params: list = [exchange, symbol]
    where = ""
    if since_ms:
        where = "AND ts_ms >= ?"
        params.append(since_ms)
    params.append(limit)
    with _connect() as conn:
        cur = conn.execute(
            f"""
            SELECT ts_ms, rate, interval_hours, mark_price, fetched_at
            FROM funding_history
            WHERE exchange = ? AND symbol = ? {where}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            params,
        )
        rows = cur.fetchall()
    return [
        {
            "ts_ms": ts_ms,
            "rate": rate,
            "interval_hours": interval_hours,
            "mark_price": mark_price,
            "fetched_at": fetched_at,
        }
        for ts_ms, rate, interval_hours, mark_price, fetched_at in rows
    ]


def get_or_fetch_funding_history(
    exchange: str,
    symbol: str,
    fetch_fn: Callable[[], Sequence[dict]],
    *,
    max_age_seconds: int = 120,
    since_ms: Optional[int] = None,
    limit: int = 500,
) -> List[dict]:
    """
    Return funding history from cache; refresh via fetch_fn if cache is stale.

    fetch_fn must return iterable of dicts with keys: ts_ms/timestamp, rate, interval_hours, mark_price.
    """
    last = _last_funding_fetch(exchange, symbol)
    now = datetime.now(timezone.utc)
    stale = last is None or (now - last).total_seconds() > max_age_seconds
    if stale:
        records = list(fetch_fn() or [])
        if records:
            insert_funding_history(exchange, symbol, records)
    return get_funding_history(exchange, symbol, since_ms=since_ms, limit=limit)


def get_or_fetch_symbol_meta(
    exchange: str,
    symbol: str,
    fetch_fn: Callable[[], SymbolMeta | None],
    *,
    max_age_seconds: Optional[int] = None,
) -> Optional[SymbolMeta]:
    """
    Return symbol meta from cache or fetch/store via fetch_fn if missing.
    """
    cached = get_symbol_meta(exchange, symbol)
    if cached and max_age_seconds is not None and cached.ts:
        try:
            ts_dt = datetime.fromisoformat(str(cached.ts))
            age = (datetime.now(ts_dt.tzinfo or timezone.utc) - ts_dt).total_seconds()
            if age <= max_age_seconds:
                return cached
        except Exception:
            pass
    elif cached:
        return cached
    fresh = fetch_fn()
    if fresh:
        upsert_symbol_meta(fresh)
    return fresh


def get_or_fetch_risk_limits(
    exchange: str,
    symbol: str,
    fetch_fn: Callable[[], Sequence[dict]],
) -> List[dict]:
    """
    Return risk limits from cache or fetch/store via fetch_fn if missing.
    """
    ensure_schema()
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT COUNT(*), MIN(ts) FROM risk_limits WHERE exchange = ? AND symbol = ?
            """,
            (exchange, symbol),
        )
        count, min_ts = cur.fetchone()
    if count == 0:
        data = list(fetch_fn() or [])
        if data:
            upsert_risk_limits(exchange, symbol, data)
    else:
        # If data exists but needs refresh, caller can clear table or adjust fetch_fn logic.
        pass
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT tier, max_notional, max_leverage, maintenance_margin_rate, initial_margin_rate, ts
            FROM risk_limits
            WHERE exchange = ? AND symbol = ?
            ORDER BY tier ASC
            """,
            (exchange, symbol),
        )
        rows = cur.fetchall()
    return [
        {
            "tier": tier,
            "max_notional": max_notional,
            "max_leverage": max_leverage,
            "maintenance_margin_rate": maintenance_margin_rate,
            "initial_margin_rate": initial_margin_rate,
            "ts": ts,
        }
        for tier, max_notional, max_leverage, maintenance_margin_rate, initial_margin_rate, ts in rows
    ]


__all__ = [
    "SymbolMeta",
    "ensure_schema",
    "upsert_symbol_meta",
    "get_symbol_meta",
    "get_or_fetch_symbol_meta",
    "upsert_risk_limits",
    "get_or_fetch_risk_limits",
    "insert_funding_history",
    "get_funding_history",
    "get_or_fetch_funding_history",
]
