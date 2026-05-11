from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from config import BASE_DIR

SCHEMA_VERSION = 2
DEFAULT_DB_PATH = Path(BASE_DIR) / "state" / "coin_analysis.db"
_TEST_DB_PATH: Path | None = None


def set_test_db_path(path: Path | None) -> None:
    global _TEST_DB_PATH
    _TEST_DB_PATH = path


def _db_path() -> Path:
    return _TEST_DB_PATH or DEFAULT_DB_PATH


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


@contextmanager
def _connect() -> Iterator[sqlite3.Connection]:
    db_path = _db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


@dataclass
class CoinInstrumentRow:
    canonical_symbol: str
    exchange: str
    exchange_symbol: str
    base_asset: str | None = None
    quote_asset: str | None = None
    contract_type: str | None = None
    contract_multiplier: float | None = None
    tick_size: float | None = None
    qty_step: float | None = None
    min_qty: float | None = None
    min_notional: float | None = None
    funding_interval_hours: float | None = None
    is_active: bool = True
    source_ts_ms: int | None = None
    updated_at_ms: int | None = None


@dataclass
class CoinPairRow:
    pair_key: str
    canonical_symbol: str
    exchange_a: str
    exchange_b: str
    exchange_a_symbol: str
    exchange_b_symbol: str
    is_active: bool = True
    updated_at_ms: int | None = None


@dataclass
class CoinSymbolSessionRow:
    canonical_symbol: str
    started_at_ms: int
    expires_at_ms: int
    is_tracking: bool = True
    updated_at_ms: int | None = None


@dataclass
class CoinFocusSnapshotRow:
    ts_ms: int
    canonical_symbol: str
    exchange: str
    exchange_symbol: str | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    mid: float | None = None
    mark_price: float | None = None
    index_price: float | None = None
    premium_pct: float | None = None
    funding_rate: float | None = None
    predicted_funding_rate: float | None = None
    next_funding_ts_ms: int | None = None
    quote_age_ms: int | None = None
    source_type: str | None = None
    staleness_flag: bool = False
    focus_reason: str | None = None


@dataclass
class CoinFundingHistoryRow:
    canonical_symbol: str
    exchange: str
    ts_ms: int
    funding_rate: float | None = None
    predicted_funding_rate: float | None = None
    interval_hours: float | None = None
    mark_price: float | None = None
    source_type: str | None = None
    inserted_at_ms: int | None = None


@dataclass
class CoinOpenInterestHistoryRow:
    canonical_symbol: str
    exchange: str
    ts_ms: int
    oi_contracts: float | None = None
    oi_notional: float | None = None
    interval_label: str | None = None
    source_type: str | None = None
    inserted_at_ms: int | None = None


@dataclass
class CoinFeatureSnapshotRow:
    ts_ms: int
    pair_key: str
    canonical_symbol: str
    context_mode: str
    feature_set_version: str
    direction: str
    features: dict[str, Any]
    data_quality: dict[str, Any]


@dataclass
class CoinDecisionRow:
    decision_id: str
    ts_ms: int
    mode: str
    canonical_symbol: str
    pair_key: str
    direction: str
    action: str
    decision_phase: str
    confidence_score: float
    reason_codes: list[str]
    reason_text: list[str]
    scores: dict[str, Any]
    features_ref: str | None = None
    state_ref: str | None = None
    operator_note: str | None = None


@dataclass
class CoinPaperPositionRow:
    position_key: str
    opened_at_ms: int
    closed_at_ms: int | None
    status: str
    canonical_symbol: str
    pair_key: str
    direction: str
    qty: float
    entry_context: dict[str, Any] | None = None
    updated_at_ms: int | None = None


@dataclass
class CoinTradeActivityRow:
    event_id: str
    ts_ms: int
    canonical_symbol: str
    pair_key: str | None = None
    direction: str | None = None
    activity_type: str = "unknown"
    source: str | None = None
    state_ref: str | None = None
    payload: dict[str, Any] | None = None


@dataclass
class CoinCandidateShortlistRow:
    ts_ms: int
    canonical_symbol: str
    pair_key: str | None = None
    rank: int = 0
    source_name: str | None = None
    direction_hint: str | None = None
    candidate_score: float | None = None
    funding_edge_pct: float | None = None
    entry_spread_pct: float | None = None
    premium_diff_pct: float | None = None
    oi_change_1h_pct: float | None = None
    oi_change_4h_pct: float | None = None
    reason_codes: list[str] | None = None
    payload: dict[str, Any] | None = None


@dataclass
class CoinRealPositionObservationRow:
    state_ref: str
    ts_ms: int
    canonical_symbol: str
    pair_key: str | None = None
    direction: str | None = None
    long_exchange: str | None = None
    short_exchange: str | None = None
    qty: float | None = None
    status: str = "open"
    payload: dict[str, Any] | None = None


def ensure_schema() -> None:
    with _connect() as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS ca_schema_version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_instruments (
                canonical_symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                exchange_symbol TEXT NOT NULL,
                base_asset TEXT,
                quote_asset TEXT,
                contract_type TEXT,
                contract_multiplier REAL,
                tick_size REAL,
                qty_step REAL,
                min_qty REAL,
                min_notional REAL,
                funding_interval_hours REAL,
                is_active INTEGER NOT NULL DEFAULT 1,
                source_ts_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (canonical_symbol, exchange)
            );

            CREATE TABLE IF NOT EXISTS ca_pairs (
                pair_key TEXT PRIMARY KEY,
                canonical_symbol TEXT NOT NULL,
                exchange_a TEXT NOT NULL,
                exchange_b TEXT NOT NULL,
                exchange_a_symbol TEXT NOT NULL,
                exchange_b_symbol TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_symbol_sessions (
                canonical_symbol TEXT PRIMARY KEY,
                started_at_ms INTEGER NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                is_tracking INTEGER NOT NULL DEFAULT 1,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_market_snapshots_focus (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER NOT NULL,
                canonical_symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                exchange_symbol TEXT,
                bid REAL,
                ask REAL,
                bid_size REAL,
                ask_size REAL,
                mid REAL,
                mark_price REAL,
                index_price REAL,
                premium_pct REAL,
                funding_rate REAL,
                predicted_funding_rate REAL,
                next_funding_ts_ms INTEGER,
                quote_age_ms INTEGER,
                source_type TEXT,
                staleness_flag INTEGER NOT NULL DEFAULT 0,
                focus_reason TEXT
            );

            CREATE TABLE IF NOT EXISTS ca_funding_history (
                canonical_symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                funding_rate REAL,
                predicted_funding_rate REAL,
                interval_hours REAL,
                mark_price REAL,
                source_type TEXT,
                inserted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (canonical_symbol, exchange, ts_ms)
            );

            CREATE TABLE IF NOT EXISTS ca_open_interest_history (
                canonical_symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                oi_contracts REAL,
                oi_notional REAL,
                interval_label TEXT,
                source_type TEXT,
                inserted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (canonical_symbol, exchange, ts_ms)
            );

            CREATE TABLE IF NOT EXISTS ca_feature_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER NOT NULL,
                pair_key TEXT NOT NULL,
                canonical_symbol TEXT NOT NULL,
                context_mode TEXT NOT NULL,
                feature_set_version TEXT NOT NULL,
                direction TEXT NOT NULL,
                features_json TEXT NOT NULL,
                data_quality_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_decisions (
                decision_id TEXT PRIMARY KEY,
                ts_ms INTEGER NOT NULL,
                mode TEXT NOT NULL,
                canonical_symbol TEXT NOT NULL,
                pair_key TEXT NOT NULL,
                direction TEXT NOT NULL,
                action TEXT NOT NULL,
                decision_phase TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                reason_codes_json TEXT NOT NULL,
                reason_text_json TEXT NOT NULL,
                scores_json TEXT NOT NULL,
                features_ref TEXT,
                state_ref TEXT,
                operator_note TEXT
            );

            CREATE TABLE IF NOT EXISTS ca_outcomes (
                decision_id TEXT NOT NULL,
                horizon TEXT NOT NULL,
                outcome_json TEXT NOT NULL,
                evaluated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (decision_id, horizon),
                FOREIGN KEY (decision_id) REFERENCES ca_decisions(decision_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ca_paper_positions (
                position_key TEXT PRIMARY KEY,
                opened_at_ms INTEGER NOT NULL,
                closed_at_ms INTEGER,
                status TEXT NOT NULL,
                canonical_symbol TEXT NOT NULL,
                pair_key TEXT NOT NULL,
                direction TEXT NOT NULL,
                qty REAL NOT NULL,
                entry_context_json TEXT,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_paper_legs (
                position_key TEXT NOT NULL,
                exchange TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL,
                current_qty REAL,
                fees_paid REAL,
                realized_pnl REAL,
                unrealized_pnl REAL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (position_key, exchange, side),
                FOREIGN KEY (position_key) REFERENCES ca_paper_positions(position_key) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ca_paper_events (
                event_id TEXT PRIMARY KEY,
                position_key TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT,
                FOREIGN KEY (position_key) REFERENCES ca_paper_positions(position_key) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ca_trade_activity (
                event_id TEXT PRIMARY KEY,
                ts_ms INTEGER NOT NULL,
                canonical_symbol TEXT NOT NULL,
                pair_key TEXT,
                direction TEXT,
                activity_type TEXT NOT NULL,
                source TEXT,
                state_ref TEXT,
                payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS ca_candidate_shortlist_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER NOT NULL,
                canonical_symbol TEXT NOT NULL,
                pair_key TEXT,
                rank INTEGER NOT NULL,
                source_name TEXT,
                direction_hint TEXT,
                candidate_score REAL,
                funding_edge_pct REAL,
                entry_spread_pct REAL,
                premium_diff_pct REAL,
                oi_change_1h_pct REAL,
                oi_change_4h_pct REAL,
                reason_codes_json TEXT,
                payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS ca_real_position_observations (
                state_ref TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                canonical_symbol TEXT NOT NULL,
                pair_key TEXT,
                direction TEXT,
                long_exchange TEXT,
                short_exchange TEXT,
                qty REAL,
                status TEXT NOT NULL,
                payload_json TEXT,
                PRIMARY KEY (state_ref, ts_ms)
            );

            CREATE INDEX IF NOT EXISTS idx_ca_focus_symbol_ts
                ON ca_market_snapshots_focus(canonical_symbol, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_focus_symbol_exchange_ts
                ON ca_market_snapshots_focus(canonical_symbol, exchange, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_feature_pair_ts
                ON ca_feature_snapshots(pair_key, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_decisions_symbol_ts
                ON ca_decisions(canonical_symbol, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_paper_events_position_ts
                ON ca_paper_events(position_key, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_trade_activity_symbol_ts
                ON ca_trade_activity(canonical_symbol, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_trade_activity_type_ts
                ON ca_trade_activity(activity_type, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_trade_activity_state_ts
                ON ca_trade_activity(state_ref, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_shortlist_ts_rank
                ON ca_candidate_shortlist_snapshots(ts_ms DESC, rank ASC);
            CREATE INDEX IF NOT EXISTS idx_ca_shortlist_symbol_ts
                ON ca_candidate_shortlist_snapshots(canonical_symbol, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_real_obs_symbol_ts
                ON ca_real_position_observations(canonical_symbol, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_real_obs_state_ts
                ON ca_real_position_observations(state_ref, ts_ms DESC);
            """
        )
        now_ms = _now_ms()
        conn.execute(
            """
            INSERT INTO ca_schema_version (id, version, updated_at_ms)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                version=excluded.version,
                updated_at_ms=excluded.updated_at_ms
            """,
            (SCHEMA_VERSION, now_ms),
        )


def get_schema_version() -> int:
    ensure_schema()
    with _connect() as conn:
        row = conn.execute("SELECT version FROM ca_schema_version WHERE id = 1").fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def upsert_instrument(row: CoinInstrumentRow) -> None:
    ensure_schema()
    now_ms = row.updated_at_ms or _now_ms()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_instruments (
                canonical_symbol, exchange, exchange_symbol,
                base_asset, quote_asset, contract_type,
                contract_multiplier, tick_size, qty_step, min_qty, min_notional,
                funding_interval_hours, is_active, source_ts_ms, updated_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_symbol, exchange) DO UPDATE SET
                exchange_symbol=excluded.exchange_symbol,
                base_asset=excluded.base_asset,
                quote_asset=excluded.quote_asset,
                contract_type=excluded.contract_type,
                contract_multiplier=excluded.contract_multiplier,
                tick_size=excluded.tick_size,
                qty_step=excluded.qty_step,
                min_qty=excluded.min_qty,
                min_notional=excluded.min_notional,
                funding_interval_hours=excluded.funding_interval_hours,
                is_active=excluded.is_active,
                source_ts_ms=excluded.source_ts_ms,
                updated_at_ms=excluded.updated_at_ms
            """,
            (
                row.canonical_symbol.upper(),
                row.exchange.lower(),
                row.exchange_symbol,
                row.base_asset,
                row.quote_asset,
                row.contract_type,
                row.contract_multiplier,
                row.tick_size,
                row.qty_step,
                row.min_qty,
                row.min_notional,
                row.funding_interval_hours,
                1 if row.is_active else 0,
                row.source_ts_ms,
                now_ms,
            ),
        )


def upsert_pair(row: CoinPairRow) -> None:
    ensure_schema()
    now_ms = row.updated_at_ms or _now_ms()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_pairs (
                pair_key, canonical_symbol, exchange_a, exchange_b,
                exchange_a_symbol, exchange_b_symbol, is_active, updated_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pair_key) DO UPDATE SET
                canonical_symbol=excluded.canonical_symbol,
                exchange_a=excluded.exchange_a,
                exchange_b=excluded.exchange_b,
                exchange_a_symbol=excluded.exchange_a_symbol,
                exchange_b_symbol=excluded.exchange_b_symbol,
                is_active=excluded.is_active,
                updated_at_ms=excluded.updated_at_ms
            """,
            (
                row.pair_key,
                row.canonical_symbol.upper(),
                row.exchange_a.lower(),
                row.exchange_b.lower(),
                row.exchange_a_symbol,
                row.exchange_b_symbol,
                1 if row.is_active else 0,
                now_ms,
            ),
        )


def upsert_symbol_session(row: CoinSymbolSessionRow) -> None:
    ensure_schema()
    now_ms = row.updated_at_ms or _now_ms()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_symbol_sessions (
                canonical_symbol, started_at_ms, expires_at_ms, is_tracking, updated_at_ms
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(canonical_symbol) DO UPDATE SET
                started_at_ms=excluded.started_at_ms,
                expires_at_ms=excluded.expires_at_ms,
                is_tracking=excluded.is_tracking,
                updated_at_ms=excluded.updated_at_ms
            """,
            (
                row.canonical_symbol.upper(),
                row.started_at_ms,
                row.expires_at_ms,
                1 if row.is_tracking else 0,
                now_ms,
            ),
        )


def get_active_symbol_sessions(now_ms: int | None = None) -> list[CoinSymbolSessionRow]:
    ensure_schema()
    cutoff_ms = now_ms or _now_ms()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT canonical_symbol, started_at_ms, expires_at_ms, is_tracking, updated_at_ms
            FROM ca_symbol_sessions
            WHERE is_tracking = 1 AND expires_at_ms >= ?
            ORDER BY updated_at_ms DESC
            """,
            (cutoff_ms,),
        ).fetchall()
    return [
        CoinSymbolSessionRow(
            canonical_symbol=str(item[0]),
            started_at_ms=int(item[1]),
            expires_at_ms=int(item[2]),
            is_tracking=bool(item[3]),
            updated_at_ms=int(item[4]),
        )
        for item in rows
    ]


def expire_symbol_sessions(now_ms: int | None = None) -> int:
    ensure_schema()
    cutoff_ms = now_ms or _now_ms()
    with _connect() as conn:
        cur = conn.execute(
            """
            UPDATE ca_symbol_sessions
            SET is_tracking = 0, updated_at_ms = ?
            WHERE is_tracking = 1 AND expires_at_ms < ?
            """,
            (cutoff_ms, cutoff_ms),
        )
    return int(cur.rowcount or 0)


def insert_focus_snapshot(row: CoinFocusSnapshotRow) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_market_snapshots_focus (
                ts_ms, canonical_symbol, exchange, exchange_symbol,
                bid, ask, bid_size, ask_size, mid, mark_price, index_price, premium_pct,
                funding_rate, predicted_funding_rate, next_funding_ts_ms, quote_age_ms,
                source_type, staleness_flag, focus_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.ts_ms,
                row.canonical_symbol.upper(),
                row.exchange.lower(),
                row.exchange_symbol,
                row.bid,
                row.ask,
                row.bid_size,
                row.ask_size,
                row.mid,
                row.mark_price,
                row.index_price,
                row.premium_pct,
                row.funding_rate,
                row.predicted_funding_rate,
                row.next_funding_ts_ms,
                row.quote_age_ms,
                row.source_type,
                1 if row.staleness_flag else 0,
                row.focus_reason,
            ),
        )


def get_focus_snapshots(
    canonical_symbol: str,
    *,
    exchange: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    params: list[Any] = [canonical_symbol.upper()]
    exchange_clause = ""
    if exchange:
        exchange_clause = "AND exchange = ?"
        params.append(exchange.lower())
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT ts_ms, canonical_symbol, exchange, exchange_symbol, bid, ask, bid_size, ask_size,
                   mid, mark_price, index_price, premium_pct, funding_rate, predicted_funding_rate,
                   next_funding_ts_ms, quote_age_ms, source_type, staleness_flag, focus_reason
            FROM ca_market_snapshots_focus
            WHERE canonical_symbol = ? {exchange_clause}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [
        {
            "ts_ms": int(item[0]),
            "canonical_symbol": item[1],
            "exchange": item[2],
            "exchange_symbol": item[3],
            "bid": item[4],
            "ask": item[5],
            "bid_size": item[6],
            "ask_size": item[7],
            "mid": item[8],
            "mark_price": item[9],
            "index_price": item[10],
            "premium_pct": item[11],
            "funding_rate": item[12],
            "predicted_funding_rate": item[13],
            "next_funding_ts_ms": item[14],
            "quote_age_ms": item[15],
            "source_type": item[16],
            "staleness_flag": bool(item[17]),
            "focus_reason": item[18],
        }
        for item in rows
    ]


def upsert_funding_history_rows(rows: Iterable[CoinFundingHistoryRow]) -> int:
    ensure_schema()
    materialized = list(rows or [])
    if not materialized:
        return 0
    now_ms = _now_ms()
    values = [
        (
            row.canonical_symbol.upper(),
            row.exchange.lower(),
            int(row.ts_ms),
            row.funding_rate,
            row.predicted_funding_rate,
            row.interval_hours,
            row.mark_price,
            row.source_type,
            int(row.inserted_at_ms or now_ms),
        )
        for row in materialized
        if row.canonical_symbol and row.exchange and row.ts_ms
    ]
    if not values:
        return 0
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO ca_funding_history (
                canonical_symbol, exchange, ts_ms, funding_rate, predicted_funding_rate,
                interval_hours, mark_price, source_type, inserted_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_symbol, exchange, ts_ms) DO UPDATE SET
                funding_rate=excluded.funding_rate,
                predicted_funding_rate=excluded.predicted_funding_rate,
                interval_hours=excluded.interval_hours,
                mark_price=excluded.mark_price,
                source_type=excluded.source_type,
                inserted_at_ms=excluded.inserted_at_ms
            """,
            values,
        )
    return len(values)


def get_funding_history(
    canonical_symbol: str,
    *,
    exchange: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    params: list[Any] = [canonical_symbol.upper()]
    exchange_clause = ""
    if exchange:
        exchange_clause = "AND exchange = ?"
        params.append(exchange.lower())
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT canonical_symbol, exchange, ts_ms, funding_rate, predicted_funding_rate,
                   interval_hours, mark_price, source_type, inserted_at_ms
            FROM ca_funding_history
            WHERE canonical_symbol = ? {exchange_clause}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [
        {
            "canonical_symbol": item[0],
            "exchange": item[1],
            "ts_ms": int(item[2]),
            "funding_rate": item[3],
            "predicted_funding_rate": item[4],
            "interval_hours": item[5],
            "mark_price": item[6],
            "source_type": item[7],
            "inserted_at_ms": int(item[8]),
        }
        for item in rows
    ]


def upsert_open_interest_history_rows(rows: Iterable[CoinOpenInterestHistoryRow]) -> int:
    ensure_schema()
    materialized = list(rows or [])
    if not materialized:
        return 0
    now_ms = _now_ms()
    values = [
        (
            row.canonical_symbol.upper(),
            row.exchange.lower(),
            int(row.ts_ms),
            row.oi_contracts,
            row.oi_notional,
            row.interval_label,
            row.source_type,
            int(row.inserted_at_ms or now_ms),
        )
        for row in materialized
        if row.canonical_symbol and row.exchange and row.ts_ms
    ]
    if not values:
        return 0
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO ca_open_interest_history (
                canonical_symbol, exchange, ts_ms, oi_contracts, oi_notional,
                interval_label, source_type, inserted_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_symbol, exchange, ts_ms) DO UPDATE SET
                oi_contracts=excluded.oi_contracts,
                oi_notional=excluded.oi_notional,
                interval_label=excluded.interval_label,
                source_type=excluded.source_type,
                inserted_at_ms=excluded.inserted_at_ms
            """,
            values,
        )
    return len(values)


def get_open_interest_history(
    canonical_symbol: str,
    *,
    exchange: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    params: list[Any] = [canonical_symbol.upper()]
    exchange_clause = ""
    if exchange:
        exchange_clause = "AND exchange = ?"
        params.append(exchange.lower())
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT canonical_symbol, exchange, ts_ms, oi_contracts, oi_notional,
                   interval_label, source_type, inserted_at_ms
            FROM ca_open_interest_history
            WHERE canonical_symbol = ? {exchange_clause}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [
        {
            "canonical_symbol": item[0],
            "exchange": item[1],
            "ts_ms": int(item[2]),
            "oi_contracts": item[3],
            "oi_notional": item[4],
            "interval_label": item[5],
            "source_type": item[6],
            "inserted_at_ms": int(item[7]),
        }
        for item in rows
    ]


def insert_feature_snapshot(row: CoinFeatureSnapshotRow) -> int:
    ensure_schema()
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO ca_feature_snapshots (
                ts_ms, pair_key, canonical_symbol, context_mode,
                feature_set_version, direction, features_json, data_quality_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.ts_ms,
                row.pair_key,
                row.canonical_symbol.upper(),
                row.context_mode,
                row.feature_set_version,
                row.direction,
                _json_dumps(row.features),
                _json_dumps(row.data_quality),
            ),
        )
        return int(cur.lastrowid)


def get_feature_snapshots(
    *,
    pair_key: str | None = None,
    canonical_symbol: str | None = None,
    direction: str | None = None,
    since_ts_ms: int | None = None,
    until_ts_ms: int | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    clauses: list[str] = []
    params: list[Any] = []
    if pair_key:
        clauses.append("pair_key = ?")
        params.append(pair_key)
    if canonical_symbol:
        clauses.append("canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if direction:
        clauses.append("direction = ?")
        params.append(direction)
    if since_ts_ms is not None:
        clauses.append("ts_ms >= ?")
        params.append(int(since_ts_ms))
    if until_ts_ms is not None:
        clauses.append("ts_ms <= ?")
        params.append(int(until_ts_ms))
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT id, ts_ms, pair_key, canonical_symbol, context_mode, feature_set_version,
                   direction, features_json, data_quality_json
            FROM ca_feature_snapshots
            {where}
            ORDER BY ts_ms DESC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "id": int(item[0]),
                "ts_ms": int(item[1]),
                "pair_key": item[2],
                "canonical_symbol": item[3],
                "context_mode": item[4],
                "feature_set_version": item[5],
                "direction": item[6],
                "features": json.loads(item[7] or "{}"),
                "data_quality": json.loads(item[8] or "{}"),
            }
        )
    return out


def get_feature_snapshot_by_id(feature_id: int) -> dict[str, Any] | None:
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, ts_ms, pair_key, canonical_symbol, context_mode, feature_set_version,
                   direction, features_json, data_quality_json
            FROM ca_feature_snapshots
            WHERE id = ?
            """,
            (int(feature_id),),
        ).fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "ts_ms": int(row[1]),
        "pair_key": row[2],
        "canonical_symbol": row[3],
        "context_mode": row[4],
        "feature_set_version": row[5],
        "direction": row[6],
        "features": json.loads(row[7] or "{}"),
        "data_quality": json.loads(row[8] or "{}"),
    }


def insert_decision(row: CoinDecisionRow) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_decisions (
                decision_id, ts_ms, mode, canonical_symbol, pair_key, direction,
                action, decision_phase, confidence_score, reason_codes_json,
                reason_text_json, scores_json, features_ref, state_ref, operator_note
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(decision_id) DO UPDATE SET
                ts_ms=excluded.ts_ms,
                mode=excluded.mode,
                canonical_symbol=excluded.canonical_symbol,
                pair_key=excluded.pair_key,
                direction=excluded.direction,
                action=excluded.action,
                decision_phase=excluded.decision_phase,
                confidence_score=excluded.confidence_score,
                reason_codes_json=excluded.reason_codes_json,
                reason_text_json=excluded.reason_text_json,
                scores_json=excluded.scores_json,
                features_ref=excluded.features_ref,
                state_ref=excluded.state_ref,
                operator_note=excluded.operator_note
            """,
            (
                row.decision_id,
                row.ts_ms,
                row.mode,
                row.canonical_symbol.upper(),
                row.pair_key,
                row.direction,
                row.action,
                row.decision_phase,
                float(row.confidence_score),
                _json_dumps(row.reason_codes),
                _json_dumps(row.reason_text),
                _json_dumps(row.scores),
                row.features_ref,
                row.state_ref,
                row.operator_note,
            ),
        )


def get_decisions(
    *,
    canonical_symbol: str | None = None,
    mode: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    clauses: list[str] = []
    params: list[Any] = []
    if canonical_symbol:
        clauses.append("canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if mode:
        clauses.append("mode = ?")
        params.append(mode)
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT decision_id, ts_ms, mode, canonical_symbol, pair_key, direction,
                   action, decision_phase, confidence_score, reason_codes_json,
                   reason_text_json, scores_json, features_ref, state_ref, operator_note
            FROM ca_decisions
            {where}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "decision_id": item[0],
                "ts_ms": int(item[1]),
                "mode": item[2],
                "canonical_symbol": item[3],
                "pair_key": item[4],
                "direction": item[5],
                "action": item[6],
                "decision_phase": item[7],
                "confidence_score": float(item[8]),
                "reason_codes": json.loads(item[9] or "[]"),
                "reason_text": json.loads(item[10] or "[]"),
                "scores": json.loads(item[11] or "{}"),
                "features_ref": item[12],
                "state_ref": item[13],
                "operator_note": item[14],
            }
        )
    return out


def insert_outcome(
    decision_id: str,
    horizon: str,
    outcome: dict[str, Any],
    *,
    evaluated_at_ms: int | None = None,
) -> None:
    ensure_schema()
    ts_ms = evaluated_at_ms or _now_ms()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_outcomes (decision_id, horizon, outcome_json, evaluated_at_ms)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(decision_id, horizon) DO UPDATE SET
                outcome_json=excluded.outcome_json,
                evaluated_at_ms=excluded.evaluated_at_ms
            """,
            (decision_id, horizon, _json_dumps(outcome), ts_ms),
        )


def get_outcomes(
    *,
    decision_id: str | None = None,
    canonical_symbol: str | None = None,
    horizon: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    clauses: list[str] = []
    params: list[Any] = []
    if decision_id:
        clauses.append("o.decision_id = ?")
        params.append(decision_id)
    if canonical_symbol:
        clauses.append("d.canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if horizon:
        clauses.append("o.horizon = ?")
        params.append(horizon)
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT o.decision_id, o.horizon, o.outcome_json, o.evaluated_at_ms,
                   d.canonical_symbol, d.mode, d.pair_key, d.direction, d.action
            FROM ca_outcomes o
            LEFT JOIN ca_decisions d ON d.decision_id = o.decision_id
            {where}
            ORDER BY o.evaluated_at_ms DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "decision_id": item[0],
                "horizon": item[1],
                "outcome": json.loads(item[2] or "{}"),
                "evaluated_at_ms": int(item[3]),
                "canonical_symbol": item[4],
                "mode": item[5],
                "pair_key": item[6],
                "direction": item[7],
                "action": item[8],
            }
        )
    return out


def upsert_paper_position(row: CoinPaperPositionRow) -> None:
    ensure_schema()
    now_ms = row.updated_at_ms or _now_ms()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_paper_positions (
                position_key, opened_at_ms, closed_at_ms, status, canonical_symbol,
                pair_key, direction, qty, entry_context_json, updated_at_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(position_key) DO UPDATE SET
                opened_at_ms=excluded.opened_at_ms,
                closed_at_ms=excluded.closed_at_ms,
                status=excluded.status,
                canonical_symbol=excluded.canonical_symbol,
                pair_key=excluded.pair_key,
                direction=excluded.direction,
                qty=excluded.qty,
                entry_context_json=excluded.entry_context_json,
                updated_at_ms=excluded.updated_at_ms
            """,
            (
                row.position_key,
                row.opened_at_ms,
                row.closed_at_ms,
                row.status,
                row.canonical_symbol.upper(),
                row.pair_key,
                row.direction,
                row.qty,
                _json_dumps(row.entry_context or {}),
                now_ms,
            ),
        )


def get_instruments(
    *,
    canonical_symbol: str | None = None,
    exchange: str | None = None,
    only_active: bool = True,
) -> list[dict[str, Any]]:
    ensure_schema()
    clauses: list[str] = []
    params: list[Any] = []
    if canonical_symbol:
        clauses.append("canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if exchange:
        clauses.append("exchange = ?")
        params.append(exchange.lower())
    if only_active:
        clauses.append("is_active = 1")
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT canonical_symbol, exchange, exchange_symbol, base_asset, quote_asset,
                   contract_type, contract_multiplier, tick_size, qty_step, min_qty, min_notional,
                   funding_interval_hours, is_active, source_ts_ms, updated_at_ms
            FROM ca_instruments
            {where}
            ORDER BY canonical_symbol ASC, exchange ASC
            """,
            params,
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "canonical_symbol": item[0],
                "exchange": item[1],
                "exchange_symbol": item[2],
                "base_asset": item[3],
                "quote_asset": item[4],
                "contract_type": item[5],
                "contract_multiplier": item[6],
                "tick_size": item[7],
                "qty_step": item[8],
                "min_qty": item[9],
                "min_notional": item[10],
                "funding_interval_hours": item[11],
                "is_active": bool(item[12]),
                "source_ts_ms": item[13],
                "updated_at_ms": item[14],
            }
        )
    return out


def get_pairs(
    *,
    canonical_symbol: str | None = None,
    only_active: bool = True,
) -> list[dict[str, Any]]:
    ensure_schema()
    clauses: list[str] = []
    params: list[Any] = []
    if canonical_symbol:
        clauses.append("canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if only_active:
        clauses.append("is_active = 1")
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT pair_key, canonical_symbol, exchange_a, exchange_b,
                   exchange_a_symbol, exchange_b_symbol, is_active, updated_at_ms
            FROM ca_pairs
            {where}
            ORDER BY canonical_symbol ASC, pair_key ASC
            """,
            params,
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "pair_key": item[0],
                "canonical_symbol": item[1],
                "exchange_a": item[2],
                "exchange_b": item[3],
                "exchange_a_symbol": item[4],
                "exchange_b_symbol": item[5],
                "is_active": bool(item[6]),
                "updated_at_ms": item[7],
            }
        )
    return out


def insert_paper_event(
    event_id: str,
    position_key: str,
    ts_ms: int,
    event_type: str,
    payload: dict[str, Any] | None,
) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_paper_events (event_id, position_key, ts_ms, event_type, payload_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                position_key=excluded.position_key,
                ts_ms=excluded.ts_ms,
                event_type=excluded.event_type,
                payload_json=excluded.payload_json
            """,
            (event_id, position_key, ts_ms, event_type, _json_dumps(payload or {})),
        )


def insert_trade_activity(row: CoinTradeActivityRow) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_trade_activity (
                event_id, ts_ms, canonical_symbol, pair_key, direction,
                activity_type, source, state_ref, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                ts_ms=excluded.ts_ms,
                canonical_symbol=excluded.canonical_symbol,
                pair_key=excluded.pair_key,
                direction=excluded.direction,
                activity_type=excluded.activity_type,
                source=excluded.source,
                state_ref=excluded.state_ref,
                payload_json=excluded.payload_json
            """,
            (
                row.event_id,
                int(row.ts_ms),
                row.canonical_symbol.upper(),
                row.pair_key,
                row.direction,
                str(row.activity_type or "unknown"),
                row.source,
                row.state_ref,
                _json_dumps(row.payload or {}),
            ),
        )


def get_trade_activity(
    *,
    canonical_symbol: str | None = None,
    state_ref: str | None = None,
    activity_types: Iterable[str] | None = None,
    since_ts_ms: int | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    clauses: list[str] = []
    params: list[Any] = []
    if canonical_symbol:
        clauses.append("canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if state_ref:
        clauses.append("state_ref = ?")
        params.append(state_ref)
    normalized_types = [str(item or "").strip() for item in (activity_types or []) if str(item or "").strip()]
    if normalized_types:
        placeholders = ",".join("?" for _ in normalized_types)
        clauses.append(f"activity_type IN ({placeholders})")
        params.extend(normalized_types)
    if since_ts_ms is not None:
        clauses.append("ts_ms >= ?")
        params.append(int(since_ts_ms))
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT event_id, ts_ms, canonical_symbol, pair_key, direction,
                   activity_type, source, state_ref, payload_json
            FROM ca_trade_activity
            {where}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [
        {
            "event_id": item[0],
            "ts_ms": int(item[1]),
            "canonical_symbol": item[2],
            "pair_key": item[3],
            "direction": item[4],
            "activity_type": item[5],
            "source": item[6],
            "state_ref": item[7],
            "payload": json.loads(item[8] or "{}"),
        }
        for item in rows
    ]


def insert_candidate_shortlist_rows(rows: Iterable[CoinCandidateShortlistRow]) -> int:
    ensure_schema()
    materialized = [
        row
        for row in list(rows or [])
        if row.canonical_symbol and int(row.rank or 0) > 0 and int(row.ts_ms or 0) > 0
    ]
    if not materialized:
        return 0
    values = [
        (
            int(row.ts_ms),
            row.canonical_symbol.upper(),
            row.pair_key,
            int(row.rank),
            row.source_name,
            row.direction_hint,
            row.candidate_score,
            row.funding_edge_pct,
            row.entry_spread_pct,
            row.premium_diff_pct,
            row.oi_change_1h_pct,
            row.oi_change_4h_pct,
            _json_dumps(list(row.reason_codes or [])),
            _json_dumps(row.payload or {}),
        )
        for row in materialized
    ]
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO ca_candidate_shortlist_snapshots (
                ts_ms, canonical_symbol, pair_key, rank, source_name, direction_hint,
                candidate_score, funding_edge_pct, entry_spread_pct, premium_diff_pct,
                oi_change_1h_pct, oi_change_4h_pct, reason_codes_json, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
    return len(values)


def get_candidate_shortlist(
    *,
    canonical_symbol: str | None = None,
    since_ts_ms: int | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    clauses: list[str] = []
    params: list[Any] = []
    if canonical_symbol:
        clauses.append("canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if since_ts_ms is not None:
        clauses.append("ts_ms >= ?")
        params.append(int(since_ts_ms))
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT id, ts_ms, canonical_symbol, pair_key, rank, source_name, direction_hint,
                   candidate_score, funding_edge_pct, entry_spread_pct, premium_diff_pct,
                   oi_change_1h_pct, oi_change_4h_pct, reason_codes_json, payload_json
            FROM ca_candidate_shortlist_snapshots
            {where}
            ORDER BY ts_ms DESC, rank ASC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [
        {
            "id": int(item[0]),
            "ts_ms": int(item[1]),
            "canonical_symbol": item[2],
            "pair_key": item[3],
            "rank": int(item[4]),
            "source_name": item[5],
            "direction_hint": item[6],
            "candidate_score": item[7],
            "funding_edge_pct": item[8],
            "entry_spread_pct": item[9],
            "premium_diff_pct": item[10],
            "oi_change_1h_pct": item[11],
            "oi_change_4h_pct": item[12],
            "reason_codes": json.loads(item[13] or "[]"),
            "payload": json.loads(item[14] or "{}"),
        }
        for item in rows
    ]


def get_paper_positions(*, status: str | None = None) -> list[dict[str, Any]]:
    ensure_schema()
    params: list[Any] = []
    where = ""
    if status:
        where = "WHERE status = ?"
        params.append(status)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT position_key, opened_at_ms, closed_at_ms, status, canonical_symbol,
                   pair_key, direction, qty, entry_context_json, updated_at_ms
            FROM ca_paper_positions
            {where}
            ORDER BY updated_at_ms DESC
            """,
            params,
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "position_key": item[0],
                "opened_at_ms": int(item[1]),
                "closed_at_ms": int(item[2]) if item[2] is not None else None,
                "status": item[3],
                "canonical_symbol": item[4],
                "pair_key": item[5],
                "direction": item[6],
                "qty": float(item[7]),
                "entry_context": json.loads(item[8] or "{}"),
                "updated_at_ms": int(item[9]),
            }
        )
    return out


def get_paper_events(
    position_key: str,
    *,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, position_key, ts_ms, event_type, payload_json
            FROM ca_paper_events
            WHERE position_key = ?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (position_key, safe_limit),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "event_id": item[0],
                "position_key": item[1],
                "ts_ms": int(item[2]),
                "event_type": item[3],
                "payload": json.loads(item[4] or "{}"),
            }
        )
    return out


def insert_real_position_observation(row: CoinRealPositionObservationRow) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ca_real_position_observations (
                state_ref, ts_ms, canonical_symbol, pair_key, direction,
                long_exchange, short_exchange, qty, status, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(state_ref, ts_ms) DO UPDATE SET
                canonical_symbol=excluded.canonical_symbol,
                pair_key=excluded.pair_key,
                direction=excluded.direction,
                long_exchange=excluded.long_exchange,
                short_exchange=excluded.short_exchange,
                qty=excluded.qty,
                status=excluded.status,
                payload_json=excluded.payload_json
            """,
            (
                row.state_ref,
                int(row.ts_ms),
                row.canonical_symbol.upper(),
                row.pair_key,
                row.direction,
                row.long_exchange,
                row.short_exchange,
                row.qty,
                str(row.status or "open"),
                _json_dumps(row.payload or {}),
            ),
        )


def get_real_position_observations(
    *,
    canonical_symbol: str | None = None,
    state_ref: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    ensure_schema()
    safe_limit = max(1, min(int(limit), 5000))
    clauses: list[str] = []
    params: list[Any] = []
    if canonical_symbol:
        clauses.append("canonical_symbol = ?")
        params.append(canonical_symbol.upper())
    if state_ref:
        clauses.append("state_ref = ?")
        params.append(state_ref)
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    params.append(safe_limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT state_ref, ts_ms, canonical_symbol, pair_key, direction,
                   long_exchange, short_exchange, qty, status, payload_json
            FROM ca_real_position_observations
            {where}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    out: list[dict[str, Any]] = []
    for item in rows:
        out.append(
            {
                "state_ref": item[0],
                "ts_ms": int(item[1]),
                "canonical_symbol": item[2],
                "pair_key": item[3],
                "direction": item[4],
                "long_exchange": item[5],
                "short_exchange": item[6],
                "qty": item[7],
                "status": item[8],
                "payload": json.loads(item[9] or "{}"),
            }
        )
    return out


def get_coin_analysis_table_counts() -> dict[str, int]:
    ensure_schema()
    tables = [
        "ca_market_snapshots_focus",
        "ca_funding_history",
        "ca_open_interest_history",
        "ca_feature_snapshots",
        "ca_decisions",
        "ca_outcomes",
        "ca_paper_positions",
        "ca_paper_events",
        "ca_trade_activity",
        "ca_candidate_shortlist_snapshots",
        "ca_real_position_observations",
        "ca_symbol_sessions",
    ]
    counts: dict[str, int] = {}
    with _connect() as conn:
        for table in tables:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = int((row[0] if row else 0) or 0)
        open_paper = conn.execute(
            "SELECT COUNT(*) FROM ca_paper_positions WHERE status = 'open'"
        ).fetchone()
        closed_paper = conn.execute(
            "SELECT COUNT(*) FROM ca_paper_positions WHERE status = 'closed'"
        ).fetchone()
    counts["ca_paper_positions_open"] = int((open_paper[0] if open_paper else 0) or 0)
    counts["ca_paper_positions_closed"] = int((closed_paper[0] if closed_paper else 0) or 0)
    return counts


def prune_coin_analysis_data(
    *,
    max_age_ms: int,
    closed_paper_max_age_ms: int | None = None,
    now_ms: int | None = None,
) -> dict[str, int]:
    ensure_schema()
    safe_now_ms = int(now_ms or _now_ms())
    safe_max_age_ms = max(60_000, int(max_age_ms))
    cutoff_ms = safe_now_ms - safe_max_age_ms
    closed_cutoff_ms = (
        safe_now_ms - max(60_000, int(closed_paper_max_age_ms))
        if closed_paper_max_age_ms is not None
        else cutoff_ms
    )
    deleted: dict[str, int] = {}
    with _connect() as conn:
        deleted["ca_market_snapshots_focus"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_market_snapshots_focus WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_funding_history"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_funding_history WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_open_interest_history"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_open_interest_history WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_feature_snapshots"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_feature_snapshots WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_outcomes"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_outcomes WHERE evaluated_at_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_decisions"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_decisions WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_real_position_observations"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_real_position_observations WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_trade_activity"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_trade_activity WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_candidate_shortlist_snapshots"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_candidate_shortlist_snapshots WHERE ts_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_symbol_sessions_inactive"] = int(
            (
                conn.execute(
                    "DELETE FROM ca_symbol_sessions WHERE is_tracking = 0 AND updated_at_ms < ?",
                    (cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_paper_positions_closed"] = int(
            (
                conn.execute(
                    """
                    DELETE FROM ca_paper_positions
                    WHERE status = 'closed'
                      AND COALESCE(closed_at_ms, updated_at_ms, opened_at_ms) < ?
                    """,
                    (closed_cutoff_ms,),
                ).rowcount
            )
            or 0
        )
        deleted["ca_paper_events_orphan"] = int(
            (
                conn.execute(
                    """
                    DELETE FROM ca_paper_events
                    WHERE position_key NOT IN (SELECT position_key FROM ca_paper_positions)
                    """,
                ).rowcount
            )
            or 0
        )
    deleted["total_deleted"] = int(sum(int(v or 0) for v in deleted.values()))
    return deleted


__all__ = [
    "CoinCandidateShortlistRow",
    "CoinDecisionRow",
    "CoinFeatureSnapshotRow",
    "CoinFocusSnapshotRow",
    "CoinFundingHistoryRow",
    "CoinInstrumentRow",
    "CoinOpenInterestHistoryRow",
    "CoinPairRow",
    "CoinPaperPositionRow",
    "CoinRealPositionObservationRow",
    "CoinSymbolSessionRow",
    "CoinTradeActivityRow",
    "SCHEMA_VERSION",
    "get_candidate_shortlist",
    "ensure_schema",
    "expire_symbol_sessions",
    "get_active_symbol_sessions",
    "get_decisions",
    "get_coin_analysis_table_counts",
    "get_feature_snapshot_by_id",
    "get_feature_snapshots",
    "get_funding_history",
    "get_focus_snapshots",
    "get_instruments",
    "get_open_interest_history",
    "get_outcomes",
    "get_paper_events",
    "get_real_position_observations",
    "get_trade_activity",
    "get_pairs",
    "get_paper_positions",
    "get_schema_version",
    "insert_candidate_shortlist_rows",
    "insert_decision",
    "insert_feature_snapshot",
    "insert_focus_snapshot",
    "insert_outcome",
    "insert_paper_event",
    "insert_real_position_observation",
    "insert_trade_activity",
    "prune_coin_analysis_data",
    "set_test_db_path",
    "upsert_funding_history_rows",
    "upsert_instrument",
    "upsert_open_interest_history_rows",
    "upsert_pair",
    "upsert_paper_position",
    "upsert_symbol_session",
]
