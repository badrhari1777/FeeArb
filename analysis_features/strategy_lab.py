from __future__ import annotations

import csv
import json
import math
import sqlite3
import statistics
import time
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from config import BASE_DIR


DEFAULT_DB_PATH = BASE_DIR / "state" / "coin_analysis.db"
DEFAULT_LOG_DIR = BASE_DIR / "logs"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "strategy_lab"

PUMP_METADATA_PATHS: tuple[Path, ...] = (
    BASE_DIR / "data" / "research" / "pump_lifecycle_research" / "metadata.json",
    BASE_DIR / "data" / "research" / "pump_funding_premium_window_research" / "metadata.json",
    BASE_DIR / "data" / "research" / "pump_research_next_steps" / "metadata.json",
    BASE_DIR / "data" / "research" / "pump_live_transition_research" / "metadata.json",
    BASE_DIR / "data" / "research" / "pump_spike_risk_research" / "metadata.json",
)
PUMP_MULTIEXCHANGE_ARCHIVE = (
    BASE_DIR / "data" / "research" / "pump_short_multiexchange_2024_clean"
)

PUMP_EVENT_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "source": "pump_lifecycle",
        "path": BASE_DIR / "data" / "research" / "pump_lifecycle_research" / "lifecycle_events.csv",
        "event_id": "event_id",
        "symbol": "symbol",
        "ts": "trigger_ts",
        "iso": "trigger_iso",
        "event_type": "lifecycle_trigger",
        "metric_fields": ("trigger_pump_pct", "future_high_168h_pct", "future_low_168h_pct"),
    },
    {
        "source": "pump_premium_window",
        "path": BASE_DIR
        / "data"
        / "research"
        / "pump_funding_premium_window_research"
        / "premium_event_summary.csv",
        "event_id": "event_id",
        "symbol": "symbol",
        "ts": "",
        "iso": "trigger_iso",
        "event_type": "premium_window_trigger",
        "metric_fields": ("trigger_pump_pct", "best_net_pct", "best_long_funding_pct"),
    },
    {
        "source": "pump_universe_hourly_spike",
        "path": BASE_DIR
        / "data"
        / "research"
        / "pump_spike_risk_research"
        / "universe_hourly_spike_events.csv",
        "event_id": "",
        "symbol": "symbol",
        "ts": "start_ts",
        "iso": "start_iso",
        "event_type": "hourly_surge",
        "metric_fields": ("max_rise_pct", "max_retrace_pct", "bars"),
    },
    {
        "source": "pump_live_like_trade_cases",
        "path": BASE_DIR
        / "data"
        / "research"
        / "pump_live_transition_research"
        / "historical_strategy_trades.csv",
        "event_id": "case_id",
        "symbol": "symbol",
        "ts": "entry_ts",
        "iso": "entry_iso",
        "event_type": "live_like_entry",
        "metric_fields": ("pump_pct", "net_pct", "stress_pct"),
    },
)

HORIZONS_HOURS: tuple[tuple[str, float], ...] = (
    ("15m", 0.25),
    ("1h", 1.0),
    ("4h", 4.0),
)

PUMP_LINK_FIELDS: tuple[str, ...] = (
    "pump_event_id",
    "arbitrage_event_id",
    "symbol",
    "pump_source",
    "pump_event_type",
    "pump_ts_ms",
    "pump_ts_iso",
    "arbitrage_ts_ms",
    "arbitrage_ts_iso",
    "arb_minus_pump_hours",
    "arb_mid_spread_pct",
    "arb_mark_spread_pct",
    "arb_net_capture_4h_pct",
)

PUBLIC_EXCHANGE_IDS: dict[str, str] = {
    "binance": "binanceusdm",
    "bybit": "bybit",
    "kucoin": "kucoinfutures",
    "okx": "okx",
    "gate": "gate",
    "bitget": "bitget",
    "mexc": "mexc",
}

API_EXACT_MATCH_TOLERANCE_PCT = 0.75
API_SOFT_MATCH_TOLERANCE_PCT = 2.0


@dataclass(frozen=True, slots=True)
class StrategyLabConfig:
    trigger_abs_spread_pct: float = 0.75
    strong_abs_spread_pct: float = 3.0
    trigger_abs_zscore: float = 2.0
    hard_invalid_spread_pct: float = 30.0
    min_coverage_pct: float = 70.0
    min_spread_points: int = 30
    event_cooldown_h: float = 4.0
    future_match_tolerance_min: float = 10.0
    estimated_roundtrip_cost_pct: float = 0.18


def run_strategy_lab(
    *,
    db_path: Path = DEFAULT_DB_PATH,
    log_dir: Path = DEFAULT_LOG_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    config: StrategyLabConfig | None = None,
    enrich_public_api: bool = False,
    max_api_events: int = 8,
    api_window_hours: float = 8.0,
    api_timeframe: str = "5m",
) -> dict[str, Any]:
    """Build the first no-trading Strategy Lab evidence package.

    Local event extraction is deliberately causal: the event anchor is the
    first qualifying observation after a cooldown, never the later episode
    maximum. Public API enrichment uses public clients only and does not place
    or prepare orders.
    """

    started = time.time()
    cfg = config or StrategyLabConfig()
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = _connect_readonly(db_path)
    try:
        db_inventory = inventory_database(conn)
        spread_rows = load_arbitrage_feature_rows(conn)
        events, rejected = extract_arbitrage_events(spread_rows, cfg)
        arb_summary = summarize_arbitrage_hypotheses(events, cfg)
        funding_summary = analyze_funding_persistence(conn)
    finally:
        conn.close()

    operational_inventory = inventory_operational_logs(log_dir)
    pump_inventory = inventory_pump_research(PUMP_METADATA_PATHS)
    pump_inventory.extend(inventory_pump_archive(PUMP_MULTIEXCHANGE_ARCHIVE))
    pump_events = load_pump_event_catalog(PUMP_EVENT_SOURCES)
    pump_arb_links = link_pump_and_arbitrage_events(pump_events, events)
    source_inventory = db_inventory + operational_inventory + pump_inventory

    api_samples: list[dict[str, Any]] = []
    api_summary: list[dict[str, Any]] = []
    api_error = ""
    if enrich_public_api:
        try:
            api_samples, api_summary = enrich_events_from_public_apis(
                events,
                max_events=max_api_events,
                window_hours=api_window_hours,
                timeframe=api_timeframe,
            )
        except Exception as exc:  # pylint: disable=broad-except
            api_error = f"{type(exc).__name__}: {exc}"
    _write_jsonl(output_dir / "arbitrage_api_windows.jsonl", api_samples)

    hypothesis_rows = build_hypothesis_registry(
        arbitrage_summary=arb_summary,
        funding_summary=funding_summary,
        pump_inventory=pump_inventory,
        pump_events=pump_events,
        pump_arb_links=pump_arb_links,
        api_summary=api_summary,
        config=cfg,
    )

    _write_csv(output_dir / "source_inventory.csv", source_inventory)
    _write_csv(output_dir / "arbitrage_spread_events.csv", events)
    _write_csv(output_dir / "arbitrage_rejected_data_quality.csv", rejected)
    _write_csv(output_dir / "arbitrage_hypothesis_summary.csv", arb_summary)
    _write_csv(output_dir / "funding_persistence_summary.csv", funding_summary)
    _write_csv(output_dir / "pump_event_catalog.csv", pump_events)
    _write_csv(
        output_dir / "pump_arbitrage_event_links.csv",
        pump_arb_links,
        fieldnames=PUMP_LINK_FIELDS,
    )
    _write_csv(output_dir / "arbitrage_api_summary.csv", api_summary)
    _write_csv(output_dir / "hypothesis_registry.csv", hypothesis_rows)

    metadata = {
        "schema": "strategy_lab_v1",
        "mode": "research_only_no_trading",
        "db_path": str(db_path),
        "log_dir": str(log_dir),
        "output_dir": str(output_dir),
        "config": asdict(cfg),
        "arbitrage_feature_rows": len(spread_rows),
        "arbitrage_events": len(events),
        "rejected_data_quality_rows": len(rejected),
        "arbitrage_summary_rows": len(arb_summary),
        "funding_summary_rows": len(funding_summary),
        "pump_sources": len(pump_inventory),
        "pump_event_rows": len(pump_events),
        "pump_arbitrage_links": len(pump_arb_links),
        "operational_sources": len(operational_inventory),
        "api_enrichment_requested": bool(enrich_public_api),
        "api_enriched_events": len(api_summary),
        "api_error": api_error,
        "hypotheses": len(hypothesis_rows),
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "index.md").write_text(
        render_markdown_report(
            metadata=metadata,
            source_inventory=source_inventory,
            arbitrage_summary=arb_summary,
            funding_summary=funding_summary,
            hypotheses=hypothesis_rows,
            pump_events=pump_events,
            pump_arb_links=pump_arb_links,
            api_summary=api_summary,
        ),
        encoding="utf-8",
    )
    return metadata


def _connect_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def inventory_database(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    tables = [
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'ca_%' ORDER BY name"
        )
    ]
    timestamp_columns = {
        "ca_candidate_shortlist_snapshots": "ts_ms",
        "ca_decisions": "ts_ms",
        "ca_feature_snapshots": "ts_ms",
        "ca_funding_history": "ts_ms",
        "ca_instruments": "updated_at_ms",
        "ca_market_snapshots_focus": "ts_ms",
        "ca_open_interest_history": "ts_ms",
        "ca_outcomes": "evaluated_at_ms",
        "ca_pairs": "updated_at_ms",
        "ca_paper_events": "ts_ms",
        "ca_paper_positions": "opened_at_ms",
        "ca_real_position_observations": "ts_ms",
        "ca_symbol_sessions": "started_at_ms",
        "ca_trade_activity": "ts_ms",
    }
    out: list[dict[str, Any]] = []
    for table in tables:
        if table == "ca_schema_version":
            continue
        ts_col = timestamp_columns.get(table)
        select = "COUNT(*) AS rows"
        if ts_col:
            select += f", MIN({ts_col}) AS min_ts, MAX({ts_col}) AS max_ts"
        row = conn.execute(f"SELECT {select} FROM {table}").fetchone()
        payload = dict(row or {})
        out.append(
            {
                "source_family": "ordinary_arbitrage_db",
                "source": table,
                "records": int(payload.get("rows") or 0),
                "min_ts": payload.get("min_ts"),
                "max_ts": payload.get("max_ts"),
                "min_iso": _iso_from_ms(payload.get("min_ts")),
                "max_iso": _iso_from_ms(payload.get("max_ts")),
                "notes": "SQLite read-only inventory",
            }
        )
    return out


def load_arbitrage_feature_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "ca_feature_snapshots"):
        return []
    query = """
        SELECT ts_ms, pair_key, canonical_symbol,
               json_extract(features_json, '$.common.left_exchange') AS left_exchange,
               json_extract(features_json, '$.common.right_exchange') AS right_exchange,
               json_extract(features_json, '$.common.derived_spread.mid_spread_pct') AS mid_spread_pct,
               json_extract(features_json, '$.common.derived_spread.mark_spread_pct') AS mark_spread_pct,
               json_extract(features_json, '$.common.derived_spread.index_spread_pct') AS index_spread_pct,
               json_extract(features_json, '$.common.derived_spread.open_spread_long_a_short_b_pct') AS open_ab_pct,
               json_extract(features_json, '$.common.derived_spread.open_spread_long_b_short_a_pct') AS open_ba_pct,
               json_extract(features_json, '$.common.derived_spread.close_spread_long_a_short_b_pct') AS close_ab_pct,
               json_extract(features_json, '$.common.derived_spread.close_spread_long_b_short_a_pct') AS close_ba_pct,
               json_extract(features_json, '$.common.spread_features.spread_zscore_1h') AS zscore_1h,
               json_extract(features_json, '$.common.spread_features.spread_zscore_4h') AS zscore_4h,
               json_extract(features_json, '$.common.spread_features.spread_velocity_5m') AS velocity_5m,
               json_extract(features_json, '$.common.spread_features.spread_velocity_15m') AS velocity_15m,
               json_extract(features_json, '$.common.funding.left_hourly') AS left_funding_hourly,
               json_extract(features_json, '$.common.funding.right_hourly') AS right_funding_hourly,
               json_extract(features_json, '$.common.hours_to_next_funding_min') AS hours_to_funding,
               json_extract(features_json, '$.common.oi.left_change_6h_pct') AS left_oi_change_6h_pct,
               json_extract(features_json, '$.common.oi.right_change_6h_pct') AS right_oi_change_6h_pct,
               json_extract(data_quality_json, '$.coverage_pct') AS coverage_pct,
               json_extract(data_quality_json, '$.spread_points_total') AS spread_points
        FROM ca_feature_snapshots
        WHERE direction = 'long_a_short_b'
        ORDER BY pair_key, ts_ms
    """
    return [dict(row) for row in conn.execute(query)]


def extract_arbitrage_events(
    rows: Sequence[Mapping[str, Any]],
    config: StrategyLabConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    rejected: list[dict[str, Any]] = []
    for raw in rows:
        item = dict(raw)
        mid = _finite_float(item.get("mid_spread_pct"))
        if mid is None:
            continue
        item["mid_spread_pct"] = mid
        grouped[str(item.get("pair_key") or "")].append(item)
        rejection = _quality_rejection(item, config)
        if rejection:
            rejected.append(
                {
                    "ts_ms": item.get("ts_ms"),
                    "ts_iso": _iso_from_ms(item.get("ts_ms")),
                    "symbol": item.get("canonical_symbol"),
                    "pair_key": item.get("pair_key"),
                    "mid_spread_pct": mid,
                    "mark_spread_pct": item.get("mark_spread_pct"),
                    "reason": rejection,
                }
            )

    events: list[dict[str, Any]] = []
    cooldown_ms = int(config.event_cooldown_h * 3_600_000)
    for pair_key, series in grouped.items():
        series.sort(key=lambda item: int(item.get("ts_ms") or 0))
        times = [int(item.get("ts_ms") or 0) for item in series]
        last_event_ts = -10**30
        previously_qualified = False
        for item in series:
            qualifies = _event_qualifies(item, config)
            ts_ms = int(item.get("ts_ms") or 0)
            if qualifies and not previously_qualified and ts_ms - last_event_ts >= cooldown_ms:
                events.append(_build_event_row(item, series, times, config))
                last_event_ts = ts_ms
            previously_qualified = qualifies

    events.sort(key=lambda item: (int(item.get("ts_ms") or 0), str(item.get("pair_key") or "")))
    return events, _dedupe_rejections(rejected)


def _quality_rejection(item: Mapping[str, Any], config: StrategyLabConfig) -> str:
    mid = _finite_float(item.get("mid_spread_pct"))
    if mid is None:
        return "mid_spread_missing"
    if abs(mid) >= config.hard_invalid_spread_pct:
        return "hard_invalid_spread_or_contract_mapping"
    coverage = _finite_float(item.get("coverage_pct"))
    if coverage is not None and coverage < config.min_coverage_pct:
        return "coverage_below_minimum"
    points = _finite_float(item.get("spread_points"))
    if points is not None and points < config.min_spread_points:
        return "spread_history_too_short"
    mark = _finite_float(item.get("mark_spread_pct"))
    if mark is not None and mid * mark <= 0:
        return "mid_mark_direction_conflict"
    return ""


def _event_qualifies(item: Mapping[str, Any], config: StrategyLabConfig) -> bool:
    if _quality_rejection(item, config):
        return False
    mid = abs(float(item["mid_spread_pct"]))
    if mid < config.trigger_abs_spread_pct:
        return False
    if mid >= config.strong_abs_spread_pct:
        return True
    zscore = _finite_float(item.get("zscore_1h"))
    return zscore is not None and abs(zscore) >= config.trigger_abs_zscore


def _build_event_row(
    item: Mapping[str, Any],
    series: Sequence[Mapping[str, Any]],
    times: Sequence[int],
    config: StrategyLabConfig,
) -> dict[str, Any]:
    ts_ms = int(item.get("ts_ms") or 0)
    mid = float(item["mid_spread_pct"])
    direction = "long_b_short_a" if mid > 0 else "long_a_short_b"
    entry_open = _finite_float(item.get("open_ba_pct" if mid > 0 else "open_ab_pct"))
    left_funding = _finite_float(item.get("left_funding_hourly"))
    right_funding = _finite_float(item.get("right_funding_hourly"))
    funding_alignment = None
    expected_carry_pct_per_h = None
    if left_funding is not None and right_funding is not None:
        carry_raw = (left_funding - right_funding) if mid > 0 else (right_funding - left_funding)
        expected_carry_pct_per_h = carry_raw * 100.0
        funding_alignment = "aligned" if carry_raw > 0 else "opposed" if carry_raw < 0 else "flat"
    mark = _finite_float(item.get("mark_spread_pct"))
    mark_gap = abs(mid - mark) if mark is not None else None
    event: dict[str, Any] = {
        "event_id": f"arb|{item.get('pair_key')}|{ts_ms}",
        "ts_ms": ts_ms,
        "ts_iso": _iso_from_ms(ts_ms),
        "symbol": item.get("canonical_symbol"),
        "pair_key": item.get("pair_key"),
        "left_exchange": item.get("left_exchange") or _pair_part(item.get("pair_key"), 1),
        "right_exchange": item.get("right_exchange") or _pair_part(item.get("pair_key"), 2),
        "reversion_direction": direction,
        "mid_spread_pct": mid,
        "abs_mid_spread_pct": abs(mid),
        "mark_spread_pct": mark,
        "index_spread_pct": _finite_float(item.get("index_spread_pct")),
        "mid_mark_gap_pct": mark_gap,
        "mark_confirmation": (
            "missing"
            if mark is None
            else "confirmed"
            if mark_gap is not None and mark_gap <= max(0.5, abs(mid) * 0.5)
            else "divergent"
        ),
        "entry_open_spread_pct": entry_open,
        "zscore_1h": _finite_float(item.get("zscore_1h")),
        "zscore_4h": _finite_float(item.get("zscore_4h")),
        "velocity_5m": _finite_float(item.get("velocity_5m")),
        "velocity_15m": _finite_float(item.get("velocity_15m")),
        "velocity_state": _velocity_state(mid, item.get("velocity_5m")),
        "left_funding_hourly": left_funding,
        "right_funding_hourly": right_funding,
        "expected_carry_pct_per_h": expected_carry_pct_per_h,
        "funding_alignment": funding_alignment or "missing",
        "hours_to_funding": _finite_float(item.get("hours_to_funding")),
        "left_oi_change_6h_pct": _finite_float(item.get("left_oi_change_6h_pct")),
        "right_oi_change_6h_pct": _finite_float(item.get("right_oi_change_6h_pct")),
        "coverage_pct": _finite_float(item.get("coverage_pct")),
        "spread_points": _finite_float(item.get("spread_points")),
    }
    for label, hours in HORIZONS_HOURS:
        future = _nearest_future(series, times, ts_ms, hours, config.future_match_tolerance_min)
        close_key = "close_ba_pct" if mid > 0 else "close_ab_pct"
        future_mid = _finite_float((future or {}).get("mid_spread_pct"))
        future_close = _finite_float((future or {}).get(close_key))
        gross_capture = (
            future_close - entry_open
            if future_close is not None and entry_open is not None
            else None
        )
        event[f"future_mid_spread_{label}_pct"] = future_mid
        event[f"abs_spread_reverted_{label}"] = (
            abs(future_mid) < abs(mid) if future_mid is not None else None
        )
        event[f"gross_executable_capture_{label}_pct"] = gross_capture
        event[f"net_capture_after_cost_{label}_pct"] = (
            gross_capture - config.estimated_roundtrip_cost_pct
            if gross_capture is not None
            else None
        )
    return event


def _nearest_future(
    series: Sequence[Mapping[str, Any]],
    times: Sequence[int],
    event_ts_ms: int,
    hours: float,
    tolerance_min: float,
) -> Mapping[str, Any] | None:
    target = event_ts_ms + int(hours * 3_600_000)
    index = bisect_left(times, target)
    candidates = series[max(0, index - 2) : min(len(series), index + 3)]
    if not candidates:
        return None
    best = min(candidates, key=lambda row: abs(int(row.get("ts_ms") or 0) - target))
    if abs(int(best.get("ts_ms") or 0) - target) > tolerance_min * 60_000:
        return None
    return best


def summarize_arbitrage_hypotheses(
    events: Sequence[Mapping[str, Any]],
    config: StrategyLabConfig,
) -> list[dict[str, Any]]:
    groups: list[tuple[str, str, list[Mapping[str, Any]]]] = [
        ("all_causal_events", "baseline", list(events)),
        ("spread_0p75_to_1p5", "spread_size", [e for e in events if 0.75 <= abs(float(e["mid_spread_pct"])) < 1.5]),
        ("spread_1p5_to_3", "spread_size", [e for e in events if 1.5 <= abs(float(e["mid_spread_pct"])) < 3.0]),
        ("spread_ge_3", "spread_size", [e for e in events if 3.0 <= abs(float(e["mid_spread_pct"])) < config.hard_invalid_spread_pct]),
        ("velocity_reverting", "velocity", [e for e in events if e.get("velocity_state") == "reverting"]),
        ("velocity_expanding", "velocity", [e for e in events if e.get("velocity_state") == "expanding"]),
        ("funding_aligned", "funding", [e for e in events if e.get("funding_alignment") == "aligned"]),
        ("funding_opposed", "funding", [e for e in events if e.get("funding_alignment") == "opposed"]),
        ("near_funding_le_1h", "funding_timing", [e for e in events if _le(e.get("hours_to_funding"), 1.0)]),
        ("far_from_funding_gt_1h", "funding_timing", [e for e in events if _gt(e.get("hours_to_funding"), 1.0)]),
        ("mark_confirmed", "data_quality", [e for e in events if e.get("mark_confirmation") == "confirmed"]),
        ("mark_divergent", "data_quality", [e for e in events if e.get("mark_confirmation") == "divergent"]),
    ]
    return [_summarize_event_group(slug, family, rows) for slug, family, rows in groups]


def _summarize_event_group(
    slug: str,
    family: str,
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    row: dict[str, Any] = {"group": slug, "family": family, "events": len(events)}
    for label, _hours in HORIZONS_HOURS:
        reversion = [e.get(f"abs_spread_reverted_{label}") for e in events]
        known_reversion = [bool(value) for value in reversion if value is not None]
        gross = [_finite_float(e.get(f"gross_executable_capture_{label}_pct")) for e in events]
        gross_known = [value for value in gross if value is not None]
        net = [_finite_float(e.get(f"net_capture_after_cost_{label}_pct")) for e in events]
        net_known = [value for value in net if value is not None]
        row[f"known_{label}"] = len(net_known)
        row[f"abs_spread_reversion_{label}_pct"] = _ratio_pct(sum(known_reversion), len(known_reversion))
        row[f"median_gross_capture_{label}_pct"] = _median(gross_known)
        row[f"median_net_capture_{label}_pct"] = _median(net_known)
        row[f"net_positive_{label}_pct"] = _ratio_pct(sum(value > 0 for value in net_known), len(net_known))
    return row


def analyze_funding_persistence(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "ca_funding_history"):
        return []
    grouped: dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT canonical_symbol, exchange, ts_ms, funding_rate
        FROM ca_funding_history
        WHERE funding_rate IS NOT NULL
        ORDER BY canonical_symbol, exchange, ts_ms
        """
    ):
        grouped[(str(row[0]), str(row[1]))].append((int(row[2]), float(row[3])))
    transitions: list[dict[str, Any]] = []
    for (symbol, exchange), values in grouped.items():
        for current, future in zip(values, values[1:]):
            if not 0 < future[0] - current[0] <= 12 * 3_600_000:
                continue
            transitions.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "current": current[1],
                    "next": future[1],
                    "bucket": _funding_bucket(current[1]),
                }
            )
    groups: list[tuple[str, str, list[dict[str, Any]]]] = [
        ("all", "all", transitions),
        ("negative", "sign", [row for row in transitions if row["current"] < 0]),
        ("positive", "sign", [row for row in transitions if row["current"] > 0]),
    ]
    for bucket in (
        "le_-1pct",
        "-1_to_-0p3pct",
        "-0p3_to_-0p1pct",
        "-0p1_to_0pct",
        "0_to_0p1pct",
        "0p1_to_0p3pct",
        "0p3_to_1pct",
        "ge_1pct",
    ):
        groups.append((bucket, "magnitude", [row for row in transitions if row["bucket"] == bucket]))
    for exchange in sorted({row["exchange"] for row in transitions}):
        groups.append((f"exchange_{exchange}", "exchange", [row for row in transitions if row["exchange"] == exchange]))
    out: list[dict[str, Any]] = []
    for slug, family, rows in groups:
        if not rows:
            continue
        same_sign = sum(row["current"] * row["next"] > 0 for row in rows)
        weaker = sum(abs(row["next"]) < abs(row["current"]) for row in rows)
        out.append(
            {
                "group": slug,
                "family": family,
                "transitions": len(rows),
                "symbols": len({row["symbol"] for row in rows}),
                "exchanges": len({row["exchange"] for row in rows}),
                "same_sign_pct": _ratio_pct(same_sign, len(rows)),
                "magnitude_weakens_pct": _ratio_pct(weaker, len(rows)),
                "current_median_pct": _median([row["current"] * 100 for row in rows]),
                "next_median_pct": _median([row["next"] * 100 for row in rows]),
            }
        )
    return out


def inventory_operational_logs(log_dir: Path) -> list[dict[str, Any]]:
    groups = {
        "auto_exit": sorted(log_dir.glob("auto_exit_history*.jsonl")),
        "derisk": sorted(log_dir.glob("derisk_history*.jsonl")),
        "auto_arb": [log_dir / "auto_arb_history.jsonl"],
        "protective_shadow": [log_dir / "protective_shadow_history.jsonl"],
    }
    out: list[dict[str, Any]] = []
    for group, paths in groups.items():
        event_counts: Counter[str] = Counter()
        symbol_counts: Counter[str] = Counter()
        lines = 0
        bad_json = 0
        min_ts = ""
        max_ts = ""
        size_bytes = 0
        executable_spreads: list[float] = []
        net_spreads: list[float] = []
        existing = [path for path in paths if path.exists()]
        for path in existing:
            size_bytes += path.stat().st_size
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    lines += 1
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        bad_json += 1
                        continue
                    event_counts[str(payload.get("event") or payload.get("record_type") or "unknown")] += 1
                    symbol = str(payload.get("symbol") or "")
                    if symbol:
                        symbol_counts[symbol] += 1
                    executable_spread = _finite_float(
                        payload.get("pair_spread_pct")
                        if payload.get("pair_spread_pct") is not None
                        else payload.get("spread_pct")
                    )
                    if executable_spread is not None:
                        executable_spreads.append(executable_spread)
                    net_spread = _finite_float(payload.get("net_spread_pct"))
                    if net_spread is not None:
                        net_spreads.append(net_spread)
                    ts = str(payload.get("ts") or "")
                    if ts:
                        min_ts = ts if not min_ts or ts < min_ts else min_ts
                        max_ts = ts if not max_ts or ts > max_ts else max_ts
                    for row in payload.get("rows") or []:
                        if isinstance(row, Mapping) and row.get("symbol"):
                            symbol_counts[str(row["symbol"])] += 1
        out.append(
            {
                "source_family": "ordinary_arbitrage_logs",
                "source": group,
                "records": lines,
                "min_ts": "",
                "max_ts": "",
                "min_iso": min_ts,
                "max_iso": max_ts,
                "notes": json.dumps(
                    {
                        "files": len(existing),
                        "size_mib": round(size_bytes / 1_048_576, 2),
                        "bad_json": bad_json,
                        "top_events": event_counts.most_common(8),
                        "top_symbols": symbol_counts.most_common(12),
                        "executable_spread_samples": len(executable_spreads),
                        "executable_spread_min_pct": min(executable_spreads, default=None),
                        "executable_spread_median_pct": _median(executable_spreads),
                        "executable_spread_max_pct": max(executable_spreads, default=None),
                        "net_spread_samples": len(net_spreads),
                        "net_spread_median_pct": _median(net_spreads),
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
            }
        )
    unstructured = [log_dir / "app.log", *sorted(log_dir.glob("ws_trade_*raw.log"))]
    manual_files = sorted((log_dir / "manual_exec").glob("*.log")) if (log_dir / "manual_exec").exists() else []
    files = [path for path in unstructured if path.exists()] + manual_files
    if files:
        out.append(
            {
                "source_family": "ordinary_arbitrage_logs",
                "source": "runtime_and_execution_file_inventory",
                "records": len(files),
                "min_ts": "",
                "max_ts": "",
                "min_iso": min((datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat() for path in files), default=""),
                "max_iso": max((datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat() for path in files), default=""),
                "notes": json.dumps(
                    {
                        "size_mib": round(sum(path.stat().st_size for path in files) / 1_048_576, 2),
                        "manual_execution_files": len(manual_files),
                        "raw_ws_files": len([path for path in files if path.name.startswith("ws_trade_")]),
                        "routing": "execution latency/fills and raw private streams; not treated as unbiased market events",
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
            }
        )
    return out


def inventory_pump_research(paths: Iterable[Path]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        record_count = _first_number(
            payload,
            "events",
            "samples",
            "unique_cases",
            "hourly_event_rows",
            "selected_outcomes",
        )
        min_iso = str(
            payload.get("actual_case_entry_min_iso")
            or payload.get("start_iso")
            or (payload.get("universe_coverage") or {}).get("first_candle_iso")
            or ""
        )
        max_iso = str(
            payload.get("actual_case_entry_max_iso")
            or (payload.get("universe_coverage") or {}).get("last_candle_iso")
            or payload.get("created_at")
            or ""
        )
        out.append(
            {
                "source_family": "pump_dump_research",
                "source": path.parent.name,
                "records": record_count,
                "min_ts": "",
                "max_ts": "",
                "min_iso": min_iso,
                "max_iso": max_iso,
                "notes": json.dumps(
                    {
                        "schema": payload.get("schema"),
                        "metadata_path": str(path),
                        "limitations": payload.get("limitations") or [],
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
            }
        )
    return out


def inventory_pump_archive(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    files = [item for item in path.rglob("*") if item.is_file()]
    venues = sorted(
        item.name for item in path.iterdir() if item.is_dir() and not item.name.startswith("_")
    )
    return [
        {
            "source_family": "pump_dump_archive",
            "source": path.name,
            "records": len(files),
            "min_ts": "",
            "max_ts": "",
            "min_iso": "2024-01-01T00:00:00+00:00",
            "max_iso": "",
            "notes": json.dumps(
                {
                    "metric": "files_not_market_rows",
                    "size_gib": round(sum(item.stat().st_size for item in files) / 1_073_741_824, 3),
                    "venues": venues,
                    "path": str(path),
                },
                ensure_ascii=True,
                separators=(",", ":"),
            ),
        }
    ]


def load_pump_event_catalog(sources: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Normalize existing Pump/Dump event exports without rewriting source datasets."""

    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for spec in sources:
        path = Path(str(spec.get("path") or ""))
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", newline="") as handle:
            for source_row in csv.DictReader(handle):
                symbol = str(source_row.get(str(spec.get("symbol") or "")) or "").upper()
                ts_ms = _event_timestamp_ms(
                    source_row.get(str(spec.get("ts") or "")),
                    source_row.get(str(spec.get("iso") or "")),
                )
                if not symbol or ts_ms is None:
                    continue
                raw_event_id = str(source_row.get(str(spec.get("event_id") or "")) or "")
                source = str(spec.get("source") or path.parent.name)
                event_id = raw_event_id or f"{symbol}|{ts_ms}"
                dedupe_key = (source, event_id)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                metrics = {
                    field: _finite_float(source_row.get(field))
                    for field in spec.get("metric_fields") or ()
                }
                out.append(
                    {
                        "pump_event_id": f"{source}|{event_id}",
                        "source": source,
                        "event_type": spec.get("event_type"),
                        "symbol": symbol,
                        "ts_ms": ts_ms,
                        "ts_iso": _iso_from_ms(ts_ms),
                        "metrics_json": json.dumps(
                            metrics, ensure_ascii=True, separators=(",", ":"), sort_keys=True
                        ),
                    }
                )
    out.sort(key=lambda row: (int(row["ts_ms"]), str(row["symbol"]), str(row["source"])))
    return out


def link_pump_and_arbitrage_events(
    pump_events: Sequence[Mapping[str, Any]],
    arbitrage_events: Sequence[Mapping[str, Any]],
    *,
    window_hours: float = 6.0,
) -> list[dict[str, Any]]:
    """Create same-symbol temporal links for later matched event-window studies."""

    arb_by_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for event in arbitrage_events:
        arb_by_symbol[str(event.get("symbol") or "").upper()].append(event)
    window_ms = int(window_hours * 3_600_000)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for pump in pump_events:
        pump_ts = int(pump.get("ts_ms") or 0)
        for arb in arb_by_symbol.get(str(pump.get("symbol") or "").upper(), []):
            arb_ts = int(arb.get("ts_ms") or 0)
            delta_ms = arb_ts - pump_ts
            if abs(delta_ms) > window_ms:
                continue
            key = (str(pump.get("pump_event_id") or ""), str(arb.get("event_id") or ""))
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "pump_event_id": key[0],
                    "arbitrage_event_id": key[1],
                    "symbol": pump.get("symbol"),
                    "pump_source": pump.get("source"),
                    "pump_event_type": pump.get("event_type"),
                    "pump_ts_ms": pump_ts,
                    "pump_ts_iso": pump.get("ts_iso"),
                    "arbitrage_ts_ms": arb_ts,
                    "arbitrage_ts_iso": arb.get("ts_iso"),
                    "arb_minus_pump_hours": round(delta_ms / 3_600_000, 6),
                    "arb_mid_spread_pct": arb.get("mid_spread_pct"),
                    "arb_mark_spread_pct": arb.get("mark_spread_pct"),
                    "arb_net_capture_4h_pct": arb.get("net_capture_after_cost_4h_pct"),
                }
            )
    out.sort(key=lambda row: (int(row["pump_ts_ms"]), str(row["arbitrage_event_id"])))
    return out


def enrich_events_from_public_apis(
    events: Sequence[Mapping[str, Any]],
    *,
    max_events: int,
    window_hours: float,
    timeframe: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    import ccxt  # Imported lazily so offline Strategy Lab remains lightweight.

    selected = select_api_enrichment_events(events, max_events=max_events)
    clients: dict[str, Any] = {}
    markets_by_exchange: dict[str, dict[str, Any]] = {}
    samples: list[dict[str, Any]] = []
    summary: list[dict[str, Any]] = []
    window_ms = int(window_hours * 3_600_000)
    for event in selected:
        exchange_samples: list[dict[str, Any]] = []
        for exchange in (str(event.get("left_exchange") or ""), str(event.get("right_exchange") or "")):
            if exchange not in PUBLIC_EXCHANGE_IDS:
                exchange_samples.append({"exchange": exchange, "error": "unsupported_public_exchange"})
                continue
            if exchange not in clients:
                exchange_class = getattr(ccxt, PUBLIC_EXCHANGE_IDS[exchange])
                options = {"defaultType": "future" if exchange == "binance" else "swap"}
                clients[exchange] = exchange_class(
                    {"enableRateLimit": True, "timeout": 30_000, "options": options}
                )
                markets = clients[exchange].load_markets()
                markets_by_exchange[exchange] = {
                    str(market.get("id") or "").upper(): market
                    for market in markets.values()
                    if isinstance(market, Mapping)
                }
            client = clients[exchange]
            market = _resolve_public_market(
                markets_by_exchange[exchange], str(event.get("symbol") or "")
            )
            if not market:
                exchange_samples.append({"exchange": exchange, "error": "symbol_unavailable"})
                continue
            symbol = str(market.get("symbol") or "")
            start_ms = int(event.get("ts_ms") or 0) - window_ms
            end_ms = int(event.get("ts_ms") or 0) + window_ms
            sample = {
                "exchange": exchange,
                "exchange_symbol": symbol,
                "contract_size": _finite_float(market.get("contractSize")),
                "linear": market.get("linear"),
                "inverse": market.get("inverse"),
                "ohlcv": _fetch_public_ohlcv(client, symbol, timeframe, start_ms, end_ms),
                "funding": _fetch_public_funding(client, symbol, start_ms, end_ms),
                "open_interest": _fetch_public_oi(client, symbol, timeframe, start_ms, end_ms),
            }
            exchange_samples.append(sample)
        event_sample = {
            "schema": "strategy_lab_public_event_window_v1",
            "event": dict(event),
            "timeframe": timeframe,
            "window_hours": window_hours,
            "exchanges": exchange_samples,
        }
        samples.append(event_sample)
        summary.append(_summarize_api_sample(event_sample))
    return samples, summary


def select_api_enrichment_events(
    events: Sequence[Mapping[str, Any]], *, max_events: int
) -> list[dict[str, Any]]:
    by_symbol: dict[str, Mapping[str, Any]] = {}
    for event in sorted(events, key=lambda row: abs(float(row.get("mid_spread_pct") or 0.0)), reverse=True):
        symbol = str(event.get("symbol") or "")
        if symbol and symbol not in by_symbol:
            by_symbol[symbol] = event
    selected = list(by_symbol.values())[: max(0, int(max_events))]
    return [dict(row) for row in selected]


def _fetch_public_ohlcv(
    client: Any, symbol: str, timeframe: str, start_ms: int, end_ms: int
) -> list[dict[str, Any]]:
    try:
        raw = client.fetch_ohlcv(symbol, timeframe, since=start_ms, limit=500)
    except Exception as exc:  # pylint: disable=broad-except
        return [{"error": f"{type(exc).__name__}: {exc}"[:300]}]
    rows = []
    for item in raw or []:
        if not item or item[0] is None or not start_ms <= int(item[0]) <= end_ms:
            continue
        rows.append(
            {
                "ts_ms": int(item[0]),
                "open": _finite_float(item[1]),
                "high": _finite_float(item[2]),
                "low": _finite_float(item[3]),
                "close": _finite_float(item[4]),
                "volume": _finite_float(item[5]),
            }
        )
    return rows


def _fetch_public_funding(
    client: Any, symbol: str, start_ms: int, end_ms: int
) -> list[dict[str, Any]]:
    if not client.has.get("fetchFundingRateHistory"):
        return []
    try:
        raw = client.fetch_funding_rate_history(symbol, since=start_ms, limit=200)
    except Exception as exc:  # pylint: disable=broad-except
        return [{"error": f"{type(exc).__name__}: {exc}"[:300]}]
    return [
        {
            "ts_ms": int(item.get("timestamp")),
            "funding_rate": _finite_float(item.get("fundingRate")),
        }
        for item in raw or []
        if item.get("timestamp") is not None and start_ms <= int(item["timestamp"]) <= end_ms
    ]


def _fetch_public_oi(
    client: Any, symbol: str, timeframe: str, start_ms: int, end_ms: int
) -> list[dict[str, Any]]:
    if not client.has.get("fetchOpenInterestHistory"):
        return []
    try:
        raw = client.fetch_open_interest_history(symbol, timeframe, since=start_ms, limit=500)
    except Exception as exc:  # pylint: disable=broad-except
        return [{"error": f"{type(exc).__name__}: {exc}"[:300]}]
    rows = []
    for item in raw or []:
        ts_ms = item.get("timestamp")
        if ts_ms is None or not start_ms <= int(ts_ms) <= end_ms:
            continue
        rows.append(
            {
                "ts_ms": int(ts_ms),
                "open_interest": _finite_float(
                    item.get("openInterestAmount")
                    or item.get("openInterest")
                    or item.get("baseVolume")
                    or item.get("quoteVolume")
                ),
            }
        )
    return rows


def _summarize_api_sample(sample: Mapping[str, Any]) -> dict[str, Any]:
    event = dict(sample.get("event") or {})
    exchanges = [row for row in sample.get("exchanges") or [] if isinstance(row, Mapping)]
    left = next((row for row in exchanges if row.get("exchange") == event.get("left_exchange")), None)
    right = next((row for row in exchanges if row.get("exchange") == event.get("right_exchange")), None)
    event_ts = int(event.get("ts_ms") or 0)
    row: dict[str, Any] = {
        "event_id": event.get("event_id"),
        "symbol": event.get("symbol"),
        "ts_ms": event_ts,
        "ts_iso": event.get("ts_iso"),
        "left_exchange": event.get("left_exchange"),
        "right_exchange": event.get("right_exchange"),
        "local_mid_spread_pct": event.get("mid_spread_pct"),
        "left_contract_size": (left or {}).get("contract_size"),
        "right_contract_size": (right or {}).get("contract_size"),
        "left_ohlcv_rows": len(_valid_series((left or {}).get("ohlcv"))),
        "right_ohlcv_rows": len(_valid_series((right or {}).get("ohlcv"))),
        "left_funding_rows": len(_valid_series((left or {}).get("funding"))),
        "right_funding_rows": len(_valid_series((right or {}).get("funding"))),
        "left_oi_rows": len(_valid_series((left or {}).get("open_interest"))),
        "right_oi_rows": len(_valid_series((right or {}).get("open_interest"))),
    }
    left_close = _series_value_near((left or {}).get("ohlcv"), event_ts, "close")
    right_close = _series_value_near((right or {}).get("ohlcv"), event_ts, "close")
    api_spread = _symmetric_spread_pct(left_close, right_close)
    row["api_anchor_close_spread_pct"] = api_spread
    row["api_local_abs_error_pct"] = (
        abs(api_spread - float(event["mid_spread_pct"]))
        if api_spread is not None and event.get("mid_spread_pct") is not None
        else None
    )
    for label, hours in HORIZONS_HOURS:
        target = event_ts + int(hours * 3_600_000)
        left_future = _series_value_near((left or {}).get("ohlcv"), target, "close")
        right_future = _series_value_near((right or {}).get("ohlcv"), target, "close")
        row[f"api_close_spread_{label}_pct"] = _symmetric_spread_pct(left_future, right_future)
    row["api_validation"] = (
        "confirmed"
        if row["api_local_abs_error_pct"] is not None
        and row["api_local_abs_error_pct"] <= API_EXACT_MATCH_TOLERANCE_PCT
        else "within_2pct_tolerance"
        if row["api_local_abs_error_pct"] is not None
        and row["api_local_abs_error_pct"] <= API_SOFT_MATCH_TOLERANCE_PCT
        else "divergent"
        if row["api_local_abs_error_pct"] is not None
        else "incomplete"
    )
    return row


def build_hypothesis_registry(
    *,
    arbitrage_summary: Sequence[Mapping[str, Any]],
    funding_summary: Sequence[Mapping[str, Any]],
    pump_inventory: Sequence[Mapping[str, Any]],
    pump_events: Sequence[Mapping[str, Any]],
    pump_arb_links: Sequence[Mapping[str, Any]],
    api_summary: Sequence[Mapping[str, Any]],
    config: StrategyLabConfig,
) -> list[dict[str, Any]]:
    arb_all = _row_by_group(arbitrage_summary, "all_causal_events")
    arb_large = _row_by_group(arbitrage_summary, "spread_ge_3")
    near = _row_by_group(arbitrage_summary, "near_funding_le_1h")
    far = _row_by_group(arbitrage_summary, "far_from_funding_gt_1h")
    negative = _row_by_group(funding_summary, "negative")
    extreme_negative = _row_by_group(funding_summary, "le_-1pct")
    pump_records = sum(
        int(row.get("records") or 0)
        for row in pump_inventory
        if row.get("source_family") == "pump_dump_research"
    )
    return [
        _hypothesis(
            1,
            "arb_executable_spread_filter",
            "Arbitrage entry",
            "A raw mid-spread threshold is insufficient; require executable bid/ask capture, mark confirmation and costs.",
            f"{arb_all.get('events', 0)} causal anchors; 4h net-positive share after {config.estimated_roundtrip_cost_pct:.2f}% cost = {arb_all.get('net_positive_4h_pct')}%.",
            "ready_for_filter_sweep",
            "Walk-forward sweep of executable spread, liquidity, mark gap and entry delay; paper only.",
        ),
        _hypothesis(
            2,
            "funding_sign_persistence",
            "Funding forecast",
            "The current funding sign predicts the next scheduled sign, especially for negative extremes.",
            f"Negative transitions={negative.get('transitions', 0)}, same sign={negative.get('same_sign_pct')}%; <=-1% transitions={extreme_negative.get('transitions', 0)}, same sign={extreme_negative.get('same_sign_pct')}%.",
            "ready_for_walk_forward",
            "Calibrate by exchange, interval and symbol with chronological splits; predict sign and magnitude separately.",
        ),
        _hypothesis(
            3,
            "arb_large_dislocation_delayed_reversion",
            "Arbitrage entry/exit",
            "Large confirmed dislocations may need delayed entry and wider holding windows rather than immediate market entry.",
            f">=3% events={arb_large.get('events', 0)}; median 4h net capture={arb_large.get('median_net_capture_4h_pct')}%, net-positive={arb_large.get('net_positive_4h_pct')}%.",
            "needs_timing_sweep",
            "Test 5m/15m/30m confirmation, maximum adverse excursion and maker-first execution.",
        ),
        _hypothesis(
            4,
            "pre_funding_spread_regime",
            "Arbitrage timing",
            "Time to the next funding boundary may separate compression from continuation regimes.",
            f"Near-boundary events={near.get('events', 0)}, 1h net-positive={near.get('net_positive_1h_pct')}%; far events={far.get('events', 0)}, 1h net-positive={far.get('net_positive_1h_pct')}%.",
            "needs_matched_control",
            "Match by symbol and spread magnitude so funding timing is not confounded by coin selection.",
        ),
        _hypothesis(
            5,
            "cross_exchange_lead_lag",
            "Price forecast",
            "The exchange that moves first, with volume/OI confirmation, may forecast whether the spread converges or propagates.",
            f"Public API enriched anchors={len(api_summary)}; Binance OI is available while KuCoin historical OI is usually absent.",
            "needs_api_windows",
            "Estimate lagged 5m returns and notional-volume shocks across Binance, KuCoin, Bybit and OKX.",
        ),
        _hypothesis(
            6,
            "pump_to_arbitrage_bridge",
            "Cross-strategy",
            "A pump first visible on one venue may create a temporary cross-exchange basis before the broader exhaustion signal.",
            f"Normalized Pump rows={len(pump_events)} (metadata record floor={pump_records}); same-symbol +/-6h links={len(pump_arb_links)}.",
            "event_join_built",
            "Enrich linked windows and add matched no-pump controls before testing lead venue and convergence path.",
        ),
        _hypothesis(
            7,
            "pump_short_exhaustion_multivenue",
            "Pump short",
            "Short exhaustion quality should improve when premium relief, OI fade and cross-venue price confirmation agree.",
            "Existing Pump/Dump lifecycle and transition studies provide the base event set; multi-venue confirmation is incomplete.",
            "extend_existing_research",
            "Re-score existing 126 live-like cases without changing Pump Live rules.",
        ),
        _hypothesis(
            8,
            "pump_long_discount_absorption",
            "Pump long",
            "Deep negative premium plus non-falling OI and volume absorption may identify post-liquidation rebounds.",
            "Existing 5m candidate research has a promising but selected 52-event sample.",
            "shadow_candidate_only",
            "Rebuild on all eligible events and unseen dates; preserve failed-absorption vetoes.",
        ),
        _hypothesis(
            9,
            "funding_extreme_magnitude_decay",
            "Funding forecast",
            "Extreme funding often keeps its sign while its absolute magnitude decays.",
            f"<=-1% magnitude weakens next interval in {extreme_negative.get('magnitude_weakens_pct')}% of {extreme_negative.get('transitions', 0)} transitions.",
            "ready_for_two_stage_model",
            "Model next sign first, then conditional magnitude/relief; include exchange funding caps and interval changes.",
        ),
        _hypothesis(
            10,
            "data_quality_as_strategy_veto",
            "Safety",
            "Contract multipliers, zero/stale quotes and mid/mark conflicts can mimic extraordinary arbitrage.",
            "Local logs contain impossible 200% spreads and exchange-specific contract sizes such as COTI 1 vs 10.",
            "mandatory_veto",
            "Reject unconfirmed mappings before any backtest, shadow alert or automated entry.",
        ),
        _hypothesis(
            11,
            "liquidity_shock_reversion",
            "Microstructure",
            "Order-book depth collapse may distinguish executable dislocation from a last-price illusion.",
            "Auto-exit logs contain executable books and 52k spread observations, but are conditional on existing positions.",
            "needs_episode_extraction",
            "Build position-lifecycle episodes and compare depth/slippage before trigger, wait and actual execution.",
        ),
        _hypothesis(
            12,
            "regime_aware_arbitrage_automation",
            "Automation",
            "Automation should choose ENTER/HOLD/EXIT from calibrated spread and funding forecasts, not one static grid.",
            "Current evidence is rich but selection-biased: tracked positions and pump-prefiltered symbols dominate.",
            "research_then_paper",
            "Chronological train/test, symbol holdout, fees/slippage, capacity and fail-closed data-quality gates.",
        ),
    ]


def render_markdown_report(
    *,
    metadata: Mapping[str, Any],
    source_inventory: Sequence[Mapping[str, Any]],
    arbitrage_summary: Sequence[Mapping[str, Any]],
    funding_summary: Sequence[Mapping[str, Any]],
    hypotheses: Sequence[Mapping[str, Any]],
    pump_events: Sequence[Mapping[str, Any]],
    pump_arb_links: Sequence[Mapping[str, Any]],
    api_summary: Sequence[Mapping[str, Any]],
) -> str:
    arb = _row_by_group(arbitrage_summary, "all_causal_events")
    negative = _row_by_group(funding_summary, "negative")
    extreme = _row_by_group(funding_summary, "le_-1pct")
    db_focus = next((row for row in source_inventory if row.get("source") == "ca_market_snapshots_focus"), {})
    db_features = next((row for row in source_inventory if row.get("source") == "ca_feature_snapshots"), {})
    pump_sources = [row for row in source_inventory if row.get("source_family") == "pump_dump_research"]
    pump_archives = [row for row in source_inventory if row.get("source_family") == "pump_dump_archive"]
    api_exact = sum(row.get("api_validation") == "confirmed" for row in api_summary)
    api_soft = sum(row.get("api_validation") == "within_2pct_tolerance" for row in api_summary)
    lines = [
        "# Strategy Lab — phase 1 evidence map",
        "",
        "Status: research only. This report does not arm strategies, place orders, or change live rules.",
        "",
        "## Data map",
        "",
        f"- Ordinary arbitrage DB: {db_focus.get('records', 0)} focus snapshots and {db_features.get('records', 0)} feature snapshots.",
        f"- Pump/Dump research packages discovered: {len(pump_sources)}.",
        f"- Multi-exchange Pump archives registered: {len(pump_archives)}.",
        f"- Normalized historical Pump event rows: {len(pump_events)}; same-symbol +/-6h arbitrage links: {len(pump_arb_links)}.",
        f"- Causal arbitrage event anchors: {metadata.get('arbitrage_events', 0)}; data-quality rejections: {metadata.get('rejected_data_quality_rows', 0)}.",
        f"- Public API enriched anchors: {len(api_summary)}.",
        f"- API agreement: {api_exact} exact (<=0.75 percentage points), {api_soft} within the requested 0-2 percentage-point tolerance band.",
        "",
        "## First findings",
        "",
        f"1. Raw spread mean reversion is not an executable strategy. Across {arb.get('events', 0)} first-observable triggers, absolute spread reverted in {arb.get('abs_spread_reversion_4h_pct')}% of known 4h cases, but only {arb.get('net_positive_4h_pct')}% remained positive after executable bid/ask and the configured cost estimate. Median net capture was {arb.get('median_net_capture_4h_pct')}%.",
        f"2. Funding sign persistence is materially stronger: negative funding kept its sign in {negative.get('same_sign_pct')}% of {negative.get('transitions', 0)} next observations. For current funding <= -1%, sign persistence was {extreme.get('same_sign_pct')}% across {extreme.get('transitions', 0)} transitions, while magnitude weakened in {extreme.get('magnitude_weakens_pct')}%.",
        "3. Impossible 200% spreads and mid/mark conflicts prove that contract mapping and quote-quality checks must be hard vetoes before research rows become signals.",
        "4. Ordinary logs are conditional on coins already selected or held. Pump history is also prefiltered/current-listing biased. Every promising rule therefore needs chronological and symbol-held-out validation.",
        "",
        "## Ranked hypotheses",
        "",
        "| Priority | Hypothesis | Family | Stage | Evidence | Next test |",
        "|---:|---|---|---|---|---|",
    ]
    for row in hypotheses:
        lines.append(
            "| {priority} | `{hypothesis_id}` | {family} | {stage} | {evidence} | {next_test} |".format(
                **{key: str(value).replace("|", "/") for key, value in row.items()}
            )
        )
    lines.extend(
        [
            "",
            "## Automation gate",
            "",
            "1. Rebuild event windows without look-ahead and validate public API agreement.",
            "2. Use chronological train/validation/test and leave whole symbols out of training.",
            "3. Evaluate executable prices, funding cashflows, fees, slippage, liquidity and capacity.",
            "4. Promote only stable hypotheses to paper, then shadow. Live automation requires a separate operator decision.",
            "",
            "## Generated artifacts",
            "",
            "- `source_inventory.csv`",
            "- `arbitrage_spread_events.csv`",
            "- `arbitrage_rejected_data_quality.csv`",
            "- `arbitrage_hypothesis_summary.csv`",
            "- `funding_persistence_summary.csv`",
            "- `pump_event_catalog.csv`",
            "- `pump_arbitrage_event_links.csv`",
            "- `arbitrage_api_windows.jsonl` and `arbitrage_api_summary.csv` when API enrichment is enabled",
            "- `hypothesis_registry.csv`",
            "- `metadata.json`",
            "",
        ]
    )
    return "\n".join(lines)


def _hypothesis(
    priority: int,
    hypothesis_id: str,
    family: str,
    thesis: str,
    evidence: str,
    stage: str,
    next_test: str,
) -> dict[str, Any]:
    return {
        "priority": priority,
        "hypothesis_id": hypothesis_id,
        "family": family,
        "thesis": thesis,
        "evidence": evidence,
        "stage": stage,
        "next_test": next_test,
        "live_allowed": False,
    }


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _velocity_state(mid: float, velocity: Any) -> str:
    value = _finite_float(velocity)
    if value is None or abs(value) <= 1e-12:
        return "flat_or_missing"
    return "expanding" if mid * value > 0 else "reverting"


def _funding_bucket(rate: float) -> str:
    pct = rate * 100.0
    if pct <= -1.0:
        return "le_-1pct"
    if pct <= -0.3:
        return "-1_to_-0p3pct"
    if pct <= -0.1:
        return "-0p3_to_-0p1pct"
    if pct < 0:
        return "-0p1_to_0pct"
    if pct < 0.1:
        return "0_to_0p1pct"
    if pct < 0.3:
        return "0p1_to_0p3pct"
    if pct < 1.0:
        return "0p3_to_1pct"
    return "ge_1pct"


def _resolve_public_market(markets: Mapping[str, Any], canonical_symbol: str) -> Mapping[str, Any] | None:
    direct = markets.get(canonical_symbol.upper())
    if isinstance(direct, Mapping):
        return direct
    for market in markets.values():
        if not isinstance(market, Mapping):
            continue
        joined = f"{str(market.get('base') or '').upper()}{str(market.get('quote') or '').upper()}"
        if joined == canonical_symbol.upper() and (market.get("swap") or market.get("future")):
            return market
    return None


def _valid_series(value: Any) -> list[Mapping[str, Any]]:
    return [row for row in value or [] if isinstance(row, Mapping) and not row.get("error")]


def _series_value_near(series: Any, target_ts: int, key: str) -> float | None:
    rows = _valid_series(series)
    if not rows:
        return None
    best = min(rows, key=lambda row: abs(int(row.get("ts_ms") or 0) - target_ts))
    if abs(int(best.get("ts_ms") or 0) - target_ts) > 10 * 60_000:
        return None
    return _finite_float(best.get(key))


def _symmetric_spread_pct(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    midpoint = (left + right) / 2.0
    if abs(midpoint) <= 1e-12:
        return None
    return (left - right) / midpoint * 100.0


def _dedupe_rejections(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (str(row.get("symbol") or ""), str(row.get("reason") or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(row))
    return out


def _row_by_group(rows: Sequence[Mapping[str, Any]], group: str) -> dict[str, Any]:
    return dict(next((row for row in rows if row.get("group") == group), {}))


def _first_number(payload: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, (int, float)):
            return int(value)
    return 0


def _pair_part(pair_key: Any, index: int) -> str:
    parts = str(pair_key or "").split("|")
    return parts[index] if len(parts) > index else ""


def _finite_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _le(value: Any, limit: float) -> bool:
    number = _finite_float(value)
    return number is not None and number <= limit


def _gt(value: Any, limit: float) -> bool:
    number = _finite_float(value)
    return number is not None and number > limit


def _ratio_pct(numerator: int | float, denominator: int) -> float | None:
    return round(float(numerator) / denominator * 100.0, 2) if denominator else None


def _median(values: Sequence[float]) -> float | None:
    return round(float(statistics.median(values)), 6) if values else None


def _iso_from_ms(value: Any) -> str:
    try:
        if value is None or int(value) <= 0:
            return ""
        return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return ""


def _event_timestamp_ms(raw_ts: Any, raw_iso: Any) -> int | None:
    value = _finite_float(raw_ts)
    if value is not None and value > 0:
        return int(value)
    if not raw_iso:
        return None
    try:
        return int(datetime.fromisoformat(str(raw_iso).replace("Z", "+00:00")).timestamp() * 1000)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    fieldnames: Sequence[str] | None = None,
) -> None:
    materialized = [dict(row) for row in rows]
    resolved_fields = list(fieldnames or ())
    if not resolved_fields:
        resolved_fields = list(dict.fromkeys(key for row in materialized for key in row))
    if not resolved_fields:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=resolved_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=True, separators=(",", ":"), sort_keys=True))
            handle.write("\n")


__all__ = [
    "DEFAULT_DB_PATH",
    "DEFAULT_LOG_DIR",
    "DEFAULT_OUTPUT_DIR",
    "StrategyLabConfig",
    "analyze_funding_persistence",
    "build_hypothesis_registry",
    "extract_arbitrage_events",
    "inventory_operational_logs",
    "link_pump_and_arbitrage_events",
    "load_arbitrage_feature_rows",
    "load_pump_event_catalog",
    "run_strategy_lab",
    "select_api_enrichment_events",
    "summarize_arbitrage_hypotheses",
]
