from __future__ import annotations

import math
import sqlite3
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from analysis_features.strategy_lab import (
    DEFAULT_DB_PATH,
    StrategyLabConfig,
    extract_arbitrage_events,
)
from analysis_features.strategy_lab_event_lake import (
    stable_hash,
    write_csv,
    write_json_atomic,
)


DEFAULT_OUTPUT_DIR = Path("data/research/strategy_lab_executable_spread_v1")
REPORT_SCHEMA = "strategy_lab_executable_spread_report_v1"
MINUTE_MS = 60_000


@dataclass(frozen=True, slots=True)
class ExecutableSpreadConfig:
    source_max_ts_ms: int | None = None
    entry_delays_min: tuple[int, ...] = (0, 5, 15, 30)
    outcome_horizons_min: tuple[int, ...] = (15, 60, 240, 480)
    feature_match_tolerance_min: float = 3.0
    quote_max_skew_ms: int = 15_000
    exact_price_match_tolerance_pct: float = 0.75
    soft_price_match_tolerance_pct: float = 2.0
    fee_roundtrip_pct: float = 0.12
    slippage_roundtrip_scenario_pct: float = 0.06
    expansion_stop_max_wait_min: int = 30
    expansion_relief_pct_points: float = 0.10
    trigger_abs_spread_pct: float = 0.75
    hard_invalid_spread_pct: float = 30.0
    min_coverage_pct: float = 70.0
    min_spread_points: int = 30
    event_cooldown_h: float = 4.0

    def validate(self) -> None:
        if not self.entry_delays_min or self.entry_delays_min[0] != 0:
            raise ValueError("entry delays must start at zero")
        if tuple(sorted(set(self.entry_delays_min))) != self.entry_delays_min:
            raise ValueError("entry delays must be sorted and unique")
        if not self.outcome_horizons_min or any(
            value <= 0 for value in self.outcome_horizons_min
        ):
            raise ValueError("outcome horizons must be positive")
        if self.feature_match_tolerance_min <= 0 or self.quote_max_skew_ms <= 0:
            raise ValueError("matching tolerances must be positive")
        if not (
            0 < self.exact_price_match_tolerance_pct
            <= self.soft_price_match_tolerance_pct
        ):
            raise ValueError("price matching tolerances are invalid")
        if self.fee_roundtrip_pct < 0 or self.slippage_roundtrip_scenario_pct < 0:
            raise ValueError("cost scenarios cannot be negative")
        if self.expansion_stop_max_wait_min < 1 or self.expansion_relief_pct_points <= 0:
            raise ValueError("expansion-stop configuration is invalid")
        if self.source_max_ts_ms is not None and self.source_max_ts_ms < 1:
            raise ValueError("source cutoff must be positive")


def run_executable_spread_timing(
    *,
    db_path: Path = DEFAULT_DB_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    config: ExecutableSpreadConfig | None = None,
    code_commit: str = "",
) -> dict[str, Any]:
    cfg = config or ExecutableSpreadConfig()
    cfg.validate()
    started = time.time()
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        source_snapshot = build_source_snapshot(conn, max_ts_ms=cfg.source_max_ts_ms)
        feature_rows = load_timing_feature_rows(conn, max_ts_ms=cfg.source_max_ts_ms)
        strategy_config = StrategyLabConfig(
            trigger_abs_spread_pct=cfg.trigger_abs_spread_pct,
            hard_invalid_spread_pct=cfg.hard_invalid_spread_pct,
            min_coverage_pct=cfg.min_coverage_pct,
            min_spread_points=cfg.min_spread_points,
            event_cooldown_h=cfg.event_cooldown_h,
        )
        events, source_rejections = extract_arbitrage_events(
            feature_rows, strategy_config
        )
        rows = evaluate_entry_policies(
            conn=conn,
            events=events,
            feature_rows=feature_rows,
            config=cfg,
        )
    finally:
        conn.close()

    summaries = summarize_results(rows)
    veto_summary = [
        {"reason": reason, "rows": count}
        for reason, count in sorted(
            Counter(
                str(row.get("veto_reason") or "unknown")
                for row in rows
                if row.get("status") == "VETO"
            ).items()
        )
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "timing_outcomes.csv", rows)
    write_csv(output_dir / "timing_summary.csv", summaries)
    write_csv(output_dir / "veto_summary.csv", veto_summary)
    write_csv(output_dir / "source_quality_rejections.csv", source_rejections)

    evaluated = [row for row in rows if row.get("status") == "EVALUATED"]
    metadata = {
        "schema": REPORT_SCHEMA,
        "mode": "research_replay",
        "live_actions": False,
        "paper_promotion_allowed": False,
        "shadow_promotion_allowed": False,
        "decision_status": "research_evaluation_only",
        "db_path": str(db_path),
        "code_commit": code_commit,
        "config": asdict(cfg),
        "config_hash": stable_hash(asdict(cfg)),
        "source_snapshot": source_snapshot,
        "source_snapshot_id": stable_hash(source_snapshot),
        "feature_rows": len(feature_rows),
        "causal_events": len(events),
        "source_quality_rejections": len(source_rejections),
        "outcome_rows": len(rows),
        "evaluated_rows": len(evaluated),
        "vetoed_rows": len(rows) - len(evaluated),
        "symbols": len({str(row.get("symbol") or "") for row in rows}),
        "capacity_usd_known_rows": sum(
            row.get("capacity_usd") is not None for row in evaluated
        ),
        "actual_funding_rows": sum(
            int(row.get("funding_settlements") or 0) > 0 for row in evaluated
        ),
        "price_source": "historical_top_of_book_bid_ask",
        "capacity_status": "fail_closed_without_contract_multiplier",
        "slippage_status": "fixed_scenario_until_historical_depth_exists",
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(output_dir / "metadata.json", metadata)
    (output_dir / "index.md").write_text(
        render_report(metadata, summaries, veto_summary), encoding="utf-8"
    )
    return metadata


def build_source_snapshot(
    conn: sqlite3.Connection,
    *,
    max_ts_ms: int | None,
) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    for table in (
        "ca_feature_snapshots",
        "ca_market_snapshots_focus",
        "ca_funding_history",
    ):
        where = " WHERE ts_ms <= ?" if max_ts_ms is not None else ""
        parameters = (max_ts_ms,) if max_ts_ms is not None else ()
        row = conn.execute(
            f"SELECT COUNT(*) AS rows, MIN(ts_ms) AS min_ts_ms, MAX(ts_ms) AS max_ts_ms FROM {table}{where}",
            parameters,
        ).fetchone()
        snapshot[table] = {
            "rows": int(row["rows"] or 0),
            "min_ts_ms": row["min_ts_ms"],
            "max_ts_ms": row["max_ts_ms"],
        }
    return snapshot


def load_timing_feature_rows(
    conn: sqlite3.Connection,
    *,
    max_ts_ms: int | None,
) -> list[dict[str, Any]]:
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
               json_extract(features_json, '$.common.funding.time_to_next_funding_hours_left') AS left_time_to_funding_h,
               json_extract(features_json, '$.common.funding.time_to_next_funding_hours_right') AS right_time_to_funding_h,
               json_extract(features_json, '$.common.hours_to_next_funding_min') AS hours_to_funding,
               json_extract(features_json, '$.common.oi.left_change_6h_pct') AS left_oi_change_6h_pct,
               json_extract(features_json, '$.common.oi.right_change_6h_pct') AS right_oi_change_6h_pct,
               json_extract(data_quality_json, '$.coverage_pct') AS coverage_pct,
               json_extract(data_quality_json, '$.spread_points_total') AS spread_points
        FROM ca_feature_snapshots
        WHERE direction = 'long_a_short_b'
          AND (? IS NULL OR ts_ms <= ?)
        ORDER BY pair_key, ts_ms
    """
    return [dict(row) for row in conn.execute(query, (max_ts_ms, max_ts_ms))]


def evaluate_entry_policies(
    *,
    conn: sqlite3.Connection,
    events: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
    config: ExecutableSpreadConfig,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in feature_rows:
        grouped[str(raw.get("pair_key") or "")].append(dict(raw))
    for series in grouped.values():
        series.sort(key=lambda row: int(row.get("ts_ms") or 0))

    event_times = sorted({int(event.get("ts_ms") or 0) for event in events})
    cut_one = event_times[max(0, len(event_times) // 3 - 1)] if event_times else 0
    cut_two = event_times[max(0, (2 * len(event_times)) // 3 - 1)] if event_times else 0
    output: list[dict[str, Any]] = []
    policies: list[tuple[str, int | None]] = [
        ("now" if delay == 0 else f"delay_{delay}m", delay)
        for delay in config.entry_delays_min
    ]
    policies.append(("expansion_stop", None))

    for event in events:
        pair_key = str(event.get("pair_key") or "")
        series = grouped.get(pair_key, [])
        event_ts = int(event.get("ts_ms") or 0)
        direction = str(event.get("reversion_direction") or "")
        segment = (
            "chronological_1"
            if event_ts <= cut_one
            else "chronological_2"
            if event_ts <= cut_two
            else "chronological_3"
        )
        for policy, delay in policies:
            if delay is None:
                entry = select_expansion_stop_entry(
                    series, event_ts=event_ts, config=config
                )
            else:
                entry = nearest_row(
                    series,
                    event_ts + delay * MINUTE_MS,
                    tolerance_ms=int(config.feature_match_tolerance_min * MINUTE_MS),
                )
            entry_veto = validate_entry(entry, event=event, config=config)
            entry_ts = int(entry.get("ts_ms") or 0) if entry else None
            quote_a = quote_b = None
            quote_veto = ""
            entry_open = None
            price_match = "missing"
            price_match_diff = None
            capacity_status = "unavailable"
            raw_long_size = raw_short_size = None
            if not entry_veto and entry_ts is not None:
                quote_a = latest_quote(
                    conn,
                    symbol=str(event.get("symbol") or ""),
                    exchange=str(event.get("left_exchange") or ""),
                    ts_ms=entry_ts,
                    max_skew_ms=config.quote_max_skew_ms,
                )
                quote_b = latest_quote(
                    conn,
                    symbol=str(event.get("symbol") or ""),
                    exchange=str(event.get("right_exchange") or ""),
                    ts_ms=entry_ts,
                    max_skew_ms=config.quote_max_skew_ms,
                )
                quote_veto = validate_quotes(quote_a, quote_b)
                if not quote_veto:
                    entry_open = directional_spread(
                        quote_a, quote_b, direction=direction, action="open"
                    )
                    feature_open = finite_float(
                        entry.get("open_ab_pct" if direction == "long_a_short_b" else "open_ba_pct")
                    )
                    price_match, price_match_diff = classify_price_match(
                        entry_open,
                        feature_open,
                        exact_tolerance_pct=config.exact_price_match_tolerance_pct,
                        soft_tolerance_pct=config.soft_price_match_tolerance_pct,
                    )
                    if price_match == "divergent":
                        quote_veto = "top_of_book_feature_price_divergent"
                    raw_long_size, raw_short_size = directional_sizes(
                        quote_a, quote_b, direction
                    )
                    capacity_status = (
                        "contract_multiplier_missing"
                        if raw_long_size is not None and raw_short_size is not None
                        else "top_size_missing"
                    )

            combined_veto = entry_veto or quote_veto
            for horizon_min in config.outcome_horizons_min:
                base = {
                    "event_id": event.get("event_id"),
                    "event_ts_ms": event_ts,
                    "event_ts_iso": event.get("ts_iso"),
                    "chronological_segment": segment,
                    "symbol": event.get("symbol"),
                    "pair_key": pair_key,
                    "left_exchange": event.get("left_exchange"),
                    "right_exchange": event.get("right_exchange"),
                    "direction": direction,
                    "entry_policy": policy,
                    "requested_delay_min": delay,
                    "entry_ts_ms": entry_ts,
                    "actual_delay_min": (
                        (entry_ts - event_ts) / MINUTE_MS
                        if entry_ts is not None
                        else None
                    ),
                    "horizon_min": horizon_min,
                    "entry_mid_spread_pct": finite_float((entry or {}).get("mid_spread_pct")),
                    "entry_open_spread_pct": entry_open,
                    "feature_entry_open_spread_pct": finite_float(
                        (entry or {}).get(
                            "open_ab_pct"
                            if direction == "long_a_short_b"
                            else "open_ba_pct"
                        )
                    ),
                    "top_of_book_match": price_match,
                    "top_of_book_match_diff_pct": price_match_diff,
                    "raw_long_top_size": raw_long_size,
                    "raw_short_top_size": raw_short_size,
                    "capacity_usd": None,
                    "capacity_status": capacity_status,
                    "fee_cost_pct": config.fee_roundtrip_pct,
                    "slippage_scenario_pct": config.slippage_roundtrip_scenario_pct,
                    "slippage_model": "fixed_scenario_no_historical_depth",
                    "execution_ready": False,
                    "promotion_blockers": "capacity_usd_unknown|historical_depth_missing",
                    "status": "VETO" if combined_veto else "EVALUATED",
                    "veto_reason": combined_veto,
                }
                if combined_veto or entry_ts is None:
                    output.append(base)
                    continue
                exit_ts = entry_ts + horizon_min * MINUTE_MS
                exit_row = nearest_row(
                    series,
                    exit_ts,
                    tolerance_ms=int(config.feature_match_tolerance_min * MINUTE_MS),
                )
                if exit_row is None:
                    base.update(status="VETO", veto_reason="exit_feature_missing")
                    output.append(base)
                    continue
                quote_exit_a = latest_quote(
                    conn,
                    symbol=str(event.get("symbol") or ""),
                    exchange=str(event.get("left_exchange") or ""),
                    ts_ms=int(exit_row["ts_ms"]),
                    max_skew_ms=config.quote_max_skew_ms,
                )
                quote_exit_b = latest_quote(
                    conn,
                    symbol=str(event.get("symbol") or ""),
                    exchange=str(event.get("right_exchange") or ""),
                    ts_ms=int(exit_row["ts_ms"]),
                    max_skew_ms=config.quote_max_skew_ms,
                )
                exit_veto = validate_quotes(quote_exit_a, quote_exit_b)
                if exit_veto:
                    base.update(status="VETO", veto_reason=f"exit_{exit_veto}")
                    output.append(base)
                    continue
                exit_close = directional_spread(
                    quote_exit_a, quote_exit_b, direction=direction, action="close"
                )
                if exit_close is None or entry_open is None:
                    base.update(status="VETO", veto_reason="directional_price_missing")
                    output.append(base)
                    continue
                funding = funding_cashflow(
                    conn,
                    symbol=str(event.get("symbol") or ""),
                    left_exchange=str(event.get("left_exchange") or ""),
                    right_exchange=str(event.get("right_exchange") or ""),
                    direction=direction,
                    entry_ts_ms=entry_ts,
                    exit_ts_ms=int(exit_row["ts_ms"]),
                    entry_row=entry,
                )
                if funding["status"] != "complete":
                    base.update(status="VETO", veto_reason=str(funding["reason"]))
                    output.append(base)
                    continue
                gross = exit_close - entry_open
                net = (
                    gross
                    + float(funding["cashflow_pct"])
                    - config.fee_roundtrip_pct
                    - config.slippage_roundtrip_scenario_pct
                )
                path = path_metrics(
                    series,
                    entry_ts_ms=entry_ts,
                    exit_ts_ms=int(exit_row["ts_ms"]),
                    entry_open_spread_pct=entry_open,
                    direction=direction,
                    total_cost_pct=(
                        config.fee_roundtrip_pct
                        + config.slippage_roundtrip_scenario_pct
                    ),
                )
                base.update(
                    exit_ts_ms=int(exit_row["ts_ms"]),
                    exit_close_spread_pct=exit_close,
                    gross_capture_pct=gross,
                    funding_cashflow_pct=funding["cashflow_pct"],
                    funding_settlements=funding["settlements"],
                    net_capture_pct=net,
                    net_positive=net > 0,
                    mae_pct=path["mae_pct"],
                    mfe_pct=path["mfe_pct"],
                    time_to_cost_breakeven_min=path["time_to_cost_breakeven_min"],
                )
                output.append(base)
    return output


def validate_entry(
    row: Mapping[str, Any] | None,
    *,
    event: Mapping[str, Any],
    config: ExecutableSpreadConfig,
) -> str:
    if row is None:
        return "entry_feature_missing"
    mid = finite_float(row.get("mid_spread_pct"))
    if mid is None:
        return "entry_mid_missing"
    if abs(mid) < config.trigger_abs_spread_pct:
        return "spread_below_entry_threshold"
    if abs(mid) >= config.hard_invalid_spread_pct:
        return "hard_invalid_spread_or_mapping"
    direction = "long_b_short_a" if mid > 0 else "long_a_short_b"
    if direction != event.get("reversion_direction"):
        return "direction_flipped_before_entry"
    mark = finite_float(row.get("mark_spread_pct"))
    if mark is None:
        return "mark_spread_missing"
    if mid * mark <= 0:
        return "mid_mark_direction_conflict"
    if abs(mid - mark) > max(0.5, abs(mid) * 0.5):
        return "mid_mark_gap_too_large"
    open_key = "open_ab_pct" if direction == "long_a_short_b" else "open_ba_pct"
    if finite_float(row.get(open_key)) is None:
        return "entry_bid_ask_spread_missing"
    return ""


def select_expansion_stop_entry(
    series: Sequence[Mapping[str, Any]],
    *,
    event_ts: int,
    config: ExecutableSpreadConfig,
) -> Mapping[str, Any] | None:
    anchor = nearest_row(series, event_ts, tolerance_ms=1)
    anchor_mid = finite_float((anchor or {}).get("mid_spread_pct"))
    if anchor_mid is None:
        return None
    sign = 1 if anchor_mid > 0 else -1
    peak = abs(anchor_mid)
    end_ts = event_ts + config.expansion_stop_max_wait_min * MINUTE_MS
    for row in series:
        ts_ms = int(row.get("ts_ms") or 0)
        if ts_ms <= event_ts or ts_ms > end_ts:
            continue
        mid = finite_float(row.get("mid_spread_pct"))
        if mid is None or (1 if mid > 0 else -1) != sign:
            continue
        peak = max(peak, abs(mid))
        if peak - abs(mid) >= config.expansion_relief_pct_points:
            return row
    return None


def latest_quote(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    exchange: str,
    ts_ms: int,
    max_skew_ms: int,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT ts_ms, bid, ask, bid_size, ask_size, quote_age_ms, staleness_flag
        FROM ca_market_snapshots_focus
        WHERE canonical_symbol = ? AND exchange = ?
          AND ts_ms <= ? AND ts_ms >= ?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (symbol, exchange, ts_ms, ts_ms - max_skew_ms),
    ).fetchone()
    return dict(row) if row is not None else None


def validate_quotes(
    left: Mapping[str, Any] | None,
    right: Mapping[str, Any] | None,
) -> str:
    if left is None or right is None:
        return "top_of_book_missing"
    for row in (left, right):
        bid = finite_float(row.get("bid"))
        ask = finite_float(row.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
            return "top_of_book_invalid"
        if int(row.get("staleness_flag") or 0):
            return "top_of_book_stale"
        quote_age = finite_float(row.get("quote_age_ms"))
        if quote_age is not None and quote_age > 15_000:
            return "top_of_book_stale"
    return ""


def directional_spread(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    direction: str,
    action: str,
) -> float | None:
    if direction == "long_a_short_b":
        left_price = left.get("ask" if action == "open" else "bid")
        right_price = right.get("bid" if action == "open" else "ask")
    elif direction == "long_b_short_a":
        left_price = right.get("ask" if action == "open" else "bid")
        right_price = left.get("bid" if action == "open" else "ask")
    else:
        return None
    return symmetric_spread(finite_float(left_price), finite_float(right_price))


def directional_sizes(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    direction: str,
) -> tuple[float | None, float | None]:
    if direction == "long_a_short_b":
        return finite_float(left.get("ask_size")), finite_float(right.get("bid_size"))
    return finite_float(right.get("ask_size")), finite_float(left.get("bid_size"))


def classify_price_match(
    top_of_book: float | None,
    feature_price: float | None,
    *,
    exact_tolerance_pct: float,
    soft_tolerance_pct: float,
) -> tuple[str, float | None]:
    if top_of_book is None or feature_price is None:
        return "missing", None
    difference = abs(top_of_book - feature_price)
    if difference <= exact_tolerance_pct:
        return "confirmed", difference
    if difference <= soft_tolerance_pct:
        return "within_2pct_tolerance", difference
    return "divergent", difference


def funding_cashflow(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    left_exchange: str,
    right_exchange: str,
    direction: str,
    entry_ts_ms: int,
    exit_ts_ms: int,
    entry_row: Mapping[str, Any],
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT exchange, ts_ms, funding_rate
        FROM ca_funding_history
        WHERE canonical_symbol = ? AND exchange IN (?, ?)
          AND ts_ms > ? AND ts_ms <= ?
        ORDER BY ts_ms
        """,
        (symbol, left_exchange, right_exchange, entry_ts_ms, exit_ts_ms),
    ).fetchall()
    rates: dict[str, list[float]] = {left_exchange: [], right_exchange: []}
    for row in rows:
        rate = finite_float(row["funding_rate"])
        if rate is not None:
            rates.setdefault(str(row["exchange"]), []).append(rate)
    horizon_h = (exit_ts_ms - entry_ts_ms) / 3_600_000
    for side, exchange in (("left", left_exchange), ("right", right_exchange)):
        time_to = finite_float(entry_row.get(f"{side}_time_to_funding_h"))
        if time_to is None and not rates.get(exchange):
            return {
                "status": "incomplete",
                "reason": f"{side}_funding_schedule_missing",
                "cashflow_pct": None,
                "settlements": 0,
            }
        if time_to is not None and time_to <= horizon_h + 1 / 60 and not rates.get(exchange):
            return {
                "status": "incomplete",
                "reason": f"{side}_funding_history_missing_expected",
                "cashflow_pct": None,
                "settlements": 0,
            }
    left_sum = sum(rates.get(left_exchange, []))
    right_sum = sum(rates.get(right_exchange, []))
    cashflow = (
        right_sum - left_sum
        if direction == "long_a_short_b"
        else left_sum - right_sum
    ) * 100.0
    return {
        "status": "complete",
        "reason": "",
        "cashflow_pct": cashflow,
        "settlements": len(rates.get(left_exchange, []))
        + len(rates.get(right_exchange, [])),
    }


def path_metrics(
    series: Sequence[Mapping[str, Any]],
    *,
    entry_ts_ms: int,
    exit_ts_ms: int,
    entry_open_spread_pct: float,
    direction: str,
    total_cost_pct: float,
) -> dict[str, float | None]:
    close_key = "close_ab_pct" if direction == "long_a_short_b" else "close_ba_pct"
    captures: list[tuple[int, float]] = []
    for row in series:
        ts_ms = int(row.get("ts_ms") or 0)
        if ts_ms < entry_ts_ms or ts_ms > exit_ts_ms:
            continue
        close = finite_float(row.get(close_key))
        if close is not None:
            captures.append((ts_ms, close - entry_open_spread_pct))
    if not captures:
        return {"mae_pct": None, "mfe_pct": None, "time_to_cost_breakeven_min": None}
    break_even = next(
        (
            (ts_ms - entry_ts_ms) / MINUTE_MS
            for ts_ms, capture in captures
            if capture > total_cost_pct
        ),
        None,
    )
    values = [capture for _, capture in captures]
    return {
        "mae_pct": min(values),
        "mfe_pct": max(values),
        "time_to_cost_breakeven_min": break_even,
    }


def summarize_results(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row.get("entry_policy") or ""),
                int(row.get("horizon_min") or 0),
                str(row.get("chronological_segment") or ""),
            )
        ].append(row)
    output: list[dict[str, Any]] = []
    for (policy, horizon, segment), group in sorted(grouped.items()):
        evaluated = [row for row in group if row.get("status") == "EVALUATED"]
        net = [float(row["net_capture_pct"]) for row in evaluated]
        ordered_abs = sorted((abs(value) for value in net), reverse=True)
        absolute_total = sum(ordered_abs)
        ordered_net = sorted(net, key=abs, reverse=True)
        remaining_net = ordered_net[5:]
        symbol_absolute: dict[str, float] = defaultdict(float)
        for row, value in zip(evaluated, net):
            symbol_absolute[str(row.get("symbol") or "unknown")] += abs(value)
        output.append(
            {
                "entry_policy": policy,
                "horizon_min": horizon,
                "chronological_segment": segment,
                "rows": len(group),
                "evaluated_rows": len(evaluated),
                "vetoed_rows": len(group) - len(evaluated),
                "net_positive_rate": mean_or_none(
                    [1.0 if value > 0 else 0.0 for value in net]
                ),
                "mean_net_capture_pct": mean_or_none(net),
                "median_net_capture_pct": median_or_none(net),
                "median_gross_capture_pct": median_or_none(
                    [finite_float(row.get("gross_capture_pct")) for row in evaluated]
                ),
                "median_funding_cashflow_pct": median_or_none(
                    [finite_float(row.get("funding_cashflow_pct")) for row in evaluated]
                ),
                "median_mae_pct": median_or_none(
                    [finite_float(row.get("mae_pct")) for row in evaluated]
                ),
                "median_mfe_pct": median_or_none(
                    [finite_float(row.get("mfe_pct")) for row in evaluated]
                ),
                "median_actual_delay_min": median_or_none(
                    [finite_float(row.get("actual_delay_min")) for row in evaluated]
                ),
                "top1_abs_contribution_share": (
                    ordered_abs[0] / absolute_total if absolute_total else 0.0
                ),
                "top5_abs_contribution_share": (
                    sum(ordered_abs[:5]) / absolute_total if absolute_total else 0.0
                ),
                "top_symbol_abs_contribution_share": (
                    max(symbol_absolute.values(), default=0.0) / absolute_total
                    if absolute_total
                    else 0.0
                ),
                "mean_net_without_top5_abs_pct": mean_or_none(remaining_net),
                "capacity_usd_known_rate": mean_or_none(
                    [1.0 if row.get("capacity_usd") is not None else 0.0 for row in evaluated]
                ),
            }
        )
    return output


def nearest_row(
    series: Sequence[Mapping[str, Any]],
    target_ts_ms: int,
    *,
    tolerance_ms: int,
) -> Mapping[str, Any] | None:
    candidates = [
        row
        for row in series
        if abs(int(row.get("ts_ms") or 0) - target_ts_ms) <= tolerance_ms
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda row: abs(int(row.get("ts_ms") or 0) - target_ts_ms))


def symmetric_spread(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or left <= 0 or right <= 0:
        return None
    denominator = left + right
    return 200.0 * (left - right) / denominator if denominator else None


def finite_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def mean_or_none(values: Sequence[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return statistics.fmean(clean) if clean else None


def median_or_none(values: Sequence[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return statistics.median(clean) if clean else None


def render_report(
    metadata: Mapping[str, Any],
    summaries: Sequence[Mapping[str, Any]],
    veto_summary: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Strategy Lab — Executable Spread Timing v1",
        "",
        "Status: research replay only. No paper, shadow, ARM or live actions.",
        "",
        f"- Feature rows: {metadata.get('feature_rows')}",
        f"- Causal events: {metadata.get('causal_events')}",
        f"- Evaluated outcomes: {metadata.get('evaluated_rows')}",
        f"- Vetoed outcomes: {metadata.get('vetoed_rows')}",
        f"- Price source: `{metadata.get('price_source')}`",
        f"- Capacity: `{metadata.get('capacity_status')}`",
        f"- Slippage: `{metadata.get('slippage_status')}`",
        "",
        "## Timing summary",
        "",
        "| Entry | Horizon min | Segment | Evaluated | Veto | Net positive | Median net % | Median MAE % | Median MFE % |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        lines.append(
            "| {entry_policy} | {horizon_min} | {chronological_segment} | "
            "{evaluated_rows} | {vetoed_rows} | {net_positive_rate} | "
            "{median_net_capture_pct} | {median_mae_pct} | {median_mfe_pct} |".format(
                **row
            )
        )
    lines.extend(["", "## Vetoes", ""])
    for row in veto_summary:
        lines.append(f"- `{row.get('reason')}`: {row.get('rows')}")
    lines.extend(
        [
            "",
            "Top-of-book bid/ask is historical, but USD capacity remains unavailable when contract multipliers or sizes are missing.",
            "The slippage value is a scenario, not a reconstructed depth fill. These gaps prevent paper/shadow promotion.",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "DEFAULT_OUTPUT_DIR",
    "ExecutableSpreadConfig",
    "classify_price_match",
    "directional_spread",
    "funding_cashflow",
    "run_executable_spread_timing",
    "select_expansion_stop_entry",
]
