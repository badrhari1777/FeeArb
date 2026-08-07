from __future__ import annotations

import hashlib
import json
import math
import statistics
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from analysis_features.strategy_lab_event_lake import (
    dataset_rows,
    stable_hash,
    write_csv,
    write_json_atomic,
)
from analysis_features.strategy_lab_event_lake_validation import (
    validate_event_lake_output,
)


DEFAULT_INPUT_DIR = Path("data/research/strategy_lab_event_lake_v4_full")
DEFAULT_OUTPUT_DIR = Path("data/research/strategy_lab_funding_forecast_v1")
SAMPLE_SCHEMA = "strategy_lab_funding_forecast_sample_v1"
REPORT_SCHEMA = "strategy_lab_funding_forecast_report_v1"
HOUR_MS = 3_600_000

FEATURE_NAMES = (
    "current_funding_bps",
    "funding_mean_24h_bps",
    "funding_std_24h_bps",
    "funding_change_bps",
    "funding_sign_streak",
    "funding_interval_h",
    "funding_latest_interval_h",
    "funding_interval_change_ratio",
    "current_funding_age_h",
    "projected_next_funding_in_h",
    "current_funding_hourly_bps",
    "funding_realized_1h_bps",
    "funding_realized_4h_bps",
    "funding_realized_8h_bps",
    "funding_realized_24h_bps",
    "funding_settlements_24h",
    "premium_current_bps",
    "premium_mean_1h_bps",
    "premium_mean_4h_bps",
    "premium_change_1h_bps",
    "premium_min_24h_bps",
    "basis_current_bps",
    "price_return_1h_pct",
    "price_return_4h_pct",
    "price_return_24h_pct",
    "volume_z_24h",
    "oi_change_1h_pct",
    "oi_change_4h_pct",
    "oi_change_24h_pct",
    "other_funding_bps",
    "funding_cross_exchange_diff_bps",
    "other_funding_hourly_bps",
    "funding_hourly_cross_exchange_diff_bps",
    "other_premium_bps",
    "premium_cross_exchange_diff_bps",
    "other_oi_change_4h_pct",
    "oi_cross_exchange_diff_4h_pct",
    "utc_hour_sin",
    "utc_hour_cos",
)


@dataclass(frozen=True, slots=True)
class FundingForecastConfig:
    current_funding_max_age_h: float = 12.0
    next_funding_max_wait_h: float = 12.0
    past_funding_h: int = 24
    future_cumulative_h: int = 24
    chronological_train_fraction: float = 0.70
    chronological_validation_fraction: float = 0.15
    symbol_holdout_fraction: float = 0.20
    logistic_iterations: int = 1_200
    logistic_learning_rate: float = 0.05
    logistic_l2: float = 0.01
    ridge_alpha: float = 2.0
    economic_cost_scenarios_bps: tuple[float, ...] = (0.0, 4.0, 8.0, 12.0, 16.0)
    min_train_rows: int = 20
    min_eval_rows: int = 5
    max_windows: int | None = None
    require_complete_event_lake: bool = True

    def validate(self) -> None:
        if self.current_funding_max_age_h <= 0 or self.next_funding_max_wait_h <= 0:
            raise ValueError("funding age/wait limits must be positive")
        if self.past_funding_h < 1 or self.future_cumulative_h != 24:
            raise ValueError("past funding must be positive and cumulative horizon must remain 24h")
        if not 0.5 <= self.chronological_train_fraction < 0.9:
            raise ValueError("chronological train fraction must be within 0.5..0.9")
        if not 0.05 <= self.chronological_validation_fraction < 0.3:
            raise ValueError("chronological validation fraction must be within 0.05..0.3")
        if self.chronological_train_fraction + self.chronological_validation_fraction >= 0.95:
            raise ValueError("chronological split leaves too little test history")
        if not 0.1 <= self.symbol_holdout_fraction <= 0.4:
            raise ValueError("symbol holdout fraction must be within 0.1..0.4")
        if self.logistic_iterations < 100 or self.logistic_learning_rate <= 0:
            raise ValueError("invalid logistic configuration")
        if (
            self.ridge_alpha <= 0
            or self.min_train_rows < 2
            or self.min_eval_rows < 1
        ):
            raise ValueError("invalid model/sample configuration")
        if (
            not self.economic_cost_scenarios_bps
            or any(value < 0 for value in self.economic_cost_scenarios_bps)
            or tuple(sorted(set(self.economic_cost_scenarios_bps)))
            != self.economic_cost_scenarios_bps
        ):
            raise ValueError("economic cost scenarios must be sorted unique non-negative values")
        if self.max_windows is not None and self.max_windows < 1:
            raise ValueError("max_windows must be positive")


def run_funding_forecast(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    config: FundingForecastConfig | None = None,
    code_commit: str = "",
) -> dict[str, Any]:
    cfg = config or FundingForecastConfig()
    cfg.validate()
    started = time.time()
    validation = validate_event_lake_output(
        input_dir,
        require_complete=cfg.require_complete_event_lake,
    )
    manifest = read_json(input_dir / "manifest.json")
    tasks = [dict(row) for row in manifest.get("tasks") or []]
    tasks_by_window: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        tasks_by_window.setdefault(str(task.get("physical_window_id") or ""), []).append(task)

    physical_rows = [dict(row) for row in manifest.get("physical_windows") or []]
    physical_rows.sort(
        key=lambda row: (
            min(
                (
                    int(task.get("event_ts_ms") or 0)
                    for task in tasks_by_window.get(str(row.get("physical_window_id") or ""), [])
                ),
                default=0,
            ),
            str(row.get("symbol") or ""),
            str(row.get("exchange") or ""),
        )
    )
    if not cfg.require_complete_event_lake:
        physical_rows = [
            row
            for row in physical_rows
            if (input_dir / "windows" / f"{row.get('physical_window_id')}.json").exists()
        ]
    if cfg.max_windows is not None:
        physical_rows = physical_rows[: cfg.max_windows]

    samples: list[dict[str, Any]] = []
    vetoes: list[dict[str, Any]] = []
    cross_exchange_contexts: list[dict[str, Any]] = []
    for physical in physical_rows:
        physical_window_id = str(physical.get("physical_window_id") or "")
        cache_path = input_dir / "windows" / f"{physical_window_id}.json"
        logical_tasks = sorted(
            tasks_by_window.get(physical_window_id, []),
            key=lambda row: str(row.get("event_id") or ""),
        )
        if not logical_tasks:
            raise ValueError(f"physical window has no logical tasks: {physical_window_id}")
        if not cache_path.exists():
            if cfg.require_complete_event_lake:
                raise ValueError(f"missing physical window: {physical_window_id}")
            continue
        window = read_json(cache_path)
        context = build_cross_exchange_context(
            task=logical_tasks[0],
            window=window,
            config=cfg,
        )
        if context is not None:
            cross_exchange_contexts.append(context)
        sample = build_funding_sample(
            task=logical_tasks[0],
            logical_tasks=logical_tasks,
            window=window,
            config=cfg,
        )
        if sample.get("status") == "eligible":
            samples.append(sample)
        else:
            vetoes.append(sample)

    add_cross_exchange_features(samples, contexts=cross_exchange_contexts)
    samples.sort(key=lambda row: (int(row["event_ts_ms"]), str(row["symbol"]), str(row["exchange"])))
    metrics, calibration, coefficients, predictions = evaluate_forecasts(samples, cfg)

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "samples.csv", samples)
    write_csv(output_dir / "vetoes.csv", vetoes)
    write_csv(output_dir / "metrics.csv", metrics)
    write_csv(output_dir / "calibration.csv", calibration)
    write_csv(output_dir / "coefficients.csv", coefficients)
    write_csv(output_dir / "predictions.csv", predictions)
    veto_summary = [
        {"reason": reason, "windows": count}
        for reason, count in sorted(Counter(str(row.get("veto_reason") or "unknown") for row in vetoes).items())
    ]
    write_csv(output_dir / "veto_summary.csv", veto_summary)

    metadata = {
        "schema": REPORT_SCHEMA,
        "mode": "research_replay",
        "public_only": True,
        "live_actions": False,
        "paper_promotion_allowed": False,
        "shadow_promotion_allowed": False,
        "decision_status": "research_evaluation_only",
        "input_dir": str(input_dir),
        "input_run_id": manifest.get("run_id"),
        "input_code_commit": manifest.get("code_commit"),
        "code_commit": code_commit,
        "config": asdict(cfg),
        "config_hash": stable_hash(asdict(cfg)),
        "event_lake_validation": validation,
        "physical_windows_considered": len(physical_rows),
        "eligible_samples": len(samples),
        "vetoed_windows": len(vetoes),
        "hourly_funding_samples": sum(
            row.get("current_funding_hourly_bps") is not None for row in samples
        ),
        "interval_change_feature_samples": sum(
            row.get("funding_interval_change_ratio") is not None for row in samples
        ),
        "detected_interval_shift_samples": sum(
            (ratio := finite_float(row.get("funding_interval_change_ratio"))) is not None
            and (ratio < 0.75 or ratio > 1.25)
            for row in samples
        ),
        "symbols": len({str(row["symbol"]) for row in samples}),
        "exchanges": sorted({str(row["exchange"]) for row in samples}),
        "cross_exchange_premium_samples": sum(
            row.get("premium_cross_exchange_diff_bps") is not None for row in samples
        ),
        "cross_exchange_funding_samples": sum(
            row.get("funding_cross_exchange_diff_bps") is not None for row in samples
        ),
        "cross_exchange_hourly_funding_samples": sum(
            row.get("funding_hourly_cross_exchange_diff_bps") is not None
            for row in samples
        ),
        "cross_exchange_oi_samples": sum(
            row.get("oi_cross_exchange_diff_4h_pct") is not None for row in samples
        ),
        "other_exchange_oi_context_samples": sum(
            row.get("other_oi_change_4h_pct") is not None for row in samples
        ),
        "metric_rows": len(metrics),
        "economic_horizons_h": [4, 8, 24],
        "economic_cost_scenarios_bps": list(cfg.economic_cost_scenarios_bps),
        "elapsed_sec": round(time.time() - started, 3),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "final_result_allowed": bool(cfg.require_complete_event_lake),
    }
    write_json_atomic(output_dir / "metadata.json", metadata)
    (output_dir / "index.md").write_text(
        render_report(metadata, metrics, veto_summary), encoding="utf-8"
    )
    return metadata


def build_funding_sample(
    *,
    task: Mapping[str, Any],
    logical_tasks: Sequence[Mapping[str, Any]],
    window: Mapping[str, Any],
    config: FundingForecastConfig,
) -> dict[str, Any]:
    event_ts = int(task.get("event_ts_ms") or 0)
    end_ms = int(task.get("end_ms") or window.get("end_ms") or 0)
    base = {
        "schema": SAMPLE_SCHEMA,
        "sample_id": str(task.get("physical_window_id") or window.get("physical_window_id") or ""),
        "physical_window_id": str(task.get("physical_window_id") or ""),
        "logical_event_ids": "|".join(sorted(str(row.get("event_id") or "") for row in logical_tasks)),
        "logical_event_count": len(logical_tasks),
        "event_type": str(task.get("event_type") or ""),
        "symbol": str(task.get("symbol") or window.get("symbol") or ""),
        "exchange": str(task.get("exchange") or window.get("exchange") or ""),
        "event_ts_ms": event_ts,
        "event_ts_iso": datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc).isoformat()
        if event_ts > 0
        else "",
        "status": "veto",
        "veto_reason": "",
    }
    if not bool((window.get("market") or {}).get("available")):
        return {**base, "veto_reason": "market_unavailable"}
    series = dict(window.get("series") or {})
    funding = sorted_finite_rows(series.get("funding"), "funding_rate")
    past_funding = [row for row in funding if int(row["ts_ms"]) <= event_ts]
    future_funding = [row for row in funding if int(row["ts_ms"]) > event_ts]
    if not past_funding:
        return {**base, "veto_reason": "current_funding_missing"}
    current = past_funding[-1]
    current_age_h = (event_ts - int(current["ts_ms"])) / HOUR_MS
    if current_age_h > config.current_funding_max_age_h:
        return {**base, "veto_reason": "current_funding_stale", "current_funding_age_h": current_age_h}
    if not future_funding:
        return {**base, "veto_reason": "next_funding_missing"}
    next_row = future_funding[0]
    next_wait_h = (int(next_row["ts_ms"]) - event_ts) / HOUR_MS
    if next_wait_h > config.next_funding_max_wait_h:
        return {**base, "veto_reason": "next_funding_too_far", "next_funding_wait_h": next_wait_h}

    premium = sorted_finite_rows(series.get("premium"), "close")
    ohlcv = sorted_finite_rows(series.get("ohlcv"), "close")
    if latest_at_or_before(premium, event_ts, "close") is None:
        return {**base, "veto_reason": "premium_missing_at_event"}
    if latest_at_or_before(ohlcv, event_ts, "close") is None:
        return {**base, "veto_reason": "ohlcv_missing_at_event"}

    funding_24 = [
        row for row in past_funding if int(row["ts_ms"]) >= event_ts - config.past_funding_h * HOUR_MS
    ]
    funding_values = [float(row["funding_rate"]) for row in funding_24]
    current_rate = float(current["funding_rate"])
    previous_rate = float(past_funding[-2]["funding_rate"]) if len(past_funding) >= 2 else current_rate
    median_interval_h = funding_interval_hours(past_funding)
    latest_interval_h = latest_funding_interval_hours(past_funding)
    interval_change_ratio = funding_interval_change_ratio(past_funding)
    projected_next_funding_in_h = (
        max(0.0, latest_interval_h - current_age_h)
        if latest_interval_h is not None
        else None
    )
    current_funding_hourly_bps = (
        current_rate * 10_000.0 / latest_interval_h
        if latest_interval_h not in (None, 0.0)
        else None
    )
    next_interval_h = normalize_funding_interval_hours(
        (int(next_row["ts_ms"]) - int(current["ts_ms"])) / HOUR_MS
    )
    next_funding_hourly_bps = (
        float(next_row["funding_rate"]) * 10_000.0 / next_interval_h
        if next_interval_h > 0
        else None
    )
    future_by_horizon = {
        hours: [
            row
            for row in future_funding
            if int(row["ts_ms"]) <= event_ts + hours * HOUR_MS
        ]
        for hours in (4, 8, 24)
    }
    current_sign = rate_sign(current_rate)
    next_rate = float(next_row["funding_rate"])
    duration_observed_until_ts_ms = min(end_ms, int(future_funding[-1]["ts_ms"]))
    same_sign_duration_h = max(
        0.0, (duration_observed_until_ts_ms - event_ts) / HOUR_MS
    )
    same_sign_settlements = 0
    duration_censored = True
    for row in future_funding:
        if rate_sign(float(row["funding_rate"])) != current_sign:
            same_sign_duration_h = max(0.0, (int(row["ts_ms"]) - event_ts) / HOUR_MS)
            duration_observed_until_ts_ms = int(row["ts_ms"])
            duration_censored = False
            break
        same_sign_settlements += 1

    mark = sorted_finite_rows(series.get("mark"), "close")
    index = sorted_finite_rows(series.get("index"), "close")
    oi = sorted_finite_rows(series.get("open_interest"), "open_interest")
    premium_current = latest_at_or_before(premium, event_ts, "close")
    mark_current = latest_at_or_before(mark, event_ts, "close")
    index_current = latest_at_or_before(index, event_ts, "close")
    basis_bps = (
        (mark_current / index_current - 1.0) * 10_000.0
        if mark_current is not None and index_current not in (None, 0.0)
        else None
    )
    utc_hour = datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc).hour
    angle = 2.0 * math.pi * utc_hour / 24.0
    sample = {
        **base,
        "status": "eligible",
        "veto_reason": "",
        "current_funding_ts_ms": int(current["ts_ms"]),
        "current_funding_age_h": current_age_h,
        "next_funding_ts_ms": int(next_row["ts_ms"]),
        "next_funding_wait_h": next_wait_h,
        "current_funding_bps": current_rate * 10_000.0,
        "funding_mean_24h_bps": mean_or_none(funding_values, multiplier=10_000.0),
        "funding_std_24h_bps": std_or_none(funding_values, multiplier=10_000.0),
        "funding_change_bps": (current_rate - previous_rate) * 10_000.0,
        "funding_sign_streak": funding_sign_streak(past_funding),
        "funding_interval_h": median_interval_h,
        "funding_latest_interval_h": latest_interval_h,
        "funding_interval_change_ratio": interval_change_ratio,
        "projected_next_funding_in_h": projected_next_funding_in_h,
        "current_funding_hourly_bps": current_funding_hourly_bps,
        "funding_realized_1h_bps": trailing_sum(
            past_funding, event_ts, 1, "funding_rate", 10_000.0
        ),
        "funding_realized_4h_bps": trailing_sum(
            past_funding, event_ts, 4, "funding_rate", 10_000.0
        ),
        "funding_realized_8h_bps": trailing_sum(
            past_funding, event_ts, 8, "funding_rate", 10_000.0
        ),
        "funding_realized_24h_bps": trailing_sum(
            past_funding, event_ts, 24, "funding_rate", 10_000.0
        ),
        "funding_settlements_24h": trailing_count(past_funding, event_ts, 24),
        "premium_current_bps": optional_mul(premium_current, 10_000.0),
        "premium_mean_1h_bps": trailing_mean(premium, event_ts, 1, "close", 10_000.0),
        "premium_mean_4h_bps": trailing_mean(premium, event_ts, 4, "close", 10_000.0),
        "premium_change_1h_bps": trailing_change(premium, event_ts, 1, "close", 10_000.0),
        "premium_min_24h_bps": trailing_min(premium, event_ts, 24, "close", 10_000.0),
        "basis_current_bps": basis_bps,
        "price_return_1h_pct": trailing_return(ohlcv, event_ts, 1, "close"),
        "price_return_4h_pct": trailing_return(ohlcv, event_ts, 4, "close"),
        "price_return_24h_pct": trailing_return(ohlcv, event_ts, 24, "close"),
        "volume_z_24h": trailing_volume_z(ohlcv, event_ts, 24),
        "oi_change_1h_pct": trailing_return(oi, event_ts, 1, "open_interest"),
        "oi_change_4h_pct": trailing_return(oi, event_ts, 4, "open_interest"),
        "oi_change_24h_pct": trailing_return(oi, event_ts, 24, "open_interest"),
        "other_funding_bps": None,
        "funding_cross_exchange_diff_bps": None,
        "other_funding_hourly_bps": None,
        "funding_hourly_cross_exchange_diff_bps": None,
        "other_premium_bps": None,
        "premium_cross_exchange_diff_bps": None,
        "other_oi_change_4h_pct": None,
        "oi_cross_exchange_diff_4h_pct": None,
        "utc_hour_sin": math.sin(angle),
        "utc_hour_cos": math.cos(angle),
        "target_next_positive": 1 if next_rate > 0 else 0,
        "target_sign_persisted": 1 if rate_sign(next_rate) == current_sign else 0,
        "target_next_funding_bps": next_rate * 10_000.0,
        "target_next_interval_h": next_interval_h,
        "target_next_funding_hourly_bps": next_funding_hourly_bps,
        "target_weakened": 1 if abs(next_rate) < abs(current_rate) else 0,
        "target_same_sign_duration_h": same_sign_duration_h,
        "target_same_sign_settlements": same_sign_settlements,
        "target_duration_censored": 1 if duration_censored else 0,
        "target_duration_observed_until_ts_ms": duration_observed_until_ts_ms,
        "target_cumulative_funding_4h_bps": funding_rows_sum_bps(future_by_horizon[4]),
        "target_cumulative_funding_8h_bps": funding_rows_sum_bps(future_by_horizon[8]),
        "target_cumulative_funding_24h_bps": funding_rows_sum_bps(
            future_by_horizon[24]
        ),
        "target_future_settlements_4h": len(future_by_horizon[4]),
        "target_future_settlements_8h": len(future_by_horizon[8]),
        "target_future_settlements_24h": len(
            future_by_horizon[24]
        ),
    }
    return sample


def build_cross_exchange_context(
    *,
    task: Mapping[str, Any],
    window: Mapping[str, Any],
    config: FundingForecastConfig,
) -> dict[str, Any] | None:
    """Build causal feature-only context without requiring a future label."""
    if not bool((window.get("market") or {}).get("available")):
        return None
    event_ts = int(task.get("event_ts_ms") or 0)
    series = dict(window.get("series") or {})
    funding = sorted_finite_rows(series.get("funding"), "funding_rate")
    past_funding = [row for row in funding if int(row["ts_ms"]) <= event_ts]
    current_funding_bps: float | None = None
    current_funding_hourly_bps: float | None = None
    if past_funding:
        current = past_funding[-1]
        age_h = (event_ts - int(current["ts_ms"])) / HOUR_MS
        if age_h <= config.current_funding_max_age_h:
            current_funding_bps = float(current["funding_rate"]) * 10_000.0
            latest_interval_h = latest_funding_interval_hours(past_funding)
            if latest_interval_h not in (None, 0.0):
                current_funding_hourly_bps = current_funding_bps / latest_interval_h
    premium = sorted_finite_rows(series.get("premium"), "close")
    oi = sorted_finite_rows(series.get("open_interest"), "open_interest")
    return {
        "symbol": str(task.get("symbol") or window.get("symbol") or ""),
        "exchange": str(task.get("exchange") or window.get("exchange") or ""),
        "event_ts_ms": event_ts,
        "current_funding_bps": current_funding_bps,
        "current_funding_hourly_bps": current_funding_hourly_bps,
        "premium_current_bps": optional_mul(
            latest_at_or_before(premium, event_ts, "close"), 10_000.0
        ),
        "oi_change_4h_pct": trailing_return(oi, event_ts, 4, "open_interest"),
    }


def add_cross_exchange_features(
    samples: list[dict[str, Any]],
    *,
    contexts: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for source in contexts if contexts is not None else samples:
        row = dict(source)
        grouped.setdefault((str(row["symbol"]), int(row["event_ts_ms"])), []).append(row)
    for row in samples:
        rows = grouped.get((str(row["symbol"]), int(row["event_ts_ms"])), [])
        other = next(
            (item for item in rows if item.get("exchange") != row.get("exchange")),
            None,
        )
        if other is None:
            continue
        other_funding = finite_float(other.get("current_funding_bps"))
        own_funding = finite_float(row.get("current_funding_bps"))
        other_funding_hourly = finite_float(other.get("current_funding_hourly_bps"))
        own_funding_hourly = finite_float(row.get("current_funding_hourly_bps"))
        other_premium = finite_float(other.get("premium_current_bps"))
        own_premium = finite_float(row.get("premium_current_bps"))
        other_oi = finite_float(other.get("oi_change_4h_pct"))
        own_oi = finite_float(row.get("oi_change_4h_pct"))
        row["other_funding_bps"] = other_funding
        row["funding_cross_exchange_diff_bps"] = optional_diff(own_funding, other_funding)
        row["other_funding_hourly_bps"] = other_funding_hourly
        row["funding_hourly_cross_exchange_diff_bps"] = optional_diff(
            own_funding_hourly, other_funding_hourly
        )
        row["other_premium_bps"] = other_premium
        row["premium_cross_exchange_diff_bps"] = optional_diff(own_premium, other_premium)
        row["other_oi_change_4h_pct"] = other_oi
        row["oi_cross_exchange_diff_4h_pct"] = optional_diff(own_oi, other_oi)


def evaluate_forecasts(
    samples: Sequence[Mapping[str, Any]],
    config: FundingForecastConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if not samples:
        return [], [], [], []
    split_sets = build_evaluation_splits(samples, config)
    metrics: list[dict[str, Any]] = []
    calibration: list[dict[str, Any]] = []
    coefficients: list[dict[str, Any]] = []
    predictions: list[dict[str, Any]] = []
    for split_name, train, evaluation in split_sets:
        if len(train) < config.min_train_rows or len(evaluation) < config.min_eval_rows:
            metrics.append(
                {
                    "split": split_name,
                    "target": "all",
                    "model": "insufficient_sample",
                    "train_rows": len(train),
                    "eval_rows": len(evaluation),
                }
            )
            continue
        transformer = fit_transformer(train, FEATURE_NAMES)
        train_x = [transform_row(row, transformer) for row in train]
        eval_x = [transform_row(row, transformer) for row in evaluation]
        expanded_names = list(transformer["expanded_names"])

        for target, label in (
            ("target_next_positive", "next_sign"),
            ("target_weakened", "weakening"),
        ):
            train_y = [float(row[target]) for row in train]
            eval_y = [float(row[target]) for row in evaluation]
            model = fit_logistic(train_x, train_y, config)
            probabilities = [predict_logistic(model, row) for row in eval_x]
            model_metrics = classification_metrics(eval_y, probabilities)
            metrics.append(
                {
                    "split": split_name,
                    "target": label,
                    "model": "logistic_v1",
                    "train_rows": len(train),
                    "eval_rows": len(evaluation),
                    **model_metrics,
                }
            )
            if target == "target_next_positive":
                persistence = [1.0 if float(row["current_funding_bps"]) > 0 else 0.0 for row in evaluation]
                metrics.append(
                    {
                        "split": split_name,
                        "target": label,
                        "model": "current_sign_persistence",
                        "train_rows": len(train),
                        "eval_rows": len(evaluation),
                        **classification_metrics(eval_y, persistence),
                    }
                )
                calibration.extend(calibration_rows(split_name, eval_y, probabilities))
                for horizon_h in (4, 8, 24):
                    target_field = f"target_cumulative_funding_{horizon_h}h_bps"
                    for cost_bps in config.economic_cost_scenarios_bps:
                        for model_name, directions in (
                            ("logistic_sign_direction", probabilities),
                            ("current_sign_direction", persistence),
                        ):
                            metrics.append(
                                {
                                    "split": split_name,
                                    "target": f"funding_capture_{horizon_h}h_bps_proxy",
                                    "model": model_name,
                                    "train_rows": len(train),
                                    "eval_rows": len(evaluation),
                                    **economic_proxy_metrics(
                                        evaluation,
                                        directions,
                                        target_field=target_field,
                                        cost_bps=cost_bps,
                                    ),
                                }
                            )
            for feature, coefficient in zip(["intercept", *expanded_names], model):
                coefficients.append(
                    {
                        "split": split_name,
                        "target": label,
                        "model": "logistic_v1",
                        "feature": feature,
                        "coefficient": coefficient,
                    }
                )
            for row, probability in zip(evaluation, probabilities):
                prediction = prediction_identity(row, split_name)
                prediction.update(
                    {
                        "target": label,
                        "actual": row[target],
                        "prediction": probability,
                    }
                )
                predictions.append(prediction)

        for target, label, baseline_field, baseline_name in (
            (
                "target_next_funding_bps",
                "next_magnitude_bps",
                "current_funding_bps",
                "current_funding_rate",
            ),
            (
                "target_next_funding_hourly_bps",
                "next_hourly_magnitude_bps",
                "current_funding_hourly_bps",
                "current_hourly_funding_rate",
            ),
            (
                "target_next_interval_h",
                "next_interval_h",
                "funding_latest_interval_h",
                "current_funding_interval",
            ),
            (
                "target_same_sign_duration_h",
                "same_sign_duration_h",
                None,
                "train_median_duration",
            ),
        ):
            train_indices = [
                index
                for index, row in enumerate(train)
                if finite_float(row.get(target)) is not None
                and (
                    baseline_field is None
                    or finite_float(row.get(baseline_field)) is not None
                )
            ]
            eval_indices = [
                index
                for index, row in enumerate(evaluation)
                if finite_float(row.get(target)) is not None
                and (
                    baseline_field is None
                    or finite_float(row.get(baseline_field)) is not None
                )
            ]
            excluded_censored = 0
            if target == "target_same_sign_duration_h":
                train_indices = [
                    index
                    for index, row in enumerate(train)
                    if not bool(int(row.get("target_duration_censored") or 0))
                ]
                eval_indices = [
                    index
                    for index, row in enumerate(evaluation)
                    if not bool(int(row.get("target_duration_censored") or 0))
                ]
                excluded_censored = len(evaluation) - len(eval_indices)
            target_train = [train[index] for index in train_indices]
            target_evaluation = [evaluation[index] for index in eval_indices]
            target_train_x = [train_x[index] for index in train_indices]
            target_eval_x = [eval_x[index] for index in eval_indices]
            if (
                len(target_train) < config.min_train_rows
                or len(target_evaluation) < config.min_eval_rows
            ):
                metrics.append(
                    {
                        "split": split_name,
                        "target": label,
                        "model": "insufficient_uncensored_sample",
                        "train_rows": len(target_train),
                        "eval_rows": len(target_evaluation),
                        "censored_eval_rows_excluded": excluded_censored,
                    }
                )
                continue
            train_y = [float(row[target]) for row in target_train]
            eval_y = [float(row[target]) for row in target_evaluation]
            model = fit_ridge(target_train_x, train_y, alpha=config.ridge_alpha)
            predicted = [predict_linear(model, row) for row in target_eval_x]
            metrics.append(
                {
                    "split": split_name,
                    "target": label,
                    "model": "ridge_v1",
                    "train_rows": len(target_train),
                    "eval_rows": len(target_evaluation),
                    "censored_eval_rows_excluded": excluded_censored,
                    **regression_metrics(eval_y, predicted),
                }
            )
            if baseline_field:
                baseline = [float(row[baseline_field]) for row in target_evaluation]
            else:
                median_duration = statistics.median(train_y)
                baseline = [median_duration] * len(target_evaluation)
            metrics.append(
                {
                    "split": split_name,
                    "target": label,
                    "model": baseline_name,
                    "train_rows": len(target_train),
                    "eval_rows": len(target_evaluation),
                    "censored_eval_rows_excluded": excluded_censored,
                    **regression_metrics(eval_y, baseline),
                }
            )
            for feature, coefficient in zip(["intercept", *expanded_names], model):
                coefficients.append(
                    {
                        "split": split_name,
                        "target": label,
                        "model": "ridge_v1",
                        "feature": feature,
                        "coefficient": coefficient,
                    }
                )
    return metrics, calibration, coefficients, predictions


def build_evaluation_splits(
    samples: Sequence[Mapping[str, Any]],
    config: FundingForecastConfig,
) -> list[tuple[str, list[Mapping[str, Any]], list[Mapping[str, Any]]]]:
    ordered = sorted(samples, key=lambda row: (int(row["event_ts_ms"]), str(row["symbol"]), str(row["exchange"])))
    timestamps = sorted({int(row["event_ts_ms"]) for row in ordered})
    train_cut = max(1, min(len(timestamps) - 2, math.floor(len(timestamps) * config.chronological_train_fraction)))
    validation_cut = max(
        train_cut + 1,
        min(
            len(timestamps) - 1,
            math.floor(
                len(timestamps)
                * (config.chronological_train_fraction + config.chronological_validation_fraction)
            ),
        ),
    )
    train_ts = set(timestamps[:train_cut])
    validation_ts = set(timestamps[train_cut:validation_cut])
    test_ts = set(timestamps[validation_cut:])
    chronological_train = [row for row in ordered if int(row["event_ts_ms"]) in train_ts]
    chronological_validation = [row for row in ordered if int(row["event_ts_ms"]) in validation_ts]
    chronological_test = [row for row in ordered if int(row["event_ts_ms"]) in test_ts]

    symbols = sorted({str(row["symbol"]) for row in ordered})
    holdout_symbols = {
        symbol
        for symbol in symbols
        if stable_bucket(symbol, 10_000) < int(config.symbol_holdout_fraction * 10_000)
    }
    if not holdout_symbols and symbols:
        holdout_symbols = {symbols[-1]}
    if holdout_symbols == set(symbols) and len(symbols) > 1:
        holdout_symbols.remove(symbols[0])
    symbol_train = [row for row in ordered if str(row["symbol"]) not in holdout_symbols]
    symbol_holdout = [row for row in ordered if str(row["symbol"]) in holdout_symbols]
    return [
        ("chronological_validation", chronological_train, chronological_validation),
        ("chronological_test", chronological_train + chronological_validation, chronological_test),
        ("unseen_symbol_holdout", symbol_train, symbol_holdout),
    ]


def fit_transformer(
    rows: Sequence[Mapping[str, Any]], feature_names: Sequence[str]
) -> dict[str, Any]:
    medians: dict[str, float] = {}
    means: dict[str, float] = {}
    scales: dict[str, float] = {}
    expanded: list[str] = []
    for feature in feature_names:
        values = [value for row in rows if (value := finite_float(row.get(feature))) is not None]
        median = statistics.median(values) if values else 0.0
        imputed = [finite_float(row.get(feature)) if finite_float(row.get(feature)) is not None else median for row in rows]
        mean = statistics.fmean(imputed) if imputed else 0.0
        scale = statistics.pstdev(imputed) if len(imputed) > 1 else 1.0
        medians[feature] = median
        means[feature] = mean
        scales[feature] = scale if scale > 1e-12 else 1.0
        expanded.extend((feature, f"{feature}__missing"))
    return {
        "feature_names": list(feature_names),
        "expanded_names": expanded,
        "medians": medians,
        "means": means,
        "scales": scales,
    }


def transform_row(row: Mapping[str, Any], transformer: Mapping[str, Any]) -> list[float]:
    out: list[float] = []
    for feature in transformer["feature_names"]:
        raw = finite_float(row.get(feature))
        missing = 1.0 if raw is None else 0.0
        value = float(transformer["medians"][feature]) if raw is None else raw
        out.append((value - float(transformer["means"][feature])) / float(transformer["scales"][feature]))
        out.append(missing)
    return out


def fit_logistic(
    xs: Sequence[Sequence[float]], ys: Sequence[float], config: FundingForecastConfig
) -> list[float]:
    if not xs:
        return [0.0]
    width = len(xs[0])
    prevalence = min(1.0 - 1e-6, max(1e-6, statistics.fmean(ys)))
    weights = [math.log(prevalence / (1.0 - prevalence)), *([0.0] * width)]
    count = float(len(xs))
    for _ in range(config.logistic_iterations):
        gradients = [0.0] * len(weights)
        for row, target in zip(xs, ys):
            probability = sigmoid(weights[0] + sum(weight * value for weight, value in zip(weights[1:], row)))
            error = probability - target
            gradients[0] += error
            for index, value in enumerate(row, start=1):
                gradients[index] += error * value
        weights[0] -= config.logistic_learning_rate * gradients[0] / count
        for index in range(1, len(weights)):
            penalty = config.logistic_l2 * weights[index]
            weights[index] -= config.logistic_learning_rate * (gradients[index] / count + penalty)
    return weights


def fit_ridge(xs: Sequence[Sequence[float]], ys: Sequence[float], *, alpha: float) -> list[float]:
    if not xs:
        return [0.0]
    design = [[1.0, *map(float, row)] for row in xs]
    width = len(design[0])
    xtx = [[0.0] * width for _ in range(width)]
    xty = [0.0] * width
    for row, target in zip(design, ys):
        for left in range(width):
            xty[left] += row[left] * target
            for right in range(width):
                xtx[left][right] += row[left] * row[right]
    for index in range(1, width):
        xtx[index][index] += alpha
    return solve_linear_system(xtx, xty)


def solve_linear_system(matrix: Sequence[Sequence[float]], vector: Sequence[float]) -> list[float]:
    size = len(vector)
    augmented = [list(matrix[index]) + [float(vector[index])] for index in range(size)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-12:
            augmented[pivot][column] = 1e-12
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            augmented[row] = [
                value - factor * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[column])
            ]
    return [augmented[index][-1] for index in range(size)]


def predict_logistic(model: Sequence[float], row: Sequence[float]) -> float:
    return sigmoid(float(model[0]) + sum(weight * value for weight, value in zip(model[1:], row)))


def predict_linear(model: Sequence[float], row: Sequence[float]) -> float:
    return float(model[0]) + sum(weight * value for weight, value in zip(model[1:], row))


def classification_metrics(actual: Sequence[float], predicted: Sequence[float]) -> dict[str, float]:
    clipped = [min(1.0 - 1e-12, max(1e-12, float(value))) for value in predicted]
    return {
        "accuracy": statistics.fmean(
            1.0 if (probability >= 0.5) == (target >= 0.5) else 0.0
            for target, probability in zip(actual, clipped)
        ),
        "brier": statistics.fmean((probability - target) ** 2 for target, probability in zip(actual, clipped)),
        "log_loss": statistics.fmean(
            -(target * math.log(probability) + (1.0 - target) * math.log(1.0 - probability))
            for target, probability in zip(actual, clipped)
        ),
        "positive_rate": statistics.fmean(actual),
        "mean_prediction": statistics.fmean(clipped),
    }


def regression_metrics(actual: Sequence[float], predicted: Sequence[float]) -> dict[str, float]:
    errors = [forecast - target for target, forecast in zip(actual, predicted)]
    return {
        "mae": statistics.fmean(abs(error) for error in errors),
        "rmse": math.sqrt(statistics.fmean(error * error for error in errors)),
        "bias": statistics.fmean(errors),
    }


def economic_proxy_metrics(
    rows: Sequence[Mapping[str, Any]],
    sign_probabilities: Sequence[float],
    *,
    target_field: str,
    cost_bps: float,
) -> dict[str, float]:
    gross = [
        (1.0 if probability >= 0.5 else -1.0)
        * float(row[target_field])
        for row, probability in zip(rows, sign_probabilities)
    ]
    net = [value - cost_bps for value in gross]
    return {
        "mean_gross_funding_bps": statistics.fmean(gross),
        "median_gross_funding_bps": statistics.median(gross),
        "mean_net_after_cost_scenario_bps": statistics.fmean(net),
        "net_positive_rate": statistics.fmean(1.0 if value > 0 else 0.0 for value in net),
        "cost_scenario_bps": cost_bps,
    }


def calibration_rows(split: str, actual: Sequence[float], predicted: Sequence[float]) -> list[dict[str, Any]]:
    buckets: dict[int, list[tuple[float, float]]] = {}
    for target, probability in zip(actual, predicted):
        bucket = min(9, max(0, int(float(probability) * 10)))
        buckets.setdefault(bucket, []).append((target, probability))
    return [
        {
            "split": split,
            "target": "next_sign",
            "bucket": bucket,
            "lower_probability": bucket / 10,
            "upper_probability": (bucket + 1) / 10,
            "rows": len(values),
            "mean_prediction": statistics.fmean(value[1] for value in values),
            "observed_positive_rate": statistics.fmean(value[0] for value in values),
        }
        for bucket, values in sorted(buckets.items())
    ]


def prediction_identity(row: Mapping[str, Any], split: str) -> dict[str, Any]:
    return {
        "split": split,
        "sample_id": row.get("sample_id"),
        "event_ts_ms": row.get("event_ts_ms"),
        "symbol": row.get("symbol"),
        "exchange": row.get("exchange"),
    }


def sorted_finite_rows(dataset: Any, value_key: str) -> list[dict[str, float]]:
    out: dict[int, dict[str, float]] = {}
    for row in dataset_rows(dataset):
        ts_ms = finite_int(row.get("ts_ms"))
        value = finite_float(row.get(value_key))
        if ts_ms is not None and value is not None:
            extras = {
                key: extra_value
                for key, extra_value in row.items()
                if key not in {"ts_ms", value_key}
            }
            out[ts_ms] = {"ts_ms": ts_ms, value_key: value, **extras}
    return [out[key] for key in sorted(out)]


def latest_at_or_before(rows: Sequence[Mapping[str, Any]], ts_ms: int, key: str) -> float | None:
    for row in reversed(rows):
        if int(row["ts_ms"]) <= ts_ms:
            return finite_float(row.get(key))
    return None


def trailing_values(
    rows: Sequence[Mapping[str, Any]], ts_ms: int, hours: int, key: str
) -> list[float]:
    start = ts_ms - hours * HOUR_MS
    return [
        value
        for row in rows
        if start <= int(row["ts_ms"]) <= ts_ms
        if (value := finite_float(row.get(key))) is not None
    ]


def trailing_sum(
    rows: Sequence[Mapping[str, Any]],
    ts_ms: int,
    hours: int,
    key: str,
    multiplier: float,
) -> float | None:
    start = ts_ms - hours * HOUR_MS
    values = [
        value
        for row in rows
        if start < int(row["ts_ms"]) <= ts_ms
        if (value := finite_float(row.get(key))) is not None
    ]
    return sum(values) * multiplier if values else None


def trailing_count(
    rows: Sequence[Mapping[str, Any]], ts_ms: int, hours: int
) -> int:
    start = ts_ms - hours * HOUR_MS
    return sum(1 for row in rows if start < int(row["ts_ms"]) <= ts_ms)


def trailing_mean(
    rows: Sequence[Mapping[str, Any]], ts_ms: int, hours: int, key: str, multiplier: float
) -> float | None:
    values = trailing_values(rows, ts_ms, hours, key)
    return statistics.fmean(values) * multiplier if values else None


def trailing_min(
    rows: Sequence[Mapping[str, Any]], ts_ms: int, hours: int, key: str, multiplier: float
) -> float | None:
    values = trailing_values(rows, ts_ms, hours, key)
    return min(values) * multiplier if values else None


def trailing_change(
    rows: Sequence[Mapping[str, Any]], ts_ms: int, hours: int, key: str, multiplier: float
) -> float | None:
    current = latest_at_or_before(rows, ts_ms, key)
    previous = latest_at_or_before(rows, ts_ms - hours * HOUR_MS, key)
    return (current - previous) * multiplier if current is not None and previous is not None else None


def trailing_return(
    rows: Sequence[Mapping[str, Any]], ts_ms: int, hours: int, key: str
) -> float | None:
    current = latest_at_or_before(rows, ts_ms, key)
    previous = latest_at_or_before(rows, ts_ms - hours * HOUR_MS, key)
    if current is None or previous in (None, 0.0):
        return None
    return (current / previous - 1.0) * 100.0


def trailing_volume_z(rows: Sequence[Mapping[str, Any]], ts_ms: int, hours: int) -> float | None:
    eligible = [
        row
        for row in rows
        if ts_ms - hours * HOUR_MS <= int(row["ts_ms"]) <= ts_ms
        and finite_float(row.get("volume")) is not None
    ]
    if len(eligible) < 3:
        return None
    current = float(eligible[-1]["volume"])
    history = [float(row["volume"]) for row in eligible[:-1]]
    scale = statistics.pstdev(history)
    return (current - statistics.fmean(history)) / scale if scale > 1e-12 else 0.0


def funding_sign_streak(rows: Sequence[Mapping[str, Any]]) -> int:
    if not rows:
        return 0
    sign = rate_sign(float(rows[-1]["funding_rate"]))
    count = 0
    for row in reversed(rows):
        if rate_sign(float(row["funding_rate"])) != sign:
            break
        count += 1
    return count


def funding_interval_hours(rows: Sequence[Mapping[str, Any]]) -> float | None:
    if len(rows) < 2:
        return None
    recent = list(rows[-6:])
    deltas = [
        normalize_funding_interval_hours(
            (int(right["ts_ms"]) - int(left["ts_ms"])) / HOUR_MS
        )
        for left, right in zip(recent, recent[1:])
        if int(right["ts_ms"]) > int(left["ts_ms"])
    ]
    return statistics.median(deltas) if deltas else None


def latest_funding_interval_hours(
    rows: Sequence[Mapping[str, Any]],
) -> float | None:
    if len(rows) < 2:
        return None
    delta_h = normalize_funding_interval_hours(
        (int(rows[-1]["ts_ms"]) - int(rows[-2]["ts_ms"])) / HOUR_MS
    )
    return delta_h if delta_h > 0 else None


def funding_interval_change_ratio(
    rows: Sequence[Mapping[str, Any]],
) -> float | None:
    if len(rows) < 4:
        return None
    recent = list(rows[-7:])
    deltas = [
        normalize_funding_interval_hours(
            (int(right["ts_ms"]) - int(left["ts_ms"])) / HOUR_MS
        )
        for left, right in zip(recent, recent[1:])
        if int(right["ts_ms"]) > int(left["ts_ms"])
    ]
    if len(deltas) < 3:
        return None
    latest = deltas[-1]
    prior_deltas = deltas[:-1]
    baseline = statistics.median(prior_deltas) if prior_deltas else None
    return latest / baseline if latest is not None and baseline not in (None, 0.0) else None


def funding_rows_sum_bps(rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(float(row["funding_rate"]) for row in rows) * 10_000.0


def normalize_funding_interval_hours(value: float) -> float:
    for common in (0.5, 1.0, 2.0, 4.0, 8.0, 12.0, 24.0):
        if abs(value - common) <= 1.0 / 60.0:
            return common
    return round(value, 6)


def rate_sign(value: float) -> int:
    if value > 1e-12:
        return 1
    if value < -1e-12:
        return -1
    return 0


def mean_or_none(values: Sequence[float], *, multiplier: float = 1.0) -> float | None:
    return statistics.fmean(values) * multiplier if values else None


def std_or_none(values: Sequence[float], *, multiplier: float = 1.0) -> float | None:
    return statistics.pstdev(values) * multiplier if len(values) >= 2 else 0.0 if values else None


def optional_mul(value: float | None, multiplier: float) -> float | None:
    return value * multiplier if value is not None else None


def optional_diff(left: float | None, right: float | None) -> float | None:
    return left - right if left is not None and right is not None else None


def stable_bucket(value: str, modulus: int) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:16], 16) % modulus


def finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def finite_int(value: Any) -> int | None:
    parsed = finite_float(value)
    return int(parsed) if parsed is not None else None


def sigmoid(value: float) -> float:
    if value >= 0:
        exp_value = math.exp(-min(60.0, value))
        return 1.0 / (1.0 + exp_value)
    exp_value = math.exp(max(-60.0, value))
    return exp_value / (1.0 + exp_value)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid Funding Forecast input JSON: {path}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"Funding Forecast input is not an object: {path}")
    return dict(payload)


def render_report(
    metadata: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    veto_summary: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Strategy Lab — Funding Forecast v1",
        "",
        "Status: research replay only. No orders, ARM changes or live decisions.",
        "",
        f"- Event Lake run: `{metadata.get('input_run_id')}`",
        f"- Complete input gate: `{metadata.get('final_result_allowed')}`",
        f"- Physical windows considered: {metadata.get('physical_windows_considered')}",
        f"- Eligible samples: {metadata.get('eligible_samples')}",
        f"- Vetoed windows: {metadata.get('vetoed_windows')}",
        f"- Hourly-normalized funding samples: {metadata.get('hourly_funding_samples')}",
        f"- Detected interval shifts: {metadata.get('detected_interval_shift_samples')}",
        f"- Symbols: {metadata.get('symbols')}",
        "",
        "## Metrics",
        "",
        "| Split | Target | Model | Cost bps | Train | Eval | Accuracy | Brier | Log loss | MAE | RMSE | Gross bps | Net bps |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in metrics:
        lines.append(
            "| {split} | {target} | {model} | {cost_scenario_bps} | {train_rows} | {eval_rows} | {accuracy} | "
            "{brier} | {log_loss} | {mae} | {rmse} | {mean_gross_funding_bps} | "
            "{mean_net_after_cost_scenario_bps} |".format(
                **{key: row.get(key, "") for key in (
                    "split", "target", "model", "cost_scenario_bps", "train_rows", "eval_rows", "accuracy",
                    "brier", "log_loss", "mae", "rmse", "mean_gross_funding_bps",
                    "mean_net_after_cost_scenario_bps"
                )}
            )
        )
    lines.extend(["", "## Data-quality vetoes", ""])
    for row in veto_summary:
        lines.append(f"- `{row.get('reason')}`: {row.get('windows')}")
    lines.extend(
        [
            "",
            "Partial/in-progress runs validate plumbing only and must not be used for model conclusions.",
            "Economic execution and spread timing remain separate Strategy Lab phases.",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "FEATURE_NAMES",
    "FundingForecastConfig",
    "add_cross_exchange_features",
    "build_cross_exchange_context",
    "build_evaluation_splits",
    "build_funding_sample",
    "evaluate_forecasts",
    "fit_logistic",
    "run_funding_forecast",
]
