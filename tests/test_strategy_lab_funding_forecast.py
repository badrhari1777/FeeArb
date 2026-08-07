from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from analysis_features.strategy_lab_event_lake import EventLakeConfig, run_event_lake
from analysis_features.strategy_lab_funding_forecast import (
    FundingForecastConfig,
    add_cross_exchange_features,
    build_cross_exchange_context,
    build_evaluation_splits,
    build_funding_sample,
    evaluate_forecasts,
    run_funding_forecast,
)


BASE_TS = 1_767_225_600_000


def task(*, exchange: str = "binance", window_id: str = "window-one") -> dict[str, object]:
    return {
        "physical_window_id": window_id,
        "event_id": f"event-{exchange}",
        "event_type": "pump_trigger",
        "symbol": "TESTUSDT",
        "exchange": exchange,
        "event_ts_ms": BASE_TS,
        "start_ms": BASE_TS - 24 * 3_600_000,
        "end_ms": BASE_TS + 72 * 3_600_000,
        "timeframe": "5m",
    }


def dataset(rows: list[dict[str, float | int]]) -> dict[str, object]:
    return {"supported": True, "calls": 1, "rows": rows, "error": ""}


def window(*, exchange: str = "binance", window_id: str = "window-one") -> dict[str, object]:
    candle_rows = [
        {
            "ts_ms": BASE_TS - hours * 3_600_000,
            "open": 100.0 + (24 - hours),
            "high": 101.0 + (24 - hours),
            "low": 99.0 + (24 - hours),
            "close": 100.0 + (24 - hours),
            "volume": 100.0 + (24 - hours) * 2.0,
        }
        for hours in range(24, -1, -1)
    ]
    premium_rows = [
        {"ts_ms": BASE_TS - hours * 3_600_000, "close": -0.001 - hours * 0.00001}
        for hours in range(24, -1, -1)
    ]
    mark_rows = [
        {"ts_ms": BASE_TS - 3_600_000, "close": 100.2},
        {"ts_ms": BASE_TS, "close": 102.1},
    ]
    index_rows = [
        {"ts_ms": BASE_TS - 3_600_000, "close": 100.0},
        {"ts_ms": BASE_TS, "close": 102.0},
    ]
    oi_rows = [
        {"ts_ms": BASE_TS - 24 * 3_600_000, "open_interest": 1_000.0},
        {"ts_ms": BASE_TS - 4 * 3_600_000, "open_interest": 1_100.0},
        {"ts_ms": BASE_TS - 1 * 3_600_000, "open_interest": 1_150.0},
        {"ts_ms": BASE_TS, "open_interest": 1_200.0},
    ]
    funding_rows = [
        {"ts_ms": BASE_TS - 8 * 3_600_000, "funding_rate": -0.001},
        {"ts_ms": BASE_TS - 1 * 3_600_000, "funding_rate": -0.002},
        {"ts_ms": BASE_TS + 4 * 3_600_000, "funding_rate": -0.001},
        {"ts_ms": BASE_TS + 8 * 3_600_000, "funding_rate": 0.001},
        {"ts_ms": BASE_TS + 16 * 3_600_000, "funding_rate": 0.002},
    ]
    return {
        "schema": "strategy_lab_public_window_v4",
        "physical_window_id": window_id,
        "symbol": "TESTUSDT",
        "exchange": exchange,
        "start_ms": BASE_TS - 24 * 3_600_000,
        "end_ms": BASE_TS + 72 * 3_600_000,
        "timeframe": "5m",
        "public_only": True,
        "market": {"available": True},
        "series": {
            "ohlcv": dataset(candle_rows),
            "funding": dataset(funding_rows),
            "premium": dataset(premium_rows),
            "mark": dataset(mark_rows),
            "index": dataset(index_rows),
            "open_interest": dataset(oi_rows),
        },
    }


def test_sample_features_are_causal_and_targets_use_only_future_rows() -> None:
    config = FundingForecastConfig(min_train_rows=2, min_eval_rows=1)
    base_window = window()
    logical = [task(), {**task(), "event_id": "second-logical-source"}]

    sample = build_funding_sample(
        task=logical[0], logical_tasks=logical, window=base_window, config=config
    )
    changed_future = deepcopy(base_window)
    changed_future["series"]["premium"]["rows"].append(
        {"ts_ms": BASE_TS + 1, "close": 99.0}
    )
    changed_future["series"]["funding"]["rows"].append(
        {"ts_ms": BASE_TS + 1, "funding_rate": -0.5}
    )
    changed = build_funding_sample(
        task=logical[0], logical_tasks=logical, window=changed_future, config=config
    )

    assert sample["status"] == "eligible"
    assert sample["logical_event_count"] == 2
    assert sample["current_funding_bps"] == -20.0
    assert sample["funding_latest_interval_h"] == 7.0
    assert sample["current_funding_hourly_bps"] == -20.0 / 7.0
    assert sample["projected_next_funding_in_h"] == 6.0
    assert sample["funding_realized_8h_bps"] == -20.0
    assert sample["funding_realized_24h_bps"] == -30.0
    assert sample["target_next_funding_bps"] == -10.0
    assert sample["target_next_interval_h"] == 5.0
    assert sample["target_next_funding_hourly_bps"] == -2.0
    assert sample["target_cumulative_funding_4h_bps"] == -10.0
    assert sample["target_cumulative_funding_8h_bps"] == 0.0
    assert sample["target_cumulative_funding_24h_bps"] == 20.0
    assert sample["target_sign_persisted"] == 1
    assert sample["target_weakened"] == 1
    assert sample["target_same_sign_duration_h"] == 8.0
    assert sample["target_same_sign_settlements"] == 1
    assert sample["target_duration_censored"] == 0
    assert changed["premium_current_bps"] == sample["premium_current_bps"]
    assert changed["premium_mean_4h_bps"] == sample["premium_mean_4h_bps"]
    assert changed["funding_latest_interval_h"] == sample["funding_latest_interval_h"]
    assert changed["current_funding_hourly_bps"] == sample["current_funding_hourly_bps"]
    assert changed["target_next_funding_bps"] != sample["target_next_funding_bps"]


def test_missing_next_funding_is_vetoed_fail_closed() -> None:
    missing = window()
    missing["series"]["funding"]["rows"] = [
        row
        for row in missing["series"]["funding"]["rows"]
        if row["ts_ms"] <= BASE_TS
    ]

    sample = build_funding_sample(
        task=task(),
        logical_tasks=[task()],
        window=missing,
        config=FundingForecastConfig(min_train_rows=2, min_eval_rows=1),
    )

    assert sample["status"] == "veto"
    assert sample["veto_reason"] == "next_funding_missing"


def test_unobserved_sign_change_is_marked_as_censored_duration() -> None:
    unchanged = window()
    for row in unchanged["series"]["funding"]["rows"]:
        if row["ts_ms"] > BASE_TS:
            row["funding_rate"] = -abs(row["funding_rate"])

    sample = build_funding_sample(
        task=task(),
        logical_tasks=[task()],
        window=unchanged,
        config=FundingForecastConfig(min_train_rows=2, min_eval_rows=1),
    )

    assert sample["status"] == "eligible"
    assert sample["target_duration_censored"] == 1
    assert sample["target_same_sign_duration_h"] == 16.0
    assert sample["target_duration_observed_until_ts_ms"] == BASE_TS + 16 * 3_600_000


def test_hft_like_interval_shift_distinguishes_two_percent_per_8h_and_per_1h() -> None:
    config = FundingForecastConfig(min_train_rows=2, min_eval_rows=1)
    eight_hour = window(window_id="eight-hour")
    eight_hour["series"]["funding"]["rows"] = [
        {"ts_ms": BASE_TS - 16 * 3_600_000, "funding_rate": -0.0001},
        {"ts_ms": BASE_TS - 8 * 3_600_000, "funding_rate": -0.0002},
        {"ts_ms": BASE_TS, "funding_rate": -0.02},
        {"ts_ms": BASE_TS + 8 * 3_600_000, "funding_rate": -0.02},
    ]
    shifted = window(window_id="shifted")
    shifted["series"]["funding"]["rows"] = [
        {"ts_ms": BASE_TS - 17 * 3_600_000, "funding_rate": -0.0001},
        {"ts_ms": BASE_TS - 9 * 3_600_000, "funding_rate": -0.0002},
        {"ts_ms": BASE_TS - 1 * 3_600_000, "funding_rate": -0.0004},
        {"ts_ms": BASE_TS, "funding_rate": -0.02},
        {"ts_ms": BASE_TS + 1 * 3_600_000, "funding_rate": -0.02},
        {"ts_ms": BASE_TS + 2 * 3_600_000, "funding_rate": -0.00052006},
        {"ts_ms": BASE_TS + 3 * 3_600_000, "funding_rate": -0.00225497},
    ]

    eight_sample = build_funding_sample(
        task=task(window_id="eight-hour"),
        logical_tasks=[task(window_id="eight-hour")],
        window=eight_hour,
        config=config,
    )
    shifted_sample = build_funding_sample(
        task=task(window_id="shifted"),
        logical_tasks=[task(window_id="shifted")],
        window=shifted,
        config=config,
    )

    assert eight_sample["funding_latest_interval_h"] == 8.0
    assert eight_sample["current_funding_hourly_bps"] == -25.0
    assert eight_sample["target_next_funding_hourly_bps"] == -25.0
    assert shifted_sample["funding_latest_interval_h"] == 1.0
    assert shifted_sample["funding_interval_change_ratio"] == 0.125
    assert shifted_sample["current_funding_hourly_bps"] == -200.0
    assert shifted_sample["target_next_funding_hourly_bps"] == -200.0
    assert shifted_sample["target_cumulative_funding_4h_bps"] == -227.7503


def test_cross_exchange_features_keep_each_exchange_direction() -> None:
    left = build_funding_sample(
        task=task(exchange="binance", window_id="left"),
        logical_tasks=[task(exchange="binance", window_id="left")],
        window=window(exchange="binance", window_id="left"),
        config=FundingForecastConfig(min_train_rows=2, min_eval_rows=1),
    )
    right = dict(left)
    right.update(
        {
            "sample_id": "right",
            "physical_window_id": "right",
            "exchange": "bybit",
            "current_funding_bps": -5.0,
            "current_funding_hourly_bps": -1.0,
            "premium_current_bps": -3.0,
        }
    )

    add_cross_exchange_features([left, right])

    assert left["funding_cross_exchange_diff_bps"] == -15.0
    assert right["funding_cross_exchange_diff_bps"] == 15.0
    assert left["funding_hourly_cross_exchange_diff_bps"] == -20.0 / 7.0 + 1.0
    assert left["premium_cross_exchange_diff_bps"] == left["premium_current_bps"] + 3.0


def test_unlabelled_venue_still_supplies_causal_cross_exchange_context() -> None:
    config = FundingForecastConfig(min_train_rows=2, min_eval_rows=1)
    left_task = task(exchange="binance", window_id="left")
    left_window = window(exchange="binance", window_id="left")
    left = build_funding_sample(
        task=left_task,
        logical_tasks=[left_task],
        window=left_window,
        config=config,
    )
    right_task = task(exchange="bybit", window_id="right")
    right_window = window(exchange="bybit", window_id="right")
    right_window["series"]["funding"]["rows"] = []
    right_label = build_funding_sample(
        task=right_task,
        logical_tasks=[right_task],
        window=right_window,
        config=config,
    )
    contexts = [
        build_cross_exchange_context(task=left_task, window=left_window, config=config),
        build_cross_exchange_context(task=right_task, window=right_window, config=config),
    ]

    add_cross_exchange_features([left], contexts=[row for row in contexts if row])

    assert right_label["veto_reason"] == "current_funding_missing"
    assert left["other_funding_bps"] is None
    assert left["other_premium_bps"] is not None
    assert left["premium_cross_exchange_diff_bps"] is not None
    assert left["other_oi_change_4h_pct"] is not None
    assert left["oi_cross_exchange_diff_4h_pct"] is not None


def synthetic_samples(count: int = 120) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    symbols = [f"S{index}USDT" for index in range(12)]
    for index in range(count):
        driver = -2.0 if index % 4 in (0, 1) else 2.0
        target = 1 if driver > 0 else 0
        current = 1.0 if index % 2 == 0 else -1.0
        row: dict[str, object] = {
            "sample_id": f"sample-{index}",
            "symbol": symbols[index % len(symbols)],
            "exchange": "binance" if index % 2 == 0 else "bybit",
            "event_ts_ms": BASE_TS + index * 3_600_000,
            "current_funding_bps": current,
            "current_funding_hourly_bps": current,
            "funding_latest_interval_h": 1.0,
            "premium_current_bps": driver,
            "target_next_positive": target,
            "target_weakened": target,
            "target_next_funding_bps": driver * 3.0,
            "target_next_funding_hourly_bps": driver * 3.0,
            "target_next_interval_h": 1.0,
            "target_same_sign_duration_h": 8.0 + driver,
            "target_cumulative_funding_4h_bps": driver * 2.0,
            "target_cumulative_funding_8h_bps": driver * 3.0,
            "target_cumulative_funding_24h_bps": driver * 5.0,
        }
        rows.append(row)
    return rows


def test_splits_are_strictly_chronological_and_symbol_disjoint() -> None:
    rows = synthetic_samples()
    splits = build_evaluation_splits(
        rows, FundingForecastConfig(min_train_rows=2, min_eval_rows=1)
    )
    by_name = {name: (train, evaluation) for name, train, evaluation in splits}
    block_one_train, block_one_eval = by_name["chronological_block_1"]
    validation_train, validation_eval = by_name["chronological_validation"]
    chrono_train, chrono_test = by_name["chronological_test"]
    symbol_train, symbol_test = by_name["unseen_symbol_holdout"]

    assert max(int(row["event_ts_ms"]) for row in block_one_train) < min(
        int(row["event_ts_ms"]) for row in block_one_eval
    )
    assert max(int(row["event_ts_ms"]) for row in block_one_eval) < min(
        int(row["event_ts_ms"]) for row in validation_eval
    )
    assert max(int(row["event_ts_ms"]) for row in validation_train) < min(
        int(row["event_ts_ms"]) for row in validation_eval
    )

    assert max(int(row["event_ts_ms"]) for row in chrono_train) < min(
        int(row["event_ts_ms"]) for row in chrono_test
    )
    assert {row["symbol"] for row in symbol_train}.isdisjoint(
        {row["symbol"] for row in symbol_test}
    )


def test_logistic_forecast_beats_current_sign_baseline_on_synthetic_driver() -> None:
    metrics, calibration, coefficients, predictions = evaluate_forecasts(
        synthetic_samples(),
        FundingForecastConfig(
            min_train_rows=20,
            min_eval_rows=5,
            logistic_iterations=800,
        ),
    )
    chrono = [
        row
        for row in metrics
        if row.get("split") == "chronological_test" and row.get("target") == "next_sign"
    ]
    model = next(row for row in chrono if row["model"] == "logistic_v1")
    baseline = next(row for row in chrono if row["model"] == "current_sign_persistence")

    assert model["accuracy"] > 0.9
    assert model["brier"] < baseline["brier"]
    economic = next(
        row
        for row in metrics
        if row.get("split") == "chronological_test"
        and row.get("target") == "funding_capture_24h_bps_proxy"
        and row.get("model") == "logistic_sign_direction"
    )
    assert "mean_net_after_cost_scenario_bps" in economic
    assert 0.0 <= economic["top1_abs_contribution_share"] <= 1.0
    assert 0.0 <= economic["top5_abs_contribution_share"] <= 1.0
    assert 0.0 <= economic["top_symbol_abs_contribution_share"] <= 1.0
    assert economic["mean_gross_without_top5_abs_bps"] is not None
    sensitivity = {
        row["cost_scenario_bps"]
        for row in metrics
        if row.get("split") == "chronological_test"
        and row.get("target") == "funding_capture_24h_bps_proxy"
        and row.get("model") == "logistic_sign_direction"
    }
    assert sensitivity == {0.0, 4.0, 8.0, 12.0, 16.0}
    assert any(row.get("target") == "next_hourly_magnitude_bps" for row in metrics)
    assert any(row.get("target") == "next_interval_h" for row in metrics)
    assert calibration
    assert coefficients
    assert predictions


def test_complete_event_lake_fixture_writes_research_only_artifacts(tmp_path: Path) -> None:
    class FundingFixtureProvider:
        public_only = True

        def fetch_window(self, source_task: dict[str, object]) -> dict[str, object]:
            result = window(
                exchange=str(source_task["exchange"]),
                window_id=str(source_task["physical_window_id"]),
            )
            result.update(
                {
                    "symbol": source_task["symbol"],
                    "start_ms": source_task["start_ms"],
                    "end_ms": source_task["end_ms"],
                    "timeframe": source_task["timeframe"],
                }
            )
            return result

    lake_dir = tmp_path / "lake"
    output_dir = tmp_path / "forecast"
    run_event_lake(
        output_dir=lake_dir,
        config=EventLakeConfig(exchanges=("binance", "bybit"), max_events=1),
        catalog_rows=[
            {
                "pump_event_id": "fixture|TESTUSDT|event",
                "source": "fixture",
                "event_type": "pump_trigger",
                "symbol": "TESTUSDT",
                "ts_ms": BASE_TS,
                "ts_iso": "2026-01-01T00:00:00+00:00",
            }
        ],
        execute_public=True,
        provider=FundingFixtureProvider(),
        code_commit="fixture-commit",
    )

    result = run_funding_forecast(
        input_dir=lake_dir,
        output_dir=output_dir,
        config=FundingForecastConfig(
            min_train_rows=2,
            min_eval_rows=1,
            logistic_iterations=100,
        ),
        code_commit="forecast-commit",
    )
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))

    assert result["eligible_samples"] == 2
    assert metadata["final_result_allowed"] is True
    assert metadata["live_actions"] is False
    assert metadata["paper_promotion_allowed"] is False
    assert metadata["shadow_promotion_allowed"] is False
    assert metadata["cross_exchange_premium_samples"] == 2
    assert metadata["hourly_funding_samples"] == 2
    assert metadata["economic_horizons_h"] == [4, 8, 24]
    assert metadata["economic_cost_scenarios_bps"] == [0.0, 4.0, 8.0, 12.0, 16.0]
    assert (output_dir / "samples.csv").exists()
    assert "Partial/in-progress runs" in (output_dir / "index.md").read_text(
        encoding="utf-8"
    )
