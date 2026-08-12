from __future__ import annotations

import asyncio
import gzip
import json
import os
from pathlib import Path

import pytest

from strategy_lab.preflight import (
    PREFLIGHT_VERSION,
    _process_memory_bytes,
    eligible_registry_symbols,
    exclusive_preflight_lock,
    run_capacity_preflight,
    verified_observation_symbols,
)


def _registry() -> dict:
    return {
        "vectors": {
            symbol: {
                "binance": {"exchange_symbol": symbol},
                "bybit": {"exchange_symbol": symbol},
                "okx": {"exchange_symbol": symbol},
                "kucoin": {"exchange_symbol": symbol},
                "gate": {"exchange_symbol": symbol},
            }
            for symbol in ("AAAUSDT", "BBBUSDT", "CCCUSDT")
        }
    }


async def _successful_feed(_registry, symbols, *, duration_sec, max_symbols):
    del duration_sec, max_symbols
    venues = ("binance", "bybit", "okx", "kucoin", "gate")
    observations = [
        {
            "observation_version": "test",
            "exchange": venue,
            "canonical_symbol": symbol,
            "received_at_ms": 1,
            "best_bid": 100.0,
            "best_ask": 101.0,
            "mark_price": 100.5,
            "funding_rate": 0.001,
            "open_interest": 1000.0,
            "volume_24h_quote": 50000.0,
        }
        for symbol in symbols
        for venue in venues
    ]
    expected = len(observations)
    return {
        "observations": observations,
        "observation_count": expected,
        "plan": {"expected_pairs": expected},
        "pair_coverage_pct": 100.0,
        "invalid_bbo": [],
        "venue_coverage": {
            venue: {"expected": len(symbols), "observed": len(symbols), "coverage_pct": 100.0}
            for venue in venues
        },
        "venue_status": {
            venue: {
                "status": "completed",
                "connections": 1,
                "messages": 2,
                "updates": len(symbols),
                "parse_errors": 0,
                "subscription_errors": 0,
                "rest_errors": 0,
                "error": None,
            }
            for venue in venues
        },
        "field_availability": {
            field: {
                venue: len(symbols)
                if field in {
                    "best_bid", "best_ask", "mark_price", "funding_rate",
                    "open_interest", "volume_24h_quote",
                }
                else 0
                for venue in venues
            }
            for field in (
                "best_bid", "best_ask", "last_price", "mark_price", "index_price",
                "funding_rate", "predicted_funding_rate", "next_funding_time_ms",
                "open_interest", "volume_24h_quote",
            )
        },
    }


def test_eligible_symbols_require_two_venues_and_preserve_candidate_order():
    registry = _registry()
    registry["vectors"]["ONEUSDT"] = {"binance": {"exchange_symbol": "ONEUSDT"}}
    assert eligible_registry_symbols(
        registry, ["BBBUSDT", "ONEUSDT", "AAAUSDT", "BBBUSDT", "MISSING"]
    ) == ["BBBUSDT", "AAAUSDT"]


def test_verified_symbols_preserve_union_order_and_exclude_registry_vetoes():
    verification = [
        {"canonical_symbol": "AAAUSDT", "eligible_for_observation": True},
        {"canonical_symbol": "BBBUSDT", "eligible_for_observation": False},
        {"canonical_symbol": "CCCUSDT", "eligible_for_observation": True},
        # A second provider may independently verify the same canonical asset.
        {"canonical_symbol": "BBBUSDT", "eligible_for_observation": True},
    ]
    assert verified_observation_symbols(
        ["CCCUSDT", "BBBUSDT", "AAAUSDT", "CCCUSDT", "VETOUSDT"], verification
    ) == ["CCCUSDT", "BBBUSDT", "AAAUSDT"]


def test_process_memory_probe_is_available_on_windows():
    current, peak = _process_memory_bytes()
    if os.name == "nt":
        assert current is not None
        assert peak is not None
    if current is not None:
        assert current > 0
        assert peak is not None and peak >= current


def test_capacity_preflight_rotates_symbols_and_writes_auditable_artifacts(tmp_path: Path):
    output_dir = tmp_path / "preflight-test"
    report = asyncio.run(run_capacity_preflight(
        _registry(),
        ["AAAUSDT", "BBBUSDT", "CCCUSDT"],
        output_dir=output_dir,
        duration_sec=2,
        cycle_interval_sec=1,
        cycle_duration_sec=1,
        max_symbols_per_cycle=2,
        feed_runner=_successful_feed,
    ))

    assert report["preflight_version"] == PREFLIGHT_VERSION
    assert report["mode"] == "research_only_no_trading"
    assert report["trade_signal"] is False
    assert report["cycles"]["attempted"] == 2
    assert report["cycles"]["failed"] == 0
    assert report["eligible_symbol_count"] == 3
    assert report["sampled_symbol_count"] == 3
    assert report["feed"]["pair_coverage_pct"] == 100.0
    assert report["feed"]["connections"] == 10
    assert report["feed"]["field_coverage"]["open_interest"]["okx"]["coverage_pct"] == 100.0
    assert report["qa"] == {"verdict": "PASS", "failures": [], "warnings": []}
    assert report["resources"]["compressed_bytes_per_row"] > 0

    persisted = json.loads((output_dir / "report.json").read_text(encoding="utf-8"))
    status = json.loads((output_dir / "status.json").read_text(encoding="utf-8"))
    cycles = [json.loads(line) for line in (output_dir / "cycles.jsonl").read_text(encoding="utf-8").splitlines()]
    with gzip.open(output_dir / "observations.jsonl.gz", "rt", encoding="utf-8") as stream:
        observations = [json.loads(line) for line in stream]
    assert persisted["qa"]["verdict"] == "PASS"
    assert status["status"] == "completed"
    assert [row["symbols"] for row in cycles] == [
        ["AAAUSDT", "BBBUSDT"],
        ["CCCUSDT", "AAAUSDT"],
    ]
    assert len(observations) == 20
    assert {row["cycle_index"] for row in observations} == {0, 1}


def test_capacity_preflight_continues_after_cycle_error_and_fails_qa(tmp_path: Path):
    calls = 0

    async def flaky_feed(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary feed failure")
        return await _successful_feed(*args, **kwargs)

    report = asyncio.run(run_capacity_preflight(
        _registry(),
        ["AAAUSDT", "BBBUSDT"],
        output_dir=tmp_path / "flaky",
        duration_sec=2,
        cycle_interval_sec=1,
        cycle_duration_sec=1,
        max_symbols_per_cycle=2,
        feed_runner=flaky_feed,
    ))

    assert report["cycles"]["attempted"] == 2
    assert report["cycles"]["completed"] == 1
    assert report["cycles"]["failed"] == 1
    assert report["qa"]["verdict"] == "FAIL"
    assert "cycle_success_below_90pct" in report["qa"]["failures"]


def test_preflight_lock_rejects_live_owner_and_recovers_stale_pid(tmp_path: Path, monkeypatch):
    lock_path = tmp_path / "preflight.lock"
    lock_path.write_text(json.dumps({"pid": 123}), encoding="utf-8")
    monkeypatch.setattr("strategy_lab.preflight._pid_is_alive", lambda pid: pid == 123)
    with pytest.raises(RuntimeError, match="already_running"):
        with exclusive_preflight_lock(lock_path):
            pass

    monkeypatch.setattr("strategy_lab.preflight._pid_is_alive", lambda _pid: False)
    with exclusive_preflight_lock(lock_path):
        assert lock_path.exists()
    assert not lock_path.exists()


@pytest.mark.parametrize("duration", [0, 86401])
def test_capacity_preflight_rejects_out_of_bounds_duration(tmp_path: Path, duration: float):
    with pytest.raises(ValueError, match="duration_sec"):
        asyncio.run(run_capacity_preflight(
            _registry(), ["AAAUSDT"], output_dir=tmp_path / str(duration), duration_sec=duration
        ))
