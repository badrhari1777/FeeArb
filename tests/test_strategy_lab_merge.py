from __future__ import annotations

import pytest

from analysis_features.strategy_lab_merge import merge_local_public_windows


def window(source: str, *, exchange: str = "binance") -> dict:
    public = source == "public"
    return {
        "event_id": "event-1",
        "symbol": "TESTUSDT",
        "exchange": exchange,
        "start_ms": 1,
        "end_ms": 2,
        "series": {
            "ohlcv": {
                "resolution": None if public else "1h",
                "rows": [{"ts_ms": 1}] * (12 if public else 1),
                "error": "",
            },
            "funding": {
                "resolution": "event",
                "rows": [] if public else [{"ts_ms": 1}],
                "error": "",
            },
            "open_interest": {"rows": [], "error": "missing"},
        },
    }


def test_merge_prefers_finer_public_ohlcv_and_local_gap_fill() -> None:
    merged = merge_local_public_windows(window("local"), window("public"))

    assert merged["series"]["ohlcv"]["selected_source"] == "public_cache"
    assert merged["series"]["ohlcv"]["resolution"] == "5m"
    assert merged["series"]["funding"]["selected_source"] == "local_archive"
    assert merged["series"]["open_interest"]["selected_source"] is None
    assert len(merged["series"]["ohlcv"]["provenance"]) == 2


def test_merge_is_deterministic_and_does_not_mix_rows() -> None:
    first = merge_local_public_windows(window("local"), window("public"))
    second = merge_local_public_windows(window("local"), window("public"))

    assert first == second
    assert len(first["series"]["ohlcv"]["rows"]) == 12
    assert first["series"]["ohlcv"]["selected_hash"] == second["series"]["ohlcv"]["selected_hash"]


def test_merge_rejects_identity_conflict() -> None:
    with pytest.raises(ValueError, match="exchange"):
        merge_local_public_windows(window("local"), window("public", exchange="bybit"))
