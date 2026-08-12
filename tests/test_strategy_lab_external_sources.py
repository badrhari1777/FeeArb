from __future__ import annotations

import asyncio
from pathlib import Path

from strategy_lab.arbitragescanner_source import parse_arbitragescanner_payload
from strategy_lab.coinglass_source import parse_coinglass_dom_rows
from strategy_lab.external_contract import ExternalLeg, ExternalObservation, merge_external_candidates
from strategy_lab.instrument_registry import InstrumentContract, build_registry_payload
from strategy_lab.observatory import StrategyLabObservatory


def _arbitragescanner_fixture() -> list[dict[str, object]]:
    return [
        {
            "tokenId": "layer-token-id",
            "symbol": "LAYERUSDT",
            "ticker": "LAYER",
            "maxSpread": 9.9,
            "rates": [
                {"exchange": "binance_futures", "rate": 0.12, "symbol": "LAYERUSDT"},
                {"exchange": "bybit_futures", "rate": -0.08, "symbol": "LAYERUSDT"},
                {"exchange": "okex_futures", "rate": 0.02, "symbol": "LAYER-USDT-SWAP"},
                {"exchange": "kucoin_futures", "rate": 0.04, "symbol": "LAYERUSDTM"},
                {"exchange": "gate_futures", "rate": 0.01, "symbol": "LAYER_USDT"},
                {"exchange": "mexc_futures", "rate": -5.0, "symbol": "LAYER_USDT"},
            ],
        },
        {
            "tokenId": "single-leg",
            "symbol": "ONLYUSDT",
            "rates": [{"exchange": "binance_futures", "rate": 0.1, "symbol": "ONLYUSDT"}],
        },
    ]


def test_arbitragescanner_uses_exact_five_correct_sign_and_provider_symbols() -> None:
    result = parse_arbitragescanner_payload(
        _arbitragescanner_fixture(),
        observed_at="2026-08-12T00:00:00+00:00",
    )

    assert result["raw_count"] == 2
    assert result["eligible_count"] == 1
    row = result["observations"][0]
    assert row.source_asset_id == "arbitragescanner:layer-token-id"
    assert row.canonical_symbol == "LAYER"
    assert row.long_exchange == "bybit"
    assert row.short_exchange == "binance"
    assert row.funding_dispersion == 0.002
    assert {leg.exchange for leg in row.legs} == {"binance", "bybit", "okx", "kucoin", "gate"}
    assert next(leg.exchange_symbol for leg in row.legs if leg.exchange == "okx") == "LAYER-USDT-SWAP"
    assert len(result["quarantined"]) == 1


def test_arbitragescanner_rejects_html_shape() -> None:
    try:
        parse_arbitragescanner_payload({"html": "not a list"})
    except ValueError as exc:
        assert "must be a list" in str(exc)
    else:  # pragma: no cover - assertion branch
        raise AssertionError("invalid response shape was accepted")


def test_coinglass_dom_rows_require_exact_target_pair() -> None:
    result = parse_coinglass_dom_rows(
        [
            {
                "cells": [
                    "1",
                    "Long KAITO/USDT KuCoin\nShort KAITO/USDT Bybit",
                    "444.24%",
                    "0.2029%",
                    "-0.85%",
                    "$8.72M\n$9.70M",
                ]
            },
            {
                "cells": [
                    "2",
                    "Long TEST/USDT MEXC\nShort TEST/USDT Bybit",
                    "100%",
                    "0.1%",
                    "0.2%",
                ]
            },
        ],
        observed_at="2026-08-12T00:00:00+00:00",
    )

    assert result["eligible_count"] == 1
    row = result["observations"][0]
    assert row.canonical_symbol == "KAITO"
    assert row.long_exchange == "kucoin"
    assert row.short_exchange == "bybit"
    assert row.source_net_funding_rate == 0.002029
    assert row.source_spread_rate == -0.0085
    assert result["quarantined"][0]["reason"] == "exchange_outside_exact_five"


def _observation(source: str, symbol: str, *, rank: int | None = None, asset_id: str | None = None) -> ExternalObservation:
    return ExternalObservation(
        source=source,
        source_asset_id=asset_id or f"{source}:{symbol}",
        canonical_symbol=symbol,
        observed_at="2026-08-12T00:00:00+00:00",
        legs=[ExternalLeg("bybit", f"{symbol}USDT"), ExternalLeg("binance", f"{symbol}USDT")],
        long_exchange="bybit",
        short_exchange="binance",
        funding_dispersion=0.001,
        source_rank=rank,
    )


def test_union_prioritizes_overlap_and_quarantines_asset_id_collision() -> None:
    rows = merge_external_candidates(
        [_observation("coinglass", "ACE", rank=2), _observation("coinglass", "BMT", rank=1)],
        [
            _observation("arbitragescanner", "ACE", asset_id="arbitragescanner:ace"),
            _observation("arbitragescanner", "DUP", asset_id="arbitragescanner:dup-a"),
            _observation("arbitragescanner", "DUP", asset_id="arbitragescanner:dup-b"),
        ],
    )

    assert [row["canonical_symbol"] for row in rows] == ["ACE", "BMT"]
    assert rows[0]["source_overlap"] is True
    assert rows[0]["trade_signal"] is False


def test_observatory_preserves_last_good_on_empty_refresh(tmp_path: Path) -> None:
    observatory = StrategyLabObservatory(state_dir=tmp_path)
    good = _observation("arbitragescanner", "ACE")

    async def good_fetch() -> dict[str, object]:
        return {
            "observed_at": "2026-08-12T00:00:00+00:00",
            "raw_count": 1,
            "observations": [good],
            "quarantined": [],
        }

    async def empty_fetch() -> dict[str, object]:
        return {"raw_count": 0, "observations": [], "quarantined": []}

    first = asyncio.run(
        observatory.refresh(
            sources=["arbitragescanner"],
            fetchers={"arbitragescanner": good_fetch},
        )
    )
    second = asyncio.run(
        observatory.refresh(
            sources=["arbitragescanner"],
            fetchers={"arbitragescanner": empty_fetch},
        )
    )

    assert first["candidate_count"] == 1
    assert second["candidate_count"] == 1
    assert second["sources"]["arbitragescanner"]["status"] == "stale"
    assert second["sources"]["arbitragescanner"]["last_good_used"] is True
    assert (tmp_path / "latest.json").exists()


def _registry_fixture() -> dict[str, object]:
    rows = {
        "binance": [InstrumentContract("binance", "ACE", "ACEUSDT", "ACE", "USDT", "USDT", "TRADING", True)],
        "bybit": [InstrumentContract("bybit", "ACE", "ACEUSDT", "ACE", "USDT", "USDT", "Trading", True)],
        "okx": [],
        "kucoin": [],
        "gate": [],
    }
    source_status = {
        exchange: {"status": "fresh", "count": len(items), "error": None}
        for exchange, items in rows.items()
    }
    return build_registry_payload(rows, source_status=source_status)


def _feed_report_fixture() -> dict[str, object]:
    return {
        "mode": "bounded_research_only_no_trading",
        "scheduler_enabled": False,
        "observation_count": 2,
        "pair_coverage_pct": 100.0,
        "symbols_with_two_venues": 1,
        "invalid_bbo": [],
        "venue_status": {
            "binance": {"subscription_errors": 0},
            "bybit": {"subscription_errors": 0},
        },
        "venue_coverage": {
            "binance": {"expected": 1, "observed": 1, "coverage_pct": 100.0},
            "bybit": {"expected": 1, "observed": 1, "coverage_pct": 100.0},
        },
        "missing_pairs": [],
        "freshness_ms": {"max": 10, "median": 5},
        "field_availability": {},
        "observations": [],
        "trade_signal": False,
        "research_only": True,
    }


def test_observatory_registry_and_feed_are_bounded_and_preserve_last_good(tmp_path: Path) -> None:
    observatory = StrategyLabObservatory(state_dir=tmp_path)
    good = _observation("arbitragescanner", "ACE")

    async def source_fetch() -> dict[str, object]:
        return {"observed_at": good.observed_at, "raw_count": 1, "observations": [good], "quarantined": []}

    async def registry_fetch(observations) -> dict[str, object]:
        assert [item.canonical_symbol for item in observations] == ["ACE"]
        return _registry_fixture()

    async def feed_runner(registry, symbols, *, duration_sec, max_symbols) -> dict[str, object]:
        assert set(registry["vectors"]["ACE"]) == {"binance", "bybit"}
        assert symbols == ["ACE"]
        assert duration_sec == 7
        assert max_symbols == 1
        return _feed_report_fixture()

    asyncio.run(observatory.refresh(sources=["arbitragescanner"], fetchers={"arbitragescanner": source_fetch}))
    registry_state = asyncio.run(observatory.refresh_registry(registry_fetcher=registry_fetch))
    feed_state = asyncio.run(
        observatory.run_feed_probe(duration_sec=7, max_symbols=1, feed_runner=feed_runner)
    )

    assert registry_state["registry"]["status"] == "fresh"
    assert registry_state["registry"]["eligible_candidate_count"] == 1
    assert set(registry_state["registry"]["snapshot"]["vectors"]) == {"ACE"}
    assert feed_state["feed_probe"]["status"] == "fresh"
    assert feed_state["feed_probe"]["quality"]["ready_for_bounded_research"] is True
    assert feed_state["feed_probe"]["report"]["trade_signal"] is False

    async def registry_failure(_observations):
        raise RuntimeError("registry unavailable")

    async def feed_failure(*_args, **_kwargs):
        raise RuntimeError("feed unavailable")

    stale_feed = asyncio.run(observatory.run_feed_probe(feed_runner=feed_failure))
    stale_registry = asyncio.run(observatory.refresh_registry(registry_fetcher=registry_failure))

    assert stale_registry["registry"]["status"] == "stale"
    assert stale_registry["registry"]["last_good_used"] is True
    assert stale_registry["registry"]["snapshot"]["vectors"]["ACE"]["bybit"]["active"] is True
    assert stale_feed["feed_probe"]["status"] == "stale"
    assert stale_feed["feed_probe"]["last_good_used"] is True
    assert stale_feed["feed_probe"]["report"]["pair_coverage_pct"] == 100.0

    restored = StrategyLabObservatory(state_dir=tmp_path).status()
    assert restored["registry"]["status"] == "stale"
    assert restored["feed_probe"]["status"] == "stale"
    assert restored["scheduler_enabled"] is False


def test_external_refresh_marks_registry_and_feed_stale(tmp_path: Path) -> None:
    observatory = StrategyLabObservatory(state_dir=tmp_path)
    first_observation = _observation("arbitragescanner", "ACE")

    async def source_fetch() -> dict[str, object]:
        return {
            "observed_at": first_observation.observed_at,
            "raw_count": 1,
            "observations": [first_observation],
            "quarantined": [],
        }

    async def registry_fetch(_observations) -> dict[str, object]:
        return _registry_fixture()

    async def feed_runner(*_args, **_kwargs) -> dict[str, object]:
        return _feed_report_fixture()

    asyncio.run(observatory.refresh(sources=["arbitragescanner"], fetchers={"arbitragescanner": source_fetch}))
    asyncio.run(observatory.refresh_registry(registry_fetcher=registry_fetch))
    asyncio.run(observatory.run_feed_probe(feed_runner=feed_runner))
    refreshed = asyncio.run(
        observatory.refresh(sources=["arbitragescanner"], fetchers={"arbitragescanner": source_fetch})
    )

    assert refreshed["registry"]["status"] == "stale"
    assert refreshed["registry"]["last_good_used"] is True
    assert refreshed["feed_probe"]["status"] == "stale"
    assert refreshed["feed_probe"]["last_good_used"] is True
    try:
        asyncio.run(observatory.run_feed_probe(feed_runner=feed_runner))
    except ValueError as exc:
        assert "Registry is stale" in str(exc)
    else:  # pragma: no cover - assertion branch
        raise AssertionError("feed probe accepted stale Instrument Registry")
