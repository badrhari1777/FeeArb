from __future__ import annotations

import asyncio

from strategy_lab.instrument_registry import InstrumentContract, build_registry_payload
from strategy_lab.public_feed import (
    build_probe_plan,
    parse_binance_message,
    parse_binance_premium_index,
    parse_bybit_message,
    parse_gate_message,
    parse_kucoin_message,
    parse_okx_message,
    run_bounded_public_feed,
)


def _registry() -> dict:
    rows = {
        "binance": [InstrumentContract("binance", "BTC", "BTCUSDT", "BTC", "USDT", "USDT", "TRADING", True)],
        "bybit": [InstrumentContract("bybit", "BTC", "BTCUSDT", "BTC", "USDT", "USDT", "Trading", True)],
        "okx": [InstrumentContract("okx", "BTC", "BTC-USDT-SWAP", "BTC", "USDT", "USDT", "live", True)],
        "kucoin": [InstrumentContract("kucoin", "BTC", "XBTUSDTM", "BTC", "USDT", "USDT", "Open", True)],
        "gate": [InstrumentContract("gate", "BTC", "BTC_USDT", "BTC", "USDT", "USDT", "trading", True)],
    }
    return build_registry_payload(rows)


def test_parsers_normalize_five_venue_market_fields() -> None:
    binance = parse_binance_message({"e": "markPriceUpdate", "E": 1000, "s": "BTCUSDT", "p": "100", "i": "99", "r": "0.001", "T": 2000})[0]
    bybit = parse_bybit_message({"topic": "tickers.BTCUSDT", "ts": 1000, "data": {"symbol": "BTCUSDT", "bid1Price": "99", "ask1Price": "101", "markPrice": "100", "fundingRate": "0.001"}})[0]
    okx = parse_okx_message({"arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"}, "data": [{"instId": "BTC-USDT-SWAP", "fundingRate": "0.001", "premium": "-0.002", "fundingTime": "2000", "ts": "1000"}]})[0]
    gate = parse_gate_message({"event": "update", "channel": "futures.book_ticker", "result": {"s": "BTC_USDT", "b": "99", "a": "101", "t": 1000}})[0]
    kucoin = parse_kucoin_message({"type": "message", "topic": "/contract/instrument:XBTUSDTM", "subject": "mark.index.price", "data": {"markPrice": 100, "indexPrice": 99, "timestamp": 1000}})[0]

    assert binance["funding_rate"] == 0.001
    assert bybit["best_bid"] == 99.0 and bybit["best_ask"] == 101.0
    assert okx["premium_index"] == -0.002
    assert gate["exchange_symbol"] == "BTC_USDT"
    assert kucoin["mark_price"] == 100.0 and kucoin["index_price"] == 99.0


def test_binance_bulk_premium_snapshot_normalizes_mark_and_funding() -> None:
    rows = parse_binance_premium_index([
        {
            "symbol": "BTCUSDT",
            "markPrice": "100",
            "indexPrice": "99",
            "lastFundingRate": "0.001",
            "nextFundingTime": 2000,
            "time": 1000,
        }
    ])

    assert rows == [
        {
            "exchange_symbol": "BTCUSDT",
            "source_channel": "premiumIndex.rest",
            "source_ts_ms": 1_000_000,
            "mark_price": 100.0,
            "index_price": 99.0,
            "funding_rate": 0.001,
            "next_funding_time_ms": 2_000_000,
        }
    ]


def test_probe_plan_uses_registry_exact_symbols_and_one_connection_per_venue() -> None:
    plan = build_probe_plan(_registry(), ["BTC"])

    assert plan["expected_pairs"] == 5
    assert plan["expected_connections"] == 5
    assert plan["venues"]["okx"][0]["exchange_symbol"] == "BTC-USDT-SWAP"
    assert plan["venues"]["kucoin"][0]["exchange_symbol"] == "XBTUSDTM"


def test_bounded_probe_is_research_only_and_reports_coverage() -> None:
    async def fake_runner(symbols, deadline, emit, stats):
        del deadline
        stats["connections"] += 1
        for symbol in symbols:
            stats["messages"] += 1
            stats["updates"] += 1
            emit(symbol, {"exchange_symbol": symbol, "source_channel": "fixture", "source_ts_ms": 1000, "best_bid": 99.0, "best_ask": 101.0})

    result = asyncio.run(
        run_bounded_public_feed(
            _registry(), ["BTC"], duration_sec=1,
            venue_runners={exchange: fake_runner for exchange in ("binance", "bybit", "okx", "kucoin", "gate")},
        )
    )

    assert result["observation_count"] == 5
    assert result["pair_coverage_pct"] == 100.0
    assert result["missing_pairs"] == []
    assert result["venue_coverage"]["gate"]["coverage_pct"] == 100.0
    assert result["symbols_with_two_venues"] == 1
    assert result["invalid_bbo"] == []
    assert result["scheduler_enabled"] is False
    assert result["trade_signal"] is False
    assert {row["exchange_symbol"] for row in result["observations"]} == {"BTCUSDT", "BTC-USDT-SWAP", "XBTUSDTM", "BTC_USDT"}


def test_bounded_probe_cancels_a_runner_that_ignores_market_deadline() -> None:
    async def slow_runner(symbols, deadline, emit, stats):
        del symbols, deadline, emit
        stats["connections"] += 1
        await asyncio.sleep(10)

    result = asyncio.run(
        run_bounded_public_feed(
            _registry(), ["BTC"], duration_sec=1,
            venue_runners={exchange: slow_runner for exchange in ("binance", "bybit", "okx", "kucoin", "gate")},
        )
    )

    assert result["duration_sec"] < 5.0
    assert {row["status"] for row in result["venue_status"].values()} == {"deadline_cancelled"}
