from __future__ import annotations

import asyncio

from strategy_lab.external_contract import ExternalLeg, ExternalObservation
from strategy_lab.instrument_registry import (
    build_registry_payload,
    fetch_instrument_registry,
    parse_binance_instruments,
    parse_bybit_instruments,
    parse_gate_contract,
    parse_kucoin_instruments,
    parse_okx_instruments,
    verify_external_candidates,
)


def _observation(symbol: str = "BTC") -> ExternalObservation:
    return ExternalObservation(
        source="arbitragescanner",
        source_asset_id=f"as:{symbol}",
        canonical_symbol=symbol,
        observed_at="2026-08-12T00:00:00+00:00",
        legs=[
            ExternalLeg("binance", f"{symbol}USDT"),
            ExternalLeg("bybit", f"{symbol}USDT"),
            ExternalLeg("okx", f"{symbol}-USDT-SWAP"),
            ExternalLeg("kucoin", f"{'XBT' if symbol == 'BTC' else symbol}USDTM"),
            ExternalLeg("gate", f"{symbol}_USDT"),
        ],
        long_exchange="bybit",
        short_exchange="binance",
        funding_dispersion=0.001,
    )


def _fixture_payloads() -> dict[str, object]:
    return {
        "binance": {
            "symbols": [{
                "symbol": "BTCUSDT", "pair": "BTCUSDT", "contractType": "PERPETUAL",
                "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT",
                "marginAsset": "USDT", "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }],
        },
        "bybit": {"retCode": 0, "result": {"list": [{
            "symbol": "BTCUSDT", "contractType": "LinearPerpetual", "status": "Trading",
            "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT",
            "isPreListing": False, "fundingInterval": 480,
            "priceFilter": {"tickSize": "0.1"},
            "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001", "minNotionalValue": "5"},
        }]}},
        "okx": {"code": "0", "data": [{
            "instId": "BTC-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT",
            "state": "live", "ctType": "linear", "ctVal": "0.01", "ctValCcy": "BTC",
            "tickSz": "0.1", "lotSz": "0.01", "minSz": "0.01",
        }]},
        "kucoin": {"code": "200000", "data": [{
            "symbol": "XBTUSDTM", "baseCurrency": "XBT", "quoteCurrency": "USDT",
            "settleCurrency": "USDT", "expireDate": None, "status": "Open",
            "isInverse": False, "multiplier": 0.001, "tickSize": 0.1, "lotSize": 1,
            "currentFundingRateGranularity": 28800000,
        }]},
        "gate": {
            "name": "BTC_USDT", "status": "trading", "in_delisting": False,
            "quanto_multiplier": "0.0001", "order_price_round": "0.1",
            "order_size_min": 1, "funding_interval": 28800,
        },
    }


def test_all_five_parsers_preserve_exact_symbols_and_contract_metadata() -> None:
    payloads = _fixture_payloads()
    rows = [
        parse_binance_instruments(payloads["binance"])[0],
        parse_bybit_instruments(payloads["bybit"])[0],
        parse_okx_instruments(payloads["okx"])[0],
        parse_kucoin_instruments(payloads["kucoin"])[0],
        parse_gate_contract(payloads["gate"]),
    ]

    assert [row.exchange for row in rows] == ["binance", "bybit", "okx", "kucoin", "gate"]
    assert {row.canonical_symbol for row in rows} == {"BTC"}
    assert rows[2].exchange_symbol == "BTC-USDT-SWAP"
    assert rows[3].exchange_symbol == "XBTUSDTM"
    assert rows[3].canonical_symbol == "BTC"
    assert rows[4].funding_interval_hours == 8.0


def test_registry_quarantines_ambiguous_same_venue_contracts() -> None:
    payloads = _fixture_payloads()
    btc = parse_binance_instruments(payloads["binance"])[0]
    duplicate = parse_binance_instruments(payloads["binance"])[0]
    duplicate.exchange_symbol = "BTCUSDT-SECOND"
    registry = build_registry_payload({"binance": [btc, duplicate]})

    assert registry["contract_count"] == 0
    assert registry["ambiguous"][0]["reason"] == "multiple_active_contracts"


def test_bounded_fetch_builds_five_venue_vector_and_verifies_external_symbols() -> None:
    payloads = _fixture_payloads()

    async def bulk(exchange: str) -> object:
        return payloads[exchange]

    async def gate(symbol: str) -> object:
        assert symbol == "BTC_USDT"
        return payloads["gate"]

    registry = asyncio.run(
        fetch_instrument_registry(
            [_observation()],
            bulk_fetchers={exchange: (lambda exchange=exchange: bulk(exchange)) for exchange in ("binance", "bybit", "okx", "kucoin")},
            gate_fetcher=gate,
        )
    )
    verification = verify_external_candidates(registry, [_observation()])[0]

    assert registry["venue_counts"] == {"binance": 1, "bybit": 1, "okx": 1, "kucoin": 1, "gate": 1}
    assert registry["common_symbol_counts"]["5"] == 1
    assert verification["verified_venue_count"] == 5
    assert verification["eligible_for_observation"] is True
    assert verification["trade_signal"] is False


def test_external_exact_symbol_mismatch_is_fail_closed() -> None:
    payloads = _fixture_payloads()
    registry = build_registry_payload(
        {
            "binance": parse_binance_instruments(payloads["binance"]),
            "bybit": parse_bybit_instruments(payloads["bybit"]),
        }
    )
    observation = _observation()
    observation.legs[1].exchange_symbol = "ETHUSDT"
    verification = verify_external_candidates(registry, [observation])[0]

    assert verification["eligible_for_observation"] is False
    assert verification["vetoes"] == ["external_symbol_asset_mismatch"]
    assert verification["external_symbol_asset_mismatch_exchanges"] == ["bybit"]


def test_provider_generic_symbol_format_uses_registry_exact_symbol_without_veto() -> None:
    payloads = _fixture_payloads()
    registry = build_registry_payload(
        {
            "okx": parse_okx_instruments(payloads["okx"]),
            "kucoin": parse_kucoin_instruments(payloads["kucoin"]),
        }
    )
    observation = _observation()
    observation.legs[2].exchange_symbol = "BTCUSDT"
    observation.legs[3].exchange_symbol = "BTCUSDT"
    verification = verify_external_candidates(registry, [observation])[0]

    assert verification["eligible_for_observation"] is True
    assert verification["vetoes"] == []
    assert verification["provider_symbol_format_difference_exchanges"] == ["okx", "kucoin"]
