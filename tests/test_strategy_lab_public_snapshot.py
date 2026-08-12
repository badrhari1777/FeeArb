from strategy_lab.public_snapshot import (
    parse_binance_oi_snapshot,
    parse_binance_ticker_snapshot,
    parse_gate_ticker_snapshot,
    parse_kucoin_contract_snapshot,
    parse_kucoin_ticker_snapshot,
    parse_okx_oi_snapshot,
    parse_okx_ticker_snapshot,
)


def test_binance_snapshots_supply_volume_last_and_open_interest():
    ticker = parse_binance_ticker_snapshot({
        "symbol": "PROMUSDT", "closeTime": 1700000000000,
        "lastPrice": "2.5", "volume": "100", "quoteVolume": "250",
    })[0]
    oi = parse_binance_oi_snapshot({
        "symbol": "PROMUSDT", "time": 1700000001000, "openInterest": "42",
    })[0]
    assert ticker["last_price"] == 2.5
    assert ticker["volume_24h_base"] == 100.0
    assert ticker["volume_24h_quote"] == 250.0
    assert oi["open_interest"] == 42.0


def test_okx_snapshots_supply_bbo_derived_quote_volume_and_usd_oi():
    ticker = parse_okx_ticker_snapshot({
        "code": "0",
        "data": [{
            "instId": "PROM-USDT-SWAP", "ts": "1700000000000", "last": "2.5",
            "bidPx": "2.4", "bidSz": "10", "askPx": "2.6", "askSz": "12",
            "volCcy24h": "100",
        }],
    })[0]
    oi = parse_okx_oi_snapshot({
        "code": "0",
        "data": [{
            "instId": "PROM-USDT-SWAP", "ts": "1700000001000",
            "oi": "42", "oiCcy": "420", "oiUsd": "1050",
        }],
    })[0]
    assert ticker["best_bid"] == 2.4
    assert ticker["best_ask"] == 2.6
    assert ticker["volume_24h_quote"] == 250.0
    assert ticker["volume_24h_quote_derived"] is True
    assert oi["open_interest"] == 42.0
    assert oi["open_interest_notional"] == 1050.0


def test_kucoin_contract_snapshot_supplies_research_fields_and_notional():
    row = parse_kucoin_contract_snapshot({
        "code": "200000",
        "data": {
            "symbol": "PROMUSDTM", "markPrice": 2.5, "indexPrice": 2.51,
            "lastTradePrice": 2.49, "fundingFeeRate": -0.001,
            "predictedFundingFeeRate": -0.0005,
            "nextFundingRateDateTime": 1700000000000,
            "openInterest": "100", "multiplier": 0.1,
            "volumeOf24h": 200, "turnoverOf24h": 500,
        },
    })[0]
    assert row["funding_rate"] == -0.001
    assert row["open_interest"] == 100.0
    assert row["open_interest_notional"] == 25.0
    assert row["volume_24h_quote"] == 500.0
    assert row["next_funding_time_ms"] == 1700000000000


def test_kucoin_ticker_snapshot_supplies_bbo_for_quiet_contracts():
    row = parse_kucoin_ticker_snapshot({
        "code": "200000",
        "data": {
            "symbol": "PROMUSDTM", "ts": 1700000000000000000,
            "price": "2.5", "bestBidPrice": "2.4", "bestBidSize": 10,
            "bestAskPrice": "2.6", "bestAskSize": 12,
        },
    })[0]
    assert row["source_ts_ms"] == 1700000000000
    assert row["best_bid"] == 2.4
    assert row["best_ask"] == 2.6


def test_gate_ticker_snapshot_supplies_bbo_and_full_research_fields():
    row = parse_gate_ticker_snapshot([{
        "contract": "PROM_USDT", "highest_bid": "2.4", "highest_size": "10",
        "lowest_ask": "2.6", "lowest_size": "12", "last": "2.5",
        "mark_price": "2.51", "index_price": "2.52", "funding_rate": "-0.001",
        "funding_rate_indicative": "-0.0005", "total_size": "100",
        "volume_24h_base": "200", "volume_24h_quote": "500",
    }])[0]
    assert row["best_bid"] == 2.4
    assert row["best_ask"] == 2.6
    assert row["funding_rate"] == -0.001
    assert row["open_interest"] == 100.0
    assert row["volume_24h_quote"] == 500.0


def test_snapshot_parsers_fail_closed_on_invalid_shapes():
    assert parse_binance_ticker_snapshot([]) == []
    assert parse_binance_oi_snapshot({}) == []
    assert parse_okx_ticker_snapshot({"code": "1"}) == []
    assert parse_okx_oi_snapshot({"code": "1"}) == []
    assert parse_kucoin_contract_snapshot({"code": "400000"}) == []
    assert parse_kucoin_ticker_snapshot({"code": "400000"}) == []
    assert parse_gate_ticker_snapshot({}) == []
