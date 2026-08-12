"""Candidate-scoped public REST snapshots for Strategy Lab feed enrichment."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable
from urllib.parse import quote

import aiohttp


BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
OKX_TICKER_URL = "https://www.okx.com/api/v5/market/ticker?instId={symbol}"
OKX_OI_URL = "https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId={symbol}"
KUCOIN_CONTRACT_URL = "https://api-futures.kucoin.com/api/v1/contracts/{symbol}"
KUCOIN_TICKER_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"
GATE_TICKER_URL = "https://fx-api.gateio.ws/api/v4/futures/usdt/tickers?contract={symbol}"

Emit = Callable[[str, dict[str, Any]], None]
Parser = Callable[[object], list[dict[str, Any]]]


def _float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ts_ms(value: object) -> int | None:
    number = _float(value)
    if number is None or number <= 0:
        return None
    if number > 1e17:
        number /= 1_000_000.0
    elif number > 1e14:
        number /= 1_000.0
    elif number < 1e11:
        number *= 1_000.0
    return int(number)


def _patch(symbol: object, channel: str, source_ts_ms: object, **fields: object) -> dict[str, Any]:
    return {
        "exchange_symbol": str(symbol or "").upper(),
        "source_channel": channel,
        "source_ts_ms": _ts_ms(source_ts_ms),
        **{key: value for key, value in fields.items() if value is not None and value != ""},
    }


def parse_binance_ticker_snapshot(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not payload.get("symbol"):
        return []
    return [_patch(
        payload.get("symbol"), "ticker24hr.rest", payload.get("closeTime"),
        last_price=_float(payload.get("lastPrice")),
        volume_24h_base=_float(payload.get("volume")),
        volume_24h_quote=_float(payload.get("quoteVolume")),
    )]


def parse_binance_oi_snapshot(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not payload.get("symbol"):
        return []
    return [_patch(
        payload.get("symbol"), "openInterest.rest", payload.get("time"),
        open_interest=_float(payload.get("openInterest")),
    )]


def parse_okx_ticker_snapshot(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or str(payload.get("code")) != "0":
        return []
    result: list[dict[str, Any]] = []
    for row in payload.get("data") or []:
        if not isinstance(row, dict) or not row.get("instId"):
            continue
        last = _float(row.get("last"))
        volume_base = _float(row.get("volCcy24h"))
        result.append(_patch(
            row.get("instId"), "ticker.rest", row.get("ts"),
            best_bid=_float(row.get("bidPx")), best_bid_size=_float(row.get("bidSz")),
            best_ask=_float(row.get("askPx")), best_ask_size=_float(row.get("askSz")),
            last_price=last, volume_24h_base=volume_base,
            volume_24h_quote=(last * volume_base if last is not None and volume_base is not None else None),
            volume_24h_quote_derived=True,
        ))
    return result


def parse_okx_oi_snapshot(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or str(payload.get("code")) != "0":
        return []
    result: list[dict[str, Any]] = []
    for row in payload.get("data") or []:
        if not isinstance(row, dict) or not row.get("instId"):
            continue
        result.append(_patch(
            row.get("instId"), "open-interest.rest", row.get("ts"),
            open_interest=_float(row.get("oi")),
            open_interest_base=_float(row.get("oiCcy")),
            open_interest_notional=_float(row.get("oiUsd")),
        ))
    return result


def parse_kucoin_contract_snapshot(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or str(payload.get("code")) != "200000":
        return []
    row = payload.get("data")
    if not isinstance(row, dict) or not row.get("symbol"):
        return []
    open_interest = _float(row.get("openInterest"))
    multiplier = _float(row.get("multiplier"))
    mark = _float(row.get("markPrice"))
    return [_patch(
        row.get("symbol"), "contract.rest", row.get("time") or row.get("timestamp"),
        last_price=_float(row.get("lastTradePrice")), mark_price=mark,
        index_price=_float(row.get("indexPrice")),
        funding_rate=_float(row.get("fundingFeeRate")),
        predicted_funding_rate=_float(row.get("predictedFundingFeeRate")),
        next_funding_time_ms=_ts_ms(row.get("nextFundingRateDateTime")),
        open_interest=open_interest,
        open_interest_notional=(
            open_interest * multiplier * mark
            if open_interest is not None and multiplier is not None and mark is not None
            else None
        ),
        volume_24h_base=_float(row.get("volumeOf24h")),
        volume_24h_quote=_float(row.get("turnoverOf24h")),
    )]


def parse_kucoin_ticker_snapshot(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or str(payload.get("code")) != "200000":
        return []
    row = payload.get("data")
    if not isinstance(row, dict) or not row.get("symbol"):
        return []
    return [_patch(
        row.get("symbol"), "ticker.rest", row.get("ts"),
        best_bid=_float(row.get("bestBidPrice")), best_bid_size=_float(row.get("bestBidSize")),
        best_ask=_float(row.get("bestAskPrice")), best_ask_size=_float(row.get("bestAskSize")),
        last_price=_float(row.get("price")),
    )]


def parse_gate_ticker_snapshot(payload: object) -> list[dict[str, Any]]:
    rows = payload if isinstance(payload, list) else []
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("contract"):
            continue
        result.append(_patch(
            row.get("contract"), "tickers.rest", row.get("time") or row.get("time_ms"),
            best_bid=_float(row.get("highest_bid")), best_bid_size=_float(row.get("highest_size")),
            best_ask=_float(row.get("lowest_ask")), best_ask_size=_float(row.get("lowest_size")),
            last_price=_float(row.get("last")), mark_price=_float(row.get("mark_price")),
            index_price=_float(row.get("index_price")), funding_rate=_float(row.get("funding_rate")),
            predicted_funding_rate=_float(row.get("funding_rate_indicative")),
            open_interest=_float(row.get("total_size")),
            volume_24h_base=_float(row.get("volume_24h_base")),
            volume_24h_quote=_float(row.get("volume_24h_quote")),
        ))
    return result


def _requests(exchange: str, symbols: list[str]) -> list[tuple[str, Parser]]:
    result: list[tuple[str, Parser]] = []
    for symbol in symbols:
        encoded = quote(str(symbol or "").upper(), safe="_-.")
        if exchange == "binance":
            result.extend([
                (BINANCE_TICKER_URL.format(symbol=encoded), parse_binance_ticker_snapshot),
                (BINANCE_OI_URL.format(symbol=encoded), parse_binance_oi_snapshot),
            ])
        elif exchange == "okx":
            result.extend([
                (OKX_TICKER_URL.format(symbol=encoded), parse_okx_ticker_snapshot),
                (OKX_OI_URL.format(symbol=encoded), parse_okx_oi_snapshot),
            ])
        elif exchange == "kucoin":
            result.extend([
                (KUCOIN_CONTRACT_URL.format(symbol=encoded), parse_kucoin_contract_snapshot),
                (KUCOIN_TICKER_URL.format(symbol=encoded), parse_kucoin_ticker_snapshot),
            ])
        elif exchange == "gate":
            result.append((GATE_TICKER_URL.format(symbol=encoded), parse_gate_ticker_snapshot))
    return result


async def seed_public_snapshots(
    exchange: str,
    symbols: list[str],
    emit: Emit,
    stats: dict[str, Any],
    *,
    concurrency: int = 5,
) -> None:
    """Seed missing research fields; failures remain visible and never block WS BBO."""
    requests = _requests(exchange, symbols)
    if not requests:
        return
    semaphore = asyncio.Semaphore(max(1, min(10, int(concurrency))))
    started = time.monotonic()
    byte_count = 0

    async with aiohttp.ClientSession(headers={"User-Agent": "FeeArb-StrategyLab-Research/1.0"}) as session:
        async def fetch(url: str, parser: Parser) -> None:
            nonlocal byte_count
            stats["rest_requests"] = int(stats.get("rest_requests") or 0) + 1
            try:
                async with semaphore:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as response:
                        response.raise_for_status()
                        body = await response.read()
                byte_count += len(body)
                payload = json.loads(body)
                patches = parser(payload)
                if not patches:
                    raise ValueError("snapshot_parser_returned_no_rows")
                for patch in patches:
                    emit(patch["exchange_symbol"], patch)
                    stats["updates"] += 1
                    stats["rest_updates"] = int(stats.get("rest_updates") or 0) + 1
            except Exception as exc:  # pylint: disable=broad-except
                stats["rest_errors"] = int(stats.get("rest_errors") or 0) + 1
                if not stats.get("last_rest_error"):
                    stats["last_rest_error"] = f"{type(exc).__name__}: {exc}"

        await asyncio.gather(*(fetch(url, parser) for url, parser in requests))
    stats["rest_bytes"] = int(stats.get("rest_bytes") or 0) + byte_count
    stats["rest_latency_ms"] = round((time.monotonic() - started) * 1000.0, 3)
