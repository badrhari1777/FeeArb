"""Bounded multiplexed public-feed probe for Strategy Lab Phase O0."""

from __future__ import annotations

import asyncio
import json
import statistics
import time
from typing import Any, Awaitable, Callable, Iterable
import uuid

import aiohttp
import websockets

from .external_contract import TARGET_EXCHANGES, utc_now_iso


OWN_OBSERVATION_VERSION = "strategy_lab_own_observation_v1"
PROBE_VERSION = "strategy_lab_bounded_public_feed_v1"
MAX_PROBE_SYMBOLS = 10
MAX_PROBE_DURATION_SEC = 30.0

BINANCE_WS_URL = "wss://fstream.binance.com/ws"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
GATE_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
KUCOIN_TOKEN_URL = "https://api-futures.kucoin.com/api/v1/bullet-public"
BINANCE_PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"


def _float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ts_ms(value: object) -> int | None:
    number = _float(value)
    if number is None or number <= 0:
        return None
    if number > 1e17:  # nanoseconds
        number /= 1_000_000.0
    elif number > 1e14:  # microseconds
        number /= 1_000.0
    elif number < 1e11:  # seconds
        number *= 1_000.0
    return int(number)


def _patch(symbol: object, channel: str, source_ts_ms: object, **fields: object) -> dict[str, Any]:
    clean = {key: value for key, value in fields.items() if value is not None and value != ""}
    return {
        "exchange_symbol": str(symbol or "").upper(),
        "source_channel": channel,
        "source_ts_ms": _ts_ms(source_ts_ms),
        **clean,
    }


def parse_binance_message(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    event = payload.get("e")
    symbol = payload.get("s")
    if event == "bookTicker":
        return [_patch(
            symbol, "bookTicker", payload.get("E") or payload.get("T"),
            best_bid=_float(payload.get("b")), best_bid_size=_float(payload.get("B")),
            best_ask=_float(payload.get("a")), best_ask_size=_float(payload.get("A")),
        )]
    if event == "markPriceUpdate":
        return [_patch(
            symbol, "markPrice", payload.get("E"), mark_price=_float(payload.get("p")),
            index_price=_float(payload.get("i")), funding_rate=_float(payload.get("r")),
            next_funding_time_ms=_ts_ms(payload.get("T")),
        )]
    return []


def parse_binance_premium_index(payload: object) -> list[dict[str, Any]]:
    """Normalize the public bulk REST snapshot used when markPrice WS is silent."""

    rows = payload if isinstance(payload, list) else [payload]
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("symbol"):
            continue
        result.append(_patch(
            row.get("symbol"), "premiumIndex.rest", row.get("time"),
            mark_price=_float(row.get("markPrice")), index_price=_float(row.get("indexPrice")),
            funding_rate=_float(row.get("lastFundingRate")),
            next_funding_time_ms=_ts_ms(row.get("nextFundingTime")),
        ))
    return result


def parse_bybit_message(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not str(payload.get("topic") or "").startswith("tickers."):
        return []
    data = payload.get("data")
    rows = data if isinstance(data, list) else [data]
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        result.append(_patch(
            row.get("symbol"), "tickers", payload.get("ts"),
            best_bid=_float(row.get("bid1Price")), best_bid_size=_float(row.get("bid1Size")),
            best_ask=_float(row.get("ask1Price")), best_ask_size=_float(row.get("ask1Size")),
            last_price=_float(row.get("lastPrice")), mark_price=_float(row.get("markPrice")),
            index_price=_float(row.get("indexPrice")), funding_rate=_float(row.get("fundingRate")),
            next_funding_time_ms=_ts_ms(row.get("nextFundingTime")),
            open_interest=_float(row.get("openInterest")),
            open_interest_notional=_float(row.get("openInterestValue")),
            volume_24h_base=_float(row.get("volume24h")),
            volume_24h_quote=_float(row.get("turnover24h")),
        ))
    return result


def parse_okx_message(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("arg"), dict):
        return []
    channel = str(payload["arg"].get("channel") or "")
    rows = payload.get("data") or []
    result: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        symbol = row.get("instId") or payload["arg"].get("instId")
        if channel == "tickers":
            result.append(_patch(
                symbol, channel, row.get("ts"), best_bid=_float(row.get("bidPx")),
                best_bid_size=_float(row.get("bidSz")), best_ask=_float(row.get("askPx")),
                best_ask_size=_float(row.get("askSz")), last_price=_float(row.get("last")),
                volume_24h_base=_float(row.get("volCcy24h")),
            ))
        elif channel == "mark-price":
            result.append(_patch(symbol, channel, row.get("ts"), mark_price=_float(row.get("markPx"))))
        elif channel == "funding-rate":
            result.append(_patch(
                symbol, channel, row.get("ts"), funding_rate=_float(row.get("fundingRate")),
                predicted_funding_rate=_float(row.get("nextFundingRate")),
                next_funding_time_ms=_ts_ms(row.get("fundingTime")),
                premium_index=_float(row.get("premium")),
            ))
    return result


def parse_gate_message(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or payload.get("event") not in ("update", "all"):
        return []
    channel = str(payload.get("channel") or "")
    raw = payload.get("result")
    rows = raw if isinstance(raw, list) else [raw]
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if channel == "futures.book_ticker":
            result.append(_patch(
                row.get("s"), channel, row.get("t") or payload.get("time_ms"),
                best_bid=_float(row.get("b")), best_bid_size=_float(row.get("B")),
                best_ask=_float(row.get("a")), best_ask_size=_float(row.get("A")),
            ))
        elif channel == "futures.tickers":
            result.append(_patch(
                row.get("contract"), channel, row.get("t") or payload.get("time_ms"),
                last_price=_float(row.get("last")), mark_price=_float(row.get("mark_price")),
                index_price=_float(row.get("index_price")), funding_rate=_float(row.get("funding_rate")),
                predicted_funding_rate=_float(row.get("funding_rate_indicative")),
                next_funding_time_ms=_ts_ms(row.get("funding_next_apply")),
                open_interest=_float(row.get("total_size")),
                volume_24h_base=_float(row.get("volume_24h_base")),
                volume_24h_quote=_float(row.get("volume_24h_quote")),
            ))
    return result


def parse_kucoin_message(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or payload.get("type") != "message":
        return []
    row = payload.get("data")
    if not isinstance(row, dict):
        return []
    topic = str(payload.get("topic") or "")
    symbol = row.get("symbol") or topic.rsplit(":", 1)[-1]
    if payload.get("subject") == "tickerV2":
        return [_patch(
            symbol, "tickerV2", row.get("ts"), best_bid=_float(row.get("bestBidPrice")),
            best_bid_size=_float(row.get("bestBidSize")), best_ask=_float(row.get("bestAskPrice")),
            best_ask_size=_float(row.get("bestAskSize")),
        )]
    if payload.get("subject") == "mark.index.price":
        return [_patch(
            symbol, "instrument.mark_index", row.get("timestamp"),
            mark_price=_float(row.get("markPrice")), index_price=_float(row.get("indexPrice")),
            open_interest=_float(row.get("openInterest")),
        )]
    if payload.get("subject") == "funding.rate":
        return [_patch(
            symbol, "instrument.funding", row.get("timestamp"),
            funding_rate=_float(row.get("fundingRate")),
        )]
    return []


def build_probe_plan(
    registry: dict[str, Any],
    symbols: Iterable[str],
    *,
    max_symbols: int = MAX_PROBE_SYMBOLS,
) -> dict[str, Any]:
    vectors = registry.get("vectors") or {}
    selected: list[str] = []
    for raw in symbols:
        symbol = str(raw or "").upper()
        if symbol and symbol in vectors and symbol not in selected:
            selected.append(symbol)
        if len(selected) >= min(MAX_PROBE_SYMBOLS, max(1, int(max_symbols))):
            break
    venues: dict[str, list[dict[str, str]]] = {exchange: [] for exchange in TARGET_EXCHANGES}
    for symbol in selected:
        for exchange, contract in (vectors.get(symbol) or {}).items():
            if exchange not in venues or not contract.get("active"):
                continue
            venues[exchange].append(
                {"canonical_symbol": symbol, "exchange_symbol": str(contract["exchange_symbol"]).upper()}
            )
    expected_pairs = sum(len(rows) for rows in venues.values())
    return {
        "symbols": selected,
        "venues": venues,
        "symbol_count": len(selected),
        "expected_pairs": expected_pairs,
        "expected_connections": sum(bool(rows) for rows in venues.values()),
    }


def _merge_patch(
    observations: dict[tuple[str, str], dict[str, Any]],
    exchange: str,
    canonical_symbol: str,
    patch: dict[str, Any],
) -> None:
    received_at_ms = int(time.time() * 1000)
    key = (exchange, canonical_symbol)
    row = observations.setdefault(
        key,
        {
            "observation_version": OWN_OBSERVATION_VERSION,
            "exchange": exchange,
            "canonical_symbol": canonical_symbol,
            "exchange_symbol": patch["exchange_symbol"],
            "first_received_at_ms": received_at_ms,
            "source_channels": [],
            "message_count": 0,
            "trade_signal": False,
            "research_only": True,
        },
    )
    channel = str(patch.get("source_channel") or "")
    if channel and channel not in row["source_channels"]:
        row["source_channels"].append(channel)
    row["received_at_ms"] = received_at_ms
    row["message_count"] = int(row.get("message_count") or 0) + 1
    source_ts = patch.get("source_ts_ms")
    if source_ts is not None:
        row["source_ts_ms"] = max(int(row.get("source_ts_ms") or 0), int(source_ts))
    for key_name, value in patch.items():
        if key_name not in {"source_channel", "source_ts_ms"} and value is not None:
            row[key_name] = value


def build_feed_report(
    plan: dict[str, Any],
    observations: dict[tuple[str, str], dict[str, Any]],
    venue_status: dict[str, dict[str, Any]],
    *,
    started_at_ms: int,
    completed_at_ms: int | None = None,
) -> dict[str, Any]:
    end_ms = int(completed_at_ms or time.time() * 1000)
    rows = sorted(observations.values(), key=lambda row: (row["canonical_symbol"], row["exchange"]))
    ages = [max(0, end_ms - int(row.get("received_at_ms") or end_ms)) for row in rows]
    field_names = (
        "best_bid", "best_ask", "last_price", "mark_price", "index_price", "funding_rate",
        "predicted_funding_rate", "next_funding_time_ms", "open_interest", "volume_24h_quote",
    )
    field_availability = {
        field: {exchange: sum(1 for row in rows if row["exchange"] == exchange and row.get(field) is not None) for exchange in TARGET_EXCHANGES}
        for field in field_names
    }
    invalid_bbo = [
        f"{row['exchange']}:{row['canonical_symbol']}"
        for row in rows
        if row.get("best_bid") is not None and row.get("best_ask") is not None
        and float(row["best_bid"]) > float(row["best_ask"])
    ]
    by_symbol: dict[str, int] = {}
    for row in rows:
        by_symbol[row["canonical_symbol"]] = by_symbol.get(row["canonical_symbol"], 0) + 1
    expected_rows = [
        (exchange, item["canonical_symbol"])
        for exchange, venue_rows in (plan.get("venues") or {}).items()
        for item in venue_rows
    ]
    observed_keys = {(row["exchange"], row["canonical_symbol"]) for row in rows}
    missing_pairs = [f"{exchange}:{symbol}" for exchange, symbol in expected_rows if (exchange, symbol) not in observed_keys]
    expected = int(plan.get("expected_pairs") or 0)
    venue_coverage: dict[str, dict[str, Any]] = {}
    for exchange in TARGET_EXCHANGES:
        venue_expected = sum(1 for venue, _symbol in expected_rows if venue == exchange)
        venue_observed = sum(1 for venue, _symbol in observed_keys if venue == exchange)
        venue_coverage[exchange] = {
            "expected": venue_expected,
            "observed": venue_observed,
            "coverage_pct": round(venue_observed / venue_expected * 100.0, 3) if venue_expected else None,
        }
    return {
        "probe_version": PROBE_VERSION,
        "observation_version": OWN_OBSERVATION_VERSION,
        "mode": "bounded_research_only_no_trading",
        "scheduler_enabled": False,
        "started_at_ms": started_at_ms,
        "completed_at_ms": end_ms,
        "duration_sec": round((end_ms - started_at_ms) / 1000.0, 3),
        "plan": plan,
        "venue_status": venue_status,
        "observations": rows,
        "observation_count": len(rows),
        "pair_coverage_pct": round(len(rows) / expected * 100.0, 3) if expected else 0.0,
        "venue_coverage": venue_coverage,
        "missing_pairs": missing_pairs,
        "symbols_with_two_venues": sum(1 for count in by_symbol.values() if count >= 2),
        "freshness_ms": {
            "max": max(ages) if ages else None,
            "median": statistics.median(ages) if ages else None,
        },
        "field_availability": field_availability,
        "invalid_bbo": invalid_bbo,
        "trade_signal": False,
        "research_only": True,
    }


Emit = Callable[[str, dict[str, Any]], None]
VenueRunner = Callable[[list[str], float, Emit, dict[str, Any]], Awaitable[None]]


async def _receive_loop(ws: Any, deadline: float, parser: Callable[[object], list[dict[str, Any]]], emit: Emit, stats: dict[str, Any]) -> None:
    while time.monotonic() < deadline:
        try:
            message = await asyncio.wait_for(ws.recv(), timeout=max(0.05, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            break
        stats["messages"] += 1
        try:
            payload = json.loads(message)
        except (TypeError, json.JSONDecodeError):
            stats["parse_errors"] += 1
            continue
        if not isinstance(payload, dict):
            stats["parse_errors"] += 1
            continue
        is_subscription_error = (
            payload.get("success") is False
            or payload.get("event") == "error"
            or payload.get("type") == "error"
            or bool(payload.get("error"))
            or (payload.get("code") is not None and payload.get("msg") is not None)
        )
        if is_subscription_error:
            stats["subscription_errors"] += 1
            if not stats.get("last_subscription_error"):
                stats["last_subscription_error"] = str(payload)[:500]
        for patch in parser(payload):
            emit(patch["exchange_symbol"], patch)
            stats["updates"] += 1


async def _binance_runner(symbols: list[str], deadline: float, emit: Emit, stats: dict[str, Any]) -> None:
    params = [f"{symbol.lower()}@bookTicker" for symbol in symbols]
    params += [f"{symbol.lower()}@markPrice@1s" for symbol in symbols]
    async with websockets.connect(BINANCE_WS_URL, open_timeout=15, close_timeout=1, ping_interval=20, ping_timeout=20) as ws:
        stats["connections"] += 1
        await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))
        # Binance's mark-price subscription has occasionally acknowledged but
        # emitted no updates in bounded probes. Seed the same public fields with
        # one bulk REST request; BBO remains websocket-native.
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    BINANCE_PREMIUM_INDEX_URL,
                    timeout=aiohttp.ClientTimeout(total=min(10.0, max(1.0, deadline - time.monotonic()))),
                ) as response:
                    response.raise_for_status()
                    snapshot = await response.json()
            stats["rest_requests"] = int(stats.get("rest_requests") or 0) + 1
            selected = set(symbols)
            for patch in parse_binance_premium_index(snapshot):
                if patch["exchange_symbol"] in selected:
                    emit(patch["exchange_symbol"], patch)
                    stats["updates"] += 1
        except Exception as exc:  # pylint: disable=broad-except
            stats["rest_errors"] = int(stats.get("rest_errors") or 0) + 1
            stats["last_rest_error"] = f"{type(exc).__name__}: {exc}"
        await _receive_loop(ws, deadline, parse_binance_message, emit, stats)


async def _bybit_runner(symbols: list[str], deadline: float, emit: Emit, stats: dict[str, Any]) -> None:
    async with websockets.connect(BYBIT_WS_URL, open_timeout=15, close_timeout=1, ping_interval=20, ping_timeout=20) as ws:
        stats["connections"] += 1
        await ws.send(json.dumps({"op": "subscribe", "args": [f"tickers.{symbol}" for symbol in symbols]}))
        await _receive_loop(ws, deadline, parse_bybit_message, emit, stats)


async def _okx_runner(symbols: list[str], deadline: float, emit: Emit, stats: dict[str, Any]) -> None:
    args = [
        {"channel": channel, "instId": symbol}
        for symbol in symbols
        for channel in ("tickers", "mark-price", "funding-rate")
    ]
    async with websockets.connect(OKX_WS_URL, open_timeout=15, close_timeout=1, ping_interval=20, ping_timeout=20) as ws:
        stats["connections"] += 1
        await ws.send(json.dumps({"op": "subscribe", "args": args}))
        await _receive_loop(ws, deadline, parse_okx_message, emit, stats)


async def _gate_runner(symbols: list[str], deadline: float, emit: Emit, stats: dict[str, Any]) -> None:
    async with websockets.connect(GATE_WS_URL, open_timeout=15, close_timeout=1, ping_interval=20, ping_timeout=20) as ws:
        stats["connections"] += 1
        for channel in ("futures.tickers", "futures.book_ticker"):
            await ws.send(json.dumps({
                "time": int(time.time()), "channel": channel, "event": "subscribe", "payload": symbols,
            }))
        await _receive_loop(ws, deadline, parse_gate_message, emit, stats)


async def _kucoin_runner(symbols: list[str], deadline: float, emit: Emit, stats: dict[str, Any]) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.post(KUCOIN_TOKEN_URL, timeout=aiohttp.ClientTimeout(total=15)) as response:
            payload = await response.json()
    data = payload.get("data") or {}
    servers = data.get("instanceServers") or []
    if not servers or not data.get("token"):
        raise RuntimeError("KuCoin public websocket token unavailable")
    endpoint = str(servers[0].get("endpoint") or "")
    if not endpoint:
        raise RuntimeError("KuCoin public websocket endpoint unavailable")
    ws_url = f"{endpoint}?token={data['token']}&connectId={uuid.uuid4().hex}"
    async with websockets.connect(ws_url, open_timeout=15, close_timeout=1, ping_interval=None, ping_timeout=20) as ws:
        stats["connections"] += 1
        await ws.send(json.dumps({
            "id": "ticker", "type": "subscribe",
            "topic": f"/contractMarket/tickerV2:{','.join(symbols)}",
            "privateChannel": False, "response": True,
        }))
        for index, symbol in enumerate(symbols):
            await ws.send(json.dumps({
                "id": f"instrument-{index}", "type": "subscribe",
                "topic": f"/contract/instrument:{symbol}",
                "privateChannel": False, "response": True,
            }))
        await _receive_loop(ws, deadline, parse_kucoin_message, emit, stats)


async def run_bounded_public_feed(
    registry: dict[str, Any],
    symbols: Iterable[str],
    *,
    duration_sec: float = 12.0,
    max_symbols: int = MAX_PROBE_SYMBOLS,
    venue_runners: dict[str, VenueRunner] | None = None,
) -> dict[str, Any]:
    duration = min(MAX_PROBE_DURATION_SEC, max(1.0, float(duration_sec)))
    plan = build_probe_plan(registry, symbols, max_symbols=max_symbols)
    if not plan["symbols"] or plan["expected_pairs"] < 2:
        raise ValueError("Bounded feed probe requires at least one registry symbol and two venue pairs")
    runners: dict[str, VenueRunner] = {
        "binance": _binance_runner,
        "bybit": _bybit_runner,
        "okx": _okx_runner,
        "kucoin": _kucoin_runner,
        "gate": _gate_runner,
    }
    runners.update(venue_runners or {})
    observations: dict[tuple[str, str], dict[str, Any]] = {}
    venue_status: dict[str, dict[str, Any]] = {}
    started_at_ms = int(time.time() * 1000)
    deadline = time.monotonic() + duration

    async def run_venue(exchange: str, rows: list[dict[str, str]]) -> None:
        if not rows:
            venue_status[exchange] = {"status": "not_requested", "connections": 0, "messages": 0, "updates": 0, "parse_errors": 0, "subscription_errors": 0, "error": None}
            return
        stats: dict[str, Any] = {"status": "running", "connections": 0, "messages": 0, "updates": 0, "parse_errors": 0, "subscription_errors": 0, "error": None}
        venue_status[exchange] = stats
        symbol_lookup = {row["exchange_symbol"]: row["canonical_symbol"] for row in rows}

        def emit(exchange_symbol: str, patch: dict[str, Any]) -> None:
            canonical = symbol_lookup.get(str(exchange_symbol or "").upper())
            if canonical:
                _merge_patch(observations, exchange, canonical, patch)
            else:
                stats["unknown_symbol_messages"] = int(stats.get("unknown_symbol_messages") or 0) + 1

        try:
            await runners[exchange](list(symbol_lookup), deadline, emit, stats)
        except Exception as exc:  # pylint: disable=broad-except
            stats["status"] = "error"
            stats["error"] = f"{type(exc).__name__}: {exc}"
        else:
            stats["status"] = "completed"

    venue_tasks = [
        asyncio.create_task(run_venue(exchange, plan["venues"][exchange]))
        for exchange in TARGET_EXCHANGES
    ]
    wall_limit_sec = duration + 3.0
    try:
        await asyncio.wait_for(asyncio.gather(*venue_tasks), timeout=wall_limit_sec)
    except asyncio.TimeoutError:
        for task in venue_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*venue_tasks, return_exceptions=True)
        for stats in venue_status.values():
            if stats.get("status") == "running":
                if int(stats.get("updates") or 0) > 0:
                    stats["status"] = "completed_at_deadline"
                    stats["error"] = None
                else:
                    stats["status"] = "deadline_cancelled"
                    stats["error"] = "probe_wall_clock_deadline"
    return build_feed_report(
        plan,
        observations,
        venue_status,
        started_at_ms=started_at_ms,
        completed_at_ms=int(time.time() * 1000),
    )
