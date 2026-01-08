import argparse
import asyncio
import json
import math
import time
from typing import Any, Dict, Iterable, List, Optional

import aiohttp
import websockets

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_TICKER_URL = "https://api.bybit.com/v5/market/tickers"
BYBIT_ORDERBOOK_URL = "https://api.bybit.com/v5/market/orderbook"
BYBIT_INSTRUMENT_URL = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"

MEXC_WS_URL = "wss://contract.mexc.com/ws"
MEXC_TICKER_URL = "https://contract.mexc.com/api/v1/contract/ticker"
MEXC_FUNDING_URL = "https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}"
MEXC_DEPTH_URL = "https://contract.mexc.com/api/v1/contract/depth/{symbol}"
MEXC_DETAIL_URL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_KLINE_URL = "https://contract.mexc.com/api/v1/contract/kline/{symbol}"


def _as_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _orderbook_summary(bids: List[List[Any]], asks: List[List[Any]], levels: int = 20) -> Dict[str, Any]:
    top_bids = bids[:levels]
    top_asks = asks[:levels]
    best_bid = _as_float(top_bids[0][0]) if top_bids else None
    best_ask = _as_float(top_asks[0][0]) if top_asks else None
    spread = None
    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid
    bid_depth = sum(_as_float(level[1]) or 0.0 for level in top_bids)
    ask_depth = sum(_as_float(level[1]) or 0.0 for level in top_asks)
    return {
        "levels": {
            "bids": [[_as_float(p), _as_float(q)] for p, q, *rest in top_bids],
            "asks": [[_as_float(p), _as_float(q)] for p, q, *rest in top_asks],
        },
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
    }


def _atr_pct_from_candles(candles: List[Dict[str, float]]) -> Optional[float]:
    if len(candles) < 2:
        return None
    candles = sorted(candles, key=lambda x: x["time"])
    trs = []
    prev_close = candles[0]["close"]
    for candle in candles[1:]:
        high = candle["high"]
        low = candle["low"]
        close = candle["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    if not trs:
        return None
    atr = sum(trs) / len(trs)
    last_close = candles[-1]["close"]
    if last_close == 0:
        return None
    return atr / last_close


def _sigma_from_closes(candles: List[Dict[str, float]]) -> Optional[float]:
    candles = sorted(candles, key=lambda x: x["time"])
    if len(candles) < 2:
        return None
    returns = []
    prev_close = candles[0]["close"]
    for candle in candles[1:]:
        close = candle["close"]
        if prev_close and close:
            returns.append(math.log(close / prev_close))
        prev_close = close
    if not returns:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    return math.sqrt(variance)


async def _fetch_json(session: aiohttp.ClientSession, url: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json()


def _chunked(seq: Iterable[Any], size: int) -> Iterable[List[Any]]:
    chunk: List[Any] = []
    for item in seq:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


async def _bybit_ws_snapshot(symbols: List[str], max_wait: float = 3.0) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}

    results: Dict[str, Dict[str, Any]] = {symbol: {} for symbol in symbols}
    pending = set(symbols)
    try:
        async with websockets.connect(
            BYBIT_WS_URL,
            ping_interval=20,
            ping_timeout=20,
            max_size=1_000_000,
        ) as ws:
            args = []
            for symbol in symbols:
                args.append({"channel": "tickers", "instId": symbol, "instType": "linear"})
            for symbol in symbols:
                args.append({"channel": "orderbook.50", "instId": symbol, "instType": "linear"})
            for batch in _chunked(args, 8):
                await ws.send(json.dumps({"op": "subscribe", "args": batch}))

            deadline = asyncio.get_event_loop().time() + max_wait
            while pending and asyncio.get_event_loop().time() < deadline:
                timeout = max(0.1, deadline - asyncio.get_event_loop().time())
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break
                data = json.loads(message)
                if data.get("op") == "subscribe":
                    continue
                topic = data.get("topic") or data.get("channel")
                payload = data.get("data")
                if not topic or payload is None:
                    continue
                if isinstance(payload, list) and payload:
                    payload = payload[0]
                if not isinstance(payload, dict):
                    continue
                if topic.startswith("tickers."):
                    symbol_code = topic.split(".", 1)[1]
                    entry = results.get(symbol_code)
                    if entry is not None:
                        entry["ticker"] = payload
                        if "orderbook" in entry:
                            pending.discard(symbol_code)
                elif topic.startswith("orderbook."):
                    symbol_code = topic.rsplit(".", 1)[-1]
                    entry = results.get(symbol_code)
                    if entry is not None:
                        entry["orderbook"] = payload
                        if "ticker" in entry:
                            pending.discard(symbol_code)
    except Exception:
        return {}

    return results


async def _mexc_ws_snapshot(symbols: List[str], max_wait: float = 3.0) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}

    results: Dict[str, Dict[str, Any]] = {symbol: {} for symbol in symbols}
    pending = set(symbols)
    try:
        async with websockets.connect(
            MEXC_WS_URL,
            ping_interval=20,
            ping_timeout=20,
            max_size=1_000_000,
            extra_headers={
                "User-Agent": "Mozilla/5.0",
                "Origin": "https://contract.mexc.com",
            },
        ) as ws:
            for idx, symbol in enumerate(symbols, start=1):
                await ws.send(json.dumps({"method": "sub.ticker", "params": [symbol], "id": idx}))
                await ws.send(json.dumps({"method": "sub.depth", "params": [symbol, 20], "id": idx + 1000}))
                await ws.send(json.dumps({"method": "sub.fundingRate", "params": [symbol], "id": idx + 2000}))

            deadline = asyncio.get_event_loop().time() + max_wait
            while pending and asyncio.get_event_loop().time() < deadline:
                timeout = max(0.1, deadline - asyncio.get_event_loop().time())
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break
                data = json.loads(message)
                method = data.get("method")
                params = data.get("params") or []
                if not method or not isinstance(params, list):
                    continue
                payload = params[0] if params else None
                if not isinstance(payload, dict):
                    continue
                symbol_code = payload.get("symbol")
                entry = results.get(symbol_code)
                if entry is None:
                    continue
                if method == "push.ticker":
                    entry["ticker"] = payload
                elif method == "push.depth":
                    entry["orderbook"] = payload
                elif method == "push.fundingRate":
                    entry["funding"] = payload
                if all(key in entry for key in ("ticker", "orderbook", "funding")):
                    pending.discard(symbol_code)
    except Exception:
        return {}

    return results


async def _fetch_bybit_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    ws_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    params = {"category": "linear", "symbol": symbol}
    ticker_resp = await _fetch_json(session, BYBIT_TICKER_URL, params=params)
    ticker = ticker_resp.get("result", {}).get("list", [{}])[0]
    if ws_snapshots and symbol in ws_snapshots and "ticker" in ws_snapshots[symbol]:
        ticker = {**ticker, **ws_snapshots[symbol]["ticker"]}

    if ws_snapshots and symbol in ws_snapshots and "orderbook" in ws_snapshots[symbol]:
        book_data = ws_snapshots[symbol]["orderbook"]
        bids = book_data.get("b", [])
        asks = book_data.get("a", [])
    else:
        orderbook_resp = await _fetch_json(
            session,
            BYBIT_ORDERBOOK_URL,
            params={"category": "linear", "symbol": symbol, "limit": 25},
        )
        bids = orderbook_resp.get("result", {}).get("b", [])
        asks = orderbook_resp.get("result", {}).get("a", [])
    ob_summary = _orderbook_summary(bids, asks)

    instrument_resp = await _fetch_json(session, BYBIT_INSTRUMENT_URL, params=params)
    instrument = instrument_resp.get("result", {}).get("list", [{}])[0]

    async def fetch_kline(interval: str, limit: int) -> List[Dict[str, float]]:
        kline_resp = await _fetch_json(
            session,
            BYBIT_KLINE_URL,
            params={"category": "linear", "symbol": symbol, "interval": interval, "limit": limit},
        )
        series = kline_resp.get("result", {}).get("list", [])
        candles = []
        for entry in series:
            ts, open_, high, low, close, volume, *_rest = entry
            candles.append(
                {
                    "time": int(ts),
                    "open": _as_float(open_) or 0.0,
                    "high": _as_float(high) or 0.0,
                    "low": _as_float(low) or 0.0,
                    "close": _as_float(close) or 0.0,
                    "volume": _as_float(volume) or 0.0,
                }
            )
        return candles

    kline_1h = await fetch_kline("60", 15)
    kline_4h = await fetch_kline("240", 15)
    kline_1m = await fetch_kline("1", 15)

    volume_1h = kline_1h[0]["volume"] if kline_1h else None
    volume_24h = _as_float(ticker.get("turnover24h"))
    open_interest = _as_float(ticker.get("openInterestValue")) or _as_float(ticker.get("openInterest"))
    relative_oi_24h = (open_interest / volume_24h) if open_interest and volume_24h else None

    now_ms = int(time.time() * 1000)
    next_funding_time = _as_float(ticker.get("nextFundingTime"))
    time_to_funding = (next_funding_time - now_ms) / 1000 if next_funding_time else None

    return {
        "exchange": "bybit",
        "symbol": symbol,
        "funding": {
            "current": _as_float(ticker.get("fundingRate")),
            "next_rate_estimate": _as_float(ticker.get("predictedFundingRate")),
            "interval_hours": _as_float(ticker.get("fundingIntervalHour")),
            "next_timestamp": next_funding_time,
            "seconds_to_funding": time_to_funding,
        },
        "orderbook": ob_summary,
        "volume": {
            "vol_1h": volume_1h,
            "vol_24h": volume_24h,
        },
        "open_interest": {
            "absolute": open_interest,
            "oi_over_24h_volume": relative_oi_24h,
            "oi_over_freefloat": None,
        },
        "volatility": {
            "atr_pct_1h": _atr_pct_from_candles(kline_1h),
            "atr_pct_4h": _atr_pct_from_candles(kline_4h),
            "sigma_15m": _sigma_from_closes(kline_1m),
        },
        "contract_specs": {
            "tick_size": _as_float(instrument.get("priceFilter", {}).get("tickSize")),
            "lot_size": _as_float(instrument.get("lotSizeFilter", {}).get("qtyStep")),
            "min_notional": _as_float(instrument.get("lotSizeFilter", {}).get("minNotionalValue")),
            "max_leverage": _as_float(instrument.get("leverageFilter", {}).get("maxLeverage")),
        },
    }


async def _fetch_mexc_detail_map(session: aiohttp.ClientSession) -> Dict[str, Dict[str, Any]]:
    detail_resp = await _fetch_json(session, MEXC_DETAIL_URL)
    return {item.get("symbol"): item for item in detail_resp.get("data", [])}


async def _fetch_mexc_ticker_map(session: aiohttp.ClientSession) -> Dict[str, Dict[str, Any]]:
    ticker_resp = await _fetch_json(session, MEXC_TICKER_URL)
    return {item.get("symbol"): item for item in ticker_resp.get("data", [])}


async def _fetch_mexc_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    detail_map: Dict[str, Dict[str, Any]],
    ticker_map: Dict[str, Dict[str, Any]],
    ws_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    ticker = ticker_map.get(symbol, {})
    if ws_snapshots and symbol in ws_snapshots and "ticker" in ws_snapshots[symbol]:
        ticker = {**ticker, **ws_snapshots[symbol]["ticker"]}

    if ws_snapshots and symbol in ws_snapshots and "orderbook" in ws_snapshots[symbol]:
        depth_data = ws_snapshots[symbol]["orderbook"]
        bids = depth_data.get("bids", []) or depth_data.get("b", [])
        asks = depth_data.get("asks", []) or depth_data.get("a", [])
    else:
        depth_resp = await _fetch_json(session, MEXC_DEPTH_URL.format(symbol=symbol), params={"limit": 20})
        depth_data = depth_resp.get("data", {})
        bids = depth_data.get("bids", []) or depth_data.get("b", [])
        asks = depth_data.get("asks", []) or depth_data.get("a", [])

    ob_summary = _orderbook_summary(bids, asks)

    if ws_snapshots and symbol in ws_snapshots and "funding" in ws_snapshots[symbol]:
        funding_data = ws_snapshots[symbol]["funding"]
    else:
        funding_resp = await _fetch_json(session, MEXC_FUNDING_URL.format(symbol=symbol))
        funding_data = funding_resp.get("data", {})

    async def fetch_kline(interval: str, limit: int) -> List[Dict[str, float]]:
        resp = await _fetch_json(session, MEXC_KLINE_URL.format(symbol=symbol), params={"interval": interval, "limit": limit})
        data = resp.get("data", {})
        times = data.get("time", [])
        opens = data.get("open", [])
        highs = data.get("high", [])
        lows = data.get("low", [])
        closes = data.get("close", [])
        vols = data.get("vol", [])
        candles = []
        for idx, ts in enumerate(times):
            candles.append(
                {
                    "time": int(ts) * 1000,
                    "open": _as_float(opens[idx]) or 0.0,
                    "high": _as_float(highs[idx]) or 0.0,
                    "low": _as_float(lows[idx]) or 0.0,
                    "close": _as_float(closes[idx]) or 0.0,
                    "volume": _as_float(vols[idx]) or 0.0,
                }
            )
        return candles

    kline_1h = await fetch_kline("Min60", 15)
    kline_4h = await fetch_kline("Hour4", 15)
    kline_1m = await fetch_kline("Min1", 15)

    volume_1h = kline_1h[0]["volume"] if kline_1h else None
    volume_24h = _as_float(ticker.get("amount24"))
    open_interest = _as_float(ticker.get("holdVol"))
    relative_oi_24h = (open_interest / volume_24h) if open_interest and volume_24h else None

    now_ms = int(time.time() * 1000)
    next_funding_time = _as_float(funding_data.get("nextSettleTime"))
    time_to_funding = (next_funding_time - now_ms) / 1000 if next_funding_time else None

    detail = detail_map.get(symbol, {})

    return {
        "exchange": "mexc",
        "symbol": symbol,
        "funding": {
            "current": _as_float(ticker.get("fundingRate")),
            "next_rate_estimate": _as_float(funding_data.get("fundingRate")),
            "interval_hours": _as_float(funding_data.get("collectCycle")),
            "next_timestamp": next_funding_time,
            "seconds_to_funding": time_to_funding,
        },
        "orderbook": ob_summary,
        "volume": {
            "vol_1h": volume_1h,
            "vol_24h": volume_24h,
        },
        "open_interest": {
            "absolute": open_interest,
            "oi_over_24h_volume": relative_oi_24h,
            "oi_over_freefloat": None,
        },
        "volatility": {
            "atr_pct_1h": _atr_pct_from_candles(kline_1h),
            "atr_pct_4h": _atr_pct_from_candles(kline_4h),
            "sigma_15m": _sigma_from_closes(kline_1m),
        },
        "contract_specs": {
            "tick_size": detail.get("priceUnit"),
            "lot_size": detail.get("volUnit"),
            "min_volume": detail.get("minVol"),
            "max_leverage": detail.get("maxLeverage"),
        },
    }


async def gather_metrics(bybit_symbols: List[str], mexc_symbols: List[str], *, use_ws: bool = False) -> Dict[str, Any]:
    ws_bybit: Dict[str, Dict[str, Any]] = {}
    ws_mexc: Dict[str, Dict[str, Any]] = {}
    if use_ws:
        ws_bybit = await _bybit_ws_snapshot(bybit_symbols)
        ws_mexc = await _mexc_ws_snapshot(mexc_symbols)

    async with aiohttp.ClientSession(headers={"User-Agent": "futures-metrics-probe/0.1"}) as session:
        bybit_tasks = [
            asyncio.create_task(_fetch_bybit_symbol(session, sym, ws_bybit))
            for sym in bybit_symbols
        ]
        detail_map = await _fetch_mexc_detail_map(session)
        ticker_map = await _fetch_mexc_ticker_map(session)
        mexc_tasks = [
            asyncio.create_task(_fetch_mexc_symbol(session, sym, detail_map, ticker_map, ws_mexc))
            for sym in mexc_symbols
        ]
        bybit_results = await asyncio.gather(*bybit_tasks, return_exceptions=True)
        mexc_results = await asyncio.gather(*mexc_tasks, return_exceptions=True)

    def normalize(results: Iterable[Any]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for result in results:
            if isinstance(result, Exception):
                items.append({"error": str(result)})
            else:
                items.append(result)
        return items

    return {
        "bybit": normalize(bybit_results),
        "mexc": normalize(mexc_results),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe Bybit and MEXC contract metrics")
    parser.add_argument("--bybit", default="BTCUSDT,ETHUSDT", help="Comma-separated Bybit symbols")
    parser.add_argument("--mexc", default="BTC_USDT,ETH_USDT", help="Comma-separated MEXC symbols")
    parser.add_argument("--use-ws", action="store_true", help="Augment REST data with websocket snapshots (best effort)")
    args = parser.parse_args()

    bybit_symbols = [token.strip().upper() for token in args.bybit.split(",") if token.strip()]
    mexc_symbols = [token.strip().upper() for token in args.mexc.split(",") if token.strip()]

    metrics = asyncio.run(gather_metrics(bybit_symbols, mexc_symbols, use_ws=args.use_ws))
    print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
