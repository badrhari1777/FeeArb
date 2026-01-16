import argparse
import os
import sys
import asyncio
import hashlib
import hmac
import json
import time

import websockets

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from execution.accounts import EXCHANGE_SPECS, ExchangeGateway, _bootstrap_env, _ccxt_perp_symbol


BYBIT_TRADE_WS_URL = "wss://stream.bybit.com/v5/trade"


def _resolve_symbol(raw: str) -> str:
    symbol = (raw or "").strip().upper()
    if not symbol:
        return symbol
    if symbol.endswith("USDT") or symbol.endswith("USD"):
        return symbol
    return f"{symbol}USDT"


def _bybit_gateway() -> ExchangeGateway:
    for spec in EXCHANGE_SPECS:
        if spec.slug == "bybit":
            return ExchangeGateway(spec)
    raise RuntimeError("bybit spec not found")


def _sign_bybit_ws(api_secret: str, expires_ms: int) -> str:
    payload = f"GET/realtime{expires_ms}"
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


async def _await_auth(ws: websockets.WebSocketClientProtocol, *, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
        except Exception:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            print(f"[rx] {raw}")
            continue
        print(f"[rx] {payload}")
        if payload.get("op") == "auth":
            return payload.get("success") is True or str(payload.get("retCode")) == "0"
        if payload.get("op") == "ping":
            await ws.send(json.dumps({"op": "pong"}))
    return False


async def _read_for_ack(
    ws: websockets.WebSocketClientProtocol,
    *,
    timeout: float,
    expected_op: str,
) -> dict | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
        except Exception:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            print(f"[rx] {raw}")
            continue
        print(f"[rx] {payload}")
        if payload.get("op") == "ping":
            pong = {"op": "pong"}
            await ws.send(json.dumps(pong))
            print(f"[tx] {pong}")
        if payload.get("op") == expected_op:
            return payload
    return None


async def _fetch_order(gateway: ExchangeGateway, symbol: str, order_id: str) -> None:
    await gateway.ensure_client()
    client = gateway.client
    if client is None:
        print("[err] ccxt client unavailable for fetch")
        return
    ccxt_symbol = _ccxt_perp_symbol(symbol)
    try:
        await client.load_markets()
    except Exception:
        pass
    try:
        order = await client.fetch_order(order_id, ccxt_symbol, {"acknowledged": True})
    except Exception as exc:
        print(f"[err] fetch failed: {exc}")
        return
    print(f"[fetch] {order}")


async def run(
    symbol: str,
    qty: float,
    price: float,
    side: str,
    timeout: float,
    fetch_delay: float,
    cancel_delay: float,
    fetch_rest: bool,
) -> None:
    _bootstrap_env(force=True)
    gateway = _bybit_gateway()
    gateway.refresh_credentials(force_env=True)
    if not gateway.api_key or not gateway.api_secret:
        raise RuntimeError("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env")

    symbol = _resolve_symbol(symbol)
    if not symbol:
        raise RuntimeError("Symbol is required")
    if side.lower() not in ("buy", "sell"):
        raise RuntimeError("Side must be buy or sell")

    async with websockets.connect(
        BYBIT_TRADE_WS_URL,
        ping_interval=20,
        ping_timeout=10,
    ) as ws:
        expires = int(time.time() * 1000) + 5000
        signature = _sign_bybit_ws(gateway.api_secret, expires)
        auth_payload = {"op": "auth", "args": [gateway.api_key, expires, signature]}
        await ws.send(json.dumps(auth_payload))
        print(f"[tx] {auth_payload}")

        ok = await _await_auth(ws)
        if not ok:
            raise RuntimeError("Bybit WS auth failed")

        req_id = f"req-{int(time.time() * 1000)}"
        api_ts = int(time.time() * 1000)
        recv_window = 5000
        order_payload = {
            "op": "order.create",
            "reqId": req_id,
            "header": {
                "X-BAPI-TIMESTAMP": str(api_ts),
                "X-BAPI-RECV-WINDOW": str(recv_window),
            },
            "args": [
                {
                    "category": "linear",
                    "symbol": symbol,
                    "side": "Buy" if side.lower() == "buy" else "Sell",
                    "orderType": "Limit",
                    "qty": str(qty),
                    "price": str(price),
                    "timeInForce": "GTC",
                    "apiTimestamp": str(api_ts),
                    "recvWindow": recv_window,
                }
            ],
        }
        await ws.send(json.dumps(order_payload))
        print(f"[tx] {order_payload}")

        ack = await _read_for_ack(ws, timeout=timeout, expected_op="order.create")
        if not ack:
            print(f"[warn] no order.create response within {timeout:.0f}s")
            return
        data = ack.get("data") if isinstance(ack.get("data"), dict) else None
        order_id = data.get("orderId") if data else None
        if not order_id:
            print("[warn] order_id missing; cannot fetch/cancel")
            return

        if fetch_rest:
            if fetch_delay > 0:
                print(f"[wait] {fetch_delay:.0f}s before fetch")
                await asyncio.sleep(fetch_delay)
            await _fetch_order(gateway, symbol, order_id)
        else:
            print("[skip] fetch disabled (ws-only mode)")

        if cancel_delay > 0:
            print(f"[wait] {cancel_delay:.0f}s before cancel")
            await asyncio.sleep(cancel_delay)
        cancel_ts = int(time.time() * 1000)
        cancel_payload = {
            "op": "order.cancel",
            "header": {
                "X-BAPI-TIMESTAMP": str(cancel_ts),
                "X-BAPI-RECV-WINDOW": "5000",
            },
            "args": [
                {
                    "category": "linear",
                    "symbol": symbol,
                    "orderId": order_id,
                    "apiTimestamp": str(cancel_ts),
                    "recvWindow": 5000,
                }
            ],
        }
        await ws.send(json.dumps(cancel_payload))
        print(f"[tx] {cancel_payload}")
        cancel_ack = await _read_for_ack(ws, timeout=timeout, expected_op="order.cancel")
        if not cancel_ack:
            print(f"[warn] no order.cancel response within {timeout:.0f}s")
            return
        print(f"[rx] {cancel_ack}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a single Bybit WS limit order and print responses.")
    parser.add_argument("--symbol", default="RIVERUSDT", help="Symbol, e.g. RIVER or RIVERUSDT")
    parser.add_argument("--qty", type=float, default=1.0, help="Order quantity")
    parser.add_argument("--price", type=float, default=14.0, help="Limit price")
    parser.add_argument("--side", default="buy", help="buy or sell")
    parser.add_argument("--timeout", type=float, default=20.0, help="Seconds to wait for responses")
    parser.add_argument("--fetch-delay", type=float, default=10.0, help="Seconds to wait before fetch")
    parser.add_argument("--cancel-delay", type=float, default=10.0, help="Seconds to wait before cancel")
    parser.add_argument(
        "--fetch-rest",
        action="store_true",
        help="Use REST fetch via ccxt before cancel (disabled by default)",
    )
    args = parser.parse_args()

    asyncio.run(
        run(
            args.symbol,
            args.qty,
            args.price,
            args.side,
            args.timeout,
            args.fetch_delay,
            args.cancel_delay,
            args.fetch_rest,
        )
    )


if __name__ == "__main__":
    main()
