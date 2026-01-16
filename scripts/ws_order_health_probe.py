import argparse
import asyncio
import os
import sys
import time

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from execution.ws_orders import LiveOrderTracker
from exchanges import normalize_exchange_name

DEFAULT_EXCHANGES = ["bybit", "okx", "gate", "bitget", "kucoin", "bingx"]


def _resolve_symbol(raw: str) -> str:
    symbol = (raw or "").strip().upper()
    if not symbol:
        return symbol
    if symbol.endswith(("USDT", "USDC", "USD")):
        return symbol
    return f"{symbol}USDT"


def _format_age(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}s"


async def _probe_exchange(exchange: str, symbol: str, duration: float, interval: float) -> None:
    tracker = LiveOrderTracker()
    await tracker.ensure([exchange], symbols={exchange: [symbol]})
    start = time.time()
    print(f"[{exchange}] probing for {duration:.0f}s (symbol={symbol})")
    while time.time() - start < duration:
        snapshot = tracker.health_snapshot(exchange)
        line = (
            f"[{exchange}] healthy={snapshot.get('healthy')} "
            f"last_rx={_format_age(snapshot.get('last_rx_sec'))} "
            f"last_order={_format_age(snapshot.get('last_order_sec'))} "
            f"last_ping={_format_age(snapshot.get('last_ping_sec'))} "
            f"last_pong={_format_age(snapshot.get('last_pong_sec'))}"
        )
        print(line)
        await asyncio.sleep(interval)
    await tracker.close()


async def run(args: argparse.Namespace) -> None:
    symbol = _resolve_symbol(args.symbol)
    if not symbol:
        raise SystemExit("symbol is required")
    if args.exchanges:
        exchanges = [normalize_exchange_name(item) for item in args.exchanges.split(",") if item.strip()]
    else:
        exchanges = list(DEFAULT_EXCHANGES)
    for exchange in exchanges:
        if exchange not in DEFAULT_EXCHANGES:
            print(f"[{exchange}] skipped (unsupported exchange)")
            continue
        try:
            await _probe_exchange(exchange, symbol, args.duration, args.interval)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[{exchange}] probe failed: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe WS order stream health via ping/pong.")
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol to subscribe (default: BTCUSDT)")
    parser.add_argument(
        "--exchanges",
        default="",
        help="Comma-separated exchanges (default: bybit,okx,gate,bitget,kucoin,bingx)",
    )
    parser.add_argument("--duration", type=float, default=45.0, help="Seconds per exchange")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between snapshots")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
