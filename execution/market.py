from __future__ import annotations

import asyncio
import math
import statistics
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Deque, Dict, Iterable, List, Optional, Tuple


@dataclass(slots=True)
class OrderBookSnapshot:
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]

    def spread(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return self.asks[0][0] - self.bids[0][0]

    def mid_price(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        best_bid = self.bids[0][0]
        best_ask = self.asks[0][0]
        return (best_bid + best_ask) / 2.0


@dataclass(slots=True)
class MarketUpdate:
    exchange: str
    symbol: str
    timestamp: float
    mark_price: float | None = None
    funding_rate: float | None = None
    next_funding_time: datetime | None = None
    predicted_funding_rate: float | None = None
    order_book: OrderBookSnapshot | None = None


@dataclass(slots=True)
class DerivedMetrics:
    spread: float | None
    mid_price: float | None
    bid_liquidity_top3: float
    ask_liquidity_top3: float
    min_top_liquidity: float
    short_term_volatility: float


class VolatilityTracker:
    """Rolling volatility estimator for mark or mid prices."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        self._window_seconds = window_seconds
        self._values: Deque[Tuple[float, float]] = deque()

    def update(self, timestamp: float, value: float) -> float:
        self._values.append((timestamp, value))
        self._trim(timestamp)
        if len(self._values) < 2:
            return 0.0
        samples = [item[1] for item in self._values]
        try:
            return statistics.pstdev(samples)
        except statistics.StatisticsError:
            return 0.0

    def _trim(self, now: float) -> None:
        limit = now - self._window_seconds
        while self._values and self._values[0][0] < limit:
            self._values.popleft()


def _top_notional(levels: Iterable[Tuple[float, float]], top_n: int) -> float:
    total = 0.0
    count = 0
    for price, size in levels:
        total += price * size
        count += 1
        if count >= top_n:
            break
    return total


def compute_metrics(
    update: MarketUpdate,
    *,
    volatility_tracker: VolatilityTracker,
    top_n: int = 3,
) -> DerivedMetrics:
    book = update.order_book
    spread = book.spread() if book else None
    mid_price = book.mid_price() if book else update.mark_price

    bid_liquidity = _top_notional(book.bids, top_n) if book else 0.0
    ask_liquidity = _top_notional(book.asks, top_n) if book else 0.0
    min_liquidity = min(bid_liquidity, ask_liquidity) if book else 0.0

    volatility = 0.0
    if mid_price and mid_price > 0:
        volatility = volatility_tracker.update(update.timestamp, mid_price)

    return DerivedMetrics(
        spread=spread,
        mid_price=mid_price,
        bid_liquidity_top3=bid_liquidity,
        ask_liquidity_top3=ask_liquidity,
        min_top_liquidity=min_liquidity,
        short_term_volatility=volatility,
    )


class FeedAdapter:
    """Base interface for exchange-specific feeds."""

    name: str

    async def run(self, queue: asyncio.Queue[MarketUpdate]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def close(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class MarketGateway:
    """Coordinate multiple market feeds and maintain derived metrics."""

    def __init__(self, *, volatility_window_seconds: float = 60.0) -> None:
        self._feeds: List[FeedAdapter] = []
        self._tasks: List[asyncio.Task] = []
        self._queue: asyncio.Queue[MarketUpdate] = asyncio.Queue()
        self._callbacks: List[Callable[[MarketUpdate, DerivedMetrics], Awaitable[None]]] = []
        self._volatility: Dict[Tuple[str, str], VolatilityTracker] = {}
        self._volatility_window_seconds = volatility_window_seconds
        self._latest_metrics: Dict[Tuple[str, str], DerivedMetrics] = {}
        self._running = False

    def register_feed(self, feed: FeedAdapter) -> None:
        if self._running:
            raise RuntimeError("Cannot register feed while running")
        self._feeds.append(feed)

    def subscribe(self, callback: Callable[[MarketUpdate, DerivedMetrics], Awaitable[None]]) -> None:
        self._callbacks.append(callback)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        for feed in self._feeds:
            task = loop.create_task(feed.run(self._queue))
            self._tasks.append(task)
        self._tasks.append(loop.create_task(self._consume_loop()))

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for feed in self._feeds:
            try:
                await feed.close()
            except Exception:
                pass
        for task in self._tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()

    async def _consume_loop(self) -> None:
        while self._running:
            update = await self._queue.get()
            tracker = self._volatility.setdefault(
                (update.exchange, update.symbol),
                VolatilityTracker(self._volatility_window_seconds),
            )
            metrics = compute_metrics(update, volatility_tracker=tracker)
            self._latest_metrics[(update.exchange, update.symbol)] = metrics
            await self._notify(update, metrics)

    async def _notify(self, update: MarketUpdate, metrics: DerivedMetrics) -> None:
        for callback in self._callbacks:
            try:
                await callback(update, metrics)
            except Exception:
                # log or ignore; placeholder for telemetry integration
                continue

    def latest_metrics(self, exchange: str, symbol: str) -> Optional[DerivedMetrics]:
        return self._latest_metrics.get((exchange, symbol))


class MockFeed(FeedAdapter):
    """Simple in-memory feed for tests and dry runs."""

    def __init__(self, name: str = "mock") -> None:
        self.name = name
        self._running = False

    async def run(self, queue: asyncio.Queue[MarketUpdate]) -> None:
        self._running = True
        ts = time.time()
        update = MarketUpdate(
            exchange=self.name,
            symbol="BTCUSDT",
            timestamp=ts,
            mark_price=100.0,
            funding_rate=0.0001,
            order_book=OrderBookSnapshot(
                bids=[(99.9, 5.0), (99.8, 4.0), (99.7, 3.0)],
                asks=[(100.1, 5.0), (100.2, 4.0), (100.3, 3.0)],
            ),
        )
        await queue.put(update)
        while self._running:
            await asyncio.sleep(0.1)

    async def close(self) -> None:
        self._running = False


import contextlib  # kept at end to avoid circular import in type checking
