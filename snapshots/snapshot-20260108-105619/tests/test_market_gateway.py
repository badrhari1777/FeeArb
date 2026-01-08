from __future__ import annotations

import asyncio
import unittest

from execution.market import MarketGateway, MockFeed


class MarketGatewayTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_mock_feed_metrics(self) -> None:
        gateway = MarketGateway()
        feed = MockFeed(name="bybit")
        gateway.register_feed(feed)

        received = asyncio.Event()

        async def on_update(update, metrics):
            if update.exchange == "bybit":
                received.set()

        gateway.subscribe(on_update)
        await gateway.start()
        await asyncio.wait_for(received.wait(), timeout=1.0)
        metrics = gateway.latest_metrics("bybit", "BTCUSDT")
        self.assertIsNotNone(metrics)
        if metrics:
            self.assertGreater(metrics.min_top_liquidity, 0.0)
        await gateway.stop()


if __name__ == "__main__":
    unittest.main()
