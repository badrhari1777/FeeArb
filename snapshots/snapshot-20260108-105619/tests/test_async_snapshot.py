from __future__ import annotations

import unittest

try:
    import aiohttp  # noqa: F401
except ImportError:  # pragma: no cover - optional dependency missing
    AIOHTTP_AVAILABLE = False
else:
    AIOHTTP_AVAILABLE = True

from pipeline import collect_snapshot_async


@unittest.skipUnless(AIOHTTP_AVAILABLE, "aiohttp is required for async snapshot tests")
class AsyncSnapshotTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_collect_snapshot_async_without_sources(self) -> None:
        snapshot = await collect_snapshot_async(
            source_settings={"arbitragescanner": False, "coinglass": False},
            exchange_settings={"bybit": False, "mexc": False},
        )
        self.assertEqual(snapshot.screener_rows, [])
        self.assertEqual(snapshot.coinglass_rows, [])
        self.assertEqual(snapshot.universe, [])
        self.assertEqual(snapshot.opportunities, [])
        self.assertIn("ArbitrageScanner fetch disabled via settings.", snapshot.messages)
        self.assertIn("Coinglass fetch disabled via settings.", snapshot.messages)
