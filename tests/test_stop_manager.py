from __future__ import annotations

import asyncio
import unittest

from risk.config import RiskConfig
from risk.stop_manager import ProtectiveOrderManager


class _FakeClient:
    async def fetch_open_orders(self, symbol: str, params: dict | None = None):  # noqa: ARG002
        return []


class _FakeGateway:
    slug = "binance"

    def __init__(self) -> None:
        self.client = _FakeClient()

    def map_symbol(self, symbol: str) -> str:
        return symbol


class StopManagerBinanceAlgoSideTestCase(unittest.TestCase):
    def test_fetch_existing_keeps_position_side_for_invalid_side_check(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())

        async def _fake_fetch_algo(_gateway, _symbol):
            return [
                {
                    "algoId": "1000000001",
                    "symbol": "ZKPUSDT",
                    "side": "SELL",
                    "orderType": "STOP_MARKET",
                    "quantity": "55008.0",
                    "triggerPrice": "0.07315",
                    "reduceOnly": True,
                    "positionSide": "BOTH",
                }
            ]

        manager._fetch_binance_open_algo_orders = _fake_fetch_algo  # type: ignore[method-assign]

        async def _run() -> None:
            existing = await manager._fetch_existing(
                _FakeGateway(),
                "ZKPUSDT",
                None,
                "long",
                mark_price=0.10203,
                entry_price=0.10110,
            )
            self.assertFalse(existing.get("invalid_side"))
            self.assertEqual(len(existing.get("stop_orders") or []), 1)
            self.assertEqual(len(existing.get("take_orders") or []), 0)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
