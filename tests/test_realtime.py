from __future__ import annotations

import asyncio
import unittest

from webapp.realtime import ConnectionManager


class _DummyWebSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.closed = False
        self.sent = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, payload) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed = True

    async def receive_text(self) -> str:
        await asyncio.sleep(0.01)
        return ""


class ConnectionManagerTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast(self) -> None:
        manager = ConnectionManager()
        websocket = _DummyWebSocket()
        await manager.connect(websocket)
        await manager.broadcast({"foo": "bar"})
        self.assertTrue(websocket.accepted)
        self.assertEqual(websocket.sent[-1], {"foo": "bar"})
        await manager.disconnect(websocket)
        self.assertTrue(websocket.closed)


if __name__ == "__main__":
    unittest.main()
