from __future__ import annotations

import asyncio
import json
from typing import List

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            if websocket in self._connections:
                self._connections.remove(websocket)
        try:
            await websocket.close()
        except Exception:  # pragma: no cover
            pass

    async def broadcast(self, message: dict[str, object]) -> None:
        async with self._lock:
            targets: List[WebSocket] = list(self._connections)
        for websocket in targets:
            try:
                await websocket.send_json(message)
            except Exception:
                await self.disconnect(websocket)
