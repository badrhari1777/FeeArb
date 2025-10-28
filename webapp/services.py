from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

from pipeline import collect_snapshot, DataSnapshot


class DataService:
    def __init__(self, refresh_interval: int = 300) -> None:
        self.refresh_interval = refresh_interval
        self._snapshot: Optional[DataSnapshot] = None
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None

    async def startup(self) -> None:
        await self.refresh_snapshot()
        if self._task is None:
            self._task = asyncio.create_task(self._scheduler())

    async def shutdown(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _scheduler(self) -> None:
        while True:
            await asyncio.sleep(self.refresh_interval)
            await self.refresh_snapshot()

    async def refresh_snapshot(self) -> None:
        async with self._lock:
            self._snapshot = await asyncio.to_thread(collect_snapshot)

    def latest_snapshot(self) -> Optional[DataSnapshot]:
        return self._snapshot

    def latest_snapshot_dict(self) -> dict[str, object] | None:
        if self._snapshot is None:
            return None
        return self._snapshot.as_dict()
