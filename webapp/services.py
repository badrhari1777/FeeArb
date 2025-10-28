from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Literal, Optional

from pipeline import collect_snapshot, DataSnapshot

RefreshResult = Literal["completed", "in_progress", "failed"]

logger = logging.getLogger(__name__)


class DataService:
    def __init__(self, refresh_interval: int = 300) -> None:
        self.refresh_interval = refresh_interval
        self._snapshot: Optional[DataSnapshot] = None
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._bootstrap_task: Optional[asyncio.Task] = None
        self._status: str = "idle"
        self._last_error: Optional[str] = None
        self._last_refreshed: Optional[datetime] = None
        self._in_progress: bool = False

    async def startup(self) -> None:
        async with self._lock:
            self._status = "pending"
        if self._task is None:
            self._task = asyncio.create_task(self._scheduler())
        if self._bootstrap_task is None or self._bootstrap_task.done():
            self._bootstrap_task = asyncio.create_task(self.refresh_snapshot())

    async def shutdown(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._bootstrap_task:
            self._bootstrap_task.cancel()
            try:
                await self._bootstrap_task
            except asyncio.CancelledError:
                pass
            self._bootstrap_task = None

    async def _scheduler(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.refresh_interval)
                result = await self.refresh_snapshot()
                if result == "failed":
                    logger.warning(
                        "Scheduled snapshot refresh failed; will retry after interval."
                    )
        except asyncio.CancelledError:
            raise

    async def refresh_snapshot(self) -> RefreshResult:
        async with self._lock:
            if self._in_progress:
                return "in_progress"
            self._in_progress = True
            self._status = "pending"
            self._last_error = None

        outcome: RefreshResult = "completed"
        try:
            snapshot = await asyncio.to_thread(collect_snapshot)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Snapshot refresh raised an error")
            outcome = "failed"
            async with self._lock:
                self._last_error = str(exc)
                self._status = "error"
        else:
            async with self._lock:
                self._snapshot = snapshot
                self._status = "ready"
                self._last_error = None
                self._last_refreshed = datetime.now(timezone.utc)
        finally:
            async with self._lock:
                self._in_progress = False

        return outcome

    def latest_snapshot(self) -> Optional[DataSnapshot]:
        return self._snapshot

    def latest_snapshot_dict(self) -> dict[str, object] | None:
        if self._snapshot is None:
            return None
        return self._snapshot.as_dict()

    def state_payload(self) -> dict[str, object]:
        snapshot_dict = self._snapshot.as_dict() if self._snapshot else None
        status = self._status
        if status == "idle" and snapshot_dict:
            status = "ready"
        return {
            "status": status,
            "refresh_interval": self.refresh_interval,
            "last_error": self._last_error,
            "last_updated": (
                self._last_refreshed.isoformat() if self._last_refreshed else None
            ),
            "snapshot": snapshot_dict,
            "refresh_in_progress": self._in_progress,
        }
