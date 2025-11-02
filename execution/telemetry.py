from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable, Deque, Dict, Iterable, Optional

from .settings import ExecutionSettings


class TelemetryClient:
    def __init__(self, settings: ExecutionSettings) -> None:
        self._settings = settings
        self._log_path = Path(settings.telemetry.structured_log_path)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer: Deque[Dict[str, object]] = deque(
            maxlen=settings.telemetry.max_events_in_memory
        )
        self._listeners: list[Callable[[Dict[str, object]], Awaitable[None] | None]] = []
        self._queue: asyncio.Queue[Dict[str, object]] = asyncio.Queue()
        self._pending: list[Dict[str, object]] = []
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._consumer())
        # flush pending events
        for entry in self._pending:
            await self._queue.put(entry)
        self._pending.clear()

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    def emit(self, event: str, payload: Optional[Dict[str, object]] = None) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "payload": payload or {},
        }
        self._buffer.append(entry)
        if self._task is None or self._task.done():
            self._pending.append(entry)
        else:
            try:
                self._queue.put_nowait(entry)
            except asyncio.QueueFull:  # pragma: no cover - unlikely
                self._pending.append(entry)

    def register_listener(
        self, callback: Callable[[Dict[str, object]], Awaitable[None] | None]
    ) -> None:
        self._listeners.append(callback)

    def tail(self, limit: int = 50) -> Iterable[Dict[str, object]]:
        return list(list(self._buffer)[-limit:])

    @property
    def log_path(self) -> Path:
        return self._log_path

    async def _consumer(self) -> None:
        while True:
            entry = await self._queue.get()
            try:
                await asyncio.to_thread(self._append_to_file, entry)
                for listener in list(self._listeners):
                    try:
                        result = listener(entry)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:  # pragma: no cover - listener errors logged silently
                        continue
            finally:
                self._queue.task_done()

    def _append_to_file(self, entry: Dict[str, object]) -> None:
        line = json.dumps(entry)
        with self._log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
