from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional

from pipeline import DataSnapshot, collect_snapshot_async
from project_settings import SettingsManager
from execution import (
    ExecutionSettingsManager,
    WalletService,
    PositionManager,
    TelemetryClient,
)
from execution.allocator import Allocator
from execution.lifecycle import LifecycleController
from execution.settings import ExecutionSettings
from .realtime import ConnectionManager

RefreshResult = Literal["completed", "in_progress", "failed"]

logger = logging.getLogger(__name__)


class DataService:
    def __init__(self, settings_manager: SettingsManager | None = None) -> None:
        self._settings_manager = settings_manager or SettingsManager()
        self._parser_interval = self._settings_manager.current.parser_refresh_seconds
        self._snapshot: Optional[DataSnapshot] = None
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._bootstrap_task: Optional[asyncio.Task] = None
        self._status: str = "idle"
        self._last_error: Optional[str] = None
        self._last_refreshed: Optional[datetime] = None
        self._in_progress: bool = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._events: List[dict[str, Any]] = []
        self._exchange_status: Dict[str, dict[str, Any]] = {}
        self._exec_settings_manager = ExecutionSettingsManager()
        self._execution_settings: ExecutionSettings = self._exec_settings_manager.current
        self._wallet = WalletService(self._execution_settings.balance.initial_balances)
        self._positions = PositionManager(self._wallet)
        self._allocator = Allocator(self._wallet, self._positions, self._execution_settings)
        self._lifecycle = LifecycleController(self._execution_settings, self._positions, self._allocator)
        self._telemetry = TelemetryClient(self._execution_settings)
        self._telemetry_events: List[dict[str, Any]] = []
        self._realtime: ConnectionManager | None = None
        self._telemetry.register_listener(self._handle_telemetry_event)


    async def startup(self) -> None:
        self._loop = asyncio.get_running_loop()
        async with self._lock:
            self._status = "pending"
            self._parser_interval = self._settings_manager.current.parser_refresh_seconds
        if self._task is None:
            await self._restart_scheduler()
        if self._bootstrap_task is None or self._bootstrap_task.done():
            self._bootstrap_task = asyncio.create_task(self.refresh_snapshot())
        await self._telemetry.start()

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
        await self._telemetry.stop()

    def attach_realtime(self, manager: ConnectionManager) -> None:
        self._realtime = manager

    async def _scheduler(self) -> None:
        try:
            while True:
                interval = max(self._parser_interval, 1)
                await asyncio.sleep(interval)
                result = await self.refresh_snapshot()
                if result == "failed":
                    logger.warning(
                        "Scheduled snapshot refresh failed; will retry after interval."
                    )
        except asyncio.CancelledError:
            raise

    async def _restart_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._task = asyncio.create_task(self._scheduler())

    async def refresh_snapshot(self) -> RefreshResult:
        async with self._lock:
            if self._in_progress:
                return "in_progress"
            self._in_progress = True
            self._status = "pending"
            self._last_error = None
            self._events = []
            self._exchange_status = {}
        self._record_event(
            "refresh:start",
            {"message": "Snapshot refresh started"},
        )

        outcome: RefreshResult = "completed"
        loop = self._loop or asyncio.get_running_loop()
        progress_cb = self._make_progress_callback(loop)
        current_settings = self._settings_manager.current
        source_flags = dict(current_settings.sources)
        exchange_flags = dict(current_settings.exchanges)
        try:
            snapshot = await collect_snapshot_async(
                progress_cb,
                source_settings=source_flags,
                exchange_settings=exchange_flags,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Snapshot refresh raised an error")
            outcome = "failed"
            self._record_event(
                "refresh:failed",
                {"message": "Snapshot refresh failed", "error": str(exc)},
            )
            async with self._lock:
                self._last_error = str(exc)
                self._status = "error"
        else:
            self._record_event(
                "refresh:completed",
                {
                    "message": "Snapshot refresh completed successfully",
                    "opportunity_count": len(snapshot.opportunities),
                },
            )
            async with self._lock:
                self._snapshot = snapshot
                self._status = "ready"
                self._last_error = None
                self._last_refreshed = datetime.now(timezone.utc)
                self._parser_interval = current_settings.parser_refresh_seconds
                self._exchange_status = {
                    entry.get("exchange", f"exchange-{idx}"): entry
                    for idx, entry in enumerate(snapshot.exchange_status)
                }
        finally:
            async with self._lock:
                self._in_progress = False

        return outcome

    async def on_settings_updated(self) -> None:
        async with self._lock:
            self._parser_interval = self._settings_manager.current.parser_refresh_seconds
        await self._restart_scheduler()

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
        settings_payload = self._settings_manager.as_dict()
        parser_interval = int(
            settings_payload.get("parser_refresh_seconds", self._parser_interval)
        )
        table_interval = int(
            settings_payload.get("table_refresh_seconds", parser_interval)
        )
        return {
            "status": status,
            "refresh_interval": table_interval,
            "parser_refresh_interval": parser_interval,
            "last_error": self._last_error,
            "last_updated": (
                self._last_refreshed.isoformat() if self._last_refreshed else None
            ),
            "snapshot": snapshot_dict,
            "refresh_in_progress": self._in_progress,
            "events": list(self._events),
            "exchange_status": list(self._exchange_status.values()),
            "settings": settings_payload,
            "execution": self._execution_state(),
        }

    def telemetry_backlog(self, limit: int = 50) -> List[dict[str, Any]]:
        return list(self._telemetry_events[-limit:])

    def _execution_state(self) -> dict[str, object]:
        return {
            "wallets": [
                {
                    "exchange": account.exchange,
                    "total": account.total_balance,
                    "available": account.available,
                    "reserved": account.reserved,
                    "in_positions": account.in_positions,
                }
                for account in self._wallet.accounts()
            ],
            "reservations": [
                {
                    "allocation_id": allocation.allocation_id,
                    "symbol": allocation.symbol,
                    "long_exchange": allocation.long_exchange,
                    "short_exchange": allocation.short_exchange,
                    "notional": allocation.notional,
                    "created_at": _fmt_ts(allocation.created_at),
                }
                for allocation in self._allocator.pending_allocations()
            ],
            "positions": [
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "strategy": position.strategy,
                    "status": position.status,
                    "notional": position.legs["long"].target_amount,
                    "hedged_at": _fmt_ts(position.hedged_at),
                    "observation_started": _fmt_ts(position.observation_started_at),
                    "exit_started": _fmt_ts(position.exit_started_at),
                }
                for position in self._positions.active_positions()
            ],
            "telemetry": list(self._telemetry_events),
        }


    def _make_progress_callback(
        self, loop: asyncio.AbstractEventLoop
    ) -> Callable[[str, dict[str, Any] | None], None]:
        def _callback(event: str, payload: dict[str, Any] | None = None) -> None:
            data = dict(payload or {})
            loop.call_soon_threadsafe(self._record_event, event, data)
            if event.startswith("exchange:") and data:
                exchange = data.get("exchange")
                if exchange:
                    loop.call_soon_threadsafe(
                        self._update_exchange_status,
                        exchange,
                        event,
                        data,
                    )

        return _callback

    def _record_event(self, event: str, payload: dict[str, Any]) -> None:
        entry = {
            "event": event,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._events.append(entry)
        if len(self._events) > 200:
            del self._events[:-200]

    def _update_exchange_status(
        self, exchange: str, event: str, payload: dict[str, Any]
    ) -> None:
        status_map = {
            "exchange:success": "ok",
            "exchange:error": "failed",
            "exchange:missing": "missing",
            "exchange:start": "pending",
        }
        status = status_map.get(event, payload.get("status"))
        entry = {
            "exchange": exchange,
            "status": status or payload.get("status") or "unknown",
            "message": payload.get("message"),
            "count": payload.get("count"),
            "error": payload.get("error"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._exchange_status[exchange] = entry

    async def _handle_telemetry_event(self, entry: dict[str, Any]) -> None:
        self._telemetry_events.append(entry)
        if len(self._telemetry_events) > 200:
            self._telemetry_events = self._telemetry_events[-200:]
        if self._realtime is not None:
            await self._realtime.broadcast(entry)


def _fmt_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
