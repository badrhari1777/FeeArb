"""Lifecycle orchestration for the active FeeArb runtime.

This mixin keeps startup, shutdown and scheduler ownership out of the HTTP/data
facade.  It deliberately contains no trading decisions: exchange-effect methods
remain on their owning Manual, Grid and protection controllers.
"""

from __future__ import annotations

import asyncio
import logging

from exchanges import normalize_exchange_name
from utils import purge_expired


logger = logging.getLogger(__name__)


class RuntimeLifecycleMixin:
    """Own active background-task lifecycle for ``DataService``."""

    async def startup(self) -> None:
        self._loop = asyncio.get_running_loop()
        purge_expired()
        async with self._lock:
            self._status = "pending"
            self._account_interval = self._settings_manager.current.account_refresh_seconds
        await self._accounts.start()
        # Do an immediate balance/positions pull before other work.
        await self._accounts.refresh_now(force_env=True)
        await self._refresh_positions_market_snapshots(force=True)
        await self._maybe_sync_protective_orders()
        # Candidate discovery belongs to Strategy Lab and must not start as a
        # side effect of the trading dashboard lifecycle.
        async with self._lock:
            self._status = "ready"
        if self._positions_market_task is None:
            self._positions_market_task = asyncio.create_task(self._positions_market_scheduler())
        if self._protective_task is None:
            self._protective_task = asyncio.create_task(self._protective_scheduler())
        if self._runtime_modules.auto_arb_grid and self._automation_task is None:
            self._automation_task = asyncio.create_task(self._automation_scheduler())
        await self._telemetry.start()

    async def shutdown(self) -> None:
        if self._settings_refresh_task:
            self._settings_refresh_task.cancel()
            try:
                await self._settings_refresh_task
            except asyncio.CancelledError:
                pass
            self._settings_refresh_task = None
            self._settings_refresh_pending = False
        for attr in ("_positions_market_task", "_automation_task", "_protective_task"):
            task = getattr(self, attr)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, attr, None)
        await self._market_data.shutdown()
        await self._telemetry.stop()
        await self._accounts.stop()
        await self._manual.close()
        await self._protective_manager.close()

    async def _protective_scheduler(self) -> None:
        """Independent loop for balance/position driven protective upkeep."""
        try:
            while True:
                interval = max(30, int(self._protective_interval or self._account_interval))
                await asyncio.sleep(interval)
                await self._maybe_sync_protective_orders()
        except asyncio.CancelledError:
            raise

    async def _restart_protective_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._protective_task:
            self._protective_task.cancel()
            try:
                await self._protective_task
            except asyncio.CancelledError:
                pass
            self._protective_task = None
        self._protective_task = asyncio.create_task(self._protective_scheduler())

    async def _positions_market_scheduler(self) -> None:
        """Refresh market snapshots for live positions on a separate cadence."""
        try:
            while True:
                interval = max(30, int(self._positions_market_interval or self._account_interval))
                await asyncio.sleep(interval)
                await self._refresh_positions_market_snapshots()
        except asyncio.CancelledError:
            raise

    async def _restart_positions_market_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._positions_market_task:
            self._positions_market_task.cancel()
            try:
                await self._positions_market_task
            except asyncio.CancelledError:
                pass
            self._positions_market_task = None
        self._positions_market_task = asyncio.create_task(self._positions_market_scheduler())

    async def on_settings_updated(self) -> None:
        async with self._lock:
            current = self._settings_manager.current
            self._account_interval = current.account_refresh_seconds
            self._positions_market_interval = current.positions_market_refresh_seconds
            self._summary_interval = current.summary_refresh_seconds
            self._risk_config = self._risk_config_from_settings()
            self._protective_manager.update_config(self._risk_config)
            self._protective_interval = getattr(
                self._risk_config,
                "position_check_interval_sec",
                self._protective_interval,
            )
            self._apply_alert_settings()
        await self._restart_protective_scheduler()
        await self._restart_positions_market_scheduler()
        self._accounts.update_interval(self._account_interval)
        self._accounts.update_summary_interval(self._summary_interval)
        self._accounts.update_enabled_exchanges(self._account_monitor_enabled_exchanges())
        self._settings_refresh_pending = True
        if self._settings_refresh_task is None or self._settings_refresh_task.done():
            self._settings_refresh_task = asyncio.create_task(
                self._refresh_operational_state_after_settings()
            )

    async def _refresh_operational_state_after_settings(self) -> None:
        try:
            while self._settings_refresh_pending:
                self._settings_refresh_pending = False
                await self._accounts.refresh_now(force_env=True)
                await self._refresh_positions_market_snapshots(force=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Post-settings operational refresh failed: %s", exc)
        finally:
            self._settings_refresh_task = None

    def _account_monitor_enabled_exchanges(self) -> set[str]:
        """Poll only venues enabled by at least one active read surface."""
        current = self._settings_manager.current
        result: set[str] = set()
        for flags in (
            getattr(current, "exchanges", None) or {},
            getattr(current, "analysis_exchanges", None) or {},
        ):
            for name, enabled in flags.items():
                normalized = normalize_exchange_name(str(name))
                if enabled and normalized:
                    result.add(normalized)
        return result


__all__ = ["RuntimeLifecycleMixin"]
