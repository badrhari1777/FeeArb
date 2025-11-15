from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional

from pipeline import (
    DataSnapshot,
    SourceSnapshot,
    build_snapshot_from_sources,
    collect_sources_async,
)
from orchestrator.models import MarketSnapshot
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
from execution.accounts import AccountMonitor, normalize_symbol
from utils import purge_expired

RefreshResult = Literal["completed", "in_progress", "failed"]

logger = logging.getLogger(__name__)


class DataService:
    def __init__(self, settings_manager: SettingsManager | None = None) -> None:
        self._settings_manager = settings_manager or SettingsManager()
        self._parser_interval = self._settings_manager.current.parser_refresh_seconds
        self._exchange_interval = self._settings_manager.current.exchange_refresh_seconds
        self._account_interval = self._settings_manager.current.account_refresh_seconds
        self._snapshot: Optional[DataSnapshot] = None
        self._cached_sources: Optional[SourceSnapshot] = None
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._bootstrap_task: Optional[asyncio.Task] = None
        self._status: str = "idle"
        self._last_error: Optional[str] = None
        self._last_refreshed: Optional[datetime] = None
        self._last_source_refresh: Optional[datetime] = None
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
        self._telemetry.register_listener(self._handle_telemetry_event)
        self._accounts = AccountMonitor(refresh_interval=self._account_interval)


    async def startup(self) -> None:
        self._loop = asyncio.get_running_loop()
        purge_expired()
        async with self._lock:
            self._status = "pending"
            self._parser_interval = self._settings_manager.current.parser_refresh_seconds
            self._exchange_interval = self._settings_manager.current.exchange_refresh_seconds
            self._account_interval = self._settings_manager.current.account_refresh_seconds
        await self._accounts.start()
        # Do an immediate balance/positions pull before other work.
        await self._accounts.refresh_now(force_env=True)
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
        await self._accounts.stop()

    async def _scheduler(self) -> None:
        try:
            while True:
                interval = max(self._exchange_interval, 1)
                await asyncio.sleep(interval)
                result = await self.refresh_snapshot(
                    force_sources=self._sources_due(),
                    force_accounts=False,
                )
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

    def _sources_due(self) -> bool:
        if self._cached_sources is None or self._last_source_refresh is None:
            return True
        age = datetime.now(timezone.utc) - self._last_source_refresh
        return age.total_seconds() >= max(self._parser_interval, 1)

    async def refresh_snapshot(self, *, force_sources: bool = True, force_accounts: bool = False) -> RefreshResult:
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
        sources: Optional[SourceSnapshot] = self._cached_sources

        need_sources = (
            force_sources
            or sources is None
            or self._sources_due()
        )
        if need_sources:
            try:
                sources = await collect_sources_async(
                    progress_cb,
                    source_settings=source_flags,
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Source refresh raised an error")
                self._record_event(
                    "sources:failed",
                    {"message": "Source refresh failed", "error": str(exc)},
                )
                if self._cached_sources is None:
                    outcome = "failed"
                    self._record_event(
                        "refresh:failed",
                        {
                            "message": "Snapshot refresh failed (no cached sources)",
                            "error": str(exc),
                        },
                    )
                    async with self._lock:
                        self._last_error = str(exc)
                        self._status = "error"
                        self._in_progress = False
                    return outcome
                sources = self._cached_sources
                # attach warning for downstream reporting
                warning_message = "Source refresh failed; using cached data."
                if warning_message not in sources.messages:
                    sources.messages.append(warning_message)
            else:
                self._cached_sources = sources
                self._last_source_refresh = sources.generated_at

        try:
            snapshot = await build_snapshot_from_sources(
                sources,
                progress_cb=progress_cb,
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
                self._exchange_interval = current_settings.exchange_refresh_seconds
                self._exchange_status = {
                    entry.get("exchange", f"exchange-{idx}"): entry
                    for idx, entry in enumerate(snapshot.exchange_status)
                }
        finally:
            async with self._lock:
                self._in_progress = False

        if force_accounts:
            try:
                await self._accounts.refresh_now(force_env=True)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Account refresh failed: %s", exc)

        return outcome

    async def on_settings_updated(self) -> None:
        async with self._lock:
            current = self._settings_manager.current
            self._parser_interval = current.parser_refresh_seconds
            self._exchange_interval = current.exchange_refresh_seconds
            self._account_interval = current.account_refresh_seconds
        await self._restart_scheduler()
        self._accounts.update_interval(self._account_interval)
        # Kick an async refresh so UI sees new cadence sooner.
        asyncio.create_task(self._accounts.refresh_now(force_env=True))

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
        exchange_interval = int(
            settings_payload.get("exchange_refresh_seconds", self._exchange_interval)
        )
        account_interval = int(
            settings_payload.get("account_refresh_seconds", self._account_interval)
        )
        return {
            "status": status,
            "refresh_interval": table_interval,
            "parser_refresh_interval": parser_interval,
            "exchange_refresh_interval": exchange_interval,
            "account_refresh_interval": account_interval,
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
            "accounts": self._account_state(),
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

    def _account_state(self) -> dict[str, object]:
        payload = self._accounts.snapshot()
        positions = payload.get("positions") or []
        payload["positions_by_symbol"] = self._positions_by_symbol(positions)
        return payload

    def _positions_by_symbol(self, positions: List[dict[str, Any]]) -> List[dict[str, Any]]:
        if not positions:
            return []
        market_lookup = self._market_snapshot_lookup()
        grouped: dict[str, dict[str, Any]] = {}
        for entry in positions:
            symbol_norm = entry.get("symbol_normalized") or normalize_symbol(entry.get("symbol"))
            if not symbol_norm:
                continue
            container = grouped.setdefault(
                symbol_norm,
                {"symbol": symbol_norm, "net_contracts": 0.0, "net_notional": 0.0, "legs": []},
            )
            side = (entry.get("side") or "").lower()
            contracts = float(entry.get("contracts") or 0.0)
            if side == "short":
                signed_contracts = -contracts
            else:
                signed_contracts = contracts
            container["net_contracts"] += signed_contracts
            notional = float(entry.get("notional") or 0.0)
            container["net_notional"] += notional if signed_contracts >= 0 else -notional
            key = (str(entry.get("exchange")).lower(), symbol_norm)
            snapshot = market_lookup.get(key)
            container["legs"].append(
                {
                    "exchange": entry.get("exchange"),
                    "side": side or None,
                    "contracts": contracts or None,
                    "notional": notional if notional else None,
                    "entry_price": entry.get("entry_price"),
                    "mark_price": entry.get("mark_price"),
                    "unrealized_pnl": entry.get("unrealized_pnl"),
                    "leverage": entry.get("leverage"),
                    "liquidation_price": entry.get("liquidation_price"),
                    "timestamp": entry.get("timestamp"),
                    "funding_rate": snapshot.funding_rate if snapshot else None,
                    "next_funding": (
                        snapshot.next_funding_time.isoformat()
                        if snapshot and snapshot.next_funding_time
                        else None
                    ),
                    "snapshot_mark": snapshot.mark_price if snapshot else None,
                }
            )
        for value in grouped.values():
            value["legs"].sort(key=lambda leg: (leg.get("side") or "", leg.get("exchange") or ""))
        return sorted(grouped.values(), key=lambda item: item["symbol"])

    def _market_snapshot_lookup(self) -> dict[tuple[str, str], MarketSnapshot]:
        if not self._snapshot or not self._snapshot.market_snapshots:
            return {}
        lookup: dict[tuple[str, str], MarketSnapshot] = {}
        for exchange, mapping in self._snapshot.market_snapshots.items():
            for snapshot in mapping.values():
                if isinstance(snapshot, MarketSnapshot):
                    key = (exchange.lower(), normalize_symbol(snapshot.symbol))
                    lookup[key] = snapshot
                elif isinstance(snapshot, dict):
                    symbol = snapshot.get("symbol")
                    funding = snapshot.get("funding_rate")
                    next_funding = snapshot.get("next_funding_time")
                    mark_price = snapshot.get("mark_price")
                    key = (exchange.lower(), normalize_symbol(symbol))
                    lookup[key] = MarketSnapshot(
                        exchange=exchange,
                        symbol=symbol or "",
                        exchange_symbol=snapshot.get("exchange_symbol") or "",
                        funding_rate=funding,
                        next_funding_time=(
                            datetime.fromisoformat(next_funding)
                            if isinstance(next_funding, str)
                            else None
                        ),
                        mark_price=mark_price,
                        bid=snapshot.get("bid"),
                        ask=snapshot.get("ask"),
                        raw={},
                        bid_size=snapshot.get("bid_size"),
                        ask_size=snapshot.get("ask_size"),
                        funding_interval_hours=snapshot.get("funding_interval_hours"),
                    )
        return lookup


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


def _fmt_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
