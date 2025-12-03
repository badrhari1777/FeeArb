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
from execution.accounts import _safe_float
from execution.allocator import Allocator
from execution.lifecycle import LifecycleController
from execution.settings import ExecutionSettings
from execution.accounts import AccountMonitor, normalize_symbol
from risk.config import default_risk_config, RiskConfig
from risk.stop_manager import ProtectiveOrderManager
from utils import purge_expired
from utils.cache_db import get_or_fetch_funding_history
from exchanges import get_adapter, normalize_exchange_name

RefreshResult = Literal["completed", "in_progress", "failed"]

logger = logging.getLogger(__name__)
funding_logger = logging.getLogger("funding")
if not funding_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    funding_logger.addHandler(handler)
funding_logger.setLevel(logging.INFO)
funding_logger.propagate = False


def _dedupe_settle(symbol: str | None) -> str:
    """Trim duplicated settle suffixes like USDTUSDT -> USDT to align lookup keys."""
    if not symbol:
        return ""
    normalized = normalize_symbol(symbol)
    for suffix in ("USDT", "USDC", "USD"):
        double = suffix + suffix
        while normalized.endswith(double):
            normalized = normalized[: -len(suffix)]
    return normalized


def _strip_settle(symbol: str) -> str:
    """Remove a single settle suffix (USDT/USDC/USD) for cross-venue matching."""
    upper = symbol.upper()
    for suffix in ("USDT", "USDC", "USD"):
        if upper.endswith(suffix):
            return upper[: -len(suffix)]
    return upper


class DataService:
    def __init__(self, settings_manager: SettingsManager | None = None) -> None:
        self._settings_manager = settings_manager or SettingsManager()
        self._parser_interval = self._settings_manager.current.parser_refresh_seconds
        self._exchange_interval = self._settings_manager.current.exchange_refresh_seconds
        self._account_interval = self._settings_manager.current.account_refresh_seconds
        self._summary_interval = self._settings_manager.current.summary_refresh_seconds
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
        self._funding_cache: dict[tuple[str, str], tuple[float | None, str | None, float | None, float]] = {}
        self._exec_settings_manager = ExecutionSettingsManager()
        self._execution_settings: ExecutionSettings = self._exec_settings_manager.current
        self._wallet = WalletService(self._execution_settings.balance.initial_balances)
        self._positions = PositionManager(self._wallet)
        self._allocator = Allocator(self._wallet, self._positions, self._execution_settings)
        self._lifecycle = LifecycleController(self._execution_settings, self._positions, self._allocator)
        self._telemetry = TelemetryClient(self._execution_settings)
        self._telemetry_events: List[dict[str, Any]] = []
        self._telemetry.register_listener(self._handle_telemetry_event)
        self._accounts = AccountMonitor(
            refresh_interval=self._account_interval,
            summary_interval=self._summary_interval,
        )
        self._risk_config: RiskConfig = self._risk_config_from_settings()
        self._protective_manager = ProtectiveOrderManager(self._risk_config)
        self._last_protective: dict[tuple[str, str, str], dict[str, float | None]] = {}
        self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", 180)
        self._protective_task: Optional[asyncio.Task] = None

    def _extend_universe_with_positions(self, sources: SourceSnapshot) -> SourceSnapshot:
        """Include symbols from live positions so market snapshots stay fresh for the UI."""
        # Keep “universe” strictly for opportunity discovery; positions are handled separately.
        return sources


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
        await self._maybe_sync_protective_orders()
        if self._task is None:
            await self._restart_scheduler()
        if self._bootstrap_task is None or self._bootstrap_task.done():
            self._bootstrap_task = asyncio.create_task(self.refresh_markets())
        if self._protective_task is None:
            self._protective_task = asyncio.create_task(self._protective_scheduler())
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
        if self._protective_task:
            self._protective_task.cancel()
            try:
                await self._protective_task
            except asyncio.CancelledError:
                pass
            self._protective_task = None
        await self._telemetry.stop()
        await self._accounts.stop()

    async def _scheduler(self) -> None:
        try:
            while True:
                interval = max(self._exchange_interval, 1)
                await asyncio.sleep(interval)
                result = await self.refresh_markets(
                    force_sources=self._sources_due(),
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

    def _sources_due(self) -> bool:
        if self._cached_sources is None or self._last_source_refresh is None:
            return True
        age = datetime.now(timezone.utc) - self._last_source_refresh
        return age.total_seconds() >= max(self._parser_interval, 1)

    async def refresh_markets(self, *, force_sources: bool = True) -> RefreshResult:
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

        sources = self._extend_universe_with_positions(sources)
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

        return outcome

    async def on_settings_updated(self) -> None:
        async with self._lock:
            current = self._settings_manager.current
            self._parser_interval = current.parser_refresh_seconds
            self._exchange_interval = current.exchange_refresh_seconds
            self._account_interval = current.account_refresh_seconds
            self._summary_interval = current.summary_refresh_seconds
            self._risk_config = self._risk_config_from_settings()
            self._protective_manager.update_config(self._risk_config)
            self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", self._protective_interval)
        await self._restart_scheduler()
        await self._restart_protective_scheduler()
        self._accounts.update_interval(self._account_interval)
        self._accounts.update_summary_interval(self._summary_interval)
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
        summary_interval = int(
            settings_payload.get("summary_refresh_seconds", getattr(self, "_summary_interval", 1800))
        )
        return {
            "status": status,
            "refresh_interval": table_interval,
            "parser_refresh_interval": parser_interval,
            "exchange_refresh_interval": exchange_interval,
            "account_refresh_interval": account_interval,
            "summary_refresh_interval": summary_interval,
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

    def _reduction_candidates(
        self,
        grouped_positions: dict[str, list[dict[str, Any]]],
        balances: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not grouped_positions or not balances:
            return []
        risky: dict[str, dict[str, Any]] = {}
        for bal in balances:
            exchange = str(bal.get("exchange") or "").lower()
            if not exchange:
                continue
            total = _safe_float(bal.get("total"))
            available = _safe_float(bal.get("available"))
            margin_ratio = _safe_float(bal.get("margin_ratio"))
            if total is None:
                continue
            min_buffer = max(0.15 * total, 500)
            stress = False
            reason_bits = []
            if margin_ratio is not None and margin_ratio >= 0.8:
                stress = True
                reason_bits.append(f"margin_ratio={margin_ratio}")
            if available is not None and available < min_buffer:
                stress = True
                reason_bits.append(f"available={available}<{int(min_buffer)}")
            if stress:
                risky[exchange] = {
                    "margin_ratio": margin_ratio,
                    "available": available,
                    "reason": "; ".join(reason_bits) or "low_buffer",
                }
        if not risky:
            return []

        candidates: list[dict[str, Any]] = []
        for symbol, legs in grouped_positions.items():
            longs = [leg for leg in legs if leg.get("side") == "long"]
            shorts = [leg for leg in legs if leg.get("side") == "short"]
            for leg in legs:
                ex = str(leg.get("exchange") or "").lower()
                if ex not in risky:
                    continue
                opposite_pool = shorts if leg.get("side") == "long" else longs
                if not opposite_pool:
                    continue
                # Pick the largest opposite leg to pair against.
                opposite = max(opposite_pool, key=lambda item: abs(item.get("quantity") or 0.0))
                qty = abs(leg.get("quantity") or 0.0)
                opp_qty = abs(opposite.get("quantity") or 0.0)
                if qty <= 0 or opp_qty <= 0:
                    continue
                suggested_close = round(min(qty, opp_qty) * 0.25, 6)
                funding = leg.get("funding_rate")
                funding_cost = None
                if funding is not None:
                    funding_cost = funding if leg.get("side") == "long" else -funding
                reason = f"{risky[ex]['reason']}"
                if funding_cost is not None and funding_cost > 0:
                    reason += f"; funding_cost~{round(funding_cost*100,4)}%/int"
                candidates.append(
                    {
                        "exchange": leg.get("exchange"),
                        "symbol": symbol,
                        "side": leg.get("side"),
                        "quantity": qty,
                        "close_quantity": suggested_close,
                        "paired_exchange": opposite.get("exchange"),
                        "funding_rate": funding,
                        "margin_ratio": risky[ex].get("margin_ratio"),
                        "reason": reason,
                    }
                )

        return sorted(
            candidates,
            key=lambda item: (
                -(item.get("margin_ratio") or 0.0),
                -(item.get("funding_rate") or 0.0),
                -item.get("quantity", 0.0),
            ),
        )

    def _account_state(self) -> dict[str, object]:
        payload = self._accounts.snapshot()
        positions = payload.get("positions") or []
        balances = self._sanitize_balances(payload.get("balances") or [])
        payload["balances"] = balances
        market_lookup = self._market_snapshot_lookup()
        positions_by_symbol, grouped = self._positions_by_symbol(
            positions,
            return_grouped=True,
            market_lookup=market_lookup,
        )
        payload["positions_by_symbol"] = positions_by_symbol
        payload["reduction_candidates"] = self._reduction_candidates(grouped, balances)
        return payload

    @staticmethod
    def _sanitize_balances(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def _num(val: Any) -> float | None:
            try:
                return float(val)
            except Exception:  # pylint: disable=broad-except
                return None

        cleaned: list[dict[str, Any]] = []
        for row in rows:
            row = dict(row)
            for key in ("total", "available", "used", "margin_ratio", "equity", "buffer_pct", "initial_margin", "maintenance_margin"):
                val = row.get(key)
                row[key] = _num(val)
            cleaned.append(row)
        return cleaned

    def _positions_by_symbol(
        self,
        positions: List[dict[str, Any]],
        return_grouped: bool = False,
        market_lookup: Optional[dict[tuple[str, str], MarketSnapshot]] = None,
    ) -> tuple[List[dict[str, Any]], dict[str, list[dict[str, Any]]]] | List[dict[str, Any]]:
        if not positions:
            return ([], {}) if return_grouped else []
        market_lookup = market_lookup or {}
        grouped: dict[str, dict[str, Any]] = {}
        for entry in positions:
            symbol_norm = _dedupe_settle(
                entry.get("symbol_normalized") or normalize_symbol(entry.get("symbol"))
            )
            if not symbol_norm:
                continue
            lookup_symbols = [symbol_norm]
            base_symbol = _strip_settle(symbol_norm)
            if base_symbol and base_symbol not in lookup_symbols:
                lookup_symbols.append(base_symbol)
            container = grouped.setdefault(symbol_norm, {"symbol": symbol_norm, "legs": []})
            side = (entry.get("side") or "").lower()
            contracts = float(entry.get("contracts") or 0.0)
            contract_size = float(entry.get("contract_size") or 1.0)
            coin_qty = float(entry.get("coin_qty") or contracts * contract_size)
            funding_rate = None
            next_funding_iso = None
            signed_coin = -coin_qty if side == "short" else coin_qty
            notional = float(entry.get("notional") or 0.0)
            snapshot = None
            for sym in lookup_symbols:
                key = (str(entry.get("exchange")).lower(), sym)
                snapshot = market_lookup.get(key)
                if snapshot:
                    break
            entry_price = entry.get("entry_price")
            mark_price = entry.get("mark_price")
            unrealized = entry.get("unrealized_pnl")
            if snapshot:
                funding_rate = snapshot.funding_rate
                next_funding_iso = (
                    snapshot.next_funding_time.isoformat()
                    if snapshot.next_funding_time
                    else None
                )
                if mark_price is None and snapshot.mark_price is not None:
                    mark_price = snapshot.mark_price
            live_rate, live_next, live_mark = self._funding_live(
                entry.get("exchange"),
                entry.get("symbol"),
                symbol_norm,
                entry.get("exchange_symbol"),
            )
            if live_rate is not None:
                funding_rate = live_rate
            if live_next is not None:
                next_funding_iso = live_next
            if live_mark is not None:
                mark_price = live_mark
            if (
                unrealized is None
                and entry_price is not None
                and mark_price is not None
            ):
                try:
                    unrealized = (float(mark_price) - float(entry_price)) * signed_coin
                except Exception:  # pylint: disable=broad-except
                    unrealized = entry.get("unrealized_pnl")
            next_funding_eta = None
            if next_funding_iso:
                try:
                    nf_dt = datetime.fromisoformat(next_funding_iso)
                    delta = nf_dt - datetime.now(timezone.utc)
                    if delta.total_seconds() > 0:
                        hours, remainder = divmod(int(delta.total_seconds()), 3600)
                        minutes = remainder // 60
                        next_funding_eta = f"{hours}h {minutes:02d}m"
                    else:
                        next_funding_eta = "passed"
                except Exception:  # pylint: disable=broad-except
                    next_funding_eta = None
            # Drop non-numeric funding artifacts (e.g., stray strings)
            try:
                if funding_rate is not None:
                    funding_rate = float(funding_rate)
            except Exception:  # pylint: disable=broad-except
                funding_rate = None
            if mark_price is None and entry_price is not None:
                # Fallback to entry so we at least display and compute PnL as 0.
                mark_price = entry_price
            dist_to_liq_pct = None
            liq_price = entry.get("liquidation_price")
            if liq_price is not None and mark_price not in (None, 0):
                try:
                    dist_to_liq_pct = abs(float(liq_price) - float(mark_price)) / abs(float(mark_price)) * 100.0
                except Exception:  # pylint: disable=broad-except
                    dist_to_liq_pct = None
            stop_price = self._target_stop_price(side, liq_price, mark_price=mark_price, entry_price=entry_price)
            container["legs"].append(
                {
                    "exchange": entry.get("exchange"),
                    "side": side or None,
                    "quantity": signed_coin,
                    "amount": abs(notional) if notional else None,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": unrealized,
                    "funding_rate": funding_rate,
                    "next_funding": next_funding_iso,
                    "next_funding_eta": next_funding_eta,
                    "leverage": entry.get("leverage"),
                    "liquidation_price": entry.get("liquidation_price"),
                    "margin_mode": entry.get("margin_mode"),
                    "margin_used": entry.get("margin_used"),
                    "dist_to_liq_pct": dist_to_liq_pct,
                    "stop_price": stop_price,
                    "take_price": None,
                    "expected_funding": (
                        (
                            (funding_rate or 0.0)
                            * (abs(notional) if notional else 0.0)
                            * (-1.0 if side == "long" else 1.0)
                        )
                        if funding_rate is not None and notional
                        else None
                    ),
                }
            )

        rows: list[dict[str, Any]] = []
        grouped_simple: dict[str, list[dict[str, Any]]] = {}
        for symbol, data in sorted(grouped.items(), key=lambda item: item[0]):
            legs = sorted(data["legs"], key=lambda leg: (leg.get("exchange") or ""))
            grouped_simple[symbol] = legs
            # Derive mirrored take/stop with spread consideration for two-legged pairs.
            if len(legs) == 2:
                long_leg = next((l for l in legs if l.get("side") == "long"), None)
                short_leg = next((l for l in legs if l.get("side") == "short"), None)
                if long_leg and short_leg:
                    long_stop = self._target_stop_price(
                        "long",
                        long_leg.get("liquidation_price"),
                        mark_price=_safe_float(long_leg.get("mark_price")),
                        entry_price=_safe_float(long_leg.get("entry_price")),
                    )
                    short_stop = self._target_stop_price(
                        "short",
                        short_leg.get("liquidation_price"),
                        mark_price=_safe_float(short_leg.get("mark_price")),
                        entry_price=_safe_float(short_leg.get("entry_price")),
                    )
                    # Spread-aware mirror: convert stop across exchanges via mark ratio.
                    lm = _safe_float(long_leg.get("mark_price"))
                    sm = _safe_float(short_leg.get("mark_price"))
                    long_to_short_ratio = (sm / lm) if lm and sm else 1.0
                    short_to_long_ratio = (lm / sm) if lm and sm else 1.0
                    long_take = short_stop * short_to_long_ratio if short_stop is not None else None
                    short_take = long_stop * long_to_short_ratio if long_stop is not None else None
                    # Apply throttling: only update if change > threshold_pct.
                    threshold = getattr(self._risk_config, "stop_requote_threshold_pct", 0.005)
                    for leg, stop_target, take_target in (
                        (long_leg, long_stop, long_take),
                        (short_leg, short_stop, short_take),
                    ):
                        key = (
                            str(leg.get("exchange") or ""),
                            str(leg.get("symbol") or ""),
                            str(leg.get("side") or ""),
                        )
                        last = self._last_protective.get(key, {})
                        def _should_update(prev: float | None, new: float | None) -> tuple[bool, float | None]:
                            if new is None:
                                return False, prev
                            if prev is None or prev <= 0:
                                return True, new
                            try:
                                delta = abs(new - prev) / prev
                            except Exception:
                                delta = 1.0
                            if delta >= threshold:
                                return True, new
                            return False, prev
                        update_stop, stop_val = _should_update(last.get("stop"), stop_target)
                        update_take, take_val = _should_update(last.get("take"), take_target)
                        if update_stop or update_take:
                            self._last_protective[key] = {
                                "stop": stop_val,
                                "take": take_val,
                            }
                        leg["stop_price"] = stop_val
                        leg["take_price"] = take_val
            rows.extend(
                [
                    {
                        "type": "leg",
                        "symbol": symbol,
                        **leg,
                    }
                    for leg in legs
                ]
            )
            summary = self._summarize_symbol(symbol, legs)
            if summary:
                rows.append(summary)
        if return_grouped:
            return rows, grouped_simple
        return rows

    def _funding_live(
        self,
        exchange: str | None,
        position_symbol: str | None,
        normalized_symbol: str,
        raw_exchange_symbol: str | None = None,
    ) -> tuple[float | None, str | None, float | None]:
        if not exchange:
            funding_logger.warning("funding failed exchange=? symbol=%s reason=no_exchange", normalized_symbol)
            return None, None, None
        try:
            adapter = get_adapter(normalize_exchange_name(exchange))
        except KeyError:
            funding_logger.warning(
                "funding failed exchange=%s symbol=%s reason=adapter_missing",
                exchange,
                normalized_symbol,
            )
            return None, None, None
        exchange_symbol = None

        canonical_symbol = _dedupe_settle(normalized_symbol)
        for suffix in ("UMCBL", "DMCBL", "SWAP", "PERP"):
            if canonical_symbol.endswith(suffix):
                canonical_symbol = canonical_symbol[: -len(suffix)]
                break

        candidates = [
            raw_exchange_symbol or "",
            position_symbol or "",
            canonical_symbol,
            normalized_symbol,
        ]
        for cand in candidates:
            if not cand:
                continue
            cand = _dedupe_settle(str(cand))
            mapped = None
            try:
                mapped = adapter.map_symbol(str(cand))
            except Exception:  # pylint: disable=broad-except
                mapped = None
            if mapped:
                # If mapping only adds duplicated suffixes, keep the original.
                if mapped.replace("_", "").replace("-", "") == cand.replace("_", "").replace("-", ""):
                    exchange_symbol = cand
                else:
                    exchange_symbol = mapped
                break
        if not exchange_symbol:
            exchange_symbol = _dedupe_settle(position_symbol or raw_exchange_symbol or normalized_symbol)

        key = (normalize_exchange_name(exchange), exchange_symbol or canonical_symbol)
        now_ts = datetime.now(tz=timezone.utc).timestamp()

        logger.info(
            "funding fetch start exchange=%s key=%s canonical=%s candidates=%s",
            exchange,
            key,
            canonical_symbol,
            candidates,
        )

        # Try live snapshot first for freshest funding; fallback to cached history (<=2m), then ccxt.
        try:
            snapshots = adapter.fetch_market_snapshots([canonical_symbol])
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Market snapshot fetch failed for %s %s: %s", exchange, canonical_symbol, exc)
            snapshots = []

        if snapshots:
            snap = snapshots[0]
            rate = getattr(snap, "funding_rate", None)
            next_time = getattr(snap, "next_funding_time", None)
            next_funding_iso = next_time.isoformat() if next_time else None
            mark_val = getattr(snap, "mark_price", None)
            if rate is not None or next_funding_iso is not None or mark_val is not None:
                funding_logger.info(
                    "funding ok source=snapshot exchange=%s symbol=%s rate=%s next=%s mark=%s",
                    exchange,
                    canonical_symbol,
                    rate,
                    next_funding_iso,
                    mark_val,
                )
                self._funding_cache[key] = (rate, next_funding_iso, mark_val, now_ts)
                return rate, next_funding_iso, mark_val

        def _fetch() -> list[dict]:
            if hasattr(adapter, "funding_history"):
                try:
                    # Pass canonical symbol; adapters will map appropriately.
                    return adapter.funding_history(canonical_symbol, limit=50)  # type: ignore[attr-defined]
                except Exception:  # pylint: disable=broad-except
                    return []
            return []

        history = get_or_fetch_funding_history(
            normalize_exchange_name(exchange),
            exchange_symbol,
            _fetch,
            max_age_seconds=120,
            limit=5,
        )
        if history:
            latest = next((item for item in history if item.get("rate") is not None), None)
            if latest is None:
                history = []
            else:
                rate = latest.get("rate")
                ts_ms = latest.get("ts_ms") or latest.get("timestamp")
                interval_hours = latest.get("interval_hours") or 8.0
                next_funding_iso = None
                mark_val = latest.get("mark_price")
                if ts_ms and isinstance(ts_ms, (int, float)) and ts_ms > 0:
                    try:
                        ts_ms = int(ts_ms)
                        next_ms = ts_ms + int((interval_hours or 8.0) * 3600 * 1000)
                        next_funding_iso = datetime.fromtimestamp(next_ms / 1000, tz=timezone.utc).isoformat()
                    except Exception:  # pylint: disable=broad-except
                        next_funding_iso = None
                self._funding_cache[key] = (rate, next_funding_iso, mark_val, now_ts)
                funding_logger.info(
                    "funding ok source=history exchange=%s symbol=%s rate=%s next=%s mark=%s",
                    exchange,
                    exchange_symbol,
                    rate,
                    next_funding_iso,
                    mark_val,
                )
                return rate, next_funding_iso, mark_val
        else:
            logger.debug("Funding history empty for %s %s", exchange, exchange_symbol)

        # Additional fallback for Bitget: use ccxt funding rate directly if history/snapshot failed.
        if normalize_exchange_name(exchange) == "bitget":
            try:
                import ccxt  # type: ignore

                client = ccxt.bitget({"options": {"defaultType": "swap"}})
                mapped = adapter.map_symbol(canonical_symbol) or canonical_symbol
                # Load markets to get consistent ids for exotic symbols.
                try:
                    client.load_markets()
                except Exception:  # pylint: disable=broad-except
                    pass
                # ccxt expects pair format SYMBOL/USDT:USDT; fall back to mapped contract and raw market ids.
                try_symbols = [
                    f"{canonical_symbol}/USDT:USDT",
                    mapped,
                    f"{canonical_symbol}USDT_UMCBL",
                    f"{canonical_symbol}USD_DMCBL",
                ]
                funding = None
                last_exc: Exception | None = None
                for cand in try_symbols:
                    if not cand:
                        continue
                    try:
                        funding = client.fetch_funding_rate(cand)
                        break
                    except Exception as exc:  # pylint: disable=broad-except
                        last_exc = exc
                        continue
                if funding:
                    rate = _safe_float(funding.get("fundingRate"))
                    next_ts = funding.get("fundingTimestamp")
                    next_iso = None
                    try:
                        if next_ts:
                            next_iso = datetime.fromtimestamp(float(next_ts) / 1000, tz=timezone.utc).isoformat()
                    except Exception:  # pylint: disable=broad-except
                        next_iso = None
                    mark_val = _safe_float(
                        funding.get("markPrice")
                        or funding.get("indexPrice")
                        or funding.get("mark")
                    )
                    funding_logger.info(
                        "funding ok source=ccxt exchange=%s symbol=%s rate=%s next=%s mark=%s",
                        exchange,
                        canonical_symbol,
                        rate,
                        next_iso,
                        mark_val,
                    )
                    self._funding_cache[key] = (rate, next_iso, mark_val, now_ts)
                    return rate, next_iso, mark_val
                if last_exc:
                    logger.debug("Bitget ccxt fallback attempts failed for %s: %s", canonical_symbol, last_exc)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Bitget ccxt fallback failed for %s: %s", canonical_symbol, exc)

        funding_logger.warning("funding failed exchange=%s symbol=%s reason=unavailable", exchange, canonical_symbol)
        return None, None, None

    def _summarize_symbol(self, symbol: str, legs: List[dict[str, Any]]) -> dict[str, Any] | None:
        if not legs:
            return None
        long_legs = [leg for leg in legs if (leg.get("side") or "").lower() == "long"]
        short_legs = [leg for leg in legs if (leg.get("side") or "").lower() == "short"]

        def _weighted_avg(items: List[dict[str, Any]], key: str, weight_key: str = "amount") -> float | None:
            total_w = 0.0
            total_v = 0.0
            for item in items:
                val = item.get(key)
                weight_raw = item.get(weight_key) or 0.0
                weight = abs(weight_raw) if weight_key == "quantity" else weight_raw
                if val is None:
                    continue
                total_w += weight
                total_v += float(val) * float(weight)
            if total_w <= 0:
                return None
            return total_v / total_w

        # Use coin quantities (absolute) to weight price averages across venues.
        long_entry = _weighted_avg(long_legs, "entry_price", weight_key="quantity")
        short_entry = _weighted_avg(short_legs, "entry_price", weight_key="quantity")
        long_mark = _weighted_avg(long_legs, "mark_price", weight_key="quantity")
        short_mark = _weighted_avg(short_legs, "mark_price", weight_key="quantity")
        long_funding = _weighted_avg(long_legs, "funding_rate", weight_key="quantity")
        short_funding = _weighted_avg(short_legs, "funding_rate", weight_key="quantity")

        def _spread_pct(a: float | None, b: float | None) -> float | None:
            if a is None or b is None or b == 0:
                return None
            return (a - b) / b * 100.0

        entry_diff_pct = _spread_pct(long_entry, short_entry)
        mark_diff_pct = _spread_pct(long_mark, short_mark)
        funding_spread = None
        if long_funding is not None and short_funding is not None:
            funding_spread = short_funding - long_funding

        net_quantity = sum(leg.get("quantity") or 0.0 for leg in legs)
        pnl_total = sum(leg.get("unrealized_pnl") or 0.0 for leg in legs)

        soonest_next = None
        for leg in legs:
            ts = leg.get("next_funding")
            if not ts:
                continue
            try:
                candidate = datetime.fromisoformat(ts)
            except Exception:  # pylint: disable=broad-except
                continue
            if soonest_next is None or candidate < soonest_next:
                soonest_next = candidate

        expected_total = None
        for leg in legs:
            val = leg.get("expected_funding")
            if val is None:
                continue
            expected_total = (expected_total or 0.0) + float(val)

        return {
            "type": "summary",
            "symbol": symbol,
            "exchange": "TOTAL",
            "quantity": net_quantity,
            "amount": None,
            "entry_price": entry_diff_pct,
            "mark_price": mark_diff_pct,
            "unrealized_pnl": pnl_total,
            "funding_rate": funding_spread,
            "expected_funding": expected_total,
            "next_funding": soonest_next.isoformat() if soonest_next else None,
            "long_entry_avg": long_entry,
            "short_entry_avg": short_entry,
            "long_mark_avg": long_mark,
            "short_mark_avg": short_mark,
        }

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

    def _target_stop_price(
        self,
        side: str | None,
        liq_price: float | None,
        *,
        mark_price: float | None = None,
        entry_price: float | None = None,
    ) -> float | None:
        """Compute protective stop from liquidation; fallback if liq is missing/zero."""
        base_liq = None
        if liq_price is not None and liq_price > 0:
            base_liq = liq_price
        else:
            fallback = mark_price or entry_price
            if fallback is None or fallback <= 0:
                return None
            # Heuristic: if liq missing, place far from current price to avoid zero/invalid triggers.
            base_liq = fallback * (
                getattr(self._risk_config, "fallback_liq_factor_long", 0.33)
                if side == "long"
                else getattr(self._risk_config, "fallback_liq_factor_short", 1.66)
            )
        try:
            gap = float(self._risk_config.stop_gap_from_liq_pct)
        except Exception:
            gap = 0.07
        if gap <= 0:
            return None
        if side == "short":
            return base_liq * max(0.0001, (1.0 - gap))
        return base_liq * (1.0 + gap)

    def _risk_config_from_settings(self) -> RiskConfig:
        settings = self._settings_manager.current
        protective = getattr(settings, "protective", {}) or {}
        cfg = default_risk_config()
        try:
            cfg.stop_gap_from_liq_pct = float(protective.get("stop_gap_from_liq_pct", cfg.stop_gap_from_liq_pct))
            cfg.stop_requote_threshold_pct = float(
                protective.get("stop_requote_threshold_pct", cfg.stop_requote_threshold_pct)
            )
            cfg.fallback_liq_factor_long = float(
                protective.get("fallback_liq_factor_long", cfg.fallback_liq_factor_long)
            )
            cfg.fallback_liq_factor_short = float(
                protective.get("fallback_liq_factor_short", cfg.fallback_liq_factor_short)
            )
            cfg.target_safe_buffer_pct = float(
                protective.get("target_safe_buffer_pct", cfg.target_safe_buffer_pct)
            )
            cfg.warning_buffer_pct = float(protective.get("warning_buffer_pct", cfg.warning_buffer_pct))
            cfg.panic_buffer_pct = float(protective.get("panic_buffer_pct", cfg.panic_buffer_pct))
            cfg.min_free_balance_abs = float(protective.get("min_free_balance_abs", cfg.min_free_balance_abs))
            cfg.min_free_balance_rel = float(protective.get("min_free_balance_rel", cfg.min_free_balance_rel))
            cfg.balance_check_interval_sec = int(
                protective.get("balance_check_interval_sec", cfg.balance_check_interval_sec)
            )
            cfg.position_check_interval_sec = int(
                protective.get("position_check_interval_sec", cfg.position_check_interval_sec)
            )
            cfg.panic_close_batch_size = int(
                protective.get("panic_close_batch_size", cfg.panic_close_batch_size)
            )
            cfg.telegram_alert_chat_id = str(protective.get("telegram_alert_chat_id", cfg.telegram_alert_chat_id))
        except Exception:
            pass
        return cfg

    async def _maybe_sync_protective_orders(self) -> None:
        """Best-effort protective order sync if enabled in settings."""
        settings = self._settings_manager.current
        protective = getattr(settings, "protective", {}) or {}
        auto_protect = bool(protective.get("auto_protect_enabled", True))
        auto_take = bool(protective.get("auto_take_enabled", True))
        if not auto_protect and not auto_take:
            return
        snapshot = self._accounts.snapshot()
        positions = snapshot.get("positions") or []
        anti_orphan = bool(protective.get("anti_orphan_enabled", False))
        try:
            actions = await self._protective_manager.sync_protective_orders(
                positions,
                anti_orphan_enabled=anti_orphan,
            )
            if actions:
                summary = {
                    "message": "Protective orders synced",
                    "count": len(actions),
                    "updated": sum(1 for a in actions if a.get("status") == "updated"),
                    "unchanged": sum(1 for a in actions if a.get("status") == "unchanged"),
                    "timeout": sum(1 for a in actions if a.get("status") == "timeout"),
                    "error": sum(1 for a in actions if a.get("status") == "error"),
                }
                # Build a human-readable per-symbol summary.
                per_symbol: dict[str, list[str]] = {}
                for action in actions:
                    sym = str(action.get("symbol") or "").upper()
                    exch = str(action.get("exchange") or "")
                    status = action.get("status")
                    stop_val = action.get("target_stop")
                    take_val = action.get("target_take")
                    reason = action.get("reason") or action.get("error")
                    parts = [f"{exch}: {status}"]
                    if stop_val is not None:
                        parts.append(f"sl={stop_val}")
                    if take_val is not None:
                        parts.append(f"tp={take_val}")
                    if reason:
                        parts.append(f"reason={reason}")
                    per_symbol.setdefault(sym, []).append(", ".join(parts))
                summary["details"] = {k: v for k, v in per_symbol.items()}
                self._record_event("protective:sync", summary)
                # Emit compact overall status instead of per-leg spam.
                failures = [a for a in actions if a.get("status") not in ("updated", "unchanged")]
                if failures:
                    logger.warning(
                        "protective sync issues: %s",
                        "; ".join(
                            f"{f.get('exchange')} {f.get('symbol')} status={f.get('status')} err={f.get('error') or f.get('reason')}"
                            for f in failures
                        ),
                    )
                else:
                    logger.info("protective sync ok: all stops/takes placed")
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Protective sync failed: %s", exc)

    async def refresh_snapshot(self, *, force_accounts: bool = False) -> RefreshResult:
        """Compatibility wrapper used by the HTTP API."""
        if force_accounts:
            await self._accounts.refresh_now(force_env=True)
        return await self.refresh_markets(force_sources=True)


def _fmt_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
