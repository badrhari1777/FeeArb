from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple

from config import STATE_DIR
from .positions import PositionManager, PositionState
from .settings import ExecutionSettings, allocation_for_edge
from .storage import JsonStateStore
from .wallet import Reservation, WalletService


@dataclass
class PreparedAllocation:
    allocation_id: str
    symbol: str
    long_exchange: str
    short_exchange: str
    edge: float
    notional: float
    long_reservation_id: str
    short_reservation_id: str
    created_at: float

    def to_dict(self) -> Dict[str, object]:
        return {
            "allocation_id": self.allocation_id,
            "symbol": self.symbol,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange,
            "edge": self.edge,
            "notional": self.notional,
            "long_reservation_id": self.long_reservation_id,
            "short_reservation_id": self.short_reservation_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PreparedAllocation":
        return cls(
            allocation_id=str(payload["allocation_id"]),
            symbol=str(payload["symbol"]),
            long_exchange=str(payload["long_exchange"]),
            short_exchange=str(payload["short_exchange"]),
            edge=float(payload["edge"]),
            notional=float(payload["notional"]),
            long_reservation_id=str(payload["long_reservation_id"]),
            short_reservation_id=str(payload["short_reservation_id"]),
            created_at=float(payload["created_at"]),
        )


class AllocationError(RuntimeError):
    """Base class for allocator failures."""


class SymbolLockedError(AllocationError):
    pass


class CooldownActiveError(AllocationError):
    pass


class AllocationLimitsExceeded(AllocationError):
    pass


class Allocator:
    """Coordinate wallet reservations and position creation."""

    DEFAULT_STATE_PATH = STATE_DIR / "allocator_state.json"

    def __init__(
        self,
        wallet: WalletService,
        positions: PositionManager,
        settings: ExecutionSettings,
        *,
        state_path: Path | None = None,
    ) -> None:
        self._wallet = wallet
        self._positions = positions
        self._settings = settings
        self._store = JsonStateStore(state_path or self.DEFAULT_STATE_PATH)
        self._pending: Dict[str, PreparedAllocation] = {}
        self._pending_by_symbol: Dict[str, str] = {}
        self._cooldown_until: Dict[str, float] = {}
        self._load_state()

    # ------------------------------------------------------------------ #
    # Allocation workflow
    # ------------------------------------------------------------------ #
    def prepare_allocation(
        self,
        *,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        expected_edge: float,
        min_notional: float = 100.0,
        reason: str = "execution",
    ) -> PreparedAllocation:
        now = time.time()
        symbol_upper = symbol.upper()
        self._assert_symbol_available(symbol_upper, now)
        self._assert_capacity()

        long_allowed = self._max_notional_for_exchange(long_exchange)
        short_allowed = self._max_notional_for_exchange(short_exchange)
        allocation_fraction = allocation_for_edge(
            self._settings.allocation_rules, expected_edge
        )
        target_long = min(long_allowed, long_allowed * allocation_fraction)
        target_short = min(short_allowed, short_allowed * allocation_fraction)
        notional = min(target_long, target_short)

        if notional < min_notional:
            raise AllocationLimitsExceeded(
                f"Notional {notional:.2f} below minimum {min_notional:.2f}"
            )

        long_res = self._wallet.reserve(
            long_exchange,
            amount=notional,
            symbol=symbol_upper,
            reason=f"{reason}:long",
        )
        try:
            short_res = self._wallet.reserve(
                short_exchange,
                amount=notional,
                symbol=symbol_upper,
                reason=f"{reason}:short",
            )
        except Exception:
            self._wallet.release(long_res.reservation_id)
            raise

        allocation = PreparedAllocation(
            allocation_id=str(uuid.uuid4()),
            symbol=symbol_upper,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            edge=expected_edge,
            notional=notional,
            long_reservation_id=long_res.reservation_id,
            short_reservation_id=short_res.reservation_id,
            created_at=now,
        )
        self._register_pending(allocation)
        self._persist()
        return allocation

    def cancel_allocation(self, allocation_id: str) -> None:
        allocation = self._pending.pop(allocation_id, None)
        if not allocation:
            return
        self._pending_by_symbol.pop(allocation.symbol, None)
        self._wallet.release(allocation.long_reservation_id)
        self._wallet.release(allocation.short_reservation_id)
        self._persist()

    def activate_allocation(
        self,
        allocation_id: str,
        *,
        strategy: str,
        metadata: Optional[Mapping[str, object]] = None,
    ) -> PositionState:
        allocation = self._pending.get(allocation_id)
        if not allocation:
            raise AllocationError(f"Unknown allocation id {allocation_id}")

        position = self._positions.create_position(
            symbol=allocation.symbol,
            strategy=strategy,
            long_reservation=allocation.long_reservation_id,
            short_reservation=allocation.short_reservation_id,
            long_exchange=allocation.long_exchange,
            short_exchange=allocation.short_exchange,
            notional=allocation.notional,
            metadata=dict(metadata or {}),
        )
        # Once position is created, remove pending buffer.
        self._pending.pop(allocation_id, None)
        self._pending_by_symbol.pop(allocation.symbol, None)
        self._persist()
        return position

    # ------------------------------------------------------------------ #
    # Cooldown tracking
    # ------------------------------------------------------------------ #
    def mark_symbol_closed(self, symbol: str, *, cooldown_seconds: Optional[int] = None) -> None:
        duration = cooldown_seconds or self._settings.balance.symbol_cooldown_seconds
        if duration <= 0:
            self._cooldown_until.pop(symbol.upper(), None)
        else:
            self._cooldown_until[symbol.upper()] = time.time() + duration
        self._persist()

    def cooldown_remaining(self, symbol: str, now: Optional[float] = None) -> float:
        now = now or time.time()
        expiry = self._cooldown_until.get(symbol.upper())
        if not expiry:
            return 0.0
        return max(0.0, expiry - now)

    # ------------------------------------------------------------------ #
    # Introspection helpers
    # ------------------------------------------------------------------ #
    def pending_allocations(self) -> Iterable[PreparedAllocation]:
        return list(self._pending.values())

    def is_symbol_locked(self, symbol: str) -> bool:
        symbol = symbol.upper()
        return symbol in self._pending_by_symbol or bool(
            self._positions.active_symbol(symbol)
        )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _assert_symbol_available(self, symbol: str, now: float) -> None:
        if self.is_symbol_locked(symbol):
            raise SymbolLockedError(f"Symbol {symbol} already active")
        cooldown = self.cooldown_remaining(symbol, now)
        if cooldown > 0:
            raise CooldownActiveError(f"Symbol {symbol} in cooldown for {cooldown:.0f}s")

    def _assert_capacity(self) -> None:
        max_positions = self._settings.balance.max_positions
        max_symbols = self._settings.balance.max_symbols
        active_positions = list(self._positions.active_positions())
        active_symbols = {position.symbol for position in active_positions}

        if len(active_positions) + len(self._pending) >= max_positions:
            raise AllocationLimitsExceeded("Maximum concurrent positions reached")
        if len(active_symbols) + len(self._pending_by_symbol) >= max_symbols:
            raise AllocationLimitsExceeded("Maximum active symbols reached")

    def _max_notional_for_exchange(self, exchange: str) -> float:
        account = self._wallet.get_account(exchange)
        reserve_ratio = self._settings.balance.reserve_ratio
        working_cap = max(0.0, account.total_balance * (1 - reserve_ratio))
        used = account.in_positions + account.reserved
        remaining_cap = max(0.0, working_cap - used)
        return min(account.available, remaining_cap)

    def estimate_notional(
        self,
        *,
        long_exchange: str,
        short_exchange: str,
        expected_edge: float,
    ) -> float:
        """Return the estimated notional available for the pair without reserving funds."""
        long_allowed = self._max_notional_for_exchange(long_exchange)
        short_allowed = self._max_notional_for_exchange(short_exchange)
        allocation_fraction = allocation_for_edge(self._settings.allocation_rules, expected_edge)
        target_long = long_allowed * allocation_fraction
        target_short = short_allowed * allocation_fraction
        return min(long_allowed, short_allowed, target_long, target_short)

    def _register_pending(self, allocation: PreparedAllocation) -> None:
        self._pending[allocation.allocation_id] = allocation
        self._pending_by_symbol[allocation.symbol] = allocation.allocation_id

    def _persist(self) -> None:
        payload = {
            "pending": [item.to_dict() for item in self._pending.values()],
            "cooldowns": {symbol: expiry for symbol, expiry in self._cooldown_until.items()},
        }
        self._store.save(payload)

    def _load_state(self) -> None:
        payload = self._store.load(default=None)
        if not payload:
            return
        pending = {}
        for raw in payload.get("pending", []):
            allocation = PreparedAllocation.from_dict(raw)
            # Only keep allocations whose reservations still exist.
            if self._wallet.get_reservation(allocation.long_reservation_id) and self._wallet.get_reservation(
                allocation.short_reservation_id
            ):
                pending[allocation.allocation_id] = allocation
                self._pending_by_symbol[allocation.symbol] = allocation.allocation_id
            else:
                if self._wallet.get_reservation(allocation.long_reservation_id):
                    self._wallet.release(allocation.long_reservation_id)
                if self._wallet.get_reservation(allocation.short_reservation_id):
                    self._wallet.release(allocation.short_reservation_id)
        self._pending = pending
        cooldowns = payload.get("cooldowns") or {}
        self._cooldown_until = {
            symbol.upper(): float(expiry) for symbol, expiry in cooldowns.items()
        }
