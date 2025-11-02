from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional

from config import STATE_DIR
from .storage import JsonStateStore
from .wallet import ReservationNotFoundError, WalletService

PositionStatus = Literal[
    "pending",
    "hedged",
    "observing",
    "exit_pending",
    "closed",
    "cancelled",
]

Side = Literal["long", "short"]


@dataclass
class PositionLegState:
    exchange: str
    side: Side
    reservation_id: str
    target_amount: float
    filled_amount: float = 0.0
    avg_price: float | None = None
    last_fill_at: float | None = None

    def record_fill(self, amount: float, price: float) -> None:
        if amount <= 0:
            raise ValueError("Fill amount must be positive")
        new_filled = self.filled_amount + amount
        if new_filled - self.target_amount > 1e-9:
            raise ValueError("Filled amount exceeds target")
        total_value = (self.avg_price or 0.0) * self.filled_amount + price * amount
        self.filled_amount = new_filled
        self.avg_price = total_value / self.filled_amount if self.filled_amount else price
        self.last_fill_at = time.time()

    @property
    def is_filled(self) -> bool:
        return self.filled_amount >= self.target_amount - 1e-9


@dataclass
class PositionState:
    position_id: str
    symbol: str
    strategy: str
    status: PositionStatus
    created_at: float
    updated_at: float
    legs: Dict[str, PositionLegState] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    hedged_at: float | None = None
    observation_started_at: float | None = None
    exit_started_at: float | None = None
    closed_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["legs"] = {key: asdict(value) for key, value in self.legs.items()}
        return payload


class PositionExistsError(RuntimeError):
    pass


class PositionNotFoundError(KeyError):
    pass


class InvalidPositionStateError(RuntimeError):
    pass


class PositionManager:
    """Track synthetic positions and orchestrate lifecycle transitions."""

    DEFAULT_STATE_PATH = STATE_DIR / "positions_state.json"

    def __init__(
        self,
        wallet: WalletService,
        *,
        state_path: Path | str | None = None,
    ) -> None:
        self._wallet = wallet
        self._store = JsonStateStore(state_path or self.DEFAULT_STATE_PATH)
        self._positions: Dict[str, PositionState] = {}
        self._active_by_symbol: Dict[str, str] = {}
        self._load_state()

    # ------------------------------------------------------------------ #
    # Creation and lifecycle
    # ------------------------------------------------------------------ #
    def create_position(
        self,
        *,
        symbol: str,
        strategy: str,
        long_reservation: str,
        short_reservation: str,
        long_exchange: str,
        short_exchange: str,
        notional: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> PositionState:
        if symbol in self._active_by_symbol:
            raise PositionExistsError(f"Symbol {symbol} already active")

        # Commit reservations before creating the position to avoid dangling funds.
        self._wallet.commit(long_reservation)
        self._wallet.commit(short_reservation)

        now = time.time()
        position = PositionState(
            position_id=str(uuid.uuid4()),
            symbol=symbol,
            strategy=strategy,
            status="pending",
            created_at=now,
            updated_at=now,
            metadata=dict(metadata or {}),
            legs={
                "long": PositionLegState(
                    exchange=long_exchange,
                    side="long",
                    reservation_id=long_reservation,
                    target_amount=notional,
                ),
                "short": PositionLegState(
                    exchange=short_exchange,
                    side="short",
                    reservation_id=short_reservation,
                    target_amount=notional,
                ),
            },
        )
        self._positions[position.position_id] = position
        self._active_by_symbol[symbol] = position.position_id
        self._persist()
        return position

    def record_fill(
        self,
        position_id: str,
        *,
        leg: Side,
        amount: float,
        price: float,
    ) -> PositionState:
        position = self._get_position(position_id)
        leg_state = position.legs[leg]
        leg_state.record_fill(amount, price)
        position.updated_at = time.time()
        self._persist()
        return position

    def mark_hedged(self, position_id: str) -> PositionState:
        position = self._get_position(position_id)
        if not all(item.is_filled for item in position.legs.values()):
            raise InvalidPositionStateError("Cannot hedge before both legs are filled")
        position.status = "hedged"
        position.hedged_at = time.time()
        position.updated_at = position.hedged_at
        self._persist()
        return position

    def start_observation(self, position_id: str) -> PositionState:
        position = self._get_position(position_id)
        if position.status not in ("hedged", "observing"):
            raise InvalidPositionStateError("Position must be hedged before observation")
        position.status = "observing"
        if position.observation_started_at is None:
            position.observation_started_at = time.time()
        position.updated_at = time.time()
        self._persist()
        return position

    def mark_exit_pending(self, position_id: str) -> PositionState:
        position = self._get_position(position_id)
        if position.status not in ("hedged", "observing"):
            raise InvalidPositionStateError("Position must be active before exit")
        position.status = "exit_pending"
        position.exit_started_at = time.time()
        position.updated_at = position.exit_started_at
        self._persist()
        return position

    def close_position(
        self, position_id: str, *, release_unfilled: bool = False
    ) -> PositionState:
        position = self._get_position(position_id)
        if position.status == "closed":
            return position

        for leg_state in position.legs.values():
            filled = leg_state.filled_amount if not release_unfilled else leg_state.target_amount
            if filled > 0:
                self._wallet.close_position(
                    leg_state.exchange,
                    amount=filled,
                )
            if leg_state.filled_amount < leg_state.target_amount and release_unfilled:
                try:
                    self._wallet.release(leg_state.reservation_id)
                except ReservationNotFoundError:
                    pass

        position.status = "closed"
        position.closed_at = time.time()
        position.updated_at = position.closed_at
        self._active_by_symbol.pop(position.symbol, None)
        self._persist()
        return position

    def cancel_position(self, position_id: str) -> PositionState:
        position = self._get_position(position_id)
        if position.status == "closed":
            raise InvalidPositionStateError("Cannot cancel a closed position")

        for leg_state in position.legs.values():
            try:
                self._wallet.release(leg_state.reservation_id)
            except ReservationNotFoundError:
                # May already be committed if cancellation happens late.
                pass

        position.status = "cancelled"
        position.updated_at = time.time()
        self._active_by_symbol.pop(position.symbol, None)
        self._persist()
        return position

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def get(self, position_id: str) -> PositionState:
        return self._get_position(position_id)

    def positions(self) -> Iterable[PositionState]:
        return self._positions.values()

    def active_positions(self) -> Iterable[PositionState]:
        return [
            position
            for position in self._positions.values()
            if position.status not in ("closed", "cancelled")
        ]

    def active_symbol(self, symbol: str) -> Optional[PositionState]:
        position_id = self._active_by_symbol.get(symbol)
        if not position_id:
            return None
        return self._positions[position_id]

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _load_state(self) -> None:
        payload = self._store.load(default=None)
        if not payload:
            return
        positions: Dict[str, PositionState] = {}
        for entry in payload.get("positions", []):
            legs = {
                key: PositionLegState(
                    exchange=value["exchange"],
                    side=value["side"],
                    reservation_id=value["reservation_id"],
                    target_amount=float(value["target_amount"]),
                    filled_amount=float(value.get("filled_amount", 0.0)),
                    avg_price=value.get("avg_price"),
                    last_fill_at=value.get("last_fill_at"),
                )
                for key, value in entry.get("legs", {}).items()
            }
            position = PositionState(
                position_id=entry["position_id"],
                symbol=entry["symbol"],
                strategy=entry.get("strategy", "unknown"),
                status=entry.get("status", "pending"),
                created_at=float(entry["created_at"]),
                updated_at=float(entry.get("updated_at", entry["created_at"])),
                metadata=entry.get("metadata", {}),
                hedged_at=entry.get("hedged_at"),
                observation_started_at=entry.get("observation_started_at"),
                exit_started_at=entry.get("exit_started_at"),
                closed_at=entry.get("closed_at"),
                legs=legs,
            )
            positions[position.position_id] = position
            if position.status not in ("closed", "cancelled"):
                self._active_by_symbol[position.symbol] = position.position_id
        self._positions = positions

    def _persist(self) -> None:
        payload = {"positions": [position.to_dict() for position in self._positions.values()]}
        self._store.save(payload)

    def _get_position(self, position_id: str) -> PositionState:
        try:
            return self._positions[position_id]
        except KeyError as exc:
            raise PositionNotFoundError(position_id) from exc
