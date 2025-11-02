from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional

from config import STATE_DIR
from .storage import JsonStateStore

ReservationStatus = Literal["active", "committed", "released"]


@dataclass
class WalletAccount:
    exchange: str
    total_balance: float
    available: float
    reserved: float
    in_positions: float


@dataclass
class Reservation:
    reservation_id: str
    exchange: str
    symbol: str
    amount: float
    reason: str
    status: ReservationStatus
    created_at: float
    updated_at: float


class InsufficientFundsError(RuntimeError):
    pass


class ReservationNotFoundError(KeyError):
    pass


class WalletService:
    """Track paper balances, reservations, and position allocations."""

    DEFAULT_STATE_PATH = STATE_DIR / "wallet_state.json"

    def __init__(
        self,
        initial_balances: Dict[str, float],
        *,
        state_path: Path | str | None = None,
    ) -> None:
        self._store = JsonStateStore(state_path or self.DEFAULT_STATE_PATH)
        self._accounts: Dict[str, WalletAccount] = {}
        self._reservations: Dict[str, Reservation] = {}
        self._load_state(initial_balances)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def accounts(self) -> Iterable[WalletAccount]:
        return self._accounts.values()

    def get_account(self, exchange: str) -> WalletAccount:
        try:
            return self._accounts[exchange]
        except KeyError as exc:
            raise KeyError(f"Unknown exchange '{exchange}'") from exc

    def reserve(
        self, exchange: str, *, amount: float, symbol: str, reason: str
    ) -> Reservation:
        account = self.get_account(exchange)
        if amount <= 0:
            raise ValueError("Reservation amount must be positive")
        if account.available < amount - 1e-9:
            raise InsufficientFundsError(
                f"Exchange {exchange} has only {account.available:.2f} available"
            )

        now = time.time()
        reservation = Reservation(
            reservation_id=str(uuid.uuid4()),
            exchange=exchange,
            symbol=symbol,
            amount=amount,
            reason=reason,
            status="active",
            created_at=now,
            updated_at=now,
        )

        account.available -= amount
        account.reserved += amount
        self._reservations[reservation.reservation_id] = reservation
        self._persist()
        return reservation

    def release(self, reservation_id: str) -> Reservation:
        reservation = self._get_reservation(reservation_id)
        if reservation.status == "released":
            return reservation

        account = self.get_account(reservation.exchange)
        account.available += reservation.amount
        account.reserved -= reservation.amount
        reservation.status = "released"
        reservation.updated_at = time.time()
        self._persist()
        return reservation

    def commit(self, reservation_id: str) -> Reservation:
        reservation = self._get_reservation(reservation_id)
        if reservation.status == "committed":
            return reservation
        if reservation.status == "released":
            raise RuntimeError("Cannot commit a released reservation")

        account = self.get_account(reservation.exchange)
        account.reserved -= reservation.amount
        account.in_positions += reservation.amount
        reservation.status = "committed"
        reservation.updated_at = time.time()
        self._persist()
        return reservation

    def close_position(self, exchange: str, *, amount: float) -> WalletAccount:
        account = self.get_account(exchange)
        if amount <= 0:
            raise ValueError("Amount must be positive")
        if account.in_positions < amount - 1e-9:
            raise InsufficientFundsError(
                f"Exchange {exchange} has only {account.in_positions:.2f} in positions"
            )

        account.in_positions -= amount
        account.available += amount
        self._persist()
        return account

    def adjust_total(self, exchange: str, *, delta: float) -> WalletAccount:
        account = self.get_account(exchange)
        new_total = account.total_balance + delta
        if new_total < 0:
            raise ValueError("Total balance cannot become negative")
        account.total_balance = new_total
        account.available += delta
        if account.available < 0:
            # Avoid floating point noise
            raise RuntimeError("Available balance would become negative")
        self._persist()
        return account

    def active_reservations(self, exchange: Optional[str] = None) -> List[Reservation]:
        result = [
            reservation
            for reservation in self._reservations.values()
            if reservation.status == "active"
        ]
        if exchange:
            result = [item for item in result if item.exchange == exchange]
        return sorted(result, key=lambda item: item.created_at)

    def committed_reservations(self) -> List[Reservation]:
        return [
            reservation
            for reservation in self._reservations.values()
            if reservation.status == "committed"
        ]

    def get_reservation(self, reservation_id: str) -> Reservation | None:
        return self._reservations.get(reservation_id)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _load_state(self, initial_balances: Dict[str, float]) -> None:
        payload = self._store.load(default=None)
        if not payload:
            for exchange, balance in initial_balances.items():
                self._accounts[exchange] = WalletAccount(
                    exchange=exchange,
                    total_balance=float(balance),
                    available=float(balance),
                    reserved=0.0,
                    in_positions=0.0,
                )
            self._persist()
            return

        accounts = {
            entry["exchange"]: WalletAccount(
                exchange=entry["exchange"],
                total_balance=float(entry["total_balance"]),
                available=float(entry["available"]),
                reserved=float(entry["reserved"]),
                in_positions=float(entry.get("in_positions", 0.0)),
            )
            for entry in payload.get("accounts", [])
        }
        self._accounts = accounts

        reservations = {}
        for entry in payload.get("reservations", []):
            reservation = Reservation(
                reservation_id=entry["reservation_id"],
                exchange=entry["exchange"],
                symbol=entry["symbol"],
                amount=float(entry["amount"]),
                reason=entry.get("reason", ""),
                status=entry.get("status", "active"),  # fall back for legacy state
                created_at=float(entry["created_at"]),
                updated_at=float(entry.get("updated_at", entry["created_at"])),
            )
            reservations[reservation.reservation_id] = reservation
        self._reservations = reservations

        # Merge missing exchanges from initial balances
        for exchange, balance in initial_balances.items():
            if exchange not in self._accounts:
                self._accounts[exchange] = WalletAccount(
                    exchange=exchange,
                    total_balance=float(balance),
                    available=float(balance),
                    reserved=0.0,
                    in_positions=0.0,
                )
        self._persist()

    def _persist(self) -> None:
        payload = {
            "accounts": [asdict(account) for account in self._accounts.values()],
            "reservations": [asdict(res) for res in self._reservations.values()],
        }
        self._store.save(payload)

    def _get_reservation(self, reservation_id: str) -> Reservation:
        try:
            return self._reservations[reservation_id]
        except KeyError as exc:
            raise ReservationNotFoundError(reservation_id) from exc
