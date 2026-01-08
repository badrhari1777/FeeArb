from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from .allocator import Allocator
from .settings import ExecutionSettings
from .telemetry import TelemetryClient
from .wallet import WalletService


@dataclass(slots=True)
class RiskStatus:
    paused: bool
    orphan_failures: int
    slippage_failures: int
    session_pnl: float
    reason: Optional[str] = None


class RiskGuard:
    def __init__(
        self,
        settings: ExecutionSettings,
        allocator: Allocator,
        wallet: WalletService,
        telemetry: TelemetryClient | None = None,
    ) -> None:
        self._settings = settings
        self._allocator = allocator
        self._wallet = wallet
        self._paused = False
        self._pause_reason: Optional[str] = None
        self._orphan_failures = 0
        self._slippage_failures = 0
        self._session_pnl = 0.0
        self._telemetry = telemetry

    def can_allocate(self, symbol: str, notional: float) -> tuple[bool, Optional[str]]:
        if self._paused:
            return False, self._pause_reason

        risk = self._settings.risk
        for account in self._wallet.accounts():
            leverage = notional / account.total_balance if account.total_balance else 0.0
            if leverage > risk.max_leverage:
                reason = f"leverage_limit_exceeded:{account.exchange}"
                self._emit("risk:blocked", {"symbol": symbol, "reason": reason})
                return False, reason

        if self._orphan_failures >= risk.max_orphan_failures:
            reason = "orphan_failures"
            self._emit("risk:blocked", {"symbol": symbol, "reason": reason})
            return False, reason
        if self._slippage_failures >= risk.max_slippage_failures:
            reason = "slippage_failures"
            self._emit("risk:blocked", {"symbol": symbol, "reason": reason})
            return False, reason
        if self._session_pnl <= risk.kill_switch_loss_pct:
            reason = "kill_switch"
            self._emit("risk:blocked", {"symbol": symbol, "reason": reason})
            return False, reason

        return True, None

    def record_orphan_failure(self) -> None:
        self._orphan_failures += 1
        if self._orphan_failures >= self._settings.risk.max_orphan_failures:
            self._paused = True
            self._pause_reason = "orphan_failures"
            self._emit("risk:pause", {"reason": "orphan_failures"})

    def record_slippage_failure(self) -> None:
        self._slippage_failures += 1
        if self._slippage_failures >= self._settings.risk.max_slippage_failures:
            self._paused = True
            self._pause_reason = "slippage_failures"
            self._emit("risk:pause", {"reason": "slippage_failures"})

    def record_pnl(self, delta: float) -> None:
        self._session_pnl += delta
        if self._session_pnl <= self._settings.risk.kill_switch_loss_pct:
            self._paused = True
            self._pause_reason = "kill_switch"
            self._emit("risk:pause", {"reason": "kill_switch", "session_pnl": self._session_pnl})

    def resume(self) -> None:
        self._paused = False
        self._pause_reason = None
        self._orphan_failures = 0
        self._slippage_failures = 0
        self._session_pnl = 0.0
        self._emit("risk:resume", {})

    def status(self) -> RiskStatus:
        return RiskStatus(
            paused=self._paused,
            orphan_failures=self._orphan_failures,
            slippage_failures=self._slippage_failures,
            session_pnl=self._session_pnl,
            reason=self._pause_reason,
        )

    def _emit(self, event: str, payload: dict[str, object]) -> None:
        if self._telemetry:
            self._telemetry.emit(event, payload)
