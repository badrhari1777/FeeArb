from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from config import EXECUTION_SETTINGS_PATH, STATE_DIR


@dataclass(slots=True)
class AllocationBracket:
    """Allocation percentage for an expected edge interval."""

    min_edge: float  # inclusive
    max_edge: Optional[float]  # exclusive; None => no upper bound
    allocation_pct: float  # fraction of total working balance (0..1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_edge": self.min_edge,
            "max_edge": self.max_edge,
            "allocation_pct": self.allocation_pct,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "AllocationBracket":
        return cls(
            min_edge=float(payload.get("min_edge", 0.0)),
            max_edge=(
                float(payload["max_edge"]) if payload.get("max_edge") is not None else None
            ),
            allocation_pct=float(payload.get("allocation_pct", 0.1)),
        )


@dataclass(slots=True)
class BalanceSettings:
    initial_balances: Dict[str, float] = field(default_factory=lambda: {"bybit": 5000.0, "mexc": 4000.0})
    reserve_ratio: float = 0.30
    max_positions: int = 5
    max_symbols: int = 5
    symbol_cooldown_seconds: int = 600

    def to_dict(self) -> Dict[str, Any]:
        return {
            "initial_balances": dict(self.initial_balances),
            "reserve_ratio": self.reserve_ratio,
            "max_positions": self.max_positions,
            "max_symbols": self.max_symbols,
            "symbol_cooldown_seconds": self.symbol_cooldown_seconds,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "BalanceSettings":
        return cls(
            initial_balances={
                str(key): float(value)
                for key, value in (payload.get("initial_balances") or {}).items()
            }
            or {"bybit": 5000.0, "mexc": 4000.0},
            reserve_ratio=float(payload.get("reserve_ratio", 0.30)),
            max_positions=int(payload.get("max_positions", 5)),
            max_symbols=int(payload.get("max_symbols", 5)),
            symbol_cooldown_seconds=int(payload.get("symbol_cooldown_seconds", 600)),
        )


@dataclass(slots=True)
class ModeThresholds:
    base_spread_threshold: float = 0.0025  # 0.25%
    high_spread_threshold: float = 0.0050  # 0.50%
    extreme_spread_threshold: float = 0.0070  # 0.70%
    entry_window_seconds: int = 180  # default entry limit
    extended_entry_window_seconds: int = 900  # for high spread
    observation_window_seconds: int = 1800  # post-settlement monitoring window (30m)
    ladder_step_seconds: int = 10
    ladder_requote_offset: float = 0.0001  # 1 bp
    orphan_timeout_ms: int = 400
    slippage_bps_cap: float = 6.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_spread_threshold": self.base_spread_threshold,
            "high_spread_threshold": self.high_spread_threshold,
            "extreme_spread_threshold": self.extreme_spread_threshold,
            "entry_window_seconds": self.entry_window_seconds,
            "extended_entry_window_seconds": self.extended_entry_window_seconds,
            "observation_window_seconds": self.observation_window_seconds,
            "ladder_step_seconds": self.ladder_step_seconds,
            "ladder_requote_offset": self.ladder_requote_offset,
            "orphan_timeout_ms": self.orphan_timeout_ms,
            "slippage_bps_cap": self.slippage_bps_cap,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ModeThresholds":
        return cls(
            base_spread_threshold=float(payload.get("base_spread_threshold", 0.0025)),
            high_spread_threshold=float(payload.get("high_spread_threshold", 0.0050)),
            extreme_spread_threshold=float(payload.get("extreme_spread_threshold", 0.0070)),
            entry_window_seconds=int(payload.get("entry_window_seconds", 180)),
            extended_entry_window_seconds=int(
                payload.get("extended_entry_window_seconds", 900)
            ),
            observation_window_seconds=int(
                payload.get("observation_window_seconds", 1800)
            ),
            ladder_step_seconds=int(payload.get("ladder_step_seconds", 10)),
            ladder_requote_offset=float(payload.get("ladder_requote_offset", 0.0001)),
            orphan_timeout_ms=int(payload.get("orphan_timeout_ms", 400)),
            slippage_bps_cap=float(payload.get("slippage_bps_cap", 6.0)),
        )


@dataclass(slots=True)
class RiskSettings:
    base_leverage: float = 2.0
    max_leverage: float = 3.0
    min_lr_distance: float = 0.4
    max_orphan_failures: int = 3
    max_slippage_failures: int = 3
    kill_switch_loss_pct: float = -0.03  # cumulative session PnL threshold

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_leverage": self.base_leverage,
            "max_leverage": self.max_leverage,
            "min_lr_distance": self.min_lr_distance,
            "max_orphan_failures": self.max_orphan_failures,
            "max_slippage_failures": self.max_slippage_failures,
            "kill_switch_loss_pct": self.kill_switch_loss_pct,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "RiskSettings":
        return cls(
            base_leverage=float(payload.get("base_leverage", 2.0)),
            max_leverage=float(payload.get("max_leverage", 3.0)),
            min_lr_distance=float(payload.get("min_lr_distance", 0.4)),
            max_orphan_failures=int(payload.get("max_orphan_failures", 3)),
            max_slippage_failures=int(payload.get("max_slippage_failures", 3)),
            kill_switch_loss_pct=float(payload.get("kill_switch_loss_pct", -0.03)),
        )


@dataclass(slots=True)
class TelemetrySettings:
    event_log_path: Path = STATE_DIR / "events.log"
    structured_log_path: Path = STATE_DIR / "execution_events.jsonl"
    max_events_in_memory: int = 1000

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_log_path": str(self.event_log_path),
            "structured_log_path": str(self.structured_log_path),
            "max_events_in_memory": self.max_events_in_memory,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TelemetrySettings":
        return cls(
            event_log_path=Path(payload.get("event_log_path", STATE_DIR / "events.log")),
            structured_log_path=Path(
                payload.get("structured_log_path", STATE_DIR / "execution_events.jsonl")
            ),
            max_events_in_memory=int(payload.get("max_events_in_memory", 1000)),
        )


@dataclass(slots=True)
class ExecutionSettings:
    balance: BalanceSettings = field(default_factory=BalanceSettings)
    thresholds: ModeThresholds = field(default_factory=ModeThresholds)
    risk: RiskSettings = field(default_factory=RiskSettings)
    telemetry: TelemetrySettings = field(default_factory=TelemetrySettings)
    allocation_rules: List[AllocationBracket] = field(
        default_factory=lambda: [
            AllocationBracket(min_edge=0.0, max_edge=0.0030, allocation_pct=0.10),
            AllocationBracket(min_edge=0.0030, max_edge=0.0050, allocation_pct=0.18),
            AllocationBracket(min_edge=0.0050, max_edge=0.0075, allocation_pct=0.25),
            AllocationBracket(min_edge=0.0075, max_edge=None, allocation_pct=0.35),
        ]
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "balance": self.balance.to_dict(),
            "thresholds": self.thresholds.to_dict(),
            "risk": self.risk.to_dict(),
            "telemetry": self.telemetry.to_dict(),
            "allocation_rules": [rule.to_dict() for rule in self.allocation_rules],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any] | None) -> "ExecutionSettings":
        if not payload:
            return cls()
        allocation_payload = payload.get("allocation_rules") or []
        allocation_rules = (
            [AllocationBracket.from_dict(item) for item in allocation_payload]
            if allocation_payload
            else None
        )
        instance = cls(
            balance=BalanceSettings.from_dict(payload.get("balance") or {}),
            thresholds=ModeThresholds.from_dict(payload.get("thresholds") or {}),
            risk=RiskSettings.from_dict(payload.get("risk") or {}),
            telemetry=TelemetrySettings.from_dict(payload.get("telemetry") or {}),
            allocation_rules=allocation_rules or cls().allocation_rules,
        )
        return instance

    def with_updates(self, payload: Mapping[str, Any]) -> "ExecutionSettings":
        updated = replace(self)
        if "balance" in payload:
            updated.balance = BalanceSettings.from_dict(payload["balance"])
        if "thresholds" in payload:
            updated.thresholds = ModeThresholds.from_dict(payload["thresholds"])
        if "risk" in payload:
            updated.risk = RiskSettings.from_dict(payload["risk"])
        if "telemetry" in payload:
            updated.telemetry = TelemetrySettings.from_dict(payload["telemetry"])
        if "allocation_rules" in payload:
            updated.allocation_rules = [
                AllocationBracket.from_dict(item) for item in payload["allocation_rules"]
            ]
        return updated


class ExecutionSettingsManager:
    """Load/save runtime execution settings from JSON."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or EXECUTION_SETTINGS_PATH
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._settings = self._load()

    @property
    def current(self) -> ExecutionSettings:
        return self._settings

    def reload(self) -> ExecutionSettings:
        self._settings = self._load()
        return self._settings

    def as_dict(self) -> Dict[str, Any]:
        return self._settings.to_dict()

    def update(self, payload: Mapping[str, Any]) -> ExecutionSettings:
        candidate = self._settings.with_updates(payload)
        self._settings = candidate
        self.save()
        return self._settings

    def save(self) -> None:
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(self._settings.to_dict(), handle, indent=2)

    def _load(self) -> ExecutionSettings:
        if not self._path.exists():
            defaults = ExecutionSettings()
            self.save_default(defaults)
            return defaults
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return ExecutionSettings()
        return ExecutionSettings.from_dict(payload)

    def save_default(self, settings: ExecutionSettings) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(settings.to_dict(), handle, indent=2)


def allocation_for_edge(
    rules: Iterable[AllocationBracket],
    edge: float,
) -> float:
    """Return allocation fraction for the provided effective spread (edge)."""
    for rule in rules:
        upper = rule.max_edge if rule.max_edge is not None else float("inf")
        if rule.min_edge <= edge < upper:
            return rule.allocation_pct
    # Default fallback: smallest allocation.
    if rules:
        return min(rules, key=lambda item: item.allocation_pct).allocation_pct
    return 0.0
