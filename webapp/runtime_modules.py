"""Authoritative production-module boundary for the FeeArb web runtime.

The flags in this module describe product ownership, not operator-tunable
trading settings.  Retired decision engines must not be possible to re-enable
through ``data/settings.json`` or a stale browser form.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class RuntimeModules:
    """Modules that are allowed to own recurring production work."""

    account_monitor: bool = True
    positions_market: bool = True
    protective_orders: bool = True
    manual_execution: bool = True
    auto_arb_grid: bool = True
    pump_live: bool = True
    strategy_lab_observatory: bool = True

    def to_dict(self) -> dict[str, bool]:
        return asdict(self)


RUNTIME_MODULES = RuntimeModules()


__all__ = ["RUNTIME_MODULES", "RuntimeModules"]
