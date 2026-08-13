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

    # Superseded decision products.  Their historical code and evidence stay
    # available during the staged refactor, but they have no recurring runtime
    # ownership and cannot place orders.
    auto_exit: bool = False
    auto_strategies: bool = False
    position_reduction: bool = False
    legacy_coin_analysis: bool = False
    legacy_candidate_discovery: bool = False

    def to_dict(self) -> dict[str, bool]:
        return asdict(self)


RUNTIME_MODULES = RuntimeModules()


__all__ = ["RUNTIME_MODULES", "RuntimeModules"]
