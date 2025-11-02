"""Execution layer services for funding arbitrage strategies."""

from .adapters import SimulatedTradingClient, TradingClient
from .engine import ExecutionEngine
from .fills import FillSimulator, FillQuote
from .lifecycle import LifecycleController
from .orders import OrderFill, OrderRequest
from .positions import PositionManager
from .settings import (
    AllocationBracket,
    ExecutionSettings,
    ExecutionSettingsManager,
    allocation_for_edge,
)
from .telemetry import TelemetryClient
from .wallet import WalletService

__all__ = [
    "WalletService",
    "PositionManager",
    "LifecycleController",
    "ExecutionEngine",
    "AllocationBracket",
    "ExecutionSettings",
    "ExecutionSettingsManager",
    "allocation_for_edge",
    "FillSimulator",
    "FillQuote",
    "TradingClient",
    "SimulatedTradingClient",
    "OrderRequest",
    "OrderFill",
    "TelemetryClient",
]
