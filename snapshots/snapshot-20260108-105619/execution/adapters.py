from __future__ import annotations

import asyncio
import logging
from typing import Dict

from .fills import FillSimulator
from .orders import OrderFill, OrderRequest
from .telemetry import TelemetryClient

logger = logging.getLogger("execution.adapters")


class TradingClient:
    """Abstract async trading client."""

    def __init__(self, exchange: str) -> None:
        self.exchange = exchange

    async def submit(self, request: OrderRequest) -> OrderFill:
        raise NotImplementedError


class SimulatedTradingClient(TradingClient):
    """Placeholder trading client that simulates immediate fills."""

    def __init__(
        self,
        exchange: str,
        *,
        fill_simulator: FillSimulator | None = None,
        telemetry: TelemetryClient | None = None,
    ) -> None:
        super().__init__(exchange)
        self._simulator = fill_simulator
        self._telemetry = telemetry

    async def submit(self, request: OrderRequest) -> OrderFill:
        await asyncio.sleep(0.05)  # simulate network latency
        price = request.price or 0.0
        if self._telemetry:
            self._telemetry.emit(
                "order:simulated",
                {
                    "exchange": request.exchange,
                    "symbol": request.symbol,
                    "side": request.side,
                    "quantity": request.quantity,
                    "order_type": request.order_type,
                },
            )
        return OrderFill(
            exchange=request.exchange,
            symbol=request.symbol,
            side=request.side,
            requested_quantity=request.quantity,
            filled_quantity=request.quantity,
            average_price=price,
            status="filled",
            client_order_id=request.client_order_id,
        )


def build_simulated_clients(
    exchanges: Dict[str, str],
    *,
    fill_simulator: FillSimulator | None = None,
    telemetry: TelemetryClient | None = None,
) -> Dict[str, TradingClient]:
    return {
        name: SimulatedTradingClient(
            exchange=name,
            fill_simulator=fill_simulator,
            telemetry=telemetry,
        )
        for name in exchanges
    }

