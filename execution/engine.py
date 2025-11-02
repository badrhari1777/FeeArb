from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from orchestrator.models import FundingOpportunity

from .adapters import TradingClient
from .allocator import AllocationError, Allocator, PreparedAllocation
from .fills import FillSimulator
from .market import MarketGateway
from .orders import OrderRequest, OrderFill
from .positions import PositionManager, PositionState
from .risk import RiskGuard
from .settings import ExecutionSettings
from .signals import SignalDecision
from .telemetry import TelemetryClient

logger = logging.getLogger("execution.engine")


@dataclass(slots=True)
class ExecutionResult:
    decision: SignalDecision
    position: Optional[PositionState]
    error: Optional[str] = None


class BaseStrategy:
    name = "mode_a"

    async def execute(
        self,
        decision: SignalDecision,
        allocation: PreparedAllocation,
        engine: "ExecutionEngine",
    ) -> PositionState:
        position = engine.allocator.activate_allocation(
            allocation.allocation_id,
            strategy=self.name,
            metadata={
                "mode": self.name,
                "edge": decision.expected_edge,
                "notional": allocation.notional,
            },
        )

        metrics_long = engine.market.latest_metrics(decision.long_exchange, decision.symbol)
        metrics_short = engine.market.latest_metrics(decision.short_exchange, decision.symbol)
        opportunity = decision.opportunity

        price_long = _select_price(opportunity, metrics_long, leg="long")
        price_short = _select_price(opportunity, metrics_short, leg="short")
        if engine.simulator:
            quote = engine.simulator.simulate(decision, allocation, metrics_long, metrics_short)
            price_long = quote.long_price or price_long
            price_short = quote.short_price or price_short
            if engine._risk_guard:
                if quote.orphan_time_ms > engine._settings.thresholds.orphan_timeout_ms:
                    engine._risk_guard.record_orphan_failure()
                if quote.raw_slippage_bps > engine._settings.thresholds.slippage_bps_cap:
                    engine._risk_guard.record_slippage_failure()

        amount = allocation.notional

        long_fill = await engine.submit_order(
            OrderRequest(
                exchange=decision.long_exchange,
                symbol=decision.symbol,
                side="buy",
                quantity=amount,
                order_type="market",
                price=price_long,
            )
        )
        short_fill = await engine.submit_order(
            OrderRequest(
                exchange=decision.short_exchange,
                symbol=decision.symbol,
                side="sell",
                quantity=amount,
                order_type="market",
                price=price_short,
            )
        )

        engine.positions.record_fill(
            position.position_id,
            leg="long",
            amount=long_fill.filled_quantity,
            price=long_fill.average_price,
        )
        engine.positions.record_fill(
            position.position_id,
            leg="short",
            amount=short_fill.filled_quantity,
            price=short_fill.average_price,
        )
        engine.positions.mark_hedged(position.position_id)
        engine.positions.start_observation(position.position_id)
        return engine.positions.get(position.position_id)


class ModeAStrategy(BaseStrategy):
    name = "mode_a"


class ModeBStrategy(BaseStrategy):
    name = "mode_b"


class ModeCStrategy(BaseStrategy):
    name = "mode_c"


class ExecutionEngine:
    def __init__(
        self,
        settings: ExecutionSettings,
        allocator: Allocator,
        positions: PositionManager,
        market: MarketGateway,
        risk_guard: RiskGuard | None = None,
        telemetry: TelemetryClient | None = None,
        simulator: FillSimulator | None = None,
        trading_clients: Dict[str, TradingClient] | None = None,
    ) -> None:
        self._settings = settings
        self._allocator = allocator
        self._positions = positions
        self._market = market
        self._risk_guard = risk_guard
        self._telemetry = telemetry
        self._simulator = simulator
        self._strategies: Dict[str, BaseStrategy] = {
            "mode_a": ModeAStrategy(),
            "mode_b": ModeBStrategy(),
            "mode_c": ModeCStrategy(),
        }
        self._trading_clients: Dict[str, TradingClient] = trading_clients or {}
        self._lock = asyncio.Lock()

    @property
    def allocator(self) -> Allocator:
        return self._allocator

    @property
    def positions(self) -> PositionManager:
        return self._positions

    @property
    def market(self) -> MarketGateway:
        return self._market

    @property
    def simulator(self) -> FillSimulator | None:
        return self._simulator

    def register_trading_client(self, exchange: str, client: TradingClient) -> None:
        self._trading_clients[exchange] = client

    def get_trading_client(self, exchange: str) -> TradingClient:
        try:
            return self._trading_clients[exchange]
        except KeyError as exc:
            raise RuntimeError(f"No trading client registered for {exchange}") from exc

    async def submit_order(self, request: OrderRequest) -> OrderFill:
        client = self.get_trading_client(request.exchange)
        if self._telemetry:
            self._telemetry.emit(
                "order:submit",
                {
                    "exchange": request.exchange,
                    "symbol": request.symbol,
                    "side": request.side,
                    "quantity": request.quantity,
                    "order_type": request.order_type,
                },
            )
        fill = await client.submit(request)
        if self._telemetry:
            self._telemetry.emit(
                "order:fill",
                {
                    "exchange": fill.exchange,
                    "symbol": fill.symbol,
                    "side": fill.side,
                    "quantity": fill.filled_quantity,
                    "avg_price": fill.average_price,
                    "status": fill.status,
                },
            )
        return fill

    async def execute_decision(self, decision: SignalDecision) -> ExecutionResult:
        async with self._lock:
            if self._risk_guard:
                allowed, reason = self._risk_guard.can_allocate(decision.symbol, decision.notional)
                if not allowed:
                    logger.warning("Risk guard blocked allocation for %s: %s", decision.symbol, reason)
                    return ExecutionResult(decision=decision, position=None, error=reason)
            try:
                allocation = self._allocator.prepare_allocation(
                    symbol=decision.symbol,
                    long_exchange=decision.long_exchange,
                    short_exchange=decision.short_exchange,
                    expected_edge=decision.expected_edge,
                    min_notional=decision.notional,
                    reason=decision.mode,
                )
                self._emit(
                    "allocation_prepared",
                    {
                        "symbol": decision.symbol,
                        "mode": decision.mode,
                        "notional": allocation.notional,
                    },
                )
            except AllocationError as exc:
                logger.warning("Allocation failed for %s: %s", decision.symbol, exc)
                self._emit(
                    "allocation_failed",
                    {"symbol": decision.symbol, "error": str(exc)},
                )
                return ExecutionResult(decision=decision, position=None, error=str(exc))

        strategy = self._strategies.get(decision.mode, ModeAStrategy())
        try:
            position = await strategy.execute(decision, allocation, self)
            logger.info(
                "Executed %s with %s -> position %s", decision.symbol, strategy.name, position.position_id
            )
            self._emit(
                "execution_success",
                {
                    "symbol": decision.symbol,
                    "mode": strategy.name,
                    "position_id": position.position_id,
                    "notional": allocation.notional,
                },
            )
            return ExecutionResult(decision=decision, position=position)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Strategy %s failed for %s", strategy.name, decision.symbol)
            self._allocator.cancel_allocation(allocation.allocation_id)
            self._emit(
                "execution_failed",
                {
                    "symbol": decision.symbol,
                    "mode": strategy.name,
                    "error": str(exc),
                },
            )
            return ExecutionResult(decision=decision, position=None, error=str(exc))

    def register_strategy(self, name: str, strategy: BaseStrategy) -> None:
        self._strategies[name] = strategy

    def _emit(self, event: str, payload: dict[str, object]) -> None:
        if self._telemetry:
            self._telemetry.emit(event, payload)


def _select_price(
    opportunity: FundingOpportunity | None,
    metrics,
    *,
    leg: str,
) -> float:
    if opportunity:
        if leg == "long":
            for candidate in (
                opportunity.long_ask,
                opportunity.long_mark,
                opportunity.short_mark,
            ):
                if candidate:
                    return float(candidate)
        else:
            for candidate in (
                opportunity.short_bid,
                opportunity.short_mark,
                opportunity.long_mark,
            ):
                if candidate:
                    return float(candidate)
    if metrics and metrics.mid_price:
        return float(metrics.mid_price)
    return 0.0
