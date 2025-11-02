from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Iterable

from pipeline import collect_snapshot_async

from .engine import ExecutionEngine
from .lifecycle import LifecycleController
from .market import MarketGateway
from .signals import SignalDecision, SignalEngine
from .telemetry import TelemetryClient

logger = logging.getLogger("execution.orchestrator")


class ExecutionOrchestrator:
    def __init__(
        self,
        signal_engine: SignalEngine,
        execution_engine: ExecutionEngine,
        lifecycle: LifecycleController,
        market_gateway: MarketGateway,
        telemetry: TelemetryClient | None = None,
        *,
        snapshot_fetcher: Callable[[], Awaitable] | None = None,
    ) -> None:
        self._signal_engine = signal_engine
        self._execution_engine = execution_engine
        self._lifecycle = lifecycle
        self._market_gateway = market_gateway
        self._telemetry = telemetry
        self._snapshot_fetcher = snapshot_fetcher or collect_snapshot_async
        self._lock = asyncio.Lock()

    async def run_once(self) -> None:
        async with self._lock:
            snapshot = await self._snapshot_fetcher()
            decisions = self._signal_engine.generate(snapshot.opportunities)
            if self._telemetry:
                self._telemetry.emit(
                    "orchestrator:decisions",
                    {
                        "count": len(decisions),
                    },
                )
            await self._execute_decisions(decisions)
            self._lifecycle.evaluate()

    async def run_forever(self, interval: float) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Orchestrator run failed: %s", exc)
                if self._telemetry:
                    self._telemetry.emit(
                        "orchestrator:error",
                        {"error": str(exc)},
                    )
            await asyncio.sleep(interval)

    async def _execute_decisions(self, decisions: Iterable[SignalDecision]) -> None:
        for decision in decisions:
            result = await self._execution_engine.execute_decision(decision)
            if self._telemetry:
                payload = {
                    "symbol": decision.symbol,
                    "mode": decision.mode,
                    "error": result.error,
                }
                self._telemetry.emit("orchestrator:execution", payload)

