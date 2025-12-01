from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Protocol

from .config import RiskConfig


class StopAdapter(Protocol):
    name: str

    def cancel_position_stops(self, position_id: str) -> None: ...

    def set_stop_market(
        self,
        position_id: str,
        stop_price: float,
        quantity: float,
        reduce_only: bool = True,
        close_on_trigger: bool = True,
    ) -> None: ...


@dataclass(slots=True)
class StopAction:
    exchange: str
    position_id: str
    symbol: str
    side: Literal["long", "short"]
    stop_price: float
    quantity: float
    status: str
    message: str | None = None


class StopManager:
    """Calculates and updates fail-safe stop orders near liquidation price."""

    def __init__(self, config: RiskConfig, adapters: dict[str, StopAdapter]) -> None:
        self._config = config
        self._adapters = adapters

    def target_stop_price(
        self,
        side: Literal["long", "short"],
        liq_price: float | None,
    ) -> float | None:
        if liq_price is None or liq_price <= 0:
            return None
        gap = self._config.stop_gap_from_liq_pct
        if side == "long":
            return liq_price * (1.0 + gap)
        return liq_price * (1.0 - gap)

    def update_stops(
        self,
        positions: List[dict],
    ) -> List[StopAction]:
        """Best-effort routine to refresh stop-market orders for each position."""
        actions: List[StopAction] = []
        for pos in positions:
            exchange = str(pos.get("exchange") or "")
            adapter = self._adapters.get(exchange)
            if not adapter:
                continue
            side = (pos.get("side") or "").lower()
            liq_price = pos.get("liquidation_price") or pos.get("liquidationPrice")
            stop_price = self.target_stop_price(side, liq_price)
            if stop_price is None:
                actions.append(
                    StopAction(
                        exchange=exchange,
                        position_id=str(pos.get("position_id") or pos.get("positionId") or ""),
                        symbol=str(pos.get("symbol") or ""),
                        side="long" if side == "long" else "short",
                        stop_price=0.0,
                        quantity=float(pos.get("contracts") or pos.get("quantity") or 0.0),
                        status="skipped",
                        message="No liquidation price available",
                    )
                )
                continue
            qty = float(pos.get("contracts") or pos.get("quantity") or 0.0)
            pid = str(pos.get("position_id") or pos.get("positionId") or "")
            symbol = str(pos.get("symbol") or "")
            try:
                adapter.cancel_position_stops(pid)
                adapter.set_stop_market(
                    pid,
                    stop_price=stop_price,
                    quantity=abs(qty),
                    reduce_only=True,
                    close_on_trigger=True,
                )
                actions.append(
                    StopAction(
                        exchange=exchange,
                        position_id=pid,
                        symbol=symbol,
                        side="long" if side == "long" else "short",
                        stop_price=stop_price,
                        quantity=abs(qty),
                        status="placed",
                        message=None,
                    )
                )
            except Exception as exc:  # pragma: no cover - integration path
                actions.append(
                    StopAction(
                        exchange=exchange,
                        position_id=pid,
                        symbol=symbol,
                        side="long" if side == "long" else "short",
                        stop_price=stop_price,
                        quantity=abs(qty),
                        status="error",
                        message=str(exc),
                    )
                )
        return actions
