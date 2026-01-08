from __future__ import annotations

import time
from typing import List, Optional

from .allocator import Allocator
from .positions import PositionManager, PositionState
from .settings import ExecutionSettings


class LifecycleController:
    def __init__(
        self,
        settings: ExecutionSettings,
        positions: PositionManager,
        allocator: Allocator,
    ) -> None:
        self._settings = settings
        self._positions = positions
        self._allocator = allocator

    def evaluate(self, now: Optional[float] = None) -> List[PositionState]:
        now = now or time.time()
        pending_exit: List[PositionState] = []
        window = self._settings.thresholds.observation_window_seconds
        for position in self._positions.active_positions():
            if position.status in ("hedged", "observing"):
                start = position.observation_started_at or position.hedged_at
                if start and now - start >= window:
                    updated = self._positions.mark_exit_pending(position.position_id)
                    pending_exit.append(updated)
        return pending_exit

    def close(self, position_id: str, *, release_unfilled: bool = False) -> PositionState:
        position = self._positions.close_position(position_id, release_unfilled=release_unfilled)
        self._allocator.mark_symbol_closed(position.symbol)
        return position
