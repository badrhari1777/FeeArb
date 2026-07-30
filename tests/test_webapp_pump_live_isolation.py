from __future__ import annotations

from importlib import import_module

from execution.pump_live import PUMP_LIVE_STATE_DIR

webapp_app = import_module("webapp.app")

def test_webapp_test_singleton_cannot_touch_real_pump_live_state() -> None:
    controller = webapp_app.bybit_pump_short_lab._pump_live

    assert controller.state_dir != PUMP_LIVE_STATE_DIR
    assert controller._background_monitor is False
    assert controller._thread is None
