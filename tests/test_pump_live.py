from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from execution.pump_live import (
    ARM_CONFIRMATION,
    BybitPumpLiveGateway,
    PumpLiveConfig,
    PumpLiveController,
    build_live_legs,
)


class FakePumpGateway:
    def __init__(self) -> None:
        self.balance = {"total": 1_000.0, "available": 1_000.0, "used": 0.0}
        self.positions: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.take_profits: list[tuple[str, float]] = []
        self.protections: list[tuple[str, float, float]] = []
        self.margin_adds: list[tuple[str, float]] = []
        self.margin_removes: list[tuple[str, float]] = []
        self.leverage_calls: list[tuple[str, float]] = []
        self.canceled: list[str] = []
        self.fail_market = False
        self.liq_after_add: float | None = None
        self.liq_after_remove: float | None = None
        self.balance_failures: list[Exception] = []
        self.preflight_existing_state_errors = False

    def credentials_status(self) -> dict[str, Any]:
        return {
            "env_file_exists": True,
            "api_key_present": True,
            "api_secret_present": True,
            "sub_uid_present": True,
            "ready": True,
            "testnet": False,
        }

    def preflight(self, config: PumpLiveConfig) -> dict[str, Any]:
        del config
        errors: list[str] = []
        if self.preflight_existing_state_errors:
            if self.positions:
                errors.append("pump_live_subaccount_has_existing_positions")
            if any(not bool(item.get("reduce_only")) for item in self.orders):
                errors.append("pump_live_subaccount_has_unknown_open_orders")
        return {
            "ready": not errors,
            "checked_at_ms": int(time.time() * 1000),
            "credentials": self.credentials_status(),
            "account": {
                "margin_mode": "ISOLATED_MARGIN",
                "total_usdt": self.balance["total"],
                "available_usdt": self.balance["available"],
                "positions": len(self.positions),
                "open_orders": len(self.orders),
            },
            "errors": errors,
            "warnings": ["api_key_has_no_ip_binding_dynamic_ip_mode"],
        }

    def prepare_account(self) -> dict[str, Any]:
        return {"status": "prepared"}

    def fetch_balance(self) -> dict[str, Any]:
        if self.balance_failures:
            raise self.balance_failures.pop(0)
        return dict(self.balance)

    def fetch_positions(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self.positions]

    def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in self.orders
            if item.get("status") == "open" and (symbol is None or item.get("symbol") == symbol)
        ]

    def fetch_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        del symbol
        return next(
            (dict(item) for item in self.orders if item.get("id") == order_id),
            {"id": order_id, "status": "unknown"},
        )

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        del symbol
        return {"last": 10.0, "bid": 9.99, "ask": 10.01}

    def set_leverage(self, symbol: str, leverage: float) -> None:
        self.leverage_calls.append((symbol, leverage))

    def guarded_market_order(
        self,
        *,
        symbol: str,
        side: str,
        notional_usd: float | None,
        qty: float | None,
        reduce_only: bool,
        order_link_id: str,
        max_slippage_bps: float,
    ) -> dict[str, Any]:
        del max_slippage_bps
        if self.fail_market:
            raise RuntimeError("simulated_market_timeout")
        if side == "sell":
            fill_qty = float(notional_usd or 0.0) / 10.0
            self.positions = [item for item in self.positions if item.get("symbol") != symbol]
            self.positions.append(
                {
                    "symbol": symbol,
                    "side": "short",
                    "qty": fill_qty,
                    "avg_price": 10.0,
                    "mark_price": 10.0,
                    "liq_price": 15.0,
                    "leverage": 3.0,
                    "margin_mode": "isolated",
                    "position_idx": 0,
                    "unrealized_pnl": 0.0,
                }
            )
        elif reduce_only:
            fill_qty = float(qty or 0.0)
            self.positions = [item for item in self.positions if item.get("symbol") != symbol]
        else:  # pragma: no cover - defensive
            raise AssertionError("unexpected fake market order")
        return {
            "id": f"market-{len(self.orders) + 1}",
            "order_link_id": order_link_id,
            "status": "closed",
            "filled": fill_qty,
            "average": 10.0,
        }

    def create_ladder_order(
        self,
        *,
        symbol: str,
        notional_usd: float,
        price: float,
        order_link_id: str,
    ) -> dict[str, Any]:
        order = {
            "id": f"ladder-{len(self.orders) + 1}",
            "order_link_id": order_link_id,
            "symbol": symbol,
            "status": "open",
            "filled": 0.0,
            "average": None,
            "price": price,
            "notional_usd": notional_usd,
        }
        self.orders.append(order)
        return dict(order)

    def cancel_order(self, order_id: str, symbol: str) -> None:
        del symbol
        self.canceled.append(order_id)
        for order in self.orders:
            if order.get("id") == order_id:
                order["status"] = "canceled"

    def set_full_protection(
        self,
        symbol: str,
        *,
        take_profit_price: float,
        stop_loss_price: float,
    ) -> dict[str, Any]:
        self.take_profits.append((symbol, take_profit_price))
        self.protections.append((symbol, take_profit_price, stop_loss_price))
        return {"status": "ok"}

    def add_margin(self, symbol: str, amount_usd: float) -> dict[str, Any]:
        self.margin_adds.append((symbol, amount_usd))
        self.balance["available"] -= amount_usd
        if self.liq_after_add is not None:
            for position in self.positions:
                if position.get("symbol") == symbol:
                    position["liq_price"] = self.liq_after_add
        return {"status": "ok"}

    def remove_margin(self, symbol: str, amount_usd: float) -> dict[str, Any]:
        self.margin_removes.append((symbol, amount_usd))
        self.balance["available"] += amount_usd
        if self.liq_after_remove is not None:
            for position in self.positions:
                if position.get("symbol") == symbol:
                    position["liq_price"] = self.liq_after_remove
        return {"status": "ok"}


class FakePumpNotifier:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[tuple[str, str]] = []

    async def send_text_status(self, text: str, *, title: str | None = None) -> str:
        if self.fail:
            raise RuntimeError("simulated_notification_failure")
        self.messages.append((str(title or ""), text))
        return "ok"


def write_env(path: Path, *, entry_cap: int = 1) -> None:
    path.write_text(
        "\n".join(
            [
                "BYBIT_PUMP_API_KEY=fake",
                "BYBIT_PUMP_API_SECRET=fake",
                "BYBIT_PUMP_SUB_UID=123",
                f"PUMP_LIVE_ENTRY_CAP={entry_cap}",
                "PUMP_LIVE_POLL_INTERVAL_SEC=15",
                "PUMP_LIVE_MAX_SLIPPAGE_BPS=50",
            ]
        ),
        encoding="utf-8",
    )


def ready_decision(
    ts_ms: int,
    *,
    symbol: str = "TESTUSDT",
    event_id: str = "test-event",
) -> dict[str, Any]:
    return {
        "strategy_id": "main_pullback_tier",
        "symbol": symbol,
        "event_id": event_id,
        "state": "entry_ready",
        "reason": "strategy_conditions_met",
        "ts_ms": ts_ms,
        "last_close": 10.0,
        "pump_pct": 120.0,
        "tier": {
            "rule_slug": "step50_legs3_tapered_tp25_336",
            "ladder_step_pct": 50.0,
            "ladder_legs": 3,
            "leg_weights": [1.0, 2.0, 3.0],
            "tp_pct": 25.0,
            "max_hold_h": 336,
        },
    }


def test_1000_plan_has_four_175_slots_and_orders_clear_bybit_minimum() -> None:
    config = PumpLiveConfig()
    assert config.slot_margin_usd == 175.0
    legs = build_live_legs(
        tier={
            "ladder_legs": 3,
            "ladder_step_pct": 50.0,
            "leg_weights": [1.0, 2.0, 3.0],
        },
        slot_margin_usd=config.slot_margin_usd,
        leverage=config.leverage,
        reference_price=10.0,
    )
    assert round(sum(item["margin_usd"] for item in legs), 6) == 175.0
    assert min(item["notional_usd"] for item in legs) == 87.5
    assert min(item["notional_usd"] for item in legs) > 5.0


def test_arm_ignores_old_signal_and_opens_new_main_signal(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed = controller.arm(ARM_CONFIRMATION)
    armed_at = int(armed["armed_at_ms"])
    assert controller.submit_decisions([ready_decision(armed_at - 1)])["accepted"] == 0
    assert controller.submit_decisions([ready_decision(armed_at + 1)])["accepted"] == 1

    status = controller.run_cycle()
    open_items = [item for item in status["positions"] if item["status"] != "closed"]
    assert len(open_items) == 1
    assert open_items[0]["symbol"] == "TESTUSDT"
    assert len(open_items[0]["legs"]) == 3
    assert len(gateway.orders) == 2
    assert gateway.leverage_calls == [("TESTUSDT", 3.0)]
    assert gateway.take_profits[-1] == ("TESTUSDT", 7.5)
    assert gateway.protections[-1] == ("TESTUSDT", 7.5, 14.625)
    assert open_items[0]["stop_price"] == 14.625


def test_flat_position_needs_two_cycles_then_cancels_ladder(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions = []

    first = controller.run_cycle()
    assert first["open_positions"] == 1
    assert len(gateway.canceled) == 2
    assert first["entry_armed"] is False
    assert first["blocked_reason"] == "position_absent_unconfirmed"
    second = controller.run_cycle()
    assert second["open_positions"] == 0
    assert len(gateway.canceled) == 2


def test_restart_disarms_entries_but_keeps_recovery_monitor_state(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "live_state.json").write_text(
        json.dumps(
            {
                "schema": "pump_live_state_v1",
                "status": "armed",
                "monitor_enabled": True,
                "entry_armed": True,
                "positions": [
                    {
                        "live_id": "x",
                        "symbol": "TESTUSDT",
                        "status": "open",
                        "legs": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    status = controller.status()
    assert status["entry_armed"] is False
    assert status["monitor_enabled"] is True
    assert status["status"] == "recovery_monitoring"


def test_new_entry_blocks_when_available_would_break_reserve(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.balance["available"] = 474.99
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    status = controller.run_cycle()
    assert status["open_positions"] == 0
    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "available_balance_below_new_slot_guard"


def test_liquidation_buffer_adds_margin_from_reserved_cash(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0

    status = controller.run_cycle()

    assert gateway.margin_adds == [("TESTUSDT", 25.0)]
    assert status["positions"][0]["margin_topup_usd"] == 25.0


def test_critical_buffer_bypasses_topup_cooldown_and_closes(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    controller._state["positions"][0]["last_topup_at_ms"] = int(time.time() * 1000)  # pylint: disable=protected-access
    gateway.balance["available"] = 25.0
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 14.0

    status = controller.run_cycle()

    assert gateway.positions == []
    assert status["positions"][0]["status"] == "closing"
    assert status["positions"][0]["close_reason"] == "emergency_liq_buffer"


def test_topup_is_immediately_verified_and_closes_if_buffer_stays_critical(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 14.0

    status = controller.run_cycle()

    assert gateway.margin_adds == [("TESTUSDT", 50.0)]
    assert gateway.positions == []
    assert status["positions"][0]["close_reason"] == "emergency_buffer_after_topup"


def test_only_bot_added_margin_is_removed_after_safe_hysteresis(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0
    controller.run_cycle()
    assert controller.status()["positions"][0]["margin_topup_usd"] == 25.0

    controller._state["positions"][0]["last_topup_at_ms"] = (  # pylint: disable=protected-access
        int(time.time() * 1000) - 1_900_000
    )
    gateway.positions[0]["mark_price"] = 10.0
    gateway.positions[0]["liq_price"] = 15.0
    controller.run_cycle()
    assert gateway.margin_removes == []

    status = controller.run_cycle()

    assert gateway.margin_removes == [("TESTUSDT", 25.0)]
    assert status["positions"][0]["margin_topup_usd"] == 0.0
    assert status["positions"][0]["liq_buffer_pct"] == 50.0


def test_unsafe_margin_reduction_is_rolled_back_immediately(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.liq_after_add = 15.0
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0
    controller.run_cycle()
    controller._state["positions"][0]["last_topup_at_ms"] = (  # pylint: disable=protected-access
        int(time.time() * 1000) - 1_900_000
    )
    gateway.positions[0]["mark_price"] = 10.0
    gateway.positions[0]["liq_price"] = 15.0
    gateway.liq_after_remove = 12.5
    controller.run_cycle()

    status = controller.run_cycle()

    assert gateway.margin_removes == [("TESTUSDT", 25.0)]
    assert gateway.margin_adds == [("TESTUSDT", 25.0), ("TESTUSDT", 25.0)]
    assert status["positions"][0]["margin_topup_usd"] == 25.0
    assert status["positions"][0]["liq_price"] == 15.0


def test_exchange_stop_is_resynced_when_liquidation_price_moves(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    assert gateway.protections[-1] == ("TESTUSDT", 7.5, 14.625)

    gateway.positions[0]["liq_price"] = 16.0
    status = controller.run_cycle()

    assert gateway.protections[-1] == ("TESTUSDT", 7.5, 15.6)
    assert status["positions"][0]["stop_price"] == 15.6


def test_four_slot_cap_can_open_four_distinct_main_signals(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    decisions = [
        ready_decision(
            armed_at + index + 1,
            symbol=f"TEST{index}USDT",
            event_id=f"test-event-{index}",
        )
        for index in range(4)
    ]

    assert controller.submit_decisions(decisions)["accepted"] == 4
    status = controller.run_cycle()

    assert status["config"]["entry_cap"] == 4
    assert status["open_positions"] == 4
    assert {item["symbol"] for item in status["positions"]} == {
        "TEST0USDT",
        "TEST1USDT",
        "TEST2USDT",
        "TEST3USDT",
    }


def test_four_positions_each_keep_fifty_dollar_rescue_quota(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions(
        [
            ready_decision(
                armed_at + index + 1,
                symbol=f"TEST{index}USDT",
                event_id=f"quota-{index}",
            )
            for index in range(4)
        ]
    )
    controller.run_cycle()
    for position in gateway.positions:
        position["mark_price"] = 13.0
        position["liq_price"] = 15.0

    controller.run_cycle()
    for item in controller._state["positions"]:  # pylint: disable=protected-access
        item["last_topup_at_ms"] = int(time.time() * 1000) - 400_000
    controller.run_cycle()
    for item in controller._state["positions"]:  # pylint: disable=protected-access
        item["last_topup_at_ms"] = int(time.time() * 1000) - 400_000
    status = controller.run_cycle()

    by_symbol: dict[str, float] = {}
    for symbol, amount in gateway.margin_adds:
        by_symbol[symbol] = by_symbol.get(symbol, 0.0) + amount
    assert by_symbol == {
        "TEST0USDT": 50.0,
        "TEST1USDT": 50.0,
        "TEST2USDT": 50.0,
        "TEST3USDT": 50.0,
    }
    assert status["entry_armed"] is True
    assert sum(item["margin_topup_usd"] for item in status["positions"]) == 200.0


def test_shared_emergency_pool_prioritizes_smallest_liquidation_buffer(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    gateway = FakePumpGateway()
    gateway.liq_after_add = 20.0
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions(
        [
            ready_decision(
                armed_at + index + 1,
                symbol=f"TEST{index}USDT",
                event_id=f"priority-{index}",
            )
            for index in range(4)
        ]
    )
    controller.run_cycle()
    now = int(time.time() * 1000)
    for item in controller._state["positions"]:  # pylint: disable=protected-access
        item["margin_topup_usd"] = 50.0
        item["last_topup_at_ms"] = now - 400_000
    marks = {
        "TEST0USDT": 13.1,
        "TEST1USDT": 13.2,
        "TEST2USDT": 13.5,
        "TEST3USDT": 14.0,
    }
    for position in gateway.positions:
        position["mark_price"] = marks[position["symbol"]]
        position["liq_price"] = 15.0
    gateway.margin_adds.clear()

    controller.run_cycle()

    assert gateway.margin_adds[:2] == [
        ("TEST3USDT", 50.0),
        ("TEST2USDT", 25.0),
    ]
    assert sum(amount for _, amount in gateway.margin_adds) == 75.0


def test_new_slot_is_blocked_when_existing_topups_consume_its_rescue_quota(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions(
        [
            ready_decision(
                armed_at + index + 1,
                symbol=f"TEST{index}USDT",
                event_id=f"budget-{index}",
            )
            for index in range(3)
        ]
    )
    controller.run_cycle()
    for item in controller._state["positions"]:  # pylint: disable=protected-access
        item["margin_topup_usd"] = (
            175.0 if item["symbol"] == "TEST0USDT" else 50.0
        )
    controller.submit_decisions(
        [
            ready_decision(
                int(time.time() * 1000),
                symbol="TEST3USDT",
                event_id="budget-3",
            )
        ]
    )

    status = controller.run_cycle()

    assert status["open_positions"] == 3
    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "rescue_budget_below_new_slot_guard"


def test_topup_notification_uses_injected_shared_route(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    notifier = FakePumpNotifier()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
        notifier=notifier,
        background_notifications=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0

    status = controller.run_cycle()

    assert gateway.margin_adds == [("TESTUSDT", 25.0)]
    assert any(title == "Pump Live TOP-UP TESTUSDT" for title, _ in notifier.messages)
    topup_message = next(text for title, text in notifier.messages if "TOP-UP" in title)
    assert "Сумма: $25.00" in topup_message
    assert "Свободно после: $975.00" in topup_message
    assert status["notifications"]["last_event"] == "margin_added"
    assert status["notifications"]["last_status"] == "ok"


def test_notification_failure_never_blocks_margin_protection(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
        notifier=FakePumpNotifier(fail=True),
        background_notifications=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0

    status = controller.run_cycle()

    assert gateway.margin_adds == [("TESTUSDT", 25.0)]
    assert status["positions"][0]["margin_topup_usd"] == 25.0
    assert status["notifications"]["last_status"] == "error"
    assert status["notifications"]["last_error"] == "simulated_notification_failure"


def test_emergency_buffer_closes_when_no_topup_cash_is_available(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.balance["available"] = 25.0
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 14.0

    status = controller.run_cycle()

    assert gateway.margin_adds == []
    assert gateway.positions == []
    assert status["positions"][0]["status"] == "closing"
    assert status["positions"][0]["close_reason"] == "emergency_liq_buffer"


def test_ambiguous_first_order_is_persisted_and_entries_are_disarmed(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.fail_market = True
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])

    status = controller.run_cycle()

    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "entry_execution_error"
    assert status["positions"][0]["status"] == "opening_uncertain"
    assert status["positions"][0]["legs"][0]["order_link_id"].startswith("FAP")


def test_reduce_only_exchange_order_does_not_block_reconciliation(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )

    assert controller._unknown_open_orders(  # pylint: disable=protected-access
        [{"id": "tp", "symbol": "TESTUSDT", "order_link_id": "", "reduce_only": True}]
    ) == []


def test_non_main_strategy_can_never_queue_a_live_entry(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    decision = ready_decision(armed_at + 1)
    decision["strategy_id"] = "long_broad"

    result = controller.submit_decisions([decision])

    assert result["accepted"] == 0
    assert controller.status()["pending_signals"] == []


def test_transient_monitor_error_recovers_entries_after_two_healthy_cycles(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller.arm(ARM_CONFIRMATION)
    gateway.balance_failures.append(TimeoutError("temporary balance timeout"))

    failed = controller.run_cycle()
    assert failed["entry_armed"] is False
    assert failed["blocked_reason"] == "monitor_cycle_transient_error"
    assert failed["transient_recovery_pending"] is True

    first_healthy = controller.run_cycle()
    assert first_healthy["entry_armed"] is False
    assert first_healthy["healthy_recovery_cycles"] == 1

    recovered = controller.run_cycle()
    assert recovered["entry_armed"] is True
    assert recovered["blocked_reason"] is None
    assert recovered["transient_recovery_pending"] is False
    assert any(row["event"] == "monitor_recovered" for row in recovered["recent_events"])


def test_non_transient_monitor_error_stays_disarmed(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller.arm(ARM_CONFIRMATION)
    gateway.balance_failures.append(RuntimeError("invalid credentials"))

    failed = controller.run_cycle()
    assert failed["entry_armed"] is False
    assert failed["blocked_reason"] == "monitor_cycle_error"
    assert failed["transient_recovery_pending"] is False

    controller.run_cycle()
    controller.run_cycle()
    assert controller.status()["entry_armed"] is False
    assert controller.status()["blocked_reason"] == "monitor_cycle_error"


def test_state_save_retries_transient_windows_replace_lock(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    from execution import pump_live as pump_live_module

    original_replace = pump_live_module.os.replace
    attempts = 0

    def flaky_replace(source: Any, target: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            raise PermissionError(5, "simulated Windows file lock", str(target))
        original_replace(source, target)

    monkeypatch.setattr(pump_live_module.os, "replace", flaky_replace)

    status = controller.arm(ARM_CONFIRMATION)

    assert status["entry_armed"] is True
    assert attempts >= 3
    assert not list((tmp_path / "state").glob("live_state.*.tmp"))


def test_bybit_full_protection_treats_not_modified_as_success(
    monkeypatch: Any,
) -> None:
    class AlreadyProtectedClient:
        @staticmethod
        def price_to_precision(symbol: str, value: float) -> str:
            del symbol
            return str(value)

        @staticmethod
        def private_post_v5_position_trading_stop(payload: dict[str, Any]) -> dict[str, Any]:
            del payload
            raise RuntimeError(
                'bybit {"retCode":34040,"retMsg":"not modified","result":{}}'
            )

    gateway = BybitPumpLiveGateway()
    monkeypatch.setattr(gateway, "_ensure_client", lambda: AlreadyProtectedClient())
    monkeypatch.setattr(
        gateway,
        "_market",
        lambda symbol: {"id": symbol, "symbol": f"{symbol[:-4]}/USDT:USDT"},
    )

    result = gateway.set_full_protection(
        "TESTUSDT",
        take_profit_price=7.5,
        stop_loss_price=14.625,
    )

    assert result == {"status": "already_set"}


def test_arm_can_resume_fully_tracked_position_after_restart(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    state_dir = tmp_path / "state"
    gateway = FakePumpGateway()
    first = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(first.arm(ARM_CONFIRMATION)["armed_at_ms"])
    first.submit_decisions([ready_decision(armed_at + 1)])
    first.run_cycle()
    gateway.preflight_existing_state_errors = True

    recovered = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    resumed = recovered.arm(ARM_CONFIRMATION)

    assert resumed["open_positions"] == 1
    assert resumed["entry_armed"] is True
    assert resumed["blocked_reason"] is None
    assert resumed["last_preflight"]["ready"] is True
    assert resumed["last_preflight"]["errors"] == []
    assert resumed["last_preflight"]["raw_ready"] is False
    assert resumed["last_preflight"]["resume_mode"] == "tracked_positions_verified"
    assert set(resumed["last_preflight"]["resume_tolerated_errors"]) == {
        "pump_live_subaccount_has_existing_positions",
        "pump_live_subaccount_has_unknown_open_orders",
    }
    assert any(row["event"] == "armed_resumed" for row in resumed["recent_events"])


def test_arm_resume_rejects_unknown_exchange_position(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    state_dir = tmp_path / "state"
    gateway = FakePumpGateway()
    first = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(first.arm(ARM_CONFIRMATION)["armed_at_ms"])
    first.submit_decisions([ready_decision(armed_at + 1)])
    first.run_cycle()
    gateway.positions.append(
        {
            "symbol": "ROGUEUSDT",
            "side": "short",
            "qty": 1.0,
            "avg_price": 1.0,
            "mark_price": 1.0,
            "liq_price": 1.5,
            "leverage": 3.0,
            "margin_mode": "isolated",
            "position_idx": 0,
            "unrealized_pnl": 0.0,
        }
    )
    gateway.preflight_existing_state_errors = True
    recovered = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )

    try:
        recovered.arm(ARM_CONFIRMATION)
    except RuntimeError as exc:
        assert "pump_live_resume_unknown_exchange_state" in str(exc)
    else:  # pragma: no cover - regression guard
        raise AssertionError("resume unexpectedly accepted an unknown position")
