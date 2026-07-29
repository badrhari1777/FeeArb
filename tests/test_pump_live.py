from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from execution.pump_live import (
    ARM_CONFIRMATION,
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
        self.margin_adds: list[tuple[str, float]] = []
        self.leverage_calls: list[tuple[str, float]] = []
        self.canceled: list[str] = []
        self.fail_market = False

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
        return {
            "ready": True,
            "checked_at_ms": int(time.time() * 1000),
            "credentials": self.credentials_status(),
            "account": {
                "margin_mode": "ISOLATED_MARGIN",
                "total_usdt": self.balance["total"],
                "available_usdt": self.balance["available"],
                "positions": len(self.positions),
                "open_orders": len(self.orders),
            },
            "errors": [],
            "warnings": ["api_key_has_no_ip_binding_dynamic_ip_mode"],
        }

    def prepare_account(self) -> dict[str, Any]:
        return {"status": "prepared"}

    def fetch_balance(self) -> dict[str, Any]:
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

    def set_full_take_profit(self, symbol: str, price: float) -> dict[str, Any]:
        self.take_profits.append((symbol, price))
        return {"status": "ok"}

    def add_margin(self, symbol: str, amount_usd: float) -> dict[str, Any]:
        self.margin_adds.append((symbol, amount_usd))
        self.balance["available"] -= amount_usd
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
