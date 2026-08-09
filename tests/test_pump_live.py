from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from execution.pump_live import (
    ARM_CONFIRMATION,
    BybitPumpLiveGateway,
    CAPITAL_SET_CONFIRMATION,
    PumpLiveConfig,
    PumpLiveController,
    build_capital_manager_status,
    build_capital_regime_status,
    build_live_legs,
    entry_prefund_target_check,
    load_pump_live_config,
    required_entry_prefund_usd,
    required_available_for_new_slot,
)


def test_capital_regime_reports_locked_and_temporary_cash() -> None:
    state = {
        "last_balance": {"wallet": 1050.0, "available": 380.0},
        "capital_manager": {"temporary_transfer_outstanding_usd": 50.0},
        "positions": [
            {
                "status": "open",
                "symbol": "HEIUSDT",
                "liq_buffer_pct": 14.0,
                "margin_topup_usd": 75.0,
                "margin_prefund_floor_usd": 75.0,
            },
            {
                "status": "open",
                "symbol": "BLUAIUSDT",
                "liq_buffer_pct": 52.0,
                "margin_topup_usd": 35.0,
                "margin_prefund_floor_usd": 25.0,
            },
        ],
    }

    result = build_capital_regime_status(state, PumpLiveConfig())

    assert result["mode"] == "stress"
    assert result["min_liq_buffer_symbol"] == "HEIUSDT"
    assert result["total_topup_usd"] == 110.0
    assert result["prefund_floor_usd"] == 100.0
    assert result["removable_topup_usd"] == 10.0
    assert result["temporary_occupied_usd"] == 50.0
    assert result["new_slot_required_available_usd"] == 365.0
    assert result["new_slot_headroom_usd"] == 15.0


def test_capital_regime_is_calm_without_open_positions() -> None:
    result = build_capital_regime_status(
        {"last_balance": {"wallet": 1000.0, "available": 1000.0}},
        PumpLiveConfig(),
    )

    assert result["mode"] == "calm"
    assert result["min_liq_buffer_pct"] is None


class FakePumpGateway:
    def __init__(self) -> None:
        self.balance = {
            "total": 1_000.0,
            "wallet": 1_000.0,
            "available": 1_000.0,
            "used": 0.0,
        }
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
        self.liq_after_add_sequence: list[float] = []
        self.liq_after_remove: float | None = None
        self.balance_failures: list[Exception] = []
        self.preflight_existing_state_errors = False
        self.initial_liq_price = 15.0
        self.operations: list[str] = []
        self.closed_trade_summary: dict[str, Any] = {"status": "unavailable"}

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

    def fetch_closed_trade_summary(
        self,
        symbol: str,
        *,
        opened_at_ms: int,
        closed_at_ms: int,
    ) -> dict[str, Any]:
        return {
            **self.closed_trade_summary,
            "symbol": symbol,
            "opened_at_ms": opened_at_ms,
            "closed_at_ms": closed_at_ms,
        }

    def set_leverage(self, symbol: str, leverage: float) -> None:
        self.leverage_calls.append((symbol, leverage))
        self.operations.append(f"leverage:{symbol}")

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
            self.operations.append(f"market_sell:{symbol}")
            fill_qty = float(notional_usd or 0.0) / 10.0
            self.positions = [item for item in self.positions if item.get("symbol") != symbol]
            self.positions.append(
                {
                    "symbol": symbol,
                    "side": "short",
                    "qty": fill_qty,
                    "avg_price": 10.0,
                    "mark_price": 10.0,
                    "liq_price": self.initial_liq_price,
                    "leverage": 3.0,
                    "margin_mode": "isolated",
                    "position_idx": 0,
                    "unrealized_pnl": 0.0,
                }
            )
        elif reduce_only:
            self.operations.append(f"market_reduce:{symbol}")
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
        self.operations.append(f"ladder:{symbol}:{price}")
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
        self.operations.append(f"add_margin:{symbol}:{amount_usd}")
        self.margin_adds.append((symbol, amount_usd))
        self.balance["available"] -= amount_usd
        liq_after_add = (
            self.liq_after_add_sequence.pop(0)
            if self.liq_after_add_sequence
            else self.liq_after_add
        )
        if liq_after_add is not None:
            for position in self.positions:
                if position.get("symbol") == symbol:
                    position["liq_price"] = liq_after_add
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


def write_env(
    path: Path,
    *,
    entry_cap: int = 1,
    prefund_enabled: bool = False,
) -> None:
    path.write_text(
        "\n".join(
            [
                "BYBIT_PUMP_API_KEY=fake",
                "BYBIT_PUMP_API_SECRET=fake",
                "BYBIT_PUMP_SUB_UID=123",
                f"PUMP_LIVE_ENTRY_CAP={entry_cap}",
                "PUMP_LIVE_POLL_INTERVAL_SEC=15",
                "PUMP_LIVE_MAX_SLIPPAGE_BPS=50",
                (
                    "PUMP_LIVE_MARGIN_PREFUND_ENABLED=1"
                    if prefund_enabled
                    else "PUMP_LIVE_MARGIN_PREFUND_ENABLED=0"
                ),
                "PUMP_LIVE_MARGIN_PREFUND_SAFETY_PCT=2.5",
                "PUMP_LIVE_MARGIN_PREFUND_TOLERANCE_PCT=2.0",
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
    assert config.entry_margin_prefund_tolerance_pct == 2.0
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


def test_capital_observation_reports_growth_without_changing_active_slot(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    gateway = FakePumpGateway()
    gateway.balance.update(
        {"total": 1_043.943424, "wallet": 1_043.943424, "available": 1_043.943424}
    )
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )

    status = controller.run_cycle()
    capital = status["capital_manager"]

    assert capital["mode"] == "observe"
    assert capital["application_enabled"] is False
    assert capital["effective_strategy_capital_usd"] == 1_043.943424
    assert capital["active_slot_margin_usd"] == 175.0
    assert capital["recommended_slot_margin_usd"] == 180.0
    assert capital["next_capped_slot_margin_usd"] == 175.0
    assert capital["recommendation"] == "hold_band"
    assert status["config"]["slot_margin_usd"] == 175.0


def test_operator_capital_declaration_tracks_wallet_delta_and_persists(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    state_dir = tmp_path / "state"
    gateway = FakePumpGateway()
    gateway.balance.update(
        {"total": 1_500.0, "wallet": 1_500.0, "available": 1_500.0}
    )
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )

    status = controller.set_strategy_capital(
        1_400.0,
        CAPITAL_SET_CONFIRMATION,
        "keep 100 as excluded reserve",
    )
    capital = status["capital_manager"]

    assert capital["declared_strategy_capital_usd"] == 1_400.0
    assert capital["declared_account_wallet_usd"] == 1_500.0
    assert capital["equity_adjustment_usd"] == -100.0
    assert capital["effective_strategy_capital_usd"] == 1_400.0
    assert capital["recommended_slot_margin_usd"] == 245.0
    assert capital["next_capped_slot_margin_usd"] == 215.0
    assert capital["recommendation"] == "increase_ready"
    assert status["config"]["slot_margin_usd"] == 175.0
    assert any(row["event"] == "capital_declared" for row in status["recent_events"])

    gateway.balance.update(
        {"total": 1_550.0, "wallet": 1_550.0, "available": 1_550.0}
    )
    controller.run_cycle()
    recovered = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    ).status()

    assert recovered["capital_manager"]["effective_strategy_capital_usd"] == 1_450.0
    assert recovered["capital_manager"]["equity_adjustment_usd"] == -100.0
    assert recovered["capital_manager"]["active_slot_margin_usd"] == 175.0


def test_capital_declaration_cannot_exceed_exchange_wallet(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )

    try:
        controller.set_strategy_capital(1_100.0, CAPITAL_SET_CONFIRMATION)
    except RuntimeError as exc:
        assert str(exc) == "pump_live_strategy_capital_exceeds_wallet_balance"
    else:  # pragma: no cover - regression guard
        raise AssertionError("capital above wallet was unexpectedly accepted")


def test_observation_readiness_requires_time_and_new_closed_trades() -> None:
    now = int(time.time() * 1000)
    state = {
        "last_balance": {"wallet": 1_100.0, "total": 1_100.0},
        "positions": [
            {"status": "closed", "closed_at_ms": now - index}
            for index in range(10)
        ],
        "capital_manager": {
            "active_strategy_capital_usd": 1_000.0,
            "declared_strategy_capital_usd": 1_100.0,
            "declared_account_wallet_usd": 1_100.0,
            "equity_adjustment_usd": 0.0,
            "observation_started_at_ms": now - 15 * 86_400_000,
            "closed_trades_baseline": 0,
        },
    }

    result = build_capital_manager_status(state, PumpLiveConfig(), now_ms=now)

    assert result["observation_ready"] is True
    assert result["observation_closed_trades"] == 10
    assert result["recommendation"] == "increase_ready"
    assert result["recommended_slot_margin_usd"] == 190.0
    assert result["next_capped_slot_margin_usd"] == 190.0


def test_observed_capital_never_resizes_live_strategy_legs(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4)
    gateway = FakePumpGateway()
    gateway.balance.update(
        {"total": 1_500.0, "wallet": 1_500.0, "available": 1_500.0}
    )
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller.set_strategy_capital(1_500.0, CAPITAL_SET_CONFIRMATION)
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])

    status = controller.run_cycle()
    position = next(item for item in status["positions"] if item["status"] != "closed")

    assert sum(float(leg["margin_usd"]) for leg in position["legs"]) == 175.0
    assert status["capital_manager"]["recommended_slot_margin_usd"] == 260.0
    assert status["capital_manager"]["next_capped_slot_margin_usd"] == 215.0
    assert status["config"]["slot_margin_usd"] == 175.0


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


def test_entry_prefunds_margin_before_ladders_without_changing_strategy(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 16.0
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

    item = status["positions"][0]
    assert item["status"] == "open"
    assert item["margin_topup_usd"] == 25.0
    assert item["margin_prefund_floor_usd"] == 25.0
    assert item["margin_prefund_status"] == "confirmed"
    assert item["stop_price"] == 15.6
    assert len(item["legs"]) == 3
    assert [leg["margin_usd"] for leg in item["legs"]] == [
        29.166667,
        58.333333,
        87.5,
    ]
    assert len(gateway.orders) == 2
    margin_index = next(
        index
        for index, operation in enumerate(gateway.operations)
        if operation.startswith("add_margin:")
    )
    ladder_index = next(
        index
        for index, operation in enumerate(gateway.operations)
        if operation.startswith("ladder:")
    )
    assert margin_index < ladder_index


def test_filled_second_leg_refreshes_position_and_full_protection(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 16.0
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    decision = ready_decision(armed_at + 1)
    decision["tier"].update(
        {
            "rule_slug": "step50_legs2_tapered_tp25_720",
            "ladder_legs": 2,
            "leg_weights": [1.0, 2.0],
            "max_hold_h": 720,
        }
    )
    controller.submit_decisions([decision])

    opened = controller.run_cycle()
    item = opened["positions"][0]
    second = item["legs"][1]
    assert second["status"] == "open"
    assert second["trigger_price"] == 15.0
    assert item["margin_prefund_status"] == "confirmed"
    assert item["stop_price"] == 15.6

    ladder_order = next(
        order for order in gateway.orders if order["id"] == second["order_id"]
    )
    second_qty = float(ladder_order["notional_usd"]) / float(ladder_order["price"])
    total_qty = 17.5 + second_qty
    average = (17.5 * 10.0 + second_qty * 15.0) / total_qty
    ladder_order.update(
        {"status": "closed", "filled": second_qty, "average": 15.0}
    )
    gateway.positions = [
        {
            "symbol": "TESTUSDT",
            "side": "short",
            "qty": total_qty,
            "avg_price": average,
            "mark_price": 15.0,
            "liq_price": 19.0,
            "leverage": 3.0,
            "margin_mode": "isolated",
            "position_idx": 0,
            "unrealized_pnl": 0.0,
        }
    ]

    refreshed = controller.run_cycle()

    item = refreshed["positions"][0]
    second = item["legs"][1]
    assert second["status"] == "filled"
    assert second["filled_qty"] == second_qty
    assert second["avg_fill_price"] == 15.0
    assert item["qty"] == total_qty
    assert item["avg_entry_price"] == average
    assert item["tp_price"] == average * 0.75
    assert item["stop_price"] == 19.0 * 0.975
    assert gateway.protections[-1] == (
        "TESTUSDT",
        average * 0.75,
        19.0 * 0.975,
    )


def test_prefund_formula_matches_current_tier_l2_amounts() -> None:
    current_liq = (1.0 + 1.0 / 3.0) / 1.025
    tier_first_notionals = {
        "ordinary_lt80": 105.0,
        "strong_80_100": 175.0,
        "strong_100_250": 87.5,
        "super_250_plus": 175.0,
    }

    amounts = {
        tier: required_entry_prefund_usd(
            qty=notional,
            current_liq_price=current_liq,
            next_ladder_price=1.5,
            stop_gap_from_liq_pct=2.5,
            safety_above_next_ladder_pct=2.5,
            maintenance_margin_rate=0.025,
            taker_fee_rate=0.00055,
            round_up_usd=5.0,
        )
        for tier, notional in tier_first_notionals.items()
    }

    assert amounts == {
        "ordinary_lt80": 30.0,
        "strong_80_100": 50.0,
        "strong_100_250": 25.0,
        "super_250_plus": 50.0,
    }


def test_prefund_tolerance_is_two_percent_of_required_clearance() -> None:
    accepted = entry_prefund_target_check(
        verified_stop_price=102.45,
        target_stop_price=102.5,
        next_ladder_price=100.0,
        tolerance_pct=2.0,
    )
    rejected = entry_prefund_target_check(
        verified_stop_price=102.449,
        target_stop_price=102.5,
        next_ladder_price=100.0,
        tolerance_pct=2.0,
    )

    assert accepted["ready"] is True
    assert accepted["tolerance_used"] is True
    assert accepted["minimum_clearance_pct"] == 2.45
    assert rejected["ready"] is False


def test_prefund_adds_bounded_five_dollar_correction_after_confirmed_move(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add_sequence = [15.7, 16.0]
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
    item = status["positions"][0]

    assert item["status"] == "open"
    assert item["margin_prefund_status"] == "confirmed"
    assert item["margin_topup_usd"] == 30.0
    assert item["margin_prefund_floor_usd"] == 30.0
    assert item["margin_prefund_verification"]["ready"] is True
    assert gateway.margin_adds == [("TESTUSDT", 25.0), ("TESTUSDT", 5.0)]
    assert len(gateway.orders) == 2


def test_prefund_accepts_only_clearance_relative_tolerance_without_correction(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 15.762
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
    verification = status["positions"][0]["margin_prefund_verification"]

    assert status["positions"][0]["margin_topup_usd"] == 25.0
    assert verification["ready"] is True
    assert verification["tolerance_used"] is True
    assert gateway.margin_adds == [("TESTUSDT", 25.0)]


def test_prefund_correction_is_bounded_to_three_confirmed_steps(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add_sequence = [15.0, 15.1, 15.2, 15.3]
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
    assert status["positions"][0]["status"] == "opening_uncertain"
    assert status["positions"][0]["margin_topup_usd"] == 40.0
    assert gateway.margin_adds == [
        ("TESTUSDT", 25.0),
        ("TESTUSDT", 5.0),
        ("TESTUSDT", 5.0),
        ("TESTUSDT", 5.0),
    ]
    assert gateway.orders == []


def test_arm_recovers_exact_prefund_failure_without_duplicate_ladders(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add_sequence = [15.7, 15.7]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])

    failed = controller.run_cycle()
    assert failed["positions"][0]["status"] == "opening_uncertain"
    assert failed["positions"][0]["margin_topup_usd"] == 30.0
    assert gateway.orders == []

    gateway.preflight_existing_state_errors = True
    gateway.liq_after_add_sequence = [16.0]
    recovered = controller.arm(ARM_CONFIRMATION)

    assert recovered["entry_armed"] is True
    assert recovered["blocked_reason"] is None
    assert recovered["positions"][0]["status"] == "open"
    assert recovered["positions"][0]["margin_topup_usd"] == 35.0
    assert recovered["positions"][0]["last_error"] is None
    assert len(gateway.orders) == 2
    assert all(leg["status"] == "open" for leg in recovered["positions"][0]["legs"][1:])

    controller.arm(ARM_CONFIRMATION)
    assert len(gateway.orders) == 2


def test_arm_does_not_recover_unrecognized_opening_uncertain_state(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 13.0
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
    position = controller._state["positions"][0]  # pylint: disable=protected-access
    position["last_error"] = "unrecognized_failure"
    gateway.preflight_existing_state_errors = True
    margin_adds_before = list(gateway.margin_adds)

    try:
        controller.arm(ARM_CONFIRMATION)
    except RuntimeError as exc:
        assert "pump_live_resume_unknown_exchange_state" in str(exc)
    else:  # pragma: no cover - regression guard
        raise AssertionError("unrecognized degraded position was unexpectedly recovered")

    assert gateway.margin_adds == margin_adds_before
    assert gateway.orders == []


def test_prefund_keeps_all_five_strategy_ladders(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 16.0
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    decision = ready_decision(armed_at + 1)
    decision["pump_pct"] = 55.0
    decision["tier"] = {
        "rule_slug": "step50_legs5_equal_tp25_720",
        "ladder_step_pct": 50.0,
        "ladder_legs": 5,
        "leg_weights": [1.0, 1.0, 1.0, 1.0, 1.0],
        "tp_pct": 25.0,
        "max_hold_h": 720,
    }
    controller.submit_decisions([decision])

    status = controller.run_cycle()

    item = status["positions"][0]
    assert item["margin_prefund_floor_usd"] == 30.0
    assert len(item["legs"]) == 5
    assert len(gateway.orders) == 4
    assert [leg["trigger_price"] for leg in item["legs"]] == [
        10.0,
        15.0,
        20.0,
        25.0,
        30.0,
    ]


def test_margin_reduction_never_removes_entry_prefund_floor(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 16.0
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
    item = controller._state["positions"][0]  # pylint: disable=protected-access
    item["margin_topup_usd"] = 50.0
    item["last_topup_at_ms"] = int(time.time() * 1000) - 1_900_000
    gateway.positions[0]["mark_price"] = 10.0
    gateway.positions[0]["liq_price"] = 20.0

    controller.run_cycle()
    status = controller.run_cycle()
    controller.run_cycle()
    controller.run_cycle()

    assert gateway.margin_removes == [("TESTUSDT", 25.0)]
    assert status["positions"][0]["margin_topup_usd"] == 25.0
    assert status["positions"][0]["margin_prefund_floor_usd"] == 25.0


def test_prefund_defaults_on_but_can_be_disabled_in_local_config(
    tmp_path: Path,
) -> None:
    missing = load_pump_live_config(tmp_path / "missing.env")
    assert missing.entry_margin_prefund_enabled is True
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=False)
    disabled = load_pump_live_config(env_path)
    assert disabled.entry_margin_prefund_enabled is False


def test_prefund_failure_keeps_first_leg_protected_and_does_not_place_ladders(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 13.0
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

    item = status["positions"][0]
    assert item["status"] == "opening_uncertain"
    assert item["margin_prefund_status"] == "target_unconfirmed"
    assert item["margin_topup_usd"] == 25.0
    assert item["margin_prefund_floor_usd"] == 25.0
    assert gateway.orders == []
    assert gateway.positions
    assert gateway.protections[-1][:2] == ("TESTUSDT", 7.5)
    assert round(gateway.protections[-1][2], 6) == 12.675
    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "entry_execution_error"


def test_prefund_floor_survives_restart_recovery_state(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    state_dir = tmp_path / "state"
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 16.0
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()

    recovered = PumpLiveController(
        gateway=gateway,
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    status = recovered.status()

    assert status["status"] == "recovery_monitoring"
    assert status["positions"][0]["margin_prefund_floor_usd"] == 25.0
    assert status["positions"][0]["margin_prefund_status"] == "confirmed"


def test_four_prefunded_positions_keep_every_original_ladder(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.initial_liq_price = 13.0
    gateway.liq_after_add = 16.0
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
                event_id=f"prefund-{index}",
            )
            for index in range(4)
        ]
    )

    status = controller.run_cycle()

    assert status["open_positions"] == 4
    assert len(gateway.orders) == 8
    assert sum(item["margin_prefund_floor_usd"] for item in status["positions"]) == 100.0
    assert all(len(item["legs"]) == 3 for item in status["positions"])
    assert status["entry_armed"] is True


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


def test_flat_position_persists_exact_exchange_accounting(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.closed_trade_summary = {
        "status": "complete",
        "entry_qty": 17.5,
        "exit_qty": 17.5,
        "entry_notional_usd": 175.0,
        "avg_entry_price": 10.0,
        "avg_exit_price": 7.49,
        "gross_pnl_usd": 43.925,
        "fees_usd": 0.33,
        "funding_pnl_usd": -0.25,
        "net_pnl_usd": 43.345,
        "net_return_on_entry_notional_pct": 24.768571,
    }
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

    controller.run_cycle()
    status = controller.run_cycle()

    closed = next(item for item in status["positions"] if item["status"] == "closed")
    assert closed["close_accounting_status"] == "complete"
    assert closed["avg_exit_price"] == 7.49
    assert closed["realized_pnl_usd"] == 43.345
    assert closed["fees_usd"] == 0.33
    assert closed["funding_pnl_usd"] == -0.25
    event = next(row for row in status["recent_events"] if row["event"] == "position_confirmed_flat")
    assert event["realized_pnl_usd"] == 43.345


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


def test_new_slot_guard_does_not_reserve_existing_topups_twice(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4, prefund_enabled=False)
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
                event_id=f"existing-{index}",
            )
            for index in range(3)
        ]
    )
    controller.run_cycle()
    topups = [25.0, 35.0, 75.0]
    for item, amount in zip(
        controller._state["positions"],  # pylint: disable=protected-access
        topups,
    ):
        item["margin_topup_usd"] = amount
    gateway.balance["available"] = 380.95
    controller.submit_decisions(
        [
            ready_decision(
                int(time.time() * 1000),
                symbol="TEST3USDT",
                event_id="fourth-slot",
            )
        ]
    )

    status = controller.run_cycle()

    config = controller.config()
    assert required_available_for_new_slot(
        config,
        current_total_topup_usd=sum(topups),
    ) == 340.0
    assert status["open_positions"] == 4
    assert status["entry_armed"] is True


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


def test_warning_buffer_can_request_cash_then_add_full_allowed_margin(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.liq_after_add = 20.0
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
    requests: list[dict[str, Any]] = []

    def transfer_provider(**payload: Any) -> dict[str, Any]:
        requests.append(payload)
        gateway.balance["available"] += 50.0
        return {"status": "complete", "amount_usd": 50.0, "transfer_id": "auto-1"}

    controller.set_risk_transfer_provider(transfer_provider)
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.balance["available"] = 25.0
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 14.8

    status = controller.run_cycle()

    assert len(requests) == 1
    assert requests[0]["symbol"] == "TESTUSDT"
    assert requests[0]["requested_usd"] == 50.0
    assert gateway.margin_adds == [("TESTUSDT", 50.0)]
    assert status["positions"][0]["margin_topup_usd"] == 50.0
    assert any("AUTO TRANSFER" in title for title, _text in notifier.messages)


def test_emergency_buffer_never_waits_for_risk_transfer(tmp_path: Path) -> None:
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
    requests: list[dict[str, Any]] = []
    controller.set_risk_transfer_provider(lambda **payload: requests.append(payload) or {})
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.balance["available"] = 25.0
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 14.0

    status = controller.run_cycle()

    assert requests == []
    assert gateway.positions == []
    assert status["positions"][0]["close_reason"] == "emergency_liq_buffer"


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


def test_bybit_balance_uses_exact_usdt_wallet_not_usd_conversion(
    monkeypatch: Any,
) -> None:
    class BalanceClient:
        @staticmethod
        def fetch_balance(params: dict[str, Any]) -> dict[str, Any]:
            assert params == {"type": "swap", "accountType": "UNIFIED"}
            return {
                "USDT": {
                    "total": 1_043.94342401,
                    "free": 1_043.94342401,
                },
                "info": {
                    "result": {
                        "list": [
                            {
                                "totalWalletBalance": "1042.5592786",
                                "totalEquity": "1042.5592786",
                                "totalAvailableBalance": "1043.94342401",
                                "coin": [
                                    {"coin": "ETC", "walletBalance": "0.00001822"},
                                    {
                                        "coin": "USDT",
                                        "walletBalance": "1043.94342401",
                                    },
                                ],
                            }
                        ]
                    }
                },
            }

    gateway = BybitPumpLiveGateway()
    monkeypatch.setattr(gateway, "_ensure_client", lambda: BalanceClient())

    balance = gateway.fetch_balance()

    assert balance["total"] == 1_043.94342401
    assert balance["wallet"] == 1_043.94342401
    assert balance["available"] == 1_043.94342401
    assert balance["used"] == 0.0


def test_bybit_private_read_resyncs_time_and_retries_once(
    monkeypatch: Any,
) -> None:
    class TimeSyncBalanceClient:
        def __init__(self) -> None:
            self.options = {"timeDifference": -1_170}
            self.calls = 0
            self.sync_calls = 0

        def load_time_difference(self) -> int:
            self.sync_calls += 1
            self.options["timeDifference"] = 25
            return 25

        def fetch_balance(self, params: dict[str, Any]) -> dict[str, Any]:
            assert params == {"type": "swap", "accountType": "UNIFIED"}
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError(
                    'bybit {"retCode":10002,"retMsg":"invalid request, please check '
                    "your server timestamp or recv_window param: "
                    'req_timestamp[2000],server_timestamp[830],recv_window[5000]"}'
                )
            return {
                "USDT": {"total": 1000.0, "free": 1000.0},
                "info": {
                    "result": {
                        "list": [
                            {
                                "totalAvailableBalance": "1000",
                                "coin": [
                                    {"coin": "USDT", "walletBalance": "1000"},
                                ],
                            }
                        ]
                    }
                },
            }

    client = TimeSyncBalanceClient()
    gateway = BybitPumpLiveGateway()
    monkeypatch.setattr(gateway, "_ensure_client", lambda: client)

    balance = gateway.fetch_balance()

    assert balance["wallet"] == 1000.0
    assert client.calls == 2
    assert client.sync_calls == 1
    assert client.options["timeDifference"] == 25


def test_bybit_private_write_resyncs_before_retrying_rejected_order(
    monkeypatch: Any,
) -> None:
    class TimeSyncOrderClient:
        def __init__(self) -> None:
            self.options = {"timeDifference": -1_170}
            self.calls = 0
            self.sync_calls = 0

        @staticmethod
        def price_to_precision(symbol: str, value: float) -> str:
            del symbol
            return str(value)

        @staticmethod
        def amount_to_precision(symbol: str, value: float) -> str:
            del symbol
            return str(value)

        def load_time_difference(self) -> int:
            self.sync_calls += 1
            self.options["timeDifference"] = 20
            return 20

        def create_order(self, *args: Any) -> dict[str, Any]:
            del args
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError(
                    'bybit {"retCode":10002,"retMsg":"invalid request, please check '
                    "your server timestamp or recv_window param: "
                    'req_timestamp[2000],server_timestamp[830],recv_window[5000]"}'
                )
            return {
                "id": "order-1",
                "status": "open",
                "filled": 0.0,
                "average": None,
            }

    client = TimeSyncOrderClient()
    gateway = BybitPumpLiveGateway()
    monkeypatch.setattr(gateway, "_ensure_client", lambda: client)
    monkeypatch.setattr(
        gateway,
        "_market",
        lambda symbol: {"id": symbol, "symbol": f"{symbol[:-4]}/USDT:USDT"},
    )

    order = gateway.create_ladder_order(
        symbol="TESTUSDT",
        notional_usd=100.0,
        price=10.0,
        order_link_id="FAP-test",
    )

    assert order["id"] == "order-1"
    assert client.calls == 2
    assert client.sync_calls == 1


def test_bybit_private_request_does_not_retry_unrelated_error(
    monkeypatch: Any,
) -> None:
    class InvalidCredentialsClient:
        def __init__(self) -> None:
            self.options = {"timeDifference": 0}
            self.calls = 0
            self.sync_calls = 0

        def load_time_difference(self) -> None:
            self.sync_calls += 1

        def fetch_balance(self, params: dict[str, Any]) -> dict[str, Any]:
            del params
            self.calls += 1
            raise RuntimeError("bybit invalid credentials")

    client = InvalidCredentialsClient()
    gateway = BybitPumpLiveGateway()
    monkeypatch.setattr(gateway, "_ensure_client", lambda: client)

    try:
        gateway.fetch_balance()
    except RuntimeError as exc:
        assert "invalid credentials" in str(exc)
    else:  # pragma: no cover - regression guard
        raise AssertionError("unrelated error was unexpectedly swallowed")

    assert client.calls == 1
    assert client.sync_calls == 0


def test_monitor_time_errors_share_notification_dedupe_key_and_recovery_notifies(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    notifier = FakePumpNotifier()
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
        notifier=notifier,
        background_notifications=False,
    )
    first_error = (
        'bybit {"retCode":10002,"retMsg":"invalid request, please check your '
        "server timestamp or recv_window param: "
        'req_timestamp[2000],server_timestamp[830],recv_window[5000]"}'
    )
    second_error = (
        'bybit {"retCode":10002,"retMsg":"invalid request, please check your '
        "server timestamp or recv_window param: "
        'req_timestamp[3000],server_timestamp[1830],recv_window[5000]"}'
    )

    controller._event("monitor_error", {"error": first_error})  # pylint: disable=protected-access
    controller._event("monitor_error", {"error": second_error})  # pylint: disable=protected-access
    controller._event("monitor_recovered", {"healthy_cycles": 2})  # pylint: disable=protected-access

    assert len(notifier.messages) == 2
    assert "ошибка мониторинга" in notifier.messages[0][0].lower()
    assert "восстановлен" in notifier.messages[1][0].lower()
    status = controller.status()
    assert status["notifications"]["last_event"] == "monitor_recovered"
    deliveries = [
        row
        for row in status["recent_events"]
        if row.get("event") == "notification_delivery"
    ]
    assert [row["source_event"] for row in deliveries] == [
        "monitor_error",
        "monitor_recovered",
    ]


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
