from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

from execution.pump_live import (
    ARM_CONFIRMATION,
    ARM_CONFIRMATION_V2,
    BybitPumpLiveGateway,
    CAPITAL_SET_CONFIRMATION,
    CAPITAL_PROMOTE_CONFIRMATION,
    PREFUND_NEXT_LADDER_CONFIRMATION_PREFIX,
    MARGIN_MANAGER_V2_CURRENT,
    MARGIN_MANAGER_V3_SHARED,
    MARGIN_MANAGER_V4_ON_DEMAND,
    RISK_POLICY_V1,
    RISK_POLICY_V2,
    RISK_POLICY_V3,
    PumpLiveConfig,
    PumpLiveController,
    build_capital_manager_status,
    build_capital_regime_status,
    build_capital_rescue_shadow,
    build_live_legs,
    config_from_risk_snapshot,
    entry_prefund_target_check,
    ladder_prefund_plan,
    projected_ladder_margin_reserve,
    load_pump_live_config,
    plan_safe_margin_reduction,
    required_entry_prefund_usd,
    required_margin_for_liq_buffer_usd,
    required_available_for_new_slot,
    risk_policy_config,
    risk_policy_snapshot,
)


def test_pump_live_page_supports_pool600_arm_contract() -> None:
    template = (
        PROJECT_ROOT / "webapp" / "templates" / "pump_short_strategies.html"
    ).read_text(encoding="utf-8")
    javascript = (
        PROJECT_ROOT / "webapp" / "static" / "pump_short_strategies.js"
    ).read_text(encoding="utf-8")

    assert "shared Pump cash" in template
    assert "four $175 slots" not in template
    assert "v3_3000_pool600" in javascript
    assert "policy === 'v2_3000' || policy === 'v3_3000_pool600'" in javascript
    assert "new v2 $525 canary" not in javascript


def test_versioned_risk_policies_have_fixed_1000_and_3000_envelopes() -> None:
    runtime = PumpLiveConfig(entry_cap=3, poll_interval_sec=7)

    legacy = risk_policy_config(RISK_POLICY_V1, runtime)
    promoted = risk_policy_config(RISK_POLICY_V2, runtime)

    assert legacy.slot_margin_usd == 175.0
    assert legacy.max_total_topup_usd == 275.0
    assert promoted.total_capital_usd == 3_000.0
    assert promoted.slot_margin_usd == 525.0
    assert promoted.reserve_usd == 900.0
    assert promoted.guaranteed_position_topup_usd == 150.0
    assert promoted.max_total_topup_usd == 825.0
    assert promoted.operating_cash_floor_usd == 75.0
    assert promoted.entry_cap == 3
    assert promoted.poll_interval_sec == 7


def test_on_demand_policy_derives_600_new_position_budget_from_3000() -> None:
    runtime = PumpLiveConfig(
        margin_manager_policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
    )

    policy = risk_policy_config(RISK_POLICY_V3, runtime)
    five = build_live_legs(
        tier={"ladder_legs": 5, "leg_weights": [1, 1, 1, 1, 1]},
        slot_margin_usd=policy.slot_margin_usd,
        leverage=policy.leverage,
        reference_price=1.0,
    )
    three = build_live_legs(
        tier={"ladder_legs": 3, "leg_weights": [1, 2, 3]},
        slot_margin_usd=policy.slot_margin_usd,
        leverage=policy.leverage,
        reference_price=1.0,
    )
    two = build_live_legs(
        tier={"ladder_legs": 2, "leg_weights": [1, 2]},
        slot_margin_usd=policy.slot_margin_usd,
        leverage=policy.leverage,
        reference_price=1.0,
    )

    assert policy.slot_margin_usd == 600.0
    assert [row["margin_usd"] for row in five] == [120.0] * 5
    assert [row["margin_usd"] for row in three] == [100.0, 200.0, 300.0]
    assert [row["margin_usd"] for row in two] == [200.0, 400.0]


@pytest.mark.parametrize(
    ("legs_count", "weights"),
    [
        (2, [1, 2]),
        (3, [1, 2, 3]),
        (5, [1, 1, 1, 1, 1]),
    ],
)
def test_on_demand_every_next_fill_keeps_old_and_projected_stop_clear(
    legs_count: int,
    weights: list[float],
) -> None:
    config = risk_policy_config(
        RISK_POLICY_V3,
        PumpLiveConfig(margin_manager_policy_id=MARGIN_MANAGER_V4_ON_DEMAND),
    )
    legs = build_live_legs(
        tier={
            "ladder_legs": legs_count,
            "ladder_step_pct": 50.0,
            "leg_weights": weights,
        },
        slot_margin_usd=config.slot_margin_usd,
        leverage=config.leverage,
        reference_price=1.0,
    )
    qty = float(legs[0]["notional_usd"]) / float(legs[0]["trigger_price"])
    current_liq = (
        (1.0 + 1.0 / config.leverage)
        / (1.0 + config.entry_margin_prefund_mmr)
    )
    factor = (
        qty
        * (1.0 + config.entry_margin_prefund_mmr)
        * (1.0 + config.entry_margin_prefund_taker_fee_rate)
    )

    for target_leg in legs[1:]:
        plan = ladder_prefund_plan(
            policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
            qty=qty,
            current_liq_price=current_liq,
            legs=legs,
            target_leg=target_leg,
            leverage=config.leverage,
            stop_gap_from_liq_pct=config.exchange_stop_gap_from_liq_pct,
            safety_above_next_ladder_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
            final_fill_buffer_pct=config.projected_final_fill_buffer_pct,
            maintenance_margin_rate=config.entry_margin_prefund_mmr,
            taker_fee_rate=config.entry_margin_prefund_taker_fee_rate,
            round_up_increment_usd=config.entry_margin_prefund_round_usd,
            projected_reaction_buffer_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
        )
        required = float(plan["required_add_usd"])
        current_liq += required / factor
        verified = ladder_prefund_plan(
            policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
            qty=qty,
            current_liq_price=current_liq,
            legs=legs,
            target_leg=target_leg,
            leverage=config.leverage,
            stop_gap_from_liq_pct=config.exchange_stop_gap_from_liq_pct,
            safety_above_next_ladder_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
            final_fill_buffer_pct=config.projected_final_fill_buffer_pct,
            maintenance_margin_rate=config.entry_margin_prefund_mmr,
            taker_fee_rate=config.entry_margin_prefund_taker_fee_rate,
            round_up_increment_usd=config.entry_margin_prefund_round_usd,
            projected_reaction_buffer_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
        )
        fill_price = float(target_leg["trigger_price"])
        old_stop = current_liq * (
            1.0 - config.exchange_stop_gap_from_liq_pct / 100.0
        )
        projected_stop = float(verified["projected_stop_price"])

        assert verified["required_add_usd"] == 0.0
        assert old_stop / fill_price - 1.0 >= (
            config.on_demand_fill_reaction_buffer_pct / 100.0 - 1e-9
        )
        assert projected_stop / fill_price - 1.0 >= (
            config.on_demand_fill_reaction_buffer_pct / 100.0 - 1e-9
        )

        qty = float(verified["projected_qty"])
        current_liq = float(verified["projected_liq_price"])
        factor = (
            qty
            * (1.0 + config.entry_margin_prefund_mmr)
            * (1.0 + config.entry_margin_prefund_taker_fee_rate)
        )


def test_current_live_like_margin_release_is_capped_by_next_gate() -> None:
    cases = [
        {
            "symbol": "1000RATSUSDT",
            "qty": 1_750.0,
            "liq": 0.08431,
            "mark": 0.03985,
            "topup": 35.0,
            "expected": 10.0,
            "legs": [
                {"step": 1, "trigger_price": 0.04989, "notional_usd": 87.5},
                {"step": 2, "trigger_price": 0.074835, "notional_usd": 175.0},
                {"step": 3, "trigger_price": 0.09978, "notional_usd": 262.5},
            ],
            "next_index": 1,
        },
        {
            "symbol": "BLUAIUSDT",
            "qty": 9_810.0,
            "liq": 0.039656,
            "mark": 0.027909,
            "topup": 125.0,
            "expected": 20.0,
            "legs": [
                {"step": 1, "trigger_price": 0.017841, "notional_usd": 105.0},
                {"step": 2, "trigger_price": 0.0267615, "notional_usd": 105.0},
                {"step": 3, "trigger_price": 0.035682, "notional_usd": 105.0},
                {"step": 4, "trigger_price": 0.0446025, "notional_usd": 105.0},
                {"step": 5, "trigger_price": 0.053523, "notional_usd": 105.0},
            ],
            "next_index": 2,
        },
        {
            "symbol": "ACEUSDT",
            "qty": 2_690.9,
            "liq": 0.21304,
            "mark": 0.10869,
            "topup": 165.0,
            "expected": 75.0,
            "legs": [
                {"step": 1, "trigger_price": 0.11704, "notional_usd": 315.0},
                {"step": 2, "trigger_price": 0.17556, "notional_usd": 315.0},
                {"step": 3, "trigger_price": 0.23408, "notional_usd": 315.0},
                {"step": 4, "trigger_price": 0.2926, "notional_usd": 315.0},
                {"step": 5, "trigger_price": 0.35112, "notional_usd": 315.0},
            ],
            "next_index": 1,
        },
    ]

    for case in cases:
        plan = plan_safe_margin_reduction(
            qty=float(case["qty"]),
            current_liq_price=float(case["liq"]),
            mark_price=float(case["mark"]),
            removable_margin_usd=float(case["topup"]),
            target_buffer_pct=25.0,
            legs=case["legs"],
            target_leg=case["legs"][int(case["next_index"])],
            leverage=3.0,
            stop_gap_from_liq_pct=2.5,
            safety_above_next_ladder_pct=2.5,
            final_fill_buffer_pct=20.0,
            maintenance_margin_rate=0.025,
            taker_fee_rate=0.00055,
            round_down_increment_usd=5.0,
            projected_reaction_buffer_pct=8.0,
        )

        assert plan["reason"] == "ready", case["symbol"]
        assert plan["amount_usd"] == case["expected"], case["symbol"]
        assert plan["simulated_buffer_pct"] >= 25.0
        assert plan["next_gate_plan"]["required_add_usd"] == 0.0


def test_on_demand_full_next_fill_survives_old_stop_and_arms_following_step(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V4_ON_DEMAND,
    )
    gateway = FakePumpGateway()
    gateway.balance.update(
        {"total": 3_000.0, "wallet": 3_000.0, "available": 3_000.0}
    )
    gateway.liq_after_add_sequence = [18.0]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    armed_at = int(controller.arm(ARM_CONFIRMATION_V2)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1)])

    first = controller.run_cycle()
    item = first["positions"][0]
    second_leg = item["legs"][1]
    fill_price = float(second_leg["trigger_price"])
    stop_before_fill = float(item["stop_price"])
    projected_liq = float(item["margin_prefund_plan"]["projected_liq_price"])
    projected_qty = float(item["margin_prefund_plan"]["projected_qty"])

    assert stop_before_fill / fill_price - 1.0 >= 0.12
    assert item["margin_prefund_verification"]["projected"]["ready"] is True
    for order in gateway.orders:
        if order.get("id") == second_leg["order_id"]:
            order.update(
                {
                    "status": "filled",
                    "filled": projected_qty - float(item["qty"]),
                    "average": fill_price,
                }
            )
    gateway.positions[0].update(
        {
            "qty": projected_qty,
            "avg_price": (
                float(item["qty"]) * float(item["avg_entry_price"])
                + (projected_qty - float(item["qty"])) * fill_price
            )
            / projected_qty,
            "mark_price": fill_price,
            "liq_price": projected_liq,
        }
    )
    third_leg = item["legs"][2]
    third_plan = ladder_prefund_plan(
        policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
        qty=projected_qty,
        current_liq_price=projected_liq,
        legs=item["legs"],
        target_leg=third_leg,
        leverage=3.0,
        stop_gap_from_liq_pct=2.5,
        safety_above_next_ladder_pct=12.0,
        final_fill_buffer_pct=20.0,
        maintenance_margin_rate=0.025,
        taker_fee_rate=0.00055,
        round_up_increment_usd=5.0,
        projected_reaction_buffer_pct=12.0,
    )
    factor = projected_qty * 1.025 * 1.00055
    gateway.liq_after_add_sequence = [
        projected_liq + float(third_plan["required_add_usd"]) / factor
    ]
    protection_count = len(gateway.protections)

    after_fill = controller.run_cycle()
    updated = after_fill["positions"][0]
    newly_synced_stops = [row[2] for row in gateway.protections[protection_count:]]

    assert updated["status"] == "open"
    assert updated["legs"][1]["status"] == "filled"
    assert updated["legs"][2]["status"] == "open"
    assert updated["ladder_gate_status"] == "ready"
    assert newly_synced_stops
    assert min(newly_synced_stops) / fill_price - 1.0 >= 0.12
    assert not any(op.startswith("market_reduce:") for op in gateway.operations)


def test_on_demand_bmt_three_leg_path_prefunds_12_pct_before_each_fill() -> None:
    config = risk_policy_config(
        RISK_POLICY_V3,
        PumpLiveConfig(margin_manager_policy_id=MARGIN_MANAGER_V4_ON_DEMAND),
    )
    legs = [
        {
            "step": 1,
            "trigger_price": 0.019848,
            "notional_usd": 300.0,
            "status": "filled",
        },
        {
            "step": 2,
            "trigger_price": 0.029772,
            "notional_usd": 600.0,
            "status": "planned",
        },
        {
            "step": 3,
            "trigger_price": 0.039696,
            "notional_usd": 900.0,
            "status": "planned",
        },
    ]
    qty = 15_117.0
    liq = 0.03155
    required_by_step: list[float] = []

    for target_leg in legs[1:]:
        plan = ladder_prefund_plan(
            policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
            qty=qty,
            current_liq_price=liq,
            legs=legs,
            target_leg=target_leg,
            leverage=config.leverage,
            stop_gap_from_liq_pct=config.exchange_stop_gap_from_liq_pct,
            safety_above_next_ladder_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
            final_fill_buffer_pct=config.projected_final_fill_buffer_pct,
            maintenance_margin_rate=config.entry_margin_prefund_mmr,
            taker_fee_rate=config.entry_margin_prefund_taker_fee_rate,
            round_up_increment_usd=config.entry_margin_prefund_round_usd,
            projected_reaction_buffer_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
        )
        required = float(plan["required_add_usd"])
        required_by_step.append(required)
        factor = (
            qty
            * (1.0 + config.entry_margin_prefund_mmr)
            * (1.0 + config.entry_margin_prefund_taker_fee_rate)
        )
        liq += required / factor
        fill_price = float(target_leg["trigger_price"])
        old_stop = liq * (1.0 - config.exchange_stop_gap_from_liq_pct / 100.0)
        assert old_stop / fill_price - 1.0 >= 0.12

        verified = ladder_prefund_plan(
            policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
            qty=qty,
            current_liq_price=liq,
            legs=legs,
            target_leg=target_leg,
            leverage=config.leverage,
            stop_gap_from_liq_pct=config.exchange_stop_gap_from_liq_pct,
            safety_above_next_ladder_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
            final_fill_buffer_pct=config.projected_final_fill_buffer_pct,
            maintenance_margin_rate=config.entry_margin_prefund_mmr,
            taker_fee_rate=config.entry_margin_prefund_taker_fee_rate,
            round_up_increment_usd=config.entry_margin_prefund_round_usd,
            projected_reaction_buffer_pct=(
                config.on_demand_fill_reaction_buffer_pct
            ),
        )
        assert verified["required_add_usd"] == 0.0
        projected_stop = float(verified["projected_stop_price"])
        assert projected_stop / fill_price - 1.0 >= 0.12
        qty = float(verified["projected_qty"])
        liq = float(verified["projected_liq_price"])
        target_leg["status"] = "filled"

    assert required_by_step == [45.0, 315.0]
    assert (liq / float(legs[-1]["trigger_price"]) - 1.0) * 100.0 >= 20.0


def test_risk_snapshot_remains_immutable_when_runtime_defaults_change() -> None:
    snapshot = risk_policy_snapshot(RISK_POLICY_V1, PumpLiveConfig())
    changed_runtime = PumpLiveConfig(
        deployable_capital_usd=2_100.0,
        reserve_usd=900.0,
        margin_topup_chunk_usd=75.0,
    )

    restored = config_from_risk_snapshot(snapshot, changed_runtime)

    assert restored.slot_margin_usd == 175.0
    assert restored.reserve_usd == 300.0
    assert restored.margin_topup_chunk_usd == 25.0


def test_capital_promotion_keeps_legacy_sizing_and_uses_available_v2_slots(
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
    controller.submit_decisions([ready_decision(armed_at + 1, symbol="OLDUSDT")])
    controller.run_cycle()
    legacy = next(item for item in controller.status()["positions"] if item["symbol"] == "OLDUSDT")
    assert sum(float(leg["margin_usd"]) for leg in legacy["legs"]) == 175.0

    gateway.balance.update(
        {"total": 3_000.0, "wallet": 3_000.0, "available": 2_800.0}
    )
    controller.record_temporary_transfer(
        direction="main_to_pump",
        amount_usd=2_000.0,
        transfer_id="capital-transfer",
    )
    promoted = controller.promote_strategy_capital(
        target_capital_usd=3_000.0,
        confirmation=CAPITAL_PROMOTE_CONFIRMATION,
        promotion_id="pump-capital-v2-3000",
    )
    assert controller.status()["entry_armed"] is False
    v2_armed_at = int(controller.arm(ARM_CONFIRMATION_V2)["armed_at_ms"])
    controller.submit_decisions(
        [
            ready_decision(v2_armed_at + 1, symbol="NEWUSDT", event_id="v2-entry"),
            ready_decision(v2_armed_at + 2, symbol="EXTRAUSDT", event_id="v2-extra"),
        ]
    )
    status = controller.run_cycle()

    assert promoted["active_risk_policy_id"] == RISK_POLICY_V2
    assert promoted["last_capital_promotion_amount_usd"] == 2_000.0
    assert status["active_risk_policy"]["slot_margin_usd"] == 525.0
    legacy = next(item for item in status["positions"] if item["symbol"] == "OLDUSDT")
    new = next(item for item in status["positions"] if item["symbol"] == "NEWUSDT")
    assert legacy["risk_policy_id"] == RISK_POLICY_V1
    assert sum(float(leg["margin_usd"]) for leg in legacy["legs"]) == 175.0
    assert new["risk_policy_id"] == RISK_POLICY_V2
    assert sum(float(leg["margin_usd"]) for leg in new["legs"]) == 525.0
    extra = next(item for item in status["positions"] if item["symbol"] == "EXTRAUSDT")
    assert extra["risk_policy_id"] == RISK_POLICY_V2
    assert sum(float(leg["margin_usd"]) for leg in extra["legs"]) == 525.0
    assert status["pending_signals"] == []

    restarted = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    with pytest.raises(ValueError, match="arm_confirmation_invalid"):
        restarted.arm(ARM_CONFIRMATION)
    resumed = restarted.arm(ARM_CONFIRMATION_V2)
    assert resumed["entry_armed"] is True
    assert resumed["active_risk_policy"]["policy_id"] == RISK_POLICY_V2


def test_capital_promotion_counts_existing_profit_before_external_principal(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.balance.update(
        {"total": 1_087.0, "wallet": 1_087.0, "available": 1_087.0}
    )
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller.set_strategy_capital(1_087.0, CAPITAL_SET_CONFIRMATION)
    controller.arm(ARM_CONFIRMATION)
    gateway.balance.update(
        {"total": 3_087.0, "wallet": 3_087.0, "available": 3_087.0}
    )
    controller.record_temporary_transfer(
        direction="main_to_pump",
        amount_usd=2_000.0,
        transfer_id="round-capital-transfer",
    )

    result = controller.promote_strategy_capital(
        target_capital_usd=3_000.0,
        confirmation=CAPITAL_PROMOTE_CONFIRMATION,
        promotion_id="pump-capital-v2-3000",
    )

    assert result["last_capital_promotion_amount_usd"] == 1_913.0
    assert result["external_strategy_contribution_usd"] == 1_913.0
    assert result["temporary_transfer_outstanding_usd"] == 87.0
    assert result["effective_strategy_capital_usd"] == 3_000.0
    assert controller.status()["entry_armed"] is False


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


def test_on_demand_capital_regime_uses_account_free_cash_bands() -> None:
    config = risk_policy_config(
        RISK_POLICY_V3,
        PumpLiveConfig(margin_manager_policy_id=MARGIN_MANAGER_V4_ON_DEMAND),
    )
    state = {
        "last_balance": {"wallet": 3_000.0, "available": 750.0},
        "capital_manager": {"active_risk_policy_id": RISK_POLICY_V2},
        "positions": [{"status": "open", "symbol": "SAFEUSDT", "liq_buffer_pct": 60.0}],
    }

    warning = build_capital_regime_status(state, config)
    state["last_balance"]["available"] = 450.0
    stress = build_capital_regime_status(state, config)
    state["last_balance"]["available"] = 250.0
    emergency = build_capital_regime_status(state, config)

    assert warning["cash_mode"] == "warning"
    assert warning["available_free_pct"] == 25.0
    assert stress["cash_mode"] == "stress"
    assert emergency["cash_mode"] == "emergency"


def test_on_demand_cash_thresholds_are_normalized_in_descending_order(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    env_path.write_text(
        "\n".join(
            [
                "PUMP_LIVE_ACCOUNT_ENTRY_FREE_PCT=15",
                "PUMP_LIVE_ACCOUNT_WARNING_FREE_PCT=30",
                "PUMP_LIVE_ACCOUNT_STRESS_FREE_PCT=25",
            ]
        ),
        encoding="utf-8",
    )

    config = load_pump_live_config(env_path)

    assert config.account_entry_free_pct == 15.0
    assert config.account_warning_free_pct == 15.0
    assert config.account_stress_free_pct == 15.0


def test_exact_margin_target_restores_25_percent_buffer() -> None:
    required = required_margin_for_liq_buffer_usd(
        qty=100.0,
        current_liq_price=12.0,
        mark_price=10.0,
        target_buffer_pct=25.0,
        maintenance_margin_rate=0.025,
        taker_fee_rate=0.00055,
        round_up_increment_usd=5.0,
    )

    assert required == 55.0


def test_capital_regime_uses_active_v2_portfolio_reserve_across_mixed_positions() -> None:
    state = {
        "last_balance": {"wallet": 3_000.0, "available": 1_600.0},
        "capital_manager": {"active_risk_policy_id": RISK_POLICY_V2},
        "positions": [
            {"status": "open", "symbol": "OLDUSDT", "margin_topup_usd": 25.0},
            {"status": "open", "symbol": "NEWUSDT", "margin_topup_usd": 75.0},
        ],
    }

    result = build_capital_regime_status(state, PumpLiveConfig())

    assert result["active_risk_policy_id"] == RISK_POLICY_V2
    assert result["new_slot_required_available_usd"] == 1_325.0
    assert result["new_slot_headroom_usd"] == 275.0


def test_capital_rescue_shadow_prefers_profitable_position_near_take_profit() -> None:
    state = {
        "positions": [
            {
                "status": "open",
                "symbol": "THREATUSDT",
                "unrealized_pnl_usd": -10.0,
                "liq_buffer_pct": 14.0,
                "mark_price": 1.0,
                "tp_price": 0.8,
                "stop_price": 1.1,
            },
            {
                "status": "open",
                "symbol": "HEIUSDT",
                "unrealized_pnl_usd": 40.0,
                "liq_buffer_pct": 107.0,
                "mark_price": 0.1605,
                "tp_price": 0.1559,
                "stop_price": 0.3,
                "margin_topup_usd": 75.0,
                "legs": [{"status": "open"}, {"status": "filled"}],
            },
            {
                "status": "open",
                "symbol": "RATSUSDT",
                "unrealized_pnl_usd": 8.0,
                "liq_buffer_pct": 80.0,
                "mark_price": 1.0,
                "tp_price": 0.7,
                "stop_price": 1.5,
            },
        ]
    }

    result = build_capital_rescue_shadow(
        state,
        PumpLiveConfig(),
        threatened_symbol="THREATUSDT",
        required_usd=50.0,
    )

    assert result["mode"] == "shadow"
    assert result["execution_enabled"] is False
    assert result["recommended_donor"]["symbol"] == "HEIUSDT"
    assert result["recommended_donor"]["suggested_reduce_fraction"] == 0.25
    assert result["recommended_donor"]["remaining_ladder_orders"] == 1


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
        self.margin_add_attempts: list[tuple[str, float]] = []
        self.margin_add_failures: list[Exception] = []
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
        self.margin_add_attempts.append((symbol, amount_usd))
        if self.margin_add_failures:
            raise self.margin_add_failures.pop(0)
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
    margin_manager_policy: str = MARGIN_MANAGER_V2_CURRENT,
    auto_rescue_reduction: bool = False,
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
                f"PUMP_LIVE_MARGIN_MANAGER_POLICY={margin_manager_policy}",
                "PUMP_LIVE_PROJECTED_FINAL_FILL_BUFFER_PCT=20",
                "PUMP_LIVE_PROJECTED_EXCHANGE_CAP_REACTION_BUFFER_PCT=8",
                "PUMP_LIVE_ON_DEMAND_FILL_REACTION_BUFFER_PCT=12",
                "PUMP_LIVE_SHARED_RESCUE_FACILITY_CAP_USD=2000",
                (
                    "PUMP_LIVE_SHARED_MAX_POSITION_TOPUP_USD=5000"
                    if margin_manager_policy == MARGIN_MANAGER_V4_ON_DEMAND
                    else "PUMP_LIVE_SHARED_MAX_POSITION_TOPUP_USD=2000"
                ),
                "PUMP_LIVE_ACCOUNT_ENTRY_FREE_PCT=30",
                "PUMP_LIVE_ACCOUNT_WARNING_FREE_PCT=20",
                "PUMP_LIVE_ACCOUNT_STRESS_FREE_PCT=10",
                (
                    "PUMP_LIVE_AUTO_RESCUE_REDUCTION_ENABLED=1"
                    if auto_rescue_reduction
                    else "PUMP_LIVE_AUTO_RESCUE_REDUCTION_ENABLED=0"
                ),
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


def test_operator_prefund_uses_live_position_and_confirms_next_ladder(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 1_955.0})
    gateway.positions = [
        {
            "symbol": "BLUAIUSDT",
            "side": "short",
            "qty": 9_810.0,
            "avg_price": 0.02138577,
            "mark_price": 0.028026,
            "liq_price": 0.032313,
            "leverage": 3.0,
            "margin_mode": "isolated",
            "position_idx": 0,
            "unrealized_pnl": -65.0,
        }
    ]
    gateway.liq_after_add = 0.0378
    gateway.orders = [
        {"id": "l3", "symbol": "BLUAIUSDT", "status": "open"},
        {"id": "l4", "symbol": "BLUAIUSDT", "status": "open"},
    ]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "live_id": "bluai",
            "symbol": "BLUAIUSDT",
            "status": "open",
            "qty": 9_810.0,
            "mark_price": 0.028026,
            "liq_price": 0.032313,
            "margin_topup_usd": 50.0,
            "margin_prefund_floor_usd": 35.0,
            "legs": [
                {"step": 1, "status": "filled", "trigger_price": 0.017841},
                {"step": 2, "status": "filled", "trigger_price": 0.0267615},
                {"step": 3, "status": "open", "trigger_price": 0.035682, "order_id": "l3"},
                {"step": 4, "status": "open", "trigger_price": 0.0446025, "order_id": "l4"},
            ],
        }
    ]

    with pytest.raises(ValueError, match="pump_live_prefund_confirmation_invalid"):
        controller.prefund_next_ladder("BLUAIUSDT", "wrong")
    result = controller.prefund_next_ladder(
        "BLUAIUSDT",
        f"{PREFUND_NEXT_LADDER_CONFIRMATION_PREFIX} BLUAIUSDT",
    )

    assert result["status"] == "confirmed"
    assert result["step"] == 3
    assert result["amount_usd"] == 55.0
    assert result["position_topup_usd"] == 105.0
    assert result["margin_prefund_floor_usd"] == 105.0
    assert result["verification"]["ready"] is True
    assert gateway.margin_adds == [("BLUAIUSDT", 55.0)]
    assert gateway.protections[-1][2] == pytest.approx(0.0378 * 0.975)


def test_existing_multi_order_ladder_is_migrated_to_only_next_live(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 1_900.0})
    gateway.positions = [
        {
            "symbol": "BLUAIUSDT",
            "side": "short",
            "qty": 9_810.0,
            "avg_price": 0.02138577,
            "mark_price": 0.0281,
            "liq_price": 0.037698,
            "unrealized_pnl": -65.0,
        }
    ]
    gateway.orders = [
        {"id": "l3", "symbol": "BLUAIUSDT", "status": "open", "order_link_id": "FAPbluaiL3"},
        {"id": "l4", "symbol": "BLUAIUSDT", "status": "open", "order_link_id": "FAPbluaiL4"},
        {"id": "l5", "symbol": "BLUAIUSDT", "status": "open", "order_link_id": "FAPbluaiL5"},
    ]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "live_id": "bluai",
            "symbol": "BLUAIUSDT",
            "status": "open",
            "qty": 9_810.0,
            "mark_price": 0.0281,
            "liq_price": 0.037698,
            "margin_topup_usd": 105.0,
            "margin_prefund_floor_usd": 105.0,
            "risk_policy_id": RISK_POLICY_V1,
            "risk_policy": risk_policy_snapshot(RISK_POLICY_V1, PumpLiveConfig()),
            "legs": [
                {"step": 1, "status": "filled", "trigger_price": 0.017841},
                {"step": 2, "status": "filled", "trigger_price": 0.0267615},
                {"step": 3, "status": "open", "trigger_price": 0.035682, "order_id": "l3"},
                {"step": 4, "status": "open", "trigger_price": 0.0446025, "order_id": "l4"},
                {"step": 5, "status": "open", "trigger_price": 0.053523, "order_id": "l5"},
            ],
        }
    ]

    status = controller.run_cycle()
    item = status["positions"][0]

    assert gateway.canceled == ["l4", "l5"]
    assert [leg["status"] for leg in item["legs"]] == [
        "filled",
        "filled",
        "open",
        "planned",
        "planned",
    ]
    assert item["ladder_gate_status"] == "ready"
    assert item["ladder_gate_step"] == 3

    # A concurrent legacy monitor may observe the confirmed cancellations
    # before the new controller persists `planned`. Durable gate events are the
    # only authority allowed to restore those exact legs.
    for leg, old_order_id in zip(item["legs"][3:], ("l4", "l5"), strict=True):
        leg.update(
            {
                "status": "canceled",
                "order_id": old_order_id,
                "error": "ladder_order_no_longer_open",
            }
        )
    recovered = controller.run_cycle()["positions"][0]
    assert [leg["status"] for leg in recovered["legs"][3:]] == ["planned", "planned"]
    assert [leg["order_id"] for leg in recovered["legs"][3:]] == [None, None]


def test_legacy_position_uses_active_v2_margin_envelope_for_next_step(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 1_900.0})
    gateway.positions = [
        {
            "symbol": "BLUAIUSDT",
            "side": "short",
            "qty": 12_750.0,
            "avg_price": 0.0247,
            "mark_price": 0.03,
            "liq_price": 0.0386,
            "unrealized_pnl": -130.0,
        }
    ]
    gateway.liq_after_add = 0.0475
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "live_id": "bluai",
            "symbol": "BLUAIUSDT",
            "status": "open",
            "qty": 12_750.0,
            "mark_price": 0.03,
            "liq_price": 0.0386,
            "margin_topup_usd": 105.0,
            "margin_prefund_floor_usd": 105.0,
            "risk_policy_id": RISK_POLICY_V1,
            "risk_policy": risk_policy_snapshot(RISK_POLICY_V1, PumpLiveConfig()),
            "legs": [
                {"step": 1, "status": "filled", "trigger_price": 0.017841},
                {"step": 2, "status": "filled", "trigger_price": 0.0267615},
                {"step": 3, "status": "filled", "trigger_price": 0.035682},
                {"step": 4, "status": "planned", "trigger_price": 0.0446025, "notional_usd": 105.0},
                {"step": 5, "status": "planned", "trigger_price": 0.053523, "notional_usd": 105.0},
            ],
        }
    ]

    status = controller.run_cycle()
    item = status["positions"][0]

    assert gateway.margin_adds == [("BLUAIUSDT", 110.0)]
    assert item["margin_topup_usd"] == 215.0
    assert item["margin_topup_usd"] > 175.0
    assert item["margin_prefund_floor_usd"] == 215.0
    assert item["margin_continuation_policy_id"] == RISK_POLICY_V2
    assert item["legs"][3]["status"] == "open"
    assert item["legs"][4]["status"] == "planned"


def test_next_ladder_is_deferred_when_margin_envelope_is_exhausted(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.positions = [
        {
            "symbol": "TESTUSDT",
            "side": "short",
            "qty": 20.0,
            "avg_price": 12.0,
            "mark_price": 10.0,
            "liq_price": 16.0,
            "unrealized_pnl": 40.0,
        }
    ]
    gateway.orders = [
        {"id": "l3", "symbol": "TESTUSDT", "status": "open", "order_link_id": "FAPtestL3"}
    ]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "live_id": "test",
            "symbol": "TESTUSDT",
            "status": "open",
            "qty": 20.0,
            "mark_price": 10.0,
            "liq_price": 16.0,
            "margin_topup_usd": 175.0,
            "margin_prefund_floor_usd": 175.0,
            "risk_policy_id": RISK_POLICY_V1,
            "risk_policy": risk_policy_snapshot(RISK_POLICY_V1, PumpLiveConfig()),
            "legs": [
                {"step": 1, "status": "filled", "trigger_price": 10.0},
                {"step": 2, "status": "filled", "trigger_price": 15.0},
                {"step": 3, "status": "open", "trigger_price": 20.0, "order_id": "l3"},
            ],
        }
    ]

    status = controller.run_cycle()
    item = status["positions"][0]

    assert gateway.margin_adds == []
    assert gateway.canceled == ["l3"]
    assert item["legs"][2]["status"] == "planned"
    assert item["ladder_gate_status"] == "blocked"
    assert status["blocked_reason"] == "next_ladder_margin_not_confirmed"
    assert status["last_error"] is None


def test_next_ladder_prefund_uses_guarded_main_transfer_only_for_cash_gap(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, entry_cap=4, prefund_enabled=True)
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 100.0})
    gateway.positions = [
        {
            "symbol": "BLUAIUSDT",
            "side": "short",
            "qty": 12_750.0,
            "avg_price": 0.0247,
            "mark_price": 0.03,
            "liq_price": 0.0386,
            "unrealized_pnl": -130.0,
        }
    ]
    gateway.liq_after_add = 0.0475
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "live_id": "bluai",
            "symbol": "BLUAIUSDT",
            "status": "open",
            "qty": 12_750.0,
            "mark_price": 0.03,
            "liq_price": 0.0386,
            "margin_topup_usd": 105.0,
            "margin_prefund_floor_usd": 105.0,
            "risk_policy_id": RISK_POLICY_V1,
            "risk_policy": risk_policy_snapshot(RISK_POLICY_V1, PumpLiveConfig()),
            "legs": [
                {"step": 1, "status": "filled", "trigger_price": 0.017841},
                {"step": 2, "status": "filled", "trigger_price": 0.0267615},
                {"step": 3, "status": "filled", "trigger_price": 0.035682},
                {"step": 4, "status": "planned", "trigger_price": 0.0446025, "notional_usd": 105.0},
            ],
        }
    ]
    calls: list[dict[str, Any]] = []

    def transfer(**kwargs: Any) -> dict[str, Any]:
        calls.append(dict(kwargs))
        amount = float(kwargs["requested_usd"])
        gateway.balance["available"] += amount
        return {"status": "complete", "amount_usd": amount, "transfer_id": "risk-transfer"}

    controller.set_risk_transfer_provider(transfer)

    status = controller.run_cycle()
    item = status["positions"][0]

    assert calls[0]["symbol"] == "BLUAIUSDT"
    assert calls[0]["requested_usd"] == 85.0
    assert gateway.margin_adds == [("BLUAIUSDT", 110.0)]
    assert item["legs"][3]["status"] == "open"
    assert any(
        event["event"] == "next_ladder_transfer_complete"
        for event in status["recent_events"]
    )


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
    assert open_items[0]["risk_policy_id"] == RISK_POLICY_V1
    assert open_items[0]["risk_policy"]["slot_margin_usd"] == 175.0
    assert len(open_items) == 1
    assert open_items[0]["symbol"] == "TESTUSDT"
    assert len(open_items[0]["legs"]) == 3
    assert len(gateway.orders) == 1
    assert gateway.leverage_calls == [("TESTUSDT", 3.0)]
    assert gateway.take_profits[-1] == ("TESTUSDT", 7.5)
    assert gateway.protections[-1] == ("TESTUSDT", 7.5, 14.625)
    assert open_items[0]["stop_price"] == 14.625


def test_entry_persists_full_scanner_snapshot_in_state_and_events(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    state_dir = tmp_path / "state"
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    armed_at = int(controller.arm(ARM_CONFIRMATION)["armed_at_ms"])
    decision = ready_decision(armed_at + 1, symbol="AUDITUSDT", event_id="audit-1")
    decision.update(
        {
            "source_status": "entry_candidate",
            "source_reason": "eligible",
            "scan_ts_ms": armed_at + 1,
            "pullback_from_high_pct": 22.0,
            "funding_prev_24h_pct": -0.2,
            "oi_change_24h_pct": 12.0,
            "long_ratio": 0.52,
            "hours_since_trigger": 4.0,
            "scanner_snapshot": {
                "schema": "pump_signal_scanner_snapshot_v1",
                "symbol": "AUDITUSDT",
                "event_id": "audit-1",
                "return_24h_pct": 32.5,
                "trigger_pump_pct": 120.0,
                "pullback_from_high_pct": 22.0,
                "funding_prev_24h_pct": -0.2,
                "oi_change_4h_pct": 4.5,
                "oi_change_24h_pct": 12.0,
                "long_ratio": 0.52,
                "premium_latest_pct": -0.11,
                "premium_min_24h_pct": -0.42,
                "premium_relief_1h_pct": 0.18,
                "volume_z_24h": 3.7,
                "data_quality": {"funding": "ok", "open_interest": "ok"},
            },
        }
    )

    assert controller.submit_decisions([decision])["accepted"] == 1
    pending = controller.status()["pending_signals"][0]
    assert pending["scanner_snapshot"] == decision["scanner_snapshot"]

    status = controller.run_cycle()
    position = next(
        item
        for item in status["positions"]
        if item["symbol"] == "AUDITUSDT" and item["status"] != "closed"
    )
    assert position["open_decision"]["scanner_snapshot"] == decision["scanner_snapshot"]
    assert position["open_decision"]["funding_prev_24h_pct"] == -0.2
    assert position["open_decision"]["oi_change_24h_pct"] == 12.0

    events = [
        json.loads(line)
        for line in controller.events_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    queued = next(item for item in events if item["event"] == "signals_queued")
    opened = next(item for item in events if item["event"] == "live_position_opened")
    assert queued["decisions"][0]["scanner_snapshot"] == decision["scanner_snapshot"]
    assert opened["open_decision"]["scanner_snapshot"] == decision["scanner_snapshot"]

    recovered = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=state_dir,
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    ).status()
    recovered_position = next(
        item for item in recovered["positions"] if item["symbol"] == "AUDITUSDT"
    )
    assert recovered_position["open_decision"]["scanner_snapshot"] == decision[
        "scanner_snapshot"
    ]


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
    assert len(gateway.orders) == 1
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
    assert len(gateway.orders) == 1


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
    assert len(gateway.orders) == 1
    assert [leg["status"] for leg in recovered["positions"][0]["legs"][1:]] == [
        "open",
        "planned",
    ]

    controller.arm(ARM_CONFIRMATION)
    assert len(gateway.orders) == 1


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


def test_prefund_keeps_five_leg_plan_but_only_next_ladder_live(tmp_path: Path) -> None:
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
    assert len(gateway.orders) == 1
    assert [leg["status"] for leg in item["legs"]] == [
        "filled",
        "open",
        "planned",
        "planned",
        "planned",
    ]
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


def test_margin_manager_versions_are_separate_and_current_is_default(
    tmp_path: Path,
) -> None:
    missing = load_pump_live_config(tmp_path / "missing.env")
    assert missing.margin_manager_policy_id == MARGIN_MANAGER_V2_CURRENT

    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    shared = load_pump_live_config(env_path)

    assert shared.margin_manager_policy_id == MARGIN_MANAGER_V3_SHARED
    assert shared.projected_final_fill_buffer_pct == 20.0
    assert shared.projected_exchange_cap_reaction_buffer_pct == 8.0
    assert shared.on_demand_fill_reaction_buffer_pct == 12.0
    assert shared.shared_rescue_facility_cap_usd == 2_000.0
    assert shared.shared_max_position_topup_usd == 2_000.0


def test_projected_prefund_uses_bluai_full_fill_and_following_ladder() -> None:
    legs = [
        {"step": 1, "trigger_price": 0.017841, "notional_usd": 105.0},
        {"step": 2, "trigger_price": 0.0267615, "notional_usd": 105.0},
        {"step": 3, "trigger_price": 0.035682, "notional_usd": 105.0},
        {"step": 4, "trigger_price": 0.0446025, "notional_usd": 105.0},
        {"step": 5, "trigger_price": 0.053523, "notional_usd": 105.0},
    ]

    plan = ladder_prefund_plan(
        policy_id=MARGIN_MANAGER_V3_SHARED,
        qty=9_810.0,
        current_liq_price=0.037698,
        legs=legs,
        target_leg=legs[2],
        leverage=3.0,
        stop_gap_from_liq_pct=2.5,
        safety_above_next_ladder_pct=2.5,
        final_fill_buffer_pct=20.0,
        maintenance_margin_rate=0.025,
        taker_fee_rate=0.00055,
        round_up_increment_usd=5.0,
    )

    assert plan["target_kind"] == "following_ladder"
    assert plan["target_leg_step"] == 3
    assert plan["following_leg_step"] == 4
    assert plan["reference_price"] == 0.0446025
    assert plan["required_add_usd"] == 95.0
    assert plan["projected_qty"] > 12_750
    assert plan["hard_target_enforced"] is True


def test_projected_manager_uses_exchange_cap_reaction_buffer_for_bluai(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    controller = PumpLiveController(
        gateway=FakePumpGateway(),
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    legs = [
        {"step": 1, "status": "filled", "trigger_price": 0.017841, "notional_usd": 105.0},
        {"step": 2, "status": "filled", "trigger_price": 0.0267615, "notional_usd": 105.0},
        {"step": 3, "status": "planned", "trigger_price": 0.035682, "notional_usd": 105.0},
        {"step": 4, "status": "planned", "trigger_price": 0.0446025, "notional_usd": 105.0},
        {"step": 5, "status": "planned", "trigger_price": 0.053523, "notional_usd": 105.0},
    ]
    item = {
        "symbol": "BLUAIUSDT",
        "qty": 9_810.0,
        "liq_price": 0.037698,
        "position_value_usd": 272.44332,
        "position_margin_usd": 175.23916387,
        "maintenance_margin_usd": 11.20543195,
        "legs": legs,
    }

    plan, check = controller._build_prefund_plan_and_check(  # pylint: disable=protected-access
        item,
        controller.config(),
        target_leg=legs[2],
    )

    assert check["ready"] is False
    assert plan["target_kind"] == "exchange_margin_cap_reaction_buffer"
    assert plan["exchange_margin_add_capacity_usd"] == 80.0
    assert plan["strict_required_add_usd"] == 95.0
    assert plan["required_add_usd"] == 20.0
    assert plan["clearance_pct"] == 8.0
    assert plan["hard_target_enforced"] is False

    item["liq_price"] = 0.0398
    item["position_margin_usd"] = 195.23916387
    verified_plan, verified = controller._build_prefund_plan_and_check(  # pylint: disable=protected-access
        item,
        controller.config(),
        target_leg=legs[2],
    )

    assert verified_plan["target_kind"] == "exchange_margin_cap_reaction_buffer"
    assert verified["ready"] is True
    assert verified["current"]["verified_clearance_pct"] >= 8.0
    assert verified["projected"]["ready"] is True


def test_on_demand_entry_reserves_only_first_action_and_excludes_rescue_cash(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V4_ON_DEMAND,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 3_000.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    config = risk_policy_config(RISK_POLICY_V3, controller.config())

    ready = controller._on_demand_entry_admission(  # pylint: disable=protected-access
        balance=gateway.balance,
        open_items=[],
        candidate_config=config,
        tier={"ladder_legs": 3, "ladder_step_pct": 50.0, "leg_weights": [1, 2, 3]},
    )

    assert ready["ready"] is True
    assert ready["new_slot_margin_usd"] == 600.0
    assert ready["new_first_leg_margin_usd"] == 100.0
    assert ready["new_next_order_margin_usd"] == 200.0
    assert ready["future_ladders_reserved"] is False
    assert ready["only_next_ladder_reserved"] is True
    assert ready["new_action_cash_usd"] < 600.0

    controller._state["capital_manager"][  # pylint: disable=protected-access
        "temporary_transfer_outstanding_usd"
    ] = 2_700.0
    blocked = controller._on_demand_entry_admission(  # pylint: disable=protected-access
        balance=gateway.balance,
        open_items=[],
        candidate_config=config,
        tier={"ladder_legs": 3, "ladder_step_pct": 50.0, "leg_weights": [1, 2, 3]},
    )

    assert blocked["ready"] is False
    assert blocked["reason"] == "ondemand_post_action_free_below_entry_floor"
    assert blocked["temporary_rescue_cash_excluded_usd"] == 2_700.0


def test_on_demand_manager_keeps_old_snapshot_and_sizes_only_new_entry_at_600(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=False,
        margin_manager_policy=MARGIN_MANAGER_V4_ON_DEMAND,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 3_000.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "live_id": "old",
            "symbol": "OLDUSDT",
            "status": "closed",
            "risk_policy_id": RISK_POLICY_V2,
            "risk_policy": risk_policy_snapshot(RISK_POLICY_V2, PumpLiveConfig()),
            "legs": build_live_legs(
                tier={"ladder_legs": 5, "leg_weights": [1, 1, 1, 1, 1]},
                slot_margin_usd=525.0,
                leverage=3.0,
                reference_price=1.0,
            ),
        }
    ]
    armed_at = int(controller.arm(ARM_CONFIRMATION_V2)["armed_at_ms"])
    controller.submit_decisions([ready_decision(armed_at + 1, symbol="NEWUSDT")])

    status = controller.run_cycle()
    old = next(row for row in status["positions"] if row["symbol"] == "OLDUSDT")
    new = next(row for row in status["positions"] if row["symbol"] == "NEWUSDT")

    assert sum(float(leg["margin_usd"]) for leg in old["legs"]) == 525.0
    assert new["risk_policy_id"] == RISK_POLICY_V3
    assert sum(float(leg["margin_usd"]) for leg in new["legs"]) == 600.0


def test_on_demand_cash_relief_cancels_only_entry_ladders(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, margin_manager_policy=MARGIN_MANAGER_V4_ON_DEMAND)
    gateway = FakePumpGateway()
    gateway.orders = [
        {"id": "a-l2", "symbol": "AUSDT", "status": "open"},
        {"id": "b-l2", "symbol": "BUSDT", "status": "open"},
    ]
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
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "symbol": "AUSDT",
            "status": "open",
            "legs": [
                {"step": 1, "status": "filled"},
                {"step": 2, "status": "open", "order_id": "a-l2", "margin_usd": 100.0},
            ],
        },
        {
            "symbol": "BUSDT",
            "status": "open",
            "legs": [
                {"step": 1, "status": "filled"},
                {"step": 2, "status": "open", "order_id": "b-l2", "margin_usd": 200.0},
            ],
        },
    ]

    result = controller._cancel_ladders_for_cash_relief(  # pylint: disable=protected-access
        threatened_symbol="AUSDT"
    )

    assert gateway.canceled == ["b-l2", "a-l2"]
    assert result["released_order_margin_usd"] == 300.0
    assert controller._state["account_ladders_paused"] is True  # pylint: disable=protected-access
    assert all(
        row["legs"][1]["status"] == "planned"
        for row in controller._state["positions"]  # pylint: disable=protected-access
    )
    assert any(title == "Pump Live CASH RELIEF AUSDT" for title, _ in notifier.messages)


def test_on_demand_rescue_closes_profitable_donor_before_loser(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        margin_manager_policy=MARGIN_MANAGER_V4_ON_DEMAND,
        auto_rescue_reduction=True,
    )
    gateway = FakePumpGateway()
    gateway.positions = [
        {"symbol": "DONORUSDT", "side": "short", "qty": 10.0},
        {"symbol": "THREATUSDT", "side": "short", "qty": 20.0},
    ]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    donor = {
        "live_id": "donor",
        "symbol": "DONORUSDT",
        "status": "open",
        "qty": 10.0,
        "mark_price": 10.0,
        "tp_price": 9.0,
        "stop_price": 14.0,
        "liq_buffer_pct": 50.0,
        "unrealized_pnl_usd": 25.0,
        "legs": [{"step": 1, "status": "filled"}],
    }
    threat = {
        "live_id": "threat",
        "symbol": "THREATUSDT",
        "status": "open",
        "qty": 20.0,
        "mark_price": 10.0,
        "tp_price": 8.0,
        "stop_price": 11.0,
        "liq_buffer_pct": 12.0,
        "unrealized_pnl_usd": -200.0,
        "legs": [{"step": 1, "status": "filled"}],
    }
    controller._state["positions"] = [donor, threat]  # pylint: disable=protected-access

    result = controller._initiate_capital_rescue_reduction(  # pylint: disable=protected-access
        threatened=threat,
        config=controller.config(),
        required_usd=100.0,
    )

    assert result is True
    assert donor["status"] == "closing"
    assert threat["status"] == "open"
    assert "market_reduce:DONORUSDT" in gateway.operations
    assert controller._state["entry_armed"] is False  # pylint: disable=protected-access
    assert controller._state["blocked_reason"] == "capital_rescue_profitable_donor"  # pylint: disable=protected-access


@pytest.mark.parametrize(
    ("legs_count", "weights", "expected_immediate", "expected_full_path"),
    [
        (5, [1, 1, 1, 1, 1], 310.0, 1_445.0),
        (3, [1, 2, 3], 290.0, 530.0),
        (2, [1, 2], 310.0, 310.0),
    ],
)
def test_shared_entry_gates_immediate_safety_and_reports_full_path_stress(
    tmp_path: Path,
    legs_count: int,
    weights: list[int],
    expected_immediate: float,
    expected_full_path: float,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 3_000.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    config = risk_policy_config(RISK_POLICY_V2, controller.config())

    result = controller._shared_entry_admission(  # pylint: disable=protected-access
        balance=gateway.balance,
        open_items=[],
        candidate_config=config,
        tier={
            "ladder_legs": legs_count,
            "ladder_step_pct": 50.0,
            "leg_weights": weights,
        },
    )

    assert result["ready"] is True
    assert result["new_slot_margin_usd"] == 525.0
    assert result["new_initial_safety_usd"] == expected_immediate
    assert result["new_full_path_safety_usd"] == expected_full_path
    assert result["main_funds_count_for_entry"] is False


def test_shared_entry_blocks_when_temporary_rescue_cash_creates_false_headroom(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 1_200.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "temporary_transfer_outstanding_usd": 500.0,
    }
    config = risk_policy_config(RISK_POLICY_V2, controller.config())

    result = controller._shared_entry_admission(  # pylint: disable=protected-access
        balance=gateway.balance,
        open_items=[],
        candidate_config=config,
        tier={
            "ladder_legs": 5,
            "ladder_step_pct": 50.0,
            "leg_weights": [1, 1, 1, 1, 1],
        },
    )

    assert result["ready"] is False
    assert result["reason"] == "shared_projected_cash_below_new_slot"
    assert result["rescue_only_temporary_lock_usd"] == 500.0
    assert result["entry_headroom_usd"] < 0


def test_shared_entry_blocks_third_slot_when_two_positions_own_future_margin(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 1_500.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    config = risk_policy_config(RISK_POLICY_V2, controller.config())
    open_items = []
    for symbol in ("ONEUSDT", "TWOUSDT"):
        open_items.append(
            {
                "symbol": symbol,
                "qty": 100.0,
                "liq_price": 5.0,
                "margin_topup_usd": 0.0,
                "legs": [
                    {"step": 1, "status": "filled", "trigger_price": 1.0, "margin_usd": 105.0},
                    {"step": 2, "status": "open", "trigger_price": 1.5, "notional_usd": 105.0, "margin_usd": 105.0},
                    {"step": 3, "status": "planned", "trigger_price": 2.0, "notional_usd": 105.0, "margin_usd": 105.0},
                    {"step": 4, "status": "planned", "trigger_price": 2.5, "notional_usd": 105.0, "margin_usd": 105.0},
                    {"step": 5, "status": "planned", "trigger_price": 3.0, "notional_usd": 105.0, "margin_usd": 105.0},
                ],
            }
        )

    result = controller._shared_entry_admission(  # pylint: disable=protected-access
        balance=gateway.balance,
        open_items=open_items,
        candidate_config=config,
        tier={
            "ladder_legs": 5,
            "ladder_step_pct": 50.0,
            "leg_weights": [1, 1, 1, 1, 1],
        },
    )

    assert result["ready"] is False
    assert result["reason"] == "shared_projected_cash_below_new_slot"
    assert result["planned_base_margin_usd"] == 630.0
    assert result["required_available_usd"] > gateway.balance["available"]


def test_shared_entry_blocks_fourth_slot_when_rescue_envelope_is_consumed(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 3_000.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    config = risk_policy_config(RISK_POLICY_V2, controller.config())
    open_items = [
        {
            "symbol": f"P{index}USDT",
            "qty": 100.0,
            "liq_price": 2.0,
            "margin_topup_usd": 900.0,
            "legs": [
                {
                    "step": 1,
                    "status": "filled",
                    "trigger_price": 1.0,
                    "margin_usd": 525.0,
                }
            ],
        }
        for index in range(3)
    ]

    result = controller._shared_entry_admission(  # pylint: disable=protected-access
        balance=gateway.balance,
        open_items=open_items,
        candidate_config=config,
        tier={
            "ladder_legs": 5,
            "ladder_step_pct": 50.0,
            "leg_weights": [1, 1, 1, 1, 1],
        },
    )

    assert result["ready"] is False
    assert result["reason"] == "shared_projected_topup_cap_below_new_slot"
    assert result["desired_total_topup_usd"] == 3_010.0
    assert result["max_total_topup_usd"] == 2_825.0


def test_margin_manager_switch_is_runtime_dynamic_for_legacy_position_snapshot() -> None:
    old_snapshot = risk_policy_snapshot(
        RISK_POLICY_V1,
        PumpLiveConfig(margin_manager_policy_id=MARGIN_MANAGER_V2_CURRENT),
    )
    active = config_from_risk_snapshot(
        old_snapshot,
        PumpLiveConfig(
            margin_manager_policy_id=MARGIN_MANAGER_V4_ON_DEMAND,
            projected_final_fill_buffer_pct=25.0,
            projected_exchange_cap_reaction_buffer_pct=12.0,
            on_demand_fill_reaction_buffer_pct=14.0,
            shared_rescue_facility_cap_usd=600.0,
            shared_max_position_topup_usd=1_100.0,
        ),
    )

    assert active.margin_manager_policy_id == MARGIN_MANAGER_V4_ON_DEMAND
    assert active.projected_final_fill_buffer_pct == 25.0
    assert active.projected_exchange_cap_reaction_buffer_pct == 12.0
    assert active.on_demand_fill_reaction_buffer_pct == 14.0
    assert active.shared_rescue_facility_cap_usd == 600.0
    assert active.shared_max_position_topup_usd == 1_100.0
    assert active.total_capital_usd == 1_000.0


def test_shared_projected_admission_blocks_order_before_market_submission(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 3_000.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    armed_at = int(controller.arm(ARM_CONFIRMATION_V2)["armed_at_ms"])
    decision = ready_decision(armed_at + 1)
    decision["tier"] = {
        "rule_slug": "step50_legs5_equal_tp25_720",
        "ladder_step_pct": 50.0,
        "ladder_legs": 5,
        "leg_weights": [1, 1, 1, 1, 1],
        "tp_pct": 25.0,
        "max_hold_h": 720,
    }
    assert controller.submit_decisions([decision])["accepted"] == 1
    gateway.balance["available"] = 850.0

    status = controller.run_cycle()

    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "shared_projected_cash_below_new_slot"
    assert status["positions"] == []
    assert not any(operation.startswith("market_sell:") for operation in gateway.operations)


def test_shared_projected_gate_cancels_old_stop_race_then_recreates_l3(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        entry_cap=4,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 2_000.0})
    gateway.positions = [
        {
            "symbol": "BLUAIUSDT",
            "side": "short",
            "qty": 9_810.0,
            "avg_price": 0.02138577,
            "mark_price": 0.0277,
            "liq_price": 0.037698,
            "unrealized_pnl": -60.0,
        }
    ]
    gateway.orders = [
        {
            "id": "bluai-l3-old",
            "symbol": "BLUAIUSDT",
            "status": "open",
            "filled": 0.0,
            "average": None,
            "reduce_only": False,
        }
    ]
    gateway.liq_after_add_sequence = [0.047, 0.0471]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    controller._state["positions"] = [  # pylint: disable=protected-access
        {
            "live_id": "bluai",
            "symbol": "BLUAIUSDT",
            "status": "open",
            "qty": 9_810.0,
            "avg_entry_price": 0.02138577,
            "mark_price": 0.0277,
            "liq_price": 0.037698,
            "margin_topup_usd": 105.0,
            "margin_prefund_floor_usd": 105.0,
            "risk_policy_id": RISK_POLICY_V1,
            "risk_policy": risk_policy_snapshot(RISK_POLICY_V1, controller.config()),
            "tier": {"tp_pct": 25.0},
            "opened_at_ms": int(time.time() * 1000),
            "legs": [
                {"step": 1, "status": "filled", "trigger_price": 0.017841, "notional_usd": 105.0},
                {"step": 2, "status": "filled", "trigger_price": 0.0267615, "notional_usd": 105.0},
                {"step": 3, "status": "open", "trigger_price": 0.035682, "notional_usd": 105.0, "margin_usd": 35.0, "order_id": "bluai-l3-old"},
                {"step": 4, "status": "planned", "trigger_price": 0.0446025, "notional_usd": 105.0, "margin_usd": 35.0},
                {"step": 5, "status": "planned", "trigger_price": 0.053523, "notional_usd": 105.0, "margin_usd": 35.0},
            ],
        }
    ]

    status = controller.run_cycle()
    item = status["positions"][0]

    assert status["last_error"] is None
    assert gateway.canceled == ["bluai-l3-old"]
    assert gateway.margin_adds == [("BLUAIUSDT", 95.0), ("BLUAIUSDT", 5.0)]
    assert item["margin_topup_usd"] == 205.0
    assert item["margin_prefund_floor_usd"] == 205.0
    assert item["margin_continuation_policy_id"] == MARGIN_MANAGER_V3_SHARED
    assert item["margin_prefund_verification"]["ready"] is True
    assert item["margin_prefund_verification"]["projected"]["ready"] is True
    assert item["legs"][2]["status"] == "open"
    assert item["legs"][2]["order_id"] != "bluai-l3-old"


def test_bybit_margin_cap_rejection_switches_once_to_ready_fallback(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 2_200.0})
    gateway.positions = [
        {
            "symbol": "BLUAIUSDT",
            "side": "short",
            "qty": 9_810.0,
            "avg_price": 0.02138577,
            "mark_price": 0.0281,
            "liq_price": 0.039656,
            "position_value_usd": 300.0,
            "position_margin_usd": 195.0,
            "maintenance_margin_usd": 11.0,
            "unrealized_pnl": -60.0,
        }
    ]
    gateway.orders = [
        {
            "id": "bluai-l3-old",
            "symbol": "BLUAIUSDT",
            "status": "open",
            "reduce_only": False,
        }
    ]
    gateway.margin_add_failures = [
        RuntimeError(
            'bybit {"retCode":10001,"retMsg":"can not set pm more than pv"}'
        )
    ]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    item = {
        "live_id": "bluai-cap",
        "symbol": "BLUAIUSDT",
        "status": "open",
        "qty": 9_810.0,
        "avg_entry_price": 0.02138577,
        "mark_price": 0.0281,
        "liq_price": 0.039656,
        "position_value_usd": 300.0,
        "position_margin_usd": 195.0,
        "maintenance_margin_usd": 11.0,
        "margin_topup_usd": 125.0,
        "margin_prefund_floor_usd": 125.0,
        "tier": {"tp_pct": 25.0},
        "legs": [
            {"step": 1, "status": "filled", "trigger_price": 0.017841},
            {"step": 2, "status": "filled", "trigger_price": 0.0267615},
            {
                "step": 3,
                "status": "open",
                "trigger_price": 0.035682,
                "notional_usd": 105.0,
                "margin_usd": 35.0,
                "order_id": "bluai-l3-old",
            },
            {"step": 4, "status": "planned", "trigger_price": 0.0446025},
            {"step": 5, "status": "planned", "trigger_price": 0.053523},
        ],
    }
    controller._state["positions"] = [item]  # pylint: disable=protected-access

    controller._maintain_ladder_gate(  # pylint: disable=protected-access
        item,
        controller._position_config(item, controller.config()),  # pylint: disable=protected-access
    )

    assert gateway.margin_add_attempts == [("BLUAIUSDT", 75.0)]
    assert gateway.margin_adds == []
    assert item["ladder_gate_status"] == "ready"
    assert item["margin_prefund_status"] == "already_protected_exchange_cap"
    assert item["margin_prefund_verification"]["ready"] is True
    assert item["margin_prefund_plan"]["exchange_cap_rejection_active"] is True
    assert item["legs"][2]["status"] == "open"
    assert item["legs"][2]["order_id"] != "bluai-l3-old"
    assert sum(
        row["event"] == "exchange_margin_cap_rejection"
        for row in controller.status()["recent_events"]
    ) == 1


def test_bybit_margin_cap_rejection_defers_without_repeated_write_or_event(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        prefund_enabled=True,
        margin_manager_policy=MARGIN_MANAGER_V3_SHARED,
    )
    gateway = FakePumpGateway()
    gateway.positions = [
        {
            "symbol": "TESTUSDT",
            "side": "short",
            "qty": 100.0,
            "avg_price": 10.0,
            "mark_price": 10.0,
            "liq_price": 15.0,
            "position_value_usd": 3_000.0,
            "position_margin_usd": 100.0,
            "maintenance_margin_usd": 10.0,
            "unrealized_pnl": 0.0,
        }
    ]
    gateway.margin_add_failures = [
        RuntimeError(
            'bybit {"retCode":10001,"retMsg":"can not set pm more than pv"}'
        )
    ]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    item = {
        "live_id": "cap-deferred",
        "symbol": "TESTUSDT",
        "status": "open",
        "qty": 100.0,
        "avg_entry_price": 10.0,
        "mark_price": 10.0,
        "liq_price": 15.0,
        "position_value_usd": 3_000.0,
        "position_margin_usd": 100.0,
        "maintenance_margin_usd": 10.0,
        "margin_topup_usd": 0.0,
        "legs": [
            {"step": 1, "status": "filled", "trigger_price": 10.0},
            {
                "step": 2,
                "status": "planned",
                "trigger_price": 14.0,
                "notional_usd": 100.0,
                "margin_usd": 33.333333,
            },
            {"step": 3, "status": "planned", "trigger_price": 20.0},
        ],
    }
    controller._state["positions"] = [item]  # pylint: disable=protected-access
    config = controller._position_config(item, controller.config())  # pylint: disable=protected-access

    controller._maintain_ladder_gate(item, config)  # pylint: disable=protected-access
    controller._maintain_ladder_gate(item, config)  # pylint: disable=protected-access

    assert len(gateway.margin_add_attempts) == 1
    assert gateway.margin_adds == []
    assert item["ladder_gate_status"] == "blocked"
    assert item["ladder_gate_error"] == "pump_live_exchange_margin_cap_deferred"
    events = controller.status()["recent_events"]
    assert sum(row["event"] == "exchange_margin_cap_rejection" for row in events) == 1
    assert sum(row["event"] == "next_ladder_gate_blocked" for row in events) == 1


def test_projected_reserve_uses_exchange_cap_fallback_step_by_step() -> None:
    legs = [
        {"step": 1, "status": "filled", "trigger_price": 10.0},
        {"step": 2, "status": "planned", "trigger_price": 14.0, "notional_usd": 100.0},
        {"step": 3, "status": "planned", "trigger_price": 20.0, "notional_usd": 100.0},
    ]
    strict = projected_ladder_margin_reserve(
        qty=100.0,
        current_liq_price=15.0,
        legs=legs,
        target_legs=legs[1:],
        leverage=3.0,
        stop_gap_from_liq_pct=2.5,
        safety_above_next_ladder_pct=2.5,
        final_fill_buffer_pct=20.0,
        maintenance_margin_rate=0.025,
        taker_fee_rate=0.00055,
        round_up_increment_usd=5.0,
        correction_steps=3,
    )
    capped = projected_ladder_margin_reserve(
        qty=100.0,
        current_liq_price=15.0,
        legs=legs,
        target_legs=legs[1:],
        leverage=3.0,
        stop_gap_from_liq_pct=2.5,
        safety_above_next_ladder_pct=2.5,
        final_fill_buffer_pct=20.0,
        maintenance_margin_rate=0.025,
        taker_fee_rate=0.00055,
        round_up_increment_usd=5.0,
        correction_steps=3,
        position_margin_usd=1_200.0,
        exchange_cap_reaction_buffer_pct=8.0,
    )

    assert capped["path_cap_ready"] is True
    assert capped["total_reserve_usd"] < strict["total_reserve_usd"]
    assert capped["steps"][0]["exchange_cap_fallback"] is True
    assert capped["steps"][0]["exchange_cap_ready"] is True


def test_shared_admission_prices_actual_two_three_and_five_leg_candidate(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, margin_manager_policy=MARGIN_MANAGER_V3_SHARED)
    gateway = FakePumpGateway()
    gateway.balance.update({"total": 3_000.0, "wallet": 3_000.0, "available": 3_000.0})
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["capital_manager"] = {  # pylint: disable=protected-access
        "active_risk_policy_id": RISK_POLICY_V2,
        "active_strategy_capital_usd": 3_000.0,
        "application_enabled": True,
    }
    config = controller._active_policy_config(controller.config())  # pylint: disable=protected-access
    results = []
    for count in (2, 3, 5):
        results.append(
            controller._shared_entry_admission(  # pylint: disable=protected-access
                balance=gateway.balance,
                open_items=[],
                candidate_config=config,
                tier={
                    "ladder_legs": count,
                    "ladder_step_pct": 50.0,
                    "leg_weights": [1.0] * count,
                },
            )
        )

    assert [row["candidate_ladder_legs"] for row in results] == [2, 3, 5]
    assert all(row["ready"] for row in results)
    assert [row["new_initial_safety_usd"] for row in results] == [460.0, 505.0, 310.0]
    assert [row["new_full_path_safety_usd"] for row in results] == [460.0, 840.0, 1_445.0]


def test_duplicate_canceled_ladder_link_is_reissued_with_new_generation(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, margin_manager_policy=MARGIN_MANAGER_V3_SHARED)
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    item = {
        "live_id": "duplicate-link",
        "symbol": "TESTUSDT",
        "status": "open_degraded",
        "last_error": "bybit 110072 OrderLinkedID is duplicate",
        "legs": [
            {"step": 1, "status": "filled"},
            {
                "step": 2,
                "status": "error",
                "notional_usd": 100.0,
                "trigger_price": 15.0,
                "error": "bybit 110072 OrderLinkedID is duplicate",
            },
            {
                "step": 3,
                "status": "error",
                "notional_usd": 100.0,
                "trigger_price": 20.0,
                "error": "bybit 110072 OrderLinkedID is duplicate",
            },
        ],
    }
    controller._state["positions"] = [item]  # pylint: disable=protected-access

    controller._recover_duplicate_ladder_links(  # pylint: disable=protected-access
        item,
        item["legs"],
    )
    errors = controller._place_planned_ladders(item)  # pylint: disable=protected-access

    assert errors == []
    assert item["status"] == "open"
    assert item["last_error"] is None
    assert item["legs"][1]["status"] == "open"
    assert item["legs"][1]["order_link_generation"] == 1
    assert item["legs"][1]["order_link_id"].endswith("L2R1")
    assert item["legs"][2]["status"] == "planned"
    assert item["legs"][2]["order_link_generation"] == 1
    assert len(gateway.orders) == 1


def test_duplicate_ladder_link_recovery_fails_closed_with_unknown_order(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path, margin_manager_policy=MARGIN_MANAGER_V3_SHARED)
    gateway = FakePumpGateway()
    gateway.orders = [
        {
            "id": "unknown-l2",
            "symbol": "TESTUSDT",
            "status": "open",
            "reduce_only": False,
        }
    ]
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    item = {
        "live_id": "duplicate-link",
        "symbol": "TESTUSDT",
        "status": "open_degraded",
        "legs": [
            {"step": 1, "status": "filled"},
            {
                "step": 2,
                "status": "error",
                "error": "bybit 110072 OrderLinkedID is duplicate",
            },
        ],
    }

    with pytest.raises(
        RuntimeError,
        match="pump_live_duplicate_link_recovery_unknown_open_order",
    ):
        controller._recover_duplicate_ladder_links(  # pylint: disable=protected-access
            item,
            item["legs"],
        )

    assert item["legs"][1]["status"] == "error"


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


def test_four_prefunded_positions_keep_one_margin_gated_ladder_each(tmp_path: Path) -> None:
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
    assert len(gateway.orders) == 4
    assert sum(item["margin_prefund_floor_usd"] for item in status["positions"]) == 100.0
    assert all(len(item["legs"]) == 3 for item in status["positions"])
    assert status["entry_armed"] is True


def test_flat_position_needs_two_cycles_then_cancels_ladder(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.closed_trade_summary = {"status": "complete"}
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
    assert len(gateway.canceled) == 1
    assert first["entry_armed"] is False
    assert first["blocked_reason"] == "position_absent_unconfirmed"
    second = controller.run_cycle()
    assert second["open_positions"] == 0
    assert len(gateway.canceled) == 1
    assert second["entry_armed"] is False
    assert second["close_recovery_healthy_cycles"] == 1
    recovered = controller.run_cycle()
    assert recovered["entry_armed"] is True
    assert recovered["blocked_reason"] is None
    assert recovered["status"] == "armed"
    assert any(
        row["event"] == "position_close_recovered"
        for row in recovered["recent_events"]
    )


def test_flat_position_does_not_override_prior_operator_disarm(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.closed_trade_summary = {"status": "complete"}
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
    controller.disarm("operator_disarm")
    gateway.positions = []

    controller.run_cycle()
    controller.run_cycle()
    status = controller.run_cycle()

    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "position_absent_unconfirmed"
    assert status["close_recovery_pending"] is False


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
    assert status["positions"][0]["risk_policy_id"] == RISK_POLICY_V1
    assert status["positions"][0]["risk_policy"]["max_position_topup_usd"] == 175.0


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


def test_critical_buffer_closes_even_immediately_after_topup(tmp_path: Path) -> None:
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


def test_warning_position_can_receive_consecutive_verified_topups_without_cooldown(
    tmp_path: Path,
) -> None:
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
    status = controller.run_cycle()

    assert gateway.margin_adds == [("TESTUSDT", 25.0), ("TESTUSDT", 25.0)]
    assert status["positions"][0]["margin_topup_usd"] == 50.0


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


def test_on_demand_margin_reduction_defers_without_exchange_trial(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(
        env_path,
        margin_manager_policy=MARGIN_MANAGER_V4_ON_DEMAND,
    )
    gateway = FakePumpGateway()
    controller = PumpLiveController(
        gateway=gateway,
        state_dir=tmp_path / "state",
        env_path=env_path,
        start_recovery_monitor=False,
        background_monitor=False,
    )
    item = {
        "symbol": "BLUAIUSDT",
        "status": "open",
        "qty": 9_810.0,
        "mark_price": 0.027909,
        "liq_price": 0.0378,
        "margin_topup_usd": 105.0,
        "margin_prefund_floor_usd": 105.0,
        "last_topup_at_ms": int(time.time() * 1000) - 1_900_000,
        "legs": [
            {"step": 1, "status": "filled", "trigger_price": 0.017841, "notional_usd": 105.0},
            {"step": 2, "status": "filled", "trigger_price": 0.0267615, "notional_usd": 105.0},
            {"step": 3, "status": "open", "trigger_price": 0.035682, "notional_usd": 105.0},
            {"step": 4, "status": "planned", "trigger_price": 0.0446025, "notional_usd": 105.0},
            {"step": 5, "status": "planned", "trigger_price": 0.053523, "notional_usd": 105.0},
        ],
    }
    controller._state["positions"] = [item]  # pylint: disable=protected-access
    config = replace(
        controller.config(),
        margin_reduce_trigger_buffer_pct=30.0,
        margin_reduce_cooldown_sec=0,
    )
    buffer_pct = (float(item["liq_price"]) / float(item["mark_price"]) - 1.0) * 100.0

    controller._maybe_reduce_bot_margin(item, config, buffer_pct)  # pylint: disable=protected-access
    controller._maybe_reduce_bot_margin(item, config, buffer_pct)  # pylint: disable=protected-access

    assert gateway.margin_removes == []
    assert gateway.margin_adds == []
    assert item["margin_reduce_confirm_count"] == 2
    assert item["margin_reduce_deferred_reason"] == "current_next_gate_not_ready"
    assert item["margin_reduce_plan"]["amount_usd"] == 0.0


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
    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "portfolio_risk_freeze"
    assert sum(item["margin_topup_usd"] for item in status["positions"]) == 200.0


def test_warning_freezes_entries_before_topup_and_drops_stale_pending_signal(
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
    controller.submit_decisions([ready_decision(armed_at + 1, event_id="first")])
    controller.run_cycle()
    controller.submit_decisions(
        [ready_decision(armed_at + 2, symbol="SECONDUSDT", event_id="stale")]
    )
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0
    gateway.operations.clear()

    status = controller.run_cycle()

    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "portfolio_risk_freeze"
    assert status["portfolio_risk_freeze_active"] is True
    assert status["portfolio_risk_freeze_symbol"] == "TESTUSDT"
    assert status["portfolio_risk_recovery_cycles"] == 0
    assert status["pending_signals"] == []
    assert status["open_positions"] == 1
    assert gateway.margin_adds == [("TESTUSDT", 25.0)]
    assert gateway.operations[0] == "add_margin:TESTUSDT:25.0"
    assert not any(operation == "market_sell:SECONDUSDT" for operation in gateway.operations)
    assert any(
        row["event"] == "portfolio_risk_freeze"
        for row in status["recent_events"]
    )


def test_warning_freeze_requires_two_calm_cycles_and_a_fresh_signal(
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
    controller.submit_decisions([ready_decision(armed_at + 1)])
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0
    controller.run_cycle()
    gateway.positions[0]["mark_price"] = 15.0 / 1.24
    gateway.positions[0]["liq_price"] = 15.0

    still_frozen = controller.run_cycle()
    gateway.positions[0]["mark_price"] = 15.0 / 1.26
    first_calm = controller.run_cycle()
    recovered = controller.run_cycle()

    assert still_frozen["entry_armed"] is False
    assert still_frozen["portfolio_risk_recovery_cycles"] == 0
    assert first_calm["entry_armed"] is False
    assert first_calm["portfolio_risk_recovery_cycles"] == 1
    assert recovered["entry_armed"] is True
    assert recovered["blocked_reason"] is None
    assert recovered["portfolio_risk_freeze_active"] is False
    assert recovered["pending_signals"] == []
    assert any(
        row["event"] == "portfolio_risk_recovered"
        for row in recovered["recent_events"]
    )

    new_armed_at = int(recovered["armed_at_ms"])
    queued = controller.submit_decisions(
        [
            ready_decision(
                new_armed_at + 1,
                symbol="SECONDUSDT",
                event_id="fresh-after-risk",
            )
        ]
    )
    opened = controller.run_cycle()
    assert queued["accepted"] == 1
    assert opened["open_positions"] == 2


def test_missing_exchange_position_never_counts_as_calm_recovery(tmp_path: Path) -> None:
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
    with controller._lock:  # pylint: disable=protected-access
        item = controller._state["positions"][0]  # pylint: disable=protected-access
        item["mark_price"] = 10.0
        item["liq_price"] = 15.0
    gateway.positions = []

    status = controller.run_cycle()

    assert status["entry_armed"] is False
    assert status["portfolio_risk_recovery_cycles"] == 0


def test_operator_disarm_is_never_overridden_by_risk_recovery(tmp_path: Path) -> None:
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
    controller.disarm("operator_disarm")
    gateway.positions[0]["mark_price"] = 10.0
    gateway.positions[0]["liq_price"] = 15.0

    controller.run_cycle()
    status = controller.run_cycle()

    assert status["entry_armed"] is False
    assert status["blocked_reason"] == "operator_disarm"
    assert status["portfolio_risk_freeze_active"] is False


def test_arm_rejects_tracked_position_inside_warning_band(tmp_path: Path) -> None:
    env_path = tmp_path / "pump_live.env"
    write_env(env_path)
    gateway = FakePumpGateway()
    gateway.preflight_existing_state_errors = True
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
    controller.disarm("operator_disarm")
    gateway.positions[0]["mark_price"] = 13.0
    gateway.positions[0]["liq_price"] = 15.0

    with pytest.raises(RuntimeError, match="arm_portfolio_risk_not_ready"):
        controller.arm(ARM_CONFIRMATION)

    status = controller.status()
    assert status["entry_armed"] is False
    assert status["portfolio_risk_freeze_active"] is True
    assert status["portfolio_risk_restore_armed"] is False


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


def test_bybit_positions_preserve_exchange_margin_capacity_fields(
    monkeypatch: Any,
) -> None:
    class PositionClient:
        @staticmethod
        def fetch_positions(
            symbols: Any,
            params: dict[str, Any],
        ) -> list[dict[str, Any]]:
            assert symbols is None
            assert params == {"category": "linear", "settleCoin": "USDT"}
            return [
                {
                    "symbol": "BLUAI/USDT:USDT",
                    "side": "short",
                    "contracts": 9_810.0,
                    "entryPrice": 0.02138577,
                    "markPrice": 0.027772,
                    "liquidationPrice": 0.037698,
                    "leverage": 3.0,
                    "marginMode": "isolated",
                    "unrealizedPnl": -62.0,
                    "info": {
                        "symbol": "BLUAIUSDT",
                        "positionValue": "272.44332",
                        "positionIM": "175.23916387",
                        "positionMM": "11.20543195",
                        "positionIdx": "0",
                    },
                }
            ]

    gateway = BybitPumpLiveGateway()
    monkeypatch.setattr(gateway, "_ensure_client", lambda: PositionClient())

    position = gateway.fetch_positions()[0]

    assert position["position_value_usd"] == 272.44332
    assert position["position_margin_usd"] == 175.23916387
    assert position["maintenance_margin_usd"] == 11.20543195


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
