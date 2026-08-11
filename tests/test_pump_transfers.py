from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from execution.pump_live import PumpLiveController
from execution.pump_transfers import (
    BybitPumpTransferGateway,
    PUMP_TRANSFER_IN_CONFIRMATION,
    PUMP_CAPITAL_PROMOTE_CONFIRMATION,
    PUMP_TRANSFER_RETURN_CONFIRMATION,
    PumpTemporaryTransferController,
)


class FakeTransferGateway:
    def __init__(self) -> None:
        self.balances = {
            "main": {
                "wallet_usd": 5000.0,
                "transfer_balance_usd": 5000.0,
                "transfer_safe_usd": 5000.0,
            },
            "pump": {
                "wallet_usd": 1043.86,
                "transfer_balance_usd": 380.95,
                "transfer_safe_usd": 380.95,
            },
        }
        self.records: dict[str, dict[str, Any]] = {}
        self.fail_create = False
        self.create_calls: list[dict[str, Any]] = []

    def credentials_status(self) -> dict[str, Any]:
        return {"ready": True, "master_key_source": "dedicated_pump_transfer"}

    def preflight(self) -> dict[str, Any]:
        return {
            "ready": True,
            "ready_in": True,
            "ready_out": True,
            "balances": self.fetch_balances(),
            "errors": [],
            "warnings": [],
            "minimum_test_usdt": 0.01,
        }

    def fetch_balances(self) -> dict[str, dict[str, Any]]:
        return {role: dict(balance) for role, balance in self.balances.items()}

    def create_transfer(
        self,
        *,
        direction: str,
        amount_usdt: str,
        transfer_id: str,
    ) -> dict[str, Any]:
        self.create_calls.append(
            {
                "direction": direction,
                "amount_usdt": amount_usdt,
                "transfer_id": transfer_id,
            }
        )
        if self.fail_create:
            raise TimeoutError("network outcome unknown")
        amount = float(amount_usdt)
        if direction == "main_to_pump":
            self.balances["main"]["wallet_usd"] -= amount
            self.balances["main"]["transfer_balance_usd"] -= amount
            self.balances["main"]["transfer_safe_usd"] -= amount
            self.balances["pump"]["wallet_usd"] += amount
            self.balances["pump"]["transfer_balance_usd"] += amount
            self.balances["pump"]["transfer_safe_usd"] += amount
        else:
            self.balances["pump"]["wallet_usd"] -= amount
            self.balances["pump"]["transfer_balance_usd"] -= amount
            self.balances["pump"]["transfer_safe_usd"] -= amount
            self.balances["main"]["wallet_usd"] += amount
            self.balances["main"]["transfer_balance_usd"] += amount
            self.balances["main"]["transfer_safe_usd"] += amount
        self.records[transfer_id] = {
            "transfer_id": transfer_id,
            "status": "SUCCESS",
            "coin": "USDT",
            "amount": amount_usdt,
            "direction": direction,
        }
        return {"transfer_id": transfer_id, "status": "SUCCESS"}

    def fetch_transfer(self, *, direction: str, transfer_id: str) -> dict[str, Any] | None:
        return self.records.get(transfer_id)


class FakeAccounting:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.temporary_outstanding_usd = 0.0
        self._status = {
            "positions": [
                {"status": "open", "margin_topup_usd": amount}
                for amount in (25.0, 35.0, 75.0)
            ],
            "config": {
                "total_capital_usd": 1000.0,
                "max_total_topup_usd": 275.0,
                "operating_cash_floor_usd": 25.0,
            },
            "last_balance": {
                "wallet": 1043.86,
                "total": 1043.86,
                "available": 380.95,
            },
            "capital_manager": {"active_strategy_capital_usd": 1000.0},
        }
        self.main_payload = {
            "status": "ready",
            "account_last_updated": datetime.now(timezone.utc).isoformat(),
            "balances": [
                {
                    "exchange": "bybit",
                    "total": 5000.0,
                    "available": 4000.0,
                    "used": 1000.0,
                    "margin_ratio": 0.2,
                    "status": "ok",
                }
            ],
            "cards": [
                {
                    "symbol": "MAINUSDT",
                    "liq_distance_pct": 40.0,
                    "legs": [
                        {"exchange": "bybit", "side": "long", "stop_price": 1.0}
                    ],
                }
            ],
        }

    def status(self) -> dict[str, Any]:
        return self._status

    def record_temporary_transfer(
        self,
        *,
        direction: str,
        amount_usd: float,
        transfer_id: str,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "direction": direction,
                "amount_usd": amount_usd,
                "transfer_id": transfer_id,
            }
        )
        delta = amount_usd if direction == "main_to_pump" else -amount_usd
        self.temporary_outstanding_usd += delta
        self._status["last_balance"]["wallet"] += delta
        self._status["last_balance"]["total"] += delta
        self._status["last_balance"]["available"] += delta
        return {"excluded_from_strategy_growth": True}

    def promote_strategy_capital(
        self,
        *,
        target_capital_usd: float,
        confirmation: str,
        promotion_id: str,
    ) -> dict[str, Any]:
        assert confirmation == PUMP_CAPITAL_PROMOTE_CONFIRMATION
        effective = (
            self._status["last_balance"]["wallet"]
            - self.temporary_outstanding_usd
        )
        promoted = max(0.0, target_capital_usd - effective)
        assert promoted <= self.temporary_outstanding_usd + 1e-9
        self.temporary_outstanding_usd -= promoted
        self._status["capital_manager"].update(
            {
                "active_strategy_capital_usd": target_capital_usd,
                "active_risk_policy_id": "v2_3000",
                "last_capital_promotion_amount_usd": promoted,
                "last_capital_promotion_id": promotion_id,
            }
        )
        return dict(self._status["capital_manager"])


def _controller(tmp_path: Path) -> tuple[PumpTemporaryTransferController, FakeTransferGateway, FakeAccounting]:
    gateway = FakeTransferGateway()
    accounting = FakeAccounting()
    controller = PumpTemporaryTransferController(
        accounting=accounting,
        gateway=gateway,
        state_dir=tmp_path,
        env_path=tmp_path / "pump.env",
        sleep=lambda _seconds: None,
        main_portfolio_provider=lambda: accounting.main_payload,
    )
    return controller, gateway, accounting


def _enable_auto_transfer(path: Path, **overrides: float) -> None:
    values = {
        "PUMP_LIVE_AUTO_TRANSFER_ENABLED": "1",
        "PUMP_LIVE_AUTO_TRANSFER_MAIN_MIN_AVAILABLE_USD": "500",
        "PUMP_LIVE_AUTO_TRANSFER_MAIN_MAX_MARGIN_RATIO": "0.75",
        "PUMP_LIVE_AUTO_TRANSFER_MAIN_MIN_LIQ_BUFFER_PCT": "25",
        "PUMP_LIVE_AUTO_TRANSFER_MAIN_MAX_DATA_AGE_SEC": "180",
        "PUMP_LIVE_AUTO_TRANSFER_MAX_INCIDENT_USD": "250",
        "PUMP_LIVE_AUTO_TRANSFER_FACILITY_CAP_USD": "2000",
        "PUMP_LIVE_AUTO_TRANSFER_DAILY_ALERT_USD": "500",
        "PUMP_LIVE_AUTO_TRANSFER_ROUND_USD": "5",
    }
    values.update({key: str(value) for key, value in overrides.items()})
    path.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()),
        encoding="utf-8",
    )


def test_auto_risk_transfer_rounds_up_and_tracks_daily_temporary_cash(tmp_path: Path) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    _enable_auto_transfer(tmp_path / "pump.env")

    result = controller.auto_transfer_for_risk(
        requested_usd=12.01,
        symbol="HEIUSDT",
        liq_buffer_pct=18.0,
        desired_topup_usd=25.0,
        available_usd=12.99,
    )

    assert result["status"] == "complete"
    assert result["amount_usd"] == 15.0
    assert gateway.create_calls[0]["amount_usdt"] == "15"
    operation = controller.status()["operations"][-1]
    assert operation["origin"] == "auto_risk"
    assert operation["context"]["symbol"] == "HEIUSDT"
    assert controller.status()["temporary_outstanding_usd"] == 15.0
    assert controller.status()["auto_risk"]["daily_used_usd"] == 15.0


def test_auto_risk_transfer_preserves_projected_main_available_floor(
    tmp_path: Path,
) -> None:
    controller, gateway, accounting = _controller(tmp_path)
    _enable_auto_transfer(tmp_path / "pump.env")
    gateway.balances["main"].update(
        {"wallet_usd": 2010.0, "transfer_balance_usd": 2010.0, "transfer_safe_usd": 2010.0}
    )
    accounting.main_payload["balances"][0].update(
        {"total": 2010.0, "available": 510.0, "used": 1500.0, "margin_ratio": 1500 / 2010}
    )

    result = controller.auto_transfer_for_risk(
        requested_usd=15.0,
        symbol="HEIUSDT",
        liq_buffer_pct=18.0,
        desired_topup_usd=25.0,
        available_usd=10.0,
    )

    assert result["status"] == "blocked"
    assert "main_projected" in result["reason"]
    assert gateway.create_calls == []


def test_auto_risk_transfer_allows_consecutive_confirmed_incidents_without_cooldown(
    tmp_path: Path,
) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    _enable_auto_transfer(tmp_path / "pump.env")
    first = controller.auto_transfer_for_risk(
        requested_usd=10.0,
        symbol="HEIUSDT",
        liq_buffer_pct=18.0,
        desired_topup_usd=25.0,
        available_usd=15.0,
    )
    second = controller.auto_transfer_for_risk(
        requested_usd=10.0,
        symbol="HEIUSDT",
        liq_buffer_pct=17.0,
        desired_topup_usd=25.0,
        available_usd=15.0,
    )

    assert first["status"] == "complete"
    assert second["status"] == "complete"
    assert len(gateway.create_calls) == 2
    assert first["transfer_id"] != second["transfer_id"]
    assert controller.status()["auto_risk"]["daily_used_usd"] == 20.0


def test_auto_risk_transfer_enforces_aggregate_2000_facility_cap(tmp_path: Path) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    _enable_auto_transfer(
        tmp_path / "pump.env",
        PUMP_LIVE_AUTO_TRANSFER_MAX_INCIDENT_USD=2000,
        PUMP_LIVE_AUTO_TRANSFER_FACILITY_CAP_USD=2000,
    )
    controller._state["temporary_outstanding_usd"] = 1_950.0  # pylint: disable=protected-access

    blocked = controller.auto_transfer_for_risk(
        requested_usd=55.0,
        symbol="HEIUSDT",
        liq_buffer_pct=18.0,
        desired_topup_usd=55.0,
        available_usd=0.0,
    )
    allowed = controller.auto_transfer_for_risk(
        requested_usd=50.0,
        symbol="HEIUSDT",
        liq_buffer_pct=18.0,
        desired_topup_usd=50.0,
        available_usd=0.0,
    )

    assert blocked["status"] == "blocked"
    assert blocked["reason"] == "temporary_rescue_facility_cap_exceeded"
    assert allowed["status"] == "complete"
    assert len(gateway.create_calls) == 1


def test_auto_risk_transfer_allows_watch_account_when_projection_stays_safe(
    tmp_path: Path,
) -> None:
    controller, gateway, accounting = _controller(tmp_path)
    _enable_auto_transfer(tmp_path / "pump.env")
    gateway.balances["main"].update(
        {"wallet_usd": 3000.0, "transfer_balance_usd": 1900.0, "transfer_safe_usd": 1900.0}
    )
    accounting.main_payload["balances"][0].update(
        {"total": 3000.0, "available": 1050.0, "used": 1950.0, "margin_ratio": 0.65}
    )
    accounting.main_payload["cards"][0]["liq_distance_pct"] = 30.0

    result = controller.auto_transfer_for_risk(
        requested_usd=150.0,
        symbol="BLUAIUSDT",
        liq_buffer_pct=14.0,
        desired_topup_usd=150.0,
        available_usd=25.0,
    )

    assert result["status"] == "complete"
    assert result["amount_usd"] == 150.0
    assert result["main_risk"]["current_margin_ratio"] == pytest.approx(0.65)
    assert result["main_risk"]["projected_margin_ratio"] == pytest.approx(1950 / 2850)


def test_auto_risk_transfer_blocks_when_any_main_position_is_undersecured(
    tmp_path: Path,
) -> None:
    controller, gateway, accounting = _controller(tmp_path)
    _enable_auto_transfer(tmp_path / "pump.env")
    accounting.main_payload["cards"][0]["liq_distance_pct"] = 24.9

    result = controller.auto_transfer_for_risk(
        requested_usd=50.0,
        symbol="BLUAIUSDT",
        liq_buffer_pct=14.0,
        desired_topup_usd=50.0,
        available_usd=25.0,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "main_position_liq_buffer_below_floor"
    assert gateway.create_calls == []


def test_daily_transfer_threshold_warns_but_does_not_block_rescue(tmp_path: Path) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    _enable_auto_transfer(
        tmp_path / "pump.env",
        PUMP_LIVE_AUTO_TRANSFER_DAILY_ALERT_USD=10,
    )

    result = controller.auto_transfer_for_risk(
        requested_usd=15.0,
        symbol="BLUAIUSDT",
        liq_buffer_pct=14.0,
        desired_topup_usd=15.0,
        available_usd=25.0,
    )

    assert result["status"] == "complete"
    assert "daily_transfer_alert_threshold_exceeded" in result["warnings"]
    assert len(gateway.create_calls) == 1


def test_gateway_prefers_dedicated_sub_transfer_credentials(tmp_path: Path) -> None:
    main_env = tmp_path / "main.env"
    pump_env = tmp_path / "pump.env"
    main_env.write_text("BYBIT_API_KEY=main-key\nBYBIT_API_SECRET=main-secret\n", encoding="utf-8")
    pump_env.write_text(
        "\n".join(
            (
                "BYBIT_PUMP_API_KEY=trading-key",
                "BYBIT_PUMP_API_SECRET=trading-secret",
                "BYBIT_PUMP_SUB_TRANSFER_API_KEY=transfer-key",
                "BYBIT_PUMP_SUB_TRANSFER_API_SECRET=transfer-secret",
            )
        ),
        encoding="utf-8",
    )
    gateway = BybitPumpTransferGateway(
        main_env_path=main_env,
        pump_env_path=pump_env,
    )

    key, secret, _testnet, source = gateway._credentials("pump")  # pylint: disable=protected-access

    assert (key, secret) == ("transfer-key", "transfer-secret")
    assert source == "dedicated_pump_sub_transfer"
    assert gateway.credentials_status()["pump_key_source"] == source


def _gateway_with_identities(
    tmp_path: Path,
    *,
    master_wallet: list[str],
    pump_wallet: list[str],
) -> BybitPumpTransferGateway:
    gateway = BybitPumpTransferGateway(
        main_env_path=tmp_path / "main.env",
        pump_env_path=tmp_path / "pump.env",
    )
    identities = {
        "master": {
            "isMaster": True,
            "readOnly": 0,
            "uta": 1,
            "userID": "100",
            "parentUid": "0",
            "permissions": {"Wallet": master_wallet},
        },
        "pump": {
            "isMaster": False,
            "readOnly": 0,
            "uta": 1,
            "userID": "200",
            "parentUid": "100",
            "permissions": {"Wallet": pump_wallet},
        },
    }

    class TransferCoinClient:
        @staticmethod
        def private_get_v5_asset_transfer_query_transfer_coin_list(
            _params: dict[str, Any],
        ) -> dict[str, Any]:
            raise AssertionError("same-account-type member transfer must skip this endpoint")

    gateway.credentials_status = lambda: {  # type: ignore[method-assign]
        "ready": True,
        "master_key_source": "dedicated_pump_transfer",
    }
    gateway._identity = lambda role, refresh=False: identities[role]  # type: ignore[method-assign]  # noqa: ARG005
    gateway._client = lambda _role: TransferCoinClient()  # type: ignore[method-assign]
    gateway._request = lambda _role, _operation, callback: callback()  # type: ignore[method-assign]
    gateway.fetch_balances = lambda: {  # type: ignore[method-assign]
        "main": {"wallet_usd": 5000.0, "transfer_safe_usd": 5000.0},
        "pump": {"wallet_usd": 1000.0, "transfer_safe_usd": 300.0},
    }
    return gateway


def test_gateway_requires_complete_least_privilege_wallet_permissions(tmp_path: Path) -> None:
    gateway = _gateway_with_identities(
        tmp_path,
        master_wallet=["AccountTransfer", "SubMemberTransfer"],
        pump_wallet=["AccountTransfer", "SubMemberTransferList"],
    )

    result = gateway.preflight()

    assert result["ready"] is True
    assert result["ready_in"] is True
    assert result["ready_out"] is True


def test_gateway_rejects_master_withdraw_permission(tmp_path: Path) -> None:
    gateway = _gateway_with_identities(
        tmp_path,
        master_wallet=["AccountTransfer", "SubMemberTransfer", "Withdraw"],
        pump_wallet=["AccountTransfer", "SubMemberTransferList"],
    )

    result = gateway.preflight()

    assert result["ready"] is False
    assert result["ready_in"] is False
    assert "pump_transfer_master_withdraw_permission_forbidden" in result["errors"]


def test_minimum_round_trip_is_confirmed_and_accounted(tmp_path: Path) -> None:
    controller, gateway, accounting = _controller(tmp_path)

    inbound = controller.transfer_in(0.01, PUMP_TRANSFER_IN_CONFIRMATION)
    returned = controller.transfer_return(0.01, PUMP_TRANSFER_RETURN_CONFIRMATION)

    assert inbound["operation"]["status"] == "complete"
    assert returned["operation"]["status"] == "complete"
    assert controller.status()["temporary_outstanding_usd"] == 0.0
    assert controller.status()["cumulative_in_usd"] == 0.01
    assert controller.status()["cumulative_returned_usd"] == 0.01
    assert [item["direction"] for item in accounting.calls] == [
        "main_to_pump",
        "pump_to_main",
    ]
    assert [item["amount_usdt"] for item in gateway.create_calls] == ["0.01", "0.01"]


def test_subcent_transfer_remainder_is_excluded_rounding_dust(tmp_path: Path) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    gateway.balances["pump"].update(
        {
            "wallet_usd": 3000.000036,
            "transfer_balance_usd": 100.0,
            "transfer_safe_usd": 100.0,
        }
    )
    controller._state["temporary_outstanding_usd"] = 87.804836  # pylint: disable=protected-access

    result = controller.transfer_return(87.8048, PUMP_TRANSFER_RETURN_CONFIRMATION)

    assert result["status"]["temporary_outstanding_usd"] == 0.0
    assert result["status"]["rounding_dust_usd"] == 0.000036


def test_existing_subcent_outstanding_migrates_to_rounding_dust(tmp_path: Path) -> None:
    (tmp_path / "temporary_transfers.json").write_text(
        '{"temporary_outstanding_usd": 0.000036}',
        encoding="utf-8",
    )

    controller, _gateway, _accounting = _controller(tmp_path)

    assert controller.status()["temporary_outstanding_usd"] == 0.0
    assert controller.status()["rounding_dust_usd"] == 0.000036


def test_inbound_requires_complete_round_trip_capability(tmp_path: Path) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    gateway.preflight = lambda: {  # type: ignore[method-assign]
        "ready": False,
        "ready_in": False,
        "ready_out": True,
        "balances": gateway.fetch_balances(),
        "errors": ["pump_transfer_master_permission_missing"],
        "warnings": [],
    }

    with pytest.raises(RuntimeError, match="round_trip_preflight_not_ready"):
        controller.transfer_in(0.01, PUMP_TRANSFER_IN_CONFIRMATION)

    assert gateway.create_calls == []


def test_return_cannot_exceed_temporary_principal(tmp_path: Path) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    controller.transfer_in(1.0, PUMP_TRANSFER_IN_CONFIRMATION)

    with pytest.raises(RuntimeError, match="return_exceeds_safe_limit"):
        controller.transfer_return(1.01, PUMP_TRANSFER_RETURN_CONFIRMATION)

    assert len(gateway.create_calls) == 1


def test_unknown_submission_is_persisted_and_blocks_duplicate(tmp_path: Path) -> None:
    controller, gateway, _accounting = _controller(tmp_path)
    gateway.fail_create = True

    with pytest.raises(RuntimeError, match="outcome_unknown"):
        controller.transfer_in(0.01, PUMP_TRANSFER_IN_CONFIRMATION)
    with pytest.raises(RuntimeError, match="round_trip_preflight_not_ready"):
        controller.transfer_in(0.01, PUMP_TRANSFER_IN_CONFIRMATION)

    assert controller.status()["pending"]["status"] == "outcome_unknown"
    assert len(gateway.create_calls) == 1


def test_manual_large_transfer_requires_safe_projected_main_account(tmp_path: Path) -> None:
    controller, gateway, accounting = _controller(tmp_path)
    accounting.main_payload["balances"][0]["available"] = 2_100.0

    with pytest.raises(RuntimeError, match="main_risk_gate_failed"):
        controller.transfer_in(2_000.0, PUMP_TRANSFER_IN_CONFIRMATION)

    assert gateway.create_calls == []


def test_capital_promotion_uses_only_required_principal_and_is_idempotent(
    tmp_path: Path,
) -> None:
    controller, _gateway, accounting = _controller(tmp_path)
    controller.transfer_in(2_000.0, PUMP_TRANSFER_IN_CONFIRMATION)

    result = controller.promote_capital(3_000.0, PUMP_CAPITAL_PROMOTE_CONFIRMATION)
    duplicate = controller.promote_capital(
        3_000.0,
        PUMP_CAPITAL_PROMOTE_CONFIRMATION,
    )

    assert result["operation"]["amount_usd"] == 1956.14
    assert result["status"]["temporary_outstanding_usd"] == 43.86
    assert result["status"]["cumulative_promoted_usd"] == 1956.14
    assert accounting._status["capital_manager"]["active_risk_policy_id"] == "v2_3000"
    assert duplicate["idempotent"] is True
    assert duplicate["status"]["temporary_outstanding_usd"] == 43.86


def test_success_record_must_match_expected_coin_and_amount(tmp_path: Path) -> None:
    controller, gateway, accounting = _controller(tmp_path)

    original_create = gateway.create_transfer

    def create_with_mismatched_record(**kwargs: Any) -> dict[str, Any]:
        result = original_create(**kwargs)
        gateway.records[kwargs["transfer_id"]]["amount"] = "0.02"
        return result

    gateway.create_transfer = create_with_mismatched_record  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="confirmation_mismatch"):
        controller.transfer_in(0.01, PUMP_TRANSFER_IN_CONFIRMATION)

    assert accounting.calls == []
    assert controller.status()["pending"]["status"] == "confirmation_mismatch"


def test_pump_capital_manager_excludes_temporary_cashflow_idempotently(tmp_path: Path) -> None:
    class MinimalPumpGateway:
        def credentials_status(self) -> dict[str, Any]:
            return {"ready": False}

    controller = PumpLiveController(
        gateway=MinimalPumpGateway(),  # type: ignore[arg-type]
        state_dir=tmp_path,
        env_path=tmp_path / "pump.env",
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["last_balance"] = {  # pylint: disable=protected-access
        "wallet": 1100.0,
        "total": 1100.0,
        "available": 1100.0,
    }

    first = controller.record_temporary_transfer(
        direction="main_to_pump",
        amount_usd=100.0,
        transfer_id="transfer-in-1",
    )
    duplicate = controller.record_temporary_transfer(
        direction="main_to_pump",
        amount_usd=100.0,
        transfer_id="transfer-in-1",
    )
    controller._state["last_balance"] = {  # pylint: disable=protected-access
        "wallet": 1000.0,
        "total": 1000.0,
        "available": 1000.0,
    }
    returned = controller.record_temporary_transfer(
        direction="pump_to_main",
        amount_usd=100.0,
        transfer_id="transfer-out-1",
    )

    assert first["effective_strategy_capital_usd"] == 1000.0
    assert duplicate["temporary_transfer_in_usd"] == 100.0
    assert returned["effective_strategy_capital_usd"] == 1000.0
    assert returned["temporary_transfer_outstanding_usd"] == 0.0
    assert returned["temporary_transfer_returned_usd"] == 100.0


def test_pump_capital_manager_keeps_subcent_dust_excluded(tmp_path: Path) -> None:
    class MinimalPumpGateway:
        def credentials_status(self) -> dict[str, Any]:
            return {"ready": False}

    state_dir = tmp_path / "state"
    controller = PumpLiveController(
        gateway=MinimalPumpGateway(),  # type: ignore[arg-type]
        state_dir=state_dir,
        env_path=tmp_path / "pump.env",
        start_recovery_monitor=False,
        background_monitor=False,
    )
    controller._state["last_balance"] = {  # pylint: disable=protected-access
        "wallet": 1087.804836,
        "total": 1087.804836,
        "available": 1087.804836,
    }
    controller.record_temporary_transfer(
        direction="main_to_pump",
        amount_usd=87.804836,
        transfer_id="dust-in",
    )
    controller._state["last_balance"] = {  # pylint: disable=protected-access
        "wallet": 1000.000036,
        "total": 1000.000036,
        "available": 1000.000036,
    }

    returned = controller.record_temporary_transfer(
        direction="pump_to_main",
        amount_usd=87.8048,
        transfer_id="dust-out",
    )

    assert returned["temporary_transfer_outstanding_usd"] == 0.0
    assert returned["temporary_transfer_rounding_dust_usd"] == 0.000036
    assert returned["equity_adjustment_usd"] == -0.000036
    assert returned["effective_strategy_capital_usd"] == 1000.0

    restarted = PumpLiveController(
        gateway=MinimalPumpGateway(),  # type: ignore[arg-type]
        state_dir=state_dir,
        env_path=tmp_path / "pump.env",
        start_recovery_monitor=False,
        background_monitor=False,
    )
    assert restarted.status()["capital_manager"]["temporary_transfer_outstanding_usd"] == 0.0
    assert restarted.status()["capital_manager"]["temporary_transfer_rounding_dust_usd"] == 0.000036
