from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from execution.pump_live import PumpLiveController
from execution.pump_transfers import (
    BybitPumpTransferGateway,
    PUMP_TRANSFER_IN_CONFIRMATION,
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
        self._status["last_balance"]["wallet"] += delta
        self._status["last_balance"]["total"] += delta
        self._status["last_balance"]["available"] += delta
        return {"excluded_from_strategy_growth": True}


def _controller(tmp_path: Path) -> tuple[PumpTemporaryTransferController, FakeTransferGateway, FakeAccounting]:
    gateway = FakeTransferGateway()
    accounting = FakeAccounting()
    controller = PumpTemporaryTransferController(
        accounting=accounting,
        gateway=gateway,
        state_dir=tmp_path,
        sleep=lambda _seconds: None,
    )
    return controller, gateway, accounting


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
            return {"result": {"list": ["USDT"]}}

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
