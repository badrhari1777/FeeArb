from __future__ import annotations

from importlib import import_module
from unittest.mock import patch

from fastapi.testclient import TestClient


webapp_app = import_module("webapp.app")


def test_transfer_status_and_preflight_are_read_only_delegates() -> None:
    client = TestClient(webapp_app.app)
    with (
        patch.object(
            webapp_app.bybit_pump_short_lab,
            "pump_transfer_status",
            return_value={"temporary_outstanding_usd": 0.0},
        ),
        patch.object(
            webapp_app.bybit_pump_short_lab,
            "pump_transfer_preflight",
            return_value={"ready": False, "errors": ["permission_missing"]},
        ),
    ):
        status = client.get("/api/pump-short/live/transfers")
        preflight = client.post("/api/pump-short/live/transfers/preflight")

    assert status.status_code == 200
    assert status.json()["temporary_outstanding_usd"] == 0.0
    assert preflight.status_code == 200
    assert preflight.json()["ready"] is False


def test_transfer_in_maps_confirmation_and_preflight_failures() -> None:
    client = TestClient(webapp_app.app)
    with patch.object(
        webapp_app.bybit_pump_short_lab,
        "pump_transfer_in",
        side_effect=ValueError("pump_temporary_transfer_in_confirmation_invalid"),
    ):
        invalid = client.post(
            "/api/pump-short/live/transfers/in",
            json={"amount_usdt": 0.01, "confirmation": "wrong"},
        )
    with patch.object(
        webapp_app.bybit_pump_short_lab,
        "pump_transfer_in",
        side_effect=RuntimeError("pump_temporary_transfer_round_trip_preflight_not_ready"),
    ):
        blocked = client.post(
            "/api/pump-short/live/transfers/in",
            json={
                "amount_usdt": 0.01,
                "confirmation": "TRANSFER TEMPORARY USDT MAIN TO PUMP",
            },
        )

    assert invalid.status_code == 400
    assert blocked.status_code == 409


def test_transfer_return_success_contract() -> None:
    client = TestClient(webapp_app.app)
    with patch.object(
        webapp_app.bybit_pump_short_lab,
        "pump_transfer_return",
        return_value={"operation": {"status": "complete", "amount_usd": 0.01}},
    ) as transfer:
        response = client.post(
            "/api/pump-short/live/transfers/return",
            json={
                "amount_usdt": 0.01,
                "confirmation": "RETURN TEMPORARY USDT PUMP TO MAIN",
            },
        )

    assert response.status_code == 200
    assert response.json()["operation"]["status"] == "complete"
    transfer.assert_called_once_with(
        0.01,
        "RETURN TEMPORARY USDT PUMP TO MAIN",
    )


def test_capital_promotion_api_is_explicit_and_maps_conflicts() -> None:
    client = TestClient(webapp_app.app)
    with patch.object(
        webapp_app.bybit_pump_short_lab,
        "pump_live_promote_strategy_capital",
        return_value={"operation": {"status": "complete", "amount_usd": 1912.2}},
    ) as promote:
        response = client.post(
            "/api/pump-short/live/capital/promote",
            json={
                "target_capital_usd": 3000.0,
                "confirmation": "PROMOTE PUMP CAPITAL 3000",
            },
        )
    with patch.object(
        webapp_app.bybit_pump_short_lab,
        "pump_live_promote_strategy_capital",
        side_effect=RuntimeError("pump_live_capital_promotion_principal_insufficient"),
    ):
        blocked = client.post(
            "/api/pump-short/live/capital/promote",
            json={
                "target_capital_usd": 3000.0,
                "confirmation": "PROMOTE PUMP CAPITAL 3000",
            },
        )

    assert response.status_code == 200
    assert response.json()["operation"]["status"] == "complete"
    promote.assert_called_once_with(3000.0, "PROMOTE PUMP CAPITAL 3000")
    assert blocked.status_code == 409
