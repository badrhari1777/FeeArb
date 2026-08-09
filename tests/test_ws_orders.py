from __future__ import annotations

from execution.ws_orders import _kucoin_zero_fill_stop_event


def test_kucoin_triggered_stop_with_zero_fill_emits_critical_event() -> None:
    event = _kucoin_zero_fill_stop_event(
        {
            "orderId": "stop-1",
            "symbol": "TUTUSDTM",
            "side": "buy",
            "status": "done",
            "size": "11310",
            "remainSize": "11310",
            "stopTriggered": True,
            "stopPrice": "0.25710",
            "remark": "No counterparty orders available within the price protection range.",
            "ts": "1786259402037000000",
        },
        filled_qty=0.0,
    )

    assert event is not None
    assert event["severity"] == "critical"
    assert event["symbol"] == "TUTUSDTM"
    assert event["filled_qty"] == 0.0
    assert "price protection" in event["remark"]


def test_kucoin_triggered_stop_with_fill_is_not_failure() -> None:
    event = _kucoin_zero_fill_stop_event(
        {
            "orderId": "stop-2",
            "symbol": "TUTUSDTM",
            "stopTriggered": True,
        },
        filled_qty=11310.0,
    )

    assert event is None


def test_kucoin_untriggered_zero_fill_order_is_not_failure() -> None:
    event = _kucoin_zero_fill_stop_event(
        {
            "orderId": "limit-1",
            "symbol": "TUTUSDTM",
            "stopTriggered": False,
        },
        filled_qty=0.0,
    )

    assert event is None
