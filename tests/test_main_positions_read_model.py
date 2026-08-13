from __future__ import annotations

from datetime import datetime, timezone

from webapp.main_positions_read_model import (
    build_main_positions_payload,
    compact_account_balances,
)


def test_builder_is_deterministic_and_uses_real_notional_fields() -> None:
    now = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    legs = [
        {
            "exchange": "binance",
            "side": "long",
            "quantity": 10.0,
            "current_notional": 120.0,
            "entry_notional": 100.0,
            "leverage": 3.0,
            "dist_to_liq_pct": 25.0,
        },
        {
            "exchange": "kucoin",
            "side": "short",
            "quantity": -10.0,
            "current_notional": 118.0,
            "entry_notional": 102.0,
            "leverage": 3.0,
            "dist_to_liq_pct": 18.0,
        },
    ]
    rows = [
        {
            "type": "summary",
            "symbol": "TESTUSDT",
            "long_exchange": "binance",
            "short_exchange": "kucoin",
            "long_legs_count": 1,
            "short_legs_count": 1,
            "unrealized_pnl": 2.0,
            "next_funding": "2026-08-13T13:00:00+00:00",
        }
    ]

    payload = build_main_positions_payload(
        status="ready",
        accounts_snapshot={"last_updated": "2026-08-13T12:00:00+00:00"},
        balances=[],
        rows=rows,
        grouped={"TESTUSDT": legs},
        now_utc=now,
    )

    card = payload["cards"][0]
    assert card["position_summary"]["current_exposure_usdt"] == 118.0
    assert card["position_summary"]["entry_exposure_usdt"] == 100.0
    assert card["minutes_to_next_funding"] == 60.0
    assert card["risk_level"] == "warn"


def test_compact_balances_preserves_exchange_error_status() -> None:
    rows = compact_account_balances(
        [{"exchange": "gate", "status": "partial", "error": "timeout"}]
    )

    assert rows == [
        {
            "exchange": "gate",
            "asset": "USDT",
            "total": None,
            "available": None,
            "used": None,
            "margin_ratio": None,
            "status": "partial",
            "error": "timeout",
            "updated_at": None,
        }
    ]
