from __future__ import annotations

from analysis_features.pump_premium_event_pages import diagnose_event


def test_diagnose_event_marks_clean_take_profit() -> None:
    diagnosis = diagnose_event(
        {},
        {
            "net_pct": "29.82",
            "exit_reason": "take_profit",
            "entry_wait_h": "0",
            "entry_premium_pct": "-2",
            "entry_oi_change_4h_pct": "10",
        },
        {},
    )

    assert diagnosis == "clean_discount_squeeze_tp"


def test_diagnose_event_marks_toxic_premium_trap() -> None:
    diagnosis = diagnose_event(
        {},
        {
            "net_pct": "-25",
            "exit_reason": "stop_loss",
            "entry_wait_h": "0",
            "entry_premium_pct": "-8",
            "entry_oi_change_4h_pct": "10",
        },
        {},
    )

    assert diagnosis == "toxic_premium_trap"
