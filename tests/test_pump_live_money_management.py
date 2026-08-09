from __future__ import annotations

import csv
import json
from pathlib import Path

from analysis_features.pump_live_money_management import (
    DEFAULT_POLICIES,
    current_wallet_snapshot,
    peak_concurrent_prefund_usd,
    policy_summary,
    target_capital_migration_snapshot,
    write_report,
)


def _trades() -> list[dict[str, object]]:
    return [
        {
            "strategy_id": "main_pullback_tier",
            "entry_ts": 100,
            "exit_ts": 300,
            "pump_pct": 60,
            "rule_slug": "step50_legs5_equal_tp25_720",
            "pnl_usd": 75,
        },
        {
            "strategy_id": "main_pullback_tier",
            "entry_ts": 200,
            "exit_ts": 400,
            "pump_pct": 120,
            "rule_slug": "step50_legs3_tapered_tp25_336",
            "pnl_usd": -30,
        },
    ]


def test_policy_budget_and_linear_replay() -> None:
    summary = policy_summary(_trades(), DEFAULT_POLICIES[0])

    assert summary["protected_capital_required_usd"] == 1000.0
    assert summary["unallocated_after_protection_usd"] == 0.0
    assert summary["historical_net_pnl_usd"] == 10.5
    assert summary["fits_total_capital"] is True


def test_peak_prefund_uses_concurrent_tier_requirements() -> None:
    assert peak_concurrent_prefund_usd(_trades()) == 55.0


def test_current_snapshot_exposes_legacy_double_reserve() -> None:
    state = {
        "positions": [
            {"status": "open", "margin_topup_usd": amount}
            for amount in (25.0, 35.0, 75.0)
        ]
    }

    snapshot = current_wallet_snapshot(
        state,
        wallet_total_usd=1043.862297,
        wallet_available_usd=380.957704,
    )

    assert snapshot["required_available_before_next_slot_usd"] == 340.0
    assert snapshot["next_slot_safe_by_dynamic_guard"] is True
    assert snapshot["legacy_static_guard_pass"] is False


def test_write_report_creates_reproducible_artifacts(tmp_path: Path) -> None:
    trades_path = tmp_path / "trades.csv"
    with trades_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_trades()[0]))
        writer.writeheader()
        writer.writerows(_trades())
    live_state = tmp_path / "state.json"
    live_state.write_text(json.dumps({"positions": []}), encoding="utf-8")

    result = write_report(
        historical_trades_path=trades_path,
        output_dir=tmp_path / "out",
        live_state_path=live_state,
        wallet_total_usd=1000.0,
        wallet_available_usd=1000.0,
    )

    assert result["research_only"] is True
    assert (tmp_path / "out" / "policy_summary.csv").exists()
    assert (tmp_path / "out" / "metadata.json").exists()


def test_three_legacy_positions_can_only_use_versioned_gradual_3000_migration() -> None:
    state = {
        "positions": [
            {"status": "open", "margin_topup_usd": amount}
            for amount in (25.0, 35.0, 75.0)
        ]
    }

    result = target_capital_migration_snapshot(
        state,
        wallet_total_usd=1043.862297,
        target_capital_usd=3000.0,
    )

    assert result["deposit_to_exact_target_usd"] == 1956.137703
    assert result["target_slot_margin_usd"] == 525.0
    assert result["target_reserve_usd"] == 900.0
    assert result["target_max_total_topup_usd"] == 825.0
    assert result["gradual_first_mixed_commitment_usd"] == 1950.0
    assert result["gradual_first_mixed_headroom_usd"] == 1050.0
    assert result["current_runtime_supports_mixed_cohorts"] is False
