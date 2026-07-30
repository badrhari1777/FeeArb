from __future__ import annotations

import json

from analysis_features.pump_live_margin_stress import (
    ShortPosition,
    build_bank_margin_stress,
    simulate_bank_rise,
    short_liquidation_price_usdt,
    write_bank_margin_stress,
)


def test_bank_formula_matches_observed_bybit_liquidation_price() -> None:
    position = ShortPosition(
        qty=1010.0,
        avg_entry_price=0.17180881,
        leverage=3.0,
        maintenance_margin_rate=0.025,
    )

    calculated = short_liquidation_price_usdt(position)

    assert abs(calculated - 0.22349) < 0.00001


def test_bank_needs_two_25_dollar_chunks_before_second_ladder() -> None:
    report = build_bank_margin_stress()
    summary = report["summary"]
    row_25 = next(
        row for row in report["margin_rows"] if row["extra_margin_usd"] == 25.0
    )
    row_50 = next(
        row for row in report["margin_rows"] if row["extra_margin_usd"] == 50.0
    )

    assert 25.0 < summary["required_extra_for_ladder_usd"] < 50.0
    assert summary["rounded_topup_for_ladder_usd"] == 50.0
    assert row_25["second_ladder_reachable_before_stop"] is False
    assert row_50["second_ladder_reachable_before_stop"] is True
    assert row_50["post_fill_stop_price"] > 0.30


def test_four_slots_fit_ladder_rescue_but_not_full_per_coin_rescue() -> None:
    report = build_bank_margin_stress()
    rows = {row["scenario"]: row for row in report["portfolio_rows"]}
    summary = report["summary"]

    assert rows["four_reach_second_ladder"]["required_usd"] == 900.0
    assert rows["four_reach_second_ladder"]["fits_1000"] is True
    assert rows["configured_total_topup_cap"]["required_usd"] == 975.0
    assert rows["configured_total_topup_cap"]["remaining_usd"] == 25.0
    assert rows["four_use_full_per_position_cap"]["required_usd"] == 1400.0
    assert rows["four_use_full_per_position_cap"]["fits_1000"] is False
    assert summary["full_rescue_shortfall_usd"] == 400.0
    assert summary["reserve_budget_check_usd"] == 300.0


def test_report_writes_reproducible_artifacts(tmp_path) -> None:
    report = write_bank_margin_stress(tmp_path)

    payload = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert payload["schema"] == report["schema"]
    assert (tmp_path / "bank_margin_levels.csv").exists()
    assert (tmp_path / "bank_prefund_comparison.csv").exists()
    assert (tmp_path / "tier_ladder_protection.csv").exists()
    assert (tmp_path / "portfolio_policy_comparison.csv").exists()
    assert (tmp_path / "portfolio_capacity.csv").exists()
    assert (tmp_path / "rise_scenarios.csv").exists()


def test_bank_rise_speed_scenarios_stop_before_liquidation() -> None:
    slow = simulate_bank_rise(duration_sec=6 * 3600)
    fast = simulate_bank_rise(duration_sec=60)
    spike = simulate_bank_rise(duration_sec=30)

    assert slow["ladder_filled"] is True
    assert slow["extra_margin_usd"] <= 175.0
    assert slow["closed"] is True
    assert slow["close_price"] < slow["final_liq_price"]
    assert all(
        event["buffer_before_pct"] <= 15.0
        for event in slow["events"]
        if event["event"] == "margin_added"
        and event["extra_margin_usd"] > 50.0
    )

    assert fast["ladder_filled"] is True
    assert fast["extra_margin_usd"] >= 50.0
    assert fast["closed"] is True
    assert fast["close_price"] < fast["final_liq_price"]

    assert spike["ladder_filled"] is False
    assert spike["extra_margin_usd"] == 0.0
    assert spike["close_reason"] == "exchange_stop"


def test_bank_prefund_tradeoff_preserves_only_sensible_four_position_levels() -> None:
    report = build_bank_margin_stress()
    rows = {
        row["upfront_topup_usd"]: row
        for row in report["bank_prefund_rows"]
    }

    assert rows[45.0]["l2_reachable_before_stop"] is True
    assert 0.0 < rows[45.0]["stop_clearance_above_l2_pct"] < 2.0
    assert 2.5 < rows[50.0]["stop_clearance_above_l2_pct"] < 3.5
    assert rows[50.0]["free_capital_after_four_usd"] == 100.0
    assert rows[60.0]["free_capital_after_four_usd"] == 60.0
    assert rows[75.0]["operating_floor_preserved"] is False
    assert (
        rows[60.0]["post_l2_stop_loss_usd"]
        > rows[50.0]["post_l2_stop_loss_usd"]
    )


def test_tier_ladder_protection_exposes_deep_ladder_cap_limit() -> None:
    report = build_bank_margin_stress()
    rows = {
        (row["tier"], row["filled_through_leg"]): row
        for row in report["tier_ladder_protection_rows"]
    }

    assert rows[("ordinary_lt80", 1)]["rounded_total_topup_usd"] == 30.0
    assert rows[("strong_80_100", 1)]["rounded_total_topup_usd"] == 50.0
    assert rows[("strong_100_250", 1)]["rounded_total_topup_usd"] == 25.0
    assert rows[("super_250_plus", 1)]["rounded_total_topup_usd"] == 50.0
    assert rows[("strong_100_250", 2)]["rounded_total_topup_usd"] == 95.0
    assert rows[("strong_100_250", 2)]["within_portfolio_topup_cap"] is True
    assert rows[("ordinary_lt80", 3)]["rounded_total_topup_usd"] == 195.0
    assert rows[("ordinary_lt80", 3)]["within_position_topup_cap"] is False
    assert rows[("ordinary_lt80", 3)]["within_portfolio_topup_cap"] is False


def test_rebudgeting_trade_size_retains_more_free_cash_and_reduces_tail_loss() -> None:
    report = build_bank_margin_stress()
    rows = {
        row["policy"]: row
        for row in report["portfolio_policy_rows"]
    }
    same_size = rows["same_size_prefund_50"]
    balanced = rows["rebudget_150_trade_plus_50_protection"]
    conservative = rows["rebudget_125_trade_plus_50_protection"]

    assert same_size["free_capital_at_four_positions_usd"] == 100.0
    assert balanced["free_capital_at_four_positions_usd"] == 200.0
    assert conservative["free_capital_at_four_positions_usd"] == 300.0
    assert balanced["l2_reachable_before_stop"] is True
    assert conservative["l2_reachable_before_stop"] is True
    assert (
        balanced["post_l2_stop_loss_usd"]
        < same_size["post_l2_stop_loss_usd"]
    )
    assert (
        conservative["post_l2_stop_loss_usd"]
        < balanced["post_l2_stop_loss_usd"]
    )
