from __future__ import annotations

from pathlib import Path

from analysis_features.pump_short_dynamic_combo_report import (
    filter_cases_by_start,
    parse_date_to_ms,
    parse_slots,
    select_top_combo_policies,
    topup_cashflows,
)


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_select_top_combo_policies_uses_unique_strict_then_capped(tmp_path: Path) -> None:
    write_text(
        tmp_path / "strict_live_candidate_policy_summary.csv",
        "\n".join(
            [
                "policy_slug,policy_mode,gate_slug,rule_slug,description",
                "p1,gate_override,pump_ge_80,r1,first",
                "p1,gate_override,pump_ge_80,r1,duplicate",
                "p2,gate_override,pump_ge_100,r2,second",
            ]
        ),
    )
    write_text(
        tmp_path / "capped_live_candidate_policy_summary.csv",
        "\n".join(
            [
                "policy_slug,policy_mode,gate_slug,rule_slug,description",
                "p2,gate_override,pump_ge_100,r2,duplicate",
                "p3,static,always,r3,third",
            ]
        ),
    )

    policies = select_top_combo_policies(tmp_path, combo_limit=3)

    assert [policy.slug for policy in policies] == ["p1", "p2", "p3"]
    assert policies[0].gate_slug == "pump_ge_80"


def test_topup_cashflows_adds_and_releases_external_cash() -> None:
    rows = topup_cashflows(
        combo_rank=1,
        summary={"policy_slug": "p1", "slots": 2, "sizing_mode": "dynamic"},
        trades=[
            {
                "symbol": "AAAUSDT",
                "case_id": "c1",
                "entry_ts": 1000,
                "entry_iso": "entry",
                "exit_ts": 2000,
                "exit_iso": "exit",
                "topup_usd": 150.0,
            }
        ],
    )

    assert [row["event"] for row in rows] == ["add_external_topup", "release_external_topup"]
    assert rows[0]["external_topup_open_usd"] == 150.0
    assert rows[1]["external_topup_open_usd"] == 0.0


def test_parse_start_date_slots_and_filter_cases() -> None:
    start = parse_date_to_ms("2024-01-01")
    assert start is not None
    assert parse_slots("1,2,3,4,5") == (1, 2, 3, 4, 5)
    assert filter_cases_by_start(
        [{"entry_ts": start - 1}, {"entry_ts": start}, {"entry_ts": start + 1}],
        start,
    ) == [{"entry_ts": start}, {"entry_ts": start + 1}]
