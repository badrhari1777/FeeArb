from __future__ import annotations

import tempfile
import time
import unittest
import asyncio
import json
from importlib import import_module
from pathlib import Path
from unittest.mock import patch

from analysis_collectors.bybit_pump_short import BybitInstrument
from webapp.bybit_pump_short_lab import (
    BybitPumpShortLab,
    PUMP_SHADOW_SCHEDULE_STATE_FILE,
    PUMP_STRATEGY_CATALOG,
    apply_pump_cycle_paper_rows,
    apply_pump_strategy_paper_rows,
    build_active_window_summary,
    build_pump_dashboard_state,
    build_pump_strategy_monitor_state,
    classify_strategy_signal,
    normalize_run_config,
    normalize_shadow_config,
    normalize_shadow_schedule_config,
    read_analysis_report,
    read_cycle_paper_summary,
    read_first_csv_rows,
    read_strategy_paper_summary,
    read_latest_csv_rows,
    select_active_window_rows,
)


class BybitPumpShortLabTestCase(unittest.TestCase):
    def test_start_returns_status_without_waiting_for_worker_lock(self) -> None:
        lab = BybitPumpShortLab(restore_shadow_schedule=False)
        config = normalize_run_config(max_symbols=1, sleep_sec=0.1)

        with patch.object(lab, "_run_worker", return_value=None):
            started = time.monotonic()
            status = lab.start(config)
            elapsed = time.monotonic() - started

        self.assertLess(elapsed, 1.0)
        self.assertEqual(status["status"], "starting")

    def test_web_monitor_selection_skips_non_crypto_by_default(self) -> None:
        lab = BybitPumpShortLab(restore_shadow_schedule=False)
        config = normalize_run_config(max_symbols=10, symbols=[])
        instruments = [
            make_instrument("ASMLUSDT", "ASML", 300),
            make_instrument("TQQQUSDT", "TQQQ", 200),
            make_instrument("SIRENUSDT", "SIREN", 100),
        ]

        selected = lab._select_instruments(instruments, config)  # pylint: disable=protected-access

        self.assertEqual([item.symbol for item in selected], ["SIRENUSDT"])

    def test_read_latest_csv_rows_returns_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "summary.csv"
            path.write_text("symbol,pump_score\nA,1\nB,2\nC,3\n", encoding="utf-8")

            rows = read_latest_csv_rows(path, limit=2)

        self.assertEqual([row["symbol"] for row in rows], ["B", "C"])

    def test_read_analysis_report_returns_top_rules(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / "analysis_metadata.json").write_text('{"events": 2, "episodes": 1}', encoding="utf-8")
            (output_dir / "candidate_rule_profiles.csv").write_text(
                "profile,profile_rank,entry_strategy\nconservative,1,A\nbalanced,1,B\n",
                encoding="utf-8",
            )
            (output_dir / "anti_overfit_report.csv").write_text(
                "anti_overfit_status,entry_strategy\nrobust_candidate,A\noverfit_risk,B\n",
                encoding="utf-8",
            )
            (output_dir / "best_rules.csv").write_text("rank,entry_strategy\n1,A\n2,B\n3,C\n", encoding="utf-8")
            (output_dir / "worst_tail_events.csv").write_text("symbol,mae_pct\nX,100\nY,90\n", encoding="utf-8")

            first = read_first_csv_rows(output_dir / "best_rules.csv", limit=2)
            report = read_analysis_report(output_dir)

        self.assertEqual([row["entry_strategy"] for row in first], ["A", "B"])
        self.assertEqual(report["metadata"]["events"], 2)
        self.assertEqual(report["candidate_profiles"][0]["profile"], "conservative")
        self.assertEqual(report["anti_overfit"][0]["anti_overfit_status"], "robust_candidate")
        self.assertEqual([row["entry_strategy"] for row in report["best_rules"][:2]], ["A", "B"])
        self.assertEqual(report["worst_tail_events"][0]["symbol"], "X")

    def test_shadow_status_reads_latest_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lab = BybitPumpShortLab(restore_shadow_schedule=False)
            cfg = normalize_shadow_config(output_dir=tmp, max_symbols=2)
            output_dir = Path(tmp)
            (output_dir / "shadow_metadata.json").write_text('{"rows": 1, "entry_candidates": 1}', encoding="utf-8")
            (output_dir / "shadow_scan_latest.csv").write_text(
                "status,symbol\nentry_candidate,TESTUSDT\n",
                encoding="utf-8",
            )
            (output_dir / "paper_positions.json").write_text(
                '{"schema": "bybit_pump_short_paper_v1", "positions": [{"status": "open", "symbol": "TESTUSDT"}]}',
                encoding="utf-8",
            )
            lab._shadow_state["config"]["output_dir"] = str(cfg.output_dir)  # pylint: disable=protected-access

            status = lab.shadow_status()

        self.assertEqual(status["metadata"]["entry_candidates"], 1)
        self.assertEqual(status["latest_rows"][0]["symbol"], "TESTUSDT")
        self.assertEqual(status["paper"]["open_positions"], 1)

    def test_shadow_schedule_runs_once_without_network_when_scanner_is_patched(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lab = BybitPumpShortLab(restore_shadow_schedule=False)
            config = normalize_shadow_schedule_config(
                output_dir=tmp,
                max_symbols=1,
                interval_sec=1,
                max_runs=1,
            )

            with patch(
                "webapp.bybit_pump_short_lab.run_shadow_scan",
                return_value={"rows": 1, "entry_candidates": 0, "requests_made": 3},
            ) as scanner:
                lab.start_shadow_schedule(config)
                thread = lab._shadow_schedule_thread  # pylint: disable=protected-access
                self.assertIsNotNone(thread)
                thread.join(timeout=2.0)

            status = lab.shadow_schedule_status()
            shadow = lab.shadow_status()
            persisted = json.loads((Path(tmp) / PUMP_SHADOW_SCHEDULE_STATE_FILE).read_text(encoding="utf-8"))

        self.assertFalse(thread.is_alive())
        self.assertEqual(status["status"], "complete")
        self.assertEqual(status["runs_started"], 1)
        self.assertEqual(status["runs_completed"], 1)
        self.assertEqual(status["runs_failed"], 0)
        self.assertEqual(status["last_metadata"]["requests_made"], 3)
        self.assertEqual(shadow["status"], "complete")
        self.assertFalse(persisted["enabled"])
        self.assertEqual(persisted["status"], "complete")
        scanner.assert_called_once()

    def test_shadow_schedule_restores_from_persisted_enabled_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / PUMP_SHADOW_SCHEDULE_STATE_FILE).write_text(
                json.dumps(
                    {
                        "enabled": True,
                        "status": "waiting",
                        "config": {
                            "output_dir": str(output_dir),
                            "lookback_days": 14,
                            "sleep_sec": 0.1,
                            "max_symbols": 1,
                            "symbols": [],
                            "newest_first": True,
                            "recent_event_hours": 168,
                            "interval_sec": 60,
                            "run_immediately": True,
                            "max_runs": None,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch("webapp.bybit_pump_short_lab.DEFAULT_SHADOW_OUTPUT_DIR", output_dir):
                with patch.object(BybitPumpShortLab, "_run_shadow_schedule_worker", return_value=None) as worker:
                    lab = BybitPumpShortLab()
                    thread = lab._shadow_schedule_thread  # pylint: disable=protected-access
                    if thread:
                        thread.join(timeout=1.0)

            status = lab.shadow_schedule_status()

        self.assertIsNotNone(thread)
        self.assertEqual(status["status"], "starting")
        self.assertTrue(status["enabled"])
        self.assertEqual(status["last_event"], "shadow schedule restored after restart")
        worker.assert_called_once()

    def test_dashboard_state_models_capital_steps_and_topup(self) -> None:
        shadow_status = {
            "status": "complete",
            "updated_at_ms": 1_900_010_800_000,
            "metadata": {"entry_candidates": 1},
            "latest_rows": [
                {
                    "status": "entry_candidate",
                    "symbol": "TESTUSDT",
                    "trigger_pump_pct": "120",
                    "pullback_from_high_pct": "22",
                    "funding_prev_24h_pct": "-0.2",
                    "reason": "matched_profile_conditions",
                }
            ],
            "paper": {
                "positions": [
                    {
                        "paper_id": "TEST",
                        "status": "open",
                        "symbol": "TESTUSDT",
                        "opened_at_ms": 1_900_000_000_000,
                        "updated_at_ms": 1_900_010_800_000,
                        "entry_price": 1.0,
                        "current_price": 2.2,
                        "remaining_weight": 1.0,
                        "realized_net_pct": 0.0,
                        "combined_net_pct": -120.18,
                        "mfe_pct": 0.0,
                        "mae_pct": 120.0,
                        "profile": "balanced",
                        "entry_strategy": "pb20_oi50_lr_mid_ladder4_step_50",
                        "exit_strategy": "tp25_full_168",
                        "last_snapshot": {
                            "funding_prev_24h_pct": "-0.2",
                            "oi_change_24h_pct": "12",
                            "pullback_from_high_pct": "22",
                        },
                    }
                ],
                "open_positions": 1,
                "closed_positions": 0,
            },
            "files": {},
        }

        dashboard = build_pump_dashboard_state(shadow_status, {"status": "waiting"})
        position = dashboard["positions"]["open"][0]

        self.assertEqual(dashboard["capital"]["active_open_positions"], 1)
        self.assertEqual(dashboard["capital"]["free_slots"], 2)
        self.assertAlmostEqual(dashboard["capital"]["per_step_notional_usd"], 250.0)
        self.assertEqual(position["filled_steps"], 3)
        self.assertAlmostEqual(position["avg_entry_price"], 1.5)
        self.assertGreater(position["current_topup_needed_usd"], 0.0)
        self.assertEqual(dashboard["signals"]["entry_candidates"][0]["symbol"], "TESTUSDT")

    def test_strategy_monitor_classifies_tiered_pumps(self) -> None:
        strategy = next(item for item in PUMP_STRATEGY_CATALOG if item["strategy_id"] == "main_pullback_tier")
        decision = classify_strategy_signal(
            strategy,
            {
                "status": "entry_candidate",
                "symbol": "SIRENUSDT",
                "event_id": "SIREN-1",
                "trigger_pump_pct": "140",
                "pullback_from_high_pct": "21",
                "funding_prev_24h_pct": "-0.2",
                "oi_change_24h_pct": "12",
                "long_ratio": "0.52",
                "last_close": "1.2",
            },
        )

        self.assertEqual(decision["state"], "entry_ready")
        self.assertEqual(decision["tier"]["rule_slug"], "step50_legs3_tapered_tp25_336")
        self.assertEqual(decision["tier"]["ladder_legs"], 3)

    def test_strategy_monitor_payload_and_audit_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            shadow_status = {
                "status": "complete",
                "updated_at_ms": 1_900_010_800_000,
                "config": {"output_dir": str(output_dir)},
                "metadata": {"ts_ms": 1_900_010_800_000, "rows": 1},
                "latest_rows": [
                    {
                        "status": "entry_candidate",
                        "symbol": "SIRENUSDT",
                        "event_id": "SIREN-1",
                        "trigger_pump_pct": "140",
                        "pullback_from_high_pct": "21",
                        "funding_prev_24h_pct": "-0.2",
                        "oi_change_24h_pct": "12",
                        "long_ratio": "0.52",
                        "last_close": "1.2",
                    }
                ],
                "paper": {"positions": [], "open_positions": 0, "closed_positions": 0},
                "files": {},
            }

            monitor = build_pump_strategy_monitor_state(shadow_status, {"status": "waiting"})
            lab = BybitPumpShortLab(restore_shadow_schedule=False)
            lab._write_strategy_monitor_audit_if_new(output_dir, monitor)  # pylint: disable=protected-access
            lab._write_strategy_monitor_audit_if_new(output_dir, monitor)  # pylint: disable=protected-access
            audit_lines = (output_dir / "strategy_monitor_audit.jsonl").read_text(encoding="utf-8").splitlines()

        main = next(item for item in monitor["strategies"] if item["strategy_id"] == "main_pullback_tier")
        self.assertEqual(monitor["schema"], "pump_short_strategy_monitor_v1")
        self.assertEqual(main["signals"]["counts"]["entry_ready"], 1)
        self.assertEqual(main["current_state"]["state"], "entry_ready")
        self.assertEqual(len(audit_lines), 1)

    def test_strategy_paper_opens_adds_topup_and_closes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            open_rows = [
                {
                    "ts_ms": "1900000000000",
                    "status": "entry_candidate",
                    "symbol": "SIRENUSDT",
                    "event_id": "SIREN-1",
                    "trigger_pump_pct": "140",
                    "pullback_from_high_pct": "21",
                    "funding_prev_24h_pct": "-0.2",
                    "oi_change_24h_pct": "12",
                    "long_ratio": "0.52",
                    "last_close": "1.0",
                }
            ]
            add_rows = [dict(open_rows[0], ts_ms="1900003600000", last_close="3.0")]
            close_rows = [dict(open_rows[0], ts_ms="1900007200000", last_close="1.0")]

            opened = apply_pump_strategy_paper_rows(open_rows, output_dir=output_dir)
            added = apply_pump_strategy_paper_rows(add_rows, output_dir=output_dir)
            closed = apply_pump_strategy_paper_rows(close_rows, output_dir=output_dir)
            summary = read_strategy_paper_summary(output_dir=output_dir)

        main_positions = [
            item for item in summary["positions"]
            if item.get("strategy_id") == "main_pullback_tier"
        ]
        main = main_positions[0]
        main_summary = summary["strategy_summaries"]["main_pullback_tier"]
        events = summary["events_latest"]

        self.assertGreaterEqual(opened["open_positions"], 1)
        self.assertGreater(added["current_topup_usd"], 0.0)
        self.assertGreaterEqual(closed["closed_positions"], 1)
        self.assertEqual(main["status"], "closed")
        self.assertEqual(main["exit_reason"], "target_25")
        self.assertGreater(main["realized_pnl_usd"], 0.0)
        self.assertGreater(main_summary["realized_pnl_usd"], 0.0)
        self.assertTrue(any(event.get("event") == "paper_add_leg" for event in events))
        self.assertTrue(any(event.get("event") == "paper_topup_peak" for event in events))
        self.assertTrue(any(event.get("event") == "paper_close" for event in events))

    def test_cycle_paper_opens_long_and_short_then_closes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            open_rows = [
                {
                    "ts_ms": "1900000000000",
                    "status": "entry_candidate",
                    "symbol": "LONGUSDT",
                    "event_id": "LONG-1",
                    "trigger_pump_pct": "80",
                    "hours_since_trigger": "1",
                    "premium_latest_pct": "-2.0",
                    "premium_min_24h_pct": "-3.0",
                    "premium_relief_1h_pct": "0.5",
                    "oi_change_4h_pct": "25",
                    "oi_change_24h_pct": "30",
                    "volume_z_24h": "2.0",
                    "last_close": "10.0",
                },
                {
                    "ts_ms": "1900000000000",
                    "status": "entry_candidate",
                    "symbol": "SHORTUSDT",
                    "event_id": "SHORT-1",
                    "trigger_pump_pct": "140",
                    "pullback_from_high_pct": "21",
                    "funding_prev_24h_pct": "-0.2",
                    "oi_change_24h_pct": "12",
                    "long_ratio": "0.52",
                    "hours_since_trigger": "1",
                    "last_close": "1.0",
                },
            ]
            close_rows = [
                dict(open_rows[0], ts_ms="1900003600000", last_close="13.0"),
                dict(open_rows[1], ts_ms="1900003600000", last_close="0.75"),
            ]

            opened = apply_pump_cycle_paper_rows(open_rows, output_dir=output_dir)
            closed = apply_pump_cycle_paper_rows(close_rows, output_dir=output_dir)
            summary = read_cycle_paper_summary(output_dir=output_dir)
            monitor = build_pump_strategy_monitor_state(
                {"status": "complete", "config": {"output_dir": str(output_dir)}, "cycle_paper": summary, "latest_rows": []},
                {"status": "waiting"},
            )

        positions = summary["positions"]
        events = summary["events_latest"]

        self.assertEqual(opened["open_positions"], 2)
        self.assertEqual(opened["long_open_positions"], 1)
        self.assertEqual(opened["short_open_positions"], 1)
        self.assertEqual(closed["closed_positions"], 2)
        self.assertEqual(summary["open_positions"], 0)
        self.assertEqual({item["side"] for item in positions}, {"long", "short"})
        self.assertTrue(all(item["status"] == "closed" for item in positions))
        self.assertGreater(summary["summary"]["realized_pnl_usd"], 0.0)
        self.assertEqual(monitor["cycle_paper"]["closed_count"], 2)
        self.assertIn("track_summaries", summary)
        self.assertIn("candidate_shadow", summary)
        self.assertIn("skip_summary", summary)
        self.assertIn("candidate_paper", summary)
        self.assertTrue(summary["track_summaries"])
        self.assertEqual(monitor["cycle_paper"]["candidate_shadow"]["mode"], "shadow_paper_independent_slots")
        self.assertIn("paper_track_summaries", monitor["cycle_paper"]["candidate_shadow"])
        self.assertIn("candidate_paper", monitor["cycle_paper"])
        self.assertGreaterEqual(summary["candidate_paper"]["closed_positions"], 1)
        self.assertTrue(any(item["track_id"] == "short_clean_p100_l3_shadow" for item in summary["candidate_paper"]["track_summaries"]))
        self.assertTrue(any(item["track_id"] == "short_main_tiered" for item in summary["track_summaries"]))
        self.assertTrue(any(event.get("event") == "cycle_open" for event in events))
        self.assertTrue(any(event.get("event") == "cycle_close" for event in events))

    def test_cycle_paper_short_pnl_uses_leg_level_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            open_rows = [
                {
                    "ts_ms": "1900000000000",
                    "status": "entry_candidate",
                    "symbol": "SHORTUSDT",
                    "event_id": "SHORT-1",
                    "trigger_pump_pct": "140",
                    "pullback_from_high_pct": "21",
                    "funding_prev_24h_pct": "-0.2",
                    "oi_change_24h_pct": "12",
                    "long_ratio": "0.52",
                    "hours_since_trigger": "1",
                    "last_close": "1.0",
                }
            ]
            add_rows = [dict(open_rows[0], ts_ms="1900003600000", last_close="1.5")]
            mark_rows = [dict(open_rows[0], ts_ms="1900007200000", last_close="1.3")]

            apply_pump_cycle_paper_rows(open_rows, output_dir=output_dir)
            apply_pump_cycle_paper_rows(add_rows, output_dir=output_dir)
            apply_pump_cycle_paper_rows(mark_rows, output_dir=output_dir)
            summary = read_cycle_paper_summary(output_dir=output_dir)

        position = next(item for item in summary["positions"] if item.get("track_id") == "short_main_tiered")

        self.assertEqual(position["filled_steps"], 2)
        self.assertLess(position["avg_entry_price"], 1.3334)
        self.assertLess(position["current_pnl_pct"], 0.0)
        self.assertLess(position["combined_pnl_usd"], 0.0)

    def test_active_window_summary_and_monitor_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            rows = [
                {
                    "ts_ms": "1900000000000",
                    "status": "watch_ratio",
                    "symbol": "EVAAUSDT",
                    "event_id": "EVAA-1",
                    "trigger_ts": "1900000000000",
                    "trigger_pump_pct": "120",
                    "pullback_from_high_pct": "14",
                    "last_close": "1.0",
                },
                {"ts_ms": "1900000000000", "status": "no_recent_pump", "symbol": "BLASTUSDT"},
            ]

            selected = select_active_window_rows(rows, output_dir=output_dir, max_symbols=5)
            sample = {
                "symbol": "EVAAUSDT",
                "trigger_ts": 1_900_000_000_000,
                "end_ts": 1_900_003_600_000,
                "row": selected[0],
                "series": {
                    "klines": [
                        {"ts_ms": 1_900_000_000_000, "close": 1.0, "volume": 100},
                        {"ts_ms": 1_900_003_300_000, "close": 1.2, "volume": 200},
                        {"ts_ms": 1_900_003_600_000, "close": 1.3, "volume": 400},
                    ],
                    "premium_index": [
                        {"ts_ms": 1_900_000_000_000, "close": -0.02, "low": -0.025},
                        {"ts_ms": 1_900_003_600_000, "close": -0.015, "low": -0.016},
                    ],
                    "mark_price": [{"ts_ms": 1_900_003_600_000, "close": 1.29}],
                    "index_price": [{"ts_ms": 1_900_003_600_000, "close": 1.30}],
                    "open_interest": [
                        {"ts_ms": 1_900_000_000_000, "open_interest": 1000},
                        {"ts_ms": 1_900_003_600_000, "open_interest": 1300},
                    ],
                    "funding": [{"ts_ms": 1_900_003_600_000, "funding_rate": -0.01}],
                },
            }

            summary = build_active_window_summary(sample)
            monitor = build_pump_strategy_monitor_state(
                {
                    "status": "complete",
                    "config": {"output_dir": str(output_dir)},
                    "active_window": {
                        "schema": "pump_active_window_v1",
                        "updated_at_ms": 1_900_003_600_000,
                        "symbols": 1,
                        "errors": 0,
                        "rows": [summary],
                    },
                    "latest_rows": rows,
                },
                {"status": "waiting"},
            )

        self.assertEqual([item["symbol"] for item in selected], ["EVAAUSDT"])
        self.assertAlmostEqual(summary["premium_latest_pct_5m"], -1.5)
        self.assertAlmostEqual(summary["return_from_trigger_pct_5m"], 30.0)
        self.assertEqual(monitor["active_window"]["symbols"], 1)
        self.assertEqual(monitor["active_window"]["rows"][0]["symbol"], "EVAAUSDT")

    def test_slow_pump_watch_is_limited_and_never_opens_paper(self) -> None:
        slow_rows = [
            {
                "ts_ms": "1900100000000",
                "status": "watch_slow_pump",
                "symbol": f"SLOW{index}USDT",
                "slow_pump_event_id": f"slow-{index}",
                "slow_pump_trigger_ts": "1900000000000",
                "slow_pump_window_h": "72",
                "slow_pump_threshold_pct": "75",
                "slow_pump_return_pct": str(90 - index),
                "slow_pump_trigger_close": "1.8",
                "slow_pump_pullback_from_high_pct": "20",
                "slow_pump_stage": "distribution",
                "premium_latest_pct": "-2",
                "oi_change_4h_pct": "20",
                "oi_change_24h_pct": "10",
                "long_ratio": "0.52",
                "funding_prev_24h_pct": "0.1",
                "hours_since_trigger": "1",
                "last_close": "1.4",
            }
            for index in range(7)
        ]
        normal_watch = {
            "ts_ms": "1900100000000",
            "status": "watch_ratio",
            "symbol": "FASTUSDT",
            "event_id": "fast-1",
            "trigger_ts": "1900000000000",
            "trigger_pump_pct": "120",
        }

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            with patch(
                "webapp.bybit_pump_short_lab.open_paper_positions_for_active_window",
                return_value=[
                    {
                        "symbol": "SLOW6USDT",
                        "status": "open",
                        "track_id": "legacy_fast_track",
                    }
                ],
            ):
                selected = select_active_window_rows([*slow_rows, normal_watch], output_dir=output_dir, max_symbols=20)
            paper = apply_pump_cycle_paper_rows(slow_rows, output_dir=output_dir)
            monitor = build_pump_strategy_monitor_state(
                {
                    "config": {"output_dir": str(output_dir)},
                    "latest_rows": [normal_watch],
                    "slow_pump_watch_rows": slow_rows,
                },
                {"status": "waiting"},
            )

        self.assertEqual(selected[0]["symbol"], "SLOW6USDT")
        self.assertIn("FASTUSDT", {row["symbol"] for row in selected})
        self.assertEqual(sum(1 for row in selected if row["status"] == "watch_slow_pump"), 6)
        active_open = next(row for row in selected if row["symbol"] == "SLOW6USDT")
        self.assertTrue(active_open["active_open_paper"])
        self.assertEqual(active_open["active_source"], "legacy_fast_track")
        self.assertEqual(paper["positions"], 0)
        decision = classify_strategy_signal(PUMP_STRATEGY_CATALOG[0], slow_rows[0])
        self.assertEqual(decision["state"], "waiting_pump")
        self.assertEqual(decision["reason"], "research_only_slow_pump")
        self.assertEqual(monitor["slow_pump_watch"]["count"], 7)
        self.assertEqual(monitor["slow_pump_watch"]["mode"], "research_only_no_trades")

    def test_dashboard_api_returns_operator_payload(self) -> None:
        try:
            webapp_app = import_module("webapp.app")
        except ModuleNotFoundError as exc:
            if exc.name == "fastapi":
                self.skipTest("fastapi is not installed in this test environment")
            raise
        fake_payload = {
            "schema": "pump_short_operator_dashboard_v1",
            "capital": {"initial_capital_usd": 1000.0},
            "positions": {"open": []},
        }
        with patch.object(webapp_app.bybit_pump_short_lab, "pump_dashboard_status", return_value=fake_payload):
            response = asyncio.run(webapp_app.pump_short_dashboard_api())

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertEqual(payload["schema"], "pump_short_operator_dashboard_v1")
        self.assertEqual(payload["capital"]["initial_capital_usd"], 1000.0)

    def test_strategy_api_returns_monitor_payload(self) -> None:
        try:
            webapp_app = import_module("webapp.app")
        except ModuleNotFoundError as exc:
            if exc.name == "fastapi":
                self.skipTest("fastapi is not installed in this test environment")
            raise
        fake_payload = {
            "schema": "pump_short_strategy_monitor_v1",
            "strategies": [{"strategy_id": "main_pullback_tier"}],
        }
        with patch.object(webapp_app.bybit_pump_short_lab, "strategy_monitor_status", return_value=fake_payload):
            response = asyncio.run(webapp_app.pump_short_strategies_api())

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertEqual(payload["schema"], "pump_short_strategy_monitor_v1")
        self.assertEqual(payload["strategies"][0]["strategy_id"], "main_pullback_tier")


def make_instrument(symbol: str, base_coin: str, launch_time_ms: int) -> BybitInstrument:
    return BybitInstrument(
        symbol=symbol,
        base_coin=base_coin,
        quote_coin="USDT",
        launch_time_ms=launch_time_ms,
        status="Trading",
        funding_interval_min=480,
        upper_funding_rate=0.01,
        lower_funding_rate=-0.01,
        min_order_qty=1.0,
        qty_step=1.0,
        min_notional=5.0,
        max_leverage=5.0,
        raw={},
    )


if __name__ == "__main__":
    unittest.main()
