from __future__ import annotations

import unittest

from analysis_features.bybit_pump_short_outcomes import (
    Series,
    build_anti_overfit_report,
    build_symbol_concentration,
    build_symbol_holdout_robustness,
    build_time_split_robustness,
    detect_pump_events,
    detect_pump_episodes,
    event_behavior_features,
    simulate_event_exit_outcomes,
    simulate_event_long_hold_outcomes,
    simulate_event_outcomes,
)


class BybitPumpShortOutcomesTestCase(unittest.TestCase):
    def test_detects_threshold_crossing_and_simulates_outcome(self) -> None:
        base_ts = 1_800_000_000_000
        closes = [1.0] * 12 + [1.6, 1.8, 1.4, 1.2, 1.0] + [0.9] * 30
        ts = [base_ts + i * 3_600_000 for i in range(len(closes))]
        series = Series(
            symbol="TESTUSDT",
            launch_ms=base_ts - 10 * 86_400_000,
            ts=ts,
            open=list(closes),
            high=[value * 1.05 for value in closes],
            low=[value * 0.95 for value in closes],
            close=list(closes),
            funding=[(ts[20], 0.001)],
            oi={stamp: 100.0 for stamp in ts},
            long_ratio={stamp: 0.5 for stamp in ts},
        )

        events = detect_pump_events(series)
        target = [event for event in events if event.config_window_h == 4]
        self.assertTrue(target)
        self.assertGreaterEqual(target[0].pump_pct, 50.0)

        episodes = detect_pump_episodes(series, events)
        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0].symbol, "TESTUSDT")
        self.assertGreater(episodes[0].high_from_start_pct or 0.0, 0.0)

        rows = simulate_event_outcomes(series, target[0])
        immediate_24h = [
            row
            for row in rows
            if row["strategy"] == "immediate" and row["horizon_h"] == 24
        ]
        self.assertEqual(len(immediate_24h), 1)
        self.assertGreater(immediate_24h[0]["net_exit_pct"], 0.0)

        confirmed = [
            row
            for row in rows
            if row["strategy"] == "pb20_oi50_lr_mid" and row["horizon_h"] == 24
        ]
        self.assertEqual(len(confirmed), 1)
        self.assertGreater(confirmed[0]["entry_ts"], target[0].trigger_ts)

        exit_rows = simulate_event_exit_outcomes(series, target[0])
        full_tp = [
            row
            for row in exit_rows
            if row["entry_strategy"] == "immediate" and row["exit_strategy"] == "tp25_full_168"
        ]
        self.assertEqual(len(full_tp), 1)
        self.assertEqual(full_tp[0]["exit_reason"], "target_25")
        self.assertLess(full_tp[0]["time_in_trade_h"], 168)

        partial = [
            row
            for row in exit_rows
            if row["entry_strategy"] == "immediate" and row["exit_strategy"] == "tp25_50_70_thirds_336"
        ]
        self.assertEqual(len(partial), 1)
        self.assertIn("tp25", partial[0]["exit_events"])

    def test_event_behavior_features_classify_extreme_funding_and_oi_blowoff(self) -> None:
        base_ts = 1_800_000_000_000
        closes = [1.0] * 12 + [2.2] + [2.4] * 90
        ts = [base_ts + i * 3_600_000 for i in range(len(closes))]
        oi = {stamp: 100.0 for stamp in ts}
        for idx, stamp in enumerate(ts[13:80], start=13):
            oi[stamp] = 100.0 + (idx - 12) * 5.0
        series = Series(
            symbol="TOXICUSDT",
            launch_ms=base_ts - 5 * 86_400_000,
            ts=ts,
            open=list(closes),
            high=[value * 1.05 for value in closes],
            low=[value * 0.95 for value in closes],
            close=list(closes),
            funding=[(stamp, -0.025) for stamp in ts[13:20]],
            oi=oi,
            long_ratio={stamp: 0.72 for stamp in ts},
        )

        event = [item for item in detect_pump_events(series) if item.config_window_h == 4][0]
        features = event_behavior_features(series, event)
        exit_rows = simulate_event_exit_outcomes(series, event)

        self.assertEqual(features["funding_regime"], "extreme_negative_funding")
        self.assertEqual(features["oi_regime"], "oi_blowoff")
        self.assertEqual(features["long_ratio_regime"], "crowded_long_70_plus")
        self.assertEqual(features["age_regime"], "new_lt_7d")
        self.assertTrue(exit_rows)
        self.assertEqual(exit_rows[0]["funding_regime"], "extreme_negative_funding")

    def test_long_hold_outputs_cover_runner_and_repump_diagnostics(self) -> None:
        base_ts = 1_800_000_000_000
        closes = [1.0] * 12 + [1.6] + [1.2] * 30 + [0.8] * 30 + [2.2] * 10 + [0.7] * 80
        ts = [base_ts + i * 3_600_000 for i in range(len(closes))]
        series = Series(
            symbol="DECAYUSDT",
            launch_ms=base_ts - 20 * 86_400_000,
            ts=ts,
            open=list(closes),
            high=[value * 1.05 for value in closes],
            low=[value * 0.95 for value in closes],
            close=list(closes),
            funding=[(stamp, 0.001) for stamp in ts[13::8]],
            oi={stamp: 100.0 for stamp in ts},
            long_ratio={stamp: 0.5 for stamp in ts},
        )

        event = [item for item in detect_pump_events(series) if item.config_window_h == 4][0]
        rows = simulate_event_long_hold_outcomes(series, event)
        runner = [
            row
            for row in rows
            if row["entry_strategy"] == "immediate" and row["exit_strategy"] == "cover50_72h_runner_90d"
        ]

        self.assertEqual(len(runner), 1)
        self.assertIn("cover50_72h", runner[0]["exit_events"])
        self.assertGreater(runner[0]["funding_full_period_pct"], 0.0)
        self.assertGreaterEqual(runner[0]["repump_30_count"], 1)
        self.assertIn("time_to_decay_50_h", runner[0])

    def test_builds_robustness_reports(self) -> None:
        rows = []
        base_ts = 1_800_000_000_000
        symbols = [f"T{idx:02d}USDT" for idx in range(12)]
        for index in range(120):
            rows.append(
                {
                    "symbol": symbols[index % len(symbols)],
                    "trigger_ts": base_ts + index * 3_600_000,
                    "entry_strategy": "pb20_oi50_lr_mid_ladder3_step_50",
                    "exit_strategy": "time_full_72",
                    "net_exit_pct": 20.0 if index < 80 else 15.0,
                    "mae_pct": 40.0,
                    "mfe_pct": 30.0,
                    "win": 1,
                    "catastrophic_100": 0,
                    "catastrophic_300": 0,
                    "liquidation_proxy_3x": 1,
                    "liquidation_proxy_1x": 0,
                }
            )

        time_rows = build_time_split_robustness(rows)
        symbol_rows = build_symbol_holdout_robustness(rows)
        concentration_rows = build_symbol_concentration(rows)
        anti_rows = build_anti_overfit_report(time_rows, symbol_rows, concentration_rows)

        self.assertEqual(time_rows[0]["robustness_status"], "robust")
        self.assertTrue(symbol_rows)
        self.assertTrue(concentration_rows)
        self.assertEqual(anti_rows[0]["anti_overfit_status"], "robust_candidate")


if __name__ == "__main__":
    unittest.main()
