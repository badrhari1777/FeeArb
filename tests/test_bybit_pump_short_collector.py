from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from analysis_collectors.bybit_pump_short import (
    BybitCollectorConfig,
    BybitInstrument,
    BybitPumpShortCollector,
    build_symbol_summary,
    dedupe_instruments,
    dedupe_sort_by_ts,
    interval_to_ms,
    pct_change_from_hours,
)


class _FakeBybitPumpShortCollector(BybitPumpShortCollector):
    def __init__(self, config: BybitCollectorConfig) -> None:
        super().__init__(config)
        self.collected_symbols: list[str] = []

    def load_instruments(self) -> list[BybitInstrument]:
        return [
            BybitInstrument(
                symbol="NEWUSDT",
                base_coin="NEW",
                quote_coin="USDT",
                launch_time_ms=1_800_000_000_000,
                status="Trading",
                funding_interval_min=480,
                upper_funding_rate=0.00375,
                lower_funding_rate=-0.00375,
                min_order_qty=1.0,
                qty_step=1.0,
                min_notional=5.0,
                max_leverage=25.0,
                raw={},
            ),
            BybitInstrument(
                symbol="OLDUSDT",
                base_coin="OLD",
                quote_coin="USDT",
                launch_time_ms=1_700_000_000_000,
                status="Trading",
                funding_interval_min=480,
                upper_funding_rate=0.00375,
                lower_funding_rate=-0.00375,
                min_order_qty=1.0,
                qty_step=1.0,
                min_notional=5.0,
                max_leverage=25.0,
                raw={},
            ),
        ]

    def collect_symbol(self, instrument: BybitInstrument) -> dict:
        self.collected_symbols.append(instrument.symbol)
        return {
            "schema": "test",
            "symbol": instrument.symbol,
            "summary": {
                "ts_iso": "2026-06-23T00:00:00+00:00",
                "symbol": instrument.symbol,
                "launch_iso": "2026-06-01T00:00:00+00:00",
                "age_days": 1.0,
                "last_close": 1.0,
                "return_24h_pct": 100.0,
                "return_3d_pct": 250.0,
                "return_7d_pct": 400.0,
                "return_14d_pct": 400.0,
                "drawdown_from_lookback_high_pct": -25.0,
                "lookback_high_pct_from_first": 500.0,
                "funding_latest_pct": -0.05,
                "funding_sum_24h_pct": -0.1,
                "funding_sum_3d_pct": -0.2,
                "funding_sum_7d_pct": -0.3,
                "oi_change_4h_pct": -10.0,
                "oi_change_24h_pct": 50.0,
                "long_account_ratio": 0.55,
                "pump_score": 75.0,
                "continuation_risk_score": 35.0,
                "candidate_tier": "research_short_candidate",
                "data_quality": "{}",
            },
            "series": {},
        }


class BybitPumpShortCollectorTestCase(unittest.TestCase):
    def test_summary_scores_pump_and_funding_features(self) -> None:
        instrument = BybitInstrument(
            symbol="PUMPUSDT",
            base_coin="PUMP",
            quote_coin="USDT",
            launch_time_ms=1_800_000_000_000,
            status="Trading",
            funding_interval_min=480,
            upper_funding_rate=0.00375,
            lower_funding_rate=-0.00375,
            min_order_qty=1.0,
            qty_step=1.0,
            min_notional=5.0,
            max_leverage=25.0,
            raw={},
        )
        base_ts = 1_800_000_000_000
        klines = []
        for hour in range(0, 169):
            ts_ms = base_ts + hour * 3_600_000
            close = 1.0 + hour * 0.02
            klines.append(
                {
                    "ts_ms": ts_ms,
                    "open": close,
                    "high": close * 1.1,
                    "low": close * 0.95,
                    "close": close,
                    "volume": 100.0,
                    "turnover": 100.0,
                }
            )
        funding = [
            {"ts_ms": base_ts + 160 * 3_600_000, "funding_rate": -0.001},
            {"ts_ms": base_ts + 168 * 3_600_000, "funding_rate": -0.002},
        ]
        oi = [
            {"ts_ms": base_ts + 144 * 3_600_000, "open_interest": 100.0},
            {"ts_ms": base_ts + 168 * 3_600_000, "open_interest": 150.0},
        ]
        ratios = [{"ts_ms": base_ts + 168 * 3_600_000, "buy_ratio": 0.58, "sell_ratio": 0.42}]

        summary = build_symbol_summary(
            instrument=instrument,
            ts_ms=base_ts + 168 * 3_600_000,
            klines_1h=klines,
            funding=funding,
            open_interest=oi,
            long_short=ratios,
        )

        self.assertEqual(summary["symbol"], "PUMPUSDT")
        self.assertGreater(summary["return_7d_pct"], 300.0)
        self.assertLess(summary["funding_sum_24h_pct"], 0.0)
        self.assertEqual(summary["oi_change_24h_pct"], 50.0)
        self.assertIn(summary["candidate_tier"], {"watchlist", "research_short_candidate"})

    def test_return_window_requires_enough_history(self) -> None:
        closes = [
            (1_800_000_000_000, 10.0),
            (1_800_000_000_000 + 11 * 3_600_000, 12.0),
        ]

        self.assertIsNone(pct_change_from_hours(closes, 24))
        self.assertAlmostEqual(pct_change_from_hours(closes, 10) or 0.0, 20.0)

    def test_interval_and_dedupe_helpers(self) -> None:
        self.assertEqual(interval_to_ms("60"), 3_600_000)
        self.assertEqual(interval_to_ms("1h"), 3_600_000)
        self.assertEqual(interval_to_ms("1d"), 86_400_000)

        rows = dedupe_sort_by_ts(
            [
                {"ts_ms": 3, "value": "old"},
                {"ts_ms": 1, "value": "first"},
                {"ts_ms": 3, "value": "new"},
                {"ts_ms": None, "value": "skip"},
            ]
        )

        self.assertEqual(rows, [{"ts_ms": 1, "value": "first"}, {"ts_ms": 3, "value": "new"}])

    def test_dedupe_instruments_by_symbol(self) -> None:
        first = BybitInstrument(
            symbol="DUPUSDT",
            base_coin="DUP",
            quote_coin="USDT",
            launch_time_ms=1,
            status="Trading",
            funding_interval_min=480,
            upper_funding_rate=None,
            lower_funding_rate=None,
            min_order_qty=None,
            qty_step=None,
            min_notional=None,
            max_leverage=None,
            raw={"version": 1},
        )
        second = BybitInstrument(
            symbol="DUPUSDT",
            base_coin="DUP",
            quote_coin="USDT",
            launch_time_ms=2,
            status="Trading",
            funding_interval_min=480,
            upper_funding_rate=None,
            lower_funding_rate=None,
            min_order_qty=None,
            qty_step=None,
            min_notional=None,
            max_leverage=None,
            raw={"version": 2},
        )

        rows = dedupe_instruments([first, second])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].launch_time_ms, 2)

    def test_collect_resumes_and_writes_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / "done_symbols.txt").write_text("NEWUSDT\n", encoding="utf-8")
            collector = _FakeBybitPumpShortCollector(
                BybitCollectorConfig(output_dir=output_dir, sleep_sec=0.0)
            )

            stats = collector.collect(resume=True)

            self.assertEqual(stats.symbols_seen, 2)
            self.assertEqual(stats.symbols_skipped, 1)
            self.assertEqual(stats.symbols_collected, 1)
            self.assertEqual(collector.collected_symbols, ["OLDUSDT"])

            with (output_dir / "symbol_summary.csv").open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "OLDUSDT")


if __name__ == "__main__":
    unittest.main()
