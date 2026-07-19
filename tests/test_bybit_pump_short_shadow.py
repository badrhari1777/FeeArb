from __future__ import annotations

import unittest

from analysis_collectors.bybit_pump_short import BybitInstrument
from analysis_features.bybit_pump_short_shadow import (
    ShadowScanConfig,
    classify_shadow_sample,
    pullback_threshold_from_strategy,
    select_instruments,
)


class BybitPumpShortShadowTestCase(unittest.TestCase):
    def test_classifies_confirmed_shadow_entry_candidate(self) -> None:
        base_ts = 1_900_000_000_000
        closes = [1.0] * 24 + [1.7, 2.0, 1.8, 1.55, 1.5, 1.45] + [1.45] * 24
        klines = []
        oi = []
        ratios = []
        for idx, close in enumerate(closes):
            ts = base_ts + idx * 3_600_000
            klines.append(
                {
                    "ts_ms": ts,
                    "open": close,
                    "high": close * 1.02,
                    "low": close * 0.98,
                    "close": close,
                }
            )
            oi.append({"ts_ms": ts, "open_interest": 100.0})
            ratios.append({"ts_ms": ts, "buy_ratio": 0.52, "sell_ratio": 0.48})
        sample = {
            "symbol": "TESTUSDT",
            "instrument": {"launch_time_ms": base_ts - 30 * 86_400_000},
            "summary": {"symbol": "TESTUSDT", "last_close": closes[-1], "pump_score": 80.0},
            "series": {
                "klines_1h": klines,
                "funding": [{"ts_ms": base_ts + 48 * 3_600_000, "funding_rate": 0.0001}],
                "open_interest_1h": oi,
                "long_short_1h": ratios,
            },
        }
        profiles = [
            {
                "profile": "conservative",
                "profile_rank": "1",
                "entry_strategy": "pb20_oi50_lr_mid_ladder3_step_50",
                "exit_strategy": "tp25_full_168",
                "anti_overfit_status": "robust_candidate",
            }
        ]

        row = classify_shadow_sample(sample, profiles=profiles, scan_ts_ms=base_ts + 60 * 3_600_000)

        self.assertEqual(row["status"], "entry_candidate")
        self.assertEqual(row["matched_profile"], "conservative")
        self.assertEqual(row["matched_entry_strategy"], "pb20_oi50_lr_mid_ladder3_step_50")

    def test_parses_pullback_threshold(self) -> None:
        self.assertEqual(pullback_threshold_from_strategy("pb20_oi50_lr_mid"), 20.0)
        self.assertIsNone(pullback_threshold_from_strategy("immediate"))

    def test_select_instruments_skips_non_crypto_contracts_by_default(self) -> None:
        instruments = [
            make_instrument("ASMLUSDT", "ASML", 300, symbol_type="stock"),
            make_instrument("TQQQUSDT", "TQQQ", 200),
            make_instrument("SIRENUSDT", "SIREN", 100),
        ]

        selected = select_instruments(instruments, ShadowScanConfig(max_symbols=10, symbols=[]))

        self.assertEqual([item.symbol for item in selected], ["SIRENUSDT"])

    def test_select_instruments_keeps_requested_non_crypto_symbol(self) -> None:
        instruments = [
            make_instrument("ASMLUSDT", "ASML", 300),
            make_instrument("SIRENUSDT", "SIREN", 100),
        ]

        selected = select_instruments(instruments, ShadowScanConfig(max_symbols=10, symbols=["ASMLUSDT"]))

        self.assertEqual([item.symbol for item in selected], ["ASMLUSDT"])


def make_instrument(
    symbol: str,
    base_coin: str,
    launch_time_ms: int,
    *,
    symbol_type: str = "",
) -> BybitInstrument:
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
        raw={"symbolType": symbol_type} if symbol_type else {},
    )


if __name__ == "__main__":
    unittest.main()
