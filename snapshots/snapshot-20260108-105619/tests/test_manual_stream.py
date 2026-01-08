from __future__ import annotations

import unittest

from webapp.manual_symbols import (
    _normalize_bingx_symbol,
    _normalize_bitget_symbol,
    _normalize_bybit_symbol,
    _normalize_gate_symbol,
    _normalize_kucoin_symbol,
    _normalize_mexc_symbol,
    _normalize_okx_symbol,
)


class ManualStreamSymbolTestCase(unittest.TestCase):
    def test_perp_symbol_normalization(self) -> None:
        raw = "BTC/USDT:USDT"
        self.assertEqual(_normalize_bybit_symbol(raw), "BTCUSDT")
        self.assertEqual(_normalize_bingx_symbol(raw), "BTC-USDT")
        self.assertEqual(_normalize_mexc_symbol(raw), "BTC_USDT")
        self.assertEqual(_normalize_bitget_symbol(raw), "BTCUSDT")
        self.assertEqual(_normalize_okx_symbol(raw), "BTC-USDT-SWAP")
        self.assertEqual(_normalize_gate_symbol(raw), "BTC_USDT")
        self.assertEqual(_normalize_kucoin_symbol(raw), "BTCUSDTM")

    def test_plain_symbol_normalization(self) -> None:
        raw = "ETHUSDT"
        self.assertEqual(_normalize_bybit_symbol(raw), "ETHUSDT")
        self.assertEqual(_normalize_bingx_symbol(raw), "ETH-USDT")
        self.assertEqual(_normalize_mexc_symbol(raw), "ETH_USDT")
        self.assertEqual(_normalize_bitget_symbol(raw), "ETHUSDT")
        self.assertEqual(_normalize_okx_symbol(raw), "ETH-USDT-SWAP")
        self.assertEqual(_normalize_gate_symbol(raw), "ETH_USDT")
        self.assertEqual(_normalize_kucoin_symbol(raw), "ETHUSDTM")

    def test_base_symbol_normalization(self) -> None:
        raw = "FLOW"
        self.assertEqual(_normalize_bybit_symbol(raw), "FLOWUSDT")
        self.assertEqual(_normalize_bingx_symbol(raw), "FLOW-USDT")
        self.assertEqual(_normalize_mexc_symbol(raw), "FLOW_USDT")
        self.assertEqual(_normalize_bitget_symbol(raw), "FLOWUSDT")
        self.assertEqual(_normalize_okx_symbol(raw), "FLOW-USDT-SWAP")
        self.assertEqual(_normalize_gate_symbol(raw), "FLOW_USDT")
        self.assertEqual(_normalize_kucoin_symbol(raw), "FLOWUSDTM")


if __name__ == "__main__":
    unittest.main()
