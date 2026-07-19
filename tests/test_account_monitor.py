from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import types
from unittest.mock import AsyncMock

import execution.accounts as accounts_module
from execution.accounts import (
    AccountMonitor,
    ExchangeGateway,
    EXCHANGE_SPECS,
    _extract_leverage,
    _filter_markets_with_ids,
)


class ExchangeGatewayMarketFilterTestCase(unittest.TestCase):
    def test_filter_markets_removes_missing_ids(self) -> None:
        markets = _filter_markets_with_ids(
            [
                {"id": "BTC-USDT-SWAP", "symbol": "BTC/USDT:USDT"},
                {"id": None, "symbol": "BROKEN"},
                {"symbol": "ALSO-BROKEN"},
            ]
        )
        self.assertEqual(markets, [{"id": "BTC-USDT-SWAP", "symbol": "BTC/USDT:USDT"}])

    def test_concurrent_ensure_client_builds_once(self) -> None:
        gateway = ExchangeGateway(EXCHANGE_SPECS[0])
        gateway.api_key = "key"
        gateway.api_secret = "secret"
        gateway._cred_signature = ("key", "secret", "")  # type: ignore[attr-defined]
        builds = 0

        class _Client:
            async def close(self) -> None:
                return None

        async def _build_client():
            nonlocal builds
            builds += 1
            await asyncio.sleep(0.01)
            return _Client()

        gateway._build_client = _build_client  # type: ignore[method-assign]

        async def _run() -> None:
            await asyncio.gather(
                gateway.ensure_client(),
                gateway.ensure_client(),
                gateway.ensure_client(),
            )
            await gateway.close()

        asyncio.run(_run())
        self.assertEqual(builds, 1)


class _FakeBitgetUtaClient:
    def __init__(self) -> None:
        self.balance_params: list[dict] = []
        self.uta_account_calls: list[dict] = []
        self.positions_params: list[dict] = []

    async def fetch_balance(self, params: dict | None = None) -> dict:
        self.balance_params.append(dict(params or {}))
        return {
            "USDT": {"free": 42.0, "used": 8.0, "total": 50.0},
            "free": {"USDT": 42.0},
            "used": {"USDT": 8.0},
            "total": {"USDT": 50.0},
            "info": {"mgnRatio": "0.16", "imr": "8", "mmr": "1"},
        }

    async def privateUtaGetV3AccountAssets(self, params: dict | None = None) -> dict:
        self.uta_account_calls.append(dict(params or {}))
        return {
            "requestTime": "1782235598649",
            "data": {
                "accountEquity": "50",
                "usdtEquity": "50",
                "unrealisedPnl": "1.5",
                "imr": "8",
                "mmr": "1",
                "mgnRatio": "0.16",
                "assets": [
                    {
                        "coin": "USDT",
                        "equity": "50",
                        "usdValue": "50",
                        "balance": "50",
                        "available": "42",
                        "debt": "0",
                        "locked": "8",
                    }
                ],
            },
        }

    async def fetch_positions(self, params: dict | None = None) -> list[dict]:
        self.positions_params.append(dict(params or {}))
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "contracts": 0.001,
                "contractSize": 1.0,
                "side": "long",
                "entryPrice": 100000.0,
                "markPrice": 101000.0,
                "leverage": 1,
                "liquidationPrice": 50000.0,
                "marginMode": "cross",
                "initialMargin": 100.0,
                "maintenanceMargin": 1.0,
                "unrealizedPnl": 1.0,
                "info": {"symbol": "BTCUSDT", "posSide": "long", "holdMode": "hedge_mode"},
            }
        ]


class ExchangeGatewayBitgetUtaTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_bitget_balance_uses_uta_private_params_by_default(self) -> None:
        spec = next(item for item in EXCHANGE_SPECS if item.slug == "bitget")
        gateway = ExchangeGateway(spec)
        gateway._client = _FakeBitgetUtaClient()
        gateway._unavailable_reason = None

        balance = await gateway.fetch_balance()

        self.assertEqual(balance.get("exchange"), "bitget")
        self.assertEqual(balance.get("available"), 42.0)
        self.assertEqual(balance.get("total"), 50.0)
        self.assertEqual(balance.get("used"), 8.0)
        self.assertEqual(balance.get("initial_margin"), 8.0)
        self.assertEqual(balance.get("maintenance_margin"), 1.0)
        self.assertEqual(gateway.client.uta_account_calls, [{}])  # type: ignore[union-attr]
        self.assertEqual(gateway.client.balance_params, [])  # type: ignore[union-attr]

    async def test_bitget_positions_use_uta_private_params_by_default(self) -> None:
        spec = next(item for item in EXCHANGE_SPECS if item.slug == "bitget")
        gateway = ExchangeGateway(spec)
        gateway._client = _FakeBitgetUtaClient()
        gateway._unavailable_reason = None

        positions = await gateway.fetch_positions()

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].get("symbol_normalized"), "BTCUSDT")
        self.assertEqual(gateway.client.positions_params, [{"type": "swap", "uta": True}])  # type: ignore[union-attr]


class _PartialCollectGateway:
    has_credentials = True
    available = True
    unavailable_reason = None

    async def refresh_credentials_async(self, *, force_env: bool = False) -> None:  # noqa: ARG002
        return None

    async def ensure_client(self) -> None:
        return None

    async def fetch_balance(self) -> dict:
        raise RuntimeError("bitget {\"code\":\"40014\",\"msg\":\"Incorrect permissions, need UTA manage read\"}")

    async def fetch_positions(self) -> list[dict]:
        return [
            {
                "exchange": "bitget",
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "coin_qty": 0.001,
            }
        ]

    def requires_cycle_close(self) -> bool:
        return False

    async def close(self) -> None:
        return None


class AccountMonitorPartialCollectTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_balance_error_does_not_drop_positions(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        monitor._gateways = {"bitget": _PartialCollectGateway()}  # type: ignore[assignment]

        balances, positions, status, refreshed = await monitor._collect_all(force_env=True)

        self.assertEqual(balances, [])
        self.assertEqual(len(positions), 1)
        self.assertEqual(refreshed is not None, True)
        self.assertEqual(status[0]["exchange"], "bitget")
        self.assertEqual(status[0]["status"], "partial")
        self.assertTrue(status[0]["positions_fetch_ok"])
        self.assertIn("UTA manage read", status[0]["balance_error"])

    async def test_disabled_exchange_is_not_polled(self) -> None:
        enabled_gateway = _PartialCollectGateway()
        disabled_gateway = _PartialCollectGateway()
        monitor = AccountMonitor(
            refresh_interval=60,
            summary_interval=60,
            enabled_exchanges={"bitget"},
        )
        monitor._gateways = {  # type: ignore[assignment]
            "bitget": enabled_gateway,
            "mexc": disabled_gateway,
        }

        _balances, positions, status, _refreshed = await monitor._collect_all(force_env=True)

        self.assertEqual(len(positions), 1)
        self.assertEqual([item["exchange"] for item in status], ["bitget"])


class BalanceStatusRowsTestCase(unittest.TestCase):
    def test_missing_balance_gets_diagnostic_row_from_status(self) -> None:
        from webapp.services import DataService  # pylint: disable=import-outside-toplevel

        rows = DataService._balances_with_status_rows(
            [],
            [
                {
                    "exchange": "bitget",
                    "status": "partial",
                    "balance_error": "need UTA manage read",
                    "checked_at": "2026-06-23T00:00:00+00:00",
                }
            ],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["exchange"], "bitget")
        self.assertEqual(rows[0]["status"], "partial")
        self.assertEqual(rows[0]["error"], "need UTA manage read")


class AccountMonitorMarginUsedTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.monitor = AccountMonitor(refresh_interval=60, summary_interval=60)

    def test_margin_used_prefers_explicit_field(self) -> None:
        position = {"margin_used": 12.5}
        self.assertAlmostEqual(self.monitor._position_margin_used(position), 12.5)

    def test_margin_used_falls_back_to_initial_margin(self) -> None:
        position = {
            "margin_used": None,
            "initial_margin": 646.16934626,
            "leverage": None,
            "notional": 1938.508,
        }
        self.assertAlmostEqual(
            self.monitor._position_margin_used(position) or 0.0,
            646.16934626,
        )

    def test_margin_used_falls_back_to_raw_info_margin(self) -> None:
        position = {
            "raw": {
                "info": {
                    "positionInitialMargin": "123.45",
                }
            }
        }
        self.assertAlmostEqual(self.monitor._position_margin_used(position) or 0.0, 123.45)

    def test_margin_used_falls_back_to_notional_leverage(self) -> None:
        position = {"notional": -1000.0, "leverage": 5.0}
        self.assertAlmostEqual(self.monitor._position_margin_used(position) or 0.0, 200.0)

    def test_margin_used_prefers_kucoin_pos_margin(self) -> None:
        position = {
            "raw": {
                "posMargin": "145.5",
                "info": {
                    "positionInitialMargin": "123.45",
                },
            }
        }
        self.assertAlmostEqual(self.monitor._position_margin_used(position) or 0.0, 145.5)


class AccountMonitorLeverageExtractTestCase(unittest.TestCase):
    def test_extract_leverage_prefers_real_leverage(self) -> None:
        leverage, source = _extract_leverage(
            {
                "leverage": "3",
                "realLeverage": "4.8",
            }
        )
        self.assertAlmostEqual(leverage or 0.0, 4.8)
        self.assertEqual(source, "payload.realLeverage")

    def test_extract_leverage_prefers_info_real_leverage(self) -> None:
        leverage, source = _extract_leverage(
            {
                "info": {
                    "leverage": "3",
                    "realLeverage": "4.2",
                }
            }
        )
        self.assertAlmostEqual(leverage or 0.0, 4.2)
        self.assertEqual(source, "info.realLeverage")


class ExchangeGatewayTimeSyncTestCase(unittest.TestCase):
    def test_identifies_kucoin_timestamp_error(self) -> None:
        spec = next(spec for spec in EXCHANGE_SPECS if spec.slug == "kucoin")
        gateway = ExchangeGateway(spec)
        self.assertTrue(
            gateway._is_time_sync_error(
                RuntimeError('kucoinfutures {"code":"400002","msg":"Invalid KC-API-TIMESTAMP"}')
            )
        )

    def test_kucoin_client_preloads_time_difference(self) -> None:
        spec = next(spec for spec in EXCHANGE_SPECS if spec.slug == "kucoin")
        gateway = ExchangeGateway(spec)
        gateway.api_key = "key"
        gateway.api_secret = "secret"
        gateway.password = "passphrase"

        created_clients: list[object] = []

        class _FakeKucoinClient:
            def __init__(self, _config) -> None:
                self.load_time_difference = AsyncMock()
                created_clients.append(self)

        original_ccxt_async = accounts_module.ccxt_async
        accounts_module.ccxt_async = types.SimpleNamespace(kucoinfutures=_FakeKucoinClient)
        try:
            asyncio.run(gateway._build_client())
        finally:
            accounts_module.ccxt_async = original_ccxt_async
        self.assertEqual(len(created_clients), 1)
        created_clients[0].load_time_difference.assert_awaited_once()  # type: ignore[attr-defined]


class AccountMonitorSummaryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.monitor = AccountMonitor(refresh_interval=60, summary_interval=60)

    def test_build_positions_summary_variant_three(self) -> None:
        positions = [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "symbol_normalized": "BTCUSDT",
                "side": "long",
                "coin_qty": 900,
                "notional": 12400,
                "entry_price": 100.0,
                "mark_price": 101.0,
                "funding_rate": -0.00006,
            },
            {
                "exchange": "okx",
                "symbol": "BTCUSDT",
                "symbol_normalized": "BTCUSDT",
                "side": "short",
                "coin_qty": 1000,
                "notional": 13800,
                "entry_price": 100.5,
                "mark_price": 100.7,
                "funding_rate": 0.00008,
            },
            {
                "exchange": "bybit",
                "symbol": "ETHUSDT",
                "symbol_normalized": "ETHUSDT",
                "side": "long",
                "coin_qty": 500,
                "notional": 8100,
                "entry_price": 200.0,
                "mark_price": 199.9,
                "funding_rate": -0.00004,
            },
            {
                "exchange": "gate",
                "symbol": "ETHUSDT",
                "symbol_normalized": "ETHUSDT",
                "side": "short",
                "coin_qty": 500,
                "notional": 8300,
                "entry_price": 200.2,
                "mark_price": 200.1,
                "funding_rate": 0.00003,
            },
        ]
        text = self.monitor._build_positions_summary(
            positions,
            "2026-02-16T11:40:00+00:00",
        )
        lines = text.splitlines()
        self.assertEqual(lines[0], "14:40 Positions")
        self.assertIn("BN,OK  BTCUSDT  $12.4k  +0.80  +0.0140%  qDelta -100", lines)
        eth_line = next((line for line in lines if "ETHUSDT" in line), "")
        self.assertIn("BY,GT  ETHUSDT  $8.1k", eth_line)
        self.assertNotIn("qDelta", eth_line)

    def test_summary_slot_window(self) -> None:
        slot = self.monitor._summary_slot_key(
            datetime(2026, 2, 16, 11, 40, tzinfo=timezone.utc)
        )
        self.assertEqual(slot, "2026-02-16 14")
        self.assertEqual(
            self.monitor._summary_slot_key(
                datetime(2026, 2, 16, 11, 59, tzinfo=timezone.utc)
            ),
            "2026-02-16 14",
        )
        self.assertIsNone(
            self.monitor._summary_slot_key(
                datetime(2026, 2, 16, 11, 39, tzinfo=timezone.utc)
            )
        )
        self.assertIsNone(
            self.monitor._summary_slot_key(
                datetime(2026, 2, 16, 12, 0, tzinfo=timezone.utc)
            )
        )

    def test_build_positions_summary_dedupes_duplicated_settle_suffix(self) -> None:
        text = self.monitor._build_positions_summary(
            [
                {
                    "exchange": "binance",
                    "symbol": "RIVERUSDTUSDT",
                    "symbol_normalized": "RIVERUSDTUSDT",
                    "side": "long",
                    "coin_qty": 10,
                    "notional": 100,
                    "entry_price": 10.0,
                    "mark_price": 10.1,
                    "funding_rate": 0.0001,
                },
                {
                    "exchange": "okx",
                    "symbol": "RIVERUSDT",
                    "symbol_normalized": "RIVERUSDT",
                    "side": "short",
                    "coin_qty": 10,
                    "notional": 100,
                    "entry_price": 10.0,
                    "mark_price": 10.2,
                    "funding_rate": 0.0002,
                },
            ],
            "2026-02-16T11:40:00+00:00",
        )
        self.assertIn("RIVERUSDT", text)
        self.assertNotIn("RIVERUSDTUSDT", text)

    def test_normalize_symbol_dedupes_ccxt_perpetual_settle_suffix(self) -> None:
        self.assertEqual(
            accounts_module.normalize_symbol("ESPORTS/USDT:USDT"),
            "ESPORTSUSDT",
        )
        self.assertEqual(
            accounts_module.normalize_symbol("ESPORTSUSDTUSDT"),
            "ESPORTSUSDT",
        )

    def test_summary_slot_claim_dedupes_with_sent_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self.monitor._summary_slot_marker_dir = Path(tmpdir)
            self.monitor._summary_slot_marker_dir.mkdir(parents=True, exist_ok=True)
            slot_key = "2026-02-16 14"
            acquired, claim_path, _reason = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired)
            self.assertIsNotNone(claim_path)
            self.monitor._finalize_summary_slot_claim(slot_key, claim_path, "ok")
            acquired_again, _claim_again, reason_again = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertFalse(acquired_again)
            self.assertEqual(reason_again, "sent")

    def test_summary_slot_claim_releases_on_http_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self.monitor._summary_slot_marker_dir = Path(tmpdir)
            self.monitor._summary_slot_marker_dir.mkdir(parents=True, exist_ok=True)
            slot_key = "2026-02-16 14"
            acquired, claim_path, _reason = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired)
            self.assertIsNotNone(claim_path)
            self.monitor._finalize_summary_slot_claim(slot_key, claim_path, "http_error")
            acquired_again, _claim_again, reason_again = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired_again)
            self.assertNotEqual(reason_again, "sent")

    def test_summary_slot_claim_blocks_after_ambiguous_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self.monitor._summary_slot_marker_dir = Path(tmpdir)
            self.monitor._summary_slot_marker_dir.mkdir(parents=True, exist_ok=True)
            slot_key = "2026-02-16 14"
            acquired, claim_path, _reason = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertTrue(acquired)
            self.assertIsNotNone(claim_path)
            self.monitor._finalize_summary_slot_claim(slot_key, claim_path, "error")
            acquired_again, _claim_again, reason_again = self.monitor._acquire_summary_slot_claim(slot_key)
            self.assertFalse(acquired_again)
            self.assertEqual(reason_again, "claimed")


class _FakeGateFuturesRestClient:
    def __init__(self) -> None:
        self.account_calls: list[dict] = []
        self.positions_calls: list[dict] = []
        self.contract_calls: list[dict] = []

    async def privateFuturesGetSettleAccounts(self, request: dict) -> dict:
        self.account_calls.append(dict(request))
        return {
            "currency": "USDT",
            "available": "1070.31441",
            "position_margin": "2.98789797",
            "order_margin": "0",
            "unrealised_pnl": "0.002997",
            "maintenance_margin": "0.09075398625",
            "update_time": "1778486401",
        }

    async def privateFuturesGetSettlePositions(self, request: dict) -> list[dict]:
        self.positions_calls.append(dict(request))
        return [
            {
                "contract": "BTC_USDT",
                "size": "-3",
                "value": "24.201063",
                "entry_price": "80680.2",
                "mark_price": "80670.21",
                "unrealised_pnl": "0.002997",
                "leverage": "10",
                "liq_price": "90307.01",
                "pos_margin_mode": "isolated",
                "margin": "2.98964123256",
                "initial_margin": "2.440072176975",
                "maintenance_margin": "0.09075398625",
                "update_time": "1778486401",
            }
        ]

    async def publicFuturesGetSettleContractsContract(self, request: dict) -> dict:
        self.contract_calls.append(dict(request))
        return {
            "name": "BTC_USDT",
            "quanto_multiplier": "0.0001",
            "order_price_round": "0.1",
        }


class _FakeGateIsolatedBalanceClient(_FakeGateFuturesRestClient):
    async def privateFuturesGetSettleAccounts(self, request: dict) -> dict:
        self.account_calls.append(dict(request))
        return {
            "currency": "USDT",
            "available": "1070.28113",
            "position_margin": "0",
            "isolated_position_margin": "2.98964123256",
            "order_margin": "0",
            "maintenance_margin": "0.090709425",
            "unrealised_pnl": "0.01488",
            "update_time": "1778486401",
            "total": "2.987897970545",
        }


class ExchangeGatewayGateFuturesRestTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_gate_balance_uses_futures_account_endpoint(self) -> None:
        spec = next(item for item in EXCHANGE_SPECS if item.slug == "gate")
        gateway = ExchangeGateway(spec)
        gateway._client = _FakeGateFuturesRestClient()
        gateway._unavailable_reason = None

        balance = await gateway.fetch_balance()

        self.assertEqual(balance.get("exchange"), "gate")
        self.assertEqual(balance.get("asset"), "USDT")
        self.assertAlmostEqual(balance.get("available") or 0.0, 1070.31441)
        self.assertAlmostEqual(balance.get("used") or 0.0, 2.98789797)
        self.assertAlmostEqual(balance.get("initial_margin") or 0.0, 2.98789797)
        self.assertEqual(gateway.client.account_calls, [{"settle": "usdt"}])  # type: ignore[union-attr]

    async def test_gate_balance_prefers_isolated_position_margin_over_zero_position_margin(self) -> None:
        spec = next(item for item in EXCHANGE_SPECS if item.slug == "gate")
        gateway = ExchangeGateway(spec)
        gateway._client = _FakeGateIsolatedBalanceClient()
        gateway._unavailable_reason = None

        balance = await gateway.fetch_balance()

        self.assertAlmostEqual(balance.get("used") or 0.0, 2.98964123256)
        self.assertAlmostEqual(balance.get("initial_margin") or 0.0, 2.98964123256)
        self.assertAlmostEqual(balance.get("total") or 0.0, 1073.27077123256)

    async def test_gate_positions_use_futures_position_and_contract_endpoints(self) -> None:
        spec = next(item for item in EXCHANGE_SPECS if item.slug == "gate")
        gateway = ExchangeGateway(spec)
        gateway._client = _FakeGateFuturesRestClient()
        gateway._unavailable_reason = None

        positions = await gateway.fetch_positions()

        self.assertEqual(len(positions), 1)
        position = positions[0]
        self.assertEqual(position.get("exchange"), "gate")
        self.assertEqual(position.get("exchange_symbol"), "BTC_USDT")
        self.assertEqual(position.get("side"), "short")
        self.assertAlmostEqual(position.get("contracts") or 0.0, 3.0)
        self.assertAlmostEqual(position.get("contract_size") or 0.0, 0.0001)
        self.assertAlmostEqual(position.get("coin_qty") or 0.0, 0.0003)
        self.assertAlmostEqual(position.get("notional") or 0.0, 24.201063)
        self.assertEqual(gateway.client.positions_calls, [{"settle": "usdt"}])  # type: ignore[union-attr]
        self.assertEqual(
            gateway.client.contract_calls,  # type: ignore[union-attr]
            [{"settle": "usdt", "contract": "BTC_USDT"}],
        )


class _FakeGateClient:
    def __init__(self, *, fail_add: bool = False) -> None:
        self.fail_add = fail_add
        self.markets = {
            "ORCA/USDT:USDT": {
                "id": "ORCA_USDT",
                "symbol": "ORCA/USDT:USDT",
                "settle": "usdt",
                "settleId": "USDT",
                "swap": True,
            }
        }
        self.currencies = {
            "USDT": {"precision": 8},
            "usdt": {"precision": 8},
        }
        self.add_margin_calls: list[tuple[str, float, dict]] = []
        self.reduce_margin_calls: list[tuple[str, float, dict]] = []
        self.dual_margin_calls: list[dict] = []

    async def load_markets(self) -> dict:
        return self.markets

    def market(self, symbol: str) -> dict:
        return self.markets[symbol]

    def number_to_string(self, value: float) -> str:
        return f"{float(value):.8f}".rstrip("0").rstrip(".")

    async def add_margin(self, symbol: str, amount: float, params: dict) -> dict:
        self.add_margin_calls.append((symbol, amount, dict(params)))
        if self.fail_add:
            raise RuntimeError('gate {"label":"INVALID_PROTOCOL","message":"invalid argument: #3"}')
        return {"status": "ok", "method": "add_margin"}

    async def reduce_margin(self, symbol: str, amount: float, params: dict) -> dict:
        self.reduce_margin_calls.append((symbol, amount, dict(params)))
        return {"status": "ok", "method": "reduce_margin"}

    async def privateFuturesPostSettleDualCompPositionsContractMargin(self, request: dict) -> dict:
        self.dual_margin_calls.append(dict(request))
        return {"status": "ok", "method": "dual_comp_margin"}


class _FakeGateway:
    def __init__(self, client: _FakeGateClient) -> None:
        self.client = client

    async def refresh_credentials_async(self, force_env: bool = True) -> None:
        _ = force_env

    async def ensure_client(self) -> None:
        return None

    def map_symbol(self, symbol: str) -> str:
        return symbol

    async def close(self) -> None:
        return None


class _DirectGateMarginClient:
    def __init__(self) -> None:
        self.futures_margin_calls: list[dict] = []
        self.add_margin_calls: list[tuple[str, float, dict]] = []

    async def load_markets(self) -> dict:
        raise RuntimeError("gate GET https://api.gateio.ws/api/v4/spot/currencies timeout")

    def number_to_string(self, value: float) -> str:
        return f"{float(value):.8f}".rstrip("0").rstrip(".")

    async def add_margin(self, symbol: str, amount: float, params: dict) -> dict:
        self.add_margin_calls.append((symbol, amount, dict(params)))
        return {"status": "unexpected"}

    async def privateFuturesPostSettlePositionsContractMargin(self, request: dict) -> dict:
        self.futures_margin_calls.append(dict(request))
        return {"status": "ok", "method": "single_margin"}


class _DirectGateGateway:
    def __init__(self, client: _DirectGateMarginClient) -> None:
        self.client = client

    async def refresh_credentials_async(self, force_env: bool = True) -> None:
        _ = force_env

    async def ensure_client(self) -> None:
        return None

    def map_symbol(self, symbol: str) -> str:
        return symbol

    async def close(self) -> None:
        return None


class AccountMonitorGateMarginTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_gate_dual_mode_add_uses_dual_endpoint(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _FakeGateClient()
        monitor._gateways = {"gate": _FakeGateway(client)}
        position = {
            "symbol": "ORCA/USDT:USDT",
            "exchange_symbol": "ORCA_USDT",
            "side": "long",
            "raw": {"info": {"mode": "dual_long"}},
        }
        result = await monitor._modify_margin(
            exchange="gate",
            position=position,
            amount=114.06611570247928,
            action="add",
        )
        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(len(client.add_margin_calls), 0)
        self.assertEqual(len(client.dual_margin_calls), 1)
        request = client.dual_margin_calls[0]
        self.assertEqual(request.get("contract"), "ORCA_USDT")
        self.assertEqual(request.get("dual_side"), "dual_long")
        self.assertEqual(request.get("change"), "114.0661157")

    async def test_gate_add_falls_back_to_dual_endpoint_on_invalid_protocol(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _FakeGateClient(fail_add=True)
        monitor._gateways = {"gate": _FakeGateway(client)}
        position = {
            "symbol": "ORCA/USDT:USDT",
            "exchange_symbol": "ORCA_USDT",
            "side": "short",
            "raw": {"info": {"mode": "single"}},
        }
        result = await monitor._modify_margin(
            exchange="gate",
            position=position,
            amount=114.06611570247928,
            action="add",
        )
        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(len(client.add_margin_calls), 1)
        self.assertAlmostEqual(client.add_margin_calls[0][1], 114.0661157)
        self.assertEqual(len(client.dual_margin_calls), 1)
        request = client.dual_margin_calls[0]
        self.assertEqual(request.get("dual_side"), "dual_short")
        self.assertEqual(request.get("change"), "114.0661157")

    async def test_gate_single_margin_uses_direct_futures_endpoint_when_markets_timeout(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _DirectGateMarginClient()
        monitor._gateways = {"gate": _DirectGateGateway(client)}
        position = {
            "symbol": "H/USDT:USDT",
            "exchange_symbol": "H_USDT",
            "side": "long",
            "raw": {"info": {"contract": "H_USDT", "mode": "single"}},
        }
        result = await monitor._modify_margin(
            exchange="gate",
            position=position,
            amount=12.345678901,
            action="add",
        )
        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(client.add_margin_calls, [])
        self.assertEqual(
            client.futures_margin_calls,
            [{"settle": "usdt", "contract": "H_USDT", "change": "12.3456789"}],
        )


class _FakeOkxClient:
    def __init__(self, *, max_add_amount: float) -> None:
        self.max_add_amount = float(max_add_amount)
        self.markets = {
            "SAHARA/USDT:USDT": {
                "id": "SAHARA-USDT-SWAP",
                "symbol": "SAHARA/USDT:USDT",
                "swap": True,
            }
        }
        self.markets_by_id = {
            "SAHARA-USDT-SWAP": self.markets["SAHARA/USDT:USDT"],
        }
        self.add_margin_calls: list[tuple[str, float, dict]] = []
        self.reduce_margin_calls: list[tuple[str, float, dict]] = []

    async def load_markets(self) -> dict:
        return self.markets

    async def add_margin(self, symbol: str, amount: float, params: dict) -> dict:
        self.add_margin_calls.append((symbol, amount, dict(params)))
        if float(amount) > self.max_add_amount:
            raise RuntimeError(
                'okx {"code":"59301","data":[],"msg":"Margin adjustment failed because it exceeds the maximum limit"}'
            )
        return {"status": "ok", "method": "add_margin"}

    async def reduce_margin(self, symbol: str, amount: float, params: dict) -> dict:
        self.reduce_margin_calls.append((symbol, amount, dict(params)))
        return {"status": "ok", "method": "reduce_margin"}


class _FakeOkxGateway:
    def __init__(self, client: _FakeOkxClient) -> None:
        self.client = client

    async def refresh_credentials_async(self, force_env: bool = True) -> None:
        _ = force_env

    async def ensure_client(self) -> None:
        return None

    def map_symbol(self, symbol: str) -> str:
        return symbol

    async def close(self) -> None:
        return None


class AccountMonitorOkxMarginTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_okx_add_margin_retries_smaller_amount_on_limit_error(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _FakeOkxClient(max_add_amount=70.0)
        monitor._gateways = {"okx": _FakeOkxGateway(client)}
        position = {
            "symbol": "SAHARA/USDT:USDT",
            "exchange_symbol": "SAHARA-USDT-SWAP",
            "side": "short",
            "raw": {"info": {"posSide": "short"}},
        }

        result = await monitor._modify_margin(
            exchange="okx",
            position=position,
            amount=100.0,
            action="add",
        )

        self.assertEqual(result.get("status"), "ok")
        self.assertAlmostEqual(result.get("requested_amount") or 0.0, 100.0)
        self.assertAlmostEqual(result.get("amount") or 0.0, 70.0)
        self.assertEqual(result.get("retry_amounts"), [90.0, 80.0, 70.0])
        self.assertEqual(len(client.add_margin_calls), 4)
        self.assertEqual(client.add_margin_calls[-1][2].get("posSide"), "short")

    async def test_okx_maybe_adjust_keeps_actual_amount_after_retry(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _FakeOkxClient(max_add_amount=150.0)
        monitor._gateways = {"okx": _FakeOkxGateway(client)}
        position = {
            "symbol": "SAHARA/USDT:USDT",
            "exchange_symbol": "SAHARA-USDT-SWAP",
            "side": "short",
            "margin_used": 1000.0,
            "raw": {"info": {"posSide": "short"}},
        }

        result = await monitor._maybe_adjust_isolated_margin(
            exchange="okx",
            position=position,
            balance_entry={"available": 500.0},
            buffer_pct=0.24,
        )

        self.assertEqual(result.get("status"), "ok")
        self.assertAlmostEqual(result.get("requested_amount") or 0.0, 250.0)
        self.assertAlmostEqual(result.get("amount") or 0.0, 150.0)
        self.assertEqual(result.get("retry_amounts"), [225.0, 200.0, 175.0, 150.0])

    async def test_okx_maybe_reduce_targets_buffer_back_to_target(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        client = _FakeOkxClient(max_add_amount=9999.0)
        monitor._gateways = {"okx": _FakeOkxGateway(client)}
        position = {
            "symbol": "SAHARA/USDT:USDT",
            "exchange_symbol": "SAHARA-USDT-SWAP",
            "side": "short",
            "margin_used": 1000.0,
            "raw": {"info": {"posSide": "short"}},
        }

        result = await monitor._maybe_reduce_isolated_margin(
            exchange="okx",
            position=position,
            buffer_pct=0.40,
        )

        self.assertEqual(result.get("status"), "ok")
        self.assertAlmostEqual(result.get("requested_amount") or 0.0, 250.0)
        self.assertAlmostEqual(result.get("amount") or 0.0, 250.0)
        self.assertEqual(len(client.reduce_margin_calls), 1)
        self.assertAlmostEqual(client.reduce_margin_calls[0][1], 250.0)


class AccountMonitorKucoinMarginTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_kucoin_adjust_margin_for_leverage_adds_only_when_above_target(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        monitor._modify_margin = AsyncMock(return_value={"status": "ok"})  # type: ignore[method-assign]
        position = {
            "exchange": "kucoin",
            "symbol": "ARIA/USDT:USDT",
            "side": "short",
            "margin_mode": "isolated",
            "raw": {
                "positionValue": "500",
                "posMargin": "100",
                "realLeverage": "5",
            },
        }

        result = await monitor._kucoin_adjust_margin_for_leverage(position, 3.0)

        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(result.get("action"), "add")
        self.assertAlmostEqual(result.get("current_leverage") or 0.0, 5.0)
        self.assertAlmostEqual(result.get("amount") or 0.0, 66.91666666666667, places=6)
        monitor._modify_margin.assert_awaited_once()
        kwargs = monitor._modify_margin.await_args.kwargs
        self.assertEqual(kwargs["exchange"], "kucoin")
        self.assertEqual(kwargs["action"], "add")

    async def test_kucoin_adjust_margin_for_leverage_never_reduces_below_target(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        monitor._modify_margin = AsyncMock(return_value={"status": "ok"})  # type: ignore[method-assign]
        position = {
            "exchange": "kucoin",
            "symbol": "ARIA/USDT:USDT",
            "side": "short",
            "margin_mode": "isolated",
            "raw": {
                "positionValue": "500",
                "posMargin": "260",
                "realLeverage": "1.92",
            },
        }

        result = await monitor._kucoin_adjust_margin_for_leverage(position, 3.0)

        self.assertEqual(result.get("status"), "ok")
        self.assertIsNone(result.get("action"))
        self.assertEqual(result.get("reason"), "topup_only_below_target")
        monitor._modify_margin.assert_not_awaited()

    async def test_kucoin_reduce_isolated_margin_is_blocked(self) -> None:
        monitor = AccountMonitor(refresh_interval=60, summary_interval=60)
        result = await monitor._maybe_reduce_isolated_margin(
            exchange="kucoin",
            position={
                "exchange": "kucoin",
                "symbol": "ARIA/USDT:USDT",
                "side": "short",
                "margin_mode": "isolated",
                "margin_used": 100.0,
            },
            buffer_pct=0.40,
        )

        self.assertEqual(result.get("status"), "skip")
        self.assertEqual(result.get("reason"), "kucoin_topup_only")


if __name__ == "__main__":
    unittest.main()
