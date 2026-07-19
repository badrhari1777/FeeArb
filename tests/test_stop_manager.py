from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import patch

from risk.config import RiskConfig
from risk.stop_manager import ProtectiveOrderManager, ProtectiveTarget, TakeTarget
from utils.cache_db import SymbolMeta


class _FakeClient:
    async def fetch_open_orders(self, symbol: str, params: dict | None = None):  # noqa: ARG002
        return []


class _FakeGateway:
    slug = "binance"

    def __init__(self) -> None:
        self.client = _FakeClient()

    def map_symbol(self, symbol: str) -> str:
        return symbol


class _RecordingOrderClient:
    def __init__(self) -> None:
        self.orders: list[dict[str, object]] = []

    async def create_order(self, symbol: str, type: str, side: str, amount: float, params: dict | None = None):
        self.orders.append(
            {
                "symbol": symbol,
                "type": type,
                "side": side,
                "amount": amount,
                "params": dict(params or {}),
            }
        )
        return {"id": "1"}


class _RecordingGateway:
    def __init__(self, slug: str, client: _RecordingOrderClient) -> None:
        self.slug = slug
        self.client = client

    def map_symbol(self, symbol: str) -> str:
        return symbol


class _RetryRecordingGateway(_RecordingGateway):
    def __init__(self, slug: str, client: _RecordingOrderClient) -> None:
        super().__init__(slug, client)
        self.retry_operations: list[str] = []

    async def _call_with_time_sync_retry(self, operation: str, callback):
        self.retry_operations.append(operation)
        return await callback()


class StopManagerBinanceAlgoSideTestCase(unittest.TestCase):
    def test_fetch_existing_keeps_position_side_for_invalid_side_check(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())

        async def _fake_fetch_algo(_gateway, _symbol):
            return [
                {
                    "algoId": "1000000001",
                    "symbol": "ZKPUSDT",
                    "side": "SELL",
                    "orderType": "STOP_MARKET",
                    "quantity": "55008.0",
                    "triggerPrice": "0.07315",
                    "reduceOnly": True,
                    "positionSide": "BOTH",
                }
            ]

        manager._fetch_binance_open_algo_orders = _fake_fetch_algo  # type: ignore[method-assign]

        async def _run() -> None:
            existing = await manager._fetch_existing(
                _FakeGateway(),
                "ZKPUSDT",
                None,
                "long",
                mark_price=0.10203,
                entry_price=0.10110,
            )
            self.assertFalse(existing.get("invalid_side"))
            self.assertEqual(len(existing.get("stop_orders") or []), 1)
            self.assertEqual(len(existing.get("take_orders") or []), 0)

        asyncio.run(_run())

    def test_place_binance_algo_conditional_uses_mark_price_and_price_protect(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        gateway = _FakeGateway()
        captured: dict[str, object] = {}
        manager._refresh_symbol_meta = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

        async def _fake_request(
            _gateway,
            *,
            method: str,
            path: str,
            params: dict[str, object],
        ):
            captured["method"] = method
            captured["path"] = path
            captured["params"] = dict(params)
            return {"ok": True}

        manager._binance_algo_request = _fake_request  # type: ignore[method-assign]

        async def _run() -> None:
            await manager._place_binance_algo_conditional(
                gateway,
                ProtectiveTarget(
                    stop=100.0,
                    takes=[],
                    quantity=2.0,
                    side="long",
                    exchange="binance",
                    symbol="BTCUSDT",
                    position_id="p1",
                    pos_side="both",
                ),
                order_type="STOP_MARKET",
                trigger_price=99.5,
                quantity=2.0,
            )

        asyncio.run(_run())
        params = captured.get("params") or {}
        self.assertEqual(captured.get("method"), "POST")
        self.assertEqual(captured.get("path"), "algoOrder")
        self.assertEqual(params.get("workingType"), "MARK_PRICE")
        self.assertEqual(params.get("priceProtect"), "TRUE")

    def test_place_binance_algo_conditional_uses_symbol_meta_precision_fallback(self) -> None:
        class _NoPrecisionClient(_FakeClient):
            def price_to_precision(self, _symbol, _price):  # noqa: D401
                raise RuntimeError("market missing")

            def amount_to_precision(self, _symbol, _amount):  # noqa: D401
                raise RuntimeError("market missing")

        manager = ProtectiveOrderManager(RiskConfig())
        gateway = _FakeGateway()
        gateway.client = _NoPrecisionClient()
        captured: dict[str, object] = {}
        manager._refresh_symbol_meta = lambda *_args, **_kwargs: SymbolMeta(  # type: ignore[method-assign]
            exchange="binance",
            symbol="LABUSDT",
            tick_size=0.0001,
            price_step=0.0001,
            qty_step=1.0,
        )

        async def _fake_request(
            _gateway,
            *,
            method: str,  # noqa: ARG001
            path: str,  # noqa: ARG001
            params: dict[str, object],
        ):
            captured["params"] = dict(params)
            return {"ok": True}

        manager._binance_algo_request = _fake_request  # type: ignore[method-assign]

        async def _run() -> None:
            await manager._place_binance_algo_conditional(
                gateway,
                ProtectiveTarget(
                    stop=100.0,
                    takes=[],
                    quantity=12.9,
                    side="long",
                    exchange="binance",
                    symbol="LAB/USDT:USDT",
                    position_id="p1",
                    pos_side="both",
                ),
                order_type="STOP_MARKET",
                trigger_price=14.818691234,
                quantity=12.9,
            )

        asyncio.run(_run())
        params = captured.get("params") or {}
        self.assertEqual(params.get("symbol"), "LAB/USDT:USDT")
        self.assertEqual(params.get("triggerPrice"), "14.8187")
        self.assertEqual(params.get("quantity"), "12")


class StopManagerTriggerBasisParamsTestCase(unittest.TestCase):
    def test_bybit_stop_and_take_use_mark_price_trigger(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RecordingGateway("bybit", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="long",
            exchange="bybit",
            symbol="BTCUSDT",
            position_id="p1",
        )

        async def _run() -> None:
            await manager._place_stop(gateway, target, 99.5)
            await manager._place_take(gateway, target, 105.0)

        asyncio.run(_run())
        stop_params = client.orders[0]["params"]
        take_params = client.orders[1]["params"]
        self.assertEqual(stop_params.get("slTriggerBy"), "MarkPrice")
        self.assertEqual(take_params.get("tpTriggerBy"), "MarkPrice")

    def test_okx_stop_and_take_use_mark_trigger_type(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RecordingGateway("okx", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="short",
            exchange="okx",
            symbol="BTC-USDT-SWAP",
            position_id="p1",
            pos_side="short",
            margin_mode="isolated",
        )

        async def _run() -> None:
            await manager._place_stop(gateway, target, 101.0)
            await manager._place_take(gateway, target, 95.0)

        asyncio.run(_run())
        stop_params = client.orders[0]["params"]
        take_params = client.orders[1]["params"]
        self.assertEqual(stop_params.get("slTriggerPxType"), "mark")
        self.assertEqual(take_params.get("tpTriggerPxType"), "mark")

    def test_bitget_stop_and_take_use_uta_strategy_mark_trigger(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RecordingGateway("bitget", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="short",
            exchange="bitget",
            symbol="BTCUSDT",
            position_id="p1",
        )

        async def _run() -> None:
            await manager._place_stop(gateway, target, 101.0)
            await manager._place_take(gateway, target, 95.0)

        asyncio.run(_run())
        stop_params = client.orders[0]["params"]
        take_params = client.orders[1]["params"]
        self.assertIs(stop_params.get("uta"), True)
        self.assertIs(take_params.get("uta"), True)
        self.assertEqual(stop_params.get("slTriggerBy"), "mark")
        self.assertEqual(take_params.get("tpTriggerBy"), "mark")
        self.assertEqual(stop_params.get("posSide"), "short")
        self.assertEqual(take_params.get("posSide"), "short")
        self.assertNotIn("planType", stop_params)
        self.assertNotIn("planType", take_params)

    def test_bitget_classic_stop_and_take_use_contract_tpsl_mark_trigger(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RecordingGateway("bitget", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="short",
            exchange="bitget",
            symbol="BTCUSDT",
            position_id="p1",
        )

        async def _run() -> None:
            with patch.dict("os.environ", {"BITGET_ACCOUNT_MODE": "classic"}):
                await manager._place_stop(gateway, target, 101.0)
                await manager._place_take(gateway, target, 95.0)

        asyncio.run(_run())
        stop_params = client.orders[0]["params"]
        take_params = client.orders[1]["params"]
        self.assertEqual(stop_params.get("triggerType"), "mark_price")
        self.assertEqual(take_params.get("triggerType"), "mark_price")
        self.assertEqual(stop_params.get("planType"), "loss_plan")
        self.assertEqual(take_params.get("planType"), "profit_plan")
        self.assertEqual(stop_params.get("executePrice"), "0")
        self.assertEqual(take_params.get("executePrice"), "0")
        self.assertEqual(stop_params.get("holdSide"), "short")
        self.assertEqual(take_params.get("holdSide"), "short")
        self.assertNotIn("slTriggerType", stop_params)
        self.assertNotIn("tpTriggerType", take_params)
        self.assertNotIn("tpslMode", stop_params)
        self.assertNotIn("tpslMode", take_params)

    def test_kucoin_stop_and_take_use_mark_price_type(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RecordingGateway("kucoin", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="long",
            exchange="kucoin",
            symbol="BTCUSDTM",
            position_id="p1",
            margin_mode="isolated",
        )

        async def _run() -> None:
            await manager._place_stop(gateway, target, 99.0)
            await manager._place_take(gateway, target, 104.0)

        asyncio.run(_run())
        stop_params = client.orders[0]["params"]
        take_params = client.orders[1]["params"]
        self.assertEqual(stop_params.get("stopPriceType"), "MP")
        self.assertEqual(take_params.get("stopPriceType"), "MP")

    def test_gate_stop_and_take_use_mark_price_type(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RecordingGateway("gate", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="long",
            exchange="gate",
            symbol="BTC_USDT",
            position_id="p1",
        )

        async def _run() -> None:
            await manager._place_stop(gateway, target, 99.0)
            await manager._place_take(gateway, target, 104.0)

        asyncio.run(_run())
        stop_params = client.orders[0]["params"]
        take_params = client.orders[1]["params"]
        self.assertEqual(stop_params.get("price_type"), 1)
        self.assertEqual(take_params.get("price_type"), 1)

    def test_bingx_stop_and_take_use_mark_price_working_type(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RecordingGateway("bingx", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="short",
            exchange="bingx",
            symbol="BTC-USDT",
            position_id="p1",
        )

        async def _run() -> None:
            await manager._place_stop(gateway, target, 101.0)
            await manager._place_take(gateway, target, 95.0)

        asyncio.run(_run())
        stop_params = client.orders[0]["params"]
        take_params = client.orders[1]["params"]
        self.assertEqual(stop_params.get("workingType"), "MARK_PRICE")
        self.assertEqual(take_params.get("workingType"), "MARK_PRICE")

    def test_kucoin_create_uses_gateway_time_sync_retry(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _RecordingOrderClient()
        gateway = _RetryRecordingGateway("kucoin", client)
        target = ProtectiveTarget(
            stop=100.0,
            takes=[],
            quantity=2.0,
            side="long",
            exchange="kucoin",
            symbol="BTCUSDTM",
            position_id="p1",
            margin_mode="isolated",
        )

        async def _run() -> None:
            await manager._place_stop(gateway, target, 99.0)
            await manager._place_take(gateway, target, 104.0)

        asyncio.run(_run())
        self.assertEqual(gateway.retry_operations, ["create_order", "create_order"])


class StopManagerStaleGuardTestCase(unittest.TestCase):
    def test_needs_stop_update_when_order_is_stale(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        old_ts = time.time() - 600.0
        should_update, _delta = manager._needs_stop_update(
            [{"id": "1", "price": 100.0, "qty": 1.0, "created_ts": old_ts}],
            target_stop=100.2,  # 0.2% delta (below 0.5% threshold)
            target_qty=1.0,
            price_threshold=0.005,
            qty_threshold=0.01,
            max_age_sec=120,
        )
        self.assertTrue(should_update)

    def test_needs_stop_update_unchanged_when_recent_and_within_threshold(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        recent_ts = time.time() - 10.0
        should_update, _delta = manager._needs_stop_update(
            [{"id": "1", "price": 100.0, "qty": 1.0, "created_ts": recent_ts}],
            target_stop=100.2,  # 0.2% delta (below 0.5% threshold)
            target_qty=1.0,
            price_threshold=0.005,
            qty_threshold=0.01,
            max_age_sec=120,
        )
        self.assertFalse(should_update)

    def test_close_all_take_does_not_require_reported_quantity(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        should_update, delta = manager._needs_take_update(
            [{"id": "1", "price": 90.0, "qty": None, "close_all": True}],
            [TakeTarget(price=90.0, quantity=25000.0)],
            price_threshold=0.005,
            qty_threshold=0.01,
        )
        self.assertFalse(should_update)
        self.assertEqual(delta, 0.0)

    def test_short_fallback_take_is_bounded_away_from_zero(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig(fallback_take_rr_pct=1.0))
        price = manager._fallback_take_price(
            "short",
            mark_price=0.40,
            entry_price=None,
            ratio=1.0,
        )
        self.assertEqual(price, 0.20)

    def test_invalid_peer_take_uses_safe_fallback_side(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig(fallback_take_rr_pct=0.10))
        targets = manager._compute_targets(
            "HUSDT",
            [
                {
                    "exchange": "binance",
                    "symbol": "H/USDT:USDT",
                    "side": "short",
                    "contracts": 1000.0,
                    "mark_price": 0.40,
                    "entry_price": 0.35,
                    "liquidation_price": 0.60,
                },
                {
                    "exchange": "kucoin",
                    "symbol": "H/USDT:USDT",
                    "side": "long",
                    "contracts": 100.0,
                    "mark_price": 0.41,
                    "entry_price": 0.34,
                    "liquidation_price": 0.20,
                },
            ],
        )
        short_target = next(target for target in targets if target.side == "short")
        self.assertTrue(short_target.takes)
        self.assertLess(short_target.takes[0].price, short_target.mark_price or 0.0)


class _CacheClient:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch_open_orders(self, symbol: str, params: dict | None = None):  # noqa: ARG002
        self.calls += 1
        return [
            {
                "id": "1",
                "type": "stop_market",
                "side": "sell",
                "amount": 1.0,
                "stopPrice": 100.0,
                "reduceOnly": True,
                "timestamp": int(time.time() * 1000),
                "info": {"reduceOnly": True},
            }
        ]


class _CacheGateway:
    slug = "mexc"

    def __init__(self, client: _CacheClient) -> None:
        self.client = client

    def map_symbol(self, symbol: str) -> str:
        return symbol


class StopManagerExistingCacheTestCase(unittest.TestCase):
    def test_force_fetch_bypasses_existing_cache(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _CacheClient()
        gateway = _CacheGateway(client)

        async def _run() -> None:
            await manager._fetch_existing(gateway, "BTCUSDT", None, "long")
            await manager._fetch_existing(gateway, "BTCUSDT", None, "long")
            await manager._fetch_existing(gateway, "BTCUSDT", None, "long", force_fetch=True)

        asyncio.run(_run())
        self.assertEqual(client.calls, 2)


class _BitgetPlanClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def fetch_open_orders(self, symbol: str, params: dict | None = None):
        self.calls.append((symbol, dict(params or {})))
        if not params:
            return []
        if params.get("planType") != "profit_loss" and not (params.get("uta") and params.get("trigger")):
            return []
        return [
            {
                "id": "sl-plan",
                "type": "market",
                "side": "sell",
                "amount": 1.0,
                "stopLossPrice": 99.0,
                "reduceOnly": True,
                "timestamp": int(time.time() * 1000),
                "info": {
                    "orderId": "sl-plan",
                    "planType": "loss_plan",
                    "triggerPrice": "99",
                    "size": "1",
                    "posSide": "long",
                },
            },
            {
                "id": "tp-plan",
                "type": "market",
                "side": "sell",
                "amount": 1.0,
                "takeProfitPrice": 104.0,
                "reduceOnly": True,
                "timestamp": int(time.time() * 1000),
                "info": {
                    "orderId": "tp-plan",
                    "planType": "profit_plan",
                    "triggerPrice": "104",
                    "size": "1",
                    "posSide": "long",
                },
            },
        ]


class _BitgetPlanGateway:
    slug = "bitget"

    def __init__(self, client: _BitgetPlanClient) -> None:
        self.client = client

    def map_symbol(self, symbol: str) -> str:
        return symbol


class StopManagerBitgetPlanFetchTestCase(unittest.TestCase):
    def test_fetch_existing_includes_bitget_uta_strategy_orders(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _BitgetPlanClient()
        gateway = _BitgetPlanGateway(client)

        async def _run() -> dict:
            return await manager._fetch_existing(
                gateway,
                "BTC/USDT:USDT",
                None,
                "long",
                mark_price=100.0,
                entry_price=100.0,
                force_fetch=True,
            )

        result = asyncio.run(_run())
        self.assertEqual([call[1] for call in client.calls], [{"uta": True}, {"uta": True, "trigger": True}])
        self.assertEqual(result.get("order_ids"), ["sl-plan", "tp-plan"])
        self.assertEqual([item.get("id") for item in result.get("stop_orders") or []], ["sl-plan"])
        self.assertEqual([item.get("id") for item in result.get("take_orders") or []], ["tp-plan"])

    def test_fetch_existing_includes_bitget_classic_profit_loss_plan_orders(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _BitgetPlanClient()
        gateway = _BitgetPlanGateway(client)

        async def _run() -> dict:
            with patch.dict("os.environ", {"BITGET_ACCOUNT_MODE": "classic"}):
                return await manager._fetch_existing(
                    gateway,
                    "BTC/USDT:USDT",
                    None,
                    "long",
                    mark_price=100.0,
                    entry_price=100.0,
                    force_fetch=True,
                )

        result = asyncio.run(_run())
        self.assertEqual([call[1] for call in client.calls], [{}, {"trigger": True, "planType": "profit_loss"}])
        self.assertEqual(result.get("order_ids"), ["sl-plan", "tp-plan"])


class _SyncClient:
    def __init__(self, fail_ids: set[str] | None = None) -> None:
        self._fail_ids = set(fail_ids or set())
        self.cancel_calls: list[tuple[str, str, dict | None]] = []

    async def cancel_order(self, order_id: str, symbol: str, params: dict | None = None):
        self.cancel_calls.append((order_id, symbol, params))
        if order_id in self._fail_ids:
            raise RuntimeError("cancel_failed")
        return {"id": order_id}


class _SyncGateway:
    slug = "bybit"

    def __init__(self, client: _SyncClient) -> None:
        self.client = client

    async def refresh_credentials_async(self) -> None:
        return None

    async def ensure_client(self) -> None:
        return None

    def requires_cycle_close(self) -> bool:
        return False

    async def close(self) -> None:
        return None

    def map_symbol(self, symbol: str) -> str:
        return symbol


class _RetrySyncGateway(_SyncGateway):
    def __init__(self, client: _SyncClient) -> None:
        super().__init__(client)
        self.retry_operations: list[str] = []

    async def _call_with_time_sync_retry(self, operation: str, callback):
        self.retry_operations.append(operation)
        return await callback()


class _BitgetSyncGateway(_SyncGateway):
    slug = "bitget"


class _KucoinSyncGateway(_SyncGateway):
    slug = "kucoin"


class StopManagerCancelSafetyTestCase(unittest.TestCase):
    def test_cleanup_orphaned_protective_orders_cancels_existing_orders(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _SyncClient()
        gateway = _SyncGateway(client)
        manager._gateways = {"bybit": gateway}

        fetch_calls: list[tuple[str, bool]] = []

        async def _fake_fetch_existing(
            _gw,
            _symbol,
            _position_id,
            side,
            *,
            mark_price=None,  # noqa: ARG001
            entry_price=None,  # noqa: ARG001
            force_fetch: bool = False,
        ):
            fetch_calls.append((side, force_fetch))
            if len(fetch_calls) == 1:
                return {
                    "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                    "take_orders": [{"id": "old-take", "price": 90.0, "qty": 1.0}],
                    "unknown_orders": [],
                    "order_ids": ["old-stop", "old-take"],
                    "algo_order_ids": [],
                    "invalid_side": False,
                }
            return {
                "stop_orders": [],
                "take_orders": [],
                "unknown_orders": [],
                "order_ids": [],
                "algo_order_ids": [],
                "invalid_side": False,
            }

        manager._fetch_existing = _fake_fetch_existing  # type: ignore[method-assign]

        async def _run() -> list[dict]:
            return await manager.cleanup_orphaned_protective_orders("bybit", "BTCUSDT")

        result = asyncio.run(_run())
        self.assertEqual([item.get("status") for item in result], ["cleanup_cancelled", "cleanup_skipped_no_orders"])
        self.assertEqual([call[0] for call in client.cancel_calls], ["old-stop", "old-take"])
        self.assertEqual(fetch_calls, [("long", True), ("long", True), ("short", True)])

    def test_cleanup_orphaned_protective_orders_reports_pending_cancel(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _SyncClient()
        gateway = _SyncGateway(client)
        manager._gateways = {"bybit": gateway}

        fetch_calls = 0

        async def _fake_fetch_existing(
            _gw,
            _symbol,
            _position_id,
            _side,
            *,
            mark_price=None,  # noqa: ARG001
            entry_price=None,  # noqa: ARG001
            force_fetch: bool = False,  # noqa: ARG001
        ):
            nonlocal fetch_calls
            fetch_calls += 1
            return {
                "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                "take_orders": [],
                "unknown_orders": [],
                "order_ids": ["old-stop"],
                "algo_order_ids": [],
                "invalid_side": False,
            }

        manager._fetch_existing = _fake_fetch_existing  # type: ignore[method-assign]

        async def _run() -> list[dict]:
            return await manager.cleanup_orphaned_protective_orders("bybit", "BTCUSDT", sides=("long",))

        result = asyncio.run(_run())
        self.assertEqual(result[0].get("status"), "cleanup_cancel_pending")
        self.assertEqual(result[0].get("cancel_pending_ids"), ["old-stop"])
        self.assertEqual([call[0] for call in client.cancel_calls], ["old-stop"])
        self.assertEqual(fetch_calls, 2)

    def test_sync_leg_skips_new_orders_when_cancel_fails(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _SyncClient(fail_ids={"old-stop"})
        gateway = _SyncGateway(client)
        manager._gateways = {"bybit": gateway}

        fetch_force_flags: list[bool] = []
        place_stop_calls = 0

        async def _fake_fetch_existing(
            _gw,
            _symbol,
            _position_id,
            _side,
            *,
            mark_price=None,  # noqa: ARG001
            entry_price=None,  # noqa: ARG001
            force_fetch: bool = False,
        ):
            fetch_force_flags.append(force_fetch)
            return {
                "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                "take_orders": [],
                "unknown_orders": [],
                "order_ids": ["old-stop"],
                "algo_order_ids": [],
                "invalid_side": False,
            }

        async def _fake_place_stop(_gw, _target, _price):
            nonlocal place_stop_calls
            place_stop_calls += 1

        manager._fetch_existing = _fake_fetch_existing  # type: ignore[method-assign]
        manager._place_stop = _fake_place_stop  # type: ignore[method-assign]

        async def _run() -> dict:
            return await manager._sync_leg(
                ProtectiveTarget(
                    stop=101.0,
                    takes=[],
                    quantity=1.0,
                    side="long",
                    exchange="bybit",
                    symbol="BTCUSDT",
                    position_id="p1",
                )
            )

        result = asyncio.run(_run())
        self.assertEqual(result.get("status"), "cancel_failed")
        self.assertEqual(place_stop_calls, 0)
        self.assertEqual(fetch_force_flags, [False])
        self.assertEqual([call[0] for call in client.cancel_calls], ["old-stop"])

    def test_sync_leg_uses_gateway_time_sync_retry_for_cancel(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _SyncClient()
        gateway = _RetrySyncGateway(client)
        manager._gateways = {"bybit": gateway}

        async def _fake_fetch_existing(
            _gw,
            _symbol,
            _position_id,
            _side,
            *,
            mark_price=None,  # noqa: ARG001
            entry_price=None,  # noqa: ARG001
            force_fetch: bool = False,  # noqa: ARG001
        ):
            return {
                "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                "take_orders": [],
                "unknown_orders": [],
                "order_ids": ["old-stop"],
                "algo_order_ids": [],
                "invalid_side": False,
            }

        async def _fake_place_stop(_gw, _target, _price):
            return None

        manager._fetch_existing = _fake_fetch_existing  # type: ignore[method-assign]
        manager._place_stop = _fake_place_stop  # type: ignore[method-assign]

        async def _run() -> None:
            await manager._sync_leg(
                ProtectiveTarget(
                    stop=101.0,
                    takes=[],
                    quantity=1.0,
                    side="long",
                    exchange="bybit",
                    symbol="BTCUSDT",
                    position_id="p1",
                )
            )

        asyncio.run(_run())
        self.assertEqual(gateway.retry_operations, ["cancel_order"])

    def test_sync_leg_treats_kucoin_already_final_cancel_as_resolved(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _SyncClient(fail_ids={"old-stop"})

        async def _kucoin_cancel(order_id: str, symbol: str, params: dict | None = None):
            client.cancel_calls.append((order_id, symbol, params))
            raise RuntimeError('kucoinfutures {"msg":"The order cannot be canceled.","code":"100004"}')

        client.cancel_order = _kucoin_cancel  # type: ignore[method-assign]
        gateway = _KucoinSyncGateway(client)
        manager._gateways = {"kucoin": gateway}
        place_stop_calls = 0
        fetch_count = 0

        async def _fake_fetch_existing(
            _gw,
            _symbol,
            _position_id,
            _side,
            *,
            mark_price=None,  # noqa: ARG001
            entry_price=None,  # noqa: ARG001
            force_fetch: bool = False,
        ):
            nonlocal fetch_count
            fetch_count += 1
            if force_fetch:
                return {
                    "stop_orders": [],
                    "take_orders": [],
                    "unknown_orders": [],
                    "order_ids": [],
                    "algo_order_ids": [],
                    "invalid_side": False,
                }
            return {
                "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                "take_orders": [],
                "unknown_orders": [],
                "order_ids": ["old-stop"],
                "algo_order_ids": [],
                "invalid_side": False,
            }

        async def _fake_place_stop(_gw, _target, _price):
            nonlocal place_stop_calls
            place_stop_calls += 1

        manager._fetch_existing = _fake_fetch_existing  # type: ignore[method-assign]
        manager._place_stop = _fake_place_stop  # type: ignore[method-assign]

        async def _run() -> dict:
            return await manager._sync_leg(
                ProtectiveTarget(
                    stop=101.0,
                    takes=[],
                    quantity=1.0,
                    side="long",
                    exchange="kucoin",
                    symbol="BTCUSDTM",
                    position_id="p1",
                )
            )

        result = asyncio.run(_run())
        self.assertEqual(result.get("status"), "updated")
        self.assertEqual(place_stop_calls, 1)
        self.assertEqual(fetch_count, 2)
        self.assertEqual(client.cancel_calls, [("old-stop", "BTCUSDTM", {"stop": True})])

    def test_sync_leg_cancels_bitget_uta_tpsl_as_trigger_strategy_order(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _SyncClient()
        gateway = _BitgetSyncGateway(client)
        manager._gateways = {"bitget": gateway}

        fetch_calls = 0
        place_stop_calls = 0

        async def _fake_fetch_existing(
            _gw,
            _symbol,
            _position_id,
            _side,
            *,
            mark_price=None,  # noqa: ARG001
            entry_price=None,  # noqa: ARG001
            force_fetch: bool = False,  # noqa: ARG001
        ):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return {
                    "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                    "take_orders": [],
                    "unknown_orders": [],
                    "order_ids": ["old-stop"],
                    "algo_order_ids": [],
                    "invalid_side": False,
                }
            return {
                "stop_orders": [],
                "take_orders": [],
                "unknown_orders": [],
                "order_ids": [],
                "algo_order_ids": [],
                "invalid_side": False,
            }

        async def _fake_place_stop(_gw, _target, _price):
            nonlocal place_stop_calls
            place_stop_calls += 1

        manager._fetch_existing = _fake_fetch_existing  # type: ignore[method-assign]
        manager._place_stop = _fake_place_stop  # type: ignore[method-assign]

        async def _run() -> dict:
            return await manager._sync_leg(
                ProtectiveTarget(
                    stop=101.0,
                    takes=[],
                    quantity=1.0,
                    side="long",
                    exchange="bitget",
                    symbol="BTC/USDT:USDT",
                    position_id="p1",
                )
            )

        result = asyncio.run(_run())
        self.assertEqual(result.get("status"), "updated")
        self.assertEqual(place_stop_calls, 1)
        self.assertEqual(
            client.cancel_calls,
            [("old-stop", "BTC/USDT:USDT", {"uta": True, "trigger": True})],
        )

    def test_sync_leg_skips_new_orders_when_cancel_not_confirmed(self) -> None:
        manager = ProtectiveOrderManager(RiskConfig())
        client = _SyncClient()
        gateway = _SyncGateway(client)
        manager._gateways = {"bybit": gateway}

        fetch_force_flags: list[bool] = []
        fetch_calls = 0
        place_stop_calls = 0

        async def _fake_fetch_existing(
            _gw,
            _symbol,
            _position_id,
            _side,
            *,
            mark_price=None,  # noqa: ARG001
            entry_price=None,  # noqa: ARG001
            force_fetch: bool = False,
        ):
            nonlocal fetch_calls
            fetch_calls += 1
            fetch_force_flags.append(force_fetch)
            # Simulate stale open order snapshot even after cancel request.
            if fetch_calls == 1:
                return {
                    "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                    "take_orders": [],
                    "unknown_orders": [],
                    "order_ids": ["old-stop"],
                    "algo_order_ids": [],
                    "invalid_side": False,
                }
            return {
                "stop_orders": [{"id": "old-stop", "price": 100.0, "qty": 1.0}],
                "take_orders": [],
                "unknown_orders": [],
                "order_ids": ["old-stop"],
                "algo_order_ids": [],
                "invalid_side": False,
            }

        async def _fake_place_stop(_gw, _target, _price):
            nonlocal place_stop_calls
            place_stop_calls += 1

        manager._fetch_existing = _fake_fetch_existing  # type: ignore[method-assign]
        manager._place_stop = _fake_place_stop  # type: ignore[method-assign]

        async def _run() -> dict:
            return await manager._sync_leg(
                ProtectiveTarget(
                    stop=101.0,
                    takes=[],
                    quantity=1.0,
                    side="long",
                    exchange="bybit",
                    symbol="BTCUSDT",
                    position_id="p1",
                )
            )

        result = asyncio.run(_run())
        self.assertEqual(result.get("status"), "cancel_pending")
        self.assertEqual(result.get("cancel_pending_ids"), ["old-stop"])
        self.assertEqual(place_stop_calls, 0)
        self.assertEqual(fetch_force_flags, [False, True])
        self.assertEqual([call[0] for call in client.cancel_calls], ["old-stop"])


if __name__ == "__main__":
    unittest.main()
