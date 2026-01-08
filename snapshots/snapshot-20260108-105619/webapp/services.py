from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
import json
import time
from typing import Any, Callable, Dict, List, Literal, Optional
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline import (
    DataSnapshot,
    SourceSnapshot,
    build_snapshot_from_sources,
    collect_sources_async,
)
from orchestrator.models import MarketSnapshot
from project_settings import SettingsManager
from execution.manual import ManualTradeManager, _apply_price_offset
from execution import (
    ExecutionSettingsManager,
    WalletService,
    PositionManager,
    TelemetryClient,
)
from execution.accounts import _safe_float
from execution.allocator import Allocator
from execution.lifecycle import LifecycleController
from execution.settings import ExecutionSettings
from execution.accounts import AccountMonitor, normalize_symbol
from risk.config import default_risk_config, RiskConfig
from risk.stop_manager import ProtectiveOrderManager
from utils import purge_expired
from utils.cache_db import get_or_fetch_funding_history
from exchanges import get_adapter, normalize_exchange_name
from uuid import uuid4

RefreshResult = Literal["completed", "in_progress", "failed"]

logger = logging.getLogger(__name__)
DEFAULT_MANUAL_LEVERAGE = 3.0
funding_logger = logging.getLogger("funding")
if not funding_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    funding_logger.addHandler(handler)
funding_logger.setLevel(logging.INFO)
funding_logger.propagate = False


def _dedupe_settle(symbol: str | None) -> str:
    """Trim duplicated settle suffixes like USDTUSDT -> USDT to align lookup keys."""
    if not symbol:
        return ""
    normalized = normalize_symbol(symbol)
    for suffix in ("USDT", "USDC", "USD"):
        double = suffix + suffix
        while normalized.endswith(double):
            normalized = normalized[: -len(suffix)]
    return normalized


def _strip_settle(symbol: str) -> str:
    """Remove a single settle suffix (USDT/USDC/USD) for cross-venue matching."""
    upper = symbol.upper()
    for suffix in ("USDT", "USDC", "USD"):
        if upper.endswith(suffix):
            return upper[: -len(suffix)]
    return upper


def _ccxt_perp_symbol(symbol: str) -> str:
    """Best-effort CCXT perp notation (e.g. BTCUSDT -> BTC/USDT:USDT)."""
    normalized = normalize_symbol(symbol)
    for suffix in ("USDT", "USDC", "USD"):
        if normalized.endswith(suffix):
            base = normalized[: -len(suffix)]
            return f"{base}/{suffix}:{suffix}"
    return f"{normalized}/USDT:USDT"


def _fetch_json(url: str) -> dict:
    """Tiny helper around urlopen with a browser UA."""
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=15) as resp:
        return json.load(resp)


def _fetch_bybit_candles(symbol: str, limit: int) -> list[dict[str, Any]]:
    params = urlencode({"category": "linear", "symbol": symbol, "interval": "1", "limit": limit})
    url = f"https://api.bybit.com/v5/market/kline?{params}"
    data = _fetch_json(url)
    series = data.get("result", {}).get("list") or []
    candles: list[dict[str, Any]] = []
    for item in series:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        try:
            candles.append(
                {
                    "ts_ms": int(item[0]),
                    "open": _safe_float(item[1]),
                    "high": _safe_float(item[2]),
                    "low": _safe_float(item[3]),
                    "close": _safe_float(item[4]),
                    "volume": _safe_float(item[5]),
                }
            )
        except Exception:
            continue
    return candles


def _fetch_mexc_candles(symbol: str, limit: int) -> list[dict[str, Any]]:
    params = urlencode({"interval": "Min1", "limit": limit})
    url = f"https://contract.mexc.com/api/v1/contract/kline/{symbol}?{params}"
    data = _fetch_json(url)
    series = data.get("data") or []
    candles: list[dict[str, Any]] = []
    for item in series:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        try:
            ts_ms = int(item[0]) if item[0] else None
            candles.append(
                {
                    "ts_ms": ts_ms,
                    "open": _safe_float(item[1]),
                    "high": _safe_float(item[2]),
                    "low": _safe_float(item[3]),
                    "close": _safe_float(item[4]),
                    "volume": _safe_float(item[5]),
                }
            )
        except Exception:
            continue
    return candles


def _ccxt_client(exchange: str):
    """Return a ccxt client configured for perpetual swaps."""
    try:
        import ccxt  # type: ignore
    except Exception as exc:  # pylint: disable=broad-except
        raise RuntimeError("ccxt not available") from exc

    name = normalize_exchange_name(exchange)
    opts = {"options": {"defaultType": "swap"}}
    if name == "kucoin":
        return ccxt.kucoinfutures(opts)
    if name == "bybit":
        return ccxt.bybit(opts)
    if name == "mexc":
        return ccxt.mexc(opts)
    if name == "bitget":
        return ccxt.bitget(opts)
    if name == "okx":
        return ccxt.okx(opts)
    if name == "gate":
        return ccxt.gate(opts)
    if name == "bingx":
        return ccxt.bingx(opts)
    if name == "htx":
        return ccxt.huobi(opts)
    return None


def _fetch_candles_ccxt(exchange: str, canonical_symbol: str, limit: int) -> list[dict[str, Any]]:
    client = _ccxt_client(exchange)
    if client is None:
        return []
    try:
        client.load_markets()
    except Exception:  # pylint: disable=broad-except
        # load_markets is optional; continue best-effort.
        pass

    def _translate(symbol: str) -> str:
        # ccxt prefers slash notation. Try a few variants.
        perp = _ccxt_perp_symbol(symbol)
        base = _strip_settle(symbol)
        return perp if perp in getattr(client, "symbols", []) else perp

    candidates = [
        _translate(canonical_symbol),
        canonical_symbol,
    ]
    # Some exchanges expect dash separators (e.g. OKX uses BTC-USDT-SWAP as id but ccxt symbol is BTC/USDT:USDT).
    for symbol in getattr(client, "symbols", []) or []:
        upper = str(symbol).upper()
        if _strip_settle(canonical_symbol) in upper and ":USD" in upper:
            candidates.append(symbol)
    seen: set[str] = set()
    for cand in candidates:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        try:
            ohlcv = client.fetch_ohlcv(cand, timeframe="1m", limit=limit)
        except Exception:  # pylint: disable=broad-except
            continue
        candles: list[dict[str, Any]] = []
        for row in ohlcv or []:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            candles.append(
                {
                    "ts_ms": int(row[0]),
                    "open": _safe_float(row[1]),
                    "high": _safe_float(row[2]),
                    "low": _safe_float(row[3]),
                    "close": _safe_float(row[4]),
                    "volume": _safe_float(row[5]),
                }
            )
        if candles:
            return candles
    return []


def _load_funding_history_cached(
    exchange: str,
    exchange_symbol: str,
    canonical_symbol: str,
    limit: int,
    adapter: Any,
) -> list[dict]:
    """Fetch funding history with caching, falling back to adapter hook."""

    def _fetch() -> list[dict]:
        if hasattr(adapter, "funding_history"):
            try:
                return adapter.funding_history(canonical_symbol, limit=max(limit * 2, limit))
            except Exception:  # pylint: disable=broad-except
                return []
        return []

    try:
        return get_or_fetch_funding_history(
            normalize_exchange_name(exchange),
            exchange_symbol,
            _fetch,
            max_age_seconds=300,
            limit=limit,
        )
    except Exception:  # pylint: disable=broad-except
        return []


class DataService:
    def __init__(self, settings_manager: SettingsManager | None = None) -> None:
        self._settings_manager = settings_manager or SettingsManager()
        self._parser_interval = self._settings_manager.current.parser_refresh_seconds
        self._exchange_interval = self._settings_manager.current.exchange_refresh_seconds
        self._account_interval = self._settings_manager.current.account_refresh_seconds
        self._summary_interval = self._settings_manager.current.summary_refresh_seconds
        self._snapshot: Optional[DataSnapshot] = None
        self._cached_sources: Optional[SourceSnapshot] = None
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._bootstrap_task: Optional[asyncio.Task] = None
        self._status: str = "idle"
        self._last_error: Optional[str] = None
        self._last_refreshed: Optional[datetime] = None
        self._last_source_refresh: Optional[datetime] = None
        self._in_progress: bool = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._events: List[dict[str, Any]] = []
        self._exchange_status: Dict[str, dict[str, Any]] = {}
        self._funding_cache: dict[tuple[str, str], tuple[float | None, str | None, float | None, float]] = {}
        self._exec_settings_manager = ExecutionSettingsManager()
        self._execution_settings: ExecutionSettings = self._exec_settings_manager.current
        self._wallet = WalletService(self._execution_settings.balance.initial_balances)
        self._positions = PositionManager(self._wallet)
        self._allocator = Allocator(self._wallet, self._positions, self._execution_settings)
        self._lifecycle = LifecycleController(self._execution_settings, self._positions, self._allocator)
        self._telemetry = TelemetryClient(self._execution_settings)
        self._telemetry_events: List[dict[str, Any]] = []
        self._telemetry.register_listener(self._handle_telemetry_event)
        self._accounts = AccountMonitor(
            refresh_interval=self._account_interval,
            summary_interval=self._summary_interval,
        )
        self._risk_config: RiskConfig = self._risk_config_from_settings()
        self._protective_manager = ProtectiveOrderManager(self._risk_config)
        self._last_protective: dict[tuple[str, str, str], dict[str, float | None]] = {}
        self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", 180)
        self._protective_task: Optional[asyncio.Task] = None
        self._manual = ManualTradeManager()
        self._manual_runs: Dict[str, dict[str, Any]] = {}
        self._manual_run_ttl = 3600
        self._mexc_alert_cooldown = 600  # seconds
        self._last_mexc_alert: dict[tuple[str, str], float] = {}
        self._send_missing_stop_alerts = True
        self._apply_alert_settings()

    def _extend_universe_with_positions(self, sources: SourceSnapshot) -> SourceSnapshot:
        """Include symbols from live positions so market snapshots stay fresh for the UI."""
        # Keep “universe” strictly for opportunity discovery; positions are handled separately.
        return sources


    async def startup(self) -> None:
        self._loop = asyncio.get_running_loop()
        purge_expired()
        async with self._lock:
            self._status = "pending"
            self._parser_interval = self._settings_manager.current.parser_refresh_seconds
            self._exchange_interval = self._settings_manager.current.exchange_refresh_seconds
            self._account_interval = self._settings_manager.current.account_refresh_seconds
        await self._accounts.start()
        # Do an immediate balance/positions pull before other work.
        await self._accounts.refresh_now(force_env=True)
        await self._maybe_sync_protective_orders()
        if self._task is None:
            await self._restart_scheduler()
        if self._bootstrap_task is None or self._bootstrap_task.done():
            self._bootstrap_task = asyncio.create_task(self.refresh_markets())
        if self._protective_task is None:
            self._protective_task = asyncio.create_task(self._protective_scheduler())
        await self._telemetry.start()

    async def shutdown(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._bootstrap_task:
            self._bootstrap_task.cancel()
            try:
                await self._bootstrap_task
            except asyncio.CancelledError:
                pass
            self._bootstrap_task = None
        if self._protective_task:
            self._protective_task.cancel()
            try:
                await self._protective_task
            except asyncio.CancelledError:
                pass
            self._protective_task = None
        await self._telemetry.stop()
        await self._accounts.stop()

    async def _scheduler(self) -> None:
        try:
            while True:
                interval = max(self._exchange_interval, 1)
                await asyncio.sleep(interval)
                result = await self.refresh_markets(
                    force_sources=self._sources_due(),
                )
                if result == "failed":
                    logger.warning(
                        "Scheduled snapshot refresh failed; will retry after interval."
                    )
        except asyncio.CancelledError:
            raise

    async def _restart_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._task = asyncio.create_task(self._scheduler())

    async def _protective_scheduler(self) -> None:
        """Independent loop for balance/position driven protective upkeep."""
        try:
            while True:
                interval = max(30, int(self._protective_interval or self._account_interval))
                await asyncio.sleep(interval)
                await self._maybe_sync_protective_orders()
        except asyncio.CancelledError:
            raise

    async def _restart_protective_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._protective_task:
            self._protective_task.cancel()
            try:
                await self._protective_task
            except asyncio.CancelledError:
                pass
            self._protective_task = None
        self._protective_task = asyncio.create_task(self._protective_scheduler())

    def _sources_due(self) -> bool:
        if self._cached_sources is None or self._last_source_refresh is None:
            return True
        age = datetime.now(timezone.utc) - self._last_source_refresh
        return age.total_seconds() >= max(self._parser_interval, 1)

    async def refresh_markets(self, *, force_sources: bool = True) -> RefreshResult:
        async with self._lock:
            if self._in_progress:
                return "in_progress"
            self._in_progress = True
            self._status = "pending"
            self._last_error = None
            self._events = []
            self._exchange_status = {}
        self._record_event(
            "refresh:start",
            {"message": "Snapshot refresh started"},
        )

        outcome: RefreshResult = "completed"
        loop = self._loop or asyncio.get_running_loop()
        progress_cb = self._make_progress_callback(loop)
        current_settings = self._settings_manager.current
        source_flags = dict(current_settings.sources)
        exchange_flags = dict(current_settings.exchanges)
        sources: Optional[SourceSnapshot] = self._cached_sources

        need_sources = (
            force_sources
            or sources is None
            or self._sources_due()
        )
        if need_sources:
            try:
                sources = await collect_sources_async(
                    progress_cb,
                    source_settings=source_flags,
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Source refresh raised an error")
                self._record_event(
                    "sources:failed",
                    {"message": "Source refresh failed", "error": str(exc)},
                )
                if self._cached_sources is None:
                    outcome = "failed"
                    self._record_event(
                        "refresh:failed",
                        {
                            "message": "Snapshot refresh failed (no cached sources)",
                            "error": str(exc),
                        },
                    )
                    async with self._lock:
                        self._last_error = str(exc)
                        self._status = "error"
                        self._in_progress = False
                    return outcome
                sources = self._cached_sources
                # attach warning for downstream reporting
                warning_message = "Source refresh failed; using cached data."
                if warning_message not in sources.messages:
                    sources.messages.append(warning_message)
            else:
                self._cached_sources = sources
                self._last_source_refresh = sources.generated_at

        sources = self._extend_universe_with_positions(sources)
        try:
            snapshot = await build_snapshot_from_sources(
                sources,
                progress_cb=progress_cb,
                exchange_settings=exchange_flags,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Snapshot refresh raised an error")
            outcome = "failed"
            self._record_event(
                "refresh:failed",
                {"message": "Snapshot refresh failed", "error": str(exc)},
            )
            async with self._lock:
                self._last_error = str(exc)
                self._status = "error"
        else:
            self._record_event(
                "refresh:completed",
                {
                    "message": "Snapshot refresh completed successfully",
                    "opportunity_count": len(snapshot.opportunities),
                },
            )
            async with self._lock:
                self._snapshot = snapshot
                self._status = "ready"
                self._last_error = None
                self._last_refreshed = datetime.now(timezone.utc)
                self._parser_interval = current_settings.parser_refresh_seconds
                self._exchange_interval = current_settings.exchange_refresh_seconds
                self._exchange_status = {
                    entry.get("exchange", f"exchange-{idx}"): entry
                    for idx, entry in enumerate(snapshot.exchange_status)
                }
        finally:
            async with self._lock:
                self._in_progress = False

        return outcome

    async def on_settings_updated(self) -> None:
        async with self._lock:
            current = self._settings_manager.current
            self._parser_interval = current.parser_refresh_seconds
            self._exchange_interval = current.exchange_refresh_seconds
            self._account_interval = current.account_refresh_seconds
            self._summary_interval = current.summary_refresh_seconds
            self._risk_config = self._risk_config_from_settings()
            self._protective_manager.update_config(self._risk_config)
            self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", self._protective_interval)
            self._apply_alert_settings()
        await self._restart_scheduler()
        await self._restart_protective_scheduler()
        self._accounts.update_interval(self._account_interval)
        self._accounts.update_summary_interval(self._summary_interval)
        # Kick an async refresh so UI sees new cadence sooner.
        asyncio.create_task(self._accounts.refresh_now(force_env=True))

    async def manual_enter(self, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("dry_run"):
            payload = dict(payload)
            payload.setdefault("constraints_exchanges", self._manual_constraints_exchanges())
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.enter(payload)
        return await self._start_manual_run("enter", payload, None)

    async def manual_exit(self, payload: dict[str, Any]) -> dict[str, Any]:
        positions = self._accounts.snapshot().get("positions") or []
        if payload.get("dry_run"):
            payload = dict(payload)
            payload.setdefault("constraints_exchanges", self._manual_constraints_exchanges())
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.exit(payload, positions)
        return await self._start_manual_run("exit", payload, positions)

    async def manual_roll(self, payload: dict[str, Any]) -> dict[str, Any]:
        positions = self._accounts.snapshot().get("positions") or []
        if payload.get("dry_run"):
            payload = dict(payload)
            payload.setdefault("constraints_exchanges", self._manual_constraints_exchanges())
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.roll(payload, positions)
        return await self._start_manual_run("roll", payload, positions)

    async def manual_analyze(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload)
        payload.setdefault("constraints_exchanges", self._manual_constraints_exchanges())
        return await self._manual.analyze(payload)

    def _manual_constraints_exchanges(self) -> list[str]:
        enabled = self._settings_manager.enabled_analysis_exchanges()
        return [name for name, is_enabled in enabled.items() if is_enabled]

    async def manual_test_limit(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._manual_test_order(payload, order_type="limit")

    async def manual_test_market(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._manual_test_order(payload, order_type="market")

    async def manual_test_cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
        symbol = normalize_symbol(str(payload.get("symbol") or ""))
        order_id = str(payload.get("order_id") or "").strip()
        if not exchange or not symbol or not order_id:
            return {"errors": ["exchange, symbol, and order_id are required"]}
        client, error = await self._manual_test_client(exchange)
        if not client:
            return {"errors": [error or "client_unavailable"]}
        ccxt_symbol = await self._manual._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return {"errors": [f"{exchange}: unable to resolve symbol {symbol}"]}
        try:
            result = await client.cancel_order(order_id, ccxt_symbol)
        except Exception as exc:  # pylint: disable=broad-except
            return {"errors": [str(exc)]}
        return {
            "exchange": exchange,
            "symbol": symbol,
            "ccxt_symbol": ccxt_symbol,
            "order_id": order_id,
            "status": "cancel_requested",
            "result": result,
        }

    async def _manual_test_client(self, exchange: str) -> tuple[Any | None, str | None]:
        gateway = self._manual._gateways.get(exchange)
        if gateway is None:
            return None, f"{exchange}: gateway unavailable"
        await gateway.refresh_credentials_async(force_env=True)
        await gateway.ensure_client()
        if gateway.client is None:
            return None, gateway.unavailable_reason or "client unavailable"
        return gateway.client, None

    async def _manual_test_order(self, payload: dict[str, Any], *, order_type: str) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
        symbol = normalize_symbol(str(payload.get("symbol") or ""))
        side = str(payload.get("side") or "").lower()
        qty = _safe_float(payload.get("qty"))
        price = _safe_float(payload.get("price"))
        offset_bps = _safe_float(payload.get("offset_bps"))
        offset_ticks = int(_safe_float(payload.get("offset_ticks")) or 0)
        reduce_only = bool(payload.get("reduce_only"))
        position_side = str(payload.get("position_side") or "").strip()
        margin_mode = str(payload.get("margin_mode") or "").strip().upper()

        errors: list[str] = []
        if not exchange:
            errors.append("exchange is required")
        if not symbol:
            errors.append("symbol is required")
        if side not in ("buy", "sell"):
            errors.append("side must be buy or sell")
        if qty is None or qty <= 0:
            errors.append("qty must be > 0")
        if errors:
            return {"errors": errors}

        client, error = await self._manual_test_client(exchange)
        if not client:
            return {"errors": [error or "client_unavailable"]}

        ccxt_symbol = await self._manual._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return {"errors": [f"{exchange}: unable to resolve symbol {symbol}"]}

        if hasattr(client, "set_leverage"):
            leverage_params: dict[str, object] = {}
            mode = margin_mode.lower() if margin_mode else ""
            if mode in ("isolated", "cross"):
                if exchange == "okx":
                    leverage_params["tdMode"] = mode
                elif exchange == "bitget":
                    leverage_params["marginMode"] = mode
                else:
                    leverage_params["marginMode"] = mode
            try:
                await client.set_leverage(DEFAULT_MANUAL_LEVERAGE, ccxt_symbol, leverage_params or None)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("%s set_leverage failed: %s", exchange, exc)

        constraints = self._manual._extract_market_constraints(client, ccxt_symbol)
        contract_size = constraints.get("contract_size")
        qty_contracts = None
        order_qty = float(qty)
        if contract_size and contract_size > 0:
            qty_contracts = float(qty) / contract_size
            order_qty = qty_contracts
        orderbook = None
        best_bid = None
        best_ask = None
        try:
            orderbook = await client.fetch_order_book(ccxt_symbol, limit=5)
            bids = orderbook.get("bids") or []
            asks = orderbook.get("asks") or []
            best_bid = _safe_float(bids[0][0]) if bids else None
            best_ask = _safe_float(asks[0][0]) if asks else None
        except Exception:  # pylint: disable=broad-except
            orderbook = None
        if best_bid is None or best_ask is None:
            try:
                ticker = await client.fetch_ticker(ccxt_symbol)
                best_bid = best_bid or _safe_float(ticker.get("bid"))
                best_ask = best_ask or _safe_float(ticker.get("ask"))
                last_price = _safe_float(ticker.get("last")) or _safe_float(ticker.get("mark"))
                if best_bid is None and last_price is not None:
                    best_bid = last_price
                if best_ask is None and last_price is not None:
                    best_ask = last_price
            except Exception:  # pylint: disable=broad-except
                pass

        limit_price = price
        if order_type == "limit" and (limit_price is None or limit_price <= 0):
            base = best_bid if side == "buy" else best_ask
            limit_price = _apply_price_offset(
                base,
                side=side,
                offset_bps=offset_bps or 0.0,
                offset_ticks=offset_ticks or 0,
                price_step=constraints.get("price_step"),
                round_mode="passive",
            )
        if order_type == "limit" and (limit_price is None or limit_price <= 0):
            return {
                "errors": ["limit price unavailable (empty orderbook?)"],
                "exchange": exchange,
                "symbol": symbol,
                "ccxt_symbol": ccxt_symbol,
                "best_bid": best_bid,
                "best_ask": best_ask,
            }

        params: dict[str, Any] = {}
        if reduce_only:
            params["reduceOnly"] = True
        if exchange == "bitget" and not position_side:
            position_side = "net"
            if margin_mode:
                params["marginMode"] = margin_mode.lower()
        if exchange == "okx" and margin_mode:
            params["tdMode"] = margin_mode.lower()
        if exchange == "kucoin":
            margin_mode = margin_mode or "ISOLATED"
            params["marginMode"] = margin_mode
            params["marginType"] = margin_mode
        if position_side:
            params["posSide"] = position_side
            params["positionSide"] = position_side

        try:
            if order_type == "limit":
                order = await client.create_order(ccxt_symbol, "limit", side, order_qty, limit_price, params)
            else:
                order = await client.create_order(ccxt_symbol, "market", side, order_qty, None, params)
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc)
            if exchange == "kucoin" and ("330005" in message or "margin mode" in message.lower()):
                if hasattr(client, "set_margin_mode") and margin_mode:
                    try:
                        await client.set_margin_mode(margin_mode, ccxt_symbol)
                        if order_type == "limit":
                            order = await client.create_order(
                                ccxt_symbol,
                                "limit",
                                side,
                                order_qty,
                                limit_price,
                                params,
                            )
                        else:
                            order = await client.create_order(
                                ccxt_symbol,
                                "market",
                                side,
                                order_qty,
                                None,
                                params,
                            )
                    except Exception as retry_exc:  # pylint: disable=broad-except
                        return {"errors": [str(retry_exc)]}
                else:
                    return {"errors": ["kucoin set_margin_mode unavailable; switch margin mode in UI"]}
            elif exchange == "bitget" and "40774" in message:
                retry_params = dict(params)
                if params.get("posSide") == "net":
                    retry_params.pop("posSide", None)
                    retry_params.pop("positionSide", None)
                    retry_params["hedged"] = True
                    pos_side = "long" if side == "buy" else "short"
                    retry_params["posSide"] = pos_side
                    retry_params["positionSide"] = pos_side
                else:
                    retry_params.pop("hedged", None)
                    retry_params["posSide"] = "net"
                    retry_params["positionSide"] = "net"
                try:
                    if order_type == "limit":
                        order = await client.create_order(
                            ccxt_symbol,
                            "limit",
                            side,
                            order_qty,
                            limit_price,
                            retry_params,
                        )
                    else:
                        order = await client.create_order(
                            ccxt_symbol,
                            "market",
                            side,
                            order_qty,
                            None,
                            retry_params,
                        )
                except Exception as retry_exc:  # pylint: disable=broad-except
                    return {"errors": [str(retry_exc)]}
            else:
                return {"errors": [message]}

        return {
            "exchange": exchange,
            "symbol": symbol,
            "ccxt_symbol": ccxt_symbol,
            "order_type": order_type,
            "side": side,
            "qty": qty,
            "leverage": DEFAULT_MANUAL_LEVERAGE,
            "qty_contracts": qty_contracts,
            "contract_size": contract_size,
            "price": limit_price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "constraints": constraints,
            "reduce_only": reduce_only,
            "position_side": position_side or None,
            "margin_mode": margin_mode or None,
            "params": params,
            "order": order,
            "order_id": order.get("id") if isinstance(order, dict) else None,
        }

    async def manual_exec_status(self, exec_id: str) -> dict[str, Any]:
        self._prune_manual_runs()
        run = self._manual_runs.get(exec_id)
        if not run:
            return {"error": "execution_not_found"}
        return {
            "execution_id": exec_id,
            "status": run.get("status"),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
            "logs": list(run.get("logs") or []),
            "result": run.get("result"),
            "error": run.get("error"),
        }

    async def _start_manual_run(
        self,
        action: str,
        payload: dict[str, Any],
        positions: Optional[list[dict[str, Any]]],
    ) -> dict[str, Any]:
        self._prune_manual_runs()
        exec_id = uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        run: dict[str, Any] = {
            "execution_id": exec_id,
            "action": action,
            "status": "running",
            "created_at": now,
            "updated_at": now,
            "created_at_ts": time.time(),
            "logs": [],
            "result": None,
            "error": None,
        }
        self._manual_runs[exec_id] = run

        def _log_cb(entry: dict[str, Any]) -> None:
            logs = run["logs"]
            logs.append(entry)
            if len(logs) > 200:
                del logs[:-200]
            run["updated_at"] = datetime.now(timezone.utc).isoformat()

        async def _runner() -> None:
            try:
                if action == "enter":
                    result = await self._manual.enter(payload, log_cb=_log_cb)
                elif action == "exit":
                    result = await self._manual.exit(payload, positions or [], log_cb=_log_cb)
                elif action == "roll":
                    result = await self._manual.roll(payload, positions or [], log_cb=_log_cb)
                else:
                    result = {"errors": [f"unsupported manual action {action}"]}
                run["result"] = result
                if result.get("errors"):
                    run["status"] = "completed_with_errors"
                else:
                    run["status"] = "completed"
            except Exception as exc:  # pylint: disable=broad-except
                run["status"] = "failed"
                run["error"] = str(exc)
            run["updated_at"] = datetime.now(timezone.utc).isoformat()

        asyncio.create_task(_runner())
        return {"execution_id": exec_id, "status": "running"}

    def _prune_manual_runs(self) -> None:
        now = time.time()
        expired = [
            key
            for key, run in self._manual_runs.items()
            if (now - float(run.get("created_at_ts") or 0)) > self._manual_run_ttl
        ]
        for key in expired:
            self._manual_runs.pop(key, None)

    def latest_snapshot(self) -> Optional[DataSnapshot]:
        return self._snapshot

    def latest_snapshot_dict(self) -> dict[str, object] | None:
        if self._snapshot is None:
            return None
        return self._snapshot.as_dict()

    def state_payload(self) -> dict[str, object]:
        snapshot_dict = self._snapshot.as_dict() if self._snapshot else None
        status = self._status
        if status == "idle" and snapshot_dict:
            status = "ready"
        settings_payload = self._settings_manager.as_dict()
        parser_interval = int(
            settings_payload.get("parser_refresh_seconds", self._parser_interval)
        )
        table_interval = int(
            settings_payload.get("table_refresh_seconds", parser_interval)
        )
        exchange_interval = int(
            settings_payload.get("exchange_refresh_seconds", self._exchange_interval)
        )
        account_interval = int(
            settings_payload.get("account_refresh_seconds", self._account_interval)
        )
        summary_interval = int(
            settings_payload.get("summary_refresh_seconds", getattr(self, "_summary_interval", 1800))
        )
        return {
            "status": status,
            "refresh_interval": table_interval,
            "parser_refresh_interval": parser_interval,
            "exchange_refresh_interval": exchange_interval,
            "account_refresh_interval": account_interval,
            "summary_refresh_interval": summary_interval,
            "last_error": self._last_error,
            "last_updated": (
                self._last_refreshed.isoformat() if self._last_refreshed else None
            ),
            "snapshot": snapshot_dict,
            "refresh_in_progress": self._in_progress,
            "events": list(self._events),
            "exchange_status": list(self._exchange_status.values()),
            "settings": settings_payload,
            "execution": self._execution_state(),
            "accounts": self._account_state(),
        }

    def telemetry_backlog(self, limit: int = 50) -> List[dict[str, Any]]:
        return list(self._telemetry_events[-limit:])

    def _execution_state(self) -> dict[str, object]:
        return {
            "wallets": [
                {
                    "exchange": account.exchange,
                    "total": account.total_balance,
                    "available": account.available,
                    "reserved": account.reserved,
                    "in_positions": account.in_positions,
                }
                for account in self._wallet.accounts()
            ],
            "reservations": [
                {
                    "allocation_id": allocation.allocation_id,
                    "symbol": allocation.symbol,
                    "long_exchange": allocation.long_exchange,
                    "short_exchange": allocation.short_exchange,
                    "notional": allocation.notional,
                    "created_at": _fmt_ts(allocation.created_at),
                }
                for allocation in self._allocator.pending_allocations()
            ],
            "positions": [
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "strategy": position.strategy,
                    "status": position.status,
                    "notional": position.legs["long"].target_amount,
                    "hedged_at": _fmt_ts(position.hedged_at),
                    "observation_started": _fmt_ts(position.observation_started_at),
                    "exit_started": _fmt_ts(position.exit_started_at),
                }
                for position in self._positions.active_positions()
            ],
            "telemetry": list(self._telemetry_events),
        }

    def _reduction_candidates(
        self,
        grouped_positions: dict[str, list[dict[str, Any]]],
        balances: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not grouped_positions or not balances:
            return []
        risky: dict[str, dict[str, Any]] = {}
        for bal in balances:
            exchange = str(bal.get("exchange") or "").lower()
            if not exchange:
                continue
            total = _safe_float(bal.get("total"))
            available = _safe_float(bal.get("available"))
            margin_ratio = _safe_float(bal.get("margin_ratio"))
            if total is None:
                continue
            min_buffer = max(0.15 * total, 500)
            stress = False
            reason_bits = []
            if margin_ratio is not None and margin_ratio >= 0.8:
                stress = True
                reason_bits.append(f"margin_ratio={margin_ratio}")
            if available is not None and available < min_buffer:
                stress = True
                reason_bits.append(f"available={available}<{int(min_buffer)}")
            if stress:
                risky[exchange] = {
                    "margin_ratio": margin_ratio,
                    "available": available,
                    "reason": "; ".join(reason_bits) or "low_buffer",
                }
        if not risky:
            return []

        candidates: list[dict[str, Any]] = []
        for symbol, legs in grouped_positions.items():
            longs = [leg for leg in legs if leg.get("side") == "long"]
            shorts = [leg for leg in legs if leg.get("side") == "short"]
            for leg in legs:
                ex = str(leg.get("exchange") or "").lower()
                if ex not in risky:
                    continue
                opposite_pool = shorts if leg.get("side") == "long" else longs
                if not opposite_pool:
                    continue
                # Pick the largest opposite leg to pair against.
                opposite = max(opposite_pool, key=lambda item: abs(item.get("quantity") or 0.0))
                qty = abs(leg.get("quantity") or 0.0)
                opp_qty = abs(opposite.get("quantity") or 0.0)
                if qty <= 0 or opp_qty <= 0:
                    continue
                suggested_close = round(min(qty, opp_qty) * 0.25, 6)
                funding = leg.get("funding_rate")
                funding_cost = None
                if funding is not None:
                    funding_cost = funding if leg.get("side") == "long" else -funding
                reason = f"{risky[ex]['reason']}"
                if funding_cost is not None and funding_cost > 0:
                    reason += f"; funding_cost~{round(funding_cost*100,4)}%/int"
                candidates.append(
                    {
                        "exchange": leg.get("exchange"),
                        "symbol": symbol,
                        "side": leg.get("side"),
                        "quantity": qty,
                        "close_quantity": suggested_close,
                        "paired_exchange": opposite.get("exchange"),
                        "funding_rate": funding,
                        "margin_ratio": risky[ex].get("margin_ratio"),
                        "reason": reason,
                    }
                )

        return sorted(
            candidates,
            key=lambda item: (
                -(item.get("margin_ratio") or 0.0),
                -(item.get("funding_rate") or 0.0),
                -item.get("quantity", 0.0),
            ),
        )

    def _account_state(self) -> dict[str, object]:
        payload = self._accounts.snapshot()
        positions = payload.get("positions") or []
        balances = self._sanitize_balances(payload.get("balances") or [])
        payload["balances"] = balances
        market_lookup = self._market_snapshot_lookup()
        positions_by_symbol, grouped = self._positions_by_symbol(
            positions,
            return_grouped=True,
            market_lookup=market_lookup,
        )
        payload["positions_by_symbol"] = positions_by_symbol
        payload["reduction_candidates"] = self._reduction_candidates(grouped, balances)
        return payload

    @staticmethod
    def _sanitize_balances(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def _num(val: Any) -> float | None:
            try:
                return float(val)
            except Exception:  # pylint: disable=broad-except
                return None

        cleaned: list[dict[str, Any]] = []
        for row in rows:
            row = dict(row)
            for key in ("total", "available", "used", "margin_ratio", "equity", "buffer_pct", "initial_margin", "maintenance_margin"):
                val = row.get(key)
                row[key] = _num(val)
            cleaned.append(row)
        return cleaned

    def _positions_by_symbol(
        self,
        positions: List[dict[str, Any]],
        return_grouped: bool = False,
        market_lookup: Optional[dict[tuple[str, str], MarketSnapshot]] = None,
    ) -> tuple[List[dict[str, Any]], dict[str, list[dict[str, Any]]]] | List[dict[str, Any]]:
        if not positions:
            return ([], {}) if return_grouped else []
        market_lookup = market_lookup or {}
        grouped: dict[str, dict[str, Any]] = {}
        for entry in positions:
            symbol_norm = _dedupe_settle(
                entry.get("symbol_normalized") or normalize_symbol(entry.get("symbol"))
            )
            if not symbol_norm:
                continue
            lookup_symbols = [symbol_norm]
            base_symbol = _strip_settle(symbol_norm)
            if base_symbol and base_symbol not in lookup_symbols:
                lookup_symbols.append(base_symbol)
            container = grouped.setdefault(symbol_norm, {"symbol": symbol_norm, "legs": []})
            side = (entry.get("side") or "").lower()
            contracts = float(entry.get("contracts") or 0.0)
            contract_size = float(entry.get("contract_size") or 1.0)
            coin_qty = float(entry.get("coin_qty") or contracts * contract_size)
            funding_rate = None
            next_funding_iso = None
            signed_coin = -coin_qty if side == "short" else coin_qty
            notional = float(entry.get("notional") or 0.0)
            snapshot = None
            for sym in lookup_symbols:
                key = (str(entry.get("exchange")).lower(), sym)
                snapshot = market_lookup.get(key)
                if snapshot:
                    break
            entry_price = entry.get("entry_price")
            mark_price = entry.get("mark_price")
            unrealized = entry.get("unrealized_pnl")
            if snapshot:
                funding_rate = snapshot.funding_rate
                next_funding_iso = (
                    snapshot.next_funding_time.isoformat()
                    if snapshot.next_funding_time
                    else None
                )
                if mark_price is None and snapshot.mark_price is not None:
                    mark_price = snapshot.mark_price
            live_rate, live_next, live_mark = self._funding_live(
                entry.get("exchange"),
                entry.get("symbol"),
                symbol_norm,
                entry.get("exchange_symbol"),
            )
            if live_rate is not None:
                funding_rate = live_rate
            if live_next is not None:
                next_funding_iso = live_next
            if live_mark is not None:
                mark_price = live_mark
            if (
                unrealized is None
                and entry_price is not None
                and mark_price is not None
            ):
                try:
                    unrealized = (float(mark_price) - float(entry_price)) * signed_coin
                except Exception:  # pylint: disable=broad-except
                    unrealized = entry.get("unrealized_pnl")
            next_funding_eta = None
            if next_funding_iso:
                try:
                    nf_dt = datetime.fromisoformat(next_funding_iso)
                    delta = nf_dt - datetime.now(timezone.utc)
                    if delta.total_seconds() > 0:
                        hours, remainder = divmod(int(delta.total_seconds()), 3600)
                        minutes = remainder // 60
                        next_funding_eta = f"{hours}h {minutes:02d}m"
                    else:
                        next_funding_eta = "passed"
                except Exception:  # pylint: disable=broad-except
                    next_funding_eta = None
            # Drop non-numeric funding artifacts (e.g., stray strings)
            try:
                if funding_rate is not None:
                    funding_rate = float(funding_rate)
            except Exception:  # pylint: disable=broad-except
                funding_rate = None
            if mark_price is None and entry_price is not None:
                # Fallback to entry so we at least display and compute PnL as 0.
                mark_price = entry_price
            dist_to_liq_pct = None
            liq_price = entry.get("liquidation_price")
            if liq_price is not None and mark_price not in (None, 0):
                try:
                    dist_to_liq_pct = abs(float(liq_price) - float(mark_price)) / abs(float(mark_price)) * 100.0
                except Exception:  # pylint: disable=broad-except
                    dist_to_liq_pct = None
            stop_price = self._target_stop_price(side, liq_price, mark_price=mark_price, entry_price=entry_price)
            container["legs"].append(
                {
                    "exchange": entry.get("exchange"),
                    "side": side or None,
                    "quantity": signed_coin,
                    "amount": abs(notional) if notional else None,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": unrealized,
                    "funding_rate": funding_rate,
                    "next_funding": next_funding_iso,
                    "next_funding_eta": next_funding_eta,
                    "leverage": entry.get("leverage"),
                    "liquidation_price": entry.get("liquidation_price"),
                    "margin_mode": entry.get("margin_mode"),
                    "margin_used": entry.get("margin_used"),
                    "dist_to_liq_pct": dist_to_liq_pct,
                    "stop_price": stop_price,
                    "take_price": None,
                    "expected_funding": (
                        (
                            (funding_rate or 0.0)
                            * (abs(notional) if notional else 0.0)
                            * (-1.0 if side == "long" else 1.0)
                        )
                        if funding_rate is not None and notional
                        else None
                    ),
                }
            )

        rows: list[dict[str, Any]] = []
        grouped_simple: dict[str, list[dict[str, Any]]] = {}
        for symbol, data in sorted(grouped.items(), key=lambda item: item[0]):
            legs = sorted(data["legs"], key=lambda leg: (leg.get("exchange") or ""))
            grouped_simple[symbol] = legs
            # Derive mirrored take/stop with spread consideration for hedged pairs (any count >=2).
            longs = [l for l in legs if l.get("side") == "long"]
            shorts = [l for l in legs if l.get("side") == "short"]
            if longs and shorts:
                primary_long = longs[0]
                primary_short = shorts[0]
                long_stop = self._target_stop_price(
                    "long",
                    primary_long.get("liquidation_price"),
                    mark_price=_safe_float(primary_long.get("mark_price")),
                    entry_price=_safe_float(primary_long.get("entry_price")),
                )
                short_stop = self._target_stop_price(
                    "short",
                    primary_short.get("liquidation_price"),
                    mark_price=_safe_float(primary_short.get("mark_price")),
                    entry_price=_safe_float(primary_short.get("entry_price")),
                )
                # Spread-aware mirror: convert stop across exchanges via mark ratio.
                lm = _safe_float(primary_long.get("mark_price") or primary_long.get("entry_price"))
                sm = _safe_float(primary_short.get("mark_price") or primary_short.get("entry_price"))
                long_to_short_ratio = (sm / lm) if lm and sm else 1.0
                short_to_long_ratio = (lm / sm) if lm and sm else 1.0
                threshold = getattr(self._risk_config, "stop_requote_threshold_pct", 0.005)

                def _should_update(prev: float | None, new: float | None) -> tuple[bool, float | None]:
                    if new is None:
                        return False, prev
                    if prev is None or prev <= 0:
                        return True, new
                    try:
                        delta = abs(new - prev) / prev
                    except Exception:
                        delta = 1.0
                    if delta >= threshold:
                        return True, new
                    return False, prev

                def _apply_targets(leg: dict[str, Any], stop_target: float | None, take_target: float | None) -> None:
                    key = (
                        str(leg.get("exchange") or ""),
                        str(leg.get("symbol") or ""),
                        str(leg.get("side") or ""),
                    )
                    last = self._last_protective.get(key, {})
                    update_stop, stop_val = _should_update(last.get("stop"), stop_target)
                    update_take, take_val = _should_update(last.get("take"), take_target)
                    if update_stop or update_take:
                        self._last_protective[key] = {
                            "stop": stop_val,
                            "take": take_val,
                        }
                    leg["stop_price"] = stop_val
                    leg["take_price"] = take_val

                for leg in longs:
                    take_target = short_stop * short_to_long_ratio if short_stop is not None else None
                    _apply_targets(leg, leg.get("stop_price"), take_target)
                for leg in shorts:
                    take_target = long_stop * long_to_short_ratio if long_stop is not None else None
                    _apply_targets(leg, leg.get("stop_price"), take_target)
            rows.extend(
                [
                    {
                        "type": "leg",
                        "symbol": symbol,
                        **leg,
                    }
                    for leg in legs
                ]
            )
            summary = self._summarize_symbol(symbol, legs)
            if summary:
                rows.append(summary)
        if return_grouped:
            return rows, grouped_simple
        return rows

    def _funding_live(
        self,
        exchange: str | None,
        position_symbol: str | None,
        normalized_symbol: str,
        raw_exchange_symbol: str | None = None,
    ) -> tuple[float | None, str | None, float | None]:
        if not exchange:
            funding_logger.warning("funding failed exchange=? symbol=%s reason=no_exchange", normalized_symbol)
            return None, None, None
        try:
            adapter = get_adapter(normalize_exchange_name(exchange))
        except KeyError:
            funding_logger.warning(
                "funding failed exchange=%s symbol=%s reason=adapter_missing",
                exchange,
                normalized_symbol,
            )
            return None, None, None
        exchange_symbol = None

        canonical_symbol = _dedupe_settle(normalized_symbol)
        for suffix in ("UMCBL", "DMCBL", "SWAP", "PERP"):
            if canonical_symbol.endswith(suffix):
                canonical_symbol = canonical_symbol[: -len(suffix)]
                break

        candidates = [
            raw_exchange_symbol or "",
            position_symbol or "",
            canonical_symbol,
            normalized_symbol,
        ]
        for cand in candidates:
            if not cand:
                continue
            cand = _dedupe_settle(str(cand))
            mapped = None
            try:
                mapped = adapter.map_symbol(str(cand))
            except Exception:  # pylint: disable=broad-except
                mapped = None
            if mapped:
                # If mapping only adds duplicated suffixes, keep the original.
                if mapped.replace("_", "").replace("-", "") == cand.replace("_", "").replace("-", ""):
                    exchange_symbol = cand
                else:
                    exchange_symbol = mapped
                break
        if not exchange_symbol:
            exchange_symbol = _dedupe_settle(position_symbol or raw_exchange_symbol or normalized_symbol)

        key = (normalize_exchange_name(exchange), exchange_symbol or canonical_symbol)
        now_ts = datetime.now(tz=timezone.utc).timestamp()

        logger.info(
            "funding fetch start exchange=%s key=%s canonical=%s candidates=%s",
            exchange,
            key,
            canonical_symbol,
            candidates,
        )

        # Try live snapshot first for freshest funding; fallback to cached history (<=2m), then ccxt.
        try:
            snapshots = adapter.fetch_market_snapshots([canonical_symbol])
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Market snapshot fetch failed for %s %s: %s", exchange, canonical_symbol, exc)
            snapshots = []

        if snapshots:
            snap = snapshots[0]
            rate = getattr(snap, "funding_rate", None)
            next_time = getattr(snap, "next_funding_time", None)
            next_funding_iso = next_time.isoformat() if next_time else None
            mark_val = getattr(snap, "mark_price", None)
            if rate is not None or next_funding_iso is not None or mark_val is not None:
                funding_logger.info(
                    "funding ok source=snapshot exchange=%s symbol=%s rate=%s next=%s mark=%s",
                    exchange,
                    canonical_symbol,
                    rate,
                    next_funding_iso,
                    mark_val,
                )
                self._funding_cache[key] = (rate, next_funding_iso, mark_val, now_ts)
                return rate, next_funding_iso, mark_val

        def _fetch() -> list[dict]:
            if hasattr(adapter, "funding_history"):
                try:
                    # Pass canonical symbol; adapters will map appropriately.
                    return adapter.funding_history(canonical_symbol, limit=50)  # type: ignore[attr-defined]
                except Exception:  # pylint: disable=broad-except
                    return []
            return []

        history = get_or_fetch_funding_history(
            normalize_exchange_name(exchange),
            exchange_symbol,
            _fetch,
            max_age_seconds=120,
            limit=5,
        )
        if history:
            latest = next((item for item in history if item.get("rate") is not None), None)
            if latest is None:
                history = []
            else:
                rate = latest.get("rate")
                ts_ms = latest.get("ts_ms") or latest.get("timestamp")
                interval_hours = latest.get("interval_hours") or 8.0
                next_funding_iso = None
                mark_val = latest.get("mark_price")
                if ts_ms and isinstance(ts_ms, (int, float)) and ts_ms > 0:
                    try:
                        ts_ms = int(ts_ms)
                        next_ms = ts_ms + int((interval_hours or 8.0) * 3600 * 1000)
                        next_funding_iso = datetime.fromtimestamp(next_ms / 1000, tz=timezone.utc).isoformat()
                    except Exception:  # pylint: disable=broad-except
                        next_funding_iso = None
                self._funding_cache[key] = (rate, next_funding_iso, mark_val, now_ts)
                funding_logger.info(
                    "funding ok source=history exchange=%s symbol=%s rate=%s next=%s mark=%s",
                    exchange,
                    exchange_symbol,
                    rate,
                    next_funding_iso,
                    mark_val,
                )
                return rate, next_funding_iso, mark_val
        else:
            logger.debug("Funding history empty for %s %s", exchange, exchange_symbol)

        # Additional fallback for Bitget: use ccxt funding rate directly if history/snapshot failed.
        if normalize_exchange_name(exchange) == "bitget":
            try:
                import ccxt  # type: ignore

                client = ccxt.bitget({"options": {"defaultType": "swap"}})
                mapped = adapter.map_symbol(canonical_symbol) or canonical_symbol
                # Load markets to get consistent ids for exotic symbols.
                try:
                    client.load_markets()
                except Exception:  # pylint: disable=broad-except
                    pass
                # ccxt expects pair format SYMBOL/USDT:USDT; fall back to mapped contract and raw market ids.
                try_symbols = [
                    f"{canonical_symbol}/USDT:USDT",
                    mapped,
                    f"{canonical_symbol}USDT_UMCBL",
                    f"{canonical_symbol}USD_DMCBL",
                ]
                funding = None
                last_exc: Exception | None = None
                for cand in try_symbols:
                    if not cand:
                        continue
                    try:
                        funding = client.fetch_funding_rate(cand)
                        break
                    except Exception as exc:  # pylint: disable=broad-except
                        last_exc = exc
                        continue
                if funding:
                    rate = _safe_float(funding.get("fundingRate"))
                    next_ts = funding.get("fundingTimestamp")
                    next_iso = None
                    try:
                        if next_ts:
                            next_iso = datetime.fromtimestamp(float(next_ts) / 1000, tz=timezone.utc).isoformat()
                    except Exception:  # pylint: disable=broad-except
                        next_iso = None
                    mark_val = _safe_float(
                        funding.get("markPrice")
                        or funding.get("indexPrice")
                        or funding.get("mark")
                    )
                    funding_logger.info(
                        "funding ok source=ccxt exchange=%s symbol=%s rate=%s next=%s mark=%s",
                        exchange,
                        canonical_symbol,
                        rate,
                        next_iso,
                        mark_val,
                    )
                    self._funding_cache[key] = (rate, next_iso, mark_val, now_ts)
                    return rate, next_iso, mark_val
                if last_exc:
                    logger.debug("Bitget ccxt fallback attempts failed for %s: %s", canonical_symbol, last_exc)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Bitget ccxt fallback failed for %s: %s", canonical_symbol, exc)

        funding_logger.warning("funding failed exchange=%s symbol=%s reason=unavailable", exchange, canonical_symbol)
        return None, None, None

    def _summarize_symbol(self, symbol: str, legs: List[dict[str, Any]]) -> dict[str, Any] | None:
        if not legs:
            return None
        long_legs = [leg for leg in legs if (leg.get("side") or "").lower() == "long"]
        short_legs = [leg for leg in legs if (leg.get("side") or "").lower() == "short"]

        def _weighted_avg(items: List[dict[str, Any]], key: str, weight_key: str = "amount") -> float | None:
            total_w = 0.0
            total_v = 0.0
            for item in items:
                val = item.get(key)
                weight_raw = item.get(weight_key) or 0.0
                weight = abs(weight_raw) if weight_key == "quantity" else weight_raw
                if val is None:
                    continue
                total_w += weight
                total_v += float(val) * float(weight)
            if total_w <= 0:
                return None
            return total_v / total_w

        # Use coin quantities (absolute) to weight price averages across venues.
        long_entry = _weighted_avg(long_legs, "entry_price", weight_key="quantity")
        short_entry = _weighted_avg(short_legs, "entry_price", weight_key="quantity")
        long_mark = _weighted_avg(long_legs, "mark_price", weight_key="quantity")
        short_mark = _weighted_avg(short_legs, "mark_price", weight_key="quantity")
        long_funding = _weighted_avg(long_legs, "funding_rate", weight_key="quantity")
        short_funding = _weighted_avg(short_legs, "funding_rate", weight_key="quantity")

        def _spread_pct(a: float | None, b: float | None) -> float | None:
            if a is None or b is None or a == 0:
                return None
            return (a - b) / a * 100.0

        entry_diff_pct = _spread_pct(long_entry, short_entry)
        mark_diff_pct = _spread_pct(long_mark, short_mark)
        funding_spread = None
        if long_funding is not None and short_funding is not None:
            funding_spread = short_funding - long_funding

        net_quantity = sum(leg.get("quantity") or 0.0 for leg in legs)
        pnl_total = sum(leg.get("unrealized_pnl") or 0.0 for leg in legs)

        soonest_next = None
        for leg in legs:
            ts = leg.get("next_funding")
            if not ts:
                continue
            try:
                candidate = datetime.fromisoformat(ts)
            except Exception:  # pylint: disable=broad-except
                continue
            if soonest_next is None or candidate < soonest_next:
                soonest_next = candidate

        expected_total = None
        for leg in legs:
            val = leg.get("expected_funding")
            if val is None:
                continue
            expected_total = (expected_total or 0.0) + float(val)

        return {
            "type": "summary",
            "symbol": symbol,
            "exchange": "TOTAL",
            "quantity": net_quantity,
            "amount": None,
            "entry_price": entry_diff_pct,
            "mark_price": mark_diff_pct,
            "unrealized_pnl": pnl_total,
            "funding_rate": funding_spread,
            "expected_funding": expected_total,
            "next_funding": soonest_next.isoformat() if soonest_next else None,
            "long_entry_avg": long_entry,
            "short_entry_avg": short_entry,
            "long_mark_avg": long_mark,
            "short_mark_avg": short_mark,
        }

    def _market_snapshot_lookup(self) -> dict[tuple[str, str], MarketSnapshot]:
        if not self._snapshot or not self._snapshot.market_snapshots:
            return {}
        lookup: dict[tuple[str, str], MarketSnapshot] = {}
        for exchange, mapping in self._snapshot.market_snapshots.items():
            for snapshot in mapping.values():
                if isinstance(snapshot, MarketSnapshot):
                    key = (exchange.lower(), normalize_symbol(snapshot.symbol))
                    lookup[key] = snapshot
                elif isinstance(snapshot, dict):
                    symbol = snapshot.get("symbol")
                    funding = snapshot.get("funding_rate")
                    next_funding = snapshot.get("next_funding_time")
                    mark_price = snapshot.get("mark_price")
                    key = (exchange.lower(), normalize_symbol(symbol))
                    lookup[key] = MarketSnapshot(
                        exchange=exchange,
                        symbol=symbol or "",
                        exchange_symbol=snapshot.get("exchange_symbol") or "",
                        funding_rate=funding,
                        next_funding_time=(
                            datetime.fromisoformat(next_funding)
                            if isinstance(next_funding, str)
                            else None
                        ),
                        mark_price=mark_price,
                        bid=snapshot.get("bid"),
                        ask=snapshot.get("ask"),
                        raw={},
                        bid_size=snapshot.get("bid_size"),
                        ask_size=snapshot.get("ask_size"),
                        funding_interval_hours=snapshot.get("funding_interval_hours"),
                    )
        return lookup


    def _make_progress_callback(
        self, loop: asyncio.AbstractEventLoop
    ) -> Callable[[str, dict[str, Any] | None], None]:
        def _callback(event: str, payload: dict[str, Any] | None = None) -> None:
            data = dict(payload or {})
            loop.call_soon_threadsafe(self._record_event, event, data)
            if event.startswith("exchange:") and data:
                exchange = data.get("exchange")
                if exchange:
                    loop.call_soon_threadsafe(
                        self._update_exchange_status,
                        exchange,
                        event,
                        data,
                    )

        return _callback

    def _record_event(self, event: str, payload: dict[str, Any]) -> None:
        entry = {
            "event": event,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._events.append(entry)
        if len(self._events) > 200:
            del self._events[:-200]

    def _update_exchange_status(
        self, exchange: str, event: str, payload: dict[str, Any]
    ) -> None:
        status_map = {
            "exchange:success": "ok",
            "exchange:error": "failed",
            "exchange:missing": "missing",
            "exchange:start": "pending",
        }
        status = status_map.get(event, payload.get("status"))
        entry = {
            "exchange": exchange,
            "status": status or payload.get("status") or "unknown",
            "message": payload.get("message"),
            "count": payload.get("count"),
            "error": payload.get("error"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._exchange_status[exchange] = entry

    async def _handle_telemetry_event(self, entry: dict[str, Any]) -> None:
        self._telemetry_events.append(entry)
        if len(self._telemetry_events) > 200:
            self._telemetry_events = self._telemetry_events[-200:]

    def _target_stop_price(
        self,
        side: str | None,
        liq_price: float | None,
        *,
        mark_price: float | None = None,
        entry_price: float | None = None,
    ) -> float | None:
        """Compute protective stop from liquidation; fallback if liq is missing/zero."""
        base_liq = None
        if liq_price is not None and liq_price > 0:
            base_liq = liq_price
        else:
            fallback = mark_price or entry_price
            if fallback is None or fallback <= 0:
                return None
            # Heuristic: if liq missing, place far from current price to avoid zero/invalid triggers.
            base_liq = fallback * (
                getattr(self._risk_config, "fallback_liq_factor_long", 0.33)
                if side == "long"
                else getattr(self._risk_config, "fallback_liq_factor_short", 1.66)
            )
        try:
            gap = float(self._risk_config.stop_gap_from_liq_pct)
        except Exception:
            gap = 0.07
        if gap <= 0:
            return None
        if side == "short":
            return base_liq * max(0.0001, (1.0 - gap))
        return base_liq * (1.0 + gap)

    def _risk_config_from_settings(self) -> RiskConfig:
        settings = self._settings_manager.current
        protective = getattr(settings, "protective", {}) or {}
        cfg = default_risk_config()
        try:
            cfg.stop_gap_from_liq_pct = float(protective.get("stop_gap_from_liq_pct", cfg.stop_gap_from_liq_pct))
            cfg.stop_requote_threshold_pct = float(
                protective.get("stop_requote_threshold_pct", cfg.stop_requote_threshold_pct)
            )
            cfg.fallback_liq_factor_long = float(
                protective.get("fallback_liq_factor_long", cfg.fallback_liq_factor_long)
            )
            cfg.fallback_liq_factor_short = float(
                protective.get("fallback_liq_factor_short", cfg.fallback_liq_factor_short)
            )
            cfg.target_safe_buffer_pct = float(
                protective.get("target_safe_buffer_pct", cfg.target_safe_buffer_pct)
            )
            cfg.warning_buffer_pct = float(protective.get("warning_buffer_pct", cfg.warning_buffer_pct))
            cfg.panic_buffer_pct = float(protective.get("panic_buffer_pct", cfg.panic_buffer_pct))
            cfg.min_free_balance_abs = float(protective.get("min_free_balance_abs", cfg.min_free_balance_abs))
            cfg.min_free_balance_rel = float(protective.get("min_free_balance_rel", cfg.min_free_balance_rel))
            cfg.balance_check_interval_sec = int(
                protective.get("balance_check_interval_sec", cfg.balance_check_interval_sec)
            )
            cfg.position_check_interval_sec = int(
                protective.get("position_check_interval_sec", cfg.position_check_interval_sec)
            )
            cfg.panic_close_batch_size = int(
                protective.get("panic_close_batch_size", cfg.panic_close_batch_size)
            )
            cfg.telegram_alert_chat_id = str(protective.get("telegram_alert_chat_id", cfg.telegram_alert_chat_id))
            cfg.send_missing_stop_alerts = bool(
                protective.get("send_missing_stop_alerts", cfg.send_missing_stop_alerts)
            )
        except Exception:
            pass
        return cfg

    def _apply_alert_settings(self) -> None:
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        send_margin = bool(protective.get("send_margin_alerts", True))
        warning_buffer = _safe_float(protective.get("warning_buffer_pct"))
        panic_buffer = _safe_float(protective.get("panic_buffer_pct"))
        min_free_abs = _safe_float(protective.get("min_free_balance_abs"))
        if warning_buffer is None:
            warning_buffer = self._risk_config.warning_buffer_pct
        if panic_buffer is None:
            panic_buffer = self._risk_config.panic_buffer_pct
        if min_free_abs is None:
            min_free_abs = self._risk_config.min_free_balance_abs
        self._accounts.update_alert_settings(
            send_margin_alerts=send_margin,
            send_missing_stop_alerts=bool(protective.get("send_missing_stop_alerts", True)),
            warning_buffer_pct=warning_buffer,
            panic_buffer_pct=panic_buffer,
            min_free_balance_abs=min_free_abs,
        )
        self._send_missing_stop_alerts = bool(
            protective.get("send_missing_stop_alerts", self._send_missing_stop_alerts)
        )

    async def _maybe_sync_protective_orders(self) -> None:
        """Best-effort protective order sync if enabled in settings."""
        settings = self._settings_manager.current
        protective = getattr(settings, "protective", {}) or {}
        auto_protect = bool(protective.get("auto_protect_enabled", True))
        auto_take = bool(protective.get("auto_take_enabled", True))
        if not auto_protect and not auto_take:
            return
        snapshot = self._accounts.snapshot()
        positions = snapshot.get("positions") or []
        anti_orphan = bool(protective.get("anti_orphan_enabled", False))
        try:
            actions = await self._protective_manager.sync_protective_orders(
                positions,
                anti_orphan_enabled=anti_orphan,
            )
            if actions:
                if self._send_missing_stop_alerts:
                    await self._handle_mexc_protective_alerts(actions)
                summary = {
                    "message": "Protective orders synced",
                    "count": len(actions),
                    "updated": sum(1 for a in actions if a.get("status") == "updated"),
                    "unchanged": sum(1 for a in actions if a.get("status") == "unchanged"),
                    "timeout": sum(1 for a in actions if a.get("status") == "timeout"),
                    "error": sum(1 for a in actions if a.get("status") == "error"),
                }
                # Build a human-readable per-symbol summary.
                per_symbol: dict[str, list[str]] = {}
                for action in actions:
                    sym = str(action.get("symbol") or "").upper()
                    exch = str(action.get("exchange") or "")
                    status = action.get("status")
                    stop_val = action.get("target_stop")
                    take_val = action.get("target_take")
                    reason = action.get("reason") or action.get("error")
                    parts = [f"{exch}: {status}"]
                    if stop_val is not None:
                        parts.append(f"sl={stop_val}")
                    if take_val is not None:
                        parts.append(f"tp={take_val}")
                    if reason:
                        parts.append(f"reason={reason}")
                    per_symbol.setdefault(sym, []).append(", ".join(parts))
                summary["details"] = {k: v for k, v in per_symbol.items()}
                self._record_event("protective:sync", summary)
                # Emit compact overall status instead of per-leg spam.
                ok_states = {"updated", "unchanged", "blocked_ok"}
                failures = [a for a in actions if a.get("status") not in ok_states]
                if failures:
                    logger.warning(
                        "protective sync issues: %s",
                        "; ".join(
                            f"{f.get('exchange')} {f.get('symbol')} status={f.get('status')} err={f.get('error') or f.get('reason')}"
                            for f in failures
                        ),
                    )
                else:
                    logger.info("protective sync ok: all stops/takes placed")
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Protective sync failed: %s", exc)

    async def analyze_symbol(
        self,
        symbol: str,
        *,
        window_minutes: int = 720,
        funding_points: int = 24,
    ) -> dict[str, Any]:
        """Collect per-symbol analytics on demand without touching the global snapshot."""
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol must be provided for analysis.")

        window = max(30, min(int(window_minutes), 4320))  # clamp to 3 days of 1m bars.
        funding_limit = max(6, int(funding_points))

        exchange_flags = getattr(self._settings_manager.current, "analysis_exchanges", None) or {}
        enabled_exchanges = [
            normalize_exchange_name(name)
            for name, enabled in exchange_flags.items()
            if enabled
        ]

        tasks = [
            self._analyze_symbol_on_exchange(ex, canonical, window, funding_limit)
            for ex in enabled_exchanges
        ]
        results = await asyncio.gather(*tasks)

        return {
            "symbol": canonical,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "window_minutes": window,
            "funding_points": funding_limit,
            "exchanges": [item for item in results if item],
        }

    async def _analyze_symbol_on_exchange(
        self,
        exchange: str,
        canonical_symbol: str,
        window_minutes: int,
        funding_points: int,
    ) -> dict[str, Any] | None:
        result: dict[str, Any] = {
            "exchange": exchange,
            "symbol": canonical_symbol,
        }
        try:
            adapter = get_adapter(exchange)
        except KeyError:
            result["status"] = "error"
            result["error"] = f"Adapter for {exchange} not registered."
            return result

        try:
            exchange_symbol = adapter.map_symbol(canonical_symbol)
        except Exception:  # pylint: disable=broad-except
            exchange_symbol = None

        if not exchange_symbol:
            result["status"] = "unsupported"
            result["error"] = "Symbol not supported on this exchange."
            return result

        result["exchange_symbol"] = exchange_symbol
        errors: list[str] = []
        warnings: list[str] = []

        # Latest snapshot (bid/ask/mark/funding).
        try:
            snapshots = await adapter.fetch_market_snapshots_async([canonical_symbol])
            if snapshots:
                snap = snapshots[0]
                snapshot_dict = snap.to_dict()
                bid = snapshot_dict.get("bid")
                ask = snapshot_dict.get("ask")
                if bid is not None and ask is not None:
                    snapshot_dict["spread"] = (ask or 0.0) - (bid or 0.0)
                    snapshot_dict["mid"] = (ask + bid) / 2 if bid is not None else None
                result["snapshot"] = snapshot_dict
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"snapshot:{exc}")

        # Funding history (last N points).
        funding_history = await asyncio.to_thread(
            _load_funding_history_cached,
            exchange,
            exchange_symbol,
            canonical_symbol,
            funding_points,
            adapter,
        )
        if funding_history:
            funding_history = sorted(
                funding_history,
                key=lambda item: item.get("ts_ms") or item.get("timestamp") or 0,
                reverse=True,
            )
            funding_history = funding_history[:funding_points]
        result["funding_history"] = funding_history

        # Recent 1m candles for spread/time-sync analysis.
        try:
            candles = await asyncio.to_thread(
                self._fetch_candles_for_exchange,
                exchange,
                exchange_symbol,
                canonical_symbol,
                window_minutes,
            )
            if candles:
                result["candles_1m"] = candles
            else:
                warnings.append("candles_unavailable")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"candles:{exc}")

        status = "ok"
        if errors and warnings:
            status = "partial"
        elif errors:
            status = "error"
        elif warnings:
            status = "partial"
        result["status"] = status
        if errors:
            result["errors"] = errors
        if warnings:
            result["warnings"] = warnings
        return result

    def _fetch_candles_for_exchange(
        self,
        exchange: str,
        exchange_symbol: str,
        canonical_symbol: str,
        window_minutes: int,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(window_minutes, 4320))
        name = normalize_exchange_name(exchange)
        try:
            if name == "bybit":
                return _fetch_bybit_candles(exchange_symbol, limit)
            if name == "mexc":
                return _fetch_mexc_candles(exchange_symbol, limit)
        except URLError as exc:
            logger.debug("Candle fetch network error for %s %s: %s", exchange, exchange_symbol, exc)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Candle fetch failed for %s %s: %s", exchange, exchange_symbol, exc)
        try:
            return _fetch_candles_ccxt(name, canonical_symbol, limit)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("CCXT candle fallback failed for %s %s: %s", exchange, canonical_symbol, exc)
            return []

    async def refresh_snapshot(self, *, force_accounts: bool = False) -> RefreshResult:
        """Compatibility wrapper used by the HTTP API."""
        if force_accounts:
            await self._accounts.refresh_now(force_env=True)
        return await self.refresh_markets(force_sources=True)

    async def _handle_mexc_protective_alerts(self, actions: list[dict[str, Any]]) -> None:
        """Send reminder alerts for MEXC legs where stops cannot be auto-placed."""
        now = time.time()
        for action in actions or []:
            if str(action.get("exchange") or "").lower() != "mexc":
                continue
            status = str(action.get("status") or "")
            if status not in ("blocked_missing_stop", "blocked_bad_stop"):
                continue
            target_stop = action.get("target_stop")
            if target_stop is None:
                continue
            symbol = str(action.get("symbol") or "").upper()
            qty = action.get("quantity") or 0.0
            existing = action.get("existing") or {}
            key = ("mexc", symbol)
            last = self._last_mexc_alert.get(key, 0.0)
            if (now - last) < self._mexc_alert_cooldown:
                continue
            if status == "blocked_missing_stop":
                text = f"Позиция {symbol} {qty:g} монет стоп не стоит! поставьте стоп {target_stop}"
            else:
                text = (
                    f"Позиция {symbol} {qty:g} монет неправильный стоп {existing.get('stop')}, "
                    f"нужно поставить {target_stop}"
                )
            text = f"MEXC: {text}"
            sent = await self._accounts.send_telegram_message(text)
            if sent:
                self._last_mexc_alert[key] = now

def _fmt_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
