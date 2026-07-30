from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol
from uuid import uuid4

from config import BASE_DIR

try:
    import ccxt  # type: ignore
except ImportError:  # pragma: no cover - optional runtime dependency
    ccxt = None

logger = logging.getLogger(__name__)

PUMP_LIVE_ENV_PATH = BASE_DIR / "config" / "pump_live.env"
PUMP_LIVE_STATE_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_live"
PUMP_LIVE_STATE_FILE = "live_state.json"
PUMP_LIVE_EVENTS_FILE = "live_events.jsonl"
PUMP_ORDER_LINK_PREFIX = "FAP"
ARM_CONFIRMATION = "ARM PUMP LIVE 1000"
PREPARE_CONFIRMATION = "PREPARE PUMP SUBACCOUNT"
EMERGENCY_CONFIRMATION = "CLOSE ALL PUMP POSITIONS"
TRANSIENT_RECOVERY_CYCLES = 2
STATE_REPLACE_RETRY_DELAYS_SEC = (0.0, 0.05, 0.1, 0.2, 0.4, 0.8)


@dataclass(frozen=True, slots=True)
class PumpLiveConfig:
    total_capital_usd: float = 1_000.0
    deployable_capital_usd: float = 700.0
    reserve_usd: float = 300.0
    max_active_positions: int = 4
    entry_cap: int = 1
    leverage: float = 3.0
    poll_interval_sec: int = 15
    max_slippage_bps: float = 50.0
    warning_liq_buffer_pct: float = 20.0
    panic_liq_buffer_pct: float = 15.0
    emergency_liq_buffer_pct: float = 10.0
    exchange_stop_gap_from_liq_pct: float = 2.5
    margin_topup_chunk_usd: float = 25.0
    max_position_topup_usd: float = 175.0
    max_total_topup_usd: float = 275.0
    guaranteed_position_topup_usd: float = 50.0
    entry_margin_prefund_enabled: bool = True
    entry_margin_prefund_safety_pct: float = 2.5
    entry_margin_prefund_round_usd: float = 5.0
    entry_margin_prefund_mmr: float = 0.025
    entry_margin_prefund_taker_fee_rate: float = 0.00055
    operating_cash_floor_usd: float = 25.0
    flat_confirm_cycles: int = 2
    topup_cooldown_sec: int = 300
    margin_reduce_trigger_buffer_pct: float = 35.0
    margin_reduce_target_buffer_pct: float = 30.0
    margin_reduce_confirm_cycles: int = 2
    margin_reduce_cooldown_sec: int = 1_800
    preflight_max_age_sec: int = 300

    @property
    def slot_margin_usd(self) -> float:
        return self.deployable_capital_usd / self.max_active_positions


class PumpGateway(Protocol):
    def credentials_status(self) -> dict[str, Any]: ...

    def preflight(self, config: PumpLiveConfig) -> dict[str, Any]: ...

    def prepare_account(self) -> dict[str, Any]: ...

    def fetch_balance(self) -> dict[str, Any]: ...

    def fetch_positions(self) -> list[dict[str, Any]]: ...

    def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]: ...

    def fetch_order(self, order_id: str, symbol: str) -> dict[str, Any]: ...

    def fetch_ticker(self, symbol: str) -> dict[str, Any]: ...

    def set_leverage(self, symbol: str, leverage: float) -> None: ...

    def guarded_market_order(
        self,
        *,
        symbol: str,
        side: str,
        notional_usd: float | None,
        qty: float | None,
        reduce_only: bool,
        order_link_id: str,
        max_slippage_bps: float,
    ) -> dict[str, Any]: ...

    def create_ladder_order(
        self,
        *,
        symbol: str,
        notional_usd: float,
        price: float,
        order_link_id: str,
    ) -> dict[str, Any]: ...

    def cancel_order(self, order_id: str, symbol: str) -> None: ...

    def set_full_protection(
        self,
        symbol: str,
        *,
        take_profit_price: float,
        stop_loss_price: float,
    ) -> dict[str, Any]: ...

    def add_margin(self, symbol: str, amount_usd: float) -> dict[str, Any]: ...

    def remove_margin(self, symbol: str, amount_usd: float) -> dict[str, Any]: ...


class PumpLiveNotifier(Protocol):
    async def send_text_status(self, text: str, *, title: str | None = None) -> str: ...


def read_pump_live_env(path: Path = PUMP_LIVE_ENV_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = value.strip().strip('"').strip("'")
    return values


def load_pump_live_config(path: Path = PUMP_LIVE_ENV_PATH) -> PumpLiveConfig:
    values = read_pump_live_env(path)
    entry_cap = _safe_int(values.get("PUMP_LIVE_ENTRY_CAP"), 1)
    poll_interval = _safe_int(values.get("PUMP_LIVE_POLL_INTERVAL_SEC"), 15)
    max_slippage = _safe_float(values.get("PUMP_LIVE_MAX_SLIPPAGE_BPS"), 50.0)
    prefund_enabled = str(
        values.get("PUMP_LIVE_MARGIN_PREFUND_ENABLED", "1")
    ).strip().lower() not in {"0", "false", "no", "off"}
    prefund_safety = _safe_float(
        values.get("PUMP_LIVE_MARGIN_PREFUND_SAFETY_PCT"),
        2.5,
    )
    return PumpLiveConfig(
        entry_cap=max(1, min(4, entry_cap)),
        poll_interval_sec=max(5, min(60, poll_interval)),
        max_slippage_bps=max(1.0, min(200.0, max_slippage)),
        entry_margin_prefund_enabled=prefund_enabled,
        entry_margin_prefund_safety_pct=max(
            0.0,
            min(10.0, prefund_safety),
        ),
    )


class BybitPumpLiveGateway:
    def __init__(self, *, env_path: Path = PUMP_LIVE_ENV_PATH) -> None:
        self.env_path = env_path
        self._client: Any = None
        self._signature: tuple[str, str, bool] | None = None
        self._configured_uid = ""
        self._lock = threading.RLock()

    def _credentials(self) -> tuple[str, str, bool]:
        values = read_pump_live_env(self.env_path)
        key = values.get("BYBIT_PUMP_API_KEY", "").strip()
        secret = values.get("BYBIT_PUMP_API_SECRET", "").strip()
        self._configured_uid = values.get("BYBIT_PUMP_SUB_UID", "").strip()
        testnet = values.get("BYBIT_PUMP_TESTNET", "0").strip().lower() in {"1", "true", "yes", "on"}
        return key, secret, testnet

    def credentials_status(self) -> dict[str, Any]:
        key, secret, testnet = self._credentials()
        return {
            "env_file": str(self.env_path),
            "env_file_exists": self.env_path.exists(),
            "api_key_present": bool(key),
            "api_secret_present": bool(secret),
            "sub_uid_present": bool(self._configured_uid),
            "testnet": testnet,
            "ready": bool(key and secret and self._configured_uid),
        }

    def _ensure_client(self) -> Any:
        if ccxt is None:
            raise RuntimeError("ccxt_not_installed")
        key, secret, testnet = self._credentials()
        if not key or not secret:
            raise RuntimeError("pump_live_credentials_missing")
        signature = (key, secret, testnet)
        if self._client is not None and self._signature == signature:
            return self._client
        client = ccxt.bybit(
            {
                "apiKey": key,
                "secret": secret,
                "enableRateLimit": True,
                "options": {
                    "defaultType": "swap",
                    "defaultSettle": "USDT",
                    "adjustForTimeDifference": True,
                },
            }
        )
        if testnet:
            client.set_sandbox_mode(True)
        client.load_time_difference()
        client.load_markets()
        self._client = client
        self._signature = signature
        return client

    def _market(self, symbol: str) -> dict[str, Any]:
        client = self._ensure_client()
        normalized = _normalize_symbol(symbol)
        for market in client.markets.values():
            if (
                str(market.get("id") or "").upper() == normalized
                and market.get("swap")
                and str(market.get("settle") or "").upper() == "USDT"
            ):
                return market
        raise RuntimeError(f"pump_live_market_not_found:{normalized}")

    def _ccxt_symbol(self, symbol: str) -> str:
        return str(self._market(symbol).get("symbol") or symbol)

    def preflight(self, config: PumpLiveConfig) -> dict[str, Any]:
        with self._lock:
            checked_at_ms = _now_ms()
            credentials = self.credentials_status()
            if not credentials["ready"]:
                return {
                    "ready": False,
                    "checked_at_ms": checked_at_ms,
                    "credentials": credentials,
                    "errors": ["pump_live_credentials_missing"],
                    "warnings": [],
                }
            errors: list[str] = []
            warnings: list[str] = []
            try:
                client = self._ensure_client()
                key_payload = client.private_get_v5_user_query_api({})
                key_info = dict((key_payload or {}).get("result") or {})
                permissions = dict(key_info.get("permissions") or {})
                contract_permissions = {str(item) for item in permissions.get("ContractTrade") or []}
                is_master = bool(key_info.get("isMaster"))
                read_only = _safe_int(key_info.get("readOnly"), 1)
                user_id = str(key_info.get("userID") or key_info.get("userIDInt64") or "")
                parent_uid = str(key_info.get("parentUid") or "")
                ips = [str(item) for item in key_info.get("ips") or []]
                uta = key_info.get("uta")
                deadline_day = _optional_int(key_info.get("deadlineDay"))
                expired_at = str(key_info.get("expiredAt") or "").strip() or None
                if is_master:
                    errors.append("pump_live_key_is_master_not_subaccount")
                if read_only != 0:
                    errors.append("pump_live_key_is_read_only")
                if not {"Order", "Position"}.issubset(contract_permissions):
                    errors.append("pump_live_contract_permissions_missing")
                if uta is not None and _safe_int(uta, 0) != 1:
                    errors.append("pump_live_subaccount_is_not_unified_trading")
                if self._configured_uid and user_id and user_id != self._configured_uid:
                    errors.append("pump_live_sub_uid_mismatch")
                if not ips or "*" in ips:
                    warnings.append("api_key_has_no_ip_binding_dynamic_ip_mode")
                    if deadline_day is not None:
                        warnings.append(f"api_key_unbound_deadline_day:{deadline_day}")

                balance = self.fetch_balance()
                positions = self.fetch_positions()
                open_orders = self.fetch_open_orders()
                account_payload = client.private_get_v5_account_info({})
                account_info = dict((account_payload or {}).get("result") or {})
                margin_mode = str(account_info.get("marginMode") or "")
                if margin_mode != "ISOLATED_MARGIN":
                    errors.append("pump_live_account_margin_mode_not_isolated")
                total = _safe_float(balance.get("total"), 0.0)
                available = _safe_float(balance.get("available"), 0.0)
                if total < config.total_capital_usd * 0.95:
                    errors.append("pump_live_equity_below_950_usdt")
                if available < config.reserve_usd:
                    errors.append("pump_live_available_below_reserve")
                unknown_positions = [
                    item for item in positions if _safe_float(item.get("qty"), 0.0) > 0
                ]
                unknown_orders = [
                    item
                    for item in open_orders
                    if not bool(item.get("reduce_only"))
                ]
                if unknown_positions:
                    errors.append("pump_live_subaccount_has_existing_positions")
                if unknown_orders:
                    errors.append("pump_live_subaccount_has_unknown_open_orders")
                return {
                    "ready": not errors,
                    "checked_at_ms": checked_at_ms,
                    "credentials": credentials,
                    "key": {
                        "is_master": is_master,
                        "read_only": read_only,
                        "user_id_matches_config": not self._configured_uid or not user_id or user_id == self._configured_uid,
                        "parent_uid_present": bool(parent_uid and parent_uid != "0"),
                        "contract_permissions": sorted(contract_permissions),
                        "ip_bound": bool(ips and "*" not in ips),
                        "uta": _optional_int(uta),
                        "deadline_day": deadline_day,
                        "expired_at": expired_at,
                    },
                    "account": {
                        "margin_mode": margin_mode,
                        "total_usdt": round(total, 6),
                        "available_usdt": round(available, 6),
                        "positions": len(positions),
                        "open_orders": len(open_orders),
                    },
                    "errors": errors,
                    "warnings": warnings,
                }
            except Exception as exc:  # pylint: disable=broad-except
                return {
                    "ready": False,
                    "checked_at_ms": checked_at_ms,
                    "credentials": credentials,
                    "errors": [f"pump_live_preflight_failed:{_clean_error(exc)}"],
                    "warnings": warnings,
                }

    def prepare_account(self) -> dict[str, Any]:
        with self._lock:
            client = self._ensure_client()
            positions = self.fetch_positions()
            open_orders = self.fetch_open_orders()
            if positions or open_orders:
                raise RuntimeError("pump_live_prepare_requires_flat_account_without_orders")
            try:
                margin_result = client.private_post_v5_account_set_margin_mode(
                    {"setMarginMode": "ISOLATED_MARGIN"}
                )
            except Exception as exc:
                message = str(exc).lower()
                if "not modified" not in message and "110026" not in message:
                    raise
                margin_result = {"status": "already_isolated"}
            position_mode_result: Any = None
            try:
                position_mode_result = client.set_position_mode(False, None, {"category": "linear", "coin": "USDT"})
            except Exception as exc:  # Bybit returns a harmless not-modified error when already one-way.
                message = str(exc).lower()
                if "not modified" not in message and "same position mode" not in message and "110025" not in message:
                    raise
                position_mode_result = {"status": "already_one_way"}
            return {
                "status": "prepared",
                "margin_mode_result": _compact_exchange_result(margin_result),
                "position_mode_result": _compact_exchange_result(position_mode_result),
            }

    def fetch_balance(self) -> dict[str, Any]:
        with self._lock:
            client = self._ensure_client()
            payload = client.fetch_balance({"type": "swap", "accountType": "UNIFIED"})
            asset = dict(payload.get("USDT") or {})
            total = _optional_float(asset.get("total"))
            available = _optional_float(asset.get("free"))
            info = dict(payload.get("info") or {})
            try:
                account = dict(((info.get("result") or {}).get("list") or [{}])[0])
            except (AttributeError, IndexError, TypeError):
                account = {}
            total = _optional_float(account.get("totalEquity")) if total is None else total
            available = (
                _optional_float(account.get("totalAvailableBalance"))
                if _optional_float(account.get("totalAvailableBalance")) is not None
                else available
            )
            return {
                "total": total or 0.0,
                "available": available or 0.0,
                "used": max(0.0, (total or 0.0) - (available or 0.0)),
            }

    def fetch_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            client = self._ensure_client()
            rows = client.fetch_positions(None, {"category": "linear", "settleCoin": "USDT"})
            result: list[dict[str, Any]] = []
            for row in rows or []:
                qty = abs(_safe_float(row.get("contracts"), 0.0))
                if qty <= 1e-12:
                    continue
                info = dict(row.get("info") or {})
                result.append(
                    {
                        "symbol": _normalize_symbol(row.get("symbol") or info.get("symbol")),
                        "side": str(row.get("side") or "").lower(),
                        "qty": qty,
                        "avg_price": _safe_float(row.get("entryPrice") or info.get("avgPrice"), 0.0),
                        "mark_price": _safe_float(row.get("markPrice") or info.get("markPrice"), 0.0),
                        "liq_price": _optional_float(row.get("liquidationPrice") or info.get("liqPrice")),
                        "leverage": _safe_float(row.get("leverage") or info.get("leverage"), 0.0),
                        "margin_mode": str(row.get("marginMode") or info.get("tradeMode") or ""),
                        "position_idx": _safe_int(info.get("positionIdx"), 0),
                        "unrealized_pnl": _safe_float(row.get("unrealizedPnl"), 0.0),
                    }
                )
            return result

    def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            client = self._ensure_client()
            params: dict[str, Any] = {"category": "linear", "settleCoin": "USDT", "openOnly": 0}
            if symbol:
                params["symbol"] = self._market(symbol).get("id")
            payload = client.private_get_v5_order_realtime(params)
            rows = ((payload or {}).get("result") or {}).get("list") or []
            return [_normalize_order(row) for row in rows]

    def fetch_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        with self._lock:
            client = self._ensure_client()
            payload = client.private_get_v5_order_realtime(
                {
                    "category": "linear",
                    "symbol": self._market(symbol).get("id"),
                    "orderId": order_id,
                    "openOnly": 1,
                }
            )
            rows = ((payload or {}).get("result") or {}).get("list") or []
            return _normalize_order(rows[0]) if rows else {"id": order_id, "status": "unknown"}

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        with self._lock:
            ticker = self._ensure_client().fetch_ticker(self._ccxt_symbol(symbol))
            return {
                "last": _safe_float(ticker.get("last"), 0.0),
                "bid": _safe_float(ticker.get("bid"), 0.0),
                "ask": _safe_float(ticker.get("ask"), 0.0),
            }

    def set_leverage(self, symbol: str, leverage: float) -> None:
        with self._lock:
            client = self._ensure_client()
            ccxt_symbol = self._ccxt_symbol(symbol)
            try:
                client.set_leverage(
                    leverage,
                    ccxt_symbol,
                    {"category": "linear"},
                )
            except Exception as exc:
                message = str(exc).lower()
                if "not modified" not in message and "110043" not in message:
                    raise

    def guarded_market_order(
        self,
        *,
        symbol: str,
        side: str,
        notional_usd: float | None,
        qty: float | None,
        reduce_only: bool,
        order_link_id: str,
        max_slippage_bps: float,
    ) -> dict[str, Any]:
        with self._lock:
            client = self._ensure_client()
            ccxt_symbol = self._ccxt_symbol(symbol)
            book = client.fetch_order_book(ccxt_symbol, 50)
            levels = book.get("asks") if side == "buy" else book.get("bids")
            levels = list(levels or [])
            if not levels:
                raise RuntimeError("pump_live_orderbook_empty")
            best = _safe_float(levels[0][0], 0.0)
            if qty is None:
                if notional_usd is None or notional_usd <= 0 or best <= 0:
                    raise RuntimeError("pump_live_invalid_order_size")
                qty = notional_usd / best
            amount = float(client.amount_to_precision(ccxt_symbol, qty))
            if amount <= 0:
                raise RuntimeError("pump_live_amount_rounds_to_zero")
            remaining = amount
            quote = 0.0
            for price, level_qty, *_ in levels:
                take = min(remaining, _safe_float(level_qty, 0.0))
                quote += take * _safe_float(price, 0.0)
                remaining -= take
                if remaining <= 1e-12:
                    break
            if remaining > 1e-8:
                raise RuntimeError("pump_live_insufficient_orderbook_depth")
            vwap = quote / amount
            slippage_bps = (
                (vwap / best - 1.0) * 10_000.0
                if side == "buy"
                else (1.0 - vwap / best) * 10_000.0
            )
            if slippage_bps > max_slippage_bps:
                raise RuntimeError(
                    f"pump_live_slippage_guard:{round(slippage_bps, 3)}>{round(max_slippage_bps, 3)}"
                )
            params = {
                "category": "linear",
                "positionIdx": 0,
                "reduceOnly": bool(reduce_only),
                "orderLinkId": order_link_id[:36],
            }
            order = client.create_order(ccxt_symbol, "market", side, amount, None, params)
            normalized = _normalize_ccxt_order(order)
            normalized["estimated_slippage_bps"] = round(slippage_bps, 6)
            normalized["requested_qty"] = amount
            return normalized

    def create_ladder_order(
        self,
        *,
        symbol: str,
        notional_usd: float,
        price: float,
        order_link_id: str,
    ) -> dict[str, Any]:
        with self._lock:
            client = self._ensure_client()
            ccxt_symbol = self._ccxt_symbol(symbol)
            order_price = float(client.price_to_precision(ccxt_symbol, price))
            qty = float(client.amount_to_precision(ccxt_symbol, notional_usd / order_price))
            if qty <= 0:
                raise RuntimeError("pump_live_ladder_amount_rounds_to_zero")
            order = client.create_order(
                ccxt_symbol,
                "limit",
                "sell",
                qty,
                order_price,
                {
                    "category": "linear",
                    "positionIdx": 0,
                    "reduceOnly": False,
                    "postOnly": True,
                    "timeInForce": "PostOnly",
                    "orderLinkId": order_link_id[:36],
                },
            )
            return _normalize_ccxt_order(order)

    def cancel_order(self, order_id: str, symbol: str) -> None:
        with self._lock:
            client = self._ensure_client()
            try:
                client.cancel_order(
                    order_id,
                    self._ccxt_symbol(symbol),
                    {"category": "linear", "orderId": order_id},
                )
            except Exception as exc:
                message = str(exc).lower()
                if "order not exists" not in message and "110001" not in message:
                    raise

    def set_full_protection(
        self,
        symbol: str,
        *,
        take_profit_price: float,
        stop_loss_price: float,
    ) -> dict[str, Any]:
        with self._lock:
            client = self._ensure_client()
            market = self._market(symbol)
            ccxt_symbol = str(market.get("symbol"))
            take_trigger = client.price_to_precision(ccxt_symbol, take_profit_price)
            stop_trigger = client.price_to_precision(ccxt_symbol, stop_loss_price)
            try:
                payload = client.private_post_v5_position_trading_stop(
                    {
                        "category": "linear",
                        "symbol": market.get("id"),
                        "takeProfit": str(take_trigger),
                        "stopLoss": str(stop_trigger),
                        "tpTriggerBy": "MarkPrice",
                        "slTriggerBy": "MarkPrice",
                        "tpslMode": "Full",
                        "tpOrderType": "Market",
                        "slOrderType": "Market",
                        "positionIdx": 0,
                    }
                )
            except Exception as exc:
                message = str(exc).lower()
                if "not modified" not in message and "34040" not in message:
                    raise
                return {"status": "already_set"}
            return _compact_exchange_result(payload)

    def add_margin(self, symbol: str, amount_usd: float) -> dict[str, Any]:
        with self._lock:
            market = self._market(symbol)
            payload = self._ensure_client().private_post_v5_position_add_margin(
                {
                    "category": "linear",
                    "symbol": market.get("id"),
                    "margin": str(round(amount_usd, 4)),
                    "positionIdx": 0,
                }
            )
            return _compact_exchange_result(payload)

    def remove_margin(self, symbol: str, amount_usd: float) -> dict[str, Any]:
        with self._lock:
            market = self._market(symbol)
            payload = self._ensure_client().private_post_v5_position_add_margin(
                {
                    "category": "linear",
                    "symbol": market.get("id"),
                    "margin": str(round(-abs(amount_usd), 4)),
                    "positionIdx": 0,
                }
            )
            return _compact_exchange_result(payload)


class PumpLiveController:
    def __init__(
        self,
        *,
        gateway: PumpGateway | None = None,
        state_dir: Path = PUMP_LIVE_STATE_DIR,
        env_path: Path = PUMP_LIVE_ENV_PATH,
        start_recovery_monitor: bool = True,
        background_monitor: bool = True,
        notifier: PumpLiveNotifier | None = None,
        background_notifications: bool = True,
    ) -> None:
        self.env_path = env_path
        self.state_dir = state_dir
        self.state_path = state_dir / PUMP_LIVE_STATE_FILE
        self.events_path = state_dir / PUMP_LIVE_EVENTS_FILE
        self.gateway = gateway or BybitPumpLiveGateway(env_path=env_path)
        self.notifier = notifier
        self._background_monitor = background_monitor
        self._background_notifications = background_notifications
        self._lock = threading.RLock()
        self._event_lock = threading.Lock()
        self._notification_lock = threading.Lock()
        self._notification_last_sent: dict[str, int] = {}
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = self._load_state()
        self._state["entry_armed"] = False
        self._state["transient_recovery_pending"] = False
        self._state["healthy_recovery_cycles"] = 0
        if self._open_positions(self._state):
            self._state["monitor_enabled"] = True
            self._state["status"] = "recovery_monitoring"
            self._save_state_locked()
            if start_recovery_monitor:
                self.start_monitor()
        elif self._state.get("status") not in {"disabled", "stopped"}:
            self._state["monitor_enabled"] = False
            self._state["status"] = "disarmed_after_restart"
            self._state["blocked_reason"] = "backend_restart"
            self._save_state_locked()

    def config(self) -> PumpLiveConfig:
        return load_pump_live_config(self.env_path)

    def status(self) -> dict[str, Any]:
        with self._lock:
            payload = json.loads(json.dumps(self._state, ensure_ascii=True))
        config = self.config()
        payload["config"] = asdict(config)
        payload["config"]["slot_margin_usd"] = round(config.slot_margin_usd, 6)
        payload["credentials"] = self.gateway.credentials_status()
        payload["monitor_thread_alive"] = bool(self._thread and self._thread.is_alive())
        payload["state_file"] = str(self.state_path)
        payload["events_file"] = str(self.events_path)
        payload["open_positions"] = len(self._open_positions(payload))
        payload["notifications"] = {
            "configured": self.notifier is not None,
            "last_event": payload.get("last_notification_event"),
            "last_status": payload.get("last_notification_status"),
            "last_at_ms": payload.get("last_notification_at_ms"),
            "last_error": payload.get("last_notification_error"),
        }
        payload["recent_events"] = _read_latest_jsonl(self.events_path, limit=30)
        return payload

    def preflight(self) -> dict[str, Any]:
        result = self.gateway.preflight(self.config())
        with self._lock:
            self._state["last_preflight"] = result
            self._state["updated_at_ms"] = _now_ms()
            self._save_state_locked()
        self._event("preflight", {"ready": result.get("ready"), "errors": result.get("errors") or []})
        return result

    def prepare_account(self, confirmation: str) -> dict[str, Any]:
        if confirmation != PREPARE_CONFIRMATION:
            raise ValueError("pump_live_prepare_confirmation_invalid")
        preflight = self.gateway.preflight(self.config())
        non_mode_errors = [
            item
            for item in preflight.get("errors") or []
            if item != "pump_live_account_margin_mode_not_isolated"
        ]
        if non_mode_errors:
            raise RuntimeError("pump_live_prepare_preflight_blocked:" + ",".join(non_mode_errors))
        result = self.gateway.prepare_account()
        verified = self.preflight()
        self._event("account_prepared", {"ready": verified.get("ready")})
        return {"prepare": result, "preflight": verified}

    def arm(self, confirmation: str) -> dict[str, Any]:
        if confirmation != ARM_CONFIRMATION:
            raise ValueError("pump_live_arm_confirmation_invalid")
        preflight = self.preflight()
        with self._lock:
            open_items = list(self._open_positions(self._state))
        if open_items:
            self._resume_tracked_positions(preflight, open_items)
            event = "armed_resumed"
        else:
            if not preflight.get("ready"):
                raise RuntimeError("pump_live_arm_preflight_not_ready")
            event = "armed"
        with self._lock:
            now = _now_ms()
            self._state.update(
                {
                    "status": "armed",
                    "monitor_enabled": True,
                    "entry_armed": True,
                    "armed_at_ms": now,
                    "updated_at_ms": now,
                    "blocked_reason": None,
                    "pending_signals": [],
                    "transient_recovery_pending": False,
                    "healthy_recovery_cycles": 0,
                }
            )
            self._save_state_locked()
        self._event(event, {"entry_cap": self.config().entry_cap, "positions": len(open_items)})
        self.start_monitor()
        return self.status()

    def _resume_tracked_positions(
        self,
        preflight: Mapping[str, Any],
        open_items: list[dict[str, Any]],
    ) -> None:
        tolerated = {
            "pump_live_subaccount_has_existing_positions",
            "pump_live_subaccount_has_unknown_open_orders",
        }
        remaining_errors = [
            str(item)
            for item in preflight.get("errors") or []
            if str(item) not in tolerated
        ]
        if remaining_errors:
            raise RuntimeError(
                "pump_live_resume_preflight_not_ready:" + ",".join(remaining_errors)
            )
        exchange_positions = self.gateway.fetch_positions()
        open_orders = self.gateway.fetch_open_orders()
        unknown_positions = self._unknown_exchange_positions(exchange_positions)
        unknown_orders = self._unknown_open_orders(open_orders)
        tracked_symbols = {
            _normalize_symbol(item.get("symbol"))
            for item in open_items
            if item.get("status") == "open"
        }
        exchange_symbols = {
            _normalize_symbol(item.get("symbol"))
            for item in exchange_positions
            if item.get("side") == "short" and _safe_float(item.get("qty"), 0.0) > 0
        }
        missing_symbols = sorted(tracked_symbols - exchange_symbols)
        degraded = [
            _normalize_symbol(item.get("symbol"))
            for item in open_items
            if item.get("status") != "open"
        ]
        if unknown_positions or unknown_orders or missing_symbols or degraded:
            raise RuntimeError(
                "pump_live_resume_unknown_exchange_state:"
                f"unknown_positions={len(unknown_positions)},"
                f"unknown_orders={len(unknown_orders)},"
                f"missing={','.join(missing_symbols)},"
                f"degraded={','.join(degraded)}"
            )
        exchange_by_symbol = {
            _normalize_symbol(item.get("symbol")): item
            for item in exchange_positions
            if item.get("side") == "short"
        }
        config = self.config()
        for item in open_items:
            self._apply_exchange_position(
                item,
                exchange_by_symbol[_normalize_symbol(item.get("symbol"))],
            )
            self._sync_full_protection(item, config, force=True)
        resume_preflight = dict(preflight)
        resume_preflight.update(
            {
                "ready": True,
                "errors": [],
                "resume_mode": "tracked_positions_verified",
                "raw_ready": bool(preflight.get("ready")),
                "resume_tolerated_errors": [
                    str(item)
                    for item in preflight.get("errors") or []
                    if str(item) in tolerated
                ],
            }
        )
        with self._lock:
            self._state["last_preflight"] = resume_preflight
            self._state["updated_at_ms"] = _now_ms()
            self._save_state_locked()

    def disarm(self, reason: str = "operator_disarm") -> dict[str, Any]:
        with self._lock:
            has_open_positions = bool(self._open_positions(self._state))
            self._state["entry_armed"] = False
            self._state["monitor_enabled"] = has_open_positions
            self._state["status"] = "monitoring" if has_open_positions else "disarmed"
            self._state["blocked_reason"] = reason
            self._state["transient_recovery_pending"] = False
            self._state["healthy_recovery_cycles"] = 0
            self._state["updated_at_ms"] = _now_ms()
            self._state["pending_signals"] = []
            self._save_state_locked()
        self._event("disarmed", {"reason": reason})
        self._wake.set()
        return self.status()

    def stop_monitor(self) -> dict[str, Any]:
        with self._lock:
            if self._open_positions(self._state):
                raise RuntimeError("pump_live_monitor_required_while_positions_open")
            self._state["entry_armed"] = False
            self._state["monitor_enabled"] = False
            self._state["status"] = "stopped"
            self._state["transient_recovery_pending"] = False
            self._state["healthy_recovery_cycles"] = 0
            self._state["updated_at_ms"] = _now_ms()
            self._save_state_locked()
        self._stop.set()
        self._wake.set()
        return self.status()

    def submit_decisions(self, decisions: list[dict[str, Any]]) -> dict[str, Any]:
        with self._lock:
            now = _now_ms()
            self._state["last_signal_batch_at_ms"] = now
            self._state["last_signal_count"] = len(decisions)
            if not self._state.get("entry_armed"):
                self._state["last_entry_ready"] = [
                    _compact_decision(item)
                    for item in decisions
                    if item.get("state") == "entry_ready"
                ][:20]
                self._save_state_locked()
                return {"accepted": 0, "armed": False}
            armed_at = _safe_int(self._state.get("armed_at_ms"), 0)
            seen = {str(item) for item in self._state.get("seen_events") or []}
            pending = list(self._state.get("pending_signals") or [])
            pending_keys = {_decision_key(item) for item in pending}
            accepted = 0
            for decision in decisions:
                if decision.get("strategy_id") != "main_pullback_tier":
                    continue
                if decision.get("state") != "entry_ready":
                    continue
                if _safe_int(decision.get("ts_ms"), 0) < armed_at:
                    continue
                key = _decision_key(decision)
                if not key or key in seen or key in pending_keys:
                    continue
                pending.append(_compact_decision(decision))
                pending_keys.add(key)
                accepted += 1
            self._state["pending_signals"] = pending[-100:]
            self._state["updated_at_ms"] = now
            self._save_state_locked()
        if accepted:
            self._event("signals_queued", {"accepted": accepted})
            self._wake.set()
        return {"accepted": accepted, "armed": True}

    def emergency_close_all(self, confirmation: str) -> dict[str, Any]:
        if confirmation != EMERGENCY_CONFIRMATION:
            raise ValueError("pump_live_emergency_confirmation_invalid")
        with self._lock:
            self._state["entry_armed"] = False
            self._state["monitor_enabled"] = True
            self._state["emergency_close_requested"] = True
            self._state["status"] = "emergency_closing"
            self._state["transient_recovery_pending"] = False
            self._state["healthy_recovery_cycles"] = 0
            self._state["updated_at_ms"] = _now_ms()
            self._save_state_locked()
        self._event("emergency_close_requested", {})
        self.start_monitor()
        self._wake.set()
        return self.status()

    def start_monitor(self) -> None:
        if not self._background_monitor:
            return
        with self._lock:
            if self._thread and self._thread.is_alive():
                self._wake.set()
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._monitor_loop,
                name="bybit-pump-live-monitor",
                daemon=True,
            )
            self._thread.start()

    def run_cycle(self) -> dict[str, Any]:
        config = self.config()
        try:
            balance = self.gateway.fetch_balance()
            exchange_positions = self.gateway.fetch_positions()
            open_orders = self.gateway.fetch_open_orders()
            with self._lock:
                self._state["last_balance"] = balance
                self._state["last_exchange_positions"] = exchange_positions
                self._state["last_open_orders"] = open_orders
                self._state["last_cycle_at_ms"] = _now_ms()
            self._reconcile(exchange_positions, open_orders, balance, config)
            if self._emergency_requested():
                self._execute_emergency_close(exchange_positions, open_orders, config)
            else:
                self._process_pending_signals(balance, exchange_positions, open_orders, config)
                self._maintain_positions(config)
            recovered = False
            with self._lock:
                recovery_pending = bool(
                    self._state.get("transient_recovery_pending")
                    and self._state.get("blocked_reason") == "monitor_cycle_transient_error"
                )
                if recovery_pending:
                    healthy = _safe_int(self._state.get("healthy_recovery_cycles"), 0) + 1
                    self._state["healthy_recovery_cycles"] = healthy
                    if healthy >= TRANSIENT_RECOVERY_CYCLES:
                        self._state["entry_armed"] = True
                        self._state["transient_recovery_pending"] = False
                        self._state["healthy_recovery_cycles"] = 0
                        self._state["blocked_reason"] = None
                        recovered = True
                if self._state.get("monitor_enabled") and not recovery_pending:
                    self._state["status"] = (
                        "armed" if self._state.get("entry_armed") else "monitoring"
                    )
                elif self._state.get("monitor_enabled"):
                    self._state["status"] = "recovering_monitor"
                if recovered:
                    self._state["status"] = "armed"
                self._state["last_error"] = None
                self._state["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            if recovered:
                self._event(
                    "monitor_recovered",
                    {"healthy_cycles": TRANSIENT_RECOVERY_CYCLES},
                )
            return self.status()
        except Exception as exc:  # pylint: disable=broad-except
            error = _clean_error(exc)
            transient = _is_transient_monitor_error(exc)
            logger.exception("Pump live monitor cycle failed: %s", error)
            with self._lock:
                recovery_pending = bool(
                    transient
                    and (
                        self._state.get("entry_armed")
                        or self._state.get("transient_recovery_pending")
                    )
                )
                self._state["last_error"] = error
                self._state["entry_armed"] = False
                self._state["transient_recovery_pending"] = recovery_pending
                self._state["healthy_recovery_cycles"] = 0
                if recovery_pending:
                    self._state["status"] = "recovering_monitor"
                    self._state["blocked_reason"] = "monitor_cycle_transient_error"
                else:
                    self._state["status"] = "error_monitoring"
                    self._state["blocked_reason"] = "monitor_cycle_error"
                self._state["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            self._event(
                "monitor_error",
                {
                    "error": error,
                    "transient": transient,
                    "auto_recovery_pending": recovery_pending,
                },
            )
            return self.status()

    def _monitor_loop(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                enabled = bool(self._state.get("monitor_enabled"))
            if not enabled:
                return
            self.run_cycle()
            self._wake.wait(self.config().poll_interval_sec)
            self._wake.clear()

    def _process_pending_signals(
        self,
        balance: dict[str, Any],
        exchange_positions: list[dict[str, Any]],
        open_orders: list[dict[str, Any]],
        config: PumpLiveConfig,
    ) -> None:
        with self._lock:
            if not self._state.get("entry_armed"):
                return
            pending = list(self._state.get("pending_signals") or [])
        if not pending:
            return
        unknown_positions = self._unknown_exchange_positions(exchange_positions)
        unknown_orders = self._unknown_open_orders(open_orders)
        if unknown_positions or unknown_orders:
            self.disarm("unknown_exchange_state")
            return
        for decision in pending:
            with self._lock:
                if not self._state.get("entry_armed"):
                    break
                open_items = self._open_positions(self._state)
                if len(open_items) >= min(config.entry_cap, config.max_active_positions):
                    break
                if any(
                    _normalize_symbol(item.get("symbol")) == _normalize_symbol(decision.get("symbol"))
                    for item in open_items
                ):
                    self._remove_pending_locked(decision)
                    continue
                total_topup = sum(
                    _safe_float(item.get("margin_topup_usd"), 0.0)
                    for item in open_items
                )
                guaranteed_deficit = sum(
                    max(
                        0.0,
                        config.guaranteed_position_topup_usd
                        - _safe_float(item.get("margin_topup_usd"), 0.0),
                    )
                    for item in open_items
                )
                rescue_required_after_entry = (
                    total_topup
                    + guaranteed_deficit
                    + config.guaranteed_position_topup_usd
                )
            if rescue_required_after_entry > config.max_total_topup_usd + 1e-9:
                self.disarm("rescue_budget_below_new_slot_guard")
                break
            required_available = config.reserve_usd + config.slot_margin_usd
            if _safe_float(balance.get("available"), 0.0) + 1e-9 < required_available:
                self.disarm("available_balance_below_new_slot_guard")
                break
            self._open_new_position(decision, config)
            balance = self.gateway.fetch_balance()

    def _open_new_position(self, decision: dict[str, Any], config: PumpLiveConfig) -> None:
        symbol = _normalize_symbol(decision.get("symbol"))
        tier = dict(decision.get("tier") or {})
        key = _decision_key(decision)
        live_id = uuid4().hex
        position: dict[str, Any] | None = None
        try:
            ticker = self.gateway.fetch_ticker(symbol)
            reference_price = (
                _safe_float(ticker.get("last"), 0.0)
                or _safe_float(ticker.get("bid"), 0.0)
                or _safe_float(decision.get("last_close"), 0.0)
            )
            if reference_price <= 0:
                raise RuntimeError("pump_live_reference_price_missing")
            legs = build_live_legs(
                tier=tier,
                slot_margin_usd=config.slot_margin_usd,
                leverage=config.leverage,
                reference_price=reference_price,
            )
            position = {
                "live_id": live_id,
                "strategy_id": "main_pullback_tier",
                "account_alias": "bybit_pump",
                "symbol": symbol,
                "event_key": key,
                "event_id": decision.get("event_id"),
                "status": "opening",
                "opened_at_ms": _now_ms(),
                "updated_at_ms": _now_ms(),
                "closed_at_ms": None,
                "tier": tier,
                "legs": legs,
                "qty": 0.0,
                "avg_entry_price": 0.0,
                "mark_price": reference_price,
                "liq_price": None,
                "tp_price": None,
                "stop_price": None,
                "max_hold_h": _safe_int(tier.get("max_hold_h"), 168),
                "flat_confirm_count": 0,
                "margin_topup_usd": 0.0,
                "margin_prefund_floor_usd": 0.0,
                "margin_prefund_target_stop_price": None,
                "margin_prefund_next_ladder_price": None,
                "margin_prefund_confirmed_at_ms": None,
                "margin_prefund_status": (
                    "pending"
                    if config.entry_margin_prefund_enabled and len(legs) > 1
                    else "not_required"
                ),
                "last_topup_at_ms": None,
                "last_margin_reduce_at_ms": None,
                "margin_reduce_confirm_count": 0,
                "last_error": None,
                "open_decision": _compact_decision(decision),
            }
            with self._lock:
                self._state.setdefault("positions", []).append(position)
                seen = list(self._state.get("seen_events") or [])
                seen.append(key)
                self._state["seen_events"] = seen[-5000:]
                self._remove_pending_locked(decision)
                self._save_state_locked()
            self.gateway.set_leverage(symbol, config.leverage)
            first = legs[0]
            first_link_id = _order_link(live_id, "L1")
            first["order_link_id"] = first_link_id
            with self._lock:
                self._save_state_locked()
            order = self.gateway.guarded_market_order(
                symbol=symbol,
                side="sell",
                notional_usd=_safe_float(first.get("notional_usd"), 0.0),
                qty=None,
                reduce_only=False,
                order_link_id=first_link_id,
                max_slippage_bps=config.max_slippage_bps,
            )
            first.update(
                {
                    "status": "filled" if order.get("status") in {"closed", "filled"} else "submitted",
                    "order_id": order.get("id"),
                    "order_link_id": order.get("order_link_id") or first_link_id,
                    "filled_qty": order.get("filled"),
                    "avg_fill_price": order.get("average"),
                }
            )
            actual_first_price = _safe_float(order.get("average"), reference_price)
            ladder_step_pct = _safe_float(tier.get("ladder_step_pct"), 50.0)
            for leg_index, leg in enumerate(legs):
                leg["trigger_price"] = round(
                    actual_first_price * (1.0 + ladder_step_pct / 100.0 * leg_index),
                    12,
                )
            with self._lock:
                position["status"] = "open"
                position["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            self._maintain_single_position(position, config)
            self._ensure_entry_margin_prefund(position, config)
            ladder_errors: list[str] = []
            for index, leg in enumerate(legs[1:], start=2):
                ladder_link_id = _order_link(live_id, f"L{index}")
                leg["order_link_id"] = ladder_link_id
                with self._lock:
                    self._save_state_locked()
                try:
                    ladder_order = self.gateway.create_ladder_order(
                        symbol=symbol,
                        notional_usd=_safe_float(leg.get("notional_usd"), 0.0),
                        price=_safe_float(leg.get("trigger_price"), 0.0),
                        order_link_id=ladder_link_id,
                    )
                    leg.update(
                        {
                            "status": "open",
                            "order_id": ladder_order.get("id"),
                            "order_link_id": ladder_order.get("order_link_id") or ladder_link_id,
                        }
                    )
                except Exception as exc:  # keep the real first leg protected and disarm new entries
                    leg["status"] = "error"
                    leg["error"] = _clean_error(exc)
                    ladder_errors.append(leg["error"])
            with self._lock:
                position["updated_at_ms"] = _now_ms()
                if ladder_errors:
                    position["status"] = "open_degraded"
                    position["last_error"] = ";".join(ladder_errors)
                    self._state["entry_armed"] = False
                    self._state["blocked_reason"] = "ladder_order_error"
                    self._state["transient_recovery_pending"] = False
                    self._state["healthy_recovery_cycles"] = 0
                self._save_state_locked()
            self._event(
                "live_position_opened",
                {
                    "symbol": symbol,
                    "live_id": live_id,
                    "slot_margin_usd": config.slot_margin_usd,
                    "ladder_legs": len(legs),
                    "ladder_errors": ladder_errors,
                    "margin_prefund_floor_usd": _safe_float(
                        position.get("margin_prefund_floor_usd"),
                        0.0,
                    ),
                },
            )
            self._maintain_single_position(position, config)
        except Exception as exc:
            error = _clean_error(exc)
            with self._lock:
                if position is not None:
                    position["status"] = "opening_uncertain"
                    position["last_error"] = error
                    position["updated_at_ms"] = _now_ms()
                else:
                    seen = list(self._state.get("seen_events") or [])
                    seen.append(key)
                    self._state["seen_events"] = seen[-5000:]
                    self._remove_pending_locked(decision)
                self._save_state_locked()
            self.disarm("entry_execution_error")
            self._event("live_entry_failed", {"symbol": symbol, "event_key": key, "error": error})

    def _maintain_positions(self, config: PumpLiveConfig) -> None:
        with self._lock:
            items = [item for item in self._state.get("positions") or [] if item.get("status") not in {"closed"}]

        def risk_key(item: Mapping[str, Any]) -> float:
            buffer_pct = _short_liq_buffer_pct(
                _safe_float(item.get("mark_price"), 0.0),
                _optional_float(item.get("liq_price")),
            )
            return buffer_pct if buffer_pct is not None else math.inf

        items.sort(key=risk_key)
        for item in items:
            self._maintain_single_position(item, config)

    def _maintain_single_position(self, item: dict[str, Any], config: PumpLiveConfig) -> None:
        symbol = _normalize_symbol(item.get("symbol"))
        positions = self.gateway.fetch_positions()
        exchange = next(
            (
                row
                for row in positions
                if _normalize_symbol(row.get("symbol")) == symbol and row.get("side") == "short"
            ),
            None,
        )
        if not exchange:
            return
        old_qty = _safe_float(item.get("qty"), 0.0)
        old_avg = _safe_float(item.get("avg_entry_price"), 0.0)
        old_liq = _optional_float(item.get("liq_price"))
        self._apply_exchange_position(item, exchange)
        qty = _safe_float(item.get("qty"), 0.0)
        avg = _safe_float(item.get("avg_entry_price"), 0.0)
        mark = _safe_float(item.get("mark_price"), 0.0)
        liq = _optional_float(item.get("liq_price"))
        if item.get("status") == "closing":
            return
        self._refresh_leg_statuses(item)
        desired_stop = self._desired_emergency_stop(liq, config)
        if desired_stop is not None and mark >= desired_stop:
            self._close_position(item, "emergency_exchange_stop_reached", config)
            return
        self._sync_full_protection(
            item,
            config,
            force=(
                abs(qty - old_qty) > max(1e-8, qty * 1e-6)
                or abs(avg - old_avg) > max(1e-10, avg * 1e-6)
                or _material_float_change(liq, old_liq)
            ),
        )
        max_hold_ms = _safe_int(item.get("max_hold_h"), 168) * 3_600_000
        if _now_ms() - _safe_int(item.get("opened_at_ms"), _now_ms()) >= max_hold_ms:
            self._close_position(item, "time_stop", config)
            return
        self._maybe_topup_or_emergency(item, config)

    def _apply_exchange_position(
        self,
        item: dict[str, Any],
        exchange: Mapping[str, Any],
    ) -> None:
        with self._lock:
            item.update(
                {
                    "qty": _safe_float(exchange.get("qty"), 0.0),
                    "avg_entry_price": _safe_float(exchange.get("avg_price"), 0.0),
                    "mark_price": _safe_float(exchange.get("mark_price"), 0.0),
                    "liq_price": _optional_float(exchange.get("liq_price")),
                    "unrealized_pnl_usd": _safe_float(exchange.get("unrealized_pnl"), 0.0),
                    "flat_confirm_count": 0,
                    "updated_at_ms": _now_ms(),
                }
            )
            self._save_state_locked()

    @staticmethod
    def _desired_emergency_stop(
        liq_price: float | None,
        config: PumpLiveConfig,
    ) -> float | None:
        if liq_price is None or liq_price <= 0:
            return None
        gap = max(0.1, min(20.0, config.exchange_stop_gap_from_liq_pct))
        return liq_price * (1.0 - gap / 100.0)

    def _sync_full_protection(
        self,
        item: dict[str, Any],
        config: PumpLiveConfig,
        *,
        force: bool = False,
    ) -> None:
        symbol = _normalize_symbol(item.get("symbol"))
        qty = _safe_float(item.get("qty"), 0.0)
        avg = _safe_float(item.get("avg_entry_price"), 0.0)
        mark = _safe_float(item.get("mark_price"), 0.0)
        liq = _optional_float(item.get("liq_price"))
        tp_pct = _safe_float((item.get("tier") or {}).get("tp_pct"), 25.0)
        desired_tp = avg * (1.0 - tp_pct / 100.0) if avg > 0 else 0.0
        desired_stop = self._desired_emergency_stop(liq, config)
        if qty <= 0 or desired_tp <= 0 or desired_stop is None:
            raise RuntimeError("pump_live_full_protection_inputs_missing")
        if desired_tp >= mark or desired_stop <= mark:
            raise RuntimeError("pump_live_full_protection_price_invalid")
        old_tp = _safe_float(item.get("tp_price"), 0.0)
        old_stop = _safe_float(item.get("stop_price"), 0.0)
        needs_sync = (
            force
            or old_tp <= 0
            or old_stop <= 0
            or abs(desired_tp - old_tp) > max(1e-10, desired_tp * 1e-6)
            or abs(desired_stop - old_stop) > max(1e-10, desired_stop * 1e-6)
        )
        if not needs_sync:
            return
        self.gateway.set_full_protection(
            symbol,
            take_profit_price=desired_tp,
            stop_loss_price=desired_stop,
        )
        now = _now_ms()
        with self._lock:
            item["tp_price"] = desired_tp
            item["stop_price"] = desired_stop
            item["protection_updated_at_ms"] = now
            item["updated_at_ms"] = now
            self._save_state_locked()
        self._event(
            "full_protection_synced",
            {
                "symbol": symbol,
                "take_profit_price": desired_tp,
                "stop_loss_price": desired_stop,
                "qty": qty,
            },
        )

    def _refresh_leg_statuses(self, item: dict[str, Any]) -> None:
        symbol = _normalize_symbol(item.get("symbol"))
        open_by_id = {
            str(order.get("id") or ""): order
            for order in self.gateway.fetch_open_orders(symbol)
            if order.get("id")
        }
        changed = False
        for leg in item.get("legs") or []:
            order_id = str(leg.get("order_id") or "")
            if not order_id or leg.get("status") not in {"open", "submitted"}:
                continue
            order = open_by_id.get(order_id)
            if order is None:
                order = self.gateway.fetch_order(order_id, symbol)
            status = str(order.get("status") or "")
            if status in {"filled", "closed"}:
                leg["status"] = "filled"
                leg["filled_qty"] = order.get("filled")
                leg["avg_fill_price"] = order.get("average")
                changed = True
            elif status in {"canceled", "cancelled", "rejected"}:
                leg["status"] = status
                leg["error"] = "ladder_order_no_longer_open"
                changed = True
                self.disarm("ladder_order_lost")
        if changed:
            with self._lock:
                item["updated_at_ms"] = _now_ms()
                self._save_state_locked()

    def _maybe_topup_or_emergency(self, item: dict[str, Any], config: PumpLiveConfig) -> None:
        mark = _safe_float(item.get("mark_price"), 0.0)
        liq = _optional_float(item.get("liq_price"))
        if not liq or mark <= 0:
            return
        if liq <= mark:
            self._close_position(item, "emergency_liq_price_reached", config)
            return
        buffer_pct = (liq / mark - 1.0) * 100.0
        with self._lock:
            item["liq_buffer_pct"] = buffer_pct
            self._save_state_locked()
        if buffer_pct > config.warning_liq_buffer_pct:
            self._maybe_reduce_bot_margin(item, config, buffer_pct)
            return
        with self._lock:
            item["margin_reduce_confirm_count"] = 0
            self._save_state_locked()
        now = _now_ms()
        last_topup = _safe_int(item.get("last_topup_at_ms"), 0)
        topup_in_cooldown = now - last_topup < config.topup_cooldown_sec * 1000
        if topup_in_cooldown and buffer_pct > config.emergency_liq_buffer_pct:
            return
        position_topup = _safe_float(item.get("margin_topup_usd"), 0.0)
        with self._lock:
            open_items = self._open_positions(self._state)
            total_topup = sum(
                _safe_float(row.get("margin_topup_usd"), 0.0)
                for row in open_items
            )
            reserved_for_other_positions = sum(
                max(
                    0.0,
                    config.guaranteed_position_topup_usd
                    - _safe_float(row.get("margin_topup_usd"), 0.0),
                )
                for row in open_items
                if row is not item
            )
        balance = self.gateway.fetch_balance()
        available = _safe_float(balance.get("available"), 0.0)
        desired = (
            config.margin_topup_chunk_usd * 2.0
            if buffer_pct <= config.panic_liq_buffer_pct
            else config.margin_topup_chunk_usd
        )
        position_cap = (
            config.max_position_topup_usd
            if buffer_pct <= config.panic_liq_buffer_pct
            else min(
                config.max_position_topup_usd,
                config.guaranteed_position_topup_usd,
            )
        )
        allowed = min(
            desired,
            max(0.0, position_cap - position_topup),
            max(
                0.0,
                config.max_total_topup_usd
                - total_topup
                - reserved_for_other_positions,
            ),
            max(0.0, available - config.operating_cash_floor_usd),
        )
        if allowed >= 1.0:
            symbol = _normalize_symbol(item.get("symbol"))
            self.gateway.add_margin(symbol, allowed)
            position_topup_after = position_topup + allowed
            total_topup_after = total_topup + allowed
            available_after = max(0.0, available - allowed)
            with self._lock:
                item["margin_topup_usd"] = position_topup_after
                item["last_topup_at_ms"] = now
                item["margin_reduce_confirm_count"] = 0
                item["updated_at_ms"] = now
                self._save_state_locked()
            verified = self._fetch_exchange_short(symbol)
            if verified is None:
                self.disarm("margin_topup_position_unconfirmed")
                self._event(
                    "margin_topup_verification_failed",
                    {"symbol": symbol, "amount_usd": allowed},
                )
                return
            self._apply_exchange_position(item, verified)
            verified_mark = _safe_float(item.get("mark_price"), 0.0)
            verified_liq = _optional_float(item.get("liq_price"))
            verified_buffer = _short_liq_buffer_pct(verified_mark, verified_liq)
            self._event(
                "margin_added",
                {
                    "symbol": symbol,
                    "amount_usd": allowed,
                    "liq_buffer_pct_before": buffer_pct,
                    "liq_buffer_pct_after": verified_buffer,
                    "position_topup_usd": position_topup_after,
                    "total_topup_usd": total_topup_after,
                    "available_after_usd": available_after,
                },
            )
            if verified_buffer is None or verified_buffer <= config.emergency_liq_buffer_pct:
                self._close_position(item, "emergency_buffer_after_topup", config)
                return
            self._sync_full_protection(item, config, force=True)
            return
        if (
            buffer_pct > config.panic_liq_buffer_pct
            and position_topup + 1e-9 >= config.guaranteed_position_topup_usd
        ):
            return
        if buffer_pct <= config.emergency_liq_buffer_pct:
            self._close_position(item, "emergency_liq_buffer", config)
        else:
            self.disarm("margin_reserve_insufficient")
            self._event(
                "margin_topup_blocked",
                {
                    "symbol": item.get("symbol"),
                    "liq_buffer_pct": buffer_pct,
                    "available_usd": available,
                    "position_topup_usd": position_topup,
                    "total_topup_usd": total_topup,
                },
            )

    def _ensure_entry_margin_prefund(
        self,
        item: dict[str, Any],
        config: PumpLiveConfig,
    ) -> None:
        legs = list(item.get("legs") or [])
        if not config.entry_margin_prefund_enabled or len(legs) < 2:
            with self._lock:
                item["margin_prefund_status"] = "disabled_or_not_required"
                item["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            return
        symbol = _normalize_symbol(item.get("symbol"))
        exchange = self._fetch_exchange_short(symbol)
        if exchange is None:
            raise RuntimeError("pump_live_margin_prefund_position_unconfirmed")
        self._apply_exchange_position(item, exchange)
        qty = _safe_float(item.get("qty"), 0.0)
        liq = _optional_float(item.get("liq_price"))
        next_ladder = _safe_float(legs[1].get("trigger_price"), 0.0)
        if qty <= 0 or liq is None or next_ladder <= 0:
            raise RuntimeError("pump_live_margin_prefund_inputs_missing")
        target_stop = next_ladder * (
            1.0 + config.entry_margin_prefund_safety_pct / 100.0
        )
        buffer_before = _short_liq_buffer_pct(
            _safe_float(item.get("mark_price"), 0.0),
            liq,
        )
        required = required_entry_prefund_usd(
            qty=qty,
            current_liq_price=liq,
            next_ladder_price=next_ladder,
            stop_gap_from_liq_pct=config.exchange_stop_gap_from_liq_pct,
            safety_above_next_ladder_pct=(
                config.entry_margin_prefund_safety_pct
            ),
            maintenance_margin_rate=config.entry_margin_prefund_mmr,
            taker_fee_rate=config.entry_margin_prefund_taker_fee_rate,
            round_up_usd=config.entry_margin_prefund_round_usd,
        )
        position_topup = _safe_float(item.get("margin_topup_usd"), 0.0)
        if required < 1e-9:
            with self._lock:
                item["margin_prefund_floor_usd"] = position_topup
                item["margin_prefund_target_stop_price"] = target_stop
                item["margin_prefund_next_ladder_price"] = next_ladder
                item["margin_prefund_confirmed_at_ms"] = _now_ms()
                item["margin_prefund_status"] = "already_protected"
                item["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            return
        with self._lock:
            open_items = self._open_positions(self._state)
            total_topup = sum(
                _safe_float(row.get("margin_topup_usd"), 0.0)
                for row in open_items
            )
            reserved_for_other_positions = sum(
                max(
                    0.0,
                    config.guaranteed_position_topup_usd
                    - _safe_float(row.get("margin_topup_usd"), 0.0),
                )
                for row in open_items
                if row is not item
            )
        balance = self.gateway.fetch_balance()
        available = _safe_float(balance.get("available"), 0.0)
        allowed = min(
            max(0.0, config.max_position_topup_usd - position_topup),
            max(
                0.0,
                config.max_total_topup_usd
                - total_topup
                - reserved_for_other_positions,
            ),
            max(0.0, available - config.operating_cash_floor_usd),
        )
        if required > allowed + 1e-9:
            with self._lock:
                item["margin_prefund_status"] = "reserve_insufficient"
                item["margin_prefund_target_stop_price"] = target_stop
                item["margin_prefund_next_ladder_price"] = next_ladder
                item["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            self._event(
                "margin_prefund_blocked",
                {
                    "symbol": symbol,
                    "required_usd": required,
                    "allowed_usd": allowed,
                    "available_usd": available,
                },
            )
            raise RuntimeError("pump_live_margin_prefund_reserve_insufficient")
        self.gateway.add_margin(symbol, required)
        now = _now_ms()
        position_topup_after = position_topup + required
        with self._lock:
            item["margin_topup_usd"] = position_topup_after
            item["margin_prefund_floor_usd"] = position_topup_after
            item["margin_prefund_target_stop_price"] = target_stop
            item["margin_prefund_next_ladder_price"] = next_ladder
            item["margin_prefund_status"] = "verifying"
            item["last_topup_at_ms"] = now
            item["margin_reduce_confirm_count"] = 0
            item["updated_at_ms"] = now
            self._save_state_locked()
        verified = self._fetch_exchange_short(symbol)
        if verified is None:
            self.disarm("margin_prefund_position_unconfirmed")
            self._event(
                "margin_prefund_verification_failed",
                {"symbol": symbol, "amount_usd": required},
            )
            raise RuntimeError("pump_live_margin_prefund_position_unconfirmed")
        self._apply_exchange_position(item, verified)
        verified_stop = self._desired_emergency_stop(
            _optional_float(item.get("liq_price")),
            config,
        )
        verified_buffer = _short_liq_buffer_pct(
            _safe_float(item.get("mark_price"), 0.0),
            _optional_float(item.get("liq_price")),
        )
        if verified_stop is None or verified_stop + 1e-9 < target_stop:
            with self._lock:
                item["margin_prefund_status"] = "target_unconfirmed"
                item["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            self.disarm("margin_prefund_target_unconfirmed")
            self._event(
                "margin_prefund_verification_failed",
                {
                    "symbol": symbol,
                    "amount_usd": required,
                    "target_stop_price": target_stop,
                    "verified_stop_price": verified_stop,
                },
            )
            raise RuntimeError("pump_live_margin_prefund_target_unconfirmed")
        with self._lock:
            item["margin_prefund_confirmed_at_ms"] = _now_ms()
            item["margin_prefund_status"] = "confirmed"
            item["updated_at_ms"] = _now_ms()
            self._save_state_locked()
        self._event(
            "margin_added",
            {
                "symbol": symbol,
                "reason": "entry_prefund",
                "amount_usd": required,
                "position_topup_usd": position_topup_after,
                "margin_prefund_floor_usd": position_topup_after,
                "target_stop_price": target_stop,
                "verified_stop_price": verified_stop,
                "liq_buffer_pct_before": buffer_before,
                "liq_buffer_pct_after": verified_buffer,
                "total_topup_usd": total_topup + required,
                "available_after_usd": max(0.0, available - required),
            },
        )
        self._sync_full_protection(item, config, force=True)

    def _fetch_exchange_short(self, symbol: str) -> dict[str, Any] | None:
        return next(
            (
                row
                for row in self.gateway.fetch_positions()
                if _normalize_symbol(row.get("symbol")) == symbol and row.get("side") == "short"
            ),
            None,
        )

    def _maybe_reduce_bot_margin(
        self,
        item: dict[str, Any],
        config: PumpLiveConfig,
        buffer_pct: float,
    ) -> None:
        tracked_topup = _safe_float(item.get("margin_topup_usd"), 0.0)
        prefund_floor = min(
            tracked_topup,
            max(
                0.0,
                _safe_float(item.get("margin_prefund_floor_usd"), 0.0),
            ),
        )
        removable_topup = max(0.0, tracked_topup - prefund_floor)
        if (
            removable_topup < 1.0
            or buffer_pct < config.margin_reduce_trigger_buffer_pct
        ):
            with self._lock:
                item["margin_reduce_confirm_count"] = 0
                self._save_state_locked()
            return
        confirm_count = _safe_int(item.get("margin_reduce_confirm_count"), 0) + 1
        with self._lock:
            item["margin_reduce_confirm_count"] = confirm_count
            self._save_state_locked()
        if confirm_count < config.margin_reduce_confirm_cycles:
            return
        now = _now_ms()
        last_adjust = max(
            _safe_int(item.get("last_topup_at_ms"), 0),
            _safe_int(item.get("last_margin_reduce_at_ms"), 0),
        )
        if now - last_adjust < config.margin_reduce_cooldown_sec * 1000:
            return
        symbol = _normalize_symbol(item.get("symbol"))
        amount = min(config.margin_topup_chunk_usd, removable_topup)
        if amount < 1.0:
            return
        self.gateway.remove_margin(symbol, amount)
        verified = self._fetch_exchange_short(symbol)
        if verified is None:
            self.disarm("margin_reduce_position_unconfirmed")
            self._event(
                "margin_reduce_verification_failed",
                {"symbol": symbol, "amount_usd": amount},
            )
            return
        self._apply_exchange_position(item, verified)
        verified_buffer = _short_liq_buffer_pct(
            _safe_float(item.get("mark_price"), 0.0),
            _optional_float(item.get("liq_price")),
        )
        if verified_buffer is None or verified_buffer < config.margin_reduce_target_buffer_pct:
            self.gateway.add_margin(symbol, amount)
            restored = self._fetch_exchange_short(symbol)
            if restored is not None:
                self._apply_exchange_position(item, restored)
            with self._lock:
                item["last_margin_reduce_at_ms"] = now
                item["margin_reduce_confirm_count"] = 0
                self._save_state_locked()
            self._event(
                "margin_reduce_rolled_back",
                {
                    "symbol": symbol,
                    "amount_usd": amount,
                    "liq_buffer_pct_after_remove": verified_buffer,
                },
            )
            self._sync_full_protection(item, config, force=True)
            return
        remaining = max(0.0, tracked_topup - amount)
        with self._lock:
            item["margin_topup_usd"] = remaining
            item["last_margin_reduce_at_ms"] = now
            item["margin_reduce_confirm_count"] = 0
            item["updated_at_ms"] = now
            self._save_state_locked()
        self._event(
            "margin_removed",
            {
                "symbol": symbol,
                "amount_usd": amount,
                "liq_buffer_pct_before": buffer_pct,
                "liq_buffer_pct_after": verified_buffer,
                "position_topup_usd": remaining,
            },
        )
        self._sync_full_protection(item, config, force=True)

    def _close_position(self, item: dict[str, Any], reason: str, config: PumpLiveConfig) -> None:
        symbol = _normalize_symbol(item.get("symbol"))
        qty = _safe_float(item.get("qty"), 0.0)
        if qty <= 0:
            return
        self._cancel_position_orders(item)
        order = self.gateway.guarded_market_order(
            symbol=symbol,
            side="buy",
            notional_usd=None,
            qty=qty,
            reduce_only=True,
            order_link_id=_order_link(str(item.get("live_id") or uuid4().hex), "EXIT"),
            max_slippage_bps=(
                max(config.max_slippage_bps, 300.0)
                if reason.startswith("emergency_")
                else config.max_slippage_bps
            ),
        )
        with self._lock:
            item["status"] = "closing"
            item["close_reason"] = reason
            item["close_order_id"] = order.get("id")
            item["updated_at_ms"] = _now_ms()
            self._save_state_locked()
        self._event("position_close_submitted", {"symbol": symbol, "reason": reason, "qty": qty})

    def _execute_emergency_close(
        self,
        exchange_positions: list[dict[str, Any]],
        open_orders: list[dict[str, Any]],
        config: PumpLiveConfig,
    ) -> None:
        for order in open_orders:
            if str(order.get("order_link_id") or "").startswith(PUMP_ORDER_LINK_PREFIX):
                self.gateway.cancel_order(str(order.get("id") or ""), str(order.get("symbol") or ""))
        for position in exchange_positions:
            if position.get("side") != "short":
                continue
            symbol = _normalize_symbol(position.get("symbol"))
            qty = _safe_float(position.get("qty"), 0.0)
            if qty <= 0:
                continue
            self.gateway.guarded_market_order(
                symbol=symbol,
                side="buy",
                notional_usd=None,
                qty=qty,
                reduce_only=True,
                order_link_id=_order_link(uuid4().hex, "PANIC"),
                max_slippage_bps=max(config.max_slippage_bps, 300.0),
            )
        with self._lock:
            self._state["emergency_close_requested"] = False
            self._state["entry_armed"] = False
            self._state["status"] = "emergency_close_submitted"
            self._state["blocked_reason"] = "emergency_close"
            self._state["transient_recovery_pending"] = False
            self._state["healthy_recovery_cycles"] = 0
            self._save_state_locked()
        self._event("emergency_close_submitted", {"positions": len(exchange_positions)})

    def _reconcile(
        self,
        exchange_positions: list[dict[str, Any]],
        open_orders: list[dict[str, Any]],
        balance: dict[str, Any],
        config: PumpLiveConfig,
    ) -> None:
        del balance
        exchange_by_symbol = {
            _normalize_symbol(item.get("symbol")): item
            for item in exchange_positions
            if item.get("side") == "short"
        }
        unknown_positions = self._unknown_exchange_positions(exchange_positions)
        unknown_orders = self._unknown_open_orders(open_orders)
        if unknown_positions or unknown_orders:
            with self._lock:
                self._state["entry_armed"] = False
                self._state["blocked_reason"] = "unknown_exchange_state"
                self._state["transient_recovery_pending"] = False
                self._state["healthy_recovery_cycles"] = 0
                self._state["unknown_positions"] = unknown_positions
                self._state["unknown_orders"] = unknown_orders
                self._save_state_locked()
        with self._lock:
            ledger_items = self._open_positions(self._state)
        for item in ledger_items:
            symbol = _normalize_symbol(item.get("symbol"))
            actual = exchange_by_symbol.get(symbol)
            if actual:
                with self._lock:
                    item["qty"] = _safe_float(actual.get("qty"), 0.0)
                    item["avg_entry_price"] = _safe_float(actual.get("avg_price"), 0.0)
                    item["mark_price"] = _safe_float(actual.get("mark_price"), 0.0)
                    item["liq_price"] = actual.get("liq_price")
                    item["flat_confirm_count"] = 0
                    item["updated_at_ms"] = _now_ms()
                    self._save_state_locked()
                continue
            count = _safe_int(item.get("flat_confirm_count"), 0) + 1
            if count == 1:
                self._cancel_position_orders(item)
                with self._lock:
                    self._state["entry_armed"] = False
                    self._state["blocked_reason"] = "position_absent_unconfirmed"
                    self._state["transient_recovery_pending"] = False
                    self._state["healthy_recovery_cycles"] = 0
                    self._save_state_locked()
                self._event("position_absent_first_cycle", {"symbol": symbol})
            with self._lock:
                item["flat_confirm_count"] = count
                item["updated_at_ms"] = _now_ms()
                self._save_state_locked()
            if count >= config.flat_confirm_cycles:
                with self._lock:
                    item["status"] = "closed"
                    item["closed_at_ms"] = _now_ms()
                    item["close_reason"] = item.get("close_reason") or "exchange_position_flat"
                    item["qty"] = 0.0
                    if not self._open_positions(self._state) and not self._state.get("entry_armed"):
                        self._state["monitor_enabled"] = False
                        self._state["status"] = "disarmed_flat"
                    self._save_state_locked()
                self._event(
                    "position_confirmed_flat",
                    {"symbol": symbol, "reason": item.get("close_reason")},
                )

    def _cancel_position_orders(self, item: dict[str, Any]) -> None:
        symbol = _normalize_symbol(item.get("symbol"))
        for leg in item.get("legs") or []:
            if leg.get("status") not in {"open", "submitted"} or not leg.get("order_id"):
                continue
            try:
                self.gateway.cancel_order(str(leg.get("order_id")), symbol)
                leg["status"] = "canceled"
            except Exception as exc:  # pylint: disable=broad-except
                leg["cancel_error"] = _clean_error(exc)
        with self._lock:
            self._save_state_locked()

    def _unknown_exchange_positions(self, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self._lock:
            owned = {
                (_normalize_symbol(item.get("symbol")), "short")
                for item in self._open_positions(self._state)
            }
        return [
            {"symbol": item.get("symbol"), "side": item.get("side"), "qty": item.get("qty")}
            for item in positions
            if (_normalize_symbol(item.get("symbol")), str(item.get("side") or "").lower()) not in owned
        ]

    def _unknown_open_orders(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self._lock:
            owned_links = {
                str(leg.get("order_link_id") or "")
                for item in self._open_positions(self._state)
                for leg in item.get("legs") or []
                if leg.get("order_link_id")
            }
        return [
            {
                "id": item.get("id"),
                "symbol": item.get("symbol"),
                "order_link_id": item.get("order_link_id"),
            }
            for item in orders
            if (
                not bool(item.get("reduce_only"))
                and (
                    not str(item.get("order_link_id") or "").startswith(PUMP_ORDER_LINK_PREFIX)
                    or str(item.get("order_link_id") or "") not in owned_links
                )
            )
        ]

    def _remove_pending_locked(self, decision: Mapping[str, Any]) -> None:
        key = _decision_key(decision)
        self._state["pending_signals"] = [
            item
            for item in self._state.get("pending_signals") or []
            if _decision_key(item) != key
        ]

    def _emergency_requested(self) -> bool:
        with self._lock:
            return bool(self._state.get("emergency_close_requested"))

    @staticmethod
    def _open_positions(state: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [
            item
            for item in state.get("positions") or []
            if item.get("status") not in {"closed"}
        ]

    def _load_state(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return {
            "schema": "pump_live_state_v1",
            "status": payload.get("status") or "disabled",
            "monitor_enabled": bool(payload.get("monitor_enabled")),
            "entry_armed": False,
            "armed_at_ms": payload.get("armed_at_ms"),
            "updated_at_ms": payload.get("updated_at_ms"),
            "positions": list(payload.get("positions") or []),
            "seen_events": list(payload.get("seen_events") or []),
            "pending_signals": [],
            "last_preflight": payload.get("last_preflight"),
            "last_balance": payload.get("last_balance"),
            "last_exchange_positions": payload.get("last_exchange_positions") or [],
            "last_open_orders": payload.get("last_open_orders") or [],
            "last_cycle_at_ms": payload.get("last_cycle_at_ms"),
            "last_signal_batch_at_ms": payload.get("last_signal_batch_at_ms"),
            "last_signal_count": payload.get("last_signal_count") or 0,
            "last_entry_ready": payload.get("last_entry_ready") or [],
            "last_error": payload.get("last_error"),
            "blocked_reason": payload.get("blocked_reason"),
            "transient_recovery_pending": False,
            "healthy_recovery_cycles": 0,
            "emergency_close_requested": bool(payload.get("emergency_close_requested")),
            "last_notification_event": payload.get("last_notification_event"),
            "last_notification_status": payload.get("last_notification_status"),
            "last_notification_at_ms": payload.get("last_notification_at_ms"),
            "last_notification_error": payload.get("last_notification_error"),
        }

    def _save_state_locked(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self._state, ensure_ascii=True, indent=2, sort_keys=True)
        temp = self.state_path.with_name(
            f"{self.state_path.stem}.{uuid4().hex}.tmp"
        )
        try:
            with temp.open("w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            for attempt, delay in enumerate(STATE_REPLACE_RETRY_DELAYS_SEC):
                if delay > 0:
                    time.sleep(delay)
                try:
                    os.replace(temp, self.state_path)
                    return
                except OSError as exc:
                    last_attempt = attempt == len(STATE_REPLACE_RETRY_DELAYS_SEC) - 1
                    if last_attempt or not _is_retryable_state_replace_error(exc):
                        raise
        finally:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                logger.warning("Pump live temporary state cleanup failed: %s", temp)

    def _event(self, event: str, payload: dict[str, Any]) -> None:
        row = {"ts_ms": _now_ms(), "event": event, **payload}
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._append_event(row)
        notification = _pump_live_notification(event, payload)
        if self.notifier is not None and notification is not None:
            title, text, dedupe_key, cooldown_sec = notification
            self._dispatch_notification(
                event=event,
                title=title,
                text=text,
                dedupe_key=dedupe_key,
                cooldown_sec=cooldown_sec,
            )

    def _append_event(self, row: Mapping[str, Any]) -> None:
        with self._event_lock:
            with self.events_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(dict(row), ensure_ascii=True, sort_keys=True) + "\n")

    def _dispatch_notification(
        self,
        *,
        event: str,
        title: str,
        text: str,
        dedupe_key: str,
        cooldown_sec: int,
    ) -> None:
        now = _now_ms()
        with self._notification_lock:
            last_sent = self._notification_last_sent.get(dedupe_key, 0)
            if cooldown_sec > 0 and now - last_sent < cooldown_sec * 1000:
                return
            self._notification_last_sent[dedupe_key] = now

        def deliver() -> None:
            status = "error"
            error: str | None = None
            try:
                if self.notifier is not None:
                    status = str(asyncio.run(self.notifier.send_text_status(text, title=title)))
            except Exception as exc:  # notifications must never stop risk handling
                error = _clean_error(exc)
                logger.warning("Pump live notification failed event=%s error=%s", event, error)
            delivered_at = _now_ms()
            with self._lock:
                self._state["last_notification_event"] = event
                self._state["last_notification_status"] = status
                self._state["last_notification_at_ms"] = delivered_at
                self._state["last_notification_error"] = error
                self._save_state_locked()
            delivery_row: dict[str, Any] = {
                "ts_ms": delivered_at,
                "event": "notification_delivery",
                "source_event": event,
                "status": status,
            }
            if error:
                delivery_row["error"] = error
            self._append_event(delivery_row)

        if self._background_notifications:
            threading.Thread(
                target=deliver,
                name=f"pump-live-notify-{event}",
                daemon=True,
            ).start()
        else:
            deliver()


def _pump_live_notification(
    event: str,
    payload: Mapping[str, Any],
) -> tuple[str, str, str, int] | None:
    symbol = _normalize_symbol(payload.get("symbol"))
    symbol_line = f"\nМонета: {symbol}" if symbol else ""
    if event == "armed":
        cap = _safe_int(payload.get("entry_cap"), 0)
        return (
            "FeeArb Pump Live включён",
            f"Pump Live: разрешены новые входы\nЛимит одновременно: {cap}",
            "armed",
            0,
        )
    if event == "disarmed":
        reason = str(payload.get("reason") or "unknown")
        return (
            "FeeArb Pump Live остановил входы",
            f"Pump Live: новые входы отключены\nПричина: {reason}",
            f"disarmed:{reason}",
            60,
        )
    if event == "live_position_opened":
        slot_margin = _safe_float(payload.get("slot_margin_usd"), 0.0)
        legs = _safe_int(payload.get("ladder_legs"), 0)
        errors = list(payload.get("ladder_errors") or [])
        text = (
            f"Pump Live: открыта SHORT-позиция{symbol_line}"
            f"\nМаржа слота: ${slot_margin:.2f}"
            f"\nСтупеней: {legs}"
        )
        if errors:
            text += f"\nВНИМАНИЕ: ошибки лестницы: {len(errors)}"
        return (
            f"Pump Live вход {symbol}",
            text,
            f"opened:{payload.get('live_id') or symbol}",
            0,
        )
    if event == "live_entry_failed":
        error = str(payload.get("error") or "unknown")
        return (
            f"Pump Live ошибка входа {symbol}",
            f"Pump Live: вход не завершён{symbol_line}\nОшибка: {error}\nНовые входы отключены.",
            f"entry_failed:{symbol}:{error}",
            300,
        )
    if event == "margin_added":
        amount = _safe_float(payload.get("amount_usd"), 0.0)
        buffer_pct = _safe_float(payload.get("liq_buffer_pct_before"), 0.0)
        buffer_after = _optional_float(payload.get("liq_buffer_pct_after"))
        position_total = _safe_float(payload.get("position_topup_usd"), 0.0)
        portfolio_total = _safe_float(payload.get("total_topup_usd"), 0.0)
        available_after = _safe_float(payload.get("available_after_usd"), 0.0)
        buffer_after_line = (
            f"\nБуфер после проверки: {buffer_after:.2f}%"
            if buffer_after is not None
            else "\nБуфер после проверки: не подтверждён"
        )
        return (
            f"Pump Live TOP-UP {symbol}",
            (
                f"Pump Live: добавлена изолированная маржа{symbol_line}"
                f"\nСумма: ${amount:.2f}"
                f"\nБуфер до пополнения: {buffer_pct:.2f}%"
                f"{buffer_after_line}"
                f"\nВсего по позиции: ${position_total:.2f}"
                f"\nВсего по Pump-портфелю: ${portfolio_total:.2f}"
                f"\nСвободно после: ${available_after:.2f}"
            ),
            f"margin_added:{symbol}:{position_total:.2f}",
            0,
        )
    if event == "margin_topup_blocked":
        buffer_pct = _safe_float(payload.get("liq_buffer_pct"), 0.0)
        available = _safe_float(payload.get("available_usd"), 0.0)
        position_total = _safe_float(payload.get("position_topup_usd"), 0.0)
        portfolio_total = _safe_float(payload.get("total_topup_usd"), 0.0)
        return (
            f"Pump Live TOP-UP заблокирован {symbol}",
            (
                f"Pump Live: не удалось добавить допустимую маржу{symbol_line}"
                f"\nБуфер до ликвидации: {buffer_pct:.2f}%"
                f"\nСвободно: ${available:.2f}"
                f"\nДобавлено по позиции: ${position_total:.2f}"
                f"\nДобавлено всего: ${portfolio_total:.2f}"
                "\nНовые входы отключены."
            ),
            f"margin_blocked:{symbol}",
            300,
        )
    if event == "margin_removed":
        amount = _safe_float(payload.get("amount_usd"), 0.0)
        buffer_after = _safe_float(payload.get("liq_buffer_pct_after"), 0.0)
        remaining = _safe_float(payload.get("position_topup_usd"), 0.0)
        return (
            f"Pump Live возврат резерва {symbol}",
            (
                f"Pump Live: снята ранее добавленная маржа{symbol_line}"
                f"\nСумма: ${amount:.2f}"
                f"\nБуфер после: {buffer_after:.2f}%"
                f"\nОсталось добавленной маржи: ${remaining:.2f}"
            ),
            f"margin_removed:{symbol}:{remaining:.2f}",
            0,
        )
    if event == "margin_reduce_rolled_back":
        amount = _safe_float(payload.get("amount_usd"), 0.0)
        buffer_after = _optional_float(payload.get("liq_buffer_pct_after_remove"))
        return (
            f"Pump Live возврат маржи отменён {symbol}",
            (
                f"Pump Live: снятие маржи немедленно отменено{symbol_line}"
                f"\nСумма возвращена в позицию: ${amount:.2f}"
                f"\nБуфер после пробного снятия: {buffer_after if buffer_after is not None else 'не подтверждён'}"
            ),
            f"margin_reduce_rollback:{symbol}",
            300,
        )
    if event in {"margin_topup_verification_failed", "margin_reduce_verification_failed"}:
        amount = _safe_float(payload.get("amount_usd"), 0.0)
        return (
            f"Pump Live маржа не подтверждена {symbol}",
            (
                f"Pump Live: результат изменения маржи не подтверждён{symbol_line}"
                f"\nСумма: ${amount:.2f}"
                "\nНовые входы отключены; требуется проверка позиции на бирже."
            ),
            f"{event}:{symbol}",
            300,
        )
    if event == "position_close_submitted":
        reason = str(payload.get("reason") or "unknown")
        qty = _safe_float(payload.get("qty"), 0.0)
        return (
            f"Pump Live выход {symbol}",
            (
                f"Pump Live: отправлен reduce-only выход{symbol_line}"
                f"\nКоличество: {qty:.8f}"
                f"\nПричина: {reason}"
            ),
            f"close_submitted:{symbol}:{reason}",
            0,
        )
    if event == "position_absent_first_cycle":
        return (
            f"Pump Live проверяет закрытие {symbol}",
            (
                f"Pump Live: позиция не найдена в первом полном скане{symbol_line}"
                "\nОрдера добора отменены, новые входы отключены; ожидается второй скан."
            ),
            f"absent_first:{symbol}",
            300,
        )
    if event == "position_confirmed_flat":
        reason = str(payload.get("reason") or "exchange_position_flat")
        return (
            f"Pump Live позиция закрыта {symbol}",
            f"Pump Live: отсутствие позиции подтверждено двумя сканами{symbol_line}\nПричина: {reason}",
            f"confirmed_flat:{symbol}:{reason}",
            0,
        )
    if event == "emergency_close_requested":
        return (
            "FeeArb Pump Live аварийный выход",
            "Pump Live: оператор запросил закрытие всех Pump-позиций.",
            "emergency_requested",
            60,
        )
    if event == "emergency_close_submitted":
        count = _safe_int(payload.get("positions"), 0)
        return (
            "FeeArb Pump Live аварийные ордера",
            f"Pump Live: аварийные reduce-only выходы отправлены\nПозиций: {count}",
            "emergency_submitted",
            60,
        )
    if event == "monitor_error":
        error = str(payload.get("error") or "unknown")
        return (
            "FeeArb Pump Live ошибка мониторинга",
            f"Pump Live: защитный цикл завершился ошибкой\nОшибка: {error}\nНовые входы отключены.",
            f"monitor_error:{error}",
            300,
        )
    return None


def required_entry_prefund_usd(
    *,
    qty: float,
    current_liq_price: float,
    next_ladder_price: float,
    stop_gap_from_liq_pct: float,
    safety_above_next_ladder_pct: float,
    maintenance_margin_rate: float,
    taker_fee_rate: float,
    round_up_usd: float,
) -> float:
    if qty <= 0 or current_liq_price <= 0 or next_ladder_price <= 0:
        raise ValueError("pump_live_margin_prefund_inputs_invalid")
    stop_gap = max(0.1, min(20.0, stop_gap_from_liq_pct))
    target_stop = next_ladder_price * (
        1.0 + max(0.0, safety_above_next_ladder_pct) / 100.0
    )
    target_liq = target_stop / (1.0 - stop_gap / 100.0)
    if current_liq_price + 1e-12 >= target_liq:
        return 0.0
    required = (
        (target_liq - current_liq_price)
        * qty
        * (1.0 + max(0.0, maintenance_margin_rate))
        * (1.0 + max(0.0, taker_fee_rate))
    )
    increment = max(0.0001, round_up_usd)
    return math.ceil(required / increment - 1e-12) * increment


def build_live_legs(
    *,
    tier: Mapping[str, Any],
    slot_margin_usd: float,
    leverage: float,
    reference_price: float,
) -> list[dict[str, Any]]:
    legs_count = max(1, _safe_int(tier.get("ladder_legs"), 1))
    step_pct = _safe_float(tier.get("ladder_step_pct"), 50.0)
    weights = [_safe_float(item, 0.0) for item in tier.get("leg_weights") or []][:legs_count]
    if len(weights) < legs_count:
        weights.extend([1.0] * (legs_count - len(weights)))
    total_weight = sum(max(0.0, item) for item in weights) or float(legs_count)
    result: list[dict[str, Any]] = []
    for index in range(legs_count):
        margin = slot_margin_usd * max(0.0, weights[index]) / total_weight
        trigger = reference_price * (1.0 + step_pct / 100.0 * index)
        result.append(
            {
                "step": index + 1,
                "weight": weights[index],
                "trigger_price": round(trigger, 12),
                "margin_usd": round(margin, 6),
                "notional_usd": round(margin * leverage, 6),
                "status": "planned",
                "order_id": None,
                "order_link_id": None,
                "filled_qty": 0.0,
                "avg_fill_price": None,
            }
        )
    return result


def _normalize_order(row: Mapping[str, Any]) -> dict[str, Any]:
    status_map = {
        "New": "open",
        "PartiallyFilled": "open",
        "Filled": "filled",
        "Cancelled": "canceled",
        "Rejected": "rejected",
        "Deactivated": "canceled",
    }
    return {
        "id": str(row.get("orderId") or ""),
        "order_link_id": str(row.get("orderLinkId") or ""),
        "symbol": _normalize_symbol(row.get("symbol")),
        "side": str(row.get("side") or "").lower(),
        "status": status_map.get(str(row.get("orderStatus") or ""), str(row.get("orderStatus") or "").lower()),
        "qty": _safe_float(row.get("qty"), 0.0),
        "filled": _safe_float(row.get("cumExecQty"), 0.0),
        "average": _optional_float(row.get("avgPrice")),
        "price": _optional_float(row.get("price")),
        "reduce_only": _as_bool(row.get("reduceOnly")),
    }


def _normalize_ccxt_order(row: Mapping[str, Any]) -> dict[str, Any]:
    info = dict(row.get("info") or {})
    return {
        "id": str(row.get("id") or info.get("orderId") or ""),
        "order_link_id": str(info.get("orderLinkId") or ""),
        "status": str(row.get("status") or "").lower(),
        "filled": _safe_float(row.get("filled"), 0.0),
        "average": _optional_float(row.get("average")),
        "price": _optional_float(row.get("price")),
    }


def _compact_exchange_result(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {"ok": True}
    return {
        "retCode": payload.get("retCode"),
        "retMsg": payload.get("retMsg"),
        "status": payload.get("status"),
    }


def _compact_decision(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": decision.get("strategy_id"),
        "symbol": _normalize_symbol(decision.get("symbol")),
        "event_id": decision.get("event_id"),
        "state": decision.get("state"),
        "reason": decision.get("reason"),
        "ts_ms": _safe_int(decision.get("ts_ms"), 0),
        "last_close": _optional_float(decision.get("last_close")),
        "pump_pct": _optional_float(decision.get("pump_pct")),
        "tier": dict(decision.get("tier") or {}),
    }


def _decision_key(decision: Mapping[str, Any]) -> str:
    symbol = _normalize_symbol(decision.get("symbol"))
    event_id = str(decision.get("event_id") or "")
    tier = dict(decision.get("tier") or {})
    rule = str(tier.get("rule_slug") or "")
    ts_ms = _safe_int(decision.get("ts_ms"), 0)
    if not symbol:
        return ""
    return "|".join((symbol, event_id or str(ts_ms), rule))


def _order_link(live_id: str, suffix: str) -> str:
    return f"{PUMP_ORDER_LINK_PREFIX}{live_id[:12]}{suffix}"[:36]


def _normalize_symbol(value: Any) -> str:
    raw = str(value or "").upper().strip()
    if ":" in raw:
        raw = raw.split(":", 1)[0]
    return raw.replace("/", "").replace("-", "").replace("_", "")


def _read_latest_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _is_retryable_state_replace_error(exc: OSError) -> bool:
    return isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in {
        5,
        32,
        33,
    }


def _is_transient_monitor_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError, PermissionError)):
        return True
    network_error = getattr(ccxt, "NetworkError", None) if ccxt is not None else None
    if network_error is not None and isinstance(exc, network_error):
        return True
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "timed out",
            "timeout",
            "temporarily unavailable",
            "connection reset",
            "remote disconnected",
            "network is unreachable",
            "winerror 5",
            "winerror 32",
            "winerror 33",
        )
    )


def _clean_error(exc: Exception) -> str:
    text = str(exc).replace("\r", " ").replace("\n", " ")
    return text[:500]


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _material_float_change(current: float | None, previous: float | None) -> bool:
    if current is None or previous is None:
        return current != previous
    return abs(current - previous) > max(1e-10, abs(current) * 1e-6)


def _short_liq_buffer_pct(mark_price: float, liq_price: float | None) -> float | None:
    if mark_price <= 0 or liq_price is None or liq_price <= mark_price:
        return None
    return (liq_price / mark_price - 1.0) * 100.0


def _safe_float(value: Any, default: float) -> float:
    parsed = _optional_float(value)
    return parsed if parsed is not None else default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _now_ms() -> int:
    return int(time.time() * 1000)


__all__ = [
    "ARM_CONFIRMATION",
    "BybitPumpLiveGateway",
    "EMERGENCY_CONFIRMATION",
    "PREPARE_CONFIRMATION",
    "PUMP_LIVE_ENV_PATH",
    "PumpLiveConfig",
    "PumpLiveController",
    "build_live_legs",
    "load_pump_live_config",
    "required_entry_prefund_usd",
]
