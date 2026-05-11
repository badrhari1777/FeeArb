from __future__ import annotations

import asyncio
import logging
import os
import time
import atexit
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Set, Tuple

from config import BASE_DIR, STATE_DIR
from utils.notifications import NotificationRouter

try:
    import ccxt.async_support as ccxt_async  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    ccxt_async = None

logger = logging.getLogger(__name__)
_LAST_ENV_MTIME: float | None = None
KUCOIN_LEVERAGE_MARGIN_BUFFER_PCT = 0.0015
KUCOIN_LEVERAGE_MARGIN_MIN_DELTA = 1.0
KUCOIN_LEVERAGE_TARGET_TOLERANCE = 0.05
SUMMARY_TZ = timezone(timedelta(hours=3))
SUMMARY_SLOT_MINUTE = 40
SUMMARY_SLOT_WINDOW_MINUTES = 20
SUMMARY_QTY_DELTA_EPS = 1e-8
SUMMARY_SLOT_RETENTION_SEC = 72 * 3600
MARGIN_ADD_TRIGGER_BUFFER_PCT = 0.27
MARGIN_TARGET_BUFFER_PCT = 0.30
MARGIN_REDUCE_TRIGGER_BUFFER_PCT = 0.33
EXCHANGE_ABBR: dict[str, str] = {
    "binance": "BN",
    "okx": "OK",
    "bybit": "BY",
    "gate": "GT",
    "bitget": "BG",
    "bingx": "BX",
    "mexc": "MX",
    "kucoin": "KC",
}


def _bootstrap_env(force: bool = False) -> None:
    """Load .env values so account monitor sees updates without restarts."""
    global _LAST_ENV_MTIME  # pylint: disable=global-statement
    env_path = Path(BASE_DIR) / ".env"
    if not env_path.exists():
        return
    try:
        mtime = env_path.stat().st_mtime
    except OSError:
        return
    if not force and _LAST_ENV_MTIME is not None and mtime == _LAST_ENV_MTIME:
        return
    try:
        with env_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key:
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ[key] = value
    except OSError:
        logger.debug("Unable to read .env for account monitor")
        return
    _LAST_ENV_MTIME = mtime


@dataclass(slots=True)
class ExchangeSpec:
    slug: str
    ccxt_id: str
    key_var: str
    secret_var: str
    password_var: str | None = None
    settle_currency: str = "USDT"
    options: Dict[str, Any] = field(default_factory=dict)
    balance_params: Dict[str, Any] = field(default_factory=dict)
    position_params: Dict[str, Any] = field(default_factory=dict)


EXCHANGE_SPECS: Tuple[ExchangeSpec, ...] = (
    ExchangeSpec(
        slug="bybit",
        ccxt_id="bybit",
        key_var="BYBIT_API_KEY",
        secret_var="BYBIT_API_SECRET",
        settle_currency="USDT",
        options={"defaultType": "swap", "defaultSettle": "USDT"},
        balance_params={"type": "swap"},
        position_params={"type": "swap"},
    ),
    ExchangeSpec(
        slug="okx",
        ccxt_id="okx",
        key_var="OKX_API_KEY",
        secret_var="OKX_API_SECRET",
        password_var="OKX_API_PASSPHRASE",
        settle_currency="USDT",
        options={"defaultType": "swap", "defaultSettle": "usdt"},
        balance_params={"type": "swap"},
        position_params={"type": "swap"},
    ),
    ExchangeSpec(
        slug="binance",
        ccxt_id="binanceusdm",
        key_var="BINANCE_API_KEY",
        secret_var="BINANCE_API_SECRET",
        settle_currency="USDT",
        options={"defaultType": "future", "adjustForTimeDifference": True},
        balance_params={"type": "future"},
        position_params={"type": "future"},
    ),
    ExchangeSpec(
        slug="bingx",
        ccxt_id="bingx",
        key_var="BINGX_API_KEY",
        secret_var="BINGX_API_SECRET",
        settle_currency="USDT",
        options={
            "defaultType": "swap",
            "defaultSettle": "USDT",
            # Help avoid signature drift on keyed requests.
            "adjustForTimeDifference": True,
            # Include recvWindow to mirror official samples.
            "recvWindow": 10_000,
        },
        balance_params={"type": "swap", "recvWindow": 10_000},
        position_params={"type": "swap", "recvWindow": 10_000},
    ),
    ExchangeSpec(
        slug="bitget",
        ccxt_id="bitget",
        key_var="BITGET_API_KEY",
        secret_var="BITGET_API_SECRET",
        password_var="BITGET_API_PASSPHRASE",
        settle_currency="USDT",
        options={"defaultType": "swap", "defaultSettle": "USDT"},
        balance_params={"type": "swap"},
        position_params={"type": "swap"},
    ),
    ExchangeSpec(
        slug="kucoin",
        ccxt_id="kucoinfutures",
        key_var="KUCOIN_API_KEY",
        secret_var="KUCOIN_API_SECRET",
        password_var="KUCOIN_API_PASSPHRASE",
        settle_currency="USDT",
        options={"defaultType": "swap", "defaultSettle": "USDT"},
        balance_params={"type": "contract", "currency": "USDT"},
        position_params={"type": "swap"},
    ),
    ExchangeSpec(
        slug="gate",
        ccxt_id="gate",
        key_var="GATE_API_KEY",
        secret_var="GATE_API_SECRET",
        settle_currency="USDT",
        options={"defaultType": "swap", "defaultSettle": "usdt"},
        balance_params={"type": "swap"},
        position_params={"type": "swap"},
    ),
    ExchangeSpec(
        slug="mexc",
        ccxt_id="mexc",
        key_var="MEXC_API_KEY",
        secret_var="MEXC_API_SECRET",
        settle_currency="USDT",
        options={
            "defaultType": "swap",
            # Protect against minor clock drift; MEXC requires reqTime within recvWindow.
            "recvWindow": 60_000,
            "adjustForTimeDifference": True,
            "useServerTime": True,
            "timeout": 20_000,  # ms
        },
        balance_params={"type": "swap", "recvWindow": 60_000},
        position_params={"type": "swap", "recvWindow": 60_000},
    ),
)


def normalize_symbol(symbol: str | None) -> str:
    """Normalise a symbol so it can be compared across venues."""
    if not symbol:
        return ""
    cleaned = []
    for char in symbol.upper():
        if char.isalnum():
            cleaned.append(char)
    return "".join(cleaned)


def _dedupe_settle(symbol: str | None) -> str:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return ""
    for settle in ("USDT", "USDC", "USD"):
        duplicated = settle + settle
        if normalized.endswith(duplicated):
            return normalized[: -len(duplicated)] + settle
    return normalized


def _ccxt_perp_symbol(symbol: str | None) -> str:
    """Best-effort CCXT perp notation (e.g. BTCUSDT -> BTC/USDT:USDT)."""
    normalized = _dedupe_settle(symbol)
    for suffix in ("USDT", "USDC", "USD"):
        if normalized.endswith(suffix):
            base = normalized[: -len(suffix)]
            return f"{base}/{suffix}:{suffix}"
    return f"{normalized}/USDT:USDT"


class ExchangeGateway:
    """Thin wrapper around a ccxt client with exchange-specific defaults."""

    def __init__(self, spec: ExchangeSpec) -> None:
        self.spec = spec
        self.slug = spec.slug
        self.api_key = ""
        self.api_secret = ""
        self.password = ""
        self._market_meta_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._cred_signature: tuple[str, str, str] | None = None
        self._unavailable_reason: str | None = None
        self._client = None
        self._client_needs_close = False
        self._cycles_open = 0
        self.refresh_credentials(force_env=True)
        # Async client is created lazily.

    @property
    def client(self):
        return self._client

    async def _build_client(self):
        if ccxt_async is None:
            self._unavailable_reason = "ccxt.async_support is not installed"
            return None
        exchange_cls = getattr(ccxt_async, self.spec.ccxt_id, None)
        if exchange_cls is None:
            self._unavailable_reason = f"ccxt_async.{self.spec.ccxt_id} is unavailable"
            return None
        if not self.has_credentials:
            return exchange_cls({"options": dict(self.spec.options)})
        config: Dict[str, Any] = {
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "options": dict(self.spec.options),
        }
        if self.password:
            config["password"] = self.password
        try:
            client = exchange_cls(config)
            if self.slug in {"mexc", "bingx", "binance", "kucoin"}:
                # Align timestamps with server to avoid signature/recvWindow drift.
                try:
                    await client.load_time_difference()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.debug("%s time sync failed; continuing without adjustment: %s", self.slug, exc)
        except Exception as exc:  # pylint: disable=broad-except
            self._unavailable_reason = str(exc)
            logger.warning("%s: failed to instantiate ccxt async client: %s", self.slug, exc)
            return None
        self._unavailable_reason = None
        return client

    @property
    def has_credentials(self) -> bool:
        if not self.api_key or not self.api_secret:
            return False
        if self.spec.password_var:
            return bool(self.password)
        return True

    @property
    def available(self) -> bool:
        return self.client is not None and self._unavailable_reason is None

    @property
    def unavailable_reason(self) -> str | None:
        return self._unavailable_reason

    def refresh_credentials(self, force_env: bool = False) -> None:
        """Reload credentials from .env to pick up edits without restarts."""
        _bootstrap_env(force_env)
        key = os.getenv(self.spec.key_var, "").strip()
        secret = os.getenv(self.spec.secret_var, "").strip()
        password = (
            os.getenv(self.spec.password_var, "").strip() if self.spec.password_var else ""
        )
        signature = (key, secret, password)
        if signature == self._cred_signature:
            return
        self.api_key, self.api_secret, self.password = signature
        self._cred_signature = signature
        if self._client is not None:
            self._client_needs_close = True
        self._unavailable_reason = None

    async def refresh_credentials_async(self, force_env: bool = False) -> None:
        """Async variant used when a loop is running so we can close clients cleanly."""
        _bootstrap_env(force_env)
        key = os.getenv(self.spec.key_var, "").strip()
        secret = os.getenv(self.spec.secret_var, "").strip()
        password = (
            os.getenv(self.spec.password_var, "").strip() if self.spec.password_var else ""
        )
        signature = (key, secret, password)
        if signature == self._cred_signature:
            return
        self.api_key, self.api_secret, self.password = signature
        self._cred_signature = signature
        if self._client is not None:
            await self.close()
        self._unavailable_reason = None

    async def close(self) -> None:
        client = self._client
        self._client = None
        self._client_needs_close = False
        self._cycles_open = 0
        if client and hasattr(client, "close"):
            try:
                await client.close()
            except Exception:  # pylint: disable=broad-except
                pass

    def __del__(self) -> None:
        """Best-effort sync closer for GC paths (defensive)."""
        try:
            client = getattr(self, "_client", None)
            if client and hasattr(client, "close"):
                # Fire-and-forget; we are in GC, so cannot await.
                import asyncio

                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(client.close())
                    else:
                        loop.run_until_complete(client.close())
                except Exception:
                    pass
        except Exception:
            pass

    def requires_cycle_close(self) -> bool:
        """Some exchanges (e.g., mexc) require close every cycle to avoid connector leaks."""
        return self.slug == "mexc"

    async def ensure_client(self) -> None:
        if not self.has_credentials:
            if self._client:
                await self._client.close()
            self._client = None
            return
        if self._client_needs_close and self._client:
            await self._client.close()
            self._client = None
            self._client_needs_close = False
        if self._client is None:
            self._client = await self._build_client()
            if self._client is None and self._unavailable_reason is None:
                self._unavailable_reason = "Failed to initialise ccxt client"
            else:
                self._cycles_open = 0
        else:
            self._cycles_open += 1

    def _is_time_sync_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        if "-1021" in message:
            return True
        if "timestamp for this request was" in message:
            return True
        if "timestamp outside recvwindow" in message:
            return True
        if "invalid nonce" in message:
            return True
        if "kc-api-timestamp" in message:
            return True
        if "invalid kc-api-timestamp" in message:
            return True
        if "400002" in message and "timestamp" in message:
            return True
        if '"code":"400002"' in message:
            return True
        return False

    async def _sync_time_difference(self) -> bool:
        if not self.client or not hasattr(self.client, "load_time_difference"):
            return False
        try:
            await self.client.load_time_difference()
            return True
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("%s: time sync failed: %s", self.slug, exc)
            return False

    async def _call_with_time_sync_retry(
        self,
        operation: str,
        callback: Callable[[], Any],
    ) -> Any:
        try:
            return await callback()
        except Exception as exc:  # pylint: disable=broad-except
            if not self._is_time_sync_error(exc):
                raise
            synced = await self._sync_time_difference()
            if not synced:
                raise
            logger.info("%s: retrying %s after exchange time sync", self.slug, operation)
            return await callback()

    def map_symbol(self, symbol: str) -> str:
        """Map canonical symbol to exchange-specific if supported by ccxt."""
        if self.client and hasattr(self.client, "market_id"):  # type: ignore[truthy-bool]
            try:
                return self.client.market_id(symbol)  # type: ignore[union-attr]
            except Exception:
                return symbol
        return symbol

    def _gate_settle(self) -> str:
        return str(self.spec.settle_currency or "USDT").lower()

    @staticmethod
    def _gate_settle_for_contract(contract: str | None) -> str:
        normalized = str(contract or "").upper().strip()
        if normalized.endswith("_USD") or normalized.endswith("USD"):
            return "btc"
        return "usdt"

    async def _gate_contract_meta(self, contract: str) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        settle = self._gate_settle_for_contract(contract)
        cache_key = (settle, contract)
        cached = self._market_meta_cache.get(cache_key)
        if cached is not None:
            return cached

        async def _fetch() -> dict[str, Any]:
            return await self.client.publicFuturesGetSettleContractsContract(  # type: ignore[attr-defined]
                {"settle": settle, "contract": contract}
            )

        payload = await self._call_with_time_sync_retry(
            "publicFuturesGetSettleContractsContract",
            _fetch,
        )
        meta = payload if isinstance(payload, dict) else {}
        self._market_meta_cache[cache_key] = meta
        return meta

    async def _fetch_gate_balance(self) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        settle = self._gate_settle()

        async def _fetch() -> dict[str, Any]:
            return await self.client.privateFuturesGetSettleAccounts(  # type: ignore[attr-defined]
                {"settle": settle}
            )

        payload = await self._call_with_time_sync_retry(
            "privateFuturesGetSettleAccounts",
            _fetch,
        )
        info_obj = payload if isinstance(payload, dict) else {}
        asset = str(info_obj.get("currency") or self.spec.settle_currency or "USDT").upper()
        available = _safe_float(info_obj.get("available"))
        if available is None:
            available = _safe_float(info_obj.get("cross_available"))
        position_margin = _safe_float(info_obj.get("position_margin"))
        if position_margin is None or position_margin <= 0:
            position_margin = _safe_float(info_obj.get("isolated_position_margin"))
        if position_margin is None or position_margin <= 0:
            position_margin = _safe_float(info_obj.get("position_initial_margin"))
        if position_margin is None or position_margin <= 0:
            position_margin = _safe_float(info_obj.get("cross_initial_margin"))
        order_margin = _safe_float(info_obj.get("order_margin"))
        if order_margin is None or order_margin <= 0:
            order_margin = _safe_float(info_obj.get("cross_order_margin"))
        used = (position_margin or 0.0) + (order_margin or 0.0)
        total_value = None
        if available is not None:
            total_value = (available or 0.0) + used
        if total_value is None:
            total_value = _safe_float(info_obj.get("total"))
        unrealized = _safe_float(info_obj.get("unrealised_pnl"))
        if unrealized is None:
            unrealized = _safe_float(info_obj.get("cross_unrealised_pnl"))
        maintenance_margin = _safe_float(info_obj.get("maintenance_margin"))
        if maintenance_margin is None:
            maintenance_margin = _safe_float(info_obj.get("cross_maintenance_margin"))
        equity = None
        try:
            if total_value is not None:
                equity = float(total_value) + float(unrealized or 0.0)
        except Exception:
            equity = None
        buffer_pct = None
        try:
            if total_value and available is not None:
                buffer_pct = (float(available) / float(total_value)) * 100.0
        except Exception:
            buffer_pct = None
        return {
            "exchange": self.slug,
            "asset": asset,
            "total": total_value,
            "available": available,
            "used": used if used > 0 else None,
            "unrealized_pnl": unrealized,
            "margin_ratio": None,
            "equity": equity,
            "buffer_pct": buffer_pct,
            "initial_margin": position_margin if position_margin and position_margin > 0 else None,
            "maintenance_margin": maintenance_margin,
            "timestamp": _ts_to_iso(info_obj.get("update_time")),
        }

    async def _fetch_gate_positions(self) -> list[dict[str, Any]]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        settle = self._gate_settle()

        async def _fetch() -> list[dict[str, Any]]:
            return await self.client.privateFuturesGetSettlePositions(  # type: ignore[attr-defined]
                {"settle": settle}
            )

        positions = await self._call_with_time_sync_retry(
            "privateFuturesGetSettlePositions",
            _fetch,
        )
        result: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()
        for payload in positions or []:
            if not isinstance(payload, dict):
                continue
            signed_contracts = _safe_float(payload.get("size"))
            if signed_contracts is None or abs(signed_contracts) < 1e-8:
                continue
            contract = str(payload.get("contract") or payload.get("id") or "").strip()
            if not contract:
                continue
            meta = await self._gate_contract_meta(contract)
            contract_size = _safe_float(meta.get("quanto_multiplier"), default=1.0) or 1.0
            contracts = abs(float(signed_contracts))
            coin_qty = contracts * contract_size
            side = "long" if signed_contracts > 0 else "short"
            normalized_symbol = normalize_symbol(contract)
            symbol = _ccxt_perp_symbol(normalized_symbol) if normalized_symbol else contract
            notional = abs(float(_safe_float(payload.get("value")) or 0.0))
            entry_price = _safe_float(payload.get("entry_price"))
            mark_price = _safe_float(payload.get("mark_price"))
            if not notional and mark_price is not None:
                notional = coin_qty * mark_price
            gate_payload = {
                "symbol": symbol,
                "leverage": payload.get("leverage") or payload.get("lever"),
                "margin_mode": payload.get("pos_margin_mode"),
                "info": payload,
            }
            leverage, leverage_source = _extract_leverage(gate_payload)
            margin_mode, margin_mode_source = _extract_margin_mode(gate_payload, self.slug)
            result.append(
                {
                    "exchange": self.slug,
                    "symbol": symbol,
                    "exchange_symbol": contract,
                    "symbol_normalized": normalized_symbol,
                    "contracts": contracts,
                    "contract_size": contract_size,
                    "coin_qty": coin_qty,
                    "notional": notional or None,
                    "side": side,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": _safe_float(payload.get("unrealised_pnl")),
                    "percentage": None,
                    "leverage": leverage,
                    "liquidation_price": _safe_float(payload.get("liq_price")),
                    "margin_mode": margin_mode,
                    "margin_mode_source": margin_mode_source,
                    "leverage_source": leverage_source,
                    "margin_used": _safe_float(payload.get("margin")) or _safe_float(payload.get("initial_margin")),
                    "initial_margin": _safe_float(payload.get("initial_margin")),
                    "maintenance_margin": _safe_float(payload.get("maintenance_margin")),
                    "timestamp": _ts_to_iso(payload.get("update_time")) or now,
                    "raw": {"info": payload},
                }
            )
        return result

    async def fetch_balance(self) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        if self.slug == "gate":
            return await self._fetch_gate_balance()
        params = dict(self.spec.balance_params)
        balance = await self._call_with_time_sync_retry(
            "fetch_balance",
            lambda: self.client.fetch_balance(params=params),
        )
        mexc_meta: dict[str, float | None] | None = None
        if self.slug == "gate":
            self._patch_gate_balance(balance)
        elif self.slug == "mexc":
            mexc_meta = self._patch_mexc_balance(balance)
        asset = self.spec.settle_currency.upper()
        asset_row = balance.get(asset) or balance.get(asset.lower()) or {}
        totals = balance.get("total") or {}
        frees = balance.get("free") or {}
        useds = balance.get("used") or {}
        total_value = _safe_float(asset_row.get("total")) or _safe_float(totals.get(asset))
        free_value = _safe_float(asset_row.get("free")) or _safe_float(frees.get(asset))
        used_value = _safe_float(asset_row.get("used")) or _safe_float(useds.get(asset))
        info_obj = balance.get("info", {}) if isinstance(balance.get("info"), dict) else {}
        unrealized = _safe_float(info_obj.get("unrealisedPnl"))
        if mexc_meta and mexc_meta.get("unrealized") is not None:
            unrealized = mexc_meta.get("unrealized")
        # Align "available" with Bybit UI: prefer totalAvailableBalance from the raw payload.
        if self.slug == "bybit":
            info = info_obj or {}
            try:
                avail = info.get("result", {}).get("list", [{}])[0].get("totalAvailableBalance")
                override = _safe_float(avail)
                if override is not None:
                    free_value = override
            except Exception:  # pragma: no cover - defensive
                pass
        margin_ratio = _safe_float(info_obj.get("marginRatio"))
        # Attempt to pull margin fields if available.
        initial_margin = _safe_float(info_obj.get("initialMargin")) or _safe_float(info_obj.get("totalInitialMargin"))
        maintenance_margin = _safe_float(
            info_obj.get("maintenanceMargin") or info_obj.get("totalMaintenanceMargin")
        )
        # Bybit nested structure
        if self.slug == "bybit":
            try:
                bybit_entry = info_obj.get("result", {}).get("list", [{}])[0]
                if initial_margin is None:
                    initial_margin = _safe_float(bybit_entry.get("totalInitialMargin"))
                if maintenance_margin is None:
                    maintenance_margin = _safe_float(bybit_entry.get("totalMaintenanceMargin"))
                if margin_ratio is None:
                    margin_ratio = _safe_float(bybit_entry.get("marginRatio"))
            except Exception:  # pylint: disable=broad-except
                pass
        timestamp = balance.get("timestamp") or balance.get("datetime")
        equity = None
        try:
            if total_value is not None and isinstance(total_value, (int, float)):
                equity = float(total_value) + float(unrealized or 0.0)
        except Exception:
            equity = None
        buffer_pct = None
        try:
            if total_value and free_value is not None:
                buffer_pct = (float(free_value) / float(total_value)) * 100.0
        except Exception:
            buffer_pct = None
        if margin_ratio is None and total_value not in (None, 0) and used_value is not None:
            try:
                margin_ratio = abs(float(used_value)) / abs(float(total_value))
            except Exception:  # pylint: disable=broad-except
                margin_ratio = None
        return {
            "exchange": self.slug,
            "asset": asset,
            "total": total_value,
            "available": free_value,
            "used": used_value,
            "unrealized_pnl": unrealized,
            "margin_ratio": margin_ratio,
            "equity": equity,
            "buffer_pct": buffer_pct,
            "initial_margin": initial_margin,
            "maintenance_margin": maintenance_margin,
            "timestamp": _ts_to_iso(timestamp),
        }

    async def fetch_positions(self) -> List[dict[str, Any]]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        if self.slug == "gate":
            return await self._fetch_gate_positions()
        params = dict(self.spec.position_params)
        async def _fetch_positions() -> list[dict[str, Any]]:
            try:
                return await self.client.fetch_positions(params=params)  # type: ignore[attr-defined]
            except AttributeError:
                return []

        positions = await self._call_with_time_sync_retry(
            "fetch_positions",
            _fetch_positions,
        )
        result: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()
        for payload in positions or []:
            contracts = _safe_float(
                payload.get("contracts")
                or payload.get("positionAmt")
                or payload.get("size")
                or payload.get("amount")
            )
            # Some venues (e.g., Gate) return contractSize even when size=0; ignore those ghosts.
            if not contracts or abs(contracts) < 1e-8:
                continue
            symbol = payload.get("symbol") or payload.get("id")
            normalized = normalize_symbol(symbol)
            side = (payload.get("side") or "").lower()
            notional = _safe_float(payload.get("notional"))
            contract_size = _safe_float(payload.get("contractSize"), default=1.0)
            coin_qty = contracts * (contract_size or 1.0)
            info = payload.get("info") or {}
            entry_px = _safe_float(payload.get("entryPrice"))
            if entry_px is None:
                entry_px = _safe_float(
                    payload.get("avgPrice")
                    or payload.get("avgEntryPrice")
                    or payload.get("averagePrice")
                )
            if entry_px is None and isinstance(info, dict):
                entry_px = _safe_float(
                    info.get("entryPrice")
                    or info.get("avgPrice")
                    or info.get("avgEntryPrice")
                    or info.get("averagePrice")
                    or info.get("avgCost")
                    or info.get("avg_cost")
                    or info.get("openPrice")
                    or info.get("open_price")
                )
            if notional is None and entry_px is not None and contracts:
                # For venues with contract sizes != 1 (e.g., MEXC), include contract_size to avoid under-reporting notional.
                notional = contracts * (contract_size or 1.0) * entry_px
            leverage, leverage_source = _extract_leverage(payload)
            liq_price = _safe_float(payload.get("liquidationPrice"))
            if liq_price is None:
                info = payload.get("info") or {}
                liq_price = _safe_float(info.get("liquidationPrice"))
            margin_mode, margin_mode_source = _extract_margin_mode(payload, self.slug)
            mark_px = _safe_float(payload.get("markPrice"))
            if mark_px is None and isinstance(info, dict):
                mark_px = _safe_float(
                    info.get("markPrice")
                    or info.get("mark_price")
                    or info.get("mark")
                    or info.get("currentMarkPrice")
                )
            initial_margin = _safe_float(
                payload.get("initialMargin")
                or payload.get("positionInitialMargin")
                or payload.get("positionIM")
            )
            maintenance_margin = _safe_float(
                payload.get("maintenanceMargin")
                or payload.get("maintMargin")
                or payload.get("positionMaintenanceMargin")
                or payload.get("positionMaintMargin")
                or payload.get("positionMM")
            )
            if initial_margin is None:
                initial_margin = _safe_float(
                    info.get("initialMargin")
                    or info.get("positionInitialMargin")
                    or info.get("positionIM")
                )
            if maintenance_margin is None:
                maintenance_margin = _safe_float(
                    info.get("maintenanceMargin")
                    or info.get("maintMargin")
                    or info.get("positionMaintenanceMargin")
                    or info.get("positionMaintMargin")
                    or info.get("positionMM")
                )
            margin_used = None
            try:
                if leverage and leverage > 0 and notional is not None:
                    margin_used = abs(notional) / leverage
            except Exception:
                margin_used = None
            result.append(
                {
                    "exchange": self.slug,
                    "symbol": symbol,
                    "exchange_symbol": payload.get("symbol"),
                    "symbol_normalized": normalized,
                    "contracts": contracts,
                    "contract_size": contract_size,
                    "coin_qty": coin_qty,
                    "notional": notional,
                    "side": side or None,
                    "entry_price": entry_px,
                    "mark_price": mark_px,
                    "unrealized_pnl": _safe_float(payload.get("unrealizedPnl")),
                    "percentage": _safe_float(payload.get("percentage")),
                    "leverage": leverage,
                    "liquidation_price": liq_price,
                    "margin_mode": margin_mode,
                    "margin_mode_source": margin_mode_source,
                    "leverage_source": leverage_source,
                    "margin_used": margin_used,
                    "initial_margin": initial_margin,
                    "maintenance_margin": maintenance_margin,
                    "timestamp": _ts_to_iso(payload.get("timestamp")) or now,
                    "raw": payload,
                }
            )
        return result

    def _patch_gate_balance(self, balance: dict[str, Any]) -> None:
        """Gate.io futures returns a near-zero total; rebuild from raw fields."""
        info_list = balance.get("info")
        if not isinstance(info_list, list) or not info_list:
            return
        entry = info_list[0]
        try:
            available = float(entry.get("available", 0))  # free funds
            cross_initial = float(entry.get("cross_initial_margin", 0)) or float(
                entry.get("position_initial_margin", 0)
            )
            cross_order = float(entry.get("cross_order_margin", 0))
        except (TypeError, ValueError):
            return
        total = available + cross_initial + cross_order
        used = cross_initial + cross_order
        asset = self.spec.settle_currency.upper()
        patched = {"free": available, "used": used, "total": total}
        # Overwrite normalized slots
        balance[asset] = patched
        balance.setdefault("free", {})[asset] = available
        balance.setdefault("used", {})[asset] = used
        balance.setdefault("total", {})[asset] = total

    def _patch_mexc_balance(self, balance: dict[str, Any]) -> dict[str, float | None] | None:
        """Augment ccxt swap balance with position margin & equity fields."""
        info = balance.get("info") if isinstance(balance, dict) else None
        data = info.get("data") if isinstance(info, dict) else None
        if not isinstance(data, list):
            return None
        asset = self.spec.settle_currency.upper()
        entry = None
        for item in data:
            try:
                if str(item.get("currency", "")).upper() == asset:
                    entry = item
                    break
            except AttributeError:
                continue
        if not isinstance(entry, dict):
            return None
        available = _safe_float(entry.get("availableBalance"), default=0.0) or 0.0
        position_margin = _safe_float(entry.get("positionMargin"), default=0.0) or 0.0
        frozen = _safe_float(entry.get("frozenBalance"), default=0.0) or 0.0
        equity = _safe_float(entry.get("equity"))
        unrealized = _safe_float(entry.get("unrealized"))
        used = position_margin + frozen
        total = equity if equity is not None else available + used + (unrealized or 0.0)
        patched = {"free": available, "used": used, "total": total}
        balance[asset] = patched
        balance.setdefault("free", {})[asset] = available
        balance.setdefault("used", {})[asset] = used
        balance.setdefault("total", {})[asset] = total
        return {
            "available": available,
            "used": used,
            "total": total,
            "unrealized": unrealized,
        }


class AccountMonitor:
    """Background refresher that keeps ccxt balances/positions in memory."""

    def __init__(
        self,
        refresh_interval: int = 120,
        summary_interval: int = 1800,
        on_margin_adjust: Callable[[list[dict[str, Any]]], Awaitable[None] | None] | None = None,
        notifier: NotificationRouter | None = None,
    ) -> None:
        self._interval = max(30, refresh_interval)
        self._summary_interval = max(30, summary_interval)
        self._on_margin_adjust = on_margin_adjust
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}
        self._lock = asyncio.Lock()
        self._balances: list[dict[str, Any]] = []
        self._positions: list[dict[str, Any]] = []
        self._status: list[dict[str, Any]] = []
        self._last_updated: str | None = None
        self._task: asyncio.Task | None = None
        self._last_summary_slot: str | None = None
        self._summary_slot_marker_dir = STATE_DIR / "telegram_summary_slots"
        self._alert_cooldown = 600  # seconds
        self._alert_lock = asyncio.Lock()
        self._notifier = notifier or NotificationRouter()
        self._margin_alerts_enabled = True
        self._warning_buffer_pct = 0.20
        self._panic_buffer_pct = 0.15
        self._min_free_balance_abs = 0.0
        self._min_free_balance_rel = 0.0
        self._target_buffer_pct = MARGIN_TARGET_BUFFER_PCT
        self._margin_add_trigger_buffer_pct = MARGIN_ADD_TRIGGER_BUFFER_PCT
        self._margin_reduce_trigger_buffer_pct = MARGIN_REDUCE_TRIGGER_BUFFER_PCT
        self._auto_margin_enabled = True
        self._auto_margin_reduce_enabled = True
        self._enforce_isolated_margin = True
        self._enforce_leverage = True
        self._target_leverage = 3.0
        self._kucoin_isolated_topup_only = True
        self._margin_add_pct = 0.10
        self._margin_add_panic_pct = 0.20
        self._margin_reduce_pct = 0.10
        self._margin_adjust_cooldown = 300
        self._last_margin_adjust: dict[tuple[str, str, str, str], float] = {}
        self._missing_stop_alerts_enabled = True
        self._active_position_alerts: Set[tuple[str, str, str]] = set()
        self._last_position_alert_sent: dict[tuple[str, str, str], float] = {}
        self._summary_slot_marker_dir.mkdir(parents=True, exist_ok=True)
        atexit.register(self._sync_close_gateways)

    async def start(self) -> None:
        if self._task:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        # Close any open exchange clients to release connections.
        await asyncio.gather(
            *(gateway.close() for gateway in self._gateways.values()),
            return_exceptions=True,
        )

    def _sync_close_gateways(self) -> None:
        """Best-effort sync closer for interpreter exit (reload/reloader paths)."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        tasks = [gateway.close() for gateway in self._gateways.values()]
        try:
            loop.run_until_complete(asyncio.gather(*tasks))
        except Exception:  # pylint: disable=broad-except
            pass

    async def refresh_now(self, *, force_env: bool = False) -> None:
        await self._refresh(force_env=force_env)

    async def refresh_now_for_protective(self, *, force_env: bool = False) -> None:
        """Force a fresh balances/positions pull without margin actions/alerts side effects."""
        await self._refresh(
            force_env=force_env,
            enforce_margin=False,
            evaluate_alerts=False,
            send_summary=False,
        )

    def update_interval(self, seconds: int) -> None:
        self._interval = max(30, int(seconds))

    def update_summary_interval(self, seconds: int) -> None:
        self._summary_interval = max(30, int(seconds))
        self._last_summary_slot = None

    def update_alert_settings(
        self,
        *,
        send_margin_alerts: bool | None = None,
        send_missing_stop_alerts: bool | None = None,
        notification_primary_channel: str | None = None,
        notification_fallback_channel: str | None = None,
        telegram_chat_id: str | None = None,
        warning_buffer_pct: float | None = None,
        panic_buffer_pct: float | None = None,
        min_free_balance_abs: float | None = None,
        min_free_balance_rel: float | None = None,
        target_buffer_pct: float | None = None,
        auto_margin_enabled: bool | None = None,
        auto_margin_reduce_enabled: bool | None = None,
        enforce_isolated_margin: bool | None = None,
        enforce_leverage: bool | None = None,
        target_leverage: float | None = None,
        kucoin_isolated_topup_only: bool | None = None,
        margin_add_pct: float | None = None,
        margin_add_panic_pct: float | None = None,
        margin_reduce_pct: float | None = None,
        margin_add_trigger_buffer_pct: float | None = None,
        margin_reduce_trigger_buffer_pct: float | None = None,
        margin_adjust_cooldown_sec: int | None = None,
    ) -> None:
        self._notifier.update_config(
            primary_channel=notification_primary_channel,
            fallback_channel=notification_fallback_channel,
            telegram_chat_id=telegram_chat_id,
        )
        if send_margin_alerts is not None:
            self._margin_alerts_enabled = bool(send_margin_alerts)
        if send_missing_stop_alerts is not None:
            self._missing_stop_alerts_enabled = bool(send_missing_stop_alerts)
        if warning_buffer_pct is not None:
            self._warning_buffer_pct = max(0.0, float(warning_buffer_pct))
        if panic_buffer_pct is not None:
            self._panic_buffer_pct = max(0.0, float(panic_buffer_pct))
        if min_free_balance_abs is not None:
            self._min_free_balance_abs = max(0.0, float(min_free_balance_abs))
        if min_free_balance_rel is not None:
            self._min_free_balance_rel = max(0.0, float(min_free_balance_rel))
        if target_buffer_pct is not None:
            _ = target_buffer_pct
        if auto_margin_enabled is not None:
            self._auto_margin_enabled = bool(auto_margin_enabled)
        if auto_margin_reduce_enabled is not None:
            self._auto_margin_reduce_enabled = bool(auto_margin_reduce_enabled)
        if enforce_isolated_margin is not None:
            self._enforce_isolated_margin = bool(enforce_isolated_margin)
        if enforce_leverage is not None:
            self._enforce_leverage = bool(enforce_leverage)
        if target_leverage is not None:
            self._target_leverage = max(0.0, float(target_leverage))
        if kucoin_isolated_topup_only is not None:
            self._kucoin_isolated_topup_only = bool(kucoin_isolated_topup_only)
        if margin_add_pct is not None:
            self._margin_add_pct = max(0.0, float(margin_add_pct))
        if margin_add_panic_pct is not None:
            self._margin_add_panic_pct = max(0.0, float(margin_add_panic_pct))
        if margin_reduce_pct is not None:
            self._margin_reduce_pct = max(0.0, float(margin_reduce_pct))
        if margin_add_trigger_buffer_pct is not None:
            _ = margin_add_trigger_buffer_pct
        if margin_reduce_trigger_buffer_pct is not None:
            _ = margin_reduce_trigger_buffer_pct
        if margin_adjust_cooldown_sec is not None:
            self._margin_adjust_cooldown = max(0, int(margin_adjust_cooldown_sec))
        # Hardcoded liquidation-buffer policy:
        # add when below 27%, reduce when above 33%, target is 30%.
        self._target_buffer_pct = MARGIN_TARGET_BUFFER_PCT
        self._margin_add_trigger_buffer_pct = MARGIN_ADD_TRIGGER_BUFFER_PCT
        self._margin_reduce_trigger_buffer_pct = MARGIN_REDUCE_TRIGGER_BUFFER_PCT

    def snapshot(self) -> dict[str, Any]:
        return {
            "balances": [dict(entry) for entry in self._balances],
            "positions": [dict(entry) for entry in self._positions],
            "status": [dict(entry) for entry in self._status],
            "last_updated": self._last_updated,
        }

    async def _run(self) -> None:
        try:
            while True:
                await self._refresh()
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            raise

    async def _refresh(
        self,
        *,
        force_env: bool = False,
        enforce_margin: bool = True,
        evaluate_alerts: bool = True,
        send_summary: bool = True,
    ) -> None:
        balances, positions, status, refreshed = await self._collect_all(
            force_env=force_env
        )
        if enforce_margin:
            await self._maybe_enforce_margin_settings(positions)
        margin_events: list[dict[str, Any]] = []
        if evaluate_alerts:
            margin_events = await self._maybe_send_alerts(balances, positions)
        if send_summary:
            await self._maybe_send_summary(balances, positions, refreshed)
        async with self._lock:
            self._balances = balances
            self._positions = positions
            self._status = status
            if refreshed:
                self._last_updated = refreshed
        if evaluate_alerts and margin_events and self._on_margin_adjust:
            try:
                maybe_awaitable = self._on_margin_adjust(list(margin_events))
                if asyncio.iscoroutine(maybe_awaitable):
                    await maybe_awaitable
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("margin-adjust callback failed: %s", exc)

    async def _collect_all(
        self,
        force_env: bool = False,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str | None]:
        """Fetch balances/positions concurrently across exchanges."""
        balances: list[dict[str, Any]] = []
        positions: list[dict[str, Any]] = []
        status_entries: list[dict[str, Any]] = []
        refreshed: str | None = None
        timestamp = datetime.now(timezone.utc).isoformat()

        async def _fetch_positions_with_retry(gateway: ExchangeGateway) -> list[dict[str, Any]]:
            last_exc: Exception | None = None
            timeouts = (20.0, 8.0)
            for timeout in timeouts:
                try:
                    return await asyncio.wait_for(gateway.fetch_positions(), timeout=timeout)
                except Exception as exc:  # pylint: disable=broad-except
                    last_exc = exc
            if last_exc:
                raise last_exc
            return []

        async def _collect_exchange(slug: str, gateway: ExchangeGateway) -> None:
            nonlocal refreshed
            await gateway.refresh_credentials_async(force_env=force_env)
            entry = {
                "exchange": slug,
                "checked_at": timestamp,
            }
            try:
                await gateway.ensure_client()
                if not gateway.has_credentials:
                    entry["status"] = "missing_credentials"
                    entry["message"] = "Add API keys to .env"
                    return
                if not gateway.available:
                    entry["status"] = "unavailable"
                    entry["error"] = gateway.unavailable_reason or "client unavailable"
                    return
                balance = await asyncio.wait_for(gateway.fetch_balance(), timeout=15.0)
                if not balance.get("timestamp"):
                    # Ensure UI shows when this snapshot was taken even if the exchange omits a timestamp.
                    balance["timestamp"] = timestamp
                positions_result = await _fetch_positions_with_retry(gateway)
                balances.append(balance)
                positions.extend(positions_result)
                entry["status"] = "ok"
                entry["message"] = "Credentials verified"
                entry["positions_count"] = len(positions_result)
                refreshed = timestamp
            except Exception as exc:  # pylint: disable=broad-except
                entry["status"] = "error"
                entry["error"] = str(exc)
                entry["positions_error"] = str(exc)
                logger.warning("%s: account refresh failed: %s", slug, exc)
            status_entries.append(entry)

        tasks = []
        for slug, gateway in self._gateways.items():
            coro = _collect_exchange(slug, gateway)
            tasks.append(asyncio.create_task(coro))

        # Execute all exchanges concurrently; swallow per-exchange timeouts to keep others responsive.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for slug, result in zip(self._gateways.keys(), results):
            if isinstance(result, Exception):
                status_entries.append(
                    {
                        "exchange": slug,
                        "status": "error",
                        "error": str(result),
                        "checked_at": timestamp,
                    }
                )
        return balances, positions, status_entries, refreshed

    async def _maybe_send_summary(
        self,
        balances: list[dict[str, Any]],
        positions: list[dict[str, Any]],
        refreshed_at: str | None,
    ) -> None:
        """Send an hourly position digest via Telegram around xx:40."""
        slot_key = self._summary_slot_key()
        if slot_key is None:
            return
        if slot_key == self._last_summary_slot:
            return
        acquired, claim_path, reason = self._acquire_summary_slot_claim(slot_key)
        if not acquired:
            if reason in {"sent", "claimed"}:
                self._last_summary_slot = slot_key
            return
        warnings = await self._mexc_stop_warnings(positions) if self._missing_stop_alerts_enabled else []
        summary_positions = await self._positions_for_summary(positions)
        text = self._build_positions_summary(summary_positions, refreshed_at, warnings)
        send_status = await self._send_notification_text_status(text, title="FeeArb positions digest")
        self._finalize_summary_slot_claim(slot_key, claim_path, send_status)
        if send_status == "ok":
            self._last_summary_slot = slot_key

    def _summary_slot_file_stem(self, slot_key: str) -> str:
        return slot_key.replace(" ", "_").replace(":", "-")

    def _summary_slot_claim_path(self, slot_key: str) -> Path:
        return self._summary_slot_marker_dir / f"{self._summary_slot_file_stem(slot_key)}.claim"

    def _summary_slot_sent_path(self, slot_key: str) -> Path:
        return self._summary_slot_marker_dir / f"{self._summary_slot_file_stem(slot_key)}.sent"

    def _prune_summary_slot_markers(self) -> None:
        cutoff = time.time() - SUMMARY_SLOT_RETENTION_SEC
        try:
            markers = list(self._summary_slot_marker_dir.glob("*.claim"))
            markers.extend(self._summary_slot_marker_dir.glob("*.sent"))
        except OSError:
            return
        for marker in markers:
            try:
                if marker.stat().st_mtime < cutoff:
                    marker.unlink(missing_ok=True)
            except OSError:
                continue

    def _acquire_summary_slot_claim(self, slot_key: str) -> tuple[bool, Path | None, str]:
        self._prune_summary_slot_markers()
        sent_path = self._summary_slot_sent_path(slot_key)
        if sent_path.exists():
            return False, None, "sent"
        claim_path = self._summary_slot_claim_path(slot_key)
        try:
            fd = os.open(str(claim_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False, None, "claimed"
        except OSError as exc:
            logger.warning("summary slot claim failed (%s): %s", slot_key, exc)
            return True, None, "claim_bypass"
        try:
            marker_payload = f"pid={os.getpid()} ts={datetime.now(timezone.utc).isoformat()}\n"
            os.write(fd, marker_payload.encode("utf-8"))
        finally:
            os.close(fd)
        return True, claim_path, "claimed"

    def _finalize_summary_slot_claim(self, slot_key: str, claim_path: Path | None, send_status: str) -> None:
        if claim_path is None:
            return
        if send_status == "ok":
            sent_path = self._summary_slot_sent_path(slot_key)
            try:
                claim_path.replace(sent_path)
            except FileNotFoundError:
                return
            except OSError as exc:
                logger.warning("summary slot marker finalize failed (%s): %s", slot_key, exc)
            return
        if send_status in {"http_error", "skipped"}:
            try:
                claim_path.unlink(missing_ok=True)
            except OSError:
                pass
            return
        # Ambiguous transport failure: keep claim for this slot to suppress duplicates.
        logger.warning(
            "summary send uncertain for slot %s; retries suppressed until next slot",
            slot_key,
        )

    async def _positions_for_summary(self, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        prepared = [dict(item) for item in (positions or [])]
        missing: dict[tuple[str, str, str], list[int]] = {}
        for idx, item in enumerate(prepared):
            rate = _safe_float(item.get("funding_rate") or item.get("fundingRate"))
            if rate is not None:
                item["funding_rate"] = rate
                continue
            exchange = str(item.get("exchange") or "").lower()
            raw_symbol = str(item.get("symbol") or "").strip()
            symbol_norm = _dedupe_settle(item.get("symbol_normalized") or raw_symbol)
            if not exchange or not symbol_norm:
                continue
            missing.setdefault((exchange, raw_symbol, symbol_norm), []).append(idx)
        for (exchange, raw_symbol, symbol_norm), indexes in missing.items():
            rate = await self._fetch_summary_funding_rate(exchange, raw_symbol, symbol_norm)
            if rate is None:
                continue
            for idx in indexes:
                prepared[idx]["funding_rate"] = rate
        return prepared

    async def _fetch_summary_funding_rate(
        self,
        exchange: str,
        raw_symbol: str,
        symbol_norm: str,
    ) -> float | None:
        gateway = self._gateways.get(exchange)
        if gateway is None:
            return None
        try:
            await gateway.ensure_client()
        except Exception:  # pylint: disable=broad-except
            return None
        client = gateway.client
        if client is None:
            return None
        candidates: list[str] = []
        for candidate in (
            raw_symbol,
            gateway.map_symbol(raw_symbol) if raw_symbol else "",
            gateway.map_symbol(symbol_norm) if symbol_norm else "",
            _ccxt_perp_symbol(symbol_norm) if symbol_norm else "",
            symbol_norm,
        ):
            cand = str(candidate or "").strip()
            if not cand:
                continue
            if cand in candidates:
                continue
            candidates.append(cand)
        for candidate in candidates:
            try:
                async def _fetch() -> dict[str, Any]:
                    return await client.fetch_funding_rate(candidate)  # type: ignore[union-attr]

                payload = await asyncio.wait_for(
                    gateway._call_with_time_sync_retry("fetch_funding_rate", _fetch),
                    timeout=12.0,
                )
                rate = _safe_float((payload or {}).get("fundingRate"))
                if rate is not None:
                    return rate
            except Exception:  # pylint: disable=broad-except
                continue
        return None

    def _summary_slot_key(self, now_utc: datetime | None = None) -> str | None:
        now = (now_utc or datetime.now(timezone.utc)).astimezone(SUMMARY_TZ)
        minute = int(now.minute)
        if minute < SUMMARY_SLOT_MINUTE:
            return None
        if minute >= (SUMMARY_SLOT_MINUTE + SUMMARY_SLOT_WINDOW_MINUTES):
            return None
        return now.strftime("%Y-%m-%d %H")

    def _build_positions_summary(
        self,
        positions: list[dict[str, Any]],
        refreshed_at: str | None,
        warnings: list[str] | None = None,
    ) -> str:
        time_only = "unknown"
        if refreshed_at:
            try:
                dt = datetime.fromisoformat(refreshed_at)
                dt = dt.astimezone(SUMMARY_TZ)
                time_only = dt.strftime("%H:%M")
            except Exception:  # pylint: disable=broad-except
                time_only = refreshed_at
        header = f"{time_only} Positions"
        lines = [header]

        grouped: dict[str, dict[str, Any]] = {}
        for entry in positions:
            symbol = _dedupe_settle(
                entry.get("symbol_normalized") or entry.get("symbol")
            )
            if not symbol:
                continue
            side = str(entry.get("side") or "").lower()
            exchange = str(entry.get("exchange") or "").lower()
            qty_raw = _safe_float(entry.get("coin_qty"))
            if qty_raw is None:
                contracts = _safe_float(entry.get("contracts")) or 0.0
                contract_size = _safe_float(entry.get("contract_size"), default=1.0) or 1.0
                qty_raw = contracts * contract_size
            qty_abs = abs(float(qty_raw or 0.0))
            if side == "long":
                qty_signed = qty_abs
            elif side == "short":
                qty_signed = -qty_abs
            else:
                qty_signed = float(qty_raw or 0.0)
            notional = abs(float(_safe_float(entry.get("notional")) or 0.0))
            if notional <= 0 and qty_abs > 0:
                entry_px = _safe_float(entry.get("entry_price"))
                if entry_px is not None and entry_px > 0:
                    notional = qty_abs * entry_px
            grouped.setdefault(symbol, {"symbol": symbol, "legs": []})
            grouped[symbol]["legs"].append(
                {
                    "exchange": exchange,
                    "side": side,
                    "qty_abs": qty_abs,
                    "qty_signed": qty_signed,
                    "notional": notional,
                    "entry_price": _safe_float(entry.get("entry_price")),
                    "mark_price": _safe_float(entry.get("mark_price")),
                    "funding_rate": _safe_float(entry.get("funding_rate") or entry.get("fundingRate")),
                }
            )

        if not grouped:
            lines.append("No live positions.")

        def _weighted_avg(items: list[dict[str, Any]], key: str) -> float | None:
            total_w = 0.0
            total_v = 0.0
            for item in items:
                val = _safe_float(item.get(key))
                if val is None:
                    continue
                weight = _safe_float(item.get("qty_abs"))
                if weight is None or weight <= 0:
                    weight = _safe_float(item.get("notional"))
                if weight is None or weight <= 0:
                    continue
                total_w += weight
                total_v += val * weight
            if total_w <= 0:
                return None
            return total_v / total_w

        def _spread_pct(long_price: float | None, short_price: float | None) -> float | None:
            if long_price is None or short_price is None or long_price == 0:
                return None
            return (long_price - short_price) / long_price * 100.0

        def _fmt_signed(value: float | None, decimals: int, suffix: str = "") -> str:
            if value is None:
                return "-"
            return f"{value:+.{decimals}f}{suffix}"

        def _fmt_notional(value: float) -> str:
            if value >= 1_000_000:
                return f"${value / 1_000_000:.1f}m"
            if value >= 1_000:
                return f"${value / 1_000:.1f}k"
            return f"${value:.0f}"

        def _fmt_qty(value: float) -> str:
            rounded = round(value)
            if abs(value - rounded) < 1e-6:
                return str(int(rounded))
            text = f"{value:.4f}"
            return text.rstrip("0").rstrip(".")

        def _exchange_codes(legs: list[dict[str, Any]]) -> str:
            long_legs = sorted(
                [leg for leg in legs if leg.get("side") == "long"],
                key=lambda leg: str(leg.get("exchange") or ""),
            )
            short_legs = sorted(
                [leg for leg in legs if leg.get("side") == "short"],
                key=lambda leg: str(leg.get("exchange") or ""),
            )
            other_legs = sorted(
                [leg for leg in legs if leg.get("side") not in {"long", "short"}],
                key=lambda leg: str(leg.get("exchange") or ""),
            )
            ordered = long_legs + short_legs + other_legs
            seen: set[str] = set()
            out: list[str] = []
            for leg in ordered:
                exchange = str(leg.get("exchange") or "").lower()
                if not exchange or exchange in seen:
                    continue
                seen.add(exchange)
                out.append(EXCHANGE_ABBR.get(exchange, exchange[:2].upper()))
            return ",".join(out) if out else "-"

        for symbol in sorted(grouped.keys()):
            legs = grouped[symbol]["legs"]
            long_legs = [leg for leg in legs if leg.get("side") == "long"]
            short_legs = [leg for leg in legs if leg.get("side") == "short"]
            long_entry = _weighted_avg(long_legs, "entry_price")
            short_entry = _weighted_avg(short_legs, "entry_price")
            long_mark = _weighted_avg(long_legs, "mark_price")
            short_mark = _weighted_avg(short_legs, "mark_price")
            entry_spread = _spread_pct(long_entry, short_entry)
            mark_spread = _spread_pct(long_mark, short_mark)
            spread_delta = (
                mark_spread - entry_spread
                if mark_spread is not None and entry_spread is not None
                else None
            )
            long_funding = _weighted_avg(long_legs, "funding_rate")
            short_funding = _weighted_avg(short_legs, "funding_rate")
            funding_delta = (
                short_funding - long_funding
                if short_funding is not None and long_funding is not None
                else None
            )
            long_notional = sum(float(_safe_float(leg.get("notional")) or 0.0) for leg in long_legs)
            short_notional = sum(float(_safe_float(leg.get("notional")) or 0.0) for leg in short_legs)
            total_notional = sum(float(_safe_float(leg.get("notional")) or 0.0) for leg in legs)
            nominal = 0.0
            if long_notional > 0 and short_notional > 0:
                nominal = min(long_notional, short_notional)
            elif long_notional > 0 or short_notional > 0:
                nominal = max(long_notional, short_notional)
            else:
                nominal = total_notional
            qty_delta = sum(float(_safe_float(leg.get("qty_signed")) or 0.0) for leg in legs)
            qty_suffix = ""
            if abs(qty_delta) > SUMMARY_QTY_DELTA_EPS:
                qty_suffix = f"  qDelta {_fmt_qty(qty_delta)}"
            funding_delta_text = (
                _fmt_signed(funding_delta * 100.0, 4, "%")
                if funding_delta is not None
                else "-"
            )
            lines.append(
                (
                    f"{_exchange_codes(legs)}  {symbol}  {_fmt_notional(nominal)}  "
                    f"{_fmt_signed(spread_delta, 2)}  {funding_delta_text}"
                    f"{qty_suffix}"
                )
            )

        for idx, warning in enumerate(warnings or []):
            prefix = "Warnings:" if idx == 0 else " -"
            lines.append(f"{prefix} {warning}")
        return "\n".join(lines)

    async def _mexc_stop_warnings(self, positions: list[dict[str, Any]]) -> list[str]:
        """Check MEXC legs for protective stops and emit warnings for balance digest."""
        mexc_positions = [p for p in positions if str(p.get("exchange") or "").lower() == "mexc"]
        if not mexc_positions:
            return []
        gateway = self._gateways.get("mexc")
        if gateway is None:
            return ["WARNING mexc: шлюз недоступен, стопы не проверены"]
        try:
            await gateway.refresh_credentials_async()
            await gateway.ensure_client()
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("mexc stop check init failed: %s", exc)
            return ["WARNING mexc: не удалось подготовить клиент для проверки стопов"]
        if not gateway.client:
            return ["WARNING mexc: нет клиента, стопы не проверены"]

        warnings: list[str] = []
        cache: dict[str, dict[str, Any]] = {}
        for pos in mexc_positions:
            symbol = str(pos.get("symbol") or pos.get("symbol_normalized") or "")
            if not symbol:
                continue
            existing = cache.get(symbol)
            if existing is None:
                try:
                    existing = await self._fetch_existing_orders(gateway, symbol)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.debug("mexc stop fetch failed for %s: %s", symbol, exc)
                    existing = {"order_ids": [], "error": str(exc)}
                cache[symbol] = existing
            stop_val = _safe_float(existing.get("stop"))
            if stop_val is None or stop_val <= 0:
                warnings.append(f"WARNING mexc {normalize_symbol(symbol)}: стоп не найден")
        return warnings

    async def _fetch_existing_orders(self, gateway: ExchangeGateway, symbol: str) -> dict[str, Any]:
        orders: list[dict[str, Any]] = []
        mapped_symbol = gateway.map_symbol(symbol)
        try:
            orders = await gateway.client.fetch_open_orders(mapped_symbol)  # type: ignore[union-attr]
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("open orders fetch failed for %s %s: %s", gateway.slug, symbol, exc)
            return {"order_ids": [], "error": str(exc)}
        stop = None
        take = None
        order_ids: list[str] = []
        for order in orders or []:
            oid = str(order.get("id") or "")
            if oid:
                order_ids.append(oid)
            info = order.get("info") or {}
            stop_px = _safe_float(info.get("stopLossPrice") or order.get("stopPrice") or info.get("triggerPrice"))
            take_px = _safe_float(info.get("takeProfitPrice"))
            reduce_flag = info.get("reduceOnly") or info.get("reduce_only") or order.get("reduceOnly")
            if reduce_flag is False:
                continue
            if stop_px:
                stop = stop_px
            if take_px:
                take = take_px
        return {"stop": stop, "take": take, "order_ids": order_ids}

    async def _maybe_send_alerts(
        self,
        balances: list[dict[str, Any]],
        positions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Evaluate balances and positions for Telegram alerts."""
        if not balances:
            return []
        margin_events: list[dict[str, Any]] = []
        position_alerts: list[tuple[tuple[str, str, str], dict[str, Any]]] = []
        resolved_positions: list[tuple[str, str, str]] = []
        now = time.time()
        total_isolated = 0
        below_add_trigger = 0
        above_reduce_trigger = 0
        add_ok = 0
        add_fail = 0
        reduce_ok = 0
        reduce_fail = 0
        min_buffer: float | None = None

        balances_by_exchange: dict[str, dict[str, Any]] = {}
        for entry in balances:
            exchange = str(entry.get("exchange") or "").lower()
            if exchange:
                balances_by_exchange[exchange] = entry

        positions_by_exchange: dict[str, list[dict[str, Any]]] = {}
        for pos in positions or []:
            exchange = str(pos.get("exchange") or "").lower()
            if not exchange:
                continue
            positions_by_exchange.setdefault(exchange, []).append(pos)

        for exchange, exchange_positions in positions_by_exchange.items():
            entry = balances_by_exchange.get(exchange)
            asset = str(entry.get("asset") or "").upper() if entry else ""
            isolated_positions = [
                pos for pos in exchange_positions if str(pos.get("margin_mode") or "").lower() == "isolated"
            ]
            if not isolated_positions:
                continue
            for pos in isolated_positions:
                total_isolated += 1
                symbol = normalize_symbol(pos.get("symbol") or pos.get("symbol_normalized"))
                if not symbol:
                    continue
                side = str(pos.get("side") or "").lower() or "unknown"
                key = (exchange, symbol, side)
                buffer_pct = self._position_liq_buffer_pct(pos)
                if buffer_pct is None:
                    continue
                if min_buffer is None or buffer_pct < min_buffer:
                    min_buffer = buffer_pct
                if buffer_pct < self._margin_add_trigger_buffer_pct:
                    below_add_trigger += 1
                    margin_action = await self._maybe_adjust_isolated_margin(
                        exchange=exchange,
                        position=pos,
                        balance_entry=entry or {},
                        buffer_pct=buffer_pct,
                    )
                    status = str(margin_action.get("status") or "")
                    if status == "ok":
                        add_ok += 1
                        amount_val = _safe_float(margin_action.get("amount"))
                        target_val = _safe_float(margin_action.get("target_buffer_pct"))
                        margin_events.append(
                            {
                                "exchange": exchange,
                                "symbol": symbol,
                                "side": side,
                                "action": "add",
                                "amount": amount_val,
                                "buffer_pct": buffer_pct,
                                "target_buffer_pct": target_val,
                                "ts": now,
                            }
                        )
                        logger.info(
                            "%s %s %s margin top-up ok: +%s (buffer %.2f%% -> %.2f%%)",
                            exchange,
                            symbol,
                            side,
                            margin_action.get("amount"),
                            buffer_pct * 100.0,
                            self._target_buffer_pct * 100.0,
                        )
                        continue
                    detail = margin_action.get("error") or margin_action.get("reason") or status
                    if status in ("disabled", "cooldown", "skip", "no_action"):
                        logger.info(
                            "%s %s %s margin top-up skipped: %s",
                            exchange,
                            symbol,
                            side,
                            detail,
                        )
                    else:
                        add_fail += 1
                        logger.warning(
                            "%s %s %s margin top-up failed: %s",
                            exchange,
                            symbol,
                            side,
                            detail,
                        )
                    if self._margin_alerts_enabled:
                        qty = _safe_float(pos.get("coin_qty")) or _safe_float(pos.get("contracts"))
                        position_alerts.append(
                            (
                                key,
                                {
                                    "exchange": exchange,
                                    "asset": asset,
                                    "symbol": symbol,
                                    "side": side,
                                    "margin_mode": "isolated",
                                    "severity": "low_buffer",
                                    "buffer_pct": buffer_pct,
                                    "mark_price": _safe_float(pos.get("mark_price")),
                                    "liq_price": _safe_float(pos.get("liquidation_price")),
                                    "quantity": qty,
                                    "available": _safe_float((entry or {}).get("available")),
                                    "margin_action": margin_action,
                                },
                            )
                        )
                elif buffer_pct > self._margin_reduce_trigger_buffer_pct:
                    above_reduce_trigger += 1
                    reduce_action = await self._maybe_reduce_isolated_margin(
                        exchange=exchange,
                        position=pos,
                        buffer_pct=buffer_pct,
                    )
                    status = str(reduce_action.get("status") or "")
                    if status == "ok":
                        reduce_ok += 1
                        amount_val = _safe_float(reduce_action.get("amount"))
                        target_val = _safe_float(reduce_action.get("target_buffer_pct"))
                        margin_events.append(
                            {
                                "exchange": exchange,
                                "symbol": symbol,
                                "side": side,
                                "action": "reduce",
                                "amount": amount_val,
                                "buffer_pct": buffer_pct,
                                "target_buffer_pct": target_val,
                                "ts": now,
                            }
                        )
                        logger.info(
                            "%s %s %s margin reduce ok: -%s (buffer %.2f%% -> %.2f%%)",
                            exchange,
                            symbol,
                            side,
                            reduce_action.get("amount"),
                            buffer_pct * 100.0,
                            self._target_buffer_pct * 100.0,
                        )
                    else:
                        detail = reduce_action.get("error") or reduce_action.get("reason") or status
                        if status in ("disabled", "cooldown", "skip", "no_action"):
                            logger.info(
                                "%s %s %s margin reduce skipped: %s",
                                exchange,
                                symbol,
                                side,
                                detail,
                            )
                        else:
                            reduce_fail += 1
                            logger.warning(
                                "%s %s %s margin reduce failed: %s",
                                exchange,
                                symbol,
                                side,
                                detail,
                            )
                else:
                    if key in self._active_position_alerts:
                        resolved_positions.append(key)

        if total_isolated:
            if below_add_trigger == 0 and above_reduce_trigger == 0:
                if min_buffer is not None:
                    logger.info(
                        "Isolated margin check ok: positions=%s min_buffer=%.2f%%",
                        total_isolated,
                        min_buffer * 100.0,
                    )
                else:
                    logger.info("Isolated margin check ok: positions=%s", total_isolated)
            else:
                logger.info(
                    "Isolated margin policy: positions=%s below_add=%s above_reduce=%s add_ok=%s add_fail=%s reduce_ok=%s reduce_fail=%s",
                    total_isolated,
                    below_add_trigger,
                    above_reduce_trigger,
                    add_ok,
                    add_fail,
                    reduce_ok,
                    reduce_fail,
                )

        async with self._alert_lock:
            for key in resolved_positions:
                self._active_position_alerts.discard(key)

            for key, payload in position_alerts:
                last = self._last_position_alert_sent.get(key, 0.0)
                if (now - last) < self._alert_cooldown and key in self._active_position_alerts:
                    continue
                if await self._send_telegram_position_alert(payload):
                    self._active_position_alerts.add(key)
                    self._last_position_alert_sent[key] = now
        return margin_events

    async def send_notification_message(self, text: str, *, title: str | None = None) -> bool:
        """Expose notification sending for external callers (throttling handled by caller)."""
        return await self._send_notification_text(text, title=title)

    async def send_telegram_message(self, text: str) -> bool:
        """Backward-compatible alias used by older callers."""
        return await self.send_notification_message(text)


    async def _send_telegram_position_alert(self, payload: dict[str, Any]) -> bool:
        text = self._format_position_alert_message(payload)
        return await self._send_notification_text(text, title="FeeArb margin alert")

    def _format_position_alert_message(self, payload: dict[str, Any]) -> str:
        exchange = payload.get("exchange", "").upper()
        symbol = payload.get("symbol", "")
        side = payload.get("side", "")
        severity = payload.get("severity", "warning")
        buffer_pct = payload.get("buffer_pct")
        mark_price = payload.get("mark_price")
        liq_price = payload.get("liq_price")
        qty = payload.get("quantity")
        asset = payload.get("asset", "")
        available = payload.get("available")
        free_ok = available is not None and available > 0
        margin_action = payload.get("margin_action") or {}
        parts = [
            f"[ALERT] Isolated margin risk on {exchange} {symbol} ({side})",
            f"Mode: isolated | Severity: {severity}",
        ]
        if buffer_pct is not None:
            parts.append(f"Buffer to liq: {buffer_pct * 100:.2f}%")
        if mark_price is not None:
            parts.append(f"Mark: {mark_price}")
        if liq_price is not None:
            parts.append(f"Liq: {liq_price}")
        if qty is not None:
            try:
                parts.append(f"Qty: {float(qty):g}")
            except Exception:
                parts.append(f"Qty: {qty}")
        if available is None:
            parts.append("Free balance: unknown")
        else:
            status = "can add margin" if free_ok else "no free balance"
            parts.append(f"Free balance: {available} {asset} ({status})")
        if margin_action:
            action = margin_action.get("action")
            status = margin_action.get("status")
            amount = margin_action.get("amount")
            reason = margin_action.get("error") or margin_action.get("reason")
            if action:
                parts.append(f"Margin action: {action} ({status})")
            if amount:
                parts.append(f"Margin amount: {amount}")
            if reason:
                parts.append(f"Margin note: {reason}")
        return "\n".join(str(p) for p in parts)

    def _position_liq_buffer_pct(self, position: dict[str, Any]) -> float | None:
        mark_price = _safe_float(position.get("mark_price"))
        liq_price = _safe_float(position.get("liquidation_price"))
        side = str(position.get("side") or "").lower()
        if mark_price is None or mark_price <= 0 or liq_price is None or liq_price <= 0:
            return None
        if side == "long" and liq_price < mark_price:
            return (mark_price - liq_price) / mark_price
        if side == "short" and liq_price > mark_price:
            return (liq_price - mark_price) / mark_price
        return None

    def _position_margin_used(self, position: dict[str, Any]) -> float | None:
        margin_used = _safe_float(position.get("margin_used"))
        if margin_used is not None and margin_used > 0:
            return margin_used
        raw = position.get("raw") if isinstance(position, dict) else None
        if isinstance(raw, dict):
            for key in ("posMargin", "positionBalance", "positionIM", "posInit", "margin"):
                value = _safe_float(raw.get(key))
                if value is not None and value > 0:
                    return value
            info = raw.get("info")
            if isinstance(info, dict):
                for key in ("posMargin", "positionBalance", "positionIM", "posInit", "margin"):
                    value = _safe_float(info.get(key))
                    if value is not None and value > 0:
                        return value
        initial_margin = _safe_float(
            position.get("initial_margin")
            or position.get("position_initial_margin")
            or position.get("position_im")
        )
        if initial_margin is not None and initial_margin > 0:
            return initial_margin
        if isinstance(raw, dict):
            for key in (
                "initialMargin",
                "positionInitialMargin",
                "positionIM",
                "isolatedWallet",
                "positionBalance",
                "margin",
            ):
                value = _safe_float(raw.get(key))
                if value is not None and value > 0:
                    return value
            info = raw.get("info")
            if isinstance(info, dict):
                for key in (
                    "initialMargin",
                    "positionInitialMargin",
                    "positionIM",
                    "isolatedWallet",
                    "positionBalance",
                    "margin",
                ):
                    value = _safe_float(info.get(key))
                    if value is not None and value > 0:
                        return value
        notional = _safe_float(position.get("notional"))
        leverage = _safe_float(position.get("leverage"))
        if notional is None or leverage is None or leverage <= 0:
            return None
        return abs(notional) / leverage

    def _position_margin_base(self, position: Mapping[str, Any]) -> float | None:
        raw = position.get("raw") if isinstance(position, dict) else None
        if isinstance(raw, dict):
            for key in ("collateral", "positionBalance", "posMargin"):
                value = _safe_float(raw.get(key))
                if value is not None and value > 0:
                    return value
            info = raw.get("info")
            if isinstance(info, dict):
                for key in ("positionBalance", "posMargin", "positionIM", "margin"):
                    value = _safe_float(info.get(key))
                if value is not None and value > 0:
                    return value
        return self._position_margin_used(dict(position)) if isinstance(position, dict) else None

    def _position_value(self, position: Mapping[str, Any]) -> float | None:
        raw = position.get("raw") if isinstance(position, dict) else None
        if isinstance(raw, dict):
            value = _safe_float(raw.get("positionValue"))
            if value is not None and value != 0:
                return value
            info = raw.get("info")
            if isinstance(info, dict):
                value = _safe_float(info.get("positionValue"))
                if value is not None and value != 0:
                    return value
        notional = _safe_float(position.get("notional"))
        if notional is not None and notional != 0:
            return notional
        return None

    def _position_effective_leverage(self, position: Mapping[str, Any]) -> float | None:
        leverage = _safe_float(position.get("leverage"))
        if leverage is not None and leverage >= 0:
            return leverage
        raw = position.get("raw") if isinstance(position, dict) else None
        if isinstance(raw, dict):
            for key in ("realLeverage", "real_leverage", "effectiveLeverage", "leverage", "lever", "leverRate"):
                leverage = _safe_float(raw.get(key))
                if leverage is not None and leverage >= 0:
                    return leverage
            info = raw.get("info")
            if isinstance(info, dict):
                for key in ("realLeverage", "real_leverage", "effectiveLeverage", "leverage", "lever", "leverRate"):
                    leverage = _safe_float(info.get(key))
                    if leverage is not None and leverage >= 0:
                        return leverage
        position_value = self._position_value(position)
        base_margin = self._position_margin_base(position)
        if position_value is None or base_margin is None or base_margin <= 0:
            return None
        return abs(position_value) / base_margin

    def _position_side_hint(self, position: Mapping[str, Any]) -> str | None:
        side = str(position.get("side") or "").lower()
        if side in ("long", "short"):
            return side
        return None

    async def _resolve_margin_symbol(self, gateway: ExchangeGateway, position: Mapping[str, Any]) -> str | None:
        raw_symbol = (
            position.get("symbol")
            or position.get("exchange_symbol")
            or position.get("symbol_normalized")
        )
        if not raw_symbol:
            return None
        symbol = str(raw_symbol)
        client = gateway.client
        if client is None:
            return None
        markets = getattr(client, "markets", None) or {}
        if not markets:
            try:
                await client.load_markets()
            except Exception:  # pylint: disable=broad-except
                markets = getattr(client, "markets", None) or {}
        if symbol in markets:
            return symbol
        exchange_symbol = position.get("exchange_symbol") or gateway.map_symbol(symbol)
        markets_by_id = getattr(client, "markets_by_id", None) or {}
        if exchange_symbol and exchange_symbol in markets_by_id:
            return markets_by_id[exchange_symbol].get("symbol") or symbol
        return _ccxt_perp_symbol(symbol)

    def _margin_params_for_position(self, exchange: str, position: Mapping[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = {}
        side = self._position_side_hint(position)
        if exchange == "okx":
            raw = position.get("raw") or {}
            info = raw.get("info") if isinstance(raw, dict) else None
            pos_side = None
            if isinstance(info, dict):
                pos_side = info.get("posSide")
            if not pos_side:
                pos_side = raw.get("posSide") if isinstance(raw, dict) else None
            if not pos_side and side:
                pos_side = side
            if pos_side:
                params["posSide"] = pos_side
        if exchange == "bitget" and side:
            params["holdSide"] = side
        if exchange == "bingx":
            raw = position.get("raw") or {}
            info = raw.get("info") if isinstance(raw, dict) else None
            pos_id = None
            pos_side = None
            if isinstance(info, dict):
                pos_id = info.get("positionId") or info.get("position_id")
                pos_side = info.get("positionSide") or info.get("position_side")
            if not pos_id:
                pos_id = raw.get("positionId") or raw.get("position_id") or raw.get("id") or position.get("id")
            if not pos_side and side:
                pos_side = "LONG" if side == "long" else "SHORT"
            if pos_id:
                params["positionId"] = pos_id
            if pos_side:
                params["positionSide"] = str(pos_side).upper()
        if exchange == "mexc":
            raw = position.get("raw") or {}
            pos_id = raw.get("positionId") or raw.get("position_id") or raw.get("id") or position.get("id")
            if pos_id:
                params["positionId"] = pos_id
        return params

    async def _bybit_add_margin(
        self,
        client: Any,
        symbol: str,
        amount: float,
        position: Mapping[str, Any],
    ) -> Any:
        await client.load_markets()
        market = client.market(symbol)
        category = "linear"
        if market:
            if market.get("inverse"):
                category = "inverse"
        raw_amount = abs(float(amount))
        margin_value = client.amount_to_precision(symbol, raw_amount)
        if amount < 0:
            margin_value = "-" + str(margin_value)
        params: dict[str, Any] = {
            "category": category,
            "symbol": market.get("id") if market else symbol,
            "margin": margin_value,
        }
        raw = position.get("raw") or {}
        position_idx = raw.get("positionIdx") or raw.get("position_idx")
        if position_idx is None:
            info = raw.get("info") if isinstance(raw, dict) else None
            if isinstance(info, dict):
                position_idx = info.get("positionIdx") or info.get("position_idx")
        if position_idx is None:
            side = self._position_side_hint(position)
            if side == "long":
                position_idx = 1
            elif side == "short":
                position_idx = 2
            else:
                position_idx = 0
        params["positionIdx"] = int(position_idx)
        return await client.private_post_v5_position_add_margin(params)

    async def _kucoin_withdraw_margin(
        self,
        client: Any,
        symbol: str,
        amount: float,
    ) -> Any:
        await client.load_markets()
        market = client.market(symbol)
        precision_amount = client.amount_to_precision(symbol, amount)
        request: dict[str, Any] = {
            "symbol": market.get("id") if market else symbol,
            "withdrawAmount": precision_amount,
        }
        try:
            return await client.request(
                "margin/withdrawMargin",
                api="futuresPrivate",
                method="POST",
                params=request,
            )
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc).lower()
            if "not found" in message or "404" in message:
                legacy = {
                    "symbol": market.get("id") if market else symbol,
                    "margin": precision_amount,
                    "bizNo": client.uuid(),
                }
                return await client.request(
                    "position/margin/withdraw-margin",
                    api="futuresPrivate",
                    method="POST",
                    params=legacy,
                )
            raise

    def _gate_side_to_dual_side(self, value: Any) -> str | None:
        side = str(value or "").strip().lower()
        if side in {"dual_long", "long", "buy", "bid"}:
            return "dual_long"
        if side in {"dual_short", "short", "sell", "ask"}:
            return "dual_short"
        return None

    def _gate_dual_side_for_position(self, position: Mapping[str, Any]) -> str | None:
        raw = position.get("raw") or {}
        info = raw.get("info") if isinstance(raw, dict) else None
        candidates = [
            position.get("side"),
            position.get("dual_side"),
            raw.get("side") if isinstance(raw, dict) else None,
            raw.get("dual_side") if isinstance(raw, dict) else None,
            info.get("side") if isinstance(info, dict) else None,
            info.get("dual_side") if isinstance(info, dict) else None,
            raw.get("mode") if isinstance(raw, dict) else None,
            info.get("mode") if isinstance(info, dict) else None,
        ]
        for candidate in candidates:
            dual_side = self._gate_side_to_dual_side(candidate)
            if dual_side:
                return dual_side
        return None

    def _gate_is_dual_mode_position(self, position: Mapping[str, Any]) -> bool:
        raw = position.get("raw") or {}
        info = raw.get("info") if isinstance(raw, dict) else None
        mode_candidates = [
            position.get("mode"),
            position.get("position_mode"),
            raw.get("mode") if isinstance(raw, dict) else None,
            raw.get("position_mode") if isinstance(raw, dict) else None,
            info.get("mode") if isinstance(info, dict) else None,
            info.get("position_mode") if isinstance(info, dict) else None,
        ]
        for mode_value in mode_candidates:
            mode = str(mode_value or "").strip().lower()
            if not mode:
                continue
            if mode.startswith("dual"):
                return True
            if mode == "single":
                return False
        bool_candidates = [
            position.get("in_dual_mode"),
            position.get("dual_mode"),
            raw.get("in_dual_mode") if isinstance(raw, dict) else None,
            raw.get("dual_mode") if isinstance(raw, dict) else None,
            info.get("in_dual_mode") if isinstance(info, dict) else None,
            info.get("dual_mode") if isinstance(info, dict) else None,
        ]
        for value in bool_candidates:
            if isinstance(value, bool):
                return value
            if value is None:
                continue
            cleaned = str(value).strip().lower()
            if cleaned in {"true", "1"}:
                return True
            if cleaned in {"false", "0"}:
                return False
        return False

    async def _gate_prepare_margin_change(
        self,
        client: Any,
        symbol: str,
        signed_amount: float,
    ) -> tuple[float, Mapping[str, Any] | None]:
        digits = 8
        market: Mapping[str, Any] | None = None
        try:
            await client.load_markets()
            market = client.market(symbol)
            settle = None
            if isinstance(market, dict):
                settle = market.get("settle") or market.get("settleId")
            currencies = getattr(client, "currencies", None) or {}
            if settle:
                settle_key = str(settle)
                currency = (
                    currencies.get(settle_key)
                    or currencies.get(settle_key.upper())
                    or currencies.get(settle_key.lower())
                )
                precision = currency.get("precision") if isinstance(currency, dict) else None
                if isinstance(precision, (int, float)) and precision >= 0:
                    digits = max(0, min(12, int(precision)))
        except Exception:  # pylint: disable=broad-except
            market = None
        normalized = round(float(signed_amount), digits)
        if normalized == 0 and signed_amount != 0:
            normalized = float(signed_amount)
        return normalized, market

    def _gate_is_margin_protocol_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        if "invalid_protocol" in message and ("invalid argument" in message or "#3" in message):
            return True
        if "missing_required_param" in message and "dual_side" in message:
            return True
        return False

    def _is_add_margin_limit_error(self, exchange: str, exc: Exception) -> bool:
        if exchange != "okx":
            return False
        message = str(exc).lower()
        if "59301" in message:
            return True
        if "margin adjustment failed" in message and "maximum limit" in message:
            return True
        if "exceeds the maximum limit" in message:
            return True
        return False

    async def _gate_update_margin_dual(
        self,
        client: Any,
        symbol: str,
        position: Mapping[str, Any],
        signed_change: float,
        params: Mapping[str, Any],
        market_hint: Mapping[str, Any] | None = None,
    ) -> Any:
        if not hasattr(client, "privateFuturesPostSettleDualCompPositionsContractMargin"):
            raise RuntimeError("gate_dual_margin_unsupported")
        market = market_hint
        if market is None:
            await client.load_markets()
            market = client.market(symbol)
        contract = None
        settle = "usdt"
        if isinstance(market, dict):
            contract = market.get("id")
            settle_value = market.get("settle") or market.get("settleId")
            if settle_value:
                settle = str(settle_value).lower()
        if not contract:
            contract = position.get("exchange_symbol") or position.get("symbol")
        if not contract:
            raise RuntimeError("gate_contract_unavailable")
        dual_side = self._gate_dual_side_for_position(position)
        if not dual_side:
            raise RuntimeError("gate_dual_side_unavailable")
        change_value = (
            client.number_to_string(signed_change)
            if hasattr(client, "number_to_string")
            else str(signed_change)
        )
        request: dict[str, Any] = {
            "settle": settle,
            "contract": contract,
            "change": change_value,
            "dual_side": dual_side,
        }
        if isinstance(params, Mapping):
            request.update(params)
        return await client.privateFuturesPostSettleDualCompPositionsContractMargin(request)

    async def _gate_modify_margin(
        self,
        client: Any,
        symbol: str,
        position: Mapping[str, Any],
        signed_amount: float,
        params: Mapping[str, Any],
    ) -> Any:
        normalized_change, market = await self._gate_prepare_margin_change(client, symbol, signed_amount)
        if self._gate_is_dual_mode_position(position):
            return await self._gate_update_margin_dual(
                client=client,
                symbol=symbol,
                position=position,
                signed_change=normalized_change,
                params=params,
                market_hint=market,
            )
        try:
            if normalized_change >= 0:
                if not hasattr(client, "add_margin"):
                    raise RuntimeError("add_margin_unsupported")
                return await client.add_margin(symbol, normalized_change, dict(params))
            if not hasattr(client, "reduce_margin"):
                raise RuntimeError("reduce_margin_unsupported")
            return await client.reduce_margin(symbol, abs(normalized_change), dict(params))
        except Exception as exc:  # pylint: disable=broad-except
            if self._gate_is_margin_protocol_error(exc) and self._gate_dual_side_for_position(position):
                return await self._gate_update_margin_dual(
                    client=client,
                    symbol=symbol,
                    position=position,
                    signed_change=normalized_change,
                    params=params,
                    market_hint=market,
                )
            raise

    async def _modify_margin(
        self,
        *,
        exchange: str,
        position: Mapping[str, Any],
        amount: float,
        action: str,
    ) -> dict[str, Any]:
        gateway = self._gateways.get(exchange)
        if gateway is None:
            return {"status": "error", "error": "gateway_unavailable"}
        try:
            await gateway.refresh_credentials_async(force_env=True)
            await gateway.ensure_client()
        except Exception as exc:  # pylint: disable=broad-except
            return {"status": "error", "error": f"client_init_failed: {exc}"}
        client = gateway.client
        if client is None:
            return {"status": "error", "error": "client_unavailable"}
        symbol = await self._resolve_margin_symbol(gateway, position)
        if not symbol:
            return {"status": "error", "error": "symbol_unavailable"}
        params = self._margin_params_for_position(exchange, position)

        async def _add_margin(amount_value: float) -> Any:
            if exchange == "bybit" and hasattr(client, "private_post_v5_position_add_margin"):
                return await self._bybit_add_margin(client, symbol, amount_value, position)
            if exchange == "gate":
                return await self._gate_modify_margin(
                    client=client,
                    symbol=symbol,
                    position=position,
                    signed_amount=amount_value,
                    params=params,
                )
            if not hasattr(client, "add_margin"):
                raise RuntimeError("add_margin_unsupported")
            return await client.add_margin(symbol, amount_value, params)

        async def _reduce_margin(amount_value: float) -> Any:
            if exchange == "bybit" and hasattr(client, "private_post_v5_position_add_margin"):
                return await self._bybit_add_margin(client, symbol, -abs(amount_value), position)
            if exchange == "gate":
                return await self._gate_modify_margin(
                    client=client,
                    symbol=symbol,
                    position=position,
                    signed_amount=-abs(amount_value),
                    params=params,
                )
            if exchange == "kucoin":
                return await self._kucoin_withdraw_margin(client, symbol, amount_value)
            if not hasattr(client, "reduce_margin"):
                raise RuntimeError("reduce_margin_unsupported")
            reduce_amount = -amount_value if exchange == "bitget" else amount_value
            return await client.reduce_margin(symbol, reduce_amount, params)

        try:
            if action == "add":
                try:
                    result = await _add_margin(amount)
                    return {"status": "ok", "result": result, "amount": amount}
                except Exception as exc:  # pylint: disable=broad-except
                    message = str(exc)
                    if "add_margin_unsupported" in message:
                        return {"status": "error", "error": "add_margin_unsupported"}
                    if not self._is_add_margin_limit_error(exchange, exc):
                        return {"status": "error", "error": message}
                    requested = amount
                    attempts: list[float] = []
                    for shrink in (0.9, 0.8, 0.7, 0.6, 0.5):
                        candidate = requested * shrink
                        if candidate <= 0:
                            break
                        attempts.append(candidate)
                        try:
                            result = await _add_margin(candidate)
                            return {
                                "status": "ok",
                                "result": result,
                                "amount": candidate,
                                "requested_amount": requested,
                                "retry_amounts": attempts,
                            }
                        except Exception as retry_exc:  # pylint: disable=broad-except
                            message = str(retry_exc)
                            if not self._is_add_margin_limit_error(exchange, retry_exc):
                                break
                    return {"status": "error", "error": message}
            else:
                try:
                    result = await _reduce_margin(amount)
                    return {"status": "ok", "result": result, "amount": amount}
                except Exception as exc:  # pylint: disable=broad-except
                    message = str(exc)
                    if "reduce_margin_unsupported" in message:
                        return {"status": "error", "error": "reduce_margin_unsupported"}
                    requested = amount
                    candidate = amount
                    attempts: list[float] = []
                    for shrink in (0.9, 0.95):
                        candidate *= shrink
                        if candidate <= 0:
                            break
                        attempts.append(candidate)
                        try:
                            result = await _reduce_margin(candidate)
                            return {
                                "status": "ok",
                                "result": result,
                                "amount": candidate,
                                "requested_amount": requested,
                                "retry_amounts": attempts,
                            }
                        except Exception as retry_exc:  # pylint: disable=broad-except
                            message = str(retry_exc)
                    return {"status": "error", "error": message}
            return {"status": "ok", "result": result, "amount": amount}
        except Exception as exc:  # pylint: disable=broad-except
            return {"status": "error", "error": str(exc)}

    async def _maybe_enforce_margin_settings(self, positions: list[dict[str, Any]]) -> None:
        if not self._enforce_isolated_margin and not self._enforce_leverage:
            return
        for position in positions or []:
            exchange = str(position.get("exchange") or "").lower()
            if not exchange:
                continue
            qty = _safe_float(position.get("coin_qty")) or _safe_float(position.get("contracts"))
            if qty is None or qty == 0:
                continue
            symbol = normalize_symbol(position.get("symbol") or position.get("symbol_normalized"))
            if not symbol:
                continue
            side = self._position_side_hint(position) or "unknown"
            margin_mode = str(position.get("margin_mode") or "").lower() or None
            leverage = self._position_effective_leverage(position)
            if self._enforce_isolated_margin and margin_mode != "isolated":
                if exchange == "binance":
                    # Binance does not allow margin type changes while a position exists.
                    logger.info("%s %s %s margin mode enforce skipped (position open)", exchange, symbol, side)
                else:
                    key = (exchange, symbol, side, "mode")
                    if self._margin_enforce_ready(key):
                        result = await self._set_margin_mode(exchange, position, "isolated")
                        if result.get("status") == "ok":
                            logger.info("%s %s %s margin mode set to isolated", exchange, symbol, side)
                            margin_mode = "isolated"
                        else:
                            logger.warning(
                                "%s %s %s margin mode enforce failed: %s",
                                exchange,
                                symbol,
                                side,
                                result.get("error") or result,
                            )
            if self._enforce_leverage:
                target = self._target_leverage
                if target and (leverage is None or abs(leverage - target) > 0.05):
                    if exchange == "binance":
                        logger.info("%s %s %s leverage enforce skipped (position open)", exchange, symbol, side)
                        continue
                    key = (exchange, symbol, side, "leverage")
                    if self._margin_enforce_ready(key):
                        effective_mode = margin_mode or "isolated"
                        if exchange == "kucoin" and effective_mode != "cross":
                            result = await self._kucoin_adjust_margin_for_leverage(position, target)
                        else:
                            result = await self._set_leverage(exchange, position, effective_mode, target)
                        if result.get("status") == "ok":
                            if result.get("action"):
                                logger.info(
                                    "%s %s %s leverage target %s via %s margin",
                                    exchange,
                                    symbol,
                                    side,
                                    target,
                                    result.get("action"),
                                )
                                if exchange == "kucoin" and result.get("action") == "add":
                                    self._last_margin_adjust[(exchange, symbol, side, "add")] = time.time()
                            else:
                                logger.info(
                                    "%s %s %s leverage set to %s",
                                    exchange,
                                    symbol,
                                    side,
                                    target,
                                )
                        else:
                            logger.warning(
                                "%s %s %s leverage enforce failed: %s",
                                exchange,
                                symbol,
                                side,
                                result.get("error") or result,
                            )

    def _margin_enforce_ready(self, key: tuple[str, str, str, str]) -> bool:
        now = time.time()
        last = self._last_margin_adjust.get(key, 0.0)
        if (now - last) < self._margin_adjust_cooldown:
            return False
        self._last_margin_adjust[key] = now
        return True

    async def _set_margin_mode(
        self,
        exchange: str,
        position: Mapping[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        gateway = self._gateways.get(exchange)
        if gateway is None:
            return {"status": "error", "error": "gateway_unavailable"}
        try:
            await gateway.refresh_credentials_async(force_env=True)
            await gateway.ensure_client()
        except Exception as exc:  # pylint: disable=broad-except
            return {"status": "error", "error": f"client_init_failed: {exc}"}
        client = gateway.client
        if client is None:
            return {"status": "error", "error": "client_unavailable"}
        if not hasattr(client, "set_margin_mode"):
            return {"status": "error", "error": "set_margin_mode_unsupported"}
        symbol = await self._resolve_margin_symbol(gateway, position)
        if not symbol:
            return {"status": "error", "error": "symbol_unavailable"}
        params = self._margin_params_for_position(exchange, position)
        if exchange == "okx":
            params = dict(params)
            params["lever"] = int(self._target_leverage)
        if exchange == "kucoin":
            mode = str(mode).strip().upper()
        try:
            if params:
                await client.set_margin_mode(mode, symbol, params)
            else:
                await client.set_margin_mode(mode, symbol)
            return {"status": "ok"}
        except Exception as exc:  # pylint: disable=broad-except
            return {"status": "error", "error": str(exc)}

    async def _set_leverage(
        self,
        exchange: str,
        position: Mapping[str, Any],
        margin_mode: str,
        leverage: float,
    ) -> dict[str, Any]:
        if leverage is None or not math.isfinite(leverage) or leverage <= 0:
            return {"status": "error", "error": "invalid_leverage"}
        gateway = self._gateways.get(exchange)
        if gateway is None:
            return {"status": "error", "error": "gateway_unavailable"}
        try:
            await gateway.refresh_credentials_async(force_env=True)
            await gateway.ensure_client()
        except Exception as exc:  # pylint: disable=broad-except
            return {"status": "error", "error": f"client_init_failed: {exc}"}
        client = gateway.client
        if client is None:
            return {"status": "error", "error": "client_unavailable"}
        if not hasattr(client, "set_leverage"):
            return {"status": "error", "error": "set_leverage_unsupported"}
        symbol = await self._resolve_margin_symbol(gateway, position)
        if not symbol:
            return {"status": "error", "error": "symbol_unavailable"}
        params = self._leverage_params_for_position(exchange, position, margin_mode)
        if exchange == "binance":
            params = dict(params)
            params["leverage"] = int(round(leverage))
        try:
            await client.set_leverage(leverage, symbol, params or None)
            return {"status": "ok"}
        except Exception as exc:  # pylint: disable=broad-except
            if exchange == "bingx":
                message = str(exc).lower()
                if "109400" in message or "invalid parameters" in message or "invalid parameter" in message:
                    fallback_params = dict(params or {})
                    fallback_params["side"] = "BOTH"
                    try:
                        await client.set_leverage(leverage, symbol, fallback_params)
                        return {
                            "status": "ok",
                            "fallback": {
                                "side": fallback_params.get("side"),
                                "error": str(exc),
                            },
                        }
                    except Exception as fallback_exc:  # pylint: disable=broad-except
                        return {"status": "error", "error": str(fallback_exc)}
            return {"status": "error", "error": str(exc)}

    async def _kucoin_adjust_margin_for_leverage(
        self,
        position: Mapping[str, Any],
        target_leverage: float,
    ) -> dict[str, Any]:
        if target_leverage <= 0:
            return {"status": "error", "error": "invalid_target_leverage"}
        position_value = self._position_value(position)
        if position_value is None or position_value == 0:
            return {"status": "error", "error": "position_value_unavailable"}
        base_margin = self._position_margin_base(position)
        if base_margin is None or base_margin <= 0:
            return {"status": "error", "error": "base_margin_unavailable"}
        current_leverage = self._position_effective_leverage(position)
        if self._kucoin_isolated_topup_only and current_leverage is not None and current_leverage <= (
            float(target_leverage) + KUCOIN_LEVERAGE_TARGET_TOLERANCE
        ):
            return {
                "status": "ok",
                "action": None,
                "amount": 0.0,
                "target_margin": abs(position_value) / target_leverage,
                "base_margin": base_margin,
                "target_leverage": target_leverage,
                "current_leverage": current_leverage,
                "reason": "topup_only_below_target",
            }
        target_margin = abs(position_value) / target_leverage * (1.0 + KUCOIN_LEVERAGE_MARGIN_BUFFER_PCT)
        delta = target_margin - base_margin
        if delta <= 0 or abs(delta) < KUCOIN_LEVERAGE_MARGIN_MIN_DELTA:
            if not self._kucoin_isolated_topup_only and delta < -KUCOIN_LEVERAGE_MARGIN_MIN_DELTA:
                action = "reduce"
                amount = abs(delta)
                result = await self._modify_margin(
                    exchange="kucoin",
                    position=position,
                    amount=amount,
                    action=action,
                )
                result.update(
                    {
                        "action": action,
                        "amount": amount,
                        "target_margin": target_margin,
                        "base_margin": base_margin,
                        "target_leverage": target_leverage,
                        "current_leverage": current_leverage,
                        "buffer_pct": KUCOIN_LEVERAGE_MARGIN_BUFFER_PCT,
                    }
                )
                return result
            return {
                "status": "ok",
                "action": None,
                "amount": 0.0,
                "target_margin": target_margin,
                "base_margin": base_margin,
                "target_leverage": target_leverage,
                "current_leverage": current_leverage,
                "reason": "topup_only_amount_too_small",
            }
        action = "add"
        amount = abs(delta)
        result = await self._modify_margin(
            exchange="kucoin",
            position=position,
            amount=amount,
            action=action,
        )
        result.update(
            {
                "action": action,
                "amount": amount,
                "target_margin": target_margin,
                "base_margin": base_margin,
                "target_leverage": target_leverage,
                "current_leverage": current_leverage,
                "buffer_pct": KUCOIN_LEVERAGE_MARGIN_BUFFER_PCT,
            }
        )
        return result

    def _leverage_params_for_position(
        self,
        exchange: str,
        position: Mapping[str, Any],
        margin_mode: str,
    ) -> dict[str, Any]:
        params = self._margin_params_for_position(exchange, position)
        mode = str(margin_mode or "").lower()
        if mode in ("isolated", "cross"):
            if exchange == "okx":
                params["tdMode"] = mode
            else:
                params["marginMode"] = mode
        if exchange == "bingx":
            raw = position.get("raw") or {}
            info = raw.get("info") if isinstance(raw, dict) else None
            pos_side = None
            if isinstance(info, dict):
                pos_side = (
                    info.get("positionSide")
                    or info.get("position_side")
                    or info.get("posSide")
                    or info.get("ps")
                )
            if not pos_side and isinstance(raw, dict):
                pos_side = (
                    raw.get("positionSide")
                    or raw.get("position_side")
                    or raw.get("posSide")
                    or raw.get("ps")
                )
            if pos_side:
                normalized = str(pos_side).upper()
                if normalized in ("LONG", "SHORT", "BOTH"):
                    params["side"] = normalized
            if "side" not in params:
                side = self._position_side_hint(position)
                if side:
                    params["side"] = "LONG" if side == "long" else "SHORT"
        if exchange == "bybit":
            raw = position.get("raw") or {}
            pos_idx = raw.get("positionIdx") or raw.get("position_idx")
            if pos_idx is None:
                side = self._position_side_hint(position)
                if side == "long":
                    pos_idx = 1
                elif side == "short":
                    pos_idx = 2
                else:
                    pos_idx = 0
            params["positionIdx"] = int(pos_idx)
        return params

    async def _maybe_adjust_isolated_margin(
        self,
        *,
        exchange: str,
        position: Mapping[str, Any],
        balance_entry: Mapping[str, Any],
        buffer_pct: float,
    ) -> dict[str, Any]:
        if not self._auto_margin_enabled:
            return {"status": "disabled"}
        add_trigger = max(0.0, float(self._margin_add_trigger_buffer_pct))
        target_buffer = max(0.0, float(self._target_buffer_pct))
        if target_buffer <= 0:
            return {"status": "skip", "reason": "target_buffer_invalid"}
        if buffer_pct >= add_trigger:
            return {"status": "no_action", "reason": "buffer_above_add_trigger"}
        if buffer_pct >= target_buffer:
            return {"status": "no_action", "reason": "buffer_at_target"}
        symbol = normalize_symbol(position.get("symbol") or position.get("symbol_normalized"))
        side = self._position_side_hint(position) or "unknown"
        key = (exchange, symbol, side, "add")
        now = time.time()
        last = self._last_margin_adjust.get(key, 0.0)
        if (now - last) < self._margin_adjust_cooldown:
            return {"status": "cooldown"}
        margin_used = self._position_margin_used(dict(position))
        if margin_used is None or margin_used <= 0:
            return {"status": "skip", "reason": "margin_used_missing"}
        available = _safe_float(balance_entry.get("available"))
        if available is None or available <= 0:
            return {
                "status": "no_funds",
                "available": available,
            }
        if buffer_pct <= 0:
            return {"status": "skip", "reason": "buffer_invalid"}
        required_add = margin_used * (target_buffer / buffer_pct - 1.0)
        add_amt = min(available, max(0.0, required_add))
        if add_amt <= 0:
            return {"status": "skip", "reason": "amount_too_small"}
        result = await self._modify_margin(
            exchange=exchange,
            position=position,
            amount=add_amt,
            action="add",
        )
        if result.get("status") == "ok":
            self._last_margin_adjust[key] = now
        result.update(
            {
                "action": "add",
                "buffer_pct": buffer_pct,
                "add_trigger_buffer_pct": add_trigger,
                "target_buffer_pct": target_buffer,
                "desired_add": required_add,
                "available": available,
            }
        )
        result.setdefault("amount", add_amt)
        result.setdefault("requested_amount", add_amt)
        return result

    async def _maybe_reduce_isolated_margin(
        self,
        *,
        exchange: str,
        position: Mapping[str, Any],
        buffer_pct: float,
    ) -> dict[str, Any]:
        if not self._auto_margin_reduce_enabled:
            return {"status": "disabled"}
        margin_mode = str(position.get("margin_mode") or "").strip().lower()
        if exchange == "kucoin" and margin_mode != "cross" and self._kucoin_isolated_topup_only:
            return {"status": "skip", "reason": "kucoin_topup_only"}
        reduce_trigger = max(0.0, float(self._margin_reduce_trigger_buffer_pct))
        target_buffer = max(0.0, float(self._target_buffer_pct))
        if target_buffer <= 0:
            return {"status": "skip", "reason": "target_buffer_invalid"}
        if buffer_pct <= reduce_trigger:
            return {"status": "no_action", "reason": "buffer_below_reduce_trigger"}
        if buffer_pct <= target_buffer:
            return {"status": "no_action", "reason": "buffer_at_target"}
        symbol = normalize_symbol(position.get("symbol") or position.get("symbol_normalized"))
        side = self._position_side_hint(position) or "unknown"
        key = (exchange, symbol, side, "reduce")
        now = time.time()
        last = self._last_margin_adjust.get(key, 0.0)
        if (now - last) < self._margin_adjust_cooldown:
            return {"status": "cooldown"}
        margin_used = self._position_margin_used(dict(position))
        if margin_used is None or margin_used <= 0:
            return {"status": "skip", "reason": "margin_used_missing"}
        reduce_amt = margin_used * (1.0 - target_buffer / buffer_pct)
        if reduce_amt <= 0:
            return {"status": "skip", "reason": "amount_too_small"}
        result = await self._modify_margin(
            exchange=exchange,
            position=position,
            amount=reduce_amt,
            action="reduce",
        )
        if result.get("status") == "ok":
            self._last_margin_adjust[key] = now
        result.update(
            {
                "action": "reduce",
                "amount": reduce_amt,
                "requested_amount": reduce_amt,
                "buffer_pct": buffer_pct,
                "reduce_trigger_buffer_pct": reduce_trigger,
                "target_buffer_pct": target_buffer,
            }
        )
        return result

    async def _send_notification_text_status(self, text: str, *, title: str | None = None) -> str:
        return await self._notifier.send_text_status(text, title=title)

    async def _send_notification_text(self, text: str, *, title: str | None = None) -> bool:
        return await self._notifier.send_text(text, title=title)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_margin_mode(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if not cleaned:
            return None
        if cleaned.isdigit():
            try:
                as_int = int(cleaned)
            except ValueError:
                as_int = None
            if as_int == 0:
                return "cross"
            if as_int == 1:
                return "isolated"
        if cleaned in {"cross", "crossed", "cross_margin", "crossed_margin", "regular_margin", "regular"}:
            return "cross"
        if cleaned in {"isolated", "isol", "isolated_margin", "fixed"}:
            return "isolated"
        if cleaned in {"portfolio", "portfolio_margin"}:
            return "portfolio"
        return None
    if isinstance(value, (int, float)):
        if value == 0:
            return "cross"
        if value == 1:
            return "isolated"
    return None


def _extract_margin_mode(payload: dict[str, Any], slug: str) -> tuple[str | None, str | None]:
    for key in ("marginMode", "margin_mode", "marginType", "margin_type"):
        mode = _normalize_margin_mode(payload.get(key))
        if mode:
            return mode, f"payload.{key}"
    info = payload.get("info")
    if isinstance(info, dict):
        for key in ("marginMode", "margin_mode", "marginType", "margin_type", "mgnMode"):
            mode = _normalize_margin_mode(info.get(key))
            if mode:
                return mode, f"info.{key}"
    if slug == "bybit" and isinstance(info, dict):
        trade_mode = _safe_float(info.get("tradeMode"))
        if trade_mode is not None:
            position_balance = _safe_float(info.get("positionBalance")) or _safe_float(
                payload.get("positionBalance")
            )
            position_im = _safe_float(info.get("positionIM")) or _safe_float(payload.get("positionIM"))
            if trade_mode == 1:
                return "isolated", "info.tradeMode"
            if trade_mode == 0:
                if position_balance is not None and position_im is not None and position_balance > 0:
                    diff = abs(position_balance - position_im)
                    if diff / position_balance <= 0.05:
                        return "isolated", "positionBalance_hint"
                return "cross", "info.tradeMode"
    trade_mode = payload.get("tradeMode")
    if slug != "bybit":
        mode = _normalize_margin_mode(trade_mode)
        if mode:
            return mode, "payload.tradeMode"
    if slug == "gate":
        leverage = _safe_float(payload.get("leverage"))
        if leverage is not None:
            return ("cross" if leverage == 0 else "isolated"), "leverage_fallback"
    return None, None


def _extract_leverage(payload: dict[str, Any]) -> tuple[float | None, str | None]:
    for key in ("realLeverage", "real_leverage", "effectiveLeverage"):
        leverage = _safe_float(payload.get(key))
        if leverage is not None:
            return leverage, f"payload.{key}"
    leverage = _safe_float(payload.get("leverage"))
    if leverage is not None:
        return leverage, "payload.leverage"
    info = payload.get("info")
    if isinstance(info, dict):
        for key in ("realLeverage", "real_leverage", "effectiveLeverage", "leverage", "lever", "leverRate"):
            leverage = _safe_float(info.get(key))
            if leverage is not None:
                return leverage, f"info.{key}"
    return None, None


def _ts_to_iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, str) and value.isdigit():
            value = int(value)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / (1000 if value > 10**12 else 1), tz=timezone.utc).isoformat()
        return datetime.fromisoformat(str(value)).astimezone(timezone.utc).isoformat()
    except Exception:  # pylint: disable=broad-except
        return None
