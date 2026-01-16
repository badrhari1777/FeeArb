from __future__ import annotations

import asyncio
import logging
import os
import time
import atexit
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

import aiohttp
from config import BASE_DIR

try:
    import ccxt.async_support as ccxt_async  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    ccxt_async = None

logger = logging.getLogger(__name__)
_LAST_ENV_MTIME: float | None = None


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


def _ccxt_perp_symbol(symbol: str | None) -> str:
    """Best-effort CCXT perp notation (e.g. BTCUSDT -> BTC/USDT:USDT)."""
    normalized = normalize_symbol(symbol)
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
            if self.slug in {"mexc", "bingx"}:
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

    def map_symbol(self, symbol: str) -> str:
        """Map canonical symbol to exchange-specific if supported by ccxt."""
        if self.client and hasattr(self.client, "market_id"):  # type: ignore[truthy-bool]
            try:
                return self.client.market_id(symbol)  # type: ignore[union-attr]
            except Exception:
                return symbol
        return symbol

    async def fetch_balance(self) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        params = dict(self.spec.balance_params)
        balance = await self.client.fetch_balance(params=params)
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
        params = dict(self.spec.position_params)
        try:
            positions = await self.client.fetch_positions(params=params)  # type: ignore[attr-defined]
        except AttributeError:
            positions = []
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
            entry_px = _safe_float(payload.get("entryPrice"))
            if notional is None and entry_px is not None and contracts:
                # For venues with contract sizes != 1 (e.g., MEXC), include contract_size to avoid under-reporting notional.
                notional = contracts * (contract_size or 1.0) * entry_px
            leverage, leverage_source = _extract_leverage(payload)
            liq_price = _safe_float(payload.get("liquidationPrice"))
            if liq_price is None:
                info = payload.get("info") or {}
                liq_price = _safe_float(info.get("liquidationPrice"))
            margin_mode, margin_mode_source = _extract_margin_mode(payload, self.slug)
            info = payload.get("info") or {}
            initial_margin = _safe_float(
                payload.get("initialMargin")
                or payload.get("positionInitialMargin")
                or payload.get("positionIM")
            )
            maintenance_margin = _safe_float(
                payload.get("maintenanceMargin")
                or payload.get("positionMaintenanceMargin")
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
                    or info.get("positionMaintenanceMargin")
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
                    "entry_price": _safe_float(payload.get("entryPrice")),
                    "mark_price": _safe_float(payload.get("markPrice")),
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

    def __init__(self, refresh_interval: int = 120, summary_interval: int = 1800) -> None:
        self._interval = max(30, refresh_interval)
        self._summary_interval = max(30, summary_interval)
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}
        self._lock = asyncio.Lock()
        self._balances: list[dict[str, Any]] = []
        self._positions: list[dict[str, Any]] = []
        self._status: list[dict[str, Any]] = []
        self._last_updated: str | None = None
        self._task: asyncio.Task | None = None
        self._next_summary_at: float = 0.0
        self._alert_cooldown = 600  # seconds
        self._active_alerts: Set[tuple[str, str]] = set()
        self._last_alert_sent: dict[tuple[str, str], float] = {}
        self._alert_lock = asyncio.Lock()
        self._telegram_warned = False
        self._margin_alerts_enabled = True
        self._warning_buffer_pct = 0.20
        self._panic_buffer_pct = 0.15
        self._min_free_balance_abs = 500.0
        self._min_free_balance_rel = 0.10
        self._target_buffer_pct = 0.25
        self._auto_margin_enabled = True
        self._auto_margin_reduce_enabled = True
        self._enforce_isolated_margin = True
        self._enforce_leverage = True
        self._target_leverage = 3.0
        self._margin_add_pct = 0.10
        self._margin_add_panic_pct = 0.20
        self._margin_reduce_pct = 0.10
        self._margin_adjust_cooldown = 300
        self._last_margin_adjust: dict[tuple[str, str, str, str], float] = {}
        self._missing_stop_alerts_enabled = True
        self._active_position_alerts: Set[tuple[str, str, str]] = set()
        self._last_position_alert_sent: dict[tuple[str, str, str], float] = {}
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

    def update_interval(self, seconds: int) -> None:
        self._interval = max(30, int(seconds))

    def update_summary_interval(self, seconds: int) -> None:
        self._summary_interval = max(30, int(seconds))
        # Force next send to honour new cadence
        self._next_summary_at = 0.0

    def update_alert_settings(
        self,
        *,
        send_margin_alerts: bool | None = None,
        send_missing_stop_alerts: bool | None = None,
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
        margin_add_pct: float | None = None,
        margin_add_panic_pct: float | None = None,
        margin_reduce_pct: float | None = None,
        margin_adjust_cooldown_sec: int | None = None,
    ) -> None:
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
            self._target_buffer_pct = max(0.0, float(target_buffer_pct))
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
        if margin_add_pct is not None:
            self._margin_add_pct = max(0.0, float(margin_add_pct))
        if margin_add_panic_pct is not None:
            self._margin_add_panic_pct = max(0.0, float(margin_add_panic_pct))
        if margin_reduce_pct is not None:
            self._margin_reduce_pct = max(0.0, float(margin_reduce_pct))
        if margin_adjust_cooldown_sec is not None:
            self._margin_adjust_cooldown = max(0, int(margin_adjust_cooldown_sec))

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

    async def _refresh(self, *, force_env: bool = False) -> None:
        balances, positions, status, refreshed = await self._collect_all(
            force_env=force_env
        )
        await self._maybe_enforce_margin_settings(positions)
        await self._maybe_send_alerts(balances, positions)
        await self._maybe_send_summary(balances, positions, refreshed)
        async with self._lock:
            self._balances = balances
            self._positions = positions
            self._status = status
            if refreshed:
                self._last_updated = refreshed

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
        """Send a periodic balance digest via Telegram."""
        now = time.time()
        if now < self._next_summary_at:
            return
        warnings = await self._mexc_stop_warnings(positions) if self._missing_stop_alerts_enabled else []
        text = self._build_balance_summary(balances, refreshed_at, warnings)
        if await self._send_telegram_text(text):
            self._next_summary_at = now + self._summary_interval

    def _build_balance_summary(
        self,
        balances: list[dict[str, Any]],
        refreshed_at: str | None,
        warnings: list[str] | None = None,
    ) -> str:
        tz = timezone(timedelta(hours=3))
        timestamp = "unknown"
        time_only = "unknown"
        if refreshed_at:
            try:
                dt = datetime.fromisoformat(refreshed_at)
                dt = dt.astimezone(tz)
                timestamp = dt.strftime("%Y-%m-%d %H:%M:%S GMT+3")
                time_only = dt.strftime("%H:%M")
            except Exception:  # pylint: disable=broad-except
                timestamp = refreshed_at
                time_only = refreshed_at
        header = f"Balance summary {time_only}"
        by_exchange: dict[str, dict[str, Any]] = {}
        for entry in balances:
            slug = str(entry.get("exchange") or "").lower()
            if slug and slug not in by_exchange:
                by_exchange[slug] = entry

        lines = [header]
        missing: list[str] = []
        for spec in EXCHANGE_SPECS:
            slug = spec.slug
            entry = by_exchange.get(slug)
            if not entry:
                missing.append(slug)
                continue
            asset = entry.get("asset") or spec.settle_currency
            total = int(round(_safe_float(entry.get("total"), default=0.0) or 0.0))
            available = int(round(_safe_float(entry.get("available"), default=0.0) or 0.0))
            lines.append(f"{slug}: {available} / {total} {asset}")

        if missing:
            lines.append("Missing data: " + ", ".join(missing))
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
    ) -> None:
        """Evaluate balances and positions for Telegram alerts."""
        if not balances or not self._margin_alerts_enabled:
            return
        alerts: list[tuple[tuple[str, str], dict[str, Any]]] = []
        position_alerts: list[tuple[tuple[str, str, str], dict[str, Any]]] = []
        resolved_keys: list[tuple[str, str]] = []
        resolved_positions: list[tuple[str, str, str]] = []
        now = time.time()

        positions_by_exchange: dict[str, list[dict[str, Any]]] = {}
        for pos in positions or []:
            exchange = str(pos.get("exchange") or "").lower()
            if not exchange:
                continue
            positions_by_exchange.setdefault(exchange, []).append(pos)

        for entry in balances:
            exchange = str(entry.get("exchange") or "").lower()
            asset = str(entry.get("asset") or "").upper()
            if not exchange or not asset:
                continue
            key = (exchange, asset)
            exchange_positions = positions_by_exchange.get(exchange, [])
            isolated_positions = [
                pos for pos in exchange_positions if str(pos.get("margin_mode") or "").lower() == "isolated"
            ]
            cross_positions = [
                pos for pos in exchange_positions if str(pos.get("margin_mode") or "").lower() != "isolated"
            ]
            has_isolated = bool(isolated_positions)
            has_cross = bool(cross_positions)

            if has_isolated and not has_cross:
                if key in self._active_alerts:
                    resolved_keys.append(key)
            else:
                total = _safe_float(entry.get("total"), default=0.0) or 0.0
                available = _safe_float(entry.get("available"))
                used = _safe_float(entry.get("used"), default=0.0) or 0.0
                margin_ratio = _safe_float(entry.get("margin_ratio"))
                if used <= 0:
                    if key in self._active_alerts:
                        resolved_keys.append(key)
                else:
                    risk_triggered = False
                    if available is not None:
                        pct = (available / total) if total > 0 else None
                        if (pct is not None and pct < 0.15) or available < 500:
                            risk_triggered = True
                    if margin_ratio is not None and margin_ratio > 0.8:
                        risk_triggered = True

                    if risk_triggered:
                        alerts.append(
                            (
                                key,
                                {
                                    "exchange": exchange,
                                    "asset": asset,
                                    "entry": entry,
                                    "total": total,
                                    "available": available,
                                    "used": used,
                                    "margin_ratio": margin_ratio,
                                },
                            )
                        )
                    else:
                        if available is not None and total > 0:
                            if available > max(0.2 * total, 700):
                                resolved_keys.append(key)
                        elif available is not None and available > 700:
                            resolved_keys.append(key)

            if has_isolated:
                for pos in isolated_positions:
                    symbol = normalize_symbol(pos.get("symbol") or pos.get("symbol_normalized"))
                    if not symbol:
                        continue
                    side = str(pos.get("side") or "").lower() or "unknown"
                    key = (exchange, symbol, side)
                    buffer_pct = self._position_liq_buffer_pct(pos)
                    if buffer_pct is None:
                        continue
                    severity = None
                    if buffer_pct <= self._panic_buffer_pct:
                        severity = "panic"
                    elif buffer_pct <= self._warning_buffer_pct:
                        severity = "warning"
                    if severity:
                        margin_action = await self._maybe_adjust_isolated_margin(
                            exchange=exchange,
                            position=pos,
                            balance_entry=entry,
                            buffer_pct=buffer_pct,
                            severity=severity,
                        )
                        if margin_action.get("status") == "ok":
                            logger.info(
                                "%s %s %s margin top-up ok: +%s",
                                exchange,
                                symbol,
                                side,
                                margin_action.get("amount"),
                            )
                            continue
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
                                    "severity": severity,
                                    "buffer_pct": buffer_pct,
                                    "mark_price": _safe_float(pos.get("mark_price")),
                                    "liq_price": _safe_float(pos.get("liquidation_price")),
                                    "quantity": qty,
                                    "available": _safe_float(entry.get("available")),
                                    "margin_action": margin_action,
                                },
                            )
                        )
                    else:
                        if key in self._active_position_alerts:
                            resolved_positions.append(key)
                        reduce_action = await self._maybe_reduce_isolated_margin(
                            exchange=exchange,
                            position=pos,
                            buffer_pct=buffer_pct,
                        )
                        if reduce_action and reduce_action.get("status") == "ok":
                            logger.info(
                                "%s %s %s margin reduced: -%s",
                                exchange,
                                symbol,
                                side,
                                reduce_action.get("amount"),
                            )

        async with self._alert_lock:
            for key in resolved_keys:
                self._active_alerts.discard(key)
            for key in resolved_positions:
                self._active_position_alerts.discard(key)

            for key, payload in alerts:
                last = self._last_alert_sent.get(key, 0.0)
                if (now - last) < self._alert_cooldown and key in self._active_alerts:
                    continue
                if await self._send_telegram_alert(payload):
                    self._active_alerts.add(key)
                    self._last_alert_sent[key] = now

            for key, payload in position_alerts:
                last = self._last_position_alert_sent.get(key, 0.0)
                if (now - last) < self._alert_cooldown and key in self._active_position_alerts:
                    continue
                if await self._send_telegram_position_alert(payload):
                    self._active_position_alerts.add(key)
                    self._last_position_alert_sent[key] = now

    async def _send_telegram_alert(self, payload: dict[str, Any]) -> bool:
        text = self._format_alert_message(payload)
        return await self._send_telegram_text(text)

    async def send_telegram_message(self, text: str) -> bool:
        """Expose Telegram sending for external callers (throttling handled by caller)."""
        return await self._send_telegram_text(text)

    def _format_alert_message(self, payload: dict[str, Any]) -> str:
        exchange = payload.get("exchange", "").upper()
        asset = payload.get("asset", "")
        entry = payload.get("entry", {})
        available = payload.get("available")
        used = payload.get("used")
        total = payload.get("total")
        margin_ratio = payload.get("margin_ratio")
        ts = entry.get("timestamp") or datetime.now(timezone.utc).isoformat()
        parts = [
            f"[ALERT] Low margin buffer on {exchange} {asset}",
            f"Total: {total}",
            f"Available: {available}",
            f"Used: {used}",
        ]
        if margin_ratio is not None:
            parts.append(f"Margin ratio: {margin_ratio}")
        parts.append(f"Timestamp: {ts}")
        return "\n".join(str(p) for p in parts)

    async def _send_telegram_position_alert(self, payload: dict[str, Any]) -> bool:
        text = self._format_position_alert_message(payload)
        return await self._send_telegram_text(text)

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
        free_ok = available is not None and available >= self._min_free_balance_abs
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
            status = "can add margin" if free_ok else "low for top-up"
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
        notional = _safe_float(position.get("notional"))
        leverage = _safe_float(position.get("leverage"))
        if notional is None or leverage is None or leverage <= 0:
            return None
        return abs(notional) / leverage

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
        if exchange == "okx" and side:
            params["posSide"] = side
        if exchange == "bitget" and side:
            params["holdSide"] = side
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
        params: dict[str, Any] = {
            "category": category,
            "symbol": market.get("id") if market else symbol,
            "margin": client.amount_to_precision(symbol, amount),
        }
        raw = position.get("raw") or {}
        position_idx = raw.get("positionIdx") or raw.get("position_idx")
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
        try:
            if action == "add":
                if exchange == "bybit" and hasattr(client, "private_post_v5_position_add_margin"):
                    result = await self._bybit_add_margin(client, symbol, amount, position)
                elif hasattr(client, "add_margin"):
                    result = await client.add_margin(symbol, amount, params)
                else:
                    return {"status": "error", "error": "add_margin_unsupported"}
            else:
                if exchange == "bybit":
                    return {"status": "error", "error": "reduce_margin_unsupported"}
                if not hasattr(client, "reduce_margin"):
                    return {"status": "error", "error": "reduce_margin_unsupported"}
                reduce_amount = -amount if exchange == "bitget" else amount
                result = await client.reduce_margin(symbol, reduce_amount, params)
            return {"status": "ok", "result": result}
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
            leverage = _safe_float(position.get("leverage"))
            if self._enforce_isolated_margin and margin_mode != "isolated":
                key = (exchange, symbol, side, "mode")
                if self._margin_enforce_ready(key):
                    result = await self._set_margin_mode(exchange, position, "isolated")
                    if result.get("status") == "ok":
                        logger.info("%s %s %s margin mode set to isolated", exchange, symbol, side)
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
                    key = (exchange, symbol, side, "leverage")
                    if self._margin_enforce_ready(key):
                        result = await self._set_leverage(exchange, position, margin_mode or "isolated", target)
                        if result.get("status") == "ok":
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
        try:
            await client.set_leverage(leverage, symbol, params or None)
            return {"status": "ok"}
        except Exception as exc:  # pylint: disable=broad-except
            return {"status": "error", "error": str(exc)}

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
        severity: str,
    ) -> dict[str, Any]:
        if not self._auto_margin_enabled:
            return {"status": "disabled"}
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
        min_free = max(self._min_free_balance_abs, self._min_free_balance_rel * margin_used)
        if available is None or available <= min_free:
            return {
                "status": "no_funds",
                "available": available,
                "min_required": min_free,
            }
        max_add = max(0.0, available - min_free)
        add_pct = self._margin_add_panic_pct if severity == "panic" else self._margin_add_pct
        add_amt = min(max_add, margin_used * add_pct)
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
        result.update({"action": "add", "amount": add_amt, "buffer_pct": buffer_pct})
        return result

    async def _maybe_reduce_isolated_margin(
        self,
        *,
        exchange: str,
        position: Mapping[str, Any],
        buffer_pct: float,
    ) -> dict[str, Any] | None:
        if not self._auto_margin_reduce_enabled:
            return None
        if buffer_pct < (self._target_buffer_pct * 1.75):
            return None
        symbol = normalize_symbol(position.get("symbol") or position.get("symbol_normalized"))
        side = self._position_side_hint(position) or "unknown"
        key = (exchange, symbol, side, "reduce")
        now = time.time()
        last = self._last_margin_adjust.get(key, 0.0)
        if (now - last) < self._margin_adjust_cooldown:
            return None
        margin_used = self._position_margin_used(dict(position))
        if margin_used is None or margin_used <= 0:
            return None
        reduce_amt = margin_used * self._margin_reduce_pct
        if reduce_amt <= 0:
            return None
        result = await self._modify_margin(
            exchange=exchange,
            position=position,
            amount=reduce_amt,
            action="reduce",
        )
        if result.get("status") == "ok":
            self._last_margin_adjust[key] = now
        result.update({"action": "reduce", "amount": reduce_amt, "buffer_pct": buffer_pct})
        return result

    async def _send_telegram_text(self, text: str) -> bool:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            if not self._telegram_warned:
                logger.info("Telegram send skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
                self._telegram_warned = True
            return False
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": text}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(url, data=data) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.warning("Telegram alert failed (%s): %s", resp.status, body)
                        return False
            return True
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Telegram alert error: %s", exc)
            return False


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
        for key in ("marginMode", "margin_mode", "marginType", "margin_type", "mgnMode", "tradeMode"):
            mode = _normalize_margin_mode(info.get(key))
            if mode:
                return mode, f"info.{key}"
    trade_mode = payload.get("tradeMode")
    mode = _normalize_margin_mode(trade_mode)
    if mode:
        return mode, "payload.tradeMode"
    if slug == "gate":
        leverage = _safe_float(payload.get("leverage"))
        if leverage is not None:
            return ("cross" if leverage == 0 else "isolated"), "leverage_fallback"
    return None, None


def _extract_leverage(payload: dict[str, Any]) -> tuple[float | None, str | None]:
    leverage = _safe_float(payload.get("leverage"))
    if leverage is not None:
        return leverage, "payload.leverage"
    info = payload.get("info")
    if isinstance(info, dict):
        for key in ("leverage", "lever", "leverRate"):
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
