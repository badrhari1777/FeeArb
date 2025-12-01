from __future__ import annotations

import asyncio
import logging
import os
import time
import atexit
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
        options={"defaultType": "swap", "defaultSettle": "USDT"},
        balance_params={"type": "swap"},
        position_params={"type": "swap"},
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
            if self.slug == "mexc":
                # Align timestamps with server to avoid recvWindow errors.
                try:
                    await client.load_time_difference()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.debug("MEXC time sync failed; continuing without adjustment: %s", exc)
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
            leverage = _safe_float(payload.get("leverage"))
            liq_price = _safe_float(payload.get("liquidationPrice"))
            margin_mode = payload.get("marginMode") or payload.get("margin_mode")
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
                    "margin_used": margin_used,
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
        await self._maybe_send_alerts(balances)
        await self._maybe_send_summary(balances, refreshed)
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

    async def _maybe_send_summary(self, balances: list[dict[str, Any]], refreshed_at: str | None) -> None:
        """Send a periodic balance digest via Telegram."""
        now = time.time()
        if now < self._next_summary_at:
            return
        text = self._build_balance_summary(balances, refreshed_at)
        if await self._send_telegram_text(text):
            self._next_summary_at = now + self._summary_interval

    def _build_balance_summary(self, balances: list[dict[str, Any]], refreshed_at: str | None) -> str:
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
        return "\n".join(lines)

    async def _maybe_send_alerts(self, balances: list[dict[str, Any]]) -> None:
        """Evaluate balances and send Telegram alert on high-risk accounts."""
        if not balances:
            return
        alerts: list[tuple[tuple[str, str], dict[str, Any]]] = []
        resolved_keys: list[tuple[str, str]] = []
        now = time.time()

        for entry in balances:
            exchange = str(entry.get("exchange") or "").lower()
            asset = str(entry.get("asset") or "").upper()
            if not exchange or not asset:
                continue
            key = (exchange, asset)
            total = _safe_float(entry.get("total"), default=0.0) or 0.0
            available = _safe_float(entry.get("available"))
            used = _safe_float(entry.get("used"), default=0.0) or 0.0
            margin_ratio = _safe_float(entry.get("margin_ratio"))
            if used <= 0:
                if key in self._active_alerts:
                    resolved_keys.append(key)
                continue

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

        async with self._alert_lock:
            for key in resolved_keys:
                self._active_alerts.discard(key)

            for key, payload in alerts:
                last = self._last_alert_sent.get(key, 0.0)
                if (now - last) < self._alert_cooldown and key in self._active_alerts:
                    continue
                if await self._send_telegram_alert(payload):
                    self._active_alerts.add(key)
                    self._last_alert_sent[key] = now

    async def _send_telegram_alert(self, payload: dict[str, Any]) -> bool:
        text = self._format_alert_message(payload)
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
