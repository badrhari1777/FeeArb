from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import BASE_DIR

try:
    import ccxt  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    ccxt = None

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
        self.refresh_credentials(force_env=True)
        self.ensure_client()

    @property
    def client(self):
        return self._client

    def _build_client(self):
        if ccxt is None:
            self._unavailable_reason = "ccxt is not installed"
            return None
        exchange_cls = getattr(ccxt, self.spec.ccxt_id, None)
        if exchange_cls is None:
            self._unavailable_reason = f"ccxt.{self.spec.ccxt_id} is unavailable"
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
                    client.load_time_difference()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.debug("MEXC time sync failed; continuing without adjustment: %s", exc)
        except Exception as exc:  # pylint: disable=broad-except
            self._unavailable_reason = str(exc)
            logger.warning("%s: failed to instantiate ccxt client: %s", self.slug, exc)
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
        self._client = None
        self._unavailable_reason = None

    def ensure_client(self) -> None:
        if not self.has_credentials:
            self._client = None
            return
        if self._client is None:
            self._client = self._build_client()
            if self._client is None and self._unavailable_reason is None:
                self._unavailable_reason = "Failed to initialise ccxt client"

    def fetch_balance(self) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        params = dict(self.spec.balance_params)
        balance = self.client.fetch_balance(params=params)
        if self.slug == "gate":
            self._patch_gate_balance(balance)
        asset = self.spec.settle_currency.upper()
        asset_row = balance.get(asset) or balance.get(asset.lower()) or {}
        totals = balance.get("total") or {}
        frees = balance.get("free") or {}
        useds = balance.get("used") or {}
        total_value = _safe_float(asset_row.get("total")) or _safe_float(totals.get(asset))
        free_value = _safe_float(asset_row.get("free")) or _safe_float(frees.get(asset))
        used_value = _safe_float(asset_row.get("used")) or _safe_float(useds.get(asset))
        unrealized = _safe_float(balance.get("info", {}).get("unrealisedPnl")) if isinstance(balance.get("info"), dict) else None
        margin_ratio = _safe_float(balance.get("info", {}).get("marginRatio")) if isinstance(balance.get("info"), dict) else None
        timestamp = balance.get("timestamp") or balance.get("datetime")
        return {
            "exchange": self.slug,
            "asset": asset,
            "total": total_value,
            "available": free_value,
            "used": used_value,
            "unrealized_pnl": unrealized,
            "margin_ratio": margin_ratio,
            "timestamp": _ts_to_iso(timestamp),
        }

    def fetch_positions(self) -> List[dict[str, Any]]:
        if not self.client:
            raise RuntimeError(self._unavailable_reason or "exchange client unavailable")
        params = dict(self.spec.position_params)
        try:
            positions = self.client.fetch_positions(params=params)  # type: ignore[attr-defined]
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
            if notional is None and payload.get("entryPrice") and contracts:
                notional = contracts * _safe_float(payload.get("entryPrice"), default=0.0)
            result.append(
                {
                    "exchange": self.slug,
                    "symbol": symbol,
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
                    "leverage": _safe_float(payload.get("leverage")),
                    "liquidation_price": _safe_float(payload.get("liquidationPrice")),
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


class AccountMonitor:
    """Background refresher that keeps ccxt balances/positions in memory."""

    def __init__(self, refresh_interval: int = 120) -> None:
        self._interval = max(30, refresh_interval)
        self._gateways = {spec.slug: ExchangeGateway(spec) for spec in EXCHANGE_SPECS}
        self._lock = asyncio.Lock()
        self._balances: list[dict[str, Any]] = []
        self._positions: list[dict[str, Any]] = []
        self._status: list[dict[str, Any]] = []
        self._last_updated: str | None = None
        self._task: asyncio.Task | None = None

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

    async def refresh_now(self, *, force_env: bool = False) -> None:
        await self._refresh(force_env=force_env)

    def update_interval(self, seconds: int) -> None:
        self._interval = max(30, int(seconds))

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
        loop = asyncio.get_running_loop()
        balances, positions, status, refreshed = await loop.run_in_executor(
            None, lambda: self._collect_all(force_env=force_env)
        )
        async with self._lock:
            self._balances = balances
            self._positions = positions
            self._status = status
            if refreshed:
                self._last_updated = refreshed

    def _collect_all(
        self,
        force_env: bool = False,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str | None]:
        balances: list[dict[str, Any]] = []
        positions: list[dict[str, Any]] = []
        status_entries: list[dict[str, Any]] = []
        refreshed: str | None = None
        timestamp = datetime.now(timezone.utc).isoformat()
        for slug, gateway in self._gateways.items():
            gateway.refresh_credentials(force_env=force_env)
            gateway.ensure_client()
            entry = {
                "exchange": slug,
                "checked_at": timestamp,
            }
            if not gateway.has_credentials:
                entry["status"] = "missing_credentials"
                entry["message"] = "Add API keys to .env"
                status_entries.append(entry)
                continue
            if not gateway.available:
                entry["status"] = "unavailable"
                entry["error"] = gateway.unavailable_reason or "client unavailable"
                status_entries.append(entry)
                continue
            try:
                balances.append(gateway.fetch_balance())
                positions.extend(gateway.fetch_positions())
                entry["status"] = "ok"
                entry["message"] = "Credentials verified"
                refreshed = timestamp
            except Exception as exc:  # pylint: disable=broad-except
                entry["status"] = "error"
                entry["error"] = str(exc)
                logger.warning("%s: account refresh failed: %s", slug, exc)
            status_entries.append(entry)
        return balances, positions, status_entries, refreshed


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
