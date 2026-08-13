from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import time
import traceback
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot
from project_settings import SettingsManager
from execution.manual import (
    ManualTradeManager,
    _apply_price_offset,
    estimate_fill,
    max_qty_for_slippage,
    suggest_expensive_leg,
    spread_pct,
    venue_liquidity_tier,
)
from execution import (
    ExecutionSettingsManager,
    WalletService,
    PositionManager,
    TelemetryClient,
)
from execution.accounts import _safe_float, bitget_position_side, bitget_private_params, bitget_uta_enabled
from execution.allocator import Allocator
from execution.lifecycle import LifecycleController
from execution.settings import ExecutionSettings
from execution.storage import RotatingJsonlEventStore
from execution.grid_state import GridStateRepository
from execution.auto_arb_grid import (
    MAX_LEVELS,
    MIN_LEVELS,
    build_grid_levels,
    build_grid_pending_transition,
    apply_grid_decision_confirmation,
    complete_pending_grid_transition,
    decide_grid_transition,
    reduce_grid_transition_execution,
    reduce_missing_grid_execution,
    reduce_partial_grid_transition,
    grid_completion_tolerance,
    grid_dust_only_errors,
    grid_hedge_imbalance_tolerance,
    grid_level_count_for_existing_qty,
    grid_level_for_qty,
    grid_live_conflict,
    grid_live_conflict_message,
    grid_non_closeable_dust,
    grid_partial_adoption_level_for_qty,
    grid_reset_after_flat_repair,
    grid_rules_share_live_ownership,
    grid_symbol_ownership_key,
    grid_transition_completion_tolerance,
    normalize_level_count,
    recommend_level_count,
)
from execution.accounts import AccountMonitor, normalize_symbol
from risk.config import default_risk_config, RiskConfig
from risk.stop_manager import ProtectiveOrderManager
from utils.notifications import NotificationRouter
from utils import purge_expired
from utils.cache_db import get_or_fetch_funding_history
from utils.funding import (
    enrich_history_intervals,
    infer_funding_interval_hours,
    is_stale_next_funding_iso,
    project_next_funding_time_iso,
)
from exchanges import get_adapter_cached, normalize_exchange_name
from config import BASE_DIR, STATE_DIR
from .funding_history import (
    ADAPTER_FACTORIES,
    FUNDING_HISTORY_DEFAULT_EXCHANGES,
    FUNDING_HISTORY_EXCLUDED_EXCHANGES,
    FUNDING_HISTORY_MAX_POINTS,
    FUNDING_HISTORY_WINDOWS_HOURS,
    FundingHistoryService,
    _compact_funding_history_rows,
    _funding_history_ts_ms,
    _funding_net_hourly_series,
    _resolve_funding_interval_hours,
)
from .market_data import MarketDataBus
from .manual_symbols import _normalize_input_symbol
from .main_positions_read_model import (
    _select_position_pair_from_legs,
    build_main_positions_payload,
    compact_account_balances,
)
from .manual_spread_service import ManualSpreadService
from .runtime_modules import RUNTIME_MODULES, RuntimeModules
from uuid import uuid4

FUNDING_CACHE_TTL_SEC = 120
POSITIONS_MARKET_CONCURRENCY = 3
AUTO_ARB_STATE_PATH = STATE_DIR / "auto_arb_rules.json"
AUTO_ARB_HISTORY_PATH = BASE_DIR / "logs" / "auto_arb_history.jsonl"
AUTO_ARB_COMPLETION_TOLERANCE_PCT = 1.0
AUTO_ARB_RETRY_SEC = 2.0
PROTECTIVE_SHADOW_HISTORY_PATH = BASE_DIR / "logs" / "protective_shadow_history.jsonl"
PROTECTIVE_SHADOW_HISTORY_MAX_BYTES = 64 * 1024 * 1024
PROTECTIVE_SHADOW_EVENT_LIMIT = 100
PROTECTIVE_SHADOW_HEARTBEAT_SEC = 900.0
MANUAL_EXEC_LOG_DIR = BASE_DIR / "logs" / "manual_exec"

logger = logging.getLogger(__name__)
DEFAULT_MANUAL_LEVERAGE = 3.0
MANUAL_MARGIN_REDUCE_BUFFER_MULT = 1.2
BINANCE_MIN_MARGIN_BUFFER_PCT = 0.0
BYBIT_MIN_MARGIN_BUFFER_PCT = 0.01
BITGET_MIN_MARGIN_BUFFER_PCT = 0.01
GATE_MIN_MARGIN_BUFFER_PCT = 0.03
OKX_MIN_MARGIN_BUFFER_PCT = 0.0
KUCOIN_MIN_MARGIN_BUFFER_PCT = 0.0015
DEFAULT_MIN_MARGIN_BUFFER_PCT = 0.01
funding_logger = logging.getLogger("funding")
if not funding_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    funding_logger.addHandler(handler)
funding_logger.setLevel(logging.INFO)
funding_logger.propagate = False

funding_test_logger = logging.getLogger("funding_tests")
if not funding_test_logger.handlers:
    log_path = BASE_DIR / "logs" / "funding_tests.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    funding_test_logger.addHandler(handler)
    funding_test_logger.setLevel(logging.INFO)
    funding_test_logger.propagate = False


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


def _normalize_manual_symbol(symbol: str | None) -> str:
    """Normalize symbols from UI inputs (remove venue suffixes like -SWAP, USDTM)."""
    normalized = normalize_symbol(symbol or "")
    if not normalized:
        return ""
    for suffix in ("UMCBL", "DMCBL"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    if normalized.endswith("USDTM"):
        normalized = normalized[:-1]
    for suffix in ("SWAP", "PERP"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized




def _funding_eta(next_funding_iso: str | None) -> tuple[int | None, str | None]:
    if not next_funding_iso:
        return None, None
    try:
        dt = datetime.fromisoformat(next_funding_iso)
    except Exception:
        return None, None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = (dt - datetime.now(timezone.utc)).total_seconds()
    seconds = int(delta)
    if seconds < 0:
        return seconds, "passed"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return seconds, f"{hours}h {minutes:02d}m"


def _symbol_match_values(symbol: str | None) -> set[str]:
    normalized = _normalize_manual_symbol(symbol)
    if not normalized:
        return set()
    return {normalized, _strip_settle(normalized)}


def _position_matches_symbol(position: Mapping[str, Any], match_keys: set[str]) -> bool:
    if not match_keys:
        return False
    for key in ("symbol_normalized", "symbol", "exchange_symbol"):
        candidate = position.get(key)
        if not candidate:
            continue
        for normalized in _symbol_match_values(str(candidate)):
            if normalized in match_keys:
                return True
    return False


def _position_pair_quantities(
    positions: Iterable[Mapping[str, Any]],
    *,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
) -> dict[str, float]:
    match_keys = _symbol_match_values(symbol)
    long_exchange = normalize_exchange_name(long_exchange)
    short_exchange = normalize_exchange_name(short_exchange)
    long_qty = 0.0
    short_qty = 0.0
    for position in positions or []:
        if not _position_matches_symbol(position, match_keys):
            continue
        exchange = normalize_exchange_name(str(position.get("exchange") or ""))
        qty_raw = _safe_float(position.get("coin_qty"))
        if qty_raw is None:
            qty_raw = _safe_float(position.get("quantity"))
        if qty_raw is None:
            qty_raw = _safe_float(position.get("contracts"))
        qty = abs(float(qty_raw or 0.0))
        if qty <= 0:
            continue
        side = str(position.get("side") or "").strip().lower()
        if side not in {"long", "short"}:
            if float(qty_raw or 0.0) > 0:
                side = "long"
            elif float(qty_raw or 0.0) < 0:
                side = "short"
        if exchange == long_exchange and side == "long":
            long_qty += qty
        elif exchange == short_exchange and side == "short":
            short_qty += qty
    hedged_qty = min(long_qty, short_qty)
    imbalance_qty = abs(long_qty - short_qty)
    imbalance_pct = (imbalance_qty / hedged_qty * 100.0) if hedged_qty > 0 else 0.0
    return {
        "long_qty": float(long_qty),
        "short_qty": float(short_qty),
        "hedged_qty": float(hedged_qty),
        "imbalance_qty": float(imbalance_qty),
        "imbalance_pct": float(imbalance_pct),
    }






def _protective_issue_kind(message: Any) -> str | None:
    text = str(message or "").strip()
    if not text:
        return None
    lower = text.lower()
    if "invalid api-key" in lower or "permissions for action" in lower or "authenticationerror" in lower:
        return "auth_error"
    if (
        "exchangenotavailable" in lower
        or "timeout while contacting dns servers" in lower
        or "dns server returned answer with no data" in lower
        or "getaddrinfo failed" in lower
        or "bad gateway" in lower
        or "network" in lower
    ):
        return "network_error"
    return None


def _manual_position_metrics(position: Mapping[str, Any]) -> dict[str, float | None]:
    mark = _safe_float(position.get("mark_price"))
    liq = _safe_float(position.get("liquidation_price"))
    distance = None
    distance_pct = None
    if mark is not None and liq is not None and mark:
        distance = abs(mark - liq)
        distance_pct = abs(mark - liq) / abs(mark) * 100.0
    return {
        "mark_price": mark,
        "liquidation_price": liq,
        "liq_distance": distance,
        "liq_distance_pct": distance_pct,
        "margin_used": _safe_float(position.get("margin_used")),
        "initial_margin": _safe_float(position.get("initial_margin")),
        "maintenance_margin": _safe_float(position.get("maintenance_margin")),
        "leverage": _safe_float(position.get("leverage")),
    }


def _manual_position_view(
    position: Mapping[str, Any] | None,
    *,
    estimates: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not position:
        return None
    margin_mode = position.get("margin_mode")
    leverage = _safe_float(position.get("leverage"))
    view: dict[str, Any] = {
        "symbol": position.get("symbol"),
        "exchange_symbol": position.get("exchange_symbol"),
        "side": position.get("side"),
        "margin_mode": margin_mode,
        "margin_mode_source": position.get("margin_mode_source"),
        "margin_mode_known": margin_mode is not None,
        "leverage": leverage,
        "leverage_source": position.get("leverage_source"),
        "leverage_known": leverage is not None,
    }
    notes: list[str] = []
    if margin_mode is None:
        notes.append("margin_mode_unknown")
    if leverage is None:
        notes.append("leverage_unknown")
    if notes:
        view["notes"] = notes
    if estimates:
        view.update(estimates)
    return view


def _manual_margin_estimates(
    position: Mapping[str, Any] | None,
    balance: Mapping[str, Any] | None,
    *,
    buffer_mult: float = MANUAL_MARGIN_REDUCE_BUFFER_MULT,
) -> dict[str, Any]:
    if not position:
        return {
            "equity_est": None,
            "base_margin_est": None,
            "base_margin_source": None,
            "max_reduce_est": None,
            "max_add_est": _safe_float(balance.get("available")) if balance else None,
            "reduce_buffer_mult": buffer_mult,
            "max_reduce_source": None,
        }
    exchange = normalize_exchange_name(str(position.get("exchange") or ""))
    raw = position.get("raw") if isinstance(position, dict) else None
    info = raw.get("info") if isinstance(raw, dict) else None

    base_margin = None
    base_source = None
    if isinstance(raw, dict):
        base_margin = _safe_float(raw.get("positionBalance")) or _safe_float(raw.get("collateral"))
        if base_margin is not None:
            base_source = "raw.positionBalance" if raw.get("positionBalance") is not None else "raw.collateral"
    if base_margin is None and isinstance(info, dict):
        base_margin = _safe_float(info.get("positionBalance")) or _safe_float(info.get("positionIM"))
        if base_margin is not None:
            base_source = (
                "info.positionBalance" if info.get("positionBalance") is not None else "info.positionIM"
            )
    if base_margin is None:
        base_margin = _safe_float(position.get("margin_used")) or _safe_float(position.get("initial_margin"))
        if base_margin is not None:
            base_source = (
                "position.margin_used" if position.get("margin_used") is not None else "position.initial_margin"
            )

    unrealized = _safe_float(position.get("unrealized_pnl"))
    if unrealized is None and isinstance(raw, dict):
        unrealized = _safe_float(raw.get("unrealizedPnl"))
    if unrealized is None and isinstance(info, dict):
        unrealized = _safe_float(info.get("unrealisedPnl"))

    maintenance = _safe_float(position.get("maintenance_margin"))
    equity = None
    if base_margin is not None:
        equity = base_margin + (unrealized or 0.0)

    max_reduce = None
    max_reduce_source = None
    reduce_buffer_mult = buffer_mult
    min_required = None
    min_required_source: list[str] = []

    position_value = None
    if isinstance(info, dict):
        position_value = _safe_float(info.get("positionValue"))
    if position_value is None and isinstance(raw, dict):
        position_value = _safe_float(raw.get("positionValue"))
    if position_value is None:
        position_value = _safe_float(position.get("notional"))

    leverage = _safe_float(position.get("leverage"))
    if leverage is None and isinstance(info, dict):
        leverage = _safe_float(info.get("leverage"))

    buffer_pct = DEFAULT_MIN_MARGIN_BUFFER_PCT
    if exchange == "binance":
        buffer_pct = BINANCE_MIN_MARGIN_BUFFER_PCT
    elif exchange == "bybit":
        buffer_pct = BYBIT_MIN_MARGIN_BUFFER_PCT
    elif exchange == "bitget":
        buffer_pct = BITGET_MIN_MARGIN_BUFFER_PCT
    elif exchange == "gate":
        buffer_pct = GATE_MIN_MARGIN_BUFFER_PCT
    elif exchange == "okx":
        buffer_pct = OKX_MIN_MARGIN_BUFFER_PCT
    elif exchange == "kucoin":
        buffer_pct = KUCOIN_MIN_MARGIN_BUFFER_PCT

    target_leverage = leverage
    if exchange == "kucoin":
        target_leverage = DEFAULT_MANUAL_LEVERAGE

    if position_value is not None and target_leverage:
        min_required = abs(position_value) / target_leverage * (1.0 + buffer_pct)
        min_required_source.append("positionValue/target_leverage+buffer")
        min_required_source.append(f"buffer_pct={buffer_pct:.4f}")
        if exchange == "kucoin":
            min_required_source.append(f"target_leverage={DEFAULT_MANUAL_LEVERAGE:g}")

    if exchange == "okx":
        initial_margin = _safe_float(position.get("initial_margin"))
        if initial_margin is None and isinstance(raw, dict):
            initial_margin = _safe_float(raw.get("initialMargin"))
        if initial_margin is not None:
            min_required = initial_margin * (1.0 + OKX_MIN_MARGIN_BUFFER_PCT)
            min_required_source = [
                "position.initial_margin",
                f"buffer_pct={OKX_MIN_MARGIN_BUFFER_PCT:.4f}",
            ]

    if exchange == "gate" and isinstance(info, dict):
        info_margin = _safe_float(
            info.get("margin")
            or info.get("position_margin")
            or info.get("positionMargin")
        )
        info_initial = _safe_float(
            info.get("initialMargin")
            or info.get("initial_margin")
            or info.get("initMargin")
            or info.get("init_margin")
        )
        if info_margin is not None:
            base_margin = info_margin
            base_source = "info.margin"
            equity = base_margin + (unrealized or 0.0)
        if info_initial is not None:
            min_required = info_initial * (1.0 + GATE_MIN_MARGIN_BUFFER_PCT)
            min_required_source = [
                "info.initial_margin",
                f"buffer_pct={GATE_MIN_MARGIN_BUFFER_PCT:.4f}",
            ]

    if exchange == "binance":
        info_margin = None
        info_margin_source = None
        if isinstance(raw, dict):
            info_margin = _safe_float(raw.get("isolatedWallet") or raw.get("isolatedMargin"))
            if info_margin is not None:
                info_margin_source = (
                    "raw.isolatedWallet" if raw.get("isolatedWallet") is not None else "raw.isolatedMargin"
                )
        if info_margin is None and isinstance(info, dict):
            info_margin = _safe_float(info.get("isolatedWallet") or info.get("isolatedMargin"))
            if info_margin is not None:
                info_margin_source = (
                    "info.isolatedWallet" if info.get("isolatedWallet") is not None else "info.isolatedMargin"
                )
        if info_margin is not None:
            base_margin = info_margin
            base_source = info_margin_source
            equity = base_margin + (unrealized or 0.0)
        if maintenance is None:
            if isinstance(raw, dict):
                maintenance = _safe_float(
                    raw.get("maintMargin")
                    or raw.get("maintenanceMargin")
                    or raw.get("positionMaintMargin")
                    or raw.get("positionMaintenanceMargin")
                )
            if maintenance is None and isinstance(info, dict):
                maintenance = _safe_float(
                    info.get("maintMargin")
                    or info.get("maintenanceMargin")
                    or info.get("positionMaintMargin")
                    or info.get("positionMaintenanceMargin")
                )
        if maintenance is not None:
            min_required = maintenance * (1.0 + BINANCE_MIN_MARGIN_BUFFER_PCT)
            min_required_source = [
                "maintenance_margin",
                f"buffer_pct={BINANCE_MIN_MARGIN_BUFFER_PCT:.4f}",
            ]

    if exchange == "bingx" and isinstance(info, dict):
        info_margin = _safe_float(info.get("margin"))
        max_reduction = _safe_float(info.get("maxMarginReduction"))
        if info_margin is not None:
            base_margin = info_margin
            base_source = "info.margin"
            equity = base_margin + (unrealized or 0.0)
        if max_reduction is not None:
            max_reduce = max(0.0, max_reduction)
            max_reduce_source = "info.maxMarginReduction"
            reduce_buffer_mult = 1.0
            if base_margin is not None:
                min_required = max(0.0, base_margin - max_reduce)
                min_required_source = ["info.margin - info.maxMarginReduction"]

    if max_reduce is None and base_margin is not None and min_required is not None:
        max_reduce = max(0.0, base_margin - min_required)
        max_reduce_source = "base_margin_minus_min_required"
        reduce_buffer_mult = 1.0 + buffer_pct

    if max_reduce is None and equity is not None and maintenance is not None:
        max_reduce = max(0.0, equity - maintenance * buffer_mult)
        max_reduce_source = "equity_minus_maintenance_buffer"

    return {
        "equity_est": equity,
        "base_margin_est": base_margin,
        "base_margin_source": base_source,
        "min_required_margin_est": min_required,
        "min_required_margin_source": min_required_source or None,
        "target_leverage": target_leverage if exchange == "kucoin" else None,
        "max_reduce_est": max_reduce,
        "max_add_est": _safe_float(balance.get("available")) if balance else None,
        "reduce_buffer_mult": reduce_buffer_mult,
        "max_reduce_source": max_reduce_source,
    }


def _fetch_json_any(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:
        return json.load(resp)


def _filter_payload_list(payload: Any, key: str, match_value: str) -> tuple[Any, bool]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            filtered = [
                item for item in data if isinstance(item, dict) and item.get(key) == match_value
            ]
            if filtered != data:
                trimmed = dict(payload)
                trimmed["data"] = filtered
                return trimmed, True
    if isinstance(payload, list):
        filtered = [
            item for item in payload if isinstance(item, dict) and item.get(key) == match_value
        ]
        if filtered != payload:
            return filtered, True
    return payload, False






class DataService:
    def __init__(
        self,
        settings_manager: SettingsManager | None = None,
        *,
        runtime_modules: RuntimeModules = RUNTIME_MODULES,
    ) -> None:
        self._settings_manager = settings_manager or SettingsManager()
        self._runtime_modules = runtime_modules
        self._account_interval = self._settings_manager.current.account_refresh_seconds
        self._positions_market_interval = self._settings_manager.current.positions_market_refresh_seconds
        self._summary_interval = self._settings_manager.current.summary_refresh_seconds
        self._lock = asyncio.Lock()
        self._status: str = "idle"
        self._last_error: Optional[str] = None
        self._in_progress: bool = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._events: List[dict[str, Any]] = []
        self._funding_cache: dict[tuple[str, str], tuple[float | None, str | None, float | None, float]] = {}
        self._funding_history_service = FundingHistoryService()
        self._exec_settings_manager = ExecutionSettingsManager()
        self._execution_settings: ExecutionSettings = self._exec_settings_manager.current
        self._wallet = WalletService(self._execution_settings.balance.initial_balances)
        self._positions = PositionManager(self._wallet)
        self._allocator = Allocator(self._wallet, self._positions, self._execution_settings)
        self._lifecycle = LifecycleController(self._execution_settings, self._positions, self._allocator)
        self._telemetry = TelemetryClient(self._execution_settings)
        self._telemetry_events: List[dict[str, Any]] = []
        self._telemetry.register_listener(self._handle_telemetry_event)
        self._notifier = NotificationRouter()
        self._accounts = AccountMonitor(
            refresh_interval=self._account_interval,
            summary_interval=self._summary_interval,
            on_margin_adjust=self._on_margin_adjust_events,
            notifier=self._notifier,
            enabled_exchanges=self._account_monitor_enabled_exchanges(),
        )
        self._positions_market_lock = asyncio.Lock()
        self._positions_market_cache: dict[tuple[str, str], MarketSnapshot] = {}
        self._positions_market_cache_ts: dict[tuple[str, str], datetime] = {}
        self._positions_market_last_refresh: Optional[datetime] = None
        self._positions_market_last_error: Optional[str] = None
        self._positions_market_last_key: tuple[str, ...] | None = None
        self._positions_market_status: list[dict[str, Any]] = []
        self._positions_market_diffs: list[dict[str, Any]] = []
        self._positions_market_last_account_update: str | None = None
        self._positions_market_task: Optional[asyncio.Task] = None
        self._settings_refresh_task: Optional[asyncio.Task] = None
        self._settings_refresh_pending = False
        self._positions_market_sem = asyncio.Semaphore(POSITIONS_MARKET_CONCURRENCY)
        self._risk_config: RiskConfig = self._risk_config_from_settings()
        self._protective_manager = ProtectiveOrderManager(self._risk_config, notifier=self._notifier)
        self._last_protective: dict[tuple[str, str, str], dict[str, float | None]] = {}
        self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", 180)
        self._protective_task: Optional[asyncio.Task] = None
        self._protective_orphan_sweep_interval_sec = 15 * 60
        self._protective_orphan_sweep_last_ts = 0.0
        self._protective_orphan_sweep_inflight = False
        self._protective_shadow_history_store = RotatingJsonlEventStore(
            PROTECTIVE_SHADOW_HISTORY_PATH,
            max_bytes=PROTECTIVE_SHADOW_HISTORY_MAX_BYTES,
            max_backups=3,
        )
        self._protective_shadow_events: list[dict[str, Any]] = []
        self._protective_shadow_fingerprints: dict[str, str] = {}
        self._protective_shadow_last_ts: dict[str, float] = {}
        self._market_data = MarketDataBus()
        self._manual_spread_service = ManualSpreadService(
            market_data_provider=lambda: self._market_data,
            positions_market_provider=lambda: (
                self._positions_market_cache,
                self._positions_market_cache_ts,
            ),
        )
        self._manual = ManualTradeManager(orderbook_provider=self._market_data)
        self._manual_runs: Dict[str, dict[str, Any]] = {}
        self._manual_run_ttl = 3600
        self._grid_state = GridStateRepository(
            state_path=AUTO_ARB_STATE_PATH,
            history_path=AUTO_ARB_HISTORY_PATH,
        )
        # Compatibility aliases keep the transition migration incremental.
        self._auto_arb_store = self._grid_state.store
        self._auto_arb_history_store = self._grid_state.history
        self._auto_arb = self._grid_state.state
        self._auto_arb_lock = self._grid_state.lock
        self._auto_arb_task: Optional[asyncio.Task] = None
        self._auto_arb_poll_sec = 3.0
        self._automation_task: Optional[asyncio.Task] = None
        self._automation_poll_sec = 2.0
        self._mexc_alert_cooldown = 600  # seconds
        self._last_mexc_alert: dict[tuple[str, str], float] = {}
        self._send_missing_stop_alerts = True
        self._margin_logic_state: dict[str, str] = {}
        self._margin_logic_log: list[dict[str, Any]] = []
        self._margin_logic_log_limit = 80
        self._account_state_cache_key: tuple[Any, ...] | None = None
        self._account_state_cache: dict[str, object] | None = None
        self._apply_alert_settings()

    @property
    def notification_router(self) -> NotificationRouter:
        """Shared primary/fallback notification route for all live subsystems."""
        return self._notifier


    async def send_test_notification(
        self,
        *,
        message: str,
        title: str = "FeeArb test notification",
    ) -> dict[str, Any]:
        self._apply_alert_settings()
        status = await self._notifier.send_text_status(str(message or ""), title=str(title or "FeeArb test notification"))
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        return {
            "status": status,
            "primary_channel": str(protective.get("notification_primary_channel", "ntfy") or "ntfy"),
            "fallback_channel": str(protective.get("notification_fallback_channel", "none") or "none"),
            "title": str(title or "FeeArb test notification"),
        }

    async def analyze_funding_history(
        self,
        symbol: str,
        *,
        exchanges: Iterable[str] | None = None,
        windows_hours: Iterable[int | float] | None = None,
        funding_points: int = FUNDING_HISTORY_MAX_POINTS,
    ) -> dict[str, Any]:
        return await self._funding_history_service.analyze_funding_history(
            symbol,
            exchanges=exchanges,
            windows_hours=windows_hours,
            funding_points=funding_points,
        )


    async def startup(self) -> None:
        self._loop = asyncio.get_running_loop()
        purge_expired()
        async with self._lock:
            self._status = "pending"
            self._account_interval = self._settings_manager.current.account_refresh_seconds
        await self._accounts.start()
        # Do an immediate balance/positions pull before other work.
        await self._accounts.refresh_now(force_env=True)
        await self._refresh_positions_market_snapshots(force=True)
        await self._maybe_sync_protective_orders()
        # The legacy main-dashboard candidate collectors are retired. External
        # discovery now belongs to the isolated Strategy Lab Observatory and
        # must never start as a side effect of the trading dashboard startup.
        async with self._lock:
            self._status = "ready"
        if self._positions_market_task is None:
            self._positions_market_task = asyncio.create_task(self._positions_market_scheduler())
        if self._protective_task is None:
            self._protective_task = asyncio.create_task(self._protective_scheduler())
        if self._runtime_modules.auto_arb_grid and self._automation_task is None:
            self._automation_task = asyncio.create_task(self._automation_scheduler())
        await self._telemetry.start()

    async def shutdown(self) -> None:
        if self._settings_refresh_task:
            self._settings_refresh_task.cancel()
            try:
                await self._settings_refresh_task
            except asyncio.CancelledError:
                pass
            self._settings_refresh_task = None
            self._settings_refresh_pending = False
        if self._positions_market_task:
            self._positions_market_task.cancel()
            try:
                await self._positions_market_task
            except asyncio.CancelledError:
                pass
            self._positions_market_task = None
        if self._automation_task:
            self._automation_task.cancel()
            try:
                await self._automation_task
            except asyncio.CancelledError:
                pass
            self._automation_task = None
        if self._auto_arb_task:
            self._auto_arb_task.cancel()
            try:
                await self._auto_arb_task
            except asyncio.CancelledError:
                pass
            self._auto_arb_task = None
        if self._protective_task:
            self._protective_task.cancel()
            try:
                await self._protective_task
            except asyncio.CancelledError:
                pass
            self._protective_task = None
        await self._market_data.shutdown()
        await self._telemetry.stop()
        await self._accounts.stop()
        await self._manual.close()
        await self._protective_manager.close()

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

    async def _positions_market_scheduler(self) -> None:
        """Refresh market snapshots for live positions on a separate cadence."""
        try:
            while True:
                interval = max(30, int(self._positions_market_interval or self._account_interval))
                await asyncio.sleep(interval)
                await self._refresh_positions_market_snapshots()
        except asyncio.CancelledError:
            raise

    async def _restart_positions_market_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._positions_market_task:
            self._positions_market_task.cancel()
            try:
                await self._positions_market_task
            except asyncio.CancelledError:
                pass
            self._positions_market_task = None
        self._positions_market_task = asyncio.create_task(self._positions_market_scheduler())


    async def on_settings_updated(self) -> None:
        async with self._lock:
            current = self._settings_manager.current
            self._account_interval = current.account_refresh_seconds
            self._positions_market_interval = current.positions_market_refresh_seconds
            self._summary_interval = current.summary_refresh_seconds
            self._risk_config = self._risk_config_from_settings()
            self._protective_manager.update_config(self._risk_config)
            self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", self._protective_interval)
            self._apply_alert_settings()
        # Legacy market-discovery scheduling intentionally stays disabled.
        await self._restart_protective_scheduler()
        await self._restart_positions_market_scheduler()
        self._accounts.update_interval(self._account_interval)
        self._accounts.update_summary_interval(self._summary_interval)
        self._accounts.update_enabled_exchanges(self._account_monitor_enabled_exchanges())
        # Build the public position-market universe from the newly refreshed
        # account snapshot. Quick repeated saves coalesce into one follow-up.
        self._settings_refresh_pending = True
        if self._settings_refresh_task is None or self._settings_refresh_task.done():
            self._settings_refresh_task = asyncio.create_task(
                self._refresh_operational_state_after_settings()
            )

    async def _refresh_operational_state_after_settings(self) -> None:
        try:
            while self._settings_refresh_pending:
                self._settings_refresh_pending = False
                await self._accounts.refresh_now(force_env=True)
                await self._refresh_positions_market_snapshots(force=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Post-settings operational refresh failed: %s", exc)
        finally:
            self._settings_refresh_task = None

    def _account_monitor_enabled_exchanges(self) -> set[str]:
        """Use both venue selectors so disabling a venue everywhere stops private polling."""
        current = self._settings_manager.current
        result: set[str] = set()
        for flags in (
            getattr(current, "exchanges", None) or {},
            getattr(current, "analysis_exchanges", None) or {},
        ):
            for name, enabled in flags.items():
                normalized = normalize_exchange_name(str(name))
                if enabled and normalized:
                    result.add(normalized)
        return result

    async def manual_enter(self, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("dry_run"):
            payload = dict(payload)
            payload.setdefault("constraints_exchanges", self._manual_pair_constraints(payload, action="enter"))
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.enter(payload)
        return await self._start_manual_run("enter", payload, None)

    async def manual_exit(self, payload: dict[str, Any]) -> dict[str, Any]:
        positions = self._accounts.snapshot().get("positions") or []
        if payload.get("dry_run"):
            payload = dict(payload)
            payload.setdefault("constraints_exchanges", self._manual_pair_constraints(payload, action="exit"))
        if payload.get("dry_run"):
            return await self._manual.exit(payload, positions)
        if not payload.get("async_run"):
            result = await self._manual.exit(payload, positions)
            await self._cleanup_protective_after_exit(payload, result)
            return result
        return await self._start_manual_run("exit", payload, positions)

    async def _cleanup_protective_after_exit(
        self,
        payload: Mapping[str, Any],
        result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Remove exit-pair protection only where a fresh scan proves the leg is gone."""
        symbol = normalize_symbol(
            str(result.get("symbol") or payload.get("symbol") or "")
        )
        exchanges = {
            normalize_exchange_name(str(payload.get(field) or ""))
            for field in ("long_exchange", "short_exchange")
        }
        targets = {
            (exchange, symbol)
            for exchange in exchanges
            if exchange and symbol
        }
        if not targets:
            return []
        try:
            actions = await self._cleanup_verified_orphan_protective_targets(
                targets,
                reason="manual_exit_completed",
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "protective cleanup after exit failed symbol=%s exchanges=%s: %s",
                symbol,
                sorted(exchanges),
                exc,
            )
            return []
        result["protective_cleanup"] = actions
        return actions

    async def manual_orphan_cleanup(self, payload: dict[str, Any]) -> dict[str, Any]:
        positions = self._accounts.snapshot().get("positions") or []
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.orphan_cleanup(payload, positions)
        return await self._start_manual_run("orphan_cleanup", payload, positions)

    async def manual_roll(self, payload: dict[str, Any]) -> dict[str, Any]:
        positions = self._accounts.snapshot().get("positions") or []
        if payload.get("dry_run"):
            payload = dict(payload)
            payload.setdefault("constraints_exchanges", self._manual_pair_constraints(payload, action="roll"))
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.roll(payload, positions)
        return await self._start_manual_run("roll", payload, positions)

    def _save_auto_arb_config(self) -> None:
        self._grid_state.save()

    def auto_arb_payload(self) -> dict[str, Any]:
        return self._grid_state.payload()

    async def auto_arb_spreads(
        self,
        *,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
    ) -> dict[str, Any]:
        long_quote = await self._mobile_quote_for_exchange(long_exchange, symbol)
        short_quote = await self._mobile_quote_for_exchange(short_exchange, symbol)
        entry_spread = spread_pct(
            _safe_float(long_quote.get("ask")),
            _safe_float(short_quote.get("bid")),
        )
        exit_spread = spread_pct(
            _safe_float(long_quote.get("bid")),
            _safe_float(short_quote.get("ask")),
        )
        errors: list[str] = []
        if entry_spread is None:
            errors.append("Executable entry spread is unavailable.")
        if exit_spread is None:
            errors.append("Executable exit spread is unavailable.")
        return {
            "status": "ok" if not errors else "partial",
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "entry_spread_pct": entry_spread,
            "exit_spread_pct": exit_spread,
            "long_quote": long_quote,
            "short_quote": short_quote,
            "errors": errors,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def analyze_auto_arb(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = normalize_symbol(str(payload.get("symbol") or "")).upper()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        if not symbol or not long_exchange or not short_exchange:
            raise ValueError("symbol, long_exchange, and short_exchange are required.")
        if long_exchange == short_exchange:
            raise ValueError("long_exchange and short_exchange must be different.")

        warnings: list[str] = []
        setup_mode = str(payload.get("setup_mode") or "entry_range").strip().lower()
        existing_exit_setup = setup_mode in {
            "existing_position_exit_range",
            "adopt_existing_full_grid",
        }
        if setup_mode not in {
            "entry_range",
            "existing_position_exit_range",
            "adopt_existing_full_grid",
        }:
            raise ValueError(
                "setup_mode must be entry_range, existing_position_exit_range, "
                "or adopt_existing_full_grid."
            )
        range_start = _safe_float(payload.get("range_start_pct"))
        range_end = _safe_float(payload.get("range_end_pct"))
        exit_range_start = _safe_float(payload.get("exit_range_start_pct"))
        exit_range_end = _safe_float(payload.get("exit_range_end_pct"))
        if existing_exit_setup:
            if exit_range_start is None:
                exit_range_start = range_start
            if exit_range_end is None:
                exit_range_end = range_end
            if exit_range_start is None or exit_range_end is None:
                raise ValueError("exit_range_start_pct and exit_range_end_pct are required.")
            if math.isclose(float(exit_range_start), float(exit_range_end), abs_tol=1e-12):
                raise ValueError("Exit spread range must contain at least two different values.")
        elif range_start is None or range_end is None:
            raise ValueError("range_start_pct and range_end_pct are required.")
        budget_mode = str(payload.get("budget_mode") or "qty").strip().lower()
        if budget_mode not in {"qty", "notional"}:
            raise ValueError("budget_mode must be qty or notional.")
        requested_count = payload.get("level_count")
        requested_count_norm = (
            normalize_level_count(requested_count)
            if requested_count
            else None
        )
        inferred_exit_count = None
        if existing_exit_setup:
            exit_span = abs(float(exit_range_end) - float(exit_range_start))
            whole_step_count = int(round(exit_span))
            if math.isclose(exit_span, float(whole_step_count), abs_tol=1e-9):
                candidate_count = whole_step_count + 1
                if MIN_LEVELS <= candidate_count <= MAX_LEVELS:
                    inferred_exit_count = candidate_count
        try:
            account_snapshot = self._accounts.snapshot() or {}
            quantities = _position_pair_quantities(
                account_snapshot.get("positions") or [],
                symbol=symbol,
                long_exchange=long_exchange,
                short_exchange=short_exchange,
            )
        except Exception as exc:  # pylint: disable=broad-except
            quantities = {}
            warnings.append(f"Existing position check failed: {exc}")
        existing_hedged_qty = _safe_float(quantities.get("hedged_qty")) if quantities else None
        max_qty = _safe_float(payload.get("max_qty"))
        max_notional = _safe_float(payload.get("max_notional"))
        if existing_exit_setup and (existing_hedged_qty is None or existing_hedged_qty <= 0):
            raise ValueError(
                "No existing balanced position was found for this symbol and exchange pair."
            )
        if setup_mode == "existing_position_exit_range":
            if existing_hedged_qty is None or existing_hedged_qty <= 0:
                raise ValueError(
                    "No existing balanced position was found for this symbol and exchange pair."
                )
            budget_mode = "qty"
            max_qty = float(existing_hedged_qty)
            max_notional = None
        if setup_mode != "existing_position_exit_range":
            if budget_mode == "qty" and (max_qty is None or max_qty <= 0):
                raise ValueError("max_qty must be greater than zero.")
            if budget_mode == "notional" and (max_notional is None or max_notional <= 0):
                raise ValueError("max_notional must be greater than zero.")

        max_slippage_bps = max(0.0, _safe_float(payload.get("max_slippage_bps")) or 8.0)
        live_spreads = await self.auto_arb_spreads(
            symbol=symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
        )
        reference_prices = [
            _safe_float((live_spreads.get("long_quote") or {}).get("ask")),
            _safe_float((live_spreads.get("short_quote") or {}).get("bid")),
        ]
        reference_prices = [value for value in reference_prices if value and value > 0]
        reference_price = sum(reference_prices) / len(reference_prices) if reference_prices else None
        total_qty = max_qty
        if budget_mode == "notional":
            if reference_price is None:
                raise ValueError("Unable to convert the USDT budget because live prices are unavailable.")
            total_qty = float(max_notional) / reference_price
        if total_qty is None or total_qty <= 0:
            raise ValueError("Unable to resolve total grid quantity.")
        if (
            setup_mode == "adopt_existing_full_grid"
            and existing_hedged_qty is not None
            and float(existing_hedged_qty) > float(total_qty)
        ):
            raise ValueError(
                "Existing hedged quantity is larger than the configured full grid max_qty."
            )

        manual_base = {
            "symbol": symbol,
            "qty": float(total_qty),
            "notional": None,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "max_slippage_bps": max_slippage_bps,
            "use_orderbook_check": True,
            "dry_run": True,
            "async_run": False,
            "margin_mode": "isolated",
        }
        plans: dict[str, Any] = {}
        for action in ("enter", "exit"):
            try:
                plans[action] = await self.manual_analyze(
                    {
                        **manual_base,
                        "action": action,
                        "mode": "smart-enter" if action == "enter" else "smart-exit",
                    }
                )
            except Exception as exc:  # pylint: disable=broad-except
                plans[action] = {"errors": [str(exc)]}
                warnings.append(f"{action} dry run failed: {exc}")

        safe_candidates: list[float] = []
        min_chunk_candidates: list[float] = []
        for plan in plans.values():
            if not isinstance(plan, Mapping):
                continue
            recommended = _safe_float(
                plan.get("recommended_chunk_qty") or plan.get("recommended_qty")
            )
            minimum = _safe_float(plan.get("min_chunk_qty"))
            if recommended and recommended > 0:
                safe_candidates.append(recommended)
            if minimum and minimum > 0:
                min_chunk_candidates.append(minimum)
        liquidity_factor = _safe_float(payload.get("liquidity_safety_factor")) or 0.70
        liquidity_factor = min(1.0, max(0.1, liquidity_factor))
        fallback_count = requested_count_norm or inferred_exit_count or normalize_level_count(6)
        safe_chunk = min(safe_candidates) * liquidity_factor if safe_candidates else total_qty / fallback_count
        if min_chunk_candidates:
            safe_chunk = max(safe_chunk, max(min_chunk_candidates))
        safe_chunk = min(float(total_qty), safe_chunk)
        recommended_count = recommend_level_count(
            total_qty=total_qty,
            safe_chunk_qty=safe_chunk,
        )
        level_count = requested_count_norm or inferred_exit_count or recommended_count
        existing_position_fit = None
        if existing_hedged_qty and existing_hedged_qty > 0:
            fit = self._auto_arb_level_count_for_existing_qty(
                total_qty=float(total_qty),
                existing_qty=float(existing_hedged_qty),
                preferred_count=level_count,
            )
            if fit is not None:
                original_level_count = int(level_count)
                selected_count = int(fit["level_count"])
                fit.update(
                    {
                        "long_qty": float(quantities.get("long_qty") or 0.0),
                        "short_qty": float(quantities.get("short_qty") or 0.0),
                        "imbalance_qty": float(quantities.get("imbalance_qty") or 0.0),
                        "imbalance_pct": float(quantities.get("imbalance_pct") or 0.0),
                        "requested_level_count": requested_count_norm,
                        "recommended_level_count": recommended_count,
                        "original_level_count": original_level_count,
                        "level_count_adjusted": bool(
                            fit["matches"] and selected_count != original_level_count
                        ),
                    }
                )
                if fit["matches"]:
                    level_count = selected_count
                    fit["adoption_will_match"] = True
                    fit["adoption_exact"] = True
                    fit["adoption_partial"] = False
                    if fit["level_count_adjusted"]:
                        warnings.append(
                            "Grid levels adjusted from "
                            f"{original_level_count} to {selected_count} so the "
                            f"existing {float(existing_hedged_qty):g} qty matches "
                            f"level {int(fit['level'])} within tolerance."
                        )
                elif setup_mode == "adopt_existing_full_grid":
                    fit["adoption_will_match"] = False
                    fit["adoption_exact"] = False
                    fit["adoption_partial"] = True
                else:
                    fit["adoption_will_match"] = False
                    fit["adoption_exact"] = False
                    fit["adoption_partial"] = False
                    warnings.append(
                        "Existing hedged quantity does not match any grid level "
                        f"within {AUTO_ARB_COMPLETION_TOLERANCE_PCT:g}% tolerance "
                        f"for level counts {MIN_LEVELS}-{MAX_LEVELS}; closest is "
                        f"{selected_count} levels, level {int(fit['level'])}, "
                        f"diff {float(fit['diff_qty']):g} > tolerance "
                        f"{float(fit['tolerance_qty']):g}."
                    )
                existing_position_fit = fit
        if existing_exit_setup:
            exit_low = min(float(exit_range_start), float(exit_range_end))
            exit_high = max(float(exit_range_start), float(exit_range_end))
            grid_step_for_range = (exit_high - exit_low) / (level_count - 1)
            range_start = exit_high - grid_step_for_range
            range_end = exit_low - grid_step_for_range
            if float(exit_range_start) > float(exit_range_end):
                warnings.append(
                    "Exit range was reordered from high-to-low to low-to-high for current-position Grid setup."
                )
            imbalance_tolerance = self._auto_arb_completion_tolerance(
                {"chunk_qty": float(total_qty) / level_count}
            )
            if setup_mode == "adopt_existing_full_grid" and existing_hedged_qty:
                imbalance_tolerance = max(
                    imbalance_tolerance,
                    self._auto_arb_completion_tolerance(
                        {"chunk_qty": float(existing_hedged_qty)}
                    ),
                )
            if quantities and float(quantities.get("imbalance_qty") or 0.0) > imbalance_tolerance:
                warnings.append(
                    "Current long/short quantities are imbalanced; Live activation may be blocked until they match."
                )
        chunk_qty = float(total_qty) / level_count
        if (
            setup_mode == "adopt_existing_full_grid"
            and existing_position_fit is not None
            and existing_hedged_qty
            and not bool(existing_position_fit.get("adoption_exact"))
        ):
            partial_level = max(
                0,
                min(
                    int(level_count),
                    int(math.ceil(float(existing_hedged_qty) / chunk_qty)),
                ),
            )
            if partial_level > 0:
                existing_position_fit["closest_level"] = existing_position_fit.get("level")
                existing_position_fit["level"] = partial_level
                existing_position_fit["adoption_level"] = partial_level
                existing_position_fit["adoption_will_match"] = True
                existing_position_fit["adoption_partial"] = True
                existing_position_fit["chunk_qty"] = chunk_qty
                existing_position_fit["cumulative_qty"] = partial_level * chunk_qty
                warnings.append(
                    "Existing hedged quantity will be adopted as partial level "
                    f"{partial_level}/{level_count}; transitions will be sized from "
                    "the real current position."
                )
        grid_step = abs(float(range_start) - float(range_end)) / (level_count - 1)
        exit_gap = grid_step
        levels = build_grid_levels(
            range_start_pct=range_start,
            range_end_pct=range_end,
            level_count=level_count,
            exit_gap_pct=exit_gap,
            max_qty=total_qty,
        )
        for level in levels:
            qty = float(level.get("qty") or 0.0)
            cumulative_qty = float(level.get("cumulative_qty") or 0.0)
            level["chunk_notional_estimate"] = (
                round(qty * reference_price, 8) if reference_price else None
            )
            level["cumulative_notional_estimate"] = (
                round(cumulative_qty * reference_price, 8) if reference_price else None
            )
            level["entry_action"] = (
                f"BUY {long_exchange} / SELL {short_exchange}"
            )
            level["exit_action"] = (
                f"SELL {long_exchange} / BUY {short_exchange}"
            )
            level["entry_condition"] = (
                f"entry spread <= {float(level['entry_spread_pct']):g}%"
            )
            level["exit_condition"] = (
                f"exit spread >= {float(level['exit_spread_pct']):g}%"
            )
        if not safe_candidates:
            warnings.append("Dry run did not return a safe chunk; budget/count fallback was used.")

        config = {
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "direction": "long_spread",
            "setup_mode": setup_mode,
            "budget_mode": budget_mode,
            "max_qty": float(total_qty),
            "max_notional": float(max_notional) if max_notional else None,
            "exit_range_start_pct": float(exit_range_start) if exit_range_start is not None else None,
            "exit_range_end_pct": float(exit_range_end) if exit_range_end is not None else None,
            "exit_range_low_pct": (
                min(float(exit_range_start), float(exit_range_end))
                if exit_range_start is not None and exit_range_end is not None
                else None
            ),
            "exit_range_high_pct": (
                max(float(exit_range_start), float(exit_range_end))
                if exit_range_start is not None and exit_range_end is not None
                else None
            ),
            "range_start_pct": float(range_start),
            "range_end_pct": float(range_end),
            "level_count": level_count,
            "chunk_qty": chunk_qty,
            "reference_price": reference_price,
            "chunk_notional_estimate": (
                chunk_qty * reference_price if reference_price else None
            ),
            "total_notional_estimate": (
                float(total_qty) * reference_price if reference_price else None
            ),
            "exit_gap_pct": float(exit_gap),
            "exit_gap_mode": "arithmetic_grid_step",
            "grid_interval_count": level_count - 1,
            "max_slippage_bps": max_slippage_bps,
            "liquidity_safety_factor": liquidity_factor,
            "existing_position_fit": existing_position_fit,
            "confirm_samples": max(1, int(payload.get("confirm_samples") or 2)),
            "max_levels_per_cycle": 1,
            "levels": levels,
        }
        return {
            "status": "ok" if not warnings else "warning",
            "mode": "preview",
            "config": config,
            "live_spreads": live_spreads,
            "safe_chunk_qty": safe_chunk,
            "reference_price": reference_price,
            "grid_step_pct": grid_step,
            "warnings": warnings,
            "dry_run": plans,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def upsert_auto_arb_rule(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        requested_id = str(payload.get("id") or "").strip()
        if bool(payload.get("live")):
            async with self._auto_arb_lock:
                conflict = self._auto_arb_live_grid_conflict(
                    payload,
                    exclude_rule_id=requested_id,
                )
            if conflict is not None:
                raise ValueError(self._auto_arb_live_grid_conflict_message(conflict))
        if requested_id:
            async with self._auto_arb_lock:
                existing_rule = (self._auto_arb.get("rules") or {}).get(requested_id)
                if isinstance(existing_rule, Mapping) and (
                    existing_rule.get("mode") == "live"
                    or existing_rule.get("active_execution_id")
                ):
                    raise ValueError(
                        "Live grid reconfiguration is not available yet. "
                        "Switch it to Shadow first; the real position will remain unchanged."
                    )
        analysis = await self.analyze_auto_arb(payload)
        config = dict(analysis["config"])
        rule_id = requested_id or uuid4().hex[:12]
        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._auto_arb_lock:
            existing = (self._auto_arb.get("rules") or {}).get(rule_id) or {}
            generation = int(existing.get("generation") or 0) + 1
            rule = {
                **config,
                "id": rule_id,
                "version": 1,
                "generation": generation,
                "mode": "shadow",
                "enabled": bool(payload.get("enabled", True)),
                "status": "waiting_data",
                "blocked_reason": None,
                "shadow_level": int(existing.get("shadow_level") or 0),
                "shadow_qty": float(existing.get("shadow_qty") or 0.0),
                "live_level": int(existing.get("live_level") or 0),
                "actual_hedged_qty": float(existing.get("actual_hedged_qty") or 0.0),
                "active_execution_id": None,
                "active_action": None,
                "active_from_level": None,
                "active_to_level": None,
                "active_target_qty": None,
                "active_start_hedged_qty": None,
                "pending_transition": None,
                "adopted_level": int(existing.get("adopted_level") or 0),
                "adopted_qty": float(existing.get("adopted_qty") or 0.0),
                "adopted_at": existing.get("adopted_at"),
                "next_eligible_ts": 0.0,
                "pending_action": None,
                "pending_samples": 0,
                "last_decision": None,
                "live_entry_spread_pct": None,
                "live_exit_spread_pct": None,
                "created_at": existing.get("created_at") or now_iso,
                "updated_at": now_iso,
            }
            self._auto_arb.setdefault("rules", {})[rule_id] = rule
            self._save_auto_arb_config()
        self._auto_arb_history_store.append(
            {
                "event": "rule_upserted",
                "rule_id": rule_id,
                "generation": generation,
                "config": config,
                "ts": now_iso,
            }
        )
        return {"rule": rule, "analysis": analysis}

    async def set_auto_arb_rule_enabled(self, rule_id: str, enabled: bool) -> dict[str, Any]:
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                raise ValueError("Auto-arbitrage rule not found.")
            if not enabled and (
                rule.get("active_execution_id")
                or rule.get("transition_starting")
            ):
                raise ValueError("Wait for the active grid execution to finish before pausing.")
            if enabled and rule.get("mode") == "live":
                raise ValueError(
                    "Use 'Enable Live' again so positions and Live limits are revalidated."
                )
            rule["enabled"] = bool(enabled)
            rule["status"] = "waiting_data" if enabled else "paused"
            rule["pending_action"] = None
            rule["pending_samples"] = 0
            rule["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._save_auto_arb_config()
            result = dict(rule)
        self._auto_arb_history_store.append(
            {
                "event": "rule_resumed" if enabled else "rule_paused",
                "rule_id": rule_id,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {"rule": result}

    @staticmethod
    def _auto_arb_symbol_ownership_key(rule: Mapping[str, Any]) -> str:
        return grid_symbol_ownership_key(rule)

    @staticmethod
    def _auto_arb_rules_share_live_ownership(
        left: Mapping[str, Any],
        right: Mapping[str, Any],
    ) -> bool:
        return grid_rules_share_live_ownership(left, right)

    def _auto_arb_live_grid_conflict(
        self,
        rule: Mapping[str, Any],
        *,
        exclude_rule_id: str = "",
    ) -> Mapping[str, Any] | None:
        return grid_live_conflict(
            self._auto_arb.get("rules") or {},
            rule,
            exclude_rule_id=exclude_rule_id,
        )

    @staticmethod
    def _auto_arb_live_grid_conflict_message(conflict: Mapping[str, Any]) -> str:
        return grid_live_conflict_message(conflict)

    @staticmethod
    def _auto_arb_completion_tolerance(
        rule: Mapping[str, Any],
        target_qty: float | None = None,
    ) -> float:
        return grid_completion_tolerance(rule, target_qty)

    @classmethod
    def _auto_arb_transition_completion_tolerance(
        cls,
        rule: Mapping[str, Any],
        transition_qty: float | None = None,
    ) -> float:
        return grid_transition_completion_tolerance(rule, transition_qty)

    @classmethod
    def _auto_arb_hedge_imbalance_tolerance(
        cls,
        rule: Mapping[str, Any],
        *,
        transition_qty: float | None = None,
        hedged_qty: float | None = None,
    ) -> float:
        return grid_hedge_imbalance_tolerance(
            rule,
            transition_qty=transition_qty,
            hedged_qty=hedged_qty,
        )

    @staticmethod
    def _auto_arb_non_closeable_dust(
        result: Mapping[str, Any] | None,
        remaining_qty: float,
    ) -> bool:
        return grid_non_closeable_dust(result, remaining_qty)

    @staticmethod
    def _auto_arb_dust_only_errors(
        result: Mapping[str, Any] | None,
    ) -> bool:
        return grid_dust_only_errors(result)

    @classmethod
    def _auto_arb_reset_after_flat_repair(
        cls,
        rule: dict[str, Any],
        hedged_qty: float,
    ) -> bool:
        return grid_reset_after_flat_repair(rule, hedged_qty)

    @classmethod
    def _auto_arb_level_for_qty(
        cls,
        rule: Mapping[str, Any],
        hedged_qty: float,
    ) -> int | None:
        return grid_level_for_qty(rule, hedged_qty)

    @classmethod
    def _auto_arb_partial_adoption_level_for_qty(
        cls,
        rule: Mapping[str, Any],
        hedged_qty: float,
    ) -> int | None:
        return grid_partial_adoption_level_for_qty(rule, hedged_qty)

    @staticmethod
    def _auto_arb_level_count_for_existing_qty(
        *,
        total_qty: float,
        existing_qty: float,
        preferred_count: int,
    ) -> dict[str, Any] | None:
        return grid_level_count_for_existing_qty(
            total_qty=total_qty,
            existing_qty=existing_qty,
            preferred_count=preferred_count,
        )

    async def _auto_arb_refresh_quantities(self, rule: Mapping[str, Any]) -> dict[str, float]:
        refresh = getattr(self._accounts, "refresh_now_for_protective", None)
        if callable(refresh):
            await asyncio.wait_for(refresh(force_env=True), timeout=45.0)
        snapshot = self._accounts.snapshot() or {}
        return _position_pair_quantities(
            snapshot.get("positions") or [],
            symbol=str(rule.get("symbol") or ""),
            long_exchange=str(rule.get("long_exchange") or ""),
            short_exchange=str(rule.get("short_exchange") or ""),
        )

    async def _auto_arb_entry_risk_limit_preflight(
        self,
        rule: Mapping[str, Any],
        *,
        target_position_qty: float,
    ) -> dict[str, Any]:
        checker = getattr(self._manual, "entry_risk_limit_preflight", None)
        if not callable(checker):
            return {
                "ready": False,
                "checked": False,
                "reason": "risk_limit_preflight_unavailable",
                "errors": ["Grid risk-limit preflight is unavailable."],
            }
        result = await checker(
            symbol=str(rule.get("symbol") or ""),
            long_exchange=str(rule.get("long_exchange") or ""),
            short_exchange=str(rule.get("short_exchange") or ""),
            target_position_qty=max(0.0, float(target_position_qty or 0.0)),
            leverage=3.0,
            reference_price=None,
        )
        if not isinstance(result, Mapping):
            return {
                "ready": False,
                "checked": False,
                "reason": "risk_limit_preflight_invalid",
                "errors": ["Grid risk-limit preflight returned an invalid response."],
            }
        return dict(result)

    @staticmethod
    def _auto_arb_risk_limit_error(preflight: Mapping[str, Any]) -> str:
        errors = [str(item) for item in (preflight.get("errors") or []) if item]
        message = "; ".join(errors) or str(
            preflight.get("reason") or "risk_limit_preflight_failed"
        )
        required_level = preflight.get("required_level")
        required_limit = _safe_float(preflight.get("required_max_risk_limit_usd"))
        if required_level is not None and required_limit is not None:
            message += (
                f". KuCoin isolated level {int(required_level)} "
                f"({required_limit:g} USDT) or a smaller Grid is required"
            )
        if preflight.get("change_cancels_open_orders"):
            message += ". Changing the KuCoin level cancels open orders"
        return message

    async def arm_auto_arb_live(self, rule_id: str, confirmation: str) -> dict[str, Any]:
        expected_confirmation = f"LIVE {rule_id}"
        if str(confirmation or "").strip() != expected_confirmation:
            raise ValueError(f"Type '{expected_confirmation}' to enable Live mode.")
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                raise ValueError("Auto-arbitrage rule not found.")
            if rule.get("active_execution_id"):
                raise ValueError("The grid already has an active execution.")
            rule_copy = dict(rule)
            live_grid_conflict = self._auto_arb_live_grid_conflict(
                rule_copy,
                exclude_rule_id=rule_id,
            )

        if live_grid_conflict is not None:
            raise ValueError(
                self._auto_arb_live_grid_conflict_message(live_grid_conflict)
            )

        if self._running_manual_execution():
            raise ValueError("Another manual or automatic execution is currently running.")

        try:
            quantities = await self._auto_arb_refresh_quantities(rule_copy)
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError(f"Unable to refresh positions before Live activation: {exc}") from exc
        live_level = self._auto_arb_level_for_qty(
            rule_copy,
            float(quantities.get("hedged_qty") or 0.0),
        )
        if live_level is None and str(rule_copy.get("setup_mode") or "") == "adopt_existing_full_grid":
            live_level = self._auto_arb_partial_adoption_level_for_qty(
                rule_copy,
                float(quantities.get("hedged_qty") or 0.0),
            )
        if live_level is None:
            raise ValueError(
                "The existing hedged quantity does not match a grid level. "
                "Flatten it or configure a grid that matches the real position."
            )
        imbalance_qty = float(quantities.get("imbalance_qty") or 0.0)
        tolerance = self._auto_arb_completion_tolerance(rule_copy)
        if str(rule_copy.get("setup_mode") or "") == "adopt_existing_full_grid":
            tolerance = max(
                tolerance,
                self._auto_arb_completion_tolerance(
                    rule_copy,
                    float(quantities.get("hedged_qty") or 0.0),
                ),
            )
        if imbalance_qty > tolerance:
            raise ValueError(
                "Long and short quantities are imbalanced; Live Grid cannot take ownership."
            )

        risk_limit_preflight = await self._auto_arb_entry_risk_limit_preflight(
            rule_copy,
            target_position_qty=float(rule_copy.get("max_qty") or 0.0),
        )
        entry_blocked_reason = (
            None
            if bool(risk_limit_preflight.get("ready"))
            else self._auto_arb_risk_limit_error(risk_limit_preflight)
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                raise ValueError("Auto-arbitrage rule not found.")
            live_grid_conflict = self._auto_arb_live_grid_conflict(
                rule,
                exclude_rule_id=rule_id,
            )
            if live_grid_conflict is not None:
                raise ValueError(
                    self._auto_arb_live_grid_conflict_message(live_grid_conflict)
                )
            rule["mode"] = "live"
            rule["enabled"] = True
            rule["live_level"] = int(live_level)
            rule["actual_hedged_qty"] = float(quantities.get("hedged_qty") or 0.0)
            rule["adopted_level"] = int(live_level)
            rule["adopted_qty"] = float(quantities.get("hedged_qty") or 0.0)
            rule["adopted_at"] = now_iso
            rule["pending_transition"] = None
            rule["next_eligible_ts"] = 0.0
            rule["status"] = "waiting_entry" if live_level == 0 else "monitoring"
            rule["blocked_reason"] = None
            rule["pending_action"] = None
            rule["pending_samples"] = 0
            rule["risk_limit_preflight"] = risk_limit_preflight
            rule["entry_blocked_reason"] = entry_blocked_reason
            rule["entry_next_eligible_ts"] = 0.0
            rule["updated_at"] = now_iso
            self._save_auto_arb_config()
            result = dict(rule)
        self._auto_arb_history_store.append(
            {
                "event": "live_armed",
                "rule_id": rule_id,
                "generation": result.get("generation"),
                "live_level": live_level,
                "actual_hedged_qty": quantities.get("hedged_qty"),
                "entry_risk_limited": bool(entry_blocked_reason),
                "entry_blocked_reason": entry_blocked_reason,
                "ts": now_iso,
            }
        )
        return {"rule": result, "live_limits": self.auto_arb_payload()["live_limits"]}

    async def set_auto_arb_shadow(self, rule_id: str) -> dict[str, Any]:
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                raise ValueError("Auto-arbitrage rule not found.")
            if rule.get("active_execution_id"):
                raise ValueError("Wait for the active grid execution before switching to Shadow.")
            rule["mode"] = "shadow"
            rule["enabled"] = False
            rule["status"] = "paused"
            rule["pending_action"] = None
            rule["pending_samples"] = 0
            rule["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._save_auto_arb_config()
            result = dict(rule)
        self._auto_arb_history_store.append(
            {
                "event": "switched_to_shadow",
                "rule_id": rule_id,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {"rule": result}

    async def delete_auto_arb_rule(self, rule_id: str) -> dict[str, Any]:
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if rule is None:
                raise ValueError("Auto-arbitrage rule not found.")
            if rule.get("active_execution_id"):
                raise ValueError("Wait for the active grid execution before deleting the rule.")
            (self._auto_arb.get("rules") or {}).pop(rule_id, None)
            self._save_auto_arb_config()
        self._auto_arb_history_store.append(
            {
                "event": "rule_deleted",
                "rule_id": rule_id,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {"status": "deleted", "id": rule_id}

    def auto_arb_history(self, rule_id: str, limit: int = 100) -> dict[str, Any]:
        path = self._auto_arb_history_store.path
        rows: list[dict[str, Any]] = []
        if path.exists():
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                lines = []
            for line in reversed(lines):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(row.get("rule_id") or "") != rule_id:
                    continue
                rows.append(row)
                if len(rows) >= max(1, min(int(limit), 500)):
                    break
        return {"rule_id": rule_id, "events": list(reversed(rows))}

    async def _reconcile_auto_arb_execution(self, rule_id: str) -> bool:
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                return False
            exec_id = str(rule.get("active_execution_id") or "")
            if not exec_id:
                return True
            rule_copy = dict(rule)
        run = self._manual_runs.get(exec_id)
        if not isinstance(run, Mapping):
            repair_quantities: dict[str, float] | None = None
            reconcile_error = None
            quantities: dict[str, float] | None = None
            try:
                quantities = await self._auto_arb_refresh_quantities(rule_copy)
            except Exception as exc:  # pylint: disable=broad-except
                reconcile_error = str(exc)
            now_iso = datetime.now(timezone.utc).isoformat()
            event: dict[str, Any] = {
                "event": "live_execution_state_missing",
                "rule_id": rule_id,
                "execution_id": exec_id,
                "action": rule_copy.get("active_action"),
                "ts": now_iso,
            }
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    missing_reduced = reduce_missing_grid_execution(
                        current,
                        execution_id=exec_id,
                        quantities=quantities,
                        reconcile_error=reconcile_error,
                        now_iso=now_iso,
                        now_ts=time.time(),
                        retry_sec=AUTO_ARB_RETRY_SEC,
                    )
                    event = dict(missing_reduced["event"])
                    if missing_reduced["repair_required"] and quantities is not None:
                        repair_quantities = dict(quantities)
                    current["updated_at"] = now_iso
                    self._save_auto_arb_config()
            self._auto_arb_history_store.append(event)
            if repair_quantities is not None:
                await self._start_auto_arb_hedge_repair(rule_id, repair_quantities)
            return False
        status = str(run.get("status") or "")
        if status == "running":
            return False

        now_iso = datetime.now(timezone.utc).isoformat()
        event: dict[str, Any] = {
            "event": "live_execution_reconciled",
            "rule_id": rule_id,
            "execution_id": exec_id,
            "execution_status": status,
            "action": rule_copy.get("active_action"),
            "from_level": rule_copy.get("active_from_level"),
            "to_level": rule_copy.get("active_to_level"),
            "ts": now_iso,
        }
        quantities: dict[str, float] | None = None
        reconcile_error = None
        try:
            quantities = await self._auto_arb_refresh_quantities(rule_copy)
        except Exception as exc:  # pylint: disable=broad-except
            reconcile_error = str(exc)

        repair_quantities: dict[str, float] | None = None
        completed = False
        async with self._auto_arb_lock:
            current = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(current, dict):
                return False
            active_action = str(current.get("active_action") or "")
            current["active_execution_id"] = None
            current["active_action"] = None
            current["active_from_level"] = None
            current["active_to_level"] = None
            current["active_target_qty"] = None
            start_hedged_qty = _safe_float(current.get("active_start_hedged_qty"))
            current["active_start_hedged_qty"] = None
            if quantities is not None:
                hedged_qty = float(quantities.get("hedged_qty") or 0.0)
                imbalance_qty = float(quantities.get("imbalance_qty") or 0.0)
                transition = dict(current.get("pending_transition") or {})
                total_transition_qty = max(
                    0.0,
                    float(transition.get("target_qty") or 0.0),
                )
                hedge_tolerance = self._auto_arb_hedge_imbalance_tolerance(
                    current,
                    transition_qty=total_transition_qty or None,
                    hedged_qty=hedged_qty,
                )
                current["actual_hedged_qty"] = hedged_qty
                current["last_execution"] = {
                    "execution_id": exec_id,
                    "status": status,
                    "error": run.get("error"),
                    "result": run.get("result"),
                    "observed_hedged_qty": hedged_qty,
                    "observed_imbalance_qty": imbalance_qty,
                    "reconciled_at": now_iso,
                }
                if active_action == "repair":
                    if imbalance_qty <= hedge_tolerance:
                        flat_repair_reset = self._auto_arb_reset_after_flat_repair(
                            current,
                            hedged_qty,
                        )
                        if flat_repair_reset:
                            current["status"] = "waiting_entry"
                        else:
                            current["status"] = (
                                f"partial_{transition.get('action')}"
                                if transition
                                else (
                                    "waiting_entry"
                                    if not current.get("live_level")
                                    else "monitoring"
                                )
                            )
                        current["blocked_reason"] = None
                        current["next_eligible_ts"] = time.time() + AUTO_ARB_RETRY_SEC
                        event["event"] = "live_hedge_repaired"
                        event["actual_hedged_qty"] = hedged_qty
                        event["imbalance_qty"] = imbalance_qty
                        event["flat_repair_reset"] = flat_repair_reset
                        completed = True
                    else:
                        current["status"] = "hedge_repair_retry"
                        run_result = run.get("result")
                        result_errors = []
                        if isinstance(run_result, Mapping):
                            result_errors = [
                                str(item) for item in (run_result.get("errors") or [])
                            ]
                        repair_error = str(run.get("error") or "") or "; ".join(result_errors)
                        current["blocked_reason"] = (
                            repair_error or "hedge_imbalance_above_tolerance"
                        )
                        current["next_eligible_ts"] = time.time() + AUTO_ARB_RETRY_SEC
                        if status == "completed" or not repair_error:
                            repair_quantities = dict(quantities)
                        event["event"] = "live_hedge_repair_partial"
                        event["imbalance_qty"] = imbalance_qty
                        if repair_error:
                            event["error"] = repair_error
                elif transition:
                    transition_reduced = reduce_grid_transition_execution(
                        current,
                        transition=transition,
                        active_action=active_action,
                        execution_id=exec_id,
                        execution_status=status,
                        execution_error=run.get("error"),
                        execution_result=run.get("result"),
                        hedged_qty=hedged_qty,
                        imbalance_qty=imbalance_qty,
                        start_hedged_qty=start_hedged_qty,
                        now_iso=now_iso,
                        now_ts=time.time(),
                        retry_sec=AUTO_ARB_RETRY_SEC,
                    )
                    event.update(transition_reduced["event"])
                    if transition_reduced["repair_required"]:
                        repair_quantities = dict(quantities)
                    completed = bool(transition_reduced["completed"])
                else:
                    current["actual_hedged_qty"] = hedged_qty
                    current["status"] = "monitoring"
                    current["blocked_reason"] = None
                    completed = True
                if (
                    not current.get("enabled")
                    and imbalance_qty <= hedge_tolerance
                    and repair_quantities is None
                ):
                    current["status"] = "paused"
                    current["blocked_reason"] = None
                    current["next_eligible_ts"] = 0.0
            else:
                current["active_execution_id"] = exec_id
                current["active_action"] = rule_copy.get("active_action")
                current["active_from_level"] = rule_copy.get("active_from_level")
                current["active_to_level"] = rule_copy.get("active_to_level")
                current["active_target_qty"] = rule_copy.get("active_target_qty")
                current["active_start_hedged_qty"] = rule_copy.get(
                    "active_start_hedged_qty"
                )
                current["status"] = "waiting_reconcile"
                current["blocked_reason"] = (
                    f"position_refresh_failed: {reconcile_error}"
                    if reconcile_error
                    else f"execution_{status or 'unknown'}"
                )
                current["next_eligible_ts"] = time.time() + 30.0
                event["event"] = "live_reconcile_deferred"
                event["error"] = reconcile_error or run.get("error")
                event["result"] = run.get("result")
            current["updated_at"] = now_iso
            self._save_auto_arb_config()
        self._auto_arb_history_store.append(event)
        if repair_quantities is not None:
            await self._start_auto_arb_hedge_repair(rule_id, repair_quantities)
        return completed

    async def _start_auto_arb_hedge_repair(
        self,
        rule_id: str,
        quantities: Mapping[str, Any],
    ) -> None:
        if self._running_manual_execution():
            return
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                return
            if not rule.get("enabled") and str(rule.get("status") or "") not in {
                "hedge_repair_required",
                "hedge_repair_retry",
            }:
                return
            rule_copy = dict(rule)
        long_qty = float(quantities.get("long_qty") or 0.0)
        short_qty = float(quantities.get("short_qty") or 0.0)
        imbalance_qty = abs(long_qty - short_qty)
        tolerance = self._auto_arb_hedge_imbalance_tolerance(
            rule_copy,
            hedged_qty=float(quantities.get("hedged_qty") or 0.0),
        )
        if imbalance_qty <= tolerance:
            now_iso = datetime.now(timezone.utc).isoformat()
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if not isinstance(current, dict):
                    return
                transition = dict(current.get("pending_transition") or {})
                remaining_qty = max(0.0, float(transition.get("remaining_qty") or 0.0))
                transition_action = str(transition.get("action") or "")
                transition_tolerance = self._auto_arb_transition_completion_tolerance(
                    current,
                    float(transition.get("target_qty") or 0.0) or None,
                )
                last_execution = current.get("last_execution")
                last_result = (
                    last_execution.get("result")
                    if isinstance(last_execution, Mapping)
                    else None
                )
                non_closeable_dust = (
                    bool(transition)
                    and remaining_qty > 0
                    and self._auto_arb_non_closeable_dust(
                        last_result if isinstance(last_result, Mapping) else None,
                        remaining_qty,
                    )
                )
                hedged_qty = float(quantities.get("hedged_qty") or 0.0)
                flat_repair_reset = self._auto_arb_reset_after_flat_repair(
                    current,
                    hedged_qty,
                )
                if flat_repair_reset:
                    current["status"] = "waiting_entry"
                elif transition and (remaining_qty <= transition_tolerance or non_closeable_dust):
                    target_level = int(
                        transition.get("to_level")
                        or current.get("live_level")
                        or 0
                    )
                    current["live_level"] = target_level
                    current["pending_transition"] = None
                    current["status"] = "waiting_entry" if target_level == 0 else "monitoring"
                elif transition:
                    current["status"] = f"partial_{transition_action or 'transition'}"
                else:
                    current["status"] = (
                        "waiting_entry" if not current.get("live_level") else "monitoring"
                    )
                current["active_execution_id"] = None
                current["active_action"] = None
                current["active_from_level"] = None
                current["active_to_level"] = None
                current["active_target_qty"] = None
                current["active_start_hedged_qty"] = None
                current["actual_hedged_qty"] = hedged_qty
                current["blocked_reason"] = None
                current["pending_action"] = None
                current["pending_samples"] = 0
                current["next_eligible_ts"] = time.time() + AUTO_ARB_RETRY_SEC
                current["updated_at"] = now_iso
                self._save_auto_arb_config()
            self._auto_arb_history_store.append(
                {
                    "event": "live_hedge_imbalance_within_tolerance",
                    "rule_id": rule_id,
                    "imbalance_qty": imbalance_qty,
                    "tolerance_qty": tolerance,
                    "remaining_qty": remaining_qty,
                    "non_closeable_dust_completed": bool(non_closeable_dust),
                    "flat_repair_reset": flat_repair_reset,
                    "ts": now_iso,
                }
            )
            return
        cleanup_long = long_qty > short_qty
        cleanup_exchange = (
            rule_copy.get("long_exchange")
            if cleanup_long
            else rule_copy.get("short_exchange")
        )
        cleanup_side = "long" if cleanup_long else "short"
        close_side = "sell" if cleanup_side == "long" else "buy"
        preflight: dict[str, Any] = {}
        try:
            preflight = await self._manual.analyze_rebalance(
                exchange=str(cleanup_exchange or ""),
                symbol=str(rule_copy.get("symbol") or ""),
                side=close_side,
                qty_base=imbalance_qty,
                max_slippage_bps=float(rule_copy.get("max_slippage_bps") or 8.0),
            )
        except Exception as exc:  # pylint: disable=broad-except
            preflight = {"errors": [str(exc)]}
        min_required = _safe_float(preflight.get("min_qty_required"))
        if min_required and imbalance_qty < min_required:
            now_iso = datetime.now(timezone.utc).isoformat()
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if not isinstance(current, dict):
                    return
                transition = dict(current.get("pending_transition") or {})
                target_level = int(
                    transition.get("to_level")
                    or current.get("live_level")
                    or 0
                )
                current["active_execution_id"] = None
                current["active_action"] = None
                current["active_from_level"] = None
                current["active_to_level"] = None
                current["active_target_qty"] = None
                current["active_start_hedged_qty"] = None
                current["actual_hedged_qty"] = float(quantities.get("hedged_qty") or 0.0)
                current["live_level"] = target_level
                current["pending_transition"] = None
                current["pending_action"] = None
                current["pending_samples"] = 0
                current["status"] = "waiting_entry" if target_level == 0 else "monitoring"
                current["blocked_reason"] = None
                current["next_eligible_ts"] = time.time() + AUTO_ARB_RETRY_SEC
                current["updated_at"] = now_iso
                self._save_auto_arb_config()
            self._auto_arb_history_store.append(
                {
                    "event": "live_hedge_repair_non_closeable_dust",
                    "rule_id": rule_id,
                    "live_level": target_level,
                    "cleanup_exchange": cleanup_exchange,
                    "cleanup_side": cleanup_side,
                    "imbalance_qty": imbalance_qty,
                    "min_qty_required": min_required,
                    "preflight": preflight,
                    "ts": now_iso,
                }
            )
            return
        payload = {
            "symbol": rule_copy.get("symbol"),
            "qty": imbalance_qty,
            "cleanup_exchange": cleanup_exchange,
            "cleanup_position_side": cleanup_side,
            "panic_cleanup_mode": False,
            "max_slippage_bps": float(rule_copy.get("max_slippage_bps") or 8.0),
            "max_runtime_sec": 120,
            "reprice_sec": 4.0,
            "use_orderbook_check": True,
            "fallback_to_market": False,
            "async_run": True,
            "dry_run": False,
            "margin_mode": "isolated",
            "auto_arb_agent": True,
            "auto_arb_rule_id": rule_id,
            "auto_arb_rule_generation": int(rule_copy.get("generation") or 0),
        }
        result = await self.manual_orphan_cleanup(payload)
        exec_id = str((result or {}).get("execution_id") or "")
        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._auto_arb_lock:
            current = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(current, dict):
                return
            if exec_id:
                current["active_execution_id"] = exec_id
                current["active_action"] = "repair"
                current["active_start_hedged_qty"] = float(
                    quantities.get("hedged_qty") or 0.0
                )
                current["status"] = "repairing_hedge"
                current["blocked_reason"] = None
            else:
                current["status"] = "hedge_repair_retry"
                current["blocked_reason"] = str(
                    (result or {}).get("error") or "hedge_repair_worker_busy"
                )
                current["next_eligible_ts"] = time.time() + AUTO_ARB_RETRY_SEC
            current["updated_at"] = now_iso
            self._save_auto_arb_config()
        self._auto_arb_history_store.append(
            {
                "event": "live_hedge_repair_started" if exec_id else "live_hedge_repair_deferred",
                "rule_id": rule_id,
                "execution_id": exec_id or None,
                "cleanup_exchange": cleanup_exchange,
                "cleanup_side": cleanup_side,
                "qty": imbalance_qty,
                "result": result,
                "ts": now_iso,
            }
        )

    async def _start_auto_arb_live_transition(
        self,
        rule_id: str,
        action: str,
        from_level: int,
        to_level: int,
    ) -> None:
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict) or not rule.get("enabled") or rule.get("mode") != "live":
                return
            rule_copy = dict(rule)
            live_grid_conflict = self._auto_arb_live_grid_conflict(
                rule_copy,
                exclude_rule_id=rule_id,
            )
            if live_grid_conflict is not None:
                rule["status"] = "blocked_conflict"
                rule["blocked_reason"] = (
                    f"matching_live_grid_rule:{live_grid_conflict.get('id')}"
                )
                rule["pending_action"] = None
                rule["pending_samples"] = 0
                rule["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._save_auto_arb_config()
                return
        running = self._running_manual_execution()
        if running:
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    current["status"] = "blocked_conflict"
                    current["blocked_reason"] = (
                        f"execution_running:{running.get('execution_id')}"
                    )
                    current["pending_action"] = None
                    current["pending_samples"] = 0
                    current["updated_at"] = datetime.now(timezone.utc).isoformat()
                    self._save_auto_arb_config()
            return
        levels = rule_copy.get("levels") or []
        level_index = to_level - 1 if action == "enter" else from_level - 1
        if level_index < 0 or level_index >= len(levels):
            raise ValueError("Grid transition level is outside the configured range.")
        level_qty = float(levels[level_index].get("qty") or 0.0)
        level_target_qty = (
            float(levels[to_level - 1].get("cumulative_qty") or 0.0)
            if to_level > 0
            else 0.0
        )
        try:
            quantities = await self._auto_arb_refresh_quantities(rule_copy)
        except Exception as exc:  # pylint: disable=broad-except
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    current["status"] = "waiting_positions"
                    current["blocked_reason"] = f"position_refresh_failed: {exc}"
                    current["next_eligible_ts"] = time.time() + 30.0
                    current["updated_at"] = datetime.now(timezone.utc).isoformat()
                    self._save_auto_arb_config()
            return
        current_hedged_qty = float(quantities.get("hedged_qty") or 0.0)
        pending_built = build_grid_pending_transition(
            existing_transition=(rule_copy.get("pending_transition") or {}),
            action=action,
            from_level=from_level,
            to_level=to_level,
            level_qty=level_qty,
            level_target_qty=level_target_qty,
            current_hedged_qty=current_hedged_qty,
            now_iso=datetime.now(timezone.utc).isoformat(),
        )
        transition = dict(pending_built["transition"])
        qty = float(pending_built["qty"])
        total_transition_qty = float(pending_built["total_transition_qty"])
        position_target_qty = float(pending_built["position_target_qty"])
        tolerance = self._auto_arb_transition_completion_tolerance(
            rule_copy,
            total_transition_qty,
        )
        if qty <= tolerance:
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    current["live_level"] = to_level
                    current["pending_transition"] = None
                    current["actual_hedged_qty"] = current_hedged_qty
                    current["status"] = "waiting_entry" if to_level == 0 else "monitoring"
                    current["blocked_reason"] = None
                    current["updated_at"] = datetime.now(timezone.utc).isoformat()
                    self._save_auto_arb_config()
            return
        if action == "enter":
            risk_limit_preflight = await self._auto_arb_entry_risk_limit_preflight(
                rule_copy,
                target_position_qty=current_hedged_qty + qty,
            )
            if not bool(risk_limit_preflight.get("ready")):
                now_iso = datetime.now(timezone.utc).isoformat()
                blocked_reason = self._auto_arb_risk_limit_error(
                    risk_limit_preflight
                )
                async with self._auto_arb_lock:
                    current = (self._auto_arb.get("rules") or {}).get(rule_id)
                    if isinstance(current, dict):
                        current["status"] = "blocked_risk_limit"
                        current["blocked_reason"] = blocked_reason
                        current["entry_blocked_reason"] = blocked_reason
                        current["risk_limit_preflight"] = risk_limit_preflight
                        current["pending_action"] = None
                        current["pending_samples"] = 0
                        current["entry_next_eligible_ts"] = time.time() + 300.0
                        current["updated_at"] = now_iso
                        self._save_auto_arb_config()
                self._auto_arb_history_store.append(
                    {
                        "event": "live_entry_risk_limit_blocked",
                        "rule_id": rule_id,
                        "from_level": from_level,
                        "to_level": to_level,
                        "target_position_qty": current_hedged_qty + qty,
                        "risk_limit_preflight": risk_limit_preflight,
                        "ts": now_iso,
                    }
                )
                return
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    current["risk_limit_preflight"] = risk_limit_preflight
                    current["entry_blocked_reason"] = None
                    current["entry_next_eligible_ts"] = 0.0
        worst_tier = max(
            venue_liquidity_tier(str(rule_copy.get("long_exchange") or "")),
            venue_liquidity_tier(str(rule_copy.get("short_exchange") or "")),
        )
        chunk_notional = 750.0 if worst_tier <= 1 else 500.0 if worst_tier == 2 else 250.0
        trigger_level = levels[level_index]
        payload = {
            "symbol": rule_copy.get("symbol"),
            "qty": qty,
            "notional": None,
            "mode": "smart-enter" if action == "enter" else "smart-exit",
            "max_slippage_bps": float(rule_copy.get("max_slippage_bps") or 8.0),
            "spread_min_pct": (
                -100.0
                if action == "enter"
                else float(
                    transition.get("spread_min_pct")
                    if transition.get("spread_min_pct") is not None
                    else trigger_level.get("exit_spread_pct")
                )
            ),
            "spread_max_pct": (
                float(trigger_level.get("entry_spread_pct")) if action == "enter" else 100.0
            ),
            "timeout_sec": 0,
            "max_runtime_sec": 120,
            "reprice_sec": 5.0,
            "chunk_qty": None,
            "chunk_notional": chunk_notional,
            "force_chunk_qty": False,
            "use_orderbook_check": True,
            "allow_liquidity_chunking": True,
            "fallback_to_market": False,
            "hedge_order_type": "limit",
            "hedge_limit_mode": "passive" if action == "enter" else "aggressive",
            "hedge_favorable_bps": 2.0,
            "hedge_adverse_bps": 8.0,
            "hedge_reprice_min_sec": 3.0 if action == "exit" else 5.0,
            "async_run": True,
            "dry_run": False,
            "long_exchange": rule_copy.get("long_exchange"),
            "short_exchange": rule_copy.get("short_exchange"),
            "margin_mode": "isolated",
            "auto_arb_agent": True,
            "auto_arb_rule_id": rule_id,
            "auto_arb_rule_generation": int(rule_copy.get("generation") or 0),
        }
        async with self._auto_arb_lock:
            current = (self._auto_arb.get("rules") or {}).get(rule_id)
            if (
                not isinstance(current, dict)
                or not current.get("enabled")
                or current.get("mode") != "live"
                or int(current.get("generation") or 0)
                != int(rule_copy.get("generation") or 0)
            ):
                if isinstance(current, dict) and not current.get("enabled"):
                    current["status"] = "paused"
                    current["pending_action"] = None
                    current["pending_samples"] = 0
                    current["updated_at"] = datetime.now(timezone.utc).isoformat()
                    self._save_auto_arb_config()
                return
            current["transition_starting"] = True
        try:
            result = (
                await self.manual_enter(payload)
                if action == "enter"
                else await self.manual_exit(payload)
            )
        except Exception:
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    current.pop("transition_starting", None)
            raise
        exec_id = str((result or {}).get("execution_id") or "")
        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._auto_arb_lock:
            current = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(current, dict):
                return
            current.pop("transition_starting", None)
            current["pending_action"] = None
            current["pending_samples"] = 0
            if exec_id:
                transition["last_start_hedged_qty"] = float(
                    quantities.get("hedged_qty") or 0.0
                )
                transition["updated_at"] = now_iso
                current["pending_transition"] = transition
                current["active_execution_id"] = exec_id
                current["active_action"] = action
                current["active_from_level"] = from_level
                current["active_to_level"] = to_level
                current["active_target_qty"] = position_target_qty
                current["active_start_hedged_qty"] = float(
                    quantities.get("hedged_qty") or 0.0
                )
                current["status"] = f"executing_{action}"
                current["blocked_reason"] = None
            else:
                current["pending_transition"] = transition
                current["status"] = "blocked_conflict"
                current["blocked_reason"] = str(
                    (result or {}).get("error") or "execution_worker_busy"
                )
                current["next_eligible_ts"] = time.time() + AUTO_ARB_RETRY_SEC
            current["updated_at"] = now_iso
            self._save_auto_arb_config()
        self._auto_arb_history_store.append(
            {
                "event": f"live_{action}_started" if exec_id else "live_start_failed",
                "rule_id": rule_id,
                "execution_id": exec_id or None,
                "from_level": from_level,
                "to_level": to_level,
                "qty": qty,
                "liquidity_chunking": True,
                "result": result,
                "ts": now_iso,
            }
        )

    async def _auto_arb_cycle(self) -> None:
        async with self._auto_arb_lock:
            rules = [
                dict(rule)
                for rule in (self._auto_arb.get("rules") or {}).values()
                if isinstance(rule, dict)
                and (rule.get("enabled") or rule.get("active_execution_id"))
            ]
        for rule in rules:
            rule_id = str(rule.get("id") or "")
            if rule.get("mode") == "live" and rule.get("active_execution_id"):
                if (
                    str(rule.get("status") or "") == "waiting_reconcile"
                    and time.time() < float(rule.get("next_eligible_ts") or 0.0)
                ):
                    continue
                await self._reconcile_auto_arb_execution(rule_id)
                continue
            if rule.get("mode") == "live":
                async with self._auto_arb_lock:
                    current = (self._auto_arb.get("rules") or {}).get(rule_id)
                    live_grid_conflict = (
                        self._auto_arb_live_grid_conflict(
                            current,
                            exclude_rule_id=rule_id,
                        )
                        if isinstance(current, Mapping)
                        else None
                    )
                    if isinstance(current, dict) and live_grid_conflict is not None:
                        conflict_reason = (
                            f"matching_live_grid_rule:{live_grid_conflict.get('id')}"
                        )
                        changed = (
                            current.get("status") != "blocked_conflict"
                            or current.get("blocked_reason") != conflict_reason
                        )
                        current["status"] = "blocked_conflict"
                        current["blocked_reason"] = conflict_reason
                        current["pending_action"] = None
                        current["pending_samples"] = 0
                        current["updated_at"] = datetime.now(timezone.utc).isoformat()
                        self._save_auto_arb_config()
                    else:
                        changed = False
                if live_grid_conflict is not None:
                    if changed:
                        self._auto_arb_history_store.append(
                            {
                                "event": "live_grid_conflict_blocked",
                                "rule_id": rule_id,
                                "conflicting_rule_id": live_grid_conflict.get("id"),
                                "symbol": rule.get("symbol"),
                                "ts": datetime.now(timezone.utc).isoformat(),
                            }
                        )
                    continue
            if time.time() < float(rule.get("next_eligible_ts") or 0.0):
                continue
            if str(rule.get("status") or "") in {
                "hedge_repair_required",
                "hedge_repair_retry",
            }:
                try:
                    quantities = await self._auto_arb_refresh_quantities(rule)
                except Exception:  # pylint: disable=broad-except
                    continue
                await self._start_auto_arb_hedge_repair(rule_id, quantities)
                continue
            live_spreads = await self.auto_arb_spreads(
                symbol=str(rule.get("symbol") or ""),
                long_exchange=str(rule.get("long_exchange") or ""),
                short_exchange=str(rule.get("short_exchange") or ""),
            )
            entry_spread = _safe_float(live_spreads.get("entry_spread_pct"))
            exit_spread = _safe_float(live_spreads.get("exit_spread_pct"))
            now_iso = datetime.now(timezone.utc).isoformat()
            transition_event = None
            live_transition = None
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if not isinstance(current, dict) or not current.get("enabled"):
                    continue
                current["live_entry_spread_pct"] = entry_spread
                current["live_exit_spread_pct"] = exit_spread
                current["last_quote_at"] = now_iso
                if entry_spread is None or exit_spread is None:
                    current["status"] = "waiting_data"
                    current["blocked_reason"] = "entry_or_exit_spread_unavailable"
                    current["pending_action"] = None
                    current["pending_samples"] = 0
                else:
                    mode = str(current.get("mode") or "shadow")
                    current_level = (
                        int(current.get("live_level") or 0)
                        if mode == "live"
                        else int(current.get("shadow_level") or 0)
                    )
                    pending_transition = (
                        dict(current.get("pending_transition") or {})
                        if mode == "live"
                        else {}
                    )
                    if pending_transition:
                        pending_remaining = max(
                            0.0,
                            float(pending_transition.get("remaining_qty") or 0.0),
                        )
                        pending_tolerance = self._auto_arb_transition_completion_tolerance(
                            current,
                            float(pending_transition.get("target_qty") or 0.0) or None,
                        )
                        pending_filled = max(
                            0.0,
                            float(pending_transition.get("filled_qty") or 0.0),
                        )
                        last_execution = current.get("last_execution")
                        last_result = (
                            last_execution.get("result")
                            if isinstance(last_execution, Mapping)
                            else None
                        )
                        completed_transition = complete_pending_grid_transition(
                            current,
                            pending_transition=pending_transition,
                            current_level=current_level,
                            last_result=(
                                last_result if isinstance(last_result, Mapping) else None
                            ),
                            now_iso=now_iso,
                            now_ts=time.time(),
                            retry_sec=AUTO_ARB_RETRY_SEC,
                        )
                        if completed_transition:
                            current_level = int(completed_transition["current_level"])
                            decision = dict(completed_transition["decision"])
                            transition_event = dict(
                                completed_transition["transition_event"]
                            )
                            pending_transition = dict(
                                completed_transition["pending_transition"]
                            )
                        else:
                            partial_reduced = reduce_partial_grid_transition(
                                current,
                                pending_transition=pending_transition,
                                current_level=current_level,
                                entry_spread_pct=entry_spread,
                                exit_spread_pct=exit_spread,
                                now_iso=now_iso,
                            )
                            decision = dict(partial_reduced["decision"])
                            transition_event = (
                                dict(partial_reduced["transition_event"])
                                if partial_reduced.get("transition_event")
                                else None
                            )
                            pending_transition = dict(
                                partial_reduced["pending_transition"]
                            )
                    else:
                        decision = decide_grid_transition(
                            entry_spread_pct=entry_spread,
                            exit_spread_pct=exit_spread,
                            levels=current.get("levels") or [],
                            current_level=current_level,
                            max_levels_per_cycle=current.get("max_levels_per_cycle") or 1,
                        )
                    reduced = apply_grid_decision_confirmation(
                        current,
                        decision=decision,
                        mode=mode,
                        current_level=current_level,
                        pending_transition=(
                            dict(pending_transition) if pending_transition else None
                        ),
                        entry_spread_pct=entry_spread,
                        exit_spread_pct=exit_spread,
                        now_iso=now_iso,
                        now_ts=time.time(),
                    )
                    action = str(reduced["action"])
                    if reduced.get("live_transition"):
                        live_transition = tuple(reduced["live_transition"])
                    if reduced.get("transition_event"):
                        transition_event = dict(reduced["transition_event"])
                current["updated_at"] = now_iso
                self._save_auto_arb_config()
            if transition_event:
                self._auto_arb_history_store.append(transition_event)
            if live_transition:
                await self._start_auto_arb_live_transition(*live_transition)
                if self._running_manual_execution():
                    break

    async def _auto_arb_scheduler(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(1.0, float(self._auto_arb_poll_sec)))
                try:
                    await self._auto_arb_cycle()
                except Exception:  # pylint: disable=broad-except
                    logger.exception("Auto-arbitrage shadow cycle failed")
        except asyncio.CancelledError:
            raise

    async def manual_analyze(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload)
        payload.setdefault(
            "constraints_exchanges",
            self._manual_pair_constraints(payload, action=payload.get("action") or "enter"),
        )
        return await self._manual.analyze(payload)

    async def position_action(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = normalize_symbol(str(payload.get("symbol") or "")).upper()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        action = str(payload.get("action") or "").strip().lower()
        percent = _safe_float(payload.get("percent"))
        dry_run = bool(payload.get("dry_run", True))
        async_run = bool(payload.get("async_run", True))
        max_runtime_sec = int(
            _safe_float(payload.get("max_runtime_sec"))
            or 300
        )
        if not symbol or not long_exchange or not short_exchange:
            raise ValueError("symbol, long_exchange, and short_exchange are required.")
        if action not in {"add", "exit"}:
            raise ValueError("action must be 'add' or 'exit'.")
        if percent is None or percent <= 0 or percent > 100:
            raise ValueError("percent must be greater than 0 and no more than 100.")

        refresh = getattr(self._accounts, "refresh_now_for_protective", None)
        if callable(refresh):
            try:
                await asyncio.wait_for(refresh(force_env=True), timeout=45.0)
            except Exception as exc:  # pylint: disable=broad-except
                raise ValueError(f"Unable to refresh positions before action: {exc}") from exc
        account_snapshot = self._accounts.snapshot() or {}
        quantities = _position_pair_quantities(
            account_snapshot.get("positions") or [],
            symbol=symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
        )
        hedged_qty = float(quantities.get("hedged_qty") or 0.0)
        if hedged_qty <= 0:
            raise ValueError(
                f"No hedged pair found for {symbol}: "
                f"{long_exchange} long={quantities.get('long_qty', 0.0):g}, "
                f"{short_exchange} short={quantities.get('short_qty', 0.0):g}."
            )
        action_qty = hedged_qty * float(percent) / 100.0
        context = {
            "symbol": symbol,
            "action": action,
            "percent": float(percent),
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            **quantities,
            "action_qty": float(action_qty),
            "quantity_basis": "min_long_short_coin_qty",
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        manual_payload = {
            "symbol": symbol,
            "qty": float(action_qty),
            "notional": None,
            "mode": "smart-enter" if action == "add" else "smart-exit",
            "max_slippage_bps": 12.0 if action == "add" else 8.0,
            "timeout_sec": 15,
            "max_runtime_sec": max_runtime_sec,
            "reprice_sec": 5.0,
            "chunk_qty": None,
            "chunk_notional": None,
            "force_chunk_qty": False,
            "use_orderbook_check": True,
            "allow_liquidity_chunking": True,
            "fallback_to_market": False,
            "exit_close_full_pair": bool(action == "exit" and float(percent) >= 100.0),
            "exit_dust_max_legs": 2,
            "async_run": bool(async_run and not dry_run),
            "dry_run": dry_run,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "margin_mode": "isolated",
        }
        result = (
            await self.manual_enter(manual_payload)
            if action == "add"
            else await self.manual_exit(manual_payload)
        )
        output = dict(result or {})
        output["position_action"] = context
        return output

    def _manual_pair_constraints(self, payload: Mapping[str, Any], *, action: str) -> list[str]:
        exchanges: list[str] = []
        if action == "roll":
            exchanges.extend(
                [
                    normalize_exchange_name(str(payload.get("from_exchange") or "")),
                    normalize_exchange_name(str(payload.get("to_exchange") or "")),
                ]
            )
        else:
            exchanges.extend(
                [
                    normalize_exchange_name(str(payload.get("long_exchange") or "")),
                    normalize_exchange_name(str(payload.get("short_exchange") or "")),
                ]
            )
        seen: set[str] = set()
        unique: list[str] = []
        for name in exchanges:
            if not name or name in seen:
                continue
            seen.add(name)
            unique.append(name)
        return unique

    def _funding_raw_payload(
        self,
        exchange: str,
        symbol: str,
        exchange_symbol: str,
        adapter: Any,
        *,
        history_limit: int = 12,
    ) -> dict[str, Any]:
        name = normalize_exchange_name(exchange)
        raw: dict[str, Any] = {
            "exchange": name,
            "symbol": symbol,
            "exchange_symbol": exchange_symbol,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "snapshot": [],
            "history": [],
        }

        def add_entry(
            target: str,
            label: str,
            url: str,
            *,
            payload: Any = None,
            error: str | None = None,
            filtered: bool = False,
            note: str | None = None,
        ) -> None:
            entry: dict[str, Any] = {"label": label, "url": url}
            if filtered:
                entry["filtered"] = True
            if note:
                entry["note"] = note
            if error:
                entry["error"] = error
            else:
                entry["payload"] = payload
            raw[target].append(entry)

        def fetch_and_add(
            target: str,
            label: str,
            url: str,
            *,
            filter_key: str | None = None,
            filter_value: str | None = None,
            note: str | None = None,
        ) -> None:
            try:
                payload = _fetch_json_any(url)
                filtered = False
                if filter_key and filter_value:
                    payload, filtered = _filter_payload_list(payload, filter_key, filter_value)
                add_entry(
                    target,
                    label,
                    url,
                    payload=payload,
                    filtered=filtered,
                    note=note,
                )
            except Exception as exc:  # pylint: disable=broad-except
                add_entry(target, label, url, error=str(exc), note=note)

        if name == "bybit":
            base_url = getattr(adapter, "base_url", "https://api.bybit.com")
            category = "linear" if exchange_symbol.endswith("USDT") else "inverse"
            tickers_url = f"{base_url}/v5/market/tickers?" + urlencode(
                {"category": category, "symbol": exchange_symbol}
            )
            fetch_and_add("snapshot", "tickers", tickers_url)
            history_url = f"{base_url}/v5/market/funding/history?" + urlencode(
                {"category": category, "symbol": exchange_symbol, "limit": history_limit}
            )
            fetch_and_add("history", "funding_history", history_url)
        elif name == "binance":
            base_url = getattr(adapter, "base_url", "https://fapi.binance.com")
            premium_url = f"{base_url}/fapi/v1/premiumIndex?" + urlencode(
                {"symbol": exchange_symbol}
            )
            fetch_and_add("snapshot", "premium_index", premium_url)
            book_url = f"{base_url}/fapi/v1/ticker/bookTicker?" + urlencode(
                {"symbol": exchange_symbol}
            )
            fetch_and_add("snapshot", "book_ticker", book_url)
            history_url = f"{base_url}/fapi/v1/fundingRate?" + urlencode(
                {"symbol": exchange_symbol, "limit": history_limit}
            )
            fetch_and_add("history", "funding_rate", history_url)
        elif name == "bingx":
            base_url = getattr(adapter, "base_url", "https://open-api.bingx.com")
            contracts_url = f"{base_url}/openApi/swap/v2/quote/contracts"
            fetch_and_add(
                "snapshot",
                "contracts",
                contracts_url,
                filter_key="symbol",
                filter_value=exchange_symbol,
                note="filtered to symbol",
            )
            funding_url = f"{base_url}/openApi/swap/v2/quote/fundingRate?" + urlencode(
                {"symbol": exchange_symbol}
            )
            fetch_and_add(
                "snapshot",
                "funding_rate",
                funding_url,
                note="also used for history",
            )
        elif name == "bitget":
            base_url = getattr(adapter, "base_url", "https://api.bitget.com")
            ticker_url = f"{base_url}/api/mix/v2/market/ticker?" + urlencode(
                {"symbol": exchange_symbol}
            )
            fetch_and_add("snapshot", "ticker", ticker_url)
            funding_url = f"{base_url}/api/mix/v2/market/current-fundRate?" + urlencode(
                {"symbol": exchange_symbol}
            )
            fetch_and_add("snapshot", "current_fundRate", funding_url)
            history_url = f"{base_url}/api/mix/v2/market/history-fundRate?" + urlencode(
                {"symbol": exchange_symbol, "pageSize": history_limit}
            )
            fetch_and_add("history", "history_fundRate", history_url)
        elif name == "okx":
            base_url = getattr(adapter, "base_url", "https://www.okx.com")
            funding_url = f"{base_url}/api/v5/public/funding-rate?" + urlencode(
                {"instId": exchange_symbol}
            )
            fetch_and_add("snapshot", "funding_rate", funding_url)
            ticker_url = f"{base_url}/api/v5/market/ticker?" + urlencode(
                {"instId": exchange_symbol}
            )
            fetch_and_add("snapshot", "ticker", ticker_url)
            history_url = f"{base_url}/api/v5/public/funding-rate-history?" + urlencode(
                {"instId": exchange_symbol, "limit": history_limit}
            )
            fetch_and_add("history", "funding_rate_history", history_url)
        elif name == "gate":
            base_url = getattr(adapter, "base_url", "https://fx-api.gateio.ws/api/v4")
            settle_resolver = getattr(adapter, "settle_for_symbol", None)
            if callable(settle_resolver):
                settle = settle_resolver(exchange_symbol) or "usdt"
            else:
                settle = "btc" if str(exchange_symbol or "").upper().endswith("_USD") else "usdt"
            ticker_url = f"{base_url}/futures/{settle}/tickers?" + urlencode(
                {"contract": exchange_symbol}
            )
            fetch_and_add("snapshot", "tickers", ticker_url)
            contract_url = f"{base_url}/futures/{settle}/contracts/{exchange_symbol}"
            fetch_and_add("snapshot", "contract", contract_url)
            history_url = f"{base_url}/futures/{settle}/funding_rate?" + urlencode(
                {"contract": exchange_symbol, "limit": history_limit}
            )
            fetch_and_add("history", "funding_rate", history_url)
        elif name == "mexc":
            ticker_url = getattr(
                adapter, "ticker_url", "https://contract.mexc.com/api/v1/contract/ticker"
            )
            fetch_and_add(
                "snapshot",
                "ticker",
                ticker_url,
                filter_key="symbol",
                filter_value=exchange_symbol,
                note="filtered to symbol",
            )
            funding_tpl = getattr(
                adapter,
                "funding_url_tpl",
                "https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}",
            )
            funding_url = funding_tpl.format(symbol=exchange_symbol)
            fetch_and_add("snapshot", "funding_rate", funding_url)
            history_url = (
                "https://contract.mexc.com/api/v1/contract/funding_rate/history/"
                + exchange_symbol
            )
            fetch_and_add("history", "funding_rate_history", history_url)
        elif name == "kucoin":
            base_url = getattr(adapter, "base_url", "https://api-futures.kucoin.com")
            contracts_url = f"{base_url}/api/v1/contracts/active"
            fetch_and_add(
                "snapshot",
                "contracts",
                contracts_url,
                filter_key="symbol",
                filter_value=exchange_symbol,
                note="filtered to symbol",
            )
            ticker_url = f"{base_url}/api/v1/ticker?" + urlencode(
                {"symbol": exchange_symbol}
            )
            fetch_and_add("snapshot", "ticker", ticker_url)
            now_ms = int(time.time() * 1000)
            from_ms = now_ms - 7 * 24 * 3600 * 1000
            history_url = f"{base_url}/api/v1/contract/funding-rates?" + urlencode(
                {"symbol": exchange_symbol, "from": from_ms, "to": now_ms}
            )
            fetch_and_add("history", "funding_rates", history_url)
        else:
            raw["errors"] = [f"{exchange}: raw funding fetch not implemented"]

        return raw

    async def manual_test_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
        symbol = str(payload.get("symbol") or "").strip()
        side = str(payload.get("side") or "").strip().lower()
        if side in ("", "auto", "any", "none"):
            side = ""

        errors: list[str] = []
        warnings: list[str] = []
        if not exchange:
            errors.append("exchange is required")
        if not symbol:
            errors.append("symbol is required")
        if side and side not in ("long", "short"):
            errors.append("side must be long/short or empty")
        if errors:
            return {"errors": errors}

        gateway, error = await self._manual_test_gateway(exchange)
        if not gateway:
            return {"errors": [error or "gateway_unavailable"]}

        if exchange == "binance":
            client = gateway.client
            if client is None:
                return {"errors": ["client_unavailable"]}
            ccxt_symbol = await self._manual._resolve_market_symbol(client, symbol)
            if not ccxt_symbol:
                return {"errors": [f"{exchange}: unable to resolve symbol {symbol}"]}
            if margin_mode:
                try:
                    await client.set_margin_mode(margin_mode, ccxt_symbol)
                except Exception as exc:  # pylint: disable=broad-except
                    return {
                        "errors": [str(exc)],
                        "exchange": exchange,
                        "symbol": symbol,
                        "margin_mode": margin_mode,
                    }
            try:
                target = int(round(float(leverage)))
                result = await client.set_leverage(target, ccxt_symbol, {})
            except Exception as exc:  # pylint: disable=broad-except
                return {"errors": [str(exc)], "exchange": exchange, "symbol": symbol}
            return {
                "exchange": exchange,
                "symbol": symbol,
                "side": side or None,
                "margin_mode": margin_mode or None,
                "target_leverage": leverage,
                "result": result,
                "warnings": ["binance leverage set by symbol"],
            }

        try:
            positions = await gateway.fetch_positions()
        except Exception as exc:  # pylint: disable=broad-except
            return {"errors": [str(exc)]}

        position, pos_errors = self._manual_test_select_position(positions, symbol, side)
        if pos_errors:
            return {"errors": pos_errors}

        balance = None
        try:
            balance = await gateway.fetch_balance()
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"{exchange}: balance fetch failed: {exc}")

        metrics = _manual_position_metrics(position)
        estimates = _manual_margin_estimates(position, balance)
        available = estimates.get("max_add_est")
        max_reduce = estimates.get("max_reduce_est")

        return {
            "exchange": exchange,
            "symbol": symbol,
            "side": position.get("side") or side or None,
            "position": position,
            "position_view": _manual_position_view(position, estimates=estimates),
            "position_raw": position.get("raw"),
            "metrics": metrics,
            "balance": balance,
            "max_add_est": available,
            "max_reduce_est": max_reduce,
            "warnings": warnings,
        }

    async def manual_test_margin(self, payload: dict[str, Any], *, action: str) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
        symbol = str(payload.get("symbol") or "").strip()
        side = str(payload.get("side") or "").strip().lower()
        if side in ("", "auto", "any", "none"):
            side = ""
        amount = _safe_float(payload.get("amount"))

        errors: list[str] = []
        warnings: list[str] = []
        if not exchange:
            errors.append("exchange is required")
        if not symbol:
            errors.append("symbol is required")
        if side and side not in ("long", "short"):
            errors.append("side must be long/short or empty")
        if amount is None or amount <= 0:
            errors.append("amount must be > 0")
        if errors:
            return {"errors": errors}

        gateway, error = await self._manual_test_gateway(exchange)
        if not gateway:
            return {"errors": [error or "gateway_unavailable"]}

        try:
            positions = await gateway.fetch_positions()
        except Exception as exc:  # pylint: disable=broad-except
            return {"errors": [str(exc)]}

        position, pos_errors = self._manual_test_select_position(positions, symbol, side)
        if pos_errors:
            return {"errors": pos_errors}

        before_metrics = _manual_position_metrics(position)
        before_balance = None
        try:
            before_balance = await gateway.fetch_balance()
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"{exchange}: balance fetch failed: {exc}")
        before_estimates = _manual_margin_estimates(position, before_balance)

        result = await self._accounts._modify_margin(
            exchange=exchange, position=position, amount=float(amount), action=action
        )
        if result.get("status") != "ok":
            return {
                "errors": [str(result.get("error") or "margin_update_failed")],
                "exchange": exchange,
                "symbol": symbol,
                "side": position.get("side") or side or None,
                "action": action,
                "amount": amount,
                "before": {
                    "position": position,
                    "position_view": _manual_position_view(position, estimates=before_estimates),
                    "position_raw": position.get("raw"),
                    "metrics": before_metrics,
                    "balance": before_balance,
                },
                "result": result,
                "warnings": warnings,
            }

        after_position = None
        after_metrics = None
        after_estimates = None
        try:
            after_positions = await gateway.fetch_positions()
            after_position, pos_errors = self._manual_test_select_position(
                after_positions, symbol, side
            )
            if pos_errors:
                warnings.extend(pos_errors)
            if after_position:
                after_metrics = _manual_position_metrics(after_position)
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"{exchange}: refresh failed: {exc}")

        after_balance = None
        try:
            after_balance = await gateway.fetch_balance()
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"{exchange}: balance refresh failed: {exc}")
        if after_position:
            after_estimates = _manual_margin_estimates(after_position, after_balance)

        return {
            "exchange": exchange,
            "symbol": symbol,
            "side": position.get("side") or side or None,
            "action": action,
            "amount": amount,
            "before": {
                "position": position,
                "position_view": _manual_position_view(position, estimates=before_estimates),
                "position_raw": position.get("raw"),
                "metrics": before_metrics,
                "balance": before_balance,
            },
            "after": {
                "position": after_position,
                "position_view": _manual_position_view(after_position, estimates=after_estimates),
                "position_raw": after_position.get("raw") if after_position else None,
                "metrics": after_metrics,
                "balance": after_balance,
            },
            "result": result,
            "warnings": warnings,
        }

    async def manual_test_leverage(self, payload: dict[str, Any]) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
        symbol = str(payload.get("symbol") or "").strip()
        side = str(payload.get("side") or "").strip().lower()
        if side in ("", "auto", "any", "none"):
            side = ""
        leverage = _safe_float(payload.get("leverage"))
        margin_mode = str(payload.get("margin_mode") or "").strip().lower()
        if margin_mode in ("", "auto", "any", "none"):
            margin_mode = ""

        errors: list[str] = []
        warnings: list[str] = []
        if not exchange:
            errors.append("exchange is required")
        if not symbol:
            errors.append("symbol is required")
        if side and side not in ("long", "short"):
            errors.append("side must be long/short or empty")
        if leverage is None or leverage <= 0:
            errors.append("leverage must be > 0")
        if margin_mode and margin_mode not in ("isolated", "cross"):
            warnings.append("margin_mode must be isolated/cross or empty")
            margin_mode = ""
        if errors:
            return {"errors": errors}

        gateway, error = await self._manual_test_gateway(exchange)
        if not gateway:
            return {"errors": [error or "gateway_unavailable"]}

        try:
            positions = await gateway.fetch_positions()
        except Exception as exc:  # pylint: disable=broad-except
            return {"errors": [str(exc)]}

        position, pos_errors = self._manual_test_select_position(positions, symbol, side)
        if pos_errors:
            return {"errors": pos_errors}

        before_metrics = _manual_position_metrics(position)
        before_balance = None
        try:
            before_balance = await gateway.fetch_balance()
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"{exchange}: balance fetch failed: {exc}")
        before_estimates = _manual_margin_estimates(position, before_balance)

        effective_margin_mode = margin_mode or str(position.get("margin_mode") or "")
        if effective_margin_mode:
            effective_margin_mode = effective_margin_mode.lower()
        if not effective_margin_mode or effective_margin_mode not in ("isolated", "cross"):
            warnings.append("margin_mode defaulted to isolated")
            effective_margin_mode = "isolated"

        result = await self._accounts._set_leverage(
            exchange=exchange,
            position=position,
            margin_mode=effective_margin_mode,
            leverage=float(leverage),
        )
        if result.get("status") != "ok":
            return {
                "errors": [str(result.get("error") or "leverage_update_failed")],
                "exchange": exchange,
                "symbol": symbol,
                "side": position.get("side") or side or None,
                "margin_mode": effective_margin_mode,
                "target_leverage": leverage,
                "before": {
                    "position": position,
                    "position_view": _manual_position_view(position, estimates=before_estimates),
                    "position_raw": position.get("raw"),
                    "metrics": before_metrics,
                    "balance": before_balance,
                },
                "result": result,
                "warnings": warnings,
            }

        after_position = None
        after_metrics = None
        after_estimates = None
        try:
            after_positions = await gateway.fetch_positions()
            after_position, pos_errors = self._manual_test_select_position(
                after_positions, symbol, side
            )
            if pos_errors:
                warnings.extend(pos_errors)
            if after_position:
                after_metrics = _manual_position_metrics(after_position)
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"{exchange}: refresh failed: {exc}")

        after_balance = None
        try:
            after_balance = await gateway.fetch_balance()
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"{exchange}: balance refresh failed: {exc}")
        if after_position:
            after_estimates = _manual_margin_estimates(after_position, after_balance)

        return {
            "exchange": exchange,
            "symbol": symbol,
            "side": position.get("side") or side or None,
            "margin_mode": effective_margin_mode,
            "target_leverage": leverage,
            "before": {
                "position": position,
                "position_view": _manual_position_view(position, estimates=before_estimates),
                "position_raw": position.get("raw"),
                "metrics": before_metrics,
                "balance": before_balance,
            },
            "after": {
                "position": after_position,
                "position_view": _manual_position_view(after_position, estimates=after_estimates),
                "position_raw": after_position.get("raw") if after_position else None,
                "metrics": after_metrics,
                "balance": after_balance,
            },
            "result": result,
            "warnings": warnings,
        }

    async def manual_test_binance_leverage(self, payload: dict[str, Any]) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(payload.get("exchange") or "binance"))
        symbol = str(payload.get("symbol") or "").strip()
        leverage = _safe_float(payload.get("leverage"))
        margin_mode = str(payload.get("margin_mode") or "").strip().lower()
        if margin_mode in ("", "auto", "any", "none"):
            margin_mode = ""

        errors: list[str] = []
        if not symbol:
            errors.append("symbol is required")
        if leverage is None or leverage <= 0:
            errors.append("leverage must be > 0")
        if exchange != "binance":
            errors.append("exchange must be binance")
        if margin_mode and margin_mode not in ("isolated", "cross"):
            errors.append("margin_mode must be isolated/cross or empty")
        if errors:
            return {"errors": errors}

        gateway, error = await self._manual_test_gateway(exchange)
        if not gateway:
            return {"errors": [error or "gateway_unavailable"]}
        client = gateway.client
        if client is None:
            return {"errors": ["client_unavailable"]}
        ccxt_symbol = await self._manual._resolve_market_symbol(client, symbol)
        if not ccxt_symbol:
            return {"errors": [f"{exchange}: unable to resolve symbol {symbol}"]}

        if margin_mode:
            try:
                await client.set_margin_mode(margin_mode, ccxt_symbol)
            except Exception as exc:  # pylint: disable=broad-except
                return {
                    "errors": [str(exc)],
                    "exchange": exchange,
                    "symbol": symbol,
                    "margin_mode": margin_mode,
                }

        try:
            target = int(round(float(leverage)))
            result = await client.set_leverage(target, ccxt_symbol, {})
        except Exception as exc:  # pylint: disable=broad-except
            return {"errors": [str(exc)], "exchange": exchange, "symbol": symbol}

        return {
            "exchange": exchange,
            "symbol": symbol,
            "margin_mode": margin_mode or None,
            "target_leverage": leverage,
            "result": result,
            "warnings": ["binance leverage set by symbol (binance-only test)"],
        }


    async def manual_test_funding(self, payload: dict[str, Any]) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
        raw_symbol = str(payload.get("symbol") or "")
        symbol = _normalize_input_symbol(raw_symbol)
        include_raw = bool(payload.get("include_raw"))
        history_limit = _safe_float(payload.get("history_limit"))
        if history_limit is None:
            history_limit_value = 12
        else:
            history_limit_value = int(history_limit)
        if history_limit_value < 1:
            history_limit_value = 1
        if history_limit_value > 200:
            history_limit_value = 200

        errors: list[str] = []
        warnings: list[str] = []
        if not exchange:
            errors.append("exchange is required")
        if not symbol:
            errors.append("symbol is required")
        if errors:
            return {"errors": errors}

        try:
            adapter = get_adapter_cached(exchange)
        except KeyError:
            return {"errors": [f"{exchange}: adapter unavailable"]}

        try:
            exchange_symbol = adapter.map_symbol(symbol)
        except Exception as exc:  # pylint: disable=broad-except
            exchange_symbol = None
            warnings.append(f"{exchange}: symbol map failed: {exc}")

        if not exchange_symbol:
            return {"errors": [f"{exchange}: unsupported symbol {symbol}"]}

        attempts: list[dict[str, Any]] = []
        history_payload: list[dict[str, Any]] = []
        funding_rate = None
        next_funding_iso = None
        funding_interval_hours = None
        mark_price = None
        rate_source = None
        next_source = None
        interval_source = None
        mark_source = None

        try:
            snapshots = await adapter.fetch_market_snapshots_async([symbol])
            if snapshots:
                snap = snapshots[0]
                snap_next = snap.next_funding_time.isoformat() if snap.next_funding_time else None
                snap_next_stale = bool(snap_next) and is_stale_next_funding_iso(snap_next)
                if snap_next_stale:
                    warnings.append(f"{exchange}: snapshot next_funding_time is stale")
                attempts.append(
                    {
                        "source": "snapshot",
                        "status": "ok",
                        "funding_rate": snap.funding_rate,
                        "next_funding_time": snap_next,
                        "funding_interval_hours": snap.funding_interval_hours,
                        "mark_price": snap.mark_price,
                        "stale_next_funding_time": snap_next_stale,
                    }
                )
                if snap.funding_rate is not None and rate_source is None:
                    funding_rate = snap.funding_rate
                    rate_source = "snapshot"
                if snap_next and (not snap_next_stale) and next_source is None:
                    next_funding_iso = snap_next
                    next_source = "snapshot"
                if snap.funding_interval_hours is not None and interval_source is None:
                    funding_interval_hours = snap.funding_interval_hours
                    interval_source = "snapshot"
                if snap.mark_price is not None and mark_source is None:
                    mark_price = snap.mark_price
                    mark_source = "snapshot"
            else:
                attempts.append({"source": "snapshot", "status": "empty"})
        except Exception as exc:  # pylint: disable=broad-except
            attempts.append({"source": "snapshot", "status": "error", "error": str(exc)})

        try:
            history = await asyncio.to_thread(
                _load_funding_history_cached,
                exchange,
                exchange_symbol,
                symbol,
                history_limit_value,
                adapter,
            )
            if history:
                history = enrich_history_intervals(
                    history,
                    snapshot_interval=funding_interval_hours,
                )
                history_sorted = sorted(
                    history,
                    key=lambda item: _funding_history_ts_ms(
                        item.get("ts_ms") or item.get("timestamp")
                    )
                    or 0,
                    reverse=True,
                )
                latest = next((item for item in history_sorted if item.get("rate") is not None), None)
                hist_rate = _safe_float(latest.get("rate")) if latest else None
                hist_interval = infer_funding_interval_hours(
                    history_sorted,
                    snapshot_interval=funding_interval_hours,
                )
                hist_mark = _safe_float(latest.get("mark_price")) if latest else None
                hist_next = project_next_funding_time_iso(
                    history_sorted,
                    interval_hours=hist_interval,
                )

                history_payload = []
                for entry in history_sorted[:history_limit_value]:
                    ts_ms = _funding_history_ts_ms(
                        entry.get("ts_ms") or entry.get("timestamp")
                    )
                    ts_iso = None
                    if ts_ms:
                        ts_iso = datetime.fromtimestamp(
                            ts_ms / 1000, tz=timezone.utc
                        ).isoformat()
                    history_payload.append(
                        {
                            "ts_ms": ts_ms,
                            "ts_iso": ts_iso,
                            "rate": _safe_float(entry.get("rate")),
                            "interval_hours": _safe_float(entry.get("interval_hours")),
                            "mark_price": _safe_float(entry.get("mark_price")),
                        }
                    )

                attempts.append(
                    {
                        "source": "history",
                        "status": "ok",
                        "funding_rate": hist_rate,
                        "next_funding_time": hist_next,
                        "funding_interval_hours": hist_interval,
                        "mark_price": hist_mark,
                    }
                )

                if hist_rate is not None and rate_source is None:
                    funding_rate = hist_rate
                    rate_source = "history"
                if hist_next and next_source is None:
                    next_funding_iso = hist_next
                    next_source = "history"
                if hist_interval is not None and interval_source is None:
                    funding_interval_hours = hist_interval
                    interval_source = "history"
                if hist_mark is not None and mark_source is None:
                    mark_price = hist_mark
                    mark_source = "history"
            else:
                attempts.append({"source": "history", "status": "empty"})
        except Exception as exc:  # pylint: disable=broad-except
            attempts.append({"source": "history", "status": "error", "error": str(exc)})

        if normalize_exchange_name(exchange) == "bitget" and (
            funding_rate is None or next_funding_iso is None
        ):
            try:
                import ccxt  # type: ignore

                client = ccxt.bitget({"options": {"defaultType": "swap"}})
                base = symbol
                for suffix in ("USDT", "USDC", "USD"):
                    if base.endswith(suffix):
                        base = base[: -len(suffix)]
                        break
                try:
                    client.load_markets()
                except Exception:  # pylint: disable=broad-except
                    pass
                candidates = [
                    f"{base}/USDT:USDT",
                    exchange_symbol,
                    f"{base}USDT_UMCBL",
                    f"{base}USD_DMCBL",
                ]
                funding = None
                last_exc: Exception | None = None
                for cand in candidates:
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
                            next_iso = datetime.fromtimestamp(
                                float(next_ts) / 1000, tz=timezone.utc
                            ).isoformat()
                    except Exception:  # pylint: disable=broad-except
                        next_iso = None
                    mark = _safe_float(
                        funding.get("markPrice")
                        or funding.get("indexPrice")
                        or funding.get("mark")
                    )
                    attempts.append(
                        {
                            "source": "ccxt",
                            "status": "ok",
                            "funding_rate": rate,
                            "next_funding_time": next_iso,
                            "funding_interval_hours": None,
                            "mark_price": mark,
                        }
                    )
                    if rate is not None and rate_source is None:
                        funding_rate = rate
                        rate_source = "ccxt"
                    if next_iso and next_source is None:
                        next_funding_iso = next_iso
                        next_source = "ccxt"
                    if mark is not None and mark_source is None:
                        mark_price = mark
                        mark_source = "ccxt"
                elif last_exc:
                    attempts.append({"source": "ccxt", "status": "error", "error": str(last_exc)})
            except Exception as exc:  # pylint: disable=broad-except
                attempts.append({"source": "ccxt", "status": "error", "error": str(exc)})

        seconds_to_next, next_eta = _funding_eta(next_funding_iso)
        response = {
            "exchange": exchange,
            "symbol": symbol,
            "exchange_symbol": exchange_symbol,
            "funding_rate": funding_rate,
            "funding_interval_hours": funding_interval_hours,
            "next_funding_time": next_funding_iso,
            "seconds_to_next": seconds_to_next,
            "next_funding_eta": next_eta,
            "mark_price": mark_price,
            "history_limit": history_limit_value,
            "funding_history": history_payload,
            "sources": {
                "rate": rate_source,
                "next_funding_time": next_source,
                "funding_interval_hours": interval_source,
                "mark_price": mark_source,
            },
            "attempts": attempts,
            "warnings": warnings,
        }

        if funding_rate is None and next_funding_iso is None:
            response["errors"] = [f"{exchange}: funding unavailable"]

        if include_raw:
            try:
                response["raw"] = await asyncio.to_thread(
                    self._funding_raw_payload,
                    exchange,
                    symbol,
                    exchange_symbol,
                    adapter,
                    history_limit=history_limit_value,
                )
            except Exception as exc:  # pylint: disable=broad-except
                response["raw_error"] = str(exc)

        try:
            funding_test_logger.info(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "exchange": exchange,
                        "symbol": symbol,
                        "exchange_symbol": exchange_symbol,
                        "funding_rate": funding_rate,
                        "funding_interval_hours": funding_interval_hours,
                        "next_funding_time": next_funding_iso,
                        "seconds_to_next": seconds_to_next,
                        "sources": response.get("sources"),
                        "attempts": attempts,
                        "warnings": warnings,
                    },
                    ensure_ascii=True,
                )
            )
        except Exception:  # pylint: disable=broad-except
            pass

        return response

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
            params = bitget_private_params({}) if exchange == "bitget" else {}
            result = await client.cancel_order(order_id, ccxt_symbol, params)
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

    async def _manual_test_gateway(self, exchange: str) -> tuple[Any | None, str | None]:
        gateway = self._accounts._gateways.get(exchange)
        if gateway is None:
            return None, f"{exchange}: gateway unavailable"
        await gateway.refresh_credentials_async(force_env=True)
        await gateway.ensure_client()
        if not gateway.has_credentials:
            return None, f"{exchange}: missing_credentials"
        if not gateway.available:
            return None, gateway.unavailable_reason or "client unavailable"
        return gateway, None

    def _manual_test_select_position(
        self,
        positions: list[dict[str, Any]],
        symbol: str,
        side: str,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        match_keys = _symbol_match_values(symbol)
        matches: list[dict[str, Any]] = []
        for position in positions or []:
            if side:
                pos_side = str(position.get("side") or "").lower()
                if pos_side != side:
                    continue
            if _position_matches_symbol(position, match_keys):
                matches.append(position)
        if not matches:
            return None, [f"{symbol}: position not found"]
        if len(matches) > 1:
            options = []
            for entry in matches:
                opt = f"{entry.get('symbol')} ({entry.get('side')})"
                options.append(opt)
            return None, [f"{symbol}: multiple positions match: {', '.join(options)}"]
        return matches[0], []

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

        if exchange != "kucoin" and hasattr(client, "set_leverage"):
            leverage_params: dict[str, object] = {}
            mode = margin_mode.lower() if margin_mode else ""
            if mode in ("isolated", "cross"):
                if exchange == "okx":
                    leverage_params["tdMode"] = mode
                elif exchange == "bitget":
                    leverage_params = bitget_private_params(leverage_params)
                    leverage_params["marginMode"] = "isolated" if mode == "isolated" else "crossed"
                    leverage_params["posSide"] = bitget_position_side(side, reduce_only=reduce_only)
                else:
                    leverage_params["marginMode"] = mode
            elif exchange == "bitget":
                leverage_params = bitget_private_params(leverage_params)
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
        if exchange == "bitget":
            if bitget_uta_enabled():
                params = bitget_private_params(params)
                if margin_mode:
                    mode = margin_mode.lower()
                    params["marginMode"] = "isolated" if mode == "isolated" else "crossed"
                if not position_side:
                    position_side = bitget_position_side(side, reduce_only=reduce_only)
                    params["hedged"] = True
            elif not position_side:
                position_side = "net"
                if margin_mode:
                    params["marginMode"] = margin_mode.lower()
        if exchange == "okx" and margin_mode:
            params["tdMode"] = margin_mode.lower()
        if exchange == "kucoin":
            margin_mode = margin_mode or "ISOLATED"
            params["marginMode"] = margin_mode
            params["marginType"] = margin_mode
            params["leverage"] = int(DEFAULT_MANUAL_LEVERAGE)
            if not position_side:
                position_side = "BOTH"
        if position_side:
            if exchange != "kucoin":
                params["posSide"] = position_side
            if not (exchange == "bitget" and bitget_uta_enabled()):
                params["positionSide"] = position_side

        try:
            if order_type == "limit":
                order = await client.create_order(ccxt_symbol, "limit", side, order_qty, limit_price, params)
            else:
                order = await client.create_order(ccxt_symbol, "market", side, order_qty, None, params)
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc)
            if exchange == "bitget" and not bitget_uta_enabled() and "40774" in message:
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

    async def manual_exec_runs(self) -> dict[str, Any]:
        self._prune_manual_runs()
        runs: list[dict[str, Any]] = []
        for exec_id, run in self._manual_runs.items():
            runs.append(
                {
                    "execution_id": exec_id,
                    "action": run.get("action"),
                    "status": run.get("status"),
                    "created_at": run.get("created_at"),
                    "updated_at": run.get("updated_at"),
                    "stop_requested": bool(run.get("stop_requested")),
                    "logs": len(run.get("logs") or []),
                }
            )
        runs.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
        return {"runs": runs}

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
            "stop_requested": bool(run.get("stop_requested")),
            "stop_force_finalize": bool(run.get("stop_force_finalize")),
            "stop_reason": run.get("stop_reason") or None,
            "logs": list(run.get("logs") or []),
            "result": run.get("result") or {},
            "error": run.get("error"),
            "log_path": run.get("log_path"),
        }

    async def manual_exec_log(self, exec_id: str) -> dict[str, Any]:
        self._prune_manual_runs()
        run = self._manual_runs.get(exec_id)
        if not run:
            return {"error": "execution_not_found"}
        log_path = run.get("log_path")
        if not log_path:
            return {"error": "log_unavailable"}
        try:
            path = Path(log_path).resolve()
            base = MANUAL_EXEC_LOG_DIR.resolve()
            if not str(path).startswith(str(base)):
                return {"error": "log_unavailable"}
            if not path.exists():
                return {"error": "log_missing"}
            content = path.read_text(encoding="utf-8", errors="replace")
            return {"log": content}
        except Exception as exc:  # pylint: disable=broad-except
            return {"error": f"log_read_failed: {exc}"}

    async def manual_exec_stop(
        self,
        exec_id: str,
        *,
        force_finalize: bool = False,
        reason: str | None = None,
    ) -> dict[str, Any]:
        self._prune_manual_runs()
        run = self._manual_runs.get(exec_id)
        if not run:
            return {"error": "execution_not_found"}
        if run.get("status") != "running":
            return {
                "execution_id": exec_id,
                "status": run.get("status"),
                "stop_requested": bool(run.get("stop_requested")),
                "stop_force_finalize": bool(run.get("stop_force_finalize")),
            }
        run["stop_requested"] = True
        run["stop_force_finalize"] = bool(force_finalize)
        run["stop_reason"] = str(reason or "")
        run["updated_at"] = datetime.now(timezone.utc).isoformat()
        run["updated_at_ts"] = time.time()
        logs = run.get("logs")
        if isinstance(logs, list):
            logs.append(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "event": "stop",
                    "message": "User stop requested",
                    "data": {
                        "force_finalize": bool(force_finalize),
                        "reason": str(reason or ""),
                    },
                }
            )
            if len(logs) > 200:
                del logs[:-200]
        return {
            "execution_id": exec_id,
            "status": run.get("status"),
            "stop_requested": True,
            "stop_force_finalize": bool(force_finalize),
        }

    async def _start_manual_run(
        self,
        action: str,
        payload: dict[str, Any],
        positions: Optional[list[dict[str, Any]]],
    ) -> dict[str, Any]:
        self._prune_manual_runs()
        running = self._running_manual_execution()
        if running:
            return {
                "error": "execution_busy",
                "status": "blocked",
                "running_execution_id": running.get("execution_id"),
                "running_action": running.get("action"),
            }
        exec_id = uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        now_ts = time.time()
        log_path = None
        try:
            MANUAL_EXEC_LOG_DIR.mkdir(parents=True, exist_ok=True)
            log_path = MANUAL_EXEC_LOG_DIR / f"{exec_id}.log"
        except Exception:
            log_path = None
        run: dict[str, Any] = {
            "execution_id": exec_id,
            "action": action,
            "status": "running",
            "created_at": now,
            "updated_at": now,
            "created_at_ts": now_ts,
            "updated_at_ts": now_ts,
            "logs": [],
            "result": None,
            "error": None,
            "stop_requested": False,
            "stop_force_finalize": False,
            "stop_reason": "",
            "log_path": str(log_path) if log_path else None,
            "payload_symbol": normalize_symbol(str((payload or {}).get("symbol") or "")),
            "auto_arb_agent": bool((payload or {}).get("auto_arb_agent")),
            "auto_arb_rule_id": str((payload or {}).get("auto_arb_rule_id") or ""),
            "auto_arb_rule_generation": max(
                0,
                int((payload or {}).get("auto_arb_rule_generation") or 0),
            ),
        }
        self._manual_runs[exec_id] = run

        def _append_log(entry: dict[str, Any]) -> None:
            if not log_path:
                return
            try:
                ts = entry.get("ts") or datetime.now(timezone.utc).isoformat()
                event = entry.get("event") or ""
                message = entry.get("message") or ""
                data = entry.get("data") or {}
                line = f"{ts} | {event} | {message}"
                if data:
                    line += f" | data={json.dumps(data, ensure_ascii=True)}"
                line += "\n"
                log_path.open("a", encoding="utf-8").write(line)
            except Exception:
                return

        def _log_cb(entry: dict[str, Any]) -> None:
            logs = run["logs"]
            logs.append(entry)
            if len(logs) > 200:
                del logs[:-200]
            now_log = datetime.now(timezone.utc).isoformat()
            run["updated_at"] = now_log
            run["updated_at_ts"] = time.time()
            _append_log(entry)

        def _stop_cb() -> dict[str, Any] | bool:
            if not run.get("stop_requested"):
                return False
            return {
                "requested": True,
                "force_finalize": bool(run.get("stop_force_finalize")),
                "reason": run.get("stop_reason") or None,
            }

        async def _runner() -> None:
            try:
                if action == "enter":
                    result = await self._manual.enter(payload, log_cb=_log_cb, stop_cb=_stop_cb)
                elif action == "exit":
                    result = await self._manual.exit(payload, positions or [], log_cb=_log_cb, stop_cb=_stop_cb)
                elif action == "orphan_cleanup":
                    result = await self._manual.orphan_cleanup(
                        payload, positions or [], log_cb=_log_cb, stop_cb=_stop_cb
                    )
                elif action == "roll":
                    result = await self._manual.roll(payload, positions or [], log_cb=_log_cb, stop_cb=_stop_cb)
                else:
                    result = {"errors": [f"unsupported manual action {action}"]}
                if action == "exit" and isinstance(result, dict):
                    cleanup_actions = await self._cleanup_protective_after_exit(payload, result)
                    if cleanup_actions:
                        _log_cb(
                            {
                                "ts": datetime.now(timezone.utc).isoformat(),
                                "event": "protective_cleanup",
                                "message": "post-exit protective cleanup completed",
                                "data": {
                                    "actions": [
                                        {
                                            "exchange": item.get("exchange"),
                                            "symbol": item.get("symbol"),
                                            "status": item.get("status"),
                                            "cancel_order_ids": item.get("cancel_order_ids"),
                                        }
                                        for item in cleanup_actions
                                    ]
                                },
                            }
                        )
                run["result"] = result
                result_warnings = [str(item) for item in (result.get("warnings") or [])]
                result_actions = [
                    dict(item)
                    for item in (result.get("actions") or [])
                    if isinstance(item, Mapping)
                ]
                result_filled_qty = sum(
                    max(0.0, _safe_float(item.get("filled_qty")) or 0.0)
                    for item in result_actions
                )
                remaining_qty = _safe_float(result.get("remaining_qty")) or 0.0
                requested_qty = (
                    _safe_float(result.get("qty"))
                    or _safe_float(payload.get("qty"))
                    or 0.0
                )
                result_errors = [str(item) for item in (result.get("errors") or [])]
                dust_tolerance = max(
                    1e-8,
                    requested_qty * AUTO_ARB_COMPLETION_TOLERANCE_PCT / 100.0,
                )
                dust_only_errors = self._auto_arb_dust_only_errors(result)
                auto_arb_dust_completion = (
                    bool(run.get("auto_arb_agent"))
                    and result_filled_qty > 0
                    and remaining_qty > 0
                    and (not result_errors or dust_only_errors)
                    and (
                        remaining_qty <= dust_tolerance
                        or dust_only_errors
                    )
                )
                if auto_arb_dust_completion:
                    if result_errors:
                        result["dust_errors"] = list(result_errors)
                        result_warnings.extend(
                            f"non-closeable dust: {item}" for item in result_errors
                        )
                    result["errors"] = []
                    result["warnings"] = list(dict.fromkeys(result_warnings))
                    result["completed_with_dust"] = True
                    result["dust_remaining_qty"] = remaining_qty
                incomplete_runtime_end = remaining_qty > 0 and any(
                    "runtime ended" in warning.lower()
                    and any(token in warning.lower() for token in ("not entered", "not exited", "not rolled"))
                    for warning in result_warnings
                )
                no_fill_runtime_end = (
                    incomplete_runtime_end
                    and result_filled_qty <= 0
                    and not result.get("errors")
                )
                if no_fill_runtime_end:
                    run["status"] = "completed_no_fill"
                elif auto_arb_dust_completion:
                    run["status"] = "completed_with_dust"
                elif result.get("errors") or incomplete_runtime_end:
                    run["status"] = "completed_with_errors"
                    if result.get("errors"):
                        _append_log(
                            {
                                "ts": datetime.now(timezone.utc).isoformat(),
                                "event": "errors",
                                "message": "Result errors",
                                "data": {"errors": result.get("errors")},
                            }
                        )
                else:
                    run["status"] = "completed"
                fills_by_exchange: dict[str, float] = {}
                order_ids: list[str] = []
                for item in result_actions:
                    exchange = normalize_exchange_name(str(item.get("exchange") or ""))
                    filled_qty = _safe_float(item.get("filled_qty")) or 0.0
                    if exchange and filled_qty > 0:
                        fills_by_exchange[exchange] = (
                            fills_by_exchange.get(exchange, 0.0) + filled_qty
                        )
                    order_id = str(item.get("order_id") or "")
                    if order_id:
                        order_ids.append(order_id)
                terminal_reason = "completed"
                if auto_arb_dust_completion:
                    terminal_reason = "completed_with_dust"
                elif result.get("errors"):
                    terminal_reason = "completed_with_errors"
                elif no_fill_runtime_end:
                    terminal_reason = "no_fill_before_runtime"
                elif incomplete_runtime_end:
                    terminal_reason = "runtime_ended_incomplete"
                elif "stopped_by_user" in result_warnings:
                    terminal_reason = "stopped_by_user"
                elif "condition_not_met" in result_warnings:
                    terminal_reason = "condition_not_met"
                _log_cb(
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "event": "summary",
                        "message": "execution summary",
                        "data": {
                            "execution_id": exec_id,
                            "action": action,
                            "symbol": result.get("symbol") or payload.get("symbol"),
                            "requested_qty": _safe_float(result.get("qty"))
                            or _safe_float(payload.get("qty")),
                            "remaining_qty": _safe_float(result.get("remaining_qty")),
                            "fills_by_exchange": fills_by_exchange,
                            "order_ids": order_ids,
                            "order_count": len(order_ids),
                            "cancel_event_count": sum(
                                1
                                for entry in (run.get("logs") or [])
                                if entry.get("event") == "cancel"
                            ),
                            "duration_sec": round(
                                max(0.0, time.time() - float(run.get("created_at_ts") or time.time())),
                                3,
                            ),
                            "terminal_reason": terminal_reason,
                            "warning_count": len(result_warnings),
                            "error_count": len(result.get("errors") or []),
                            "dust_error_count": len(result.get("dust_errors") or []),
                        },
                    }
                )
            except Exception as exc:  # pylint: disable=broad-except
                run["status"] = "failed"
                run["error"] = str(exc)
                _append_log(
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "event": "exception",
                        "message": "Execution failed",
                        "data": {"error": str(exc), "traceback": traceback.format_exc()},
                    }
                )
            run["updated_at"] = datetime.now(timezone.utc).isoformat()
            run["updated_at_ts"] = time.time()

        asyncio.create_task(_runner())
        return {"execution_id": exec_id, "status": "running"}

    def _prune_manual_runs(self) -> None:
        now = time.time()
        expired = [
            key
            for key, run in self._manual_runs.items()
            if (
                run.get("status") != "running"
                and (now - float(run.get("updated_at_ts") or run.get("created_at_ts") or 0))
                > self._manual_run_ttl
            )
        ]
        for key in expired:
            self._manual_runs.pop(key, None)
    def _account_state_cache_token(self, payload: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            payload.get("last_updated"),
            len(payload.get("balances") or []),
            len(payload.get("positions") or []),
            len(payload.get("status") or []),
            self._positions_market_last_refresh.isoformat() if self._positions_market_last_refresh else None,
            self._positions_market_last_error,
            len(self._positions_market_cache),
            len(self._positions_market_status),
            len(self._positions_market_diffs),
            (self._margin_logic_log[-1].get("timestamp") if self._margin_logic_log else None),
            id(self._risk_config),
            int(self._positions_market_interval or 0),
        )

    def dashboard_runtime_payload(self) -> dict[str, object]:
        """Return compact cached runtime state for the main dashboard.

        This intentionally skips retired decision modules and the expanded
        account diagnostics tree. The companion ``mobile_positions_payload``
        supplies the whitelisted position cards.
        """

        accounts_snapshot = self._accounts.snapshot()
        status = self._status
        if status == "idle" and accounts_snapshot.get("last_updated"):
            status = "ready"
        settings_payload = self._settings_manager.as_dict()
        return {
            "status": status,
            "last_error": self._last_error,
            "last_updated": accounts_snapshot.get("last_updated"),
            "refresh_in_progress": self._in_progress,
            "refresh_intervals": {
                "dashboard_sec": int(
                    settings_payload.get("table_refresh_seconds", 60)
                ),
                "accounts_sec": int(
                    settings_payload.get("account_refresh_seconds", self._account_interval)
                ),
                "positions_market_sec": int(
                    settings_payload.get(
                        "positions_market_refresh_seconds",
                        self._positions_market_interval,
                    )
                ),
                "summary_sec": int(
                    settings_payload.get(
                        "summary_refresh_seconds",
                        getattr(self, "_summary_interval", 1800),
                    )
                ),
            },
            "events": list(self._events)[-20:],
            "exchange_status": list(accounts_snapshot.get("status") or []),
            "settings": settings_payload,
            "runtime_modules": self._runtime_modules.to_dict(),
            "grid": self.auto_arb_payload(),
        }

    def mobile_positions_payload(self) -> dict[str, Any]:
        accounts_snapshot = self._accounts.snapshot()
        positions = accounts_snapshot.get("positions") or []
        balances = compact_account_balances(
            self._balances_with_status_rows(
                self._sanitize_balances(accounts_snapshot.get("balances") or []),
                accounts_snapshot.get("status") or [],
            )
        )
        market_lookup, market_ts_lookup = self._positions_market_snapshot_lookup()
        rows, grouped = self._positions_by_symbol(
            positions,
            return_grouped=True,
            market_lookup=market_lookup,
            market_ts_lookup=market_ts_lookup,
        )
        return build_main_positions_payload(
            status=self._status,
            accounts_snapshot=accounts_snapshot,
            balances=balances,
            rows=rows,
            grouped=grouped,
        )


    async def mobile_manual_spread(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return await self._manual_spread_service.analyze(payload)

    async def _mobile_quote_for_exchange(self, exchange: str, symbol: str) -> dict[str, Any]:
        return await self._manual_spread_service.quote_for_exchange(exchange, symbol)

    def mobile_manual_defaults_payload(self) -> dict[str, Any]:
        settings_payload = self._settings_manager.as_dict()
        accounts_snapshot = self._accounts.snapshot()
        analysis_exchanges = settings_payload.get("analysis_exchanges") or {}
        enabled_exchanges = [
            normalize_exchange_name(str(name))
            for name, enabled in analysis_exchanges.items()
            if enabled
        ]
        if not enabled_exchanges:
            enabled_exchanges = [
                normalize_exchange_name(str(name))
                for name in analysis_exchanges.keys()
            ]
        enabled_exchanges = [name for name in enabled_exchanges if name]
        manual_settings = getattr(self._settings_manager.current, "manual", {}) or {}
        return {
            "status": self._status if self._status != "idle" else ("ready" if accounts_snapshot.get("last_updated") else "idle"),
            "last_updated": accounts_snapshot.get("last_updated"),
            "exchanges": enabled_exchanges,
            "actions": ["enter", "exit", "roll"],
            "main_modes": [
                {"id": "smart", "label": "Smart"},
                {"id": "fast", "label": "Fast"},
            ],
            "roll_modes": [
                {"id": "smart-roll", "label": "Smart"},
                {"id": "limit-first-expensive", "label": "Limit first"},
                {"id": "dual-limit", "label": "Dual limit"},
                {"id": "dual-market", "label": "Dual market"},
                {"id": "limit-then-market-fallback", "label": "Limit then market"},
            ],
            "expensive_leg_options": {
                "enter_exit": [
                    {"id": None, "label": "Auto hint"},
                    {"id": "long", "label": "Long leg"},
                    {"id": "short", "label": "Short leg"},
                ],
                "roll": [
                    {"id": None, "label": "Auto hint"},
                    {"id": "to", "label": "To leg"},
                    {"id": "from", "label": "From leg"},
                ],
            },
            "defaults": {
                "max_slippage_bps": 8.0,
                "margin_mode": "isolated",
                "timeout_sec": 15,
                "max_runtime_sec": 300,
                "reprice_sec": 2.0,
                "chunk_qty": None,
                "chunk_notional": None,
                "force_chunk_qty": False,
                "hedge_order_type": "market",
                "hedge_limit_mode": "aggressive",
                "hedge_favorable_bps": 2.0,
                "hedge_adverse_bps": 6.0,
                "hedge_reprice_min_sec": 2.0,
                "hedge_timeout_sec": 5.0,
                "limit_offset_bps": 0.0,
                "limit_offset_ticks": 0,
                "max_limit_deviation_bps": 30.0,
                "use_orderbook_check": True,
                "exit_allow_flip": False,
                "expensive_leg": None,
                "ws_orders_health": dict(manual_settings.get("ws_orders_health") or {}),
            },
            "advanced_sections": [
                "execution",
                "chunking",
                "hedge",
                "safety",
                "system",
            ],
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


    def _account_state(self) -> dict[str, object]:
        payload = self._accounts.snapshot()
        cache_key = self._account_state_cache_token(payload)
        if self._account_state_cache_key == cache_key and self._account_state_cache is not None:
            return self._account_state_cache
        payload = dict(payload)
        positions = payload.get("positions") or []
        balances = self._balances_with_status_rows(
            self._sanitize_balances(payload.get("balances") or []),
            payload.get("status") or [],
        )
        payload["balances"] = balances
        market_lookup, market_ts_lookup = self._positions_market_snapshot_lookup()
        positions_by_symbol = self._positions_by_symbol(
            positions,
            market_lookup=market_lookup,
            market_ts_lookup=market_ts_lookup,
        )
        payload["positions_by_symbol"] = positions_by_symbol
        payload["positions_market"] = self._positions_market_state(positions)
        payload["margin_diagnostics"] = self._margin_diagnostics(positions, balances)
        payload["margin_logic_log"] = list(self._margin_logic_log)
        payload["protective_shadow_events"] = list(self._protective_shadow_events)
        self._account_state_cache_key = self._account_state_cache_token(payload)
        self._account_state_cache = payload
        return payload

    def _margin_logic_event(self, event: str, payload: Mapping[str, Any]) -> None:
        entry = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        entry.update(dict(payload or {}))
        self._margin_logic_log.append(entry)
        if len(self._margin_logic_log) > self._margin_logic_log_limit:
            self._margin_logic_log = self._margin_logic_log[-self._margin_logic_log_limit :]

    def _protective_shadow_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        identity: str,
    ) -> None:
        now_ts = time.time()
        stable_payload = dict(payload or {})
        fingerprint = json.dumps(stable_payload, sort_keys=True, default=str)
        event_key = f"{event}|{identity}"
        previous = self._protective_shadow_fingerprints.get(event_key)
        last_ts = float(self._protective_shadow_last_ts.get(event_key) or 0.0)
        if previous == fingerprint and (now_ts - last_ts) < PROTECTIVE_SHADOW_HEARTBEAT_SEC:
            return
        row = {
            "record_type": "shadow",
            "event": event,
            "ts": datetime.now(timezone.utc).isoformat(),
            **stable_payload,
        }
        self._protective_shadow_fingerprints[event_key] = fingerprint
        self._protective_shadow_last_ts[event_key] = now_ts
        self._protective_shadow_events.append(row)
        if len(self._protective_shadow_events) > PROTECTIVE_SHADOW_EVENT_LIMIT:
            self._protective_shadow_events = self._protective_shadow_events[
                -PROTECTIVE_SHADOW_EVENT_LIMIT:
            ]
        try:
            self._protective_shadow_history_store.append(row)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("failed to append protective shadow history: %s", exc)

    def _margin_diagnostics(self, positions: list[dict[str, Any]], balances: list[dict[str, Any]]) -> list[dict[str, Any]]:
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        add_enabled = bool(protective.get("auto_margin_enabled", True))
        reduce_enabled = bool(protective.get("auto_margin_reduce_enabled", False))
        enforce_mode = bool(protective.get("enforce_isolated_margin", True))
        enforce_leverage = bool(protective.get("enforce_leverage", True))
        target_leverage = _safe_float(protective.get("target_leverage"))
        if target_leverage is None or target_leverage <= 0:
            target_leverage = DEFAULT_MANUAL_LEVERAGE
        kucoin_topup_only = bool(protective.get("kucoin_isolated_topup_only", True))
        add_trigger = 0.27
        reduce_trigger = 0.33
        target_buffer = 0.30
        balances_by_exchange = {
            normalize_exchange_name(str(item.get("exchange") or "")): item
            for item in balances or []
            if str(item.get("exchange") or "").strip()
        }
        rows: list[dict[str, Any]] = []
        for position in positions or []:
            exchange = normalize_exchange_name(str(position.get("exchange") or ""))
            if not exchange:
                continue
            symbol = _dedupe_settle(position.get("symbol_normalized") or normalize_symbol(position.get("symbol")))
            if not symbol:
                continue
            side = str(position.get("side") or "").lower() or None
            balance = balances_by_exchange.get(exchange)
            estimates = _manual_margin_estimates(position, balance)
            margin_mode = str(position.get("margin_mode") or "").lower() or None
            leverage = _safe_float(position.get("leverage"))
            liq_buffer_ratio = None
            mark_price = _safe_float(position.get("mark_price"))
            liq_price = _safe_float(position.get("liquidation_price"))
            if mark_price and liq_price and mark_price > 0 and liq_price > 0:
                if side == "long" and liq_price < mark_price:
                    liq_buffer_ratio = (mark_price - liq_price) / mark_price
                elif side == "short" and liq_price > mark_price:
                    liq_buffer_ratio = (liq_price - mark_price) / mark_price

            decision = "hold"
            reason = "in_range"
            if margin_mode != "isolated":
                if enforce_mode:
                    decision = "set_mode"
                    reason = "enforce_isolated_margin"
                else:
                    decision = "observe"
                    reason = "margin_mode_not_isolated"
            elif exchange == "kucoin" and enforce_leverage:
                if leverage is None:
                    decision = "observe"
                    reason = "leverage_unknown"
                elif leverage > (float(target_leverage) + 0.05):
                    decision = "add_margin"
                    reason = "kucoin_target_leverage"
                elif kucoin_topup_only:
                    decision = "hold"
                    reason = "kucoin_topup_only_target_met"
                else:
                    decision = "hold"
                    reason = "target_leverage_met"
            elif liq_buffer_ratio is None:
                decision = "observe"
                reason = "liq_buffer_unknown"
            elif liq_buffer_ratio < add_trigger:
                if add_enabled:
                    decision = "add_margin"
                    reason = "low_liq_buffer"
                else:
                    decision = "blocked"
                    reason = "auto_add_disabled"
            elif liq_buffer_ratio > reduce_trigger:
                if exchange == "kucoin" and margin_mode == "isolated" and kucoin_topup_only:
                    decision = "hold"
                    reason = "kucoin_topup_only"
                elif reduce_enabled:
                    decision = "reduce_margin"
                    reason = "high_liq_buffer"
                else:
                    decision = "blocked"
                    reason = "auto_reduce_disabled"
                    margin_used = _safe_float(position.get("margin_used"))
                    if margin_used is None:
                        margin_used = _safe_float(position.get("initial_margin"))
                    reduce_amount = None
                    if margin_used is not None and margin_used > 0 and liq_buffer_ratio > 0:
                        reduce_amount = margin_used * (
                            1.0 - target_buffer / liq_buffer_ratio
                        )
                    self._protective_shadow_event(
                        "margin_reduce_candidate",
                        {
                            "exchange": exchange,
                            "symbol": symbol,
                            "side": side,
                            "margin_mode": margin_mode,
                            "liq_buffer_pct": liq_buffer_ratio * 100.0,
                            "reduce_trigger_pct": reduce_trigger * 100.0,
                            "target_buffer_pct": target_buffer * 100.0,
                            "margin_used_usd": margin_used,
                            "planned_reduce_usd": (
                                max(0.0, float(reduce_amount))
                                if reduce_amount is not None
                                else None
                            ),
                            "live_enabled": False,
                            "reason": reason,
                        },
                        identity=f"{exchange}|{symbol}|{side or '-'}",
                    )

            key = f"{exchange}|{symbol}|{side or '-'}"
            fingerprint = "|".join(
                [
                    str(decision),
                    str(reason),
                    str(margin_mode or ""),
                    str(round(leverage, 4) if leverage is not None else "na"),
                    str(round(liq_buffer_ratio, 4) if liq_buffer_ratio is not None else "na"),
                ]
            )
            if self._margin_logic_state.get(key) != fingerprint:
                self._margin_logic_state[key] = fingerprint
                self._margin_logic_event(
                    "decision",
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "side": side,
                        "decision": decision,
                        "reason": reason,
                        "margin_mode": margin_mode,
                        "leverage": leverage,
                        "liq_buffer_pct": (liq_buffer_ratio * 100.0) if liq_buffer_ratio is not None else None,
                    },
                )

            rows.append(
                {
                    "key": key,
                    "exchange": exchange,
                    "symbol": symbol,
                    "side": side,
                    "margin_mode": margin_mode,
                    "margin_mode_source": position.get("margin_mode_source"),
                    "leverage": leverage,
                    "leverage_source": position.get("leverage_source"),
                    "target_leverage": target_leverage,
                    "liq_buffer_pct": (liq_buffer_ratio * 100.0) if liq_buffer_ratio is not None else None,
                    "add_trigger_pct": add_trigger * 100.0,
                    "reduce_trigger_pct": reduce_trigger * 100.0,
                    "target_buffer_pct": target_buffer * 100.0,
                    "base_margin_est": _safe_float(estimates.get("base_margin_est")),
                    "base_margin_source": estimates.get("base_margin_source"),
                    "min_required_margin_est": _safe_float(estimates.get("min_required_margin_est")),
                    "min_required_margin_source": estimates.get("min_required_margin_source"),
                    "max_add_est": _safe_float(estimates.get("max_add_est")),
                    "max_reduce_est": _safe_float(estimates.get("max_reduce_est")),
                    "decision": decision,
                    "reason": reason,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        rows.sort(key=lambda item: (str(item.get("decision") or ""), str(item.get("exchange") or ""), str(item.get("symbol") or "")))
        return rows

    def _positions_market_snapshot_lookup(
        self,
    ) -> tuple[dict[tuple[str, str], MarketSnapshot], dict[tuple[str, str], datetime]]:
        return dict(self._positions_market_cache), dict(self._positions_market_cache_ts)

    def _positions_market_state(self, positions: list[dict[str, Any]] | None = None) -> dict[str, object]:
        last_updated = (
            self._positions_market_last_refresh.isoformat()
            if self._positions_market_last_refresh
            else None
        )
        symbols = len(self._positions_market_last_key or ())
        positions = positions or []
        return {
            "last_updated": last_updated,
            "last_error": self._positions_market_last_error,
            "symbols": symbols,
            "exchanges": len(self._positions_market_status),
            "status": list(self._positions_market_status),
            "diffs": list(self._positions_market_diffs),
            "margin_issues": self._positions_margin_report(positions),
        }

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

    @staticmethod
    def _balances_with_status_rows(
        rows: list[dict[str, Any]],
        status_entries: list[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        result = [dict(row) for row in rows]
        seen = {
            normalize_exchange_name(str(row.get("exchange") or ""))
            for row in result
            if str(row.get("exchange") or "").strip()
        }
        for entry in status_entries or []:
            exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
            if not exchange or exchange in seen:
                continue
            status = str(entry.get("status") or "").strip().lower()
            if status not in {"error", "partial", "unavailable", "missing_credentials"}:
                continue
            error = (
                entry.get("balance_error")
                or entry.get("error")
                or entry.get("message")
                or entry.get("positions_error")
            )
            result.append(
                {
                    "exchange": exchange,
                    "asset": "USDT",
                    "total": None,
                    "available": None,
                    "used": None,
                    "margin_ratio": None,
                    "equity": None,
                    "buffer_pct": None,
                    "initial_margin": None,
                    "maintenance_margin": None,
                    "status": status or "unknown",
                    "error": str(error or ""),
                    "timestamp": entry.get("checked_at"),
                }
            )
            seen.add(exchange)
        result.sort(key=lambda item: normalize_exchange_name(str(item.get("exchange") or "")))
        return result

    @staticmethod
    def _parse_iso_ts(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        try:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            return datetime.fromisoformat(str(value)).astimezone(timezone.utc)
        except Exception:  # pylint: disable=broad-except
            return None

    @classmethod
    def _position_scan_evidence(
        cls,
        accounts_snapshot: Mapping[str, Any] | None,
        exchanges: Iterable[str],
        *,
        now_ts: float | None = None,
        stale_after_sec: float = 180.0,
    ) -> dict[str, Any]:
        """Describe whether position absence is backed by fresh successful venue scans."""
        required = sorted(
            {
                normalize_exchange_name(str(exchange))
                for exchange in exchanges
                if normalize_exchange_name(str(exchange))
                and normalize_exchange_name(str(exchange)) != "multileg"
            }
        )
        now_value = float(now_ts or time.time())
        status_by_exchange = {
            normalize_exchange_name(str(item.get("exchange") or "")): dict(item)
            for item in ((accounts_snapshot or {}).get("status") or [])
            if normalize_exchange_name(str(item.get("exchange") or ""))
        }
        rows: list[dict[str, Any]] = []
        trusted = bool(required)
        for exchange in required:
            status = status_by_exchange.get(exchange) or {}
            checked_dt = cls._parse_iso_ts(status.get("checked_at"))
            checked_ts = checked_dt.timestamp() if checked_dt else None
            age_sec = max(0.0, now_value - checked_ts) if checked_ts is not None else None
            explicit_fetch_ok = status.get("positions_fetch_ok")
            positions_fetch_ok = bool(explicit_fetch_ok) if explicit_fetch_ok is not None else (
                str(status.get("status") or "").lower() == "ok"
                and not status.get("positions_error")
            )
            row_trusted = bool(
                positions_fetch_ok
                and age_sec is not None
                and age_sec <= max(30.0, float(stale_after_sec))
            )
            trusted = trusted and row_trusted
            rows.append(
                {
                    "exchange": exchange,
                    "trusted": row_trusted,
                    "positions_fetch_ok": positions_fetch_ok,
                    "positions_count": status.get("positions_count"),
                    "checked_at": status.get("checked_at"),
                    "age_sec": round(age_sec, 3) if age_sec is not None else None,
                    "status": status.get("status"),
                    "positions_error": status.get("positions_error"),
                }
            )
        evidence_id = "|".join(
            f"{row['exchange']}:{row.get('checked_at') or ''}"
            for row in rows
        )
        return {
            "trusted": trusted,
            "required_exchanges": required,
            "evidence_id": evidence_id if trusted else "",
            "rows": rows,
        }

    @staticmethod

    def _collect_positions_market_symbols(
        self, positions: list[dict[str, Any]]
    ) -> dict[str, list[str]]:
        by_exchange: dict[str, set[str]] = {}
        for entry in positions:
            exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
            if not exchange:
                continue
            symbol_norm = _dedupe_settle(
                entry.get("symbol_normalized") or normalize_symbol(entry.get("symbol"))
            )
            if not symbol_norm:
                continue
            by_exchange.setdefault(exchange, set()).add(symbol_norm)
        return {ex: sorted(symbols) for ex, symbols in by_exchange.items()}

    async def _refresh_positions_market_snapshots(self, *, force: bool = False) -> None:
        async with self._positions_market_lock:
            accounts_snapshot = self._accounts.snapshot()
            positions = accounts_snapshot.get("positions") or []
            account_last_updated = accounts_snapshot.get("last_updated")
            now = datetime.now(timezone.utc)
            interval = max(30, int(self._positions_market_interval or self._account_interval))
            grace_sec = min(10, max(2, interval // 10))
            if (
                not force
                and account_last_updated
                and account_last_updated == self._positions_market_last_account_update
                and self._positions_market_last_refresh
            ):
                age = (now - self._positions_market_last_refresh).total_seconds()
                if age < (interval + grace_sec):
                    return
            symbols_by_exchange = self._collect_positions_market_symbols(positions)
            key_bits = [
                f"{exchange}:{symbol}"
                for exchange, symbols in sorted(symbols_by_exchange.items())
                for symbol in symbols
            ]
            key = tuple(sorted(key_bits))
            if (
                not force
                and self._positions_market_last_refresh
                and key == (self._positions_market_last_key or ())
            ):
                age = (now - self._positions_market_last_refresh).total_seconds()
                if age < (interval + grace_sec):
                    return

            if not symbols_by_exchange:
                self._positions_market_cache = {}
                self._positions_market_cache_ts = {}
                self._positions_market_last_refresh = now
                self._positions_market_last_error = None
                self._positions_market_last_key = ()
                self._positions_market_last_account_update = account_last_updated
                self._positions_market_status = []
                self._positions_market_diffs = []
                return

            async def _fetch_exchange(exchange: str, symbols: list[str]) -> dict[str, Any]:
                async with self._positions_market_sem:
                    try:
                        adapter = get_adapter_cached(exchange)
                    except KeyError as exc:
                        return {
                            "exchange": exchange,
                            "status": "missing_adapter",
                            "symbols": len(symbols),
                            "snapshots": [],
                            "error": str(exc),
                        }
                    try:
                        snapshots = await adapter.fetch_market_snapshots_async(symbols)
                        return {
                            "exchange": exchange,
                            "status": "ok",
                            "symbols": len(symbols),
                            "snapshots": snapshots,
                        }
                    except Exception as exc:  # pylint: disable=broad-except
                        return {
                            "exchange": exchange,
                            "status": "error",
                            "symbols": len(symbols),
                            "snapshots": [],
                            "error": str(exc),
                        }

            tasks = [
                asyncio.create_task(_fetch_exchange(exchange, symbols))
                for exchange, symbols in symbols_by_exchange.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            new_cache: dict[tuple[str, str], MarketSnapshot] = {}
            new_ts: dict[tuple[str, str], datetime] = {}
            status_rows: list[dict[str, Any]] = []
            errors: list[str] = []
            for exchange, result in zip(symbols_by_exchange.keys(), results):
                if isinstance(result, Exception):
                    status_rows.append(
                        {
                            "exchange": exchange,
                            "status": "error",
                            "symbols": len(symbols_by_exchange.get(exchange, [])),
                            "error": str(result),
                        }
                    )
                    errors.append(str(result))
                    continue
                status_rows.append(
                    {
                        "exchange": result.get("exchange", exchange),
                        "status": result.get("status"),
                        "symbols": result.get("symbols"),
                        "count": len(result.get("snapshots") or []),
                        "error": result.get("error"),
                    }
                )
                if result.get("error"):
                    errors.append(str(result.get("error")))
                for snapshot in result.get("snapshots") or []:
                    if not isinstance(snapshot, MarketSnapshot):
                        continue
                    snap_symbol = normalize_symbol(snapshot.symbol)
                    if not snap_symbol:
                        continue
                    key_entry = (exchange, snap_symbol)
                    new_cache[key_entry] = snapshot
                    new_ts[key_entry] = now

            self._positions_market_cache = new_cache
            self._positions_market_cache_ts = new_ts
            self._positions_market_last_refresh = now
            self._positions_market_last_error = "; ".join(errors) if errors else None
            self._positions_market_last_key = key
            self._positions_market_last_account_update = account_last_updated
            self._positions_market_status = status_rows
            self._positions_market_diffs = self._positions_market_diff_report(
                positions,
                new_cache,
                new_ts,
            )

    def _positions_market_diff_report(
        self,
        positions: list[dict[str, Any]],
        market_cache: dict[tuple[str, str], MarketSnapshot],
        market_ts_lookup: dict[tuple[str, str], datetime],
        *,
        max_rows: int = 12,
    ) -> list[dict[str, Any]]:
        diffs: list[dict[str, Any]] = []
        for entry in positions:
            exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
            if not exchange:
                continue
            symbol_norm = _dedupe_settle(
                entry.get("symbol_normalized") or normalize_symbol(entry.get("symbol"))
            )
            if not symbol_norm:
                continue
            lookup_symbols = [symbol_norm]
            base_symbol = _strip_settle(symbol_norm)
            if base_symbol and base_symbol not in lookup_symbols:
                lookup_symbols.append(base_symbol)
            snapshot = None
            snap_ts = None
            for sym in lookup_symbols:
                key = (exchange, sym)
                snapshot = market_cache.get(key)
                if snapshot:
                    snap_ts = market_ts_lookup.get(key)
                    break
            if not snapshot:
                continue

            position_ts = self._parse_iso_ts(entry.get("timestamp"))
            pos_mark = _safe_float(entry.get("mark_price"))
            snap_mark = _safe_float(snapshot.mark_price)
            if pos_mark is not None and snap_mark is not None and pos_mark != 0:
                delta_pct = abs(snap_mark - pos_mark) / abs(pos_mark) * 100.0
                if delta_pct >= 0.1:
                    diffs.append(
                        {
                            "exchange": exchange,
                            "symbol": symbol_norm,
                            "field": "mark_price",
                            "position": pos_mark,
                            "snapshot": snap_mark,
                            "delta_pct": delta_pct,
                            "position_ts": position_ts.isoformat() if position_ts else None,
                            "snapshot_ts": snap_ts.isoformat() if snap_ts else None,
                        }
                    )

            pos_rate = _safe_float(entry.get("funding_rate") or entry.get("fundingRate"))
            snap_rate = _safe_float(snapshot.funding_rate)
            if pos_rate is not None and snap_rate is not None:
                delta_rate = abs(snap_rate - pos_rate)
                if delta_rate >= 0.0001:
                    diffs.append(
                        {
                            "exchange": exchange,
                            "symbol": symbol_norm,
                            "field": "funding_rate",
                            "position": pos_rate,
                            "snapshot": snap_rate,
                            "delta": delta_rate,
                            "position_ts": position_ts.isoformat() if position_ts else None,
                            "snapshot_ts": snap_ts.isoformat() if snap_ts else None,
                        }
                    )

            if len(diffs) >= max_rows:
                break
        return diffs

    def _positions_margin_report(
        self,
        positions: list[dict[str, Any]],
        *,
        max_rows: int = 12,
    ) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        for entry in positions:
            exchange = normalize_exchange_name(str(entry.get("exchange") or ""))
            if not exchange:
                continue
            symbol_norm = _dedupe_settle(
                entry.get("symbol_normalized") or normalize_symbol(entry.get("symbol"))
            )
            if not symbol_norm:
                continue
            side = str(entry.get("side") or "").lower() or None
            margin_mode = entry.get("margin_mode")
            margin_mode_source = entry.get("margin_mode_source")
            leverage = _safe_float(entry.get("leverage"))
            leverage_source = entry.get("leverage_source")
            issue_bits: list[str] = []
            if margin_mode is None:
                issue_bits.append("mode:missing")
            elif str(margin_mode).lower() != "isolated":
                issue_bits.append(f"mode:{margin_mode}")
            if leverage is None:
                issue_bits.append("lev:missing")
            elif abs(leverage - DEFAULT_MANUAL_LEVERAGE) > 0.05:
                issue_bits.append(f"lev:{leverage:g}")
            if not issue_bits:
                continue
            issues.append(
                {
                    "exchange": exchange,
                    "symbol": symbol_norm,
                    "side": side,
                    "margin_mode": margin_mode,
                    "margin_mode_source": margin_mode_source,
                    "leverage": leverage,
                    "leverage_source": leverage_source,
                    "issues": issue_bits,
                }
            )
            if len(issues) >= max_rows:
                break
        return issues

    def _positions_by_symbol(
        self,
        positions: List[dict[str, Any]],
        return_grouped: bool = False,
        market_lookup: Optional[dict[tuple[str, str], MarketSnapshot]] = None,
        market_ts_lookup: Optional[dict[tuple[str, str], datetime]] = None,
    ) -> tuple[List[dict[str, Any]], dict[str, list[dict[str, Any]]]] | List[dict[str, Any]]:
        if not positions:
            return ([], {}) if return_grouped else []
        market_lookup = market_lookup or {}
        market_ts_lookup = market_ts_lookup or {}
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
            exchange_notional = abs(float(entry.get("notional") or 0.0)) or None
            funding_rate = _safe_float(entry.get("funding_rate") or entry.get("fundingRate"))
            next_funding_iso = (
                entry.get("next_funding")
                or entry.get("next_funding_time")
                or entry.get("nextFunding")
            )
            if isinstance(next_funding_iso, datetime):
                next_funding_iso = next_funding_iso.isoformat()
            elif isinstance(next_funding_iso, (int, float)) and next_funding_iso > 0:
                try:
                    ts_val = float(next_funding_iso)
                    if ts_val > 1e12:
                        ts_val = ts_val / 1000.0
                    next_funding_iso = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat()
                except Exception:  # pylint: disable=broad-except
                    next_funding_iso = None
            exchange_name = str(entry.get("exchange") or "").lower()
            snapshot = None
            snapshot_ts = None
            funding_interval_hours = _safe_float(entry.get("funding_interval_hours"))
            for sym in lookup_symbols:
                key = (exchange_name, sym)
                snapshot = market_lookup.get(key)
                if snapshot:
                    snapshot_ts = market_ts_lookup.get(key)
                    break
            entry_price = entry.get("entry_price")
            mark_price = entry.get("mark_price")
            mark_price_source = "position" if _safe_float(mark_price) not in (None, 0) else None
            unrealized = entry.get("unrealized_pnl")
            snapshot_funding_stale = False
            if snapshot:
                interval_sec = int(self._positions_market_interval or 60)
                if snapshot_ts:
                    try:
                        age_sec = (datetime.now(timezone.utc) - snapshot_ts).total_seconds()
                    except Exception:  # pylint: disable=broad-except
                        age_sec = None
                    if age_sec is not None and age_sec > max(30, interval_sec):
                        snapshot_funding_stale = True
                if snapshot.next_funding_time:
                    try:
                        next_dt = snapshot.next_funding_time.astimezone(timezone.utc)
                        if next_dt < (datetime.now(timezone.utc) - timedelta(minutes=5)):
                            snapshot_funding_stale = True
                    except Exception:  # pylint: disable=broad-except
                        pass
                if snapshot.funding_rate is not None:
                    if not snapshot_funding_stale:
                        funding_rate = snapshot.funding_rate
                if snapshot.funding_interval_hours is not None:
                    funding_interval_hours = snapshot.funding_interval_hours
                if snapshot.next_funding_time:
                    if not snapshot_funding_stale:
                        next_funding_iso = snapshot.next_funding_time.isoformat()
                snap_mark = _safe_float(snapshot.mark_price)
                if snap_mark is not None:
                    mark_val = _safe_float(mark_price)
                    if mark_val is None or mark_val == 0:
                        mark_price = snap_mark
                        mark_price_source = "positions_market"
                    elif exchange_name == "bingx":
                        try:
                            delta_pct = abs(snap_mark - mark_val) / abs(mark_val) * 100.0
                        except Exception:
                            delta_pct = None
                        if delta_pct is None or delta_pct >= 0.1:
                            mark_price = snap_mark
                            mark_price_source = "positions_market"
            needs_live = funding_rate is None or next_funding_iso is None or snapshot_funding_stale
            if needs_live:
                rate_live, next_live, mark_live = self._funding_live(
                    entry.get("exchange"),
                    entry.get("symbol"),
                    symbol_norm,
                    raw_exchange_symbol=entry.get("symbol"),
                )
                if funding_rate is None or snapshot_funding_stale:
                    funding_rate = rate_live
                if next_funding_iso is None or snapshot_funding_stale:
                    next_funding_iso = next_live
                if mark_price is None and mark_live is not None:
                    mark_price = mark_live
                    mark_price_source = "funding_live"
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
            current_mark_price = _safe_float(mark_price)
            current_notional = None
            if current_mark_price is not None and current_mark_price > 0 and abs(coin_qty) > 0:
                current_notional = abs(coin_qty) * current_mark_price
            entry_notional = None
            entry_price_value = _safe_float(entry_price)
            if entry_price_value is not None and entry_price_value > 0 and abs(coin_qty) > 0:
                entry_notional = abs(coin_qty) * entry_price_value
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
                    # Public position valuation is deliberately exchange-neutral:
                    # base-asset quantity multiplied by the current Mark Price.
                    # Native venue fields such as KuCoin posCost and Binance
                    # notional do not share the same meaning and remain diagnostic.
                    "amount": current_notional,
                    "current_notional": current_notional,
                    "entry_notional": entry_notional,
                    "exchange_notional": exchange_notional,
                    "valuation_status": "current" if current_notional is not None else "unavailable",
                    "mark_price_source": mark_price_source,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "current_mark_price": current_mark_price,
                    "unrealized_pnl": unrealized,
                    "funding_rate": funding_rate,
                    "funding_interval_hours": funding_interval_hours,
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
                            * current_notional
                            * (-1.0 if side == "long" else 1.0)
                        )
                        if funding_rate is not None and current_notional is not None
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
                selected_pair = _select_position_pair_from_legs(legs)
                if selected_pair:
                    summary["selected_long_exchange"] = selected_pair.get("long_exchange")
                    summary["selected_short_exchange"] = selected_pair.get("short_exchange")
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
            adapter = get_adapter_cached(normalize_exchange_name(exchange))
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
        cached = self._funding_cache.get(key)
        if cached:
            rate, next_iso, mark_val, cached_ts = cached
            if now_ts - cached_ts <= FUNDING_CACHE_TTL_SEC:
                return rate, next_iso, mark_val

        logger.debug(
            "funding fetch start exchange=%s key=%s canonical=%s candidates=%s",
            exchange,
            key,
            canonical_symbol,
            candidates,
        )

        # Try live snapshot first for freshest funding; fallback to cached history (<=2m), then ccxt.
        snapshot_rate = None
        snapshot_mark = None
        snapshot_interval = None
        try:
            snapshots = adapter.fetch_market_snapshots([canonical_symbol])
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("Market snapshot fetch failed for %s %s: %s", exchange, canonical_symbol, exc)
            snapshots = []

        if snapshots:
            snap = snapshots[0]
            rate = _safe_float(getattr(snap, "funding_rate", None))
            next_time = getattr(snap, "next_funding_time", None)
            next_funding_iso = next_time.isoformat() if next_time else None
            if next_funding_iso and is_stale_next_funding_iso(next_funding_iso):
                next_funding_iso = None
            mark_val = _safe_float(getattr(snap, "mark_price", None))
            snapshot_rate = rate
            snapshot_mark = mark_val
            snapshot_interval = _safe_float(getattr(snap, "funding_interval_hours", None))
            if next_funding_iso is not None:
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
            history = enrich_history_intervals(history, snapshot_interval=snapshot_interval)
            latest = next((item for item in history if item.get("rate") is not None), None)
            if latest is None:
                history = []
            else:
                hist_rate = _safe_float(latest.get("rate"))
                interval_hours = infer_funding_interval_hours(history, snapshot_interval=snapshot_interval)
                next_funding_iso = project_next_funding_time_iso(history, interval_hours=interval_hours)
                mark_val = _safe_float(latest.get("mark_price"))
                rate = hist_rate if hist_rate is not None else snapshot_rate
                if mark_val is None:
                    mark_val = snapshot_mark
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

        if normalize_exchange_name(exchange) != "bitget" and (
            snapshot_rate is not None or snapshot_mark is not None
        ):
            funding_logger.info(
                "funding ok source=snapshot_partial exchange=%s symbol=%s rate=%s next=%s mark=%s",
                exchange,
                canonical_symbol,
                snapshot_rate,
                None,
                snapshot_mark,
            )
            self._funding_cache[key] = (snapshot_rate, None, snapshot_mark, now_ts)
            return snapshot_rate, None, snapshot_mark

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

        if snapshot_rate is not None or snapshot_mark is not None:
            funding_logger.info(
                "funding ok source=snapshot_partial exchange=%s symbol=%s rate=%s next=%s mark=%s",
                exchange,
                canonical_symbol,
                snapshot_rate,
                None,
                snapshot_mark,
            )
            self._funding_cache[key] = (snapshot_rate, None, snapshot_mark, now_ts)
            return snapshot_rate, None, snapshot_mark

        funding_logger.warning("funding failed exchange=%s symbol=%s reason=unavailable", exchange, canonical_symbol)
        return None, None, None

    def _summarize_symbol(self, symbol: str, legs: List[dict[str, Any]]) -> dict[str, Any] | None:
        if not legs:
            return None
        long_legs = [leg for leg in legs if (leg.get("side") or "").lower() == "long"]
        short_legs = [leg for leg in legs if (leg.get("side") or "").lower() == "short"]
        long_exchange = long_legs[0].get("exchange") if len(long_legs) == 1 else None
        short_exchange = short_legs[0].get("exchange") if len(short_legs) == 1 else None

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
        long_quantity = sum(abs(_safe_float(leg.get("quantity")) or 0.0) for leg in long_legs)
        short_quantity = sum(abs(_safe_float(leg.get("quantity")) or 0.0) for leg in short_legs)
        hedged_quantity = min(long_quantity, short_quantity)
        imbalance_quantity = abs(long_quantity - short_quantity)
        imbalance_pct = (
            imbalance_quantity / hedged_quantity * 100.0
            if hedged_quantity > 0
            else None
        )
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
            "long_quantity": long_quantity,
            "short_quantity": short_quantity,
            "hedged_quantity": hedged_quantity,
            "imbalance_quantity": imbalance_quantity,
            "imbalance_pct": imbalance_pct,
            "long_legs_count": len(long_legs),
            "short_legs_count": len(short_legs),
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
        }

    def _record_event(self, event: str, payload: dict[str, Any]) -> None:
        entry = {
            "event": event,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._events.append(entry)
        if len(self._events) > 200:
            del self._events[:-200]

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
            cfg.stop_force_requote_max_age_sec = int(
                protective.get("stop_force_requote_max_age_sec", cfg.stop_force_requote_max_age_sec)
            )
            cfg.fallback_liq_factor_long = float(
                protective.get("fallback_liq_factor_long", cfg.fallback_liq_factor_long)
            )
            cfg.fallback_liq_factor_short = float(
                protective.get("fallback_liq_factor_short", cfg.fallback_liq_factor_short)
            )
            cfg.fallback_take_rr_pct = float(
                protective.get("fallback_take_rr_pct", cfg.fallback_take_rr_pct)
            )
            cfg.target_safe_buffer_pct = float(
                protective.get("target_safe_buffer_pct", cfg.target_safe_buffer_pct)
            )
            cfg.warning_buffer_pct = float(protective.get("warning_buffer_pct", cfg.warning_buffer_pct))
            cfg.panic_buffer_pct = float(protective.get("panic_buffer_pct", cfg.panic_buffer_pct))
            cfg.min_free_balance_abs = float(protective.get("min_free_balance_abs", cfg.min_free_balance_abs))
            cfg.min_free_balance_rel = float(protective.get("min_free_balance_rel", cfg.min_free_balance_rel))
            cfg.position_check_interval_sec = int(
                protective.get("position_check_interval_sec", cfg.position_check_interval_sec)
            )
            cfg.telegram_alert_chat_id = str(protective.get("telegram_alert_chat_id", cfg.telegram_alert_chat_id))
            cfg.notification_primary_channel = str(
                protective.get("notification_primary_channel", cfg.notification_primary_channel)
            )
            cfg.notification_fallback_channel = str(
                protective.get("notification_fallback_channel", cfg.notification_fallback_channel)
            )
            cfg.send_missing_stop_alerts = bool(
                protective.get("send_missing_stop_alerts", cfg.send_missing_stop_alerts)
            )
        except Exception:
            pass
        return cfg

    def _apply_alert_settings(self) -> None:
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        send_margin = bool(protective.get("send_margin_alerts", True))
        notification_primary_channel = str(protective.get("notification_primary_channel", "ntfy") or "ntfy")
        notification_fallback_channel = str(protective.get("notification_fallback_channel", "none") or "none")
        telegram_chat_id = str(protective.get("telegram_alert_chat_id", "") or "")
        warning_buffer = _safe_float(protective.get("warning_buffer_pct"))
        panic_buffer = _safe_float(protective.get("panic_buffer_pct"))
        min_free_abs = _safe_float(protective.get("min_free_balance_abs"))
        min_free_rel = _safe_float(protective.get("min_free_balance_rel"))
        target_buffer = _safe_float(protective.get("target_safe_buffer_pct"))
        auto_margin_enabled = protective.get("auto_margin_enabled")
        auto_margin_reduce_enabled = protective.get("auto_margin_reduce_enabled")
        margin_add_pct = _safe_float(protective.get("margin_add_pct"))
        margin_add_panic_pct = _safe_float(protective.get("margin_add_panic_pct"))
        margin_reduce_pct = _safe_float(protective.get("margin_reduce_pct"))
        margin_add_trigger_buffer_pct = _safe_float(protective.get("margin_add_trigger_buffer_pct"))
        margin_reduce_trigger_buffer_pct = _safe_float(protective.get("margin_reduce_trigger_buffer_pct"))
        margin_adjust_cooldown = protective.get("margin_adjust_cooldown_sec")
        enforce_isolated_margin = protective.get("enforce_isolated_margin")
        enforce_leverage = protective.get("enforce_leverage")
        target_leverage = _safe_float(protective.get("target_leverage"))
        kucoin_isolated_topup_only = protective.get("kucoin_isolated_topup_only")
        if warning_buffer is None:
            warning_buffer = self._risk_config.warning_buffer_pct
        if panic_buffer is None:
            panic_buffer = self._risk_config.panic_buffer_pct
        if min_free_abs is None:
            min_free_abs = self._risk_config.min_free_balance_abs
        if min_free_rel is None:
            min_free_rel = self._risk_config.min_free_balance_rel
        if target_buffer is None:
            target_buffer = self._risk_config.target_safe_buffer_pct
        self._accounts.update_alert_settings(
            send_margin_alerts=send_margin,
            send_missing_stop_alerts=bool(protective.get("send_missing_stop_alerts", True)),
            notification_primary_channel=notification_primary_channel,
            notification_fallback_channel=notification_fallback_channel,
            telegram_chat_id=telegram_chat_id,
            warning_buffer_pct=warning_buffer,
            panic_buffer_pct=panic_buffer,
            min_free_balance_abs=min_free_abs,
            min_free_balance_rel=min_free_rel,
            target_buffer_pct=target_buffer,
            auto_margin_enabled=auto_margin_enabled if auto_margin_enabled is not None else None,
            auto_margin_reduce_enabled=auto_margin_reduce_enabled if auto_margin_reduce_enabled is not None else None,
            enforce_isolated_margin=enforce_isolated_margin if enforce_isolated_margin is not None else None,
            enforce_leverage=enforce_leverage if enforce_leverage is not None else None,
            target_leverage=target_leverage,
            kucoin_isolated_topup_only=(
                bool(kucoin_isolated_topup_only) if kucoin_isolated_topup_only is not None else None
            ),
            margin_add_pct=margin_add_pct,
            margin_add_panic_pct=margin_add_panic_pct,
            margin_reduce_pct=margin_reduce_pct,
            margin_add_trigger_buffer_pct=margin_add_trigger_buffer_pct,
            margin_reduce_trigger_buffer_pct=margin_reduce_trigger_buffer_pct,
            margin_adjust_cooldown_sec=margin_adjust_cooldown,
        )
        self._protective_manager.update_config(self._risk_config)
        self._send_missing_stop_alerts = bool(
            protective.get("send_missing_stop_alerts", self._send_missing_stop_alerts)
        )


    def _running_manual_execution(self) -> dict[str, Any] | None:
        for exec_id, run in self._manual_runs.items():
            if run.get("status") == "running":
                return {
                    "execution_id": exec_id,
                    "action": run.get("action"),
                }
        return None


    async def _automation_cycle(self) -> None:
        """Run only production-owned automation modules.

        Auto Exit, Auto Strategy and position-reduction/de-risk decision loops
        are retired from runtime.  Grid keeps its independent state machine and
        execution safety checks.
        """
        if self._runtime_modules.auto_arb_grid:
            await self._auto_arb_cycle()

    async def _automation_scheduler(self) -> None:
        while True:
            try:
                await self._automation_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("automation loop failed: %s", exc)
            await asyncio.sleep(self._automation_poll_sec)



    async def _on_margin_adjust_events(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        for item in events:
            self._margin_logic_event(
                "action",
                {
                    "exchange": normalize_exchange_name(str(item.get("exchange") or "")),
                    "symbol": normalize_symbol(item.get("symbol")),
                    "side": str(item.get("side") or "").lower() or None,
                    "action": item.get("action"),
                    "amount": _safe_float(item.get("amount")),
                    "buffer_pct": (
                        (_safe_float(item.get("buffer_pct")) or 0.0) * 100.0
                        if item.get("buffer_pct") is not None
                        else None
                    ),
                    "target_buffer_pct": (
                        (_safe_float(item.get("target_buffer_pct")) or 0.0) * 100.0
                        if item.get("target_buffer_pct") is not None
                        else None
                    ),
                },
            )
        exchanges = {
            normalize_exchange_name(str(item.get("exchange") or ""))
            for item in events
            if str(item.get("exchange") or "").strip()
        }
        symbols = {
            normalize_symbol(item.get("symbol"))
            for item in events
            if normalize_symbol(item.get("symbol"))
        }
        logger.info(
            "margin adjust events: count=%s exchanges=%s symbols=%s; triggering protective sync",
            len(events),
            sorted(exchanges),
            sorted(symbols),
        )
        # Force a fresh positions pull before protective resync so target stops use
        # current liquidation prices right after margin changes.
        await self._accounts.refresh_now_for_protective(force_env=True)
        await self._maybe_sync_protective_orders(
            target_exchanges=exchanges or None,
            target_symbols=symbols or None,
            reason="margin_adjust",
            force_fetch_existing=True,
            verify_after_sync=True,
            emergency_retry=True,
        )

    async def _cleanup_verified_orphan_protective_targets(
        self,
        targets: Iterable[tuple[str, str]],
        *,
        reason: str,
    ) -> list[dict[str, Any]]:
        """Cancel protective orders only after a fresh successful empty position scan."""
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        if not bool(protective.get("orphan_cleanup_enabled", True)):
            return []
        normalized_targets = sorted(
            {
                (normalize_exchange_name(str(exchange)), normalize_symbol(symbol))
                for exchange, symbol in targets
                if normalize_exchange_name(str(exchange)) and normalize_symbol(symbol)
            }
        )
        if not normalized_targets:
            return []
        await self._accounts.refresh_now_for_protective(force_env=True)
        snapshot = self._accounts.snapshot() or {}
        positions = list(snapshot.get("positions") or [])
        now_ts = time.time()
        actions: list[dict[str, Any]] = []
        for exchange, symbol in normalized_targets:
            evidence = self._position_scan_evidence(
                snapshot,
                {exchange},
                now_ts=now_ts,
                stale_after_sec=90.0,
            )
            has_position = any(
                normalize_exchange_name(str(position.get("exchange") or "")) == exchange
                and _strip_settle(
                    normalize_symbol(
                        position.get("symbol_normalized")
                        or position.get("symbol")
                        or position.get("exchange_symbol")
                    )
                )
                == _strip_settle(symbol)
                for position in positions
            )
            if has_position or not bool(evidence.get("trusted")):
                actions.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "status": "cleanup_skipped_position_or_untrusted",
                        "has_position": has_position,
                        "position_evidence": evidence,
                    }
                )
                continue
            cleanup = await self._protective_manager.cleanup_orphaned_protective_orders(
                exchange,
                symbol,
                force_fetch_existing=True,
            )
            actions.extend(cleanup)
        logger.info(
            "protective orphan cleanup (%s): targets=%s actions=%s",
            reason,
            normalized_targets,
            [
                {
                    "exchange": item.get("exchange"),
                    "symbol": item.get("symbol"),
                    "status": item.get("status"),
                    "cancel_order_ids": item.get("cancel_order_ids"),
                }
                for item in actions
            ],
        )
        return actions

    async def _maybe_sweep_orphan_protective_orders(
        self,
        *,
        reason: str,
        force: bool = False,
    ) -> list[dict[str, Any]]:
        """Discover protection with no position and remove it after trusted scans."""
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        if not bool(protective.get("orphan_cleanup_enabled", True)):
            return []
        now_ts = time.time()
        if self._protective_orphan_sweep_inflight:
            return []
        if (
            not force
            and now_ts - float(self._protective_orphan_sweep_last_ts or 0.0)
            < float(self._protective_orphan_sweep_interval_sec)
        ):
            return []
        self._protective_orphan_sweep_inflight = True
        self._protective_orphan_sweep_last_ts = now_ts
        try:
            snapshot = self._accounts.snapshot() or {}
            exchange_health = snapshot.get("exchange_health") or {}
            healthy_exchanges = {
                normalize_exchange_name(str(exchange))
                for exchange, health in (
                    exchange_health.items()
                    if isinstance(exchange_health, Mapping)
                    else []
                )
                if str((health or {}).get("health") or "").lower() == "healthy"
            }
            if not healthy_exchanges:
                healthy_exchanges = self._account_monitor_enabled_exchanges()
            healthy_exchanges = {
                exchange
                for exchange in healthy_exchanges
                if bool(
                    self._position_scan_evidence(
                        snapshot,
                        {exchange},
                        now_ts=now_ts,
                        stale_after_sec=90.0,
                    ).get("trusted")
                )
            }
            # Gate's CCXT all-symbol conditional-order call currently fails
            # during public market bootstrap. Keep targeted post-exit cleanup,
            # but do not turn that unsupported global read into recurring noise.
            healthy_exchanges &= {"binance", "bitget", "bybit", "kucoin", "okx"}
            if not healthy_exchanges:
                return []
            discovery = await self._protective_manager.discover_open_protective_targets(
                healthy_exchanges
            )
            targets = {
                (
                    normalize_exchange_name(str(item.get("exchange") or "")),
                    normalize_symbol(item.get("symbol")),
                )
                for item in (discovery.get("targets") or [])
                if isinstance(item, Mapping)
            }
            targets = {
                (exchange, symbol)
                for exchange, symbol in targets
                if exchange and symbol
            }
            errors = list(discovery.get("errors") or [])
            if errors:
                logger.warning("protective orphan discovery issues (%s): %s", reason, errors)
            if not targets:
                return []
            return await self._cleanup_verified_orphan_protective_targets(
                targets,
                reason=reason,
            )
        finally:
            self._protective_orphan_sweep_inflight = False

    async def _maybe_sync_protective_orders(
        self,
        *,
        target_exchanges: set[str] | None = None,
        target_symbols: set[str] | None = None,
        reason: str = "scheduler",
        force_fetch_existing: bool = False,
        verify_after_sync: bool = False,
        emergency_retry: bool = False,
    ) -> None:
        """Best-effort protective order sync if enabled in settings."""
        settings = self._settings_manager.current
        protective = getattr(settings, "protective", {}) or {}
        auto_protect = bool(protective.get("auto_protect_enabled", True))
        auto_take = bool(protective.get("auto_take_enabled", True))
        if not auto_protect and not auto_take:
            return
        snapshot = self._accounts.snapshot()
        positions = snapshot.get("positions") or []
        if target_exchanges or target_symbols:
            filtered: list[dict[str, Any]] = []
            for pos in positions:
                exchange = normalize_exchange_name(str(pos.get("exchange") or ""))
                symbol = normalize_symbol(pos.get("symbol") or pos.get("symbol_normalized"))
                if target_exchanges and exchange not in target_exchanges:
                    continue
                if target_symbols and symbol not in target_symbols:
                    continue
                filtered.append(pos)
            positions = filtered
            if not positions:
                if target_exchanges and target_symbols and (auto_protect or auto_take):
                    await self._cleanup_verified_orphan_protective_targets(
                        {
                            (exchange, symbol)
                            for exchange in target_exchanges
                            for symbol in target_symbols
                        },
                        reason=reason,
                    )
                    return
                logger.info(
                    "protective sync skipped (%s): no matching positions for exchanges=%s symbols=%s",
                    reason,
                    sorted(target_exchanges or set()),
                    sorted(target_symbols or set()),
                )
                return
        if auto_protect or auto_take:
            try:
                actions = await self._protective_manager.sync_protective_orders(
                    positions,
                    force_fetch_existing=force_fetch_existing,
                )
                if actions:
                    if self._send_missing_stop_alerts:
                        await self._handle_mexc_protective_alerts(actions)
                    summary = {
                        "message": f"Protective orders synced ({reason})",
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
                        action_reason = action.get("reason") or action.get("error")
                        parts = [f"{exch}: {status}"]
                        if stop_val is not None:
                            parts.append(f"sl={stop_val}")
                        if take_val is not None:
                            parts.append(f"tp={take_val}")
                        if action_reason:
                            parts.append(f"reason={action_reason}")
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
                                (
                                    f"{f.get('exchange')} {f.get('symbol')} "
                                    f"status={_protective_issue_kind(f.get('error') or f.get('reason')) or f.get('status')} "
                                    f"err={f.get('error') or f.get('reason')}"
                                )
                                for f in failures
                            ),
                        )
                    else:
                        logger.info("protective sync ok: all stops/takes placed")
                if verify_after_sync:
                    verify_actions = await self._protective_manager.verify_protective_orders(
                        positions,
                        force_fetch_existing=True,
                    )
                    verify_issues = [
                        item
                        for item in verify_actions
                        if str(item.get("status") or "").lower() in {"issue", "error"}
                    ]
                    self._record_event(
                        "protective:verify",
                        {
                            "message": f"Protective verify completed ({reason})",
                            "count": len(verify_actions),
                            "issues": len(verify_issues),
                        },
                    )
                    if verify_issues:
                        logger.warning(
                            "protective verify issues (%s): %s",
                            reason,
                            "; ".join(
                                f"{v.get('exchange')} {v.get('symbol')} side={v.get('side')} reason={v.get('reason')}"
                                for v in verify_issues
                            ),
                        )
                        if emergency_retry:
                            issue_keys = {
                                (
                                    normalize_exchange_name(str(v.get("exchange") or "")),
                                    normalize_symbol(v.get("symbol")),
                                    str(v.get("side") or "").lower(),
                                )
                                for v in verify_issues
                            }
                            retry_positions: list[dict[str, Any]] = []
                            for pos in positions:
                                ex = normalize_exchange_name(str(pos.get("exchange") or ""))
                                sym = normalize_symbol(pos.get("symbol") or pos.get("symbol_normalized"))
                                side = str(pos.get("side") or "").lower()
                                if (ex, sym, side) in issue_keys:
                                    retry_positions.append(pos)
                            if retry_positions:
                                retry_actions = await self._protective_manager.sync_protective_orders(
                                    retry_positions,
                                    force_fetch_existing=True,
                                )
                                self._record_event(
                                    "protective:emergency_retry",
                                    {
                                        "message": f"Protective emergency retry executed ({reason})",
                                        "count": len(retry_actions),
                                    },
                                )
                                final_verify = await self._protective_manager.verify_protective_orders(
                                    retry_positions,
                                    force_fetch_existing=True,
                                )
                                unresolved = [
                                    item
                                    for item in final_verify
                                    if str(item.get("status") or "").lower() in {"issue", "error"}
                                ]
                                if unresolved:
                                    logger.error(
                                        "protective emergency unresolved (%s): %s",
                                        reason,
                                        "; ".join(
                                            f"{u.get('exchange')} {u.get('symbol')} side={u.get('side')} reason={u.get('reason')}"
                                            for u in unresolved
                                        ),
                                    )
                                else:
                                    logger.info(
                                        "protective emergency retry resolved all verify issues (%s)",
                                        reason,
                                    )
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Protective sync failed: %s", exc)
        if not target_exchanges and not target_symbols and reason == "scheduler":
            try:
                await self._maybe_sweep_orphan_protective_orders(
                    reason="protective_scheduler",
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Protective orphan sweep failed: %s", exc)


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
            sent = await self._accounts.send_notification_message(text, title="FeeArb MEXC protective alert")
            if sent:
                self._last_mexc_alert[key] = now

def _fmt_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
