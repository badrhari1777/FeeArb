from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional
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
from execution.storage import JsonStateStore
from execution.accounts import AccountMonitor, normalize_symbol
from risk.config import default_risk_config, RiskConfig
from risk.stop_manager import ProtectiveOrderManager
from utils import purge_expired
from utils.cache_db import get_or_fetch_funding_history
from utils.funding import (
    enrich_history_intervals,
    infer_funding_interval_hours,
    is_stale_next_funding_iso,
    parse_timestamp_ms,
    project_next_funding_time_iso,
)
from exchanges import get_adapter_cached, normalize_exchange_name
from config import BASE_DIR, STATE_DIR
from .market_data import MarketDataBus
from .manual_symbols import _normalize_input_symbol
from uuid import uuid4

FUNDING_CACHE_TTL_SEC = 120
POSITIONS_MARKET_CONCURRENCY = 3
AUTO_EXIT_POLL_SEC = 2.0
AUTO_EXIT_LOG_COOLDOWN_SEC = 30.0
AUTO_EXIT_DEFAULTS = {
    "max_runtime_sec": 600,
    "cooldown_sec": 300,
    "require_live": True,
    "auto_clear_no_position_sec": 120,
}
AUTO_EXIT_STATE_PATH = STATE_DIR / "auto_exit_rules.json"
MANUAL_EXEC_LOG_DIR = BASE_DIR / "logs" / "manual_exec"
COIN_ANALYSIS_CORE_EXCHANGES: tuple[str, ...] = ("binance", "okx")
COIN_ANALYSIS_CACHE_TTL_SEC = 90

RefreshResult = Literal["completed", "in_progress", "failed"]

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


def _funding_history_ts_ms(value: object) -> int | None:
    return parse_timestamp_ms(value)


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
    if name == "binance":
        return ccxt.binanceusdm(opts)
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
        candles_map: dict[int, dict[str, Any]] = {}

        def _ingest(rows: list[Any]) -> None:
            for row in rows or []:
                if not isinstance(row, (list, tuple)) or len(row) < 6:
                    continue
                ts_ms = _funding_history_ts_ms(row[0])
                if ts_ms is None:
                    continue
                candles_map[ts_ms] = {
                    "ts_ms": int(ts_ms),
                    "open": _safe_float(row[1]),
                    "high": _safe_float(row[2]),
                    "low": _safe_float(row[3]),
                    "close": _safe_float(row[4]),
                    "volume": _safe_float(row[5]),
                }

        # Try paginated pull first so large windows (e.g. 72h of 1m bars) are not truncated.
        try:
            now_ms = int(time.time() * 1000)
            minute_ms = 60 * 1000
            since_ms = now_ms - int(limit * minute_ms)
            cursor = since_ms
            guard = 0
            while guard < 16 and len(candles_map) < limit:
                guard += 1
                batch_limit = min(1000, max(100, limit - len(candles_map)))
                batch = client.fetch_ohlcv(cand, timeframe="1m", since=cursor, limit=batch_limit)
                if not batch:
                    break
                _ingest(batch)
                last_ts = _funding_history_ts_ms(batch[-1][0]) if isinstance(batch[-1], (list, tuple)) else None
                if last_ts is None:
                    break
                next_cursor = int(last_ts + minute_ms)
                if next_cursor <= cursor:
                    break
                cursor = next_cursor
                if cursor > now_ms + minute_ms:
                    break
            if candles_map:
                ordered = [candles_map[key] for key in sorted(candles_map.keys(), reverse=True)]
                return ordered[:limit]
        except Exception:  # pylint: disable=broad-except
            pass

        # Fallback: single-shot fetch.
        try:
            ohlcv = client.fetch_ohlcv(cand, timeframe="1m", limit=limit)
        except Exception:  # pylint: disable=broad-except
            continue
        _ingest(ohlcv or [])
        if candles_map:
            ordered = [candles_map[key] for key in sorted(candles_map.keys(), reverse=True)]
            return ordered[:limit]
    return []


def _load_funding_history_cached(
    exchange: str,
    exchange_symbol: str,
    canonical_symbol: str,
    limit: int,
    adapter: Any,
) -> list[dict]:
    """Fetch funding history with caching, falling back to adapter hook."""
    fetch_limit = max(limit, min(limit + 8, 220))
    if hasattr(adapter, "funding_history"):
        try:
            return adapter.funding_history(canonical_symbol, limit=fetch_limit)
        except Exception:  # pylint: disable=broad-except
            return []

    def _fetch() -> list[dict]:
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


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if percentile <= 0:
        return min(values)
    if percentile >= 100:
        return max(values)
    ordered = sorted(values)
    rank = (len(ordered) - 1) * (percentile / 100.0)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return ordered[low]
    frac = rank - low
    return ordered[low] * (1.0 - frac) + ordered[high] * frac


def _resolve_funding_interval_hours(
    history: list[dict[str, Any]],
    snapshot_interval: float | None,
) -> float | None:
    return infer_funding_interval_hours(history, snapshot_interval=snapshot_interval)


def _spread_series_from_candles(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
) -> list[dict[str, float]]:
    left_map: dict[int, float] = {}
    right_map: dict[int, float] = {}
    for row in left or []:
        ts_ms = _funding_history_ts_ms(row.get("ts_ms"))
        close = _safe_float(row.get("close"))
        if ts_ms and close is not None and close > 0:
            left_map[ts_ms] = close
    for row in right or []:
        ts_ms = _funding_history_ts_ms(row.get("ts_ms"))
        close = _safe_float(row.get("close"))
        if ts_ms and close is not None and close > 0:
            right_map[ts_ms] = close
    common = sorted(set(left_map.keys()) & set(right_map.keys()), reverse=True)
    series: list[dict[str, float]] = []
    for ts_ms in common:
        left_px = left_map[ts_ms]
        right_px = right_map[ts_ms]
        mid = (left_px + right_px) / 2.0
        if mid <= 0:
            continue
        spread_pct = (left_px - right_px) / mid * 100.0
        series.append(
            {
                "ts_ms": float(ts_ms),
                "left_close": left_px,
                "right_close": right_px,
                "spread_pct": spread_pct,
            }
        )
    return series


def _weighted_mean_recent(
    series: list[dict[str, float]],
    *,
    value_key: str,
    now_ts_ms: int,
    half_life_hours: float = 24.0,
) -> float | None:
    if not series:
        return None
    if half_life_hours <= 0:
        half_life_hours = 24.0
    lam = math.log(2.0) / half_life_hours
    weighted_sum = 0.0
    weights = 0.0
    for row in series:
        val = _safe_float(row.get(value_key))
        ts_ms = _funding_history_ts_ms(row.get("ts_ms"))
        if val is None or ts_ms is None:
            continue
        age_h = max(0.0, (now_ts_ms - ts_ms) / 1000.0 / 3600.0)
        w = math.exp(-lam * age_h)
        weighted_sum += val * w
        weights += w
    if weights <= 0:
        return None
    return weighted_sum / weights


def _oi_change_pct(history: list[dict[str, Any]], hours: int) -> float | None:
    if not history:
        return None
    ordered = sorted(
        history,
        key=lambda item: _funding_history_ts_ms(item.get("ts_ms") or item.get("timestamp")) or 0,
        reverse=True,
    )
    latest = ordered[0]
    latest_ts = _funding_history_ts_ms(latest.get("ts_ms") or latest.get("timestamp"))
    latest_val = _safe_float(latest.get("open_interest_notional") or latest.get("open_interest_contracts"))
    if latest_ts is None or latest_val is None or latest_val == 0:
        return None
    target_ts = latest_ts - int(hours * 3600 * 1000)
    baseline = None
    for row in ordered[1:]:
        ts_ms = _funding_history_ts_ms(row.get("ts_ms") or row.get("timestamp"))
        if ts_ms is None:
            continue
        if ts_ms <= target_ts:
            baseline = _safe_float(
                row.get("open_interest_notional") or row.get("open_interest_contracts")
            )
            break
    if baseline is None or baseline == 0:
        return None
    return (latest_val - baseline) / abs(baseline) * 100.0


class DataService:
    def __init__(self, settings_manager: SettingsManager | None = None) -> None:
        self._settings_manager = settings_manager or SettingsManager()
        self._parser_interval = self._settings_manager.current.parser_refresh_seconds
        self._exchange_interval = self._settings_manager.current.exchange_refresh_seconds
        self._account_interval = self._settings_manager.current.account_refresh_seconds
        self._positions_market_interval = self._settings_manager.current.positions_market_refresh_seconds
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
        self._last_snapshot_sources_at: Optional[datetime] = None
        self._last_snapshot_universe_key: tuple[str, ...] | None = None
        self._last_snapshot_exchanges_key: tuple[str, ...] | None = None
        self._last_source_flags_key: tuple[tuple[str, bool], ...] | None = None
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
        self._positions_market_sem = asyncio.Semaphore(POSITIONS_MARKET_CONCURRENCY)
        self._risk_config: RiskConfig = self._risk_config_from_settings()
        self._protective_manager = ProtectiveOrderManager(self._risk_config)
        self._last_protective: dict[tuple[str, str, str], dict[str, float | None]] = {}
        self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", 180)
        self._protective_task: Optional[asyncio.Task] = None
        self._rebalance_prev_positions: dict[tuple[str, str, str], float] = {}
        self._rebalance_last: dict[tuple[str, str], float] = {}
        self._rebalance_blocked_exchanges: set[str] = {"mexc"}
        self._market_data = MarketDataBus()
        self._manual = ManualTradeManager(orderbook_provider=self._market_data)
        self._manual_runs: Dict[str, dict[str, Any]] = {}
        self._manual_run_ttl = 3600
        self._auto_exit_store = JsonStateStore(AUTO_EXIT_STATE_PATH)
        self._auto_exit: dict[str, Any] = self._load_auto_exit_config()
        self._auto_exit_lock = asyncio.Lock()
        self._auto_exit_task: Optional[asyncio.Task] = None
        self._auto_exit_poll_sec = AUTO_EXIT_POLL_SEC
        self._auto_exit_inflight = False
        self._auto_exit_live_spreads: dict[str, float] = {}
        self._auto_exit_events: list[dict[str, Any]] = []
        self._auto_exit_event_limit = 60
        self._auto_exit_last_log_ts: dict[str, float] = {}
        self._auto_exit_log_cooldown_sec = AUTO_EXIT_LOG_COOLDOWN_SEC
        self._coin_analysis_cache: dict[tuple[str, int, int, tuple[str, ...]], tuple[float, dict[str, Any]]] = {}
        self._mexc_alert_cooldown = 600  # seconds
        self._last_mexc_alert: dict[tuple[str, str], float] = {}
        self._send_missing_stop_alerts = True
        self._apply_alert_settings()

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
        await self._refresh_positions_market_snapshots(force=True)
        await self._maybe_sync_protective_orders()
        if self._task is None:
            await self._restart_scheduler()
        if self._bootstrap_task is None or self._bootstrap_task.done():
            self._bootstrap_task = asyncio.create_task(self.refresh_markets())
        if self._positions_market_task is None:
            self._positions_market_task = asyncio.create_task(self._positions_market_scheduler())
        if self._protective_task is None:
            self._protective_task = asyncio.create_task(self._protective_scheduler())
        if self._auto_exit_task is None:
            self._auto_exit_task = asyncio.create_task(self._auto_exit_scheduler())
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
        if self._positions_market_task:
            self._positions_market_task.cancel()
            try:
                await self._positions_market_task
            except asyncio.CancelledError:
                pass
            self._positions_market_task = None
        if self._auto_exit_task:
            self._auto_exit_task.cancel()
            try:
                await self._auto_exit_task
            except asyncio.CancelledError:
                pass
            self._auto_exit_task = None
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

    def _sources_due(self) -> bool:
        if self._cached_sources is None or self._last_source_refresh is None:
            return True
        age = datetime.now(timezone.utc) - self._last_source_refresh
        return age.total_seconds() >= max(self._parser_interval, 1)

    async def refresh_markets(self, *, force_sources: bool = True) -> RefreshResult:
        async with self._lock:
            if self._in_progress:
                return "in_progress"
            prev_exchange_status = dict(self._exchange_status)
            prev_snapshot = self._snapshot
            prev_last_refreshed = self._last_refreshed
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
        source_flags_key = tuple(sorted((name, bool(enabled)) for name, enabled in source_flags.items()))
        source_flags_changed = (
            self._last_source_flags_key is not None and source_flags_key != self._last_source_flags_key
        )

        need_sources = (
            force_sources
            or sources is None
            or self._sources_due()
            or source_flags_changed
        )
        if need_sources:
            try:
                sources = await collect_sources_async(
                    progress_cb,
                    source_settings=source_flags,
                    exchange_settings=exchange_flags,
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
                self._last_source_flags_key = source_flags_key
        if sources is None:
            async with self._lock:
                self._last_error = "sources_unavailable"
                self._status = "error"
            return "failed"

        symbols = [str(entry.get("symbol") or "") for entry in sources.universe if entry.get("symbol")]
        universe_key = tuple(sorted(symbols))
        exchanges_key = tuple(sorted(name for name, enabled in exchange_flags.items() if enabled))
        skip_exchange_refresh = (
            not need_sources
            and self._snapshot is not None
            and self._last_snapshot_sources_at is not None
            and sources.generated_at == self._last_snapshot_sources_at
            and universe_key == (self._last_snapshot_universe_key or ())
            and exchanges_key == (self._last_snapshot_exchanges_key or ())
        )
        if skip_exchange_refresh:
            self._record_event(
                "refresh:skipped",
                {
                    "message": "Snapshot refresh skipped (sources unchanged)",
                    "reason": "sources_unchanged",
                    "generated_at": sources.generated_at.isoformat(),
                },
            )
            async with self._lock:
                self._snapshot = prev_snapshot
                self._status = "ready" if prev_snapshot else "idle"
                self._last_error = None
                self._last_refreshed = prev_last_refreshed
                self._exchange_status = prev_exchange_status
            async with self._lock:
                self._in_progress = False
            return "completed"

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
                self._last_snapshot_sources_at = sources.generated_at
                self._last_snapshot_universe_key = universe_key
                self._last_snapshot_exchanges_key = exchanges_key
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
            self._positions_market_interval = current.positions_market_refresh_seconds
            self._summary_interval = current.summary_refresh_seconds
            self._risk_config = self._risk_config_from_settings()
            self._protective_manager.update_config(self._risk_config)
            self._protective_interval = getattr(self._risk_config, "position_check_interval_sec", self._protective_interval)
            self._apply_alert_settings()
        await self._restart_scheduler()
        await self._restart_protective_scheduler()
        await self._restart_positions_market_scheduler()
        self._accounts.update_interval(self._account_interval)
        self._accounts.update_summary_interval(self._summary_interval)
        # Kick an async refresh so UI sees new cadence sooner.
        asyncio.create_task(self._accounts.refresh_now(force_env=True))
        asyncio.create_task(self._refresh_positions_market_snapshots(force=True))

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
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.exit(payload, positions)
        return await self._start_manual_run("exit", payload, positions)

    async def manual_roll(self, payload: dict[str, Any]) -> dict[str, Any]:
        positions = self._accounts.snapshot().get("positions") or []
        if payload.get("dry_run"):
            payload = dict(payload)
            payload.setdefault("constraints_exchanges", self._manual_pair_constraints(payload, action="roll"))
        if payload.get("dry_run") or not payload.get("async_run"):
            return await self._manual.roll(payload, positions)
        return await self._start_manual_run("roll", payload, positions)

    async def manual_analyze(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload)
        payload.setdefault(
            "constraints_exchanges",
            self._manual_pair_constraints(payload, action=payload.get("action") or "enter"),
        )
        return await self._manual.analyze(payload)

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
            base_url = getattr(adapter, "base_url", "https://api.gateio.ws/api/v4")
            ticker_url = f"{base_url}/futures/usdt/tickers?" + urlencode(
                {"contract": exchange_symbol}
            )
            fetch_and_add("snapshot", "tickers", ticker_url)
            contract_url = f"{base_url}/futures/usdt/contracts/{exchange_symbol}"
            fetch_and_add("snapshot", "contract", contract_url)
            history_url = f"{base_url}/futures/usdt/funding_rate?" + urlencode(
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

    async def manual_test_coin_analysis(self, payload: dict[str, Any]) -> dict[str, Any]:
        symbol = _normalize_input_symbol(str(payload.get("symbol") or ""))
        window_minutes = int(_safe_float(payload.get("window_minutes")) or 4320)
        funding_points = int(_safe_float(payload.get("funding_points")) or 120)
        include_series = bool(payload.get("include_series"))

        if not symbol:
            return {"errors": ["symbol is required"]}

        window_minutes = max(60, min(window_minutes, 4320))
        funding_points = max(24, min(funding_points, 200))
        try:
            analysis = await self.analyze_symbol(
                symbol,
                window_minutes=window_minutes,
                funding_points=funding_points,
            )
        except Exception as exc:  # pylint: disable=broad-except
            return {
                "errors": [str(exc)],
                "symbol": symbol,
                "window_minutes": window_minutes,
                "funding_points": funding_points,
            }

        if not include_series:
            for row in analysis.get("exchanges") or []:
                candles = row.pop("candles_1m", None)
                if candles is not None:
                    row["candles_1m_count"] = len(candles)

        pairs = list(analysis.get("pair_analysis") or [])
        best_pair = None
        if pairs:
            best_pair = max(
                pairs,
                key=lambda item: _safe_float(item.get("score")) or 0.0,
            )

        summary: dict[str, Any] = {
            "symbol": analysis.get("symbol"),
            "analysis_exchanges": analysis.get("analysis_exchanges"),
            "exchange_count": len(analysis.get("exchanges") or []),
            "pair_count": len(pairs),
            "bot_decision": (analysis.get("bot_logic") or {}).get("decision"),
            "bot_score": (analysis.get("bot_logic") or {}).get("score"),
        }
        if best_pair:
            spread = best_pair.get("spread") or {}
            funding = best_pair.get("funding_hourly") or {}
            summary["best_pair"] = {
                "left_exchange": best_pair.get("left_exchange"),
                "right_exchange": best_pair.get("right_exchange"),
                "score": best_pair.get("score"),
                "recommendation": best_pair.get("recommendation"),
                "spread_current_pct": spread.get("current_pct"),
                "spread_p95_abs_pct": spread.get("p95_abs_pct"),
                "spread_z_score": spread.get("z_score"),
                "spread_coverage_pct": spread.get("coverage_pct"),
                "funding_delta_hourly": funding.get("delta"),
                "reasons": best_pair.get("reasons") or [],
            }

        return {
            "symbol": analysis.get("symbol"),
            "window_minutes": window_minutes,
            "funding_points": funding_points,
            "include_series": include_series,
            "summary": summary,
            "analysis": analysis,
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
            params["leverage"] = int(DEFAULT_MANUAL_LEVERAGE)
            if not position_side:
                position_side = "BOTH"
        if position_side:
            if exchange != "kucoin":
                params["posSide"] = position_side
            params["positionSide"] = position_side

        try:
            if order_type == "limit":
                order = await client.create_order(ccxt_symbol, "limit", side, order_qty, limit_price, params)
            else:
                order = await client.create_order(ccxt_symbol, "market", side, order_qty, None, params)
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc)
            if exchange == "bitget" and "40774" in message:
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
            "logs": list(run.get("logs") or []),
            "result": run.get("result"),
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

    async def manual_exec_stop(self, exec_id: str) -> dict[str, Any]:
        self._prune_manual_runs()
        run = self._manual_runs.get(exec_id)
        if not run:
            return {"error": "execution_not_found"}
        if run.get("status") != "running":
            return {
                "execution_id": exec_id,
                "status": run.get("status"),
                "stop_requested": bool(run.get("stop_requested")),
            }
        run["stop_requested"] = True
        run["updated_at"] = datetime.now(timezone.utc).isoformat()
        run["updated_at_ts"] = time.time()
        logs = run.get("logs")
        if isinstance(logs, list):
            logs.append(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "event": "stop",
                    "message": "User stop requested",
                    "data": {},
                }
            )
            if len(logs) > 200:
                del logs[:-200]
        return {
            "execution_id": exec_id,
            "status": run.get("status"),
            "stop_requested": True,
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
            "log_path": str(log_path) if log_path else None,
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

        def _stop_cb() -> bool:
            return bool(run.get("stop_requested"))

        async def _runner() -> None:
            try:
                if action == "enter":
                    result = await self._manual.enter(payload, log_cb=_log_cb, stop_cb=_stop_cb)
                elif action == "exit":
                    result = await self._manual.exit(payload, positions or [], log_cb=_log_cb, stop_cb=_stop_cb)
                elif action == "roll":
                    result = await self._manual.roll(payload, positions or [], log_cb=_log_cb, stop_cb=_stop_cb)
                else:
                    result = {"errors": [f"unsupported manual action {action}"]}
                run["result"] = result
                if result.get("errors"):
                    run["status"] = "completed_with_errors"
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
            except Exception as exc:  # pylint: disable=broad-except
                run["status"] = "failed"
                run["error"] = str(exc)
                _append_log(
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "event": "exception",
                        "message": "Execution failed",
                        "data": {"error": str(exc)},
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
        positions_market_interval = int(
            settings_payload.get(
                "positions_market_refresh_seconds",
                self._positions_market_interval,
            )
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
            "positions_market_refresh_interval": positions_market_interval,
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
            "auto_exit": self.auto_exit_payload(),
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
        market_lookup, market_ts_lookup = self._positions_market_snapshot_lookup()
        positions_by_symbol, grouped = self._positions_by_symbol(
            positions,
            return_grouped=True,
            market_lookup=market_lookup,
            market_ts_lookup=market_ts_lookup,
        )
        payload["positions_by_symbol"] = positions_by_symbol
        payload["reduction_candidates"] = self._reduction_candidates(grouped, balances)
        payload["positions_market"] = self._positions_market_state()
        return payload

    def _positions_market_snapshot_lookup(
        self,
    ) -> tuple[dict[tuple[str, str], MarketSnapshot], dict[tuple[str, str], datetime]]:
        return dict(self._positions_market_cache), dict(self._positions_market_cache_ts)

    def _positions_market_state(self) -> dict[str, object]:
        last_updated = (
            self._positions_market_last_refresh.isoformat()
            if self._positions_market_last_refresh
            else None
        )
        symbols = len(self._positions_market_last_key or ())
        positions = self._accounts.snapshot().get("positions") or []
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
    def _parse_iso_ts(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        try:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            return datetime.fromisoformat(str(value)).astimezone(timezone.utc)
        except Exception:  # pylint: disable=broad-except
            return None

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
            notional = float(entry.get("notional") or 0.0)
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
            for sym in lookup_symbols:
                key = (exchange_name, sym)
                snapshot = market_lookup.get(key)
                if snapshot:
                    snapshot_ts = market_ts_lookup.get(key)
                    break
            entry_price = entry.get("entry_price")
            mark_price = entry.get("mark_price")
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
                if snapshot.next_funding_time:
                    if not snapshot_funding_stale:
                        next_funding_iso = snapshot.next_funding_time.isoformat()
                snap_mark = _safe_float(snapshot.mark_price)
                if snap_mark is not None:
                    mark_val = _safe_float(mark_price)
                    if mark_val is None or mark_val == 0:
                        mark_price = snap_mark
                    elif exchange_name == "bingx":
                        try:
                            delta_pct = abs(snap_mark - mark_val) / abs(mark_val) * 100.0
                        except Exception:
                            delta_pct = None
                        if delta_pct is None or delta_pct >= 0.1:
                            mark_price = snap_mark
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

        logger.info(
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
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
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
            cfg.position_check_interval_sec = int(
                protective.get("position_check_interval_sec", cfg.position_check_interval_sec)
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
        min_free_rel = _safe_float(protective.get("min_free_balance_rel"))
        target_buffer = _safe_float(protective.get("target_safe_buffer_pct"))
        auto_margin_enabled = protective.get("auto_margin_enabled")
        auto_margin_reduce_enabled = protective.get("auto_margin_reduce_enabled")
        margin_add_pct = _safe_float(protective.get("margin_add_pct"))
        margin_add_panic_pct = _safe_float(protective.get("margin_add_panic_pct"))
        margin_reduce_pct = _safe_float(protective.get("margin_reduce_pct"))
        margin_adjust_cooldown = protective.get("margin_adjust_cooldown_sec")
        enforce_isolated_margin = protective.get("enforce_isolated_margin")
        enforce_leverage = protective.get("enforce_leverage")
        target_leverage = _safe_float(protective.get("target_leverage"))
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
            margin_add_pct=margin_add_pct,
            margin_add_panic_pct=margin_add_panic_pct,
            margin_reduce_pct=margin_reduce_pct,
            margin_adjust_cooldown_sec=margin_adjust_cooldown,
        )
        self._send_missing_stop_alerts = bool(
            protective.get("send_missing_stop_alerts", self._send_missing_stop_alerts)
        )

    def _load_auto_exit_config(self) -> dict[str, Any]:
        payload = self._auto_exit_store.load({"defaults": dict(AUTO_EXIT_DEFAULTS), "rules": {}})
        return self._normalize_auto_exit_config(payload)

    @staticmethod
    def _normalize_auto_exit_config(payload: Mapping[str, Any] | None) -> dict[str, Any]:
        defaults = dict(AUTO_EXIT_DEFAULTS)
        rules: dict[str, Any] = {}
        if payload and isinstance(payload, Mapping):
            incoming_defaults = payload.get("defaults")
            if isinstance(incoming_defaults, Mapping):
                if incoming_defaults.get("max_runtime_sec") is not None:
                    try:
                        defaults["max_runtime_sec"] = int(incoming_defaults.get("max_runtime_sec"))
                    except Exception:
                        pass
                if incoming_defaults.get("cooldown_sec") is not None:
                    try:
                        defaults["cooldown_sec"] = max(0, int(incoming_defaults.get("cooldown_sec")))
                    except Exception:
                        pass
                if "require_live" in incoming_defaults:
                    defaults["require_live"] = bool(incoming_defaults.get("require_live"))
                if incoming_defaults.get("auto_clear_no_position_sec") is not None:
                    try:
                        defaults["auto_clear_no_position_sec"] = max(0, int(incoming_defaults.get("auto_clear_no_position_sec")))
                    except Exception:
                        pass
            incoming_rules = payload.get("rules")
            if isinstance(incoming_rules, Mapping):
                for key, rule in incoming_rules.items():
                    if not isinstance(rule, Mapping):
                        continue
                    symbol = str(rule.get("symbol") or "").upper().strip()
                    long_ex = normalize_exchange_name(str(rule.get("long_exchange") or ""))
                    short_ex = normalize_exchange_name(str(rule.get("short_exchange") or ""))
                    target = _safe_float(rule.get("target_spread_pct"))
                    if not symbol or not long_ex or not short_ex or target is None:
                        continue
                    rule_key = f"{symbol}|{long_ex}|{short_ex}"
                    rules[rule_key] = {
                        "symbol": symbol,
                        "long_exchange": long_ex,
                            "short_exchange": short_ex,
                            "target_spread_pct": float(target),
                            "enabled": bool(rule.get("enabled", True)),
                            "last_triggered_ts": float(rule.get("last_triggered_ts") or 0.0),
                            "updated_at": rule.get("updated_at"),
                            "missing_since_ts": float(rule.get("missing_since_ts") or 0.0),
                        }
        return {"defaults": defaults, "rules": rules}

    @staticmethod
    def _auto_exit_key(symbol: str, long_exchange: str, short_exchange: str) -> str:
        return f"{normalize_symbol(symbol)}|{normalize_exchange_name(long_exchange)}|{normalize_exchange_name(short_exchange)}"

    def auto_exit_payload(self) -> dict[str, Any]:
        payload = json.loads(json.dumps(self._auto_exit))
        payload["live_spreads"] = dict(self._auto_exit_live_spreads)
        payload["events"] = list(self._auto_exit_events)
        return payload

    async def update_auto_exit_defaults(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        async with self._auto_exit_lock:
            defaults = self._auto_exit.get("defaults", {})
            runtime = payload.get("max_runtime_sec")
            cooldown = payload.get("cooldown_sec")
            require_live = payload.get("require_live")
            auto_clear = payload.get("auto_clear_no_position_sec")
            if runtime is not None:
                defaults["max_runtime_sec"] = max(30, int(runtime))
            if cooldown is not None:
                defaults["cooldown_sec"] = max(0, int(cooldown))
            if require_live is not None:
                defaults["require_live"] = bool(require_live)
            if auto_clear is not None:
                defaults["auto_clear_no_position_sec"] = max(0, int(auto_clear))
            self._auto_exit["defaults"] = defaults
            self._auto_exit_store.save(self._auto_exit)
        return {"auto_exit": self.auto_exit_payload()}

    async def update_auto_exit_rule(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = str(payload.get("symbol") or "").upper().strip()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        enabled = bool(payload.get("enabled", True))
        target = _safe_float(payload.get("target_spread_pct"))
        if not symbol or not long_exchange or not short_exchange:
            raise ValueError("symbol, long_exchange, and short_exchange are required.")
        key = self._auto_exit_key(symbol, long_exchange, short_exchange)
        async with self._auto_exit_lock:
            rules = self._auto_exit.get("rules", {})
            if not enabled:
                rules.pop(key, None)
            else:
                if target is None:
                    raise ValueError("target_spread_pct is required when enabled.")
                now_iso = datetime.now(timezone.utc).isoformat()
                prev = rules.get(key, {})
                rules[key] = {
                    "symbol": symbol,
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "target_spread_pct": float(target),
                    "enabled": True,
                    "last_triggered_ts": float(prev.get("last_triggered_ts") or 0.0),
                    "updated_at": now_iso,
                    "missing_since_ts": 0.0,
                }
            self._auto_exit["rules"] = rules
            self._auto_exit_store.save(self._auto_exit)
        return {"auto_exit": self.auto_exit_payload()}

    def _auto_exit_has_running(self) -> bool:
        return self._auto_exit_running_exec() is not None

    def _auto_exit_running_exec(self) -> dict[str, Any] | None:
        for exec_id, run in self._manual_runs.items():
            if run.get("status") == "running":
                return {
                    "execution_id": exec_id,
                    "action": run.get("action"),
                }
        return None

    async def _auto_exit_scheduler(self) -> None:
        while True:
            try:
                await self._auto_exit_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("auto-exit loop failed: %s", exc)
            await asyncio.sleep(self._auto_exit_poll_sec)

    async def _auto_exit_cycle(self) -> None:
        if self._auto_exit_inflight:
            return
        self._auto_exit_inflight = True
        try:
            async with self._auto_exit_lock:
                config = json.loads(json.dumps(self._auto_exit))
            positions = self._accounts.snapshot().get("positions") or []
            if not positions:
                return
            _, grouped = self._positions_by_symbol(positions, return_grouped=True)
            rules = config.get("rules") or {}
            defaults = config.get("defaults") or {}
            max_runtime_sec = int(defaults.get("max_runtime_sec", AUTO_EXIT_DEFAULTS["max_runtime_sec"]))
            cooldown_sec = int(defaults.get("cooldown_sec", AUTO_EXIT_DEFAULTS["cooldown_sec"]))
            require_live = bool(defaults.get("require_live", AUTO_EXIT_DEFAULTS["require_live"]))
            auto_clear_sec = int(defaults.get("auto_clear_no_position_sec", AUTO_EXIT_DEFAULTS["auto_clear_no_position_sec"]))
            now_ts = time.time()

            live_spreads: dict[str, float] = {}
            for symbol_key, legs in grouped.items():
                longs = [leg for leg in legs if str(leg.get("side") or "").lower() == "long"]
                shorts = [leg for leg in legs if str(leg.get("side") or "").lower() == "short"]
                if len(longs) != 1 or len(shorts) != 1:
                    continue
                long_ex = normalize_exchange_name(str(longs[0].get("exchange") or ""))
                short_ex = normalize_exchange_name(str(shorts[0].get("exchange") or ""))
                if not long_ex or not short_ex:
                    continue
                spread = await self._auto_exit_live_spread(symbol_key, long_ex, short_ex)
                if spread is None:
                    continue
                live_spreads[self._auto_exit_key(symbol_key, long_ex, short_ex)] = float(spread)
            async with self._auto_exit_lock:
                self._auto_exit_live_spreads = live_spreads

            if not rules:
                return
            running = self._auto_exit_running_exec()
            if running:
                self._auto_exit_log_event(
                    "global",
                    "skip_running",
                    {"reason": "execution_running", **running},
                    now_ts,
                )
                return

            rules_to_remove: set[str] = set()
            rules_to_update: dict[str, dict[str, Any]] = {}

            def mark_missing(rule_key: str, rule_state: Mapping[str, Any], reason: str, payload: dict[str, Any]) -> bool:
                missing_since = float(rule_state.get("missing_since_ts") or 0.0)
                if missing_since <= 0:
                    missing_since = now_ts
                    rules_to_update[rule_key] = {"missing_since_ts": missing_since}
                elapsed = max(0.0, now_ts - missing_since)
                if auto_clear_sec and elapsed >= auto_clear_sec:
                    rules_to_remove.add(rule_key)
                    self._auto_exit_event(
                        "auto_clear",
                        {
                            "symbol": payload.get("symbol"),
                            "long_exchange": payload.get("long_exchange"),
                            "short_exchange": payload.get("short_exchange"),
                            "reason": reason,
                            "elapsed_sec": round(elapsed, 1),
                        },
                    )
                    return True
                if auto_clear_sec:
                    payload["auto_clear_remaining_sec"] = round(max(0.0, auto_clear_sec - elapsed), 1)
                self._auto_exit_log_event(rule_key, "skip", payload, now_ts)
                return True

            def clear_missing(rule_key: str, rule_state: Mapping[str, Any]) -> None:
                if float(rule_state.get("missing_since_ts") or 0.0) > 0:
                    rules_to_update[rule_key] = {"missing_since_ts": 0.0}

            for key, rule in rules.items():
                if not rule.get("enabled", True):
                    continue
                symbol = str(rule.get("symbol") or "").upper().strip()
                symbol_key = normalize_symbol(symbol)
                long_exchange = normalize_exchange_name(str(rule.get("long_exchange") or ""))
                short_exchange = normalize_exchange_name(str(rule.get("short_exchange") or ""))
                target = _safe_float(rule.get("target_spread_pct"))
                if not symbol or not long_exchange or not short_exchange or target is None:
                    continue
                last_trigger = float(rule.get("last_triggered_ts") or 0.0)
                if cooldown_sec and (now_ts - last_trigger) < cooldown_sec:
                    remaining = max(0, cooldown_sec - (now_ts - last_trigger))
                    self._auto_exit_log_event(
                        key,
                        "skip",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "cooldown",
                            "remaining_sec": round(remaining, 1),
                        },
                        now_ts,
                    )
                    continue
                legs = grouped.get(symbol_key) or []
                if not legs:
                    mark_missing(
                        key,
                        rule,
                        "no_position",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "no_position",
                        },
                    )
                    continue
                long_count = sum(1 for leg in legs if str(leg.get("side") or "").lower() == "long")
                short_count = sum(1 for leg in legs if str(leg.get("side") or "").lower() == "short")
                if long_count != 1 or short_count != 1:
                    mark_missing(
                        key,
                        rule,
                        "multi_leg",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "multi_leg",
                            "long_legs": long_count,
                            "short_legs": short_count,
                        },
                    )
                    continue
                long_leg = next(
                    (leg for leg in legs if str(leg.get("side") or "").lower() == "long" and normalize_exchange_name(str(leg.get("exchange") or "")) == long_exchange),
                    None,
                )
                short_leg = next(
                    (leg for leg in legs if str(leg.get("side") or "").lower() == "short" and normalize_exchange_name(str(leg.get("exchange") or "")) == short_exchange),
                    None,
                )
                if not long_leg or not short_leg:
                    mark_missing(
                        key,
                        rule,
                        "legs_missing",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "legs_missing",
                        },
                    )
                    continue
                qty_long = abs(float(long_leg.get("quantity") or 0.0))
                qty_short = abs(float(short_leg.get("quantity") or 0.0))
                qty = min(qty_long, qty_short)
                if qty <= 0:
                    mark_missing(
                        key,
                        rule,
                        "zero_qty",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "zero_qty",
                        },
                    )
                    continue
                clear_missing(key, rule)
                spread = live_spreads.get(self._auto_exit_key(symbol_key, long_exchange, short_exchange))
                if spread is None and require_live:
                    self._auto_exit_log_event(
                        key,
                        "skip",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "live_missing",
                        },
                        now_ts,
                    )
                    continue
                if spread is None:
                    continue
                if spread < float(target):
                    self._auto_exit_log_event(
                        key,
                        "wait",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "spread_pct": float(spread),
                            "target_pct": float(target),
                        },
                        now_ts,
                    )
                    continue
                if self._auto_exit_has_running():
                    break
                self._auto_exit_event(
                    "trigger",
                    {
                        "symbol": symbol,
                        "long_exchange": long_exchange,
                        "short_exchange": short_exchange,
                        "spread_pct": float(spread),
                        "target_pct": float(target),
                        "qty": qty,
                    },
                )
                payload = {
                    "symbol": symbol,
                    "qty": qty,
                    "notional": None,
                    "mode": "smart-exit",
                    "max_slippage_bps": 8,
                    "spread_min_pct": float(target),
                    "spread_max_pct": 10,
                    "timeout_sec": 0,
                    "max_runtime_sec": max_runtime_sec,
                    "reprice_sec": 3,
                    "chunk_qty": None,
                    "chunk_notional": None,
                    "force_chunk_qty": False,
                    "use_orderbook_check": False,
                    "fallback_to_market": False,
                    "async_run": True,
                    "dry_run": False,
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "margin_mode": "isolated",
                }
                result = await self.manual_exit(payload)
                logger.info(
                    "auto-exit triggered symbol=%s long=%s short=%s spread=%.4f target=%.4f result=%s",
                    symbol,
                    long_exchange,
                    short_exchange,
                    float(spread),
                    float(target),
                    result,
                )
                self._auto_exit_event(
                    "start",
                    {
                        "symbol": symbol,
                        "long_exchange": long_exchange,
                        "short_exchange": short_exchange,
                        "result": result,
                    },
                )
                async with self._auto_exit_lock:
                    stored_rules = self._auto_exit.get("rules", {})
                    stored = stored_rules.get(key)
                    if stored is not None:
                        stored["last_triggered_ts"] = now_ts
                        stored["updated_at"] = datetime.now(timezone.utc).isoformat()
                        self._auto_exit_store.save(self._auto_exit)
                break
            if rules_to_remove or rules_to_update:
                async with self._auto_exit_lock:
                    stored_rules = self._auto_exit.get("rules", {})
                    changed = False
                    for key in rules_to_remove:
                        if key in stored_rules:
                            stored_rules.pop(key, None)
                            changed = True
                    for key, updates in rules_to_update.items():
                        stored = stored_rules.get(key)
                        if stored is None:
                            continue
                        for field, value in updates.items():
                            stored[field] = value
                            changed = True
                    if changed:
                        self._auto_exit_store.save(self._auto_exit)
        finally:
            self._auto_exit_inflight = False

    async def _auto_exit_live_spread(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
    ) -> float | None:
        book_long = await self._market_data.get_orderbook(long_exchange, symbol, depth=5, max_age_sec=15.0)
        book_short = await self._market_data.get_orderbook(short_exchange, symbol, depth=5, max_age_sec=15.0)
        if not book_long or not book_short:
            return None
        bids_long = book_long.get("bids") or []
        asks_long = book_long.get("asks") or []
        bids_short = book_short.get("bids") or []
        asks_short = book_short.get("asks") or []
        if not bids_long or not asks_long or not bids_short or not asks_short:
            return None
        try:
            long_mid = (float(bids_long[0][0]) + float(asks_long[0][0])) / 2.0
            short_mid = (float(bids_short[0][0]) + float(asks_short[0][0])) / 2.0
        except Exception:
            return None
        if long_mid == 0:
            return None
        return (long_mid - short_mid) / long_mid * 100.0

    def _auto_exit_event(self, event: str, payload: Mapping[str, Any]) -> None:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
        }
        entry.update(dict(payload or {}))
        self._auto_exit_events.append(entry)
        if len(self._auto_exit_events) > self._auto_exit_event_limit:
            self._auto_exit_events = self._auto_exit_events[-self._auto_exit_event_limit :]

    def _auto_exit_log_event(
        self,
        key: str,
        event: str,
        payload: Mapping[str, Any],
        now_ts: float,
    ) -> None:
        log_key = f"{key}:{event}"
        last_ts = self._auto_exit_last_log_ts.get(log_key, 0.0)
        if (now_ts - last_ts) < self._auto_exit_log_cooldown_sec:
            return
        self._auto_exit_last_log_ts[log_key] = now_ts
        self._auto_exit_event(event, payload)

    def _rebalance_position_qty(self, position: Mapping[str, Any]) -> float | None:
        qty = _safe_float(position.get("coin_qty"))
        if qty is None or qty == 0:
            qty = _safe_float(position.get("contracts"))
        if qty is None or qty == 0:
            qty = _safe_float(position.get("amount"))
        return qty

    def _rebalance_position_side(
        self,
        position: Mapping[str, Any],
        qty_hint: float | None,
    ) -> str | None:
        raw_side = str(position.get("side") or "").lower()
        if raw_side in ("long", "short"):
            return raw_side
        if raw_side == "buy":
            return "long"
        if raw_side == "sell":
            return "short"
        if qty_hint is None:
            return None
        if qty_hint < 0:
            return "short"
        if qty_hint > 0:
            return "long"
        return None

    def _rebalance_positions_snapshot(
        self,
        positions: list[dict[str, Any]],
    ) -> dict[tuple[str, str, str], float]:
        snapshot: dict[tuple[str, str, str], float] = {}
        for position in positions or []:
            exchange = normalize_exchange_name(str(position.get("exchange") or ""))
            if not exchange:
                continue
            raw_symbol = (
                position.get("symbol")
                or position.get("symbol_normalized")
                or position.get("exchange_symbol")
            )
            symbol_norm = _strip_settle(normalize_symbol(str(raw_symbol or "")))
            if not symbol_norm:
                continue
            qty_raw = self._rebalance_position_qty(position)
            side = self._rebalance_position_side(position, qty_raw)
            if not side or qty_raw is None:
                continue
            qty = abs(qty_raw)
            if qty <= 0:
                continue
            key = (symbol_norm, exchange, side)
            snapshot[key] = snapshot.get(key, 0.0) + qty
        return snapshot

    async def _maybe_rebalance_positions(self, positions: list[dict[str, Any]]) -> None:
        settings = self._settings_manager.current
        protective = getattr(settings, "protective", {}) or {}
        auto_rebalance = bool(protective.get("auto_rebalance_enabled", False))
        current = self._rebalance_positions_snapshot(positions)
        if not auto_rebalance:
            self._rebalance_prev_positions = current
            return
        if not self._rebalance_prev_positions:
            self._rebalance_prev_positions = current
            return
        delta_pct = _safe_float(protective.get("rebalance_delta_pct"))
        delta_pct = 0.2 if delta_pct is None else max(0.0, min(delta_pct, 1.0))
        cooldown = int(protective.get("rebalance_cooldown_sec", 120) or 0)
        limit_timeout = int(protective.get("rebalance_limit_timeout_sec", 10) or 10)
        limit_offset_bps = _safe_float(protective.get("rebalance_limit_offset_bps")) or 0.0
        max_slippage_bps = _safe_float(protective.get("rebalance_max_slippage_bps")) or 0.0
        now_ts = time.time()
        reductions: dict[tuple[str, str], float] = {}
        for key, prev_qty in self._rebalance_prev_positions.items():
            if prev_qty <= 0:
                continue
            current_qty = current.get(key, 0.0)
            drop = prev_qty - current_qty
            if drop <= 0:
                continue
            if drop < prev_qty * delta_pct:
                continue
            symbol, _exchange, side = key
            reduce_side = "long" if side == "short" else "short"
            reductions[(symbol, reduce_side)] = reductions.get((symbol, reduce_side), 0.0) + drop
        actions: list[dict[str, Any]] = []
        for (symbol, reduce_side), drop_qty in reductions.items():
            cooldown_key = (symbol, reduce_side)
            last_ts = self._rebalance_last.get(cooldown_key)
            if last_ts is not None and cooldown > 0 and now_ts - last_ts < cooldown:
                continue
            symbol_actions: list[dict[str, Any]] = []
            legs: list[dict[str, Any]] = []
            for position in positions or []:
                exchange = normalize_exchange_name(str(position.get("exchange") or ""))
                if not exchange or exchange in self._rebalance_blocked_exchanges:
                    continue
                raw_symbol = (
                    position.get("symbol")
                    or position.get("symbol_normalized")
                    or position.get("exchange_symbol")
                )
                normalized_symbol = normalize_symbol(str(raw_symbol or ""))
                symbol_norm = _strip_settle(normalized_symbol)
                if symbol_norm != symbol:
                    continue
                symbol_trade = _dedupe_settle(normalized_symbol)
                qty_raw = self._rebalance_position_qty(position)
                side = self._rebalance_position_side(position, qty_raw)
                if not side or side != reduce_side or qty_raw is None:
                    continue
                qty = abs(qty_raw)
                if qty <= 0:
                    continue
                legs.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol_trade,
                        "qty": qty,
                        "margin_mode": position.get("margin_mode"),
                    }
                )
            total_qty = sum(leg["qty"] for leg in legs)
            if total_qty <= 0:
                continue
            target_qty = min(drop_qty, total_qty)
            remaining = target_qty
            order_side = "sell" if reduce_side == "long" else "buy"
            for leg in sorted(legs, key=lambda item: item["qty"], reverse=True):
                if remaining <= 0:
                    break
                leg_qty = min(remaining, leg["qty"])
                try:
                    result = await self._manual.agent_rebalance(
                        exchange=leg["exchange"],
                        symbol=leg.get("symbol") or symbol,
                        side=order_side,
                        qty_base=leg_qty,
                        margin_mode=leg.get("margin_mode"),
                        limit_timeout_sec=limit_timeout,
                        limit_offset_bps=limit_offset_bps,
                        max_slippage_bps=max_slippage_bps,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    result = {
                        "exchange": leg["exchange"],
                        "status": "error",
                        "error": str(exc),
                        "requested_qty": leg_qty,
                    }
                actions.append(
                    {
                        "symbol": symbol,
                        "symbol_trade": leg.get("symbol") or symbol,
                        "side": reduce_side,
                        "exchange": leg["exchange"],
                        "requested_qty": leg_qty,
                        "result": result,
                    }
                )
                symbol_actions.append(actions[-1])
                filled_qty = _safe_float(result.get("filled_qty")) or 0.0
                if filled_qty <= 0 and result.get("status") == "filled":
                    filled_qty = leg_qty
                remaining = max(0.0, remaining - filled_qty)
            if symbol_actions:
                self._rebalance_last[cooldown_key] = now_ts
        if actions:
            self._record_event(
                "protective:rebalance",
                {
                    "message": "Auto rebalance executed",
                    "count": len(actions),
                    "actions": actions,
                },
            )
        self._rebalance_prev_positions = current

    async def _maybe_sync_protective_orders(self) -> None:
        """Best-effort protective order sync if enabled in settings."""
        settings = self._settings_manager.current
        protective = getattr(settings, "protective", {}) or {}
        auto_protect = bool(protective.get("auto_protect_enabled", True))
        auto_take = bool(protective.get("auto_take_enabled", True))
        auto_rebalance = bool(protective.get("auto_rebalance_enabled", False))
        if not auto_protect and not auto_take and not auto_rebalance:
            return
        snapshot = self._accounts.snapshot()
        positions = snapshot.get("positions") or []
        if auto_protect or auto_take:
            try:
                actions = await self._protective_manager.sync_protective_orders(
                    positions,
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
        if auto_rebalance:
            try:
                await self._maybe_rebalance_positions(positions)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Protective rebalance failed: %s", exc)

    async def analyze_symbol(
        self,
        symbol: str,
        *,
        window_minutes: int = 4320,
        funding_points: int = 120,
    ) -> dict[str, Any]:
        """Collect on-demand historical analytics for a symbol."""
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol must be provided for analysis.")

        window = max(60, min(int(window_minutes), 4320))
        funding_limit = max(24, min(int(funding_points), 200))

        exchange_flags = getattr(self._settings_manager.current, "analysis_exchanges", None) or {}
        enabled_exchanges = {
            normalize_exchange_name(name)
            for name, enabled in exchange_flags.items()
            if enabled
        }
        selected_exchanges = [
            ex for ex in COIN_ANALYSIS_CORE_EXCHANGES if not enabled_exchanges or ex in enabled_exchanges
        ]
        if not selected_exchanges:
            raise ValueError("Enable at least one core analysis exchange (binance/okx).")
        global_warnings: list[str] = []
        if len(selected_exchanges) < 2:
            global_warnings.append("pair_analysis_limited: less than 2 core exchanges enabled")

        cache_key = (canonical, window, funding_limit, tuple(selected_exchanges))
        now_ts = time.time()
        cached = self._coin_analysis_cache.get(cache_key)
        if cached:
            cached_at, cached_payload = cached
            if now_ts - cached_at <= COIN_ANALYSIS_CACHE_TTL_SEC:
                out = dict(cached_payload)
                out["cache_hit"] = True
                return out

        tasks = [
            self._analyze_symbol_on_exchange(ex, canonical, window, funding_limit)
            for ex in selected_exchanges
        ]
        results = await asyncio.gather(*tasks)
        exchange_rows = [item for item in results if item]

        pair_analysis: list[dict[str, Any]] = []
        for i in range(len(exchange_rows)):
            for j in range(i + 1, len(exchange_rows)):
                pair_analysis.append(
                    self._analyze_pair(exchange_rows[i], exchange_rows[j], window)
                )
        bot_logic = self._decide_coin_candidate(pair_analysis)

        response = {
            "symbol": canonical,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "window_minutes": window,
            "funding_points": funding_limit,
            "analysis_exchanges": selected_exchanges,
            "warnings": global_warnings,
            "exchanges": exchange_rows,
            "pair_analysis": pair_analysis,
            "bot_logic": bot_logic,
            "cache_hit": False,
        }
        self._coin_analysis_cache[cache_key] = (now_ts, response)
        if len(self._coin_analysis_cache) > 32:
            oldest_key = min(
                self._coin_analysis_cache.keys(),
                key=lambda key_item: self._coin_analysis_cache[key_item][0],
            )
            self._coin_analysis_cache.pop(oldest_key, None)
        return response

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
            adapter = get_adapter_cached(exchange)
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
        snapshot_dict: dict[str, Any] = {}
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
        if "snapshot" not in result:
            result["snapshot"] = {}

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
                key=lambda item: _funding_history_ts_ms(
                    item.get("ts_ms") or item.get("timestamp")
                )
                or 0,
                reverse=True,
            )
            funding_history = funding_history[:funding_points]
        result["funding_history"] = funding_history
        funding_interval_hours = _resolve_funding_interval_hours(
            funding_history,
            _safe_float(snapshot_dict.get("funding_interval_hours")),
        )
        result["funding_interval_hours_resolved"] = funding_interval_hours
        latest_funding_rate = None
        if funding_history:
            latest_funding_rate = _safe_float(funding_history[0].get("rate"))
        if latest_funding_rate is None:
            latest_funding_rate = _safe_float(snapshot_dict.get("funding_rate"))
        result["latest_funding_rate"] = latest_funding_rate

        # Recent 1m candles for spread/time-sync analysis.
        try:
            candles = await asyncio.to_thread(
                self._fetch_candles_for_exchange,
                exchange,
                exchange_symbol,
                canonical_symbol,
                window_minutes,
            )
            candles = sorted(
                candles or [],
                key=lambda row: _funding_history_ts_ms(row.get("ts_ms")) or 0,
                reverse=True,
            )
            if candles:
                result["candles_1m"] = candles
            else:
                warnings.append("candles_unavailable")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"candles:{exc}")

        # Open interest history (only exchanges with robust historical API in this phase).
        oi_payload = await asyncio.to_thread(
            self._fetch_open_interest_for_exchange,
            exchange,
            canonical_symbol,
            window_minutes,
        )
        result["open_interest"] = oi_payload
        if oi_payload.get("status") not in ("ok", "partial"):
            warnings.append(f"oi:{oi_payload.get('status') or 'unavailable'}")

        candles_count = len(result.get("candles_1m") or [])
        expected = max(1, window_minutes)
        coverage_pct = candles_count / expected * 100.0
        result["data_quality"] = {
            "candles_expected": expected,
            "candles_received": candles_count,
            "candles_coverage_pct": round(coverage_pct, 2),
            "funding_points_received": len(funding_history),
            "oi_points_received": len(oi_payload.get("history") or []),
        }
        if coverage_pct < 70:
            warnings.append("candles_coverage_low")

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

    def _analyze_pair(
        self,
        left: Mapping[str, Any],
        right: Mapping[str, Any],
        window_minutes: int,
    ) -> dict[str, Any]:
        left_ex = str(left.get("exchange") or "")
        right_ex = str(right.get("exchange") or "")
        series = _spread_series_from_candles(
            list(left.get("candles_1m") or []),
            list(right.get("candles_1m") or []),
        )
        now_ts_ms = int(time.time() * 1000)
        spread_values = [float(row.get("spread_pct") or 0.0) for row in series]
        current_spread = spread_values[0] if spread_values else None
        p25 = _percentile(spread_values, 25)
        p50 = _percentile(spread_values, 50)
        p75 = _percentile(spread_values, 75)
        p95 = _percentile([abs(v) for v in spread_values], 95)
        mean = sum(spread_values) / len(spread_values) if spread_values else None
        std = None
        if spread_values and len(spread_values) > 1 and mean is not None:
            var = sum((v - mean) ** 2 for v in spread_values) / len(spread_values)
            std = math.sqrt(max(0.0, var))
        z_score = None
        if current_spread is not None and mean is not None and std and std > 1e-12:
            z_score = (current_spread - mean) / std
        weighted_mean = _weighted_mean_recent(
            series,
            value_key="spread_pct",
            now_ts_ms=now_ts_ms,
            half_life_hours=24.0,
        )
        coverage_pct = (len(series) / max(1, window_minutes)) * 100.0

        int_left = _safe_float(left.get("funding_interval_hours_resolved"))
        int_right = _safe_float(right.get("funding_interval_hours_resolved"))
        interval_match = (
            int_left is not None
            and int_right is not None
            and abs(int_left - int_right) <= 0.05
        )

        left_rate = _safe_float(left.get("latest_funding_rate"))
        right_rate = _safe_float(right.get("latest_funding_rate"))
        left_hourly = (left_rate / int_left) if left_rate is not None and int_left else None
        right_hourly = (right_rate / int_right) if right_rate is not None and int_right else None
        funding_delta_hourly = (
            left_hourly - right_hourly
            if left_hourly is not None and right_hourly is not None
            else None
        )

        oi_left = (left.get("open_interest") or {}).get("history") or []
        oi_right = (right.get("open_interest") or {}).get("history") or []
        oi_left_6h = _oi_change_pct(list(oi_left), 6)
        oi_right_6h = _oi_change_pct(list(oi_right), 6)
        oi_divergence_6h = None
        if oi_left_6h is not None and oi_right_6h is not None:
            oi_divergence_6h = abs(oi_left_6h - oi_right_6h)

        score = 50.0
        reasons: list[str] = []
        if not interval_match:
            score -= 30.0
            reasons.append("funding_interval_mismatch")
        if coverage_pct < 70.0:
            score -= 20.0
            reasons.append("spread_history_low_coverage")
        if funding_delta_hourly is not None:
            score += min(25.0, abs(funding_delta_hourly) * 100000.0)
        else:
            score -= 10.0
            reasons.append("funding_delta_unavailable")
        if z_score is not None and abs(z_score) >= 2.5:
            score -= 15.0
            reasons.append("spread_extreme_zscore")
        if p95 is not None and current_spread is not None and abs(current_spread) > 1e-9:
            tail_ratio = p95 / abs(current_spread)
            if tail_ratio >= 3.0:
                score -= 10.0
                reasons.append("historical_tail_risk")
        if oi_divergence_6h is not None and oi_divergence_6h >= 25.0:
            score -= 10.0
            reasons.append("oi_divergence_high")

        score = max(0.0, min(100.0, score))
        recommendation = "reject"
        if interval_match and coverage_pct >= 70.0 and score >= 70.0:
            recommendation = "enter_candidate"
        elif interval_match and coverage_pct >= 50.0 and score >= 50.0:
            recommendation = "watch"

        return {
            "left_exchange": left_ex,
            "right_exchange": right_ex,
            "window_minutes": window_minutes,
            "funding_interval_hours": {
                "left": int_left,
                "right": int_right,
                "match": interval_match,
            },
            "funding_hourly": {
                "left": left_hourly,
                "right": right_hourly,
                "delta": funding_delta_hourly,
            },
            "spread": {
                "points": len(series),
                "coverage_pct": round(coverage_pct, 2),
                "current_pct": current_spread,
                "weighted_mean_pct": weighted_mean,
                "p25_pct": p25,
                "p50_pct": p50,
                "p75_pct": p75,
                "p95_abs_pct": p95,
                "z_score": z_score,
            },
            "open_interest": {
                "left_change_6h_pct": oi_left_6h,
                "right_change_6h_pct": oi_right_6h,
                "divergence_6h_pct": oi_divergence_6h,
            },
            "score": round(score, 2),
            "recommendation": recommendation,
            "reasons": reasons,
        }

    def _decide_coin_candidate(self, pairs: list[Mapping[str, Any]]) -> dict[str, Any]:
        if not pairs:
            return {
                "decision": "reject",
                "reason": "no_pairs_available",
                "recommended_pair": None,
                "score": 0,
            }
        ordered = sorted(
            pairs,
            key=lambda row: float(row.get("score") or 0.0),
            reverse=True,
        )
        best = ordered[0]
        best_score = float(best.get("score") or 0.0)
        reco = str(best.get("recommendation") or "reject")
        decision = "reject"
        if reco == "enter_candidate":
            decision = "enter_candidate"
        elif reco == "watch":
            decision = "watch"
        return {
            "decision": decision,
            "score": round(best_score, 2),
            "recommended_pair": {
                "left_exchange": best.get("left_exchange"),
                "right_exchange": best.get("right_exchange"),
            },
            "reason": "best_pair_score",
            "pair_reasons": list(best.get("reasons") or []),
            "note": "decision is advisory; execute only after manual dry-run checks",
        }

    def _fetch_open_interest_for_exchange(
        self,
        exchange: str,
        canonical_symbol: str,
        window_minutes: int,
    ) -> dict[str, Any]:
        name = normalize_exchange_name(exchange)
        if name not in ("binance", "okx"):
            return {
                "status": "unsupported",
                "history": [],
                "current": None,
                "error": "oi_history_not_supported_in_phase",
            }
        client = _ccxt_client(name)
        if client is None:
            return {
                "status": "error",
                "history": [],
                "current": None,
                "error": "ccxt_client_unavailable",
            }
        try:
            client.load_markets()
        except Exception:
            pass
        candidates = [
            _ccxt_perp_symbol(canonical_symbol),
            canonical_symbol,
        ]
        history_raw = None
        symbol_used = None
        since_ms = int(time.time() * 1000) - int(max(60, window_minutes) * 60 * 1000)
        limit = max(24, min(300, int(window_minutes / 60) + 16))
        seen: set[str] = set()
        for cand in candidates:
            if not cand or cand in seen:
                continue
            seen.add(cand)
            try:
                history_raw = client.fetch_open_interest_history(cand, "1h", since_ms, limit)
                if history_raw:
                    symbol_used = cand
                    break
            except Exception:
                continue
        history: list[dict[str, Any]] = []
        for row in history_raw or []:
            ts_ms = _funding_history_ts_ms(row.get("timestamp") or row.get("ts"))
            if not ts_ms:
                continue
            oi_contracts = _safe_float(
                row.get("openInterestAmount")
                or row.get("openInterest")
                or row.get("baseVolume")
            )
            oi_notional = _safe_float(
                row.get("openInterestValue")
                or row.get("quoteVolume")
                or row.get("openInterestUsd")
            )
            history.append(
                {
                    "ts_ms": ts_ms,
                    "open_interest_contracts": oi_contracts,
                    "open_interest_notional": oi_notional,
                }
            )
        history.sort(key=lambda item: item.get("ts_ms") or 0, reverse=True)

        current = None
        if symbol_used:
            try:
                now_row = client.fetch_open_interest(symbol_used)
                current = {
                    "ts_ms": _funding_history_ts_ms(now_row.get("timestamp")),
                    "open_interest_contracts": _safe_float(
                        now_row.get("openInterestAmount")
                        or now_row.get("openInterest")
                        or now_row.get("baseVolume")
                    ),
                    "open_interest_notional": _safe_float(
                        now_row.get("openInterestValue")
                        or now_row.get("quoteVolume")
                        or now_row.get("openInterestUsd")
                    ),
                }
            except Exception:
                current = None
        if not history and not current:
            return {
                "status": "empty",
                "history": [],
                "current": None,
            }
        status = "ok" if history else "partial"
        return {
            "status": status,
            "history": history,
            "current": current,
            "symbol": symbol_used,
            "source": "ccxt_open_interest_history_1h",
        }

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
