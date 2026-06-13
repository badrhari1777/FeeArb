from __future__ import annotations

import asyncio
import csv
import io
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
from execution.manual import (
    AUTO_EXIT_MARKET_FALLBACK_MAX_TIER,
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
from execution.accounts import _safe_float
from execution.allocator import Allocator
from execution.lifecycle import LifecycleController
from execution.settings import ExecutionSettings
from execution.storage import JsonStateStore, JsonlEventStore
from execution.auto_arb_grid import (
    build_grid_levels,
    decide_grid_transition,
    normalize_level_count,
    recommend_level_count,
)
from execution.auto_strategies import (
    StrategyCandidate,
    action_priority,
    choose_candidate,
    current_step,
    reconcile_step_progress,
    trigger_edge,
    trigger_matches,
)
from execution.accounts import AccountMonitor, normalize_symbol
from risk.config import default_risk_config, RiskConfig
from risk.derisk_manager import (
    build_exchange_health,
    classify_residual_leg,
    derisk_candidate_score,
    derive_cluster_rules,
    exchange_stress_state,
    hedged_pair_key,
    normalize_hedge_cluster_config,
    panic_severity,
    price_velocity_bps,
    qty_mismatch_ratio,
    standalone_key,
)
from risk.stop_manager import ProtectiveOrderManager
from utils.notifications import NotificationRouter
from utils import purge_expired
from utils.cache_db import get_or_fetch_funding_history
from utils.funding import (
    enrich_history_intervals,
    infer_funding_interval_hours,
    is_stale_next_funding_iso,
    normalize_interval_hours,
    parse_timestamp_ms,
    project_next_funding_time_iso,
)
from analysis_storage import (
    CoinCandidateShortlistRow,
    CoinDecisionRow,
    CoinFeatureSnapshotRow,
    CoinFocusSnapshotRow,
    CoinFundingHistoryRow,
    CoinOpenInterestHistoryRow,
    CoinPaperPositionRow,
    CoinRealPositionObservationRow,
    CoinSymbolSessionRow,
    CoinTradeActivityRow,
    expire_symbol_sessions,
    get_active_symbol_sessions,
    get_candidate_shortlist,
    get_coin_analysis_table_counts,
    get_decisions,
    get_feature_snapshot_by_id,
    get_feature_snapshots,
    get_funding_history,
    get_focus_snapshots,
    get_open_interest_history,
    get_outcomes,
    get_paper_events,
    get_paper_positions,
    get_real_position_observations,
    get_trade_activity,
    insert_candidate_shortlist_rows,
    insert_decision,
    insert_feature_snapshot,
    insert_focus_snapshot,
    insert_outcome,
    insert_paper_event,
    insert_real_position_observation,
    insert_trade_activity,
    prune_coin_analysis_data,
    upsert_funding_history_rows,
    upsert_open_interest_history_rows,
    upsert_paper_position,
    upsert_symbol_session,
)
from analysis_registry import build_pair_key
from analysis_features import build_pair_feature_snapshots
from analysis_decisions.constants import COIN_ANALYSIS_FEATURE_SET_VERSION
from analysis_decisions import (
    action_from_entry_score,
    evaluate_candidate_pairs,
    evaluate_position_signal,
    normalize_reason_codes,
)
from exchanges import ADAPTER_FACTORIES, get_adapter_cached, normalize_exchange_name
from config import BASE_DIR, STATE_DIR, EXCHANGE_COMMISSIONS, SUPPORTED_EXCHANGES
from .market_data import MarketDataBus
from .manual_symbols import _normalize_input_symbol
from uuid import uuid4

FUNDING_CACHE_TTL_SEC = 120
POSITIONS_MARKET_CONCURRENCY = 3
AUTO_EXIT_POLL_SEC = 2.0
AUTO_EXIT_LOG_COOLDOWN_SEC = 30.0
DERISK_LOG_COOLDOWN_SEC = 15.0
DERISK_EVENT_LIMIT = 80
DERISK_OUTCOME_HORIZONS_SEC = {
    "1m": 60.0,
    "5m": 300.0,
    "15m": 900.0,
}
AUTO_EXIT_DEFAULTS = {
    "max_runtime_sec": 120,
    "cooldown_sec": 300,
    "require_live": True,
    "auto_clear_no_position_sec": 120,
    "restore_spread_on_missing": True,
}
AUTO_EXIT_STATE_PATH = STATE_DIR / "auto_exit_rules.json"
AUTO_EXIT_HISTORY_PATH = BASE_DIR / "logs" / "auto_exit_history.jsonl"
AUTO_ARB_STATE_PATH = STATE_DIR / "auto_arb_rules.json"
AUTO_ARB_HISTORY_PATH = BASE_DIR / "logs" / "auto_arb_history.jsonl"
AUTO_STRATEGY_STATE_PATH = STATE_DIR / "auto_strategies.json"
AUTO_STRATEGY_HISTORY_PATH = BASE_DIR / "logs" / "auto_strategy_history.jsonl"
AUTO_STRATEGY_DEFAULTS = {
    "completion_tolerance_pct": 1.0,
    "max_runtime_sec": 120,
    "poll_sec": 2.0,
    "balance_retry_sec": 60,
}
AUTO_ARB_LIVE_MAX_CHUNK_NOTIONAL_USD = 50.0
AUTO_ARB_LIVE_MAX_TOTAL_NOTIONAL_USD = 100.0
HEDGE_CLUSTER_STATE_PATH = STATE_DIR / "hedge_clusters.json"
DERISK_HISTORY_PATH = BASE_DIR / "logs" / "derisk_history.jsonl"
DERISK_OUTCOME_STATE_PATH = STATE_DIR / "derisk_outcome_state.json"
AUTO_EXIT_MULTILEG_MARKER = "multileg"
AUTO_EXIT_MULTILEG_PAIR_BUFFER_PCT = 0.02
AUTO_EXIT_EXECUTABLE_MAX_SLIPPAGE_BPS = 8.0
AUTO_EXIT_SIGNATURE_QTY_TOLERANCE_PCT = 0.10
AUTO_EXIT_SIGNATURE_ENTRY_TOLERANCE_PCT = 0.005
AUTO_EXIT_POLICY_BY_TIER = {
    1: {
        "chunk_notional_cap_usd": 750.0,
        "market_cleanup_notional_cap_usd": 1500.0,
        "edge_buffer_bps": 2.0,
    },
    2: {
        "chunk_notional_cap_usd": 500.0,
        "market_cleanup_notional_cap_usd": 800.0,
        "edge_buffer_bps": 4.0,
    },
}
AUTO_EXIT_DEFAULT_POLICY = {
    "chunk_notional_cap_usd": 250.0,
    "market_cleanup_notional_cap_usd": 0.0,
    "edge_buffer_bps": 8.0,
}
AUTO_EXIT_V1_CONFIRM_CYCLES = 2
AUTO_EXIT_V1_TAKE_PROFIT_MIN_BPS = 40.0
AUTO_EXIT_V1_TAKE_PROFIT_FUNDING_MULT = 4.0
AUTO_EXIT_V1_SOFT_EXIT_THRESHOLD_BPS = -4.0
AUTO_EXIT_V1_HOLD_THRESHOLD_BPS = 2.0
AUTO_EXIT_V1_REVERSION_CREDIT_CAP_BPS = 12.0
AUTO_EXIT_POLICY_SETTINGS_DEFAULTS = {
    "tier1": dict(AUTO_EXIT_POLICY_BY_TIER[1]),
    "tier2": dict(AUTO_EXIT_POLICY_BY_TIER[2]),
    "lower_tier": dict(AUTO_EXIT_DEFAULT_POLICY),
}
MANUAL_EXEC_LOG_DIR = BASE_DIR / "logs" / "manual_exec"
COIN_ANALYSIS_CORE_EXCHANGES: tuple[str, ...] = ("binance", "kucoin")
COIN_ANALYSIS_CACHE_TTL_SEC = 90
FUNDING_HISTORY_DEFAULT_EXCHANGES: tuple[str, ...] = ("binance", "kucoin")
FUNDING_HISTORY_WINDOWS_HOURS: tuple[int, ...] = (4, 12, 24, 72)
FUNDING_HISTORY_MAX_POINTS = 200
COIN_ANALYSIS_SESSION_TTL_SEC = 30 * 60
COIN_FOCUS_POLL_SEC = 5.0
COIN_SHORTLIST_POLL_SEC = 180.0
COIN_OUTCOME_POLL_SEC = 60.0
COIN_OUTCOME_AUTO_HORIZONS: tuple[str, ...] = ("15m", "1h", "4h", "to_next_funding", "to_exit")
COIN_OUTCOME_MATURITY_GRACE_MS = 60 * 1000
COIN_OUTCOME_MAX_SYMBOLS_PER_CYCLE = 25
COIN_RETENTION_POLL_SEC = 6 * 3600.0
COIN_RETENTION_MAX_AGE_DAYS_DEFAULT = 45
COIN_RETENTION_CLOSED_PAPER_DAYS_DEFAULT = 120
COIN_POSITION_WATCHER_POLL_SEC = 45.0
COIN_POSITION_WATCHER_SYMBOL_COOLDOWN_SEC = 45.0
COIN_REVIEW_MISSED_ENTRY_SCORE_MIN = 60.0
COIN_REVIEW_MISSED_ENTRY_LOOKAHEAD_MS = 6 * 3600 * 1000
COIN_REVIEW_LATE_EXIT_NET_DELTA_MIN = -0.02
COIN_REVIEW_STALE_POSITION_AGE_MS = 24 * 3600 * 1000
COIN_REVIEW_BAD_ENTRY_NET_DELTA_MAX = -0.03
COIN_REVIEW_GOOD_NO_TRADE_NET_DELTA_MAX = -0.02
COIN_REVIEW_GOOD_EXIT_ALT_DELTA_MAX = -0.02
COIN_REVIEW_BAD_HOLD_NET_DELTA_MAX = -0.03
COIN_REVIEW_TOP_ITEMS_LIMIT = 5
OUTCOME_DEFAULT_TAKER_FEE_RATE = 0.0005
OUTCOME_ASSUMED_SLIPPAGE_BPS_PER_LEG = 4.0

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


def _auto_exit_select_pair_from_legs(
    legs: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    long_legs: list[dict[str, Any]] = []
    short_legs: list[dict[str, Any]] = []
    for leg in legs or []:
        side = str(leg.get("side") or "").lower()
        if side not in ("long", "short"):
            continue
        exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
        qty = abs(_safe_float(leg.get("quantity")) or 0.0)
        if not exchange or qty <= 0:
            continue
        item = {
            "side": side,
            "exchange": exchange,
            "qty": float(qty),
            "raw": dict(leg),
        }
        if side == "long":
            long_legs.append(item)
        else:
            short_legs.append(item)
    if not long_legs or not short_legs:
        return None

    mode = "single_pair"
    selected_min_side = None
    selected_min_exchange = None
    selected_min_qty = None

    if len(long_legs) == 1 and len(short_legs) == 1:
        selected_long = long_legs[0]
        selected_short = short_legs[0]
    else:
        mode = "multileg_min_leg"
        all_legs = long_legs + short_legs
        min_leg = min(
            all_legs,
            key=lambda item: (float(item.get("qty") or 0.0), str(item.get("exchange") or ""), str(item.get("side") or "")),
        )
        selected_min_side = str(min_leg.get("side") or "")
        selected_min_exchange = str(min_leg.get("exchange") or "")
        selected_min_qty = float(min_leg.get("qty") or 0.0)
        if selected_min_side == "long":
            selected_long = min_leg
            selected_short = max(short_legs, key=lambda item: (float(item.get("qty") or 0.0), str(item.get("exchange") or "")))
        else:
            selected_short = min_leg
            selected_long = max(long_legs, key=lambda item: (float(item.get("qty") or 0.0), str(item.get("exchange") or "")))

    qty = min(float(selected_long.get("qty") or 0.0), float(selected_short.get("qty") or 0.0))
    if qty <= 0:
        return None

    return {
        "mode": mode,
        "long_legs": len(long_legs),
        "short_legs": len(short_legs),
        "long_exchange": str(selected_long.get("exchange") or ""),
        "short_exchange": str(selected_short.get("exchange") or ""),
        "long_qty": float(selected_long.get("qty") or 0.0),
        "short_qty": float(selected_short.get("qty") or 0.0),
        "qty": float(qty),
        "long_leg": dict(selected_long.get("raw") or {}),
        "short_leg": dict(selected_short.get("raw") or {}),
        "selected_min_side": selected_min_side,
        "selected_min_exchange": selected_min_exchange,
        "selected_min_qty": selected_min_qty,
    }


def _is_auto_exit_multileg_rule(long_exchange: str | None, short_exchange: str | None) -> bool:
    return (
        normalize_exchange_name(str(long_exchange or "")) == AUTO_EXIT_MULTILEG_MARKER
        and normalize_exchange_name(str(short_exchange or "")) == AUTO_EXIT_MULTILEG_MARKER
    )


def _auto_exit_normalized_signature_leg(leg: Mapping[str, Any]) -> dict[str, Any] | None:
    exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
    side = str(leg.get("side") or "").lower()
    if side not in {"long", "short"} or not exchange:
        return None
    qty = abs(_safe_float(leg.get("quantity")) or _safe_float(leg.get("qty")) or 0.0)
    if qty <= 0:
        return None
    entry_price = _safe_float(leg.get("entry_price"))
    mark_price = _safe_float(leg.get("mark_price"))
    amount = abs(_safe_float(leg.get("amount")) or _safe_float(leg.get("notional")) or 0.0)
    return {
        "exchange": exchange,
        "side": side,
        "qty": float(qty),
        "entry_price": float(entry_price) if entry_price is not None else None,
        "mark_price": float(mark_price) if mark_price is not None else None,
        "amount": float(amount) if amount > 0 else None,
    }


def _auto_exit_position_signature(
    symbol: str,
    legs: Iterable[Mapping[str, Any]],
    *,
    rule_long_exchange: str,
    rule_short_exchange: str,
    selected_pair: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    symbol_key = normalize_symbol(symbol)
    long_exchange = normalize_exchange_name(rule_long_exchange)
    short_exchange = normalize_exchange_name(rule_short_exchange)
    is_multileg = _is_auto_exit_multileg_rule(long_exchange, short_exchange)
    source_legs: list[Mapping[str, Any]] = []
    if is_multileg:
        source_legs = list(legs or [])
    else:
        selected = selected_pair or _auto_exit_select_pair_from_legs(legs)
        if selected:
            long_leg = dict(selected.get("long_leg") or {})
            short_leg = dict(selected.get("short_leg") or {})
            if (
                normalize_exchange_name(str(long_leg.get("exchange") or "")) == long_exchange
                and str(long_leg.get("side") or "").lower() == "long"
            ):
                source_legs.append(long_leg)
            if (
                normalize_exchange_name(str(short_leg.get("exchange") or "")) == short_exchange
                and str(short_leg.get("side") or "").lower() == "short"
            ):
                source_legs.append(short_leg)
    normalized = []
    for leg in source_legs:
        item = _auto_exit_normalized_signature_leg(leg)
        if item:
            normalized.append(item)
    normalized.sort(key=lambda item: (str(item.get("exchange") or ""), str(item.get("side") or "")))
    if len(normalized) < 2:
        return None
    leg_keys = [
        f"{item['exchange']}:{item['side']}:{round(float(item.get('qty') or 0.0), 8)}:"
        f"{round(float(item.get('entry_price') or 0.0), 8)}"
        for item in normalized
    ]
    return {
        "version": 1,
        "symbol": symbol_key,
        "mode": "multileg" if is_multileg else "pair",
        "rule_long_exchange": long_exchange,
        "rule_short_exchange": short_exchange,
        "legs": normalized,
        "fingerprint": "|".join(leg_keys),
    }


def _auto_exit_signature_match(
    expected: Mapping[str, Any] | None,
    current: Mapping[str, Any] | None,
) -> tuple[bool, str]:
    if not isinstance(expected, Mapping) or not expected.get("legs"):
        return False, "unbound_position_signature"
    if not isinstance(current, Mapping) or not current.get("legs"):
        return False, "position_signature_unavailable"
    if normalize_symbol(str(expected.get("symbol") or "")) != normalize_symbol(str(current.get("symbol") or "")):
        return False, "position_signature_symbol_changed"
    expected_legs = {
        (normalize_exchange_name(str(item.get("exchange") or "")), str(item.get("side") or "").lower()): item
        for item in list(expected.get("legs") or [])
        if isinstance(item, Mapping)
    }
    current_legs = {
        (normalize_exchange_name(str(item.get("exchange") or "")), str(item.get("side") or "").lower()): item
        for item in list(current.get("legs") or [])
        if isinstance(item, Mapping)
    }
    if set(expected_legs.keys()) != set(current_legs.keys()):
        return False, "position_signature_legs_changed"
    for key, expected_leg in expected_legs.items():
        current_leg = current_legs.get(key) or {}
        expected_qty = _safe_float(expected_leg.get("qty")) or 0.0
        current_qty = _safe_float(current_leg.get("qty")) or 0.0
        if expected_qty <= 0 or current_qty <= 0:
            return False, "position_signature_qty_unavailable"
        qty_delta = abs(current_qty - expected_qty) / max(abs(expected_qty), 1e-12)
        if qty_delta > AUTO_EXIT_SIGNATURE_QTY_TOLERANCE_PCT:
            return False, "position_signature_qty_changed"
        expected_entry = _safe_float(expected_leg.get("entry_price"))
        current_entry = _safe_float(current_leg.get("entry_price"))
        if expected_entry is not None and current_entry is not None and expected_entry > 0 and current_entry > 0:
            entry_delta = abs(current_entry - expected_entry) / max(abs(expected_entry), 1e-12)
            if entry_delta > AUTO_EXIT_SIGNATURE_ENTRY_TOLERANCE_PCT:
                return False, "position_signature_entry_changed"
    return True, "position_signature_match"


def _auto_exit_overall_spread_from_legs(
    legs: Iterable[Mapping[str, Any]],
    live_mid_by_exchange: Mapping[str, float] | None = None,
) -> float | None:
    live_mid_by_exchange = live_mid_by_exchange or {}
    long_qty = 0.0
    long_notional = 0.0
    short_qty = 0.0
    short_notional = 0.0
    for leg in legs or []:
        side = str(leg.get("side") or "").lower()
        if side not in ("long", "short"):
            continue
        exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
        qty = abs(_safe_float(leg.get("quantity")) or 0.0)
        if qty <= 0:
            continue
        live_mid = _safe_float(live_mid_by_exchange.get(exchange))
        mark = _safe_float(leg.get("mark_price"))
        entry = _safe_float(leg.get("entry_price"))
        price = live_mid if live_mid and live_mid > 0 else mark if mark and mark > 0 else entry
        if not price or price <= 0:
            continue
        if side == "long":
            long_qty += qty
            long_notional += qty * price
        else:
            short_qty += qty
            short_notional += qty * price
    if long_qty <= 0 or short_qty <= 0:
        return None
    long_avg = long_notional / long_qty if long_qty > 0 else None
    short_avg = short_notional / short_qty if short_qty > 0 else None
    if not long_avg or long_avg == 0 or short_avg is None:
        return None
    return (long_avg - short_avg) / long_avg * 100.0


def _auto_exit_spread_trigger_status(
    *,
    is_multileg: bool,
    target_pct: float,
    overall_spread_pct: float | None,
    pair_spread_pct: float | None,
    pair_net_spread_pct: float | None,
    edge_buffer_pct: float,
) -> dict[str, Any]:
    if is_multileg:
        trigger_spread = overall_spread_pct
        required_spread = float(target_pct)
        live_ready = (
            trigger_spread is not None
            and pair_spread_pct is not None
            and pair_net_spread_pct is not None
        )
    else:
        trigger_spread = pair_spread_pct
        required_spread = float(target_pct) + float(edge_buffer_pct)
        live_ready = trigger_spread is not None and pair_net_spread_pct is not None
    return {
        "trigger_spread_pct": trigger_spread,
        "required_spread_pct": required_spread,
        "live_ready": bool(live_ready),
        "target_reached": bool(
            live_ready
            and (
                float(trigger_spread) >= float(required_spread)
                if is_multileg
                else float(pair_net_spread_pct) >= float(required_spread)
            )
        ),
        "scope": "overall_basket" if is_multileg else "pair_executable",
    }


def _auto_exit_pair_fee_bps(long_exchange: str, short_exchange: str) -> float:
    long_fee = float(EXCHANGE_COMMISSIONS.get(normalize_exchange_name(long_exchange), {}).get("taker", 0.0))
    short_fee = float(EXCHANGE_COMMISSIONS.get(normalize_exchange_name(short_exchange), {}).get("taker", 0.0))
    return (long_fee + short_fee) * 10_000.0


def _auto_exit_policy_settings(manual_settings: Mapping[str, Any] | None = None) -> dict[str, dict[str, float]]:
    merged: dict[str, dict[str, float]] = {
        key: dict(value) for key, value in AUTO_EXIT_POLICY_SETTINGS_DEFAULTS.items()
    }
    incoming = (manual_settings or {}).get("auto_exit_policy")
    if not isinstance(incoming, Mapping):
        return merged
    for tier_key, defaults in merged.items():
        section = incoming.get(tier_key)
        if not isinstance(section, Mapping):
            continue
        normalized_section = dict(defaults)
        for field_name in (
            "chunk_notional_cap_usd",
            "market_cleanup_notional_cap_usd",
            "edge_buffer_bps",
        ):
            value = _safe_float(section.get(field_name))
            if value is not None and value >= 0:
                normalized_section[field_name] = float(value)
        merged[tier_key] = normalized_section
    return merged


def _auto_exit_policy_for_pair(
    long_exchange: str,
    short_exchange: str,
    manual_settings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    long_tier = venue_liquidity_tier(long_exchange)
    short_tier = venue_liquidity_tier(short_exchange)
    worst_tier = max(long_tier, short_tier)
    settings_policy = _auto_exit_policy_settings(manual_settings)
    policy_key = "tier1" if worst_tier <= 1 else "tier2" if worst_tier == 2 else "lower_tier"
    base = dict(settings_policy.get("lower_tier", AUTO_EXIT_DEFAULT_POLICY))
    base.update(settings_policy.get(policy_key, {}))
    base["long_tier"] = long_tier
    base["short_tier"] = short_tier
    base["worst_tier"] = worst_tier
    base["policy_key"] = policy_key
    return base


def _auto_exit_executable_metrics_from_books(
    *,
    long_exchange: str,
    short_exchange: str,
    long_book: Mapping[str, Any] | None,
    short_book: Mapping[str, Any] | None,
    qty: float,
    max_slippage_bps: float = AUTO_EXIT_EXECUTABLE_MAX_SLIPPAGE_BPS,
    fee_bps: float = 0.0,
    edge_buffer_bps: float = 0.0,
    chunk_notional_cap_usd: float | None = None,
) -> dict[str, Any] | None:
    qty_val = _safe_float(qty) or 0.0
    if qty_val <= 0:
        return None
    bids_long = list((long_book or {}).get("bids") or [])
    asks_short = list((short_book or {}).get("asks") or [])
    if not bids_long or not asks_short:
        return None
    max_qty_long = max_qty_for_slippage(bids_long, side="sell", max_bps=max_slippage_bps)
    max_qty_short = max_qty_for_slippage(asks_short, side="buy", max_bps=max_slippage_bps)
    max_candidates = [val for val in (max_qty_long, max_qty_short) if val is not None and val > 0]
    if not max_candidates:
        return None
    liquidity_cap_qty = min(float(val) for val in max_candidates)
    if liquidity_cap_qty <= 0:
        return None
    chunk_qty = min(qty_val, liquidity_cap_qty)
    best_bid_long = _safe_float(bids_long[0][0]) if bids_long else None
    best_ask_short = _safe_float(asks_short[0][0]) if asks_short else None
    reference_price = max(
        [price for price in (best_bid_long, best_ask_short) if price and price > 0],
        default=None,
    )
    if chunk_notional_cap_usd and chunk_notional_cap_usd > 0 and reference_price and reference_price > 0:
        chunk_qty = min(chunk_qty, float(chunk_notional_cap_usd) / float(reference_price))
    if chunk_qty <= 0:
        return None
    long_fill = estimate_fill(bids_long, chunk_qty)
    short_fill = estimate_fill(asks_short, chunk_qty)
    filled_long = _safe_float(long_fill.get("filled_qty")) or 0.0
    filled_short = _safe_float(short_fill.get("filled_qty")) or 0.0
    executable_qty = min(filled_long, filled_short, chunk_qty)
    if executable_qty <= 0:
        return None
    avg_sell_long = _safe_float(long_fill.get("avg_price"))
    avg_buy_short = _safe_float(short_fill.get("avg_price"))
    if not avg_sell_long or not avg_buy_short or avg_sell_long <= 0:
        return None
    spread_pct = (avg_sell_long - avg_buy_short) / avg_sell_long * 100.0
    chunk_notional = executable_qty * max(avg_sell_long, avg_buy_short)
    fee_pct = float(fee_bps) / 100.0
    edge_buffer_pct = float(edge_buffer_bps) / 100.0
    net_spread_pct = float(spread_pct) - fee_pct
    return {
        "spread_pct": float(spread_pct),
        "net_spread_pct": float(net_spread_pct),
        "chunk_qty": float(executable_qty),
        "chunk_notional_usd": float(chunk_notional),
        "requested_qty": float(qty_val),
        "liquidity_cap_qty": float(liquidity_cap_qty),
        "safety_factor": None,
        "fee_bps": float(fee_bps),
        "fee_pct": float(fee_pct),
        "edge_buffer_bps": float(edge_buffer_bps),
        "edge_buffer_pct": float(edge_buffer_pct),
        "avg_sell_long": float(avg_sell_long),
        "avg_buy_short": float(avg_buy_short),
        "long_remaining_qty": float(_safe_float(long_fill.get("remaining_qty")) or 0.0),
        "short_remaining_qty": float(_safe_float(short_fill.get("remaining_qty")) or 0.0),
        "max_qty_long": float(max_qty_long) if max_qty_long is not None else None,
        "max_qty_short": float(max_qty_short) if max_qty_short is not None else None,
        "chunk_notional_cap_usd": float(chunk_notional_cap_usd) if chunk_notional_cap_usd is not None else None,
        "long_exchange": normalize_exchange_name(long_exchange),
        "short_exchange": normalize_exchange_name(short_exchange),
    }


def _auto_exit_top_n_liquidity_usd(
    levels: Iterable[Iterable[float]] | None,
    *,
    top_n: int = 3,
) -> float:
    total = 0.0
    count = 0
    for level in levels or []:
        if count >= top_n or len(level) < 2:
            break
        price = _safe_float(level[0]) or 0.0
        size = _safe_float(level[1]) or 0.0
        if price <= 0 or size <= 0:
            continue
        total += price * size
        count += 1
    return float(total)


def _auto_exit_execution_order(
    *,
    long_exchange: str,
    short_exchange: str,
    long_book: Mapping[str, Any] | None,
    short_book: Mapping[str, Any] | None,
) -> dict[str, Any]:
    suggestion = suggest_expensive_leg(
        normalize_exchange_name(long_exchange),
        normalize_exchange_name(short_exchange),
        fee_table=EXCHANGE_COMMISSIONS,
        liquidity={
            normalize_exchange_name(long_exchange): _auto_exit_top_n_liquidity_usd((long_book or {}).get("bids") or []),
            normalize_exchange_name(short_exchange): _auto_exit_top_n_liquidity_usd((short_book or {}).get("asks") or []),
        },
    )
    suggested_leg = str(suggestion.get("suggested_leg") or "")
    primary_label = suggested_leg if suggested_leg in ("long", "short") else "long"
    hedge_label = "short" if primary_label == "long" else "long"
    primary_exchange = normalize_exchange_name(long_exchange if primary_label == "long" else short_exchange)
    hedge_exchange = normalize_exchange_name(short_exchange if primary_label == "long" else long_exchange)
    return {
        "primary_label": primary_label,
        "primary_exchange": primary_exchange,
        "hedge_label": hedge_label,
        "hedge_exchange": hedge_exchange,
        "reason": suggestion.get("reason"),
        "suggestion": suggestion,
    }


def _auto_exit_market_cleanup_status(
    *,
    long_exchange: str | None,
    short_exchange: str | None,
    cleanup_cap_usd: float | None,
    estimated_notional_usd: float | None,
    tier_limit: int = AUTO_EXIT_MARKET_FALLBACK_MAX_TIER,
) -> dict[str, Any]:
    def evaluate(exchange_name: str | None) -> dict[str, Any]:
        exchange = normalize_exchange_name(str(exchange_name or ""))
        if not exchange or exchange == AUTO_EXIT_MULTILEG_MARKER:
            return {"exchange": exchange or None, "allowed": None, "reason": "unavailable"}
        if venue_liquidity_tier(exchange) > max(1, int(tier_limit or 1)):
            return {"exchange": exchange, "allowed": False, "reason": "tier_blocked"}
        if cleanup_cap_usd is not None:
            if cleanup_cap_usd <= 0:
                return {"exchange": exchange, "allowed": False, "reason": "cap_zero"}
            if estimated_notional_usd is not None and estimated_notional_usd > cleanup_cap_usd:
                return {"exchange": exchange, "allowed": False, "reason": "notional_cap"}
        return {"exchange": exchange, "allowed": True, "reason": "allowed"}

    legs = [evaluate(long_exchange), evaluate(short_exchange)]
    known = [row for row in legs if row.get("allowed") is not None]
    overall_allowed = bool(known) and all(bool(row.get("allowed")) for row in known)
    parts: list[str] = []
    for row in known:
        exchange = str(row.get("exchange") or "").upper()
        reason = str(row.get("reason") or "")
        status = "allow" if row.get("allowed") else f"block:{reason}"
        parts.append(f"{exchange}:{status}")
    summary = ", ".join(parts) if parts else "unavailable"
    return {
        "allowed": overall_allowed if known else None,
        "summary": summary,
        "legs": legs,
    }


def _auto_exit_edge_delta_bps(
    net_spread_pct: float | None,
    required_net_spread_pct: float | None,
) -> float | None:
    if net_spread_pct is None or required_net_spread_pct is None:
        return None
    return (float(net_spread_pct) - float(required_net_spread_pct)) * 100.0


def _auto_exit_v1_clamp(value: float, lower: float, upper: float) -> float:
    return max(float(lower), min(float(upper), float(value)))


def _auto_exit_v1_interval_bucket(interval_minutes: float | None) -> str:
    minutes = _safe_float(interval_minutes)
    if minutes is None or minutes <= 0:
        return "unknown"
    if minutes <= 90.0:
        return "1h"
    if minutes <= 300.0:
        return "4h"
    return "8h"


def _auto_exit_v1_window(
    interval_minutes: float | None,
    minutes_to_event: float | None,
) -> dict[str, Any]:
    interval_val = _safe_float(interval_minutes)
    minutes_val = _safe_float(minutes_to_event)
    if interval_val is None or interval_val <= 0 or minutes_val is None or minutes_val < 0:
        return {
            "bucket": _auto_exit_v1_interval_bucket(interval_val),
            "stage": "unknown",
            "pre_watch_window_min": None,
            "decision_window_min": None,
            "critical_window_min": None,
            "funding_pressure_mult": None,
            "reversion_credit_mult": None,
            "take_profit_k": None,
            "hard_exit_negative_funding_bps": None,
        }
    pre_watch_window = _auto_exit_v1_clamp(interval_val * 0.25, 15.0, 45.0)
    decision_window = _auto_exit_v1_clamp(interval_val * 0.08, 10.0, 20.0)
    critical_window = _auto_exit_v1_clamp(interval_val * 0.04, 5.0, 10.0)
    bucket = _auto_exit_v1_interval_bucket(interval_val)
    stage = "open"
    if minutes_val <= critical_window:
        stage = "critical"
    elif minutes_val <= decision_window:
        stage = "decision"
    elif minutes_val <= pre_watch_window:
        stage = "watch"

    presets: dict[str, dict[str, tuple[float, float, float, float | None]]] = {
        "1h": {
            "open": (1.0, 0.30, 4.0, None),
            "watch": (1.4, 0.12, 4.0, None),
            "decision": (1.8, 0.05, 4.0, -2.0),
            "critical": (2.2, 0.0, 4.0, 0.0),
        },
        "4h": {
            "open": (0.9, 0.40, 4.0, None),
            "watch": (1.2, 0.18, 4.0, None),
            "decision": (1.6, 0.08, 4.0, -3.0),
            "critical": (1.9, 0.02, 4.0, -2.0),
        },
        "8h": {
            "open": (0.8, 0.45, 4.0, None),
            "watch": (1.1, 0.22, 4.0, None),
            "decision": (1.5, 0.10, 4.0, -4.0),
            "critical": (1.8, 0.03, 4.0, -3.0),
        },
    }
    bucket_key = bucket if bucket in presets else "8h"
    funding_mult, reversion_mult, take_profit_k, hard_exit_negative = presets[bucket_key][stage]
    return {
        "bucket": bucket,
        "stage": stage,
        "pre_watch_window_min": float(pre_watch_window),
        "decision_window_min": float(decision_window),
        "critical_window_min": float(critical_window),
        "funding_pressure_mult": float(funding_mult),
        "reversion_credit_mult": float(reversion_mult),
        "take_profit_k": float(take_profit_k),
        "hard_exit_negative_funding_bps": (
            float(hard_exit_negative) if hard_exit_negative is not None else None
        ),
    }


def _auto_exit_v1_weighted_value(
    legs: list[Mapping[str, Any]],
    side: str,
    field: str,
) -> float | None:
    total_weight = 0.0
    total_value = 0.0
    for leg in legs:
        if str(leg.get("side") or "").lower() != side:
            continue
        value = _safe_float(leg.get(field))
        weight = abs(_safe_float(leg.get("amount")) or _safe_float(leg.get("quantity")) or 0.0)
        if value is None or weight <= 0:
            continue
        total_weight += weight
        total_value += float(value) * float(weight)
    if total_weight <= 0:
        return None
    return total_value / total_weight


def _auto_exit_v1_position_context(legs: list[Mapping[str, Any]]) -> dict[str, Any]:
    long_notional = sum(
        abs(_safe_float(leg.get("amount")) or 0.0)
        for leg in legs
        if str(leg.get("side") or "").lower() == "long"
    )
    short_notional = sum(
        abs(_safe_float(leg.get("amount")) or 0.0)
        for leg in legs
        if str(leg.get("side") or "").lower() == "short"
    )
    notional_candidates = [value for value in (long_notional, short_notional) if value > 0]
    position_notional_usd = None
    if notional_candidates:
        position_notional_usd = sum(notional_candidates) / len(notional_candidates)

    entry_long = _auto_exit_v1_weighted_value(legs, "long", "entry_price")
    entry_short = _auto_exit_v1_weighted_value(legs, "short", "entry_price")
    mark_long = _auto_exit_v1_weighted_value(legs, "long", "mark_price")
    mark_short = _auto_exit_v1_weighted_value(legs, "short", "mark_price")

    def spread_pct(long_price: float | None, short_price: float | None) -> float | None:
        if long_price is None or short_price is None or long_price == 0:
            return None
        return (float(long_price) - float(short_price)) / float(long_price) * 100.0

    funding_usd = 0.0
    funding_known = False
    interval_candidates: list[float] = []
    next_candidates: list[float] = []
    now_ts = datetime.now(timezone.utc).timestamp()
    for leg in legs:
        expected = _safe_float(leg.get("expected_funding"))
        if expected is not None:
            funding_usd += float(expected)
            funding_known = True
        interval_hours = _safe_float(leg.get("funding_interval_hours"))
        if interval_hours is not None and interval_hours > 0:
            interval_candidates.append(float(interval_hours) * 60.0)
        next_iso = leg.get("next_funding")
        if next_iso:
            try:
                next_dt = datetime.fromisoformat(str(next_iso))
                minutes_to_next = max(0.0, (next_dt.timestamp() - now_ts) / 60.0)
                next_candidates.append(minutes_to_next)
            except Exception:
                pass
    return {
        "position_notional_usd": float(position_notional_usd) if position_notional_usd is not None else None,
        "entry_spread_pct": spread_pct(entry_long, entry_short),
        "mark_spread_pct": spread_pct(mark_long, mark_short),
        "funding_to_next_usd": float(funding_usd) if funding_known else None,
        "effective_interval_minutes": min(interval_candidates) if interval_candidates else None,
        "minutes_to_event": min(next_candidates) if next_candidates else None,
    }


def _auto_exit_v1_decision(
    *,
    close_now_bps: float | None,
    funding_to_next_bps: float | None,
    reversion_credit_bps: float | None,
    window: Mapping[str, Any] | None,
) -> dict[str, Any]:
    close_bps = _safe_float(close_now_bps)
    funding_bps = _safe_float(funding_to_next_bps)
    reversion_bps = _safe_float(reversion_credit_bps) or 0.0
    window = dict(window or {})
    funding_mult = _safe_float(window.get("funding_pressure_mult"))
    reversion_mult = _safe_float(window.get("reversion_credit_mult"))
    take_profit_k = _safe_float(window.get("take_profit_k"))
    hard_exit_negative = _safe_float(window.get("hard_exit_negative_funding_bps"))
    stage = str(window.get("stage") or "unknown")

    if close_bps is None:
        return {
            "decision": "skip",
            "reason": "close_now_unavailable",
            "wait_score_bps": None,
            "take_profit_threshold_bps": None,
            "risk_penalty_bps": None,
        }
    if funding_bps is None or funding_mult is None or reversion_mult is None or take_profit_k is None:
        return {
            "decision": "skip",
            "reason": "funding_context_unavailable",
            "wait_score_bps": None,
            "take_profit_threshold_bps": None,
            "risk_penalty_bps": None,
        }

    take_profit_threshold_bps = max(
        float(AUTO_EXIT_V1_TAKE_PROFIT_MIN_BPS),
        float(AUTO_EXIT_V1_TAKE_PROFIT_FUNDING_MULT) * max(float(funding_bps), 0.0),
    )
    if close_bps >= take_profit_threshold_bps:
        return {
            "decision": "exit",
            "reason": "take_profit_multiple",
            "wait_score_bps": None,
            "take_profit_threshold_bps": float(take_profit_threshold_bps),
            "risk_penalty_bps": 0.0,
        }
    if hard_exit_negative is not None:
        if stage == "critical" and funding_bps < float(hard_exit_negative):
            return {
                "decision": "exit",
                "reason": "negative_funding_critical",
                "wait_score_bps": None,
                "take_profit_threshold_bps": float(take_profit_threshold_bps),
                "risk_penalty_bps": 0.0,
            }
        if stage == "decision" and funding_bps <= float(hard_exit_negative):
            return {
                "decision": "exit",
                "reason": "negative_funding_decision_window",
                "wait_score_bps": None,
                "take_profit_threshold_bps": float(take_profit_threshold_bps),
                "risk_penalty_bps": 0.0,
            }

    risk_penalty = 0.0
    if close_bps < 0:
        risk_penalty += min(6.0, abs(float(close_bps)) * 0.5)
    if stage == "decision" and funding_bps < 0:
        risk_penalty += 1.5
    elif stage == "critical" and funding_bps < 0:
        risk_penalty += 3.0

    wait_score = float(funding_bps) * float(funding_mult)
    wait_score += max(0.0, float(reversion_bps)) * float(reversion_mult)
    wait_score -= risk_penalty
    if wait_score <= float(AUTO_EXIT_V1_SOFT_EXIT_THRESHOLD_BPS):
        return {
            "decision": "confirm_exit",
            "reason": "wait_score_negative",
            "wait_score_bps": float(wait_score),
            "take_profit_threshold_bps": float(take_profit_threshold_bps),
            "risk_penalty_bps": float(risk_penalty),
        }
    if wait_score >= float(AUTO_EXIT_V1_HOLD_THRESHOLD_BPS):
        return {
            "decision": "hold",
            "reason": "wait_score_positive",
            "wait_score_bps": float(wait_score),
            "take_profit_threshold_bps": float(take_profit_threshold_bps),
            "risk_penalty_bps": float(risk_penalty),
        }
    return {
        "decision": "hold",
        "reason": "neutral_window",
        "wait_score_bps": float(wait_score),
        "take_profit_threshold_bps": float(take_profit_threshold_bps),
        "risk_penalty_bps": float(risk_penalty),
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
    timestamp_interval = _infer_history_timestamp_interval_hours(history)
    inferred = infer_funding_interval_hours(history, snapshot_interval=snapshot_interval)
    return _resolve_row_interval_hours(inferred, timestamp_interval, snapshot_interval)


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


def _funding_position_multiplier(
    direction: str,
    *,
    leg: Literal["left", "right"],
) -> float:
    direction_text = str(direction or "").lower()
    if direction_text == "long_b_short_a":
        return 1.0 if leg == "left" else -1.0
    return -1.0 if leg == "left" else 1.0


def _funding_event_segments(
    history: list[dict[str, Any]],
    snapshot_interval: float | None,
) -> list[dict[str, float]]:
    rows = enrich_history_intervals(history or [], snapshot_interval=snapshot_interval)
    segments: list[dict[str, float]] = []
    for row in rows:
        interval_hours = _safe_float(row.get("interval_hours"))
        raw_end_ts_ms = _funding_history_ts_ms(row.get("ts_ms") or row.get("timestamp"))
        end_ts_ms = (
            _funding_history_ts_ms(row.get("slot_ts_ms"))
            or _funding_slot_ts_ms(raw_end_ts_ms, interval_hours or snapshot_interval)
            or raw_end_ts_ms
        )
        rate = _funding_rate_from_row(row)
        if not end_ts_ms or interval_hours is None or interval_hours <= 0 or rate is None:
            continue
        duration_ms = int(interval_hours * 3600.0 * 1000.0)
        if duration_ms <= 0:
            continue
        segments.append(
            {
                "start_ts_ms": float(end_ts_ms - duration_ms),
                "end_ts_ms": float(end_ts_ms),
                "interval_hours": float(interval_hours),
                "rate": float(rate),
            }
        )
    segments.sort(key=lambda item: item.get("end_ts_ms") or 0.0)
    return segments


def _funding_rate_from_row(row: Mapping[str, Any]) -> float | None:
    for key in ("rate", "fundingRate", "funding_rate"):
        if key in row:
            value = _safe_float(row.get(key))
            if value is not None:
                return value
    return None


def _funding_slot_ts_ms(ts_ms: int | float | None, interval_hours: float | None = None) -> int | None:
    if ts_ms is None:
        return None
    ts_val = int(ts_ms)
    interval = normalize_interval_hours(interval_hours)
    bucket_ms = int((interval or 1.0) * 3600.0 * 1000.0)
    if bucket_ms <= 0:
        bucket_ms = 3600 * 1000
    return int(round(ts_val / float(bucket_ms)) * bucket_ms)


def _funding_slot_iso(ts_ms: int | float | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).isoformat()


def _infer_history_timestamp_interval_hours(history: Sequence[Mapping[str, Any]]) -> float | None:
    points: list[int] = []
    for row in history or []:
        if not isinstance(row, Mapping):
            continue
        ts_ms = _funding_history_ts_ms(
            row.get("ts_ms")
            or row.get("timestamp")
            or row.get("timepoint")
            or row.get("timePoint")
            or row.get("fundingTime")
        )
        if ts_ms is not None:
            points.append(int(ts_ms))
    unique = sorted(set(points), reverse=True)
    if len(unique) < 2:
        return None
    buckets: dict[float, int] = {}
    for idx in range(len(unique) - 1):
        diff_ms = abs(unique[idx] - unique[idx + 1])
        interval = normalize_interval_hours(diff_ms / 1000.0 / 3600.0)
        if interval is None:
            continue
        bucket = round(interval * 4.0) / 4.0
        buckets[bucket] = buckets.get(bucket, 0) + 1
    if not buckets:
        return None
    return max(buckets.items(), key=lambda item: (item[1], -item[0]))[0]


def _resolve_row_interval_hours(
    declared_interval: float | None,
    timestamp_interval: float | None,
    snapshot_interval: float | None,
) -> float | None:
    declared = normalize_interval_hours(declared_interval)
    ts_interval = normalize_interval_hours(timestamp_interval)
    snapshot = normalize_interval_hours(snapshot_interval)
    if ts_interval is not None:
        if declared is None:
            return ts_interval
        tolerance = max(0.1, min(declared, ts_interval) * 0.2)
        if abs(declared - ts_interval) > tolerance:
            return ts_interval
        return declared
    return declared if declared is not None else snapshot


def _compact_funding_history_rows(
    history: list[dict[str, Any]],
    *,
    snapshot_interval: float | None,
    limit: int,
) -> list[dict[str, Any]]:
    enriched = enrich_history_intervals(history or [], snapshot_interval=snapshot_interval)
    timestamp_interval = _infer_history_timestamp_interval_hours(enriched)
    rows: list[dict[str, Any]] = []
    for item in enriched:
        ts_ms = _funding_history_ts_ms(
            item.get("ts_ms")
            or item.get("timestamp")
            or item.get("timepoint")
            or item.get("timePoint")
            or item.get("fundingTime")
        )
        rate = _funding_rate_from_row(item)
        if ts_ms is None or rate is None:
            continue
        interval_hours = _safe_float(
            item.get("interval_hours")
            or item.get("intervalHours")
            or item.get("funding_interval_hours")
        )
        interval_hours = _resolve_row_interval_hours(
            interval_hours,
            timestamp_interval,
            snapshot_interval,
        )
        slot_ts_ms = _funding_slot_ts_ms(ts_ms, interval_hours or snapshot_interval)
        predicted = None
        for predicted_key in ("predicted_rate", "predictedFundingRate", "predicted_funding_rate"):
            if predicted_key in item:
                predicted = _safe_float(item.get(predicted_key))
                if predicted is not None:
                    break
        rows.append(
            {
                "ts_ms": int(ts_ms),
                "time_utc": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                "slot_ts_ms": slot_ts_ms,
                "slot_time_utc": _funding_slot_iso(slot_ts_ms),
                "rate": float(rate),
                "rate_bps": float(rate) * 10000.0,
                "predicted_rate": predicted,
                "predicted_bps": predicted * 10000.0 if predicted is not None else None,
                "interval_hours": interval_hours,
            }
        )
    rows.sort(key=lambda row: int(row.get("ts_ms") or 0), reverse=True)
    return rows[: max(1, int(limit))]


def _funding_carry_over_window_pct(
    segments: list[dict[str, float]],
    window_start_ms: int,
    window_end_ms: int,
    *,
    multiplier: float,
) -> tuple[float | None, float]:
    if window_end_ms <= window_start_ms:
        return None, 0.0
    total_pct = 0.0
    covered_ms = 0.0
    for item in segments:
        start_ts = int(_safe_float(item.get("start_ts_ms")) or 0)
        end_ts = int(_safe_float(item.get("end_ts_ms")) or 0)
        rate = _safe_float(item.get("rate"))
        interval_hours = _safe_float(item.get("interval_hours"))
        if start_ts <= 0 or end_ts <= start_ts or rate is None or interval_hours is None or interval_hours <= 0:
            continue
        overlap_ms = min(end_ts, window_end_ms) - max(start_ts, window_start_ms)
        if overlap_ms <= 0:
            continue
        duration_ms = interval_hours * 3600.0 * 1000.0
        if duration_ms <= 0:
            continue
        covered_ms += float(overlap_ms)
        total_pct += float(multiplier) * float(rate) * (float(overlap_ms) / float(duration_ms))
    if covered_ms <= 0:
        return None, 0.0
    return total_pct, min(100.0, covered_ms / max(1.0, float(window_end_ms - window_start_ms)) * 100.0)


def _funding_history_window_label(hours: int | float) -> str:
    hours_val = int(hours)
    if hours_val == 24:
        return "1d"
    if hours_val == 72:
        return "3d"
    return f"{hours_val}h"


def _funding_history_windows(windows_hours: Iterable[int | float] | None = None) -> list[dict[str, Any]]:
    raw_values = list(windows_hours or FUNDING_HISTORY_WINDOWS_HOURS)
    out: list[dict[str, Any]] = []
    seen: set[int] = set()
    for raw in raw_values:
        hours = int(_safe_float(raw) or 0)
        if hours <= 0 or hours > 24 * 14 or hours in seen:
            continue
        seen.add(hours)
        out.append({"hours": hours, "label": _funding_history_window_label(hours)})
    if not out:
        for hours in FUNDING_HISTORY_WINDOWS_HOURS:
            out.append({"hours": hours, "label": _funding_history_window_label(hours)})
    return out


def _funding_history_exchange_windows(
    history: list[dict[str, Any]],
    interval_hours: float | None,
    windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    segments = _funding_event_segments(history, interval_hours)
    if not segments:
        return []
    latest_end_ms = int(_safe_float(segments[-1].get("end_ts_ms")) or 0)
    out: list[dict[str, Any]] = []
    for window in windows:
        hours = int(window.get("hours") or 0)
        if hours <= 0:
            continue
        start_ms = latest_end_ms - hours * 3600 * 1000
        short_pct, short_cov = _funding_carry_over_window_pct(
            segments,
            start_ms,
            latest_end_ms,
            multiplier=1.0,
        )
        long_pct, long_cov = _funding_carry_over_window_pct(
            segments,
            start_ms,
            latest_end_ms,
            multiplier=-1.0,
        )
        out.append(
            {
                "label": window.get("label"),
                "hours": hours,
                "window_start_ms": start_ms,
                "window_end_ms": latest_end_ms,
                "short_carry_bps": short_pct * 10000.0 if short_pct is not None else None,
                "long_carry_bps": long_pct * 10000.0 if long_pct is not None else None,
                "coverage_pct": min(short_cov, long_cov),
            }
        )
    return out


def _build_funding_history_pair_analysis(
    exchange_rows: list[Mapping[str, Any]],
    windows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    usable = [
        row
        for row in exchange_rows
        if row.get("status") in {"ok", "partial"} and row.get("funding_history")
    ]
    pair_rows: list[dict[str, Any]] = []
    series_source: dict[str, Any] = {}
    for i in range(len(usable)):
        for j in range(i + 1, len(usable)):
            left = usable[i]
            right = usable[j]
            left_exchange = str(left.get("exchange") or "")
            right_exchange = str(right.get("exchange") or "")
            left_history = list(left.get("funding_history") or [])
            right_history = list(right.get("funding_history") or [])
            left_interval = _safe_float(left.get("funding_interval_hours_resolved"))
            right_interval = _safe_float(right.get("funding_interval_hours_resolved"))
            left_segments = _funding_event_segments(left_history, left_interval)
            right_segments = _funding_event_segments(right_history, right_interval)
            if not left_segments or not right_segments:
                continue
            pair_end_ms = min(
                int(_safe_float(left_segments[-1].get("end_ts_ms")) or 0),
                int(_safe_float(right_segments[-1].get("end_ts_ms")) or 0),
            )
            if pair_end_ms <= 0:
                continue
            for direction in ("long_a_short_b", "long_b_short_a"):
                left_mult = _funding_position_multiplier(direction, leg="left")
                right_mult = _funding_position_multiplier(direction, leg="right")
                if direction == "long_b_short_a":
                    long_exchange = right_exchange
                    short_exchange = left_exchange
                else:
                    long_exchange = left_exchange
                    short_exchange = right_exchange
                for window in windows:
                    hours = int(window.get("hours") or 0)
                    if hours <= 0:
                        continue
                    start_ms = pair_end_ms - hours * 3600 * 1000
                    left_pct, left_cov = _funding_carry_over_window_pct(
                        left_segments,
                        start_ms,
                        pair_end_ms,
                        multiplier=left_mult,
                    )
                    right_pct, right_cov = _funding_carry_over_window_pct(
                        right_segments,
                        start_ms,
                        pair_end_ms,
                        multiplier=right_mult,
                    )
                    net_pct = None
                    if left_pct is not None and right_pct is not None:
                        net_pct = float(left_pct) + float(right_pct)
                    coverage_pct = min(left_cov, right_cov)
                    status = "ok"
                    if net_pct is None:
                        status = "insufficient_data"
                    elif coverage_pct < 95.0:
                        status = "partial"
                    pair_rows.append(
                        {
                            "pair_key": f"{left_exchange}|{right_exchange}",
                            "pair_label": f"{left_exchange} vs {right_exchange}",
                            "left_exchange": left_exchange,
                            "right_exchange": right_exchange,
                            "direction": direction,
                            "direction_label": _direction_label(direction, left_exchange, right_exchange),
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "window_label": window.get("label"),
                            "window_hours": hours,
                            "window_start_ms": start_ms,
                            "window_end_ms": pair_end_ms,
                            "left_leg_bps": left_pct * 10000.0 if left_pct is not None else None,
                            "right_leg_bps": right_pct * 10000.0 if right_pct is not None else None,
                            "net_bps": net_pct * 10000.0 if net_pct is not None else None,
                            "net_pct": net_pct * 100.0 if net_pct is not None else None,
                            "annualized_pct": (
                                net_pct / float(hours) * 24.0 * 365.0 * 100.0
                                if net_pct is not None and hours > 0
                                else None
                            ),
                            "usd_per_1000_notional": net_pct * 1000.0 if net_pct is not None else None,
                            "coverage_pct": coverage_pct,
                            "status": status,
                        }
                    )
                    if (
                        net_pct is not None
                        and (not series_source or float(net_pct) > float(series_source.get("net_pct") or -999.0))
                        and hours == 24
                    ):
                        series_source = {
                            "left_exchange": left_exchange,
                            "right_exchange": right_exchange,
                            "direction": direction,
                            "net_pct": net_pct,
                            "left_history": left_history,
                            "right_history": right_history,
                            "left_interval": left_interval,
                            "right_interval": right_interval,
                        }

    best_by_window: dict[str, Any] = {}
    for window in windows:
        label = str(window.get("label") or "")
        candidates = [
            row
            for row in pair_rows
            if str(row.get("window_label") or "") == label and _safe_float(row.get("net_bps")) is not None
        ]
        complete = [row for row in candidates if float(_safe_float(row.get("coverage_pct")) or 0.0) >= 95.0]
        pool = complete or candidates
        if not pool:
            continue
        best = max(pool, key=lambda row: float(_safe_float(row.get("net_bps")) or -999999.0))
        verdict = "favorable" if float(_safe_float(best.get("net_bps")) or 0.0) > 0 else "avoid"
        if float(_safe_float(best.get("coverage_pct")) or 0.0) < 95.0:
            verdict = "partial_data"
        best_by_window[label] = {**best, "verdict": verdict}

    spread_series: dict[str, Any] = {"points": [], "source": {}}
    if series_source:
        direction = str(series_source.get("direction") or "long_a_short_b")
        points = _funding_net_hourly_series(
            list(series_source.get("left_history") or []),
            list(series_source.get("right_history") or []),
            left_interval_hours=_safe_float(series_source.get("left_interval")),
            right_interval_hours=_safe_float(series_source.get("right_interval")),
            direction=direction,
            max_points=96,
        )
        spread_series = {
            "points": points,
            "source": {
                "left_exchange": series_source.get("left_exchange"),
                "right_exchange": series_source.get("right_exchange"),
                "direction": direction,
            },
        }

    pair_rows.sort(
        key=lambda row: (
            int(row.get("window_hours") or 0),
            -(float(_safe_float(row.get("net_bps")) or -999999.0)),
            str(row.get("pair_label") or ""),
        )
    )
    return pair_rows, best_by_window, spread_series


def _build_funding_history_timeline(
    exchange_rows: list[Mapping[str, Any]],
    *,
    max_hours: int,
) -> list[dict[str, Any]]:
    latest_ts_ms = 0
    for row in exchange_rows:
        for item in list(row.get("funding_history") or []):
            ts_ms = _funding_history_ts_ms(
                item.get("slot_ts_ms") or item.get("ts_ms") or item.get("timestamp")
            )
            if ts_ms:
                latest_ts_ms = max(latest_ts_ms, int(ts_ms))
    if latest_ts_ms <= 0:
        return []
    cutoff_ms = latest_ts_ms - max(1, int(max_hours)) * 3600 * 1000
    by_time: dict[int, dict[str, Any]] = {}
    for row in exchange_rows:
        exchange = str(row.get("exchange") or "")
        if not exchange:
            continue
        for item in list(row.get("funding_history") or []):
            raw_ts_ms = _funding_history_ts_ms(item.get("ts_ms") or item.get("timestamp"))
            slot_ts_ms = _funding_history_ts_ms(item.get("slot_ts_ms")) or _funding_slot_ts_ms(
                raw_ts_ms,
                _safe_float(item.get("interval_hours")),
            )
            if slot_ts_ms is None or slot_ts_ms < cutoff_ms:
                continue
            rate = _funding_rate_from_row(item)
            if rate is None:
                continue
            slot = by_time.setdefault(
                int(slot_ts_ms),
                {
                    "ts_ms": int(slot_ts_ms),
                    "time_utc": _funding_slot_iso(slot_ts_ms),
                    "exchanges": {},
                },
            )
            slot["exchanges"][exchange] = {
                "rate": float(rate),
                "rate_bps": float(rate) * 10000.0,
                "interval_hours": _safe_float(item.get("interval_hours")),
                "raw_ts_ms": raw_ts_ms,
                "raw_time_utc": _funding_slot_iso(raw_ts_ms),
            }
    return [by_time[key] for key in sorted(by_time.keys(), reverse=True)]


def _funding_net_hourly_series(
    left_history: list[dict[str, Any]],
    right_history: list[dict[str, Any]],
    *,
    left_interval_hours: float | None,
    right_interval_hours: float | None,
    direction: str,
    max_points: int = 168,
) -> list[dict[str, float]]:
    hour_ms = 3600 * 1000
    left_segments = _funding_event_segments(left_history, left_interval_hours)
    right_segments = _funding_event_segments(right_history, right_interval_hours)
    if not left_segments or not right_segments:
        return []

    latest_end_ms = min(
        int(_safe_float(left_segments[-1].get("end_ts_ms")) or 0),
        int(_safe_float(right_segments[-1].get("end_ts_ms")) or 0),
    )
    earliest_start_ms = max(
        int(_safe_float(left_segments[0].get("start_ts_ms")) or 0),
        int(_safe_float(right_segments[0].get("start_ts_ms")) or 0),
    )
    if latest_end_ms <= earliest_start_ms:
        return []

    bucket_end_ms = (latest_end_ms // hour_ms) * hour_ms
    if bucket_end_ms <= earliest_start_ms:
        bucket_end_ms += hour_ms

    left_mult = _funding_position_multiplier(direction, leg="left")
    right_mult = _funding_position_multiplier(direction, leg="right")
    rows: list[dict[str, float]] = []
    while bucket_end_ms - hour_ms >= earliest_start_ms:
        bucket_start_ms = bucket_end_ms - hour_ms
        left_pct, left_cov = _funding_carry_over_window_pct(
            left_segments,
            bucket_start_ms,
            bucket_end_ms,
            multiplier=left_mult,
        )
        right_pct, right_cov = _funding_carry_over_window_pct(
            right_segments,
            bucket_start_ms,
            bucket_end_ms,
            multiplier=right_mult,
        )
        if (
            left_pct is not None
            and right_pct is not None
            and left_cov >= 99.0
            and right_cov >= 99.0
        ):
            net_pct = float(left_pct) + float(right_pct)
            rows.append(
                {
                    "ts_ms": float(bucket_end_ms),
                    "left_bps": float(left_pct) * 10000.0,
                    "right_bps": float(right_pct) * 10000.0,
                    "net_bps": net_pct * 10000.0,
                }
            )
        bucket_end_ms -= hour_ms

    rows = list(reversed(rows))
    if max_points > 0 and len(rows) > max_points:
        rows = rows[-max_points:]
    return rows


def _downsample_chart_points(
    rows: list[dict[str, Any]],
    *,
    value_key: str,
    max_points: int = 96,
) -> list[dict[str, float]]:
    if max_points <= 0 or len(rows) <= max_points:
        return [
            {
                "ts_ms": float(_safe_float(item.get("ts_ms")) or 0.0),
                value_key: float(_safe_float(item.get(value_key)) or 0.0),
            }
            for item in rows
            if _safe_float(item.get("ts_ms")) is not None and _safe_float(item.get(value_key)) is not None
        ]
    step = max(1, int(math.ceil(len(rows) / float(max_points))))
    sampled = rows[::step]
    if sampled[-1] != rows[-1]:
        sampled.append(rows[-1])
    return [
        {
            "ts_ms": float(_safe_float(item.get("ts_ms")) or 0.0),
            value_key: float(_safe_float(item.get(value_key)) or 0.0),
        }
        for item in sampled
        if _safe_float(item.get("ts_ms")) is not None and _safe_float(item.get(value_key)) is not None
    ]


def _build_visual_window_rows(
    spread_series: list[dict[str, float]],
    funding_series: list[dict[str, float]],
) -> list[dict[str, Any]]:
    if not spread_series and not funding_series:
        return []
    candidate_hours = [4, 12, 24, 72]
    latest_ts_ms = 0
    if spread_series:
        latest_ts_ms = max(latest_ts_ms, int(_safe_float(spread_series[0].get("ts_ms")) or 0))
    if funding_series:
        latest_ts_ms = max(latest_ts_ms, int(_safe_float(funding_series[-1].get("ts_ms")) or 0))
    if latest_ts_ms <= 0:
        latest_ts_ms = int(time.time() * 1000)

    rows: list[dict[str, Any]] = []
    for hours in candidate_hours:
        cutoff_ms = latest_ts_ms - hours * 3600 * 1000
        spread_rows = [
            item for item in spread_series
            if int(_safe_float(item.get("ts_ms")) or 0) >= cutoff_ms
        ]
        funding_rows = [
            item for item in funding_series
            if int(_safe_float(item.get("ts_ms")) or 0) >= cutoff_ms
        ]
        if not spread_rows and not funding_rows:
            continue

        spread_values = [
            float(_safe_float(item.get("spread_pct")) or 0.0)
            for item in spread_rows
            if _safe_float(item.get("spread_pct")) is not None
        ]
        net_values = [
            float(_safe_float(item.get("net_bps")) or 0.0)
            for item in funding_rows
            if _safe_float(item.get("net_bps")) is not None
        ]
        positive_hours = len([value for value in net_values if value > 0])
        negative_hours = len([value for value in net_values if value < 0])
        net_total_bps = sum(net_values) if net_values else None
        positive_share_pct = (
            positive_hours / len(net_values) * 100.0
            if net_values
            else None
        )
        signal = "watch"
        if net_total_bps is not None:
            if net_total_bps > 0 and (positive_share_pct or 0.0) >= 65.0:
                signal = "favorable"
            elif net_total_bps < 0 and negative_hours >= max(1, int(len(net_values) * 0.5)):
                signal = "avoid"

        rows.append(
            {
                "label": f"{hours}h",
                "hours": hours,
                "funding_net_bps": net_total_bps,
                "funding_avg_hourly_bps": (
                    net_total_bps / len(net_values)
                    if net_values and net_total_bps is not None
                    else None
                ),
                "funding_positive_share_pct": positive_share_pct,
                "spread_current_pct": (
                    _safe_float(spread_rows[0].get("spread_pct"))
                    if spread_rows
                    else None
                ),
                "spread_mean_pct": (
                    sum(spread_values) / len(spread_values)
                    if spread_values
                    else None
                ),
                "spread_p95_abs_pct": _percentile([abs(value) for value in spread_values], 95),
                "spread_points": len(spread_values),
                "funding_points": len(net_values),
                "signal": signal,
            }
        )
    return rows


def _direction_label(
    direction: str,
    left_exchange: str,
    right_exchange: str,
) -> str:
    if str(direction or "").lower() == "long_b_short_a":
        return f"Long {right_exchange} / Short {left_exchange}"
    return f"Long {left_exchange} / Short {right_exchange}"


def _spread_pct_from_prices(left_px: float | None, right_px: float | None) -> float | None:
    if left_px is None or right_px is None:
        return None
    mid = (left_px + right_px) / 2.0
    if abs(mid) <= 1e-12:
        return None
    return (left_px - right_px) / mid * 100.0


def _nearest_snapshot(
    rows: list[dict[str, Any]],
    target_ts_ms: int,
    *,
    max_distance_ms: int = 10 * 60 * 1000,
) -> dict[str, Any] | None:
    best = None
    best_dist = None
    for item in rows or []:
        ts = int(_safe_float(item.get("ts_ms")) or 0)
        if ts <= 0:
            continue
        dist = abs(ts - target_ts_ms)
        if dist > max_distance_ms:
            continue
        if best is None or best_dist is None or dist < best_dist:
            best = item
            best_dist = dist
    return best


def _decision_spread_delta(
    direction: str,
    entry_open_spread_pct: float | None,
    left_snapshot: dict[str, Any] | None,
    right_snapshot: dict[str, Any] | None,
) -> tuple[float | None, float | None]:
    if not left_snapshot or not right_snapshot:
        return None, None
    bid_a = _safe_float(left_snapshot.get("bid"))
    ask_a = _safe_float(left_snapshot.get("ask"))
    bid_b = _safe_float(right_snapshot.get("bid"))
    ask_b = _safe_float(right_snapshot.get("ask"))
    direction_text = str(direction or "").lower()
    future_close = None
    if direction_text == "long_b_short_a":
        future_close = _spread_pct_from_prices(bid_b, ask_a)
    else:
        future_close = _spread_pct_from_prices(bid_a, ask_b)
    if future_close is None or entry_open_spread_pct is None:
        return future_close, None
    return future_close, future_close - entry_open_spread_pct


def _size_appropriateness_for_action(
    action: str,
    spread_delta_pct: float | None,
) -> str:
    if spread_delta_pct is None:
        return "insufficient_data"
    a = str(action or "").strip().upper()
    d = float(spread_delta_pct)
    mag = abs(d)

    if a == "ENTRY_STRONG":
        if d <= 0:
            return "wrong_direction"
        if d >= 0.12:
            return "appropriate"
        return "too_aggressive"
    if a in {"ENTRY_SMALL", "ADD_SMALL"}:
        if d <= 0:
            return "wrong_direction"
        if d >= 0.18:
            return "too_conservative"
        return "appropriate"
    if a == "FULL_EXIT":
        if d >= 0:
            return "wrong_direction"
        if d <= -0.12:
            return "appropriate"
        return "too_aggressive"
    if a == "PARTIAL_EXIT":
        if d >= 0:
            return "wrong_direction"
        if d <= -0.18:
            return "too_conservative"
        return "appropriate"
    if a in {"HOLD", "NO_TRADE", "ADD_BLOCKED"}:
        if mag <= 0.06:
            return "appropriate"
        if d > 0:
            return "too_conservative"
        return "too_aggressive"
    return "not_applicable"


def _action_size_ratio(action: str) -> float:
    a = str(action or "").strip().upper()
    if a == "ENTRY_STRONG":
        return 1.0
    if a in {"ENTRY_SMALL", "PARTIAL_EXIT"}:
        return 0.5
    if a == "ADD_SMALL":
        return 0.25
    if a == "FULL_EXIT":
        return 1.0
    return 0.0


def _execution_cost_components_pct(
    *,
    action: str,
    left_exchange: str,
    right_exchange: str,
) -> tuple[float, float, float, float]:
    size_ratio = _action_size_ratio(action)
    if size_ratio <= 0:
        return 0.0, 0.0, 0.0, size_ratio
    left_fee = float(
        (EXCHANGE_COMMISSIONS.get(str(left_exchange or "").lower(), {}) or {}).get(
            "taker",
            OUTCOME_DEFAULT_TAKER_FEE_RATE,
        )
    )
    right_fee = float(
        (EXCHANGE_COMMISSIONS.get(str(right_exchange or "").lower(), {}) or {}).get(
            "taker",
            OUTCOME_DEFAULT_TAKER_FEE_RATE,
        )
    )
    fees_pct = -(left_fee + right_fee) * 100.0 * size_ratio
    slippage_pct = -((OUTCOME_ASSUMED_SLIPPAGE_BPS_PER_LEG * 2.0) / 100.0) * size_ratio
    return fees_pct, slippage_pct, (fees_pct + slippage_pct), size_ratio


def _estimate_funding_component_pct(
    *,
    horizon: str,
    decision_ts_ms: int,
    horizon_target_ts_ms: int,
    funding_to_next_pct: float | None,
    net_funding_hourly: float | None,
    hours_to_next_funding: float | None,
) -> float | None:
    if horizon_target_ts_ms <= decision_ts_ms:
        return None
    if horizon == "to_next_funding" and funding_to_next_pct is not None:
        return float(funding_to_next_pct)

    hourly = net_funding_hourly
    if hourly is None and funding_to_next_pct is not None:
        h = _safe_float(hours_to_next_funding)
        if h is not None and h > 1e-9:
            hourly = float(funding_to_next_pct) / h
    if hourly is None:
        return None

    elapsed_h = (float(horizon_target_ts_ms) - float(decision_ts_ms)) / 3_600_000.0
    if elapsed_h <= 0:
        return None
    return float(hourly) * elapsed_h


def _is_horizon_matured(
    *,
    horizon: str,
    horizon_target_ts_ms: int | None,
    now_ts_ms: int,
) -> bool:
    target_ts = int(horizon_target_ts_ms or 0)
    if horizon in {"15m", "1h", "4h"}:
        return target_ts > 0 and target_ts <= (now_ts_ms - COIN_OUTCOME_MATURITY_GRACE_MS)
    if horizon in {"to_next_funding", "to_exit"}:
        return target_ts > 0 and target_ts <= (now_ts_ms - COIN_OUTCOME_MATURITY_GRACE_MS)
    return False


def _next_funding_target_ts_ms(
    decision_ts_ms: int,
    left_decision_snapshot: dict[str, Any] | None,
    right_decision_snapshot: dict[str, Any] | None,
    feature_common: Mapping[str, Any] | None,
) -> int | None:
    candidates: list[int] = []
    for snap in (left_decision_snapshot, right_decision_snapshot):
        if not snap:
            continue
        next_ts = int(_safe_float(snap.get("next_funding_ts_ms")) or 0)
        if next_ts > decision_ts_ms:
            candidates.append(next_ts)
    if candidates:
        return min(candidates)

    common = dict(feature_common or {})
    funding_meta = dict(common.get("funding") or {})
    left_int = _safe_float(funding_meta.get("left_interval_hours"))
    right_int = _safe_float(funding_meta.get("right_interval_hours"))
    intervals = [x for x in (left_int, right_int) if x is not None and x > 0]
    if not intervals:
        return None
    interval_h = min(intervals)
    return decision_ts_ms + int(interval_h * 3600.0 * 1000.0)


def _paper_exit_target_ts_ms(
    decision_ts_ms: int,
    state_ref: str,
    paper_positions_by_key: Mapping[str, Mapping[str, Any]],
    paper_events_by_key: Mapping[str, list[dict[str, Any]]],
) -> int | None:
    key = str(state_ref or "").strip()
    if not key:
        return None
    row = dict((paper_positions_by_key or {}).get(key) or {})
    candidates: list[int] = []
    closed_at_ms = int(_safe_float(row.get("closed_at_ms")) or 0)
    if closed_at_ms > decision_ts_ms:
        candidates.append(closed_at_ms)

    for event in list((paper_events_by_key or {}).get(key) or []):
        ts_ms = int(_safe_float(event.get("ts_ms")) or 0)
        if ts_ms <= decision_ts_ms:
            continue
        event_type = str(event.get("event_type") or "").strip().lower()
        payload = dict(event.get("payload") or {})
        status = str(payload.get("status") or "").strip().lower()
        if event_type in {"full_exit", "closed", "exit"} or status == "closed":
            candidates.append(ts_ms)
    if not candidates:
        return None
    return min(candidates)


def _real_exit_target_ts_ms(
    decision_ts_ms: int,
    state_ref: str,
    real_observations_by_key: Mapping[str, list[dict[str, Any]]],
) -> int | None:
    key = str(state_ref or "").strip()
    if not key:
        return None
    rows = list((real_observations_by_key or {}).get(key) or [])
    if not rows:
        return None
    candidates: list[int] = []
    for row in rows:
        ts_ms = int(_safe_float(row.get("ts_ms")) or 0)
        if ts_ms <= decision_ts_ms:
            continue
        if str(row.get("status") or "").strip().lower() == "closed":
            candidates.append(ts_ms)
    if not candidates:
        return None
    return min(candidates)


def _outcome_phase_bucket(phase: object) -> str:
    text = str(phase or "").strip().lower()
    if not text:
        return "unknown"
    if "pre_boundary" in text:
        return "pre_boundary"
    if "mid_interval" in text:
        return "mid_interval"
    if "emergency" in text:
        return "emergency"
    if "boundary" in text:
        return "boundary"
    if "entry" in text:
        return "entry"
    if "exit" in text:
        return "exit"
    return "other"


def _new_outcome_bucket() -> dict[str, Any]:
    return {
        "total": 0,
        "correct": 0,
        "incorrect": 0,
        "mixed": 0,
        "insufficient_data": 0,
        "unknown": 0,
        "known_total": 0,
        "correct_rate_pct": None,
    }


def _finalize_outcome_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    known_total = int(bucket.get("correct", 0)) + int(bucket.get("incorrect", 0)) + int(bucket.get("mixed", 0))
    bucket["known_total"] = known_total
    if known_total > 0:
        bucket["correct_rate_pct"] = (float(bucket.get("correct", 0)) / float(known_total)) * 100.0
    else:
        bucket["correct_rate_pct"] = None
    return bucket


def _build_outcomes_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_horizon: dict[str, dict[str, Any]] = {}
    by_phase_bucket: dict[str, dict[str, Any]] = {}
    by_phase_horizon: dict[str, dict[str, Any]] = {}
    timing_quality: dict[str, int] = {}
    size_appropriateness: dict[str, int] = {}
    wait_help = {"true": 0, "false": 0, "unknown": 0}
    early_exit_help = {"true": 0, "false": 0, "unknown": 0}

    for row in rows:
        outcome = dict(row.get("outcome") or {})
        horizon = str(row.get("horizon") or outcome.get("horizon") or "unknown")
        phase = str(outcome.get("decision_phase") or row.get("decision_phase") or "unknown")
        phase_bucket = _outcome_phase_bucket(phase)
        correctness = str(outcome.get("decision_correctness") or "unknown").strip().lower()
        if correctness not in {"correct", "incorrect", "mixed", "insufficient_data"}:
            correctness = "unknown"

        horizon_bucket = by_horizon.setdefault(horizon, _new_outcome_bucket())
        phase_bucket_row = by_phase_bucket.setdefault(phase_bucket, _new_outcome_bucket())
        phase_horizon_key = f"{phase_bucket}|{horizon}"
        phase_horizon_bucket = by_phase_horizon.setdefault(phase_horizon_key, _new_outcome_bucket())

        for bucket in (horizon_bucket, phase_bucket_row, phase_horizon_bucket):
            bucket["total"] = int(bucket.get("total", 0)) + 1
            bucket[correctness] = int(bucket.get(correctness, 0)) + 1

        quality = str(outcome.get("timing_quality") or "unknown").strip().lower()
        timing_quality[quality] = timing_quality.get(quality, 0) + 1
        size_fit = str(outcome.get("size_appropriateness") or "unknown").strip().lower()
        size_appropriateness[size_fit] = size_appropriateness.get(size_fit, 0) + 1

        wait_value = outcome.get("would_waiting_15m_help")
        if wait_value is True:
            wait_help["true"] += 1
        elif wait_value is False:
            wait_help["false"] += 1
        else:
            wait_help["unknown"] += 1

        early_exit_value = outcome.get("would_exiting_15m_earlier_help")
        if early_exit_value is True:
            early_exit_help["true"] += 1
        elif early_exit_value is False:
            early_exit_help["false"] += 1
        else:
            early_exit_help["unknown"] += 1

    for name in list(by_horizon.keys()):
        by_horizon[name] = _finalize_outcome_bucket(by_horizon[name])
    for name in list(by_phase_bucket.keys()):
        by_phase_bucket[name] = _finalize_outcome_bucket(by_phase_bucket[name])
    for name in list(by_phase_horizon.keys()):
        by_phase_horizon[name] = _finalize_outcome_bucket(by_phase_horizon[name])

    return {
        "total": len(rows),
        "by_horizon": by_horizon,
        "by_phase_bucket": by_phase_bucket,
        "by_phase_horizon": by_phase_horizon,
        "timing_quality": timing_quality,
        "size_appropriateness": size_appropriateness,
        "would_waiting_15m_help": wait_help,
        "would_exiting_15m_earlier_help": early_exit_help,
        "operator_scorecard_pre_boundary": _build_pre_boundary_operator_scorecard(rows),
    }


def _new_operator_score_bucket() -> dict[str, Any]:
    return {
        "total": 0,
        "correct": 0,
        "incorrect": 0,
        "mixed": 0,
        "insufficient_data": 0,
        "unknown": 0,
        "known_total": 0,
        "hit_rate_pct": None,
        "wrong_rate_pct": None,
        "mixed_rate_pct": None,
        "wait_help_true": 0,
        "wait_help_false": 0,
        "wait_help_unknown": 0,
        "wait_help_rate_pct": None,
        "early_exit_help_true": 0,
        "early_exit_help_false": 0,
        "early_exit_help_unknown": 0,
        "early_exit_help_rate_pct": None,
    }


def _safe_rate_pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return (float(numerator) / float(denominator)) * 100.0


def _finalize_operator_score_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    known_total = int(bucket.get("correct", 0)) + int(bucket.get("incorrect", 0)) + int(bucket.get("mixed", 0))
    bucket["known_total"] = known_total
    bucket["hit_rate_pct"] = _safe_rate_pct(int(bucket.get("correct", 0)), known_total)
    bucket["wrong_rate_pct"] = _safe_rate_pct(int(bucket.get("incorrect", 0)), known_total)
    bucket["mixed_rate_pct"] = _safe_rate_pct(int(bucket.get("mixed", 0)), known_total)

    wait_known = int(bucket.get("wait_help_true", 0)) + int(bucket.get("wait_help_false", 0))
    bucket["wait_help_rate_pct"] = _safe_rate_pct(int(bucket.get("wait_help_true", 0)), wait_known)

    early_exit_known = int(bucket.get("early_exit_help_true", 0)) + int(bucket.get("early_exit_help_false", 0))
    bucket["early_exit_help_rate_pct"] = _safe_rate_pct(
        int(bucket.get("early_exit_help_true", 0)),
        early_exit_known,
    )
    return bucket


def _build_operator_traffic_light(bucket: dict[str, Any]) -> dict[str, Any]:
    known_total = int(bucket.get("known_total", 0))
    hit_rate = _safe_float(bucket.get("hit_rate_pct"))
    wrong_rate = _safe_float(bucket.get("wrong_rate_pct"))
    wait_help_rate = _safe_float(bucket.get("wait_help_rate_pct"))
    early_exit_help_rate = _safe_float(bucket.get("early_exit_help_rate_pct"))

    score = 0
    reasons: list[str] = []

    if known_total <= 0:
        return {
            "status": "gray",
            "score": 0,
            "reasons": ["no_known_outcomes"],
        }
    if known_total >= 20:
        score += 2
        reasons.append("sample_depth_good")
    elif known_total >= 8:
        score += 1
        reasons.append("sample_depth_ok")
    else:
        score -= 1
        reasons.append("sample_depth_low")

    if hit_rate is not None:
        if hit_rate >= 70.0:
            score += 2
            reasons.append("hit_rate_strong")
        elif hit_rate >= 55.0:
            score += 1
            reasons.append("hit_rate_ok")
        elif hit_rate >= 45.0:
            reasons.append("hit_rate_mixed")
        else:
            score -= 2
            reasons.append("hit_rate_weak")

    if wrong_rate is not None:
        if wrong_rate <= 15.0:
            score += 2
            reasons.append("wrong_rate_low")
        elif wrong_rate <= 25.0:
            score += 1
            reasons.append("wrong_rate_ok")
        elif wrong_rate <= 35.0:
            reasons.append("wrong_rate_mixed")
        else:
            score -= 2
            reasons.append("wrong_rate_high")

    if wait_help_rate is not None:
        if wait_help_rate <= 25.0:
            score += 1
            reasons.append("timing_wait_signal_good")
        elif wait_help_rate > 40.0:
            score -= 1
            reasons.append("timing_wait_signal_bad")

    if early_exit_help_rate is not None:
        if early_exit_help_rate <= 25.0:
            score += 1
            reasons.append("timing_early_exit_signal_good")
        elif early_exit_help_rate > 40.0:
            score -= 1
            reasons.append("timing_early_exit_signal_bad")

    status = "red"
    if score >= 4:
        status = "green"
    elif score >= 1:
        status = "yellow"
    if known_total < 5 and status == "green":
        status = "yellow"
        reasons.append("limited_sample_cap")

    return {
        "status": status,
        "score": score,
        "reasons": reasons,
    }


def _apply_operator_score_row(bucket: dict[str, Any], row: dict[str, Any]) -> None:
    outcome = dict(row.get("outcome") or {})
    correctness = str(outcome.get("decision_correctness") or "unknown").strip().lower()
    if correctness not in {"correct", "incorrect", "mixed", "insufficient_data"}:
        correctness = "unknown"
    bucket["total"] = int(bucket.get("total", 0)) + 1
    bucket[correctness] = int(bucket.get(correctness, 0)) + 1

    wait_value = outcome.get("would_waiting_15m_help")
    if wait_value is True:
        bucket["wait_help_true"] = int(bucket.get("wait_help_true", 0)) + 1
    elif wait_value is False:
        bucket["wait_help_false"] = int(bucket.get("wait_help_false", 0)) + 1
    else:
        bucket["wait_help_unknown"] = int(bucket.get("wait_help_unknown", 0)) + 1

    early_exit_value = outcome.get("would_exiting_15m_earlier_help")
    if early_exit_value is True:
        bucket["early_exit_help_true"] = int(bucket.get("early_exit_help_true", 0)) + 1
    elif early_exit_value is False:
        bucket["early_exit_help_false"] = int(bucket.get("early_exit_help_false", 0)) + 1
    else:
        bucket["early_exit_help_unknown"] = int(bucket.get("early_exit_help_unknown", 0)) + 1


def _build_pre_boundary_operator_scorecard(rows: list[dict[str, Any]]) -> dict[str, Any]:
    filtered: list[dict[str, Any]] = []
    phases: set[str] = set()
    for row in rows:
        outcome = dict(row.get("outcome") or {})
        raw_phase = str(outcome.get("decision_phase") or row.get("decision_phase") or "unknown")
        if _outcome_phase_bucket(raw_phase) != "pre_boundary":
            continue
        filtered.append(row)
        phases.add(raw_phase)

    overall = _new_operator_score_bucket()
    by_horizon: dict[str, dict[str, Any]] = {}
    for row in filtered:
        _apply_operator_score_row(overall, row)
        horizon = str(row.get("horizon") or "unknown")
        item = by_horizon.setdefault(horizon, _new_operator_score_bucket())
        _apply_operator_score_row(item, row)

    overall = _finalize_operator_score_bucket(overall)
    overall["traffic_light"] = _build_operator_traffic_light(overall)
    for horizon in list(by_horizon.keys()):
        by_horizon[horizon] = _finalize_operator_score_bucket(by_horizon[horizon])
        by_horizon[horizon]["traffic_light"] = _build_operator_traffic_light(by_horizon[horizon])

    return {
        "phase_bucket": "pre_boundary",
        "phase_values_seen": sorted(phases),
        "total_rows": len(filtered),
        "overall": overall,
        "by_horizon": by_horizon,
        "traffic_light": dict(overall.get("traffic_light") or {}),
    }


def _new_review_score_bucket() -> dict[str, Any]:
    return {
        "total": 0,
        "correct": 0,
        "incorrect": 0,
        "mixed": 0,
        "insufficient_data": 0,
        "unknown": 0,
        "known_total": 0,
        "correct_rate_pct": None,
        "avg_net_pnl_delta_pct": None,
        "avg_alt_delta_pct": None,
        "_net_sum": 0.0,
        "_net_count": 0,
        "_alt_sum": 0.0,
        "_alt_count": 0,
    }


def _apply_review_score_row(bucket: dict[str, Any], row: Mapping[str, Any]) -> None:
    outcome = dict(row.get("outcome") or {})
    correctness = str(outcome.get("decision_correctness") or "unknown").strip().lower()
    if correctness not in {"correct", "incorrect", "mixed", "insufficient_data"}:
        correctness = "unknown"
    bucket["total"] = int(bucket.get("total", 0)) + 1
    bucket[correctness] = int(bucket.get(correctness, 0)) + 1

    net_delta_pct = _safe_float(outcome.get("net_pnl_delta_pct"))
    if net_delta_pct is not None:
        bucket["_net_sum"] = float(bucket.get("_net_sum", 0.0)) + float(net_delta_pct)
        bucket["_net_count"] = int(bucket.get("_net_count", 0)) + 1
    alt_delta = _safe_float(outcome.get("net_pnl_delta_vs_alternative"))
    if alt_delta is not None:
        bucket["_alt_sum"] = float(bucket.get("_alt_sum", 0.0)) + float(alt_delta)
        bucket["_alt_count"] = int(bucket.get("_alt_count", 0)) + 1


def _finalize_review_score_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    known_total = int(bucket.get("correct", 0)) + int(bucket.get("incorrect", 0)) + int(bucket.get("mixed", 0))
    bucket["known_total"] = known_total
    bucket["correct_rate_pct"] = _safe_rate_pct(int(bucket.get("correct", 0)), known_total)
    net_count = int(bucket.get("_net_count", 0))
    alt_count = int(bucket.get("_alt_count", 0))
    bucket["avg_net_pnl_delta_pct"] = (
        float(bucket.get("_net_sum", 0.0)) / float(net_count) if net_count > 0 else None
    )
    bucket["avg_alt_delta_pct"] = (
        float(bucket.get("_alt_sum", 0.0)) / float(alt_count) if alt_count > 0 else None
    )
    bucket.pop("_net_sum", None)
    bucket.pop("_net_count", None)
    bucket.pop("_alt_sum", None)
    bucket.pop("_alt_count", None)
    return bucket


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
        self._notifier = NotificationRouter()
        self._accounts = AccountMonitor(
            refresh_interval=self._account_interval,
            summary_interval=self._summary_interval,
            on_margin_adjust=self._on_margin_adjust_events,
            notifier=self._notifier,
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
        self._protective_manager = ProtectiveOrderManager(self._risk_config, notifier=self._notifier)
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
        self._auto_arb_store = JsonStateStore(AUTO_ARB_STATE_PATH)
        self._auto_arb_history_store = JsonlEventStore(AUTO_ARB_HISTORY_PATH)
        self._auto_arb: dict[str, Any] = self._load_auto_arb_config()
        self._auto_arb_lock = asyncio.Lock()
        self._auto_arb_task: Optional[asyncio.Task] = None
        self._auto_arb_poll_sec = 3.0
        self._auto_strategy_store = JsonStateStore(AUTO_STRATEGY_STATE_PATH)
        self._auto_strategy_history_store = JsonlEventStore(AUTO_STRATEGY_HISTORY_PATH)
        self._auto_strategies: dict[str, Any] = self._load_auto_strategy_config()
        self._auto_strategy_lock = asyncio.Lock()
        self._auto_strategy_task: Optional[asyncio.Task] = None
        self._auto_strategy_poll_sec = float(
            (self._auto_strategies.get("defaults") or {}).get("poll_sec")
            or AUTO_STRATEGY_DEFAULTS["poll_sec"]
        )
        self._auto_strategy_queue: list[dict[str, Any]] = []
        self._auto_strategy_events: list[dict[str, Any]] = []
        self._auto_strategy_event_limit = 100
        self._auto_exit_store = JsonStateStore(AUTO_EXIT_STATE_PATH)
        self._auto_exit_history_store = JsonlEventStore(AUTO_EXIT_HISTORY_PATH)
        self._auto_exit: dict[str, Any] = self._load_auto_exit_config()
        self._auto_exit_lock = asyncio.Lock()
        self._auto_exit_task: Optional[asyncio.Task] = None
        self._auto_exit_poll_sec = AUTO_EXIT_POLL_SEC
        self._auto_exit_inflight = False
        self._auto_exit_live_spreads: dict[str, float] = {}
        self._auto_exit_diagnostics: list[dict[str, Any]] = []
        self._auto_exit_v1_diagnostics: list[dict[str, Any]] = []
        self._auto_exit_events: list[dict[str, Any]] = []
        self._auto_exit_event_limit = 60
        self._auto_exit_last_log_ts: dict[str, float] = {}
        self._auto_exit_completed_run_cleanup: set[str] = set()
        self._auto_exit_log_cooldown_sec = AUTO_EXIT_LOG_COOLDOWN_SEC
        self._hedge_cluster_store = JsonStateStore(HEDGE_CLUSTER_STATE_PATH)
        self._hedge_clusters: dict[str, Any] = self._load_hedge_cluster_config()
        self._derisk_history_store = JsonlEventStore(DERISK_HISTORY_PATH)
        self._derisk_outcome_store = JsonStateStore(DERISK_OUTCOME_STATE_PATH)
        self._derisk_lock = asyncio.Lock()
        self._derisk_task: Optional[asyncio.Task] = None
        self._derisk_inflight = False
        self._derisk_exchange_health: dict[str, Any] = {}
        self._derisk_diagnostics: list[dict[str, Any]] = []
        self._derisk_events: list[dict[str, Any]] = []
        self._derisk_event_limit = DERISK_EVENT_LIMIT
        self._derisk_last_log_ts: dict[str, float] = {}
        self._derisk_log_cooldown_sec = DERISK_LOG_COOLDOWN_SEC
        self._derisk_cluster_state: dict[str, Any] = {}
        self._derisk_poll_sec = 5.0
        self._derisk_active_cycle_id: str | None = None
        self._derisk_outcome_state: dict[str, Any] = self._load_derisk_outcome_state()
        self._coin_analysis_cache: dict[tuple[str, int, int, tuple[str, ...]], tuple[float, dict[str, Any]]] = {}
        self._coin_focus_task: Optional[asyncio.Task] = None
        self._coin_focus_poll_sec = COIN_FOCUS_POLL_SEC
        self._coin_shortlist_poll_sec = COIN_SHORTLIST_POLL_SEC
        self._coin_shortlist_last_run_ts = 0.0
        self._coin_shortlist_last_cycle: dict[str, Any] = {}
        self._coin_outcomes_task: Optional[asyncio.Task] = None
        self._coin_outcomes_poll_sec = COIN_OUTCOME_POLL_SEC
        self._coin_outcomes_scheduler_enabled = True
        self._coin_outcomes_last_cycle: dict[str, Any] = {}
        self._coin_outcomes_cycle_history: list[dict[str, Any]] = []
        self._coin_outcomes_cycle_history_limit = 50
        self._coin_retention_task: Optional[asyncio.Task] = None
        self._coin_retention_poll_sec = COIN_RETENTION_POLL_SEC
        self._coin_retention_max_age_days = COIN_RETENTION_MAX_AGE_DAYS_DEFAULT
        self._coin_retention_closed_paper_days = COIN_RETENTION_CLOSED_PAPER_DAYS_DEFAULT
        self._coin_retention_last_report: dict[str, Any] = {}
        self._coin_position_watcher_task: Optional[asyncio.Task] = None
        self._coin_position_watcher_poll_sec = COIN_POSITION_WATCHER_POLL_SEC
        self._coin_position_watcher_enabled = True
        self._coin_position_watcher_last_cycle: dict[str, Any] = {}
        self._coin_position_watcher_last_by_symbol_ts: dict[str, float] = {}
        self._mexc_alert_cooldown = 600  # seconds
        self._last_mexc_alert: dict[tuple[str, str], float] = {}
        self._send_missing_stop_alerts = True
        self._margin_logic_state: dict[str, str] = {}
        self._margin_logic_log: list[dict[str, Any]] = []
        self._margin_logic_log_limit = 80
        self._snapshot_dict_cache_key: int | None = None
        self._snapshot_dict_cache: dict[str, object] | None = None
        self._account_state_cache_key: tuple[Any, ...] | None = None
        self._account_state_cache: dict[str, object] | None = None
        self._apply_alert_settings()

    async def bootstrap_symbol_session(
        self,
        symbol: str,
        *,
        ttl_sec: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol must be provided for session bootstrap.")
        ts_ms = int(now_ms or (time.time() * 1000))
        ttl = max(60, int(ttl_sec or COIN_ANALYSIS_SESSION_TTL_SEC))
        row = CoinSymbolSessionRow(
            canonical_symbol=canonical,
            started_at_ms=ts_ms,
            expires_at_ms=ts_ms + ttl * 1000,
            is_tracking=True,
            updated_at_ms=ts_ms,
        )
        await asyncio.to_thread(upsert_symbol_session, row)
        return {
            "canonical_symbol": canonical,
            "started_at_ms": row.started_at_ms,
            "expires_at_ms": row.expires_at_ms,
            "ttl_sec": ttl,
            "tracking": True,
        }

    async def start_coin_symbol_session(
        self,
        symbol: str,
        *,
        ttl_sec: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        return await self.bootstrap_symbol_session(symbol, ttl_sec=ttl_sec, now_ms=now_ms)

    async def extend_coin_symbol_session(
        self,
        symbol: str,
        *,
        ttl_sec: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        return await self.bootstrap_symbol_session(symbol, ttl_sec=ttl_sec, now_ms=now_ms)

    async def stop_coin_symbol_session(
        self,
        symbol: str,
        *,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol must be provided for session stop.")
        ts_ms = int(now_ms or (time.time() * 1000))
        row = CoinSymbolSessionRow(
            canonical_symbol=canonical,
            started_at_ms=ts_ms,
            expires_at_ms=ts_ms,
            is_tracking=False,
            updated_at_ms=ts_ms,
        )
        await asyncio.to_thread(upsert_symbol_session, row)
        return {
            "canonical_symbol": canonical,
            "stopped_at_ms": ts_ms,
            "tracking": False,
        }

    async def list_active_coin_symbol_sessions(
        self,
        *,
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        ts_ms = int(now_ms or (time.time() * 1000))
        await asyncio.to_thread(expire_symbol_sessions, ts_ms)
        active = await asyncio.to_thread(get_active_symbol_sessions, ts_ms)
        out: list[dict[str, Any]] = []
        for row in active:
            out.append(
                {
                    "canonical_symbol": row.canonical_symbol,
                    "started_at_ms": row.started_at_ms,
                    "expires_at_ms": row.expires_at_ms,
                    "updated_at_ms": row.updated_at_ms,
                    "tracking": bool(row.is_tracking),
                }
            )
        return out

    async def _record_coin_trade_activity(
        self,
        *,
        canonical_symbol: str,
        activity_type: str,
        ts_ms: int | None = None,
        pair_key: str | None = None,
        direction: str | None = None,
        source: str | None = None,
        state_ref: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        safe_symbol = normalize_symbol(canonical_symbol)
        if not safe_symbol:
            return
        safe_ts_ms = int(ts_ms or (time.time() * 1000))
        event_id = (
            f"coin-activity-{safe_symbol.lower()}-"
            f"{str(activity_type or 'unknown').lower()}-{safe_ts_ms}-{uuid4().hex[:8]}"
        )
        await asyncio.to_thread(
            insert_trade_activity,
            CoinTradeActivityRow(
                event_id=event_id,
                ts_ms=safe_ts_ms,
                canonical_symbol=safe_symbol,
                pair_key=str(pair_key or "").strip() or None,
                direction=str(direction or "").strip() or None,
                activity_type=str(activity_type or "unknown").strip().lower() or "unknown",
                source=str(source or "").strip() or None,
                state_ref=str(state_ref or "").strip() or None,
                payload=dict(payload or {}),
            ),
        )

    def _coin_shortlist_candidates_from_snapshot(self, top_n: int = 3) -> list[dict[str, Any]]:
        snapshot = self._snapshot
        if snapshot is None:
            return []
        candidates: list[dict[str, Any]] = []
        seen_symbols: set[str] = set()
        opportunities = sorted(
            list(snapshot.opportunities or []),
            key=lambda item: abs(_safe_float(getattr(item, "effective_spread", None)) or _safe_float(getattr(item, "spread", None)) or 0.0),
            reverse=True,
        )
        for item in opportunities:
            symbol = normalize_symbol(str(getattr(item, "symbol", "") or ""))
            long_exchange = normalize_exchange_name(str(getattr(item, "long_exchange", "") or ""))
            short_exchange = normalize_exchange_name(str(getattr(item, "short_exchange", "") or ""))
            if not symbol or symbol in seen_symbols:
                continue
            if {long_exchange, short_exchange} != set(COIN_ANALYSIS_CORE_EXCHANGES):
                continue
            seen_symbols.add(symbol)
            candidates.append(
                {
                    "symbol": symbol,
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "spread": _safe_float(getattr(item, "spread", None)),
                    "effective_spread": _safe_float(getattr(item, "effective_spread", None)),
                    "price_diff_pct": _safe_float(getattr(item, "price_diff_pct", None)),
                }
            )
            if len(candidates) >= max(1, int(top_n)):
                break
        return candidates

    def _coin_shortlist_row_from_analysis(
        self,
        *,
        rank: int,
        source_name: str,
        analysis: Mapping[str, Any],
    ) -> CoinCandidateShortlistRow | None:
        canonical = normalize_symbol(str(analysis.get("symbol") or ""))
        if not canonical:
            return None
        pairs = list(analysis.get("pair_analysis") or [])
        selected: Mapping[str, Any] | None = None
        for row in pairs:
            left = normalize_exchange_name(str(row.get("left_exchange") or ""))
            right = normalize_exchange_name(str(row.get("right_exchange") or ""))
            if {left, right} == set(COIN_ANALYSIS_CORE_EXCHANGES):
                selected = row
                break
        if selected is None and pairs:
            selected = pairs[0]
        if selected is None:
            return None
        direction = str((analysis.get("bot_logic") or {}).get("recommended_pair", {}).get("direction") or "long_a_short_b")
        scores = dict(selected.get("scores") or {})
        spread_block = dict(selected.get("spread") or {})
        premium_block = dict(selected.get("premium") or {})
        oi_block = dict(selected.get("open_interest") or {})
        reasons = [str(item) for item in list(selected.get("reasons") or [])[:8]]
        return CoinCandidateShortlistRow(
            ts_ms=int(time.time() * 1000),
            canonical_symbol=canonical,
            pair_key=str(selected.get("pair_key") or ""),
            rank=max(1, int(rank)),
            source_name=str(source_name or "markets_top3"),
            direction_hint=direction,
            candidate_score=_safe_float(selected.get("score")) or _safe_float(scores.get("entry_score")),
            funding_edge_pct=(
                _safe_float(((selected.get("funding") or {}).get("delta_pct")))
                or _safe_float(((selected.get("funding_hourly") or {}).get("delta")))
                or _safe_float(scores.get("funding_score"))
            ),
            entry_spread_pct=(
                _safe_float(((selected.get("derived_spread") or {}).get("open_spread_pct")))
                or _safe_float(spread_block.get("current_pct"))
            ),
            premium_diff_pct=_safe_float(premium_block.get("delta_pct")),
            oi_change_1h_pct=_safe_float(oi_block.get("oi_change_1h_pct")),
            oi_change_4h_pct=_safe_float(oi_block.get("oi_change_4h_pct")),
            reason_codes=reasons,
            payload={
                "score": _safe_float(selected.get("score")),
                "recommendation": selected.get("recommendation"),
                "decision_phase": selected.get("decision_phase"),
                "reasons": reasons,
                "spread": spread_block,
                "premium": premium_block,
                "open_interest": oi_block,
                "bot_logic": analysis.get("bot_logic") or {},
            },
        )

    async def collect_coin_candidate_shortlist_once(self, *, top_n: int = 3) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        snapshot_candidates = self._coin_shortlist_candidates_from_snapshot(top_n=max(1, int(top_n)))
        if not snapshot_candidates:
            cycle = {
                "ts_ms": now_ms,
                "top_n": max(1, int(top_n)),
                "shortlisted": 0,
                "source_candidates": 0,
                "symbols": [],
            }
            self._coin_shortlist_last_cycle = cycle
            self._coin_shortlist_last_run_ts = time.time()
            return cycle

        stored_rows: list[CoinCandidateShortlistRow] = []
        shortlisted_symbols: list[str] = []
        errors: list[dict[str, str]] = []
        for rank, candidate in enumerate(snapshot_candidates, start=1):
            symbol = normalize_symbol(str(candidate.get("symbol") or ""))
            if not symbol:
                continue
            try:
                analysis = await self.analyze_symbol(
                    symbol,
                    window_minutes=240,
                    funding_points=96,
                    use_cache=True,
                    persist_candidate_decision=False,
                    run_position_logic=False,
                )
                row = self._coin_shortlist_row_from_analysis(
                    rank=rank,
                    source_name="markets_top3",
                    analysis=analysis,
                )
                if row is None:
                    continue
                stored_rows.append(row)
                shortlisted_symbols.append(symbol)
                await self.bootstrap_symbol_session(symbol, ttl_sec=45 * 60, now_ms=now_ms)
            except Exception as exc:  # pylint: disable=broad-except
                errors.append({"symbol": symbol, "error": str(exc)})
        inserted = 0
        if stored_rows:
            inserted = await asyncio.to_thread(insert_candidate_shortlist_rows, stored_rows)
        cycle = {
            "ts_ms": now_ms,
            "top_n": max(1, int(top_n)),
            "source_candidates": len(snapshot_candidates),
            "shortlisted": inserted,
            "symbols": shortlisted_symbols,
            "errors": errors[:10],
        }
        self._coin_shortlist_last_cycle = cycle
        self._coin_shortlist_last_run_ts = time.time()
        return cycle

    async def get_coin_focus_snapshots(
        self,
        symbol: str,
        *,
        exchange: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol is required for focus snapshots.")
        name = normalize_exchange_name(exchange) if exchange else None
        rows = await asyncio.to_thread(
            get_focus_snapshots,
            canonical,
            exchange=name,
            limit=max(1, min(int(limit), 2000)),
        )
        return {
            "symbol": canonical,
            "exchange": name,
            "points": len(rows),
            "rows": rows,
        }

    async def load_focus_history(
        self,
        symbol: str,
        *,
        exchange: str | None = None,
        limit: int = 500,
        since_ts_ms: int | None = None,
        until_ts_ms: int | None = None,
    ) -> dict[str, Any]:
        payload = await self.get_coin_focus_snapshots(
            symbol,
            exchange=exchange,
            limit=max(1, min(int(limit), 5000)),
        )
        rows = list(payload.get("rows") or [])
        if since_ts_ms is not None:
            since_val = int(since_ts_ms)
            rows = [
                row for row in rows if int(_safe_float(row.get("ts_ms")) or 0) >= since_val
            ]
        if until_ts_ms is not None:
            until_val = int(until_ts_ms)
            rows = [
                row for row in rows if int(_safe_float(row.get("ts_ms")) or 0) <= until_val
            ]
        return {
            "symbol": payload.get("symbol"),
            "exchange": payload.get("exchange"),
            "points": len(rows),
            "window": {
                "since_ts_ms": int(since_ts_ms) if since_ts_ms is not None else None,
                "until_ts_ms": int(until_ts_ms) if until_ts_ms is not None else None,
            },
            "rows": rows,
        }

    async def load_bootstrap_history(
        self,
        symbol: str,
        *,
        exchange: str | None = None,
        funding_limit: int = 500,
        oi_limit: int = 500,
        since_ts_ms: int | None = None,
        until_ts_ms: int | None = None,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("symbol is required")
        name = normalize_exchange_name(exchange) if exchange else None
        funding_rows = await asyncio.to_thread(
            get_funding_history,
            canonical,
            exchange=name,
            limit=max(1, min(int(funding_limit), 5000)),
        )
        oi_rows = await asyncio.to_thread(
            get_open_interest_history,
            canonical,
            exchange=name,
            limit=max(1, min(int(oi_limit), 5000)),
        )
        if since_ts_ms is not None:
            since_val = int(since_ts_ms)
            funding_rows = [
                row for row in funding_rows if int(_safe_float(row.get("ts_ms")) or 0) >= since_val
            ]
            oi_rows = [
                row for row in oi_rows if int(_safe_float(row.get("ts_ms")) or 0) >= since_val
            ]
        if until_ts_ms is not None:
            until_val = int(until_ts_ms)
            funding_rows = [
                row for row in funding_rows if int(_safe_float(row.get("ts_ms")) or 0) <= until_val
            ]
            oi_rows = [
                row for row in oi_rows if int(_safe_float(row.get("ts_ms")) or 0) <= until_val
            ]
        funding_sources = sorted(
            {
                str(row.get("source_type") or "").strip()
                for row in funding_rows
                if str(row.get("source_type") or "").strip()
            }
        )
        oi_sources = sorted(
            {
                str(row.get("source_type") or "").strip()
                for row in oi_rows
                if str(row.get("source_type") or "").strip()
            }
        )
        return {
            "symbol": canonical,
            "exchange": name,
            "window": {
                "since_ts_ms": int(since_ts_ms) if since_ts_ms is not None else None,
                "until_ts_ms": int(until_ts_ms) if until_ts_ms is not None else None,
            },
            "funding_history": funding_rows,
            "open_interest_history": oi_rows,
            "counts": {
                "funding_points": len(funding_rows),
                "open_interest_points": len(oi_rows),
            },
            "provenance": {
                "funding_sources": funding_sources,
                "open_interest_sources": oi_sources,
            },
        }

    async def load_symbol_context(
        self,
        symbol: str,
        *,
        focus_limit: int = 500,
        funding_limit: int = 500,
        oi_limit: int = 500,
        decision_limit: int = 500,
        outcome_limit: int = 500,
        real_obs_limit: int = 500,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("symbol is required")
        sessions = await self.list_active_coin_symbol_sessions()
        active_session = next(
            (
                item
                for item in sessions
                if normalize_symbol(str(item.get("canonical_symbol") or "")) == canonical
            ),
            None,
        )
        focus = await self.load_focus_history(canonical, limit=max(1, min(int(focus_limit), 5000)))
        bootstrap = await self.load_bootstrap_history(
            canonical,
            funding_limit=max(1, min(int(funding_limit), 5000)),
            oi_limit=max(1, min(int(oi_limit), 5000)),
        )
        decisions = await asyncio.to_thread(
            get_decisions,
            canonical_symbol=canonical,
            limit=max(1, min(int(decision_limit), 5000)),
        )
        outcomes = await asyncio.to_thread(
            get_outcomes,
            canonical_symbol=canonical,
            limit=max(1, min(int(outcome_limit), 5000)),
        )
        paper = await self.get_coin_paper_positions(symbol=canonical, status="open")
        real_rows = await asyncio.to_thread(
            get_real_position_observations,
            canonical_symbol=canonical,
            limit=max(1, min(int(real_obs_limit), 5000)),
        )
        return {
            "symbol": canonical,
            "active_session": active_session,
            "focus_history": focus,
            "bootstrap_history": bootstrap,
            "decision_journal": {
                "count": len(decisions),
                "rows": decisions,
            },
            "decision_outcomes": {
                "count": len(outcomes),
                "rows": outcomes,
            },
            "paper_positions_open": paper,
            "real_position_observations": {
                "count": len(real_rows),
                "rows": real_rows,
            },
        }

    async def get_coin_paper_positions(
        self,
        *,
        symbol: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol) if symbol else None
        rows = await asyncio.to_thread(get_paper_positions, status=status)
        if canonical:
            rows = [
                row
                for row in rows
                if normalize_symbol(str(row.get("canonical_symbol") or "")) == canonical
            ]
        return {
            "symbol": canonical,
            "status": status,
            "count": len(rows),
            "rows": rows,
        }

    async def get_coin_paper_events(
        self,
        position_key: str,
        *,
        limit: int = 200,
    ) -> dict[str, Any]:
        key = str(position_key or "").strip()
        if not key:
            raise ValueError("position_key is required")
        rows = await asyncio.to_thread(
            get_paper_events,
            key,
            limit=max(1, min(int(limit), 2000)),
        )
        return {
            "position_key": key,
            "count": len(rows),
            "rows": rows,
        }

    async def get_coin_weekly_review(
        self,
        *,
        days: int = 7,
        top: int = 3,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        safe_days = max(1, min(int(days), 30))
        safe_top = max(1, min(int(top), 10))
        now_ms = int(time.time() * 1000)
        since_ts_ms = now_ms - safe_days * 24 * 3600 * 1000
        canonical = normalize_symbol(symbol) if symbol else None

        trade_rows = await asyncio.to_thread(
            get_trade_activity,
            canonical_symbol=canonical,
            since_ts_ms=since_ts_ms,
            limit=5000,
        )
        shortlist_rows = await asyncio.to_thread(
            get_candidate_shortlist,
            canonical_symbol=canonical,
            since_ts_ms=since_ts_ms,
            limit=5000,
        )
        decisions = await asyncio.to_thread(
            get_decisions,
            canonical_symbol=canonical,
            limit=5000,
        )
        decisions = [
            row for row in decisions if int(_safe_float(row.get("ts_ms")) or 0) >= since_ts_ms
        ]
        outcomes = await asyncio.to_thread(
            get_outcomes,
            canonical_symbol=canonical,
            limit=5000,
        )
        outcomes = [
            row
            for row in outcomes
            if int(_safe_float(row.get("evaluated_at_ms")) or 0) >= since_ts_ms
        ]
        paper_rows = await asyncio.to_thread(get_paper_positions, status="open")
        if canonical:
            paper_rows = [
                row
                for row in paper_rows
                if normalize_symbol(str(row.get("canonical_symbol") or "")) == canonical
            ]
        real_rows = await asyncio.to_thread(
            get_real_position_observations,
            canonical_symbol=canonical,
            limit=5000,
        )
        real_rows = [row for row in real_rows if str(row.get("status") or "") == "open"]

        recent_traded_symbols = sorted(
            {
                normalize_symbol(str(row.get("canonical_symbol") or ""))
                for row in trade_rows
                if str(row.get("canonical_symbol") or "").strip()
            }
        )
        shortlist_symbols = sorted(
            {
                normalize_symbol(str(row.get("canonical_symbol") or ""))
                for row in shortlist_rows
                if str(row.get("canonical_symbol") or "").strip()
            }
        )
        latest_shortlist_by_symbol: dict[str, dict[str, Any]] = {}
        for row in shortlist_rows:
            sym = normalize_symbol(str(row.get("canonical_symbol") or ""))
            if not sym or sym in latest_shortlist_by_symbol:
                continue
            latest_shortlist_by_symbol[sym] = row
        top_candidate_symbols = [
            sym
            for sym, _row in sorted(
                latest_shortlist_by_symbol.items(),
                key=lambda item: (
                    -int(_safe_float(item[1].get("ts_ms")) or 0),
                    int(_safe_float(item[1].get("rank")) or 999),
                ),
            )[:safe_top]
        ]
        activity_by_type: dict[str, int] = {}
        for row in trade_rows:
            key_name = str(row.get("activity_type") or "unknown")
            activity_by_type[key_name] = int(activity_by_type.get(key_name, 0) + 1)

        trade_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for row in trade_rows:
            sym = normalize_symbol(str(row.get("canonical_symbol") or ""))
            if not sym:
                continue
            trade_by_symbol.setdefault(sym, []).append(row)

        review_tags: list[dict[str, Any]] = []
        seen_missed_entries: set[tuple[str, int, int]] = set()
        for row in shortlist_rows:
            sym = normalize_symbol(str(row.get("canonical_symbol") or ""))
            rank = int(_safe_float(row.get("rank")) or 0)
            candidate_score = _safe_float(row.get("candidate_score"))
            shortlist_ts_ms = int(_safe_float(row.get("ts_ms")) or 0)
            if not sym or shortlist_ts_ms <= 0:
                continue
            if rank <= 0 or rank > safe_top:
                continue
            if candidate_score is None or candidate_score < float(COIN_REVIEW_MISSED_ENTRY_SCORE_MIN):
                continue
            nearby_trade = False
            for trade in trade_by_symbol.get(sym, []):
                trade_ts_ms = int(_safe_float(trade.get("ts_ms")) or 0)
                if abs(trade_ts_ms - shortlist_ts_ms) <= COIN_REVIEW_MISSED_ENTRY_LOOKAHEAD_MS:
                    nearby_trade = True
                    break
            if nearby_trade:
                continue
            dedupe_key = (sym, shortlist_ts_ms, rank)
            if dedupe_key in seen_missed_entries:
                continue
            seen_missed_entries.add(dedupe_key)
            funding_edge_pct = _safe_float(row.get("funding_edge_pct")) or 0.0
            entry_spread_pct = _safe_float(row.get("entry_spread_pct")) or 0.0
            impact_score = round(
                float(candidate_score)
                + max(0.0, 8.0 - float(rank) * 2.0)
                + min(12.0, abs(float(entry_spread_pct)) * 20.0)
                + min(10.0, abs(float(funding_edge_pct)) * 100.0),
                2,
            )
            review_tags.append(
                {
                    "tag": "missed_entry",
                    "symbol": sym,
                    "ts_ms": shortlist_ts_ms,
                    "severity": "warn" if impact_score >= 75.0 else "info",
                    "impact_score": impact_score,
                    "rank": rank,
                    "candidate_score": candidate_score,
                    "pair_key": row.get("pair_key"),
                    "reason": "strong_shortlist_without_trade",
                    "payload": {
                        "funding_edge_pct": funding_edge_pct,
                        "entry_spread_pct": entry_spread_pct,
                        "reason_codes": list(row.get("reason_codes") or []),
                    },
                }
            )

        for row in outcomes:
            outcome = dict(row.get("outcome") or {})
            action = str(row.get("action") or outcome.get("decision_action") or "").upper()
            correctness = str(outcome.get("decision_correctness") or "").strip().lower()
            net_delta_pct = _safe_float(outcome.get("net_pnl_delta_pct"))
            timing_quality = str(outcome.get("timing_quality") or "")
            net_alt = _safe_float(outcome.get("net_pnl_delta_vs_alternative"))

            if action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL"}:
                is_bad_entry = False
                if correctness == "incorrect":
                    is_bad_entry = True
                elif net_delta_pct is not None and net_delta_pct <= float(COIN_REVIEW_BAD_ENTRY_NET_DELTA_MAX):
                    is_bad_entry = True
                if is_bad_entry:
                    impact_score = round(
                        60.0
                        + min(25.0, abs(float(net_delta_pct or 0.0)) * 100.0)
                        + (10.0 if correctness == "incorrect" else 0.0)
                        + (8.0 if timing_quality == "poor" else 0.0),
                        2,
                    )
                    review_tags.append(
                        {
                            "tag": "bad_entry",
                            "symbol": normalize_symbol(str(row.get("canonical_symbol") or "")),
                            "ts_ms": int(_safe_float(row.get("evaluated_at_ms")) or 0),
                            "severity": "high" if impact_score >= 85.0 else "warn",
                            "impact_score": impact_score,
                            "decision_id": row.get("decision_id"),
                            "action": action,
                            "horizon": row.get("horizon"),
                            "phase": outcome.get("decision_phase"),
                            "reason": "entry_underperformed",
                            "payload": {
                                "decision_correctness": correctness,
                                "timing_quality": timing_quality,
                                "net_pnl_delta_pct": net_delta_pct,
                                "net_pnl_delta_vs_alternative": outcome.get("net_pnl_delta_vs_alternative"),
                            },
                        }
                    )

            if action in {"PARTIAL_EXIT", "FULL_EXIT"}:
                is_good_exit = (
                    correctness == "correct"
                    and net_alt is not None
                    and net_alt <= float(COIN_REVIEW_GOOD_EXIT_ALT_DELTA_MAX)
                )
                if is_good_exit:
                    impact_score = round(
                        55.0
                        + min(22.0, abs(float(net_alt or 0.0)) * 100.0)
                        + (8.0 if timing_quality == "good" else 0.0)
                        + (4.0 if action == "FULL_EXIT" else 0.0),
                        2,
                    )
                    review_tags.append(
                        {
                            "tag": "good_exit",
                            "symbol": normalize_symbol(str(row.get("canonical_symbol") or "")),
                            "ts_ms": int(_safe_float(row.get("evaluated_at_ms")) or 0),
                            "severity": "info" if impact_score < 70.0 else "warn",
                            "impact_score": impact_score,
                            "decision_id": row.get("decision_id"),
                            "action": action,
                            "horizon": row.get("horizon"),
                            "phase": outcome.get("decision_phase"),
                            "reason": "timely_exit_confirmed",
                            "payload": {
                                "decision_correctness": correctness,
                                "timing_quality": timing_quality,
                                "net_pnl_delta_pct": net_delta_pct,
                                "net_pnl_delta_vs_alternative": net_alt,
                            },
                        }
                    )

            if action in {"NO_TRADE", "ADD_BLOCKED"}:
                is_good_no_trade = (
                    correctness == "correct"
                    and net_delta_pct is not None
                    and net_delta_pct <= float(COIN_REVIEW_GOOD_NO_TRADE_NET_DELTA_MAX)
                )
                if is_good_no_trade:
                    impact_score = round(
                        50.0
                        + min(20.0, abs(float(net_delta_pct or 0.0)) * 100.0)
                        + (6.0 if timing_quality == "good" else 0.0),
                        2,
                    )
                    review_tags.append(
                        {
                            "tag": "good_no_trade",
                            "symbol": normalize_symbol(str(row.get("canonical_symbol") or "")),
                            "ts_ms": int(_safe_float(row.get("evaluated_at_ms")) or 0),
                            "severity": "info" if impact_score < 65.0 else "warn",
                            "impact_score": impact_score,
                            "decision_id": row.get("decision_id"),
                            "action": action,
                            "horizon": row.get("horizon"),
                            "phase": outcome.get("decision_phase"),
                            "reason": "avoided_bad_entry",
                            "payload": {
                                "decision_correctness": correctness,
                                "timing_quality": timing_quality,
                                "net_pnl_delta_pct": net_delta_pct,
                            },
                        }
                    )

            if action == "HOLD":
                is_bad_hold = False
                if correctness == "incorrect":
                    is_bad_hold = True
                elif net_delta_pct is not None and net_delta_pct <= float(COIN_REVIEW_BAD_HOLD_NET_DELTA_MAX):
                    is_bad_hold = True
                if is_bad_hold and not bool(outcome.get("would_exiting_15m_earlier_help")):
                    impact_score = round(
                        58.0
                        + min(25.0, abs(float(net_delta_pct or 0.0)) * 100.0)
                        + (8.0 if timing_quality == "poor" else 0.0),
                        2,
                    )
                    review_tags.append(
                        {
                            "tag": "bad_hold",
                            "symbol": normalize_symbol(str(row.get("canonical_symbol") or "")),
                            "ts_ms": int(_safe_float(row.get("evaluated_at_ms")) or 0),
                            "severity": "warn" if impact_score < 85.0 else "high",
                            "impact_score": impact_score,
                            "decision_id": row.get("decision_id"),
                            "action": action,
                            "horizon": row.get("horizon"),
                            "phase": outcome.get("decision_phase"),
                            "reason": "holding_underperformed",
                            "payload": {
                                "decision_correctness": correctness,
                                "timing_quality": timing_quality,
                                "net_pnl_delta_pct": net_delta_pct,
                                "net_pnl_delta_vs_alternative": net_alt,
                            },
                        }
                    )

            if action not in {"HOLD", "ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL"}:
                continue
            if not bool(outcome.get("would_exiting_15m_earlier_help")):
                continue
            if net_alt is None or net_alt > float(COIN_REVIEW_LATE_EXIT_NET_DELTA_MIN):
                continue
            impact_score = round(
                70.0
                + min(30.0, abs(float(net_alt)) * 100.0)
                + (10.0 if str(outcome.get("timing_quality") or "") == "poor" else 0.0),
                2,
            )
            review_tags.append(
                {
                    "tag": "late_exit",
                    "symbol": normalize_symbol(str(row.get("canonical_symbol") or "")),
                    "ts_ms": int(_safe_float(row.get("evaluated_at_ms")) or 0),
                    "severity": "high" if impact_score >= 90.0 else "warn",
                    "impact_score": impact_score,
                    "decision_id": row.get("decision_id"),
                    "action": action,
                    "horizon": row.get("horizon"),
                    "phase": outcome.get("decision_phase"),
                    "reason": "earlier_exit_would_help",
                    "payload": {
                        "timing_quality": outcome.get("timing_quality"),
                        "net_pnl_delta_vs_alternative": net_alt,
                        "net_pnl_delta_pct": outcome.get("net_pnl_delta_pct"),
                    },
                }
            )

        trade_by_state: dict[str, list[dict[str, Any]]] = {}
        for row in trade_rows:
            state_ref = str(row.get("state_ref") or "").strip()
            if not state_ref:
                continue
            trade_by_state.setdefault(state_ref, []).append(row)
        for row in paper_rows:
            position_key = str(row.get("position_key") or "").strip()
            opened_at_ms = int(_safe_float(row.get("opened_at_ms")) or 0)
            if not position_key or opened_at_ms <= 0:
                continue
            if (now_ms - opened_at_ms) < int(COIN_REVIEW_STALE_POSITION_AGE_MS):
                continue
            state_trades = list(trade_by_state.get(position_key) or [])
            meaningful_followup = False
            for trade in state_trades:
                trade_type = str(trade.get("activity_type") or "")
                trade_ts_ms = int(_safe_float(trade.get("ts_ms")) or 0)
                if trade_type != "paper_enter" and trade_ts_ms > opened_at_ms:
                    meaningful_followup = True
                    break
            if meaningful_followup:
                continue
            age_hours = round((now_ms - opened_at_ms) / 3_600_000.0, 2)
            impact_score = round(
                40.0 + min(35.0, max(0.0, float(age_hours) - 24.0) * 1.5),
                2,
            )
            review_tags.append(
                {
                    "tag": "stale_position",
                    "symbol": normalize_symbol(str(row.get("canonical_symbol") or "")),
                    "ts_ms": now_ms,
                    "severity": "warn" if impact_score >= 50.0 else "info",
                    "impact_score": impact_score,
                    "state_ref": position_key,
                    "pair_key": row.get("pair_key"),
                    "reason": "open_paper_position_no_followup_actions",
                    "payload": {
                        "opened_at_ms": opened_at_ms,
                        "age_hours": age_hours,
                        "qty": row.get("qty"),
                    },
                }
            )

        tag_counts: dict[str, int] = {}
        severity_counts: dict[str, int] = {}
        for row in review_tags:
            key_name = str(row.get("tag") or "unknown")
            tag_counts[key_name] = int(tag_counts.get(key_name, 0) + 1)
            severity = str(row.get("severity") or "unknown")
            severity_counts[severity] = int(severity_counts.get(severity, 0) + 1)
        review_tags.sort(
            key=lambda item: (
                -float(_safe_float(item.get("impact_score")) or 0.0),
                -int(_safe_float(item.get("ts_ms")) or 0),
                str(item.get("tag") or ""),
            )
        )
        entry_review_tags = [
            item for item in review_tags if str(item.get("tag") or "") in {"missed_entry", "bad_entry", "good_no_trade"}
        ]
        exit_review_tags = [
            item for item in review_tags if str(item.get("tag") or "") in {"late_exit", "stale_position", "good_exit", "bad_hold"}
        ]

        def _top_review_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {
                    "tag": item.get("tag"),
                    "symbol": item.get("symbol"),
                    "severity": item.get("severity"),
                    "impact_score": item.get("impact_score"),
                    "reason": item.get("reason"),
                    "ts_ms": item.get("ts_ms"),
                    "ref": item.get("state_ref") or item.get("decision_id") or item.get("pair_key"),
                }
                for item in rows[: int(COIN_REVIEW_TOP_ITEMS_LIMIT)]
            ]

        top_review_items = [
            {
                "tag": item.get("tag"),
                "symbol": item.get("symbol"),
                "severity": item.get("severity"),
                "impact_score": item.get("impact_score"),
                "reason": item.get("reason"),
                "ts_ms": item.get("ts_ms"),
                "ref": item.get("state_ref") or item.get("decision_id") or item.get("pair_key"),
            }
            for item in review_tags[: int(COIN_REVIEW_TOP_ITEMS_LIMIT)]
        ]

        entry_action_set = {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "NO_TRADE", "ADD_BLOCKED"}
        exit_action_set = {"HOLD", "PARTIAL_EXIT", "FULL_EXIT"}
        entry_action_scorecards: dict[str, dict[str, Any]] = {}
        exit_action_scorecards: dict[str, dict[str, Any]] = {}
        phase_scorecards: dict[str, dict[str, Any]] = {}
        for row in outcomes:
            outcome = dict(row.get("outcome") or {})
            action = str(row.get("action") or outcome.get("decision_action") or "").upper()
            phase_bucket = _outcome_phase_bucket(outcome.get("decision_phase") or row.get("decision_phase"))
            if action in entry_action_set:
                bucket = entry_action_scorecards.setdefault(action, _new_review_score_bucket())
                _apply_review_score_row(bucket, row)
            if action in exit_action_set:
                bucket = exit_action_scorecards.setdefault(action, _new_review_score_bucket())
                _apply_review_score_row(bucket, row)
            phase_bucket_row = phase_scorecards.setdefault(phase_bucket, _new_review_score_bucket())
            _apply_review_score_row(phase_bucket_row, row)
        for name in list(entry_action_scorecards.keys()):
            entry_action_scorecards[name] = _finalize_review_score_bucket(entry_action_scorecards[name])
        for name in list(exit_action_scorecards.keys()):
            exit_action_scorecards[name] = _finalize_review_score_bucket(exit_action_scorecards[name])
        for name in list(phase_scorecards.keys()):
            phase_scorecards[name] = _finalize_review_score_bucket(phase_scorecards[name])

        return {
            "schema_version": "coin_review_v1",
            "scope": {
                "symbol": canonical,
                "days": safe_days,
                "top": safe_top,
                "since_ts_ms": since_ts_ms,
                "until_ts_ms": now_ms,
            },
            "recent_trade_activity": trade_rows,
            "recent_traded_symbols": recent_traded_symbols,
            "shortlist_history": shortlist_rows,
            "shortlist_symbols": shortlist_symbols,
            "top_candidate_symbols": top_candidate_symbols,
            "position_symbols": {
                "paper_open": sorted(
                    {
                        normalize_symbol(str(row.get("canonical_symbol") or ""))
                        for row in paper_rows
                        if str(row.get("canonical_symbol") or "").strip()
                    }
                ),
                "real_manual_open": sorted(
                    {
                        normalize_symbol(str(row.get("canonical_symbol") or ""))
                        for row in real_rows
                        if str(row.get("canonical_symbol") or "").strip()
                    }
                ),
            },
            "paper_positions": paper_rows,
            "real_position_observations": real_rows,
            "decision_journal": decisions,
            "decision_outcomes": outcomes,
            "review_tags": review_tags,
            "entry_review": {
                "tags": entry_review_tags,
                "summary": {
                    "total": len(entry_review_tags),
                    "tag_counts": {
                        "missed_entry": len(
                            [item for item in entry_review_tags if str(item.get("tag") or "") == "missed_entry"]
                        ),
                        "bad_entry": len(
                            [item for item in entry_review_tags if str(item.get("tag") or "") == "bad_entry"]
                        ),
                        "good_no_trade": len(
                            [item for item in entry_review_tags if str(item.get("tag") or "") == "good_no_trade"]
                        ),
                    },
                    "top_items": _top_review_items(entry_review_tags),
                    "action_scorecards": entry_action_scorecards,
                },
            },
            "exit_review": {
                "tags": exit_review_tags,
                "summary": {
                    "total": len(exit_review_tags),
                    "tag_counts": {
                        "late_exit": len(
                            [item for item in exit_review_tags if str(item.get("tag") or "") == "late_exit"]
                        ),
                        "stale_position": len(
                            [item for item in exit_review_tags if str(item.get("tag") or "") == "stale_position"]
                        ),
                        "good_exit": len(
                            [item for item in exit_review_tags if str(item.get("tag") or "") == "good_exit"]
                        ),
                        "bad_hold": len(
                            [item for item in exit_review_tags if str(item.get("tag") or "") == "bad_hold"]
                        ),
                    },
                    "top_items": _top_review_items(exit_review_tags),
                    "action_scorecards": exit_action_scorecards,
                },
            },
            "summary": {
                "trade_activity_total": len(trade_rows),
                "trade_activity_by_type": activity_by_type,
                "symbols_traded_count": len(recent_traded_symbols),
                "symbols_shortlisted_count": len(shortlist_symbols),
                "paper_open_positions": len(paper_rows),
                "real_manual_open_positions": len(real_rows),
                "decisions_total": len(decisions),
                "outcomes_total": len(outcomes),
                "review_tag_counts": tag_counts,
                "review_tag_severity_counts": severity_counts,
                "top_review_items": top_review_items,
                "phase_scorecards": phase_scorecards,
            },
        }

    async def export_coin_review_json(
        self,
        *,
        symbol: str | None = None,
        days: int = 7,
        top: int = 3,
        include_live_analysis: bool = False,
    ) -> dict[str, Any]:
        review_payload = await self.get_coin_weekly_review(days=days, top=top, symbol=symbol)
        canonical = normalize_symbol(symbol) if symbol else None
        symbol_export = None
        if canonical:
            symbol_export = await self.export_coin_analysis_json(
                canonical,
                include_live_analysis=include_live_analysis,
            )
        return {
            "schema_version": "coin_review_v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "review": review_payload,
            "symbol_export": symbol_export,
        }

    async def export_coin_review_csv(
        self,
        *,
        symbol: str | None = None,
        days: int = 7,
        top: int = 3,
    ) -> str:
        review_payload = await self.get_coin_weekly_review(days=days, top=top, symbol=symbol)
        rows: list[dict[str, Any]] = []
        for item in list(review_payload.get("recent_trade_activity") or []):
            rows.append(
                {
                    "record_type": "trade_activity",
                    "symbol": item.get("canonical_symbol"),
                    "ts_ms": item.get("ts_ms"),
                    "pair_key": item.get("pair_key"),
                    "direction": item.get("direction"),
                    "activity_type": item.get("activity_type"),
                    "source": item.get("source"),
                    "state_ref": item.get("state_ref"),
                    "rank": None,
                    "candidate_score": None,
                    "reason_codes": None,
                    "payload_json": json.dumps(item.get("payload") or {}, ensure_ascii=True, sort_keys=True),
                }
            )
        for item in list(review_payload.get("shortlist_history") or []):
            rows.append(
                {
                    "record_type": "shortlist_candidate",
                    "symbol": item.get("canonical_symbol"),
                    "ts_ms": item.get("ts_ms"),
                    "pair_key": item.get("pair_key"),
                    "direction": item.get("direction_hint"),
                    "activity_type": None,
                    "source": item.get("source_name"),
                    "state_ref": None,
                    "rank": item.get("rank"),
                    "candidate_score": item.get("candidate_score"),
                    "reason_codes": json.dumps(item.get("reason_codes") or [], ensure_ascii=True, sort_keys=True),
                    "payload_json": json.dumps(item.get("payload") or {}, ensure_ascii=True, sort_keys=True),
                }
            )
        for item in list(review_payload.get("review_tags") or []):
            rows.append(
                {
                    "record_type": "review_tag",
                    "symbol": item.get("symbol"),
                    "ts_ms": item.get("ts_ms"),
                    "pair_key": item.get("pair_key"),
                    "direction": item.get("direction"),
                    "activity_type": item.get("tag"),
                    "source": item.get("reason"),
                    "state_ref": item.get("state_ref") or item.get("decision_id"),
                    "rank": item.get("rank"),
                    "candidate_score": item.get("impact_score"),
                    "reason_codes": json.dumps([item.get("severity")] if item.get("severity") else [], ensure_ascii=True, sort_keys=True),
                    "payload_json": json.dumps(item.get("payload") or {}, ensure_ascii=True, sort_keys=True),
                }
            )

        output = io.StringIO()
        fieldnames = [
            "record_type",
            "symbol",
            "ts_ms",
            "pair_key",
            "direction",
            "activity_type",
            "source",
            "state_ref",
            "rank",
            "candidate_score",
            "reason_codes",
            "payload_json",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(rows, key=lambda item: int(_safe_float(item.get("ts_ms")) or 0), reverse=True):
            writer.writerow(row)
        return output.getvalue()

    async def export_coin_analysis_json(
        self,
        symbol: str,
        *,
        include_live_analysis: bool = True,
        window_minutes: int = 240,
        funding_points: int = 96,
        focus_limit: int = 1000,
        funding_limit: int = 1000,
        oi_limit: int = 1000,
        feature_limit: int = 1000,
        decision_limit: int = 1000,
        outcome_limit: int = 1000,
        paper_event_limit: int = 500,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("symbol is required")

        analysis = None
        if include_live_analysis:
            analysis = await self.analyze_symbol(
                canonical,
                window_minutes=max(60, min(int(window_minutes), 4320)),
                funding_points=max(24, min(int(funding_points), 200)),
            )

        focus_rows = await asyncio.to_thread(
            get_focus_snapshots,
            canonical,
            limit=max(1, min(int(focus_limit), 5000)),
        )
        funding_rows = await asyncio.to_thread(
            get_funding_history,
            canonical,
            limit=max(1, min(int(funding_limit), 5000)),
        )
        oi_rows = await asyncio.to_thread(
            get_open_interest_history,
            canonical,
            limit=max(1, min(int(oi_limit), 5000)),
        )
        feature_rows = await asyncio.to_thread(
            get_feature_snapshots,
            canonical_symbol=canonical,
            limit=max(1, min(int(feature_limit), 5000)),
        )
        decision_rows = await asyncio.to_thread(
            get_decisions,
            canonical_symbol=canonical,
            limit=max(1, min(int(decision_limit), 5000)),
        )
        outcome_rows = await asyncio.to_thread(
            get_outcomes,
            canonical_symbol=canonical,
            limit=max(1, min(int(outcome_limit), 5000)),
        )
        paper_rows = await asyncio.to_thread(get_paper_positions, status=None)
        paper_rows = [
            row
            for row in paper_rows
            if normalize_symbol(str(row.get("canonical_symbol") or "")) == canonical
        ]
        paper_events: dict[str, list[dict[str, Any]]] = {}
        for row in paper_rows:
            key = str(row.get("position_key") or "")
            if not key:
                continue
            events = await asyncio.to_thread(
                get_paper_events,
                key,
                limit=max(1, min(int(paper_event_limit), 5000)),
            )
            paper_events[key] = events

        recommended_action = None
        reason_codes: list[str] = []
        reason_text: list[str] = []
        if isinstance(analysis, Mapping):
            bot_logic = analysis.get("bot_logic") or {}
            recommended_action = bot_logic.get("recommended_action")
            reason_codes = list(bot_logic.get("reason_codes") or bot_logic.get("pair_reasons") or [])
            reason_text = list(bot_logic.get("reason_text") or [])
        if recommended_action is None and decision_rows:
            recommended_action = decision_rows[0].get("action")
        if not reason_codes and decision_rows:
            reason_codes = list(decision_rows[0].get("reason_codes") or [])
        if not reason_text and decision_rows:
            reason_text = list(decision_rows[0].get("reason_text") or [])

        return {
            "schema_version": "coin_export_v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "symbol": canonical,
            "recommended_action": recommended_action,
            "reason_list": {
                "reason_codes": reason_codes,
                "reason_text": reason_text,
            },
            "analysis": analysis,
            "raw_market_data": {
                "focus_snapshots": focus_rows,
                "funding_history": funding_rows,
                "open_interest_history": oi_rows,
            },
            "derived_features": feature_rows,
            "scores": [
                {
                    "decision_id": row.get("decision_id"),
                    "mode": row.get("mode"),
                    "action": row.get("action"),
                    "confidence_score": row.get("confidence_score"),
                    "scores": row.get("scores"),
                }
                for row in decision_rows
            ],
            "decision_journal": decision_rows,
            "decision_outcomes": outcome_rows,
            "decision_outcome_summary": _build_outcomes_summary(outcome_rows),
            "paper": {
                "positions": paper_rows,
                "events_by_position": paper_events,
            },
        }

    def _build_coin_timeline_rows(
        self,
        export_payload: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        symbol = str(export_payload.get("symbol") or "")
        raw_market_data = export_payload.get("raw_market_data") or {}
        focus_rows = list(raw_market_data.get("focus_snapshots") or [])
        for item in focus_rows:
            rows.append(
                {
                    "ts_ms": item.get("ts_ms"),
                    "record_type": "focus_snapshot",
                    "symbol": symbol,
                    "exchange": item.get("exchange"),
                    "pair_key": "",
                    "position_key": "",
                    "direction": "",
                    "action": "",
                    "status": "",
                    "value_1": item.get("mid"),
                    "value_2": item.get("mark_price"),
                    "value_3": item.get("funding_rate"),
                    "payload_json": json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                }
            )
        funding_rows = list(raw_market_data.get("funding_history") or [])
        for item in funding_rows:
            rows.append(
                {
                    "ts_ms": item.get("ts_ms"),
                    "record_type": "funding_history",
                    "symbol": symbol,
                    "exchange": item.get("exchange"),
                    "pair_key": "",
                    "position_key": "",
                    "direction": "",
                    "action": "",
                    "status": "",
                    "value_1": item.get("funding_rate"),
                    "value_2": item.get("predicted_funding_rate"),
                    "value_3": item.get("interval_hours"),
                    "payload_json": json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                }
            )
        oi_rows = list(raw_market_data.get("open_interest_history") or [])
        for item in oi_rows:
            rows.append(
                {
                    "ts_ms": item.get("ts_ms"),
                    "record_type": "oi_history",
                    "symbol": symbol,
                    "exchange": item.get("exchange"),
                    "pair_key": "",
                    "position_key": "",
                    "direction": "",
                    "action": "",
                    "status": "",
                    "value_1": item.get("oi_contracts"),
                    "value_2": item.get("oi_notional"),
                    "value_3": "",
                    "payload_json": json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                }
            )

        for item in list(export_payload.get("derived_features") or []):
            rows.append(
                {
                    "ts_ms": item.get("ts_ms"),
                    "record_type": "feature_snapshot",
                    "symbol": symbol,
                    "exchange": "",
                    "pair_key": item.get("pair_key"),
                    "position_key": "",
                    "direction": item.get("direction"),
                    "action": "",
                    "status": "",
                    "value_1": ((item.get("features") or {}).get("scores") or {}).get("entry_score"),
                    "value_2": ((item.get("features") or {}).get("scores") or {}).get("continuation_risk_score"),
                    "value_3": "",
                    "payload_json": json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                }
            )
        for item in list(export_payload.get("decision_journal") or []):
            rows.append(
                {
                    "ts_ms": item.get("ts_ms"),
                    "record_type": "decision",
                    "symbol": symbol,
                    "exchange": "",
                    "pair_key": item.get("pair_key"),
                    "position_key": item.get("state_ref") or "",
                    "direction": item.get("direction"),
                    "action": item.get("action"),
                    "status": item.get("mode"),
                    "value_1": item.get("confidence_score"),
                    "value_2": "",
                    "value_3": "",
                    "payload_json": json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                }
            )
        for item in list(export_payload.get("decision_outcomes") or []):
            outcome = item.get("outcome") or {}
            rows.append(
                {
                    "ts_ms": item.get("evaluated_at_ms"),
                    "record_type": "decision_outcome",
                    "symbol": symbol,
                    "exchange": "",
                    "pair_key": item.get("pair_key"),
                    "position_key": "",
                    "direction": item.get("direction"),
                    "action": item.get("action"),
                    "status": outcome.get("decision_correctness"),
                    "value_1": outcome.get("spread_delta_pct"),
                    "value_2": outcome.get("funding_to_next_pct"),
                    "value_3": outcome.get("net_pnl_delta_vs_alternative"),
                    "payload_json": json.dumps(item, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                }
            )
        paper = export_payload.get("paper") or {}
        for pos in list(paper.get("positions") or []):
            rows.append(
                {
                    "ts_ms": pos.get("updated_at_ms") or pos.get("opened_at_ms"),
                    "record_type": "paper_position",
                    "symbol": symbol,
                    "exchange": "",
                    "pair_key": pos.get("pair_key"),
                    "position_key": pos.get("position_key"),
                    "direction": pos.get("direction"),
                    "action": "",
                    "status": pos.get("status"),
                    "value_1": pos.get("qty"),
                    "value_2": "",
                    "value_3": "",
                    "payload_json": json.dumps(pos, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                }
            )
        for position_key, events in dict(paper.get("events_by_position") or {}).items():
            for event in list(events or []):
                rows.append(
                    {
                        "ts_ms": event.get("ts_ms"),
                        "record_type": "paper_event",
                        "symbol": symbol,
                        "exchange": "",
                        "pair_key": "",
                        "position_key": position_key,
                        "direction": "",
                        "action": event.get("event_type"),
                        "status": "",
                        "value_1": "",
                        "value_2": "",
                        "value_3": "",
                        "payload_json": json.dumps(event, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                    }
                )
        rows.sort(
            key=lambda row: int(_safe_float(row.get("ts_ms")) or 0),
            reverse=True,
        )
        return rows

    async def export_coin_timeline_csv(
        self,
        symbol: str,
        *,
        include_live_analysis: bool = True,
        window_minutes: int = 240,
        funding_points: int = 96,
    ) -> str:
        payload = await self.export_coin_analysis_json(
            symbol,
            include_live_analysis=include_live_analysis,
            window_minutes=window_minutes,
            funding_points=funding_points,
        )
        rows = self._build_coin_timeline_rows(payload)
        columns = [
            "ts_ms",
            "record_type",
            "symbol",
            "exchange",
            "pair_key",
            "position_key",
            "direction",
            "action",
            "status",
            "value_1",
            "value_2",
            "value_3",
            "payload_json",
        ]
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in columns})
        return buffer.getvalue()

    async def export_coin_timeline_parquet(
        self,
        symbol: str,
        *,
        include_live_analysis: bool = True,
        window_minutes: int = 240,
        funding_points: int = 96,
    ) -> bytes:
        payload = await self.export_coin_analysis_json(
            symbol,
            include_live_analysis=include_live_analysis,
            window_minutes=window_minutes,
            funding_points=funding_points,
        )
        rows = self._build_coin_timeline_rows(payload)
        try:
            import pandas as pd  # type: ignore
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError("parquet export requires pandas + pyarrow") from exc
        try:
            frame = pd.DataFrame(rows)
            out = io.BytesIO()
            frame.to_parquet(out, index=False, engine="pyarrow")
            return out.getvalue()
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError("failed to generate parquet timeline") from exc

    async def replay_coin_candidate_signals(
        self,
        symbol: str,
        *,
        limit: int = 1000,
        since_ts_ms: int | None = None,
        until_ts_ms: int | None = None,
        include_stored_decisions: bool = True,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("symbol is required")
        safe_limit = max(1, min(int(limit), 5000))
        feature_rows = await asyncio.to_thread(
            get_feature_snapshots,
            canonical_symbol=canonical,
            since_ts_ms=since_ts_ms,
            until_ts_ms=until_ts_ms,
            limit=safe_limit,
        )
        if not feature_rows:
            return {
                "symbol": canonical,
                "replay_points": 0,
                "timeline": [],
                "summary": {
                    "actions": {},
                    "decision_states": {},
                },
            }

        decisions_by_feature: dict[str, dict[str, Any]] = {}
        if include_stored_decisions:
            decision_rows = await asyncio.to_thread(
                get_decisions,
                canonical_symbol=canonical,
                mode="manual_candidate",
                limit=safe_limit,
            )
            for item in decision_rows:
                feature_ref = str(item.get("features_ref") or "").strip()
                if feature_ref:
                    decisions_by_feature[feature_ref] = item

        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in feature_rows:
            ts_ms = int(_safe_float(row.get("ts_ms")) or 0)
            if ts_ms <= 0:
                continue
            grouped.setdefault(ts_ms, []).append(row)

        timeline: list[dict[str, Any]] = []
        for ts_ms in sorted(grouped.keys(), reverse=True):
            rows = grouped[ts_ms]
            candidates: list[dict[str, Any]] = []
            feature_ids: list[int] = []
            for row in rows:
                features = row.get("features") or {}
                common = features.get("common") or {}
                scores = features.get("scores") or {}
                reasons = list(features.get("reasons") or [])
                funding = common.get("funding") or {}
                left_interval = _safe_float(funding.get("left_interval_hours"))
                right_interval = _safe_float(funding.get("right_interval_hours"))
                interval_match = (
                    left_interval is not None
                    and right_interval is not None
                    and abs(left_interval - right_interval) <= 0.05
                )
                if left_interval is None or right_interval is None:
                    interval_match = True
                entry_score = _safe_float(scores.get("entry_score"))
                direction = str(row.get("direction") or "long_a_short_b")
                pair_key = str(row.get("pair_key") or "")
                left_exchange = str(common.get("left_exchange") or "")
                right_exchange = str(common.get("right_exchange") or "")
                coverage_pct = _safe_float((row.get("data_quality") or {}).get("coverage_pct"))
                if coverage_pct is None:
                    coverage_pct = 100.0
                feature_id = int(_safe_float(row.get("id")) or 0)
                if feature_id > 0:
                    feature_ids.append(feature_id)
                candidates.append(
                    {
                        "pair_key": pair_key,
                        "left_exchange": left_exchange,
                        "right_exchange": right_exchange,
                        "selected_direction": direction,
                        "selected_action": action_from_entry_score(entry_score),
                        "score": entry_score if entry_score is not None else 0.0,
                        "decision_phase": common.get("decision_phase") or "exploratory",
                        "spread": {"coverage_pct": coverage_pct},
                        "funding_interval_hours": {"match": interval_match},
                        "reasons": reasons,
                        "feature_snapshot_ids": {direction: feature_id},
                    }
                )
            if not candidates:
                continue
            recomputed = evaluate_candidate_pairs(candidates)
            recommended_pair = recomputed.get("recommended_pair") or {}
            feature_ref = str(recommended_pair.get("feature_snapshot_id") or "")
            stored = decisions_by_feature.get(feature_ref) if feature_ref else None
            timeline.append(
                {
                    "ts_ms": ts_ms,
                    "feature_snapshot_ids": sorted(feature_ids, reverse=True),
                    "recomputed": {
                        "decision": recomputed.get("decision"),
                        "recommended_action": recomputed.get("recommended_action"),
                        "score": recomputed.get("score"),
                        "pair": recommended_pair,
                        "reason_codes": list(recomputed.get("reason_codes") or []),
                    },
                    "stored": (
                        {
                            "decision_id": stored.get("decision_id"),
                            "action": stored.get("action"),
                            "decision_phase": stored.get("decision_phase"),
                            "score": stored.get("confidence_score"),
                            "reason_codes": list(stored.get("reason_codes") or []),
                        }
                        if stored
                        else None
                    ),
                }
            )

        action_stats: dict[str, int] = {}
        decision_stats: dict[str, int] = {}
        for row in timeline:
            action = str((row.get("recomputed") or {}).get("recommended_action") or "NO_TRADE")
            action_stats[action] = action_stats.get(action, 0) + 1
            decision = str((row.get("recomputed") or {}).get("decision") or "reject")
            decision_stats[decision] = decision_stats.get(decision, 0) + 1

        return {
            "symbol": canonical,
            "replay_points": len(timeline),
            "timeline": timeline,
            "summary": {
                "actions": action_stats,
                "decision_states": decision_stats,
            },
        }

    async def get_coin_outcomes(
        self,
        symbol: str,
        *,
        limit: int = 500,
        horizons: list[str] | None = None,
        phase_buckets: list[str] | None = None,
        actions: list[str] | None = None,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("symbol is required")
        rows = await asyncio.to_thread(
            get_outcomes,
            canonical_symbol=canonical,
            limit=max(1, min(int(limit), 5000)),
        )
        horizon_filters = {
            str(item or "").strip().lower()
            for item in list(horizons or [])
            if str(item or "").strip()
        }
        phase_filters = {
            _outcome_phase_bucket(item)
            for item in list(phase_buckets or [])
            if str(item or "").strip()
        }
        action_filters = {
            str(item or "").strip().upper()
            for item in list(actions or [])
            if str(item or "").strip()
        }
        if horizon_filters or phase_filters or action_filters:
            filtered_rows: list[dict[str, Any]] = []
            for row in rows:
                outcome = dict(row.get("outcome") or {})
                row_horizon = str(row.get("horizon") or "").strip().lower()
                row_phase_bucket = _outcome_phase_bucket(outcome.get("decision_phase"))
                row_action = str(row.get("action") or "").strip().upper()
                if horizon_filters and row_horizon not in horizon_filters:
                    continue
                if phase_filters and row_phase_bucket not in phase_filters:
                    continue
                if action_filters and row_action not in action_filters:
                    continue
                filtered_rows.append(row)
            rows = filtered_rows
        return {
            "symbol": canonical,
            "count": len(rows),
            "filters": {
                "horizons": sorted(horizon_filters),
                "phase_buckets": sorted(phase_filters),
                "actions": sorted(action_filters),
            },
            "summary": _build_outcomes_summary(rows),
            "rows": rows,
        }

    async def evaluate_coin_outcomes(
        self,
        symbol: str,
        *,
        horizons: list[str] | None = None,
        decision_limit: int = 500,
        force: bool = False,
        only_matured: bool = False,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("symbol is required")
        now_ts_ms = int(now_ms or (time.time() * 1000))
        raw_horizons = list(horizons or ["15m", "1h", "4h"])
        fixed_horizon_map = {
            "15m": 15 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
        }
        dynamic_horizons = {"to_next_funding", "to_exit"}
        normalized_horizons: list[str] = []
        for item in raw_horizons:
            key = str(item or "").strip().lower()
            if not key:
                continue
            if (key in fixed_horizon_map or key in dynamic_horizons) and key not in normalized_horizons:
                normalized_horizons.append(key)
        if not normalized_horizons:
            raise ValueError("horizons must include any of: 15m,1h,4h,to_next_funding,to_exit")

        decisions = await asyncio.to_thread(
            get_decisions,
            canonical_symbol=canonical,
            limit=max(1, min(int(decision_limit), 5000)),
        )
        decisions = [
            row
            for row in decisions
            if str(row.get("mode") or "") in {"manual_candidate", "manual_position_review"}
        ]
        if not decisions:
            return {
                "symbol": canonical,
                "evaluated": 0,
                "skipped": 0,
                "deferred": 0,
                "horizons": normalized_horizons,
                "only_matured": bool(only_matured),
                "summary": _build_outcomes_summary([]),
                "rows": [],
            }

        existing_rows = await asyncio.to_thread(get_outcomes, canonical_symbol=canonical, limit=5000)
        existing_map = {
            (str(row.get("decision_id") or ""), str(row.get("horizon") or "")): row
            for row in existing_rows
        }

        focus_rows = await asyncio.to_thread(get_focus_snapshots, canonical, limit=5000)
        by_exchange: dict[str, list[dict[str, Any]]] = {}
        for row in focus_rows:
            exchange = str(row.get("exchange") or "").lower()
            if not exchange:
                continue
            by_exchange.setdefault(exchange, []).append(row)
        for exchange in list(by_exchange.keys()):
            by_exchange[exchange].sort(
                key=lambda item: int(_safe_float(item.get("ts_ms")) or 0),
                reverse=True,
            )

        paper_positions_by_key: dict[str, dict[str, Any]] = {}
        paper_events_by_key: dict[str, list[dict[str, Any]]] = {}
        real_observations_by_key: dict[str, list[dict[str, Any]]] = {}
        if "to_exit" in normalized_horizons:
            paper_positions = await asyncio.to_thread(get_paper_positions, status=None)
            for row in paper_positions:
                key = str(row.get("position_key") or "").strip()
                if key:
                    paper_positions_by_key[key] = row
            for key in list(paper_positions_by_key.keys()):
                events = await asyncio.to_thread(get_paper_events, key, limit=1000)
                paper_events_by_key[key] = list(events or [])
            real_rows = await asyncio.to_thread(
                get_real_position_observations,
                canonical_symbol=canonical,
                limit=5000,
            )
            for row in real_rows:
                key = str(row.get("state_ref") or "").strip()
                if not key:
                    continue
                real_observations_by_key.setdefault(key, []).append(row)
            for key in list(real_observations_by_key.keys()):
                real_observations_by_key[key].sort(
                    key=lambda item: int(_safe_float(item.get("ts_ms")) or 0),
                )

        evaluated_rows: list[dict[str, Any]] = []
        evaluated = 0
        skipped = 0
        deferred = 0
        for decision in decisions:
            decision_id = str(decision.get("decision_id") or "")
            action = str(decision.get("action") or "").upper()
            direction = str(decision.get("direction") or "long_a_short_b")
            decision_ts_ms = int(_safe_float(decision.get("ts_ms")) or 0)
            pair_key = str(decision.get("pair_key") or "")
            pair_parts = pair_key.split("|")
            left_exchange = pair_parts[1].lower() if len(pair_parts) >= 3 else ""
            right_exchange = pair_parts[2].lower() if len(pair_parts) >= 3 else ""

            feature_row = None
            feature_ref = str(decision.get("features_ref") or "").strip()
            if feature_ref.isdigit():
                feature_row = await asyncio.to_thread(get_feature_snapshot_by_id, int(feature_ref))
            if feature_row is None:
                continue
            feature_payload = dict(feature_row.get("features") or {})
            directional = dict(feature_payload.get("directional") or {})
            common = dict(feature_payload.get("common") or {})
            if not left_exchange:
                left_exchange = str(common.get("left_exchange") or "").lower()
            if not right_exchange:
                right_exchange = str(common.get("right_exchange") or "").lower()
            entry_open_spread = _safe_float(directional.get("open_spread_pct"))
            funding_to_next = _safe_float(directional.get("funding_to_next_pct"))
            net_funding_hourly = _safe_float(directional.get("net_funding_hourly"))
            decision_phase = str(decision.get("decision_phase") or common.get("decision_phase") or "exploratory")
            state_ref = str(decision.get("state_ref") or "").strip()
            hours_to_next_funding = _safe_float(common.get("hours_to_next_funding_min"))
            left_decision_snapshot = _nearest_snapshot(
                by_exchange.get(left_exchange, []),
                decision_ts_ms,
                max_distance_ms=60 * 60 * 1000,
            )
            right_decision_snapshot = _nearest_snapshot(
                by_exchange.get(right_exchange, []),
                decision_ts_ms,
                max_distance_ms=60 * 60 * 1000,
            )
            next_funding_target_ts = _next_funding_target_ts_ms(
                decision_ts_ms,
                left_decision_snapshot,
                right_decision_snapshot,
                common,
            )
            if (
                hours_to_next_funding is None
                and next_funding_target_ts is not None
                and next_funding_target_ts > decision_ts_ms
            ):
                hours_to_next_funding = (next_funding_target_ts - decision_ts_ms) / 3_600_000.0

            for horizon in normalized_horizons:
                key = (decision_id, horizon)
                if key in existing_map and not force:
                    skipped += 1
                    continue
                horizon_target_ts_ms = None
                if horizon in fixed_horizon_map:
                    horizon_target_ts_ms = decision_ts_ms + fixed_horizon_map[horizon]
                elif horizon == "to_next_funding":
                    horizon_target_ts_ms = next_funding_target_ts
                elif horizon == "to_exit":
                    if state_ref.startswith("paper-"):
                        horizon_target_ts_ms = _paper_exit_target_ts_ms(
                            decision_ts_ms,
                            state_ref,
                            paper_positions_by_key,
                            paper_events_by_key,
                        )
                    elif state_ref.startswith("real-"):
                        horizon_target_ts_ms = _real_exit_target_ts_ms(
                            decision_ts_ms,
                            state_ref,
                            real_observations_by_key,
                        )
                target_ts = int(horizon_target_ts_ms or 0)
                has_target_ts = target_ts > 0
                if only_matured and not _is_horizon_matured(
                    horizon=horizon,
                    horizon_target_ts_ms=(target_ts if has_target_ts else None),
                    now_ts_ms=now_ts_ms,
                ):
                    deferred += 1
                    continue
                target_plus_15m = target_ts + 15 * 60 * 1000
                target_minus_15m = max(decision_ts_ms, target_ts - 15 * 60 * 1000)

                left_target = _nearest_snapshot(by_exchange.get(left_exchange, []), target_ts) if has_target_ts else None
                right_target = _nearest_snapshot(by_exchange.get(right_exchange, []), target_ts) if has_target_ts else None
                future_close_spread, spread_delta = _decision_spread_delta(
                    direction,
                    entry_open_spread,
                    left_target,
                    right_target,
                )

                left_wait = _nearest_snapshot(by_exchange.get(left_exchange, []), target_plus_15m) if has_target_ts else None
                right_wait = _nearest_snapshot(by_exchange.get(right_exchange, []), target_plus_15m) if has_target_ts else None
                _future_wait, spread_delta_wait = _decision_spread_delta(
                    direction,
                    entry_open_spread,
                    left_wait,
                    right_wait,
                )

                left_prev = _nearest_snapshot(by_exchange.get(left_exchange, []), target_minus_15m) if has_target_ts else None
                right_prev = _nearest_snapshot(by_exchange.get(right_exchange, []), target_minus_15m) if has_target_ts else None
                _future_prev, spread_delta_prev = _decision_spread_delta(
                    direction,
                    entry_open_spread,
                    left_prev,
                    right_prev,
                )
                (
                    fees_pnl_delta_pct,
                    slippage_pnl_delta_pct,
                    execution_costs_pct,
                    action_size_ratio,
                ) = _execution_cost_components_pct(
                    action=action,
                    left_exchange=left_exchange,
                    right_exchange=right_exchange,
                )
                funding_component_pct = _estimate_funding_component_pct(
                    horizon=horizon,
                    decision_ts_ms=decision_ts_ms,
                    horizon_target_ts_ms=target_ts,
                    funding_to_next_pct=funding_to_next,
                    net_funding_hourly=net_funding_hourly,
                    hours_to_next_funding=hours_to_next_funding,
                )
                direction_component_correct = None
                if spread_delta is not None and action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "HOLD"}:
                    direction_component_correct = spread_delta >= 0

                spread_component_correct = None
                if spread_delta is not None:
                    if action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "HOLD"}:
                        spread_component_correct = spread_delta >= 0
                    elif action in {"PARTIAL_EXIT", "FULL_EXIT", "NO_TRADE", "ADD_BLOCKED"}:
                        spread_component_correct = spread_delta <= 0

                funding_component_correct = None
                if funding_component_pct is not None:
                    if action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "HOLD"}:
                        funding_component_correct = funding_component_pct >= 0
                    elif action in {"PARTIAL_EXIT", "FULL_EXIT", "NO_TRADE", "ADD_BLOCKED"}:
                        funding_component_correct = funding_component_pct <= 0

                correctness_values = [
                    bool(value)
                    for value in (spread_component_correct, funding_component_correct)
                    if value is not None
                ]
                if not correctness_values:
                    decision_correctness = "insufficient_data"
                elif all(correctness_values):
                    decision_correctness = "correct"
                elif not any(correctness_values):
                    decision_correctness = "incorrect"
                else:
                    decision_correctness = "mixed"

                timing_quality = "unknown"
                if spread_delta is not None:
                    if action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "HOLD"}:
                        if spread_delta >= 0.08:
                            timing_quality = "good"
                        elif spread_delta >= 0:
                            timing_quality = "neutral"
                        else:
                            timing_quality = "poor"
                    else:
                        if spread_delta <= -0.08:
                            timing_quality = "good"
                        elif spread_delta <= 0:
                            timing_quality = "neutral"
                        else:
                            timing_quality = "poor"

                would_waiting_15m_help = None
                if spread_delta is not None and spread_delta_wait is not None:
                    if action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "HOLD"}:
                        would_waiting_15m_help = spread_delta_wait > (spread_delta + 0.02)
                    else:
                        would_waiting_15m_help = spread_delta_wait < (spread_delta - 0.02)

                would_exiting_15m_earlier_help = None
                if spread_delta is not None and spread_delta_prev is not None:
                    if action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "HOLD"}:
                        would_exiting_15m_earlier_help = spread_delta_prev > (spread_delta + 0.02)
                    else:
                        would_exiting_15m_earlier_help = spread_delta_prev < (spread_delta - 0.02)

                net_pnl_delta_vs_alternative = None
                if spread_delta is not None and spread_delta_wait is not None:
                    funding_component_wait = _estimate_funding_component_pct(
                        horizon=horizon,
                        decision_ts_ms=decision_ts_ms,
                        horizon_target_ts_ms=target_plus_15m,
                        funding_to_next_pct=funding_to_next,
                        net_funding_hourly=net_funding_hourly,
                        hours_to_next_funding=hours_to_next_funding,
                    )
                    net_now = float(spread_delta)
                    if funding_component_pct is not None:
                        net_now += float(funding_component_pct)
                    net_now += float(execution_costs_pct)
                    net_wait = float(spread_delta_wait)
                    if funding_component_wait is not None:
                        net_wait += float(funding_component_wait)
                    net_wait += float(execution_costs_pct)
                    if action in {"ENTRY_SMALL", "ENTRY_STRONG", "ADD_SMALL", "HOLD"}:
                        net_pnl_delta_vs_alternative = net_now - net_wait
                    else:
                        net_pnl_delta_vs_alternative = net_wait - net_now

                net_pnl_delta_pct = float(execution_costs_pct)
                if spread_delta is not None:
                    net_pnl_delta_pct += float(spread_delta)
                if funding_component_pct is not None:
                    net_pnl_delta_pct += float(funding_component_pct)

                size_appropriateness = _size_appropriateness_for_action(action, spread_delta)

                outcome = {
                    "decision_correctness": decision_correctness,
                    "direction_component_correct": direction_component_correct,
                    "funding_component_correct": funding_component_correct,
                    "spread_component_correct": spread_component_correct,
                    "size_appropriateness": size_appropriateness,
                    "timing_quality": timing_quality,
                    "would_waiting_15m_help": would_waiting_15m_help,
                    "would_exiting_15m_earlier_help": would_exiting_15m_earlier_help,
                    "net_pnl_delta_vs_alternative": net_pnl_delta_vs_alternative,
                    "horizon": horizon,
                    "horizon_target_ts_ms": target_ts if has_target_ts else None,
                    "spread_delta_pct": spread_delta,
                    "spread_pnl_delta_pct": spread_delta,
                    "funding_pnl_delta_pct": funding_component_pct,
                    "fees_pnl_delta_pct": fees_pnl_delta_pct,
                    "slippage_pnl_delta_pct": slippage_pnl_delta_pct,
                    "execution_costs_pct": execution_costs_pct,
                    "net_pnl_delta_pct": net_pnl_delta_pct,
                    "future_close_spread_pct": future_close_spread,
                    "entry_open_spread_pct": entry_open_spread,
                    "funding_to_next_pct": funding_to_next,
                    "decision_action": action,
                    "decision_phase": decision_phase,
                    "pair_key": pair_key,
                    "direction": direction,
                    "pnl_assumptions": {
                        "action_size_ratio": action_size_ratio,
                        "fees_model": "taker_both_legs",
                        "slippage_bps_per_leg": OUTCOME_ASSUMED_SLIPPAGE_BPS_PER_LEG,
                    },
                }
                await asyncio.to_thread(
                    insert_outcome,
                    decision_id,
                    horizon,
                    outcome,
                    evaluated_at_ms=int(time.time() * 1000),
                )
                evaluated += 1
                evaluated_rows.append(
                    {
                        "decision_id": decision_id,
                        "horizon": horizon,
                        "outcome": outcome,
                    }
                )

        return {
            "symbol": canonical,
            "evaluated": evaluated,
            "skipped": skipped,
            "deferred": deferred,
            "horizons": normalized_horizons,
            "only_matured": bool(only_matured),
            "summary": _build_outcomes_summary(evaluated_rows),
            "rows": evaluated_rows,
        }

    async def get_coin_outcomes_auto_status(
        self,
        *,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        now_ts_ms = int(time.time() * 1000)
        last_cycle = dict(self._coin_outcomes_last_cycle or {})
        last_cycle_ts_ms = int(_safe_float(last_cycle.get("ts_ms")) or 0)
        last_cycle_age_sec = None
        if last_cycle_ts_ms > 0:
            last_cycle_age_sec = max(0.0, (now_ts_ms - last_cycle_ts_ms) / 1000.0)

        health_status = "healthy"
        health_reasons: list[str] = []

        def _escalate(level: str, reason: str) -> None:
            nonlocal health_status
            order = {"healthy": 0, "warn": 1, "stale": 2}
            if order.get(level, 0) > order.get(health_status, 0):
                health_status = level
            if reason not in health_reasons:
                health_reasons.append(reason)

        scheduler_running = bool(
            self._coin_outcomes_task is not None and not self._coin_outcomes_task.done()
        )
        scheduler_enabled = bool(self._coin_outcomes_scheduler_enabled)
        poll_sec = max(1.0, float(self._coin_outcomes_poll_sec))
        if not scheduler_running:
            _escalate("stale", "scheduler_not_running")
        if not scheduler_enabled:
            _escalate("warn", "scheduler_paused")
        if last_cycle_ts_ms <= 0:
            _escalate("warn", "cycle_not_started")
        elif last_cycle_age_sec is not None:
            if last_cycle_age_sec > (poll_sec * 4.0):
                if scheduler_enabled:
                    _escalate("stale", "cycle_stale")
            elif last_cycle_age_sec > (poll_sec * 2.0):
                if scheduler_enabled:
                    _escalate("warn", "cycle_delayed")
        cycle_errors = int(_safe_float(last_cycle.get("errors")) or 0)
        if cycle_errors > 0:
            _escalate("warn", "cycle_errors")

        payload: dict[str, Any] = {
            "scheduler_running": scheduler_running,
            "scheduler_enabled": scheduler_enabled,
            "poll_sec": poll_sec,
            "auto_horizons": list(COIN_OUTCOME_AUTO_HORIZONS),
            "last_cycle": last_cycle,
            "recent_cycles": list(self._coin_outcomes_cycle_history[-10:]),
            "last_cycle_age_sec": last_cycle_age_sec,
            "now_ts_ms": now_ts_ms,
            "health": {
                "status": health_status,
                "reasons": health_reasons,
            },
        }

        if symbol is None:
            return payload

        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("symbol is required")

        decisions = await asyncio.to_thread(
            get_decisions,
            canonical_symbol=canonical,
            limit=5000,
        )
        decisions = [
            row
            for row in decisions
            if str(row.get("mode") or "") in {"manual_candidate", "manual_position_review"}
        ]
        outcomes = await asyncio.to_thread(
            get_outcomes,
            canonical_symbol=canonical,
            limit=5000,
        )
        existing = {
            (str(row.get("decision_id") or ""), str(row.get("horizon") or ""))
            for row in outcomes
        }
        missing_by_horizon = {h: 0 for h in COIN_OUTCOME_AUTO_HORIZONS}
        missing_total = 0
        for decision in decisions:
            decision_id = str(decision.get("decision_id") or "")
            if not decision_id:
                continue
            for horizon in COIN_OUTCOME_AUTO_HORIZONS:
                if (decision_id, horizon) in existing:
                    continue
                missing_total += 1
                missing_by_horizon[horizon] += 1

        payload["symbol"] = canonical
        payload["symbol_pending"] = {
            "decisions_total": len(decisions),
            "missing_total": missing_total,
            "missing_by_horizon": missing_by_horizon,
        }
        if missing_total >= 500:
            _escalate("stale", "symbol_backlog_huge")
        elif missing_total >= 100:
            _escalate("warn", "symbol_backlog_growing")
        payload["health"] = {
            "status": health_status,
            "reasons": health_reasons,
        }
        return payload

    async def set_coin_outcomes_scheduler_enabled(self, enabled: bool) -> dict[str, Any]:
        self._coin_outcomes_scheduler_enabled = bool(enabled)
        return await self.get_coin_outcomes_auto_status()

    async def run_coin_position_watcher_once(
        self,
        *,
        force: bool = False,
        symbols: list[str] | None = None,
        window_minutes: int = 240,
        funding_points: int = 96,
    ) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        if not self._coin_position_watcher_enabled and not force:
            cycle = {
                "ts_ms": now_ms,
                "enabled": False,
                "force": bool(force),
                "reason": "watcher_disabled",
                "symbols_total": 0,
                "symbols_processed": 0,
                "symbols_skipped_cooldown": 0,
                "errors": 0,
                "position_decisions_saved": 0,
            }
            self._coin_position_watcher_last_cycle = cycle
            return cycle

        canonical_symbols: set[str] = set()
        if symbols:
            for item in symbols:
                canonical = normalize_symbol(str(item or ""))
                if canonical:
                    canonical_symbols.add(canonical)
        else:
            canonical_symbols = await self._collect_coin_held_symbols()

        sorted_symbols = sorted(canonical_symbols)
        poll_window = max(60, min(int(window_minutes), 4320))
        funding_limit = max(24, min(int(funding_points), 200))
        cooldown_sec = max(0.0, float(COIN_POSITION_WATCHER_SYMBOL_COOLDOWN_SEC))
        skipped_cooldown = 0
        processed = 0
        error_count = 0
        position_decisions_saved = 0
        analyzed_symbols: list[str] = []
        cycle_errors: list[dict[str, str]] = []

        for canonical in sorted_symbols:
            if not force:
                last_ts = float(self._coin_position_watcher_last_by_symbol_ts.get(canonical) or 0.0)
                if last_ts > 0.0 and (time.time() - last_ts) < cooldown_sec:
                    skipped_cooldown += 1
                    continue
            try:
                payload = await self.analyze_symbol(
                    canonical,
                    window_minutes=poll_window,
                    funding_points=funding_limit,
                    use_cache=False,
                    persist_candidate_decision=False,
                    run_position_logic=True,
                )
                summary = (payload.get("position_logic") or {}).get("summary") or {}
                position_decisions_saved += int(
                    (_safe_float(summary.get("paper_decisions_saved")) or 0)
                    + (_safe_float(summary.get("real_decisions_saved")) or 0)
                )
                processed += 1
                analyzed_symbols.append(canonical)
                self._coin_position_watcher_last_by_symbol_ts[canonical] = time.time()
            except Exception as exc:  # pylint: disable=broad-except
                error_count += 1
                cycle_errors.append({"symbol": canonical, "error": str(exc)})

        cycle = {
            "ts_ms": now_ms,
            "enabled": bool(self._coin_position_watcher_enabled),
            "force": bool(force),
            "symbols_total": len(sorted_symbols),
            "symbols_processed": processed,
            "symbols_skipped_cooldown": skipped_cooldown,
            "errors": error_count,
            "position_decisions_saved": position_decisions_saved,
            "window_minutes": poll_window,
            "funding_points": funding_limit,
            "symbols_analyzed": analyzed_symbols,
            "error_details": cycle_errors[:20],
        }
        self._coin_position_watcher_last_cycle = cycle
        return cycle

    async def get_coin_position_watcher_status(
        self,
        *,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        last_cycle = dict(self._coin_position_watcher_last_cycle or {})
        last_cycle_ts_ms = int(_safe_float(last_cycle.get("ts_ms")) or 0)
        last_cycle_age_sec = None
        if last_cycle_ts_ms > 0:
            last_cycle_age_sec = max(0.0, (now_ms - last_cycle_ts_ms) / 1000.0)
        payload: dict[str, Any] = {
            "enabled": bool(self._coin_position_watcher_enabled),
            "scheduler_running": bool(
                self._coin_position_watcher_task is not None
                and not self._coin_position_watcher_task.done()
            ),
            "poll_sec": float(self._coin_position_watcher_poll_sec),
            "symbol_cooldown_sec": float(COIN_POSITION_WATCHER_SYMBOL_COOLDOWN_SEC),
            "last_cycle": last_cycle,
            "last_cycle_age_sec": last_cycle_age_sec,
            "now_ts_ms": now_ms,
        }
        if symbol:
            canonical = normalize_symbol(symbol)
            if not canonical:
                raise ValueError("symbol is required")
            payload["symbol"] = canonical
            last_symbol_ts = float(self._coin_position_watcher_last_by_symbol_ts.get(canonical) or 0.0)
            payload["symbol_last_run_ts_ms"] = int(last_symbol_ts * 1000) if last_symbol_ts > 0 else None
            if last_symbol_ts > 0:
                payload["symbol_last_run_age_sec"] = max(0.0, time.time() - last_symbol_ts)
        return payload

    async def set_coin_position_watcher_enabled(self, enabled: bool) -> dict[str, Any]:
        self._coin_position_watcher_enabled = bool(enabled)
        return await self.get_coin_position_watcher_status()

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
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol must be provided for funding history analysis.")

        supported = [
            exchange
            for exchange in SUPPORTED_EXCHANGES
            if normalize_exchange_name(exchange) in ADAPTER_FACTORIES
        ]
        requested = list(exchanges or FUNDING_HISTORY_DEFAULT_EXCHANGES)
        selected: list[str] = []
        for item in requested:
            exchange = normalize_exchange_name(str(item or "").strip())
            if not exchange or exchange not in supported or exchange in selected:
                continue
            selected.append(exchange)
        if not selected:
            selected = [exchange for exchange in FUNDING_HISTORY_DEFAULT_EXCHANGES if exchange in supported]
        if not selected:
            raise ValueError("Enable at least one supported exchange.")

        windows = _funding_history_windows(windows_hours)
        max_window_hours = max(int(window.get("hours") or 0) for window in windows)
        fetch_limit = max(24, min(int(funding_points or FUNDING_HISTORY_MAX_POINTS), FUNDING_HISTORY_MAX_POINTS))
        fetch_limit = max(fetch_limit, min(FUNDING_HISTORY_MAX_POINTS, max_window_hours + 16))

        tasks = [
            self._analyze_funding_history_on_exchange(exchange, canonical, fetch_limit, windows)
            for exchange in selected
        ]
        exchange_rows = [row for row in await asyncio.gather(*tasks) if row]
        pair_windows, best_by_window, spread_series = _build_funding_history_pair_analysis(
            exchange_rows,
            windows,
        )
        timeline = _build_funding_history_timeline(exchange_rows, max_hours=max_window_hours)
        chart_series: dict[str, Any] = {}
        for row in exchange_rows:
            exchange = str(row.get("exchange") or "")
            if not exchange:
                continue
            history_asc = list(reversed(list(row.get("funding_history") or [])))
            chart_series[exchange] = [
                {
                    "ts_ms": int(item.get("ts_ms") or 0),
                    "rate_bps": _safe_float(item.get("rate_bps")),
                    "interval_hours": _safe_float(item.get("interval_hours")),
                }
                for item in history_asc
                if _safe_float(item.get("rate_bps")) is not None
            ][-120:]

        warnings: list[str] = []
        if len([row for row in exchange_rows if row.get("funding_history")]) < 2:
            warnings.append("pair_analysis_limited: fewer than two exchanges returned funding history")

        return {
            "symbol": canonical,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "supported_exchanges": supported,
            "default_exchanges": list(FUNDING_HISTORY_DEFAULT_EXCHANGES),
            "selected_exchanges": selected,
            "funding_points": fetch_limit,
            "windows": windows,
            "warnings": warnings,
            "exchanges": exchange_rows,
            "best_by_window": best_by_window,
            "pair_windows": pair_windows,
            "timeline": timeline,
            "charts": {
                "exchange_rates": chart_series,
                "best_pair_hourly": spread_series,
            },
            "method": {
                "carry_formula": "long leg receives -funding_rate, short leg receives +funding_rate",
                "interval_handling": "funding events are converted to time segments and prorated by overlap inside each analysis window",
                "windows": [window.get("label") for window in windows],
            },
        }

    async def _analyze_funding_history_on_exchange(
        self,
        exchange: str,
        canonical_symbol: str,
        funding_points: int,
        windows: list[dict[str, Any]],
    ) -> dict[str, Any]:
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
        warnings: list[str] = []
        snapshot_dict: dict[str, Any] = {}
        try:
            snapshots = await adapter.fetch_market_snapshots_async([canonical_symbol])
            if snapshots:
                snapshot_dict = snapshots[0].to_dict()
        except Exception as exc:  # pylint: disable=broad-except
            warnings.append(f"snapshot_unavailable:{exc}")
        result["snapshot"] = snapshot_dict

        raw_history = await asyncio.to_thread(
            _load_funding_history_cached,
            exchange,
            exchange_symbol,
            canonical_symbol,
            funding_points,
            adapter,
        )
        interval_hours = _resolve_funding_interval_hours(
            raw_history,
            _safe_float(snapshot_dict.get("funding_interval_hours")),
        )
        funding_history = _compact_funding_history_rows(
            raw_history,
            snapshot_interval=interval_hours,
            limit=funding_points,
        )
        interval_hours = _resolve_funding_interval_hours(
            funding_history,
            interval_hours,
        )
        await asyncio.to_thread(
            self._persist_coin_funding_history,
            canonical_symbol,
            exchange,
            funding_history,
            interval_hours,
        )

        result["funding_history"] = funding_history
        result["funding_interval_hours_resolved"] = interval_hours
        result["latest_funding_rate"] = (
            _safe_float(funding_history[0].get("rate")) if funding_history else _safe_float(snapshot_dict.get("funding_rate"))
        )
        result["latest_funding_bps"] = (
            float(result["latest_funding_rate"]) * 10000.0
            if result.get("latest_funding_rate") is not None
            else None
        )
        result["windows"] = _funding_history_exchange_windows(funding_history, interval_hours, windows)
        result["data_quality"] = {
            "funding_points_received": len(funding_history),
            "oldest_ts_ms": funding_history[-1].get("ts_ms") if funding_history else None,
            "latest_ts_ms": funding_history[0].get("ts_ms") if funding_history else None,
        }
        if funding_history and interval_hours is None:
            warnings.append("funding_interval_unresolved")
        if funding_history and len(funding_history) < 2:
            warnings.append("funding_history_short")
        if not funding_history:
            result["status"] = "error"
            result["error"] = "Funding history unavailable for this symbol/exchange."
        elif warnings:
            result["status"] = "partial"
        else:
            result["status"] = "ok"
        if warnings:
            result["warnings"] = warnings
        return result

    async def coin_paper_enter(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        canonical = normalize_symbol(str(payload.get("symbol") or ""))
        if not canonical:
            raise ValueError("symbol is required")
        qty = _safe_float(payload.get("qty"))
        if qty is None or qty <= 0:
            raise ValueError("qty must be > 0")
        window_minutes = int(_safe_float(payload.get("window_minutes")) or 240)
        funding_points = int(_safe_float(payload.get("funding_points")) or 96)
        window_minutes = max(60, min(window_minutes, 4320))
        funding_points = max(24, min(funding_points, 200))

        pair_key = str(payload.get("pair_key") or "").strip()
        direction = str(payload.get("direction") or "").strip().lower()
        if direction not in ("long_a_short_b", "long_b_short_a"):
            direction = ""

        analysis = await self.analyze_symbol(
            canonical,
            window_minutes=window_minutes,
            funding_points=funding_points,
        )
        bot_logic = analysis.get("bot_logic") or {}
        recommended_pair = bot_logic.get("recommended_pair") or {}
        if not pair_key:
            pair_key = str(recommended_pair.get("pair_key") or "")
        if not pair_key:
            pair_key = build_pair_key(canonical, "binance", "kucoin")
        if not direction:
            direction = str(recommended_pair.get("direction") or "long_a_short_b")
        if direction not in ("long_a_short_b", "long_b_short_a"):
            direction = "long_a_short_b"

        action = str(payload.get("action") or bot_logic.get("recommended_action") or "ENTRY_SMALL")
        action = action.upper()
        now_ms = int(time.time() * 1000)
        position_key = str(payload.get("position_key") or "").strip()
        if not position_key:
            position_key = f"paper-{canonical.lower()}-{now_ms}-{uuid4().hex[:8]}"

        pair_row = None
        for row in list(analysis.get("pair_analysis") or []):
            if str(row.get("pair_key") or "") == pair_key:
                pair_row = row
                break
        if pair_row is None and (analysis.get("pair_analysis") or []):
            pair_row = list(analysis.get("pair_analysis") or [])[0]

        entry_context = {
            "source": str(payload.get("source") or "coin_analysis_manual"),
            "source_decision_id": (analysis.get("decision_journal") or {}).get("decision_id"),
            "entry_action": action,
            "entry_note": str(payload.get("note") or ""),
            "bot_decision": bot_logic.get("decision"),
            "bot_recommended_action": bot_logic.get("recommended_action"),
            "pair_key": pair_key,
            "direction": direction,
            "analysis_window_minutes": window_minutes,
            "analysis_funding_points": funding_points,
            "entry_spread_context": {
                "derived_spread": (pair_row or {}).get("derived_spread"),
                "decision_phase": (pair_row or {}).get("decision_phase"),
                "score": (pair_row or {}).get("score"),
            },
        }
        await asyncio.to_thread(
            upsert_paper_position,
            CoinPaperPositionRow(
                position_key=position_key,
                opened_at_ms=now_ms,
                closed_at_ms=None,
                status="open",
                canonical_symbol=canonical,
                pair_key=pair_key,
                direction=direction,
                qty=float(qty),
                entry_context=entry_context,
                updated_at_ms=now_ms,
            ),
        )
        event_id = f"paper-event-{uuid4().hex}"
        await asyncio.to_thread(
            insert_paper_event,
            event_id,
            position_key,
            now_ms,
            "entry",
            {
                "qty": float(qty),
                "action": action,
                "pair_key": pair_key,
                "direction": direction,
                "decision_id": (analysis.get("decision_journal") or {}).get("decision_id"),
            },
        )
        await self._record_coin_trade_activity(
            canonical_symbol=canonical,
            ts_ms=now_ms,
            pair_key=pair_key,
            direction=direction,
            activity_type="paper_enter",
            source="coin_paper_enter",
            state_ref=position_key,
            payload={
                "qty": float(qty),
                "action": action,
                "decision_id": (analysis.get("decision_journal") or {}).get("decision_id"),
                "event_id": event_id,
            },
        )
        return {
            "ok": True,
            "position_key": position_key,
            "status": "open",
            "action": action,
            "qty": float(qty),
            "pair_key": pair_key,
            "direction": direction,
            "decision_id": (analysis.get("decision_journal") or {}).get("decision_id"),
        }

    async def coin_paper_apply_action(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        position_key = str(payload.get("position_key") or "").strip()
        if not position_key:
            raise ValueError("position_key is required")
        action = str(payload.get("action") or "").strip().upper()
        if action not in {"HOLD", "PARTIAL_EXIT", "FULL_EXIT", "ADD_SMALL", "ADD_BLOCKED"}:
            raise ValueError("action must be one of HOLD/PARTIAL_EXIT/FULL_EXIT/ADD_SMALL/ADD_BLOCKED")

        all_rows = await asyncio.to_thread(get_paper_positions, status=None)
        current = None
        for row in all_rows:
            if str(row.get("position_key") or "") == position_key:
                current = row
                break
        if current is None:
            raise ValueError("position_key not found")

        status = str(current.get("status") or "open")
        if status != "open":
            raise ValueError("position is not open")
        current_qty = _safe_float(current.get("qty")) or 0.0
        if current_qty <= 0:
            raise ValueError("position qty is zero")

        fraction = _safe_float(payload.get("fraction"))
        absolute_qty = _safe_float(payload.get("qty"))
        if fraction is None:
            if action in {"PARTIAL_EXIT", "ADD_SMALL"}:
                fraction = 0.25
            else:
                fraction = 1.0
        fraction = max(0.0, min(float(fraction), 1.0))

        qty_delta = 0.0
        if action == "PARTIAL_EXIT":
            qty_delta = -(absolute_qty if absolute_qty is not None else current_qty * fraction)
        elif action == "FULL_EXIT":
            qty_delta = -current_qty
        elif action == "ADD_SMALL":
            qty_delta = absolute_qty if absolute_qty is not None else current_qty * fraction
        elif action in {"HOLD", "ADD_BLOCKED"}:
            qty_delta = 0.0

        next_qty = current_qty + qty_delta
        if next_qty < 0:
            next_qty = 0.0

        now_ms = int(time.time() * 1000)
        next_status = "closed" if next_qty <= 1e-12 else "open"
        closed_at_ms = now_ms if next_status == "closed" else None
        entry_context = dict(current.get("entry_context") or {})
        entry_context["last_paper_action"] = action
        entry_context["last_paper_action_ts_ms"] = now_ms
        entry_context["last_paper_action_delta"] = qty_delta
        await asyncio.to_thread(
            upsert_paper_position,
            CoinPaperPositionRow(
                position_key=position_key,
                opened_at_ms=int(current.get("opened_at_ms") or now_ms),
                closed_at_ms=closed_at_ms,
                status=next_status,
                canonical_symbol=str(current.get("canonical_symbol") or ""),
                pair_key=str(current.get("pair_key") or ""),
                direction=str(current.get("direction") or "long_a_short_b"),
                qty=float(next_qty),
                entry_context=entry_context,
                updated_at_ms=now_ms,
            ),
        )
        event_type = action.lower()
        event_id = f"paper-event-{uuid4().hex}"
        await asyncio.to_thread(
            insert_paper_event,
            event_id,
            position_key,
            now_ms,
            event_type,
            {
                "action": action,
                "qty_before": current_qty,
                "qty_delta": qty_delta,
                "qty_after": next_qty,
                "fraction": fraction,
            },
        )
        activity_type = {
            "HOLD": "paper_hold",
            "PARTIAL_EXIT": "paper_partial_exit",
            "FULL_EXIT": "paper_full_exit",
            "ADD_SMALL": "paper_add_small",
            "ADD_BLOCKED": "paper_add_blocked",
        }.get(action, "paper_action")
        await self._record_coin_trade_activity(
            canonical_symbol=str(current.get("canonical_symbol") or ""),
            ts_ms=now_ms,
            pair_key=str(current.get("pair_key") or ""),
            direction=str(current.get("direction") or ""),
            activity_type=activity_type,
            source="coin_paper_action",
            state_ref=position_key,
            payload={
                "event_id": event_id,
                "action": action,
                "qty_before": current_qty,
                "qty_delta": qty_delta,
                "qty_after": next_qty,
                "fraction": fraction,
            },
        )
        return {
            "ok": True,
            "position_key": position_key,
            "action": action,
            "status": next_status,
            "qty_before": current_qty,
            "qty_delta": qty_delta,
            "qty_after": next_qty,
            "closed_at_ms": closed_at_ms,
        }

    def _coin_analysis_selected_exchanges(self) -> list[str]:
        exchange_flags = getattr(self._settings_manager.current, "analysis_exchanges", None) or {}
        enabled_exchanges = {
            normalize_exchange_name(name)
            for name, enabled in exchange_flags.items()
            if enabled
        }
        selected = [
            ex for ex in COIN_ANALYSIS_CORE_EXCHANGES if not enabled_exchanges or ex in enabled_exchanges
        ]
        return selected

    async def _collect_focus_symbol_snapshots(
        self,
        canonical_symbol: str,
        exchanges: list[str],
        *,
        focus_reason: str = "symbol_session",
    ) -> int:
        inserted = 0
        for exchange in exchanges:
            try:
                adapter = get_adapter_cached(exchange)
            except KeyError:
                continue
            try:
                snapshots = await adapter.fetch_market_snapshots_async([canonical_symbol])
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug(
                    "Coin focus snapshot fetch failed for %s %s: %s",
                    exchange,
                    canonical_symbol,
                    exc,
                )
                continue
            if not snapshots:
                continue
            snap = snapshots[0]
            ts_ms = int(time.time() * 1000)
            bid = _safe_float(snap.bid)
            ask = _safe_float(snap.ask)
            mid = None
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
            index_price = _safe_float(
                ((snap.raw or {}).get("premiumIndex") or {}).get("indexPrice")
            )
            if index_price is None:
                index_price = _safe_float(
                    ((snap.raw or {}).get("contract") or {}).get("indexPrice")
                )
            mark_price = _safe_float(snap.mark_price)
            premium_pct = None
            if mark_price is not None and index_price is not None and abs(index_price) > 1e-12:
                premium_pct = (mark_price - index_price) / index_price * 100.0
            predicted_funding = _safe_float(
                ((snap.raw or {}).get("contract") or {}).get("predictedFundingFeeRate")
            )
            next_funding_ts_ms = None
            if snap.next_funding_time is not None:
                try:
                    next_funding_ts_ms = int(snap.next_funding_time.timestamp() * 1000)
                except Exception:  # pylint: disable=broad-except
                    next_funding_ts_ms = None

            await asyncio.to_thread(
                insert_focus_snapshot,
                CoinFocusSnapshotRow(
                    ts_ms=ts_ms,
                    canonical_symbol=canonical_symbol,
                    exchange=exchange,
                    exchange_symbol=snap.exchange_symbol,
                    bid=bid,
                    ask=ask,
                    bid_size=_safe_float(snap.bid_size),
                    ask_size=_safe_float(snap.ask_size),
                    mid=mid,
                    mark_price=mark_price,
                    index_price=index_price,
                    premium_pct=premium_pct,
                    funding_rate=_safe_float(snap.funding_rate),
                    predicted_funding_rate=predicted_funding,
                    next_funding_ts_ms=next_funding_ts_ms,
                    quote_age_ms=0,
                    source_type="rest_adapter",
                    staleness_flag=False,
                    focus_reason=focus_reason,
                ),
            )
            inserted += 1
        return inserted

    async def _collect_coin_held_symbols(self) -> set[str]:
        held_symbols: set[str] = set()
        open_paper = await asyncio.to_thread(get_paper_positions, status="open")
        for row in open_paper:
            sym = normalize_symbol(str(row.get("canonical_symbol") or ""))
            if sym:
                held_symbols.add(sym)

        snapshot = self._accounts.snapshot() or {}
        for pos in list(snapshot.get("positions") or []):
            sym = _normalize_manual_symbol(
                str(
                    pos.get("symbol_normalized")
                    or pos.get("symbol")
                    or pos.get("exchange_symbol")
                    or ""
                )
            )
            if sym:
                held_symbols.add(sym)
        return held_symbols

    async def _collect_coin_focus_targets(self) -> dict[str, Any]:
        session_rows = await self.list_active_coin_symbol_sessions()
        session_symbols = {
            normalize_symbol(str(item.get("canonical_symbol") or ""))
            for item in session_rows
            if str(item.get("canonical_symbol") or "").strip()
        }
        session_symbols.discard("")
        held_symbols = await self._collect_coin_held_symbols()

        all_symbols = sorted(session_symbols | held_symbols)
        reason_by_symbol: dict[str, str] = {}
        for sym in all_symbols:
            in_session = sym in session_symbols
            is_held = sym in held_symbols
            if in_session and is_held:
                reason_by_symbol[sym] = "session_or_held"
            elif in_session:
                reason_by_symbol[sym] = "symbol_session"
            else:
                reason_by_symbol[sym] = "held_position"
        return {
            "symbols": all_symbols,
            "reason_by_symbol": reason_by_symbol,
            "session_symbols": len(session_symbols),
            "held_symbols": len(held_symbols),
        }

    async def collect_coin_focus_once(self) -> dict[str, int]:
        exchanges = self._coin_analysis_selected_exchanges()
        targets = await self._collect_coin_focus_targets()
        symbols = list(targets.get("symbols") or [])
        reason_by_symbol = dict(targets.get("reason_by_symbol") or {})
        if not symbols or not exchanges:
            return {
                "symbols": len(symbols),
                "rows": 0,
                "session_symbols": int(targets.get("session_symbols") or 0),
                "held_symbols": int(targets.get("held_symbols") or 0),
            }
        rows_inserted = 0
        for symbol in symbols:
            rows_inserted += await self._collect_focus_symbol_snapshots(
                symbol,
                exchanges,
                focus_reason=str(reason_by_symbol.get(symbol) or "symbol_session"),
            )
        return {
            "symbols": len(symbols),
            "rows": rows_inserted,
            "session_symbols": int(targets.get("session_symbols") or 0),
            "held_symbols": int(targets.get("held_symbols") or 0),
        }

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
        if self._derisk_task is None:
            self._derisk_task = asyncio.create_task(self._derisk_scheduler())
        if self._coin_focus_task is None:
            self._coin_focus_task = asyncio.create_task(self._coin_focus_scheduler())
        if self._coin_outcomes_task is None:
            self._coin_outcomes_task = asyncio.create_task(self._coin_outcomes_scheduler())
        if self._coin_retention_task is None:
            self._coin_retention_task = asyncio.create_task(self._coin_retention_scheduler())
        if self._coin_position_watcher_task is None:
            self._coin_position_watcher_task = asyncio.create_task(
                self._coin_position_watcher_scheduler()
            )
        if not self._coin_retention_last_report:
            try:
                await self.run_coin_analysis_retention_once(reason="startup")
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Coin retention startup run failed: %s", exc)
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
        if self._auto_arb_task:
            self._auto_arb_task.cancel()
            try:
                await self._auto_arb_task
            except asyncio.CancelledError:
                pass
            self._auto_arb_task = None
        if self._auto_strategy_task:
            self._auto_strategy_task.cancel()
            try:
                await self._auto_strategy_task
            except asyncio.CancelledError:
                pass
            self._auto_strategy_task = None
        if self._derisk_task:
            self._derisk_task.cancel()
            try:
                await self._derisk_task
            except asyncio.CancelledError:
                pass
            self._derisk_task = None
        if self._coin_focus_task:
            self._coin_focus_task.cancel()
            try:
                await self._coin_focus_task
            except asyncio.CancelledError:
                pass
            self._coin_focus_task = None
        if self._coin_outcomes_task:
            self._coin_outcomes_task.cancel()
            try:
                await self._coin_outcomes_task
            except asyncio.CancelledError:
                pass
            self._coin_outcomes_task = None
        if self._coin_retention_task:
            self._coin_retention_task.cancel()
            try:
                await self._coin_retention_task
            except asyncio.CancelledError:
                pass
            self._coin_retention_task = None
        if self._coin_position_watcher_task:
            self._coin_position_watcher_task.cancel()
            try:
                await self._coin_position_watcher_task
            except asyncio.CancelledError:
                pass
            self._coin_position_watcher_task = None
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

    async def _derisk_scheduler(self) -> None:
        try:
            while True:
                await self._auto_derisk_cycle()
                await asyncio.sleep(max(2.0, float(self._derisk_poll_sec or 5.0)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("emergency de-risk loop failed: %s", exc)

    async def _restart_derisk_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._derisk_task:
            self._derisk_task.cancel()
            try:
                await self._derisk_task
            except asyncio.CancelledError:
                pass
            self._derisk_task = None
        self._derisk_task = asyncio.create_task(self._derisk_scheduler())

    async def _coin_focus_scheduler(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(1.0, float(self._coin_focus_poll_sec)))
                try:
                    await self.collect_coin_focus_once()
                    if (time.time() - float(self._coin_shortlist_last_run_ts or 0.0)) >= max(
                        30.0,
                        float(self._coin_shortlist_poll_sec),
                    ):
                        await self.collect_coin_candidate_shortlist_once(top_n=3)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Coin focus collector cycle failed: %s", exc)
        except asyncio.CancelledError:
            raise

    async def _restart_coin_focus_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._coin_focus_task:
            self._coin_focus_task.cancel()
            try:
                await self._coin_focus_task
            except asyncio.CancelledError:
                pass
            self._coin_focus_task = None
        self._coin_focus_task = asyncio.create_task(self._coin_focus_scheduler())

    async def _coin_outcomes_scheduler(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(5.0, float(self._coin_outcomes_poll_sec)))
                if not self._coin_outcomes_scheduler_enabled:
                    continue
                try:
                    await self.evaluate_matured_coin_outcomes_once()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Coin outcomes evaluator cycle failed: %s", exc)
        except asyncio.CancelledError:
            raise

    async def _restart_coin_outcomes_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._coin_outcomes_task:
            self._coin_outcomes_task.cancel()
            try:
                await self._coin_outcomes_task
            except asyncio.CancelledError:
                pass
            self._coin_outcomes_task = None
        self._coin_outcomes_task = asyncio.create_task(self._coin_outcomes_scheduler())

    async def _coin_position_watcher_scheduler(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(5.0, float(self._coin_position_watcher_poll_sec)))
                if not self._coin_position_watcher_enabled:
                    continue
                try:
                    await self.run_coin_position_watcher_once()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Coin position watcher cycle failed: %s", exc)
        except asyncio.CancelledError:
            raise

    async def _restart_coin_position_watcher_scheduler(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._coin_position_watcher_task:
            self._coin_position_watcher_task.cancel()
            try:
                await self._coin_position_watcher_task
            except asyncio.CancelledError:
                pass
            self._coin_position_watcher_task = None
        self._coin_position_watcher_task = asyncio.create_task(
            self._coin_position_watcher_scheduler()
        )

    async def _coin_retention_scheduler(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(60.0, float(self._coin_retention_poll_sec)))
                try:
                    await self.run_coin_analysis_retention_once(reason="scheduler")
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Coin retention cycle failed: %s", exc)
        except asyncio.CancelledError:
            raise

    async def run_coin_analysis_retention_once(
        self,
        *,
        max_age_days: int | None = None,
        closed_paper_days: int | None = None,
        reason: str = "manual",
    ) -> dict[str, Any]:
        max_days = max(1, int(max_age_days or self._coin_retention_max_age_days))
        closed_days = max(1, int(closed_paper_days or self._coin_retention_closed_paper_days))
        now_ms = int(time.time() * 1000)
        before = await asyncio.to_thread(get_coin_analysis_table_counts)
        deleted = await asyncio.to_thread(
            prune_coin_analysis_data,
            max_age_ms=max_days * 24 * 3600 * 1000,
            closed_paper_max_age_ms=closed_days * 24 * 3600 * 1000,
            now_ms=now_ms,
        )
        after = await asyncio.to_thread(get_coin_analysis_table_counts)
        report = {
            "ts_ms": now_ms,
            "reason": str(reason or "manual"),
            "max_age_days": max_days,
            "closed_paper_days": closed_days,
            "deleted": deleted,
            "before": before,
            "after": after,
        }
        self._coin_retention_last_report = report
        return report

    async def get_coin_analysis_maintenance_status(self) -> dict[str, Any]:
        counts = await asyncio.to_thread(get_coin_analysis_table_counts)
        return {
            "retention": {
                "scheduler_running": bool(
                    self._coin_retention_task is not None and not self._coin_retention_task.done()
                ),
                "poll_sec": float(self._coin_retention_poll_sec),
                "max_age_days": int(self._coin_retention_max_age_days),
                "closed_paper_days": int(self._coin_retention_closed_paper_days),
                "last_report": dict(self._coin_retention_last_report or {}),
            },
            "table_counts": counts,
        }

    def _record_coin_outcomes_cycle(self, cycle: Mapping[str, Any]) -> None:
        item = dict(cycle or {})
        self._coin_outcomes_last_cycle = item
        self._coin_outcomes_cycle_history.append(item)
        limit = max(1, int(self._coin_outcomes_cycle_history_limit or 50))
        if len(self._coin_outcomes_cycle_history) > limit:
            self._coin_outcomes_cycle_history = self._coin_outcomes_cycle_history[-limit:]

    async def evaluate_matured_coin_outcomes_once(
        self,
        *,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        now_ts_ms = int(time.time() * 1000)
        scope_symbol = normalize_symbol(symbol) if symbol else None
        if symbol is not None and not scope_symbol:
            raise ValueError("symbol is required")

        decisions = await asyncio.to_thread(
            get_decisions,
            canonical_symbol=scope_symbol,
            limit=5000,
        )
        decisions = [
            row
            for row in decisions
            if str(row.get("mode") or "") in {"manual_candidate", "manual_position_review"}
        ]
        if not decisions:
            cycle = {
                "ts_ms": now_ts_ms,
                "symbols_total": 0,
                "symbols_processed": 0,
                "evaluated": 0,
                "skipped": 0,
                "deferred": 0,
                "errors": 0,
                "scope_symbol": scope_symbol,
            }
            self._record_coin_outcomes_cycle(cycle)
            return cycle

        existing = await asyncio.to_thread(get_outcomes, limit=5000)
        existing_keys = {
            (str(row.get("decision_id") or ""), str(row.get("horizon") or ""))
            for row in existing
        }

        latest_ts_by_symbol: dict[str, int] = {}
        pending_by_symbol: dict[str, int] = {}
        for row in decisions:
            decision_id = str(row.get("decision_id") or "")
            symbol = normalize_symbol(str(row.get("canonical_symbol") or ""))
            if not decision_id or not symbol:
                continue
            ts_ms = int(_safe_float(row.get("ts_ms")) or 0)
            if ts_ms > latest_ts_by_symbol.get(symbol, 0):
                latest_ts_by_symbol[symbol] = ts_ms
            pending = 0
            for horizon in COIN_OUTCOME_AUTO_HORIZONS:
                if (decision_id, horizon) not in existing_keys:
                    pending += 1
            if pending > 0:
                pending_by_symbol[symbol] = pending_by_symbol.get(symbol, 0) + pending

        pending_symbols = pending_by_symbol.keys()
        ordered_symbols = sorted(
            pending_symbols,
            key=lambda s: latest_ts_by_symbol.get(s, 0),
            reverse=True,
        )
        if scope_symbol is None and COIN_OUTCOME_MAX_SYMBOLS_PER_CYCLE > 0:
            ordered_symbols = ordered_symbols[:COIN_OUTCOME_MAX_SYMBOLS_PER_CYCLE]

        total_evaluated = 0
        total_skipped = 0
        total_deferred = 0
        errors = 0
        for symbol in ordered_symbols:
            try:
                result = await self.evaluate_coin_outcomes(
                    symbol,
                    horizons=list(COIN_OUTCOME_AUTO_HORIZONS),
                    decision_limit=1000,
                    force=False,
                    only_matured=True,
                    now_ms=now_ts_ms,
                )
                total_evaluated += int(result.get("evaluated") or 0)
                total_skipped += int(result.get("skipped") or 0)
                total_deferred += int(result.get("deferred") or 0)
            except Exception:  # pylint: disable=broad-except
                errors += 1
                logger.exception("Coin outcomes auto-cycle failed for %s", symbol)

        cycle = {
            "ts_ms": now_ts_ms,
            "symbols_total": len(pending_by_symbol),
            "symbols_processed": len(ordered_symbols),
            "evaluated": total_evaluated,
            "skipped": total_skipped,
            "deferred": total_deferred,
            "errors": errors,
            "scope_symbol": scope_symbol,
        }
        self._record_coin_outcomes_cycle(cycle)
        return cycle

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
        await self._restart_derisk_scheduler()
        await self._restart_coin_focus_scheduler()
        await self._restart_coin_outcomes_scheduler()
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

    def _load_auto_strategy_config(self) -> dict[str, Any]:
        raw = self._auto_strategy_store.load(
            {"version": 1, "defaults": dict(AUTO_STRATEGY_DEFAULTS), "strategies": {}}
        )
        if not isinstance(raw, Mapping):
            raw = {}
        defaults = dict(AUTO_STRATEGY_DEFAULTS)
        if isinstance(raw.get("defaults"), Mapping):
            defaults.update(raw["defaults"])
        strategies = raw.get("strategies")
        return {
            "version": 1,
            "defaults": defaults,
            "strategies": dict(strategies) if isinstance(strategies, Mapping) else {},
        }

    def _save_auto_strategy_config(self) -> None:
        self._auto_strategy_store.save(self._auto_strategies)

    def _auto_strategy_event(self, event: str, payload: Mapping[str, Any]) -> None:
        row = {
            "event": event,
            "ts": datetime.now(timezone.utc).isoformat(),
            **dict(payload),
        }
        self._auto_strategy_events.append(row)
        if len(self._auto_strategy_events) > self._auto_strategy_event_limit:
            self._auto_strategy_events = self._auto_strategy_events[-self._auto_strategy_event_limit :]
        self._auto_strategy_history_store.append(row)

    def auto_strategy_payload(self) -> dict[str, Any]:
        strategies = [
            dict(item)
            for item in (self._auto_strategies.get("strategies") or {}).values()
            if isinstance(item, Mapping)
        ]
        strategies.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        running = self._auto_exit_running_exec()
        running_detail = None
        if running:
            run = self._manual_runs.get(str(running.get("execution_id") or "")) or {}
            running_detail = {
                **running,
                "strategy_id": run.get("auto_strategy_id"),
                "step_id": run.get("auto_strategy_step_id"),
                "auto_exit_agent": bool(run.get("auto_exit_agent")),
                "auto_arb_agent": bool(run.get("auto_arb_agent")),
                "created_at": run.get("created_at"),
                "updated_at": run.get("updated_at"),
                "stage": ((run.get("logs") or [{}])[-1] or {}).get("event"),
                "message": ((run.get("logs") or [{}])[-1] or {}).get("message"),
            }
        return {
            "version": 1,
            "mode": "live",
            "defaults": dict(self._auto_strategies.get("defaults") or {}),
            "strategies": strategies,
            "queue": list(self._auto_strategy_queue),
            "running": running_detail,
            "events": list(self._auto_strategy_events),
            "legacy": {
                "spread_v1": self.auto_exit_payload(),
                "grid": self.auto_arb_payload(),
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def analyze_auto_strategy(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        strategy_type = str(payload.get("type") or "").strip().lower()
        if strategy_type not in {"enter_ladder", "exit_ladder"}:
            raise ValueError("Strategy type must be enter_ladder or exit_ladder.")
        symbol = normalize_symbol(str(payload.get("symbol") or "")).upper()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        raw_steps = payload.get("steps")
        if not symbol or not long_exchange or not short_exchange:
            raise ValueError("Symbol, long exchange and short exchange are required.")
        if not isinstance(raw_steps, list) or not raw_steps:
            raise ValueError("At least one strategy step is required.")
        action = "enter" if strategy_type == "enter_ladder" else "exit"
        hedged_qty = 0.0
        if action == "exit":
            hedged_qty = float(
                _position_pair_quantities(
                    self._accounts.snapshot().get("positions") or [],
                    symbol=symbol,
                    long_exchange=long_exchange,
                    short_exchange=short_exchange,
                ).get("hedged_qty")
                or 0.0
            )
        plans: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_steps):
            if not isinstance(raw, Mapping):
                raise ValueError(f"Step {index + 1} is invalid.")
            qty = _safe_float(raw.get("qty"))
            notional = _safe_float(raw.get("notional_usd"))
            percent = _safe_float(raw.get("percent"))
            if action == "exit" and percent and percent > 0:
                qty = hedged_qty * min(100.0, percent) / 100.0
                notional = None
            if not ((qty and qty > 0) or (notional and notional > 0)):
                raise ValueError(f"Step {index + 1}: unable to resolve qty or USDT notional.")
            plan = await self.manual_analyze(
                {
                    "action": action,
                    "mode": "smart-enter" if action == "enter" else "smart-exit",
                    "symbol": symbol,
                    "qty": qty,
                    "notional": notional,
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "max_slippage_bps": _safe_float(raw.get("max_slippage_bps")) or 8.0,
                    "use_orderbook_check": True,
                    "dry_run": True,
                    "async_run": False,
                    "margin_mode": "isolated",
                }
            )
            plans.append(
                {
                    "step": index + 1,
                    "requested_qty": qty,
                    "requested_notional_usd": notional,
                    "plan": plan,
                }
            )
        return {
            "type": strategy_type,
            "action": action,
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "hedged_qty": hedged_qty if action == "exit" else None,
            "steps": plans,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def upsert_auto_strategy(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        strategy_type = str(payload.get("type") or "").strip().lower()
        if strategy_type not in {"enter_ladder", "exit_ladder"}:
            raise ValueError("Strategy type must be enter_ladder or exit_ladder.")
        symbol = normalize_symbol(str(payload.get("symbol") or "")).upper()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        if not symbol or not long_exchange or not short_exchange:
            raise ValueError("Symbol, long exchange and short exchange are required.")
        if long_exchange == short_exchange:
            raise ValueError("Long and short exchanges must differ.")
        raw_steps = payload.get("steps")
        if not isinstance(raw_steps, list) or not raw_steps:
            raise ValueError("At least one strategy step is required.")
        action = "enter" if strategy_type == "enter_ladder" else "exit"
        now_iso = datetime.now(timezone.utc).isoformat()
        strategy_id = str(payload.get("id") or uuid4().hex[:12])
        async with self._auto_strategy_lock:
            existing = (self._auto_strategies.get("strategies") or {}).get(strategy_id)
            if isinstance(existing, Mapping):
                active = current_step(existing)
                if active and active.get("active_execution_id"):
                    raise ValueError("Wait for the active strategy execution before editing.")
            generation = max(1, int((existing or {}).get("generation") or 0) + 1)
            steps: list[dict[str, Any]] = []
            for index, raw in enumerate(raw_steps):
                if not isinstance(raw, Mapping):
                    raise ValueError(f"Step {index + 1} is invalid.")
                spread_target = _safe_float(raw.get("spread_target_pct"))
                if spread_target is None:
                    raise ValueError(f"Step {index + 1}: spread target is required.")
                qty = _safe_float(raw.get("qty"))
                notional = _safe_float(raw.get("notional_usd"))
                percent = _safe_float(raw.get("percent"))
                if action == "enter" and not ((qty and qty > 0) or (notional and notional > 0)):
                    raise ValueError(f"Step {index + 1}: enter qty or USDT notional is required.")
                if action == "exit" and not (
                    (qty and qty > 0)
                    or (notional and notional > 0)
                    or (percent and 0 < percent <= 100)
                ):
                    raise ValueError(f"Step {index + 1}: exit qty, USDT or percent is required.")
                step_id = str(raw.get("id") or f"{strategy_id}-{index + 1}")
                steps.append(
                    {
                        "id": step_id,
                        "index": index,
                        "action": action,
                        "spread_target_pct": float(spread_target),
                        "funding_min_pct": _safe_float(raw.get("funding_min_pct")),
                        "qty": float(qty) if qty and qty > 0 else None,
                        "notional_usd": float(notional) if notional and notional > 0 else None,
                        "percent": float(percent) if percent and percent > 0 else None,
                        "chunk_notional_usd": _safe_float(raw.get("chunk_notional_usd")),
                        "max_slippage_bps": _safe_float(raw.get("max_slippage_bps")) or 8.0,
                        "max_runtime_sec": int(
                            _safe_float(raw.get("max_runtime_sec"))
                            or (self._auto_strategies.get("defaults") or {}).get("max_runtime_sec")
                            or 120
                        ),
                        "target_qty": None,
                        "filled_qty": 0.0,
                        "remaining_qty": None,
                        "baseline_hedged_qty": None,
                        "status": "waiting",
                        "active_execution_id": None,
                        "last_trigger": None,
                        "last_result": None,
                        "waiting_since_ts": time.time(),
                    }
                )
            strategy = {
                "id": strategy_id,
                "generation": generation,
                "type": strategy_type,
                "action": action,
                "name": str(payload.get("name") or f"{action.upper()} {symbol}"),
                "symbol": symbol,
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "enabled": bool(payload.get("enabled", True)),
                "status": "waiting",
                "steps": steps,
                "created_at": (existing or {}).get("created_at") or now_iso,
                "updated_at": now_iso,
            }
            self._auto_strategies.setdefault("strategies", {})[strategy_id] = strategy
            self._save_auto_strategy_config()
        self._auto_strategy_event(
            "strategy_saved",
            {"strategy_id": strategy_id, "type": strategy_type, "symbol": symbol},
        )
        return self.auto_strategy_payload()

    async def set_auto_strategy_enabled(self, strategy_id: str, enabled: bool) -> dict[str, Any]:
        async with self._auto_strategy_lock:
            strategy = (self._auto_strategies.get("strategies") or {}).get(strategy_id)
            if not isinstance(strategy, dict):
                raise ValueError("Strategy not found.")
            step = current_step(strategy)
            if not enabled and step and step.get("active_execution_id"):
                raise ValueError("Stop the active execution before pausing the strategy.")
            strategy["enabled"] = bool(enabled)
            strategy["status"] = "waiting" if enabled else "paused"
            strategy["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._save_auto_strategy_config()
        self._auto_strategy_event(
            "strategy_enabled" if enabled else "strategy_paused",
            {"strategy_id": strategy_id},
        )
        return self.auto_strategy_payload()

    async def delete_auto_strategy(self, strategy_id: str) -> dict[str, Any]:
        async with self._auto_strategy_lock:
            strategy = (self._auto_strategies.get("strategies") or {}).get(strategy_id)
            if not isinstance(strategy, Mapping):
                raise ValueError("Strategy not found.")
            step = current_step(strategy)
            if step and step.get("active_execution_id"):
                raise ValueError("Stop the active execution before deleting the strategy.")
            self._auto_strategies.setdefault("strategies", {}).pop(strategy_id, None)
            self._save_auto_strategy_config()
        self._auto_strategy_event("strategy_deleted", {"strategy_id": strategy_id})
        return self.auto_strategy_payload()

    async def _auto_strategy_funding_delta_pct(
        self,
        *,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
    ) -> float | None:
        rates: dict[str, float | None] = {}
        for exchange in (long_exchange, short_exchange):
            cached = self._positions_market_cache.get((exchange, symbol))
            rate = _safe_float(getattr(cached, "funding_rate", None)) if cached else None
            if rate is None:
                try:
                    adapter = get_adapter_cached(exchange)
                    snapshots = await adapter.fetch_market_snapshots_async([symbol])
                except Exception:  # pylint: disable=broad-except
                    snapshots = []
                for snapshot in snapshots or []:
                    if isinstance(snapshot, MarketSnapshot) and normalize_symbol(snapshot.symbol) == symbol:
                        rate = _safe_float(snapshot.funding_rate)
                        break
            rates[exchange] = rate
        long_rate = rates.get(long_exchange)
        short_rate = rates.get(short_exchange)
        if long_rate is None or short_rate is None:
            return None
        return (float(short_rate) - float(long_rate)) * 100.0

    def _auto_strategy_pair_quantities(self, strategy: Mapping[str, Any]) -> dict[str, float]:
        return _position_pair_quantities(
            self._accounts.snapshot().get("positions") or [],
            symbol=str(strategy.get("symbol") or ""),
            long_exchange=str(strategy.get("long_exchange") or ""),
            short_exchange=str(strategy.get("short_exchange") or ""),
        )

    def _auto_strategy_step_ref(
        self,
        strategy: dict[str, Any],
        step_id: str,
    ) -> dict[str, Any] | None:
        for step in strategy.get("steps") or []:
            if isinstance(step, dict) and str(step.get("id") or "") == step_id:
                return step
        return None

    async def _reconcile_auto_strategy_execution(
        self,
        strategy_id: str,
        step_id: str,
        execution_id: str,
    ) -> None:
        run = self._manual_runs.get(execution_id)
        if not isinstance(run, Mapping) or str(run.get("status") or "") == "running":
            return
        try:
            await self._accounts.refresh_now(force_env=True)
        except Exception:  # pylint: disable=broad-except
            pass
        async with self._auto_strategy_lock:
            strategy = (self._auto_strategies.get("strategies") or {}).get(strategy_id)
            if not isinstance(strategy, dict):
                return
            step = self._auto_strategy_step_ref(strategy, step_id)
            if not isinstance(step, dict) or str(step.get("active_execution_id") or "") != execution_id:
                return
            quantities = self._auto_strategy_pair_quantities(strategy)
            current_qty = float(quantities.get("hedged_qty") or 0.0)
            baseline_qty = float(step.get("baseline_hedged_qty") or 0.0)
            action = str(strategy.get("action") or "")
            observed_filled = (
                max(0.0, current_qty - baseline_qty)
                if action == "enter"
                else max(0.0, baseline_qty - current_qty)
            )
            tolerance_pct = float(
                (self._auto_strategies.get("defaults") or {}).get("completion_tolerance_pct")
                or 1.0
            )
            reconciled = reconcile_step_progress(
                step,
                observed_filled_qty=observed_filled,
                tolerance_pct=tolerance_pct,
            )
            step.update(reconciled)
            step["active_execution_id"] = None
            step["last_result"] = {
                "execution_id": execution_id,
                "status": run.get("status"),
                "error": run.get("error"),
                "result": run.get("result"),
                "observed_hedged_qty": current_qty,
                "observed_filled_qty": observed_filled,
            }
            step["updated_at"] = datetime.now(timezone.utc).isoformat()
            step["next_eligible_ts"] = time.time() + 2.0
            if str(step.get("status")) in {"completed", "completed_with_dust"}:
                next_step = current_step(strategy)
                strategy["status"] = "completed" if next_step is None else "waiting"
            else:
                errors = []
                result = run.get("result")
                if isinstance(result, Mapping):
                    errors = [str(item) for item in (result.get("errors") or [])]
                if errors and not observed_filled:
                    joined = " ".join(errors).lower()
                    step["status"] = "blocked_balance" if "balance" in joined else "waiting"
                    retry_sec = (
                        int((self._auto_strategies.get("defaults") or {}).get("balance_retry_sec") or 60)
                        if step["status"] == "blocked_balance"
                        else 2
                    )
                    step["next_eligible_ts"] = time.time() + retry_sec
                strategy["status"] = str(step.get("status") or "waiting")
            strategy["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._save_auto_strategy_config()
            event_payload = {
                "strategy_id": strategy_id,
                "step_id": step_id,
                "execution_id": execution_id,
                "status": step.get("status"),
                "target_qty": step.get("target_qty"),
                "filled_qty": step.get("filled_qty"),
                "remaining_qty": step.get("remaining_qty"),
                "completion_tolerance_qty": step.get("completion_tolerance_qty"),
            }
        self._auto_strategy_event("step_reconciled", event_payload)

    async def _start_auto_strategy_step(
        self,
        strategy_id: str,
        step_id: str,
        trigger: Mapping[str, Any],
    ) -> None:
        if self._auto_exit_running_exec():
            return
        try:
            await self._accounts.refresh_now(force_env=True)
        except Exception:  # pylint: disable=broad-except
            pass
        async with self._auto_strategy_lock:
            strategy = (self._auto_strategies.get("strategies") or {}).get(strategy_id)
            if not isinstance(strategy, dict) or not strategy.get("enabled"):
                return
            step = self._auto_strategy_step_ref(strategy, step_id)
            if not isinstance(step, dict) or step.get("active_execution_id"):
                return
            quantities = self._auto_strategy_pair_quantities(strategy)
            hedged_qty = float(quantities.get("hedged_qty") or 0.0)
            reference_price = max(
                [
                    value
                    for value in (
                        _safe_float(trigger.get("long_mid")),
                        _safe_float(trigger.get("short_mid")),
                    )
                    if value and value > 0
                ],
                default=None,
            )
            target_qty = _safe_float(step.get("target_qty"))
            if target_qty is None:
                requested_qty = _safe_float(step.get("qty"))
                requested_notional = _safe_float(step.get("notional_usd"))
                requested_percent = _safe_float(step.get("percent"))
                if requested_qty and requested_qty > 0:
                    target_qty = requested_qty
                elif requested_notional and reference_price:
                    target_qty = requested_notional / reference_price
                elif str(strategy.get("action")) == "exit" and requested_percent:
                    target_qty = hedged_qty * min(100.0, requested_percent) / 100.0
                if not target_qty or target_qty <= 0:
                    step["status"] = "blocked_minimum"
                    step["last_result"] = {"errors": ["Unable to resolve target quantity."]}
                    strategy["status"] = "blocked_minimum"
                    self._save_auto_strategy_config()
                    return
                if str(strategy.get("action")) == "exit":
                    target_qty = min(float(target_qty), hedged_qty)
                step["target_qty"] = float(target_qty)
                step["remaining_qty"] = float(target_qty)
                step["baseline_hedged_qty"] = hedged_qty
            remaining_qty = max(
                0.0,
                _safe_float(step.get("remaining_qty"))
                if step.get("remaining_qty") is not None
                else float(target_qty),
            )
            if str(strategy.get("action")) == "exit":
                remaining_qty = min(remaining_qty, hedged_qty)
            if remaining_qty <= 0:
                step.update(
                    reconcile_step_progress(
                        step,
                        observed_filled_qty=float(step.get("target_qty") or 0.0),
                        tolerance_pct=float(
                            (self._auto_strategies.get("defaults") or {}).get(
                                "completion_tolerance_pct", 1.0
                            )
                        ),
                    )
                )
                self._save_auto_strategy_config()
                return
            worst_tier = max(
                venue_liquidity_tier(str(strategy.get("long_exchange") or "")),
                venue_liquidity_tier(str(strategy.get("short_exchange") or "")),
            )
            default_chunk_notional = 750.0 if worst_tier <= 1 else 500.0 if worst_tier == 2 else 250.0
            action = str(strategy.get("action") or "")
            spread_target = float(step.get("spread_target_pct"))
            run_payload = {
                "symbol": strategy.get("symbol"),
                "qty": remaining_qty,
                "notional": None,
                "mode": "smart-enter" if action == "enter" else "smart-exit",
                "max_slippage_bps": float(step.get("max_slippage_bps") or 8.0),
                "spread_min_pct": -100.0 if action == "enter" else spread_target,
                "spread_max_pct": spread_target if action == "enter" else 100.0,
                "timeout_sec": 0,
                "max_runtime_sec": int(step.get("max_runtime_sec") or 120),
                "reprice_sec": 5,
                "chunk_qty": None,
                "chunk_notional": _safe_float(step.get("chunk_notional_usd"))
                or default_chunk_notional,
                "force_chunk_qty": False,
                "use_orderbook_check": True,
                "fallback_to_market": False,
                "hedge_order_type": "limit",
                "hedge_limit_mode": "passive",
                "async_run": True,
                "dry_run": False,
                "long_exchange": strategy.get("long_exchange"),
                "short_exchange": strategy.get("short_exchange"),
                "margin_mode": "isolated",
                "auto_strategy_agent": True,
                "auto_strategy_id": strategy_id,
                "auto_strategy_step_id": step_id,
                "auto_strategy_generation": int(strategy.get("generation") or 1),
            }
            step["status"] = "queued"
            step["last_trigger"] = dict(trigger)
            strategy["status"] = "queued"
            self._save_auto_strategy_config()
        result = (
            await self.manual_enter(run_payload)
            if action == "enter"
            else await self.manual_exit(run_payload)
        )
        execution_id = str((result or {}).get("execution_id") or "")
        async with self._auto_strategy_lock:
            strategy = (self._auto_strategies.get("strategies") or {}).get(strategy_id)
            if not isinstance(strategy, dict):
                return
            step = self._auto_strategy_step_ref(strategy, step_id)
            if not isinstance(step, dict):
                return
            if execution_id:
                step["active_execution_id"] = execution_id
                step["status"] = "executing"
                strategy["status"] = "executing"
            else:
                step["status"] = "blocked_conflict"
                step["last_result"] = dict(result or {})
                step["next_eligible_ts"] = time.time() + 2.0
                strategy["status"] = "blocked_conflict"
            strategy["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._save_auto_strategy_config()
        self._auto_strategy_event(
            "step_started" if execution_id else "step_start_blocked",
            {
                "strategy_id": strategy_id,
                "step_id": step_id,
                "execution_id": execution_id or None,
                "qty": remaining_qty,
                "trigger": dict(trigger),
                "result": result,
            },
        )

    async def _auto_strategy_cycle(self) -> None:
        async with self._auto_strategy_lock:
            snapshots = [
                dict(strategy)
                for strategy in (self._auto_strategies.get("strategies") or {}).values()
                if isinstance(strategy, Mapping) and strategy.get("enabled")
            ]
        for strategy in snapshots:
            step = current_step(strategy)
            if not step:
                continue
            execution_id = str(step.get("active_execution_id") or "")
            if execution_id:
                await self._reconcile_auto_strategy_execution(
                    str(strategy.get("id") or ""),
                    str(step.get("id") or ""),
                    execution_id,
                )
        if self._auto_exit_running_exec():
            self._auto_strategy_queue = []
            return

        candidates: list[StrategyCandidate] = []
        candidate_data: dict[tuple[str, str], dict[str, Any]] = {}
        for strategy in snapshots:
            strategy_id = str(strategy.get("id") or "")
            step = current_step(strategy)
            if not step or step.get("active_execution_id"):
                continue
            if time.time() < float(step.get("next_eligible_ts") or 0.0):
                continue
            spreads = await self.auto_arb_spreads(
                symbol=str(strategy.get("symbol") or ""),
                long_exchange=str(strategy.get("long_exchange") or ""),
                short_exchange=str(strategy.get("short_exchange") or ""),
            )
            action = str(strategy.get("action") or "")
            spread_value = _safe_float(
                spreads.get("entry_spread_pct") if action == "enter" else spreads.get("exit_spread_pct")
            )
            funding_delta = await self._auto_strategy_funding_delta_pct(
                symbol=str(strategy.get("symbol") or ""),
                long_exchange=str(strategy.get("long_exchange") or ""),
                short_exchange=str(strategy.get("short_exchange") or ""),
            )
            target = _safe_float(step.get("spread_target_pct"))
            matched, reason = trigger_matches(
                action=action,
                spread_pct=spread_value,
                spread_target_pct=target,
                funding_delta_pct=funding_delta,
                funding_min_pct=_safe_float(step.get("funding_min_pct")),
            )
            trigger = {
                "matched": matched,
                "reason": reason,
                "spread_pct": spread_value,
                "spread_target_pct": target,
                "funding_delta_pct": funding_delta,
                "funding_min_pct": _safe_float(step.get("funding_min_pct")),
                "long_mid": _safe_float((spreads.get("long_quote") or {}).get("mid")),
                "short_mid": _safe_float((spreads.get("short_quote") or {}).get("mid")),
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
            async with self._auto_strategy_lock:
                current = (self._auto_strategies.get("strategies") or {}).get(strategy_id)
                if isinstance(current, dict):
                    current_step_ref = self._auto_strategy_step_ref(
                        current, str(step.get("id") or "")
                    )
                    if current_step_ref:
                        current_step_ref["last_trigger"] = trigger
                        if not matched and current_step_ref.get("status") not in {
                            "partial",
                            "blocked_balance",
                        }:
                            current_step_ref["status"] = "waiting"
                        current["status"] = str(current_step_ref.get("status") or "waiting")
                        current["updated_at"] = datetime.now(timezone.utc).isoformat()
                        self._save_auto_strategy_config()
            if not matched or spread_value is None or target is None:
                continue
            candidate = StrategyCandidate(
                strategy_id=strategy_id,
                step_id=str(step.get("id") or ""),
                action=action,
                priority=action_priority(action, str(strategy.get("type") or "")),
                edge=trigger_edge(
                    action=action,
                    spread_pct=float(spread_value),
                    spread_target_pct=float(target),
                ),
                waiting_since_ts=float(step.get("waiting_since_ts") or time.time()),
            )
            candidates.append(candidate)
            candidate_data[(candidate.strategy_id, candidate.step_id)] = trigger
        ordered = sorted(
            candidates,
            key=lambda item: (
                item.priority,
                -item.edge,
                item.waiting_since_ts,
                item.strategy_id,
                item.step_id,
            ),
        )
        self._auto_strategy_queue = [
            {
                "strategy_id": item.strategy_id,
                "step_id": item.step_id,
                "action": item.action,
                "priority": item.priority,
                "edge": item.edge,
            }
            for item in ordered
        ]
        selected = choose_candidate(candidates)
        if selected:
            await self._start_auto_strategy_step(
                selected.strategy_id,
                selected.step_id,
                candidate_data[(selected.strategy_id, selected.step_id)],
            )

    async def _auto_strategy_scheduler(self) -> None:
        while True:
            try:
                await self._auto_strategy_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("auto-strategy loop failed: %s", exc)
            await asyncio.sleep(max(1.0, self._auto_strategy_poll_sec))

    def _load_auto_arb_config(self) -> dict[str, Any]:
        raw = self._auto_arb_store.load({"version": 1, "rules": {}})
        if not isinstance(raw, Mapping):
            raw = {}
        rules = raw.get("rules")
        return {
            "version": 1,
            "rules": dict(rules) if isinstance(rules, Mapping) else {},
        }

    def _save_auto_arb_config(self) -> None:
        self._auto_arb_store.save(self._auto_arb)

    def auto_arb_payload(self) -> dict[str, Any]:
        rules = list((self._auto_arb.get("rules") or {}).values())
        rules.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return {
            "version": 1,
            "mode": "shadow_and_restricted_live",
            "live_limits": {
                "max_chunk_notional_usd": AUTO_ARB_LIVE_MAX_CHUNK_NOTIONAL_USD,
                "max_total_notional_usd": AUTO_ARB_LIVE_MAX_TOTAL_NOTIONAL_USD,
                "max_live_rules": 1,
            },
            "rules": rules,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

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

        range_start = _safe_float(payload.get("range_start_pct"))
        range_end = _safe_float(payload.get("range_end_pct"))
        if range_start is None or range_end is None:
            raise ValueError("range_start_pct and range_end_pct are required.")
        budget_mode = str(payload.get("budget_mode") or "qty").strip().lower()
        if budget_mode not in {"qty", "notional"}:
            raise ValueError("budget_mode must be qty or notional.")
        max_qty = _safe_float(payload.get("max_qty"))
        max_notional = _safe_float(payload.get("max_notional"))
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
        warnings: list[str] = []
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
        requested_count = payload.get("level_count")
        fallback_count = normalize_level_count(requested_count or 6)
        safe_chunk = min(safe_candidates) * liquidity_factor if safe_candidates else total_qty / fallback_count
        if min_chunk_candidates:
            safe_chunk = max(safe_chunk, max(min_chunk_candidates))
        safe_chunk = min(float(total_qty), safe_chunk)
        level_count = (
            normalize_level_count(requested_count)
            if requested_count
            else recommend_level_count(total_qty=total_qty, safe_chunk_qty=safe_chunk)
        )
        chunk_qty = float(total_qty) / level_count
        exit_gap = _safe_float(payload.get("exit_gap_pct"))
        if exit_gap is None or exit_gap <= 0:
            exit_gap = max(0.25, max_slippage_bps * 4.0 / 100.0)
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
        grid_step = abs(float(range_start) - float(range_end)) / (level_count - 1)
        if grid_step <= exit_gap:
            warnings.append(
                "Grid step is not wider than the exit gap; reduce level count or exit gap."
            )
        if not safe_candidates:
            warnings.append("Dry run did not return a safe chunk; budget/count fallback was used.")

        config = {
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "direction": "negative_expansion",
            "budget_mode": budget_mode,
            "max_qty": float(total_qty),
            "max_notional": float(max_notional) if max_notional else None,
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
            "max_slippage_bps": max_slippage_bps,
            "liquidity_safety_factor": liquidity_factor,
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
            if not enabled and rule.get("active_execution_id"):
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

    def _auto_arb_auto_exit_conflict(self, rule: Mapping[str, Any]) -> bool:
        symbol = normalize_symbol(str(rule.get("symbol") or "")).upper()
        long_exchange = normalize_exchange_name(str(rule.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(rule.get("short_exchange") or ""))
        for candidate in (self._auto_exit.get("rules") or {}).values():
            if not isinstance(candidate, Mapping):
                continue
            if not bool(candidate.get("enabled") or candidate.get("v1_enabled")):
                continue
            if normalize_symbol(str(candidate.get("symbol") or "")).upper() != symbol:
                continue
            candidate_long = normalize_exchange_name(str(candidate.get("long_exchange") or ""))
            candidate_short = normalize_exchange_name(str(candidate.get("short_exchange") or ""))
            if candidate_long == long_exchange and candidate_short == short_exchange:
                return True
        return False

    @staticmethod
    def _auto_arb_level_for_qty(rule: Mapping[str, Any], hedged_qty: float) -> int | None:
        qty = max(0.0, float(hedged_qty or 0.0))
        total_qty = max(0.0, float(rule.get("max_qty") or 0.0))
        tolerance = max(1e-8, total_qty * 1e-5)
        if qty <= tolerance:
            return 0
        for level in rule.get("levels") or []:
            cumulative = float(level.get("cumulative_qty") or 0.0)
            if abs(qty - cumulative) <= tolerance:
                return int(level.get("level") or 0)
        return None

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

    async def arm_auto_arb_live(self, rule_id: str, confirmation: str) -> dict[str, Any]:
        expected_confirmation = f"LIVE {rule_id}"
        if str(confirmation or "").strip() != expected_confirmation:
            raise ValueError(f"Type '{expected_confirmation}' to enable restricted Live mode.")
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                raise ValueError("Auto-arbitrage rule not found.")
            if rule.get("active_execution_id"):
                raise ValueError("The grid already has an active execution.")
            for candidate in (self._auto_arb.get("rules") or {}).values():
                if (
                    isinstance(candidate, Mapping)
                    and candidate.get("id") != rule_id
                    and candidate.get("mode") == "live"
                    and candidate.get("enabled")
                ):
                    raise ValueError("Only one restricted Live grid may be enabled.")
            rule_copy = dict(rule)

        chunk_notional = _safe_float(rule_copy.get("chunk_notional_estimate"))
        total_notional = _safe_float(rule_copy.get("total_notional_estimate"))
        if chunk_notional is None or total_notional is None:
            raise ValueError("Live mode requires a current reference price and notional estimate.")
        if chunk_notional > AUTO_ARB_LIVE_MAX_CHUNK_NOTIONAL_USD + 1e-9:
            raise ValueError(
                f"Chunk estimate {chunk_notional:.2f} USDT exceeds the restricted Live limit "
                f"of {AUTO_ARB_LIVE_MAX_CHUNK_NOTIONAL_USD:.2f} USDT."
            )
        if total_notional > AUTO_ARB_LIVE_MAX_TOTAL_NOTIONAL_USD + 1e-9:
            raise ValueError(
                f"Total estimate {total_notional:.2f} USDT exceeds the restricted Live limit "
                f"of {AUTO_ARB_LIVE_MAX_TOTAL_NOTIONAL_USD:.2f} USDT."
            )
        if self._auto_arb_auto_exit_conflict(rule_copy):
            raise ValueError("Disable the matching Auto Exit rule before enabling Grid Live.")
        if self._auto_exit_running_exec():
            raise ValueError("Another manual or automatic execution is currently running.")

        try:
            quantities = await self._auto_arb_refresh_quantities(rule_copy)
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError(f"Unable to refresh positions before Live activation: {exc}") from exc
        live_level = self._auto_arb_level_for_qty(
            rule_copy,
            float(quantities.get("hedged_qty") or 0.0),
        )
        if live_level is None:
            raise ValueError(
                "The existing hedged quantity does not match a grid level. "
                "Flatten it or configure a grid that matches the real position."
            )
        imbalance_qty = float(quantities.get("imbalance_qty") or 0.0)
        tolerance = max(1e-8, float(rule_copy.get("max_qty") or 0.0) * 1e-5)
        if imbalance_qty > tolerance:
            raise ValueError(
                "Long and short quantities are imbalanced; Live Grid cannot take ownership."
            )

        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._auto_arb_lock:
            rule = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(rule, dict):
                raise ValueError("Auto-arbitrage rule not found.")
            rule["mode"] = "live"
            rule["enabled"] = True
            rule["live_level"] = int(live_level)
            rule["actual_hedged_qty"] = float(quantities.get("hedged_qty") or 0.0)
            rule["status"] = "waiting_entry" if live_level == 0 else "monitoring"
            rule["blocked_reason"] = None
            rule["pending_action"] = None
            rule["pending_samples"] = 0
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
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    current["enabled"] = False
                    current["status"] = "error"
                    current["blocked_reason"] = "active_execution_state_missing"
                    current["updated_at"] = datetime.now(timezone.utc).isoformat()
                    self._save_auto_arb_config()
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
        success = status == "completed" and not (
            isinstance(run.get("result"), Mapping) and run["result"].get("errors")
        )
        quantities: dict[str, float] | None = None
        reconcile_error = None
        if success:
            try:
                quantities = await self._auto_arb_refresh_quantities(rule_copy)
            except Exception as exc:  # pylint: disable=broad-except
                reconcile_error = str(exc)
                success = False

        async with self._auto_arb_lock:
            current = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(current, dict):
                return False
            current["active_execution_id"] = None
            current["active_action"] = None
            current["active_from_level"] = None
            current["active_to_level"] = None
            expected_qty = _safe_float(current.get("active_target_qty"))
            current["active_target_qty"] = None
            if success and quantities is not None:
                hedged_qty = float(quantities.get("hedged_qty") or 0.0)
                imbalance_qty = float(quantities.get("imbalance_qty") or 0.0)
                tolerance = max(1e-8, float(current.get("max_qty") or 0.0) * 1e-5)
                matched_level = self._auto_arb_level_for_qty(current, hedged_qty)
                target_level = int(rule_copy.get("active_to_level") or 0)
                target_matches = matched_level == target_level
                qty_matches = expected_qty is None or abs(hedged_qty - expected_qty) <= tolerance
                if target_matches and qty_matches and imbalance_qty <= tolerance:
                    current["live_level"] = target_level
                    current["actual_hedged_qty"] = hedged_qty
                    current["status"] = "waiting_entry" if target_level == 0 else "monitoring"
                    current["blocked_reason"] = None
                    event["event"] = f"live_{rule_copy.get('active_action')}"
                    event["live_level"] = target_level
                    event["actual_hedged_qty"] = hedged_qty
                else:
                    current["enabled"] = False
                    current["status"] = "paused_reconcile_mismatch"
                    current["blocked_reason"] = "actual_position_does_not_match_target_level"
                    current["actual_hedged_qty"] = hedged_qty
                    event["event"] = "live_reconcile_mismatch"
                    event["matched_level"] = matched_level
                    event["actual_hedged_qty"] = hedged_qty
                    event["imbalance_qty"] = imbalance_qty
                    success = False
            else:
                current["enabled"] = False
                current["status"] = "error"
                current["blocked_reason"] = (
                    f"position_refresh_failed: {reconcile_error}"
                    if reconcile_error
                    else f"execution_{status or 'unknown'}"
                )
                event["event"] = "live_execution_failed"
                event["error"] = reconcile_error or run.get("error")
                event["result"] = run.get("result")
            current["updated_at"] = now_iso
            self._save_auto_arb_config()
        self._auto_arb_history_store.append(event)
        return success

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
        running = self._auto_exit_running_exec()
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
        if self._auto_arb_auto_exit_conflict(rule_copy):
            async with self._auto_arb_lock:
                current = (self._auto_arb.get("rules") or {}).get(rule_id)
                if isinstance(current, dict):
                    current["status"] = "blocked_conflict"
                    current["blocked_reason"] = "matching_auto_exit_rule_enabled"
                    current["pending_action"] = None
                    current["pending_samples"] = 0
                    current["updated_at"] = datetime.now(timezone.utc).isoformat()
                    self._save_auto_arb_config()
            return

        levels = rule_copy.get("levels") or []
        level_index = to_level - 1 if action == "enter" else from_level - 1
        if level_index < 0 or level_index >= len(levels):
            raise ValueError("Grid transition level is outside the configured range.")
        qty = float(levels[level_index].get("qty") or 0.0)
        target_qty = (
            float(levels[to_level - 1].get("cumulative_qty") or 0.0)
            if to_level > 0
            else 0.0
        )
        payload = {
            "symbol": rule_copy.get("symbol"),
            "qty": qty,
            "notional": None,
            "mode": "smart-enter" if action == "enter" else "smart-exit",
            "max_slippage_bps": float(rule_copy.get("max_slippage_bps") or 8.0),
            "timeout_sec": 15,
            "max_runtime_sec": 600,
            "reprice_sec": 5.0,
            "chunk_qty": qty,
            "chunk_notional": None,
            "force_chunk_qty": True,
            "use_orderbook_check": True,
            "fallback_to_market": False,
            "async_run": True,
            "dry_run": False,
            "long_exchange": rule_copy.get("long_exchange"),
            "short_exchange": rule_copy.get("short_exchange"),
            "margin_mode": "isolated",
            "auto_arb_agent": True,
            "auto_arb_rule_id": rule_id,
            "auto_arb_rule_generation": int(rule_copy.get("generation") or 0),
        }
        result = (
            await self.manual_enter(payload)
            if action == "enter"
            else await self.manual_exit(payload)
        )
        exec_id = str((result or {}).get("execution_id") or "")
        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._auto_arb_lock:
            current = (self._auto_arb.get("rules") or {}).get(rule_id)
            if not isinstance(current, dict):
                return
            current["pending_action"] = None
            current["pending_samples"] = 0
            if exec_id:
                current["active_execution_id"] = exec_id
                current["active_action"] = action
                current["active_from_level"] = from_level
                current["active_to_level"] = to_level
                current["active_target_qty"] = target_qty
                current["status"] = f"executing_{action}"
                current["blocked_reason"] = None
            else:
                current["enabled"] = False
                current["status"] = "error"
                current["blocked_reason"] = "manual_execution_did_not_return_execution_id"
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
                "result": result,
                "ts": now_iso,
            }
        )

    async def _auto_arb_cycle(self) -> None:
        async with self._auto_arb_lock:
            rules = [
                dict(rule)
                for rule in (self._auto_arb.get("rules") or {}).values()
                if isinstance(rule, dict) and rule.get("enabled")
            ]
        for rule in rules:
            rule_id = str(rule.get("id") or "")
            if rule.get("mode") == "live" and rule.get("active_execution_id"):
                await self._reconcile_auto_arb_execution(rule_id)
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
                    decision = decide_grid_transition(
                        entry_spread_pct=entry_spread,
                        exit_spread_pct=exit_spread,
                        levels=current.get("levels") or [],
                        current_level=current_level,
                        max_levels_per_cycle=current.get("max_levels_per_cycle") or 1,
                    )
                    action = decision["action"]
                    current["last_decision"] = decision
                    current["blocked_reason"] = None
                    if action == "none":
                        current["pending_action"] = None
                        current["pending_samples"] = 0
                        current["status"] = "waiting_entry" if not current_level else "monitoring"
                    else:
                        if current.get("pending_action") == action:
                            current["pending_samples"] = int(current.get("pending_samples") or 0) + 1
                        else:
                            current["pending_action"] = action
                            current["pending_samples"] = 1
                        current["status"] = f"confirming_{action}"
                        required = max(1, int(current.get("confirm_samples") or 2))
                        if int(current["pending_samples"]) >= required:
                            previous_level = current_level
                            new_level = int(decision["target_level"])
                            if mode == "live":
                                current["status"] = f"queued_{action}"
                                live_transition = (rule_id, action, previous_level, new_level)
                            else:
                                current["shadow_level"] = new_level
                                levels = current.get("levels") or []
                                current["shadow_qty"] = (
                                    float(levels[new_level - 1].get("cumulative_qty") or 0.0)
                                    if new_level > 0 and new_level <= len(levels)
                                    else 0.0
                                )
                                current["status"] = f"shadow_{action}"
                                current["pending_action"] = None
                                current["pending_samples"] = 0
                                transition_event = {
                                    "event": f"shadow_{action}",
                                    "rule_id": current.get("id"),
                                    "generation": current.get("generation"),
                                    "symbol": current.get("symbol"),
                                    "long_exchange": current.get("long_exchange"),
                                    "short_exchange": current.get("short_exchange"),
                                    "from_level": previous_level,
                                    "to_level": new_level,
                                    "shadow_qty": current.get("shadow_qty"),
                                    "entry_spread_pct": entry_spread,
                                    "exit_spread_pct": exit_spread,
                                    "ts": now_iso,
                                }
                current["updated_at"] = now_iso
                self._save_auto_arb_config()
            if transition_event:
                self._auto_arb_history_store.append(transition_event)
            if live_transition:
                await self._start_auto_arb_live_transition(*live_transition)

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
            "max_slippage_bps": 8.0,
            "timeout_sec": 15,
            "max_runtime_sec": 600,
            "reprice_sec": 5.0,
            "chunk_qty": None,
            "chunk_notional": None,
            "force_chunk_qty": False,
            "use_orderbook_check": True,
            "fallback_to_market": False,
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
            "stop_force_finalize": bool(run.get("stop_force_finalize")),
            "stop_reason": run.get("stop_reason") or None,
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
        running = self._auto_exit_running_exec()
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
            "auto_exit_agent": bool((payload or {}).get("auto_exit_agent")),
            "payload_symbol": normalize_symbol(str((payload or {}).get("symbol") or "")),
            "auto_exit_rule_key": str((payload or {}).get("auto_exit_rule_key") or ""),
            "auto_exit_rule_generation": max(
                0,
                int((payload or {}).get("auto_exit_rule_generation") or 0),
            ),
            "auto_exit_trigger_mode": str((payload or {}).get("auto_exit_trigger_mode") or ""),
            "auto_exit_exit_percent": _safe_float((payload or {}).get("auto_exit_exit_percent")),
            "auto_exit_hedged_qty": _safe_float((payload or {}).get("auto_exit_hedged_qty")),
            "auto_exit_requested_qty": _safe_float((payload or {}).get("auto_exit_requested_qty")),
            "auto_arb_agent": bool((payload or {}).get("auto_arb_agent")),
            "auto_arb_rule_id": str((payload or {}).get("auto_arb_rule_id") or ""),
            "auto_arb_rule_generation": max(
                0,
                int((payload or {}).get("auto_arb_rule_generation") or 0),
            ),
            "auto_strategy_agent": bool((payload or {}).get("auto_strategy_agent")),
            "auto_strategy_id": str((payload or {}).get("auto_strategy_id") or ""),
            "auto_strategy_step_id": str((payload or {}).get("auto_strategy_step_id") or ""),
            "auto_strategy_generation": max(
                0,
                int((payload or {}).get("auto_strategy_generation") or 0),
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
        if expired:
            self._auto_exit_completed_run_cleanup.intersection_update(self._manual_runs.keys())

    def latest_snapshot(self) -> Optional[DataSnapshot]:
        return self._snapshot

    def _latest_snapshot_dict_cached(self) -> dict[str, object] | None:
        snapshot = self._snapshot
        if snapshot is None:
            self._snapshot_dict_cache_key = None
            self._snapshot_dict_cache = None
            return None
        cache_key = id(snapshot)
        if self._snapshot_dict_cache_key != cache_key or self._snapshot_dict_cache is None:
            self._snapshot_dict_cache = snapshot.as_dict()
            self._snapshot_dict_cache_key = cache_key
        return self._snapshot_dict_cache

    def latest_snapshot_dict(self) -> dict[str, object] | None:
        return self._latest_snapshot_dict_cached()

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

    def state_payload(self) -> dict[str, object]:
        snapshot_dict = self._latest_snapshot_dict_cached()
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
            "auto_arb": self.auto_arb_payload(),
            "auto_strategies": self.auto_strategy_payload(),
            "emergency_derisk": self.derisk_payload(),
            "coin_analysis": {
                "focus_poll_sec": self._coin_focus_poll_sec,
                "shortlist_poll_sec": self._coin_shortlist_poll_sec,
                "shortlist_last_cycle": dict(self._coin_shortlist_last_cycle or {}),
                "outcomes_poll_sec": self._coin_outcomes_poll_sec,
                "outcomes_scheduler_enabled": bool(self._coin_outcomes_scheduler_enabled),
                "outcomes_last_cycle": dict(self._coin_outcomes_last_cycle),
                "outcomes_recent_cycles": list(self._coin_outcomes_cycle_history[-10:]),
                "position_watcher_poll_sec": self._coin_position_watcher_poll_sec,
                "position_watcher_enabled": bool(self._coin_position_watcher_enabled),
                "position_watcher_last_cycle": dict(self._coin_position_watcher_last_cycle or {}),
                "retention_poll_sec": self._coin_retention_poll_sec,
                "retention_max_age_days": self._coin_retention_max_age_days,
                "retention_closed_paper_days": self._coin_retention_closed_paper_days,
                "retention_last_report": dict(self._coin_retention_last_report or {}),
            },
            "execution": self._execution_state(),
            "accounts": self._account_state(),
        }

    def mobile_positions_payload(self) -> dict[str, Any]:
        accounts_snapshot = self._accounts.snapshot()
        positions = accounts_snapshot.get("positions") or []
        balances = self._mobile_compact_balances(
            self._sanitize_balances(accounts_snapshot.get("balances") or [])
        )
        market_lookup, market_ts_lookup = self._positions_market_snapshot_lookup()
        rows, grouped = self._positions_by_symbol(
            positions,
            return_grouped=True,
            market_lookup=market_lookup,
            market_ts_lookup=market_ts_lookup,
        )
        auto_exit = self.auto_exit_payload()
        rules = {
            str(key): dict(value or {})
            for key, value in (auto_exit.get("rules") or {}).items()
            if isinstance(key, str)
        }
        live_spreads = {
            str(key): _safe_float(value)
            for key, value in (auto_exit.get("live_spreads") or {}).items()
            if isinstance(key, str)
        }
        diagnostics_by_key = {
            str(entry.get("key") or ""): dict(entry)
            for entry in (auto_exit.get("diagnostics") or [])
            if isinstance(entry, Mapping) and entry.get("key")
        }

        def _parse_iso(value: Any) -> datetime | None:
            if not value:
                return None
            if isinstance(value, datetime):
                return value.astimezone(timezone.utc)
            try:
                return datetime.fromisoformat(str(value)).astimezone(timezone.utc)
            except Exception:  # pylint: disable=broad-except
                return None

        def _minutes_to(value: Any) -> float | None:
            dt = _parse_iso(value)
            if dt is None:
                return None
            return round((dt - datetime.now(timezone.utc)).total_seconds() / 60.0, 2)

        def _weighted_avg(items: list[Mapping[str, Any]], key: str) -> float | None:
            total_weight = 0.0
            total_value = 0.0
            for item in items:
                value = _safe_float(item.get(key))
                weight = abs(_safe_float(item.get("quantity")) or 0.0)
                if value is None or weight <= 0:
                    continue
                total_weight += weight
                total_value += value * weight
            if total_weight <= 0:
                return None
            return total_value / total_weight

        def _pair_amount_usdt(longs: list[Mapping[str, Any]], shorts: list[Mapping[str, Any]]) -> float | None:
            long_total = sum(abs(_safe_float(item.get("amount")) or 0.0) for item in longs)
            short_total = sum(abs(_safe_float(item.get("amount")) or 0.0) for item in shorts)
            if long_total > 0 and short_total > 0:
                return min(long_total, short_total)
            gross = long_total + short_total
            return gross if gross > 0 else None

        def _pair_label(summary_row: Mapping[str, Any], selected_pair: Mapping[str, Any] | None) -> str:
            long_exchange = normalize_exchange_name(str(summary_row.get("long_exchange") or ""))
            short_exchange = normalize_exchange_name(str(summary_row.get("short_exchange") or ""))
            long_count = int(summary_row.get("long_legs_count") or 0)
            short_count = int(summary_row.get("short_legs_count") or 0)
            if long_exchange and short_exchange and long_count == 1 and short_count == 1:
                return f"{long_exchange.upper()} / {short_exchange.upper()}"
            if selected_pair:
                long_sel = normalize_exchange_name(str(selected_pair.get("long_exchange") or ""))
                short_sel = normalize_exchange_name(str(selected_pair.get("short_exchange") or ""))
                if long_sel and short_sel:
                    return f"{long_sel.upper()} / {short_sel.upper()} ({str(selected_pair.get('mode') or 'pair')})"
            return "multi-leg"

        def _auto_exit_state(
            symbol: str,
            summary_row: Mapping[str, Any],
            legs: list[Mapping[str, Any]],
        ) -> dict[str, Any]:
            selected_pair = _auto_exit_select_pair_from_legs(legs)
            candidate_keys: list[str] = []
            if selected_pair:
                candidate_keys.append(
                    self._auto_exit_key(
                        symbol,
                        str(selected_pair.get("long_exchange") or ""),
                        str(selected_pair.get("short_exchange") or ""),
                    )
                )
            summary_long = str(summary_row.get("long_exchange") or "")
            summary_short = str(summary_row.get("short_exchange") or "")
            if summary_long and summary_short:
                candidate_keys.append(self._auto_exit_key(symbol, summary_long, summary_short))
            candidate_keys.append(self._auto_exit_key(symbol, AUTO_EXIT_MULTILEG_MARKER, AUTO_EXIT_MULTILEG_MARKER))
            rule_key = None
            rule = None
            for key in candidate_keys:
                if key in rules:
                    rule_key = key
                    rule = rules.get(key)
                    break
            if rule is None:
                for key, item in rules.items():
                    if normalize_symbol(str(item.get("symbol") or "")) == normalize_symbol(symbol):
                        rule_key = key
                        rule = item
                        break
            live_spread = None
            if rule_key:
                live_spread = live_spreads.get(rule_key)
            if live_spread is None:
                live_spread = _safe_float(summary_row.get("mark_price"))
            diagnostic = diagnostics_by_key.get(rule_key or "")
            spread_enabled = bool((rule or {}).get("enabled", False))
            v1_enabled = bool((rule or {}).get("v1_enabled", False))
            raw_status = str((diagnostic or {}).get("status") or "")
            if not spread_enabled and not v1_enabled:
                status = "off"
            elif live_spread is None:
                status = "no_live_spread"
            elif raw_status in {"wait", "cooldown", "running", "skip"}:
                status = "waiting"
            else:
                status = "armed"
            return {
                "key": rule_key,
                "spread_enabled": spread_enabled,
                "v1_enabled": v1_enabled,
                "target_spread_pct": _safe_float((rule or {}).get("target_spread_pct")),
                "exit_percent": _safe_float((rule or {}).get("exit_percent")) or 100.0,
                "exit_once": bool((rule or {}).get("exit_once", True)),
                "live_spread_pct": live_spread,
                "live_spread_source": "auto_exit" if rule_key and rule_key in live_spreads else "summary_mark",
                "status": status,
                "raw_status": raw_status or None,
                "reason": (diagnostic or {}).get("reason"),
                "updated_at": (rule or {}).get("updated_at"),
                "selected_pair": dict(selected_pair or {}),
            }

        cards: list[dict[str, Any]] = []
        for row in rows:
            if str(row.get("type") or "") != "summary":
                continue
            symbol = normalize_symbol(str(row.get("symbol") or ""))
            legs = [dict(item) for item in (grouped.get(symbol) or [])]
            longs = [leg for leg in legs if str(leg.get("side") or "").lower() == "long"]
            shorts = [leg for leg in legs if str(leg.get("side") or "").lower() == "short"]
            selected_pair = _auto_exit_select_pair_from_legs(legs)
            auto_exit_state = _auto_exit_state(symbol, row, legs)
            next_funding_iso = row.get("next_funding")
            minutes_to_next = _minutes_to(next_funding_iso)
            liq_distances = [
                abs(_safe_float(leg.get("dist_to_liq_pct")) or 0.0)
                for leg in legs
                if _safe_float(leg.get("dist_to_liq_pct")) is not None
            ]
            liq_distance_pct = min(liq_distances) if liq_distances else None
            quantity_abs = max(
                [abs(_safe_float(leg.get("quantity")) or 0.0) for leg in legs],
                default=0.0,
            )
            pair_amount = _pair_amount_usdt(longs, shorts)
            selected_long_exchange = normalize_exchange_name(
                str((selected_pair or {}).get("long_exchange") or row.get("long_exchange") or "")
            )
            selected_short_exchange = normalize_exchange_name(
                str((selected_pair or {}).get("short_exchange") or row.get("short_exchange") or "")
            )
            long_quantity = float((selected_pair or {}).get("long_qty") or 0.0)
            short_quantity = float((selected_pair or {}).get("short_qty") or 0.0)
            hedged_quantity = float((selected_pair or {}).get("qty") or 0.0)
            imbalance_quantity = abs(long_quantity - short_quantity)
            imbalance_pct = (
                imbalance_quantity / hedged_quantity * 100.0
                if hedged_quantity > 0
                else None
            )
            long_leverage = _weighted_avg(longs, "leverage")
            short_leverage = _weighted_avg(shorts, "leverage")
            cards.append(
                {
                    "symbol": symbol,
                    "pair_label": _pair_label(row, selected_pair),
                    "is_multi_leg": bool(selected_pair and str(selected_pair.get("mode") or "") != "single_pair"),
                    "long_exchange": selected_long_exchange or None,
                    "short_exchange": selected_short_exchange or None,
                    "net_pnl": _safe_float(row.get("unrealized_pnl")),
                    "expected_funding": _safe_float(row.get("expected_funding")),
                    "live_spread_pct": _safe_float(auto_exit_state.get("live_spread_pct")),
                    "next_funding": next_funding_iso,
                    "minutes_to_next_funding": minutes_to_next,
                    "liq_distance_pct": liq_distance_pct,
                    "risk_level": (
                        "high"
                        if liq_distance_pct is not None and liq_distance_pct <= 10.0
                        else "warn"
                        if liq_distance_pct is not None and liq_distance_pct <= 20.0
                        else "ok"
                    ),
                    "flags": {
                        "risk": bool(liq_distance_pct is not None and liq_distance_pct <= 20.0),
                        "funding_soon": bool(minutes_to_next is not None and minutes_to_next <= 120.0),
                        "auto_exit_on": bool(auto_exit_state.get("spread_enabled")),
                    },
                    "auto_exit": auto_exit_state,
                    "position_summary": {
                        "quantity": quantity_abs if quantity_abs > 0 else None,
                        "long_quantity": long_quantity if long_quantity > 0 else None,
                        "short_quantity": short_quantity if short_quantity > 0 else None,
                        "hedged_quantity": hedged_quantity if hedged_quantity > 0 else None,
                        "imbalance_quantity": imbalance_quantity,
                        "imbalance_pct": imbalance_pct,
                        "amount_usdt": pair_amount,
                        "gross_amount_usdt": sum(abs(_safe_float(leg.get("amount")) or 0.0) for leg in legs) or None,
                        "pair_entry_spread_pct": _safe_float(row.get("entry_price")),
                        "pair_mark_spread_pct": _safe_float(row.get("mark_price")),
                        "long_entry_avg": _safe_float(row.get("long_entry_avg")),
                        "short_entry_avg": _safe_float(row.get("short_entry_avg")),
                        "long_mark_avg": _safe_float(row.get("long_mark_avg")),
                        "short_mark_avg": _safe_float(row.get("short_mark_avg")),
                        "long_leverage_avg": long_leverage,
                        "short_leverage_avg": short_leverage,
                    },
                    "risk": {
                        "liq_distance_pct": liq_distance_pct,
                        "long_liq_price": _safe_float((longs[0] if longs else {}).get("liquidation_price")),
                        "short_liq_price": _safe_float((shorts[0] if shorts else {}).get("liquidation_price")),
                        "long_stop_price": _safe_float((longs[0] if longs else {}).get("stop_price")),
                        "short_stop_price": _safe_float((shorts[0] if shorts else {}).get("stop_price")),
                        "long_take_price": _safe_float((longs[0] if longs else {}).get("take_price")),
                        "short_take_price": _safe_float((shorts[0] if shorts else {}).get("take_price")),
                    },
                    "funding": {
                        "net_funding_rate": _safe_float(row.get("funding_rate")),
                        "expected_funding": _safe_float(row.get("expected_funding")),
                        "next_funding": next_funding_iso,
                        "minutes_to_next_funding": minutes_to_next,
                    },
                    "legs": legs,
                }
            )

        cards.sort(
            key=lambda item: (
                _minutes_to(item.get("next_funding")) if item.get("next_funding") else 10**9,
                str(item.get("symbol") or ""),
            )
        )
        return {
            "status": self._status if self._status != "idle" else ("ready" if self._snapshot else "idle"),
            "last_updated": self._last_refreshed.isoformat() if self._last_refreshed else None,
            "account_last_updated": accounts_snapshot.get("last_updated"),
            "balances": balances,
            "cards": cards,
            "filters": {
                "all": len(cards),
                "risk": sum(1 for card in cards if bool((card.get("flags") or {}).get("risk"))),
                "funding_soon": sum(1 for card in cards if bool((card.get("flags") or {}).get("funding_soon"))),
                "auto_exit_on": sum(1 for card in cards if bool((card.get("flags") or {}).get("auto_exit_on"))),
            },
        }

    @staticmethod
    def _mobile_compact_balances(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        compact: list[dict[str, Any]] = []
        for row in rows:
            exchange = normalize_exchange_name(str(row.get("exchange") or ""))
            if not exchange:
                continue
            total = _safe_float(row.get("total"))
            available = _safe_float(row.get("available"))
            used = _safe_float(row.get("used"))
            margin_ratio = _safe_float(row.get("margin_ratio"))
            equity = _safe_float(row.get("equity"))
            if total is None and equity is not None:
                total = equity
            if used is None and total is not None and available is not None:
                used = max(0.0, float(total) - float(available))
            if margin_ratio is None and total and used is not None and total > 0:
                margin_ratio = float(used) / float(total)
            if margin_ratio is None:
                status = "unknown"
            elif margin_ratio >= 0.8:
                status = "stress"
            elif margin_ratio >= 0.6:
                status = "watch"
            else:
                status = "ok"
            compact.append(
                {
                    "exchange": exchange,
                    "asset": row.get("asset") or row.get("currency") or "USDT",
                    "total": total,
                    "available": available,
                    "used": used,
                    "margin_ratio": margin_ratio,
                    "status": status,
                    "updated_at": row.get("updated_at") or row.get("timestamp"),
                }
            )
        compact.sort(key=lambda item: str(item.get("exchange") or ""))
        return compact

    async def mobile_manual_spread(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = normalize_symbol(str(payload.get("symbol") or "")).upper()
        action = str(payload.get("action") or "enter").lower()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        from_exchange = normalize_exchange_name(str(payload.get("from_exchange") or ""))
        to_exchange = normalize_exchange_name(str(payload.get("to_exchange") or ""))
        side = str(payload.get("side") or "long").lower()
        errors: list[str] = []
        warnings: list[str] = []

        buy_exchange = ""
        sell_exchange = ""
        if not symbol:
            errors.append("symbol is required")
        if action == "roll":
            if side == "long":
                buy_exchange = to_exchange
                sell_exchange = from_exchange
            elif side == "short":
                buy_exchange = from_exchange
                sell_exchange = to_exchange
            else:
                errors.append("side must be long or short")
            if not from_exchange or not to_exchange:
                errors.append("from_exchange and to_exchange are required")
        elif action == "exit":
            buy_exchange = short_exchange
            sell_exchange = long_exchange
            if not long_exchange or not short_exchange:
                errors.append("long_exchange and short_exchange are required")
        else:
            action = "enter"
            buy_exchange = long_exchange
            sell_exchange = short_exchange
            if not long_exchange or not short_exchange:
                errors.append("long_exchange and short_exchange are required")

        if buy_exchange and sell_exchange and buy_exchange == sell_exchange:
            warnings.append("buy and sell exchanges are the same")
        if errors:
            return {
                "status": "error",
                "symbol": symbol,
                "action": action,
                "errors": errors,
                "warnings": warnings,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        buy_quote = await self._mobile_quote_for_exchange(buy_exchange, symbol)
        sell_quote = await self._mobile_quote_for_exchange(sell_exchange, symbol)
        buy_price = _safe_float(buy_quote.get("ask"))
        sell_price = _safe_float(sell_quote.get("bid"))
        if buy_price is None:
            errors.append(f"{buy_exchange}: ask unavailable")
        if sell_price is None:
            errors.append(f"{sell_exchange}: bid unavailable")
        spread_val = spread_pct(buy_price, sell_price)
        if spread_val is None and not errors:
            errors.append("spread unavailable")
        status = "ok" if not errors else "partial"
        return {
            "status": status,
            "symbol": symbol,
            "action": action,
            "side": side if action == "roll" else None,
            "buy_exchange": buy_exchange,
            "sell_exchange": sell_exchange,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "spread_pct": spread_val,
            "quotes": {
                buy_exchange: buy_quote,
                sell_exchange: sell_quote,
            },
            "errors": errors,
            "warnings": warnings,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _mobile_quote_for_exchange(self, exchange: str, symbol: str) -> dict[str, Any]:
        exchange = normalize_exchange_name(exchange)
        symbol = normalize_symbol(symbol)
        quote: dict[str, Any] = {
            "exchange": exchange,
            "symbol": symbol,
            "bid": None,
            "ask": None,
            "mid": None,
            "mark_price": None,
            "source": None,
            "updated_at": None,
            "age_sec": None,
        }
        if not exchange or not symbol:
            return quote

        try:
            book = await self._market_data.get_orderbook(exchange, symbol, depth=1, max_age_sec=15.0)
        except Exception:  # pylint: disable=broad-except
            book = None
        if book:
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid = _safe_float((bids[0] if bids else [None])[0])
            ask = _safe_float((asks[0] if asks else [None])[0])
            if bid is not None or ask is not None:
                ts = _safe_float(book.get("timestamp"))
                quote.update(
                    {
                        "bid": bid,
                        "ask": ask,
                        "mid": ((bid + ask) / 2.0) if bid and ask else None,
                        "source": "websocket",
                        "updated_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
                        "age_sec": round(time.time() - ts, 3) if ts else None,
                    }
                )
                return quote

        cached = self._positions_market_cache.get((exchange, symbol))
        cached_ts = self._positions_market_cache_ts.get((exchange, symbol))
        if cached and (cached.bid is not None or cached.ask is not None or cached.mark_price is not None):
            bid = _safe_float(cached.bid)
            ask = _safe_float(cached.ask)
            quote.update(
                {
                    "bid": bid,
                    "ask": ask,
                    "mid": ((bid + ask) / 2.0) if bid and ask else _safe_float(cached.mark_price),
                    "mark_price": _safe_float(cached.mark_price),
                    "source": "positions_market_cache",
                    "updated_at": cached_ts.isoformat() if cached_ts else None,
                    "age_sec": round((datetime.now(timezone.utc) - cached_ts).total_seconds(), 3) if cached_ts else None,
                }
            )
            return quote

        try:
            adapter = get_adapter_cached(exchange)
            snapshots = await adapter.fetch_market_snapshots_async([symbol])
        except Exception as exc:  # pylint: disable=broad-except
            quote["error"] = str(exc)
            return quote
        for snapshot in snapshots or []:
            if not isinstance(snapshot, MarketSnapshot):
                continue
            if normalize_symbol(snapshot.symbol) != symbol:
                continue
            bid = _safe_float(snapshot.bid)
            ask = _safe_float(snapshot.ask)
            quote.update(
                {
                    "bid": bid,
                    "ask": ask,
                    "mid": ((bid + ask) / 2.0) if bid and ask else _safe_float(snapshot.mark_price),
                    "mark_price": _safe_float(snapshot.mark_price),
                    "source": "public_rest",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "age_sec": 0.0,
                }
            )
            return quote
        quote["error"] = "snapshot unavailable"
        return quote

    def mobile_manual_defaults_payload(self) -> dict[str, Any]:
        settings_payload = self._settings_manager.as_dict()
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
            "status": self._status if self._status != "idle" else ("ready" if self._snapshot else "idle"),
            "last_updated": self._last_refreshed.isoformat() if self._last_refreshed else None,
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
                "max_runtime_sec": 60,
                "reprice_sec": 2.0,
                "chunk_qty": None,
                "chunk_notional": None,
                "force_chunk_qty": False,
                "hedge_order_type": "market",
                "hedge_limit_mode": "passive",
                "hedge_favorable_bps": 2.0,
                "hedge_adverse_bps": 6.0,
                "hedge_reprice_min_sec": 2.0,
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
        cache_key = self._account_state_cache_token(payload)
        if self._account_state_cache_key == cache_key and self._account_state_cache is not None:
            return self._account_state_cache
        payload = dict(payload)
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
        payload["positions_market"] = self._positions_market_state(positions)
        payload["margin_diagnostics"] = self._margin_diagnostics(positions, balances)
        payload["margin_logic_log"] = list(self._margin_logic_log)
        payload["exchange_health"] = json.loads(json.dumps(self._derisk_exchange_health))
        payload["hedge_clusters"] = self._active_hedge_clusters()
        payload["derisk_diagnostics"] = list(self._derisk_diagnostics)
        payload["derisk_events"] = list(self._derisk_events)
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

    def _margin_diagnostics(self, positions: list[dict[str, Any]], balances: list[dict[str, Any]]) -> list[dict[str, Any]]:
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        add_enabled = bool(protective.get("auto_margin_enabled", True))
        reduce_enabled = bool(protective.get("auto_margin_reduce_enabled", True))
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
            funding_interval_hours = None
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
                selected_pair = _auto_exit_select_pair_from_legs(legs)
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
            cfg.stop_force_requote_max_age_sec = int(
                protective.get("stop_force_requote_max_age_sec", cfg.stop_force_requote_max_age_sec)
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

    def _load_hedge_cluster_config(self) -> dict[str, Any]:
        payload = self._hedge_cluster_store.load({"rules": {}})
        return normalize_hedge_cluster_config(payload)

    def hedge_cluster_payload(self) -> dict[str, Any]:
        return json.loads(json.dumps(self._hedge_clusters))

    async def update_hedge_cluster_rule(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = normalize_symbol(payload.get("symbol"))
        if not symbol:
            raise ValueError("symbol is required.")
        kind = str(payload.get("kind") or payload.get("strategy_type") or "hedged_pair").strip().lower()
        async with self._derisk_lock:
            rules = dict(self._hedge_clusters.get("rules") or {})
            if kind == "standalone":
                exchange = normalize_exchange_name(str(payload.get("exchange") or ""))
                side = str(payload.get("side") or "").strip().lower() or None
                if not exchange:
                    raise ValueError("exchange is required for standalone rules.")
                key = standalone_key(symbol, exchange, side)
                enabled = bool(payload.get("enabled", True))
                if not enabled:
                    rules.pop(key, None)
                else:
                    rules[key] = {
                        "kind": "standalone",
                        "symbol": symbol,
                        "exchange": exchange,
                        "side": side,
                        "enabled": True,
                        "source": str(payload.get("source") or "manual"),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
            else:
                long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
                short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
                if not long_exchange or not short_exchange:
                    raise ValueError("long_exchange and short_exchange are required for hedged_pair rules.")
                key = hedged_pair_key(symbol, long_exchange, short_exchange)
                enabled = bool(payload.get("enabled", True))
                if not enabled:
                    rules.pop(key, None)
                else:
                    qty_tolerance_pct = _safe_float(payload.get("qty_tolerance_pct"))
                    if qty_tolerance_pct is None or qty_tolerance_pct < 0:
                        qty_tolerance_pct = 0.1
                    rules[key] = {
                        "kind": "hedged_pair",
                        "symbol": symbol,
                        "long_exchange": long_exchange,
                        "short_exchange": short_exchange,
                        "enabled": True,
                        "qty_tolerance_pct": float(qty_tolerance_pct),
                        "rehedge_allowed": bool(payload.get("rehedge_allowed", False)),
                        "source": str(payload.get("source") or "manual"),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
            self._hedge_clusters = normalize_hedge_cluster_config({"rules": rules})
            self._hedge_cluster_store.save(self._hedge_clusters)
        return {"hedge_clusters": self.hedge_cluster_payload()}

    def _active_hedge_clusters(self) -> dict[str, Any]:
        explicit = self.hedge_cluster_payload()
        auto_exit_rules = (self._auto_exit or {}).get("rules") or {}
        return derive_cluster_rules(explicit, auto_exit_rules)

    def _load_derisk_outcome_state(self) -> dict[str, Any]:
        payload = self._derisk_outcome_store.load({"tracked": {}})
        tracked: dict[str, Any] = {}
        incoming = (payload or {}).get("tracked") if isinstance(payload, Mapping) else None
        if isinstance(incoming, Mapping):
            for cycle_id, raw in incoming.items():
                if not isinstance(raw, Mapping):
                    continue
                cid = str(raw.get("cycle_id") or cycle_id or "").strip()
                symbol = normalize_symbol(str(raw.get("symbol") or ""))
                if not cid or not symbol:
                    continue
                horizons_in = raw.get("horizons")
                horizons: dict[str, Any] = {}
                if isinstance(horizons_in, Mapping):
                    for name, meta in horizons_in.items():
                        if not isinstance(meta, Mapping):
                            continue
                        target_ts = _safe_float(meta.get("target_ts"))
                        if target_ts is None or target_ts <= 0:
                            continue
                        horizons[str(name)] = {
                            "target_ts": float(target_ts),
                            "emitted": bool(meta.get("emitted", False)),
                        }
                if not horizons:
                    continue
                tracked[cid] = {
                    "cycle_id": cid,
                    "symbol": symbol,
                    "key": str(raw.get("key") or "").strip() or None,
                    "action_type": str(raw.get("action_type") or "").strip() or None,
                    "created_ts": float(_safe_float(raw.get("created_ts")) or 0.0),
                    "baseline": dict(raw.get("baseline") or {}),
                    "horizons": horizons,
                }
        return {"tracked": tracked}

    def _save_derisk_outcome_state(self) -> None:
        self._derisk_outcome_store.save(self._derisk_outcome_state)

    def _derisk_settings(self) -> dict[str, Any]:
        protective = getattr(self._settings_manager.current, "protective", {}) or {}
        poll_sec = _safe_float(protective.get("derisk_poll_sec"))
        if poll_sec is not None and poll_sec > 0:
            self._derisk_poll_sec = float(poll_sec)
        def _num(name: str, default: float) -> float:
            value = _safe_float(protective.get(name))
            return float(default if value is None else value)
        return {
            "enabled": bool(protective.get("auto_derisk_enabled", False)),
            "shadow_mode": bool(protective.get("auto_derisk_shadow_mode", True)),
            "orphan_cleanup_enabled": bool(protective.get("orphan_cleanup_enabled", True)),
            "target_buffer_pct": _num("derisk_target_buffer_pct", 0.30),
            "warning_buffer_pct": _num("derisk_warning_buffer_pct", 0.20),
            "panic_buffer_pct": _num("derisk_panic_buffer_pct", 0.15),
            "recovery_buffer_pct": _num("derisk_recovery_buffer_pct", 0.35),
            "min_free_balance_abs": _num("derisk_min_free_balance_abs", 500.0),
            "stale_positions_max_sec": int(protective.get("derisk_stale_positions_max_sec", 180) or 180),
            "failure_block_count": int(protective.get("derisk_failure_block_count", 2) or 2),
            "confirm_cycles": int(protective.get("derisk_confirm_cycles", 2) or 2),
            "cooldown_sec": int(protective.get("derisk_cooldown_sec", 120) or 120),
            "velocity_trigger_bps": _num("derisk_velocity_trigger_bps", 120.0),
            "qty_tolerance_pct": _num("derisk_qty_tolerance_pct", 0.10),
            "max_single_action_notional_usd": _num("derisk_max_single_action_notional_usd", 500.0),
            "market_cleanup_only_in_emergency": bool(
                protective.get("derisk_market_cleanup_only_in_emergency", True)
            ),
            "dust_notional_usd": _num("derisk_dust_notional_usd", 10.0),
        }

    def derisk_payload(self) -> dict[str, Any]:
        return {
            "settings": self._derisk_settings(),
            "exchange_health": json.loads(json.dumps(self._derisk_exchange_health)),
            "clusters": self._active_hedge_clusters(),
            "diagnostics": list(self._derisk_diagnostics),
            "events": list(self._derisk_events),
        }

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
                        incoming_runtime = int(incoming_defaults.get("max_runtime_sec"))
                        defaults["max_runtime_sec"] = (
                            AUTO_EXIT_DEFAULTS["max_runtime_sec"]
                            if incoming_runtime == 600
                            else incoming_runtime
                        )
                    except Exception:
                        pass
                if incoming_defaults.get("cooldown_sec") is not None:
                    try:
                        defaults["cooldown_sec"] = max(0, int(incoming_defaults.get("cooldown_sec")))
                    except Exception:
                        pass
                if "require_live" in incoming_defaults:
                    defaults["require_live"] = bool(incoming_defaults.get("require_live"))
                if "restore_spread_on_missing" in incoming_defaults:
                    defaults["restore_spread_on_missing"] = bool(
                        incoming_defaults.get("restore_spread_on_missing")
                    )
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
                    spread_enabled = bool(rule.get("enabled", target is not None))
                    v1_enabled = bool(rule.get("v1_enabled", False))
                    exit_percent = _safe_float(rule.get("exit_percent"))
                    if exit_percent is None or exit_percent <= 0 or exit_percent > 100:
                        exit_percent = 100.0
                    if not symbol or not long_ex or not short_ex or (target is None and not v1_enabled):
                        continue
                    rule_key = f"{symbol}|{long_ex}|{short_ex}"
                    rules[rule_key] = {
                        "symbol": symbol,
                        "long_exchange": long_ex,
                        "short_exchange": short_ex,
                        "target_spread_pct": float(target) if target is not None else None,
                        "enabled": spread_enabled,
                        "v1_enabled": v1_enabled,
                        "exit_percent": float(exit_percent),
                        "exit_once": bool(rule.get("exit_once", True)),
                        "persist_on_missing": bool(rule.get("persist_on_missing", True)),
                        "last_triggered_ts": float(rule.get("last_triggered_ts") or 0.0),
                        "last_v1_triggered_ts": float(rule.get("last_v1_triggered_ts") or 0.0),
                        "updated_at": rule.get("updated_at"),
                        "missing_since_ts": float(rule.get("missing_since_ts") or 0.0),
                        "v1_pending_exit_cycles": max(0, int(rule.get("v1_pending_exit_cycles") or 0)),
                        "position_signature": (
                            dict(rule.get("position_signature"))
                            if isinstance(rule.get("position_signature"), Mapping)
                            else None
                        ),
                        "bound_at": rule.get("bound_at"),
                        "signature_status": str(rule.get("signature_status") or ""),
                        "rule_generation": max(1, int(rule.get("rule_generation") or 1)),
                        "spread_target_qty": _safe_float(rule.get("spread_target_qty")),
                        "spread_remaining_qty": _safe_float(rule.get("spread_remaining_qty")),
                        "v1_target_qty": _safe_float(rule.get("v1_target_qty")),
                        "v1_remaining_qty": _safe_float(rule.get("v1_remaining_qty")),
                    }
        return {"defaults": defaults, "rules": rules}

    @staticmethod
    def _auto_exit_key(symbol: str, long_exchange: str, short_exchange: str) -> str:
        return f"{normalize_symbol(symbol)}|{normalize_exchange_name(long_exchange)}|{normalize_exchange_name(short_exchange)}"

    def auto_exit_payload(self) -> dict[str, Any]:
        payload = json.loads(json.dumps(self._auto_exit))
        payload["live_spreads"] = dict(self._auto_exit_live_spreads)
        payload["diagnostics"] = list(self._auto_exit_diagnostics)
        payload["v1_diagnostics"] = list(self._auto_exit_v1_diagnostics)
        payload["events"] = list(self._auto_exit_events)
        return payload

    def _current_auto_exit_position_signature(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
    ) -> dict[str, Any] | None:
        try:
            positions = self._accounts.snapshot().get("positions") or []
            market_lookup, market_ts_lookup = self._positions_market_snapshot_lookup()
            _, grouped = self._positions_by_symbol(
                positions,
                return_grouped=True,
                market_lookup=market_lookup,
                market_ts_lookup=market_ts_lookup,
            )
            symbol_key = normalize_symbol(symbol)
            legs = grouped.get(symbol_key) or []
            selected = _auto_exit_select_pair_from_legs(legs)
            return _auto_exit_position_signature(
                symbol_key,
                legs,
                rule_long_exchange=long_exchange,
                rule_short_exchange=short_exchange,
                selected_pair=selected,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("auto-exit signature build failed: %s", exc)
            return None

    async def update_auto_exit_defaults(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        async with self._auto_exit_lock:
            defaults = self._auto_exit.get("defaults", {})
            runtime = payload.get("max_runtime_sec")
            cooldown = payload.get("cooldown_sec")
            require_live = payload.get("require_live")
            auto_clear = payload.get("auto_clear_no_position_sec")
            restore_spread = payload.get("restore_spread_on_missing")
            if runtime is not None:
                defaults["max_runtime_sec"] = max(30, int(runtime))
            if cooldown is not None:
                defaults["cooldown_sec"] = max(0, int(cooldown))
            if require_live is not None:
                defaults["require_live"] = bool(require_live)
            if restore_spread is not None:
                defaults["restore_spread_on_missing"] = bool(restore_spread)
            if auto_clear is not None:
                defaults["auto_clear_no_position_sec"] = max(0, int(auto_clear))
            self._auto_exit["defaults"] = defaults
            self._auto_exit_store.save(self._auto_exit)
        return {"auto_exit": self.auto_exit_payload()}

    async def clear_auto_exit_spread_cache(
        self,
        symbol: str | None = None,
        *,
        clear_v1: bool = False,
    ) -> dict[str, Any]:
        symbol_key = normalize_symbol(symbol or "") if symbol else ""
        removed = 0
        disabled = 0
        cleared_v1 = 0
        async with self._auto_exit_lock:
            rules = self._auto_exit.get("rules", {})
            if not isinstance(rules, dict):
                rules = {}
                self._auto_exit["rules"] = rules
            for key in list(rules.keys()):
                rule = rules.get(key)
                if not isinstance(rule, dict):
                    continue
                rule_symbol = normalize_symbol(str(rule.get("symbol") or ""))
                if symbol_key and rule_symbol != symbol_key:
                    continue
                if clear_v1:
                    rules.pop(key, None)
                    removed += 1
                    if bool(rule.get("v1_enabled", False)):
                        cleared_v1 += 1
                    continue
                if not bool(rule.get("enabled", False)) and rule.get("target_spread_pct") is None:
                    continue
                if bool(rule.get("v1_enabled", False)):
                    rule["enabled"] = False
                    rule["target_spread_pct"] = None
                    rule["persist_on_missing"] = False
                    rule["missing_since_ts"] = 0.0
                    rule["position_signature"] = None
                    rule["signature_status"] = "spread_cache_cleared"
                    rule["updated_at"] = datetime.now(timezone.utc).isoformat()
                    disabled += 1
                else:
                    rules.pop(key, None)
                    removed += 1
            if removed or disabled:
                self._auto_exit_store.save(self._auto_exit)
        self._auto_exit_event(
            "spread_cache_clear",
            {
                "symbol": symbol_key or None,
                "removed": removed,
                "disabled": disabled,
                "cleared_v1": cleared_v1,
                "clear_v1": bool(clear_v1),
            },
        )
        return {
            "removed": removed,
            "disabled": disabled,
            "cleared_v1": cleared_v1,
            "auto_exit": self.auto_exit_payload(),
        }

    async def update_auto_exit_rule(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = str(payload.get("symbol") or "").upper().strip()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        if not symbol or not long_exchange or not short_exchange:
            raise ValueError("symbol, long_exchange, and short_exchange are required.")
        key = self._auto_exit_key(symbol, long_exchange, short_exchange)
        async with self._auto_exit_lock:
            rules = self._auto_exit.get("rules", {})
            prev = dict(rules.get(key, {}) or {})
            spread_enabled_in = payload.get("spread_enabled")
            if spread_enabled_in is None and "enabled" in payload:
                spread_enabled_in = payload.get("enabled")
            v1_enabled_in = payload.get("v1_enabled")
            exit_percent_in = _safe_float(payload.get("exit_percent"))
            exit_once_in = payload.get("exit_once")
            persist_on_missing_in = payload.get("persist_on_missing")
            defaults = self._auto_exit.get("defaults", {}) or {}
            default_persist_on_missing = bool(
                defaults.get(
                    "restore_spread_on_missing",
                    AUTO_EXIT_DEFAULTS["restore_spread_on_missing"],
                )
            )
            spread_enabled = (
                bool(prev.get("enabled", False))
                if spread_enabled_in is None
                else bool(spread_enabled_in)
            )
            v1_enabled = (
                bool(prev.get("v1_enabled", False))
                if v1_enabled_in is None
                else bool(v1_enabled_in)
            )
            exit_percent = (
                _safe_float(prev.get("exit_percent")) or 100.0
                if exit_percent_in is None
                else float(exit_percent_in)
            )
            if exit_percent <= 0 or exit_percent > 100:
                raise ValueError("exit_percent must be greater than 0 and no more than 100.")
            exit_once = (
                bool(prev.get("exit_once", True))
                if exit_once_in is None
                else bool(exit_once_in)
            )
            persist_on_missing = (
                bool(prev.get("persist_on_missing", default_persist_on_missing))
                if persist_on_missing_in is None
                else bool(persist_on_missing_in)
            )
            target = _safe_float(payload.get("target_spread_pct"))
            if not spread_enabled:
                target = None
            elif target is None:
                target = _safe_float(prev.get("target_spread_pct"))
            if not spread_enabled and not v1_enabled:
                rules.pop(key, None)
            else:
                if spread_enabled and target is None:
                    raise ValueError("target_spread_pct is required when enabled.")
                now_iso = datetime.now(timezone.utc).isoformat()
                position_signature = self._current_auto_exit_position_signature(
                    symbol,
                    long_exchange,
                    short_exchange,
                )
                signature_status = "bound" if position_signature else "binding_position_missing"
                rule_generation = max(1, int(prev.get("rule_generation") or 0) + 1)
                rules[key] = {
                    "symbol": symbol,
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "target_spread_pct": float(target) if target is not None else None,
                    "enabled": bool(spread_enabled),
                    "v1_enabled": bool(v1_enabled),
                    "exit_percent": float(exit_percent),
                    "exit_once": bool(exit_once),
                    "persist_on_missing": bool(persist_on_missing),
                    "last_triggered_ts": float(prev.get("last_triggered_ts") or 0.0),
                    "last_v1_triggered_ts": float(prev.get("last_v1_triggered_ts") or 0.0),
                    "updated_at": now_iso,
                    "missing_since_ts": 0.0,
                    "v1_pending_exit_cycles": max(0, int(prev.get("v1_pending_exit_cycles") or 0)),
                    "position_signature": position_signature,
                    "bound_at": now_iso if position_signature else None,
                    "signature_status": signature_status,
                    "rule_generation": rule_generation,
                    "spread_target_qty": None,
                    "spread_remaining_qty": None,
                    "v1_target_qty": None,
                    "v1_remaining_qty": None,
                }
            self._auto_exit["rules"] = rules
            self._auto_exit_store.save(self._auto_exit)
            stored_rule = dict(rules.get(key) or {})
        self._auto_exit_event(
            "rule_update",
            {
                "key": key,
                "symbol": symbol,
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "spread_enabled": bool(stored_rule.get("enabled", False)),
                "v1_enabled": bool(stored_rule.get("v1_enabled", False)),
                "target_spread_pct": stored_rule.get("target_spread_pct"),
                "exit_percent": stored_rule.get("exit_percent"),
                "exit_once": stored_rule.get("exit_once"),
                "rule_generation": stored_rule.get("rule_generation"),
                "signature_status": stored_rule.get("signature_status"),
                "removed": not bool(stored_rule),
            },
        )
        return {"auto_exit": self.auto_exit_payload()}

    def _derisk_event(self, event: str, payload: Mapping[str, Any]) -> None:
        entry = {
            "event": event,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        if self._derisk_active_cycle_id:
            entry["cycle_id"] = self._derisk_active_cycle_id
        entry.update(dict(payload or {}))
        self._derisk_events.append(entry)
        if len(self._derisk_events) > self._derisk_event_limit:
            self._derisk_events = self._derisk_events[-self._derisk_event_limit :]
        self._append_derisk_history(
            {
                "record_type": "event",
                **dict(entry),
            }
        )

    def _append_derisk_history(self, payload: Mapping[str, Any]) -> None:
        try:
            self._derisk_history_store.append(dict(payload or {}))
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("failed to append de-risk history: %s", exc)

    @staticmethod
    def _compact_derisk_result(result: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(result, Mapping):
            return None
        return {
            "status": result.get("status"),
            "execution_id": result.get("execution_id"),
            "mode": result.get("mode"),
            "action": result.get("action"),
            "remaining_qty": result.get("remaining_qty"),
            "error_count": len(list(result.get("errors") or [])),
            "warning_count": len(list(result.get("warnings") or [])),
            "risk_flags": list(result.get("risk_flags") or []),
        }

    @staticmethod
    def _compact_derisk_health_rows(exchange_health: Mapping[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for exchange in sorted(str(key) for key in (exchange_health or {}).keys()):
            item = dict((exchange_health or {}).get(exchange) or {})
            rows.append(
                {
                    "exchange": exchange,
                    "health": item.get("health"),
                    "last_status": item.get("last_status"),
                    "last_error_kind": item.get("last_error_kind"),
                    "consecutive_failures": int(item.get("consecutive_failures") or 0),
                    "stale_sec": _safe_float(item.get("stale_sec")),
                }
            )
        return rows

    @staticmethod
    def _compact_derisk_balance_rows(
        balances: list[Mapping[str, Any]],
        exchange_stress: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in balances or []:
            exchange = normalize_exchange_name(str(item.get("exchange") or ""))
            if not exchange:
                continue
            stress = dict((exchange_stress or {}).get(exchange) or {})
            rows.append(
                {
                    "exchange": exchange,
                    "asset": item.get("asset"),
                    "total": _safe_float(item.get("total")),
                    "available": _safe_float(item.get("available")),
                    "used": _safe_float(item.get("used")),
                    "buffer_pct": _safe_float(item.get("buffer_pct")),
                    "stress_status": stress.get("status"),
                    "stress_score": _safe_float(stress.get("stress_score")),
                    "target_free_usd": _safe_float(stress.get("target_free_usd")),
                    "deficit_usd": _safe_float(stress.get("deficit_usd")),
                }
            )
        return rows

    @staticmethod
    def _compact_derisk_diagnostic_row(row: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(row or {})
        result = {
            "kind": payload.get("kind"),
            "key": payload.get("key"),
            "symbol": payload.get("symbol"),
            "status": payload.get("status"),
            "reason": payload.get("reason"),
            "long_exchange": payload.get("long_exchange"),
            "short_exchange": payload.get("short_exchange"),
            "orphan_exchange": payload.get("orphan_exchange"),
            "orphan_position_side": payload.get("orphan_position_side"),
            "orphan_qty": _safe_float(payload.get("orphan_qty")),
            "stress_exchange": payload.get("stress_exchange"),
            "stress_status": payload.get("stress_status"),
            "stress_score": _safe_float(payload.get("stress_score")),
            "cluster_notional_usd": _safe_float(payload.get("cluster_notional_usd")),
            "cluster_unrealized_pnl_usd": _safe_float(payload.get("cluster_unrealized_pnl_usd")),
            "funding_to_next_usd": _safe_float(payload.get("funding_to_next_usd")),
            "minutes_to_event": _safe_float(payload.get("minutes_to_event")),
            "action_qty": _safe_float(payload.get("action_qty")),
            "action_mode": payload.get("action_mode"),
            "candidate_score": _safe_float(payload.get("candidate_score")),
            "residual_status": payload.get("residual_status"),
            "qty_mismatch_ratio": _safe_float(payload.get("qty_mismatch_ratio")),
            "missing_cycles": int(payload.get("missing_cycles") or 0),
            "confirm_cycles": int(payload.get("confirm_cycles") or 0),
            "health_blocked": bool(payload.get("health_blocked", False)),
            "long_health": payload.get("long_health"),
            "short_health": payload.get("short_health"),
            "long_error_kind": payload.get("long_error_kind"),
            "short_error_kind": payload.get("short_error_kind"),
            "cluster_conflict": bool(payload.get("cluster_conflict", False)),
            "cluster_conflict_reason": payload.get("cluster_conflict_reason"),
            "unexpected_leg_count": int(payload.get("unexpected_leg_count") or 0),
            "duplicate_visible_leg_count": int(payload.get("duplicate_visible_leg_count") or 0),
            "updated_at": payload.get("updated_at"),
        }
        overlap_conflicts = list(payload.get("overlap_conflicts") or [])
        if overlap_conflicts:
            result["overlap_conflicts"] = overlap_conflicts
        unexpected_legs = list(payload.get("unexpected_legs") or [])
        if unexpected_legs:
            result["unexpected_legs"] = unexpected_legs
        return result

    def _persist_derisk_cycle_history(
        self,
        *,
        cycle_id: str,
        settings: Mapping[str, Any],
        balances: list[Mapping[str, Any]],
        exchange_stress: Mapping[str, Any],
        diagnostics: list[Mapping[str, Any]],
        cycle_action: Mapping[str, Any] | None,
        running_execution: Mapping[str, Any] | None,
    ) -> None:
        status_counts: dict[str, int] = {}
        compact_rows: list[dict[str, Any]] = []
        for row in diagnostics or []:
            status = str(row.get("status") or "")
            if status:
                status_counts[status] = status_counts.get(status, 0) + 1
            kind = str(row.get("kind") or "")
            if (
                kind in {"candidate", "orphan_candidate"}
                or status not in {"", "healthy", "ranked"}
                or str(row.get("stress_status") or "") in {"stress", "panic"}
                or str(row.get("action_mode") or "") not in {"", "none"}
            ):
                compact_rows.append(self._compact_derisk_diagnostic_row(row))
        history_row = {
            "record_type": "cycle",
            "cycle_id": cycle_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "settings": {
                "enabled": bool(settings.get("enabled")),
                "shadow_mode": bool(settings.get("shadow_mode")),
                "orphan_cleanup_enabled": bool(settings.get("orphan_cleanup_enabled", True)),
                "target_buffer_pct": _safe_float(settings.get("target_buffer_pct")),
                "warning_buffer_pct": _safe_float(settings.get("warning_buffer_pct")),
                "panic_buffer_pct": _safe_float(settings.get("panic_buffer_pct")),
                "recovery_buffer_pct": _safe_float(settings.get("recovery_buffer_pct")),
                "confirm_cycles": int(settings.get("confirm_cycles") or 0),
                "cooldown_sec": int(settings.get("cooldown_sec") or 0),
                "qty_tolerance_pct": _safe_float(settings.get("qty_tolerance_pct")),
                "max_single_action_notional_usd": _safe_float(settings.get("max_single_action_notional_usd")),
                "dust_notional_usd": _safe_float(settings.get("dust_notional_usd")),
            },
            "status_counts": status_counts,
            "exchange_health": self._compact_derisk_health_rows(self._derisk_exchange_health),
            "balances": self._compact_derisk_balance_rows(balances, exchange_stress),
            "running_execution": dict(running_execution or {}) or None,
            "cycle_action": dict(cycle_action or {}) or None,
            "rows": compact_rows,
        }
        self._append_derisk_history(history_row)

    @staticmethod
    def _compact_balance_lookup_rows(
        balances: list[Mapping[str, Any]],
        exchange_health: Mapping[str, Any],
        exchanges: list[str],
    ) -> list[dict[str, Any]]:
        wanted = {normalize_exchange_name(name) for name in exchanges if str(name).strip()}
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in balances or []:
            exchange = normalize_exchange_name(str(item.get("exchange") or ""))
            if not exchange or exchange not in wanted or exchange in seen:
                continue
            health = dict((exchange_health or {}).get(exchange) or {})
            rows.append(
                {
                    "exchange": exchange,
                    "available": _safe_float(item.get("available")),
                    "used": _safe_float(item.get("used")),
                    "total": _safe_float(item.get("total")),
                    "buffer_pct": _safe_float(item.get("buffer_pct")),
                    "health": health.get("health"),
                    "last_error_kind": health.get("last_error_kind"),
                }
            )
            seen.add(exchange)
        return rows

    def _build_derisk_outcome_snapshot(
        self,
        *,
        diagnostics: list[Mapping[str, Any]],
        balances: list[Mapping[str, Any]],
        cycle_action: Mapping[str, Any],
    ) -> dict[str, Any]:
        key = str(cycle_action.get("key") or "").strip()
        symbol = normalize_symbol(str(cycle_action.get("symbol") or ""))
        cluster = next(
            (
                dict(row)
                for row in diagnostics
                if str(row.get("kind") or "") == "cluster"
                and (
                    (key and str(row.get("key") or "") == key)
                    or (not key and normalize_symbol(str(row.get("symbol") or "")) == symbol)
                )
            ),
            {},
        )
        candidate = next(
            (
                dict(row)
                for row in diagnostics
                if str(row.get("kind") or "") in {"candidate", "orphan_candidate"}
                and (
                    (key and str(row.get("key") or "") == key)
                    or (not key and normalize_symbol(str(row.get("symbol") or "")) == symbol)
                )
            ),
            {},
        )
        exchanges = [
            str(cluster.get("stress_exchange") or ""),
            str(cluster.get("orphan_exchange") or ""),
            str(cluster.get("long_exchange") or ""),
            str(cluster.get("short_exchange") or ""),
        ]
        return {
            "symbol": symbol,
            "key": key or None,
            "action_type": str(cycle_action.get("type") or ""),
            "cluster_status": cluster.get("status"),
            "cluster_reason": cluster.get("reason"),
            "stress_exchange": cluster.get("stress_exchange"),
            "stress_status": cluster.get("stress_status"),
            "stress_score": _safe_float(cluster.get("stress_score")),
            "orphan_exchange": cluster.get("orphan_exchange"),
            "orphan_position_side": cluster.get("orphan_position_side"),
            "orphan_qty": _safe_float(cluster.get("orphan_qty")),
            "candidate_score": _safe_float(candidate.get("candidate_score") or cluster.get("candidate_score")),
            "action_qty": _safe_float(cycle_action.get("action_qty") or cluster.get("action_qty")),
            "action_mode": cluster.get("action_mode"),
            "cluster_notional_usd": _safe_float(cluster.get("cluster_notional_usd")),
            "cluster_unrealized_pnl_usd": _safe_float(cluster.get("cluster_unrealized_pnl_usd")),
            "funding_to_next_usd": _safe_float(cluster.get("funding_to_next_usd")),
            "minutes_to_event": _safe_float(cluster.get("minutes_to_event")),
            "unexpected_leg_count": int(cluster.get("unexpected_leg_count") or 0),
            "cluster_conflict": bool(cluster.get("cluster_conflict", False)),
            "balances": self._compact_balance_lookup_rows(
                balances,
                self._derisk_exchange_health,
                exchanges,
            ),
        }

    @staticmethod
    def _derisk_outcome_label(
        initial: Mapping[str, Any],
        current: Mapping[str, Any],
    ) -> tuple[str, list[str]]:
        reasons: list[str] = []
        action_type = str(initial.get("action_type") or "")
        if action_type == "orphan_trigger":
            initial_orphan = _safe_float(initial.get("orphan_qty")) or 0.0
            current_orphan = _safe_float(current.get("orphan_qty"))
            current_status = str(current.get("cluster_status") or "")
            if (current_orphan or 0.0) <= 0 and current_status not in {"confirmed_orphan", "suspected_orphan"}:
                reasons.append("orphan_resolved")
                return "improved", reasons
            if initial_orphan > 0 and current_orphan is not None and current_orphan < initial_orphan:
                reasons.append("orphan_qty_reduced")
                return "improved", reasons
            if current_status == "confirmed_orphan":
                reasons.append("orphan_still_confirmed")
                return "worsened", reasons
            reasons.append("orphan_not_resolved")
            return "unchanged", reasons

        initial_stress = str(initial.get("stress_status") or "ok")
        current_stress = str(current.get("stress_status") or "ok")
        stress_rank = {"ok": 0, "stress": 1, "panic": 2}
        initial_buffer = None
        current_buffer = None
        initial_exchange = normalize_exchange_name(str(initial.get("stress_exchange") or ""))
        current_exchange = normalize_exchange_name(str(current.get("stress_exchange") or initial_exchange))
        for item in list(initial.get("balances") or []):
            if normalize_exchange_name(str(item.get("exchange") or "")) == initial_exchange:
                initial_buffer = _safe_float(item.get("buffer_pct"))
                break
        for item in list(current.get("balances") or []):
            if normalize_exchange_name(str(item.get("exchange") or "")) == current_exchange:
                current_buffer = _safe_float(item.get("buffer_pct"))
                break
        if stress_rank.get(current_stress, 3) < stress_rank.get(initial_stress, 3):
            reasons.append("stress_status_improved")
            return "improved", reasons
        if stress_rank.get(current_stress, 3) > stress_rank.get(initial_stress, 3):
            reasons.append("stress_status_worsened")
            return "worsened", reasons
        if initial_buffer is not None and current_buffer is not None:
            if current_buffer >= initial_buffer + 1.0:
                reasons.append("buffer_pct_recovered")
                return "improved", reasons
            if current_buffer <= initial_buffer - 1.0:
                reasons.append("buffer_pct_deteriorated")
                return "worsened", reasons
        reasons.append("stress_state_stable")
        return "unchanged", reasons

    def _register_derisk_outcome_tracking(
        self,
        *,
        cycle_id: str,
        now_ts: float,
        cycle_action: Mapping[str, Any] | None,
        diagnostics: list[Mapping[str, Any]],
        balances: list[Mapping[str, Any]],
    ) -> None:
        action = dict(cycle_action or {})
        action_type = str(action.get("type") or "")
        if action_type not in {"trigger", "orphan_trigger"}:
            return
        if not (normalize_symbol(str(action.get("symbol") or ""))):
            return
        baseline = self._build_derisk_outcome_snapshot(
            diagnostics=diagnostics,
            balances=balances,
            cycle_action=action,
        )
        horizons = {
            name: {"target_ts": float(now_ts) + float(delay), "emitted": False}
            for name, delay in DERISK_OUTCOME_HORIZONS_SEC.items()
        }
        minutes_to_event = _safe_float(baseline.get("minutes_to_event"))
        if minutes_to_event is not None and minutes_to_event > 0:
            funding_target_ts = float(now_ts) + float(minutes_to_event) * 60.0
            if funding_target_ts > float(now_ts) + 5.0:
                horizons["to_next_funding"] = {"target_ts": funding_target_ts, "emitted": False}
        tracked = dict(self._derisk_outcome_state.get("tracked") or {})
        tracked[str(cycle_id)] = {
            "cycle_id": str(cycle_id),
            "symbol": baseline.get("symbol"),
            "key": baseline.get("key"),
            "action_type": action_type,
            "created_ts": float(now_ts),
            "baseline": baseline,
            "horizons": horizons,
        }
        self._derisk_outcome_state["tracked"] = tracked
        self._save_derisk_outcome_state()

    def _evaluate_pending_derisk_outcomes(
        self,
        *,
        now_ts: float,
        diagnostics: list[Mapping[str, Any]],
        balances: list[Mapping[str, Any]],
    ) -> None:
        tracked = dict(self._derisk_outcome_state.get("tracked") or {})
        if not tracked:
            return
        updated: dict[str, Any] = {}
        for cycle_id, item in tracked.items():
            row = dict(item or {})
            baseline = dict(row.get("baseline") or {})
            symbol = normalize_symbol(str(row.get("symbol") or baseline.get("symbol") or ""))
            if not symbol:
                continue
            action_view = {
                "type": row.get("action_type"),
                "symbol": symbol,
                "key": row.get("key"),
                "action_qty": baseline.get("action_qty"),
            }
            current = self._build_derisk_outcome_snapshot(
                diagnostics=diagnostics,
                balances=balances,
                cycle_action=action_view,
            )
            horizons_out: dict[str, Any] = {}
            changed = False
            for horizon_name, meta in dict(row.get("horizons") or {}).items():
                target_ts = float(_safe_float(meta.get("target_ts")) or 0.0)
                emitted = bool(meta.get("emitted", False))
                if emitted or target_ts <= 0:
                    if emitted:
                        horizons_out[str(horizon_name)] = {"target_ts": target_ts, "emitted": True}
                    continue
                if float(now_ts) >= target_ts:
                    label, reasons = self._derisk_outcome_label(baseline, current)
                    self._append_derisk_history(
                        {
                            "record_type": "outcome",
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "cycle_id": str(cycle_id),
                            "source_action_type": row.get("action_type"),
                            "symbol": symbol,
                            "key": row.get("key"),
                            "horizon": str(horizon_name),
                            "target_ts": target_ts,
                            "age_sec": max(0.0, float(now_ts) - float(_safe_float(row.get("created_ts")) or 0.0)),
                            "heuristic_outcome": {
                                "label": label,
                                "reasons": reasons,
                            },
                            "initial": baseline,
                            "current": current,
                        }
                    )
                    horizons_out[str(horizon_name)] = {"target_ts": target_ts, "emitted": True}
                    changed = True
                else:
                    horizons_out[str(horizon_name)] = {"target_ts": target_ts, "emitted": False}
            if any(not bool(meta.get("emitted", False)) for meta in horizons_out.values()):
                row["horizons"] = horizons_out
                updated[str(cycle_id)] = row
            elif not changed:
                row["horizons"] = horizons_out
                updated[str(cycle_id)] = row
        if updated != tracked:
            self._derisk_outcome_state["tracked"] = updated
            self._save_derisk_outcome_state()

    def _derisk_log_event(
        self,
        key: str,
        event: str,
        payload: Mapping[str, Any],
        now_ts: float,
    ) -> None:
        log_key = f"{key}|{event}"
        last_ts = float(self._derisk_last_log_ts.get(log_key) or 0.0)
        if (now_ts - last_ts) < float(self._derisk_log_cooldown_sec):
            return
        self._derisk_last_log_ts[log_key] = now_ts
        self._derisk_event(event, payload)

    async def _auto_derisk_cycle(self) -> None:
        if self._derisk_inflight:
            return
        self._derisk_inflight = True
        cycle_id = uuid4().hex
        self._derisk_active_cycle_id = cycle_id
        try:
            settings = self._derisk_settings()
            accounts_snapshot = self._accounts.snapshot() or {}
            positions = list(accounts_snapshot.get("positions") or [])
            balances = self._sanitize_balances(accounts_snapshot.get("balances") or [])
            status_entries = list(accounts_snapshot.get("status") or [])
            now_ts = time.time()
            self._derisk_exchange_health = build_exchange_health(
                status_entries,
                previous=self._derisk_exchange_health,
                now_ts=now_ts,
                stale_after_sec=int(settings.get("stale_positions_max_sec", 180)),
                failure_block_count=int(settings.get("failure_block_count", 2)),
            )
            clusters_payload = self._active_hedge_clusters()
            cluster_rules = dict(clusters_payload.get("rules") or {})
            diagnostics: list[dict[str, Any]] = []
            grouped: dict[str, list[dict[str, Any]]] = {}
            if positions:
                _rows, grouped = self._positions_by_symbol(
                    positions,
                    return_grouped=True,
                    market_lookup={},
                    market_ts_lookup={},
                )
            balances_by_exchange = {
                normalize_exchange_name(str(item.get("exchange") or "")): dict(item)
                for item in balances
                if str(item.get("exchange") or "").strip()
            }
            exchange_stress = {
                exchange: exchange_stress_state(
                    balance,
                    target_buffer_pct=float(settings.get("target_buffer_pct", 0.30)),
                    warning_buffer_pct=float(settings.get("warning_buffer_pct", 0.20)),
                    panic_buffer_pct=float(settings.get("panic_buffer_pct", 0.15)),
                    min_free_balance_abs=float(settings.get("min_free_balance_abs", 500.0)),
                )
                for exchange, balance in balances_by_exchange.items()
            }
            orphan_targets: list[dict[str, Any]] = []
            candidates: list[dict[str, Any]] = []
            confirm_cycles = max(1, int(settings.get("confirm_cycles", 2)))
            qty_tolerance_default = float(settings.get("qty_tolerance_pct", 0.10))
            cycle_action: dict[str, Any] | None = None
            expected_cluster_legs: dict[tuple[str, str, str], list[str]] = {}

            def _position_side(value: Any, qty_hint: float | None = None) -> str:
                side_raw = str(value or "").strip().lower()
                if side_raw in {"long", "buy"}:
                    return "long"
                if side_raw in {"short", "sell"}:
                    return "short"
                if qty_hint is not None:
                    if qty_hint < 0:
                        return "short"
                    if qty_hint > 0:
                        return "long"
                return ""

            def _leg_qty(leg: Mapping[str, Any]) -> float:
                qty = _safe_float(leg.get("coin_qty"))
                if qty is None:
                    qty = _safe_float(leg.get("quantity"))
                if qty is None:
                    qty = _safe_float(leg.get("contracts"))
                if qty is None:
                    qty = _safe_float(leg.get("amount"))
                return abs(float(qty or 0.0))

            def _aggregate_symbol_legs(
                raw_legs: list[dict[str, Any]],
                *,
                symbol_name: str,
                long_ex: str,
                short_ex: str,
            ) -> tuple[dict[tuple[str, str], dict[str, Any]], list[dict[str, Any]], int]:
                expected = {(long_ex, "long"), (short_ex, "short")}
                aggregates: dict[tuple[str, str], dict[str, Any]] = {}
                unexpected: list[dict[str, Any]] = []
                duplicate_count = 0
                for raw_leg in raw_legs:
                    exchange = normalize_exchange_name(str(raw_leg.get("exchange") or ""))
                    qty_hint = _safe_float(raw_leg.get("coin_qty"))
                    if qty_hint is None:
                        qty_hint = _safe_float(raw_leg.get("quantity"))
                    if qty_hint is None:
                        qty_hint = _safe_float(raw_leg.get("contracts"))
                    if qty_hint is None:
                        qty_hint = _safe_float(raw_leg.get("amount"))
                    side = _position_side(raw_leg.get("side"), qty_hint)
                    if not exchange or not side:
                        continue
                    key = (exchange, side)
                    qty_abs = abs(float(qty_hint or 0.0))
                    if key not in expected:
                        if qty_abs > 0:
                            unexpected.append(
                                {
                                    "exchange": exchange,
                                    "side": side,
                                    "qty": qty_abs,
                                    "symbol": symbol_name,
                                }
                            )
                        continue
                    current = aggregates.get(key)
                    if current is None:
                        current = dict(raw_leg)
                        current["exchange"] = exchange
                        current["side"] = side
                        current["_aggregate_count"] = 1
                        current["coin_qty"] = qty_abs
                        current["quantity"] = qty_abs
                        current["contracts"] = qty_abs
                        current["amount"] = _safe_float(raw_leg.get("amount")) or 0.0
                        current["notional"] = _safe_float(raw_leg.get("notional"))
                        current["unrealized_pnl"] = _safe_float(raw_leg.get("unrealized_pnl")) or 0.0
                        current["margin_used"] = _safe_float(raw_leg.get("margin_used"))
                        aggregates[key] = current
                        continue
                    duplicate_count += 1
                    current["_aggregate_count"] = int(current.get("_aggregate_count") or 1) + 1
                    current["coin_qty"] = float(current.get("coin_qty") or 0.0) + qty_abs
                    current["quantity"] = float(current.get("quantity") or 0.0) + qty_abs
                    current["contracts"] = float(current.get("contracts") or 0.0) + qty_abs
                    current["amount"] = (
                        float(current.get("amount") or 0.0)
                        + float(_safe_float(raw_leg.get("amount")) or 0.0)
                    )
                    current_notional = _safe_float(current.get("notional"))
                    add_notional = _safe_float(raw_leg.get("notional"))
                    if current_notional is not None or add_notional is not None:
                        current["notional"] = float(current_notional or 0.0) + float(add_notional or 0.0)
                    current["unrealized_pnl"] = (
                        float(current.get("unrealized_pnl") or 0.0)
                        + float(_safe_float(raw_leg.get("unrealized_pnl")) or 0.0)
                    )
                    current_margin = _safe_float(current.get("margin_used"))
                    add_margin = _safe_float(raw_leg.get("margin_used"))
                    if current_margin is not None or add_margin is not None:
                        current["margin_used"] = float(current_margin or 0.0) + float(add_margin or 0.0)
                return aggregates, unexpected, duplicate_count

            for cluster_key, cluster in cluster_rules.items():
                if not isinstance(cluster, Mapping):
                    continue
                if not bool(cluster.get("enabled", True)):
                    continue
                if str(cluster.get("kind") or "") != "hedged_pair":
                    continue
                symbol = normalize_symbol(cluster.get("symbol"))
                long_exchange = normalize_exchange_name(str(cluster.get("long_exchange") or ""))
                short_exchange = normalize_exchange_name(str(cluster.get("short_exchange") or ""))
                if not symbol or not long_exchange or not short_exchange:
                    continue
                for expected_leg in (
                    (symbol, long_exchange, "long"),
                    (symbol, short_exchange, "short"),
                ):
                    expected_cluster_legs.setdefault(expected_leg, []).append(str(cluster_key))

            for cluster_key, cluster in cluster_rules.items():
                if not isinstance(cluster, Mapping):
                    continue
                if not bool(cluster.get("enabled", True)):
                    continue
                if str(cluster.get("kind") or "") != "hedged_pair":
                    continue
                symbol = normalize_symbol(cluster.get("symbol"))
                long_exchange = normalize_exchange_name(str(cluster.get("long_exchange") or ""))
                short_exchange = normalize_exchange_name(str(cluster.get("short_exchange") or ""))
                if not symbol or not long_exchange or not short_exchange:
                    continue
                symbol_legs = list(grouped.get(symbol) or [])
                aggregated_legs, unexpected_legs, duplicate_visible_leg_count = _aggregate_symbol_legs(
                    symbol_legs,
                    symbol_name=symbol,
                    long_ex=long_exchange,
                    short_ex=short_exchange,
                )
                long_leg = dict(aggregated_legs.get((long_exchange, "long")) or {}) or None
                short_leg = dict(aggregated_legs.get((short_exchange, "short")) or {}) or None
                long_health = dict(self._derisk_exchange_health.get(long_exchange) or {})
                short_health = dict(self._derisk_exchange_health.get(short_exchange) or {})
                state = dict(self._derisk_cluster_state.get(cluster_key) or {})
                missing_cycles = int(state.get("missing_cycles") or 0)
                health_block = (
                    str(long_health.get("health") or "") != "healthy"
                    or str(short_health.get("health") or "") != "healthy"
                )
                overlap_conflicts = sorted(
                    {
                        key
                        for key in (
                            expected_cluster_legs.get((symbol, long_exchange, "long")) or []
                        ) + (
                            expected_cluster_legs.get((symbol, short_exchange, "short")) or []
                        )
                        if str(key) != str(cluster_key)
                    }
                )
                cluster_conflict = bool(unexpected_legs) or bool(overlap_conflicts)
                status = "healthy"
                reason = "ok"
                mismatch_ratio = None
                orphan_confirmed = False
                if cluster_conflict:
                    status = "blocked_by_cluster_conflict"
                    reason = "extra_visible_legs" if unexpected_legs else "overlapping_cluster_leg"
                    missing_cycles = 0
                elif not long_leg and not short_leg:
                    status = "missing_all_legs"
                    reason = "no_visible_positions"
                    if health_block:
                        status = "blocked_by_exchange_health"
                        reason = "exchange_health_untrusted"
                elif not long_leg or not short_leg:
                    if health_block:
                        status = "blocked_by_exchange_health"
                        reason = "exchange_health_untrusted"
                        missing_cycles = 0
                    else:
                        missing_cycles += 1
                        orphan_confirmed = missing_cycles >= confirm_cycles
                        status = "confirmed_orphan" if orphan_confirmed else "suspected_orphan"
                        reason = "single_leg_visible"
                else:
                    missing_cycles = 0
                    mismatch_ratio = qty_mismatch_ratio(
                        long_leg.get("coin_qty") or long_leg.get("quantity"),
                        short_leg.get("coin_qty") or short_leg.get("quantity"),
                    )
                    tolerance = _safe_float(cluster.get("qty_tolerance_pct"))
                    if tolerance is None or tolerance < 0:
                        tolerance = qty_tolerance_default
                    if mismatch_ratio is not None and mismatch_ratio > float(tolerance):
                        if health_block:
                            status = "blocked_by_exchange_health"
                            reason = "qty_mismatch_but_unhealthy"
                        else:
                            missing_cycles += 1
                            orphan_confirmed = missing_cycles >= confirm_cycles
                            status = "confirmed_orphan" if orphan_confirmed else "suspected_orphan"
                            reason = "qty_mismatch"
                state["missing_cycles"] = missing_cycles
                state["last_status"] = status
                state["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._derisk_cluster_state[cluster_key] = state

                cluster_notional = None
                cluster_pnl = None
                funding_to_next_usd = None
                minutes_to_event = None
                interval_minutes = None
                orphan_exchange = None
                orphan_position_side = None
                orphan_qty = None
                stress_exchange = None
                stress_status = "ok"
                stress_score = None
                action_qty = None
                candidate_score = None
                residual_status = None
                action_mode = "none"

                if long_leg and short_leg:
                    cluster_notional = sum(
                        abs(_safe_float(leg.get("amount")) or _safe_float(leg.get("notional")) or 0.0)
                        for leg in (long_leg, short_leg)
                    ) / 2.0
                    cluster_pnl = sum(_safe_float(leg.get("unrealized_pnl")) or 0.0 for leg in (long_leg, short_leg))
                    context = _auto_exit_v1_position_context([long_leg, short_leg])
                    funding_to_next_usd = _safe_float(context.get("funding_to_next_usd"))
                    minutes_to_event = _safe_float(context.get("minutes_to_event"))
                    interval_minutes = _safe_float(context.get("effective_interval_minutes"))
                    long_stress = dict(exchange_stress.get(long_exchange) or {})
                    short_stress = dict(exchange_stress.get(short_exchange) or {})
                    long_score = float(long_stress.get("stress_score") or 0.0)
                    short_score = float(short_stress.get("stress_score") or 0.0)
                    if long_score > 0 or short_score > 0:
                        stress_exchange = long_exchange if long_score >= short_score else short_exchange
                        chosen_stress = long_stress if stress_exchange == long_exchange else short_stress
                        stressed_leg = long_leg if stress_exchange == long_exchange else short_leg
                        stress_status = str(chosen_stress.get("status") or "ok")
                        stress_score = _safe_float(chosen_stress.get("stress_score"))
                        margin_relief_full = (
                            abs(_safe_float(stressed_leg.get("margin_used")) or 0.0)
                            or (abs(_safe_float(stressed_leg.get("notional")) or 0.0) / max(float(_safe_float(stressed_leg.get("leverage")) or 3.0), 1.0))
                        )
                        deficit_usd = _safe_float(chosen_stress.get("deficit_usd")) or 0.0
                        if margin_relief_full > 0 and deficit_usd > 0:
                            action_fraction = min(1.0, float(deficit_usd) / float(margin_relief_full))
                            pair_qty = min(
                                abs(_safe_float(long_leg.get("coin_qty")) or _safe_float(long_leg.get("quantity")) or 0.0),
                                abs(_safe_float(short_leg.get("coin_qty")) or _safe_float(short_leg.get("quantity")) or 0.0),
                            )
                            action_qty = pair_qty * action_fraction if pair_qty > 0 else None
                            if cluster_notional and pair_qty and pair_qty > 0:
                                max_notional = float(settings.get("max_single_action_notional_usd", 500.0))
                                scaled = min(float(cluster_notional) * action_fraction, max_notional)
                                action_qty = pair_qty * (scaled / float(cluster_notional)) if cluster_notional > 0 else action_qty
                            candidate_score = derisk_candidate_score(
                                margin_relief_usd=min(float(margin_relief_full), float(deficit_usd)),
                                close_cost_usd=max(0.0, -(float(cluster_pnl or 0.0))),
                                funding_to_next_usd=funding_to_next_usd,
                                minutes_to_funding=minutes_to_event,
                                interval_minutes=interval_minutes,
                                pressure_credit_usd=5.0 if stress_status == "panic" else 0.0,
                            )
                            residual_qty = max(0.0, pair_qty - float(action_qty or 0.0))
                            residual_notional = (
                                float(cluster_notional) * (residual_qty / pair_qty)
                                if cluster_notional and pair_qty > 0
                                else None
                            )
                            residual_status = classify_residual_leg(
                                qty=residual_qty,
                                notional_usd=residual_notional,
                                dust_notional_usd=float(settings.get("dust_notional_usd", 10.0)),
                            )
                            action_mode = "partial"
                            max_notional = float(settings.get("max_single_action_notional_usd", 500.0))
                            if residual_status in {
                                "dust_suspect",
                                "precision_blocked",
                                "below_min_qty",
                                "below_min_notional",
                            }:
                                allow_full_cleanup = (
                                    str(stress_status) == "panic"
                                    or (cluster_notional is not None and float(cluster_notional) <= max_notional)
                                )
                                if allow_full_cleanup and pair_qty > 0:
                                    action_qty = pair_qty
                                    residual_status = "flat"
                                    action_mode = "full_cleanup"
                            candidates.append(
                                {
                                    "key": cluster_key,
                                    "symbol": symbol,
                                    "long_exchange": long_exchange,
                                    "short_exchange": short_exchange,
                                    "stress_exchange": stress_exchange,
                                    "stress_status": stress_status,
                                    "cluster_notional_usd": cluster_notional,
                                    "cluster_unrealized_pnl_usd": cluster_pnl,
                                    "funding_to_next_usd": funding_to_next_usd,
                                    "minutes_to_event": minutes_to_event,
                                    "interval_minutes": interval_minutes,
                                    "action_qty": action_qty,
                                    "action_mode": action_mode,
                                    "candidate_score": candidate_score,
                                    "residual_status": residual_status,
                                    "stress_score": stress_score,
                                }
                            )
                if status in {"suspected_orphan", "confirmed_orphan"}:
                    if long_leg and not short_leg:
                        orphan_exchange = long_exchange
                        orphan_position_side = "long"
                        orphan_qty = abs(
                            _safe_float(long_leg.get("coin_qty"))
                            or _safe_float(long_leg.get("quantity"))
                            or 0.0
                        )
                    elif short_leg and not long_leg:
                        orphan_exchange = short_exchange
                        orphan_position_side = "short"
                        orphan_qty = abs(
                            _safe_float(short_leg.get("coin_qty"))
                            or _safe_float(short_leg.get("quantity"))
                            or 0.0
                        )
                    elif long_leg and short_leg and mismatch_ratio is not None:
                        long_qty = abs(
                            _safe_float(long_leg.get("coin_qty"))
                            or _safe_float(long_leg.get("quantity"))
                            or 0.0
                        )
                        short_qty = abs(
                            _safe_float(short_leg.get("coin_qty"))
                            or _safe_float(short_leg.get("quantity"))
                            or 0.0
                        )
                        if long_qty > short_qty:
                            orphan_exchange = long_exchange
                            orphan_position_side = "long"
                            orphan_qty = max(0.0, long_qty - short_qty)
                        elif short_qty > long_qty:
                            orphan_exchange = short_exchange
                            orphan_position_side = "short"
                            orphan_qty = max(0.0, short_qty - long_qty)
                    if (
                        status == "confirmed_orphan"
                        and bool(settings.get("orphan_cleanup_enabled", True))
                        and orphan_exchange
                        and orphan_position_side
                        and orphan_qty
                        and float(orphan_qty) > 0
                    ):
                        orphan_targets.append(
                            {
                                "key": cluster_key,
                                "symbol": symbol,
                                "orphan_exchange": orphan_exchange,
                                "orphan_position_side": orphan_position_side,
                                "orphan_qty": float(orphan_qty),
                                "reason": reason,
                                "long_exchange": long_exchange,
                                "short_exchange": short_exchange,
                            }
                        )

                diagnostics.append(
                    {
                        "key": cluster_key,
                        "kind": "cluster",
                        "symbol": symbol,
                        "source": cluster.get("source"),
                        "long_exchange": long_exchange,
                        "short_exchange": short_exchange,
                        "status": status,
                        "reason": reason,
                        "health_blocked": health_block,
                        "long_health": long_health.get("health"),
                        "short_health": short_health.get("health"),
                        "long_error_kind": long_health.get("last_error_kind"),
                        "short_error_kind": short_health.get("last_error_kind"),
                        "missing_cycles": missing_cycles,
                        "confirm_cycles": confirm_cycles,
                        "qty_mismatch_ratio": mismatch_ratio,
                        "cluster_conflict": cluster_conflict,
                        "cluster_conflict_reason": reason if status == "blocked_by_cluster_conflict" else None,
                        "overlap_conflicts": overlap_conflicts,
                        "unexpected_legs": unexpected_legs,
                        "unexpected_leg_count": len(unexpected_legs),
                        "duplicate_visible_leg_count": duplicate_visible_leg_count,
                        "cluster_notional_usd": cluster_notional,
                        "cluster_unrealized_pnl_usd": cluster_pnl,
                        "funding_to_next_usd": funding_to_next_usd,
                        "minutes_to_event": minutes_to_event,
                        "orphan_exchange": orphan_exchange,
                        "orphan_position_side": orphan_position_side,
                        "orphan_qty": orphan_qty,
                        "stress_exchange": stress_exchange,
                        "stress_status": stress_status,
                        "stress_score": stress_score,
                        "action_qty": action_qty,
                        "action_mode": action_mode,
                        "candidate_score": candidate_score,
                        "residual_status": residual_status,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

                if status in {
                    "suspected_orphan",
                    "confirmed_orphan",
                    "blocked_by_exchange_health",
                    "blocked_by_cluster_conflict",
                }:
                    self._derisk_log_event(
                        cluster_key,
                        "cluster_status",
                        {
                            "symbol": symbol,
                            "status": status,
                            "reason": reason,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "orphan_exchange": orphan_exchange,
                            "orphan_position_side": orphan_position_side,
                            "orphan_qty": orphan_qty,
                            "cluster_conflict": cluster_conflict,
                            "overlap_conflicts": overlap_conflicts,
                            "unexpected_leg_count": len(unexpected_legs),
                            "long_health": long_health.get("health"),
                            "short_health": short_health.get("health"),
                        },
                        now_ts,
                    )

            orphan_targets.sort(
                key=lambda item: (
                    -float(item.get("orphan_qty") or 0.0),
                    str(item.get("symbol") or ""),
                )
            )
            for item in orphan_targets:
                diagnostics.append(
                    {
                        **dict(item),
                        "kind": "orphan_candidate",
                        "status": "ranked",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
            candidates.sort(
                key=lambda item: (
                    {"panic": 0, "stress": 1, "ok": 2}.get(str(item.get("stress_status") or "ok"), 3),
                    float(item.get("candidate_score")) if item.get("candidate_score") is not None else float("inf"),
                )
            )
            for item in candidates:
                diagnostics.append(
                    {
                        **dict(item),
                        "kind": "candidate",
                        "status": "ranked",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            if (
                orphan_targets
                and bool(settings.get("enabled"))
                and not bool(settings.get("shadow_mode"))
                and bool(settings.get("orphan_cleanup_enabled", True))
            ):
                top_orphan = orphan_targets[0]
                running = self._auto_exit_running_exec()
                if running:
                    await self.manual_exec_stop(
                        str(running.get("execution_id") or ""),
                        force_finalize=True,
                        reason="orphan_cleanup_priority",
                    )
                    self._derisk_log_event(
                        str(top_orphan.get("key") or "global"),
                        "preempt_requested",
                        {
                            "symbol": top_orphan.get("symbol"),
                            "execution_id": running.get("execution_id"),
                            "reason": "orphan_cleanup_priority",
                            "orphan_exchange": top_orphan.get("orphan_exchange"),
                            "orphan_qty": top_orphan.get("orphan_qty"),
                        },
                        now_ts,
                    )
                    cycle_action = {
                        "type": "preempt_requested",
                        "priority": "orphan",
                        "reason": "orphan_cleanup_priority",
                        "execution_id": running.get("execution_id"),
                        "symbol": top_orphan.get("symbol"),
                        "orphan_exchange": top_orphan.get("orphan_exchange"),
                        "orphan_qty": top_orphan.get("orphan_qty"),
                    }
                else:
                    top_state = dict(self._derisk_cluster_state.get(str(top_orphan.get("key") or "")) or {})
                    last_action_ts = float(top_state.get("last_action_ts") or 0.0)
                    cooldown_sec = int(settings.get("cooldown_sec", 120))
                    if (now_ts - last_action_ts) >= cooldown_sec:
                        top_state["last_action_ts"] = now_ts
                        self._derisk_cluster_state[str(top_orphan.get("key") or "")] = top_state
                        payload = {
                            "symbol": top_orphan.get("symbol"),
                            "qty": float(top_orphan.get("orphan_qty") or 0.0),
                            "cleanup_exchange": top_orphan.get("orphan_exchange"),
                            "cleanup_position_side": top_orphan.get("orphan_position_side"),
                            "panic_cleanup_mode": True,
                            "max_slippage_bps": 18.0,
                            "async_run": True,
                            "dry_run": False,
                            "auto_exit_agent": False,
                            "risk_emergency_agent": True,
                            "margin_mode": "isolated",
                        }
                        result = await self.manual_orphan_cleanup(payload)
                        self._derisk_event(
                            "orphan_trigger",
                            {
                                "symbol": top_orphan.get("symbol"),
                                "key": top_orphan.get("key"),
                                "orphan_exchange": top_orphan.get("orphan_exchange"),
                                "orphan_position_side": top_orphan.get("orphan_position_side"),
                                "orphan_qty": top_orphan.get("orphan_qty"),
                                "reason": top_orphan.get("reason"),
                                "result": result,
                            },
                        )
                        cycle_action = {
                            "type": "orphan_trigger",
                            "symbol": top_orphan.get("symbol"),
                            "key": top_orphan.get("key"),
                            "orphan_exchange": top_orphan.get("orphan_exchange"),
                            "orphan_position_side": top_orphan.get("orphan_position_side"),
                            "orphan_qty": top_orphan.get("orphan_qty"),
                            "result": self._compact_derisk_result(result),
                        }
            elif candidates:
                top = candidates[0]
                should_execute = (
                    bool(settings.get("enabled"))
                    and not bool(settings.get("shadow_mode"))
                    and str(top.get("stress_status") or "") in {"stress", "panic"}
                    and top.get("action_qty")
                    and top.get("candidate_score") is not None
                )
                if should_execute:
                    running = self._auto_exit_running_exec()
                    if running:
                        await self.manual_exec_stop(
                            str(running.get("execution_id") or ""),
                            force_finalize=True,
                            reason="emergency_derisk_priority",
                        )
                        self._derisk_log_event(
                            str(top.get("key") or "global"),
                            "preempt_requested",
                            {
                                "symbol": top.get("symbol"),
                                "execution_id": running.get("execution_id"),
                                "reason": "emergency_derisk_priority",
                            },
                            now_ts,
                        )
                        cycle_action = {
                            "type": "preempt_requested",
                            "priority": "stress",
                            "reason": "emergency_derisk_priority",
                            "execution_id": running.get("execution_id"),
                            "symbol": top.get("symbol"),
                            "stress_exchange": top.get("stress_exchange"),
                            "stress_status": top.get("stress_status"),
                            "action_qty": top.get("action_qty"),
                        }
                    else:
                        top_state = dict(self._derisk_cluster_state.get(str(top.get("key") or "")) or {})
                        last_action_ts = float(top_state.get("last_action_ts") or 0.0)
                        cooldown_sec = int(settings.get("cooldown_sec", 120))
                        if (now_ts - last_action_ts) >= cooldown_sec:
                            top_state["last_action_ts"] = now_ts
                            self._derisk_cluster_state[str(top.get("key") or "")] = top_state
                            payload = {
                                "symbol": top.get("symbol"),
                                "qty": float(top.get("action_qty") or 0.0),
                                "notional": None,
                                "mode": "smart-exit",
                                "max_slippage_bps": 12 if str(top.get("stress_status")) == "panic" else 8,
                                "spread_min_pct": -100.0,
                                "spread_max_pct": 100.0,
                                "timeout_sec": 0,
                                "max_runtime_sec": 90 if str(top.get("stress_status")) == "panic" else 180,
                                "reprice_sec": 2,
                                "chunk_qty": None,
                                "chunk_notional": float(settings.get("max_single_action_notional_usd", 500.0)),
                                "force_chunk_qty": False,
                                "limit_offset_bps": 0.0,
                                "limit_offset_ticks": 0,
                                "use_orderbook_check": False,
                                "fallback_to_market": (
                                    str(top.get("stress_status")) == "panic"
                                    and bool(settings.get("market_cleanup_only_in_emergency", True))
                                ),
                                "hedge_order_type": "limit",
                                "hedge_limit_mode": "aggressive" if str(top.get("stress_status")) == "panic" else "passive",
                                "hedge_favorable_bps": 1.0,
                                "hedge_adverse_bps": 6.0 if str(top.get("stress_status")) == "panic" else 4.0,
                                "hedge_reprice_min_sec": 3.0,
                                "max_limit_deviation_bps": 25.0,
                                "async_run": True,
                                "dry_run": False,
                                "auto_exit_agent": False,
                                "risk_emergency_agent": True,
                                "long_exchange": top.get("long_exchange"),
                                "short_exchange": top.get("short_exchange"),
                                "margin_mode": "isolated",
                            }
                            result = await self.manual_exit(payload)
                            self._derisk_event(
                                "trigger",
                                {
                                    "symbol": top.get("symbol"),
                                    "key": top.get("key"),
                                    "stress_exchange": top.get("stress_exchange"),
                                    "stress_status": top.get("stress_status"),
                                    "action_qty": top.get("action_qty"),
                                    "candidate_score": top.get("candidate_score"),
                                    "result": result,
                                },
                            )
                            cycle_action = {
                                "type": "trigger",
                                "symbol": top.get("symbol"),
                                "key": top.get("key"),
                                "stress_exchange": top.get("stress_exchange"),
                                "stress_status": top.get("stress_status"),
                                "action_qty": top.get("action_qty"),
                                "candidate_score": top.get("candidate_score"),
                                "result": self._compact_derisk_result(result),
                            }

            self._derisk_diagnostics = diagnostics
            self._evaluate_pending_derisk_outcomes(
                now_ts=now_ts,
                diagnostics=diagnostics,
                balances=balances,
            )
            self._persist_derisk_cycle_history(
                cycle_id=cycle_id,
                settings=settings,
                balances=balances,
                exchange_stress=exchange_stress,
                diagnostics=diagnostics,
                cycle_action=cycle_action,
                running_execution=self._auto_exit_running_exec(),
            )
            self._register_derisk_outcome_tracking(
                cycle_id=cycle_id,
                now_ts=now_ts,
                cycle_action=cycle_action,
                diagnostics=diagnostics,
                balances=balances,
            )
        finally:
            if self._derisk_active_cycle_id == cycle_id:
                self._derisk_active_cycle_id = None
            self._derisk_inflight = False

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

    async def _cleanup_completed_auto_exit_spread_rules(self) -> None:
        for exec_id, run in list(self._manual_runs.items()):
            if exec_id in self._auto_exit_completed_run_cleanup:
                continue
            if run.get("action") != "exit" or not bool(run.get("auto_exit_agent")):
                continue
            status = str(run.get("status") or "")
            if status == "running":
                continue
            self._auto_exit_completed_run_cleanup.add(exec_id)
            if status != "completed":
                continue
            symbol = normalize_symbol(str(run.get("payload_symbol") or ""))
            if not symbol:
                continue
            result = run.get("result")
            if isinstance(result, Mapping) and result.get("errors"):
                continue
            result_remaining_qty = (
                _safe_float(result.get("remaining_qty"))
                if isinstance(result, Mapping)
                else None
            )
            requested_qty = _safe_float(run.get("auto_exit_requested_qty"))
            if requested_qty is None:
                requested_qty = _safe_float(run.get("auto_exit_hedged_qty"))
                exit_percent = _safe_float(run.get("auto_exit_exit_percent"))
                if requested_qty is not None and exit_percent is not None:
                    requested_qty = requested_qty * min(100.0, max(0.0, exit_percent)) / 100.0
            completion_tolerance = max(
                1e-9,
                (float(requested_qty) * 0.01) if requested_qty is not None else 1e-9,
            )
            one_shot_fulfilled = (
                result_remaining_qty is not None
                and float(result_remaining_qty) <= completion_tolerance
            )
            rule_key = str(run.get("auto_exit_rule_key") or "")
            run_generation = max(0, int(run.get("auto_exit_rule_generation") or 0))
            if not rule_key:
                self._auto_exit_event(
                    "rule_preserved_after_exit",
                    {
                        "symbol": symbol,
                        "execution_id": exec_id,
                        "reason": "run_rule_identity_missing",
                    },
                )
                continue

            positions = self._accounts.snapshot().get("positions") or []
            market_lookup, market_ts_lookup = self._positions_market_snapshot_lookup()
            _, grouped = self._positions_by_symbol(
                positions,
                return_grouped=True,
                market_lookup=market_lookup,
                market_ts_lookup=market_ts_lookup,
            )
            residual_legs = grouped.get(symbol) or []
            now_iso = datetime.now(timezone.utc).isoformat()
            event_payload: dict[str, Any] = {
                "symbol": symbol,
                "execution_id": exec_id,
                "rule_key": rule_key,
                "run_generation": run_generation,
                "residual_leg_count": len(residual_legs),
                "remaining_qty": result_remaining_qty,
                "one_shot_fulfilled": one_shot_fulfilled,
            }
            async with self._auto_exit_lock:
                stored_rules = self._auto_exit.get("rules", {})
                stored = stored_rules.get(rule_key) if isinstance(stored_rules, dict) else None
                if not isinstance(stored, dict):
                    event_payload["reason"] = "rule_missing"
                else:
                    current_generation = max(1, int(stored.get("rule_generation") or 1))
                    event_payload["current_generation"] = current_generation
                    if run_generation and current_generation != run_generation:
                        event_payload["reason"] = "rule_changed_after_run_started"
                    else:
                        trigger_mode = str(run.get("auto_exit_trigger_mode") or "spread")
                        target_field = "v1_target_qty" if trigger_mode == "v1" else "spread_target_qty"
                        remaining_field = "v1_remaining_qty" if trigger_mode == "v1" else "spread_remaining_qty"
                        fixed_target_qty = _safe_float(stored.get(target_field)) or requested_qty
                        completion_tolerance = max(
                            1e-9,
                            float(fixed_target_qty or 0.0) * 0.01,
                        )
                        one_shot_fulfilled = (
                            result_remaining_qty is not None
                            and float(result_remaining_qty) <= completion_tolerance
                        )
                        event_payload["fixed_target_qty"] = fixed_target_qty
                        event_payload["completion_tolerance_qty"] = completion_tolerance
                        event_payload["dust_completed"] = bool(
                            one_shot_fulfilled
                            and result_remaining_qty is not None
                            and float(result_remaining_qty) > 0
                        )
                        stored[target_field] = fixed_target_qty
                        stored[remaining_field] = (
                            0.0 if one_shot_fulfilled else result_remaining_qty
                        )
                    if not (run_generation and current_generation != run_generation) and residual_legs:
                        current_signature = _auto_exit_position_signature(
                            symbol,
                            residual_legs,
                            rule_long_exchange=str(stored.get("long_exchange") or ""),
                            rule_short_exchange=str(stored.get("short_exchange") or ""),
                            selected_pair=_auto_exit_select_pair_from_legs(residual_legs),
                        )
                        if current_signature:
                            stored["position_signature"] = current_signature
                            stored["bound_at"] = now_iso
                            stored["signature_status"] = "rebound_after_partial_auto_exit"
                            event_payload["reason"] = "residual_pair_rebound"
                        else:
                            stored["signature_status"] = "residual_position_after_auto_exit"
                            event_payload["reason"] = "residual_position_preserved"
                        stored["missing_since_ts"] = 0.0
                        stored["updated_at"] = now_iso
                        if bool(stored.get("exit_once", True)) and one_shot_fulfilled:
                            stored["enabled"] = False
                            stored["v1_enabled"] = False
                            stored["signature_status"] = "one_shot_completed"
                            event_payload["reason"] = "one_shot_completed_residual_pair"
                        elif result_remaining_qty is not None and result_remaining_qty > completion_tolerance:
                            event_payload["reason"] = "partial_exit_remaining"
                        self._auto_exit_store.save(self._auto_exit)
                    elif not (run_generation and current_generation != run_generation):
                        if bool(stored.get("exit_once", True)) and one_shot_fulfilled:
                            stored["enabled"] = False
                            stored["v1_enabled"] = False
                            stored["signature_status"] = "one_shot_completed"
                            event_payload["reason"] = "one_shot_completed_no_position"
                        else:
                            stored["signature_status"] = "awaiting_position_restore_after_auto_exit"
                            event_payload["reason"] = (
                                "partial_exit_remaining"
                                if result_remaining_qty is not None
                                and result_remaining_qty > completion_tolerance
                                else "no_position_rule_preserved"
                            )
                        stored["missing_since_ts"] = time.time()
                        stored["updated_at"] = now_iso
                        self._auto_exit_store.save(self._auto_exit)
            self._auto_exit_event("rule_preserved_after_exit", event_payload)

    async def _auto_exit_scheduler(self) -> None:
        while True:
            try:
                await self._auto_exit_cycle()
                await self._auto_strategy_cycle()
                await self._auto_arb_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("auto-agent loop failed: %s", exc)
            await asyncio.sleep(self._auto_exit_poll_sec)

    async def _auto_exit_cycle(self) -> None:
        if self._auto_exit_inflight:
            return
        self._auto_exit_inflight = True
        try:
            await self._cleanup_completed_auto_exit_spread_rules()
            async with self._auto_exit_lock:
                config = json.loads(json.dumps(self._auto_exit))
            positions = self._accounts.snapshot().get("positions") or []
            market_lookup, market_ts_lookup = self._positions_market_snapshot_lookup()
            _, grouped = self._positions_by_symbol(
                positions,
                return_grouped=True,
                market_lookup=market_lookup,
                market_ts_lookup=market_ts_lookup,
            )
            rules = config.get("rules") or {}
            defaults = config.get("defaults") or {}
            max_runtime_sec = int(defaults.get("max_runtime_sec", AUTO_EXIT_DEFAULTS["max_runtime_sec"]))
            cooldown_sec = int(defaults.get("cooldown_sec", AUTO_EXIT_DEFAULTS["cooldown_sec"]))
            require_live = bool(defaults.get("require_live", AUTO_EXIT_DEFAULTS["require_live"]))
            auto_clear_sec = int(defaults.get("auto_clear_no_position_sec", AUTO_EXIT_DEFAULTS["auto_clear_no_position_sec"]))
            restore_spread_on_missing = bool(
                defaults.get(
                    "restore_spread_on_missing",
                    AUTO_EXIT_DEFAULTS["restore_spread_on_missing"],
                )
            )
            now_ts = time.time()

            live_spreads: dict[str, float] = {}
            live_mid_cache: dict[tuple[str, str], float | None] = {}
            live_book_cache: dict[tuple[str, str], dict[str, Any] | None] = {}

            async def resolve_leg_book(symbol_name: str, exchange: str) -> dict[str, Any] | None:
                key = (normalize_symbol(symbol_name), normalize_exchange_name(exchange))
                if key in live_book_cache:
                    return live_book_cache.get(key)
                book = await self._market_data.get_orderbook(exchange, symbol_name, depth=20, max_age_sec=15.0)
                live_book_cache[key] = dict(book) if book else None
                return live_book_cache.get(key)

            async def resolve_leg_mid(symbol_name: str, exchange: str) -> float | None:
                key = (normalize_symbol(symbol_name), normalize_exchange_name(exchange))
                if key in live_mid_cache:
                    return live_mid_cache.get(key)
                book = await resolve_leg_book(symbol_name, exchange)
                if not book:
                    live_mid_cache[key] = None
                    return None
                bids = book.get("bids") or []
                asks = book.get("asks") or []
                if not bids or not asks:
                    live_mid_cache[key] = None
                    return None
                try:
                    bid = float(bids[0][0])
                    ask = float(asks[0][0])
                except Exception:
                    live_mid_cache[key] = None
                    return None
                if bid <= 0 or ask <= 0:
                    live_mid_cache[key] = None
                    return None
                mid = (bid + ask) / 2.0
                live_mid_cache[key] = mid
                return mid

            async def resolve_pair_exit_metrics(
                symbol_name: str,
                long_ex: str,
                short_ex: str,
                qty_base: float,
            ) -> dict[str, Any] | None:
                long_book = await resolve_leg_book(symbol_name, long_ex)
                short_book = await resolve_leg_book(symbol_name, short_ex)
                manual_settings = getattr(self._settings_manager.current, "manual", {}) or {}
                policy = _auto_exit_policy_for_pair(long_ex, short_ex, manual_settings=manual_settings)
                fee_bps = _auto_exit_pair_fee_bps(long_ex, short_ex)
                metrics = _auto_exit_executable_metrics_from_books(
                    long_exchange=long_ex,
                    short_exchange=short_ex,
                    long_book=long_book,
                    short_book=short_book,
                    qty=qty_base,
                    max_slippage_bps=AUTO_EXIT_EXECUTABLE_MAX_SLIPPAGE_BPS,
                    fee_bps=fee_bps,
                    edge_buffer_bps=_safe_float(policy.get("edge_buffer_bps")) or 0.0,
                    chunk_notional_cap_usd=_safe_float(policy.get("chunk_notional_cap_usd")),
                )
                if metrics:
                    metrics["long_book"] = long_book
                    metrics["short_book"] = short_book
                    metrics["policy"] = policy
                    live_spreads[self._auto_exit_key(symbol_name, long_ex, short_ex)] = float(metrics["spread_pct"])
                return metrics

            async def resolve_overall_spread(symbol_name: str, legs_list: list[dict[str, Any]]) -> float | None:
                mids: dict[str, float] = {}
                exchanges: set[str] = set()
                for leg in legs_list:
                    exchange = normalize_exchange_name(str(leg.get("exchange") or ""))
                    if exchange:
                        exchanges.add(exchange)
                for exchange in exchanges:
                    mid = await resolve_leg_mid(symbol_name, exchange)
                    if mid and mid > 0:
                        mids[exchange] = mid
                spread_val = _auto_exit_overall_spread_from_legs(legs_list, mids)
                if spread_val is None:
                    spread_val = _auto_exit_overall_spread_from_legs(legs_list, {})
                if spread_val is not None:
                    live_spreads[self._auto_exit_key(symbol_name, AUTO_EXIT_MULTILEG_MARKER, AUTO_EXIT_MULTILEG_MARKER)] = float(spread_val)
                return spread_val

            async with self._auto_exit_lock:
                self._auto_exit_live_spreads = live_spreads
            rules_to_remove: set[str] = set()
            rules_to_update: dict[str, dict[str, Any]] = {}
            diagnostics_rows: list[dict[str, Any]] = []
            v1_diagnostics_rows: list[dict[str, Any]] = []

            def merge_rule_updates(rule_key: str, updates: Mapping[str, Any]) -> None:
                merged = dict(rules_to_update.get(rule_key) or {})
                merged.update(dict(updates or {}))
                rules_to_update[rule_key] = merged

            def append_diagnostic(
                rule_key: str,
                rule_state: Mapping[str, Any],
                *,
                status: str,
                reason: str | None = None,
                selected_pair: Mapping[str, Any] | None = None,
                pair_metrics: Mapping[str, Any] | None = None,
                overall_spread: float | None = None,
                trigger_spread: float | None = None,
                target_pct: float | None = None,
                required_net_spread_pct: float | None = None,
            ) -> None:
                symbol = str(rule_state.get("symbol") or "").upper().strip()
                rule_long_exchange = normalize_exchange_name(str(rule_state.get("long_exchange") or ""))
                rule_short_exchange = normalize_exchange_name(str(rule_state.get("short_exchange") or ""))
                selected_long_exchange = normalize_exchange_name(str((selected_pair or {}).get("long_exchange") or ""))
                selected_short_exchange = normalize_exchange_name(str((selected_pair or {}).get("short_exchange") or ""))
                policy = dict((pair_metrics or {}).get("policy") or {})
                if (
                    not policy
                    and rule_long_exchange
                    and rule_short_exchange
                    and not _is_auto_exit_multileg_rule(rule_long_exchange, rule_short_exchange)
                ):
                    manual_settings = getattr(self._settings_manager.current, "manual", {}) or {}
                    policy = _auto_exit_policy_for_pair(
                        rule_long_exchange,
                        rule_short_exchange,
                        manual_settings=manual_settings,
                    )
                cleanup_status = _auto_exit_market_cleanup_status(
                    long_exchange=selected_long_exchange or rule_long_exchange,
                    short_exchange=selected_short_exchange or rule_short_exchange,
                    cleanup_cap_usd=_safe_float(policy.get("market_cleanup_notional_cap_usd")),
                    estimated_notional_usd=_safe_float((pair_metrics or {}).get("chunk_notional_usd")),
                )
                execution_order = _auto_exit_execution_order(
                    long_exchange=selected_long_exchange or rule_long_exchange,
                    short_exchange=selected_short_exchange or rule_short_exchange,
                    long_book=(pair_metrics or {}).get("long_book"),
                    short_book=(pair_metrics or {}).get("short_book"),
                )
                net_spread_pct = _safe_float((pair_metrics or {}).get("net_spread_pct"))
                edge_delta_bps = _auto_exit_edge_delta_bps(
                    net_spread_pct,
                    required_net_spread_pct,
                )
                diagnostics_rows.append(
                    {
                        "key": rule_key,
                        "symbol": symbol,
                        "rule_long_exchange": rule_long_exchange,
                        "rule_short_exchange": rule_short_exchange,
                        "selected_long_exchange": selected_long_exchange or None,
                        "selected_short_exchange": selected_short_exchange or None,
                        "selection_mode": str((selected_pair or {}).get("mode") or ""),
                        "long_legs": int((selected_pair or {}).get("long_legs") or 0),
                        "short_legs": int((selected_pair or {}).get("short_legs") or 0),
                        "status": status,
                        "reason": reason,
                        "signature_status": rule_state.get("signature_status"),
                        "bound_at": rule_state.get("bound_at"),
                        "target_spread_pct": float(target_pct) if target_pct is not None else None,
                        "gross_spread_pct": float(trigger_spread) if trigger_spread is not None else None,
                        "overall_spread_pct": float(overall_spread) if overall_spread is not None else None,
                        "net_spread_pct": net_spread_pct,
                        "required_net_spread_pct": float(required_net_spread_pct) if required_net_spread_pct is not None else None,
                        "edge_delta_bps": edge_delta_bps,
                        "chunk_qty": _safe_float((pair_metrics or {}).get("chunk_qty")),
                        "chunk_notional_usd": _safe_float((pair_metrics or {}).get("chunk_notional_usd")),
                        "liquidity_cap_qty": _safe_float((pair_metrics or {}).get("liquidity_cap_qty")),
                        "safety_factor": _safe_float((pair_metrics or {}).get("safety_factor")),
                        "fee_bps": _safe_float((pair_metrics or {}).get("fee_bps")),
                        "edge_buffer_bps": _safe_float((pair_metrics or {}).get("edge_buffer_bps")),
                        "policy_key": policy.get("policy_key"),
                        "worst_tier": policy.get("worst_tier"),
                        "primary_label": execution_order.get("primary_label"),
                        "primary_exchange": execution_order.get("primary_exchange"),
                        "hedge_label": execution_order.get("hedge_label"),
                        "hedge_exchange": execution_order.get("hedge_exchange"),
                        "decision_reason": execution_order.get("reason"),
                        "chunk_notional_cap_usd": _safe_float(policy.get("chunk_notional_cap_usd")),
                        "market_cleanup_notional_cap_usd": _safe_float(policy.get("market_cleanup_notional_cap_usd")),
                        "market_cleanup_allowed": cleanup_status.get("allowed"),
                        "market_cleanup_summary": cleanup_status.get("summary"),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            def append_v1_diagnostic(
                rule_key: str,
                rule_state: Mapping[str, Any],
                *,
                status: str,
                reason: str | None = None,
                selected_pair: Mapping[str, Any] | None = None,
                pair_metrics: Mapping[str, Any] | None = None,
                decision: Mapping[str, Any] | None = None,
                window: Mapping[str, Any] | None = None,
                context: Mapping[str, Any] | None = None,
                close_now_bps: float | None = None,
                funding_to_next_bps: float | None = None,
                reversion_credit_bps: float | None = None,
                pending_exit_cycles: int | None = None,
            ) -> None:
                symbol = str(rule_state.get("symbol") or "").upper().strip()
                rule_long_exchange = normalize_exchange_name(str(rule_state.get("long_exchange") or ""))
                rule_short_exchange = normalize_exchange_name(str(rule_state.get("short_exchange") or ""))
                selected_long_exchange = normalize_exchange_name(str((selected_pair or {}).get("long_exchange") or ""))
                selected_short_exchange = normalize_exchange_name(str((selected_pair or {}).get("short_exchange") or ""))
                decision_payload = dict(decision or {})
                window_payload = dict(window or {})
                context_payload = dict(context or {})
                v1_diagnostics_rows.append(
                    {
                        "key": rule_key,
                        "symbol": symbol,
                        "rule_long_exchange": rule_long_exchange,
                        "rule_short_exchange": rule_short_exchange,
                        "selected_long_exchange": selected_long_exchange or None,
                        "selected_short_exchange": selected_short_exchange or None,
                        "selection_mode": str((selected_pair or {}).get("mode") or ""),
                        "status": status,
                        "reason": reason or decision_payload.get("reason"),
                        "signature_status": rule_state.get("signature_status"),
                        "bound_at": rule_state.get("bound_at"),
                        "decision": decision_payload.get("decision"),
                        "effective_interval_minutes": _safe_float(context_payload.get("effective_interval_minutes")),
                        "minutes_to_event": _safe_float(context_payload.get("minutes_to_event")),
                        "interval_bucket": window_payload.get("bucket"),
                        "window_stage": window_payload.get("stage"),
                        "funding_pressure_mult": _safe_float(window_payload.get("funding_pressure_mult")),
                        "reversion_credit_mult": _safe_float(window_payload.get("reversion_credit_mult")),
                        "hard_exit_negative_funding_bps": _safe_float(
                            window_payload.get("hard_exit_negative_funding_bps")
                        ),
                        "take_profit_k": _safe_float(window_payload.get("take_profit_k")),
                        "take_profit_threshold_bps": _safe_float(
                            decision_payload.get("take_profit_threshold_bps")
                        ),
                        "wait_score_bps": _safe_float(decision_payload.get("wait_score_bps")),
                        "risk_penalty_bps": _safe_float(decision_payload.get("risk_penalty_bps")),
                        "close_now_bps": _safe_float(close_now_bps),
                        "funding_to_next_bps": _safe_float(funding_to_next_bps),
                        "reversion_credit_bps": _safe_float(reversion_credit_bps),
                        "position_notional_usd": _safe_float(context_payload.get("position_notional_usd")),
                        "entry_spread_pct": _safe_float(context_payload.get("entry_spread_pct")),
                        "mark_spread_pct": _safe_float(context_payload.get("mark_spread_pct")),
                        "chunk_qty": _safe_float((pair_metrics or {}).get("chunk_qty")),
                        "chunk_notional_usd": _safe_float((pair_metrics or {}).get("chunk_notional_usd")),
                        "net_spread_pct": _safe_float((pair_metrics or {}).get("net_spread_pct")),
                        "gross_spread_pct": _safe_float((pair_metrics or {}).get("spread_pct")),
                        "pending_exit_cycles": int(pending_exit_cycles or 0),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            if not rules:
                async with self._auto_exit_lock:
                    self._auto_exit_diagnostics = []
                    self._auto_exit_v1_diagnostics = []
                return
            running = self._auto_exit_running_exec()
            if running:
                for key, rule in rules.items():
                    target = _safe_float(rule.get("target_spread_pct"))
                    spread_rule_enabled = bool(rule.get("enabled", True)) and target is not None
                    v1_rule_enabled = bool(rule.get("v1_enabled", False))
                    if spread_rule_enabled:
                        append_diagnostic(key, rule, status="running", reason="execution_running")
                    if spread_rule_enabled or v1_rule_enabled:
                        append_v1_diagnostic(
                            key,
                            rule,
                            status="running",
                            reason="execution_running",
                            pending_exit_cycles=int(rule.get("v1_pending_exit_cycles") or 0),
                        )
                async with self._auto_exit_lock:
                    self._auto_exit_diagnostics = diagnostics_rows
                    self._auto_exit_v1_diagnostics = v1_diagnostics_rows
                self._auto_exit_log_event(
                    "global",
                    "skip_running",
                    {"reason": "execution_running", **running},
                    now_ts,
                )
                return

            def mark_missing(
                rule_key: str,
                rule_state: Mapping[str, Any],
                reason: str,
                payload: dict[str, Any],
                *,
                spread_enabled: bool,
                v1_enabled: bool,
                selected_pair: Mapping[str, Any] | None = None,
                pair_metrics: Mapping[str, Any] | None = None,
            ) -> bool:
                missing_since = float(rule_state.get("missing_since_ts") or 0.0)
                persist_on_missing = bool(rule_state.get("persist_on_missing", True)) and restore_spread_on_missing
                if missing_since <= 0:
                    missing_since = now_ts
                    merge_rule_updates(rule_key, {"missing_since_ts": missing_since})
                elapsed = max(0.0, now_ts - missing_since)
                if not persist_on_missing and auto_clear_sec and elapsed >= auto_clear_sec:
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
                if not persist_on_missing and auto_clear_sec:
                    payload["auto_clear_remaining_sec"] = round(max(0.0, auto_clear_sec - elapsed), 1)
                if persist_on_missing:
                    payload["persist_on_missing"] = True
                self._auto_exit_log_event(rule_key, "skip", payload, now_ts)
                if spread_enabled:
                    append_diagnostic(
                        rule_key,
                        rule_state,
                        status="skip",
                        reason=reason,
                        selected_pair=selected_pair,
                        pair_metrics=pair_metrics,
                    )
                if v1_enabled:
                    append_v1_diagnostic(
                        rule_key,
                        rule_state,
                        status="skip",
                        reason=reason,
                        selected_pair=selected_pair,
                        pair_metrics=pair_metrics,
                        pending_exit_cycles=int(rule_state.get("v1_pending_exit_cycles") or 0),
                    )
                return True

            def clear_missing(rule_key: str, rule_state: Mapping[str, Any]) -> None:
                if float(rule_state.get("missing_since_ts") or 0.0) > 0:
                    merge_rule_updates(rule_key, {"missing_since_ts": 0.0})

            for key, rule in rules.items():
                symbol = str(rule.get("symbol") or "").upper().strip()
                symbol_key = normalize_symbol(symbol)
                long_exchange = normalize_exchange_name(str(rule.get("long_exchange") or ""))
                short_exchange = normalize_exchange_name(str(rule.get("short_exchange") or ""))
                rule_is_multileg = _is_auto_exit_multileg_rule(long_exchange, short_exchange)
                target = _safe_float(rule.get("target_spread_pct"))
                spread_rule_enabled = bool(rule.get("enabled", True)) and target is not None
                v1_rule_enabled = bool(rule.get("v1_enabled", False))
                v1_monitor_enabled = spread_rule_enabled or v1_rule_enabled
                if not symbol or not long_exchange or not short_exchange or (not spread_rule_enabled and not v1_rule_enabled):
                    continue
                spread_on_cooldown = False
                if spread_rule_enabled and cooldown_sec and (now_ts - float(rule.get("last_triggered_ts") or 0.0)) < cooldown_sec:
                    spread_on_cooldown = True
                    remaining = max(0, cooldown_sec - (now_ts - float(rule.get("last_triggered_ts") or 0.0)))
                    self._auto_exit_log_event(
                        key,
                        "skip",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "cooldown",
                            "remaining_sec": round(remaining, 1),
                            "trigger_mode": "spread",
                        },
                        now_ts,
                    )
                    append_diagnostic(key, rule, status="cooldown", reason="cooldown")
                v1_on_cooldown = False
                if v1_rule_enabled and cooldown_sec and (now_ts - float(rule.get("last_v1_triggered_ts") or 0.0)) < cooldown_sec:
                    v1_on_cooldown = True
                    remaining = max(0, cooldown_sec - (now_ts - float(rule.get("last_v1_triggered_ts") or 0.0)))
                    append_v1_diagnostic(
                        key,
                        rule,
                        status="cooldown",
                        reason="cooldown",
                        pending_exit_cycles=int(rule.get("v1_pending_exit_cycles") or 0),
                    )
                if spread_on_cooldown and v1_on_cooldown:
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
                        spread_enabled=spread_rule_enabled and not spread_on_cooldown,
                        v1_enabled=v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown),
                    )
                    continue
                selected = _auto_exit_select_pair_from_legs(legs)
                if not selected:
                    mark_missing(
                        key,
                        rule,
                        "legs_unavailable",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "legs_unavailable",
                        },
                        spread_enabled=spread_rule_enabled and not spread_on_cooldown,
                        v1_enabled=v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown),
                    )
                    continue
                selected_mode = str(selected.get("mode") or "single_pair")
                selected_long_exchange = normalize_exchange_name(str(selected.get("long_exchange") or ""))
                selected_short_exchange = normalize_exchange_name(str(selected.get("short_exchange") or ""))
                if not selected_long_exchange or not selected_short_exchange:
                    mark_missing(
                        key,
                        rule,
                        "legs_unavailable",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "legs_unavailable",
                        },
                        spread_enabled=spread_rule_enabled and not spread_on_cooldown,
                        v1_enabled=v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown),
                        selected_pair=selected,
                    )
                    continue
                if (
                    not rule_is_multileg
                    and selected_mode != "single_pair"
                ):
                    mark_missing(
                        key,
                        rule,
                        "multi_leg_pair_rule",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "multi_leg_pair_rule",
                            "selected_mode": selected_mode,
                            "long_legs": int(selected.get("long_legs") or 0),
                            "short_legs": int(selected.get("short_legs") or 0),
                        },
                        spread_enabled=spread_rule_enabled and not spread_on_cooldown,
                        v1_enabled=v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown),
                        selected_pair=selected,
                    )
                    continue
                if (
                    not rule_is_multileg
                    and (
                        selected_long_exchange != long_exchange
                        or selected_short_exchange != short_exchange
                    )
                ):
                    mark_missing(
                        key,
                        rule,
                        "legs_missing",
                        {
                            "symbol": symbol,
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                            "reason": "legs_missing",
                            "selected_long_exchange": selected_long_exchange,
                            "selected_short_exchange": selected_short_exchange,
                        },
                        spread_enabled=spread_rule_enabled and not spread_on_cooldown,
                        v1_enabled=v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown),
                        selected_pair=selected,
                    )
                    continue
                hedged_qty = float(selected.get("qty") or 0.0)
                exit_percent = _safe_float(rule.get("exit_percent")) or 100.0
                if exit_percent <= 0 or exit_percent > 100:
                    exit_percent = 100.0
                qty = hedged_qty * float(exit_percent) / 100.0
                spread_qty = min(
                    hedged_qty,
                    _safe_float(rule.get("spread_remaining_qty")) or qty,
                )
                v1_qty = min(
                    hedged_qty,
                    _safe_float(rule.get("v1_remaining_qty")) or qty,
                )
                if hedged_qty <= 0 or qty <= 0:
                    mark_missing(
                        key,
                        rule,
                        "zero_qty",
                        {
                            "symbol": symbol,
                            "long_exchange": selected_long_exchange,
                            "short_exchange": selected_short_exchange,
                            "reason": "zero_qty",
                            "hedged_qty": hedged_qty,
                            "exit_percent": exit_percent,
                        },
                        spread_enabled=spread_rule_enabled and not spread_on_cooldown,
                        v1_enabled=v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown),
                        selected_pair=selected,
                    )
                    continue
                clear_missing(key, rule)
                current_signature = _auto_exit_position_signature(
                    symbol_key,
                    legs,
                    rule_long_exchange=long_exchange,
                    rule_short_exchange=short_exchange,
                    selected_pair=selected,
                )
                signature_ok, signature_reason = _auto_exit_signature_match(
                    rule.get("position_signature"),
                    current_signature,
                )
                if not signature_ok:
                    merge_rule_updates(
                        key,
                        {
                            "signature_status": signature_reason,
                            "missing_since_ts": 0.0,
                        },
                    )
                    self._auto_exit_log_event(
                        key,
                        "skip",
                        {
                            "symbol": symbol,
                            "long_exchange": selected_long_exchange,
                            "short_exchange": selected_short_exchange,
                            "rule_long_exchange": long_exchange,
                            "rule_short_exchange": short_exchange,
                            "reason": signature_reason,
                            "trigger_mode": "signature_guard",
                        },
                        now_ts,
                    )
                    if spread_rule_enabled and not spread_on_cooldown:
                        append_diagnostic(
                            key,
                            {**dict(rule), "signature_status": signature_reason},
                            status="skip",
                            reason=signature_reason,
                            selected_pair=selected,
                        )
                    if v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown):
                        append_v1_diagnostic(
                            key,
                            {**dict(rule), "signature_status": signature_reason},
                            status="skip",
                            reason=signature_reason,
                            selected_pair=selected,
                            pending_exit_cycles=int(rule.get("v1_pending_exit_cycles") or 0),
                        )
                    continue
                if str(rule.get("signature_status") or "") != "position_signature_match":
                    merge_rule_updates(key, {"signature_status": "position_signature_match"})
                pair_exit_metrics = await resolve_pair_exit_metrics(
                    symbol_key,
                    selected_long_exchange,
                    selected_short_exchange,
                    qty,
                )
                selected_pair_spread = _safe_float((pair_exit_metrics or {}).get("spread_pct"))
                selected_pair_net_spread = _safe_float((pair_exit_metrics or {}).get("net_spread_pct"))
                pair_chunk_qty = _safe_float((pair_exit_metrics or {}).get("chunk_qty"))
                pair_chunk_notional = _safe_float((pair_exit_metrics or {}).get("chunk_notional_usd"))
                pair_policy = (pair_exit_metrics or {}).get("policy") or {}
                edge_buffer_bps = _safe_float((pair_exit_metrics or {}).get("edge_buffer_bps")) or 0.0
                fee_bps = _safe_float((pair_exit_metrics or {}).get("fee_bps")) or 0.0
                edge_buffer_pct = _safe_float((pair_exit_metrics or {}).get("edge_buffer_pct")) or 0.0
                overall_spread = None
                if rule_is_multileg:
                    overall_spread = await resolve_overall_spread(symbol_key, legs)
                spread_trigger = _auto_exit_spread_trigger_status(
                    is_multileg=rule_is_multileg,
                    target_pct=float(target) if target is not None else 0.0,
                    overall_spread_pct=overall_spread,
                    pair_spread_pct=selected_pair_spread,
                    pair_net_spread_pct=selected_pair_net_spread,
                    edge_buffer_pct=edge_buffer_pct,
                )
                trigger_spread = _safe_float(spread_trigger.get("trigger_spread_pct"))
                required_net_spread_pct = _safe_float(spread_trigger.get("required_spread_pct"))
                spread_scope = str(spread_trigger.get("scope") or "pair_executable")
                position_context: dict[str, Any] | None = None
                funding_to_next_bps = None
                close_now_bps = None
                reversion_credit_bps = None
                window: dict[str, Any] | None = None
                v1_decision: dict[str, Any] | None = None
                pending_cycles = max(0, int(rule.get("v1_pending_exit_cycles") or 0))
                if v1_monitor_enabled and (not v1_rule_enabled or not v1_on_cooldown):
                    position_context = _auto_exit_v1_position_context(legs)
                    position_notional_usd = _safe_float(position_context.get("position_notional_usd"))
                    funding_to_next_usd = _safe_float(position_context.get("funding_to_next_usd"))
                    if (
                        funding_to_next_usd is not None
                        and position_notional_usd is not None
                        and position_notional_usd > 0
                    ):
                        funding_to_next_bps = float(funding_to_next_usd) / float(position_notional_usd) * 10000.0
                    close_now_bps = (
                        float(selected_pair_net_spread) * 100.0
                        if selected_pair_net_spread is not None
                        else None
                    )
                    entry_spread_pct = _safe_float(position_context.get("entry_spread_pct"))
                    mark_spread_pct = _safe_float(position_context.get("mark_spread_pct"))
                    if entry_spread_pct is not None and mark_spread_pct is not None:
                        reversion_credit_bps = min(
                            float(AUTO_EXIT_V1_REVERSION_CREDIT_CAP_BPS),
                            max(0.0, (float(entry_spread_pct) - float(mark_spread_pct)) * 100.0),
                        )
                    window = _auto_exit_v1_window(
                        position_context.get("effective_interval_minutes"),
                        position_context.get("minutes_to_event"),
                    )
                    v1_decision = _auto_exit_v1_decision(
                        close_now_bps=close_now_bps,
                        funding_to_next_bps=funding_to_next_bps,
                        reversion_credit_bps=reversion_credit_bps,
                        window=window,
                    )
                standard_triggered = False
                if spread_rule_enabled and not spread_on_cooldown:
                    if not bool(spread_trigger.get("live_ready")) and require_live:
                        self._auto_exit_log_event(
                            key,
                            "skip",
                            {
                                "symbol": symbol,
                                "long_exchange": selected_long_exchange,
                                "short_exchange": selected_short_exchange,
                                "rule_long_exchange": long_exchange,
                                "rule_short_exchange": short_exchange,
                                "selection_mode": selected_mode,
                                "spread_scope": spread_scope,
                                "chunk_qty": pair_chunk_qty,
                                "chunk_notional_usd": pair_chunk_notional,
                                "net_spread_pct": selected_pair_net_spread,
                                "required_net_spread_pct": required_net_spread_pct,
                                "fee_bps": fee_bps,
                                "edge_buffer_bps": edge_buffer_bps,
                                "policy": pair_policy,
                                "reason": "live_missing",
                                "trigger_mode": "spread",
                            },
                            now_ts,
                        )
                        append_diagnostic(
                            key,
                            rule,
                            status="skip",
                            reason="live_missing",
                            selected_pair=selected,
                            pair_metrics=pair_exit_metrics,
                            overall_spread=overall_spread,
                            trigger_spread=trigger_spread,
                            target_pct=float(target),
                            required_net_spread_pct=required_net_spread_pct,
                        )
                    elif trigger_spread is None:
                        append_diagnostic(
                            key,
                            rule,
                            status="skip",
                            reason="spread_unavailable",
                            selected_pair=selected,
                            pair_metrics=pair_exit_metrics,
                            overall_spread=overall_spread,
                            trigger_spread=trigger_spread,
                            target_pct=float(target),
                            required_net_spread_pct=required_net_spread_pct,
                        )
                    elif not bool(spread_trigger.get("target_reached")):
                        self._auto_exit_log_event(
                            key,
                            "wait",
                            {
                                "symbol": symbol,
                                "long_exchange": selected_long_exchange,
                                "short_exchange": selected_short_exchange,
                                "rule_long_exchange": long_exchange,
                                "rule_short_exchange": short_exchange,
                                "selection_mode": selected_mode,
                                "spread_scope": spread_scope,
                                "spread_pct": float(trigger_spread),
                                "overall_spread_pct": float(overall_spread) if overall_spread is not None else None,
                                "pair_spread_pct": float(selected_pair_spread) if selected_pair_spread is not None else None,
                                "net_spread_pct": selected_pair_net_spread,
                                "required_net_spread_pct": required_net_spread_pct,
                                "chunk_qty": pair_chunk_qty,
                                "chunk_notional_usd": pair_chunk_notional,
                                "fee_bps": fee_bps,
                                "edge_buffer_bps": edge_buffer_bps,
                                "policy": pair_policy,
                                "pair_exit_metrics": pair_exit_metrics,
                                "target_pct": float(target),
                                "trigger_mode": "spread",
                            },
                            now_ts,
                        )
                        append_diagnostic(
                            key,
                            rule,
                            status="wait",
                            reason="target_not_reached",
                            selected_pair=selected,
                            pair_metrics=pair_exit_metrics,
                            overall_spread=overall_spread,
                            trigger_spread=trigger_spread,
                            target_pct=float(target),
                            required_net_spread_pct=required_net_spread_pct,
                        )
                    else:
                        payload_spread_min = float(target) + (fee_bps + edge_buffer_bps) / 100.0
                        if rule_is_multileg and selected_pair_spread is not None:
                            payload_spread_min = max(
                                payload_spread_min,
                                float(selected_pair_spread) - AUTO_EXIT_MULTILEG_PAIR_BUFFER_PCT,
                            )
                        if not self._auto_exit_has_running():
                            self._auto_exit_event(
                                "trigger",
                                {
                                    "symbol": symbol,
                                    "long_exchange": selected_long_exchange,
                                    "short_exchange": selected_short_exchange,
                                    "rule_long_exchange": long_exchange,
                                    "rule_short_exchange": short_exchange,
                                    "selection_mode": selected_mode,
                                    "long_legs": int(selected.get("long_legs") or 0),
                                    "short_legs": int(selected.get("short_legs") or 0),
                                    "selected_min_side": selected.get("selected_min_side"),
                                    "selected_min_exchange": selected.get("selected_min_exchange"),
                                    "selected_min_qty": selected.get("selected_min_qty"),
                                    "spread_scope": spread_scope,
                                    "spread_pct": float(trigger_spread),
                                    "overall_spread_pct": float(overall_spread) if overall_spread is not None else None,
                                    "pair_spread_pct": float(selected_pair_spread) if selected_pair_spread is not None else None,
                                    "net_spread_pct": selected_pair_net_spread,
                                    "required_net_spread_pct": required_net_spread_pct,
                                    "chunk_qty": pair_chunk_qty,
                                    "chunk_notional_usd": pair_chunk_notional,
                                    "fee_bps": fee_bps,
                                    "edge_buffer_bps": edge_buffer_bps,
                                    "policy": pair_policy,
                                    "pair_exit_metrics": pair_exit_metrics,
                                    "target_pct": float(target),
                                    "pair_exit_target_pct": float(payload_spread_min),
                                    "qty": spread_qty,
                                    "hedged_qty": hedged_qty,
                                    "exit_percent": exit_percent,
                                    "trigger_mode": "spread",
                                },
                            )
                            append_diagnostic(
                                key,
                                rule,
                                status="trigger",
                                selected_pair=selected,
                                pair_metrics=pair_exit_metrics,
                                overall_spread=overall_spread,
                                trigger_spread=trigger_spread,
                                target_pct=float(target),
                                required_net_spread_pct=required_net_spread_pct,
                            )
                            payload = {
                                "symbol": symbol,
                                "qty": spread_qty,
                                "notional": None,
                                "mode": "smart-exit",
                                "max_slippage_bps": 8,
                                "spread_min_pct": float(payload_spread_min),
                                "spread_max_pct": 10,
                                "timeout_sec": 0,
                                "max_runtime_sec": max_runtime_sec,
                                "reprice_sec": 5,
                                "chunk_qty": None,
                                "chunk_notional": _safe_float(pair_policy.get("chunk_notional_cap_usd")),
                                "force_chunk_qty": False,
                                "limit_offset_bps": 0.0,
                                "limit_offset_ticks": 0,
                                "use_orderbook_check": False,
                                "fallback_to_market": False,
                                "hedge_order_type": "limit",
                                "hedge_offset_bps": 0.0,
                                "hedge_offset_ticks": 0,
                                "hedge_limit_mode": "passive",
                                "hedge_favorable_bps": 2.0,
                                "hedge_adverse_bps": 8.0,
                                "hedge_reprice_min_sec": 6.0,
                                "max_limit_deviation_bps": 30.0,
                                "async_run": True,
                                "dry_run": False,
                                "auto_exit_agent": True,
                                "auto_exit_rule_key": key,
                                "auto_exit_rule_generation": max(
                                    1,
                                    int(rule.get("rule_generation") or 1),
                                ),
                                "auto_exit_trigger_mode": "spread",
                                "auto_exit_exit_percent": float(exit_percent),
                                "auto_exit_hedged_qty": float(hedged_qty),
                                "auto_exit_requested_qty": float(spread_qty),
                                "auto_exit_dynamic_chunk": True,
                                "auto_exit_market_cleanup_notional_max": _safe_float(
                                    pair_policy.get("market_cleanup_notional_cap_usd")
                                ),
                                "long_exchange": selected_long_exchange,
                                "short_exchange": selected_short_exchange,
                                "margin_mode": "isolated",
                            }
                            result = await self.manual_exit(payload)
                            logger.info(
                                "auto-exit triggered symbol=%s long=%s short=%s spread=%.4f target=%.4f pair_target=%.4f result=%s",
                                symbol,
                                selected_long_exchange,
                                selected_short_exchange,
                                float(trigger_spread),
                                float(target),
                                float(payload_spread_min),
                                result,
                            )
                            self._auto_exit_event(
                                "start",
                                {
                                    "symbol": symbol,
                                    "long_exchange": selected_long_exchange,
                                    "short_exchange": selected_short_exchange,
                                    "rule_long_exchange": long_exchange,
                                    "rule_short_exchange": short_exchange,
                                    "selection_mode": selected_mode,
                                    "result": result,
                                    "trigger_mode": "spread",
                                },
                            )
                            async with self._auto_exit_lock:
                                stored_rules = self._auto_exit.get("rules", {})
                                stored = stored_rules.get(key)
                                if stored is not None:
                                    if _safe_float(stored.get("spread_target_qty")) is None:
                                        stored["spread_target_qty"] = float(qty)
                                    stored["spread_remaining_qty"] = float(spread_qty)
                                    stored["last_triggered_ts"] = now_ts
                                    stored["updated_at"] = datetime.now(timezone.utc).isoformat()
                                    self._auto_exit_store.save(self._auto_exit)
                            standard_triggered = True
                if (
                    standard_triggered
                    and not v1_rule_enabled
                    and v1_monitor_enabled
                    and v1_decision is not None
                ):
                    if pending_cycles > 0:
                        merge_rule_updates(key, {"v1_pending_exit_cycles": 0})
                    append_v1_diagnostic(
                        key,
                        rule,
                        status="shadow" if str(v1_decision.get("decision") or "") != "skip" else "skip",
                        reason=str(v1_decision.get("reason") or "shadow"),
                        selected_pair=selected,
                        pair_metrics=pair_exit_metrics,
                        decision=v1_decision,
                        window=window,
                        context=position_context,
                        close_now_bps=close_now_bps,
                        funding_to_next_bps=funding_to_next_bps,
                        reversion_credit_bps=reversion_credit_bps,
                        pending_exit_cycles=0,
                    )
                if standard_triggered:
                    break
                if v1_rule_enabled and not v1_on_cooldown and v1_decision is not None:
                    decision_name = str(v1_decision.get("decision") or "")
                    if decision_name == "skip":
                        if pending_cycles > 0:
                            merge_rule_updates(key, {"v1_pending_exit_cycles": 0})
                        append_v1_diagnostic(
                            key,
                            rule,
                            status="skip",
                            reason=str(v1_decision.get("reason") or "skip"),
                            selected_pair=selected,
                            pair_metrics=pair_exit_metrics,
                            decision=v1_decision,
                            window=window,
                            context=position_context,
                            close_now_bps=close_now_bps,
                            funding_to_next_bps=funding_to_next_bps,
                            reversion_credit_bps=reversion_credit_bps,
                            pending_exit_cycles=pending_cycles,
                        )
                        continue
                    if decision_name == "hold":
                        if pending_cycles > 0:
                            merge_rule_updates(key, {"v1_pending_exit_cycles": 0})
                        append_v1_diagnostic(
                            key,
                            rule,
                            status="hold",
                            reason=str(v1_decision.get("reason") or "hold"),
                            selected_pair=selected,
                            pair_metrics=pair_exit_metrics,
                            decision=v1_decision,
                            window=window,
                            context=position_context,
                            close_now_bps=close_now_bps,
                            funding_to_next_bps=funding_to_next_bps,
                            reversion_credit_bps=reversion_credit_bps,
                            pending_exit_cycles=0,
                        )
                        continue

                    trigger_reason = str(v1_decision.get("reason") or "v1_exit")
                    immediate_exit = decision_name == "exit"
                    if not immediate_exit:
                        pending_cycles += 1
                        merge_rule_updates(key, {"v1_pending_exit_cycles": pending_cycles})
                        if pending_cycles < int(AUTO_EXIT_V1_CONFIRM_CYCLES):
                            append_v1_diagnostic(
                                key,
                                rule,
                                status="wait",
                                reason="soft_exit_confirmation",
                                selected_pair=selected,
                                pair_metrics=pair_exit_metrics,
                                decision=v1_decision,
                                window=window,
                                context=position_context,
                                close_now_bps=close_now_bps,
                                funding_to_next_bps=funding_to_next_bps,
                                reversion_credit_bps=reversion_credit_bps,
                                pending_exit_cycles=pending_cycles,
                            )
                            continue
                    if self._auto_exit_has_running():
                        break
                    if selected_pair_spread is None:
                        append_v1_diagnostic(
                            key,
                            rule,
                            status="skip",
                            reason="pair_live_missing",
                            selected_pair=selected,
                            pair_metrics=pair_exit_metrics,
                            decision=v1_decision,
                            window=window,
                            context=position_context,
                            close_now_bps=close_now_bps,
                            funding_to_next_bps=funding_to_next_bps,
                            reversion_credit_bps=reversion_credit_bps,
                            pending_exit_cycles=pending_cycles,
                        )
                        continue
                    payload_spread_min = float(selected_pair_spread) - AUTO_EXIT_MULTILEG_PAIR_BUFFER_PCT
                    self._auto_exit_event(
                        "trigger",
                        {
                            "symbol": symbol,
                            "long_exchange": selected_long_exchange,
                            "short_exchange": selected_short_exchange,
                            "rule_long_exchange": long_exchange,
                            "rule_short_exchange": short_exchange,
                            "selection_mode": selected_mode,
                            "spread_scope": "v1_hold_exit",
                            "spread_pct": float(selected_pair_spread),
                            "net_spread_pct": selected_pair_net_spread,
                            "chunk_qty": pair_chunk_qty,
                            "chunk_notional_usd": pair_chunk_notional,
                            "qty": v1_qty,
                            "hedged_qty": hedged_qty,
                            "exit_percent": exit_percent,
                            "trigger_mode": "v1",
                            "v1_reason": trigger_reason,
                            "v1_wait_score_bps": _safe_float(v1_decision.get("wait_score_bps")),
                            "v1_funding_to_next_bps": funding_to_next_bps,
                            "v1_close_now_bps": close_now_bps,
                        },
                    )
                    append_v1_diagnostic(
                        key,
                        rule,
                        status="trigger",
                        reason=trigger_reason,
                        selected_pair=selected,
                        pair_metrics=pair_exit_metrics,
                        decision=v1_decision,
                        window=window,
                        context=position_context,
                        close_now_bps=close_now_bps,
                        funding_to_next_bps=funding_to_next_bps,
                        reversion_credit_bps=reversion_credit_bps,
                        pending_exit_cycles=0 if immediate_exit else pending_cycles,
                    )
                    payload = {
                        "symbol": symbol,
                        "qty": v1_qty,
                        "notional": None,
                        "mode": "smart-exit",
                        "max_slippage_bps": 8,
                        "spread_min_pct": float(payload_spread_min),
                        "spread_max_pct": 10,
                        "timeout_sec": 0,
                        "max_runtime_sec": max_runtime_sec,
                        "reprice_sec": 5,
                        "chunk_qty": None,
                        "chunk_notional": _safe_float(pair_policy.get("chunk_notional_cap_usd")),
                        "force_chunk_qty": False,
                        "limit_offset_bps": 0.0,
                        "limit_offset_ticks": 0,
                        "use_orderbook_check": False,
                        "fallback_to_market": False,
                        "hedge_order_type": "limit",
                        "hedge_offset_bps": 0.0,
                        "hedge_offset_ticks": 0,
                        "hedge_limit_mode": "passive",
                        "hedge_favorable_bps": 2.0,
                        "hedge_adverse_bps": 8.0,
                        "hedge_reprice_min_sec": 6.0,
                        "max_limit_deviation_bps": 30.0,
                        "async_run": True,
                        "dry_run": False,
                        "auto_exit_agent": True,
                        "auto_exit_rule_key": key,
                        "auto_exit_rule_generation": max(
                            1,
                            int(rule.get("rule_generation") or 1),
                        ),
                        "auto_exit_trigger_mode": "v1",
                        "auto_exit_exit_percent": float(exit_percent),
                        "auto_exit_hedged_qty": float(hedged_qty),
                        "auto_exit_requested_qty": float(v1_qty),
                        "auto_exit_dynamic_chunk": True,
                        "auto_exit_market_cleanup_notional_max": _safe_float(
                            pair_policy.get("market_cleanup_notional_cap_usd")
                        ),
                        "long_exchange": selected_long_exchange,
                        "short_exchange": selected_short_exchange,
                        "margin_mode": "isolated",
                    }
                    result = await self.manual_exit(payload)
                    self._auto_exit_event(
                        "start",
                        {
                            "symbol": symbol,
                            "long_exchange": selected_long_exchange,
                            "short_exchange": selected_short_exchange,
                            "rule_long_exchange": long_exchange,
                            "rule_short_exchange": short_exchange,
                            "selection_mode": selected_mode,
                            "result": result,
                            "trigger_mode": "v1",
                            "v1_reason": trigger_reason,
                        },
                    )
                    async with self._auto_exit_lock:
                        stored_rules = self._auto_exit.get("rules", {})
                        stored = stored_rules.get(key)
                        if stored is not None:
                            if _safe_float(stored.get("v1_target_qty")) is None:
                                stored["v1_target_qty"] = float(qty)
                            stored["v1_remaining_qty"] = float(v1_qty)
                            stored["last_v1_triggered_ts"] = now_ts
                            stored["updated_at"] = datetime.now(timezone.utc).isoformat()
                            stored["v1_pending_exit_cycles"] = 0
                            self._auto_exit_store.save(self._auto_exit)
                    break
                if not v1_rule_enabled and v1_monitor_enabled and v1_decision is not None:
                    if pending_cycles > 0:
                        merge_rule_updates(key, {"v1_pending_exit_cycles": 0})
                    append_v1_diagnostic(
                        key,
                        rule,
                        status="shadow" if str(v1_decision.get("decision") or "") != "skip" else "skip",
                        reason=str(v1_decision.get("reason") or "shadow"),
                        selected_pair=selected,
                        pair_metrics=pair_exit_metrics,
                        decision=v1_decision,
                        window=window,
                        context=position_context,
                        close_now_bps=close_now_bps,
                        funding_to_next_bps=funding_to_next_bps,
                        reversion_credit_bps=reversion_credit_bps,
                        pending_exit_cycles=0,
                    )
            async with self._auto_exit_lock:
                self._auto_exit_live_spreads = dict(live_spreads)
                self._auto_exit_diagnostics = diagnostics_rows
                self._auto_exit_v1_diagnostics = v1_diagnostics_rows
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
        try:
            self._auto_exit_history_store.append(entry)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("failed to append auto-exit history: %s", exc)

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
        auto_rebalance = bool(protective.get("auto_rebalance_enabled", False))
        if not auto_protect and not auto_take and not auto_rebalance:
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
        use_cache: bool = True,
        persist_candidate_decision: bool = True,
        run_position_logic: bool = True,
    ) -> dict[str, Any]:
        """Collect on-demand historical analytics for a symbol."""
        canonical = normalize_symbol(symbol)
        if not canonical:
            raise ValueError("Symbol must be provided for analysis.")
        session_state = await self.bootstrap_symbol_session(canonical)

        window = max(60, min(int(window_minutes), 4320))
        funding_limit = max(24, min(int(funding_points), 200))

        selected_exchanges = self._coin_analysis_selected_exchanges()
        if not selected_exchanges:
            raise ValueError("Enable at least one core analysis exchange (binance/kucoin).")
        global_warnings: list[str] = []
        if len(selected_exchanges) < 2:
            global_warnings.append("pair_analysis_limited: less than 2 core exchanges enabled")

        cache_key = (canonical, window, funding_limit, tuple(selected_exchanges))
        now_ts = time.time()
        cached = self._coin_analysis_cache.get(cache_key) if use_cache else None
        if cached:
            cached_at, cached_payload = cached
            if now_ts - cached_at <= COIN_ANALYSIS_CACHE_TTL_SEC:
                out = dict(cached_payload)
                out["cache_hit"] = True
                out["symbol_session"] = session_state
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
        visual_analysis = self._build_coin_visual_analysis(
            exchange_rows,
            pair_analysis,
            bot_logic,
        )
        decision_journal: dict[str, Any] | None = None
        if persist_candidate_decision:
            decision_journal = await asyncio.to_thread(
                self._persist_coin_candidate_decision,
                canonical,
                bot_logic,
                pair_analysis,
            )
        position_logic: dict[str, Any] = {}
        if run_position_logic:
            position_logic = await asyncio.to_thread(
                self._evaluate_symbol_positions,
                canonical,
                pair_analysis,
            )

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
            "visual_analysis": visual_analysis,
            "decision_journal": decision_journal or {},
            "position_logic": position_logic,
            "symbol_session": session_state,
            "cache_hit": False,
        }
        if use_cache:
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
        funding_rows_upserted = await asyncio.to_thread(
            self._persist_coin_funding_history,
            canonical_symbol,
            exchange,
            funding_history,
            funding_interval_hours,
        )
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
        oi_rows_upserted = await asyncio.to_thread(
            self._persist_coin_open_interest_history,
            canonical_symbol,
            exchange,
            oi_payload,
        )
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
            "funding_rows_upserted": funding_rows_upserted,
            "oi_rows_upserted": oi_rows_upserted,
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

    def _persist_coin_funding_history(
        self,
        canonical_symbol: str,
        exchange: str,
        history: list[dict[str, Any]],
        fallback_interval_hours: float | None,
    ) -> int:
        rows: list[CoinFundingHistoryRow] = []
        for item in history or []:
            ts_ms = _funding_history_ts_ms(
                item.get("ts_ms")
                or item.get("timestamp")
                or item.get("timepoint")
                or item.get("timePoint")
                or item.get("fundingTime")
            )
            if not ts_ms:
                continue
            interval_hours = _safe_float(
                item.get("interval_hours")
                or item.get("intervalHours")
                or item.get("funding_interval_hours")
            )
            rows.append(
                CoinFundingHistoryRow(
                    canonical_symbol=canonical_symbol,
                    exchange=exchange,
                    ts_ms=int(ts_ms),
                    funding_rate=_safe_float(
                        item.get("rate")
                        or item.get("fundingRate")
                        or item.get("funding_rate")
                    ),
                    predicted_funding_rate=_safe_float(
                        item.get("predicted_rate")
                        or item.get("predictedFundingRate")
                        or item.get("predicted_funding_rate")
                    ),
                    interval_hours=interval_hours if interval_hours is not None else fallback_interval_hours,
                    mark_price=_safe_float(item.get("mark_price") or item.get("markPrice")),
                    source_type=str(
                        item.get("source_type")
                        or item.get("source")
                        or "adapter_funding_history"
                    ),
                )
            )
        return upsert_funding_history_rows(rows)

    def _persist_coin_open_interest_history(
        self,
        canonical_symbol: str,
        exchange: str,
        oi_payload: Mapping[str, Any],
    ) -> int:
        source = str(oi_payload.get("source") or "open_interest_snapshot")
        rows: list[CoinOpenInterestHistoryRow] = []
        seen_ts: set[int] = set()
        for item in list(oi_payload.get("history") or []):
            ts_ms = _funding_history_ts_ms(item.get("ts_ms") or item.get("timestamp"))
            if not ts_ms:
                continue
            ts_int = int(ts_ms)
            seen_ts.add(ts_int)
            rows.append(
                CoinOpenInterestHistoryRow(
                    canonical_symbol=canonical_symbol,
                    exchange=exchange,
                    ts_ms=ts_int,
                    oi_contracts=_safe_float(
                        item.get("open_interest_contracts")
                        or item.get("oi_contracts")
                        or item.get("openInterestAmount")
                        or item.get("openInterest")
                    ),
                    oi_notional=_safe_float(
                        item.get("open_interest_notional")
                        or item.get("oi_notional")
                        or item.get("openInterestValue")
                        or item.get("openInterestUsd")
                    ),
                    interval_label=str(item.get("interval_label") or "1h"),
                    source_type=source,
                )
            )

        current = oi_payload.get("current") or {}
        current_ts = _funding_history_ts_ms(current.get("ts_ms") or current.get("timestamp"))
        if current_ts:
            current_ts_int = int(current_ts)
            if current_ts_int not in seen_ts:
                rows.append(
                    CoinOpenInterestHistoryRow(
                        canonical_symbol=canonical_symbol,
                        exchange=exchange,
                        ts_ms=current_ts_int,
                        oi_contracts=_safe_float(
                            current.get("open_interest_contracts")
                            or current.get("oi_contracts")
                            or current.get("openInterestAmount")
                            or current.get("openInterest")
                        ),
                        oi_notional=_safe_float(
                            current.get("open_interest_notional")
                            or current.get("oi_notional")
                            or current.get("openInterestValue")
                            or current.get("openInterestUsd")
                        ),
                        interval_label="current",
                        source_type=source,
                    )
                )

        return upsert_open_interest_history_rows(rows)

    def _analyze_pair(
        self,
        left: Mapping[str, Any],
        right: Mapping[str, Any],
        window_minutes: int,
    ) -> dict[str, Any]:
        left_ex = str(left.get("exchange") or "")
        right_ex = str(right.get("exchange") or "")
        canonical_symbol = str(left.get("symbol") or right.get("symbol") or "")
        pair_key = build_pair_key(canonical_symbol, left_ex, right_ex)
        series = _spread_series_from_candles(
            list(left.get("candles_1m") or []),
            list(right.get("candles_1m") or []),
        )
        now_ts_ms = int(time.time() * 1000)
        coverage_pct = (len(series) / max(1, window_minutes)) * 100.0
        spread_values = [float(row.get("spread_pct") or 0.0) for row in series]
        p25 = _percentile(spread_values, 25)
        p50 = _percentile(spread_values, 50)
        p75 = _percentile(spread_values, 75)
        p95 = _percentile([abs(v) for v in spread_values], 95)
        weighted_mean = _weighted_mean_recent(
            series,
            value_key="spread_pct",
            now_ts_ms=now_ts_ms,
            half_life_hours=24.0,
        )

        feature_pack = build_pair_feature_snapshots(
            pair_key=pair_key,
            canonical_symbol=canonical_symbol,
            left_exchange=left_ex,
            right_exchange=right_ex,
            left=left,
            right=right,
            spread_series=series,
            coverage_pct=coverage_pct,
            now_ts_ms=now_ts_ms,
        )
        direction_rows = list(feature_pack.get("directions") or [])
        if not direction_rows:
            direction_rows = [
                {
                    "direction": "long_a_short_b",
                    "action": "NO_TRADE",
                    "reasons": ["data_quality_low"],
                    "scores": {"entry_score": 0.0},
                    "directional": {},
                }
            ]
        selected = max(
            direction_rows,
            key=lambda row: _safe_float((row.get("scores") or {}).get("entry_score")) or 0.0,
        )

        feature_snapshot_ids: dict[str, int] = {}
        for item in direction_rows:
            direction = str(item.get("direction") or "")
            if not direction:
                continue
            payload_features = {
                "common": feature_pack.get("common") or {},
                "directional": item.get("directional") or {},
                "scores": item.get("scores") or {},
                "reasons": list(item.get("reasons") or []),
            }
            try:
                feature_id = insert_feature_snapshot(
                    CoinFeatureSnapshotRow(
                        ts_ms=now_ts_ms,
                        pair_key=pair_key,
                        canonical_symbol=canonical_symbol,
                        context_mode="candidate",
                        feature_set_version=COIN_ANALYSIS_FEATURE_SET_VERSION,
                        direction=direction,
                        features=payload_features,
                        data_quality=feature_pack.get("data_quality") or {},
                    )
                )
                feature_snapshot_ids[direction] = feature_id
            except Exception:  # pylint: disable=broad-except
                continue

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

        selected_scores = selected.get("scores") or {}
        score = _safe_float(selected_scores.get("entry_score")) or 0.0
        reasons = list(selected.get("reasons") or [])
        selected_action = str(selected.get("action") or "NO_TRADE")
        selected_direction = str(selected.get("direction") or "long_a_short_b")
        recommendation = "reject"
        if interval_match and selected_action == "ENTRY_STRONG" and score >= 70.0:
            recommendation = "enter_candidate"
        elif interval_match and selected_action in ("ENTRY_SMALL", "ENTRY_STRONG") and score >= 50.0:
            recommendation = "watch"

        spread_features = (
            ((feature_pack.get("common") or {}).get("spread_features") or {})
            if isinstance(feature_pack.get("common"), Mapping)
            else {}
        )
        current_spread = _safe_float(spread_features.get("current_spread_pct"))
        z_score = _safe_float(spread_features.get("spread_zscore_1h"))

        return {
            "pair_key": pair_key,
            "canonical_symbol": canonical_symbol,
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
            "derived_spread": (feature_pack.get("common") or {}).get("derived_spread"),
            "decision_phase": (feature_pack.get("common") or {}).get("decision_phase"),
            "hours_to_next_funding_min": _safe_float(
                (feature_pack.get("common") or {}).get("hours_to_next_funding_min")
            ),
            "directional_features": direction_rows,
            "selected_direction": selected_direction,
            "selected_action": selected_action,
            "feature_snapshot_ids": feature_snapshot_ids,
            "open_interest": {
                "left_change_6h_pct": oi_left_6h,
                "right_change_6h_pct": oi_right_6h,
                "divergence_6h_pct": oi_divergence_6h,
            },
            "score": round(score, 2),
            "recommendation": recommendation,
            "reasons": reasons,
        }

    def _build_coin_visual_analysis(
        self,
        exchange_rows: list[Mapping[str, Any]],
        pair_analysis: list[Mapping[str, Any]],
        bot_logic: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not exchange_rows or not pair_analysis:
            return {}

        recommended_pair = dict(bot_logic.get("recommended_pair") or {})
        target_pair_key = str(recommended_pair.get("pair_key") or "")
        selected_pair = None
        if target_pair_key:
            for item in pair_analysis:
                if str(item.get("pair_key") or "") == target_pair_key:
                    selected_pair = dict(item)
                    break
        if selected_pair is None:
            selected_pair = dict(
                max(
                    pair_analysis,
                    key=lambda row: _safe_float(row.get("score")) or 0.0,
                )
            )

        left_exchange = str(selected_pair.get("left_exchange") or "")
        right_exchange = str(selected_pair.get("right_exchange") or "")
        exchange_map = {
            str(item.get("exchange") or ""): item
            for item in exchange_rows
            if item
        }
        left_row = exchange_map.get(left_exchange)
        right_row = exchange_map.get(right_exchange)
        if not left_row or not right_row:
            return {}

        direction = str(
            selected_pair.get("selected_direction")
            or recommended_pair.get("direction")
            or "long_a_short_b"
        )
        spread_series = _spread_series_from_candles(
            list(left_row.get("candles_1m") or []),
            list(right_row.get("candles_1m") or []),
        )
        spread_chart_rows = list(reversed(spread_series))
        funding_series = _funding_net_hourly_series(
            list(left_row.get("funding_history") or []),
            list(right_row.get("funding_history") or []),
            left_interval_hours=_safe_float(left_row.get("funding_interval_hours_resolved")),
            right_interval_hours=_safe_float(right_row.get("funding_interval_hours_resolved")),
            direction=direction,
        )
        window_rows = _build_visual_window_rows(spread_series, funding_series)
        spread_values = [
            float(_safe_float(item.get("spread_pct")) or 0.0)
            for item in spread_series
            if _safe_float(item.get("spread_pct")) is not None
        ]
        net_values = [
            float(_safe_float(item.get("net_bps")) or 0.0)
            for item in funding_series
            if _safe_float(item.get("net_bps")) is not None
        ]
        direction_label = _direction_label(direction, left_exchange, right_exchange)
        notes = [
            "Funding is normalized to hourly carry and compared in bps for mixed intervals.",
            "Spread chart uses synchronized 1m candle closes, so it is indicative rather than executable.",
        ]
        if not funding_series:
            notes.append("Funding chart is empty because one side has no usable history window yet.")
        if not spread_series:
            notes.append("Spread chart is empty because overlapping candle timestamps were not found.")

        return {
            "pair_key": str(selected_pair.get("pair_key") or ""),
            "pair_label": f"{left_exchange} vs {right_exchange}",
            "direction": direction,
            "direction_label": direction_label,
            "selected_action": str(selected_pair.get("selected_action") or bot_logic.get("recommended_action") or "NO_TRADE"),
            "recommendation": str(selected_pair.get("recommendation") or "reject"),
            "score": _safe_float(selected_pair.get("score")),
            "summary": {
                "spread_current_pct": (
                    _safe_float(spread_series[0].get("spread_pct"))
                    if spread_series
                    else None
                ),
                "spread_mean_pct": (
                    sum(spread_values) / len(spread_values)
                    if spread_values
                    else None
                ),
                "funding_net_hourly_bps": (
                    sum(net_values[-4:]) / len(net_values[-4:])
                    if net_values
                    else None
                ),
                "funding_positive_share_pct": (
                    len([value for value in net_values if value > 0]) / len(net_values) * 100.0
                    if net_values
                    else None
                ),
            },
            "windows": window_rows,
            "charts": {
                "spread": {
                    "points": _downsample_chart_points(spread_chart_rows, value_key="spread_pct", max_points=120),
                    "value_key": "spread_pct",
                },
                "funding": {
                    "points": _downsample_chart_points(funding_series, value_key="net_bps", max_points=120),
                    "value_key": "net_bps",
                },
            },
            "notes": notes,
        }

    def _decide_coin_candidate(self, pairs: list[Mapping[str, Any]]) -> dict[str, Any]:
        return evaluate_candidate_pairs(pairs)

    def _persist_coin_candidate_decision(
        self,
        canonical_symbol: str,
        bot_logic: Mapping[str, Any],
        pair_analysis: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        recommended_pair = dict(bot_logic.get("recommended_pair") or {})
        pair_key = str(
            recommended_pair.get("pair_key")
            or (pair_analysis[0].get("pair_key") if pair_analysis else build_pair_key(canonical_symbol, "binance", "kucoin"))
            or build_pair_key(canonical_symbol, "binance", "kucoin")
        )
        direction = str(recommended_pair.get("direction") or "long_a_short_b")
        action = str(bot_logic.get("recommended_action") or "NO_TRADE")
        decision_phase = str(
            bot_logic.get("decision_phase")
            or recommended_pair.get("decision_phase")
            or "exploratory"
        )
        score = _safe_float(bot_logic.get("score")) or 0.0
        reason_codes = normalize_reason_codes(
            list(bot_logic.get("reason_codes") or bot_logic.get("pair_reasons") or [])
        )
        if not reason_codes:
            reason_codes = ["data_quality_low"]
        reason_text = [
            str(item)
            for item in list(bot_logic.get("reason_text") or [])
            if str(item).strip()
        ]
        if not reason_text:
            reason_text = [str(bot_logic.get("reason") or "candidate_rule_engine")]
        scores = dict(bot_logic.get("scores") or {})
        scores.setdefault("best_pair_score", score)

        decision_id = f"ca-{canonical_symbol.lower()}-{now_ms}-{uuid4().hex[:8]}"
        insert_decision(
            CoinDecisionRow(
                decision_id=decision_id,
                ts_ms=now_ms,
                mode="manual_candidate",
                canonical_symbol=canonical_symbol,
                pair_key=pair_key,
                direction=direction,
                action=action,
                decision_phase=decision_phase,
                confidence_score=float(score),
                reason_codes=reason_codes,
                reason_text=reason_text,
                scores=scores,
                features_ref=(
                    str(recommended_pair.get("feature_snapshot_id"))
                    if recommended_pair.get("feature_snapshot_id") is not None
                    else None
                ),
            )
        )
        return {
            "decision_id": decision_id,
            "pair_key": pair_key,
            "direction": direction,
            "action": action,
            "decision_phase": decision_phase,
            "score": round(score, 2),
            "reason_codes": reason_codes,
        }

    def _evaluate_symbol_positions(
        self,
        canonical_symbol: str,
        pair_analysis: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        def _pair_row_for_exchanges(
            long_exchange: str,
            short_exchange: str,
        ) -> tuple[Mapping[str, Any] | None, str]:
            for row in pair_analysis:
                left_ex = normalize_exchange_name(str(row.get("left_exchange") or ""))
                right_ex = normalize_exchange_name(str(row.get("right_exchange") or ""))
                if long_exchange == left_ex and short_exchange == right_ex:
                    return row, "long_a_short_b"
                if long_exchange == right_ex and short_exchange == left_ex:
                    return row, "long_b_short_a"
            return (pair_analysis[0], "long_a_short_b") if pair_analysis else (None, "long_a_short_b")

        def _direction_feature_for_pair(
            pair_row: Mapping[str, Any] | None,
            direction: str,
        ) -> Mapping[str, Any] | None:
            if pair_row is None:
                return None
            for item in list((pair_row or {}).get("directional_features") or []):
                if str(item.get("direction") or "") == direction:
                    return item
            return None

        open_paper = get_paper_positions(status="open")
        paper_positions = [
            row
            for row in open_paper
            if normalize_symbol(str(row.get("canonical_symbol") or "")) == canonical_symbol
        ]

        by_pair_key = {
            str(row.get("pair_key") or ""): row
            for row in pair_analysis
            if row.get("pair_key")
        }
        paper_rows_out: list[dict[str, Any]] = []
        real_rows_out: list[dict[str, Any]] = []
        saved_paper = 0
        saved_real = 0
        now_ms = int(time.time() * 1000)
        historical_reviews = get_decisions(
            canonical_symbol=canonical_symbol,
            mode="manual_position_review",
            limit=5000,
        )
        historical_outcomes = get_outcomes(
            canonical_symbol=canonical_symbol,
            limit=5000,
        )
        latest_outcome_by_decision_id: dict[str, dict[str, Any]] = {}
        for row in historical_outcomes:
            decision_id = str(row.get("decision_id") or "")
            if not decision_id or decision_id in latest_outcome_by_decision_id:
                continue
            latest_outcome_by_decision_id[decision_id] = row
        latest_review_by_state_ref: dict[str, dict[str, Any]] = {}
        for row in historical_reviews:
            state_ref = str(row.get("state_ref") or "").strip()
            decision_id = str(row.get("decision_id") or "")
            if not state_ref or not decision_id or state_ref in latest_review_by_state_ref:
                continue
            outcome_row = latest_outcome_by_decision_id.get(decision_id)
            if not outcome_row:
                continue
            outcome_payload = dict(outcome_row.get("outcome") or {})
            latest_review_by_state_ref[state_ref] = {
                "latest_review_decision_id": decision_id,
                "latest_review_decision_ts_ms": int(_safe_float(row.get("ts_ms")) or 0),
                "latest_review_horizon": str(outcome_row.get("horizon") or ""),
                "latest_review_evaluated_at_ms": int(
                    _safe_float(outcome_row.get("evaluated_at_ms")) or 0
                ),
                "latest_correctness": str(
                    outcome_payload.get("decision_correctness") or ""
                ).strip().lower()
                or None,
                "latest_timing_quality": str(
                    outcome_payload.get("timing_quality") or ""
                ).strip().lower()
                or None,
            }

        def _apply_latest_review_payload(
            row: dict[str, Any],
            *,
            state_ref: str | None,
        ) -> None:
            key = str(state_ref or "").strip()
            if not key:
                return
            review = latest_review_by_state_ref.get(key) or {}
            if not review:
                return
            row.update(review)

        for pos in paper_positions:
            position_key = str(pos.get("position_key") or "")
            pair_key = str(pos.get("pair_key") or "")
            pair_row = by_pair_key.get(pair_key)
            if pair_row is None and pair_analysis:
                pair_row = pair_analysis[0]
                pair_key = str(pair_row.get("pair_key") or pair_key)
            direction = str(pos.get("direction") or "")
            if not direction and pair_row is not None:
                direction = str(pair_row.get("selected_direction") or "long_a_short_b")
            if not direction:
                direction = "long_a_short_b"
            decision_phase = str(
                (pair_row or {}).get("decision_phase") or "exploratory"
            )
            spread_coverage_pct = _safe_float(
                ((pair_row or {}).get("spread") or {}).get("coverage_pct")
            ) or 0.0
            direction_feature = None
            for item in list((pair_row or {}).get("directional_features") or []):
                if str(item.get("direction") or "") == direction:
                    direction_feature = item
                    break
            signal = evaluate_position_signal(
                position_key=position_key,
                pair_key=pair_key,
                direction=direction,
                qty=_safe_float(pos.get("qty")) or 0.0,
                decision_phase=decision_phase,
                spread_coverage_pct=spread_coverage_pct,
                direction_feature=direction_feature,
            )
            feature_snapshot_id = ((pair_row or {}).get("feature_snapshot_ids") or {}).get(direction)
            decision_id = f"ca-pos-{canonical_symbol.lower()}-{now_ms}-{uuid4().hex[:8]}"
            insert_decision(
                CoinDecisionRow(
                    decision_id=decision_id,
                    ts_ms=now_ms,
                    mode="manual_position_review",
                    canonical_symbol=canonical_symbol,
                    pair_key=pair_key or build_pair_key(canonical_symbol, "binance", "kucoin"),
                    direction=direction,
                    action=str(signal.get("action") or "HOLD"),
                    decision_phase=str(signal.get("decision_phase") or "exploratory"),
                    confidence_score=_safe_float(signal.get("confidence_score")) or 0.0,
                    reason_codes=normalize_reason_codes(list(signal.get("reason_codes") or [])),
                    reason_text=[str(x) for x in list(signal.get("reason_text") or [])],
                    scores=dict(signal.get("scores") or {}),
                    features_ref=(
                        str(feature_snapshot_id) if feature_snapshot_id is not None else None
                    ),
                    state_ref=position_key or None,
                )
            )
            saved_paper += 1
            paper_rows_out.append(
                {
                    "decision_id": decision_id,
                    "decision_ts_ms": now_ms,
                    "position_key": position_key,
                    "pair_key": pair_key,
                    "direction": direction,
                    "action": signal.get("action"),
                    "position_source": "paper",
                    "decision_phase": signal.get("decision_phase"),
                    "minutes_to_next_funding": _safe_float(
                        (pair_row or {}).get("hours_to_next_funding_min")
                    ),
                    "confidence_score": signal.get("confidence_score"),
                    "reason_codes": list(signal.get("reason_codes") or []),
                    "reason_text": list(signal.get("reason_text") or []),
                    "scores": dict(signal.get("scores") or {}),
                    "features_ref": str(feature_snapshot_id) if feature_snapshot_id is not None else None,
                }
            )
            _apply_latest_review_payload(paper_rows_out[-1], state_ref=position_key)

        snapshot = self._accounts.snapshot() or {}
        raw_positions = list(snapshot.get("positions") or [])
        symbol_positions_raw: list[dict[str, Any]] = []
        for pos in raw_positions:
            exchange = normalize_exchange_name(str(pos.get("exchange") or ""))
            if not exchange:
                continue
            symbol_norm = _normalize_manual_symbol(
                str(
                    pos.get("symbol_normalized")
                    or pos.get("symbol")
                    or pos.get("exchange_symbol")
                    or ""
                )
            )
            if symbol_norm != canonical_symbol:
                continue
            qty_raw = self._rebalance_position_qty(pos)
            side = self._rebalance_position_side(pos, qty_raw)
            if qty_raw is None or not side:
                continue
            qty_abs = abs(float(qty_raw))
            if qty_abs <= 0:
                continue
            symbol_positions_raw.append(
                {
                    "exchange": exchange,
                    "side": side,
                    "qty": qty_abs,
                    "raw": dict(pos),
                }
            )

        aggregated: dict[tuple[str, str], float] = {}
        for item in symbol_positions_raw:
            key = (str(item.get("exchange") or ""), str(item.get("side") or ""))
            aggregated[key] = aggregated.get(key, 0.0) + float(item.get("qty") or 0.0)
        symbol_positions: list[dict[str, Any]] = []
        for (exchange, side), qty in aggregated.items():
            if qty <= 0:
                continue
            symbol_positions.append(
                {
                    "exchange": exchange,
                    "side": side,
                    "qty": qty,
                }
            )

        long_legs = [dict(item) for item in symbol_positions if str(item.get("side") or "") == "long"]
        short_legs = [dict(item) for item in symbol_positions if str(item.get("side") or "") == "short"]
        observed_real_keys: set[str] = set()
        if long_legs and short_legs:
            while long_legs and short_legs:
                best: tuple[int, int, int, int] | None = None
                best_pair_row: Mapping[str, Any] | None = None
                best_direction = "long_a_short_b"
                for li, long_leg in enumerate(long_legs):
                    for si, short_leg in enumerate(short_legs):
                        pair_row, direction = _pair_row_for_exchanges(
                            str(long_leg.get("exchange") or ""),
                            str(short_leg.get("exchange") or ""),
                        )
                        matched_qty = min(
                            float(long_leg.get("qty") or 0.0),
                            float(short_leg.get("qty") or 0.0),
                        )
                        if matched_qty <= 0:
                            continue
                        pair_priority = 1 if pair_row is not None else 0
                        candidate = (pair_priority, int(matched_qty * 1_000_000), li, si)
                        if best is None or candidate > best:
                            best = candidate
                            best_pair_row = pair_row
                            best_direction = direction
                if best is None:
                    break
                _priority, _qty_key, li, si = best
                long_leg = long_legs[li]
                short_leg = short_legs[si]
                matched_qty = min(
                    float(long_leg.get("qty") or 0.0),
                    float(short_leg.get("qty") or 0.0),
                )
                if matched_qty <= 0:
                    break
                long_exchange = str(long_leg.get("exchange") or "")
                short_exchange = str(short_leg.get("exchange") or "")
                pair_row = best_pair_row
                pair_key = str((pair_row or {}).get("pair_key") or "")
                if not pair_key:
                    pair_key = build_pair_key(canonical_symbol, "binance", "kucoin")
                decision_phase = str((pair_row or {}).get("decision_phase") or "exploratory")
                spread_coverage_pct = _safe_float(
                    ((pair_row or {}).get("spread") or {}).get("coverage_pct")
                ) or 0.0
                direction_feature = _direction_feature_for_pair(pair_row, best_direction)
                feature_snapshot_id = ((pair_row or {}).get("feature_snapshot_ids") or {}).get(best_direction)
                position_key = (
                    f"real-{canonical_symbol.lower()}-"
                    f"{long_exchange}-long-{short_exchange}-short"
                )
                observed_real_keys.add(position_key)
                signal = evaluate_position_signal(
                    position_key=position_key,
                    pair_key=pair_key,
                    direction=best_direction,
                    qty=matched_qty,
                    decision_phase=decision_phase,
                    spread_coverage_pct=spread_coverage_pct,
                    direction_feature=direction_feature,
                )
                decision_id = f"ca-real-{canonical_symbol.lower()}-{now_ms}-{uuid4().hex[:8]}"
                insert_decision(
                    CoinDecisionRow(
                        decision_id=decision_id,
                        ts_ms=now_ms,
                        mode="manual_position_review",
                        canonical_symbol=canonical_symbol,
                        pair_key=pair_key,
                        direction=best_direction,
                        action=str(signal.get("action") or "HOLD"),
                        decision_phase=str(signal.get("decision_phase") or "exploratory"),
                        confidence_score=_safe_float(signal.get("confidence_score")) or 0.0,
                        reason_codes=normalize_reason_codes(list(signal.get("reason_codes") or [])),
                        reason_text=[str(x) for x in list(signal.get("reason_text") or [])],
                        scores=dict(signal.get("scores") or {}),
                        features_ref=(
                            str(feature_snapshot_id) if feature_snapshot_id is not None else None
                        ),
                        state_ref=position_key,
                    )
                )
                saved_real += 1
                real_rows_out.append(
                    {
                        "decision_id": decision_id,
                        "decision_ts_ms": now_ms,
                        "position_key": position_key,
                        "pair_key": pair_key,
                        "direction": best_direction,
                        "action": signal.get("action"),
                        "position_source": "real_manual",
                        "long_exchange": long_exchange,
                        "short_exchange": short_exchange,
                        "matched_qty": matched_qty,
                        "decision_phase": signal.get("decision_phase"),
                        "minutes_to_next_funding": _safe_float(
                            (pair_row or {}).get("hours_to_next_funding_min")
                        ),
                        "confidence_score": signal.get("confidence_score"),
                        "reason_codes": list(signal.get("reason_codes") or []),
                        "reason_text": list(signal.get("reason_text") or []),
                        "scores": dict(signal.get("scores") or {}),
                        "features_ref": str(feature_snapshot_id) if feature_snapshot_id is not None else None,
                    }
                )
                _apply_latest_review_payload(real_rows_out[-1], state_ref=position_key)
                insert_real_position_observation(
                    CoinRealPositionObservationRow(
                        state_ref=position_key,
                        ts_ms=now_ms,
                        canonical_symbol=canonical_symbol,
                        pair_key=pair_key,
                        direction=best_direction,
                        long_exchange=long_exchange,
                        short_exchange=short_exchange,
                        qty=matched_qty,
                        status="open",
                        payload={
                            "source": "analyze_symbol",
                            "action": signal.get("action"),
                            "decision_id": decision_id,
                        },
                    )
                )
                insert_trade_activity(
                    CoinTradeActivityRow(
                        event_id=f"coin-activity-{position_key}-{now_ms}-open",
                        ts_ms=now_ms,
                        canonical_symbol=canonical_symbol,
                        pair_key=pair_key,
                        direction=best_direction,
                        activity_type="real_open_detected",
                        source="analyze_symbol",
                        state_ref=position_key,
                        payload={
                            "qty": matched_qty,
                            "decision_id": decision_id,
                            "action": signal.get("action"),
                            "long_exchange": long_exchange,
                            "short_exchange": short_exchange,
                        },
                    )
                )

                long_left = max(0.0, float(long_leg.get("qty") or 0.0) - matched_qty)
                short_left = max(0.0, float(short_leg.get("qty") or 0.0) - matched_qty)
                if long_left <= 1e-9:
                    long_legs.pop(li)
                else:
                    long_legs[li]["qty"] = long_left
                if short_left <= 1e-9:
                    short_legs.pop(si)
                else:
                    short_legs[si]["qty"] = short_left

        latest_real_status: dict[str, str] = {}
        for row in get_real_position_observations(canonical_symbol=canonical_symbol, limit=5000):
            key = str(row.get("state_ref") or "")
            if not key or key in latest_real_status:
                continue
            latest_real_status[key] = str(row.get("status") or "")
        for state_ref, status in latest_real_status.items():
            if status != "open":
                continue
            if state_ref in observed_real_keys:
                continue
            insert_real_position_observation(
                CoinRealPositionObservationRow(
                    state_ref=state_ref,
                    ts_ms=now_ms,
                    canonical_symbol=canonical_symbol,
                    qty=0.0,
                    status="closed",
                    payload={
                        "source": "analyze_symbol",
                        "reason": "missing_in_current_snapshot",
                    },
                )
            )
            insert_trade_activity(
                CoinTradeActivityRow(
                    event_id=f"coin-activity-{state_ref}-{now_ms}-closed",
                    ts_ms=now_ms,
                    canonical_symbol=canonical_symbol,
                    activity_type="real_closed_detected",
                    source="analyze_symbol",
                    state_ref=state_ref,
                    payload={
                        "reason": "missing_in_current_snapshot",
                    },
                )
            )

        unmatched_legs = len(long_legs) + len(short_legs)

        return {
            "paper": paper_rows_out,
            "real_manual": real_rows_out,
            "summary": {
                "paper_positions": len(paper_positions),
                "paper_decisions_saved": saved_paper,
                "real_positions": len(real_rows_out),
                "real_decisions_saved": saved_real,
                "real_legs_detected": len(symbol_positions),
                "real_unpaired_legs": unmatched_legs,
                "real_observations_open": len(observed_real_keys),
            },
        }

    def _fetch_kucoin_open_interest_snapshot(self, canonical_symbol: str) -> dict[str, Any] | None:
        try:
            adapter = get_adapter_cached("kucoin")
        except KeyError:
            return None
        try:
            snapshots = adapter.fetch_market_snapshots([canonical_symbol])
        except Exception:  # pylint: disable=broad-except
            return None
        if not snapshots:
            return None
        snap = snapshots[0]
        raw = snap.raw or {}
        contract = raw.get("contract") or {}
        ticker = raw.get("ticker") or {}
        oi_contracts = _safe_float(
            contract.get("openInterest")
            or contract.get("openInterestSize")
            or contract.get("openInterestAmount")
            or ticker.get("openInterest")
        )
        oi_notional = _safe_float(
            contract.get("openInterestValue")
            or contract.get("openInterestUsd")
            or ticker.get("turnoverOf24h")
            or ticker.get("turnover24h")
        )
        if oi_contracts is None and oi_notional is None:
            return None
        ts_ms = _funding_history_ts_ms(
            contract.get("ts")
            or contract.get("timestamp")
            or ticker.get("ts")
            or ticker.get("time")
        )
        if not ts_ms:
            ts_ms = int(time.time() * 1000)
        point = {
            "ts_ms": int(ts_ms),
            "open_interest_contracts": oi_contracts,
            "open_interest_notional": oi_notional,
        }
        return {
            "status": "partial",
            "history": [point],
            "current": point,
            "symbol": snap.exchange_symbol,
            "source": "kucoin_contract_snapshot",
        }

    def _fetch_open_interest_for_exchange(
        self,
        exchange: str,
        canonical_symbol: str,
        window_minutes: int,
    ) -> dict[str, Any]:
        name = normalize_exchange_name(exchange)
        if name not in ("binance", "okx", "kucoin"):
            return {
                "status": "unsupported",
                "history": [],
                "current": None,
                "error": "oi_history_not_supported_in_phase",
            }

        client = _ccxt_client(name)
        if client is None:
            if name == "kucoin":
                fallback = self._fetch_kucoin_open_interest_snapshot(canonical_symbol)
                if fallback:
                    return fallback
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
                    "interval_label": "1h",
                }
            )
        history.sort(key=lambda item: item.get("ts_ms") or 0, reverse=True)

        current = None
        for cand in [symbol_used, *candidates]:
            if not cand:
                continue
            try:
                now_row = client.fetch_open_interest(cand)
            except Exception:
                continue
            current = {
                "ts_ms": _funding_history_ts_ms(now_row.get("timestamp")) or int(time.time() * 1000),
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
            symbol_used = cand
            break

        if not history and not current and name == "kucoin":
            fallback = self._fetch_kucoin_open_interest_snapshot(canonical_symbol)
            if fallback:
                return fallback

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
            sent = await self._accounts.send_notification_message(text, title="FeeArb MEXC protective alert")
            if sent:
                self._last_mexc_alert[key] = now

def _fmt_ts(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
