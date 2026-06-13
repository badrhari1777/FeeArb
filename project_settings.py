"""Runtime configuration for the funding arbitrage monitor.

This module centralises user-adjustable settings that are persisted on disk.
It exposes a small manager responsible for validating, loading and saving the
settings.  Only non-sensitive values belong here - credentials should stay in
environment variables or `.env`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Dict, Final, Iterable, Mapping

from config import BASE_DIR, SUPPORTED_EXCHANGES

_DEFAULT_SETTINGS_PATH: Final[Path] = BASE_DIR / "data" / "settings.json"

DEFAULT_SOURCES: Final[Dict[str, bool]] = {
    "arbitragescanner": True,
    "coinglass": True,
}

DEFAULT_EXCHANGES: Final[Dict[str, bool]] = {
    "binance": True,
    "okx": True,
    "bybit": False,
    "gate": False,
    "bitget": False,
    "bingx": False,
    "mexc": False,
    "kucoin": False,
}

# Broader set for deep-dive analysis (can include venues not in SUPPORTED_EXCHANGES).
DEFAULT_ANALYSIS_EXCHANGES: Final[Dict[str, bool]] = {
    "binance": True,
    "bybit": True,
    "bingx": True,
    "bitget": True,
    "okx": True,
    "gate": True,
    "mexc": True,
    "kucoin": True,
}

MIN_REFRESH_SECONDS: Final[int] = 30
MAX_REFRESH_SECONDS: Final[int] = 24 * 60 * 60  # one day


def _default_manual_auto_exit_policy() -> Dict[str, Dict[str, float]]:
    return {
        "tier1": {
            "chunk_notional_cap_usd": 350.0,
            "market_cleanup_notional_cap_usd": 1500.0,
            "edge_buffer_bps": 2.0,
        },
        "tier2": {
            "chunk_notional_cap_usd": 250.0,
            "market_cleanup_notional_cap_usd": 800.0,
            "edge_buffer_bps": 4.0,
        },
        "lower_tier": {
            "chunk_notional_cap_usd": 150.0,
            "market_cleanup_notional_cap_usd": 0.0,
            "edge_buffer_bps": 8.0,
        },
    }


def _normalise_bool_map(
    baseline: Mapping[str, bool],
    incoming: Mapping[str, object] | None,
    *,
    allow_new_keys: bool = True,
) -> Dict[str, bool]:
    """Return a bool map starting from the baseline and applying incoming keys."""
    result = dict(baseline)
    if not incoming:
        return result
    for key, value in incoming.items():
        if not allow_new_keys and key not in result:
            continue
        result[key] = bool(value)
    return result


@dataclass(slots=True)
class AppSettings:
    """In-memory representation of persisted application settings."""

    sources: Dict[str, bool] = field(
        default_factory=lambda: dict(DEFAULT_SOURCES)
    )
    exchanges: Dict[str, bool] = field(
        default_factory=lambda: dict(DEFAULT_EXCHANGES)
    )
    analysis_exchanges: Dict[str, bool] = field(
        default_factory=lambda: dict(DEFAULT_ANALYSIS_EXCHANGES)
    )
    parser_refresh_seconds: int = 1200  # 20 minutes
    exchange_refresh_seconds: int = 300  # Funding Opportunities refresh (5 minutes)
    table_refresh_seconds: int = 60  # Page refresh
    account_refresh_seconds: int = 60  # Account/positions refresh (1 minute)
    positions_market_refresh_seconds: int = 60  # Positions market snapshot refresh
    summary_refresh_seconds: int = 1800  # Balance digest (30 minutes)
    protective: Dict[str, object] = field(
        default_factory=lambda: {
            "auto_protect_enabled": True,
            "auto_take_enabled": True,
            "send_margin_alerts": True,
            "send_missing_stop_alerts": True,
            "notification_primary_channel": "ntfy",
            "notification_fallback_channel": "telegram",
            "auto_margin_enabled": True,
            "auto_margin_reduce_enabled": True,
            "enforce_isolated_margin": True,
            "enforce_leverage": True,
            "target_leverage": 3.0,
            "kucoin_isolated_topup_only": True,
            "auto_rebalance_enabled": False,
            "stop_gap_from_liq_pct": 0.025,
            "stop_requote_threshold_pct": 0.0025,
            "stop_force_requote_max_age_sec": 60,
            "fallback_liq_factor_long": 0.33,
            "fallback_liq_factor_short": 1.66,
            "rebalance_delta_pct": 0.20,
            "rebalance_cooldown_sec": 120,
            "rebalance_limit_timeout_sec": 10,
            "rebalance_limit_offset_bps": 2.0,
            "rebalance_max_slippage_bps": 8.0,
            "target_safe_buffer_pct": 0.30,
            "margin_add_trigger_buffer_pct": 0.27,
            "margin_reduce_trigger_buffer_pct": 0.33,
            "warning_buffer_pct": 0.20,
            "panic_buffer_pct": 0.15,
            "min_free_balance_abs": 500.0,
            "min_free_balance_rel": 0.10,
            "margin_add_pct": 0.10,
            "margin_add_panic_pct": 0.20,
            "margin_reduce_pct": 0.10,
            "margin_adjust_cooldown_sec": 300,
            "position_check_interval_sec": 60,
            "auto_derisk_enabled": False,
            "auto_derisk_shadow_mode": True,
            "orphan_cleanup_enabled": True,
            "derisk_poll_sec": 5,
            "derisk_target_buffer_pct": 0.30,
            "derisk_warning_buffer_pct": 0.20,
            "derisk_panic_buffer_pct": 0.15,
            "derisk_recovery_buffer_pct": 0.35,
            "derisk_min_free_balance_abs": 500.0,
            "derisk_stale_positions_max_sec": 180,
            "derisk_failure_block_count": 2,
            "derisk_confirm_cycles": 2,
            "derisk_cooldown_sec": 120,
            "derisk_velocity_trigger_bps": 120.0,
            "derisk_qty_tolerance_pct": 0.10,
            "derisk_max_single_action_notional_usd": 500.0,
            "derisk_market_cleanup_only_in_emergency": True,
            "derisk_dust_notional_usd": 10.0,
        }
    )
    manual: Dict[str, object] = field(
        default_factory=lambda: {
            "enter_live_orderbook": False,
            "enter_live_depth": 5,
            "exit_live_orderbook": False,
            "exit_live_depth": 5,
            "auto_exit_policy": _default_manual_auto_exit_policy(),
            "ws_orders_health": {
                "bybit": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "binance": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "okx": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "gate": {
                    "heartbeat_interval": 20.0,
                    "heartbeat_timeout": 60.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 15.0,
                },
                "bitget": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "kucoin": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "bingx": {
                    "heartbeat_interval": 30.0,
                    "heartbeat_timeout": 90.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 20.0,
                },
            },
        }
    )

    def with_updates(self, payload: Mapping[str, object]) -> "AppSettings":
        """Return a new settings instance with the provided updates applied."""
        updated = replace(self)
        if "sources" in payload:
            updated.sources = _normalise_bool_map(
                DEFAULT_SOURCES, payload["sources"]
            )
        else:
            updated.sources = _normalise_bool_map(
                DEFAULT_SOURCES, self.sources, allow_new_keys=True
            )
        if "exchanges" in payload:
            updated.exchanges = _normalise_bool_map(
                _default_exchanges(), payload["exchanges"]
            )
        else:
            updated.exchanges = _normalise_bool_map(
                _default_exchanges(), self.exchanges, allow_new_keys=True
            )
        if "analysis_exchanges" in payload:
            updated.analysis_exchanges = _normalise_bool_map(
                DEFAULT_ANALYSIS_EXCHANGES, payload["analysis_exchanges"]
            )
        else:
            updated.analysis_exchanges = _normalise_bool_map(
                DEFAULT_ANALYSIS_EXCHANGES, self.analysis_exchanges, allow_new_keys=True
            )
        updated.parser_refresh_seconds = int(
            payload.get("parser_refresh_seconds", self.parser_refresh_seconds)
        )
        updated.exchange_refresh_seconds = int(
            payload.get(
                "exchange_refresh_seconds",
                self.exchange_refresh_seconds,
            )
        )
        updated.table_refresh_seconds = int(
            payload.get("table_refresh_seconds", self.table_refresh_seconds)
        )
        updated.account_refresh_seconds = int(
            payload.get("account_refresh_seconds", self.account_refresh_seconds)
        )
        updated.positions_market_refresh_seconds = int(
            payload.get(
                "positions_market_refresh_seconds",
                self.positions_market_refresh_seconds,
            )
        )
        # Allow optional summary_refresh_seconds; fall back to current value when omitted/None.
        summary_value = payload.get("summary_refresh_seconds", self.summary_refresh_seconds)
        if summary_value is None:
            summary_value = self.summary_refresh_seconds
        updated.summary_refresh_seconds = int(summary_value)
        protective_value = payload.get("protective", self.protective)
        if protective_value is None:
            protective_value = self.protective
        updated.protective = dict(protective_value)
        manual_value = payload.get("manual", self.manual)
        if manual_value is None:
            manual_value = self.manual
        updated.manual = dict(manual_value)
        return updated.normalised()

    def normalised(self) -> "AppSettings":
        """Ensure the settings align with the latest defaults."""
        self.sources = _normalise_bool_map(DEFAULT_SOURCES, self.sources)
        self.exchanges = _normalise_bool_map(
            _default_exchanges(), self.exchanges
        )
        self.analysis_exchanges = _normalise_bool_map(
            DEFAULT_ANALYSIS_EXCHANGES, self.analysis_exchanges
        )
        defaults = {
            "auto_protect_enabled": True,
            "auto_take_enabled": True,
            "send_margin_alerts": True,
            "send_missing_stop_alerts": True,
            "notification_primary_channel": "ntfy",
            "notification_fallback_channel": "telegram",
            "auto_margin_enabled": True,
            "auto_margin_reduce_enabled": True,
            "enforce_isolated_margin": True,
            "enforce_leverage": True,
            "target_leverage": 3.0,
            "kucoin_isolated_topup_only": True,
            "auto_rebalance_enabled": False,
            "stop_gap_from_liq_pct": 0.025,
            "stop_requote_threshold_pct": 0.0025,
            "stop_force_requote_max_age_sec": 60,
            "fallback_liq_factor_long": 0.33,
            "fallback_liq_factor_short": 1.66,
            "rebalance_delta_pct": 0.20,
            "rebalance_cooldown_sec": 120,
            "rebalance_limit_timeout_sec": 10,
            "rebalance_limit_offset_bps": 2.0,
            "rebalance_max_slippage_bps": 8.0,
            "target_safe_buffer_pct": 0.30,
            "margin_add_trigger_buffer_pct": 0.27,
            "margin_reduce_trigger_buffer_pct": 0.33,
            "warning_buffer_pct": 0.20,
            "panic_buffer_pct": 0.15,
            "min_free_balance_abs": 500.0,
            "min_free_balance_rel": 0.10,
            "margin_add_pct": 0.10,
            "margin_add_panic_pct": 0.20,
            "margin_reduce_pct": 0.10,
            "margin_adjust_cooldown_sec": 300,
            "position_check_interval_sec": 60,
            "auto_derisk_enabled": False,
            "auto_derisk_shadow_mode": True,
            "orphan_cleanup_enabled": True,
            "derisk_poll_sec": 5,
            "derisk_target_buffer_pct": 0.30,
            "derisk_warning_buffer_pct": 0.20,
            "derisk_panic_buffer_pct": 0.15,
            "derisk_recovery_buffer_pct": 0.35,
            "derisk_min_free_balance_abs": 500.0,
            "derisk_stale_positions_max_sec": 180,
            "derisk_failure_block_count": 2,
            "derisk_confirm_cycles": 2,
            "derisk_cooldown_sec": 120,
            "derisk_velocity_trigger_bps": 120.0,
            "derisk_qty_tolerance_pct": 0.10,
            "derisk_max_single_action_notional_usd": 500.0,
            "derisk_market_cleanup_only_in_emergency": True,
            "derisk_dust_notional_usd": 10.0,
        }
        merged = dict(defaults)
        if isinstance(self.protective, dict):
            merged.update(self.protective)
        self.protective = merged
        manual_defaults = {
            "enter_live_orderbook": False,
            "enter_live_depth": 5,
            "exit_live_orderbook": False,
            "exit_live_depth": 5,
            "auto_exit_policy": _default_manual_auto_exit_policy(),
            "ws_orders_health": {
                "binance": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "bybit": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "okx": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "gate": {
                    "heartbeat_interval": 20.0,
                    "heartbeat_timeout": 60.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 15.0,
                },
                "bitget": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "kucoin": {
                    "heartbeat_interval": 15.0,
                    "heartbeat_timeout": 45.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 12.0,
                },
                "bingx": {
                    "heartbeat_interval": 30.0,
                    "heartbeat_timeout": 90.0,
                    "reconnect_attempts": 3,
                    "reconnect_grace_sec": 20.0,
                },
            },
        }
        manual = dict(manual_defaults)
        if isinstance(self.manual, dict):
            manual.update(self.manual)
        auto_exit_policy = manual.get("auto_exit_policy")
        merged_auto_exit_policy = _default_manual_auto_exit_policy()
        if isinstance(auto_exit_policy, Mapping):
            for tier_key, defaults in merged_auto_exit_policy.items():
                incoming_section = auto_exit_policy.get(tier_key)
                if isinstance(incoming_section, Mapping):
                    merged_auto_exit_policy[tier_key] = dict(defaults)
                    merged_auto_exit_policy[tier_key].update(incoming_section)
        manual["auto_exit_policy"] = merged_auto_exit_policy
        self.manual = manual
        return self

    def validate(self) -> None:
        """Validate invariants, raising ValueError if anything is invalid."""
        if not any(self.sources.values()):
            raise ValueError("At least one data source must remain enabled.")
        if not any(self.exchanges.values()):
            raise ValueError("At least one exchange must remain enabled.")
        if not any(self.analysis_exchanges.values()):
            raise ValueError("At least one analysis exchange must remain enabled.")
        if (
            self.parser_refresh_seconds < MIN_REFRESH_SECONDS
            or self.table_refresh_seconds < MIN_REFRESH_SECONDS
            or self.exchange_refresh_seconds < MIN_REFRESH_SECONDS
            or self.account_refresh_seconds < MIN_REFRESH_SECONDS
            or self.positions_market_refresh_seconds < MIN_REFRESH_SECONDS
            or self.summary_refresh_seconds < MIN_REFRESH_SECONDS
        ):
            raise ValueError(
                f"Refresh intervals must be >= {MIN_REFRESH_SECONDS} seconds."
            )
        if (
            self.parser_refresh_seconds > MAX_REFRESH_SECONDS
            or self.table_refresh_seconds > MAX_REFRESH_SECONDS
            or self.exchange_refresh_seconds > MAX_REFRESH_SECONDS
            or self.account_refresh_seconds > MAX_REFRESH_SECONDS
            or self.positions_market_refresh_seconds > MAX_REFRESH_SECONDS
            or self.summary_refresh_seconds > MAX_REFRESH_SECONDS
        ):
            raise ValueError(
                f"Refresh intervals must be <= {MAX_REFRESH_SECONDS} seconds."
            )
        try:
            protective = self.protective or {}
            if protective.get("stop_gap_from_liq_pct", 0.025) < 0:
                raise ValueError("stop_gap_from_liq_pct must be >= 0.")
            if protective.get("stop_requote_threshold_pct", 0.0025) < 0:
                raise ValueError("stop_requote_threshold_pct must be >= 0.")
            if float(protective.get("target_leverage", 3.0) or 0.0) <= 0:
                raise ValueError("target_leverage must be > 0.")
            if float(protective.get("derisk_target_buffer_pct", 0.30) or 0.0) < 0:
                raise ValueError("derisk_target_buffer_pct must be >= 0.")
            if float(protective.get("derisk_warning_buffer_pct", 0.20) or 0.0) < 0:
                raise ValueError("derisk_warning_buffer_pct must be >= 0.")
            if float(protective.get("derisk_panic_buffer_pct", 0.15) or 0.0) < 0:
                raise ValueError("derisk_panic_buffer_pct must be >= 0.")
            if float(protective.get("derisk_recovery_buffer_pct", 0.35) or 0.0) < 0:
                raise ValueError("derisk_recovery_buffer_pct must be >= 0.")
            if int(protective.get("derisk_confirm_cycles", 2) or 0) < 1:
                raise ValueError("derisk_confirm_cycles must be >= 1.")
            if int(protective.get("derisk_failure_block_count", 2) or 0) < 1:
                raise ValueError("derisk_failure_block_count must be >= 1.")
            primary_channel = str(protective.get("notification_primary_channel", "telegram") or "").strip().lower()
            if primary_channel not in {"telegram", "pushbullet", "ntfy"}:
                raise ValueError("notification_primary_channel must be telegram, pushbullet, or ntfy.")
            fallback_channel = str(protective.get("notification_fallback_channel", "none") or "").strip().lower()
            if fallback_channel not in {"none", "telegram", "pushbullet", "ntfy"}:
                raise ValueError("notification_fallback_channel must be none, telegram, pushbullet, or ntfy.")
        except Exception as exc:
            raise ValueError(f"Invalid protective settings: {exc}") from exc
        try:
            manual = self.manual or {}
            auto_exit_policy = manual.get("auto_exit_policy") or {}
            for tier_key, section in auto_exit_policy.items():
                if not isinstance(section, Mapping):
                    raise ValueError(f"manual.auto_exit_policy.{tier_key} must be an object.")
                for field_name in (
                    "chunk_notional_cap_usd",
                    "market_cleanup_notional_cap_usd",
                    "edge_buffer_bps",
                ):
                    value = float(section.get(field_name, 0.0) or 0.0)
                    if value < 0:
                        raise ValueError(f"manual.auto_exit_policy.{tier_key}.{field_name} must be >= 0.")
        except Exception as exc:
            raise ValueError(f"Invalid manual settings: {exc}") from exc

    def to_dict(self) -> Dict[str, object]:
        return {
            "sources": dict(self.sources),
            "exchanges": dict(self.exchanges),
            "analysis_exchanges": dict(self.analysis_exchanges),
            "parser_refresh_seconds": self.parser_refresh_seconds,
            "exchange_refresh_seconds": self.exchange_refresh_seconds,
            "table_refresh_seconds": self.table_refresh_seconds,
            "account_refresh_seconds": self.account_refresh_seconds,
            "positions_market_refresh_seconds": self.positions_market_refresh_seconds,
            "summary_refresh_seconds": self.summary_refresh_seconds,
            "protective": dict(self.protective),
            "manual": dict(self.manual),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object] | None) -> "AppSettings":
        if not payload:
            return cls()
        instance = cls()
        instance = instance.with_updates(payload)
        return instance.normalised()


def _default_exchanges() -> Dict[str, bool]:
    return dict(DEFAULT_EXCHANGES)


class SettingsManager:
    """Thin wrapper around settings persistence."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or _DEFAULT_SETTINGS_PATH
        self._settings = self._load()

    @property
    def current(self) -> AppSettings:
        return self._settings

    def as_dict(self) -> Dict[str, object]:
        return self._settings.to_dict()

    def update(self, payload: Mapping[str, object]) -> AppSettings:
        candidate = self._settings.with_updates(payload)
        candidate.validate()
        self._settings = candidate.normalised()
        self.save()
        return self._settings

    def set(self, new_settings: AppSettings) -> None:
        new_settings.validate()
        self._settings = new_settings.normalised()
        self.save()

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(self._settings.to_dict(), handle, indent=2)

    def _load(self) -> AppSettings:
        if not self._path.exists():
            return AppSettings()
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            raise ValueError(f"Failed to load settings: {exc}") from exc
        return AppSettings.from_dict(data)

    def reload(self) -> AppSettings:
        self._settings = self._load()
        return self._settings

    def enabled_sources(self) -> Dict[str, bool]:
        return dict(self._settings.sources)

    def enabled_exchanges(self) -> Dict[str, bool]:
        return dict(self._settings.exchanges)

    def enabled_analysis_exchanges(self) -> Dict[str, bool]:
        return dict(self._settings.analysis_exchanges)
