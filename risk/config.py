from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RiskConfig:
    target_safe_buffer_pct: float = 0.30  # 30%
    warning_buffer_pct: float = 0.20      # 20%
    panic_buffer_pct: float = 0.15        # 15%

    stop_gap_from_liq_pct: float = 0.025  # 2.5% gap from liquidation
    stop_requote_threshold_pct: float = 0.0025  # 0.25% change required to re-quote
    stop_force_requote_max_age_sec: int = 60  # force stop refresh when older than this
    fallback_liq_factor_long: float = 0.33
    fallback_liq_factor_short: float = 1.66
    fallback_take_rr_pct: float = 0.30    # 30% fallback when no peer stop is available

    min_free_balance_abs: float = 500.0
    min_free_balance_rel: float = 0.10    # 10% of used margin

    position_check_interval_sec: int = 60

    protective_warn_cooldown_sec: int = 600

    telegram_alert_chat_id: str = ""
    notification_primary_channel: str = "ntfy"
    notification_fallback_channel: str = "telegram"
    send_missing_stop_alerts: bool = True


def default_risk_config() -> RiskConfig:
    return RiskConfig()
