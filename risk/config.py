from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RiskConfig:
    target_safe_buffer_pct: float = 0.25  # 25%
    warning_buffer_pct: float = 0.20      # 20%
    panic_buffer_pct: float = 0.15        # 15%

    stop_gap_from_liq_pct: float = 0.07   # 7% gap from liquidation
    stop_requote_threshold_pct: float = 0.005  # 0.5% change required to re-quote
    fallback_liq_factor_long: float = 0.33
    fallback_liq_factor_short: float = 1.66
    fallback_take_rr_pct: float = 1.0     # +100% / -100% take-profit fallback vs base

    min_free_balance_abs: float = 500.0
    min_free_balance_rel: float = 0.10    # 10% of used margin

    position_check_interval_sec: int = 600

    protective_warn_cooldown_sec: int = 600

    telegram_alert_chat_id: str = ""
    send_missing_stop_alerts: bool = True


def default_risk_config() -> RiskConfig:
    return RiskConfig()
