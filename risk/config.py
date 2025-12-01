from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RiskConfig:
    target_safe_buffer_pct: float = 0.25  # 25%
    warning_buffer_pct: float = 0.20      # 20%
    panic_buffer_pct: float = 0.15        # 15%

    stop_gap_from_liq_pct: float = 0.07   # 7% от ликвидации
    stop_requote_threshold_pct: float = 0.005  # 0.5% изменение цены стопа/тейка для обновления

    min_free_balance_abs: float = 500.0
    min_free_balance_rel: float = 0.10    # 10% от маржи на бирже

    balance_check_interval_sec: int = 60
    position_check_interval_sec: int = 60
    panic_close_batch_size: int = 2

    telegram_alert_chat_id: str = ""


def default_risk_config() -> RiskConfig:
    return RiskConfig()
