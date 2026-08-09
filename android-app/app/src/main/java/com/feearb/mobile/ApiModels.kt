package com.feearb.mobile

import com.google.gson.JsonElement
import com.google.gson.JsonObject

data class MobilePositionsResponse(
    val status: String? = null,
    val last_updated: String? = null,
    val account_last_updated: String? = null,
    val balances: List<BalanceDto> = emptyList(),
    val balance_summary: BalanceSummaryDto = BalanceSummaryDto(),
    val cards: List<PositionCardDto> = emptyList(),
    val filters: Map<String, Int> = emptyMap(),
)

data class BalanceDto(
    val exchange: String = "",
    val account_alias: String? = null,
    val account_label: String? = null,
    val account_type: String? = null,
    val asset: String? = null,
    val total: Double? = null,
    val available: Double? = null,
    val used: Double? = null,
    val temporary_occupied_usd: Double? = null,
    val margin_ratio: Double? = null,
    val status: String? = null,
    val error: String? = null,
    val updated_at: String? = null,
)

data class BalanceAggregateDto(
    val asset: String? = "USDT",
    val total: Double? = null,
    val available: Double? = null,
    val used: Double? = null,
    val temporary_occupied_usd: Double? = null,
    val reporting_accounts: Int = 0,
    val healthy_accounts: Int = 0,
)

data class BalanceSummaryDto(
    val asset: String? = "USDT",
    val overall: BalanceAggregateDto = BalanceAggregateDto(),
    val bybit_main: BalanceAggregateDto = BalanceAggregateDto(),
    val bybit_pump: BalanceAggregateDto = BalanceAggregateDto(),
    val bybit_combined: BalanceAggregateDto = BalanceAggregateDto(),
)

data class PositionCardDto(
    val symbol: String = "",
    val pair_label: String = "",
    val is_multi_leg: Boolean = false,
    val long_exchange: String? = null,
    val short_exchange: String? = null,
    val net_pnl: Double? = null,
    val expected_funding: Double? = null,
    val live_spread_pct: Double? = null,
    val next_funding: String? = null,
    val minutes_to_next_funding: Double? = null,
    val liq_distance_pct: Double? = null,
    val risk_level: String? = null,
    val flags: Map<String, Boolean> = emptyMap(),
    val auto_exit: AutoExitStateDto = AutoExitStateDto(),
    val position_summary: PositionSummaryDto = PositionSummaryDto(),
    val risk: PositionRiskDto = PositionRiskDto(),
    val funding: PositionFundingDto = PositionFundingDto(),
    val legs: List<PositionLegDto> = emptyList(),
)

data class AutoExitStateDto(
    val key: String? = null,
    val spread_enabled: Boolean = false,
    val v1_enabled: Boolean = false,
    val target_spread_pct: Double? = null,
    val exit_percent: Double? = 100.0,
    val exit_once: Boolean = true,
    val live_spread_pct: Double? = null,
    val live_spread_source: String? = null,
    val status: String? = null,
    val raw_status: String? = null,
    val reason: String? = null,
    val updated_at: String? = null,
)

data class PositionSummaryDto(
    val quantity: Double? = null,
    val long_quantity: Double? = null,
    val short_quantity: Double? = null,
    val hedged_quantity: Double? = null,
    val imbalance_quantity: Double? = null,
    val imbalance_pct: Double? = null,
    val amount_usdt: Double? = null,
    val gross_amount_usdt: Double? = null,
    val pair_entry_spread_pct: Double? = null,
    val pair_mark_spread_pct: Double? = null,
    val long_entry_avg: Double? = null,
    val short_entry_avg: Double? = null,
    val long_mark_avg: Double? = null,
    val short_mark_avg: Double? = null,
    val long_leverage_avg: Double? = null,
    val short_leverage_avg: Double? = null,
)

data class PositionRiskDto(
    val liq_distance_pct: Double? = null,
    val long_liq_price: Double? = null,
    val short_liq_price: Double? = null,
    val long_stop_price: Double? = null,
    val short_stop_price: Double? = null,
    val long_take_price: Double? = null,
    val short_take_price: Double? = null,
)

data class PositionFundingDto(
    val net_funding_rate: Double? = null,
    val expected_funding: Double? = null,
    val next_funding: String? = null,
    val minutes_to_next_funding: Double? = null,
)

data class PositionLegDto(
    val exchange: String? = null,
    val side: String? = null,
    val quantity: Double? = null,
    val amount: Double? = null,
    val entry_price: Double? = null,
    val mark_price: Double? = null,
    val unrealized_pnl: Double? = null,
    val funding_rate: Double? = null,
    val next_funding: String? = null,
    val next_funding_eta: String? = null,
    val leverage: Double? = null,
    val liquidation_price: Double? = null,
    val margin_mode: String? = null,
    val dist_to_liq_pct: Double? = null,
    val stop_price: Double? = null,
    val take_price: Double? = null,
    val expected_funding: Double? = null,
)

data class PositionsOverviewResponse(
    val schema: String? = null,
    val generated_at_ms: Long? = null,
    val summary: PositionsOverviewSummaryDto = PositionsOverviewSummaryDto(),
    val main: PositionsOverviewMainDto = PositionsOverviewMainDto(),
    val pump: PumpOverviewDto = PumpOverviewDto(),
)

data class PositionsOverviewSummaryDto(
    val main_positions: Int = 0,
    val pump_positions: Int = 0,
    val pump_cap: Int = 0,
    val total_unrealized_pnl_usd: Double? = null,
    val main_unrealized_pnl_usd: Double? = null,
    val pump_unrealized_pnl_usd: Double? = null,
    val min_liq_buffer_pct: Double? = null,
    val protection_issues: Int = 0,
    val high_risk_positions: Int = 0,
    val warning_risk_positions: Int = 0,
    val main_age_sec: Double? = null,
    val pump_age_sec: Double? = null,
)

data class PositionsOverviewMainDto(
    val status: String? = null,
    val last_updated: String? = null,
    val account_last_updated: String? = null,
    val balances: List<BalanceDto> = emptyList(),
    val filters: Map<String, Int> = emptyMap(),
    val positions: List<PositionCardDto> = emptyList(),
)

data class PumpOverviewDto(
    val status: String? = null,
    val entry_armed: Boolean = false,
    val blocked_reason: String? = null,
    val last_error: String? = null,
    val last_cycle_at_ms: Long? = null,
    val monitor_thread_alive: Boolean = false,
    val config: PumpOverviewConfigDto = PumpOverviewConfigDto(),
    val balance: PumpOverviewBalanceDto = PumpOverviewBalanceDto(),
    val capital_regime: PumpCapitalRegimeDto = PumpCapitalRegimeDto(),
    val auto_transfer: PumpAutoTransferDto = PumpAutoTransferDto(),
    val notifications: PumpOverviewNotificationsDto = PumpOverviewNotificationsDto(),
    val positions: List<PumpPositionDto> = emptyList(),
    val recent_events: List<JsonObject> = emptyList(),
)

data class PumpOverviewConfigDto(
    val total_capital_usd: Double? = null,
    val deployable_capital_usd: Double? = null,
    val reserve_usd: Double? = null,
    val entry_cap: Int = 0,
    val max_active_positions: Int = 0,
    val slot_margin_usd: Double? = null,
    val warning_liq_buffer_pct: Double? = null,
    val panic_liq_buffer_pct: Double? = null,
    val emergency_liq_buffer_pct: Double? = null,
    val exchange_stop_gap_from_liq_pct: Double? = null,
    val max_position_topup_usd: Double? = null,
    val max_total_topup_usd: Double? = null,
    val margin_reduce_trigger_buffer_pct: Double? = null,
    val margin_reduce_target_buffer_pct: Double? = null,
)

data class PumpOverviewBalanceDto(
    val total_usd: Double? = null,
    val available_usd: Double? = null,
    val used_usd: Double? = null,
    val temporary_occupied_usd: Double? = null,
)

data class PumpCapitalRegimeDto(
    val mode: String? = null,
    val min_liq_buffer_pct: Double? = null,
    val min_liq_buffer_symbol: String? = null,
    val prefund_floor_usd: Double? = null,
    val removable_topup_usd: Double? = null,
    val temporary_occupied_usd: Double? = null,
    val new_slot_headroom_usd: Double? = null,
)

data class PumpAutoTransferDto(
    val enabled: Boolean = false,
    val main_wallet_floor_usd: Double? = null,
    val max_single_usd: Double? = null,
    val daily_cap_usd: Double? = null,
    val daily_used_usd: Double? = null,
    val daily_remaining_usd: Double? = null,
    val last_attempt_at_ms: Long? = null,
)

data class PumpOverviewNotificationsDto(
    val configured: Boolean = false,
    val last_event: String? = null,
    val last_status: String? = null,
    val last_at_ms: Long? = null,
    val last_error: String? = null,
)

data class PumpPositionDto(
    val module: String? = null,
    val account_alias: String? = null,
    val live_id: String? = null,
    val strategy_id: String? = null,
    val symbol: String = "",
    val side: String = "short",
    val status: String? = null,
    val qty: Double? = null,
    val avg_entry_price: Double? = null,
    val mark_price: Double? = null,
    val unrealized_pnl_usd: Double? = null,
    val liq_price: Double? = null,
    val liq_buffer_pct: Double? = null,
    val risk_level: String? = null,
    val tp_price: Double? = null,
    val stop_price: Double? = null,
    val protection_updated_at_ms: Long? = null,
    val margin_topup_usd: Double? = null,
    val margin_topup_cap_usd: Double? = null,
    val margin_reduce_confirm_count: Int = 0,
    val opened_at_ms: Long? = null,
    val age_h: Double? = null,
    val max_hold_h: Double? = null,
    val remaining_hold_h: Double? = null,
    val close_reason: String? = null,
    val last_error: String? = null,
    val tier: Map<String, JsonElement> = emptyMap(),
    val legs: List<PumpPositionLegDto> = emptyList(),
    val legs_filled: Int = 0,
    val legs_open: Int = 0,
)

data class PumpPositionLegDto(
    val step: Int? = null,
    val weight: Double? = null,
    val trigger_price: Double? = null,
    val margin_usd: Double? = null,
    val notional_usd: Double? = null,
    val status: String? = null,
    val filled_qty: Double? = null,
    val avg_fill_price: Double? = null,
)

data class ManualDefaultsResponse(
    val status: String? = null,
    val last_updated: String? = null,
    val exchanges: List<String> = emptyList(),
    val actions: List<String> = emptyList(),
    val main_modes: List<OptionDto> = emptyList(),
    val roll_modes: List<OptionDto> = emptyList(),
    val expensive_leg_options: ExpensiveLegOptionsDto = ExpensiveLegOptionsDto(),
    val defaults: ManualDefaultsDto = ManualDefaultsDto(),
    val advanced_sections: List<String> = emptyList(),
)

data class OptionDto(
    val id: String? = null,
    val label: String = "",
)

data class ExpensiveLegOptionsDto(
    val enter_exit: List<OptionDto> = emptyList(),
    val roll: List<OptionDto> = emptyList(),
)

data class ManualDefaultsDto(
    val max_slippage_bps: Double? = null,
    val margin_mode: String? = null,
    val timeout_sec: Int? = null,
    val max_runtime_sec: Int? = null,
    val reprice_sec: Double? = null,
    val chunk_qty: Double? = null,
    val chunk_notional: Double? = null,
    val force_chunk_qty: Boolean = false,
    val hedge_order_type: String? = null,
    val hedge_limit_mode: String? = null,
    val hedge_favorable_bps: Double? = null,
    val hedge_adverse_bps: Double? = null,
    val hedge_reprice_min_sec: Double? = null,
    val hedge_timeout_sec: Double? = null,
    val limit_offset_bps: Double? = null,
    val limit_offset_ticks: Int? = null,
    val max_limit_deviation_bps: Double? = null,
    val use_orderbook_check: Boolean = true,
    val exit_allow_flip: Boolean = false,
    val expensive_leg: String? = null,
)

data class AutoExitRuleRequest(
    val symbol: String,
    val long_exchange: String,
    val short_exchange: String,
    val enabled: Boolean,
    val spread_enabled: Boolean? = null,
    val target_spread_pct: Double?,
    val exit_percent: Double? = null,
    val exit_once: Boolean? = true,
)

data class AutoArbRuleRequest(
    val id: String? = null,
    val symbol: String,
    val long_exchange: String,
    val short_exchange: String,
    val setup_mode: String = "entry_range",
    val budget_mode: String = "notional",
    val max_qty: Double? = null,
    val max_notional: Double? = null,
    val range_start_pct: Double,
    val range_end_pct: Double,
    val exit_range_start_pct: Double? = null,
    val exit_range_end_pct: Double? = null,
    val level_count: Int? = null,
    val max_slippage_bps: Double = 8.0,
    val liquidity_safety_factor: Double = 0.70,
    val confirm_samples: Int = 2,
    val enabled: Boolean = true,
    val live: Boolean = false,
)

data class PositionActionRequest(
    val symbol: String,
    val long_exchange: String,
    val short_exchange: String,
    val action: String,
    val percent: Double,
    val dry_run: Boolean,
    val async_run: Boolean,
    val max_runtime_sec: Int? = null,
)

data class ManualRequest(
    val symbol: String,
    val qty: Double? = null,
    val notional: Double? = null,
    val mode: String,
    val max_slippage_bps: Double? = null,
    val spread_min_pct: Double? = null,
    val spread_max_pct: Double? = null,
    val timeout_sec: Int? = null,
    val max_runtime_sec: Int? = null,
    val reprice_sec: Double? = null,
    val chunk_qty: Double? = null,
    val chunk_notional: Double? = null,
    val force_chunk_qty: Boolean? = null,
    val hedge_order_type: String? = null,
    val hedge_limit_mode: String? = null,
    val hedge_favorable_bps: Double? = null,
    val hedge_adverse_bps: Double? = null,
    val hedge_reprice_min_sec: Double? = null,
    val hedge_timeout_sec: Double? = null,
    val limit_offset_bps: Double? = null,
    val limit_offset_ticks: Int? = null,
    val max_limit_deviation_bps: Double? = null,
    val use_orderbook_check: Boolean = true,
    val allow_liquidity_chunking: Boolean = true,
    val fallback_to_market: Boolean = false,
    val async_run: Boolean = true,
    val dry_run: Boolean = false,
    val expensive_leg: String? = null,
    val margin_mode: String? = null,
    val long_exchange: String? = null,
    val short_exchange: String? = null,
    val from_exchange: String? = null,
    val to_exchange: String? = null,
    val side: String? = null,
    val exit_allow_flip: Boolean? = null,
    val action: String? = null,
)

data class MobileManualSpreadRequest(
    val symbol: String,
    val action: String = "enter",
    val long_exchange: String? = null,
    val short_exchange: String? = null,
    val from_exchange: String? = null,
    val to_exchange: String? = null,
    val side: String? = null,
)

data class MobileManualSpreadResponse(
    val status: String? = null,
    val symbol: String? = null,
    val action: String? = null,
    val side: String? = null,
    val buy_exchange: String? = null,
    val sell_exchange: String? = null,
    val buy_price: Double? = null,
    val sell_price: Double? = null,
    val spread_pct: Double? = null,
    val quotes: Map<String, ManualQuoteDto> = emptyMap(),
    val errors: List<String> = emptyList(),
    val warnings: List<String> = emptyList(),
    val generated_at: String? = null,
)

data class ManualQuoteDto(
    val exchange: String? = null,
    val symbol: String? = null,
    val bid: Double? = null,
    val ask: Double? = null,
    val mid: Double? = null,
    val mark_price: Double? = null,
    val source: String? = null,
    val updated_at: String? = null,
    val age_sec: Double? = null,
    val error: String? = null,
)

data class ManualExecStatusResponse(
    val execution_id: String? = null,
    val status: String? = null,
    val error: String? = null,
    val stop_requested: Boolean = false,
    val result: JsonElement? = null,
    val logs: List<ManualExecLogEntry> = emptyList(),
)

data class ManualExecLogEntry(
    val ts: String? = null,
    val event: String? = null,
    val message: String? = null,
    val data: JsonElement? = null,
)
