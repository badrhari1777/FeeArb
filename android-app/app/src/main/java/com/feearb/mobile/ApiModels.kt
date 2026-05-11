package com.feearb.mobile

import com.google.gson.JsonObject

data class MobilePositionsResponse(
    val status: String? = null,
    val last_updated: String? = null,
    val cards: List<PositionCardDto> = emptyList(),
    val filters: Map<String, Int> = emptyMap(),
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
    val live_spread_pct: Double? = null,
    val live_spread_source: String? = null,
    val status: String? = null,
    val raw_status: String? = null,
    val reason: String? = null,
    val updated_at: String? = null,
)

data class PositionSummaryDto(
    val quantity: Double? = null,
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
    val target_spread_pct: Double?,
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
    val limit_offset_bps: Double? = null,
    val limit_offset_ticks: Int? = null,
    val max_limit_deviation_bps: Double? = null,
    val use_orderbook_check: Boolean = true,
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

data class ManualExecStatusResponse(
    val execution_id: String? = null,
    val status: String? = null,
    val error: String? = null,
    val stop_requested: Boolean = false,
    val result: JsonObject? = null,
    val logs: List<ManualExecLogEntry> = emptyList(),
)

data class ManualExecLogEntry(
    val ts: String? = null,
    val event: String? = null,
    val message: String? = null,
    val data: JsonObject? = null,
)
