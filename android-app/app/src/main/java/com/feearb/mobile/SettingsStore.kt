package com.feearb.mobile

import android.content.Context

private const val PREFS_NAME = "feearb_mobile_prefs"
private const val KEY_BASE_URL = "base_url"
private const val KEY_REMOTE_ACCESS_TOKEN = "remote_access_token"
private const val KEY_ADV_MAX_SLIPPAGE = "adv_max_slippage"
private const val KEY_ADV_TIMEOUT = "adv_timeout"
private const val KEY_ADV_RUNTIME = "adv_runtime"
private const val KEY_ADV_RUNTIME_MINUTES = "adv_runtime_minutes"
private const val KEY_ADV_UNTIL_FILLED = "adv_until_filled"
private const val KEY_EXECUTION_TIMING_POLICY_VERSION = "execution_timing_policy_version"
private const val KEY_ADV_REPRICE = "adv_reprice"
private const val KEY_ADV_CHUNK_QTY = "adv_chunk_qty"
private const val KEY_ADV_CHUNK_NOTIONAL = "adv_chunk_notional"
private const val KEY_ADV_FORCE_CHUNK = "adv_force_chunk"
private const val KEY_ADV_HEDGE_TYPE = "adv_hedge_type"
private const val KEY_ADV_HEDGE_MODE = "adv_hedge_mode"
private const val KEY_ADV_HEDGE_FAVORABLE = "adv_hedge_favorable"
private const val KEY_ADV_HEDGE_ADVERSE = "adv_hedge_adverse"
private const val KEY_ADV_HEDGE_REPRICE = "adv_hedge_reprice"
private const val KEY_ADV_LIMIT_BPS = "adv_limit_bps"
private const val KEY_ADV_LIMIT_TICKS = "adv_limit_ticks"
private const val KEY_ADV_MAX_LIMIT_DEV = "adv_max_limit_dev"
private const val KEY_ADV_USE_ORDERBOOK = "adv_use_orderbook"
private const val KEY_ADV_EXIT_ALLOW_FLIP = "adv_exit_allow_flip"
private const val KEY_ADV_MARGIN_MODE = "adv_margin_mode"

class SettingsStore(context: Context) {
    private val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun loadBaseUrl(): String = prefs.getString(KEY_BASE_URL, "http://10.0.2.2:8000/") ?: "http://10.0.2.2:8000/"

    fun saveBaseUrl(value: String) {
        prefs.edit().putString(KEY_BASE_URL, value).apply()
    }

    fun loadRemoteAccessToken(): String = prefs.getString(KEY_REMOTE_ACCESS_TOKEN, "").orEmpty()

    fun saveRemoteAccessToken(value: String) {
        prefs.edit().putString(KEY_REMOTE_ACCESS_TOKEN, value).apply()
    }

    fun loadAdvancedSettings(defaults: ManualDefaultsDto?): AdvancedSettingsUiState {
        val savedRuntimeMinutes = prefs.getString(KEY_ADV_RUNTIME_MINUTES, null)
        val legacyRuntimeSeconds = prefs.getString(KEY_ADV_RUNTIME, null)
        val runtimeMinutes = resolveExecutionRuntimeMinutes(
            savedMinutes = savedRuntimeMinutes,
            legacySeconds = legacyRuntimeSeconds,
            backendDefaultSeconds = defaults?.max_runtime_sec,
            previousPolicyVersion = prefs.getInt(KEY_EXECUTION_TIMING_POLICY_VERSION, 0),
        )
        prefs.edit()
            .putString(KEY_ADV_RUNTIME_MINUTES, runtimeMinutes)
            .remove(KEY_ADV_RUNTIME)
            .putInt(KEY_EXECUTION_TIMING_POLICY_VERSION, EXECUTION_TIMING_POLICY_VERSION)
            .apply()
        return AdvancedSettingsUiState(
            maxSlippageBps = prefs.getString(KEY_ADV_MAX_SLIPPAGE, defaults?.max_slippage_bps?.toString().orEmpty()).orEmpty(),
            timeoutSec = prefs.getString(KEY_ADV_TIMEOUT, defaults?.timeout_sec?.toString().orEmpty()).orEmpty(),
            maxRuntimeMinutes = runtimeMinutes,
            untilFilled = prefs.getBoolean(KEY_ADV_UNTIL_FILLED, false),
            repriceSec = prefs.getString(KEY_ADV_REPRICE, defaults?.reprice_sec?.toString().orEmpty()).orEmpty(),
            chunkQty = prefs.getString(KEY_ADV_CHUNK_QTY, defaults?.chunk_qty?.toString().orEmpty()).orEmpty(),
            chunkNotional = prefs.getString(KEY_ADV_CHUNK_NOTIONAL, defaults?.chunk_notional?.toString().orEmpty()).orEmpty(),
            forceChunkQty = prefs.getBoolean(KEY_ADV_FORCE_CHUNK, defaults?.force_chunk_qty ?: false),
            hedgeOrderType = prefs.getString(KEY_ADV_HEDGE_TYPE, defaults?.hedge_order_type ?: "market").orEmpty(),
            hedgeLimitMode = prefs.getString(KEY_ADV_HEDGE_MODE, defaults?.hedge_limit_mode ?: "aggressive").orEmpty(),
            hedgeFavorableBps = prefs.getString(KEY_ADV_HEDGE_FAVORABLE, defaults?.hedge_favorable_bps?.toString().orEmpty()).orEmpty(),
            hedgeAdverseBps = prefs.getString(KEY_ADV_HEDGE_ADVERSE, defaults?.hedge_adverse_bps?.toString().orEmpty()).orEmpty(),
            hedgeRepriceMinSec = prefs.getString(KEY_ADV_HEDGE_REPRICE, defaults?.hedge_reprice_min_sec?.toString().orEmpty()).orEmpty(),
            limitOffsetBps = prefs.getString(KEY_ADV_LIMIT_BPS, defaults?.limit_offset_bps?.toString().orEmpty()).orEmpty(),
            limitOffsetTicks = prefs.getString(KEY_ADV_LIMIT_TICKS, defaults?.limit_offset_ticks?.toString().orEmpty()).orEmpty(),
            maxLimitDeviationBps = prefs.getString(KEY_ADV_MAX_LIMIT_DEV, defaults?.max_limit_deviation_bps?.toString().orEmpty()).orEmpty(),
            useOrderbookCheck = prefs.getBoolean(KEY_ADV_USE_ORDERBOOK, defaults?.use_orderbook_check ?: true),
            exitAllowFlip = prefs.getBoolean(KEY_ADV_EXIT_ALLOW_FLIP, defaults?.exit_allow_flip ?: false),
            marginMode = prefs.getString(KEY_ADV_MARGIN_MODE, defaults?.margin_mode ?: "isolated").orEmpty(),
        )
    }

    fun saveAdvancedSettings(state: AdvancedSettingsUiState) {
        prefs.edit()
            .putString(KEY_ADV_MAX_SLIPPAGE, state.maxSlippageBps)
            .putString(KEY_ADV_TIMEOUT, state.timeoutSec)
            .putString(KEY_ADV_RUNTIME_MINUTES, state.maxRuntimeMinutes)
            .putBoolean(KEY_ADV_UNTIL_FILLED, state.untilFilled)
            .putString(KEY_ADV_REPRICE, state.repriceSec)
            .putString(KEY_ADV_CHUNK_QTY, state.chunkQty)
            .putString(KEY_ADV_CHUNK_NOTIONAL, state.chunkNotional)
            .putBoolean(KEY_ADV_FORCE_CHUNK, state.forceChunkQty)
            .putString(KEY_ADV_HEDGE_TYPE, state.hedgeOrderType)
            .putString(KEY_ADV_HEDGE_MODE, state.hedgeLimitMode)
            .putString(KEY_ADV_HEDGE_FAVORABLE, state.hedgeFavorableBps)
            .putString(KEY_ADV_HEDGE_ADVERSE, state.hedgeAdverseBps)
            .putString(KEY_ADV_HEDGE_REPRICE, state.hedgeRepriceMinSec)
            .putString(KEY_ADV_LIMIT_BPS, state.limitOffsetBps)
            .putString(KEY_ADV_LIMIT_TICKS, state.limitOffsetTicks)
            .putString(KEY_ADV_MAX_LIMIT_DEV, state.maxLimitDeviationBps)
            .putBoolean(KEY_ADV_USE_ORDERBOOK, state.useOrderbookCheck)
            .putBoolean(KEY_ADV_EXIT_ALLOW_FLIP, state.exitAllowFlip)
            .putString(KEY_ADV_MARGIN_MODE, state.marginMode)
            .apply()
    }
}
