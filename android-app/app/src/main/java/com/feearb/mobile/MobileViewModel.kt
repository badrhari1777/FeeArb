package com.feearb.mobile

import android.app.Application
import androidx.compose.runtime.Immutable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

enum class PositionFilter(val label: String) {
    All("All"),
    Risk("Risk"),
    FundingSoon("Funding Soon"),
    AutoExitOn("Auto Exit On"),
}

enum class PositionSort(val label: String) {
    ByPnl("By PnL"),
    ByLiqRisk("By Liq Risk"),
    ByNextFunding("By Next Funding"),
    BySymbol("By Symbol"),
}

@Immutable
data class AdvancedSettingsUiState(
    val maxSlippageBps: String = "",
    val timeoutSec: String = "",
    val maxRuntimeSec: String = "",
    val repriceSec: String = "",
    val chunkQty: String = "",
    val chunkNotional: String = "",
    val forceChunkQty: Boolean = false,
    val hedgeOrderType: String = "market",
    val hedgeLimitMode: String = "passive",
    val hedgeFavorableBps: String = "",
    val hedgeAdverseBps: String = "",
    val hedgeRepriceMinSec: String = "",
    val limitOffsetBps: String = "",
    val limitOffsetTicks: String = "",
    val maxLimitDeviationBps: String = "",
    val useOrderbookCheck: Boolean = true,
    val exitAllowFlip: Boolean = false,
    val marginMode: String = "isolated",
)

@Immutable
data class ManualFormUiState(
    val action: String = "enter",
    val symbol: String = "",
    val qty: String = "",
    val notional: String = "",
    val longExchange: String = "",
    val shortExchange: String = "",
    val fromExchange: String = "",
    val toExchange: String = "",
    val side: String = "long",
    val mode: String = "smart",
    val rollMode: String = "smart-roll",
    val expensiveLeg: String? = null,
)

@Immutable
data class MobileUiState(
    val baseUrl: String = "http://10.0.2.2:8000/",
    val statusText: String = "",
    val positionsLoading: Boolean = false,
    val positionsErrorText: String? = null,
    val manualLoading: Boolean = false,
    val manualDefaultsLoading: Boolean = false,
    val manualDefaultsErrorText: String? = null,
    val positionsResponse: MobilePositionsResponse = MobilePositionsResponse(),
    val manualDefaults: ManualDefaultsResponse? = null,
    val positionFilter: PositionFilter = PositionFilter.All,
    val positionSort: PositionSort = PositionSort.ByNextFunding,
    val manualForm: ManualFormUiState = ManualFormUiState(),
    val advancedSettings: AdvancedSettingsUiState = AdvancedSettingsUiState(),
    val manualPlanText: String = "No plan yet.",
    val manualStatusText: String = "",
    val executionId: String? = null,
    val executionStatus: String? = null,
    val executionLogText: String = "No execution logs yet.",
    val executeConfirmationText: String? = null,
)

class MobileViewModel(application: Application) : AndroidViewModel(application) {
    private val settingsStore = SettingsStore(application.applicationContext)
    private val gson = GsonBuilder().setPrettyPrinting().create()
    private var api: FeeArbApi = FeeArbApiFactory.create(settingsStore.loadBaseUrl())
    private var pollingJob: Job? = null
    private var pendingExecuteRequest: ManualRequest? = null
    private var pendingExecuteAction: String? = null

    var uiState by mutableStateOf(
        MobileUiState(
            baseUrl = settingsStore.loadBaseUrl(),
            statusText = "Connecting...",
        )
    )
        private set

    init {
        refreshAll()
    }

    fun refreshAll() {
        refreshPositions()
        loadManualDefaults()
    }

    fun refreshPositions() {
        viewModelScope.launch {
            uiState = uiState.copy(
                positionsLoading = true,
                positionsErrorText = null,
                statusText = "Refreshing positions...",
            )
            runCatching { api.getMobilePositions() }
                .onSuccess { payload ->
                    uiState = uiState.copy(
                        positionsLoading = false,
                        positionsErrorText = null,
                        positionsResponse = payload,
                        statusText = "Positions updated.",
                    )
                }
                .onFailure { error ->
                    uiState = uiState.copy(
                        positionsLoading = false,
                        positionsErrorText = error.message ?: "unknown error",
                        statusText = "Positions refresh failed: ${error.message}",
                    )
                }
        }
    }

    fun loadManualDefaults() {
        viewModelScope.launch {
            uiState = uiState.copy(manualDefaultsLoading = true, manualDefaultsErrorText = null)
            runCatching { api.getManualDefaults() }
                .onSuccess { payload ->
                    val exchanges = payload.exchanges
                    val currentForm = uiState.manualForm
                    val nextForm = currentForm.copy(
                        longExchange = currentForm.longExchange.ifBlank { exchanges.getOrNull(0).orEmpty() },
                        shortExchange = currentForm.shortExchange.ifBlank { exchanges.getOrNull(1) ?: exchanges.getOrNull(0).orEmpty() },
                        fromExchange = currentForm.fromExchange.ifBlank { exchanges.getOrNull(1) ?: exchanges.getOrNull(0).orEmpty() },
                        toExchange = currentForm.toExchange.ifBlank { exchanges.getOrNull(0).orEmpty() },
                    )
                    uiState = uiState.copy(
                        manualDefaultsLoading = false,
                        manualDefaultsErrorText = null,
                        manualDefaults = payload,
                        manualForm = nextForm,
                        advancedSettings = settingsStore.loadAdvancedSettings(payload.defaults),
                        statusText = "Manual defaults loaded.",
                    )
                }
                .onFailure { error ->
                    uiState = uiState.copy(
                        manualDefaultsLoading = false,
                        manualDefaultsErrorText = error.message ?: "unknown error",
                        statusText = "Manual defaults failed: ${error.message}",
                    )
                }
        }
    }

    fun updateBaseUrl(value: String) {
        uiState = uiState.copy(baseUrl = value)
    }

    fun applyBaseUrl() {
        val normalized = uiState.baseUrl.trim().ifBlank { "http://10.0.2.2:8000/" }
        settingsStore.saveBaseUrl(normalized)
        api = FeeArbApiFactory.create(normalized)
        uiState = uiState.copy(baseUrl = normalized, statusText = "Base URL updated.")
        refreshAll()
    }

    fun updateFilter(filter: PositionFilter) {
        uiState = uiState.copy(positionFilter = filter)
    }

    fun updateSort(sort: PositionSort) {
        uiState = uiState.copy(positionSort = sort)
    }

    fun updateManualForm(transform: (ManualFormUiState) -> ManualFormUiState) {
        uiState = uiState.copy(manualForm = transform(uiState.manualForm))
    }

    fun updateAdvancedSettings(transform: (AdvancedSettingsUiState) -> AdvancedSettingsUiState) {
        val next = transform(uiState.advancedSettings)
        settingsStore.saveAdvancedSettings(next)
        uiState = uiState.copy(advancedSettings = next)
    }

    fun prefillManualFromPosition(card: PositionCardDto) {
        uiState = uiState.copy(
            manualForm = uiState.manualForm.copy(
                symbol = card.symbol,
                longExchange = card.long_exchange.orEmpty(),
                shortExchange = card.short_exchange.orEmpty(),
                fromExchange = card.short_exchange.orEmpty(),
                toExchange = card.long_exchange.orEmpty(),
            )
        )
    }

    fun saveAutoExit(card: PositionCardDto, enabled: Boolean, targetSpreadPct: String) {
        val longExchange = card.long_exchange ?: return
        val shortExchange = card.short_exchange ?: return
        viewModelScope.launch {
            val target = targetSpreadPct.toDoubleOrNull()
            if (enabled && target == null) {
                uiState = uiState.copy(statusText = "Auto-exit target spread is required.")
                return@launch
            }
            runCatching {
                api.updateAutoExitRule(
                    AutoExitRuleRequest(
                        symbol = card.symbol,
                        long_exchange = longExchange,
                        short_exchange = shortExchange,
                        enabled = enabled,
                        target_spread_pct = if (enabled) target else null,
                    )
                )
            }.onSuccess {
                uiState = uiState.copy(statusText = "Auto-exit updated.")
                refreshPositions()
            }.onFailure { error ->
                uiState = uiState.copy(statusText = "Auto-exit update failed: ${error.message}")
            }
        }
    }

    fun analyzeManual() {
        val request = buildManualRequest(dryRun = true, includeAction = true) ?: return
        viewModelScope.launch {
            clearPendingExecute()
            uiState = uiState.copy(manualLoading = true, manualStatusText = "Analyzing...")
            runCatching { api.manualAnalyze(request) }
                .onSuccess { payload ->
                    val hintedChunk = request.chunk_qty ?: payload.optDoubleOrNull("recommended_chunk_qty") ?: payload.optDoubleOrNull("min_chunk_qty")
                    if (uiState.advancedSettings.chunkQty.isBlank() && hintedChunk != null) {
                        val nextSettings = uiState.advancedSettings.copy(chunkQty = formatCompact(hintedChunk))
                        settingsStore.saveAdvancedSettings(nextSettings)
                        uiState = uiState.copy(advancedSettings = nextSettings)
                    }
                    uiState = uiState.copy(
                        manualLoading = false,
                        manualPlanText = formatPlan(payload),
                        manualStatusText = if (payload.hasErrors()) "Analyze completed with errors." else "Analyze completed.",
                    )
                }
                .onFailure { error ->
                    uiState = uiState.copy(
                        manualLoading = false,
                        manualStatusText = "Analyze failed: ${error.message}",
                    )
                }
        }
    }

    fun executeManual() {
        val preflightRequest = buildManualRequest(dryRun = true, includeAction = false) ?: return
        clearPendingExecute()
        viewModelScope.launch {
            uiState = uiState.copy(manualLoading = true, manualStatusText = "Running preflight...")
            val endpoint = endpointForAction(uiState.manualForm.action)
            runCatching { endpoint(preflightRequest) }
                .onSuccess { preflight ->
                    uiState = uiState.copy(
                        manualLoading = false,
                        manualPlanText = formatPlan(preflight),
                    )
                    if (preflight.hasErrors()) {
                        uiState = uiState.copy(manualStatusText = "Preflight failed. Fix errors first.")
                        return@onSuccess
                    }
                    val executeRequest = buildManualRequest(dryRun = false, includeAction = false) ?: return@onSuccess
                    pendingExecuteRequest = executeRequest
                    pendingExecuteAction = uiState.manualForm.action
                    uiState = uiState.copy(
                        executeConfirmationText = buildExecuteConfirmation(executeRequest),
                        manualStatusText = "Preflight passed. Review the payload and confirm execution.",
                    )
                }
                .onFailure { error ->
                    uiState = uiState.copy(
                        manualLoading = false,
                        manualStatusText = "Preflight failed: ${error.message}",
                    )
                }
        }
    }

    fun confirmExecute() {
        val executeRequest = pendingExecuteRequest ?: run {
            uiState = uiState.copy(manualStatusText = "Nothing to execute.")
            return
        }
        val action = pendingExecuteAction ?: uiState.manualForm.action
        val endpoint = endpointForAction(action)
        viewModelScope.launch {
            uiState = uiState.copy(
                manualLoading = true,
                executeConfirmationText = null,
                executionId = null,
                executionStatus = null,
                executionLogText = "No execution logs yet.",
                manualStatusText = "Submitting execution...",
            )
            runCatching { endpoint(executeRequest) }
                .onSuccess { payload ->
                    val executionId = payload.get("execution_id")?.asString
                    clearPendingExecute()
                    uiState = uiState.copy(
                        manualLoading = false,
                        manualPlanText = formatPlan(payload),
                    )
                    if (!executionId.isNullOrBlank()) {
                        uiState = uiState.copy(
                            executionId = executionId,
                            executionStatus = "running",
                            manualStatusText = "Execution started.",
                        )
                        startPollingExecution(executionId)
                    } else {
                        uiState = uiState.copy(manualStatusText = "Execution completed.")
                    }
                }
                .onFailure { error ->
                    clearPendingExecute()
                    uiState = uiState.copy(
                        manualLoading = false,
                        manualStatusText = "Execution failed: ${error.message}",
                    )
                }
        }
    }

    fun cancelExecute() {
        if (pendingExecuteRequest == null && uiState.executeConfirmationText == null) {
            return
        }
        clearPendingExecute()
        uiState = uiState.copy(manualStatusText = "Execution canceled before submit.")
    }

    fun stopExecution() {
        val execId = uiState.executionId ?: return
        viewModelScope.launch {
            runCatching { api.stopManualExec(execId) }
                .onSuccess {
                    uiState = uiState.copy(manualStatusText = "Stop requested...")
                }
                .onFailure { error ->
                    uiState = uiState.copy(manualStatusText = "Stop failed: ${error.message}")
                }
        }
    }

    fun visibleCards(): List<PositionCardDto> {
        val filtered = uiState.positionsResponse.cards.filter { card ->
            when (uiState.positionFilter) {
                PositionFilter.All -> true
                PositionFilter.Risk -> card.flags["risk"] == true
                PositionFilter.FundingSoon -> card.flags["funding_soon"] == true
                PositionFilter.AutoExitOn -> card.flags["auto_exit_on"] == true
            }
        }
        return when (uiState.positionSort) {
            PositionSort.ByPnl -> filtered.sortedByDescending { it.net_pnl ?: Double.NEGATIVE_INFINITY }
            PositionSort.ByLiqRisk -> filtered.sortedBy { it.liq_distance_pct ?: Double.POSITIVE_INFINITY }
            PositionSort.ByNextFunding -> filtered.sortedBy { it.minutes_to_next_funding ?: Double.POSITIVE_INFINITY }
            PositionSort.BySymbol -> filtered.sortedBy { it.symbol }
        }
    }

    private fun startPollingExecution(executionId: String) {
        pollingJob?.cancel()
        pollingJob = viewModelScope.launch {
            while (true) {
                runCatching { api.manualExecStatus(executionId) }
                    .onSuccess { payload ->
                        uiState = uiState.copy(
                            executionId = executionId,
                            executionStatus = payload.status,
                            executionLogText = formatLogs(payload.logs),
                            manualPlanText = payload.result?.let(::formatPlan) ?: uiState.manualPlanText,
                            manualStatusText = when (payload.status) {
                                "running" -> if (payload.stop_requested) "Stop requested; waiting..." else "Execution running..."
                                "completed" -> "Execution completed."
                                "completed_with_errors" -> "Execution completed with errors."
                                "failed" -> "Execution failed: ${payload.error ?: "unknown error"}"
                                else -> "Execution status: ${payload.status ?: "-"}"
                            },
                        )
                        if (payload.status != "running") {
                            return@launch
                        }
                    }
                    .onFailure { error ->
                        uiState = uiState.copy(manualStatusText = "Execution polling failed: ${error.message}")
                        return@launch
                    }
                delay(2_000)
            }
        }
    }

    private fun endpointForAction(action: String): suspend (ManualRequest) -> JsonObject {
        return when (action.lowercase()) {
            "exit" -> { payload -> api.manualExit(payload) }
            "roll" -> { payload -> api.manualRoll(payload) }
            else -> { payload -> api.manualEnter(payload) }
        }
    }

    private fun buildManualRequest(dryRun: Boolean, includeAction: Boolean): ManualRequest? {
        val form = uiState.manualForm
        val advanced = uiState.advancedSettings
        val symbol = form.symbol.trim().uppercase()
        if (symbol.isBlank()) {
            uiState = uiState.copy(manualStatusText = "Symbol is required.")
            return null
        }
        val qty = form.qty.toDoubleOrNull()
        val notional = form.notional.toDoubleOrNull()
        if (form.action != "exit" && qty == null && notional == null) {
            uiState = uiState.copy(manualStatusText = "Qty or notional is required.")
            return null
        }
        val mode = if (form.action == "roll") {
            form.rollMode
        } else if (form.mode == "fast") {
            if (form.action == "exit") "fast-exit" else "fast-enter"
        } else {
            if (form.action == "exit") "smart-exit" else "smart-enter"
        }
        return ManualRequest(
            symbol = symbol,
            qty = qty,
            notional = notional,
            mode = mode,
            max_slippage_bps = advanced.maxSlippageBps.toDoubleOrNull(),
            timeout_sec = advanced.timeoutSec.toIntOrNull(),
            max_runtime_sec = advanced.maxRuntimeSec.toIntOrNull(),
            reprice_sec = advanced.repriceSec.toDoubleOrNull(),
            chunk_qty = advanced.chunkQty.toDoubleOrNull(),
            chunk_notional = advanced.chunkNotional.toDoubleOrNull(),
            force_chunk_qty = advanced.forceChunkQty,
            hedge_order_type = advanced.hedgeOrderType.ifBlank { null },
            hedge_limit_mode = advanced.hedgeLimitMode.ifBlank { null },
            hedge_favorable_bps = advanced.hedgeFavorableBps.toDoubleOrNull(),
            hedge_adverse_bps = advanced.hedgeAdverseBps.toDoubleOrNull(),
            hedge_reprice_min_sec = advanced.hedgeRepriceMinSec.toDoubleOrNull(),
            limit_offset_bps = advanced.limitOffsetBps.toDoubleOrNull(),
            limit_offset_ticks = advanced.limitOffsetTicks.toIntOrNull(),
            max_limit_deviation_bps = advanced.maxLimitDeviationBps.toDoubleOrNull(),
            use_orderbook_check = advanced.useOrderbookCheck,
            fallback_to_market = false,
            async_run = !dryRun,
            dry_run = dryRun,
            expensive_leg = form.expensiveLeg,
            margin_mode = advanced.marginMode.ifBlank { null },
            long_exchange = if (form.action != "roll") form.longExchange.ifBlank { null } else null,
            short_exchange = if (form.action != "roll") form.shortExchange.ifBlank { null } else null,
            from_exchange = if (form.action == "roll") form.fromExchange.ifBlank { null } else null,
            to_exchange = if (form.action == "roll") form.toExchange.ifBlank { null } else null,
            side = if (form.action == "roll") form.side else null,
            exit_allow_flip = if (form.action == "exit") advanced.exitAllowFlip else null,
            action = if (includeAction) form.action else null,
        )
    }

    private fun clearPendingExecute() {
        pendingExecuteRequest = null
        pendingExecuteAction = null
        uiState = uiState.copy(executeConfirmationText = null)
    }

    private fun formatPlan(payload: JsonObject): String {
        val lines = mutableListOf<String>()
        appendJsonArray(lines, "Errors", payload.getAsJsonArray("errors"))
        appendJsonArray(lines, "Warnings", payload.getAsJsonArray("warnings"))
        payload.optDoubleOrNull("spread_pct")?.let { lines += "Spread: ${formatCompact(it)}%" }
        payload.optDoubleOrNull("recommended_qty")?.let { lines += "Recommended qty: ${formatCompact(it)}" }
        payload.optDoubleOrNull("recommended_notional")?.let { lines += "Recommended notional: ${formatCompact(it)}" }
        payload.optDoubleOrNull("min_chunk_qty")?.let { lines += "Min chunk qty: ${formatCompact(it)}" }
        payload.optDoubleOrNull("recommended_chunk_qty")?.let { lines += "Recommended chunk qty: ${formatCompact(it)}" }
        return if (lines.isNotEmpty()) lines.joinToString("\n") else gson.toJson(payload)
    }

    private fun formatLogs(logs: List<ManualExecLogEntry>): String {
        if (logs.isEmpty()) return "No execution logs yet."
        return logs.joinToString("\n") { entry ->
            val ts = entry.ts?.let { "[$it] " }.orEmpty()
            val event = entry.event?.let { "$it: " }.orEmpty()
            val message = entry.message.orEmpty()
            val data = entry.data?.takeIf { it.size() > 0 }?.let { " ${gson.toJson(it)}" }.orEmpty()
            "$ts$event$message$data"
        }
    }

    private fun buildExecuteConfirmation(request: ManualRequest): String {
        val lines = mutableListOf<String>()
        val action = pendingExecuteAction ?: uiState.manualForm.action
        lines += "Action: ${action.replaceFirstChar(Char::uppercase)}"
        lines += "Symbol: ${request.symbol}"
        when (action) {
            "roll" -> {
                lines += "From -> To: ${(request.from_exchange ?: "-").uppercase()} -> ${(request.to_exchange ?: "-").uppercase()}"
                lines += "Side: ${request.side ?: "-"}"
            }
            else -> {
                lines += "Long / Short: ${(request.long_exchange ?: "-").uppercase()} / ${(request.short_exchange ?: "-").uppercase()}"
            }
        }
        request.qty?.let { lines += "Qty: ${formatCompact(it)}" }
        request.notional?.let { lines += "Notional: ${formatCompact(it)}" }
        lines += "Mode: ${request.mode}"
        lines += "Expensive leg: ${request.expensive_leg ?: "auto_hint"}"
        request.chunk_qty?.let { lines += "Chunk qty: ${formatCompact(it)}" }
        request.chunk_notional?.let { lines += "Chunk notional: ${formatCompact(it)}" }
        request.max_slippage_bps?.let { lines += "Max slippage: ${formatCompact(it)} bps" }
        request.margin_mode?.let { lines += "Margin mode: $it" }
        return lines.joinToString("\n")
    }

    private fun appendJsonArray(lines: MutableList<String>, title: String, array: JsonArray?) {
        if (array == null || array.size() == 0) return
        lines += "$title:"
        array.forEach { lines += "  - ${it.asString}" }
    }

    private fun JsonObject.optDoubleOrNull(key: String): Double? {
        val element = get(key) ?: return null
        return runCatching { element.asDouble }.getOrNull()
    }

    private fun JsonObject.hasErrors(): Boolean {
        val element = get("errors")
        return element != null && element.isJsonArray && element.asJsonArray.size() > 0
    }

    private fun formatCompact(value: Double): String {
        val text = String.format("%.6f", value)
        return text.trimEnd('0').trimEnd('.')
    }

    class Factory(private val application: Application) : ViewModelProvider.Factory {
        @Suppress("UNCHECKED_CAST")
        override fun <T : ViewModel> create(modelClass: Class<T>): T {
            return MobileViewModel(application) as T
        }
    }
}
