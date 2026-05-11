@file:OptIn(
    androidx.compose.foundation.layout.ExperimentalLayoutApi::class,
    androidx.compose.material3.ExperimentalMaterial3Api::class,
)

package com.feearb.mobile

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ListAlt
import androidx.compose.material.icons.outlined.Settings
import androidx.compose.material.icons.outlined.SwapHoriz
import androidx.compose.material3.AssistChip
import androidx.compose.material3.AssistChipDefaults
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.FilterChip
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import com.feearb.mobile.ui.theme.FeeArbTheme
import kotlin.math.absoluteValue

private enum class AppTab { Positions, Manual, Settings }

@Composable
fun FeeArbApp(viewModel: MobileViewModel) {
    FeeArbTheme {
        var currentTab by remember { mutableStateOf(AppTab.Positions) }
        val state = viewModel.uiState
        state.executeConfirmationText?.let { confirmationText ->
            AlertDialog(
                onDismissRequest = viewModel::cancelExecute,
                title = { Text("Confirm live execution") },
                text = { Text(confirmationText) },
                confirmButton = {
                    Button(onClick = viewModel::confirmExecute, enabled = !state.manualLoading) {
                        Text(if (state.manualLoading) "Submitting..." else "Execute")
                    }
                },
                dismissButton = {
                    TextButton(onClick = viewModel::cancelExecute, enabled = !state.manualLoading) {
                        Text("Cancel")
                    }
                },
            )
        }
        Scaffold(
            topBar = {
                TopAppBar(
                    title = {
                        Column {
                            Text("FeeArb Mobile")
                            Text(
                                text = state.statusText,
                                style = MaterialTheme.typography.labelSmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                maxLines = 1,
                                overflow = TextOverflow.Ellipsis,
                            )
                        }
                    },
                    actions = {
                        TextButton(onClick = {
                            if (currentTab == AppTab.Positions) viewModel.refreshPositions() else viewModel.refreshAll()
                        }) {
                            Text("Refresh")
                        }
                    },
                )
            },
            bottomBar = {
                NavigationBar {
                    NavigationBarItem(
                        selected = currentTab == AppTab.Positions,
                        onClick = { currentTab = AppTab.Positions },
                        icon = { Icon(Icons.AutoMirrored.Outlined.ListAlt, contentDescription = null) },
                        label = { Text("Positions") },
                    )
                    NavigationBarItem(
                        selected = currentTab == AppTab.Manual,
                        onClick = { currentTab = AppTab.Manual },
                        icon = { Icon(Icons.Outlined.SwapHoriz, contentDescription = null) },
                        label = { Text("Manual") },
                    )
                    NavigationBarItem(
                        selected = currentTab == AppTab.Settings,
                        onClick = { currentTab = AppTab.Settings },
                        icon = { Icon(Icons.Outlined.Settings, contentDescription = null) },
                        label = { Text("Settings") },
                    )
                }
            },
        ) { padding ->
            when (currentTab) {
                AppTab.Positions -> PositionsScreen(
                    cards = viewModel.visibleCards(),
                    totalCards = state.positionsResponse.cards.size,
                    loading = state.positionsLoading,
                    errorText = state.positionsErrorText,
                    lastUpdated = state.positionsResponse.last_updated,
                    selectedFilter = state.positionFilter,
                    selectedSort = state.positionSort,
                    filters = state.positionsResponse.filters,
                    onFilterSelected = viewModel::updateFilter,
                    onSortSelected = viewModel::updateSort,
                    onSaveAutoExit = viewModel::saveAutoExit,
                    onUseInManual = {
                        viewModel.prefillManualFromPosition(it)
                        currentTab = AppTab.Manual
                    },
                    modifier = Modifier.padding(padding),
                )
                AppTab.Manual -> ManualScreen(
                    state = state,
                    onFormChange = viewModel::updateManualForm,
                    onAnalyze = viewModel::analyzeManual,
                    onExecute = viewModel::executeManual,
                    onStop = viewModel::stopExecution,
                    onReloadDefaults = viewModel::loadManualDefaults,
                    modifier = Modifier.padding(padding),
                )
                AppTab.Settings -> SettingsScreen(
                    state = state,
                    onBaseUrlChange = viewModel::updateBaseUrl,
                    onApplyBaseUrl = viewModel::applyBaseUrl,
                    onAdvancedChange = viewModel::updateAdvancedSettings,
                    modifier = Modifier.padding(padding),
                )
            }
        }
    }
}

@Composable
private fun PositionsScreen(
    cards: List<PositionCardDto>,
    totalCards: Int,
    loading: Boolean,
    errorText: String?,
    lastUpdated: String?,
    selectedFilter: PositionFilter,
    selectedSort: PositionSort,
    filters: Map<String, Int>,
    onFilterSelected: (PositionFilter) -> Unit,
    onSortSelected: (PositionSort) -> Unit,
    onSaveAutoExit: (PositionCardDto, Boolean, String) -> Unit,
    onUseInManual: (PositionCardDto) -> Unit,
    modifier: Modifier = Modifier,
) {
    LazyColumn(
        modifier = modifier.fillMaxSize(),
        contentPadding = PaddingValues(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        item {
            SectionCard("Filters") {
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    PositionFilter.entries.forEach { filter ->
                        val count = when (filter) {
                            PositionFilter.All -> filters["all"] ?: cards.size
                            PositionFilter.Risk -> filters["risk"] ?: 0
                            PositionFilter.FundingSoon -> filters["funding_soon"] ?: 0
                            PositionFilter.AutoExitOn -> filters["auto_exit_on"] ?: 0
                        }
                        FilterChip(
                            selected = selectedFilter == filter,
                            onClick = { onFilterSelected(filter) },
                            label = { Text("${filter.label} ($count)") },
                        )
                    }
                }
                Spacer(Modifier.height(10.dp))
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    PositionSort.entries.forEach { sort ->
                        FilterChip(
                            selected = selectedSort == sort,
                            onClick = { onSortSelected(sort) },
                            label = { Text(sort.label) },
                        )
                    }
                }
            }
        }
        if (loading) {
            item {
                StateCard(
                    title = "Refreshing positions",
                    body = "Fetching latest mobile positions payload from backend.",
                    loading = true,
                )
            }
        }
        if (!errorText.isNullOrBlank()) {
            item {
                StateCard(
                    title = "Positions error",
                    body = errorText,
                )
            }
        }
        if (cards.isEmpty()) {
            item {
                StateCard(
                    title = if (totalCards == 0) "No positions" else "No cards match filters",
                    body = when {
                        totalCards == 0 && !lastUpdated.isNullOrBlank() -> "Backend returned no active positions. Last update: $lastUpdated"
                        totalCards == 0 -> "Backend returned no active positions."
                        else -> "Try switching the filter or sort chips above."
                    },
                )
            }
        }
        items(cards, key = { "${it.symbol}-${it.pair_label}" }) { card ->
            PositionCard(card = card, onSaveAutoExit = { enabled, target -> onSaveAutoExit(card, enabled, target) }, onUseInManual = { onUseInManual(card) })
        }
    }
}

@Composable
private fun PositionCard(card: PositionCardDto, onSaveAutoExit: (Boolean, String) -> Unit, onUseInManual: () -> Unit) {
    var expanded by remember(card.symbol, card.pair_label) { mutableStateOf(false) }
    var autoEnabled by remember(card.symbol, card.pair_label, card.auto_exit.spread_enabled) { mutableStateOf(card.auto_exit.spread_enabled) }
    var targetText by remember(card.symbol, card.pair_label, card.auto_exit.target_spread_pct) { mutableStateOf(card.auto_exit.target_spread_pct?.toString().orEmpty()) }
    Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.35f))) {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Column {
                    Text(card.symbol, style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                    Text(card.pair_label, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
                Spacer(Modifier.width(8.dp))
                RiskChip(card.liq_distance_pct, card.risk_level)
            }
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
                MetricBlock("Net PnL", formatSigned(card.net_pnl))
                MetricBlock("Exp. Funding", formatSigned(card.expected_funding))
            }
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
                MetricBlock("Live spread", formatPercent(card.live_spread_pct))
                MetricBlock("Next funding", formatMinutes(card.minutes_to_next_funding))
                MetricBlock("Liq dist", formatPercent(card.liq_distance_pct))
            }
            SectionCard("Auto Exit") {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text("Spread Exit")
                    Spacer(Modifier.width(8.dp))
                    StatusPill(card.auto_exit.status ?: "off")
                    Spacer(Modifier.width(8.dp))
                    Switch(checked = autoEnabled, onCheckedChange = { autoEnabled = it })
                }
                OutlinedTextField(value = targetText, onValueChange = { targetText = it }, label = { Text("Target spread %") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    listOf("-0.5", "-1.0", "-2.0").forEach { preset ->
                        AssistChip(onClick = { targetText = preset }, label = { Text(preset) })
                    }
                }
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = { onSaveAutoExit(autoEnabled, targetText) }) { Text("Save") }
                    Button(onClick = onUseInManual) { Text("Manual") }
                    Button(onClick = { expanded = !expanded }) { Text(if (expanded) "Collapse" else "Expand") }
                }
            }
            AnimatedVisibility(expanded) {
                Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                    SectionCard("Position Summary") {
                        KeyValue("Quantity", formatNumber(card.position_summary.quantity))
                        KeyValue("Amount USDT", formatNumber(card.position_summary.amount_usdt))
                        KeyValue("Gross amount", formatNumber(card.position_summary.gross_amount_usdt))
                        KeyValue("Entry spread", formatPercent(card.position_summary.pair_entry_spread_pct))
                        KeyValue("Mark spread", formatPercent(card.position_summary.pair_mark_spread_pct))
                    }
                    SectionCard("Risk") {
                        KeyValue("Liq distance", formatPercent(card.risk.liq_distance_pct))
                        KeyValue("Long liq", formatNumber(card.risk.long_liq_price))
                        KeyValue("Short liq", formatNumber(card.risk.short_liq_price))
                        KeyValue("Long stop", formatNumber(card.risk.long_stop_price))
                        KeyValue("Short stop", formatNumber(card.risk.short_stop_price))
                    }
                    SectionCard("Funding") {
                        KeyValue("Net funding", formatPercent(card.funding.net_funding_rate?.times(100.0)))
                        KeyValue("Expected funding", formatSigned(card.funding.expected_funding))
                        KeyValue("Next funding", formatMinutes(card.funding.minutes_to_next_funding))
                    }
                    SectionCard("More") {
                        KeyValue("Status", card.auto_exit.status ?: "-")
                        KeyValue("Raw status", card.auto_exit.raw_status ?: "-")
                        KeyValue("Reason", card.auto_exit.reason ?: "-")
                    }
                    SectionCard("Legs") {
                        card.legs.forEachIndexed { index, leg ->
                            if (index > 0) HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
                            KeyValue("Exchange", (leg.exchange ?: "-").uppercase())
                            KeyValue("Side", leg.side ?: "-")
                            KeyValue("Qty", formatNumber(leg.quantity?.absoluteValue))
                            KeyValue("Entry", formatNumber(leg.entry_price))
                            KeyValue("Mark", formatNumber(leg.mark_price))
                            KeyValue("PnL", formatSigned(leg.unrealized_pnl))
                            KeyValue("Leverage", formatNumber(leg.leverage))
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun ManualScreen(
    state: MobileUiState,
    onFormChange: ((ManualFormUiState) -> ManualFormUiState) -> Unit,
    onAnalyze: () -> Unit,
    onExecute: () -> Unit,
    onStop: () -> Unit,
    onReloadDefaults: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val defaults = state.manualDefaults
    val exchanges = defaults?.exchanges.orEmpty()
    val expOptions = if (state.manualForm.action == "roll") defaults?.expensive_leg_options?.roll.orEmpty() else defaults?.expensive_leg_options?.enter_exit.orEmpty()
    val rollModes = defaults?.roll_modes.orEmpty()
    val manualReady = defaults != null && exchanges.isNotEmpty()
    LazyColumn(modifier = modifier.fillMaxSize(), contentPadding = PaddingValues(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        if (state.manualDefaultsLoading) {
            item {
                StateCard(
                    title = "Loading defaults",
                    body = "Fetching exchanges, modes, and advanced defaults from backend.",
                    loading = true,
                )
            }
        }
        if (!state.manualDefaultsErrorText.isNullOrBlank()) {
            item {
                SectionCard("Manual defaults error") {
                    Text(state.manualDefaultsErrorText)
                    Button(onClick = onReloadDefaults) { Text("Retry") }
                }
            }
        }
        if (defaults == null && !state.manualDefaultsLoading) {
            item {
                StateCard(
                    title = "Manual defaults unavailable",
                    body = "Open Settings, verify the base URL, then retry loading defaults.",
                )
            }
        }
        if (defaults != null && exchanges.isEmpty()) {
            item {
                StateCard(
                    title = "No enabled exchanges",
                    body = "Backend returned manual defaults but no enabled exchanges. Check project settings on the server.",
                )
            }
        }
        item {
            SectionCard("Manual Trade") {
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    listOf("enter", "exit", "roll").forEach { action ->
                        FilterChip(selected = state.manualForm.action == action, onClick = { onFormChange { it.copy(action = action) } }, label = { Text(action.replaceFirstChar(Char::uppercase)) })
                    }
                }
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = state.manualForm.symbol, onValueChange = { value -> onFormChange { it.copy(symbol = value) } }, label = { Text("Symbol") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = state.manualForm.qty, onValueChange = { value -> onFormChange { it.copy(qty = value) } }, label = { Text("Qty") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = state.manualForm.notional, onValueChange = { value -> onFormChange { it.copy(notional = value) } }, label = { Text("Notional") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(10.dp))
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    listOf("smart", "fast").forEach { mode ->
                        FilterChip(selected = state.manualForm.mode == mode, onClick = { onFormChange { it.copy(mode = mode) } }, label = { Text(mode.replaceFirstChar(Char::uppercase)) })
                    }
                }
                if (state.manualForm.action == "roll" && rollModes.isNotEmpty()) {
                    Spacer(Modifier.height(8.dp))
                    SimpleSelect(
                        "Roll mode",
                        rollModes.firstOrNull { it.id == state.manualForm.rollMode }?.label ?: state.manualForm.rollMode,
                        rollModes.map { it.label }
                    ) { selected ->
                        val option = rollModes.firstOrNull { it.label == selected }
                        onFormChange { it.copy(rollMode = option?.id ?: it.rollMode) }
                    }
                }
                Spacer(Modifier.height(8.dp))
                if (state.manualForm.action == "roll") {
                    SimpleSelect("From exchange", state.manualForm.fromExchange, exchanges) { selected -> onFormChange { it.copy(fromExchange = selected) } }
                } else {
                    SimpleSelect("Long exchange", state.manualForm.longExchange, exchanges) { selected -> onFormChange { it.copy(longExchange = selected) } }
                    Spacer(Modifier.height(8.dp))
                    SimpleSelect("Short exchange", state.manualForm.shortExchange, exchanges) { selected -> onFormChange { it.copy(shortExchange = selected) } }
                }
                if (state.manualForm.action == "roll") {
                    Spacer(Modifier.height(8.dp))
                    SimpleSelect("To exchange", state.manualForm.toExchange, exchanges) { selected -> onFormChange { it.copy(toExchange = selected) } }
                    Spacer(Modifier.height(8.dp))
                    SimpleSelect("Side", state.manualForm.side, listOf("long", "short")) { selected -> onFormChange { it.copy(side = selected) } }
                }
                Spacer(Modifier.height(8.dp))
                SimpleSelect("Expensive leg", expOptions.firstOrNull { it.id == state.manualForm.expensiveLeg }?.label ?: "Auto hint", expOptions.map { it.label }) { selected ->
                    val option = expOptions.firstOrNull { it.label == selected }
                    onFormChange { it.copy(expensiveLeg = option?.id) }
                }
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = state.advancedSettings.maxSlippageBps, onValueChange = {}, label = { Text("Max slippage (Settings)") }, modifier = Modifier.fillMaxWidth(), enabled = false, singleLine = true)
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = state.advancedSettings.marginMode, onValueChange = {}, label = { Text("Margin mode (Settings)") }, modifier = Modifier.fillMaxWidth(), enabled = false, singleLine = true)
                Spacer(Modifier.height(10.dp))
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = onAnalyze, enabled = manualReady && !state.manualLoading) { Text("Analyze") }
                    Button(onClick = onExecute, enabled = manualReady && !state.manualLoading) { Text("Execute") }
                    Button(onClick = onStop, enabled = state.executionId != null && !state.manualLoading) { Text("Stop") }
                }
                if (state.manualLoading) {
                    Spacer(Modifier.height(4.dp))
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        CircularProgressIndicator(modifier = Modifier.width(18.dp), strokeWidth = 2.dp)
                        Text("Working...", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    }
                }
                Text(state.manualStatusText, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
        item { SectionCard("Plan Summary") { MonoBlock(state.manualPlanText) } }
        item {
            SectionCard("Execution") {
                KeyValue("Execution id", state.executionId ?: "-")
                KeyValue("Status", state.executionStatus ?: "-")
                MonoBlock(state.executionLogText)
            }
        }
    }
}

@Composable
private fun SettingsScreen(
    state: MobileUiState,
    onBaseUrlChange: (String) -> Unit,
    onApplyBaseUrl: () -> Unit,
    onAdvancedChange: ((AdvancedSettingsUiState) -> AdvancedSettingsUiState) -> Unit,
    modifier: Modifier = Modifier,
) {
    LazyColumn(modifier = modifier.fillMaxSize(), contentPadding = PaddingValues(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        item {
            SectionCard("Connection") {
                OutlinedTextField(value = state.baseUrl, onValueChange = onBaseUrlChange, label = { Text("Backend base URL") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                Button(onClick = onApplyBaseUrl) { Text("Apply") }
            }
        }
        item {
            SectionCard("Execution") {
                SettingsField("Max slippage bps", state.advancedSettings.maxSlippageBps) { value -> onAdvancedChange { it.copy(maxSlippageBps = value) } }
                SettingsField("Timeout sec", state.advancedSettings.timeoutSec) { value -> onAdvancedChange { it.copy(timeoutSec = value) } }
                SettingsField("Max runtime sec", state.advancedSettings.maxRuntimeSec) { value -> onAdvancedChange { it.copy(maxRuntimeSec = value) } }
                SettingsField("Reprice sec", state.advancedSettings.repriceSec) { value -> onAdvancedChange { it.copy(repriceSec = value) } }
            }
        }
        item {
            SectionCard("Chunking") {
                SettingsField("Chunk qty", state.advancedSettings.chunkQty) { value -> onAdvancedChange { it.copy(chunkQty = value) } }
                SettingsField("Chunk notional", state.advancedSettings.chunkNotional) { value -> onAdvancedChange { it.copy(chunkNotional = value) } }
                BooleanSetting("Force chunk qty", state.advancedSettings.forceChunkQty) { value -> onAdvancedChange { it.copy(forceChunkQty = value) } }
            }
        }
        item {
            SectionCard("Hedge") {
                SimpleSelect("Hedge order type", state.advancedSettings.hedgeOrderType, listOf("market", "limit")) { selected -> onAdvancedChange { it.copy(hedgeOrderType = selected) } }
                Spacer(Modifier.height(8.dp))
                SimpleSelect("Hedge limit mode", state.advancedSettings.hedgeLimitMode, listOf("passive", "aggressive")) { selected -> onAdvancedChange { it.copy(hedgeLimitMode = selected) } }
                SettingsField("Favorable bps", state.advancedSettings.hedgeFavorableBps) { value -> onAdvancedChange { it.copy(hedgeFavorableBps = value) } }
                SettingsField("Adverse bps", state.advancedSettings.hedgeAdverseBps) { value -> onAdvancedChange { it.copy(hedgeAdverseBps = value) } }
            }
        }
        item {
            SectionCard("Safety") {
                SettingsField("Limit offset bps", state.advancedSettings.limitOffsetBps) { value -> onAdvancedChange { it.copy(limitOffsetBps = value) } }
                SettingsField("Limit offset ticks", state.advancedSettings.limitOffsetTicks) { value -> onAdvancedChange { it.copy(limitOffsetTicks = value) } }
                SettingsField("Max limit deviation bps", state.advancedSettings.maxLimitDeviationBps) { value -> onAdvancedChange { it.copy(maxLimitDeviationBps = value) } }
                BooleanSetting("Use orderbook check", state.advancedSettings.useOrderbookCheck) { value -> onAdvancedChange { it.copy(useOrderbookCheck = value) } }
                BooleanSetting("Exit allow flip", state.advancedSettings.exitAllowFlip) { value -> onAdvancedChange { it.copy(exitAllowFlip = value) } }
                SimpleSelect("Margin mode", state.advancedSettings.marginMode, listOf("isolated", "cross")) { selected -> onAdvancedChange { it.copy(marginMode = selected) } }
            }
        }
        item {
            SectionCard("System") {
                Text("WS health / reconnect controls stay on backend in v1 and are intentionally hidden from the main phone workflow.", color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
    }
}

@Composable
private fun SectionCard(title: String, content: @Composable ColumnScope.() -> Unit) {
    Card {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text(title, style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            content()
        }
    }
}

@Composable
private fun StateCard(title: String, body: String, loading: Boolean = false) {
    SectionCard(title) {
        if (loading) {
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
                CircularProgressIndicator(modifier = Modifier.width(18.dp), strokeWidth = 2.dp)
                Text(body, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        } else {
            Text(body, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}

@Composable
private fun KeyValue(label: String, value: String) {
    Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
        Text(label, color = MaterialTheme.colorScheme.onSurfaceVariant)
        Text(value, fontWeight = FontWeight.Medium)
    }
}

@Composable
private fun MetricBlock(label: String, value: String, modifier: Modifier = Modifier) {
    Column(modifier = modifier) {
        Text(label, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        Text(value, style = MaterialTheme.typography.titleSmall, fontWeight = FontWeight.SemiBold)
    }
}

@Composable
private fun RiskChip(distancePct: Double?, riskLevel: String?) {
    val color = when (riskLevel) {
        "high" -> Color(0xFFB3261E)
        "warn" -> Color(0xFFE67E22)
        else -> Color(0xFF2E7D32)
    }
    AssistChip(
        onClick = {},
        enabled = false,
        label = { Text("Liq ${formatPercent(distancePct)}") },
        colors = AssistChipDefaults.assistChipColors(
            disabledContainerColor = color.copy(alpha = 0.12f),
            disabledLabelColor = color,
        ),
    )
}

@Composable
private fun StatusPill(status: String) {
    val color = when (status) {
        "armed" -> Color(0xFF2E7D32)
        "waiting" -> Color(0xFF1565C0)
        "no_live_spread" -> Color(0xFFE67E22)
        else -> Color(0xFF6B7280)
    }
    AssistChip(
        onClick = {},
        enabled = false,
        label = { Text(status.replace('_', ' ')) },
        colors = AssistChipDefaults.assistChipColors(
            disabledContainerColor = color.copy(alpha = 0.12f),
            disabledLabelColor = color,
        ),
    )
}

@Composable
private fun MonoBlock(text: String) {
    Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.25f))) {
        Text(text, modifier = Modifier.fillMaxWidth().padding(12.dp), style = MaterialTheme.typography.bodySmall)
    }
}

@Composable
private fun SettingsField(label: String, value: String, onChange: (String) -> Unit) {
    OutlinedTextField(value = value, onValueChange = onChange, label = { Text(label) }, modifier = Modifier.fillMaxWidth(), singleLine = true)
}

@Composable
private fun BooleanSetting(label: String, value: Boolean, onChange: (Boolean) -> Unit) {
    Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.SpaceBetween) {
        Text(label)
        Switch(checked = value, onCheckedChange = onChange)
    }
}

@Composable
private fun SimpleSelect(label: String, value: String, options: List<String>, onSelected: (String) -> Unit) {
    Column {
        Text(label, style = MaterialTheme.typography.labelMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
        Spacer(Modifier.height(6.dp))
        if (options.isEmpty()) {
            Text("No options loaded yet.", color = MaterialTheme.colorScheme.onSurfaceVariant, style = MaterialTheme.typography.bodySmall)
        } else {
            FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                options.forEach { option ->
                    FilterChip(
                        selected = option == value,
                        onClick = { onSelected(option) },
                        label = { Text(option.replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }) },
                    )
                }
            }
        }
    }
}

private fun formatNumber(value: Double?): String = value?.let {
    val text = String.format("%.4f", it)
    text.trimEnd('0').trimEnd('.')
} ?: "-"

private fun formatSigned(value: Double?): String = value?.let {
    val prefix = if (it > 0) "+" else ""
    prefix + formatNumber(it)
} ?: "-"

private fun formatPercent(value: Double?): String = value?.let { "${formatNumber(it)}%" } ?: "-"

private fun formatMinutes(value: Double?): String {
    if (value == null) return "-"
    if (value < 0) return "passed"
    val totalMinutes = value.toInt()
    val hours = totalMinutes / 60
    val minutes = totalMinutes % 60
    return "${hours}h ${minutes}m"
}
