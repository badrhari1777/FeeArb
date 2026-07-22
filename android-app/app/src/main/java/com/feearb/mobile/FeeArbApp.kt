@file:OptIn(
    androidx.compose.foundation.layout.ExperimentalLayoutApi::class,
    androidx.compose.material3.ExperimentalMaterial3Api::class,
)

package com.feearb.mobile

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.text.KeyboardOptions
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
import androidx.compose.material.icons.outlined.AccountBalanceWallet
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
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import com.feearb.mobile.ui.theme.FeeArbTheme
import kotlin.math.absoluteValue

private enum class AppTab(val title: String) {
    Balances("Balances"),
    Positions("Positions"),
    Manual("Manual"),
    Grid("Grid"),
    Settings("Settings"),
}

@Composable
fun FeeArbApp(viewModel: MobileViewModel) {
    FeeArbTheme {
        var currentTab by remember { mutableStateOf(AppTab.Balances) }
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
        state.positionActionConfirmationText?.let { confirmationText ->
            AlertDialog(
                onDismissRequest = viewModel::cancelPositionAction,
                title = { Text("Confirm position action") },
                text = { Text(confirmationText) },
                confirmButton = {
                    Button(
                        onClick = viewModel::confirmPositionAction,
                        enabled = !state.positionActionLoading,
                    ) {
                        Text(if (state.positionActionLoading) "Submitting..." else "Execute")
                    }
                },
                dismissButton = {
                    TextButton(
                        onClick = viewModel::cancelPositionAction,
                        enabled = !state.positionActionLoading,
                    ) {
                        Text("Cancel")
                    }
                },
            )
        }
        state.gridConfirmationText?.let { confirmationText ->
            AlertDialog(
                onDismissRequest = viewModel::cancelGridStart,
                title = { Text("Confirm Live Grid") },
                text = { Text(confirmationText) },
                confirmButton = {
                    Button(onClick = viewModel::confirmGridStart, enabled = !state.gridLoading) {
                        Text(if (state.gridLoading) "Starting..." else "Start Live")
                    }
                },
                dismissButton = {
                    TextButton(onClick = viewModel::cancelGridStart, enabled = !state.gridLoading) {
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
                            Text(currentTab.title)
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
                            if (currentTab == AppTab.Manual || currentTab == AppTab.Settings) {
                                viewModel.refreshAll()
                            } else {
                                viewModel.refreshPositions()
                            }
                        }) {
                            Text("Refresh")
                        }
                    },
                )
            },
            bottomBar = {
                NavigationBar {
                    NavigationBarItem(
                        selected = currentTab == AppTab.Balances,
                        onClick = { currentTab = AppTab.Balances },
                        icon = { Icon(Icons.Outlined.AccountBalanceWallet, contentDescription = null) },
                        label = { Text("Balances") },
                    )
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
                        selected = currentTab == AppTab.Grid,
                        onClick = { currentTab = AppTab.Grid },
                        icon = { Icon(Icons.AutoMirrored.Outlined.ListAlt, contentDescription = null) },
                        label = { Text("Grid") },
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
                AppTab.Balances -> BalancesScreen(
                    balances = state.positionsResponse.balances,
                    loading = state.positionsLoading,
                    errorText = state.positionsErrorText,
                    lastUpdated = state.positionsResponse.account_last_updated ?: state.positionsResponse.last_updated,
                    modifier = Modifier.padding(padding),
                )
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
                    onUseInManual = {
                        viewModel.prefillManualFromPosition(it)
                        currentTab = AppTab.Manual
                    },
                    onUseInGrid = {
                        viewModel.prefillGridFromPosition(it)
                        currentTab = AppTab.Grid
                    },
                    onPositionAction = viewModel::startPositionAction,
                    onSaveAutoExit = viewModel::saveAutoExit,
                    actionLoading = state.positionActionLoading,
                    modifier = Modifier.padding(padding),
                )
                AppTab.Manual -> ManualScreen(
                    state = state,
                    onFormChange = viewModel::updateManualForm,
                    onAdvancedChange = viewModel::updateAdvancedSettings,
                    onAnalyze = viewModel::analyzeManual,
                    onExecute = viewModel::executeManual,
                    onStop = viewModel::stopExecution,
                    onRefreshSpread = viewModel::refreshManualSpread,
                    onReloadDefaults = viewModel::loadManualDefaults,
                    modifier = Modifier.padding(padding),
                )
                AppTab.Grid -> GridScreen(
                    state = state,
                    onFormChange = viewModel::updateGridForm,
                    onAnalyze = viewModel::analyzeGrid,
                    onStart = viewModel::startGrid,
                    onRefresh = viewModel::refreshGridStatus,
                    modifier = Modifier.padding(padding),
                )
                AppTab.Settings -> SettingsScreen(
                    state = state,
                    onBaseUrlChange = viewModel::updateBaseUrl,
                    onRemoteAccessTokenChange = viewModel::updateRemoteAccessToken,
                    onApplyConnectionSettings = viewModel::applyConnectionSettings,
                    onAdvancedChange = viewModel::updateAdvancedSettings,
                    modifier = Modifier.padding(padding),
                )
            }
        }
    }
}

@Composable
private fun BalancesScreen(
    balances: List<BalanceDto>,
    loading: Boolean,
    errorText: String?,
    lastUpdated: String?,
    modifier: Modifier = Modifier,
) {
    val totalBalance = balances.mapNotNull { it.total }.sum()
    val availableBalance = balances.mapNotNull { it.available }.sum()
    val usedBalance = balances.mapNotNull { it.used }.sum()
    val assets = balances.mapNotNull { it.asset?.uppercase() }.distinct()
    val totalLabel = if (assets.size == 1) "Total ${assets.first()}" else "Total balance"
    val healthyCount = balances.count { it.status == "ok" }

    LazyColumn(
        modifier = modifier.fillMaxSize(),
        contentPadding = PaddingValues(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        item {
            Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f))) {
                Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
                    Text(totalLabel, style = MaterialTheme.typography.labelLarge, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    Text(
                        formatMoney(totalBalance),
                        style = MaterialTheme.typography.headlineMedium,
                        fontWeight = FontWeight.SemiBold,
                        color = MaterialTheme.colorScheme.primary,
                    )
                    Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
                        MetricBlock("Available", formatMoney(availableBalance), Modifier.weight(1f))
                        MetricBlock("Used", formatMoney(usedBalance), Modifier.weight(1f))
                    }
                    KeyValue("Exchanges reporting", "${balances.size}")
                    KeyValue("Healthy", "$healthyCount / ${balances.size}")
                    if (!lastUpdated.isNullOrBlank()) {
                        Text(
                            "Updated: $lastUpdated",
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                }
            }
        }
        if (loading) {
            item {
                StateCard(
                    title = "Refreshing balances",
                    body = "Fetching current exchange balances from backend.",
                    loading = true,
                )
            }
        }
        if (!errorText.isNullOrBlank()) {
            item { StateCard(title = "Balance refresh error", body = errorText) }
        }
        if (balances.isEmpty() && !loading) {
            item {
                StateCard(
                    title = "No balances",
                    body = "Backend returned no exchange balances.",
                )
            }
        }
        items(balances, key = { "${it.exchange}-${it.asset}" }) { balance ->
            BalanceCard(balance)
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
    onUseInManual: (PositionCardDto) -> Unit,
    onUseInGrid: (PositionCardDto) -> Unit,
    onPositionAction: (PositionCardDto, String, String) -> Unit,
    onSaveAutoExit: (PositionCardDto, Boolean, String, String) -> Unit,
    actionLoading: Boolean,
    modifier: Modifier = Modifier,
) {
    var filtersExpanded by remember { mutableStateOf(false) }
    LazyColumn(
        modifier = modifier.fillMaxSize(),
        contentPadding = PaddingValues(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        item {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Text(
                        "${cards.size} positions · ${selectedFilter.label} · ${selectedSort.label}",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    TextButton(onClick = { filtersExpanded = !filtersExpanded }) {
                        Text(if (filtersExpanded) "Hide filters" else "Filters")
                    }
                }
                AnimatedVisibility(filtersExpanded) {
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
            PositionCard(
                card = card,
                actionLoading = actionLoading,
                onUseInManual = { onUseInManual(card) },
                onUseInGrid = { onUseInGrid(card) },
                onPositionAction = { action, percent -> onPositionAction(card, action, percent) },
                onSaveAutoExit = { enabled, target, percent ->
                    onSaveAutoExit(card, enabled, target, percent)
                },
            )
        }
    }
}

@Composable
private fun BalanceCard(balance: BalanceDto) {
    Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.35f))) {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
        Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween, verticalAlignment = Alignment.CenterVertically) {
            Column {
                Text(balance.exchange.uppercase(), style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                Text(balance.asset ?: "USDT", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
            StatusPill(balance.status ?: "unknown")
        }
        Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
            MetricBlock("Total", formatMoney(balance.total), Modifier.weight(1f))
            MetricBlock("Available", formatMoney(balance.available), Modifier.weight(1f))
        }
        MetricBlock("Used", formatMoney(balance.used))
        KeyValue("Margin", formatRatio(balance.margin_ratio))
        if (!balance.error.isNullOrBlank()) {
            Text(
                balance.error,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.error,
            )
        }
        if (!balance.updated_at.isNullOrBlank()) {
            Text(
                "Updated: ${balance.updated_at}",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        }
    }
}

@Composable
private fun PositionCard(
    card: PositionCardDto,
    actionLoading: Boolean,
    onUseInManual: () -> Unit,
    onUseInGrid: () -> Unit,
    onPositionAction: (String, String) -> Unit,
    onSaveAutoExit: (Boolean, String, String) -> Unit,
) {
    var expanded by remember(card.symbol, card.pair_label) { mutableStateOf(false) }
    var actionPercent by remember(card.symbol, card.pair_label) { mutableStateOf("100") }
    var autoExitEnabled by remember(card.symbol, card.auto_exit.spread_enabled) {
        mutableStateOf(card.auto_exit.spread_enabled)
    }
    var autoExitTarget by remember(card.symbol, card.auto_exit.target_spread_pct) {
        mutableStateOf(card.auto_exit.target_spread_pct?.let(::formatNumber).orEmpty())
    }
    var autoExitPercent by remember(card.symbol, card.auto_exit.exit_percent) {
        mutableStateOf(formatNumber(card.auto_exit.exit_percent ?: 100.0))
    }
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.35f)),
    ) {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.SpaceBetween,
            ) {
                Column {
                    Text(card.symbol, style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                    Text(card.pair_label, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
                RiskChip(card.liq_distance_pct, card.risk_level)
            }
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
                MetricBlock("Hedged qty", formatNumber(card.position_summary.hedged_quantity), Modifier.weight(1f))
                MetricBlock("Net PnL", formatSigned(card.net_pnl), Modifier.weight(1f))
                MetricBlock("Exp. funding", formatSigned(card.expected_funding), Modifier.weight(1f))
            }
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
                MetricBlock("Enter spread", formatPercent(card.position_summary.pair_entry_spread_pct), Modifier.weight(1f))
                MetricBlock("Next funding", formatMinutes(card.minutes_to_next_funding), Modifier.weight(1f))
                MetricBlock("Liq dist", formatPercent(card.liq_distance_pct), Modifier.weight(1f))
            }
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
                MetricBlock("Live spread", formatPercent(card.live_spread_pct), Modifier.weight(1f))
                MetricBlock("Long qty", formatNumber(card.position_summary.long_quantity), Modifier.weight(1f))
                MetricBlock("Short qty", formatNumber(card.position_summary.short_quantity), Modifier.weight(1f))
            }
            HorizontalDivider()
            Text("Position size", style = MaterialTheme.typography.labelLarge)
            FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                listOf("25", "50", "75", "100").forEach { value ->
                    FilterChip(
                        selected = actionPercent == value,
                        onClick = { actionPercent = value },
                        label = { Text("$value%") },
                    )
                }
            }
            OutlinedTextField(
                value = actionPercent,
                onValueChange = { actionPercent = it },
                label = { Text("Custom percent") },
                suffix = { Text("%") },
                modifier = Modifier.fillMaxWidth(),
                singleLine = true,
            )
            Row(horizontalArrangement = Arrangement.spacedBy(10.dp), modifier = Modifier.fillMaxWidth()) {
                Button(
                    onClick = { onPositionAction("add", actionPercent) },
                    enabled = !actionLoading,
                    modifier = Modifier.weight(1f),
                ) {
                    Text("Add $actionPercent%")
                }
                Button(
                    onClick = { onPositionAction("exit", actionPercent) },
                    enabled = !actionLoading,
                    modifier = Modifier.weight(1f),
                ) {
                    Text("Exit $actionPercent%")
                }
            }
            HorizontalDivider()
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.SpaceBetween,
            ) {
                Column {
                    Text("Auto exit", style = MaterialTheme.typography.labelLarge)
                    Text(
                        "${card.auto_exit.status ?: "off"} · one time",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                Switch(checked = autoExitEnabled, onCheckedChange = { autoExitEnabled = it })
            }
            Row(horizontalArrangement = Arrangement.spacedBy(10.dp), modifier = Modifier.fillMaxWidth()) {
                OutlinedTextField(
                    value = autoExitTarget,
                    onValueChange = { autoExitTarget = it },
                    label = { Text("Spread target") },
                    suffix = { Text("%") },
                    modifier = Modifier.weight(1f),
                    singleLine = true,
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal),
                )
                OutlinedTextField(
                    value = autoExitPercent,
                    onValueChange = { autoExitPercent = it },
                    label = { Text("Exit size") },
                    suffix = { Text("%") },
                    modifier = Modifier.weight(1f),
                    singleLine = true,
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal),
                )
            }
            FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                listOf("25", "50", "75", "100").forEach { value ->
                    AssistChip(
                        onClick = { autoExitPercent = value },
                        label = { Text("$value%") },
                    )
                }
                Button(onClick = { onSaveAutoExit(autoExitEnabled, autoExitTarget, autoExitPercent) }) {
                    Text("Save auto exit")
                }
            }
            Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                TextButton(onClick = onUseInManual) { Text("Manual setup") }
                TextButton(onClick = onUseInGrid) { Text("Grid setup") }
                TextButton(onClick = { expanded = !expanded }) { Text(if (expanded) "Hide details" else "Details") }
            }
            AnimatedVisibility(expanded) {
                Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                    SectionCard("Position Summary") {
                        KeyValue("Quantity", formatNumber(card.position_summary.quantity))
                        KeyValue("Hedged quantity", formatNumber(card.position_summary.hedged_quantity))
                        KeyValue("Long quantity", formatNumber(card.position_summary.long_quantity))
                        KeyValue("Short quantity", formatNumber(card.position_summary.short_quantity))
                        KeyValue("Imbalance quantity", formatNumber(card.position_summary.imbalance_quantity))
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
    onAdvancedChange: ((AdvancedSettingsUiState) -> AdvancedSettingsUiState) -> Unit,
    onAnalyze: () -> Unit,
    onExecute: () -> Unit,
    onStop: () -> Unit,
    onRefreshSpread: () -> Unit,
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
                OutlinedTextField(value = state.manualForm.notional, onValueChange = { value -> onFormChange { it.copy(notional = value) } }, label = { Text("Notional USDT") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = state.advancedSettings.chunkQty, onValueChange = { value -> onAdvancedChange { it.copy(chunkQty = value) } }, label = { Text("Chunk qty") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = state.advancedSettings.chunkNotional, onValueChange = { value -> onAdvancedChange { it.copy(chunkNotional = value) } }, label = { Text("Chunk notional USDT") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                if (state.manualForm.action == "roll") {
                    SimpleSelect(
                        "Spread condition",
                        if (state.manualForm.rollTriggerOperator == "gte") "\u2265" else "\u2264",
                        listOf("\u2264", "\u2265"),
                    ) { selected ->
                        onFormChange { it.copy(rollTriggerOperator = if (selected == "\u2265") "gte" else "lte") }
                    }
                }
                OutlinedTextField(
                    value = state.manualForm.triggerSpreadPct,
                    onValueChange = { value -> onFormChange { it.copy(triggerSpreadPct = value) } },
                    label = {
                        Text(
                            when (state.manualForm.action) {
                                "enter" -> "Enter when spread \u2264"
                                "exit" -> "Exit when spread \u2265"
                                else -> "Roll trigger spread"
                            }
                        )
                    },
                    suffix = { Text("%") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                )
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
                Text("Spread Preview", style = MaterialTheme.typography.titleSmall, fontWeight = FontWeight.SemiBold)
                MonoBlock(state.manualSpreadText)
                Button(onClick = onRefreshSpread, enabled = manualReady && !state.manualSpreadLoading) {
                    Text(if (state.manualSpreadLoading) "Loading..." else "Refresh spread")
                }
                KeyValue("Max slippage", "${state.advancedSettings.maxSlippageBps.ifBlank { "-" }} bps")
                KeyValue("Margin mode", state.advancedSettings.marginMode.ifBlank { "-" })
                KeyValue(
                    "Execution time",
                    if (state.advancedSettings.untilFilled) {
                        "Until filled (max 30 min)"
                    } else {
                        "${state.advancedSettings.maxRuntimeMinutes.ifBlank { "default" }} min"
                    },
                )
                Spacer(Modifier.height(10.dp))
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = onAnalyze, enabled = manualReady && !state.manualLoading) { Text("Dry Run") }
                    Button(
                        onClick = onExecute,
                        enabled = manualReady && !state.manualLoading && state.executionStatus != "running",
                    ) { Text("Execute") }
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
private fun GridScreen(
    state: MobileUiState,
    onFormChange: ((GridFormUiState) -> GridFormUiState) -> Unit,
    onAnalyze: () -> Unit,
    onStart: () -> Unit,
    onRefresh: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val defaults = state.manualDefaults
    val exchanges = defaults?.exchanges.orEmpty()
    val ready = defaults != null && exchanges.isNotEmpty()
    val setupLabels = listOf("New grid", "Adopt full grid")
    val currentSetupLabel = when (state.gridForm.setupMode) {
        "adopt_existing_full_grid" -> "Adopt full grid"
        else -> "New grid"
    }
    LazyColumn(
        modifier = modifier.fillMaxSize(),
        contentPadding = PaddingValues(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        item {
            SectionCard("Live Grid") {
                SimpleSelect("Setup", currentSetupLabel, setupLabels) { selected ->
                    onFormChange {
                        it.copy(
                            setupMode = if (selected == "Adopt full grid") {
                                "adopt_existing_full_grid"
                            } else {
                                "entry_range"
                            }
                        )
                    }
                }
                OutlinedTextField(
                    value = state.gridForm.symbol,
                    onValueChange = { value -> onFormChange { it.copy(symbol = value) } },
                    label = { Text("Symbol") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                )
                SimpleSelect("Long exchange", state.gridForm.longExchange, exchanges) { selected ->
                    onFormChange { it.copy(longExchange = selected) }
                }
                SimpleSelect("Short exchange", state.gridForm.shortExchange, exchanges) { selected ->
                    onFormChange { it.copy(shortExchange = selected) }
                }
                OutlinedTextField(
                    value = state.gridForm.maxNotional,
                    onValueChange = { value -> onFormChange { it.copy(maxNotional = value) } },
                    label = { Text("Full budget") },
                    suffix = { Text("USDT") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                )
                Row(horizontalArrangement = Arrangement.spacedBy(10.dp), modifier = Modifier.fillMaxWidth()) {
                    OutlinedTextField(
                        value = state.gridForm.rangeStartPct,
                        onValueChange = { value -> onFormChange { it.copy(rangeStartPct = value) } },
                        label = { Text(if (state.gridForm.setupMode == "adopt_existing_full_grid") "Exit high" else "Entry start") },
                        suffix = { Text("%") },
                        modifier = Modifier.weight(1f),
                        singleLine = true,
                    )
                    OutlinedTextField(
                        value = state.gridForm.rangeEndPct,
                        onValueChange = { value -> onFormChange { it.copy(rangeEndPct = value) } },
                        label = { Text(if (state.gridForm.setupMode == "adopt_existing_full_grid") "Exit low" else "Entry end") },
                        suffix = { Text("%") },
                        modifier = Modifier.weight(1f),
                        singleLine = true,
                    )
                }
                OutlinedTextField(
                    value = state.gridForm.levelCount,
                    onValueChange = { value -> onFormChange { it.copy(levelCount = value) } },
                    label = { Text("Levels") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                )
                KeyValue("Max slippage", "${state.advancedSettings.maxSlippageBps.ifBlank { "8" }} bps")
                KeyValue("Confirm samples", "2")
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = onAnalyze, enabled = ready && !state.gridLoading) {
                        Text("Dry Run")
                    }
                    Button(onClick = onStart, enabled = ready && !state.gridLoading) {
                        Text("Start Live Grid")
                    }
                    TextButton(onClick = onRefresh, enabled = !state.gridLoading) {
                        Text("Refresh")
                    }
                }
                if (state.gridLoading) {
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        CircularProgressIndicator(modifier = Modifier.width(18.dp), strokeWidth = 2.dp)
                        Text("Working...", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    }
                }
                Text(state.gridStatusText, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
        item {
            SectionCard("Preview") {
                MonoBlock(state.gridPlanText)
            }
        }
        item {
            SectionCard("Active Grid Rules") {
                MonoBlock(state.gridRulesText)
            }
        }
    }
}

@Composable
private fun SettingsScreen(
    state: MobileUiState,
    onBaseUrlChange: (String) -> Unit,
    onRemoteAccessTokenChange: (String) -> Unit,
    onApplyConnectionSettings: () -> Unit,
    onAdvancedChange: ((AdvancedSettingsUiState) -> AdvancedSettingsUiState) -> Unit,
    modifier: Modifier = Modifier,
) {
    var executionTimingExpanded by remember { mutableStateOf(false) }
    LazyColumn(modifier = modifier.fillMaxSize(), contentPadding = PaddingValues(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        item {
            SectionCard("Connection") {
                OutlinedTextField(value = state.baseUrl, onValueChange = onBaseUrlChange, label = { Text("Backend base URL") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                OutlinedTextField(
                    value = state.remoteAccessToken,
                    onValueChange = onRemoteAccessTokenChange,
                    label = { Text("Remote access token") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    visualTransformation = PasswordVisualTransformation(),
                )
                Text(
                    "Leave the token empty for local or Tailscale access.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                Spacer(Modifier.height(8.dp))
                Button(onClick = onApplyConnectionSettings) { Text("Apply") }
            }
        }
        item {
            SectionCard("Execution timing") {
                KeyValue(
                    "Current limit",
                    if (state.advancedSettings.untilFilled) {
                        "Until filled, max 30 min"
                    } else {
                        "${state.advancedSettings.maxRuntimeMinutes.ifBlank { "backend default" }} min"
                    },
                )
                TextButton(onClick = { executionTimingExpanded = !executionTimingExpanded }) {
                    Text(if (executionTimingExpanded) "Hide execution settings" else "Execution settings")
                }
                AnimatedVisibility(executionTimingExpanded) {
                    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        SettingsField(
                            "Execution time, minutes (1-30)",
                            state.advancedSettings.maxRuntimeMinutes,
                        ) { value ->
                            onAdvancedChange { it.copy(maxRuntimeMinutes = value.filter(Char::isDigit)) }
                        }
                        BooleanSetting(
                            "Until filled (max 30 min)",
                            state.advancedSettings.untilFilled,
                        ) { value ->
                            onAdvancedChange { it.copy(untilFilled = value) }
                        }
                        Text(
                            "Smart execution keeps retrying while quantity remains. The checkbox uses a hard 30-minute ceiling; Stop remains available at any time.",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                }
            }
        }
        item {
            SectionCard("Trading Safety") {
                SettingsField("Max slippage bps", state.advancedSettings.maxSlippageBps) { value -> onAdvancedChange { it.copy(maxSlippageBps = value) } }
                SimpleSelect("Margin mode", state.advancedSettings.marginMode, listOf("isolated", "cross")) { selected -> onAdvancedChange { it.copy(marginMode = selected) } }
                Spacer(Modifier.height(8.dp))
                BooleanSetting("Use orderbook check", state.advancedSettings.useOrderbookCheck) { value -> onAdvancedChange { it.copy(useOrderbookCheck = value) } }
                BooleanSetting("Exit allow flip", state.advancedSettings.exitAllowFlip) { value -> onAdvancedChange { it.copy(exitAllowFlip = value) } }
                BooleanSetting("Force chunk qty", state.advancedSettings.forceChunkQty) { value -> onAdvancedChange { it.copy(forceChunkQty = value) } }
            }
        }
        item {
            SectionCard("System") {
                Text("Reprice controls, hedge offsets, and WS controls stay on the backend in this mobile build.", color = MaterialTheme.colorScheme.onSurfaceVariant)
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
        "ok" -> Color(0xFF2E7D32)
        "watch" -> Color(0xFFE67E22)
        "stress" -> Color(0xFFB91C1C)
        "error" -> Color(0xFFB91C1C)
        "partial" -> Color(0xFFE67E22)
        "unavailable" -> Color(0xFFB91C1C)
        "missing_credentials" -> Color(0xFFB91C1C)
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

private fun formatNumber(value: Double?): String = value?.let(::formatInputNumber) ?: "-"

private fun formatMoney(value: Double?): String = value?.let {
    String.format("%,.2f", it)
} ?: "-"

private fun formatMoney(value: Double): String = String.format("%,.2f", value)

private fun formatSigned(value: Double?): String = value?.let {
    val prefix = if (it > 0) "+" else ""
    prefix + formatNumber(it)
} ?: "-"

private fun formatPercent(value: Double?): String = value?.let { "${formatNumber(it)}%" } ?: "-"

private fun formatRatio(value: Double?): String = value?.let { "${formatNumber(it * 100.0)}%" } ?: "-"

private fun formatMinutes(value: Double?): String {
    if (value == null) return "-"
    if (value < 0) return "passed"
    val totalMinutes = value.toInt()
    val hours = totalMinutes / 60
    val minutes = totalMinutes % 60
    return "${hours}h ${minutes}m"
}
