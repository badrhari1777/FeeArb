package com.feearb.mobile

const val POSITIONS_OVERVIEW_STALE_AFTER_SEC = 45.0
const val POSITIONS_OVERVIEW_POLL_INTERVAL_MS = 15_000L

enum class PositionModuleFilter(val label: String) {
    All("All"),
    Main("Main"),
    Pump("Pump"),
}

enum class OverviewRiskTone {
    Ok,
    Warn,
    High,
    Unknown,
}

internal fun PositionModuleFilter.showsMain(): Boolean =
    this == PositionModuleFilter.All || this == PositionModuleFilter.Main

internal fun PositionModuleFilter.showsPump(): Boolean =
    this == PositionModuleFilter.All || this == PositionModuleFilter.Pump

internal fun positionsOverviewIsStale(summary: PositionsOverviewSummaryDto): Boolean {
    val ages = listOfNotNull(summary.main_age_sec, summary.pump_age_sec)
    return ages.isEmpty() || ages.any { it > POSITIONS_OVERVIEW_STALE_AFTER_SEC }
}

internal fun overviewRiskTone(
    summary: PositionsOverviewSummaryDto,
    pump: PumpOverviewDto,
): OverviewRiskTone {
    if (
        summary.protection_issues > 0 ||
        summary.high_risk_positions > 0 ||
        !pump.last_error.isNullOrBlank()
    ) {
        return OverviewRiskTone.High
    }
    if (
        summary.warning_risk_positions > 0 ||
        positionsOverviewIsStale(summary) ||
        (pump.entry_armed && !pump.monitor_thread_alive)
    ) {
        return OverviewRiskTone.Warn
    }
    if (
        summary.main_age_sec == null ||
        summary.pump_age_sec == null
    ) {
        return OverviewRiskTone.Unknown
    }
    return OverviewRiskTone.Ok
}
