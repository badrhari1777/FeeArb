package com.feearb.mobile

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class PositionsOverviewUiTest {
    @Test
    fun moduleFilterKeepsMainAndPumpSeparated() {
        assertTrue(PositionModuleFilter.All.showsMain())
        assertTrue(PositionModuleFilter.All.showsPump())
        assertTrue(PositionModuleFilter.Main.showsMain())
        assertFalse(PositionModuleFilter.Main.showsPump())
        assertFalse(PositionModuleFilter.Pump.showsMain())
        assertTrue(PositionModuleFilter.Pump.showsPump())
    }

    @Test
    fun staleOrStoppedArmedMonitorIsWarning() {
        val freshSummary = PositionsOverviewSummaryDto(main_age_sec = 5.0, pump_age_sec = 10.0)
        assertEquals(
            OverviewRiskTone.Warn,
            overviewRiskTone(
                freshSummary,
                PumpOverviewDto(entry_armed = true, monitor_thread_alive = false),
            ),
        )
        assertEquals(
            OverviewRiskTone.Warn,
            overviewRiskTone(
                freshSummary.copy(pump_age_sec = 46.0),
                PumpOverviewDto(entry_armed = false, monitor_thread_alive = false),
            ),
        )
    }

    @Test
    fun protectionIssueOrPumpErrorIsHighRisk() {
        val healthyPump = PumpOverviewDto(entry_armed = true, monitor_thread_alive = true)
        assertEquals(
            OverviewRiskTone.High,
            overviewRiskTone(
                PositionsOverviewSummaryDto(
                    main_age_sec = 5.0,
                    pump_age_sec = 5.0,
                    protection_issues = 1,
                ),
                healthyPump,
            ),
        )
        assertEquals(
            OverviewRiskTone.High,
            overviewRiskTone(
                PositionsOverviewSummaryDto(main_age_sec = 5.0, pump_age_sec = 5.0),
                healthyPump.copy(last_error = "monitor failed"),
            ),
        )
    }

    @Test
    fun freshProtectedOverviewIsOk() {
        assertEquals(
            OverviewRiskTone.Ok,
            overviewRiskTone(
                PositionsOverviewSummaryDto(main_age_sec = 5.0, pump_age_sec = 10.0),
                PumpOverviewDto(entry_armed = true, monitor_thread_alive = true),
            ),
        )
    }
}
