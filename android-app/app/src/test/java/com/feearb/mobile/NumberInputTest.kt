package com.feearb.mobile

import java.util.Locale
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class NumberInputTest {
    @Test
    fun executionTimingDefaultsAndMigratesToFiveMinutes() {
        assertEquals(
            "5",
            resolveExecutionRuntimeMinutes(
                savedMinutes = null,
                legacySeconds = null,
                backendDefaultSeconds = 300,
                previousPolicyVersion = EXECUTION_TIMING_POLICY_VERSION,
            ),
        )
        assertEquals(
            "5",
            resolveExecutionRuntimeMinutes(
                savedMinutes = "1",
                legacySeconds = null,
                backendDefaultSeconds = 300,
                previousPolicyVersion = 1,
            ),
        )
        assertEquals(
            "1",
            resolveExecutionRuntimeMinutes(
                savedMinutes = "1",
                legacySeconds = null,
                backendDefaultSeconds = 300,
                previousPolicyVersion = EXECUTION_TIMING_POLICY_VERSION,
            ),
        )
    }

    @Test
    fun executionTimingIsBoundedToTenMinutes() {
        assertEquals(
            "10",
            resolveExecutionRuntimeMinutes(
                savedMinutes = "30",
                legacySeconds = null,
                backendDefaultSeconds = 300,
                previousPolicyVersion = EXECUTION_TIMING_POLICY_VERSION,
            ),
        )
        assertEquals(300, executionRuntimeSeconds("5", untilFilled = false))
        assertEquals(600, executionRuntimeSeconds("1", untilFilled = true))
    }

    @Test
    fun parsesIntegerAndFractionalForms() {
        assertEquals(7.0, "7".toInputDoubleOrNull()!!, 0.0)
        assertEquals(7.0, "7.".toInputDoubleOrNull()!!, 0.0)
        assertEquals(7.1, "7.1".toInputDoubleOrNull()!!, 0.0)
        assertEquals(7.0, "7,".toInputDoubleOrNull()!!, 0.0)
        assertEquals(7.1, "7,1".toInputDoubleOrNull()!!, 0.0)
        assertEquals(7.1, " 7,1 ".toInputDoubleOrNull()!!, 0.0)
    }

    @Test
    fun rejectsEmptyAndMalformedValues() {
        assertNull("".toInputDoubleOrNull())
        assertNull("7.1.2".toInputDoubleOrNull())
        assertNull("seven".toInputDoubleOrNull())
    }

    @Test
    fun formatsInputIndependentlyOfDeviceLocale() {
        val originalLocale = Locale.getDefault()
        try {
            Locale.setDefault(Locale("ru", "RU"))
            assertEquals("100", formatInputNumber(100.0))
            assertEquals("7.1", formatInputNumber(7.1))
        } finally {
            Locale.setDefault(originalLocale)
        }
    }
}
