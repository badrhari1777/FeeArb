package com.feearb.mobile

import java.util.Locale
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class NumberInputTest {
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
