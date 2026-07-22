package com.feearb.mobile

import java.util.Locale

internal fun String.toInputDoubleOrNull(): Double? {
    val normalized = trim().replace(',', '.')
    if (normalized.isEmpty()) return null
    return normalized.toDoubleOrNull()
}

internal fun formatInputNumber(value: Double): String {
    val text = String.format(Locale.US, "%.4f", value)
    return text.trimEnd('0').trimEnd('.')
}
