package com.feearb.mobile

internal const val MIN_EXECUTION_RUNTIME_MINUTES = 1
internal const val DEFAULT_EXECUTION_RUNTIME_MINUTES = 5
internal const val MAX_EXECUTION_RUNTIME_MINUTES = 10
internal const val EXECUTION_TIMING_POLICY_VERSION = 2

internal fun resolveExecutionRuntimeMinutes(
    savedMinutes: String?,
    legacySeconds: String?,
    backendDefaultSeconds: Int?,
    previousPolicyVersion: Int,
): String {
    val savedValue = savedMinutes?.trim()?.toIntOrNull()
    val legacyValue = legacySeconds
        ?.trim()
        ?.toIntOrNull()
        ?.let { seconds -> (seconds.coerceAtLeast(1) + 59) / 60 }
    val backendValue = backendDefaultSeconds
        ?.let { seconds -> (seconds.coerceAtLeast(1) + 59) / 60 }
    var resolved = savedValue
        ?: legacyValue
        ?: backendValue
        ?: DEFAULT_EXECUTION_RUNTIME_MINUTES
    if (
        previousPolicyVersion < EXECUTION_TIMING_POLICY_VERSION &&
        resolved == MIN_EXECUTION_RUNTIME_MINUTES
    ) {
        resolved = DEFAULT_EXECUTION_RUNTIME_MINUTES
    }
    return resolved.coerceIn(
        MIN_EXECUTION_RUNTIME_MINUTES,
        MAX_EXECUTION_RUNTIME_MINUTES,
    ).toString()
}

internal fun executionRuntimeSeconds(
    runtimeMinutes: String,
    untilFilled: Boolean,
): Int {
    if (untilFilled) {
        return MAX_EXECUTION_RUNTIME_MINUTES * 60
    }
    val minutes = runtimeMinutes.trim().toIntOrNull()
        ?: DEFAULT_EXECUTION_RUNTIME_MINUTES
    return minutes.coerceIn(
        MIN_EXECUTION_RUNTIME_MINUTES,
        MAX_EXECUTION_RUNTIME_MINUTES,
    ) * 60
}
