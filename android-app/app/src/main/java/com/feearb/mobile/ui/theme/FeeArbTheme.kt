package com.feearb.mobile.ui.theme

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color

private val FeeArbColors = darkColorScheme(
    primary = Color(0xFFDBA949),
    onPrimary = Color(0xFF1A1303),
    secondary = Color(0xFF8FC7B5),
    background = Color(0xFF0C1217),
    surface = Color(0xFF111A22),
    surfaceVariant = Color(0xFF1A2630),
    onSurface = Color(0xFFE7EDF2),
    onSurfaceVariant = Color(0xFF9EB0BF),
    error = Color(0xFFFF7A67),
)

@Composable
fun FeeArbTheme(content: @Composable () -> Unit) {
    MaterialTheme(
        colorScheme = FeeArbColors,
        content = content,
    )
}
