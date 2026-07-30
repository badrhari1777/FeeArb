package com.feearb.mobile

import org.junit.Assert.assertEquals
import org.junit.Test

class BalanceModelsTest {
    @Test
    fun balanceKeySeparatesBybitMainAndPumpAccounts() {
        val main = BalanceDto(exchange = "bybit", account_alias = "main", asset = "USDT")
        val pump = BalanceDto(exchange = "bybit", account_alias = "bybit_pump", asset = "USDT")

        assertEquals("bybit-main-USDT", balanceRowKey(main))
        assertEquals("bybit-bybit_pump-USDT", balanceRowKey(pump))
    }

    @Test
    fun balanceTotalUsesServerSummaryAndHasLegacyFallback() {
        val balances = listOf(
            BalanceDto(exchange = "bybit", total = 100.0),
            BalanceDto(exchange = "bybit", account_alias = "bybit_pump", total = 25.0),
        )

        assertEquals(125.0, balanceTotal(null, balances) { it.total }, 0.0001)
        assertEquals(130.0, balanceTotal(130.0, balances) { it.total }, 0.0001)
    }
}
