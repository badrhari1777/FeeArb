package com.feearb.mobile

internal fun balanceRowKey(balance: BalanceDto): String =
    listOf(
        balance.exchange.lowercase(),
        balance.account_alias?.lowercase() ?: "main",
        balance.asset?.uppercase() ?: "USDT",
    ).joinToString("-")

internal fun balanceTotal(
    summaryValue: Double?,
    balances: List<BalanceDto>,
    selector: (BalanceDto) -> Double?,
): Double = summaryValue ?: balances.mapNotNull(selector).sum()
