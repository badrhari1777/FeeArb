package com.feearb.mobile

import com.google.gson.GsonBuilder
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class PositionsOverviewModelsTest {
    private val gson = GsonBuilder().create()

    @Test
    fun parsesPumpOverviewWithoutTradingCredentials() {
        val payload = """
            {
              "schema": "positions_overview_v1",
              "generated_at_ms": 1785353561077,
              "summary": {
                "main_positions": 1,
                "pump_positions": 1,
                "pump_cap": 4,
                "total_unrealized_pnl_usd": 9.25,
                "min_liq_buffer_pct": 18.5,
                "protection_issues": 0
              },
              "main": {
                "status": "ready",
                "positions": [{
                  "symbol": "BTCUSDT",
                  "pair_label": "BYBIT / BINANCE",
                  "expected_funding": 0.42,
                  "position_summary": {
                    "current_exposure_usdt": 1282.14,
                    "gross_current_exposure_usdt": 2551.76,
                    "entry_exposure_usdt": 1638.44,
                    "gross_entry_exposure_usdt": 3360.53
                  },
                  "legs": [{
                    "exchange": "kucoin",
                    "current_notional": 1282.14,
                    "entry_notional": 1722.09,
                    "exchange_notional": 1722.09,
                    "current_mark_price": 0.16982,
                    "mark_price_source": "position",
                    "valuation_status": "current",
                    "expected_funding": 0.21
                  }]
                }]
              },
              "pump": {
                "status": "armed",
                "entry_armed": true,
                "monitor_thread_alive": true,
                "last_cycle_at_ms": 1785353560000,
                "config": {
                  "entry_cap": 4,
                  "reserve_usd": 300,
                  "max_position_topup_usd": 175
                },
                "balance": {
                  "total_usd": 1000,
                  "available_usd": 825,
                  "used_usd": 175,
                  "temporary_occupied_usd": 50
                },
                "capital_regime": {
                  "mode": "normal",
                  "prefund_floor_usd": 25,
                  "temporary_occupied_usd": 50
                },
                "auto_transfer": {
                  "enabled": true,
                  "main_wallet_floor_usd": 2000,
                  "daily_remaining_usd": 185
                },
                "notifications": {
                  "configured": true,
                  "last_status": "ok"
                },
                "positions": [{
                  "module": "pump_live",
                  "symbol": "DEXEUSDT",
                  "side": "short",
                  "status": "open",
                  "qty": 1000,
                  "avg_entry_price": 4.2,
                  "mark_price": 3.91,
                  "unrealized_pnl_usd": 18.4,
                  "liq_price": 5.61,
                  "liq_buffer_pct": 31.5,
                  "risk_level": "ok",
                  "tp_price": 2.94,
                  "stop_price": 5.47,
                  "margin_topup_usd": 25,
                  "margin_topup_cap_usd": 175,
                  "age_h": 18,
                  "max_hold_h": 336,
                  "remaining_hold_h": 318,
                  "legs_filled": 1,
                  "legs_open": 2,
                  "legs": [{
                    "step": 1,
                    "status": "filled",
                    "trigger_price": 4.2,
                    "filled_qty": 1000
                  }]
                }]
              }
            }
        """.trimIndent()

        val result = gson.fromJson(payload, PositionsOverviewResponse::class.java)

        assertEquals("positions_overview_v1", result.schema)
        assertEquals(1, result.summary.main_positions)
        assertEquals(1, result.summary.pump_positions)
        assertEquals(4, result.summary.pump_cap)
        assertTrue(result.pump.entry_armed)
        assertTrue(result.pump.monitor_thread_alive)
        assertEquals(1000.0, result.pump.balance.total_usd ?: 0.0, 0.0001)
        assertEquals(50.0, result.pump.balance.temporary_occupied_usd ?: 0.0, 0.0001)
        assertEquals("normal", result.pump.capital_regime.mode)
        assertTrue(result.pump.auto_transfer.enabled)
        assertEquals(185.0, result.pump.auto_transfer.daily_remaining_usd ?: 0.0, 0.0001)
        assertEquals(1282.14, result.main.positions.single().position_summary.current_exposure_usdt ?: 0.0, 0.0001)
        assertEquals(2551.76, result.main.positions.single().position_summary.gross_current_exposure_usdt ?: 0.0, 0.0001)
        assertEquals(1282.14, result.main.positions.single().legs.single().current_notional ?: 0.0, 0.0001)
        assertEquals("current", result.main.positions.single().legs.single().valuation_status)
        assertEquals("DEXEUSDT", result.pump.positions.single().symbol)
        assertEquals(31.5, result.pump.positions.single().liq_buffer_pct ?: 0.0, 0.0001)
        assertEquals("filled", result.pump.positions.single().legs.single().status)
    }

    @Test
    fun missingOptionalPumpFieldsRemainSafe() {
        val result = gson.fromJson(
            """{"summary":{},"main":{},"pump":{"status":"disarmed_after_restart"}}""",
            PositionsOverviewResponse::class.java,
        )

        assertEquals(0, result.summary.pump_positions)
        assertEquals(0, result.summary.pump_cap)
        assertFalse(result.pump.entry_armed)
        assertFalse(result.pump.monitor_thread_alive)
        assertTrue(result.pump.positions.isEmpty())
        assertTrue(result.pump.recent_events.isEmpty())
    }
}
