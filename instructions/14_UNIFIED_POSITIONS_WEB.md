# Unified Web Positions

The web dashboard exposes the same read-only position contract on two surfaces:

- the `Positions` panel on `/`, with `All positions`, `Main module`, and
  `Pump Live` tabs;
- the detailed `/positions` control center.

Both use:

```text
GET /api/positions/overview
```

The endpoint deliberately keeps unlike rows separate. `main.positions`
contains hedged strategy cards and their exchange legs. `pump.positions`
contains one-sided Pump Live shorts from the dedicated `bybit_pump`
subaccount. The summary combines counts, unrealized PnL, minimum liquidation
buffer, risk counts, source age, and incomplete protection visibility.

## Main Dashboard

`All positions` is the default and shows the existing main table followed by a
compact Pump table. The module tabs only change visibility; they never change
orders, strategy state, or account settings. The `Detailed positions` link
opens `/positions`.

## Detailed Control Center

`/positions` is intentionally read-only. It shows:

- main hedged cards, legs, spread, liquidation distance and stop/take coverage;
- Pump balance and reserve, arm/monitor state, every open Pump position,
  ladder legs, TP, catastrophic SL, liquidation buffer, tracked top-up, and
  remaining hold time;
- the latest Pump durable protection events.

Trading and emergency controls stay on their owning pages:

- main execution: `/manual`; Grid control: `/auto-arbitrage`;
- Pump arm/disarm/emergency controls: `/pump-short-strategies`.

This separation prevents a control intended for one account from being applied
accidentally to another.

## Data and Safety Rules

- Exchange-side position data remains authoritative.
- Main-position dollar valuation is exchange-neutral on every supported venue
  (Binance, Bybit, KuCoin, OKX, Gate, Bitget, MEXC, and BingX):
  `current_notional = abs(base_coin_quantity) * current_mark_price`.
- Native exchange fields such as `notional`, `posCost`, and `value` are not
  interchangeable. They remain available as `exchange_notional` for
  diagnostics but are never presented as the current position value.
- `entry_notional` is calculated separately from the entry price. The public
  `amount` field remains as a compatibility alias for `current_notional`.
- Estimated next funding in USDT uses the same current notional. A positive
  value means expected receipt and a negative value means expected payment for
  that leg. When the venue interval is known, the rate is displayed together
  with its period (for example, `0.01% / 8h`).
- If no real Mark Price is available, current notional and estimated funding
  stay unavailable. Entry price is not silently presented as current value.
- Hedge balance continues to use base-coin quantities, not dollar notionals;
  differing venue prices must not create a false quantity rebalance.
- Main and Pump freshness ages are shown independently.
- Missing TP/SL visibility is a protection issue, not silently treated as OK.
- Pump API credentials and key identity/preflight details are not included in
  the unified endpoint.
- The detailed page refreshes every 15 seconds and has an explicit refresh
  button.

## Deployment checkpoint: 2026-08-10

Commit `b4151b0` was loaded by a supervised backend restart using the canonical
Windows stop script and the `FeeArb Public UI (Tailscale Funnel)` scheduled
task. Before restart there were no active Manual/Grid executions. Pump Live
recovered two owned positions (`1000RATSUSDT`, `BLUAIUSDT`) in monitoring mode
with entries disarmed, both TP/SL pairs visible, no monitor error, and three
clean 15-second cycles. Do not infer ARM from this deployment checkpoint; the
active `v2_3000` policy still requires explicit `ARM PUMP LIVE 3000`.

Fresh TUT validation after deployment showed current exposures near
`$1222.30 / $1230.73` while KuCoin's diagnostic entry-like native notional
remained near `$1722.09`. The signed next-funding estimates were approximately
`-$0.332 / +$1.264`, or `+$0.931` net for the next venue payments at that
snapshot.

The operator subsequently supplied the exact `ARM PUMP LIVE 3000`
confirmation. The ownership-aware resume preflight returned `ready=true` and
the controller entered `armed` under `v2_3000`. Three following monitor cycles
kept both positions and TP/SL intact, reported no error/block/risk freeze, held
the minimum Pump liquidation buffer near `76.39%`, and observed no pending or
new signal during that verification window.
