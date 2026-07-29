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

- main hedged cards, legs, spread, liquidation distance, stop/take coverage,
  and auto-exit state;
- Pump balance and reserve, arm/monitor state, every open Pump position,
  ladder legs, TP, catastrophic SL, liquidation buffer, tracked top-up, and
  remaining hold time;
- the latest Pump durable protection events.

Trading and emergency controls stay on their owning pages:

- main execution: `/manual` and `/strategies`;
- Pump arm/disarm/emergency controls: `/pump-short-strategies`.

This separation prevents a control intended for one account from being applied
accidentally to another.

## Data and Safety Rules

- Exchange-side position data remains authoritative.
- Main and Pump freshness ages are shown independently.
- Missing TP/SL visibility is a protection issue, not silently treated as OK.
- Pump API credentials and key identity/preflight details are not included in
  the unified endpoint.
- The detailed page refreshes every 15 seconds and has an explicit refresh
  button.
