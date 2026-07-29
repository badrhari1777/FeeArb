# Pump Live Canary on a Separate Bybit Subaccount

This is the operator procedure for the first real Pump/Dump canary. The live
path is disabled by default and is isolated from the existing FeeArb Bybit
account.

## Current Live Limits

- Bybit mainnet Unified Trading subaccount.
- Total Pump capital: `$1000`.
- Deployable capital: `$700`.
- Protected local reserve: `$300`.
- Four hard strategy slots of `$175` isolated margin each.
- Operational entry cap is `4`; each position still has its own fixed `$175`
  isolated-margin slot.
- Leverage: `3x`.
- Live strategy: `main_pullback_tier` short only.
- Long, slow-pump, super-pump, clean-control, and cycle strategies remain
  paper/shadow only.
- No automatic transfer from the master account. The `$1000` is transferred
  manually before arming.

## 1. Create the Bybit Account and Key

1. Create a separate Bybit Unified Trading subaccount for Pump/Dump.
2. Transfer exactly `$1000 USDT` to it manually.
3. Confirm there are no positions and no ordinary open orders on it.
4. Create an API key from that subaccount:
   - read/write;
   - `ContractTrade: Order`;
   - `ContractTrade: Position`;
   - no Withdrawal;
   - no master-account transfer permission;
   - no Spot or Options permission.
5. Record the subaccount UID separately.

For the operator's dynamic IP, the first canary may use a key without an IP
binding. The read-only preflight shows Bybit's `deadlineDay` and `expiredAt`
fields so the key can be replaced before expiry. Bybit currently documents a
`90`-day lifetime for an unbound key, shortened to `7` days after the account
password is changed. Never paste the key or secret into chat, email, a
screenshot, or a tracked project file.

## 2. Fill the Local Ignored File

Edit:

```text
C:\Projects\FeeArb\config\pump_live.env
```

Fill only:

```text
BYBIT_PUMP_API_KEY=
BYBIT_PUMP_API_SECRET=
BYBIT_PUMP_SUB_UID=
```

Keep these first-canary values unchanged:

```text
BYBIT_PUMP_TESTNET=0
PUMP_LIVE_ENTRY_CAP=4
PUMP_LIVE_POLL_INTERVAL_SEC=15
PUMP_LIVE_MAX_SLIPPAGE_BPS=50
```

The file is excluded by `.gitignore`. The tracked template is
`config/pump_live.env.example`.

After the file is filled, restart the FeeArb backend once so the newly deployed
Pump live routes/controller are loaded. Do this as a supervised restart because
the same backend may be monitoring Grid or manual positions. Before restart,
verify there is no active execution; after restart, verify the main health
endpoints and confirm `GET /api/pump-short/live` returns `200`. Do not arm as
part of the restart.

## 3. Run the Read-Only Gate

Open `/pump-short-strategies` and use `Read-only preflight`.

The gate verifies:

- the API key belongs to a subaccount, not the master account;
- the configured UID matches the key UID;
- the key is read/write and has `Order` plus `Position`;
- the account is Unified Trading;
- equity is at least `$950` and available USDT is at least `$300`;
- the account has no existing positions or unowned opening orders;
- account margin mode is isolated;
- the current IP-binding/expiry state is visible.

If the only error is that account margin mode is not isolated, use
`Prepare isolated / one-way` and enter:

```text
PREPARE PUMP SUBACCOUNT
```

Then run the read-only preflight again. Do not arm while any other error is
present.

## 4. Start Signal Collection and Arm Four Slots

The existing paper/shadow schedule remains the signal source. Confirm it is
`running` or `waiting`; start it from the same page if needed.

To permit new real entries, use `Arm live` and enter:

```text
ARM PUMP LIVE 1000
```

Arming never adopts an old paper position. Only a new
`main_pullback_tier/entry_ready` decision produced after arming can be queued.
Every backend restart automatically disables new entries; existing Pump
positions continue in recovery monitoring.

## 5. What the First Position Does

- The first ladder leg is a guarded market short.
- Remaining legs are post-only short limits at the strategy's actual tier
  prices and weights.
- A full-position market take-profit and a catastrophic exchange-side
  stop-loss are synchronized after every quantity, average-entry, or
  liquidation-price change. The stop sits `2.5%` inside the current
  liquidation price and uses Mark Price, so it remains available if the
  backend or API monitor is temporarily unavailable.
- A time stop uses the tier's configured maximum hold.
- The monitor polls every `15` seconds.
- New entries are blocked on unknown positions/orders, insufficient reserve,
  execution uncertainty, or a monitor error.
- If a position is absent in one complete scan, remaining add orders are
  cancelled immediately and entries are disarmed. It is marked closed only
  after two consecutive flat scans.
- Liquidation distance is monitored. Margin is added from the subaccount
  reserve in capped `$25/$50` steps; per-position and portfolio top-up caps are
  enforced. Warning/panic/emergency buffers are `20% / 15% / 10%`.
- Every top-up is followed immediately by a fresh position read. If the buffer
  is still at or below `10%`, the bot does not wait for the five-minute top-up
  cooldown: it cancels adds and submits a reduce-only emergency close.
- Only margin previously added and recorded by Pump Live can be removed
  automatically. Removal starts after two consecutive scans at or above a
  `35%` buffer and at least 30 minutes after the previous margin adjustment,
  in `$25` chunks. The post-removal position is read immediately; a buffer
  below `30%` causes the same amount to be restored at once.
- If the emergency buffer is reached and no allowed reserve remains, the bot
  cancels its adds and submits a reduce-only market close. Normal orders use a
  `50 bps` depth guard; emergency exits allow up to `300 bps`.
- Pump Live uses the same configured primary/fallback notification router as
  the main FeeArb account monitor. Arm/disarm, live entry, margin top-up,
  blocked top-up, close submission, confirmed flat state, emergency close, and
  monitor errors are sent there. Delivery runs outside the protection cycle;
  a notification failure is audited but cannot block margin or exit logic.

## 6. Four-Slot Live Review

On 2026-07-29 the operator explicitly chose to test the complete four-slot
strategy immediately after a clean subaccount preflight, rather than wait for
automatic promotion from one slot. Review every first real case against:

1. Entry price and quantity match the live ledger.
2. Remaining ladder orders have the expected prices and quantities.
3. The Bybit full-position TP matches the current average entry.
4. A filled add causes TP recalculation.
5. Backend restart leaves new entries disarmed and resumes position monitoring.
6. Exit leaves no position and no Pump ladder orders.
7. `live_events.jsonl` contains no unresolved error.

There is still no automatic promotion or sizing growth. Any change above four
slots, above `$175` per slot, or any automated master/sub transfer requires a
new explicit operator decision.

## Emergency Controls

- `Disarm entries` stops new positions but keeps monitoring existing ones.
- `Emergency close all` requires:

```text
CLOSE ALL PUMP POSITIONS
```

It cancels Pump-owned opening orders and submits reduce-only market closes for
short positions on this dedicated subaccount.

Runtime evidence:

```text
data/research/bybit_pump_short_live/live_state.json
data/research/bybit_pump_short_live/live_events.jsonl
```

Exchange positions and orders remain authoritative; the local files are the
durable strategy ledger and audit trail. Notification attempts are appended as
`notification_delivery` rows and the most recent delivery status is persisted
in `live_state.json`.
