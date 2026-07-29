# Pump Live Canary on a Separate Bybit Subaccount

This is the operator procedure for the first real Pump/Dump canary. The live
path is disabled by default and is isolated from the existing FeeArb Bybit
account.

## Fixed First-Canary Limits

- Bybit mainnet Unified Trading subaccount.
- Total Pump capital: `$1000`.
- Deployable capital: `$700`.
- Protected local reserve: `$300`.
- Four hard strategy slots of `$175` isolated margin each.
- Operational entry cap starts at `1`; the code still sizes that position as
  one `$175` slot.
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
PUMP_LIVE_ENTRY_CAP=1
PUMP_LIVE_POLL_INTERVAL_SEC=15
PUMP_LIVE_MAX_SLIPPAGE_BPS=50
```

The file is excluded by `.gitignore`. The tracked template is
`config/pump_live.env.example`.

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

## 4. Start Signal Collection and Arm One Slot

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
- A full-position market take-profit is synchronized after every quantity or
  average-entry change.
- A time stop uses the tier's configured maximum hold.
- The monitor polls every `15` seconds.
- New entries are blocked on unknown positions/orders, insufficient reserve,
  execution uncertainty, or a monitor error.
- If a position is absent in one complete scan, remaining add orders are
  cancelled immediately and entries are disarmed. It is marked closed only
  after two consecutive flat scans.
- Liquidation distance is monitored. Margin is added from the subaccount
  reserve in capped `$25/$50` steps; per-position and portfolio top-up caps are
  enforced. If the emergency buffer is reached and no allowed reserve remains,
  the bot cancels its adds and submits a reduce-only market close. Normal
  orders use a `50 bps` depth guard; emergency exits allow up to `300 bps`.

## 6. First-Trade Review Gate

Do not raise `PUMP_LIVE_ENTRY_CAP` above `1` until one complete real case has
been checked:

1. Entry price and quantity match the live ledger.
2. Remaining ladder orders have the expected prices and quantities.
3. The Bybit full-position TP matches the current average entry.
4. A filled add causes TP recalculation.
5. Backend restart leaves new entries disarmed and resumes position monitoring.
6. Exit leaves no position and no Pump ladder orders.
7. `live_events.jsonl` contains no unresolved error.

After that review, raising the cap to `2`, and later to `4`, is a separate
operator decision. There is no automatic promotion.

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
durable strategy ledger and audit trail.
