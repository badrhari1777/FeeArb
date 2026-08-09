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
- Reserve policy inside the protected `$300`: `$50` is guaranteed for each of
  the four positions (`$200` total), another `$75` is a shared emergency pool,
  and at least `$25` remains as the hard account floor. The configured
  `$175` per-position and `$275` portfolio top-up limits are emergency ceilings,
  not an amount that all four positions can consume simultaneously.
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
PUMP_LIVE_MARGIN_PREFUND_ENABLED=1
PUMP_LIVE_MARGIN_PREFUND_SAFETY_PCT=2.5
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
Every backend restart initially disables new entries; existing Pump positions
continue in recovery monitoring. `Arm live` may safely resume with tracked
positions only after the controller matches all exchange positions and opening
orders to its durable ledger and force-resynchronizes full TP/SL protection.
Unknown, missing, or degraded state remains blocked. A successful resume is
reported as a ready `tracked_positions_verified` preflight instead of leaving
the UI on the expected existing-position warning.

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
  execution uncertainty, or a hard monitor error. A transient network,
  timeout, or Windows state-file replacement error pauses entries fail-closed
  and automatically rearms them only after two consecutive healthy monitor
  cycles. Hard or unclassified errors remain sticky until operator review.
- Durable state writes use a unique temporary file plus bounded retry/backoff
  for transient Windows sharing violations; an interrupted writer cannot reuse
  another cycle's temporary path.
- If a position is absent in one complete scan, remaining add orders are
  cancelled immediately and entries are disarmed. It is marked closed only
  after two consecutive flat scans.
- Liquidation distance is monitored. Margin is added from the subaccount
  reserve in capped `$25/$50` steps; per-position and portfolio top-up caps are
  enforced. Warning/panic/emergency buffers are `20% / 15% / 10%`.
- Normal top-ups stop at the position's guaranteed `$50`. At or below the
  `15%` panic buffer, positions are processed from the smallest liquidation
  buffer upward and may use the shared `$75` emergency pool while preserving
  `$50` quotas for every other open position.
- A new slot is rejected if opening it would make the guaranteed rescue quota
  for all resulting positions unavailable.
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
5. Backend restart resumes position monitoring fail-closed; explicit `Arm live`
   succeeds only after owned-position/order validation and protection resync.
6. Exit leaves no position and no Pump ladder orders.
7. `live_events.jsonl` contains no unresolved error.

There is still no automatic promotion or sizing growth. Any change above four
slots, above `$175` per slot, or any automated master/sub transfer requires a
new explicit operator decision.

## Capital Observation Setting

Pump Live has a separate durable strategy-capital setting on
`/pump-short-strategies`. It is intentionally deployed in `observe` mode:

- the active live capital remains `$1000`;
- the active live slot remains `$175`;
- saving strategy capital never resizes an open position, an existing ladder,
  or a future live order in observe mode;
- the manager reads the exact Bybit USDT `walletBalance`, excluding unrealized
  PnL and avoiding the fluctuating USD conversion used by
  `totalWalletBalance`;
- the entered value is the amount currently eligible for future strategy
  sizing. It may be lower than the wallet when part of the account must remain
  excluded rescue cash, but it cannot exceed the current wallet;
- the difference between entered strategy capital and current wallet is
  persisted, so later realized balance changes remain visible without turning
  the excluded reserve into strategy capital;
- every change is written as `capital_declared` in `live_events.jsonl`.

The page shows active slot, calculated slot, and the maximum next slot after
the agreed `+25%` growth cap. Calculation uses the existing `70% deployable /
30% reserve / 4 slots` allocation, rounds down to `$5`, waits for `+10%`
capital before recommending growth, and recommends reduction after `-5%`.

Observation readiness requires at least `14` days and `10` newly closed live
trades. Even after those gates, application remains disabled until a separate
operator-approved implementation. A daily/event-driven activation policy is
therefore not yet capable of changing live size.

If capital is manually deposited during observation, first complete the
deposit, then save the exact portion that should count as strategy capital.
Leave emergency/rescue funds outside that value. The future automatic
master-to-sub transfer layer must update this exclusion itself so a rescue
transfer can never be mistaken for profit.

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

## BANK Margin Stress Reference

Reproduce the deterministic isolated-margin stress model with:

```text
.venv\Scripts\python.exe scripts\pump_live_margin_stress.py
```

Outputs are written under
`data/research/pump_live_margin_stress/` (`summary.json`,
`bank_margin_levels.csv`, `portfolio_capacity.csv`, and
`rise_scenarios.csv`).

For the captured BANK position (`1010` short, average `0.17180881`, Bybit
liquidation `0.22349`, exchange stop `0.2179`, second ladder `1350 @ 0.25766`),
the model reproduces liquidation at `0.223491`. No top-up, and even one `$25`
top-up, leaves the stop below the second ladder. The minimum extra isolated
margin that places the protected stop beyond `0.25766` is about `$42.24`;
therefore the executable policy needs two `$25` chunks (`$50`) before that
ladder can be reached safely. With `$50` added, the pre-fill liquidation/stop
are about `0.271762 / 0.264968`; after the second leg fills, the recalculated
position is about `2360` BANK at average `0.220919`, with liquidation/stop near
`0.308032 / 0.300331`.

This is a deterministic policy regression, not a promise that an exchange order
will fill before a stop in a price gap. The exchange-side Mark Price stop remains
the final protection for moves faster than the 15-second monitor.

## Historical Spike Research (Not Active Policy)

The offline report `docs/pump_spike_risk_research.md` analyzes the archived
Bybit universe from 2024. It found 849 market-wide hourly surge episodes of at
least 30%, including 428 wick-like episodes, but only one 30%+ active
15-minute burst among 39 reconstructed entries that pass the current main
filters. One main-gated super-pump case remains inside the 15-minute
warning-to-stop uncertainty bucket, and one case crossed the initial stop and
L2 in the same 15-minute candle.

That report originally proposed selective `$50` pre-funding only for
pump `>=250%` plus a separately gated L2. The approved policy below supersedes
the selective part: every tier now receives an actual-position-based entry
prefund, while the normal 2/3/5 strategy remains unchanged. Targeted 1-minute
evidence remains useful for measuring residual gap risk, not for changing the
strategy ladder.

## Entry Margin Prefund (Active Policy)

`docs/pump_live_margin_management_research.md` and
`scripts/pump_live_margin_stress.py` contain the supporting calculation. The
active margin-only policy keeps `$175` per slot and does not change the
strategy's tier, 2/3/5 ladder count, prices, weights, TP, or hold time.

After the actual L1 fill, Pump Live calculates the minimum added isolated
margin required to place the exchange stop `2.5%` above actual L2, rounds
upward to `$5`, adds it, reads the Bybit position again, and refreshes TP/SL.
Approximate current-tier amounts are `$30 / $50 / $25 / $50` for ordinary,
strong `80–100%`, strong `100–250%`, and super tiers. Actual fill, liquidation,
quantity, and L2 price drive the live calculation.

Verification allows at most `2%` of the requested clearance as calculation
tolerance. This is not `2%` of coin price: for the requested `2.5%` stop
clearance above L2, the minimum accepted clearance is `2.45%`. If the refreshed
Bybit liquidation remains below that boundary, Pump Live rereads the position
before changing margin. It may add at most three correction steps of `$5`, and
only after the preceding add produced a confirmed outward liquidation-price
move. Every step repeats the position, portfolio, other-position guarantee,
available-balance, and `$25` operating-floor limits. Missing/stale position
confirmation or exhausted capacity remains fail-closed.

The confirmed entry prefund is stored as a non-removable bot-margin floor.
Warning/panic top-ups continue above it; safe reduction may return only the
excess. If prefund or verification fails, L1 remains exchange-protected,
entries are disarmed, and remaining ladders are not submitted under uncertain
execution.

An explicit `ARM PUMP LIVE 1000` may recover only the exact durable failure
shape `opening_uncertain + target_unconfirmed +
pump_live_margin_prefund_target_unconfirmed`: L1 must be confirmed filled,
every later ladder must still be unsubmitted `planned`, exchange positions and
orders must belong to Pump Live, and all normal preflight errors except expected
tracked-position/order warnings must be absent. ARM then rechecks/adds bounded
prefund, refreshes full TP/SL, submits each still-planned ladder once, records
the recovery, and only then enables new entries. Any other degraded or unknown
state still blocks ARM; do not edit `live_state.json` manually.

All normal 2/3/5 ladders remain unchanged. There is no L3–L5 portfolio gate
or cancellation from this margin policy. Automatic master-to-subaccount
transfer is a separate future layer; current position/portfolio top-up caps,
other-position guarantees, shared emergency pool, and `$25` hard floor remain
in force until that layer is implemented and approved.

### Filled ladder reconciliation

Each open ladder is an exchange-side order and remains executable during a
brief local monitor interruption. On the next healthy cycle Pump Live reads the
new exchange quantity, average entry, mark, and liquidation price; marks the
ladder leg filled; and forces a new Bybit `tpslMode=Full` TP/SL synchronization.
The stop and take-profit therefore follow the complete enlarged position rather
than the original L1 quantity. A two-leg tier ends at L2 by design; absence of
L3 is not a lost order for that tier.

## Bybit Timestamp Recovery

Pump Live uses a long-lived synchronous CCXT client. A system-clock correction
can make the client's cached `timeDifference` stale and cause Bybit
`retCode=10002` even when the current Windows clock is correct.

Every authenticated Pump operation now uses the same guarded request path:

1. execute the request normally;
2. only for a confirmed Bybit timestamp/`recv_window` error, call
   `load_time_difference()`;
3. retry that rejected operation exactly once;
4. propagate the second failure or any unrelated error unchanged.

This applies to account reads, positions/orders, entry and ladder orders,
cancellation, leverage, TP/SL, and isolated-margin add/remove. Network
timeouts are not automatically replayed as orders because their exchange
outcome may be unknown.

The monitor remains fail-closed while an error is unresolved. After two full
healthy cycles it restores `armed` and sends a recovery notification. Repeated
errors use a stable error-family key for notification cooldown, so changing
request/server timestamps cannot create a message every 15 seconds.

## Live/Paper Fidelity and Close Accounting

The broad `1h` scanner remains the discovery source, but an entry-ready row is
now handed to Pump Live immediately after that symbol is processed. Pump Live
therefore no longer waits for the remaining hundreds of symbols in the same
scan. The normal ownership, slot, liquidity, risk, and duplicate-entry guards
still decide whether the signal can become an order.

After a live position is confirmed flat, Pump Live reads Bybit fills and the
account transaction log for that position window. It persists exchange-derived
entry/exit quantities and average prices, trading fees, funding, gross PnL, net
PnL, and return in `live_state.json`. A one-shot startup backfill fills these
fields for older closed Pump-owned positions that do not yet have close
accounting. The exchange records are authoritative; ticker snapshots are not
used as realized-PnL substitutes.

This adds no recurring private API polling. Private history is requested only
when a close needs accounting or a missing historical close is backfilled.

## Four-slot cash guard correction (2026-08-09)

The `$300` reserve is a budget: four `$50` position guarantees, a `$75` shared
emergency pool, and a `$25` operating floor. Confirmed entry prefunds count
toward the corresponding `$50` guarantees; they are not an additional reserve.

The new-slot available-balance gate therefore uses:

```text
slot_margin + (max_total_topup - current_confirmed_topup) + operating_floor
```

The independent rescue-quota gate remains unchanged. This avoids reserving
already-spent prefund twice while still preserving the entire unused `$275`
portfolio top-up capacity and `$25` floor.

Snapshot evidence: three open positions had `$135` confirmed top-up and
`$380.957704` available. The retired static test required `$475`; the corrected
complete-cap test requires `$340` and leaves `$40.957704` headroom. The source
change does not affect a running backend until an explicit restart and ARM.

Reproducible analysis:

```text
.venv\Scripts\python.exe scripts\pump_live_money_management.py
```

See `docs/pump_live_money_management_2026-08-09.md`. That review preceded the
explicit transfer implementation below; automatic rescue transfers remain
disabled even after the operator-only controller was added.

## Temporary main/sub transfers (2026-08-09)

The staged transfer controller is explicit only; it never moves money from a
liquidation monitor or automatically arms entries. It supports a minimum
project test of `$0.01 USDT` in both directions after a complete read-only
round-trip preflight.

Main -> Pump requires `AccountTransfer` plus `SubMemberTransfer` on a master
read/write API key. Pump -> main requires `AccountTransfer` plus
`SubMemberTransferList` on the Pump subaccount key; the extra permission is
needed for the fail-closed transfer-safe balance query. Prefer a dedicated
master key without `Withdraw`, configured only in ignored
`config/pump_live.env` as:

```text
BYBIT_PUMP_MASTER_TRANSFER_API_KEY=
BYBIT_PUMP_MASTER_TRANSFER_API_SECRET=
```

Every request persists its UUID before submission and must become `SUCCESS` in
universal-transfer history. Unknown outcomes are reconciled by the same UUID
and are never blindly repeated. Return is limited to confirmed temporary
principal and cannot cross the exchange transfer-safe balance, unused top-up
capacity, `$25` operating floor, or active strategy-capital floor.

Confirmed contributions update `equity_adjustment_usd` in the opposite
direction, so temporary cash cannot look like strategy profit or trigger a
false compounding recommendation. Returning the same principal reverses the
adjustment. Full design and the versioned `$3000` migration decision:
`docs/pump_live_temporary_transfers_and_3000_migration.md`.

Fresh capability evidence: the Pump key has `SubMemberTransferList` but lacks
`AccountTransfer`; the current master key has neither required Wallet
permission. Therefore the live round trip remains blocked and no one-way
transfer is allowed until both least-privilege gates are configured.
