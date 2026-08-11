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
- Guarded automatic rescue transfer from main is available only for an
  already-open position whose liquidation buffer is `>10%` and `<=20%`, when
  risk/top-up caps still allow margin but Pump free cash is insufficient.
  Normal funding and returns remain operator-controlled.

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

After the explicit `$3000` promotion, use `ARM PUMP LIVE 3000` instead. The UI
selects the confirmation from the active persisted risk policy.

Arming never adopts an old paper position. Only a new
`main_pullback_tier/entry_ready` decision produced after arming can be queued.
Every backend restart initially disables new entries; existing Pump positions
continue in recovery monitoring. `Arm live` may safely resume with tracked
positions only after the controller matches all exchange positions and opening
orders to its durable ledger and force-resynchronizes full TP/SL protection.
Unknown, missing, or degraded state remains blocked. A successful resume is
reported as a ready `tracked_positions_verified` preflight instead of leaving
the UI on the expected existing-position warning.

Every newly classified strategy decision contains a versioned
`pump_signal_scanner_snapshot_v1` audit snapshot. It preserves every field in
the source scanner row, including price returns, pump/continuation scores,
trigger and pullback data, funding, OI changes, long/short ratio, premium-index
features, volume z-score, matched profile, slow-pump fields, request metadata,
and parsed data-quality counts. Pump Live keeps the snapshot while the signal
is pending and in `position.open_decision`; `signals_queued`,
`live_position_opened`, and `live_entry_failed` also retain it in
`live_events.jsonl`. New scanner columns are copied automatically, so adding an
indicator does not require separately extending the live audit schema. This is
observability only and does not change classification, sizing, entry, ladder,
protection, or exit behavior. Positions opened before this format was deployed
keep their historical compact decision and are not backfilled from a newer
scan, because that would mix entry-time and later observations.

The same public Bybit event rows continue through all shadow/paper branches.
`main_pullback_tier` is both the primary shadow model and the only branch that
may submit an `entry_ready` decision to Pump Live. `conservative_control`,
`super_pump_shadow`, `pb20_baseline`, and `pb25_deeper_pullback` continue to
calculate independent paper positions and results on real observed events but
cannot place an exchange order. Cycle and candidate paper tracks remain
independent from the live position limit and capital ledger.

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
- Liquidation distance is monitored. A v1 position uses `$25/$50` steps and a
  v2 position uses proportional `$75/$150` steps; immutable per-position and
  active portfolio top-up caps are enforced. Warning/panic/emergency buffers
  remain `20% / 15% / 10%`.
- Normal v1 top-ups stop at the position's guaranteed `$50`; v2 uses `$150`. At or below the
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

The policy-appropriate explicit ARM confirmation may recover only the exact durable failure
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
guarded transfer implementation below.

## Temporary main/sub transfers (2026-08-09)

The transfer controller keeps explicit operator endpoints and also supplies a
bounded main -> Pump rescue callback to the liquidation monitor. It never arms
entries. The live callback is attempted only when the position is in
`WARNING` (`15..20%`) or `STRESS` (`10..15%`), Pump cash is the limiting factor,
and position/portfolio top-up caps still allow the full margin step. At or
below `10%` it is skipped so exchange stop/emergency close is never delayed.

Main -> Pump requires `AccountTransfer` plus `SubMemberTransfer` on a master
read/write API key. Pump -> main requires `AccountTransfer` plus
`SubMemberTransferList` on the Pump subaccount key; the extra permission is
needed for the fail-closed transfer-safe balance query. Prefer a dedicated
master key without `Withdraw`, configured only in ignored
`config/pump_live.env` as:

```text
BYBIT_PUMP_MASTER_TRANSFER_API_KEY=
BYBIT_PUMP_MASTER_TRANSFER_API_SECRET=
BYBIT_PUMP_SUB_TRANSFER_API_KEY=
BYBIT_PUMP_SUB_TRANSFER_API_SECRET=
```

The dedicated sub-transfer key belongs to the configured Pump subaccount and
needs Wallet `AccountTransfer` plus `SubMemberTransferList`; it should not have
trading permissions. If omitted, the controller falls back to the Pump trading
key but applies the same Wallet permission gate.

Every request persists its UUID before submission and must become `SUCCESS` in
universal-transfer history. Unknown outcomes are reconciled by the same UUID
and are never blindly repeated. Return is limited to confirmed temporary
principal and cannot cross the exchange transfer-safe balance, unused top-up
capacity, `$25` operating floor, or active strategy-capital floor.

Automatic rescue defaults and hard guards are configured only in ignored
`config/pump_live.env`:

```text
PUMP_LIVE_AUTO_TRANSFER_ENABLED=1
PUMP_LIVE_AUTO_TRANSFER_MAIN_MIN_AVAILABLE_USD=500
PUMP_LIVE_AUTO_TRANSFER_MAIN_MAX_MARGIN_RATIO=0.75
PUMP_LIVE_AUTO_TRANSFER_MAIN_MIN_LIQ_BUFFER_PCT=25
PUMP_LIVE_AUTO_TRANSFER_MAIN_MAX_DATA_AGE_SEC=180
PUMP_LIVE_AUTO_TRANSFER_MAX_INCIDENT_USD=250
PUMP_LIVE_AUTO_TRANSFER_DAILY_ALERT_USD=500
PUMP_LIVE_AUTO_TRANSFER_ROUND_USD=5
```

The requested cash shortfall is rounded upward to `$5`. The main account colour
is not an independent gate: `GREEN` and `WATCH` may both lend when the exact
post-transfer projection stays below the configured `75%` Bybit margin ratio,
leaves at least `$500` available, every active main position has confirmed
stops and at least a `25%` liquidation buffer, all active exchange accounts are
below the existing `80%` stress boundary, and the snapshot is no older than
three minutes. The transfer is also bounded by Bybit's fresh transfer-safe
amount and a `$250` per-incident canary ceiling. The `$500` daily value is an
alert threshold, not an emergency blocker.

There is no financial time cooldown. A second confirmed transfer can occur in
the next protective cycle if the same or another position still has a real
cash deficit. Each transfer remains UUID-backed and history-confirmed; only an
unknown/pending result blocks another submission, preventing a duplicate
outcome. Likewise, a verified margin top-up no longer creates a five-minute
period in which further necessary margin is forbidden. Position and portfolio
caps, fresh balance, and a fresh exchange position read after every add remain
the hard gates. Automatic Pump -> main return is deliberately not enabled yet;
the existing guarded manual return remains available.

If main cannot safely lend, Pump Live now publishes a read-only
`capital_rescue_shadow`. It ranks protected, profitable Pump positions by low
remaining distance to TP, profit and liquidation buffer, and estimates a
`25% / 50% / 100%` reduction sufficient to release the required cash. This is
advisory only: automatic partial closing is disabled until a separate live
canary proves cancellation of remaining ladders, reduce-only execution,
position refresh, and Full TP/SL resynchronization. See
`docs/pump_capital_rescue_orchestrator.md`.

Live activation evidence, 2026-08-09: commit `fd3ba75` passed the complete
Python regression (`664 passed`, `11 warnings`). Before restart, Pump was armed
with three protected positions and main Grid had no active execution or pending
transition. The supervised restart recovered the same positions/orders;
explicit ARM passed tracked-position ownership and protection verification.
Two following monitor cycles remained armed with no error or protection issue,
the minimum Pump liquidation buffer remained above `53%`, automatic transfer
reported the new `$500` available floor, `75%` projected margin ceiling and
`$250` incident step, and HEI appeared only as the recommended shadow donor.
No transfer or position reduction was triggered during deployment.

Do not call the transferable-coin endpoint for this member transfer. Bybit
rejects `UNIFIED -> UNIFIED` there with `131203`; that endpoint compares
account types rather than different main/sub UIDs. Direction readiness uses
the fresh USDT `transferBalance` and `transferSafeAmount` instead.

Confirmed contributions update `equity_adjustment_usd` in the opposite
direction, so temporary cash cannot look like strategy profit or trigger a
false compounding recommendation. Returning the same principal reverses the
adjustment. Full design and the versioned `$3000` migration decision:
`docs/pump_live_temporary_transfers_and_3000_migration.md`.

Earlier pre-configuration evidence found only `SubMemberTransferList` on the
Pump key and no required Wallet permission on the master key. The round trip
was correctly blocked at that point; the completed configuration and live
validation below supersede that snapshot.

Update 2026-08-09: dedicated master and Pump sub-transfer keys are configured
with the required least-privilege Wallet permissions. The supervised `$0.01`
main -> Pump -> main validation completed in both directions; balances,
temporary principal, and `equity_adjustment_usd` returned exactly to their
pre-test state. Do not infer that a large current mark-price liquidation buffer
is removable prefund: HEI showed over `111%` at the current mark but only
`4.0525%` stop clearance above its next ladder. Its `$75` floor remains locked;
even a `$5` reduction would fail the `2.45%` minimum clearance.

The same update added read-only capital regimes `CALM / NORMAL / WARNING /
STRESS / EMERGENCY` and exposes confirmed outstanding main contribution as
`Temporarily occupied` in desktop, unified positions, Pump controls, and
Android balance views. This is outstanding external principal, not strategy
PnL and not necessarily removable position prefund.

Live activation evidence, 2026-08-09: commits `95bbb9f` and `0061324` passed
the complete Python regression (`659 passed`, `11 warnings`) and Android unit
tests. A supervised backend restart recovered the same three Pump positions
and thirteen orders with zero protection issues; recovery ARM succeeded and
multiple following 15-second cycles stayed armed/healthy. Auto rescue reported
enabled with `$0` daily use, no pending transfer, and `$0` temporarily
occupied because every current position remained above the `20%` trigger.
Do not describe this as a live risk-trigger transfer test: the real transfer
rail was validated by the earlier `$0.01` round trip, while warning/emergency
trigger behavior was covered by regression without deliberately stressing a
live position.

Update 2026-08-10: a normal exchange-side TP/flat close no longer leaves new
entries permanently blocked by `position_absent_unconfirmed`. Automatic
re-arming is deliberately narrow: entries must have been armed before the
first missing-position scan, the position must be flat for two scans, close
accounting must be complete, no unknown position/order may exist, and every
remaining Pump position must be open with confirmed TP/SL for two additional
healthy scans. Operator disarm, emergency close, incomplete accounting, an
unknown exchange object, or degraded protection is never overridden. A
restart also clears the pending automatic recovery and still requires the
normal explicit ARM/resume verification.

Update 2026-08-10, `$3000` capital path: every legacy position is migrated to
an immutable `v1_1000` snapshot and is never resized. A confirmed main -> Pump
transfer remains temporary and excluded until the separate endpoint
`POST /api/pump-short/live/capital/promote` receives target `3000` and exact
confirmation `PROMOTE PUMP CAPITAL 3000`. The promotion consumes only the
principal needed to make effective strategy capital exactly `$3000`; profits
reduce that amount and excess transferred cash remains temporary/returnable.
Future entries then use `v2_3000` (`$525` slot, `$150` guarantee, `$825`
portfolio top-up cap, `$75` floor). The former one-concurrent-v2 canary was
replaced on 2026-08-10 by the same four-position cap plus fresh available-cash,
portfolio top-up and operating-floor admission gates described below.
Promotion neither places orders nor arms entries; it disarms the
entry gate and requires a fresh v2 preflight/ARM after the accounting boundary. Restart recovery
under v2 requires `ARM PUMP LIVE 3000`; the old `$1000` confirmation is rejected.
Realized PnL, external contribution, and the 70/30 profit allocation target are
reported separately; profit does not silently compound live size.
Manual capital funding above the `$0.01` rail test is now also fail-closed on
the fresh projected main-account state: the exact post-debit available balance,
margin ratio, position protection, liquidation buffers and data age must pass
before the universal transfer is submitted.

Pre-transfer deployment evidence, 2026-08-10: commits `8825663` and `06efe96` passed the
complete Python regression (`673 passed`, `11 warnings`). A supervised backend
restart found no active Grid execution, pending Grid transition, or running
manual execution. Pump recovered `1000RATSUSDT` and `BLUAIUSDT` with ten owned
orders, immutable `v1_1000` snapshots and zero protection issues. Explicit
`ARM PUMP LIVE 1000` passed `tracked_positions_verified`; three following
monitor cycles stayed armed with no blocked reason/error and minimum
liquidation buffer about `69.6..69.8%`. No transfer or capital promotion was
performed. At that snapshot Pump effective capital was `$1087.804836`, so the
exact external amount to `$3000` was `$1912.195164`. Fresh main-account
projection passed both that amount (about `$1119.93` Bybit main remaining) and
a round `$2000` debit (about `$1032.13` remaining), with zero projected Bybit
margin use, verified stops and minimum cross-account position buffer `31.30%`.
The operator subsequently authorized the transfer after a fresh repeat of all
gates. FeeArb transferred `$2000` main -> Pump, confirmed it on Bybit, and
capitalized only `$1912.195164`. The remaining `$87.804836` stayed temporary;
`$87.8048` was safely returned Pump -> main. Bybit's four-decimal
`transferSafeAmount` left `$0.000036` in the Pump wallet. Because that amount is
below the project `$0.01` transfer minimum, it is now recorded as excluded
rounding dust rather than returnable `Temporarily occupied` principal. Its
negative equity adjustment is retained, so it cannot appear as Pump profit.

After promotion, the Pump wallet was `$3000.000036` and effective strategy
capital was exactly `$3000`. Explicit `ARM PUMP LIVE 3000` succeeded; three
following cycles stayed armed with the same two legacy positions, ten orders,
zero protection issues and minimum liquidation buffer about `69.7..69.9%`.
No v2 position had opened at that checkpoint. The next eligible new entry is
the single `$525` v2 canary; existing positions remain immutable v1. The
rounding-dust hardening passed the complete regression (`676 passed`, `11
warnings`).

Post-hardening deployment evidence: commit `fddfdcc` was deployed by the
supervised stop/start scripts only after Grid showed zero active executions and
pending transitions and Manual showed zero running executions. Restart was
fail-closed (`entry_armed=false`) and migrated both ledgers to `$0` temporary
outstanding plus `$0.000036` excluded dust. Legacy `ARM PUMP LIVE 1000` was
rejected with HTTP 400; explicit `ARM PUMP LIVE 3000` passed ownership and
protection recovery. Three complete monitor cycles stayed armed with no blocked
reason/error, two v1 positions, zero v2 positions, ten orders and a live
monitor. Final regime was `CALM`, minimum liquidation buffer `71.62%`, and new
slot headroom `$1223.16`.

## Risk-first entry freeze (2026-08-10)

The monitor now evaluates and maintains every existing position before it can
process a pending entry. Immediately after the fresh exchange reconciliation,
all new entries are frozen when any tracked position is not fully `open`, has
an unavailable liquidation buffer, or is at/below its `20%` warning boundary.
The freeze happens before margin top-up or main -> Pump rescue funding and
does not disable monitoring, Full TP/SL, ladder reconciliation, margin adds or
emergency handling.

Any pending entry captured before the freeze is discarded rather than replayed
after a potentially long risk incident. Signals received while frozen are also
not queued; the scanner must provide a fresh `entry_ready` event after recovery.
If no freeze is active, pending-entry admission occurs only after position
maintenance and uses newly fetched balance, positions and orders, so a top-up
cannot leave entry sizing with stale available cash.

An automatically-created risk freeze may restore its prior armed state only
after two consecutive complete cycles where every remaining position is
strictly above the dedicated `25%` entry-restore threshold, is exchange-present and `open`, and has
positive confirmed TP and catastrophic SL. The second recovery cycle only
re-arms; a fresh signal can execute no earlier than the next cycle. Explicit
operator disarm, backend restart, emergency close, unknown exchange state,
reserve failure and other hard errors are never auto-overridden. Explicit ARM
also rejects a tracked position still inside the warning band.

Risk order is therefore: freeze/clear stale entries -> maintain the lowest
buffer first -> use Pump reserve -> attempt projection-gated main funding above
the `10%` emergency boundary -> emit the profitable-donor shadow if funding is
blocked -> close the threatened position reduce-only at the emergency boundary.
Automatic donor reduction remains disabled and requires its separate approved
micro-canary. Targeted Pump/transfer/API regression passed (`95` tests), and
the complete Python suite passed (`681 passed`, `11 warnings`).

## Sequential ladder margin gate (2026-08-10)

Pump now keeps only the nearest unfilled ladder order live on Bybit. The full
2/3/5-leg strategy plan, trigger prices, weights and notional remain unchanged
in the durable position, but later legs stay `planned`. After a fill is
exchange-confirmed, the monitor recalculates the actual isolated-margin need
from fresh quantity and liquidation price. It requires the prospective Full
catastrophic stop to be at least `2.5%` above the next trigger (with the existing
relative `0..2%` verification tolerance), rounds the required add upward to
`$5`, adds margin, rereads the exchange position, synchronizes TP/SL, and only
then submits that one next ladder.

On first deployment, an old position with several live ladder orders is
migrated in place: the nearest order is retained and every later order is
cancelled and exchange-confirmed before being returned to `planned`. A cancel
that races with a fill cancels all remaining later orders, refreshes the
position and disarms entries for a clean next cycle. If the calculated margin
cannot fit the position/portfolio envelope or cannot be confirmed, the next
ladder is cancelled/deferred, Full TP/SL remains active, monitoring continues,
and entries stay fail-closed with `next_ladder_margin_not_confirmed`.
If a concurrently finishing legacy monitor overwrites an already-confirmed
gate cancellation as generic `ladder_order_lost`, only the exact durable
`symbol + step + old_order_id` gate event may restore that leg to `planned`;
unexplained operator/exchange cancellations are never recreated automatically.

Entry notional remains cohort-immutable: legacy `v1_1000` positions keep their
original `$175` ladder plan. Margin defence is different from entry sizing: an
open legacy position may use the currently active `v2_3000` per-position
defence ceiling (`$525`) while the shared `$825` top-up cap, guarantees for the
other positions and `$75` operating floor still apply. This lets free Pump cash
protect a valid next step instead of stopping at the obsolete legacy `$175`
ceiling. If only cash is missing but those risk limits allow the add, the
existing projection-gated main -> Pump rescue provider is tried before the
ladder is deferred.

The old hard `v2_concurrent_entry_cap=1` is now reported as legacy state and no
longer blocks a fourth coin by itself. Admission remains bounded by four total
positions, the `$525` new slot, all unused shared top-up capacity and the `$75`
floor. A warning position still freezes fresh signals at `<=20%`; recovery is
now decoupled from the margin-removal `35%` CALM boundary and needs two healthy
cycles strictly above `25%`. Backend restart and operator disarm still never
auto-arm.

An explicit emergency operator endpoint is available for a tracked open
position:

```text
POST /api/pump-short/live/prefund-next-ladder
symbol: BLUAIUSDT
confirmation: PREFUND PUMP NEXT LADDER BLUAIUSDT
```

It uses the same serialized operation path, risk envelopes, durable accounting,
exchange verification and Full TP/SL synchronization as the automatic gate; it
is not an arbitrary untracked Bybit margin call. Pre-deployment verification
passed the focused Pump/API/positions set (`87` tests at the first checkpoint),
the expanded Pump suite (`77` tests), Python compilation, JS contract review,
and the complete project regression (`696 passed`, `8` subtests, `13`
pre-existing warnings).

Live deployment evidence, 2026-08-10: BLUAI first received an explicit
ledger-aware `$55` prefund before the wider change. Tracked top-up/floor became
`$105/$105`; Bybit liquidation moved from `0.032313` to `0.037698`, and the
confirmed Full stop moved to `0.03675555`, `3.009%` above L3 `0.035682`.
Commits `ba9ae06` and `5a011a0` were then deployed after the full `696`-test
regression. The pre-restart TUT Grid state had no active execution or fill
(`completed_no_fill`, `filled_qty=0`) and survived as
`partial_enter_waiting_trigger`; Manual had zero running executions.

The live migration retained only 1000RATS L2, BLUAI L3 and ACE L2. Their later
legs are durable `planned`; all three report `ladder_gate_status=ready` and
`margin_continuation_policy_id=v2_3000`. Exchange state is exactly three ladder
orders plus six reduce-only Full TP/SL orders. No additional margin was added
during migration: total tracked top-up/floor stayed `$220/$220`. Cancelling the
six distant orders released their order reservation, increasing fresh Pump
available balance to `$2375.444987`; the capital regime reports `$1205` required
for a fourth slot and `$1170.444987` headroom. BLUAI remained protected around
`35.6..35.8%` buffer. Restart stayed deliberately fail-closed with
`entry_armed=false`; no automatic or implicit ARM was performed.

Deployment evidence: commit `7e60848` was deployed only after confirming Pump
`CALM`, zero pending signals/transfers, zero Grid executions/transitions and
zero running Manual executions. The restart recovered two immutable v1
positions and ten orders with entries disarmed. `ARM PUMP LIVE 3000` passed
`tracked_positions_verified`; three complete risk-first cycles then remained
armed with no blocked reason/error, no active freeze, zero pending signals and
minimum liquidation buffer `72.22..72.26%`. No v2 position opened during the
deployment observation.

Manual dual-sizing deployment evidence, 2026-08-10: commit `29237af` was
deployed by the supervised stop/start scripts only after confirming a clean
worktree, zero running Manual executions, and zero active or pending Grid
transitions. Restart recovery was fail-closed and restored the two immutable
v1 positions (`1000RATSUSDT`, `BLUAIUSDT`) plus ten owned orders with zero
protection issues. The raw read-only gate reported only the expected existing
position/order errors; explicit `ARM PUMP LIVE 3000` then passed the ownership
and protection-aware resume preflight. Three following complete monitor cycles
remained armed with no blocked reason, last error, risk freeze, or pending
signal. Minimum Pump liquidation buffer was about `79.5%` at final verification.

## Shared-margin and post-fill decision (2026-08-10)

`ARM PUMP LIVE 3000` was explicitly requested and accepted with three tracked
positions. Three following 15-second cycles remained armed in `CALM`, with a
live monitor, no blocked reason/error, about `$2375.44` available and about
`$1170.44` fourth-slot headroom. BLUAI had L3 live at `0.035682`, Full stop
`0.03675555`, liquidation `0.037698` and only `3.009%` old-stop clearance.

The current gate protects access to the next ladder before its fill but leaves
the previous Full stop unchanged until the monitor observes that fill. Higher
short fills improve the projected liquidation, but a fast continuation can hit
the old stop first. The research-only replay in
`docs/pump_live_shared_margin_research_2026-08-10.md` rebuilds 126 cases since
the 2024 boundary and 40 eligible candidates. The current `$525` gate has one
historical capacity breach and 38 old-stop race fills. A `$525` projected
fill/next-step gate retains 35 trades with zero modelled breaches/races and a
conservative `$355` peak main loan; `$600` is the later shadow candidate, not
an approved live resize.

Canonical next implementation: retain exchange-isolated positions, pool Pump
cash at portfolio level, project the complete next fill before placing its
order, and verify enough margin for both the old and projected stop to clear
the following ladder. Main funds are rescue-only after fresh main-risk
projection, never normal entry capital. Unlimited final-leg rescue is rejected;
use a bounded facility, then donor reduction and threatened-position derisk.
No margin, ladder, sizing or transfer setting was changed by this research.

## Shared projected margin manager v3 (2026-08-11)

The current sequential manager is retained as the explicit rollback version
`v2_current_next`. The approved manager is `v3_shared_projected`; it is selected
only by `PUMP_LIVE_MARGIN_MANAGER_POLICY` and every backend restart remains
fail-closed until the operator supplies the capital-matched ARM phrase.

Both versions keep exchange positions on isolated margin. Existing ladder
notional is immutable, and every new `v2_3000` position still has exactly a
`$525` full ladder. `v3_shared_projected` changes cash allocation and safety
gates, not the entry strategy:

- For every nearest live/planned ladder, project its complete fill from fresh
  exchange quantity and liquidation. The current and projected post-fill Full
  stop must both clear the following ladder. For the final ladder, the stop
  must retain a bounded `20%` continuation buffer.
- Before admitting a new symbol, hard-reserve all future base ladder margin,
  the immediately executable next-gate margin for every open position and the
  candidate, up to three `$5` correction increments for that gate, and the
  `$75` operating floor. The full remaining path is calculated separately as
  `full-path stress`; it does not block entry because distant orders are not
  live and each later step must pass the same gate after the preceding fill.
- Pump-owned available cash alone decides new entry. Main-account money is a
  rescue-only facility: temporarily borrowed principal is subtracted from
  entry headroom even if it is visible in Pump available balance.
- The rescue ceiling is `$2000` aggregate and `$2000` for one position, but it
  is not a promise or automatic debit. Each real transfer still passes fresh
  main-account margin/liquidation/freshness checks, idempotent confirmation and
  the lower operational per-request limit. If funding cannot be confirmed, the
  next ladder is deferred; protection monitoring remains active.
- Shared capacity is dynamic: full ladder capital is reserved for positions
  that actually exist, not four fixed private envelopes. Lowest liquidation
  buffer is maintained first. An already-live ladder that fails the new gate
  is cancelled and confirmed before margin work, then recreated only after a
  fresh exchange read and Full TP/SL confirmation.
- `v3_shared_projected` uses hard exchange-confirmed targets. Bybit forbids
  isolated `position margin > position value`; if that makes the strict
  following-step prefund impossible, fresh `positionValue`, `positionIM` and
  `positionMM` define a conservative exchange add ceiling. The fallback then
  requires both current and projected stops at least `8%` above the immediate
  filling ladder and reconciles live ladders every `5s`. After a fill, it
  rereads the larger position and funds the next gate before placing another
  order. This is recorded as `exchange_margin_cap_reaction_buffer`, never as a
  strict following-step guarantee. The old relative `0..2%` tolerance remains
  only for `v2_current_next`; neither v3 boundary accepts a shortfall.
- Exact Bybit `10001 / can not set pm more than pv` rejection is treated as
  exchange-cap evidence, not a generic retryable failure. The controller reads
  the position once, records a qty/step circuit breaker and evaluates the 8%
  fallback. If already protected it proceeds without another add; otherwise it
  defers the ladder and emits one blocked event. The write is not retried until
  qty/step changes or position value increases materially (more than 5%).
- Bybit retains used `orderLinkId` values after cancellation. A ladder rejected
  with exact `110072` duplicate-link evidence is recoverable only when a fresh
  exchange read finds zero non-reduce orders for that symbol. The controller
  then persists the next link generation (`L2R1`, etc.) and recreates only the
  nearest leg. Any unknown live order keeps the position fail-closed.
- Under `FEEARB_TESTING=1`, every default Pump-lab instance uses a private
  temporary state directory with recovery/background monitors disabled. The
  required release check hashes the production `live_state.json` and
  `live_events.jsonl` before and after targeted and complete pytest runs.

Live migration result on 2026-08-11: the ledger was reconciled to confirmed
exchange/event totals of `$35` for RATS, `$125` for BLUAI and `$165` for ACE;
this reconciliation did not submit a transfer or margin request. Exact
duplicate-link recovery then created one `R1` nearest ladder per symbol while
leaving later legs `planned`. Four consecutive hot monitor cycles stayed
healthy with three non-reduce ladders and six Full reduce-only TP/SL orders.
The operator-authorized `ARM PUMP LIVE 3000` succeeded with manager
`v3_shared_projected`, strategy capital `$3000`, and new-position ladder size
`$525`. That first deployment snapshot denied a fourth entry under the former
complete-path cash gate. The same-day next-gate refinement supersedes that
decision while preserving the negative full-path value as a stress warning.

Next-gate refinement deployment on 2026-08-11 completed with the same three
owned positions, exactly three nearest non-reduce ladders and six Full TP/SL
orders. `ARM PUMP LIVE 3000` succeeded; following monitor cycles remained
armed with no error and no margin add. For the displayed equal five-leg
candidate, hard required available was `$1382.50` against about `$2270.39`
available (`+$887.89` headroom). Full-path stress remained `-$2062.11` and is
shown as warning context rather than entry authority.

The Pump page and `/api/positions/overview` expose `margin_manager`,
`shared_pool`, next-gate headroom, full-path stress headroom and the effective
per-position cap. Rollback
requires changing only the manager variable back to `v2_current_next`, a safe
restart, verification of all owned orders/protection and a fresh explicit ARM;
it never rewrites historical position sizing.

## Shared on-demand capital manager v4 (2026-08-11)

The active operator policy is `v4_shared_ondemand`. Its rollback values are
`v3_shared_projected` and `v2_current_next`; changing the environment value
always requires a safe backend restart, exchange reconciliation and a new
explicit ARM.

Operational contract:

1. All Pump positions stay on Bybit isolated margin. The common pool is the
   allocator for free Pump USDT, not cross margin.
2. At Pump strategy capital `$3000`, only a new symbol receives policy
   `v3_3000_pool600` and a `$600` complete ladder. The unchanged strategy
   weights split it as `$120 x 5`, `$100/$200/$300`, or `$200/$400`.
   Existing positions continue with the exact risk-policy snapshot stored when
   they entered. The other `$600` in `$3000 - 4 x $600` is not a separately
   held reserve; actual availability is decided by the free-cash gates.
3. A new symbol is admitted only if its L1, its one next order, and immediate
   safety prefund fit now and still leave at least 30% Pump-owned free cash plus
   the `$75` floor. Unsubmitted future steps are not reserved. Temporary main
   rescue principal is subtracted before this calculation.
4. Displayed account cash bands are calm at `>=30%`, warning at `20..30%`,
   stress at `10..20%`, and emergency below `10%`. They combine with the worst
   position liquidation buffer. A failed 30% admission gate blocks only the
   candidate; it does not abandon monitoring of existing positions.
5. Each position warning at `<=20%` asks for the exact `$5`-rounded margin that
   restores a 25% exchange-read liquidation buffer. The controller first uses
   existing Pump cash. If it is short, it tries the automatic main rescue
   facility before any reduction. Outstanding temporary principal cannot
   exceed `$2000`; the transfer still needs fresh main-account risk data and
   the configured `$500 / 75% / 25%` projected main safety gates.
6. If the transfer is unavailable or insufficient, the controller cancels and
   confirms only non-reduce Pump ladder orders, never Full TP/SL. Ladders remain
   paused until Pump free cash is at least 30% and all positions are above the
   20% warning line.
7. If defence is still unfunded and automatic rescue reduction is enabled, one
   whole protected profitable donor closest to TP is closed reduce-only. If no
   donor is eligible, the threatened position is closed. The first live version
   intentionally does not perform a partial donor cut because that requires a
   separately tested cancel/rescale/re-protect transaction. A threatened
   position at or below the 10% emergency buffer is closed directly.
8. Surplus isolated margin can be returned only after two healthy `>=35%`
   reads and the existing 30-minute adjustment cooldown. Removal targets 25%,
   must preserve the immediate next-ladder safety gate, is confirmed from
   Bybit, and is rolled back if either condition fails.
9. Capital observation calculates a proportional future budget at 20% of
   Pump-owned strategy capital, rounded down to `$5`. Periodic automatic
   adoption is not enabled in this release. A later approved rebase will apply
   only to positions opened after that rebase.

The `$3000` cohort, including `v3_3000_pool600`, uses the exact confirmation
`ARM PUMP LIVE 3000`. Every backend restart remains disarmed regardless of the
previous state.

First v4 deployment completed on 2026-08-11. The backend restarted disarmed,
reconciled three owned Pump positions and nine Bybit orders, and selected
`v4_shared_ondemand / v3_3000_pool600`. After one complete monitor cycle every
immediate ladder gate was ready; explicit `ARM PUMP LIVE 3000` succeeded.
Three later cycles stayed armed with one nearest ladder per position, no error,
no blocked reason, and no net balance/top-up change. Before ARM, the initial v4
cycle trial-removed and immediately restored `$25` BLUAI, `$165` ACE and `$35`
RATS after their next gates failed. The pre-write planner below supersedes that
exchange churn. Final observed
wallet/available was about `$2999.72/$2270.39` (75.69% free, `CALM`), temporary
outstanding principal was zero, and the unified view reported zero protection
issues. The default five-leg admission sample required `$360` now and projected
63.69% free Pump-owned cash after that action, so a fourth candidate was ready
subject to its real tier and signal gates.

## Next-ladder fill and safe margin release (2026-08-11)

Exactly one non-reduce next-ladder order may be physically open for each Pump
position. Its `margin_usd` is the order's 3x initial-margin budget: Bybit
reserves it from available cash while the order is open, and it becomes initial
margin for the added contracts when filled. It is not sufficient by itself for
the short reaction window, so v4 also verifies isolated prefund before the
order may remain live.

For `v4_shared_ondemand`, both boundaries now use the same dedicated
`PUMP_LIVE_ON_DEMAND_FILL_REACTION_BUFFER_PCT=12` value and accept no
tolerance:

1. The currently active Full Stop must be at least 12% above the next order's
   fill price. This is the pre-fill reaction budget while the monitor has not
   yet observed the larger quantity.
2. A full-order fill is projected in advance, including its 3x initial margin.
   The Full Stop derived from projected liquidation must also be at least 12% above
   the same fill price. After the fill, the monitor rereads quantity, average
   and liquidation, synchronizes Full TP/SL, and only then funds/places the
   following ladder.

The older `PUMP_LIVE_MARGIN_PREFUND_SAFETY_PCT=2.5` and
`PUMP_LIVE_PROJECTED_EXCHANGE_CAP_REACTION_BUFFER_PCT=8` remain rollback
inputs for `v2_current_next` and `v3_shared_projected`; they do not weaken the
active v4 boundary. The v4 value is dynamic, so after a safe restart it also
applies to already-open positions whose immutable entry-sizing snapshot was
created under an earlier margin-manager version.

Margin release uses the inverse sequence. It still requires two consecutive
`>=35%` position-buffer reads and 30 minutes since the last add/remove. Before
calling Bybit, `plan_safe_margin_reduction` finds the largest `$5` increment
that leaves at least a 30% buffer and preserves both next-fill boundaries. If
no `$5` increment is safe, no exchange call is made. A submitted removal is
still reread from Bybit; any unexpected buffer or next-gate failure causes an
immediate rollback and Full TP/SL resync.

Live rollout evidence: after a safe disarmed restart and explicit
`ARM PUMP LIVE 3000`, the first eligible reduction cycle released `$10` RATS,
`$20` BLUAI and `$75` ACE. Pump available cash rose from about `$2270.39` to
`$2375.39`; the resulting tracked top-ups were `$25/$105/$90`, with no
rollback. Direct Bybit read showed nine orders: physical RATS L2 at `0.074835`
(exchange display tick `0.07484`), BLUAI L3 at `0.035682`, ACE L2 at `0.17556`,
and six reduce-only Full TP/SL orders. Later monitor reads remained armed with
no error/block and all gates ready. Current/projected clearances were
RATS `2.59%/16.45%`, BLUAI `3.01%/8.51%`, and ACE `3.15%/12.62%`, versus the
required `2.5%/8%`; the unified positions endpoint reported zero protection
issues.

## 12% v4 fill-reaction hardening (2026-08-11)

A fresh BMT audit found that the earlier current/projected `2.5%/8%` pair did
not fully cover the period between a Bybit ladder fill and the next exchange
reconciliation. The hot-ladder loop sleeps for `5s`, but measured start-to-start
cycles were about `7.35s` including network and processing time. BMT Mark Price
also produced post-entry one-minute rises above the old current-stop clearance.
The risk was a protected Full SL exit before the controller could resync the
larger position, not an unprotected liquidation.

The active v4 contract is therefore `12%/12%` before every physical next
ladder may remain open. The pre-write gate cancels and confirms an existing
nearest order if necessary, adds and verifies isolated margin, resynchronizes
Full TP/SL, and only then recreates that one order. No distant ladder is made
live. Margin release uses the same 12% boundary, so surplus cannot be removed
when it would reopen the fill-reaction window.

The exact BMT three-leg regression starts from exchange-like state
`qty=15117`, `liq=0.03155`, L2 `0.029772` and L3 `0.039696`. It requires about
`$45` before L2 and `$315` before L3, verifies the still-active stop and the
fully-filled projected stop at or above 12% for both transitions, and leaves
the projected post-L3 liquidation buffer above 20%. These values are model
outputs rounded to `$5`; live Bybit reads remain authoritative.

Verified Python compilation, focused Pump tests (`112 passed`), the expanded
Pump/API/lab set (`211 passed`, `6 warnings`), and the complete project suite
(`743 passed`, `8` subtests, `13` pre-existing warnings). The production event
journal hash was unchanged by the test runs; the live state file continued to
advance normally under the already-running monitor.
