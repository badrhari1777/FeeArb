# Pump Live temporary transfers and $3000 migration — 2026-08-09

## Current decision

The transfer layer is fail-closed and does not enable live entries. Operator
endpoints remain explicit; a separate bounded callback may now automatically
move only main -> Pump rescue cash when an already-open position enters the
`10..20%` liquidation-buffer band and Pump cash alone prevents an otherwise
permitted top-up. At `<=10%`, the callback is skipped and exchange protection /
emergency close proceeds without waiting. The first live validation was a
`$0.01 USDT` round trip:

1. main `UNIFIED` -> Pump subaccount `UNIFIED`;
2. confirm `SUCCESS` from universal-transfer history;
3. verify both balances and record the temporary contribution exclusion;
4. Pump subaccount `UNIFIED` -> main `UNIFIED`;
5. confirm `SUCCESS`, balances, zero outstanding temporary principal, and zero
   net strategy-growth adjustment from the round trip.

Official Bybit documentation does not publish a separate universal-transfer
minimum. FeeArb therefore uses a conservative project minimum of `$0.01` and
always checks the live `transferBalance` / `transferSafeAmount` first.

## Least-privilege credential gate

- Main -> Pump requires a master read/write key with Wallet
  `AccountTransfer` and `SubMemberTransfer`.
- Pump -> main uses the Pump subaccount key with Wallet
  `AccountTransfer` and `SubMemberTransferList`. `AccountTransfer` is required
  by the fail-closed balance/safe-amount check even though Bybit documents only
  `SubMemberTransferList` for submitting the outbound universal transfer.
- The UIDs must form the configured parent/subaccount pair and both accounts
  must be UTA.
- Bybit's transferable-coin endpoint is not used for this member transfer: it
  rejects `UNIFIED -> UNIFIED` with `131203` because it models account-type
  changes, not different member UIDs. Fresh USDT `transferBalance` and
  `transferSafeAmount` remain mandatory for each enabled direction.
- A dedicated master transfer key is preferred:
  `BYBIT_PUMP_MASTER_TRANSFER_API_KEY` and
  `BYBIT_PUMP_MASTER_TRANSFER_API_SECRET` in ignored
  `config/pump_live.env`.
- A dedicated Pump subaccount transfer key is preferred through
  `BYBIT_PUMP_SUB_TRANSFER_API_KEY` and
  `BYBIT_PUMP_SUB_TRANSFER_API_SECRET`. It needs Wallet
  `AccountTransfer` plus `SubMemberTransferList` and no trading permission.
  The existing Pump trading key remains an explicit fallback only.
- If those values are empty, preflight inspects the root `.env` Bybit key. It
  does not weaken the permission gate.
- A master transfer key with `Withdraw` is rejected.

Fresh read-only evidence on 2026-08-09: the Pump key has
`SubMemberTransferList` and matches the configured sub UID and main parent, but
lacks `AccountTransfer` for the safe-balance query. The current main key is
master/read-write/UTA but has no Wallet permissions. The complete round trip is
therefore blocked. No one-way live test was submitted.

## Runtime contract

Endpoints:

- `GET /api/pump-short/live/transfers`;
- `POST /api/pump-short/live/transfers/preflight`;
- `POST /api/pump-short/live/transfers/in`;
- `POST /api/pump-short/live/transfers/return`;
- `POST /api/pump-short/live/transfers/reconcile`.

Exact confirmations:

```text
TRANSFER TEMPORARY USDT MAIN TO PUMP
RETURN TEMPORARY USDT PUMP TO MAIN
```

The controller persists a UUID and pending state before the Bybit POST. A
timeout or unknown response is never blindly resubmitted: later reconciliation
queries that exact UUID. Another transfer is blocked until the pending outcome
is resolved. A successful response is still checked in universal-transfer
history before local accounting changes.

Return is bounded by all of:

- confirmed outstanding temporary principal;
- Bybit transfer-safe amount;
- available balance after unused top-up capacity and operating floor;
- wallet balance above active strategy capital.

Runtime evidence is ignored by Git:

```text
data/research/bybit_pump_short_live/temporary_transfers.json
data/research/bybit_pump_short_live/temporary_transfer_events.jsonl
```

## Profit and capital accounting

After a confirmed main -> Pump transfer, Pump Live records the amount as
temporary outstanding principal and subtracts it from the capital observation
through `equity_adjustment_usd`. The same cash therefore cannot trigger a false
growth recommendation or appear as strategy profit. A confirmed return adds
the amount back to the adjustment and reduces outstanding principal.

Accounting is idempotent by transfer UUID. Cumulative input and returned
amounts remain visible even after outstanding principal returns to zero.
Exchange-derived trade fees, funding and realized PnL remain separate.

## Automatic rescue funding contract

- Default source configuration is disabled; live activation is an ignored-env
  operator setting.
- Trigger: `liq_buffer_pct <= 20` and `> 10`, risk capacity at least `$1`, and
  Pump available cash above its `$25` floor is smaller than the allowed top-up.
- The transfer request is the exact cash shortfall rounded upward to `$5`.
- No partial send: the rounded amount must fit the `$50` single-transfer cap,
  `$200` UTC-day cap, Bybit transfer-safe amount, and `$2000` main-wallet floor.
- A successful transfer is fetched back through Pump balance before isolated
  margin is added. Failure/uncertainty never relaxes top-up caps or protection.
- Five-minute cooldown and durable pending UUID prevent duplicate transfers.
- A completed contribution remains `temporary_transfer_outstanding_usd`, is
  excluded from observed profit, and is shown as `Temporarily occupied`.
- Automatic return is not enabled. Manual return retains all principal,
  reserve, operating-floor, and active-capital checks.

The live status also reports `CALM (>35%)`, `NORMAL (20..35%)`, `WARNING
(15..20%)`, `STRESS (10..15%)`, and `EMERGENCY (<=10%)`, together with minimum
buffer symbol, total top-up, immutable prefund floor, removable excess, new-slot
headroom, and temporary external principal.

## Can the account move to $3000 with three positions open?

Financially yes; the current runtime policy cannot yet do it safely by simply
changing one global number.

At the captured wallet `$1043.862297`, the deposit needed to reach exactly
`$3000` is approximately `$1956.137703`. The wallet is live and this amount must
be recalculated immediately before an actual transfer.

The proportional `$3000` target is:

- deployable: `$2100`;
- reserve: `$900`;
- four slots: `$525` margin each, or up to `$1575` notional at `3x`;
- four guaranteed top-ups: `$150` each (`$600` total);
- shared emergency pool: `$225`;
- operating floor: `$75`;
- portfolio top-up cap: `$825`.

The three current `$175` positions must remain immutable. Their existing
quantities, ladders, prefund floors, TP/SL, hold clocks and accounting continue
under policy `v1_1000`. Recalculating their remaining orders to `$525` would
change risk mid-trade and is forbidden.

Recommended staged migration:

1. Add the future capital as an excluded contribution while keeping active
   policy `v1_1000` and `$175` entries.
2. Complete an execution-aware `$3000` margin/spike replay. The approximate
   prefunds and rescue caps scale with quantity and cannot be inferred only
   from the old ROI replay.
3. Add immutable per-position `risk_policy_id` and portfolio accounting that
   understands simultaneous legacy and new cohorts.
4. Only after regression, enable `v2_3000` for new signals. Existing positions
   remain v1 until flat; each freed slot migrates to `$525` independently.
5. Promote the deposited contribution from temporary/excluded capacity to
   declared strategy capital only with an explicit operator action. It remains
   an external cashflow for performance reporting.

With three legacy positions and one future `$525` v2 slot, the conservative
capital commitment is `$525 legacy + $525 new + $900 reserve = $1950`, leaving
`$1050` headroom inside `$3000`. Capacity is sufficient, but current code does
not yet carry immutable mixed-cohort risk policies, so this configuration is
research/design only and must not be armed as `$3000` yet.

## Protective-order scope clarification

Timer-only stop rotation is disabled globally for every exchange through
`stop_force_requote_max_age_sec=0`. A valid exchange stop is no longer replaced
only because it is 60 seconds old.

The create -> verify -> cancel -> verify replacement sequence is currently
KuCoin-specific because conditional-order semantics and duplicate-stop support
differ by venue. Other exchanges may still replace protection after a real
price/quantity/side mismatch; they no longer rotate solely on age.

## Live minimum round-trip validation (2026-08-09)

The dedicated master and Pump sub-transfer credentials passed identity,
parent/sub relationship, UTA, permission, history, and transfer-safe balance
checks. A supervised `$0.01 USDT` main -> Pump -> main round trip completed
with a different persisted UUID for each direction and `SUCCESS` confirmation
from universal-transfer history.

Main and Pump wallet/transfer-safe balances returned exactly to their captured
pre-test values. Temporary outstanding principal returned to zero,
`equity_adjustment_usd` returned to zero, cumulative input/return remained
`$0.01 / $0.01`, and there was no pending reconciliation. Pump stayed armed;
three tracked positions and thirteen orders were unchanged.

## HEI prefund efficiency decision (2026-08-09)

HEI's current mark-price liquidation buffer above `111%` is not the correct
measure for releasing its `$75` prefund. The short is far in profit at the
current mark, but a spike back to the already-resting L2 removes that
unrealized profit before the order fills. The applicable geometry is:

- current mark about `0.15767`;
- L2 `0.311955`;
- exchange liquidation `0.33292`;
- catastrophic stop `0.324597`;
- liquidation buffer evaluated at L2: `6.7205%`;
- stop clearance above L2: `4.0525%`;
- minimum accepted clearance: `2.45%` (`2.5%` target with the existing
  bounded tolerance).

Using the current quantity, configured MMR/fee model, and actual exchange
liquidation, at most about `$4.43` can be removed before the verified clearance
falls below `2.45%`. The live adjustment step is `$5`; removing one step would
leave only about `2.2426%`, so the safely releasable amount is `$0` under the
current policy. Removing `$25` would put the stop about `5.0%` below L2 and
defeat the purpose of guaranteeing the second ladder before the catastrophic
stop.

Capital-efficiency candidate, shadow only: calculate and expose both current
mark buffer and next-ladder stop clearance, plus a rounded
`max_releasable_prefund_usd`. Observe recommendations without removing the
immutable floor. Promotion should require exchange-refreshed liquidation,
two confirmations, cooldown, and a post-removal verification/rollback; price
fall alone must never justify releasing margin. Main/sub transfers can improve
slow capital allocation, but cannot replace prefunding needed for a one-tick
spike.
