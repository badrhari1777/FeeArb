# Pump Live temporary transfers and $3000 migration — 2026-08-09

## Current decision

The transfer layer is explicit and fail-closed. It does not automatically move
money in response to liquidation distance and it does not enable live entries.
The first live validation is a `$0.01 USDT` round trip:

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
- A dedicated master transfer key is preferred:
  `BYBIT_PUMP_MASTER_TRANSFER_API_KEY` and
  `BYBIT_PUMP_MASTER_TRANSFER_API_SECRET` in ignored
  `config/pump_live.env`.
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
