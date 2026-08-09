# Pump Live money management review — 2026-08-09

## Scope

Read-only reconstruction of the current Pump signals and a reproducible
four-slot capital replay. The code change in this block only corrects the entry
cash guard; it does not restart/ARM Pump Live, transfer funds, change existing
orders, or resize open positions.

Reproduce the historical/current calculation with:

```text
.venv\Scripts\python.exe scripts\pump_live_money_management.py ^
  --wallet-total-usd 1043.86229721 ^
  --wallet-available-usd 380.95770385
```

Ignored outputs are written to
`data/research/pump_live_money_management/`.

## TUT signal verdict

TUT was first observed only as a research-only slow pump. After a normal pump
event became available, it never reached `entry_ready` in the inspected scan
history:

- 2026-08-09 09:00 MSK event: `pump=56.802%`;
- latest known pullback: `47.013%`, funding `+0.459654%`, OI 24h `-5.543%`;
- blocking long/short ratio: `0.2931`, while the live-main band is
  `0.45..0.65`;
- earlier normal-event ratios were approximately `0.2378`, `0.2453`, `0.2751`
  and `0.2931`.

Therefore Pump Live had no valid TUT entry to execute. The disabled-entry state
did not suppress a qualifying TUT signal.

## BLUAI signal reconstruction

The event triggered at 2026-08-08 10:00 MSK with trigger close `0.020255` and
`pump=61.858718%`. The later event high reconstructed from the scanner was
about `0.02750`, consistent with the operator's approximate `0.0277`.

The system correctly waited:

- hours 1–8: long ratio `0.4064..0.4383`, below the live band, while OI 24h was
  `+57.6..+73.6%`, also above the `50%` cap;
- hour 11: ratio entered the band at `0.4594`, but OI remained `+53.88%` and
  pullback was only `3.50%`;
- hour 22: ratio was `0.4537`, but OI was `+92.94%` and pullback only `0.92%`;
- hour 25: all gates aligned: ratio `0.4877`, OI `+3.707%`, funding 24h
  `+0.198665%`, pullback `34.883%`.

That final row produced `entry_candidate -> entry_ready` at last close
`0.017909`; the live L1 filled at average `0.0178077`. This was the intended
`pb25`, five-equal-leg tier, not a late reaction to the old high.

## Why entries became disabled

At the fresh read-only snapshot Pump Live had three open positions
(`1000RATS`, `HEI`, `BLUAI`), total wallet `$1043.862297`, available
`$380.957704`, and confirmed prefund/top-up `$25 + $75 + $35 = $135`.

The fourth live-main signal was HFT at 2026-08-09 12:26:25 MSK. It passed the
strategy gates (`pump=60.105%`, pullback `72.353%`, funding 24h `-0.70864%`,
OI `-9.941%`, ratio `0.639`). Twenty seconds later the old cash guard disarmed
entries with `available_balance_below_new_slot_guard`.

The old guard required `reserve $300 + slot $175 = $475` available. That
double-counted the `$135` already moved from available cash into owned
position prefunds, even though the separate portfolio-rescue guard correctly
counted those top-ups.

The corrected guard requires:

```text
next full slot
+ unused portfolio top-up cap
+ operating cash floor
```

For the snapshot this is `$175 + ($275 - $135) + $25 = $340`. Available cash
was `$380.96`, leaving `$40.96` headroom under the complete cap. The separate
four-position `$50` guarantee check still applies, as do the `$175` per-coin
and `$275` portfolio top-up caps.

## Purpose of the $300 reserve

The reserve is fully allocated risk capacity, not a permanently untouched
cash balance:

- `$200`: four guarantees of `$50` per position;
- `$75`: shared emergency pool for the most stressed positions;
- `$25`: hard operating floor.

Entry prefunds are part of the per-position guarantees. Requiring the full
original `$300` after those prefunds were already added was inconsistent with
this design.

## Four-slot historical replay

The new research runner reuses the exact 35 `main_pullback_tier` portfolio
trades from the existing transition replay and linearly scales the fixed
`$750` historical slot. It also reconstructs concurrent tier-aware entry
prefunds. This remains a survivor-biased research replay, not a live forecast.

| policy | slot | protected capital | free after full protection | historical ROI on $1000 | max DD | peak tier prefund |
|---|---:|---:|---:|---:|---:|---:|
| current | $175 | $1000 | $0 | 173.36% | 3.15% | $140 |
| balanced | $150 | $900 | $100 | 148.59% | 2.70% | $140 |
| conservative | $125 | $800 | $200 | 123.83% | 2.25% | $140 |

Decision for the current open cycle: keep the persisted `$175` geometry. A
mid-cycle global resize would make the controller's aggregate budget disagree
with already-open `$175` ladders. For the next flat-account policy review,
`$150` is the best candidate: it preserves four signals and the same rescue
structure while leaving an additional `$100` unallocated. Promote it only
after a separate replay with live fill/slippage and after all current positions
are flat.

## Main-to-subaccount transfer status

The repository contains the researched Bybit V5 capability map:

- query subaccount balance;
- `universal-transfer` with caller-generated UUID;
- query transfer history and require `SUCCESS`;
- re-read the Pump position before adding isolated margin.

There is no executable Pump transfer controller or API route in the current
source. The configured Pump key is the dedicated subaccount trading key; the
local Pump env contains no separate master transfer credentials. Automatic
transfer therefore remains unimplemented and must not be assumed available.

Safe next design, still requiring separate approval: manual-approved transfer
requests with a master-side key, idempotent UUID, main-account protected-floor
preflight, confirmed `SUCCESS`, Pump wallet re-read, and an exclusion ledger so
rescue deposits cannot be misclassified as strategy profit. Transfers should
be early capacity operations, never the last defence near liquidation.
