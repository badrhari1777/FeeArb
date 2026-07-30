# Pump Live Margin Management Research

## Scope

This is a deterministic risk/capital study for the `$1000` Pump Live Bybit
subaccount. It does not change or authorize live execution.

Reproduce the calculation with:

```text
.venv\Scripts\python.exe scripts\pump_live_margin_stress.py
```

Generated CSV/JSON artifacts are written under
`data/research/pump_live_margin_stress/`.

## Formula and assumptions

The model uses the current Bybit isolated USDT-short liquidation formula:

```text
liq =
  (entry * qty
   + entry * qty / leverage
   + added_margin / (1 + taker_fee)
   + maintenance_margin_deduction)
  / (qty * (1 + maintenance_margin_rate))

exchange_stop = liq * (1 - 2.5%)
```

Assumptions match the current Pump Live canary:

- capital `$1000`;
- four positions;
- current trade-margin capacity `$175` per position (`$700` total);
- isolated `3x`;
- conservative MMR `2.5%`;
- taker fee `0.055%`;
- stop `2.5%` inside liquidation;
- operating cash floor `$25`;
- maximum portfolio top-up `$275`.

Actual exchange liquidation price remains authoritative because the symbol risk
tier, maintenance deduction, fees, fills, and rounding can differ.

## BANK exact comparison

Captured BANK geometry:

- first short `1010 @ 0.17180881`;
- second ladder `1350 @ 0.25766`;
- combined position after L2 approximately `2360 @ 0.22091860`.

| Upfront margin | Stop vs L2 | L2 protected | Loss if stopped after L2 | Free with four positions |
|---:|---:|:---:|---:|---:|
| `$0` | `-15.43%` | no | `$139.88` theoretical | `$300` |
| `$25` | `-6.30%` | no | `$163.65` theoretical | `$200` |
| `$45` | `+1.01%` | yes, thin | `$182.66` | `$120` |
| `$50` | `+2.84%` | yes | `$187.41` | `$100` |
| `$60` | `+6.49%` | yes | `$196.92` | `$60` |
| `$75` | `+11.97%` | yes | `$211.18` | `$0` |
| `$100` | `+21.10%` | yes | `$234.95` | `-$100` |

The exact BANK minimum is about `$42.24`. `$45` technically clears L2 but has
only about `1%` room. `$50` is the smallest practical current-size value.
Adding more than `$50` keeps moving liquidation away, but also moves the
catastrophic stop away and increases the realized loss if the stop is hit.
Therefore "more margin" is not unconditionally lower risk.

## Protecting every next ladder

The table below targets an exchange stop at least `2.5%` above the next ladder
and rounds required top-up to `$5`.

| Tier | Position filled through | Next ladder | Total added margin required | Fits `$175` coin cap | Fits portfolio with three other `$50` guarantees |
|---|---:|---:|---:|:---:|:---:|
| ordinary `<80%`, 5 equal | L1 | L2 | `$30` | yes | yes |
| ordinary `<80%`, 5 equal | L2 | L3 | `$100` | yes | yes |
| ordinary `<80%`, 5 equal | L3 | L4 | `$195` | no | no |
| ordinary `<80%`, 5 equal | L4 | L5 | `$315` | no | no |
| strong `80–100%`, 2 tapered | L1 | L2 | `$50` | yes | yes |
| strong `100–250%`, 3 tapered | L1 | L2 | `$25` | yes | yes |
| strong `100–250%`, 3 tapered | L2 | L3 | `$95` | yes | yes |
| super `>=250%`, 2 tapered | L1 | L2 | `$50` | yes | yes |

Consequences:

- every current tier can safely expose L2 inside the existing guaranteed `$50`;
- only one position at a time should consume the shared pool to expose L3:
  ordinary needs about `$100` total added margin and strong `100–250%` needs
  about `$95`;
- ordinary L4/L5 cannot be made continuously safe while preserving four
  position guarantees and the `$25` floor on a `$1000` account;
- deeper add orders must therefore be gated/cancelled when their protection
  budget is unavailable, rather than relying on a later top-up.

In the 39 current-main-gated historical windows, L2 was reached in `5` cases.
Only one case reached the L3 region; it was the strong `100–250%` HUSDT case.
No ordinary `<80%` case reached L3. This supports treating deep-stage capacity
as a scarce shared risk unit rather than reserving it for every position.

## Size and reserve alternatives

The two-leg tapered tier is the tightest first-to-second-ladder case.

| Policy | Trade margin per coin | Upfront protection | Four-position commitment | Free capital | Post-L2 stop loss |
|---|---:|---:|---:|---:|---:|
| current dynamic | `$175` | `$0` | `$700` | `$300` | L2 depends on monitor top-up |
| same size, prefund | `$175` | `$50` | `$900` | `$100` | `$188.39` |
| same size, wider | `$175` | `$60` | `$940` | `$60` | `$197.90` |
| balanced rebudget | `$150` | `$50` | `$800` | `$200` | `$168.27` |
| conservative rebudget | `$125` | `$50` | `$700` | `$300` | `$148.14` |

Reducing trade margin reduces both upside and downside approximately
proportionally. It does not change signal selection, but it does change the
live portfolio return relative to the historical `$175`-slot replay.

## Best mathematical candidates

### Keep current trade size

Use actual position/order geometry to set the stop target `2.5%` above L2,
round the required top-up upward to `$5`, add it before exposing L2, and verify
the refreshed Bybit liquidation and stop. Approximate first-stage amounts are:

- ordinary `<80%`: `$30`;
- strong `80–100%`: `$50`;
- strong `100–250%`: `$25`;
- super `>=250%`: `$50`.

The historical gated tier mix (`15 / 4 / 12 / 8`) gives a simple average of
about `$34.62` pre-funded per position. Four positions at that mix would leave
about `$161.54`; the worst four-position tier mix still leaves `$100`.

This dominates flat `$50` on capital and stop-loss distance for ordinary and
strong `100–250%` entries, but is slightly more complex.

### Balanced first-live profile

Use `$150` trade margin plus dynamic first-stage protection:

- `$30 / $50 / $25 / $50` by the four tiers;
- at least `5%` stop clearance above L2 for each normalized tier;
- worst four-position commitment `$800`, leaving `$200`;
- roughly `14.3%` lower position exposure than the current `$175` slot;
- two-leg post-L2 catastrophic loss falls from about `$188.39` to `$168.27`.

This is the best risk/free-capital compromise for a `$1000` canary if lower
growth is acceptable.

## Approved margin-only implementation

On 2026-07-30 the operator approved the margin-control part while explicitly
keeping the trading strategy unchanged:

- trade margin remains `$175` per slot;
- tier selection, ladder count, prices, weights, TP, and hold time are
  unchanged;
- all configured 2/3/5 ladder legs remain part of the live strategy;
- after the actual L1 fill, Pump Live calculates the minimum added isolated
  margin required to put the exchange stop `2.5%` above the actual L2 price;
- the amount is rounded upward to `$5`, added through the isolated-position
  margin API, then verified from a fresh Bybit position read;
- the verified entry prefund becomes a non-removable bot-margin floor;
- later warning/panic top-ups continue above that floor, and only the excess
  can be returned by the existing safe-removal hysteresis;
- normal execution does not gate, cancel, resize, or remove L3–L5 based on the
  margin study.

If the entry prefund cannot be added or confirmed, the already-filled L1 keeps
its exchange TP/SL, new entries are disarmed, and remaining ladders are not
submitted under uncertain protection. This is an execution fail-safe, not a
strategy rule.

Automatic transfer from the master account is deliberately separate and is
not implemented by this change. Until it exists, the existing per-position,
portfolio, guaranteed-reserve, shared-emergency, and hard-floor limits remain
authoritative.

## Implemented margin state machine

1. Calculate required margin from the actual first fill, actual next order,
   current Bybit risk tier, and current exchange liquidation.
2. Add only enough margin to place the stop above the next ladder by the chosen
   safety percentage; round upward and cap it.
3. Read the position again and refresh TP/SL.
4. Submit every remaining ladder from the selected strategy without changing
   its count, price, weight, or notional.
5. Continue warning/panic top-ups under the existing reserve caps.
6. Continue removing only Pump-added excess margin after the existing recovery
   confirmations and cooldown.
7. Never remove the confirmed entry-prefund floor while the position is open.

The next research step is a historical PnL replay comparing:

- current on-demand `$175`;
- tier-aware `$175`;
- balanced `$150`;
- conservative `$125`;
- L3 allowed versus L3 gated.

That replay remains useful for future sizing decisions, but the approved live
implementation keeps current `$175` sizing and every strategy ladder.
