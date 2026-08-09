# Pump Capital Rescue Orchestrator

Status: staged implementation, 2026-08-09.

## Objective

Protect Pump positions without treating an arbitrary timer or a fixed main
wallet floor as the capital decision. Every rescue decision must answer two
questions from fresh state:

1. How much cash does Pump require now?
2. Will main remain adequately margined and protected after transferring that
   exact amount?

## Implemented live transfer policy

- Pump reserve is used first.
- Main -> Pump is considered only above the Pump emergency boundary; at or
  below `10%`, the threatened position is closed without waiting for funding.
- Main `GREEN` is not required. `WATCH` may lend when the post-transfer Bybit
  margin ratio remains below `0.75`, available Bybit cash remains at least
  `$500`, all active main accounts remain below the existing `0.80` stress
  ratio, all main positions have verified stops and liquidation buffers of at
  least `25%`, and account data is no older than `180` seconds.
- A transfer is capped at the exact rounded deficit, Bybit transfer-safe cash,
  dynamic main capacity, and `$250` per confirmed incident step.
- `$500` UTC-day use is an alert threshold rather than a hard risk blocker.
- No capital cooldown exists. Confirmed state changes may trigger another
  transfer in the next monitor cycle. A `PENDING` or unknown UUID is the only
  duplicate-submission block.
- Each margin add is immediately verified from the exchange. The old
  five-minute top-up wait no longer suppresses another needed, cap-compliant
  top-up.

The projection uses the existing account monitor fields: Bybit `total`,
`available`, `used`, and `margin_ratio`. For amount `x`:

```text
projected_total = total - x
projected_available = available - x
projected_margin_ratio = used / projected_total
```

This deliberately evaluates the actual effect of `$50/$100/$150`, instead of
assuming every non-GREEN main state is unsafe.

## Profit-harvest shadow

When main cannot lend, `capital_rescue_shadow` ranks other open Pump positions.
The threatened symbol, losing positions, positions inside the Pump warning
band, and positions without confirmed TP/SL are excluded. Ranking currently
favours:

1. smaller remaining distance to TP;
2. larger unrealized profit;
3. larger liquidation buffer.

It reports an estimated `25%`, `50%`, or `100%` reduction and remaining ladder
orders. It does not submit or cancel an order.

## Required next live canary

Automatic donor execution remains prohibited until all of this is implemented
and verified with a separately approved micro-canary:

1. acquire one portfolio rescue lock;
2. cancel and confirm all remaining donor ladder orders;
3. submit a bounded reduce-only partial close;
4. confirm the actual fill rather than requested quantity;
5. refresh quantity, average entry, mark and liquidation price;
6. resynchronize and verify Full TP/SL for the remainder;
7. count only confirmed released available balance;
8. record realized PnL as strategy PnL while keeping transfers as temporary
   principal;
9. re-evaluate the threatened position and stop after the exact deficit is
   covered.

If both main and Pump are stressed and no donor is safe, notification must
state the required external amount and the affected symbols. The threatened
position's exchange stop and emergency reduce-only close always remain the
last authoritative boundary.
