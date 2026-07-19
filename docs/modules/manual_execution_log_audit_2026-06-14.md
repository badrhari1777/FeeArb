# Manual execution log and performance audit

Date: 2026-06-14

## Implementation status

The first corrective pass was deployed on 2026-06-14:

- exact normalized symbol matching;
- reversed spread-range validation;
- 30-second default no-order trigger wait;
- liquidity-capped requested/forced chunks;
- per-execution margin/leverage setup cache;
- Bybit leverage no-op classification;
- aggressive guarded auto-exit hedge defaults;
- reduced heartbeat/duplicate-fill logging;
- terminal execution summaries;
- simplified Manual UI with expandable advanced and exchange settings.

Adaptive repricing and full per-chunk realized spread/fee accounting remain later
optimization work after collecting a clean post-change log sample.

## Scope

The audit covers the 25 newest files in `logs/manual_exec/`, with detailed timing
analysis of 15 fully completed executions. No live order was submitted during the
audit.

Goals:

1. Reconstruct the full course of each transaction from logs.
2. Identify avoidable execution latency.
3. Review payload defaults, chunk sizing, repricing, and safeguards.
4. Reduce noise without losing the audit trail.

## Main conclusion

Exchange request latency is not the main bottleneck. Median order submission
acknowledgement is below one second, and median delay from a primary fill to
hedge submission is about 0.56 seconds.

Most execution time is spent waiting for passive limit fills, canceling and
replacing orders, or occupying the only worker while a spread condition is not
met. Larger forced chunks do not reliably make execution faster: they bypass the
calculated 8 bps liquidity cap and produce partial fills, repeated repricing, and
final market reconciliation.

The current safeguards should not be removed wholesale. Position, balance,
market-constraint, dust, and orderbook checks are useful. The improvements should
cache repeated setup, validate contradictory payloads before queue admission,
and make waiting/repricing event-driven.

## Measured results

Across the 25 newest execution logs:

- 4,817 log records.
- 1,573 heartbeat ping/pong records, 32.7% of all records.
- 834 orderbook snapshots and 791 repeated wait records, another 33.7%.
- 131 chunk decisions.
- 46 chunk decisions exceeded the calculated `max_chunk` at the configured
  slippage limit. Every one occurred with `force_chunk_qty=true`.
- 61 duplicate WS fill records were written in addition to the main fill record.
- 30 expected Bybit `110043 leverage not modified` responses were logged as
  warnings.
- 7 runs required a final market order to reconcile the legs; one additional
  auto-exit wanted reconciliation but the tier guard correctly blocked it.

For 15 fully completed executions:

- Median total duration: 83.1 seconds.
- Mean total duration: 158.2 seconds.
- Median setup time before execution start: 8.0 seconds.
- Median start-to-first-submit: 0.94 seconds.
- Median first-submit-to-first-fill: 11.2 seconds.
- Median primary-fill-to-hedge-submit: 0.56 seconds.

Order preparation through exchange acknowledgement:

| Exchange | Median |
| --- | ---: |
| Bybit | 0.34 s |
| Binance | 0.36 s |
| KuCoin | 0.60 s |
| OKX | 0.74 s |

## Representative runs

### Efficient baseline

`d05de5b07703`, ESPORTS exit 6,250:

- Completed in 29.5 seconds.
- One main chunk plus hedge/dust work.
- Shows that the submit and hedge paths can be fast when the primary order fills.

### Normal automatic exit

`9248fa80e62f`, HOME exit 30,555:

- Completed the main execution in about 145 seconds using four dynamic chunks.
- Chunk notional cap was 250 USDT.
- Left about 507 HOME on one leg; market cleanup was blocked by the auto-exit
  tier guard.
- The chunk count was reasonable. The main improvement is faster hedge/final
  residual handling and clearer terminal reporting, not simply larger chunks.

### Slow forced execution

`f60f43d4cb43`, SIREN enter 25,000:

- Duration 748 seconds.
- 31 submitted orders and 17 cancellations.
- Requested chunk was 5,000 with `force_chunk_qty=true`.
- In 19 of 38 chunk decisions, 5,000 exceeded the calculated liquidity cap,
  sometimes by more than 30 times.
- Finished with a 521-unit market reconciliation.

This is the clearest example that forced sizing plus passive repricing can be
slower than liquidity-aware sizing.

### Worker occupied without orders

`73bf4ac09eef`, H exit:

- Payload had `spread_min_pct=-10` and `spread_max_pct=-100`.
- The range is impossible under the current comparison logic.
- The run occupied the worker for 349 seconds, wrote 66 snapshots and 66 wait
  records, and submitted no order.

`1cac7f59a70e` repeated the same class of issue with an allowed range of
`[-10, 100]` while the live spread was around `-13%`.

### Incorrect position matching

Recent H exits included the unrelated `HOMEUSDT` Bybit position. For example,
the H precheck reported 50,000 short although the real H short was 10,000 and
the other 40,000 belonged to HOME.

Cause: `_symbol_matches()` uses prefix matching when the requested symbol has no
quote suffix. `H` therefore matches `HOMEUSDT`. This can corrupt exit sizing,
reconciliation, safeguards, and logs.

## Required fixes before tuning from new statistics

### P0: correctness and worker availability

1. Replace symbol prefix matching with exact normalized base-asset matching.
   `H` must never match `HOME`.
2. Reject a spread range when both bounds exist and `min > max`.
3. Do not admit a spread-waiting job to the sole execution worker for a long
   runtime. A job that has placed no order should yield after 15-30 seconds and
   return to the strategy queue as `condition_not_met`.
4. Redefine `force_chunk_qty`. A requested chunk should normally be a target or
   upper bound and still be clamped to the live slippage cap. If bypassing
   liquidity is ever needed, it must be a separate dangerous option.
5. Reject contradictory effective payloads such as
   `max_slippage_bps=8`, `use_orderbook_check=false`, and
   `force_chunk_qty=true`, unless an explicitly armed override is used.

### P1: faster execution

1. Apply margin mode and leverage once per execution and cache the result for
   `(exchange, symbol, mode, leverage)`. Do not repeat it for every chunk.
2. Treat Bybit `110043 leverage not modified` as an informational no-op.
3. Replace fixed cancel/reprice with adaptive repricing:
   - keep an order when it remains competitive;
   - reprice only after a material price move or fill stagnation;
   - wait for WS cancel acknowledgement for up to 0.5-1 second, then use REST;
   - avoid an unconditional one-second sleep after every cancellation.
4. Keep the existing fast hedge-submit path. For exits, default the hedge to an
   aggressive limit with a short deadline; allow market escalation only within
   the existing tier/notional guard.
5. Parallelize independent initial reads and reuse fresh account/market data,
   but do not remove position, balance, constraints, dust, or orderbook checks.

## Recommended payload defaults

These values are a conservative starting profile, not final exchange-specific
optimization:

| Parameter | Manual enter | Manual exit | Auto exit |
| --- | ---: | ---: | ---: |
| `max_runtime_sec` | 180 | 180 | 120 |
| no-order spread wait | 30 s | 30 s | 15-30 s |
| `max_slippage_bps` | 8 | 8 | 8 |
| `use_orderbook_check` | true | true | true |
| `force_chunk_qty` | false | false | false |
| primary reprice | adaptive, 5 s floor | adaptive, 4-5 s floor | adaptive, 4-5 s floor |
| hedge mode | passive/adaptive | aggressive limit | aggressive limit |
| hedge reprice deadline | 4-6 s | 2-3 s | 2-3 s |
| market fallback | off | guarded | tier/notional guarded |

Chunk selection should use:

`min(remaining, dry-run/live max qty at 8 bps, venue-tier notional cap / price)`.

Suggested target notionals remain:

- liquid venue pair: 500-750 USDT;
- medium liquidity: 250-500 USDT;
- weak liquidity: 100-250 USDT.

The target must never override the live liquidity cap. The logs do not support a
general conclusion that chunks are too small. Slow runs were more strongly
associated with forced oversizing, passive fills, and cancellation churn.

## Logging redesign

### Preserve

- raw requested payload;
- resolved/effective payload after defaults and strategy overrides;
- preflight balances, positions, constraints, and selected legs;
- each order ID, side, quantity, price, reduce-only flag, and exchange response;
- fills, cancellations, reconciliation, warnings, and errors.

### Add

1. A mandatory `execution_summary` terminal record containing:
   - requested, filled, and remaining quantity for each leg;
   - average fill prices;
   - realized execution spread and estimated fees;
   - slippage against trigger/reference;
   - orders, cancellations, and reprices;
   - final imbalance/dust;
   - duration by phase;
   - exact terminal reason.
2. One `chunk_summary` per completed chunk with primary and hedge fill duration,
   prices, actual spread, fees, cumulative fill, and remaining quantity.
3. Stable structured fields: `execution_id`, `sequence`, `elapsed_ms`, `phase`,
   `chunk_id`, `leg`, `order_id`, and `reason_code`.
4. Explicit gate diagnostics: live spread, allowed condition, distance to
   trigger, and time spent waiting.

### Reduce

1. Log healthy heartbeat ping/pong only on health-state transitions or in a
   diagnostic stream.
2. Write snapshots on first observation, material spread/liquidity change,
   before an action, every 30 seconds while waiting, and at completion.
3. Aggregate repeated waits into `waiting_started`, periodic summary, and
   `waiting_ended`.
4. Merge duplicate primary fill and WS fill records into one fill event with a
   `source` field.
5. Log expected leverage no-ops once.
6. Keep unrelated symbols out of position snapshots.
7. Store the full WS health profile once by profile/version rather than copying
   it into every payload.

The preferred long-term format is immutable structured JSONL for audit and
analysis, with a compact human-readable story rendered in the UI. Full audit
information remains available without making the operator log unreadable.

## Implementation order

1. Exact symbol matching and spread-range validation.
2. Worker yield for unmet triggers.
3. Safe chunk semantics and effective-payload validation.
4. Terminal/chunk summaries plus log sampling.
5. Leverage/margin setup cache and no-op classification.
6. Adaptive primary repricing and faster guarded exit hedge.
7. Collect a new sample and compare fill time, cancellation rate, final
   reconciliation rate, realized spread, and residual dust.
