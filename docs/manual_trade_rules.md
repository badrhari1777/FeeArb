# Manual Live Trade Rules (WS-first)

Scope
- Applies to manual enter/exit/roll execution in `execution/manual.py`.
- WS order/trade updates are the primary source of truth for fills and hedge synchronization.

Sizing Caps
- The Manual web form accepts either a base-quantity cap, a per-leg USDT cap,
  or both.
- When both are present, the final base quantity is the smaller of the explicit
  quantity and the USDT-implied quantity. Neither cap is silently preferred or
  ignored.
- The USDT conversion uses the highest valid current ticker reference across
  both selected venues. If either venue reference is unavailable, planning
  fails closed before orderbook checks or order submission.
- Dry-run returns the requested caps, selected cap, selected base quantity, and
  per-venue/reference prices under `sizing`.

Order Fill Tracking (WS)
- Track fills from private order/trade streams in `execution/ws_orders.py`.
- Use filled_qty per order_id; clamp to expected qty when needed.
- WS health is based on heartbeat (ping/pong or any WS traffic), not just order updates.
- If order updates are quiet, probe heartbeat and attempt reconnect before declaring WS dead.
- REST order status is used only after WS is confirmed dead; fetch is order-id specific (trade-based fallback is disabled for primary fills).

Position Use (Entry/Exit)
- Positions are fetched only at entry (start snapshot) and exit (final reconcile).
- Final reconcile compares primary vs hedge deltas and uses market fallback when imbalance >= fallback_min.
- A 100% pair exit targets zero on both legs. A pre-existing mismatch on the
  larger leg is closed only when its residual is below the configured dust
  notional or exchange minimum; a material orphan is not closed silently.
- After execution, fresh trusted position scans gate per-exchange cancellation
  of obsolete protective orders. A periodic 15-minute sweep applies the same
  trusted-absence rule to protection left after direct exchange-side closes.
- Sweep discovery filters by the returned exchange symbol and never treats an
  explicitly non-reduce-only conditional entry as protective. Canonical and
  venue IDs are resolved to a CCXT unified symbol before reading or cancelling.

Smart (Limit) Mode
- On enter, the primary leg is a passive `post-only` maker order; immediate
  taker depth on that venue is diagnostic and does not block the entry.
- Auto hint normally chooses the thinner venue as primary, but swaps the roles
  when the proposed hedge venue cannot safely execute the minimum chunk.
- Chunk capacity is limited by urgent taker liquidity on the hedge venue plus
  the exchange-tier unhedged-notional cap, not by primary taker depth.
- Execute one chunk at a time: any primary fill cancels the primary remainder,
  confirms cancellation, and immediately hedges exactly the filled quantity.
- Smart-enter limit hedges are forced to aggressive mode. If they remain open
  for `hedge_timeout_sec` (default 5 seconds), cancel and market-fallback the
  remaining hedge quantity.
- Limit prices use filtered orderbook levels (ignore thin "junk" below min-level thresholds; default 1% of chunk notional/qty).
- Active order size is excluded from best-level checks to avoid self-outbidding.
- Reprice, timeout, runtime end, and partial-fill handling all require confirmed
  primary cancellation before placing another primary order or finishing.
- Runtime expiry with zero fills is a normal `completed_no_fill` result, not a
  liquidity error. Partial or unhedged exposure remains an error state.

Chunk Reconciliation (market fallback)
- fallback_min = min_qty_required * 1.15, rounded up to amount step.

Market Mode (Fast Enter/Exit)
- Market chunks are sent simultaneously on both legs.
- Fills are confirmed via WS order/trade updates; REST is only used when WS is stale or missing.
- After each chunk, wait for orderbook refill before the next chunk:
  - Sum liquidity within `market_refill_bps` from best bid/ask for each leg.
  - Require liquidity >= chunk * (1 + market_refill_buffer) and a newer orderbook timestamp.
  - If refill times out, reduce chunk to available liquidity (down to amount step); stop if below min.
- Market fill timeout default: `market_fill_timeout_sec = 3`.
- Refill defaults: `market_refill_bps = 10`, `market_refill_buffer = 0.15`, `market_refill_max_wait_sec = 5`.

Hedge Timing + Price Moves
- Hedge adverse move threshold default: 0.1% (10 bps).
- Adverse moves are measured from orderbook best bid/ask (WS orderbook source).
- Smart-enter hedge hard deadline default: 5 seconds (configurable 1..30).

REST vs WS
- Order placement/cancel via ccxt REST.
- WS order/trade updates for fills and order status.
- WS positions only for start/end snapshots; REST positions only if WS is stale.
- When WS is stale, reconnect attempts run before any REST fallback.
