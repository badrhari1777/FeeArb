# Manual Live Trade Rules (WS-first)

Scope
- Applies to manual enter/exit/roll execution in `execution/manual.py`.
- WS order/trade updates are the primary source of truth for fills and hedge synchronization.

Order Fill Tracking (WS)
- Track fills from private order/trade streams in `execution/ws_orders.py`.
- Use filled_qty per order_id; clamp to expected qty when needed.
- WS health is based on heartbeat (ping/pong or any WS traffic), not just order updates.
- If order updates are quiet, probe heartbeat and attempt reconnect before declaring WS dead.
- REST order status is used only after WS is confirmed dead; fetch is order-id specific (trade-based fallback is disabled for primary fills).

Position Use (Entry/Exit)
- Positions are fetched only at entry (start snapshot) and exit (final reconcile).
- Final reconcile compares primary vs hedge deltas and uses market fallback when imbalance >= fallback_min.

Smart (Limit) Mode
- Execute one chunk at a time: primary leg fills -> hedge leg executes; next chunk waits for hedge (unless residual < fallback_min).
- Limit prices use filtered orderbook levels (ignore thin "junk" below min-level thresholds; default 1% of chunk notional/qty).
- Active order size is excluded from best-level checks to avoid self-outbidding.
- Reprice waits for cancel confirmation before placing a new limit order.

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

REST vs WS
- Order placement/cancel via ccxt REST.
- WS order/trade updates for fills and order status.
- WS positions only for start/end snapshots; REST positions only if WS is stale.
- When WS is stale, reconnect attempts run before any REST fallback.
