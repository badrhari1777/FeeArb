# Manual Execution Notes

Scope
- Manual enter/exit/roll execution in `execution/manual.py`.
- Supports smart (limit + hedge control) and fast (market) modes.

Key Behavior
- Orders are placed via REST (ccxt); real-time fills come from WS order streams when live.
- WS positions are used only at start/end for reconciliation; per-chunk position checks are avoided.
- Primary fill tracking uses WS order updates; REST fallback only when WS is stale.
- Hedge control uses WS order updates; market fallback is triggered on adverse price moves.
- When WS is live but missing an order update, REST status is skipped to avoid false fills.
- Smart (limit) modes run chunks sequentially (primary fill -> hedge -> next chunk); new limits wait for cancel ack.
- Limit pricing filters out small levels (default 1% of chunk notional/qty) and improves 1 tick over best non-self level.

Chunk / Fallback Rules
- Per-chunk market fallback threshold is `min_qty_required * 1.15`.
- Final reconcile always runs (REST positions + market fallback if needed).

Fast (Market) Mode
- Market chunks are gated by orderbook refill:
  - `market_refill_bps`, `market_refill_buffer`, `market_refill_max_wait_sec`.
- Waits for WS fills before sending next chunk when WS is live.

Logging
- Execution log uses structured stages (`start`, `submit`, `result`, `warn`, etc.).
- Action summaries now include per-action timestamps (`ts`).
