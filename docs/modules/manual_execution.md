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
- A requested/forced chunk is always capped by live liquidity at the configured
  slippage limit.
- Final reconcile runs after an execution has started; it is skipped when the
  spread condition was never met and no order was submitted.
- Manual clients expose one spread threshold. Enter maps to
  `spread_max_pct` (`spread <= target`), Exit maps to `spread_min_pct`
  (`spread >= target`), and Roll lets the operator choose either direction.
- The backend retains two-bound range support for internal callers and rejects
  reversed ranges when both bounds are supplied.
- A spread-guided run releases the shared worker after `trigger_wait_sec`
  (default 30 seconds) if no first order was submitted.
- Position matching uses exact normalized base assets. A short symbol such as
  `H` cannot match `HOME`.

Fast (Market) Mode
- Market chunks are gated by orderbook refill:
  - `market_refill_bps`, `market_refill_buffer`, `market_refill_max_wait_sec`.
- Waits for WS fills before sending next chunk when WS is live.

Logging
- Execution log uses structured stages (`start`, `submit`, `result`, `warn`, etc.).
- Action summaries now include per-action timestamps (`ts`).
- Healthy WS probe ping/pong and duplicate WS fill records are omitted.
- Every async run ends with an `execution summary` record containing requested
  and remaining quantity, fills by exchange, order/cancel counts, duration, and
  terminal reason.

Manual UI
- The main row contains action, symbol, quantity, venues, mode, one spread
  threshold, and chunk quantity.
- Execution/hedge controls and per-exchange WS controls are collapsed under
  separate expandable sections.
- Desktop Manual, mobile web, and Android use the same threshold semantics.
