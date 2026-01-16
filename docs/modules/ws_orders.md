# LiveOrderTracker Notes

Scope
- Live order updates from exchange private WS streams.
- Implemented in `execution/ws_orders.py`.

Behavior
- `LiveOrderTracker.ensure()` starts per-exchange streams.
- Orders are tracked by `order_id` with `filled_qty`, `status`, and `symbol`.
- `is_healthy()` / `is_live()` are based on last WS traffic (heartbeat or data), not just order updates.

Staleness
- Manual execution checks per-exchange heartbeat timeouts before treating WS as dead.
- Silence on the order stream triggers ping/pong probes and reconnect attempts before REST fallback.
- BingX streams can have long gaps; tracker marks live on any event type and relies on larger timeouts.
- Health defaults live in `DEFAULT_WS_ORDER_HEALTH` (overridable via manual payload).
- BingX heartbeat diagnostics are logged to `logs/app.log`:
  `bingx order ws heartbeat: reason=<...> gap=<seconds>`.

Exchange Streams
- Bybit: `order` + `execution` topics.
- OKX: `orders` channel.
- Gate: `futures.orders` (private subscription).
- Bitget: `orders` channel (subscribe per-symbol `instId`, e.g. `ZKPUSDT`).
- Kucoin: `/contractMarket/tradeOrders`.
- BingX: swap user stream (`ORDER_TRADE_UPDATE` / `TRADE_UPDATE`).
