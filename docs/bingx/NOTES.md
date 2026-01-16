# BingX Swap WS Notes

Scope
- Swap (USDT-M) user data stream only.
- Docs source: `docs/bingx/swap/*.html`.

User Stream (ListenKey)
- WS URL: `wss://open-api-swap.bingx.com/swap-market?listenKey=<key>`.
- No subscriptions required; all user events are pushed after connect.
- Ping/Pong: server sends literal `Ping` (string); client must reply literal `Pong`.
- Ping cadence can exceed 15s; order-stream stale thresholds must be higher.

ListenKey Lifecycle (REST)
- Create: `POST /openApi/user/auth/userDataStream` with `X-BX-APIKEY` header.
- Extend: `PUT /openApi/user/auth/userDataStream?listenKey=...` (recommended every ~30m).
- Close: `DELETE /openApi/user/auth/userDataStream?listenKey=...`.
- Signature: docs indicate signature verification; current implementation signs query with
  HMAC-SHA256 when `api_secret` is available (adds `timestamp`).

Observed Events (Swap User Stream)
- `SNAPSHOT`: large list of per-symbol account config entries on connect.
  - Example: `{ "e": "SNAPSHOT", "ac": { "s": "RIVER-USDT", "l": 3, "S": 3, "mt": "isolated" } }`.
- `ORDER_TRADE_UPDATE`: order lifecycle updates (NEW / FILLED / CANCELED).
- `TRADE_UPDATE`: appears alongside fills; treat as optional duplicate of trade info.
- `ACCOUNT_UPDATE`: balance/position changes after trades.

Key Fields (from observed payloads)
- `o.s`: symbol (swap uses hyphen, e.g. `RIVER-USDT`).
- `o.S`: side (`BUY` / `SELL`).
- `o.o`: order type (`LIMIT` / `MARKET`).
- `o.X`: order status (`NEW`, `FILLED`, `CANCELED`).
- `o.x`: execution type (`TRADE`, etc.).
- `o.ap`: avg price; `o.z`: cum filled qty.
- `o.ps`: position side (`BOTH`).
- `o.ro`: reduce-only flag.
- `a.B`: balances; `a.P`: positions in `ACCOUNT_UPDATE`.

Manual Tests Validation
- REST orders (`/api/manual/test/limit|market|cancel`) produce `ORDER_TRADE_UPDATE`,
  `TRADE_UPDATE`, and `ACCOUNT_UPDATE` on the swap user stream.
- Initial SNAPSHOT flood is expected; filter by `e`/symbol in UI if needed.

Execution Note
- Treat WS order updates as source of truth; REST `fetch_order` can lag/misreport and
  cause double-hedge if a limit order remains open while a market fallback fires.
