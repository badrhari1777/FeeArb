# Gate WS Notes (Futures v4)

Scope
- Futures private WS for orders/positions/usertrades.
- Manual execution places orders via REST; WS provides updates.

Endpoint
- `wss://fx-ws.gateio.ws/v4/ws/usdt`

Auth and Signatures
- Private subscriptions: `auth` signature `channel=<channel>&event=<event>&time=<time>`.
- Trading API calls (`event: "api"`): signature string `api\n<channel>\n<req_param>\n<timestamp>`.
- Login call must be sent before order place/cancel:
  - `channel: "futures.login"`, `event: "api"`,
  - payload includes `api_key`, `signature`, `timestamp`, `req_id`, `request_param: ""`, `headers: {}`.

Subscriptions
- Orders: `channel: "futures.orders"`, `payload: ["<CONTRACT>"]`.
- Positions: `channel: "futures.positions"`, `payload: ["<CONTRACT>"]`.
- Usertrades: `channel: "futures.usertrades"`, `payload: ["<CONTRACT>"]`.
- Use `!all` for all contracts when needed.

Symbol Format
- Use `BASE_USDT` (underscore). Example: `RIVER_USDT`.
