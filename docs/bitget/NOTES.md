# Bitget WS Notes

Scope
- Futures private WS for orders/positions/account updates.
- Manual trading places/cancels orders via REST; WS is used for real-time updates.

Endpoints (server-side raw streams)
- Private stream: `wss://ws.bitget.com/v2/ws/private` (used by `/ws/trade-bitget-raw`).
- Trade stream for order/cancel tests: `/ws/trade-bitget-trade-raw` (VIP required for WS order placement).

Auth
- Login signature: `timestamp + "GET" + "/user/verify"` signed with HMAC-SHA256, base64-encoded.
- Requires `apiKey`, `apiSecret`, `passphrase`.

Subscriptions (private)
- `op: "subscribe"`, `args: [{ instType: "USDT-FUTURES", channel: "orders", instId: "<symbol>" }]`
- `instId: "default"` did not yield order updates in manual runs; use explicit symbols.
- Optional channels: `positions`, `fill`, `account`.

Symbol Format
- Use `BASEUSDT` (no separators). Example: `RIVERUSDT`.

Operational Notes
- WS order placement requires VIP; do not rely on WS for production order create/cancel.
- Use REST for orders; WS for fills/position updates.
