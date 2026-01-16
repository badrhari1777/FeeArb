# Kucoin Classic Futures WS Notes

Scope
- Classic futures private WS for order updates and positions.
- Manual execution uses REST for order create/cancel; WS used for updates.

Auth / Endpoint
- Fetch private token: `POST /api/v1/bullet-private`.
- Connect to returned `endpoint?token=...`.

Topics
- Orders: `/contractMarket/tradeOrders` or `/contractMarket/tradeOrders:<SYMBOL>`.
- Positions: `/contract/positionAll` or `/contract/position:<SYMBOL>`.
- Wallet: `/contractAccount/wallet`.

Symbol Format
- `BASEUSDTM` (BTC -> XBT), example: `RIVERUSDTM`.

Operational Notes
- Trade orders stream is sufficient for fills; classic docs do not list `tradeFills`.
- WS updates show `marginMode: ISOLATED` and `positionSide: BOTH` for fills.
