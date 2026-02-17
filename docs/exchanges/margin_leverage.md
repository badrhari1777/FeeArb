# Margin & Leverage Notes

Purpose
Record per-exchange observations for isolated margin add/reduce and leverage behavior.

Bybit
- Isolated reduce margin uses `/v5/position/add-margin` with a negative `margin` value.
- UI "Max reduction" matches `positionBalance - positionIMByMp` (or `positionIM`).
- Empirical result: when `positionBalance == positionIM`, withdrawable margin is 0; after adding 200, withdrawable becomes 200.
- For tests, estimate withdrawable as `max(0, positionBalance - positionValue/leverage * (1 + 1% buffer))`.
- Confirmed via manual tests: add margin, reduce margin, and set leverage all succeed on Bybit with isolated positions.
- Key fixes for production reference:
  - Margin mode: infer isolated when `positionBalance` ~= `positionIM` (tradeMode can be `0` even for isolated).
  - Reduce margin: use `/v5/position/add-margin` with negative `margin` (no `reduce_margin` endpoint).
  - Max reduce estimate: use the simple min-required formula above (avoid equity/MM math for Bybit).

Bitget
- UI "Can decrease by" aligns with `base_margin - positionValue/leverage * (1 + buffer)`.
- Current buffer: 1% (aligned with Bybit for safety).
- Confirmed via manual tests: add margin, reduce margin, set leverage all work with isolated positions.
- Note: Manual tests UI showed HTTP 500 on set leverage while Bitget UI reflected leverage change; capture server error/logs if it repeats.

BingX
- Margin add/reduce requires `positionId` and `positionSide` (LONG/SHORT); pass them from the raw position payload.
- Reduce estimate uses `info.margin` as base and `info.maxMarginReduction` as max withdrawable (matches UI).

Kucoin
- For tests, estimate withdrawable margin to reach 3x as:
  - `target_margin = positionValue/3 * (1 + 0.15% buffer)`
  - `max_reduce = base_margin - target_margin`
- Buffer estimate: ~0.15% (AXSUSDT isolated 3x, margin 376.25 -> after 281.65).
- Account monitor enforces leverage on isolated positions by adjusting margin toward the same 3x target (not via `set_leverage`).
- Manual order placement now passes `leverage=3` directly in the order payload alongside `marginMode`/`marginType` (skip `set_leverage`/`set_margin_mode` for Kucoin in manual flow).
- Default one-way mode uses `positionSide=BOTH` in Kucoin order params.
- Reduce margin uses `POST /api/v1/margin/withdrawMargin` (ccxt lacks reduceMargin; call via raw request).
- Requires extra API permissions; `400007 Access denied` usually means the key lacks transfer/withdraw permission.

MEXC
- Not tested yet (margin reduction behavior unknown).

OKX
- OKX margin adjust uses account endpoint with `posSide` and is sensitive to one-way vs hedge mode.
- Ensure `posSide` uses raw `info.posSide` when available (`net` for one-way); sending `long/short` in net mode can yield "Position does not exist".
- Withdrawable margin aligns with `position_margin - initial_margin` (buffer currently set to 0%).

Gate.io
- Reduce estimate uses Gate `info.margin` as base and `info.initial_margin` with a 3% buffer for min required.
- Tier limits/extra caps not verified; treat estimates as conservative.
