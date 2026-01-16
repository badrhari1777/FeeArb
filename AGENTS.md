# FeeArb Agent Notes

Purpose
Maintain a durable, concise project brain-dump so new sessions can resume quickly.
Update this file after each meaningful change or decision.

Project Overview
- FeeArb is an async research + execution stack for cross-exchange funding arbitrage.
- Core layers: pipeline (data), orchestrator (decisions), execution (orders/positions), webapp (FastAPI + WS UI).
- Default active venues in docs are Bybit and MEXC; adapters for more exist but are not all active.
- Execution layer is mostly simulated today; manual trading is the active "live" surface.
- Position-driven exit/roll decisions should use the positions-market cache (fresh, separate from candidates refresh).

Live Trading Focus (Manual Enter/Exit/Roll)
- Manual UI: `webapp/templates/manual.html` + `webapp/static/manual.js`.
- Manual tests UI: `webapp/templates/manual_tests.html` + `webapp/static/manual_tests.js`.
- API endpoints in `webapp/app.py`: `/api/manual/enter`, `/api/manual/exit`, `/api/manual/roll`, `/api/manual/analyze`.
- Service glue in `webapp/services.py`: delegates to `execution/manual.py` (ManualTradeManager).
- Orderbook sources: websocket feed via `webapp/market_data.py` (MarketDataBus.get_orderbook), fallback to REST via ccxt.
- All live trading orders are isolated margin at 3x leverage.
- Live trade rules doc: `docs/manual_trade_rules.md` (WS-first positions, chunk reconciliation, fallback thresholds).

Dry Run Logic (ManualTradeManager in `execution/manual.py`)
- Dry run is a "plan-only" path; it builds a full execution plan and returns diagnostics, but never places orders.
- Entry point: `_handle_pair(...)`:
  - Calls `_build_plan(...)`.
  - If `payload.dry_run` or `plan.errors`, returns the plan immediately.
  - Otherwise executes the plan via `_execute_plan(...)` (or smart/fast enter/exit).
- `_build_plan(...)` does the heavy lifting:
  - Validates action, symbol, exchanges, qty/notional; can infer qty from positions for exit/roll.
  - Resolves qty from notional via ticker if needed.
  - For each leg:
    - Ensures exchange client (requires credentials unless ccxt supports public-only for that exchange).
    - Resolves ccxt symbol.
    - Fetches orderbook: prefers websocket (`orderbook_provider.get_orderbook`), else REST.
    - Extracts market constraints (min qty, min notional, amount step, contract size).
    - Applies optional min-notional overrides and buffer.
    - Scales orderbook for contract size, computes stats and liquidity.
    - Estimates fill, slippage, and max qty under slippage limit.
    - Adds errors if insufficient liquidity when `use_orderbook_check` is true.
  - Calculates funding meta, spread %, recommended qty/notional, and chunk sizing hints.
  - Optionally collects constraints for multiple exchanges if `constraints_exchanges` is provided.
  - Returns plan payload with `errors`, `warnings`, `stats`, `slippage`, `market_constraints`, etc.
- Key outcome: dry run is a comprehensive preflight check + sizing advisor, not a simulation of execution steps.

Notes / Observations on Dry Run
- It currently collects constraints for both legs and optionally for many exchanges.
  - This can be slow, especially if you only trade on two exchanges.
  - It may also require credentials for ccxt clients even for read-only data.
- `constraints_exchanges` is injected in `webapp/services.py` for dry runs using enabled analysis exchanges.
  - This makes dry run "wide" by default, not just the chosen pair.
- Dry run depends on orderbook data:
  - If websocket live orderbook is enabled, it uses WS; otherwise REST.
  - Failures show as errors/warnings in the plan, not as execution attempts.

Manual Tests (Web UI)
- Used for step-by-step validation of exchange connectivity and order paths.
- Endpoints: `/api/manual/test/limit`, `/api/manual/test/market`, `/api/manual/test/cancel`.
- Test logic in `webapp/services.py` uses the same ccxt gateway to fetch symbol, constraints, and orderbook.

Gate WS Notes (Futures v4)
- Two signature modes:
  - Private subscriptions use `auth` with signature `channel=<channel>&event=<event>&time=<time>`.
  - Trading requests use `event: "api"` with signature `api\n<channel>\n<req_param>\n<timestamp>`.
- Must send `futures.login` (`event: api`) before `futures.order_place`/`futures.order_cancel`.
  - Login payload includes `api_key`, `signature`, `timestamp`, `req_id`, `request_param: ""`, `headers: {}`.
  - Use server time offset (`time_ms` or `header.response_time`) and retry once on timestamp errors.
- Subscriptions need a contract param: payload `[<contract>]` or `[<uid>, <contract>]`; use `!all` for all contracts.
- Validated flow: login -> subscribe (`orders`/`positions`/`usertrades`) -> order place/cancel -> WS updates (open/filled/canceled) + positions/usertrades.

Kucoin WS Notes (Classic Futures)
- Private token via `/api/v1/bullet-private`, then connect to returned `endpoint?token=...`.
- Orders stream: `/contractMarket/tradeOrders` (optionally `:SYMBOL`).
- Positions stream: `/contract/positionAll` or `/contract/position:SYMBOL`.
- Wallet stream: `/contractAccount/wallet`.
- Validation: REST limit orders (manual tests) trigger `symbolOrderChange` updates on `/contractMarket/tradeOrders:SYMBOL` (open -> match -> filled), plus wallet updates and `/contract/position:SYMBOL` position changes. Order events carry `marginMode: ISOLATED` and `positionSide: BOTH`.
- Classic docs do not list `tradeFills`; use order updates or REST for fills.

Open Questions / Decisions to Make
- Should dry run skip private-client requirements and use public-only data when possible?
- Confirm target exchanges for live trading to limit the slow path.
- Kucoin: REST order requests include leverage=3, but position updates reported `realLeverage: 1.0`; check if explicit leverage-setting API call is required.

Recent Changes
- Smart enter/exit now run chunks sequentially (primary fill -> hedge -> next chunk) and wait for cancel confirmations before new limit orders (`execution/manual.py`).
- Smart enter/exit now clear active primary order state on terminal statuses (WS/REST), unblocking hedge placement after fills (`execution/manual.py`).
- Smart enter/exit now hedge strictly per-chunk and drop sub-min hedge remainders (no cross-chunk `unhedged_qty` carry); hedging blocks next chunk until done (`execution/manual.py`).
- Removed `max_unhedged_pct` from manual UI/payload and backend schema (`webapp/templates/manual.html`, `webapp/static/manual.js`, `webapp/app.py`).
- Manual stop now skips any post-loop market reconcile/hedge actions in smart/fast modes (`execution/manual.py`).
- BingX order WS now logs explicit connect/listenKey milestones and heartbeat timeout details to `app.log` (`execution/ws_orders.py`).
- Symbol Positions now prefer position-sourced fields and only fill missing funding/mark from snapshots or live funding fetch (`webapp/services.py`).
- Limit pricing now filters orderbook "junk" levels (default 1% of chunk notional/qty), improves 1 tick over best non-self price, and excludes own active size (`execution/manual.py`).
- Hedge limit repricing/cancel now waits for terminal status before placing new orders or switching to market (`execution/manual.py`).
- WS raw streams now suppress `CancelledError` during shutdown to avoid noisy ASGI errors (`webapp/ws_trade_gate_raw.py`, `webapp/ws_trade_private_raw.py`).
- BingX WS raw keepalive now suppresses `CancelledError` on shutdown to avoid noisy ASGI errors (`webapp/ws_trade_bingx_raw.py`).
- OKX/Kucoin/Bitget/Bybit trade raw WS readers now suppress `CancelledError` during shutdown (`webapp/ws_trade_okx_raw.py`, `webapp/ws_trade_kucoin_raw.py`, `webapp/ws_trade_bitget_raw.py`, `webapp/ws_trade_bitget_trade_raw.py`, `webapp/ws_trade_raw.py`).
- Execution log now wraps and pretty-prints payload entries to avoid horizontal overflow (`webapp/templates/manual.html`, `webapp/static/styles.css`, `webapp/static/manual.js`).
- BingX swap user stream validated: REST order/cancel triggers `ORDER_TRADE_UPDATE`, `TRADE_UPDATE`, and `ACCOUNT_UPDATE`; initial `SNAPSHOT` flood is expected; ping requires literal `Pong`.
- Manual API defaults `async_run` to true so Execute uses async runs even if the UI cache is stale (`webapp/app.py`).
- Manual endpoints now log incoming payloads to app logger for debugging missing exec ids (`webapp/app.py`).
- Manual orderbook liquidity gating now checks chunk size (when provided) instead of full qty, with detailed error messages (`execution/manual.py`).
- Fixed dry-run crash when liquidity check passed (guarded missing message in `execution/manual.py`).
- Smart-exit no longer does pre-exit position alignment via market orders; market submits now include explicit `reason=` in logs (`execution/manual.py`).
Recent Changes
- Live order/trade WS tracking added in `execution/ws_orders.py` for Bybit/OKX/Gate/Bitget/Kucoin/BingX.
- Smart enter/exit now sync primary fills and hedge control via WS order updates (REST only when WS is stale).
- Fast enter/exit (market) now waits for WS order fills and orderbook refill before next chunk; final reconcile uses REST positions.
- Manual UI now exposes market refill settings (bps/buffer/max wait) for Fast (market) mode.
- Smart enter/exit no longer uses trades-fallback for primary fills; REST status checks skip trade-based recovery when WS is stale.
- Manual UI now shows raw WS order streams for the selected long/short exchanges (auto-connect + subscribe).
- Manual UI now adds Copy buttons for live orderbook, dry-run plan, execution log, and WS raw logs.
- Manual action summaries now include per-action timestamps (`ts`) when available.
- WS order stream stale threshold raised to 45s; BingX WS now marks live on all event types and logs heartbeat gaps to `app.log` for diagnostics.
- Added per-exchange WS notes: `docs/bitget/NOTES.md`, `docs/gate/NOTES.md`, `docs/kucoin/NOTES.md` (BingX notes already in `docs/bingx/NOTES.md`).
- Added module notes: `docs/modules/manual_execution.md`, `docs/modules/ws_orders.md`.
- Manual UI raw WS order logs now normalize symbols per exchange (Gate `BASE_USDT`, Kucoin `BASEUSDTM`, Bitget uses symbol or `default`) and wait for Gate login ack before subscribing.
- LiveOrderTracker now passes the active symbol into WS order streams; Bitget order stream subscribes to symbol-specific `instId` (BASEUSDT) instead of `default`.
- Hedge limit status checks skip REST when WS is live to avoid false fills (BingX limit + market double-fill risk).
- Position reconciliation now runs only at entry/exit fallback; per-chunk WS position checks removed.
- Hedge limit monitoring prefers WS order updates over REST order status.
- Manual live trade rules updated for WS order/trade tracking (`docs/manual_trade_rules.md`).
- BingX swap WS notes captured in `docs/bingx/NOTES.md`.
- BingX swap user stream validated: REST order/cancel triggers `ORDER_TRADE_UPDATE`, `TRADE_UPDATE`, and `ACCOUNT_UPDATE`; initial `SNAPSHOT` flood is expected; ping requires literal `Pong`.
- Manual smart enter/exit now use WS order/trade updates for fill tracking; positions are only used at entry/exit fallback.
- Manual execution now matches base-only symbols (e.g., `GMT`) against full exchange symbols (e.g., `GMTUSDT`) when reconciling positions.
- Gate WS positions stream now signs private subscriptions (auth) to match the working manual-tests flow.
- Chunk reconcile uses min_qty_required * 1.15 as the market fallback threshold; adverse move default is 0.1% (10 bps).
- Live trade rules captured in `docs/manual_trade_rules.md`.
- Dry run now scopes `constraints_exchanges` to the selected pair (long/short or from/to) instead of all analysis exchanges.
- Manual UI hides min-notional/min-level inputs and removes the "Order minimums" panel from the page.
- Dry-run orderbook liquidity shortfalls are warnings (not errors); execution still treats them as errors.
- Smart enter/exit now clamp primary fill deltas to target qty and cap trades-fallback fills to expected size.
- Manual execution logs position snapshots (start/end) for the selected exchanges and symbol.
- Position symbols are no longer injected into the main universe; positions now use a separate positions-market snapshot cache.
- Isolated positions attempt auto margin top-up/reduction with cooldowns; failed top-ups surface in Telegram alerts.
- Added a standalone Bybit WS order test script at `scripts/wsbybit.py` for direct order-create responses.
- `scripts/wsbybit.py` now bootstraps the repo root onto `sys.path` so it runs outside module mode.
- Bybit WS manual trade adds `apiTimestamp`/`recvWindow`; manual tests trade log now shows full tx/rx stream.
- Manual tests WS trade log now appends raw frames + parsed payloads with timestamps (terminal-style).
- Manual trade WS fixes exchange check order and reports server-side exceptions to the client.
- `scripts/wsbybit.py` now waits longer for responses and logs pong/timeout warnings.
- `scripts/wsbybit.py` includes `apiTimestamp`/`recvWindow` in Bybit WS order.create payload.
- Bybit WS order/cancel now include `X-BAPI-TIMESTAMP`/`X-BAPI-RECV-WINDOW` headers plus string timestamps.
- Manual trade WS now forwards raw inbound frames and explicit tx events for full UI logging.
- Manual trade WS now logs server-side rx_raw and UI-bound payloads for debugging missing responses.
- Manual trade WS handles client disconnects cleanly (no crash log).
- Manual tests trade log now shows bybit/okx tx in top pane and raw rx frames in bottom pane.
- Manual trade WS now logs to `logs/manual_trade_ws.log` for server-side tracing.
- Manual trade WS uses Bybit trade-only connection to mirror wsbybit behavior.
- Manual tests trade log now dumps every WS message raw (no filters) in the bottom pane.
- Manual tests shows a log build version (`ws-trade-log-version`) to confirm updated JS is loaded.
- Manual trade WS now always uses a single Bybit trade WS connection (no private WS) to match `wsbybit.py`.
- Manual trade WS now waits synchronously for Bybit responses (wsbybit-style) after order.create.
- Manual trade WS no longer emits extra "order sent" info for Bybit to keep logs wsbybit-like.
- Manual tests now auto-fills order id from Bybit order.create ack payloads.
- `scripts/wsbybit.py` now supports fetch+cancel flow with delays after order.create.
- `scripts/wsbybit.py` now does cancel via WS and REST fetch is optional (`--fetch-rest`).
- Bybit manual trade uses private order stream + trade stream with reqId futures to await acks.
- Manual trade WS now reads trade acks directly (wsbybit-style) while private stream handles order updates.
- Bybit manual trade now uses a single trade reader loop with pre-registered reqId futures to avoid missed acks.
- Manual tests Bybit WS now uses a single trade socket and logs all raw frames (no private stream).
- Manual tests Bybit WS no longer waits for ack; reader loop emits raw/parsed events only.
- Manual tests Bybit WS removes private WS support code (trade-only).
- Manual tests page removes the private WS order test block (trade WS UI disabled).
- Manual tests adds a new WS Trade Raw (Bybit) block using /ws/trade-raw with plain text logging.
- WS Trade Raw block now has a one-click order button that auto-fills timestamp/reqId.
- Manual tests adds a WS Trade Private Raw (Bybit) block with quick subscribe buttons.
- WS Trade Raw block now includes a cancel button with auto timestamp.
- Bybit WS tests: trade ACKs, private order/execution/position streams validated; `execution.fast` + `order` + `position` enough for WS-only flow (with one REST snapshot at start).
- Manual tests now uses raw WS blocks for Bybit trade/private; full raw logs are written to `logs/ws_trade_raw.log` and `logs/ws_trade_private_raw.log`.
- MEXC: no WS buy test; keep public live spread only.
- Added OKX raw WS endpoint at `/ws/trade-okx-raw` with server-side login and raw logging to `logs/ws_trade_okx_raw.log`.
- Manual tests adds an OKX raw WS block with order/cancel/subscribe controls and raw send/log console.
- OKX WS trade sends a top-level `id` per request (required to avoid `Parameter id can not be empty`).
- OKX WS flow validated: `order` ack gives `ordId`, `orders` stream shows `state` transitions (live/filled/canceled), `positions` stream reflects fills (size change to 100 and back to 0 on sell).
- Added raw WS endpoints + manual tests blocks for Bitget, Gate, Kucoin, and BingX:
  - Bitget: trade WS `/ws/trade-bitget-trade-raw` for order/cancel; private WS `/ws/trade-bitget-raw` for `orders`/`positions` streams (login uses `timestamp + GET + /user/verify` signature).
  - Gate: `/ws/trade-gate-raw` with server-side signing for private messages; buttons for orders/positions/fills using `futures.*` channels.
  - Kucoin: `/ws/trade-kucoin-raw` uses `bullet-private` token; buttons subscribe to `tradeOrders`, `position:<symbol>`, and `wallet`.
  - BingX: `/ws/trade-bingx-raw` uses REST listenKey + `open-api-swap.bingx.com/swap-market?listenKey=...`; user stream is listenKey-driven and does not require subscriptions.
- BingX swap-only user stream (USDT-M); coin-m `cswap` endpoints are not used.
- New log files: `logs/ws_trade_bitget_raw.log`, `logs/ws_trade_gate_raw.log`, `logs/ws_trade_kucoin_raw.log`, `logs/ws_trade_bingx_raw.log`.
- Bitget trade WS now uses `wss://ws.bitget.com/v2/ws/private` with `op: "trade"` + channels `place-order`/`cancel-order` (per saved Bitget contract docs); UI sends required params (`marginCoin`, `marginMode`, `force`, etc.).
- Manual tests now use a single Bitget WS Trade Raw panel (no separate private block) with quick subscribe buttons for `orders`, `positions`, `fill`, and `account`.
- Bitget private WS streams (`orders`/`positions`/`fill`/`account`) validated; WS order placement requires VIP, so live/manual orders should go via REST while keeping WS for real-time updates.
- Gate WS auth signing now follows Gate v4 docs (`channel=<channel>&event=<event>&time=<time>`), and WS order/cancel use `futures.order_place`/`futures.order_cancel` with `event: api` + signed payload.
- Gate WS now sends `futures.login` (`event: api`) on connect so `futures.order_place` works without "Not login" errors.
- Gate WS login now tracks server time from `time_ms`/`header.response_time`, retries on timestamp errors, and uses server-time offsets for signing.
- Gate WS flow validated: `futures.login` + `futures.orders`/`futures.positions`/`futures.usertrades` subscriptions work; `futures.order_place`/`futures.order_cancel` via `event: api` return ack + result, and WS updates reflect open/filled/canceled states plus position/usertrades updates.
- Kucoin WS raw now reads key version from `ExchangeGateway.spec.options` to avoid crashes when `gateway.options` is missing.
- Kucoin classic WS flow validated: REST limit orders (manual tests) emit tradeOrders updates (open/match/filled), position changes on `/contract/position:SYMBOL`, and wallet updates.
- Kucoin/BingX WS raw panels now include REST order/cancel buttons (limit/market) plus order-id capture to drive WS updates.
- BingX WS raw now uses listenKey keepalive (30m) + close on shutdown; manual tests add extend/close listenKey buttons.
- BingX user stream ping now replies with literal `Pong` per docs to avoid disconnects.
- Symbol Positions now use a dedicated positions-market snapshot cache (per-exchange batch fetch, separate refresh interval) and prefer the freshest source by timestamp; per-position funding live fetches removed.
- Positions-market refresh is decoupled from ArbitrageScanner/Coinglass; universe extension for positions removed; UI adds diagnostics under Symbol Positions (last refresh, per-exchange status, diff hints).
- WS order liveness now uses heartbeat/ping traffic (not just order updates); manual execution probes reconnect before REST fallback.
- Manual UI includes per-exchange WS health settings (heartbeat interval/timeout, reconnect attempts/grace).
- Added WS order health probe script at `scripts/ws_order_health_probe.py`.
- WS order streams now mark live on any inbound WS message (including pings) and on connect to prevent false stale when only heartbeat traffic flows.
- Manual UI now autofills `Chunk qty (base)` from dry-run recommended chunk (max chunk hint), allowing manual edits afterward.
- Added "Force chunk qty" toggle to keep manual chunk size and bypass slippage max-chunk cap when enabled.
- Manual execution now supports stop requests: `/api/manual/exec/{id}/stop` flips `stop_requested`, UI Stop button wires to it, and smart/fast loops halt further chunks (smart modes cancel active primary order).
- Smart-exit no longer overrides user-specified qty/notional with live positions (only warns if requested qty exceeds positions).
- Manual tests UI now has per-exchange WS liveness monitors with script logs, silence timers, and ping probes (separate from raw WS logs).
- Manual tests UI now includes per-exchange Reconnect buttons for WS raw panels.
- Manual execution logs now emit "story" lines for WS health, start-of-run, primary fill deltas, and order submit/status (including hedge limits); manual log rendering hides JSON for story lines for readability.
- WS order health now marks outbound pings as live (ping-send) to avoid false stale on quiet streams; WS health story lines now show missing-stream errors.
- WS order health now exposes warmup state (`warming`, `since_start_sec`) and manual execution treats warmup as non-stale to avoid immediate REST fallback at stream startup.
- Manual execution now logs WS probe lifecycle (server ping, probe ping/pong, heartbeat timeouts, reconnect attempts) via LiveOrderTracker event callbacks.
- Manual execution now surfaces WS connect/auth/listenKey errors (auth missing/failed, listenKey failures, connect errors) from ws_orders streams to explain stale states.
- ws_orders no longer calls `stop()` inside `_connect_and_listen` (replaced with `_reset_connection`) to avoid self-cancel and to allow streams to actually receive frames.
- Manual execution now exposes `/api/manual/exec` for active run summaries; manual UI shows current execution id and resumes polling after reload.
- Settings update now tolerates `manual`/`protective` being null in `/api/settings` payloads to avoid 500s.
- Positions now carry `margin_mode_source`/`leverage_source`; Symbol Positions diagnostics show margin mode/leverage issues vs isolated 3x target.
- Accounts monitor now attempts to enforce isolated margin + target leverage (default 3x) per position with cooldowns; failures log warnings.
- Manual execution now logs positions snapshot sources (`rest`) on start/end snapshots, and logs a pre-final-reconcile positions snapshot (with ws/rest sources) before placing final reconcile orders.
- Protective order sync now treats BingX `110424` / Bitget `22002` as no-position and skips with info log instead of hard error.
- Bitget WS orders now prefer cumulative `accBaseVolume` (or `accFillSz`/`filledQty`) and treat `baseVolume` as per-fill delta to avoid undercounting fills.
- Manual tests now include per-exchange margin/leverage panels with position fetch + add/reduce isolated margin; new endpoints `/api/manual/test/position` and `/api/manual/test/margin/(add|reduce)`; positions now expose initial/maintenance margin for diagnostics.
- Bitget protective stop/take now uses `holdSide=buy/sell` for one-way mode to avoid `holdSide` errors.
- Manual execution removes `max_unhedged_sec` time-based hedge fallbacks; UI no longer exposes the field.
- Smart-exit final reconcile now compares start vs end deltas to only market-close the lagging leg.
- Manual execution logs now include the initial manual payload in execution logs.
- Manual UI log blocks are compacted for single-screen visibility (smaller log heights, no-wrap logs).

Next Steps (Planned)
- Review and possibly trim dry-run scope (constraints collection and exchange set).
- Align dry-run latency with real trading needs (two-exchange focus).
- Re-evaluate which orderbook sources are required for dry-run vs live execution.
- Run manual margin/leverage tests per exchange (fetch position by symbol, add/reduce margin, capture liq/limits + logs).
