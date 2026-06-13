# Android V1 Plan

Purpose
- Preserve the agreed Android/mobile app scope and UX decisions so a new session can resume without re-deriving them from chat.
- V1 is intentionally narrow: operator-focused positions + manual trade, not full dashboard parity.

Scope
- Primary screens:
  - `Balances`
  - `Positions`
  - `Manual Trade`
  - `Settings`
- Explicitly out of scope for v1:
  - candidates / opportunities dashboards
  - full coin-analysis UI
  - exchange tests / raw WS tools
  - full configuration parity with desktop

Current implementation note
- The earlier combined `Dashboard` design has been split into dedicated `Balances` and `Positions` screens.
- Bottom navigation is now `Balances / Positions / Manual / Settings`, and a cold start opens `Balances`.
- `Balances` shows aggregate total/available/used values plus individual exchange cards; position controls remain isolated on `Positions`.
- Position cards now use coin-based hedged sizing: the smaller long/short leg is the 100% basis. Quick `Add` and `Exit` support 25/50/75/100 presets plus a custom percentage, with server-side preflight and confirmation.
- Auto Exit supports a configurable partial percentage and is one-shot by default. The backend recalculates the current smaller leg immediately before execution.

Current Reusable Backend / UI Context
- Existing phone-first web route: `/mobile`
  - `webapp/templates/mobile.html`
  - `webapp/static/mobile.js`
- Existing manual trading backend:
  - `/api/manual/analyze`
  - `/api/manual/enter`
  - `/api/manual/exit`
  - `/api/manual/roll`
  - `/api/manual/exec`
  - `/api/manual/exec/{id}`
- Existing positions / auto-exit backend:
  - `/api/snapshot`
  - `/api/auto-exit/rule`

Recommended Android Architecture
- Thin client over the existing FastAPI backend.
- Do not move exchange logic, credentials, or execution orchestration into Android.
- Preferred app structure:
  - bottom navigation: `Dashboard`, `Manual`, `Settings`
  - Jetpack Compose for native Android if a real app is built
- If a faster first step is needed, the existing `/mobile` route can be used as a temporary bridge/reference.

Screen 1: Dashboard
- Top section shows compact exchange balances:
  - exchange
  - total
  - available
  - used
  - margin ratio/status
- Main UI should be card-based, not a wide table.
- Each symbol should have a collapsed and expanded state.

Collapsed card
- `Symbol`
- `Pair` (`LONG_EXCHANGE / SHORT_EXCHANGE` or `multi-leg`)
- `Net PnL`
- `Expected funding`
- `Live spread`
- `Next funding`
- `Liq distance %` as a risk indicator
- compact actions:
  - auto-exit status/target when present
  - `Manual`
  - `Expand`

Expanded card
- `Position Summary`
  - quantity
  - amount USDT
  - entry
  - mark
  - leverage
- `Risk`
  - liq price
  - liq distance
  - stop price
  - take price
- `Funding`
  - funding rate
  - expected funding
  - next funding
- `Legs`
  - one row/card per leg with exchange, side, qty, entry, mark, pnl, leverage

Dashboard screen controls
- Top filters:
  - `All`
  - `Risk`
  - `Funding Soon`
  - `Auto Exit On`
- Sorting:
  - `By PnL`
  - `By Liq Risk`
  - `By Next Funding`
  - `By Symbol`

Auto-exit UX
- Keep per-position inline editing on the positions card.
- Show compact status:
  - `armed`
  - `off`
  - `waiting`
  - `no live spread`
- Optional quick target chips:
  - `-0.5`
  - `-1.0`
  - `-2.0`
- More detailed diagnostics should live behind expand/details, not on the card face.

Screen 2: Manual Trade
- V1 should be a simplified operator wizard, not a full mirror of `manual.html`.

Main fields
- `Action`: Enter / Exit / Roll
- `Symbol`
- `Qty` or `Notional`
- quick `Chunk qty` and `Chunk notional` on the main form, close to `Qty` / `Notional`
- `Long exchange`
- `Short exchange`
- `Mode`
  - `Smart`
  - `Fast`
- read-only `Spread Preview`, sourced from WS orderbook first and public market snapshots as fallback
- Optional but acceptable on main form:
  - `Max slippage`
  - `Margin mode` if it is still changed often by the operator

Main action flow
1. Enter symbol / qty / exchanges / mode
2. Verify `Spread Preview` visually
3. `Dry Run`
4. Show compact plan summary:
   - spread
   - slippage estimate
   - recommended chunk
   - warnings
   - errors
5. `Execute`
6. Show execution status + log

Advanced settings
- Hide by default.
- Read backend defaults first; allow editing only when needed.
- Put in separate screen, dialog, or collapsible section.

Visible settings in the Android v1 app should stay narrow:
- backend base URL
- max slippage
- margin mode
- orderbook check
- exit allow flip
- force chunk qty

Timeouts, runtime/reprice controls, hedge offsets, max limit deviation, and WS health controls stay on the backend in this mobile build.

Manual Trade: Expensive Leg / Auto Hint
- The operator wanted to keep the existing `Expensive leg (manual)` behavior and especially the `Auto hint` rule.
- That rule is now important for Android too because the app can expose a simple selector:
  - `Auto hint`
  - explicit override (`long/short` or `to/from` for roll)
- Current backend rule:
  1. lower-liquidity venue tier first
  2. then higher taker fee
  3. then lower liquidity
  4. then tie-break
- Current venue tiers in backend:
  - `binance` = tier 1
  - `okx` = tier 2
  - all others = tier 3

Important execution note
- `Auto hint` is no longer dry-run-only.
- Backend now resolves auto-hint consistently during actual manual execution across:
  - `limit-first-expensive`
  - `smart-enter`
  - `smart-exit`
  - `smart-roll`
  - fast enter / exit ordering
- Roll mapping is also resolved correctly:
  - suggested `long` -> `to`
  - suggested `short` -> `from`

Recommended API follow-up
- Android now works against slim mobile endpoints:
  - `GET /api/mobile/positions`
  - `GET /api/mobile/manual-defaults`
  - `POST /api/mobile/manual-spread`

Current Implementation Status
- Slim mobile backend endpoints now exist:
  - `GET /api/mobile/positions`
  - `GET /api/mobile/manual-defaults`
  - `POST /api/mobile/manual-spread`
- Native Android scaffold now exists in `android-app/`:
  - Jetpack Compose app module with bottom navigation `Dashboard / Manual / Settings`
  - `Dashboard` balance list plus position card list with filters/sorts and expand/collapse details
  - `Manual` screen wired to spread preview, dry-run, execute, and stop against existing manual APIs
  - `Settings` screen for base URL plus a narrow set of mobile-critical trade safety parameters
- Android module now builds locally with `.\gradlew.bat :app:assembleDebug`, producing `android-app/app/build/outputs/apk/debug/app-debug.apk`
- Manual live submit is now gated by an explicit confirmation dialog after preflight, and the app shows clearer loading / empty / error states on `Dashboard` and `Manual`

Implementation Priority
1. `Dashboard` balances and position cards
2. simplified `Manual Trade`
3. narrow mobile `Settings`
4. dedicated mobile endpoints

Definition of Done for Android V1
- Balances and positions are readable on a phone without horizontal table scrolling.
- Manual trade supports spread preview -> dry-run -> execute -> watch log.
- Rare tuning knobs are hidden from Android and remain backend-managed.
- `Auto hint` is available and relies on backend execution logic, not frontend guesswork.
