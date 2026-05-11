# Android V1 Plan

Purpose
- Preserve the agreed Android/mobile app scope and UX decisions so a new session can resume without re-deriving them from chat.
- V1 is intentionally narrow: operator-focused positions + manual trade, not full dashboard parity.

Scope
- Primary screens:
  - `Positions`
  - `Manual Trade`
  - `Settings / Advanced`
- Explicitly out of scope for v1:
  - candidates / opportunities dashboards
  - full coin-analysis UI
  - exchange tests / raw WS tools
  - full configuration parity with desktop

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
  - bottom navigation: `Positions`, `Manual`, `Settings`
  - Jetpack Compose for native Android if a real app is built
- If a faster first step is needed, the existing `/mobile` route can be used as a temporary bridge/reference.

Screen 1: Positions
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
- inline auto-exit controls:
  - toggle `Spread Exit`
  - editable `Exit Spread %`
  - `Save`
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

Positions screen controls
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
- quick `Chunk qty` on the main form, close to `Qty` rather than buried in advanced settings
- `Long exchange`
- `Short exchange`
- `Mode`
  - `Smart`
  - `Fast`
- Optional but acceptable on main form:
  - `Max slippage`
  - `Margin mode` if it is still changed often by the operator

Main action flow
1. Enter symbol / qty / exchanges / mode
2. `Analyze`
3. Show compact plan summary:
   - spread
   - slippage estimate
   - recommended chunk
   - warnings
   - errors
4. `Execute`
5. Show execution status + log

Advanced settings
- Hide by default.
- Read backend defaults first; allow editing only when needed.
- Put in separate screen, dialog, or collapsible section.

Advanced sections
- `Execution`
  - timeout
  - max runtime
  - reprice sec
  - max slippage
- `Chunking`
  - chunk qty
  - chunk notional
  - force chunk qty
- `Hedge`
  - hedge order type
  - hedge limit mode
  - favorable / adverse bps
- `Safety`
  - orderbook check
  - max limit deviation
  - exit allow flip
- `System`
  - WS health / reconnect controls should be hidden deepest; not part of normal operator UX

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
- Android can work against existing APIs, but a slimmer mobile endpoint would be cleaner:
  - `GET /api/mobile/positions`
  - `GET /api/mobile/manual-defaults`
- This is optional, not required for the first Android implementation.

Current Implementation Status
- Slim mobile backend endpoints now exist:
  - `GET /api/mobile/positions`
  - `GET /api/mobile/manual-defaults`
- Native Android scaffold now exists in `android-app/`:
  - Jetpack Compose app module with bottom navigation `Positions / Manual / Settings`
  - `Positions` card list with filters/sorts, inline auto-exit save, expand/collapse details
  - `Manual` screen wired to analyze/execute/stop against existing manual APIs
  - `Settings` screen for base URL plus advanced trade parameters kept out of the main manual flow
- Android module now builds locally with `.\gradlew.bat :app:assembleDebug`, producing `android-app/app/build/outputs/apk/debug/app-debug.apk`
- Manual live submit is now gated by an explicit confirmation dialog after preflight, and the app shows clearer loading / empty / error states on `Positions` and `Manual`

Implementation Priority
1. `Positions` cards with inline auto-exit editing
2. simplified `Manual Trade`
3. `Advanced` settings
4. optional dedicated mobile endpoints

Definition of Done for Android V1
- Positions are readable and actionable on a phone without horizontal table scrolling.
- Auto-exit can be edited per position directly from the positions screen.
- Manual trade supports analyze -> execute -> watch log.
- Rare tuning knobs are hidden by default but still reachable.
- `Auto hint` is available and relies on backend execution logic, not frontend guesswork.
