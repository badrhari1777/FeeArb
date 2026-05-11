# Android V1 Testing Guide

Purpose
- Bring the current Android v1 implementation to a testable first milestone.
- Give one concrete path for backend setup, Android setup, smoke testing, and issue triage.

Current Scope
- Native app module: `android-app/`
- Main screens:
  - `Positions`
  - `Manual`
  - `Settings`
- Backend mobile endpoints:
  - `GET /api/mobile/positions`
  - `GET /api/mobile/manual-defaults`

## 1. Prerequisites

Backend
- Python environment for the existing FeeArb backend
- Valid exchange/API settings only if you want to test real account data or real manual execution

Android
- Android Studio or Cursor with Android support
- Android SDK installed
- JDK 17

Network
- Backend reachable from emulator or device
- For emulator:
  - use `http://10.0.2.2:8000/`
- For physical device on same LAN:
  - use `http://<YOUR_PC_LAN_IP>:8000/`
- For HTTPS/Tailscale/Caddy:
  - use your full reachable HTTPS base URL

## 2. Backend Startup

Run the existing backend from repo root. If you normally use your own startup script, keep using it.

Typical local example:

```powershell
cd C:\Projects\FeeArb
python main.py
```

If your normal app entrypoint is different in your environment, use that instead.

Validate backend manually in a browser or curl:

```powershell
curl http://127.0.0.1:8000/api/mobile/positions
curl http://127.0.0.1:8000/api/mobile/manual-defaults
```

Expected
- both endpoints return JSON
- `positions` contains `cards`
- `manual-defaults` contains `exchanges`, `defaults`, `main_modes`

## 3. Android Project Open

Open:
- `C:\Projects\FeeArb\android-app`

The module already includes:
- `gradlew`
- `gradlew.bat`
- `gradle/wrapper/gradle-wrapper.jar`
- `gradle/wrapper/gradle-wrapper.properties`

First sync
1. Open `android-app` in Android Studio
2. Let Gradle sync
3. If Android Studio asks for SDK/platform packages, install them
4. Copy `android-app/local.properties.example` to `android-app/local.properties` if needed and set `sdk.dir`
5. If any compile issue appears, fix it in the Android module before functional testing

Optional command-line build on Windows:

```powershell
cd C:\Projects\FeeArb\android-app
copy local.properties.example local.properties
.\gradlew.bat :app:assembleDebug
```

Current expected APK output:

```text
android-app/app/build/outputs/apk/debug/app-debug.apk
```

If that fails, the failure will usually be one of:
- missing Android SDK
- missing `local.properties` / wrong `sdk.dir`
- missing platform/build-tools
- first-pass Kotlin/Compose compile issue

## 4. First App Launch

On first launch:
1. Open `Settings`
2. Confirm `Backend base URL`
3. Press `Apply`
4. Return to `Positions`
5. Press `Refresh`

Expected
- top status line updates
- `Positions` cards load if backend/account data exists
- `Manual` loads exchange defaults from `/api/mobile/manual-defaults`

## 5. Smoke Test Checklist

### A. Positions

Goal
- confirm card rendering, filtering, sorting, expand/collapse, and auto-exit save

Steps
1. Open `Positions`
2. Verify cards render without horizontal scrolling
3. Toggle filters:
   - `All`
   - `Risk`
   - `Funding Soon`
   - `Auto Exit On`
4. Toggle sorts:
   - `By PnL`
   - `By Liq Risk`
   - `By Next Funding`
   - `By Symbol`
5. Open one card with `Expand`
6. Inspect sections:
   - `Position Summary`
   - `Risk`
   - `Funding`
   - `Legs`
7. Change `Target spread %`
8. Toggle `Spread Exit`
9. Press `Save`

Expected
- filter chips change visible set
- sort changes order
- card expands/collapses cleanly
- `Save` updates backend without crashing
- after refresh, saved auto-exit state persists

### B. Manual Analyze

Goal
- confirm thin-client flow over existing manual backend

Steps
1. Open `Manual`
2. Fill:
   - `Action`
   - `Symbol`
   - `Qty` or `Notional`
   - exchanges
   - mode
3. Keep `Expensive leg = Auto hint`
4. Press `Analyze`

Expected
- `Plan Summary` fills
- errors/warnings show if backend rejects the plan
- if backend returns `recommended_chunk_qty` and local `chunk qty` is empty, app keeps using backend recommendation in advanced settings state

### C. Manual Execute

Goal
- confirm execution start, status poll, stop flow

Steps
1. Start from a valid `Analyze`
2. Press `Execute`
3. Review the confirmation dialog payload summary
4. Confirm live submit
5. Watch:
   - `Execution id`
   - `Status`
   - `Execution` log block
6. If run is long enough, press `Stop`

Expected
- app does not submit live execution immediately; it shows a confirmation dialog first
- async execution returns `execution_id`
- app polls `/api/manual/exec/{id}`
- status transitions render
- stop request is sent to backend

### D. Roll Mode

Goal
- confirm roll-specific UI exists and maps to backend modes

Steps
1. In `Manual`, switch `Action = Roll`
2. Verify visible controls:
   - `Roll mode`
   - `From exchange`
   - `To exchange`
   - `Side`
   - `Expensive leg`
3. Set `Expensive leg = Auto hint`
4. Run `Analyze`

Expected
- app uses `rollMode`
- backend receives roll payload
- auto-hint remains selectable and backend resolves actual first leg

### E. Settings / Advanced

Goal
- verify advanced parameters are not on the main manual screen but are still editable

Steps
1. Open `Settings`
2. Change:
   - `Max slippage`
   - `Timeout`
   - `Chunk qty`
   - `Chunk notional`
   - hedge fields
   - safety fields
3. Return to `Manual`
4. Run `Analyze` or `Execute`

Expected
- settings persist in app storage
- backend behavior changes according to edited fields

## 6. Recommended Test Order

Do this in order:
1. `GET /api/mobile/manual-defaults` works
2. App opens and `Settings -> Apply` succeeds
3. `Positions` refresh works
4. `Manual Analyze` works with a harmless dry-run
5. `Auto Exit Save` works on one known pair
6. `Manual Execute` only after dry-run and only in your intended environment

## 7. Safe vs Risky Testing

Safest
- run backend with no real credentials
- use screens only for rendering and API shape validation
- use `Analyze` only

Medium risk
- real credentials, but only `Analyze`
- auto-exit save on rules

Highest risk
- `Execute` on live credentials
- `Exit` / `Roll` on real open positions

For live testing
- start with smallest viable symbol/size
- keep `Auto hint` enabled if you want the backend tier/fee/liquidity rule to choose first leg
- validate on one or two pairs first

## 8. Known Gaps in This First Testable Version

These are normal for the current milestone:
- No dedicated mobile auth/session handling yet
- No websocket push channel in the app yet; polling is used for execution state
- No dedicated Android test suite yet
- No product polishing pass yet for typography and spacing
- No APK signing/release pipeline yet
- No offline cache/data persistence beyond local app settings

## 9. What Still Needs to Be Added for a More Complete App

Functional
- pull-to-refresh
- better validation messages in `Manual`
- richer auto-exit diagnostics detail drawer
- clearer live/dry-run badges

Technical
- Android instrumentation tests
- ViewModel/repository unit tests
- release build config
- environment flavors: local / LAN / production
- optional auth or token layer if app stops living behind trusted network

Product
- icon/splash
- better visual polish
- operator-first QA pass on a real device

## 10. Fast Troubleshooting

App shows no positions
- check backend `/api/mobile/positions`
- check base URL in `Settings`
- check that backend account snapshot actually has positions

Manual has no exchanges
- check backend `/api/mobile/manual-defaults`
- check `analysis_exchanges` settings

Execute never starts
- inspect backend response for `/api/manual/enter|exit|roll`
- verify required payload fields for action
- verify credentials on backend side

Gradle sync fails
- confirm Android SDK installed
- confirm JDK 17
- run `.\gradlew.bat :app:assembleDebug` and inspect first real compiler error

## 11. Definition of Testable V1

Call this first version testable when all of the below pass:
- Android Studio sync succeeds
- `.\gradlew.bat :app:assembleDebug` succeeds
- app launches on emulator or device
- `Settings -> Apply` reaches backend
- `Positions` screen renders real cards
- one auto-exit save succeeds
- `Manual Analyze` succeeds
- `Manual Execute` starts and status polling works
