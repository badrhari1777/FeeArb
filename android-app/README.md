# FeeArb Android App

Native Android v1 client for FeeArb.

Current scope:
- `Balances` with aggregate totals and per-exchange cards
- `Positions` with a read-only Main/Pump portfolio overview, module filters,
  Pump protection/risk cards, and the existing main-position controls
- `Manual Trade`
- `Settings / Advanced`

Backend contract used by the app:
- `GET /api/mobile/positions`
- `GET /api/positions/overview`
- `GET /api/mobile/manual-defaults`
- `POST /api/auto-exit/rule`
- `POST /api/position/action`
- `POST /api/manual/analyze`
- `POST /api/manual/enter`
- `POST /api/manual/exit`
- `POST /api/manual/roll`
- `GET /api/manual/exec/{id}`
- `POST /api/manual/exec/{id}/stop`

Notes:
- Default base URL is `http://10.0.2.2:8000/` for Android emulator access to local FastAPI.
- `usesCleartextTraffic=true` is enabled for LAN/dev setups.
- Version `0.2.0` uses a 5-minute execution default, exposes a 1-10 minute
  selector, and caps `Until filled` at 10 minutes for Manual and position
  `Add` / `Exit` execution paths.
- Version `0.3.0` adds `All / Main / Pump` module filters to `Positions`, a
  compact portfolio-risk summary, a dedicated Pump subaccount status card,
  read-only Pump position details (TP, emergency SL, liquidation buffer,
  top-up, hold time, and ladder progress), and lifecycle-aware 15-second
  polling only while the Positions screen is foregrounded.
- The module can be built from Android Studio, Cursor, or the bundled Gradle wrapper.
- Before the first CLI build, copy `local.properties.example` to `local.properties` and point `sdk.dir` to your Android SDK.
- Current debug build output after successful local assemble:
  - `android-app/app/build/outputs/apk/debug/app-debug.apk`

Testing guide:
- `docs/android_v1_testing.md`
- Cursor setup and test guide (Russian): `docs/cursor_android_testing_ru.md`
