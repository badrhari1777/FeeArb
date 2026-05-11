# FeeArb Android App

Native Android v1 client for FeeArb.

Current scope:
- `Positions`
- `Manual Trade`
- `Settings / Advanced`

Backend contract used by the app:
- `GET /api/mobile/positions`
- `GET /api/mobile/manual-defaults`
- `POST /api/auto-exit/rule`
- `POST /api/manual/analyze`
- `POST /api/manual/enter`
- `POST /api/manual/exit`
- `POST /api/manual/roll`
- `GET /api/manual/exec/{id}`
- `POST /api/manual/exec/{id}/stop`

Notes:
- Default base URL is `http://10.0.2.2:8000/` for Android emulator access to local FastAPI.
- `usesCleartextTraffic=true` is enabled for LAN/dev setups.
- This module was scaffolded without Gradle in the current environment, so sync/build it in Android Studio or Cursor.
- Before the first CLI build, copy `local.properties.example` to `local.properties` and point `sdk.dir` to your Android SDK.
- Current debug build output after successful local assemble:
  - `android-app/app/build/outputs/apk/debug/app-debug.apk`

Testing guide:
- `docs/android_v1_testing.md`
