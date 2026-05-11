# Android App Install And Test Guide

Purpose
- Install the current FeeArb Android v1 app on an emulator or a physical phone.
- Configure backend connectivity.
- Run the first smoke test safely.

Files
- Android project: `android-app/`
- Debug APK: `android-app/app/build/outputs/apk/debug/app-debug.apk`
- Testing runbook: `docs/android_v1_testing.md`

## 1. Prerequisites

Backend
- Working Python environment for the existing FeeArb backend
- Exchange/API credentials only if you want to test live data or live execution

Android
- Android Studio or Cursor with Android support
- Android SDK installed
- JDK 17
- `adb` available, or Android Studio device deployment

Network
- Backend reachable from emulator or phone
- Emulator URL: `http://10.0.2.2:8000/`
- Phone URL: `http://<YOUR_PC_LAN_IP>:8000/`

## 2. Start The Backend

From repo root:

```powershell
cd C:\Projects\FeeArb
python main.py
```

If your normal app entrypoint is different, use that instead.

Quick endpoint check:

```powershell
curl http://127.0.0.1:8000/api/mobile/positions
curl http://127.0.0.1:8000/api/mobile/manual-defaults
```

Expected
- Both endpoints return JSON
- `/api/mobile/positions` returns `cards`
- `/api/mobile/manual-defaults` returns `exchanges`, `defaults`, `main_modes`

## 3. Build The APK

If the APK is already present, you can skip this step.

```powershell
cd C:\Projects\FeeArb\android-app
.\gradlew.bat :app:assembleDebug
```

Expected APK:
- `C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk`

## 4. Install On Android Emulator

### Option A: Android Studio

1. Open `C:\Projects\FeeArb\android-app` in Android Studio
2. Open `Device Manager`
3. Create or start an emulator
4. Press `Run app`

### Option B: ADB

1. Start the emulator
2. Install the APK:

```powershell
adb install -r C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk
```

### Emulator App Setup

1. Open the app
2. Go to `Settings`
3. Set `Backend base URL` to:

```text
http://10.0.2.2:8000/
```

4. Press `Apply`
5. Return to `Positions`
6. Press `Refresh`

Expected
- Status line updates
- `Positions` loads cards if backend has active positions
- `Manual` loads exchanges and defaults from backend

## 5. Install On Physical Android Phone

### Network Requirements

1. Phone and PC must be on the same Wi-Fi or reachable network
2. Find your PC IPv4 address:

```powershell
ipconfig
```

Example:
- `192.168.1.50`

3. Use phone backend URL:

```text
http://192.168.1.50:8000/
```

### Important Backend Reachability Note

If the phone cannot reach the backend:
- Verify the backend is not bound only to `127.0.0.1`
- Verify Windows Firewall is not blocking port `8000`
- Verify the phone can open backend URLs in the browser

### Option A: Install With USB + ADB

1. Enable `Developer options` on the phone
2. Enable `USB debugging`
3. Connect the phone by USB
4. Install the APK:

```powershell
adb install -r C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk
```

### Option B: Install Manually

1. Copy `app-debug.apk` to the phone
2. Open the APK on the phone
3. Allow installation from that source if Android asks

### Phone App Setup

1. Open the app
2. Go to `Settings`
3. Set `Backend base URL` to your PC LAN address, for example:

```text
http://192.168.1.50:8000/
```

4. Press `Apply`
5. Return to `Positions`
6. Press `Refresh`

## 6. Connectivity Check

Before testing the app, verify backend URLs in a browser.

Emulator:
- `http://10.0.2.2:8000/api/mobile/positions`
- `http://10.0.2.2:8000/api/mobile/manual-defaults`

Phone:
- `http://<YOUR_PC_LAN_IP>:8000/api/mobile/positions`
- `http://<YOUR_PC_LAN_IP>:8000/api/mobile/manual-defaults`

Expected
- JSON opens successfully

If those URLs work in the browser, the app should be able to connect too.

## 7. Smoke Test Order

Run in this order:

1. Backend endpoints respond
2. App installs and launches
3. `Settings -> Apply` succeeds
4. `Positions -> Refresh` works
5. `Manual -> Analyze` works
6. One `Auto Exit` save works
7. `Manual -> Execute` only if you intentionally want live execution

## 8. Screen-By-Screen Test Checklist

### Positions

Check:
- Cards render without horizontal scrolling
- Filters work:
  - `All`
  - `Risk`
  - `Funding Soon`
  - `Auto Exit On`
- Sorts work:
  - `By PnL`
  - `By Liq Risk`
  - `By Next Funding`
  - `By Symbol`
- One card expands correctly
- Auto-exit target can be edited
- `Save` updates backend

### Manual

Check:
- `Action` switches between `Enter / Exit / Roll`
- `Analyze` returns plan summary
- `Expensive leg = Auto hint` is selectable
- `Execute` first shows a confirmation dialog
- After confirmation, execution status/log updates appear
- `Stop` sends a stop request when execution is running

### Settings

Check:
- Base URL can be changed and applied
- Advanced settings persist:
  - `Max slippage`
  - `Timeout`
  - `Chunk qty`
  - `Chunk notional`
  - `Hedge` fields
  - `Safety` fields

## 9. Safe Testing Levels

Safest
- No real credentials
- UI/render/API-shape validation only
- `Analyze` only

Medium risk
- Real credentials
- `Analyze` only
- Auto-exit rule save only

Highest risk
- Live `Execute`
- Real `Exit` or `Roll`
- Real open positions

For live testing
- Start with the smallest practical size
- Keep `Expensive leg = Auto hint` if you want backend tier/fee/liquidity selection
- Test on one pair first

## 10. Troubleshooting

Emulator cannot connect
- Use `10.0.2.2`, not `localhost`
- Verify backend is running

Phone cannot connect
- Check PC IP
- Check same network
- Check firewall
- Check backend bind address

`adb` not found
- Use Android SDK `platform-tools`
- Or install through Android Studio

APK install fails
- Remove old app version and retry
- Rebuild APK

App shows no positions
- Check `/api/mobile/positions`
- Check backend actually has open positions

Manual has no exchanges
- Check `/api/mobile/manual-defaults`
- Check backend settings for enabled exchanges

## 11. Current Output Files

Primary downloadable file:
- `docs/android_install_and_test_guide.docx`

Source file:
- `docs/android_install_and_test_guide.md`
