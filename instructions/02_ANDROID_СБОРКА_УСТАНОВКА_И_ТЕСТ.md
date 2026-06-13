# Сборка, установка и тест Android-приложения FeeArb

## Готовый APK

Основной debug APK после сборки:

```text
C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk
```

Также может существовать версия с датой:

```text
C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\FeeArb-mobile-test-2026-06-07.apk
```

Версия с поддержкой Cloudflare remote token:

```text
C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\FeeArb-cloudflare-ready-2026-06-10.apk
```

## Сборка

Требуются JDK 17, Android SDK и настроенный `android-app/local.properties`.

```powershell
cd C:\Projects\FeeArb\android-app
.\gradlew.bat :app:assembleDebug --no-daemon
```

## Установка на телефон через USB

1. Включите на телефоне `Developer options`.
2. Включите `USB debugging`.
3. Подключите телефон по USB и подтвердите RSA-разрешение.
4. Выполните:

```powershell
& "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe" devices -l
& "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe" install -r "C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk"
```

## Backend URL

Используйте адрес в зависимости от устройства:

```text
Эмулятор Android:       http://10.0.2.2:8000/
Телефон через Tailscale: https://desktop-0tl9bsa.tail3830e2.ts.net:8443/
Телефон через Cloudflare: https://<ПОСТОЯННЫЙ_ДОМЕН>/
```

Для Cloudflare заполните также `Remote access token`. Для локального доступа
и Tailscale оставляйте это поле пустым.

Для телефона через интернет следуйте инструкции:
[`01_ANDROID_ТЕЛЕФОН_ЧЕРЕЗ_TAILSCALE.md`](01_ANDROID_ТЕЛЕФОН_ЧЕРЕЗ_TAILSCALE.md).

## Безопасный порядок проверки

1. Убедитесь, что backend отвечает на компьютере.
2. Откройте `Settings`, задайте backend URL и нажмите `Apply`.
3. Откройте `Balances`, нажмите `Refresh` и проверьте балансы бирж.
4. Откройте `Positions` и проверьте карточки позиций.
5. В `Manual` сначала используйте только `Dry Run`/`Analyze`.
6. Проверьте символ, биржи, направление, количество, chunk size и предупреждения.
7. Не подтверждайте `Execute`, если реальная сделка не запланирована.

## Важно про реальные операции

- `Dry Run`/`Analyze` строит план и не размещает ордера.
- `Execute` после подтверждения может разместить реальные ордера.
- `Exit`, `Add`, `Roll` и Auto Exit работают с реальными позициями при активном live backend.
- Перед частичным выходом проверяйте рассчитанное количество монет и обе ноги позиции.

## Диагностика

Список устройств:

```powershell
& "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe" devices -l
```

Запуск приложения:

```powershell
& "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe" shell am start -n com.feearb.mobile/.MainActivity
```

Логи работающего приложения:

```powershell
$adb="$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe"
$appPid=& $adb shell pidof com.feearb.mobile
& $adb logcat --pid=$appPid
```

Очистка сохраненных настроек приложения:

```powershell
& "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe" shell pm clear com.feearb.mobile
```
