# Тестирование FeeArb Android в Cursor

## Текущее состояние окружения

На этом компьютере уже установлены:

- Cursor и его CLI `cursor`.
- JDK 17.
- Android SDK: `C:\Users\Pavel\AppData\Local\Android\Sdk`.
- Android Platform Tools с `adb.exe`.
- Android Emulator.
- Android API 34, 36 и 36.1.
- Виртуальные устройства `Pixel_8` и `Medium_Phone_API_36.1`.
- Gradle Wrapper внутри `android-app`.
- Настроенный `android-app/local.properties`.

Проект собирается командой:

```powershell
cd C:\Projects\FeeArb\android-app
.\gradlew.bat :app:assembleDebug --no-daemon
```

Готовый APK:

```text
C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk
```

## Расширения Cursor

Откройте в Cursor `Extensions` сочетанием `Ctrl+Shift+X`.

Рекомендуемые:

1. `Kotlin by JetBrains`
   - ID: `jetbrains.kotlin-server`
   - Подсветка, навигация, диагностика и автодополнение Kotlin.
   - Поддержка Android Gradle Plugin пока экспериментальная, поэтому истинным результатом проверки остается Gradle-сборка.

2. `Gradle for Java`
   - ID: `vscjava.vscode-gradle`
   - Показывает Gradle-проекты и задачи в боковой панели.

3. `XML`
   - ID: `redhat.vscode-xml`
   - Удобен для `AndroidManifest.xml` и Android resources.

Необязательные:

- Android Tools for VS Code: `levkosyk.vscode-android-tools`.
- Logcat Lens: `AshishKumarD.logcat-lens`.

Для сборки, установки и запуска приложения Android-плагин не обязателен. В проект добавлены готовые Cursor tasks, работающие напрямую через Gradle и Android SDK.

Cursor покажет рекомендуемые расширения при открытии проекта. Их также можно установить из терминала:

```powershell
cursor --install-extension jetbrains.kotlin-server
cursor --install-extension vscjava.vscode-gradle
cursor --install-extension redhat.vscode-xml
```

## Как открыть проект

Открывайте корень всего проекта, а не только Android-модуль:

```powershell
cursor C:\Projects\FeeArb
```

Android-код находится в:

```text
android-app\app\src\main\java\com\feearb\mobile
```

После установки Kotlin-плагина откройте любой `.kt` файл и дождитесь завершения импорта и индексации.

## Готовые задачи Cursor

Откройте:

```text
Terminal -> Run Task
```

Доступны задачи:

- `Android: Build debug APK`
- `Android: List devices`
- `Android: Start Pixel 8 emulator`
- `Android: Install debug APK`
- `Android: Launch FeeArb`
- `Android: FeeArb logcat`
- `Android: Clear app data`

Для обычного запуска достаточно:

1. Запустить backend.
2. Выполнить `Android: Start Pixel 8 emulator`.
3. Дождаться загрузки Android.
4. Выполнить `Android: Launch FeeArb`.

Задача запуска сама сначала соберет и установит APK.

## Backend для эмулятора

Backend должен работать на компьютере:

```powershell
cd C:\Projects\FeeArb
.\.venv\Scripts\python.exe -m uvicorn webapp.app:app --host 127.0.0.1 --port 8000
```

Проверьте:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/mobile/positions
Invoke-RestMethod http://127.0.0.1:8000/api/mobile/manual-defaults
```

В Android-эмуляторе используйте:

```text
http://10.0.2.2:8000/
```

`10.0.2.2` внутри стандартного Android Emulator указывает на `127.0.0.1` компьютера.

## Первый тест на эмуляторе

1. Откройте `Settings`.
2. Укажите `http://10.0.2.2:8000/`.
3. Нажмите `Apply`.
4. Откройте `Balances`.
5. Проверьте общий итог, доступные и используемые средства, затем карточки отдельных бирж.
6. Откройте `Positions` и проверьте карточки позиций, фильтры и сортировку.
7. Обновите данные.
8. Откройте `Manual`.
9. Заполните символ и биржи.
10. Запустите только `Dry Run` или `Analyze` и проверьте рассчитанное количество, chunk size, spread и предупреждения.

Не нажимайте подтверждение `Execute` во время первого UI-теста: этот запрос может отправить реальные ордера.

## Проверка Auto Exit

Проводите только на позиции, для которой действительно нужен auto-exit.

1. Включите нужную галочку.
2. Обновите экран и убедитесь, что она осталась включенной.
3. Закройте и снова откройте приложение.
4. Убедитесь, что настройка загрузилась с backend.
5. Временно отключите сеть эмулятора.
6. Включите сеть обратно и обновите экран.
7. Проверьте, что правило не исчезло после временной потери связи.

## Логи и диагностика

Запустите задачу:

```text
Android: FeeArb logcat
```

Либо используйте терминал:

```powershell
$adb="$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe"
$appPid=& $adb shell pidof com.feearb.mobile
& $adb logcat --pid=$appPid
```

Список устройств:

```powershell
& "$env:LOCALAPPDATA\Android\Sdk\platform-tools\adb.exe" devices -l
```

Если приложение ведет себя как после старой установки, выполните:

```text
Android: Clear app data
```

После этого заново задайте backend URL.

## Тест на Samsung

1. Включите `Developer options`.
2. Включите `USB debugging`.
3. Подключите телефон кабелем.
4. Подтвердите RSA-разрешение на телефоне.
5. Выполните `Android: List devices`.
6. Выполните `Android: Install debug APK`.

Для телефона URL `10.0.2.2` не подходит. Используйте LAN IP компьютера:

```text
http://192.168.x.x:8000/
```

Backend для LAN должен слушать `0.0.0.0`, а порт 8000 должен быть разрешен Windows Firewall:

```powershell
.\.venv\Scripts\python.exe -m uvicorn webapp.app:app --host 0.0.0.0 --port 8000
```

Сначала откройте `/api/mobile/positions` в браузере телефона. Только после этого проверяйте приложение.

## Критерий готовности

Окружение готово к тестированию, если:

- `Android: Build debug APK` завершается успешно.
- Эмулятор отображается в `Android: List devices`.
- APK устанавливается без ошибки.
- FeeArb запускается.
- Оба mobile endpoint возвращают `status: ready`.
- `Balances` получает балансы и считает общий итог, `Positions` получает карточки позиций.
- `Manual -> Analyze/Dry Run` работает без отправки ордеров.
