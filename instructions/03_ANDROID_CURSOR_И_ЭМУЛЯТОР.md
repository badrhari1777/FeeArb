# Android FeeArb в Cursor и эмуляторе

## Окружение проекта

- Android SDK: `C:\Users\Pavel\AppData\Local\Android\Sdk`
- JDK: версия 17
- Рекомендуемый эмулятор: `Pixel_8`
- Android-модуль: `C:\Projects\FeeArb\android-app`

## Рекомендуемые расширения Cursor

```text
jetbrains.kotlin-server
vscjava.vscode-gradle
redhat.vscode-xml
```

Установка:

```powershell
cursor --install-extension jetbrains.kotlin-server
cursor --install-extension vscjava.vscode-gradle
cursor --install-extension redhat.vscode-xml
```

## Готовые задачи Cursor

Откройте `Terminal -> Run Task`:

- `Android: Build debug APK`
- `Android: List devices`
- `Android: Start Pixel 8 emulator`
- `Android: Install debug APK`
- `Android: Launch FeeArb`
- `Android: FeeArb logcat`
- `Android: Clear app data`

Для обычного запуска:

1. Запустите backend FeeArb.
2. Выполните `Android: Start Pixel 8 emulator`.
3. Дождитесь загрузки Android.
4. Выполните `Android: Launch FeeArb`.

## URL backend в эмуляторе

В `Settings -> Backend base URL` укажите:

```text
http://10.0.2.2:8000/
```

Адрес `10.0.2.2` внутри стандартного Android Emulator указывает на локальный компьютер.

## Первый тест

1. Нажмите `Apply` в `Settings`.
2. Откройте `Balances` и нажмите `Refresh`.
3. Проверьте `Positions`.
4. Откройте `Manual`.
5. Выполните только `Dry Run`/`Analyze`.
6. Не подтверждайте `Execute` во время первичной проверки UI.
