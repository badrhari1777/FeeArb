# Подробная инструкция по установке и тестированию Android-приложения FeeArb

Назначение
- Дать один практический сценарий: что запускать, где тестировать и в каком порядке идти от безопасной проверки UI до реального smoke test.
- Снизить риск случайного live-submit во время первого прогона Android v1.

Что сейчас входит в Android v1
- Экран `Positions`
- Экран `Manual`
- Экран `Settings`

Какие backend API использует приложение
- `GET /api/mobile/positions`
- `GET /api/mobile/manual-defaults`
- `POST /api/auto-exit/rule`
- `POST /api/manual/analyze`
- `POST /api/manual/enter`
- `POST /api/manual/exit`
- `POST /api/manual/roll`
- `GET /api/manual/exec/{id}`
- `POST /api/manual/exec/{id}/stop`

Файлы
- Android-проект: `android-app/`
- APK debug-сборки: `android-app/app/build/outputs/apk/debug/app-debug.apk`
- Англоязычный technical runbook: `docs/android_v1_testing.md`
- Краткий Android README: `android-app/README.md`

## 1. Где именно тестировать

Рекомендуемый порядок такой:

1. Эмулятор Android
   - Нужен для первой проверки сборки, запуска приложения, настроек base URL, загрузки `Positions`, `Manual defaults` и общей UI-логики.
   - Здесь удобно ловить грубые ошибки layout, null/empty state, недоступность backend и проблемы с сохранением локальных настроек.

2. Физический Android-телефон в той же сети, что и backend
   - Нужен для реального операторского прогона.
   - Здесь проверяйте удобство формы, читаемость карточек, реакцию на плохую сеть, фактический доступ до backend по LAN/Tailscale/HTTPS.

3. Live execution
   - Делать только на физическом телефоне и только после того, как полностью прошли безопасные шаги `Analyze` и убедились, что payload выглядит правильно.
   - Не начинать с live `Exit` или `Roll` на реальных позициях.

Коротко:
- Эмулятор: сборка, запуск, UI, базовая связность.
- Телефон: реальный пользовательский smoke test.
- Live execute: только в самом конце.

## 2. Что нужно заранее

Backend
- Рабочее Python-окружение для FeeArb.
- Реальные API-ключи нужны только если вы хотите тестировать настоящие позиции или live execution.

Android
- Android Studio или Cursor с поддержкой Android.
- Android SDK.
- JDK 17.
- `adb` в PATH, либо установка через Android Studio.

Сеть
- Backend должен быть доступен с того устройства, где вы тестируете.
- Для эмулятора используйте `http://10.0.2.2:8000/`.
- Для физического телефона используйте `http://<IP_ВАШЕГО_ПК_В_СЕТИ>:8000/`.
- Если у вас backend открыт через Tailscale/Caddy/HTTPS, используйте полный доступный URL, который реально открывается с телефона.

## 3. Подготовка backend

Из корня репозитория:

```powershell
cd C:\Projects\FeeArb
python main.py
```

Если в вашей локальной среде backend обычно запускается иначе, используйте штатный для вас способ.

Перед Android обязательно проверьте два endpoint вручную:

```powershell
curl http://127.0.0.1:8000/api/mobile/positions
curl http://127.0.0.1:8000/api/mobile/manual-defaults
```

Ожидаемый результат:
- Оба URL отвечают JSON.
- `/api/mobile/positions` возвращает объект с `cards`.
- `/api/mobile/manual-defaults` возвращает `exchanges`, `defaults`, `main_modes`.

Если эти два URL не работают на ПК, приложение тоже не заработает. Сначала чините backend, потом Android.

## 4. Сборка APK

Если `app-debug.apk` уже существует и вы уверены, что он свежий, этот шаг можно пропустить.

Если сборки еще нет:

```powershell
cd C:\Projects\FeeArb\android-app
copy local.properties.example local.properties
```

После этого откройте `android-app/local.properties` и укажите путь к вашему Android SDK в `sdk.dir`.

Затем соберите debug APK:

```powershell
cd C:\Projects\FeeArb\android-app
.\gradlew.bat :app:assembleDebug
```

Ожидаемый результат:
- Сборка завершается без ошибки.
- Появляется файл:

```text
C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk
```

Типовые причины провала сборки:
- не установлен Android SDK
- не заполнен `local.properties`
- неверный `sdk.dir`
- не установлены platform/build-tools

## 5. Установка и первый прогон на эмуляторе

Это первый обязательный этап. На нем нужно убедиться, что приложение вообще запускается и может общаться с backend.

### Вариант A: через Android Studio

1. Откройте `C:\Projects\FeeArb\android-app` в Android Studio.
2. Дождитесь Gradle sync.
3. Если IDE просит доустановить Android packages, установите их.
4. Откройте `Device Manager`.
5. Создайте или запустите эмулятор.
6. Нажмите `Run app`.

### Вариант B: через `adb`

1. Запустите эмулятор.
2. Установите APK:

```powershell
adb install -r C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk
```

### Первая настройка приложения в эмуляторе

1. Откройте приложение FeeArb.
2. Перейдите в `Settings`.
3. В поле `Backend base URL` укажите:

```text
http://10.0.2.2:8000/
```

4. Нажмите `Apply`.
5. Вернитесь в `Positions`.
6. Нажмите `Refresh`.

Ожидаемый результат:
- Верхний статус меняется.
- Нет ошибки подключения.
- Если backend видит открытые позиции, появляются карточки.
- Если позиций нет, вы все равно должны увидеть штатный экран `No positions`, а не пустой или сломанный UI.

## 6. Установка и прогон на физическом телефоне

Это второй обязательный этап. Именно здесь лучше всего делать операторский smoke test.

### 6.1. Подготовка сети

1. Подключите телефон и ПК к одной сети Wi-Fi, либо обеспечьте реальный сетевой доступ телефона до backend.
2. Узнайте IPv4 адрес ПК:

```powershell
ipconfig
```

Пример:
- `192.168.1.50`

3. Проверьте, что backend доступен не только на `127.0.0.1`.
4. Если нужно, разрешите порт `8000` в Windows Firewall.
5. На телефоне в браузере откройте:

```text
http://192.168.1.50:8000/api/mobile/positions
http://192.168.1.50:8000/api/mobile/manual-defaults
```

Если эти URL не открываются с телефона, приложение тоже не подключится.

### 6.2. Установка APK на телефон

Вариант через USB + `adb`:

1. Включите на телефоне `Developer options`.
2. Включите `USB debugging`.
3. Подключите телефон по USB.
4. Выполните:

```powershell
adb install -r C:\Projects\FeeArb\android-app\app\build\outputs\apk\debug\app-debug.apk
```

Вариант вручную:

1. Скопируйте `app-debug.apk` на телефон.
2. Откройте APK на телефоне.
3. Разрешите установку из этого источника, если Android запросит.

### 6.3. Настройка base URL на телефоне

1. Откройте приложение.
2. Перейдите в `Settings`.
3. В `Backend base URL` укажите LAN или HTTPS/Tailscale URL, например:

```text
http://192.168.1.50:8000/
```

4. Нажмите `Apply`.
5. Вернитесь в `Positions`.
6. Нажмите `Refresh`.

## 7. Обязательный порядок теста

Идите строго в таком порядке:

1. Backend endpoints работают на ПК.
2. Backend endpoints открываются с телефона или эмулятора.
3. Приложение ставится и запускается.
4. `Settings -> Apply` отрабатывает без ошибок.
5. `Positions -> Refresh` работает.
6. `Manual defaults` загружаются, экран `Manual` не пустой.
7. `Analyze` проходит хотя бы на одном сценарии.
8. Сохранение одного `Auto Exit` правила работает.
9. Только после этого допускается `Execute`.

Не перепрыгивайте сразу к `Execute`. В Android v1 это самый рискованный шаг.

## 8. Подробный сценарий тестирования по экранам

### 8.1. Экран `Positions`

Где тестировать:
- Сначала на эмуляторе.
- Потом обязательно на физическом телефоне.

Что делать:
1. Откройте `Positions`.
2. Нажмите `Refresh`.
3. Проверьте, что верхний статус меняется на что-то вроде обновления, а потом на успешный результат или понятную ошибку.
4. Переключите фильтры:
   - `All`
   - `Risk`
   - `Funding Soon`
   - `Auto Exit On`
5. Переключите сортировки:
   - `By PnL`
   - `By Liq Risk`
   - `By Next Funding`
   - `By Symbol`
6. Если есть карточки, откройте хотя бы одну через `Expand`.
7. Просмотрите блоки:
   - `Position Summary`
   - `Risk`
   - `Funding`
   - `Legs`
8. В блоке `Auto Exit`:
   - переключите `Spread Exit`
   - задайте `Target spread %`
   - нажмите `Save`
9. Нажмите `Refresh` еще раз и убедитесь, что сохраненное состояние подтянулось обратно.

Что должно получиться:
- Карточки не ломают layout.
- Фильтры и сортировки реально меняют набор/порядок карточек.
- Раскрытие/сворачивание работает.
- `Save` не приводит к падению приложения.
- После refresh состояние auto-exit не теряется.

Практическая заметка:
- Кнопка `Manual` в карточке предзаполняет форму ручной сделки данными позиции. Это удобно для следующего шага.

### 8.2. Экран `Manual`: безопасный этап `Analyze`

Где тестировать:
- Эмулятор и телефон.
- Это основной этап первого реального smoke test.

Что важно понимать:
- `Analyze` не отправляет живые ордера.
- Это dry-run/preflight, который строит план и показывает ошибки/предупреждения.
- Если `Chunk qty` пустой, приложение может автоматически подставить `recommended_chunk_qty` или `min_chunk_qty` из backend-плана.

Что делать для сценария `Enter`:
1. Откройте `Manual`.
2. Убедитесь, что загрузились `Manual defaults`.
3. Выберите `Action = Enter`.
4. Заполните:
   - `Symbol`
   - `Qty` или `Notional`
   - `Long exchange`
   - `Short exchange`
   - `Mode = Smart` или `Fast`
5. Оставьте `Expensive leg = Auto hint`, если не хотите руками выбирать первую ногу.
6. Нажмите `Analyze`.

Что должно получиться:
- Появляется `Plan Summary`.
- Если backend не согласен с параметрами, ошибки отображаются явно.
- Если параметров достаточно, статус показывает успешный анализ.

Что делать для сценария `Exit`:
1. Откройте существующую позицию на `Positions`.
2. Нажмите `Manual` на карточке.
3. Переключите `Action = Exit`.
4. Уточните `Symbol`, `Long exchange`, `Short exchange`.
5. Если позиция уже есть на backend, можно оставить `Qty` пустым и дать backend вывести размер из позиции.
6. Нажмите `Analyze`.

Что делать для сценария `Roll`:
1. Выберите `Action = Roll`.
2. Проверьте, что появились поля:
   - `Roll mode`
   - `From exchange`
   - `To exchange`
   - `Side`
3. Заполните их.
4. Нажмите `Analyze`.

Что должно получиться:
- Для `Roll` приложение отправляет roll-specific payload.
- `Expensive leg = Auto hint` остается доступным.
- `Plan Summary` отражает результат preflight.

### 8.3. Экран `Settings`

Где тестировать:
- Эмулятор достаточно для первой проверки.
- На телефоне проверить повторно после реальной смены base URL.

Что делать:
1. Откройте `Settings`.
2. Проверьте блок `Connection`:
   - измените `Backend base URL`
   - нажмите `Apply`
3. Проверьте блок `Execution`:
   - `Max slippage bps`
   - `Timeout sec`
   - `Max runtime sec`
   - `Reprice sec`
4. Проверьте блок `Chunking`:
   - `Chunk qty`
   - `Chunk notional`
   - `Force chunk qty`
5. Проверьте блок `Hedge`:
   - `Hedge order type`
   - `Hedge limit mode`
   - `Favorable bps`
   - `Adverse bps`
6. Проверьте блок `Safety`:
   - `Limit offset bps`
   - `Limit offset ticks`
   - `Max limit deviation bps`
   - `Use orderbook check`
   - `Exit allow flip`
   - `Margin mode`
7. Вернитесь в `Manual` и снова запустите `Analyze`.

Что должно получиться:
- Настройки сохраняются локально.
- После возврата в `Manual` они используются в запросах.
- Поля `Max slippage (Settings)` и `Margin mode (Settings)` на экране `Manual` показывают текущие значения из `Settings`.

## 9. Когда и как тестировать `Execute`

`Execute` тестируйте только после того, как:
- backend стабильно отвечает,
- `Positions` работает,
- `Manual defaults` грузятся,
- `Analyze` прошел успешно хотя бы на одном сценарии,
- вы понимаете, какой payload уйдет на backend.

Что делает приложение сейчас:
- При нажатии `Execute` сначала запускается preflight.
- Если в preflight есть ошибки, live submit не идет.
- Если preflight чистый, приложение показывает confirmation dialog.
- Только после подтверждения отправляется реальный запрос.
- Если backend запускает async execution, приложение начинает опрашивать статус через `/api/manual/exec/{id}`.

Рекомендуемый порядок live smoke test:
1. Делайте это только на физическом телефоне.
2. Выберите самый маленький практический размер.
3. Начинайте с самой понятной пары/символа.
4. Лучше первый live smoke test делать на `Enter`, а не на `Exit`/`Roll`, если это допустимо вашей операционной логикой.
5. После `Execute` внимательно следите за:
   - `Execution id`
   - `Status`
   - блоком `Execution`
6. Если выполнение затянулось, проверьте `Stop`.

Что должно получиться:
- До подтверждения live submit не уходит.
- После подтверждения появляется `execution_id`, либо backend возвращает понятную ошибку.
- Статус меняется во время poll.
- Кнопка `Stop` отправляет stop request.

## 10. Безопасная схема первого smoke test

Если хотите пройти самый безопасный первый прогон, делайте так:

1. Запустите backend без реальных ключей или без намерения торговать.
2. На эмуляторе проверьте:
   - запуск приложения
   - `Settings`
   - `Positions`
   - `Manual defaults`
3. На телефоне проверьте те же шаги через реальный сетевой URL.
4. Пройдите `Analyze` для `Enter`.
5. Если есть открытая позиция, отдельно пройдите `Analyze` для `Exit`.
6. Сохраните одно правило `Auto Exit`.
7. Только после этого принимайте решение, нужен ли live `Execute`.

## 11. Что делать, если что-то не работает

Эмулятор не подключается к backend
- Используйте `10.0.2.2`, а не `localhost`.
- Проверьте, что backend реально запущен.
- Проверьте `Settings -> Backend base URL`.

Телефон не подключается
- Проверьте IP вашего ПК.
- Проверьте, что телефон и ПК в одной сети.
- Проверьте firewall.
- Проверьте, что backend слушает внешний интерфейс, а не только loopback.
- Попробуйте открыть `/api/mobile/positions` с телефона в браузере.

На `Manual` нет бирж
- Проверьте `/api/mobile/manual-defaults`.
- Проверьте enabled exchanges в backend settings.

`Analyze` падает
- Проверьте, что заполнен `Symbol`.
- Для `Enter`/`Roll` проверьте, что указан `Qty` или `Notional`.
- Посмотрите `Plan Summary`: ошибки там важнее всего.

`Execute` не стартует
- Значит preflight не прошел, либо backend вернул ошибку.
- Смотрите `Plan Summary` и `manualStatusText`.
- Не пытайтесь повторять `Execute`, пока preflight не стал чистым.

Нет позиций на `Positions`
- Проверьте `/api/mobile/positions`.
- Если backend честно возвращает ноль позиций, приложение должно показать `No positions`.
- Это не ошибка UI само по себе.

`adb` не найден
- Используйте Android SDK `platform-tools`.
- Или ставьте приложение через Android Studio.

## 12. Что записывать по итогам теста

После каждого прогона желательно зафиксировать:
- где тестировали: эмулятор или физический телефон
- какой base URL использовали
- прошел ли `Positions -> Refresh`
- прошел ли `Manual -> Analyze`
- удалось ли сохранить `Auto Exit`
- запускали ли `Execute`
- если был сбой, на каком именно шаге и какой текст ошибки показало приложение

Минимальный полезный отчет:
- устройство
- версия APK
- способ подключения к backend
- шаг воспроизведения
- фактический результат
- ожидаемый результат

## 13. Критерий “Android v1 можно тестировать дальше”

Считайте текущую версию пригодной для дальнейшего операторского тестирования, если все ниже выполнено:
- Android Studio sync проходит
- `.\gradlew.bat :app:assembleDebug` проходит
- APK ставится
- приложение открывается
- `Settings -> Apply` работает
- `Positions -> Refresh` работает
- `Manual defaults` загружаются
- хотя бы один `Analyze` проходит
- хотя бы одно `Auto Exit` правило сохраняется
- при необходимости `Execute` доходит до confirmation dialog и корректно стартует после подтверждения
