# Инструкции FeeArb

Эта папка является основным местом для пользовательских и эксплуатационных инструкций проекта.

Правила сопровождения:

- Новую инструкцию создавать здесь в формате Markdown (`.md`).
- Если подходящая инструкция уже существует, дополнять ее, а не создавать дубликат.
- Давать файлам понятные названия по задаче.
- После изменения поведения приложения одновременно обновлять связанную инструкцию.
- Технические планы, история разработки и внутренние заметки могут оставаться в `docs/`.

## Быстрый выбор

- Телефон с Android через интернет из любого места:
  [`01_ANDROID_ТЕЛЕФОН_ЧЕРЕЗ_TAILSCALE.md`](01_ANDROID_ТЕЛЕФОН_ЧЕРЕЗ_TAILSCALE.md)
- Сборка, установка и безопасная проверка Android-приложения:
  [`02_ANDROID_СБОРКА_УСТАНОВКА_И_ТЕСТ.md`](02_ANDROID_СБОРКА_УСТАНОВКА_И_ТЕСТ.md)
- Разработка и тестирование Android в Cursor:
  [`03_ANDROID_CURSOR_И_ЭМУЛЯТОР.md`](03_ANDROID_CURSOR_И_ЭМУЛЯТОР.md)
- Публичный веб-интерфейс через Tailscale Funnel:
  [`04_WEB_ИНТЕРФЕЙС_ЧЕРЕЗ_TAILSCALE_FUNNEL.md`](04_WEB_ИНТЕРФЕЙС_ЧЕРЕЗ_TAILSCALE_FUNNEL.md)
- Альтернативный веб-доступ через Cloudflare Tunnel:
  [`05_WEB_ИНТЕРФЕЙС_ЧЕРЕЗ_CLOUDFLARE_TUNNEL.md`](05_WEB_ИНТЕРФЕЙС_ЧЕРЕЗ_CLOUDFLARE_TUNNEL.md)
- Технические правила ручной live-торговли:
  [`06_ПРАВИЛА_РУЧНОЙ_LIVE_ТОРГОВЛИ.md`](06_ПРАВИЛА_РУЧНОЙ_LIVE_ТОРГОВЛИ.md)
- Настройка и проверка автоарбитражной сетки без реальных заявок:
  [`07_АВТОАРБИТРАЖ_SHADOW_РЕЖИМ.md`](07_АВТОАРБИТРАЖ_SHADOW_РЕЖИМ.md)
- Live Grid с динамическими чанками, partial retry и принятием существующей позиции:
  [`08_АВТОАРБИТРАЖ_GRID_LIVE_МАЛЫЙ_ОБЪЕМ.md`](08_АВТОАРБИТРАЖ_GRID_LIVE_МАЛЫЙ_ОБЪЕМ.md)
- Live-конструктор Auto Enter / Auto Exit, очередь и частичное исполнение:
  [`09_LIVE_АВТОСТРАТЕГИИ_ВХОДА_И_ВЫХОДА.md`](09_LIVE_АВТОСТРАТЕГИИ_ВХОДА_И_ВЫХОДА.md)
- Раздельная проверка защитных Shadow-контуров и small-volume Live для orphan/de-risk:
  [`10_ЗАЩИТНЫЕ_СТРАТЕГИИ_SHADOW_И_SMALL_VOLUME.md`](10_ЗАЩИТНЫЕ_СТРАТЕГИИ_SHADOW_И_SMALL_VOLUME.md)
- Исследование полного цикла pump/squeeze, long-стратегий и short-exhaustion:
  [`11_PUMP_LIFECYCLE_RESEARCH.md`](11_PUMP_LIFECYCLE_RESEARCH.md)
- Обязательный порядок ведения Git, проверки и логические коммиты:
  [`12_GIT_ПОРЯДОК_И_КОММИТЫ.md`](12_GIT_ПОРЯДОК_И_КОММИТЫ.md)
- Первый live-canary Pump/Dump на отдельном Bybit subaccount:
  [`13_PUMP_LIVE_BYBIT_SUBACCOUNT.md`](13_PUMP_LIVE_BYBIT_SUBACCOUNT.md)
- Unified read-only web view for main and Pump Live positions:
  [`14_UNIFIED_POSITIONS_WEB.md`](14_UNIFIED_POSITIONS_WEB.md)
- Лёгкая Android-визуализация Main и Pump Live позиций:
  [`15_ANDROID_MAIN_PUMP_POSITIONS.md`](15_ANDROID_MAIN_PUMP_POSITIONS.md)
- Исследовательский Strategy Lab для Pump/Dump, арбитражных перекосов и публичного API:
  [`16_STRATEGY_LAB.md`](16_STRATEGY_LAB.md)
- Живой пошаговый roadmap и журнал Strategy Lab для следующих ИИ-агентов:
  [`17_STRATEGY_LAB_ROADMAP.md`](17_STRATEGY_LAB_ROADMAP.md)
- Канонический план prospective-сбора funding/spread кандидатов, multi-venue median
  и roll для Strategy Lab Candidate Observatory:
  [`18_STRATEGY_LAB_CANDIDATE_OBSERVATORY.md`](18_STRATEGY_LAB_CANDIDATE_OBSERVATORY.md)
- Канонический реестр active/retired модулей и безопасный порядок большого
  рефакторинга:
  [`19_РЕЕСТР_МОДУЛЕЙ_И_РЕФАКТОРИНГ.md`](19_РЕЕСТР_МОДУЛЕЙ_И_РЕФАКТОРИНГ.md)

## Важно

Для нативного Android-приложения используется приватный Tailscale Serve:

```text
https://desktop-0tl9bsa.tail3830e2.ts.net:8443/
```

Публичный адрес без `:8443` относится к Funnel и не должен использоваться как backend URL Android-приложения.

После завершения DNS-обновления основной Android URL:

```text
https://app.feearb.ru/
```

Он требует `Remote access token`. Tailscale URL остается резервным.
