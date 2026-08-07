# Strategy Lab: безопасный исследовательский запуск

## Назначение

`Strategy Lab` объединяет исторические Pump/Dump события, локальную базу и логи
обычного межбиржевого арбитража, а при явном флаге дополняет выбранные события
публичными данными бирж.

Контур строго исследовательский:

- не включает и не выключает ARM;
- не читает приватные ключи;
- не готовит и не отправляет заявки;
- не меняет Pump Live, Grid, Auto Enter/Exit или de-risk настройки;
- любой paper/shadow/live этап оформляется отдельным решением.

## Запуск

Из корня проекта в PowerShell:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab.py
```

С публичным API-обогащением восьми крупнейших событий:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab.py --enrich-public-api --max-api-events 8
```

### Event Lake preflight и bounded pilot

Preflight не создаёт биржевые клиенты и заранее показывает задачи и оценку
публичных запросов:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_event_lake.py --symbols COTIUSDT,HFTUSDT,SIRENUSDT --exchanges binance,bybit --max-events 3
```

Ограниченный публичный replay тех же трёх событий:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_event_lake.py --symbols COTIUSDT,HFTUSDT,SIRENUSDT --exchanges binance,bybit --max-events 3 --execute-public
```

Повторный запуск обязан использовать `windows/` cache, показать
`public_calls_this_run=0` и не добавлять дубли в `ledger.jsonl`.

Результат создаётся в `data/research/strategy_lab/`. Каталог игнорируется Git и
может безопасно пересоздаваться. Исходная SQLite база открывается в режиме
`mode=ro`, а логи и Pump-выгрузки только читаются.

## Основные артефакты

- `index.md` — короткий отчёт и рейтинг гипотез;
- `source_inventory.csv` — покрытие базы и операционных логов;
- `pump_event_catalog.csv` — нормализованные Pump-события;
- `pump_arbitrage_event_links.csv` — совпадения Pump/перекос по символу ±6h;
- `arbitrage_spread_events.csv` — причинные арбитражные якоря;
- `arbitrage_rejected_data_quality.csv` — причины жёсткого veto;
- `arbitrage_hypothesis_summary.csv` — outcomes по группам;
- `funding_persistence_summary.csv` — сохранение знака/величины funding;
- `arbitrage_api_windows.jsonl` — сырые публичные окна;
- `arbitrage_api_summary.csv` — сопоставление локальной и API цены;
- `hypothesis_registry.csv` — приоритет, стадия и следующий тест;
- `metadata.json` — конфигурация и счётчики прогона.

Event Lake пишет отдельный пакет в `data/research/strategy_lab_event_lake/`:

- `manifest.json` — immutable event/task identity и оценка API-бюджета;
- `windows/<task_id>.json` — versioned public-only cache;
- `coverage.csv` — market/contract/OHLCV/funding/OI coverage и ошибки;
- `ledger.jsonl` — append-only `strategy_lab_ledger_v1`;
- `index.md` и `metadata.json` — проверяемая сводка запуска.

Ledger не заменяет и не переписывает существующие Pump Live, auto-exit,
auto-arb, de-risk или protective-shadow журналы. Их нормализация будет отдельным
read-only этапом; новые operational поля допускаются только backward-compatible.

## Трактовка API-погрешности

- `confirmed`: абсолютная разница не больше 0,75 процентного пункта;
- `within_2pct_tolerance`: разница больше 0,75, но не больше 2 п.п.;
- `divergent`: больше 2 п.п.;
- `incomplete`: одной из сторон нет.

Мягкий диапазон 0–2 п.п. применяется только для исследовательского matching.
Он не отменяет проверку contract size, bid/ask, ликвидности и mark/index и не
может сам по себе разрешать paper, shadow или live вход.

## Перед использованием вывода

Проверить `metadata.json`: `mode` должен оставаться
`research_only_no_trading`, `api_error` — пустым. Нулевое количество
`pump_arbitrage_links` ожидаемо при непересекающихся исторических периодах и не
доказывает отсутствие связи. Следующий этап — историческое multi-exchange
обогащение Pump-событий и walk-forward тесты с целыми unseen symbols.
