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

Source-specific public provider собирает также `mark`, `index` и `premium`
5m klines. Для historical OI действуют разные правила:

- Binance `/futures/data/openInterestHist` официально хранит только последний
  месяц; Event Lake использует консервативный cutoff 30 дней и старые окна
  помечает `retention_gap` без бесполезного запроса;
- Bybit `/v5/market/open-interest` допускает историю до запуска символа;
  запрос обязательно ограничивается `endTime` и листается назад, иначе API
  возвращает свежий хвост вместо старого окна.

Официальные спецификации: [Binance Futures market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[Bybit OI](https://bybit-exchange.github.io/docs/v5/market/open-interest),
[Bybit mark](https://bybit-exchange.github.io/docs/v5/market/mark-kline),
[Bybit index](https://bybit-exchange.github.io/docs/v5/market/index-kline),
[Bybit premium](https://bybit-exchange.github.io/docs/v5/market/premium-index-kline).

Оценка полного каталога без сетевых запросов:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_event_lake.py --output-dir data\research\strategy_lab_event_lake_v4_full --all-events --max-events 999999
```

Это zero-network preflight полного запуска. Он обязан показать `1 540`
логических событий, `3 080` logical tasks и `2 216` физических окон. После
отдельного подтверждения оператора тот же resumable public-only сбор запускается
с добавлением `--execute-public`.

Event Lake v4 хранит физический cache по
`exchange + symbol + start/end + timeframe`. Несколько source-specific
`event_id` могут ссылаться на один immutable window-файл, но для каждого
logical event сохраняются отдельные task, coverage и ledger record. Флаг
`--all-events` обязателен для полного каталога; без него сохраняется безопасный
bounded default — только последнее событие каждого выбранного символа.

Локальный архив без сети:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_local_archive.py --symbols COTIUSDT,HFTUSDT,SIRENUSDT --exchanges binance,bitget,bybit,kucoin,mexc,okx
```

Первый запуск строит SHA-проверяемый byte-offset index примерно 1,92 GiB
per-symbol JSONL. Повторный запуск должен показать `index_reused=true` и не
читать гигабайтные строки последовательно заново.

Zero-network merge локальных и public-cache окон:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_merge.py
```

Merge выбирает источник целиком для каждого dataset: более точный OHLCV, затем
лучшее покрытие. Строки источников не смешиваются; hashes остаются в provenance.

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
- `windows/<physical_window_id>.json` — immutable versioned public-only cache;
- `coverage.csv` — market/contract/OHLCV/funding/OI/mark/index/premium coverage;
- `ledger.jsonl` — append-only `strategy_lab_ledger_v1`;
- `index.md` и `metadata.json` — проверяемая сводка запуска.
- `full_run_estimate.json` — calls/disk/runtime для полного каталога без сети.

Ledger append защищён коротким межпроцессным lock и повторно проверяет
`record_id` непосредственно перед `fsync`. Одновременный одинаковый запуск не
должен создавать дубли; stale lock удаляется только если его PID уже не жив.

Структурная проверка активного checkpoint без ожидания завершения:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_validate_event_lake.py data\research\strategy_lab_event_lake_v4_full --allow-in-progress
```

После завершения флаг `--allow-in-progress` убирается. Строгий validator читает
каждый physical window, пересчитывает его hash и проверяет manifest, coverage,
logical event/exchange ledger key, `features_ref`, `record_id`, run/config/source
identity и итоговые metadata counts. Любая недостающая или лишняя cache-запись,
битый JSON, duplicate identity или несоответствие hash завершает проверку
ошибкой; missing market/dataset остаётся честным исследовательским coverage, а
не структурной ошибкой.

Zero-call replay обязан сохранить provenance исходного collector commit, даже
если документация или validator уже создали новые Git commits:

```powershell
$eventLakeManifest = Get-Content -Raw data\research\strategy_lab_event_lake_v4_full\manifest.json | ConvertFrom-Json
.\.venv\Scripts\python.exe scripts\strategy_lab_event_lake.py --output-dir data\research\strategy_lab_event_lake_v4_full --all-events --max-events 999999 --execute-public --code-commit $eventLakeManifest.code_commit
```

Ожидается `public_calls_this_run=0`, `status_counts.cache_reused=3080` и те же
`3 080` ledger records. Не запускать audited replay без `--code-commit` после
изменения HEAD: cache останется тем же, но manifest provenance будет переписан.

### Funding Forecast v1

Строгий причинный replay запускается только после полного Event Lake и успешного
post-run gate:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_funding_forecast.py --input-dir data\research\strategy_lab_event_lake_v4_full --output-dir data\research\strategy_lab_funding_forecast_v1
```

Для проверки кода на незавершённом collection разрешён только явно ограниченный
пилот. Его результаты не являются выводом модели и не разрешают paper/shadow:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_funding_forecast.py --input-dir data\research\strategy_lab_event_lake_v4_full --output-dir data\research\strategy_lab_funding_forecast_v1_partial --allow-in-progress --max-windows 120
```

Модуль использует только признаки с `ts <= event_ts`, а targets — только строки
с `ts > event_ts`. Отсутствующие значения не превращаются в нули: для модели
используются train-only median и отдельный missing-индикатор. Окно другой биржи
может дать известный на момент события premium/OI-контекст даже без будущей
funding-метки, но не становится размеченным train sample. Не наблюдавшаяся до
конца окна смена знака является right-censored и исключается из обычной
регрессии длительности.

Funding period учитывается двумя разными способами:

- реализованные ставки суммируются по фактическим settlement timestamps за
  `1/4/8/24h`, поэтому восемь часовых платежей не равны одному 8h-платежу;
- ставка одного settlement нормализуется как `bps/hour` по последнему причинно
  наблюдаемому интервалу. Отдельно сохраняются median interval, latest interval,
  отношение latest к предыдущей median и причинная оценка времени до следующего
  settlement.

Targets включают следующую ставку и в `bps/settlement`, и в `bps/hour`, следующий
фактический интервал, а также cumulative funding за `4/8/24h`. Экономический
proxy считается по реальным будущим settlement-строкам для costs
`0/4/8/12/16 bps`. Миллисекундный шум timestamps около стандартных интервалов
`1/2/4/8h` нормализуется, чтобы модель не учила технический jitter.

Historical Event Lake не содержит достоверной point-in-time instrument metadata.
Поэтому внезапная смена, например `8h -> 1h`, причинно видна только после первого
settlement с новым разрывом. Нельзя подставлять современный `fundingInterval` в
старое событие: это look-ahead. В будущем shadow может использовать свежие
instrument metadata и exchange `nextFundingTime` как отдельные observed-at
features.

Пакет `data/research/strategy_lab_funding_forecast_v1/` содержит:

- `samples.csv` и `vetoes.csv` — eligible causal samples и fail-closed причины;
- `metrics.csv` — chronological/unseen-symbol baselines и модели;
- `calibration.csv`, `coefficients.csv`, `predictions.csv` — аудит прогноза;
- `veto_summary.csv`, `metadata.json`, `index.md` — coverage и provenance.

Full evaluation от 2026-08-07 выполнен после строгого полного gate. Event Lake
содержит `2 216/2 216` физических окон, `3 080` logical records и прошёл
повторный replay с `0` API calls. Forecast получил `1 024` eligible samples на
`283` symbols. Next-sign model превзошла persistence baseline в трёх
последовательных временных OOS-блоках (`77,48/82,28/91,49%` против
`70,20/77,22/88,65%`) и на unseen symbols (`78,16%` против `69,42%`).

Этот результат остаётся `research_evaluation_only`. Regression величины
funding проиграла current-rate baseline, а в одном временном блоке один symbol
составил `51,51%` абсолютного funding-proxy результата. Поэтому
`paper_promotion_allowed=false` и `shadow_promotion_allowed=false`. Sign forecast
можно использовать только как кандидатный признак будущего Stage 3; следующий
безопасный шаг — execution-aware spread timing с bid/ask, liquidity, fees,
slippage, capacity и фактическими funding cashflows.

`metrics.csv` дополнительно хранит `top1_abs_contribution_share`,
`top5_abs_contribution_share`, `top_symbol_abs_contribution_share` и
`mean_gross_without_top5_abs_bps`. Эти поля обязательны для проверки, что
результат не определяется несколькими экстремальными событиями или одной
монетой.

### Executable Spread Timing v1

Следующий research-only replay обычной арбитражной базы:

```powershell
.\.venv\Scripts\python.exe scripts\strategy_lab_executable_spread.py --source-max-ts-ms 1786221561852
```

Модуль открывает `state/coin_analysis.db` в `mode=ro`, фиксирует один WAL
snapshot с явным canonical cutoff и сохраняет его counts/min/max/hash. Он использует первые причинные
spread triggers, historical top-of-book bid/ask и сравнивает `now`,
`+5m/+15m/+30m`, а также causal `expansion_stop`. Каждый вход оценивается через
`15m/1h/4h/8h`; выходной net включает фактические funding settlements и
раздельные fee/slippage scenarios. Missing funding schedule, quotes, mark или
exit не превращаются в ноль и дают `VETO`.

Артефакты в `data/research/strategy_lab_executable_spread_v1/`:

- `timing_outcomes.csv` — event/policy/horizon directed bid/ask outcomes;
- `timing_summary.csv` — chronological summaries, MAE/MFE и concentration;
- `veto_summary.csv` и `source_quality_rejections.csv` — fail-closed gaps;
- `metadata.json` и `index.md` — source snapshot, config, boundaries и итог.

Первый полный replay от 2026-08-08 опроверг общий spread-threshold: среди
`2 374` evaluated outcomes median net отрицателен на всех горизонтах
(`-0,3762/-0,3324/-0,2873/-0,3447%`), как и во всех entry policies и трёх
chronological segments. Нельзя подбирать «лучшую задержку» по этому результату;
следующий блок должен проверять селективные режимные признаки.

USD capacity остаётся unknown, потому что исторический `ca_instruments` пуст и
raw exchange sizes нельзя умножать на выдуманный contract multiplier. Slippage
пока фиксированный scenario, а не depth fill. Эти ограничения всегда держат
`paper_promotion_allowed=false` и `shadow_promotion_allowed=false`.

`metadata.final_result_allowed=true` возможно только для строгого полного
Event Lake. Funding-capture в v1 — диагностический `4/8/24h` proxy с cost sweep,
а не исполнимый арбитраж: bid/ask, slippage, capacity и lifecycle относятся к
следующему этапу Executable Spread Timing.

Локальный reader пишет в `data/research/strategy_lab_local_archive/`:

- `archive_index.json` — offsets, длины, SHA-256 и identity 1 707 записей;
- `file_inventory.csv` — роли и размер всех 42 файлов;
- `windows/*.json` — причинно обрезанные локальные окна;
- `coverage.csv`, `metadata.json`, `index.md` — фактическое покрытие.

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
