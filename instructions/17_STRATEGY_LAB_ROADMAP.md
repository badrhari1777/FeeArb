# Strategy Lab: пошаговый roadmap и журнал для ИИ-агентов

## Статус документа

Это канонический живой план развития `Strategy Lab`. Каждый ИИ-агент, который
завершает содержательный блок Strategy Lab, обязан до коммита:

1. обновить текущий этап и чек-лист;
2. записать фактические данные, тесты и ограничения в журнал выполненных работ;
3. указать точный следующий безопасный шаг;
4. синхронизировать существенное решение с `AGENTS.md` и профильной инструкцией;
5. не объявлять исследовательский результат разрешением на live.

Если реализация и этот документ расходятся, сначала проверить код и свежие
артефакты, затем исправить roadmap в том же логическом коммите.

Последнее обновление: `2026-08-09`.
Текущий этап: `Executable Spread Timing v1 core реализован; простой spread-threshold опровергнут, Stage 3 selective filters pending`.
Текущий режим: `research_only_no_trading`.

## Цель

Построить воспроизводимую систему исследования, которая объединяет:

- многолетние Pump/Dump события;
- обычные межбиржевые арбитражные перекосы;
- публичные price/mark/index/premium/funding/OI/volume данные нескольких бирж;
- реальные execution/auto-exit/auto-arb логи;
- причинные прогнозы `ENTER / WAIT / HOLD / EXIT / VETO`;
- последовательное продвижение `research -> paper -> shadow -> отдельное live-решение`.

## Неподвижные границы безопасности

- Strategy Lab не включает ARM и не изменяет Pump Live, Grid, Auto Enter/Exit,
  de-risk, позиции, заявки, плечо, маржу или API-настройки.
- Для исторического обогащения используются только публичные API без ключей.
- Перед долгим сбором выполняется bounded preflight с оценкой запросов, размера,
  доступности бирж, пропусков и времени. Долгий запуск требует явного решения.
- Пропущенные funding/OI/mark/index не заменяются нулём.
- Ошибки mapping, contract size, stale quotes и конфликт mid/mark дают `VETO`.
- Порог расхождения до 2 процентных пунктов является только исследовательской
  погрешностью matching и не разрешает вход.
- Существующие live-аудит логи не переписываются и не меняют схему ради Lab.

## Зафиксированная исходная точка

Коммит `4c31e7d` создал Strategy Lab phase 1.

- зарегистрирован архив Pump: 42 файла, 3,397 GiB, шесть бирж, данные с 2024;
- нормализовано 1 540 строк Pump-событий;
- проанализировано 203 причинных арбитражных перекоса;
- 4h raw-spread reversion: 62,42%; net-positive после 0,18% cost: 32,73%;
- медианный 4h net capture: -0,209138%;
- отрицательный funding сохранил знак в 87,76% из 5 407 переходов;
- funding <= -1% сохранил знак в 97,89% из 285 переходов;
- публичный пилот: 8 событий, 6 совпадений <=0,75 п.п., ещё 2 <=2 п.п.;
- Pump/arb связей в локальном окне ±6h нет из-за малого пересечения периодов;
- полный regression после phase 1: `590 passed`, `11 warnings`.

Главный вывод: немедленный вход по большому mid spread опровергнут. Исследовать
нужно исполнимый net spread и funding, а старые Pump-события необходимо адресно
обогащать синхронной историей нескольких бирж.

## Архитектура данных

### Event identity

Каждый объект должен иметь устойчивые ключи:

- `run_id` — один запуск конкретной версии исследования;
- `event_id` — неизменный идентификатор исходного рыночного события;
- `event_version` — версия нормализации события;
- `hypothesis_id` и `model_version` — версия проверяемого правила;
- `symbol`, `exchange`, `market_id`, `contract_size`;
- `source_ts_ms`, `observed_at_ms`, `available_at_ms` для защиты от look-ahead.

Повторно увиденный signal не является новым событием без нового `event_id`.

### Event window

Целевое историческое окно Pump-события:

- `-24h .. +72h` на 5m;
- более узкое 1m окно вокруг ключевой фазы после оценки стоимости;
- OHLCV, mark, index, premium index, funding, OI;
- contract metadata и явные missing/error поля;
- order book только там, где исторический источник надёжен.

Приоритетные биржи: Binance, Bybit, KuCoin, OKX, MEXC, Bitget.

### Единый Strategy Lab ledger

Новый append-only JSONL ledger не заменяет существующие operational/live логи.
Минимальная запись:

- `schema`, `run_id`, `record_id`, `record_type`, `mode`;
- `event_id`, `hypothesis_id`, `symbol`, `exchange_pair`, timestamp;
- `data_quality`, `missing_fields`, `veto_reasons`;
- `features_ref`/`features_hash`, прогноз и confidence;
- `decision`: `VETO / WAIT / ENTER / HOLD / EXIT / RISK_EXIT`;
- execution assumptions: fee, slippage, funding, capacity;
- outcome status и позднее присоединённый причинный outcome;
- `code_commit`, config hash и source manifest hash.

Режимы должны быть разделены полем `mode` и отдельными артефактами:

- `research_replay` — исторический причинный replay;
- `paper` — виртуальный портфель;
- `shadow` — решения на свежем рынке без заявок;
- `live` в Strategy Lab запрещён до отдельного операторского решения.

## Пошаговый план

### Этап 1. Event Lake и research/shadow телеметрия — CORE COMPLETE

- [x] Инвентаризация Pump-архива, обычной базы и operational logs.
- [x] Причинная нормализация 203 арбитражных событий.
- [x] Единый каталог 1 540 Pump event rows.
- [x] Версионированный enrichment manifest с оценкой запросов и размера.
- [x] Resumable публичное обогащение с cache/checkpoint и явными ошибками.
- [x] Единая schema Strategy Lab ledger и валидатор записей.
- [x] Bounded pilot на небольшом числе событий/бирж.
- [x] Coverage report по бирже, полю и историческому периоду.

Критерий завершения: один deterministic pilot повторяется без дубликатов,
не делает приватных запросов, не трогает live state и даёт manifest, coverage,
ledger и hash-проверяемые артефакты.

Критерий core выполнен bounded-пилотом. До масштабирования остаётся Stage 1.1:

- [x] Инвентаризировать поля 3,397 GiB локального multi-exchange архива.
- [x] Построить SHA-проверяемый byte-offset local reader.
- [x] Объединить local-first reader с public cache без потери 5m точности.
- [x] Добавить source-specific mark/index/premium endpoints.
- [x] Зафиксировать доступную глубину historical OI и причины retention gaps.
- [x] Посчитать calls/disk/time для 1 540 событий и получить отдельное решение
  перед долгим запуском.

### Этап 2. Funding Forecast v1 — FULL EVALUATED, RESEARCH HOLD

- [x] Причинные targets: следующий знак, величина, ослабление и длительность.
- [x] Baseline `next_sign = current_sign`.
- [x] Features: premium, OI, price, volume, cross-exchange difference, timing.
- [x] Interval-aware funding: settlement/hour normalization, interval shifts,
  realized/future `4/8/24h` paths и cost sensitivity.
- [x] Chronological splits и целые unseen symbols.
- [x] Calibration, Brier/log loss, sign accuracy и экономический replay.
- [x] Три последовательных временных OOS-блока и concentration audit.
- [ ] Paper decisions: promotion запрещён до execution-aware проверки и
  снижения symbol concentration; shadow только после стабильного holdout.

### Этап 3. Executable Spread Timing v1 — CORE IMPLEMENTED, SELECTIVE FILTERS PENDING

- [x] Вход сейчас и после 5/15/30m подтверждения плюс causal expansion stop.
- [x] Targets 15m/1h/4h/8h, MAE/MFE и time-to-cost-breakeven.
- [x] Исторический top-of-book bid/ask, fees и фактические funding settlements.
- [ ] Historical depth/slippage и USD capacity: текущий fixed slippage scenario
  и raw top sizes не дают promotion без contract multiplier/depth.
- [ ] Mark/index confirmation и mapping veto.
- [ ] Walk-forward и symbol holdout.
- [ ] Paper state machine `VETO/WAIT/ENTER/HOLD/EXIT`.

### Этап 4. Cross-exchange lead/lag и Pump bridge — PENDING

- [ ] Найти ведущую биржу по return/volume/OI shock.
- [ ] Разделить propagation и convergence.
- [ ] Соединить ранний basis с Pump exhaustion.
- [ ] Matched no-pump controls того же символа/режима.
- [ ] Multi-venue Pump short и long absorption исследования.

### Этап 5. Execution lifecycle и логирование — PENDING

- [ ] Нормализовать read-only lifecycle из auto-exit/auto-arb логов.
- [ ] Стадии observe/wait/trigger/order/fill/partial/hedge/exit.
- [ ] Измерить latency decay, maker miss, taker cost, partial-fill risk.
- [ ] Решить, какие новые поля добавлять в operational logs без ломки схемы.
- [ ] Добавления делать versioned и backward-compatible.

### Этап 6. Unified paper/shadow — PENDING

- [ ] Общий decision contract и отдельные портфели гипотез.
- [ ] Дедупликация по `hypothesis_id + symbol + event_id`.
- [ ] Replay current code over immutable raw ledger.
- [ ] Shadow health: process, heartbeat, cadence, errors, freshness, Git hash.
- [ ] Никакого live promotion без отдельного risk/execution аудита.

## Регрессионная лестница

Каждый этап обязан проходить подходящие уровни:

1. unit tests для формул, timestamps, dedupe, missing data и veto;
2. fixture end-to-end с детерминированными артефактами;
3. bounded real-data pilot;
4. повторный replay с проверкой идентичности/hash;
5. профильный regression существующего Pump/arb контура;
6. полный `pytest` перед коммитом;
7. `git diff --check`, staged diff/secret audit и чистое дерево после коммита.

Нельзя подменять причинный тест выбором последующего максимума, смешивать train
и future outcome или продвигать правило по одному удачному символу.

## Критерии продвижения гипотез

Research -> paper:

- положительное ожидание после реальных расходов;
- минимум три последовательных временных блока;
- положительный unseen-symbol holdout;
- результат не определяется несколькими экстремальными событиями;
- выдерживает удвоенное slippage и пропуски данных.

Paper -> shadow:

- воспроизводимый replay;
- стабильные MAE/drawdown/capacity;
- fail-closed veto;
- отдельный health/heartbeat/error контур;
- отсутствие конфликтов владения с действующими стратегиями.

Shadow -> live:

- только отдельное решение пользователя;
- отдельный safety, execution, account ownership и capital audit;
- этот roadmap сам по себе никогда не является live-разрешением.

## Журнал выполненных работ

### 2026-08-07 — Phase 1 baseline

- Коммит: `4c31e7d Add research-only Strategy Lab`.
- Добавлены `analysis_features/strategy_lab.py`, CLI, пять тестов, технический
  отчёт и инструкция.
- Выполнен восьмисобытийный public API pilot.
- Проверки: `5 passed`; полный набор `590 passed`, `11 warnings`.
- Решение: продолжать с Event Lake/enrichment manifest; live не менять.

### 2026-08-07 — Event Lake bounded pilot

- Реализованы `analysis_features/strategy_lab_event_lake.py`, CLI и шесть новых
  unit/end-to-end тестов.
- Manifest: 3 Pump events × Binance/Bybit = 6 задач; окно `-24h..+72h`, 5m;
  preflight estimate `42` публичных запроса.
- Первый канонический запуск: `30` фактических запросов, `6/6 completed`, по
  `1 152` OHLCV строк и `100%` coverage на каждую задачу.
- Funding rows: COTI `12/12`, HFT `12/11`, SIREN `78/0` для Binance/Bybit.
- Historical OI: `0/6`; Binance отклоняет старый `startTime`, Bybit возвращает
  пустую историю. Generic mark/index/premium явно отмечены missing.
- Повторный запуск: `6/6 cache_reused`, `0` API calls, ledger сохранил ровно
  `6` уникальных записей.
- Решение по логам: существующие operational/live JSONL не менять; новый
  `strategy_lab_ledger_v1` хранит research/paper/shadow provenance отдельно.
- Проверки: профильные `11 passed`; полный regression `596 passed`,
  `11 warnings`; live/ARM не затронуты.
- Коммит этого блока: `Add resumable Strategy Lab Event Lake` (см. Git history).

### 2026-08-07 — Local archive index и pilot

- Проинвентаризированы все `42` файла архива объёмом `3,397 GiB`.
- Timeseries слой: шесть `symbol_samples.jsonl`, около `1,92 GiB`, один JSON
  object на symbol-exchange; comparison `outcomes.csv` (`1,72 GiB`) для чтения
  event windows не требуется.
- Построен byte-offset index `1 707` symbol-exchange записей с проверкой числа
  строк, SHA-256 исходных JSONL/summary и identity при каждом чтении; первый
  проход `6,0s`, повторный `0,35s`.
- Zero-network pilot COTI/HFT/SIREN: `18` задач, `11` доступных символ-биржа,
  `8` OHLCV окон, `6` funding окон, исторические OI/L-S `0`, mark/index/premium
  отсутствуют в archive schema.
- Проверки: Strategy Lab `15 passed`; полный regression `600 passed`,
  `11 warnings`; live/ARM не затронуты.
- Коммит этого блока: `Index local Pump archive for Strategy Lab` (см. history).

### 2026-08-07 — Deterministic local/public merge

- Объединены `6` общих COTI/HFT/SIREN Binance/Bybit окон без сети.
- Для OHLCV во всех случаях выбран public 5m (`1 152` строк), а local 1h
  сохранён в provenance; funding выбирается по лучшему покрытию.
- Per-dataset строки не смешиваются, identity conflicts дают hard failure.
- Проверки: Strategy Lab `18 passed`; полный regression `603 passed`,
  `11 warnings`; live/ARM не затронуты.
- Коммит: `Merge Strategy Lab local and public windows` (см. history).

### 2026-08-07 — Source-specific history и full-run preflight

- Подтверждены и включены public-only historical mark/index/premium klines
  Binance и Bybit; во всех `6` pilot-окнах получено по `1 152` строк каждого
  dataset.
- Binance OI официально ограничен последним месяцем. Старые COTI/HFT/SIREN
  окна теперь дают явный `retention_gap`, `calls=0`, а не ошибочный запрос.
- Bybit OI исправлен на обратную пагинацию с bounded `endTime`: во всех трёх
  окнах получено `1 152/1 152` строк за `6` страниц вместо ложного нуля.
- Чистый pilot: `3` события × `2` биржи, `6/6 completed`, `96` фактических
  public calls за `36,56s`, `5,10 MiB` cache. Повтор: `6/6 cache_reused`,
  `0` calls, ровно `6` ledger records.
- Zero-network merge повторён: Bybit OI и все mark/index/premium выбраны из
  public cache; Binance funding по-прежнему берётся из более полного local
  archive; Binance OI остаётся честным missing.
- Случайный concurrent pilot выявил дубль append-only ledger. Сгенерированный
  ledger восстановлен до уникальных записей; запись теперь защищена
  межпроцессным lock, повторной проверкой `record_id` и `fsync`.
- Полный каталог содержит `1 540` уникальных logical event IDs, но только
  `1 108` уникальных `symbol + timestamp` окон (`432` повторных logical окон).
- Без physical-window dedupe: `3 080` задач, примерно `49 280` calls,
  `2,56 GiB`, `5h13m` по скорости pilot.
- С exact-window dedupe: `2 216` физических задач, примерно `35 456` calls,
  `1,84 GiB`, `3h45m`.
- Generated evidence:
  `data/research/strategy_lab_event_lake_v3_clean/full_run_estimate.json` и
  `data/research/strategy_lab_merged_v3_clean/`.
- Проверки: source compilation; профильный Strategy Lab/Pump/coin regression
  `58 passed`, `4 warnings`; полный regression `608 passed`, `11 warnings`.
- Live, ARM, Pump Live, Grid, Auto Enter/Exit и позиции не затронуты.

### 2026-08-07 — Exact-window cache готов к полному сбору

- Event Lake v4 разделяет `3 080` logical tasks и `2 216` физических окон по
  `exchange + symbol + start/end + timeframe`; immutable window больше не
  содержит event-specific identity.
- Каждый из `1 540` event IDs сохраняет собственные coverage и append-only
  ledger records. Повторное logical событие ссылается на тот же `features_ref`
  и hash, не вызывая повторный API download.
- Zero-network full preflight подтвердил `1 540 / 3 080 / 2 216` и оценки
  `35 456` calls с dedupe против `49 280` без него.
- Real-data VELVET duplicate pilot: `2` logical events, `4` logical tasks,
  `2` physical windows, `32` public calls, `4` ledger records; повторный запуск
  дал `0` calls и не добавил ledger-дубли.
- Plan-only report получил отдельную регрессию после найденного до запуска
  отсутствия пустых mark/index/premium колонок.
- Проверки: Event Lake fixture `14 passed`; профильная Strategy Lab/Pump/coin
  регрессия `157 passed`, `4 warnings`; полный regression `612 passed`,
  `11 warnings`.
- Пользователь подтвердил полный public-only сбор; diff/secret audit пройден,
  исходники зафиксированы отдельным commit `5169080`.

### 2026-08-07 — Полный public-only сбор запущен

- Исходники exact-window runner зафиксированы commit `5169080` при чистом
  рабочем дереве после полного regression `612 passed`.
- Запуск начат `2026-08-07 13:03 MSK` в
  `data/research/strategy_lab_event_lake_v4_full/` командой:

  ```powershell
  .\.venv\Scripts\python.exe scripts\strategy_lab_event_lake.py --output-dir data\research\strategy_lab_event_lake_v4_full --all-events --max-events 999999 --execute-public
  ```

- Preflight запуска: `1 540` events, `3 080` logical tasks, `2 216` physical
  windows, оценка `35 456` public calls.
- Ранний checkpoint на `13:05 MSK`: `22` immutable windows и `30` logical
  ledger records, collector stderr пуст. Pump Live одновременно оставался
  `armed`, monitor cycle age `0.1s`, `last_error=null`; исследовательский поток
  не меняет и не переARM-ивает live-модуль.
- Сбор resumable: повтор той же команды валидирует и повторно использует
  готовые physical windows и не дублирует ledger records.
- Пока сбор работал, добавлен отдельный fail-closed validator manifest/cache/
  coverage/ledger provenance. На checkpoint `59` windows / `85` ledger records
  режим `--allow-in-progress` подтвердил структурную согласованность; fixture и
  Event Lake tests прошли (`17 passed`), профильная регрессия — `160 passed`,
  `4 warnings`, полный regression — `615 passed`, `11 warnings`.
- Финальный gate после завершения: `2 216` window-файлов, `3 080` уникальных
  ledger `record_id`, coverage для `3 080` logical tasks, отсутствие cache/JSON
  identity errors, затем полный zero-call replay с исходным
  `--code-commit 5169080...` и обновление этого журнала.
- CLI pin исходного collector commit покрыт регрессией: Event Lake/validator
  `18 passed`; полный regression `616 passed`, `11 warnings`.
- В `13:12 MSK` запущен один hidden post-run watcher для исходного collector
  PID `5148`. Его durable runtime status:
  `data/research/strategy_lab_event_lake_v4_full/postrun_status.json`.
  Начальное состояние `waiting / collector`, watcher stderr пуст. После выхода
  collector он последовательно выполняет strict validation, audited zero-call
  replay с commit из manifest и повторный strict validation; любая ошибка
  записывает `status=failed` и не запускает следующий исследовательский этап.
  Не запускать второй collector или watcher, пока этот процесс жив; после его
  завершения сначала прочитать `postrun_status.json` и validation/replay logs.

### 2026-08-07 — Funding Forecast v1 causal plumbing

- Добавлены `analysis_features/strategy_lab_funding_forecast.py`, CLI и fixture/
  model regression. Модуль строит один sample на physical event/exchange window,
  отделяет признаки `ts <= event_ts` от targets `ts > event_ts` и fail-closed
  отклоняет missing/stale current или missing/too-far next funding.
- Реализованы next-sign/weakening logistic, next-magnitude/duration ridge,
  baseline сохранения текущего знака/ставки, chronological validation/test и
  детерминированный holdout целых symbols, calibration, Brier/log loss,
  MAE/RMSE и отдельный неисполняемый 24h funding-capture proxy.
- Межбиржевой feature-only context отделён от label eligibility: окно Bybit без
  будущей funding-метки может причинно дать Binance sample известные premium/OI,
  но не попадает в train как размеченный пример. Не наблюдавшаяся смена знака
  помечается right-censored и исключается из обычной duration regression.
- Bounded moving pilot на первых `120` уже загруженных окнах подтвердил запись
  артефактов и cross-exchange premium для всех eligible samples. Последовательные
  snapshots меняли состав и метрики по мере прихода окон; один snapshot дал
  `57` eligible / `63` veto, `57` premium contexts и `56` other-exchange OI
  contexts. Поэтому `final_result_allowed=false`, а частичные accuracy/ROI не
  являются результатом стратегии и не дают paper/shadow допуска.
- Fixture regression: `8 passed`; профильный Strategy Lab/Pump/coin regression
  `147 passed`, `4 warnings`; полный regression `624 passed`, `11 warnings`.
- Live/ARM/Pump Live/Grid/orders/positions не изменялись; активный collector и
  его post-run watcher продолжают прежний public-only процесс.

### 2026-08-07 — Funding interval-aware hardening по HFT

- Read-only HFT audit показал последовательность причин пропуска: сначала
  недостаточный pullback и long ratio ниже `45%`, затем после PB25 одновременно
  funding `-2.062154%` за прошедшие 24h и long ratio `43.26%`. Pump Live не
  получал HFT как entry-ready и не пытался открыть позицию.
- Bybit history подтвердила смену HFT settlement cadence: после 8h-платежей
  прошли два последовательных hourly settlement по `-2%`. Это выявило, что
  одного raw funding per settlement недостаточно для сравнения режимов.
- Funding Forecast теперь причинно хранит median/latest interval, interval-change
  ratio, rate `bps/hour`, projected next-settlement delay, actual realized sums
  и counts за `1/4/8/24h`; targets включают next interval, next hourly rate и
  actual cumulative funding за `4/8/24h`.
- Economic proxy расширен до горизонтов `4/8/24h` и costs
  `0/4/8/12/16 bps`. Это всё ещё не execution replay и не paper approval.
- HFT-like fixture различает `-2% / 8h = -25 bps/h` и
  `-2% / 1h = -200 bps/h`, а также причинно обнаруживает переход с ratio
  `0.125`. Point-in-time metadata старого интервала не подменяется современной.
- Moving partial pilot (`120` окон) дал `53` eligible / `67` veto:
  `52` hourly-normalized rows, `47` interval-change features, `81` metric rows,
  все пять cost scenarios. `final_result_allowed=false`, promotion запрещён.
- Interval fixture regression: `9 passed`; профильный Strategy Lab/Pump/coin
  regression `148 passed`, `4 warnings`; полный regression `625 passed`,
  `11 warnings`.
- Pump Live/ARM/Grid/orders/positions и активный Event Lake collector не менялись.

### 2026-08-07 — Full Event Lake gate и Funding Forecast final evaluation

- Collector PID `5148` завершился штатно. Post-run watcher закончил цепочку со
  статусом `complete / validated_zero_call_replay`; все stderr-файлы пусты.
- Строгий Event Lake gate подтвердил `1 540` logical events, `3 080` tasks и
  ledger records, `2 216/2 216` immutable physical windows и полный coverage.
  Audited replay сохранил collector commit `5169080`, повторно использовал все
  `3 080` logical / `2 216` physical records и сделал `0` public API calls.
- Full Funding Forecast рассмотрел `2 216` окон: `1 024` eligible samples,
  `1 192` fail-closed veto, `283` symbols, Binance/Bybit. Основные veto:
  `current_funding_missing=1054`, `market_unavailable=133`.
- Добавлен третий более ранний chronological OOS-блок и воспроизводимые
  concentration metrics: top-1/top-5 absolute event contribution, top-symbol
  absolute contribution и средний gross после исключения пяти экстремумов.
- Next-sign logistic превзошёл current-sign persistence во всех временных
  блоках: `77,48% vs 70,20%`, `82,28% vs 77,22%`, `91,49% vs 88,65%`;
  unseen-symbol holdout: `78,16% vs 69,42%`.
- Диагностический funding-capture proxy после `8 bps` cost положителен на
  горизонтах `4/8/24h` во всех четырёх срезах. Это не execution PnL: bid/ask,
  slippage, capacity, basis и lifecycle в этот proxy не входят.
- Promotion в paper/shadow запрещён. Причины: ridge для next raw/hourly
  magnitude проигрывает простому current-rate baseline; в chronological
  validation один symbol даёт `51,51%` абсолютного результата, top-5 events —
  `34,45%`; только два из четырёх срезов имеют умеренную symbol concentration.
- Решение: сохранить sign forecast как кандидатный `CARRY` feature, не считать
  его готовой стратегией и перейти к Stage 3 execution-aware spread timing.
- Проверки: Funding Forecast fixture `9 passed`; профильный Strategy Lab/Pump/
  coin regression `169 passed`, `4 warnings`; полный regression `625 passed`,
  `11 warnings`. Live/ARM/Pump Live/Grid/orders/positions не изменялись.

### 2026-08-08 — Executable Spread Timing v1 core

- Добавлены research-only module/CLI
  `strategy_lab_executable_spread.py`. SQLite открывается read-only и один WAL
  snapshot фиксируется `BEGIN`; counts/min/max и `source_snapshot_id`
  сохраняются в metadata.
- Из обычной арбитражной базы используются причинные первые triggers и
  фактические historical top-of-book bid/ask. Проверяются `now`, задержки
  `5/15/30m` и causal `expansion_stop` после relief минимум `0,10 п.п.`;
  outcomes — `15m/1h/4h/8h` после фактического времени входа.
- Net outcome складывает directed bid/ask capture и каждое реально записанное
  funding settlement, затем вычитает `0,12%` fee и отдельный `0,06%` fixed
  slippage scenario. Неизвестный funding schedule не считается нулём.
- Mark conflict/divergence, stale/missing book, price mismatch >2 п.п.,
  исчезнувший spread и missing exit/funding дают hard `VETO`. Диапазон
  `0,75–2 п.п.` остаётся только soft research match.
- Канонический cutoff `source_max_ts_ms=1786221561852`, snapshot id
  `af603972...`. Snapshot: `333 416` feature rows total (`166 708` canonical
  direction rows), `208` causal events, `22` symbols, `4 160` policy/horizon
  outcomes; `2 374` evaluated и `1 786` veto. Период событий:
  `2026-06-24..2026-08-08`.
- Простое правило входа по большому spread опровергнуто во всех горизонтах и
  трёх chronological segments. Aggregated median net:
  `-0,3762% / -0,3324% / -0,2873% / -0,3447%` для `15m/1h/4h/8h`; positive
  rate `10,94% / 22,38% / 34,12% / 35,99%`. Все entry policies также имеют
  отрицательные median и mean net.
- USD capacity намеренно неизвестна для всех rows: `ca_instruments` пуст, а
  raw bid/ask sizes без исторического contract multiplier нельзя безопасно
  переводить в notional. Slippage пока сценарный, потому что depth snapshots
  не сохранялись. Это logging/data gap и жёсткий запрет paper/shadow promotion.
- Concentration metrics по event/symbol встроены в каждую policy/horizon/time
  summary. Результат сохраняется в ignored
  `data/research/strategy_lab_executable_spread_v1/`.
- Решение: не оптимизировать задержку общего threshold. Следующий тест должен
  отбирать режимы по velocity/expansion, mark/index, funding sign/cadence,
  OI/volume и symbol-independent validation, одновременно проектируя
  backward-compatible depth/contract telemetry.
- Fixture regression: `4 passed`; профильный Strategy Lab/Pump/coin regression
  `173 passed`, `4 warnings`; полный regression `629 passed`, `11 warnings`.
  Live/ARM/Pump Live/Grid/orders/positions не изменялись.

### 2026-08-09 — checkpoint перед защитным разбором TUT

- Разработка Strategy Lab поставлена на контролируемую паузу без изменения
  исследовательских данных и кода. Последний завершённый коммит:
  `7489ffa Add Strategy Lab executable spread timing`.
- Текущая воспроизводимая точка остаётся Stage 3 core: `208` causal events,
  `22` symbols, `4 160` outcomes, из них `2 374` evaluated и `1 786` veto;
  простой spread-threshold имеет отрицательную median net на `15m/1h/4h/8h`.
- Stage 2 Funding Forecast полностью оценён и остаётся `research hold`; его
  sign forecast разрешён только как будущий `CARRY` feature.
- Не выполнены: historical depth/contract multiplier и USD capacity,
  mark/index confirmation, Stage 3.1 selective filters, walk-forward и
  whole-symbol holdout, paper state machine.
- Причина паузы: отдельный live-safety incident TUT в Manual/Grid protective
  контуре. Он не меняет результат Strategy Lab и не даёт live-разрешения.
- Возобновление начинается ровно с Stage 3.1 по пунктам ниже, после отдельного
  завершения и коммита защитного блока.

## Точный следующий шаг

1. добавить Stage 3.1 selective policy research без изменения live: сравнить
   velocity/expansion-stop, mark/index gap, funding sign/cadence, OI и volume;
2. train thresholds только на прошлом и проверить три chronological OOS-блока
   плюс whole-symbol holdout, cost sensitivity и concentration;
3. отдельно спроектировать backward-compatible сбор contract multiplier и
   нескольких уровней depth, не переписывая существующие operational logs;
4. только при положительном execution-aware holdout решить вопрос unified
   paper state machine; текущий общий spread threshold остаётся отвергнутым.
