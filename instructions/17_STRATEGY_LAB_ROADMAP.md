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

Последнее обновление: `2026-08-12`.
Текущий этап: `Phase O0 bounded Observatory готов; ожидается operator verdict перед 1h preflight`.
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

## Прежний checkpoint Stage 3.1 (заменён решением 2026-08-10 ниже)

1. добавить Stage 3.1 selective policy research без изменения live: сравнить
   velocity/expansion-stop, mark/index gap, funding sign/cadence, OI и volume;
2. train thresholds только на прошлом и проверить три chronological OOS-блока
   плюс whole-symbol holdout, cost sensitivity и concentration;
3. отдельно спроектировать backward-compatible сбор contract multiplier и
   нескольких уровней depth, не переписывая существующие operational logs;
4. только при положительном execution-aware holdout решить вопрос unified
   paper state machine; текущий общий spread threshold остаётся отвергнутым.

### 2026-08-10 — переход к Candidate Observatory

- По решению пользователя прежний узкий Stage 3.1 не продолжается как немедленный
  перебор selective thresholds. Сначала создаётся новый prospective-массив для
  совместного анализа funding, directed basis, entry/exit и roll.
- Каноническое имя runtime/research-контура:
  `Strategy Lab Candidate Observatory`; подробный контракт и план находятся в
  `instructions/18_STRATEGY_LAB_CANDIDATE_OBSERVATORY.md`.
- Первый venue universe ограничен `Binance / Bybit / OKX / KuCoin / Gate`.
  Все пять хранят медленное состояние, но hot BBO обычно нужен только для текущих
  двух ног и лучшей альтернативной roll-ноги.
- Полный depth исключён из alpha selection и получает минимальный приоритет.
  Обязательны только fresh BBO/tradeability; maker-first, chunks и фактическое
  исполнение оцениваются позднее в live-shadow.
- Отбор не сводится к Coinglass: используются exact Coinglass pairs, внутренние
  funding/spread anomalies, OI-volume-price/premium triggers, позиции/manual pins
  и matched controls.
- Для каждой монеты строится пятибиржевой vector, до `20` directed pairs,
  pair-specific rolling median/residual и варианты HOLD/EXIT/ROLL. Это покрывает
  случай, когда привычный Binance/Bybit basis `-2%` временно становится `-4%`, а
  KuCoin или Gate одновременно предлагают лучшую альтернативную ногу.
- Read-only audit обнаружил несовместимый base/suffixed symbol contract для
  Coinglass против OKX/KuCoin/Gate и непригодность текущего REST fan-out для
  нужной cadence: нормализованный пятибиржевой проход `20` symbols превысил `90s`.
  Поэтому Phase O0 требует общего Instrument Registry и multiplexed feeds, а не
  расширения существующих per-symbol loops.
- Проектный первый cap до preflight: Baseline `60` symbols at `60s`, Watch `25`
  at `30s`, Hot `8` symbols / обычно `3` venues at `5s`; верхняя оценка около
  `1,206,720` narrow venue rows/day до дедупликации. Это не подтверждённая
  пропускная способность: обязательны bounded `1h`, затем `24h` preflight.
- Следующий safe change block: Phase O0 schema/tests, five-venue Instrument
  Registry и bounded multiplexed feed prototype. Никакого long run, shadow
  position, paper/live promotion, ARM или заявки это решение не разрешает.

### 2026-08-10 — аудит Coinglass и ArbitrageScanner для Candidate Observatory

- Текущий Coinglass web parser не применяет выбранные оператором exchange
  checkboxes: отдельная headless-сессия разбирает первые `20` строк общей таблицы.
  Свежий fetch занял около `71.8s`; только `5/20` exact pairs целиком относились к
  `Binance/Bybit/OKX/KuCoin/Gate`. Поэтому parser оставлен только как будущий slow
  fallback и не включался/не изменялся в runtime.
- Официальный Coinglass funding-arbitrage API поддерживает точный
  `exchange_list=Binance,Bybit,OKX,Gate.io,KuCoin` и отдаёт pair funding intervals,
  next settlement, OI, spread и fee с update frequency `20s`. Для endpoint нужен
  Startup+ plan и API key; на момент аудита ключа в проекте нет.
- Публичный ArbitrageScanner endpoint доступен и вернул `779` rows / `23` exchange
  identifiers; exact-five offline universe содержал около `500` symbols. Но старый
  adapter нельзя включать: `okx` не совпадает с `okex_futures`, а long/short funding
  legs назначаются в обратном направлении. Кроме того, источник не отдаёт price
  basis, OI, interval, fees или BBO.
- Принят mix: own five-venue discovery остаётся ground truth; Coinglass exact pair
  и ArbitrageScanner используются как candidate seeds/independent cross-checks с
  обязательной verification собственными feeds. Source disagreement сохраняется
  как feature. Ни один внешний источник не является торговым сигналом.
- Coinglass coin futures page остаётся ручным drill-down; её core funding/OI/price/
  volume поля собираются напрямую с бирж. Actual liquidations — optional enrichment.
  Liquidation heatmap является моделью, а не raw events; numeric API существует,
  но требует Professional+, поэтому v1 не зависит от heatmap.
- Phase O0 расширен versioned `source_observation` contract, replay fixtures,
  exact aliases/sign tests и research-only adapters. Источники остаются disabled
  до отдельного bounded preflight; long run, shadow positions, paper/live, ARM и
  торговые настройки этим checkpoint не разрешены.

### 2026-08-11 — точный candidate mix и собственный top-movers intake

- Baseline/Watch/Hot теперь имеют детерминированную формулу `60/25/8`: P0
  позиции/manual pins не вытесняются, Baseline сохраняет 10 matched controls,
  Watch — 5, а остальные места заполняются multi-family first и затем
  family-balanced round-robin. Один trigger family не может занять более 40%
  event slots при наличии альтернатив.
- Пять ranked pools: external exact-five, funding, directed basis, собственный
  price momentum и OI-volume-premium. До итоговой дедупликации они дают максимум
  `15/15/15/15/10`; совпадение источников повышает monitoring priority, но не
  считается доказательством alpha.
- Новый `price_momentum_rank` считается самостоятельно из 60-секундных bulk
  snapshots на `5m/15m/1h/4h/24h/72h/7d`: top-3 gainers и losers по каждому
  окну плюс absolute/z-score intake, acceleration, cross-venue dispersion и
  lead/lag. Native 24h бирж — cross-check, а не смешанная каноническая база.
- Read-only public probe вернул `730 Binance / 812 Bybit / 442 OKX / 677 KuCoin`
  raw ticker rows; Gate bulk REST не ответил даже за 50s. Это подтверждает
  доступность дешёвого собственного ranking на первых четырёх venues и
  необходимость Gate WebSocket/slow isolated fallback, а не последовательного
  REST hot path.
- Coinglass имеет официальный multi-window `coins-price-change` API и остаётся
  независимым plan-dependent validation tag. CoinMarketCap top gainers — spot и
  market-cap/volume filtered context, не источник отдельной perpetual-квоты.
- Это уточнение design-only: collector, long run, shadow/paper, ARM, заявки и
  live-модули не запускались и не менялись. Следующий safe block остаётся Phase O0.

### 2026-08-12 — отдельная Observatory page и исправленный external intake

- Устаревший main-dashboard intake выведен из runtime: startup больше не создаёт
  legacy market scheduler/bootstrap, settings update не может снова включить
  `coinglass/arbitragescanner`, а совместимый `/api/refresh` является no-op.
  Accounts, positions-market, protective, auto-exit, coin-analysis и Pump loops
  не менялись.
- С главной страницы удалены source/exchange-opportunity настройки, parser poll,
  Data Overview, общий symbol universe и Funding Opportunities. Создана отдельная
  `/strategy-lab-observatory` без торговых controls.
- Введён `strategy_lab_external_candidate_v1`: source asset ID, canonical symbol,
  exact exchange symbol каждой ноги, source timestamp, mapping status, raw
  identity и явный `research_only/trade_signal=false`.
- Новый ArbitrageScanner adapter использует exact aliases пяти бирж, включая
  `okex_futures -> okx`, переводит provider percentage points в decimal и задаёт
  экономическое направление `long=min funding`, `short=max funding`. Live-smoke
  менялся вместе с публичной таблицей: `772..789` raw rows, стабильно `497`
  eligible exact-five symbols.
- Новый Coinglass fallback физически открывает `Exchanges`, кликает только
  `Binance/Bybit/OKX/KuCoin/Gate`, повторно читает checkbox state и ждёт, пока
  React-таблица два чтения подряд содержит только exact-five пары. Пустой,
  частичный, stale или anti-bot результат не затирает last-good. Live-smoke:
  `20/20` valid exact-five rows примерно за `9.4s`.
- Bounded combined smoke в отдельном временном state дал `30` external candidates,
  из них `16` подтверждены обоими источниками. Это monitoring priority, не alpha
  и не разрешение сделки. Scheduler Observatory остаётся выключен; долгий сбор
  не запускался.
- Контрактные/web tests покрывают aliases/sign, exact-five, mapping preservation,
  collision quarantine, overlap ranking, last-good и отсутствие legacy-блоков
  на main page. Полная регрессия: `757 passed`, `8 subtests`, `15 warnings`.
- Следующий safe block: Instrument Registry пяти бирж и bounded multiplexed public
  feed prototype. Затем отдельное операторское решение перед `1h` preflight.

### 2026-08-12 — Instrument Registry v1

- Реализован общий пятибиржевой Registry точных USDT perpetual contracts:
  Binance/Bybit/OKX/KuCoin bulk, Gate candidate-scoped exact-contract с bounded
  concurrency. Gate bulk исключён из hot path после фактических `~15 KB / 1.17 MB`
  за `30s`.
- Registry, а не внешний screener, теперь является источником executable venue
  symbols. Общий `SYMBOLUSDT` от ArbitrageScanner для OKX/KuCoin/Gate сохраняется
  как format disagreement; реальное расхождение asset identity блокируется.
- Live-smoke на 10 seeds занял `4.8s`: Gate `9/10`, `9` candidates проверены на
  двух и более venues; REDSTONE ожидаемо veto. Полная регрессия:
  `762 passed`, `8 subtests`, `15 warnings`.
- Следующий safe block: bounded multiplexed public feed и own-feed QA; long run,
  shadow/paper/live и ARM по-прежнему не разрешены.

### 2026-08-12 — bounded multiplexed public feed v1

- Реализован `strategy_lab_bounded_public_feed_v1`: максимум `10` symbols и
  `30s`, один public WebSocket на каждую из пяти бирж, общий wall-clock deadline
  и обязательные `research_only=true / trade_signal=false / scheduler=false`.
- Наблюдение `strategy_lab_own_observation_v1` нормализует BBO, last/mark/index,
  funding/predicted funding/next settlement, OI и volume, сохраняя точный
  Registry symbol и фактические source channels. Поля не подменяются нулями:
  отсутствие данных остаётся измеримой missingness.
- Binance BBO идёт через multiplexed WS. Поскольку подтверждённая mark-price
  подписка в bounded live-smoke не дала сообщений, mark/index/funding/next time
  безопасно и дёшево инициализируются одним public bulk `premiumIndex` REST
  snapshot, без per-symbol fan-out.
- QA считает expected/observed pairs и coverage каждой биржи, missing pairs,
  freshness, field availability, invalid BBO, parse/subscription/REST errors и
  число соединений. Полезный поток, завершённый общим deadline, отличается от
  источника без единого update.
- Финальный `8s` live-smoke на `ONE/COTI/B3/KAITO/RVN` дал `16/20` pairs:
  Binance `4/4`, Bybit `4/4`, OKX `3/3`, KuCoin `3/4`, Gate `2/5`; все пять
  использовали ровно по одному WS, parse/subscription errors и invalid BBO — `0`.
  Короткое окно не является capacity proof: неактивные KuCoin/Gate contracts
  могут не прислать тик до deadline.
- Контрактная регрессия feed/Registry/external intake: `15 passed`; полный suite:
  `767 passed`, `8 subtests`, `15 warnings`. Постоянный collector, shadow/paper,
  торговый signal и заявки не включались.
- Следующий safe block: встроить Registry refresh, bounded probe и собственную QA
  в отдельную Observatory page/API с last-good state. После её regression нужен
  отдельный operator verdict перед `1h` preflight.

### 2026-08-12 — Observatory Registry/feed integration

- На отдельной `/strategy-lab-observatory` появились последовательные ручные
  действия: external refresh -> Instrument Registry refresh -> bounded own-feed
  probe. API ограничивает probe `1..30s` и `1..10` symbols; scheduler, shadow
  positions и execution routes отсутствуют.
- Runtime snapshot хранит компактный Registry только для текущего candidate union,
  source status, source-specific verification, eligible count, последний feed
  report и QA verdict. Ошибка не затирает last-good.
- Любой успешный новый external refresh помечает прежние Registry и feed stale;
  feed нельзя запустить, пока Registry не обновлён. Это запрещает смешивать новый
  candidate set со старой картой контрактов.
- UI показывает по каждой бирже registry mode/count/error, feed connections,
  updates, pair coverage, missing pairs/freshness и число фактически наблюдавшихся
  venues по каждому кандидату. Все строки остаются `NO (research)`.
- End-to-end live-smoke через `StrategyLabObservatory` во временном state занял
  `21.5s`: ArbitrageScanner дал итоговые `30` кандидатов, Registry подтвердил
  `29`; `8s` feed наблюдал `85%` pairs и все пять venues. Coverage:
  Binance/Bybit/OKX/KuCoin `100%`, Gate `40%`; missing Gate pairs сохранены,
  invalid BBO — `0`.
- Профильная регрессия: `23 passed`; полный suite: `771 passed`, `8 subtests`,
  `15 warnings`. Долгий collector не запускался.
- Phase O0 implementation gate завершён. Следующее действие требует человека:
  отдельное подтверждение на bounded `1h` preflight. Оно не разрешает `24h`,
  месячный prospective-сбор, strategy shadow или торговлю.

### 2026-08-12 — bounded 1h capacity preflight реализован

- Добавлены `strategy_lab_capacity_preflight_v1` и отдельный CLI
  `scripts/strategy_lab_preflight.py`. Контур жёстко помечен
  `research_only_no_trading`, не имеет execution API, не создаёт виртуальные
  позиции и ограничивает один запуск максимумом `3600s`.
- Перед сбором CLI заново получает внешний candidate union (до `60` монет),
  строит Instrument Registry только по пяти утверждённым биржам и не начинает
  feed, если Registry не `fresh`. Точный запуск дополнительно требует фразу
  `RUN STRATEGY LAB PREFLIGHT 1H`, минимум `1 GiB` свободного диска и
  эксклюзивный lock, поэтому два сбора не умножат соединения.
- Capacity-профиль: одно окно до `10` монет каждые `60s`, собственный public
  feed работает до `30s`, окна циклически ротируются. Для `60` eligible
  кандидатов каждая монета должна попасть примерно в одно окно за `6 минут`;
  в каждом цикле открывается максимум одно соединение на каждую из
  Binance/Bybit/OKX/KuCoin/Gate.
- Сохраняются сжатые normalized observations, компактные cycle summaries,
  атомарный runtime status и финальный QA. Отчёт измеряет фактическое покрытие
  пар и бирж, поля BBO/price/mark/index/funding/predicted/settlement/OI/volume,
  errors/reconnects, crossed BBO, cadence gaps, CPU/RSS, raw/compressed
  bytes/row и прогноз диска на час/сутки.
- QA fail-closed: нулевые данные, менее `90%` успешных циклов, subscription
  error, crossed BBO, отсутствие ротации всех eligible symbols или менее
  `80%` покрытия каждой из четырёх core venues дают `FAIL`. Gate ниже `50%`
  пока даёт отдельный warning, потому что короткие event-driven окна уже
  показали его структурно худшее покрытие; это решение должно быть пересмотрено
  по факту часа.
- Профильная интеграционная регрессия до длинного запуска: `30 passed`; полный
  suite: `783 passed`, `8 subtests`, `15 warnings`. Повторный `16s` CLI smoke
  ротировал все `10/10` symbols и завершил `2/2` cycles без feed errors;
  Binance/Bybit/OKX дали `100%`, KuCoin `50%`, Gate `60%`, поэтому строгий QA
  ожидаемо оставил короткий smoke в `FAIL`. RSS-метрика после исправления:
  `64.9 -> 71.4 MB`, peak `74.9 MB`; первичный disk forecast `1.27 MB/hour`.
  Разрешение оператора на bounded `1h` получено. Оно не разрешает shadow/paper,
  live, заявки, переводы или изменение работающих торговых модулей.

Следующий регрессионный порядок: полный suite и commit реализации -> фактический
`1h` -> разбор QA. После результата разрешено самостоятельно исправлять
research-only несостыковки и продолжать обоснованные этапы roadmap; любое
человеческое действие и любой trading/shadow-position boundary остаются
отдельным stop gate.

### 2026-08-12 — preflight eligibility alignment

- Первый часовой запуск был остановлен после первого цикла: bootstrap честно
  показал `47` source-aware Registry-eligible кандидатов, но feed runner
  подготовил `54`, повторно применив только упрощённое правило `>=2 venues`.
  Полученные `36` observations не считаются результатом preflight.
- Runner теперь сохраняет порядок candidate union, но допускает символ только
  при наличии `verification.eligible_for_observation=true`; число выбранных
  символов обязано точно совпасть с `registry.eligible_candidate_count`, иначе
  запуск завершается до feed. Это сохраняет veto по mismatch/quarantine, а не
  превращает наличие контрактов в достаточное условие.
- После исправления профильная регрессия: `31 passed`; полный suite:
  `784 passed`, `8 subtests`, `15 warnings`. Следующий шаг — новый чистый `1h`.

### 2026-08-12 — bounded 1h capacity result

- Чистый run `preflight-20260812T194321Z` завершил ровно `60/60` циклов за
  `3600s`: `1910` normalized observations, `47/47` verified symbols,
  aggregate pair coverage `91.563%`, failed cycles / parse / subscription /
  REST errors / invalid BBO — `0`. Gzip, report и status сверены построчно.
- Venue coverage: Binance `561/561`, Bybit `536/536`, OKX `192/192`, KuCoin
  `511/511` — по `100%`; Gate `110/286 = 38.462%`. Gate имел соединения и
  updates без errors, то есть проблема — отсутствие события по тихому контракту,
  а не capacity/reconnect. Verdict: `PASS_WITH_WARNINGS` только из-за Gate.
- Реальная нагрузка: CPU `37.625s` (`1.045%` одного ядра), RSS start/end/peak
  `65.0/109.7/112.7 MB`, max schedule delay `0.015s`; storage `209,906 bytes`
  за час, forecast около `5.04 MB/day`. Candidate union `60` и текущие `47`
  eligible symbols система тянет с большим запасом.
- Field audit запретил немедленный `24h`: funding имеется на всех наблюдаемых
  rows четырёх core venues, но OI и quote-volume в текущем feed стабильны только
  у Bybit. Binance/OKX/KuCoin дают эти признаки `0%` rows, Gate около `21.8%`
  своих observed rows. Такой суточный датасет был бы пригоден для spread/funding,
  но недостаточен для заявленного OI-volume-price hypothesis search.

Решение: capacity phase пройден, но data-contract phase ещё нет. Перед `24h`
добавить bounded public REST seed для недостающих OI/volume/last/mark полей и
тихих Gate pairs, сохранить websocket BBO основным источником, ввести field-level
QA и повторить короткую/часовую валидацию. Торговые режимы не включать.
