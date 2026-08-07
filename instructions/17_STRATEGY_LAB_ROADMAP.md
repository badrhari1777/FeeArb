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

Последнее обновление: `2026-08-07`.
Текущий этап: `Этап 1.1 — source-specific feature completion`.
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

- [ ] Инвентаризировать поля 3,397 GiB локального multi-exchange архива.
- [ ] Читать локальный архив первым, публичный API использовать для пробелов.
- [ ] Добавить source-specific mark/index/premium endpoints.
- [ ] Зафиксировать доступную глубину historical OI и причины retention gaps.
- [ ] Посчитать calls/disk/time для 1 540 событий и получить отдельное решение
  перед долгим запуском.

### Этап 2. Funding Forecast v1 — PENDING

- [ ] Причинные targets: следующий знак, величина, ослабление и длительность.
- [ ] Baseline `next_sign = current_sign`.
- [ ] Features: premium, OI, price, volume, cross-exchange difference, timing.
- [ ] Chronological splits и целые unseen symbols.
- [ ] Calibration, Brier/log loss, sign accuracy и экономический replay.
- [ ] Paper decisions; shadow только после стабильного holdout.

### Этап 3. Executable Spread Timing v1 — PENDING

- [ ] Вход сейчас и после 5/15/30m подтверждения.
- [ ] Targets 15m/1h/4h/8h, MAE/MFE и time-to-convergence.
- [ ] Bid/ask, fee, funding, slippage, liquidity/capacity.
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

## Точный следующий шаг

Реализовать Этап 1.1 без долгого сетевого запуска:

1. описать schema 42 файлов локального multi-exchange Pump-архива;
2. построить local-first reader по `event_id/symbol/window/exchange`;
3. измерить покрытие OHLCV/funding/OI/premium/mark/index в архиве;
4. добавить source-specific API только для реально отсутствующих полей;
5. повторить bounded pilot и проверить cache/ledger identity;
6. подготовить оценку полного запуска: tasks, calls, disk, runtime, gaps;
7. запросить отдельное подтверждение перед сбором всех 1 540 событий.
