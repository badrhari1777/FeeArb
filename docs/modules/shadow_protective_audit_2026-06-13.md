# Аудит Shadow-защиты на 2026-06-13

## Область анализа

- Период: `2026-05-11 20:59 UTC` - `2026-06-13 20:30 UTC`.
- Источник: `logs/derisk_history.jsonl`, около `11.1 GB`.
- Прочитано `1,213,984` записей: `492,929` циклов, `720,805` событий,
  `247` outcome и `3` поврежденные строки.
- Дополнительно проверены текущий `/api/snapshot`, настройки, active hedge
  clusters, auto-exit rules и реализация защитных контуров.

## Общий вывод

Shadow полезен как детектор длительного стресса и потенциально односторонних
позиций, но пока не готов к Live. Главная проблема находится в ownership/cluster
model: de-risk автоматически превращает все сохраненные auto-exit правила в
активные hedge clusters, включая выключенные и устаревшие правила.

Из-за этого реальные наблюдения смешиваются с ложными orphan, conflict и
missing-состояниями. До Live нужен канонический реестр управляемых позиций.

## Emergency De-Risk

### Требования

- Находить биржу с недостатком свободного баланса или опасным margin buffer.
- Сокращать целую хеджированную пару минимально достаточным объемом.
- Ограничивать одно действие по notional.
- Учитывать стоимость закрытия, funding и срочность.
- Не исполнять действие при ненадежных данных биржи.

### Что видно по Shadow

- Production-настройка была `enabled=false`, `shadow=true` в `492,797` циклах.
- Найдено `22,861` production candidate rows:
  - `14,317` stress;
  - `8,544` panic;
  - `22,839` partial;
  - `22` full cleanup, но все относятся к тестовому DOGE fixture.
- Сигналы часто были устойчивыми:
  - ARIA: `8.12 ч`, максимум эпизода `2.55 ч`;
  - H: `6.81 ч`, максимум `1.80 ч`;
  - HOME: `5.47 ч`, максимум `2.55 ч`;
  - ESPORTS: `4.46 ч`;
  - ID: `3.76 ч`;
  - LAB: `3.43 ч`.
- Ограничение `500 USDT` применяется через масштабирование qty.
- Exchange-health gate действительно блокирует решения на ненадежных данных.

### Что не подтверждено

- Реальных Live trigger/outcome нет.
- Все `trigger`, `orphan_trigger`, `preempt_requested` и все `247 outcome`
  связаны с test execution IDs: `derisk-1`, `orphan-1`, `orphan-dup-1`, `exec-1`.
- Нельзя оценить фактические fills, slippage, остатки и восстановление buffer.
- `velocity_trigger_bps`, `recovery_buffer_pct`, `panic_severity` и
  `rehedge_allowed` сейчас не участвуют в выборе действия.

### Что корректировать

1. Не включать Live до исправления cluster ownership.
2. Добавить максимальный допустимый `candidate_score`. Сейчас любой конечный
   score может быть исполнен. В Shadow было `2,245` строк со score `>=1`;
   у SIGN медиана около `7.47`, у LAB около `1.2`.
3. Логировать `action_notional_usd` и `margin_relief_usd`, а не только coin qty.
4. Пересмотреть абсолютный floor `min_free_balance_abs=500`: он постоянно
   помечал небольшие аккаунты Bitget, BingX и Gate как stress.
5. Подключить de-risk к единому execution worker. Сейчас это отдельный scheduler
   с runtime `180 sec` для stress и `90 sec` для panic.
6. Перед sizing получать реальные min qty, min notional и amount step через
   preflight. Сейчас residual classifier получает только dust threshold `$10`.

### Готовность

`Не готов к Live`. Детектор стресса полезен, но selection и ownership требуют
исправления, а production outcome-данных нет.

## Orphan Cleanup

### Требования

- Обнаруживать пропавшую ногу или существенный qty mismatch.
- Не реагировать на stale/auth/network snapshots.
- Подтверждать orphan несколькими циклами.
- Закрывать только лишнюю ногу или разницу между ногами.
- Не оставлять неисполняемую пыль.
- Иметь приоритет над обычным входом/выходом.

### Что видно по Shadow

- `226,762` цикла содержали `confirmed_orphan`.
- `419,706` orphan candidate rows:
  - `419,684` single leg;
  - `22` qty mismatch, и это тестовые строки.
- Длительные состояния:
  - BTR: `114.0 ч`, максимум одного эпизода `15.1 ч`;
  - ESPORTS: `103.4 ч`;
  - DRIFT: `90.1 ч`;
  - AIO: `68.8 ч`;
  - STO: `68.1 ч`;
  - LAB: `39.8 ч`.
- Health gate не дает подтвердить orphan при ненадежной бирже.

### Главная проблема

Эти цифры нельзя считать числом реальных аварий. Контур не различает:

- управляемая парная позиция действительно потеряла ногу;
- оператор намеренно держит standalone position;
- старая auto-exit запись осталась после закрытия;
- выключенное auto-exit правило больше не владеет позицией.

`derive_cluster_rules()` добавляет каждое persisted auto-exit правило как
`enabled=true`, не проверяя `enabled`, `v1_enabled`, signature status и наличие
позиции. В текущем snapshot пять отсутствующих позиций все еще являются active
de-risk clusters.

### Что корректировать

1. Разрешать orphan cleanup только для кластера с явным ownership: active
   strategy ID или explicit cluster, bound к position signature.
2. Не выводить disabled/stale auto-exit rules в active clusters.
3. Хранить generation/signature, чтобы старый кластер не управлял новой позицией.
4. Увеличить подтверждение с `2 x 5 sec` хотя бы до `20-30 sec` для Live.
5. До cleanup проверять orphan notional, min qty, min notional и amount step.
   Сейчас остатки вроде `3 GUA`, `5 AIO`, `10 LAB` могут попасть в исполнение без
   проверки исполнимости.
6. Для multileg использовать отдельную модель, а не несколько pair clusters.

### Готовность

`Не готов к Live`. Без ownership возможна ликвидация намеренной standalone
позиции или позиции, ошибочно связанной со старым правилом.

## Cluster Conflict и Multileg Guard

### Требования

- Блокировать исполнение при неожиданных дополнительных ногах.
- Блокировать пересекающиеся кластеры.
- Агрегировать дублированные строки одной биржи/стороны.

### Что видно по Shadow

- `416,127` циклов содержали `blocked_by_cluster_conflict`.
- Наиболее длительные группы:
  - LAB overlapping cluster leg: `558 ч` суммарного повторения;
  - DRIFT extra visible legs: `265 ч`;
  - ESPORTS extra visible legs: `147 ч`;
  - HOME overlap: `69.6 ч`;
  - H overlap: `38.5 ч`.
- Guard работает безопасно: неоднозначные позиции не становятся кандидатами.
- Production-подтверждения duplicate aggregation почти нет; BTC rows были тестами.

### Что корректировать

- Убрать автоматически созданные stale clusters.
- Для multileg создать один кластер со списком ног.
- Показывать владельца каждой ноги: strategy ID, auto-exit rule или manual cluster.

### Готовность

`Guard готов как блокировка`, но cluster model создает слишком много конфликтов.

## Exchange Health

### Требования

- Не принимать исчезновение позиции за orphan при ошибках API.
- Блокировать auth failures, stale snapshots и повторные ошибки.

### Что видно по Shadow

- MEXC был untrusted почти весь период: `492,687` циклов.
- Bybit долго имел auth failures: `339,074` записей.
- Были stale/degraded периоды других бирж.
- Safety gate реально блокировал orphan/de-risk.

### Что корректировать

- MEXC `code=10072 Api key info invalid` классифицируется как `unknown_error`, а
  должен быть `auth_error`.
- Расширить exchange-specific error classifier.
- В outcome evaluator требовать здоровые свежие snapshots обеих бирж.

### Готовность

`В целом готов`, после расширения классификации ошибок.

## Auto Rebalance

### Требования текущего кода

- При падении qty одной ноги минимум на `20%` уменьшить противоположную сторону
  на тот же объем.
- Cooldown `120 sec`.
- Limit timeout `10 sec`, offset `2 bps`, max slippage `8 bps`.
- MEXC исключен.

### Что видно по логам

- `auto_rebalance_enabled=false`.
- При выключенном режиме код только обновляет предыдущий snapshot.
- Dedicated persistent Shadow decision log отсутствует.
- Нет данных для оценки false positives, fills или остатков.

### Что корректировать

- Реализовать настоящий Shadow: detected drop, cluster, выбранные ноги, planned
  qty, ограничения и причины block.
- Использовать exchange-health gate и position ownership.
- Не распределять сокращение по всем противоположным ногам символа без cluster ID.
- Подключить dust/min-order preflight и единый execution worker.

### Готовность

`Не оценен и не готов к Live`: Shadow telemetry фактически отсутствует.

## Auto Margin Reduce

### Требования

- Убирать избыточную isolated margin выше safe range.
- Trigger около `33%`, target около `30%`.
- Cooldown `300 sec`.
- KuCoin isolated работает в top-up-only режиме.

### Что видно по логам

- `auto_margin_reduce_enabled=false`.
- Runtime log содержит `blocked: auto_reduce_disabled`, но хранит только последние
  `80` событий в памяти.
- Persistent history нет.

### Что корректировать

- Добавить persistent Shadow recommendation log до включения Live.
- Логировать сумму reduce, buffer до/после, margin used, available, ограничения
  биржи и outcome через 1/5/15 минут.
- Не смешивать margin reduction с de-risk score.

### Готовность

`Не готов к Live`: есть диагностика, но нет истории и outcome.

## Auto Margin Add: обнаруженная Live-проблема

Этот блок не Shadow: `auto_margin_enabled=true`.

На `2026-06-13 20:36:53 UTC` Bybit успешно добавил `84.8154 USDT` маржи к
ESPORTS long, подняв расчетный buffer с `26.47%` к цели `30%`.

При этом в логах символ записан как `ESPORTSUSDTUSDT`, а KuCoin регулярно пытается
найти `ESPORTSUSDTUSDTM`. Это подтверждает дефект двойного settle suffix в
account/protective path. Bybit top-up прошел, но KuCoin symbol lookup загрязнен и
может ломать leverage/margin operations.

Приоритет: исправить normalization до включения новых защитных Live-блоков.

## Outcome Attribution и тестовое загрязнение

- Все `247 outcome` нельзя использовать как production evidence.
- Все live-like actions в history связаны с IDs из unit tests.
- Изоляция тестовых state/log paths уже исправлена, но старая история загрязнена.

Нужно исключить известные test IDs при импорте, связывать outcome с fills и
требовать здоровую reconciliation обеих бирж. Отсутствие позиции само по себе
нельзя считать `improved`.

## Телеметрия и объем логов

- `93.6%` циклов содержат хотя бы один `missing_all_legs`.
- `6,813,944` cluster rows имеют `no_visible_positions`.
- Полный cycle snapshot каждые `5 sec` создал более `11 GB` за месяц.

Что изменить:

1. Baseline cycle не чаще раза в `60 sec`.
2. Немедленно писать только transition, candidate, trigger, execution и outcome.
3. После TTL не повторять `missing_all_legs` до изменения состояния.
4. Ввести daily rotation/compression или SQLite index.
5. Архивировать текущий JSONL после исправления cluster model и начать чистую
   эпоху телеметрии.

## Рекомендуемый порядок работ

1. Исправить двойной `USDTUSDT` в account/protective path.
2. Исправить active cluster derivation и position ownership.
3. Уменьшить и ротировать de-risk history.
4. Добавить настоящий Shadow для rebalance и margin reduce.
5. Подключить market constraints/dust preflight.
6. Добавить score ceiling и action notional в de-risk.
7. Перевести защитные действия на единый execution worker с приоритетом:
   orphan cleanup, panic de-risk, stress de-risk, exit, enter.
8. Провести раздельные controlled small-volume Live прогоны de-risk и orphan.

## Статус исправлений на 2026-06-14

Пункты 1-7 реализованы:

- исправлена нормализация `USDTUSDT`;
- active clusters фильтруются по enabled/signature/current visible legs и
  содержат owner metadata;
- журнал ограничен rotation и state-change/heartbeat логированием;
- rebalance и margin reduce получили persistent Shadow history;
- Live-кандидаты проходят market constraints/dust preflight;
- введен `derisk_max_candidate_score=0.25`;
- защитные действия включены в единый Auto Agent worker.

Live по умолчанию не включен. Пункт 8 выполняется только раздельными
small-volume тестами по инструкции
`instructions/10_ЗАЩИТНЫЕ_СТРАТЕГИИ_SHADOW_И_SMALL_VOLUME.md`.
