# Strategy Lab Candidate Observatory

## Название и назначение

Каноническое название нового исследовательского контура:
`Strategy Lab Candidate Observatory` (`SLC Observatory`). В разговоре пользователя
`модуль исследования`, `поиск стратегий`, `анализ funding/spread`, `наблюдение
кандидатов` и `Strategy Lab Observatory` означают именно этот контур.

Observatory должен заранее и без торговли собирать prospective-данные, из которых
после заморозки выборки Strategy Lab сможет искать безопасные и прибыльные правила:

- появления, усиления, охлаждения и смены cadence funding;
- расширения, остановки, схождения и повторного расширения межбиржевого базиса;
- выбора пары бирж, удержания, выхода, частичного выхода и roll одной ноги;
- связи funding/spread с OI, volume, price, mark/index/premium и режимом рынка;
- последующего сравнения лучших правил в live-shadow без заявок.

Observatory не является торговым модулем и не получает права ARM, открытия,
закрытия, roll, перевода средств или изменения маржи.

## Утверждённые решения

1. Первый контур ограничен пятью биржами:
   `Binance`, `Bybit`, `OKX`, `KuCoin`, `Gate`.
2. Пять бирж — это доступный universe, а не требование опрашивать каждую биржу с
   одинаковой частотой для каждой монеты.
3. Единица наблюдения — монета с вектором состояний доступных бирж, а не заранее
   фиксированная пара. Из пяти состояний строятся до `10` ненаправленных или `20`
   направленных пар без дополнительных API-запросов.
4. Точная пара и направление внешнего кандидата, например Coinglass, сохраняются,
   но одновременно оцениваются все доступные альтернативные ноги для median и roll.
5. Глубина стакана намеренно имеет минимальный исследовательский вес:
   - она не участвует в отборе кандидатов и не является alpha-признаком;
   - полный исторический depth не является обязательным условием начала shadow;
   - сохраняются best bid/ask, freshness и, если поле приходит бесплатно, top size;
   - фактическая исполнимость проверяется maker-first/chunk поведением в shadow.
6. Отсутствующий или stale BBO, неверный symbol mapping, закрытый контракт и
   невозможность безопасно построить две ноги остаются техническими `VETO`. Это не
   depth-фильтр, а проверка существования цены и инструмента.
7. Текущий узкий Stage 3.1 не продолжается без изменений. Его causal validation,
   spread features и outcome replay переиспользуются после prospective-сбора в
   совместном `carry + basis + lifecycle` анализе.
8. Первый месяц — checkpoint качества и количества событий, а не автоматическое
   окончание сбора. Достаточность определяется независимыми событиями и режимами.

## Главная исследовательская цель

Для каждого момента и каждой допустимой направленной пары ответить:

```text
если открыть long на L и short на S сейчас,
каковы распределение net PnL, MAE, время до результата и риск
на 15m / 1h / 4h / 8h / 24h / ближайших funding settlements;
лучше ENTER, WAIT, HOLD, EXIT, PARTIAL_EXIT, ROLL или VETO?
```

Оптимизация идёт не по максимальному win rate. Цель — максимальная консервативная
net-доходность при ограниченных tail loss, MAE, drawdown, времени удержания и
потребности в марже.

## Почему нужен новый prospective-массив

Старые Pump/Dump, Coin Analysis, funding history и operational logs сохраняются как:

- development set для схемы, признаков и воспроизводимости;
- источник редких исторических режимов;
- проверка импортёров, labels и расчёта результатов.

Они не являются достаточным финальным доказательством, потому что имеют разный
sampling, selection bias, неполное межбиржевое покрытие и повторяющиеся решения.
Новый массив должен собираться до знания будущего результата по заранее
зафиксированному контракту.

## Текущее состояние проекта и найденные ограничения

На read-only аудите `2026-08-10`:

- Coinglass отдавал `20` строк, но pipeline превращал их в base-symbol universe и
  повторно выбирал пару среди включённых бирж; ни одна из `15` внутренних
  opportunities не сохранила точную исходную пару Coinglass;
- `OKX`, `KuCoin` и `Gate` adapters требуют suffixed symbol (`BOMEUSDT`), тогда как
  Coinglass universe содержит base symbol (`BOME`), поэтому текущий общий scanner
  может вернуть по ним ноль до запроса сети;
- Coin Analysis жёстко ограничен `Binance + KuCoin`, а его автоматический candidate
  shortlist фактически не соединён с текущим main scanner;
- существующий `MarketDataBus` умеет все пять выбранных бирж, но создаёт отдельный
  WebSocket loop на каждую пару `exchange + symbol`; `20 x 5` означали бы около
  `100` соединений вместо пяти мультиплексированных feeds;
- текущий REST-путь с `20` нормализованными символами на пяти биржах не завершился
  за `90s`; одиночный BTC в разовом локальном замере занял примерно
  `3.17s Binance / 1.43s Bybit / 2.26s OKX / 2.54s KuCoin / 36.54s Gate`.

Это не оценка предельной мощности бирж. Это доказательство, что Observatory нельзя
строить поверх текущего синхронного REST fan-out и per-symbol WebSocket loops.

## Аудит внешних источников кандидатов 2026-08-10

Аудит выполнен read-only: источники не включались в runtime, настройки торговли,
ARM, заявки и долгий сбор не изменялись. Цель проверки — определить роль каждого
источника до реализации Phase O0.

### Coinglass `FrArbitrage`

Что делает текущий код:

- `parsers/coinglass.py` открывает веб-страницу через headless Chromium, ждёт
  render, прокручивает её и разбирает первые `20` DOM-строк;
- фильтр `Exchanges` не нажимается и список бирж в запрос не передаётся;
- cookies/local storage обычного браузера оператора не разделяются с headless
  сессией, поэтому выбранные человеком галочки на parser не влияют;
- после парсинга pipeline сохраняет только base-symbol в общем universe и теряет
  exact-pair provenance при внутреннем пересчёте opportunity;
- свежий cache считается пригодным `20m`, хотя раннее событие funding/spread за
  это время может существенно измениться.

Свежий технический probe показал:

- успешный headless fetch занял около `71.8s` и вернул `20` строк;
- только `5/20` exact pairs целиком состояли из выбранной пятёрки бирж;
- первый запуск без принудительного UTF-8 завершился на Windows
  `UnicodeEncodeError` из-за встретившегося Unicode-symbol, повтор с UTF-8 прошёл;
- веб-приложение использует внутренний encrypted endpoint
  `interestArbitrageV2?ex=...`; привязываться к его частным headers и шифрованию
  в production нельзя.

Следовательно, текущий parser годится только как временный низкочастотный fallback
и независимый sanity-check. Он не годится как основной prospective intake.

Предпочтительный путь — официальный Coinglass API v4:

```text
GET /api/futures/funding-rate/arbitrage
usd=<research notional>
exchange_list=Binance,Bybit,OKX,Gate.io,KuCoin
```

Он поддерживает точный список пяти бирж и возвращает exact buy/sell pair, funding
обеих ног, interval, next settlement, OI обеих ног, spread, fee и APR с заявленной
частотой обновления `20s`. Ограничение: endpoint недоступен на Hobbyist, нужен
Startup или выше и `CG-API-KEY`; на дату аудита ключ/переменная в проекте отсутствуют.

Fallback без платного API допустим в Phase O0 только так:

1. периодически рендерить таблицу вне hot path;
2. получать полный доступный набор, затем применять exact alias map пяти бирж до
   лимита кандидатов, а не фильтровать уже обрезанные первые `20`;
3. сохранять raw row, source timestamp, requested exchange set и parser version;
4. не доверять направлению/ставкам до подтверждения собственными exchange feeds;
5. при DOM/schema/Unicode ошибке fail-open для остальных candidate baskets, но
   помечать Coinglass source stale/unavailable.

Автоматически кликать пять UI-checkbox технически возможно, но это хуже официального
API и хуже post-filter полного набора: selectors, локализация, internal endpoint и
anti-bot поведение не являются стабильным контрактом.

### ArbitrageScanner funding table

Публичный endpoint, который уже использует проект, на момент probe доступен без
ключа: `779` rows и `23` exchange identifiers. В row присутствуют:

```text
symbol / ticker / maxSpread
rates[].exchange
rates[].rate
rates[].nextFundingTime
```

То есть он полезен как широкий бесплатный funding-discovery source, но не содержит
достаточного контракта для решения о сделке: нет price basis, OI, funding interval,
fees, BBO freshness и executable entry/exit spread.

Старую интеграцию нельзя включать без исправления:

- текущий substring include с именем `okx` не совпадает с фактическим
  `okex_futures`, поэтому из выбранной пятёрки реально проходили только четыре;
- `build_top` назначает max funding ногой `long`, а min funding ногой `short`.
  Для обычной perpetual funding convention экономическое направление обратное:
  long должен быть на минимальной ставке, short — на максимальной;
- aliases должны быть exact, versioned и fail-closed, а не substring matching;
- funding следует хранить как raw rate плюс `nextFundingTime`; interval нельзя
  выдумывать из одного снимка.

После точного offline-фильтра
`binance_futures/bybit_futures/okex_futures/kucoin_futures/gate_futures` в свежем
ответе было около `500` symbols с минимум двумя ногами. Это подтверждает хорошее
coverage, но не качество готовых сделок.

Роль источника: `candidate seed + independent cross-check`, не ground truth и не
готовый long/short сигнал. Его собственная таблица позволяет человеку выбирать
биржи; в нашем коде точный выбор пяти бирж можно сделать надёжнее локальным alias
filter без автоматизации UI.

### Coinglass страницы конкретной монеты

`/currencies/<coin>/futures` полезна как ручной drill-down: на одной странице видны
price/index, funding, OI, volume, long/short, 24h liquidations и список бирж. Для
машинного prospective-контура основные поля целесообразнее брать напрямую с пяти
бирж с единым timestamp и quality flags. Официальные Coinglass `pairs-markets`,
funding exchange list и OI exchange list можно позднее использовать для
independent validation/backfill, но веб-страницу не нужно парсить в Baseline.

`/liquidations/<coin>` содержит aggregated long/short liquidations, recent orders
и исторические окна. Это потенциально полезный optional event feature для
washout/cascade гипотез. Реальные liquidation streams отдельных бирж и официальный
Coinglass liquidation API следует сравнить на coverage; отсутствие данных не
блокирует funding/spread candidate.

`LiquidationHeatMap` — не эквивалент потоку фактических ликвидаций. Это модельная
оценка зон потенциальной ликвидации. Прямые публичные exchange API не дают полной
раскладки чужих entry/leverage, поэтому идентичную карту самостоятельно получить
нельзя. Скриншот/canvas scraping не даёт воспроизводимого numeric contract.
Официальный API возвращает numeric axes, candle series и liquidation intensity,
но heatmap model2 доступен только на Professional/Enterprise. Поэтому heatmap:

- не участвует в Candidate Observatory v1 и не является gate;
- остаётся ручным контекстом для редких Hot событий;
- может стать отдельным versioned enrichment experiment в O2, если появится
  подходящий API plan и будет доказан incremental lift на locked holdout.

### Решение по source mix

Ни Coinglass, ни ArbitrageScanner не должны единолично определять universe. Первый
дороже/медленнее без API, второй шире и бесплатнее, но беднее по полям. Целевой mix:

1. `P0`: held positions и manual pins;
2. `P1`: собственный пятибиржевой discovery по funding/basis/OI/volume/premium;
3. `P1 seed`: exact Coinglass pair, если есть официальный API; иначе slow web
   fallback с обязательной own-feed verification;
4. `P2 seed`: ArbitrageScanner exact-five funding anomalies после исправления
   aliases/sign convention;
5. `P2`: собственные OI-volume-price/premium triggers;
6. `P3`: matched controls.

Совпадение двух независимых источников повышает monitoring priority, но не является
доказательством alpha. Расхождение external source и own feed сохраняется как
`source_disagreement` и может само стать исследовательским признаком.

Phase O0 начинается не с подписки на платный источник, а с versioned source
contract и replay fixtures. Это позволяет одинаково подключить официальный
Coinglass API, web fallback или полностью собственный discovery без изменения
downstream candidate/event schema.

## Целевая транспортная архитектура

### Instrument Registry

Раз в `15m` строится каноническая карта USDT perpetuals:

```text
canonical_symbol
exchange
exchange_symbol
available / prelaunch / reduce_only / delisting
contract_multiplier
funding_interval
funding_cap / funding_floor
first_seen / last_seen
mapping_source
```

Registry является единственным местом преобразования `BOME -> BOMEUSDT /
BOME-USDT-SWAP / BOMEUSDTM / BOME_USDT`.

### Multiplexed feeds

Цель — один устойчивый public feed на биржу и не более одного резервного:

- Binance: all-market mark/funding и BBO либо individual BBO только для hot;
- Bybit: ticker topics; один ticker уже содержит BBO, mark, index, OI, volume,
  funding, interval и next funding time;
- OKX: public ticker/funding/open-interest/mark/index channels;
- KuCoin: bulk contracts snapshot для funding/mark/index/OI/volume и общий
  WebSocket с BBO topics только для watch/hot;
- Gate: futures tickers/contract stats и BBO topics; не использовать тяжёлый
  текущий bulk REST цикл как hot path.

Reconnect, heartbeat, staleness и subscription audit сохраняются отдельно по
бирже. Feed failure одной биржи не должен останавливать остальные, но пары с этой
биржей получают `data_veto`.

## Реалистичная пропускная способность первой версии

### Уровни

| Уровень | Максимум | Биржи | Persist cadence | Назначение |
|---|---:|---:|---:|---|
| Registry/discovery | все общие контракты | до 5 | агрегат `60s`, registry `15m` | заметить funding/spread/OI/volume аномалию |
| Baseline | `60` монет | до 5 | `60s` | до-событийная история и controls |
| Watch | `25` монет | до 5 | `30s` | полный multi-venue вектор и roll alternatives |
| Hot | `8` монет, включая позиции | обычно 3 | `5s` BBO/state | текущие ноги плюс лучшая roll-нога |

Если открытые позиции занимают hot-cap, новые кандидаты не вытесняют их: уменьшается
число новых hot-кандидатов. Funding/mark/OI состояния остальных доступных бирж всё
равно остаются на Watch cadence.

### Расчёт верхней записи

Без дедупликации пересечений:

```text
baseline: 60 * 5 * 1,440       = 432,000 venue rows/day
watch:    25 * 5 * 2,880       = 360,000 venue rows/day
hot:       8 * 3 * 17,280      = 414,720 venue rows/day
total upper bound              = 1,206,720 venue rows/day
30-day upper bound             = 36,201,600 venue rows
```

Pair rows постоянно не дублируются: они вычисляются из venue rows. Для `60` монет
полная матрица — всего `1,200` направленных расчётов на evaluation tick. Это
незначительная CPU-нагрузка по сравнению с сетью и сериализацией.

Хранение:

- immutable узкие numeric partitions (предпочтительно Parquet) по date/exchange;
- SQLite только для registry, candidate/event/decision/shadow lifecycle и индексов;
- raw WebSocket JSON сохраняется лишь вокруг ошибок и ограниченных event windows;
- ориентир первой версии — не более `10 GB/month`, но окончательный лимит задаётся
  только после `24h` preflight по фактическому compressed bytes/row.

Следовательно, пять бирж достижимы при multiplexing и tiering. Пять бирж через
текущий REST fan-out — недостижимы с нужной cadence.

## Отбор кандидатов: основной принцип

Во время сбора нельзя использовать один «торговый score» и брать только красивые
случаи: это создаст selection bias. Используется несколько независимых корзин с
квотами. Один symbol/event может иметь несколько `source_tags`.

```text
coinglass_exact_pair
funding_level
funding_acceleration
funding_dispersion
funding_cadence_change
spread_level
spread_residual
spread_velocity
price_momentum_rank
price_acceleration
cross_venue_price_lead
premium_dislocation
oi_volume_price_anomaly
held_position
manual_pin
matched_control
```

Monitoring priority распределяет ограниченные Watch/Hot места, но не утверждает,
что сделка прибыльна.

### Начальное распределение Baseline/Watch

Корзины имеют мягкие квоты и дедуплицируются по symbol. Это не означает, что
каждая корзина обязана ежедневно быть заполнена:

| Источник | Целевой максимум в Baseline | Обычный Watch приоритет |
|---|---:|---|
| Открытые позиции и manual pins | без квоты, всегда первые | `P0` |
| External exact-five seeds: Coinglass/ArbitrageScanner | до `15` вместе | `P1`, если подтверждён своим feed |
| Funding level/acceleration/dispersion | до `15` | `P1/P2` |
| Spread level/residual/velocity | до `15` | `P1/P2` |
| Price momentum/rank/acceleration | до `15` | `P1/P2` |
| OI-volume-price/premium anomalies | до `10` | `P1/P2` |
| Matched controls | минимум `10`, не менее `15–20%` | `P3` |

После объединения итоговый Baseline ограничен `60`, Watch — `25`. Монета с тремя
trigger families занимает одно место, но получает повышенный приоритет и все
source tags. Если P0 занимает большую часть Watch, квоты внешних источников
сжимаются, controls сохраняются минимум в разумном объёме для честного сравнения.

Начальные research-intake triggers должны быть намеренно широкими и адаптивными,
например top/bottom `5%`, robust `|z| >= 2.5` либо два согласованных умеренных
сигнала. Абсолютный safety-net residual может собирать события от `0.5 п.п.`, но
это не entry threshold. После `7d` QA пороги можно менять только для достижения
coverage/квот, не на основании PnL; version и причина изменения обязательны.

### Точный алгоритм формирования 60 / 25 / 8

Чтобы слово «квота» не оставалось неоднозначным, первая версия использует
детерминированный порядок.

`Discovery` раз в `60s` получает узкие bulk-снимки всех торгуемых USDT perpetuals
из Instrument Registry. Это ещё не список из 60 монет и не per-symbol REST loop.
Из него независимо строятся пять ranked pools:

1. external exact-five seeds: до `15` уникальных symbols после объединения
   Coinglass и ArbitrageScanner;
2. funding: до `15`;
3. directed spread/basis: до `15`;
4. price momentum/rank: до `15`;
5. OI-volume-premium: до `10`.

Одна монета в трёх pools занимает одно место и сохраняет три source tags.

Итоговый `Baseline=60`:

```text
K = число held positions + manual pins
controls = 10
event_slots = max(0, 50 - K)
baseline = K P0 + event_slots events + 10 controls
```

Event slots сначала получают все подтверждённые own-feed кандидаты с двумя и
более независимыми trigger families, затем заполняются round-robin из пяти pools.
Пока остальные pools имеют пригодные строки, один family не может занимать более
`40%` event slots. Внутри family сортировка идёт по percentile/robust-z severity,
source freshness и числу подтверждающих venues, а не по историческому PnL. Если
`K > 50`, P0 не удаляется: Baseline временно расширяется до `K + 10 controls`.

Итоговый `Watch=25`:

```text
K = все P0 symbols
controls = 5
watch_events = max(0, 20 - K)
watch = K P0 + watch_events + 5 matched controls
```

Watch events сначала включают multi-family `P1`, затем заполняются тем же
family-balanced round-robin. Исчезнувший trigger остаётся минимум `2h` и полный
relevant funding tail; поэтому место освобождается по lifecycle, а не мгновенно.
Если P0 занял больше 20 мест, P0 сохраняется, controls уменьшаются первыми.

Итоговый `Hot=8`: все symbols реальных открытых ног занимают места первыми;
оставшиеся места получают только own-feed-confirmed P1 или экстремальный P2.
Matched controls в Hot не входят. Для Hot подписываются текущие две ноги и лучшая
roll-нога, а не все пять бирж с полным стаканом. Если открытых symbols больше
восьми, cap временно расширяется: существующая позиция не теряет risk monitoring.

Candidate identity:

```text
symbol + directed source pair + trigger family + clustered event start
```

Срабатывания одного family в пределах `15m` объединяются в событие. Новый family
добавляет tag, а не создаёт дубликат. Исчезнувший сигнал остаётся в Watch минимум
`2h` и не менее одного полного relevant settlement tail; Coinglass-кандидат после
исчезновения из рейтинга сохраняется минимум до двух ближайших settlement checks.

### Приоритеты

1. `P0`: открытые позиции и manual pins — всегда Watch, текущие ноги — Hot.
2. `P1`: одновременно сработали два и более независимых семейства либо источник
   Coinglass подтверждён собственными данными.
3. `P2`: сильный одиночный funding/spread/OI-volume trigger.
4. `P3`: matched controls и обычные состояния.

Используется hysteresis: кандидат не удаляется сразу после ухода ниже порога.
Нужны pre-event buffer, minimum watch time и post-event cooling tail.

## Корзины кандидатов

### A. Coinglass exact candidates

Сохранять без перетолкования:

- точные long/short exchange;
- funding каждой ноги и interval;
- spread, APR, OI, settlement и source timestamp;
- source rank и requested notional;
- поддерживается ли каждая нога выбранной пятёркой.

После этого строить свой пятибиржевой вектор. Coinglass pair остаётся источником
события, но не запрещает найти лучшую пару или roll.

### B. Funding candidates

Отбирать по нескольким независимым признакам:

- max-minus-min funding cashflow между биржами;
- deviation каждой биржи от cross-venue median;
- raw rate, bps/hour и точный cashflow ближайшего settlement отдельно;
- изменение за `5m/15m/1h/4h` и ускорение;
- приближение к cap/floor;
- изменение cadence `8h -> 4h -> 1h` и обратно;
- число последовательных settlements одного знака;
- расхождение current/predicted/последней realised ставки, где поля доступны;
- время до каждого settlement, потому что часы на биржах могут не совпадать.

### C. Spread/basis candidates

Для каждой направленной пары хранить:

```text
entry_basis(L,S) = bid_S / ask_L - 1
exit_basis(L,S)  = bid_L / ask_S - 1
pair_mid_basis
mark_basis
index/premium-normalized basis
```

Главный кандидатный признак — не только абсолютный spread, а отклонение от
собственной устойчивой нормы пары:

- rolling median `1h/4h/24h/7d`;
- robust MAD/z-score и percentile;
- residual `current - rolling median`;
- velocity/acceleration `1m/5m/15m`;
- persistence и число пересечений equilibrium;
- cross-venue median при наличии минимум трёх бирж.

Пример пользователя: если Binance/Bybit обычно держатся около `-2%`, а сейчас
стало `-4%`, событие имеет residual около `-2 п.п.` независимо от того, велик ли
абсолютный spread по общему порогу. При двух доступных биржах используется только
историческая норма конкретной пары; «глобальная медиана» не выдумывается.

### D. Price momentum / top movers

Готовый рейтинг сайта не нужен как основной источник. Раз в `60s` сохраняется
цена каждого доступного venue из bulk ticker/mark feed. Для каждого symbol
считаются per-venue returns, затем robust median по свежим venues и отдельно
межбиржевая dispersion/leader:

```text
5m / 15m / 1h / 4h / 24h / 72h / 7d return
current 15m - previous 15m acceleration
current 1h - previous 1h acceleration
cross-venue return dispersion
first venue to move / lagging venues
```

Для каждого окна сохраняются top `3` gainers и top `3` losers. В candidate pool
попадает только строка, которая одновременно входит в top-3 и проходит хотя бы
одно широкое intake-условие:

| Window | Initial absolute move |
|---|---:|
| `5m` | `1.5%` |
| `15m` | `2.5%` |
| `1h` | `5%` |
| `4h` | `8%` |
| `24h` | `15%` |
| `72h` | `25%` |
| `7d` | `40%` |

Вместо absolute move достаточно robust `|z| >= 2.5` относительно собственной
истории symbol/window. Порог является intake, не entry. После `7d` QA его можно
корректировать только для разумного event coverage. Монета с одной доступной
биржей не выбрасывается, но получает `single_venue`; отсутствие полного окна
даёт `insufficient_history`, а не нулевой return. New listings сохраняются
отдельной cohort, чтобы их не сравнивать с устоявшимися контрактами.

Native 24h поля бирж сохраняются как cross-check, но канонические `1h/24h/72h/7d`
рейтинги считаются из одинаковых собственных timestamped snapshots. Это устраняет
разницу rolling-24h против UTC-day и даёт KuCoin те же окна без тяжёлого
per-symbol kline fan-out.

Coinglass `coins-price-change` может независимо подтвердить `5m..24h` mover, но
остаётся plan-dependent external tag. CoinMarketCap gainers/losers относится к
spot/top-market-cap universe и полезен только как слабый внешний context: он не
должен занимать отдельную квоту perpetual candidates.

### E. OI / volume / price / premium candidates

OI сравнивается прежде всего с собственной историей той же биржи: raw OI разных
бирж может иметь разные единицы, multiplier и качество отчётности.

Триггеры:

- OI change и robust z-score `5m/15m/1h/4h/24h`;
- volume/turnover anomaly на тех же окнах;
- price return, realised range/volatility и acceleration;
- одновременный OI + volume spike;
- price/OI divergence;
- mark-index premium и отклонение premium одной биржи от остальных;
- межбиржевое lead/lag: какая биржа первой изменила price, funding или OI;
- liquidation и long/short ratio как optional enrichment, но не обязательный gate.

Квадранты `price up/down x OI up/down` сохраняются как признаки, а не как заранее
истинные объяснения «новые long», «short covering» и т.п. Их ценность должна быть
доказана outcomes.

### F. Positions and roll candidates

Каждая открытая пара автоматически становится кандидатом и получает все доступные
альтернативные ноги. Для current `long=L, short=S` оцениваются:

- оставить обе ноги;
- заменить только long `L -> K`;
- заменить только short `S -> K`;
- закрыть обе ноги;
- временный трёхногий bridge во время roll;
- split-leg/multi-leg только как отдельная поздняя shadow-гипотеза.

Research roll edge:

```text
roll_value(horizon) =
    improvement in expected funding cashflow
  + improvement in expected basis path
  - close(old leg) costs
  - open(new leg) costs
  - temporary hedge/margin risk penalty
```

Для каждой позиции сохраняется таблица всех доступных вариантов на каждом decision
tick, а не только победитель. Это позволит позже понять, когда roll был действительно
лучше HOLD/EXIT и когда красивый funding был съеден новым basis.

### G. Matched controls

Минимум `15–20%` Baseline/Watch мест резервируется для событий без сильного
сигнала, сопоставленных по:

- symbol age и ценовому диапазону;
- volatility и market regime;
- числу доступных бирж;
- времени до settlement;
- времени суток/дню недели.

Controls необходимы для поиска предвестников funding/spread spike и честной оценки
false positive rate.

## Роль стакана и исполнения

Depth намеренно исключается из primary hypothesis mining. Система не должна
отбрасывать малоликвидную maker-биржу только потому, что видимый taker depth мал.
Это согласуется с текущим исполнением: passive order ставится на менее ликвидной
стороне, фактический fill немедленно хеджируется на более ликвидной стороне, а
объём разбивается на chunks.

На prospective-сборе достаточно:

- best bid/ask и freshness;
- top size, только если уже приходит в ticker;
- tradeability/status/contract multiplier;
- грубой volume/OI context без depth-score.

На live-shadow каждого варианта дополнительно измеряются:

- сколько времени maker order ожидал fill;
- доля и последовательность partial fills;
- число chunks;
- цена hedge и задержка;
- фактическое adverse movement между fill и hedge;
- effective entry/exit basis и полные комиссии.

Таким образом depth не решает, есть ли alpha. Реальное поведение исполнителя само
показывает, какие сигналы переживают maker-first/chunk execution.

## Candidate lifecycle

```text
DISCOVERED
  -> BASELINE_BUFFERED
  -> WATCH
  -> HOT
  -> EVENT_ACTIVE
  -> COOLING
  -> CLOSED_FOR_LABELS
```

Отдельный future shadow lifecycle:

```text
SHADOW_WAIT
  -> SHADOW_ENTER
  -> SHADOW_HOLD
  -> SHADOW_PARTIAL_EXIT / SHADOW_ROLL
  -> SHADOW_EXIT
  -> OUTCOME_FINAL
```

Collector lifecycle и strategy lifecycle не смешиваются: добавление/удаление
кандидата не должно само открывать или закрывать даже shadow-позицию.

## Что сохранять в каждой venue observation

Обязательное:

- event time exchange + receive time local + latency/staleness;
- canonical/exchange symbol и instrument version;
- bid/ask, mid, last, mark, index;
- funding raw, funding bps/hour, interval, next settlement;
- realised funding после settlement;
- OI raw и normalized notional, если корректно известен multiplier;
- volume/turnover, price change/range;
- contract availability/status и source health;
- candidate tags и sampling tier.

Опциональное:

- top bid/ask size;
- liquidation stream;
- long/short ratios;
- exchange-specific predicted funding;
- public trade imbalance.

Отсутствующее поле остаётся `missing` с причиной и не заменяется нулём.

## Выводы из внешнего исследования

Публичные scanner-практики обычно комбинируют funding, spread, time-to-settlement,
spread duration, OI/volume и fee-adjusted result. Это подтверждает выбранные
семейства, но не даёт проверенного универсального entry/exit правила.

Hummingbot Cross-Exchange Market Making документирует ровно применяемую в FeeArb
идею: passive maker order на менее ликвидной площадке и taker hedge на более
ликвидной после fill. Поэтому полный depth разумно вынести из alpha selection в
shadow execution evidence.

Публичные обсуждения трейдеров дополнительно упоминают persistence сигнала,
funding interval, premium/oracle spread, OI, volume, fees и фактические fills.
Это anecdotal hypothesis input, а не доказательство доходности.

Исследования perpetual futures предупреждают о двух важных ограничениях:

- perpetual spread не обязан сходиться к фиксированной дате, в отличие от
  обычного срочного фьючерса;
- raw OI может быть несопоставим или некорректно опубликован разными биржами.

Поэтому pair-specific equilibrium и within-exchange OI changes важнее абсолютного
сравнения raw OI между площадками.

Проверенные источники на дату проектирования:

- Coinglass funding arbitrage API:
  https://docs.coinglass.com/reference/fr-arbitrage
- Coinglass futures pair markets:
  https://docs.coinglass.com/reference/pairs-markets
- Coinglass funding/OI exchange lists and liquidation endpoints:
  https://docs.coinglass.com/reference/fr-exchange-list
  https://docs.coinglass.com/reference/oi-exchange-list
  https://docs.coinglass.com/reference/endpoint-overview
- Coinglass liquidation heatmap model2 availability/schema:
  https://docs.coinglass.com/v4.0-zhtw/reference/liquidation-heatmap-model2
- ArbitrageScanner funding table and API overview:
  https://arbitragescanner.io/ru/funding-rates
  https://arbitragescanner.io/crypto-api
- Hummingbot cross-exchange market making:
  https://hummingbot.org/strategies/v1-strategies/cross-exchange-market-making/
- Binance futures all BBO stream:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
- Bybit public ticker:
  https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
- OKX API/WebSocket guide:
  https://www.okx.com/docs-v5/en/
- KuCoin symbol contract data and WebSocket limits:
  https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
  https://www.kucoin.com/docs-new/rate-limit
- Gate futures WebSocket:
  https://www.gate.com/docs/developers/futures/ws/en/
- Fundamentals of Perpetual Futures:
  https://arxiv.org/abs/2212.06888
- Reconciling Open Interest with Traded Volume in Perpetual Swaps:
  https://arxiv.org/abs/2310.14973
- Community discussion of funding drivers (anecdotal, hypothesis-only):
  https://www.reddit.com/r/algotradingcrypto/comments/1v1o58n/learning_material_on_funding_rates/
- Community discussion of persistent perp spread entry (anecdotal,
  hypothesis-only):
  https://www.reddit.com/r/algotradingcrypto/comments/1u6b2rj/roughly_3_months_of_arbitrage_started_with_2k_usdt/

## Пошаговый план реализации

## Реализованный checkpoint 2026-08-12

Phase O0 частично реализован и доступен на отдельной странице
`/strategy-lab-observatory`:

- external contract: `strategy_lab_external_candidate_v1`;
- exact-five ArbitrageScanner adapter с правильными aliases, scale и sign;
- Coinglass web fallback с реальными checkbox-click, проверкой выбранного набора,
  postcondition таблицы и last-good;
- bounded union максимум `30`, где overlap получает P1, но каждое наблюдение явно
  имеет `trade_signal=false`;
- one-shot GET/POST API без scheduler и без связи с execution;
- старый intake и его настройки убраны с главной страницы и не стартуют с backend.

Свежий combined smoke в отдельном временном каталоге: Coinglass `20/20`,
ArbitrageScanner `789 raw / 497 eligible`, итог `30`, overlap `16`. Числа являются
снимком меняющихся внешних таблиц, а не фиксированной нормой.

Не реализованы и не считаются разрешёнными этим checkpoint: постоянный own-feed
collector, `1h/24h` preflight,
месячный collector, strategy shadow и любые заявки.

### Phase O0 — data contract and bounded preflight

1. **Готово:** зафиксировать schema/version, sign convention, candidate/event identity и
   общий `source_observation` contract для own/Coinglass/ArbitrageScanner.
2. **Готово для external intake:** добавить replay fixtures и контрактные тесты для exact exchange aliases,
   long=min funding / short=max funding и потери/stale внешнего источника.
3. **Готово:** реализовать общий Instrument Registry для пяти бирж.
4. **Готово для бесплатных источников:** реализовать source adapters в выключенном по умолчанию research-контуре:
   ArbitrageScanner exact-five; Coinglass official API при наличии ключа; web
   parser только как slow fallback.
5. Сделать multiplexed public feed prototype без постоянного long run.
6. Проверить coverage common symbols, freshness, reconnect, field availability,
   source overlap/disagreement и candidate recall.
7. Провести bounded `1h`, затем `24h` preflight и измерить calls/messages,
   missingness, compressed bytes/row, CPU/RAM и gaps.
8. Обновить реальные caps только по результату preflight.

### Phase O1 — prospective collector

1. Запустить Registry/Discovery/Baseline/Watch/Hot без trading decisions.
2. Добавить immutable candidate ledger, controls и lifecycle.
3. Ввести ежедневный QA: gaps, stale feeds, mapping, duplicates, clock skew,
   venue/symbol coverage и disk forecast.
4. Первый audit через `7d`, первый research freeze через `30d`; продолжить сбор,
   если независимых событий или режимов недостаточно.

### Phase O2 — broad hypothesis search

1. Старые данные — development only; prospective dataset делится chronologically.
2. Проверить funding spike/cooling, pair equilibrium/reversion/expansion,
   OI-volume-price regimes, entry timing, exit timing и roll.
3. Использовать train/validation/locked future holdout, unseen-symbol и
   unseen-pair holdout, multiple-testing protection и concentration metrics.
4. Выбрать несколько устойчивых семейств, а не один лучший backtest.

### Phase O3 — live-shadow strategies

1. Зафиксировать версии правил до новых событий.
2. Сравнивать ENTER/WAIT/HOLD/EXIT/PARTIAL_EXIT/ROLL без заявок.
3. Использовать реальный maker-first/chunk simulator и записывать execution
   evidence вместо предварительного depth-фильтра.
4. Не переходить к paper, пока execution-aware prospective holdout не положителен
   и tail-risk gates не пройдены.

## Точный следующий безопасный шаг

Phase O0 bounded implementation завершена. Следующий шаг требует отдельного
операторского решения:

- разрешить или не разрешить bounded `1h` preflight;
- preflight остаётся research-only и не включает shadow positions/orders;
- разрешение `1h` не является разрешением `24h` или месячного сбора.

После тестов нужен отдельный операторский verdict перед `1h/24h` preflight, а после
preflight — отдельное подтверждение перед месячным prospective-сбором.

## Правило сопровождения для ИИ-агентов

После каждого meaningful блока Observatory этот документ обновляется в том же
коммите: что реализовано, coverage/QA, фактические caps, тесты, ограничения,
runtime status и точный следующий шаг. Нельзя объявлять проектный cap фактической
пропускной способностью до bounded preflight.

## Checkpoint 2026-08-12 — Instrument Registry v1

- Добавлен `strategy_lab_instrument_registry_v1`. Он хранит реальный
  `exchange_symbol`, canonical/base/quote/settle, active/status, linear/perpetual,
  contract size, tick/step/minimum и funding cadence там, где она опубликована.
- Binance, Bybit, OKX и KuCoin читаются параллельными public bulk endpoints. Gate
  намеренно работает в `candidate_scoped_exact_contract`: bulk endpoint передал
  лишь около `15 KB` из `1.17 MB` за `30s`, а отдельный контракт отвечает за
  `1–2s`; запросы ограничены текущими candidate seeds и concurrency `5`.
- `XBT` нормализуется в asset identity `BTC`, но token multipliers (`1000...`)
  не схлопываются. Несколько active-контрактов одного canonical symbol на одной
  бирже дают quarantine `multiple_active_contracts`.
- ArbitrageScanner часто возвращает общий `SYMBOLUSDT` даже для OKX/KuCoin/Gate.
  Это больше не считается исполнимым symbol ID: точный ID берётся только из
  Registry, форматное различие записывается как QA feature. Если provider-symbol
  указывает уже на другой asset, остаётся fail-closed veto
  `external_symbol_asset_mismatch`.
- Bounded live-smoke на десяти текущих ArbitrageScanner seeds завершился за
  `4.8s`: active registry `527 Binance / 702 Bybit / 431 OKX / 659 KuCoin / 9 Gate`;
  Gate подтвердил `9/10`, а отсутствующий `REDSTONE_USDT` получил ожидаемый
  `fewer_than_two_verified_venues`. Девять seeds имели минимум две проверенные
  площадки. Эти цифры являются изменяемым снимком, не capacity promise.
- Fixture/contract tests: `10 passed`; полный regression: `762 passed`,
  `8 subtests`, `15 warnings`. Trading, ARM, orders, transfers и margin не
  затрагивались.
- Точный следующий блок: bounded multiplexed BBO/ticker feed prototype поверх
  Registry, normalized own-observation contract и coverage/freshness QA. После
  него требуется отдельный operator verdict перед `1h` preflight.

## Checkpoint 2026-08-12 — bounded multiplexed public feed v1

- Добавлены `strategy_lab_bounded_public_feed_v1` и
  `strategy_lab_own_observation_v1`. Probe жёстко ограничен `10` symbols / `30s`,
  не имеет scheduler или execution API и открывает максимум один public WS на
  каждую из `Binance/Bybit/OKX/KuCoin/Gate`.
- Venue symbols берутся только из Instrument Registry. Нормализуются BBO,
  last/mark/index, funding, predicted funding, next settlement, OI и volume;
  отсутствующее поле остаётся отсутствующим и попадает в field-availability QA.
- Binance BBO остаётся websocket-native. В двух bounded проверках Binance
  `markPrice@1s` подтверждал подписку, но не присылал update; поэтому mark/index/
  funding/next funding seed берётся одним public bulk REST snapshot. Это один
  запрос на всю выбранную группу, не per-symbol loop.
- QA отчёт содержит pair и per-venue coverage, missing pairs, freshness,
  field availability, invalid BBO, connections, messages/updates и parse/
  subscription/REST errors. Общий deadline не может растянуть probe более чем
  примерно на `3s` сверх заданного market window.
- Live-smoke `8s` на пяти текущих seeds дал `16/20` observations: Binance `100%`,
  Bybit `100%`, OKX `100%`, KuCoin `75%`, Gate `40%`; у всех venues было одно
  соединение, без parse/subscription errors и crossed BBO. Это ожидаемо выявило,
  что event-driven KuCoin/Gate может не дать update по тихому контракту в столь
  коротком окне; missing pairs не скрываются.
- Tests: профильные `15 passed`; полный regression `767 passed`, `8 subtests`,
  `15 warnings`. Long run и стратегии не запускались.
- Точный следующий блок: Observatory integration — кнопки Registry refresh и
  bounded feed probe, persisted last-good snapshot, mapping/coverage/freshness
  verdict. Только после его тестов запрашивается operator approval на `1h`
  preflight; `24h` и месячный сбор остаются отдельными решениями.

## Checkpoint 2026-08-12 — Observatory Registry/feed integration

- Отдельная страница теперь проводит Phase O0 строго по цепочке: external source
  refresh -> Registry refresh -> bounded feed probe. Endpoint probe валидирует
  `duration_sec=1..30` и `max_symbols=1..10`; фонового запуска нет.
- Persisted state содержит compact candidate-only Registry, verification каждого
  external observation, eligible candidate count, feed quality и полный короткий
  report. Last-good сохраняется при ошибке. Новый успешный source refresh
  автоматически делает Registry/feed stale, а stale Registry блокирует probe.
- Feed quality требует минимум две observations, одну монету на двух venues,
  минимум две наблюдавшиеся биржи, отсутствие subscription errors и crossed BBO.
  Coverage ниже 100% остаётся допустимой для bounded research и явно показывается,
  а не маскируется.
- UI выводит registry source count/mode/error, feed status/connections/updates,
  per-venue coverage, missing pairs, freshness, Registry venues и число own-feed
  venues для каждой монеты. Торговых controls нет; signal везде `NO (research)`.
- End-to-end temporary-state smoke: `30` candidate union, `29` Registry-eligible,
  `8s` own feed `85%` pairs, все пять venues; Binance/Bybit/OKX/KuCoin `100%`,
  Gate `40%`, invalid BBO `0`. Меняющиеся числа — диагностический snapshot.
- Focused tests `23 passed`; полный regression `771 passed`, `8 subtests`,
  `15 warnings`. Следующий шаг — только отдельный operator verdict перед `1h`
  preflight. `24h`, месяц, shadow strategies и trading остаются запрещены.

## Checkpoint 2026-08-12 — 1h capacity-preflight runner

- `strategy_lab/preflight.py` запускает только bounded rotating public probes:
  максимум `3600s`, окно `10` символов, market-window до `30s`, период `60s`.
  Candidate list и точные exchange symbols берутся из только что обновлённых
  external sources + Instrument Registry; догадки по тикерам запрещены.
- Runtime artifacts находятся только в игнорируемом
  `data/research/strategy_lab_observatory/preflight/<run>/`: bootstrap snapshot,
  `observations.jsonl.gz`, `cycles.jsonl`, `status.json`, `report.json`.
  Это диагностический датасет capacity/quality, не стратегия и не сигнал.
- Ротация позволяет проверить до `60` текущих кандидатов без одновременной
  подписки на весь universe. Метрики отдельно сохраняют пять venues, поэтому
  слабый Gate не маскируется общим процентом. Core gate для Binance/Bybit/OKX/
  KuCoin — `80%`; Gate пока warning ниже `50%` до получения часовой выборки.
- Процесс имеет exact-confirmation, проверку диска, single-run lock, продолжает
  следующий цикл после локальной ошибки, но отражает её в fail-closed QA.
  Профильная интеграционная регрессия: `30 passed`; полный suite: `783 passed`,
  `8 subtests`, `15 warnings`. `16s` real CLI smoke охватил `10/10` symbols и
  завершил `2/2` cycles; core coverage был `100/100/100/50%`, Gate `60%`.
  Строгий `FAIL` короткого окна сохранён из-за KuCoin, а Windows RSS после
  исправления подтверждён (`64.9 -> 71.4 MB`, peak `74.9 MB`).
- Оператор разрешил `1h` и последующее автономное исправление research-only
  проблем. `24h` допускается только как обоснованное продолжение после анализа
  часа; месячный prospective collector, strategy shadow/paper/live, ордера и
  переводы этим не включены.

Точный следующий шаг: полный regression + commit, затем один bounded `1h` run,
разбор реального coverage/load/disk и запись результата сюда до решения о `24h`.

## Checkpoint 2026-08-12 — source-aware eligibility fix

- Первый `1h` был прерван после одного цикла и не является capacity result:
  Registry сообщил `47` eligible candidates, тогда как повторная локальная
  проверка только по `>=2 venues` выбрала `54` и могла вернуть source-vetoed
  symbols.
- CLI теперь формирует feed universe строго из persisted Registry verification
  с `eligible_for_observation=true`, сохраняя rank candidate union, и fail-fast
  сравнивает итог с `eligible_candidate_count`. Профильные тесты: `31 passed`;
  полный suite: `784 passed`, `8 subtests`, `15 warnings`.
- Точный следующий шаг: чистый bounded `1h`; прерванный каталог остаётся только
  runtime evidence раннего fail-closed контроля и в анализ не включается.

## Checkpoint 2026-08-12 — clean 1h result

- Valid run: `preflight-20260812T194321Z`, `60/60` cycles, `1910` gzip rows,
  `47/47` symbols, `91.563%` pairs, no cycle/feed/BBO errors. Integrity checks
  подтвердили одинаковые row counts в gzip/report/status и закрытый lock.
- Binance/Bybit/OKX/KuCoin дали `100%`; Gate дал `38.462%` без connection errors.
  Это event-driven snapshot gap: многие тихие Gate contracts не присылают ticker
  за `30s`, хотя подписка исправна.
- Capacity PASS: CPU `1.045%` core, peak RSS `112.7 MB`, max delay `0.015s`,
  disk `209.9 KB/hour` / `5.04 MB/day`. Уменьшать число кандидатов из-за
  локальной нагрузки не требуется.
- Data completeness пока FAIL для главной цели: OI/quote volume стабильно есть
  у Bybit, но отсутствует в собственных rows Binance/OKX/KuCoin и частично
  появляется у Gate. Поэтому `24h` на v1 не запускается.
- Следующий блок: exact public REST snapshots только для ротируемых `<=10`
  symbols (не весь universe), field-level coverage QA и повторный bounded test.
  WS остаётся источником BBO; REST не используется как торговый сигнал.

## Checkpoint 2026-08-12 — field-complete v2 smoke

- Candidate-scoped REST seed добавлен поверх WS: Binance ticker+OI, OKX
  ticker+OI, KuCoin contract+level1, Gate ticker; Bybit не делает лишний REST.
  Запросы идут только по Registry exchange symbols текущего окна, concurrency
  `5`, timeout `12s`; одна ошибка не останавливает WS, но попадает в QA.
- Field QA требует core coverage bid/ask/mark/funding/OI/quote-volume `>=80%`.
  Отчёт отдельно считает REST requests, updates, bytes и errors.
- Valid smoke `preflight-20260812T205128Z`: `PASS`, `43/43` pairs, обязательные
  поля `100%` на всех пяти venues, `60` REST requests, `34,110` response bytes,
  `0` REST/subscription/parse/BBO errors. Тихие Gate и KuCoin теперь имеют
  snapshot, но WS продолжает обновлять BBO при наличии событий.
- Full regression: `790 passed`, `8 subtests`, `15 warnings`. Точный следующий
  шаг — зафиксировать код и проверить длительную rate/error/field стабильность;
  месячный O1 и strategy shadow ещё не включены.

## Checkpoint 2026-08-13 — v2 one-hour PASS

- `preflight-20260812T205407Z`: `PASS`, `60/60`, `2115` rows, `49/49` symbols,
  `100%` pairs и `100%` bid/ask/mark/funding/OI/quote-volume на всех пяти venues.
- `2924` REST calls / `1.68 MB`, errors `0`; gzip/report/status совпали.
  Source provenance сохранён, включая явный derived flag для OKX quote-volume.
- CPU `1.45%` core, peak RSS `101.8 MB`, delay `0.016s`, disk forecast
  `6.93 MB/day`. Следующий safe step — отдельный bounded `24h` QA profile;
  prospective month/shadow/trading пока не разрешены этим результатом.

## Checkpoint 2026-08-13 — 24h QA profile

- Добавлен отдельный CLI `--profile 24h`. Он требует точную confirmation
  `RUN STRATEGY LAB PREFLIGHT 24H` и ровно `86400s`; `1h` ограничен `<=3600s`.
  Проверки выполняются до I/O, оба профиля используют один exclusive lock.
- Это long QA текущего фиксированного verified candidate set: он проверяет
  rate/timeouts, funding boundaries, field completeness, cadence, RSS/disk.
  Он не обновляет candidate universe внутри суток и не является O1 collector.
- Профильные tests: `22 passed`; связанная Observatory regression: `18 passed`.
  Полный suite: `793 passed`, `8 subtests`, `15 warnings`. Следующий шаг —
  commit, background launch и early-health check.

## Runtime 2026-08-13 — active 24h QA

- Active run `preflight-24h-20260812T215857Z`, start `21:59:17Z`, deadline
  примерно `21:59Z` 13 августа / `00:59` МСК 14 августа.
- Bootstrap PASS: `60` union, `48/48` Registry-verified, profile `24h`,
  `86400s`, fresh Coinglass + ArbitrageScanner.
- First cycle PASS: `44/44` pairs, `61` REST requests, errors/BBO failures `0`.
- Не перезапускать и не удалять lock, пока PID жив. После завершения провести
  integrity/field/rate/resource audit и обновить этот документ до O1 decision.
