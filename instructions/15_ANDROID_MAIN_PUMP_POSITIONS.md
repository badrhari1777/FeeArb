# Android: Main и Pump Live позиции

## Назначение

Версия Android-приложения `0.3.0` показывает на вкладке `Positions` позиции
основного модуля и Pump Live, не смешивая их торговую логику.

Данные Pump являются read-only. Кнопки `Add`, `Exit`, `Manual`, `Grid` и
настройка Auto Exit остаются только у позиций основного модуля.

## Переключатели

В верхней части `Positions` доступны:

- `All` — общий риск-блок, Pump-контур и основные позиции;
- `Main` — только позиции основного арбитражного модуля;
- `Pump` — только Bybit Pump subaccount и его позиции.

## Общий риск-блок

Показывает:

- количество Main и Pump позиций;
- Pump slots `использовано / лимит`;
- общий нереализованный PnL;
- минимальный ликвидационный буфер;
- количество проблем защиты;
- свежесть данных.

Статусы:

- `PROTECTED` — данные свежие, ошибок защиты и предупреждений нет;
- `CHECK` — данные старше 45 секунд, есть warning-риск либо armed-монитор не
  работает;
- `RISK` — отсутствует защита, есть high-risk позиция или Pump monitor error;
- `UNKNOWN` — backend ещё не вернул достаточно данных.

## Pump-карточка

Даже без открытых позиций отображаются:

- `armed / disarmed` и активность монитора;
- полный и доступный баланс subaccount;
- резерв;
- возраст последнего защитного цикла;
- результат последней отправки уведомления.

Для открытой Pump-позиции дополнительно отображаются:

- symbol, short side, qty и PnL;
- entry, mark и liquidation price;
- liquidation buffer;
- exchange TP и emergency SL;
- top-up `использовано / максимум`;
- прошедшее и максимальное время удержания;
- количество исполненных и открытых ступеней;
- раскрывающийся список ступеней.

## Обновление

При открытии вкладки `Positions` приложение запрашивает:

- `GET /api/mobile/positions` для существующих Main-карточек и действий;
- `GET /api/positions/overview` для общей сводки и Pump Live.

Повторный запрос выполняется каждые 15 секунд, только пока вкладка Positions
видима и приложение находится в foreground. В фоне polling отключается.

Отказ нового overview API не блокирует показ существующих Main-позиций:
ошибки двух источников отображаются раздельно.

## Безопасность

- Pump Live нельзя включить, выключить или аварийно закрыть с этого экрана.
- Pump-карточка не переиспользует Main trade actions.
- API overview не передаёт Android-приложению ключи, секреты, UID или
  preflight identity.

## Main position valuation (Android 0.4.1)

Android consumes the same backend position contract as the web dashboard; it
does not recalculate venue notionals locally. Main cards and details show:

- hedged and gross current exposure at current Mark Prices;
- separate hedged and gross entry exposure;
- per-leg current/entry exposure and current Mark Price;
- signed estimated next funding in USDT.

The universal calculation and unavailable-Mark behavior are defined in
`instructions/14_UNIFIED_POSITIONS_WEB.md`. Position actions still use
base-coin quantities, so this presentation correction does not resize or
rebalance an open hedge.

## Balance view (Android 0.4.0+)

The `Balances` tab uses `GET /api/mobile/positions` as its single read-only
source and includes the Pump Live subaccount without mixing trading ownership:

- every regular exchange row is labelled `Main account`;
- Bybit has separate `Main account` and `Pump subaccount` rows;
- the Bybit summary card shows `Main`, `Pump sub`, `Combined`, and combined
  available USDT;
- the overall balance includes all reporting main accounts plus Pump exactly
  once;
- a missing Pump snapshot is displayed as unavailable and is never converted
  into a healthy zero balance.

The same account labels and aggregates are returned under `accounts` for the
desktop main-page balance table. This view is display-only: it does not change
Pump sizing, margin management, order placement, or transfers.

The desktop `/api/dashboard` contract must preserve `accounts.balance_summary`.
If the API contains totals but the summary cards show `-`, verify rendering in
`webapp/static/dashboard.js` and refresh the cache-busted static bundle before
investigating exchange balances.
