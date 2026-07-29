# Pump/Dump Paper-to-Live Transition Research

Date: 2026-07-29

Status: the research remains the sizing evidence, and a guarded first-canary
implementation is now present. Live entry is disabled by default and after
every backend restart. This work does not create the Bybit subaccount, copy
credentials, transfer funds, or arm real entries.

## Implemented First Canary

The operator approved a smaller initial account than the conservative `$5000`
target:

- `$1000` subaccount equity;
- `$700` deployable;
- `$300` protected reserve;
- four fixed `$175` margin slots at `3x`;
- operational entry cap `1` until the first full entry/add/TP/restart/exit
  review; hard strategy cap remains `4`.

This is a linear one-fifth scale of the fixed `$5000 / $3500 deployable /
$1500 reserve / 4 slots` replay. The historical sample therefore still has
`35` trades and the same percentage statistics (`3.146%` max drawdown), while
the observed peak concurrent rescue scales from `$948.41` to about `$189.68`,
below the `$300` reserve. This scaling does not model minimum sizes, live
slippage, latency, API failures, or delisted contracts; live canary evidence
must supersede it.

Implemented controls:

- separate `bybit_pump` credentials and account identity;
- local ignored key file `config/pump_live.env`;
- main-short `main_pullback_tier` is the only live signal consumer;
- all long, slow-pump, cycle, and candidate tracks remain paper/shadow;
- preflight verifies subaccount identity, UID, permissions, UTA, equity,
  available reserve, isolated mode, and flat/clean exchange state;
- dynamic-IP keys expose Bybit `deadlineDay` and `expiredAt` in preflight;
- durable intent is written before the first order;
- guarded first market leg plus post-only rescue ladder;
- exchange-side full-position market TP, resynchronized after adds;
- tier time-stop, liquidation-buffer monitoring, capped margin top-ups, and
  emergency reduce-only close;
- two-cycle flat confirmation, with add-order cancellation on the first flat
  cycle;
- unknown exchange state, ambiguous execution, and monitor errors disarm new
  entries;
- restart recovery monitors existing positions but never rearms entries;
- manual disarm and confirmed emergency-close endpoints/UI.

The operator procedure is
`instructions/13_PUMP_LIVE_BYBIT_SUBACCOUNT.md`. Automatic transfers remain
out of scope for this phase: the operator manually funds the subaccount before
arming.

## Evidence

Reproduce the report:

```powershell
.\.venv\Scripts\python.exe scripts\pump_live_transition_research.py
```

Generated output:

```text
data/research/pump_live_transition_research/index.md
data/research/pump_live_transition_research/historical_strategy_summary.csv
data/research/pump_live_transition_research/historical_strategy_trades.csv
data/research/pump_live_transition_research/historical_cycle_reference_since_2024.csv
data/research/pump_live_transition_research/money_management_summary.csv
data/research/pump_live_transition_research/money_management_trades.csv
data/research/pump_live_transition_research/paper_strategy_summary.csv
data/research/pump_live_transition_research/paper_short_scaling_scenarios.csv
data/research/pump_live_transition_research/bybit_subaccount_capabilities.csv
```

The exact-current-rule replay has `126` unique cases after the requested 2024
boundary. The actual available entry range is 2024-09-21 through 2026-06-23.
It uses reconstructed hourly entries and currently listed symbols, so survivor
bias, delisted contracts, live slippage, transfer latency, and API failure tails
remain outside the model.

## Strategy Decision

At fixed `$3000` deployable capital and four short slots, the exact current
`main_pullback_tier` strategy ranked first:

- 35 trades, 34 wins and 1 loss;
- raw ROI `247.66%`, risk-adjusted ROI `240.85%`;
- max drawdown `4.49%`;
- historical max concurrent rescue/top-up requirement `$812.92`;
- 13 test-period trades, all winners.

`super_pump_shadow` was close at `239.12%` raw ROI and had two losses.
`conservative_control` returned `215.27%`. The narrow
`short_clean_p100_l3_shadow` had 20/20 historical wins but lower total ROI and
is a subset/candidate, not independent diversification. All short tracks reuse
many of the same events; their PnL must never be summed.

The older cycle replay still shows that a long+short portfolio can improve
historical capital use. Since 2024, the `long_broad + short_clean_p100_l3`
reference produced:

- short-only four slots: ROI `555.73%`, max drawdown `19.15%`;
- cycle `4 short + 2 long`: ROI `643.08%`, max drawdown `12.76%`;
- cycle `4 short + 1 long`: ROI `712.06%`, max drawdown `15.32%`.

This is not an apples-to-apples replacement for the exact tiered main short
strategy. It uses the narrower p100 short control. More importantly, the
current live-like paper sample has one closed long and that trade stopped for
about `-$251.80`. Therefore the first live candidate is the main short track
only. Long and extra candidate tracks remain paper/shadow until they have a
materially larger independent sample.

## Why a Dedicated Bybit Subaccount Is Recommended

The current FeeArb exchange registry has one logical `bybit` client. Position,
margin, protective-order, and exit ownership is generally keyed by exchange,
symbol, and side. On one Bybit account, two strategies trading the same symbol
and side become one exchange position after fills. `orderLinkId` can identify
orders but cannot split the resulting position, liquidation price, isolated
margin, or close quantity by strategy.

Use a dedicated Bybit UTA subaccount for Pump/Dump:

- master account: control plane, protected treasury, balance reads, and
  approved transfers;
- pump subaccount API key: Pump orders, positions, order reconciliation, and
  isolated-margin operations only;
- main FeeArb key: funding/spread/grid activity only.

Before live implementation, account identity must become part of every durable
key and record: `account_alias + exchange + symbol + side + strategy_id`.
The order-link namespace must also include the account alias and strategy.

## Bybit API Capabilities

Official V5 endpoints required by the design:

- list subaccounts: `GET /v5/user/query-sub-members`;
- inspect API-key identity and permissions: `GET /v5/user/query-api`;
- read a subaccount coin balance and transfer-safe amount:
  `GET /v5/asset/transfer/query-account-coin-balance`;
- transfer main/sub or sub/main:
  `POST /v5/asset/transfer/universal-transfer`;
- confirm transfer settlement:
  `GET /v5/asset/transfer/query-universal-transfer-list`;
- read positions: `GET /v5/position/list`;
- add isolated margin: `POST /v5/position/add-margin`.

The transfer request uses a caller-generated UUID. Acceptance of the POST is
not settlement: the bot must poll the transfer record until `SUCCESS`, then
re-read the receiving balance and position. A subaccount key can transfer only
to its parent main account; master credentials are the preferred transfer
control plane.

The current command-line diagnostics did not have access to the running
service's Bybit credentials, so the actual key's master/sub identity and wallet
permissions are not yet verified. Do this later through a service-side,
read-only permission probe; do not copy secrets into reports or logs.

## Money Management Decision

For a Pump-specific starting capital of `$5000`, use:

- `$3500` deployable across four fixed short slots: `$875` per slot;
- `$1500` free USDT reserve on the Pump subaccount;
- 3x isolated margin;
- no long slots and no dynamic sizing during the initial live canary;
- manual approval for any external top-up or master/sub transfer.

The historical fixed replay for this row had 35 trades, raw ROI `173.36%`,
risk-adjusted ROI `168.64%`, max drawdown `3.15%`, and max concurrent rescue
requirement `$948.41`, fully covered by the `$1500` reserve. The reserve is
about 1.58x that observed requirement. The `$5000 / $1000 reserve` row was
short by `$83.89` in one historical stress overlap, so `$1500` is the safer
starting choice.

Four slots are preferable for the first live risk profile. Three slots captured
33 rather than 35 trades and improved raw ROI through larger position size, but
increased max drawdown from `3.15%` to `4.19%` and raised concurrent rescue need
from `$948.41` to `$1264.54`.

Reserve target:

```text
max(
    30% of Pump equity,
    1.5 * replayed concurrent rescue need at the current slot size,
    configured absolute floor
)
```

Profit growth policy after the canary:

1. Split positive realized PnL 70% to deployable capital and 30% to reserve.
2. Increase a slot only in discrete 25% steps.
3. Require the reserve target to remain covered after the increase.
4. Cap the slot at 2x its initial size until a new historical and live-like
   review explicitly raises the cap.
5. Decrease sizing immediately after losses; never wait for a full 25% step.

Full-equity dynamic compounding produced very large historical numbers and
large top-up tails. It is a diagnostic only and is prohibited as the initial
live sizing mode.

The `$5000` above is Pump-specific capital. The funding/grid account requires
its own protected reserve and must not be counted as Pump reserve.

## Transfer and Margin State Machine

Automatic transfers are not the first-line liquidation defense. The safe
sequence is:

1. Read fresh Pump positions, isolated margin, liquidation distance, wallet
   balance, and all pending Pump executions.
2. Read main and subaccount USDT balances.
3. Calculate the transfer-safe amount and both accounts' protected floors.
4. If the Pump reserve is below its early threshold, block new Pump entries.
5. In manual phase, request operator approval.
6. Submit one idempotent universal transfer.
7. Poll the transfer record to `SUCCESS`; timeout means unknown, not failed.
8. Re-read the Pump balance and position.
9. Add isolated margin only to the still-existing, same-side Pump position.
10. Re-read liquidation distance and emit an auditable result.

Use hysteresis, a cooldown, and one transfer lock per source/destination/coin.
Never move the same USDT back and forth in response to noisy snapshots. If both
accounts are below their protected floors, stop new entries and alert; do not
drain the main strategy to rescue Pump automatically.

## Staged Live Plan

1. Keep current paper running and collect at least 25 closed main-short trades
   across at least 10 symbols, with no unresolved position or paper-state
   reconciliation failures.
2. Add `account_alias` ownership and a separate Pump Bybit adapter. Continue
   paper only.
3. Add a read-only subaccount dashboard and permission probe.
4. Add transfer shadow decisions that log what would be moved but cannot send.
5. Validate transfer and position reconciliation on Bybit testnet.
6. Enable mainnet transfers behind explicit operator approval and a small hard
   cap.
7. Start one small fixed live short slot; do not enable long or compounding.
8. Expand to two and then four short slots only after review gates pass.
9. Consider capped 25% stair growth after the four-slot canary is stable.
10. Consider automated transfers last, with independent kill switches and
    daily limits.

No stage should silently promote itself to the next one.

## Operator Decision: Full Four-Slot Validation

On 2026-07-29 the dedicated `$1000` Bybit Pump subaccount passed the read-only
gate with no positions or open orders. The operator then explicitly selected
the complete fixed strategy for immediate live error discovery:

- four concurrent `main_pullback_tier` short slots;
- `$175` isolated margin per slot, `$700` deployable total;
- `$300` local reserve with the existing capped `$25/$50` margin additions;
- all long, slow-pump, and alternative tracks remain paper/shadow only;
- no automatic transfer from the main account.

Pump-specific risk events use the same configured primary/fallback
notification route as the main FeeArb monitor. Notification delivery is
failure-isolated from order, margin, reconciliation, and emergency-close
actions, and every delivery result is written to the Pump live JSONL audit.

The live risk layer was hardened before the first position: Bybit now receives
a full-position Mark Price TP plus a catastrophic SL kept `2.5%` inside the
current short liquidation price. A margin top-up is immediately verified from
a fresh exchange position; the critical `10%` buffer bypasses the ordinary
five-minute top-up cooldown and closes if the verified buffer remains
critical. Pump can return only its own tracked top-up margin, after two
consecutive `>=35%` buffer scans and a 30-minute adjustment cooldown, in `$25`
chunks. Any removal that leaves less than `30%` is rolled back immediately.
