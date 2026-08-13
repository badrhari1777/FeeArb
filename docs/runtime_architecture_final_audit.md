# Final production-runtime architecture audit

Date: 2026-08-14

## Final boundary

`DataService` is now the compatibility facade and I/O composition root. It is
not a strategy owner. Active ownership is split as follows:

| Domain | Owner |
|---|---|
| application startup, shutdown, task restart | `webapp/service_lifecycle.py` |
| private balances and positions cache | `execution/accounts.py::AccountMonitor` |
| stop/take and orphan protection | `risk/stop_manager.py::ProtectiveOrderManager` |
| manual spread reads | `webapp/manual_spread_service.py` |
| funding-history reads | `webapp/funding_history.py` |
| main positions view | `webapp/main_positions_read_model.py` |
| Manual order execution | `execution/manual.py::ManualTradeManager` |
| Grid durable state and pure decisions | `execution/grid_state.py`, `execution/grid_state_machine.py` |
| Pump durable state and pure decisions | `execution/pump_state.py`, `execution/pump_state_machine.py` |
| Pump exchange effects | `execution/pump_live.py::PumpLiveController` |

Exchange-effect orchestration remains behind the facade because moving a
method without moving its locks, unknown-result handling and recovery contract
would only create a second unsafe owner. The state/replay migrations already
separate the deterministic decision layer from those effects.

## Recurring API budget and cache behavior

The dashboard runtime payload exposes cumulative read-only counters under
`service.api_load`.

- Account refresh is single-flight. One successful cycle makes at most one
  balance call and one positions logical request per enabled credentialed
  exchange. A failed positions request has one bounded retry. Concurrent
  compatible requests increment `coalesced_requests` and do not call a venue.
- A protective post-margin-action verification is deliberately not coalesced:
  it is safety evidence, not duplicate dashboard polling.
- Positions-market refresh reads the cached account snapshot, skips an
  unchanged/fresh universe, and makes one bulk public adapter call per exchange
  that currently has an open Main position. It does not scan all symbols.
- Funding History, Spread Monitor and Manual Tests are operator-invoked paths.
  Their cached adapters do not create recurring tasks.
- Pump uses its dedicated subaccount gateway and safety cadence. It must not
  share private account state, credentials or exchange-result locks with Main.
- Strategy Lab collectors remain isolated research tasks and are not started by
  the dashboard lifecycle.

This is the safe unified-cache boundary: common public adapters are reused,
private Main and Pump ownership stays separate, and load is measurable without
adding an exchange request.

## Retired and retained code

Removed production engines remain absent:

- `execution/auto_strategies.py`;
- `risk/derisk_manager.py`;
- retired Auto Exit, Auto Strategy, hedge-cluster and Coin Analysis routes.

Settings migration removes stale flags so an old JSON file cannot re-enable
them. A regression scans the production Python import graph for the two removed
engines.

Historical `pipeline/`, simulated execution/orchestrator tests, `analysis_*`
packages, `state/coin_analysis.db` and old research documents are retained.
They are reproducibility inputs for Strategy Lab, not production runtime. Git
history plus `docs/retired_auto_exit_derisk_lessons.md` preserves the removed
implementation lessons.

## Defect found by the final audit

The positions-market symbol collector had been converted to `@staticmethod`
but still accepted a leading `self` argument. Startup could therefore fail
before its first public positions-market refresh while the independently owned
Pump monitor remained healthy. The signature is corrected and an empty-book
refresh regression now proves that the path completes with zero API calls.

## Closed boundary

No further mechanical splitting of `DataService` is required for the current
production set. A future new strategy must arrive with its own state machine,
repository, gateway/effect controller and replay tests; it must not add another
decision loop to the facade.
