# Persisted state and replay contract

## Purpose

This contract is the safety boundary before extracting the Grid and Pump Live
state machines from their current owners. It describes what survives a backend
restart, what must always reset, and which facts must be re-read from exchanges.

The JSON snapshot is a recovery checkpoint, not authority to submit an order.
The exchange remains authoritative for positions, open orders, fills, protection,
margin and final close accounting. JSONL history is audit evidence; it is not a
complete event-sourced ledger and must not recreate orders by itself.

Contract implementation: `execution/state_replay_contract.py`.
Read-only audit: `scripts/state_replay_audit.py`.

## Shared invariants

1. State has an explicit module schema and a deterministic durable fingerprint.
2. Ownership keys are unique inside the module.
3. A restart never replays an order submission from persisted intent alone.
4. Durable facts must survive load/save/restart without mutation.
5. Volatile authorization and transient counters reset fail-closed.
6. Missing in-memory execution state is reconciled against fresh exchange
   positions/orders before the state machine can continue.
7. Unknown, corrupt or overlapping ownership is an error, never an implicit flat
   or successful transition.

A syntactically unreadable Grid snapshot is retained byte-for-byte and blocks
subsequent state saves instead of being silently replaced by an empty rules map.
Logical ownership conflicts remain persistable so the existing Grid cycle can
record and disable the conflicting legacy rule; the contract stays invalid until
that recovery completes.

## Pump Live contract

Schema: `pump_live_state_v1`.

Durable facts include positions with their `live_id`, strategy/account ownership,
ladder legs, complete risk-policy snapshot, seen event keys, capital manager,
portfolio freeze, emergency request and tracked temporary capital.

Cold-start resets:

- `entry_armed=false`;
- pending signals are discarded;
- transient/close/risk recovery counters are reset;
- open persisted positions force `recovery_monitoring`;
- every open position must be matched to the Bybit Pump subaccount and have its
  exchange stop/TP and ladder ownership verified before ARM can resume entries.

Protection absent from the snapshot is a reconciliation warning rather than proof
that the exchange is unprotected. Resume remains fail-closed until exchange proof.

## Grid contract

Schema: `auto_arb_grid_state_v1`, payload version `1`. The loader accepts the old
schema-less version once and the next save writes the explicit schema.

The complete rule is durable, including generation, adopted/live level, actual
hedged quantity, pending partial transition and active execution reference. Manual
execution workers are process memory and are intentionally not reconstructed.

If `active_execution_id` survives a restart, the only allowed next action is
`reconcile_execution_from_exchange`. Fresh long/short quantities decide whether
the transition completed, remains partial, is balanced, or needs hedge repair.
The persisted execution id alone cannot mark success and cannot resubmit an order.

Two enabled/live rules may not own the same base symbol on any shared exchange.
Paused rules with an unfinished execution retain recovery ownership until exchange
reconciliation finishes.

## Replay levels

- Snapshot replay: canonicalize durable fields and compare SHA-256 fingerprints.
- Restart projection: apply the documented cold-start resets and verify idempotency.
- Exchange reconciliation replay: inject recorded exchange positions/orders into
  the existing recovery path; assert the same terminal/partial/repair decision.
- JSONL audit: validate parseability and correlation identifiers. Do not derive
  balances, fills or order authority solely from history rows.

Before moving either state machine, tests must cover valid restart, repeated
restart, corrupt/unknown state, duplicate ownership, missing in-memory execution,
partial execution and protection uncertainty.

Grid reducer golden cases live in
`tests/fixtures/grid_transition_confirmation_v1.json`. They freeze the output for
idle, partial-frontier wait, confirmed Live queue and entry-risk cooldown states.

Partial transition replay is frozen separately in
`tests/fixtures/grid_partial_transition_v1.json`. Its cases cover frontier
continuation, a partially filled exit reversing back to enter, clearing an
unfilled stale exit, cancelling an unfilled reversal back to the original exit,
and rolling a partially filled enter back from fresh exchange quantity. The
deterministic reducer is `reduce_partial_grid_transition`; it may update only the
provided rule state and returns a decision/event. Exchange refresh, order
submission and execution reconciliation remain outside it in `DataService`.

New and resumed transition quantities are normalized by
`build_grid_pending_transition` and frozen in
`tests/fixtures/grid_pending_transition_v1.json`. The builder distinguishes an
exact continuation from a different transition, derives missing origin quantity,
preserves material remainder, and consumes `rebase_from_positions` only against
fresh hedged quantity. It does not validate exchange risk, place an order or
interpret an execution result.

Finished transition workers are reduced by
`reduce_grid_transition_execution` after a fresh two-leg quantity snapshot.
`tests/fixtures/grid_execution_result_v1.json` freezes completion, material
partial fill, non-closeable dust, balance block, generic execution retry and leg
imbalance outcomes. A persisted worker result never supplies position quantity:
fill progress and repair need are derived from fresh hedged/imbalance quantities.
The reducer returns a repair request as data; `DataService` remains the only
owner of the asynchronous reduce-only hedge-repair action.
