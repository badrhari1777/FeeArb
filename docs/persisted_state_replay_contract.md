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

When `active_execution_id` survives but its process-local worker does not,
`reduce_missing_grid_execution` accepts only the persisted rule plus fresh
exchange quantities. Balanced quantities clear the stale worker reference and
resume partial/monitoring state; a material leg imbalance requests hedge repair;
a failed refresh preserves the active reference in `waiting_reconcile`.
`tests/fixtures/grid_missing_execution_v1.json` freezes these restart outcomes,
including a flat repair that clears stale level/transition state.

The result of a reduce-only hedge-repair worker is reduced by
`reduce_grid_hedge_repair_execution`. Fresh imbalance, not the worker status,
decides whether repair succeeded. Balanced legs resume partial/monitoring state
or reset a flat Grid; a remaining imbalance either requests another cleanup or
retains an explicit execution error for a later retry. Golden cases live in
`tests/fixtures/grid_hedge_repair_result_v1.json`; the reducer never submits the
next cleanup itself.

`execution/grid_state_machine.py::GridStateMachine` now composes the pure quote
cycle reducers behind one deterministic facade. It accepts a rule, two already
observed spreads and a clock; it returns only a decision event and an optional
Live transition intent. `tests/fixtures/grid_quote_cycle_v1.json` and the legacy
reference sequence prove parity for missing data, shadow apply, Live queue,
pending completion, frontier wait and partial reversal. Exchange reads, risk
preflight, history persistence and order execution remain `DataService` ports.

The facade also exposes a dormant `plan_transition_start` method. It converts a
fresh hedged quantity plus a queued level transition into an explicit I/O intent:
submit quantity, persisted transition, position target, entry risk target and
completion tolerance. `tests/fixtures/grid_transition_start_intent_v1.json`
freezes new enter/exit, material continuation, fresh-position rebase and
already-complete outcomes. The initial parity-only checkpoint intentionally did
not wire production start execution.

The production Grid start path now consumes that same intent after its existing
fresh position refresh. Level-range validation still happens before refresh;
KuCoin risk-limit checks consume the intent's explicit entry target; completion
still short-circuits before order submission. Manual execution, exchange reads,
state locking and persistence remain in `DataService`.

`GridStateMachine.plan_execution_reconcile_io` now defines the next dormant
reconcile boundary. Given persisted active-execution fields and the process-local
Manual run snapshot, it chooses whether a fresh position refresh is required and
routes the future result to `missing_execution`, `transition_execution`,
`hedge_repair_execution`, or `settle_without_transition`. It performs no I/O and
does not mutate either input. Six cases in
`tests/fixtures/grid_execution_reconcile_io_v1.json` freeze restart-missing,
running and terminal routing. The initial parity-only checkpoint did not wire
production reconcile; exchange refresh, persistence and repair launch stayed
unchanged.

The production reconcile path now consumes the planner for its top-level
missing/running/terminal routing and repeats terminal reducer selection from the
fresh in-lock rule. The planner does not receive exchange quantities and does not
apply reducer output. Fresh position reads, active-field cleanup, reducers,
state lock/save, history append and repair launch all remain in `DataService`.

`GridStateMachine.reduce_execution_reconcile_after_refresh` defines a dormant
pure boundary after the caller has attempted the fresh two-leg position read. It
clears or restores active execution fields, records the observed execution,
routes to the existing transition/repair reducers, applies paused-state parity,
and returns only the event fragment plus completion/repair intent. It performs no
exchange read, persistence, history append, or repair launch. Seven trajectories
in `tests/fixtures/grid_execution_reconcile_after_refresh_v1.json` compare the
entire mutated rule and result with the preserved production sequence, including
position-refresh failure. The initial parity-only checkpoint did not wire
production to this facade.

Production reconcile now delegates only its in-lock post-refresh reduction to
that facade. `DataService` still owns the Manual run lookup, exchange position
refresh, event envelope, lock, durable save, history append, and any later hedge
repair launch. This keeps I/O ordering and failure boundaries outside the pure
state machine.

`GridStateMachine.plan_hedge_repair_start` now freezes the next dormant I/O
boundary. From a fresh two-leg quantity snapshot it distinguishes an imbalance
already within tolerance, the need for a Manual rebalance analysis, an
exchange-minimum non-closeable remainder, and a submit-ready cleanup. It returns
the exact analysis request or existing reduce-only cleanup payload without
mutating rule, quantities, or preflight. Five golden cases cover long and short
surpluses in `tests/fixtures/grid_hedge_repair_start_intent_v1.json`. Production
repair start was not wired in the initial parity-only checkpoint.

Production hedge-repair start now consumes the planner once before Manual
analysis and again with its preflight result. The existing in-lock settle/dust
mutations, `analyze_rebalance`, `manual_orphan_cleanup`, state save, history, and
worker ownership remain in `DataService`; the state machine only supplies routing
and exact I/O payloads.

`GridStateMachine.reduce_hedge_repair_settle` now freezes the dormant no-order
state boundary for `settle_within_tolerance` and
`settle_non_closeable_dust`. It handles partial/completed transitions, worker
minimum dust, flat repair reset, active-field cleanup and retry timing, returning
only an event fragment. Six golden trajectories compare the full mutated rule and
result with the preserved production sequence. One fixture explicitly preserves
the safety behavior that a non-flat snapshot cannot become `live_level=0` merely
because a transition target was zero. The initial parity-only checkpoint did not
wire production to this reducer.

Production hedge-repair settle now delegates the two existing in-lock no-order
branches to this reducer. `DataService` still adds the rule/timestamp event
envelope, updates `updated_at`, saves state, appends history, and owns all Manual
analysis and cleanup submission. No exchange or worker I/O moved into the state
machine.

`GridStateMachine.reduce_hedge_repair_worker_start` now freezes the dormant state
boundary after `manual_orphan_cleanup` returns. A real execution ID acquires repair
ownership and records the fresh hedged start quantity; an explicit error or empty
result schedules the existing retry and busy fallback. The reducer returns the
history event fragment but performs no Manual call, save, or history append. Three
golden cases compare the full mutated rule and event with the preserved sequence.
The initial parity-only checkpoint did not wire production to this reducer.

Production now calls the reducer inside the existing lock only after the same
`manual_orphan_cleanup` call returns. `DataService` still owns submission,
`updated_at`, durable save, rule/timestamp event envelope and history append.

`GridStateMachine.reduce_transition_worker_start` now freezes the dormant state
boundary after `manual_enter` or `manual_exit` returns. A real execution ID owns
the pending transition and records its fresh start quantity; an explicit error or
empty result preserves the transition and schedules the existing conflict retry.
It also clears the transient `transition_starting` marker and returns the history
event fragment. Four golden cases compare the full rule, transition and event.
The initial parity-only checkpoint did not wire production to this reducer.

Production now delegates transition worker result state inside the same lock after
the unchanged `manual_enter`/`manual_exit` call. Submission, exception cleanup,
`updated_at`, durable save, rule/timestamp envelope and history stay in
`DataService`.

`GridStateMachine.reduce_transition_pre_submit_outcome` now freezes the dormant
no-order boundary before Manual submission: fail-closed position refresh,
already-complete transition, entry risk-limit rejection, and successful risk
preflight state clearing. It mutates only rule state and returns an optional risk
history fragment. Four golden cases preserve the exact cooldowns and fields.
The initial parity-only checkpoint did not wire production to this reducer;
exchange and risk checks remain I/O.

Production now delegates only the state response after those existing I/O calls.
`DataService` still owns `updated_at`, durable save, and the risk history envelope;
the optional event is also generated from a pure preview so the historical event
is retained if the rule disappears before the lock is reacquired.

`GridStateMachine.reduce_transition_admission` now freezes the final dormant
start-admission state group: matching Live rule conflict, global Manual worker
conflict, disabled recheck, single-submission reservation, and exception release.
It performs no conflict lookup or Manual I/O and returns only whether submission
was admitted. Five golden cases compare the complete mutated rule. Production is
not wired to this reducer in the initial parity-only checkpoint.

Production now delegates those five mutation points while `DataService` retains
conflict lookup, generation comparison, Manual submission, timestamps and durable
save. No admission decision or external call moved into the pure state machine.
