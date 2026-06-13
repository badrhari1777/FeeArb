# Emergency De-Risk History

Purpose
- Keep a durable implementation history and rollout roadmap for the `Emergency De-Risk` contour.
- `AGENTS.md` should keep only a short pointer and recent summary; this file holds the deeper timeline.

Relationship To `AGENTS.md`
- After each meaningful `Emergency De-Risk` change:
  - add a short summary line to `AGENTS.md`;
  - append or update the detailed state here.
- If there is any conflict, this file is the detailed source for this subsystem and `AGENTS.md` is the compact session memory.

Current Scope
- Subsystem: `Emergency De-Risk`
- Main files:
  - `risk/derisk_manager.py`
  - `webapp/services.py`
  - `execution/manual.py`
  - `webapp/templates/index.html`
  - `webapp/static/app.js`
  - `tests/test_derisk_manager.py`
  - `tests/test_project_settings.py`
  - `tests/test_manual_trader.py`

Goals
- Prevent forced stop-outs / margin-call cascades when one venue runs out of safe free balance.
- Detect broken hedge states without reacting to stale or unreliable exchange data.
- Prefer reducing risk by the smallest necessary amount instead of flattening everything.
- Keep enough diagnostics and historical telemetry to analyze live behavior later and retune coefficients from real conditions.

Implemented Phases

Phase 1: Foundation
- Added separate `Emergency De-Risk` contour instead of overloading spread auto-exit.
- Added hedge-cluster registry with `hedged_pair` and `standalone`.
- Added exchange-health gating from account/balance status snapshots.
- Added UI panels for:
  - exchange health
  - hedge clusters
  - de-risk diagnostics
  - de-risk event log

Phase 2: Shadow Analytics
- Added cluster integrity evaluation:
  - `healthy`
  - `suspected_orphan`
  - `confirmed_orphan`
  - `blocked_by_exchange_health`
- Added stress evaluation from venue balances and margin buffer.
- Added candidate scoring for intact hedged pairs.
- Added dust-aware planner that may escalate partial action to `full_cleanup` when partial leave-behind looks non-closeable.

Phase 3: Live Intact Pair De-Risk
- Live mode can trigger partial de-risk for an intact `hedged_pair`.
- Live mode can preempt a running auto-exit when emergency priority is higher.
- Preemption now requests `force_finalize` so in-flight executions do not stop mid-imbalance.

Phase 4: Live Orphan Cleanup
- Added `ManualTradeManager.orphan_cleanup(...)`.
- Added panic orphan cleanup path for single-leg cleanup on one venue.
- Added forced final cleanup after orphan cleanup if residual remains visible.
- Added forced final reconciliation in fast/smart enter/exit paths when stop/preempt arrives with `force_finalize=true`.

Phase 5: Ambiguity Protection
- Duplicate visible legs on the same expected exchange/side are now aggregated before orphan sizing and mismatch detection.
- Added `blocked_by_cluster_conflict` when:
  - a symbol has unexpected extra visible legs outside the declared pair;
  - multiple hedge clusters overlap on the same expected leg.
- Diagnostics now expose:
  - `unexpected_legs`
  - `unexpected_leg_count`
  - `duplicate_visible_leg_count`
  - `overlap_conflicts`

Phase 6: Persistent History
- Added persistent `JSONL` history at `logs/derisk_history.jsonl`.
- Two record families are written:
  - `event`: important transitions and trigger/preempt actions
  - `cycle`: compact cycle snapshot for later replay/analysis

Phase 7: Outcome Attribution
- Added persistent pending-outcome tracking in `state/derisk_outcome_state.json`.
- Live `de-risk` triggers now register follow-up evaluation horizons:
  - `1m`
  - `5m`
  - `15m`
  - `to_next_funding` when the initial decision had a valid future funding boundary
- Matured follow-ups are appended into `logs/derisk_history.jsonl` as `outcome` rows with:
  - the original baseline state
  - the observed current state at the follow-up horizon
  - heuristic label `improved` / `unchanged` / `worsened`
  - short machine-readable reasons

Current Persistent `JSONL` Model

Record Type: `event`
- Written when `_derisk_event(...)` fires.
- Typical events:
  - `cluster_status`
  - `preempt_requested`
  - `trigger`
  - `orphan_trigger`
- Main fields:
  - `record_type`
  - `ts`
  - `cycle_id`
  - `event`
  - event payload fields such as `symbol`, `reason`, `stress_exchange`, `orphan_qty`, `result`

Record Type: `cycle`
- Written at the end of every de-risk cycle.
- Main fields:
  - `record_type`
  - `ts`
  - `cycle_id`
  - compact `settings`
  - `status_counts`
  - `exchange_health`
  - `balances`
  - `running_execution`
  - `cycle_action`
  - `rows`

Record Type: `outcome`
- Written when a registered follow-up horizon matures for a prior live `trigger` or `orphan_trigger`.
- Main fields:
  - `record_type`
  - `ts`
  - `cycle_id`
  - `source_action_type`
  - `symbol`
  - `key`
  - `horizon`
  - `target_ts`
  - `age_sec`
  - `heuristic_outcome`
  - `initial`
  - `current`

Why `cycle` rows matter
- They preserve the state of the environment even when no live trigger fired.
- This allows later analysis of:
  - what would likely have been chosen;
  - what was blocked and why;
  - how stress, orphan, funding, and conflict signals evolved over time.

What The Current History Is Good For
- Checking whether a trigger was caused by:
  - real venue stress;
  - confirmed orphan;
  - stale/auth conflict block;
  - cluster ambiguity.
- Comparing:
  - `stress_status`
  - `candidate_score`
  - `action_qty`
  - `funding_to_next_usd`
  - `minutes_to_event`
  - buffer deterioration
- Reviewing whether the system was consistently too aggressive or too conservative in live conditions.
- Comparing a trigger-time baseline against later `1m/5m/15m/to_next_funding` follow-ups to see whether:
  - stress really eased;
  - orphan state resolved;
  - buffer recovered or deteriorated further;
  - the system should likely become more aggressive or more conservative for similar setups.

Known Limits
- No dedicated long-term DB yet; history is append-only `JSONL`.
- Outcome attribution is heuristic and state-based for now; it is not yet a realized-PnL evaluator.
- No multi-leg `3+ venue` executor yet.
- No automatic coefficient fitting yet; the current history is the data source for later tuning, not the tuner itself.

Next Recommended Phases

P1: Re-Hedge Research Mode
- Shadow-only first.
- Detect whether re-hedging would have been better than pure de-risk under:
  - enough free balance
  - strongly favorable spread
  - acceptable funding penalty

P2: Multi-Leg Cluster Model
- Expand registry beyond a simple long/short pair.
- Needed before any safe live `3+ venue` de-risk execution.

P3: SQLite / Review UI
- Keep `JSONL` as the raw append log.
- Add importer/index for:
  - filtering by symbol / event type / venue
  - plotting stress score vs action choice
  - comparing blocked vs triggered cases
  - comparing trigger baselines with matured `outcome` follow-ups

Operational Notes
- `JSONL` is intended to be durable across backend restarts.
- `AGENTS.md` should remain concise; do not copy the entire phase history there.
