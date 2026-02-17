# Head Decision Engine Plan (v0 → v1)

## Goals
- Build a “head” (main loop) that decides *when* to trigger executions (enter/exit/roll) based on live spread and risk.
- Start small: manual UI control near positions to enable auto-exit at a target spread.
- Keep execution safe: avoid overlapping executions and prevent runaway loops.
- Make behavior explainable in logs and UI.

## Non-Goals (for v0)
- Fully autonomous entry logic (ranking candidates, funding forecasts, etc.).
- Parallel multi-leg executions across many symbols.
- Complex liquidation management (cross-portfolio optimization).

## Definitions
- **Head loop**: a periodic decision engine that evaluates positions + live market data and schedules execution jobs.
- **Execution job**: a request to `ManualTradeManager` (enter/exit/roll) with explicit parameters.
- **Live spread**: computed from live orderbook top-of-book or WAP (from WS when possible).

## Architectural Overview
1. **Data Sources**
   - Positions: existing positions snapshot (positions-market cache / REST).
   - Live orderbooks: `MarketDataBus` WS streams; REST fallback.
   - Risk signals: margin ratio / liquidation proximity / funding deterioration (future).

2. **Decision Engine**
   - Evaluates rules (e.g., auto-exit on spread).
   - Produces a small set of **execution intents**.

3. **Execution Coordinator**
   - Enforces concurrency limits and priorities.
   - Starts `ManualTradeManager` jobs or emergency market actions.

4. **State & Persistence**
   - Per-position auto-exit rules stored server-side (JSON in `state/`).
   - Active job registry with status, timestamps, and last update.

## Spread Calculation (Consistency)
- Use the same mid-spread formula as manual:
  - `primary_mid = (best_bid + best_ask) / 2`
  - `hedge_mid = (best_bid + best_ask) / 2`
  - `spread_pct = (hedge_mid - primary_mid) / primary_mid * 100`
- Always compute with **live WS orderbook** if available; fallback to REST if WS missing.
- If data source is stale (no WS updates within threshold), skip the trigger.

## Concurrency Strategy
### v0 Recommendation (Safe Default)
- **Single execution at a time** globally.
- Rationale: avoids double-hedge risk, simplifies tracking, reduces race conditions.

### v1+ (Optional)
- Allow concurrent executions **only if**:
  - They are on different symbols **and**
  - No shared exchange **or** shared exchange is low-risk (configurable)
- Enforce via:
  - Global semaphore (max_parallel = 1 by default)
  - Per-exchange lock (optional v1)
  - Per-symbol lock (required)

## Priority & Preemption
Define priorities (highest → lowest):
1. **Emergency risk reduction** (liquidation proximity)
2. **Auto-exit for existing positions**
3. **Auto-roll**
4. **Auto-enter**

Rules:
- If emergency triggers, **cancel queued jobs** and initiate market close.
- If a job is running, do not start another unless it is emergency and can safely preempt.

## v0 Feature: Auto-Exit by Target Spread
### UI (Positions table)
- Toggle: `Auto-exit`
- Input: `Target spread %` (e.g., -7.9)
- Optional: `Max runtime (sec)` and `Cooldown (sec)`

### Trigger semantics
- Example: entry spread -8.2, target -7.9
- If live spread crosses **>= -7.9** (improved spread), enqueue an **exit job**:
  - mode: `smart-exit`
  - qty: position size
  - spread_min_pct: target
  - max_runtime_sec: user-config or default
  - reduce_only: true

### v0 Decisions (Locked In)
- **Live-only trigger:** spread is evaluated from live WS orderbooks (no mark fallback).
- **2-leg only:** auto-exit applies only when there is exactly one long and one short exchange for the symbol.
- **Single execution at a time:** global lock, no parallel runs.
- If job finishes or times out, set cooldown to prevent immediate re-trigger.

### Why not rely on “positions spread”?
Positions snapshot uses cached/REST; can be stale.
Trigger uses live WS orderbooks to confirm real spread.

## Execution Coordinator (v0)
Data model (in-memory, persisted):
```json
{
  "rules": {
    "FLOW|binance|okx": {
      "auto_exit": true,
      "target_spread_pct": -7.9,
      "max_runtime_sec": 900,
      "cooldown_sec": 300
    }
  },
  "active_jobs": {
    "job_id": {
      "symbol": "FLOW",
      "type": "exit",
      "status": "running",
      "started_at": "...",
      "updated_at": "..."
    }
  }
}
```

Coordinator logic:
- On each loop:
  - Skip symbols with active jobs.
  - Check cooldown timestamps.
  - Evaluate triggers for remaining positions.
  - If triggered and no global lock, start job.

## Failure Modes & Guards
- **WS stale**: skip trigger, log warning.
- **Execution not found**: ensure job registry uses updated_at_ts (already fixed).
- **Loop churn**: add cooldown + minimum interval between triggers.
- **Partial fills**: manual execution already handles smart exit; coordinator only starts jobs.
- **Stale symbol mapping**: use normalized symbol comparison for positions vs orderbooks.

## Implementation Steps (Proposed)
### Step 1 — Settings Storage
- Add JSON storage in `state/auto_exit_rules.json`.
- Implement API endpoints:
  - `GET /api/auto-exit/rules`
  - `POST /api/auto-exit/rules` (upsert/remove)

### Step 2 — UI Controls (Positions panel)
- Add toggle + input fields next to each position.
- Save via API, display current rule state.

### Step 3 — Live Spread Service
- Reuse `MarketDataBus` orderbook sources.
- Add helper to fetch latest spread for a symbol/exchange pair.
- Define staleness thresholds and fallbacks.

### Step 4 — Head Loop (v0)
- New service task `HeadDecisionLoop`:
  - runs every N seconds
  - reads positions + rules
  - computes live spread
  - triggers exit jobs when threshold hit

### Step 5 — Execution Coordinator
- Global lock + per-symbol lock.
- Job registry; status updates from ManualTradeManager callbacks.

### Step 6 — Observability
- Log reason for trigger, spread at trigger, job id.
- Expose UI panel with active job list (optional v0).

## Future Enhancements
- Multi-stage exit: partial close, re-evaluate, close remainder.
- Risk-driven auto-reduction (margin pressure).
- Multiple simultaneous exits with per-exchange concurrency.
- “Auto-roll” decision rules.

## Open Questions
- Where to render positions controls: main UI or a dedicated “Positions Control” page?
- Should target spread be relative to entry spread or absolute?
- Emergency rules: should they execute immediately (market) or use smart-exit?
