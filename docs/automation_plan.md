# Automation Plan (Draft)

Context
- Goal: full automation from candidate discovery to enter/exit/roll.
- Current reality: deep analysis is constrained without a paid Coinglass API (candidate quality + funding data).

Guiding Principles
- Single execution at a time (prevents concurrency risk).
- Decision logic must be observable and auditable (clear logs, reasons).
- Prefer WS‑first data for spreads; keep REST as fallback.

End‑State Flow
1) Candidate discovery
2) Scoring/filters
3) Enter
4) Monitor/maintain
5) Exit or Roll
6) Post‑analysis + rules tuning

Decision Engine ("Head")
- Central loop that scans candidates + positions.
- Queue of intents (enter/exit/roll).
- Priority policy (example):
  1) Liquidation risk
  2) Margin shortage
  3) Exit‑loss
  4) Exit‑profit
  5) Roll
  6) Enter
- Strict single‑execution gate.

Auto‑Exit
- Rules per position: target spread, stop‑loss, max‑time, risk exit.
- Live spread (WS) for trigger and validation.
- Logs and execution detail per run.

Auto‑Enter
- Candidate sources: funding diff, basis, market conditions.
- Filters: liquidity, fees, exchange health.
- Entry intent: qty, legs, max slippage, chunking.
- Use existing smart‑enter logic.

Rolls
- Roll = exit + enter on improved pair.
- Options: soft roll (partial) vs hard roll (full).
- Only after exit intent is safe and liquid.

Execution Quality
- Precise error reasons (precheck failures, rejections).
- Per‑execution log files.
- WS health gating (avoid stale order streams).

UI / Ops
- Position table controls (auto‑exit rules).
- Agent log + per‑execution log link.
- Execution queue visibility (future).

Gaps / Dependencies
- Coinglass API for candidate quality.
- Funding/interest datasets for scoring.
- Better liquidity/fees model.

Phased Roadmap
Phase 1 (now)
- Auto‑Exit quality + logs (already implemented).
- Execution visibility and per‑run logs.

Phase 2 (when data available)
- Auto‑Exit multi‑rule set + priority.
- Auto‑Enter in "dry‑run first" mode.

Phase 3
- Auto‑Roll logic + safe transitions.
- Full decision engine with priorities.

Notes
- Until Coinglass API is available, full candidate automation is intentionally deferred.
