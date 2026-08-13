# Retired Auto Exit and de-risk lessons

Status: design memory only. The legacy Auto Exit and autonomous position
reduction code was removed from production on 2026-08-13. This document is not
an operator guide and does not authorize live execution.

## What must be preserved in a future Strategy Lab replacement

1. Ownership must be explicit and versioned. Use strategy id, generation,
   symbol, venues, side and position signature. A stale persisted rule must
   never block Grid or act on a position it no longer owns.
2. Decisions require fresh private position evidence from every affected
   venue. Missing, stale or failed scans are unknown, never proof of flatness.
3. Multi-leg portfolios are ambiguous. A reducer must model all legs and reject
   overlapping ownership; selecting an arbitrary pair can create an orphan.
4. Partial execution needs a fixed original target plus measured remaining
   quantity. Reversals and retries must rebase from exchange positions without
   silently increasing the target.
5. Dust, minimum quantity and contract-size constraints are execution facts,
   not strategy success. Material residuals remain failures; non-closeable dust
   must be reported separately.
6. Emergency priority cannot mean blind cancellation or market execution.
   Existing fills must first be reconciled, and any preemption must retain an
   auditable reason and final position proof.
7. Funding/spread triggers are hypotheses. They require chronological OOS,
   unseen-symbol holdout, paper and shadow evidence before operator-approved
   live use.

## Active safety boundary after removal

- exchange-native stop/take protection remains active;
- isolated-margin monitoring and add/release remain active;
- verified cleanup of protective orders with no position remains active;
- explicit manual exit and Grid hedge repair retain the generic reduce-only
  execution primitives;
- no autonomous service chooses a position or percentage to reduce.

Historical state and event files are retained outside Git for forensic use.
They must not be re-imported as active rules.
