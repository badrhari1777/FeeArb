# Pump Live budget sweep: $600-$1200

Date: 2026-08-12. This is research only. It did not change the live `$600`
budget, ARM state, orders, positions, or transfer limits.

## Question and exact meaning of the budget

The tested `$600/$650/.../$1200` value is the **complete maximum margin budget
of one symbol**, not the amount of every ladder leg. Current rules split it as:

- 5 equal legs: `$600 -> 5 x $120`, `$1200 -> 5 x $240`;
- 3 tapered legs `1:2:3`: `$600 -> $100/$200/$300`;
- 2 tapered legs `1:2`: `$600 -> $200/$400`.

The portfolio starts with `$3000` Pump-owned capital, permits four concurrent
symbols, keeps positions isolated, and allocates wallet cash as one shared
pool. Main-account liquidity is unlimited in the experiment and never blocks
an entry, fill, or safety top-up. Borrowed principal is not counted as profit.

## Data coverage

- Declared research boundary: `2024-01-01 00:00 UTC`.
- Unique reconstructed event cases after that boundary: `126`.
- Signals passing the exact current `main_pullback_tier` gates: `40`.
- Actual candidate interval: `2025-09-08 18:00 UTC` through
  `2026-06-18 02:00 UTC`.
- Four-position/same-symbol ownership constraints execute `35` trades, skip
  `2` because all four slots are occupied, and skip `3` duplicate-symbol
  overlaps.
- All 35 reconstructed ladder counts match the source outcomes.
- Chronological holdout begins `2026-05-03 23:00 UTC`; it contains 13 trades.

The user's recollection was therefore correct: files are filtered from 2024,
but the **current complete strategy** has no eligible entry before September
2025. This is not a two-year sample of 35 trades.

## What is replayed

For every selected trade and every budget level the replay starts only L1,
then reconstructs from archived `1h` candles:

1. nearest-ladder activation at a remaining distance of `35%`;
2. the current v4 `12%/12%` pre/post-fill protection margin;
3. the first candle that fills each next 50% ladder step;
4. a top-up from a `20%` liquidation-buffer warning to a `25%` target;
5. ladder deactivation after cooling to `45%` distance;
6. safe margin release toward a `25%` buffer, conservatively limited to one
   `$75` chunk per archived hour;
7. release of all isolated margin and realization of fees/funding-aware PnL at
   the historical strategy exit.

This corrects the old shared-margin report's deliberately conservative
assumption that the full symbol budget plus peak top-up remains locked for the
entire trade.

## Results

| Symbol budget | PnL | ROI on initial $3000 | Realized DD | Peak committed | Actual peak loan | Minimum own free | Largest 1h add | Loan if profits were swept |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| $600 | $5,944 | 198.1% | 3.60% | $4,120 | $0 | $1,705 | $1,305 | $1,120 |
| $650 | $6,439 | 214.6% | 3.90% | $4,460 | $0 | $1,606 | $1,415 | $1,460 |
| $700 | $6,934 | 231.1% | 4.19% | $4,795 | $0 | $1,507 | $1,520 | $1,795 |
| $750 | $7,430 | 247.7% | 4.49% | $5,135 | $0 | $1,403 | $1,630 | $2,135 |
| $800 | $7,925 | 264.2% | 4.79% | $5,470 | $0 | $1,304 | $1,740 | $2,470 |
| $850 | $8,420 | 280.7% | 5.09% | $5,805 | $0 | $1,201 | $1,850 | $2,805 |
| $900 | $8,916 | 297.2% | 5.39% | $6,145 | $0 | $1,097 | $1,955 | $3,145 |
| $950 | $9,411 | 313.7% | 5.69% | $6,480 | $0 | $988 | $2,070 | $3,480 |
| $1,000 | $9,906 | 330.2% | 5.99% | $6,820 | $0 | $899 | $2,180 | $3,820 |
| $1,050 | $10,402 | 346.7% | 6.29% | $7,155 | $0 | $795 | $2,285 | $4,155 |
| $1,100 | $10,897 | 363.2% | 6.59% | $7,490 | $0 | $672 | $2,395 | $4,490 |
| $1,150 | $11,392 | 379.7% | 6.89% | $7,835 | $0 | $598 | $2,505 | $4,835 |
| $1,200 | $11,888 | 396.3% | 7.19% | $8,180 | $0 | $519 | $2,610 | $5,180 |

All 13 chronological holdout trades remain profitable as a group. Holdout PnL
scales from `$2,163` at `$600` to `$4,326` at `$1200`. The complete sample has
34 positive outcomes and one negative outcome. That unusually high historical
win rate is evidence of a strong selected sample, not a promise of the same
future loss rate.

## Why actual borrowing is zero

This is not a calculation bug. The early positions use only their filled legs,
not their complete budgets. By the time the extreme HUSDT margin demand occurs
on 2026-06-14/15, the preceding closed trades have already increased Pump-owned
equity. At `$1200`, the minimum starting capital that would reproduce this
exact chronology without a loan is only `$2480.92`; the experiment starts with
`$3000`.

The result is highly **sequence-dependent**. HUSDT alone reaches approximately:

- `$3445` top-up at a `$600` symbol budget;
- `$4310` at `$750`;
- `$4595` at `$800`;
- `$5170` at `$900`;
- `$6890` at `$1200`.

If realized profits had been swept out instead of retained, peak borrowing
would range from `$1120` at `$600` to `$5180` at `$1200`. Equivalently, a future
HUSDT-like tail occurring early can require a loan even though the observed
chronology did not. This counterfactual is more useful for rescue-cap planning
than the literal zero actual-loan result.

## Decision

Raw historical PnL necessarily chooses `$1200`, because all selected trades and
their percentage outcomes are identical and PnL scales linearly with budget.
That is not a defensible live optimum. The replay has only 35 trades, hourly
ordering, current-listing survivor bias, and ideal in-candle controller/transfer
response. It cannot prove that `$2610` can be added before a fast spike, nor
that the unusually low historical loss count will persist.

The practical frontier is:

- `$750`: conservative next live candidate. It still satisfies the old
  `4 x budget <= $3000` identity, improves historical PnL by 25% versus `$600`,
  and its largest reconstructed one-hour add is `$1630`.
- `$800`: **balanced research recommendation**. It improves historical PnL by
  33.3%, keeps realized DD below 5%, retains at least `$1304` own free cash in
  the observed sequence, and its largest one-hour add (`$1740`) remains below
  the current `$2000` rescue-facility scale.
- `$850`: hard upper candidate under the complete current live caps. Its HUSDT
  top-up is about `$4880` versus the `$5000` per-position cap, and its largest
  one-hour add is `$1850` versus the `$2000` rescue facility. The remaining
  headroom is too small to call it the balanced choice.
- `$900`: research-only boundary. Its largest one-hour add (`$1955`) is still
  just below `$2000`, but the HUSDT top-up reaches `$5170` and therefore exceeds
  the current `$5000` per-position cap. It is not deployable unchanged.
- `$950-$1200`: reject for direct promotion. Their largest one-hour margin adds
  exceed `$2000`; the result relies increasingly on retained prior profits and
  ideal reaction. `$1100` has the best historical PnL/peak-committed ratio, but
  the difference is economically tiny and not robust enough to justify the
  tail exposure.

No live budget should be changed directly from this report. A safe progression
is `$600 -> $750` (or at most `$800`) for new positions only, followed by a
fresh live observation gate. Existing positions must keep their immutable risk
snapshots.

## Reproduction and artifacts

Run:

```powershell
.venv\Scripts\python.exe scripts\pump_live_budget_sweep_research.py
```

Generated, ignored artifacts:

- `data/research/pump_live_budget_sweep_research/report.html` - a separate
  equity, borrowed-capital, committed-capital, and concurrency chart for every
  budget level;
- `budget_summary.csv` - comparison table;
- `trade_details.csv` - per-trade ladder/margin diagnostics;
- `portfolio_timeline.csv` - time series behind every chart;
- `metadata.json` - exact inputs, parameters, coverage, and limitations.
