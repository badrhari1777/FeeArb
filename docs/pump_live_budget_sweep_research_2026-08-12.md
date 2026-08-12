# Pump Live budget sweep: capped $3000 working pool

Date: 2026-08-12. Research only. Live sizing, ARM state, positions, orders,
margin, and transfers were not changed.

## Capital model

The tested `$600/$650/.../$1200` value is the **complete maximum margin budget
of one symbol**, not every ladder leg. Current rules split it as:

- 5 equal legs: `$600 -> 5 x $120`, `$1200 -> 5 x $240`;
- 3 tapered legs `1:2:3`: `$600 -> $100/$200/$300`;
- 2 tapered legs `1:2`: `$600 -> $200/$400`.

The new primary model implements the requested accounting:

1. Pump starts with `$3000` working capital.
2. Working capital can never grow above `$3000`.
3. A loss reduces working capital below `$3000`.
4. Later profit first refills working capital to `$3000`.
5. Profit above `$3000` is immediately counted as withdrawn to another account.
6. Main-account liquidity is unlimited in the experiment, never blocks a
   strategy action, and is tracked as temporary principal until repaid.
7. Borrowed principal is never counted as PnL.

The final working capital returns to `$3000` in every tested level; total
withdrawn profit therefore equals net strategy PnL. Unlike the previous report,
closed profits do not finance later margin needs.

## Historical coverage

- Research boundary: `2024-01-01 00:00 UTC`.
- Unique reconstructed cases after it: `126`.
- Exact-current `main_pullback_tier` signals: `40`.
- Actual candidate interval: `2025-09-08 18:00 UTC` through
  `2026-06-18 02:00 UTC`.
- Four-slot/same-symbol replay: `35` executed trades, `2` slot skips and `3`
  duplicate-symbol skips.
- Chronological holdout: 13 trades from `2026-05-03 23:00 UTC`.
- Reconstructed ladder counts: `35/35` match the source outcomes.

The archive starts in 2024, but this exact strategy has no eligible candidate
before September 2025. The effective sample is 35 trades, not two full years of
continuous qualifying entries.

## On-demand actions

Every trade is reconstructed from archived `1h` candles with:

- only the actually filled L1/L2/... capital allocated;
- nearest-ladder activation at `35%` remaining distance;
- current v4 `12%/12%` pre/post-fill protection;
- `20% -> 25%` liquidation-buffer restore;
- ladder release after cooling to `45%`;
- conservative one `$75` margin-release chunk per archived hour;
- full margin release and fees/funding-aware PnL at exit.

Hourly data assumes ideal controller response inside a candle. It is useful for
capital sizing and loan cash flow, but cannot prove survival of an intra-minute
gap or real transfer latency.

## Main results

| Symbol budget | Net PnL withdrawn | DD | Peak loan | Hours in debt | Loan episodes | Longest episode | Peak one-position top-up | Largest 1h add |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| $600 | $5,944 | 3.60% | $1,120 | 42h | 2 | 24h | $3,445 | $1,305 |
| $650 | $6,439 | 3.90% | $1,460 | 50h | 1 | 50h | $3,735 | $1,415 |
| $700 | $6,934 | 4.19% | $1,795 | 55h | 2 | 54h | $4,020 | $1,520 |
| $750 | $7,430 | 4.49% | $2,135 | 56h | 1 | 56h | $4,310 | $1,630 |
| $800 | $7,925 | 4.79% | $2,470 | 56h | 1 | 56h | $4,595 | $1,740 |
| $850 | $8,420 | 5.09% | $2,805 | 56h | 1 | 56h | $4,880 | $1,850 |
| $900 | $8,916 | 5.39% | $3,145 | 56h | 1 | 56h | $5,170 | $1,955 |
| $950 | $9,411 | 5.69% | $3,480 | 56h | 1 | 56h | $5,455 | $2,070 |
| $1,000 | $9,906 | 5.99% | $3,820 | 57h | 2 | 56h | $5,745 | $2,180 |
| $1,050 | $10,402 | 6.29% | $4,155 | 59h | 2 | 56h | $6,030 | $2,285 |
| $1,100 | $10,897 | 6.59% | $4,490 | 63h | 2 | 58h | $6,315 | $2,395 |
| $1,150 | $11,392 | 6.89% | $4,835 | 65h | 2 | 58h | $6,605 | $2,505 |
| $1,200 | $11,888 | 7.19% | $5,180 | 67h | 2 | 58h | $6,890 | $2,610 |

At a 10% annual financing sensitivity, modeled borrowing cost is only `$0.26`
at `$600`, `$0.62` at `$700`, `$0.83` at `$750`, `$1.02` at `$800`, and `$2.63`
at `$1200`. The economic issue is transfer capacity and reaction speed, not
interest over these short episodes.

## Did four simultaneous coins cause borrowing?

No. The maximum observed four-position state remained well inside the fixed
`$3000` working pool:

| Symbol budget | Peak committed with 4 coins | Peak loan with 4 coins | Minimum free with 4 coins |
|---:|---:|---:|---:|
| $600 | $1,055 | $0 | $1,945 |
| $800 | $1,382 | $0 | $1,618 |
| $1,000 | $1,703 | $0 | $1,297 |
| $1,200 | $2,030 | $0 | $970 |

This validates the on-demand pool concept: four open symbols do not imply four
fully consumed symbol budgets. Most had only L1 or limited top-up allocated.
The binding risk is actual ladder/adverse movement, not the position counter.

## When borrowing occurred

At `$600`:

1. `2026-06-14 05:00 -> 23:00 UTC`, 18 hours, peak `$1005`. It started with
   HUSDT + COAI open when HUSDT required `warning_restore`. Gradual `$75`
   releases and COAI exit repaid it.
2. `2026-06-15 03:00 -> 2026-06-16 03:00 UTC`, 24 hours, peak `$1120`.
   HUSDT alone required another restore; later margin releases fully repaid it.

At `$650-$900`, the same June tail becomes one mostly continuous 50-56 hour
episode with peak debt growing from `$1460` to `$3145`.

At `$1000+`, a second earlier episode appears on 2025-11-14 because SOON and
HUSDT overlap. At `$1200` it lasts 9 hours and peaks at `$670`; the later June
episode lasts 58 hours and peaks at `$5180`.

The generated `loan_events.csv` records every individual draw and repayment.
For example, the first `$600` draw is `$760` after HUSDT warning restore; later
adds increase debt, and each safe `$75` margin release reduces it. The HTML
report groups these movements into human-readable loan episodes per budget.

## Decision under current live limits

Current comparison limits are approximately:

- shared rescue facility: `$2000`;
- maximum top-up for one position: `$5000`.

Therefore:

- `$600`: current live size; historical peak loan `$1120`, comfortably inside
  the rescue facility.
- `$650`: peak loan `$1460`, still comfortable.
- `$700`: **balanced recommendation under unchanged limits**. Peak loan `$1795`
  and peak position top-up `$4020` fit both boundaries.
- `$750`: peak loan `$2135`, already `$135` above the current rescue facility.
  It needs an explicit facility increase before promotion.
- `$800-$850`: both require materially more than `$2000` rescue capacity.
- `$900+`: also exceeds the current `$5000` one-position top-up cap in the
  HUSDT replay.
- `$950+`: its largest reconstructed one-hour add additionally exceeds `$2000`.

Raw PnL always prefers `$1200` because selected percentage outcomes scale
linearly. That is not a risk-adjusted optimum. With profits swept and current
limits unchanged, `$700` replaces the prior `$800` recommendation. A cautious
live progression would be `$600 -> $650 -> $700`, only for new positions and
with an observation gate between promotions. Existing immutable risk snapshots
must not be resized.

## Reproduction and artifacts

```powershell
.venv\Scripts\python.exe scripts\pump_live_budget_sweep_research.py
```

Ignored generated artifacts under
`data/research/pump_live_budget_sweep_research/`:

- `report.html`: separate working-capital, withdrawn-profit, debt, committed
  capital and concurrency charts plus loan-episode table for every budget;
- `budget_summary.csv`: cross-budget comparison;
- `trade_details.csv`: per-trade ladder and top-up reconstruction;
- `portfolio_timeline.csv`: chart source timeline;
- `loan_events.csv`: every borrow/repay movement and its active symbols/cause;
- `loan_episodes.csv`: grouped start/end/duration/peak loan episodes;
- `metadata.json`: exact inputs, parameters, coverage and limitations.
