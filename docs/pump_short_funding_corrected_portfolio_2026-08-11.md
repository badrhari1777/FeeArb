# Pump Short funding-corrected replay (2026-08-11)

## Decision

The old HUSDT entry was not a valid strategy entry. The archived sample had a
blank funding feature because the collector paginated funding using a fixed
eight-hour interval. HUSDT settled hourly, so the API silently returned only
the newest part of the requested range. The historical gate and the current
live classifier both treat a missing funding value as non-blocking.

This change fixes the public research collector pagination and adds a separate
fail-closed replay. It does **not** change the live classifier, live settings,
orders, positions, or ARM state. Promotion of fail-closed missing-funding
behavior to live needs a separate operator-approved change and regression.

## HUSDT reconstruction

Exact episode: `HUSDT_w24_150_1781046000000`, trigger
`2026-06-09 23:00 UTC`, pump `156.059263%`.

The old missing-data path entered at `2026-06-10 03:00 UTC`, price `0.167170`.
The restored complete previous-24h funding was `-41.230291%`, well below the
current `>-1%` gate. A forced current `$600` three-leg replay would have:

- filled `$100/$200/$300` margin (`$300/$600/$900` notional);
- made `$450.00` from the later price retrace;
- paid `$1,524.29` funding and `$3.24` conservative fees;
- ended at `-$1,077.53` net;
- needed `$3,370` cumulative isolated top-up and `$5,513.89` peak Pump cash
  after funding debit, above `$3000 + $2000` temporary rescue capacity.

Funding first recovered above `-1%` at `2026-06-11 23:00 UTC`, but not every
entry gate was ready. The first complete all-gates entry was
`2026-06-12 10:00 UTC`:

| Feature | Value |
|---|---:|
| Entry price | `0.230190` |
| Pullback | `22.390425%` |
| OI change 24h | `-19.724381%` |
| Long account ratio | `0.6489` |
| Complete funding 24h | `+0.258594%` |

The current ladder then filled at `0.230190 / 0.345285 / 0.460380`. The hourly
mark peak was `0.625347` at `2026-06-14 10:00 UTC`. Current v4 margin formulas
need `$1,590` cumulative top-up and `$2,439.19` total Pump cash, so a sole
`$3000` portfolio holds it without main rescue. TP25 is reached at
`2026-06-14 13:00 UTC` after `51h`: `$450.00` price PnL, `-$249.19` funding,
`-$3.24` fees, `+$197.57` net.

Conclusion: the correct response was to delay HUSDT, not reject the entire
episode. Funding normalized about 44 hours after the invalid old entry; the
complete gate set became valid 11 hours later.

## Portfolio comparison

Scope: 360 post-2024 archived events, 56 symbols, 10,184 restored funding
settlements and 24,652 Bybit hourly mark points. All variants require a
complete interval-aware funding window. The portfolio contract is the active
v4 policy: `$3000`, four isolated positions, fixed `$600` ladder, 3x, 30% free
Pump cash plus `$75` floor for admission, no main borrowing for entry, and
bounded `$2000` temporary rescue for existing positions. Rescue principal is
excluded from return and slot size does not compound automatically.

| Variant | Trades | Win rate | Funding | Net PnL | ROI | Realized DD | Worst trade | Max rescue |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Current `current_main_v4` | 79 | 92.41% | `-$1,315.51` | `$8,340.70` | 278.02% | 20.03% | `-$759.44` | `$0.00` |
| Former PB20, 4 equal, 168h | 87 | 81.61% | `-$1,672.87` | `$6,308.30` | 210.28% | 15.64% | `-$671.49` | `$0.00` |
| Former PB25, 5 equal, 720h | 75 | **93.33%** | `-$956.81` | `$7,213.82` | 240.46% | **5.58%** | `-$260.94` | `$0.00` |
| Current tiers, funding strictly `>0` | 63 | 92.06% | `-$991.17` | `$6,150.25` | 205.01% | 25.85% | `-$759.44` | `$105.78` |

The current tiered variant has the strongest aggregate result. PB25 gives up
4 trades and `$1,126.88` net (13.5% of the current result), but cuts realized
drawdown from 20.03% to 5.58% and the worst trade from `-$759.44` to
`-$260.94`. It is therefore the strongest safer shadow candidate, not an
automatic live replacement. Requiring strictly non-negative funding is too
restrictive on this sample: it removes 16 current-strategy trades and
`$2,190.45` net without lowering the number of losses below five.

## Reproducibility

Fresh public-API rebuild:

```powershell
.\.venv\Scripts\python.exe scripts\pump_short_funding_corrected_portfolio.py
```

Rebuild from saved raw evidence:

```powershell
.\.venv\Scripts\python.exe scripts\pump_short_funding_corrected_portfolio.py --reuse-raw
```

Generated evidence is ignored under
`data/research/pump_short_funding_corrected_portfolio/`: `report.md`,
`index.html`, comparison/audit CSVs, raw funding/mark JSONL, and metadata.

Verification: collector plus new replay `11 passed`; expanded Pump research
regression `64 passed`; full project regression `742 passed`, 8 subtests and
13 pre-existing warnings. A saved-raw rebuild reproduced the final aggregates.

## Limits

- The archived input contains current-listed survivors, so delisted-symbol
  survivor bias remains.
- Entry, ladder and TP ordering is hourly, not tick-exact. Same-candle touches
  retain the replay's conservative high-first ladder convention.
- Public settlement funding and mark prices are exact; partial fills, contract
  rounding and live slippage are approximated.
- The capacity path is synchronized across concurrent trades. It does not
  invent hypothetical PnL for donor cuts; a `$2000` breach would be reported as
  requiring derisk rather than assumed to hold forever.
