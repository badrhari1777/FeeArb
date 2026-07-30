# Pump Live Spike Risk Research

## Scope

This report studies rapid upward moves that are dangerous for the Pump Live
short strategy. It does not change or authorize live execution.

Reproduce it with:

```text
.venv\Scripts\python.exe scripts\pump_spike_risk_research.py
```

Generated artifacts are written to
`data/research/pump_spike_risk_research/`.

## Data coverage

- Collector checked 602 Bybit instruments.
- 385 current crypto instruments passed the historical pump prefilter and have
  5,322,916 hourly candles.
- Coverage runs from 2024-01-01 through 2026-06-30, or from the contract's
  listing date when later.
- The other 217 instruments did not pass the daily `+50%`, 3-day `+100%`, or
  7-day `+180%` pump prefilter and have no detailed hourly archive.
- The archive has current-listing survivor bias: contracts delisted before
  collection are absent.

## Definitions

- An hourly surge is an hourly high at least the selected percentage above the
  previous continuous hourly close.
- Qualifying bars for one symbol separated by no more than six hours are one
  episode.
- A wick-like spike is a surge episode containing a candle that closes at
  least 10% below its high.
- Strategy-active spike counts use archived 15-minute Bybit Mark Price candles
  between the reconstructed current-tier entry and exit.
- A fast protection crossing means the first margin-warning and initial-stop
  crossings are no more than 15 minutes apart. With 15-minute OHLC this is an
  uncertainty bucket; it does not prove a move was faster than the live
  15-second monitor.

## Results

Market-wide hourly episodes:

| Rise | Episodes | Wick-like | Symbols |
|---:|---:|---:|---:|
| 20% | 2,348 | 881 | 371 |
| 30% | 849 | 428 | 307 |
| 50% | 244 | 162 | 156 |
| 100% | 46 | 31 | 40 |

The exact current tier could be reconstructed for 123 setups since 2024; 118
have matching 15-minute windows. The current online filters accept 40 of these,
with 39 having 15-minute coverage:

- 7/39 had an active 15-minute burst of at least 20%;
- 1/39 had a burst of at least 30%;
- 0/39 had a burst of at least 50%;
- 1/39 had a warning-to-initial-stop crossing within the 15-minute uncertainty
  bucket (`BASEDUSDT`, pump >=250%);
- 1/39 crossed the initial stop and L2 in the same 15-minute candle
  (`SOONUSDT`), although its warning crossing occurred much earlier.

For all 118 current-tier setups before online filtering, 21 cases had a 20%+
active burst, 5 had 30%+, 2 had 50%+, and 5 entered the warning-to-stop
15-minute uncertainty bucket. Four of those five were pump >=250%.

## Margin geometry

With a `$175` slot, 3x leverage, conservative 2.5% MMR, and the stop 2.5%
inside liquidation, the required added margin to move the first-leg stop above
L2 at `+50%` is approximately:

| Rule | First-leg notional | Required margin | Stop after $50 |
|---|---:|---:|---:|
| 5 equal legs | $105.00 | $25.59 | +72.10% |
| 3 tapered legs | $87.50 | $21.33 | +81.15% |
| 2 tapered legs | $175.00 | $42.65 | +53.99% |

The existing `$50` guaranteed rescue quota is therefore sufficient for every
current tier. The two-leg super-pump tier is the tight case.

## Candidate protections

1. Current on-demand margin plus pre-placed L2 is capital-efficient, but the
   15-minute archive leaves one main-gated rapid case unresolved and one
   same-candle stop/L2 ordering case.
2. Keeping L2 absent until a confirmed `$50` position-margin addition removes
   the add-order race without increasing reserve requirements. It can miss a
   desirable L2 fill during a sudden jump.
3. Pre-funding `$50` for every entry removes the observed warning-to-stop
   latency, but 17 of 39 historical main cases never crossed the warning and
   would have tied margin unnecessarily.
4. The most capital-aware candidate is:
   - retain the exchange-side Mark Price stop;
   - keep L2 gated until `$50` margin is confirmed;
   - pre-fund `$50` immediately for pump >=250%;
   - keep `$25 + $25` on-demand top-ups for lower tiers.

In the current sample this selective policy covers the only main-gated fast
case while pre-funding 8 rather than all 39 cases; 3 of those 8 never reached
the warning threshold.

## Before any live change

- Collect targeted 1-minute windows for the five uncertain cases when the live
  Bybit workload is flat or through a separately rate-limited collector.
- Replay exact intraminute ordering where available.
- Measure missed L2 fills and PnL impact from the gated-L2 policy.
- Add exchange-level tests for creating L2 only after the margin response and
  refreshed liquidation/stop confirmation.
