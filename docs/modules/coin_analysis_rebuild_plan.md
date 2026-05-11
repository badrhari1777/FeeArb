# Coin Analysis Module Development Plan

Status
- Refined on 2026-03-31 after the latest product clarification.
- This document supersedes the earlier draft.
- Phase-1 exchange scope is locked to `binance + kucoin`.

Notes
- The full detailed plan is being rebuilt around:
  - exact spread history from our own snapshots,
  - separate storage in `state/coin_analysis.db`,
  - manual-first workflow with future paper/auto compatibility.

Primary Goal
- Build a durable analysis module for inter-exchange funding/spread trading that answers two operational questions:
  1. `pre-entry analysis`: should we open this pair now, in which direction, and at what size regime?
  2. `in-position analysis`: should we hold, partially exit, fully exit, or partially add?

API-First Refinement Added On 2026-05-03
- This section refines the module specifically for `longer-duration funding holds` on `binance + kucoin`.
- The new emphasis is:
  - detect symbols where funding stays biased in one direction for many cycles,
  - compare exchanges even when funding intervals differ (`1h` vs `4h` vs `8h`),
  - separate `carry attractiveness` from `entry spread attractiveness`,
  - move from candle-close proxy logic toward `exact spread history` from synchronized market snapshots.

## 0. Current Implementation Snapshot

What already exists in the repo
- Exchange adapters already expose:
  - current market snapshots (`funding_rate`, `next_funding_time`, `mark_price`, BBO),
  - funding history loaders,
  - contract metadata with funding interval hints.
- Current per-symbol analysis is driven from:
  - `webapp/services.py -> analyze_symbol(...)`
  - `webapp/services.py -> _analyze_symbol_on_exchange(...)`
  - `webapp/services.py -> _analyze_pair(...)`
- Current persistence already stores:
  - funding history in `ca_funding_history`,
  - focus/live snapshots in `ca_market_snapshots_focus`,
  - feature snapshots / decisions / outcomes,
  - real/manual and paper position observations.

What the current implementation does well
- Pulls exchange-native funding history and persists it.
- Resolves funding interval heuristically from history + current snapshot.
- Pulls recent `1m` candles and builds a rough spread series from synchronized candle timestamps.
- Computes first-generation pair features and decision scores.

What the current implementation is still missing for the target product
- Candle-close spread is only a proxy, not executable spread.
- Funding comparison is not yet normalized as a first-class `arbitrary-window carry engine`.
- There is no explicit model for `long-term one-sided funding persistence`.
- There is no dedicated UI for:
  - long-hold candidate ranking,
  - funding persistence tables,
  - interval-normalized funding comparison,
  - spread entry-quality diagnostics,
  - exact-vs-proxy data quality separation.
- Historical spread quality still depends too much on `1m` candle overlap instead of exact local snapshots.

## 1. Product Goal Clarified For Long-Hold Entry

Target question
- We want to know not only whether a symbol is attractive for short-lived funding capture, but whether it is suitable for `longer-duration holding`.

Operational interpretation
- The tool must answer:
  1. Is funding biased in one direction persistently enough to justify holding?
  2. Is the current inter-exchange spread acceptable for entry, or are we overpaying on entry?
  3. Is the carry edge likely to survive long enough relative to spread cost, fees, and slippage?
  4. Is the symbol liquid/stable enough to hold without frequent spread shocks or crowding risk?

Recommended product outputs
- `enter_long_hold`
- `watch_wait_better_spread`
- `watch_funding_not_persistent_enough`
- `reject_spread_too_wide`
- `reject_data_quality_low`
- `reject_carry_too_weak`

## 2. Exact Data We Need

### 2.1 Instrument / Registry Data
- canonical symbol mapping:
  - `BTCUSDT` <-> `BTCUSDT` on Binance
  - `BTCUSDT` <-> `XBTUSDTM` on KuCoin
- contract metadata:
  - base / quote / settle asset
  - multiplier / contract size
  - tick size / qty step
  - margin mode support
  - funding interval metadata
  - active / paused status

Why it is mandatory
- Without this layer, funding, candles, OI, and spread series cannot be compared reliably.

### 2.2 Current Market State
- per exchange, per symbol:
  - best bid / ask
  - bid size / ask size
  - mark price
  - index price
  - premium
  - current funding rate / latest funding rate
  - predicted / next funding rate if exchange exposes it
  - next funding timestamp
  - current funding interval
  - current open interest

Purpose
- drive live entry decision,
- show operator the current state,
- seed exact local spread history.

### 2.3 Funding History
- required raw fields per settlement point:
  - `exchange`
  - `canonical_symbol`
  - `settlement_ts_ms`
  - `funding_rate`
  - `predicted_funding_rate` if available
  - `interval_hours`
  - `mark_price` if available
  - `source_type`

Required derived fields
- funding per hour
- funding per 8h equivalent
- rolling funding sums:
  - 24h
  - 72h
  - 7d
  - 14d
- persistence / sign-stability metrics:
  - `% of intervals with same sign`
  - `longest same-sign streak`
  - `net carry minus worst single reversal`

### 2.4 Exact Spread History
- required raw snapshot fields per exchange:
  - `ts_ms`
  - `bid`
  - `ask`
  - `mid`
  - `mark_price`
  - `index_price`
  - `premium_pct`
  - `quote_age_ms`
  - `source_type`

Required pair-derived spread fields
- midpoint spread:
  - `mid_binance - mid_kucoin`
- executable open spread:
  - `buy cheaper ask` vs `sell richer bid`
- executable close spread:
  - reverse side of the same pair
- mark spread
- index spread
- premium spread

Critical decision
- For `entry timing`, the primary signal must be `executable spread`, not candle-close spread and not midpoint spread.
- For `historical context`, candle/mark/index proxies are still useful, but they must be flagged as proxy-quality.

### 2.5 Candle Data
- required mostly for context / charting / coarse backfill:
  - `1m` candles for spread proxy and local regime inspection,
  - optional `5m`, `15m`, `1h` rollups for UI and analysis speed.

Important limitation
- Candle data is not enough to reconstruct traded spread precisely.
- It is only a proxy for:
  - rough history before our local spread collector has enough points,
  - charting convenience,
  - sanity checks.

### 2.6 OI / Crowding Data
- current OI
- historical OI by interval
- OI delta over:
  - 1h
  - 4h
  - 24h
  - 7d if feasible

Why it matters
- A long-hold funding candidate with aggressively rising OI and unstable spread is more dangerous than one with steady funding and stable OI.

## 3. What We Can Pull By API Today

### 3.1 Binance
- Current market state:
  - `GET /fapi/v1/premiumIndex`
    - current `markPrice`, `indexPrice`, `lastFundingRate`, `nextFundingTime`
  - `GET /fapi/v1/ticker/bookTicker`
    - current BBO
- Funding:
  - `GET /fapi/v1/fundingRate`
    - historical settlement funding
  - `GET /fapi/v1/fundingInfo`
    - current `fundingIntervalHours` only for symbols with adjusted caps/floors/intervals
- Candles / price proxies:
  - `GET /fapi/v1/klines`
    - trade-price candles
  - `GET /fapi/v1/markPriceKlines`
    - mark-price candles
  - `GET /fapi/v1/indexPriceKlines`
    - index-price candles
  - `GET /fapi/v1/premiumIndexKlines`
    - premium-index candles
- Spread / basis context:
  - `GET /futures/data/basis`
- OI:
  - `GET /fapi/v1/openInterest`
  - `GET /futures/data/openInterestHist`

### 3.2 KuCoin
- Current market state:
  - `GET /api/v1/contracts/active`
    - contract metadata, current funding values, current granularity, `markPrice`, `indexPrice`, `nextFundingRateDateTime`, OI snapshot
  - `GET /api/v1/ticker`
    - current BBO
  - `GET /api/v1/mark-price/{symbol}/current`
    - current mark/index snapshot
  - `GET /api/v1/funding-rate/{symbol}/current`
    - current / next funding view
- Funding:
  - `GET /api/v1/contract/funding-rates`
    - historical settlement funding
- Candles:
  - `GET /api/v1/kline/query`
    - futures candles
- OI:
  - current OI can be taken from `contracts/active`
  - UTA `Get Futures Open Interest` exists, but retention is newer/shorter and should be treated as optional phase-2 enrichment

### 3.3 Practical API Conclusions
- Funding history:
  - both Binance and KuCoin give us enough to build a reliable carry history.
- Current funding state:
  - both exchanges give enough to monitor current/next funding.
- Candles:
  - both exchanges give futures candles, but KuCoin explicitly warns that candle data may be incomplete when there are no ticks.
- Exact spread:
  - neither exchange gives historical cross-exchange spread directly;
  - we must store our own synchronized snapshots.

## 4. How To Compare Different Funding Intervals Correctly

### 4.1 Core Rule
- We must stop thinking of funding as “just the latest rate”.
- Funding history must be converted into a `time-allocated carry series`.

### 4.2 Canonical Representation
- Each funding record becomes an interval:
  - `effective_from_ts_ms`
  - `effective_to_ts_ms`
  - `funding_rate_for_interval`
  - `interval_hours`
- If a settlement point is at time `T` with interval `H`, then the funding event represents carry over:
  - `(T - H, T]`

### 4.3 Comparison Method
- For any target comparison window, for example `00:00 -> 04:00 UTC`:
  - Binance 4h symbol:
    - 1 funding event covering the whole 4h window
  - KuCoin 1h symbol:
    - 4 funding events, one per hour
- We compare:
  - exact sum over the overlapping interval, not raw last values

Example
- Exchange A:
  - one `4h` rate = `+0.0400%` covering `00:00 -> 04:00`
- Exchange B:
  - four `1h` rates:
    - `+0.008%`
    - `+0.009%`
    - `+0.011%`
    - `+0.012%`
- Correct comparison over `00:00 -> 04:00`:
  - A total = `+0.0400%`
  - B total = `+0.0400%`
  - net carry difference = `0`

### 4.4 Required Derived Views
- raw event table:
  - exact settlement events
- hourly normalized view:
  - carry density per hour
- arbitrary-window aggregates:
  - 4h
  - 8h
  - 24h
  - 72h
  - 7d
  - 14d
- rolling aligned comparison:
  - `exchange_a_sum(window) - exchange_b_sum(window)`

### 4.5 Implementation Decision
- The engine should not hardcode assumptions like “everything is 8h”.
- All carry calculations must be driven from:
  - event timestamps,
  - interval_hours,
  - interval overlap math.

## 5. Candle Timing And Price Comparability

What is safe to assume
- On Binance futures, kline, mark-price kline, index-price kline, and premium-index kline are keyed by `open time`.
- On KuCoin futures, kline data is keyed by bucket start time with explicit `granularity`.
- Therefore, for the same interval size, candles can be aligned by `bucket_start_ts_ms` in UTC.

Important nuance
- “Candles close at the same time” is operationally true only if:
  - both exchanges use the same interval size for the comparison,
  - both candles actually exist,
  - there were ticks on both venues,
  - we align by time bucket, not by retrieval order.

What is not safe
- Using candles alone to decide real entry spread.
- Assuming missing KuCoin candles mean zero movement; they can also mean “no tick published for that bucket”.

Product rule
- Candle alignment is acceptable for:
  - historical proxy visualization,
  - rough spread regime detection,
  - bootstrap analysis before exact local spread history exists.
- Candle alignment is not sufficient for:
  - entry-quality decision,
  - slippage-aware spread analysis,
  - proving tradable open/close edge.

## 6. Recommended Analysis Horizons

Default recommendation for long-hold research
- `funding history default`: `14 days`
- `expanded funding view`: `30 days` if available without operational pain
- `exact local spread collector`: start immediately and retain:
  - `1s-2s focus snapshots` for tracked symbols
  - roll up to `1m / 5m / 15m / 1h`
- `entry-quality chart`: last `24h` and `72h`
- `long-hold carry chart`: `7d` and `14d`

Why `14 days` is the best default
- Long enough to detect:
  - sign persistence,
  - funding regime changes,
  - repeated interval changes,
  - OI/funding crowding behavior.
- Short enough to keep API pulls practical and responsive.

Why `30 days` should remain optional
- Useful for confirmation, but should not block normal operator workflow.
- For some high-frequency data sources, retention/paging cost rises quickly.

## 7. Target Machine Analysis

Recommended score components

### 7.1 Carry Persistence Score
- How often funding stayed on the desired side over 14d
- Same-sign streak length
- Net carry over 24h / 72h / 7d / 14d
- Worst adverse reversal inside the window

### 7.2 Entry Spread Score
- Current executable spread vs:
  - 24h median
  - 72h percentile band
  - local exact spread mean
- Entry penalty if current spread is much worse than recent typical entry

### 7.3 Spread Stability Score
- Spread volatility
- Jump frequency
- Quote freshness mismatch frequency
- Difference between midpoint and executable spread

### 7.4 Liquidity / Tradability Score
- BBO depth
- shallow sweep cost
- BBO persistence
- symbol availability on both venues

### 7.5 Crowding / Stress Score
- OI acceleration
- premium distortion
- mark/index divergence
- funding rising while spread worsens

### 7.6 Data Quality Score
- missing candle buckets
- stale funding metadata
- incomplete OI range
- insufficient exact spread history

Final outputs
- recommendation
- confidence
- operator-readable reasons
- explicit blockers

## 8. Target UI: Tables And Charts

### 8.1 Candidate Summary Table
- symbol
- suggested direction
- 24h / 72h / 14d net carry difference
- funding interval pair (`4h vs 1h`, etc.)
- persistence score
- current executable spread
- entry spread percentile
- OI change
- recommendation
- confidence

### 8.2 Funding Comparison Table
- settlement time
- Binance funding event
- KuCoin funding event
- interval hours
- normalized hourly carry
- aligned 4h/8h/24h aggregates
- net carry difference

### 8.3 Spread Diagnostics Table
- current BBO spread
- current executable open spread
- current executable close spread
- mark spread
- index spread
- premium spread
- spread freshness

### 8.4 Charts
- funding carry over time:
  - raw settlement events
  - rolling 24h and 72h sums
- exact spread history:
  - executable spread
  - mid spread
  - mark spread
- entry overlay:
  - current spread vs recent percentile bands
- OI history:
  - per exchange and delta

## 9. Immediate Engineering Plan

### Phase A: Funding Engine Hardening
- Add first-class funding normalization utilities:
  - event interval expansion
  - arbitrary-window overlap aggregation
  - hourly-density conversion
- Persist enough metadata to make the reconstruction deterministic.
- Add tests for:
  - `8h vs 8h`
  - `4h vs 1h`
  - interval changes over time for the same symbol

### Phase B: Exact Spread Collector
- Start / strengthen tracked-symbol snapshot collector for `binance + kucoin`.
- Persist exact synchronized snapshots for selected symbols and active positions.
- Derive executable spread history from stored snapshots.

### Phase C: Candidate / Long-Hold Scoring
- Add long-hold-specific feature builder and score engine.
- Keep existing candidate engine, but split outputs:
  - short-term event trade
  - long-hold funding carry

### Phase D: UI / Export
- Add dedicated long-hold page or dedicated section inside `/coin`.
- Show clear operator tables and charts.
- Export normalized funding comparison and spread history for external review.

## 10. Required Tests

Unit tests
- interval inference
- interval change handling
- overlap aggregation
- aligned 4h comparison from mixed raw intervals
- spread alignment by bucket start

Integration tests
- Binance funding history pull for candidate symbols
- KuCoin funding history pull for candidate symbols
- Binance mark/index/premium candle pull
- KuCoin futures kline pull with paging
- exact spread derivation from stored focus snapshots

Recommended symbol set for test probes
- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`
- `XRPUSDT`
- `DOGEUSDT`
- plus 2-3 current table candidates with unusual funding intervals if present

Operational validation
- For each test symbol:
  - load 14d funding history,
  - compare normalized carry between Binance and KuCoin,
  - pull 24h / 72h spread proxy,
  - verify exact spread collector begins filling local history,
  - verify recommendation is explainable.

Strategic Context
- Direction is not fixed.
- The module must evaluate both directions for every pair:
  - `LONG Binance + SHORT KuCoin`
  - `LONG KuCoin + SHORT Binance`
- Profit can come from two independent components:
  - funding carry,
  - spread/basis normalization.
- The analysis must always decompose those components separately.

Critical Product Decisions Locked In
1. Exact inter-exchange spread history cannot be built from candle closes alone.
2. We must start collecting and storing our own spread history immediately.
3. Storage for this module will be separate from existing cache tables:
   - target DB: `state/coin_analysis.db`
4. Phase 1 is manual-first, but the architecture must already support future paper auto-entry.
5. Candidate analysis and position monitoring are separate workflows and separate data products.
6. Every decision must be explainable, exportable, and reviewable later.

Why The Current `/coin` Flow Is Not Enough
- The current implementation in `webapp/services.py` is on-demand and largely stateless.
- Pair history is derived from synchronized `1m` candles, which is not precise enough for the spread we actually trade.
- There is no persistent spread/basis feature store.
- There is no durable decision journal.
- There is no position-state timeline for hindsight review.
- Current core pair logic is effectively a diagnostic, not a trustworthy decision engine.

## 1. The Hardest Problem: How To Get Spread History If We Do Not Know The Coin In Advance

User requirement clarified
- We often do not know the coin beforehand.
- In practice, the operator clicks a symbol from the table or types it manually, then runs analysis.
- The user explicitly does not want phase 1 to become "analyze every coin we import from Coinglass".

Revised conclusion
- Phase 1 should be `symbol-first`, not `full-universe-first`.
- That means:
  - the user chooses a symbol,
  - the system runs analysis for that symbol,
  - if the user opens a paper trade, that symbol becomes a tracked position,
  - after that the module keeps collecting exact local history for that symbol in real time.

What this means in practice
- We accept that for a symbol opened for the very first time, exact local cross-exchange spread history may be short or absent.
- Therefore phase 1 uses a hybrid model:
  - exact current live data from our own snapshots,
  - exchange-native historical proxies for context,
  - exact local spread collection starting immediately after analysis begins.
- This is simpler, cheaper, and closer to the user's real workflow.

Recommended collection model for phase 1

Layer A: `on-demand symbol analysis`
- Trigger:
  - user opens `/coin/{symbol}` or types a symbol manually.
- What happens:
  - validate symbol on Binance + KuCoin,
  - load current exact market state,
  - load exchange-native history needed for context,
  - start local spread capture for this symbol.
- Purpose:
  - keep the workflow close to current manual research.

Layer B: `focus history` for actively watched symbols
- Purpose:
  - support live decisioning, partial entry / exit logic, and paper monitoring.
- Activation triggers:
  - symbol opened in `/coin`,
  - symbol saved as tracked by the operator,
  - symbol used in a paper position,
  - symbol found in current real/manual positions.
- Cadence:
  - target `1s` to `2s` via websocket BBO/mark streams when available;
  - fallback `5s` REST snapshot if WS is not available or unstable.
- Stored metrics:
  - BBO,
  - mark,
  - index,
  - premium,
  - next funding,
  - quote freshness,
  - optional shallow depth snapshots for sweep-cost estimation.

Layer C: `bootstrap / backfill proxy`
- Purpose:
  - make the first analysis useful before enough local history has been collected.
- Use only for:
  - initial context,
  - approximate charts,
  - sanity checks.
- Sources:
  - Binance:
    - `markPriceKlines`
    - `indexPriceKlines`
    - `premiumIndexKlines`
    - funding history
    - basis history
    - OI history
  - KuCoin:
    - funding history
    - contract metadata snapshots
    - current/open-interest endpoints where retention permits
- Important:
  - bootstrap data is not the same as our own synchronized spread history;
  - every derived metric using bootstrap data must carry a `data_quality` / `provenance` flag.

Layer D: `optional discovery mode` for later
- Not part of phase 1 default behavior.
- Can be added later if we decide we truly need broad automatic intraday spread history for many symbols.
- If added later, it should probably start from a manually curated shortlist, not the entire external candidate feed.

Layer E: `candle-close proxy`
- Keep only as a last-resort fallback diagnostic.
- Never use candle-close spread as the primary decision input once local spread history exists.

Practical result
- Phase 1 does not waste effort on analyzing all imported candidates.
- The module becomes strong exactly where the operator actually works:
  - selected symbol analysis,
  - paper entry,
  - paper/live position monitoring,
  - later review and export.

## 2. Scope Of The First Real Build

Phase 1 scope
- Exchanges:
  - `binance`
  - `kucoin`
- Modes:
  - manual symbol analysis,
  - tracked-symbol monitoring for symbols explicitly opened or saved by the operator,
  - tracked-position monitoring for both real/manual and paper positions,
  - paper-mode journaling.

Phase 1 operating model
- The operator picks the symbol manually, either from the current UI link flow or by typing it.
- The module analyzes only that symbol/pair on demand.
- If the recommendation is actionable and the operator wants to test it, the operator opens a `paper` trade from the same flow.
- After that, the symbol enters continuous monitoring and the module produces real-time hold / partial exit / full exit / add decisions for:
  - paper positions,
  - real/manual positions.
- The center of gravity is not "scan everything", but "analyze selected symbols and then monitor open positions well".

Explicit non-goals for the first implementation wave
- Live auto-entry.
- Live auto-exit.
- Full-universe background spread collection by default.
- Coinglass-driven automatic analysis of all imported symbols.
- Multi-exchange beyond Binance/KuCoin.
- ML model training.
- Deep orderbook replay for the full universe.

What must still be future-compatible now
- directional scoring,
- partial add / partial exit decisions,
- event journaling,
- export for ChatGPT web review,
- replay/backtest hooks.

## 3. Architecture Overview

Recommended package split

Registry layer
- `analysis_registry/coin_instruments.py`
  - canonical symbol normalization,
  - Binance <-> KuCoin symbol mapping,
  - contract specs,
  - funding interval,
  - tick size / qty step / multiplier,
  - tradability status.

Collector layer
- `analysis_collectors/coin_discovery.py`
  - every-15s full-universe snapshots.
- `analysis_collectors/coin_focus.py`
  - high-frequency tracked symbol/position snapshots.
- `analysis_collectors/coin_funding.py`
  - funding history refresh and funding-now refresh.
- `analysis_collectors/coin_oi.py`
  - current OI snapshots + historical OI refresh.
- `analysis_collectors/coin_depth.py`
  - optional top-N orderbook depth snapshots for tracked symbols only.

Storage layer
- `analysis_storage/coin_db.py`
  - schema creation,
  - inserts / upserts,
  - retention,
  - rollups,
  - export queries.

Feature layer
- `analysis_features/spread.py`
  - exact spread series and stats.
- `analysis_features/funding.py`
  - funding edge, persistence, trend, flip risk.
- `analysis_features/oi.py`
  - OI trend, divergence, crowding flags.
- `analysis_features/liquidity.py`
  - top-of-book stability, sweep cost, slippage approximation.
- `analysis_features/economics.py`
  - fees, slippage, funding carry, breakeven spread.
- `analysis_features/position_state.py`
  - features relative to a concrete held position / paper position.

Decision layer
- `analysis_decisions/candidate_rules.py`
  - `NO_TRADE`, `ENTRY_SMALL`, `ENTRY_STRONG`.
- `analysis_decisions/position_rules.py`
  - `HOLD`, `PARTIAL_EXIT`, `FULL_EXIT`, `ADD_SMALL`, `ADD_BLOCKED`.
- `analysis_decisions/reason_codes.py`
  - centralized reason code registry.
- `analysis_decisions/scoring.py`
  - reusable score components and threshold configuration.

Paper engine layer
- `analysis_paper/portfolio.py`
  - paper positions and legs.
- `analysis_paper/fills.py`
  - entry/exit/add/reduce fills.
- `analysis_paper/funding.py`
  - funding accrual events.
- `analysis_paper/logs.py`
  - human-readable and machine-readable event logs.

Service/UI layer
- keep `/coin/{symbol}` as the main manual page,
- add background collectors and tracked-symbol APIs,
- add save/export endpoints,
- later add position watcher integration.

## 4. Storage Plan

DB choice
- Separate SQLite database:
  - `state/coin_analysis.db`
- Why separate DB:
  - cleaner lifecycle than mixing with existing execution cache,
  - easier backup/export/reset,
  - easier schema evolution,
  - avoids polluting current hot-path tables.

SQLite settings
- WAL mode.
- Busy timeout.
- Explicit indexes for `symbol`, `pair_key`, `ts_ms`.
- Periodic vacuum / retention pruning job.

Core schema proposal

### 4.1 Registry tables

`ca_instruments`
- `instrument_id`
- `canonical_symbol`
- `exchange`
- `exchange_symbol`
- `base_asset`
- `quote_asset`
- `contract_type`
- `contract_multiplier`
- `tick_size`
- `qty_step`
- `min_qty`
- `min_notional`
- `funding_interval_hours`
- `is_active`
- `source_ts_ms`
- `updated_at_ms`

`ca_pairs`
- `pair_key`
- `canonical_symbol`
- `exchange_a`
- `exchange_b`
- `exchange_a_symbol`
- `exchange_b_symbol`
- `is_active`
- `updated_at_ms`

### 4.2 Raw market history

`ca_market_snapshots_discovery`
- `ts_ms`
- `canonical_symbol`
- `exchange`
- `exchange_symbol`
- `bid`
- `ask`
- `bid_size`
- `ask_size`
- `mid`
- `mark_price`
- `index_price`
- `premium_pct`
- `funding_rate`
- `predicted_funding_rate`
- `next_funding_ts_ms`
- `turnover_24h`
- `volume_24h`
- `quote_age_ms`
- `source_type` (`rest_batch`)

`ca_market_snapshots_focus`
- same fields as discovery plus:
- `focus_reason` (`manual_page`, `tracked_symbol`, `paper_position`, `live_position`)
- `source_type` (`ws`, `rest_fallback`)
- `staleness_flag`
- `sequence_id` / `raw_ref` where useful

`ca_depth_snapshots`
- `ts_ms`
- `canonical_symbol`
- `exchange`
- `depth_levels_json`
- `top_n`
- `source_type`

### 4.3 Historical funding / OI / auxiliary history

`ca_funding_history`
- `ts_ms`
- `canonical_symbol`
- `exchange`
- `funding_rate`
- `predicted_funding_rate`
- `interval_hours`
- `mark_price`
- `source_type`

`ca_open_interest_history`
- `ts_ms`
- `canonical_symbol`
- `exchange`
- `oi_contracts`
- `oi_notional`
- `interval_label`
- `source_type`

`ca_aux_history`
- `ts_ms`
- `canonical_symbol`
- `exchange`
- `series_type` (`mark_kline`, `index_kline`, `premium_kline`, `basis`, `price_candle`)
- `payload_json`
- `source_type`

### 4.4 Derived spread / feature history

`ca_spread_series`
- `ts_ms`
- `pair_key`
- `canonical_symbol`
- `exchange_a`
- `exchange_b`
- `mid_spread_pct`
- `mark_spread_pct`
- `index_spread_pct`
- `open_spread_long_a_short_b_pct`
- `open_spread_long_b_short_a_pct`
- `close_spread_long_a_short_b_pct`
- `close_spread_long_b_short_a_pct`
- `premium_a_pct`
- `premium_b_pct`
- `premium_diff_pct`
- `series_quality` (`exact_discovery`, `exact_focus`, `bootstrap_proxy`, `candle_proxy`)

`ca_feature_snapshots`
- `ts_ms`
- `pair_key`
- `canonical_symbol`
- `context_mode` (`candidate`, `position`)
- `feature_set_version`
- `direction` (`long_a_short_b`, `long_b_short_a`)
- `features_json`
- `data_quality_json`

`ca_position_feature_snapshots`
- `ts_ms`
- `position_key`
- `pair_key`
- `direction`
- `features_json`
- `state_json`

### 4.5 Decision journal

`ca_decisions`
- `decision_id`
- `ts_ms`
- `mode` (`manual_candidate`, `manual_position_review`, `paper_auto_watch`, `paper_auto_signal`)
- `canonical_symbol`
- `pair_key`
- `direction`
- `action`
- `confidence_score`
- `reason_codes_json`
- `reason_text_json`
- `scores_json`
- `features_ref`
- `state_ref`
- `operator_note`

`ca_outcomes`
- `decision_id`
- `horizon` (`15m`, `1h`, `4h`, `to_next_funding`, `to_exit`)
- `outcome_json`
- `evaluated_at_ms`

`outcome_json` should evaluate correctness in decomposed form, not as a single opaque label
- `decision_correctness`
- `funding_component_correct`
- `spread_component_correct`
- `timing_quality`
- `would_waiting_15m_help`
- `would_exiting_15m_earlier_help`
- `net_pnl_delta_vs_alternative`
- `notes`

Correctness policy
- A decision should not be marked simply `right` or `wrong`.
- It should be reviewed across separate dimensions:
  - direction correctness,
  - timing correctness,
  - funding thesis correctness,
  - spread thesis correctness,
  - execution-size appropriateness.

### 4.6 Paper mode

`ca_paper_positions`
- `position_key`
- `opened_at_ms`
- `closed_at_ms`
- `status`
- `canonical_symbol`
- `pair_key`
- `direction`
- `qty`
- `entry_context_json`

`ca_paper_legs`
- `position_key`
- `exchange`
- `side`
- `entry_price`
- `current_qty`
- `fees_paid`
- `realized_pnl`
- `unrealized_pnl`

`ca_paper_events`
- `event_id`
- `position_key`
- `ts_ms`
- `event_type` (`entry`, `partial_add`, `partial_exit`, `funding_accrual`, `mark_update`, `close`)
- `payload_json`

## 5. Data Collection Plan And Intervals

This must be explicit because the user asked to define intervals.

### 5.1 Instrument registry refresh
- Full refresh:
  - on startup,
  - then every `1h`,
  - plus manual force refresh endpoint.
- Immediate refresh triggers:
  - symbol unsupported but typed by user,
  - new listing / mapping mismatch detected,
  - exchange metadata parse failure.

### 5.2 Discovery snapshots for all shared symbols
- Phase 1 default:
  - disabled.
- Reason:
  - the user wants a manual symbol-driven workflow first, not broad analysis of all external candidates.
- Future optional mode:
  - if enabled later, run `15s` snapshots on a manually curated shortlist or a tightly filtered universe.
- Retention if enabled later:
  - raw `15s` snapshots for `14d`.
  - roll up to `1m` bars for `90d`.
  - optional roll up to `5m` for `365d`.

### 5.3 Focus snapshots for tracked symbols
- Cadence:
  - primary: `1s` or `2s` via WS.
  - fallback: `5s` REST.
- Trigger:
  - manual page open,
  - tracked symbol,
  - paper/live position.
- Lifecycle:
  - opening `/coin/{symbol}` starts a hot collection window for `30m`,
  - reopening or refreshing extends that window,
  - tracked symbols can keep collection enabled until manually disabled,
  - held paper/real positions keep collection enabled while the position exists.
- Retention:
  - raw focus `1s/2s/5s` for `7d`.
  - roll up to `15s` for `30d`.
  - roll up to `1m` for `180d`.

### 5.4 Funding-now refresh
- Cadence:
  - piggyback on discovery/focus snapshots.
- Additional high-attention window:
  - every `1m` during the last `20m` before the nearest funding event for tracked symbols / positions.

### 5.5 Funding history refresh
- Cadence:
  - every `10m` regular refresh for tracked symbols.
  - every `30m` for full universe warm cache.
  - immediate refresh after funding settlement boundary.
- Retention:
  - keep full fetched history as available from exchanges.

### 5.6 Open interest current snapshots
- Cadence:
  - `5m` for full universe if endpoint cost is acceptable.
  - `1m` to `5m` for tracked symbols / positions.
- Retention:
  - raw `5m` for `30d`.
  - roll up `1h` for `180d`.

### 5.7 Open interest historical backfill
- Binance:
  - backfill `5m` / `15m` / `1h` windows where needed.
- KuCoin:
  - use current public OI / historical OI endpoints where retention allows;
  - because retention is shorter, persist locally so we do not lose history.

### 5.8 Orderbook depth snapshots
- Only for tracked symbols / positions.
- Cadence:
  - `5s` shallow depth snapshot,
  - plus on-demand snapshot at decision time / paper fill time.
- Retention:
  - raw depth snapshots for `7d`.

## 6. Instrument Registry Requirements

The registry must solve these problems cleanly
- Canonical symbol normalization:
  - `BTCUSDT` style internal symbol.
- Exchange-specific mapping:
  - Binance `BTCUSDT`
  - KuCoin `XBTUSDTM` etc.
- Contract metadata:
  - multiplier / contract size,
  - tick size,
  - qty step,
  - min qty,
  - min notional,
  - funding interval.
- Status flags:
  - active,
  - suspended,
  - delisting / not tradable,
  - mapping confidence.

Direction-agnostic pair definition
- Every pair record must allow later evaluation in both directions.
- Do not encode long/short bias into the pair key itself.
- Recommended pair key:
  - `BTCUSDT|binance|kucoin`

## 7. Derived Spread Table Definition

We need multiple spread notions because one number is not enough.

Base metrics
- `mid_spread_pct`
  - compares mids between exchanges.
- `mark_spread_pct`
  - compares mark prices between exchanges.
- `index_spread_pct`
  - compares index prices between exchanges.
- `premium_a_pct`
  - `(mark_a - index_a) / index_a`
- `premium_b_pct`
  - `(mark_b - index_b) / index_b`
- `premium_diff_pct`
  - `premium_a_pct - premium_b_pct`

Directional executable metrics
- `open_spread_long_a_short_b_pct`
  - entry economics using `ask_a` and `bid_b`
- `open_spread_long_b_short_a_pct`
  - entry economics using `ask_b` and `bid_a`
- `close_spread_long_a_short_b_pct`
  - if we hold `long_a_short_b`, closing economics using `bid_a` and `ask_b`
- `close_spread_long_b_short_a_pct`
  - inverse of the above

Funding-normalized metrics
- `funding_net_pct_per_interval_long_a_short_b`
- `funding_net_pct_per_hour_long_a_short_b`
- inverse direction equivalents
- `time_to_next_funding_sec_a`
- `time_to_next_funding_sec_b`

Why this matters
- We must separate:
  - "spread looks attractive in mark terms",
  - "entry is actually executable at BBO",
  - "funding is positive for our chosen direction",
  - "closing later is likely to recover costs".

## 8. Feature Engine v1

The feature engine should emit explainable primitives first, not opaque scores.

### 8.1 Spread regime features
- z-score on:
  - `1h`,
  - `4h`,
  - `24h`
- percentile on:
  - `24h`,
  - `7d`
- spread velocity:
  - `1m`,
  - `5m`,
  - `15m`
- spread acceleration:
  - velocity delta
- mean reversion speed estimate
- time-in-zone:
  - how long spread stayed beyond selected thresholds
- excursion profile:
  - worst adverse move after similar conditions

### 8.2 Premium / basis stress features
- premium on each exchange
- premium differential
- divergence between:
  - BBO spread,
  - mark spread,
  - index spread
- stress label:
  - `clean_dislocation`,
  - `last-price_noise`,
  - `mark_confirmed`,
  - `index_confirmed`,
  - `stress_divergence`

### 8.3 Funding features
- current funding rate per exchange
- predicted funding rate where available
- interval-normalized net funding by direction
- funding persistence:
  - last `3` settlements,
  - last `6` settlements,
  - weighted recent mean
- funding acceleration / decay
- proximity to zero
- flip-risk label
- settlement alignment / mismatch flag
- minutes to next funding / next interval boundary
- boundary-window label:
  - `far_from_boundary`
  - `pre_boundary_watch`
  - `decision_window_20m`
  - `decision_window_15m`
  - `post_boundary_cooldown`

### 8.3a Time-window / boundary features
- This module must explicitly understand time context, because many important decisions are taken roughly `20-15 minutes` before the end of the funding interval.
- Required features:
  - `minutes_to_next_funding_a`
  - `minutes_to_next_funding_b`
  - `decision_window_active`
  - `decision_window_type`:
    - `hourly_like`
    - `four_hour_like`
    - `eight_hour_like`
    - `mismatch`
  - spread behavior in the last:
    - `20m`
    - `15m`
    - `10m`
    - `5m`
  - funding prediction change in the last:
    - `20m`
    - `15m`
    - `5m`
  - micro-volatility and spread instability near boundary
  - `time_weighted_exit_urgency`
- Goal:
  - the engine should distinguish a stable mid-interval setup from a setup that is entering the critical pre-settlement decision zone.

### 8.4 Open interest features
- current OI per exchange
- OI delta:
  - `15m`,
  - `1h`,
  - `4h`,
  - `24h`
- OI divergence
- price vs OI regime:
  - price up + OI up,
  - price up + OI down,
  - price down + OI up,
  - price down + OI down
- crowding / squeeze heuristic

### 8.5 Liquidity / execution features
- top-of-book spread stability
- quote refresh rate
- stale quote detection
- depth within slippage bands:
  - `5 bps`,
  - `10 bps`,
  - `20 bps`
- estimated sweep cost for candidate sizes
- exit liquidity now vs rolling average
- imbalance and microstructure stability

### 8.6 Trade economics features
- maker / taker fee assumptions per exchange
- expected slippage per leg
- net carry to next funding
- net carry to next two funding events
- spread move required to breakeven
- expected value breakdown:
  - `expected_funding_pnl`
  - `expected_spread_pnl`
  - `expected_fee_cost`
  - `expected_slippage_cost`
- `spread_profit_to_next_funding_ratio`

### 8.7 Position-state features
- entry spread vs current spread
- current funding edge vs entry funding edge
- realized / unrealized PnL
- funding accrued so far
- time in trade
- distance to target exit
- distance to risk exit
- leg imbalance / quantity drift
- margin / liquidation buffer context where position data is available
- boundary context for held positions:
  - minutes to next settlement,
  - is this inside the main `20-15m` decision zone,
  - has the setup improved or deteriorated specifically during that zone.

## 9. Decision Engine v1

Signals to implement
- `NO_TRADE`
- `ENTRY_SMALL`
- `ENTRY_STRONG`
- `HOLD`
- `PARTIAL_EXIT`
- `FULL_EXIT`
- `ADD_SMALL`
- `ADD_BLOCKED`

Mandatory properties
- every signal must return:
  - action,
  - direction,
  - decision_phase,
  - confidence,
  - score components,
  - reason codes,
  - human-readable reasons.
- no black-box single number without decomposition.

`decision_phase` must be one of
- `exploratory`
- `mid_interval`
- `pre_boundary_20m`
- `pre_boundary_15m`
- `boundary_immediate`
- `post_boundary`

Why this matters
- later review must show not only what the engine decided, but also when in the cycle it decided it.

### 9.1 Candidate rule engine

Inputs
- pair feature snapshot,
- direction-specific economics,
- current data quality,
- user-configured fee/slippage assumptions.

Output
- best direction or `NO_TRADE`.

Recommended structure
- `eligibility gate`
  - symbol supported on both exchanges,
  - acceptable data quality,
  - acceptable quote freshness,
  - funding interval compatible enough,
  - minimum liquidity.
- `economics gate`
  - net expected edge after fees/slippage must be positive enough.
- `regime gate`
  - current spread should be attractive relative to history, not just large in absolute terms.
- `risk gate`
  - continuation risk / stress / crowding not above threshold.
- `sizing gate`
  - if eligible but unstable, downgrade to `ENTRY_SMALL`.

Candidate outputs by meaning
- `NO_TRADE`
  - no positive edge or risk too high.
- `ENTRY_SMALL`
  - edge exists but signal quality or market stability is moderate.
- `ENTRY_STRONG`
  - edge exists, spread regime is favorable, and continuation risk is contained.

Time-aware candidate policy
- outside the main boundary window:
  - the engine can still analyze and score,
  - but entry recommendations should usually be more conservative.
- inside the main `20-15m` window before settlement:
  - funding persistence, funding drift, and spread stability should carry more weight.
- if spread or funding changes abruptly against the thesis inside that window:
  - block new entry or downgrade to `NO_TRADE`.

### 9.2 Position rule engine

Inputs
- current position state,
- directional feature snapshot,
- accrued funding,
- exit/add economics.

Core logic groups
- `hold logic`
  - funding still favorable,
  - spread thesis intact,
  - exit economics not yet compelling.
- `partial exit logic`
  - thesis still partly valid but edge has materially decayed;
  - lock in part of the result and reduce exposure.
- `full exit logic`
  - funding edge lost or flipped,
  - spread reverted enough,
  - continuation risk dominates.
- `add logic`
  - only if:
    - direction thesis unchanged,
    - adverse move improved entry economics,
    - liquidity is adequate,
    - crowding / stress is not elevated.
- `add blocked`
  - any "add" thesis denied by data quality, crowding, or economics guard.

Time-aware position policy
- the main review zone is the last `20-15m` before the relevant funding boundary.
- in that zone the engine should explicitly reassess:
  - does funding still justify holding,
  - is spread reverting or escaping,
  - has liquidity deteriorated,
  - did premium/OI stress increase.
- if there is no strong imbalance or emergency deterioration:
  - the engine should treat this zone as the primary deliberate decision window.
- if there is sharp spread escape or funding collapse earlier:
  - the engine may still issue faster `FULL_EXIT` / `PARTIAL_EXIT` decisions outside the standard window.

### 9.3 Two scores that must be explicit

`continuation_risk_score`
- estimates probability the spread continues moving against the intended direction before funding/exit.
- built from:
  - premium stress,
  - OI crowding,
  - velocity / acceleration,
  - funding decay / flip risk,
  - liquidity deterioration.

`reversion_score`
- estimates strength of the case for spread normalization.
- built from:
  - spread percentile / z-score,
  - historical dwell / snapback behavior,
  - premium convergence,
  - funding persistence,
  - stable liquidity.

Decision style
- rules should not compare only "high score vs low score".
- they should express:
  - `reversion strong enough`
  - `continuation risk too high`
  - `funding not worth the hold`
  - `spread already normalized`

## 10. Paper Mode

Paper mode is required early, not late.
- It is the safest bridge between manual analysis and any future automation.

Paper mode capabilities
- virtual entry in either direction,
- partial scale in,
- partial scale out,
- fee accounting,
- slippage approximation,
- funding accrual,
- event log with timestamps and reasons,
- mark-to-market PnL,
- realized / unrealized split.

Paper fill model v1
- entry price uses:
  - `ask` for long leg,
  - `bid` for short leg,
  - plus configurable slippage penalty.
- exit price uses:
  - `bid` for closing long,
  - `ask` for closing short,
  - plus configurable slippage penalty.
- funding accrual:
  - based on actual settlement schedule per exchange,
  - direction-aware,
  - logged as discrete events.

Paper mode must support
- "what if I entered here?"
- "what if I exited partially here?"
- "what if I added 25% when spread widened more?"

Why paper mode matters now
- it gives us realistic decision review before touching live automation.
- it creates labeled examples for later threshold tuning.

## 11. Export Layer

Exports are a first-class requirement because the user wants analysis in normal ChatGPT web.

### 11.1 JSON export types

`decision packet`
- compact object for one analysis moment:
  - symbol,
  - pair,
  - direction,
  - timestamp,
  - decision_phase,
  - raw current market snapshot,
  - derived spread/funding/OI metrics,
  - economics,
  - action,
  - reasons,
  - score components,
  - data quality.

`position packet`
- state of one held or paper position at a selected timestamp.
- Must include:
  - position_type (`paper` or `real_manual`)
  - decision_phase
  - minutes_to_next_funding
  - latest recommended action
  - latest reasons

`timeline packet`
- compressed series around a decision window:
  - raw market data,
  - derived features,
  - decision markers.

### 11.2 CSV / Parquet exports
- CSV:
  - human inspection and spreadsheet use.
- Parquet:
  - larger timeline exports for external analysis and replay.

### 11.3 Export design rule
- exported JSON must be small enough for ChatGPT web.
- therefore every export must support:
  - full export,
  - compact export,
  - compact-with-reasons export.

## 12. Backtest / Replay Hooks

Replay is mandatory if we want to improve rules later.

Required capability
- select a symbol, pair, direction, and time range,
- replay stored raw timeline,
- recompute features,
- recompute decision outputs with a chosen feature/rule version,
- compare against previously stored decision outcome.

Replay inputs
- raw discovery snapshots,
- raw focus snapshots if available,
- funding history,
- OI history,
- optional paper events.

Replay outputs
- feature timeline,
- action timeline,
- score timeline,
- outcome summary.

Important design note
- replay must run from stored raw inputs, not only from already-derived tables.
- otherwise rule changes cannot be audited cleanly.

## 13. UI / Workflow Plan

### 13.1 Manual analysis page (`/coin`)

The page should evolve from "on-demand diagnostics" into a real analysis console.

Blocks to show
- current pair selector and direction selector,
- live exact spread block,
- spread history chart:
  - discovery history,
  - focus history if available,
  - quality markers,
  - direction overlays,
- funding panel,
- OI panel,
- liquidity / execution panel,
- economics panel,
- decision panel,
- save/export controls,
- paper actions:
  - simulate entry,
  - partial add,
  - partial exit,
  - close paper position.

Decision panel should make timing explicit
- show:
  - current phase of interval,
  - minutes to next funding / next boundary,
  - whether we are inside the main `20-15m` review window,
  - whether the current recommendation is a normal scheduled review or an early imbalance/emergency reaction.

### 13.2 Tracked symbols mode
- user can mark a symbol as:
  - `watch`,
  - `collect high-frequency history`,
  - `paper-evaluate`.
- This list is manual and operator-controlled.
- It is not automatically populated from the entire Coinglass feed in phase 1.

### 13.3 Position watcher mode
- active paper positions and active real/manual positions are watched by the same analysis framework.
- the collector should automatically intensify sampling for held pairs.
- UI must use separate blocks, not just labels:
  - `Paper Positions`
  - `Real / Manual Positions`
- Both blocks should show the same core fields so decisions are comparable, but they should never be visually mixed.
- UI and exports should clearly distinguish:
  - `paper position`,
  - `real/manual position`.
- Each position row/card should show:
  - current recommended action,
  - current decision phase,
  - minutes to next funding,
  - last decision timestamp,
  - whether the last decision later proved correct or incorrect once enough outcome data exists.

## 14. Phased Delivery Plan

### Phase 0: foundation freeze
- Finalize:
  - DB choice (`state/coin_analysis.db`)
  - schema v1
  - feature set v1
  - reason code list v1
  - retention plan
- Deliverable:
  - stable technical contract before implementation spreads across files.

### Phase 1: instrument registry + universe intersection
- Build registry refresh for Binance and KuCoin.
- Build symbol normalization and pair generation.
- Persist shared tradable universe.
- Acceptance criteria:
  - module can answer "is symbol supported on both exchanges?"
  - mapping and contract specs are stored locally.

### Phase 2: on-demand symbol analysis + focus collector
- Implement tracked-symbol / tracked-position focus mode.
- Prefer WS for BBO / mark updates.
- Add freshness/staleness flags.
- Acceptance criteria:
  - when user opens `/coin/BTC`, focus history starts immediately and can be reused by later analysis and paper monitoring.

### Phase 3: funding / OI history
- Build dedicated funding and OI collectors.
- Persist current + historical data.
- Add exchange-specific provenance flags.
- Acceptance criteria:
  - pair analysis can use stored funding/OI history instead of purely on-demand fetches.

### Phase 4: feature engine v1
- Implement spread, funding, OI, liquidity, and economics features.
- Store feature snapshots with `feature_set_version=v1`.
- Acceptance criteria:
  - feature snapshot is reproducible and exportable for a given timestamp.

### Phase 5: decision engine v1
- Implement candidate and position rules.
- Add reason codes and score decomposition.
- Acceptance criteria:
  - every action includes reasons and directional context.

### Phase 6: paper mode
- Implement paper positions, events, funding accrual, partial scaling.
- Acceptance criteria:
  - the operator can run analysis, press paper enter, and then monitor that paper position in real time with exports and replay.

### Phase 7: `/coin` UI rebuild
- Replace current diagnostic layout with stored-history-first UI.
- Add save decision, export, and paper controls.
- Acceptance criteria:
  - user can inspect, save, export, and paper-trade from one page.

### Phase 8: unified position watcher + decision journal
- real/manual positions and paper positions feed the same analysis surface.
- Save operator decisions and later evaluate outcomes.
- Acceptance criteria:
  - we can compare recommendation vs realized next `15m` / `1h` / `next funding` for both paper and real positions.
  - we can review not just PnL result, but also:
    - whether the decision timing was good,
    - whether funding logic was right,
    - whether spread logic was right,
    - whether waiting another `15m` would have been better or worse.

### Phase 9: advisory watcher
- Background evaluation for tracked / held positions.
- No live execution yet.
- Acceptance criteria:
  - module emits advisory actions with logs but does not trade.

### Phase 10: optional shortlist discovery mode
- Only if later needed, add background collection for a manual shortlist or a narrowly filtered candidate set.
- Explicitly not the first milestone.

### Phase 11: future automation gate
- Reassess whether any sub-scope is safe enough for:
  - paper auto-entry,
  - paper auto-exit,
  - much later, real auto-entry/auto-exit.
- Explicit requirement:
  - no live automation before we have a decision journal and outcome review set.

## 15. Testing Plan

Unit tests
- symbol normalization / mapping
- spread formulas
- funding normalization by interval
- OI alignment
- economics calculations
- decision rule outputs and reasons
- paper fill and funding accrual

Integration tests
- registry refresh
- batch discovery insert / rollup
- tracked symbol focus collection
- `/coin` API response from stored data
- export formats

Replay tests
- identical raw input must reproduce identical feature output for a given version
- decision rules must be deterministic for stored inputs

Data-quality tests
- stale quote detection
- missing funding interval behavior
- symbol mismatch handling
- bootstrap proxy must be visibly flagged

## 16. Observability Requirements

Every collector job should log
- started / completed / failed
- symbols processed
- rows inserted
- latency
- warning counts

Every decision should log
- action
- direction
- top reason codes
- key economics numbers
- data quality summary

Every export should log
- who/what requested it
- range
- row count
- export type

## 17. What Can Be Reused From Current Code

Safe to reuse
- exchange adapter registry and normalization helpers,
- existing Binance and KuCoin adapters as starting points,
- existing funding helper utilities,
- SQLite persistence patterns from `utils/cache_db.py`,
- `/coin` route and template as the entry point,
- existing market data / WS infrastructure for tracked-symbol live data.

Must be replaced or bypassed
- candle-close spread as the primary spread history source,
- in-memory-only coin analysis cache as the main storage,
- current single advisory `bot_logic` score,
- current core exchange scope `binance + okx`.

## 18. Immediate Next Build Order

Recommended next implementation order
1. Create `state/coin_analysis.db` schema.
2. Build instrument registry + shared universe table.
3. Add on-demand symbol session bootstrap and focus collector for clicked/tracked symbols.
4. Repoint `/coin` to stored symbol history instead of candle approximation.
5. Add funding/OI persistence and feature snapshots.
6. Add candidate rule engine.
7. Add paper mode and decision journal.
8. Add unified watcher for paper + real positions.
9. Add position rule engine.
10. Only later decide whether shortlist discovery mode is needed.

## 19. Remaining Clarifications To Confirm Before Coding Starts

These are the only real product questions still worth confirming up front.

1. Default discovery universe
- Proposed default for phase 1:
  - no always-on full-universe discovery.
- Proposed working mode:
  - only symbols explicitly opened, saved, or held become actively collected.
- If discovery is later added, start from a manual shortlist, not from the entire external candidate feed.

2. Focus collector lifecycle
- Proposed default:
  - opening `/coin/{symbol}` starts focus collection for `30m`;
  - tracked symbols keep it on indefinitely;
  - held positions keep it on while position exists.

3. Paper sizing policy for v1
- Proposed default:
  - allow fixed notional and percentage-of-reference-size modes;
  - use exact sizing steps for signal traceability in v1, not ranges inside one signal.

4. Partial entry / exit strategy semantics
- Proposed default:
  - `ENTRY_SMALL` = open `25%` of reference size,
  - `ENTRY_STRONG` = open `50%` of reference size,
  - `PARTIAL_EXIT` = close `25%` of current position size,
  - `ADD_SMALL` = add `25%` of reference size,
  - `FULL_EXIT` = close `100%`.
- Reason:
  - fixed action sizes make the decision journal, replay, and paper-result comparison much easier to interpret.

5. Export UX
- Proposed default:
  - export buttons on `/coin`,
  - plus API endpoints for compact JSON and timeline CSV/Parquet.

Notes
- The module is successful only if a future session can inspect any saved decision and answer:
  - what the market looked like,
  - what the module believed,
  - why it said enter / hold / exit / add,
  - and whether that helped.
- Profit comes later; traceability comes first.

## 20. Implementation Roadmap (Actionable)

This section is the practical build order.
- Goal:
  - provide a path that can be implemented step by step without guessing architecture every day.
- Principle:
  - every milestone should leave the repo in a runnable, inspectable state.
- Delivery style:
  - build vertical slices, not isolated fragments.

### Milestone 0: Freeze Contracts And Scaffolding

Objective
- Create the module skeleton and lock the technical contracts before feature logic spreads across the repo.

Tasks
- Create module directories:
  - `analysis_registry/`
  - `analysis_collectors/`
  - `analysis_storage/`
  - `analysis_features/`
  - `analysis_decisions/`
  - `analysis_paper/`
- Add a central constants/config module for:
  - `feature_set_version`
  - decision phases
  - default cadences
  - retention settings
  - position type enums
- Add a central reason-code registry.
- Add a central data-quality/provenance enum set.

Files likely to be created first
- `analysis_storage/coin_db.py`
- `analysis_registry/coin_instruments.py`
- `analysis_decisions/reason_codes.py`
- `analysis_decisions/constants.py`

Done when
- imports are stable,
- module names are fixed,
- enums/constants exist,
- future work can build on them without renaming churn.

### Milestone 1: Database Schema And Storage Helpers

Objective
- Make persistence real first.

Tasks
- Implement schema creation for `state/coin_analysis.db`.
- Add:
  - DB connection helper,
  - schema version table,
  - WAL mode,
  - indexes,
  - retention helpers.
- Implement insert/upsert/query helpers for:
  - instruments,
  - pairs,
  - focus market snapshots,
  - funding history,
  - OI history,
  - feature snapshots,
  - decisions,
  - outcomes,
  - paper positions,
  - paper events.

Tests
- schema creation test,
- upsert idempotency,
- query ordering by `ts_ms`,
- retention pruning test.

Done when
- a test can create the DB from scratch,
- insert rows,
- read them back,
- and rebuild the same state after restart.

### Milestone 2: Instrument Registry And Pair Resolution

Objective
- Make symbol validation and mapping deterministic.

Tasks
- Reuse current Binance/KuCoin adapter mapping logic.
- Build registry refresh:
  - load Binance metadata,
  - load KuCoin metadata,
  - normalize into canonical symbols,
  - build shared tradable pairs.
- Persist:
  - contract size,
  - tick size,
  - qty step,
  - min qty,
  - min notional,
  - funding interval,
  - tradability flags.
- Add lookup helpers:
  - `resolve_symbol(symbol)`
  - `resolve_pair(symbol, left_exchange, right_exchange)`
  - `is_supported_on_pair(symbol, "binance", "kucoin")`

Tests
- `BTC`/`XBT` normalization,
- unsupported symbol handling,
- duplicated suffix handling,
- registry refresh idempotency.

Done when
- the module can reliably answer:
  - "is this symbol tradable on both exchanges?"
  - "what exact exchange symbols and constraints apply?"

### Milestone 3: Symbol Session Bootstrap

Objective
- Support the actual user flow:
  - open symbol,
  - run analysis,
  - start local tracking.

Tasks
- Add a symbol-session service:
  - opening `/coin/{symbol}` creates or refreshes a focus session,
  - session TTL defaults to `30m`,
  - reopening extends the TTL.
- Persist session state in DB or lightweight state table.
- Add API/service helpers:
  - `start_symbol_session(symbol)`
  - `extend_symbol_session(symbol)`
  - `stop_symbol_session(symbol)`
  - `list_active_symbol_sessions()`

Likely integration points
- `webapp/app.py`
- `webapp/services.py`
- new service module under `analysis_collectors/` or `analysis_storage/`

Done when
- opening a symbol in the UI has a durable backend effect,
- and the backend knows which symbols are actively tracked right now.

### Milestone 4: Focus Collector (Exact Local History)

Objective
- Start collecting exact local spread history for selected symbols.

Tasks
- Build a collector loop that reads active sessions + held positions.
- For each tracked symbol:
  - fetch BBO,
  - fetch mark/index/funding-now,
  - compute current mid and basic spread rows,
  - store focus snapshots.
- Prefer WS where existing infra already supports it.
- Use REST fallback for gaps or unsupported streams.
- Attach freshness/staleness metadata.

Minimum viable version
- `5s` REST-only fallback is acceptable initially if it gets the pipeline working fast.
- Then tighten to WS/`1s-2s` for tracked symbols.

Tests
- collector loop on mock adapters,
- stale quote marking,
- session expiry behavior,
- restart recovery.

Done when
- selecting a symbol causes real rows to appear in `ca_market_snapshots_focus`,
- and those rows survive restart.

### Milestone 5: Historical Bootstrap Loaders

Objective
- Make first-time analysis useful before enough local history accumulates.

Tasks
- Build loaders for:
  - funding history,
  - OI history,
  - Binance mark/index/premium proxy history,
  - KuCoin historical context where available.
- Tag every historical row with provenance.
- Expose merged queries:
  - `load_symbol_context(symbol, horizon)`
  - `load_focus_history(symbol, range)`
  - `load_bootstrap_history(symbol, range)`

Tests
- provenance tagging,
- missing-source fallback,
- mixed local + bootstrap query behavior.

Done when
- first analysis on a fresh symbol can still render informative context.

### Milestone 6: Feature Engine v1

Objective
- Convert raw rows into reusable, explainable feature snapshots.

Tasks
- Implement spread feature calculators.
- Implement funding feature calculators.
- Implement OI feature calculators.
- Implement liquidity/economics calculators.
- Implement time-window features:
  - minutes to boundary,
  - `decision_phase`,
  - 20m/15m/5m drift and stress features.
- Store feature snapshots with `feature_set_version`.

Suggested split
- `analysis_features/spread.py`
- `analysis_features/funding.py`
- `analysis_features/oi.py`
- `analysis_features/liquidity.py`
- `analysis_features/economics.py`
- `analysis_features/position_state.py`

Tests
- deterministic feature snapshot generation,
- z-score/percentile math,
- time-window classification,
- direction-specific economics correctness.

Done when
- for any tracked timestamp the system can produce one stable feature snapshot and save it.

### Milestone 7: Candidate Decision Engine

Objective
- Make `/coin` return a real explainable recommendation.

Tasks
- Implement:
  - `NO_TRADE`
  - `ENTRY_SMALL`
  - `ENTRY_STRONG`
- Evaluate both directions for the pair.
- Return:
  - action,
  - direction,
  - decision phase,
  - scores,
  - reasons,
  - blocking conditions.
- Save the decision to the journal.

Tests
- no-trade on bad liquidity,
- no-trade on bad funding,
- strong entry on favorable setup,
- downgrade inside unstable pre-boundary move.

Done when
- the manual analysis endpoint can generate and persist a structured candidate decision.

### Milestone 8: Paper Entry Flow

Objective
- Turn the analysis screen into a paper-trade workflow.

Tasks
- Add API action:
  - `paper enter`
- Create:
  - paper position,
  - paper legs,
  - paper event log,
  - entry decision link.
- Position should automatically become tracked by the focus collector.
- Paper fills should use the same market snapshot model as the collector.

Likely integration points
- `webapp/app.py`
- `webapp/services.py`
- `analysis_paper/portfolio.py`
- `analysis_paper/fills.py`

Tests
- paper position creation,
- correct side mapping,
- fee/slippage accounting,
- focus tracking auto-start.

Done when
- the user can analyze a symbol and create a paper position from that same flow.

### Milestone 9: Unified Position Watcher

Objective
- Monitor paper and real/manual positions with the same decision engine.

Tasks
- Build a watcher loop that reads:
  - paper positions,
  - real/manual positions.
- Keep them separate in UI, but evaluate them through the same feature pipeline.
- Generate position-state feature snapshots.
- Generate:
  - `HOLD`
  - `PARTIAL_EXIT`
  - `FULL_EXIT`
  - `ADD_SMALL`
  - `ADD_BLOCKED`
- Save every position decision to the journal.

Critical rule
- paper and real/manual must be separate surfaces in UI/export,
- but identical decision semantics should apply to both.

Tests
- watcher reacts to paper position,
- watcher reacts to real/manual position,
- decision phase classification near funding boundary,
- emergency vs scheduled decision distinction.

Done when
- both types of positions receive live recommendations in the backend.

### Milestone 10: Outcome Evaluation And Correctness Review

Objective
- Make the journal useful for learning, not just logging.

Tasks
- Build an outcome evaluator job.
- For each decision, compute later outcomes for:
  - `15m`
  - `1h`
  - `4h`
  - `to_next_funding`
  - `to_exit`
- Evaluate decomposed correctness:
  - direction correctness,
  - timing correctness,
  - funding correctness,
  - spread correctness,
  - size appropriateness.
- Add counterfactual fields:
  - waiting `15m`,
  - exiting `15m` earlier.

Tests
- horizon calculation correctness,
- delayed evaluation after enough time passes,
- decomposition logic sanity.

Done when
- a saved decision can later be reviewed as "what happened" and "was the timing right".

### Milestone 11: UI Rebuild

Objective
- Expose the system in a way the operator can actually use.

Tasks
- Rebuild `/coin` around stored history and persisted decisions.
- Add:
  - live metrics,
  - stored history chart,
  - decision panel,
  - paper actions,
  - export actions.
- Add separate dashboard blocks:
  - `Paper Positions`
  - `Real / Manual Positions`
- Each row/card must show:
  - recommended action,
  - decision phase,
  - minutes to next funding,
  - last decision time,
  - latest correctness review when available.

Tests
- API payload shape,
- UI rendering with mixed paper + real positions,
- correct separation of blocks.

Done when
- the operator can inspect, act, and review from the UI without reading raw logs.

### Milestone 12: Export And Replay

Objective
- Make the module externally analyzable.

Tasks
- Add compact JSON export.
- Add CSV/Parquet timeline export.
- Add replay endpoint/service using stored raw inputs.
- Ensure exported packets include:
  - decision phase,
  - reasons,
  - provenance,
  - correctness review when available.

Done when
- a single symbol case can be exported and reviewed in normal ChatGPT web.

### Recommended First Vertical Slice

If we want the fastest path to something truly usable, build this exact slice first:
1. DB schema.
2. Instrument registry.
3. Symbol session bootstrap.
4. Focus collector for one selected symbol.
5. Minimal spread/funding feature snapshot.
6. Minimal candidate decision (`NO_TRADE` / `ENTRY_SMALL` / `ENTRY_STRONG`).
7. `paper enter`.
8. Paper position watcher.

Why this slice first
- it already matches the user's real workflow,
- it creates persistent data immediately,
- it gives a usable feedback loop fast,
- and everything later extends it rather than replacing it.

### Recommended Technical Order Inside The Repo

Backend-first order
1. `analysis_storage/coin_db.py`
2. `analysis_registry/coin_instruments.py`
3. focus session manager
4. focus collector
5. feature engine
6. candidate decisions
7. paper engine
8. unified watcher
9. UI/API integration
10. outcome evaluator
11. export/replay

### Definition Of A "First Working Version"

The first version is good enough when all of these are true:
- the user can open a symbol and start persistent tracking,
- the system can produce and save a candidate decision,
- the user can create a paper position,
- the watcher keeps analyzing that paper position,
- the UI shows paper and real/manual positions separately,
- every decision is timestamped, reasoned, and later reviewable.
