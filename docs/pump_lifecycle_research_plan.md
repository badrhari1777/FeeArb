# Pump Lifecycle Research Plan

Date: 2026-07-12

Purpose: define the next research track for full pump lifecycle analysis, including long squeeze entries/exits and post-pump short entries. This document is the resume point for future sessions.

## Objective

Build a research and shadow/paper pipeline that treats a pump as a lifecycle, not as a single price spike.

Lifecycle states:

```text
WATCH -> ACCUMULATION -> IGNITION -> SHORT_SQUEEZE -> BLOW_OFF -> DISTRIBUTION -> BREAKDOWN -> LONG_LIQUIDATION -> RESET
```

The system must produce separate scores:

- `squeeze_continuation_score`: whether the move still favors long/squeeze continuation.
- `pump_exhaustion_score`: whether the move has shifted toward distribution/breakdown and may allow a short.
- `security_dislocation_score`: whether automatic trading must be blocked due to abnormal market infrastructure or token risk.
- `data_quality_score`: whether the available data is good enough for a signal.

Do not collapse these into one `pump_score`.

## Current Status

- Existing Bybit pump-short research already covers post-pump short rules, pullback tiers, policy portfolios, dynamic compounding, and shadow/paper strategies.
- Best current short class remains confirmed pullback after pump, not immediate shorting.
- New work starts by adding lifecycle event replay on top of existing Bybit historical samples.
- First implementation target: rule-based lifecycle catalog and scores using available Bybit 1h candles, funding, OI, long/short ratio, and BTC context when available.

## Phase 1 Data Scope

Use existing Bybit extended dataset first:

- Input: `data/research/bybit_pump_short_extended/symbol_samples.jsonl`
- Candles: 1h perpetual OHLCV.
- Derivatives: funding history, open interest, long/short ratio.
- Instrument metadata: launch time and contract metadata already stored in samples.

Add if missing:

- BTCUSDT/ETHUSDT 1h context for relative-strength features.
- Normalized funding by interval where interval metadata is available.
- Volume/turnover z-score from 1h klines.

Do not start by collecting full tick history for all symbols. Lower timeframe data should be event-window only.

## Phase 2 Event-Window Data

For events that pass initial filters, collect focused windows:

- 15m candles from `-7d` to `+14d` around event.
- 5m or 1m candles only for strongest events.
- Mark/index/premium where available.
- Live Bybit `allLiquidation` for future events.
- Orderbook snapshots for shadow/paper fill quality.
- Binance spot/perp confirmation as external reference.

Current implementation:

- `analysis_collectors/bybit_event_window.py` collects Bybit event windows from `lifecycle_events.csv`.
- `scripts/bybit_pump_event_windows.py` is the CLI. Current safe first command:

```powershell
python scripts/bybit_pump_event_windows.py --output-dir data\research\bybit_pump_event_windows --max-events 5 --min-pump-pct 150 --interval 15 --sleep-sec 0.2 --no-resume
```

- The first small run collected `5` unique strong-pump episodes with `15m` data, `-72h/+336h` windows, `1,633` points per event for candles, premium-index, mark-price, index-price, and OI, plus funding history.
- The full 2026-07-12 run expanded this to all `284` unique pump episodes with `trigger_pump_pct >= 50%`; resume metadata shows `events_selected=284`, `events_skipped=284`, `events_failed=0` after completion. Generated files are in `data/research/bybit_pump_event_windows`.
- Bybit premium/index/mark endpoints are available through public V5 market data, so premium-index can be treated as a first-class research feature rather than inferred from mark/index only.

## Phase 3 Long Strategy Research

Create long-side research analogous to existing short research.

Entry candidates:

- `ignition_breakout`: breakout from local range on abnormal volume.
- `breakout_retest`: held retest after breakout.
- `funding_squeeze`: price rising while funding remains negative and OI grows.
- `liquidation_cascade`: first short liquidations appear while OI remains elevated.
- `spot_led`: spot confirms or leads perpetual, with limited perp premium.
- `btc_relative`: coin strongly outperforms BTC/ETH.

Exit candidates:

- Partial take profit at `+20%`, `+30%`, `+50%`, and larger pump buckets.
- Exit when funding flips from negative to positive.
- Exit when OI drops by `15-30%` from local high.
- Exit on spot/perp divergence.
- Trailing stop under local lows.
- Time stop if squeeze does not develop in `1h`, `3h`, or `6h`.

Risk assumptions for early tests:

- No averaging down on long.
- 1-3x leverage in simulation.
- Partial exits are mandatory.
- One high-risk pump position at a time in live/paper assumptions.

Funding/premium hypothesis:

- New module: `analysis_features/pump_funding_premium_window_research.py`.
- New CLI:

```powershell
python scripts/pump_funding_premium_window_research.py --input data\research\bybit_pump_event_windows\event_windows.jsonl --output-dir data\research\pump_funding_premium_window_research
```

- First tiny sample result: `5` event windows, `45` outcomes, `12` strategy rows.
- Best early row on this tiny sample: `deep_discount_survives + tp30_sl25_hold72_fundrelief`, `4` trades, `75%` win, average net `+18.61%`, median net `+29.82%`, average long funding credit `+2.37%`.
- This is not statistically enough, but it is directionally important: using negative premium/funding as the entry thesis can produce a cleaner small-sample read than the previous broad 1h continuation grid.
- Longer holds with wider stops are not automatically better. In the first sample, TP30/SL25 beat TP60/SL35 and TP100/SL50 because several events paid funding but still decayed or time-stopped.
- Full-sample 15m result: `284` event windows, `165` events with at least one premium/funding entry, `1,620` outcomes, `12` strategy rows, `288` regression rows, and `24` factor-bucket rows in `data/research/pump_funding_premium_window_research`.
- Best current full-sample row by median remains `deep_discount_survives + tp30_sl25_hold72_fundrelief`: `150` trades, `58.0%` win, average net `+7.61%`, median net `+29.82%`, min `-25.20%`, max `+37.02%`, average long funding credit `+1.15%`.
- `premium_relief + tp30_sl25_hold72_fundrelief` is close: `143` trades, `57.34%` win, average net `+7.29%`, median net `+29.82%`.
- Wider/farther exits have higher average in some rows but weaker median: `tp60/sl35` and `tp100/sl50` are more right-tail dependent and should not replace TP30 without additional filters.
- Regression read: available features still explain net weakly on all rows (`r2 ~0.027`) but explain stop/risk better. Positive 4h OI change is the clearest favorable factor for win/stop risk; very large accumulated long funding credit often means the trade sat in stress longer, not automatically that the setup is better.
- Top-strategy buckets suggest better quality when entry is immediate (`entry_wait_h=0`), OI is high/rising, volume-z is mid/high, and long funding credit is small-to-moderate. Very high funding credit bucket has worse median because it often reflects a prolonged drawdown, not free edge.
- Filter and portfolio layer added after the full run. Outputs: `premium_filter_sweep_summary.csv`, `premium_portfolio_summary.csv`, and `premium_portfolio_trades.csv`.
- Filter sweep produced `180` rows. Among filters with at least `50` trades and at least `15` test trades, the most practical top rows stay in the `TP30/SL25` family:
  - `deep_discount_survives + tp30_sl25_hold72_fundrelief + premium_not_toxic_oi_wait`: `52` trades, `69.23%` win, average net `+13.20%`, median `+29.82%`, test average `+18.15%`.
  - `deep_discount_oi + tp30_sl25_hold72_fundrelief + premium_not_toxic_oi_wait`: `50` trades, `68.00%` win, average net `+12.57%`, median `+29.82%`, test average `+17.37%`.
  - Broad `wait_le_3h` is less selective but larger: `102` trades, `62.75%` win, average net `+10.11%`, median `+29.82%`.
- Portfolio replay uses fixed `$3000` starting capital, `2x` leverage, and `1..5` slots. It is intentionally simpler than short-ladder capital math because long entries do not average down in this model.
- Practical 4-slot read under `worst_trade_pct >= -60%`: `deep_discount_survives + tp30_sl25_hold72_fundrelief + wait_le_3h`, `87` trades, ROI `+398.56%`, risk-adjusted ROI `+331.42%`, win `60.92%`, max drawdown `60.85%`, worst levered trade `-50.38%`.
- More selective 4-slot read: `deep_discount_survives + tp30_sl25_hold72_fundrelief + premium_not_toxic_oi_wait`, `43` trades, ROI `+288.77%`, risk-adjusted ROI `+257.29%`, win `69.77%`, max drawdown `25.19%`, worst levered trade `-50.38%`.
- Current best candidate for next deeper work is not the raw highest ROI row. Use `TP30/SL25`, avoid toxic extreme premium below about `-5%`, require OI not falling, and prefer entry inside `0-3h` after trigger. Next step: add 5m windows for the filtered candidate events and produce per-coin event pages before shadow.
- 5m candidate pass completed for the `52` `premium_not_toxic_oi_wait` events. Command used `scripts/bybit_pump_event_windows.py` with input `premium_not_toxic_oi_wait_events.csv`, output `data/research/bybit_pump_event_windows_5m_candidates`, interval `5`, `-72h/+336h`; result `52/52` collected, `0` failed, `2,441` Bybit requests.
- 5m premium/funding rerun output: `data/research/pump_funding_premium_window_research_5m_candidates`. It produced `52` samples, `600` outcomes, `180` filter rows, `900` portfolio rows, and `20,555` portfolio trades.
- 5m changed the read materially versus 15m for the filtered set: target `deep_discount_survives + TP30/SL25` produced `52` trades, `78.85%` win, average net `+18.96%`, median `+29.82%`, min `-25.19%`, max `+44.01%`.
- 5m practical 4-slot replay for `deep_discount_survives + TP30/SL25 + premium_not_toxic_oi_wait`: `44` trades, ROI `+481.52%`, risk-adjusted ROI `+462.63%`, win `84.09%`, max drawdown `12.59%`, worst levered trade `-50.38%`.
- Per-event HTML pages were generated by `analysis_features/pump_premium_event_pages.py` and `scripts/pump_premium_event_pages.py` into `data/research/pump_premium_event_pages`: `52` event pages plus `event_page_summary.csv`. Diagnosis counts: `40` clean discount-squeeze TPs, `10` failed discount absorption, `1` funding/partial recovery win, and `1` late-entry decay.
- New interpretation: the stricter premium/funding long candidate survives 5m validation better than expected. The next step is not broader parameter mining; it is reviewing the `10` failed absorption pages and adding vetoes for those failure modes before shadow/paper.
- Failed-absorption review added a veto sweep in `analysis_features/pump_funding_premium_window_research.py`. The 5m candidate report now has `312` filter rows, `1,560` portfolio rows, and `28,953` portfolio trade rows.
- Target row `deep_discount_survives + TP30/SL25` now has a clear selectivity curve:
  - `premium_not_toxic_oi_wait`: `51` filter trades, `80.39%` win, average net `+19.83%`; 4-slot replay `44` trades, ROI `+481.52%`, risk-adjusted `+462.63%`, `7` losing portfolio trades.
  - `veto_wait_le_30m`: `41` filter trades, `85.37%` win, average net `+22.27%`; 4-slot replay `35` trades, ROI `+419.90%`, risk-adjusted `+401.02%`, `4` losing portfolio trades.
  - `veto_wait30_oi10`: `30` filter trades, `90.00%` win, average net `+25.58%`; 4-slot replay `26` trades, ROI `+349.39%`, risk-adjusted `+330.50%`, `2` losing portfolio trades.
  - `veto_high_confidence_midpremium`: entry wait `<=30m`, OI `>=20%`, volume z `>=1`, premium band `[-3.5%, -1.2%]`; `14` filter trades, `100%` win, average net `+30.52%`; 4-slot replay `13` trades, ROI `+197.56%`, no drawdown and no losing trades in this filtered 5m sample.
- Current interpretation after veto sweep: do not replace the main 4-slot candidate with a tiny strict filter yet. Use `premium_not_toxic_oi_wait` as the broad paper track, add `veto_wait30_oi10` as the cleaner control track, and add `veto_high_confidence_midpremium` as a high-confidence alert/paper track. The strict track is promising but too small to treat as live evidence.
- Broad long portfolio simulation report was added in `analysis_features/pump_long_portfolio_sim_report.py` with CLI `scripts/pump_long_portfolio_sim_report.py`; output: `data/research/pump_long_portfolio_sim_report_5m/index.html`. It sweeps `16,848` portfolios across entry rules, exit plans, filters, slots `1..6`, leverage `1x/2x/3x`, and sizing modes `split_initial`, `split_dynamic`, `fixed_750`. CSV outputs: `simulation_summary.csv`, `simulation_trades.csv`, `equity_points.csv`, and `slot_comparison.csv`.
- Slot conclusion from the 5m long sweep:
  - If capital is split by slot (`split_initial`), `1` slot often has the highest historical ROI because every rare signal uses the full `$3000`, but it concentrates risk: broad `premium_not_toxic_oi_wait` at `2x` gives `39` trades, ROI `+1627.87%`, risk-adjusted `+1520.21%`, max drawdown `82.47%`, worst trade `-50.38%`, and skips `5` slot-overlap trades.
  - `2` slots capture all broad-track trades in this dataset: broad `premium_not_toxic_oi_wait` at `2x` gives `44` trades, ROI `+963.04%`, risk-adjusted `+925.25%`, max drawdown `25.19%`, worst trade `-50.38%`.
  - `4` slots do not add trades versus `2` slots on the broad track; they only halve per-trade budget again: `44` trades, ROI `+481.52%`, risk-adjusted `+462.63%`, max drawdown `12.59%`.
  - With fixed `$750` per trade (`fixed_750`), broad track `2/3/4/5/6` slots are identical (`44` trades, ROI `+481.52%`), while `1` slot misses `5` trades and drops to `39` trades, ROI `+406.97%`. This isolates concurrency: historically the long strategy needs at most `2` slots, not `4`.
  - Cleaner `veto_wait30_oi10` and strict `veto_high_confidence_midpremium` do not need more than `1` slot in the current sample; slot count mainly controls position size, not opportunity capture.
- Practical interpretation: run paper with `2` long slots as the main setting, not `4`. Keep `4` only as a conservative sizing variant or if future broader data shows more overlapping events. Do not promote `1` full-capital slot to live just because historical ROI is larger; its concentration/drawdown profile is materially harsher.

## Phase 7 Combined Pump-Cycle Portfolio

Goal: move from separate short and long reports to one shared-capital paper model.

Implementation slice completed:

- New module: `analysis_features/pump_cycle_portfolio_report.py`.
- New CLI: `scripts/pump_cycle_portfolio_report.py`.
- New test: `tests/test_pump_cycle_portfolio_report.py`.
- Output: `data/research/pump_cycle_portfolio_report/index.html`.
- CSV outputs: `cycle_summary.csv`, `cycle_trades.csv`, `cycle_equity.csv`.
- Inputs:
  - Long candidates from `data/research/pump_funding_premium_window_research_5m_candidates/premium_long_outcomes.csv`.
  - Short candidates from `data/research/pump_short_dynamic_combo_report_3000_2024/dynamic_combo_trades.csv`.
- Current replay dimensions:
  - Long tracks: `long_broad`, `long_clean_oi`, `long_high_conf`.
  - Short tracks: `short_clean_p100_l3`, `short_aggr_p80_l3`, `short_clean_p100_l2`.
  - Allocations: `short_only_4`, `long_only_2`, `cycle_6_4s2l`, `cycle_5_4s1l`, `cycle_5_3s2l`, `cycle_6_4s2l_dynamic`, `cycle_6_4s2l_fixed750`.

Current key read using `long_broad + short_clean_p100_l3`:

- `short_only_4`: `97` trades, ROI `+555.73%`, risk-adjusted `+526.95%`, max drawdown `19.15%`, peak top-up `$716.15`, worst trade `-$574.39`.
- `long_only_2`: `44` trades, ROI `+963.04%`, risk-adjusted `+925.25%`, max drawdown `25.19%`, no top-up, worst trade `-$755.69`.
- `cycle_6_4s2l`: `133` trades (`92` short / `41` long), ROI `+663.08%`, risk-adjusted `+643.89%`, max drawdown `12.76%`, peak top-up `$477.43`, worst trade `-$382.93`, `8` same-symbol long/short conflicts skipped.
- `cycle_5_4s1l`: `128` trades (`92` short / `36` long), ROI `+736.05%`, risk-adjusted `+713.03%`, max drawdown `15.32%`, peak top-up `$572.92`, worst trade `-$459.51`, `12` long slot skips.
- `cycle_5_3s2l`: `123` trades (`82` short / `41` long), ROI `+738.61%`, risk-adjusted `+715.59%`, max drawdown `15.32%`, peak top-up `$572.92`, worst trade `-$459.51`, `12` short slot skips.
- `cycle_6_4s2l_fixed750`: `133` trades, ROI `+994.61%`, risk-adjusted `+965.83%`, but this is a reference mode because `$750 x 6` implies larger notional capacity than a strict `$3000 / 6` shared-capital split.
- `cycle_6_4s2l_dynamic`: very high historical ROI but huge compounding tails and rescue needs; keep as diagnostic only, not paper default.

Interpretation:

- For true shared `$3000` capital, `6 slots = 4 short + 2 long` is the cleaner paper default because it preserves the intended short capacity and both long slots while lowering drawdown and peak rescue top-up.
- `5 slots` can show higher ROI because each slot is larger, but it drops either long coverage (`4S/1L`) or short coverage (`3S/2L`) and increases stress.
- Therefore use `cycle_6_4s2l` as the primary combined paper controller baseline; track `cycle_5_4s1l` and `cycle_5_3s2l` as comparison rows, not defaults.

Next implementation step:

- Convert the historical combined model into an online paper/shadow controller:
  - one shared `pump_cycle_paper_positions.json`;
  - one event log `pump_cycle_paper_events.jsonl`;
  - shared capital/accounting section;
  - max `4` short positions, max `2` long positions, max `6` total positions;
  - same-symbol conflict veto between long and short cycle positions;
  - separate rescue/top-up accounting for short positions;
  - strategy labels for `short_clean_p100_l3`, `long_broad`, `long_clean_oi`, and `long_high_conf`.

## Phase 8 Online Cycle Paper / Shadow

Implementation slice completed:

- Existing `/pump-short-strategies` now runs both the previous multi-strategy short paper controller and a shared pump-cycle paper controller.
- Shared cycle state files:
  - `data/research/bybit_pump_short_shadow/pump_cycle_paper_positions.json`;
  - `data/research/bybit_pump_short_shadow/pump_cycle_paper_positions_latest.csv`;
  - `data/research/bybit_pump_short_shadow/pump_cycle_paper_events.jsonl`.
- Cycle portfolio accounting:
  - fixed paper capital `$3000`;
  - `6` shared slots total;
  - `4` max short slots;
  - `2` max long slots;
  - fixed `$500` slot budget;
  - short legs use isolated `3x`;
  - long entries use isolated `2x`;
  - short rescue/top-up is tracked separately from strategy equity.
- Current online short track:
  - `short_clean_p100_l3`;
  - pump `>=100%`;
  - pullback `pb20`;
  - funding prev 24h `> -1.0%`;
  - OI 24h `<= 50%`;
  - long ratio `0.45..0.65`;
  - `3` tapered short legs, `50%` spacing, `TP25`, max hold `336h`.
- Current online long tracks choose only one best candidate per symbol/event:
  - `long_high_conf`: premium `[-3.5%, -1.2%]`, OI 4h `>=20%`, volume z-score `>=1`, entry within `3h`;
  - `long_clean_oi`: premium `[-5%, -1%]`, OI 4h `>=10%`, entry within `3h`;
  - `long_broad`: premium `[-5%, -1%]`, OI 4h `>=0%`, entry within `3h`.
- Long exit model:
  - one entry leg;
  - `TP30`;
  - `SL25`;
  - max hold `72h`;
  - round-trip fee model `0.18%`.
- Conflict controls:
  - do not open long and short cycle positions on the same symbol at the same time;
  - do not reopen an already-papered `(side, symbol, event_id)` after it closes;
  - write `cycle_skip` events for capacity or opposite-side conflicts.
- `analysis_collectors/bybit_pump_short.py` now collects Bybit `premium-index-price-kline` data for shadow scans.
- `analysis_features/bybit_pump_short_shadow.py` now emits online long features: `premium_latest_pct`, `premium_min_24h_pct`, `premium_relief_1h_pct`, `oi_change_4h_pct`, and `volume_z_24h`.

Operational note:

- Use `/pump-short-strategies` and the `Start paper schedule` button to run the current paper/shadow cycle.
- This is not live trading and does not place orders.
- The online cycle controller is intentionally `1h` shadow-scan based. It is close enough for paper operations, but it is not identical to the historical `5m` candidate research. If the paper logs look promising, the next precision upgrade is a focused `5m` online collector for active pump windows.

## Phase 9 Active 5m Follow-up

Implementation slice completed:

- The broad scanner remains the source of truth for symbol discovery, but its default UI schedule interval is now `7200` seconds instead of `3600` seconds because full `1000`-symbol scans can take longer than one hour.
- After each broad shadow scan, a new active-window step selects only:
  - rows with `status != no_recent_pump`;
  - rows with an event/trigger field;
  - currently open cycle/strategy paper positions.
- For selected symbols, the active-window step collects `5m` Bybit data:
  - trade candles;
  - premium-index candles;
  - mark-price candles;
  - index-price candles;
  - open interest;
  - funding history.
- Active-window outputs:
  - `data/research/bybit_pump_short_shadow/pump_active_window_latest.json`;
  - `data/research/bybit_pump_short_shadow/pump_active_window_latest.csv`;
  - `data/research/bybit_pump_short_shadow/pump_active_window_samples.jsonl`;
  - `data/research/bybit_pump_short_shadow/pump_active_window_errors.jsonl`.
- `/pump-short-strategies` now shows an `Active 5m Follow-up` table with current price/return, premium, OI, volume z-score, mark-index basis, data counts, and a provisional `long_broad` probe based on 5m features.

Interpretation:

- This is the transition from a single broad 1h scanner to a two-tier scanner:
  - broad `1h` scanner for discovery;
  - focused `5m` scanner for watch/entry/open symbols.
- Current 5m follow-up is still paper/shadow only. It does not place orders and does not yet replace the 1h paper entry engine; it produces the richer data needed to do that safely.

## Phase 10 Candidate Paper PnL and Schedule Restore

Implementation slice completed:

- Candidate shadow tracks now have independent paper accounting in addition to ready/watch/blocked signal counts.
- Candidate paper output files:
  - `data/research/bybit_pump_short_shadow/pump_cycle_candidate_paper_positions.json`;
  - `data/research/bybit_pump_short_shadow/pump_cycle_candidate_paper_positions_latest.csv`;
  - `data/research/bybit_pump_short_shadow/pump_cycle_candidate_paper_events.jsonl`.
- Candidate tracks remain independent from the live-like `4 short + 2 long` portfolio:
  - they do not consume main cycle slots;
  - each short candidate track can use up to `4` independent paper slots;
  - each long candidate track can use up to `2` independent paper slots;
  - they use the same position, leg fill, TP/SL/time-stop, fee, top-up, and PnL accounting as cycle paper.
- `/pump-short-strategies` now surfaces candidate paper positions/PnL/top-up in the Candidate Shadow Tracks table.
- Shadow schedule state now persists to:
  - `data/research/bybit_pump_short_shadow/shadow_schedule_state.json`.
- If `shadow_schedule_state.json` has `enabled=true`, `BybitPumpShortLab` restores the saved paper schedule after backend restart with `run_immediately=false`, so restart recovery waits until the next scheduled cycle instead of launching a duplicate immediate scan.

Operational checks:

- Confirm `/api/pump-short/bybit/shadow/schedule/status` is `running` or `waiting` after restart when schedule should be active.
- If it is `idle`, use `/pump-short-strategies` -> `Start paper schedule`; the saved state should then keep future backend restarts recoverable.
- A healthy scan should update `shadow_metadata.json`, `shadow_scan_latest.csv`, strategy paper files, cycle paper files, candidate paper files, and active-window files with no `shadow_errors.jsonl` or `pump_active_window_errors.jsonl` entries.

## Phase 4 Short Strategy Upgrade

Keep current pullback-tier engine, but add lifecycle gating:

- Do not short in `SHORT_SQUEEZE`.
- Consider short only in `DISTRIBUTION` or `BREAKDOWN`.
- Require some combination of:
  - funding no longer strongly negative;
  - modest positive funding is preferable to extreme positive funding;
  - OI recovers without price making new highs;
  - lower high or VWAP/level breakdown;
  - pullback tier condition;
  - no security/dislocation flags.

Backtest whether this improves current `pb20/pb25` short candidates by reducing tail events and top-up needs.

## Phase 5 Reports

Primary report: `data/research/pump_lifecycle_research/index.html`

Required outputs:

- `lifecycle_events.csv`: one row per pump event/episode.
- `lifecycle_timeline.csv`: replay rows around each event with scores and state.
- `lifecycle_score_summary.csv`: score buckets and forward outcomes.
- `long_candidate_outcomes.csv`: long entry/exit simulations.
- `short_gate_comparison.csv`: old short rules vs lifecycle-gated short rules.
- Event HTML pages with score curves and key timestamps.

## Phase 6 Shadow/Paper

After historical replay:

- Add selected strategies to a new `/pump-lifecycle-strategies` page or extend `/pump-short-strategies`.
- Run long scanner as alert/paper first, not auto-live.
- Run short lifecycle gates in paper against the existing strategy paper engine.
- Track live bid/ask, orderbook depth, modeled fill, actual funding, latency, and rule changes.

## Near-Term Implementation Checklist

- [x] Capture this plan in `docs/pump_lifecycle_research_plan.md`.
- [x] Add `analysis_features/pump_lifecycle_research.py`.
- [x] Add `scripts/pump_lifecycle_research.py`.
- [x] Add tests for score calculation and lifecycle state classification.
- [x] Generate first Bybit 1h lifecycle report from existing extended data.
- [x] Add first separate long strategy grid.
- [ ] Add event-window collector only after the first report shows useful signal separation.
- [ ] Add BTC/ETH context to lifecycle and long reports.
- [x] Add portfolio layer for long strategies because per-event average can hide poor median/winrate.

## Implementation Notes

- Initial analyzer outputs event and timeline CSVs only; it does not simulate long or short trades yet.
- `security_dislocation_score` is explicitly a placeholder because the current Bybit 1h historical dataset does not include deposits/withdrawals, exchange protection modes, token migration, or on-chain concentration data.
- BTC-relative features are populated only if `BTCUSDT` is present in the input dataset; otherwise `data_quality_score` applies a small penalty.
- The first full run produced `368` events and `70,352` timeline rows in `data/research/pump_lifecycle_research`; `has_btc_context=false`, so the next data improvement is to add BTC/ETH context before over-reading continuation quality.
- Scores now include raw points plus normalized-by-available scores. Lifecycle classification uses the normalized score so missing BTC/spot/liquidation fields do not make `SHORT_SQUEEZE` unreachable.
- First long-side module: `analysis_features/pump_long_strategy_research.py` with CLI `scripts/pump_long_strategy_research.py`.
- First long report generated `data/research/pump_long_strategy_research` with `368` events and `2,128` outcomes. Top rows have positive average net but negative median net and low winrate, so the current long grid is research-only and needs stronger filters before any shadow/paper candidate.
- Long research now includes portfolio replay files: `long_portfolio_summary.csv` and `long_portfolio_trades.csv`. The 2026-07-12 run produced `100` portfolio rows and `5,789` selected portfolio trades using `$3000`, `2x`, and `1..5` slots.
- Best risk-adjusted long replay is still a fragile squeeze-capture shape: `breakout_volume_z3 + tp60_sl15_hold48`, especially `1` slot (`ROI 1219.48%`, risk-adjusted `844.59%`, `51` trades, `43.14%` win, worst trade `-30.4%` levered, max drawdown `$10,790.56`). This is not live-ready because most rows still lose and the edge comes from large right-tail winners.
- More practical slot views reduce tail concentration but keep weak hit-rate: the same strategy with `3..5` slots falls to `406.94%`, `305.21%`, and `244.16%` ROI while still stopping out about `59%` of trades.
- Long regression diagnostics now output `long_feature_regression.csv` and `long_factor_bucket_summary.csv`. Current ridge reads are weak for net prediction (`r2` about `0.03` on all rows), stronger for adverse excursion (`r2` about `0.22`), so these features are better as risk filters than as a standalone long alpha model.
- Current long regression interpretation: higher `entry_wait_h`, `oi_change_6h_pct`, `volume_z_24h`, and `exhaustion_score` generally reduce expected long net or increase stop probability; higher `pullback_from_high_pct` helps win probability in tighter TP/SL plans. Very high volume/continuation often means greater adverse excursion, not only stronger continuation.
- Next long-research step should be filter/gate sweeps, not paper: test caps like `entry_wait_h <= 1`, exhaustion low/mid, avoid extreme `volume_z_24h`, prefer controlled pullback after breakout, and add BTC/ETH context before trusting continuation labels.
