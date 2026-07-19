# Pump Short Console Research

Purpose: keep the pump-short research state in one console-first file so work can continue without the web UI.

## Current Focus

- Validate whether post-pump shorts should be held longer than 72h.
- Target coins that already showed pump behavior in the 90d Bybit dataset.
- Separate short-term mean reversion from long-hold decay/carry behavior.
- Treat UI as optional; primary workflow is scripts, CSV/JSON outputs, and this log.

## Baseline Dataset

- Source: `data/research/bybit_pump_short`.
- Window: 90 days.
- Symbols collected: 592.
- Pump trigger events: 227.
- Pump episodes: 69.
- Symbols with pump events: 57.
- Existing analyzer outputs: `data/research/bybit_pump_short_analysis`.

## Baseline Finding

- Immediate short after a pump is too risky: large adverse tails and repeated continuation pumps.
- Current best class is confirmed pullback after pump with OI not exploding, balanced long ratio, ladder entry, and controlled exit.
- Existing exit plans only test up to 336h, so they do not answer the long-decay runner hypothesis.

## Long-Hold Hypotheses To Test

1. After a major pump and confirmed pullback, many coins continue decaying for 30-180+ days.
2. A 72h forced full exit may close the short too early.
3. Partial cover after early profit plus a long runner may outperform full 72h exit.
4. Re-pumps after the initial dump are common but may be survivable with reduced size.
5. Funding may become neutral or positive for shorts after the crowd starts catching the falling knife.

## Planned Console Workflow

1. Build the target symbol universe from `pump_events.csv`.
2. Collect extended data for target pump symbols into `data/research/bybit_pump_short_extended`.
3. Add long-hold exit plans and repump diagnostics to the analyzer.
4. Run analysis into a separate output directory, not overwriting the 90d baseline.
5. Summarize candidate long-hold rules with risk, carry, re-pump, and robustness metrics.

## 2026-06-28 Console Session

- Target universe built from 90d pump events: 57 symbols.
- Extended collection started into `data/research/bybit_pump_short_extended` with `--lookback-days 3000`, effectively from listing for these symbols when Bybit data is available.
- Extended collection completed:
  - requested pump symbols: 57
  - collected current Bybit trading symbols: 56
  - missing from current trading instruments: `SOLVUSDT`
  - failed: 0
  - public requests: 5,684
- Analyzer now emits:
  - `long_hold_outcomes.csv`
  - `long_hold_rule_summary.csv`
  - `best_long_hold_rules.csv`
- Added long-hold plans up to 365d:
  - full hold: 30d, 90d, 180d, 365d
  - time partial cover + runner: 72h, 168h, 30d cover checkpoints
  - TP partial + runner: 25%/50% target variants
- Added diagnostics per long-hold row:
  - completed horizon and available hold hours
  - weighted funding during closed slices
  - full-period funding and positive-funding share
  - max re-pump against entry plus 30/50/100% re-pump counts
  - time to 50/70/90% decay from entry
- 90d smoke output: `data/research/bybit_pump_short_analysis_long_test`.
  - `long_hold_outcomes`: 49,569 rows.
  - `best_long_hold_rules`: 19 rows.
  - Important caveat: 90d data is insufficient for 90/180/365d conclusions and many long horizons are incomplete.
- Extended output: `data/research/bybit_pump_short_extended_analysis`.
  - symbols seen: 56
  - pump events: 368
  - pump episodes: 113
  - normal exit outcomes: 62,330
  - long-hold outcomes: 81,029
  - best long-hold rules: 55

## Extended Findings

- Long-hold decay exists in the median, but full-size long hold does not pass the current risk filter.
- Best long-hold rows are dominated by 30d and 90d holds after confirmed pullback with `oi0` filters.
- Example `pb10_oi0_lr_mid_ladder3_step_50 + long_full_30d`:
  - `n=304`, completed horizon `83.2%`, win `83.6%`, avg `12.8%`, median `31.0%`
  - but `p90 MAE=185.6%`, `p95 MAE=239.4%`, catastrophic 300%+ `3.0%`, 3x liquidation proxy `52.3%`
  - median 30% re-pump count `1.5`; p90 100% re-pump count `6`
- Example `pb10_oi0_lr_mid_ladder3_step_50 + long_full_90d`:
  - `n=304`, completed horizon `43.8%`, win `82.9%`, avg `14.5%`, median `51.4%`
  - but `p90 MAE=227.9%`, `p95 MAE=736.2%`, catastrophic 300%+ `6.6%`
  - funding turns mildly helpful/neutral in aggregate: median full-period funding about `+0.24%`, positive-funding share about `90.8%`
- No long-hold rule with `n>=100` and completed horizon `>=40%` had `p90 MAE < 150%` with zero catastrophic 300%+ events.
- The stronger live-candidate class remains shorter controlled exits:
  - `pb20_oi50_lr_mid_ladder3_step_50 + tp25_full_168`
  - `n=318`, win `81.1%`, avg `14.25%`, median `24.57%`, `p90 MAE=49.57%`, `p95 MAE=53.24%`, catastrophic 300%+ `0%`, anti-overfit `robust_candidate`
- Interpretation:
  - The user's long-decay thesis is directionally supported by medians and funding behavior.
  - Full-size runner is too exposed to repeated post-pump spikes.
  - Next research step should test small runner sizing and exposure-adjusted risk, e.g. take profit/cover most exposure early and keep only 10-25% runner.

## 2026-06-28 Ladder/Grid Research

- Added a separate console research module and CLI:
  - `analysis_features/bybit_pump_short_grid_research.py`
  - `scripts/bybit_pump_short_grid_research.py`
- Full extended run output: `data/research/bybit_pump_short_extended_grid_research`.
- Input: `data/research/bybit_pump_short_extended/symbol_samples.jsonl`.
- Run size:
  - symbols: 56
  - pump events: 368
  - ladder outcomes: 144,576
  - wave/recycle outcomes: 30,528
  - ladder rules: 432
  - wave rules: 96
- Ladder sweep tested:
  - entry setups: `immediate`, `pb20_oi50_lr_mid`, `pb20_oi0_lr_mid`
  - step: 35/50/75/100/150/200%
  - max legs: 1..6
  - sizing: `equal`, `tapered`
  - exits: `tp25_full_168`, `tp25_50_halves_336`
  - metrics include deployed-return, reserved-capital-return, p90/p95/p99 MAE, p90/p95/p99 reserved margin stress, 300/700/1000% first-entry adverse tails.
- Wave/recycle sweep tested:
  - initial confirmed pullback ladder
  - cover 75% after 25/35/50% TP
  - re-add after 50/75% rebound only when OI and volume filters pass
  - OI min 25/50%, volume min 50%, 1-2 cycles, 90d runner horizon

### Ladder Findings

- Confirmed pullback is materially different from immediate shorting. Immediate entries still show attractive winrates but negative average reserved returns because rare continuation pumps dominate the mean.
- For `pb20_oi50_lr_mid + tp25_full_168 + equal sizing + 50% step`:
  - 1 leg: win `75.8%`, avg reserved `7.64%`, p90 stress `112.3%`, p95 stress `125.3%`, stress100 `11.3%`, stress200 `3.46%`.
  - 2 legs: win `79.9%`, avg reserved `5.52%`, p90 stress `59.0%`, p95 stress `81.8%`, stress100 `4.72%`, stress200 `0.63%`.
  - 3 legs: win `81.1%`, avg reserved `4.07%`, p90 stress `39.3%`, p95 stress `57.6%`, stress100 `4.09%`, stress200 `0%`.
  - 4 legs: win `81.4%`, avg reserved `3.15%`, p90 stress `29.5%`, p95 stress `43.2%`, stress100 `2.52%`, stress200 `0%`.
  - 5 legs: win `81.4%`, avg reserved `2.59%`, p90 stress `23.6%`, p95 stress `34.5%`, stress100 `0.94%`, stress200 `0%`.
  - 6 legs: win `81.4%`, avg reserved `2.16%`, p90 stress `19.7%`, p95 stress `28.8%`, stress100 `0%`, stress200 `0%`.
- Interpretation:
  - 3 legs are the best return/risk compromise if capital efficiency matters.
  - 4 legs are the current best default candidate: lower p90/p95 stress than 3 legs with only modest win/return dilution.
  - 5-6 legs are insurance mode: they reduce tail stress but reserve too much unused capital and lower avg reserved return.
  - 35% step increases winrate slightly (`83-84%`) but crowds entries closer and is more capital-reservation heavy; 50% remains cleaner for a default.
  - 75-100% steps are safer for very wide continuations but lower winrate/return for normal cases.

### Super-Pump Findings

- The worst 700-1000%+ tails mostly come from `immediate` entry. Examples include `RAVEUSDT`, where the post-trigger continuation reached more than `4600%` from the first entry level.
- In immediate-entry rules, adding more legs improves winrate but does not make the strategy acceptable:
  - `immediate + step50 + tp25_full_168`: avg reserved stays negative even at 6 legs (`-14.3%`), with 700%+ tails around `2.7%` and 1000%+ tails around `2.2%`.
  - Therefore immediate short after pump is still not a live default, regardless of ladder count.
- In confirmed-pullback super-pump subset the sample is thin (`n=7`, 3 symbols for many rows), but the best rows are still not clean:
  - positive average can appear, but p90 reserved stress is still roughly `125-290%` depending on rule.
  - Treat this as a special high-margin/avoid bucket, not a reason to increase default live size.

### Wave/Re-Add Findings

- The tested wave/recycle idea does not pass as a default yet.
- It can produce very high winrate (`~89-90%`) and good median reserved return (`~11-17%`), but the average reserved return is negative in top rows because rare 300/700/1000% tails dominate.
- Best-by-average rows still had:
  - avg reserved around `-1%` to `-4%`
  - p90 reserved stress above `130%`
  - cat300 around `11.95%`
  - cat700/cat1000 around `5.03%`
- Interpretation:
  - Covering 75% and re-adding on OI/volume rebound is not enough if the remaining runner is held through a 90d horizon at full modeled weight.
  - Next wave test should add stronger safety:
    - no re-add during `oi_expansion`/`oi_blowoff`
    - re-add only after OI cooled first, then re-expanded from a lower base
    - hard max runner size 10-25% of original
    - hard stop for runner on new high above prior local high or on funding turning extreme negative
    - evaluate runner PnL separately from core trade.

### Current Implementation Bias

- Default live research candidate after this pass:
  - entry: confirmed pullback `pb20_oi50_lr_mid`
  - ladder: 4 planned legs, 50% adverse step, equal sizing
  - exit: full cover at 25% profit or 168h time stop
  - mode: no wave re-add by default; runner only in shadow/paper until a smaller-runner test passes.
- Alternative capital-efficient mode:
  - 3 planned legs, 50% step, equal sizing
  - better reserved return, higher tail stress.
- Insurance mode:
  - 5-6 planned legs, 50% step
  - lower stress, worse reserved return; use only if the operator deliberately reserves more capital for super-pump survival.

## 2026-06-28 Advanced Funding / Runner / Re-Add Research

- Added advanced console research:
  - `analysis_features/bybit_pump_short_advanced_research.py`
  - `scripts/bybit_pump_short_advanced_research.py`
- Full output: `data/research/bybit_pump_short_advanced_research`.
- Input:
  - extended data: `data/research/bybit_pump_short_extended/symbol_samples.jsonl`
  - grid outcomes: `data/research/bybit_pump_short_extended_grid_research/ladder_sweep_outcomes.csv`
- Run size:
  - symbols: 56
  - pump events: 368
  - funding-gate ladder rows: 1,152
  - small-runner outcomes: 86,940
  - strict cooling/re-add outcomes: 4,968
  - runner rules: 280
  - cooling/re-add rules: 16
- Implementation note:
  - CSV flag aggregation was fixed so string flags `"0"`/`"1"` are counted numerically for win/tail/stress metrics.

### Funding Findings

- Funding is included in the modeled net results where available.
- For the current default candidate `pb20_oi50_lr_mid + tp25_full_168 + 50% step + 4 equal legs`:
  - all funding regimes: `n=318`, win `81.45%`, avg reserved `3.15%`, median `6.21%`, p90/p95 stress `29.50/43.17%`.
  - gate `prev24 funding > -0.50%`: `n=107`, win `87.85%`, avg reserved `3.76%`, median `6.21%`, p90/p95 stress `20.09/29.50%`, cat300 `0%`, stress200 `0%`.
  - gate `prev72 funding > -2.00%`: `n=125`, win `87.2%`, avg reserved `3.48%`, median `6.21%`, p90 stress `25.44%`.
  - excluding only extreme funding: `n=294`, win `82.31%`, avg reserved `3.41%`, p90 stress `27.20%`.
- Best funding-gated ladder rows were dominated by `prev24_gt_-0.50`.
  - Top ranked: `pb20_oi50_lr_mid + tp25_full_168 + prev24_gt_-0.50 + 50% step + 3 tapered legs`, `n=107`, win `86.92%`, avg reserved `6.53%`, median `11.04%`, p90/p95 stress `34.36/49.42%`, cat300 `0%`.
  - Cleaner lower-stress default-compatible row: `4 equal legs`, `n=107`, win `87.85%`, avg reserved `3.76%`, median `6.21%`, p90/p95 stress `20.09/29.50%`.
- Interpretation:
  - Do not enter when previous 24h funding paid by shorts is worse than about `-0.50%`.
  - A softer fallback gate is previous 72h funding above `-2.00%`.
  - Funding gate improves winrate and materially reduces stress, but cuts the sample from 318 to roughly 107 events for the strict 24h gate.

### Small Runner Findings

- Tested TP runner plans after covering 75/85/90% at 25% or 50% profit, with 10/15/25% runner held up to 30d, plus time-cover runner variants.
- Best-ranked TP runner rows all keep only a small runner and use more reserved legs:
  - `pb20_oi50_lr_mid + tp25 + cover 90% + 10% runner + 720h max + 50% step + 6 equal legs`: `n=318`, win `87.11%`, avg reserved `2.73%`, median `4.36%`, funding drag `-0.67%`, p90/p95 stress `47.02/80.53%`, cat300 `10.06%`.
  - same with 5 equal legs: avg reserved `3.32%`, median `5.23%`, p90/p95 stress `56.42/96.63%`.
  - same with 4 equal legs: avg reserved `3.41%`, median `6.54%`, p90/p95 stress `70.53/120.79%`.
- Time-cover runner variants do not beat the clean full-cover default:
  - Example `cover 90% at 168h, 10% runner to 2160h, 4 equal legs`: win `71.70%`, avg reserved `2.63%`, median `4.75%`, p90/p95 stress `80.50/112.62%`, cat300 `11.95%`.
- Interpretation:
  - The long-decay thesis is still visible, but runner exposure reintroduces 300%+ tail paths.
  - A 10% runner can be paper-tested, but it should not replace full cover as live default.
  - If runner is enabled later, it should require the funding gate, small size, and a separate hard stop/new-high risk rule.

### Strict Cooling/Re-Add Findings

- Tested stricter wave logic:
  - cover 75/90% after 25/50% TP
  - re-add only after 75% rally
  - require OI cooling at least `-40%`, then OI re-expansion at least `+50%`
  - require volume expansion at least `+100%`
  - max 1 re-add cycle, 3 or 4 ladder legs
- Best rows still failed as defaults:
  - `pb20_oi0_lr_mid + tp25 + cover 75% + 4 legs`: `n=303`, win `90.43%`, avg reserved `-2.07%`, median `8.55%`, funding drag `-1.95%`, p90/p95 stress `103.22/139.37%`, cat300 `8.91%`.
  - `pb20_oi50_lr_mid + tp25 + cover 75% + 4 legs`: `n=318`, win `89.94%`, avg reserved `-4.32%`, median `8.59%`, p90/p95 stress `102.95/136.32%`, cat300 `11.95%`.
- Interpretation:
  - Re-add/wave logic gives high winrate and attractive median, but the mean stays negative because rare continuation tails dominate.
  - It remains a research/paper feature, not a live default.

### Updated Live Bias

- Main default candidate:
  - entry: `pb20_oi50_lr_mid`
  - funding gate: previous 24h funding sum must be greater than `-0.50%`; fallback softer gate previous 72h greater than `-2.00%`
  - ladder: 4 planned legs, 50% adverse spacing, equal sizing
  - exit: full cover at 25% profit or 168h time stop
- Capital-efficient candidate:
  - same gate and exit, 3 legs, preferably compare `equal` vs `tapered` in paper.
- Insurance candidate:
  - 5-6 legs only when explicitly reserving more capital to survive super-pump tails.
- Not live by default:
  - immediate short after pump
  - 10-25% long runner
  - wave/re-add after partial cover.

## 2026-06-29 Deep Re-Add Research

- Added a dedicated re-add research module and CLI:
  - `analysis_features/bybit_pump_short_readd_research.py`
  - `scripts/bybit_pump_short_readd_research.py`
- Full output: `data/research/bybit_pump_short_readd_research`.
- Input: `data/research/bybit_pump_short_extended/symbol_samples.jsonl`.
- Run size:
  - symbols: 56
  - pump events: 368
  - confirmed entry points: 621
  - tested configs: 360
  - total outcomes: 104,760
  - outcomes with an actual re-add: 25,410
  - summary rules: 360
- Tested model families:
  - base entry remains confirmed pullback `pb20_oi50_lr_mid` or `pb20_oi0_lr_mid`
  - initial ladder: 4 equal legs, 50% adverse spacing
  - funding gates: `all` and `prev24_gt_-0.50`
  - initial cover: 75%, 90%, or 100% at 25% TP
  - re-entry triggers:
    - direct rally with OI+volume expansion (`+50/75/100%` rally, OI +25/50%, volume +50/100%)
    - rally then confirmed pullback (`+50% then -10%` or `+75% then -20%`, OI <= 50%, long ratio mid, funding > -0.50%)
  - re-entry sizing:
    - restore missing size in one order
    - restore missing size as a 3-leg ladder
    - fresh full single re-entry, the "kotleta" variant
  - risk controls: no hard stop vs hard stop at +100% against average entry

### Deep Re-Add Findings

- No re-add rule passed the strict live-quality filter of positive average, `cat300=0`, and p90 stress below `50%`.
- Best all-strategy rows improved average return but increased tail stress versus the default full-cover strategy:
  - `pb20_oi50_lr_mid + prev24_gt_-0.50 + cover 90% at tp25 + rally_oi_volume + rally 75% + OI +50% + volume +100% + restore_single + hard_stop100`
  - `n=279`, win `88.89%`, avg reserved `5.80%`, median `6.91%`, p90/p95 stress `60.57/70.53%`, cat300 `2.87%`, re-add rate `8.24%`
  - This beats the default on average return, but loses the clean `cat300=0` / low-stress profile.
- Best actual-readd subset is attractive but not enough for live approval:
  - `pb20_oi50_lr_mid + prev24_gt_-0.50 + cover 90% at tp25 + rally_oi_volume + rally 50% + OI +25% + volume +50% + restore_single + hard_stop100`
  - actual re-add subset `n=67`, win `92.54%`, avg reserved `21.41%`, median `30.38%`, funding slightly positive, cat300 `0%`
  - but p90/p95 stress `94.45/109.87%` and stress100 `7.46%`
  - Interpretation: when the re-add setup works, it works well, but it still requires much more margin tolerance than the default.
- Re-entering after a fresh confirmed pullback is cleaner on catastrophic tails but still too stressful:
  - `pb20_oi50_lr_mid + prev24_gt_-0.50 + cover 90% at tp25 + rally 75% then pullback 20% + OI <= 50 + restore_single + hard_stop100`
  - all-strategy row: `n=279`, win `88.89%`, avg reserved `6.87%`, median `7.30%`, p90/p95 stress `70.53/147.36%`, cat300 `0%`, re-add rate `35.84%`
  - This is the most interesting paper candidate because cat300 is clean, but p95 stress is too high for default live.
- "Kotleta" fresh full re-entry did not clearly beat restore sizing.
  - In the top rows, `fresh_full_single` is usually close to `restore_single` but slightly worse or similar on average and stress.
  - Restore sizing is therefore preferred over fresh full re-entry if this ever reaches paper/live.
- Re-add laddering is safer than single in some no-stop actual subsets, but it still does not solve the tail:
  - no-hard-stop actual rows can keep high winrate, but cat300 remains around `9-10%` in top rows.
  - hard stop reduces cat300 in some configurations, but p90/p95 stress remains high.

### Re-Add Decision

- Re-add is still not part of the live default.
- Paper candidate, if tested:
  - base: `pb20_oi50_lr_mid`, funding gate `prev24_gt_-0.50`, 4 equal legs, 50% spacing
  - first exit: cover 90% at 25% TP
  - re-entry: only after rally +75% and then confirmed pullback -20%, OI <= 50%, long ratio mid, funding > -0.50%
  - sizing: restore only the closed/missing size, not fresh full "kotleta"
  - risk: hard stop around +100% against re-entry average or stronger new-high invalidation must be modeled before live
- The primary live default remains:
  - no re-add
  - full close at 25% TP or 168h
  - funding gate `prev24 > -0.50`
  - 4 equal ladder legs at 50% spacing.

## 2026-06-29 Visual Human Report

- Added a human-readable visual report generator:
  - `analysis_features/bybit_pump_short_visual_report.py`
  - `scripts/bybit_pump_short_visual_report.py`
- Full output: `data/research/bybit_pump_short_visual_report`.
- Main entry point: `data/research/bybit_pump_short_visual_report/index.html`.
- Per-entered-event pages are under `data/research/bybit_pump_short_visual_report/events/`.
- CSV outputs:
  - `visual_strategy_simulations.csv` for all pump events
  - `visual_strategy_entered.csv` for entered simulations
  - `visual_strategy_skipped.csv` for skipped events
- Strategy rendered:
  - pump trigger
  - confirmed pullback 20%
  - OI 24h <= 50%
  - long ratio 0.45..0.65
  - entry funding prev24h > -0.50%
  - 4 equal short ladder legs, each `$1000` notional
  - 50% adverse spacing
  - exit by full TP 25% or 168h time stop
- Money model:
  - `$1000` means notional per ladder step, not account equity.
  - Max gross notional is `$4000`.
  - 3x isolated margin means about `$333.33` initial margin per activated step.
  - Extra margin/top-up is modeled as `max(0, peak unrealized short loss - posted initial margin)`.
  - This ignores exchange maintenance margin details and should be treated as a clear approximation for human planning, not an exchange liquidation engine.
- Full run:
  - symbols: 56
  - pump events: 368
  - entered: 274
  - unique live-like trades after deduplication: 108
  - skipped: 94
  - event detail pages: 274
  - skipped breakdown: 44 toxic funding, 50 no confirmed entry
- Important interpretation:
  - `visual_strategy_simulations.csv` is trigger-level, not live-position-level.
  - Several pump-window/threshold triggers on the same symbol can map to the same entry/exit path.
  - Live interpretation should use `visual_strategy_unique_trades.csv`.
- Aggregate unique-trade read:
  - winrate `85.19%`
  - total net PnL `+$18,069.29` using `$1000` per step
  - average net PnL `+$167.31` per unique trade
  - total funding impact `-$996.96`
  - average ROI on peak required capital `46.79%`
  - max modeled margin top-up `$4,241.35`
- Important examples:
  - largest margin top-up: `HUSDT_24h_150_1781046000000`, 4 legs, top-up `$4,241.35`, net `+$624.05`, ROI on peak capital `11.19%`.
  - worst net result cluster: `ALLOUSDT` events, 4 legs, top-up `$2,784.47`, net `-$2,133.96`, ROI `-51.82%`.
  - VELVET example: 12 entered trigger rows map to 3 unique trades; the 2026-06-12/13 VELVET trade is one unique trade confirmed by 9 trigger rows, not 9 simultaneous positions.

## Open Decisions

- Extended lookback target: start with 365 days for pump symbols, then increase if Bybit history/data quality allows.
- Live-readiness should require a separate 3x liquidation-aware risk filter; long-hold research is not live approval.

## 2026-07-08 Fresh LAB / EVAA Check

- Created fresh public-data outputs:
  - `data/research/pump_short_recent_2026_07_08`
  - `data/research/pump_short_recent_2026_07_08/_comparison`
  - `data/research/pump_short_recent_2026_07_08/_recent_scan`
  - `data/research/pump_short_recent_2026_07_08_evaa`
- LAB was checked from `2026-06-20` through `2026-07-08` across Binance, Bybit, KuCoin, Gate, Bitget, MEXC, and OKX.
  - Gate required direct futures REST because ccxt still times out on `spot/currencies`.
  - Gate LAB high in this slice was `20.15938`; latest close was about `2.55`, confirming the user's `18 -> 2` observation by scale.
  - Pump trigger across most venues: `2026-07-04 09:00 UTC`, 8h pump about `84-86%`.
- Current live-default with strict funding gate (`pb20`, previous 24h funding `> -0.50%`, 4 equal legs, 50% spacing, TP25/168h) would not have entered LAB on most venues because funding before the confirmed pullback entry was extremely toxic:
  - Binance `-3.85%`, Gate `-3.50%`, MEXC `-3.84%`, OKX `-4.07%`, Bitget `-3.99%`.
  - Bybit was barely inside a softer `>-2.0%` gate at `-1.996%`.
- If the funding gate is disabled, the `pb20` LAB trade would have entered around `2026-07-06 16:00 UTC` near `$14`, filled 1 ladder leg, and hit TP roughly `17-19h` later.
  - Net after funding/fees was still positive, but much smaller than the raw price move because funding drag was severe.
  - Example Bybit: price TP worked, but funding during hold was about `-22.06%`, leaving only about `+2.76%` net on the leg.
  - Example Gate: funding during hold about `-16.51%`, net about `+8.31%`.
- A faster `pb10` entry on LAB would have entered much earlier and survived with `33-40%` MAE, but funding drag around `-20%` to `-23%` reduced the net result to low single digits.
- Fresh top-mover scan found EVAA on Binance/Bybit as the more relevant current shadow candidate:
  - Trigger: `2026-07-07 13:00 UTC`, 8h pump about `82%`.
  - `pb20` entry: `2026-07-07 20:00 UTC` near `$2.28`.
  - Funding gate passed (`prev24` about `+0.16%` Binance / `+0.19%` Bybit).
  - By `2026-07-08 12:00 UTC`, TP had not hit; 2 ladder legs were active, MAE about `47%`, live-like unrealized/time-stop mark about `-20%` to `-21%` net on active notional.
- Interpretation:
  - LAB validates the decay thesis but also shows why funding gating is not cosmetic; toxic funding can consume most of the theoretical dump profit.
  - EVAA shows why this should move to shadow/paper first: the default logic can enter a real current pump and immediately sit in a material drawdown before the thesis resolves.
  - Next implementation step should be a fresh multi-exchange shadow runner that logs would-enter/would-skip/would-add/would-exit for Binance and Bybit first, with small-live only after at least several shadow candidates are observed end-to-end.

## 2026-07-08 TP vs Pump-Speed Research

- Added dedicated TP/speed research:
  - `analysis_features/bybit_pump_short_tp_speed_research.py`
  - `scripts/bybit_pump_short_tp_speed_research.py`
  - output: `data/research/bybit_pump_short_tp_speed_research`
- Scope:
  - extended Bybit source data: `data/research/bybit_pump_short_extended/symbol_samples.jsonl`
  - entry setup fixed to the live-like candidate: `pb20_oi50_lr_mid`
  - 4 equal ladder legs, 50% adverse spacing, 168h max hold
  - fixed TP grid: `15/20/25/30/35/40/50/60`
  - funding gates: `all` and `prev24_gt_-0.50`
  - adaptive rules tested from pump size / pump velocity: fixed `25`, pump-percent tiers, velocity tiers, conservative velocity tiers, and hybrid tiers
- Full run:
  - symbols: `56`
  - pump events: `368`
  - fixed TP outcomes: `5112`
  - adaptive TP outcomes: `3195`
- Strict funding-gated fixed TP read (`prev24_gt_-0.50`, `n=307`):
  - `TP15`: win `90.88%`, TP hit `89.90%`, avg reserved `+2.34%`, avg hold `41.83h`
  - `TP25`: win `81.76%`, TP hit `72.31%`, avg reserved `+3.16%`, avg hold `76.08h`
  - `TP35`: win `77.20%`, TP hit `49.51%`, avg reserved `+3.39%`, avg hold `108.84h`
  - `TP50`: win `74.59%`, TP hit `30.62%`, avg reserved `+4.19%`, avg hold `132.69h`
  - `TP60`: win `73.62%`, TP hit `26.06%`, avg reserved `+3.87%`, avg hold `138.95h`
- Interpretation:
  - `TP50` is better than `TP25` by average reserved return on this Bybit sample, but materially worse by winrate, TP-hit rate, and time in trade.
  - `TP35` is only a mild improvement over `TP25` by average return, with a large drop in TP-hit rate.
  - Risk-adjusted/rule-score ranking still prefers lower TP (`15/20/25`) in many speed buckets because higher TP spends longer exposed and loses hit-rate.
  - Highest-average bucket read supports adaptive larger TP mainly when the pump is very fast or very large, but the strongest `400%+` bucket has only `16-19` strict events, so it is a paper/shadow hypothesis, not a live default.
  - The best live-default candidate remains `TP25/168h` unless the goal is explicitly to trade lower winrate for higher average return.

## 2026-07-08 Binance/Bybit Shadow Mode

- Added console-first multi-exchange shadow scanner:
  - `analysis_features/pump_short_shadow_multiexchange.py`
  - `scripts/pump_short_shadow_multiexchange.py`
  - default output: `data/research/pump_short_shadow_binance_bybit`
- Purpose:
  - collect fresh full samples for interesting pump symbols on Binance and Bybit
  - classify live-like entry/watch/block states across several strategy variants
  - check first-leg orderbook liquidity for a `$1000` short entry under a 20 bps slippage budget
  - write CSV/Markdown outputs that can later feed paper/live decisions
- Variants currently scanned:
  - `default_tp25`: `pb20`, prev24 funding `> -0.50%`, OI 24h `<= 50%` if available, 4 equal legs, 50% spacing, TP25/168h
  - `default_tp50_avg`: same entry, TP50/168h
  - `fast_pb10_tp25`: earlier `pb10`, funding `> -0.50%`, OI 24h `<= 100%`, TP25/168h
  - `speed_or_superpump_tp50`: `pb20`, funding `> -0.50%`, OI 24h `<= 100%`, TP50 only if pump velocity `>= 15%/h` or pump `>= 150%`
  - `funding_soft_tp25`: `pb20`, softer funding gate `> -1.00%`, TP25/168h
- Smoke run on `LABUSDT` and `EVAAUSDT`:
  - output: `data/research/pump_short_shadow_binance_bybit_smoke`
  - rows `20`, entry candidates `8`, samples `4`, errors `0`
  - EVAA passed on Binance and Bybit by signal; Binance had `$1081.71` short notional available within 20 bps for a `$1000` first leg, while Bybit had only `$757.37`, so Bybit was a signal but not first-leg liquidity-ok at that size
  - LAB was blocked by funding on both exchanges, matching the earlier LAB conclusion
- Full baseline run:
  - output: `data/research/pump_short_shadow_binance_bybit`
  - rows `271`, entry candidates `87`, interesting samples saved `163`, errors `0`, requests `1858`
  - unique candidate symbols:
    - Binance: `AERGOUSDT`, `ARPAUSDT`, `BIRBUSDT`, `EVAAUSDT`, `MUSDT`, `THEUSDT`, `TLMUSDT`, `VANRYUSDT`, `VELVETUSDT`
    - Bybit: `10000NEXUSDT`, `AERGOUSDT`, `ARPAUSDT`, `BIRBUSDT`, `ESUSDT`, `EVAAUSDT`, `MUSDT`, `OPGUSDT`, `THEUSDT`, `TLMUSDT`, `VANRYUSDT`, `VELVETUSDT`
  - first-leg liquidity at `$1000/20bps` passed for all Binance candidate variants in this run; on Bybit, `ESUSDT` and `EVAAUSDT` failed the liquidity check at that size
- Operational interpretation:
  - shadow is not only historical logging; it now answers whether the live bot would see a current entry, which strategy variant would trigger, whether funding blocks it, and whether the first `$1000` short step is realistically executable by the orderbook check
  - next step is to add persistent paper-position lifecycle for these multi-exchange variants: would-enter, would-add ladder step, would-hit TP, would-time-stop, and compare variants over real forward time

## 2026-07-09 Pump Bot Algorithm + Capital Allocation

- Added human-readable strategy/technical plan:
  - `docs/pump_short_shadow_live_algorithm.md`
- Added capital allocation analyzer:
  - `analysis_features/pump_short_capital_allocation.py`
  - `scripts/pump_short_capital_allocation.py`
- Scope:
  - source outcomes: `data/research/pump_short_multiexchange_2024_clean/_comparison/outcomes.csv`
  - exchanges: Binance and Bybit
  - strategy: `pb20_wait168_fgm0p5_ladder4_step50_tp25_hold168`
  - leverage: `3x`
  - ladder: 4 equal steps
  - capital cases: `$3000` and `$1000`
  - slot model: `coin_budget = capital / max_active_coins`, `step_margin = coin_budget / 4`, `step_notional = step_margin * 3`
- Outputs:
  - `data/research/pump_short_capital_allocation_3000`
  - `data/research/pump_short_capital_allocation_1000`
- `$3000` Bybit read:
  - slot 1: per-step notional `$2250`, trades `141`, net PnL `+$36,259.96`, ROI initial `1208.67%`, max single rescue top-up `$12,291.12`
  - slot 3: per-step notional `$750`, trades `341`, net PnL `+$19,075.09`, ROI initial `635.84%`, max single rescue top-up `$11,213.28`
  - slot 5: per-step notional `$450`, trades `475`, net PnL `+$19,098.94`, ROI initial `636.63%`, max single rescue top-up `$6,727.97`
- `$3000` Binance read:
  - slot 4 was the best practical row among diversified settings: per-step notional `$562.50`, trades `326`, net PnL `+$21,706.65`, ROI initial `723.55%`, max single rescue top-up `$4,082.26`
  - slots `5+` turned negative under this unchanged rule because more lower-quality Binance events were admitted
- Interpretation:
  - Bybit remains the cleaner primary venue for the current rule.
  - Binance should not copy the same live rule blindly; it needs stricter exchange-specific filtering before live.
  - Pure ROI favors fewer slots, but risk concentration and rescue top-up favor smaller per-step notional.
  - For initial `$1000` small live, recommended starting point is Bybit primary with `max_active_coins = 3..5`, giving about `$250..$150` notional per step.
  - Manual top-up sums in the report are cumulative historical rescue requirements, not simultaneous locked capital; `max_single_manual_topup_usd` is the more practical worst-case alert number.

## 2026-07-09 Bybit Funding Window + TP Capital Grid

- Added focused Bybit-only grid:
  - `analysis_features/pump_short_bybit_funding_tp_capital_grid.py`
  - `scripts/pump_short_bybit_funding_tp_capital_grid.py`
  - output: `data/research/pump_short_bybit_funding_tp_capital_grid`
- Scope:
  - source samples: `data/research/pump_short_multiexchange_2024_clean/bybit/symbol_samples.jsonl`
  - entry: `pb20`, 4 equal ladder legs, 50% adverse spacing, 168h max hold
  - funding windows: previous settled funding sum from `24h` down to `3h`
  - funding thresholds: `> -1.0%` through `> -0.5%` in `0.1%` steps
  - TP values: `25/30/35/45`
  - capital cases: `$1000` and `$3000`
  - slot cases: `max_active_coins = 1..4`
- Run result:
  - outcomes: `418,932`
  - raw strategy rows: `528`
  - capital/slot rows: `4,224`
- Important modeling note:
  - Funding gate uses the sum of already settled funding events in the previous N hours.
  - If a short window such as `3h` has no Bybit settlement inside it, the previous-window funding sum is `0.0%`.
- ROI read:
  - `$1000` and `$3000` have the same ROI for the same slot count because the model scales step notional proportionally to capital; USD PnL and top-up sizes scale about 3x.
  - Best pure ROI remains concentrated and top-up-heavy: slot `1`, `21-24h > -1.0%`, `TP30`, ROI `1279.10%`, but `$1000` max single top-up was `$3,436.93`; `$3000` max single top-up was `$10,310.78`.
  - Best slot `3`: `4h > -1.0%`, `TP45`, ROI `823.63%`, trades `269`, but average hold about `149.19h`.
  - Best slot `4`: `3h > -0.9%`, `TP25`, ROI `629.15%`, trades `415`, average hold about `106.14h`, and much higher TP hit-rate than TP45.
  - Current default-like `24h > -0.5%`, `TP25`: slot `3` ROI `635.84%`; slot `4` ROI `623.66%`.
- Interpretation:
  - Loosening funding from `>-0.5%` toward `>-1.0%` did not materially improve the practical TP25/slot4 result; `3h > -0.9%` was only slightly better than `24h > -0.5%`.
  - Higher TP can improve raw average and some slot ROI, but it lowers TP-hit rate and extends hold time; `TP45` is more of a paper/high-exposure variant than a clean small-live default.
  - For small live, the grid still supports Bybit `3..4` slots with TP25 as the cleaner operational default; TP35/45 should be shadowed before replacing TP25.

## 2026-07-09 Bybit Strategy Graphic Report, $1000

- Added graphical strategy report:
  - `analysis_features/pump_short_strategy_graphic_report.py`
  - `scripts/pump_short_strategy_graphic_report.py`
  - output: `data/research/pump_short_strategy_graphic_report_1000`
  - main report: `data/research/pump_short_strategy_graphic_report_1000/index.html`
- Scope:
  - capital: `$1000`
  - source: selected trades from `data/research/pump_short_bybit_funding_tp_capital_grid`
  - strategies shown in this order:
    1. main default: `3` coins, `24h > -0.5%`, `TP25`
    2. best pure ROI: `1` coin, `21h > -1.0%`, `TP30`
    3. best 3-coin ROI: `3` coins, `4h > -1.0%`, `TP45`
    4. best 4-coin row: `4` coins, `3h > -0.9%`, `TP25`
    5. default logic with 4 coins: `4` coins, `24h > -0.5%`, `TP25`
- Outputs:
  - `strategy_summary.csv`
  - `actions.csv`
  - `topups.csv`
  - `index.html`
- Report contents:
  - strategy comparison table
  - per-strategy equity chart from 2024-01-01 to 2026-06-30
  - red top-up markers for trades that exceeded allocated coin budget
  - aligned active-coins chart showing idle, in-market, and full-capacity periods
  - all selected trades with entry/exit time, coin, active count after entry, legs filled, notional, PnL, MAE, funding, and top-up
  - top-up-only tables
- Key added practical metrics:
  - Main default `3` coins: ROI initial `635.84%`, trades `341`, idle `24.33%`, active `75.67%`, full-capacity `30.83%`, full-capacity reached `173` times.
  - Main default max single extra top-up `$3,737.76`; conservative max concurrent extra top-up `$5,103.44`.
  - Main default ROI on `$1000 + max concurrent top-up` is `104.18%`.
  - Best pure ROI `1` coin: ROI initial `1279.10%`, but active/full-capacity `68.01%` because one coin equals full capacity; ROI on `$1000 + max concurrent top-up` `288.29%`.
  - Best 3-coin `TP45`: ROI initial `823.63%`, idle `20.17%`, full-capacity `40.69%`, ROI on `$1000 + max concurrent top-up` `171.95%`.
  - Best 4-coin TP25 and default 4-coin TP25 are close: ROI initial `629.15%` / `623.67%`; ROI on `$1000 + max concurrent top-up` `130.32%` / `129.19%`.
- Important limitation:
  - Selected-trades data stores max MAE/top-up per trade, not exact MAE timestamp. The report marks top-up on the trade entry time and computes max concurrent top-up conservatively across open trade intervals.

## 2026-07-10 Bybit Strategy Compounding Report, $1000

- Added dynamic-capital strategy report:
  - `analysis_features/pump_short_strategy_compound_report.py`
  - `scripts/pump_short_strategy_compound_report.py`
  - output: `data/research/pump_short_strategy_compound_report_1000`
  - main report: `data/research/pump_short_strategy_compound_report_1000/index.html`
- Model:
  - start capital is `$1000`
  - each new entry uses `capital at entry / max_active_coins / 4 ladder steps * 3x`
  - realized PnL changes strategy capital only after the trade closes
  - manual top-up is temporary external rescue cash and is not added to strategy capital
  - if realized strategy capital falls to `<= 0`, later entries are skipped as insolvent
- Outputs:
  - `compound_strategy_summary.csv`
  - `compound_actions.csv`
  - `compound_topups.csv`
  - `metadata.json`
  - `index.html`
- Generated run:
  - strategies: `5`
  - actions: `1,131`
  - temporary top-up rows: `110`
- Key result under compounding:
  - Main default `3` coins / `24h > -0.5%` / `TP25`: final capital `-$316.40`, ROI `-131.64%`, trades taken `68`, skipped insolvent `273`, max concurrent temporary top-up `$3,447.40`.
  - Best pure ROI `1` coin / `21h > -1.0%` / `TP30`: final capital `-$555.62`, ROI `-155.56%`, trades taken `34`, skipped insolvent `90`, max concurrent top-up `$12,938.29`.
  - Best 3-coin ROI `4h > -1.0%` / `TP45`: final capital `-$13,918.35`, ROI `-1491.83%`, trades taken `202`, skipped insolvent `67`, max concurrent top-up `$387,261.69`.
  - Best 4-coin row `3h > -0.9%` / `TP25`: final capital `$19,070.70`, ROI `1807.07%`, trades taken `415`, skipped insolvent `0`, max concurrent top-up `$121,633.32`.
  - Default logic with 4 coins `24h > -0.5%` / `TP25`: final capital `$18,253.81`, ROI `1725.38%`, trades taken `412`, skipped insolvent `0`, max concurrent top-up `$116,423.20`.
- Interpretation:
  - This is a different risk picture than the fixed-size report. Reinvesting profits makes the 4-coin TP25 variants much more profitable on closed equity, but the temporary top-up requirement grows with position size and becomes very large near the end of the curve.
  - The 3-coin default is not robust in this strict compounding model because early realized losses can reduce capital enough that later trades cannot continue.
  - The practical read is not "increase size automatically with no cap"; it is that dynamic sizing needs a hard max step/notional cap or a rule that only scales after withdrawing profit/reserving rescue capital.
- Verified:
  - `python scripts/pump_short_strategy_compound_report.py`
  - `python -m pytest tests/test_pump_short_strategy_compound_report.py tests/test_pump_short_strategy_graphic_report.py tests/test_pump_short_bybit_funding_tp_capital_grid.py` (`4 passed`)
