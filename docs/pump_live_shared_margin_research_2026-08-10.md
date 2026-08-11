# Pump Live shared-margin and post-fill research — 2026-08-10

## Scope

This began as a reproducible research-only comparison for a `$3000` Pump
portfolio. The replay remains research evidence; the separately versioned
implementation selected from it is documented at the end of this file.
Reproduce the replay with:

```text
.venv\Scripts\python.exe scripts\pump_live_shared_margin_research.py
```

Ignored artifacts are written to
`data/research/pump_live_shared_margin_research/`.

## Historical coverage

The runner rebuilds the causal `main_pullback_tier` candidate stream instead
of reading only the 35 previously admitted portfolio trades:

- research boundary: 2024-01-01 UTC;
- 126 unique reconstructed Pump cases after that boundary;
- 40 candidates pass the current funding/OI/long-ratio/tier rules and have a
  matching historical outcome;
- the first eligible candidate is 2025-09-08 and the last is 2026-06-18;
- four slots admit 35 trades; two later candidates overlap a full portfolio
  and three duplicate an already-open symbol.

The archive is current-listing survivor-biased. Entries and exits are rebuilt
from hourly data. Therefore the replay can identify a 15-second stop race but
cannot prove whether an individual historical candle would have hit that stop
before the monitor reacted.

## BLUAI live geometry at the ARM checkpoint

Fresh exchange-backed state after `ARM PUMP LIVE 3000`:

- quantity `9810`, average `0.02138577`, mark about `0.02772`;
- exchange liquidation `0.037698`, Full stop `0.03675555`;
- tracked prefund/top-up `$105`;
- live L3 `0.035682`; planned L4 `0.0446025`, L5 `0.053523`;
- the current stop is only `3.009%` above L3.

Filling L3 adds about `2942.66` coins (`$105` notional at the trigger). The
isolated-short liquidation direction improves because the higher entry raises
the weighted average: the formula projects post-fill liquidation near
`0.04014` and a newly synchronized stop near `0.03913`. The exact exchange
value may differ and must always be reread.

The critical race is that Bybit does not automatically move the already-set
stop when L3 fills. Until FeeArb sees the fill and resynchronizes protection,
the old `0.03675555` stop remains only about `3%` above L3. A fast continuation
can therefore close BLUAI even though the post-fill liquidation itself moved
away.

The post-fill/next-step model requires roughly `$200` total tracked extra
margin before L3 is allowed to remain live, versus `$105` now. The production
formula rounds the initial increment to `$95`, then allows only bounded `$5`
corrections against fresh exchange rereads. At about `$200`, both the old pre-fill stop and projected
post-L3 stop are about `2.8%..3.2%` above L4, removing the 15-second L3 race
and making L4 reachable. This amount was calculated only; it was not added.

## Policy replay

`Historical ROI` is the existing net outcome model scaled by the fixed
isolated-margin slot. It is valid only when the policy has zero capacity
breaches. A top-up requirement is conservatively held for the full trade, so
loan duration and loan-hours are deliberately overstated.

| Policy | Trades | ROI on Pump $3000 | Max DD | Capacity breaches | Old-stop race fills | Peak combined top-up | Peak main loan | Conservative loan time |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| current `$525`, current gate | 35 | 173.36% | 3.15% | 1 | 38 | `$719` | `$0` | 0 h |
| safe `$525`, projected gate, bounded `$2000` rescue facility | 35 | 173.36% | 3.15% | 0 | 0 | `$1180` | `$355` | 300 h |
| safe `$600`, projected gate | 35 | 198.13% | 3.60% | 0 | 0 | `$1345` | `$820` | 300 h |
| safe `$625`, projected gate | 35 | 206.38% | 3.75% | 0 | 0 | `$1400` | `$975` | 300 h |
| `$650`, `$1000` loan | 35 | 214.64% | 3.90% | 3 | 0 | `$1460` | `$1135` | 688 h |
| user `$750`, rescue-only loan | 33 | 233.45% | 4.49% | 0 | 0 | `$1260` | `$585` | 406 h |
| user `$750`, entry loan allowed | 35 | 247.66% | 4.49% | 0 | 0 | `$1680` | `$1755` | 688 h |
| aggressive `$725`, rescue-only | 35 | 239.40% | 4.34% | 0 | 0 | `$1625` | `$1600` | 688 h |

Why the rescue-only `$750` policy executes only 33 trades: four full slots are
already `$3000`; the `$75` operating floor makes the fourth slot impossible
without borrowing for entry. Allowing entry borrowing restores 35 trades but
turns the main account into normal strategy capital and reaches `$1755`
borrowed in the conservative replay.

The current `$525` policy is not fully capacity-valid in this replay. HUSDT on
2026-06-10 required about `$569` at this scale because historical stress was
`208.39%`, above the `$525` per-position top-up cap. Its attractive conditional
return must not be presented as protected return.

## Decision

The best first change is not a global cross-position exchange margin mode and
not `$750` slots. Keep isolated margin per coin, but manage the cash reserve as
one shared portfolio pool:

1. Keep new live slots at `$525` while existing legacy positions retain their
   immutable ladder size.
2. Replace the current-next gate with a projected-fill/next-step gate. Before
   placing a ladder, simulate its complete fill from actual quantity, price,
   contract size and risk tier. Add and verify enough isolated margin so both
   the still-live old stop and projected post-fill stop clear the following
   ladder. Use a bounded final-leg continuation buffer when no following
   ladder exists.
3. React to a fill from the order/private stream and resynchronize TP/SL
   immediately; keep the 15-second monitor as reconciliation, not the primary
   fill reaction.
4. Admit new positions from Pump-owned capital only. Main borrowing must be a
   rescue facility, never ordinary entry capital.
5. Allocate the shared reserve to the lowest-buffer position first. A positive
   position must not keep a private unused reserve while another position is
   near its stop.
6. Use main funding only after a fresh projected main-account safety check.
   Keep idempotent transfers, no risk cooldown, and an aggregate facility cap.
7. If the next-step guarantee cannot be funded, cancel/defer that ladder before
   it can fill. If an already-filled final position cannot be protected, use
   bounded profitable-donor reduction and then reduce/close the threatened
   position. “Any means” must never mean unlimited borrowing.

For the present accounts, the safe `$525` projected policy is the deployment
candidate. It preserved all 35 trades, removed all 38 modelled old-stop race
fills, and needed at most `$355` from main in the conservative replay. The
configured `$2000` rescue facility is an aggregate ceiling, not assumed cash:
every transfer still needs a fresh main-account projection and its smaller
operational request limit. Main funds never make a new-entry admission pass.

After the projected gate has live/shadow fill evidence, `$600` is the best
next sizing experiment: it raises conditional replay ROI to `198.13%`, keeps
max drawdown under `3.60%`, and stays within a `$1000` rescue facility. It
requires main transfer-safe balance of at least about `$1500` to preserve the
current `$500` main floor. `$625` uses `$975` of that `$1000` facility and has
too little error allowance; `$650+` and the `$725/$750` variants remain
aggressive shadow-only candidates.

## Remaining model limitations

- obtain 1-minute or tick/private-stream evidence for fill-to-stop timing;
- replay exchange fees, funding, partial fills and contract rounding at the
  proposed `$600` size;
- stress simultaneous peaks instead of assuming historical peaks repeat in the
  same order;
- shadow the projected gate on new real events;
- test main funding unavailable, stale, rejected and partially available;
- test the final-leg reduction path before enabling it.

## Versioned implementation — 2026-08-11

The previous production manager is preserved as
`v2_current_next`. It remains the code/config fallback and can be restored by
setting `PUMP_LIVE_MARGIN_MANAGER_POLICY=v2_current_next` followed by a safe
backend restart and explicit ARM. The new manager is
`v3_shared_projected`; changing manager versions does not resize any position.

`v3_shared_projected` keeps every Bybit position on isolated margin and keeps
new v2 entry ladders fixed at `$525`. It changes only portfolio admission and
margin defence:

1. Before an order for the next ladder can remain live, it projects the full
   fill using the actual exchange quantity and liquidation price. The strict
   target asks both the current stop and projected post-fill stop to clear the
   following ladder; the last ladder asks for a `20%` continuation buffer.
2. The manager hard-reserves future base ladder margins plus the immediately
   executable next-gate margin for all open positions and the candidate, up to
   three `$5` exchange-correction steps for that gate, and the `$75` Pump
   operating floor. It still walks the full remaining path, but reports that
   separately as stress instead of blocking cash: no distant order can become
   live before another exchange-confirmed gate.
3. The shared Pump pool is dynamic. It reserves complete base ladder size only
   for positions that actually exist; it does not permanently strand four
   private `$525` envelopes. Immediate unsafe gates block admission; negative
   full-path stress warns that later rescue/defer may be needed but cannot cause
   an unguarded fill because only the nearest ladder is live.
4. The bounded rescue envelope is `$2000` aggregate and `$2000` per position.
   This is only permission to protect an existing position. The actual main to
   Pump transfer still requires fresh main safety, idempotent confirmation and
   the existing per-request controls. Temporary borrowed principal is removed
   from new-entry headroom, so main funds cannot finance an ordinary entry.
5. A live ladder that no longer satisfies the projected gate is cancelled and
   exchange-confirmed before margin is changed. After margin and Full TP/SL are
   reread and confirmed, only that nearest ladder is recreated. A fill race or
   uncertain cancel remains fail-closed.
6. Bybit also enforces `position margin <= position value`. When that exchange
   ceiling makes the strict following-ladder target physically impossible,
   the manager does not retry an impossible add. It derives a conservative
   add ceiling from fresh `positionValue`, `positionIM` and `positionMM`, then
   requires both current and projected stops to be at least `8%` above the
   immediate filling ladder. The hot-ladder reconciliation interval drops from
   `15s` to `5s`; after the fill, the larger position is reread and the next
   gate is funded before another ladder can be placed. This fallback is
   explicitly recorded and is not reported as the strict guarantee.
7. The earlier relative `0..2%` tolerance remains available only in the
   rollback manager. Both the strict target and the exchange-cap `8%` fallback
   use hard exchange-confirmed boundaries; a shortfall is never accepted.

The Pump control page and unified positions API/page expose the active manager,
dynamic new-slot headroom and effective per-position top-up cap. The research
runner now labels the deployed replay candidate
`safe_pool_525_rescue2000`; its historical result remains 35 trades, one loss,
`173.36%` conditional ROI, `3.15%` maximum drawdown, zero modelled capacity
breaches, zero old-stop races and `$355` peak main rescue use. These replay
returns remain survivor-biased and do not guarantee live results.

Pre-deployment regression on 2026-08-11 passed `90` focused Pump tests,
`99` Pump/research/positions tests, Python compilation, frontend contract
coverage and the complete project suite (`713 passed`, `8` subtests,
`13` pre-existing warnings). Node.js is not installed in this Windows
environment, so JavaScript was covered by the page/API contracts and diff
review rather than `node --check`.

The first fail-closed deployment attempt exposed the Bybit ceiling before ARM.
All three old nearest ladders were cancelled and confirmed, while Full TP/SL
remained present. Strict requested adds were `$70` for 1000RATS, `$95` for
BLUAI and `$200` for ACE; Bybit rejected them with `retCode 10001` / `can not
set pm more than pv`. Fresh raw exchange fields showed respectively about
`$74.95/$54.19`, `$272.44/$175.24`, and `$325.09/$195.40` position value versus
position margin. No margin was added, no ladder was recreated, and entries
remained disarmed. The exchange-cap fallback was added from that evidence;
its first required adds are `$10`, `$20`, and `$25`, each followed by an exact
exchange reread before any ladder recreation. The earlier zero-race historical
result belongs to the strict mathematical target; the live `8% + 5s` fallback
needs separate evidence and must not inherit that claim.

That attempt also exposed Bybit client-link retention: a cancelled ladder ID
cannot be submitted again and returns `110072`. Recovery is deliberately
narrow. The exact duplicate error is eligible only after a fresh exchange read
proves there is no non-reduce order for the symbol; the leg then receives a
persisted generation suffix and only the nearest ladder is recreated. Unknown
orders remain fail-closed. Pump-lab default controllers are now isolated under
`FEEARB_TESTING=1`; the focused `115`-test run and complete `716`-test run both
passed while production state/event hashes stayed byte-for-byte unchanged.

The pre-isolation regression had already caused one real `$20` BLUAI add and
three real `$25` ACE adds through separately constructed stale controllers.
The exchange and durable margin events confirmed those mutations. With the
backend stopped, tracked top-up/floor values were therefore reconciled to
`$125` BLUAI and `$165` ACE; RATS was already correct at `$35`. No new add or
transfer was sent during reconciliation or after restart. The new generation
recovery recreated exactly one nearest `R1` ladder for each position and left
all farther legs planned. Four observed hot cycles were `armed`, error-free and
stable at three non-reduce ladders plus six Full TP/SL orders after explicit
`ARM PUMP LIVE 3000`. The shared planner rejects another five-leg candidate at
the current state because its complete projected path exceeds the combined
top-up capacity, despite about `$2270` exchange-available cash.

## Next-gate admission correction (2026-08-11)

The complete-path block above was too conservative for the enforced execution
state machine. Only the nearest ladder can be live, so distant fills cannot
consume their hypothetical margin before an intervening exchange read, margin
gate and Full TP/SL synchronization. Admission now hard-gates future base
ladder margin plus the immediate next gate for every existing position and the
actual 2/3/5-leg candidate. The exchange-capped complete path remains visible
as stress headroom.

On the captured RATS/BLUAI/ACE state, existing immediate safety shortfall was
`$0`. Required Pump available/headroom was approximately `$1532.50/+737.89`
for a two-leg candidate, `$1577.50/+692.89` for three legs and
`$1382.50/+887.89` for five equal legs. Corresponding full-path stress
headroom remained negative (`-$1077.11`, `-$1457.11`, `-$2062.11`). Thus a
fourth signal may be admitted now, but every later fill remains sequentially
gated; stress is not presented as guaranteed funded capacity.

The BLUAI `10001` incident also gained an exact circuit breaker. One rejected
margin write records qty/step and current position value, refreshes once and
uses the 8% exchange-cap fallback. It neither repeats the write nor duplicates
blocked events until exposure changes or position value grows more than 5%.
Focused regression passed `124`; the complete suite passed `720` plus `8`
subtests, with both production Pump state/event hashes unchanged.

Deployment preserved the three owned positions, three nearest non-reduce
ladders and six Full TP/SL orders. Explicit `ARM PUMP LIVE 3000` succeeded.
Following monitor cycles stayed armed, next-gate admission stayed ready at
about `+$887.89`, and no error or margin mutation occurred. TUT Grid and Manual
execution state were unchanged.

## On-demand shared-pool implementation v4 (2026-08-11)

The operator rejected reservation arithmetic based on every possible future
ladder. The production candidate is therefore `v4_shared_ondemand`, with
`v2_current_next` and `v3_shared_projected` retained as restart-selectable
rollback modes.

The v4 accounting boundary is deliberately simple:

- Bybit positions remain isolated. "Shared pool" means only that free Pump
  USDT is allocated centrally when an action is due; losses and liquidation
  prices do not become cross margin.
- At declared Pump strategy capital `$3000`, a new symbol receives a complete
  `$600` budget. Existing tier weights produce `$120 x 5`, `$100/$200/$300`,
  or `$200/$400`. Existing positions retain their immutable `$175` or `$525`
  snapshot and are never resized by a policy migration. The remaining `$600`
  in the four-slot sizing identity is not a cash reserve and is not locked;
  actual action admission is governed only by free cash and the bands below.
- Admission accounts for exchange available balance, L1, the single next live
  ladder and the immediate prefund needed to make that next action safe. It
  does not reserve a distant, unsubmitted ladder. After that action, Pump-owned
  cash must remain at least 30% of Pump-owned wallet and at least `$75`.
- Confirmed temporary main-to-Pump principal is subtracted from both wallet and
  available entry cash. It may rescue an existing position but can never admit
  a new symbol. The aggregate outstanding facility is `$2000`; every transfer
  still requires the existing fresh main-account projection, `$500` projected
  main available floor, 75% maximum margin ratio and 25% minimum liquidation
  buffer where positions exist.
- Account cash bands are calm `>=30%`, warning `20..30%`, stress `10..20%`,
  and emergency `<10%`. They are combined with the lowest position liquidation
  buffer in the displayed regime. Entry admission naturally blocks below the
  30% post-action boundary without permanently disarming position monitoring.
- When a position reaches the 20% warning boundary, the controller derives the
  rounded margin needed to restore a 25% exchange-read buffer. If free Pump cash
  is insufficient it first tries the guarded main rescue transfer. It then
  cancels and confirms only Pump-owned non-reduce ladder orders, largest other
  order first; Full TP/SL remains untouched. Paused ladders return only after
  free cash is again at least 30% and every position is above 20%.
- If those steps still cannot fund the defence, the enabled first live version
  closes one whole protected profitable donor, ranked by distance to TP and
  profit, using reduce-only execution. With no eligible donor it closes the
  threatened position. Partial donor cuts are deliberately deferred because a
  correct partial implementation must also cancel, rescale and re-protect that
  donor's remaining ladder. At or below the existing 10% emergency position
  buffer, the threatened position is closed directly.
- Above a 35% position buffer, the existing two-cycle and 30-minute hysteresis
  may return only margin that leaves at least 25% and keeps the immediate next
  ladder safe. A legacy prefund floor no longer strands cash under v4, but every
  removal is exchange-verified and rolled back on a failed safety check.

Capital sizing is intentionally split into two decisions. V4 activates the
`$600` budget for new entries now. The observation layer already computes the
same 20% proportion from Pump-owned capital, rounded down to `$5`, with +10%
growth, -5% reduction and maximum +25% increase recommendations. Automatic
periodic adoption remains disabled until a later capital-manager review; when
implemented, a rebase may change only subsequent entries and may never rewrite
an open position's risk snapshot.

Pre-deployment verification passed Python compilation, `126` focused
Pump/transfer tests, `159` expanded Pump/API/lab/positions tests and the full
project suite (`730 passed`, `8` subtests, `13` pre-existing warnings). The
production `live_events.jsonl` SHA-256 remained unchanged throughout testing.
The browser contract explicitly recognizes `v3_3000_pool600` as a `$3000`
cohort and therefore requests `ARM PUMP LIVE 3000`; obsolete `$175/$525`
operator text was removed and the static cache version was advanced.
