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
   fill using the actual exchange quantity and liquidation price. Both the
   current stop and the projected post-fill stop must clear the following
   ladder. The last ladder instead uses a bounded `20%` continuation buffer.
2. The manager also walks every remaining ladder sequentially when deciding
   whether a new symbol fits. It reserves all future base margins, all
   projected margin additions, up to three `$5` exchange-correction steps per
   margin gate, and the `$75` Pump operating floor.
3. The shared Pump pool is dynamic. It reserves the complete ladder size only
   for positions that actually exist; it does not permanently strand four
   private `$525` envelopes. If two positions consume the safe path, the third
   is rejected; likewise for the fourth.
4. The bounded rescue envelope is `$2000` aggregate and `$2000` per position.
   This is only permission to protect an existing position. The actual main to
   Pump transfer still requires fresh main safety, idempotent confirmation and
   the existing per-request controls. Temporary borrowed principal is removed
   from new-entry headroom, so main funds cannot finance an ordinary entry.
5. A live ladder that no longer satisfies the projected gate is cancelled and
   exchange-confirmed before margin is changed. After margin and Full TP/SL are
   reread and confirmed, only that nearest ladder is recreated. A fill race or
   uncertain cancel remains fail-closed.
6. The earlier relative `0..2%` tolerance remains available only in the
   rollback manager. The projected manager records estimate deviation but
   requires the hard exchange-confirmed target; a shortfall is never accepted
   as safe.

The Pump control page and unified positions API/page expose the active manager,
dynamic new-slot headroom and effective per-position top-up cap. The research
runner now labels the deployed replay candidate
`safe_pool_525_rescue2000`; its historical result remains 35 trades, one loss,
`173.36%` conditional ROI, `3.15%` maximum drawdown, zero modelled capacity
breaches, zero old-stop races and `$355` peak main rescue use. These replay
returns remain survivor-biased and do not guarantee live results.

Pre-deployment regression on 2026-08-11 passed `88` focused Pump tests,
`96` Pump/research/positions tests, Python compilation, frontend contract
coverage and the complete project suite (`711 passed`, `8` subtests,
`13` pre-existing warnings). Node.js is not installed in this Windows
environment, so JavaScript was covered by the page/API contracts and diff
review rather than `node --check`.
