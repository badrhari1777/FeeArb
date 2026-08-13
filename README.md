# FeeArb

Пользовательские и эксплуатационные инструкции собраны в
[`instructions/00_README_ИНСТРУКЦИИ.md`](instructions/00_README_ИНСТРУКЦИИ.md).

FeeArb is an asynchronous trading, protection and research workspace for
cross-exchange funding arbitrage and the separate Bybit Pump/Dump strategy.
The production web runtime is intentionally smaller than the historical
research tree: only the modules listed below may own recurring work.

## Highlights
- **Active execution** – Manual Enter/Exit/Roll, Auto-Arbitrage Grid and Pump
  Live. Pump uses a dedicated Bybit subaccount and an explicit ARM contract.
- **Active protection** – cached account monitoring, isolated-margin control,
  stop/take verification and orphan protective-order cleanup. Autonomous
  position reduction and the old Auto Exit/Auto Strategy engines are retired.
- **Read models** – the main dashboard, detailed positions view, funding
  history and manual spread monitor consume cached snapshots wherever possible.
- **Research boundary** – Strategy Lab and Pump shadow/paper collectors are
  research-only. Historical `pipeline/`, analysis packages and databases are
  retained for reproducibility, but do not belong to the main dashboard loop.
- **API-load visibility** – account and positions-market refresh counters are
  exposed in the cached dashboard runtime payload; they do not trigger extra
  exchange requests.
- **Testing** – pytest covers state replay, live safety contracts, execution,
  cached read models and the production import boundary.

## Architecture Overview
```
AccountMonitor (single-flight private refresh) ─┐
Positions-market public cache ──────────────────┼─> cached dashboard/read models
ProtectiveOrderManager ─────────────────────────┘

ManualTradeManager ───────────────> explicit operator execution
GridStateRepository + StateMachine -> guarded Grid execution
PumpStateRepository + StateMachine -> guarded Bybit Pump execution

Strategy Lab / historical pipeline -> research and shadow only
```

## Project Layout
```
.
|-- pipeline/                    # Historical/offline candidate collection
|-- orchestrator/
|   |-- models.py                # Dataclasses shared across systems
|   `-- opportunities.py         # Exchange polling + opportunity builder
|-- execution/
|   |-- accounts.py              # Private account gateway + single-flight cache
|   |-- manual.py                # Manual live execution owner
|   |-- auto_arb_grid.py         # Pure Grid decisions/reducers
|   |-- grid_state*.py           # Versioned Grid state/replay
|   |-- pump_live.py             # Pump exchange-effect controller
|   `-- pump_state*.py           # Versioned Pump state/replay + pure reducers
|-- exchanges/                   # Public funding/market adapters
|-- webapp/
|   |-- app.py                   # FastAPI routes + WebSocket endpoint
|   |-- services.py              # Compatibility facade / I/O composition root
|   |-- service_lifecycle.py     # Active task lifecycle owner
|   |-- funding_history.py       # Operator-invoked funding read service
|   |-- manual_spread_service.py # Operator-invoked spread read service
|   |-- main_positions_read_model.py
|   |-- dashboard.py             # Compact dashboard read-model builder
|   |-- templates/index.html     # Operational dashboard
|   `-- static/dashboard.js      # Compact cached-state dashboard client
|-- analysis_*/                  # Historical/research-only packages
|-- tests/                       # pytest suite
|-- project_settings.py          # JSON-backed settings manager
|-- requirements.txt             # Runtime dependencies
|-- scripts/exchange_probe.py    # Diagnostics for raw exchange snapshots
`-- README.md

# Runtime artifacts (ignored)
# |-- data/                      # Cached source data + raw payloads
# `-- logs/                      # Application + execution logs
```

## Setup
```bash
python -m venv .venv
# Windows PowerShell
.\\.venv\\Scripts\\Activate.ps1
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env  # customise if private endpoints/keys needed later
```

## Configuration
`data/settings.json` is created on first run. Current monitoring and protection
settings are available in the collapsed settings block at the bottom of the
dashboard. Legacy candidate sources are owned by Strategy Lab and are not
controlled from the main page.

- `parser_refresh_seconds`: cadence for external scrapers (ArbitrageScanner, Coinglass, symbol universe).
- `exchange_refresh_seconds`: frequency of exchange adapter polling (reuses the latest scraper data).
- `table_refresh_seconds`: client polling cadence (used by the web UI to fetch snapshots).
- At least one source and exchange must remain enabled.

Execution settings live in `data/execution_settings.json` (auto-generated). They hold wallet balances, allocation brackets, risk thresholds, telemetry paths, etc. You can edit them before launching for different paper balances or allocation heuristics.

## CLI Snapshot Runner
```bash
python main.py
```
Outputs formatted tables plus timestamped CSV/JSON in `data/` and logs under `logs/`.

### Exchange Probe (ad-hoc diagnostics)
```bash
python scripts/exchange_probe.py --symbol BTC --exchanges bybit mexc
```
Writes `data/debug/exchange_probe_<symbol>_<timestamp>.json` with normalized snapshots, depth, and raw API payloads.

### Optional: Prime caches
If first run would spend time downloading Chromium (pyppeteer) or populating caches:
```bash
python -m webapp.manual_refresh
```

## Running the Dashboard
```bash
uvicorn webapp.app:app --reload
# or with explicit venv python:
python -m uvicorn webapp.app:app --reload
```
Visit `http://127.0.0.1:8000/`.

### Dashboard Behaviour
- **Immediate render**: placeholders fade as soon as async events arrive; no blocking UI.
- **Activity log**: backend events (`screener:start`, `exchange:error`, `execution_success`, `risk:pause`, etc.) stream in over the WebSocket feed.
- **Exchange grid**: status chips per adapter (`ok`, `pending`, `failed`, `missing`) and message/last count.
- **Execution panel**: paper balances, reservations, open positions, and live execution log driven by telemetry.
- **Manual refresh**: runs an async snapshot/execute cycle and refreshes tables immediately; scheduled polling keeps REST data in sync.

## Failure Handling
- Missing dependencies or network errors are surfaced as status messages; pipeline continues with partial data.
- Risk guard emits telemetry when tripwires fire (`risk:blocked`, `risk:pause`, `risk:resume`).
- Telemetry queue is resilient: if WebSocket clients drop, they reconnect and receive the backlog.

## Tests
```bash
python -m unittest discover -s tests
```
(Skips async snapshot test if `aiohttp` is unavailable.) Includes orchestrator, trading engine, telemetry, and realtime connection coverage.

## Operational Notes
- Logs: see `logs/app.log` and `state/execution_events.jsonl` (path configurable) for full trace of execution and telemetry events.
- Coinglass scraping still relies on `requests-html`/pyppeteer; Chromium download progress is visible via the telemetry stream.
- Wallet/position state persists under `state/` so the paper balances survive restarts.

## Contributing
- Emit telemetry for any new subsystem; the UI and WebSocket feed rely on structured events.
- New exchange adapters should subclass `exchanges.base.ExchangeAdapter` and populate `MarketSnapshot` with funding, depth, and next funding timestamps.
- Trading clients should implement the async `TradingClient` interface (`execution/adapters.py`).
- Frontend additions should stick to vanilla JS, use the escape helpers, and extend the telemetry renderers where applicable.

## Roadmap Ideas
1. Plug real Bybit/MEXC REST/WebSocket trading clients into `execution/adapters.py`.
2. Persist telemetry/events to a time-series store for historical analysis.
3. Add strategy-level analytics (P&L curves, funding capture) to the dashboard.
4. Package orchestrator/uvicorn in Docker with configurable env overrides.
