# FeeArb

Research tooling for cross-exchange funding-rate arbitrage. The latest architecture is fully asynchronous end-to-end: data ingestion, signal generation, execution, and telemetry all run on asyncio primitives, while a FastAPI dashboard streams progress (and fills) to the browser over WebSockets. Bybit and MEXC USDT perpetuals remain the default active venues, with additional adapters scaffolded for future expansion.

## Highlights
- **Async ingestion** – `pipeline/data_pipeline.py` fetches ArbitrageScanner and Coinglass concurrently via `aiohttp`, merges caches, and emits granular progress events.
- **Execution layer** – `execution/` houses wallet/reservation services, position lifecycle management, an extensible `ExecutionEngine`, trading clients (simulated for now), and a fill simulator that estimates slippage/orphan risk.
- **Risk & telemetry** – risk guard enforces leverage/orphan/slippage policies and emits telemetry (`risk:*` events). The `TelemetryClient` is asynchronous, persisting JSONL logs and broadcasting events to subscribers.
- **Real-time dashboard** – FastAPI serves the state, while a WebSocket feed (`/ws/telemetry`) streams live activity into the UI (execution log, risk alerts, etc.). Manual refresh remains available but no longer blocks the page.
- **Testing** – unit tests cover async snapshot collection, trading engine, orchestrator, telemetry, and other subsystems (`python -m unittest discover -s tests`).

## Architecture Overview
```
ArbitrageScanner  ┐
Coinglass         │  (async fetch via aiohttp)        ┌─> SignalEngine ┐
Exchange adapters ┘ ─> collect_snapshot_async -> DataSnapshot -> .. -> ExecutionEngine -> TradingClient(s) -> Fill telemetry
                                                                               │
                                            LifecycleController <--------------┘
                                                                    │
                                            TelemetryClient (async queue => JSONL + WebSocket broadcast)
```

## Project Layout
```
.
|-- main.py                      # CLI snapshot runner (blocking wrapper around async collector)
|-- pipeline/
|   `-- data_pipeline.py         # Async aggregation of sources + exchange opportunities
|-- orchestrator/
|   |-- models.py                # Dataclasses shared across systems
|   `-- opportunities.py         # Exchange polling + opportunity builder
|-- execution/
|   |-- adapters.py              # Trading client interfaces (simulated + stubs for real exchange APIs)
|   |-- allocator.py             # Wallet reservations, cooldown tracking
|   |-- engine.py                # Strategy execution, order submission, position updates
|   |-- fills.py                 # Fill simulator (slippage/orphan estimation)
|   |-- lifecycle.py             # Position observation + exit rules
|   |-- market.py                # Websocket-ready market gateway / derived metrics
|   |-- orders.py                # Order/Fills dataclasses
|   |-- orchestrator.py          # Glue: snapshot -> decisions -> execution
|   |-- risk.py                  # Risk guard emitting telemetry
|   `-- telemetry.py             # Async telemetry queue + JSONL writer
|-- parsers/                     # ArbitrageScanner & Coinglass scrapers
|-- exchanges/                   # Funding snapshot adapters (Bybit + MEXC active)
|-- webapp/
|   |-- app.py                   # FastAPI routes + WebSocket endpoint
|   |-- services.py              # Data service (async scheduler, telemetry bridge)
|   |-- realtime.py              # WebSocket connection manager
|   |-- templates/index.html     # Dashboard
|   `-- static/app.js            # Vanilla JS frontend (polling + websocket)
|-- tests/                       # pytest/unittest suite (async-friendly)
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
`data/settings.json` is created on first run. Toggle sources/exchanges or adjust refresh timers via the dashboard's **Configuration** panel or by editing the file manually.

- `parser_refresh_seconds`: backend orchestration loop interval (snapshot + orchestrator).
- `table_refresh_seconds`: client polling cadence (still used for REST refreshes).
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
