# FeeArb

Research tooling for cross-exchange funding-rate arbitrage. The project now ships with a FastAPI-powered dashboard that streams progress from every upstream source, so long-running scrapes and partial exchange data no longer block the UI.

## Feature Highlights
- Parse ArbitrageScanner and Coinglass leaderboards (with caching) to build a combined symbol universe.
- Poll exchange adapters sequentially, assemble synthetic funding opportunities, and emit structured progress events (`exchange:start`, `exchange:success`, `exchange:error`, etc.).
- Persist rich `DataSnapshot` objects that include opportunities, universe membership, raw payloads, exchange status, and pipeline messages.
- Serve a reactive frontend (FastAPI + vanilla JS) with:
  - Empty-state placeholders instead of hard waits during initial loads.
  - Status pill, activity timeline, and exchange response grid that update as soon as each stage completes.
  - Manual refresh button that triggers backend refreshes without freezing the page.

## Project Layout
```
.
├── main.py                     # CLI pipeline runner
├── pipeline/
│   ├── __init__.py
│   └── data_pipeline.py        # Snapshot assembly + progress hooks
├── orchestrator/
│   ├── models.py               # Dataclasses used across the pipeline
│   └── opportunities.py        # Exchange polling + opportunity builder
├── parsers/                    # ArbitrageScanner & Coinglass scrapers
├── exchanges/                  # Adapter implementations (Bybit, MEXC, ...)
├── webapp/
│   ├── app.py                  # FastAPI routing + templates
│   ├── services.py             # Async scheduler & event tracking
│   ├── templates/index.html    # Dashboard layout
│   └── static/                 # Frontend JS/CSS
├── utils/                      # Logging, caching, I/O helpers
├── .env.example                # Environment template
└── README.md

# Runtime artifacts (ignored by Git)
# ├── data/                     # Cached screener/coinglass + raw exchange payloads
# └── logs/                     # Application logs
```

## Setup
```powershell
python -m venv .\.venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env          # adjust if private endpoints are needed later
```

## CLI Snapshot Runner
```powershell
python .\main.py
```
Outputs: terminal tables plus timestamped CSV/JSON in `data/` and logs in `logs/app.log`.

## Pre-populating Cache (optional)
If the first web request would spend time downloading Chromium (pyppeteer) or populating caches, run:
```powershell
python -m webapp.manual_refresh
```
This performs a one-off snapshot so the dashboard renders immediately on its first load.

## Running the Web Dashboard
```powershell
.\.venv\Scripts\uvicorn webapp.app:app --reload
```
Navigate to `http://127.0.0.1:8000/`.

### Dashboard Behavior
- **Immediate render:** The page shows a muted placeholder section until data arrives; no blocking while the backend works.
- **Progress tracking:** Each pipeline stage emits events that appear in the activity log (`screener:complete`, `exchange:error`, `snapshot:ready`, etc.).
- **Exchange grid:** Displays every adapter with live status chips (`ok`, `pending`, `failed`, `missing`) and snapshot counts/error messages.
- **Manual refresh:** Clicking the refresh button triggers a backend refresh, disables the button during execution, and re-renders each table as soon as new data is available. Automatic polling keeps data current using the configured refresh interval.

## Failure Handling
- Missing or failed exchanges no longer block the snapshot; partial data still renders.
- The dashboard highlights exchanges with issues and surfaces the latest error message.
- Pipeline messages also note when scraping sources (ArbitrageScanner, Coinglass) return empty results.

## Operational Notes
- Logs: see `logs/app.log` for detailed trace output, including any stack traces during adapter failures.
- Pyppeteer: Coinglass scraping may download Chromium on first run. Progress events keep the UI informed during this stage.
- Cache: Screener and Coinglass responses persist under `data/` with TTL enforcement to avoid repeated heavy scrapes.

## Contributing
- `pipeline/data_pipeline.py` exposes a `progress_cb` hook; new stages should emit structured events for the UI.
- New exchange adapters should subclass `exchanges.base.ExchangeAdapter` and return `MarketSnapshot` instances populated with funding rates, bid/ask, mark, and next funding times.
- Frontend additions should stick to vanilla JS and sanitize DOM output with the existing helper functions.

## Roadmap Ideas
1. Parallelize exchange polling with per-adapter timeouts to shorten refresh windows.
2. Add WebSocket streaming for exchanges that provide live funding updates.
3. Surface opportunity deltas (compare with previous snapshot) and alert thresholds.
4. Package the dashboard for container deployment with optional HTTPS termination.
