# FeeArb

Funding fee arbitrage research playground.

## Current Capabilities
- Pull promising symbols from `screener.arbitragescanner.io` with Binance excluded.
- Re-validate the shortlist on Bybit and MEXC via public REST endpoints (funding rate + mark price).
- Render terminal tables, keep an activity log, and persist outputs as CSV/JSON snapshots.
- Provide `.env` scaffolding for future API keys.

## Project Layout

```
.
├── main.py                      # Pipeline runner (screen → validate → report)
├── parsers/
│   └── arbitragescanner.py      # Aggregator fetch + coarse filtering
├── exchanges/
│   ├── __init__.py
│   ├── base.py                  # Exchange adapter interface
│   ├── bybit.py                 # Bybit public REST adapter
│   └── mexc.py                  # MEXC public REST adapter
├── orchestrator/
│   ├── __init__.py
│   ├── models.py                # Shared dataclasses
│   └── realtime_validator.py    # Combines exchange data into a leaderboard
├── utils/
│   ├── __init__.py
│   ├── io.py                    # CSV/JSON helpers
│   └── logging.py               # Console + file logging setup
├── .env                         # Blank API key slots (ignored by Git)
├── .env.example                 # Template for .env
└── README.md

# Generated at runtime:
# ├── data/raw/                  # Raw exchange payloads
# ├── data/screener/             # Screener snapshots
# ├── data/validated/            # Validation snapshots
# └── logs/app.log               # Process log
```

## How to Run

```powershell
.\.venv\Scripts\Activate.ps1    # optional
python .\main.py
```

You will see two tables in the terminal (screener vs. validated) and timestamped CSV/JSON files inside `data/`. The activity log lives in `logs/app.log`.

## Environment Variables

- Use `.env` (or copy from `.env.example`) to populate `BYBIT_API_KEY`, `BYBIT_API_SECRET`, `MEXC_API_KEY`, `MEXC_API_SECRET` once private endpoints are required.
- Add further settings (alerts, storage, rate limits) as the project grows.

## Next Steps

1. Add WebSocket subscriptions for live updates from Bybit and MEXC.
2. Enrich validation with liquidity metrics (volume, open interest, bid/ask spread).
3. Implement private API adapters and a paper-trading executor.
4. Extend monitoring (alerts, health checks, metrics).

