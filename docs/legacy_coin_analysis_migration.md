# Legacy Coin Analysis migration boundary

## Status

The legacy Coin Analysis product runtime was retired on 2026-08-13. It no
longer owns background tasks, production routes, browser controls, paper
positions, decisions, outcome evaluation, retention, focus collection, or
position watching.

## Data that must be preserved

- `state/coin_analysis.db` and its WAL/shm files are historical research input.
- The `analysis_storage`, `analysis_features`, `analysis_decisions`, and
  `analysis_registry` packages remain offline compatibility code while Strategy
  Lab still reads their schema or uses their deterministic transforms.
- Existing database/storage tests remain the schema guard. Removing those
  packages or migrating the database requires a separate, reproducible Strategy
  Lab import and row-count/checksum comparison.
- Historical logs and exports are not runtime configuration and must not be
  deleted as part of web cleanup.

## Production boundary

- No `/api/coin/*` route exists.
- `/api/manual/test/coin-analysis` and its manual-test UI do not exist.
- `DataService` does not import the legacy analysis packages or write the legacy
  database.
- Manual Spread Monitor and manual Funding History remain active operator tools.
  Funding History returns its result to the caller and cache layer but does not
  dual-write the legacy Coin Analysis database.
- Strategy Lab owns all new candidate observation and research datasets. It may
  read the historical Coin Analysis database in read-only mode only.

## Future removal gate

Delete the offline compatibility packages only after all of the following are
true:

1. Strategy Lab has a versioned replacement schema and importer.
2. A frozen migration report proves source row counts, time ranges, symbols,
   checksums, and rejected-row reasons.
3. No Strategy Lab script or test imports the legacy packages.
4. The original database remains in a documented backup/archive location.
