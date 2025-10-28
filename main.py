from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from config import PARSE_CACHE_TTL_SECONDS, SUPPORTED_EXCHANGES
from exchanges import get_adapter, normalize_exchange_name
from orchestrator.opportunities import compute_opportunities, format_opportunity_table
from orchestrator.models import FundingOpportunity
from parsers import arbitragescanner, coinglass
from utils import load_cache, save_cache, save_csv, save_json, setup_logging


def main() -> None:
    setup_logging()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    screener_rows, screener_from_cache = _load_screener_snapshot(timestamp)
    coinglass_rows, coinglass_from_cache = _load_coinglass_snapshot(timestamp)

    print("=== ArbitrageScanner Top (Binance excluded) ===")
    print(arbitragescanner.format_table(screener_rows[:10]))
    print("\n=== Coinglass Top ===")
    print(coinglass.format_table(coinglass_rows[:10]))

    universe = _build_symbol_universe(screener_rows, coinglass_rows)
    print("\n=== Combined Symbol Universe ===")
    print(_format_symbol_universe(universe))

    symbols = [entry["symbol"] for entry in universe]
    if not symbols:
        logging.warning("No symbols available for opportunity scan")
        return

    adapters = []
    seen_exchanges = set()
    for name in SUPPORTED_EXCHANGES:
        canonical = normalize_exchange_name(name)
        if canonical in seen_exchanges:
            continue
        seen_exchanges.add(canonical)
        try:
            adapters.append(get_adapter(canonical))
        except KeyError:
            logging.warning("No adapter implemented for %s", name)
    if not adapters:
        logging.error("No exchange adapters available")
        return

    opportunities, raw_payloads = compute_opportunities(symbols, adapters)
    if opportunities:
        print("\n=== Funding Rate Opportunities ===")
        print(format_opportunity_table(opportunities))

        opp_csv = Path("data/opportunities") / f"opportunities_{timestamp}.csv"
        save_csv([_opportunity_to_dict(item) for item in opportunities], opp_csv)
        logging.info("Opportunities snapshot saved to %s", opp_csv)
    else:
        logging.warning("No funding opportunities detected")

    for exchange, payload in raw_payloads.items():
        raw_path = Path("data/raw") / f"{exchange}_{timestamp}.json"
        save_json(payload, raw_path)
        logging.info("Raw %s payload saved to %s", exchange, raw_path)

    if not screener_from_cache:
        screener_csv = Path("data/screener") / f"screener_{timestamp}.csv"
        save_csv(screener_rows, screener_csv)
        logging.info("Screener snapshot saved to %s", screener_csv)

    if not coinglass_from_cache:
        coinglass_csv = Path("data/coinglass") / f"coinglass_{timestamp}.csv"
        save_csv([row.to_dict() for row in coinglass_rows], coinglass_csv)
        logging.info("Coinglass snapshot saved to %s", coinglass_csv)


def _load_screener_snapshot(timestamp: str) -> tuple[list[dict], bool]:
    cached = load_cache("screener_latest.json", PARSE_CACHE_TTL_SECONDS)
    if cached:
        logging.info("Using cached ArbitrageScanner data")
        return list(cached["data"]), True

    logging.info("Fetching candidates from ArbitrageScanner...")
    data = arbitragescanner.fetch_json()
    rows = arbitragescanner.build_top(data, exclude=("binance",), limit=20)
    save_cache("screener_latest.json", {"data": rows, "fetched_at": timestamp})
    logging.info("Screener returned %s candidates", len(rows))
    return rows, False


def _load_coinglass_snapshot(timestamp: str) -> tuple[list[coinglass.CoinglassRow], bool]:
    cached = load_cache("coinglass_latest.json", PARSE_CACHE_TTL_SECONDS)
    if cached:
        logging.info("Using cached Coinglass data")
        rows = [coinglass.CoinglassRow(**item) for item in cached["data"]]
        return rows, True

    logging.info("Fetching candidates from Coinglass...")
    rows = coinglass.fetch_rows(limit=20)
    save_cache("coinglass_latest.json", {"data": [asdict(row) for row in rows], "fetched_at": timestamp})
    logging.info("Coinglass returned %s candidates", len(rows))
    return rows, False


def _build_symbol_universe(
    screener_rows: list[dict], coinglass_rows: list[coinglass.CoinglassRow]
) -> list[dict[str, object]]:
    universe: dict[str, set[str]] = {}
    for item in screener_rows:
        universe.setdefault(item["symbol"], set()).add("arbitragescanner")
    for row in coinglass_rows:
        universe.setdefault(row.symbol, set()).add("coinglass")
    return [
        {"symbol": symbol, "sources": ", ".join(sorted(sources))}
        for symbol, sources in sorted(universe.items())
    ]


def _format_symbol_universe(rows: Iterable[dict[str, object]]) -> str:
    header = f"{'Symbol':<10} {'Sources':<32}"
    lines = [header, "-" * len(header)]
    for row in rows:
        lines.append(f"{row['symbol']:<10} {row['sources']:<32}")
    return "\n".join(lines)


def _opportunity_to_dict(item: FundingOpportunity) -> dict[str, object]:
    def fmt(value: datetime | None) -> str:
        return value.isoformat() if value else ""

    return {
        "symbol": item.symbol,
        "long_exchange": item.long_exchange,
        "long_rate": item.long_rate,
        "short_exchange": item.short_exchange,
        "short_rate": item.short_rate,
        "spread": item.spread,
        "long_mark": item.long_mark,
        "short_mark": item.short_mark,
        "long_bid": item.long_bid,
        "long_ask": item.long_ask,
        "short_bid": item.short_bid,
        "short_ask": item.short_ask,
        "long_next_funding": fmt(item.long_next_funding),
        "short_next_funding": fmt(item.short_next_funding),
        "participants": item.participants,
    }


if __name__ == "__main__":
    main()
