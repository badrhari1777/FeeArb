from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from pipeline import (
    collect_snapshot,
    format_coinglass_table,
    format_opportunities,
    format_screener_table,
    format_universe_table,
)
from utils import save_csv, save_json, setup_logging


def main() -> None:
    setup_logging()

    snapshot = collect_snapshot()
    timestamp = snapshot.generated_at.strftime("%Y%m%d_%H%M%S")

    print("=== ArbitrageScanner Top (Binance excluded) ===")
    print(format_screener_table(snapshot.screener_rows[:10]))

    print("\n=== Coinglass Top ===")
    print(format_coinglass_table(snapshot.coinglass_rows[:10]))

    print("\n=== Combined Symbol Universe ===")
    print(format_universe_table(snapshot.universe))

    if snapshot.opportunities:
        print("\n=== Funding Rate Opportunities ===")
        print(format_opportunities(snapshot.opportunities))

        opp_csv = Path("data/opportunities") / f"opportunities_{timestamp}.csv"
        save_csv([
            _opportunity_to_dict(item) for item in snapshot.opportunities
        ], opp_csv)
        logging.info("Opportunities snapshot saved to %s", opp_csv)
    else:
        logging.warning("No funding opportunities detected")

    for exchange, payload in snapshot.raw_payloads.items():
        raw_path = Path("data/raw") / f"{exchange}_{timestamp}.json"
        save_json(payload, raw_path)
        logging.info("Raw %s payload saved to %s", exchange, raw_path)

    if not snapshot.screener_from_cache:
        screener_csv = Path("data/screener") / f"screener_{timestamp}.csv"
        save_csv(snapshot.screener_rows, screener_csv)
        logging.info("Screener snapshot saved to %s", screener_csv)

    if not snapshot.coinglass_from_cache:
        coinglass_csv = Path("data/coinglass") / f"coinglass_{timestamp}.csv"
        save_csv([row.to_dict() for row in snapshot.coinglass_rows], coinglass_csv)
        logging.info("Coinglass snapshot saved to %s", coinglass_csv)


def _opportunity_to_dict(item) -> dict[str, object]:
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
        "price_diff": item.price_diff,
        "price_diff_pct": item.price_diff_pct,
        "effective_spread": item.effective_spread,
        "participants": item.participants,
    }


if __name__ == "__main__":
    main()
