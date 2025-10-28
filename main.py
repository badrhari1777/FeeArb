from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from orchestrator import format_validation_table, validate_candidates
from orchestrator.models import ValidationResult
from parsers import arbitragescanner, coinglass
from utils import save_csv, save_json, setup_logging


def main() -> None:
    setup_logging()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logging.info("Fetching candidates from ArbitrageScanner...")
    data = arbitragescanner.fetch_json()
    screener_top = arbitragescanner.build_top(data, exclude=("binance",), limit=20)
    logging.info("Screener returned %s candidates", len(screener_top))

    print("=== ArbitrageScanner Top 10 (Binance excluded) ===")
    print(arbitragescanner.format_table(screener_top[:10]))

    screener_csv = Path("data/screener") / f"screener_{timestamp}.csv"
    save_csv(screener_top, screener_csv)
    logging.info("Screener snapshot saved to %s", screener_csv)

    logging.info("Fetching candidates from Coinglass...")
    try:
        coinglass_rows = coinglass.fetch_rows(limit=20)
        logging.info("Coinglass returned %s candidates", len(coinglass_rows))
    except Exception:
        logging.exception("Failed to fetch Coinglass data")
        coinglass_rows = []

    if coinglass_rows:
        print("\n=== Coinglass Top 10 ===")
        print(coinglass.format_table(coinglass_rows[:10]))

        coinglass_csv = Path("data/coinglass") / f"coinglass_{timestamp}.csv"
        save_csv([row.to_dict() for row in coinglass_rows], coinglass_csv)
        logging.info("Coinglass snapshot saved to %s", coinglass_csv)

    combined_top = _build_combined_top(screener_top[:10], coinglass_rows[:10])

    if combined_top:
        print("\n=== Combined Top 10 (Scanner + Coinglass) ===")
        print(_format_combined_table(combined_top))
    else:
        logging.warning("No combined candidates available")

    candidate_symbols = [item["symbol"] for item in combined_top]
    logging.info("Validating %s symbols on Bybit and MEXC", len(candidate_symbols))

    results, raw = validate_candidates(candidate_symbols, top_limit=10)

    if not results:
        logging.warning("No overlapping markets between Bybit and MEXC")
        return

    print("\n=== Validated Top (Bybit vs MEXC) ===")
    print(format_validation_table(results))

    validated_csv = Path("data/validated") / f"validated_{timestamp}.csv"
    save_csv([_result_to_dict(r) for r in results], validated_csv)
    logging.info("Validation snapshot saved to %s", validated_csv)

    for exchange, payload in raw.items():
        raw_path = Path("data/raw") / f"{exchange}_{timestamp}.json"
        save_json(payload, raw_path)
        logging.info("Raw %s payload saved to %s", exchange, raw_path)


def _build_combined_top(
    scanner_rows: list[dict[str, object]],
    coinglass_rows: list[coinglass.CoinglassRow],
) -> list[dict[str, object]]:
    combined: dict[str, dict[str, object]] = {}

    for item in scanner_rows:
        symbol = item["symbol"]
        entry = {
            "symbol": symbol,
            "source": "arbitragescanner",
            "spread": float(item["spread"]),
            "long_exchange": item["long_exchange"],
            "short_exchange": item["short_exchange"],
            "details": (
                f"{float(item['long_fee']) * 100:.2f}% vs "
                f"{float(item['short_fee']) * 100:.2f}%"
            ),
        }
        combined[symbol] = entry

    for row in coinglass_rows:
        spread = row.net_funding_rate
        if spread <= 0:
            continue
        entry = {
            "symbol": row.symbol,
            "source": "coinglass",
            "spread": spread,
            "long_exchange": row.long_exchange or "N/A",
            "short_exchange": row.short_exchange or "N/A",
            "details": (
                f"APR {row.apr * 100:.1f}%, Spread {row.spread_rate * 100:.2f}%"
            ),
        }
        existing = combined.get(row.symbol)
        if not existing or spread > existing["spread"]:
            combined[row.symbol] = entry

    sorted_rows = sorted(combined.values(), key=lambda x: x["spread"], reverse=True)
    return sorted_rows[:10]


def _format_combined_table(rows: list[dict[str, object]]) -> str:
    header = (
        f"{'Symbol':<10} {'Source':<16} {'Spread%':>9} "
        f"{'Long':>14} {'Short':>14} {'Details':<32}"
    )
    lines = [header, "-" * len(header)]
    for row in rows:
        spread_pct = float(row["spread"]) * 100
        lines.append(
            f"{row['symbol']:<10} {row['source']:<16} {spread_pct:>8.2f}% "
            f"{row['long_exchange']:>14} {row['short_exchange']:>14} "
            f"{row['details']:<32}"
        )
    return "\n".join(lines)


def _result_to_dict(result: ValidationResult) -> dict[str, object]:
    def fmt_dt(value: datetime | None) -> str:
        return value.isoformat() if value is not None else ""

    return {
        "symbol": result.symbol,
        "long_exchange": result.long_exchange,
        "long_rate": result.long_rate,
        "long_mark": result.long_mark,
        "long_next_funding": fmt_dt(result.long_next_funding),
        "short_exchange": result.short_exchange,
        "short_rate": result.short_rate,
        "short_mark": result.short_mark,
        "short_next_funding": fmt_dt(result.short_next_funding),
        "spread": result.spread,
    }


if __name__ == "__main__":
    main()
