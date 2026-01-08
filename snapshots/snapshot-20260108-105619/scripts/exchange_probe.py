from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import SUPPORTED_EXCHANGES
from exchanges import get_adapter, normalize_exchange_name
from orchestrator.models import MarketSnapshot
from utils import setup_logging

logger = logging.getLogger("exchange_probe")

DEFAULT_BASE_SYMBOL = "BTC"
DEFAULT_SUFFIXES = ("USDT",)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch funding-related market snapshots for each configured exchange "
            "and store the raw payloads for inspection."
        )
    )
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        help="Base symbol or fully-qualified contract (default: BTC). "
        "Provide multiple --symbol options to query several bases.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path for the resulting JSON file. "
        "Defaults to data/debug/exchange_probe_<symbol>_<timestamp>.json.",
    )
    parser.add_argument(
        "--exchanges",
        nargs="*",
        help="Optional list of exchanges to poll (defaults to all supported exchanges).",
    )
    return parser.parse_args()


def _expand_symbols(symbols: Sequence[str]) -> list[str]:
    """Return canonical tickers that match the adapter expectations."""
    expanded: set[str] = set()
    for raw in symbols:
        candidate = raw.upper().replace("/", "").strip()
        if not candidate:
            continue
        if any(candidate.endswith(suffix) for suffix in DEFAULT_SUFFIXES):
            expanded.add(candidate)
            continue
        for suffix in DEFAULT_SUFFIXES:
            expanded.add(f"{candidate}{suffix}")
    return sorted(expanded)


def _snapshot_to_dict(snapshot: MarketSnapshot) -> dict[str, object]:
    return {
        "symbol": snapshot.symbol,
        "exchange_symbol": snapshot.exchange_symbol,
        "funding_rate": snapshot.funding_rate,
        "next_funding_time": (
            snapshot.next_funding_time.isoformat() if snapshot.next_funding_time else None
        ),
        "funding_interval_hours": snapshot.funding_interval_hours,
        "mark_price": snapshot.mark_price,
        "bid": snapshot.bid,
        "ask": snapshot.ask,
        "bid_size": snapshot.bid_size,
        "ask_size": snapshot.ask_size,
        "raw": snapshot.raw,
    }


def _collect_snapshots(
    exchanges: Iterable[str], symbols: Sequence[str]
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []

    for name in exchanges:
        canonical = normalize_exchange_name(name)
        try:
            adapter = get_adapter(canonical)
        except KeyError as exc:
            logger.error("No adapter registered for exchange %s: %s", name, exc)
            entries.append(
                {
                    "name": name,
                    "status": "missing_adapter",
                    "error": str(exc),
                    "snapshots": [],
                }
            )
            continue

        logger.info("Fetching snapshots for %s", adapter.name)
        try:
            snapshots = adapter.fetch_market_snapshots(symbols)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("%s adapter failed: %s", adapter.name, exc)
            entries.append(
                {
                    "name": adapter.name,
                    "status": "error",
                    "error": str(exc),
                    "snapshots": [],
                }
            )
            continue

        entries.append(
            {
                "name": adapter.name,
                "status": "ok",
                "symbol_count": len(snapshots),
                "snapshots": [_snapshot_to_dict(item) for item in snapshots],
            }
        )

    return entries


def main() -> None:
    args = parse_args()
    setup_logging()

    requested_symbols = args.symbols or [DEFAULT_BASE_SYMBOL]
    canonical_symbols = _expand_symbols(requested_symbols)
    if not canonical_symbols:
        raise SystemExit("No valid symbols provided.")

    exchanges = (
        args.exchanges
        if args.exchanges
        else SUPPORTED_EXCHANGES
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = args.output
    if output_path is None:
        first_symbol = requested_symbols[0].upper().replace("/", "")
        output_path = Path("data/debug") / f"exchange_probe_{first_symbol.lower()}_{timestamp}.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "requested_symbols": requested_symbols,
        "canonical_symbols": canonical_symbols,
        "exchanges_polled": list(exchanges),
        "results": _collect_snapshots(exchanges, canonical_symbols),
    }

    output_path.write_text(json.dumps(payload, indent=2))
    print(f"Saved exchange snapshots to {output_path}")


if __name__ == "__main__":
    main()
