from __future__ import annotations

import logging
from typing import Iterable, Sequence

from exchanges import BybitAdapter, MexcAdapter
from orchestrator.models import MarketSnapshot, ValidationResult


logger = logging.getLogger(__name__)


def validate_candidates(
    candidates: Sequence[str],
    *,
    top_limit: int = 10,
) -> tuple[list[ValidationResult], dict[str, list[dict]]]:
    """Fetch snapshots from Bybit and MEXC and build a funding spread leaderboard."""

    adapters = [BybitAdapter(), MexcAdapter()]
    snapshots_by_exchange: dict[str, dict[str, MarketSnapshot]] = {}
    raw_by_exchange: dict[str, list[dict]] = {}

    for adapter in adapters:
        snapshots = adapter.fetch_market_snapshots(candidates)
        logger.info("%s: fetched %s snapshots", adapter.name, len(snapshots))
        snapshots_by_exchange[adapter.name] = {snap.symbol: snap for snap in snapshots}
        raw_by_exchange[adapter.name] = [snap.raw for snap in snapshots]

    results: list[ValidationResult] = []

    for symbol in candidates:
        bybit_snap = snapshots_by_exchange.get("bybit", {}).get(symbol)
        mexc_snap = snapshots_by_exchange.get("mexc", {}).get(symbol)

        if not bybit_snap or not mexc_snap:
            logger.debug("Skip %s: missing data on one of the exchanges", symbol)
            continue

        if bybit_snap.funding_rate is None or mexc_snap.funding_rate is None:
            logger.debug("Skip %s: funding rate missing", symbol)
            continue

        long_snap, short_snap = (bybit_snap, mexc_snap)
        if (bybit_snap.funding_rate or 0) < (mexc_snap.funding_rate or 0):
            long_snap, short_snap = short_snap, long_snap

        spread = (long_snap.funding_rate or 0) - (short_snap.funding_rate or 0)

        results.append(
            ValidationResult(
                symbol=symbol,
                long_exchange=long_snap.exchange,
                short_exchange=short_snap.exchange,
                long_rate=long_snap.funding_rate or 0.0,
                short_rate=short_snap.funding_rate or 0.0,
                spread=spread,
                long_mark=long_snap.mark_price,
                short_mark=short_snap.mark_price,
                long_next_funding=long_snap.next_funding_time,
                short_next_funding=short_snap.next_funding_time,
            )
        )

    results.sort(key=lambda r: r.spread, reverse=True)
    return results[:top_limit], raw_by_exchange


def format_validation_table(results: Iterable[ValidationResult]) -> str:
    header = (
        f"{'Symbol':<10} {'Spread':>10} "
        f"{'Long':>12} {'LongRate':>10} {'LongMark':>12} "
        f"{'Short':>12} {'ShortRate':>10} {'ShortMark':>12}"
    )
    lines = [header, "-" * len(header)]
    for item in results:
        lines.append(
            f"{item.symbol:<10} {item.spread:>10.6f} "
            f"{item.long_exchange:>12} {item.long_rate:>10.6f} {(_fmt_price(item.long_mark)):>12} "
            f"{item.short_exchange:>12} {item.short_rate:>10.6f} {(_fmt_price(item.short_mark)):>12}"
        )
    return "\n".join(lines)


def _fmt_price(value: float | None) -> str:
    return f"{value:.4f}" if value is not None else "-"

