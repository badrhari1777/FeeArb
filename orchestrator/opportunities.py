from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List, Sequence

from orchestrator.models import FundingOpportunity, MarketSnapshot

from exchanges.base import ExchangeAdapter

logger = logging.getLogger(__name__)


def gather_snapshots(
    adapters: Sequence[ExchangeAdapter], symbols: Iterable[str]
) -> tuple[dict[str, dict[str, MarketSnapshot]], dict[str, list[dict]]]:
    snapshots_by_exchange: dict[str, dict[str, MarketSnapshot]] = {}
    raw_payloads: dict[str, list[dict]] = {}

    for adapter in adapters:
        try:
            snapshots = adapter.fetch_market_snapshots(symbols)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("%s adapter failed: %s", adapter.name, exc)
            continue

        snapshots_by_exchange[adapter.name] = {snap.symbol: snap for snap in snapshots}
        raw_payloads[adapter.name] = [snap.raw for snap in snapshots]
        logger.info("%s: fetched %s snapshots", adapter.name, len(snapshots))

    return snapshots_by_exchange, raw_payloads


def compute_opportunities(
    symbols: Iterable[str],
    adapters: Sequence[ExchangeAdapter],
    min_spread: float = 0.0,
) -> tuple[List[FundingOpportunity], dict[str, list[dict]]]:
    snapshots_by_exchange, raw_payloads = gather_snapshots(adapters, symbols)

    opportunities: list[FundingOpportunity] = []

    for symbol in {sym.upper() for sym in symbols}:
        available: list[MarketSnapshot] = []
        for exchange_snapshots in snapshots_by_exchange.values():
            snap = exchange_snapshots.get(symbol)
            if snap is not None:
                available.append(snap)

        if len(available) < 2:
            continue

        longs = [s for s in available if s.funding_rate is not None]
        shorts = longs  # same list for reuse
        if len(longs) < 2:
            continue

        long_snap = max(longs, key=lambda s: s.funding_rate or float("-inf"))
        short_snap = min(shorts, key=lambda s: s.funding_rate or float("inf"))

        if long_snap.funding_rate is None or short_snap.funding_rate is None:
            continue

        spread = long_snap.funding_rate - short_snap.funding_rate
        if spread <= min_spread:
            continue

        opportunities.append(
            FundingOpportunity(
                symbol=symbol,
                long_exchange=long_snap.exchange,
                short_exchange=short_snap.exchange,
                long_rate=long_snap.funding_rate,
                short_rate=short_snap.funding_rate,
                spread=spread,
                long_mark=long_snap.mark_price,
                short_mark=short_snap.mark_price,
                long_bid=long_snap.bid,
                long_ask=long_snap.ask,
                short_bid=short_snap.bid,
                short_ask=short_snap.ask,
                long_next_funding=long_snap.next_funding_time,
                short_next_funding=short_snap.next_funding_time,
                participants=len(available),
            )
        )

    opportunities.sort(key=lambda item: item.spread, reverse=True)
    return opportunities, raw_payloads


def format_opportunity_table(rows: Sequence[FundingOpportunity]) -> str:
    header = (
        f"{'Symbol':<10} {'Long':>12} {'LongRate%':>10} {'Short':>12} "
        f"{'ShortRate%':>11} {'Spread%':>9} {'T+ (hrs)':>9}"
    )
    lines = [header, "-" * len(header)]
    for row in rows:
        hours_to_funding = _hours_until(row.long_next_funding, row.short_next_funding)
        lines.append(
            f"{row.symbol:<10} {row.long_exchange:>12} {row.long_rate * 100:>9.3f}% "
            f"{row.short_exchange:>12} {row.short_rate * 100:>10.3f}% "
            f"{row.spread * 100:>8.3f}% {hours_to_funding:>9.2f}"
        )
    return "\n".join(lines)


def _hours_until(*times: datetime | None) -> float:
    valid = [t for t in times if t is not None]
    if not valid:
        return 0.0
    from datetime import datetime as _dt

    now = _dt.utcnow().replace(tzinfo=valid[0].tzinfo)
    deltas = [(t - now).total_seconds() / 3600 for t in valid]
    positive = [d for d in deltas if d > 0]
    if not positive:
        return 0.0
    return min(positive)
