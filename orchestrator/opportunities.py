from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, List, Sequence

from orchestrator.models import FundingOpportunity, MarketSnapshot

from exchanges.base import ExchangeAdapter

logger = logging.getLogger(__name__)


ProgressCallback = Callable[[str, dict[str, Any]], None]


def _emit(progress_cb: ProgressCallback | None, event: str, payload: dict[str, Any]) -> None:
    if progress_cb:
        progress_cb(event, payload)


async def gather_snapshots(
    adapters: Sequence[ExchangeAdapter],
    symbols: Iterable[str],
    progress_cb: ProgressCallback | None = None,
) -> tuple[dict[str, dict[str, MarketSnapshot]], dict[str, list[dict]], List[dict[str, Any]]]:
    snapshots_by_exchange: dict[str, dict[str, MarketSnapshot]] = {}
    raw_payloads: dict[str, list[dict]] = {}
    status_entries: list[dict[str, Any]] = []

    async def _run_adapter(adapter: ExchangeAdapter) -> None:
        _emit(
            progress_cb,
            "exchange:start",
            {
                "exchange": adapter.name,
                "message": f"Querying {adapter.name}...",
            },
        )
        try:
            snapshots = await adapter.fetch_market_snapshots_async(symbols)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("%s adapter failed: %s", adapter.name, exc)
            status = {
                "exchange": adapter.name,
                "status": "failed",
                "error": str(exc),
            }
            status_entries.append(status)
            _emit(
                progress_cb,
                "exchange:error",
                {
                    "exchange": adapter.name,
                    "message": f"{adapter.name} failed: {exc}",
                    "error": str(exc),
                },
            )
            return

        snapshots_by_exchange[adapter.name] = {snap.symbol: snap for snap in snapshots}
        raw_payloads[adapter.name] = [snap.raw for snap in snapshots]
        status = {
            "exchange": adapter.name,
            "status": "ok",
            "count": len(snapshots),
        }
        status_entries.append(status)
        logger.info("%s: fetched %s snapshots", adapter.name, len(snapshots))
        _emit(
            progress_cb,
            "exchange:success",
            {
                "exchange": adapter.name,
                "message": f"{adapter.name}: fetched {len(snapshots)} snapshots",
                "count": len(snapshots),
            },
        )

    await asyncio.gather(*(_run_adapter(adapter) for adapter in adapters))

    missing = [
        {
            "exchange": adapter.name,
            "status": "missing",
            "error": "no data returned",
        }
        for adapter in adapters
        if adapter.name not in {entry["exchange"] for entry in status_entries}
    ]
    if missing:
        status_entries.extend(missing)
        for entry in missing:
            _emit(
                progress_cb,
                "exchange:missing",
                {
                    "exchange": entry["exchange"],
                    "message": f"{entry['exchange']}: no data returned",
                },
            )

    _emit(
        progress_cb,
        "exchange:complete",
        {
            "message": "Exchange polling finished",
            "summary": status_entries,
        },
    )

    return snapshots_by_exchange, raw_payloads, status_entries


async def compute_opportunities(
    symbols: Iterable[str],
    adapters: Sequence[ExchangeAdapter],
    min_spread: float = 0.0,
    progress_cb: ProgressCallback | None = None,
) -> tuple[List[FundingOpportunity], dict[str, list[dict]], List[dict[str, Any]]]:
    snapshots_by_exchange, raw_payloads, status_entries = await gather_snapshots(
        adapters, symbols, progress_cb=progress_cb
    )

    opportunities: list[FundingOpportunity] = []

    for symbol in {sym.upper() for sym in symbols}:
        available: list[MarketSnapshot] = []
        for exchange_snapshots in snapshots_by_exchange.values():
            snap = exchange_snapshots.get(symbol)
            if snap is None:
                continue
            available.append(snap)

        if len(available) < 2:
            continue

        available.sort(key=lambda item: item.funding_rate or 0.0, reverse=True)
        short_snap = available[0]
        long_snap = available[-1]

        if long_snap.funding_rate is None or short_snap.funding_rate is None:
            continue

        spread = short_snap.funding_rate - long_snap.funding_rate
        if spread < min_spread:
            continue

        long_buy_price = _preferred_price(
            long_snap.ask, long_snap.mark_price, long_snap.bid
        )
        short_sell_price = _preferred_price(
            short_snap.bid, short_snap.mark_price, short_snap.ask
        )
        price_diff, price_diff_pct = _price_metrics(long_buy_price, short_sell_price)
        effective_spread = spread + price_diff_pct

        long_liquidity_usd = _liquidity_in_usd(long_snap.ask_size, long_snap.ask)
        short_liquidity_usd = _liquidity_in_usd(short_snap.bid_size, short_snap.bid)

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
                long_ask=long_snap.ask,
                long_liquidity=long_snap.ask_size,
                long_liquidity_usd=long_liquidity_usd,
                short_bid=short_snap.bid,
                short_liquidity=short_snap.bid_size,
                short_liquidity_usd=short_liquidity_usd,
                long_next_funding=long_snap.next_funding_time,
                short_next_funding=short_snap.next_funding_time,
                long_funding_interval_hours=long_snap.funding_interval_hours,
                short_funding_interval_hours=short_snap.funding_interval_hours,
                price_diff=price_diff,
                price_diff_pct=price_diff_pct,
                effective_spread=effective_spread,
                participants=len(available),
            )
        )

    opportunities.sort(key=lambda item: item.spread, reverse=True)
    return opportunities, raw_payloads, status_entries


def format_opportunity_table(rows: Sequence[FundingOpportunity]) -> str:
    header = (
        f"{'Symbol':<10} {'Long':>10} {'LongRate%':>10} {'LongAsk':>10} {'LongUSDT':>12} {'LongNext':>20} {'LongInt(h)':>12} "
        f"{'Short':>10} {'ShortRate%':>11} {'ShortBid':>10} {'ShortUSDT':>12} {'ShortNext':>20} {'ShortInt(h)':>13} "
        f"{'Spread%':>9} {'PxGap%':>8} {'Eff%':>8} {'Parts':>6}"
    )
    lines = [header, "-" * len(header)]
    for row in rows:
        long_next = _format_next_funding(row.long_next_funding)
        short_next = _format_next_funding(row.short_next_funding)
        long_ask = _fmt_numeric(row.long_ask, decimals=4, width=10)
        long_liq = _fmt_numeric(row.long_liquidity_usd, decimals=2, width=12)
        short_bid = _fmt_numeric(row.short_bid, decimals=4, width=10)
        short_liq = _fmt_numeric(row.short_liquidity_usd, decimals=2, width=12)
        long_interval = _fmt_numeric(row.long_funding_interval_hours, decimals=2, width=12)
        short_interval = _fmt_numeric(row.short_funding_interval_hours, decimals=2, width=13)
        lines.append(
            f"{row.symbol:<10} {row.long_exchange:>10} {row.long_rate * 100:>10.3f}% "
            f"{long_ask} {long_liq} {long_next:>20} {long_interval} "
            f"{row.short_exchange:>10} {row.short_rate * 100:>11.3f}% "
            f"{short_bid} {short_liq} {short_next:>20} {short_interval} "
            f"{row.spread * 100:>9.3f}% {row.price_diff_pct * 100:>7.3f}% "
            f"{row.effective_spread * 100:>7.3f}% {row.participants:>6}"
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


def _preferred_price(*candidates: float | None) -> float | None:
    for value in candidates:
        if value is not None and value > 0:
            return value
    return None


def _price_metrics(long_price: float | None, short_price: float | None) -> tuple[float, float]:
    if long_price is None or short_price is None:
        return 0.0, 0.0
    price_diff = short_price - long_price
    avg = (long_price + short_price) / 2.0 or 1.0
    price_diff_pct = price_diff / avg
    return price_diff, price_diff_pct


_GMT_PLUS_3 = timezone(timedelta(hours=3))


def _format_next_funding(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    return dt.astimezone(_GMT_PLUS_3).strftime("%Y-%m-%d %H:%M:%S GMT+3")


def _liquidity_in_usd(size: float | None, price: float | None) -> float | None:
    if size is None or price is None:
        return None
    if size <= 0 or price <= 0:
        return None
    return size * price


def _fmt_numeric(value: float | None, *, decimals: int, width: int) -> str:
    if value is None:
        return f"{'-':>{width}}"
    return f"{value:>{width}.{decimals}f}"
