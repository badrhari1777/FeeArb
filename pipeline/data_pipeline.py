from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Iterable, List, Mapping, Sequence

from config import PARSE_CACHE_TTL_SECONDS, SUPPORTED_EXCHANGES
from project_settings import DEFAULT_SOURCES
from exchanges import get_adapter, normalize_exchange_name
from orchestrator.models import FundingOpportunity
from orchestrator.opportunities import (
    compute_opportunities,
    format_opportunity_table,
)
from parsers import arbitragescanner, coinglass
from utils import load_cache, save_cache

logger = logging.getLogger(__name__)


@dataclass
class DataSnapshot:
    generated_at: datetime
    screener_rows: List[dict]
    coinglass_rows: List[coinglass.CoinglassRow]
    universe: List[dict[str, object]]
    opportunities: List[FundingOpportunity]
    raw_payloads: dict[str, list[dict]]
    screener_from_cache: bool = False
    coinglass_from_cache: bool = False
    exchange_status: List[dict[str, object]] = field(default_factory=list)
    messages: List[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "screener_rows": self.screener_rows,
            "coinglass_rows": [row.to_dict() for row in self.coinglass_rows],
            "universe": self.universe,
            "opportunities": [
                _opportunity_dict(item) for item in self.opportunities
            ],
            "raw_payloads": self.raw_payloads,
            "messages": self.messages,
            "screener_from_cache": self.screener_from_cache,
            "coinglass_from_cache": self.coinglass_from_cache,
            "exchange_status": self.exchange_status,
        }


ProgressCallback = Callable[[str, dict[str, Any]], None]


def collect_snapshot(
    progress_cb: ProgressCallback | None = None,
    *,
    source_settings: Mapping[str, bool] | None = None,
    exchange_settings: Mapping[str, bool] | None = None,
) -> DataSnapshot:
    timestamp = datetime.now(timezone.utc)

    def _emit(event: str, payload: dict[str, Any] | None = None) -> None:
        if progress_cb:
            progress_cb(event, payload or {})

    messages: list[str] = []
    sources = _effective_sources(source_settings)
    exchanges = _effective_exchanges(exchange_settings)

    if sources.get('arbitragescanner', True):
        _emit(
            'screener:start',
            {
                'message': 'Fetching ArbitrageScanner candidates...',
            },
        )
        screener_rows, screener_from_cache = _load_screener_snapshot()
        _emit(
            'screener:complete',
            {
                'message': f'ArbitrageScanner returned {len(screener_rows)} rows',
                'count': len(screener_rows),
                'from_cache': screener_from_cache,
            },
        )
        if not screener_rows:
            messages.append('ArbitrageScanner returned no candidates.')
    else:
        screener_rows = []
        screener_from_cache = True
        messages.append('ArbitrageScanner fetch disabled via settings.')
        _emit(
            'screener:skipped',
            {
                'message': 'ArbitrageScanner polling skipped (disabled in settings).',
            },
        )

    if sources.get('coinglass', True):
        _emit(
            'coinglass:start',
            {
                'message': 'Fetching Coinglass arbitrage table...',
            },
        )
        coinglass_rows, coinglass_from_cache = _load_coinglass_snapshot()
        _emit(
            'coinglass:complete',
            {
                'message': f'Coinglass returned {len(coinglass_rows)} rows',
                'count': len(coinglass_rows),
                'from_cache': coinglass_from_cache,
            },
        )
        if not coinglass_rows:
            messages.append('Coinglass returned no rows.')
    else:
        coinglass_rows = []
        coinglass_from_cache = True
        messages.append('Coinglass fetch disabled via settings.')
        _emit(
            'coinglass:skipped',
            {
                'message': 'Coinglass polling skipped (disabled in settings).',
            },
        )

    universe = _build_symbol_universe(screener_rows, coinglass_rows)
    _emit(
        'universe:ready',
        {
            'message': f'Symbol universe built with {len(universe)} items',
            'count': len(universe),
        },
    )
    if not universe:
        messages.append('Symbol universe is empty. Enable at least one data source.')

    symbols = [entry['symbol'] for entry in universe]

    adapters = _active_adapters(exchanges)
    if adapters and symbols:
        _emit(
            'exchanges:start',
            {
                'message': f'Fetching exchange snapshots for {len(symbols)} symbols...',
                'symbol_count': len(symbols),
                'exchange_count': len(adapters),
            },
        )
        opportunities, raw_payloads, exchange_status = compute_opportunities(
            symbols, adapters, progress_cb=progress_cb
        )
        _emit(
            'opportunities:complete',
            {
                'message': f'Computed {len(opportunities)} funding opportunities',
                'count': len(opportunities),
            },
        )
    else:
        opportunities = []
        raw_payloads = {}
        exchange_status = []
        if not adapters:
            messages.append('Exchange polling skipped - all exchanges disabled.')
            _emit(
                'exchanges:skipped',
                {
                    'message': 'Exchange polling skipped (no exchanges enabled).',
                },
            )
        if not symbols:
            _emit(
                'opportunities:skipped',
                {
                    'message': 'Opportunity scan skipped (no symbols available).',
                },
            )

    failed_exchanges = [
        entry['exchange']
        for entry in exchange_status
        if entry.get('status') != 'ok'
    ]
    if failed_exchanges:
        messages.append(
            'Missing data from exchanges: ' + ', '.join(sorted(failed_exchanges))
        )

    _emit(
        'snapshot:ready',
        {
            'message': 'Snapshot assembled successfully',
            'opportunity_count': len(opportunities),
            'failed_exchanges': failed_exchanges,
        },
    )

    return DataSnapshot(
        generated_at=timestamp,
        screener_rows=screener_rows,
        coinglass_rows=coinglass_rows,
        universe=universe,
        opportunities=opportunities,
        raw_payloads=raw_payloads,
        exchange_status=exchange_status,
        screener_from_cache=screener_from_cache,
        coinglass_from_cache=coinglass_from_cache,
        messages=messages,
    )


def format_screener_table(rows: Sequence[dict]) -> str:
    header = (
        f"{'Symbol':<10} {'Spread':>10} "
        f"{'Long':>18} {'LongFee':>10} {'Short':>18} {'ShortFee':>10}"
    )
    lines = [header, "-" * len(header)]
    for item in rows:
        lines.append(
            f"{item['symbol']:<10} {item['spread']:>10.6f} "
            f"{item['long_exchange']:>18} {item['long_fee']:>10.6f} "
            f"{item['short_exchange']:>18} {item['short_fee']:>10.6f}"
        )
    return "\n".join(lines)


def format_coinglass_table(rows: Sequence[coinglass.CoinglassRow]) -> str:
    header = (
        f"{'Rank':>4} {'Symbol':<8} {'Pair':<12} "
        f"{'Long':>14} {'Short':>14} {'Net%':>8} {'APR%':>9} {'Spread%':>9}"
    )
    lines = [header, "-" * len(header)]
    for row in rows:
        lines.append(
            f"{row.ranking:>4} {row.symbol:<8} {row.pair:<12} "
            f"{row.long_exchange:>14} {row.short_exchange:>14} "
            f"{row.net_funding_rate * 100:>7.2f}% {row.apr * 100:>8.2f}% "
            f"{row.spread_rate * 100:>8.2f}%"
        )
    return "\n".join(lines)


def format_universe_table(rows: Iterable[dict[str, object]]) -> str:
    header = f"{'Symbol':<10} {'Sources':<32}"
    lines = [header, "-" * len(header)]
    for row in rows:
        lines.append(f"{row['symbol']:<10} {row['sources']:<32}")
    return "\n".join(lines)


def format_opportunities(rows: Sequence[FundingOpportunity]) -> str:
    return format_opportunity_table(rows)


def _normalize_screener_rows(rows: Iterable[dict]) -> list[dict]:
    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = arbitragescanner.normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        normalized_row = dict(row)
        normalized_row["symbol"] = symbol
        normalized.append(normalized_row)
    return normalized


def _load_screener_snapshot() -> tuple[list[dict], bool]:
    cached = load_cache("screener_latest.json", PARSE_CACHE_TTL_SECONDS)
    if cached:
        logger.info("Using cached ArbitrageScanner data")
        rows = _normalize_screener_rows(cached.get("data", []))
        return rows, True

    logger.info("Fetching candidates from ArbitrageScanner...")
    data = arbitragescanner.fetch_json()
    rows = _normalize_screener_rows(
        arbitragescanner.build_top(data, exclude=("binance",), limit=20)
    )
    save_cache(
        "screener_latest.json",
        {"data": rows, "fetched_at": datetime.now(timezone.utc).isoformat()},
    )
    logger.info("Screener returned %s candidates", len(rows))
    return rows, False


def _load_coinglass_snapshot() -> tuple[list[coinglass.CoinglassRow], bool]:
    cached = load_cache("coinglass_latest.json", PARSE_CACHE_TTL_SECONDS)
    if cached:
        logger.info("Using cached Coinglass data")
        try:
            rows = [_coinglass_row_from_cache(item) for item in cached["data"]]
            return rows, True
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Cached Coinglass payload invalid, refetching: %s", exc)

    logger.info("Fetching candidates from Coinglass...")
    rows = coinglass.fetch_rows(limit=20)
    save_cache(
        "coinglass_latest.json",
        {
            "data": [row.to_dict() for row in rows],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info("Coinglass returned %s candidates", len(rows))
    return rows, False


def _build_symbol_universe(
    screener_rows: Sequence[dict], coinglass_rows: Sequence[coinglass.CoinglassRow]
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


def _effective_sources(settings: Mapping[str, bool] | None) -> dict[str, bool]:
    if not settings:
        return dict(DEFAULT_SOURCES)
    result = dict(DEFAULT_SOURCES)
    for key, value in settings.items():
        result[key] = bool(value)
    return result


def _effective_exchanges(settings: Mapping[str, bool] | None) -> list[str]:
    if not settings:
        return list(SUPPORTED_EXCHANGES)
    active: list[str] = []
    for name, enabled in settings.items():
        if not enabled:
            continue
        canonical = normalize_exchange_name(name)
        if canonical not in active:
            active.append(canonical)
    return active


def _active_adapters(enabled: Iterable[str] | None = None):
    adapters = []
    seen: set[str] = set()
    candidates = list(enabled) if enabled is not None else list(SUPPORTED_EXCHANGES)
    for name in candidates:
        canonical = normalize_exchange_name(name)
        if canonical in seen:
            continue
        seen.add(canonical)
        try:
            adapters.append(get_adapter(canonical))
        except KeyError:
            logger.warning('No adapter implemented for %s', canonical)
    return adapters


def _opportunity_dict(item: FundingOpportunity) -> dict[str, object]:
    def fmt(dt: datetime | None) -> str:
        return _format_time_gmt3(dt)

    return {
        "symbol": item.symbol,
        "long_exchange": item.long_exchange,
        "long_rate": item.long_rate,
        "long_mark": item.long_mark,
        "long_ask": item.long_ask,
        "long_liquidity": item.long_liquidity,
        "long_liquidity_usd": item.long_liquidity_usd,
        "short_exchange": item.short_exchange,
        "short_rate": item.short_rate,
        "short_mark": item.short_mark,
        "short_bid": item.short_bid,
        "short_liquidity": item.short_liquidity,
        "short_liquidity_usd": item.short_liquidity_usd,
        "spread": item.spread,
        "price_diff": item.price_diff,
        "price_diff_pct": item.price_diff_pct,
        "effective_spread": item.effective_spread,
        "long_next_funding": fmt(item.long_next_funding),
        "short_next_funding": fmt(item.short_next_funding),
        "long_funding_interval_hours": item.long_funding_interval_hours,
        "short_funding_interval_hours": item.short_funding_interval_hours,
        "participants": item.participants,
    }


def _coinglass_row_from_cache(item: dict) -> coinglass.CoinglassRow:
    return coinglass.CoinglassRow(
        ranking=int(item.get("ranking", 0)),
        symbol=str(item.get("symbol") or ""),
        pair=str(item.get("pair") or ""),
        long_exchange=str(item.get("long_exchange") or ""),
        short_exchange=str(item.get("short_exchange") or ""),
        apr=_percent_to_decimal(item, "apr"),
        net_funding_rate=_percent_to_decimal(item, "net_funding_rate"),
        spread_rate=_percent_to_decimal(item, "spread_rate"),
        open_interest=_open_interest_from_cache(item),
        settlement=str(item.get("settlement") or ""),
        trade_links=_trade_links_from_cache(item),
    )


def _percent_to_decimal(payload: dict, key: str) -> float:
    if f"{key}_percent" in payload:
        return _safe_float(payload.get(f"{key}_percent")) / 100.0
    return _safe_float(payload.get(key))


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _open_interest_from_cache(item: dict) -> list[str]:
    if "open_interest" in item and isinstance(item["open_interest"], list):
        return [str(entry) for entry in item["open_interest"] if entry]
    entries: list[str] = []
    if item.get("open_interest_long"):
        entries.append(str(item["open_interest_long"]))
    if item.get("open_interest_short"):
        entries.append(str(item["open_interest_short"]))
    return entries


def _trade_links_from_cache(item: dict) -> list[str]:
    links = item.get("trade_links")
    if isinstance(links, list):
        return [str(link) for link in links if link]
    if isinstance(links, str):
        return [part for part in links.split(";") if part]
    return []


_GMT_PLUS_3 = timezone(timedelta(hours=3))


def _format_time_gmt3(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(_GMT_PLUS_3).strftime("%Y-%m-%d %H:%M:%S GMT+3")
