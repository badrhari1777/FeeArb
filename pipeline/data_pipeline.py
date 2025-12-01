from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Iterable, List, Mapping, Sequence

from typing import TYPE_CHECKING

try:
    import aiohttp
except ImportError as exc:  # pragma: no cover - optional dependency
    aiohttp = None  # type: ignore[assignment]
    _aiohttp_import_error = exc
else:
    _aiohttp_import_error = None

if TYPE_CHECKING:  # pragma: no cover
    from aiohttp import ClientSession

from config import PARSE_CACHE_TTL_SECONDS, SUPPORTED_EXCHANGES
from project_settings import DEFAULT_SOURCES
from exchanges import get_adapter, normalize_exchange_name
from orchestrator.models import FundingOpportunity, MarketSnapshot
from orchestrator.opportunities import (
    compute_opportunities,
    format_opportunity_table,
)
from parsers import arbitragescanner, coinglass
from pipeline.source_cache import fetch_with_cache_async
from utils.sources import get_cached_source, upsert_cached_source
from utils import load_cache, save_cache

logger = logging.getLogger(__name__)
FRESH_CACHE_SECONDS = 600  # 10 minutes


@dataclass
class SourceSnapshot:
    generated_at: datetime
    screener_rows: List[dict]
    coinglass_rows: List[coinglass.CoinglassRow]
    universe: List[dict[str, object]]
    screener_from_cache: bool = False
    coinglass_from_cache: bool = False
    messages: List[str] = field(default_factory=list)


@dataclass
class DataSnapshot(SourceSnapshot):
    opportunities: List[FundingOpportunity] = field(default_factory=list)
    raw_payloads: dict[str, list[dict]] = field(default_factory=dict)
    exchange_status: List[dict[str, object]] = field(default_factory=list)
    market_snapshots: dict[str, dict[str, MarketSnapshot]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        base = {
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
            "market_snapshots": {
                exchange: [snapshot.to_dict() for snapshot in snapshots.values()]
                for exchange, snapshots in self.market_snapshots.items()
            },
        }
        return base


ProgressCallback = Callable[[str, dict[str, Any]], None]


async def collect_sources_async(
    progress_cb: ProgressCallback | None = None,
    *,
    source_settings: Mapping[str, bool] | None = None,
) -> SourceSnapshot:
    if aiohttp is None:
        raise RuntimeError(
            'aiohttp is required for asynchronous snapshot collection. Install it via "pip install aiohttp".'
        ) from _aiohttp_import_error
    timestamp = datetime.now(timezone.utc)

    def _emit(event: str, payload: dict[str, Any] | None = None) -> None:
        if progress_cb:
            progress_cb(event, payload or {})

    messages: list[str] = []
    sources = _effective_sources(source_settings)

    timeout = aiohttp.ClientTimeout(total=30)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        if sources.get("arbitragescanner", True):
            # If cache is fresh (<10m), skip live fetch.
            cached = load_cache("screener_latest.json", FRESH_CACHE_SECONDS)
            screener_rows = []
            screener_from_cache = False
            screener_warning = None
            if cached:
                screener_rows = _normalize_screener_rows(cached.get("data", []))
                screener_from_cache = True
                screener_warning = "ArbitrageScanner cache <10m; live fetch skipped."
            else:
                _emit(
                    "screener:start",
                    {
                        "message": "Fetching ArbitrageScanner candidates...",
                    },
                )
                try:
                    (
                        screener_rows,
                        screener_from_cache,
                        screener_warning,
                    ) = await _load_screener_snapshot_async(session)
                    upsert_cached_source("arbitragescanner", {"rows": screener_rows})
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("ArbitrageScanner fetch failed: %s", exc)
                    cached = get_cached_source("arbitragescanner")
                    if cached and isinstance(cached, dict):
                        screener_rows = cached.get("rows", []) or []
                        screener_from_cache = True
                        screener_warning = "ArbitrageScanner offline; using cached candidates."
            _emit(
                "screener:complete",
                {
                    "message": f"ArbitrageScanner returned {len(screener_rows)} rows",
                    "count": len(screener_rows),
                    "from_cache": screener_from_cache,
                },
            )
            if screener_warning:
                messages.append(screener_warning)
        else:
            screener_rows = []
            screener_from_cache = False
            messages.append("ArbitrageScanner disabled via settings; cache not used.")
            _emit(
                "screener:skipped",
                {
                    "message": "ArbitrageScanner polling skipped (disabled in settings).",
                },
            )

        if sources.get("coinglass", True):
            cached = load_cache("coinglass_latest.json", FRESH_CACHE_SECONDS)
            coinglass_rows = []
            coinglass_from_cache = False
            coinglass_warning = None
            if cached:
                try:
                    coinglass_rows = [_coinglass_row_from_cache(item) for item in cached["data"]]
                    coinglass_from_cache = True
                    coinglass_warning = "Coinglass cache <10m; live fetch skipped."
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Cached Coinglass payload invalid: %s", exc)
            if not coinglass_rows:
                _emit(
                    "coinglass:start",
                    {
                        "message": "Fetching Coinglass arbitrage table...",
                    },
                )
                try:
                    (
                        coinglass_rows,
                        coinglass_from_cache,
                        coinglass_warning,
                    ) = await _load_coinglass_snapshot_async(session)
                    upsert_cached_source("coinglass", {"rows": [row.to_dict() for row in coinglass_rows]})
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Coinglass fetch failed: %s", exc)
                    cached = get_cached_source("coinglass")
                    if cached and isinstance(cached, dict):
                        raw_rows = cached.get("rows", []) or []
                        coinglass_rows = []
                        for item in raw_rows:
                            try:
                                coinglass_rows.append(coinglass.CoinglassRow.from_dict(item))
                            except Exception:  # pylint: disable=broad-except
                                continue
                        coinglass_from_cache = True
                        coinglass_warning = "Coinglass offline; using cached table."
            _emit(
                "coinglass:complete",
                {
                    "message": f"Coinglass returned {len(coinglass_rows)} rows",
                    "count": len(coinglass_rows),
                    "from_cache": coinglass_from_cache,
                },
            )
            if coinglass_warning:
                messages.append(coinglass_warning)
        else:
            coinglass_rows = []
            coinglass_from_cache = False
            messages.append("Coinglass disabled via settings; cache not used.")
            _emit(
                "coinglass:skipped",
                {
                    "message": "Coinglass polling skipped (disabled in settings).",
                },
            )

    universe = _build_symbol_universe(screener_rows, coinglass_rows)
    _emit(
        "universe:ready",
        {
            "message": f"Symbol universe built with {len(universe)} items",
            "count": len(universe),
        },
    )
    if not universe:
        messages.append("Symbol universe is empty. Enable at least one data source.")

    return SourceSnapshot(
        generated_at=timestamp,
        screener_rows=screener_rows,
        coinglass_rows=coinglass_rows,
        universe=universe,
        screener_from_cache=screener_from_cache,
        coinglass_from_cache=coinglass_from_cache,
        messages=messages,
    )


async def build_snapshot_from_sources(
    sources: SourceSnapshot,
    progress_cb: ProgressCallback | None = None,
    *,
    exchange_settings: Mapping[str, bool] | None = None,
) -> DataSnapshot:
    def _emit(event: str, payload: dict[str, Any] | None = None) -> None:
        if progress_cb:
            progress_cb(event, payload or {})

    messages = list(sources.messages)
    exchanges = _effective_exchanges(exchange_settings)
    adapters = _active_adapters(exchanges)
    symbols = [entry["symbol"] for entry in sources.universe]

    if adapters and symbols:
        _emit(
            "exchanges:start",
            {
                "message": f"Fetching exchange snapshots for {len(symbols)} symbols...",
                "symbol_count": len(symbols),
                "exchange_count": len(adapters),
            },
        )
        (
            opportunities,
            raw_payloads,
            exchange_status,
            snapshot_map,
        ) = await compute_opportunities(
            symbols,
            adapters,
            0.0,
            progress_cb,
        )
        _emit(
            "opportunities:complete",
            {
                "message": f"Computed {len(opportunities)} funding opportunities",
                "count": len(opportunities),
            },
        )
    else:
        opportunities = []
        raw_payloads = {}
        exchange_status = []
        snapshot_map = {}
        if not adapters:
            messages.append("Exchange polling skipped - all exchanges disabled.")
            _emit(
                "exchanges:skipped",
                {
                    "message": "Exchange polling skipped (no exchanges enabled).",
                },
            )
        if not symbols:
            _emit(
                "opportunities:skipped",
                {
                    "message": "Opportunity scan skipped (no symbols available).",
                },
            )

    failed_exchanges = [
        entry["exchange"]
        for entry in exchange_status
        if entry.get("status") != "ok"
    ]
    if failed_exchanges:
        messages.append(
            "Missing data from exchanges: " + ", ".join(sorted(failed_exchanges))
        )

    _emit(
        "snapshot:ready",
        {
            "message": "Snapshot assembled successfully",
            "opportunity_count": len(opportunities),
            "failed_exchanges": failed_exchanges,
        },
    )

    return DataSnapshot(
        generated_at=sources.generated_at,
        screener_rows=sources.screener_rows,
        coinglass_rows=sources.coinglass_rows,
        universe=sources.universe,
        opportunities=opportunities,
        raw_payloads=raw_payloads,
        exchange_status=exchange_status,
        market_snapshots=snapshot_map,
        screener_from_cache=sources.screener_from_cache,
        coinglass_from_cache=sources.coinglass_from_cache,
        messages=messages,
    )


async def collect_snapshot_async(
    progress_cb: ProgressCallback | None = None,
    *,
    source_settings: Mapping[str, bool] | None = None,
    exchange_settings: Mapping[str, bool] | None = None,
) -> DataSnapshot:
    sources = await collect_sources_async(
        progress_cb,
        source_settings=source_settings,
    )
    return await build_snapshot_from_sources(
        sources,
        progress_cb,
        exchange_settings=exchange_settings,
    )


def collect_snapshot(
    progress_cb: ProgressCallback | None = None,
    *,
    source_settings: Mapping[str, bool] | None = None,
    exchange_settings: Mapping[str, bool] | None = None,
) -> DataSnapshot:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            collect_snapshot_async(
                progress_cb,
                source_settings=source_settings,
                exchange_settings=exchange_settings,
            )
        )
    else:
        if loop.is_running():
            raise RuntimeError(
                "collect_snapshot cannot be called from a running event loop; use collect_snapshot_async instead."
            )
        return loop.run_until_complete(
            collect_snapshot_async(
                progress_cb,
                source_settings=source_settings,
                exchange_settings=exchange_settings,
            )
        )


async def _load_screener_snapshot_async(
    session: "ClientSession",
) -> tuple[list[dict], bool, str | None]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://screener.arbitragescanner.io/",
    }

    try:
        async with session.get(arbitragescanner.URL, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("ArbitrageScanner fetch failed: %s", exc)
        # Fallback to cached snapshot if it is reasonably fresh.
        cached = load_cache("screener_latest.json", PARSE_CACHE_TTL_SECONDS)
        if cached:
            rows = _normalize_screener_rows(cached.get("data", []))
            return rows, True, "ArbitrageScanner offline; using cached candidates."
        return [], False, "ArbitrageScanner returned no candidates."

    rows = _normalize_screener_rows(
        arbitragescanner.build_top(data, exclude=("binance",), limit=20)
    )
    save_cache(
        "screener_latest.json",
        {"data": rows, "fetched_at": datetime.now(timezone.utc).isoformat()},
    )
    logger.info("ArbitrageScanner returned %s candidates", len(rows))
    return rows, False, None


async def _load_coinglass_snapshot_async(
    session: "ClientSession",
) -> tuple[list[coinglass.CoinglassRow], bool, str | None]:
    def _fetch_rows_wrapper() -> list[coinglass.CoinglassRow]:
        try:
            return coinglass.fetch_rows(limit=20)
        except AttributeError:
            # Older parser versions exposed `fetch` instead of `fetch_rows`
            fetch = getattr(coinglass, "fetch", None)
            if callable(fetch):
                return fetch(limit=20)  # type: ignore[call-arg]
            raise

    try:
        rows = await asyncio.to_thread(_fetch_rows_wrapper)
    except RuntimeError as exc:
        logger.warning("Coinglass parsing skipped: %s", exc)
        return [], False, str(exc)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Coinglass fetch failed: %s", exc)
        cached = load_cache("coinglass_latest.json", PARSE_CACHE_TTL_SECONDS)
        if cached:
            try:
                rows = [_coinglass_row_from_cache(item) for item in cached["data"]]
                return rows, True, "Coinglass request failed; using cached table."
            except Exception as err:  # pylint: disable=broad-except
                logger.warning("Cached Coinglass payload invalid: %s", err)
        return [], False, "Coinglass request failed."

    if not rows:
        return [], False, "Coinglass returned no rows."

    save_cache(
        "coinglass_latest.json",
        {
            "data": [row.to_dict() for row in rows],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info("Coinglass returned %s candidates", len(rows))
    return rows, False, None


def _parse_coinglass_html(html: str, *, limit: int) -> list[coinglass.CoinglassRow]:
    if getattr(coinglass, "BeautifulSoup", None) is None:
        raise RuntimeError(
            'BeautifulSoup (beautifulsoup4) is required for Coinglass parsing. Install it via "pip install beautifulsoup4".'
        )

    soup = coinglass.BeautifulSoup(html, "html.parser")  # type: ignore[attr-defined]
    rows: list[coinglass.CoinglassRow] = []
    for tr in soup.select("table tbody tr"):
        entry = coinglass._parse_row(tr)  # type: ignore[attr-defined]
        if entry:
            rows.append(entry)
            if len(rows) >= limit:
                break
    return rows


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
            logger.warning("No adapter implemented for %s", canonical)
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
    entries = []
    if "open_interest_long" in item:
        entries.append(str(item.get("open_interest_long") or ""))
    if "open_interest_short" in item:
        entries.append(str(item.get("open_interest_short") or ""))
    return entries


def _trade_links_from_cache(item: dict) -> list[str]:
    raw = item.get("trade_links")
    if isinstance(raw, str):
        return [segment for segment in raw.split(";") if segment]
    if isinstance(raw, list):
        return [str(entry) for entry in raw]
    return []


def _format_time_gmt3(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    gmt3 = timezone(timedelta(hours=3))
    return dt.astimezone(gmt3).strftime("%Y-%m-%d %H:%M:%S GMT+3")
