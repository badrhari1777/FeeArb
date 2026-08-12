"""Five-venue perpetual Instrument Registry for Strategy Lab research."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
import json
from typing import Any, Awaitable, Callable, Iterable
from urllib.parse import quote
from urllib.request import Request, urlopen

from .external_contract import ExternalObservation, TARGET_EXCHANGES, utc_now_iso


REGISTRY_VERSION = "strategy_lab_instrument_registry_v1"
ASSET_ALIASES = {"XBT": "BTC"}

BINANCE_INSTRUMENTS_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BYBIT_INSTRUMENTS_URL = (
    "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
)
OKX_INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
KUCOIN_INSTRUMENTS_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
GATE_CONTRACT_URL = "https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/{symbol}"


def _float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _canonical_asset(value: object) -> str:
    asset = str(value or "").strip().upper()
    return ASSET_ALIASES.get(asset, asset)


def canonical_from_exchange_symbol(exchange: str, value: object) -> str:
    """Interpret a provider symbol without pretending it is venue-executable."""

    text = str(value or "").strip().upper()
    suffixes = {
        "okx": ("-USDT-SWAP", "USDT"),
        "kucoin": ("USDTM", "USDT"),
        "gate": ("_USDT", "USDT"),
        "binance": ("USDT",),
        "bybit": ("USDT",),
    }.get(str(exchange or "").lower(), ("USDT",))
    for suffix in suffixes:
        if text.endswith(suffix):
            text = text[: -len(suffix)]
            break
    return _canonical_asset(text)


@dataclass(slots=True)
class InstrumentContract:
    exchange: str
    canonical_symbol: str
    exchange_symbol: str
    base_asset: str
    quote_asset: str
    settle_asset: str
    status: str
    active: bool
    linear: bool = True
    perpetual: bool = True
    contract_size: float | None = None
    price_tick: float | None = None
    qty_step: float | None = None
    min_qty: float | None = None
    min_notional: float | None = None
    funding_interval_hours: float | None = None
    mapping_notes: list[str] = field(default_factory=list)
    raw_identity: dict[str, Any] = field(default_factory=dict)
    registry_version: str = REGISTRY_VERSION

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _binance_contract(row: dict[str, Any]) -> InstrumentContract | None:
    if str(row.get("contractType") or "").upper() != "PERPETUAL":
        return None
    base = _canonical_asset(row.get("baseAsset"))
    quote_asset = str(row.get("quoteAsset") or "").upper()
    settle_asset = str(row.get("marginAsset") or "").upper()
    if not base or quote_asset != "USDT" or settle_asset != "USDT":
        return None
    filters = {
        str(item.get("filterType") or ""): item
        for item in row.get("filters") or []
        if isinstance(item, dict)
    }
    price_filter = filters.get("PRICE_FILTER") or {}
    lot_filter = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE") or {}
    notional_filter = filters.get("MIN_NOTIONAL") or {}
    status = str(row.get("status") or "unknown")
    return InstrumentContract(
        exchange="binance",
        canonical_symbol=base,
        exchange_symbol=str(row.get("symbol") or "").upper(),
        base_asset=base,
        quote_asset=quote_asset,
        settle_asset=settle_asset,
        status=status,
        active=status.upper() == "TRADING",
        contract_size=_float(row.get("contractSize")) or 1.0,
        price_tick=_float(price_filter.get("tickSize")),
        qty_step=_float(lot_filter.get("stepSize")),
        min_qty=_float(lot_filter.get("minQty")),
        min_notional=_float(notional_filter.get("notional") or notional_filter.get("minNotional")),
        raw_identity={"pair": row.get("pair"), "underlying_type": row.get("underlyingType")},
    )


def parse_binance_instruments(payload: object) -> list[InstrumentContract]:
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise ValueError("Binance instruments response has invalid shape")
    return [item for row in payload["symbols"] if isinstance(row, dict) if (item := _binance_contract(row))]


def _bybit_contract(row: dict[str, Any]) -> InstrumentContract | None:
    if str(row.get("contractType") or "").lower() != "linearperpetual":
        return None
    base = _canonical_asset(row.get("baseCoin"))
    quote_asset = str(row.get("quoteCoin") or "").upper()
    settle_asset = str(row.get("settleCoin") or "").upper()
    if not base or quote_asset != "USDT" or settle_asset != "USDT":
        return None
    price_filter = row.get("priceFilter") or {}
    lot_filter = row.get("lotSizeFilter") or {}
    status = str(row.get("status") or "unknown")
    active = status.lower() == "trading" and not bool(row.get("isPreListing"))
    return InstrumentContract(
        exchange="bybit",
        canonical_symbol=base,
        exchange_symbol=str(row.get("symbol") or "").upper(),
        base_asset=base,
        quote_asset=quote_asset,
        settle_asset=settle_asset,
        status=status,
        active=active,
        contract_size=_float(row.get("contractSize")) or 1.0,
        price_tick=_float(price_filter.get("tickSize")),
        qty_step=_float(lot_filter.get("qtyStep")),
        min_qty=_float(lot_filter.get("minOrderQty")),
        min_notional=_float(lot_filter.get("minNotionalValue") or lot_filter.get("minOrderValue")),
        funding_interval_hours=(
            _float(row.get("fundingInterval")) / 60.0
            if _float(row.get("fundingInterval")) is not None
            else None
        ),
        raw_identity={"symbol_id": row.get("symbolId"), "pre_listing": row.get("isPreListing")},
    )


def parse_bybit_instruments(payload: object) -> list[InstrumentContract]:
    if not isinstance(payload, dict) or payload.get("retCode") != 0:
        raise ValueError("Bybit instruments response is not successful")
    rows = (payload.get("result") or {}).get("list")
    if not isinstance(rows, list):
        raise ValueError("Bybit instruments response has invalid shape")
    return [item for row in rows if isinstance(row, dict) if (item := _bybit_contract(row))]


def _okx_contract(row: dict[str, Any]) -> InstrumentContract | None:
    inst_id = str(row.get("instId") or "").upper()
    if (
        str(row.get("instType") or "").upper() != "SWAP"
        or not inst_id.endswith("-USDT-SWAP")
        or str(row.get("settleCcy") or "").upper() != "USDT"
    ):
        return None
    base = _canonical_asset(inst_id[: -len("-USDT-SWAP")])
    if not base:
        return None
    status = str(row.get("state") or "unknown")
    return InstrumentContract(
        exchange="okx",
        canonical_symbol=base,
        exchange_symbol=inst_id,
        base_asset=base,
        quote_asset="USDT",
        settle_asset="USDT",
        status=status,
        active=status.lower() == "live",
        linear=str(row.get("ctType") or "").lower() != "inverse",
        contract_size=_float(row.get("ctVal")),
        price_tick=_float(row.get("tickSz")),
        qty_step=_float(row.get("lotSz")),
        min_qty=_float(row.get("minSz")),
        raw_identity={"inst_family": row.get("instFamily"), "ct_val_ccy": row.get("ctValCcy")},
    )


def parse_okx_instruments(payload: object) -> list[InstrumentContract]:
    if not isinstance(payload, dict) or str(payload.get("code")) != "0":
        raise ValueError("OKX instruments response is not successful")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("OKX instruments response has invalid shape")
    return [item for row in rows if isinstance(row, dict) if (item := _okx_contract(row))]


def _kucoin_contract(row: dict[str, Any]) -> InstrumentContract | None:
    base = _canonical_asset(row.get("baseCurrency"))
    quote_asset = str(row.get("quoteCurrency") or "").upper()
    settle_asset = str(row.get("settleCurrency") or "").upper()
    if not base or quote_asset != "USDT" or settle_asset != "USDT" or row.get("expireDate"):
        return None
    status = str(row.get("status") or "unknown")
    granularity = _float(row.get("currentFundingRateGranularity") or row.get("fundingRateGranularity"))
    return InstrumentContract(
        exchange="kucoin",
        canonical_symbol=base,
        exchange_symbol=str(row.get("symbol") or "").upper(),
        base_asset=base,
        quote_asset=quote_asset,
        settle_asset=settle_asset,
        status=status,
        active=status.lower() == "open",
        linear=not bool(row.get("isInverse")),
        contract_size=_float(row.get("multiplier")),
        price_tick=_float(row.get("tickSize")),
        qty_step=_float(row.get("lotSize")),
        min_qty=_float(row.get("lotSize")),
        funding_interval_hours=granularity / 3_600_000.0 if granularity else None,
        raw_identity={"display_symbol": row.get("displaySymbol"), "market_type": row.get("marketType")},
    )


def parse_kucoin_instruments(payload: object) -> list[InstrumentContract]:
    if not isinstance(payload, dict) or str(payload.get("code")) != "200000":
        raise ValueError("KuCoin instruments response is not successful")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("KuCoin instruments response has invalid shape")
    return [item for row in rows if isinstance(row, dict) if (item := _kucoin_contract(row))]


def parse_gate_contract(payload: object) -> InstrumentContract:
    if not isinstance(payload, dict) or not payload.get("name"):
        raise ValueError("Gate contract response has invalid shape")
    exchange_symbol = str(payload["name"]).upper()
    if not exchange_symbol.endswith("_USDT"):
        raise ValueError("Gate contract is not USDT settled")
    base = _canonical_asset(exchange_symbol[: -len("_USDT")])
    status = str(payload.get("status") or "unknown")
    active = status.lower() == "trading" and not bool(payload.get("in_delisting"))
    interval = _float(payload.get("funding_interval"))
    return InstrumentContract(
        exchange="gate",
        canonical_symbol=base,
        exchange_symbol=exchange_symbol,
        base_asset=base,
        quote_asset="USDT",
        settle_asset="USDT",
        status=status,
        active=active,
        contract_size=_float(payload.get("quanto_multiplier")),
        price_tick=_float(payload.get("order_price_round")),
        qty_step=_float(payload.get("order_size_min")),
        min_qty=_float(payload.get("order_size_min")),
        funding_interval_hours=interval / 3600.0 if interval else None,
        raw_identity={"contract_type": payload.get("contract_type"), "type": payload.get("type")},
    )


def _get_json(url: str, timeout: int) -> object:
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "FeeArb-StrategyLab/1"})
    with urlopen(request, timeout=timeout) as response:  # nosec B310 - fixed public endpoints
        return json.loads(response.read().decode("utf-8"))


BulkFetcher = Callable[[], Awaitable[object]]
GateFetcher = Callable[[str], Awaitable[object]]


async def _url_fetch(url: str, timeout: int = 20) -> object:
    return await asyncio.to_thread(_get_json, url, timeout)


def gate_symbols_from_observations(observations: Iterable[ExternalObservation], limit: int = 30) -> list[str]:
    symbols: list[str] = []
    for observation in observations:
        reported = next((leg.exchange_symbol for leg in observation.legs if leg.exchange == "gate"), None)
        reported_text = str(reported or "").upper()
        # ArbitrageScanner often returns a generic SYMBOLUSDT value in every
        # venue leg. Only an already Gate-shaped value is executable identity.
        value = (
            reported_text
            if reported_text.endswith("_USDT")
            else f"{observation.canonical_symbol}_USDT".upper()
        )
        if value and value not in symbols:
            symbols.append(value)
        if len(symbols) >= max(0, int(limit)):
            break
    return symbols


def build_registry_payload(
    by_exchange: dict[str, list[InstrumentContract]],
    *,
    source_status: dict[str, dict[str, Any]] | None = None,
    observed_at: str | None = None,
) -> dict[str, Any]:
    contracts: list[InstrumentContract] = []
    ambiguous: list[dict[str, Any]] = []
    for exchange in TARGET_EXCHANGES:
        rows = list(by_exchange.get(exchange) or [])
        grouped: dict[str, list[InstrumentContract]] = {}
        for row in rows:
            if row.active and row.linear and row.perpetual:
                grouped.setdefault(row.canonical_symbol, []).append(row)
        for canonical, variants in grouped.items():
            if len(variants) > 1:
                ambiguous.append(
                    {
                        "exchange": exchange,
                        "canonical_symbol": canonical,
                        "exchange_symbols": sorted(item.exchange_symbol for item in variants),
                        "reason": "multiple_active_contracts",
                    }
                )
                continue
            contracts.append(variants[0])
    contracts.sort(key=lambda item: (item.canonical_symbol, TARGET_EXCHANGES.index(item.exchange)))
    vectors: dict[str, dict[str, dict[str, Any]]] = {}
    for item in contracts:
        vectors.setdefault(item.canonical_symbol, {})[item.exchange] = item.as_dict()
    common_counts = {
        str(minimum): sum(1 for venues in vectors.values() if len(venues) >= minimum)
        for minimum in range(2, len(TARGET_EXCHANGES) + 1)
    }
    return {
        "registry_version": REGISTRY_VERSION,
        "observed_at": observed_at or utc_now_iso(),
        "selected_exchanges": list(TARGET_EXCHANGES),
        "source_status": source_status or {},
        "contracts": [item.as_dict() for item in contracts],
        "contract_count": len(contracts),
        "symbol_count": len(vectors),
        "venue_counts": {exchange: sum(1 for item in contracts if item.exchange == exchange) for exchange in TARGET_EXCHANGES},
        "common_symbol_counts": common_counts,
        "vectors": vectors,
        "ambiguous": ambiguous,
        "trade_signal": False,
        "research_only": True,
    }


async def fetch_instrument_registry(
    observations: Iterable[ExternalObservation] = (),
    *,
    bulk_fetchers: dict[str, BulkFetcher] | None = None,
    gate_fetcher: GateFetcher | None = None,
    gate_limit: int = 30,
    gate_concurrency: int = 5,
) -> dict[str, Any]:
    fetchers: dict[str, BulkFetcher] = {
        "binance": lambda: _url_fetch(BINANCE_INSTRUMENTS_URL),
        "bybit": lambda: _url_fetch(BYBIT_INSTRUMENTS_URL),
        "okx": lambda: _url_fetch(OKX_INSTRUMENTS_URL),
        "kucoin": lambda: _url_fetch(KUCOIN_INSTRUMENTS_URL),
    }
    fetchers.update(bulk_fetchers or {})
    parsers = {
        "binance": parse_binance_instruments,
        "bybit": parse_bybit_instruments,
        "okx": parse_okx_instruments,
        "kucoin": parse_kucoin_instruments,
    }
    by_exchange: dict[str, list[InstrumentContract]] = {}
    status: dict[str, dict[str, Any]] = {}

    async def collect_bulk(exchange: str) -> None:
        try:
            payload = await fetchers[exchange]()
            rows = parsers[exchange](payload)
        except Exception as exc:  # pylint: disable=broad-except
            status[exchange] = {"status": "error", "count": 0, "error": f"{type(exc).__name__}: {exc}"}
            by_exchange[exchange] = []
        else:
            by_exchange[exchange] = rows
            status[exchange] = {"status": "fresh", "count": len(rows), "error": None, "mode": "bulk"}

    await asyncio.gather(*(collect_bulk(exchange) for exchange in ("binance", "bybit", "okx", "kucoin")))

    gate_symbols = gate_symbols_from_observations(observations, gate_limit)
    semaphore = asyncio.Semaphore(max(1, int(gate_concurrency)))
    gate_rows: list[InstrumentContract] = []
    gate_errors: list[str] = []

    async def default_gate_fetch(symbol: str) -> object:
        return await _url_fetch(GATE_CONTRACT_URL.format(symbol=quote(symbol, safe="_")), timeout=12)

    selected_gate_fetcher = gate_fetcher or default_gate_fetch

    async def collect_gate(symbol: str) -> None:
        async with semaphore:
            try:
                row = parse_gate_contract(await selected_gate_fetcher(symbol))
            except Exception as exc:  # pylint: disable=broad-except
                gate_errors.append(f"{symbol}:{type(exc).__name__}")
            else:
                gate_rows.append(row)

    if gate_symbols:
        await asyncio.gather(*(collect_gate(symbol) for symbol in gate_symbols))
        gate_state = "fresh" if gate_rows else "error"
    else:
        gate_state = "not_requested"
    by_exchange["gate"] = gate_rows
    status["gate"] = {
        "status": gate_state,
        "count": len(gate_rows),
        "error": ",".join(gate_errors[:5]) or None,
        "error_count": len(gate_errors),
        "requested_count": len(gate_symbols),
        "mode": "candidate_scoped_exact_contract",
    }
    return build_registry_payload(by_exchange, source_status=status)


def verify_external_candidates(
    registry: dict[str, Any],
    observations: Iterable[ExternalObservation],
) -> list[dict[str, Any]]:
    vectors = registry.get("vectors") or {}
    results: list[dict[str, Any]] = []
    for observation in observations:
        venues = dict(vectors.get(observation.canonical_symbol) or {})
        asset_mismatches: list[str] = []
        format_differences: list[str] = []
        for leg in observation.legs:
            resolved = venues.get(leg.exchange)
            if not resolved:
                continue
            reported_asset = canonical_from_exchange_symbol(leg.exchange, leg.exchange_symbol)
            if reported_asset != observation.canonical_symbol:
                asset_mismatches.append(leg.exchange)
            elif str(resolved.get("exchange_symbol") or "").upper() != str(leg.exchange_symbol or "").upper():
                format_differences.append(leg.exchange)
        available = sorted(venues, key=lambda item: TARGET_EXCHANGES.index(item))
        vetoes: list[str] = []
        if asset_mismatches:
            vetoes.append("external_symbol_asset_mismatch")
        if len(available) < 2:
            vetoes.append("fewer_than_two_verified_venues")
        results.append(
            {
                "source": observation.source,
                "source_asset_id": observation.source_asset_id,
                "canonical_symbol": observation.canonical_symbol,
                "verified_venues": available,
                "verified_venue_count": len(available),
                "external_symbol_asset_mismatch_exchanges": asset_mismatches,
                "provider_symbol_format_difference_exchanges": format_differences,
                "vetoes": vetoes,
                "eligible_for_observation": not vetoes,
                "trade_signal": False,
                "research_only": True,
            }
        )
    return results
