from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from analysis_storage.coin_db import (
    CoinInstrumentRow,
    CoinPairRow,
    upsert_instrument,
    upsert_pair,
)

BINANCE_FAPI_BASE = "https://fapi.binance.com"
KUCOIN_FAPI_BASE = "https://api-futures.kucoin.com"


@dataclass
class RegistryRefreshStats:
    binance_instruments: int = 0
    kucoin_instruments: int = 0
    shared_pairs: int = 0
    skipped_binance_symbols: int = 0
    skipped_kucoin_symbols: int = 0


def refresh_binance_kucoin_registry() -> RegistryRefreshStats:
    """Refresh and persist active Binance/KuCoin USDT perpetual registry."""
    binance_rows, skipped_binance = load_binance_instruments()
    kucoin_rows, skipped_kucoin = load_kucoin_instruments()

    for row in binance_rows:
        upsert_instrument(row)
    for row in kucoin_rows:
        upsert_instrument(row)

    by_symbol_binance = {row.canonical_symbol: row for row in binance_rows}
    by_symbol_kucoin = {row.canonical_symbol: row for row in kucoin_rows}
    shared_symbols = sorted(set(by_symbol_binance).intersection(by_symbol_kucoin))

    for symbol in shared_symbols:
        b_row = by_symbol_binance[symbol]
        k_row = by_symbol_kucoin[symbol]
        upsert_pair(
            CoinPairRow(
                pair_key=build_pair_key(symbol, "binance", "kucoin"),
                canonical_symbol=symbol,
                exchange_a="binance",
                exchange_b="kucoin",
                exchange_a_symbol=b_row.exchange_symbol,
                exchange_b_symbol=k_row.exchange_symbol,
                is_active=True,
            )
        )

    return RegistryRefreshStats(
        binance_instruments=len(binance_rows),
        kucoin_instruments=len(kucoin_rows),
        shared_pairs=len(shared_symbols),
        skipped_binance_symbols=skipped_binance,
        skipped_kucoin_symbols=skipped_kucoin,
    )


def load_binance_instruments() -> tuple[list[CoinInstrumentRow], int]:
    payload = _get_json(f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo")
    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(symbols, list):
        return [], 0

    out: list[CoinInstrumentRow] = []
    skipped = 0
    for item in symbols:
        if not isinstance(item, dict):
            skipped += 1
            continue
        if str(item.get("contractType") or "").upper() != "PERPETUAL":
            continue
        if str(item.get("status") or "").upper() != "TRADING":
            continue
        quote_asset = str(item.get("quoteAsset") or "").upper()
        if quote_asset != "USDT":
            continue
        exchange_symbol = str(item.get("symbol") or "").upper().strip()
        canonical = binance_exchange_symbol_to_canonical(exchange_symbol)
        if not canonical:
            skipped += 1
            continue
        filters = {
            str(f.get("filterType") or ""): f
            for f in (item.get("filters") or [])
            if isinstance(f, dict)
        }
        price_filter = filters.get("PRICE_FILTER") or {}
        lot_filter = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE") or {}
        notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
        out.append(
            CoinInstrumentRow(
                canonical_symbol=canonical,
                exchange="binance",
                exchange_symbol=exchange_symbol,
                base_asset=str(item.get("baseAsset") or "").upper() or None,
                quote_asset=quote_asset,
                contract_type="perpetual",
                contract_multiplier=_to_float(item.get("contractSize")) or 1.0,
                tick_size=_to_float(price_filter.get("tickSize")),
                qty_step=_to_float(lot_filter.get("stepSize")),
                min_qty=_to_float(lot_filter.get("minQty")),
                min_notional=_to_float(
                    notional_filter.get("notional") or notional_filter.get("minNotional")
                ),
                funding_interval_hours=8.0,
                is_active=True,
            )
        )
    return out, skipped


def load_kucoin_instruments() -> tuple[list[CoinInstrumentRow], int]:
    payload = _get_json(f"{KUCOIN_FAPI_BASE}/api/v1/contracts/active")
    contracts = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(contracts, list):
        return [], 0

    out: list[CoinInstrumentRow] = []
    skipped = 0
    for item in contracts:
        if not isinstance(item, dict):
            skipped += 1
            continue
        exchange_symbol = str(item.get("symbol") or "").upper().strip()
        canonical = kucoin_exchange_symbol_to_canonical(exchange_symbol)
        if not canonical:
            skipped += 1
            continue
        quote_currency = str(item.get("quoteCurrency") or "").upper()
        if quote_currency and quote_currency != "USDT":
            continue
        status = str(item.get("status") or "")
        if status and status.lower() not in {"open", "active"}:
            continue
        out.append(
            CoinInstrumentRow(
                canonical_symbol=canonical,
                exchange="kucoin",
                exchange_symbol=exchange_symbol,
                base_asset=str(item.get("baseCurrency") or "").upper() or None,
                quote_asset=quote_currency or "USDT",
                contract_type="perpetual",
                contract_multiplier=_to_float(
                    item.get("multiplier") or item.get("lotSize") or item.get("contractSize")
                ),
                tick_size=_to_float(item.get("tickSize")),
                qty_step=_to_float(item.get("lotSize")),
                min_qty=_to_float(item.get("lotSize")),
                min_notional=None,
                funding_interval_hours=_kucoin_funding_interval_hours(item),
                is_active=True,
            )
        )
    return out, skipped


def build_pair_key(canonical_symbol: str, exchange_a: str, exchange_b: str) -> str:
    return f"{canonical_symbol.upper()}|{exchange_a.lower()}|{exchange_b.lower()}"


def binance_exchange_symbol_to_canonical(exchange_symbol: str) -> str | None:
    symbol = str(exchange_symbol or "").upper().strip()
    if not symbol:
        return None
    if ":" in symbol:
        symbol = symbol.split(":", 1)[0]
    symbol = symbol.replace("/", "").replace("-", "").replace("_", "")
    while symbol.endswith("USDTUSDT"):
        symbol = symbol[:-4]
    if symbol.endswith("USDUSDT"):
        symbol = symbol[:-4]
    if symbol.endswith("USDT"):
        return symbol
    return None


def kucoin_exchange_symbol_to_canonical(exchange_symbol: str) -> str | None:
    symbol = str(exchange_symbol or "").upper().strip()
    if not symbol.endswith("USDTM"):
        return None
    base = symbol[:-5]
    if not base:
        return None
    if base == "XBT":
        base = "BTC"
    return f"{base}USDT"


def _kucoin_funding_interval_hours(contract_info: dict[str, Any]) -> float | None:
    values: list[float] = []
    for key in ("currentFundingRateGranularity", "fundingRateGranularity"):
        raw = _to_float(contract_info.get(key))
        if not raw or raw <= 0:
            continue
        seconds = raw / 1000.0 if raw > 100000 else raw
        hours = seconds / 3600.0
        if hours > 0:
            values.append(hours)
    if not values:
        return None
    return min(values)


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_json(url: str) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    try:
        with urlopen(req, timeout=20) as resp:  # nosec
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        payload = exc.read()
        if payload:
            try:
                return json.loads(payload.decode("utf-8"))
            except Exception:  # pylint: disable=broad-except
                pass
        return {"code": exc.code, "msg": str(exc)}


__all__ = [
    "RegistryRefreshStats",
    "BINANCE_FAPI_BASE",
    "KUCOIN_FAPI_BASE",
    "binance_exchange_symbol_to_canonical",
    "build_pair_key",
    "kucoin_exchange_symbol_to_canonical",
    "load_binance_instruments",
    "load_kucoin_instruments",
    "refresh_binance_kucoin_registry",
]

