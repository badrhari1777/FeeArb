from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter
from utils.cache_db import SymbolMeta, get_or_fetch_symbol_meta, get_or_fetch_funding_history
from utils.funding import enrich_history_intervals, normalize_interval_hours, parse_timestamp_ms

logger = logging.getLogger(__name__)


class GateAdapter(ExchangeAdapter):
    name = "gate"
    base_url = "https://fx-api.gateio.ws/api/v4"
    _META_TTL_SECONDS = 86_400  # 24h

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}_USDT"
        if symbol.endswith("USD"):
            base = symbol[:-3]
            return f"{base}_USD"
        return None

    def settle_for_symbol(self, symbol: str | None) -> str | None:
        if not symbol:
            return None
        normalized = str(symbol).upper().strip()
        if normalized.endswith("_USDT") or normalized.endswith("USDT"):
            return "usdt"
        if normalized.endswith("_USD") or normalized.endswith("USD"):
            return "btc"
        return None

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        snapshots: list[MarketSnapshot] = []
        targets = {
            sym.upper(): (self.map_symbol(sym), self.settle_for_symbol(sym))
            for sym in symbols
        }
        targets = {
            canon: (contract, settle)
            for canon, (contract, settle) in targets.items()
            if contract and settle
        }
        if not targets:
            return []

        ticker_maps: dict[str, dict[str, dict]] = {}
        contract_maps: dict[str, dict[str, dict]] = {}
        for settle in sorted({settle for _contract, settle in targets.values()}):
            try:
                ticker_payload = _get_json(f"{self.base_url}/futures/{settle}/tickers")
                tickers = ticker_payload if isinstance(ticker_payload, list) else []
                ticker_maps[settle] = {
                    item.get("contract"): item
                    for item in tickers
                    if isinstance(item, dict) and item.get("contract")
                }
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Gate bulk tickers fetch failed for settle=%s: %s", settle, exc)
                ticker_maps[settle] = {}
            try:
                contracts_payload = _get_json(f"{self.base_url}/futures/{settle}/contracts")
                contracts = contracts_payload if isinstance(contracts_payload, list) else []
                contract_maps[settle] = {
                    item.get("name"): item
                    for item in contracts
                    if isinstance(item, dict) and item.get("name")
                }
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Gate bulk contracts fetch failed for settle=%s: %s", settle, exc)
                contract_maps[settle] = {}

        for canonical, (contract, settle) in targets.items():
            ticker_item = ticker_maps.get(settle, {}).get(contract)
            if not ticker_item:
                ticker_url = (
                    f"{self.base_url}/futures/{settle}/tickers?"
                    + urlencode({"contract": contract})
                )
                fallback_ticker = _get_json(ticker_url)
                if not isinstance(fallback_ticker, list) or not fallback_ticker:
                    logger.info("Gate: empty ticker for %s", contract)
                    continue
                ticker_item = fallback_ticker[0]

            contract_payload = contract_maps.get(settle, {}).get(contract)
            if contract_payload is None:
                contract_url = f"{self.base_url}/futures/{settle}/contracts/{contract}"
                contract_payload = _get_json(contract_url)
            self._cache_symbol_meta(contract, contract_payload)

            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=contract,
                    funding_rate=_to_float(
                        ticker_item.get("funding_rate")
                        or ticker_item.get("funding_rate_indicative")
                    ),
                    next_funding_time=_to_datetime(contract_payload.get("funding_next_apply")),
                    funding_interval_hours=normalize_interval_hours(
                        contract_payload.get("funding_interval")
                        or contract_payload.get("funding_interval_hour")
                        or contract_payload.get("funding_interval_hours")
                    ),
                    mark_price=_to_float(ticker_item.get("mark_price")),
                    bid=_to_float(ticker_item.get("highest_bid")),
                    ask=_to_float(ticker_item.get("lowest_ask")),
                    raw={"ticker": ticker_item, "contract": contract_payload},
                )
            )

        return snapshots

    def _cache_symbol_meta(self, contract: str, payload: dict | None) -> None:
        def _fetch() -> SymbolMeta | None:
            if not isinstance(payload, dict):
                return None
            return SymbolMeta(
                exchange=self.name,
                symbol=contract,
                contract_size=_to_float(payload.get("quanto_multiplier")),
                price_step=_to_float(payload.get("order_price_round")),
                qty_step=_to_float(payload.get("order_size_min")),
                min_qty=_to_float(payload.get("order_size_min")),
                max_qty=_to_float(payload.get("order_size_max")),
                min_notional=_to_float(payload.get("order_price_deviate")),
                max_leverage=_to_float(payload.get("leverage_max")),
                tick_size=_to_float(payload.get("order_price_round")),
            )

        get_or_fetch_symbol_meta(
            self.name,
            contract,
            _fetch,
            max_age_seconds=self._META_TTL_SECONDS,
        )

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return cached funding history (expected 8h interval) with ~2m refresh."""
        contract = self.map_symbol(symbol) or symbol
        settle = self.settle_for_symbol(contract) or "usdt"

        def _fetch() -> list[dict]:
            url = (
                f"{self.base_url}/futures/{settle}/funding_rate?"
                + urlencode({"contract": contract, "limit": limit})
            )
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("Gate funding history fetch failed for %s: %s", contract, exc)
                return []
            if not isinstance(payload, list):
                return []
            out: list[dict] = []
            for item in payload:
                ts_ms = parse_timestamp_ms(item.get("t"))
                out.append(
                    {
                        "ts_ms": ts_ms or 0,
                        "rate": _to_float(item.get("r")),
                        "interval_hours": None,
                        "mark_price": _to_float(item.get("p")),
                    }
                )
            return enrich_history_intervals(out)

        return get_or_fetch_funding_history(
            self.name,
            contract,
            _fetch,
            max_age_seconds=120,
            limit=limit,
        )


def _get_json(url: str) -> dict | list:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:  # nosec
        import json

        return json.loads(resp.read().decode("utf-8"))


def _to_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc)
