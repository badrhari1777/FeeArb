from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from orchestrator.models import MarketSnapshot

from .base import ExchangeAdapter

logger = logging.getLogger(__name__)


class KucoinAdapter(ExchangeAdapter):
    name = "kucoin"
    base_url = "https://api-futures.kucoin.com"

    def __init__(self) -> None:
        self._contracts: Dict[str, dict] | None = None

    def map_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            if base == "BTC":
                base = "XBT"
            return f"{base}USDTM"
        return None

    def fetch_market_snapshots(self, symbols: Iterable[str]) -> List[MarketSnapshot]:
        contracts = self._load_contracts()
        snapshots: list[MarketSnapshot] = []

        for canonical in {sym.upper() for sym in symbols}:
            contract_symbol = self.map_symbol(canonical)
            if not contract_symbol:
                logger.debug("KuCoin: unsupported symbol %s", canonical)
                continue

            contract_info = contracts.get(contract_symbol)
            if not contract_info:
                logger.info("KuCoin: contract %s not found", contract_symbol)
                continue

            ticker_url = (
                f"{self.base_url}/api/v1/ticker?" + urlencode({"symbol": contract_symbol})
            )
            ticker_payload = _get_json(ticker_url)
            if ticker_payload.get("code") != "200000":
                logger.warning(
                    "KuCoin: ticker error for %s: %s",
                    contract_symbol,
                    ticker_payload.get("msg"),
                )
                continue
            ticker_item = ticker_payload.get("data") or {}

            snapshots.append(
                MarketSnapshot(
                    exchange=self.name,
                    symbol=canonical,
                    exchange_symbol=contract_symbol,
                    funding_rate=_to_float(contract_info.get("fundingFeeRate")),
                    next_funding_time=_to_datetime(contract_info.get("nextFundingRateDateTime")),
                    mark_price=_to_float(contract_info.get("markPrice")),
                    bid=_to_float(ticker_item.get("bestBidPrice")),
                    ask=_to_float(ticker_item.get("bestAskPrice")),
                    raw={"contract": contract_info, "ticker": ticker_item},
                )
            )

        return snapshots

    def _load_contracts(self) -> Dict[str, dict]:
        if self._contracts is not None:
            return self._contracts

        url = f"{self.base_url}/api/v1/contracts/active"
        payload = _get_json(url)
        if payload.get("code") != "200000":
            logger.warning("KuCoin: contracts error: %s", payload.get("msg"))
            self._contracts = {}
            return self._contracts

        data = payload.get("data") or []
        self._contracts = {item.get("symbol"): item for item in data if isinstance(item, dict)}
        return self._contracts

    def funding_history(self, symbol: str, limit: int = 200) -> list[dict]:
        """Return recent funding history."""
        contract = self.map_symbol(symbol) or symbol

        def _fetch() -> list[dict]:
            params = urlencode({"symbol": contract, "pageSize": limit})
            url = f"{self.base_url}/api/v1/contract/funding-rates?{params}"
            try:
                payload = _get_json(url)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("KuCoin funding history fetch failed for %s: %s", contract, exc)
                return []
            out: list[dict] = []
            if payload.get("code") == "200000":
                for item in payload.get("data") or []:
                    ts_ms = _to_float(item.get("timePoint") or item.get("ts")) or 0
                    out.append(
                        {
                            "ts_ms": int(ts_ms),
                            "rate": _to_float(item.get("fundingRate") or item.get("funding_rate")),
                            "interval_hours": 8.0,
                            "mark_price": _to_float(item.get("markPrice")),
                        }
                    )
            return out

        try:
            from utils.cache_db import get_or_fetch_funding_history

            return get_or_fetch_funding_history(
                self.name,
                contract,
                _fetch,
                max_age_seconds=300,
                limit=limit,
            )
        except Exception:  # pylint: disable=broad-except
            return _fetch()


def _get_json(url: str) -> dict:
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
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
