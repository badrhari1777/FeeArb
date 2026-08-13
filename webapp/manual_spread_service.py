from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Callable, Mapping

from exchanges import get_adapter_cached, normalize_exchange_name
from execution.accounts import _safe_float, normalize_symbol
from execution.manual import spread_pct
from orchestrator.models import MarketSnapshot


class ManualSpreadService:
    def __init__(
        self,
        *,
        market_data_provider: Callable[[], Any],
        positions_market_provider: Callable[
            [],
            tuple[
                Mapping[tuple[str, str], MarketSnapshot],
                Mapping[tuple[str, str], datetime],
            ],
        ],
    ) -> None:
        self._market_data_provider = market_data_provider
        self._positions_market_provider = positions_market_provider

    async def analyze(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = normalize_symbol(str(payload.get("symbol") or "")).upper()
        action = str(payload.get("action") or "enter").lower()
        long_exchange = normalize_exchange_name(str(payload.get("long_exchange") or ""))
        short_exchange = normalize_exchange_name(str(payload.get("short_exchange") or ""))
        from_exchange = normalize_exchange_name(str(payload.get("from_exchange") or ""))
        to_exchange = normalize_exchange_name(str(payload.get("to_exchange") or ""))
        side = str(payload.get("side") or "long").lower()
        errors: list[str] = []
        warnings: list[str] = []

        buy_exchange = ""
        sell_exchange = ""
        if not symbol:
            errors.append("symbol is required")
        if action == "roll":
            if side == "long":
                buy_exchange = to_exchange
                sell_exchange = from_exchange
            elif side == "short":
                buy_exchange = from_exchange
                sell_exchange = to_exchange
            else:
                errors.append("side must be long or short")
            if not from_exchange or not to_exchange:
                errors.append("from_exchange and to_exchange are required")
        elif action == "exit":
            buy_exchange = short_exchange
            sell_exchange = long_exchange
            if not long_exchange or not short_exchange:
                errors.append("long_exchange and short_exchange are required")
        else:
            action = "enter"
            buy_exchange = long_exchange
            sell_exchange = short_exchange
            if not long_exchange or not short_exchange:
                errors.append("long_exchange and short_exchange are required")

        if buy_exchange and sell_exchange and buy_exchange == sell_exchange:
            warnings.append("buy and sell exchanges are the same")
        if errors:
            return {
                "status": "error",
                "symbol": symbol,
                "action": action,
                "errors": errors,
                "warnings": warnings,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        buy_quote = await self.quote_for_exchange(buy_exchange, symbol)
        sell_quote = await self.quote_for_exchange(sell_exchange, symbol)
        buy_price = _safe_float(buy_quote.get("ask"))
        sell_price = _safe_float(sell_quote.get("bid"))
        if buy_price is None:
            errors.append(f"{buy_exchange}: ask unavailable")
        if sell_price is None:
            errors.append(f"{sell_exchange}: bid unavailable")
        spread_val = spread_pct(buy_price, sell_price)
        if spread_val is None and not errors:
            errors.append("spread unavailable")
        status = "ok" if not errors else "partial"
        return {
            "status": status,
            "symbol": symbol,
            "action": action,
            "side": side if action == "roll" else None,
            "buy_exchange": buy_exchange,
            "sell_exchange": sell_exchange,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "spread_pct": spread_val,
            "quotes": {
                buy_exchange: buy_quote,
                sell_exchange: sell_quote,
            },
            "errors": errors,
            "warnings": warnings,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def quote_for_exchange(self, exchange: str, symbol: str) -> dict[str, Any]:
        exchange = normalize_exchange_name(exchange)
        symbol = normalize_symbol(symbol)
        quote: dict[str, Any] = {
            "exchange": exchange,
            "symbol": symbol,
            "bid": None,
            "ask": None,
            "mid": None,
            "mark_price": None,
            "source": None,
            "updated_at": None,
            "age_sec": None,
        }
        if not exchange or not symbol:
            return quote

        try:
            book = await self._market_data_provider().get_orderbook(exchange, symbol, depth=1, max_age_sec=15.0)
        except Exception:  # pylint: disable=broad-except
            book = None
        if book:
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid = _safe_float((bids[0] if bids else [None])[0])
            ask = _safe_float((asks[0] if asks else [None])[0])
            if bid is not None or ask is not None:
                ts = _safe_float(book.get("timestamp"))
                quote.update(
                    {
                        "bid": bid,
                        "ask": ask,
                        "mid": ((bid + ask) / 2.0) if bid and ask else None,
                        "source": "websocket",
                        "updated_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
                        "age_sec": round(time.time() - ts, 3) if ts else None,
                    }
                )
                return quote

        market_cache, market_cache_ts = self._positions_market_provider()
        cached = market_cache.get((exchange, symbol))
        cached_ts = market_cache_ts.get((exchange, symbol))
        if cached and (cached.bid is not None or cached.ask is not None or cached.mark_price is not None):
            bid = _safe_float(cached.bid)
            ask = _safe_float(cached.ask)
            quote.update(
                {
                    "bid": bid,
                    "ask": ask,
                    "mid": ((bid + ask) / 2.0) if bid and ask else _safe_float(cached.mark_price),
                    "mark_price": _safe_float(cached.mark_price),
                    "source": "positions_market_cache",
                    "updated_at": cached_ts.isoformat() if cached_ts else None,
                    "age_sec": round((datetime.now(timezone.utc) - cached_ts).total_seconds(), 3) if cached_ts else None,
                }
            )
            return quote

        try:
            adapter = get_adapter_cached(exchange)
            snapshots = await adapter.fetch_market_snapshots_async([symbol])
        except Exception as exc:  # pylint: disable=broad-except
            quote["error"] = str(exc)
            return quote
        for snapshot in snapshots or []:
            if not isinstance(snapshot, MarketSnapshot):
                continue
            if normalize_symbol(snapshot.symbol) != symbol:
                continue
            bid = _safe_float(snapshot.bid)
            ask = _safe_float(snapshot.ask)
            quote.update(
                {
                    "bid": bid,
                    "ask": ask,
                    "mid": ((bid + ask) / 2.0) if bid and ask else _safe_float(snapshot.mark_price),
                    "mark_price": _safe_float(snapshot.mark_price),
                    "source": "public_rest",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "age_sec": 0.0,
                }
            )
            return quote
        quote["error"] = "snapshot unavailable"
        return quote
