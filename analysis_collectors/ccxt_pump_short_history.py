from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import ccxt

from config import BASE_DIR

DEFAULT_OUTPUT_ROOT = BASE_DIR / "data" / "research" / "pump_short_multiexchange"
DEFAULT_START_MS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

EXCHANGE_IDS: dict[str, str] = {
    "binance": "binanceusdm",
    "bybit": "bybit",
    "okx": "okx",
    "gate": "gate",
    "bitget": "bitget",
    "mexc": "mexc",
    "kucoin": "kucoinfutures",
}


@dataclass(slots=True)
class CcxtPumpShortCollectorConfig:
    exchange: str
    output_root: Path = DEFAULT_OUTPUT_ROOT
    start_ms: int = DEFAULT_START_MS
    end_ms: int | None = None
    sleep_sec: float = 0.05
    daily_prefilter: bool = True
    min_daily_pump_pct: float = 50.0
    min_3d_pump_pct: float = 100.0
    min_7d_pump_pct: float = 180.0
    ohlcv_limit: int = 1000
    funding_limit: int = 1000
    max_retries: int = 3


@dataclass(slots=True)
class CcxtCollectionStats:
    symbols_seen: int = 0
    symbols_prefiltered: int = 0
    symbols_collected: int = 0
    symbols_skipped: int = 0
    symbols_failed: int = 0
    requests_made: int = 0


class CcxtPumpShortHistoryCollector:
    """Public market-data collector for cross-exchange pump-short research.

    It uses ccxt for common futures OHLCV/funding history and writes the same
    high-level sample shape as the Bybit collector where possible. OI and
    long/short series are optional because most exchanges do not expose deep
    public history for them.
    """

    def __init__(self, config: CcxtPumpShortCollectorConfig) -> None:
        self.config = config
        self.exchange_name = normalize_exchange(config.exchange)
        self.exchange_id = EXCHANGE_IDS[self.exchange_name]
        self.output_dir = config.output_root / self.exchange_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats = CcxtCollectionStats()
        self.client = self._make_client()

    def collect(
        self,
        *,
        symbols: Iterable[str] | None = None,
        max_symbols: int | None = None,
        resume: bool = True,
    ) -> CcxtCollectionStats:
        instruments = self.load_instruments()
        requested = {normalize_symbol(item) for item in symbols or [] if normalize_symbol(item)}
        if requested:
            instruments = [item for item in instruments if normalize_symbol(item["symbol"]) in requested]
        instruments = sorted(instruments, key=lambda item: str(item.get("symbol") or ""))
        if max_symbols is not None:
            instruments = instruments[: max(0, max_symbols)]

        done = self._read_done_symbols() if resume else set()
        self.stats.symbols_seen = len(instruments)
        for instrument in instruments:
            symbol = str(instrument["symbol"])
            if resume and normalize_symbol(symbol) in done:
                self.stats.symbols_skipped += 1
                continue
            try:
                if self.config.daily_prefilter and not self._passes_daily_prefilter(symbol):
                    self.stats.symbols_prefiltered += 1
                    self._append_done_symbol(symbol)
                    continue
                sample = self.collect_symbol(instrument)
            except Exception as exc:  # pylint: disable=broad-except
                self.stats.symbols_failed += 1
                self._append_jsonl(
                    self.output_dir / "errors.jsonl",
                    {
                        "ts_ms": now_ms(),
                        "exchange": self.exchange_name,
                        "symbol": symbol,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
                continue

            self._append_jsonl(self.output_dir / "symbol_samples.jsonl", sample)
            self._append_summary(sample["summary"])
            self._append_done_symbol(symbol)
            self.stats.symbols_collected += 1
        self._write_json(self.output_dir / "collection_metadata.json", self._metadata())
        return self.stats

    def load_instruments(self) -> list[dict[str, Any]]:
        if self.exchange_name == "okx":
            return self._load_okx_instruments()
        self._request_pause()
        markets = self.client.load_markets()
        instruments: list[dict[str, Any]] = []
        for market in markets.values():
            if not isinstance(market, dict):
                continue
            if market.get("active") is False:
                continue
            if not market.get("swap"):
                continue
            quote = str(market.get("quote") or "").upper()
            settle = str(market.get("settle") or "").upper()
            if quote != "USDT" and settle != "USDT":
                continue
            symbol = str(market.get("symbol") or "")
            if not symbol:
                continue
            instruments.append(
                {
                    "exchange": self.exchange_name,
                    "symbol": symbol,
                    "canonical_symbol": canonical_symbol_from_market(market),
                    "id": market.get("id"),
                    "base": market.get("base"),
                    "quote": market.get("quote"),
                    "settle": market.get("settle"),
                    "active": market.get("active"),
                    "contract_size": market.get("contractSize"),
                    "raw": compact_market(market),
                }
            )
        self._write_json(
            self.output_dir / "instruments_latest.json",
            {
                "ts_ms": now_ms(),
                "exchange": self.exchange_name,
                "count": len(instruments),
                "symbols": instruments,
            },
        )
        return instruments

    def collect_symbol(self, instrument: dict[str, Any]) -> dict[str, Any]:
        symbol = str(instrument["symbol"])
        canonical_symbol = str(instrument.get("canonical_symbol") or canonical_symbol_from_exchange_symbol(symbol))
        end_ms = self.config.end_ms or now_ms()
        klines_1h = self.fetch_ohlcv(symbol, "1h", self.config.start_ms, end_ms, self.config.ohlcv_limit)
        funding = self.fetch_funding(symbol, self.config.start_ms, end_ms)
        open_interest = self.fetch_open_interest(symbol, self.config.start_ms, end_ms)
        summary = build_symbol_summary(
            exchange=self.exchange_name,
            symbol=canonical_symbol,
            exchange_symbol=symbol,
            ts_ms=end_ms,
            klines_1h=klines_1h,
            funding=funding,
            open_interest=open_interest,
        )
        return {
            "schema": "ccxt_pump_short_sample_v1",
            "exchange": self.exchange_name,
            "ts_ms": end_ms,
            "symbol": canonical_symbol,
            "exchange_symbol": symbol,
            "instrument": instrument,
            "summary": summary,
            "series": {
                "klines_1h": klines_1h,
                "funding": funding,
                "open_interest_1h": open_interest,
                "long_short_1h": [],
            },
        }

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        if self.exchange_name == "okx":
            return self._fetch_okx_ohlcv(symbol, timeframe, start_ms, end_ms)
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        step_ms = timeframe_to_ms(timeframe)
        while cursor <= end_ms:
            batch = self._retry(lambda: self.client.fetch_ohlcv(symbol, timeframe, since=cursor, limit=limit))
            if not batch:
                break
            for row in batch:
                if not row or row[0] is None:
                    continue
                ts_ms = int(row[0])
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue
                rows.append(
                    {
                        "ts_ms": ts_ms,
                        "open": to_float(row[1]),
                        "high": to_float(row[2]),
                        "low": to_float(row[3]),
                        "close": to_float(row[4]),
                        "volume": to_float(row[5]),
                        "turnover": None,
                    }
                )
            last_ts = int(batch[-1][0])
            next_cursor = last_ts + step_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(batch) < max(2, limit // 2):
                break
        return dedupe_sort_by_ts(rows)

    def fetch_funding(self, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        if self.exchange_name == "okx":
            return self._fetch_okx_funding(symbol, start_ms, end_ms)
        if not self.client.has.get("fetchFundingRateHistory"):
            return []
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        while cursor <= end_ms:
            batch = self._retry(
                lambda: self.client.fetch_funding_rate_history(
                    symbol,
                    since=cursor,
                    limit=self.config.funding_limit,
                ),
                optional=True,
            )
            if not batch:
                break
            last_ts: int | None = None
            for item in batch:
                ts_ms = to_int(item.get("timestamp"))
                rate = to_float(item.get("fundingRate") or item.get("funding_rate"))
                if ts_ms is None:
                    continue
                last_ts = ts_ms
                if start_ms <= ts_ms <= end_ms:
                    rows.append({"ts_ms": ts_ms, "funding_rate": rate})
            if last_ts is None or last_ts < cursor:
                break
            cursor = last_ts + 1
            if len(batch) < self.config.funding_limit:
                break
        return dedupe_sort_by_ts(rows)

    def fetch_open_interest(self, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        if self.exchange_name == "okx":
            return []
        if not self.client.has.get("fetchOpenInterestHistory"):
            return []
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        while cursor <= end_ms:
            batch = self._retry(
                lambda: self.client.fetch_open_interest_history(
                    symbol,
                    timeframe="1h",
                    since=cursor,
                    limit=500,
                ),
                optional=True,
            )
            if not batch:
                break
            last_ts: int | None = None
            for item in batch:
                ts_ms = to_int(item.get("timestamp"))
                value = (
                    item.get("openInterestAmount")
                    or item.get("openInterest")
                    or item.get("baseVolume")
                    or item.get("quoteVolume")
                )
                if ts_ms is None:
                    continue
                last_ts = ts_ms
                if start_ms <= ts_ms <= end_ms:
                    rows.append({"ts_ms": ts_ms, "open_interest": to_float(value)})
            if last_ts is None or last_ts < cursor:
                break
            cursor = last_ts + 3_600_000
            if len(batch) < 500:
                break
        return dedupe_sort_by_ts(rows)

    def _passes_daily_prefilter(self, symbol: str) -> bool:
        rows = self.fetch_ohlcv(symbol, "1d", self.config.start_ms, self.config.end_ms or now_ms(), 1000)
        if len(rows) < 2:
            return False
        for idx, row in enumerate(rows):
            high = row.get("high")
            if high is None:
                continue
            for days, threshold in (
                (1, self.config.min_daily_pump_pct),
                (3, self.config.min_3d_pump_pct),
                (7, self.config.min_7d_pump_pct),
            ):
                start = max(0, idx - days)
                base = rows[start].get("close")
                if base and pct_change(high, base) is not None and pct_change(high, base) >= threshold:
                    return True
        return False

    def _make_client(self) -> Any:
        cls = getattr(ccxt, self.exchange_id)
        options: dict[str, Any] = {"defaultType": "swap"}
        if self.exchange_name == "binance":
            options = {"defaultType": "future"}
        if self.exchange_name == "okx":
            options = {"defaultType": "swap"}
        return cls({"enableRateLimit": True, "options": options, "timeout": 30000})

    def _load_okx_instruments(self) -> list[dict[str, Any]]:
        payload = self._get_json_url(
            "https://www.okx.com/api/v5/public/instruments?"
            + urlencode({"instType": "SWAP"})
        )
        rows = payload.get("data") if isinstance(payload, dict) else []
        instruments: list[dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("settleCcy") or "").upper() != "USDT":
                continue
            if str(row.get("state") or "").lower() != "live":
                continue
            symbol = str(row.get("instId") or "")
            if not symbol:
                continue
            instruments.append(
                {
                    "exchange": self.exchange_name,
                    "symbol": symbol,
                    "canonical_symbol": canonical_symbol_from_exchange_symbol(symbol),
                    "id": symbol,
                    "base": row.get("uly"),
                    "quote": "USDT",
                    "settle": "USDT",
                    "active": True,
                    "contract_size": to_float(row.get("ctVal")),
                    "raw": row,
                }
            )
        self._write_json(
            self.output_dir / "instruments_latest.json",
            {
                "ts_ms": now_ms(),
                "exchange": self.exchange_name,
                "count": len(instruments),
                "symbols": instruments,
            },
        )
        return instruments

    def _fetch_okx_ohlcv(self, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        bar = "1D" if timeframe == "1d" else "1H"
        rows: list[dict[str, Any]] = []
        cursor = end_ms + timeframe_to_ms(timeframe)
        while cursor >= start_ms:
            payload = self._get_json_url(
                "https://www.okx.com/api/v5/market/history-candles?"
                + urlencode({"instId": symbol, "bar": bar, "limit": "100", "after": str(cursor)})
            )
            data = payload.get("data") if isinstance(payload, dict) else []
            if not data:
                break
            min_ts: int | None = None
            for row in data:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                ts_ms = to_int(row[0])
                if ts_ms is None:
                    continue
                min_ts = ts_ms if min_ts is None else min(min_ts, ts_ms)
                if start_ms <= ts_ms <= end_ms:
                    rows.append(
                        {
                            "ts_ms": ts_ms,
                            "open": to_float(row[1]),
                            "high": to_float(row[2]),
                            "low": to_float(row[3]),
                            "close": to_float(row[4]),
                            "volume": to_float(row[5]),
                            "turnover": to_float(row[7]) if len(row) > 7 else None,
                        }
                    )
            if min_ts is None or min_ts >= cursor:
                break
            cursor = min_ts
            if min_ts < start_ms:
                break
        return dedupe_sort_by_ts(rows)

    def _fetch_okx_funding(self, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = end_ms + 1
        while cursor >= start_ms:
            payload = self._get_json_url(
                "https://www.okx.com/api/v5/public/funding-rate-history?"
                + urlencode({"instId": symbol, "limit": "100", "after": str(cursor)})
            )
            data = payload.get("data") if isinstance(payload, dict) else []
            if not data:
                break
            min_ts: int | None = None
            for row in data:
                if not isinstance(row, dict):
                    continue
                ts_ms = to_int(row.get("fundingTime"))
                if ts_ms is None:
                    continue
                min_ts = ts_ms if min_ts is None else min(min_ts, ts_ms)
                if start_ms <= ts_ms <= end_ms:
                    rows.append({"ts_ms": ts_ms, "funding_rate": to_float(row.get("realizedRate") or row.get("fundingRate"))})
            if min_ts is None or min_ts >= cursor:
                break
            cursor = min_ts
            if min_ts < start_ms:
                break
        return dedupe_sort_by_ts(rows)

    def _get_json_url(self, url: str) -> dict[str, Any]:
        def _fetch() -> dict[str, Any]:
            req = Request(url, headers={"Accept": "application/json", "User-Agent": "FeeArbResearch/1.0"})
            with urlopen(req, timeout=30) as resp:  # nosec
                return json.loads(resp.read().decode("utf-8"))

        return self._retry(_fetch)

    def _retry(self, fn: Any, *, optional: bool = False) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                self._request_pause()
                return fn()
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
                if optional:
                    return []
                if attempt >= self.config.max_retries:
                    break
                time.sleep(min(15.0, 1.5 * (attempt + 1)))
        raise RuntimeError(last_error)

    def _request_pause(self) -> None:
        self.stats.requests_made += 1
        if self.config.sleep_sec > 0:
            time.sleep(self.config.sleep_sec)

    def _metadata(self) -> dict[str, Any]:
        return {
            "schema": "ccxt_pump_short_collection_v1",
            "exchange": self.exchange_name,
            "exchange_id": self.exchange_id,
            "start_ms": self.config.start_ms,
            "start_iso": ms_to_iso(self.config.start_ms),
            "end_ms": self.config.end_ms,
            "end_iso": ms_to_iso(self.config.end_ms),
            "daily_prefilter": self.config.daily_prefilter,
            "stats": asdict(self.stats),
        }

    def _read_done_symbols(self) -> set[str]:
        path = self.output_dir / "done_symbols.txt"
        if not path.exists():
            return set()
        return {normalize_symbol(line.strip()) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}

    def _append_done_symbol(self, symbol: str) -> None:
        with (self.output_dir / "done_symbols.txt").open("a", encoding="utf-8") as handle:
            handle.write(f"{normalize_symbol(symbol)}\n")

    def _append_summary(self, summary: dict[str, Any]) -> None:
        path = self.output_dir / "symbol_summary.csv"
        fieldnames = [
            "exchange",
            "symbol",
            "exchange_symbol",
            "ts_iso",
            "price_history_hours",
            "last_close",
            "lookback_high_pct_from_first",
            "drawdown_from_lookback_high_pct",
            "funding_sum_24h_pct",
            "funding_sum_7d_pct",
            "oi_points",
            "pump_score",
            "data_quality",
        ]
        write_header = not path.exists()
        with path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow(summary)

    @staticmethod
    def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
            handle.write("\n")

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def build_symbol_summary(
    *,
    exchange: str,
    symbol: str,
    exchange_symbol: str,
    ts_ms: int,
    klines_1h: list[dict[str, Any]],
    funding: list[dict[str, Any]],
    open_interest: list[dict[str, Any]],
) -> dict[str, Any]:
    closes = [(int(row["ts_ms"]), row.get("close")) for row in klines_1h if row.get("close")]
    highs = [row.get("high") for row in klines_1h if row.get("high")]
    first_close = closes[0][1] if closes else None
    last_close = closes[-1][1] if closes else None
    lookback_high = max(highs) if highs else None
    price_history_hours = (closes[-1][0] - closes[0][0]) / 3_600_000.0 if len(closes) >= 2 else None
    high_from_first = pct_change(lookback_high, first_close) if lookback_high and first_close else None
    drawdown = pct_change(last_close, lookback_high) if last_close and lookback_high else None
    data_quality = {
        "klines_1h": len(klines_1h),
        "funding": len(funding),
        "open_interest_1h": len(open_interest),
        "long_short_1h": 0,
    }
    return {
        "exchange": exchange,
        "symbol": symbol,
        "exchange_symbol": exchange_symbol,
        "ts_ms": ts_ms,
        "ts_iso": ms_to_iso(ts_ms),
        "price_history_hours": round_float(price_history_hours, 2),
        "last_close": round_float(last_close),
        "lookback_high_pct_from_first": round_float(high_from_first),
        "drawdown_from_lookback_high_pct": round_float(drawdown),
        "funding_sum_24h_pct": round_float(scale_pct(sum_funding_since(funding, ts_ms - 86_400_000))),
        "funding_sum_7d_pct": round_float(scale_pct(sum_funding_since(funding, ts_ms - 7 * 86_400_000))),
        "oi_points": len(open_interest),
        "pump_score": round_float(min(max(high_from_first or 0.0, 0.0), 1000.0) / 10.0),
        "data_quality": json.dumps(data_quality, ensure_ascii=True, sort_keys=True),
    }


def compact_market(market: dict[str, Any]) -> dict[str, Any]:
    keys = ("id", "symbol", "base", "quote", "settle", "swap", "linear", "active", "contractSize", "created")
    return {key: market.get(key) for key in keys}


def normalize_exchange(value: Any) -> str:
    name = str(value or "").strip().lower()
    if name not in EXCHANGE_IDS:
        raise ValueError(f"unsupported_exchange:{name}")
    return name


def normalize_symbol(value: Any) -> str:
    text = str(value or "").upper().strip()
    for part in ("/", ":", "-", "_"):
        text = text.replace(part, "")
    return text


def canonical_symbol_from_market(market: dict[str, Any]) -> str:
    base = str(market.get("base") or "").upper().strip()
    quote = str(market.get("quote") or market.get("settle") or "USDT").upper().strip()
    if base and quote:
        return f"{base}{quote}"
    return canonical_symbol_from_exchange_symbol(market.get("symbol") or market.get("id"))


def canonical_symbol_from_exchange_symbol(value: Any) -> str:
    text = str(value or "").upper().strip()
    if text.endswith("-USDT-SWAP"):
        return text.replace("-USDT-SWAP", "USDT").replace("-", "")
    if "/USDT" in text:
        return text.split("/USDT", 1)[0].replace("/", "").replace("-", "").replace("_", "") + "USDT"
    if text.endswith("_USDT"):
        return text.replace("_", "")
    if text.endswith("-USDT"):
        return text.replace("-", "")
    return normalize_symbol(text)


def timeframe_to_ms(value: str) -> int:
    text = str(value).strip().lower()
    unit = text[-1]
    qty = int(text[:-1] or "1")
    if unit == "m":
        return qty * 60_000
    if unit == "h":
        return qty * 3_600_000
    if unit == "d":
        return qty * 86_400_000
    raise ValueError(f"unsupported_timeframe:{value}")


def dedupe_sort_by_ts(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ts: dict[int, dict[str, Any]] = {}
    for row in rows:
        ts_ms = row.get("ts_ms")
        if ts_ms is None:
            continue
        by_ts[int(ts_ms)] = row
    return [by_ts[ts_ms] for ts_ms in sorted(by_ts)]


def sum_funding_since(rows: list[dict[str, Any]], start_ms: int) -> float | None:
    values = [row.get("funding_rate") for row in rows if (row.get("ts_ms") or 0) >= start_ms]
    values = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return sum(values) if values else None


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in {None, 0}:
        return None
    return (float(current) / float(previous) - 1.0) * 100.0


def to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def round_float(value: float | None, digits: int = 8) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def scale_pct(value: float | None) -> float | None:
    return value * 100.0 if value is not None else None


def now_ms() -> int:
    return int(time.time() * 1000)


def ms_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


__all__ = [
    "CcxtPumpShortCollectorConfig",
    "CcxtPumpShortHistoryCollector",
    "CcxtCollectionStats",
    "EXCHANGE_IDS",
]
