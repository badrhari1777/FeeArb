from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import BASE_DIR

BYBIT_API_BASE = "https://api.bybit.com"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short"
DEFAULT_USER_AGENT = "FeeArbResearch/1.0 (+public-market-data; slow-rate)"
NON_CRYPTO_BASE_COINS = {
    "AAPL",
    "ABNB",
    "AMZN",
    "ASML",
    "AVGO",
    "COIN",
    "CRCL",
    "GLW",
    "GOOG",
    "GOOGL",
    "HPE",
    "KLAC",
    "KORU",
    "LRCX",
    "META",
    "MSFT",
    "MSTR",
    "NVDA",
    "QQQ",
    "SMCI",
    "SPY",
    "STXX",
    "TSLA",
    "TQQQ",
    "UVXY",
}


@dataclass(slots=True)
class BybitCollectorConfig:
    output_dir: Path = DEFAULT_OUTPUT_DIR
    sleep_sec: float = 0.8
    timeout_sec: float = 20.0
    lookback_days: int = 30
    max_retries: int = 3
    stop_on_403: bool = True
    user_agent: str = DEFAULT_USER_AGENT


@dataclass(slots=True)
class BybitInstrument:
    symbol: str
    base_coin: str
    quote_coin: str
    launch_time_ms: int | None
    status: str
    funding_interval_min: int | None
    upper_funding_rate: float | None
    lower_funding_rate: float | None
    min_order_qty: float | None
    qty_step: float | None
    min_notional: float | None
    max_leverage: float | None
    raw: dict[str, Any]


@dataclass(slots=True)
class CollectionStats:
    symbols_seen: int = 0
    symbols_collected: int = 0
    symbols_skipped: int = 0
    symbols_failed: int = 0
    requests_made: int = 0


class BybitPumpShortCollector:
    """Slow Bybit public-data collector for pump-short research.

    The collector intentionally fetches one symbol at a time and sleeps between
    every HTTP request. It is designed for long unattended research runs rather
    than fast market polling.
    """

    def __init__(self, config: BybitCollectorConfig | None = None) -> None:
        self.config = config or BybitCollectorConfig()
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats = CollectionStats()

    def collect(
        self,
        *,
        symbols: Iterable[str] | None = None,
        max_symbols: int | None = None,
        newest_first: bool = True,
        resume: bool = True,
    ) -> CollectionStats:
        instruments = self.load_instruments()
        requested = {normalize_symbol(item) for item in symbols or [] if normalize_symbol(item)}
        if requested:
            instruments = [item for item in instruments if item.symbol in requested]
        instruments = sorted(
            instruments,
            key=lambda item: item.launch_time_ms or 0,
            reverse=newest_first,
        )
        instruments = dedupe_instruments(instruments)
        if max_symbols is not None:
            instruments = instruments[: max(0, max_symbols)]

        done = self._read_done_symbols() if resume else set()
        self.stats.symbols_seen = len(instruments)
        for instrument in instruments:
            if resume and instrument.symbol in done:
                self.stats.symbols_skipped += 1
                continue
            try:
                sample = self.collect_symbol(instrument)
            except Exception as exc:  # pylint: disable=broad-except
                self.stats.symbols_failed += 1
                self._append_jsonl(
                    self.config.output_dir / "errors.jsonl",
                    {
                        "ts_ms": now_ms(),
                        "symbol": instrument.symbol,
                        "error": str(exc),
                    },
                )
                continue

            self._append_jsonl(self.config.output_dir / "symbol_samples.jsonl", sample)
            self._append_summary(sample["summary"])
            self._append_done_symbol(instrument.symbol)
            self.stats.symbols_collected += 1
        return self.stats

    def load_instruments(self) -> list[BybitInstrument]:
        instruments: list[BybitInstrument] = []
        cursor: str | None = None
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            payload = self._get_json("/v5/market/instruments-info", params)
            result = payload.get("result") if isinstance(payload, dict) else {}
            rows = result.get("list") if isinstance(result, dict) else []
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                if str(row.get("contractType") or "") != "LinearPerpetual":
                    continue
                if str(row.get("quoteCoin") or "").upper() != "USDT":
                    continue
                if str(row.get("status") or "") != "Trading":
                    continue
                symbol = normalize_symbol(row.get("symbol"))
                if not symbol:
                    continue
                lot = row.get("lotSizeFilter") if isinstance(row.get("lotSizeFilter"), dict) else {}
                leverage = row.get("leverageFilter") if isinstance(row.get("leverageFilter"), dict) else {}
                instruments.append(
                    BybitInstrument(
                        symbol=symbol,
                        base_coin=str(row.get("baseCoin") or "").upper(),
                        quote_coin=str(row.get("quoteCoin") or "").upper(),
                        launch_time_ms=to_int(row.get("launchTime")),
                        status=str(row.get("status") or ""),
                        funding_interval_min=to_int(row.get("fundingInterval")),
                        upper_funding_rate=to_float(row.get("upperFundingRate")),
                        lower_funding_rate=to_float(row.get("lowerFundingRate")),
                        min_order_qty=to_float(lot.get("minOrderQty")),
                        qty_step=to_float(lot.get("qtyStep")),
                        min_notional=to_float(lot.get("minNotionalValue")),
                        max_leverage=to_float(leverage.get("maxLeverage")),
                        raw=row,
                    )
                )
            cursor = str(result.get("nextPageCursor") or "") if isinstance(result, dict) else ""
            if not cursor:
                break
        self._write_json(
            self.config.output_dir / "instruments_latest.json",
            {
                "ts_ms": now_ms(),
                "count": len(instruments),
                "symbols": [instrument_to_dict(item) for item in instruments],
            },
        )
        return instruments

    def collect_symbol(self, instrument: BybitInstrument) -> dict[str, Any]:
        end_ms = now_ms()
        start_ms = max(
            end_ms - self.config.lookback_days * 86_400_000,
            (instrument.launch_time_ms or 0),
        )
        klines_1h = self.fetch_klines(instrument.symbol, interval="60", start_ms=start_ms, end_ms=end_ms)
        premium_index_1h = self.fetch_price_klines(
            "/v5/market/premium-index-price-kline",
            instrument.symbol,
            interval="60",
            start_ms=start_ms,
            end_ms=end_ms,
        )
        funding = self.fetch_funding_history(instrument.symbol, start_ms=start_ms, end_ms=end_ms)
        open_interest = self.fetch_open_interest(
            instrument.symbol,
            interval_time="1h",
            start_ms=start_ms,
            end_ms=end_ms,
        )
        long_short = self.fetch_long_short_ratio(
            instrument.symbol,
            period="1h",
            start_ms=start_ms,
            end_ms=end_ms,
        )
        summary = build_symbol_summary(
            instrument=instrument,
            ts_ms=end_ms,
            klines_1h=klines_1h,
            funding=funding,
            open_interest=open_interest,
            long_short=long_short,
        )
        return {
            "schema": "bybit_pump_short_sample_v1",
            "ts_ms": end_ms,
            "symbol": instrument.symbol,
            "instrument": instrument_to_dict(instrument),
            "summary": summary,
            "series": {
                "klines_1h": klines_1h,
                "premium_index_1h": premium_index_1h,
                "funding": funding,
                "open_interest_1h": open_interest,
                "long_short_1h": long_short,
            },
        }

    def fetch_klines(
        self,
        symbol: str,
        *,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        interval_ms = interval_to_ms(interval)
        max_span_ms = max(1, limit - 1) * interval_ms
        parsed: list[dict[str, Any]] = []
        cursor_start = start_ms
        while cursor_start <= end_ms:
            cursor_end = min(end_ms, cursor_start + max_span_ms)
            payload = self._get_json(
                "/v5/market/kline",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval,
                    "start": cursor_start,
                    "end": cursor_end,
                    "limit": limit,
                },
            )
            rows = ((payload.get("result") or {}).get("list") or []) if isinstance(payload, dict) else []
            for row in rows:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                parsed.append(
                    {
                        "ts_ms": to_int(row[0]),
                        "open": to_float(row[1]),
                        "high": to_float(row[2]),
                        "low": to_float(row[3]),
                        "close": to_float(row[4]),
                        "volume": to_float(row[5]),
                        "turnover": to_float(row[6]),
                    }
                )
            cursor_start = cursor_end + interval_ms
        return dedupe_sort_by_ts(parsed)

    def fetch_price_klines(
        self,
        path: str,
        symbol: str,
        *,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        interval_ms = interval_to_ms(interval)
        max_span_ms = max(1, limit - 1) * interval_ms
        parsed: list[dict[str, Any]] = []
        cursor_start = start_ms
        while cursor_start <= end_ms:
            cursor_end = min(end_ms, cursor_start + max_span_ms)
            payload = self._get_json(
                path,
                {
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval,
                    "start": cursor_start,
                    "end": cursor_end,
                    "limit": limit,
                },
            )
            rows = ((payload.get("result") or {}).get("list") or []) if isinstance(payload, dict) else []
            for row in rows:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                parsed.append(
                    {
                        "ts_ms": to_int(row[0]),
                        "open": to_float(row[1]),
                        "high": to_float(row[2]),
                        "low": to_float(row[3]),
                        "close": to_float(row[4]),
                    }
                )
            cursor_start = cursor_end + interval_ms
        return dedupe_sort_by_ts(parsed)

    def fetch_funding_history(
        self,
        symbol: str,
        *,
        start_ms: int,
        end_ms: int,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        # Funding intervals are instrument-specific and can change (notably
        # HUSDT settled hourly). Paginate backwards from the oldest returned
        # row instead of assuming every instrument settles every eight hours.
        cursor_end = end_ms
        while cursor_end >= start_ms:
            payload = self._get_json(
                "/v5/market/funding/history",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "startTime": start_ms,
                    "endTime": cursor_end,
                    "limit": limit,
                },
            )
            rows = ((payload.get("result") or {}).get("list") or []) if isinstance(payload, dict) else []
            page = [
                {
                    "ts_ms": to_int(row.get("fundingRateTimestamp")),
                    "funding_rate": to_float(row.get("fundingRate")),
                }
                for row in rows
                if isinstance(row, dict)
            ]
            page = [row for row in page if row["ts_ms"]]
            if not page:
                break
            parsed.extend(page)
            oldest = min(row["ts_ms"] for row in page)
            if oldest <= start_ms or len(page) < limit:
                break
            next_end = oldest - 1
            if next_end >= cursor_end:
                break
            cursor_end = next_end
        return dedupe_sort_by_ts(parsed)

    def fetch_open_interest(
        self,
        symbol: str,
        *,
        interval_time: str,
        start_ms: int,
        end_ms: int,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        interval_ms = interval_to_ms(interval_time)
        max_span_ms = max(1, limit - 1) * interval_ms
        parsed: list[dict[str, Any]] = []
        cursor_start = start_ms
        while cursor_start <= end_ms:
            cursor_end = min(end_ms, cursor_start + max_span_ms)
            payload = self._get_json(
                "/v5/market/open-interest",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "intervalTime": interval_time,
                    "startTime": cursor_start,
                    "endTime": cursor_end,
                    "limit": limit,
                },
            )
            rows = ((payload.get("result") or {}).get("list") or []) if isinstance(payload, dict) else []
            parsed.extend(
                {
                    "ts_ms": to_int(row.get("timestamp")),
                    "open_interest": to_float(row.get("openInterest")),
                    "single_open_interest": to_float(row.get("singleOpenInterest")),
                }
                for row in rows
                if isinstance(row, dict)
            )
            cursor_start = cursor_end + interval_ms
        return dedupe_sort_by_ts(parsed)

    def fetch_long_short_ratio(
        self,
        symbol: str,
        *,
        period: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        interval_ms = interval_to_ms(period)
        max_span_ms = max(1, limit - 1) * interval_ms
        parsed: list[dict[str, Any]] = []
        cursor_start = start_ms
        while cursor_start <= end_ms:
            cursor_end = min(end_ms, cursor_start + max_span_ms)
            payload = self._get_json(
                "/v5/market/account-ratio",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "period": period,
                    "startTime": cursor_start,
                    "endTime": cursor_end,
                    "limit": limit,
                },
            )
            rows = ((payload.get("result") or {}).get("list") or []) if isinstance(payload, dict) else []
            parsed.extend(
                {
                    "ts_ms": to_int(row.get("timestamp")),
                    "buy_ratio": to_float(row.get("buyRatio")),
                    "sell_ratio": to_float(row.get("sellRatio")),
                }
                for row in rows
                if isinstance(row, dict)
            )
            cursor_start = cursor_end + interval_ms
        return dedupe_sort_by_ts(parsed)

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{BYBIT_API_BASE}{path}?{urlencode({k: v for k, v in params.items() if v is not None})}"
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            if self.stats.requests_made > 0 and self.config.sleep_sec > 0:
                time.sleep(self.config.sleep_sec)
            self.stats.requests_made += 1
            req = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": self.config.user_agent,
                },
            )
            try:
                with urlopen(req, timeout=self.config.timeout_sec) as resp:  # nosec
                    payload = json.loads(resp.read().decode("utf-8"))
                code = str(payload.get("retCode", "0")) if isinstance(payload, dict) else "0"
                if code not in {"0", ""}:
                    raise RuntimeError(f"Bybit retCode={code}: {payload.get('retMsg')}")
                return payload
            except HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                last_error = RuntimeError(f"HTTP {exc.code} from Bybit: {body[:500]}")
                if exc.code == 403 and self.config.stop_on_403:
                    raise last_error
                if exc.code in {403, 429}:
                    time.sleep(min(60.0, 5.0 * (attempt + 1)))
                    continue
            except (URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
                last_error = exc
                if attempt < self.config.max_retries:
                    time.sleep(min(30.0, 2.0 * (attempt + 1)))
                    continue
                break
        raise RuntimeError(f"Bybit request failed for {path}: {last_error}")

    def _read_done_symbols(self) -> set[str]:
        path = self.config.output_dir / "done_symbols.txt"
        if not path.exists():
            return set()
        return {normalize_symbol(line.strip()) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}

    def _append_done_symbol(self, symbol: str) -> None:
        with (self.config.output_dir / "done_symbols.txt").open("a", encoding="utf-8") as handle:
            handle.write(f"{symbol}\n")

    def _append_summary(self, summary: dict[str, Any]) -> None:
        path = self.config.output_dir / "symbol_summary.csv"
        fieldnames = [
            "ts_iso",
            "symbol",
            "launch_iso",
            "age_days",
            "price_history_hours",
            "last_close",
            "return_24h_pct",
            "return_3d_pct",
            "return_7d_pct",
            "return_14d_pct",
            "drawdown_from_lookback_high_pct",
            "lookback_high_pct_from_first",
            "funding_latest_pct",
            "funding_sum_24h_pct",
            "funding_sum_3d_pct",
            "funding_sum_7d_pct",
            "oi_change_4h_pct",
            "oi_change_24h_pct",
            "long_account_ratio",
            "pump_score",
            "continuation_risk_score",
            "candidate_tier",
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
    instrument: BybitInstrument,
    ts_ms: int,
    klines_1h: list[dict[str, Any]],
    funding: list[dict[str, Any]],
    open_interest: list[dict[str, Any]],
    long_short: list[dict[str, Any]],
) -> dict[str, Any]:
    closes = [(int(row["ts_ms"]), row.get("close")) for row in klines_1h if row.get("close")]
    highs = [row.get("high") for row in klines_1h if row.get("high")]
    first_close = closes[0][1] if closes else None
    last_close = closes[-1][1] if closes else None
    price_history_hours = (
        (closes[-1][0] - closes[0][0]) / 3_600_000.0 if len(closes) >= 2 else None
    )
    lookback_high = max(highs) if highs else None

    return_24h = pct_change_from_hours(closes, 24)
    return_3d = pct_change_from_hours(closes, 72)
    return_7d = pct_change_from_hours(closes, 168)
    return_14d = pct_change_from_hours(closes, 336)
    drawdown = pct_change(last_close, lookback_high) if last_close and lookback_high else None
    high_from_first = pct_change(lookback_high, first_close) if lookback_high and first_close else None

    funding_latest = funding[-1].get("funding_rate") if funding else None
    oi_change_4h = oi_change_pct(open_interest, 4)
    oi_change_24h = oi_change_pct(open_interest, 24)
    latest_ratio = latest_long_ratio(long_short)
    pump_score = score_pump(return_24h, return_3d, return_7d, high_from_first)
    continuation_risk = score_continuation_risk(
        return_24h=return_24h,
        return_3d=return_3d,
        drawdown_from_high=drawdown,
        oi_change_4h=oi_change_4h,
        long_account_ratio=latest_ratio,
    )
    data_quality = {
        "klines_1h": len(klines_1h),
        "funding": len(funding),
        "open_interest_1h": len(open_interest),
        "long_short_1h": len(long_short),
    }
    return {
        "ts_ms": ts_ms,
        "ts_iso": ms_to_iso(ts_ms),
        "symbol": instrument.symbol,
        "launch_ms": instrument.launch_time_ms,
        "launch_iso": ms_to_iso(instrument.launch_time_ms),
        "age_days": round((ts_ms - instrument.launch_time_ms) / 86_400_000, 4)
        if instrument.launch_time_ms
        else None,
        "price_history_hours": round_float(price_history_hours, 2),
        "last_close": round_float(last_close),
        "return_24h_pct": round_float(return_24h),
        "return_3d_pct": round_float(return_3d),
        "return_7d_pct": round_float(return_7d),
        "return_14d_pct": round_float(return_14d),
        "drawdown_from_lookback_high_pct": round_float(drawdown),
        "lookback_high_pct_from_first": round_float(high_from_first),
        "funding_latest_pct": round_float(scale_pct(funding_latest)),
        "funding_sum_24h_pct": round_float(scale_pct(sum_funding_since(funding, ts_ms - 86_400_000))),
        "funding_sum_3d_pct": round_float(scale_pct(sum_funding_since(funding, ts_ms - 3 * 86_400_000))),
        "funding_sum_7d_pct": round_float(scale_pct(sum_funding_since(funding, ts_ms - 7 * 86_400_000))),
        "oi_change_4h_pct": round_float(oi_change_4h),
        "oi_change_24h_pct": round_float(oi_change_24h),
        "long_account_ratio": round_float(latest_ratio),
        "pump_score": round_float(pump_score),
        "continuation_risk_score": round_float(continuation_risk),
        "candidate_tier": classify_candidate(pump_score, continuation_risk, funding_latest),
        "data_quality": json.dumps(data_quality, ensure_ascii=True, sort_keys=True),
    }


def pct_change_from_hours(closes: list[tuple[int, float | None]], hours: int) -> float | None:
    if len(closes) < 2:
        return None
    latest_ts, latest_close = closes[-1]
    if not latest_close:
        return None
    cutoff = latest_ts - hours * 3_600_000
    prior = None
    for ts_ms, close in reversed(closes):
        if ts_ms <= cutoff and close:
            prior = close
            break
    if prior is None:
        return None
    return pct_change(latest_close, prior) if prior else None


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in {None, 0}:
        return None
    return (float(current) / float(previous) - 1.0) * 100.0


def interval_to_ms(value: str) -> int:
    text = str(value or "").strip().lower()
    if not text:
        raise ValueError("empty_interval")
    if text.endswith("min"):
        return int(text[:-3] or "1") * 60_000
    unit = text[-1]
    if unit in {"m", "h", "d", "w"}:
        number = int(text[:-1] or "1")
        if unit == "m":
            return number * 60_000
        if unit == "h":
            return number * 3_600_000
        if unit == "d":
            return number * 86_400_000
        return number * 7 * 86_400_000
    return int(text) * 60_000


def dedupe_sort_by_ts(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ts: dict[int, dict[str, Any]] = {}
    for row in rows:
        ts_ms = row.get("ts_ms")
        if ts_ms is None:
            continue
        by_ts[int(ts_ms)] = row
    return [by_ts[ts_ms] for ts_ms in sorted(by_ts)]


def dedupe_instruments(instruments: Iterable[BybitInstrument]) -> list[BybitInstrument]:
    by_symbol: dict[str, BybitInstrument] = {}
    for item in instruments:
        by_symbol[item.symbol] = item
    return list(by_symbol.values())


def sum_funding_since(rows: list[dict[str, Any]], start_ms: int) -> float | None:
    values = [row.get("funding_rate") for row in rows if (row.get("ts_ms") or 0) >= start_ms]
    values = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not values:
        return None
    return sum(values)


def oi_change_pct(rows: list[dict[str, Any]], hours: int) -> float | None:
    points = [(int(row["ts_ms"]), row.get("open_interest")) for row in rows if row.get("open_interest")]
    if len(points) < 2:
        return None
    latest_ts, latest_oi = points[-1]
    cutoff = latest_ts - hours * 3_600_000
    prior = None
    for ts_ms, value in reversed(points):
        if ts_ms <= cutoff and value:
            prior = value
            break
    if prior is None:
        prior = points[0][1]
    return pct_change(latest_oi, prior)


def latest_long_ratio(rows: list[dict[str, Any]]) -> float | None:
    if not rows:
        return None
    value = rows[-1].get("buy_ratio")
    return float(value) if value is not None else None


def score_pump(
    return_24h: float | None,
    return_3d: float | None,
    return_7d: float | None,
    high_from_first: float | None,
) -> float:
    score = 0.0
    score += min(max(return_24h or 0.0, 0.0), 150.0) * 0.20
    score += min(max(return_3d or 0.0, 0.0), 300.0) * 0.18
    score += min(max(return_7d or 0.0, 0.0), 600.0) * 0.10
    score += min(max(high_from_first or 0.0, 0.0), 800.0) * 0.05
    return min(score, 100.0)


def score_continuation_risk(
    *,
    return_24h: float | None,
    return_3d: float | None,
    drawdown_from_high: float | None,
    oi_change_4h: float | None,
    long_account_ratio: float | None,
) -> float:
    score = 0.0
    if (return_24h or 0.0) > 50.0:
        score += 20.0
    if (return_3d or 0.0) > 150.0:
        score += 20.0
    if drawdown_from_high is None or drawdown_from_high > -10.0:
        score += 25.0
    if (oi_change_4h or 0.0) > 20.0:
        score += 20.0
    if long_account_ratio is not None and long_account_ratio > 0.62:
        score += 15.0
    return min(score, 100.0)


def classify_candidate(
    pump_score: float,
    continuation_risk_score: float,
    funding_latest: float | None,
) -> str:
    if pump_score < 35.0:
        return "ignore_no_extreme_pump"
    if continuation_risk_score >= 70.0:
        return "watch_only_high_continuation_risk"
    if funding_latest is not None and funding_latest < -0.003:
        return "watch_only_toxic_negative_funding"
    if pump_score >= 60.0 and continuation_risk_score <= 45.0:
        return "research_short_candidate"
    return "watchlist"


def instrument_to_dict(instrument: BybitInstrument) -> dict[str, Any]:
    return {
        "symbol": instrument.symbol,
        "base_coin": instrument.base_coin,
        "quote_coin": instrument.quote_coin,
        "launch_time_ms": instrument.launch_time_ms,
        "launch_iso": ms_to_iso(instrument.launch_time_ms),
        "status": instrument.status,
        "funding_interval_min": instrument.funding_interval_min,
        "upper_funding_rate": instrument.upper_funding_rate,
        "lower_funding_rate": instrument.lower_funding_rate,
        "min_order_qty": instrument.min_order_qty,
        "qty_step": instrument.qty_step,
        "min_notional": instrument.min_notional,
        "max_leverage": instrument.max_leverage,
    }


def normalize_symbol(value: Any) -> str:
    symbol = str(value or "").upper().strip()
    return symbol.replace("/", "").replace("-", "").replace("_", "")


def is_crypto_pump_short_instrument(instrument: BybitInstrument) -> bool:
    if str(instrument.raw.get("symbolType") or "").lower() == "stock":
        return False
    return instrument.base_coin.upper() not in NON_CRYPTO_BASE_COINS


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


def scale_pct(value: float | None) -> float | None:
    return value * 100.0 if value is not None else None


def round_float(value: float | None, digits: int = 8) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def now_ms() -> int:
    return int(time.time() * 1000)


def ms_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


__all__ = [
    "BYBIT_API_BASE",
    "DEFAULT_OUTPUT_DIR",
    "BybitCollectorConfig",
    "BybitInstrument",
    "BybitPumpShortCollector",
    "CollectionStats",
    "build_symbol_summary",
    "classify_candidate",
    "is_crypto_pump_short_instrument",
    "normalize_symbol",
]
