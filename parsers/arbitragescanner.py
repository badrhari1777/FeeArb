from __future__ import annotations

import json
from typing import Iterable
from urllib.request import Request, urlopen


URL = "https://screener.arbitragescanner.io/api/funding-table?fid=arbitragescanner"
QUOTE_SUFFIXES = ("USDT",)


def normalize_symbol(value: object) -> str:
    """Return a normalized base symbol without common quote suffixes."""
    symbol = str(value or "").strip().upper()
    for suffix in QUOTE_SUFFIXES:
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)]
            break
    return symbol


def fetch_json(url: str = URL, timeout: int = 20) -> list[dict]:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json",
            "Referer": "https://screener.arbitragescanner.io/",
        },
    )
    with urlopen(req, timeout=timeout) as resp:  # nosec - trusted public API
        raw = resp.read()
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, list):
        raise ValueError("Expected top-level list in ArbitrageScanner response")
    return data


def build_top(
    rows: Iterable[dict],
    *,
    exclude: tuple[str, ...] = ("binance",),
    limit: int = 10,
) -> list[dict]:
    excludes = tuple(s.lower() for s in (exclude or ()))
    results: list[dict] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = normalize_symbol(row.get("symbol"))
        rates = row.get("rates") or []
        filtered = [
            item
            for item in rates
            if isinstance(item, dict)
            and "exchange" in item
            and "rate" in item
            and not any(ex in str(item["exchange"]).lower() for ex in excludes)
        ]
        if len(filtered) < 2 or not symbol:
            continue

        best_long = max(filtered, key=lambda x: float(x["rate"]))
        best_short = min(filtered, key=lambda x: float(x["rate"]))
        spread = float(best_long["rate"]) - float(best_short["rate"])

        results.append(
            {
                "symbol": symbol,
                "spread": spread,
                "long_exchange": str(best_long["exchange"]),
                "long_fee": float(best_long["rate"]),
                "short_exchange": str(best_short["exchange"]),
                "short_fee": float(best_short["rate"]),
            }
        )

    results.sort(key=lambda x: x["spread"], reverse=True)
    return results[:limit]


def format_table(items: list[dict]) -> str:
    header = f"{'Symbol':<10} {'Spread':>10} {'Long':>18} {'LongFee':>10} {'Short':>18} {'ShortFee':>10}"
    lines = [header, "-" * len(header)]
    for item in items:
        lines.append(
            f"{item['symbol']:<10} {item['spread']:>10.6f} "
            f"{item['long_exchange']:>18} {item['long_fee']:>10.6f} "
            f"{item['short_exchange']:>18} {item['short_fee']:>10.6f}"
        )
    return "\n".join(lines)
