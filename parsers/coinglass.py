from __future__ import annotations

import os
import asyncio
import os
from dataclasses import dataclass
from typing import List, Sequence

os.environ.setdefault("PYPPETEER_NO_SIGNAL_HANDLERS", "1")

from bs4 import BeautifulSoup
from requests_html import HTMLSession

try:
    from pyppeteer import launcher as _pyppeteer_launcher
except ImportError:  # pragma: no cover - optional dependency already handled above
    _pyppeteer_launcher = None
else:
    if not getattr(_pyppeteer_launcher.launch, "_fee_arb_patched", False):
        _original_launch = _pyppeteer_launcher.launch

        async def _patched_launch(options: dict | None = None, **kwargs):
            params = dict(options or {})
            params.setdefault("handleSIGINT", False)
            params.setdefault("handleSIGTERM", False)
            params.setdefault("handleSIGHUP", False)
            return await _original_launch(params, **kwargs)

        _patched_launch._fee_arb_patched = True  # type: ignore[attr-defined]
        _pyppeteer_launcher.launch = _patched_launch

    try:  # Ensure top-level pyppeteer.launch also respects the signal settings
        import pyppeteer
    except ImportError:  # pragma: no cover - optional dependency
        pyppeteer = None  # type: ignore[assignment]
    else:
        if not getattr(pyppeteer.launch, "_fee_arb_patched", False):
            _original_launch_fn = pyppeteer.launch

            async def _patched_top_level_launch(options: dict | None = None, **kwargs):
                params = dict(options or {})
                params.setdefault("handleSIGINT", False)
                params.setdefault("handleSIGTERM", False)
                params.setdefault("handleSIGHUP", False)
                return await _original_launch_fn(params, **kwargs)

            _patched_top_level_launch._fee_arb_patched = True  # type: ignore[attr-defined]
            pyppeteer.launch = _patched_top_level_launch

COINGLASS_URL = "https://www.coinglass.com/FrArbitrage"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
# Newer pyppeteer releases require specifying a valid Chromium revision.
TARGET_CHROMIUM_REVISION = "1263111"
os.environ.setdefault("PYPPETEER_CHROMIUM_REVISION", TARGET_CHROMIUM_REVISION)

try:
    from pyppeteer import chromium_downloader

    if chromium_downloader.REVISION != TARGET_CHROMIUM_REVISION:
        chromium_downloader.REVISION = TARGET_CHROMIUM_REVISION
        archive = chromium_downloader.windowsArchive
        chromium_downloader.downloadURLs.update(
            {
                "linux": f"{chromium_downloader.BASE_URL}/Linux_x64/{TARGET_CHROMIUM_REVISION}/chrome-linux.zip",
                "mac": f"{chromium_downloader.BASE_URL}/Mac/{TARGET_CHROMIUM_REVISION}/chrome-mac.zip",
                "win32": f"{chromium_downloader.BASE_URL}/Win/{TARGET_CHROMIUM_REVISION}/{archive}.zip",
                "win64": f"{chromium_downloader.BASE_URL}/Win_x64/{TARGET_CHROMIUM_REVISION}/{archive}.zip",
            }
        )
        chromium_downloader.chromiumExecutable.update(
            {
                "linux": chromium_downloader.DOWNLOADS_FOLDER
                / TARGET_CHROMIUM_REVISION
                / "chrome-linux"
                / "chrome",
                "mac": chromium_downloader.DOWNLOADS_FOLDER
                / TARGET_CHROMIUM_REVISION
                / "chrome-mac"
                / "Chromium.app"
                / "Contents"
                / "MacOS"
                / "Chromium",
                "win32": chromium_downloader.DOWNLOADS_FOLDER
                / TARGET_CHROMIUM_REVISION
                / archive
                / "chrome.exe",
                "win64": chromium_downloader.DOWNLOADS_FOLDER
                / TARGET_CHROMIUM_REVISION
                / archive
                / "chrome.exe",
            }
        )
except ImportError:
    chromium_downloader = None


@dataclass
class CoinglassRow:
    ranking: int
    symbol: str
    pair: str
    long_exchange: str
    short_exchange: str
    apr: float  # decimal value (e.g. 10.1309 -> 1013.09%)
    net_funding_rate: float  # decimal value (0.004626 -> 0.4626%)
    spread_rate: float  # decimal value (0.0019 -> 0.19%)
    open_interest: List[str]
    settlement: str
    trade_links: List[str]

    def to_dict(self) -> dict[str, object]:
        long_oi = self.open_interest[0] if len(self.open_interest) > 0 else ""
        short_oi = self.open_interest[1] if len(self.open_interest) > 1 else ""
        return {
            "ranking": self.ranking,
            "symbol": self.symbol,
            "pair": self.pair,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange,
            "apr_percent": self.apr * 100.0,
            "net_funding_rate_percent": self.net_funding_rate * 100.0,
            "spread_rate_percent": self.spread_rate * 100.0,
            "open_interest_long": long_oi,
            "open_interest_short": short_oi,
            "settlement": self.settlement,
            "trade_links": ";".join(self.trade_links),
        }


def fetch_rows(limit: int = 20) -> List[CoinglassRow]:
    """Render the Coinglass arbitrage table and parse the first `limit` rows."""

    session = HTMLSession()
    response = session.get(COINGLASS_URL, headers=DEFAULT_HEADERS)
    _ensure_event_loop()
    response.html.render(timeout=120, sleep=3, scrolldown=3)
    soup = BeautifulSoup(response.html.html, "html.parser")
    session.close()

    rows: List[CoinglassRow] = []
    for tr in soup.select("table tbody tr"):
        entry = _parse_row(tr)
        if entry:
            rows.append(entry)
            if len(rows) >= limit:
                break
    return rows


def format_table(rows: Sequence[CoinglassRow]) -> str:
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


def _parse_row(tr) -> CoinglassRow | None:
    cells = tr.find_all("td")
    if len(cells) < 9:
        return None

    ranking_text = cells[0].get_text(strip=True)
    if not ranking_text.isdigit():
        return None

    ranking = int(ranking_text)
    symbol = ""
    symbol_block = cells[1].select_one(".symbol-name")
    if symbol_block:
        symbol = symbol_block.get_text(strip=True)

    pair = ""
    long_exchange = ""
    short_exchange = ""

    for stack in cells[2].select(".MuiStack-root"):
        side_el = stack.select_one(".rise-color, .fall-color")
        side = side_el.get_text(strip=True).lower() if side_el else ""
        pair_el = stack.select_one(".cg-style-35ezg3")
        exchange_el = stack.select_one(".cg-style-p1hdku")
        pair_text = pair_el.get_text(strip=True) if pair_el else ""
        exchange_text = exchange_el.get_text(strip=True) if exchange_el else ""

        if "long" in side:
            long_exchange = exchange_text or long_exchange
            pair = pair_text or pair
        elif "short" in side:
            short_exchange = exchange_text or short_exchange
            pair = pair or pair_text

    apr = _parse_percent(cells[3].get_text(strip=True))
    net_funding_rate = _parse_percent(cells[4].get_text(strip=True))
    spread_rate = _parse_percent(cells[5].get_text(strip=True))

    oi_values = [
        item.get_text(strip=True)
        for item in cells[6].select(".Number")
        if item and item.get_text(strip=True)
    ]
    if not oi_values:
        text = cells[6].get_text(separator=" ", strip=True)
        if text:
            oi_values = [value for value in text.split() if value]

    settlement = cells[7].get_text(strip=True)
    trade_links = [
        anchor["href"] for anchor in cells[8].select("a[href]") if anchor["href"]
    ]

    return CoinglassRow(
        ranking=ranking,
        symbol=symbol or pair.split("/")[0] if pair else "",
        pair=pair or f"{symbol}/USDT",
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        apr=apr,
        net_funding_rate=net_funding_rate,
        spread_rate=spread_rate,
        open_interest=oi_values,
        settlement=settlement,
        trade_links=trade_links,
    )


def _parse_percent(value: str) -> float:
    value = (value or "").replace("%", "").replace(",", "").strip()
    if not value:
        return 0.0
    try:
        return float(value) / 100.0
    except ValueError:
        return 0.0
def _ensure_event_loop() -> None:
    """Guarantee an asyncio event loop exists for pyppeteer rendering."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
