"""Low-frequency Coinglass checkbox adapter for research candidate discovery."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Iterable

from .external_contract import (
    ExternalLeg,
    ExternalObservation,
    TARGET_EXCHANGES,
    canonical_symbol,
    utc_now_iso,
)


COINGLASS_URL = "https://www.coinglass.com/FrArbitrage"
DISPLAY_TO_EXCHANGE = {
    "Binance": "binance",
    "Bybit": "bybit",
    "OKX": "okx",
    "KuCoin": "kucoin",
    "Gate": "gate",
}
KNOWN_EXCHANGE_LABELS = (
    "Binance", "Bybit", "OKX", "Crypto.com", "CoinEx", "CME", "Deribit",
    "Bitfinex", "dYdX", "Bitmex", "HTX", "Kraken", "Bitget", "BingX",
    "Gate", "Bitunix", "MEXC", "WhiteBIT", "KuCoin",
)


def _percent_decimal(value: object) -> float | None:
    text = str(value or "").replace(",", "").strip()
    match = re.search(r"([-+]?\d+(?:\.\d+)?)\s*%", text)
    if not match:
        return None
    return float(match.group(1)) / 100.0


def _portfolio_parts(text: str) -> tuple[str, str, str] | None:
    compact = " ".join(str(text or "").split())
    pattern = re.compile(
        r"Long\s+([A-Za-z0-9._-]+)\s*/\s*USDT\s+([A-Za-z0-9.]+).*?"
        r"Short\s+([A-Za-z0-9._-]+)\s*/\s*USDT\s+([A-Za-z0-9.]+)",
        re.IGNORECASE,
    )
    match = pattern.search(compact)
    if not match:
        return None
    long_symbol, long_exchange, short_symbol, short_exchange = match.groups()
    symbol = canonical_symbol(long_symbol)
    if symbol != canonical_symbol(short_symbol):
        return None
    return symbol, long_exchange, short_exchange


def parse_coinglass_dom_rows(
    rows: Iterable[dict[str, Any]],
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    timestamp = observed_at or utc_now_iso()
    observations: list[ExternalObservation] = []
    quarantined: list[dict[str, Any]] = []
    for index, raw in enumerate(rows):
        cells = [str(value or "") for value in (raw.get("cells") or [])]
        portfolio_index = next((i for i, value in enumerate(cells) if "Long" in value and "Short" in value), None)
        if portfolio_index is None:
            quarantined.append({"row": index, "reason": "portfolio_cell_missing"})
            continue
        parts = _portfolio_parts(cells[portfolio_index])
        if parts is None:
            quarantined.append({"row": index, "reason": "portfolio_parse_failed"})
            continue
        symbol, long_display, short_display = parts
        long_exchange = DISPLAY_TO_EXCHANGE.get(long_display)
        short_exchange = DISPLAY_TO_EXCHANGE.get(short_display)
        if long_exchange not in TARGET_EXCHANGES or short_exchange not in TARGET_EXCHANGES:
            quarantined.append(
                {
                    "row": index,
                    "canonical_symbol": symbol,
                    "reason": "exchange_outside_exact_five",
                    "long_exchange": long_display,
                    "short_exchange": short_display,
                }
            )
            continue
        after = cells[portfolio_index + 1 :]
        percentages = [value for value in after if _percent_decimal(value) is not None]
        apr = _percent_decimal(percentages[0]) if len(percentages) > 0 else None
        net = _percent_decimal(percentages[1]) if len(percentages) > 1 else None
        spread = _percent_decimal(percentages[2]) if len(percentages) > 2 else None
        rank = None
        for value in cells[: portfolio_index + 1]:
            match = re.fullmatch(r"\s*(\d+)\s*", value)
            if match:
                rank = int(match.group(1))
                break
        pair = f"{symbol}/USDT"
        observations.append(
            ExternalObservation(
                source="coinglass",
                source_asset_id=f"coinglass:{symbol}",
                canonical_symbol=symbol,
                observed_at=timestamp,
                legs=[
                    ExternalLeg(long_exchange, pair, source_exchange=long_display),
                    ExternalLeg(short_exchange, pair, source_exchange=short_display),
                ],
                long_exchange=long_exchange,
                short_exchange=short_exchange,
                funding_dispersion=net,
                source_rank=rank or index + 1,
                source_spread_rate=spread,
                source_net_funding_rate=net,
                source_apr=apr,
                raw_identity={"links": list(raw.get("links") or [])},
            )
        )
    if observations and any(
        leg.exchange not in TARGET_EXCHANGES for item in observations for leg in item.legs
    ):
        raise ValueError("Coinglass exact-five postcondition failed")
    return {
        "source": "coinglass",
        "observed_at": timestamp,
        "raw_count": len(list(rows)) if isinstance(rows, list) else len(observations) + len(quarantined),
        "eligible_count": len(observations),
        "observations": observations,
        "quarantined": quarantined,
        "selected_exchanges": list(TARGET_EXCHANGES),
    }


def _browser_executable() -> str | None:
    configured = os.getenv("COINGLASS_BROWSER_PATH", "").strip()
    candidates = [
        configured,
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]
    return next((candidate for candidate in candidates if candidate and Path(candidate).exists()), None)


async def _click_visible_text(page: Any, label: str, *, prefer_button: bool = False) -> bool:
    box = await page.evaluate(
        r"""(label, preferButton) => {
          const clean = value => String(value || '').replace(/\s+/g, ' ').trim();
          let source = Array.from(document.querySelectorAll('body *'));
          if (preferButton) source = source.filter(node => node.tagName === 'BUTTON');
          const nodes = source
            .filter(node => clean(node.innerText) === label)
            .map(node => {
              const rect = node.getBoundingClientRect();
              const style = window.getComputedStyle(node);
              return {node, x: rect.x, y: rect.y, width: rect.width, height: rect.height,
                visible: rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none'};
            })
            .filter(item => item.visible)
            .sort((a, b) => (a.width * a.height) - (b.width * b.height));
          if (!nodes.length) return null;
          nodes[0].node.scrollIntoView({block: 'center', inline: 'center'});
          const rect = nodes[0].node.getBoundingClientRect();
          return {x: rect.x, y: rect.y, width: rect.width, height: rect.height};
        }""",
        label,
        prefer_button,
    )
    if not box:
        return False
    await page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    return True


async def _exchange_checkbox_states(page: Any) -> dict[str, bool]:
    return await page.evaluate(
        r"""(known) => {
          const clean = value => String(value || '').replace(/\s+/g, ' ').trim();
          const result = {};
          for (const label of known) {
            const labels = Array.from(document.querySelectorAll('body *'))
              .filter(node => clean(node.innerText) === label);
            for (const labelNode of labels) {
              let node = labelNode;
              for (let depth = 0; depth < 12 && node; depth += 1, node = node.parentElement) {
                const inputs = node.querySelectorAll('input[type="checkbox"]');
                if (inputs.length === 1) {
                  result[label] = Boolean(inputs[0].checked);
                  break;
                }
              }
              if (Object.prototype.hasOwnProperty.call(result, label)) break;
            }
          }
          return result;
        }""",
        list(KNOWN_EXCHANGE_LABELS),
    )


async def _click_exchange_checkbox(page: Any, label: str) -> bool:
    box = await page.evaluate(
        r"""(label) => {
          const clean = value => String(value || '').replace(/\s+/g, ' ').trim();
          const labels = Array.from(document.querySelectorAll('body *'))
            .filter(node => clean(node.innerText) === label);
          for (const labelNode of labels) {
            let node = labelNode;
            for (let depth = 0; depth < 12 && node; depth += 1, node = node.parentElement) {
              const inputs = node.querySelectorAll('input[type="checkbox"]');
              if (inputs.length === 1) {
                labelNode.scrollIntoView({block: 'center', inline: 'center'});
                const rect = labelNode.getBoundingClientRect();
                const style = window.getComputedStyle(labelNode);
                if (rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden') {
                  return {x: rect.x, y: rect.y, width: rect.width, height: rect.height};
                }
              }
            }
          }
          return null;
        }""",
        label,
    )
    if not box:
        return False
    await page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    return True


async def _read_table_rows(page: Any, limit: int) -> list[dict[str, Any]]:
    return await page.evaluate(
        """(limit) => Array.from(document.querySelectorAll('table tbody tr'))
          .filter(tr => {
            const text = tr.innerText || '';
            return tr.querySelectorAll('td').length > 0 && text.indexOf('Long') >= 0 && text.indexOf('Short') >= 0;
          })
          .slice(0, limit)
          .map(tr => ({
            cells: Array.from(tr.querySelectorAll('td')).map(td => (td.innerText || '').trim()),
            links: Array.from(tr.querySelectorAll('a[href]')).map(a => a.href)
          }))""",
        int(limit),
    )


async def fetch_coinglass(*, limit: int = 20, timeout_ms: int = 45_000) -> dict[str, Any]:
    try:
        from pyppeteer import launch
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("pyppeteer is required for Coinglass checkbox intake") from exc

    executable = _browser_executable()
    profile = tempfile.mkdtemp(prefix="feearb-coinglass-")
    try:
        browser = await launch(
            headless=True,
            executablePath=executable,
            autoClose=False,
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                f"--user-data-dir={profile}",
            ],
        )
        try:
            page = await browser.newPage()
            await page.setViewport({"width": 1440, "height": 1000})
            await page.setUserAgent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            response = await page.goto(COINGLASS_URL, {"waitUntil": "domcontentloaded", "timeout": timeout_ms})
            if response is None or response.status >= 400:
                raise RuntimeError(f"Coinglass page unavailable: HTTP {response.status if response else 'none'}")
            await page.waitForFunction(
                "() => document.body && document.body.innerText.indexOf('Exchanges') >= 0",
                {"timeout": timeout_ms},
            )
            if not await _click_visible_text(page, "Exchanges", prefer_button=True):
                raise RuntimeError("Coinglass exchange filter was not found")
            await asyncio.sleep(0.5)
            desired_display = set(DISPLAY_TO_EXCHANGE)
            states = await _exchange_checkbox_states(page)
            if not desired_display.issubset(states):
                raise RuntimeError(
                    "Coinglass target checkboxes were not all discovered: "
                    + ",".join(sorted(states))
                )
            for label in KNOWN_EXCHANGE_LABELS:
                if label not in states:
                    continue
                should_be_checked = label in desired_display
                if bool(states[label]) != should_be_checked:
                    if not await _click_exchange_checkbox(page, label):
                        raise RuntimeError(f"Coinglass checkbox is not clickable: {label}")
                    await asyncio.sleep(0.12)
            await asyncio.sleep(1.0)
            final_states = await _exchange_checkbox_states(page)
            verified = {label for label, checked in final_states.items() if checked}
            if verified != desired_display:
                raise RuntimeError(
                    "Coinglass exact-five verification failed: " + ",".join(sorted(verified))
                )
            await page.waitForFunction(
                "() => document.querySelectorAll('table tbody tr').length > 0",
                {"timeout": timeout_ms},
            )
            stable_signature = None
            stable_reads = 0
            dom_rows: list[dict[str, Any]] = []
            for _ in range(30):
                candidate_rows = await _read_table_rows(page, limit)
                candidate_parsed = parse_coinglass_dom_rows(candidate_rows)
                outside = [
                    item
                    for item in candidate_parsed["quarantined"]
                    if item.get("reason") == "exchange_outside_exact_five"
                ]
                signature = tuple(
                    (
                        item.canonical_symbol,
                        item.long_exchange,
                        item.short_exchange,
                    )
                    for item in candidate_parsed["observations"]
                )
                if signature and not outside:
                    stable_reads = stable_reads + 1 if signature == stable_signature else 1
                    stable_signature = signature
                    dom_rows = candidate_rows
                    if stable_reads >= 2:
                        break
                else:
                    stable_reads = 0
                    stable_signature = None
                await asyncio.sleep(0.5)
            if stable_reads < 2:
                raise RuntimeError("Coinglass table did not stabilize on the exact-five filter")
        finally:
            await browser.close()
            await asyncio.sleep(0.5)
    finally:
        shutil.rmtree(profile, ignore_errors=True)
    parsed = parse_coinglass_dom_rows(dom_rows)
    if not parsed["observations"]:
        raise RuntimeError("Coinglass returned no valid exact-five rows")
    return parsed
