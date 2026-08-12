"""ArbitrageScanner discovery adapter for the Strategy Lab Observatory."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Iterable
from urllib.request import Request, urlopen

from .external_contract import (
    ExternalLeg,
    ExternalObservation,
    TARGET_EXCHANGES,
    canonical_symbol,
    utc_now_iso,
)


ARBITRAGESCANNER_URL = "https://screener.arbitragescanner.io/api/funding-table?fid=arbitragescanner"
EXCHANGE_ALIASES = {
    "binance_futures": "binance",
    "bybit_futures": "bybit",
    "okex_futures": "okx",
    "kucoin_futures": "kucoin",
    "gate_futures": "gate",
}


def _rate_to_decimal(value: object) -> float:
    # The public table represents values in percentage points: 0.01 => 0.01%.
    return float(value) / 100.0


def parse_arbitragescanner_payload(
    payload: object,
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, list):
        raise ValueError("ArbitrageScanner response must be a list")
    timestamp = observed_at or utc_now_iso()
    observations: list[ExternalObservation] = []
    quarantined: list[dict[str, Any]] = []

    for index, row in enumerate(payload):
        if not isinstance(row, dict):
            quarantined.append({"row": index, "reason": "row_not_object"})
            continue
        display_symbol = canonical_symbol(row.get("ticker") or row.get("symbol"))
        token_id = str(row.get("tokenId") or "").strip()
        source_asset_id = f"arbitragescanner:{token_id or row.get('ticker') or row.get('symbol') or index}"
        legs_by_exchange: dict[str, ExternalLeg] = {}
        mapping_notes: list[str] = []
        for raw_leg in row.get("rates") or []:
            if not isinstance(raw_leg, dict):
                continue
            source_exchange = str(raw_leg.get("exchange") or "").strip().lower()
            exchange = EXCHANGE_ALIASES.get(source_exchange)
            if exchange not in TARGET_EXCHANGES:
                continue
            try:
                funding_rate = _rate_to_decimal(raw_leg.get("rate"))
            except (TypeError, ValueError):
                mapping_notes.append(f"invalid_rate:{source_exchange}")
                continue
            exchange_symbol = str(
                raw_leg.get("symbol") or raw_leg.get("ticker") or row.get("symbol") or row.get("ticker") or ""
            ).strip().upper()
            leg = ExternalLeg(
                exchange=exchange,
                exchange_symbol=exchange_symbol,
                funding_rate=funding_rate,
                next_funding_time=(
                    str(raw_leg.get("nextFundingTime")) if raw_leg.get("nextFundingTime") is not None else None
                ),
                source_exchange=source_exchange,
            )
            previous = legs_by_exchange.get(exchange)
            if previous and previous.exchange_symbol != leg.exchange_symbol:
                mapping_notes.append(f"ambiguous_exchange_symbol:{exchange}")
                continue
            legs_by_exchange[exchange] = leg

        legs = list(legs_by_exchange.values())
        if len(legs) < 2 or not display_symbol:
            quarantined.append(
                {
                    "source_asset_id": source_asset_id,
                    "canonical_symbol": display_symbol,
                    "reason": "fewer_than_two_target_legs" if len(legs) < 2 else "missing_symbol",
                }
            )
            continue
        long_leg = min(legs, key=lambda item: item.funding_rate if item.funding_rate is not None else float("inf"))
        short_leg = max(legs, key=lambda item: item.funding_rate if item.funding_rate is not None else float("-inf"))
        dispersion = float(short_leg.funding_rate or 0.0) - float(long_leg.funding_rate or 0.0)
        mapping_status = "ambiguous" if any(note.startswith("ambiguous_") for note in mapping_notes) else "resolved"
        observation = ExternalObservation(
            source="arbitragescanner",
            source_asset_id=source_asset_id,
            canonical_symbol=display_symbol,
            observed_at=timestamp,
            legs=sorted(legs, key=lambda item: TARGET_EXCHANGES.index(item.exchange)),
            long_exchange=long_leg.exchange,
            short_exchange=short_leg.exchange,
            funding_dispersion=dispersion,
            mapping_status=mapping_status,
            mapping_notes=mapping_notes,
            raw_identity={
                "token_id": token_id or None,
                "symbol": row.get("symbol"),
                "ticker": row.get("ticker"),
                "provider_max_spread": row.get("maxSpread"),
            },
        )
        if mapping_status == "resolved":
            observations.append(observation)
        else:
            quarantined.append(observation.as_dict())

    observations.sort(key=lambda item: item.funding_dispersion or 0.0, reverse=True)
    return {
        "source": "arbitragescanner",
        "observed_at": timestamp,
        "raw_count": len(payload),
        "eligible_count": len(observations),
        "observations": observations,
        "quarantined": quarantined,
        "selected_exchanges": list(TARGET_EXCHANGES),
    }


def _fetch_json(url: str, timeout: int) -> object:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Referer": "https://screener.arbitragescanner.io/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        },
    )
    with urlopen(request, timeout=timeout) as response:  # nosec B310 - fixed public endpoint
        content_type = str(response.headers.get("Content-Type") or "").lower()
        raw = response.read()
    if "json" not in content_type and raw.lstrip().startswith(b"<"):
        raise ValueError("ArbitrageScanner returned HTML instead of JSON")
    return json.loads(raw.decode("utf-8"))


async def fetch_arbitragescanner(
    *,
    timeout: int = 20,
    url: str = ARBITRAGESCANNER_URL,
) -> dict[str, Any]:
    payload = await asyncio.to_thread(_fetch_json, url, timeout)
    return parse_arbitragescanner_payload(payload)
