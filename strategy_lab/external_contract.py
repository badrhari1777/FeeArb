"""Versioned contract shared by Strategy Lab external candidate sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, Iterable


CONTRACT_VERSION = "strategy_lab_external_candidate_v1"
TARGET_EXCHANGES = ("binance", "bybit", "okx", "kucoin", "gate")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_symbol(value: object) -> str:
    """Normalize only presentation syntax; never collapse token multipliers."""

    text = str(value or "").strip().upper()
    text = re.sub(r"\s+", "", text)
    for suffix in ("-USDT-SWAP", "_USDT", "/USDT", "USDTM", "USDT"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
            break
    return text


@dataclass(slots=True)
class ExternalLeg:
    exchange: str
    exchange_symbol: str
    funding_rate: float | None = None
    next_funding_time: str | None = None
    source_exchange: str | None = None


@dataclass(slots=True)
class ExternalObservation:
    source: str
    source_asset_id: str
    canonical_symbol: str
    observed_at: str
    legs: list[ExternalLeg]
    long_exchange: str
    short_exchange: str
    funding_dispersion: float | None
    source_rank: int | None = None
    source_spread_rate: float | None = None
    source_net_funding_rate: float | None = None
    source_apr: float | None = None
    mapping_status: str = "resolved"
    mapping_notes: list[str] = field(default_factory=list)
    raw_identity: dict[str, Any] = field(default_factory=dict)
    contract_version: str = CONTRACT_VERSION

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_key(observation: ExternalObservation) -> str:
    return observation.canonical_symbol


def merge_external_candidates(
    coinglass: Iterable[ExternalObservation],
    arbitragescanner: Iterable[ExternalObservation],
    *,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Build a bounded discovery list; ranking is monitoring priority, not alpha."""

    groups: dict[str, dict[str, list[ExternalObservation]]] = {}
    for item in list(coinglass) + list(arbitragescanner):
        if not item.canonical_symbol or item.mapping_status != "resolved":
            continue
        groups.setdefault(item.canonical_symbol, {}).setdefault(item.source, []).append(item)

    rows: list[dict[str, Any]] = []
    for symbol, by_source in groups.items():
        cg = sorted(by_source.get("coinglass", []), key=lambda item: item.source_rank or 10**9)
        arb = sorted(
            by_source.get("arbitragescanner", []),
            key=lambda item: item.funding_dispersion or 0.0,
            reverse=True,
        )
        source_asset_ids = sorted(
            {item.source_asset_id for items in by_source.values() for item in items}
        )
        ambiguous = len({item.source_asset_id for item in arb}) > 1
        if ambiguous:
            # Same display symbol with different provider asset IDs must not be
            # silently collapsed into one instrument.
            continue
        best_cg = cg[0] if cg else None
        best_arb = arb[0] if arb else None
        overlap = bool(best_cg and best_arb)
        rows.append(
            {
                "canonical_symbol": symbol,
                "source_tags": sorted(by_source),
                "source_overlap": overlap,
                "coinglass_rank": best_cg.source_rank if best_cg else None,
                "funding_dispersion": best_arb.funding_dispersion if best_arb else None,
                "long_exchange": (
                    best_cg.long_exchange if best_cg else best_arb.long_exchange if best_arb else None
                ),
                "short_exchange": (
                    best_cg.short_exchange if best_cg else best_arb.short_exchange if best_arb else None
                ),
                "source_asset_ids": source_asset_ids,
                "monitoring_priority": "P1" if overlap or best_cg else "P2",
                "trade_signal": False,
                "research_only": True,
            }
        )

    rows.sort(
        key=lambda row: (
            0 if row["source_overlap"] else 1,
            row["coinglass_rank"] if row["coinglass_rank"] is not None else 10**9,
            -(row["funding_dispersion"] or 0.0),
            row["canonical_symbol"],
        )
    )
    return rows[: max(0, int(limit))]
