"""Bounded, research-only external candidate observatory."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable
import uuid

from config import BASE_DIR

from .arbitragescanner_source import fetch_arbitragescanner
from .coinglass_source import fetch_coinglass
from .external_contract import CONTRACT_VERSION, ExternalObservation, TARGET_EXCHANGES, merge_external_candidates
from .instrument_registry import fetch_instrument_registry, verify_external_candidates
from .public_feed import run_bounded_public_feed


Fetcher = Callable[[], Awaitable[dict[str, Any]]]
RegistryFetcher = Callable[[Iterable[ExternalObservation]], Awaitable[dict[str, Any]]]
FeedRunner = Callable[..., Awaitable[dict[str, Any]]]


class StrategyLabObservatory:
    """One-shot source intake. It has no scheduler and no trading dependency."""

    def __init__(self, *, state_dir: Path | None = None, candidate_limit: int = 30) -> None:
        self.state_dir = state_dir or BASE_DIR / "data" / "research" / "strategy_lab_observatory" / "runtime"
        self.snapshot_path = self.state_dir / "latest.json"
        self.candidate_limit = int(candidate_limit)
        self._lock = asyncio.Lock()
        self._running = False
        self._running_operation: str | None = None
        self._payload = self._empty_payload()
        self._load()

    @staticmethod
    def _empty_source() -> dict[str, Any]:
        return {
            "status": "never_run",
            "last_attempt_at": None,
            "last_success_at": None,
            "error": None,
            "raw_count": 0,
            "eligible_count": 0,
            "quarantined_count": 0,
            "last_good_used": False,
            "observations": [],
        }

    def _empty_payload(self) -> dict[str, Any]:
        return {
            "contract_version": CONTRACT_VERSION,
            "mode": "research_only_no_trading",
            "scheduler_enabled": False,
            "running": False,
            "updated_at": None,
            "selected_exchanges": list(TARGET_EXCHANGES),
            "candidate_limit": self.candidate_limit,
            "sources": {
                "coinglass": self._empty_source(),
                "arbitragescanner": self._empty_source(),
            },
            "candidates": [],
            "candidate_count": 0,
            "registry": {
                "status": "never_run",
                "last_attempt_at": None,
                "last_success_at": None,
                "error": None,
                "last_good_used": False,
                "snapshot": None,
                "verification": [],
                "eligible_candidate_count": 0,
            },
            "feed_probe": {
                "status": "never_run",
                "last_attempt_at": None,
                "last_success_at": None,
                "error": None,
                "last_good_used": False,
                "quality": None,
                "report": None,
            },
        }

    def _load(self) -> None:
        if not self.snapshot_path.exists():
            return
        try:
            payload = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if isinstance(payload, dict) and payload.get("contract_version") == CONTRACT_VERSION:
            payload["running"] = False
            defaults = self._empty_payload()
            payload.setdefault("registry", defaults["registry"])
            payload.setdefault("feed_probe", defaults["feed_probe"])
            self._payload = payload

    def _write(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        temp_path = self.state_dir / f"latest.{uuid.uuid4().hex}.tmp"
        temp_path.write_text(json.dumps(self._payload, indent=2, ensure_ascii=False), encoding="utf-8")
        temp_path.replace(self.snapshot_path)

    def status(self) -> dict[str, Any]:
        payload = json.loads(json.dumps(self._payload))
        payload["running"] = self._running
        payload["running_operation"] = self._running_operation
        return payload

    def _candidate_observations(self) -> list[ExternalObservation]:
        ranks = {
            str(row.get("canonical_symbol") or ""): index
            for index, row in enumerate(self._payload.get("candidates") or [])
        }
        items: list[ExternalObservation] = []
        for source in ("coinglass", "arbitragescanner"):
            items.extend(self._deserialize_observations(
                self._payload["sources"][source].get("observations") or []
            ))
        return sorted(
            (item for item in items if item.canonical_symbol in ranks),
            key=lambda item: (ranks[item.canonical_symbol], item.source),
        )

    @staticmethod
    def _compact_registry(registry: dict[str, Any], symbols: Iterable[str]) -> dict[str, Any]:
        selected = {str(symbol or "").upper() for symbol in symbols}
        vectors = registry.get("vectors") or {}
        ambiguous = registry.get("ambiguous") or {}
        return {
            "registry_version": registry.get("registry_version"),
            "created_at": registry.get("created_at"),
            "source_status": registry.get("source_status") or {},
            "contract_count": registry.get("contract_count") or {},
            "vectors": {symbol: vectors[symbol] for symbol in selected if symbol in vectors},
            "ambiguous": {symbol: ambiguous[symbol] for symbol in selected if symbol in ambiguous},
            "research_only": True,
            "trade_signal": False,
        }

    @staticmethod
    def _feed_quality(report: dict[str, Any]) -> dict[str, Any]:
        reasons: list[str] = []
        if int(report.get("observation_count") or 0) < 2:
            reasons.append("fewer_than_two_observations")
        if int(report.get("symbols_with_two_venues") or 0) < 1:
            reasons.append("no_symbol_observed_on_two_venues")
        if report.get("invalid_bbo"):
            reasons.append("invalid_bbo")
        venue_status = report.get("venue_status") or {}
        if any(int(row.get("subscription_errors") or 0) > 0 for row in venue_status.values()):
            reasons.append("subscription_errors")
        observed_venues = sum(
            int(row.get("observed") or 0) > 0
            for row in (report.get("venue_coverage") or {}).values()
        )
        if observed_venues < 2:
            reasons.append("fewer_than_two_observed_venues")
        return {
            "ready_for_bounded_research": not reasons,
            "reasons": reasons,
            "observed_venues": observed_venues,
            "pair_coverage_pct": report.get("pair_coverage_pct"),
            "research_only": True,
            "trade_signal": False,
        }

    def _mark_downstream_stale(self) -> None:
        registry = dict(self._payload.get("registry") or {})
        if registry.get("snapshot"):
            registry.update({
                "status": "stale",
                "last_good_used": True,
                "error": "external_candidates_refreshed_refresh_registry",
            })
            self._payload["registry"] = registry
        feed = dict(self._payload.get("feed_probe") or {})
        if feed.get("report"):
            feed.update({
                "status": "stale",
                "last_good_used": True,
                "error": "external_candidates_refreshed_rerun_registry_and_feed",
            })
            self._payload["feed_probe"] = feed

    @staticmethod
    def _serialize_observations(items: list[ExternalObservation]) -> list[dict[str, Any]]:
        return [item.as_dict() for item in items]

    @staticmethod
    def _deserialize_observations(items: list[dict[str, Any]]) -> list[ExternalObservation]:
        from .external_contract import ExternalLeg

        result: list[ExternalObservation] = []
        for item in items:
            values = dict(item)
            values["legs"] = [ExternalLeg(**leg) for leg in values.get("legs") or []]
            result.append(ExternalObservation(**values))
        return result

    async def refresh(
        self,
        *,
        sources: list[str] | None = None,
        fetchers: dict[str, Fetcher] | None = None,
    ) -> dict[str, Any]:
        selected = sources or ["arbitragescanner", "coinglass"]
        unknown = sorted(set(selected) - {"arbitragescanner", "coinglass"})
        if unknown:
            raise ValueError(f"Unknown Strategy Lab sources: {', '.join(unknown)}")
        if self._lock.locked():
            payload = self.status()
            payload["refresh_result"] = "in_progress"
            return payload
        default_fetchers: dict[str, Fetcher] = {
            "arbitragescanner": fetch_arbitragescanner,
            "coinglass": fetch_coinglass,
        }
        default_fetchers.update(fetchers or {})

        async with self._lock:
            self._running = True
            self._running_operation = "external_refresh"
            any_source_succeeded = False
            try:
                for source in selected:
                    now = datetime.now(timezone.utc).isoformat()
                    previous = dict(self._payload["sources"].get(source) or self._empty_source())
                    previous["last_attempt_at"] = now
                    previous["status"] = "running"
                    previous["error"] = None
                    self._payload["sources"][source] = previous
                    try:
                        result = await default_fetchers[source]()
                        observations = list(result.get("observations") or [])
                        if not observations:
                            raise RuntimeError(f"{source} returned no eligible observations")
                    except Exception as exc:  # pylint: disable=broad-except
                        previous["status"] = "stale" if previous.get("observations") else "error"
                        previous["error"] = f"{type(exc).__name__}: {exc}"
                        previous["last_good_used"] = bool(previous.get("observations"))
                    else:
                        any_source_succeeded = True
                        previous.update(
                            {
                                "status": "fresh",
                                "last_success_at": str(result.get("observed_at") or now),
                                "error": None,
                                "raw_count": int(result.get("raw_count") or 0),
                                "eligible_count": len(observations),
                                "quarantined_count": len(result.get("quarantined") or []),
                                "last_good_used": False,
                                "observations": self._serialize_observations(observations),
                            }
                        )
                    self._payload["sources"][source] = previous

                coinglass = self._deserialize_observations(
                    self._payload["sources"]["coinglass"].get("observations") or []
                )
                arbitragescanner = self._deserialize_observations(
                    self._payload["sources"]["arbitragescanner"].get("observations") or []
                )
                candidates = merge_external_candidates(
                    coinglass,
                    arbitragescanner,
                    limit=self.candidate_limit,
                )
                self._payload["candidates"] = candidates
                self._payload["candidate_count"] = len(candidates)
                if any_source_succeeded:
                    self._mark_downstream_stale()
                self._payload["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._payload["running"] = False
                self._write()
            finally:
                self._running = False
                self._running_operation = None
        payload = self.status()
        payload["refresh_result"] = "completed"
        return payload

    async def refresh_registry(
        self,
        *,
        registry_fetcher: RegistryFetcher | None = None,
    ) -> dict[str, Any]:
        if self._lock.locked():
            payload = self.status()
            payload["registry_refresh_result"] = "in_progress"
            return payload
        observations = self._candidate_observations()
        if not observations:
            raise ValueError("Refresh external candidates before Instrument Registry")
        candidate_symbols = [
            str(row.get("canonical_symbol") or "").upper()
            for row in self._payload.get("candidates") or []
        ]
        fetcher = registry_fetcher or fetch_instrument_registry

        async with self._lock:
            self._running = True
            self._running_operation = "registry_refresh"
            previous = dict(self._payload.get("registry") or self._empty_payload()["registry"])
            now = datetime.now(timezone.utc).isoformat()
            previous.update({"status": "running", "last_attempt_at": now, "error": None})
            self._payload["registry"] = previous
            try:
                try:
                    registry = await fetcher(observations)
                    source_status = registry.get("source_status") or {}
                    required = ("binance", "bybit", "okx", "kucoin", "gate")
                    incomplete = [
                        exchange for exchange in required
                        if (source_status.get(exchange) or {}).get("status") not in {"fresh", "not_requested"}
                    ]
                    if incomplete:
                        raise RuntimeError(f"incomplete_registry_sources:{','.join(incomplete)}")
                    verification = verify_external_candidates(registry, observations)
                    eligible_symbols = {
                        row["canonical_symbol"]
                        for row in verification
                        if row.get("eligible_for_observation")
                    }
                    if not eligible_symbols:
                        raise RuntimeError("registry_verified_no_observation_candidates")
                    compact = self._compact_registry(registry, candidate_symbols)
                except Exception as exc:  # pylint: disable=broad-except
                    previous["status"] = "stale" if previous.get("snapshot") else "error"
                    previous["error"] = f"{type(exc).__name__}: {exc}"
                    previous["last_good_used"] = bool(previous.get("snapshot"))
                else:
                    previous.update({
                        "status": "fresh",
                        "last_success_at": str(compact.get("created_at") or now),
                        "error": None,
                        "last_good_used": False,
                        "snapshot": compact,
                        "verification": verification,
                        "eligible_candidate_count": len(eligible_symbols),
                    })
                self._payload["registry"] = previous
                self._payload["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._write()
            finally:
                self._running = False
                self._running_operation = None
        payload = self.status()
        payload["registry_refresh_result"] = "completed"
        return payload

    async def run_feed_probe(
        self,
        *,
        duration_sec: float = 12.0,
        max_symbols: int = 5,
        feed_runner: FeedRunner | None = None,
    ) -> dict[str, Any]:
        if self._lock.locked():
            payload = self.status()
            payload["feed_probe_result"] = "in_progress"
            return payload
        registry_state = self._payload.get("registry") or {}
        registry = registry_state.get("snapshot")
        if not isinstance(registry, dict):
            raise ValueError("Refresh Instrument Registry before the feed probe")
        if registry_state.get("status") != "fresh":
            raise ValueError("Instrument Registry is stale; refresh it before the feed probe")
        candidates = [
            str(row.get("canonical_symbol") or "").upper()
            for row in self._payload.get("candidates") or []
        ]
        vectors = registry.get("vectors") or {}
        symbols = [symbol for symbol in candidates if len(vectors.get(symbol) or {}) >= 2]
        if not symbols:
            raise ValueError("Instrument Registry has no two-venue candidates")
        runner = feed_runner or run_bounded_public_feed

        async with self._lock:
            self._running = True
            self._running_operation = "feed_probe"
            previous = dict(self._payload.get("feed_probe") or self._empty_payload()["feed_probe"])
            now = datetime.now(timezone.utc).isoformat()
            previous.update({"status": "running", "last_attempt_at": now, "error": None})
            self._payload["feed_probe"] = previous
            try:
                try:
                    report = await runner(
                        registry,
                        symbols,
                        duration_sec=duration_sec,
                        max_symbols=max_symbols,
                    )
                    quality = self._feed_quality(report)
                    if not quality["ready_for_bounded_research"]:
                        raise RuntimeError(
                            "feed_quality_failed:" + ",".join(quality["reasons"])
                        )
                except Exception as exc:  # pylint: disable=broad-except
                    previous["status"] = "stale" if previous.get("report") else "error"
                    previous["error"] = f"{type(exc).__name__}: {exc}"
                    previous["last_good_used"] = bool(previous.get("report"))
                else:
                    previous.update({
                        "status": "fresh",
                        "last_success_at": datetime.now(timezone.utc).isoformat(),
                        "error": None,
                        "last_good_used": False,
                        "quality": quality,
                        "report": report,
                    })
                self._payload["feed_probe"] = previous
                self._payload["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._write()
            finally:
                self._running = False
                self._running_operation = None
        payload = self.status()
        payload["feed_probe_result"] = "completed"
        return payload
