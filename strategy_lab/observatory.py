"""Bounded, research-only external candidate observatory."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Awaitable, Callable
import uuid

from config import BASE_DIR

from .arbitragescanner_source import fetch_arbitragescanner
from .coinglass_source import fetch_coinglass
from .external_contract import CONTRACT_VERSION, ExternalObservation, TARGET_EXCHANGES, merge_external_candidates


Fetcher = Callable[[], Awaitable[dict[str, Any]]]


class StrategyLabObservatory:
    """One-shot source intake. It has no scheduler and no trading dependency."""

    def __init__(self, *, state_dir: Path | None = None, candidate_limit: int = 30) -> None:
        self.state_dir = state_dir or BASE_DIR / "data" / "research" / "strategy_lab_observatory" / "runtime"
        self.snapshot_path = self.state_dir / "latest.json"
        self.candidate_limit = int(candidate_limit)
        self._lock = asyncio.Lock()
        self._running = False
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
            self._payload = payload

    def _write(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        temp_path = self.state_dir / f"latest.{uuid.uuid4().hex}.tmp"
        temp_path.write_text(json.dumps(self._payload, indent=2, ensure_ascii=False), encoding="utf-8")
        temp_path.replace(self.snapshot_path)

    def status(self) -> dict[str, Any]:
        payload = json.loads(json.dumps(self._payload))
        payload["running"] = self._running
        return payload

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
                self._payload["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._payload["running"] = False
                self._write()
            finally:
                self._running = False
        payload = self.status()
        payload["refresh_result"] = "completed"
        return payload
