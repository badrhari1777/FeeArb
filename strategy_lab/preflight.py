"""Bounded capacity preflight for the research-only Strategy Lab feed."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import time
from typing import Any, Awaitable, Callable, Iterable, Iterator
import uuid

from .external_contract import TARGET_EXCHANGES
from .public_feed import run_bounded_public_feed


PREFLIGHT_VERSION = "strategy_lab_capacity_preflight_v1"
MAX_PREFLIGHT_DURATION_SEC = 3600.0
DEFAULT_CYCLE_INTERVAL_SEC = 60.0
DEFAULT_CYCLE_DURATION_SEC = 30.0
DEFAULT_MAX_SYMBOLS_PER_CYCLE = 10
FIELD_NAMES = (
    "best_bid",
    "best_ask",
    "last_price",
    "mark_price",
    "index_price",
    "funding_rate",
    "predicted_funding_rate",
    "next_funding_time_ms",
    "open_interest",
    "volume_24h_quote",
)

FeedRunner = Callable[..., Awaitable[dict[str, Any]]]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def _process_memory_bytes() -> tuple[int | None, int | None]:
    """Return current and process peak RSS without adding a runtime dependency."""
    if os.name == "nt":
        try:
            import ctypes  # pylint: disable=import-outside-toplevel
            from ctypes import wintypes  # pylint: disable=import-outside-toplevel

            class ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            psapi.GetProcessMemoryInfo.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(ProcessMemoryCounters),
                wintypes.DWORD,
            ]
            psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
            counters = ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            handle = kernel32.GetCurrentProcess()
            ok = psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            )
            if ok:
                return int(counters.WorkingSetSize), int(counters.PeakWorkingSetSize)
        except (AttributeError, OSError, ValueError):
            return None, None
    else:
        try:
            import resource  # pylint: disable=import-outside-toplevel

            peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            if peak > 0:
                # Linux reports KiB; macOS reports bytes.
                if os.uname().sysname.lower() != "darwin":
                    peak *= 1024
                return None, peak
        except (AttributeError, OSError, ValueError):
            return None, None
    return None, None


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


@contextmanager
def exclusive_preflight_lock(lock_path: Path) -> Iterator[None]:
    """Prevent two local capacity probes from multiplying public connections."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        try:
            existing = json.loads(lock_path.read_text(encoding="utf-8"))
            existing_pid = int(existing.get("pid") or 0)
        except (OSError, ValueError, json.JSONDecodeError):
            existing_pid = 0
        if existing_pid and _pid_is_alive(existing_pid):
            raise RuntimeError(f"strategy_lab_preflight_already_running:pid={existing_pid}")
        lock_path.unlink(missing_ok=True)
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError("strategy_lab_preflight_lock_race") from exc
    try:
        payload = json.dumps({"pid": os.getpid(), "started_at": utc_now_iso()}).encode("utf-8")
        os.write(descriptor, payload)
        os.close(descriptor)
        descriptor = -1
        yield
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        lock_path.unlink(missing_ok=True)


def eligible_registry_symbols(
    registry: dict[str, Any], candidates: Iterable[str]
) -> list[str]:
    vectors = registry.get("vectors") or {}
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        symbol = str(candidate or "").upper()
        if symbol and symbol not in seen and len(vectors.get(symbol) or {}) >= 2:
            result.append(symbol)
            seen.add(symbol)
    return result


def verified_observation_symbols(
    candidates: Iterable[str], verification: Iterable[dict[str, Any]]
) -> list[str]:
    """Keep candidate order but honor Observatory's source-aware Registry verdict."""
    verified = {
        str(row.get("canonical_symbol") or "").upper()
        for row in verification
        if row.get("eligible_for_observation")
    }
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        symbol = str(candidate or "").upper()
        if symbol in verified and symbol not in seen:
            result.append(symbol)
            seen.add(symbol)
    return result


def _rotation_window(symbols: list[str], cycle_index: int, max_symbols: int) -> list[str]:
    if len(symbols) <= max_symbols:
        return list(symbols)
    start = (cycle_index * max_symbols) % len(symbols)
    return [symbols[(start + offset) % len(symbols)] for offset in range(max_symbols)]


def _initial_totals(symbols: list[str]) -> dict[str, Any]:
    return {
        "cycles_attempted": 0,
        "cycles_completed": 0,
        "cycles_failed": 0,
        "observations": 0,
        "expected_pairs": 0,
        "invalid_bbo": 0,
        "subscription_errors": 0,
        "parse_errors": 0,
        "rest_errors": 0,
        "rest_requests": 0,
        "rest_updates": 0,
        "rest_bytes": 0,
        "connections": 0,
        "messages": 0,
        "updates": 0,
        "raw_observation_bytes": 0,
        "sampled_symbols": set(),
        "observed_symbols": set(),
        "scheduled_delay_sec": [],
        "cycle_errors": [],
        "eligible_symbols": list(symbols),
        "venues": {
            venue: {"expected": 0, "observed": 0, "connections": 0, "messages": 0,
                    "updates": 0, "parse_errors": 0, "subscription_errors": 0,
                    "rest_errors": 0, "rest_requests": 0, "rest_updates": 0,
                    "rest_bytes": 0, "cycles_with_error": 0}
            for venue in TARGET_EXCHANGES
        },
        "fields": {field: {venue: 0 for venue in TARGET_EXCHANGES} for field in FIELD_NAMES},
    }


def _accumulate_report(totals: dict[str, Any], report: dict[str, Any]) -> None:
    totals["cycles_completed"] += 1
    observations = list(report.get("observations") or [])
    totals["observations"] += len(observations)
    totals["expected_pairs"] += int((report.get("plan") or {}).get("expected_pairs") or 0)
    totals["invalid_bbo"] += len(report.get("invalid_bbo") or [])
    totals["observed_symbols"].update(
        str(row.get("canonical_symbol") or "") for row in observations if row.get("canonical_symbol")
    )
    venue_coverage = report.get("venue_coverage") or {}
    venue_status = report.get("venue_status") or {}
    fields = report.get("field_availability") or {}
    for venue in TARGET_EXCHANGES:
        coverage = venue_coverage.get(venue) or {}
        status = venue_status.get(venue) or {}
        venue_total = totals["venues"][venue]
        venue_total["expected"] += int(coverage.get("expected") or 0)
        venue_total["observed"] += int(coverage.get("observed") or 0)
        for key in (
            "connections", "messages", "updates", "parse_errors", "subscription_errors",
            "rest_errors", "rest_requests", "rest_updates", "rest_bytes",
        ):
            value = int(status.get(key) or 0)
            venue_total[key] += value
            totals[key] += value
        if status.get("status") in {"error", "deadline_cancelled"} or status.get("error"):
            venue_total["cycles_with_error"] += 1
        for field in FIELD_NAMES:
            totals["fields"][field][venue] += int((fields.get(field) or {}).get(venue) or 0)


def _percent(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator) * 100.0, 3)


def _finalize_report(
    *,
    run_id: str,
    started_at: str,
    completed_at: str,
    configured_duration_sec: float,
    actual_duration_sec: float,
    cycle_interval_sec: float,
    cycle_duration_sec: float,
    max_symbols_per_cycle: int,
    totals: dict[str, Any],
    compressed_bytes: int,
    cycles_bytes: int,
    cpu_seconds: float,
    rss_start: int | None,
    rss_end: int | None,
    rss_peak: int | None,
) -> dict[str, Any]:
    venue_metrics: dict[str, Any] = {}
    for venue, row in totals["venues"].items():
        venue_metrics[venue] = {
            **row,
            "coverage_pct": _percent(row["observed"], row["expected"]),
        }
    field_metrics: dict[str, dict[str, Any]] = {}
    for field, venue_counts in totals["fields"].items():
        field_metrics[field] = {}
        for venue, available in venue_counts.items():
            observed = int(venue_metrics[venue]["observed"])
            field_metrics[field][venue] = {
                "available": int(available),
                "observed": observed,
                "coverage_pct": _percent(int(available), observed),
            }
    observations = int(totals["observations"])
    raw_bytes = int(totals["raw_observation_bytes"])
    duration_for_rate = max(actual_duration_sec, 0.001)
    disk_bytes = compressed_bytes + cycles_bytes
    compressed_per_row = round(compressed_bytes / observations, 3) if observations else None
    raw_per_row = round(raw_bytes / observations, 3) if observations else None
    delay_values = list(totals["scheduled_delay_sec"])

    failures: list[str] = []
    warnings: list[str] = []
    attempted = int(totals["cycles_attempted"])
    completed = int(totals["cycles_completed"])
    cycle_success_pct = _percent(completed, attempted) or 0.0
    if observations == 0:
        failures.append("no_observations")
    if cycle_success_pct < 90.0:
        failures.append("cycle_success_below_90pct")
    if int(totals["invalid_bbo"]) > 0:
        failures.append("invalid_bbo")
    if int(totals["subscription_errors"]) > 0:
        failures.append("subscription_errors")
    if int(totals["rest_errors"]) > 0:
        warnings.append("rest_snapshot_errors")
    for venue in ("binance", "bybit", "okx", "kucoin"):
        coverage = venue_metrics[venue]["coverage_pct"]
        if coverage is None or coverage < 80.0:
            failures.append(f"{venue}_coverage_below_80pct")
    gate_coverage = venue_metrics["gate"]["coverage_pct"]
    if gate_coverage is None or gate_coverage < 50.0:
        warnings.append("gate_coverage_below_50pct")
    required_fields = (
        "best_bid", "best_ask", "mark_price", "funding_rate", "open_interest",
        "volume_24h_quote",
    )
    for venue in ("binance", "bybit", "okx", "kucoin"):
        for field in required_fields:
            coverage = field_metrics[field][venue]["coverage_pct"]
            if coverage is None or coverage < 80.0:
                failures.append(f"{venue}_{field}_coverage_below_80pct")
    for field in required_fields:
        coverage = field_metrics[field]["gate"]["coverage_pct"]
        if coverage is not None and coverage < 50.0:
            warnings.append(f"gate_{field}_coverage_below_50pct")
    eligible = set(totals["eligible_symbols"])
    sampled = set(totals["sampled_symbols"])
    observed = set(totals["observed_symbols"])
    if eligible - sampled:
        failures.append("eligible_symbols_not_rotated")
    if sampled - observed:
        warnings.append("sampled_symbols_without_observation")
    max_delay = max(delay_values) if delay_values else None
    if max_delay is not None and max_delay > max(5.0, cycle_interval_sec * 0.5):
        warnings.append("cycle_schedule_delay_high")

    verdict = "FAIL" if failures else ("PASS_WITH_WARNINGS" if warnings else "PASS")
    return {
        "preflight_version": PREFLIGHT_VERSION,
        "run_id": run_id,
        "mode": "research_only_no_trading",
        "scheduler_enabled": False,
        "trade_signal": False,
        "started_at": started_at,
        "completed_at": completed_at,
        "configured_duration_sec": configured_duration_sec,
        "actual_duration_sec": round(actual_duration_sec, 3),
        "cycle_interval_sec": cycle_interval_sec,
        "cycle_duration_sec": cycle_duration_sec,
        "max_symbols_per_cycle": max_symbols_per_cycle,
        "eligible_symbol_count": len(eligible),
        "sampled_symbol_count": len(sampled),
        "observed_symbol_count": len(observed),
        "unobserved_symbols": sorted(sampled - observed),
        "cycles": {
            "attempted": attempted,
            "completed": completed,
            "failed": int(totals["cycles_failed"]),
            "success_pct": cycle_success_pct,
            "max_schedule_delay_sec": round(max_delay, 3) if max_delay is not None else None,
            "errors": list(totals["cycle_errors"]),
        },
        "feed": {
            "observations": observations,
            "expected_pairs": int(totals["expected_pairs"]),
            "pair_coverage_pct": _percent(observations, int(totals["expected_pairs"])),
            "invalid_bbo": int(totals["invalid_bbo"]),
            "subscription_errors": int(totals["subscription_errors"]),
            "parse_errors": int(totals["parse_errors"]),
            "rest_errors": int(totals["rest_errors"]),
            "connections": int(totals["connections"]),
            "messages": int(totals["messages"]),
            "updates": int(totals["updates"]),
            "rest_requests": int(totals["rest_requests"]),
            "rest_updates": int(totals["rest_updates"]),
            "rest_bytes": int(totals["rest_bytes"]),
            "venues": venue_metrics,
            "field_observations": totals["fields"],
            "field_coverage": field_metrics,
        },
        "resources": {
            "cpu_seconds": round(cpu_seconds, 3),
            "cpu_wall_pct_single_core": round(cpu_seconds / duration_for_rate * 100.0, 3),
            "rss_start_bytes": rss_start,
            "rss_end_bytes": rss_end,
            "rss_peak_bytes": rss_peak,
            "rss_growth_bytes": rss_end - rss_start if rss_end is not None and rss_start is not None else None,
            "raw_observation_bytes": raw_bytes,
            "raw_bytes_per_row": raw_per_row,
            "compressed_observation_bytes": compressed_bytes,
            "compressed_bytes_per_row": compressed_per_row,
            "cycle_summary_bytes": cycles_bytes,
            "disk_bytes_total": disk_bytes,
            "disk_bytes_per_hour_forecast": round(disk_bytes / duration_for_rate * 3600.0),
            "disk_bytes_per_day_forecast": round(disk_bytes / duration_for_rate * 86400.0),
        },
        "qa": {"verdict": verdict, "failures": failures, "warnings": warnings},
    }


async def run_capacity_preflight(
    registry: dict[str, Any],
    candidates: Iterable[str],
    *,
    output_dir: Path,
    duration_sec: float = MAX_PREFLIGHT_DURATION_SEC,
    cycle_interval_sec: float = DEFAULT_CYCLE_INTERVAL_SEC,
    cycle_duration_sec: float = DEFAULT_CYCLE_DURATION_SEC,
    max_symbols_per_cycle: int = DEFAULT_MAX_SYMBOLS_PER_CYCLE,
    feed_runner: FeedRunner | None = None,
) -> dict[str, Any]:
    """Run rotating bounded probes and persist raw observations plus QA metrics."""
    duration = float(duration_sec)
    interval = float(cycle_interval_sec)
    cycle_duration = float(cycle_duration_sec)
    max_symbols = int(max_symbols_per_cycle)
    if not 1.0 <= duration <= MAX_PREFLIGHT_DURATION_SEC:
        raise ValueError(f"duration_sec must be in 1..{int(MAX_PREFLIGHT_DURATION_SEC)}")
    if interval < 1.0 or interval > duration:
        raise ValueError("cycle_interval_sec must be in 1..duration_sec")
    if not 1.0 <= cycle_duration <= 30.0:
        raise ValueError("cycle_duration_sec must be in 1..30")
    if not 1 <= max_symbols <= 10:
        raise ValueError("max_symbols_per_cycle must be in 1..10")
    symbols = eligible_registry_symbols(registry, candidates)
    if not symbols:
        raise ValueError("preflight requires at least one two-venue registry symbol")

    output_dir.mkdir(parents=True, exist_ok=False)
    observations_path = output_dir / "observations.jsonl.gz"
    cycles_path = output_dir / "cycles.jsonl"
    status_path = output_dir / "status.json"
    report_path = output_dir / "report.json"
    runner = feed_runner or run_bounded_public_feed
    run_id = output_dir.parent.name if output_dir.name == "run" else output_dir.name
    started_at = utc_now_iso()
    start_monotonic = time.monotonic()
    cpu_started = time.process_time()
    rss_start, rss_peak_seen = _process_memory_bytes()
    totals = _initial_totals(symbols)
    _atomic_write_json(status_path, {
        "preflight_version": PREFLIGHT_VERSION,
        "run_id": run_id,
        "status": "running",
        "mode": "research_only_no_trading",
        "started_at": started_at,
        "pid": os.getpid(),
        "eligible_symbol_count": len(symbols),
        "cycles_attempted": 0,
        "cycles_completed": 0,
        "last_error": None,
    })

    with gzip.open(observations_path, "wt", encoding="utf-8", compresslevel=6) as observation_file, cycles_path.open("w", encoding="utf-8") as cycle_file:
        cycle_index = 0
        while True:
            scheduled = start_monotonic + cycle_index * interval
            if scheduled >= start_monotonic + duration:
                break
            delay = scheduled - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            actual_cycle_start = time.monotonic()
            schedule_delay = max(0.0, actual_cycle_start - scheduled)
            totals["scheduled_delay_sec"].append(schedule_delay)
            window = _rotation_window(symbols, cycle_index, max_symbols)
            totals["sampled_symbols"].update(window)
            totals["cycles_attempted"] += 1
            cycle_payload: dict[str, Any] = {
                "cycle_index": cycle_index,
                "scheduled_offset_sec": round(cycle_index * interval, 3),
                "schedule_delay_sec": round(schedule_delay, 3),
                "started_at": utc_now_iso(),
                "symbols": window,
                "status": "running",
            }
            try:
                report = await runner(
                    registry,
                    window,
                    duration_sec=min(cycle_duration, max(1.0, start_monotonic + duration - time.monotonic())),
                    max_symbols=max_symbols,
                )
            except Exception as exc:  # pylint: disable=broad-except
                error = f"{type(exc).__name__}: {exc}"
                totals["cycles_failed"] += 1
                totals["cycle_errors"].append({"cycle_index": cycle_index, "error": error})
                cycle_payload.update({"status": "error", "error": error, "completed_at": utc_now_iso()})
            else:
                _accumulate_report(totals, report)
                for observation in report.get("observations") or []:
                    stored = {
                        "preflight_version": PREFLIGHT_VERSION,
                        "run_id": run_id,
                        "cycle_index": cycle_index,
                        **observation,
                    }
                    line = json.dumps(stored, ensure_ascii=False, separators=(",", ":"))
                    totals["raw_observation_bytes"] += len(line.encode("utf-8")) + 1
                    observation_file.write(line + "\n")
                cycle_payload.update({
                    "status": "completed",
                    "completed_at": utc_now_iso(),
                    "observation_count": int(report.get("observation_count") or 0),
                    "expected_pairs": int((report.get("plan") or {}).get("expected_pairs") or 0),
                    "pair_coverage_pct": report.get("pair_coverage_pct"),
                    "venue_coverage": report.get("venue_coverage") or {},
                    "venue_status": report.get("venue_status") or {},
                    "invalid_bbo": report.get("invalid_bbo") or [],
                })
            observation_file.flush()
            cycle_file.write(json.dumps(cycle_payload, ensure_ascii=False, separators=(",", ":")) + "\n")
            cycle_file.flush()
            rss_current, rss_process_peak = _process_memory_bytes()
            peaks = [value for value in (rss_peak_seen, rss_current, rss_process_peak) if value is not None]
            rss_peak_seen = max(peaks) if peaks else None
            _atomic_write_json(status_path, {
                "preflight_version": PREFLIGHT_VERSION,
                "run_id": run_id,
                "status": "running",
                "mode": "research_only_no_trading",
                "started_at": started_at,
                "updated_at": utc_now_iso(),
                "pid": os.getpid(),
                "eligible_symbol_count": len(symbols),
                "last_cycle_index": cycle_index,
                "cycles_attempted": totals["cycles_attempted"],
                "cycles_completed": totals["cycles_completed"],
                "cycles_failed": totals["cycles_failed"],
                "observations": totals["observations"],
                "last_error": totals["cycle_errors"][-1] if totals["cycle_errors"] else None,
            })
            cycle_index += 1

        remaining = start_monotonic + duration - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)

    completed_at = utc_now_iso()
    actual_duration = time.monotonic() - start_monotonic
    rss_end, rss_process_peak = _process_memory_bytes()
    peaks = [value for value in (rss_peak_seen, rss_end, rss_process_peak) if value is not None]
    rss_peak = max(peaks) if peaks else None
    report = _finalize_report(
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        configured_duration_sec=duration,
        actual_duration_sec=actual_duration,
        cycle_interval_sec=interval,
        cycle_duration_sec=cycle_duration,
        max_symbols_per_cycle=max_symbols,
        totals=totals,
        compressed_bytes=observations_path.stat().st_size,
        cycles_bytes=cycles_path.stat().st_size,
        cpu_seconds=time.process_time() - cpu_started,
        rss_start=rss_start,
        rss_end=rss_end,
        rss_peak=rss_peak,
    )
    _atomic_write_json(report_path, report)
    _atomic_write_json(status_path, {
        "preflight_version": PREFLIGHT_VERSION,
        "run_id": run_id,
        "status": "completed",
        "mode": "research_only_no_trading",
        "started_at": started_at,
        "completed_at": completed_at,
        "pid": os.getpid(),
        "qa_verdict": report["qa"]["verdict"],
        "cycles_attempted": report["cycles"]["attempted"],
        "cycles_completed": report["cycles"]["completed"],
        "cycles_failed": report["cycles"]["failed"],
        "observations": report["feed"]["observations"],
        "report_path": str(report_path),
    })
    return report
