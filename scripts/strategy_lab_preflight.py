"""Run the explicitly confirmed one-hour Strategy Lab capacity preflight."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import BASE_DIR  # noqa: E402
from strategy_lab.observatory import StrategyLabObservatory  # noqa: E402
from strategy_lab.preflight import (  # noqa: E402
    DEFAULT_CYCLE_DURATION_SEC,
    DEFAULT_CYCLE_INTERVAL_SEC,
    DEFAULT_MAX_SYMBOLS_PER_CYCLE,
    DEFAULT_PREFLIGHT_DURATION_SEC,
    MAX_PREFLIGHT_DURATION_SEC,
    exclusive_preflight_lock,
    run_capacity_preflight,
    verified_observation_symbols,
)


CONFIRMATIONS = {
    "1h": "RUN STRATEGY LAB PREFLIGHT 1H",
    "24h": "RUN STRATEGY LAB PREFLIGHT 24H",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=tuple(CONFIRMATIONS), default="1h")
    parser.add_argument("--confirm", required=True, help="Exact profile confirmation text")
    parser.add_argument("--duration-sec", type=float, default=None)
    parser.add_argument("--cycle-interval-sec", type=float, default=DEFAULT_CYCLE_INTERVAL_SEC)
    parser.add_argument("--cycle-duration-sec", type=float, default=DEFAULT_CYCLE_DURATION_SEC)
    parser.add_argument("--max-symbols-per-cycle", type=int, default=DEFAULT_MAX_SYMBOLS_PER_CYCLE)
    parser.add_argument("--candidate-limit", type=int, default=60)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=BASE_DIR / "data" / "research" / "strategy_lab_observatory" / "preflight",
    )
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> dict[str, object]:
    if args.confirm != CONFIRMATIONS[args.profile]:
        raise ValueError("invalid_confirmation")
    duration_sec = (
        DEFAULT_PREFLIGHT_DURATION_SEC if args.duration_sec is None else float(args.duration_sec)
    )
    if args.profile == "1h" and not 1.0 <= duration_sec <= DEFAULT_PREFLIGHT_DURATION_SEC:
        raise ValueError("1h profile duration_sec must be in 1..3600")
    if args.profile == "24h" and duration_sec != MAX_PREFLIGHT_DURATION_SEC:
        raise ValueError("24h profile requires duration_sec=86400")
    if args.candidate_limit < 1 or args.candidate_limit > 60:
        raise ValueError("candidate_limit must be in 1..60")
    free_bytes = shutil.disk_usage(args.output_root.parent).free
    if free_bytes < 1024 ** 3:
        raise RuntimeError("preflight_requires_at_least_1GiB_free_disk")

    run_id = f"preflight-{args.profile}-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_root / run_id
    bootstrap_dir = output_dir / "bootstrap"
    observatory = StrategyLabObservatory(
        state_dir=bootstrap_dir,
        candidate_limit=args.candidate_limit,
    )
    external = await observatory.refresh()
    if not external.get("candidates"):
        raise RuntimeError("external_refresh_returned_no_candidates")
    registry_result = await observatory.refresh_registry()
    registry_state = registry_result.get("registry") or {}
    if registry_state.get("status") != "fresh" or not registry_state.get("snapshot"):
        raise RuntimeError(f"registry_preflight_failed:{registry_state.get('error')}")
    candidate_order = [
        str(row.get("canonical_symbol") or "").upper()
        for row in registry_result.get("candidates") or []
    ]
    candidates = verified_observation_symbols(
        candidate_order,
        registry_state.get("verification") or [],
    )
    if len(candidates) != int(registry_state.get("eligible_candidate_count") or 0):
        raise RuntimeError(
            "registry_eligible_count_mismatch:"
            f"selected={len(candidates)},reported={registry_state.get('eligible_candidate_count')}"
        )
    bootstrap_summary = {
        "mode": "research_only_no_trading",
        "trade_signal": False,
        "profile": args.profile,
        "configured_duration_sec": duration_sec,
        "candidate_union_count": len(candidate_order),
        "verified_candidate_count": len(candidates),
        "source_status": {
            name: {
                "status": row.get("status"),
                "eligible_count": row.get("eligible_count"),
                "error": row.get("error"),
                "last_good_used": row.get("last_good_used"),
            }
            for name, row in (registry_result.get("sources") or {}).items()
        },
        "registry_status": registry_state.get("status"),
        "registry_eligible_candidate_count": registry_state.get("eligible_candidate_count"),
        "free_disk_bytes_before": free_bytes,
    }
    (output_dir / "bootstrap_summary.json").write_text(
        json.dumps(bootstrap_summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return await run_capacity_preflight(
        registry_state["snapshot"],
        candidates,
        output_dir=output_dir / "run",
        duration_sec=duration_sec,
        cycle_interval_sec=args.cycle_interval_sec,
        cycle_duration_sec=args.cycle_duration_sec,
        max_symbols_per_cycle=args.max_symbols_per_cycle,
    )


def main() -> int:
    args = parse_args()
    lock_path = args.output_root / "preflight.lock"
    try:
        with exclusive_preflight_lock(lock_path):
            report = asyncio.run(main_async(args))
    except Exception as exc:  # pylint: disable=broad-except
        print(json.dumps({"status": "error", "error": f"{type(exc).__name__}: {exc}"}))
        return 1
    print(json.dumps({
        "status": "completed",
        "run_id": report.get("run_id"),
        "qa": report.get("qa"),
        "cycles": report.get("cycles"),
        "feed": {
            "observations": (report.get("feed") or {}).get("observations"),
            "pair_coverage_pct": (report.get("feed") or {}).get("pair_coverage_pct"),
        },
        "resources": report.get("resources"),
    }, ensure_ascii=False))
    return 0 if (report.get("qa") or {}).get("verdict") != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
