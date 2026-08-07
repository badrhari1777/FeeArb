from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab_event_lake import (
    DEFAULT_OUTPUT_DIR,
    EventLakeConfig,
    estimate_full_catalog_run,
    run_event_lake,
    write_json_atomic,
)
from analysis_features.strategy_lab import PUMP_EVENT_SOURCES, load_pump_event_catalog


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a bounded public-only Strategy Lab event lake."
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--exchanges", default="binance,bybit")
    parser.add_argument("--max-events", type=int, default=3)
    parser.add_argument("--pre-hours", type=int, default=24)
    parser.add_argument("--post-hours", type=int, default=72)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--execute-public", action="store_true")
    parser.add_argument(
        "--all-events",
        action="store_true",
        help="Select every logical event up to --max-events instead of one latest event per symbol.",
    )
    parser.add_argument("--estimate-full-catalog", action="store_true")
    parser.add_argument("--pilot-calls", type=int, default=0)
    parser.add_argument("--pilot-elapsed-sec", type=float, default=0.0)
    parser.add_argument(
        "--code-commit",
        default="",
        help="Pin provenance to the original collector commit during an audited replay.",
    )
    return parser.parse_args()


def split_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def main() -> None:
    args = parse_args()
    config = EventLakeConfig(
        exchanges=split_csv(args.exchanges),
        symbols=tuple(item.upper() for item in split_csv(args.symbols)),
        max_events=max(1, args.max_events),
        pre_hours=max(0, args.pre_hours),
        post_hours=max(1, args.post_hours),
        timeframe=args.timeframe,
        selection_mode="all_events" if args.all_events else "latest_per_symbol",
    )
    if args.estimate_full_catalog:
        rows = load_pump_event_catalog(PUMP_EVENT_SOURCES)
        window_files = list((args.output_dir / "windows").glob("*.json"))
        average_window_bytes = (
            sum(path.stat().st_size for path in window_files) / len(window_files)
            if window_files
            else None
        )
        metadata_path = args.output_dir / "metadata.json"
        pilot_metadata = (
            json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata_path.exists()
            else {}
        )
        estimate = estimate_full_catalog_run(
            rows,
            config,
            as_of_ms=time.time_ns() // 1_000_000,
            average_window_bytes=average_window_bytes,
            pilot_calls=args.pilot_calls
            or int(pilot_metadata.get("public_calls_this_run") or 0)
            or None,
            pilot_elapsed_sec=args.pilot_elapsed_sec
            or float(pilot_metadata.get("elapsed_sec") or 0)
            or None,
        )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        write_json_atomic(args.output_dir / "full_run_estimate.json", estimate)
        print(estimate)
        return
    metadata = run_event_lake(
        output_dir=args.output_dir,
        config=config,
        execute_public=args.execute_public,
        code_commit=args.code_commit or None,
    )
    print(metadata)


if __name__ == "__main__":
    main()
