from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab_event_lake import (
    DEFAULT_OUTPUT_DIR,
    EventLakeConfig,
    run_event_lake,
)


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
    )
    metadata = run_event_lake(
        output_dir=args.output_dir,
        config=config,
        execute_public=args.execute_public,
    )
    print(metadata)


if __name__ == "__main__":
    main()
