from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab import PUMP_EVENT_SOURCES, load_pump_event_catalog
from analysis_features.strategy_lab_local_archive import (
    DEFAULT_ARCHIVE_ROOT,
    DEFAULT_OUTPUT_DIR,
    LocalArchiveConfig,
    run_local_archive_pilot,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a zero-network Strategy Lab local archive pilot."
    )
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--symbols", default="COTIUSDT,HFTUSDT,SIRENUSDT")
    parser.add_argument("--exchanges", default="binance,bitget,bybit,kucoin,mexc,okx")
    parser.add_argument("--pre-hours", type=int, default=24)
    parser.add_argument("--post-hours", type=int, default=72)
    return parser.parse_args()


def split_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def main() -> None:
    args = parse_args()
    config = LocalArchiveConfig(
        symbols=tuple(item.upper() for item in split_csv(args.symbols)),
        exchanges=split_csv(args.exchanges),
        pre_hours=max(0, args.pre_hours),
        post_hours=max(1, args.post_hours),
    )
    metadata = run_local_archive_pilot(
        events=load_pump_event_catalog(PUMP_EVENT_SOURCES),
        archive_root=args.archive_root,
        output_dir=args.output_dir,
        config=config,
    )
    print(metadata)


if __name__ == "__main__":
    main()
