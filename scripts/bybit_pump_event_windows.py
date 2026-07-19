from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_collectors.bybit_event_window import (  # noqa: E402
    DEFAULT_EVENT_INPUT,
    DEFAULT_OUTPUT_DIR,
    EventWindowConfig,
    collect_bybit_event_windows,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Bybit event-window data around known pump events.")
    parser.add_argument("--input-events", type=Path, default=DEFAULT_EVENT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--interval", action="append", dest="intervals", default=None, help="Bybit kline interval, e.g. 15 or 5. Repeatable.")
    parser.add_argument("--pre-hours", type=int, default=72)
    parser.add_argument("--post-hours", type=int, default=336)
    parser.add_argument("--min-pump-pct", type=float, default=80.0)
    parser.add_argument("--max-events", type=int, default=25)
    parser.add_argument("--symbol", action="append", dest="symbols", default=None)
    parser.add_argument("--sleep-sec", type=float, default=0.8)
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args()
    metadata = collect_bybit_event_windows(
        EventWindowConfig(
            input_events=args.input_events,
            output_dir=args.output_dir,
            intervals=tuple(args.intervals or ["15"]),
            pre_hours=args.pre_hours,
            post_hours=args.post_hours,
            min_pump_pct=args.min_pump_pct,
            max_events=args.max_events,
            sleep_sec=args.sleep_sec,
            resume=not args.no_resume,
        ),
        symbols=args.symbols,
    )
    print(metadata)


if __name__ == "__main__":
    main()
