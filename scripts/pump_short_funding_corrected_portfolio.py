from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_funding_corrected_portfolio import (  # noqa: E402
    DEFAULT_EVENTS,
    DEFAULT_OUTPUT,
    DEFAULT_SAMPLES,
    run_funding_corrected_portfolio,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Funding-corrected Pump Short portfolio replay")
    parser.add_argument("--samples", type=Path, default=DEFAULT_SAMPLES)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    parser.add_argument("--reuse-raw", action="store_true", help="Reuse saved funding/mark JSONL evidence")
    args = parser.parse_args()
    result = run_funding_corrected_portfolio(
        samples_path=args.samples,
        events_path=args.events,
        output_dir=args.output_dir,
        sleep_sec=max(0.0, args.sleep_sec),
        reuse_raw=args.reuse_raw,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
