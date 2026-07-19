from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_short_super_pump_tier_research import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PER_EVENT_DIR,
    parse_date_to_ms,
    run_super_pump_tier_research,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run super-pump tier research.")
    parser.add_argument("--per-event-dir", type=Path, default=DEFAULT_PER_EVENT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--top-rules-per-bucket", type=int, default=8)
    args = parser.parse_args()
    metadata = run_super_pump_tier_research(
        per_event_dir=args.per_event_dir,
        output_dir=args.output_dir,
        start_ts_ms=parse_date_to_ms(args.start_date),
        top_rules_per_bucket=args.top_rules_per_bucket,
    )
    print(metadata)


if __name__ == "__main__":
    main()
