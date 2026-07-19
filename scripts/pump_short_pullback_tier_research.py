from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_short_dynamic_combo_report import parse_date_to_ms
from analysis_features.pump_short_pullback_tier_research import (
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_DIR,
    run_pullback_tier_research,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run pullback tier research for pump-short strategy.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--top-candidates-per-bucket", type=int, default=8)
    args = parser.parse_args()
    metadata = run_pullback_tier_research(
        input_path=args.input,
        output_dir=args.output_dir,
        start_ts_ms=parse_date_to_ms(args.start_date),
        top_candidates_per_bucket=args.top_candidates_per_bucket,
    )
    print(metadata)


if __name__ == "__main__":
    main()
