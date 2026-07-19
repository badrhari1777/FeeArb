from __future__ import annotations
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_lifecycle_research import DEFAULT_INPUT
from analysis_features.pump_long_strategy_research import (
    DEFAULT_OUTPUT_DIR,
    run_long_strategy_research,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bybit pump long strategy research.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()
    metadata = run_long_strategy_research(
        input_path=args.input,
        output_dir=args.output_dir,
        max_events=args.max_events,
    )
    print(metadata)


if __name__ == "__main__":
    main()
