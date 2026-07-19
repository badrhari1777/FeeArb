from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_short_policy_portfolio_research import (
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    run_policy_portfolio_research,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bybit pump-short policy portfolio research.")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--top-selected-limit", type=int, default=80)
    args = parser.parse_args()
    metadata = run_policy_portfolio_research(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        top_selected_limit=args.top_selected_limit,
    )
    print(metadata)


if __name__ == "__main__":
    main()
