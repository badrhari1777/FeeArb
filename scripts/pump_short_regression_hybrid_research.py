from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_short_regression_hybrid_research import (
    DEFAULT_COMPOUND_ACTIONS,
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_DIR,
    run_regression_hybrid_research,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run pump-short regression and hybrid-rule research.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--compound-actions", type=Path, default=DEFAULT_COMPOUND_ACTIONS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-defensive-rules", type=int, default=80)
    args = parser.parse_args()
    metadata = run_regression_hybrid_research(
        input_path=args.input,
        compound_actions_path=args.compound_actions,
        output_dir=args.output_dir,
        max_defensive_rules=args.max_defensive_rules,
    )
    print(metadata)


if __name__ == "__main__":
    main()
