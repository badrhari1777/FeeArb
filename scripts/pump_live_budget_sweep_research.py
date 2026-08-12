from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_live_budget_sweep_research import (  # noqa: E402
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_DIR,
    run_budget_sweep,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay Pump Live budgets from $600 to $1200")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    result = run_budget_sweep(input_path=args.input, output_dir=args.output_dir)
    print(json.dumps({key: value for key, value in result.items() if key != "summaries"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
