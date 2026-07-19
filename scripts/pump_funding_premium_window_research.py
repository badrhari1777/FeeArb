from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_funding_premium_window_research import (  # noqa: E402
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_DIR,
    run_funding_premium_window_research,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run funding/premium event-window long research.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    metadata = run_funding_premium_window_research(input_path=args.input, output_dir=args.output_dir)
    print(metadata)


if __name__ == "__main__":
    main()
