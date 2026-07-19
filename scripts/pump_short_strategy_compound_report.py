"""Build the $1000 compounding pump-short strategy report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_strategy_compound_report import (  # noqa: E402
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    run_strategy_compound_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory with selected_trades.csv from the funding/TP capital grid.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the compounding report will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metadata = run_strategy_compound_report(input_dir=args.input_dir, output_dir=args.output_dir)
    print(f"Report: {metadata['report_path']}")
    print(f"Strategies: {metadata['strategies']}")
    print(f"Actions: {metadata['action_rows']}")
    print(f"Top-ups: {metadata['topup_rows']}")


if __name__ == "__main__":
    main()
