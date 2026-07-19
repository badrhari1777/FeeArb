from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.bybit_pump_short_tail_runner_report import (  # noqa: E402
    DEFAULT_GRID_OUTCOMES,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RUNNER_OUTCOMES,
    run_tail25_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Bybit pump-short 25% tail runner report.")
    parser.add_argument(
        "--grid-outcomes",
        type=Path,
        default=DEFAULT_GRID_OUTCOMES,
        help=f"Baseline ladder outcomes CSV. Default: {DEFAULT_GRID_OUTCOMES}",
    )
    parser.add_argument(
        "--runner-outcomes",
        type=Path,
        default=DEFAULT_RUNNER_OUTCOMES,
        help=f"Small-runner outcomes CSV. Default: {DEFAULT_RUNNER_OUTCOMES}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = run_tail25_report(
        grid_outcomes_path=args.grid_outcomes,
        runner_outcomes_path=args.runner_outcomes,
        output_dir=args.output_dir,
    )
    print(
        "Bybit pump-short tail25 report complete: "
        f"baseline_rows={metadata['baseline_rows']}, "
        f"runner_rows={metadata['runner_rows']}, "
        f"comparison_rows={metadata['comparison_rows']}, "
        f"symbols={metadata['symbols']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
