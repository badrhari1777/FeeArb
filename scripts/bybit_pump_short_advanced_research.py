from __future__ import annotations
import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.bybit_pump_short_advanced_research import (  # noqa: E402
    DEFAULT_GRID_OUTCOMES,
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_DIR,
    run_advanced_research,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run advanced Bybit pump-short research sweeps.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"JSONL input. Default: {DEFAULT_INPUT}")
    parser.add_argument(
        "--grid-outcomes",
        type=Path,
        default=DEFAULT_GRID_OUTCOMES,
        help=f"Ladder outcome CSV for funding-gate postprocessing. Default: {DEFAULT_GRID_OUTCOMES}",
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
    metadata = run_advanced_research(
        input_path=args.input,
        grid_outcomes_path=args.grid_outcomes,
        output_dir=args.output_dir,
    )
    print(
        "Bybit pump-short advanced research complete: "
        f"symbols={metadata['symbols_seen']}, "
        f"events={metadata['events']}, "
        f"funding_gate_rows={metadata['funding_gate_rows']}, "
        f"runner_outcomes={metadata['runner_outcomes']}, "
        f"cooling_readd_outcomes={metadata['cooling_readd_outcomes']}, "
        f"elapsed_sec={metadata['elapsed_sec']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
