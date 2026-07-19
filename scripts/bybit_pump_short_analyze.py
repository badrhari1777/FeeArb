from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.bybit_pump_short_outcomes import (  # noqa: E402
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_DIR,
    run_analysis,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Bybit pump-short collector output.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"JSONL input. Default: {DEFAULT_INPUT}")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = run_analysis(input_path=args.input, output_dir=args.output_dir)
    print(
        "Bybit pump-short analysis complete: "
        f"symbols={metadata['symbols_seen']}, "
        f"events={metadata['events']}, "
        f"outcomes={metadata['outcomes']}, "
        f"elapsed_sec={metadata['elapsed_sec']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
