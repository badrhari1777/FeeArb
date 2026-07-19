from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_strategy_graphic_report import (  # noqa: E402
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    run_strategy_graphic_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build graphical pump-short strategy report for selected $1000 variants.")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = run_strategy_graphic_report(input_dir=args.input_dir, output_dir=args.output_dir)
    print(
        "pump-short strategy graphic report complete: "
        f"strategies={len(metadata['strategies'])}, "
        f"actions={metadata['action_rows']}, "
        f"topups={metadata['topup_rows']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
