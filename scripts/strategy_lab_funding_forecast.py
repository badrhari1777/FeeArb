from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab_funding_forecast import (
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    FundingForecastConfig,
    run_funding_forecast,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build causal Funding Forecast v1 research artifacts."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--allow-in-progress", action="store_true")
    parser.add_argument("--max-windows", type=int, default=0)
    return parser.parse_args()


def current_git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            timeout=5,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def main() -> None:
    args = parse_args()
    config = FundingForecastConfig(
        require_complete_event_lake=not args.allow_in_progress,
        max_windows=max(1, args.max_windows) if args.max_windows else None,
    )
    result = run_funding_forecast(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        config=config,
        code_commit=current_git_commit(),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
