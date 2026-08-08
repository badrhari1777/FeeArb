from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab import DEFAULT_DB_PATH
from analysis_features.strategy_lab_executable_spread import (
    DEFAULT_OUTPUT_DIR,
    ExecutableSpreadConfig,
    run_executable_spread_timing,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay causal executable spread timing in research-only mode."
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--source-max-ts-ms", type=int, default=0)
    return parser.parse_args()


def current_git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=5
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def main() -> None:
    args = parse_args()
    result = run_executable_spread_timing(
        db_path=args.db_path,
        output_dir=args.output_dir,
        config=ExecutableSpreadConfig(
            source_max_ts_ms=args.source_max_ts_ms or None,
        ),
        code_commit=current_git_commit(),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
