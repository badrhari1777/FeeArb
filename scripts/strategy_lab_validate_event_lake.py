from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab_event_lake_validation import (
    validate_event_lake_output,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Strategy Lab Event Lake cache and provenance."
    )
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--allow-in-progress", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = validate_event_lake_output(
        args.output_dir,
        require_complete=not args.allow_in_progress,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
