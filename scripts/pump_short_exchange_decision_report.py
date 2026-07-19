from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_exchange_decision_report import (  # noqa: E402
    DEFAULT_COMPARISON_DIR,
    DEFAULT_RESEARCH_ROOT,
    build_exchange_decision_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build focused exchange decision report from pump-short outputs.")
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument("--comparison-dir", type=Path, default=DEFAULT_COMPARISON_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = build_exchange_decision_report(
        research_root=args.research_root,
        comparison_dir=args.comparison_dir,
    )
    print(f"exchange decision report complete: {metadata['report']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
