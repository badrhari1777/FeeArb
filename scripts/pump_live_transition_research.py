from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_live_transition_research import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PAPER_DIR,
    DEFAULT_PER_EVENT_DIR,
    DEFAULT_PULLBACK_DIR,
    run_pump_live_transition_research,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research Pump/Dump paper-to-live sizing and account isolation.")
    parser.add_argument("--per-event-dir", type=Path, default=DEFAULT_PER_EVENT_DIR)
    parser.add_argument("--pullback-dir", type=Path, default=DEFAULT_PULLBACK_DIR)
    parser.add_argument("--paper-dir", type=Path, default=DEFAULT_PAPER_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_pump_live_transition_research(
        per_event_dir=args.per_event_dir,
        pullback_dir=args.pullback_dir,
        paper_dir=args.paper_dir,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
