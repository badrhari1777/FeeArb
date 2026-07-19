from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_premium_event_pages import (  # noqa: E402
    DEFAULT_OUTCOMES_INPUT,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_WINDOWS_INPUT,
    build_premium_event_pages,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build per-event premium/funding candidate HTML pages.")
    parser.add_argument("--windows", type=Path, default=DEFAULT_WINDOWS_INPUT)
    parser.add_argument("--outcomes", type=Path, default=DEFAULT_OUTCOMES_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    metadata = build_premium_event_pages(
        windows_input=args.windows,
        outcomes_input=args.outcomes,
        output_dir=args.output_dir,
    )
    print(metadata)


if __name__ == "__main__":
    main()
