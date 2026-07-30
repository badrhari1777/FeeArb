from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from analysis_features.pump_spike_risk_research import (  # noqa: E402
    DEFAULT_EVENT_WINDOWS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PER_EVENT_DIR,
    DEFAULT_PULLBACK_DIR,
    DEFAULT_UNIVERSE_DIR,
    run_pump_spike_risk_research,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze historical Bybit spike risk and Pump Live protection."
    )
    parser.add_argument("--universe-dir", type=Path, default=DEFAULT_UNIVERSE_DIR)
    parser.add_argument("--per-event-dir", type=Path, default=DEFAULT_PER_EVENT_DIR)
    parser.add_argument("--pullback-dir", type=Path, default=DEFAULT_PULLBACK_DIR)
    parser.add_argument(
        "--event-windows", type=Path, default=DEFAULT_EVENT_WINDOWS
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    result = run_pump_spike_risk_research(
        universe_dir=args.universe_dir,
        per_event_dir=args.per_event_dir,
        pullback_dir=args.pullback_dir,
        event_windows_path=args.event_windows,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
