from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_live_shared_margin_research import write_report  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay Pump Live shared-margin and bounded main-loan policies since 2024."
    )
    parser.add_argument(
        "--per-event-dir",
        type=Path,
        default=Path("data/research/pump_short_per_event_strategy_research"),
    )
    parser.add_argument(
        "--pullback-dir",
        type=Path,
        default=Path("data/research/pump_short_pullback_tier_research"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/research/pump_live_shared_margin_research"),
    )
    args = parser.parse_args()
    result = write_report(
        per_event_dir=args.per_event_dir,
        pullback_dir=args.pullback_dir,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
