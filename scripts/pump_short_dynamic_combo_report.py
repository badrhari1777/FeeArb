from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_short_dynamic_combo_report import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PER_EVENT_DIR,
    DEFAULT_POLICY_DIR,
    parse_date_to_ms,
    parse_slots,
    run_dynamic_combo_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run dynamic $3000 pump-short combo report.")
    parser.add_argument("--per-event-dir", type=Path, default=DEFAULT_PER_EVENT_DIR)
    parser.add_argument("--policy-dir", type=Path, default=DEFAULT_POLICY_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--combo-limit", type=int, default=10)
    parser.add_argument("--start-date", default=None, help="Optional UTC start date, e.g. 2024-01-01.")
    parser.add_argument("--slots", default="1,2,3,4", help="Comma-separated slot counts, e.g. 1,2,3,4,5.")
    args = parser.parse_args()
    metadata = run_dynamic_combo_report(
        per_event_dir=args.per_event_dir,
        policy_dir=args.policy_dir,
        output_dir=args.output_dir,
        combo_limit=args.combo_limit,
        start_ts_ms=parse_date_to_ms(args.start_date),
        slots=parse_slots(args.slots),
    )
    print(metadata)


if __name__ == "__main__":
    main()
