from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab import (
    DEFAULT_DB_PATH,
    DEFAULT_LOG_DIR,
    DEFAULT_OUTPUT_DIR,
    StrategyLabConfig,
    run_strategy_lab,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the no-trading Strategy Lab evidence package."
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--trigger-abs-spread-pct", type=float, default=0.75)
    parser.add_argument("--strong-abs-spread-pct", type=float, default=3.0)
    parser.add_argument("--trigger-abs-zscore", type=float, default=2.0)
    parser.add_argument("--estimated-roundtrip-cost-pct", type=float, default=0.18)
    parser.add_argument("--enrich-public-api", action="store_true")
    parser.add_argument("--max-api-events", type=int, default=8)
    parser.add_argument("--api-window-hours", type=float, default=8.0)
    parser.add_argument("--api-timeframe", default="5m")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = StrategyLabConfig(
        trigger_abs_spread_pct=args.trigger_abs_spread_pct,
        strong_abs_spread_pct=args.strong_abs_spread_pct,
        trigger_abs_zscore=args.trigger_abs_zscore,
        estimated_roundtrip_cost_pct=args.estimated_roundtrip_cost_pct,
    )
    metadata = run_strategy_lab(
        db_path=args.db,
        log_dir=args.log_dir,
        output_dir=args.output_dir,
        config=config,
        enrich_public_api=args.enrich_public_api,
        max_api_events=max(0, args.max_api_events),
        api_window_hours=max(1.0, args.api_window_hours),
        api_timeframe=args.api_timeframe,
    )
    print(metadata)


if __name__ == "__main__":
    main()
