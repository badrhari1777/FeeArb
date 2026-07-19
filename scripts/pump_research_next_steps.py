from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_research_next_steps import (
    DEFAULT_ACTIVE_WINDOW,
    DEFAULT_CYCLE_SUMMARY,
    DEFAULT_CYCLE_TRADES,
    DEFAULT_EVENT_WINDOWS,
    DEFAULT_LONG_OUTCOMES,
    DEFAULT_LONG_PORTFOLIO,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SHADOW_HISTORY,
    run_pump_research_next_steps,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the next pump-cycle research package.")
    parser.add_argument("--event-windows", type=Path, default=DEFAULT_EVENT_WINDOWS)
    parser.add_argument("--long-outcomes", type=Path, default=DEFAULT_LONG_OUTCOMES)
    parser.add_argument("--long-portfolio", type=Path, default=DEFAULT_LONG_PORTFOLIO)
    parser.add_argument("--cycle-summary", type=Path, default=DEFAULT_CYCLE_SUMMARY)
    parser.add_argument("--cycle-trades", type=Path, default=DEFAULT_CYCLE_TRADES)
    parser.add_argument("--shadow-history", type=Path, default=DEFAULT_SHADOW_HISTORY)
    parser.add_argument("--active-window", type=Path, default=DEFAULT_ACTIVE_WINDOW)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--fetch-market-context", action="store_true")
    parser.add_argument("--market-context", type=Path, default=None)
    parser.add_argument("--market-sleep-sec", type=float, default=0.05)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metadata = run_pump_research_next_steps(
        event_windows_path=args.event_windows,
        long_outcomes_path=args.long_outcomes,
        long_portfolio_path=args.long_portfolio,
        cycle_summary_path=args.cycle_summary,
        cycle_trades_path=args.cycle_trades,
        shadow_history_path=args.shadow_history,
        active_window_path=args.active_window,
        output_dir=args.output_dir,
        fetch_market_context=args.fetch_market_context,
        market_context_path=args.market_context,
        market_sleep_sec=args.market_sleep_sec,
    )
    print(metadata)


if __name__ == "__main__":
    main()
