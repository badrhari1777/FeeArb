from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_cycle_portfolio_report import (  # noqa: E402
    DEFAULT_LONG_OUTCOMES,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SHORT_TRADES,
    STARTING_CAPITAL_USD,
    run_pump_cycle_portfolio_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run combined pump-cycle long+short portfolio report.")
    parser.add_argument("--long-outcomes", type=Path, default=DEFAULT_LONG_OUTCOMES)
    parser.add_argument("--short-trades", type=Path, default=DEFAULT_SHORT_TRADES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--capital", type=float, default=STARTING_CAPITAL_USD)
    args = parser.parse_args()
    metadata = run_pump_cycle_portfolio_report(
        long_outcomes_path=args.long_outcomes,
        short_trades_path=args.short_trades,
        output_dir=args.output_dir,
        starting_capital_usd=args.capital,
    )
    print(metadata)


if __name__ == "__main__":
    main()
