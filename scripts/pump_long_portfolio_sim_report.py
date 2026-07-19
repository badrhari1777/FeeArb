from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_long_portfolio_sim_report import (  # noqa: E402
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_DIR,
    STARTING_CAPITAL_USD,
    run_pump_long_portfolio_sim_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run broad pump-long portfolio simulations and render an HTML report.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--capital", type=float, default=STARTING_CAPITAL_USD)
    args = parser.parse_args()
    metadata = run_pump_long_portfolio_sim_report(
        input_path=args.input,
        output_dir=args.output_dir,
        starting_capital_usd=args.capital,
    )
    print(metadata)


if __name__ == "__main__":
    main()
