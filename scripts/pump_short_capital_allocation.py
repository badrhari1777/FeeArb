from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_capital_allocation import (  # noqa: E402
    DEFAULT_CAPITAL_USD,
    DEFAULT_EXCHANGES,
    DEFAULT_INPUT,
    DEFAULT_LEVERAGE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_STRATEGY,
    run_capital_allocation_analysis,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze pump-short capital allocation by concurrent coin slots.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--capital-usd", type=float, default=DEFAULT_CAPITAL_USD)
    parser.add_argument("--leverage", type=float, default=DEFAULT_LEVERAGE)
    parser.add_argument("--max-slots", type=int, default=10)
    parser.add_argument("--strategy", default=DEFAULT_STRATEGY)
    parser.add_argument("--exchanges", nargs="*", default=list(DEFAULT_EXCHANGES), choices=["binance", "bybit"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = run_capital_allocation_analysis(
        input_path=args.input,
        output_dir=args.output_dir,
        capital_usd=args.capital_usd,
        leverage=args.leverage,
        max_slots=args.max_slots,
        strategy=args.strategy,
        exchanges=tuple(args.exchanges),
    )
    print(
        "pump-short capital allocation complete: "
        f"trades={metadata['trades_loaded']}, "
        f"summary_rows={metadata['summary_rows']}, "
        f"capital={metadata['capital_usd']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
