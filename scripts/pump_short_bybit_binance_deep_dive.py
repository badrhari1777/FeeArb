from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_bybit_binance_deep_dive import (  # noqa: E402
    DEFAULT_CAPITAL_USD,
    DEFAULT_INPUT_ROOT,
    DEFAULT_LEG_NOTIONAL_USD,
    DEFAULT_OUTPUT_DIR,
    run_deep_dive,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deep Bybit vs Binance pump-short research.")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--capital-usd", type=float, default=DEFAULT_CAPITAL_USD)
    parser.add_argument("--leg-notional-usd", type=float, default=DEFAULT_LEG_NOTIONAL_USD)
    parser.add_argument("--no-bybit-funding-backfill", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = run_deep_dive(
        input_root=args.input_root,
        output_dir=args.output_dir,
        capital_usd=args.capital_usd,
        leg_notional_usd=args.leg_notional_usd,
        backfill_bybit_funding=not args.no_bybit_funding_backfill,
    )
    print(
        "bybit/binance deep dive complete: "
        f"events={metadata['events']}, strategies={metadata['strategies']}, "
        f"summary_rows={metadata['strategy_summary_rows']}, output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
