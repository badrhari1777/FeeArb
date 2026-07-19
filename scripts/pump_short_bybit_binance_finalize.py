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
    finalize_deep_dive_from_existing,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize Bybit/Binance deep-dive report from existing summaries.")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--capital-usd", type=float, default=DEFAULT_CAPITAL_USD)
    parser.add_argument("--leg-notional-usd", type=float, default=DEFAULT_LEG_NOTIONAL_USD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = finalize_deep_dive_from_existing(
        input_root=args.input_root,
        output_dir=args.output_dir,
        capital_usd=args.capital_usd,
        leg_notional_usd=args.leg_notional_usd,
    )
    print(
        "bybit/binance deep dive finalize complete: "
        f"selected={metadata['selected_strategies']}, capital_rows={metadata['capital_selected_rows']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
