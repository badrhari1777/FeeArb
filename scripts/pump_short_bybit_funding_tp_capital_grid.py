from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_bybit_funding_tp_capital_grid import (  # noqa: E402
    DEFAULT_CAPITALS_USD,
    DEFAULT_EXCHANGE,
    DEFAULT_FUNDING_MIN_PCTS,
    DEFAULT_FUNDING_WINDOWS_H,
    DEFAULT_INPUT_ROOT,
    DEFAULT_LEVERAGE,
    DEFAULT_MAX_SLOTS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_TP_PCTS,
    run_bybit_funding_tp_capital_grid,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run focused Bybit pump-short funding/TP capital grid.")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE)
    parser.add_argument("--funding-windows-h", nargs="*", type=int, default=list(DEFAULT_FUNDING_WINDOWS_H))
    parser.add_argument("--funding-min-pcts", nargs="*", type=float, default=list(DEFAULT_FUNDING_MIN_PCTS))
    parser.add_argument("--tp-pcts", nargs="*", type=float, default=list(DEFAULT_TP_PCTS))
    parser.add_argument("--capitals-usd", nargs="*", type=float, default=list(DEFAULT_CAPITALS_USD))
    parser.add_argument("--max-slots", type=int, default=DEFAULT_MAX_SLOTS)
    parser.add_argument("--leverage", type=float, default=DEFAULT_LEVERAGE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = run_bybit_funding_tp_capital_grid(
        input_root=args.input_root,
        output_dir=args.output_dir,
        exchange=args.exchange,
        funding_windows_h=tuple(args.funding_windows_h),
        funding_min_pcts=tuple(args.funding_min_pcts),
        tp_pcts=tuple(args.tp_pcts),
        capitals_usd=tuple(args.capitals_usd),
        max_slots=args.max_slots,
        leverage=args.leverage,
    )
    print(
        "pump-short Bybit funding/TP capital grid complete: "
        f"outcomes={metadata['outcomes']}, "
        f"summary_rows={metadata['capital_slot_summary_rows']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
