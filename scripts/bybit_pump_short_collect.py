from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_collectors.bybit_pump_short import (
    DEFAULT_OUTPUT_DIR,
    BybitCollectorConfig,
    BybitPumpShortCollector,
    normalize_symbol,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect slow Bybit public data for pump-short research."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Per-symbol lookback window for candles/funding/OI/long-short data.",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.8,
        help="Sleep after each public HTTP request. Keep this conservative for unattended runs.",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Collect only this many symbols from the selected universe.",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated Bybit symbols, e.g. ARXUSDT,REUSDT. Empty means all USDT perpetuals.",
    )
    parser.add_argument(
        "--oldest-first",
        action="store_true",
        help="Process oldest listings first. Default is newest listings first.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore done_symbols.txt and collect selected symbols again.",
    )
    parser.add_argument(
        "--continue-on-403",
        action="store_true",
        help="Retry 403 responses instead of stopping immediately. Usually leave off.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [normalize_symbol(item) for item in args.symbols.split(",") if normalize_symbol(item)]
    config = BybitCollectorConfig(
        output_dir=args.output_dir,
        sleep_sec=max(0.0, args.sleep_sec),
        lookback_days=max(1, args.lookback_days),
        stop_on_403=not args.continue_on_403,
    )
    collector = BybitPumpShortCollector(config)
    stats = collector.collect(
        symbols=symbols or None,
        max_symbols=args.max_symbols,
        newest_first=not args.oldest_first,
        resume=not args.no_resume,
    )
    print(
        "Bybit pump-short collection complete: "
        f"seen={stats.symbols_seen}, "
        f"collected={stats.symbols_collected}, "
        f"skipped={stats.symbols_skipped}, "
        f"failed={stats.symbols_failed}, "
        f"requests={stats.requests_made}, "
        f"output={config.output_dir}"
    )
    return 0 if stats.symbols_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
