from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_features.pump_short_shadow_multiexchange import (  # noqa: E402
    DEFAULT_EXCHANGES,
    DEFAULT_OUTPUT_DIR,
    ShadowScanConfig,
    run_shadow_scan,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run pump-short shadow scan for Binance/Bybit.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--exchanges", nargs="*", default=list(DEFAULT_EXCHANGES), choices=["binance", "bybit"])
    parser.add_argument("--lookback-days", type=int, default=21)
    parser.add_argument("--recent-event-hours", type=int, default=168)
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    parser.add_argument("--max-symbols-per-exchange", type=int, default=None)
    parser.add_argument("--symbols", nargs="*", default=())
    parser.add_argument("--no-prefilter", action="store_true")
    parser.add_argument("--min-daily-pump-pct", type=float, default=35.0)
    parser.add_argument("--min-3d-pump-pct", type=float, default=70.0)
    parser.add_argument("--min-7d-pump-pct", type=float, default=120.0)
    parser.add_argument("--leg-notional-usd", type=float, default=1000.0)
    parser.add_argument("--orderbook-slippage-bps", type=float, default=20.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = run_shadow_scan(
        ShadowScanConfig(
            output_dir=args.output_dir,
            exchanges=tuple(args.exchanges),
            lookback_days=args.lookback_days,
            recent_event_hours=args.recent_event_hours,
            sleep_sec=args.sleep_sec,
            max_symbols_per_exchange=args.max_symbols_per_exchange,
            symbols=tuple(args.symbols or ()),
            daily_prefilter=not args.no_prefilter,
            min_daily_pump_pct=args.min_daily_pump_pct,
            min_3d_pump_pct=args.min_3d_pump_pct,
            min_7d_pump_pct=args.min_7d_pump_pct,
            leg_notional_usd=args.leg_notional_usd,
            orderbook_slippage_bps=args.orderbook_slippage_bps,
        )
    )
    print(
        "pump-short shadow scan complete: "
        f"exchanges={','.join(metadata['exchanges'])}, "
        f"rows={metadata['rows']}, "
        f"entry_candidates={metadata['entry_candidates']}, "
        f"samples={metadata['samples_written']}, "
        f"errors={metadata['errors']}, "
        f"requests={metadata['requests_made']}, "
        f"elapsed_sec={metadata['elapsed_sec']}, "
        f"output={metadata['output_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
