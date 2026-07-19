from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analysis_collectors.ccxt_pump_short_history import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT,
    CcxtPumpShortCollectorConfig,
    CcxtPumpShortHistoryCollector,
    EXCHANGE_IDS,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect public pump-short history via ccxt.")
    parser.add_argument("--exchange", required=True, choices=sorted(EXCHANGE_IDS))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start", default="2024-01-01", help="UTC date, default 2024-01-01")
    parser.add_argument("--end", default="", help="UTC date, optional")
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--no-prefilter", action="store_true")
    parser.add_argument("--min-daily-pump-pct", type=float, default=50.0)
    parser.add_argument("--min-3d-pump-pct", type=float, default=100.0)
    parser.add_argument("--min-7d-pump-pct", type=float, default=180.0)
    return parser.parse_args()


def parse_date_ms(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def main() -> int:
    args = parse_args()
    config = CcxtPumpShortCollectorConfig(
        exchange=args.exchange,
        output_root=args.output_root,
        start_ms=parse_date_ms(args.start) or 0,
        end_ms=parse_date_ms(args.end),
        sleep_sec=args.sleep_sec,
        daily_prefilter=not args.no_prefilter,
        min_daily_pump_pct=args.min_daily_pump_pct,
        min_3d_pump_pct=args.min_3d_pump_pct,
        min_7d_pump_pct=args.min_7d_pump_pct,
    )
    collector = CcxtPumpShortHistoryCollector(config)
    stats = collector.collect(symbols=args.symbols, max_symbols=args.max_symbols, resume=not args.no_resume)
    print(
        "pump-short collect complete: "
        f"exchange={args.exchange}, seen={stats.symbols_seen}, collected={stats.symbols_collected}, "
        f"prefiltered={stats.symbols_prefiltered}, skipped={stats.symbols_skipped}, "
        f"failed={stats.symbols_failed}, requests={stats.requests_made}, output={collector.output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
