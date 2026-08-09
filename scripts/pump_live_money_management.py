from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_live_money_management import write_report  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay four-slot Pump Live money management.")
    parser.add_argument(
        "--historical-trades",
        type=Path,
        default=Path("data/research/pump_live_transition_research/historical_strategy_trades.csv"),
    )
    parser.add_argument(
        "--live-state",
        type=Path,
        default=Path("data/research/bybit_pump_short_live/live_state.json"),
    )
    parser.add_argument("--wallet-total-usd", type=float)
    parser.add_argument("--wallet-available-usd", type=float)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/research/pump_live_money_management"),
    )
    args = parser.parse_args()
    result = write_report(
        historical_trades_path=args.historical_trades,
        output_dir=args.output_dir,
        live_state_path=args.live_state,
        wallet_total_usd=args.wallet_total_usd,
        wallet_available_usd=args.wallet_available_usd,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
