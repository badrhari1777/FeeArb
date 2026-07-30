from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.pump_live_margin_stress import write_bank_margin_stress  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a deterministic BANK Pump Live margin stress report.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/research/pump_live_margin_stress"),
    )
    args = parser.parse_args()
    report = write_bank_margin_stress(args.output_dir)
    summary = report["summary"]
    print(
        "required_extra_for_ladder_usd="
        f"{summary['required_extra_for_ladder_usd']}"
    )
    print(
        "rounded_topup_for_ladder_usd="
        f"{summary['rounded_topup_for_ladder_usd']}"
    )
    print(
        "full_rescue_shortfall_usd="
        f"{summary['full_rescue_shortfall_usd']}"
    )


if __name__ == "__main__":
    main()
