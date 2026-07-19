from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
EXCHANGES = ("binance", "bybit", "okx", "gate", "bitget", "mexc", "kucoin")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run unattended pump-short collection and comparison.")
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    parser.add_argument("--output-root", default="data/research/pump_short_multiexchange")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--exchanges", nargs="*", default=list(EXCHANGES))
    parser.add_argument("--skip-collect", action="store_true")
    parser.add_argument("--no-prefilter", action="store_true")
    return parser.parse_args()


def run(cmd: list[str]) -> None:
    print("RUN", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT_DIR, check=False)


def main() -> int:
    args = parse_args()
    python = str(ROOT_DIR / ".venv" / "Scripts" / "python.exe")
    if not Path(python).exists():
        python = sys.executable
    exchanges = [item for item in args.exchanges if item != "bingx"]
    if not args.skip_collect:
        for exchange in exchanges:
            cmd = [
                python,
                "scripts/pump_short_collect_exchange.py",
                "--exchange",
                exchange,
                "--start",
                args.start,
                "--sleep-sec",
                str(args.sleep_sec),
                "--output-root",
                args.output_root,
            ]
            if args.max_symbols is not None:
                cmd.extend(["--max-symbols", str(args.max_symbols)])
            if args.no_prefilter:
                cmd.append("--no-prefilter")
            run(cmd)
    run(
        [
            python,
            "scripts/pump_short_cross_exchange_research.py",
            "--input-root",
            args.output_root,
            "--output-dir",
            str(Path(args.output_root) / "_comparison"),
        ]
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
