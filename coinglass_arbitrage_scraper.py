"""Standalone helper to dump Coinglass arbitrage rows as JSON."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from parsers.coinglass import fetch_rows


def main() -> None:
    rows = fetch_rows(limit=20)
    payload = [asdict(row) for row in rows]
    text = json.dumps(payload, ensure_ascii=False, indent=2)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path("data/coinglass")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"coinglass_scraper_{timestamp}.json"
    output_path.write_text(text, encoding="utf-8")

    print(text)
    print(f"\nSaved snapshot to {output_path}")


if __name__ == "__main__":
    main()
