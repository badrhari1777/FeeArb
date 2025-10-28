"""Standalone helper to dump Coinglass arbitrage rows as JSON."""

from __future__ import annotations

import json
from dataclasses import asdict

from parsers.coinglass import fetch_rows


def main() -> None:
    rows = fetch_rows(limit=20)
    payload = [asdict(row) for row in rows]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
