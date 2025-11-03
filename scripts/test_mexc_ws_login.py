from __future__ import annotations

import asyncio
import sys

from exchanges.mexc import MexcAdapter


async def _run() -> int:
    adapter = MexcAdapter()
    if not adapter.has_api_credentials:
        print("MEXC credentials are not configured in environment or .env", file=sys.stderr)
        return 1
    print("Attempting MEXC websocket login...")
    try:
        success = await adapter.test_private_connection()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Login attempt raised an exception: {exc!r}", file=sys.stderr)
        return 2
    if success:
        print("MEXC websocket authentication succeeded.")
        return 0
    print("MEXC websocket authentication failed (see logs for details).", file=sys.stderr)
    return 3


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
