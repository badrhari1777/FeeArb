from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from execution.state_replay_contract import audit_grid_state, audit_pump_state


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only persisted-state contract audit.")
    parser.add_argument("module", choices=("pump_live", "grid"))
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    try:
        payload = _load(args.path)
    except (OSError, json.JSONDecodeError) as exc:
        print(
            json.dumps(
                {
                    "module": args.module,
                    "valid": False,
                    "issues": [
                        {
                            "severity": "error",
                            "code": "state_read_failed",
                            "path": str(args.path),
                            "message": str(exc),
                        }
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    report = (
        audit_pump_state(payload)
        if args.module == "pump_live"
        else audit_grid_state(payload)
    )
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0 if report.valid else 2


if __name__ == "__main__":
    raise SystemExit(main())
