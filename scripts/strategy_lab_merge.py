from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis_features.strategy_lab_merge import run_merge_pilot


if __name__ == "__main__":
    print(
        run_merge_pilot(
            local_dir=ROOT / "data" / "research" / "strategy_lab_local_archive",
            public_dir=ROOT / "data" / "research" / "strategy_lab_event_lake",
            output_dir=ROOT / "data" / "research" / "strategy_lab_merged_pilot",
        )
    )
