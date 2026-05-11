from __future__ import annotations

import sys
from pathlib import Path

import uvicorn


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root))
    uvicorn.run(
        "webapp.app:app",
        host="127.0.0.1",
        port=8000,
        workers=1,
        reload=False,
    )
