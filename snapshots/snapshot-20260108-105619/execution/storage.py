from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any


class JsonStateStore:
    """Atomic JSON persistence helper with naive corruption handling."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def load(self, default: Any) -> Any:
        with self._lock:
            if not self._path.exists():
                return default
            try:
                raw = self._path.read_text(encoding="utf-8")
                return json.loads(raw)
            except (OSError, json.JSONDecodeError):
                return default

    def save(self, data: Any) -> None:
        payload = json.dumps(data, indent=2, sort_keys=True)
        tmp_path = self._path.with_suffix(".tmp")
        with self._lock:
            tmp_path.write_text(payload, encoding="utf-8")
            tmp_path.replace(self._path)
