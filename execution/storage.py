from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
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


class JsonlEventStore:
    """Append-only JSONL persistence helper."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def append(self, data: Any) -> None:
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(payload)
                handle.write("\n")


class RotatingJsonlEventStore(JsonlEventStore):
    """Size-bounded JSONL store that keeps a small set of timestamped archives."""

    def __init__(
        self,
        path: Path | str,
        *,
        max_bytes: int,
        max_backups: int = 3,
    ) -> None:
        super().__init__(path)
        self._max_bytes = max(1, int(max_bytes))
        self._max_backups = max(0, int(max_backups))

    def _rotate_if_needed(self, incoming_bytes: int) -> None:
        try:
            current_size = self._path.stat().st_size
        except FileNotFoundError:
            return
        if current_size <= 0 or current_size + incoming_bytes <= self._max_bytes:
            return
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive = self._path.with_name(f"{self._path.stem}.{stamp}{self._path.suffix}")
        suffix = 1
        while archive.exists():
            archive = self._path.with_name(
                f"{self._path.stem}.{stamp}.{suffix}{self._path.suffix}"
            )
            suffix += 1
        self._path.replace(archive)
        if self._max_backups <= 0:
            archive.unlink(missing_ok=True)
            return
        archives = sorted(
            self._path.parent.glob(f"{self._path.stem}.*{self._path.suffix}"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        for stale in archives[self._max_backups :]:
            stale.unlink(missing_ok=True)

    def append(self, data: Any) -> None:
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        encoded_size = len(payload.encode("utf-8")) + 1
        with self._lock:
            self._rotate_if_needed(encoded_size)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(payload)
                handle.write("\n")
