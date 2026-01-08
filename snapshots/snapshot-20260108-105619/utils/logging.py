from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(log_dir: Path | None = None) -> None:
    """Configure console and file logging once."""

    log_dir = log_dir or Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "app.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

