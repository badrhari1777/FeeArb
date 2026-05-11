from __future__ import annotations

import logging
from pathlib import Path


def _is_same_file_handler(handler: logging.Handler, log_path: Path) -> bool:
    if not isinstance(handler, logging.FileHandler):
        return False
    try:
        return Path(handler.baseFilename).resolve() == log_path.resolve()
    except Exception:
        return False


def _ensure_file_handler(logger: logging.Logger, log_path: Path, formatter: logging.Formatter) -> None:
    for handler in logger.handlers:
        if _is_same_file_handler(handler, log_path):
            return
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def setup_logging(log_dir: Path | None = None) -> None:
    """Configure console and file logging, including uvicorn process logs."""

    log_dir = log_dir or Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = (log_dir / "app.log").resolve()

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    if root.level in (logging.NOTSET, 0) or root.level > logging.INFO:
        root.setLevel(logging.INFO)

    # If nothing configured yet, keep default console output.
    if not root.handlers:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)

    # Always ensure app.log receives records as a console mirror.
    _ensure_file_handler(root, log_path, formatter)

    # Uvicorn loggers may bypass root via propagate=False; mirror those too.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(name)
        if not uv_logger.propagate:
            _ensure_file_handler(uv_logger, log_path, formatter)
