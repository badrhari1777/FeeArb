from __future__ import annotations

import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path

APP_LOG_MAX_BYTES = 100 * 1024 * 1024
APP_LOG_BACKUP_COUNT = 5


def redact_sensitive_text(value: object) -> str:
    text = str(value)
    text = re.sub(
        r"(?i)([?&](?:signature|api[_-]?key|apikey|token|secret|passphrase)=)[^&\s]+",
        r"\1<redacted>",
        text,
    )
    text = re.sub(
        r'(?i)(["\'](?:signature|api[_-]?key|apikey|token|secret|passphrase)["\']\s*:\s*["\'])[^"\']+',
        r"\1<redacted>",
        text,
    )
    return text


class _SensitiveDataFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_sensitive_text(record.getMessage())
        record.args = ()
        return True


def _is_same_file_handler(handler: logging.Handler, log_path: Path) -> bool:
    if not isinstance(handler, logging.FileHandler):
        return False
    try:
        return Path(handler.baseFilename).resolve() == log_path.resolve()
    except Exception:
        return False


def _ensure_file_handler(logger: logging.Logger, log_path: Path, formatter: logging.Formatter) -> None:
    for handler in list(logger.handlers):
        if _is_same_file_handler(handler, log_path):
            if isinstance(handler, RotatingFileHandler):
                return
            logger.removeHandler(handler)
            handler.close()
    file_handler = RotatingFileHandler(
        log_path,
        mode="a",
        maxBytes=APP_LOG_MAX_BYTES,
        backupCount=APP_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(_SensitiveDataFilter())
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
