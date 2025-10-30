"""Runtime configuration for the funding arbitrage monitor.

This module centralises user-adjustable settings that are persisted on disk.
It exposes a small manager responsible for validating, loading and saving the
settings.  Only non-sensitive values belong here - credentials should stay in
environment variables or `.env`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Dict, Final, Iterable, Mapping

from config import BASE_DIR, SUPPORTED_EXCHANGES

_DEFAULT_SETTINGS_PATH: Final[Path] = BASE_DIR / "data" / "settings.json"

DEFAULT_SOURCES: Final[Dict[str, bool]] = {
    "arbitragescanner": True,
    "coinglass": True,
}

DEFAULT_EXCHANGES: Final[Dict[str, bool]] = {
    name: True for name in SUPPORTED_EXCHANGES
}

MIN_REFRESH_SECONDS: Final[int] = 30
MAX_REFRESH_SECONDS: Final[int] = 24 * 60 * 60  # one day


def _normalise_bool_map(
    baseline: Mapping[str, bool],
    incoming: Mapping[str, object] | None,
    *,
    allow_new_keys: bool = True,
) -> Dict[str, bool]:
    """Return a bool map starting from the baseline and applying incoming keys."""
    result = dict(baseline)
    if not incoming:
        return result
    for key, value in incoming.items():
        if not allow_new_keys and key not in result:
            continue
        result[key] = bool(value)
    return result


@dataclass(slots=True)
class AppSettings:
    """In-memory representation of persisted application settings."""

    sources: Dict[str, bool] = field(
        default_factory=lambda: dict(DEFAULT_SOURCES)
    )
    exchanges: Dict[str, bool] = field(
        default_factory=lambda: dict(DEFAULT_EXCHANGES)
    )
    parser_refresh_seconds: int = 300
    table_refresh_seconds: int = 300

    def with_updates(self, payload: Mapping[str, object]) -> "AppSettings":
        """Return a new settings instance with the provided updates applied."""
        updated = replace(self)
        if "sources" in payload:
            updated.sources = _normalise_bool_map(
                DEFAULT_SOURCES, payload["sources"]
            )
        else:
            updated.sources = _normalise_bool_map(
                DEFAULT_SOURCES, self.sources, allow_new_keys=True
            )
        if "exchanges" in payload:
            updated.exchanges = _normalise_bool_map(
                _default_exchanges(), payload["exchanges"]
            )
        else:
            updated.exchanges = _normalise_bool_map(
                _default_exchanges(), self.exchanges, allow_new_keys=True
            )
        updated.parser_refresh_seconds = int(
            payload.get("parser_refresh_seconds", self.parser_refresh_seconds)
        )
        updated.table_refresh_seconds = int(
            payload.get("table_refresh_seconds", self.table_refresh_seconds)
        )
        return updated.normalised()

    def normalised(self) -> "AppSettings":
        """Ensure the settings align with the latest defaults."""
        self.sources = _normalise_bool_map(DEFAULT_SOURCES, self.sources)
        self.exchanges = _normalise_bool_map(
            _default_exchanges(), self.exchanges
        )
        return self

    def validate(self) -> None:
        """Validate invariants, raising ValueError if anything is invalid."""
        if not any(self.sources.values()):
            raise ValueError("At least one data source must remain enabled.")
        if not any(self.exchanges.values()):
            raise ValueError("At least one exchange must remain enabled.")
        if (
            self.parser_refresh_seconds < MIN_REFRESH_SECONDS
            or self.table_refresh_seconds < MIN_REFRESH_SECONDS
        ):
            raise ValueError(
                f"Refresh intervals must be >= {MIN_REFRESH_SECONDS} seconds."
            )
        if (
            self.parser_refresh_seconds > MAX_REFRESH_SECONDS
            or self.table_refresh_seconds > MAX_REFRESH_SECONDS
        ):
            raise ValueError(
                f"Refresh intervals must be <= {MAX_REFRESH_SECONDS} seconds."
            )

    def to_dict(self) -> Dict[str, object]:
        return {
            "sources": dict(self.sources),
            "exchanges": dict(self.exchanges),
            "parser_refresh_seconds": self.parser_refresh_seconds,
            "table_refresh_seconds": self.table_refresh_seconds,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object] | None) -> "AppSettings":
        if not payload:
            return cls()
        instance = cls()
        instance = instance.with_updates(payload)
        return instance.normalised()


def _default_exchanges() -> Dict[str, bool]:
    return dict(DEFAULT_EXCHANGES)


class SettingsManager:
    """Thin wrapper around settings persistence."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or _DEFAULT_SETTINGS_PATH
        self._settings = self._load()

    @property
    def current(self) -> AppSettings:
        return self._settings

    def as_dict(self) -> Dict[str, object]:
        return self._settings.to_dict()

    def update(self, payload: Mapping[str, object]) -> AppSettings:
        candidate = self._settings.with_updates(payload)
        candidate.validate()
        self._settings = candidate.normalised()
        self.save()
        return self._settings

    def set(self, new_settings: AppSettings) -> None:
        new_settings.validate()
        self._settings = new_settings.normalised()
        self.save()

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(self._settings.to_dict(), handle, indent=2)

    def _load(self) -> AppSettings:
        if not self._path.exists():
            return AppSettings()
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            raise ValueError(f"Failed to load settings: {exc}") from exc
        return AppSettings.from_dict(data)

    def reload(self) -> AppSettings:
        self._settings = self._load()
        return self._settings

    def enabled_sources(self) -> Dict[str, bool]:
        return dict(self._settings.sources)

    def enabled_exchanges(self) -> Dict[str, bool]:
        return dict(self._settings.exchanges)

    def refresh_intervals(self) -> tuple[int, int]:
        return (
            self._settings.parser_refresh_seconds,
            self._settings.table_refresh_seconds,
        )

