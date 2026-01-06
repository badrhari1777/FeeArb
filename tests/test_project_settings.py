from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from project_settings import SettingsManager
from webapp.services import DataService


class ProjectSettingsTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.settings_path = Path(self.tmp_dir.name) / "settings.json"
        self.manager = SettingsManager(path=self.settings_path)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_protective_toggles_persist(self) -> None:
        """Ensure disabling protective toggles survives reload."""
        self.manager.update(
            {"protective": {"auto_protect_enabled": False, "auto_take_enabled": False}}
        )
        reloaded = SettingsManager(path=self.settings_path)
        protective = reloaded.current.protective
        self.assertFalse(protective.get("auto_protect_enabled"))
        self.assertFalse(protective.get("auto_take_enabled"))

    async def test_protective_sync_skipped_when_disabled(self) -> None:
        """Protective sync should short-circuit when both toggles are off."""
        self.manager.update(
            {"protective": {"auto_protect_enabled": False, "auto_take_enabled": False}}
        )
        service = DataService(settings_manager=self.manager)

        class _SentinelProtective:
            called = False

            async def sync_protective_orders(self, *args, **kwargs):
                self.called = True
                return []

        service._protective_manager = _SentinelProtective()  # type: ignore[attr-defined]
        # _maybe_sync_protective_orders reads snapshot to enumerate positions; keep it minimal.
        service._accounts = type("X", (), {"snapshot": lambda self=None: {}})()  # type: ignore

        await service._maybe_sync_protective_orders()
        self.assertFalse(service._protective_manager.called)  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
