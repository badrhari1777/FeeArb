from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from execution.settings import (
    ExecutionSettings,
    ExecutionSettingsManager,
    allocation_for_edge,
)


class ExecutionSettingsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.settings_path = Path(self.tmp_dir.name) / "execution_settings.json"
        self.manager = ExecutionSettingsManager(path=self.settings_path)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_defaults_written_on_first_load(self) -> None:
        self.assertTrue(self.settings_path.exists())
        with self.settings_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertIn("balance", data)
        self.assertIn("thresholds", data)

    def test_update_persists(self) -> None:
        self.manager.update({"balance": {"reserve_ratio": 0.25}})
        reloaded = ExecutionSettingsManager(path=self.settings_path)
        self.assertAlmostEqual(reloaded.current.balance.reserve_ratio, 0.25)

    def test_allocation_for_edge(self) -> None:
        settings = ExecutionSettings()
        self.assertGreater(allocation_for_edge(settings.allocation_rules, 0.006), 0.0)
        self.assertLess(
            allocation_for_edge(settings.allocation_rules, 0.002),
            allocation_for_edge(settings.allocation_rules, 0.008),
        )


if __name__ == "__main__":
    unittest.main()
