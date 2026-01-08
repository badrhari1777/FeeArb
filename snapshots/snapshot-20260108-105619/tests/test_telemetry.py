from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from execution.settings import ExecutionSettings
from execution.telemetry import TelemetryClient


class TelemetryClientTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        settings = ExecutionSettings()
        settings.telemetry.structured_log_path = self.tmp_path / "events.jsonl"
        settings.telemetry.event_log_path = self.tmp_path / "events.log"
        self.telemetry = TelemetryClient(settings)
        await self.telemetry.start()

    async def asyncTearDown(self) -> None:
        await self.telemetry.stop()
        self.tmp.cleanup()

    async def test_emit_and_tail(self) -> None:
        captured = []

        async def _listener(entry):
            captured.append(entry)

        self.telemetry.register_listener(_listener)
        self.telemetry.emit("test_event", {"foo": "bar"})
        await asyncio.sleep(0.05)
        events = list(self.telemetry.tail())
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "test_event")
        self.assertEqual(captured[0]["payload"], {"foo": "bar"})
        with self.telemetry.log_path.open("r", encoding="utf-8") as handle:
            line = handle.readline()
        payload = json.loads(line)
        self.assertEqual(payload["event"], "test_event")


if __name__ == "__main__":
    unittest.main()
