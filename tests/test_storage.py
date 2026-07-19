from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.storage import RotatingJsonlEventStore


class RotatingJsonlEventStoreTestCase(unittest.TestCase):
    def test_rotates_oversized_jsonl_before_append(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "events.jsonl"
            store = RotatingJsonlEventStore(
                path,
                max_bytes=40,
                max_backups=2,
            )

            store.append({"value": "a" * 30})
            store.append({"value": "b" * 30})

            archives = list(path.parent.glob("events.*.jsonl"))
            self.assertEqual(len(archives), 1)
            self.assertIn("b" * 30, path.read_text(encoding="utf-8"))
            self.assertIn("a" * 30, archives[0].read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
