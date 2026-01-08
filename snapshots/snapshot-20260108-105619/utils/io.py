from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, Mapping


def save_csv(items: Iterable[Mapping[str, object]], path: Path) -> None:
    """Persist a collection of flat dictionaries to CSV."""

    path.parent.mkdir(parents=True, exist_ok=True)
    items_list = list(items)
    if not items_list:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(items_list[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(items_list)


def save_json(payload: object, path: Path) -> None:
    """Persist a Python object as JSON with indentation."""

    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(text, encoding="utf-8")

