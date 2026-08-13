from __future__ import annotations

import ast
from pathlib import Path

from webapp.service_lifecycle import RuntimeLifecycleMixin
from webapp.services import DataService


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOTS = (
    ROOT / "webapp",
    ROOT / "execution",
    ROOT / "risk",
)
RETIRED_MODULES = {
    "execution.auto_strategies",
    "risk.derisk_manager",
}


def _python_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_retired_trading_engines_have_no_production_import_path() -> None:
    references: list[str] = []
    for root in PRODUCTION_ROOTS:
        for path in root.rglob("*.py"):
            for imported in _python_imports(path):
                if imported in RETIRED_MODULES:
                    references.append(f"{path.relative_to(ROOT)} -> {imported}")

    assert references == []
    assert not (ROOT / "execution" / "auto_strategies.py").exists()
    assert not (ROOT / "risk" / "derisk_manager.py").exists()


def test_data_service_uses_extracted_runtime_lifecycle_owner() -> None:
    assert issubclass(DataService, RuntimeLifecycleMixin)
    assert DataService.startup.__module__ == "webapp.service_lifecycle"
    assert DataService.shutdown.__module__ == "webapp.service_lifecycle"
