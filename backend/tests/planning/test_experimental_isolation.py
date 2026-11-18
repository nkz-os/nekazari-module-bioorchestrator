"""The experimental phenology model feeds nothing operative (spec 2026-11-18, decision 1)."""
import ast
from pathlib import Path

APP = Path(__file__).parents[2] / "app"
FORBIDDEN = ("app.services.phenology_vp", "app.planning.phenology_experimental")
ALLOWED = {"planning/phenology_dates.py", "planning/phenology_experimental.py"}


def _imports(path: Path) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
            out.update(f"{node.module}.{a.name}" for a in node.names)
    return out


def test_experimental_not_used_by_operative_consumers():
    offenders = []
    for path in sorted(APP.rglob("*.py")):
        rel = path.relative_to(APP).as_posix()
        if rel in ALLOWED or rel == "services/phenology_vp.py":
            continue
        hits = {i for i in _imports(path) if i in FORBIDDEN or i.startswith(tuple(f + "." for f in FORBIDDEN))}
        if hits:
            offenders.append((rel, sorted(hits)))
    assert offenders == []


def test_operative_modules_are_scanned():
    for rel in ("services/engines/aquacrop_engine.py", "planning/late_frost.py", "services/crop_calendar.py",
                "services/crop_simulation.py", "planning/in_season.py"):
        assert (APP / rel).exists(), rel
