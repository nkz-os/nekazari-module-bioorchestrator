"""The experimental phenology model feeds nothing operative (spec 2026-11-18, decision 1)."""
import ast
from pathlib import Path

APP = Path(__file__).parents[2] / "app"
FORBIDDEN = ("app.services.phenology_vp", "app.planning.phenology_experimental", "app.planning.cereal_type")
ALLOWED = {"planning/phenology_dates.py", "planning/phenology_experimental.py", "planning/cereal_type.py"}


def _imports(path: Path, root: Path = APP) -> set[str]:
    """Absolute dotted names imported, relative imports resolved against the file's package."""
    pkg = ["app", *path.relative_to(root).parent.parts]
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = pkg[: len(pkg) - (node.level - 1)] if node.level else []
            mod = ".".join([*base, *([node.module] if node.module else [])])
            if not mod:
                continue
            out.add(mod)
            out.update(f"{mod}.{a.name}" for a in node.names)
    return out


def test_experimental_not_used_by_operative_consumers():
    offenders = []
    for path in sorted(APP.rglob("*.py")):
        rel = path.relative_to(APP).as_posix()
        if rel in ALLOWED or rel == "services/phenology_vp.py":
            continue
        forbidden = FORBIDDEN
        if rel == "api/v1/planning.py":  # the route builds the cereal-type fetcher, nothing else
            forbidden = tuple(f for f in FORBIDDEN if f != "app.planning.cereal_type")
        hits = {i for i in _imports(path) if i in forbidden or i.startswith(tuple(f + "." for f in forbidden))}
        if hits:
            offenders.append((rel, sorted(hits)))
    assert offenders == []


def test_relative_imports_are_detected(tmp_path):
    (tmp_path / "planning").mkdir()
    (tmp_path / "services").mkdir()
    p1, p2 = tmp_path / "planning" / "x.py", tmp_path / "services" / "y.py"
    p1.write_text("from .phenology_experimental import a\nfrom .cereal_type import b\n")
    p2.write_text("from . import phenology_vp\n")
    assert "app.planning.phenology_experimental" in _imports(p1, tmp_path)
    assert "app.planning.cereal_type" in _imports(p1, tmp_path)
    assert "app.services.phenology_vp" in _imports(p2, tmp_path)


def test_operative_modules_are_scanned():
    for rel in ("services/engines/aquacrop_engine.py", "planning/late_frost.py", "services/crop_calendar.py",
                "services/crop_simulation.py", "planning/in_season.py"):
        assert (APP / rel).exists(), rel
