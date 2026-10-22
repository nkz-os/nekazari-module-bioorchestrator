"""Compliance guard: the module must ship the platform-default AGPL-3.0 LICENSE.

The nkz platform is AGPL-3.0 (CLAUDE.md §VI). Sibling modules carry an AGPL
LICENSE file at the module root; bioorchestrator was missing one.
"""
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_license_file_present_and_agpl():
    license_path = _REPO_ROOT / "LICENSE"
    assert license_path.exists(), "module must ship a LICENSE file at its root"
    text = license_path.read_text(encoding="utf-8")
    assert "AFFERO GENERAL PUBLIC LICENSE" in text
    assert "Version 3" in text
