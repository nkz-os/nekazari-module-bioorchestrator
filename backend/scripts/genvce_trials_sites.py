"""Field sites of the GENVCE results map for ``data/registries/sites.yaml``.

The results map (open data, see the GENVCE-TRIALS source) publishes one locality record per trial place with
its coordinates. This script turns the localities of its index (``data/results_map/index.json`` of the raw
data repository) into registry entries:

  * one ``field`` site per locality, id ``ES-GENVCE-TRIALS-<SLUG>``, the published coordinates rounded to
    five decimals (the registry's precision policy: about 1 m, never a surveyed point) and a
    ``coordinate_source`` naming the record and the download date, ``status: assumption`` (whether the point
    marks the trial field or the municipality is unconfirmed);
  * a locality whose name already resolves in the registry is NOT generated (the site index refuses two
    entries with the same name or alias): it is reported as a collision with the existing id and the
    published coordinates, for a hand edit of that entry.

Re-running against a registry that already holds the entries prints nothing (idempotent). The output is
pasted at the end of ``sites.yaml``; the file is never rewritten by the script.

Usage:
  python scripts/genvce_trials_sites.py --index <index.json> [--sites data/registries/sites.yaml]
                                        [--download-date YYYY-MM-DD] [--out -]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.kg.registries import DEFAULT_REGISTRIES_PATH, Registries, load_registries

ID_PREFIX = "ES-GENVCE-TRIALS-"
COORDINATE_DECIMALS = 5  # registry policy: municipality/field level, never a surveyed point
SOURCE_ID = "GENVCE-TRIALS"
NOTE = ("ASSUMPTION: coordinates as published by the GENVCE results map; whether they mark the trial field or "
        "the municipality is unconfirmed (0.9-5.8 km from town centres where compared).")


def slug_id(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^A-Z0-9]+", "-", ascii_name.upper()).strip("-")
    if not slug:
        raise ValueError(f"no slug for locality name {name!r}")
    return ID_PREFIX + slug


def _entry(locality: dict[str, Any], download_date: str) -> dict[str, Any]:
    return {
        "id": slug_id(locality["name"]),
        "name": locality["name"],
        "site_kind": "field",
        "country": "ES",
        "aliases": [],
        "latitude": round(float(locality["latitude"]), COORDINATE_DECIMALS),
        "longitude": round(float(locality["longitude"]), COORDINATE_DECIMALS),
        "coordinate_source": (f"GENVCE results map, localidad record id {locality['id']} (download {download_date}), "
                              f"rounded to {COORDINATE_DECIMALS} decimals"),
        "sources": [SOURCE_ID],
        "status": "assumption",
        "note": NOTE,
    }


def plan_entries(index: dict[str, Any], registries: Registries, *, download_date: str,
                 skip_registered: bool = True) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``(new entries, collisions)``; with ``skip_registered`` false every locality becomes an entry."""
    new: list[dict[str, Any]] = []
    collisions: list[dict[str, Any]] = []
    for locality in sorted(index["localities"], key=lambda loc: loc["name"]):
        existing = registries.site(locality["name"])
        if existing is not None and not existing.id.startswith(ID_PREFIX):
            collisions.append({
                "locality": locality["name"], "existing_id": existing.id,
                "latitude": round(float(locality["latitude"]), COORDINATE_DECIMALS),
                "longitude": round(float(locality["longitude"]), COORDINATE_DECIMALS),
                "coordinate_source": _entry(locality, download_date)["coordinate_source"],
            })
            if skip_registered:
                continue
        if existing is not None and skip_registered:
            continue
        new.append(_entry(locality, download_date))
    return new, collisions


def render(entries: list[dict[str, Any]]) -> str:
    """The entries as list items indented for the ``sites:`` block of sites.yaml."""
    out: list[str] = []
    for entry in entries:
        text = yaml.safe_dump([entry], allow_unicode=True, sort_keys=False, width=110)
        out.append("\n".join("  " + line for line in text.rstrip("\n").split("\n")))
    return "\n".join(out) + ("\n" if out else "")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--index", required=True, help="results map index.json")
    parser.add_argument("--sites", default=str(DEFAULT_REGISTRIES_PATH / "sites.yaml"))
    parser.add_argument("--download-date", default=None, help="defaults to the index's downloaded_at")
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)
    index = json.loads(Path(args.index).read_text(encoding="utf-8"))
    registries = load_registries(Path(args.sites).parent)
    download_date = args.download_date or index["downloaded_at"]
    new, collisions = plan_entries(index, registries, download_date=download_date)
    text = render(new)
    if args.out == "-":
        sys.stdout.write(text)
    else:
        Path(args.out).write_text(text, encoding="utf-8")
    for collision in collisions:
        print(f"collision: {collision['locality']!r} already resolves to {collision['existing_id']}; "
              f"published coordinates {collision['latitude']}, {collision['longitude']} "
              f"({collision['coordinate_source']}) — edit that entry by hand", file=sys.stderr)
    print(f"{len(new)} new entries, {len(collisions)} collisions", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
