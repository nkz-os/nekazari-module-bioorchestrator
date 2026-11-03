"""Download Eurostat apro_cpshr (EU standard humidity) for every NUTS 2 region (or country, --level country) and the mapped crops.

Writes data/eurostat/apro_cpshr_<nuts2|country>_<date>.json.gz: the raw JSON-stat document plus the
request URL, the retrieval time and Eurostat's dataset update time (provenance).
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import sys
import urllib.request
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.eurostat_yields import (
    DATASET,
    GEO_LEVELS,
    NUTS_VERSION_NOTE,
    request_url,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Snapshot Eurostat regional crop statistics")
    parser.add_argument("--level", choices=GEO_LEVELS, default="nuts2",
                        help="geographic level: NUTS 2 regions (default) or countries")
    parser.add_argument("--data-dir", type=Path, default=Path(os.getenv("DATA_DIR", "./data")))
    args = parser.parse_args()

    crop_map = yaml.safe_load((args.data_dir / "eurostat" / "crop_codes.yaml").read_text())
    url = request_url(list(crop_map["crops"]), args.level)
    with urllib.request.urlopen(url, timeout=180) as resp:
        doc = json.load(resp)
    now = dt.datetime.now(dt.UTC)
    snapshot = {
        "dataset": DATASET,
        "request_url": url,
        "retrieved_at": now.isoformat(timespec="seconds"),
        "dataset_updated": doc.get("updated"),
        "nuts_version": NUTS_VERSION_NOTE,
        "jsonstat": doc,
    }
    out = args.data_dir / "eurostat" / f"{DATASET}_{args.level}_{now.date().isoformat()}.json.gz"
    with gzip.open(out, "wt", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
    print(f"[fetch_eurostat_yields] {len(doc.get('value') or {})} values -> {out}")


if __name__ == "__main__":
    main()
