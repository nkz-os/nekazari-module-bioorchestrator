#!/usr/bin/env python3
"""Convert IFAPA/ITACyL extracted JSON to JSON-LD for NavarraIngester.

Usage:
    python scripts/convert_ifapa_itacyl_to_jsonld.py \
        --ifapa /data/ifapa_enriched.json \
        --itacyl /data/itacyl_cleaned.json \
        --output /data/ifapa_itacyl_trials.jsonld
"""

import json
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────────────────

CTX = "https://nkz.robotika.cloud/ngsi-ld/bioorchestrator-context.jsonld"

# Climate class → NKZ URI
CLIMATE_URIS = {
    "Csa": "nkz:Koppen/Csa",
    "Csb": "nkz:Koppen/Csb",
    "BSk": "nkz:Koppen/BSk",
    "BSh": "nkz:Koppen/BSh",
    "Cfb": "nkz:Koppen/Cfb",
}

# Irrigation → URI
IRRIG_URIS = {
    "secano": "nkz:Irrigation/Rainfed",
    "regadío": "nkz:Irrigation/Irrigated",
    "rainfed": "nkz:Irrigation/Rainfed",
    "irrigated": "nkz:Irrigation/Irrigated",
    "conventional": "nkz:Irrigation/Rainfed",
}

EPPO_SCIENTIFIC = {
    "CIEAR": "Cicer arietinum", "LENCU": "Lens culinaris",
    "LTHSA": "Lathyrus sativus", "PIBAR": "Pisum sativum",
    "VICER": "Vicia ervilia", "VICFX": "Vicia faba",
    "VICNA": "Vicia narbonensis", "VICSA": "Vicia sativa",
    "SOYBN": "Glycine max", "AVESA": "Avena sativa",
    "SECCE": "Secale cereale", "HORVX": "Hordeum vulgare",
    "TRZAX": "Triticum aestivum", "ZEAMX": "Zea mays",
}

# Known trial sites with coordinates
KNOWN_SITES = {
    "Andalucía":       {"name": "Andalucía (IFAPA)",   "climate": "BSh", "lat": 37.5, "lon": -4.5},
    "Castilla y León": {"name": "Castilla y León",      "climate": "BSk", "lat": 41.6, "lon": -4.7},
    "Palencia":        {"name": "Palencia",             "climate": "BSk", "lat": 42.0, "lon": -4.5},
    "Zamadueñas":      {"name": "Zamadueñas",           "climate": "BSk", "lat": 41.5, "lon": -4.7},
    "Córdoba (IFAPA)": {"name": "Córdoba (IFAPA)",      "climate": "BSh", "lat": 37.9, "lon": -4.8},
}


def convert_to_jsonld(ifapa_path: str, itacyl_path: str) -> dict:
    """Convert both extracted JSON files to a single JSON-LD document."""
    graph = []
    site_registry: dict[str, str] = {}  # location_name -> @id
    article_registry: dict[str, str] = {}  # article_key -> @id
    article_counter = 0

    def get_or_create_site(name: str, climate: str, source: str) -> str:
        """Get or create TrialSite node, return its @id."""
        key = name.lower().strip()
        if key in site_registry:
            return site_registry[key]

        site_id = f"urn:nkz:site:{key.replace(' ', '_').replace('(', '').replace(')', '')}"

        # Try to find known site info
        known = KNOWN_SITES.get(name, {})
        site_node = {
            "@id": site_id,
            "@type": "TrialSite",
            "name": known.get("name", name),
            "municipality": name,
            "agroclimatic_zone": CLIMATE_URIS.get(climate, f"nkz:Koppen/{climate}"),
            "climateClass": climate,
            "latitude": known.get("lat"),
            "longitude": known.get("lon"),
            "annualRainfallMm": None,
            "soilType": None,
            "soilTexture": None,
            "dataSource": source,
        }
        graph.append(site_node)
        site_registry[key] = site_id
        return site_id

    def get_or_create_article(source: str, title: str, year: int) -> str:
        """Get or create ArticleSource node."""
        nonlocal article_counter
        key = f"{source}|{title}|{year}"
        if key in article_registry:
            return article_registry[key]

        article_counter += 1
        art_id = f"urn:nkz:{source.lower()}:report:{year}_{title[:30].replace(' ', '_')}"
        art_node = {
            "@id": art_id,
            "@type": "ArticleSource",
            "source": source,
            "issue_number": year,
            "article_title": title,
            "year": year,
            "article_author": "",
            "topic": "Experimentación",
            "confidence": "medium",
        }
        graph.append(art_node)
        article_registry[key] = art_id
        return art_id

    # ── Process IFAPA records ─────────────────────────────────────────
    if ifapa_path and Path(ifapa_path).exists():
        with open(ifapa_path) as f:
            ifapa_data = json.load(f)

        for i, rec in enumerate(ifapa_data):
            eppo = (rec.get("species_eppo") or "").strip().upper()
            if not eppo or len(eppo) != 5:
                continue

            yield_val = rec.get("yield_kg_ha")
            if not yield_val or yield_val <= 0:
                continue

            location = rec.get("location", "Andalucía")
            climate = rec.get("climate_class", "BSh")
            campaign = str(rec.get("campaign", "2020"))
            year_match = __import__("re").search(r"(\d{4})", campaign)
            year = int(year_match.group(1)) if year_match else 2020
            variety = (rec.get("variety") or "unknown").strip().lower()
            source_pdf = rec.get("source_pdf", "")
            source = "IFAPA"
            title = f"IFAPA leguminosas {campaign}"

            site_id = get_or_create_site(location, climate, source)
            art_id = get_or_create_article(source, title, year)
            trial_id = f"urn:nkz:{source.lower()}:trial:{year}:{eppo.lower()}:{variety[:20]}"

            trial_node = {
                "@id": trial_id,
                "@type": "VarietyTrial",
                "crop_eppo": f"eppo:{eppo}",
                "crop_scientific": EPPO_SCIENTIFIC.get(eppo, ""),
                "variety": variety,
                "agroclimatic_zone": CLIMATE_URIS.get(climate, f"nkz:Koppen/{climate}"),
                "year": year,
                "yield_kg_ha": yield_val,
                "yield_relative_pct": None,
                "quality_params": {},
                "disease_scores": {},
                "irrigation_regime": IRRIG_URIS.get("rainfed"),
                "trial_location": location,
                "confidence": "medium",
                "refArticleSource": {"@id": art_id},
                "refTrialSite": {"@id": site_id},
                "metadata": {
                    "source": source,
                    "issue_number": year,
                    "campaign": campaign,
                    "article_title": title,
                    "year": year,
                    "extraction_date": "2026-06-14",
                },
            }
            graph.append(trial_node)

    # ── Process ITACyL records ────────────────────────────────────────
    if itacyl_path and Path(itacyl_path).exists():
        with open(itacyl_path) as f:
            itacyl_data = json.load(f)

        for i, rec in enumerate(itacyl_data):
            eppo = (rec.get("species_eppo") or "").strip().upper()
            if not eppo or len(eppo) != 5:
                continue

            yield_val = rec.get("yield_kg_ha")
            if not yield_val or yield_val <= 0:
                continue

            location = rec.get("location", "Castilla y León")
            climate = rec.get("climate_class", "BSk")
            campaign = str(rec.get("campaign", "2003"))
            year_match = __import__("re").search(r"(\d{4})", campaign)
            year = int(year_match.group(1)) if year_match else 2003
            variety = (rec.get("variety") or "unknown").strip().lower()
            source_pdf = rec.get("source_pdf", "")
            source = "ITACyL"
            title = f"ITACyL leguminosas {campaign}"

            site_id = get_or_create_site(location, climate, source)
            art_id = get_or_create_article(source, title, year)
            trial_id = f"urn:nkz:{source.lower()}:trial:{year}:{eppo.lower()}:{variety[:20]}"

            trial_node = {
                "@id": trial_id,
                "@type": "VarietyTrial",
                "crop_eppo": f"eppo:{eppo}",
                "crop_scientific": EPPO_SCIENTIFIC.get(eppo, ""),
                "variety": variety,
                "agroclimatic_zone": CLIMATE_URIS.get(climate, f"nkz:Koppen/{climate}"),
                "year": year,
                "yield_kg_ha": yield_val,
                "yield_relative_pct": None,
                "quality_params": {},
                "disease_scores": {},
                "irrigation_regime": IRRIG_URIS.get("rainfed"),
                "trial_location": location,
                "confidence": "medium",
                "refArticleSource": {"@id": art_id},
                "refTrialSite": {"@id": site_id},
                "metadata": {
                    "source": source,
                    "issue_number": year,
                    "campaign": campaign,
                    "article_title": title,
                    "year": year,
                    "extraction_date": "2026-06-14",
                    "source_pdf": source_pdf,
                },
            }
            graph.append(trial_node)

    return {
        "@context": CTX,
        "@graph": graph,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--ifapa", default="", help="Path to ifapa_enriched.json")
    parser.add_argument("--itacyl", default="", help="Path to itacyl_cleaned.json")
    parser.add_argument("--output", required=True, help="Output JSON-LD path")
    args = parser.parse_args()

    data = convert_to_jsonld(args.ifapa, args.itacyl)
    graph = data["@graph"]

    # Stats
    types = {}
    for n in graph:
        t = n.get("@type", "unknown")
        types[t] = types.get(t, 0) + 1

    print(f"Conversion complete: {len(graph)} nodes")
    for t, c in sorted(types.items()):
        print(f"  {t}: {c}")

    with open(args.output, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Wrote to {args.output}")


if __name__ == "__main__":
    main()
