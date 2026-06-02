#!/usr/bin/env python3
"""Ingest IFAPA + ITACyL trial observations as VarietyTrial nodes in Neo4j."""

import urllib.request, json, base64

NEO4J_URL = "http://bioorchestrator-neo4j:7474/db/neo4j/tx/commit"
AUTH_B64 = base64.b64encode(b"neo4j:bioorchestrator").decode()
HEADERS = {"Content-Type": "application/json", "Authorization": f"Basic {AUTH_B64}"}

RAINFED_URI = "http://aims.fao.org/aos/agrovoc/c_6436"
CLIMATE_BSK = "http://aims.fao.org/aos/agrovoc/c_29565"
CLIMATE_CSA = "http://aims.fao.org/aos/agrovoc/c_29557"

SCIENTIFIC_NAMES = {
    "CIEAR": "Cicer arietinum",
    "PIBAR": "Pisum sativum",
    "LENCU": "Lens culinaris",
    "VICFX": "Vicia faba",
    "VICSA": "Vicia sativa",
    "LTHSA": "Lathyrus sativus",
    "SOYBN": "Glycine max",
}

CLIMATE_URIS = {"BSk": CLIMATE_BSK, "Csa": CLIMATE_CSA, "BSh": CLIMATE_CSA}


def run_cypher(statement, params=None):
    payload = {"statements": [{"statement": statement, "parameters": params or {}}]}
    data = json.dumps(payload).encode()
    req = urllib.request.Request(NEO4J_URL, data=data, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def ingest_observations():
    # Load both JSON files (assuming they're accessible on this pod/filesystem)
    import os
    # Try multiple possible paths
    all_obs = []
    for path in [
        "/home/g/Documents/nekazari/nkz-ifapa-scraper/data/output/ifapa_extracted.json",
        "/home/g/Documents/nekazari/nkz-itacyl-scraper/data/output/itacyl_extracted.json",
        "data/output/ifapa_extracted.json",
        "data/output/itacyl_extracted.json",
    ]:
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
                all_obs.extend(data)
                print(f"Loaded {len(data)} from {path}")

    if not all_obs:
        print("ERROR: No JSON files found. Checking filesystem...")
        for root, dirs, files in os.walk("/"):
            for f in files:
                if "extracted.json" in f:
                    print(f"  Found: {os.path.join(root, f)}")
            if root.count(os.sep) > 4:
                break
        return

    ingested = 0
    skipped = 0
    errors = 0

    for i, obs in enumerate(all_obs):
        yld = obs.get("yield_kg_ha")
        if not yld or yld <= 0:
            skipped += 1
            continue

        variety = obs.get("variety", "unknown").strip().lower()
        eppo = obs.get("species_eppo", "")
        location = obs.get("location", "unknown")
        campaign = obs.get("campaign", "unknown")
        mgmt = obs.get("management", "conventional")
        climate = obs.get("climate_class", "Csa")
        source_pdf = obs.get("source_pdf", "")

        if not eppo or len(eppo) != 5:
            skipped += 1
            continue

        # Extract year from campaign string
        year_match = re.search(r"(\d{4})", str(campaign))
        year = int(year_match.group(1)) if year_match else 2000

        scientific = SCIENTIFIC_NAMES.get(eppo, "")
        climate_uri = CLIMATE_URIS.get(climate, CLIMATE_CSA)

        # Build mergeKey
        merge_key = f"eppo:{eppo}|{variety}|{location.lower()}|{RAINFED_URI}|{year}"

        metadata = json.dumps({
            "source": "IFAPA" if "ifapa" in source_pdf.lower() or "ecologico" in source_pdf.lower() or "ecotipos" in source_pdf.lower() or "kabuli" in source_pdf.lower() or "guia" in source_pdf.lower() or "lenteja" in source_pdf.lower() or "habas" in source_pdf.lower() or "leguminosas" in source_pdf.lower() or "guisante_prot" in source_pdf.lower() else "ITACyL",
            "source_pdf": source_pdf,
            "extraction_method": "nkz-scraper",
            "extraction_date": "2026-06-02",
        })

        try:
            result = run_cypher("""
                MERGE (vt:VarietyTrial {mergeKey: $mergeKey})
                SET vt.variety = $variety,
                    vt.cropScientific = $scientific,
                    vt.cropEppo = $eppo,
                    vt.trialLocation = $location,
                    vt.yieldKgHa = $yield,
                    vt.year = $year,
                    vt.management = $management,
                    vt.irrigationRegime = $rainfed,
                    vt.agroclimaticZone = $climate_uri,
                    vt.confidence = 'medium',
                    vt.metadata = $metadata,
                    vt.updatedAt = datetime()
                RETURN vt.mergeKey
            """, {
                "mergeKey": merge_key,
                "variety": variety,
                "scientific": scientific,
                "eppo": eppo,
                "location": location,
                "yield": yld,
                "year": year,
                "management": mgmt,
                "rainfed": RAINFED_URI,
                "climate_uri": climate_uri,
                "metadata": metadata,
            })

            if result.get("errors"):
                errors += 1
                if errors <= 3:
                    print(f"  ERROR: {result['errors'][:200]}")
            else:
                ingested += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  EXCEPTION: {e}")

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(all_obs)} — {ingested} ingested, {skipped} skipped, {errors} errors")

    print(f"\nDone: {ingested} ingested, {skipped} skipped, {errors} errors")

    # Verify
    result = run_cypher("MATCH (vt:VarietyTrial) WHERE vt.confidence = 'medium' RETURN count(vt) AS new_trials")
    count = result["results"][0]["data"][0]["row"][0] if result.get("results") else 0
    print(f"New VarietyTrial nodes in Neo4j: {count}")


if __name__ == "__main__":
    import re
    ingest_observations()
