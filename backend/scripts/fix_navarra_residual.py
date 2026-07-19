#!/usr/bin/env python3
"""One-shot prod fix for Navarra residual debt (null-climate sites, orphan MT)."""
from __future__ import annotations

import asyncio
import json
import os
import sys

from neo4j import AsyncGraphDatabase

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://bioorchestrator-neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
SOURCE_ID = "NAVARRA-AGRARIA"

if not NEO4J_PASSWORD:
    raise SystemExit("Falta NEO4J_PASSWORD. Expórtela antes de ejecutar.")

# Mirror navarra site_enricher SITE_COORDS + Köppen heuristic
SITE_GEO: dict[str, tuple[float, float, int, str]] = {
    "la sarda": (42.57, -1.72, 480, "Cfb"),
    "obanos": (42.75, -1.73, 420, "Cfb"),
    "red genvce": (42.65, -1.65, 450, "Cfb"),
    "muniain de la solana": (42.62, -1.97, 430, "Cfb"),
}


async def main() -> int:
    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    async with driver.session() as session:
        for site_key, (lat, lon, elev, climate) in SITE_GEO.items():
            if site_key == "muniain de la solana":
                await session.run(
                    """
                    MERGE (t:TrialSite {siteKey: $sk})
                    ON CREATE SET
                        t.name = $name,
                        t.municipality = $name,
                        t.region = 'Navarra',
                        t.source_id = $sid,
                        t.sourceIds = [$sid],
                        t.mergeKey = $sid + '|' + toLower($name) + '|' + toLower($name)
                    SET t.latitude = $lat,
                        t.longitude = $lon,
                        t.elevationM = $elev,
                        t.climateClass = $climate,
                        t.climate_class = $climate
                    """,
                    sk=site_key,
                    name="Muniain de la Solana",
                    sid=SOURCE_ID,
                    lat=lat,
                    lon=lon,
                    elev=elev,
                    climate=climate,
                )
            else:
                await session.run(
                    """
                    MATCH (t:TrialSite {siteKey: $sk})
                    SET t.latitude = $lat,
                        t.longitude = $lon,
                        t.elevationM = $elev,
                        t.climateClass = $climate,
                        t.climate_class = $climate
                    """,
                    sk=site_key,
                    lat=lat,
                    lon=lon,
                    elev=elev,
                    climate=climate,
                )

        # Link orphan MT — location aliases + missing site
        links = [
            ("Barasoian", "barasoain"),
            ("Arróniz", "arroniz"),
            ("Muniain de la Solana", "muniain de la solana"),
        ]
        linked = 0
        for loc, sk in links:
            r = await session.run(
                """
                MATCH (m:ManagementTrial {source_id: $sid, trialLocation: $loc})
                WHERE NOT (m)-[:TRIAL_AT]->()
                MATCH (t:TrialSite {siteKey: $sk})
                MERGE (m)-[:TRIAL_AT]->(t)
                SET m.trialLocationKey = $sk
                RETURN count(m) AS c
                """,
                sid=SOURCE_ID,
                loc=loc,
                sk=sk,
            )
            row = await r.single()
            linked += int(row["c"] or 0)

        # Demote olive fruit-weight yields from ranking pool
        olive = await session.run(
            """
            MATCH (v:VarietyTrial {source_id: $sid})
            WHERE v.cropEppo CONTAINS 'OLVEU' AND v.yieldKgHa > 3000
            SET v.rankingEligible = false,
                v.yieldMetric = coalesce(v.yieldMetric, 'fruit_weight_kg_ha')
            RETURN count(v) AS c
            """,
            sid=SOURCE_ID,
        )
        olive_n = (await olive.single())["c"]

        # Verify
        async def _count(q: str, **p) -> int:
            row = await (await session.run(q, **p)).single()
            return int(row["c"])

        stats = {
            "orphan_vt": await _count(
                "MATCH (v:VarietyTrial {source_id: $sid}) "
                "WHERE NOT (v)-[:TRIAL_AT]->() RETURN count(v) AS c",
                sid=SOURCE_ID,
            ),
            "orphan_mt": await _count(
                "MATCH (m:ManagementTrial {source_id: $sid}) "
                "WHERE NOT (m)-[:TRIAL_AT]->() RETURN count(m) AS c",
                sid=SOURCE_ID,
            ),
            "null_clim_sites": await _count(
                "MATCH (t:TrialSite)<-[:TRIAL_AT]-(:VarietyTrial {source_id: $sid}) "
                "WHERE t.climateClass IS NULL AND t.climate_class IS NULL "
                "RETURN count(DISTINCT t) AS c",
                sid=SOURCE_ID,
            ),
        }
        print(json.dumps({
            "mt_linked": linked,
            "olive_demoted": olive_n,
            **stats,
        }))
    await driver.close()
    if stats.get("orphan_vt") or stats.get("orphan_mt"):
        print("WARN residual orphans remain", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
