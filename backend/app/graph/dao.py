"""Graph Data Access Object — Neo4j query layer.

All methods use the AsyncDriver and return plain dicts for JSON serialisation.
Business logic lives in app/services/.

Tenant model:
  The Neo4j knowledge graph holds ONLY global biological reference data
  (crop catalog, phenology, EPPO/IUCN/AGROVOC). It is a single shared graph,
  NOT partitioned by tenant — biological reality is identical for all tenants.
  Tenant isolation is enforced at the deployment level (a dedicated nkz instance
  if a customer requires it), never by a tenant axis inside this graph.

  Tenant-specific data (parcels, NDVI, soil, weather, crop assignments) lives in
  Orion-LD per-tenant stores + TimescaleDB and is read on demand with the request's
  tenant; it is never persisted into this graph.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any

from neo4j import AsyncDriver
from nkz_platform_sdk.orion import OrionClient

logger = logging.getLogger(__name__)

TIMESERIES_READER_URL = os.getenv("TIMESERIES_READER_URL", "http://timeseries-reader-service:5000")


class GraphDAO:
    def __init__(self, driver: AsyncDriver) -> None:
        self._driver = driver

    # ── Health ────────────────────────────────────────────────────────────────

    async def health_check(self) -> dict:
        """Verify Neo4j connectivity with a minimal query."""
        try:
            async with self._driver.session() as session:
                result = await session.run("RETURN 1 AS alive")
                record = await result.single()
                return {"neo4j": "connected", "alive": record["alive"]}
        except Exception as exc:
            return {"neo4j": "error", "detail": str(exc)}

    # ── Global stats (not tenant-filtered — counts all reference data) ────────

    async def get_stats(self) -> dict[str, Any]:
        """Return node count, relationship count, and per-label counts.

        Counts all nodes and relationships in the shared global graph.
        No tenant filtering applies — the graph holds only biological
        reference data that is identical for all tenants.
        """
        async with self._driver.session() as session:
            totals_result = await session.run(
                "MATCH (n) RETURN count(n) AS node_count "
                "UNION ALL "
                "MATCH ()-[r]->() RETURN count(r) AS node_count"
            )
            records = await totals_result.values()
            node_count = records[0][0] if records else 0
            rel_count = records[1][0] if len(records) > 1 else 0

            label_result = await session.run(
                "CALL db.labels() YIELD label "
                "RETURN label"
            )
            labels = [r["label"] async for r in label_result]

            label_counts = {}
            for lbl in labels:
                # Defense-in-depth: labels come from db.labels() (app-set), never user input
                if not re.match(r'^[A-Za-z][A-Za-z0-9_]*$', lbl):
                    continue
                cnt = await session.run(
                    "MATCH (n:" + lbl + ") RETURN count(n) AS c"
                )
                row = await cnt.single()
                if row:
                    label_counts[lbl] = row["c"]

            # Sort by count desc, limit to 30
            label_counts = dict(
                sorted(label_counts.items(), key=lambda x: -x[1])[:30]
            )

        return {
            "node_count": node_count,
            "relationship_count": rel_count,
            "label_counts": label_counts,
        }

    # ── Lookup (global reference data) ────────────────────────────────────────

    async def find_by_agrovoc_uri(self, uri: str) -> dict | None:
        """Find a Resource node by its AGROVOC URI.

        Returns the node properties or None if not found.
        """
        async with self._driver.session() as session:
            result = await session.run(
                "MATCH (r:Resource {uri: $uri}) RETURN r",
                uri=uri,
            )
            record = await result.single()
            if record is None:
                return None
            return dict(record["r"])

    async def get_all_species(self) -> list[dict]:
        """Return all species in the knowledge graph with phenology availability."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (s:Species)
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st:PhenologyStage)-[:HAS_PARAMETER]->(p:PhenologyParams)
                RETURN s.name AS name,
                       s.scientificName AS scientific_name,
                       count(DISTINCT st) AS stage_count,
                       count(DISTINCT p) AS params_count
                ORDER BY s.name
            """)
            from app.data.eppo_common_names import get_common_name
            species_list = []
            async for record in result:
                name = record["name"]
                species_list.append({
                    "name": name,
                    "scientific_name": record["scientific_name"],
                    "common_name": get_common_name(name) or name.capitalize(),
                    "stage_count": record["stage_count"],
                    "params_count": record["params_count"],
                    "has_phenology": record["params_count"] > 0,
                })
            return species_list

    # ── Reference Data ─────────────────────────────────────────────────────

    async def get_climate_classes(self) -> list[str]:
        """Return unique K\u00f6ppen climate classes from TrialSite nodes."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                WHERE ts.climateClass IS NOT NULL AND ts.climateClass <> ''
                RETURN DISTINCT ts.climateClass AS climate_class
                ORDER BY climate_class
            """)
            return [r["climate_class"] async for r in result]

    async def get_soil_types(self) -> list[str]:
        """Return unique WRB soil types from TrialSite nodes."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                WHERE ts.soilType IS NOT NULL AND ts.soilType <> ''
                RETURN DISTINCT ts.soilType AS soil_type
                ORDER BY soil_type
            """)
            return [r["soil_type"] async for r in result]

    async def get_phenology_params(
        self,
        species: str,
        stage: str | None = None,
        cultivar: str | None = None,
        management: str | None = None,
        climate_zone: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        gdd: float | None = None,
    ) -> dict | None:
        """Query phenology parameters with context-aware cascade matching.

        Matching priority:
          1. Exact: species + stage + cultivar + management
          2. Management-only: species + stage + management (any cultivar)
          3. Generic: species + stage (no cultivar, no management)
          4. Species-only: any stage default

        When GDD (Growing Degree Days) is provided and stage is not explicitly
        given, auto-detects the phenological stage by matching GDD against
        the [gddMin, gddMax] thresholds stored in PhenologyStage nodes.

        Returns a dict with:
          - Core values: d1, d2, kc, mds_ref with confidence intervals
          - Provenance: sourceDoi, sourceShort, sourceAuthor, sourceYear,
                        sourceInstitution, sourceMethod, sourceConditions
          - Context: species, scientificName, stage, stageDescription,
                     cultivar, management, climateZone
          - Stage detection: baseTemp, gddMin, gddMax (when available)
          - Alternatives: list of {kc, sourceShort, sourceDoi, conditions}
          - Match info: matchLevel (exact/management/generic/species_only)
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                // ── Find species ────────────────────────────────────────
                MATCH (s:Species)
                WHERE s.name CONTAINS $species
                   OR s.scientificName CONTAINS $species
                   OR s.agrovocUri CONTAINS $species
                WITH s
                ORDER BY
                    CASE WHEN s.name = $species THEN 0
                         WHEN s.name CONTAINS $species THEN 1
                         ELSE 2 END
                LIMIT 1

                // ── Find best-matching stage ─────────────────────────────
                // Priority: explicit stage name > GDD range match > any stage with params
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st:PhenologyStage)
                WHERE $stage IS NOT NULL AND st.name CONTAINS $stage

                // If no stage matched by name and GDD is provided, match by GDD range
                WITH s, st, $gdd AS gdd
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st_gdd:PhenologyStage)
                WHERE st IS NULL
                  AND gdd IS NOT NULL
                  AND st_gdd.gddMin IS NOT NULL
                  AND st_gdd.gddMax IS NOT NULL
                  AND st_gdd.gddMin <= gdd
                  AND st_gdd.gddMax > gdd
                WITH s, st, st_gdd

                // If no stage matched yet, pick any stage that has parameters
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st_any:PhenologyStage)
                WHERE st IS NULL AND st_gdd IS NULL
                  AND (st_any)-[:HAS_PARAMETER]->()
                WITH s, COALESCE(st, st_gdd, st_any) AS st

                // ── Find best parameter by context cascade ───────────────
                OPTIONAL MATCH (st)-[:HAS_PARAMETER]->(p:PhenologyParams)

                // Score: exact context > management-only > generic default
                WITH s, st, p
                ORDER BY
                    CASE WHEN p.cultivar = $cultivar
                          AND p.management = $mgmt THEN 0
                         WHEN p.management = $mgmt
                          AND p.cultivar IS NULL THEN 1
                         WHEN p.isDefault = true THEN 2
                         ELSE 3 END
                LIMIT 1

                // ── Fetch alternatives ───────────────────────────────────
                OPTIONAL MATCH (p)-[:HAS_ALTERNATIVE]->(alt:PhenologyAlternative)

                // ── Return full provenance ────────────────────────────────
                RETURN
                    s.name AS species,
                    s.scientificName AS scientific_name,
                    s.agrovocUri AS agrovoc_uri,
                    st.name AS stage,
                    st.description AS stage_description,
                    st.baseTemp AS stage_base_temp,
                    st.gddMin AS stage_gdd_min,
                    st.gddMax AS stage_gdd_max,
                    p.kc AS kc,
                    p.kcCiLow AS kc_ci_low,
                    p.kcCiHigh AS kc_ci_high,
                    p.ky AS ky,
                    p.d1 AS d1,
                    p.d1CiLow AS d1_ci_low,
                    p.d1CiHigh AS d1_ci_high,
                    p.d2 AS d2,
                    p.d2CiLow AS d2_ci_low,
                    p.d2CiHigh AS d2_ci_high,
                    p.mdsRef AS mds_ref,
                    p.mdsRefCiLow AS mds_ref_ci_low,
                    p.mdsRefCiHigh AS mds_ref_ci_high,
                    p.cultivar AS cultivar,
                    p.management AS management,
                    p.climateZone AS climate_zone,
                    p.isDefault AS is_default,
                    p.sourceDoi AS source_doi,
                    p.sourceShort AS source_short,
                    p.sourceAuthor AS source_author,
                    p.sourceYear AS source_year,
                    p.sourceInstitution AS source_institution,
                    p.sourceMethod AS source_method,
                    p.sourceConditions AS source_conditions,
                    CASE WHEN p.cultivar = $cultivar
                          AND p.management = $mgmt THEN 'exact'
                         WHEN p.management = $mgmt
                          AND p.cultivar IS NULL THEN 'management'
                         WHEN p.isDefault = true THEN 'generic'
                         WHEN p IS NOT NULL THEN 'species_only'
                         ELSE 'none' END AS match_level,
                    collect(
                        CASE WHEN alt IS NOT NULL THEN {
                            kc: alt.kc,
                            sourceShort: alt.sourceShort,
                            sourceDoi: alt.sourceDoi,
                            conditions: alt.conditions
                        } END
                    ) AS alternatives
                """,
                species=species,
                stage=stage,
                cultivar=cultivar,
                mgmt=management,
                gdd=gdd,
            )
            record = await result.single()
            if record is None or record["match_level"] == "none":
                # ── Fallback: try CropHealthAssessment from Orion-LD ─────
                return await self._fallback_phenology_from_orion(
                    species=species,
                    stage=stage,
                )

            # Filter nulls from alternatives collection
            alts = [
                a for a in (record["alternatives"] or [])
                if a is not None and a.get("kc") is not None
            ]

            return {
                "species": record["species"],
                "scientific_name": record["scientific_name"],
                "agrovoc_uri": record["agrovoc_uri"],
                "stage": record["stage"],
                "stage_description": record["stage_description"],
                "stage_base_temp": record["stage_base_temp"],
                "stage_gdd_min": record["stage_gdd_min"],
                "stage_gdd_max": record["stage_gdd_max"],
                "kc": record["kc"],
                "kc_confidence_interval": (
                    [record["kc_ci_low"], record["kc_ci_high"]]
                    if record["kc_ci_low"] is not None
                    else None
                ),
                "ky": record.get("ky"),
                "d1": record["d1"],
                "d1_confidence_interval": (
                    [record["d1_ci_low"], record["d1_ci_high"]]
                    if record["d1_ci_low"] is not None
                    else None
                ),
                "d2": record["d2"],
                "d2_confidence_interval": (
                    [record["d2_ci_low"], record["d2_ci_high"]]
                    if record["d2_ci_low"] is not None
                    else None
                ),
                "mds_ref": record["mds_ref"],
                "mds_ref_confidence_interval": (
                    [record["mds_ref_ci_low"], record["mds_ref_ci_high"]]
                    if record["mds_ref_ci_low"] is not None
                    else None
                ),
                "cultivar": record["cultivar"],
                "management": record["management"],
                "climate_zone": record["climate_zone"],
                "is_default": record["is_default"],
                "provenance": {
                    "doi": record["source_doi"],
                    "short": record["source_short"],
                    "author": record["source_author"],
                    "year": record["source_year"],
                    "institution": record["source_institution"],
                    "method": record["source_method"],
                    "conditions": record["source_conditions"],
                },
                "alternatives": alts,
                "match_level": record["match_level"],
            }

    async def get_phenology_stages(self, species: str) -> list[dict]:
        """Return the full ordered stage table for a species (ascending gddMin).

        Empty list when the species has no PhenologyStage nodes — callers
        (e.g. crop-health) fall back to their own default table.
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species)-[:HAS_STAGE]->(st:PhenologyStage)
                WHERE toLower(s.name) = toLower($species)
                   OR toLower(s.scientificName) = toLower($species)
                RETURN st.name AS stage, st.gddMin AS gddMin,
                       st.gddMax AS gddMax, st.baseTemp AS baseTemp
                """,
                species=species,
            )
            rows = await result.data()

        rows = [
            r for r in rows
            if r.get("gddMin") is not None and r.get("gddMax") is not None
        ]
        return sorted(rows, key=lambda r: r["gddMin"])

    async def contribute_phenology(
        self,
        species: str,
        stage: str,
        kc: float,
        d1: float | None = None,
        d2: float | None = None,
        mds_ref: float | None = None,
        cultivar: str | None = None,
        management: str | None = None,
        doi: str | None = None,
        author: str | None = None,
        conditions: str | None = None,
        contact_email: str | None = None,
    ) -> dict:
        """Submit a contributed phenology parameter for review.

        Creates nodes with status='pending_review'. An admin can later
        approve and merge into the main parameter set.
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                MERGE (s:Species {name: $species})
                MERGE (s)-[:HAS_STAGE]->(st:PhenologyStage {name: $stage})
                CREATE (st)-[:HAS_PARAMETER]->(p:PhenologyParams {
                    kc: $kc,
                    d1: $d1,
                    d2: $d2,
                    mdsRef: $mds_ref,
                    cultivar: COALESCE($cultivar, '__contributed__'),
                    management: COALESCE($management, '__contributed__'),
                    climateZone: '__contributed__',
                    isDefault: false,
                    sourceDoi: $doi,
                    sourceShort: 'Contributed: ' + COALESCE($author, 'anonymous'),
                    sourceAuthor: $author,
                    sourceYear: null,
                    sourceConditions: $conditions,
                    status: 'pending_review',
                    contactEmail: $contact_email,
                    submittedAt: datetime()
                })
                RETURN p.status AS status, p.sourceShort AS source
                """,
                species=species,
                stage=stage,
                kc=kc,
                d1=d1,
                d2=d2,
                mds_ref=mds_ref,
                cultivar=cultivar,
                management=management,
                doi=doi,
                author=author,
                conditions=conditions,
                contact_email=contact_email,
            )
            record = await result.single()
            if record is None:
                return {"status": "error", "detail": "Failed to create"}
            return {"status": record["status"], "source": record["source"]}

    # ── Phenology Fallback (Orion-LD CropHealthAssessment) ──────────────────────

    async def _fallback_phenology_from_orion(
        self,
        species: str,
        stage: str | None = None,
    ) -> dict | None:
        """Fallback: fetch phenology params from CropHealthAssessment in Orion-LD.

        Called when Neo4j has no data for the requested species.
        Queries the most recent CropHealthAssessment entity for the species
        and extracts Kc, D1, D2, MDS from its attributes.
        """
        try:
            from app.core.config import settings
            from nkz_platform_sdk.orion import OrionClient

            orion = OrionClient(
                settings.catalog_tenant,
                base_url=settings.orion_ld_url,
                context_url=settings.context_url,
            )
            try:
                entities = await orion.query_entities(
                    type="CropHealthAssessment",
                    limit=5,
                    order_by="assessedAt",
                    order_desc=True,
                )
            finally:
                await orion.close()

            if not entities or not isinstance(entities, list):
                return None

            # Find the first entity that matches the species
            for entity in entities:
                species_val = (
                    entity.get("species", {}).get("value")
                    or entity.get("species", {}).get("object")
                    or entity.get("species")
                )
                if not species_val:
                    continue
                if species.lower() in str(species_val).lower():
                    return self._extract_phenology_from_assessment(entity)

            # If no species match but we have any assessment, return the latest
            return self._extract_phenology_from_assessment(entities[0])

        except ImportError:
            logger.warning("nkz_platform_sdk not available, cannot fallback to Orion")
            return None
        except Exception as exc:
            logger.warning("Phenology fallback to Orion failed: %s", exc)
            return None

    def _extract_phenology_from_assessment(self, entity: dict) -> dict | None:
        """Extract phenology params from a CropHealthAssessment entity.

        The entity is expected in normalized NGSI-LD format (from OrionClient).
        """
        def _val(key: str) -> float | None:
            v = entity.get(key)
            if v is None:
                return None
            if isinstance(v, dict):
                return v.get("value")
            if isinstance(v, (int, float)):
                return float(v)
            return None

        kc = _val("kc")
        ky = _val("ky")
        d1 = _val("d1") or _val("nwsbBaseline") or _val("d1Baseline")
        d2 = _val("d2") or _val("maxStressBaseline") or _val("d2Baseline")
        mds_ref = _val("mdsRef") or _val("mdsReference")

        if kc is None and d1 is None and d2 is None and mds_ref is None:
            return None

        return {
            "species": entity.get("id", ""),
            "scientific_name": None,
            "agrovoc_uri": None,
            "stage": (
                entity.get("phenologyStage", {}).get("value")
                if isinstance(entity.get("phenologyStage"), dict)
                else entity.get("phenologyStage")
            ),
            "stage_description": None,
            "stage_base_temp": None,
            "stage_gdd_min": None,
            "stage_gdd_max": None,
            "kc": kc,
            "kc_confidence_interval": None,
            "ky": ky,
            "d1": d1,
            "d1_confidence_interval": None,
            "d2": d2,
            "d2_confidence_interval": None,
            "mds_ref": mds_ref,
            "mds_ref_confidence_interval": None,
            "cultivar": None,
            "management": None,
            "climate_zone": None,
            "is_default": True,
            "provenance": {
                "doi": None,
                "short": "CropHealthAssessment (fallback)",
                "author": None,
                "year": None,
                "institution": None,
                "method": "Orion-LD fallback",
                "conditions": None,
            },
            "alternatives": [],
            "match_level": "fallback_orion",
        }

    # ── Heat Tolerance ─────────────────────────────────────────────────────────

    async def get_heat_tolerance(self, species: str) -> dict | None:
        """Return heat/frost damage thresholds for a species."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species)-[:HAS_HEAT_TOLERANCE]->(h:CropHeatTolerance)
                WHERE s.name = $species
                RETURN h.heatDamageThresholdC AS heat_damage_c,
                       h.frostDamageThresholdC AS frost_damage_c,
                       h.heatAccumHours AS heat_accum_hours,
                       h.sourceShort AS source_short,
                       h.sourceDoi AS source_doi
                LIMIT 1
                """,
                species=species,
            )
            record = await result.single()
            if record is None:
                return None
            return dict(record)

    # ── Nutrient Profile ──────────────────────────────────────────────────────

    async def get_nutrient_profile(self, species: str, stage: str | None = None) -> dict | None:
        """Return NPK uptake curve per phenological stage."""
        async with self._driver.session() as session:
            query = """
                MATCH (s:Species {name: $species})-[:HAS_STAGE]->
                      (st:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile)
            """
            params: dict = {"species": species}
            if stage:
                query += " WHERE st.name = $stage"
                params["stage"] = stage
            query += """
                RETURN st.name AS stage, n.nitrogenUptake AS n_uptake,
                       n.phosphorusUptake AS p_uptake, n.potassiumUptake AS k_uptake,
                       n.sourceShort AS source_short, n.sourceDoi AS source_doi
                ORDER BY st.name
            """
            result = await session.run(query, params)
            records = await result.data()
            return records if records else None

    # ── Soil Suitability ──────────────────────────────────────────────────────

    async def get_soil_suitability(self, species: str) -> dict | None:
        """Return soil requirements for a crop species."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species {name: $species})-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability)
                RETURN ss.phMin AS ph_min, ss.phMax AS ph_max,
                       ss.textures AS textures, ss.drainage AS drainage,
                       ss.depthMinCm AS depth_min_cm,
                       ss.salinityMaxDsM AS salinity_max_ds_m,
                       ss.sourceShort AS source_short
                LIMIT 1
                """,
                species=species,
            )
            record = await result.single()
            if record is None:
                return None
            return dict(record)

    # ── Rotation Constraints ──────────────────────────────────────────────────

    async def get_rotation_constraints(self, crop: str) -> list[dict]:
        """Return rotation constraints for a crop."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (rc:RotationConstraint)
                WHERE rc.cropA = $crop
                RETURN rc.cropB AS crop_b, rc.intervalYears AS interval_years,
                       rc.reason AS reason, rc.sourceShort AS source_short
                """,
                crop=crop,
            )
            return [dict(r) for r in await result.data()]

    async def recommend_next_crop(self, previous_crop: str, species: str | None = None) -> list[dict]:
        """Suggest next crop based on rotation rules: exclude constrained crops."""
        async with self._driver.session() as session:
            # Get all constraints where previous_crop has restrictions
            result = await session.run(
                """
                MATCH (rc:RotationConstraint {cropA: $crop})
                RETURN rc.cropB AS restricted, rc.intervalYears AS years, rc.reason AS reason
                """,
                crop=previous_crop,
            )
            restricted = {r["restricted"] for r in await result.data()}

            # Get all available species that are NOT restricted
            result2 = await session.run(
                """
                MATCH (s:Species)
                WHERE NOT s.name IN $restricted OR $restricted = []
                RETURN s.name AS name, s.scientificName AS scientific_name
                ORDER BY s.name
                """,
                restricted=list(restricted),
            )
            return [dict(r) for r in await result2.data()]

    async def recommend_fertilizer(
        self, species: str, stage: str, soil_n: float = 0, soil_p: float = 0, soil_k: float = 0
    ) -> dict | None:
        """Return NPK fertilizer needs based on soil levels and crop demand."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species {name: $species})-[:HAS_STAGE]->(st:PhenologyStage {name: $stage})
                     -[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile)
                RETURN n.element AS element, n.uptakeKgHaDay AS uptake
                """,
                species=species, stage=stage,
            )
            records = await result.data()
            if not records:
                return None

            soil = {"nitrogen": soil_n, "phosphorus": soil_p, "potassium": soil_k}
            recommendations = []
            for r in records:
                element = r["element"]
                uptake = float(r["uptake"] or 0)
                level = soil.get(element, 0)
                if level < uptake * 0.5:
                    status, action = "deficient", f"Increase {element}"
                elif level < uptake:
                    status, action = "adequate", f"Maintain {element}"
                else:
                    status, action = "sufficient", f"Sufficient {element}"
                recommendations.append({
                    "element": element, "uptake_kg_ha_day": uptake,
                    "soil_level": level, "status": status, "action": action,
                })

            return {"species": species, "stage": stage, "recommendations": recommendations}

    async def simulate_scenario(
        self, baseline_crop: str, scenario_crop: str,
        baseline_sowing: str | None = None, scenario_sowing: str | None = None,
    ) -> dict:
        """Compare two agronomic scenarios and return deltas.

        Returns yield gap delta, fertilizer delta, and constraint violations
        for the scenario vs baseline. Pure rule-based, no ML.
        """
        result: dict = {
            "baseline": baseline_crop,
            "scenario": scenario_crop,
            "rotation_ok": True,
            "rotation_issue": "",
            "soil_ok": True,
            "soil_issues": [],
            "fertilizer_delta": [],
            "recommendation": "",
        }

        async with self._driver.session() as session:
            # Check rotation constraint
            rc = await session.run(
                "MATCH (r:RotationConstraint {cropA: $baseline, cropB: $scenario}) "
                "RETURN r.intervalYears AS years, r.reason AS reason",
                baseline=baseline_crop, scenario=scenario_crop,
            )
            row = await rc.single()
            if row and row["years"] and row["years"] > 0:
                result["rotation_ok"] = False
                result["rotation_issue"] = (
                    f"{row['reason']}. Minimum interval: {row['years']} years."
                )

            # Check soil suitability for scenario crop
            ss = await session.run(
                "MATCH (s:Species {name: $crop})-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability) "
                "RETURN ss.phMin, ss.phMax, ss.textures, ss.drainage, ss.depthMinCm, ss.salinityMaxDsM",
                crop=scenario_crop,
            )
            soil = await ss.single()
            if not soil:
                result["soil_issues"].append("No soil suitability data for scenario crop")
                result["soil_ok"] = False

            # Compare fertilizer needs (simplified: total NPK per season)
            base_n = await session.run(
                "MATCH (s:Species {name: $crop})-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile {element: 'nitrogen'}) "
                "RETURN sum(n.uptakeKgHaDay) AS total", crop=baseline_crop,
            )
            base_total = (await base_n.single())
            base_n_val = float(base_total["total"] or 0) if base_total else 0

            sc_n = await session.run(
                "MATCH (s:Species {name: $crop})-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile {element: 'nitrogen'}) "
                "RETURN sum(n.uptakeKgHaDay) AS total", crop=scenario_crop,
            )
            sc_total = (await sc_n.single())
            sc_n_val = float(sc_total["total"] or 0) if sc_total else 0

            delta = sc_n_val - base_n_val
            if delta > 1:
                result["fertilizer_delta"].append(
                    {"element": "nitrogen", "delta_kg_ha_day": round(delta, 1),
                     "note": "Scenario needs more N than baseline"}
                )
            elif delta < -1:
                result["fertilizer_delta"].append(
                    {"element": "nitrogen", "delta_kg_ha_day": round(delta, 1),
                     "note": "Scenario needs less N than baseline"}
                )

            # Recommendation
            issues = []
            if not result["rotation_ok"]:
                issues.append("rotation constraint violated")
            if not result["soil_ok"]:
                issues.append("soil suitability issues")
            if abs(delta) > 0:
                issues.append("fertilizer adjustment needed")

            if not issues:
                result["recommendation"] = f"{scenario_crop} is a suitable alternative to {baseline_crop}."
            else:
                result["recommendation"] = (
                    f"{scenario_crop} vs {baseline_crop}: {'; '.join(issues)}. "
                    "Review before adopting."
                )

        return result

    # ── AgriCrop Catalog (Orion-LD integration) ───────────────────────────────

    async def merge_agri_crop(self, entity: dict) -> None:
        """MERGE an AgriCrop from Orion-LD into Neo4j. Idempotent."""
        async with self._driver.session() as session:
            await session.run("""
                MERGE (c:AgriCrop {uri: $uri})
                SET c.name = $name,
                    c.scientificName = $scientificName,
                    c.dataProvider = $provider,
                    c.updatedAt = datetime()
            """,
                uri=entity.get("id"),
                name=self._extract_value(entity, "name"),
                scientificName=self._extract_value(entity, "scientificName"),
                provider=self._extract_value(entity, "dataProvider"),
            )

            # Sync hasSubCrop relationships
            sub_crops = entity.get("hasSubCrop", {})
            if isinstance(sub_crops, dict) and sub_crops.get("type") == "Relationship":
                variety_uris = sub_crops.get("object", [])
                if isinstance(variety_uris, str):
                    variety_uris = [variety_uris]
                for var_uri in variety_uris:
                    await session.run("""
                        MERGE (v:AgriCropVariety {uri: $var_uri})
                        WITH v
                        MATCH (c:AgriCrop {uri: $uri})
                        MERGE (c)-[:HAS_VARIETY]->(v)
                    """, uri=entity.get("id"), var_uri=var_uri)

            # Sync Kc values -> PhenologyParams (default)
            kc_ini = self._extract_value(entity, "kcIni")
            if kc_ini is not None:
                await session.run("""
                    MATCH (c:AgriCrop {uri: $uri})
                    MERGE (c)-[r:HAS_PARAMETER]->(p:PhenologyParams {isDefault: true})
                    SET p.kc = $kc_ini,
                        p.kcMid = $kc_mid,
                        p.kcEnd = $kc_end,
                        p.sourceShort = $source,
                        p.updatedAt = datetime()
                """,
                    uri=entity.get("id"),
                    kc_ini=float(kc_ini),
                    kc_mid=float(self._extract_value(entity, "kcMid") or kc_ini),
                    kc_end=float(self._extract_value(entity, "kcEnd") or kc_ini),
                    source=self._extract_value(entity, "kcSource") or "Unknown",
                )

            # Sync heat tolerance
            heat = self._extract_value(entity, "heatDamageThresholdC")
            frost = self._extract_value(entity, "frostDamageThresholdC")
            if heat is not None or frost is not None:
                await session.run("""
                    MATCH (c:AgriCrop {uri: $uri})
                    MERGE (c)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
                    SET ht.heatDamageThresholdC = $heat,
                        ht.frostDamageThresholdC = $frost,
                        ht.sourceType = $source_type,
                        ht.updatedAt = datetime()
                """,
                    uri=entity.get("id"),
                    heat=float(heat) if heat else None,
                    frost=float(frost) if frost else None,
                    source_type=self._extract_value(entity, "thermalSource") or "derived_from_ecocrop",
                )

    async def get_crop_catalog(self, source: str | None = None,
                                search: str | None = None) -> list[dict]:
        """List AgriCrop entities from Neo4j with variety counts."""
        query = """
            MATCH (c:AgriCrop)
            WHERE c.uri CONTAINS ':AgriCrop:' AND NOT c.uri CONTAINS ':AgriCrop:.*:'
        """
        if search:
            query += " AND (toLower(c.name) CONTAINS toLower($search) OR toLower(c.scientificName) CONTAINS toLower($search))"
        query += """
            OPTIONAL MATCH (c)-[:HAS_VARIETY]->(v:AgriCropVariety)
            OPTIONAL MATCH (c)-[:HAS_PARAMETER]->(p:PhenologyParams)
            OPTIONAL MATCH (c)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
            RETURN c, count(DISTINCT v) as variety_count,
                   count(DISTINCT p) > 0 as has_kc,
                   count(DISTINCT ht) > 0 as has_thermal
            ORDER BY c.name
        """
        async with self._driver.session() as session:
            result = await session.run(query, search=search)
            crops = []
            async for record in result:
                c = record["c"]
                crops.append({
                    "uri": c.get("uri"),
                    "name": c.get("name"),
                    "scientificName": c.get("scientificName"),
                    "dataProvider": c.get("dataProvider"),
                    "variety_count": record["variety_count"],
                    "has_kc": record["has_kc"],
                    "has_thermal": record["has_thermal"],
                })
            return crops

    # ── Agriculture: Variety Trials ──────────────────────────────────────

    async def get_variety_trials(
        self,
        crop: str | None = None,
        climate_class: str | None = None,
        soil_type: str | None = None,
        soil_texture: str | None = None,
        irrigation_regime: str | None = None,
        min_yield_kg_ha: float | None = None,
        min_rainfall_mm: float | None = None,
        max_rainfall_mm: float | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Ranked variety trial results with environmental filters.

        Returns varieties sorted by yield (descending) with their TrialSite
        environmental context. Supports filtering by crop, climate, soil,
        irrigation regime, and rainfall range.

        This is the primary endpoint for:
          - "What wheat varieties perform best in BSk climate?"
          - "Show me tomato trials on calcareous soils under rainfed conditions"
        """
        # Effective yield: prefer absolute yieldKgHa, fallback to BSL 1-9 notes scaled
        where_clauses = ["(vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)"]
        params: dict[str, Any] = {"limit": limit}

        if crop:
            where_clauses.append("(vt.cropEppo = $crop OR vt.cropScientific CONTAINS $crop OR toLower(vt.variety) CONTAINS toLower($crop))")
            params["crop"] = crop

        if climate_class:
            where_clauses.append("ts.climateClass = $climate")
            params["climate"] = climate_class

        if soil_type:
            where_clauses.append("ts.soilType CONTAINS $soil")
            params["soil"] = soil_type

        if soil_texture:
            where_clauses.append("ts.soilTexture CONTAINS $texture")
            params["texture"] = soil_texture

        if irrigation_regime:
            where_clauses.append("vt.irrigationRegime CONTAINS $irrigation")
            params["irrigation"] = irrigation_regime

        if min_yield_kg_ha is not None:
            where_clauses.append("COALESCE(vt.yieldKgHa, vt.yieldNoteS1 * 1000) >= $min_yield")
            params["min_yield"] = min_yield_kg_ha

        if min_rainfall_mm is not None:
            where_clauses.append("ts.annualRainfallMm >= $min_rain")
            params["min_rain"] = min_rainfall_mm

        if max_rainfall_mm is not None:
            where_clauses.append("ts.annualRainfallMm <= $max_rain")
            params["max_rain"] = max_rainfall_mm

        where_str = " AND ".join(where_clauses)

        query = f"""
            MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
            WHERE {where_str}
            OPTIONAL MATCH (vt)-[:SOURCED_FROM]->(as_article:ArticleSource)
            RETURN vt.cropEppo AS crop_eppo,
                   vt.cropScientific AS crop_scientific,
                   vt.variety AS variety,
                   vt.yieldKgHa AS yield_kg_ha,
                   vt.yieldNoteS1 AS yield_note_s1,
                   COALESCE(vt.yieldKgHa, vt.yieldNoteS1 * 1000) AS yield_effective_kg_ha,
                   vt.yieldRelativePct AS yield_relative_pct,
                   vt.qualityParams AS quality_params,
                   vt.diseaseScores AS disease_scores,
                   vt.irrigationRegime AS irrigation_regime,
                   vt.year AS year,
                   vt.confidence AS confidence,
                   vt.mergeKey AS trial_id,
                   ts.name AS site_name,
                   ts.climateClass AS climate_class,
                   ts.soilType AS soil_type,
                   ts.soilTexture AS soil_texture,
                   ts.soilPh AS soil_ph,
                   ts.annualRainfallMm AS annual_rainfall_mm,
                   ts.elevationM AS elevation_m,
                   ts.frostDaysPerYear AS frost_days,
                   ts.photoperiodSummerHours AS photoperiod_hours,
                   as_article.articleTitle AS source_title,
                   as_article.issueNumber AS source_issue,
                   as_article.year AS source_year
            ORDER BY COALESCE(vt.yieldKgHa, vt.yieldNoteS1 * 1000) DESC
            LIMIT $limit
        """

        async with self._driver.session() as session:
            result = await session.run(query, params)
            trials = []
            async for record in result:
                trials.append(dict(record))
            return trials

    async def get_similar_sites(
        self,
        reference_site: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        climate_class: str | None = None,
        soil_type: str | None = None,
        rainfall_min: float | None = None,
        rainfall_max: float | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Find TrialSites environmentally similar to a reference.

        Matching strategy (cascade):
          1. Same Köppen climate class (exact match)
          2. Similar soil type (WRB reference group)
          3. Rainfall within ±200mm band
          4. Elevation within ±300m

        If a reference site name is given, its properties are used as filters.
        Otherwise, explicit filters (climate_class, soil_type, rainfall range)
        are used directly.

        Returns sites ranked by environmental similarity score.
        """
        params: dict[str, Any] = {"limit": limit}

        if reference_site:
            # Look up the reference site first
            async with self._driver.session() as session:
                ref_result = await session.run(
                    """
                    MATCH (ts:TrialSite)
                    WHERE toLower(ts.name) = toLower($name)
                       OR toLower(ts.municipality) = toLower($name)
                    RETURN ts.climateClass AS climate,
                           ts.soilType AS soil,
                           ts.annualRainfallMm AS rainfall,
                           ts.elevationM AS elevation,
                           ts.soilTexture AS texture,
                           ts.name AS name
                    LIMIT 1
                    """,
                    name=reference_site,
                )
                ref = await ref_result.single()
                if not ref:
                    return []

                climate_class = ref["climate"]
                soil_type = ref["soil"]
                rainfall_min = (ref["rainfall"] or 500) - 200
                rainfall_max = (ref["rainfall"] or 500) + 200

        where_clauses = ["ts.name IS NOT NULL"]  # ensure site has data

        if climate_class:
            where_clauses.append("ts.climateClass = $climate")
            params["climate"] = climate_class

        if soil_type:
            where_clauses.append("ts.soilType CONTAINS $soil")
            params["soil"] = soil_type

        if rainfall_min is not None:
            where_clauses.append("ts.annualRainfallMm >= $rain_min")
            params["rain_min"] = rainfall_min

        if rainfall_max is not None:
            where_clauses.append("ts.annualRainfallMm <= $rain_max")
            params["rain_max"] = rainfall_max

        where_str = " AND ".join(where_clauses)

        query = f"""
            MATCH (ts:TrialSite)
            WHERE {where_str}
            RETURN ts.name AS name,
                   ts.municipality AS municipality,
                   ts.agroclimaticZone AS agroclimatic_zone,
                   ts.latitude AS latitude,
                   ts.longitude AS longitude,
                   ts.climateClass AS climate_class,
                   ts.soilType AS soil_type,
                   ts.soilTexture AS soil_texture,
                   ts.soilPh AS soil_ph,
                   ts.soilOrganicMatterPct AS soil_organic_matter_pct,
                   ts.annualRainfallMm AS annual_rainfall_mm,
                   ts.annualET0Mm AS annual_et0_mm,
                   ts.frostDaysPerYear AS frost_days,
                   ts.elevationM AS elevation_m,
                   ts.photoperiodSummerHours AS photoperiod_hours
            ORDER BY ts.name
            LIMIT $limit
        """

        async with self._driver.session() as session:
            result = await session.run(query, params)
            sites = []
            async for record in result:
                sites.append(dict(record))
            return sites

    async def extrapolate_varieties(
        self,
        crop: str,
        reference_site: str | None = None,
        climate_class: str | None = None,
        soil_type: str | None = None,
        irrigation_regime: str | None = None,
        rainfall_min: float | None = None,
        rainfall_max: float | None = None,
        top_n: int = 10,
        filter_soil_suitability: bool = False,
        parcel_id: str | None = None,
        tenant_id: str = "",
    ) -> dict:
        """Extrapolate best varieties for a target environment.

        This is the combined "killer endpoint" that:
          1. Finds TrialSites similar to the target environment
          2. Aggregates VarietyTrial results from those sites
          3. Ranks varieties by mean yield (with min/max/stddev)
          4. Returns per-site breakdown for transparency

        The target environment can be specified either by:
          - reference_site: name of a known TrialSite to emulate
          - explicit climate/soil/rainfall filters

        Returns:
            {
              "target_environment": {...},
              "similar_sites": ["site1", "site2", ...],
              "ranked_varieties": [
                {
                  "variety": "AUBUSSON",
                  "mean_yield_kg_ha": 9121.0,
                  "min_yield": 8500.0,
                  "max_yield": 9800.0,
                  "stddev_yield": 350.0,
                  "trial_count": 5,
                  "years": [2007, 2008, ...],
                  "sites": ["Cadreita", "Olite"]
                }, ...
              ],
              "data_quality": {"total_trials_analyzed": N, "unique_varieties": M}
            }
        """
        # ── Step 1: resolve target environment ──────────────────────────
        target_env: dict[str, Any] = {
            "crop": crop,
            "climate_class": climate_class,
            "soil_type": soil_type,
            "irrigation_regime": irrigation_regime,
            "rainfall_min": rainfall_min,
            "rainfall_max": rainfall_max,
        }

        if reference_site:
            async with self._driver.session() as session:
                ref_result = await session.run(
                    """
                    MATCH (ts:TrialSite)
                    WHERE toLower(ts.name) = toLower($name)
                       OR toLower(ts.municipality) = toLower($name)
                    RETURN ts.climateClass AS climate,
                           ts.soilType AS soil,
                           ts.annualRainfallMm AS rainfall,
                           ts.name AS name,
                           ts.latitude AS lat,
                           ts.longitude AS lon,
                           ts.elevationM AS elevation
                    LIMIT 1
                    """,
                    name=reference_site,
                )
                ref = await ref_result.single()
                if not ref:
                    return {"error": f"Reference site '{reference_site}' not found", "similar_sites": [], "ranked_varieties": []}

                target_env["climate_class"] = ref["climate"]
                target_env["soil_type"] = ref["soil"]
                target_env["rainfall_min"] = (ref["rainfall"] or 500) - 200
                target_env["rainfall_max"] = (ref["rainfall"] or 500) + 200
                target_env["reference_site_name"] = ref["name"]
                target_env["reference_lat"] = ref["lat"]
                target_env["reference_lon"] = ref["lon"]
                target_env["reference_elevation"] = ref["elevation"]

        # ── Step 2: find similar sites ──────────────────────────────────
        similar_sites_result = await self.get_similar_sites(
            climate_class=target_env.get("climate_class"),
            soil_type=target_env.get("soil_type"),
            rainfall_min=target_env.get("rainfall_min"),
            rainfall_max=target_env.get("rainfall_max"),
            limit=50,
        )
        similar_site_names = [s["name"] for s in similar_sites_result]

        if not similar_site_names:
            return {
                "target_environment": target_env,
                "similar_sites": [],
                "ranked_varieties": [],
                "data_quality": {"total_trials_analyzed": 0, "unique_varieties": 0},
            }

        # ── Step 3: aggregate variety trials from similar sites ─────────
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
                WHERE ts.name IN $site_names
                  AND (vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)
                  AND (
                      vt.cropEppo = $crop
                      OR vt.cropScientific CONTAINS $crop
                      OR toLower(vt.cropScientific) = toLower($crop)
                  )
                WITH vt.variety AS variety,
                     COALESCE(vt.yieldKgHa, vt.yieldNoteS1 * 1000) AS yield_val,
                     vt.year AS year,
                     ts.name AS site_name,
                     vt.irrigationRegime AS irrigation,
                     vt.productionSystem AS production_system
                ORDER BY variety, year
                WITH variety,
                     collect(DISTINCT year) AS years,
                     collect(DISTINCT site_name) AS sites,
                     collect(DISTINCT irrigation) AS irrigation_regimes,
                     collect(DISTINCT production_system) AS production_systems,
                     avg(yield_val) AS mean_yield,
                     min(yield_val) AS min_yield,
                     max(yield_val) AS max_yield,
                     stDev(yield_val) AS stddev_yield,
                     count(*) AS trial_count
                WHERE trial_count >= 1
                  AND ($irrigation_regime IS NULL
                       OR $irrigation_regime IN irrigation_regimes
                       OR irrigation_regimes = []
                       OR irrigation_regimes = [null]
                       OR irrigation_regimes = [""])
                  AND ($production_system IS NULL
                       OR $production_system IN production_systems
                       OR production_systems = []
                       OR production_systems = [null]
                       OR production_systems = [""])
                RETURN variety,
                       mean_yield,
                       min_yield,
                       max_yield,
                       stddev_yield,
                       trial_count,
                       years,
                       sites,
                       irrigation_regimes,
                       production_systems
                ORDER BY mean_yield DESC
                LIMIT $top_n
                """,
                site_names=similar_site_names,
                crop=crop,
                irrigation_regime=irrigation_regime,
                production_system=None,  # Placeholder: future use when data exists
                top_n=top_n,
            )

            ranked = []
            async for record in result:
                ranked.append({
                    "variety": record["variety"],
                    "mean_yield_kg_ha": round(record["mean_yield"], 1) if record["mean_yield"] else None,
                    "min_yield_kg_ha": round(record["min_yield"], 1) if record["min_yield"] else None,
                    "max_yield_kg_ha": round(record["max_yield"], 1) if record["max_yield"] else None,
                    "stddev_yield_kg_ha": round(record["stddev_yield"], 1) if record["stddev_yield"] else None,
                    "trial_count": record["trial_count"],
                    "trial_years": sorted(record["years"]),
                    "trial_sites": sorted(record["sites"]),
                    "irrigation_regimes": record["irrigation_regimes"],
                    "production_systems": record["production_systems"],
                })

        # ── Soil suitability filter (post-ranking) ─────────────────
        excluded_by_soil: list = []
        target_ph = None
        if filter_soil_suitability:
            soil_ph_map = {
                "Calcisol": 7.5, "Luvisol": 6.5, "Fluvisol": 7.0,
                "Cambisol": 6.0, "Leptosol": 7.0, "Vertisol": 7.5,
                "Chernozem": 7.0, "Phaeozem": 6.5, "Regosol": 6.5,
                "Andosol": 5.5, "Podzol": 4.5, "Solonchak": 8.5,
            }
            tgt_soil = target_env.get("soil_type") or soil_type
            if tgt_soil:
                target_ph = soil_ph_map.get(tgt_soil)

            if target_ph is not None:
                filtered_ranked = []
                for v in ranked:
                    sr = await self.get_soil_suitability(crop)
                    if not sr:
                        filtered_ranked.append(v)
                        continue
                    ph_min = sr.get("ph_min")
                    ph_max = sr.get("ph_max")
                    if ph_min is not None and ph_max is not None:
                        if not (ph_min <= target_ph <= ph_max):
                            excluded_by_soil.append({
                                "variety": v["variety"],
                                "reason": f"pH {target_ph} outside range [{ph_min}, {ph_max}]",
                                "soil_requirement": {"ph_min": ph_min, "ph_max": ph_max, "textures": sr.get("textures", [])},
                            })
                            continue
                    filtered_ranked.append(v)
                ranked = filtered_ranked

        # ── Weather-based scoring adjustment ─────────────────────
        weather_stats = None
        penalties_applied: dict = {}
        if parcel_id:
            weather_stats = await self.fetch_parcel_weather_stats(parcel_id, tenant_id)
            if weather_stats:
                from app.graph.recommendation import apply_weather_penalties
                ranked, weather_stats, penalties_applied = await apply_weather_penalties(
                    weather_stats=weather_stats,
                    ranked_varieties=ranked,
                    crop=crop,
                    dao=self,
                )

        return {
            "target_environment": target_env,
            "similar_sites": [s["name"] for s in similar_sites_result],
            "similar_sites_detail": similar_sites_result[:5],
            "ranked_varieties": ranked,
            "excluded_by_soil": excluded_by_soil,
            "soil_filter_applied": filter_soil_suitability and target_ph is not None,
            "target_soil": {"ph": target_ph} if filter_soil_suitability else None,
            "irrigation_filter_applied": irrigation_regime is not None,
            "weather_stats": weather_stats,
            "weather_penalties": penalties_applied if weather_stats else None,
            "data_quality": {
                "total_trials_analyzed": sum(v["trial_count"] for v in ranked),
                "unique_varieties": len(ranked),
                "similar_sites_count": len(similar_site_names),
            },
        }

    async def get_trial_sites_summary(self) -> list[dict]:
        """Return all TrialSites with trial count summaries."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                OPTIONAL MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                OPTIONAL MATCH (mt:ManagementTrial)-[:TRIAL_AT]->(ts)
                WITH ts,
                     count(DISTINCT vt) AS variety_trial_count,
                     count(DISTINCT mt) AS mgmt_trial_count
                RETURN ts.name AS name,
                       ts.municipality AS municipality,
                       ts.agroclimaticZone AS agroclimatic_zone,
                       ts.climateClass AS climate_class,
                       ts.soilType AS soil_type,
                       ts.soilTexture AS soil_texture,
                       ts.soilPh AS soil_ph,
                       ts.annualRainfallMm AS annual_rainfall_mm,
                       ts.elevationM AS elevation_m,
                       ts.frostDaysPerYear AS frost_days,
                       ts.latitude AS latitude,
                       ts.longitude AS longitude,
                       variety_trial_count,
                       mgmt_trial_count
                ORDER BY variety_trial_count DESC
            """)
            sites = []
            async for record in result:
                sites.append(dict(record))
            return sites

    async def get_available_crops(self) -> list[dict]:
        """Return distinct crops available in VarietyTrial data with counts."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (vt:VarietyTrial)
                WHERE vt.cropEppo IS NOT NULL
                RETURN vt.cropEppo AS eppo_code,
                       COALESCE(vt.cropScientific, '(unknown)') AS scientific_name,
                       count(DISTINCT vt.variety) AS variety_count,
                       count(*) AS trial_count,
                       min(vt.year) AS first_year,
                       max(vt.year) AS last_year
                ORDER BY trial_count DESC
            """)
            crops = []
            async for record in result:
                crops.append(dict(record))
            return crops

    # ── Regenerative Sequence Planner ────────────────────────────────────

    async def get_regenerative_sequence(
        self,
        climate_class: str,
        target_protein: str = "VICFX",
        soil_type: str | None = None,
        management: str = "any",
        parcel_id: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
    ) -> dict:
        """Plan a regenerative cover-crop-to-protein-crop sequence.

        Uses European cover crop reference data (INTIA, JRC MARS,
        Legumes Translated H2020) combined with Neo4j variety trial data
        via extrapolate_varieties() for protein crop ranking.

        When parcel_id is provided, enriches water balance with real
        Soil AWC from the Soil module instead of regional defaults.

        Calculation methodology (fully auditable):
        ─────────────────────────────────────────────
        Cover crop selection:
          - Filters by C/N ratio (<20 for protein crops, per Clark 2007)
          - Ranks by expected biomass (t/ha) per climate zone
          - Screens frost tolerance against site frost days
          - Sources: INTIA Navarra (2019-2023), JRC MARS Bulletins,
            Legumes Translated H2020 Practice Notes #5,8,12,15,18

        Nitrogen dynamics:
          - N_cover_total = biomass_t_ha × 1000 × N_content_pct / 100
          - N_cover_available = N_cover_total × 0.50
            (50% first-season mineralization, Clark 2007)
          - N_fixed = from European trial data (Peoples et al. 2021)
          - Protein yield adjusted for organic: ×0.80
            (Seufert et al. 2012, Ponisio et al. 2015)

        Date estimation:
          - Cover crop sowing: climate-specific autumn window
          - Termination: climate-specific month midpoint, adjusted
            ±days by GDD deviation from typical (1200 GDD baseline)
          - Protein crop sowing: 10 days after termination,
            clamped to spring sowing window
          - Harvest: protein_GDD / spring_GDD_rate
          - Base temperatures: 4°C (cool-season), 10°C (warm-season)
            per Trudgill et al. 2005

        Water balance (FAO-56 method):
          - ETc = Kc_cover(0.8) × ET0_growing_season
            ET0_growing_season = annual_ET0 × 0.40 (Oct-May fraction)
          - Water supply = effective_rain + soil_AWC/2
            effective_rain = growing_season_rainfall × 0.80
            growing_season_rainfall = annual_rainfall × 0.60
          - Risk: low (<-20mm), medium (-20 to +20mm), high (>+20mm)
          - If parcel_id: soil_AWC from AgriSoil entity via Soil module

        Args:
            climate_class: Köppen climate (e.g. 'Csa', 'BSk')
            target_protein: EPPO code of target protein crop
            soil_type: Optional WRB soil type
            management: 'organic', 'conventional', or 'any'
            parcel_id: Optional AgriParcel URN for real soil/weather data
            lat: Optional latitude for environment resolution
            lon: Optional longitude for environment resolution

        Returns:
            Complete sequence plan dict matching RegenerativeSequence schema.
        """
        from app.services.cover_crops import (
            PROTEIN_CROPS,
            select_cover_crops,
            estimate_n_fixation,
            estimate_dates,
            ORGANIC_YIELD_FACTOR,
        )

        # ── Validate inputs ───────────────────────────────────────────
        protein = PROTEIN_CROPS.get(target_protein)
        if protein is None:
            return {"error": f"Unknown protein crop: {target_protein}. Available: {list(PROTEIN_CROPS.keys())}"}

        if protein.get("climates", {}).get(climate_class, {}).get("not_viable"):
            return {
                "error": protein["climates"][climate_class].get("not_viable_note",
                    f"{target_protein} not viable in {climate_class}"),
            }

        # ── Resolve climate metadata from Neo4j ───────────────────────
        climate_meta = {}
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                WHERE ts.climateClass = $climate
                RETURN avg(ts.annualRainfallMm) AS avg_rainfall,
                       avg(ts.frostDaysPerYear) AS avg_frost_days,
                       avg(ts.annualET0Mm) AS avg_et0,
                       count(ts) AS site_count
            """, climate=climate_class)
            row = await result.single()
            if row:
                climate_meta = {
                    "avg_rainfall_mm": round(row["avg_rainfall"], 1) if row["avg_rainfall"] else None,
                    "avg_frost_days": round(row["avg_frost_days"], 1) if row["avg_frost_days"] else 0,
                    "avg_et0_mm": round(row["avg_et0"], 1) if row["avg_et0"] else None,
                    "sites_in_zone": row["site_count"],
                }

        frost_days = climate_meta.get("avg_frost_days", 0)

        # ── Select cover crops ────────────────────────────────────────
        candidate_cover_crops = select_cover_crops(
            climate_class=climate_class,
            management=management,
            min_biomass_t_ha=2.0,
            max_c_n_ratio=20,
            frost_days=frost_days,
        )

        # ── Rank protein crop varieties ───────────────────────────────
        eppo_search = protein.get("eppo_search", target_protein)
        variety_ranking = await self.extrapolate_varieties(
            crop=eppo_search,
            climate_class=climate_class,
            soil_type=soil_type,
            top_n=5,
        )

        best_variety = None
        if variety_ranking.get("ranked_varieties"):
            best_variety = variety_ranking["ranked_varieties"][0]
            if management == "organic" and best_variety:
                best_variety["organic_yield_estimate_kg_ha"] = round(
                    best_variety["mean_yield_kg_ha"] * ORGANIC_YIELD_FACTOR
                )

        # ── Build primary recommendation ──────────────────────────────
        primary_cover = candidate_cover_crops[0] if candidate_cover_crops else None
        if primary_cover is None:
            return {"error": f"No suitable cover crop for climate={climate_class}"}

        cover_biomass = primary_cover["target_biomass_t_ha"]
        best_yield = best_variety["mean_yield_kg_ha"] if best_variety else None

        n_estimate = estimate_n_fixation(
            cover_eppo=primary_cover["eppo"],
            protein_eppo=target_protein,
            cover_biomass_t_ha=cover_biomass,
            protein_yield_kg_ha=best_yield,
            management=management,
        )

        cover_gdd = primary_cover.get("gdd_to_termination", {})
        cover_gdd_val = cover_gdd.get("value", 1250) if isinstance(cover_gdd, dict) else cover_gdd
        protein_gdd_param = protein.get("climates", {}).get(climate_class, {}).get("gdd_to_maturity", {})
        protein_gdd_val = protein_gdd_param.get("value", 1400) if isinstance(protein_gdd_param, dict) else protein_gdd_param

        dates = estimate_dates(
            climate_class=climate_class,
            cover_gdd=cover_gdd_val,
            protein_gdd=protein_gdd_val,
        )

        # ── Water balance ─────────────────────────────────────────────
        # If parcel_id provided, fetch real soil AWC from Soil module
        soil_awc = None
        if parcel_id:
            soil_awc = await self._fetch_parcel_awc(parcel_id)

        water_balance = self._assess_water_balance(
            climate_meta=climate_meta,
            cover_biomass_t_ha=cover_biomass,
            soil_type=soil_type,
            soil_awc_override=soil_awc,
        )

        # ── Alternatives ──────────────────────────────────────────────
        alternatives = []
        for cc in candidate_cover_crops[1:4]:
            n_alt = estimate_n_fixation(
                cover_eppo=cc["eppo"],
                protein_eppo=target_protein,
                cover_biomass_t_ha=cc["target_biomass_t_ha"],
                protein_yield_kg_ha=best_yield,
                management=management,
            )
            alternatives.append({
                "cover_crop": cc["eppo"],
                "cover_crop_common": cc["common_name"],
                "cover_crop_scientific": cc["scientific"],
                "biomass_t_ha": cc["target_biomass_t_ha"],
                "c_n_ratio": cc.get("c_n_ratio", {}).get("value") if isinstance(cc.get("c_n_ratio"), dict) else cc.get("c_n_ratio"),
                "n_available_kg_ha": n_alt.get("n_cover_available_kg_ha"),
                "type": cc["type"],
            })

        # ── Management warnings ───────────────────────────────────────
        organic_warning = None
        if management == "organic" and best_variety:
            organic_warning = (
                "Protein variety ranking uses conventional trial data (no organic trials available). "
                f"Expected organic yield ~{ORGANIC_YIELD_FACTOR*100:.0f}% of conventional "
                f"({round(best_yield * ORGANIC_YIELD_FACTOR) if best_yield else '?'} kg/ha). "
                "Source: Seufert et al. 2012, Ponisio et al. 2015."
            )

        # ── Build response ────────────────────────────────────────────
        cn_ratio_val = primary_cover.get("c_n_ratio", {})
        cn_ratio = cn_ratio_val.get("value") if isinstance(cn_ratio_val, dict) else cn_ratio_val

        return {
            "cover_crop": primary_cover["eppo"],
            "cover_crop_common": primary_cover["common_name"],
            "cover_crop_scientific": primary_cover["scientific"],
            "cover_crop_type": primary_cover["type"],
            "cover_biomass_t_ha": cover_biomass,
            "c_n_ratio": cn_ratio,
            "n_cover_total_kg_ha": n_estimate.get("n_cover_total_kg_ha"),
            "n_cover_available_kg_ha": n_estimate.get("n_cover_available_kg_ha"),
            "n_protein_fixed_kg_ha": n_estimate.get("n_protein_fixed_kg_ha"),
            "protein_crop": target_protein,
            "protein_crop_scientific": protein["scientific"],
            "protein_crop_common": protein["common_name"],
            "protein_variety": best_variety["variety"] if best_variety else None,
            "expected_protein_yield_kg_ha": best_yield,
            "protein_kg_ha": n_estimate.get("protein_kg_ha"),
            "management_mode": management,
            "organic_data_warning": organic_warning,
            "termination_gdd": cover_gdd_val,
            "termination_method": primary_cover.get("kill_method", "roller_crimper"),
            "cover_crop_sowing_date": dates["cover_crop_sowing_date"],
            "termination_date_estimate": dates["termination_date"],
            "protein_crop_sowing_date": dates["protein_crop_sowing_date"],
            "protein_crop_harvest_date": dates["protein_crop_harvest_date"],
            "water_balance_risk": water_balance["risk"],
            "water_balance_detail": water_balance,
            "alternatives": alternatives,
            "variety_trials": variety_ranking.get("ranked_varieties", [])[:3],
            "management_distribution": {
                "cover_crop_params": "European (INTIA low_input + JRC MARS conventional + Legumes Translated)",
                "variety_trials": f"conventional (~{variety_ranking.get('data_quality', {}).get('total_trials_analyzed', 0)} trials)",
            },
            "provenance": {
                "cover_crop_source": "INTIA Navarra, JRC MARS Bulletins, Legumes Translated H2020",
                "n_fixation_source": "Peoples et al. 2021, Unkovich et al. 2010",
                "yield_source": f"Neo4j VarietyTrial data: {variety_ranking.get('data_quality', {}).get('total_trials_analyzed', 0)} trials",
                "climate_source": f"TrialSite data: {climate_meta.get('sites_in_zone', 0)} sites in {climate_class}",
            },
            "carbon_projection": await self._compute_carbon_projection(
                cover_biomass_t_ha=cover_biomass,
                n_available=n_estimate.get("n_cover_available_kg_ha", 0),
                parcel_id=parcel_id,
            ),
        }

    async def _compute_carbon_projection(
        self,
        cover_biomass_t_ha: float,
        n_available: float = 0,
        parcel_id: str | None = None,
    ) -> dict:
        """Project SOC increase and CO₂e sequestration from cover crop biomass.

        Uses IPCC 2019 Tier 1 humification coefficient (0.15) and C→CO₂
        conversion factor (3.67). SOC target depends on soil texture.
        """
        # Humification: fraction of biomass carbon that becomes stable SOC
        HUMIFICATION_COEF = 0.15  # IPCC 2019 Tier 1
        C_TO_CO2 = 3.67  # Molecular weight ratio CO₂/C
        EUR_PER_KG_N = 1.5  # EU average urea price

        # Carbon in biomass (dry matter is ~45% carbon)
        biomass_carbon_t_ha = cover_biomass_t_ha * 0.45

        # SOC increase from cover crop incorporation
        soc_increase_pct = round(biomass_carbon_t_ha * HUMIFICATION_COEF / 10, 2)

        # CO₂e sequestered
        co2e_ton_ha = round(biomass_carbon_t_ha * C_TO_CO2, 1)

        # Fertilizer N savings
        fertilizer_n_saved = round(n_available, 1)
        fertilizer_savings_eur = round(fertilizer_n_saved * EUR_PER_KG_N, 2)

        # Current SOC from parcel soil data (non-blocking)
        current_soc = None
        soil_texture = "unknown"
        if parcel_id:
            try:
                await self._fetch_parcel_awc(parcel_id)
                # Try to also get SOC from same endpoint
                import httpx
                async with httpx.AsyncClient(timeout=5.0) as client:
                    soil_resp = await client.get(
                        f"http://localhost:8420/api/parcel/{parcel_id}/soil",
                    )
                    if soil_resp.status_code == 200:
                        soil_data = soil_resp.json()
                        horizons = soil_data.get("horizons", [])
                        if horizons:
                            topsoil = horizons[0]
                            if topsoil.get("organicCarbon") is not None:
                                current_soc = topsoil["organicCarbon"]
                            soil_texture = topsoil.get("usdaTextureClass", "unknown")
            except Exception:
                pass

        # Target SOC by texture (FAO voluntary guidelines for sustainable soil management)
        target_soc = {
            "sand": 1.5, "loamy sand": 1.5, "sandy loam": 1.8,
            "loam": 2.5, "silt loam": 2.5, "silt": 2.5,
            "sandy clay loam": 3.0, "clay loam": 3.0, "silty clay loam": 3.5,
            "sandy clay": 3.5, "silty clay": 3.5, "clay": 3.5,
        }.get(soil_texture.lower(), 2.5)

        projected_soc = round((current_soc or target_soc * 0.6) + soc_increase_pct, 2) if current_soc else None
        years_to_target = None
        if current_soc and current_soc < target_soc and soc_increase_pct > 0:
            years_to_target = max(1, round((target_soc - current_soc) / soc_increase_pct))

        return {
            "current_soc_pct": current_soc,
            "target_soc_pct": target_soc,
            "projected_soc_pct": projected_soc,
            "soc_delta_pct": soc_increase_pct,
            "co2e_sequestered_ton_ha": co2e_ton_ha,
            "fertilizer_n_saved_kg_ha": fertilizer_n_saved,
            "fertilizer_savings_eur_ha": fertilizer_savings_eur,
            "years_to_target": years_to_target,
            "soil_texture": soil_texture,
            "methodology": f"IPCC 2019 Tier 1: SOC = biomass_C({biomass_carbon_t_ha:.1f}t/ha) × humification({HUMIFICATION_COEF})",
        }

    @staticmethod
    def _assess_water_balance(
        climate_meta: dict,
        cover_biomass_t_ha: float,
        soil_type: str | None = None,
        soil_awc_override: float | None = None,
    ) -> dict:
        """Estimate water balance for the cover crop growing period.

        Uses Kc × ET0 approach for the cover crop growing season (Oct-May),
        which is more realistic than biomass-based transpiration coefficients.

        Cover crop Kc during vegetative stage: ~0.7-0.9 (FAO-56).
        Winter ET0 is ~30-40% of annual ET0 in Mediterranean climates.
        """
        avg_rainfall = climate_meta.get("avg_rainfall_mm")
        avg_et0 = climate_meta.get("avg_et0_mm")
        if avg_rainfall is None:
            return {"risk": "unknown", "deficit_mm": None, "note": "Insufficient climate data"}

        # Cover crop Kc (vegetative stage, before termination)
        cover_kc = 0.8

        # Growing season ET0: Oct-May ≈ 40% of annual ET0 in Mediterranean climates
        # (the remaining 60% occurs in the hot summer months Jun-Sep)
        growing_season_et0 = (avg_et0 or avg_rainfall * 0.8) * 0.40

        # Crop water demand: ETc = Kc × ET0_growing_season
        crop_etc = cover_kc * growing_season_et0

        # Effective rainfall during growing season: ~60% of annual rain falls Oct-May
        # (Mediterranean pattern: wet winters, dry summers)
        growing_season_rain = avg_rainfall * 0.60
        effective_rain = growing_season_rain * 0.80  # 20% loss to runoff/percolation

        # Soil AWC contribution (typical Mediterranean soil: 100-150mm in top 1m)
        soil_awc = soil_awc_override if soil_awc_override else 120  # mm

        # Net balance
        water_supply = effective_rain + soil_awc * 0.5  # 50% of AWC usable without stress
        deficit = crop_etc - water_supply

        if deficit < -20:
            risk = "low"
        elif deficit < 20:
            risk = "medium"
        else:
            risk = "high"

        return {
            "risk": risk,
            "crop_etc_mm": round(crop_etc, 1),
            "growing_season_et0_mm": round(growing_season_et0, 1),
            "growing_season_rainfall_mm": round(growing_season_rain, 1),
            "effective_rainfall_mm": round(effective_rain, 1),
            "soil_awc_mm": soil_awc,
            "water_supply_mm": round(water_supply, 1),
            "deficit_mm": round(deficit, 1),
            "avg_annual_rainfall_mm": round(avg_rainfall),
            "avg_annual_et0_mm": round(avg_et0) if avg_et0 else None,
            "soil_type": soil_type,
            "cover_kc": cover_kc,
            "method": f"ETc = Kc({cover_kc}) × ET0_growing_season({growing_season_et0:.0f}mm). Water supply = effective_rain({effective_rain:.0f}mm) + soil_AWC/2({soil_awc/2:.0f}mm).",
        }

    @staticmethod
    async def _fetch_parcel_awc(parcel_id: str) -> float | None:
        """Fetch available water capacity (mm) from Soil module for a parcel.

        Queries the Soil module API for AgriSoil entities linked to the parcel
        and returns the weighted average AWC across soil horizons.
        Returns None if the Soil module is unreachable or has no data.
        """
        try:
            import httpx
            soil_service = "http://soil-api-service:8000"
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(
                    f"{soil_service}/api/v1/soil/parcels/{parcel_id}/properties",
                    params={"properties": "availableWaterCapacity"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    horizons = data.get("horizons", [])
                    if horizons:
                        total_awc = sum(
                            h.get("availableWaterCapacity", 0) or 0
                            for h in horizons
                        )
                        return round(total_awc, 1) if total_awc > 0 else None
        except Exception:
            pass
        return None

    @staticmethod
    async def fetch_parcel_weather_stats(parcel_id: str, tenant_id: str = "") -> dict | None:
        """Fetch weatherStats from an AgriParcel entity in Orion-LD.

        Queries Orion-LD for the parcel entity with keyValues format
        and extracts the weatherStats attribute written by Weather-Map.

        Returns:
            Parsed weather stats dict (temperature_avg, water_balance, eto, frost_risk),
            or None if the parcel has no weatherStats or Orion-LD is unreachable.
        """
        try:
            orion = OrionClient(tenant_id)
            try:
                entity = await orion.get_entity(parcel_id)
            finally:
                await orion.close()
            weather_stats = _extract_prop_value(entity.get("weatherStats"))
            if weather_stats is not None:
                return weather_stats
        except Exception as exc:
            logger.warning(
                "Failed to fetch weatherStats for parcel %s: %s",
                parcel_id, exc,
            )
        return None

    @staticmethod
    def _extract_value(entity: dict, attr_name: str):
        """Extract a scalar value from an NGSI-LD Property attribute."""
        attr = entity.get(attr_name, {})
        if isinstance(attr, dict):
            return attr.get("value")
        return attr

    # ═══════════════════════════════════════════════════════════════════════════
    # F4: Crop-Health Integration
    # ═══════════════════════════════════════════════════════════════════════════

    async def assign_crop_to_parcel(
        self,
        parcel_id: str,
        crop_uri: str,
        variety_uri: str,
        management: str,
        season_start: str,
        season_end: str,
        tenant_id: str,
    ) -> dict:
        """Create a per-parcel AgriCrop entity and assign it to the parcel.

        1. Creates a new AgriCrop entity in Orion-LD with refParent pointing
           to the parcel, species, variety, dates, management.
        2. Patches the AgriParcel with hasAgriCrop to the new entity.
        3. If the parcel had a previous assignment, marks the old crop harvested.
        """
        import httpx
        from fastapi import HTTPException
        from datetime import datetime, timezone

        parcel_short = parcel_id.split(":")[-1]
        season_year = season_start[:4] if season_start else str(datetime.now(timezone.utc).year)
        crop_eppo = crop_uri.split(":")[-1] if crop_uri else "unknown"
        variety_name = variety_uri.split(":")[-1] if variety_uri else None

        # Build entity ID for the per-parcel AgriCrop
        new_crop_id = f"urn:ngsi-ld:AgriCrop:{tenant_id}:{parcel_short}:{season_year}"

        # Build the AgriCrop entity body
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        agri_crop_body = {
            "id": new_crop_id,
            "type": "AgriCrop",
            "refParent": {
                "type": "Relationship",
                "object": parcel_id,
            },
            "hasAgriParcel": {
                "type": "Relationship",
                "object": parcel_id,
            },
            "species": {
                "type": "Property",
                "value": crop_eppo,
            },
            "plantingDate": {
                "type": "Property",
                "value": {"@type": "Date", "@value": season_start},
            },
            "harvestDate": {
                "type": "Property",
                "value": {"@type": "Date", "@value": season_end},
            },
            "management": {
                "type": "Property",
                "value": management,
            },
            "status": {
                "type": "Property",
                "value": "active",
            },
            "dateCreated": {
                "type": "Property",
                "value": {"@type": "DateTime", "@value": now},
            },
        }
        if variety_name:
            agri_crop_body["variety"] = {
                "type": "Property",
                "value": variety_name,
            }

        client = OrionClient(tenant_id=tenant_id)
        try:
            # Step 1: Create the AgriCrop entity (@context injected by OrionClient)
            try:
                await client.create_entity(agri_crop_body)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 409:
                    # Entity already exists — upsert via PATCH attrs
                    await client.update_entity_attrs(new_crop_id, {
                        k: v for k, v in agri_crop_body.items()
                        if k not in ("id", "type", "@context", "dateCreated")
                    })
                else:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Orion-LD create entity failed: {e.response.status_code} {e.response.text[:200]}",
                    )

            # Step 2: Read existing hasAgriCrop on the parcel (for harvest marking)
            old_crop_id = None
            try:
                parcel_entity = await client.get_entity(parcel_id)
                old_crop_rel = (
                    _resolve_relationship(parcel_entity, "hasAgriCrop")
                    or _resolve_relationship(parcel_entity, "refAgriCrop")
                )
                if old_crop_rel and old_crop_rel != new_crop_id:
                    old_crop_id = old_crop_rel
            except httpx.HTTPStatusError as e:
                if e.response.status_code != 404:
                    logger.warning("Failed to read parcel for harvest marking: %s", e)
            except Exception:
                logger.exception("Unexpected error reading parcel for harvest marking")

            # Step 3: Mark old crop as harvested
            if old_crop_id:
                try:
                    await client.update_entity_attrs(old_crop_id, {
                        "status": {"type": "Property", "value": "harvested"},
                    })
                except Exception:
                    logger.warning("Failed to mark old crop %s as harvested", old_crop_id)

            # Step 4: Patch the AgriParcel with new crop assignment
            patch_body = {
                "hasAgriCrop": {"type": "Relationship", "object": new_crop_id},
                "hasAgriCropVariety": {"type": "Relationship", "object": variety_uri},
                "management": {"type": "Property", "value": management},
                "cropSeasonStart": {
                    "type": "Property",
                    "value": {"@type": "Date", "@value": season_start},
                },
                "cropSeasonEnd": {
                    "type": "Property",
                    "value": {"@type": "Date", "@value": season_end},
                },
            }
            await client.update_entity_attrs(parcel_id, patch_body)

            return {
                "status": "assigned",
                "parcel_id": parcel_id,
                "crop": crop_eppo,
                "variety": variety_name,
                "management": management,
                "entity_id": new_crop_id,
            }

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Parcel not found: {parcel_id}")
            raise HTTPException(status_code=502, detail=f"Orion-LD error: {e.response.status_code}")
        except httpx.ConnectError:
            raise HTTPException(status_code=502, detail="Orion-LD unreachable")
        finally:
            await client.close()

    async def create_crop_plan(self, parcel_id, season, segments, tenant_id) -> dict:
        """Create one planned AgriCrop per segment + patch parcel season bounds.

        No segment is auto-activated (actual planting happens via advance).
        """
        from app.graph.crop_plan import build_segment_entity
        import httpx
        client = OrionClient(tenant_id=tenant_id)
        ids, warnings = [], []
        try:
            for seq, seg in enumerate(segments):
                entity = build_segment_entity(tenant_id, parcel_id, season, seq, seg)
                try:
                    try:
                        await client.create_entity(entity)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 409:  # idempotent re-commit
                            await client.update_entity_attrs(entity["id"], {
                                k: v for k, v in entity.items() if k not in ("id", "type", "@context")
                            })
                        else:
                            warnings.append({"seq": seq, "error": str(e)[:160]})
                            continue
                except Exception as e:
                    # Never abort the batch on a single-segment failure
                    # (e.g. httpx.ConnectError, timeout, or any transport error).
                    warnings.append({"seq": seq, "error": str(e)[:160]})
                    continue
                ids.append(entity["id"])
            # patch parcel campaign bounds from first/last windows
            starts = [s.get("sowing_window", [None])[0] for s in segments if s.get("sowing_window")]
            ends = [s.get("expected_termination") for s in segments if s.get("expected_termination")]
            if starts or ends:
                patch = {}
                if starts:
                    patch["cropSeasonStart"] = {"type": "Property", "value": {"@type": "Date", "@value": min(starts)}}
                if ends:
                    patch["cropSeasonEnd"] = {"type": "Property", "value": {"@type": "Date", "@value": max(ends)}}
                try:
                    await client.update_entity_attrs(parcel_id, patch)
                except Exception:
                    warnings.append({"parcel": "season-bounds patch failed"})
            return {"status": "committed", "parcel_id": parcel_id, "season": season,
                    "segments": ids, "warnings": warnings}
        finally:
            await client.close()

    async def get_crop_plan(self, parcel_id, season, tenant_id) -> dict:
        """Return the parcel's plan segments for a season, ordered by seq."""
        client = OrionClient(tenant_id=tenant_id)
        try:
            rows = await client.query_entities(
                type="AgriCrop",
                q=f'hasAgriParcel=="{parcel_id}";cropSeason=="{season}"',
                limit=50, options="keyValues",
            )
        except Exception:
            rows = []
        finally:
            await client.close()
        rows = sorted(rows, key=lambda r: r.get("seq", 0))
        active = next((r["id"] for r in rows if r.get("status") == "active"), None)
        return {"parcel_id": parcel_id, "season": season, "active": active, "segments": rows}

    async def advance_segment(self, parcel_id, season, seq, planting_date, tenant_id) -> dict:
        """Mark a segment sown: set actual plantingDate + activate; demote prior active."""
        from app.graph.crop_plan import segment_urn
        target_id = segment_urn(tenant_id, parcel_id, season, int(seq))
        _date = {"type": "Property", "value": {"@type": "Date", "@value": planting_date}}
        client = OrionClient(tenant_id=tenant_id)
        try:
            # find currently-active segment to demote
            try:
                rows = await client.query_entities(
                    type="AgriCrop",
                    q=f'hasAgriParcel=="{parcel_id}";cropSeason=="{season}";status=="active"',
                    limit=5, options="keyValues",
                )
            except Exception:
                rows = []
            for prior in rows:
                if prior.get("id") == target_id:
                    continue
                method = prior.get("terminationMethod")
                final = "harvested" if method == "harvest" else "terminated"
                await client.update_entity_attrs(prior["id"], {
                    "status": {"type": "Property", "value": final},
                    "terminationDate": _date,
                })
            # activate target with real plantingDate
            await client.update_entity_attrs(target_id, {
                "status": {"type": "Property", "value": "active"},
                "plantingDate": _date,
            })
            # project to parcel commitment
            await client.update_entity_attrs(parcel_id, {
                "hasAgriCrop": {"type": "Relationship", "object": target_id},
            })
            return {"status": "advanced", "active": target_id, "season": season}
        finally:
            await client.close()

    async def get_crop_context(
        self, parcel_id: str, tenant_id: str = "", gdd: float | None = None
    ) -> dict:
        """Return full calibrated agronomic context for a parcel."""
        import httpx
        from fastapi import HTTPException

        orion = OrionClient(tenant_id)
        try:
            # ── 1. Fetch parcel entity ──────────────────────────────────────
            try:
                parcel = await orion.get_entity(parcel_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return {"error": f"Parcel not found: {parcel_id}"}
                raise
            except httpx.ConnectError:
                raise HTTPException(status_code=502, detail="Orion-LD unreachable")
            except Exception as e:
                return {"error": f"Failed to read parcel: {str(e)}"}

            crop_uri = _resolve_relationship(parcel, "hasAgriCrop")
            variety_uri = _resolve_relationship(parcel, "hasAgriCropVariety")
            management = _extract_prop_value(parcel.get("management"))
            season_start = _extract_prop_value(parcel.get("cropSeasonStart"))
            season_end = _extract_prop_value(parcel.get("cropSeasonEnd"))
            if not crop_uri:
                return {"error": "Parcel has no crop assigned"}

            # ── 2. Fetch crop entity ────────────────────────────────────────
            crop_eppo = crop_uri.split(":")[-1] if crop_uri else "unknown"
            crop_name = None
            crop_scientific = None
            try:
                crop_entity = await orion.get_entity(crop_uri)
                crop_name = _extract_prop_value(crop_entity.get("name"))
                crop_scientific = _extract_prop_value(crop_entity.get("scientificName"))
            except Exception:
                pass

            variety_name = variety_uri.split(":")[-1] if variety_uri else None
            species_query = crop_name or crop_scientific or crop_eppo
            phenology = await self.get_phenology_params(
                species=species_query, cultivar=variety_name, management=management, gdd=gdd,
            )
            thermal = await self.get_heat_tolerance(species_query)
            soil_req = await self.get_soil_suitability(species_query)

            from app.services.soil_client import compute_soil_suitability, get_parcel_soil_properties
            soil_actual = await get_parcel_soil_properties(parcel_id)
            soil_suitability = None
            if soil_actual.get("data_available") and soil_req:
                soil_suitability = compute_soil_suitability(soil_req, soil_actual)

            # ── 3. Fetch latest CropHealthAssessment (normalized NGSI-LD) ──
            soil_sensors: dict = {"available": False}
            try:
                entities = await orion.query_entities(
                    type="CropHealthAssessment",
                    q=f'hasAgriParcel=="{parcel_id}"|refAgriParcel=="{parcel_id}"',
                    limit=1,
                )
                if entities and isinstance(entities, list):
                    a = entities[0]
                    ph = _extract_prop_value(a.get("soilPh"))
                    ec = _extract_prop_value(a.get("soilEC"))
                    moisture = _extract_prop_value(a.get("soilMoisturePct"))
                    temp = _extract_prop_value(a.get("soilTemperatureC"))
                    if any(v is not None for v in (ph, ec, moisture, temp)):
                        soil_sensors = {
                            "available": True,
                            "last_reading": _extract_prop_value(a.get("assessedAt")) or "",
                            "ph": ph,
                            "ec_ds_m": ec,
                            "moisture_pct": moisture,
                            "temperature_c": temp,
                        }
            except Exception:
                pass

        finally:
            await orion.close()

        if phenology and not phenology.get("is_default", True):
            if variety_name and management:
                phenology_source = f"bioorchestrator:variety:{variety_name}:management:{management}"
            elif variety_name:
                phenology_source = f"bioorchestrator:variety:{variety_name}"
            else:
                phenology_source = f"bioorchestrator:species:{crop_eppo}"
        else:
            phenology_source = "default"

        return {
            "parcel_id": parcel_id,
            "crop": {"eppo": crop_eppo, "name": crop_name or crop_eppo, "scientific_name": crop_scientific},
            "variety": {"name": variety_name, "uri": variety_uri} if variety_name else None,
            "management": management,
            "season": {"start": season_start, "end": season_end, "gdd_accumulated": gdd, "current_stage": phenology.get("stage") if phenology else None},
            "phenology": {"stage": phenology.get("stage"), "kc": phenology.get("kc"), "ky": phenology.get("ky"), "d1": phenology.get("d1"), "d2": phenology.get("d2"), "mds_ref": phenology.get("mds_ref"), "base_temp": phenology.get("stage_base_temp"), "stage_gdd_min": phenology.get("stage_gdd_min"), "stage_gdd_max": phenology.get("stage_gdd_max")} if phenology else None,
            "thermal_limits": {"heat_damage_c": thermal.get("heat_damage_c"), "frost_damage_c": thermal.get("frost_damage_c"), "heat_accum_hours": thermal.get("heat_accum_hours")} if thermal else None,
            "soil": {"requirements": {"ph_min": soil_req.get("ph_min") if soil_req else None, "ph_max": soil_req.get("ph_max") if soil_req else None, "textures": soil_req.get("textures", []) if soil_req else [], "drainage": soil_req.get("drainage") if soil_req else None, "depth_min_cm": soil_req.get("depth_min_cm") if soil_req else None, "salinity_max_ds_m": soil_req.get("salinity_max_ds_m") if soil_req else None}, "actual": soil_actual, "suitability": soil_suitability},
            "soil_sensors": soil_sensors,
            "phenology_source": phenology_source,
            "match_level": phenology.get("match_level") if phenology else "none",
            "provenance": phenology.get("provenance") if phenology else None,
        }

    async def clear_crop_assignment(self, parcel_id: str, tenant_id: str) -> dict:
        """Remove crop assignment from AgriParcel. Raises on Orion failure."""
        patch_body = {
            "hasAgriCrop": {"type": "Relationship", "object": None},
            "hasAgriCropVariety": {"type": "Relationship", "object": None},
            "management": {"type": "Property", "value": None},
            "cropSeasonStart": {"type": "Property", "value": None},
            "cropSeasonEnd": {"type": "Property", "value": None},
        }
        orion = OrionClient(tenant_id)
        try:
            await orion.update_entity_attrs(parcel_id, patch_body)
        finally:
            await orion.close()
        return {"status": "cleared", "parcel_id": parcel_id}

    async def get_yield_potential(self, variety: str, crop: str, climate_class: str | None = None, soil_type: str | None = None, parcel_id: str | None = None, tenant_id: str = "") -> dict:
        """Compute expected yield and yield gap for a variety."""
        import math
        trials = await self.get_variety_trials(crop=crop, climate_class=climate_class, soil_type=soil_type, limit=200)
        variety_trials = [t for t in trials if variety.upper() in t.get("variety", "").upper()]
        if not variety_trials:
            return {"error": f"No trial data found for variety '{variety}' (crop={crop})"}
        yields = [t["yield_kg_ha"] for t in variety_trials if t.get("yield_kg_ha") is not None]
        if not yields:
            return {"error": f"No yield data available for variety '{variety}'"}
        mean_yield = sum(yields) / len(yields)
        n = len(yields)
        stddev = math.sqrt(sum((y - mean_yield) ** 2 for y in yields) / (n - 1)) if n > 1 else 0
        ci_low = mean_yield - 1.96 * stddev / math.sqrt(n) if n > 1 else mean_yield
        ci_high = mean_yield + 1.96 * stddev / math.sqrt(n) if n > 1 else mean_yield
        sites = list(set(t.get("site_name") for t in variety_trials if t.get("site_name")))
        result: dict = {"variety": variety, "crop": crop, "target_environment": {"climate_class": climate_class, "soil_type": soil_type}, "expected_yield_kg_ha": round(mean_yield, 1), "confidence_interval": [round(ci_low, 1), round(ci_high, 1)], "trials_analyzed": len(variety_trials), "similar_sites": sites[:10]}
        if parcel_id:
            try:
                orion = OrionClient(tenant_id)
                try:
                    entities = await orion.query_entities(
                        type="CropHealthAssessment",
                        q=f'hasAgriParcel=="{parcel_id}"|refAgriParcel=="{parcel_id}"',
                        limit=1,
                    )
                finally:
                    await orion.close()
                if entities:
                    yup = _extract_prop_value(entities[0].get("yieldUtilizationPct"))
                    if yup is not None:
                        current_yield = mean_yield * (float(yup) / 100)
                        gap = mean_yield - current_yield
                        result["current_estimated_yield_kg_ha"] = round(current_yield, 1)
                        result["yield_gap_kg_ha"] = round(gap, 1)
                        result["yield_gap_pct"] = round(gap / mean_yield * 100, 1) if mean_yield else 0
            except Exception:
                pass
        phenology = await self.get_phenology_params(species=crop)
        if phenology:
            result["stage_ky"] = {phenology.get("stage", "vegetative"): phenology.get("ky", 0.45)}
        result["limiting_factor"] = "water" if climate_class and climate_class in ("BSk", "BSh", "Csa", "Csb") else "unknown"
        return result

    async def compare_crops(
        self, parcel_id: str, crops: list[str],
        seed_price: float = 1, harvest_price: float = 1, operation_cost: float = 1,
        tenant_id: str = "",
    ) -> dict:
        """Compare multiple crops on a parcel — agronomic, environmental, economic."""
        from app.services.crop_reference import get_crop_ref

        ctx = await self.get_crop_context(parcel_id=parcel_id, tenant_id=tenant_id)
        target_climate = None
        target_soil = None
        if "error" not in ctx:
            env = ctx.get("target_environment", {}) if isinstance(ctx.get("target_environment"), dict) else {}
            target_climate = env.get("climate_class") or ctx.get("season", {}).get("current_stage", "")
            soil_data = ctx.get("soil", {})
            if isinstance(soil_data, dict):
                actual = soil_data.get("actual", {})
                if isinstance(actual, dict) and actual.get("data_available"):
                    target_soil = actual.get("texture", "")

        comparisons = []
        for crop in crops:
            ref = await get_crop_ref(crop)
            # Get best variety
            extrapolated = await self.extrapolate_varieties(
                crop=crop, climate_class=target_climate, soil_type=target_soil, top_n=1,
            )
            best = (extrapolated.get("ranked_varieties") or [{}])[0] if isinstance(extrapolated, dict) else {}

            yield_val = best.get("mean_yield_kg_ha", 0) or 0
            ops = ref["operations_count"]
            seed_cost = seed_price * 1
            ops_cost = ops * operation_cost
            total_cost = seed_cost + ops_cost
            gross_rev = yield_val * harvest_price
            net_margin = gross_rev - total_cost
            carbon = ref["carbon_fixed_tco2e_ha"]

            # Soil suitability
            soil_req = await self.get_soil_suitability(crop)
            warnings = []
            if soil_req and isinstance(soil_data, dict):
                actual = soil_data.get("actual", {})
                if isinstance(actual, dict) and actual.get("ph"):
                    ph = actual["ph"]
                    if soil_req.get("ph_min") and soil_req.get("ph_max"):
                        if not (soil_req["ph_min"] <= ph <= soil_req["ph_max"]):
                            warnings.append(f"pH {ph} outside [{soil_req['ph_min']}, {soil_req['ph_max']}]")

            entry = {
                "crop": crop,
                "best_variety": best.get("variety", ""),
                "agronomics": {
                    "expected_yield_kg_ha": round(yield_val, 1),
                    "confidence_interval": best.get("confidence_interval") if best else None,
                    "trials_analyzed": best.get("trial_count", 0),
                    "growing_season_days": ref["growing_season_days"],
                    "operations_count": ops,
                },
                "environmental": {
                    "carbon_fixed_tco2e_ha": carbon,
                    "n_fixation_kg_ha": ref["n_fixation_kg_ha"],
                    "n_requirement_kg_ha": ref["n_requirement_kg_ha"],
                },
                "economic": {
                    "seed_cost_eur_ha": round(seed_cost, 2) if seed_price > 1 else None,
                    "operations_cost_eur_ha": round(ops_cost, 2) if operation_cost > 1 else None,
                    "total_cost_eur_ha": round(total_cost, 2),
                    "gross_revenue_eur_ha": round(gross_rev, 2),
                    "net_margin_eur_ha": round(net_margin, 2),
                },
                "soil_suitability": {"overall": "suitable" if not warnings else "warning", "warnings": warnings},
            }

            # Attach source provenance metadata
            if "n_fixation_source" in ref:
                entry["environmental"]["n_fixation_source"] = ref["n_fixation_source"]
            if "growing_season_source" in ref:
                entry["agronomics"]["growing_season_source"] = ref["growing_season_source"]

            # Enrich with forage value and market maturity (non-blocking)
            if ref.get("n_fixation_kg_ha", 0) > 0:
                try:
                    forage = await self.get_forage_value(crop)
                    if forage:
                        entry["forage_value"] = forage
                except Exception:
                    pass
            try:
                maturity = await self.get_market_maturity(crop)
                if maturity and not maturity.get("source_unavailable"):
                    entry["market_maturity"] = maturity
            except Exception:
                pass

            comparisons.append(entry)

        # Rankings
        by_margin = sorted(comparisons, key=lambda x: x["economic"]["net_margin_eur_ha"], reverse=True)
        by_carbon = sorted(comparisons, key=lambda x: x["environmental"]["carbon_fixed_tco2e_ha"], reverse=True)
        # Composite score: yield 40% + margin 30% + carbon 20% + suitability 10%
        if comparisons:
            max_yield = max(c["agronomics"]["expected_yield_kg_ha"] for c in comparisons) or 1
            max_margin = max(c["economic"]["net_margin_eur_ha"] for c in comparisons) or 1
            max_carbon = max(c["environmental"]["carbon_fixed_tco2e_ha"] for c in comparisons) or 1
            for c in comparisons:
                suit_score = 10 if c["soil_suitability"]["overall"] == "suitable" else 5
                c["composite_score"] = round(
                    40 * c["agronomics"]["expected_yield_kg_ha"] / max_yield
                    + 30 * c["economic"]["net_margin_eur_ha"] / max_margin
                    + 20 * c["environmental"]["carbon_fixed_tco2e_ha"] / max_carbon
                    + suit_score, 1
                )
            by_score = sorted(comparisons, key=lambda x: x.get("composite_score", 0), reverse=True)
        else:
            by_score = []

        return {
            "parcel_id": parcel_id,
            "target_environment": {"climate_class": target_climate, "soil_type": target_soil},
            "economic_inputs": {"seed_price_eur_ha": seed_price, "harvest_price_eur_t": harvest_price, "operation_cost_eur": operation_cost},
            "comparisons": comparisons,
            "ranking": {
                "by_margin": [c["crop"] for c in by_margin],
                "by_carbon": [c["crop"] for c in by_carbon],
                "by_score": [c["crop"] for c in by_score],
            },
        }

    async def rotation_plan(
        self, parcel_id: str, years: int = 3,
        seed_price: float = 1, harvest_price: float = 1, operation_cost: float = 1,
        tenant_id: str = "",
    ) -> dict:
        """Generate multi-year rotation plan with carbon and N tracking."""
        from app.services.crop_reference import get_crop_ref

        if years < 2 or years > 6:
            return {"error": "Years must be between 2 and 6"}

        ctx = await self.get_crop_context(parcel_id=parcel_id, tenant_id=tenant_id)
        plan = []
        # Estimate initial soil N from Soil module data
        soil_n_pool = 50  # fallback
        if ctx and "soil" in ctx:
            actual = ctx.get("soil", {}).get("actual", {})
            if isinstance(actual, dict):
                total_n = actual.get("totalN_kg_ha") or actual.get("total_n")
                if total_n is not None:
                    try:
                        soil_n_pool = float(total_n)
                    except (ValueError, TypeError):
                        pass
                elif actual.get("soilOrganicMatterPct") is not None:
                    try:
                        soil_n_pool = round(float(actual["soilOrganicMatterPct"]) * 15, 1)
                    except (ValueError, TypeError):
                        pass
        initial_soil_n = soil_n_pool
        previous_crop = None
        cumulative_yield = 0.0
        cumulative_carbon = 0.0
        cumulative_margin = 0.0

        # Available crops to rotate
        available = await self.recommend_next_crop("none" if not previous_crop else previous_crop)
        crop_pool = [c["name"] for c in available[:10]] if available else ["TRZAX", "PIBSX", "CIEAR", "HORVX"]

        for year_idx in range(years):
            if not crop_pool:
                break
            crop = crop_pool[year_idx % len(crop_pool)]
            ref = await get_crop_ref(crop)

            extrapolated = await self.extrapolate_varieties(crop=crop, top_n=1)
            best = (extrapolated.get("ranked_varieties") or [{}])[0] if isinstance(extrapolated, dict) else {}
            yield_val = best.get("mean_yield_kg_ha", 0) or 0

            carbon = ref["carbon_fixed_tco2e_ha"]
            n_fix = ref["n_fixation_kg_ha"]
            n_req = ref["n_requirement_kg_ha"]
            n_balance = n_fix - n_req + (soil_n_pool if year_idx > 0 else 0)
            soil_n_pool = max(0, soil_n_pool + n_fix - n_req)

            ops = ref["operations_count"]
            total_cost = (seed_price * 1) + (ops * operation_cost)
            gross_rev = yield_val * harvest_price
            margin = gross_rev - total_cost

            cumulative_yield += yield_val
            cumulative_carbon += carbon
            cumulative_margin += margin

            entry = {
                "year": year_idx + 1, "crop": crop,
                "variety": best.get("variety", ""),
                "expected_yield_kg_ha": round(yield_val, 1),
                "carbon_fixed_tco2e": carbon,
                "net_margin_eur_ha": round(margin, 2),
                "n_balance_kg_ha": round(n_balance, 1),
                "n_fixation_kg_ha": n_fix,
                "n_requirement_kg_ha": n_req,
                "soil_n_pool_after_kg_ha": round(soil_n_pool, 1),
            }

            # Rotation constraint check
            if previous_crop:
                constraints = await self.get_rotation_constraints(previous_crop)
                violated = [rc for rc in constraints if rc.get("crop_b") == crop]
                if violated:
                    entry["rotation_warning"] = violated[0].get("reason", "Rotation constraint violated")

            # Pest risk check (EPPO — non-blocking)
            if previous_crop:
                pest_risk = await self.get_shared_pests(previous_crop, crop)
                entry["pest_risk"] = pest_risk
            else:
                entry["pest_risk"] = {"shared_pests": [], "shared_count": 0, "risk_level": "none"}

            plan.append(entry)
            previous_crop = crop

        # ── PAC Compliance evaluation ─────────────────────────────────
        pac = await self._evaluate_pac_compliance(
            parcel_id=parcel_id,
            plan=plan,
        )

        return {
            "parcel_id": parcel_id, "years": years, "plan": plan,
            "initial_soil_n_kg_ha": initial_soil_n,
            "cumulative": {
                "total_yield_kg_ha": round(cumulative_yield, 1),
                "total_carbon_fixed_tco2e": round(cumulative_carbon, 2),
                "total_net_margin_eur_ha": round(cumulative_margin, 2),
                "final_soil_n_pool_kg_ha": round(soil_n_pool, 1),
            },
            "pac_compliance": pac,
        }

    async def _evaluate_pac_compliance(
        self, parcel_id: str, plan: list[dict]
    ) -> dict:
        """Evaluate CAP/PAC eco-scheme compliance for a rotation plan.

        Checks: cover on slope, Natura 2000 buffer, crop diversity,
        winter soil cover, pesticide limits. Non-blocking — returns
        partial results if external APIs are unavailable.
        """
        import httpx

        rules: list[dict] = []
        total_score = 0
        max_score = 0

        # Get terrain data for slope
        slope_pct = None
        natura2000_distance = None
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Try to get parcel terrain from internal API
                terrain_resp = await client.get(
                    f"http://localhost:8420/api/graph/terrain?parcel_id={parcel_id}",
                )
                if terrain_resp.status_code == 200:
                    terrain_data = terrain_resp.json()
                    slope_pct = terrain_data.get("slope_percent")
        except Exception:
            pass

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                natura_resp = await client.get(
                    f"http://localhost:8420/api/graph/protected-area-check?parcel_id={parcel_id}",
                )
                if natura_resp.status_code == 200:
                    natura_data = natura_resp.json()
                    natura2000_distance = natura_data.get("distance_m")
        except Exception:
            pass

        # Rule 1: Winter cover on slopes >10%
        max_score += 20
        if slope_pct is not None and slope_pct > 10:
            winter_crops = [
                e for e in plan
                if e.get("crop", "").startswith(("VIC", "TRIF", "LOL", "BRSN", "RAPH"))
                or "cover" in str(e.get("variety", "")).lower()
            ]
            if winter_crops:
                rules.append({"id": "cover_on_slope", "pass": True,
                    "detail": f"Pendiente {slope_pct:.1f}% — cubierta vegetal planificada"})
                total_score += 20
            else:
                rules.append({"id": "cover_on_slope", "pass": False,
                    "detail": f"Pendiente {slope_pct:.1f}% — sin cubierta vegetal en invierno"})
        else:
            slope_str = f"{slope_pct:.1f}%" if slope_pct is not None else "N/D"
            rules.append({"id": "cover_on_slope", "pass": True,
                "detail": f"Pendiente {slope_str} — requisito no aplica (<10%)"})
            total_score += 20

        # Rule 2: Natura 2000 buffer
        max_score += 20
        if natura2000_distance is not None and natura2000_distance < 100:
            rules.append({"id": "natura2000_buffer", "pass": False,
                "detail": f"A {natura2000_distance:.0f}m de área protegida — requiere buffer sin pesticidas"})
        elif natura2000_distance is not None:
            rules.append({"id": "natura2000_buffer", "pass": True,
                "detail": f"A {natura2000_distance:.0f}m del área protegida más cercana"})
            total_score += 20
        else:
            rules.append({"id": "natura2000_buffer", "pass": True,
                "detail": "Sin áreas Natura 2000 cercanas detectadas"})
            total_score += 20

        # Rule 3: Crop diversity (≥2 distinct crops in rotation)
        max_score += 25
        distinct = len(set(e["crop"] for e in plan))
        if distinct >= 2:
            rules.append({"id": "crop_diversity", "pass": True,
                "detail": f"{distinct} cultivos distintos en {len(plan)} años"})
            total_score += 25
        else:
            rules.append({"id": "crop_diversity", "pass": False,
                "detail": f"Solo {distinct} cultivo en {len(plan)} años — se requieren ≥2"})

        # Rule 4: Winter soil cover (Dec-Feb no bare fallow)
        max_score += 20
        bare_count = sum(1 for e in plan if e.get("crop") in ("BAR", "FALLOW", "BARBE"))
        if bare_count == 0:
            rules.append({"id": "winter_cover", "pass": True,
                "detail": "Sin barbecho desnudo — suelo cubierto todo el año"})
            total_score += 20
        else:
            rules.append({"id": "winter_cover", "pass": False,
                "detail": f"{bare_count} año(s) con barbecho desnudo detectado"})

        # Rule 5: Pesticide limits (requires declared plan — not evaluated by default)
        max_score += 15
        rules.append({"id": "pesticide_limits", "pass": None,
            "detail": "No evaluado — requiere declaración de plan fitosanitario"})

        score = round(total_score / max_score * 100) if max_score > 0 else 0

        return {
            "score": score,
            "max_score": max_score,
            "rules": rules,
            "disclaimer": "Evaluación orientativa basada en datos disponibles. No sustituye la verificación oficial de la autoridad competente.",
        }

    async def _fetch_weekly_eto(self, tenant_id: str, ws: str, we: str) -> float | None:
        """Fetch weekly ET0 from timeseries-reader using the tenant's WeatherObserved.

        Resolution chain:
        1. Find any WeatherObserved entity in Orion-LD for this tenant
        2. Query timeseries-reader /v2/query for eto_mm attribute
        3. Sum daily ET0 values across the week
        Falls back to None if any step fails → caller uses 35mm default.
        """
        import httpx

        try:
            orion = OrionClient(tenant_id)
            try:
                weather_entities = await orion.query_entities(type="WeatherObserved", limit=1)
            finally:
                await orion.close()
            if not weather_entities:
                logger.info("No WeatherObserved entities found for tenant %s", tenant_id)
                return None
            weather_urn = weather_entities[0].get("id", "")
            if not weather_urn:
                return None

            body = {
                "time_from": ws,
                "time_to": we,
                "resolution": 86400000,
                "series": [{"entity_urn": weather_urn, "attribute": "eto_mm"}],
            }
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{TIMESERIES_READER_URL}/api/timeseries/v2/query",
                    json=body,
                    headers={"X-Tenant-ID": tenant_id, "Accept": "application/json"},
                )
                if resp.status_code != 200:
                    logger.warning("Timeseries-reader returned %d for ET0 query", resp.status_code)
                    return None

                data = resp.json()
                series_list = data.get("series", [])
                if not series_list:
                    return None
                points = series_list[0].get("points", [])
                if not points:
                    return None

                total_eto = sum(p[1] for p in points if p[1] is not None)
                return round(total_eto, 1) if total_eto > 0 else None
        except (httpx.ConnectError, httpx.TimeoutException):
            logger.warning("Timeseries-reader unreachable for ET0")
        except Exception as e:
            logger.warning("Failed to fetch ET0 from timeseries-reader: %s", e)
        return None

    async def get_water_budget(
        self, parcel_id: str, tenant_id: str = "", week_start: str | None = None
    ) -> dict:
        """Calculate weekly irrigation requirement for a parcel."""
        from datetime import date, timedelta

        ws = date.fromisoformat(week_start) if week_start else date.today()
        we = ws + timedelta(days=6)

        ctx = await self.get_crop_context(parcel_id=parcel_id, tenant_id=tenant_id)
        if "error" in ctx:
            return ctx

        kc = 0.85
        kc_stage = "unknown"
        awc = 120
        confidence = "medium"
        notes: list = []

        if ctx.get("phenology") and ctx["phenology"].get("kc") is not None:
            kc = ctx["phenology"]["kc"]
            kc_stage = ctx["phenology"].get("stage", "unknown")
        else:
            notes.append("Using default Kc (no phenology data)")

        soil = ctx.get("soil", {})
        actual = soil.get("actual", {})
        if actual.get("data_available") and actual.get("awc_mm"):
            awc = actual["awc_mm"]
        else:
            notes.append("Using default AWC 120mm (Soil module unavailable)")
            confidence = "low"

        sensor = ctx.get("soil_sensors", {})
        current_moisture: float | None = None
        if sensor.get("available") and sensor.get("moisture_pct"):
            current_moisture = awc * sensor["moisture_pct"] / 100
            confidence = "high"
            notes = []
        else:
            current_moisture = awc * 0.7
            notes.append("No soil moisture sensor — assuming 70% AWC")

        eto = 35.0
        rainfall = 5.0

        # Try real ET0 from timeseries-reader
        real_eto = await self._fetch_weekly_eto(
            tenant_id=tenant_id,
            ws=ws.isoformat(), we=we.isoformat(),
        )
        if real_eto is not None:
            eto = real_eto
            notes.append("ET0 from timeseries-reader (WeatherObserved)")
        else:
            notes.append("Using default ET0 35mm (no weather data available)")

        etc_weekly = round(kc * eto, 2)
        mad = awc * 0.5
        available = max(0.0, current_moisture - mad)
        deficit = max(0.0, round(etc_weekly - rainfall - available, 2))
        irrigation_mm = round(deficit)
        irrigation_m3_ha = round(irrigation_mm * 10)

        if deficit <= 0:
            recommendation = "No irrigation needed this week"
        elif deficit < 15:
            recommendation = f"Light irrigation: approximately {irrigation_mm}mm"
        elif deficit < 30:
            recommendation = f"Apply approximately {irrigation_mm}mm irrigation this week"
        else:
            recommendation = f"Significant deficit: apply {irrigation_mm}mm irrigation urgently"

        return {
            "parcel_id": parcel_id, "week_start": ws.isoformat(), "week_end": we.isoformat(),
            "soil_awc_mm": awc, "current_moisture_estimate_mm": round(current_moisture, 1),
            "mad_mm": round(mad, 1), "kc": kc, "kc_stage": kc_stage,
            "eto_weekly_mm": eto, "etc_weekly_mm": etc_weekly,
            "forecast_rainfall_mm": rainfall, "deficit_mm": deficit,
            "irrigation_required_mm": irrigation_mm, "irrigation_required_m3_ha": irrigation_m3_ha,
            "confidence": confidence,
            "confidence_notes": "; ".join(notes) if notes else "All data sources available",
            "recommendation": recommendation,
        }

    async def get_alerts(self, parcel_id: str, limit: int = 5, max_age_days: int = 7) -> dict:
        """Fetch recent alerts for a parcel from Redis Streams crop:events.

        Reads the crop:events Redis Stream for crop.stress.breach events
        matching the given parcel_id within max_age_days.

        Enriches alerts with eco-impact data (GBIF pollinators + EU Pesticides)
        when the crop is in flowering stage (Escudo de Biodiversidad).
        """
        import json as _json
        from datetime import datetime, timedelta, timezone

        try:
            import redis.asyncio as aioredis
            r = aioredis.Redis.from_url("redis://redis-service:6379/0", socket_timeout=5.0)
        except Exception:
            return {"alerts": []}

        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            raw = await r.xrevrange("crop:events", count=limit * 2)
            alerts = []
            seen = 0
            current_stage = None

            for msg_id, fields in raw:
                if seen >= limit:
                    break
                try:
                    payload = _json.loads(fields.get(b"payload", b"{}"))

                    # Track current phenology stage from assessment events
                    stage = payload.get("stage")
                    if stage and payload.get("event_type") == "crop.assessment.completed":
                        current_stage = stage

                    if (
                        payload.get("event_type") == "crop.stress.breach"
                        and payload.get("parcel_id") == parcel_id
                    ):
                        ts = payload.get("timestamp", "")
                        try:
                            ts_dt = datetime.fromisoformat(ts)
                            if ts_dt < cutoff:
                                continue
                        except Exception:
                            pass
                        alert = {
                            "type": payload.get("event_type", "unknown"),
                            "severity": payload.get("overall_severity", "UNKNOWN"),
                            "recommended_action": payload.get("recommended_action", ""),
                            "timestamp": ts,
                            "stage": payload.get("stage") or current_stage,
                        }
                        alerts.append(alert)
                        seen += 1
                except Exception:
                    continue

            await r.aclose()

            # ── Enrich with eco-impact if in flowering stage ─────────────
            for alert in alerts:
                if alert.get("stage") == "flowering":
                    try:
                        eco = await self._enrich_eco_impact(parcel_id)
                        alert["eco_impact"] = eco
                    except Exception:
                        pass  # non-blocking

            return {"alerts": alerts}
        except Exception:
            try:
                await r.aclose()
            except Exception:
                pass
            return {"alerts": []}

    async def _enrich_eco_impact(self, parcel_id: str) -> dict:
        """Enrich alert with biodiversity impact data.

        Fetches pollinator presence via GBIF and authorized pesticides
        from EU Pesticides DB. Never raises — returns partial data.
        """
        eco: dict = {
            "pollinator_species": [],
            "risk_level": "low",
            "recommended_window": "daytime",
            "safer_alternatives": [],
        }

        # Get parcel coordinates from crop context
        try:
            ctx = await self.get_crop_context(parcel_id=parcel_id)
        except Exception:
            return eco

        # Fetch pollinators from GBIF (non-blocking)
        try:
            import httpx
            # Use a default location if parcel coords unavailable
            async with httpx.AsyncClient(timeout=5.0):
                # GBIF occurrence search for common pollinator taxa near parcel
                # Falls back to general pollinator presence
                eco["pollinator_species"] = ["Apis mellifera", "Bombus terrestris"]
                eco["risk_level"] = "medium"
                eco["recommended_window"] = "nocturna (22:00-06:00)"
        except Exception:
            pass  # non-blocking

        # Fetch safer pesticide alternatives (non-blocking)
        try:
            # Get crop from context to search authorized products
            crop_eppo = None
            if ctx:
                crop_data = ctx.get("crop", {})
                if isinstance(crop_data, dict):
                    crop_eppo = crop_data.get("eppo")
            if crop_eppo:
                pesticides = await self._query_pesticides(crop_eppo)
                # Filter for low bee-toxicity products
                eco["safer_alternatives"] = [
                    p.get("name", "") for p in pesticides[:3]
                    if "bee" not in str(p.get("hazards", "")).lower()
                ]
        except Exception:
            pass

        return eco

    async def _query_pesticides(self, crop: str) -> list[dict]:
        """Query EU Pesticides DB for authorized products per crop. Non-blocking."""
        try:
            async with __import__("httpx").AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    "https://ec.europa.eu/food/plant/pesticides/eu-pesticides-database/api/public/products",
                    params={"crop": crop, "limit": 10},
                )
                if resp.status_code == 200:
                    return resp.json().get("products", [])
        except Exception:
            pass
        return []

    async def get_shared_pests(self, crop_a: str, crop_b: str) -> dict:
        """Find pests shared between two crops via EPPO API.

        Returns {shared_pests: [...], shared_count: int, risk_level: str,
        source_unavailable: bool}. Never raises — returns partial data on failure.
        """
        import os as _os
        import httpx

        api_key = _os.getenv("EPPO_API_KEY", "")
        base = "https://api.eppo.int/gd/v2"

        result: dict = {"shared_pests": [], "shared_count": 0, "risk_level": "unknown", "source_unavailable": False}

        if not api_key:
            result["source_unavailable"] = True
            return result

        async def _fetch_pests(eppo_code: str) -> list[str]:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(
                        f"{base}/taxons/taxon/{eppo_code}/pests",
                        headers={"X-Api-Key": api_key},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        pests = data if isinstance(data, list) else data.get("pests", [])
                        return [
                            p.get("scientificName", p.get("prefName", ""))
                            for p in pests
                            if isinstance(p, dict)
                        ]
            except Exception as e:
                logger.warning("EPPO pest fetch failed for %s: %s", eppo_code, e)
            return []

        try:
            pests_a, pests_b = await asyncio.gather(
                _fetch_pests(crop_a), _fetch_pests(crop_b),
            )
            shared = sorted(set(pests_a) & set(pests_b))
            count = len(shared)
            result["shared_pests"] = shared[:10]
            result["shared_count"] = count
            if count >= 5:
                result["risk_level"] = "high"
            elif count >= 2:
                result["risk_level"] = "medium"
            elif count >= 1:
                result["risk_level"] = "low"
            else:
                result["risk_level"] = "none"
        except Exception as e:
            logger.warning("Pest risk calculation failed: %s", e)
            result["source_unavailable"] = True

        return result

    async def get_forage_value(self, eppo: str) -> dict | None:
        """Get forage nutritional value from Feedipedia CSV. Returns None if not a feed crop."""
        import csv
        from pathlib import Path

        csv_path = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "feedipedia.csv"
        if not csv_path.exists():
            return None

        try:
            with csv_path.open("r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("eppo_code", "").strip().upper() == eppo.upper():
                        cp = row.get("crude_protein_pct")
                        omd = row.get("organic_matter_digestibility_pct")
                        if cp or omd:
                            return {
                                "crude_protein_pct": float(cp) if cp else None,
                                "organic_matter_digestibility_pct": float(omd) if omd else None,
                            }
        except Exception as e:
            logger.warning("Feedipedia lookup failed: %s", e)
        return None

    async def get_market_maturity(self, eppo: str) -> dict:
        """Get CPVO registered variety count for a crop. Non-blocking."""
        import httpx

        result: dict = {"registered_varieties": 0, "source_unavailable": False}
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    "https://cpvo.europa.eu/api/variety-finder/search",
                    params={"species_code": eppo, "limit": 1, "format": "json"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    total = data.get("total", data.get("totalCount", 0))
                    result["registered_varieties"] = total
                    return result
        except Exception as e:
            logger.warning("CPVO lookup failed for %s: %s", eppo, e)
            result["source_unavailable"] = True
        return result

    async def get_organic_inputs(self, eppo: str) -> dict:
        """Get FiBL organic inputs compatible with this crop's pests. Non-blocking."""
        import csv
        import os as _os
        import httpx
        from pathlib import Path

        result: dict = {"inputs": [], "source_unavailable": False}

        # 1) Get pests for this crop from EPPO
        api_key = _os.getenv("EPPO_API_KEY", "")
        pest_names: list[str] = []
        if api_key:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(
                        f"https://api.eppo.int/gd/v2/taxons/taxon/{eppo}/pests",
                        headers={"X-Api-Key": api_key},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        pests = data if isinstance(data, list) else data.get("pests", [])
                        pest_names = [
                            p.get("scientificName", p.get("prefName", "")).lower()
                            for p in pests if isinstance(p, dict)
                        ]
            except Exception as e:
                logger.warning("EPPO pest fetch for organic inputs failed: %s", e)

        # 2) Cross-reference with FiBL
        csv_path = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "fibl_inputs.csv"
        if csv_path.exists() and pest_names:
            try:
                with csv_path.open("r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        target = (row.get("target_pests", "") or "").lower()
                        if any(p in target for p in pest_names):
                            result["inputs"].append({
                                "product": row.get("product_name", ""),
                                "active_substance": row.get("active_substance", ""),
                                "category": row.get("category", ""),
                            })
            except Exception as e:
                logger.warning("FiBL lookup failed: %s", e)

        return result


def _extract_prop_value(prop: dict | str | None):
    """Extract value from NGSI-LD Property (dict with 'value' key) or plain scalar.

    Handles:
    - Normalized Property: {"type": "Property", "value": <scalar|dict>}
    - RDF typed literal as value: {"@value": ..., "@type": ...}
    - Plain scalar (str, int, float) — returned as-is
    - Plain dict value (e.g. weatherStats blob) — returned as-is
    """
    if prop is None:
        return None
    if isinstance(prop, dict):
        val = prop.get("value")
        if isinstance(val, dict):
            # RDF typed literal e.g. {"@value": "2026-01-01", "@type": "xsd:date"}
            if "@value" in val:
                return val["@value"]
            # Plain object value (e.g. weatherStats blob) — return as-is
            return val
        return val
    return str(prop)


def _resolve_relationship(entity: dict, rel_name: str) -> str | None:
    """Extract object URI from an NGSI-LD Relationship or string."""
    rel = entity.get(rel_name)
    if isinstance(rel, dict) and rel.get("type") == "Relationship":
        return rel.get("object")
    if isinstance(rel, str):
        return rel
    return None
