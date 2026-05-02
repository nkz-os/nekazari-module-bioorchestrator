"""Graph Data Access Object — Neo4j query layer.

All methods use the AsyncDriver and return plain dicts for JSON serialisation.
Business logic lives in app/services/.

Tenant model:
  The knowledge graph stores global reference data (AGROVOC taxonomy, EPPO codes,
  IUCN species, phenology parameters). This data is inherently multi-tenant-safe
  because it describes biological reality, not tenant-specific state.

  Tenant-scoped data (future Phase 2 features like custom DSS rules, user-created
  crop rotation plans) must be linked to a :Tenant node via [:BELONGS_TO] and
  filtered by tenant_id in every query. Methods accept an optional tenant_id
  parameter that is ignored for global data but will be enforced when
  tenant-scoped subgraphs are added.
"""

from __future__ import annotations

from typing import Any

from neo4j import AsyncDriver


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

    async def get_stats(self, tenant_id: str | None = None) -> dict[str, Any]:
        """Return node count, relationship count, and per-label counts.

        When tenant_id is provided and tenant-scoped subgraphs exist,
        results are filtered to that tenant's view. For global reference
        data this parameter is a no-op.
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
                "CALL apoc.cypher.run('MATCH (n:`' + label + '`) RETURN count(n) AS c', {}) "
                "YIELD value "
                "RETURN label, value.c AS count "
                "ORDER BY count DESC LIMIT 30"
            )
            label_records = await label_result.data()
            label_counts = {r["label"]: r["count"] for r in label_records}

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
        tenant_id: str | None = None,
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
                // Priority: explicit stage name > GDD range match > any stage
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
                WITH s, COALESCE(st, st_gdd) AS st

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
            )
            record = await result.single()
            if record is None or record["match_level"] == "none":
                return None

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
