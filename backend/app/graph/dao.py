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

    async def get_all_species(self) -> list[dict]:
        """Return all species in the knowledge graph with phenology availability."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (s:Species)
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st:PhenologyStage)-[:HAS_PARAMS]->(p:PhenologyParams)
                RETURN s.name AS name,
                       s.scientificName AS scientific_name,
                       count(DISTINCT st) AS stage_count,
                       count(DISTINCT p) AS params_count
                ORDER BY s.name
            """)
            return [
                {
                    "name": record["name"],
                    "scientific_name": record["scientific_name"],
                    "stage_count": record["stage_count"],
                    "params_count": record["params_count"],
                    "has_phenology": record["params_count"] > 0,
                }
                async for record in result
            ]

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
        where_clauses = ["vt.yieldKgHa IS NOT NULL"]
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
            where_clauses.append("vt.yieldKgHa >= $min_yield")
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
            ORDER BY vt.yieldKgHa DESC
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
                  AND vt.yieldKgHa IS NOT NULL
                  AND (
                      vt.cropEppo = $crop
                      OR vt.cropScientific CONTAINS $crop
                      OR toLower(vt.cropScientific) = toLower($crop)
                  )
                WITH vt.variety AS variety,
                     vt.yieldKgHa AS yield_val,
                     vt.year AS year,
                     ts.name AS site_name,
                     vt.irrigationRegime AS irrigation
                ORDER BY variety, year
                WITH variety,
                     collect(DISTINCT year) AS years,
                     collect(DISTINCT site_name) AS sites,
                     avg(yield_val) AS mean_yield,
                     min(yield_val) AS min_yield,
                     max(yield_val) AS max_yield,
                     stDev(yield_val) AS stddev_yield,
                     count(*) AS trial_count
                WHERE trial_count >= 1
                RETURN variety,
                       mean_yield,
                       min_yield,
                       max_yield,
                       stddev_yield,
                       trial_count,
                       years,
                       sites
                ORDER BY mean_yield DESC
                LIMIT $top_n
                """,
                site_names=similar_site_names,
                crop=crop,
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
                })

        return {
            "target_environment": target_env,
            "similar_sites": [s["name"] for s in similar_sites_result],
            "similar_sites_detail": similar_sites_result[:5],  # top 5 for brevity
            "ranked_varieties": ranked,
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
            COVER_CROPS,
            PROTEIN_CROPS,
            select_cover_crops,
            estimate_n_fixation,
            estimate_dates,
            lookup,
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
        """Write crop assignment to Orion-LD AgriParcel entity.

        Creates/updates hasAgriCrop, hasAgriCropVariety Relationships
        and management, cropSeasonStart, cropSeasonEnd Properties.
        Overwrites any existing assignment (one active crop per parcel).
        """
        import httpx
        from fastapi import HTTPException

        from app.ingestion.orion import OrionIngestionClient

        orion = OrionIngestionClient()

        patch_body = {
            "hasAgriCrop": {
                "type": "Relationship",
                "object": crop_uri,
            },
            "hasAgriCropVariety": {
                "type": "Relationship",
                "object": variety_uri,
            },
            "management": {
                "type": "Property",
                "value": management,
            },
            "cropSeasonStart": {
                "type": "Property",
                "value": {"@type": "Date", "@value": season_start},
            },
            "cropSeasonEnd": {
                "type": "Property",
                "value": {"@type": "Date", "@value": season_end},
            },
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.patch(
                    f"{orion.base}/ngsi-ld/v1/entities/{parcel_id}/attrs",
                    json=patch_body,
                    headers={
                        "Content-Type": "application/ld+json",
                        "NGSILD-Tenant": tenant_id,
                        "Fiware-Service": tenant_id,
                        "Fiware-ServicePath": "/",
                    },
                )
                if resp.status_code in (204, 200):
                    return {
                        "status": "assigned",
                        "parcel_id": parcel_id,
                        "variety": variety_uri.split(":")[-1],
                        "crop": crop_uri.split(":")[-1],
                        "management": management,
                    }
                elif resp.status_code == 404:
                    raise HTTPException(
                        status_code=404, detail=f"Parcel not found: {parcel_id}"
                    )
                else:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Orion-LD returned {resp.status_code}: {resp.text[:200]}",
                    )
        except httpx.ConnectError:
            raise HTTPException(status_code=502, detail="Orion-LD unreachable")

    async def clear_crop_assignment(
        self, parcel_id: str, tenant_id: str
    ) -> dict:
        """Remove crop assignment from AgriParcel."""
        import httpx

        from app.ingestion.orion import OrionIngestionClient

        orion = OrionIngestionClient()

        patch_body = {
            "hasAgriCrop": {"type": "Relationship", "object": None},
            "hasAgriCropVariety": {"type": "Relationship", "object": None},
            "management": {"type": "Property", "value": None},
            "cropSeasonStart": {"type": "Property", "value": None},
            "cropSeasonEnd": {"type": "Property", "value": None},
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.patch(
                f"{orion.base}/ngsi-ld/v1/entities/{parcel_id}/attrs",
                json=patch_body,
                headers={
                    "Content-Type": "application/ld+json",
                    "NGSILD-Tenant": tenant_id,
                    "Fiware-Service": tenant_id,
                    "Fiware-ServicePath": "/",
                },
            )
        return {"status": "cleared", "parcel_id": parcel_id}
