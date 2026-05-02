"""Seed Neo4j with curated phenology parameters and scientific provenance.

Reads data/phenology_sources.yaml and populates the knowledge graph:
    (:Species)-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_PARAMETER]->(:PhenologyParams)

Each PhenologyParams node includes full provenance (DOI, author, conditions, CI).
Idempotent — uses MERGE, safe to re-run.

Usage:
    python scripts/seed_phenology.py [--data-dir ./data] [--neo4j-uri bolt://...]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import yaml
from neo4j import Driver, GraphDatabase


def connect(uri: str, user: str, password: str) -> Driver:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    print(f"[seed_phenology] Connected to {uri}")
    return driver


def seed(driver: Driver, data: dict[str, Any]) -> dict[str, int]:
    """Load phenology data into Neo4j. Returns {species, stages, params, alternatives}."""
    counts = {"species": 0, "stages": 0, "params": 0, "alternatives": 0}

    with driver.session() as session:
        # ── Stage detection config ──────────────────────────────────────
        stage_cfg = data.get("stage_detection", {})

        # ── Species + Stages + Parameters ────────────────────────────────
        for sp in data["species"]:
            sp_name = sp["name"]
            sp_sci = sp["scientific_name"]
            sp_uri = sp.get("agrovoc_uri", "")

            # Upsert Species
            session.run(
                """
                MERGE (s:Species {name: $name})
                SET s.scientificName = $sci,
                    s.agrovocUri = $uri
                """,
                name=sp_name, sci=sp_sci, uri=sp_uri,
            )
            counts["species"] += 1

            # Stage detection config for this species
            sp_stages_cfg = stage_cfg.get(sp_name, {})

            for stage in sp["stages"]:
                st_name = stage["name"]
                st_desc = stage.get("description", "")
                # GDD thresholds
                st_cfg_list = sp_stages_cfg.get("stages", [])
                st_cfg = next((s for s in st_cfg_list if s["name"] == st_name), {})
                gdd_range = st_cfg.get("gdd_from_budbreak") or st_cfg.get("gdd_from_emergence")
                base_temp = sp_stages_cfg.get("base_temp_c")

                # Upsert Stage
                session.run(
                    """
                    MATCH (s:Species {name: $sp_name})
                    MERGE (s)-[:HAS_STAGE]->(st:PhenologyStage {name: $st_name})
                    SET st.description = $desc,
                        st.baseTemp = $base_temp,
                        st.gddMin = $gdd_min,
                        st.gddMax = $gdd_max
                    """,
                    sp_name=sp_name,
                    st_name=st_name,
                    desc=st_desc,
                    base_temp=base_temp,
                    gdd_min=gdd_range[0] if gdd_range else None,
                    gdd_max=gdd_range[1] if gdd_range else None,
                )
                counts["stages"] += 1

                for i, param in enumerate(stage.get("parameters", [])):
                    src = param.get("source", {})

                    # Upsert Parameter node with provenance
                    result = session.run(
                        """
                        MATCH (:Species {name: $sp_name})-[:HAS_STAGE]->
                              (st:PhenologyStage {name: $st_name})
                        MERGE (st)-[:HAS_PARAMETER]->(p:PhenologyParams {
                            cultivar: COALESCE($cultivar, "__generic__"),
                            management: COALESCE($mgmt, "__standard__"),
                            climateZone: COALESCE($climate_zone, "__any__")
                        })
                        SET p.kc = $kc,
                            p.kcCiLow = $kc_ci_low,
                            p.kcCiHigh = $kc_ci_high,
                            p.d1 = $d1,
                            p.d1CiLow = $d1_ci_low,
                            p.d1CiHigh = $d1_ci_high,
                            p.d2 = $d2,
                            p.d2CiLow = $d2_ci_low,
                            p.d2CiHigh = $d2_ci_high,
                            p.mdsRef = $mds_ref,
                            p.mdsRefCiLow = $mds_ref_ci_low,
                            p.mdsRefCiHigh = $mds_ref_ci_high,
                            p.sourceDoi = $src_doi,
                            p.sourceShort = $src_short,
                            p.sourceAuthor = $src_author,
                            p.sourceYear = $src_year,
                            p.sourceInstitution = $src_institution,
                            p.sourceMethod = $src_method,
                            p.sourceConditions = $src_conditions,
                            p.isDefault = $is_default
                        RETURN p
                        """,
                        sp_name=sp_name,
                        st_name=st_name,
                        cultivar=param.get("cultivar"),
                        mgmt=param.get("management"),
                        climate_zone=param.get("climate_zone"),
                        kc=param.get("kc"),
                        kc_ci_low=param["kc_ci"][0] if param.get("kc_ci") else None,
                        kc_ci_high=param["kc_ci"][1] if param.get("kc_ci") else None,
                        d1=param.get("d1"),
                        d1_ci_low=param["d1_ci"][0] if param.get("d1_ci") else None,
                        d1_ci_high=param["d1_ci"][1] if param.get("d1_ci") else None,
                        d2=param.get("d2"),
                        d2_ci_low=param["d2_ci"][0] if param.get("d2_ci") else None,
                        d2_ci_high=param["d2_ci"][1] if param.get("d2_ci") else None,
                        mds_ref=param.get("mds_ref"),
                        mds_ref_ci_low=param["mds_ref_ci"][0] if param.get("mds_ref_ci") else None,
                        mds_ref_ci_high=param["mds_ref_ci"][1] if param.get("mds_ref_ci") else None,
                        src_doi=src.get("doi"),
                        src_short=src.get("short"),
                        src_author=src.get("author"),
                        src_year=src.get("year"),
                        src_institution=src.get("institution"),
                        src_method=src.get("method"),
                        src_conditions=src.get("conditions"),
                        is_default=(i == 0),  # first param per stage = default
                    )
                    counts["params"] += 1

                    # ── Alternatives ─────────────────────────────────────
                    for alt in param.get("alternatives", []):
                        session.run(
                            """
                            MATCH (:Species {name: $sp_name})-[:HAS_STAGE]->
                                  (st:PhenologyStage {name: $st_name})-[:HAS_PARAMETER]->
                                  (p:PhenologyParams {cultivar: $cultivar, management: $mgmt})
                            MERGE (alt_node:PhenologyAlternative {
                                cultivar: $cultivar,
                                management: $mgmt,
                                sourceShort: $alt_source
                            })
                            SET alt_node.kc = $kc,
                                alt_node.sourceDoi = $alt_doi,
                                alt_node.conditions = $alt_conditions
                            MERGE (p)-[:HAS_ALTERNATIVE]->(alt_node)
                            """,
                            sp_name=sp_name,
                            st_name=st_name,
                            cultivar=param.get("cultivar"),
                            mgmt=param.get("management"),
                            kc=alt.get("kc"),
                            alt_source=alt.get("source_short", ""),
                            alt_doi=alt.get("source_doi"),
                            alt_conditions=alt.get("conditions", ""),
                        )
                        counts["alternatives"] += 1

        # ── Seed Heat Tolerance ────────────────────────────────────────
        ht_data = data.get("crop_heat_tolerance", {})
        for sp_name, ht in ht_data.items():
            src = ht.get("source", {})
            session.run(
                """
                MATCH (s:Species {name: $species})
                MERGE (s)-[:HAS_HEAT_TOLERANCE]->(h:CropHeatTolerance {species: $species})
                SET h.heatDamageThresholdC = $heat,
                    h.frostDamageThresholdC = $frost,
                    h.heatAccumHours = $hours,
                    h.sourceShort = $src_short,
                    h.sourceDoi = $src_doi,
                    h.sourceAuthor = $src_author,
                    h.sourceYear = $src_year,
                    h.sourceConditions = $src_cond
                """,
                species=sp_name,
                heat=ht.get("heat_damage_threshold_c"),
                frost=ht.get("frost_damage_threshold_c"),
                hours=ht.get("heat_accum_hours"),
                src_short=src.get("short"),
                src_doi=src.get("doi"),
                src_author=src.get("author"),
                src_year=src.get("year"),
                src_cond=src.get("conditions"),
            )
            counts["params"] += 1

        # ── Seed Nutrient Profiles ──────────────────────────────────────
        np_data = data.get("crop_nutrient_profiles", {})
        for sp_name, nutrients in np_data.items():
            src = nutrients.get("source", {})
            for element in ["nitrogen", "phosphorus", "potassium"]:
                el_data = nutrients.get(element, {})
                for stage_name, vals in el_data.items():
                    if stage_name == "source":
                        continue
                    session.run(
                        """
                        MATCH (s:Species {name: $species})-[:HAS_STAGE]->(st:PhenologyStage {name: $stage})
                        MERGE (st)-[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile {species: $species, stage: $stage, element: $el})
                        SET n.uptakeKgHaDay = $uptake,
                            n.totalKgHa = $total,
                            n.sourceShort = $src_short,
                            n.sourceDoi = $src_doi
                        """,
                        species=sp_name,
                        stage=stage_name,
                        el=element,
                        uptake=vals.get("uptake_kg_ha_day"),
                        total=vals.get("total_kg_ha"),
                        src_short=src.get("short"),
                        src_doi=src.get("doi"),
                    )
                    counts["params"] += 1

        # ── Seed Soil Suitability ───────────────────────────────────────
        ss_data = data.get("crop_soil_suitability", {})
        for sp_name, ss in ss_data.items():
            src = ss.get("source", {})
            session.run(
                """
                MATCH (s:Species {name: $species})
                MERGE (s)-[:HAS_SOIL_SUITABILITY]->(ssn:CropSoilSuitability {species: $species})
                SET ssn.phMin = $ph_min, ssn.phMax = $ph_max,
                    ssn.textures = $textures, ssn.drainage = $drainage,
                    ssn.depthMinCm = $depth, ssn.salinityMaxDsM = $salinity,
                    ssn.sourceShort = $src_short
                """,
                species=sp_name,
                ph_min=ss.get("ph_min"), ph_max=ss.get("ph_max"),
                textures=ss.get("textures", []), drainage=ss.get("drainage", []),
                depth=ss.get("depth_min_cm"), salinity=ss.get("salinity_max_ds_m"),
                src_short=src.get("short"),
            )
            counts["params"] += 1

        # ── Seed Rotation Constraints ───────────────────────────────────
        rc_data = data.get("rotation_constraints", [])
        for rc in rc_data:
            session.run(
                """
                MERGE (r:RotationConstraint {cropA: $crop_a, cropB: $crop_b})
                SET r.intervalYears = $interval,
                    r.reason = $reason,
                    r.sourceShort = $src_short
                """,
                crop_a=rc.get("crop_a"), crop_b=rc.get("crop_b"),
                interval=rc.get("interval_years"),
                reason=rc.get("reason"),
                src_short=rc.get("source_short"),
            )
            counts["params"] += 1

    return counts


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed Neo4j with curated phenology parameters"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("DATA_DIR", "./data")),
        help="Directory containing phenology_sources.yaml",
    )
    parser.add_argument(
        "--neo4j-uri",
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    )
    parser.add_argument(
        "--neo4j-user",
        default=os.getenv("NEO4J_USER", "neo4j"),
    )
    parser.add_argument(
        "--neo4j-password",
        default=os.getenv("NEO4J_PASSWORD", "bioorchestrator"),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse YAML but do not write to Neo4j",
    )
    args = parser.parse_args()

    yaml_path = args.data_dir / "phenology_sources.yaml"
    if not yaml_path.exists():
        print(f"[seed_phenology] ERROR: {yaml_path} not found")
        raise SystemExit(1)

    with yaml_path.open("r") as f:
        data = yaml.safe_load(f)

    species_count = len(data.get("species", []))
    print(f"[seed_phenology] Loaded {species_count} species from {yaml_path}")

    if args.dry_run:
        for sp in data["species"]:
            stages = len(sp.get("stages", []))
            params = sum(len(s.get("parameters", [])) for s in sp.get("stages", []))
            print(f"  {sp['name']}: {stages} stages, {params} parameter sets")
        return

    driver = connect(args.neo4j_uri, args.neo4j_user, args.neo4j_password)
    try:
        counts = seed(driver, data)
        print(
            f"[seed_phenology] Done — "
            f"{counts['species']} species, "
            f"{counts['stages']} stages, "
            f"{counts['params']} parameter sets, "
            f"{counts['alternatives']} alternatives"
        )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
