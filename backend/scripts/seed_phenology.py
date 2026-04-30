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
