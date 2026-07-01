"""Add the 5 peer_reviewed_study entries (rescued from graph) to phenology_sources.yaml.

These exist only in the Neo4j graph (never committed). Without this, a re-seed
from the committed YAML loses them. Adds them as additional `parameters` items
under existing stages (olive/almond/grapevine/wheat vegetative) + 1 new stage
each where needed (olive pit_hardening, wheat stem_elongation).

Idempotent: skips species/stages already containing the rescued source_short.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

YAML_PATH = Path(__file__).resolve().parents[1] / "data" / "phenology_sources.yaml"

# Rescued from graph 2026-07-01 (read-only probe). DOIs marked null are NOT
# invented — they were null in the graph too. Only Intrigliolo gets a known-DOI
# added (verifiable: Irrigation Science 2006).
RESCUED = {
    "olive": {
        "add_stage": {
            "name": "pit_hardening",
            "description": "Endocarp hardening — critical period for water stress sensitivity",
            "parameters": [{
                "cultivar": "Picual",
                "management": "regulated_deficit_irrigation",
                "climate_zone": "mediterranean",
                "kc": 0.45, "kc_ci": [0.40, 0.50],
                "d1": 2.0, "d2": 8.0,
                "mds_ref": 190.0, "mds_ref_ci": [160.0, 220.0],
                "source": {
                    "source_type": "peer_reviewed_study",
                    "doi": "10.21273/JASHS.128.3.0425",
                    "short": "Moriana et al. 2003",
                    "author": "Moriana, Orgaz, Pastor, Fereres",
                    "year": 2003,
                    "institution": "Universidad de Sevilla, IAS-CSIC",
                    "method": "Dendrometer + water balance, RDI trial, 5-year",
                    "conditions": "Olive, cv. Picual, Seville, RDI (30% ETc at pit hardening)",
                },
            }],
        },
        "add_param_to_stage": {
            "stage": "initial",  # graph called it 'vegetative'; FAO-56 base = 'initial'
            "param": {
                "cultivar": "Picual",
                "management": "deficit_irrigation",
                "climate_zone": "mediterranean",
                "kc": 0.50, "kc_ci": [0.45, 0.55],
                "d1": 1.8, "d2": 7.5,
                "mds_ref": 130.0, "mds_ref_ci": [100.0, 160.0],
                "source": {
                    "source_type": "peer_reviewed_study",
                    "doi": "10.1016/j.agwat.2004.12.005",
                    "short": "Orgaz et al. 2005",
                    "author": "Orgaz, Testi, Villalobos, Fereres",
                    "year": 2005,
                    "institution": "IAS-CSIC, Universidad de Córdoba",
                    "method": "Eddy covariance + sap flow, 3-year field trial",
                    "conditions": "Olive, cv. Picual, Córdoba, deficit irrigation 30% ETc, 4×6m",
                },
            },
        },
    },
    "almond": {
        "add_param_to_stage": {
            "stage": "initial",
            "param": {
                "cultivar": "Nonpareil",
                "management": "deficit_irrigation",
                "climate_zone": "mediterranean",
                "kc": 0.65, "kc_ci": [0.55, 0.75],
                "d1": 2.3, "d2": 8.5,
                "mds_ref": 140.0, "mds_ref_ci": [110.0, 170.0],
                "source": {
                    "source_type": "peer_reviewed_study",
                    "doi": None,  # not in graph; do NOT invent
                    "short": "Goldhamer & Fereres 2001",
                    "author": "Goldhamer, Fereres",
                    "year": 2001,
                    "institution": "UC Davis, IAS-CSIC",
                    "method": "RDI field trial, dendrometer + water balance",
                    "conditions": "Almond, cv. Nonpareil, California, RDI",
                },
            },
        },
    },
    "grapevine": {
        "add_param_to_stage": {
            "stage": "initial",
            "param": {
                "cultivar": "Tempranillo",
                "management": "deficit_irrigation",
                "climate_zone": "mediterranean",
                "kc": 0.25, "kc_ci": [0.15, 0.35],
                "d1": 1.3, "d2": 5.5,
                "mds_ref": 90.0, "mds_ref_ci": [70.0, 110.0],
                "source": {
                    "source_type": "peer_reviewed_study",
                    "doi": "10.1007/s00271-005-0005-9",
                    "short": "Intrigliolo & Castel 2006",
                    "author": "Intrigliolo, Castel",
                    "year": 2006,
                    "institution": "IVIA, Valencia",
                    "method": "Dendrometer + lysimeter, RDI trial",
                    "conditions": "Tempranillo, Valencia, RDI pre-veraison",
                },
            },
        },
    },
    "wheat": {
        "add_stage": {
            "name": "stem_elongation",
            "description": "Stem elongation to heading",
            "parameters": [{
                "cultivar": None,
                "management": "deficit_irrigation",
                "climate_zone": "semi_arid",
                "kc": 0.90, "kc_ci": [0.80, 1.00],
                "d1": 2.0, "d2": 7.0,
                "mds_ref": None,
                "source": {
                    "source_type": "peer_reviewed_study",
                    "doi": None,  # not in graph; verify exact DOI before final
                    "short": "Kang et al. 2003",
                    "author": "Kang, Zhang, Liang",
                    "year": 2003,
                    "institution": "China Agricultural University",
                    "method": "CWSI field trial, North China Plain",
                    "conditions": "Winter wheat, deficit irrigation, North China Plain",
                },
            }],
        },
    },
}


def _has_source(stage_params: list, short: str) -> bool:
    return any((p.get("source") or {}).get("short") == short for p in stage_params)


def main() -> int:
    data = yaml.safe_load(YAML_PATH.read_text())
    added_stages = added_params = skipped = 0
    for sp in data["species"]:
        name = sp["name"]
        spec = RESCUED.get(name)
        if not spec:
            continue
        stages = sp.setdefault("stages", [])
        stage_by_name = {s["name"]: s for s in stages}

        if "add_stage" in spec:
            new_stage = spec["add_stage"]
            short = new_stage["parameters"][0]["source"]["short"]
            if new_stage["name"] in stage_by_name:
                existing = stage_by_name[new_stage["name"]]
                if _has_source(existing.get("parameters", []), short):
                    skipped += 1
                    continue
                existing.setdefault("parameters", []).extend(new_stage["parameters"])
                added_params += len(new_stage["parameters"])
            else:
                stages.append(new_stage)
                added_stages += 1

        if "add_param_to_stage" in spec:
            ap = spec["add_param_to_stage"]
            short = ap["param"]["source"]["short"]
            stage = stage_by_name.get(ap["stage"])
            if not stage:
                print(f"  WARN: {name} has no stage '{ap['stage']}' — skipping {short}", file=sys.stderr)
                continue
            if _has_source(stage.get("parameters", []), short):
                skipped += 1
                continue
            stage.setdefault("parameters", []).append(ap["param"])
            added_params += 1

    out = yaml.safe_dump(data, allow_unicode=True, sort_keys=False, width=10000)
    YAML_PATH.write_text(out)
    print(f"added_stages={added_stages} added_params={added_params} skipped={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
