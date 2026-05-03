# BioOrchestrator — NKZ Module

Multi-domain biodiversity knowledge graph and phenology parameter API for the [Nekazari](https://nkz-os.org) precision agriculture platform.

## What it does

BioOrchestrator is the **knowledge layer** of the crop health ecosystem. It stores scientifically-provenanced agronomic parameters (crop coefficients, phenology baselines, soil requirements, nutrient profiles, rotation constraints) in a Neo4j graph database and exposes them via REST API.

It powers the [Crop Health Engine](https://github.com/nkz-os/nekazari-module-crop-health) with real-time phenology parameters and provides a **Phenology Browser** UI for researchers to explore parameters with DOI traceability.

## Architecture

```
IkerKeta (ETL pipeline)
    │
    │  JSON-LD export
    ▼
Neo4j + n10s (Knowledge Graph)
    │
    │  REST API
    ▼
┌─────────────────────────────┐
│ BioOrchestrator             │
│                             │
│ FastAPI (port 8420)         │
│ ├── /api/graph/phenology-params  ← Crop Health consumes this
│ ├── /api/graph/soil-data         ← SoilGrids proxy
│ ├── /api/graph/heat-tolerance    ← Thermal damage thresholds
│ ├── /api/graph/nutrient-profile  ← NPK uptake curves
│ ├── /api/graph/recommendations/  ← Planner: rotation, fertilizer
│ └── /api/v1/*                    ← IkerKeta native API
│                             │
│ React IIFE (frontend)       │
│ ├── Sources Dashboard       │
│ ├── Pipeline Runner         │
│ ├── Phenology Browser       │
│ └── DAD-IS Explorer         │
└─────────────────────────────┘
```

## Key features

- **Phenology parameters** with scientific provenance (DOI, confidence intervals, alternative values)
- **GDD-based stage auto-detection** — pass `gdd=1240` and get the correct phenological stage
- **Cascade matching**: exact cultivar → management → generic → species-only
- **Scientific contribution UI** — researchers can submit parameters for review
- **Planner**: rule-based crop rotation, fertilizer, soil suitability, and scenario simulation
- **7 proxy endpoints** for external APIs: SoilGrids, Natura 2000, EU Pesticides, CPVO Varieties, GBIF, Copernicus DEM, ERA5
- **25 IkerKeta connectors** feeding the knowledge graph

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe |
| GET | `/readyz` | Readiness probe |
| GET | `/api/graph/health` | Neo4j connectivity |
| GET | `/api/graph/stats` | Node/relationship counts |
| GET | `/api/graph/phenology-params` | Query parameters (species, stage, cultivar, gdd, lat, lon) |
| POST | `/api/graph/phenology-params/contribute` | Submit parameter for review |
| GET | `/api/graph/heat-tolerance` | Thermal damage thresholds per species |
| GET | `/api/graph/nutrient-profile` | NPK uptake per stage |
| GET | `/api/graph/soil-suitability` | Soil requirements per species |
| GET | `/api/graph/soil-data` | Real-time SoilGrids 2.0 proxy |
| GET | `/api/graph/protected-area-check` | Natura 2000 check |
| GET | `/api/graph/varieties` | CPVO registered varieties |
| GET | `/api/graph/pesticides` | EU authorized substances |
| GET | `/api/graph/pollinators` | GBIF pollinator species |
| GET | `/api/graph/terrain` | Copernicus DEM elevation |
| GET | `/api/graph/climate-reference` | ERA5 climate data |
| GET | `/api/graph/rotation-constraints` | Rotation rules |
| GET | `/api/graph/recommendations/next-crop` | Crop suggestion |
| GET | `/api/graph/recommendations/fertilizer` | NPK needs |
| GET | `/api/graph/recommendations/simulate` | Compare scenarios |

## Knowledge Graph (Neo4j)

| Entity Type | Count | Description |
|-------------|-------|-------------|
| Species | 4 | Olive, Almond, Grapevine, Wheat |
| PhenologyStage | 9 | Per-species growth stages with GDD thresholds |
| PhenologyParams | 14 | Kc, D1, D2, MDS_ref with DOI provenance |
| CropHeatTolerance | 4 | Thermal damage thresholds |
| CropNutrientProfile | 27 | NPK uptake per stage |
| CropSoilSuitability | 4 | pH, texture, drainage requirements |
| RotationConstraint | 6 | Crop rotation rules |

## Data sources

BioOrchestrator proxies 7 external APIs via IkerKeta connectors:

| Source | License | Endpoint |
|--------|---------|----------|
| SoilGrids 2.0 (ISRIC) | CC-BY 4.0 | `/api/graph/soil-data` |
| Copernicus DEM (GLO-30) | Copernicus free | `/api/graph/terrain` |
| Natura 2000 (EEA) | EEA reuse | `/api/graph/protected-area-check` |
| EU Pesticides DB | EC public data | `/api/graph/pesticides` |
| CPVO Variety Database | CPVO public | `/api/graph/varieties` |
| GBIF (CC0/CC-BY filtered) | CC0/CC-BY | `/api/graph/pollinators` |
| ERA5-Land (Copernicus CDS) | Copernicus free | `/api/graph/climate-reference` |

## Deployment

```bash
# Build backend
docker build --network=host --no-cache \
  -t ghcr.io/nkz-os/bioorchestrator-backend:0.1.0 \
  -f backend/Dockerfile .

# Build Neo4j with n10s plugin
docker build --network=host --no-cache \
  -t ghcr.io/nkz-os/bioorchestrator-neo4j:5.26 \
  -f backend/neo4j/Dockerfile backend/neo4j/

# Build frontend IIFE
npm install && npx vite build
mc cp dist/nkz-module.js minio/frontend-static/modules/bioorchestrator/nekazari-module.js

# Apply K8s
kubectl apply -f k8s/neo4j-statefulset.yaml -n nekazari
kubectl apply -f k8s/deployment.yaml -n nekazari
kubectl apply -f k8s/service.yaml -n nekazari

# Seed knowledge graph
kubectl exec -n nekazari deploy/bioorchestrator-backend -- \
  python scripts/seed_phenology.py --neo4j-uri bolt://bioorchestrator-neo4j:7687
```

See `docs/deployment.md` for full runbook.

## Related modules

- [IkerKeta](https://github.com/nkz-os/ikerketa) — ETL pipeline (25 connectors)
- [Crop Health Engine](https://github.com/nkz-os/nekazari-module-crop-health) — Real-time biophysical inference
- [Nekazari Platform](https://github.com/nkz-os/nkz) — Main platform

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nkz-os/nkz/blob/main/LICENSE).
