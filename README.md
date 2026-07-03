<p align="center">
  <img src="https://img.shields.io/badge/license-AGPL--3.0-blue?style=flat-square" alt="License: AGPL-3.0">
  <img src="https://img.shields.io/badge/python-3.12+-3670A0?style=flat-square&logo=python&logoColor=white" alt="Python 3.12+">
  <img src="https://img.shields.io/badge/typescript-5.x-3178C6?style=flat-square&logo=typescript&logoColor=white" alt="TypeScript 5.x">
  <img src="https://img.shields.io/badge/fastapi-0.115+-009688?style=flat-square&logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/neo4j-5.26-4581C3?style=flat-square&logo=neo4j&logoColor=white" alt="Neo4j 5.26">
  <img src="https://img.shields.io/badge/module%20federation-2.0-646CFF?style=flat-square&logo=webpack&logoColor=white" alt="Module Federation 2.0">
</p>

<h1 align="center">🧬 BioOrchestrator</h1>
<p align="center"><strong>Multi-domain biodiversity knowledge graph · Crop agronomy engine · Regenerative planning</strong></p>
<p align="center"><em>A <a href="https://nkz-os.org">Nekazari Platform</a> module — powered by 29 scientific data connectors</em></p>

---

## What is BioOrchestrator?

BioOrchestrator is the **agronomic intelligence layer** of the Nekazari precision agriculture platform. It combines a Neo4j knowledge graph with 29 federated scientific data sources to deliver:

- **Crop knowledge** — 34k+ variety trials, phenology parameters, thermal limits, NPK profiles, soil requirements, rotation constraints
- **Parcel intelligence** — crop comparator, rotation planner, multi-segment crop plan, regenerative sequence designer, water budget, alerts
- **Closed-loop advisories** — when Crop-Health writes a new phenological stage to the broker, BioOrchestrator evaluates agronomist-editable action rules and emits a `CropAdvisory` (e.g. *terminate the cover crop at flowering with a roller crimper*) — the crop-health ⇄ bioorch feedback loop (Contract 2)
- **Reference data** — crop catalog, climate explorer, phenology browser, organic inputs, DAD-IS livestock breeds

All parameters carry **full scientific provenance** (`sourceType`: global_standard / peer_reviewed_study / extension_advisory, plus DOI, institution, method) so every recommendation is auditable. Trial yields distinguish **measured vs derived** (`yield_provenance`) so approximations are never presented as measurements.

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
                    │         Nekazari Host (React)            │
                    │   Module Federation Runtime v2.0         │
                    └──────────────┬───────────────────────────┘
                                   │ loads remote
                    ┌──────────────▼───────────────────────────┐
                    │         BioOrchestrator Frontend         │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ 18 tabs · 29 source badges · 6 i18n│ │
                    │  │ Parcel: Variety · Compare · Rotate  │ │
                    │  │  Health · Water · Regen · Alerts    │ │
                    │  │ Ref: Catalog · Climate · Phenology  │ │
                    │  │  Thermal · NPK · Soil · Rotation    │ │
                    │  │  Organic · Pipeline · DAD-IS · Src  │ │
                    │  └─────────────────────────────────────┘ │
                    └──────────────┬───────────────────────────┘
                                   │ HTTP (cookie auth)
                    ┌──────────────▼───────────────────────────┐
                    │       BioOrchestrator Backend            │
                    │           FastAPI :8420                  │
                    │  ┌──────────────────────────────────────┐│
                    │  │ /api/graph/*       — 49 endpoints    ││
                    │  │ /api/crop/catalog  — 7 endpoints     ││
                    │  │ /api/bioorchestrator — sources, pipe ││
                    │  │ /api/dadis/*       — 4 endpoints     ││
                    │  │ /api/parcel/*      — soil+vegetation ││
                    │  └──────────────────────────────────────┘│
                    └──────┬──────────────┬────────────────────┘
                           │ bolt://      │ HTTP
              ┌────────────▼───┐  ┌───────▼──────────────────┐
              │   Neo4j 5.26   │  │  29 External Data Sources │
              │  + APOC plugin │  │  ┌───────────────────────┐│
              │                │  │  │EPPO · EcoCrop · FiBL  ││
              │ AgriCrop       │  │  │CPVO · GBIF · SoilGrids││
              │ TrialSite      │  │  │ERA5 · Copernicus DEM  ││
              │ VarietyTrial   │  │  │Natura2000 · EU Pestic ││
              │ PhenologyParams│  │  │AgriKnowledge · IPCC   ││
              │ CropHeatTol.   │  │  │Feedipedia · INTIA     ││
              │ CropNutrientP. │  │  │JRC MARS · FAO GAEZ    ││
              │ RotationConstr.│  │  │DAD-IS · Redis alerts  ││
              │ SoilSuitability│  │  │…and 10 more           ││
              │ AgriKnowledge  │  │  └───────────────────────┘│
              └────────────────┘  └──────────────────────────┘
```

---

## Features

### 🌾 Parcel Tools

| Tool | Description |
|------|-------------|
| **Variety Finder** | Rank varieties by climate, soil, yield — extrapolated from Neo4j trial data |
| **Crop Comparator** | Side-by-side agronomic, environmental, and economic comparison with forage value and market maturity |
| **Rotation Planner** | Multi-year rotation with N balance, C fixation, pest risk, **PAC compliance score** (eco-schemes) |
| **Parcel Health** | Real-time CWSI/MDS/water balance from Crop-Health module with historical chart |
| **Water Budget** | ETc × Kc irrigation demand using real ET0 from timeseries-reader |
| **Regenerative Sequence** | Cover crop → protein crop design with N fixation, water balance, **carbon projection** (SOC, CO₂e, €) |
| **Crop Plan & Advisories** | Multi-segment rotation (cover → main → catch) committed as an `AgriCrop` set; **`CropAdvisory`** recommendations auto-triggered when Crop-Health reports a phenology change (Contract 2 action-rule loop) |
| **Alerts** | Redis-sourced parcel alerts with severity filter, **🐝 eco-warnings** (GBIF pollinators + pesticide safety) |

### 📚 Reference Data

| Tool | Description |
|------|-------------|
| **Crop Catalog** | Browse 18+ EPPO crops with variety counts, market maturity badges, data completeness indicators |
| **Climate Explorer** | Köppen climate zones with trial site counts, rainfall, frost days |
| **Phenology Browser** | GDD-based stage lookup with Kc/D1/D2/MDS parameters, DOI provenance, cascade matching |
| **Thermal Tolerance** | Heat/frost damage thresholds per species (EcoCrop-derived or published) |
| **NPK Profiles** | N/P/K uptake curves per phenological stage from peer-reviewed literature |
| **Soil Requirements** | pH, texture, drainage, depth, salinity per crop (EcoCrop GAEZ v4) |
| **Rotation Constraints** | Crop rotation rules with scientific sources |
| **Organic Inputs** | Authorized FiBL products per crop pest (cross-referenced with EPPO taxonomy) |
| **Pipeline Runner** | Trigger IkerKeta ETL ingestion per source |
| **DAD-IS Explorer** | FAO livestock breed discovery (per-user API credentials) |
| **Data Sources** | 29-connector dashboard with availability status |

### 🧪 Dynamic Crop Reference

Crop N fixation and growing season are resolved from **multiple live sources** (never hardcoded):

1. **AgriKnowledge** (Neo4j) — real measurements from INTIA/IFAPA/ITACyL
2. **EPPO Taxonomy API** — auto-detect Fabaceae/Poaceae family N fixation
3. **EcoCrop CSV** (FAO GAEZ) — growing season days per crop
4. **IPCC 2019 Tier 1** — static fallback for 18 crops

### 🌍 Sustainability & Compliance

BioOrchestrator enriches agronomic decisions with real-time environmental intelligence:

| Feature | How it works |
|---------|-------------|
| 🐝 **Biodiversity Shield** | When Crop-Health detects flowering (via GDD phenology), BioOrchestrator cross-references GBIF pollinator presence and EU Pesticides bee toxicity — emitting eco-warnings with safer alternatives and recommended nocturnal application windows |
| 🇪🇺 **PAC Compliance** | Evaluates rotation plans against CAP eco-scheme rules: winter cover on slopes >10% (Copernicus DEM), Natura 2000 buffer zones, crop diversity minimums, winter soil cover, pesticide limits — produces a 0-100% compliance score |
| 🌱 **Carbon Projection** | Calculates SOC increase from cover crop biomass using IPCC 2019 Tier 1 humification coefficients, projects years to reach optimal SOC by soil texture, and estimates fertilizer savings (€/ha) from biological N fixation |

### 🔄 Closed-Loop Advisories (Contract 2)

BioOrchestrator plans and recommends; Crop-Health observes real state. The two close a loop through the Orion-LD broker — no polling, no HTTP push:

```
Crop-Health writes CropHealthAssessment.phenologyStage=flowering  (broker, per-tenant)
        │  NGSI-LD subscription (watchedAttributes: phenologyStage)
        ▼
POST /api/graph/internal/phenology-update  →  evaluate agronomist-editable ActionRules (Neo4j)
        ▼
CropAdvisory upserted to the broker  →  GET /api/graph/agriculture/advisories
```

BioOrchestrator **reads** the state Crop-Health computed — it never recomputes phenology. A matched rule (e.g. *cover crop reached flowering + roller-crimper termination*) produces a `CropAdvisory` (a recommendation in the Plan layer), never an `AgriParcelOperation` (the actual event, owned by Field-Operations). The subscription is registered lazily, per-tenant, when a crop is assigned.

### 🌍 Multi-language

Full i18n support in **6 languages**: English, Spanish, Basque, French, Portuguese, Catalan.

---

## API Reference

### Agriculture (`/api/graph/agriculture/*`)

| Endpoint | Description |
|----------|-------------|
| `GET /crops` | List available EPPO crops |
| `GET /trial-sites` | List trial sites with climate metadata |
| `GET /variety-trials` | Raw variety trial results with env filters |
| `GET /similar-sites` | Find sites similar to a target environment |
| `GET /extrapolate` | Rank varieties for a crop+climate+soil combination — each variety carries `yield_provenance` (measured/partial/derived) and optional weather-adjusted penalties |
| `GET /crop-name` | Localized crop name from EPPO code (used by Field-Operations/ROPO) |
| `GET /compare-crops` | Multi-crop comparison (agronomic, environmental, economic) |
| `GET /rotation-plan` | Multi-year rotation + PAC compliance evaluation (eco-schemes) |
| `GET /regenerative-sequence` | Cover crop → protein crop + carbon projection (SOC, CO₂e) |
| `GET /crop-context` | Full crop context for a parcel (phenology, thermal, soil) |
| `GET /yield-potential` | Expected yield with confidence interval from trials |
| `GET /water-budget` | ETc-based irrigation demand per parcel |
| `POST /assign-crop` | Assign variety + management — single source of truth for `AgriParcel.hasAgriCrop`; registers the per-tenant phenology subscription |
| `POST /crop-plan` · `GET /crop-plan` | Multi-segment crop plan (cover/main/catch) as a dated `AgriCrop` set |
| `POST /crop-plan/{parcel}/segments/{seq}/advance` | Mark a segment sown — planned → active → harvested transition |
| `GET /advisories` | `CropAdvisory` recommendations for a parcel (output of the Contract-2 action-rule loop) |
| `GET /alerts` | Redis-sourced alerts + eco-impact enrichment (pollinators, pesticides) |
| `GET /organic-inputs` | FiBL authorized inputs per crop pest |

### Graph (`/api/graph/*`)

| Endpoint | Description |
|----------|-------------|
| `GET /species` | List species in graph |
| `GET /phenology-params` | Kc/D1/D2/MDS with GDD stage matching + `sourceType` provenance tier |
| `GET /phenology-stages` | Phenological stages per species |
| `POST /phenology-params/contribute` | Submit parameter for review |
| `GET · POST · PUT /action-rules` | Agronomist-editable rules evaluated by the Contract-2 loop |
| `GET /heat-tolerance` | Thermal damage thresholds |
| `GET /nutrient-profile` | NPK uptake per stage |
| `GET /soil-suitability` | Soil requirements per species |
| `GET /rotation-constraints` | Crop rotation rules |
| `GET /recommendations/next-crop` | Crop suggestion after rotation |
| `GET /recommendations/fertilizer` | NPK fertilizer needs |
| `GET /recommendations/simulate` | A/B crop scenario comparison |
| `GET /soil-data` | SoilGrids 2.0 + LUCAS 2018 proxy |
| `GET /protected-area-check` | Natura 2000 proximity |
| `GET /varieties` | CPVO registered varieties |
| `GET /pollinators` | GBIF pollinator species |
| `GET /terrain` | Copernicus DEM elevation |
| `GET /climate-reference` | ERA5-Land climate normals |

### Catalog (`/api/crop/catalog/*`)

| Endpoint | Description |
|----------|-------------|
| `GET /` | List crops with variety counts, data flags |
| `GET /{id}` | Full crop detail with phenology, thermal, NPK, rotation |
| `POST /ingest` | Trigger ingestion from source |
| `POST /contribute` | Submit agronomic parameter with DOI |
| `GET /thermal-summary` | Species with/without thermal data |
| `GET /npk-summary` | Species with/without NPK data |
| `POST /derive-thermal` | Auto-derive thermal thresholds from EcoCrop |

---

## Data Sources (29 connectors)

| Source | Type | Used In |
|--------|------|---------|
| **EPPO Global Database** | Taxonomy + pests | CropReference, PestRisk, OrganicInputs |
| **EcoCrop (FAO GAEZ)** | Crop requirements | Thermal, Soil, GrowingSeason |
| **CPVO Variety Finder** | Registered varieties | Comparator, Catalog, MarketMaturity |
| **AgriKnowledge (Neo4j)** | Research measurements | N fixation (INTIA/IFAPA/ITACyL) |
| **Feedipedia** | Livestock feed values | Comparator (forage value) |
| **IPCC 2019 Tier 1** | Emission factors | CropReference fallback |
| **FiBL Organic Inputs** | Authorized products | OrganicInputs (pest cross-ref) |
| **SoilGrids 2.0** | Soil properties | Parcel soil data |
| **LUCAS 2018** | Topsoil data | Soil module enrichment |
| **Copernicus DEM** | Elevation | Terrain, climate resolution |
| **ERA5-Land** | Climate reanalysis | Climate reference, ET0 |
| **Sentinel-2 L2A** | Vegetation indices | NDVI for ParcelHealth |
| **Natura 2000** | Protected areas | Land-use constraints |
| **EU Pesticides DB** | Authorized substances | Crop protection |
| **GBIF** | Species occurrences | Pollinators |
| **FAO AGRIS** | Agricultural literature | Provenance enrichment |
| **AGROVOC** | Thesaurus | Entity linking |
| **JRC MARS Bulletins** | Crop monitoring | Cover crop reference |
| **INTIA Navarra** | Field trials | Cover crop + N fixation |
| **EU Trial Reports** | Variety trials (LfL, COBORU, SLU, Arvalis, CSIC, IFAPA, BRESOV…) | Extrapolate, Comparator (real measured yields) |
| **Legumes Translated** | H2020 practice notes | Cover crop management |
| **DAD-IS** | Livestock breeds | Breed discovery |
| **GlobalTreeSearch** | Tree species | Forestry domain |
| **Redis Streams** | Real-time events | Parcel alerts |
| **Timeseries Reader** | Time-series data | ET0, historical weather |
| **Soil Module** | Parcel soil data | AWC, horizons, organic matter |
| **Crop-Health** | Biophysical inference | CWSI, MDS, yield gap |
| **IkerKeta** | ETL pipeline | All initial graph seeding |
| **Neo4j (internal)** | Knowledge graph | Variety trials, parameters |
| **Orion-LD (internal)** | NGSI-LD context broker | Entity state |

---

## Quick Start

### Prerequisites

- Neo4j 5.26+ with APOC plugin
- Python 3.12+
- Node.js 20+
- pnpm

### Backend

```bash
cd backend
pip install -r requirements.txt

# Set environment
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=yourpassword
export EPPO_API_KEY=your_eppo_key     # optional — enables pest risk + taxonomy
export KEYCLOAK_URL=https://auth.example.com/auth
export TIMESERIES_READER_URL=http://timeseries-reader:5000
export REDIS_URL=redis://redis:6379   # optional — enables alerts

uvicorn app.main:app --host 0.0.0.0 --port 8420 --reload
```

### Frontend (Module Federation)

```bash
pnpm install
pnpm dev        # dev server
pnpm build      # production build → dist/
```

### Docker

```bash
# Backend
docker build -t ghcr.io/nkz-os/bioorchestrator-backend:0.1.0 -f backend/Dockerfile .

# Neo4j with APOC
docker build -t ghcr.io/nkz-os/bioorchestrator-neo4j:5.26 -f backend/neo4j/Dockerfile backend/neo4j/
```

### Kubernetes

```bash
kubectl apply -f k8s/neo4j-statefulset.yaml -n nekazari
kubectl apply -f k8s/deployment.yaml -n nekazari
kubectl apply -f k8s/service.yaml -n nekazari
```

### Seed the Graph

```bash
kubectl exec -n nekazari deploy/bioorchestrator-backend -- \
  python scripts/seed_phenology.py --neo4j-uri bolt://bioorchestrator-neo4j:7687
```

### Ingest variety trials

Each source has a `BaseIngester` subclass under `app/ingestion/` that maps its
JSON-LD `@graph` to canonical nodes (dry-run by default):

```bash
python -m app.ingestion.eu_trials_ingester \
  --jsonld ../nkz-cpvo-scraper/data/jsonld/eu_trials_v1.jsonld --apply
```

The scraper emits one `TrialSite` per distinct `trial_location` (name matched
exactly) so trials link via `TRIAL_AT` and become visible to `extrapolate`.

---

## Health Checks

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | Liveness — returns 200 if process is alive |
| `GET /readyz` | Readiness — checks Neo4j + IkerKeta connectivity |

Both are exempt from K8s probe rate limiting (`@limiter.exempt`).

---

## Related Modules

| Module | Description |
|--------|-------------|
| [Nekazari Platform](https://github.com/nkz-os/nkz) | Main monorepo — host shell, API gateway, Keycloak |
| [IkerKeta](https://github.com/nkz-os/ikerketa) | ETL pipeline — 29 connectors feeding the knowledge graph |
| [Crop Health Engine](https://github.com/nkz-os/nekazari-module-crop-health) | Real-time CWSI/MDS inference — consumes BioOrchestrator phenology |
| [Soil Module](https://github.com/nkz-os/nekazari-module-soil) | Soil horizons, AWC — consumed by water budget & regenerative sequence |
| [Vegetation Health](https://github.com/nkz-os/nekazari-module-vegetation-health) | Sentinel-2 NDVI — parcel vegetation indices |
| [Timeseries Reader](https://github.com/nkz-os/nekazari-module-timeseries-reader) | ET0 + weather time-series — consumed by water budget |
| [GitOps Config](https://github.com/nkz-os/gitops-config) | Production K8s manifests, ArgoCD apps |

---

## License

**AGPL-3.0-or-later** — see [LICENSE](https://github.com/nkz-os/nkz/blob/main/LICENSE).

Built with ❤️ for the [Nekazari](https://nkz-os.org) open-source precision agriculture platform.
