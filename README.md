<p align="center">
  <img src="https://img.shields.io/badge/license-AGPL--3.0-blue?style=flat-square" alt="License: AGPL-3.0">
  <img src="https://img.shields.io/badge/python-3.12+-3670A0?style=flat-square&logo=python&logoColor=white" alt="Python 3.12+">
  <img src="https://img.shields.io/badge/typescript-5.x-3178C6?style=flat-square&logo=typescript&logoColor=white" alt="TypeScript 5.x">
  <img src="https://img.shields.io/badge/fastapi-0.115+-009688?style=flat-square&logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/neo4j-5.26-4581C3?style=flat-square&logo=neo4j&logoColor=white" alt="Neo4j 5.26">
  <img src="https://img.shields.io/badge/module%20federation-2.0-646CFF?style=flat-square&logo=webpack&logoColor=white" alt="Module Federation 2.0">
</p>

<h1 align="center">🧬 BioOrchestrator</h1>
<p align="center"><strong>Crop planning · Agronomic knowledge graph · Campaign simulation</strong></p>
<p align="center"><em>A <a href="https://nkz-os.org">Nekazari Platform</a> module — powered by 29 scientific data connectors</em></p>

---

## What is BioOrchestrator?

BioOrchestrator has two layers:

- **Crop planning** (the module's page, *Planificación de cultivos* in Spanish): decide what to sow on each parcel, with which variety and when, campaign by campaign. Plans are written to the broker as planned `AgriCrop` cycles that every other module can read.
- **Agronomic knowledge service** (the backend): a Neo4j knowledge graph fed by scientific connectors, plus the crop engines. Other modules use it through the API: Crop Health for phenology parameters and the in-season yield forecast, Field Operations for crop names, Carbon and others for reference data.

What it provides:

- **Crop knowledge**: variety trials, phenology parameters, thermal limits, NPK profiles, soil requirements, rotation constraints
- **Planning**: crop fit per parcel, variety evidence, sowing calendar, cover crops, rotation checks, multi-year plans
- **Estimates**: expected yield per source (trial evidence, AquaCrop), never averaged, gated by validation
- **Campaign simulation**: AquaCrop runs with switchable engines, daily curves and full input provenance; the same engine serves the in-season forecast shown in Crop Health
- **Closed-loop advisories**: when Crop Health writes a new phenological stage, agronomist-editable rules emit a `CropAdvisory` (Contract 2)

All parameters carry **full scientific provenance** (`sourceType`: global_standard / peer_reviewed_study / extension_advisory, plus DOI, institution, method) so every recommendation is auditable. Trial yields distinguish **measured vs derived** (`yield_provenance`) so approximations are never presented as measurements.

---

## User guide: crop planning

The page lists every parcel of the farm against four campaigns: the three before the one being planned, and that one. From July on, the default campaign is next year's. The campaign selector also offers the previous and the next one.

Cell symbols:

| Symbol | Meaning |
|--------|---------|
| ○ planned | decided here, not sown yet |
| ◐ declared | someone stated it happened |
| ● actual | a completed field operation records it |

To plan a campaign:

1. Press **Choose** (or **Change**) in the parcel's cell for that campaign.
2. Answer **Rainfed or irrigated?**. Evidence and simulations never mix the two, so planning waits for it. Pick the management (conventional or organic).
3. Walk the wizard:
   - **crop**: each crop shows a verdict (fits, with caveats, does not fit) and its reasons;
   - **variety**: relative yield, stability and number of trials, only for crops with trials (optional);
   - **sowing fortnight**: the recommended window for the area is shaded; perennials ask for the number of years;
   - **cover crop**: optional, with its termination method;
   - **summary**: the expected harvest and the estimates table.
4. **Save plan** writes the planned cycles. The cell shows ○.

Sowing and harvest are recorded in **Field Operations** (*Record sowing*, *Record harvest*), with their real date. A recorded sowing turns the cell into ●, locks it ("changed from field operations") and enables two links: **Simulation of the current campaign** and **View forecast in Crop Health**.

The **estimates table** shows one row per source with its range, its basis and its validation label. Sources are never averaged; a warning appears when they disagree. A figure is shown only for validated crops and regions (today, rainfed wheat in Spain); other rows explain why there is no figure. The first AquaCrop run in a new area takes a few minutes while 15 years of weather are fetched.

The **campaign simulation** runs AquaCrop for a parcel and shows yield, potential, water gap, harvest date, daily canopy and water-stress curves, and every input with its source. It opens pre-filled from Crop Health (`crop_slug`, `sowing_date`, `irrigation` in the URL) and then joins the day's in-season run instead of computing it again.

In the 3D viewer, a selected parcel shows a **Parcel plan** panel with the previous and target campaigns and the same links.

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
                    │  │ Crop planning · 6 i18n              │ │
                    │  │ Farm matrix · Parcel wizard         │ │
                    │  │ Estimates · Campaign simulation     │ │
                    │  │ Viewer widget: parcel plan          │ │
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
              │ CropHeatTol.   │  │  │MAPA · FertiliCalc     ││
              │ CropNutrientP. │  │  │JRC MARS · FAO GAEZ    ││
              │ RotationConstr.│  │  │DAD-IS · Redis alerts  ││
              │ SoilSuitability│  │  │…and 10 more           ││
              │ AgriKnowledge  │  │  └───────────────────────┘│
              └────────────────┘  └──────────────────────────┘
```

---

## Knowledge service capabilities

The former multi-tab interface was replaced by crop planning. The capabilities below remain available through the API to other modules and to administrators.

### 🌾 Parcel Tools

| Tool | Description |
|------|-------------|
| **What to sow** | Ranked crop and variety recommendations by climate, soil and trial data, with side-by-side comparison |
| **Parcel Health** | Real-time CWSI/MDS/water balance from Crop-Health module with historical chart |
| **Water Budget** | ETc × Kc irrigation demand using real ET0 from timeseries-reader |
| **Regenerative Sequence** | Cover crop → protein crop design with N fixation, water balance, **carbon projection** (SOC, CO₂e, €) |
| **Crop Plan & Advisories** | Multi-segment rotation (cover → main → catch) committed as an `AgriCrop` set; **`CropAdvisory`** recommendations auto-triggered when Crop-Health reports a phenology change (Contract 2 action-rule loop) |
| **Alerts** | Redis-sourced parcel alerts with severity filter |

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

1. **Nutrient & Balance Reference** (`crop_balance_coefficients.yaml`) — open-licensed coefficients from MAPA, Panagos (JRC), ESSD 2024 and FertiliCalc (UCO/CSIC)
2. **EPPO Taxonomy API** — auto-detect Fabaceae/Poaceae family N fixation
3. **EcoCrop CSV** (FAO GAEZ) — growing season days per crop
4. **IPCC 2019 Tier 1** — static fallback for 18 crops

### 🧬 How Species & Varieties are Populated (The 6-Layer Architecture)

Adding a crop to BioOrchestrator is not a single database entry; it requires populating a multi-layered NGSI-LD knowledge graph to unlock the predictive engines across the platform. A fully unlocked crop must fulfill these 6 layers:

1. **Base Taxonomy (EPPO Code)**
   - **Where**: `species_registry.yaml` & `normalization_registry.py`
   - **Role**: The foundational key. If an EPPO code (e.g., `BRSOB` for broccoli) is not registered, the core API Gateway will reject the creation of `AgriCrop` entities.

2. **Phenological & Hydrological Models (FAO-56)**
   - **Where**: `phenology_sources.yaml`
   - **Role**: Defines Kc curves (crop coefficients) and growth stage durations. Without this, the `Hydrology` and `Vegetation-Health` modules cannot calculate crop evapotranspiration (ETc) to issue precision irrigation recommendations.

3. **Agronomic Trials (VarietyTrials & TrialSites)**
   - **Where**: Neo4j Graph (ingested via public scrapers)
   - **Role**: Powers the *Extrapolate* feature. This allows the AI to move beyond generic advice to precision recommendations (e.g., *"Under Csa climate and clay soil, variety X yields 15% more than variety Y"*).

4. **Edaphoclimatic Ranges (GAEZ / EcoCrop)**
   - **Where**: Neo4j (`CropHeatTol.`, `SoilSuitability`)
   - **Role**: Establishes physical survival limits (Base temperature for GDD, frost tolerance, optimal pH, salinity limits). It acts as the red light for the "What to sow" recommender to warn against planting in unsuitable environments.

5. **Pest & Disease Catalog (Host-Pathogen Map)**
   - **Where**: EPPO API / Neo4j
   - **Role**: Links EPPO pathogen codes (e.g., mildew) to the host species. This is mandatory for the `Crop-Health` module to trigger Disease Alerts when weather conditions favor an outbreak.

6. **Nutritional Extractions (N-P-K Curves)**
   - **Where**: Neo4j (`CropNutrientP.`)
   - **Role**: Defines how many kilos of Nitrogen, Phosphorus, and Potassium the plant consumes per ton of yield. The `Soil` module requires this to generate precise fertilization recipes and calculate Carbon Footprints.

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
CropAdvisory upserted to the broker
```

BioOrchestrator **reads** the state Crop-Health computed — it never recomputes phenology. A matched rule (e.g. *cover crop reached flowering + roller-crimper termination*) produces a `CropAdvisory` (a recommendation in the Plan layer), never an `AgriParcelOperation` (the actual event, owned by Field-Operations). The subscription is registered lazily, per-tenant, when a crop is assigned.

### 🌍 Multi-language

Full i18n support in **6 languages**: English, Spanish, Basque, French, Portuguese, Catalan.

---

## API Reference

### Planning (`/api/graph/planning/*`, contract 2.2)

| Endpoint | Description |
|----------|-------------|
| `GET /farm` | Parcels × campaigns matrix with cycles, provenance, regime and rotation warnings |
| `GET /parcels/{id}/fit` | Crop fit verdicts for a parcel (soil, pH, frost, water deficit, rotation) |
| `GET /parcels/{id}/varieties` | Variety evidence: relative yield, stability, trials and years |
| `GET /parcels/{id}/calendar` | Sowing fortnights, recommended window and a coherent cover crop |
| `GET /parcels/{id}/estimates` | Comparison of expected yield per source (trial evidence, AquaCrop) |
| `GET /parcels/{id}/estimates/aquacrop` | Asynchronous AquaCrop estimate (poll with `job_id`) |
| `GET /parcels/{id}/estimates/in-season` | In-season forecast of a sown parcel, read by Crop Health |
| `PUT /parcels/{id}/plan` | Write the planned cycles of a campaign |

### Agriculture (`/api/graph/agriculture/*`)

| Endpoint | Description |
|----------|-------------|
| `GET /crops` | List available EPPO crops |
| `GET /trial-sites` | List trial sites with climate metadata |
| `GET /variety-trials` | Raw variety trial results with env filters |
| `GET /crop-name` | Localized crop name from EPPO code (used by Field-Operations/ROPO) |
| `GET /crop-context` | Full crop context for a parcel (phenology, thermal, soil) |
| `POST /crop-plan` · `GET /crop-plan` | Multi-segment crop plan (cover/main/catch) as a dated `AgriCrop` set |
| `POST /crop-plan/{parcel}/segments/{seq}/advance` | Mark a segment sown — planned → active → harvested transition |
| `GET /organic-inputs` | FiBL authorized inputs per crop pest |
| `POST /crop-simulation/jobs` · `GET /crop-simulation/jobs/{id}` | Campaign simulation (AquaCrop), asynchronous; identical inputs share the day's job |

### Graph (`/api/graph/*`)

| Endpoint | Description |
|----------|-------------|
| `GET /species` | List species in graph |
| `GET /phenology-params` | Kc/D1/D2/MDS with GDD stage matching + `sourceType` provenance tier |
| `GET /phenology-stages` | Phenological stages per species |
| `POST /phenology-params/contribute` | Submit parameter for review (TechnicalConsultant, TenantAdmin or PlatformAdmin) |
| `GET · POST · PUT /action-rules` | Agronomist-editable rules evaluated by the Contract-2 loop (POST/PUT: PlatformAdmin) |
| `GET /heat-tolerance` | Thermal damage thresholds |
| `GET /nutrient-profile` | NPK uptake per stage |
| `GET /soil-suitability` | Soil requirements per species |
| `GET /rotation-constraints` | Crop rotation rules |
| `GET /recommendations/fertilizer` | NPK fertilizer needs |
| `GET /varieties` | Registered varieties in catalog |

### Catalog (`/api/crop/catalog/*`)

| Endpoint | Description |
|----------|-------------|
| `GET /` | List crops with variety counts, data flags |
| `GET /{id}` | Full crop detail with phenology, thermal, NPK, rotation |
| `POST /ingest` | Trigger ingestion from source (PlatformAdmin) |
| `POST /contribute` | Submit agronomic parameters (`kc`, `kcIni`, `kcMid`, `kcEnd`, `d1`, `d2`, `mdsRef`, `ky`) with DOI (TechnicalConsultant, TenantAdmin or PlatformAdmin). Stored as `pending_review` with the verified contributor; applied to the Orion catalog only when the caller is PlatformAdmin (approval is a PlatformAdmin action). 404 for an unknown crop |
| `GET /thermal-summary` | Species with/without thermal data |
| `GET /npk-summary` | Species with/without NPK data |
| `POST /derive-thermal` | Auto-derive thermal thresholds from EcoCrop (PlatformAdmin) |

---

## Data Sources (29 connectors)

| Source | Type | Used In |
|--------|------|---------|
| **EPPO Global Database** | Taxonomy + pests | CropReference, PestRisk, OrganicInputs |
| **EcoCrop (FAO GAEZ)** | Crop requirements | Thermal, Soil, GrowingSeason |
| **CPVO Variety Finder** | Registered varieties | Comparator, Catalog, MarketMaturity |
| **MAPA Guía Fertilización 2010** | Official fertilization guide | NPK balance, cover crop & legume N fixation |
| **FertiliCalc (UCO/CSIC)** | Agronomic reference model (GPL-3.0) | Residue C/N, nutrient balance defaults |
| **ESSD 2024 / Dryad CC0** | Global & EU nutrient dataset | Biological N fixation, dry matter & harvest index |
| **Panagos et al. 2022 (JRC)** | EU phosphorus removal dataset | Phosphorus balance |
| **Eurostat (`apro_cpshr`)** | NUTS 2 regional crop statistics | Regional yield baselines |
| **GENVCE / CREA** | Official variety trial networks | Variety comparison and yield evidence |
| **Redis Streams** | Real-time events | Parcel alerts |
| **Timeseries Reader** | Time-series data | ET0, historical weather |
| **Soil Module** | Parcel soil data | AWC, horizons, organic matter |
| **Crop-Health** | Biophysical inference | CWSI, MDS, yield gap |
| **IkerKeta** | ETL pipeline | All initial graph seeding |
| **Neo4j (internal)** | Knowledge graph | Variety trials, parameters |
| **Orion-LD (internal)** | NGSI-LD context broker | Entity state |
| **GGCMI Phase 3 crop calendar** v1.01 (Jägermeyr et al. 2021, [doi:10.5281/zenodo.5062513](https://doi.org/10.5281/zenodo.5062513), CC BY 4.0) | Typical sowing/maturity day per crop, 0.5° grid | What to sow: typical sowing calendar at the parcel (`backend/data/ggcmi_calendar_europe.json`, built by `backend/scripts/build_ggcmi_calendar.py`) |

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
export AGROCLIMATIC_VECTOR=v1  # optional — parcel similarity: v1 (default, Köppen) | v2 (CHELSA vector) | hybrid (Köppen, v2 if no result); v2/hybrid only apply when CHELSA_PARCEL_CLIMATE_ENABLED is on
export CHELSA_PARCEL_CLIMATE_ENABLED=1  # optional (default off) — parcel-environment climate from CHELSA v2.1 cells

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
kubectl apply -f k8s/neo4j-statefulset.yaml -n <namespace>
kubectl apply -f k8s/deployment.yaml -n <namespace>
kubectl apply -f k8s/service.yaml -n <namespace>
```

### Seed the Graph

```bash
kubectl exec -n <namespace> deploy/bioorchestrator-backend -- \
  python scripts/seed_phenology.py --neo4j-uri bolt://bioorchestrator-neo4j:7687
```

### Ingest variety trials

GENVCE and CREA are built with the gated pipeline (dry run by default, scratch target only):
see [docs/KG_PIPELINE.md](docs/KG_PIPELINE.md). The `BaseIngester` flow below is deprecated for them
and remains for the other sources.

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
| [Crop Health Engine](https://github.com/nkz-os/nekazari-module-crop-health) | Crop state, risks and the in-season yield forecast; consumes BioOrchestrator phenology and the in-season estimate |
| [Field Operations](https://github.com/nkz-os/nkz-module-field-operations) | Records sowings and harvests; turns planned cycles into actual ones |
| [Soil Module](https://github.com/nkz-os/nekazari-module-soil) | Soil horizons, AWC — consumed by water budget & regenerative sequence |
| [Vegetation Health](https://github.com/nkz-os/nekazari-module-vegetation-health) | Sentinel-2 NDVI — parcel vegetation indices |
| [Timeseries Reader](https://github.com/nkz-os/nekazari-module-timeseries-reader) | ET0 + weather time-series — consumed by water budget |
| [GitOps Config](https://github.com/nkz-os/gitops-config) | Production K8s manifests, ArgoCD apps |

---

## License

**AGPL-3.0-or-later** — see [LICENSE](https://github.com/nkz-os/nkz/blob/main/LICENSE).

Built with ❤️ for the [Nekazari](https://nkz-os.org) open-source precision agriculture platform.
