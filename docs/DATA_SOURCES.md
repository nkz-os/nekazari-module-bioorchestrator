# BioOrchestrator — Data Sources & Knowledge Graph Reference

> **Last updated:** 2026-07-18 (counts regenerated from the production graph)  
> **VarietyTrials:** 35,357 | **ManagementTrials:** 437 | **TrialSites:** 232 | **Species:** 45  
> **Unique varieties:** 5,409 | **Data sources:** 18 | **Relationships:** 50,610  
> **Climate coverage (sites):** Cfb 103, BSk 42, Csa 28, Dfb 23, Dfa 7, Cwb 3, BSh 2,
> Csb 1, Cfa 1, unclassified 22
>
> ⚠️ These counts age with every ingest. Regenerate them rather than trusting them:
> `MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) ORDER BY count(*) DESC`
>
> 🔄 **Data normalization**: All ingested data now passes through
> `normalize_nodes()` in `BaseIngester`. Traits are translated to AGROVOC,
> variety names are standardised to uppercase, locations are resolved to
> canonical names, and mergeKeys use a unified format.
> See `app/ingestion/normalization_registry.py` and `ESPECIFICACION_SCRAPER_INGESTA.md`.

---

## 1. Knowledge Graph Entity Types (Neo4j)

| Label | Count | Description |
|-------|-------|-------------|
| `VarietyTrial` | 35,357 | Crop variety trial observation (yield, quality, disease scores) |
| `CropNutrientProfile` | 577 | NPK uptake per phenological stage |
| `ArticleSource` | 560 | Bibliographic reference (journal issue, report, PDF) |
| `ManagementTrial` | 437 | Agronomic management experiment (fertilizer, irrigation, tillage) |
| `TrialSite` | 232 | Geolocated experimental station with climate + soil metadata |
| `PhenologyParams` | 197 | Kc, stage duration (d1/d2), reference MDS, with bibliographic provenance |
| `PhenologyStage` | 189 | Phenological stage per species |
| `RotationConstraint` | 48 | Pairwise crop rotation compatibility rules |
| `AgriCrop` | 45 | Crop catalog node (bridge to the NGSI-LD context broker) |
| `CropHeatTolerance` | 45 | Heat / frost damage thresholds per species |
| `CropSoilSuitability` | 45 | pH range, textures, drainage, depth requirements |
| `Species` | 45 | Plant species node (EPPO code, AGROVOC URI) |
| `HarvestData` | 3 | Regional harvest summary statistics |
| `Rootstock` | 3 | Rootstock (fruit trees) |

Platform metadata labels (`Capability` 28, `AgriKnowledge` 8, `Module` 2, `ActionRule` 2,
`Entitlement` 0) are **not agronomic** and can be ignored for data analysis.

### Known data-quality caveats

Documented so they are not mistaken for signal. Full detail in the external review
package README:

- Only **20,723 of 35,357** trials (58.6 %) carry `yieldKgHa`; **6,090** of those are
  *derived* from BSL relative notes via an empirical factor, not measured — treat as a
  separate population (`yieldDerivationMethod = bsl_note_empirical_factor`).
- `yieldMetric` (grain vs fruit vs dry matter) is set on **0.1 %** of nodes: yield scales
  are **not comparable across crops**. Never aggregate across species without segmenting.
- `irrigationRegime` is present on only **28.1 %**.
- **182 `mergeKey` values are shared** by distinct observations — there is **no uniqueness
  constraint** on `VarietyTrial.mergeKey`. 96 groups are EU-TRIAL-REPORTS site trials
  whose location never reached the key; 85 are LfL Bayern **aggregate columns**
  (site means / growing regions), not single-site trials.
- A `TrialSite` literally named `unknown`, with no climate or coordinates, is referenced
  by **84 trials**.
- Only **108 of 232** sites carry the full agro-climatic vector (rainfall, ET0, frost
  days, elevation) used by the site-similarity engine; the rest fall back to Köppen.

### Relationship types

| Relationship | From | To | Meaning |
|---|---|---|---|
| `TRIAL_AT` | VarietyTrial, ManagementTrial | TrialSite | Trial was conducted at this site |
| `SOURCED_FROM` | VarietyTrial, ManagementTrial, HarvestData | ArticleSource | Data originates from this publication |
| `TRIAL_OF` | VarietyTrial | Species | Trial evaluates varieties of this species |
| `HAS_STAGE` | Species | PhenologyStage | Species has this phenological stage |
| `HAS_PARAMETER` | PhenologyStage | PhenologyParams | Kc, GDD parameters for this stage |
| `HAS_HEAT_TOLERANCE` | Species | CropHeatTolerance | Thermal limits for this species |
| `HAS_NUTRIENT_PROFILE` | PhenologyStage | CropNutrientProfile | NPK needs during this stage |
| `HAS_SOIL_SUITABILITY` | Species | CropSoilSuitability | Edaphic preferences |
| `HAS_VARIETY` | AgriCrop | AgriCropVariety | Registered variety of this species |

---

## 2. Variety Trial Sources (Scraped PDF/HTML → JSON-LD → Neo4j)

### Active loadable sources (`commercial_use: allowed`)

Only sources verified with `commercial_use: allowed` in `backend/data/registries/sources.yaml` are loadable in production:

| # | Source (`source_id`) | Country | Scope |
|---|--------|---------|-------|
| 1 | **GENVCE** (`GENVCE` / `GENVCE-TRIALS`) | Spain | Arable crops (wheat, barley, oats, triticale, rye, rapeseed, sunflower, maize) |
| 2 | **CREA** (`CREA`) | Italy | Maize national variety trials (2021–2025) |
| 3 | **MAPA Fertilization Guide** (`mapa_guia_2010`) | Spain | NPK balance, legume N fixation |
| 4 | **Panagos et al. 2022 (JRC)** (`panagos_2022`) | EU | Phosphorus plant removal coefficients |
| 5 | **ESSD 2024 / Ludemann et al.** (`essd_2024_s3` / `ludemann_cc0`) | International | Crop nutrient concentrations, harvest indices, biological N fixation |
| 6 | **Eurostat (`apro_cpshr`)** (`eurostat_apro_cpshr`) | EU | NUTS 2 regional crop production and yield series |
| 7 | **FertiliCalc (UCO/CSIC)** (`fertilicalc_script_2024`) | Spain | Residue C/N and nutrient balance reference defaults |

### Pipeline per source

> GENVCE and CREA are built by the gated pipeline in [KG_PIPELINE.md](./KG_PIPELINE.md).

---

## 3. API Endpoints (BioOrchestrator Backend)

### Graph API (`/api/bioorchestrator/graph/*`)

| Endpoint | Description | Data source |
|----------|-------------|-------------|
| `GET /health` | Neo4j connectivity check | Neo4j |
| `GET /stats` | Node/relationship counts | Neo4j |
| `GET /species` | Species catalog with variety counts | AgriCrop, VarietyTrial |
| `GET /phenology-params` | Kc, GDD, rooting depth per species/stage | PhenologyParams |
| `POST /phenology-params/contribute` | Community contribution of phenology data | User → Neo4j |
| `GET /heat-tolerance` | Tmin, Tmax, Topt per species | CropHeatTolerance |
| `GET /nutrient-profile` | NPK uptake per stage | CropNutrientProfile |
| `GET /soil-suitability` | pH, texture, drainage, depth reqs | CropSoilSuitability |
| `GET /varieties` | Variety list for a species | Orion-LD `hasSubCrop` |

### Parcel API (`/api/bioorchestrator/parcel/*`)

| Endpoint | Description | Data source |
|----------|-------------|-------------|
| `GET /{parcel_id}/vegetation` | Vegetation index time series (NDVI, EVI, SAVI, etc.) | Vegetation-Health → TimescaleDB |
| `GET /{parcel_id}/soil` | Soil horizons and properties for parcel | Soil Module → Orion-LD |

### Catalog API (`/api/bioorchestrator/crop/*`)

| Endpoint | Description | Data source |
|----------|-------------|-------------|
| `GET /catalog` | Crop species/variety catalog with data availability flags | Neo4j + Orion-LD |
| `GET /catalog/{crop_id}` | Full detail: phenology, thermal, soil, NPK, varieties | Neo4j (AgriCrop graph) |
| `POST /catalog/ingest` | Trigger EcoCrop/CPVO ingestion | Ikerketa connectors |
| `POST /catalog/contribute` | Community parameter contribution | User → Neo4j |
| `GET /catalog/thermal-summary` | Heat tolerance coverage stats | CropHeatTolerance |
| `GET /catalog/npk-summary` | Nutrient profile coverage stats | CropNutrientProfile |

### DAD-IS API (`/api/bioorchestrator/dadis/*`)

| Endpoint | Description | Auth |
|----------|-------------|------|
| `GET /countries` | DAD-IS country list | Per-user API key |
| `GET /species` | Livestock species list | Per-user API key |
| `POST /breeds` | Breed search by classification, country, species | Per-user API key |
| `GET /breeds/{id}` | Detailed breed information | Per-user API key |

---

## 5. Crop-Health Integration Points

Crop-Health consumes BioOrchestrator data to calibrate its inference engines:

| Crop-Health Engine | BioOrchestrator Input | What it enables |
|--------------------|----------------------|-----------------|
| **CWSI** (water stress) | Kc per variety/stage from PhenologyParams | Crop-specific transpiration baseline |
| **MDS** (dendrometry) | MDS_ref per variety from PhenologyParams | Variety-specific growth benchmarks |
| **Water Balance** | Soil AWC from Soil module, ET0 from ERA5 | Field-capacity water budget |
| **Vigor** (satellite) | NDVI/SAVI from Vegetation-Health, GDD stage | Stage-contextual vigor interpretation |
| **Composite Stress** | Ky coefficients per stage (FAO-33) | Stage-weighted stress index |
| **Yield Gap** | Historical yields from VarietyTrials in similar climate | Actual vs potential yield for YOUR variety |
| **WUE** | Water applied vs biomass from Water Balance + Yield | kg produce per m³ water |



## 6. Climate Coverage Map

| Köppen | Zone Name | Trials | Sites | Key Regions |
|--------|-----------|--------|-------|-------------|
| **BSk** | Cold semi-arid | ~1,000 | 12 | Navarra, Castilla, Aragón |
| **Csa** | Hot-summer Mediterranean | ~4,000 | 14 | Andalusia, Catalonia, Provence, Alentejo, Algarve |
| **Cfb** | Oceanic | ~3,200 | 44 | Cornisa cantábrica, Galicia, Brittany, Bavaria, W Hungary |
| **BSh** | Hot semi-arid | ~220 | 1 | Córdoba interior |
| **Dfb** | Warm-summer continental | ~660 | 12 | Bavaria, N Hungary hills |
| **Dfa** | Hot-summer continental | ~400 | 6 | Great Hungarian Plain (Alföld) |
| **Csb** | Warm-summer Mediterranean | ~250 | 4 | Central/Northern Portugal, Galicia coast |

---

## 7. Pending Sources (Proposed)

| Source | Country | Climate | Status |
|--------|---------|---------|--------|
| **CREA** | Italy | Csa, BSk, Cfa | Proposed — fragmented sources, Italian language |
| **Arvalis** | France | Cfb, Csa | Proposed — major French cereal institute |
| **ITGC** | Algeria | BWh, BSk | Proposed — North African durum wheat trials |
| **LfL historical** | Germany | Dfb | Blocked — Wayback Machine has no snapshots; direct contact needed |

---

## 8. International Nomenclature Standards

All sources adhere to these standards:

| Domain | Standard | Example |
|--------|----------|---------|
| Climate | **Köppen-Geiger** | `Dfa`, `Csb`, `BSk` — never local names |
| Crop taxonomy | **EPPO codes** | `TRZAX` (wheat), `ZEAMX` (maize) |
| Soil classification | **WRB** | `Chernozem`, `Luvisol`, `Calcisol` |
| Units | **UCUM** | `kg/ha`, `t/ha`, `mm`, `%` |
| Variables | **ICASA** (AgMIP) | `YAMH` (yield), `IRRC` (irrigation) |
| Ontology | **AGROVOC** (FAO) | `c_7951` (winter wheat) |
| NGSI-LD refs | **`ref<Type>` — DEPRECATED** | `refTrialSite`, `refArticleSource` still exist in the JSON-LD ingest bundles, but the platform standard for **new** relationships is the FIWARE Smart Data Model naming (`hasAgriParcel`, `hasAgriCrop`, `hasDevice`) or a descriptive `verbNoun` (`locatedAt`, `belongsTo`). Do not add new `ref<Type>` attributes. |
