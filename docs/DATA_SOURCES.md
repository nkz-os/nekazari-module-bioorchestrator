# BioOrchestrator — Data Sources & Knowledge Graph Reference

> **Last updated:** 2026-06-17  
> **Neo4j VarietyTrials:** 33,378 | **ManagementTrials:** 181 | **TrialSites:** 206 | **Species:** 27  
> **Unique varieties:** 5,854 | **Climate coverage:** BSk, Csa, Cfb, BSh, Dfb, Dfa  
> **Data sources:** 12 | **Relationships:** 43,318
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
| `VarietyTrial` | 9,738 | Crop variety trial observation (yield, quality, disease scores) |
| `TrialSite` | 107 | Geolocated experimental station with climate + soil metadata |
| `ArticleSource` | 405 | Bibliographic reference (journal issue, report, PDF) |
| `ManagementTrial` | 224 | Agronomic management experiment (fertilizer, irrigation, tillage) |
| `HarvestData` | 3 | Regional harvest summary statistics |
| `Species` | ~200 | Plant species node (EPPO code, AGROVOC URI) |
| `PhenologyStage` / `PhenologyParams` | ~400 | Kc coefficients, GDD thresholds, rooting depth per stage |
| `CropHeatTolerance` | ~150 | Tmin, Tmax, Topt, lethal thresholds per species |
| `CropNutrientProfile` | ~200 | NPK uptake per phenological stage |
| `CropSoilSuitability` | ~150 | pH range, textures, drainage, depth requirements |
| `RotationConstraint` | ~50 | Pairwise crop rotation compatibility rules |
| `AgriCrop` / `AgriCropVariety` | ~500 | Orion-LD backed crop catalog (CPVO + EcoCrop) |

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

### Active sources (7 climates covered)

| # | Source | Country | Crops | Period | Trials (agg.) | Sites | Köppen |
|---|--------|---------|-------|--------|--------|-------|--------|
| 1 | **BSL Bundessortenamt** | Germany | Maize, wheat, barley, rapeseed, rye, oats, triticale, potato | 2019–2026 | ~15,500 | 6+ (multiple per climate) | Cfb, Dfb |
| 2 | **GENVCE** | Spain | Wheat, barley, oats, triticale, rye, rapeseed, sunflower, legumes | 2005–2025 | ~2,711 | 12 | Csa, BSk, Cfb, BSh |
| 3 | **Navarra Agraria** (INTIA) | Spain | Wheat, barley, oats, triticale, pea, faba bean | 2004–2024 | ~97 | 40 | BSk, Cfb |
| 4 | **NÉBIH / VSZT** | Hungary | Winter wheat, maize, rapeseed | 2014–2025 | ~838 | 33 | Dfa, Dfb, Cfb |
| 5 | **INIAV / CerealTech** | Portugal | Soft wheat, durum wheat, barley | 2019–2025 | ~545 | 4 | Csb, Csa |
| 6 | **ITACyL** | Spain | Wheat, barley, rapeseed | 2020–2025 | ~525 | 15 | Cfb, BSkg |
| 7 | **LfL Bayern** | Germany | Winter/spring wheat, barley, oats, rye, triticale, rapeseed, maize, potato | 2025 | ~517 | 11 | Dfb, Cfb |
| 8 | **CREA** | Italy | Durum wheat, barley | 2020–2025 | ~320 | 8 | Csa, Cfa |
| 9 | **AHDB** | UK | Wheat, barley, oats, rapeseed | 2020–2025 | ~350 | 15 | Cfb |
| 10 | **CTIFL** | France | Strawberry, tomato, peach, apricot, cherry, apple, pear | 2019–2025 | ~455 | 12 | Csa, Cfb |
| 11 | **IFAPA** | Spain | Wheat, barley, sunflower, legumes | 2020–2025 | ~58 | 5 | Csa |
| 12 | **FAO EcoCrop** | International | 2,500+ species (environmental envelopes) | Static | — | — | All |

### Pipeline per source

```
PDF/HTML download → PyMuPDF text extraction → LLM structured extraction
→ Pydantic validation → JSON-LD (NGSI-LD refTrialSite/refArticleSource)
→ navarra_ingester.py → Neo4j MERGE
```

### Repositories

| Scraper | Repo path | Status |
|---------|-----------|--------|
| Navarra | `nkz-navarra-agraria/` | Production |
| GENVCE | `nkz-genvce-scraper/` | Production |
| CTIFL | `nkz-ctifl-scraper/` | Production (Magento scanner added) |
| LfL | `nkz-lfl-scraper/` | Production (HTML scraper added) |
| NÉBIH | `nkz-nebih-scraper/` | Production (new) |
| INIAV | `nkz-iniav-scraper/` | Production (OCR required for CerealTech PDFs) |

---

## 3. IkerKeta Ontology Connectors (n10s RDF → Neo4j)

Ikerketa is the agronomic ETL pipeline that enriches the knowledge graph with
standardized ontologies and external databases. Its 26 connectors feed directly
into Neo4j via the `init_graph.py` n10s ingestion pipeline.

### Taxonomic & Phytosanitary

| Connector | Source | What it provides | Entity types |
|-----------|--------|------------------|-------------|
| **EPPO** | gd.eppo.int (REST API v2) | Plant pest/disease taxonomy, host ranges, geo-distribution | `Pest`, `Pathogen`, `Host` |
| **AGROVOC** | agrovoc.fao.org (SPARQL) | Multilingual agricultural thesaurus — 40K+ concepts, semantic relations | `Concept`, `SemanticRelation` |
| **CABI CPC** | cabi.org (fixtures) | Crop protection compendium — pest distribution maps | `Pest`, `Distribution` |
| **USDA PLANTS** | plants.usda.gov (CSV) | North American plant checklist — taxonomy, native status | `Plant`, `Taxon` |
| **USPEST** | uspest.org | Degree-day phenology models for pest forecasting | `PhenologyModel`, `GDDParams` |

### Crop Science & Agronomy

| Connector | Source | What it provides | Entity types |
|-----------|--------|------------------|-------------|
| **EcoCrop** | FAO GAEZ (CSV) | Crop environmental requirements — 2,500+ species, climate + soil envelopes | `Crop`, `EdaphicProfile`, `ClimaticProfile` |
| **CPVO Varieties** | cpvo.europa.eu (API) | EU registered plant varieties — 45K+ varieties across 300+ species | `AgriCropVariety`, `AgriCrop` |
| **FiBL** | fibl.org (CSV) | Organic input products list — fertilizers, biopesticides | `OrganicInput` |
| **DG SANTE** | ec.europa.eu (API) | EU Pesticides Database — approved/withdrawn active substances | `Pesticide`, `ApprovalStatus` |
| **EU Pesticides** | ec.europa.eu | Pesticide MRLs, approval status, withdrawal dates | `Pesticide`, `Regulation` |

### Livestock & Forages

| Connector | Source | What it provides | Entity types |
|-----------|--------|------------------|-------------|
| **DAD-IS** | fao.org/dad-is (API) | Domestic Animal Diversity — 15K+ breeds across 37 species | `Breed`, `LivestockSpecies` |
| **Feedipedia** | feedipedia.org (API) | Animal feed resources — nutritional composition for 1,400+ feeds | `FeedResource`, `NutrientProfile` |
| **Forages** | tropicalforages.info (CSV) | CIAT/ILRI forage species — tropical legume & grass cultivars | `Forage`, `Cultivar` |
| **WAHIS** | wahis.woah.org (API) | World Animal Health — disease outbreaks, control measures | `AnimalDisease`, `Outbreak` |

### Forestry & Agroforestry

| Connector | Source | What it provides | Entity types |
|-----------|--------|------------------|-------------|
| **Agroforestree** | ICRAF (API) | Agroforestry tree species — 600+ species, uses, management | `TreeSpecies`, `EcosystemService` |
| **GlobAllomeTree** | FAO/CIRAD (API) | Allometric equations — biomass/carbon estimation for 5,000+ trees | `AllometricEquation` |
| **GlobalTreeSearch** | BGCI (CSV) | Complete global tree checklist — 60K+ species | `TreeSpecies`, `ConservationStatus` |
| **EUFORGEN** | euforgen.org | European forest genetic resources — distribution maps, gene conservation units | `ForestSpecies`, `GeneticUnit` |
| **IUCN Trees** | iucnredlist.org (API) | Tree species conservation status — threat categories, population trends | `TreeSpecies`, `ConservationStatus` |

### Geospatial & Climate

| Connector | Source | What it provides | Entity types |
|-----------|--------|------------------|-------------|
| **SoilGrids 2.0** | ISRIC (WCS) | Global soil properties — 250m resolution, 7 depths, 18 properties | `SoilProperty`, `SoilProfile` |
| **ERA5 Climate** | Copernicus CDS (API) | Hourly climate reanalysis — temperature, precipitation, radiation, wind | `ClimateData`, `TimeSeries` |
| **Copernicus DEM** | Copernicus (COG) | GLO-30 Digital Elevation Model — 30m resolution Europe | `Elevation`, `Terrain` |
| **Natura 2000** | EEA (API) | European protected areas — 27K+ sites, habitat types, species | `ProtectedArea`, `Habitat` |

### Biodiversity & Pollinators

| Connector | Source | What it provides | Entity types |
|-----------|--------|------------------|-------------|
| **GBIF Pollinators** | gbif.org (API) | Global pollinator occurrence records — bees, butterflies, hoverflies | `Pollinator`, `Occurrence` |
| **Companion Planting** | GitHub (CSV) | Curated companion planting datasets — beneficial crop associations | `CompanionRelation`, `Crop` |

---

## 4. API Endpoints (BioOrchestrator Backend)

### Graph API (`/api/bioorchestrator/graph/*`)

| Endpoint | Description | Data source |
|----------|-------------|-------------|
| `GET /health` | Neo4j connectivity check | Neo4j |
| `GET /stats` | Node/relationship counts | Neo4j |
| `GET /species` | Species catalog with variety counts | AgriCrop, VarietyTrial |
| `GET /phenology-params` | Kc, GDD, rooting depth per species/stage | PhenologyParams, EcoCrop |
| `POST /phenology-params/contribute` | Community contribution of phenology data | User → Neo4j |
| `GET /heat-tolerance` | Tmin, Tmax, Topt per species | CropHeatTolerance |
| `GET /nutrient-profile` | NPK uptake per stage | CropNutrientProfile |
| `GET /soil-suitability` | pH, texture, drainage, depth reqs | CropSoilSuitability |
| `GET /recommendations/next-crop` | Rotation successor recommendation | RotationConstraint |
| `GET /recommendations/simulate` | What-if crop scenario simulation | RotationConstraint, SoilSuitability, NutrientProfile |
| `GET /varieties` | Variety list for a species | AgriCropVariety (CPVO) |
| `GET /pesticides` | Approved/withdrawn pesticides for crop | DG SANTE, EU Pesticides |
| `GET /pollinators` | Pollinator presence near coordinates | GBIF Pollinators |
| `GET /protected-area-check` | Natura 2000 overlap check | Natura 2000 |
| `GET /soil-data` | SoilGrids properties at coordinates | SoilGrids 2.0 |
| `GET /terrain` | Elevation/slope at coordinates | Copernicus DEM |
| `GET /climate-reference` | Climate normals at coordinates | ERA5 Climate |

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
| NGSI-LD refs | **ref<Type>** | `refTrialSite`, `refArticleSource` |
