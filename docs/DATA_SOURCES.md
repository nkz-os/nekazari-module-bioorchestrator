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

### Active sources (7 climates covered)

Trial counts below are **exact**, from the production graph on 2026-07-18
(`MATCH (vt:VarietyTrial) RETURN vt.source_id, count(*)`).

| # | Source (`source_id`) | Country | Crops | Trials |
|---|--------|---------|-------|-------:|
| 1 | **BSL Bundessortenamt** (`BSL`) | Germany | Maize, wheat, barley, rapeseed, rye, oats, triticale, potato | 18,422 |
| 2 | **GENVCE** (`GENVCE`) | Spain | Wheat, barley, oats, triticale, rye, rapeseed, sunflower, legumes | 7,044 |
| 3 | **Navarra Agraria / INTIA** (`NAVARRA-AGRARIA`) | Spain | Wheat, barley, oats, triticale, pea, faba bean | 4,834 |
| 4 | **NÉBIH / VSZT** (`NEBIH`) | Hungary | Winter wheat, maize, rapeseed | 838 |
| 5 | **EU trial reports** (`EU-TRIAL-REPORTS`) | EU | Cereals | 724 |
| 6 | **LfL Bayern** (`LFL-BAYERN`) | Germany | Winter/spring wheat, barley, oats, rye, triticale, rapeseed, maize, potato | 694 |
| 7 | **CREA** (`CREA`) | Italy | Durum wheat, barley | 659 |
| 8 | **Legacy load** (`LEGACY`) | — | Mixed, pre-dates per-source traceability | 576 |
| 9 | **ITACyL** (`ITACYL`) | Spain | Wheat, barley, rapeseed | 523 |
| 10 | **INIAV / CerealTech** (`INIAV-LVR`) | Portugal | Soft wheat, durum wheat, barley | 306 |
| 11 | **AHDB** (`AHDB`) | UK | Wheat, barley, oats, rapeseed | 247 |
| 12 | **CTIFL** (`CTIFL`) | France | Strawberry, tomato, peach, apricot, cherry, apple, pear | 133 |
| 13 | **INTIA experimental** (`INTIA-EXP`) | Spain | Cover crops, legumes | 118 |
| 14 | **IFAPA** (`IFAPA` + `IFAPA_ALMOND`) | Spain | Wheat, barley, sunflower, legumes, almond | 135 |
| 15 | **TAGEM** (`TAGEM_TR_2012`) | Turkey | Cereals | 57 |
| 16 | **INRA Maroc** (`INRAMAROC`) | Morocco | Cereals | 42 |
| 17 | **EVENA** (`EVENA`) | Spain | Cereals | 5 |
| 18 | **FAO EcoCrop** | International | 2,500+ species (environmental envelopes) | reference only |

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
| **CPVO Varieties** ⚠️ **NOT OPERATIONAL** | cpvo.europa.eu (API) | Intended: EU registered plant varieties. **The endpoint returns 404** (verified 2026-07-19) and there are **0 `AgriCropVariety` nodes** in the graph. The connector also extracts only name, species, registration year and maintainer — **no growing cycle, no maturity class**: CPVO is a plant-variety-rights registry, not an agronomic one. | — |
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
| `GET /varieties` | Variety list for a species | `AgriCropVariety` — **returns empty**: the label has 0 nodes (see CPVO above) |
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
| NGSI-LD refs | **`ref<Type>` — DEPRECATED** | `refTrialSite`, `refArticleSource` still exist in the JSON-LD ingest bundles, but the platform standard for **new** relationships is the FIWARE Smart Data Model naming (`hasAgriParcel`, `hasAgriCrop`, `hasDevice`) or a descriptive `verbNoun` (`locatedAt`, `belongsTo`). Do not add new `ref<Type>` attributes. |
