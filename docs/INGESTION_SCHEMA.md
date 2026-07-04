# BioOrchestrator — Ingestion Schema

Canonical contract for **scraper output** (`nkz-*-scraper`) and **ingester input**
(`nkz-module-bioorchestrator/backend/app/ingestion/`).

Scrapers emit JSON-LD. Ingesters subclass `BaseIngester`, call `normalize_nodes()`, then
`MERGE` into Neo4j. Scrapers must **not** translate traits, normalize locations, or invent
yields — the ingester pipeline does that.

**Code references:** `base_ingester.py`, template `crea_ingester.py`, `navarra_ingester.py`,
`sources_registry.json`, `validate_source.py`.

---

## 1. Pipeline

```
Official source (PDF/Excel/HTML)
        ↓
Scraper → data/*.jsonld  (@graph, snake_case fields)
        ↓
*_ingester.py (BaseIngester subclass)
        ↓
normalize_nodes()  →  MERGE Neo4j (VarietyTrial, TrialSite, …)
```

Register every new source in `backend/data/sources_registry.json`. `SOURCE_ID` in the
ingester must match `source_id` on JSON-LD nodes.

---

## 2. JSON-LD envelope

```json
{
  "@context": "https://nkz.robotika.cloud/ngsi-ld/bioorchestrator-context.jsonld",
  "@graph": []
}
```

| `@type` | Required | Role |
|---------|----------|------|
| `VarietyTrial` | ≥1 per file (typical) | Cultivar × site × year × measured traits/yield |
| `TrialSite` | **Strongly recommended** | Geo/climate anchor; without it, `TRIAL_AT` may not link → trials invisible to `extrapolate` |
| `ArticleSource` | Recommended | Provenance / citation |
| `ManagementTrial` | Optional | Treatment experiments (fertilization, density, phenology GDD, …) |
| `HarvestData` | Optional | Regional or model harvest summaries (not variety trials) |

Use **snake_case** in scraper JSON-LD (`crop_eppo`, `yield_kg_ha`, `trial_location`).
Ingesters map to camelCase Neo4j properties (`cropEppo`, `yieldKgHa`, `trialLocation`).

---

## 3. Node schemas

### 3.1 `ArticleSource`

```json
{
  "@id": "urn:nkz:source:MY_SOURCE",
  "@type": "ArticleSource",
  "source": "Human-readable name",
  "source_id": "MY_SOURCE",
  "issue_number": 150,
  "article_title": "Article title",
  "article_author": "Author",
  "year": 2020
}
```

| Field | Required | Notes |
|-------|----------|-------|
| `source_id` | yes | Must equal ingester `SOURCE_ID` and `sources_registry.json` entry |
| `article_title` | no | Improves `SOURCED_FROM` traceability |

### 3.2 `TrialSite`

```json
{
  "@id": "urn:nkz:site:dicastillo",
  "@type": "TrialSite",
  "name": "Dicastillo",
  "municipality": "Dicastillo",
  "region": "Navarra",
  "climate_class": "BSk",
  "latitude": 42.5,
  "longitude": -1.6,
  "elevation_m": 400
}
```

| Field | Required | Notes |
|-------|----------|-------|
| `name` | yes | Must match `trial_location` on linked `VarietyTrial` nodes (case-insensitive match in ingester) |
| `climate_class` | no | Köppen: `Csa`, `Cfb`, `BSk`, `Dfb`, `BSh`, … |
| `latitude` / `longitude` | no | Decimal degrees |

Location aliases (e.g. two spellings for the same site) belong in
`normalization_registry.LOCATION_NORMALIZATION`, not in divergent scraper strings.

### 3.3 `VarietyTrial`

```json
{
  "@id": "urn:nkz:trial:example:2021:prndu:guara",
  "@type": "VarietyTrial",
  "source_id": "MY_SOURCE",
  "variety": "Guara",
  "crop_eppo": "eppo:PRNDU",
  "crop_scientific": "Prunus dulcis",
  "year": 2021,
  "trial_location": "Dicastillo",
  "yield_kg_ha": 1327.36,
  "irrigation_regime": "secano",
  "production_system": "conventional",
  "quality_params": { "pelonas_pct": 4.0 },
  "disease_scores": {},
  "confidence": "high",
  "refTrialSite": { "@id": "urn:nkz:site:dicastillo" },
  "refArticleSource": { "@id": "urn:nkz:source:MY_SOURCE" }
}
```

| JSON-LD field | Neo4j property | Required | Notes |
|---------------|----------------|----------|-------|
| `crop_eppo` | `cropEppo` | **yes** | `eppo:TRZAX` or bare `TRZAX`; missing → trial rejected |
| `variety` | `variety` | **yes** | |
| `year` | `year` | **yes** | Integer > 1900 |
| `trial_location` | `trialLocation` | **yes** | Must align with a `TrialSite.name` |
| `yield_kg_ha` | `yieldKgHa` | no | **Measured only** — never infer from charts or relative notes |
| `yield_note_s1` | `yieldNoteS1` | no | Scalar BSL 1–9 only; **never a JSON object** |
| `yield_relative_pct` | `yieldRelativePct` | no | |
| `irrigation_regime` | `irrigationRegime` | no | `secano`, `regadío`, or AGROVOC URI |
| `production_system` | `productionSystem` | no | e.g. `organic`, `conventional` — not `management` |
| `quality_params` | `qualityParams` | no | Object; use for non-yield metrics (Brix, oil %, cumulative totals labelled honestly) |
| `agronomic_traits` | `agronomicTraits` | no | Keep source-language keys; registry translates |
| `disease_scores` | `diseaseScores` | no | |
| `confidence` | `confidence` | no | `high` \| `medium` \| `low` |

**Yield honesty (platform rule):** Do not fabricate `yield_kg_ha` from relative scores, chart
estimates without flagging `confidence: medium`, or multi-year cumulative totals. Put
cumulative or non-annual values in `quality_params` with explicit keys (e.g.
`cumulative_oil_kg_ha_1998_2009`). Never use `yield_note_s1 * 1000` as a substitute for
missing kg/ha.

**Review gate (recommended for new extractions):** emit `skip_ingestion: true` and
`review_status: pending` until a human approves; remove before production merge.

### 3.4 Perennial extension (planned — optional fields)

The graph model extends `VarietyTrial` for woody/perennial crops **without a new label**.
These fields are optional (null for annual crops). Ingester support lands with
`AlmondIfapaIngester` and related PRs — emit them in JSON-LD now if the source provides
the data.

| JSON-LD field | Neo4j property | Notes |
|---------------|----------------|-------|
| `rootstock` | `rootstock` | e.g. `Garnem`, `GF-677`; null = own-rooted |
| `scion` | `scion` | When distinct from `variety` |
| `training_system` | `trainingSystem` | `hedgerow`, `shd`, `vaso`, `espaldera`, … |
| `planting_year` | `plantingYear` | Orchard establishment year |
| `planting_density_trees_ha` | `plantingDensityTreesHa` | |
| `yield_metric` | (metadata) | Disambiguate `kernel_kg_ha`, `oil_kg_ha`, `olive_fruit_kg_ha` before mapping to `yieldKgHa` |

Catalog node `Rootstock` + edge `USES_ROOTSTOCK` are ingester-managed when `rootstock` is set.

### 3.5 `ManagementTrial`

```json
{
  "@type": "ManagementTrial",
  "source_id": "MY_SOURCE",
  "crop_eppo": "eppo:TRZAX",
  "experiment_type": "FertilizationTrial",
  "treatment": "N 0 kg/ha",
  "result_metric": "yield_kg_ha",
  "result_value": 2800.0,
  "result_unit": "kg.ha-1",
  "year": 2001,
  "trial_location": "Navarra"
}
```

Standard `experiment_type` values: `FertilizationTrial`, `PlantingDensityTrial`,
`PestControlTrial`, `OrganicTrial`, `OtherTrial`. Add new types in the ingester, not in
scrapers.

---

## 4. Idempotency (`mergeKey`)

Each node needs a stable `mergeKey` for `MERGE`. Recommended pattern for trials:

```
{source_id}|eppo:{EPPO}|{variety}|{location}|{irrigation}|{year}
```

`BaseIngester` also computes `mergeKeyNormalized` and `mergeKey|content_hash` for
deduplication. Do not emit `mergeKeyNormalized` from scrapers.

---

## 5. Ingester checklist

1. Subclass `BaseIngester`; set `SOURCE_ID`.
2. Implement `_parse_nodes()` → `{trial_sites, article_sources, variety_trials, management_trials}`.
3. Register source in `sources_registry.json`.
4. Add traits/locations/EPPO to `normalization_registry.py` if needed.
5. Validate:  
   `cd backend && python -m app.ingestion.validate_source MY_SOURCE path/to/trials.jsonld`
6. Dry-run ingest:  
   `python -m app.ingestion.my_source_ingester --jsonld path/to/trials.jsonld`

Template: `app/ingestion/crea_ingester.py`.

---

## 6. Scraper anti-patterns

| Do not | Do instead |
|--------|------------|
| Translate trait keys to English | Keep source language; extend `TRAIT_REGISTRY` |
| Normalize 1–9 scales to % | Leave raw; `normalize_nodes()` handles |
| Omit `TrialSite` nodes | Emit one site per distinct `trial_location` |
| Put dicts in `yield_note_s1` | Use `quality_params` for structured extras |
| Coerce cumulative yields to annual `yield_kg_ha` | Label in `quality_params` |
| Hardcode secrets or prod URLs in JSON-LD | Use env/config in scrapers only |

---

## 7. Registered sources (ingesters)

| `SOURCE_ID` | Ingester module |
|-------------|-----------------|
| `BSL` | `bsl_ingester.py` |
| `GENVCE` | `genvce_ingester.py` |
| `NAVARRA-AGRARIA` | `navarra_ingester.py` |
| `INIAV` | `iniav_ingester.py` |
| `NEBIH` | `nebih_ingester.py` |
| `CTIFL` | `ctifl_ingester.py` |
| `IFAPA` | `ifapa_ingester.py` (legumes; `IFAPA_ALMOND` perennial pilot pending) |
| `ITACYL` | `itacyl_ingester.py` |
| `LFL` | `lfl_ingester.py` |
| `AHDB` | `ahdb_ingester.py` |
| `CREA` | `crea_ingester.py` |
| `INTIA-EXP` | `intia_exp_ingester.py` |
| `EU-TRIAL-REPORTS` | `eu_trials_ingester.py` |

Navarra JSON-LD converter reference: `nkz-navarra-agraria/src/navarra_agraria/jsonld_converter.py`.

---

## 8. Validation and tests

```bash
cd nkz-module-bioorchestrator/backend
PYTHONPATH=. python -m app.ingestion.validate_source NAVARRA-AGRARIA /path/to/trials.jsonld
PYTHONPATH=. python -m pytest tests/test_base_ingester.py tests/test_navarra_ingester_structure.py -q
```

Entity-manager / platform tests: see root `AGENTS.md` §2 (run affected suites before claiming green).
