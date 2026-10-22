# BioOrchestrator — Knowledge-graph build pipeline

Reproducible, gated build of the trial knowledge graph from registries, per-source contracts and a raw-data
repository. It replaces the per-source `BaseIngester` scripts described in
[INGESTION_SCHEMA.md](./INGESTION_SCHEMA.md) (legacy, kept until the readers move to Observations).
The build never touches the graph the backend serves: it writes only to a scratch Neo4j and ends with a
deterministic export. Code: `backend/app/kg/`. Local environment: [`backend/kg-build/README.md`](../backend/kg-build/README.md).

## 1. Pipeline

```
raw extractions (pinned commit of the raw-data repository)
   -> adapter     (app/kg/adapters/<source>.py: raw files -> raw rows, every decision reported)
   -> contract    (data/sources/<SOURCE>.yaml: raw rows -> canonical rows; closed schema, unknown key = error)
   -> gate        (app/kg/gate.py: licence, identity, vocabularies, ranges, duplicates; errors stop the build)
   -> loader      (app/kg/loader.py: batched MERGE by natural key, Observation is the truth)
   -> link        (app/kg/link.py: every unit hangs on one site/variety/crop/study/document)
   -> enrich      (app/kg/enrich.py: CHELSA climate for FIELD sites only; aggregates are never enriched)
   -> verify      (app/kg/verify.py: counts, duplicates, orphans, yield metric, licences)
   -> export      (app/kg/export.py: deterministic archive + export_hash)
```

Inputs are data, not code: `backend/data/registries/` (crops, varieties, sites, variables, units,
vocabularies, ranges, sources, GENVCE zone definitions, irrigation thresholds) and `backend/data/sources/`
(one contract per source). The registries hash, both git SHAs, the dependency-lock hash and the Neo4j image
digest are recorded in the build manifest, so two builds from the same inputs give the same `export_hash`.

Model in one paragraph: a `Study` groups `ObservationUnit`s (crop x variety x observed site x season x
irrigation x production system, plus factors); each unit has `Observation`s (variable, value, unit,
qualifier). Yield is an observation like any trait; the unit keeps a derived copy of the variables flagged
`denormalize` in `variables.yaml`, under the property names the current queries read. Nothing is inferred:
a metric, basis, moisture or purpose the source does not state is a contract `default` that must quote the
source (or say `unknown`), otherwise the row is a reported gap.

## 2. CLI and safety model

```
export NKZ_DATA_SOURCES_DIR=<raw-data checkout>
export NEO4J_PASSWORD=<scratch instance password>

python -m app.kg build --sources GENVCE,CREA --profile production        # dry run (default)
python -m app.kg build --target bolt://localhost:7687 --target-label local-1 --execute
python -m app.kg build ... --execute --allow-existing                     # idempotent re-run
python -m app.kg export --target bolt://localhost:7687 --out build.tar    # read-only
```

Run from `backend/`. Exit codes: 0 ok / dry run, 1 gate refused or a stage failed, 2 safety refusal or usage.
Each stage writes a JSON manifest under `backend/kg-build/out/<build-id>/` (gitignored).

The default is a **dry run**: adapt, map, gate and count, with no write of any kind. A write needs all of:

| Rule | Why |
|---|---|
| `--execute` | writing is opt-in |
| `--target-label` of the form `local\|test\|ci\|scratch\|build[-suffix]` | naming a target is a deliberate act; production-like labels are rejected |
| target host is loopback or listed in `NKZ_KG_ALLOWED_TARGET_HOSTS` (empty by default) | no host is allowed implicitly |
| target is not the `NEO4J_URI` of the environment's backend | the served graph is untouchable |
| target empty, or `--allow-existing`; a graph with `VarietyTrial` nodes lacking `unitKey` (the legacy schema) is refused even with `--allow-existing`, unless it carries the scratch marker below | the legacy schema is recognised as the served graph; only a marked restored copy may hold it |
| environment marker | a build on an empty target writes one `(:KgBuildTarget {label, created_by_build})`; a non-empty target is written only if it carries that marker with the same scratch label. No marker, another label or several markers: refused. The marker is excluded from the export, so a graph restored from an export cannot be built into |
| gate passed for every source (`production` profile also needs `publishable`) | a refused gate writes nothing, not even the schema |
| clean git worktrees (module and raw-data repository) unless `--allow-dirty` | reproducibility; the dirty state is recorded |

### 2.1 Building on a restored copy of the served graph

To replace only some sources and keep every other (legacy sources, reference knowledge), build on a **restored
copy** of the served graph, never on the served graph. The order is fixed, and each step refuses what the
previous one did not leave:

```
python -m app.kg mark-target      --target <bolt URI> --target-label build-copy --execute   # 1 empty target -> marker
python backend/scripts/neo4j_restore_from_export.py --archive <backup>.tar --uri <bolt URI> \
       --confirm-empty-target --allow-build-marker                                      # 2 restore the copy
python -m app.kg migrate-restored --target <bolt URI> --target-label build-copy [--execute]   # 3 schema
python -m app.kg replace-sources  --sources GENVCE,CREA --target <bolt URI> --target-label build-copy [--execute]  # 4
python -m app.kg build --sources GENVCE,CREA --target <bolt URI> --target-label build-copy --execute --allow-existing  # 5
python -m app.kg export --target <bolt URI> --out build.tar                              # 6 marker is left out
```

1. `mark-target` writes the marker only on a completely empty target (no node, constraint or index); it is
   idempotent for the same label and refuses anything else. The marker is what later allows legacy trials in the
   target, so it cannot be added after the data is there. The restore script refuses a marked target unless
   `--allow-build-marker` is given (build copies only; the production restore of step 6 must not use it, and the
   script prints the marker count and fails if one is present without the flag).
2. The restore needs an empty database; run it against the marked target.
3. `migrate-restored` applies the migrations **schema only** (constraints and indexes; the data statements are
   skipped, so the legacy trials are not rewritten). A pre-flight reads every UNIQUE constraint the migrations
   would create and the copy does not have, and fails, writing nothing, if its keys are duplicated (it prints
   up to ten offending keys and the number of groups) or if a plain index on the same properties blocks it
   (nothing is dropped). The default is that pre-flight alone.
4. `replace-sources` removes the named sources and nothing else: their trials (legacy `VarietyTrial` and F1
   `ObservationUnit`, matched by `source_id` / `dataSource` spellings) with their `Observation` nodes, and the
   studies, article sources and sites that are left with no trial of any other source. Sites tagged with the source
   (`source_id` / `sourceIds`) are removed only when no trial points at them; `--extra-site-keys` names more
   empty sites. The default is a dry run printing counts per source, label and crop and the explicit list of sites
   to delete and to keep. Batches of at most 500 trials per transaction; a second run deletes nothing; label
   counts outside the removal set are checked after the run.
5. `build` as usual; the schema must be exactly the current one (step 3 leaves it so).
6. Restore the exported archive into the instance that will serve (never adopt the build instance, which holds
   the marker), and check `MATCH (m:KgBuildTarget) RETURN count(m)` is 0 there. A graph without the marker is
   refused by `replace-sources`, `migrate-restored` and `build`.

`replace-sources`, `migrate-restored` and `mark-target` follow the same rules as `build`: scratch label, loopback
or allow-listed host, not the backend's own `NEO4J_URI`, `--execute`.

Credentials come from `NEO4J_USER` / `NEO4J_PASSWORD` only. `--chelsa-online` fetches missing CHELSA cells;
by default only the local cache is used and a field site without climate is a reported gap.

Related scripts (`backend/scripts/`): `kg_calibrate_irrigation.py` (see section 4.4),
`kg_sizing_benchmark.py` (store size and transaction memory at projected volume).

## 3. Adding a source

1. **Licence first.** Add the source to `registries/sources.yaml`: holder, licence id, the literal quote, check
   date, `commercial_use` (`allowed` / `permission_granted` / `denied` / unknown), and, where the licence
   requires it, the literal `attribution_text`, `attribution_url`, download date and conditions. A source
   whose licence is not `allowed` or `permission_granted` is a gate error under the `production` profile
   (a warning and `not_publishable` under `local-test`). Attribution text is shown to users with every
   recommendation that uses the source; copy it verbatim.
2. **Raw layer.** Extractions live in the raw-data repository, never in this one. The contract pins the
   commit it was written against.
3. **Adapter** (`app/kg/adapters/<source>.py`, exposing `load(path)`): raw files to raw rows, plus a warning
   for every decision (rows excluded, labels preferred, regimes left out). Do not map values onto places
   or regimes the document does not print. Register it in `SOURCES` in `app/kg/cli.py`.
4. **Contract** (`data/sources/<SOURCE>.yaml`): `source_id`, `raw`, `adapter`, `document`, `study`, `unit`
   (observed fields as printed, factors, purpose, yield with unit/metric/basis/moisture), `observations`,
   `ignore` (every unmapped raw key needs a reason), `sites`, and `expected`. The schema is closed; see the
   docstring of `app/kg/contracts.py` for every key.
5. **Registries.** Add what the gate will otherwise flag: crops (EPPO code + aliases), site rows (an
   observed zone or stratum is an `aggregate` site with country and no coordinates; a field site needs
   coordinates and a cited source), variables/units/vocabularies for new raw keys, varieties (`candidate`
   until reviewed; no fuzzy auto-merge), and `reviewed` ranges with a cited source (only `reviewed` ranges
   error; `assumption` ranges warn).
6. **Expected counts.** Fill `expected: {units, observations, sites}` from a dry run you have inspected;
   the engine fails if the build differs, so a changed extraction cannot slip in.
7. **Tests.** Adapter test on a small fixture, a contract test on the canonical output, and a loader/verify
   run on the scratch Neo4j (`backend/tests/kg/`, Docker needed). Run `pytest tests/kg` with
   `NKZ_DATA_SOURCES_DIR` set so the real-data tests are not skipped, and `ruff check app/ tests/ scripts/`.
8. **Dry run, then build** into a scratch target and read the gate report and the verify output before anyone
   considers the export.

## 4. Evidence semantics (read before changing a reader or a source)

### 4.1 Field vs regional tier

A source that prints the mean of a group of trials (a climatic zone, a yield stratum, a geographic group,
the whole network) is not a field trial. Its observed label becomes an **aggregate** site (`siteKind`
aggregate): never given coordinates, a climate class or a CHELSA cell, and never mapped onto a reference
city. Its yields are the **regional tier**: reported with the zone label, trust capped at `low`, no
variety ranking from a single regional mean, data gaps `regional_evidence_only` and `no_measured_yield`
where no per-plot yield exists. Field trials at a located site are the **field tier** and are matched by
climate class. The two never share a number: `verify` reports them apart.

### 4.2 Zone matching (GENVCE)

GENVCE defines its zones per campaign by thresholds on April mean temperature and annual rainfall (class
limits vary by report). `registries/genvce_zone_definitions.yaml` holds one definition per report with its
citation. At build time a unit gets a `zoneKey` only if its document family and campaign have a published
definition, its crop is covered, and its zone label states the classes the definition needs. At read time a
Spanish parcel is classified with the same definition from its CHELSA 1981-2010 climatology, giving zones
that are the parcel's (allow), decidably another (deny) or undecidable (neither, e.g. exactly on a limit).

**Caveat, always exposed in the response:** GENVCE classifies each trial by the weather of its campaign;
the parcel is classified by climatology, so the zone is an approximation of the one GENVCE would assign.
The opaque zone id exchanged with clients carries threshold classes only, never a climate value or
coordinates. Units without a zone key (unpublished definition, organic reports whose labels are not
verified, labels that state no class) stay at the country level.

### 4.3 Country-level fallback

When the regional evidence is not narrowed to a zone it is matched by the parcel's country and labelled:
data gap `regional_country_level`. It is a pooled answer for the whole country and must not be read as
local. A site with a country is never matched by climate; a site without one is matched by climate.

### 4.4 Irrigation regime

- A regime **stated by the source** (a table label such as "Secanos" or "Regadios", or a document-level
  statement) always wins and is the only way a unit gets a regime.
- A trial whose source states the **opposite** regime of the request is excluded from the answer; it is
  never pooled with the requested regime.
- A trial whose source states **no regime** is kept, labelled: data gap `irrigation_regime_unknown` and the
  count in `evidence.irrigation_unknown_trials`. If *every* listed trial is unknown the gap is
  `irrigation_regime_unknown_all`, trust is `low` in every tier, and the yield is still returned.
- **No regime is derived.** Extractions often carry regimes that were assumptions of the extraction step; the
  adapters drop them (counted). A yield-threshold derivation (`app/kg/irrigation_cutoff.py`,
  `registries/irrigation_thresholds.yaml`) exists but is inert: validation showed that a per-crop cutoff
  does not generalise across years, so every crop is `not_calibrated`, and groups defined by yield or
  mixed regimes never get a regime. `kg_calibrate_irrigation.py --write-registry` refuses cutoffs that
  fail the leave-one-year-out guard.

### 4.4b Production system (organic vs conventional)

Organic and conventional units are never pooled, in any tier or endpoint. The request carries the system
as `management` (`organic` | `conventional` | `any`, default `any`).

- `any`, `conventional` or no value: every unit whose production system is organic is **excluded**; a unit
  that states no system stays (it is not organic). A recommendation that leaves organic units out carries the
  data gap `organic_units_excluded`.
- `organic`: **only** organic units are read. A unit that states no system is not organic. No yield factor
  stands in for missing organic data; a crop with no organic units has no recommendation.

### 4.5 Productivity class and confidence

Tables split by yield stratum (high/medium/low productivity) store the stratum in its own non-key field
`productivityClass`; it is not an irrigation regime and does not enter the unit key. The loader writes
neither `confidence` nor `rankingEligible`: a model's self-assessment is not evidence. Trust is computed
at read time from the evidence policy (tier, number of trials, gaps), and clients must accept
`confidence = null`.

## 5. Known limits

- Readers still query the unit-level derived copy and the legacy JSON properties; moving them to
  Observations and removing the legacy properties is the next phase.
- `ManagementTrial` with factor levels, long-term plots, phenology and regional statistics are not part of
  this build; reference knowledge and other legacy sources are still loaded by the legacy scripts.
- The policy heuristics in `evidence_policy.py` (source-specific rules) are to be replaced by contract data.
- Ranges: only a few `assumption` ranges exist, so most values are not range-checked (the gate counts every
  skipped check). Reviewed, cited ranges are needed before adding sources.
- Variety aliases are data (`varieties.yaml`); typo variants stay separate varieties until reviewed.
