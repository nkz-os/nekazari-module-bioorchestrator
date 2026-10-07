# KG build environment

Builds the knowledge graph from the registries, the source contracts and the raw-data repository into a
scratch Neo4j, then verifies it and writes a deterministic export. It never touches the graph the backend serves.

## Usage

    export NKZ_DATA_SOURCES_DIR=<raw-data repository checkout>
    export NEO4J_PASSWORD=<password of the scratch instance>
    NEO4J_PASSWORD=$NEO4J_PASSWORD docker compose -f kg-build/docker-compose.yml up -d

    # dry run (default): adapt, map, gate, count diffs. No write of any kind.
    python -m app.kg build --sources GENVCE,CREA --profile production

    # build: needs --execute and a scratch label; first run needs an empty target
    python -m app.kg build --target bolt://localhost:7687 --target-label local-1 --execute

    # re-run on the same graph (idempotent: same export_hash)
    python -m app.kg build --target bolt://localhost:7687 --target-label local-1 --execute --allow-existing

`--chelsa-online` fetches missing CHELSA cells (needs network); the default uses the cache file
(`kg-build/cache/chelsa-cells.json`) and leaves a field site without climate as a reported gap.
Credentials: `NEO4J_USER` (default `neo4j`), `NEO4J_PASSWORD`. Manifests: `kg-build/out/<build-id>/` (gitignored).

## Safety rules (a write needs all of them)

- `--execute`; the default is a dry run.
- `--target-label` of the form `local|test|ci|scratch|build[-suffix]`. Production-like labels are rejected.
- Target host is loopback or listed in `NKZ_KG_ALLOWED_TARGET_HOSTS` (comma separated, empty by default).
- Target is not the `NEO4J_URI` the environment's backend is configured for.
- Target empty (or `--allow-existing`). A graph holding `VarietyTrial` nodes without `unitKey` (the legacy
  graph) is refused even with `--allow-existing`, unless it carries the scratch marker below (restored copy:
  `mark-target`, restore, `migrate-restored`, `replace-sources`, `build`; see `docs/KG_PIPELINE.md` section 2.1).
- Environment marker: a build on an empty target writes one `(:KgBuildTarget {label, created_by_build})`
  node. A NON-EMPTY target is written only if it carries that marker, `created_by_build` is true, and its
  label is a scratch label equal to `--target-label`. No marker (any production graph, whatever its schema),
  another label, or more than one marker: refused, even with `--allow-existing`. The marker is not exported,
  so a graph restored from an export has none and cannot be re-built into. The legacy-signature rule above
  stays as a second line of defence.
- The quality gate passed for every source (`production` profile also needs `publishable`). A refused gate writes nothing, not even the schema.
- Clean git worktrees (module and raw-data repository) unless `--allow-dirty`; the manifest records both SHAs,
  the registries hash, the requirements hash and `NKZ_KG_NEO4J_IMAGE_DIGEST`.

Exit codes: 0 ok / dry run, 1 gate refused or a stage failed, 2 safety refusal or usage.

## Equivalence against a deployed instance (opt-in)

`tests/kg/test_equivalence_prod.py` rebuilds the graph from the real bundles in a scratch container, runs this
repository's DAO on it for a fixed set of synthetic parcels (`tests/kg/equivalence_harness.py`), reads the same
calls from a deployed instance through its public GET API, and requires every difference of a recommendation to
have a cause the rebuild is meant to produce. It needs Docker, `NKZ_DATA_SOURCES_DIR` and `NKZ_KG_PROD_API_BASE`
(no default), and runs only with `-m prod_readonly`. The collector refuses any Cypher that writes.
