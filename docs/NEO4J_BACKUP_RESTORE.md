# Neo4j backup archive and restore

The knowledge graph can be backed up online (no downtime) as a logical export
and restored into an empty Neo4j 5.x database with
`backend/scripts/neo4j_restore_from_export.py`. The restore needs Bolt access
only: no APOC, no server-side import directory.

## Archive format (`nkz-neo4j-export`, version 1)

A plain `tar` named `neo4j-YYYYMMDD-HHMMSS.tar` with four members:

| Member | Content |
|---|---|
| `manifest.json` | Neo4j version, node/relationship totals, counts per label and per relationship type, schema object counts, sha256 of the data members, content fingerprint |
| `schema.json` | Constraints and indexes, each with its `createStatement` |
| `nodes.jsonl.gz` | One node per line: `{"id", "l": [labels], "p": {properties}}` |
| `rels.jsonl.gz` | One relationship per line: `{"id", "t": type, "s": start id, "e": end id, "p": {properties}}` |

Values JSON cannot carry are tagged objects `{"$t": ...}`: datetime (with
named zone when present), local datetime, date, time, local time, duration,
point (srid and coordinates), bytes and non-finite floats. Properties of any
other type make the export fail instead of producing a lossy archive.

The exporter reads nodes and relationships as a lazily consumed Bolt stream, so
memory stays bounded on the server and in the exporter. Do not replace it with
a single-result APOC streaming export (`apoc.export.json.all` with
`stream: true`): it builds the whole export in the server heap and can kill a
small-heap instance with an out-of-memory error.

## Restore

1. Start an **empty** Neo4j 5.x (5.23 or newer) and note its Bolt URI. Never
   restore into a database that already holds data: the script refuses unless
   it has no nodes, constraints or user indexes, and needs
   `--confirm-empty-target`.
2. Install the driver in a virtual environment: `pip install neo4j`.
3. Run:

   ```bash
   NEO4J_PASSWORD=... python3 backend/scripts/neo4j_restore_from_export.py \
       --archive neo4j-20260101-031500.tar \
       --uri bolt://localhost:7687 --user neo4j --confirm-empty-target
   ```

   `--workdir` sets where the members are extracted temporarily (default: the
   current directory; they are deleted afterwards). Roughly 1 minute for ~40k
   nodes and ~50k relationships.
4. The script exits non-zero unless everything matches the manifest: totals,
   per-label and per-relationship-type counts, constraint and index counts, and
   a content fingerprint recomputed from the restored graph (every property
   value and every relationship, independent of internal ids).

If a restore fails midway, wipe the target (a fresh container is the simplest
way) and run it again. Point the application at the restored instance only
after the script prints `verification OK`.

## Format changes

Any change to the archive layout or value tags must bump `format_version` in
both the exporter and the restore script; the restore script rejects versions
it does not know.
