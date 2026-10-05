#!/usr/bin/env python3
"""Restore a Neo4j logical export (neo4j-<ts>.tar) into an EMPTY Neo4j 5.x database.

The archive comes from the scheduled Neo4j backup job (manifest.json,
schema.json, nodes.jsonl.gz, rels.jsonl.gz). The restore needs only Bolt
access: no APOC, no server-side import directory.

Safety
  * Refuses to run unless the target has no nodes, no constraints and no
    user-defined indexes, and only with --confirm-empty-target.
  * Never deletes anything it did not create; a failed run leaves a partial
    graph in the (previously empty) target: wipe the target and retry.
  * The password is read from NEO4J_PASSWORD and is never printed.

Verification (exit code != 0 on any mismatch): per-label and per-relationship-
type counts, constraint/index counts, and a content fingerprint recomputed
from the restored graph and compared with the one recorded at backup time
(covers every property value and every relationship, independent of ids).

Example (throwaway local target, never production)
  docker run -d --name neo4j-restore-drill -p 7687:7687 \\
      -e NEO4J_AUTH=neo4j/<password> neo4j:5.26-community
  NEO4J_PASSWORD=<password> python3 neo4j_restore_from_export.py \\
      --archive neo4j-20260101-031500.tar --confirm-empty-target
"""
import argparse
import base64
import gzip
import hashlib
import json
import math
import os
import tarfile
import time

import pytz  # installed with the neo4j driver; it is what the driver uses for named zones
from neo4j.spatial import CartesianPoint, Point, WGS84Point
from neo4j.time import Date, DateTime, Duration, Time

from neo4j import GraphDatabase

FORMAT = "nkz-neo4j-export"
SUPPORTED_FORMAT_VERSION = 1
BATCH = 2000
TMP_LABEL = "__NkzRestore"
TMP_ID = "__nkzid"
MOD = 1 << 256


def log(msg):
    print(f"restore: {msg}", flush=True)


def q(name):
    """Backtick-quote a label / relationship type."""
    return "`" + name.replace("`", "``") + "`"


# --- value codec (must match the exporter) ---------------------------------

def enc(v):
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return {"$t": "float", "v": repr(v)}
        return v
    if type(v) in (list, tuple):
        return [enc(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return {"$t": "bytes", "v": base64.b64encode(bytes(v)).decode("ascii")}
    if isinstance(v, DateTime):
        if v.tzinfo is None:
            return {"$t": "localdatetime", "v": v.iso_format()}
        out = {"$t": "datetime", "v": v.iso_format()}
        zone = getattr(v.tzinfo, "key", None) or getattr(v.tzinfo, "zone", None)
        if zone:
            out["zone"] = zone
        return out
    if isinstance(v, Date):
        return {"$t": "date", "v": v.iso_format()}
    if isinstance(v, Time):
        return {"$t": "time" if v.tzinfo is not None else "localtime", "v": v.iso_format()}
    if isinstance(v, Duration):
        return {"$t": "duration", "m": v.months, "d": v.days, "s": v.seconds, "n": v.nanoseconds}
    if isinstance(v, Point):
        return {"$t": "point", "srid": v.srid, "c": list(v)}
    raise SystemExit(f"restore: value of unsupported type {type(v).__name__}")


def dec(v):
    if isinstance(v, list):
        return [dec(x) for x in v]
    if not isinstance(v, dict):
        return v
    t = v["$t"]
    if t == "float":
        return float(v["v"])
    if t == "bytes":
        return base64.b64decode(v["v"])
    if t == "datetime":
        value = DateTime.from_iso_format(v["v"])
        if v.get("zone"):
            value = value.astimezone(pytz.timezone(v["zone"]))
        return value
    if t == "localdatetime":
        return DateTime.from_iso_format(v["v"])
    if t == "date":
        return Date.from_iso_format(v["v"])
    if t in ("time", "localtime"):
        return Time.from_iso_format(v["v"])
    if t == "duration":
        return Duration(months=v["m"], days=v["d"], seconds=v["s"], nanoseconds=v["n"])
    if t == "point":
        coords = tuple(v["c"])
        cls = {4326: WGS84Point, 4979: WGS84Point, 7203: CartesianPoint, 9157: CartesianPoint}.get(v["srid"])
        if cls is None:
            raise SystemExit(f"restore: unsupported point srid {v['srid']}")
        return cls(coords)
    raise SystemExit(f"restore: unknown value tag {t!r}")


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


# --- helpers ---------------------------------------------------------------

def member_json(tar, name):
    fh = tar.extractfile(name)
    if fh is None:
        raise SystemExit(f"restore: archive is missing {name}")
    return json.loads(fh.read().decode("utf-8"))


def verified_member(tar, name, workdir, want_sha):
    path = os.path.join(workdir, name)
    sha = hashlib.sha256()
    src = tar.extractfile(name)
    with open(path, "wb") as out:
        for block in iter(lambda: src.read(1024 * 1024), b""):
            sha.update(block)
            out.write(block)
    if sha.hexdigest() != want_sha:
        raise SystemExit(f"restore: {name} sha256 does not match the manifest (corrupt archive)")
    return path


def batches(path):
    batch = []
    with gzip.open(path, "rb") as fh:
        for raw in fh:
            batch.append(json.loads(raw))
            if len(batch) >= BATCH:
                yield batch
                batch = []
    if batch:
        yield batch


def run_batch(session, query, rows):
    session.run(query, rows=rows).consume()


def fingerprint_and_counts(session):
    """Same canonical form and multiset hash the exporter used."""
    node_hash, by_label, by_type = {}, {}, {}
    node_acc = rel_acc = n_nodes = n_rels = 0
    for rec in session.run("MATCH (n) RETURN elementId(n) AS id, labels(n) AS l, properties(n) AS p"):
        props = {k: enc(v) for k, v in rec["p"].items()}
        digest = hashlib.sha256(canon({"l": sorted(rec["l"]), "p": props})).digest()
        node_hash[rec["id"]] = digest.hex()
        node_acc = (node_acc + int.from_bytes(digest, "big")) % MOD
        for label in rec["l"] or ["(no label)"]:
            by_label[label] = by_label.get(label, 0) + 1
        n_nodes += 1
    for rec in session.run(
        "MATCH (a)-[r]->(b) RETURN type(r) AS t, elementId(a) AS s, elementId(b) AS e, properties(r) AS p"
    ):
        props = {k: enc(v) for k, v in rec["p"].items()}
        digest = hashlib.sha256(
            canon({"t": rec["t"], "p": props, "s": node_hash[rec["s"]], "e": node_hash[rec["e"]]})
        ).digest()
        rel_acc = (rel_acc + int.from_bytes(digest, "big")) % MOD
        by_type[rec["t"]] = by_type.get(rec["t"], 0) + 1
        n_rels += 1
    return {
        "nodes": n_nodes, "relationships": n_rels,
        "nodes_by_label": dict(sorted(by_label.items())),
        "relationships_by_type": dict(sorted(by_type.items())),
        "fingerprint": {"nodes": f"{node_acc:064x}", "relationships": f"{rel_acc:064x}"},
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive", required=True)
    ap.add_argument("--uri", default="bolt://localhost:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--database", default="neo4j")
    ap.add_argument("--workdir", default=".", help="scratch dir for the extracted members (default: cwd)")
    ap.add_argument("--confirm-empty-target", action="store_true",
                    help="required: acknowledge that the target is a fresh, empty database")
    args = ap.parse_args()

    if not args.confirm_empty_target:
        raise SystemExit("restore: refusing to run without --confirm-empty-target")
    password = os.environ.get("NEO4J_PASSWORD")
    if not password:
        raise SystemExit("restore: NEO4J_PASSWORD is not set")

    with tarfile.open(args.archive, "r:*") as tar:
        manifest = member_json(tar, "manifest.json")
        schema = member_json(tar, "schema.json")
        if manifest.get("format") != FORMAT or manifest.get("format_version") != SUPPORTED_FORMAT_VERSION:
            raise SystemExit(f"restore: unsupported archive ({manifest.get('format')!r} "
                             f"v{manifest.get('format_version')!r})")
        exp = manifest["exported"]
        log(f"archive created {manifest['created_utc']} from Neo4j {manifest['neo4j_version']}: "
            f"nodes={exp['nodes']} rels={exp['relationships']}")

        workdir = os.path.abspath(args.workdir)
        paths = []
        driver = GraphDatabase.driver(args.uri, auth=(args.user, password))
        driver.verify_connectivity()
        temp_constraint = False
        try:
            with driver.session(database=args.database) as session:
                nodes = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
                constraints = session.run("SHOW CONSTRAINTS YIELD name RETURN count(name) AS c").single()["c"]
                indexes = session.run(
                    "SHOW INDEXES YIELD type WHERE type <> 'LOOKUP' RETURN count(*) AS c"
                ).single()["c"]
                if nodes or constraints or indexes:
                    raise SystemExit(f"restore: target is NOT empty (nodes={nodes}, constraints={constraints}, "
                                     f"indexes={indexes}); refusing")
                nodes_path = verified_member(tar, "nodes.jsonl.gz", workdir, manifest["members"]["nodes.jsonl.gz"]["sha256"])
                rels_path = verified_member(tar, "rels.jsonl.gz", workdir, manifest["members"]["rels.jsonl.gz"]["sha256"])
                paths = [nodes_path, rels_path]

                session.run(f"CREATE CONSTRAINT nkz_restore_id FOR (n:{q(TMP_LABEL)}) "
                            f"REQUIRE n.{q(TMP_ID)} IS UNIQUE").consume()
                temp_constraint = True

                t0 = time.time()
                done = 0
                for batch in batches(nodes_path):
                    groups = {}
                    for rec in batch:
                        if TMP_ID in rec["p"]:
                            raise SystemExit(f"restore: node property {TMP_ID} clashes with the temporary id")
                        groups.setdefault(tuple(sorted(rec["l"])), []).append(
                            {"id": rec["id"], "p": {k: dec(v) for k, v in rec["p"].items()}}
                        )
                    for labels, rows in groups.items():
                        label_expr = "".join(f":{q(x)}" for x in (TMP_LABEL, *labels))
                        run_batch(session, f"UNWIND $rows AS r CREATE (n{label_expr}) "
                                           f"SET n = r.p SET n.{q(TMP_ID)} = r.id", rows)
                    done += len(batch)
                log(f"nodes created: {done} in {time.time() - t0:.1f}s")

                t0 = time.time()
                done = 0
                for batch in batches(rels_path):
                    groups = {}
                    for rec in batch:
                        groups.setdefault(rec["t"], []).append(
                            {"s": rec["s"], "e": rec["e"], "p": {k: dec(v) for k, v in rec["p"].items()}}
                        )
                    for rtype, rows in groups.items():
                        run_batch(session, f"UNWIND $rows AS r "
                                           f"MATCH (a:{q(TMP_LABEL)} {{{q(TMP_ID)}: r.s}}), "
                                           f"(b:{q(TMP_LABEL)} {{{q(TMP_ID)}: r.e}}) "
                                           f"CREATE (a)-[x:{q(rtype)}]->(b) SET x = r.p", rows)
                    done += len(batch)
                log(f"relationships created: {done} in {time.time() - t0:.1f}s")

                session.run(
                    f"MATCH (n:{q(TMP_LABEL)}) CALL (n) {{ REMOVE n:{q(TMP_LABEL)}, n.{q(TMP_ID)} }} "
                    f"IN TRANSACTIONS OF 10000 ROWS"
                ).consume()
                session.run("DROP CONSTRAINT nkz_restore_id").consume()
                temp_constraint = False

                for c in schema["constraints"]:
                    log(f"constraint {c['name']}")
                    session.run(c["createStatement"]).consume()
                for i in schema["indexes"]:
                    if i["type"] == "LOOKUP" or i.get("owningConstraint"):
                        continue
                    log(f"index {i['name']}")
                    session.run(i["createStatement"]).consume()
                session.run("CALL db.awaitIndexes(600)").consume()

                n_constraints = session.run("SHOW CONSTRAINTS YIELD name RETURN count(name) AS c").single()["c"]
                n_indexes = session.run("SHOW INDEXES YIELD name RETURN count(name) AS c").single()["c"]
                got = fingerprint_and_counts(session)
        finally:
            for p in paths:
                if os.path.exists(p):
                    os.remove(p)
            if temp_constraint:
                try:
                    with driver.session(database=args.database) as cleanup:
                        cleanup.run("DROP CONSTRAINT nkz_restore_id IF EXISTS").consume()
                except Exception as exc:  # noqa: BLE001
                    log(f"WARNING: could not drop temporary constraint: {exc}")
            driver.close()

    problems = []
    for key in ("nodes", "relationships"):
        if got[key] != exp[key]:
            problems.append(f"{key}: restored={got[key]} archive={exp[key]}")
    for key, what in (("nodes_by_label", "label"), ("relationships_by_type", "relationship type")):
        for k in sorted(set(got[key]) | set(exp[key])):
            if got[key].get(k) != exp[key].get(k):
                problems.append(f"{what} {k}: restored={got[key].get(k)} archive={exp[key].get(k)}")
    if n_constraints != manifest["schema_counts"]["constraints"]:
        problems.append(f"constraints: restored={n_constraints} archive={manifest['schema_counts']['constraints']}")
    if n_indexes != manifest["schema_counts"]["indexes"]:
        problems.append(f"indexes: restored={n_indexes} archive={manifest['schema_counts']['indexes']}")
    for key in ("nodes", "relationships"):
        if got["fingerprint"][key] != exp["fingerprint"][key]:
            problems.append(f"content fingerprint ({key}) differs")
    log(f"restored nodes={got['nodes']} rels={got['relationships']} labels={len(got['nodes_by_label'])} "
        f"rel types={len(got['relationships_by_type'])} constraints={n_constraints} indexes={n_indexes}")
    if problems:
        for p in problems:
            log(f"MISMATCH {p}")
        raise SystemExit("restore: VERIFICATION FAILED")
    log("verification OK: counts, schema object counts and content fingerprint match the archive manifest")


if __name__ == "__main__":
    main()
