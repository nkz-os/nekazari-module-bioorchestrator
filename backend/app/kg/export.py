"""Deterministic export of a built graph, in the format the backup job writes (plan task 9).

The archive is the one ``neo4j-backup-sftp`` produces and ``scripts/neo4j_restore_from_export.py``
restores: a tar of ``manifest.json``, ``schema.json``, ``nodes.jsonl.gz`` and ``rels.jsonl.gz``
(``{"id","l","p"}`` and ``{"id","t","s","e","p"}`` lines, tagged objects for values JSON cannot
carry), with the same content fingerprint in the manifest, so the restore verifies it unchanged.

What differs is *determinism*: two builds from the same inputs give the same bytes.

* node and relationship ids are not the server's ``elementId`` (those differ per instance) but the
  rank of the row in a canonical order: nodes by (labels, properties, incident relationships as
  seen from the node's content), relationships by (type, start, end, properties); ids are
  zero-padded so text order is rank order;
* lines are written sorted, with sorted keys; gzip and tar carry no clock (mtime 0, no names);
* ``created_utc`` is an argument (the build's declared time, default a fixed marker), never the
  wall clock;
* wall-clock properties are not graph content and are dropped (:data:`VOLATILE_PROPERTIES`:
  ``SchemaVersion.appliedAt``, set by ``datetime()`` when a migration is recorded).

``export_hash`` is the sha256 over the sorted node lines followed by the sorted relationship lines.
The export reads the whole graph into memory: it is for the local build machine, not for the
production server (whose backup job streams).
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import logging
import math
import os
import tarfile
from collections import defaultdict
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any

from neo4j.spatial import Point
from neo4j.time import Date, DateTime, Duration, Time

from neo4j import READ_ACCESS

logger = logging.getLogger(__name__)

FORMAT = "nkz-neo4j-export"
FORMAT_VERSION = 1
MOD = 1 << 256
ID_WIDTH = 10
# Not recorded for reproducibility: the build declares its own time if it wants one in the manifest.
UNRECORDED_CREATED_UTC = "1970-01-01T00:00:00+00:00"
# label -> properties that carry the wall clock; they are not content and would break determinism
VOLATILE_PROPERTIES: dict[str, frozenset[str]] = {"SchemaVersion": frozenset({"appliedAt"})}
MEMBERS = ("manifest.json", "schema.json", "nodes.jsonl.gz", "rels.jsonl.gz")


class ExportError(RuntimeError):
    """The graph cannot be exported, or an archive does not match its manifest."""


# ═════════════════════════════════════════════════════════════════════════════
# value codec (identical to the backup job's and the restore script's)
# ═════════════════════════════════════════════════════════════════════════════

def enc(v: Any) -> Any:
    """Property value -> JSON-safe value (tagged object for non-JSON types)."""
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
    raise ExportError(f"property value of unsupported type {type(v).__name__}")


def canon(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _line(obj: Any) -> bytes:
    return canon(obj) + b"\n"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _gzip(payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, compresslevel=6, mtime=0) as handle:
        handle.write(payload)
    return buffer.getvalue()


def _node_id(rank: int) -> str:
    return f"n{rank:0{ID_WIDTH}d}"


def _rel_id(rank: int) -> str:
    return f"r{rank:0{ID_WIDTH}d}"


# ═════════════════════════════════════════════════════════════════════════════
# reading and canonical ordering
# ═════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ExportResult:
    path: Path
    export_hash: str
    nodes: int
    relationships: int
    nodes_by_label: dict[str, int]
    relationships_by_type: dict[str, int]
    fingerprint: dict[str, str]
    archive_sha256: str
    ambiguous_nodes: int  # nodes that no canonical key tells apart (identical content and surroundings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path), "export_hash": self.export_hash, "nodes": self.nodes,
            "relationships": self.relationships, "nodes_by_label": self.nodes_by_label,
            "relationships_by_type": self.relationships_by_type, "fingerprint": self.fingerprint,
            "archive_sha256": self.archive_sha256, "ambiguous_nodes": self.ambiguous_nodes,
        }


def _props(labels: list[str], raw: dict[str, Any]) -> dict[str, Any]:
    drop: set[str] = set()
    for label in labels:
        drop |= VOLATILE_PROPERTIES.get(label, frozenset())
    return {k: enc(v) for k, v in raw.items() if k not in drop}


def _canonical_graph(
    node_records: list[dict[str, Any]], rel_records: list[dict[str, Any]],
) -> tuple[list[bytes], list[bytes], dict[str, Any]]:
    """Canonical lines and the manifest's ``exported`` block, from the raw records of a graph."""
    nodes: dict[str, dict[str, Any]] = {}
    for rec in node_records:
        labels = sorted(rec["l"])
        props = _props(labels, rec["p"])
        digest = hashlib.sha256(canon({"l": labels, "p": props})).digest()
        nodes[rec["id"]] = {"labels": labels, "props": props, "digest": digest, "hex": digest.hex()}

    incident: dict[str, list[tuple[str, str, str, bytes]]] = defaultdict(list)
    rels: list[dict[str, Any]] = []
    for rec in rel_records:
        try:
            start, end = nodes[rec["s"]], nodes[rec["e"]]
        except KeyError as exc:
            raise ExportError("relationship endpoint missing from the node scan (concurrent write); retry") from exc
        props = {k: enc(v) for k, v in rec["p"].items()}
        pcanon = canon(props)
        incident[rec["s"]].append((rec["t"], "out", end["hex"], pcanon))
        incident[rec["e"]].append((rec["t"], "in", start["hex"], pcanon))
        rels.append({"t": rec["t"], "s": rec["s"], "e": rec["e"], "props": props, "pcanon": pcanon,
                     "digest": hashlib.sha256(canon({"t": rec["t"], "p": props, "s": start["hex"], "e": end["hex"]})).digest()})

    def node_sort_key(item: tuple[str, dict[str, Any]]) -> tuple[Any, ...]:
        element_id, node = item
        return (node["labels"], canon(node["props"]), sorted(incident[element_id]))

    ordered = sorted(nodes.items(), key=node_sort_key)
    ambiguous = sum(1 for a, b in pairwise(ordered) if node_sort_key(a) == node_sort_key(b))
    ranks = {element_id: _node_id(i) for i, (element_id, _) in enumerate(ordered)}

    rel_rows = sorted(({"t": r["t"], "s": ranks[r["s"]], "e": ranks[r["e"]], "p": r["props"],
                        "digest": r["digest"]} for r in rels),
                      key=lambda r: (r["t"], r["s"], r["e"], canon(r["p"])))

    node_lines = sorted(_line({"id": ranks[eid], "l": n["labels"], "p": n["props"]}) for eid, n in nodes.items())
    rel_lines = [_line({"id": _rel_id(i), "t": r["t"], "s": r["s"], "e": r["e"], "p": r["p"]})
                 for i, r in enumerate(rel_rows)]
    rel_lines.sort()

    by_label: dict[str, int] = {}
    node_acc = rel_acc = 0
    for node in nodes.values():
        node_acc = (node_acc + int.from_bytes(node["digest"], "big")) % MOD
        for label in node["labels"] or ["(no label)"]:
            by_label[label] = by_label.get(label, 0) + 1
    by_type: dict[str, int] = {}
    for rel in rels:
        rel_acc = (rel_acc + int.from_bytes(rel["digest"], "big")) % MOD
        by_type[rel["t"]] = by_type.get(rel["t"], 0) + 1
    exported = {
        "nodes": len(nodes), "relationships": len(rels),
        "nodes_by_label": dict(sorted(by_label.items())),
        "relationships_by_type": dict(sorted(by_type.items())),
        "fingerprint": {"nodes": f"{node_acc:064x}", "relationships": f"{rel_acc:064x}"},
    }
    return node_lines, rel_lines, {"exported": exported, "ambiguous": ambiguous}


def export_hash_of(node_lines: list[bytes], rel_lines: list[bytes]) -> str:
    """sha256 over the sorted node lines then the sorted relationship lines."""
    digest = hashlib.sha256()
    for line in sorted(node_lines):
        digest.update(line)
    for line in sorted(rel_lines):
        digest.update(line)
    return digest.hexdigest()


async def _read_schema(session: Any) -> dict[str, Any]:
    async def rows(cypher: str) -> list[dict[str, Any]]:
        result = await session.run(cypher)
        return [dict(r) async for r in result]

    constraints = await rows(
        "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties, createStatement "
        "RETURN name, type, entityType, labelsOrTypes, properties, createStatement ORDER BY name")
    indexes = await rows(
        "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, owningConstraint, createStatement "
        "RETURN name, type, entityType, labelsOrTypes, properties, owningConstraint, createStatement ORDER BY name")
    return {"constraints": constraints, "indexes": indexes}


async def export_graph(
    driver: Any,
    out_path: str | os.PathLike[str],
    *,
    database: str | None = None,
    created_utc: str = UNRECORDED_CREATED_UTC,
) -> ExportResult:
    """Write the graph behind ``driver`` (async) to ``out_path`` as a deterministic archive."""
    async with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        components = await (await session.run("CALL dbms.components() YIELD name, versions RETURN name, versions")).single()
        neo4j_version = components["versions"][0]
        async def count(cypher: str) -> int:
            record = await (await session.run(cypher)).single()
            return int(record["c"])

        before = (await count("MATCH (n) RETURN count(n) AS c"), await count("MATCH ()-[r]->() RETURN count(r) AS c"))
        if before[0] == 0:
            raise ExportError("the database is empty; refusing to export")
        tx = await session.begin_transaction()
        try:
            node_records = [dict(r) async for r in await tx.run(
                "MATCH (n) RETURN elementId(n) AS id, labels(n) AS l, properties(n) AS p")]
            rel_records = [dict(r) async for r in await tx.run(
                "MATCH (a)-[r]->(b) RETURN elementId(r) AS id, type(r) AS t, elementId(a) AS s, "
                "elementId(b) AS e, properties(r) AS p")]
        finally:
            await tx.close()
        after = (await count("MATCH (n) RETURN count(n) AS c"), await count("MATCH ()-[r]->() RETURN count(r) AS c"))
        schema = await _read_schema(session)
    if before != after or (len(node_records), len(rel_records)) != before:
        raise ExportError(f"the graph changed during the export (before={before}, after={after}, "
                          f"read={len(node_records)}/{len(rel_records)}); retry")

    node_lines, rel_lines, info = _canonical_graph(node_records, rel_records)
    exported = info["exported"]
    export_hash = export_hash_of(node_lines, rel_lines)
    nodes_gz = _gzip(b"".join(node_lines))
    rels_gz = _gzip(b"".join(rel_lines))
    manifest = {
        "format": FORMAT, "format_version": FORMAT_VERSION, "created_utc": created_utc,
        "database": database or "neo4j", "neo4j_version": neo4j_version,
        "live_counts_before": {"nodes": before[0], "relationships": before[1]},
        "live_counts_after": {"nodes": after[0], "relationships": after[1]},
        "exported": exported, "export_hash": export_hash,
        "schema_counts": {"constraints": len(schema["constraints"]), "indexes": len(schema["indexes"])},
        "members": {
            "nodes.jsonl.gz": {"sha256": _sha256(nodes_gz), "bytes": len(nodes_gz)},
            "rels.jsonl.gz": {"sha256": _sha256(rels_gz), "bytes": len(rels_gz)},
        },
    }
    payloads = {
        "manifest.json": json.dumps(manifest, indent=2, sort_keys=True).encode(),
        "schema.json": json.dumps(schema, indent=2, sort_keys=True, default=str).encode(),
        "nodes.jsonl.gz": nodes_gz, "rels.jsonl.gz": rels_gz,
    }
    final = Path(out_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(final.name + ".part")
    with tarfile.open(tmp, "w:", format=tarfile.PAX_FORMAT) as tar:
        for name in MEMBERS:
            info_ = tarfile.TarInfo(name)
            info_.size, info_.mtime, info_.mode = len(payloads[name]), 0, 0o600
            info_.uid = info_.gid = 0
            info_.uname = info_.gname = ""
            tar.addfile(info_, io.BytesIO(payloads[name]))
    verify_archive(tmp)
    os.replace(tmp, final)
    archive_sha = _sha256(final.read_bytes())
    logger.info("kg export path=%s nodes=%d relationships=%d export_hash=%s ambiguous_nodes=%d",
                final, exported["nodes"], exported["relationships"], export_hash, info["ambiguous"])
    return ExportResult(
        path=final, export_hash=export_hash, nodes=exported["nodes"], relationships=exported["relationships"],
        nodes_by_label=exported["nodes_by_label"], relationships_by_type=exported["relationships_by_type"],
        fingerprint=exported["fingerprint"], archive_sha256=archive_sha, ambiguous_nodes=info["ambiguous"])


def verify_archive(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Check an archive against its own manifest (members, sha256, line counts, export_hash); return the manifest."""
    with tarfile.open(path, "r:") as tar:
        if tar.getnames() != list(MEMBERS):
            raise ExportError(f"unexpected archive members {tar.getnames()}")

        def member(name: str) -> bytes:
            handle = tar.extractfile(name)
            if handle is None:
                raise ExportError(f"archive is missing {name}")
            return handle.read()

        manifest = json.loads(member("manifest.json"))
        if manifest.get("format") != FORMAT or manifest.get("format_version") != FORMAT_VERSION:
            raise ExportError(f"unsupported archive format {manifest.get('format')!r} v{manifest.get('format_version')!r}")
        lines: dict[str, list[bytes]] = {}
        for name in ("nodes.jsonl.gz", "rels.jsonl.gz"):
            blob = member(name)
            if _sha256(blob) != manifest["members"][name]["sha256"]:
                raise ExportError(f"{name}: sha256 does not match the manifest")
            lines[name] = gzip.decompress(blob).splitlines(keepends=True)
        for name, want in (("nodes.jsonl.gz", manifest["exported"]["nodes"]),
                           ("rels.jsonl.gz", manifest["exported"]["relationships"])):
            if len(lines[name]) != want:
                raise ExportError(f"{name}: {len(lines[name])} lines, manifest says {want}")
        if export_hash_of(lines["nodes.jsonl.gz"], lines["rels.jsonl.gz"]) != manifest.get("export_hash"):
            raise ExportError("export_hash does not match the archive lines")
    return manifest


__all__ = [
    "UNRECORDED_CREATED_UTC",
    "VOLATILE_PROPERTIES",
    "ExportError",
    "ExportResult",
    "canon",
    "enc",
    "export_graph",
    "export_hash_of",
    "verify_archive",
]
