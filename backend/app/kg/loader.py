"""Loader of canonical bundles into Neo4j (spec section 4, amendment E4; plan task 7).

``load(bundle, driver)`` writes one source's :class:`~app.kg.contracts.Bundle` and nothing else. The
write is a pure function of the bundle and the registries: no clock, no random ids, rows sent in key
order, so the same inputs give the same graph and a second load changes nothing.

* **MERGE by natural key**, label by label in dependency order (Source, Crop / Variable / TrialSite,
  ArticleSource, Study, Variety, ObservationUnit, Observation, relationships). Every statement is an
  ``UNWIND`` over one batch and every batch is its own small transaction, so a load never holds more
  than ``batch_size`` rows in one transaction.
* **Nothing is written before the bundle is proven closed**: every unit names a document, study, site
  and variety that the bundle carries, every observation a unit, every key is unique, the registries
  are the ones the bundle was built with. A violation is a :class:`LoadError` and the graph is
  untouched. After a write, a relationship batch that matched fewer rows than it was sent is a
  :class:`LoadError` too (never a silent skip).
* **Expand and contract (E4).** The Observation is the truth. For the hot queries the loader writes
  the unit's *derived copy* of the observations whose variable is flagged ``denormalize`` in
  ``variables.yaml`` under the property names ``dao.py`` reads today (``yieldKgHa``,
  ``yieldRelativePct``...), so the existing queries keep working unchanged. A flagged variable the
  loader has no property for is an error, not a silently missing copy. The copy of the yield is
  checked against the unit row the engine built: they must agree.
* **Legacy JSON properties** (``qualityParams``, ``diseaseScores``, ``agronomicTraits`` and their
  ``...Unified`` forms) are generated *from the Observations* by the raw group in their ``raw_key``
  (``quality_params.x``). They are a compatibility copy that F2 removes.
* **Nothing is inferred.** A value the bundle does not carry is not written (``null`` removes the
  property), and the gaps of a row are stored on its node as ``field: reason`` strings.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from app.ingestion.normalization_registry import (
    normalize_variety_name,
    transform_traits_to_unified,
)

from . import identity
from .contracts import Bundle
from .model import DocumentRow, ObservationRow, SiteRow, StudyRow, UnitRow, VarietyRow
from .registries import Registries, load_registries

logger = logging.getLogger(__name__)

# Measured, not guessed (scripts/kg_sizing_benchmark.py, server sized like production: 768m heap): the
# heaviest write step (the unit MERGE, about 2.4 KB of properties a row) commits 500 rows in one transaction
# under a 16 MiB transaction-memory limit, 1000 rows need 32 MiB and 5000 rows 128 MiB; a single transaction
# of 20000 units fails even under 256 MiB. 500 keeps a transaction under 1/48 of the heap.
DEFAULT_BATCH_SIZE = 500

# Extra label of an ObservationUnit by the type of its study; ``reference`` has no unit label in F1.
UNIT_LABELS: dict[str, str | None] = {
    "variety": "VarietyTrial",
    "management": "ManagementTrial",
    "long_term": "LongTermPlot",
    "regional_stat": "RegionalStatistic",
    "reference": None,
}

# Raw groups of an extraction whose observations also go into the legacy JSON text properties.
LEGACY_JSON_GROUPS: dict[str, str] = {
    "quality_params": "qualityParams",
    "disease_scores": "diseaseScores",
    "agronomic_traits": "agronomicTraits",
}


class LoadError(RuntimeError):
    """The bundle cannot be loaded as given (or the graph did not take it). Raised before or at a write."""


@dataclass(frozen=True)
class StepReport:
    """What one write step did: rows sent, batches run, and what the server says it created."""

    name: str
    rows: int
    batches: int
    nodes_created: int
    relationships_created: int
    properties_set: int


@dataclass(frozen=True)
class LoadReport:
    source_id: str
    registries_hash: str
    steps: tuple[StepReport, ...]

    @property
    def nodes_created(self) -> int:
        return sum(step.nodes_created for step in self.steps)

    @property
    def relationships_created(self) -> int:
        return sum(step.relationships_created for step in self.steps)

    @property
    def batches(self) -> int:
        return sum(step.batches for step in self.steps)

    def step(self, name: str) -> StepReport:
        for step in self.steps:
            if step.name == name:
                return step
        raise KeyError(name)


# ═════════════════════════════════════════════════════════════════════════════
# legacy compatibility copy of a unit (pure, no database)
# ═════════════════════════════════════════════════════════════════════════════

def _legacy_value(obs: ObservationRow) -> Any:
    """The value as the source printed it (what the old JSON text held), not the converted one."""
    value = obs.value_original if obs.value_original is not None else obs.value
    if value is None:
        return obs.value_text
    # the model keeps numbers as floats; a whole number prints as the source printed it ("5", not "5.0")
    return int(value) if float(value).is_integer() else value


def _group_of(raw_key: str | None) -> tuple[str, str] | None:
    """``("quality_params", "humedad_pct")`` for a raw key of a legacy JSON group, else None."""
    if not raw_key or "." not in raw_key:
        return None
    group, _, name = raw_key.partition(".")
    return (group, name) if group in LEGACY_JSON_GROUPS and name else None


def _copy_yield(obs: ObservationRow) -> dict[str, Any]:
    return {
        "yieldKgHa": obs.value,
        "yieldMetric": obs.metric,
        "yieldBasis": obs.basis,
        "yieldMoisturePct": obs.moisture_pct,
        "yieldValueOriginal": obs.value_original,
        "yieldUnitOriginal": obs.unit_original,
        "yieldDerivationMethod": obs.derivation_method,
    }


def _copy_relative_yield(obs: ObservationRow) -> dict[str, Any]:
    return {"yieldRelativePct": obs.value}


# variable id -> (unit properties its single Observation is copied to, how). Every variable flagged
# ``denormalize`` in variables.yaml must be here (checked per load).
DENORMALIZED_COPIES: dict[str, tuple[tuple[str, ...], Callable[[ObservationRow], dict[str, Any]]]] = {
    "crop_yield": (("yieldKgHa", "yieldMetric", "yieldBasis", "yieldMoisturePct", "yieldValueOriginal",
                    "yieldUnitOriginal", "yieldDerivationMethod"), _copy_yield),
    "relative_yield_pct": (("yieldRelativePct",), _copy_relative_yield),
}


def _factor_levels(unit: UnitRow) -> list[str]:
    """One canonical JSON text per factor level, in canonical order (the order the source lists is not data)."""
    return sorted(identity.canonical_json({"t": [level.factor, level.level, level.unit]})
                  for level in unit.factor_levels)


def _gap_texts(gaps: Iterable[Any]) -> list[str]:
    return [f"{gap.field}: {gap.reason}" for gap in gaps]


def unit_properties(unit: UnitRow, observations: Sequence[ObservationRow], registries: Registries) -> dict[str, Any]:
    """Every property of an ObservationUnit node: canonical, then the derived legacy copy.

    ``observations`` are the unit's own, in any order. ``None`` values are kept in the map on purpose:
    ``SET n += map`` removes a property whose new value is null, so a rebuild with fewer facts never
    leaves a stale one behind.
    """
    key = identity.unit_key(unit)
    crop = registries.crop(unit.crop_eppo)
    props: dict[str, Any] = {
        # identity and provenance
        "unitKey": key,
        "source_id": unit.source_id,
        "dataSource": unit.source_id,
        "documentKey": unit.document_key,
        "siteKey": unit.site_key,
        "locator": unit.locator,
        # observed fields, as printed
        "cropEppo": unit.crop_eppo,
        "variety": unit.raw_variety,
        "rawSite": unit.raw_site,
        "rawSeason": unit.raw_season,
        "rawIrrigation": unit.raw_irrigation,
        "rawProductionSystem": unit.raw_production_system,
        "rootstock": unit.rootstock,
        "clone": unit.clone,
        "plantingYear": unit.planting_year,
        "rowDiscriminator": unit.row_discriminator,
        "factorLevels": _factor_levels(unit),
        # derived and normalised
        "year": unit.year,
        "irrigationRegime": unit.irrigation_regime,
        "productionSystem": unit.production_system,
        "purpose": unit.purpose,
        "productivityClass": unit.productivity_class,
        "irrigationDerivation": unit.irrigation_derivation,
        "irrigationYieldLowKgHa": unit.irrigation_yield_low_kg_ha,
        "irrigationYieldHighKgHa": unit.irrigation_yield_high_kg_ha,
        "gaps": _gap_texts(unit.gaps),
        # legacy names dao.py reads today (expand and contract)
        "mergeKey": key,
        "cropScientific": crop.scientific_name if crop is not None else None,
        "varietyNormalized": normalize_variety_name(unit.raw_variety),
        "trialLocation": unit.raw_site,
    }

    # derived copy of the observations whose variable is flagged denormalize (E4)
    by_variable: dict[str, list[ObservationRow]] = defaultdict(list)
    for obs in observations:
        by_variable[obs.variable_id].append(obs)
    for variable_id, (names, build) in DENORMALIZED_COPIES.items():
        props.update(dict.fromkeys(names))
        if not registries.variable(variable_id).denormalize:
            continue
        found = by_variable.get(variable_id, [])
        if len(found) > 1:
            raise LoadError(
                f"unit {key}: {len(found)} observations of denormalized variable {variable_id!r}; "
                "the copy needs exactly one (a qualifier, stage or date tells them apart: not copyable)")
        if found:
            props.update(build(found[0]))
    if props["yieldKgHa"] != unit.yield_kg_ha or (props["yieldKgHa"] is not None and (
            props["yieldMetric"] != unit.yield_metric or props["yieldBasis"] != unit.yield_basis
            or props["yieldMoisturePct"] != unit.yield_moisture_pct)):
        raise LoadError(
            f"unit {key}: the yield on the unit row ({unit.yield_kg_ha!r}) and in its crop_yield "
            f"observation ({props['yieldKgHa']!r}) disagree; the Observation is the truth, the bundle is inconsistent")

    # legacy JSON text from the Observations, by raw group
    groups: dict[str, dict[str, Any]] = {legacy: {} for legacy in LEGACY_JSON_GROUPS.values()}
    for obs in sorted(observations, key=lambda o: (o.raw_key or "", identity.obs_key(o))):
        located = _group_of(obs.raw_key)
        if located is not None:
            group, name = located
            groups[LEGACY_JSON_GROUPS[group]][name] = _legacy_value(obs)
    for legacy, content in groups.items():
        props[legacy] = json.dumps(content, ensure_ascii=False, sort_keys=True) if content else None
    traits_unified, disease_unified = transform_traits_to_unified(
        groups["agronomicTraits"] or None, groups["diseaseScores"] or None, unit.source_id)
    props["agronomicTraitsUnified"] = traits_unified
    props["diseaseScoresUnified"] = disease_unified
    return props


# ═════════════════════════════════════════════════════════════════════════════
# the plan: every row as a parameter map, checked closed, before anything is written
# ═════════════════════════════════════════════════════════════════════════════

_REL_NAMES = (
    "document_source", "study_source", "study_crop", "variety_crop", "unit_crop", "unit_variety",
    "unit_study", "unit_document", "trial_at", "obs_unit", "obs_variable",
)


@dataclass(frozen=True)
class _Plan:
    source: dict[str, Any]
    crops: list[dict[str, Any]]
    variables: list[dict[str, Any]]
    sites: list[dict[str, Any]]
    documents: list[dict[str, Any]]
    studies: list[dict[str, Any]]
    varieties: list[dict[str, Any]]
    units_by_label: dict[str | None, list[dict[str, Any]]]
    observations: list[dict[str, Any]]
    rels: dict[str, list[dict[str, Any]]]


def _by_key(rows: Iterable[Any], key_of: Callable[[Any], str], what: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for row in rows:
        key = key_of(row)
        if key in out:
            kind = "duplicate" if out[key] == row else "conflicting"
            raise LoadError(f"{kind} {what} rows share the key {key}")
        out[key] = row
    return out


def _site_properties(site: SiteRow, source_id: str) -> dict[str, Any]:
    return {
        "siteKey": identity.site_key(site), "name": site.name, "siteKind": site.site_kind,
        "country": site.country, "latitude": site.latitude, "longitude": site.longitude,
        "coordinateSource": site.coordinate_source, "climateClass": site.climate_class,
        "sourceIds": sorted({*site.source_ids, source_id}), "gaps": _gap_texts(site.gaps),
    }


def _document_properties(doc: DocumentRow) -> dict[str, Any]:
    key = identity.document_key(doc)
    return {
        "documentKey": key, "mergeKey": key, "source_id": doc.source_id,
        "articleTitle": doc.title, "issueNumber": doc.issue, "year": doc.year,
        "documentUrl": doc.url, "fileName": doc.file_name, "rawSha256": doc.raw_sha256,
        "gaps": _gap_texts(doc.gaps),
    }


def _study_properties(study: StudyRow) -> dict[str, Any]:
    return {
        "studyKey": identity.study_key(study), "source_id": study.source_id, "studyType": study.study_type,
        "cropEppo": study.crop_eppo, "rawSeason": study.raw_season, "rawScope": study.raw_scope,
        "name": study.name, "year": study.year, "design": study.design, "gaps": _gap_texts(study.gaps),
    }


def _variety_properties(variety: VarietyRow) -> dict[str, Any]:
    return {
        "varietyKey": identity.variety_key(variety), "cropEppo": variety.crop_eppo, "name": variety.name,
        "registryId": variety.registry_id, "status": variety.status, "aliases": list(variety.aliases),
        "sourceIds": sorted(variety.source_ids), "gaps": _gap_texts(variety.gaps),
    }


def _observation_properties(obs: ObservationRow) -> dict[str, Any]:
    return {
        "obsKey": identity.obs_key(obs), "variableId": obs.variable_id, "stage": obs.stage, "date": obs.date,
        "qualifier": obs.qualifier, "value": obs.value, "valueText": obs.value_text, "unit": obs.unit,
        "basis": obs.basis, "moisturePct": obs.moisture_pct, "metric": obs.metric, "purpose": obs.purpose,
        "valueOriginal": obs.value_original, "unitOriginal": obs.unit_original,
        "derivationMethod": obs.derivation_method, "rawKey": obs.raw_key, "locator": obs.locator,
        "gaps": _gap_texts(obs.gaps),
    }


def _source_properties(registries: Registries, source_id: str) -> dict[str, Any]:
    try:
        source = registries.source(source_id)
    except KeyError as exc:
        raise LoadError(f"source {source_id!r} is not in the sources registry") from exc
    licence = source.licence
    return {
        "sourceId": source.source_id, "name": source.name, "institution": source.institution,
        "country": source.country, "url": source.url, "licenceId": licence.licence_id,
        "licenceTermsUrl": licence.terms_url, "commercialUse": licence.commercial_use,
        "tdmReserved": licence.tdm_reserved, "attributionText": licence.attribution_text,
        "attributionUrl": licence.attribution_url, "permissionRef": licence.permission_ref,
        "licenceCheckedAt": licence.checked_at.isoformat(),
    }


def _crop_properties(registries: Registries, eppo: str) -> dict[str, Any]:
    crop = registries.crop(eppo)
    if crop is None:
        raise LoadError(f"crop {eppo!r} is not in the crops registry")
    return {
        "eppo": crop.eppo, "scientificName": crop.scientific_name, "family": crop.family,
        "mainProduct": crop.main_product, "purposes": list(crop.purposes),
    }


def _variable_properties(registries: Registries, variable_id: str) -> dict[str, Any]:
    variable = registries.variable(variable_id)
    return {
        "variableId": variable.id, "trait": variable.trait, "method": variable.method, "scale": variable.scale,
        "unit": variable.unit,
        "domainMin": variable.domain[0] if variable.domain else None,
        "domainMax": variable.domain[1] if variable.domain else None,
        "direction": variable.direction, "denormalize": variable.denormalize,
        "cropOntologyId": variable.crop_ontology_id,
    }


def _plan(bundle: Bundle, registries: Registries) -> _Plan:
    if bundle.report.registries_hash != registries.registries_hash:
        raise LoadError("the bundle was built with other registries than the ones given to the loader "
                        f"({bundle.report.registries_hash[:12]} vs {registries.registries_hash[:12]}); rebuild it")
    sid = bundle.source_id
    for variable in registries.variables:
        if variable.denormalize and variable.id not in DENORMALIZED_COPIES:
            raise LoadError(f"variable {variable.id!r} is flagged denormalize but the loader has no unit property for it")
    for kind, rows in (("document", bundle.documents), ("study", bundle.studies), ("unit", bundle.units)):
        for row in rows:
            if row.source_id != sid:
                raise LoadError(f"{kind} row of source {row.source_id!r} in the bundle of {sid!r}")

    documents = _by_key(bundle.documents, identity.document_key, "document")
    studies = _by_key(bundle.studies, identity.study_key, "study")
    sites = _by_key(bundle.sites, identity.site_key, "site")
    varieties = _by_key(bundle.varieties, identity.variety_key, "variety")
    units = _by_key(bundle.units, identity.unit_key, "unit")
    observations = _by_key(bundle.observations, identity.obs_key, "observation")

    obs_by_unit: dict[str, list[ObservationRow]] = defaultdict(list)
    for key, obs in observations.items():
        if obs.unit_key not in units:
            raise LoadError(f"observation {key} ({obs.variable_id}) is on unit {obs.unit_key}, which the bundle lacks")
        obs_by_unit[obs.unit_key].append(obs)

    crops_used = {u.crop_eppo for u in units.values()}
    crops_used |= {s.crop_eppo for s in studies.values() if s.crop_eppo}
    crops_used |= {v.crop_eppo for v in varieties.values()}
    variables_used = {o.variable_id for o in observations.values()}

    rels: dict[str, list[dict[str, Any]]] = {name: [] for name in _REL_NAMES}
    units_by_label: dict[str | None, list[dict[str, Any]]] = defaultdict(list)
    for key in sorted(units):
        unit = units[key]
        study = studies.get(unit.study_key or "")
        if study is None:
            raise LoadError(f"unit {key} has no study in the bundle (study_key {unit.study_key!r})")
        if unit.document_key not in documents:
            raise LoadError(f"unit {key} names document {unit.document_key}, which the bundle lacks")
        if unit.site_key is not None and unit.site_key not in sites:
            raise LoadError(f"unit {key} names site {unit.site_key!r}, which the bundle lacks")
        if unit.variety_key is not None and unit.variety_key not in varieties:
            raise LoadError(f"unit {key} names variety {unit.variety_key}, which the bundle lacks")
        if study.study_type not in UNIT_LABELS:
            raise LoadError(f"unit {key}: study type {study.study_type!r} has no unit label in the loader")
        props = unit_properties(unit, obs_by_unit.get(key, ()), registries)
        units_by_label[UNIT_LABELS[study.study_type]].append({"unitKey": key, "props": props})
        rels["unit_crop"].append({"a": key, "b": unit.crop_eppo})
        rels["unit_study"].append({"a": key, "b": unit.study_key})
        rels["unit_document"].append({"a": key, "b": unit.document_key})
        if unit.site_key is not None:
            rels["trial_at"].append({"a": key, "b": unit.site_key})
        if unit.variety_key is not None:
            rels["unit_variety"].append({"a": key, "b": unit.variety_key})
    for key in sorted(documents):
        rels["document_source"].append({"a": key, "b": sid})
    for key in sorted(studies):
        rels["study_source"].append({"a": key, "b": sid})
        if studies[key].crop_eppo:
            rels["study_crop"].append({"a": key, "b": studies[key].crop_eppo})
    for key in sorted(varieties):
        rels["variety_crop"].append({"a": key, "b": varieties[key].crop_eppo})
    for key in sorted(observations):
        rels["obs_unit"].append({"a": key, "b": observations[key].unit_key})
        rels["obs_variable"].append({"a": key, "b": observations[key].variable_id})

    return _Plan(
        source=_source_properties(registries, sid),
        crops=[_crop_properties(registries, eppo) for eppo in sorted(crops_used)],
        variables=[_variable_properties(registries, v) for v in sorted(variables_used)],
        sites=[_site_properties(sites[k], sid) for k in sorted(sites)],
        documents=[_document_properties(documents[k]) for k in sorted(documents)],
        studies=[_study_properties(studies[k]) for k in sorted(studies)],
        varieties=[_variety_properties(varieties[k]) for k in sorted(varieties)],
        units_by_label=dict(units_by_label),
        observations=[{"obsKey": k, "props": _observation_properties(observations[k])} for k in sorted(observations)],
        rels=rels,
    )


# ═════════════════════════════════════════════════════════════════════════════
# writing
# ═════════════════════════════════════════════════════════════════════════════

_SOURCE = "UNWIND $rows AS r MERGE (n:Source {sourceId: r.sourceId}) SET n += r"
_CROP = "UNWIND $rows AS r MERGE (n:Crop {eppo: r.eppo}) SET n += r"
_VARIABLE = "UNWIND $rows AS r MERGE (n:Variable {variableId: r.variableId}) SET n += r"
_DOCUMENT = "UNWIND $rows AS r MERGE (n:ArticleSource {documentKey: r.documentKey}) SET n += r"
_STUDY = "UNWIND $rows AS r MERGE (n:Study {studyKey: r.studyKey}) SET n += r"
_VARIETY = "UNWIND $rows AS r MERGE (n:Variety {varietyKey: r.varietyKey}) SET n += r"
_OBSERVATION = "UNWIND $rows AS r MERGE (n:Observation {obsKey: r.obsKey}) SET n += r.props"

# Coordinates and the country come from the sites registry. The climate class is kept when the
# registry has none (an enrichment step may have written it), and the sources accumulate.
_SITE = """
UNWIND $rows AS r
MERGE (n:TrialSite {siteKey: r.siteKey})
SET n.name = r.name, n.siteKind = r.siteKind, n.country = r.country,
    n.latitude = r.latitude, n.longitude = r.longitude, n.coordinateSource = r.coordinateSource,
    n.gaps = r.gaps,
    n.climateClass = coalesce(r.climateClass, n.climateClass),
    n.sourceIds = reduce(acc = coalesce(n.sourceIds, []), s IN r.sourceIds |
                         CASE WHEN s IN acc THEN acc ELSE acc + s END)
SET n.source_id = n.sourceIds[0]
"""


def _unit_statement(label: str | None) -> str:
    extra = f"SET n:{label}\n" if label else ""
    return f"UNWIND $rows AS r\nMERGE (n:ObservationUnit {{unitKey: r.unitKey}})\n{extra}SET n += r.props"


def _rel_statement(a_label: str, a_key: str, rel: str, b_label: str, b_key: str) -> str:
    return (f"UNWIND $rows AS r MATCH (a:{a_label} {{{a_key}: r.a}}) MATCH (b:{b_label} {{{b_key}: r.b}}) "
            f"MERGE (a)-[:{rel}]->(b) RETURN count(*) AS matched")


# plan.rels name -> statement, in write order (parents before children)
_RELATIONSHIPS: tuple[tuple[str, str], ...] = (
    ("document_source", _rel_statement("ArticleSource", "documentKey", "PART_OF", "Source", "sourceId")),
    ("study_source", _rel_statement("Study", "studyKey", "PART_OF", "Source", "sourceId")),
    ("study_crop", _rel_statement("Study", "studyKey", "OF_CROP", "Crop", "eppo")),
    ("variety_crop", _rel_statement("Variety", "varietyKey", "OF_CROP", "Crop", "eppo")),
    ("unit_crop", _rel_statement("ObservationUnit", "unitKey", "OF_CROP", "Crop", "eppo")),
    ("unit_variety", _rel_statement("ObservationUnit", "unitKey", "OF_VARIETY", "Variety", "varietyKey")),
    ("unit_study", _rel_statement("ObservationUnit", "unitKey", "IN_STUDY", "Study", "studyKey")),
    ("unit_document", _rel_statement("ObservationUnit", "unitKey", "SOURCED_FROM", "ArticleSource", "documentKey")),
    ("trial_at", _rel_statement("ObservationUnit", "unitKey", "TRIAL_AT", "TrialSite", "siteKey")),
    ("obs_unit", _rel_statement("Observation", "obsKey", "ON_UNIT", "ObservationUnit", "unitKey")),
    ("obs_variable", _rel_statement("Observation", "obsKey", "OF_VARIABLE", "Variable", "variableId")),
)
assert {name for name, _ in _RELATIONSHIPS} == set(_REL_NAMES)


def _chunks(rows: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


async def _run_step(
    session: Any, name: str, statement: str, rows: Sequence[Any], batch_size: int, *, all_must_match: bool = False,
) -> StepReport:
    """One statement over all rows, a transaction per batch; counters are the server's own."""
    nodes = rels = props = batches = 0
    for chunk in _chunks(rows, batch_size):
        counters: dict[str, int] = {}

        async def work(tx: Any, chunk: Sequence[Any] = chunk, counters: dict[str, int] = counters) -> int:
            result = await tx.run(statement, rows=list(chunk))
            matched = 0
            if all_must_match:
                record = await result.single()
                matched = int(record["matched"]) if record is not None else 0
            summary = await result.consume()
            c = summary.counters
            counters.update(nodes=c.nodes_created, rels=c.relationships_created, props=c.properties_set)
            return matched

        matched = await session.execute_write(work)
        if all_must_match and matched != len(chunk):
            raise LoadError(f"{name}: {len(chunk) - matched} of {len(chunk)} rows found no endpoint in the graph")
        nodes += counters["nodes"]
        rels += counters["rels"]
        props += counters["props"]
        batches += 1
    logger.info("kg load step=%s rows=%d batches=%d nodes_created=%d relationships_created=%d",
                name, len(rows), batches, nodes, rels)
    return StepReport(name, len(rows), batches, nodes, rels, props)


async def load(
    bundle: Bundle,
    driver: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
    *,
    registries: Registries | None = None,
    database: str | None = None,
) -> LoadReport:
    """MERGE one source's bundle into the graph behind ``driver`` (an async Neo4j driver). Idempotent.

    ``registries`` default to the repository's; they must be the ones the bundle was built with.
    The caller (the build orchestrator) has already run the quality gate: the loader checks that the
    bundle is *closed*, not that it is *good*.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    regs = registries if registries is not None else load_registries()
    plan = _plan(bundle, regs)

    steps: list[StepReport] = []
    async with driver.session(database=database) as session:
        steps.append(await _run_step(session, "source", _SOURCE, [plan.source], batch_size))
        steps.append(await _run_step(session, "crop", _CROP, plan.crops, batch_size))
        steps.append(await _run_step(session, "variable", _VARIABLE, plan.variables, batch_size))
        steps.append(await _run_step(session, "site", _SITE, plan.sites, batch_size))
        steps.append(await _run_step(session, "article_source", _DOCUMENT, plan.documents, batch_size))
        steps.append(await _run_step(session, "study", _STUDY, plan.studies, batch_size))
        steps.append(await _run_step(session, "variety", _VARIETY, plan.varieties, batch_size))
        for label in sorted(plan.units_by_label, key=lambda name: name or ""):
            steps.append(await _run_step(
                session, f"unit:{label or 'ObservationUnit'}", _unit_statement(label),
                plan.units_by_label[label], batch_size))
        steps.append(await _run_step(session, "observation", _OBSERVATION, plan.observations, batch_size))
        for name, statement in _RELATIONSHIPS:
            steps.append(await _run_step(
                session, f"rel:{name}", statement, plan.rels[name], batch_size, all_must_match=True))
    return LoadReport(bundle.source_id, regs.registries_hash, tuple(steps))


# ═════════════════════════════════════════════════════════════════════════════
# reference knowledge
# ═════════════════════════════════════════════════════════════════════════════

_STAMP_PHENOLOGY_SPECIES = """
MATCH (s:Species)-[:HAS_STAGE]->(st:PhenologyStage)
WHERE s.name IS NOT NULL AND st.speciesName IS NULL
WITH st, collect(DISTINCT s.name) AS names
WHERE size(names) = 1
SET st.speciesName = names[0]
RETURN count(st) AS stamped
"""


async def stamp_phenology_species_name(driver: Any, *, database: str | None = None) -> int:
    """Give every ``PhenologyStage`` its ``speciesName`` (the property ``UNIQUE(speciesName, name)`` needs).

    Production stages carry their species only through ``(Species)-[:HAS_STAGE]->``, so that
    constraint is inert until the property exists. Not part of :func:`load` (a source bundle holds no
    reference knowledge); the build orchestrator runs it after the reference seeds. A stage shared by
    several species cannot take one name and is left alone. Idempotent; returns the stages stamped.
    """
    async with driver.session(database=database) as session:
        async def work(tx: Any) -> int:
            result = await tx.run(_STAMP_PHENOLOGY_SPECIES)
            record = await result.single()
            await result.consume()
            return int(record["stamped"]) if record is not None else 0

        return int(await session.execute_write(work))


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DENORMALIZED_COPIES",
    "LEGACY_JSON_GROUPS",
    "LoadError",
    "LoadReport",
    "StepReport",
    "load",
    "stamp_phenology_species_name",
    "unit_properties",
]
