"""Fungi & mycorrhiza multi-source ingester.

The vision_2024_fungi bundle aggregates 5 distinct sources (Redalyc, Wageningen,
Nature, Hungary Acta Hort, EXCALIBUR H2020). Unlike single-source ingesters,
each trial carries its own real source_id, which we preserve (provenance lives
in the node's source_id — the pipeline never writes SOURCED_FROM). SOURCE_ID is
the dataset umbrella, used only for the registry handle and as a fallback.

Mushroom productivity is NOT kg/ha: Biological Efficiency %, bioactive yield and
colonization live in qualityParams. yieldKgHa/rankingEligible are forced off for
anything that is not a real areal yield, so fungi never pollute the yield advisor.
"""
from __future__ import annotations

import json

from app.ingestion.base_ingester import BaseIngester

AREAL_YIELD_TYPES = {
    "yield_kg_ha", "grain_yield_kg_ha", "dry_yield_kg_ha",
    "fresh_yield_kg_ha", "truffle_yield_kg_ha",
}


def _strip_prefix(value: str | None, prefix: str) -> str | None:
    if value and value.startswith(prefix):
        return value[len(prefix):]
    return value


class FungiIngester(BaseIngester):
    """Multi-source fungi/mycorrhiza ingester (subclass-local overrides only)."""

    SOURCE_ID = "VISION2024"

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        graph = data.get("@graph", [])
        sites, articles, vts, mts = [], [], [], []
        for node in graph:
            t = node.get("@type", "")
            if t == "TrialSite":
                sites.append(self._convert_site(node))
            elif t == "ArticleSource":
                articles.append(self._convert_article(node))
            elif t == "VarietyTrial":
                vts.append(self._convert_trial(node))
            elif t == "ManagementTrial":
                mts.append(self._convert_management(node))
        return {
            "trial_sites": sites,
            "article_sources": articles,
            "variety_trials": vts,
            "management_trials": mts,
        }

    def _convert_site(self, node: dict) -> dict:
        return {
            "name": node.get("name"),
            "municipality": node.get("municipality") or node.get("name"),
            "latitude": node.get("latitude"),
            "longitude": node.get("longitude"),
            "climateClass": node.get("climate_class") or node.get("koppen"),
            "elevationM": node.get("elevation_m"),
            "source_id": self.SOURCE_ID,
        }

    def _convert_article(self, node: dict) -> dict:
        source_id = _strip_prefix(node.get("id"), "source:") or self.SOURCE_ID
        year = node.get("year")
        doi = node.get("doi")
        return {
            "source_id": source_id,
            "issueNumber": node.get("volume") or year,
            "articleTitle": node.get("name"),
            "year": year,
            "document_url": f"https://doi.org/{doi}" if doi else None,
            "confidence": self._confidence_for(source_id),
            "mergeKey": f"{source_id.lower()}|{year or ''}|"
                        f"{str(node.get('name') or '').strip().lower()[:80]}",
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        source_id = node.get("source_id") or self.SOURCE_ID
        ydt = (node.get("yield_data_type") or "").lower()
        is_areal = ydt in AREAL_YIELD_TYPES
        qp = dict(node.get("quality_params") or {})
        qp["yieldDataType"] = node.get("yield_data_type")
        qp["cropCommon"] = node.get("crop_common")
        return {
            "cropEppo": eppo,
            "cropScientific": node.get("crop_scientific"),
            "variety": node.get("variety"),
            "year": node.get("year"),
            "trialLocation": node.get("trial_location"),
            "yieldKgHa": node.get("yield_kg_ha") if is_areal else None,
            "rankingEligible": bool(node.get("ranking_eligible")) if is_areal else False,
            "qualityParams": json.dumps(qp, ensure_ascii=False, sort_keys=True),
            "confidence": self._confidence_for(source_id),
            "source_id": source_id,
            "trial_id": node.get("id", ""),
            "mergeKey": node.get("mergeKey") or (
                f"{source_id}|{eppo or 'unknown'}|"
                f"{str(node.get('variety', '')).strip().lower()}|"
                f"{str(node.get('trial_location', 'unknown')).strip().lower()}|"
                f"{str(node.get('year', 0))}"
            ),
        }

    def _convert_management(self, node: dict) -> dict:
        source_id = node.get("source_id") or self.SOURCE_ID
        qp = dict(node.get("quality_params") or {})
        return {
            "cropScientific": node.get("crop_scientific"),
            "variety": node.get("crop_common"),
            "experimentType": node.get("experiment_type"),
            "treatment": node.get("treatment"),
            "resultMetric": node.get("result_metric"),
            "resultValue": node.get("result_value"),
            "resultUnit": node.get("result_unit"),
            "year": node.get("year"),
            "trialLocation": node.get("trial_location"),
            "qualityParams": json.dumps(qp, ensure_ascii=False, sort_keys=True),
            "confidence": self._confidence_for(source_id),
            "source_id": source_id,
            "mergeKey": node.get("mergeKey") or (
                f"{source_id}|mt|"
                f"{str(node.get('treatment', '')).strip().lower()[:60]}|"
                f"{str(node.get('year', 0))}"
            ),
        }

    async def _merge_management_trials(self, driver, trials: list[dict]) -> int:
        """Base fields + qualityParams (JSON) + host cropScientific."""
        active = [t for t in trials if not t.get("skip_ingestion")]
        if not active:
            return 0
        count = 0
        async with driver.session() as session:
            for node in active:
                merge_key = self._get_merge_key(node)
                if not merge_key:
                    continue
                eppo_raw = node.get("cropEppo") or ""
                await session.run(
                    """
                    MERGE (mt:ManagementTrial {mergeKey: $merge_key})
                    SET mt.source_id = $source,
                        mt.cropEppo = $eppo,
                        mt.cropScientific = $host,
                        mt.variety = $variety,
                        mt.experimentType = $exp_type,
                        mt.treatment = $treatment,
                        mt.resultMetric = $metric,
                        mt.resultValue = $value,
                        mt.resultUnit = $unit,
                        mt.year = $year,
                        mt.trialLocation = $location,
                        mt.trialLocationKey = $location_key,
                        mt.qualityParams = $quality,
                        mt.varietyNormalized = $variety_norm,
                        mt.locationNormalized = $loc_norm,
                        mt.locationCountry = $loc_country,
                        mt.unitQudtUri = $qudt_uri,
                        mt.unitUcum = $ucum,
                        mt.confidence = $confidence,
                        mt.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=node.get("source_id", self.SOURCE_ID),
                    eppo=eppo_raw.replace("eppo:", ""),
                    host=node.get("cropScientific"),
                    variety=node.get("variety"),
                    variety_norm=node.get("varietyNormalized"),
                    exp_type=node.get("experimentType"),
                    treatment=node.get("treatment"),
                    metric=node.get("resultMetric"),
                    value=node.get("resultValue"),
                    unit=node.get("resultUnit"),
                    year=node.get("year"),
                    location=node.get("trialLocation"),
                    location_key=node.get("trialLocationKey") or "",
                    loc_norm=node.get("locationNormalized"),
                    loc_country=node.get("locationCountry"),
                    qudt_uri=node.get("unitQudtUri"),
                    ucum=node.get("unitUcum"),
                    quality=node.get("qualityParams"),
                    confidence=node.get("confidence", "medium"),
                )
                count += 1
        return count

    def _confidence_for(self, source_id: str) -> str:
        from app.common.source_registry import get_source
        try:
            return get_source(source_id).get("confidence_default", "medium")
        except KeyError:
            return "medium"

    async def _merge_relationships(self, driver, variety_trials, management_trials) -> int:
        """Link trials to TrialSites across ALL source_ids present in the batch.

        Base filters by the single class SOURCE_ID (the umbrella) → zero links
        for real-source trials. We link the batch's own source_ids only, so
        other sources' orphans are never affected.
        """
        from app.graph.site_canonicalization import normalize_site_key

        for v in variety_trials:
            if not v.get("trialLocationKey") and v.get("trialLocation"):
                v["trialLocationKey"] = normalize_site_key(v["trialLocation"])
        for m in management_trials:
            if not m.get("trialLocationKey") and m.get("trialLocation"):
                m["trialLocationKey"] = normalize_site_key(m["trialLocation"])

        sids = sorted({n["source_id"] for n in (variety_trials + management_trials)
                       if n.get("source_id") and not n.get("skip_ingestion")})
        if not sids:
            return 0

        count = 0

        async def _link(session, label: str) -> int:
            result = await session.run(
                f"MATCH (n:{label}) WHERE n.source_id IN $sids "
                "MATCH (t:TrialSite) "
                "WHERE t.siteKey = n.trialLocationKey "
                "   OR t.municipalityKey = n.trialLocationKey "
                "   OR (n.locationNormalized IS NOT NULL "
                "       AND t.siteKey = toLower(trim(n.locationNormalized))) "
                "MERGE (n)-[:TRIAL_AT]->(t) "
                "RETURN count(*) AS c",
                sids=sids,
            )
            row = await result.single()
            return row["c"] if row else 0

        async with driver.session() as session:
            if any(not v.get("skip_ingestion") for v in variety_trials):
                count += await _link(session, "VarietyTrial")
            if any(not m.get("skip_ingestion") for m in management_trials):
                count += await _link(session, "ManagementTrial")
        return count

    def _enrich_article_sources(self, articles: list[dict]) -> None:
        """Enrich each article from ITS OWN registered source (not the umbrella)."""
        from app.common.source_registry import get_source
        for art in articles:
            sid = art.get("source_id") or self.SOURCE_ID
            try:
                entry = get_source(sid)
            except KeyError:
                entry = self._registry_entry
            art["source_id"] = sid
            art["institution"] = entry["institution"]
            art["license_class"] = entry["license_class"]
            art["use_type"] = entry["use_type"]
            art["official_status"] = entry["official_status"]
            art["data_format"] = entry.get("data_format", "pdf-extracted")
            art["confidence"] = art.get("confidence") or entry.get("confidence_default", "medium")
