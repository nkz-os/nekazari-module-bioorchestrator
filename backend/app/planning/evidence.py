"""Trials behind one crop at one parcel: the same evidence the recommender would use."""
from __future__ import annotations

from app.graph import zone_match
from app.graph.dao import _irrigation_uri
from app.graph.evidence_sites import evidence_sites
from app.graph.parcel_conditions import conditions_from_environment

EVIDENCE_ROWS_MAX = 5000


async def load_crop_evidence(dao, env: dict, *, crop_eppo: str, regime: str, management: str) -> dict:
    conditions = conditions_from_environment(env, irrigation_regime=regime, management=management,
                                             crops=[crop_eppo], top_n=1)
    if not conditions["climate_class"]:
        return {"status": "needs_climate"}
    result = await dao.recommend_for_conditions(conditions)
    recs = (result.get("recommendations") or []) if result.get("status") == "ok" else []
    if not recs:
        return {"status": "no_trials"}
    rec = recs[0]
    evidence, trust = rec.get("evidence") or {}, rec.get("trust") or {}
    tier = evidence.get("tier") or "field"
    similarity = "koppen" if tier == "regional" else (trust.get("similarity") or "koppen")
    country = (result.get("conditions") or {}).get("country")
    zm = evidence.get("zone_match")
    zone = (zm or {}).get("zone_id")
    sites = await evidence_sites(dao, conditions, similarity=similarity, tier=tier, country=country)
    page = await dao.list_trial_evidence(
        crop=rec["crop"]["eppo"], similar_sites=[s["name"] for s in sites], variety=None,
        irrigation_uri=_irrigation_uri(regime), management=management, page=1, page_size=EVIDENCE_ROWS_MAX,
        purpose="main", tier=tier,
        zone_ctx=(zone_match.context_from_zone_id(zone, regime)
                  if tier == "regional" and zone and zone_match.is_zone_country(country) else None),
    )
    return {"status": "ok", "tier": tier, "zone_match": zm, "country": country, "rows": page.get("items") or [],
            "data_gaps": list(trust.get("data_gaps") or []), "crop_eppo": rec["crop"]["eppo"],
            "policy_version": result.get("evidence_policy")}
