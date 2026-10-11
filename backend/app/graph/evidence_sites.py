"""Trial sites behind a recommendation (shared by the evidence endpoint and planning)."""
from __future__ import annotations


async def evidence_sites(dao, conditions: dict, *, similarity: str, tier: str, country: str | None) -> list[dict]:
    """Same site choice as the recommendation that produced ``similarity`` and ``tier``.

    vector_v2_fallback: the v2 agroclimatic neighbours (needs the four numeric climate inputs).
    regional: every site of the climate, aggregate pseudo-sites included, scoped by country.
    otherwise: every matching field site of the Köppen class.
    The evidence endpoint rejects tier=regional with a non-Köppen similarity, so the first two
    never both apply; when they would, the v2 lookup wins, as it always did there.
    """
    if similarity == "vector_v2_fallback":
        return await dao.get_similar_sites(
            climate_class=conditions["climate_class"], soil_type=conditions.get("soil_type"),
            rainfall_min=None, rainfall_max=None, limit=50,
            target_features={"rainfall": conditions.get("annual_rainfall_mm"), "et0": conditions.get("annual_et0_mm"),
                             "coldest_min": conditions.get("coldest_month_min_c"),
                             "annual_temp": conditions.get("annual_temp_c")},
            vector_version="v2",
        )
    if tier == "regional":
        return await dao.get_similar_sites(climate_class=conditions["climate_class"], soil_type=None, limit=None,
                                           include_aggregate=True, country=country)
    return await dao.get_similar_sites(climate_class=conditions["climate_class"],
                                       soil_type=conditions.get("soil_type"), limit=None)
