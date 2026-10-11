from unittest.mock import AsyncMock

from app.planning.evidence import load_crop_evidence

ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": "Csa", "country": "ES",
       "centroid": {"lat": None, "lon": None}, "soil": {"data_available": False}, "irrigation": {}, "climate_detail": {}}


def _dao(total, rows):
    dao = AsyncMock()
    dao.recommend_for_conditions.return_value = {
        "status": "ok", "conditions": {"country": "ES"}, "evidence_policy": "p",
        "recommendations": [{"crop": {"eppo": "TRZAX"}, "evidence": {"tier": "field"},
                             "trust": {"similarity": "koppen", "data_gaps": ["low_trial_count"]}}]}
    dao.get_similar_sites.return_value = [{"name": "Zona X"}]
    dao.list_trial_evidence.return_value = {"items": rows, "total": total}
    return dao


ROWS = [{"variety": "A", "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": 6000},
        {"variety": "B", "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": 5000}]


async def test_a_truncated_page_is_flagged():
    ev = await load_crop_evidence(_dao(6000, ROWS), ENV, crop_eppo="TRZAX", regime="secano", management="conventional")
    assert ev["status"] == "ok" and len(ev["rows"]) == 2
    assert ev["data_gaps"] == ["low_trial_count", "evidence_truncated"]


async def test_a_complete_page_is_not_flagged():
    ev = await load_crop_evidence(_dao(2, ROWS), ENV, crop_eppo="TRZAX", regime="secano", management="conventional")
    assert ev["data_gaps"] == ["low_trial_count"]
