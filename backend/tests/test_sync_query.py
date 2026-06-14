from unittest.mock import AsyncMock

import pytest

from app.ingestion.sync import sync_all_agri_crops


@pytest.mark.asyncio
async def test_sync_all_uses_query_entities_and_merges():
    orion = AsyncMock()
    orion.query_entities.return_value = [
        {"id": "urn:ngsi-ld:AgriCrop:wheat", "type": "AgriCrop"},
        {"id": "urn:ngsi-ld:AgriCrop:olive", "type": "AgriCrop"},
    ]
    dao = AsyncMock()
    count = await sync_all_agri_crops(dao, orion)
    assert count == 2
    orion.query_entities.assert_awaited_once_with(type="AgriCrop", limit=1000)
    assert dao.merge_agri_crop.await_count == 2
