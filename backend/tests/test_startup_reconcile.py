from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.main import _reconcile_catalog


@pytest.mark.asyncio
async def test_reconcile_syncs_all_from_catalog_tenant():
    fake_client = AsyncMock()
    with patch("app.main.OrionClient", return_value=fake_client) as ctor, \
         patch("app.main.get_driver", return_value=MagicMock()), \
         patch("app.main.sync_all_agri_crops", new=AsyncMock(return_value=45)) as sync:
        n = await _reconcile_catalog()
    assert n == 45
    assert ctor.call_args.args[0] == "default"
    sync.assert_awaited_once()
    fake_client.close.assert_awaited_once()
