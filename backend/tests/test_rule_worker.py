import pytest
from unittest.mock import AsyncMock, patch
from app.workers import rule_worker

@pytest.mark.asyncio
async def test_handler_calls_evaluate_with_orion_for_tenant():
    with patch.object(rule_worker, "OrionClient") as MockOrion, \
         patch.object(rule_worker, "get_driver", return_value=AsyncMock()), \
         patch.object(rule_worker, "evaluate", new=AsyncMock(return_value=[])) as mock_eval:
        MockOrion.return_value.close = AsyncMock()
        await rule_worker.handle_evaluate_action_rules(
            "montiko", "urn:ngsi-ld:AgriParcel:montiko:p1", {"phenology.current_stage": "flowering"})
        MockOrion.assert_called_once_with("montiko")
        mock_eval.assert_awaited_once()
        MockOrion.return_value.close.assert_awaited_once()
