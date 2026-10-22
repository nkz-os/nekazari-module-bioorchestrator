from unittest.mock import MagicMock

from app.core import knowledge
from neo4j import AsyncDriver


async def test_v1_serves_only_the_global_graph(monkeypatch):
    drv = MagicMock(spec=AsyncDriver)
    monkeypatch.setattr(knowledge, "get_driver", lambda: drv)
    sources = await knowledge.get_knowledge_sources(MagicMock())
    assert [(s.graph_id, s.driver) for s in sources] == [("global", drv)]
