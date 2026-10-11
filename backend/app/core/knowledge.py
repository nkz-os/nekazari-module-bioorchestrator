"""Graphs a request may read ("knowledge sources").

v1 serves one graph, the global one. Planning code takes its graphs from
``get_knowledge_sources`` instead of the module driver, so adding a tenant's own
graph (enterprise tier, a later phase) changes this function only. Every evidence
item the planning endpoints return names the ``graph_id`` it came from.
"""
from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request

from app.core.dependencies import get_driver
from neo4j import AsyncDriver

GLOBAL_GRAPH_ID = "global"


@dataclass(frozen=True)
class KnowledgeSource:
    graph_id: str
    driver: AsyncDriver


async def get_knowledge_sources(request: Request) -> list[KnowledgeSource]:
    """Knowledge sources of this request, global first. v1: the global graph only."""
    return [KnowledgeSource(graph_id=GLOBAL_GRAPH_ID, driver=get_driver())]
