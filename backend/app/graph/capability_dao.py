"""Neo4j async DAO for the Capability Registry: Module/Capability/Attribute upserts + queries."""
from __future__ import annotations
from typing import Iterable
from neo4j import AsyncDriver


class CapabilityDao:
    def __init__(self, driver: AsyncDriver) -> None:
        self._driver = driver

    async def close(self) -> None:
        await self._driver.close()

    async def upsert_module(self, *, module_id: str, version: str) -> None:
        async with self._driver.session() as s:
            await s.run(
                """
                MERGE (m:Module {id: $module_id})
                SET m.version = $version, m.updatedAt = datetime()
                """,
                module_id=module_id, version=version,
            )

    async def upsert_capability(
        self,
        *,
        module_id: str,
        entity_type: str,
        attribute_name: str,
        unit_code: str | None,
        temporal: str,
        spatial: str,
        sources: list[str],
        entitlement: str,
        sdm_status: str,
        sdm_proposal: str | None,
    ) -> None:
        async with self._driver.session() as s:
            await s.run(
                """
                MATCH (m:Module {id: $module_id})
                MERGE (c:Capability {entityType: $entity_type, attributeName: $attribute_name})
                SET c.unitCode = $unit_code,
                    c.temporal = $temporal,
                    c.spatial = $spatial,
                    c.sources = $sources,
                    c.entitlement = $entitlement,
                    c.sdmStatus = $sdm_status,
                    c.sdmProposal = $sdm_proposal,
                    c.updatedAt = datetime()
                MERGE (m)-[:PUBLISHES]->(c)
                """,
                module_id=module_id, entity_type=entity_type, attribute_name=attribute_name,
                unit_code=unit_code, temporal=temporal, spatial=spatial,
                sources=sources, entitlement=entitlement,
                sdm_status=sdm_status, sdm_proposal=sdm_proposal,
            )

    async def list_catalog(self) -> list[dict]:
        async with self._driver.session() as s:
            result = await s.run(
                """
                MATCH (m:Module)-[:PUBLISHES]->(c:Capability)
                RETURN m.id AS moduleId, m.version AS moduleVersion,
                       c.entityType AS entityType, c.attributeName AS attributeName,
                       c.unitCode AS unitCode, c.temporal AS temporal, c.spatial AS spatial,
                       c.sources AS sources, c.entitlement AS entitlement,
                       c.sdmStatus AS sdmStatus, c.sdmProposal AS sdmProposal
                ORDER BY c.entityType, c.attributeName
                """
            )
            records = await result.data()
            return records

    async def get_attribute_detail(self, entity_type: str, attribute_name: str) -> dict | None:
        async with self._driver.session() as s:
            result = await s.run(
                """
                MATCH (m:Module)-[:PUBLISHES]->(c:Capability {entityType:$et, attributeName:$an})
                RETURN m.id AS moduleId, c{.*} AS capability
                """,
                et=entity_type, an=attribute_name,
            )
            record = await result.single()
            return dict(record) if record else None

    async def mark_module_stale(self, module_id: str, current_attrs: Iterable[tuple[str, str]]) -> int:
        """Mark capabilities no longer declared by the module as DEPRECATED. Returns count marked."""
        async with self._driver.session() as s:
            result = await s.run(
                """
                MATCH (m:Module {id:$module_id})-[:PUBLISHES]->(c:Capability)
                WHERE NOT [c.entityType, c.attributeName] IN $current_attrs
                SET c:DEPRECATED, c.deprecatedAt = datetime()
                RETURN count(c) AS n
                """,
                module_id=module_id, current_attrs=[list(t) for t in current_attrs],
            )
            record = await result.single()
            return record["n"]
