"""Fetch capabilities.yaml from a module URL and upsert into the Capability Registry."""
from __future__ import annotations

import httpx
import yaml

from app.graph.capability_dao import CapabilityDao


class CapabilityLoader:
    def __init__(self, dao: CapabilityDao) -> None:
        self._dao = dao

    async def load_from_url(self, url: str) -> int:
        """Fetch YAML, parse, upsert; return total capabilities upserted."""
        resp = httpx.get(url, timeout=10)
        resp.raise_for_status()
        manifest = yaml.safe_load(resp.text)
        return await self.load_from_dict(manifest)

    async def load_from_dict(self, manifest: dict) -> int:
        """Parse manifest dict, upsert module + capabilities; return count upserted."""
        module_id = manifest["moduleId"]
        version = manifest["version"]
        await self._dao.upsert_module(module_id=module_id, version=version)

        n = 0
        current: list[tuple[str, str]] = []
        for entity in manifest.get("publishes", []):
            entity_type = entity["entityType"]
            sdm_status = entity.get("sdmStatus", "unknown")
            sdm_proposal = entity.get("sdmProposal")
            for attr in entity.get("attributes", []):
                await self._dao.upsert_capability(
                    module_id=module_id,
                    entity_type=entity_type,
                    attribute_name=attr["name"],
                    unit_code=attr.get("unitCode"),
                    temporal=attr["temporal"],
                    spatial=attr["spatial"],
                    sources=attr.get("sources", []),
                    entitlement=attr.get("entitlement", "open"),
                    sdm_status=sdm_status,
                    sdm_proposal=sdm_proposal,
                )
                current.append((entity_type, attr["name"]))
                n += 1

        await self._dao.mark_module_stale(module_id, current)
        return n
