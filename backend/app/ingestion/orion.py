"""NGSI-LD Orion-LD client for ingestion operations."""
import httpx
from app.core.config import settings


class OrionIngestionClient:
    """Thin wrapper around Orion-LD for AgriCrop upsert operations."""

    def __init__(self, tenant_id: str = ""):
        self.base = settings.orion_ld_url
        self.tenant_id = tenant_id
        self.ctx = settings.context_url
        self.headers = {
            "Content-Type": "application/ld+json",
            "Accept": "application/ld+json",
        }
        if tenant_id:
            self.headers["NGSILD-Tenant"] = tenant_id

    @property
    def _get_headers(self) -> dict[str, str]:
        """Headers for GET requests — include Link for @context type resolution."""
        h = {
            "Accept": "application/ld+json",
            "Link": (
                f'<{self.ctx}>; '
                'rel="http://www.w3.org/ns/json-ld#context"; '
                'type="application/ld+json"'
            ),
        }
        if self.tenant_id:
            h["NGSILD-Tenant"] = self.tenant_id
        return h

    async def upsert_entity(self, entity: dict) -> dict:
        """Create or update a single NGSI-LD entity via upsert."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.base}/ngsi-ld/v1/entityOperations/upsert",
                json=[entity],
                headers=self.headers,
                params={"options": "update"},
            )
            resp.raise_for_status()
            return resp.json()

    async def upsert_batch(self, entities: list[dict]) -> dict:
        """Batch upsert (up to Orion-LD's max payload)."""
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"{self.base}/ngsi-ld/v1/entityOperations/upsert",
                json=entities,
                headers=self.headers,
                params={"options": "update"},
            )
            resp.raise_for_status()
            return resp.json()

    async def list_by_type(self, entity_type: str, limit: int = 5000) -> list[dict]:
        """List all entities of a given type."""
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(
                f"{self.base}/ngsi-ld/v1/entities",
                params={"type": entity_type, "limit": limit},
                headers=self._get_headers,
            )
            resp.raise_for_status()
            return resp.json()

    async def query_by_relationship(
        self, entity_type: str, rel_name: str, target_id: str, limit: int = 1
    ) -> list[dict]:
        """Query entities by relationship target (e.g. hasAgriParcel=={parcel_id})."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{self.base}/ngsi-ld/v1/entities",
                params={
                    "type": entity_type,
                    "q": f'{rel_name}=="{target_id}"',
                    "limit": limit,
                },
                headers=self._get_headers,
            )
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else [data]

    async def get_entity(self, entity_id: str) -> dict | None:
        """Get a single NGSI-LD entity by id."""
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{self.base}/ngsi-ld/v1/entities/{entity_id}",
                headers=self._get_headers,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()

    async def patch_entity(self, entity_id: str, attributes: dict) -> None:
        """Patch specific attributes on an existing entity."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.patch(
                f"{self.base}/ngsi-ld/v1/entities/{entity_id}/attrs",
                json=attributes,
                headers=self.headers,
            )
            resp.raise_for_status()

    def build_entity(self, uri: str, name: str, scientific_name: str,
                     provider: str, extra_attrs: dict | None = None) -> dict:
        """Build a minimal AgriCrop NGSI-LD entity."""
        entity = {
            "id": uri,
            "type": "AgriCrop",
            "@context": [self.ctx],
            "name": {"type": "Property", "value": name},
            "scientificName": {"type": "Property", "value": scientific_name},
            "dataProvider": {"type": "Property", "value": provider},
        }
        if extra_attrs:
            entity.update(extra_attrs)
        return entity
