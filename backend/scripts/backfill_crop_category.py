"""One-shot backfill: write AgriCrop.category (crop_group) from species EPPO.

Idempotent: only patches entities that have `species` (EPPO code) and are
missing `category`. Read-only otherwise.

Usage (inside the backend container or a pod with DB/Orion access):
    TENANTS=tenant-a,tenant-b python3 -m scripts.backfill_crop_category
"""
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nkz_platform_sdk.orion import OrionClient

from app.core.config import settings
from app.species_registry import get_crop_group, resolve_species


async def backfill_tenant(tenant_id: str) -> int:
    """Patch missing AgriCrop.category for one tenant. Returns patched count."""
    patched = 0
    orion = OrionClient(
        tenant_id,
        base_url=settings.orion_ld_url,
        context_url=settings.context_url,
    )
    try:
        entities = await orion.query_entities(type="AgriCrop", options="keyValues")
    except Exception as exc:  # noqa: BLE001
        print(f"[{tenant_id}] query failed: {exc}")
        return 0

    for entity in entities:
        species = entity.get("species")
        if not species or entity.get("category"):
            continue
        slug = resolve_species(str(species))
        group = get_crop_group(slug) if slug else None
        if not group:
            continue
        try:
            # POST /attrs (append): añade el atributo nuevo `category`. PATCH /attrs
            # solo actualiza atributos existentes (y Orion devuelve 404 si no hay).
            await orion.append_entity_attrs(
                entity["id"],
                {"category": {"type": "Property", "value": group}},
            )
            patched += 1
            print(f"[{tenant_id}] {entity['id']} -> category={group}")
        except Exception as exc:  # noqa: BLE001
            print(f"[{tenant_id}] patch failed for {entity.get('id')}: {exc}")
    await orion.close()
    return patched


async def main() -> None:
    tenants = [t.strip() for t in os.getenv("TENANTS", "").split(",") if t.strip()]
    if not tenants:
        print("ERROR: set TENANTS=tenant-a,tenant-b ... (no tenants given).")
        sys.exit(1)

    dry_run = os.getenv("DRY_RUN", "0") == "1"
    total = 0
    for tenant in tenants:
        if dry_run:
            print(f"[{tenant}] DRY_RUN (no writes)")
            continue
        total += await backfill_tenant(tenant)
    print(f"done: patched {total} AgriCrop entities across {len(tenants)} tenants")


if __name__ == "__main__":
    asyncio.run(main())
