"""NKZ BioOrchestrator Backend — FastAPI wrapper for IkerKeta pipeline.

Mounts the IkerKeta status API and adds NKZ-specific endpoints
for authentication, pipeline execution, and tenant isolation.

Run standalone:
    uvicorn app.main:app --port 8420

In production (K8s):
    Deployed as part of nekazari-module-bioorchestrator pod.
"""

from __future__ import annotations

import asyncio
import json as _json
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from app.auth import NKZAuthMiddleware
from app.core.config import settings
from app.core.dependencies import close_driver, init_driver

# Module-level readiness state set during lifespan.
# K8s probes hit /healthz and /readyz every 10-30s — must be fast and never rate-limited.
_ikerketa_available = False


async def _create_orion_subscription():
    """Create the NGSI-LD subscription for AgriCrop changes (idempotent).

    Sends as application/json + Link header (NGSI-LD fragment pattern).
    Subscription body carries no @context, so ld+json would cause Orion 400.
    """
    ctx = settings.context_url
    link = f'<{ctx}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    endpoint_uri = "http://bioorchestrator-service:8420/api/ngsi-ld/notify"
    sub = {
        "type": "Subscription",
        "entities": [{"type": "AgriCrop"}],
        "watchedAttributes": ["kcIni", "kcMid", "kcEnd", "hasSubCrop",
                              "phMin", "phMax", "tempMinAbs", "tempMaxAbs",
                              "heatDamageThresholdC", "frostDamageThresholdC"],
        "notification": {
            "endpoint": {
                "uri": endpoint_uri,
                "accept": "application/json",
            }
        },
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{settings.orion_ld_url}/ngsi-ld/v1/subscriptions",
                params={"limit": 1000},
                headers={"Accept": "application/json", "Link": link},
            )
            existing = resp.json() if resp.status_code == 200 else []
            ours = any(
                isinstance(s, dict)
                and s.get("notification", {}).get("endpoint", {}).get("uri") == endpoint_uri
                for s in existing
            )
            if not ours:
                await client.post(
                    f"{settings.orion_ld_url}/ngsi-ld/v1/subscriptions",
                    json=sub,
                    headers={"Content-Type": "application/json", "Link": link},
                )
    except Exception as exc:
        print(f"[bioorchestrator] WARNING: subscription setup failed: {exc}")


async def _run_cypher_migrations(driver):
    """Execute pending Cypher migrations on startup (idempotent).

    Reads all .cypher files from cypher_migrations/ in order.
    Each statement uses IF NOT EXISTS or equivalent — safe to re-run.
    """
    from pathlib import Path
    migrations_dir = Path(__file__).parent.parent / "cypher_migrations"
    if not migrations_dir.exists():
        print("[bioorchestrator] No cypher_migrations directory — skipping")
        return 0

    cypher_files = sorted(migrations_dir.glob("*.cypher"))
    if not cypher_files:
        return 0

    executed = 0
    async with driver.session() as session:
        for cypher_file in cypher_files:
            content = cypher_file.read_text()
            statements = [s.strip() for s in content.split(";") if s.strip() and not s.strip().startswith("//")]
            for stmt in statements:
                # Only execute CREATE CONSTRAINT/INDEX statements (idempotent)
                if not stmt.upper().startswith(("CREATE CONSTRAINT", "CREATE INDEX")):
                    continue
                try:
                    await session.run(stmt)
                    executed += 1
                except Exception as exc:
                    # Constraint already exists → ok
                    if "already exists" in str(exc) or "AlreadyExists" in str(exc) or "equivalent" in str(exc):
                        pass
                    else:
                        print(f"[bioorchestrator] WARNING: migration failed: {exc}")
    return executed


async def _start_background_tasks():
    """Initialize background workers after uvicorn has bound its socket."""
    await asyncio.sleep(2)  # Give uvicorn a moment to complete startup
    try:
        from app.workers.queue import background_queue
        from app.workers.sync_worker import handle_sync_agri_crop
        background_queue.register("sync_agri_crop", handle_sync_agri_crop)
        asyncio.create_task(background_queue.run_loop())
        asyncio.create_task(_create_orion_subscription())
        print("[bioorchestrator] background tasks started")
    except Exception as exc:
        print(f"[bioorchestrator] WARNING: background tasks init failed: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle: Neo4j connection + IkerKeta availability."""
    global _ikerketa_available

    try:
        import ikerketa
        version = ikerketa.__version__ if hasattr(ikerketa, "__version__") else "0.1.0"
        print(f"[bioorchestrator] IkerKeta {version} loaded")
        _ikerketa_available = True
    except ImportError as e:
        print(f"[bioorchestrator] WARNING: ikerketa not installed: {e}")
        _ikerketa_available = False

    try:
        await init_driver()
        print("[bioorchestrator] Neo4j connected")
        # Auto-run Cypher migrations (idempotent, safe to re-run)
        from app.core.dependencies import get_neo4j_driver
        driver = await anext(get_neo4j_driver())
        migrated = await _run_cypher_migrations(driver)
        if migrated:
            print(f"[bioorchestrator] {migrated} Cypher constraints/indexes ensured")
    except Exception as exc:
        print(f"[bioorchestrator] WARNING: Neo4j unavailable on startup: {exc}")

    # Seed external capability registrations (best-effort)
    try:
        from app.graph.capability_dao import CapabilityDao
        from app.services.capability_loader import seed_external_capabilities
        seeded = await seed_external_capabilities(CapabilityDao())
        print(f"[bioorchestrator] Seeded {seeded} external capabilities")
    except Exception as exc:
        print(f"[bioorchestrator] WARNING: capability seed failed: {exc}")

    # Schedule background tasks after uvicorn binds (don't block startup)
    loop = asyncio.get_running_loop()
    loop.call_soon(lambda: asyncio.ensure_future(_start_background_tasks()))

    yield

    await close_driver()
    print("[bioorchestrator] Neo4j connection closed")


app = FastAPI(
    title="NKZ BioOrchestrator",
    description="Multi-domain biodiversity ETL pipeline for Nekazari platform",
    version="0.1.0",
    lifespan=lifespan,
)

# NKZ auth middleware (validates JWT from platform).
# MUST be added BEFORE CORSMiddleware so CORS headers wrap error responses.
app.add_middleware(NKZAuthMiddleware)

# CORS — outermost middleware: adds headers to ALL responses including
# 401/403/500 from auth and other inner middleware.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Graph API router
from app.api import router as api_router  # noqa: E402
app.include_router(api_router, prefix="/api")

# Catalog, NGSI-LD notify, and parcel data routers
from app.api.v1.catalog import router as catalog_router  # noqa: E402
from app.api.v1.notify import router as notify_router  # noqa: E402
from app.api.v1.parcel_data import router as parcel_data_router  # noqa: E402

app.include_router(catalog_router, prefix="/api/crop")
app.include_router(notify_router, prefix="/api/ngsi-ld")
app.include_router(parcel_data_router, prefix="/api")

# Register IkerKeta API routes directly on the main app
# (don't use app.mount() — it double-prefixes and / mount kills healthz)
try:
    from ikerketa.api import app as ikerketa_api
    for route in ikerketa_api.routes:
        if hasattr(route, 'path') and hasattr(route, 'endpoint') and hasattr(route, 'methods'):
            app.add_api_route(
                path=route.path,
                endpoint=route.endpoint,
                methods=list(route.methods) if route.methods else ['GET'],
                include_in_schema=False,
            )
    print(f"[bioorchestrator] IkerKeta routes registered on main app")
except ImportError:
    print("[bioorchestrator] ikerketa.api not available — running without data endpoints")


async def _store_pipeline_history(
    success: bool,
    entities: int,
    relationships: int,
    duration_seconds: float,
    sources: list[str] | None,
    errors: int,
) -> None:
    """Store a pipeline run summary in Redis history stream (best-effort)."""
    try:
        import redis.asyncio as aioredis
        r = aioredis.Redis.from_url("redis://redis-service:6379/0", socket_connect_timeout=3)
        entry = {
            "run_id": datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"),
            "success": success,
            "entities": entities,
            "relationships": relationships,
            "duration_seconds": round(duration_seconds, 2),
            "sources": sources or ["all"],
            "errors": errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await r.xadd("pipeline:history", {"payload": _json.dumps(entry)}, maxlen=50)
        await r.aclose()
    except Exception:
        pass


@app.get("/healthz")
async def healthz():
    """K8s liveness probe — always returns 200 if the process is alive.

    This endpoint is excluded from auth and must never be rate-limited.
    """
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    """K8s readiness probe — returns 200 when dependencies are available.

    Checks cached IkerKeta import state (set during lifespan).
    Must be fast (no imports, no I/O) — K8s probes every 10s.
    """
    if _ikerketa_available:
        return {"status": "ready"}
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "reason": "ikerketa not available"},
    )


@app.post("/api/pipeline/run")
async def run_pipeline_endpoint(request: Request):
    """Trigger a pipeline run via the NKZ frontend.

    Body:
        sources: list[str] | null  — connectors to run
        limit: int | null          — max records per connector
    """
    from ikerketa.pipeline import run_pipeline
    from ikerketa.report import generate_report

    body = await request.json()
    sources = body.get("sources")
    limit = body.get("limit", 50)

    result = run_pipeline(sources=sources, limit=limit, export=True)
    report = generate_report(result)

    response = {
        "success": result.failure_count == 0,
        "entities_before_dedup": result.entities_before_dedup,
        "entities_after_dedup": result.entities_after_dedup,
        "relationships": result.relationships_total,
        "crossref_matches": result.crossref_matches,
        "duration_seconds": round(result.total_duration_seconds, 2),
        "errors": result.errors,
        "report": report,
    }

    # Store in history (best-effort, non-blocking)
    await _store_pipeline_history(
        success=result.failure_count == 0,
        entities=result.entities_after_dedup,
        relationships=result.relationships_total,
        duration_seconds=result.total_duration_seconds,
        sources=sources,
        errors=len(result.errors),
    )

    return response


@app.get("/api/pipeline/progress")
async def pipeline_progress(request: Request, run_id: str = ""):
    """SSE endpoint for pipeline progress events.

    Streams progress events from the pipeline:progress Redis stream.
    Each event contains: run_id, step, total, connector, status, timestamp.
    """
    import redis.asyncio as aioredis

    async def event_stream():
        r = aioredis.Redis.from_url("redis://redis-service:6379/0", socket_connect_timeout=3)
        last_id = "0"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    events = await r.xread(
                        {"pipeline:progress": last_id}, count=10, block=2000
                    )
                except Exception:
                    break
                if events:
                    for _stream_name, messages in events:
                        for msg_id, data in messages:
                            last_id = msg_id
                            payload = _json.loads(data.get(b"payload", data.get("payload", "{}")))
                            if not run_id or payload.get("run_id") == run_id:
                                yield f"data: {_json.dumps(payload)}\n\n"
                else:
                    yield ": heartbeat\n\n"
        except Exception:
            pass
        finally:
            await r.aclose()

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/pipeline/history")
async def pipeline_history(limit: int = Query(default=10, ge=1, le=50)):
    """Return recent pipeline execution history from Redis."""
    try:
        import redis.asyncio as aioredis
        r = aioredis.Redis.from_url("redis://redis-service:6379/0", socket_connect_timeout=3)
        events = await r.xrevrange("pipeline:history", "+", "-", count=limit)
        await r.aclose()
        history = []
        for _msg_id, data in events:
            payload = _json.loads(data.get(b"payload", data.get("payload", "{}")))
            history.append(payload)
        return {"history": history}
    except Exception:
        return {"history": []}


# ═══════════════════════════════════════════════════════════════════════════════
# NGSI-LD @context endpoint — serves the BioOrchestrator JSON-LD context
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/ngsi-ld/bioorchestrator-context.jsonld")
async def serve_context():
    """Serve the BioOrchestrator JSON-LD @context.

    This endpoint is required by n10s for RDF import and by any linked data
    consumer that dereferences the @context URL found in JSON-LD documents.

    Returns the context as application/ld+json.
    """
    import json as _json
    from pathlib import Path
    ctx_path = Path(__file__).parent / "graph" / "bioorchestrator-context.jsonld"
    if not ctx_path.exists():
        return JSONResponse(
            status_code=404,
            content={"error": "Context file not found"},
        )
    return JSONResponse(
        content=_json.loads(ctx_path.read_text()),
        media_type="application/ld+json",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Navarra Agraria ingestion trigger — one-shot CLI endpoint
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/ingestion/navarra-agraria")
async def ingest_navarra_agraria(
    request: Request,
    jsonld_path: str = Query(
        default="/data/all_trials_enriched.jsonld",
        description="Path to the JSON-LD file inside the container",
    ),
    dry_run: bool = Query(default=False, description="Validate without writing"),
):
    """Ingest Navarra Agraria trial data into the Neo4j knowledge graph.

    This is a one-shot operation (idempotent via MERGE).
    The JSON-LD file must be accessible inside the bioorchestrator pod
    (e.g., mounted via ConfigMap or copied via kubectl cp).

    Returns per-type counts of merged nodes and relationships.
    """
    try:
        from app.ingestion.navarra_ingester import NavarraIngester
        from app.core.dependencies import get_neo4j_driver

        driver = await anext(get_neo4j_driver())
        ingester = NavarraIngester(driver)
        stats = await ingester.ingest(jsonld_path, dry_run=dry_run)
        return {"status": "ok", "stats": stats}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
