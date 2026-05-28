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

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from app.auth import NKZAuthMiddleware
from app.core.config import settings
from app.core.dependencies import close_driver, init_driver

# Module-level readiness state set during lifespan.
# K8s probes hit /healthz and /readyz every 10-30s — must be fast and never rate-limited.
_ikerketa_available = False


async def _create_orion_subscription():
    """Create or update the NGSI-LD subscription for AgriCrop changes."""
    import httpx
    sub = {
        "type": "Subscription",
        "entities": [{"type": "AgriCrop"}],
        "watchedAttributes": ["kcIni", "kcMid", "kcEnd", "hasSubCrop",
                              "phMin", "phMax", "tempMinAbs", "tempMaxAbs",
                              "heatDamageThresholdC", "frostDamageThresholdC"],
        "notification": {
            "endpoint": {
                "uri": "http://bioorchestrator-service:8420/api/ngsi-ld/notify",
                "accept": "application/json",
            }
        }
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{settings.orion_ld_url}/ngsi-ld/v1/subscriptions",
                params={"type": "Subscription"},
                headers={"Accept": "application/ld+json"},
            )
            existing = resp.json() if resp.status_code == 200 else []
            if not existing:
                await client.post(
                    f"{settings.orion_ld_url}/ngsi-ld/v1/subscriptions",
                    json=sub,
                    headers={"Content-Type": "application/ld+json"},
                )
    except Exception:
        pass  # Non-critical — sync works without subscription


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
    except Exception as exc:
        print(f"[bioorchestrator] WARNING: Neo4j unavailable on startup: {exc}")

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# NKZ auth middleware (validates JWT from platform)
app.add_middleware(NKZAuthMiddleware)

# Graph API router
from app.api import router as api_router  # noqa: E402
app.include_router(api_router, prefix="/api")

# Catalog and NGSI-LD notify routers
from app.api.v1.catalog import router as catalog_router  # noqa: E402
from app.api.v1.notify import router as notify_router  # noqa: E402

app.include_router(catalog_router, prefix="/api/crop")
app.include_router(notify_router, prefix="/api/ngsi-ld")

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
