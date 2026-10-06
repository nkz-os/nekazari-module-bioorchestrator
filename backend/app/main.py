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
import contextlib
import json as _json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from neo4j.exceptions import DriverError, Neo4jError
from nkz_platform_sdk.orion import OrionClient
from nkz_platform_sdk.subscriptions import SubscriptionRegistrar

from app.auth import NKZAuthMiddleware
from app.core.config import settings
from app.core.dependencies import (
    close_driver,
    get_driver,
    init_driver,
    require_platform_admin,
)
from app.graph.dao import GraphDAO
from app.ingestion.sync import sync_all_agri_crops
from app.kg.migrations import MigrationError, apply_migrations
from app.logging_setup import configure_logging

# Module-level readiness state set during lifespan.
# K8s probes hit /healthz and /readyz every 10-30s — must be fast and never rate-limited.
_ikerketa_available = False

# Set only by a deterministic schema failure of the startup migration (the data or the schema
# contradicts a constraint). /readyz then answers 503 (a rolling update keeps the previous pod);
# liveness is unaffected. Cleared as soon as a retry succeeds. Transient or connectivity errors
# never set it.
_migration_failure: dict | None = None
_migration_task: asyncio.Task | None = None

# Retry pacing after a failed startup attempt: capped exponential backoff for transient errors,
# a slow fixed period once a schema failure is latched (so readiness recovers after the data is fixed).
_MIGRATION_TRANSIENT_BACKOFF_START_S = 2.0
_MIGRATION_TRANSIENT_BACKOFF_CAP_S = 60.0
_MIGRATION_SCHEMA_RETRY_S = 300.0
# Cap on one attempt, so a server that accepts the connection but never answers cannot hold
# startup past the liveness probe; a timeout is transient and goes to the retry task.
_MIGRATION_ATTEMPT_TIMEOUT_S = 45.0

# Strong refs to long-lived background tasks (prevents GC of run_loop et al.).
_BG_TASKS: set = set()

logger = logging.getLogger(__name__)


async def _ensure_catalog_subscription():
    """Ensure the AgriCrop subscription exists in the canonical catalog tenant.

    Uses SubscriptionRegistrar (idempotent by description). The catalog lives in
    tenant `settings.catalog_tenant` ("shared"); a subscription in any other
    store (e.g. the legacy no-header one) watches an empty store and never fires.
    """
    internal_secret = os.getenv("INTERNAL_SERVICE_SECRET", "")
    registrar = SubscriptionRegistrar(
        orion_url=settings.orion_ld_url,
        notification_url="http://bioorchestrator-api-service:8420/api/ngsi-ld/notify",
        subscriptions=[{"type": "AgriCrop"}],
        module_name="bioorchestrator",
        context_url=settings.context_url,
        notification_headers=(
            {"X-Internal-Service-Secret": internal_secret}
            if internal_secret else None
        ),
    )
    try:
        result = await registrar.ensure_all([settings.catalog_tenant])
        print(f"[bioorchestrator] catalog subscription ensured: {result}")
    except Exception as exc:  # noqa: BLE001
        print(f"[bioorchestrator] WARNING: subscription setup failed: {exc}")


async def _reconcile_catalog() -> int:
    """Idempotently reconcile Neo4j with the canonical AgriCrop catalog.

    The subscription only fires on changes; this backfills pre-existing
    entities (and self-heals if Neo4j is wiped). merge_agri_crop is MERGE,
    so re-running is safe.
    """
    orion = OrionClient(
        settings.catalog_tenant,
        base_url=settings.orion_ld_url,
        context_url=settings.context_url,
    )
    try:
        dao = GraphDAO(get_driver())
        count = await sync_all_agri_crops(dao, orion)
        print(f"[bioorchestrator] catalog reconcile: {count} AgriCrop synced to Neo4j")
        return count
    finally:
        await orion.close()


def _is_deterministic_migration_failure(exc: BaseException) -> bool:
    """True when retrying cannot help without a data or file fix.

    That is a ``MigrationError`` (bad statement, constraint over duplicate data, plain index in the
    way, malformed file) unless the server code says it was transient. Everything else
    (``ServiceUnavailable``, ``SessionExpired``, connection errors, anything unexpected) is treated
    as transient: it is retried and never fails readiness.
    """
    if not isinstance(exc, MigrationError):
        return False
    return not (exc.code or "").startswith("Neo.TransientError.")


async def _attempt_startup_migrations(driver) -> bool:
    """One attempt with the strict runner (schema statements only). True on success; never raises.

    Success clears ``_migration_failure``. A deterministic failure latches it (CRITICAL log). A
    transient failure logs a WARNING and leaves readiness as it was. Data migrations (MERGE/SET)
    are not run here (``include_data=False``); they never ran at startup and are applied by
    ``scripts/apply_cypher_migrations.py``.
    """
    global _migration_failure
    try:
        report = await asyncio.wait_for(apply_migrations(driver, include_data=False),
                                        timeout=_MIGRATION_ATTEMPT_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if _is_deterministic_migration_failure(exc):
            _migration_failure = {
                "file": getattr(exc, "file", None),
                "statement_no": getattr(exc, "statement_no", None),
                "code": getattr(exc, "code", None) or type(exc).__name__,
            }
            logger.critical(
                "schema migration failed, readiness will fail until it succeeds "
                "file=%s statement_no=%s code=%s error=%s",
                _migration_failure["file"], _migration_failure["statement_no"],
                _migration_failure["code"], str(exc).replace("\n", " ")[:300],
            )
        else:
            logger.warning(
                "schema migration attempt failed transiently, will retry (readiness unchanged) "
                "error_type=%s error=%s",
                type(exc).__name__, str(exc).replace("\n", " ")[:300],
                exc_info=not isinstance(exc, (DriverError, Neo4jError, OSError)),
            )
        return False
    _migration_failure = None
    print(
        f"[bioorchestrator] schema migrations applied: {len(report.applied)} files "
        f"({report.skipped_data} data statements not run at startup)"
    )
    return True


async def _retry_startup_migrations(driver) -> None:
    """Retry until one attempt succeeds: capped backoff for transient errors, slow after a latch."""
    delay = _MIGRATION_TRANSIENT_BACKOFF_START_S
    while True:
        await asyncio.sleep(_MIGRATION_SCHEMA_RETRY_S if _migration_failure is not None else delay)
        if await _attempt_startup_migrations(driver):
            return
        delay = (
            _MIGRATION_TRANSIENT_BACKOFF_START_S
            if _migration_failure is not None
            else min(delay * 2, _MIGRATION_TRANSIENT_BACKOFF_CAP_S)
        )


async def _run_startup_migrations(driver) -> None:
    """First attempt inline (so a schema failure is known before the pod can become Ready and a
    rolling update keeps the previous pod), then a background retry task if it did not succeed."""
    global _migration_task
    if await _attempt_startup_migrations(driver):
        return
    task = asyncio.create_task(_retry_startup_migrations(driver))
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    _migration_task = task


async def _stop_startup_migration_retry() -> None:
    """Cancel the retry task (shutdown, before the driver is closed)."""
    global _migration_task
    task, _migration_task = _migration_task, None
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# Climate classes the recommender is run for at startup (the most requested ones).
_WARMUP_CLIMATE_CLASSES: tuple[str, ...] = ("Csa", "Cfb", "Dfb", "BSk")
_WARMUP_DELAY_S = 5.0
# Same shape as the what-to-sow page's default request, so a first request with default filters
# can also hit the answer cache.
_WARMUP_TOP_N = 15


def _recommend_warmup_enabled() -> bool:
    """On by default; ``RECOMMEND_WARMUP=0`` (or false/no/off) turns the warm-up off."""
    return os.getenv("RECOMMEND_WARMUP", "1").strip().lower() not in ("0", "false", "no", "off")


async def _warm_recommend() -> None:
    """Run the recommender once per common climate class (best-effort, one at a time).

    The first request after a restart would otherwise pay for query-plan compilation (seconds,
    per climate class). It scans no more than a user request does and runs after uvicorn has bound
    its socket, so readiness is never blocked. A failed warm-up only means a cold first request:
    it never raises (cancellation at shutdown still propagates).
    """
    await asyncio.sleep(_WARMUP_DELAY_S)
    try:
        dao = GraphDAO(get_driver())
    except Exception as exc:  # noqa: BLE001
        logger.warning("recommend warm-up skipped: %s", type(exc).__name__)
        return
    for climate_class in _WARMUP_CLIMATE_CLASSES:
        started = time.monotonic()
        try:
            await dao.recommend_for_conditions({
                "climate_class": climate_class, "soil_type": None, "irrigation_regime": None,
                "management": "any", "season": "all", "purpose": "main", "country": None,
                "crops": None, "top_n": _WARMUP_TOP_N,
            })
            logger.info("recommend warm-up climate_class=%s elapsed_s=%.1f",
                        climate_class, time.monotonic() - started)
        except Exception as exc:  # noqa: BLE001
            logger.warning("recommend warm-up failed climate_class=%s: %s",
                           climate_class, type(exc).__name__)


async def _start_background_tasks():
    """Initialize background workers after uvicorn has bound its socket."""
    await asyncio.sleep(2)  # Give uvicorn a moment to complete startup
    try:
        from app.workers.queue import background_queue
        from app.workers.rule_worker import handle_evaluate_action_rules
        from app.workers.sync_worker import handle_sync_agri_crop
        background_queue.register("sync_agri_crop", handle_sync_agri_crop)
        background_queue.register("evaluate_action_rules", handle_evaluate_action_rules)
        # Keep strong refs: a bare create_task can be garbage-collected, which
        # silently kills the forever-running run_loop (queue then never dispatches).
        _BG_TASKS.add(asyncio.create_task(background_queue.run_loop()))
        _BG_TASKS.add(asyncio.create_task(_ensure_catalog_subscription()))
        async def _reconcile_guarded():
            try:
                await _reconcile_catalog()
            except Exception as exc:  # noqa: BLE001
                print(f"[bioorchestrator] WARNING: catalog reconcile failed: {exc}")
        _BG_TASKS.add(asyncio.create_task(_reconcile_guarded()))
        if _recommend_warmup_enabled():
            _BG_TASKS.add(asyncio.create_task(_warm_recommend()))
        print("[bioorchestrator] background tasks started")
    except Exception as exc:  # noqa: BLE001
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
        driver = await init_driver()
        print("[bioorchestrator] Neo4j connected")
    except Exception as exc:  # noqa: BLE001
        print(f"[bioorchestrator] WARNING: Neo4j unavailable on startup: {exc}")
    else:
        # Idempotent schema migrations: a schema failure fails readiness, a transient one only retries.
        await _run_startup_migrations(driver)

    # Seed external capability registrations (best-effort)
    try:
        from app.core.dependencies import get_driver
        from app.graph.capability_dao import CapabilityDao
        from app.services.capability_loader import seed_external_capabilities
        seeded = await seed_external_capabilities(CapabilityDao(get_driver()))
        print(f"[bioorchestrator] Seeded {seeded} external capabilities")
    except Exception as exc:  # noqa: BLE001
        print(f"[bioorchestrator] WARNING: capability seed failed: {exc}")

    # Schedule background tasks after uvicorn binds (don't block startup)
    loop = asyncio.get_running_loop()
    loop.call_soon(lambda: asyncio.ensure_future(_start_background_tasks()))

    yield

    await _stop_startup_migration_retry()
    await close_driver()
    print("[bioorchestrator] Neo4j connection closed")


configure_logging(settings.log_level)

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
from app.api import router as api_router

app.include_router(api_router, prefix="/api")

# Catalog, NGSI-LD notify, and parcel data routers
from app.api.v1.catalog import router as catalog_router
from app.api.v1.notify import router as notify_router
from app.api.v1.parcel_data import router as parcel_data_router
from app.api.v1.phenology_notify import router as phenology_notify_router

app.include_router(catalog_router, prefix="/api/crop")
app.include_router(notify_router, prefix="/api/ngsi-ld")
app.include_router(parcel_data_router, prefix="/api")
app.include_router(phenology_notify_router, prefix="/api/graph/internal")

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
    print("[bioorchestrator] IkerKeta routes registered on main app")
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
    except Exception:  # noqa: BLE001,S110
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

    Checks cached IkerKeta import state and the startup schema-migration outcome (both set
    during lifespan). Must be fast (no imports, no I/O) — K8s probes every 10s.
    """
    if _migration_failure is not None:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "schema migration failed", **_migration_failure},
        )
    if _ikerketa_available:
        return {"status": "ready"}
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "reason": "ikerketa not available"},
    )


@app.post("/api/pipeline/run", dependencies=[Depends(require_platform_admin)])
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
                except Exception:  # noqa: BLE001
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
        except Exception:  # noqa: BLE001,S110
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
    except Exception:  # noqa: BLE001
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

@app.post("/api/ingestion/navarra-agraria", dependencies=[Depends(require_platform_admin)])
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
        from app.core.dependencies import get_neo4j_driver
        from app.ingestion.navarra_ingester import NavarraIngester

        driver = await anext(get_neo4j_driver())
        ingester = NavarraIngester(driver)
        stats = await ingester.ingest(jsonld_path, dry_run=dry_run)
        return {"status": "ok", "stats": stats}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e))
