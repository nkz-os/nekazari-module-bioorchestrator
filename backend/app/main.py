"""NKZ BioOrchestrator Backend — FastAPI wrapper for IkerKeta pipeline.

Mounts the IkerKeta status API and adds NKZ-specific endpoints
for authentication, pipeline execution, and tenant isolation.

Run standalone:
    uvicorn app.main:app --port 8420

In production (K8s):
    Deployed as part of nekazari-module-bioorchestrator pod.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.auth import NKZAuthMiddleware
from app.core.config import settings
from app.core.dependencies import close_driver, init_driver

# Module-level readiness state set during lifespan.
# K8s probes hit /healthz and /readyz every 10-30s — must be fast and never rate-limited.
_ikerketa_available = False


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

# Mount IkerKeta API under /api/v1
try:
    from ikerketa.api import app as ikerketa_api
    app.mount("/api/v1", ikerketa_api)
except ImportError:
    print("[bioorchestrator] ikerketa.api not available — running without data endpoints")


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
    from fastapi.responses import JSONResponse
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

    return {
        "success": result.failure_count == 0,
        "entities_before_dedup": result.entities_before_dedup,
        "entities_after_dedup": result.entities_after_dedup,
        "relationships": result.relationships_total,
        "crossref_matches": result.crossref_matches,
        "duration_seconds": round(result.total_duration_seconds, 2),
        "errors": result.errors,
        "report": report,
    }
