"""Asynchronous crop simulations: one job per distinct request per day, with progress.

A job is keyed by its inputs (tenant, parameters, the day, engine and calibration
version): an identical request on the same day returns the same job, so a finished
one answers at once (the cache) and a running one is shared. A new day is a new job
(one more day of observed weather). A failed job is run again on the next request.

The store lives in this process: the service runs a single replica (as the
background queue assumes), keeps at most ``MAX_JOBS`` jobs (oldest finished first
out) and loses them on restart (the client starts again). Simulations run one at a
time: each already fills the AquaCrop process pool. A tenant may have at most
``MAX_PENDING_PER_TENANT`` unfinished jobs, so one tenant cannot queue ahead of all
the others.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from app.services.sim_errors import SimulationError

logger = logging.getLogger(__name__)

MAX_JOBS = 256
# ASSUMPTION: 3 unfinished jobs per tenant (a parcel and a couple of sowing-date changes);
# beyond that a request is refused (429) until one finishes, about a minute per simulation.
MAX_PENDING_PER_TENANT = 3
FINISHED = ("done", "failed")

Progress = Callable[[int, int], None]
Run = Callable[[Progress], Awaitable[dict]]


@dataclass
class Job:
    id: str
    tenant_id: str
    state: str = "queued"  # queued | running | done | failed
    done: int = 0
    total: int | None = None  # scenarios, once known
    result: dict | None = None
    error: dict | None = None
    task: asyncio.Task | None = field(default=None, repr=False)


class SimJobs:
    def __init__(self, *, version: str, max_jobs: int = MAX_JOBS, max_concurrent: int = 1,
                 max_pending_per_tenant: int = MAX_PENDING_PER_TENANT):
        self._version = version
        self._max_jobs = max_jobs
        self._max_pending = max_pending_per_tenant
        self._jobs: OrderedDict[str, Job] = OrderedDict()
        self._slots = asyncio.Semaphore(max_concurrent)

    @property
    def version(self) -> str:
        """Engine and calibration version that job ids are keyed by."""
        return self._version

    @staticmethod
    def job_id(tenant_id: str, params: dict[str, Any], today: date, version: str) -> str:
        raw = json.dumps([tenant_id, sorted(params.items()), today.isoformat(), version], default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    async def start(self, tenant_id: str, params: dict[str, Any], run: Run, today: date | None = None) -> Job:
        today = today or datetime.now(UTC).date()
        jid = self.job_id(tenant_id, params, today, self._version)
        job = self._jobs.get(jid)
        if job is not None and job.state != "failed":
            self._jobs.move_to_end(jid)
            return job
        pending = sum(1 for j in self._jobs.values() if j.tenant_id == tenant_id and j.state not in FINISHED)
        if pending >= self._max_pending:
            raise SimulationError("too_many_jobs", "Other simulations of this tenant are still waiting or "
                                  "running; try again when one finishes.", 429)
        job = Job(jid, tenant_id)
        self._jobs[jid] = job
        self._jobs.move_to_end(jid)
        self._evict()
        job.task = asyncio.create_task(self._run(job, run))  # the job keeps the reference
        return job

    async def _run(self, job: Job, run: Run) -> None:
        async with self._slots:
            job.state = "running"

            def progress(done: int, total: int) -> None:
                job.done, job.total = done, total

            try:
                job.result = await run(progress)
                job.state = "done"
            except SimulationError as e:
                job.error = {"code": e.code, "message": e.message, "status_code": e.status_code}
                job.state = "failed"
            except Exception:
                logger.exception("crop_simulation_job_failed job=%s tenant=%s", job.id, job.tenant_id)
                job.error = {"code": "internal_error", "message": "The simulation failed unexpectedly.",
                             "status_code": 500}
                job.state = "failed"
            finally:
                job.task = None

    def get(self, job_id: str, tenant_id: str) -> Job | None:
        job = self._jobs.get(job_id)
        return job if job is not None and job.tenant_id == tenant_id else None

    def _evict(self) -> None:
        while len(self._jobs) > self._max_jobs:
            oldest = next((k for k, j in self._jobs.items() if j.state in FINISHED), None)
            if oldest is None:  # never drop a queued or running job
                return
            del self._jobs[oldest]

    @staticmethod
    def view(job: Job) -> dict[str, Any]:
        return {"job_id": job.id, "state": job.state, "done": job.done, "total": job.total,
                "result": job.result, "error": job.error}


def crop_simulation_version() -> str:
    """Engine and calibration versions: a change in either is a different job."""
    from app.services import yield_calibration as yc
    from app.services.engines import aquacrop_engine as aq

    cal = yc._calibration("wheat") or {}
    return f"aquacrop-{aq._version()}|calibration-{cal.get('version', 'none')}"


_SHARED: SimJobs | None = None


def shared_sim_jobs() -> SimJobs:
    """The process-wide job store (single replica): every route that starts simulations shares it."""
    global _SHARED
    if _SHARED is None:
        _SHARED = SimJobs(version=crop_simulation_version())
    return _SHARED
