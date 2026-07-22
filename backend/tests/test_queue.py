import asyncio
import logging

import pytest
from unittest.mock import AsyncMock

from app.workers.queue import BackgroundQueue


@pytest.mark.asyncio
async def test_process_one_dispatches_registered_handler():
    q = BackgroundQueue()  # REDIS_URL unset in tests -> in-memory mode
    handler = AsyncMock()
    q.register("do_thing", handler)
    await q.enqueue("do_thing", "montiko", n=3)
    await q.process_one()
    handler.assert_awaited_once_with("montiko", n=3)


@pytest.mark.asyncio
async def test_process_one_noop_on_empty_queue():
    q = BackgroundQueue()
    await q.process_one()  # must not raise


@pytest.mark.asyncio
async def test_run_loop_logs_swallowed_exception(caplog):
    q = BackgroundQueue()
    failed = asyncio.Event()

    # Importing `pcse` (elsewhere in the suite, e.g. wofost_service tests)
    # runs logging.config.dictConfig(disable_existing_loggers=True), which
    # sets .disabled = True on every logger that already existed — including
    # this one, if it was already created by an earlier test. That makes
    # logger.warning() a silent no-op regardless of caplog's level/handlers.
    # Force it back on for this test; unrelated to the sleep-vs-event race
    # this test used to have.
    logging.getLogger("app.workers.queue").disabled = False

    async def boom():
        failed.set()
        raise RuntimeError("kaboom")

    q.process_one = boom
    with caplog.at_level(logging.WARNING, logger="app.workers.queue"):
        task = asyncio.create_task(q.run_loop())
        # Wait for the handler to actually run instead of racing a fixed
        # sleep against the scheduler — flaked under CI load (#71).
        await asyncio.wait_for(failed.wait(), timeout=5)
        task.cancel()
    assert any("background task failed" in r.message for r in caplog.records)
