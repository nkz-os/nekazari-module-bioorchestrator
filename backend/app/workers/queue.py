"""Simple background task queue with Redis or in-memory fallback."""
import asyncio
import json
import os
from collections import deque
from typing import Any, Callable, Awaitable


class BackgroundQueue:
    """Lightweight task queue.

    Uses Redis streams if REDIS_URL is set, otherwise in-memory deque.
    In-memory mode is acceptable for single-worker bioorchestrator deployment.
    """

    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "")
        self._memory_queue: deque = deque()
        self._handlers: dict[str, Callable[..., Awaitable[Any]]] = {}

    def register(self, task_name: str, handler: Callable[..., Awaitable[Any]]):
        self._handlers[task_name] = handler

    async def enqueue(self, task_name: str, *args, **kwargs) -> None:
        """Enqueue a task for background processing."""
        payload = json.dumps({"task": task_name, "args": args, "kwargs": kwargs})
        if self.redis_url:
            import redis.asyncio as redis
            r = redis.from_url(self.redis_url)
            await r.xadd("bioorch:tasks", {"payload": payload})
            await r.close()
        else:
            self._memory_queue.append(payload)

    async def process_one(self):
        """Process a single item from the queue."""
        payload = None
        if self.redis_url:
            import redis.asyncio as redis
            r = redis.from_url(self.redis_url)
            items = await r.xread({"bioorch:tasks": "0"}, count=1, block=1000)
            if items:
                _, entries = items[0]
                payload = entries[0][1][b"payload"].decode()
                await r.xdel("bioorch:tasks", entries[0][0])
            await r.close()
        else:
            if self._memory_queue:
                payload = self._memory_queue.popleft()

        if payload:
            data = json.loads(payload)
            handler = self._handlers.get(data["task"])
            if handler:
                await handler(*data["args"], **data["kwargs"])

    async def run_loop(self):
        """Run the processing loop (call in background task)."""
        while True:
            try:
                await self.process_one()
            except Exception:
                await asyncio.sleep(1)


# Singleton
background_queue = BackgroundQueue()
