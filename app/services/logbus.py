"""Log fan-out bus (in-process, asyncio only).

SSE consumers subscribe here; producers (app logs, command output, audit
events) publish. This is why the deployment MUST run a single uvicorn worker:
the bus is process-local memory, not a broker. Multi-worker would silently
split subscribers from publishers. See deploy/dayhost.service + README.

Fase 2 could swap this for Redis/NATS without touching the SSE endpoint.
"""
from __future__ import annotations

import asyncio
import collections
import time
from dataclasses import dataclass
from typing import AsyncIterator


@dataclass(frozen=True)
class LogLine:
    ts: float
    level: str
    source: str
    message: str

    def as_sse(self) -> str:
        stamp = time.strftime("%H:%M:%S", time.localtime(self.ts))
        payload = f"{stamp} [{self.level.upper()}] {self.source}: {self.message}"
        # SSE frame: data lines must not contain raw newlines.
        payload = payload.replace("\r", " ").replace("\n", " ")
        return f"data: {payload}\n\n"


class LogBus:
    """Bounded ring buffer + per-subscriber queues."""

    def __init__(self, history: int = 200, queue_size: int = 500) -> None:
        self._history: collections.deque[LogLine] = collections.deque(maxlen=history)
        self._subscribers: set[asyncio.Queue[LogLine]] = set()
        self._queue_size = queue_size
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------- producer side
    def publish(self, message: str, level: str = "info", source: str = "app") -> LogLine:
        line = LogLine(ts=time.time(), level=level, source=source, message=message)
        self._history.append(line)
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(line)
            except asyncio.QueueFull:
                # Slow consumer: drop the oldest line rather than block producers.
                try:
                    queue.get_nowait()
                    queue.put_nowait(line)
                except Exception:
                    pass
        return line

    # ------------------------------------------------------------- consumer side
    async def subscribe(self) -> asyncio.Queue[LogLine]:
        queue: asyncio.Queue[LogLine] = asyncio.Queue(maxsize=self._queue_size)
        async with self._lock:
            self._subscribers.add(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[LogLine]) -> None:
        async with self._lock:
            self._subscribers.discard(queue)

    def snapshot(self) -> list[LogLine]:
        return list(self._history)

    async def stream(self, queue: asyncio.Queue[LogLine]) -> AsyncIterator[str]:
        """Yield SSE frames; sends a comment keepalive when idle."""
        try:
            # Replay recent history so a fresh console is not empty.
            for line in self.snapshot()[-50:]:
                yield line.as_sse()
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield line.as_sse()
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            await self.unsubscribe(queue)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


bus = LogBus()
