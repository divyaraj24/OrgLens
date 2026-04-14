"""
BatchBuffer — accumulates RawEvents and flushes to an OutputSink when either:
  • count  >= max_events (default 100)
  • time   >= flush_interval_seconds since last flush (default 30 s)

Thread-safe via asyncio.Lock.  The timer task runs in the background and is
cancelled cleanly on shutdown.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, List

from orglens.layers.layer1.models.raw_event import RawEvent

log = logging.getLogger(__name__)


class BatchBuffer:
    def __init__(
        self,
        sink_fn: Callable,      # async ([RawEvent]) -> None
        max_events: int = 100,
        flush_interval: float = 30.0,
    ):
        self._sink = sink_fn
        self._max = max_events
        self._interval = flush_interval
        self._buf: List[RawEvent] = []
        self._lock = asyncio.Lock()
        self._timer_task: asyncio.Task | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background flush timer. Call once after the event loop starts."""
        self._timer_task = asyncio.get_event_loop().create_task(self._timer_loop())
        log.info(
            "BatchBuffer started (max=%d, interval=%.0fs)", self._max, self._interval
        )

    async def stop(self) -> None:
        """Flush remaining events and cancel the timer task."""
        if self._timer_task:
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass
        await self._flush()
        log.info("BatchBuffer stopped")

    # ── Public API ────────────────────────────────────────────────────────────

    async def add(self, events: List[RawEvent]) -> None:
        """Add events to the buffer; flush immediately if threshold is reached."""
        async with self._lock:
            self._buf.extend(events)
            buffered = len(self._buf)

        log.debug("Buffered +%d events (total=%d)", len(events), buffered)

        if buffered >= self._max:
            await self._flush()

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _timer_loop(self) -> None:
        """Background task: flush every flush_interval seconds."""
        while True:
            await asyncio.sleep(self._interval)
            await self._flush()

    async def _flush(self) -> None:
        async with self._lock:
            if not self._buf:
                return
            batch = self._buf[:]
            self._buf.clear()

        log.info("Flushing %d events to sink", len(batch))
        try:
            await self._sink(batch)
        except Exception:
            log.exception("Sink failed — events dropped (size=%d)", len(batch))
