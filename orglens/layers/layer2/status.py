from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date

from orglens.layers.layer2.models import IngestStatusSnapshot


@dataclass
class _Counters:
    received: int = 0
    processed: int = 0
    duplicates: int = 0
    dead_letters: int = 0


class IngestStatus:
    """In-memory status counters reset daily for /api/ingest/status."""

    def __init__(self) -> None:
        self._day = date.today()
        self._counters = _Counters()
        self._lock = asyncio.Lock()

    async def mark_received(self, count: int) -> None:
        await self._with_rollover()
        async with self._lock:
            self._counters.received += count

    async def mark_processed(self, written: int, duplicates: int, dead_letters: int) -> None:
        await self._with_rollover()
        async with self._lock:
            self._counters.processed += written
            self._counters.duplicates += duplicates
            self._counters.dead_letters += dead_letters

    async def snapshot(self, queue_depth: int) -> IngestStatusSnapshot:
        await self._with_rollover()
        async with self._lock:
            return IngestStatusSnapshot(
                events_received_today=self._counters.received,
                events_processed_today=self._counters.processed,
                duplicates_dropped_today=self._counters.duplicates,
                dead_letters_today=self._counters.dead_letters,
                redis_queue_depth=queue_depth,
            )

    async def _with_rollover(self) -> None:
        today = date.today()
        if today == self._day:
            return
        async with self._lock:
            if today != self._day:
                self._day = today
                self._counters = _Counters()
