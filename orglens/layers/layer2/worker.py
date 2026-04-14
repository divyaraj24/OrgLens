from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from orglens.layers.layer2.processor import EventProcessor

log = logging.getLogger(__name__)


class StreamWorker:
    """Consumes Redis stream entries and processes one batch at a time."""

    def __init__(self, queue, processor: EventProcessor, status, store) -> None:
        self._queue = queue
        self._processor = processor
        self._status = status
        self._store = store
        self._running = False

    async def run_forever(self) -> None:
        self._running = True
        await self._queue.ensure_group()

        while self._running:
            entries = await self._queue.read_group(count=1, block_ms=5000)
            if not entries:
                continue

            for entry in entries:
                try:
                    result = await self._processor.process_batch(entry.payload, source=entry.source)
                except Exception:
                    log.exception("Batch processing failed; leaving stream entry unacknowledged")
                    continue

                if result.success:
                    await self._queue.ack(entry.stream_id)

                await self._status.mark_processed(
                    written=result.events_written,
                    duplicates=result.duplicates,
                    dead_letters=result.dead_letters,
                )

                lag_ms = self._lag_ms(entry.received_at)
                now = datetime.now(timezone.utc)
                await self._store.write_metrics(
                    window_start=now.replace(minute=0, second=0, microsecond=0),
                    window_end=now,
                    repo=None,
                    source=entry.source,
                    events_received=result.events_received,
                    events_written=result.events_written,
                    duplicates=result.duplicates,
                    dead_letters=result.dead_letters,
                    avg_lag_ms=lag_ms,
                )

    async def stop(self) -> None:
        self._running = False
        await asyncio.sleep(0)

    def _lag_ms(self, received_at: str) -> float:
        try:
            received = datetime.fromisoformat(received_at)
            if received.tzinfo is None:
                received = received.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - received.astimezone(timezone.utc)
            return max(delta.total_seconds() * 1000.0, 0.0)
        except Exception:
            return 0.0
