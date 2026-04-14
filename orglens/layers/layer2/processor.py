from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from orglens.layers.layer2.models import DeadLetterRecord, RawEventIn

log = logging.getLogger(__name__)


@dataclass
class BatchProcessResult:
    success: bool
    events_received: int
    events_written: int
    duplicates: int
    dead_letters: int


class EventProcessor:
    """Validates, deduplicates, and persists batches.

    PostgreSQL write is the source-of-truth path. MinIO archival is best-effort
    and runs asynchronously so it never blocks successful DB ingestion.
    """

    def __init__(self, store: Any, archive: Any | None = None, max_retries: int = 3) -> None:
        self._store = store
        self._archive = archive
        self._max_retries = max_retries

    async def process_batch(self, payload: list[dict[str, Any]], source: str) -> BatchProcessResult:
        received_at = datetime.now(timezone.utc)
        valid_events: list[RawEventIn] = []
        dead_letters = 0

        for raw_event in payload:
            try:
                valid_events.append(RawEventIn.model_validate(raw_event))
            except ValidationError as exc:
                dead_letters += 1
                await self._store.write_dead_letter(
                    DeadLetterRecord(
                        raw_payload=raw_event,
                        error_reason=str(exc),
                        received_at=received_at,
                        source=source,
                    )
                )

        if not valid_events:
            return BatchProcessResult(
                success=True,
                events_received=len(payload),
                events_written=0,
                duplicates=0,
                dead_letters=dead_letters,
            )

        written = 0
        duplicates = 0
        delay = 1.0
        last_error: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                written, duplicates = await self._store.write_events(valid_events)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    break
                await asyncio.sleep(delay)
                delay *= 2

        if last_error is not None:
            # Do not ack the stream entry when PostgreSQL is unavailable.
            # Redis replay + PostgreSQL dedup protects against data loss.
            raise RuntimeError(f"db_write_failed: {last_error}")

        if self._archive is not None and written > 0:
            asyncio.create_task(self._archive_task(valid_events))

        return BatchProcessResult(
            success=True,
            events_received=len(payload),
            events_written=written,
            duplicates=duplicates,
            dead_letters=dead_letters,
        )

    async def _archive_task(self, events: list[RawEventIn]) -> None:
        try:
            assert self._archive is not None
            await self._archive.append_events(events)
        except Exception:
            # MinIO is non-critical by design for ingestion correctness.
            log.exception("MinIO archival failed; continuing with PostgreSQL as source of truth")
