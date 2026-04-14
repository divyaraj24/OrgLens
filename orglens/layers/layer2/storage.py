from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable

import asyncpg

from orglens.layers.layer2.models import DeadLetterRecord, RawEventIn

CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_events (
  event_id      TEXT PRIMARY KEY,
  source        TEXT NOT NULL,
  repo          TEXT NOT NULL,
  actor         TEXT NOT NULL,
  event_type    TEXT NOT NULL,
  target        TEXT,
  module        TEXT,
  timestamp     TIMESTAMPTZ NOT NULL,
  metadata      JSONB,
  ingested_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_re_repo_module ON raw_events(repo, module);
CREATE INDEX IF NOT EXISTS idx_re_actor ON raw_events(actor);
CREATE INDEX IF NOT EXISTS idx_re_timestamp ON raw_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_re_repo_ts ON raw_events(repo, timestamp);
CREATE INDEX IF NOT EXISTS idx_re_event_type ON raw_events(event_type);

CREATE TABLE IF NOT EXISTS dead_letters (
  id            SERIAL PRIMARY KEY,
  raw_payload   JSONB NOT NULL,
  error_reason  TEXT NOT NULL,
  received_at   TIMESTAMPTZ NOT NULL,
  source        TEXT
);

CREATE TABLE IF NOT EXISTS ingest_metrics (
  id              SERIAL PRIMARY KEY,
  window_start    TIMESTAMPTZ NOT NULL,
  window_end      TIMESTAMPTZ NOT NULL,
  repo            TEXT,
  source          TEXT,
  events_received INT DEFAULT 0,
  events_written  INT DEFAULT 0,
  duplicates      INT DEFAULT 0,
  dead_letters    INT DEFAULT 0,
  avg_lag_ms      FLOAT
);

CREATE TABLE IF NOT EXISTS ingest_queue (
    id            BIGSERIAL PRIMARY KEY,
    payload       JSONB NOT NULL,
    source        TEXT,
    status        TEXT NOT NULL DEFAULT 'queued',
    attempts      INT NOT NULL DEFAULT 0,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingest_queue_ready
    ON ingest_queue(status, available_at);

CREATE TABLE IF NOT EXISTS pipeline_state (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

INSERT_EVENT_SQL = """
INSERT INTO raw_events (
  event_id, source, repo, actor, event_type, target, module, timestamp, metadata
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
ON CONFLICT (event_id) DO NOTHING
RETURNING 1;
"""

INSERT_DEAD_LETTER_SQL = """
INSERT INTO dead_letters (raw_payload, error_reason, received_at, source)
VALUES ($1::jsonb, $2, $3, $4);
"""

INSERT_METRICS_SQL = """
INSERT INTO ingest_metrics (
  window_start, window_end, repo, source,
  events_received, events_written, duplicates, dead_letters, avg_lag_ms
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
"""

INSERT_QUEUE_SQL = """
INSERT INTO ingest_queue (payload, source)
VALUES ($1::jsonb, $2)
RETURNING id;
"""

CLAIM_QUEUE_SQL = """
WITH candidate AS (
    SELECT id
    FROM ingest_queue
    WHERE status = 'queued'
        AND available_at <= NOW()
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE ingest_queue q
SET status = 'processing',
        attempts = q.attempts + 1,
        updated_at = NOW()
FROM candidate
WHERE q.id = candidate.id
RETURNING q.id, q.payload, q.source, q.attempts, q.created_at;
"""

MARK_QUEUE_DONE_SQL = """
DELETE FROM ingest_queue WHERE id = $1;
"""

REQUEUE_SQL = """
UPDATE ingest_queue
SET status = 'queued',
        available_at = NOW() + ($2::INT * INTERVAL '1 second'),
        last_error = $3,
        updated_at = NOW()
WHERE id = $1;
"""

MARK_QUEUE_DEAD_SQL = """
UPDATE ingest_queue
SET status = 'dead',
        last_error = $2,
        updated_at = NOW()
WHERE id = $1;
"""

QUEUE_DEPTH_SQL = """
SELECT COUNT(*)::BIGINT
FROM ingest_queue
WHERE status = 'queued'
    AND available_at <= NOW();
"""

INSERT_ACTIVE_REPO_SQL = """
INSERT INTO pipeline_state (key, value)
VALUES ('active_repo', $1)
ON CONFLICT (key) DO NOTHING;
"""

GET_ACTIVE_REPO_SQL = """
SELECT value FROM pipeline_state WHERE key = 'active_repo';
"""

CLEAR_ACTIVE_REPO_SQL = """
DELETE FROM pipeline_state WHERE key = 'active_repo';
"""


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
            async with self._pool.acquire() as conn:
                await conn.execute(CREATE_SCHEMA_SQL)

    async def ping(self) -> bool:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            value = await conn.fetchval("SELECT 1")
            return value == 1

    async def write_events(self, events: Iterable[RawEventIn]) -> tuple[int, int]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        written = 0
        total = 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for e in events:
                    total += 1
                    row = await conn.fetchrow(
                        INSERT_EVENT_SQL,
                        e.event_id,
                        e.source.value,
                        e.repo,
                        e.actor,
                        e.event_type.value,
                        e.target,
                        e.module,
                        e.timestamp,
                        json.dumps(e.metadata),
                    )
                    if row is not None:
                        written += 1

        duplicates = total - written
        return written, duplicates

    async def write_dead_letter(self, letter: DeadLetterRecord) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            await conn.execute(
                INSERT_DEAD_LETTER_SQL,
                json.dumps(letter.raw_payload),
                letter.error_reason,
                letter.received_at,
                letter.source,
            )

    async def write_metrics(
        self,
        window_start: datetime,
        window_end: datetime,
        repo: str | None,
        source: str | None,
        events_received: int,
        events_written: int,
        duplicates: int,
        dead_letters: int,
        avg_lag_ms: float | None,
    ) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            await conn.execute(
                INSERT_METRICS_SQL,
                window_start,
                window_end,
                repo,
                source,
                events_received,
                events_written,
                duplicates,
                dead_letters,
                avg_lag_ms,
            )

    async def enqueue_batch(self, payload: list[dict], source: str | None) -> int:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(INSERT_QUEUE_SQL, json.dumps(payload), source)
            return int(row["id"])

    async def claim_next_batch(self) -> dict | None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(CLAIM_QUEUE_SQL)
                if row is None:
                    return None
                return {
                    "id": int(row["id"]),
                    "payload": row["payload"],
                    "source": row["source"],
                    "attempts": int(row["attempts"]),
                    "created_at": row["created_at"],
                }

    async def mark_queue_done(self, queue_id: int) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            await conn.execute(MARK_QUEUE_DONE_SQL, queue_id)

    async def requeue_batch(self, queue_id: int, delay_seconds: int, error: str) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            await conn.execute(REQUEUE_SQL, queue_id, max(delay_seconds, 1), error[:2000])

    async def mark_queue_dead(self, queue_id: int, error: str) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            await conn.execute(MARK_QUEUE_DEAD_SQL, queue_id, error[:2000])

    async def queue_depth(self) -> int:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            value = await conn.fetchval(QUEUE_DEPTH_SQL)
            return int(value or 0)

    async def ensure_active_repo(self, repo: str) -> tuple[bool, str | None]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(INSERT_ACTIVE_REPO_SQL, repo)
                active = await conn.fetchval(GET_ACTIVE_REPO_SQL)
        active_repo = str(active) if active is not None else None
        return (active_repo == repo), active_repo

    async def get_active_repo(self) -> str | None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            active = await conn.fetchval(GET_ACTIVE_REPO_SQL)
            return str(active) if active is not None else None

    async def reset_repo_data(self) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                TRUNCATE TABLE
                  raw_events,
                  dead_letters,
                  ingest_metrics,
                  ingest_queue,
                  contribution_windows,
                  bus_factor_scores,
                  ownership_drift,
                  succession_risk
                RESTART IDENTITY CASCADE;
                """
            )
            await conn.execute(CLEAR_ACTIVE_REPO_SQL)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
