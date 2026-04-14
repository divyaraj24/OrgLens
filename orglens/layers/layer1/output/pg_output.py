"""
PgOutput — writes event batches to a local PostgreSQL database.

Table: raw_events
  Deduplication is handled via ON CONFLICT DO NOTHING on event_id.

Connection string format:
  postgresql://user:password@host:port/dbname
"""
from __future__ import annotations

import json
import logging
from typing import List

from orglens.layers.layer1.models.raw_event import RawEvent
from orglens.layers.layer1.output.base import OutputSink

log = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id       TEXT PRIMARY KEY,
    event_type     TEXT NOT NULL,
    source         TEXT NOT NULL,
    repo           TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,
    actor          TEXT NOT NULL,
    actor_email    TEXT,
    module         TEXT,
    sha            TEXT,
    files_changed  JSONB,
    lines_added    INTEGER DEFAULT 0,
    lines_deleted  INTEGER DEFAULT 0,
    co_authors     JSONB,
    commit_message TEXT,
    pr_number      INTEGER,
    merged_by      TEXT,
    reviewer       TEXT,
    verdict        TEXT,
    requested_reviewers JSONB,
    issue_number   INTEGER,
    assignees      JSONB,
    closer         TEXT,
    labels         JSONB,
    ingested_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raw_events_repo      ON raw_events (repo);
CREATE INDEX IF NOT EXISTS idx_raw_events_actor     ON raw_events (actor);
CREATE INDEX IF NOT EXISTS idx_raw_events_module    ON raw_events (module);
CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp ON raw_events (timestamp);
"""

INSERT_SQL = """
INSERT INTO raw_events (
    event_id, event_type, source, repo, timestamp, actor, actor_email,
    module, sha, files_changed, lines_added, lines_deleted, co_authors,
    commit_message, pr_number, merged_by, reviewer, verdict,
    requested_reviewers, issue_number, assignees, closer, labels
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10::jsonb, $11, $12, $13::jsonb,
    $14, $15, $16, $17, $18,
    $19::jsonb, $20, $21::jsonb, $22, $23::jsonb
)
ON CONFLICT (event_id) DO NOTHING;
"""


class PgOutput(OutputSink):
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool = None

    async def _get_pool(self):
        if self._pool is None:
            try:
                import asyncpg
                self._pool = await asyncpg.create_pool(self._dsn)
                async with self._pool.acquire() as conn:
                    await conn.execute(CREATE_TABLE_SQL)
                log.info("PostgreSQL pool created and schema ensured")
            except Exception as exc:
                log.error("Failed to connect to PostgreSQL: %s", exc)
                raise
        return self._pool

    async def send(self, events: List[RawEvent]) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                for e in events:
                    await conn.execute(
                        INSERT_SQL,
                        e.event_id,
                        e.event_type,
                        e.source,
                        e.repo,
                        e.timestamp,
                        e.actor,
                        e.actor_email,
                        e.module,
                        e.sha,
                        json.dumps(e.files_changed),
                        e.lines_added,
                        e.lines_deleted,
                        json.dumps(e.co_authors),
                        e.commit_message,
                        e.pr_number,
                        e.merged_by,
                        e.reviewer,
                        e.verdict,
                        json.dumps(e.requested_reviewers),
                        e.issue_number,
                        json.dumps(e.assignees),
                        e.closer,
                        json.dumps(e.labels),
                    )
        log.info("Inserted %d events into PostgreSQL (with dedup)", len(events))

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
