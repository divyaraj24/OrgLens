from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

CREATE_CONTRIBUTION_WINDOWS_SQL = """
CREATE TABLE IF NOT EXISTS contribution_windows (
  id            SERIAL PRIMARY KEY,
  repo          TEXT NOT NULL,
  module        TEXT NOT NULL,
  actor         TEXT NOT NULL,
  window_days   INT NOT NULL,
  window_end    TIMESTAMPTZ NOT NULL,
  commit_count  INT DEFAULT 0,
  pr_count      INT DEFAULT 0,
  review_count  INT DEFAULT 0,
  issue_count   INT DEFAULT 0,
  lines_changed INT DEFAULT 0,
  weight        FLOAT NOT NULL,
  owner_share   FLOAT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cw_unique
  ON contribution_windows(repo, module, actor, window_days, window_end);

CREATE INDEX IF NOT EXISTS idx_cw_lookup
  ON contribution_windows(repo, module, window_days, window_end);
"""

AGGREGATE_WINDOW_SQL = """
SELECT
  repo,
  module,
  actor,
  COUNT(*) FILTER (WHERE event_type = 'commit')::INT AS commit_count,
  COUNT(*) FILTER (WHERE event_type IN ('pr_open', 'pr_merge'))::INT AS pr_count,
  COUNT(*) FILTER (WHERE event_type = 'pr_review')::INT AS review_count,
  COUNT(*) FILTER (WHERE event_type IN ('issue_assign', 'issue_close'))::INT AS issue_count,
  SUM(
    CASE
      WHEN event_type <> 'commit' THEN 0
      WHEN COALESCE(metadata->>'lines_changed', '') ~ '^-?[0-9]+$' THEN (metadata->>'lines_changed')::INT
      ELSE
        (CASE WHEN COALESCE(metadata->>'lines_added', '') ~ '^-?[0-9]+$' THEN (metadata->>'lines_added')::INT ELSE 0 END)
        +
        (CASE WHEN COALESCE(metadata->>'lines_deleted', '') ~ '^-?[0-9]+$' THEN (metadata->>'lines_deleted')::INT ELSE 0 END)
    END
  )::INT AS lines_changed
FROM raw_events
WHERE module IS NOT NULL
  AND module <> ''
  AND timestamp > $1
  AND timestamp <= $2
  AND ($3::TEXT[] IS NULL OR repo = ANY($3::TEXT[]))
GROUP BY repo, module, actor
"""

FETCH_REPO_TIME_BOUNDS_SQL = """
SELECT
  repo,
  MIN(timestamp) AS min_ts,
  MAX(timestamp) AS max_ts
FROM raw_events
WHERE ($1::TEXT[] IS NULL OR repo = ANY($1::TEXT[]))
GROUP BY repo
ORDER BY repo
"""

INSERT_CONTRIBUTION_SQL = """
INSERT INTO contribution_windows (
  repo, module, actor, window_days, window_end,
  commit_count, pr_count, review_count, issue_count, lines_changed,
  weight, owner_share
)
VALUES (
  $1, $2, $3, $4, $5,
  $6, $7, $8, $9, $10,
  $11, $12
)
ON CONFLICT (repo, module, actor, window_days, window_end)
DO UPDATE SET
  commit_count = EXCLUDED.commit_count,
  pr_count = EXCLUDED.pr_count,
  review_count = EXCLUDED.review_count,
  issue_count = EXCLUDED.issue_count,
  lines_changed = EXCLUDED.lines_changed,
  weight = EXCLUDED.weight,
  owner_share = EXCLUDED.owner_share
"""


class AggregationStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
            async with self._pool.acquire() as conn:
                await conn.execute(CREATE_CONTRIBUTION_WINDOWS_SQL)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def aggregate_actor_module_window(
        self,
        *,
        window_days: int,
        window_end: datetime,
        repos: list[str] | None,
    ) -> list[dict[str, Any]]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        window_end = window_end.astimezone(timezone.utc)
        window_start = window_end - timedelta(days=window_days)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(AGGREGATE_WINDOW_SQL, window_start, window_end, repos)
            return [dict(r) for r in rows]

    async def fetch_repo_time_bounds(self, repos: list[str] | None) -> list[dict[str, Any]]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(FETCH_REPO_TIME_BOUNDS_SQL, repos)
            return [dict(r) for r in rows]

    async def write_windows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if self._pool is None:
            await self.connect()
        assert self._pool is not None

        payload = [
            (
                row["repo"],
                row["module"],
                row["actor"],
                int(row["window_days"]),
                row["window_end"],
                int(row["commit_count"]),
                int(row["pr_count"]),
                int(row["review_count"]),
                int(row["issue_count"]),
                int(row["lines_changed"]),
                float(row["weight"]),
                float(row["owner_share"]),
            )
            for row in rows
        ]

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(INSERT_CONTRIBUTION_SQL, payload)

        return len(rows)
