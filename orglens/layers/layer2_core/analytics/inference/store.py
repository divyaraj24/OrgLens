from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

CREATE_INFERENCE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bus_factor_scores (
  id          SERIAL PRIMARY KEY,
  repo        TEXT NOT NULL,
  module      TEXT NOT NULL,
  window_days INT NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL,
  bus_factor  INT NOT NULL,
  risk_level  TEXT NOT NULL,
  top_owners  JSONB,
  explanation TEXT
);

CREATE TABLE IF NOT EXISTS ownership_drift (
  id              SERIAL PRIMARY KEY,
  repo            TEXT NOT NULL,
  module          TEXT NOT NULL,
  computed_at     TIMESTAMPTZ NOT NULL,
  drift_score     FLOAT NOT NULL,
  risk_level      TEXT NOT NULL,
  jsd_value       FLOAT NOT NULL,
  prev_top_owner  TEXT,
  curr_top_owner  TEXT,
  owner_changed   BOOLEAN NOT NULL,
  explanation     TEXT
);

CREATE TABLE IF NOT EXISTS succession_risk (
  id             SERIAL PRIMARY KEY,
  repo           TEXT NOT NULL,
  actor          TEXT NOT NULL,
  computed_at    TIMESTAMPTZ NOT NULL,
  risk_score     FLOAT NOT NULL,
  risk_level     TEXT NOT NULL,
  criticality    FLOAT NOT NULL,
  replaceability FLOAT NOT NULL,
  continuity     FLOAT NOT NULL,
  owned_modules  JSONB,
  explanation    TEXT
);

CREATE INDEX IF NOT EXISTS idx_bf_lookup ON bus_factor_scores(repo, module, computed_at);
CREATE INDEX IF NOT EXISTS idx_drift_lookup ON ownership_drift(repo, module, computed_at);
CREATE INDEX IF NOT EXISTS idx_succ_lookup ON succession_risk(repo, actor, computed_at);
"""

FETCH_LATEST_SNAPSHOT_SQL = """
WITH latest AS (
  SELECT repo, module, MAX(window_end) AS window_end
  FROM contribution_windows
  WHERE window_days = $1
    AND ($2::TEXT[] IS NULL OR repo = ANY($2::TEXT[]))
  GROUP BY repo, module
)
SELECT
  cw.repo,
  cw.module,
  cw.actor,
  cw.owner_share,
  cw.weight,
  cw.window_end
FROM contribution_windows cw
JOIN latest l
  ON cw.repo = l.repo AND cw.module = l.module AND cw.window_end = l.window_end
WHERE cw.window_days = $1
ORDER BY cw.repo, cw.module, cw.owner_share DESC
"""

FETCH_BASELINE_SNAPSHOT_SQL = """
WITH latest AS (
  SELECT repo, module, MAX(window_end) AS latest_end
  FROM contribution_windows
  WHERE window_days = $1
    AND ($3::TEXT[] IS NULL OR repo = ANY($3::TEXT[]))
  GROUP BY repo, module
), baseline AS (
  SELECT
    l.repo,
    l.module,
    MAX(cw.window_end) AS baseline_end
  FROM latest l
  JOIN contribution_windows cw
    ON cw.repo = l.repo
   AND cw.module = l.module
   AND cw.window_days = $1
   AND cw.window_end <= l.latest_end - make_interval(days => $2)
  GROUP BY l.repo, l.module
)
SELECT
  cw.repo,
  cw.module,
  cw.actor,
  cw.owner_share,
  cw.weight,
  cw.window_end
FROM contribution_windows cw
JOIN baseline b
  ON cw.repo = b.repo AND cw.module = b.module AND cw.window_end = b.baseline_end
WHERE cw.window_days = $1
ORDER BY cw.repo, cw.module, cw.owner_share DESC
"""

FETCH_WINDOW_ENDS_SQL = """
SELECT DISTINCT window_end
FROM contribution_windows
WHERE window_days = $1
  AND ($2::TEXT[] IS NULL OR repo = ANY($2::TEXT[]))
ORDER BY window_end
"""

FETCH_SNAPSHOT_AT_SQL = """
SELECT
  repo,
  module,
  actor,
  owner_share,
  weight,
  window_end
FROM contribution_windows
WHERE window_days = $1
  AND window_end = $2
  AND ($3::TEXT[] IS NULL OR repo = ANY($3::TEXT[]))
ORDER BY repo, module, owner_share DESC
"""

FETCH_BASELINE_AT_SQL = """
WITH baseline AS (
  SELECT
    repo,
    module,
    MAX(window_end) AS baseline_end
  FROM contribution_windows
  WHERE window_days = $1
    AND window_end <= ($2::timestamptz - make_interval(days => $3::int))
    AND ($4::TEXT[] IS NULL OR repo = ANY($4::TEXT[]))
  GROUP BY repo, module
)
SELECT
  cw.repo,
  cw.module,
  cw.actor,
  cw.owner_share,
  cw.weight,
  cw.window_end
FROM contribution_windows cw
JOIN baseline b
  ON cw.repo = b.repo AND cw.module = b.module AND cw.window_end = b.baseline_end
WHERE cw.window_days = $1
ORDER BY cw.repo, cw.module, cw.owner_share DESC
"""

INSERT_BUS_FACTOR_SQL = """
INSERT INTO bus_factor_scores (
  repo, module, window_days, computed_at, bus_factor, risk_level, top_owners, explanation
)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
"""

INSERT_DRIFT_SQL = """
INSERT INTO ownership_drift (
  repo, module, computed_at, drift_score, risk_level, jsd_value,
  prev_top_owner, curr_top_owner, owner_changed, explanation
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""

INSERT_SUCCESSION_SQL = """
INSERT INTO succession_risk (
  repo, actor, computed_at, risk_score, risk_level,
  criticality, replaceability, continuity, owned_modules, explanation
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
"""


class InferenceStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
            async with self._pool.acquire() as conn:
                await conn.execute(CREATE_INFERENCE_SCHEMA_SQL)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def fetch_latest_snapshot(self, *, window_days: int, repos: list[str] | None) -> list[dict[str, Any]]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(FETCH_LATEST_SNAPSHOT_SQL, window_days, repos)
            return [dict(r) for r in rows]

    async def fetch_window_ends(self, *, window_days: int, repos: list[str] | None) -> list[datetime]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(FETCH_WINDOW_ENDS_SQL, window_days, repos)
            return [r["window_end"] for r in rows]

    async def fetch_snapshot_at(
        self,
        *,
        window_days: int,
        window_end: datetime,
        repos: list[str] | None,
    ) -> list[dict[str, Any]]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(FETCH_SNAPSHOT_AT_SQL, window_days, window_end, repos)
            return [dict(r) for r in rows]

    async def fetch_baseline_snapshot(
        self,
        *,
        window_days: int,
        baseline_offset_days: int,
        repos: list[str] | None,
    ) -> list[dict[str, Any]]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(FETCH_BASELINE_SNAPSHOT_SQL, window_days, baseline_offset_days, repos)
            return [dict(r) for r in rows]

    async def fetch_baseline_snapshot_at(
        self,
        *,
        window_days: int,
        window_end: datetime,
        baseline_offset_days: int,
        repos: list[str] | None,
    ) -> list[dict[str, Any]]:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                FETCH_BASELINE_AT_SQL,
                window_days,
                window_end,
                baseline_offset_days,
                repos,
            )
            return [dict(r) for r in rows]

    async def reset_run(self, *, computed_at: datetime) -> None:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM bus_factor_scores WHERE computed_at = $1", computed_at)
                await conn.execute("DELETE FROM ownership_drift WHERE computed_at = $1", computed_at)
                await conn.execute("DELETE FROM succession_risk WHERE computed_at = $1", computed_at)

    async def insert_bus_factor_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        payload = [
            (
                row["repo"],
                row["module"],
                int(row["window_days"]),
                row["computed_at"],
                int(row["bus_factor"]),
                row["risk_level"],
                json.dumps(row["top_owners"]),
                row["explanation"],
            )
            for row in rows
        ]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(INSERT_BUS_FACTOR_SQL, payload)
        return len(rows)

    async def insert_drift_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        payload = [
            (
                row["repo"],
                row["module"],
                row["computed_at"],
                float(row["drift_score"]),
                row["risk_level"],
                float(row["jsd_value"]),
                row.get("prev_top_owner"),
                row.get("curr_top_owner"),
                bool(row["owner_changed"]),
                row["explanation"],
            )
            for row in rows
        ]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(INSERT_DRIFT_SQL, payload)
        return len(rows)

    async def insert_succession_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        payload = [
            (
                row["repo"],
                row["actor"],
                row["computed_at"],
                float(row["risk_score"]),
                row["risk_level"],
                float(row["criticality"]),
                float(row["replaceability"]),
                float(row["continuity"]),
                json.dumps(row["owned_modules"]),
                row["explanation"],
            )
            for row in rows
        ]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(INSERT_SUCCESSION_SQL, payload)
        return len(rows)
