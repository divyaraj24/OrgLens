from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from orglens.layers.layer2_core.analytics.aggregation.settings import AggregationSettings
from orglens.layers.layer2_core.analytics.aggregation.store import AggregationStore

log = logging.getLogger(__name__)


class TimeSeriesAggregator:
    def __init__(self, settings: AggregationSettings, store: AggregationStore) -> None:
        self._settings = settings
        self._store = store

    async def run_once(self, repos: list[str] | None = None, now: datetime | None = None) -> dict[str, int]:
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        written_total = 0

        for window_days in self._settings.windows:
            aggregates = await self._store.aggregate_actor_module_window(
                window_days=window_days,
                window_end=now_utc,
                repos=repos,
            )

            by_module_total_weight: dict[tuple[str, str], float] = defaultdict(float)
            enriched: list[dict] = []

            for row in aggregates:
                weight = (
                    row["commit_count"] * self._settings.commit_weight
                    + row["lines_changed"] * self._settings.lines_weight
                    + row["pr_count"] * self._settings.pr_weight
                    + row["review_count"] * self._settings.review_weight
                    + row["issue_count"] * self._settings.issue_weight
                )
                key = (row["repo"], row["module"])
                by_module_total_weight[key] += weight
                row["weight"] = float(weight)
                row["window_days"] = int(window_days)
                row["window_end"] = now_utc
                enriched.append(row)

            for row in enriched:
                denom = by_module_total_weight[(row["repo"], row["module"])]
                row["owner_share"] = float(row["weight"] / denom) if denom > 0 else 0.0

            written = await self._store.write_windows(enriched)
            written_total += written
            log.info(
                "Aggregation window=%sd rows=%d repos=%s",
                window_days,
                written,
                "all" if repos is None else len(repos),
            )

        return {"rows_written": written_total, "windows": len(self._settings.windows)}

    async def run_history(
        self,
        repos: list[str] | None = None,
        step_days: int = 30,
        max_points: int = 240,
        now: datetime | None = None,
    ) -> dict[str, int]:
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        step_days = max(1, int(step_days))
        max_points = max(1, int(max_points))

        bounds = await self._store.fetch_repo_time_bounds(repos)
        if not bounds:
            return {"rows_written": 0, "points": 0}

        written_total = 0
        point_total = 0

        for repo_bound in bounds:
            repo = str(repo_bound["repo"])
            min_ts = repo_bound["min_ts"]
            max_ts = repo_bound["max_ts"]
            if min_ts is None or max_ts is None:
                continue

            min_ts = min_ts.astimezone(timezone.utc)
            max_ts = min(max_ts.astimezone(timezone.utc), now_utc)

            for window_days in self._settings.windows:
                points = self._build_points(
                    min_ts=min_ts,
                    max_ts=max_ts,
                    window_days=int(window_days),
                    step_days=step_days,
                    max_points=max_points,
                )
                for window_end in points:
                    aggregates = await self._store.aggregate_actor_module_window(
                        window_days=int(window_days),
                        window_end=window_end,
                        repos=[repo],
                    )

                    by_module_total_weight: dict[tuple[str, str], float] = defaultdict(float)
                    enriched: list[dict] = []

                    for row in aggregates:
                        weight = (
                            row["commit_count"] * self._settings.commit_weight
                            + row["lines_changed"] * self._settings.lines_weight
                            + row["pr_count"] * self._settings.pr_weight
                            + row["review_count"] * self._settings.review_weight
                            + row["issue_count"] * self._settings.issue_weight
                        )
                        key = (row["repo"], row["module"])
                        by_module_total_weight[key] += weight
                        row["weight"] = float(weight)
                        row["window_days"] = int(window_days)
                        row["window_end"] = window_end
                        enriched.append(row)

                    for row in enriched:
                        denom = by_module_total_weight[(row["repo"], row["module"])]
                        row["owner_share"] = float(row["weight"] / denom) if denom > 0 else 0.0

                    written = await self._store.write_windows(enriched)
                    written_total += written
                    point_total += 1

                log.info(
                    "Aggregation history repo=%s window=%sd points=%d",
                    repo,
                    window_days,
                    len(points),
                )

        return {"rows_written": written_total, "points": point_total}

    def _build_points(
        self,
        *,
        min_ts: datetime,
        max_ts: datetime,
        window_days: int,
        step_days: int,
        max_points: int,
    ) -> list[datetime]:
        first = min_ts + timedelta(days=window_days)
        if first >= max_ts:
            return [max_ts]

        points: list[datetime] = []
        cursor = first
        step = timedelta(days=step_days)
        while cursor <= max_ts:
            points.append(cursor)
            cursor += step

        if not points or points[-1] != max_ts:
            points.append(max_ts)

        if len(points) <= max_points:
            return points

        stride = max(1, len(points) // max_points)
        sampled = points[::stride]
        if sampled[-1] != points[-1]:
            sampled.append(points[-1])
        return sampled[-max_points:]
