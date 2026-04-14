from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException
import httpx

from orglens.layers.layer2_core.observability.settings import (
    ObservabilityLlmSettings,
    ObservabilitySettings,
)


def _fmt_metric_labels(labels: dict[str, str]) -> str:
    parts = []
    for key, value in labels.items():
        safe = value.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'{key}="{safe}"')
    return "{" + ",".join(parts) + "}" if parts else ""


class ObservabilityStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ping(self) -> bool:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            return await conn.fetchval("SELECT 1") == 1

    async def fetch_repos(self) -> list[str]:
        await self.connect()
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT repo FROM contribution_windows ORDER BY repo"
            )
            return [r["repo"] for r in rows]

    async def fetch_bus_factor(self, repo: str, module: str | None, window: int | None) -> list[dict[str, Any]]:
        await self.connect()
        assert self._pool is not None
        sql = """
            WITH latest AS (
              SELECT repo, module, window_days, MAX(computed_at) AS computed_at
              FROM bus_factor_scores
              WHERE repo = $1
                AND ($2::TEXT IS NULL OR module = $2)
                AND ($3::INT IS NULL OR window_days = $3)
              GROUP BY repo, module, window_days
            )
            SELECT b.*
            FROM bus_factor_scores b
            JOIN latest l
              ON b.repo = l.repo
             AND b.module = l.module
             AND b.window_days = l.window_days
             AND b.computed_at = l.computed_at
            ORDER BY b.risk_level DESC, b.bus_factor ASC
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, repo, module, window)
            return [dict(r) for r in rows]

    async def fetch_drift(self, repo: str, module: str | None) -> list[dict[str, Any]]:
        await self.connect()
        assert self._pool is not None
        sql = """
            WITH latest AS (
              SELECT repo, module, MAX(computed_at) AS computed_at
              FROM ownership_drift
              WHERE repo = $1
                AND ($2::TEXT IS NULL OR module = $2)
              GROUP BY repo, module
            )
            SELECT d.*
            FROM ownership_drift d
            JOIN latest l
              ON d.repo = l.repo
             AND d.module = l.module
             AND d.computed_at = l.computed_at
            ORDER BY d.drift_score DESC
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, repo, module)
            return [dict(r) for r in rows]

    async def fetch_succession(self, repo: str) -> list[dict[str, Any]]:
        await self.connect()
        assert self._pool is not None
        sql = """
            WITH latest AS (
              SELECT repo, actor, MAX(computed_at) AS computed_at
              FROM succession_risk
              WHERE repo = $1
              GROUP BY repo, actor
            )
            SELECT s.*
            FROM succession_risk s
            JOIN latest l
              ON s.repo = l.repo
             AND s.actor = l.actor
             AND s.computed_at = l.computed_at
            ORDER BY s.risk_score DESC
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, repo)
            return [dict(r) for r in rows]

    async def fetch_risk_summary(self, repo: str) -> dict[str, Any]:
        bus = await self.fetch_bus_factor(repo=repo, module=None, window=30)
        drift = await self.fetch_drift(repo=repo, module=None)
        succession = await self.fetch_succession(repo=repo)

        modules: dict[str, dict[str, Any]] = {}
        for row in bus:
            modules.setdefault(row["module"], {})["bus_factor"] = {
                "value": row["bus_factor"],
                "risk": row["risk_level"],
            }
        for row in drift:
            modules.setdefault(row["module"], {})["drift"] = {
                "score": row["drift_score"],
                "risk": row["risk_level"],
            }

        return {
            "repo": repo,
            "module_count": len(modules),
            "modules": [
                {"module": module_name, **payload}
                for module_name, payload in sorted(modules.items())
            ],
            "top_succession_risks": succession[:5],
        }

    async def what_if_remove_actor(self, repo: str, remove_actor: str) -> dict[str, Any]:
        await self.connect()
        assert self._pool is not None

        sql = """
            WITH latest_window AS (
              SELECT repo, module, MAX(window_end) AS window_end
              FROM contribution_windows
              WHERE repo = $1 AND window_days = 30
              GROUP BY repo, module
            )
            SELECT cw.repo, cw.module, cw.actor, cw.owner_share
            FROM contribution_windows cw
            JOIN latest_window w
              ON cw.repo = w.repo
             AND cw.module = w.module
             AND cw.window_end = w.window_end
            WHERE cw.window_days = 30
        """
        async with self._pool.acquire() as conn:
            rows = [dict(r) for r in await conn.fetch(sql, repo)]

        by_module: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_module.setdefault(row["module"], []).append(row)

        def _bf(dist: list[dict[str, Any]]) -> int:
            total = 0.0
            for idx, d in enumerate(sorted(dist, key=lambda x: float(x["owner_share"]), reverse=True), start=1):
                total += float(d["owner_share"])
                if total >= 0.5:
                    return idx
            return len(dist)

        impacted = []
        for module, dist in by_module.items():
            before = _bf(dist)
            filtered = [d for d in dist if d["actor"] != remove_actor]
            if not filtered:
                continue
            total_share = sum(float(d["owner_share"]) for d in filtered)
            if total_share <= 0:
                continue
            renormalized = [
                {**d, "owner_share": float(d["owner_share"]) / total_share}
                for d in filtered
            ]
            after = _bf(renormalized)
            if before > 1 and after == 1:
                sorted_after = sorted(renormalized, key=lambda x: float(x["owner_share"]), reverse=True)
                new_top = sorted_after[0]
                second = sorted_after[1] if len(sorted_after) > 1 else None
                gap = float(new_top["owner_share"]) - (float(second["owner_share"]) if second else 0.0)
                impacted.append(
                    {
                        "module": module,
                        "before_bus_factor": before,
                        "after_bus_factor": after,
                        "new_top_owner": new_top["actor"],
                        "ownership_gap": gap,
                    }
                )

        return {
            "repo": repo,
            "removed_actor": remove_actor,
            "critical_flips": impacted,
        }

    async def fetch_weekly_trends(self, repo: str) -> dict[str, Any]:
        await self.connect()
        assert self._pool is not None

        sql = """
            SELECT
              repo,
              module,
              date_trunc('week', computed_at) AS week_start,
              AVG(bus_factor)::FLOAT AS bus_factor_avg
            FROM bus_factor_scores
            WHERE repo = $1
              AND window_days = 30
            GROUP BY repo, module, date_trunc('week', computed_at)
            ORDER BY module, week_start
        """
        async with self._pool.acquire() as conn:
            rows = [dict(r) for r in await conn.fetch(sql, repo)]

        module_series: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            module_series[row["module"]].append(
                {
                    "week_start": row["week_start"].isoformat(),
                    "bus_factor": float(row["bus_factor_avg"]),
                }
            )

        def _classify(series: list[dict[str, Any]]) -> str:
            if len(series) < 2:
                return "stable"
            delta = float(series[-1]["bus_factor"]) - float(series[-2]["bus_factor"])
            if delta >= 0.2:
                return "improving"
            if delta <= -0.2:
                return "degrading"
            return "stable"

        def _forecast(series: list[dict[str, Any]]) -> dict[str, Any]:
            if len(series) < 3:
                return {"slope_per_week": 0.0, "weeks_to_bus_factor_1": None, "warning": False}

            y = [float(p["bus_factor"]) for p in series[-8:]]
            n = len(y)
            x = list(range(n))
            sum_x = float(sum(x))
            sum_y = float(sum(y))
            sum_x2 = float(sum(v * v for v in x))
            sum_xy = float(sum(x[i] * y[i] for i in range(n)))
            denom = n * sum_x2 - sum_x * sum_x
            if denom == 0:
                return {"slope_per_week": 0.0, "weeks_to_bus_factor_1": None, "warning": False}

            slope = (n * sum_xy - sum_x * sum_y) / denom
            latest = y[-1]
            weeks_to_one: float | None = None
            if slope < 0 and latest > 1.0:
                weeks_to_one = (1.0 - latest) / slope
                if weeks_to_one < 0:
                    weeks_to_one = None

            warning = weeks_to_one is not None and weeks_to_one <= 8.0
            return {
                "slope_per_week": round(float(slope), 4),
                "weeks_to_bus_factor_1": (round(float(weeks_to_one), 2) if weeks_to_one is not None else None),
                "warning": warning,
            }

        modules = []
        for module_name in sorted(module_series):
            series = module_series[module_name]
            modules.append(
                {
                    "module": module_name,
                    "series": series,
                    "trend": _classify(series),
                    "forecast": _forecast(series),
                }
            )

        return {"repo": repo, "modules": modules}

    async def render_metrics_text(self) -> str:
        await self.connect()
        assert self._pool is not None

        lines = [
            "# HELP orglens_bus_factor Current bus factor by module/window",
            "# TYPE orglens_bus_factor gauge",
            "# HELP orglens_drift_score Current ownership drift score",
            "# TYPE orglens_drift_score gauge",
            "# HELP orglens_succession_risk_score Current succession risk score",
            "# TYPE orglens_succession_risk_score gauge",
            "# HELP orglens_events_ingested_total Total events ingested from metrics table",
            "# TYPE orglens_events_ingested_total counter",
            "# HELP orglens_events_deduplicated_total Total deduplicated events",
            "# TYPE orglens_events_deduplicated_total counter",
            "# HELP orglens_dead_letters_total Total dead letters",
            "# TYPE orglens_dead_letters_total counter",
            "# HELP orglens_ingestion_lag_ms Average ingestion lag in milliseconds",
            "# TYPE orglens_ingestion_lag_ms gauge",
            "# HELP orglens_bus_factor_trend Weekly trend state (-1 degrading, 0 stable, 1 improving)",
            "# TYPE orglens_bus_factor_trend gauge",
            "# HELP orglens_bus_factor_collapse_warning Early warning for bus factor collapse within 8 weeks",
            "# TYPE orglens_bus_factor_collapse_warning gauge",
        ]

        async with self._pool.acquire() as conn:
            bus_rows = await conn.fetch(
                """
                WITH latest AS (
                  SELECT repo, module, window_days, MAX(computed_at) AS computed_at
                  FROM bus_factor_scores
                  GROUP BY repo, module, window_days
                )
                SELECT b.repo, b.module, b.window_days, b.bus_factor
                FROM bus_factor_scores b
                JOIN latest l
                  ON b.repo = l.repo
                 AND b.module = l.module
                 AND b.window_days = l.window_days
                 AND b.computed_at = l.computed_at
                """
            )
            for row in bus_rows:
                labels = _fmt_metric_labels(
                    {
                        "repo": row["repo"],
                        "module": row["module"],
                        "window": str(row["window_days"]),
                    }
                )
                lines.append(f"orglens_bus_factor{labels} {int(row['bus_factor'])}")

            drift_rows = await conn.fetch(
                """
                WITH latest AS (
                  SELECT repo, module, MAX(computed_at) AS computed_at
                  FROM ownership_drift
                  GROUP BY repo, module
                )
                SELECT d.repo, d.module, d.drift_score
                FROM ownership_drift d
                JOIN latest l
                  ON d.repo = l.repo
                 AND d.module = l.module
                 AND d.computed_at = l.computed_at
                """
            )
            for row in drift_rows:
                labels = _fmt_metric_labels({"repo": row["repo"], "module": row["module"]})
                lines.append(f"orglens_drift_score{labels} {float(row['drift_score'])}")

            succession_rows = await conn.fetch(
                """
                WITH latest AS (
                  SELECT repo, actor, MAX(computed_at) AS computed_at
                  FROM succession_risk
                  GROUP BY repo, actor
                )
                SELECT s.repo, s.actor, s.risk_score
                FROM succession_risk s
                JOIN latest l
                  ON s.repo = l.repo
                 AND s.actor = l.actor
                 AND s.computed_at = l.computed_at
                """
            )
            for row in succession_rows:
                labels = _fmt_metric_labels({"repo": row["repo"], "actor": row["actor"]})
                lines.append(f"orglens_succession_risk_score{labels} {float(row['risk_score'])}")

            trend_rows = await conn.fetch(
                """
                SELECT repo, module, date_trunc('week', computed_at) AS week_start, AVG(bus_factor)::FLOAT AS bf
                FROM bus_factor_scores
                WHERE window_days = 30
                GROUP BY repo, module, date_trunc('week', computed_at)
                ORDER BY repo, module, week_start
                """
            )
            grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
            for row in trend_rows:
                grouped[(row["repo"], row["module"])].append(float(row["bf"]))

            for (repo, module), values in grouped.items():
                if len(values) < 2:
                    trend_value = 0
                else:
                    delta = values[-1] - values[-2]
                    trend_value = 1 if delta >= 0.2 else (-1 if delta <= -0.2 else 0)

                slope = 0.0
                weeks_to_one = None
                if len(values) >= 3:
                    y = values[-8:]
                    n = len(y)
                    x = list(range(n))
                    sum_x = float(sum(x))
                    sum_y = float(sum(y))
                    sum_x2 = float(sum(v * v for v in x))
                    sum_xy = float(sum(x[i] * y[i] for i in range(n)))
                    denom = n * sum_x2 - sum_x * sum_x
                    if denom != 0:
                        slope = (n * sum_xy - sum_x * sum_y) / denom
                        if slope < 0 and y[-1] > 1.0:
                            calc = (1.0 - y[-1]) / slope
                            if calc >= 0:
                                weeks_to_one = calc

                warning = 1 if (weeks_to_one is not None and weeks_to_one <= 8.0) else 0
                labels = _fmt_metric_labels({"repo": repo, "module": module})
                lines.append(f"orglens_bus_factor_trend{labels} {trend_value}")
                lines.append(f"orglens_bus_factor_collapse_warning{labels} {warning}")

            ingest_rows = await conn.fetch(
                """
                SELECT
                  COALESCE(repo, 'unknown') AS repo,
                  COALESCE(source, 'unknown') AS source,
                  SUM(events_received)::BIGINT AS events_received,
                  SUM(duplicates)::BIGINT AS duplicates,
                  SUM(dead_letters)::BIGINT AS dead_letters,
                  AVG(avg_lag_ms)::FLOAT AS avg_lag_ms
                FROM ingest_metrics
                GROUP BY COALESCE(repo, 'unknown'), COALESCE(source, 'unknown')
                """
            )
            for row in ingest_rows:
                repo_source = {"repo": row["repo"], "source": row["source"]}
                lines.append(
                    f"orglens_events_ingested_total{_fmt_metric_labels(repo_source)} {int(row['events_received'])}"
                )
                lines.append(
                    f"orglens_events_deduplicated_total{_fmt_metric_labels({'repo': row['repo']})} {int(row['duplicates'])}"
                )
                lines.append(
                    f"orglens_dead_letters_total{_fmt_metric_labels({'repo': row['repo']})} {int(row['dead_letters'])}"
                )
                if row["avg_lag_ms"] is not None:
                    lines.append(
                        f"orglens_ingestion_lag_ms{_fmt_metric_labels({'repo': row['repo']})} {float(row['avg_lag_ms'])}"
                    )

        return "\n".join(lines) + "\n"


class LlmOverviewService:
    def __init__(self, settings: ObservabilityLlmSettings) -> None:
        self._settings = settings

    async def summarize(
        self,
        repo: str,
        risk_summary: dict[str, Any],
        weekly_trends: dict[str, Any],
    ) -> dict[str, Any]:
        warnings = self._warning_modules(weekly_trends)
        fallback = self._fallback_overview(repo, risk_summary, weekly_trends)

        if not self._settings.enabled:
            return {
                "provider": "heuristic",
                "model": None,
                "summary": fallback,
                "warning_modules": warnings,
                "used_llm": False,
            }

        if self._settings.provider != "openai_compatible":
            raise HTTPException(status_code=400, detail="Unsupported LLM provider configured")
        if not self._settings.api_key:
            raise HTTPException(status_code=400, detail="LLM enabled but api_key is missing")

        system_prompt = (
            "You are an engineering risk analyst for software repositories. "
            "Create a concise, evidence-based overview and near-term forecast grounded only in the input JSON. "
            "Do not invent data. Use 3 sections with headings: Overall, Forecast, Immediate Actions."
        )
        user_payload = {
            "repo": repo,
            "snapshot": self._make_llm_snapshot(risk_summary, weekly_trends),
            "focus": "bus-factor collapse risk and trend direction by module",
        }
        user_prompt = json.dumps(user_payload, default=str)

        body = {
            "model": self._settings.model,
            "temperature": self._settings.temperature,
            "max_tokens": self._settings.max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self._settings.api_key}",
            "Content-Type": "application/json",
        }

        try:
            async with httpx.AsyncClient(timeout=self._settings.timeout_seconds) as client:
                resp = await client.post(self._settings.api_url, json=body, headers=headers)
            resp.raise_for_status()
            payload = resp.json()
            content = (
                payload.get("choices", [{}])[0]
                .get("message", {})
                .get("content")
            )
            if not isinstance(content, str) or not content.strip():
                raise ValueError("LLM response missing message content")
            summary = content.strip()
        except Exception as exc:  # fallback keeps API useful during model/provider outages
            extra = ""
            if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
                snippet = exc.response.text[:240].replace("\n", " ")
                extra = f" status={exc.response.status_code} body={snippet}"
            summary = (
                f"LLM request failed ({type(exc).__name__}{extra}); falling back to heuristic overview.\n\n"
                f"{fallback}"
            )
            return {
                "provider": "heuristic_fallback",
                "model": self._settings.model,
                "summary": summary,
                "warning_modules": warnings,
                "used_llm": False,
            }

        return {
            "provider": self._settings.provider,
            "model": self._settings.model,
            "summary": summary,
            "warning_modules": warnings,
            "used_llm": True,
        }

    def _compact_risk_summary(self, risk_summary: dict[str, Any]) -> dict[str, Any]:
        modules = risk_summary.get("modules", [])
        top_succession = risk_summary.get("top_succession_risks", [])
        return {
            "repo": risk_summary.get("repo"),
            "module_count": risk_summary.get("module_count", len(modules)),
            "modules": modules[:12],
            "top_succession_risks": top_succession[:8],
        }

    def _compact_weekly_trends(self, weekly_trends: dict[str, Any]) -> dict[str, Any]:
        modules = weekly_trends.get("modules", [])
        compact_modules = []
        for row in modules[:20]:
            compact_modules.append(
                {
                    "module": row.get("module"),
                    "trend": row.get("trend"),
                    "forecast": row.get("forecast"),
                    "latest_series": (row.get("series") or [])[-4:],
                }
            )
        return {
            "repo": weekly_trends.get("repo"),
            "modules": compact_modules,
        }

    def _make_llm_snapshot(self, risk_summary: dict[str, Any], weekly_trends: dict[str, Any]) -> dict[str, Any]:
        modules = risk_summary.get("modules", [])
        trend_modules = weekly_trends.get("modules", [])
        warning_modules = self._warning_modules(weekly_trends)

        critical_bus = []
        high_bus = []
        for row in modules:
            module_name = str(row.get("module", "unknown"))
            bus = row.get("bus_factor") or {}
            risk = str(bus.get("risk", ""))
            value = bus.get("value")
            if risk.upper() == "CRITICAL" or (isinstance(value, int) and value <= 1):
                critical_bus.append(module_name)
            elif risk.upper() == "HIGH":
                high_bus.append(module_name)

        degrading = [
            str(row.get("module", "unknown"))
            for row in trend_modules
            if str(row.get("trend", "")) == "degrading"
        ]

        top_succession = []
        for row in (risk_summary.get("top_succession_risks") or [])[:3]:
            top_succession.append(
                {
                    "actor": row.get("actor"),
                    "risk_score": row.get("risk_score"),
                    "risk_level": row.get("risk_level"),
                }
            )

        return {
            "module_count": len(modules),
            "critical_bus_modules": critical_bus[:8],
            "high_bus_modules": high_bus[:8],
            "degrading_modules": degrading[:8],
            "warning_modules": warning_modules[:8],
            "top_succession": top_succession,
        }

    def _warning_modules(self, weekly_trends: dict[str, Any]) -> list[str]:
        modules = weekly_trends.get("modules", [])
        out: list[str] = []
        for row in modules:
            forecast = row.get("forecast") or {}
            if forecast.get("warning"):
                out.append(str(row.get("module", "unknown")))
        return sorted(out)

    def _fallback_overview(
        self,
        repo: str,
        risk_summary: dict[str, Any],
        weekly_trends: dict[str, Any],
    ) -> str:
        modules = risk_summary.get("modules", [])
        total_modules = len(modules)

        low_bus = 0
        for m in modules:
            bf = ((m.get("bus_factor") or {}).get("value"))
            if bf is not None and int(bf) <= 1:
                low_bus += 1

        trend_rows = weekly_trends.get("modules", [])
        degrading = [m.get("module") for m in trend_rows if m.get("trend") == "degrading"]
        warnings = self._warning_modules(weekly_trends)

        return (
            f"Overall: {repo} has {total_modules} analyzed modules; {low_bus} are currently at bus factor <= 1. "
            f"Forecast: {len(degrading)} modules show degrading weekly trend and {len(warnings)} modules "
            f"have projected collapse risk within 8 weeks. "
            "Immediate Actions: prioritize ownership expansion in warning modules, pair high-risk owners with "
            "secondary maintainers, and review PR/commit distribution weekly."
        )


def create_observability_app(
    store: ObservabilityStore, settings: ObservabilitySettings | None = None
) -> FastAPI:
    app = FastAPI(title="OrgLens Layer 2 Observability", version="0.1.0")
    llm_settings = settings.llm if settings else ObservabilityLlmSettings()
    llm_service = LlmOverviewService(llm_settings)

    @app.on_event("startup")
    async def _startup() -> None:
        await store.connect()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await store.close()

    @app.get("/health")
    async def health() -> dict[str, str]:
        ok = await store.ping()
        return {"status": "ok" if ok else "degraded"}

    @app.get("/api/repos")
    async def repos() -> dict[str, list[str]]:
        return {"repos": await store.fetch_repos()}

    @app.get("/api/risk/summary")
    async def risk_summary(repo: str) -> dict[str, Any]:
        return await store.fetch_risk_summary(repo=repo)

    @app.get("/api/busfactor")
    async def busfactor(repo: str, module: str | None = None, window: int | None = None) -> dict[str, Any]:
        rows = await store.fetch_bus_factor(repo=repo, module=module, window=window)
        return {"repo": repo, "rows": rows}

    @app.get("/api/drift")
    async def drift(repo: str, module: str | None = None) -> dict[str, Any]:
        rows = await store.fetch_drift(repo=repo, module=module)
        return {"repo": repo, "rows": rows}

    @app.get("/api/succession")
    async def succession(repo: str) -> dict[str, Any]:
        rows = await store.fetch_succession(repo=repo)
        return {"repo": repo, "rows": rows}

    @app.get("/api/whatif")
    async def whatif(repo: str, remove_actor: str) -> dict[str, Any]:
        return await store.what_if_remove_actor(repo=repo, remove_actor=remove_actor)

    @app.get("/api/trends/weekly")
    async def weekly_trends(repo: str) -> dict[str, Any]:
        return await store.fetch_weekly_trends(repo=repo)

    @app.get("/api/overview/forecast")
    async def overview_forecast(repo: str) -> dict[str, Any]:
        risk = await store.fetch_risk_summary(repo=repo)
        trends = await store.fetch_weekly_trends(repo=repo)
        overview = await llm_service.summarize(repo=repo, risk_summary=risk, weekly_trends=trends)

        return {
            "repo": repo,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "overview": overview,
            "risk_summary": risk,
            "weekly_trends": trends,
        }

    @app.get("/metrics", response_class=None)
    async def metrics() -> Any:
        # Returned as plain text for Prometheus scraping.
        from fastapi.responses import PlainTextResponse

        return PlainTextResponse(await store.render_metrics_text(), media_type="text/plain")

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "orglens-layer2-observability"}

    return app
