from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import urlparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel

from orglens.layers.layer2.models import IngestStatusSnapshot
from orglens.layers.layer2.processor import EventProcessor
from orglens.layers.layer2.settings import load_layer2_settings
from orglens.layers.layer2.storage import PostgresStore
from orglens.layers.layer2_core.analytics.aggregation.aggregator import TimeSeriesAggregator
from orglens.layers.layer2_core.analytics.aggregation.settings import load_aggregation_settings
from orglens.layers.layer2_core.analytics.aggregation.store import AggregationStore
from orglens.layers.layer2_core.analytics.inference.inference import InferenceEngine
from orglens.layers.layer2_core.analytics.inference.settings import load_inference_settings
from orglens.layers.layer2_core.analytics.inference.store import InferenceStore
from orglens.layers.layer2_core.observability.service import (
    ObservabilityStore,
    create_observability_app,
)
from orglens.layers.layer2_core.observability.settings import load_observability_settings

log = logging.getLogger("orglens.layer2_core")


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    prefix = "bearer "
    auth_lower = authorization.lower()
    if not auth_lower.startswith(prefix):
        return None
    return authorization[len(prefix):].strip()


def _extract_api_key(authorization: str | None, x_api_key: str | None) -> str | None:
    bearer = _extract_bearer_token(authorization)
    if bearer:
        return bearer
    if x_api_key:
        return x_api_key.strip()
    return None


def _extract_signature(signature: str | None) -> str | None:
    if not signature:
        return None
    candidate = signature.strip()
    if candidate.startswith("sha256="):
        candidate = candidate[len("sha256="):]
    return candidate or None


def _verify_signature(
    *,
    body: bytes,
    timestamp: str | None,
    signature: str | None,
    secret: str,
    tolerance_seconds: int,
) -> bool:
    if not timestamp or not signature:
        return False

    try:
        ts_int = int(timestamp)
    except ValueError:
        return False

    if abs(int(time.time()) - ts_int) > tolerance_seconds:
        return False

    provided = _extract_signature(signature)
    if not provided:
        return False

    message = timestamp.encode("utf-8") + b"." + body
    expected = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, provided)


class RunRequest(BaseModel):
    repo_url: str
    from_date: str | None = None
    log_level: str = "INFO"


class AnalyticsRunRequest(BaseModel):
    repo: str
    log_level: str = "INFO"


def _repo_key_from_url(repo_url: str) -> str:
    cleaned = repo_url[:-4] if repo_url.endswith(".git") else repo_url
    parsed = urlparse(cleaned)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"Invalid GitHub repository URL: {repo_url}")
    return f"{parts[-2]}/{parts[-1]}"


@dataclass
class CoreRuntime:
    config_path: str | None
    pg_store: PostgresStore
    processor: EventProcessor
    aggregation_store: AggregationStore
    inference_store: InferenceStore
    aggregator: TimeSeriesAggregator
    inference: InferenceEngine
    obs_store: ObservabilityStore
    api_key: str
    signing_secret: str
    signature_tolerance_seconds: int
    max_retries: int = 3
    base_retry_delay_seconds: int = 5
    poll_interval_seconds: float = 0.5
    _worker_task: asyncio.Task | None = None
    _running: bool = False

    async def ingest_status_snapshot(self) -> IngestStatusSnapshot:
        await self.pg_store.connect()
        assert self.pg_store._pool is not None  # noqa: SLF001
        async with self.pg_store._pool.acquire() as conn:  # noqa: SLF001
            row = await conn.fetchrow(
                """
                SELECT
                  COALESCE(SUM(events_received), 0) AS events_received_today,
                  COALESCE(SUM(events_written), 0) AS events_processed_today,
                  COALESCE(SUM(duplicates), 0) AS duplicates_dropped_today,
                  COALESCE(SUM(dead_letters), 0) AS dead_letters_today
                FROM ingest_metrics
                WHERE window_end >= date_trunc('day', now())
                """
            )
        return IngestStatusSnapshot(
            events_received_today=int(row["events_received_today"]),
            events_processed_today=int(row["events_processed_today"]),
            duplicates_dropped_today=int(row["duplicates_dropped_today"]),
            dead_letters_today=int(row["dead_letters_today"]),
            redis_queue_depth=await self.pg_store.queue_depth(),
        )

    async def start(self) -> None:
        await self.pg_store.connect()
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        self._running = False
        if self._worker_task is not None:
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task
        await self.aggregation_store.close()
        await self.inference_store.close()
        await self.pg_store.close()

    async def _worker_loop(self) -> None:
        while self._running:
            claimed = await self.pg_store.claim_next_batch()
            if claimed is None:
                await asyncio.sleep(self.poll_interval_seconds)
                continue

            queue_id = int(claimed["id"])
            payload_raw = claimed["payload"]
            source = claimed.get("source") or "unknown"
            attempts = int(claimed.get("attempts") or 1)

            payload = payload_raw
            if isinstance(payload_raw, str):
                try:
                    payload = json.loads(payload_raw)
                except json.JSONDecodeError:
                    await self.pg_store.mark_queue_dead(queue_id, "invalid queue payload: malformed JSON")
                    continue

            if not isinstance(payload, list):
                await self.pg_store.mark_queue_dead(queue_id, "invalid queue payload: expected list")
                continue

            try:
                started = datetime.now(timezone.utc)
                result = await self.processor.process_batch(payload, source=source)
                ended = datetime.now(timezone.utc)

                repo = None
                if payload and isinstance(payload[0], dict):
                    first_repo = payload[0].get("repo")
                    if isinstance(first_repo, str):
                        repo = first_repo

                await self.pg_store.write_metrics(
                    window_start=started,
                    window_end=ended,
                    repo=repo,
                    source=source,
                    events_received=result.events_received,
                    events_written=result.events_written,
                    duplicates=result.duplicates,
                    dead_letters=result.dead_letters,
                    avg_lag_ms=0.0,
                )
                await self.pg_store.mark_queue_done(queue_id)
            except Exception as exc:
                if attempts >= self.max_retries:
                    await self.pg_store.mark_queue_dead(queue_id, str(exc))
                else:
                    delay = self.base_retry_delay_seconds * (2 ** max(attempts - 1, 0))
                    await self.pg_store.requeue_batch(queue_id, delay_seconds=delay, error=str(exc))

    async def wait_for_queue_drain(self, timeout_seconds: int) -> bool:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if await self.pg_store.queue_depth() == 0:
                return True
            await asyncio.sleep(1.0)
        return False


async def _run_command(cmd: list[str]) -> dict[str, Any]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await proc.communicate()
    stdout = stdout_b.decode("utf-8", errors="replace")
    stderr = stderr_b.decode("utf-8", errors="replace")
    return {
        "cmd": cmd,
        "exit_code": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
    }


def create_core_app(config_path: str | None) -> FastAPI:
    settings = load_layer2_settings(config_path)
    aggregation_settings = load_aggregation_settings(config_path)
    inference_settings = load_inference_settings(config_path)
    observability_settings = load_observability_settings(config_path)
    pg_store = PostgresStore(settings.pg_dsn)
    aggregation_store = AggregationStore(settings.pg_dsn)
    inference_store = InferenceStore(settings.pg_dsn)
    aggregator = TimeSeriesAggregator(aggregation_settings, aggregation_store)
    inference = InferenceEngine(inference_settings, inference_store)
    processor = EventProcessor(store=pg_store, archive=None)
    obs_store = ObservabilityStore(settings.pg_dsn)

    runtime = CoreRuntime(
        config_path=config_path,
        pg_store=pg_store,
        processor=processor,
        aggregation_store=aggregation_store,
        inference_store=inference_store,
        aggregator=aggregator,
        inference=inference,
        obs_store=obs_store,
        api_key=settings.api_key,
        signing_secret=settings.api_signing_secret,
        signature_tolerance_seconds=settings.api_signature_tolerance_seconds,
        max_retries=int(max(1, int(getattr(settings, "api_queue_max_retries", 3)))),
        base_retry_delay_seconds=int(max(1, int(getattr(settings, "api_queue_retry_base_seconds", 5)))),
        poll_interval_seconds=float(getattr(settings, "api_queue_poll_interval_seconds", 0.5)),
    )

    app = create_observability_app(obs_store, observability_settings)

    @app.on_event("startup")
    async def _startup_core() -> None:
        await runtime.start()
        await runtime.aggregation_store.connect()
        await runtime.inference_store.connect()

    @app.on_event("shutdown")
    async def _shutdown_core() -> None:
        await runtime.stop()

    @app.post("/api/ingest", status_code=status.HTTP_202_ACCEPTED)
    async def ingest(
        request: Request,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
        x_orglens_timestamp: str | None = Header(default=None, alias="X-Orglens-Timestamp"),
        x_orglens_signature: str | None = Header(default=None, alias="X-Orglens-Signature"),
    ) -> dict[str, int]:
        token = _extract_api_key(authorization, x_api_key)
        if runtime.api_key and token != runtime.api_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

        raw_body = await request.body()
        if runtime.signing_secret and not _verify_signature(
            body=raw_body,
            timestamp=x_orglens_timestamp,
            signature=x_orglens_signature,
            secret=runtime.signing_secret,
            tolerance_seconds=runtime.signature_tolerance_seconds,
        ):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid ingest signature")

        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload") from exc

        if not isinstance(body, list):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Batch must be an array")

        if not body:
            return {"queued": 0, "queue_id": 0}

        repos = {
            row.get("repo")
            for row in body
            if isinstance(row, dict) and isinstance(row.get("repo"), str)
        }
        if len(repos) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Single-repo mode enforced: batch must contain exactly one repo",
            )
        batch_repo = next(iter(repos))
        ok, active_repo = await runtime.pg_store.ensure_active_repo(batch_repo)
        if not ok:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Single-repo mode enforced: active repo is {active_repo}",
            )

        source = "unknown"
        if body:
            first_source = body[0].get("source")
            if isinstance(first_source, str):
                source = first_source

        queue_id = await runtime.pg_store.enqueue_batch(body, source)
        return {"queued": len(body), "queue_id": queue_id}

    @app.post("/api/admin/reset-repo-data")
    async def reset_repo_data(
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
    ) -> dict[str, bool | str]:
        token = _extract_api_key(authorization, x_api_key)
        if runtime.api_key and token != runtime.api_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

        await runtime.pg_store.reset_repo_data()
        return {"ok": True, "message": "repository data reset complete"}

    @app.get("/api/ingest/status")
    async def ingest_status() -> dict[str, Any]:
        return (await runtime.ingest_status_snapshot()).model_dump()

    @app.post("/api/run/all")
    async def run_all(req: RunRequest) -> dict[str, Any]:
        cfg = runtime.config_path or "/app/config.aws.yaml"
        try:
            repo_key = _repo_key_from_url(req.repo_url)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        ok, active_repo = await runtime.pg_store.ensure_active_repo(repo_key)
        if not ok:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Single-repo mode enforced: active repo is {active_repo}",
            )

        cmd_pipeline = [
            "orglens-pipeline",
            "--repo-url",
            req.repo_url,
            "--config",
            cfg,
            "--log-level",
            req.log_level,
        ]
        if req.from_date:
            cmd_pipeline.extend(["--from-date", req.from_date])

        r1 = await _run_command(cmd_pipeline)
        if r1["exit_code"] != 0:
            return {"ok": False, "stage": "pipeline", "result": r1}

        drained = await runtime.wait_for_queue_drain(timeout_seconds=900)
        if not drained:
            return {
                "ok": False,
                "stage": "ingest_queue",
                "result": {"error": "Timed out waiting for queue drain", "queue_depth": await runtime.pg_store.queue_depth()},
                "pipeline": r1,
            }

        agg = await runtime.aggregator.run_history(
            repos=[repo_key],
            step_days=30,
            max_points=240,
        )
        inf = await runtime.inference.run_history(
            repos=[repo_key],
            module="all",
            step_days=30,
            max_points=240,
        )
        return {
            "ok": True,
            "repo_url": req.repo_url,
            "from_date": req.from_date,
            "pipeline": {"exit_code": r1["exit_code"]},
            "aggregate": agg,
            "infer": inf,
        }

    @app.post("/api/run/analytics")
    async def run_analytics(req: AnalyticsRunRequest) -> dict[str, Any]:
        ok, active_repo = await runtime.pg_store.ensure_active_repo(req.repo)
        if not ok:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Single-repo mode enforced: active repo is {active_repo}",
            )

        agg = await runtime.aggregator.run_history(
            repos=[req.repo],
            step_days=30,
            max_points=240,
        )
        inf = await runtime.inference.run_history(
            repos=[req.repo],
            module="all",
            step_days=30,
            max_points=240,
        )
        return {
            "ok": True,
            "aggregate": agg,
            "infer": inf,
        }

    return app


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OrgLens Layer 2 Core (minimized)")
    p.add_argument("--config", default=None, help="Path to config file")
    p.add_argument("--host", default=None, help="Host override")
    p.add_argument("--port", default=None, type=int, help="Port override")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)

    settings = load_layer2_settings(args.config)
    host = args.host or settings.api_host
    port = args.port or settings.api_port

    app = create_core_app(args.config)
    uvicorn.run(app, host=host, port=port, log_level=args.log_level.lower())


if __name__ == "__main__":
    main()
