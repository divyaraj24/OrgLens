from __future__ import annotations

import asyncio
import argparse
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from orglens.config import load_config
from orglens.layers.layer1.buffer.batch_buffer import BatchBuffer
from orglens.layers.layer1.normalizer.normalizer import Normalizer
from orglens.layers.layer1.output.api_output import ApiOutput
from orglens.layers.layer1.sources.perceval_runner import PercevalRunner
from orglens.layers.layer1.sources.webhook_listener import create_webhook_app

log = logging.getLogger("orglens.layer1_cloud")


@dataclass
class FixedFromDateState:
    from_date: str | None

    def get_last_fetch(self, repo: str, event_key: str) -> str | None:  # noqa: ARG002
        return self.from_date

    def set_last_fetch(self, repo: str, event_key: str, ts: datetime) -> None:  # noqa: ARG002
        return None

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)


class BackfillRequest(BaseModel):
    repo_url: str
    from_date: str | None = None
    github_token: str | None = None


class BackfillStartResponse(BaseModel):
    job_id: str
    repo: str
    status: str


def _parse_repo_url(repo_url: str) -> dict[str, str]:
    cleaned = repo_url[:-4] if repo_url.endswith(".git") else repo_url
    parsed = urlparse(cleaned)
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 2:
        raise ValueError(f"Invalid GitHub repository URL: {repo_url}")

    owner, repo = parts[-2], parts[-1]
    return {
        "owner": owner,
        "repo": repo,
        "git_url": repo_url if repo_url.endswith(".git") else f"{repo_url}.git",
        "repo_key": f"{owner}/{repo}",
    }


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OrgLens Layer 1 Cloud Ingestion")
    p.add_argument("--config", default=None, help="Path to config file")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", default=8080, type=int)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def create_layer1_cloud_app(config_path: str | None) -> FastAPI:
    cfg = load_config(config_path)

    normalizer = Normalizer(module_map_path=cfg.get("module_map_path", "module_map.yaml"))

    api_cfg = cfg.get("output", {}).get("api", {})
    api_url = os.getenv("ORGLENS_LAYER1_API_URL", api_cfg.get("url", "http://layer2-core:8001/api/ingest"))
    api_key = os.getenv("ORGLENS_API_KEY", api_cfg.get("api_key", ""))
    auth_scheme = os.getenv("ORGLENS_LAYER1_AUTH_SCHEME", api_cfg.get("auth_scheme", "bearer"))
    signing_secret = os.getenv("ORGLENS_INGEST_SIGNING_SECRET", api_cfg.get("signing_secret", ""))

    sink = ApiOutput(
        url=api_url,
        api_key=api_key,
        auth_scheme=auth_scheme,
        signing_secret=signing_secret,
        timeout=float(api_cfg.get("timeout_seconds", 10)),
        max_retries=int(api_cfg.get("max_retries", 3)),
    )

    buffer_cfg = cfg.get("buffer", {})
    buffer = BatchBuffer(
        sink_fn=sink.send,
        max_events=int(buffer_cfg.get("max_events", 100)),
        flush_interval=float(buffer_cfg.get("flush_interval_seconds", 10)),
    )

    webhook_cfg = cfg.get("webhook", {})
    webhook_secret = os.getenv("ORGLENS_WEBHOOK_SECRET", webhook_cfg.get("secret", "")) or None
    skip_sig = bool(webhook_cfg.get("skip_signature_check", False))

    app = create_webhook_app(
        normalizer=normalizer,
        on_events=buffer.add,
        webhook_secret=webhook_secret,
        skip_signature_check=skip_sig,
    )

    backfill_jobs: dict[str, dict[str, Any]] = {}

    async def _run_backfill_job(job_id: str, req: BackfillRequest) -> None:
        try:
            repo = _parse_repo_url(req.repo_url)
            job = backfill_jobs[job_id]
            job["repo"] = repo["repo_key"]

            count = 0

            async def on_events(events):
                nonlocal count
                count += len(events)
                job["queued"] = count
                await buffer.add(events)

            token = req.github_token or cfg.get("github", {}).get("token", "") or os.getenv("ORGLENS_GITHUB_TOKEN", "")
            state = FixedFromDateState(req.from_date)

            runner = PercevalRunner(
                repos=[
                    {
                        "owner": repo["owner"],
                        "repo": repo["repo"],
                        "git_url": repo["git_url"],
                    }
                ],
                github_token=token,
                normalizer=normalizer,
                state=state,
                on_events=on_events,
            )

            job["stage"] = "commits"
            job["progress_percent"] = 5
            await runner._run_commits(repo["repo_key"], repo["git_url"])  # noqa: SLF001

            job["stage"] = "pull_request"
            job["progress_percent"] = 70
            await runner._run_github(repo["repo_key"], repo["owner"], repo["repo"], "pull_request")  # noqa: SLF001

            job["stage"] = "issue"
            job["progress_percent"] = 85
            await runner._run_github(repo["repo_key"], repo["owner"], repo["repo"], "issue")  # noqa: SLF001

            job["stage"] = "flush"
            job["progress_percent"] = 95
            await buffer._flush()  # noqa: SLF001
            job["status"] = "completed"
            job["stage"] = "done"
            job["progress_percent"] = 100
            job["finished_at"] = datetime.now(timezone.utc).isoformat()
            job["queued"] = count
        except Exception as exc:  # noqa: BLE001
            log.exception("Backfill job failed: %s", job_id)
            job = backfill_jobs[job_id]
            job["status"] = "failed"
            job["error"] = str(exc)
            job["stage"] = "failed"
            job["finished_at"] = datetime.now(timezone.utc).isoformat()

    @app.on_event("startup")
    async def _startup() -> None:
        buffer.start()
        log.info("Layer1 cloud service started, forwarding to %s", api_url)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await buffer.stop()
        await sink.close()

    @app.post("/api/backfill/run")
    async def backfill_run(req: BackfillRequest) -> dict[str, int | str]:
        try:
            repo = _parse_repo_url(req.repo_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        count = 0

        async def on_events(events):
            nonlocal count
            count += len(events)
            await buffer.add(events)

        token = req.github_token or cfg.get("github", {}).get("token", "") or os.getenv("ORGLENS_GITHUB_TOKEN", "")
        state = FixedFromDateState(req.from_date)

        runner = PercevalRunner(
            repos=[
                {
                    "owner": repo["owner"],
                    "repo": repo["repo"],
                    "git_url": repo["git_url"],
                }
            ],
            github_token=token,
            normalizer=normalizer,
            state=state,
            on_events=on_events,
        )

        await runner.run_all()
        await buffer._flush()  # noqa: SLF001
        return {"queued": count, "repo": repo["repo_key"]}

    @app.post("/api/backfill/start", response_model=BackfillStartResponse)
    async def backfill_start(req: BackfillRequest) -> BackfillStartResponse:
        try:
            repo = _parse_repo_url(req.repo_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        job_id = str(uuid.uuid4())
        backfill_jobs[job_id] = {
            "job_id": job_id,
            "repo": repo["repo_key"],
            "status": "running",
            "stage": "starting",
            "progress_percent": 0,
            "queued": 0,
            "error": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }
        asyncio.create_task(_run_backfill_job(job_id, req))
        return BackfillStartResponse(job_id=job_id, repo=repo["repo_key"], status="running")

    @app.get("/api/backfill/status/{job_id}")
    async def backfill_status(job_id: str) -> dict[str, Any]:
        job = backfill_jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Backfill job not found")
        return job

    return app


def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)
    app = create_layer1_cloud_app(args.config)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level.lower())


if __name__ == "__main__":
    main()
