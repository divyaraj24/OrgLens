from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger("orglens.auto")


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fully automatic one-command cloud pipeline for a public GitHub repository"
    )
    p.add_argument("repo_url", help="Public GitHub repository URL")
    p.add_argument("--from-date", default=None, help="Optional lower bound date (ISO8601)")
    p.add_argument("--layer1-url", default=os.getenv("ORGLENS_LAYER1_URL", "http://127.0.0.1:8080"))
    p.add_argument("--core-url", default=os.getenv("ORGLENS_CORE_URL", "http://127.0.0.1:8001"))
    p.add_argument(
        "--use-remote-aws",
        action="store_true",
        help="Target remote AWS stack using ORGLENS_AWS_HOST (or 16.112.78.142)",
    )
    p.add_argument(
        "--aws-host",
        default=os.getenv("ORGLENS_AWS_HOST", "16.112.78.142"),
        help="AWS stack host when --use-remote-aws is provided",
    )
    p.add_argument("--api-key", default=os.getenv("ORGLENS_API_KEY", ""))
    p.add_argument("--github-token", default=os.getenv("ORGLENS_GITHUB_TOKEN", ""))
    p.add_argument("--single-repo-mode", action="store_true", default=True)
    p.add_argument("--queue-timeout", type=int, default=1800, help="Queue drain timeout in seconds")
    p.add_argument("--poll-interval", type=float, default=2.0, help="Queue poll interval in seconds")
    p.add_argument(
        "--preflight-timeout",
        type=float,
        default=20.0,
        help="Health-check timeout for target services in seconds",
    )
    p.add_argument("--report-file", default=None, help="Optional JSON report output path")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def _extract_owner_repo(repo_url: str) -> str:
    cleaned = repo_url[:-4] if repo_url.endswith(".git") else repo_url
    prefix = "https://github.com/"
    if not cleaned.startswith(prefix):
        raise ValueError(f"Unsupported repository URL: {repo_url}")
    owner_repo = cleaned[len(prefix):].strip("/")
    if "/" not in owner_repo:
        raise ValueError(f"Invalid repository URL: {repo_url}")
    return owner_repo


async def _resolve_created_at(client: httpx.AsyncClient, repo_url: str) -> str:
    owner_repo = _extract_owner_repo(repo_url)
    resp = await client.get(f"https://api.github.com/repos/{owner_repo}", timeout=30.0)
    resp.raise_for_status()
    created_at = resp.json().get("created_at")
    if not isinstance(created_at, str) or not created_at:
        raise RuntimeError("Could not resolve repository created_at")
    return created_at


async def _check_health_with_retry(
    client: httpx.AsyncClient,
    url: str,
    timeout: float,
    retries: int = 5,
    backoff_seconds: float = 2.0,
) -> None:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            resp = await client.get(url, timeout=timeout)
            resp.raise_for_status()
            return
        except httpx.HTTPError as exc:
            last_exc = exc
            if attempt == retries - 1:
                break
            await asyncio.sleep(backoff_seconds)

    assert last_exc is not None
    raise last_exc


def _auth_headers(api_key: str) -> dict[str, str]:
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}"}


def _render_progress(prefix: str, percent: float, detail: str = "") -> None:
    width = 30
    p = max(0.0, min(100.0, float(percent)))
    filled = int((p / 100.0) * width)
    bar = "#" * filled + "-" * (width - filled)
    tail = f" {detail}" if detail else ""
    sys.stdout.write(f"\r{prefix} [{bar}] {p:6.2f}%{tail}")
    sys.stdout.flush()


def _finish_progress() -> None:
    sys.stdout.write("\n")
    sys.stdout.flush()


async def _wait_for_queue_drain(
    client: httpx.AsyncClient,
    core_url: str,
    api_key: str,
    timeout_seconds: int,
    poll_interval: float,
) -> dict[str, Any]:
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    last_status: dict[str, Any] = {}
    start_depth: int | None = None
    transient_errors = 0

    while asyncio.get_event_loop().time() < deadline:
        try:
            resp = await client.get(f"{core_url}/api/ingest/status", headers=_auth_headers(api_key), timeout=20.0)
            resp.raise_for_status()
            last_status = resp.json()
            transient_errors = 0
        except (httpx.HTTPError, ValueError):
            transient_errors += 1
            if transient_errors >= 10:
                _finish_progress()
                raise
            await asyncio.sleep(poll_interval)
            continue

        depth = int(last_status.get("redis_queue_depth", 0))
        if start_depth is None:
            start_depth = max(depth, 1)

        done_ratio = (start_depth - depth) / start_depth
        _render_progress("Queue drain", done_ratio * 100.0, f"depth={depth}")
        if depth == 0:
            _render_progress("Queue drain", 100.0, "depth=0")
            _finish_progress()
            return last_status
        await asyncio.sleep(poll_interval)

    _finish_progress()

    raise TimeoutError(f"Queue did not drain in {timeout_seconds}s; last_status={last_status}")


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    started = datetime.now(timezone.utc)

    layer1_url = args.layer1_url
    core_url = args.core_url
    if args.use_remote_aws:
        layer1_url = f"http://{args.aws_host}:8080"
        core_url = f"http://{args.aws_host}:8001"

    async with httpx.AsyncClient() as client:
        if not args.single_repo_mode:
            raise RuntimeError("Single-repo mode is required and cannot be disabled.")

        await _check_health_with_retry(
            client,
            f"{layer1_url}/health",
            timeout=args.preflight_timeout,
        )
        await _check_health_with_retry(
            client,
            f"{core_url}/health",
            timeout=args.preflight_timeout,
        )

        from_date = args.from_date
        if not from_date:
            from_date = await _resolve_created_at(client, args.repo_url)

        if args.single_repo_mode:
            reset = await client.post(
                f"{core_url}/api/admin/reset-repo-data",
                headers=_auth_headers(args.api_key),
                timeout=60.0,
            )
            reset.raise_for_status()

        backfill_payload: dict[str, Any] = {
            "repo_url": args.repo_url,
            "from_date": from_date,
        }
        if args.github_token:
            backfill_payload["github_token"] = args.github_token

        backfill_start = await client.post(
            f"{layer1_url}/api/backfill/start",
            json=backfill_payload,
            timeout=60.0,
        )
        backfill_start.raise_for_status()
        backfill_job = backfill_start.json()
        backfill_job_id = backfill_job["job_id"]

        last_queued = -1
        last_stage = ""
        transient_errors = 0
        while True:
            try:
                status_resp = await client.get(
                    f"{layer1_url}/api/backfill/status/{backfill_job_id}",
                    timeout=30.0,
                )
                status_resp.raise_for_status()
                backfill_status = status_resp.json()
                transient_errors = 0
            except (httpx.HTTPError, ValueError):
                transient_errors += 1
                if transient_errors >= 10:
                    _finish_progress()
                    raise
                await asyncio.sleep(args.poll_interval)
                continue

            queued = int(backfill_status.get("queued", 0))
            state = backfill_status.get("status", "running")
            stage = str(backfill_status.get("stage", "running"))
            pct = float(backfill_status.get("progress_percent", 0))
            if queued != last_queued or stage != last_stage:
                _render_progress("Backfill", pct, f"stage={stage} queued={queued}")
                last_queued = queued
                last_stage = stage

            if state in {"completed", "failed"}:
                if state == "failed":
                    _finish_progress()
                    raise RuntimeError(f"Backfill failed: {backfill_status.get('error')}")
                _render_progress("Backfill", 100.0, f"stage=done queued={queued}")
                _finish_progress()
                backfill_result = backfill_status
                break

            await asyncio.sleep(args.poll_interval)

        status_after_drain = await _wait_for_queue_drain(
            client=client,
            core_url=core_url,
            api_key=args.api_key,
            timeout_seconds=args.queue_timeout,
            poll_interval=args.poll_interval,
        )

        owner_repo = _extract_owner_repo(args.repo_url)

        analytics = await client.post(
            f"{core_url}/api/run/analytics",
            json={"repo": owner_repo, "log_level": args.log_level},
            headers=_auth_headers(args.api_key),
            timeout=None,
        )
        analytics.raise_for_status()
        analytics_result = analytics.json()

        risk = await client.get(
            f"{core_url}/api/risk/summary",
            params={"repo": owner_repo},
            timeout=60.0,
        )
        risk.raise_for_status()

        trends = await client.get(
            f"{core_url}/api/trends/weekly",
            params={"repo": owner_repo},
            timeout=60.0,
        )
        trends.raise_for_status()

        overview_payload: dict[str, Any] | None = None
        overview_error: str | None = None
        try:
            overview = await client.get(
                f"{core_url}/api/overview/forecast",
                params={"repo": owner_repo},
                timeout=60.0,
            )
            overview.raise_for_status()
            overview_payload = overview.json()
        except (httpx.HTTPError, ValueError) as exc:
            overview_error = f"{type(exc).__name__}: {exc}"

    finished = datetime.now(timezone.utc)
    return {
        "ok": True,
        "repo_url": args.repo_url,
        "repo": _extract_owner_repo(args.repo_url),
        "target_layer1_url": layer1_url,
        "target_core_url": core_url,
        "from_date": from_date,
        "single_repo_mode": args.single_repo_mode,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": round((finished - started).total_seconds(), 3),
        "backfill": backfill_result,
        "ingest_status": status_after_drain,
        "analytics": analytics_result,
        "risk_summary": risk.json(),
        "weekly_trends": trends.json(),
        "overview_forecast": overview_payload,
        "overview_forecast_error": overview_error,
    }


def _write_report(result: dict[str, Any], report_file: str | None) -> Path:
    if report_file:
        out = Path(report_file)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = Path("reports") / f"auto_pipeline_{stamp}.json"

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return out


def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)
    result = asyncio.run(_run(args))
    report = _write_report(result, args.report_file)

    log.info("Automatic pipeline completed repo=%s", result["repo"])
    log.info("Report saved to %s", report)


if __name__ == "__main__":
    main()
