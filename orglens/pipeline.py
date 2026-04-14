from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from orglens.config import load_config
from orglens.layers.layer1.normalizer.normalizer import Normalizer
from orglens.layers.layer1.sources.perceval_runner import PercevalRunner
from orglens.layers.layer2.archive import MinioArchive
from orglens.layers.layer2.processor import EventProcessor
from orglens.layers.layer2.settings import load_layer2_settings
from orglens.layers.layer2.storage import PostgresStore

log = logging.getLogger("orglens.pipeline")


@dataclass
class _Counters:
    received: int = 0
    written: int = 0
    duplicates: int = 0
    dead_letters: int = 0


class _FileStore:
    """Minimal async store used when PostgreSQL is unavailable."""

    def __init__(self, events_file: str, dead_letters_file: str | None = None) -> None:
        self._events_file = Path(events_file)
        self._dead_letters_file = Path(dead_letters_file) if dead_letters_file else self._events_file.with_name(
            f"{self._events_file.stem}_dead_letters.jsonl"
        )
        self._seen_event_ids: set[str] = set()
        self._events_file.parent.mkdir(parents=True, exist_ok=True)
        self._dead_letters_file.parent.mkdir(parents=True, exist_ok=True)

    async def connect(self) -> None:
        return None

    async def write_events(self, events: list[Any]) -> tuple[int, int]:
        written = 0
        duplicates = 0
        with self._events_file.open("a", encoding="utf-8") as f:
            for e in events:
                event_id = getattr(e, "event_id", None)
                if event_id in self._seen_event_ids:
                    duplicates += 1
                    continue
                self._seen_event_ids.add(event_id)
                f.write(json.dumps(e.model_dump(mode="json")) + "\n")
                written += 1
        return written, duplicates

    async def write_dead_letter(self, letter: Any) -> None:
        with self._dead_letters_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(letter.model_dump(mode="json"), default=str) + "\n")

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
        return None

    async def close(self) -> None:
        return None


class _FromDateState:
    """State adapter that always fetches from a single explicit lower bound."""

    def __init__(self, from_date: str | None) -> None:
        self._from_date = from_date

    def get_last_fetch(self, repo: str, event_key: str) -> str | None:
        return self._from_date

    def set_last_fetch(self, repo: str, event_key: str, ts: datetime) -> None:
        return None

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)


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


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="One-command Layer 1 + Layer 2 pipeline run for a public repository"
    )
    p.add_argument("--repo-url", required=True, help="Public GitHub repository URL")
    p.add_argument("--config", default=None, help="Path to config file")
    p.add_argument("--from-date", default=None, help="Optional lower bound, e.g. 2026-01-01T00:00:00Z")
    p.add_argument("--github-token", default=None, help="Optional token override")
    p.add_argument(
        "--storage-mode",
        default="auto",
        choices=["auto", "postgres", "file"],
        help="Storage backend: auto (default), postgres, or file",
    )
    p.add_argument(
        "--events-file",
        default="reports/pipeline_events.jsonl",
        help="JSONL path used when storage-mode=file or auto fallback is used",
    )
    p.add_argument(
        "--report-file",
        default=None,
        help="Optional JSON report path; default is reports/pipeline_<timestamp>.json",
    )
    p.add_argument("--log-level", default="INFO", help="Logging level")
    return p.parse_args()


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    started = datetime.now(timezone.utc)

    cfg = load_config(args.config)
    repo = _parse_repo_url(args.repo_url)
    repo_cfg = [{"owner": repo["owner"], "repo": repo["repo"], "git_url": repo["git_url"]}]

    github_token = args.github_token
    if github_token is None:
        github_token = cfg.get("github", {}).get("token", "")

    normalizer = Normalizer(module_map_path=cfg.get("module_map_path", "module_map.yaml"))
    layer2 = load_layer2_settings(args.config)
    storage_backend = "postgres"
    fallback_reason: str | None = None

    store: Any
    archive: Any | None = None
    if args.storage_mode == "file":
        storage_backend = "file"
        store = _FileStore(events_file=args.events_file)
        await store.connect()
    else:
        pg_store = PostgresStore(layer2.pg_dsn)
        try:
            await pg_store.connect()
            store = pg_store
            archive = MinioArchive(
                endpoint_url=layer2.minio_endpoint,
                access_key=layer2.minio_access_key,
                secret_key=layer2.minio_secret_key,
                bucket=layer2.minio_bucket,
                secure=layer2.minio_secure,
            )
        except Exception as exc:
            if args.storage_mode == "postgres":
                raise
            storage_backend = "file"
            fallback_reason = str(exc)
            log.warning("PostgreSQL unavailable; falling back to file storage: %s", exc)
            store = _FileStore(events_file=args.events_file)
            await store.connect()

    processor = EventProcessor(store=store, archive=archive)

    counters = _Counters()

    async def on_events(events):
        counters.received += len(events)
        payload = [e.model_dump(mode="json") for e in events]
        res = await processor.process_batch(payload=payload, source=events[0].source if events else "unknown")
        counters.written += res.events_written
        counters.duplicates += res.duplicates
        counters.dead_letters += res.dead_letters

    state = _FromDateState(args.from_date)
    runner = PercevalRunner(
        repos=repo_cfg,
        github_token=github_token,
        normalizer=normalizer,
        state=state,
        on_events=on_events,
    )

    await runner.run_all()

    finished = datetime.now(timezone.utc)
    await store.write_metrics(
        window_start=started,
        window_end=finished,
        repo=repo["repo_key"],
        source=None,
        events_received=counters.received,
        events_written=counters.written,
        duplicates=counters.duplicates,
        dead_letters=counters.dead_letters,
        avg_lag_ms=0.0,
    )
    await store.close()

    return {
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": round((finished - started).total_seconds(), 3),
        "repo": repo["repo_key"],
        "repo_url": repo["git_url"],
        "from_date": args.from_date,
        "github_token_used": bool(github_token),
        "storage_backend": storage_backend,
        "events_file": args.events_file if storage_backend == "file" else None,
        "fallback_reason": fallback_reason,
        "events_received": counters.received,
        "events_written": counters.written,
        "duplicates": counters.duplicates,
        "dead_letters": counters.dead_letters,
    }


def _write_report(result: dict[str, Any], report_file: str | None) -> Path:
    if report_file:
        out = Path(report_file)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = Path("reports") / f"pipeline_{stamp}.json"

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2))
    return out


def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)
    result = asyncio.run(_run(args))
    report = _write_report(result, args.report_file)

    log.info("Pipeline run complete for %s", result["repo"])
    log.info("events_received=%d events_written=%d duplicates=%d dead_letters=%d",
             result["events_received"], result["events_written"], result["duplicates"], result["dead_letters"])
    log.info("Report saved to %s", report)


if __name__ == "__main__":
    main()
