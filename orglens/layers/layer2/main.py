from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import uvicorn

from orglens.layers.layer2.api import create_ingestion_app
from orglens.layers.layer2.archive import MinioArchive
from orglens.layers.layer2.processor import EventProcessor
from orglens.layers.layer2.queue import RedisStreamQueue
from orglens.layers.layer2.settings import Layer2Settings, load_layer2_settings
from orglens.layers.layer2.status import IngestStatus
from orglens.layers.layer2.storage import PostgresStore
from orglens.layers.layer2.worker import StreamWorker

log = logging.getLogger("orglens.layer2")


class Layer2Runtime:
    def __init__(self, settings: Layer2Settings, run_worker: bool = True) -> None:
        self.settings = settings
        self.api_key = settings.api_key
        self.signature_secret = settings.api_signing_secret
        self.signature_tolerance_seconds = settings.api_signature_tolerance_seconds

        self.queue = RedisStreamQueue(
            redis_url=settings.redis_url,
            stream_name=settings.redis_stream,
            group_name=settings.redis_group,
            consumer_name=settings.redis_consumer,
        )
        self.store = PostgresStore(settings.pg_dsn)
        self.archive = MinioArchive(
            endpoint_url=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket=settings.minio_bucket,
            secure=settings.minio_secure,
        )

        self.status = IngestStatus()
        self.processor = EventProcessor(store=self.store, archive=self.archive)
        self.worker = StreamWorker(
            queue=self.queue,
            processor=self.processor,
            status=self.status,
            store=self.store,
        )
        self._worker_task: asyncio.Task | None = None
        self._run_worker = run_worker

    async def start(self) -> None:
        await self.store.connect()
        await self.queue.ensure_group()
        if self._run_worker:
            self._worker_task = asyncio.create_task(self.worker.run_forever())

    async def stop(self) -> None:
        await self.worker.stop()
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        await self.queue.close()
        await self.store.close()


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OrgLens Layer 2 Ingestion Pipeline")
    p.add_argument("--config", default=None, help="Path to config file")
    p.add_argument("--mode", choices=["api", "file", "replay"], default="api")
    p.add_argument("--input", default=None, help="Input JSON file for --mode file")
    p.add_argument("--from-date", default=None, help="Replay lower bound date, e.g. 2024-01-01")
    p.add_argument("--repo", default=None, help="Repo to replay, e.g. django/django")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


async def _run_file_mode(runtime: Layer2Runtime, input_file: str) -> None:
    payload = json.loads(Path(input_file).read_text())
    if not isinstance(payload, list):
        raise ValueError("Input JSON must be an array of RawEvents")
    source = payload[0].get("source", "file") if payload else "file"

    result = await runtime.processor.process_batch(payload=payload, source=source)
    await runtime.status.mark_received(result.events_received)
    await runtime.status.mark_processed(
        written=result.events_written,
        duplicates=result.duplicates,
        dead_letters=result.dead_letters,
    )
    log.info(
        "File mode complete: received=%d written=%d duplicates=%d dead_letters=%d",
        result.events_received,
        result.events_written,
        result.duplicates,
        result.dead_letters,
    )


async def _run_replay_mode(runtime: Layer2Runtime, from_date: str, repo: str) -> None:
    parsed_from = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
    keys = await runtime.archive.list_keys(repo=repo, from_date=parsed_from)
    log.info("Replay mode: %d parquet files matched for repo=%s from=%s", len(keys), repo, from_date)

    for key in keys:
        rows = await runtime.archive.read_events(key)
        source = rows[0].get("source", "replay") if rows else "replay"
        result = await runtime.processor.process_batch(payload=rows, source=source)
        await runtime.status.mark_received(result.events_received)
        await runtime.status.mark_processed(
            written=result.events_written,
            duplicates=result.duplicates,
            dead_letters=result.dead_letters,
        )
        log.info(
            "Replayed %s: received=%d written=%d duplicates=%d dead_letters=%d",
            key,
            result.events_received,
            result.events_written,
            result.duplicates,
            result.dead_letters,
        )


async def _run(args: argparse.Namespace) -> None:
    settings = load_layer2_settings(args.config)

    if args.mode == "api":
        runtime = Layer2Runtime(settings=settings, run_worker=True)
        app = create_ingestion_app(runtime)
        uvicorn_cfg = uvicorn.Config(
            app,
            host=settings.api_host,
            port=settings.api_port,
            log_level=args.log_level.lower(),
        )
        server = uvicorn.Server(uvicorn_cfg)
        await server.serve()
        return

    runtime = Layer2Runtime(settings=settings, run_worker=False)
    await runtime.start()
    try:
        if args.mode == "file":
            if not args.input:
                raise ValueError("--input is required in file mode")
            await _run_file_mode(runtime, args.input)
        else:
            if not args.from_date or not args.repo:
                raise ValueError("--from-date and --repo are required in replay mode")
            await _run_replay_mode(runtime, from_date=args.from_date, repo=args.repo)
    finally:
        await runtime.stop()


def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
