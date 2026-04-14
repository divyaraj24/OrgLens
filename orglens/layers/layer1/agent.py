"""
OrgLens Layer 1 — Local Agent entry point.

Starts:
  1. APScheduler job for Perceval (every N hours, optionally immediately)
  2. uvicorn serving the FastAPI webhook listener
  3. Batch buffer background timer

All three run in the same asyncio event loop.

Usage:
    orglens-agent
    orglens-agent --output file
    orglens-agent --output api
    orglens-agent --output pg
    orglens-agent --run-once           # run Perceval once then exit
    orglens-agent --config /path/to/config.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal

import uvicorn

log = logging.getLogger("orglens")


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="OrgLens Layer 1 — Local Data Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--config", default=None, help="Path to config.yaml")
    p.add_argument(
        "--output",
        choices=["file", "api", "pg"],
        default=None,
        help="Override output.target from config (file | api | pg)",
    )
    p.add_argument(
        "--run-once",
        action="store_true",
        help="Run the Perceval backfill once then exit (no webhook server)",
    )
    p.add_argument("--log-level", default="INFO", help="Logging level")
    return p.parse_args()


async def _run_agent(args: argparse.Namespace) -> None:
    from orglens.layers.layer1.buffer.batch_buffer import BatchBuffer
    from orglens.config import load_config
    from orglens.layers.layer1.models.state import StateManager
    from orglens.layers.layer1.normalizer.normalizer import Normalizer
    from orglens.layers.layer1.output.router import build_output_sink
    from orglens.layers.layer1.sources.perceval_runner import PercevalRunner
    from orglens.layers.layer1.sources.webhook_listener import create_webhook_app

    # ── Load config ───────────────────────────────────────────────────────────
    cfg = load_config(args.config)

    # ── Build components ──────────────────────────────────────────────────────
    normalizer = Normalizer(module_map_path=cfg.get("module_map_path", "module_map.yaml"))
    state = StateManager(state_file=cfg.get("state_file", "state.json"))
    sink = build_output_sink(cfg.get("output", {}), override=args.output)

    buffer = BatchBuffer(
        sink_fn=sink.send,
        max_events=cfg.get("buffer", {}).get("max_events", 100),
        flush_interval=float(cfg.get("buffer", {}).get("flush_interval_seconds", 30)),
    )

    repos = cfg.get("repos", [])
    github_token = cfg.get("github", {}).get("token", "")
    if not github_token:
        log.warning("No GitHub token configured — Perceval GitHub calls will be rate-limited")

    runner = PercevalRunner(
        repos=repos,
        github_token=github_token,
        normalizer=normalizer,
        state=state,
        on_events=buffer.add,
    )

    # ── --run-once mode ───────────────────────────────────────────────────────
    if args.run_once:
        log.info("=== Run-once mode: Perceval backfill ===")
        buffer.start()
        await runner.run_all()
        await buffer.stop()
        await sink.close()
        log.info("Run-once complete.")
        return

    # ── Normal daemon mode ────────────────────────────────────────────────────
    buffer.start()

    # APScheduler for periodic Perceval runs
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    scheduler = AsyncIOScheduler()
    poll_hours = float(cfg.get("scheduler", {}).get("poll_interval_hours", 6))

    scheduler.add_job(
        runner.run_all,
        "interval",
        hours=poll_hours,
        id="perceval_poll",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    log.info("Perceval scheduler started (every %.1f hours)", poll_hours)

    # Optional: run once immediately on startup
    if cfg.get("scheduler", {}).get("run_on_start", True):
        log.info("Running Perceval backfill immediately on startup ...")
        asyncio.get_event_loop().create_task(runner.run_all())

    # Webhook FastAPI server
    webhook_cfg = cfg.get("webhook", {})
    app = create_webhook_app(
        normalizer=normalizer,
        on_events=buffer.add,
        webhook_secret=webhook_cfg.get("secret") or None,
        skip_signature_check=webhook_cfg.get("skip_signature_check", False),
    )
    port = int(webhook_cfg.get("port", 8080))

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        log.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Start uvicorn in background
    server_config = uvicorn.Config(app, host="0.0.0.0", port=port, loop="none",
                                   log_level="warning")
    server = uvicorn.Server(server_config)
    server_task = loop.create_task(server.serve())

    log.info("OrgLens agent running — webhook on :%d  |  press Ctrl-C to stop", port)

    await stop_event.wait()

    log.info("Shutting down …")
    server.should_exit = True
    await server_task
    scheduler.shutdown(wait=False)
    await buffer.stop()
    await sink.close()
    log.info("Goodbye.")


def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)
    try:
        asyncio.run(_run_agent(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
