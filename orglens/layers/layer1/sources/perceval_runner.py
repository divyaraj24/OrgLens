"""
PercevalRunner — runs Perceval as a subprocess for three categories:
  • git commits
  • GitHub pull requests (+ reviews)
  • GitHub issues

Each run uses --from-date for incremental fetching, then updates the
StateManager checkpoint on success.

This module is intentionally kept as thin orchestration — heavy parsing
lives in the Normalizer.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
from typing import Any, Callable, Dict, Generator, List, Optional

from orglens.layers.layer1.models.state import StateManager
from orglens.layers.layer1.normalizer.normalizer import Normalizer

log = logging.getLogger(__name__)


def _resolve_perceval_bin() -> str:
    """Resolve Perceval CLI path across different install layouts."""
    explicit = os.getenv("ORGLENS_PERCEVAL_BIN")
    if explicit:
        return explicit

    for candidate in ("perceval", "sir-perceval"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved

    # Keep previous behavior as a final fallback so errors stay explicit.
    return "perceval"


# ── Subprocess helpers ────────────────────────────────────────────────────────

def _run_perceval_git(
    git_url: str,
    from_date: Optional[str],
) -> Generator[Dict[str, Any], None, None]:
    """
    Stream Perceval git output line-by-line as parsed dicts.
    Each line is a JSON object (Perceval --json-line output).
    """
    perceval_bin = _resolve_perceval_bin()
    cmd = [perceval_bin, "git", git_url, "--json-line"]
    if from_date:
        cmd += ["--from-date", from_date]

    log.info("Running: %s", " ".join(cmd))
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            log.debug("Non-JSON line from perceval git: %s", line[:120])

    proc.wait()
    if proc.returncode not in (0, 1):
        stderr_text = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(
            f"perceval git exited {proc.returncode}: {stderr_text[:500]}"
        )


def _run_perceval_github(
    owner: str,
    repo: str,
    category: str,
    token: str,
    from_date: Optional[str],
) -> Generator[Dict[str, Any], None, None]:
    """Stream Perceval github output as parsed dicts."""
    perceval_bin = _resolve_perceval_bin()
    cmd = [
        perceval_bin, "github", owner, repo,
        "--category", category,
        "--json-line",
    ]
    if token:
        cmd += ["--api-token", token]
    if from_date:
        cmd += ["--from-date", from_date]

    log.info("Running: perceval github %s/%s --category %s --from-date %s",
             owner, repo, category, from_date)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            log.debug("Non-JSON line from perceval github: %s", line[:120])

    proc.wait()
    if proc.returncode not in (0, 1):
        stderr_text = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(
            f"perceval github exited {proc.returncode}: {stderr_text[:500]}"
        )


# ── PercevalRunner ────────────────────────────────────────────────────────────

class PercevalRunner:
    """
    Orchestrates Perceval runs for all repositories and feeds normalized
    events to a callback (typically the BatchBuffer).

    Usage:
        runner = PercevalRunner(config, normalizer, state, on_events=buffer.add)
        await runner.run_all()   # run once
    """

    def __init__(
        self,
        repos: List[Dict[str, str]],
        github_token: str,
        normalizer: Normalizer,
        state: StateManager,
        on_events: Callable,  # async callable: (List[RawEvent]) -> Awaitable[None]
    ):
        self._repos = repos
        self._token = github_token
        self._normalizer = normalizer
        self._state = state
        self._on_events = on_events

    async def run_all(self) -> None:
        """Run Perceval for all repositories (commits, PRs, issues)."""
        for repo_cfg in self._repos:
            owner = repo_cfg["owner"]
            name = repo_cfg["repo"]
            git_url = repo_cfg.get("git_url", f"https://github.com/{owner}/{name}.git")
            repo_key = f"{owner}/{name}"

            log.info("== Perceval run: %s ==", repo_key)

            await self._run_commits(repo_key, git_url)
            await self._run_github(repo_key, owner, name, "pull_request")
            await self._run_github(repo_key, owner, name, "issue")

    async def _run_commits(self, repo_key: str, git_url: str) -> None:
        from_date = self._state.get_last_fetch(repo_key, "commits")
        started_at = self._state.now_utc()
        count = 0

        def _iter():
            return _run_perceval_git(git_url, from_date)

        # Run subprocess in thread pool to avoid blocking event loop
        try:
            items = await asyncio.get_event_loop().run_in_executor(
                None, lambda: list(_iter())
            )
        except Exception as exc:
            log.error("Commit fetch failed for %s: %s", repo_key, exc)
            return

        try:
            for item in items:
                events = self._normalizer.normalize_perceval_commit(item, repo_key)
                if events:
                    await self._on_events(events)
                    count += len(events)
        except Exception as exc:
            log.error("Commit processing failed for %s: %s", repo_key, exc)
            return

        if count:
            log.info("Commits: %d events from %s", count, repo_key)
        self._state.set_last_fetch(repo_key, "commits", started_at)

    async def _run_github(
        self, repo_key: str, owner: str, name: str, category: str
    ) -> None:
        state_key = "pull_requests" if category == "pull_request" else "issues"
        from_date = self._state.get_last_fetch(repo_key, state_key)
        started_at = self._state.now_utc()
        count = 0

        try:
            items = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: list(_run_perceval_github(owner, name, category, self._token, from_date)),
            )
        except Exception as exc:
            log.error("%s fetch failed for %s: %s", category, repo_key, exc)
            return

        try:
            for item in items:
                if category == "pull_request":
                    events = self._normalizer.normalize_perceval_pr(item, repo_key)
                else:
                    events = self._normalizer.normalize_perceval_issue(item, repo_key)
                if events:
                    await self._on_events(events)
                    count += len(events)
        except Exception as exc:
            log.error("%s processing failed for %s: %s", category, repo_key, exc)
            return

        log.info("%s: %d events from %s", category, count, repo_key)
        self._state.set_last_fetch(repo_key, state_key, started_at)
