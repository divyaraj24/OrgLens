"""
Normalizer — converts raw Perceval items and GitHub webhook payloads into
the unified RawEvent schema.

Responsibilities:
  • Resolve file paths → module names via module_map.yaml
  • Extract co-authors from commit message trailers
  • Extract merged_by from PR payloads
  • Assign event_type for every event
  • Generate stable event_ids for deduplication
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import yaml

from orglens.layers.layer1.models.raw_event import EventType, RawEvent, ReviewVerdict

log = logging.getLogger(__name__)

# Matches:  Co-authored-by: Name <email@example.com>
_CO_AUTHOR_RE = re.compile(
    r"Co-authored-by:\s*(.+?)\s*<[^>]+>", re.IGNORECASE | re.MULTILINE
)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _parse_dt(value: Any) -> datetime:
    """Parse an ISO-8601 string or epoch int to an aware UTC datetime."""
    if value is None:
        return _utcnow()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    # string
    s = str(value)
    # Perceval git sometimes gives dates like 'Tue Mar 24 16:48:14 2026 -0700'
    try:
        from dateutil import parser
        dt = parser.parse(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        # Fallback to simple isoformat
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            log.warning("Could not parse datetime %r; using now", value)
            return _utcnow()


def _stable_id(*parts: str) -> str:
    """Create a short, stable dedup key from multiple strings."""
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _canonical_actor(
    login: Optional[str] = None,
    name: Optional[str] = None,
    email: Optional[str] = None,
) -> str:
    """Prefer GitHub login; fall back to email local-part; then free-form name."""
    if login:
        return login
    if email and "@" in email:
        return email.split("@", 1)[0]
    if name:
        return name
    return "unknown"


class Normalizer:
    """
    Stateless (except for the module map) converter.

    Usage:
        normalizer = Normalizer(module_map_path="module_map.yaml")
        events = normalizer.normalize_perceval_commit(perceval_item)
    """

    def __init__(self, module_map_path: str = "module_map.yaml"):
        self._module_rules: List[tuple[str, str]] = []  # (prefix, module_name)
        self._default_module: str = "other"
        self._load_module_map(module_map_path)

    # ── Module map ────────────────────────────────────────────────────────────

    def _load_module_map(self, path: str) -> None:
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            self._default_module = data.get("default_module", "other")
            for module_name, cfg in data.get("modules", {}).items():
                for prefix in cfg.get("prefixes", []):
                    self._module_rules.append((prefix, module_name))
            log.info("Loaded %d module prefix rules from %s", len(self._module_rules), path)
        except (OSError, yaml.YAMLError) as exc:
            log.warning("Could not load module map (%s); all files → 'other'", exc)

    @staticmethod
    def _sanitize_module_name(value: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
        return cleaned or "other"

    def _infer_module_from_path(self, file_path: str) -> str:
        path = (file_path or "").strip().replace("\\", "/")
        if not path:
            return self._default_module

        lower_path = path.lower().lstrip("./")
        filename = lower_path.rsplit("/", 1)[-1]

        build_files = {
            "setup.py",
            "pyproject.toml",
            "tox.ini",
            "makefile",
            "dockerfile",
            "package.json",
            "pom.xml",
            "build.gradle",
            "cargo.toml",
            "go.mod",
        }
        if filename in build_files:
            return "build"

        build_prefixes = (".github/", "docs/", "doc/", "scripts/", "ci/", "infra/")
        if lower_path.startswith(build_prefixes):
            return "build"

        if "/test" in lower_path or lower_path.startswith("tests/") or lower_path.startswith("test/"):
            return "testing"

        parts = [p for p in lower_path.split("/") if p and p not in {".", ".."}]
        if not parts:
            return self._default_module

        # Ignore filename part for extension-bearing paths.
        if "." in parts[-1]:
            parts = parts[:-1]
        if not parts:
            return self._default_module

        wrappers = {
            "src",
            "lib",
            "libs",
            "pkg",
            "packages",
            "app",
            "apps",
            "service",
            "services",
            "internal",
            "backend",
            "frontend",
            "client",
            "server",
        }
        while parts and parts[0] in wrappers:
            parts = parts[1:]
        if not parts:
            return self._default_module

        # Multi-layer module key: first two meaningful folders when available.
        if len(parts) >= 2:
            return self._sanitize_module_name(f"{parts[0]}_{parts[1]}")
        return self._sanitize_module_name(parts[0])

    def resolve_module(self, file_path: str) -> str:
        """Return the logical module name for a file path."""
        for prefix, module_name in self._module_rules:
            if file_path.startswith(prefix):
                return module_name
        return self._infer_module_from_path(file_path)

    # ── Co-author extraction ──────────────────────────────────────────────────

    @staticmethod
    def extract_co_authors(commit_message: str) -> List[str]:
        """Return a list of co-author names from 'Co-authored-by:' trailers."""
        return _CO_AUTHOR_RE.findall(commit_message or "")

    # ── Perceval: git commits ─────────────────────────────────────────────────

    def normalize_perceval_commit(self, item: Dict[str, Any], repo: str) -> List[RawEvent]:
        """
        Convert a single Perceval git item into one or more RawEvents.

        Produces:
          • One 'commit' RawEvent per file touched (module resolved per file).
          • If co-authors are present, additional 'commit' events for each.
        """
        data = item.get("data", {})
        sha = data.get("commit", "")
        author = data.get("Author", "")
        author_email = data.get("AuthorEmail", "")
        actor = _canonical_actor(name=author, email=author_email)
        authored_date = _parse_dt(data.get("AuthorDate"))
        message = data.get("message", "")
        co_authors = self.extract_co_authors(message)

        files = data.get("files", [])
        events: List[RawEvent] = []

        # Primary author events — one per file
        for f in files:
            file_path = f.get("file", "")
            # Git reports "-" for binary files
            added_raw = f.get("added", 0) or 0
            removed_raw = f.get("removed", 0) or 0
            added = 0 if added_raw == "-" else int(added_raw)
            removed = 0 if removed_raw == "-" else int(removed_raw)
            module = self.resolve_module(file_path)

            events.append(
                RawEvent(
                    event_id=_stable_id(repo, sha, file_path, "commit"),
                    event_type=EventType.COMMIT,
                    source="perceval_git",
                    repo=repo,
                    timestamp=authored_date,
                    target=file_path,
                    metadata={
                        "module": module,
                        "lines_changed": added + removed,
                        "co_authors": co_authors,
                    },
                    actor=actor,
                    actor_email=author_email,
                    module=module,
                    sha=sha,
                    files_changed=[file_path],
                    lines_added=added,
                    lines_deleted=removed,
                    co_authors=co_authors,
                    commit_message=message,
                )
            )

        # Co-author shadow events (one per co-author per file)
        for co_author in co_authors:
            for f in files:
                file_path = f.get("file", "")
                module = self.resolve_module(file_path)
                events.append(
                    RawEvent(
                        event_id=_stable_id(repo, sha, file_path, "commit", co_author),
                        event_type=EventType.COMMIT,
                        source="perceval_git",
                        repo=repo,
                        timestamp=authored_date,
                        target=file_path,
                        metadata={"module": module, "co_authored": True},
                        actor=co_author,
                        module=module,
                        sha=sha,
                        files_changed=[file_path],
                        lines_added=0,
                        lines_deleted=0,
                        commit_message=message,
                    )
                )

        return events

    # ── Perceval: pull requests ───────────────────────────────────────────────

    def normalize_perceval_pr(self, item: Dict[str, Any], repo: str) -> List[RawEvent]:
        """
        Convert a Perceval github PR item to RawEvents.

        Produces:
          • pr_open  — when the PR was created
          • pr_merge — if the PR was merged
          • pr_review — one per review (verdict captured)
        """
        data = item.get("data", {})
        pr_number = data.get("number")
        author = (data.get("user") or {}).get("login", "unknown")
        created_at = _parse_dt(data.get("created_at"))
        merged_at = data.get("merged_at")
        merged_by_obj = data.get("merged_by") or {}
        merged_by = merged_by_obj.get("login") if merged_by_obj else None

        # Primary files list (may be absent in older Perceval versions)
        files = [f.get("filename", "") for f in data.get("files", [])]
        modules = sorted({self.resolve_module(fp) for fp in files if fp})
        module_targets: List[Optional[str]] = modules if modules else [None]

        requested_reviewers = [
            (r.get("login") or "") for r in data.get("requested_reviewers", [])
        ]

        events: List[RawEvent] = []

        # pr_open
        for module in module_targets:
            events.append(
                RawEvent(
                    event_id=_stable_id(repo, str(pr_number), "pr_open", module or "all"),
                    event_type=EventType.PR_OPEN,
                    source="perceval_github",
                    repo=repo,
                    timestamp=created_at,
                    target=module or f"pr:{pr_number}",
                    metadata={
                        "requested_reviewers": requested_reviewers,
                        "files_changed": files,
                    },
                    actor=author,
                    module=module,
                    pr_number=pr_number,
                    files_changed=files,
                    requested_reviewers=requested_reviewers,
                )
            )

        # pr_merge
        if merged_at:
            for module in module_targets:
                merge_actor = _canonical_actor(login=merged_by, name=author)
                events.append(
                    RawEvent(
                        event_id=_stable_id(repo, str(pr_number), "pr_merge", module or "all"),
                        event_type=EventType.PR_MERGE,
                        source="perceval_github",
                        repo=repo,
                        timestamp=_parse_dt(merged_at),
                        target=module or f"pr:{pr_number}",
                        metadata={"merged_by": merged_by, "files_changed": files},
                        actor=merge_actor,
                        module=module,
                        pr_number=pr_number,
                        merged_by=merged_by,
                        files_changed=files,
                    )
                )

        # pr_review — one event per review
        for review in data.get("reviews", []):
            reviewer = (review.get("user") or {}).get("login", "unknown")
            raw_verdict = (review.get("state") or "COMMENTED").upper()
            try:
                verdict = ReviewVerdict(raw_verdict)
            except ValueError:
                verdict = ReviewVerdict.COMMENTED
            for module in module_targets:
                events.append(
                    RawEvent(
                        event_id=_stable_id(
                            repo,
                            str(pr_number),
                            "pr_review",
                            reviewer,
                            raw_verdict,
                            module or "all",
                        ),
                        event_type=EventType.PR_REVIEW,
                        source="perceval_github",
                        repo=repo,
                        timestamp=_parse_dt(review.get("submitted_at")),
                        target=module or f"pr:{pr_number}",
                        metadata={"review_verdict": raw_verdict},
                        actor=reviewer,
                        module=module,
                        pr_number=pr_number,
                        reviewer=reviewer,
                        verdict=verdict,
                    )
                )

        return events

    # ── Perceval: issues ──────────────────────────────────────────────────────

    def normalize_perceval_issue(self, item: Dict[str, Any], repo: str) -> List[RawEvent]:
        """
        Convert a Perceval github issue item to RawEvents.

        Produces:
          • issue_assign — one per assignee (at time of fetch)
          • issue_close  — if the issue is closed
        """
        data = item.get("data", {})
        issue_number = data.get("number")
        created_at = _parse_dt(data.get("created_at"))
        closed_at = data.get("closed_at")
        labels = [lb.get("name", "") for lb in data.get("labels", [])]
        assignees = [(a.get("login") or "") for a in data.get("assignees", [])]
        closer_obj = data.get("closed_by") or {}
        closer = closer_obj.get("login") if closer_obj else None

        events: List[RawEvent] = []

        for assignee in assignees:
            events.append(
                RawEvent(
                    event_id=_stable_id(repo, str(issue_number), "issue_assign", assignee),
                    event_type=EventType.ISSUE_ASSIGN,
                    source="perceval_github",
                    repo=repo,
                    timestamp=created_at,
                    target=f"issue:{issue_number}",
                    metadata={"labels": labels, "assignees": assignees},
                    actor=assignee,
                    issue_number=issue_number,
                    assignees=assignees,
                    labels=labels,
                )
            )

        if closed_at:
            events.append(
                RawEvent(
                    event_id=_stable_id(repo, str(issue_number), "issue_close"),
                    event_type=EventType.ISSUE_CLOSE,
                    source="perceval_github",
                    repo=repo,
                    timestamp=_parse_dt(closed_at),
                    target=f"issue:{issue_number}",
                    metadata={"closer": closer, "labels": labels},
                    actor=closer or "unknown",
                    issue_number=issue_number,
                    closer=closer,
                    assignees=assignees,
                    labels=labels,
                )
            )

        return events

    # ── GitHub Webhooks ───────────────────────────────────────────────────────

    def normalize_webhook(
        self, payload: Dict[str, Any], github_event_header: str
    ) -> List[RawEvent]:
        """
        Dispatch a raw GitHub webhook payload to the appropriate normalizer.

        github_event_header is the value of the X-GitHub-Event header.
        Returns an empty list for unsupported events.
        """
        repo_obj = payload.get("repository") or {}
        repo = repo_obj.get("full_name", "unknown/unknown")

        dispatch: Dict[str, Any] = {
            "push": self._webhook_push,
            "pull_request": self._webhook_pull_request,
            "pull_request_review": self._webhook_pr_review,
            "issues": self._webhook_issue,
        }

        handler = dispatch.get(github_event_header)
        if handler is None:
            log.debug("Ignoring unsupported webhook event: %s", github_event_header)
            return []
        return handler(payload, repo)

    # ── Webhook: push ─────────────────────────────────────────────────────────

    def _webhook_push(self, payload: Dict[str, Any], repo: str) -> List[RawEvent]:
        events: List[RawEvent] = []
        sender_login = (payload.get("sender") or {}).get("login")
        for commit in payload.get("commits", []):
            sha = commit.get("id", "")
            author = commit.get("author", {})
            actor_email = author.get("email")
            actor = _canonical_actor(
                login=sender_login or author.get("username"),
                name=author.get("name"),
                email=actor_email,
            )
            timestamp = _parse_dt(commit.get("timestamp"))
            message = commit.get("message", "")
            co_authors = self.extract_co_authors(message)

            all_files: List[str] = (
                commit.get("added", [])
                + commit.get("modified", [])
                + commit.get("removed", [])
            )

            for file_path in all_files:
                module = self.resolve_module(file_path)
                events.append(
                    RawEvent(
                        event_id=_stable_id(repo, sha, file_path, "commit"),
                        event_type=EventType.COMMIT,
                        source="webhook",
                        repo=repo,
                        timestamp=timestamp,
                        target=file_path,
                        metadata={
                            "module": module,
                            "lines_changed": 0,
                            "co_authors": co_authors,
                        },
                        actor=actor,
                        actor_email=actor_email,
                        module=module,
                        sha=sha,
                        files_changed=[file_path],
                        co_authors=co_authors,
                        commit_message=message,
                    )
                )

            # Co-author shadow events
            for co_author in co_authors:
                for file_path in all_files:
                    module = self.resolve_module(file_path)
                    events.append(
                        RawEvent(
                            event_id=_stable_id(repo, sha, file_path, "commit", co_author),
                            event_type=EventType.COMMIT,
                            source="webhook",
                            repo=repo,
                            timestamp=timestamp,
                            target=file_path,
                            metadata={"module": module, "co_authored": True},
                            actor=co_author,
                            module=module,
                            sha=sha,
                            files_changed=[file_path],
                            commit_message=message,
                        )
                    )

        return events

    # ── Webhook: pull_request ─────────────────────────────────────────────────

    def _webhook_pull_request(self, payload: Dict[str, Any], repo: str) -> List[RawEvent]:
        action = payload.get("action", "")
        pr = payload.get("pull_request", {})
        pr_number = pr.get("number")
        actor = (pr.get("user") or {}).get("login", "unknown")
        files: List[str] = []  # Webhook PR payloads don't include file lists
        events: List[RawEvent] = []

        if action == "opened":
            requested = [
                (r.get("login") or "") for r in pr.get("requested_reviewers", [])
            ]
            events.append(
                RawEvent(
                    event_id=_stable_id(repo, str(pr_number), "pr_open"),
                    event_type=EventType.PR_OPEN,
                    source="webhook",
                    repo=repo,
                    timestamp=_parse_dt(pr.get("created_at")),
                    target=f"pr:{pr_number}",
                    metadata={"requested_reviewers": requested, "files_changed": files},
                    actor=actor,
                    pr_number=pr_number,
                    files_changed=files,
                    requested_reviewers=requested,
                )
            )

        elif action == "closed" and pr.get("merged"):
            merged_by = (pr.get("merged_by") or {}).get("login")
            events.append(
                RawEvent(
                    event_id=_stable_id(repo, str(pr_number), "pr_merge"),
                    event_type=EventType.PR_MERGE,
                    source="webhook",
                    repo=repo,
                    timestamp=_parse_dt(pr.get("merged_at")),
                    target=f"pr:{pr_number}",
                    metadata={"merged_by": merged_by},
                    actor=merged_by or actor,
                    pr_number=pr_number,
                    merged_by=merged_by,
                )
            )

        return events

    # ── Webhook: pull_request_review ──────────────────────────────────────────

    def _webhook_pr_review(self, payload: Dict[str, Any], repo: str) -> List[RawEvent]:
        review = payload.get("review", {})
        pr = payload.get("pull_request", {})
        pr_number = pr.get("number")
        reviewer = (review.get("user") or {}).get("login", "unknown")
        raw_verdict = (review.get("state") or "COMMENTED").upper()
        try:
            verdict = ReviewVerdict(raw_verdict)
        except ValueError:
            verdict = ReviewVerdict.COMMENTED

        return [
            RawEvent(
                event_id=_stable_id(repo, str(pr_number), "pr_review", reviewer, raw_verdict),
                event_type=EventType.PR_REVIEW,
                source="webhook",
                repo=repo,
                timestamp=_parse_dt(review.get("submitted_at")),
                target=f"pr:{pr_number}",
                metadata={"review_verdict": raw_verdict},
                actor=reviewer,
                pr_number=pr_number,
                reviewer=reviewer,
                verdict=verdict,
            )
        ]

    # ── Webhook: issues ───────────────────────────────────────────────────────

    def _webhook_issue(self, payload: Dict[str, Any], repo: str) -> List[RawEvent]:
        action = payload.get("action", "")
        issue = payload.get("issue", {})
        issue_number = issue.get("number")
        labels = [lb.get("name", "") for lb in issue.get("labels", [])]
        assignees = [(a.get("login") or "") for a in issue.get("assignees", [])]
        events: List[RawEvent] = []

        if action == "assigned":
            assignee = (payload.get("assignee") or {}).get("login", "unknown")
            events.append(
                RawEvent(
                    event_id=_stable_id(repo, str(issue_number), "issue_assign", assignee),
                    event_type=EventType.ISSUE_ASSIGN,
                    source="webhook",
                    repo=repo,
                    timestamp=_parse_dt(issue.get("created_at")),
                    target=f"issue:{issue_number}",
                    metadata={"labels": labels, "assignees": assignees},
                    actor=assignee,
                    issue_number=issue_number,
                    assignees=assignees,
                    labels=labels,
                )
            )

        elif action == "closed":
            closer = (payload.get("sender") or {}).get("login", "unknown")
            events.append(
                RawEvent(
                    event_id=_stable_id(repo, str(issue_number), "issue_close"),
                    event_type=EventType.ISSUE_CLOSE,
                    source="webhook",
                    repo=repo,
                    timestamp=_parse_dt(issue.get("closed_at")),
                    target=f"issue:{issue_number}",
                    metadata={"closer": closer, "labels": labels},
                    actor=closer,
                    issue_number=issue_number,
                    closer=closer,
                    assignees=assignees,
                    labels=labels,
                )
            )

        return events
