"""
RawEvent — the unified schema for all events flowing through OrgLens Layer 1.

Every commit, PR action, and issue action gets normalized into one of these
before being buffered and sent to the output sink.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class EventType(str, Enum):
    COMMIT = "commit"
    PR_OPEN = "pr_open"
    PR_MERGE = "pr_merge"
    PR_REVIEW = "pr_review"
    ISSUE_ASSIGN = "issue_assign"
    ISSUE_CLOSE = "issue_close"


class ReviewVerdict(str, Enum):
    APPROVED = "APPROVED"
    CHANGES_REQUESTED = "CHANGES_REQUESTED"
    COMMENTED = "COMMENTED"


class RawEvent(BaseModel):
    """
    Unified event schema.  All fields are optional except the identity fields;
    only the fields relevant to the event_type will be populated.
    """

    # ── Identity ──────────────────────────────────────────────────────────────
    event_id: str = Field(..., description="Stable dedup key (e.g. sha, pr_number+type)")
    event_type: EventType
    source: str = Field(
        ...,
        description="'webhook' | 'perceval_git' | 'perceval_github'",
    )
    repo: str = Field(..., description="owner/repo  e.g. 'django/django'")
    timestamp: datetime = Field(..., description="Event timestamp (UTC)")
    target: str = Field(..., description="File path, module, PR ID, or issue ID")
    metadata: dict = Field(default_factory=dict)

    # ── Actor ─────────────────────────────────────────────────────────────────
    actor: str = Field(..., description="Primary actor (author, PR opener, etc.)")
    actor_email: Optional[str] = None

    # ── Module resolution ─────────────────────────────────────────────────────
    module: Optional[str] = Field(None, description="Resolved logical module name")

    # ── Commit fields ─────────────────────────────────────────────────────────
    sha: Optional[str] = None
    files_changed: List[str] = Field(default_factory=list)
    lines_added: int = 0
    lines_deleted: int = 0
    co_authors: List[str] = Field(default_factory=list)
    commit_message: Optional[str] = None

    # ── PR fields ─────────────────────────────────────────────────────────────
    pr_number: Optional[int] = None
    merged_by: Optional[str] = None
    reviewer: Optional[str] = None
    verdict: Optional[ReviewVerdict] = None
    requested_reviewers: List[str] = Field(default_factory=list)

    # ── Issue fields ──────────────────────────────────────────────────────────
    issue_number: Optional[int] = None
    assignees: List[str] = Field(default_factory=list)
    closer: Optional[str] = None
    labels: List[str] = Field(default_factory=list)

    model_config = ConfigDict(use_enum_values=True)
