from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    COMMIT = "commit"
    PR_OPEN = "pr_open"
    PR_MERGE = "pr_merge"
    PR_REVIEW = "pr_review"
    ISSUE_ASSIGN = "issue_assign"
    ISSUE_CLOSE = "issue_close"


class EventSource(str, Enum):
    WEBHOOK = "webhook"
    PERCEVAL_GIT = "perceval_git"
    PERCEVAL_GITHUB = "perceval_github"


class RawEventIn(BaseModel):
    event_id: str
    source: EventSource
    repo: str
    actor: str
    event_type: EventType
    target: str
    module: str
    timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeadLetterRecord(BaseModel):
    raw_payload: dict[str, Any]
    error_reason: str
    received_at: datetime
    source: str | None = None


class IngestStatusSnapshot(BaseModel):
    events_received_today: int
    events_processed_today: int
    duplicates_dropped_today: int
    dead_letters_today: int
    redis_queue_depth: int
